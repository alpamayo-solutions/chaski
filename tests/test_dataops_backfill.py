"""The DataOps backfill (``chaski.dataops.backfill``) against a fake door, a
fake historian and a real buffer.

The historian here is a stand-in for the ``Historian`` port: chaski ships no
implementation, and a real one needs a database. The broker-backed checks are
in ``test_dataops_backfill_integration.py``.

The reference for "what live processing produces" is the producer's own
``@on_metric`` handler called once per record, in timestamp order, on a fresh
instance over the whole history: the order live ingest dispatches in-order data.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import UTC, datetime

import pandas as pd
import pytest
from dataops_fakes import NODE_ID, FakeDoor, FakeRuntime, kv_entry, run_async, signal_entry
from node_api_fake import CLIENT_ID, CLIENT_SECRET, FakeNodeApi

from chaski.clock import Clock
from chaski.dataops import AnnotationOutput, Backfill, NodeHistorian, Producer, SignalRangeInput, every, on_metric
from chaski.dataops.backfill import INITIAL, BackfillRunner, tick_instants
from chaski.dataops.buffer import Buffer
from chaski.dataops.outputs import bind_annotation_outputs
from chaski.dataops.service import make_handler, replay_changed_producers, synthetic_record
from chaski.dataops.triggers import CronSpec, IntervalSpec
from chaski.door import Record
from chaski.doorbell import Doorbell

SIGNAL = "sig-state"
DAY = 86400.0


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


class FakeHistorian:
    """The ``Historian`` port over an in-memory point list, like a historian
    that kept what the stream no longer holds."""

    def __init__(self, points: dict[str, list[tuple[float, object]]]) -> None:
        self.points = {sid: sorted(rows) for sid, rows in points.items()}
        self.windows: list[tuple[float, float]] = []

    def window(self, signal_id, start, end):
        self.windows.append((start, end))
        rows = [(ts, v) for ts, v in self.points.get(signal_id, []) if start <= ts < end]
        return pd.DataFrame({"ts": [r[0] for r in rows], "value": [r[1] for r in rows]}, columns=["ts", "value"])

    def latest_before(self, signal_id, before):
        rows = [(ts, v) for ts, v in self.points.get(signal_id, []) if ts <= before]
        return rows[-1] if rows else None


class FakeService(FakeRuntime):
    """What the runner needs from a ``DataOpsService``."""

    def __init__(self, door, buffer, historian) -> None:
        super().__init__(door, buffer, historian)
        self._ingest = None
        self.rejected: list[tuple[str, dict]] = []
        self.runner: BackfillRunner | None = None

    def backfill_holds(self, producer: str) -> bool:
        return self.runner is not None and self.runner.holds(producer)

    def reject(self, consumer, subject, rejected) -> None:
        self.rejected.append((consumer, subject))

    def _backfill_finding_topic(self) -> str:
        return f"colca/v1/_Finding/{NODE_ID}/dataops/backfill"


def _door() -> FakeDoor:
    return FakeDoor(
        [
            signal_entry(SIGNAL, "state"),
            kv_entry(f"colca/v1/_AnnotationType/{NODE_ID}/cycle", {"id": "at-cycle", "name": "cycle"}),
        ]
    )


def _history(start: float, count: int, step: float = 600.0) -> list[tuple[float, int]]:
    """A machine state signal: two samples running, one stopped, repeating."""
    return [(start + i * step, 0 if i % 3 == 0 else 1) for i in range(count)]


def segmenter(horizon="10d", window="6h"):
    """A producer that cuts cycles from a state signal, with in-memory state
    and source-transparent reads, like a cycle segmenter."""

    class Segmenter(Producer):
        name = "segmenter"
        system_element_name = "press"
        state_version = 1
        backfill = Backfill(horizon=horizon, window=window)

        state = SignalRangeInput("state", window="1h")
        cycle = AnnotationOutput("cycle")

        def __init__(self) -> None:
            super().__init__()
            self.open: float | None = None
            self.handled: list[float] = []

        def snapshot_state(self):
            return {"open": self.open}

        def restore_state(self, state) -> None:
            self.open = state["open"]

        @on_metric("state")
        async def on_state(self, metric) -> None:
            self.handled.append(metric.timestamp)
            previous = self.state.latest_value_before(metric.timestamp - 1e-6)
            if metric.value and not previous:
                self.open = metric.timestamp
                self.cycle.write_interval(self.open, None, "running")
            elif not metric.value and previous and self.open is not None:
                self.cycle.write_interval(self.open, metric.timestamp, "done")
                self.open = None

    return Segmenter


def _attach(cls, service):
    instance = cls().attach(service)
    bind_annotation_outputs([instance], node_id=NODE_ID, mount="")
    return instance


def _annotations(door: FakeDoor) -> dict[str, dict]:
    """The last write per annotation id, as a consumer keeps them."""
    kept: dict[str, dict] = {}
    for topic, payload in door.published:
        if "/_Annotation/" in topic:
            body = json.loads(payload)
            kept[body["annotation_id"]] = {k: body[k] for k in ("time_start", "time_end", "value")}
    return kept


def _annotation_writes(door: FakeDoor) -> list[str]:
    return [json.loads(p)["annotation_id"] for t, p in door.published if "/_Annotation/" in t]


async def _reference(cls, history, tmp_path) -> dict[str, dict]:
    """What a single live pass over the whole history emits."""
    buffer = Buffer(tmp_path / "reference.sqlite3")
    try:
        door = _door()
        service = FakeService(door, buffer, None)
        instance = _attach(cls, service)
        handler = make_handler(instance.on_state)
        for offset, (ts, value) in enumerate(history, start=1):
            buffer.append(SIGNAL, ts, value)
            record = synthetic_record(SIGNAL, ts, value)
            await handler(Record(**{**record.__dict__, "offset": offset}))
        return _annotations(door)
    finally:
        buffer.close()


class _Ingest:
    """The two things the runner reads from the ingest."""

    def __init__(self) -> None:
        self.page_lock = threading.Lock()
        self.caught_up = Doorbell()
        self.caught_up.ring()


def _setup(
    cls, tmp_path, *, archive, buffered, name="buffer.sqlite3", rate=1000.0, busy=1.0, historian=None, stall_after=600.0
):
    """A service whose historian holds ``archive`` + ``buffered`` and whose
    buffer holds ``buffered``, as after the ingest drained the stream."""
    buffer = Buffer(tmp_path / name)
    for ts, value in buffered:
        buffer.append(SIGNAL, ts, value)
    if historian is None:
        historian = FakeHistorian({SIGNAL: [*archive, *buffered]})
    service = FakeService(_door(), buffer, historian)
    service._ingest = _Ingest()
    instance = _attach(cls, service)
    runner = BackfillRunner(service, [instance], rate=rate, busy=busy, stall_after=stall_after)
    service.runner = runner
    runner.plan()
    return service, instance, runner, buffer


async def _run_until_done(runner, name="segmenter", timeout=20.0):
    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    deadline = time.monotonic() + timeout
    while runner.holds(name) or runner.status() or runner._buffer.backfill_jobs(pending_only=True):
        assert not task.done() or task.exception() is None, task.exception()
        assert time.monotonic() < deadline, "backfill did not finish"
        await asyncio.sleep(0.01)
    stop.set()
    await task


# ─── the first backfill ────────────────────────────────────────────────────


@run_async
async def test_backfill_emits_what_one_live_pass_over_the_same_history_emits(tmp_path):
    now = time.time()
    history = _history(now - 3 * DAY, 3 * 144)
    cls = segmenter()
    archive, buffered = history[:300], history[300:]
    service, instance, runner, buffer = _setup(cls, tmp_path, archive=archive, buffered=buffered)
    assert runner.holds("segmenter")

    await _run_until_done(runner)

    expected = await _reference(segmenter(), history, tmp_path)
    assert _annotations(service.door) == expected
    assert len(expected) > 50
    # Every record handled once, in order, by the one instance.
    assert instance.handled == [ts for ts, _ in history]
    job = buffer.backfill_job("segmenter", INITIAL)
    assert job is not None and job["done"]
    # Handed over like a replay: the next start does not replay the buffer again.
    assert buffer.watermark("segmenter") == history[-1][0]
    buffer.close()


@run_async
async def test_a_backfill_through_the_node_api_reads_windows_larger_than_a_page(tmp_path):
    """The same backfill with ``NodeHistorian`` as the historian: each 6 h
    window holds 36 samples and the API answers 5 per page, so every window
    is read over several pages, and the result is that of one live pass."""
    now = time.time()
    # Microsecond timestamps, what the historian stores and the API returns.
    history = [(round(ts, 6), value) for ts, value in _history(now - 3 * DAY, 3 * 144)]
    archive, buffered = history[:300], history[300:]
    api = FakeNodeApi()
    api.add(SIGNAL, [*archive, *buffered])
    historian = NodeHistorian(
        "http://node.test", CLIENT_ID, CLIENT_SECRET, page_size=5, transport=api.transport(), sleep=lambda _s: None
    )
    cls = segmenter()
    service, instance, runner, buffer = _setup(cls, tmp_path, archive=archive, buffered=buffered, historian=historian)

    await _run_until_done(runner)

    expected = await _reference(segmenter(), history, tmp_path)
    assert _annotations(service.door) == expected
    assert instance.handled == [ts for ts, _ in history]
    windows = {(request["from"], request["to"]) for request in api.metric_requests}
    paged = [request for request in api.metric_requests if "cursor" in request]
    assert len(paged) >= len(windows), "every full window took more than one page"
    assert all(request["limit"] == 5 for request in api.metric_requests)
    assert api.tokens_issued == 1
    historian.close()
    buffer.close()


@run_async
async def test_live_triggers_are_held_until_the_handover_and_then_dispatch_once(tmp_path):
    now = time.time()
    history = _history(now - 2 * DAY, 2 * 144)
    cls = segmenter()
    service, instance, runner, buffer = _setup(cls, tmp_path, archive=history[:200], buffered=history[200:250])
    live = make_handler(instance.on_state)
    ingest = service._ingest

    def live_record(offset, ts, value):
        return Record(**{**synthetic_record(SIGNAL, ts, value).__dict__, "offset": offset})

    # The ingest is mid-page: the handover waits for it.
    ingest.page_lock.acquire()
    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    while not (runner.status().get("running") or {}).get("progress", 0) > 0.9:
        await asyncio.sleep(0.01)
    # Records of that page: buffered, but not dispatched while the producer is held.
    for offset, (ts, value) in enumerate(history[250:260], start=1000):
        buffer.append(SIGNAL, ts, value)
        await live(live_record(offset, ts, value))
    assert runner.holds("segmenter")
    ingest.page_lock.release()

    while runner.holds("segmenter"):
        await asyncio.sleep(0.01)
    # Later pages are dispatched live.
    for offset, (ts, value) in enumerate(history[260:], start=2000):
        buffer.append(SIGNAL, ts, value)
        await live(live_record(offset, ts, value))
    stop.set()
    await task

    assert instance.handled == [ts for ts, _ in history]
    assert _annotations(service.door) == await _reference(segmenter(), history, tmp_path)
    buffer.close()


@run_async
async def test_a_restart_mid_backfill_resumes_at_the_last_committed_window(tmp_path):
    now = time.time()
    history = _history(now - 4 * DAY, 4 * 144)
    archive, buffered = history[:500], history[500:]
    service, instance, runner, buffer = _setup(segmenter(), tmp_path, archive=archive, buffered=buffered, rate=50.0)

    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    while (buffer.backfill_job("segmenter", INITIAL) or {}).get("windows", 0) < 5:
        await asyncio.sleep(0.005)
    stop.set()
    await task
    job = buffer.backfill_job("segmenter", INITIAL)
    assert job is not None and not job["done"]
    position = job["position"]
    first_run = list(instance.handled)
    buffer.close()

    # A new process: same buffer file, a fresh instance.
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    service2 = FakeService(service.door, buffer, service.historian)
    service2._ingest = _Ingest()
    resumed = _attach(segmenter(), service2)
    runner2 = BackfillRunner(service2, [resumed], rate=1000.0, busy=1.0)
    service2.runner = runner2
    runner2.plan()
    assert runner2.holds("segmenter")
    await _run_until_done(runner2)

    # It went on at the committed window, with the checkpointed state.
    assert resumed.handled[0] >= position
    assert sorted(set(first_run) | set(resumed.handled)) == [ts for ts, _ in history]
    assert _annotations(service.door) == await _reference(segmenter(), history, tmp_path)
    buffer.close()


@run_async
async def test_without_a_historian_the_backfill_covers_what_the_buffer_holds(tmp_path):
    now = time.time()
    history = _history(now - DAY, 144)
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    for ts, value in history:
        buffer.append(SIGNAL, ts, value)
    service = FakeService(_door(), buffer, None)
    service._ingest = _Ingest()
    instance = _attach(segmenter(horizon="30d"), service)
    runner = BackfillRunner(service, [instance], rate=1000.0, busy=1.0)
    service.runner = runner
    runner.plan()

    await _run_until_done(runner)

    assert instance.handled == [ts for ts, _ in history]
    buffer.close()


@run_async
async def test_a_held_producer_is_left_out_of_the_code_change_replay(tmp_path):
    now = time.time()
    history = _history(now - DAY, 50)
    service, instance, _runner, buffer = _setup(segmenter(), tmp_path, archive=[], buffered=history)

    await replay_changed_producers(service, [instance])

    assert instance.handled == []
    assert buffer.code_hash("segmenter") is None
    buffer.close()


@run_async
async def test_a_finished_backfill_is_not_run_again_on_the_next_start(tmp_path):
    now = time.time()
    history = _history(now - DAY, 50)
    service, _instance, runner, buffer = _setup(segmenter(), tmp_path, archive=history, buffered=[])
    await _run_until_done(runner)
    writes = len(service.door.published)

    again = _attach(segmenter(), service)
    runner2 = BackfillRunner(service, [again], rate=1000.0, busy=1.0)
    service.runner = runner2
    runner2.plan()

    assert not runner2.holds("segmenter")
    await _run_until_done(runner2)
    assert again.handled == []
    assert len([t for t, _ in service.door.published[writes:] if "/_Annotation/" in t]) == 0
    buffer.close()


# ─── repair ────────────────────────────────────────────────────────────────


@run_async
async def test_a_repair_over_a_processed_range_rewrites_the_same_annotations(tmp_path):
    now = time.time()
    history = _history(now - 2 * DAY, 2 * 144)
    service, instance, runner, buffer = _setup(segmenter(), tmp_path, archive=history[:100], buffered=history[100:])
    await _run_until_done(runner)
    before = _annotations(service.door)
    writes = len(_annotation_writes(service.door))

    job = runner.request("segmenter", history[0][0], history[-1][0] + 1)
    assert job.startswith("repair-")
    await _run_until_done(runner)

    # The same ids again, nothing new: overlap with live output is idempotent.
    assert _annotations(service.door) == before
    rewritten = _annotation_writes(service.door)[writes:]
    assert rewritten and set(rewritten) <= set(before)
    # The live instance was not used, and live dispatch was never held.
    assert instance.handled == [ts for ts, _ in history]
    assert not runner.holds("segmenter")
    done = buffer.backfill_job("segmenter", job)
    assert done is not None and done["done"]
    buffer.close()


def test_a_repair_needs_a_known_producer_and_a_forward_range(tmp_path):
    _service, _instance, runner, buffer = _setup(segmenter(), tmp_path, archive=[], buffered=[])
    with pytest.raises(KeyError):
        runner.request("nope", 0.0, 1.0)
    with pytest.raises(ValueError):
        runner.request("segmenter", 2.0, 1.0)
    buffer.close()


# ─── throttle ──────────────────────────────────────────────────────────────


@run_async
async def test_the_rate_limits_windows_per_second(tmp_path):
    now = time.time()
    history = _history(now - 10 * 3600, 60)
    cls = segmenter(horizon="10h", window="1h")
    _service, _instance, runner, buffer = _setup(cls, tmp_path, archive=history, buffered=[], rate=20.0)

    began = time.monotonic()
    await _run_until_done(runner)
    elapsed = time.monotonic() - began

    windows = buffer.backfill_job("segmenter", INITIAL)["windows"]
    assert windows >= 10
    # Each window but the handover is followed by its pause.
    assert elapsed >= (windows - 1) / 20.0
    buffer.close()


@run_async
async def test_the_busy_share_limits_how_much_of_the_time_it_works(tmp_path):
    now = time.time()
    history = _history(now - 6 * 3600, 36)
    cls = segmenter(horizon="6h", window="1h")

    class Slow(cls):  # type: ignore[valid-type, misc]
        @on_metric("state")
        async def on_state(self, metric) -> None:
            time.sleep(0.01)
            await super().on_state(metric)

    _service, _instance, runner, buffer = _setup(Slow, tmp_path, archive=history, buffered=[], busy=0.25)
    began = time.monotonic()
    await _run_until_done(runner)
    elapsed = time.monotonic() - began

    busy = 0.01 * len(history)
    # At most a quarter of the time is work; the last window has no pause.
    assert elapsed >= busy + 3 * (busy - 0.01 * 6)
    buffer.close()


# ─── ticks, progress ───────────────────────────────────────────────────────


def test_tick_instants_tile_a_range_without_gaps_or_repeats():
    spec = IntervalSpec(seconds=900.0)
    start, mid, end = 1_700_000_123.0, 1_700_003_333.3, 1_700_010_000.0
    tiled = tick_instants(spec, start, mid) + tick_instants(spec, mid, end)
    assert tiled == tick_instants(spec, start, end)
    assert all(t % 900 == 0 for t in tiled)
    hourly = CronSpec("0 * * * *")
    tiled = tick_instants(hourly, start, mid) + tick_instants(hourly, mid, end)
    assert tiled == tick_instants(hourly, start, end) == [1_700_002_800.0, 1_700_006_400.0]


@run_async
async def test_ticks_fire_at_their_instants_between_the_records(tmp_path):
    now = time.time()
    start = (now - 3 * 3600) // 3600 * 3600

    class Hourly(Producer):
        name = "hourly"
        system_element_name = "press"
        backfill = Backfill(horizon="4h", window="1h")
        state = SignalRangeInput("state", window="1h")

        def __init__(self) -> None:
            super().__init__()
            self.seen: list[tuple[str, float]] = []

        @on_metric("state")
        async def on_state(self, metric) -> None:
            self.seen.append(("metric", metric.timestamp))

        @every("1h")
        async def tick(self) -> None:
            self.seen.append(("tick", self.now))

    history = [(start + 1800.0 + i * 3600, 1) for i in range(3)]
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    service = FakeService(_door(), buffer, FakeHistorian({SIGNAL: history}))
    service.clock = Clock()
    service._ingest = _Ingest()
    instance = Hourly().attach(service)
    runner = BackfillRunner(service, [instance], rate=1000.0, busy=1.0)
    service.runner = runner
    runner.plan()
    await _run_until_done(runner, "hourly")

    job = buffer.backfill_job("hourly", INITIAL)
    expected = tick_instants(IntervalSpec(seconds=3600.0), job["start"], job["position"])
    assert [ts for kind, ts in instance.seen if kind == "tick"] == expected
    assert [ts for kind, ts in instance.seen if kind == "metric"] == [ts for ts, _ in history]
    order = [ts for _, ts in instance.seen]
    assert order == sorted(order)
    buffer.close()


@run_async
async def test_progress_is_reported_and_the_finding_retired_when_done(tmp_path):
    now = time.time()
    history = _history(now - 2 * DAY, 2 * 144)
    service, _instance, runner, buffer = _setup(
        segmenter(), tmp_path, archive=history[:200], buffered=history[200:], rate=200.0
    )
    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    seen = None
    while seen is None:
        running = runner.status().get("running")
        if running and running["windows"] > 0:
            seen = running
        await asyncio.sleep(0.005)
    assert seen["producer"] == "segmenter"
    assert seen["job"] == INITIAL
    assert seen["to"] == "live edge"
    assert seen["holds_live"] is True
    assert 0 < seen["progress"] < 1
    while runner.holds("segmenter") or runner.status():
        await asyncio.sleep(0.01)
    stop.set()
    await task

    findings = [(t, p) for t, p in service.door.published if t.endswith("/_Finding/n-1/dataops/backfill")]
    assert findings[0][1] and json.loads(findings[0][1])["reason"] == "backfill"
    assert findings[-1][1] == ""  # retracted
    buffer.close()


# ─── independent mode ──────────────────────────────────────────────────────


def closer(mode="independent", horizon="10d", window="6h"):
    """A cycle segmenter whose output does not depend on state carried across
    a boundary: it writes a cycle once, at its end, and opens one only at a
    start condition, so an instance that starts mid-cycle skips that cycle."""

    class Closer(Producer):
        name = "closer"
        system_element_name = "press"
        state_version = 1
        backfill = Backfill(horizon=horizon, window=window, mode=mode)

        state = SignalRangeInput("state", window="1h")
        cycle = AnnotationOutput("cycle")

        def __init__(self) -> None:
            super().__init__()
            self.open: float | None = None
            self.handled: list[float] = []

        def snapshot_state(self):
            return {"open": self.open}

        def restore_state(self, state) -> None:
            self.open = state["open"]

        @on_metric("state")
        async def on_state(self, metric) -> None:
            self.handled.append(metric.timestamp)
            previous = self.state.latest_value_before(metric.timestamp - 1e-6)
            if metric.value and not previous:
                self.open = metric.timestamp
            elif not metric.value and previous and self.open is not None:
                self.cycle.write_interval(self.open, metric.timestamp, "done")
                self.open = None

    return Closer


async def _reference_of(cls, history, tmp_path) -> dict[str, dict]:
    """What a single live pass of ``cls`` over the whole history emits."""
    buffer = Buffer(tmp_path / "reference-closer.sqlite3")
    try:
        door = _door()
        instance = _attach(cls, FakeService(door, buffer, None))
        handler = make_handler(instance.on_state)
        for offset, (ts, value) in enumerate(history, start=1):
            buffer.append(SIGNAL, ts, value)
            await handler(Record(**{**synthetic_record(SIGNAL, ts, value).__dict__, "offset": offset}))
        return _annotations(door)
    finally:
        buffer.close()


def _live_record(offset, ts, value):
    return Record(**{**synthetic_record(SIGNAL, ts, value).__dict__, "offset": offset})


def test_backfill_mode_is_validated_and_hold_live_is_the_default():
    assert Backfill("1d").mode == "hold_live"
    assert Backfill("1d").holds_live
    assert not Backfill("1d", mode="independent").holds_live
    with pytest.raises(ValueError, match="mode"):
        Backfill("1d", mode="later")


@run_async
async def test_an_independent_backfill_leaves_live_dispatch_running_and_ends_at_the_live_start(tmp_path):
    now = time.time()
    history = _history(now - 3 * DAY, 3 * 144)
    later = _history(now + 60.0, 30)
    cls = closer(window="1h")
    service, instance, runner, buffer = _setup(cls, tmp_path, archive=history[:300], buffered=history[300:], rate=40.0)
    began = time.time()

    # Not held: the start replays the buffer on the live instance at once.
    assert not runner.holds("closer")
    job = buffer.backfill_job("closer", INITIAL)
    assert job is not None and not job["done"]
    assert now <= job["end"] <= began
    assert job["start"] == pytest.approx(job["end"] - 10 * DAY)
    await replay_changed_producers(service, [instance])
    assert instance.handled == [ts for ts, _ in history[300:]]

    # Live records are dispatched while the history runs.
    live = make_handler(instance.on_state)
    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    while not (runner.status().get("running") or {}).get("windows", 0) >= 3:
        await asyncio.sleep(0.005)
    for offset, (ts, value) in enumerate(later, start=1000):
        buffer.append(SIGNAL, ts, value)
        await live(_live_record(offset, ts, value))
    assert instance.handled[-len(later) :] == [ts for ts, _ in later]
    running = runner.status()["running"]
    assert running["holds_live"] is False
    assert running["to"] == datetime.fromtimestamp(job["end"], UTC).isoformat()
    assert 0 < running["progress"] < 1
    assert not buffer.backfill_job("closer", INITIAL)["done"]
    while buffer.backfill_jobs(pending_only=True) or runner.status():
        assert not task.done(), task
        await asyncio.sleep(0.01)
    stop.set()
    await task

    # History and live together emit what one live pass over all of it does.
    assert _annotations(service.door) == await _reference_of(closer(), [*history, *later], tmp_path)
    finished = buffer.backfill_job("closer", INITIAL)
    assert finished["done"] and finished["position"] == finished["end"] == job["end"]
    # The history ran on its own instance: the live one saw only live traffic.
    assert instance.handled == [ts for ts, _ in [*history[300:], *later]]
    # And did not overwrite the live instance's checkpoint.
    assert buffer.checkpoint("closer")[2] == {"open": instance.open}
    buffer.close()


@run_async
async def test_a_restart_mid_independent_backfill_keeps_its_live_start(tmp_path):
    now = time.time()
    history = _history(now - 4 * DAY, 4 * 144)
    cls = closer(window="2h")
    service, instance, runner, buffer = _setup(cls, tmp_path, archive=history[:500], buffered=history[500:], rate=50.0)
    await replay_changed_producers(service, [instance])
    boundary = buffer.backfill_job("closer", INITIAL)["end"]

    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    while (buffer.backfill_job("closer", INITIAL) or {}).get("windows", 0) < 5:
        await asyncio.sleep(0.005)
    stop.set()
    await task
    stopped = buffer.backfill_job("closer", INITIAL)
    assert not stopped["done"]
    buffer.close()

    # A new process, later: the live start stays where the first start put it.
    time.sleep(0.05)
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    service2 = FakeService(service.door, buffer, service.historian)
    service2._ingest = _Ingest()
    resumed = _attach(closer(window="2h"), service2)
    runner2 = BackfillRunner(service2, [resumed], rate=1000.0, busy=1.0)
    service2.runner = runner2
    runner2.plan()
    job = buffer.backfill_job("closer", INITIAL)
    assert job["end"] == boundary
    assert job["position"] == stopped["position"]
    assert not runner2.holds("closer")
    await _run_until_done(runner2, "closer")

    finished = buffer.backfill_job("closer", INITIAL)
    assert finished["done"] and finished["position"] == boundary
    assert _annotations(service.door) == await _reference_of(closer(), history, tmp_path)
    buffer.close()


@run_async
async def test_a_held_first_backfill_switched_to_independent_releases_live_at_its_position(tmp_path):
    now = time.time()
    history = _history(now - 2 * DAY, 2 * 144)
    service, _instance, runner, buffer = _setup(
        closer(mode="hold_live"), tmp_path, archive=history[:200], buffered=history[200:], rate=50.0
    )
    assert runner.holds("closer")
    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    while (buffer.backfill_job("closer", INITIAL) or {}).get("windows", 0) < 2:
        await asyncio.sleep(0.005)
    stop.set()
    await task
    held = buffer.backfill_job("closer", INITIAL)
    assert held["end"] is None and not held["done"]

    switched = _attach(closer(), service)
    runner2 = BackfillRunner(service, [switched], rate=1000.0, busy=1.0)
    service.runner = runner2
    before = time.time()
    runner2.plan()
    job = buffer.backfill_job("closer", INITIAL)
    assert not runner2.holds("closer")
    assert job["end"] >= before and job["position"] == held["position"]
    await _run_until_done(runner2, "closer")
    assert buffer.backfill_job("closer", INITIAL)["done"]
    buffer.close()


@run_async
async def test_a_repair_steps_by_its_own_window(tmp_path):
    now = time.time()
    history = _history(now - DAY, 144)
    service, _instance, runner, buffer = _setup(segmenter(window="6h"), tmp_path, archive=history, buffered=[])
    await _run_until_done(runner)
    historian = service.historian

    historian.windows.clear()
    runner.request("segmenter", now - 6 * 3600, now, window="30m")
    await _run_until_done(runner)
    assert {round(end - start) for start, end in historian.windows} == {1800}

    historian.windows.clear()
    runner.request("segmenter", now - 12 * 3600, now)
    await _run_until_done(runner)
    assert {round(end - start) for start, end in historian.windows} == {6 * 3600}

    with pytest.raises(ValueError, match="positive"):
        runner.request("segmenter", now - 3600, now, window=0)
    buffer.close()


@run_async
async def test_a_producer_without_a_backfill_declaration_repairs_with_the_window_it_asks_for(tmp_path):
    now = time.time()
    history = _history(now - DAY, 144)

    class Plain(Producer):
        name = "plain"
        system_element_name = "press"
        state = SignalRangeInput("state", window="1h")

        @on_metric("state")
        async def on_state(self, metric) -> None:
            pass

    buffer = Buffer(tmp_path / "buffer.sqlite3")
    historian = FakeHistorian({SIGNAL: history})
    service = FakeService(_door(), buffer, historian)
    service._ingest = _Ingest()
    instance = Plain().attach(service)
    runner = BackfillRunner(service, [instance], rate=1000.0, busy=1.0)
    service.runner = runner
    runner.plan()
    assert buffer.backfill_jobs() == []

    job = runner.request("plain", now - 12 * 3600, now, window="4h")
    assert buffer.backfill_job("plain", job)["window"] == 4 * 3600
    await _run_until_done(runner, "plain")
    assert [round(end - start) for start, end in historian.windows] == [4 * 3600] * 3
    buffer.close()


def test_a_buffer_from_an_older_release_gains_the_job_window_column(tmp_path):
    import sqlite3

    path = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE backfill_jobs (producer TEXT NOT NULL, job TEXT NOT NULL, start REAL NOT NULL, "
        '"end" REAL, position REAL NOT NULL, windows INTEGER NOT NULL DEFAULT 0, code_hash TEXT NOT NULL, '
        "done INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, PRIMARY KEY (producer, job))"
    )
    conn.execute("INSERT INTO backfill_jobs VALUES ('p', 'initial', 1.0, NULL, 5.0, 2, 'h', 0, 0.0)")
    conn.commit()
    conn.close()

    buffer = Buffer(path)
    job = buffer.backfill_job("p", INITIAL)
    assert job is not None and job["position"] == 5.0 and job["window"] is None
    buffer.close()


# ─── stall ─────────────────────────────────────────────────────────────────


class GatedHistorian(FakeHistorian):
    """A historian whose reads block while ``gate`` is closed: a window that
    does not finish."""

    def __init__(self, points) -> None:
        super().__init__(points)
        self.gate = threading.Event()
        self.gate.set()

    def window(self, signal_id, start, end):
        assert self.gate.wait(20), "the gate stayed closed"
        return super().window(signal_id, start, end)


@run_async
@pytest.mark.parametrize("mode", ["independent", "hold_live"])
async def test_a_backfill_that_finishes_no_window_for_its_bound_reports_stalled_until_it_moves(tmp_path, mode):
    now = time.time()
    history = _history(now - 3 * DAY, 3 * 144)
    historian = GatedHistorian({SIGNAL: history})
    cls = closer(mode=mode, window="1h")
    _service, _instance, runner, buffer = _setup(
        cls, tmp_path, archive=history, buffered=history[-10:], historian=historian, rate=1000.0, stall_after=0.3
    )
    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    try:
        while (runner.status().get("running") or {}).get("windows", 0) < 2:
            assert not task.done(), task
            await asyncio.sleep(0.005)
        assert "stalled" not in runner.status()["running"]

        historian.gate.clear()
        deadline = time.monotonic() + 10
        while "stalled" not in (running := runner.status()["running"]):
            assert time.monotonic() < deadline, "the stall was not reported"
            await asyncio.sleep(0.02)
        assert running["stalled"].startswith("no backfill window finished in ")
        assert running["holds_live"] is (mode == "hold_live")

        historian.gate.set()
        windows = running["windows"]
        while (runner.status().get("running") or {}).get("windows", 0) <= windows:
            await asyncio.sleep(0.005)
        assert "stalled" not in runner.status()["running"]
    finally:
        historian.gate.set()
        stop.set()
        await task
        buffer.close()


@run_async
async def test_the_throttles_pause_is_not_a_stall(tmp_path):
    """At half a window per second the job pauses about two seconds after
    each window; a bound shorter than that pause reports no stall."""
    now = time.time()
    history = _history(now - 3 * DAY, 3 * 144)
    cls = closer(window="1h")
    _service, _instance, runner, buffer = _setup(
        cls, tmp_path, archive=history, buffered=history[-10:], rate=0.5, stall_after=0.5
    )
    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    try:
        seen = []
        deadline = time.monotonic() + 4.5
        while time.monotonic() < deadline:
            assert not task.done(), task
            seen.append(runner.status().get("running") or {})
            await asyncio.sleep(0.05)
        assert max(r.get("windows", 0) for r in seen) >= 2
        assert not [r for r in seen if "stalled" in r]
    finally:
        stop.set()
        await task
        buffer.close()


@run_async
async def test_a_backfill_waiting_for_the_ingest_names_what_it_waits_for_when_stalled(tmp_path):
    now = time.time()
    history = _history(now - 3 * DAY, 3 * 144)
    cls = closer(mode="hold_live", window="1h")
    service, _instance, runner, buffer = _setup(
        cls, tmp_path, archive=history, buffered=history[-10:], rate=1000.0, stall_after=0.2
    )
    # The live ingest never reaches the stream head; without a historian the
    # backfill waits for it.
    service.historian = None
    service._ingest.caught_up = Doorbell()
    stop = asyncio.Event()
    task = asyncio.ensure_future(runner.run(stop))
    try:
        deadline = time.monotonic() + 10
        while "stalled" not in (running := (runner.status().get("running") or {})):
            assert time.monotonic() < deadline, "the stall was not reported"
            assert not task.done(), task
            await asyncio.sleep(0.02)
        assert running["stalled"].endswith("it waits for the live ingest to reach the stream head")
        assert running["windows"] == 0 and running["holds_live"] is True
    finally:
        stop.set()
        await task
        buffer.close()
