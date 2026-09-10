"""Unit tests for chaski.dataops.service's dispatch, trim and tick machinery.

The fetch filter (`signal_ids`) and the dispatch table (`dispatch`) answer
two different questions:

  * `signal_ids` — which points does the ingest loop buffer at all?
  * `dispatch`   — which producer methods fire when a buffered point
                    arrives?

A producer driven purely by `@every`/`@cron` never appears in `dispatch`
(nothing should call it per-record), but its declared `SignalRangeInput`
must still show up in `signal_ids` — otherwise its points never enter the
buffer and its window reads come back silently empty (design §3, §4.1).

No live colca: a fake `Door` (KV only) stands in for KV resolution, same
pattern as `test_dataops_inputs.py`.
"""

from __future__ import annotations

import asyncio
import threading
import time
from unittest.mock import patch

import pytest
from dataops_fakes import FakeDoor, FakeRuntime, run_async, signal_entry
from chaski.door import Record

from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.service import (
    build_dispatch,
    compute_trim_horizons,
    decode_metric,
    make_handler,
    off_loop,
    trim_buffer,
)
from chaski.dataops.triggers import every, on_metric


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


@pytest.fixture
def door():
    return FakeDoor([
        signal_entry("sig-tick", "tick_only_signal"),
        signal_entry("sig-event", "on_metric_signal"),
    ])


@pytest.fixture
def runtime(door, buffer):
    return FakeRuntime(door, buffer)


@pytest.fixture(autouse=True)
def _isolate_registry():
    """Defining test-only Producer subclasses here must never leak into
    other test modules."""
    saved_registry = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved_registry)


# ─── the producers under test ────────────────────────────────────────────


class TickOnlyProducer(Producer):
    """Declares an input but is driven ONLY by @every/@cron — no @on_metric
    at all. Its input's signal id must still land in the fetch filter, or
    the buffer never holds its points."""

    name = "tick_only"
    system_element_name = "SE-Tick"

    tick_input = SignalRangeInput("tick_only_signal", window="1h")

    @every("30s")
    async def tick(self) -> None:
        pass


class EventDrivenProducer(Producer):
    """Declares an input wired to @on_metric — the pre-existing case that
    already worked."""

    name = "event_driven"
    system_element_name = "SE-Event"

    event_input = SignalRangeInput("on_metric_signal", window="1h")

    @on_metric("event_input")
    async def on_event(self, metric) -> None:
        pass


def _instantiate(runtime, *classes):
    return [cls().attach(runtime) for cls in classes]


# ─── signal_ids: the fetch filter ────────────────────────────────────────


def test_tick_only_producer_contributes_its_signal_to_the_filter(runtime):
    """A @every/@cron-only producer's declared input was once never added
    to signal_ids, so its points never entered the buffer."""
    instances = _instantiate(runtime, TickOnlyProducer)
    _dispatch, signal_ids, _unresolved = build_dispatch(runtime, instances)
    assert "sig-tick" in signal_ids


def test_filter_is_the_union_of_tick_only_and_on_metric_inputs(runtime):
    """Denominator check (testing.md): a fix that swapped one omission for
    another (e.g. only tick-driven inputs) would still fail this."""
    instances = _instantiate(runtime, TickOnlyProducer, EventDrivenProducer)
    _dispatch, signal_ids, _unresolved = build_dispatch(runtime, instances)
    assert set(signal_ids) == {"sig-tick", "sig-event"}


# ─── dispatch: unchanged shape — @on_metric only ─────────────────────────


def test_dispatch_table_has_no_entry_for_a_tick_only_producer(runtime):
    """Buffering != dispatching. A tick-only producer's signal belongs in
    the filter, but nothing should call it per-record — it reads its input
    on its own schedule."""
    instances = _instantiate(runtime, TickOnlyProducer)
    dispatch, _signal_ids, _unresolved = build_dispatch(runtime, instances)
    assert dispatch == {}


def test_dispatch_table_still_has_the_on_metric_entry(runtime):
    instances = _instantiate(runtime, TickOnlyProducer, EventDrivenProducer)
    dispatch, _signal_ids, _unresolved = build_dispatch(runtime, instances)
    assert list(dispatch.keys()) == ["sig-event"]
    assert len(dispatch["sig-event"]) == 1


# ─── unresolved input: startup does not raise ────────────────────────────


class UnresolvableProducer(Producer):
    """Declares an input for a signal that doesn't exist in KV yet."""

    name = "unresolvable"
    system_element_name = "SE-Missing"

    missing_input = SignalRangeInput("no_such_signal", window="1h")

    @every("1m")
    async def tick(self) -> None:
        pass


def test_unresolvable_input_is_skipped_not_raised(runtime):
    instances = _instantiate(runtime, UnresolvableProducer)
    dispatch, signal_ids, unresolved = build_dispatch(runtime, instances)  # must not raise
    assert dispatch == {}
    assert signal_ids == []
    # Counted, not just skipped: this is what tells the service to keep
    # retrying instead of running on its timers for the rest of the process.
    assert unresolved == 1


@run_async
async def test_a_late_commissioned_signal_reaches_dispatch_without_a_restart(runtime):
    """A producer routinely starts before the tree it reads exists.

    A signal is commissioned by a separate act — `signal/autobind`, or an
    editor — so a producer that came up first used to keep the
    empty dispatch table it was born with for the life of the process. It
    still ticked, so it looked like it worked: its outputs tracked its inputs
    at the TIMER's cadence instead of the data's, with one WARNING at startup
    to say why.
    """
    import chaski.dataops.service as service_module

    class _Ingest:
        def __init__(self):
            self.bound: list[tuple[dict, list]] = []

        def rebind(self, dispatch, signal_ids):
            self.bound.append((dispatch, list(signal_ids or [])))

    ingest = _Ingest()
    stop = asyncio.Event()
    started: list[str] = []
    calls = {"n": 0}

    # First pass resolves nothing (the signals do not exist yet); the second
    # resolves — exactly what applying a plant model looks like from here.
    def fake_build(runtime, instances):
        calls["n"] += 1
        if calls["n"] == 1:
            return {}, [], 1
        return {"sig-late": ["handler"]}, ["sig-late"], 0

    with patch.object(service_module, "build_dispatch", fake_build):
        await service_module.reresolve_loop(
            runtime, [], ingest, stop, lambda: started.append("ingest"), interval_s=0.01,
        )

    assert ingest.bound == [({"sig-late": ["handler"]}, ["sig-late"])]
    assert started == ["ingest"], "the loop must start if nothing resolved at startup"


@run_async
async def test_the_retry_stops_once_everything_resolves(runtime):
    """A startup race, not a steady-state poll — a resolved input never
    becomes unresolved, so this must not keep reading KV forever."""
    import chaski.dataops.service as service_module

    class _Ingest:
        def rebind(self, dispatch, signal_ids):
            pass

    calls = {"n": 0}

    def fake_build(runtime, instances):
        calls["n"] += 1
        return {"s": ["h"]}, ["s"], 0

    with patch.object(service_module, "build_dispatch", fake_build):
        await asyncio.wait_for(
            service_module.reresolve_loop(
                runtime, [], _Ingest(), asyncio.Event(), lambda: None, interval_s=0.01,
            ),
            timeout=2,
        )

    assert calls["n"] == 1


# ─── trim horizons + periodic wiring ─────────────────────────────────────
#
# design §3: "horizon per signal = max(the largest `window` declared on it,
# the broker's metrics retention)". These pin compute_trim_horizons (the
# horizon computation) and trim_buffer (the wired call) — the two halves
# DataOpsService.serve() wires onto a periodic scheduler job.


class ShortWindowProducer(Producer):
    """Declares a window shorter than the broker's own retention."""

    name = "short_window"
    system_element_name = "SE-Short"

    short_input = SignalRangeInput("on_metric_signal", window="10s")

    @on_metric("short_input")
    async def on_event(self, metric) -> None:
        pass


class LongWindowProducer(Producer):
    """Declares a LONGER window on the SAME signal as ShortWindowProducer."""

    name = "long_window"
    system_element_name = "SE-Long"

    long_input = SignalRangeInput("on_metric_signal", window="2h")

    @on_metric("long_input")
    async def on_event(self, metric) -> None:
        pass


def test_horizon_is_the_largest_declared_window_across_producers_sharing_a_signal(runtime):
    """Two producers declare different windows on the SAME signal — the
    horizon must be the larger of the two, not whichever producer happened
    to be walked last."""
    instances = _instantiate(runtime, ShortWindowProducer, LongWindowProducer)
    horizons = compute_trim_horizons(instances, retention_s=1.0)
    assert horizons == {"sig-event": 2 * 3600.0}


def test_horizon_falls_back_to_broker_retention_when_it_exceeds_every_declared_window(runtime):
    """A short declared window must never trim tighter than the broker's
    own metrics retention — replay (§10) needs the full retained window."""
    instances = _instantiate(runtime, ShortWindowProducer)
    horizons = compute_trim_horizons(instances, retention_s=999_999.0)
    assert horizons == {"sig-event": 999_999.0}


def test_horizon_uses_the_declared_window_when_it_exceeds_retention(runtime):
    instances = _instantiate(runtime, ShortWindowProducer)
    horizons = compute_trim_horizons(instances, retention_s=1.0)
    assert horizons == {"sig-event": 10.0}


def test_horizon_skips_an_unresolved_input_rather_than_raising(runtime):
    instances = _instantiate(runtime, UnresolvableProducer)
    horizons = compute_trim_horizons(instances, retention_s=3600.0)  # must not raise
    assert horizons == {}


def test_trim_buffer_deletes_only_points_past_the_computed_horizon(buffer, runtime):
    """Wiring-level test (both directions in one test, matching
    test_dataops_buffer.py's Buffer.trim pin, but exercised through the
    actual path the service wires): a real producer's declared window flows
    through compute_trim_horizons into a trim_buffer call — a point older
    than the horizon is deleted, a point inside it survives. Plain function,
    like the job itself: sqlite work stays off the event loop."""
    now = time.time()
    buffer.append("sig-event", now - 20.0, "old")     # older than the 10s window
    buffer.append("sig-event", now - 1.0, "recent")    # inside the 10s window

    instances = _instantiate(runtime, ShortWindowProducer)

    # retention_s shorter than the declared window; trim_buffer computes
    # its own horizons from the live instances on every call.
    trim_buffer(buffer, instances, retention_s=1.0)

    df = buffer.window("sig-event", 0.0, now + 1.0)
    assert list(df["value"]) == ["recent"], "trim must delete the point past the horizon and keep the one inside it"


class LateResolvedProducer(Producer):
    """Declares an input for a signal that is NOT in KV yet at startup —
    resolves only after commissioning, like `UnresolvableProducer`, but
    reused here under a distinct signal name so the buffer content this
    test seeds is not shared with other tests in this module."""

    name = "late_resolved"
    system_element_name = "SE-Late"

    late_input = SignalRangeInput("late_signal", window="10s")

    @on_metric("late_input")
    async def on_event(self, metric) -> None:
        pass


def test_trim_buffer_recomputes_horizons_so_a_late_resolved_input_gets_trimmed(buffer, door, runtime):
    """Horizons used to be computed ONCE at startup, before `reresolve_loop`
    had resolved anything — a signal commissioned after the service started
    (the normal order: signals are bound
    after the service is already running) never appeared in the
    horizons dict, and `Buffer.trim` keeps every point for a signal absent
    from it — it grew without bound for the life of the volume. The fix
    recomputes horizons inside the trim job itself from the CURRENTLY
    resolved inputs, so a late resolve is picked up on the very next
    scheduled trim, no restart required."""
    instances = _instantiate(runtime, LateResolvedProducer)
    now = time.time()
    buffer.append("sig-late", now - 20.0, "old")  # older than the 10s window, once it resolves

    # At "startup" the signal is not commissioned in KV yet — unresolved,
    # so nothing can be trimmed for it.
    trim_buffer(buffer, instances, retention_s=1.0)
    df = buffer.window("sig-late", 0.0, now + 1.0)
    assert list(df["value"]) == ["old"], "an unresolved input must not be trimmed"

    # The signal is commissioned later: KV gains the _Signal entry (mirrors
    # a commissioning while the service keeps running).
    door.entries.append(signal_entry("sig-late", "late_signal"))

    trim_buffer(buffer, instances, retention_s=1.0)
    df = buffer.window("sig-late", 0.0, now + 1.0)
    assert list(df["value"]) == [], (
        "a late-resolved input must be trimmed on the very next trim run, without a restart"
    )


def test_the_doorbell_rings_for_a_metric_franzmq_cannot_decode():
    """franzmq decodes every inbound message before dispatch, on paho's
    network thread; a `_Metric` without `timestamp` raised inside that
    decode and killed the thread — the service went deaf with no error.
    The doorbell never reads the payload, so an undecodable message must
    still ring it, and a well-formed one must still take the typed path."""
    import json

    import franzmq
    from paho.mqtt.client import MQTTMessage

    from chaski.dataops.service import DOORBELL_WILDCARD, ring_even_if_undecodable

    client = franzmq.Client(client_id="doorbell-test")
    rings: list[str] = []
    client.message_callback_add(DOORBELL_WILDCARD, lambda c, u, m: rings.append(m.topic))
    ring_even_if_undecodable(client)

    def deliver(payload: dict) -> None:
        message = MQTTMessage(mid=1, topic=b"colca/v1/_Metric/n1/line1/temp")
        message.payload = json.dumps(payload).encode()
        client._handle_on_message(message)

    deliver({"v": 1.0, "value": 1.0, "signal_id": "s"})  # no timestamp: the decode raises
    deliver({"v": 2.0, "value": 2.0, "signal_id": "s", "timestamp": 1.0})  # decodes fine
    assert rings == ["colca/v1/_Metric/n1/line1/temp"] * 2, rings


def test_a_periodic_tick_is_scheduled_as_a_plain_function_not_a_coroutine():
    """What keeps producer ticks OFF the event loop: AsyncIOScheduler runs a
    coroutine job on the loop and a plain function in its thread-pool
    executor. `off_loop` turns a producer's (decoratively async) tick into
    the latter — and still runs the coroutine body, exceptions included.

    `off_loop` always wraps a bound Producer method in production (taking
    the producer's own lock around it — see the lock tests below), so this
    exercises it the same way rather than a bare function."""
    import inspect

    class _TickProducer(Producer):
        name = "off_loop_plain_function_test"
        system_element_name = "SE-OffLoop"

        def __init__(self):
            super().__init__()
            self.ran = []

        @every("10s")
        async def tick(self):
            self.ran.append(True)

        @every("10s")
        async def broken(self):
            raise RuntimeError("tick failed")

    inst = _TickProducer()

    job = off_loop(inst.tick)
    assert not inspect.iscoroutinefunction(job), (
        "off_loop must hand APScheduler a plain function — a coroutine job "
        "would run on the event loop and block every timer in the process"
    )
    job()
    assert inst.ran == [True]

    try:
        off_loop(inst.broken)()
    except RuntimeError as exc:
        assert "tick failed" in str(exc)
    else:
        raise AssertionError("a failing tick must propagate, not vanish")


# ─── timestamp fallback: colca's record.ts is milliseconds ───────────────


def _metric_record(payload: dict, ts: float) -> Record:
    return Record(
        offset=1, origin_offset=1, topic="colca/v1/_Metric/n-1/line1/x",
        payload=payload, ts=ts,
        written_by="connector", actor_id="svc-1", actor_label="connector", actor_kind="local",
    )


def test_decode_metric_falls_back_to_colca_record_ts_converted_to_seconds():
    """colca's record.ts is unix MILLISECONDS
    (`colca/internal/store/store.go`'s UnixMilli cutoff comparison); every
    timestamp handed to a producer's @on_metric handler is unix SECONDS. A
    payload with no `timestamp` field of its own used to decode into a
    Metric carrying the raw millisecond ts as if it were seconds — landing
    ~50,000 years in the future."""
    record = _metric_record({"signal_id": "sig-1", "value": 1.0}, ts=1_700_000_000_000.0)

    metric = decode_metric(record)

    assert metric.timestamp == pytest.approx(1_700_000_000.0)


def test_decode_metric_prefers_the_payloads_own_timestamp():
    """Denominator: the fallback must only kick in when the payload truly
    carries none of its own — an explicit payload timestamp must survive
    unconverted, proving the conversion isn't applied unconditionally."""
    record = _metric_record({"signal_id": "sig-1", "value": 1.0, "timestamp": 42.0}, ts=1_700_000_000_000.0)

    metric = decode_metric(record)

    assert metric.timestamp == 42.0


# ─── a failed pinned KV read must not drop already-resolved signals ────────


class FlakyKvDoor(FakeDoor):
    """Like FakeDoor (KV only), but can be told to refuse KV reads on
    demand — simulates a colca `/kv` 429 mid resolution pass."""

    def __init__(self, entries):
        super().__init__(entries)
        self.fail = False

    def kv(self, prefix="", *, contract=None):
        if self.fail:
            raise RuntimeError("429 Too Many Requests")
        return super().kv(prefix, contract=contract)


def test_a_failed_pinned_read_keeps_previously_resolved_ids_bound(buffer):
    """`forget_resolved` once ran unconditionally BEFORE the pass's own KV
    read, so a 429 on the pinned read (`resolve.one_pass`) meant every
    already-resolved input tried to re-resolve on its own — against a door
    still refusing requests — the fetch filter narrowed below what was
    actually bound, and `ingest.rebind` dropped already-flowing signals from
    the buffer while the cursor kept acking, silently. Only forget a
    resolved id once the pass's own KV read actually succeeded."""
    door = FlakyKvDoor([signal_entry("sig-event", "on_metric_signal")])
    runtime = FakeRuntime(door, buffer)

    instances = _instantiate(runtime, EventDrivenProducer)

    # First pass resolves normally — denominator: proves resolution works
    # before the failure is introduced.
    dispatch1, signal_ids1, unresolved1 = build_dispatch(runtime, instances)
    assert signal_ids1 == ["sig-event"]
    assert unresolved1 == 0

    # KV starts refusing on the next pass's pinned read.
    door.fail = True
    dispatch2, signal_ids2, unresolved2 = build_dispatch(runtime, instances)

    assert signal_ids2 == ["sig-event"], "an already-resolved signal must not be dropped by a failed pass"
    assert list(dispatch2.keys()) == ["sig-event"]


# ─── producer state shared across the ingest thread and the scheduler's
# thread pool must serialize on the producer's own lock ──────────────────


def test_handler_and_tick_on_the_same_producer_serialize_on_its_lock():
    """`@on_metric` handlers run on the ingest worker thread; `@every`/
    `@cron` ticks run in APScheduler's own thread pool — two real OS
    threads that can race on shared producer state with no lock (design §4:
    "handlers and ticks on one producer serialize on the producer's own
    lock"). Drives both through the exact wrappers the service hands to the
    ingest dispatch table (`make_handler`) and the scheduler (`off_loop`),
    on the SAME producer instance, and proves they cannot overlap.

    The producer overrides `__init__` for its own state and does NOT call
    `super().__init__()` — exactly what the level-4 `OvenWatch` fixture and
    user-written producers do. The lock must exist anyway (it is attached in
    `Producer.__new__`), or every dispatched metric on such a producer dies
    with `AttributeError: ... has no attribute '_lock'`."""

    class _LockedProducer(Producer):
        name = "locked_producer_test"
        system_element_name = "SE-Locked"

        def __init__(self):
            self.active = 0
            self.max_active = 0

        def _critical_section(self):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            time.sleep(0.1)
            self.active -= 1

        @on_metric("dummy_input")
        async def on_event(self, metric):
            self._critical_section()

        @every("10s")
        async def tick(self):
            self._critical_section()

    inst = _LockedProducer()
    handler = make_handler(inst.on_event)
    tick_job = off_loop(inst.tick)
    record = _metric_record({"signal_id": "s", "value": 1.0, "timestamp": 1.0}, ts=1000.0)

    barrier = threading.Barrier(2)

    def run_handler():
        barrier.wait()
        asyncio.run(handler(record))

    def run_tick():
        barrier.wait()
        tick_job()

    t1 = threading.Thread(target=run_handler)
    t2 = threading.Thread(target=run_tick)
    t1.start()
    t2.start()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    assert inst.max_active == 1, "handler and tick ran concurrently — the producer's lock did not serialize them"
