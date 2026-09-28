"""Tests for chaski.dataops.ingest.Ingest against a fake Door.

Covered: processing in order, ack only after append and dispatch, gaps, the
doorbell, retiring the previous cursor, and reprocessing after a crash before
the ack. The loop reads through a real :class:`chaski.door.Stream`.

``@run_async`` runs each coroutine test with ``asyncio.run()``, so no
pytest-asyncio is needed.
"""

from __future__ import annotations

import asyncio
import logging

import httpx
import pytest
from dataops_fakes import FakeDoor, run_async, stream_opener

from chaski.dataops.buffer import Buffer
from chaski.dataops.ingest import Ingest, cursor_name
from chaski.door import Gap, Page, Record

CURSOR_PREFIX = "c/dataops/"


def _record(offset: int, signal_id: str, value, ts: float, topic: str = "colca/v1/_Metric/n-1/line1/x") -> Record:
    return Record(
        offset=offset,
        origin_offset=offset,
        topic=topic,
        payload={"signal_id": signal_id, "value": value, "timestamp": ts},
        ts=ts,
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )


def _ingest(door: FakeDoor, buffer: Buffer, **kwargs) -> Ingest:
    return Ingest(stream_opener(door, prefix=CURSOR_PREFIX), buffer, **kwargs)


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


@pytest.fixture
def door():
    return FakeDoor()


# ------------------------------------------------------------------ ordering + ack timing


@run_async
async def test_records_processed_in_order_and_acked_only_after(door, buffer):
    events: list[str] = []

    def make_handler(name):
        async def _h(record):
            events.append(f"handled:{name}:{record.payload['signal_id']}")

        return _h

    door.queue(Page(records=[_record(5, "sig-1", 1.0, 10.0), _record(6, "sig-2", 2.0, 20.0)], next=7))
    original_ack = door.ack

    def _tracking_ack(stream, cursor, offset):
        events.append("acked")
        return original_ack(stream, cursor, offset)

    door.ack = _tracking_ack

    ingest = _ingest(
        door,
        buffer,
        dispatch={
            "sig-1": [make_handler("h1")],
            "sig-2": [make_handler("h2")],
        },
        signal_ids=["sig-1", "sig-2"],
    )

    processed = await ingest.run_once()

    assert processed == 2
    # both records handled, IN STREAM ORDER, before the ack fires
    assert events == ["handled:h1:sig-1", "handled:h2:sig-2", "acked"]
    assert door.acked == [("metrics", ingest.cursor, 6)]


@run_async
async def test_handlers_run_on_the_services_loop_and_what_they_schedule_outlives_the_page(door, buffer):
    loop = asyncio.get_running_loop()
    seen: list[str] = []
    scheduled: list[asyncio.Future] = []

    async def handler(record):
        assert asyncio.get_running_loop() is loop
        seen.append("handled")

        async def later():
            await asyncio.sleep(0.01)
            seen.append("later")

        scheduled.append(asyncio.ensure_future(later()))

    door.queue(Page(records=[_record(1, "sig-1", 1.0, 10.0)], next=2))
    ingest = _ingest(door, buffer, dispatch={"sig-1": [handler]}, signal_ids=["sig-1"])

    await ingest.run_once()
    await asyncio.sleep(0.05)

    assert seen == ["handled", "later"]


@run_async
async def test_fetch_carries_service_signal_id_filter_inside_the_service_namespace(door, buffer):
    door.queue(Page(records=[], next=1))
    ingest = _ingest(door, buffer, signal_ids=["sig-1", "sig-2"])

    await ingest.run_once()

    assert door.fetch_calls == [
        {"stream": "metrics", "cursor": ingest.cursor, "max": 1000, "signal_ids": ["sig-1", "sig-2"]}
    ]
    assert ingest.cursor == CURSOR_PREFIX + cursor_name(buffer.generation), (
        "the generational cursor lives inside the service's own cursor namespace"
    )


@run_async
async def test_rebind_changes_the_filter_but_keeps_the_cursor(door, buffer):
    """A late-resolved signal widens the fetch filter; the cursor name — and
    with it the server-side position — stays exactly what it was."""
    door.queue(Page(records=[], next=1))
    door.queue(Page(records=[], next=1))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])
    await ingest.run_once()

    ingest.rebind({}, ["sig-1", "sig-late"])
    await ingest.run_once()

    assert [c["signal_ids"] for c in door.fetch_calls] == [["sig-1"], ["sig-1", "sig-late"]]
    assert {c["cursor"] for c in door.fetch_calls} == {ingest.cursor}


# ------------------------------------------------------------------ buffer append


@run_async
async def test_processing_appends_records_to_the_buffer(door, buffer):
    door.queue(Page(records=[_record(1, "sig-1", 42.5, 100.0)], next=2))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])

    await ingest.run_once()

    df = buffer.window("sig-1", 0.0, 1000.0)
    assert list(df["ts"]) == [100.0]
    assert list(df["value"]) == [42.5]


@run_async
async def test_a_record_with_no_payload_timestamp_buffers_colca_ts_converted_to_seconds(door, buffer):
    """record.ts is in milliseconds; a payload without its own timestamp is
    buffered at record.ts converted to seconds."""
    r = Record(
        offset=1,
        origin_offset=1,
        topic="colca/v1/_Metric/n-1/line1/x",
        payload={"signal_id": "sig-1", "value": 1.0},  # no "timestamp" field
        ts=1_700_000_000_000.0,  # milliseconds, as colca sends it
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )
    door.queue(Page(records=[r], next=2))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])

    await ingest.run_once()

    df = buffer.window("sig-1", 0.0, 2_000_000_000.0)
    assert df["ts"].tolist() == pytest.approx([1_700_000_000.0]), (
        "a payload with no timestamp must buffer record.ts CONVERTED to seconds, not the raw colca milliseconds"
    )


# ------------------------------------------------------------------ gaps


@pytest.mark.parametrize("records", [[], [_record(100, "sig-1", 1.0, 200.0)]])
@run_async
async def test_gap_blocks_effects_and_acknowledgement(door, buffer, records):
    from chaski import StreamGapError

    gap = Gap(stream="metrics", from_offset=1, to_offset=99, first_ts=10.0, last_ts=90.0, approx=True)
    door.queue(Page(records=records, next=101, gap=gap))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])
    with pytest.raises(StreamGapError, match="retention gap"):
        await ingest.run_once()
    assert len(buffer.window("sig-1", 0.0, 1000.0)) == 0
    assert door.acked == []


@run_async
async def test_a_page_of_other_signals_moves_the_cursor_past_them(door, buffer):
    """Load test round 2: a filtered ingest cursor stood up to 5,000 records and
    65 s behind the stream while its service was caught up."""
    door.queue(Page(records=[], next=5001, start=1))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])

    processed = await ingest.run_once()

    assert processed == 0
    assert door.acked == [("metrics", ingest.cursor, 5000)]


@run_async
async def test_empty_page_with_no_gap_does_not_ack(door, buffer):
    door.queue(Page(records=[], next=1))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])

    processed = await ingest.run_once()

    assert processed == 0
    assert door.acked == []


# ------------------------------------------------------------------ dispatch


@run_async
async def test_on_metric_handlers_fire_only_for_matching_signal(door, buffer):
    calls: list[Record] = []

    async def handler(record):
        calls.append(record)

    r1 = _record(1, "sig-1", 1.0, 10.0)
    r2 = _record(2, "sig-unmapped", 2.0, 20.0)
    door.queue(Page(records=[r1, r2], next=3))
    ingest = _ingest(door, buffer, dispatch={"sig-1": [handler]}, signal_ids=["sig-1", "sig-unmapped"])

    await ingest.run_once()

    # presence: the matching signal fired exactly once, with the right record —
    # and the denominator proves the unmapped signal did NOT also trigger it.
    assert calls == [r1]


@run_async
async def test_multiple_handlers_fire_for_the_same_record(door, buffer):
    calls: list[str] = []

    async def h1(record):
        calls.append("h1")

    async def h2(record):
        calls.append("h2")

    door.queue(Page(records=[_record(1, "sig-1", 1.0, 10.0)], next=2))
    ingest = _ingest(door, buffer, dispatch={"sig-1": [h1, h2]}, signal_ids=["sig-1"])

    await ingest.run_once()

    assert calls == ["h1", "h2"]


@run_async
async def test_handler_invocations_never_overlap(door, buffer):
    """Ingest dispatches one record at a time, in stream order — proving
    handlers for the same (or different) producers never run concurrently,
    which is what makes @on_metric handlers deterministic under replay."""
    active = 0
    max_active = 0

    async def handler(record):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1

    door.queue(
        Page(
            records=[_record(1, "sig-1", 1.0, 10.0), _record(2, "sig-1", 2.0, 20.0)],
            next=3,
        )
    )
    ingest = _ingest(door, buffer, dispatch={"sig-1": [handler]}, signal_ids=["sig-1"])

    await ingest.run_once()

    assert max_active == 1


@run_async
async def test_a_failing_handler_leaves_the_page_unacknowledged(door, buffer):
    calls = []

    async def broken(record):
        raise RuntimeError("boom")

    async def fine(record):
        calls.append("fine")

    door.queue(Page(records=[_record(1, "sig-1", 1.0, 10.0)], next=2))
    ingest = _ingest(door, buffer, dispatch={"sig-1": [broken, fine]}, signal_ids=["sig-1"])
    with pytest.raises(RuntimeError, match="boom"):
        await ingest.run_once()
    assert calls == []
    assert door.acked == []


def test_skip_failed_handlers_is_rejected(door, buffer):
    with pytest.raises(ValueError, match="strict=False"):
        _ingest(door, buffer, strict=False)


# ------------------------------------------------------------------ doorbell


async def _poll_until(predicate, timeout: float = 2.0, interval: float = 0.01) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


@run_async
async def test_wake_triggers_an_immediate_run_once_without_waiting_for_retry_min(door, buffer):
    ingest = _ingest(door, buffer, signal_ids=["sig-1"], retry_min_s=60.0)

    def calls() -> int:
        return len(door.fetch_calls)

    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        await _poll_until(lambda: calls() >= 1)  # the drain at start
        await asyncio.sleep(0.3)
        assert calls() == 1, "an idle ingest must not fetch on a timer"

        ingest.wake()
        await _poll_until(lambda: calls() == 2)
        await asyncio.sleep(0.2)
        assert calls() == 2
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)


@run_async
async def test_a_wake_during_a_drain_is_not_lost(buffer):
    """The generation is taken before the fetch: a wake that lands while the
    page is read leads to one more drain, although the page came back empty."""

    class RingingDoor(FakeDoor):
        ingest: Ingest | None = None

        def fetch(self, stream, cursor, *, max=1000, signal_ids=None, **_):
            page = super().fetch(stream, cursor, max=max, signal_ids=signal_ids)
            if len(self.fetch_calls) == 1 and self.ingest is not None:
                self.ingest.wake()  # a record arrived while this page was read
            return page

    door = RingingDoor()
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])
    door.ingest = ingest

    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        await _poll_until(lambda: len(door.fetch_calls) >= 2)
        await asyncio.sleep(0.2)
        assert len(door.fetch_calls) == 2, "one wake, one more drain"
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)


@run_async
async def test_a_page_of_skipped_records_is_acked_and_read_on(door, buffer):
    """An empty page whose read moved on (the filter skipped every record)
    is acked past them, so they do not wait on the cursor, and the next page
    is read at once instead of taking the empty one for the head."""
    door.queue(Page(records=[], next=41, start=1))
    door.queue(Page(records=[_record(41, "sig-1", 1.0, 10.0)], next=42, start=41))
    door.queue(Page(records=[], next=42, start=42))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])

    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        await _poll_until(lambda: len(door.fetch_calls) >= 3)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)
    assert [offset for _s, _c, offset in door.acked] == [40, 41]
    assert len(buffer.window("sig-1", 0.0, 100.0)) == 1


# ------------------------------------------------------------------ transient transport-error recovery


class FlakyDoor(FakeDoor):
    """Like FakeDoor, but raises httpx.ConnectError on the first
    ``fail_times`` fetches (as if colca were restarting), then behaves
    normally."""

    def __init__(self, fail_times: int = 1) -> None:
        super().__init__()
        self._fail_remaining = fail_times

    def fetch(self, stream, cursor, *, max=1000, signal_ids=None):
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            self.fetch_calls.append({"stream": stream, "cursor": cursor, "max": max, "signal_ids": signal_ids})
            raise httpx.ConnectError("colca restarting")
        return super().fetch(stream, cursor, max=max, signal_ids=signal_ids)


@run_async
async def test_run_forever_survives_one_transient_transport_error_and_resumes_fetching(buffer, monkeypatch):
    """A transient transport error does not end the loop; it retries and
    fetches again."""
    monkeypatch.setattr(Ingest, "ERROR_BACKOFF_S", 0.02)
    door = FlakyDoor(fail_times=1)
    ingest = _ingest(door, buffer, signal_ids=["sig-1"], retry_min_s=0.02)

    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        await _poll_until(lambda: len(door.fetch_calls) >= 2, timeout=2.0)
    finally:
        stop.set()
        ingest.wake()
        await asyncio.wait_for(task, timeout=2.0)

    assert task.exception() is None, "the loop must not die on a transient transport error"
    assert len(door.fetch_calls) >= 2, "the loop must retry the fetch after the transient error"


@run_async
async def test_only_a_finished_drain_counts_as_progress(buffer, monkeypatch):
    """Retrying a transport error is not progress."""
    monkeypatch.setattr(Ingest, "ERROR_BACKOFF_S", 0.02)
    door = FlakyDoor(fail_times=1_000_000)
    ingest = _ingest(door, buffer, signal_ids=["sig-1"], retry_min_s=0.02)
    built = ingest.last_drain_at

    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        await _poll_until(lambda: len(door.fetch_calls) >= 2, timeout=2.0)
        assert ingest.last_drain_at == built
        door._fail_remaining = 0
        await _poll_until(lambda: ingest.last_drain_at > built, timeout=5.0)
    finally:
        stop.set()
        ingest.wake()
        await asyncio.wait_for(task, timeout=2.0)


@run_async
async def test_run_forever_still_dies_on_a_non_transport_error(buffer):
    """Only httpx.HTTPError is retried; any other exception still ends the task."""

    class BrokenDoor(FakeDoor):
        def fetch(self, stream, cursor, *, max=1000, signal_ids=None):
            raise RuntimeError("not a transport error")

    ingest = _ingest(BrokenDoor(), buffer, signal_ids=["sig-1"], retry_min_s=0.02)

    with pytest.raises(RuntimeError, match="not a transport error"):
        await asyncio.wait_for(ingest.run_forever(asyncio.Event()), timeout=2.0)


@run_async
async def test_ingest_honors_retry_after_without_acknowledging(door, buffer):
    import httpx

    ingest = _ingest(door, buffer, signal_ids=["sig-1"])
    stop = asyncio.Event()
    request = httpx.Request("GET", "http://node/fetch")
    response = httpx.Response(429, headers={"Retry-After": "45"}, request=request)

    async def refused():
        raise httpx.HTTPStatusError("limited", request=request, response=response)

    delays = []

    async def wait(delay, event):
        delays.append(delay)
        event.set()

    ingest._step = refused
    ingest._sleep_or_stop = wait
    await ingest.run_forever(stop)
    assert len(delays) == 1 and 45 <= delays[0] <= 54


# ------------------------------------------------------------------ generational cursor retirement


@run_async
async def test_retire_previous_generation_deletes_only_the_named_prior_cursor(door, buffer):
    # no previous generation known → nothing to delete
    ingest_fresh = _ingest(door, buffer, signal_ids=["sig-1"])
    ingest_fresh.retire_previous_generation()
    assert door.deleted == []

    # a previous generation IS known → exactly that cursor is deleted, and
    # never the current one
    door2 = FakeDoor()
    ingest = _ingest(door2, buffer, signal_ids=["sig-1"], previous_generation="01OLDGENERATIONULID")
    ingest.retire_previous_generation()

    assert door2.deleted == [("metrics", CURSOR_PREFIX + cursor_name("01OLDGENERATIONULID"))]
    assert ingest.cursor not in [c for _, c in door2.deleted]


# ------------------------------------------------------------------ crash recovery / reprocessing


@run_async
async def test_reprocessing_a_page_after_a_crash_does_not_duplicate_buffer_rows(door, buffer):
    """Simulates a crash between processing and ack: the SAME page is
    fetched twice (the un-acked cursor position never moved). Buffer
    ``append`` is INSERT OR REPLACE on (signal_id, ts), so reprocessing
    must leave exactly one row, not two."""
    handled: list[float] = []

    async def handler(record):
        handled.append(record.payload["value"])

    page = Page(records=[_record(5, "sig-1", 1.0, 10.0)], next=6)
    door.queue(page)
    door.queue(page)  # same page delivered again, as if the ack never landed

    ingest = _ingest(door, buffer, dispatch={"sig-1": [handler]}, signal_ids=["sig-1"])

    await ingest.run_once()
    await ingest.run_once()

    # dispatch re-ran both times (handlers are expected to be idempotent
    # themselves; ingest does not dedup)...
    assert handled == [1.0, 1.0]
    # ...but the buffer row is not duplicated: exactly one point survives.
    df = buffer.window("sig-1", 0.0, 100.0)
    assert len(df) == 1
    assert df["value"].iloc[0] == 1.0


@run_async
async def test_a_drain_runs_off_the_loop_so_timers_keep_firing(buffer):
    """A drain whose fetch blocks for 0.3 s runs against a 10 ms ticker on the
    same loop; the ticker keeps counting only if the drain is off the loop."""
    import time as time_mod

    class SlowDoor(FakeDoor):
        def fetch(self, stream, cursor, *, max=1000, signal_ids=None):
            time_mod.sleep(0.3)  # a synchronous door call, as in production
            return super().fetch(stream, cursor, max=max, signal_ids=signal_ids)

    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.01)

    ingest = _ingest(SlowDoor(), buffer, signal_ids=["sig-1"])
    ticker_task = asyncio.ensure_future(ticker())
    try:
        await ingest.run_once()
    finally:
        ticker_task.cancel()
    assert ticks >= 10, (
        f"the loop ticked only {ticks}x during a 0.3s drain — the drain is "
        "blocking the event loop instead of running in a worker thread"
    )


def test_the_rollup_says_what_a_window_ingested_and_that_an_empty_one_ingested_nothing(door, buffer, caplog):
    """One INFO line per 60 s window with records, drains and cursor, also when
    the window was empty."""
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])
    # The window opens at construction on the real monotonic clock; anchor it
    # to the synthetic timeline the test drives.
    ingest._window_started = 0.0

    with caplog.at_level(logging.INFO, logger="chaski.dataops.ingest"):
        ingest._note_drain(40, now=0.0)
        ingest._note_drain(2, now=30.0)
        assert not caplog.records, "the window must not log before it closes"
        ingest._note_drain(0, now=61.0)

    assert len(caplog.records) == 1
    line = caplog.records[0].getMessage()
    assert "Ingested 42 records over 3 drains" in line, line
    assert ingest.cursor in line

    with caplog.at_level(logging.INFO, logger="chaski.dataops.ingest"):
        caplog.clear()
        ingest._note_drain(0, now=123.0)

    assert len(caplog.records) == 1
    assert "Ingested 0 records over 1 drains" in caplog.records[0].getMessage()


@run_async
async def test_hint_during_empty_fetch_is_not_lost(door, buffer):
    ingest = _ingest(door, buffer, signal_ids=["sig-1"], retry_min_s=60)
    stop = asyncio.Event()
    calls = []

    async def fetch():
        calls.append(1)
        if len(calls) == 1:
            ingest.wake()  # commit races the empty fetch response
        else:
            stop.set()
        return 0

    ingest._step = fetch
    await asyncio.wait_for(ingest.run_forever(stop), timeout=1)
    assert len(calls) == 2


@run_async
async def test_adjacent_input_only_records_batch_without_future_visibility(door, buffer):
    commits = []
    buffer._conn.set_trace_callback(lambda sql: commits.append(sql) if sql == "COMMIT" else None)
    seen = []

    async def handler(record):
        seen.append(buffer.window("input", 0, 100)["value"].tolist())

    door.queue(
        Page(
            records=[
                _record(1, "input", 1, 1),
                _record(2, "input", 2, 2),
                _record(3, "trigger", 0, 3),
                _record(4, "input", 4, 4),
                _record(5, "input", 5, 5),
            ],
            next=6,
        )
    )
    ingest = _ingest(door, buffer, dispatch={"trigger": [handler]})
    await ingest.run_once()
    assert seen == [[1, 2]]
    assert buffer.window("input", 0, 100)["value"].tolist() == [1, 2, 4, 5]
    assert len(commits) == 1


@run_async
async def test_coordinated_inbox_orders_pages_keeps_future_and_survives_restart(door, tmp_path):
    import threading
    from types import SimpleNamespace

    from colca_data_contracts.payload import ClockDefinition

    from chaski.clock import Clock
    from chaski.dataops.scheduling import run_due
    from chaski.dataops.triggers import IntervalSpec

    clock = Clock(wall=lambda: 10010)
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1100, start_at=1000))
    path = tmp_path / "ordered.db"
    buffer = Buffer(path)
    events = []

    class Scheduled:
        name = "scheduled"
        _lock = threading.RLock()
        _triggers = (("tick", IntervalSpec(5)),)
        runtime = SimpleNamespace(buffer=buffer)

        async def tick(self):
            row = self.runtime.buffer.latest_before("s", clock.now())
            events.append(("tick", clock.now(), row[1] if row else None))

    scheduled = Scheduled()

    async def handler(record):
        events.append(("metric", record.payload["timestamp"], record.payload["value"]))

    async def before(at):
        await run_due([scheduled], clock, at, inclusive=False)

    async def finish(at):
        await run_due([scheduled], clock, at, inclusive=True)

    # Stream order spans two producers/pages; event-time order differs.
    future = _record(1, "s", 30, 1030)
    near = _record(2, "s", 10, 1010)
    middle = _record(3, "s", 20, 1020)
    door.queue(Page([future, near], 3))
    door.queue(Page([middle], 4))
    ingest = _ingest(door, buffer, dispatch={"s": [handler]}, strict=True)
    await ingest.run_window(1020, before, finish)
    assert events == [
        ("tick", 1005, None),
        ("metric", 1010, 10),
        ("tick", 1010, 10),
        ("tick", 1015, 10),
        ("metric", 1020, 20),
        ("tick", 1020, 20),
    ]
    assert len(buffer.input_batch(1040)) == 1
    buffer.close()
    # Lost ack repeats an intake page after restart. Completed callbacks do not
    # repeat, and the future sample remains available despite broker ack.
    buffer = Buffer(path)
    scheduled.runtime.buffer = buffer
    door.queue(Page([future, near, middle], 4))
    ingest = _ingest(door, buffer, dispatch={"s": [handler]}, strict=True)
    await ingest.run_window(1030, before, finish)
    assert events[-3:] == [("tick", 1025, 20), ("metric", 1030, 30), ("tick", 1030, 30)]
    assert len([e for e in events if e[0] == "metric"]) == 3
    assert not buffer.input_batch(1040)
    buffer.close()


@run_async
async def test_coordinated_inbox_retries_failed_effect_and_bounds_queue(door, buffer):
    failures = [True]

    async def handler(record):
        if failures[0]:
            raise RuntimeError("effect unavailable")

    async def noop(at):
        pass

    record = _record(1, "s", 1, 10)
    door.queue(Page([record], 2))
    ingest = _ingest(door, buffer, dispatch={"s": [handler]}, strict=True)
    with pytest.raises(RuntimeError, match="effect unavailable"):
        await ingest.run_window(10, noop, noop)
    assert len(buffer.input_batch(10)) == 1
    with pytest.raises(BufferError):
        buffer.queue_inputs([_record(2, "s", 2, 20)], 2, limit=1)
    failures[0] = False
    await ingest.run_window(10, noop, noop)
    assert not buffer.input_batch(10)


@run_async
async def test_coordinated_inbox_keeps_real_health_out_of_factory_schedule(door, buffer):
    from colca_data_contracts.payload import ClockDefinition

    from chaski.clock import Clock

    clock = Clock(wall=lambda: 10010)
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1100, start_at=1000))
    events = []

    async def machine(record):
        assert buffer.latest_before("health", 10010)[1] is True
        events.append(("machine", record.payload["timestamp"]))

    async def health(record):
        events.append(("health", clock.now()))

    async def before(at):
        events.append(("before", at))

    async def finish(at):
        events.append(("finish", at))

    door.queue(Page([_record(1, "health", True, 10010), _record(2, "s", 10, 1010)], 3))
    ingest = _ingest(door, buffer, dispatch={"s": [machine], "health": [health]}, strict=True)
    await ingest.run_window(1010, before, finish, real_signals={"health"}, clock=clock)
    assert events == [("before", 1010), ("machine", 1010), ("health", 1010), ("finish", 1010)]
    assert not buffer.input_batch(20000)


@run_async
async def test_window_completes_with_continuous_input(buffer):
    class LiveDoor(FakeDoor):
        position = 0

        def fetch(self, stream, cursor, *, tail=False, **kwargs):
            if tail:
                return Page([], 4)  # Three admitted records at the boundary.
            self.position += 1
            assert self.position <= 3, "must not chase the moving stream head"
            return Page([_record(self.position, "s", self.position, 10)], self.position + 1)

    door = LiveDoor()
    events = []

    async def handle(record):
        events.append(record.offset)

    async def before(at):
        pass

    async def finish(at):
        assert len(door.acked) == 3
        events.append("complete")

    ingest = _ingest(door, buffer, dispatch={"s": [handle]}, strict=True)
    await ingest.run_window(10, before, finish)
    assert events == [1, 2, 3, "complete"]
