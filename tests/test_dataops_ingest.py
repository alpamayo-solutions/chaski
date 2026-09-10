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


@run_async
async def test_gap_is_logged_and_processing_continues(door, buffer, caplog):
    gap = Gap(stream="metrics", from_offset=1, to_offset=99, first_ts=10.0, last_ts=90.0, approx=True)
    door.queue(Page(records=[_record(100, "sig-1", 1.0, 200.0)], next=101, gap=gap))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])

    with caplog.at_level(logging.WARNING, logger="chaski.dataops.ingest"):
        processed = await ingest.run_once()

    assert processed == 1
    gap_messages = [r.message for r in caplog.records if "Gap on stream" in r.message]
    assert len(gap_messages) == 1
    assert "1" in gap_messages[0] and "99" in gap_messages[0]
    # processing continued from the LWM: the record after the gap was buffered...
    assert len(buffer.window("sig-1", 0.0, 1000.0)) == 1
    # ...and the page's own last record offset is what got acked, not the gap.
    assert door.acked == [("metrics", ingest.cursor, 100)]


@run_async
async def test_gap_only_page_acks_the_gap_bound_to_clear_it(door, buffer, caplog):
    gap = Gap(stream="metrics", from_offset=1, to_offset=499, first_ts=None, last_ts=None, approx=True)
    door.queue(Page(records=[], next=500, gap=gap))
    ingest = _ingest(door, buffer, signal_ids=["sig-1"])

    with caplog.at_level(logging.WARNING, logger="chaski.dataops.ingest"):
        processed = await ingest.run_once()

    assert processed == 0
    assert door.acked == [("metrics", ingest.cursor, 499)]


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
async def test_a_failing_handler_does_not_stop_the_page_or_the_ack(door, buffer, caplog):
    """One broken @on_metric handler must not sink the whole page: the
    record still lands in the buffer, the remaining handler still runs,
    and the page still gets acked."""
    calls: list[str] = []

    async def broken(record):
        raise RuntimeError("boom")

    async def fine(record):
        calls.append("fine")

    door.queue(Page(records=[_record(1, "sig-1", 1.0, 10.0)], next=2))
    ingest = _ingest(door, buffer, dispatch={"sig-1": [broken, fine]}, signal_ids=["sig-1"])

    with caplog.at_level(logging.ERROR, logger="chaski.dataops.ingest"):
        processed = await ingest.run_once()

    assert processed == 1
    assert calls == ["fine"]
    assert len(buffer.window("sig-1", 0.0, 100.0)) == 1
    assert door.acked == [("metrics", ingest.cursor, 1)]
    assert any("on_metric handler failed" in r.message for r in caplog.records)


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
async def test_wake_triggers_an_immediate_run_once_without_waiting_for_poll_interval(door, buffer):
    calls = 0
    ingest = _ingest(door, buffer, signal_ids=["sig-1"], poll_interval_s=60.0)

    original_run_once = ingest.run_once

    async def _spy():
        nonlocal calls
        calls += 1
        return await original_run_once()

    ingest.run_once = _spy

    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        # the loop calls run_once immediately on entry, before ever sleeping
        await _poll_until(lambda: calls >= 1)
        first_count = calls

        ingest.wake()

        # with a 60s poll interval, a second call within 2s can only be the
        # doorbell short-circuiting the sleep, not the timeout firing
        await _poll_until(lambda: calls >= first_count + 1)
    finally:
        stop.set()
        ingest.wake()
        await asyncio.wait_for(task, timeout=2.0)


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
async def test_run_forever_survives_one_transient_transport_error_and_resumes_fetching(buffer):
    """A transient transport error does not end the loop; it retries and
    fetches again."""
    door = FlakyDoor(fail_times=1)
    ingest = _ingest(door, buffer, signal_ids=["sig-1"], poll_interval_s=0.02)

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
async def test_run_forever_still_dies_on_a_non_transport_error(buffer):
    """Only httpx.HTTPError is retried; any other exception still ends the task."""

    class BrokenDoor(FakeDoor):
        def fetch(self, stream, cursor, *, max=1000, signal_ids=None):
            raise RuntimeError("not a transport error")

    ingest = _ingest(BrokenDoor(), buffer, signal_ids=["sig-1"], poll_interval_s=0.02)

    with pytest.raises(RuntimeError, match="not a transport error"):
        await asyncio.wait_for(ingest.run_forever(asyncio.Event()), timeout=2.0)


def test_error_backoff_grows_with_consecutive_attempts_and_is_bounded(door, buffer):
    """The backoff grows with each attempt, with jitter, up to a bound."""
    ingest = _ingest(door, buffer, signal_ids=["sig-1"], poll_interval_s=1.0)

    first = ingest._error_backoff_s(1)
    second = ingest._error_backoff_s(2)
    many = ingest._error_backoff_s(10)

    assert 1.0 <= first <= 1.2
    assert 2.0 <= second <= 2.4
    assert Ingest.ERROR_BACKOFF_MAX_S <= many <= Ingest.ERROR_BACKOFF_MAX_S * 1.2


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
