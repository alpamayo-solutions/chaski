"""Pacing and read-ahead in ``Ingest.run_forever`` against a door that keeps
a real cursor.

After a full page the loop is behind: on a node that reads ahead (colca
0.18.2+, ``Page.start`` set) the next page is fetched while the full one is
processed, and acks go out in the background, still only for processed
records and only forward. On an older node the loop fetches, processes and
acks in turn, and never sends ``from``. After a partial page the next fetch
waits for the minimum fetch interval.
"""

from __future__ import annotations

import asyncio
import threading
import time

import httpx
from dataops_fakes import run_async

from chaski.dataops.buffer import Buffer
from chaski.dataops.ingest import Ingest
from chaski.door import Page, Record, Stream


def _record(offset: int) -> Record:
    return Record(
        offset=offset,
        origin_offset=offset,
        topic="colca/v1/_Metric/n-1/line1/x",
        payload={"signal_id": "sig-1", "value": float(offset), "timestamp": float(offset)},
        ts=float(offset) * 1000,
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )


class CursorDoor:
    """One stream of ``n`` records and a cursor like colcad's: ``fetch`` reads
    up to ``max`` records from the cursor, or from ``from_offset`` when that
    lies ahead of it; ``ack`` only moves forward. ``read_ahead=False`` is a node
    before 0.18.2: it ignores ``from`` and does not report ``start``.
    ``latency_s`` is spent in every fetch and ack."""

    def __init__(self, n: int, *, read_ahead: bool = True, latency_s: float = 0.0) -> None:
        self.records = [_record(i) for i in range(1, n + 1)]
        self.read_ahead = read_ahead
        self.latency_s = latency_s
        self.position = 1
        self.fetches: list[int | None] = []
        self.acks: list[int] = []
        self.fail_acks = 0
        self._lock = threading.Lock()

    def fetch(self, stream, cursor, *, max=1000, signal_ids=None, from_offset=None):
        time.sleep(self.latency_s)
        with self._lock:
            self.fetches.append(from_offset)
            start = self.position
            if self.read_ahead and from_offset is not None and from_offset > start:
                start = from_offset
            records = [r for r in self.records if r.offset >= start][:max]
            nxt = records[-1].offset + 1 if records else start
            return Page(records=records, next=nxt, start=start if self.read_ahead else None)

    def ack(self, stream, cursor, offset) -> bool:
        time.sleep(self.latency_s)
        with self._lock:
            if self.fail_acks > 0:
                self.fail_acks -= 1
                raise httpx.ConnectError("colca restarting")
            self.acks.append(offset)
            moved = offset + 1 > self.position
            self.position = max(self.position, offset + 1)
            return moved

    def delete_cursor(self, stream, cursor) -> None:
        pass


async def _run_until(ingest: Ingest, done, timeout: float = 5.0) -> None:
    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        deadline = asyncio.get_running_loop().time() + timeout
        while not done():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("condition not met in time")
            if task.done():
                task.result()
            await asyncio.sleep(0.005)
    finally:
        stop.set()
        ingest.wake()
        await asyncio.wait_for(task, timeout=5.0)


def _ingest(door, tmp_path, handler=None, *, page: int = 5, **kwargs) -> tuple[Ingest, Buffer]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    dispatch = {"sig-1": [handler]} if handler else {}

    def open_stream(cursor, signal_ids):
        return Stream(door, "metrics", "c/dataops/" + cursor, max=page, signal_ids=signal_ids)

    ingest = Ingest(open_stream, buffer, dispatch=dispatch, signal_ids=["sig-1"], **kwargs)
    return ingest, buffer


@run_async
async def test_the_next_page_is_fetched_while_the_current_one_is_processed(tmp_path):
    door = CursorDoor(10)
    seen: list[int] = []
    fetches_while_handling: list[int] = []

    async def handler(record):
        # The last record of the first page waits until the read-ahead fetch
        # for the second page has gone out.
        if record.offset == 5:
            for _ in range(200):
                if len(door.fetches) >= 2:
                    break
                await asyncio.sleep(0.005)
            fetches_while_handling.append(len(door.fetches))
        seen.append(record.offset)

    ingest, buffer = _ingest(door, tmp_path, handler, page=5)
    await _run_until(ingest, lambda: door.position == 11)
    buffer.close()

    assert seen == list(range(1, 11))
    assert fetches_while_handling == [2], "the second page was not fetched before the first was done"
    assert door.fetches[:2] == [None, 6]
    assert door.acks[-1] == 10
    assert door.acks == sorted(door.acks)


@run_async
async def test_an_ack_never_runs_ahead_of_processing(tmp_path):
    door = CursorDoor(20)
    processed: list[int] = []
    violations: list[tuple[int, int]] = []
    original_ack = door.ack

    def checked_ack(stream, cursor, offset):
        if processed and offset > processed[-1]:
            violations.append((offset, processed[-1]))
        return original_ack(stream, cursor, offset)

    door.ack = checked_ack

    async def handler(record):
        await asyncio.sleep(0.001)
        processed.append(record.offset)

    ingest, buffer = _ingest(door, tmp_path, handler, page=4)
    await _run_until(ingest, lambda: door.position == 21)
    buffer.close()

    assert processed == list(range(1, 21))
    assert violations == []


@run_async
async def test_an_older_node_is_read_from_the_cursor_and_acked_in_turn(tmp_path):
    door = CursorDoor(10, read_ahead=False)
    order: list[str] = []
    original_fetch, original_ack = door.fetch, door.ack

    def fetch(*args, **kwargs):
        order.append("fetch")
        return original_fetch(*args, **kwargs)

    def ack(*args, **kwargs):
        order.append("ack")
        return original_ack(*args, **kwargs)

    door.fetch, door.ack = fetch, ack
    ingest, buffer = _ingest(door, tmp_path, page=3)
    await _run_until(ingest, lambda: door.position == 11)
    buffer.close()

    assert set(door.fetches) == {None}, "a node without read-ahead must not be sent from="
    # Every page is acked before the next fetch.
    pages = "".join("F" if step == "fetch" else "A" for step in order)
    assert "FF" not in pages.rstrip("F")
    assert door.acks == [3, 6, 9, 10]


@run_async
async def test_a_failed_ack_restarts_the_read_at_the_acked_cursor(tmp_path):
    door = CursorDoor(8)
    door.fail_acks = 1
    seen: list[int] = []

    async def handler(record):
        seen.append(record.offset)

    ingest, buffer = _ingest(door, tmp_path, handler, page=4)
    await _run_until(ingest, lambda: door.position == 9)
    buffer.close()

    # The first page's ack failed; its records came again from the cursor
    # and were acked then. Nothing was skipped.
    assert set(seen) == set(range(1, 9))
    assert seen[:4] == [1, 2, 3, 4]
    assert None in door.fetches[1:], "the read did not restart at the cursor"
    assert door.acks[-1] == 8


@run_async
async def test_rebind_drops_the_page_read_ahead(tmp_path):
    door = CursorDoor(10)
    rebound = asyncio.Event()
    ingest_ref: list[Ingest] = []

    async def handler(record):
        if record.offset == 5 and not rebound.is_set():
            # The filter changes while page 2 is already read ahead.
            for _ in range(200):
                if len(door.fetches) >= 2:
                    break
                await asyncio.sleep(0.005)
            ingest_ref[0].rebind({"sig-1": [handler]}, ["sig-1"])
            rebound.set()

    ingest, buffer = _ingest(door, tmp_path, handler, page=5)
    ingest_ref.append(ingest)
    await _run_until(ingest, lambda: door.position == 11)
    buffer.close()

    # After the rebind the next read starts at the cursor, not at the page read ahead.
    assert door.fetches[:3] == [None, 6, None]


@run_async
async def test_stopping_waits_for_the_last_ack(tmp_path):
    door = CursorDoor(5, latency_s=0.05)
    ingest, buffer = _ingest(door, tmp_path, page=5)
    await _run_until(ingest, lambda: len(door.fetches) >= 1 and ingest._ack_task is not None)
    buffer.close()
    assert door.position == 6


class OverlapDoor(CursorDoor):
    """A ``CursorDoor`` that notes which fetches and acks were in flight while
    a record was handled. With read-ahead, each ack waits (briefly) for the
    handling of a later record, so an ack the loop awaited before going on
    would show as one that never overlapped processing."""

    def __init__(self, n: int, *, read_ahead: bool) -> None:
        super().__init__(n, read_ahead=read_ahead)
        self.in_flight: list[str] = []
        self.fetches_started = 0
        self.highest_handled = 0
        self.acks_overlapped: list[bool] = []

    def fetch(self, stream, cursor, **kwargs):
        with self._lock:
            self.fetches_started += 1
            self.in_flight.append("fetch")
        try:
            return super().fetch(stream, cursor, **kwargs)
        finally:
            with self._lock:
                self.in_flight.remove("fetch")

    def ack(self, stream, cursor, offset) -> bool:
        with self._lock:
            self.in_flight.append("ack")
        try:
            if self.read_ahead and offset < len(self.records):
                deadline = time.monotonic() + 2.0
                while self.highest_handled <= offset and time.monotonic() < deadline:
                    time.sleep(0.002)
                self.acks_overlapped.append(self.highest_handled > offset)
            return super().ack(stream, cursor, offset)
        finally:
            with self._lock:
                self.in_flight.remove("ack")


@run_async
async def test_read_ahead_drains_a_backlog_faster_when_fetch_ack_and_processing_take_time(tmp_path):
    """20 pages of 5. Read-ahead drains a backlog faster because the next fetch
    and the last ack run while a page is processed, where an older node takes
    the three in turn. Asserted on that overlap rather than on elapsed time,
    which a loaded runner stretches unevenly."""
    for read_ahead in (False, True):
        door = OverlapDoor(100, read_ahead=read_ahead)
        busy_while_handling: list[list[str]] = []
        next_fetch_in_time: list[bool] = []

        async def handler(record, door=door, busy=busy_while_handling, in_time=next_fetch_in_time):
            with door._lock:
                busy.append(list(door.in_flight))
            door.highest_handled = record.offset
            page = record.offset // 5
            if door.read_ahead and record.offset % 5 == 0 and page < 20:
                # The fetch of the next page has gone out before this page is done.
                for _ in range(400):
                    if door.fetches_started > page:
                        break
                    await asyncio.sleep(0.005)
                in_time.append(door.fetches_started > page)

        ingest, buffer = _ingest(door, tmp_path / str(read_ahead), None, page=5)
        ingest.rebind({"sig-1": [handler]}, ["sig-1"])
        await _run_until(ingest, lambda door=door: door.position == 101, timeout=30.0)
        buffer.close()

        assert len(busy_while_handling) == 100
        assert door.acks[-1] == 100
        assert door.acks == sorted(door.acks)
        if read_ahead:
            assert next_fetch_in_time == [True] * 19
            assert door.acks_overlapped, "no ack went out before the last page"
            assert all(door.acks_overlapped), "an ack held up processing"
        else:
            assert not any(busy_while_handling), "an older node must fetch, process and ack in turn"
            assert door.fetches_started >= 20


class FakeClock:
    """Application of the ingest's pacing to a clock the test owns: time moves
    only when the ingest sleeps to pace a fetch, or when the test says so."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += max(0.0, seconds)
        await _real_sleep(0)


_real_sleep = asyncio.sleep


class _PacedAsyncio:
    """The ``asyncio`` the ingest module sees: real, except ``sleep`` moves the fake clock."""

    def __init__(self, clock: FakeClock) -> None:
        self.sleep = clock.sleep

    def __getattr__(self, name):
        return getattr(asyncio, name)


class GrowingDoor(CursorDoor):
    """A stream that gains one record every 5 ms of the fake clock, as a live input does."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(0)
        self._clock = clock
        self._t0 = clock.now

    def fetch(self, stream, cursor, *, max=1000, signal_ids=None, from_offset=None):
        with self._lock:
            have = len(self.records)
            due = round((self._clock.now - self._t0) / 0.005)
            self.records.extend(_record(i) for i in range(have + 1, due + 1))
        return super().fetch(stream, cursor, max=max, signal_ids=signal_ids, from_offset=from_offset)


@run_async
async def test_drain_starts_are_batched_and_empty_head_reads_are_immediate(tmp_path, monkeypatch):
    """A live stream at 200 records/s, read with 1000-record pages: every page
    is partial, so fetches are spaced by the interval and pages grow instead.
    Every record rings the bell, as its MQTT message would.

    Time is the test's: records arrive and the pacing interval passes on a
    fake clock, so how much arrives while a page is processed does not depend
    on how fast the machine running the test is."""
    from chaski.dataops import ingest as ingest_module

    clock = FakeClock()
    monkeypatch.setattr(ingest_module, "time", clock)
    monkeypatch.setattr(ingest_module, "asyncio", _PacedAsyncio(clock))
    door = GrowingDoor(clock)
    ingest, buffer = _ingest(door, tmp_path, page=1000, min_fetch_interval_s=0.1)
    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))

    async def waiting_after(fetches: int) -> None:
        while not (ingest.waiting and len(door.fetches) > fetches):
            assert not task.done(), task
            await _real_sleep(0.001)

    await waiting_after(0)
    pages: list[int] = []
    while clock.now < door._t0 + 1.0:
        # One record arrives; its MQTT message rings the bell.
        before = len(door.fetches)
        clock.now += 0.005
        ingest.wake()
        await waiting_after(before)
        pages.append(len(door.fetches) - before)

    stop.set()
    ingest.wake()
    await asyncio.wait_for(task, timeout=5.0)
    buffer.close()

    # Each start waits out the interval, drains the nonempty page, then proves
    # the head with an immediate empty read: two fetches, no third.
    assert pages == [2] * len(pages), pages
    assert len(pages) == 10, pages
    # Every start reads 20 records (0.1 s at 200/s) from where the previous
    # empty head read ended, and the empty read follows from the page's end.
    starts = [1 + 20 * k for k in range(10)]
    assert door.fetches == [None, *(offset for start in starts for offset in (start, start + 20))], door.fetches
    assert door.position == 201
