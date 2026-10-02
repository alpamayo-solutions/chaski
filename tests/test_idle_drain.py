"""A filtered consumer whose topics stay silent still walks its cursor.

The bell rings only for the records a consumer reads. Without the idle drain
its cursor stood still while the stream grew past it, and the node's pruner,
which never cuts below the lowest cursor, kept the whole stream (chaski#117).
``Stream.follow``, ``consume`` and the dataops ``Ingest`` drain after
``idle_drain_s`` without a ring; the drain acks the filtered pages it walked.
"""

from __future__ import annotations

import asyncio
import builtins
import threading
import time

from dataops_fakes import FakeDoor, run_async

from chaski import Doorbell
from chaski.consume import consume
from chaski.dataops.buffer import Buffer
from chaski.dataops.ingest import Ingest
from chaski.door import Page, Record, Stream
from chaski.failures import HandlerHealth


def _record(offset: int, signal_id: str) -> Record:
    return Record(
        offset=offset,
        origin_offset=offset,
        topic=f"colca/v1/_Metric/n-1/line1/{signal_id}",
        payload={"signal_id": signal_id, "value": offset, "timestamp": float(offset)},
        ts=float(offset),
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )


class FilteringDoor(FakeDoor):
    """A door with a real cursor and a ``signal_ids`` filter like colcad's:
    a fetch scans up to ``max`` records after the cursor, returns the matching
    ones and moves ``next`` past everything it scanned."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[Record] = []
        self.cursors: dict[str, int] = {}

    def append(self, count: int, signal_id: str) -> None:
        first = len(self.records) + 1
        self.records.extend(_record(offset, signal_id) for offset in range(first, first + count))

    def fetch(self, stream, cursor, *, max=1000, signal_ids=None, from_offset=None, tail=False, **_scope):
        if tail:
            return Page(records=[], next=len(self.records) + 1)
        self.fetch_calls.append({"cursor": cursor, "signal_ids": signal_ids, "from_offset": from_offset})
        start = self.cursors.get(cursor, 0) + 1
        if from_offset is not None:
            start = builtins.max(start, from_offset)
        scanned = [r for r in self.records if r.offset >= start][:max]
        matching = [r for r in scanned if signal_ids is None or r.payload["signal_id"] in signal_ids]
        return Page(records=matching, next=(scanned[-1].offset + 1) if scanned else start, start=start)

    def ack(self, stream, cursor, offset) -> bool:
        self.acked.append((stream, cursor, offset))
        moved = offset > self.cursors.get(cursor, 0)
        self.cursors[cursor] = builtins.max(self.cursors.get(cursor, 0), offset)
        return moved


def _wait(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.01)


def _follow_in_thread(stream: Stream, bell: Doorbell, stop: threading.Event, idle_drain_s):
    seen: list[int] = []

    def run() -> None:
        for record in stream.follow(bell, stop=stop, idle_drain_s=idle_drain_s):
            seen.append(record.offset)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    return worker, seen


def test_follow_walks_a_silent_filtered_cursor_to_the_head():
    door = FilteringDoor()
    stream = Stream(door, "metrics", "c/sparse", max=50, signal_ids=["sig-quiet"])
    stop, bell = threading.Event(), Doorbell()
    worker, seen = _follow_in_thread(stream, bell, stop, idle_drain_s=0.05)
    try:
        _wait(lambda: len(door.fetch_calls) >= 1)  # the drain at start
        door.append(120, "sig-busy")  # other signals: the bell stays silent
        _wait(lambda: door.cursors.get("c/sparse") == 120)
    finally:
        stop.set()
        worker.join(timeout=5)
    assert seen == []
    assert stream.position == 120


def test_follow_without_an_idle_drain_waits_for_the_bell_alone():
    door = FilteringDoor()
    stream = Stream(door, "metrics", "c/sparse", signal_ids=["sig-quiet"])
    stop, bell = threading.Event(), Doorbell()
    worker, _seen = _follow_in_thread(stream, bell, stop, idle_drain_s=None)
    try:
        _wait(lambda: len(door.fetch_calls) >= 1)
        door.append(10, "sig-busy")
        time.sleep(0.3)
        assert "c/sparse" not in door.cursors
        bell.ring()
        _wait(lambda: door.cursors.get("c/sparse") == 10)
    finally:
        stop.set()
        worker.join(timeout=5)


def test_consume_walks_a_silent_filtered_cursor_and_still_hands_out_its_records():
    door = FilteringDoor()
    stream = Stream(door, "metrics", "c/sparse", max=50, signal_ids=["sig-quiet"])
    stop, bell = threading.Event(), Doorbell()
    handled: list[int] = []
    worker = threading.Thread(
        target=consume,
        args=(stream, lambda record: handled.append(record.offset)),
        kwargs={
            "health": HandlerHealth(),
            "reject": lambda *_: None,
            "bell": bell,
            "stop": stop,
            "idle_drain_s": 0.05,
        },
        daemon=True,
    )
    worker.start()
    try:
        _wait(lambda: len(door.fetch_calls) >= 1)
        door.append(80, "sig-busy")
        door.append(1, "sig-quiet")  # one of its own, its ring lost
        door.append(40, "sig-busy")
        _wait(lambda: door.cursors.get("c/sparse") == 121)
    finally:
        stop.set()
        worker.join(timeout=5)
    assert handled == [81]


@run_async
async def test_ingest_walks_a_silent_filtered_cursor_without_a_wake(tmp_path):
    door = FilteringDoor()
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    ingest = Ingest(
        lambda cursor, signal_ids: Stream(door, "metrics", "c/" + cursor, max=50, signal_ids=signal_ids),
        buffer,
        signal_ids=["sig-quiet"],
        idle_drain_s=0.05,
    )
    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        deadline = time.monotonic() + 5
        while not door.fetch_calls:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.01)
        door.append(120, "sig-busy")
        while door.cursors.get(ingest.cursor) != 120:
            assert time.monotonic() < deadline, f"cursor stood at {door.cursors.get(ingest.cursor)}"
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2.0)
        buffer.close()
