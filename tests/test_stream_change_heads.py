"""A stream-change hint carries the stream's head. Consumers woken by it stop
their drain at that head without reading the tail first, and a hint their
cursor already passed costs no request. Coalesced hints keep the newest head;
a reconnect is drained once whatever its head says."""

from __future__ import annotations

import asyncio
import queue
import threading
import time
from types import SimpleNamespace

import httpx

from chaski.consume import consume
from chaski.door import Hint, Page, Stream
from chaski.executor import CommandExecutor
from chaski.failures import HandlerHealth
from chaski.retained_view import RetainedView, ViewScope
from chaski.stream_changes import StreamChange, StreamChanges

TOPIC = "colca/v1/_Metric/node/temperature"
DISCONNECT = object()


class Door:
    """One stream of records behind a cursor, and a /watch fed from a queue.

    ``hint()`` sends a hint naming the stream with its current next offset;
    ``disconnect()`` ends the watch connection with a transport error.
    Every ``fetch`` is logged, tail reads separately.
    """

    def __init__(self, stream: str = "metrics") -> None:
        self.stream = stream
        self.records: list[SimpleNamespace] = []
        self.cursor = 0
        self.fetches: list[str] = []
        self.hints: queue.Queue = queue.Queue()
        self.connections = 0
        self.lock = threading.Lock()

    def put(self, value) -> None:
        with self.lock:
            offset = len(self.records) + 1
            self.records.append(SimpleNamespace(topic=TOPIC, payload=value, offset=offset, ts=0, origin_offset=offset))

    def hint(self) -> None:
        self.hints.put(Hint(streams=[self.stream], next={self.stream: len(self.records) + 1}))

    def disconnect(self) -> None:
        self.hints.put(DISCONNECT)

    def watch(self, streams, *, stop, contracts=(), interval_ms=None):
        self.connections += 1
        # A new connection names every stream first, as colca does.
        yield Hint(streams=list(streams), next={s: len(self.records) + 1 for s in streams})
        while not stop():
            try:
                hint = self.hints.get(timeout=0.01)
            except queue.Empty:
                continue
            if hint is DISCONNECT:
                raise httpx.ReadError("connection lost")
            yield hint

    def fetch(self, stream, cursor, *, max=1000, tail=False, from_offset=None, **_scope):
        with self.lock:
            if tail:
                self.fetches.append("tail")
                return Page(self.records[-1:], len(self.records) + 1)
            self.fetches.append("page")
            rows = self.records[self.cursor : self.cursor + max]
            nxt = rows[-1].offset + 1 if rows else self.cursor + 1
            return Page(rows, nxt, start=self.cursor + 1)

    def ack(self, stream, cursor, offset) -> bool:
        with self.lock:
            moved = offset > self.cursor
            self.cursor = max(self.cursor, offset)
            return moved


def _until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.005)


def _quiet(door: Door, seconds: float = 0.3) -> list[str]:
    """The fetches made while nothing happens for ``seconds``."""
    before = len(door.fetches)
    time.sleep(seconds)
    return door.fetches[before:]


# ---------------------------------------------------------------- the event


def test_a_hint_records_the_head_before_ringing_and_hands_it_to_on_change():
    door = Door()
    for value in range(3):
        door.put(value)
    seen: list[tuple[StreamChange, ...]] = []
    at_ring: list[StreamChange | None] = []
    watch = StreamChanges(door, ["metrics"], on_change=seen.append)
    signal = watch["metrics"]
    version = signal.version

    def observe():
        signal.wait(version, 5)
        at_ring.append(watch.latest("metrics"))

    observer = threading.Thread(target=observe)
    observer.start()
    watch.start()
    try:
        observer.join(5)
        assert at_ring == [StreamChange("metrics", 3, 1)]
        _until(lambda: seen)
        assert seen[0] == (StreamChange("metrics", 3, 1),)
    finally:
        watch.close()


def test_coalesced_hints_keep_the_newest_head():
    door = Door()
    watch = StreamChanges(door, ["metrics"]).start()
    try:
        _until(lambda: watch.latest("metrics") is not None)
        version = watch["metrics"].version
        for value in range(5):
            door.put(value)
            door.hint()
        _until(lambda: watch.latest("metrics").head == 5)
        # Five hints, one wake for a consumer that captured the version before them.
        assert watch["metrics"].wait(version, 1)
        assert watch.latest("metrics") == StreamChange("metrics", 5, 1)
    finally:
        watch.close()


def test_a_hint_without_an_offset_has_no_head():
    class OldDoor(Door):
        def watch(self, streams, *, stop, contracts=(), interval_ms=None):
            yield SimpleNamespace(streams=list(streams))
            while not stop():
                time.sleep(0.01)

    watch = StreamChanges(OldDoor(), ["metrics"]).start()
    try:
        _until(lambda: watch.latest("metrics") is not None)
        assert watch.latest("metrics") == StreamChange("metrics", None, 1)
        assert not watch.latest("metrics").covers(10, 1)
    finally:
        watch.close()


def test_a_reconnect_starts_a_new_subscription():
    door = Door()
    door.put(1)
    watch = StreamChanges(door, ["metrics"]).start()
    try:
        _until(lambda: watch.latest("metrics") is not None)
        assert watch.latest("metrics") == StreamChange("metrics", 1, 1)
        reconnected = watch.reconnected.version
        door.disconnect()
        assert watch.reconnected.wait(reconnected, 5)
        assert watch.latest("metrics") == StreamChange("metrics", 1, 2)
    finally:
        watch.close()


def test_covers_needs_the_same_subscription_and_a_position_at_the_head():
    change = StreamChange("metrics", 7, 2)
    assert change.covers(7, 2)
    assert change.covers(9, 2)
    assert not change.covers(6, 2)
    assert not change.covers(7, 1)  # drained on an earlier connection
    assert not change.covers(7, None)  # never drained


# ---------------------------------------------------------------- Stream.follow


def _follow(door: Door, records: list, stop: threading.Event, *, idle_drain_s: float | None = 0.05):
    stream = Stream(door, "metrics", "c/worker/metrics")

    def run():
        for record in stream.follow(stop=stop, idle_drain_s=idle_drain_s):
            records.append(record.payload)

    worker = threading.Thread(target=run)
    worker.start()
    return stream, worker


def test_follow_bounds_drains_by_the_hint_and_skips_hints_it_passed():
    door = Door()
    for value in range(3):
        door.put(value)
    records: list = []
    stop = threading.Event()
    stream, worker = _follow(door, records, stop)
    try:
        _until(lambda: stream.position == 3)
        assert records == [0, 1, 2]
        assert "tail" not in door.fetches
        # Idle drains on a passed hint cost nothing.
        assert _quiet(door) == []

        door.put(3)
        door.put(4)
        door.hint()
        _until(lambda: stream.position == 5)
        assert records == [0, 1, 2, 3, 4]
        assert "tail" not in door.fetches
        assert _quiet(door) == []
    finally:
        stop.set()
        worker.join(5)


def test_follow_drains_once_after_a_reconnect_even_at_the_same_head():
    door = Door()
    door.put(0)
    records: list = []
    stop = threading.Event()
    stream, worker = _follow(door, records, stop, idle_drain_s=None)
    try:
        _until(lambda: stream.position == 1)
        settled = len(door.fetches)
        door.disconnect()
        _until(lambda: door.connections == 2)
        _until(lambda: len(door.fetches) > settled)
        assert door.fetches[settled:] == ["page"]
        assert _quiet(door) == []
        assert records == [0]
    finally:
        stop.set()
        worker.join(5)


def test_follow_with_its_own_bell_reads_the_tail_as_before():
    door = Door()
    door.put(0)
    stream = Stream(door, "metrics", "c/worker/metrics")
    assert [r.payload for r in stream.drain()] == [0]
    assert door.fetches == ["tail", "page"]


# ---------------------------------------------------------------- Service.consume


def test_consume_skips_hints_it_passed_and_drains_new_ones_without_a_tail_read():
    door = Door()
    door.put(0)
    stream = Stream(door, "metrics", "c/worker/metrics")
    handled: list = []
    stop = threading.Event()
    worker = threading.Thread(
        target=consume,
        args=(stream, lambda record: handled.append(record.payload)),
        kwargs={"health": HandlerHealth(), "reject": lambda *a: None, "stop": stop, "idle_drain_s": 0.05},
    )
    worker.start()
    try:
        _until(lambda: stream.position == 1)
        assert _quiet(door) == []
        door.put(1)
        door.put(2)
        door.hint()
        _until(lambda: stream.position == 3)
        assert handled == [0, 1, 2]
        assert "tail" not in door.fetches
        assert _quiet(door) == []
    finally:
        stop.set()
        worker.join(5)


def test_consume_retries_a_failed_record_although_the_hint_did_not_move():
    door = Door()
    door.put(0)
    door.put(1)
    stream = Stream(door, "metrics", "c/worker/metrics")
    attempts: list = []

    def handler(record):
        attempts.append(record.payload)
        if record.payload == 1 and attempts.count(1) == 1:
            raise RuntimeError("not yet")

    stop = threading.Event()
    worker = threading.Thread(
        target=consume,
        args=(stream, handler),
        kwargs={
            "health": HandlerHealth(),
            "reject": lambda *a: None,
            "stop": stop,
            "idle_drain_s": None,
            "retry": SimpleNamespace(delay=lambda exc: 0.01, reset=lambda: None),
        },
    )
    worker.start()
    try:
        _until(lambda: stream.position == 2)
        assert attempts == [0, 1, 1]
    finally:
        stop.set()
        worker.join(5)


# ---------------------------------------------------------------- command executor


def test_the_executor_drain_uses_the_hint_and_skips_a_passed_one():
    door = Door(stream="commands")
    door.put({"not": "a command"})
    stream = Stream(door, "commands", "c/worker/commands")
    executor = CommandExecutor(door, lambda topic, payload: None, stream, {}, "node")
    watch = StreamChanges(door, ["commands"]).start()
    executor._watch = watch
    try:
        _until(lambda: watch.latest("commands") is not None)
        assert asyncio.run(executor.drain()) == 1
        assert door.fetches == ["page"]
        assert asyncio.run(executor.drain()) == 0
        assert door.fetches == ["page"]

        door.put({"not": "a command"})
        door.hint()
        _until(lambda: watch.latest("commands").head == 2)
        assert asyncio.run(executor.drain()) == 1
        assert door.fetches == ["page", "page"]
    finally:
        watch.close()


def test_the_executor_learns_it_stands_at_the_head_from_an_empty_page():
    door = Door(stream="commands")
    door.put({"not": "a command"})
    door.cursor = 1  # acknowledged by an earlier run of this service
    stream = Stream(door, "commands", "c/worker/commands")
    executor = CommandExecutor(door, lambda topic, payload: None, stream, {}, "node")
    watch = StreamChanges(door, ["commands"]).start()
    executor._watch = watch
    try:
        _until(lambda: watch.latest("commands") is not None)
        assert asyncio.run(executor.drain()) == 0
        assert stream.position == 1
        assert asyncio.run(executor.drain()) == 0
        assert door.fetches == ["page"]
    finally:
        watch.close()


# ---------------------------------------------------------------- retained view


def _hint(view: RetainedView, door: Door, subscription: int = 1) -> None:
    view.watch._record(Hint(streams=["metrics"], next={"metrics": len(door.records) + 1}), subscription)
    view.watch["metrics"].notify()


class KvDoor(Door):
    def kv(self, prefix, *, contract, depth=None):
        return []


def test_the_view_drains_to_the_hint_and_skips_a_hint_it_applied():
    door = KvDoor()
    door.put({"value": 1})
    view = RetainedView(door, ["_Metric"], ["metrics"], "c/worker/view", scope=ViewScope.whole_node())
    view.synchronize()
    door.fetches.clear()

    door.put({"value": 2})
    _hint(view, door)
    view._refresh_changed()
    assert door.fetches == ["page"]
    assert view.read()[0].payload == {"value": 2}
    assert view.position() == 2

    # The same head again (an idle wake): nothing to read.
    view.watch["metrics"].notify()
    view._refresh_changed()
    assert door.fetches == ["page"]

    # A new subscription is drained once, at the same head.
    _hint(view, door, subscription=2)
    view._refresh_changed()
    assert door.fetches == ["page", "page"]


def test_a_causal_read_still_reads_the_tail():
    door = KvDoor()
    door.put({"value": 1})
    view = RetainedView(door, ["_Metric"], ["metrics"], "c/worker/view", scope=ViewScope.whole_node())
    view.synchronize()
    _hint(view, door)
    door.fetches.clear()
    view.synchronize()
    assert door.fetches[0] == "tail"
