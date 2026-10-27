"""Checkpointed producers in live ingest: state saved once per page.

The ingest lane keeps a checkpointed producer's state at its first record of
a page and saves the checkpoint once, in the page's commit, before the ack
(:class:`chaski.dataops.base.PageCheckpoints`). These tests hold the
guarantees per-record checkpoints gave: every handler sees every record in
order, a crash between the effects and the ack does not apply a record
twice, a failed or rejected record leaves the state as it was before it, and
the next start restores the state of the last page.
"""

from __future__ import annotations

import asyncio
import threading

import httpx
import pytest
from dataops_fakes import FakeDoor, FakeRuntime, run_async, signal_entry, stream_opener

from chaski import Reject
from chaski.dataops.base import Producer, RewindFailed, restore_checkpoint
from chaski.dataops.buffer import Buffer
from chaski.dataops.ingest import Ingest
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.service import build_dispatch, off_loop
from chaski.dataops.triggers import every, on_metric
from chaski.door import Page, Record


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


class Counter(Producer):
    """Sums what it sees and lists it, per input; fails or rejects on cue."""

    name = "counter"
    system_element_name = "SE"
    state_version = 1

    a = SignalRangeInput("signal_a")
    b = SignalRangeInput("signal_b")

    def __init__(self) -> None:
        super().__init__()
        self.seen: list[list] = []
        self.total = 0.0
        self.calls = 0
        self.fail_at: set[float] = set()
        self.reject_at: set[float] = set()
        #: Holds the next ``b`` record until set; ``started`` says it is held.
        self.gate: asyncio.Event | None = None
        self.started = threading.Event()

    def snapshot_state(self):
        return {"seen": self.seen, "total": self.total}

    def restore_state(self, state) -> None:
        self.seen = [list(item) for item in state["seen"]]
        self.total = state["total"]

    async def _see(self, name: str, metric) -> None:
        if name == "b" and self.gate is not None:
            self.started.set()
            await self.gate.wait()
        self.calls += 1
        self.seen.append([name, metric.value])
        self.total += metric.value
        if metric.value in self.fail_at:
            raise RuntimeError(f"boom at {metric.value}")
        if metric.value in self.reject_at:
            raise Reject("bad value")

    @on_metric("a")
    async def on_a(self, metric) -> None:
        await self._see("a", metric)

    @on_metric("b")
    async def on_b(self, metric) -> None:
        await self._see("b", metric)

    @every("1h")
    async def tick(self) -> None:
        self.seen.append(["tick", 0])


def _record(offset: int, signal: str, value: float) -> Record:
    return Record(
        offset=offset,
        origin_offset=offset,
        topic=f"colca/v1/_Metric/n-1/x/{signal}",
        payload={"signal_id": signal, "value": value, "timestamp": 1_700_000_000.0 + offset},
        ts=(1_700_000_000.0 + offset) * 1000,
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )


def _page(start: int, values: list[tuple[str, float]]) -> Page:
    records = [_record(start + i, signal, value) for i, (signal, value) in enumerate(values)]
    return Page(records=records, next=start + len(records))


class World:
    """A door, a buffer on disk, and a service-like runtime around one Counter."""

    def __init__(self, tmp_path) -> None:
        self.path = tmp_path / "buffer.sqlite3"
        self.door = FakeDoor([signal_entry("sig-a", "signal_a"), signal_entry("sig-b", "signal_b")])
        self.rejected: list[tuple[str, dict]] = []
        self.start()

    def start(self) -> None:
        """A (re)start: a new producer from the buffer's checkpoint, as the service does."""
        self.buffer = Buffer(self.path)
        self.runtime = FakeRuntime(self.door, self.buffer)
        self.producer = Counter().attach(self.runtime)
        restore_checkpoint(self.producer)
        dispatch, signal_ids, _ = build_dispatch(self.runtime, [self.producer])
        self.ingest = Ingest(
            stream_opener(self.door),
            self.buffer,
            dispatch=dispatch,
            signal_ids=signal_ids,
            reject=lambda consumer, subject, rejection: self.rejected.append((consumer, subject)),
        )

    def restart(self) -> None:
        self.buffer.close()
        self.start()

    def close(self) -> None:
        self.buffer.close()


def _rename(values: list[tuple[str, float]]) -> list[tuple[str, float]]:
    return [("sig-" + signal, value) for signal, value in values]


@pytest.fixture
def world(tmp_path):
    w = World(tmp_path)
    try:
        yield w
    finally:
        w.close()


@run_async
async def test_every_handler_sees_every_record_in_stream_order_across_pages(world):
    world.door.queue(_page(1, _rename([("a", 1), ("b", 2), ("a", 3)])))
    world.door.queue(_page(4, _rename([("b", 4), ("b", 5), ("a", 6)])))
    await world.ingest.run_once()
    await world.ingest.run_once()
    assert world.producer.seen == [["a", 1], ["b", 2], ["a", 3], ["b", 4], ["b", 5], ["a", 6]]
    assert [ack[2] for ack in world.door.acked] == [3, 6]


@run_async
async def test_the_checkpoint_is_saved_once_per_page_before_the_ack(world, monkeypatch):
    events: list[str] = []
    save = world.buffer.save_checkpoint
    monkeypatch.setattr(world.buffer, "save_checkpoint", lambda *a: (events.append("save"), save(*a))[1])
    ack = world.door.ack
    world.door.ack = lambda *a: (events.append("ack"), ack(*a))[1]
    world.door.queue(_page(1, _rename([("a", float(i)) for i in range(50)])))
    await world.ingest.run_once()
    assert events == ["save", "ack"]
    assert world.buffer.checkpoint("counter")[3] == {"on_a": 50}


@run_async
async def test_a_crash_between_the_effects_and_the_ack_does_not_apply_a_record_twice(world):
    def lost_ack(*args):
        raise httpx.ConnectError("node away")

    world.door.ack = lost_ack
    page = _page(1, _rename([("a", 1), ("b", 2)]))
    world.door.queue(page)
    with pytest.raises(httpx.ConnectError):
        await world.ingest.run_once()
    world.restart()
    assert world.producer.seen == [["a", 1], ["b", 2]]  # the checkpoint of the page
    world.door.queue(page)  # the unacked page comes again
    world.door.queue(_page(3, _rename([("a", 3)])))
    world.door.ack = FakeDoor.ack.__get__(world.door)
    await world.ingest.run_once()
    await world.ingest.run_once()
    assert world.producer.seen == [["a", 1], ["b", 2], ["a", 3]]
    assert world.producer.total == 6


@run_async
async def test_a_page_whose_checkpoint_was_not_saved_is_applied_once_after_a_restart(world, monkeypatch):
    """The process gone before the page's checkpoint was saved: the restart
    has the previous page's state and applies the page once."""
    world.door.queue(_page(1, _rename([("a", 1)])))
    await world.ingest.run_once()

    def gone(*args):
        raise OSError("process gone")

    monkeypatch.setattr(world.buffer, "save_checkpoint", gone)
    page = _page(2, _rename([("a", 2), ("b", 3)]))
    world.door.queue(page)
    with pytest.raises(RewindFailed):
        await world.ingest.run_once()
    monkeypatch.undo()
    world.restart()
    assert world.producer.seen == [["a", 1]]
    world.door.queue(page)
    await world.ingest.run_once()
    assert world.producer.seen == [["a", 1], ["a", 2], ["b", 3]]


@run_async
async def test_a_failed_record_leaves_the_state_of_the_records_before_it(world):
    world.producer.fail_at = {4}
    world.door.queue(_page(1, _rename([("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5)])))
    with pytest.raises(RuntimeError, match="boom at 4"):
        await world.ingest.run_once()
    assert world.ingest.take_failure()[1].offset == 4
    assert world.producer.seen == [["a", 1], ["b", 2], ["a", 3]]
    assert world.producer.total == 6
    assert world.buffer.checkpoint("counter")[2]["seen"] == [["a", 1], ["b", 2], ["a", 3]]
    # The retry resumes at the failed record and applies it once.
    world.producer.fail_at = set()
    world.door.queue(_page(4, _rename([("b", 4), ("a", 5)])))
    await world.ingest.run_once()
    assert world.producer.seen == [["a", 1], ["b", 2], ["a", 3], ["b", 4], ["a", 5]]


@run_async
async def test_a_rejected_record_is_set_aside_and_the_page_goes_on(world):
    world.producer.reject_at = {2, 4}
    world.door.queue(_page(1, _rename([("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5)])))
    await world.ingest.run_once()
    assert world.producer.seen == [["a", 1], ["a", 3], ["a", 5]]
    assert world.producer.total == 9
    assert [subject["offset"] for _consumer, subject in world.rejected] == [2, 4]
    assert world.door.acked[-1][2] == 5
    world.restart()
    assert world.producer.seen == [["a", 1], ["a", 3], ["a", 5]]


@run_async
async def test_a_checkpoint_that_cannot_be_saved_puts_the_producer_back_to_the_page_start(world, monkeypatch):
    world.door.queue(_page(1, _rename([("a", 1)])))
    await world.ingest.run_once()

    def full(*args):
        raise OSError("disk full")

    monkeypatch.setattr(world.buffer, "save_checkpoint", full)
    world.door.queue(_page(2, _rename([("a", 2), ("b", 3)])))
    with pytest.raises(RewindFailed, match="before offset=2"):
        await world.ingest.run_once()
    assert world.producer.seen == [["a", 1]]
    assert world.ingest.take_failure()[1].offset == 2
    monkeypatch.undo()
    world.door.queue(_page(2, _rename([("a", 2), ("b", 3)])))
    await world.ingest.run_once()
    assert world.producer.seen == [["a", 1], ["a", 2], ["b", 3]]


@run_async
async def test_a_tick_waits_for_the_page_so_a_rewind_keeps_what_it_did(world):
    """The producer is locked for the page: a tick runs before or after it,
    never between two of its records."""
    release = asyncio.Event()
    tick = off_loop(world.producer.tick)
    world.producer.gate = release
    world.producer.fail_at = {3}
    world.door.queue(_page(1, _rename([("a", 1), ("b", 2), ("a", 3)])))
    drain = asyncio.ensure_future(world.ingest.run_once())
    await asyncio.to_thread(world.producer.started.wait, 5)
    ticking = asyncio.ensure_future(asyncio.to_thread(tick))
    await asyncio.sleep(0.05)
    assert not ticking.done(), "the tick ran while the page held the producer"
    release.set()
    with pytest.raises(RuntimeError, match="boom at 3"):
        await drain
    await ticking
    assert world.producer.seen == [["a", 1], ["b", 2], ["tick", 0]]


@run_async
async def test_a_command_on_the_loop_waits_for_the_page_that_holds_the_producer(world):
    """The producer's lock is a thread lock, and a command handler runs on the
    same loop thread as the page: it entered the lock while the page awaited
    between two records, and changed the state the page was keeping."""
    from chaski.dataops.base import turn

    world.producer.gate = asyncio.Event()
    world.door.queue(_page(1, _rename([("a", 1), ("b", 2)])))
    page = asyncio.ensure_future(world.ingest.run_once())
    while not world.producer.started.is_set():
        await asyncio.sleep(0.001)

    async def command() -> None:
        async with turn(world.producer):
            world.producer.seen.append(["command", 0])

    running = asyncio.ensure_future(command())
    await asyncio.sleep(0.05)
    assert not running.done(), "the command ran inside the page"
    world.producer.gate.set()
    await page
    await running
    assert world.producer.seen == [["a", 1], ["b", 2], ["command", 0]]
    assert world.buffer.checkpoint("counter")[2]["seen"] == [["a", 1], ["b", 2]]
