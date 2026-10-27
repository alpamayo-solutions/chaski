"""What an ingest page publishes is pipelined and confirmed before the page counts.

A handler's writes go out without a PUBACK wait each
(:mod:`chaski.publishing`); the page saves its checkpoints and is acked only
once the node confirmed all of them. A write the node refuses puts the
producer back to the page's start and the page is handled again.
"""

from __future__ import annotations

import pytest
from dataops_fakes import AckingClient, FakeDoor, FakeRuntime, kv_entry, run_async, signal_entry, stream_opener
from franzmq.errors import PublishRejected

from chaski.dataops.base import Producer, RewindFailed, restore_checkpoint
from chaski.dataops.buffer import Buffer
from chaski.dataops.ingest import Ingest
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.outputs import SignalOutput
from chaski.dataops.service import build_dispatch, off_loop
from chaski.dataops.triggers import every, on_metric
from chaski.door import Page, Record
from chaski.publishing import Pubacks
from chaski.service import Service


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


class Echo(Producer):
    """Publishes every value it sees at ``out/<value>``; keeps what it saw."""

    name = "echo"
    system_element_name = "SE"
    state_version = 1

    a = SignalRangeInput("signal_a")

    def __init__(self) -> None:
        super().__init__()
        self.seen: list[float] = []

    def snapshot_state(self):
        return {"seen": self.seen}

    def restore_state(self, state) -> None:
        self.seen = list(state["seen"])

    @on_metric("a")
    async def on_a(self, metric) -> None:
        self.seen.append(metric.value)
        self.runtime.send(f"out/{metric.value:g}", "{}")


def _page(start: int, values: list[float]) -> Page:
    records = [
        Record(
            offset=start + i,
            origin_offset=start + i,
            topic="colca/v1/_Metric/n-1/x/a",
            payload={"signal_id": "sig-a", "value": value, "timestamp": 1_700_000_000.0 + start + i},
            ts=(1_700_000_000.0 + start + i) * 1000,
            written_by="connector",
            actor_id="svc-1",
            actor_label="connector",
            actor_kind="local",
        )
        for i, value in enumerate(values)
    ]
    return Page(records=records, next=start + len(records))


class World:
    def __init__(self, tmp_path, client: AckingClient) -> None:
        self.door = FakeDoor([signal_entry("sig-a", "signal_a")])
        self.buffer = Buffer(tmp_path / "buffer.sqlite3")
        self.client = client
        service = Service("dataops", state_dir=tmp_path)
        service._client = client
        service._pubacks = Pubacks(client)
        self.runtime = FakeRuntime(self.door, self.buffer)
        self.runtime.send = service.send  # type: ignore[method-assign]
        self.producer = Echo().attach(self.runtime)
        restore_checkpoint(self.producer)
        dispatch, signal_ids, _ = build_dispatch(self.runtime, [self.producer])
        self.ingest = Ingest(stream_opener(self.door), self.buffer, dispatch=dispatch, signal_ids=signal_ids)


@pytest.fixture
def client():
    c = AckingClient(latency=0.002)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def world(tmp_path, client):
    w = World(tmp_path, client)
    try:
        yield w
    finally:
        w.buffer.close()


@run_async
async def test_a_pages_writes_are_not_waited_for_one_by_one_and_all_are_confirmed_before_the_checkpoint_and_the_ack(
    world, monkeypatch
):
    events: list[str] = []
    on_publish = world.client.on_publish
    world.client.on_publish = lambda *a: (events.append("puback"), on_publish(*a))[1]
    save = world.buffer.save_checkpoint
    monkeypatch.setattr(world.buffer, "save_checkpoint", lambda *a: (events.append("save"), save(*a))[1])
    ack = world.door.ack
    world.door.ack = lambda *a: (events.append("ack"), ack(*a))[1]
    world.door.queue(_page(1, [float(i) for i in range(20)]))

    assert await world.ingest.run_once() == 20

    assert world.client.waited == 0
    assert events == ["puback"] * 20 + ["save", "ack"]


@run_async
async def test_a_refused_write_puts_the_producer_back_and_the_page_is_handled_again(world):
    world.client.refuse = {"out/2"}
    page = _page(1, [1.0, 2.0, 3.0])
    world.door.queue(page)

    with pytest.raises(RewindFailed) as failed:
        await world.ingest.run_once()

    assert isinstance(failed.value.__cause__, Exception) and "0x99" in str(failed.value.__cause__)
    # The state of the page's start, nothing saved, nothing acked.
    assert world.producer.seen == []
    assert world.buffer.checkpoint("echo") is None
    assert world.door.acked == []
    consumer, record = world.ingest.take_failure()
    assert consumer == "echo.on_a" and record.offset == 1

    # The node admits it now: the page is handled again, once.
    world.client.refuse = set()
    world.door.queue(page)
    assert await world.ingest.run_once() == 3
    assert world.producer.seen == [1.0, 2.0, 3.0]
    assert [ack[2] for ack in world.door.acked] == [3]


class Ticker(Producer):
    name = "ticker"
    system_element_name = "SE"

    @every("1h")
    async def tick(self) -> None:
        for i in range(5):
            self.runtime.send(f"tick/{i}", "{}")


def test_a_ticks_writes_are_pipelined_and_a_refusal_fails_the_tick(world):
    from chaski.failures import HandlerHealth

    world.runtime.handler_health = HandlerHealth()
    ticker = Ticker().attach(world.runtime)
    job = off_loop(ticker.tick)

    job()
    assert world.client.waited == 0
    assert [topic for topic, _ in world.client.sent] == [f"tick/{i}" for i in range(5)]

    world.client.refuse = {"tick/3"}
    with pytest.raises(PublishRejected):
        job()
    assert "ticker.tick" in str(world.runtime.handler_health.summary())


def test_a_bound_output_does_not_look_for_queued_samples_on_every_publish(world, monkeypatch):
    calls: list[str] = []
    pending = world.buffer.pending_outputs
    monkeypatch.setattr(world.buffer, "pending_outputs", lambda *a, **k: (calls.append("look"), pending(*a, **k))[1])
    world.door.entries.append(
        kv_entry("colca/v1/_Signal/n-1/x/out", {"id": "sig-out", "data_tag": "tag-out", "is_published": True})
    )
    output = Outputs().attach(world.runtime).out
    output.bind("outputs.out", "tag-out")

    for i in range(10):
        output.publish(float(i), 1_700_000_000.0 + i)

    assert calls == ["look"]
    # A sample queued while unbound is looked for, and sent, again.
    world.buffer.queue_output("outputs.out", 1.0, 1.0)
    output._maybe_queued = True
    output.publish(11.0, 1_700_000_100.0)
    assert calls == ["look", "look", "look"]
    assert world.buffer.pending_outputs("outputs.out") == []


class Outputs(Producer):
    name = "outputs"
    system_element_name = "SE"
    out = SignalOutput("out", "float")

    @every("1h")
    async def tick(self) -> None:
        pass


def test_an_emitted_annotation_inside_a_page_is_committed_with_the_page(world, tmp_path):
    import sqlite3

    other = sqlite3.connect(str(tmp_path / "buffer.sqlite3"))
    try:
        with world.buffer.one_commit():
            world.buffer.record_emitted_annotation("dataops/x", "ann-1", 1.0)
            assert other.execute("SELECT COUNT(*) FROM emitted_annotations").fetchone()[0] == 0
        assert other.execute("SELECT COUNT(*) FROM emitted_annotations").fetchone()[0] == 1
    finally:
        other.close()
