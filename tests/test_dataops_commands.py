"""Tests for chaski.dataops.commands: the `@on_command` executor."""

from __future__ import annotations

import asyncio
import json
import time

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import httpx
import pytest
from dataops_fakes import NODE_ID, FakeDoor, FakeRuntime, run_async

from chaski.dataops import Command, CommandRejected, on_command, resolve
from chaski.dataops.base import Producer
from chaski.dataops.commands import CommandExecutor, command_topics, gather, parse_topic
from chaski.dataops.triggers import OnCommandSpec
from chaski.door import Page, Record, Stream

CURSOR = "c/dataops/commands"
SET_PRODUCT = "line1/operator/setProduct"


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


class Selection(Producer):
    name = "selection"
    system_element_name = "line1"

    def __init__(self) -> None:
        super().__init__()
        self.seen: list[Command] = []

    @on_command(SET_PRODUCT)
    @on_command("line1/operator/setRecipe", contract="_CmdParam")
    async def select(self, command: Command) -> str:
        self.seen.append(command)
        sku = command.params.get("sku")
        if sku == "boom":
            raise RuntimeError("kaputt")
        if sku == "resolve":
            resolve.resolve_system_element(self.runtime.door, "line1")
            resolve.resolve_system_element(self.runtime.door, "line2")
            return "resolved"
        if sku == "busy":
            request = httpx.Request("GET", "http://colca/kv?prefix=")
            raise httpx.HTTPStatusError("429", request=request, response=httpx.Response(429, request=request))
        if sku != "P-1":
            raise CommandRejected(422, f"unknown product {sku!r}")
        return f"product {sku} set"


def record(path: str = SET_PRODUCT, *, contract: str = "_CmdParam", offset: int = 1, **payload) -> Record:
    body = {"correlation_id": "corr-1", "expires_at": (time.time() + 30) * 1000, "command": {"sku": "P-1"}}
    body.update(payload)
    return Record(
        offset=offset,
        origin_offset=offset,
        topic=f"colca/v1/{contract}/{NODE_ID}/{path}",
        payload=body,
        ts=time.time() * 1000,
        written_by="human-1",
        actor_id="human-1",
        actor_label="anna",
        actor_kind="human",
    )


def executor(*records: Record) -> tuple[CommandExecutor, Selection, FakeDoor]:
    door = FakeDoor()
    producer = Selection().attach(FakeRuntime(door, buffer=None))
    if records:
        door.queue(Page(records=list(records), next=records[-1].offset + 1))
    runtime = producer.runtime
    return (
        CommandExecutor(door, runtime.send, Stream(door, "commands", CURSOR), gather([producer]), NODE_ID),
        producer,
        door,
    )


def acks(door: FakeDoor) -> list[tuple[str, dict]]:
    return [(topic, json.loads(payload)) for topic, payload in door.published]


# ─── declaration ─────────────────────────────────────────────────────────


def test_on_command_declares_an_exact_path_and_a_command_contract():
    assert (SET_PRODUCT, OnCommandSpec(path=SET_PRODUCT, contract="_CmdParam")) in [
        (spec.path, spec) for _name, spec in Selection._triggers
    ]
    for bad in ("line1/+/setProduct", "line1/#"):
        with pytest.raises(ValueError):
            on_command(bad)
    with pytest.raises(ValueError):
        on_command(SET_PRODUCT, contract="_Metric")


def test_gather_refuses_one_command_declared_twice():
    first = Selection().attach(FakeRuntime(FakeDoor(), buffer=None))
    second = Selection().attach(FakeRuntime(FakeDoor(), buffer=None))
    with pytest.raises(ValueError):
        gather([first, second])


def test_parse_topic():
    assert parse_topic("colca/v1/_CmdParam/n-1/line1/operator/setProduct") == ("_CmdParam", SET_PRODUCT)
    assert parse_topic("colca/v1/_CmdParam/n-1") is None


def test_command_topics_and_subscription():
    ex, _producer, _door = executor()

    class Client:
        def __init__(self) -> None:
            self.subscribed: list[tuple[str, int]] = []

        def message_callback_add(self, topic, callback) -> None:
            pass

        def subscribe(self, topic, qos):
            self.subscribed.append((topic, qos))
            return 0, len(self.subscribed)

    client = Client()
    assert ex.subscribe(client) == 2
    assert client.subscribed == [
        (f"colca/v1/_CmdParam/{NODE_ID}/line1/operator/setProduct", 1),
        (f"colca/v1/_CmdParam/{NODE_ID}/line1/operator/setRecipe", 1),
    ]


# ─── execution ───────────────────────────────────────────────────────────


@run_async
async def test_success_acks_200_beside_the_command_with_the_attested_actor():
    ex, producer, door = executor(record(actor_id="spoofed"))
    await ex.drain()

    (command,) = producer.seen
    assert (command.path, command.verb, command.contract) == (SET_PRODUCT, "setProduct", "_CmdParam")
    assert command.params == {"sku": "P-1"}
    # The record's attestation, not the payload's claim.
    assert (command.actor_id, command.actor_label, command.actor_kind) == ("human-1", "anna", "human")
    ((topic, ack),) = acks(door)
    assert topic == f"colca/v1/_Ack/{NODE_ID}/{SET_PRODUCT}"
    assert ack["correlation_id"] == "corr-1"
    assert (ack["result_code"], ack["message"]) == (200, "product P-1 set")
    assert isinstance(ack["performed_at"], float)


@run_async
async def test_expired_command_is_answered_498_and_not_run():
    ex, producer, door = executor(record(expires_at=(time.time() - 1) * 1000))
    await ex.drain()
    assert producer.seen == []
    assert acks(door)[0][1]["result_code"] == 498


@run_async
async def test_a_deadline_beyond_the_lifetime_cap_is_refused_and_not_run():
    ex, producer, door = executor(record(expires_at=(time.time() + 3600) * 1000))
    await ex.drain()
    assert producer.seen == []
    ack = acks(door)[0][1]
    assert ack["result_code"] == 400
    assert "60 s after it arrived" in ack["message"]


@run_async
async def test_a_created_at_after_arrival_is_refused():
    ex, producer, door = executor(record(created_at=(time.time() + 3600) * 1000))
    await ex.drain()
    assert producer.seen == []
    assert acks(door)[0][1]["result_code"] == 400


@run_async
async def test_an_old_command_with_a_long_life_is_refused():
    now = time.time()
    ex, producer, door = executor(record(created_at=(now - 600) * 1000, expires_at=(now + 30) * 1000))
    await ex.drain()
    assert producer.seen == []
    assert "after it was created" in acks(door)[0][1]["message"]


@run_async
async def test_a_command_within_the_cap_runs():
    now = time.time()
    ex, producer, door = executor(record(created_at=now * 1000, expires_at=(now + 15) * 1000))
    await ex.drain()
    assert len(producer.seen) == 1
    assert acks(door)[0][1]["result_code"] == 200


@run_async
async def test_rejection_answers_with_its_own_code():
    ex, _producer, door = executor(record(command={"sku": "nope"}))
    await ex.drain()
    ack = acks(door)[0][1]
    assert (ack["result_code"], ack["message"]) == (422, "unknown product 'nope'")


@run_async
async def test_unexpected_failure_answers_500():
    ex, _producer, door = executor(record(command={"sku": "boom"}))
    await ex.drain()
    ack = acks(door)[0][1]
    assert ack["result_code"] == 500
    assert "kaputt" in ack["message"]


@run_async
async def test_undeclared_commands_are_skipped_but_the_cursor_moves():
    ex, producer, door = executor(
        record("line1/operator/somethingElse", offset=1),
        record(SET_PRODUCT, contract="_CmdOperate", offset=2),
        record(offset=3),
    )
    assert await ex.drain() == 3
    assert len(producer.seen) == 1
    assert len(door.published) == 1
    # One ack of the cursor, after the whole page.
    assert door.acked == [("commands", CURSOR, 3)]


@run_async
async def test_a_command_without_correlation_id_runs_but_is_not_answered():
    ex, producer, door = executor(record(correlation_id=""))
    await ex.drain()
    assert len(producer.seen) == 1
    assert door.published == []
    assert door.acked == [("commands", CURSOR, 1)]


@run_async
async def test_run_forever_drains_at_start_and_on_wake():
    ex, producer, door = executor(record(offset=1))
    stop = asyncio.Event()
    task = asyncio.ensure_future(ex.run_forever(stop))
    for _ in range(50):
        await asyncio.sleep(0.01)
        if producer.seen:
            break
    assert len(producer.seen) == 1

    door.queue(Page(records=[record(offset=2, correlation_id="corr-2")], next=3))
    ex.wake()
    for _ in range(50):
        await asyncio.sleep(0.01)
        if len(producer.seen) == 2:
            break
    assert [c.correlation_id for c in producer.seen] == ["corr-1", "corr-2"]
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)


@run_async
async def test_an_idle_executor_reads_nothing_until_woken_and_keeps_a_wake_during_a_drain():
    """No timer behind the wake. A wake that lands while a drain reads its
    page leads to one more drain; the generation is taken before the read."""

    class RingingDoor(FakeDoor):
        executor: CommandExecutor | None = None

        def fetch(self, stream, cursor, **kwargs):
            page = super().fetch(stream, cursor, **kwargs)
            if len(self.fetch_calls) == 1 and self.executor is not None:
                self.executor.wake()  # a command arrived while the first page was read
            return page

    door = RingingDoor()
    producer = Selection().attach(FakeRuntime(door, buffer=None))
    ex = CommandExecutor(door, producer.runtime.send, Stream(door, "commands", CURSOR), gather([producer]), NODE_ID)
    door.executor = ex
    stop = asyncio.Event()
    task = asyncio.ensure_future(ex.run_forever(stop))
    for _ in range(50):
        await asyncio.sleep(0.01)
        if len(door.fetch_calls) >= 2:
            break
    await asyncio.sleep(0.3)
    assert len(door.fetch_calls) == 2, "the drain at start, and one for the wake during it; nothing on a timer"
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)


def test_the_stream_is_read_by_the_topics_that_wake_it():
    """Commands to other services are neither read nor left unread on this
    cursor: the fetch carries the executor's own command topics."""
    door = FakeDoor()
    producer = Selection().attach(FakeRuntime(door, buffer=None))
    topics = command_topics(gather([producer]), NODE_ID)
    stream = Stream(door, "commands", CURSOR, topics=topics)
    ex = CommandExecutor(door, producer.runtime.send, stream, gather([producer]), NODE_ID)
    assert ex.command_topics() == topics
    asyncio.run(ex.drain())
    assert door.fetch_calls[0]["topics"] == [
        f"colca/v1/_CmdParam/{NODE_ID}/line1/operator/setProduct",
        f"colca/v1/_CmdParam/{NODE_ID}/line1/operator/setRecipe",
    ]


@run_async
async def test_rapid_commands_do_not_read_the_whole_kv_each():
    """A command reads the node's KV only when its handler resolves something."""
    records = [record(offset=i, correlation_id=f"corr-{i}") for i in range(1, 21)]
    ex, _producer, door = executor(*records)
    await ex.drain()
    assert [ack["result_code"] for _topic, ack in acks(door)] == [200] * 20
    assert door.kv_calls == 0


@run_async
async def test_a_command_s_resolutions_share_one_kv_read():
    ex, _producer, door = executor(record(command={"sku": "resolve"}))
    await ex.drain()
    assert acks(door)[0][1]["result_code"] == 200
    assert door.kv_calls == 1


@run_async
async def test_a_node_that_is_busy_answers_503_without_its_address():
    ex, _producer, door = executor(record(command={"sku": "busy"}))
    await ex.drain()
    ack = acks(door)[0][1]
    assert ack["result_code"] == 503
    assert "http" not in ack["message"]
