"""Tests for chaski.dataops.commands: the `@on_command` executor."""

from __future__ import annotations

import asyncio
import json
import time

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from dataops_fakes import NODE_ID, FakeDoor, FakeRuntime, run_async

from chaski.dataops import Command, CommandRejected, on_command
from chaski.dataops.base import Producer
from chaski.dataops.commands import CommandExecutor, gather, parse_topic
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
        ts=1000.0,
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
    return CommandExecutor(door, Stream(door, "commands", CURSOR), gather([producer]), NODE_ID), producer, door


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

        def subscribe(self, topic, qos) -> None:
            self.subscribed.append((topic, qos))

    client = Client()
    assert ex.subscribe(client, asyncio.new_event_loop()) == 2
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
