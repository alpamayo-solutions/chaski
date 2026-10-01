"""A command's ``operation_id`` and ``on_behalf_of``: one execution per
operation, whoever retries it, and the person a service acts for carried to
the handler and into the answer. Driven with the `@on_command` fakes."""

from __future__ import annotations

import json
import time

import pytest
from dataops_fakes import NODE_ID, FakeDoor, FakeRuntime, run_async
from test_dataops_commands import SET_PRODUCT, Selection, _isolate_registry  # noqa: F401 - fixture

from chaski.command import Actor, CommandSender, EnvelopeError
from chaski.dataops.commands import CommandExecutor, gather
from chaski.door import Page, Record, Stream
from chaski.executor import Command, MemoryLedger, SqliteLedger

CURSOR = "c/dataops/commands"
HMI = {"actor_id": "01JHMI", "actor_label": "hmi-api", "actor_kind": "service"}
ANNA = {"id": "sub-anna", "label": "anna", "kind": "human"}


def record(offset: int, correlation_id: str, *, sender: dict | None = None, **payload) -> Record:
    body = {"correlation_id": correlation_id, "expires_at": (time.time() + 30) * 1000, "command": {"sku": "P-1"}}
    body.update(payload)
    who = sender or HMI
    return Record(
        offset=offset,
        origin_offset=offset,
        topic=f"colca/v1/_CmdParam/{NODE_ID}/{SET_PRODUCT}",
        payload=body,
        ts=time.time() * 1000,
        written_by=who["actor_id"],
        **who,
    )


def executor(*records: Record, ledger=None) -> tuple[CommandExecutor, Selection, FakeDoor]:
    door = FakeDoor()
    producer = Selection().attach(FakeRuntime(door, buffer=None))
    door.queue(Page(records=list(records), next=records[-1].offset + 1))
    ex = CommandExecutor(
        door,
        producer.runtime.send,
        Stream(door, "commands", CURSOR),
        gather([producer]),
        NODE_ID,
        ledger=ledger,
    )
    return ex, producer, door


def answers(door: FakeDoor) -> list[dict]:
    return [json.loads(payload) for _topic, payload in door.published]


@run_async
async def test_a_repeated_operation_is_answered_from_the_record_and_not_run_again():
    ex, producer, door = executor(
        record(1, "c-1", operation_id="op-1", on_behalf_of=ANNA),
        record(2, "c-2", operation_id="op-1", on_behalf_of=ANNA),
    )
    await ex.drain()

    assert len(producer.seen) == 1
    first, repeat = answers(door)
    assert (first["correlation_id"], first["result_code"], first["message"]) == ("c-1", 200, "product P-1 set")
    assert "replayed" not in first
    assert (repeat["correlation_id"], repeat["result_code"], repeat["message"]) == ("c-2", 200, "product P-1 set")
    assert repeat["replayed"] is True
    for ack in (first, repeat):
        assert ack["operation_id"] == "op-1"
        assert ack["on_behalf_of"] == ANNA


@run_async
async def test_a_refused_operation_is_replayed_too():
    ex, producer, door = executor(
        record(1, "c-1", operation_id="op-1", command={"sku": "nope"}),
        record(2, "c-2", operation_id="op-1", command={"sku": "nope"}),
    )
    await ex.drain()

    assert len(producer.seen) == 1
    assert [(a["result_code"], a.get("replayed", False)) for a in answers(door)] == [(422, False), (422, True)]


@run_async
async def test_the_handler_sees_the_attested_sender_and_the_person_it_acts_for():
    ex, producer, _door = executor(record(1, "c-1", operation_id="op-1", on_behalf_of=ANNA, actor_id="spoofed"))
    await ex.drain()

    (command,) = producer.seen
    assert command.sender == Actor("01JHMI", "hmi-api", "service")
    assert command.on_behalf_of == Actor("sub-anna", "anna", "human")
    assert command.operation_id == "op-1"


@run_async
async def test_an_operation_id_reused_for_a_different_command_is_refused_409():
    ex, producer, door = executor(
        record(1, "c-1", operation_id="op-1"),
        record(2, "c-2", operation_id="op-1", command={"sku": "P-2"}),
        record(3, "c-3", operation_id="op-1", on_behalf_of=ANNA),
    )
    await ex.drain()

    assert len(producer.seen) == 1
    _first, other_command, other_person = answers(door)
    for ack in (other_command, other_person):
        assert ack["result_code"] == 409
        assert ack["result"] == {"outcome": "refused"}
        assert "replayed" not in ack


@run_async
async def test_operations_are_scoped_to_their_sender():
    other = {"actor_id": "01JOTHER", "actor_label": "other-api", "actor_kind": "service"}
    ex, producer, door = executor(
        record(1, "c-1", operation_id="op-1"),
        record(2, "c-2", operation_id="op-1", sender=other),
    )
    await ex.drain()

    assert [c.actor_id for c in producer.seen] == ["01JHMI", "01JOTHER"]
    assert [a.get("replayed", False) for a in answers(door)] == [False, False]


@run_async
async def test_an_operation_started_and_never_answered_is_answered_504_on_repeat():
    ledger = MemoryLedger()
    # The service stopped while executing it: started, no answer recorded.
    started = Command(path=SET_PRODUCT, verb="setProduct", contract="_CmdParam", actor_id="01JHMI", operation_id="op-1")
    ledger.command_started(CommandExecutor._operation_key(started))

    ex, producer, door = executor(record(2, "c-2", operation_id="op-1"), ledger=ledger)
    await ex.drain()

    assert producer.seen == []
    (ack,) = answers(door)
    assert ack["result_code"] == 504
    assert ack["replayed"] is True


@run_async
async def test_a_repeat_after_a_restart_is_still_answered_from_the_record(tmp_path):
    ledger = SqliteLedger(tmp_path / "ledger.sqlite3")
    ex, producer, _door = executor(record(1, "c-1", operation_id="op-1"), ledger=ledger)
    await ex.drain()
    assert len(producer.seen) == 1
    ledger.close()

    ledger = SqliteLedger(tmp_path / "ledger.sqlite3")
    ex, producer, door = executor(record(2, "c-2", operation_id="op-1"), ledger=ledger)
    await ex.drain()
    ledger.close()

    assert producer.seen == []
    (ack,) = answers(door)
    assert (ack["result_code"], ack["replayed"]) == (200, True)


@run_async
async def test_an_expired_operation_is_not_recorded_so_its_retry_runs():
    ex, producer, door = executor(
        record(1, "c-1", operation_id="op-1", expires_at=(time.time() - 1) * 1000),
        record(2, "c-2", operation_id="op-1"),
    )
    await ex.drain()

    assert len(producer.seen) == 1
    assert [a["result_code"] for a in answers(door)] == [498, 200]


@pytest.mark.parametrize(
    "envelope",
    [
        {"operation_id": ""},
        {"operation_id": 42},
        {"operation_id": "a/b"},
        {"operation_id": "x" * 129},
        {"on_behalf_of": "sub-anna"},
        {"on_behalf_of": {"label": "anna"}},
        {"on_behalf_of": {"id": "sub-anna", "kind": "robot"}},
        {"on_behalf_of": {"id": "sub-anna", "role": "admin"}},
    ],
)
@run_async
async def test_a_malformed_envelope_is_refused_422_and_not_run(envelope):
    ex, producer, door = executor(record(1, "c-1", **envelope))
    await ex.drain()

    assert producer.seen == []
    (ack,) = answers(door)
    assert (ack["result_code"], ack["result"]) == (422, {"outcome": "refused"})


@run_async
async def test_a_person_cannot_send_a_command_on_behalf_of_someone_else():
    person = {"actor_id": "sub-franz", "actor_label": "franz", "actor_kind": "human"}
    ex, producer, door = executor(
        record(1, "c-1", sender=person, on_behalf_of=ANNA),
        record(2, "c-2", sender=person, on_behalf_of={"id": "sub-franz"}),
    )
    await ex.drain()

    assert [c.correlation_id for c in producer.seen] == ["c-2"]
    assert [a["result_code"] for a in answers(door)] == [403, 200]


class _Client:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    def message_callback_add(self, *_args) -> None: ...

    def subscribe(self, *_args, **_kwargs) -> None: ...

    def publish(self, topic, payload, qos=0) -> None:
        self.published.append((topic, json.loads(payload)))


def test_the_sender_puts_operation_and_person_beside_the_correlation_id():
    client = _Client()
    sender = CommandSender(client, NODE_ID)

    sent = sender.send(
        "_CmdParam", "a/b", {"command": {"value": 1}}, lifetime=5, operation_id="op-1", on_behalf_of="sub-anna"
    )
    sender.send("_CmdParam", "a/b", {"command": {"value": 1}}, lifetime=5, on_behalf_of=Actor("svc", "", "system"))
    sender.send("_CmdParam", "a/b", {"command": {"value": 1}}, lifetime=5)

    (_t, with_both), (_t2, system), (_t3, plain) = client.published
    assert with_both["operation_id"] == "op-1"
    assert with_both["on_behalf_of"] == {"id": "sub-anna", "kind": "human"}
    assert with_both["correlation_id"] == sent.correlation_id
    assert system["on_behalf_of"] == {"id": "svc", "kind": "system"}
    assert "operation_id" not in plain and "on_behalf_of" not in plain


def test_the_sender_refuses_a_malformed_envelope_before_publishing():
    client = _Client()
    sender = CommandSender(client, NODE_ID)
    with pytest.raises(EnvelopeError):
        sender.send("_CmdParam", "a/b", {}, lifetime=5, operation_id="a/b")
    with pytest.raises(EnvelopeError):
        sender.send("_CmdParam", "a/b", {}, lifetime=5, on_behalf_of={"id": ""})
    assert client.published == []
