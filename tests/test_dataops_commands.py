"""Tests for chaski.dataops.commands: the `@on_command` executor."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import time
from typing import ClassVar

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import httpx
import pytest
from dataops_fakes import NODE_ID, FakeDoor, FakeRuntime, run_async
from franzmq.errors import PublishTimeout

from chaski.dataops import Command, CommandRejected, commands, on_command, resolve
from chaski.dataops.base import Producer
from chaski.dataops.commands import CommandExecutor, gather, parse_topic
from chaski.dataops.triggers import OnCommandSpec
from chaski.door import Page, Record, Stream
from chaski.failures import HandlerHealth

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


@run_async
async def test_the_watch_wakes_on_every_contract_the_stream_reads():
    """The node counts what the fetch filter matches as unread on the cursor,
    so each of those records must wake a drain: the executor's own answers
    land after the head a drain captured."""
    ex, _producer, door = executor()
    watched: list[list[str]] = []

    def watch(streams, *, contracts=(), **kwargs):
        watched.append(list(contracts))
        return iter(())

    door.watch = watch
    stop = asyncio.Event()
    task = asyncio.ensure_future(ex.run_forever(stop))
    for _ in range(100):
        await asyncio.sleep(0.01)
        if watched:
            break
    stop.set()
    ex.wake()
    await asyncio.wait_for(task, timeout=1.0)
    read = {parse_topic(topic)[0] for topic in commands.stream_topics(ex._handlers, NODE_ID)}
    assert read == {"_CmdParam", "_Ack"}
    assert read <= set(watched[0])


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
async def test_a_command_without_expiry_runs_however_long_it_waited():
    """No expires_at: the command never expires. One sent a month ago, when
    the node was cut off from its sender, still runs."""
    body = record()
    del body.payload["expires_at"]
    month_ago = (time.time() - 30 * 86400) * 1000
    body = dataclasses.replace(body, ts=month_ago)
    ex, producer, door = executor(body)
    await ex.drain()
    assert len(producer.seen) == 1
    assert producer.seen[0].expires_at is None
    assert acks(door)[0][1]["result_code"] == 200


@run_async
async def test_a_long_lifetime_is_the_senders_choice_and_runs():
    now = time.time()
    ex, producer, door = executor(record(created_at=(now - 600) * 1000, expires_at=(now + 7 * 86400) * 1000))
    await ex.drain()
    assert len(producer.seen) == 1
    assert acks(door)[0][1]["result_code"] == 200


@run_async
async def test_a_progress_ack_at_its_path_does_not_count_as_its_answer():
    """A 202 says a node queued or forwarded the command; the command still
    runs."""
    progress = Record(
        offset=2,
        origin_offset=2,
        topic=f"colca/v1/_Ack/{NODE_ID}/{SET_PRODUCT}",
        payload={"correlation_id": "corr-1", "result_code": 202, "stage": "forwarded"},
        ts=time.time() * 1000,
        written_by="n-hub",
        actor_id="human-1",
        actor_label="anna",
        actor_kind="human",
    )
    ex, producer, door = executor(record(), progress)
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
async def test_commands_of_other_services_move_the_cursor_past_them():
    """Load test round 2: `c/<service>/commands` stood 8 and 14 records behind,
    up to 53 min, behind commands other services execute."""
    ex, producer, door = executor()
    door.queue(Page(records=[], next=15, start=1))
    await ex.drain()
    assert producer.seen == []
    assert door.acked == [("commands", CURSOR, 14)]


def test_the_stream_reads_only_the_declared_command_contracts():
    door = FakeDoor()
    producer = Selection().attach(FakeRuntime(door, buffer=None))
    handlers = gather([producer])
    Stream(door, "commands", CURSOR, contracts=commands.contracts(handlers)).fetch()
    assert door.fetch_calls[-1]["contracts"] == ["_CmdParam"]


@run_async
async def test_the_stream_growing_wakes_a_drain():
    """A command of another service rings no MQTT bell here; the stream's growth does."""
    ex, _producer, door = executor()
    hints = [asyncio.Event()]

    def watch(streams, *, interval_ms=None, **kwargs):
        assert list(streams) == ["commands"]
        door.queue(Page(records=[], next=8, start=1))
        from chaski.door import Hint

        yield Hint(["commands"], {})
        hints[0].set()

    door.watch = watch
    stop = asyncio.Event()
    task = asyncio.ensure_future(ex.run_forever(stop))
    for _ in range(100):
        await asyncio.sleep(0.01)
        if ("commands", CURSOR, 7) in door.acked:
            break
    stop.set()
    ex.wake()
    await asyncio.wait_for(task, timeout=1.0)
    assert ("commands", CURSOR, 7) in door.acked


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


# ─── answers survive a broker outage ─────────────────────────────────────


class FlakySend:
    """``send`` that fails ``failures`` times with a missing PUBACK, then
    lands on ``door.published``."""

    def __init__(self, door: FakeDoor, failures: int) -> None:
        self.door = door
        self.failures = failures
        self.attempts = 0

    def __call__(self, topic: str, payload: str) -> None:
        self.attempts += 1
        if self.attempts <= self.failures:
            raise PublishTimeout(topic, 0.01)
        self.door.published.append((topic, payload))


def flaky_executor(*records: Record, failures: int, ledger=None, health=None):
    door = FakeDoor()
    producer = Selection().attach(FakeRuntime(door, buffer=None))
    if records:
        door.queue(Page(records=list(records), next=records[-1].offset + 1))
    send = FlakySend(door, failures)
    ex = CommandExecutor(
        door,
        send,
        Stream(door, "commands", CURSOR),
        gather([producer]),
        NODE_ID,
        ledger=ledger,
        health=health,
    )
    return ex, producer, door, send


@pytest.fixture
def fast_answer_backoff(monkeypatch):
    monkeypatch.setattr(commands, "ANSWER_BACKOFF_MAX_S", 0.02)


@run_async
async def test_a_failed_answer_is_sent_again_before_the_cursor_moves(fast_answer_backoff):
    health = HandlerHealth()
    ex, producer, door, send = flaky_executor(record(), failures=2, health=health)
    await ex.drain()
    assert len(producer.seen) == 1, "the command runs once, however often its answer is sent"
    assert send.attempts == 3
    assert [ack["result_code"] for _topic, ack in acks(door)] == [200]
    assert door.acked == [("commands", CURSOR, 1)]
    assert health.status == "ok", "a confirmed answer clears the failure"


@run_async
async def test_the_executor_survives_a_failing_answer_and_reports_it(fast_answer_backoff):
    """Unity break test: the `_Ack` publish raised PublishTimeout, the executor
    task died, and every later command went unanswered."""
    health = HandlerHealth()
    ex, producer, door, send = flaky_executor(record(), failures=10**6, health=health)
    stop = asyncio.Event()
    task = asyncio.ensure_future(ex.run_forever(stop))
    for _ in range(200):
        await asyncio.sleep(0.01)
        if send.attempts >= 6:
            break
    assert not task.done()
    assert health.status == "unhealthy"
    assert door.acked == [], "the cursor stays before an unanswered command"
    send.failures = 0
    for _ in range(200):
        await asyncio.sleep(0.01)
        if door.acked:
            break
    assert door.acked == [("commands", CURSOR, 1)]
    assert len(producer.seen) == 1
    assert health.status == "ok"
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)


@run_async
async def test_an_answer_waits_for_the_broker_link():
    ex, _producer, door, send = flaky_executor(record(), failures=0)
    ex.link_changed(False)
    drain = asyncio.ensure_future(ex.drain())
    await asyncio.sleep(0.05)
    assert send.attempts == 0 and not drain.done()
    ex.link_changed(True)
    await asyncio.wait_for(drain, timeout=1.0)
    assert [ack["result_code"] for _topic, ack in acks(door)] == [200]


@run_async
async def test_stopping_with_an_answer_pending_leaves_the_cursor(fast_answer_backoff):
    ex, _producer, door, _send = flaky_executor(record(), failures=10**6)
    stop = asyncio.Event()
    task = asyncio.ensure_future(ex.run_forever(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    assert door.acked == []


# ─── one execution, one answer ─────────────────────────────────────────────


@run_async
async def test_a_restart_republishes_the_recorded_answer_without_running_again(tmp_path, fast_answer_backoff):
    from chaski.dataops.buffer import Buffer

    ledger = Buffer(tmp_path / "buffer.sqlite")
    first, producer, _door, _send = flaky_executor(record(), failures=10**6, ledger=ledger)
    stop = asyncio.Event()
    task = asyncio.ensure_future(first.run_forever(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    assert len(producer.seen) == 1
    ledger.close()

    ledger = Buffer(tmp_path / "buffer.sqlite")
    # The command is now past its deadline: before, a restart answered it 498.
    late = record(expires_at=(time.time() - 1) * 1000)
    second, again, door, _send = flaky_executor(late, failures=0, ledger=ledger)
    await second.drain()
    assert again.seen == []
    ((_topic, ack),) = acks(door)
    assert ack["result_code"] == 200
    assert door.acked == [("commands", CURSOR, 1)]
    ledger.close()


@run_async
async def test_a_command_started_but_not_answered_gets_504_and_does_not_run_again():
    ledger = commands.MemoryLedger()
    ledger.command_started("corr-1")
    ex, producer, door, _send = flaky_executor(record(), failures=0, ledger=ledger)
    await ex.drain()
    assert producer.seen == []
    ack = acks(door)[0][1]
    assert ack["result_code"] == 504
    assert "outcome unknown" in ack["message"]


@run_async
async def test_a_command_whose_answer_is_in_the_stream_is_neither_run_nor_answered():
    answer = Record(
        offset=2,
        origin_offset=2,
        topic=f"colca/v1/_Ack/{NODE_ID}/{SET_PRODUCT}",
        payload={"correlation_id": "corr-1", "result_code": 500, "message": "x"},
        ts=time.time() * 1000,
        written_by="svc",
        actor_id="svc",
        actor_label="",
        actor_kind="service",
    )
    ex, producer, door, _send = flaky_executor(record(offset=1), answer, failures=0)
    await ex.drain()
    assert producer.seen == []
    assert door.published == []
    assert door.acked == [("commands", CURSOR, 2)]


@run_async
async def test_a_full_page_reads_ahead_for_answers_before_executing():
    door = FakeDoor()
    producer = Selection().attach(FakeRuntime(door, buffer=None))
    stream = Stream(door, "commands", CURSOR, max=1)
    ex = CommandExecutor(door, FlakySend(door, 0), stream, gather([producer]), NODE_ID)
    door.queue(Page(records=[record(offset=1)], next=2, start=1))
    later = Record(
        offset=2,
        origin_offset=2,
        topic=f"colca/v1/_Ack/{NODE_ID}/{SET_PRODUCT}",
        payload={"correlation_id": "corr-1", "result_code": 200, "message": ""},
        ts=time.time() * 1000,
        written_by="svc",
        actor_id="svc",
        actor_label="",
        actor_kind="service",
    )
    door.queue(Page(records=[later], next=3, start=2))  # read ahead
    door.queue(Page(records=[], next=3, start=3))  # read ahead: the head
    door.queue(Page(records=[later], next=3, start=2))  # the cursor's next page
    door.queue(Page(records=[], next=3, start=3))
    await ex.drain()
    assert producer.seen == []
    assert door.published == []
    assert door.fetch_calls[1]["from_offset"] == 2


def test_the_stream_reads_the_answers_too():
    producer = Selection().attach(FakeRuntime(FakeDoor(), buffer=None))
    assert commands.stream_contracts(gather([producer])) == ["_Ack", "_CmdParam"]


# ─── an unconfirmed write is not "nothing changed" ──────────────────────────


class Writer(Producer):
    name = "writer"
    system_element_name = "line1"

    @on_command("line1/operator/setDensity")
    async def set_density(self, command: Command) -> str:
        try:
            raise PublishTimeout("colca/v1/_Metric/n-1/line1/density", 10.0)
        except PublishTimeout as exc:
            raise RuntimeError("could not write the density") from exc


@run_async
async def test_a_write_without_puback_is_answered_504_outcome_unknown():
    door = FakeDoor()
    producer = Writer().attach(FakeRuntime(door, buffer=None))
    door.queue(Page(records=[record("line1/operator/setDensity")], next=2))
    ex = CommandExecutor(door, FlakySend(door, 0), Stream(door, "commands", CURSOR), gather([producer]), NODE_ID)
    await ex.drain()
    ack = acks(door)[0][1]
    assert ack["result_code"] == commands.ACK_UNKNOWN == 504
    assert "may still take effect" in ack["message"]


# ─── the broker link and the command's deadline ───────────────────────────


@run_async
async def test_a_command_waits_for_the_broker_link_before_it_runs():
    ex, producer, door = executor(record())
    ex.link_changed(False)
    drain = asyncio.ensure_future(ex.drain())
    await asyncio.sleep(0.05)
    assert producer.seen == [] and not drain.done()
    ex.link_changed(True)
    await asyncio.wait_for(drain, timeout=1.0)
    assert len(producer.seen) == 1
    assert [ack["result_code"] for _topic, ack in acks(door)] == [200]


@run_async
async def test_a_command_that_expires_while_the_link_is_down_is_answered_498_and_not_run():
    ex, producer, door = executor(record(expires_at=(time.time() + 0.1) * 1000))
    ex.link_changed(False)
    drain = asyncio.ensure_future(ex.drain())
    await asyncio.sleep(0.3)
    assert producer.seen == []
    assert door.published == [], "the answer waits for the link too"
    ex.link_changed(True)
    await asyncio.wait_for(drain, timeout=1.0)
    assert producer.seen == []
    (ack,) = [ack for _topic, ack in acks(door)]
    assert ack["result_code"] == commands.ACK_EXPIRED == 498
    assert "broker link was down" in ack["message"]


def test_a_write_under_a_deadline_is_refused_once_it_passed_or_while_the_link_is_down(tmp_path):
    from chaski.service import NotSent, Service, writes_until

    class Client:
        connected = True

        def __init__(self) -> None:
            self.published: list[str] = []

        def is_connected(self) -> bool:
            return self.connected

        def publish(self, topic, payload, qos=0, retain=False):
            self.published.append(topic)

        def publish_tombstone(self, topic, qos=0):
            self.published.append(topic)

    svc = Service("writer", state_dir=tmp_path)
    client = svc._client = Client()
    with writes_until(time.time() + 30):
        svc.send("t/1", "{}")
        client.connected = False
        with pytest.raises(NotSent, match="link is down"):
            svc.send("t/2", "{}")
        with pytest.raises(NotSent, match="link is down"):
            svc.retract("t/3")
    client.connected = True
    with writes_until(time.time() - 1), pytest.raises(NotSent, match="deadline"):
        svc.send("t/4", "{}")
    # Without a deadline a write is handed to the client as before.
    client.connected = False
    svc.send("t/5", "{}")
    assert client.published == ["t/1", "t/5"]


class DeadlineReader(Producer):
    name = "deadline_reader"
    system_element_name = "line1"
    seen: ClassVar[list] = []

    @on_command("line1/operator/setSpeed")
    async def set_speed(self, command: Command) -> str:
        from chaski import write_deadline

        DeadlineReader.seen.append((write_deadline(), command.expires_at))
        return "ok"


@run_async
async def test_a_handler_reads_its_commands_deadline():
    from chaski import write_deadline

    door = FakeDoor()
    producer = DeadlineReader().attach(FakeRuntime(door, buffer=None))
    door.queue(Page(records=[record("line1/operator/setSpeed")], next=2))
    ex = CommandExecutor(door, producer.runtime.send, Stream(door, "commands", CURSOR), gather([producer]), NODE_ID)
    await ex.drain()
    ((deadline, expires_at),) = DeadlineReader.seen
    assert deadline == expires_at / 1000.0
    assert write_deadline() is None


class LateWriter(Producer):
    name = "late_writer"
    system_element_name = "line1"

    @on_command("line1/operator/setSpeed")
    async def set_speed(self, command: Command) -> str:
        from chaski.service import NotSent

        raise NotSent("colca/v1/_Metric/n-1/line1/speed: not sent, its deadline had passed")


@run_async
async def test_a_refused_write_is_answered_500_not_504():
    door = FakeDoor()
    producer = LateWriter().attach(FakeRuntime(door, buffer=None))
    door.queue(Page(records=[record("line1/operator/setSpeed")], next=2))
    ex = CommandExecutor(door, producer.runtime.send, Stream(door, "commands", CURSOR), gather([producer]), NODE_ID)
    await ex.drain()
    ack = acks(door)[0][1]
    assert ack["result_code"] == commands.ACK_FAILED == 500
    assert "deadline had passed" in ack["message"]
