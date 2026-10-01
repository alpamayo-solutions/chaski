"""Signal writes on ``chaski.ConnectorService``: a ``_CmdParam`` at a bound
signal's path is written through the driver, read back, and answered with
what was read. Driven with the fake node and driver of the connector tests."""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any

import pytest
from colca_data_contracts.payload import ServiceDetails
from test_connector_service import FakeDriver, FakeNode, bind, started

from chaski.command import Actor
from chaski.connector import WRITE_CONTRACT, ConnectorService, Discovery, Driver, SignalWrite, SourceDisconnectedError
from chaski.executor import Command, CommandRejected, CommandResult

SETPOINT = "Axis1/Setpoint"
PATH = "line1/press3/setpoint"


class WritableDriver(FakeDriver):
    """The fake source with one writable tag. ``applies`` is what the source
    stores for a written value (a PLC that clamps), ``refuse`` makes the
    write fail, ``lose_read_back`` the read after it."""

    def __init__(self) -> None:
        super().__init__()
        self.tags[SETPOINT] = {"name": "Setpoint", "data_type": "float"}
        self.values[SETPOINT] = 10.0
        self.writes: list[tuple[str, Any]] = []
        self.commands: list[Command] = []
        self.applies = lambda value: value
        self.refuse: Exception | None = None
        self.lose_read_back = False

    async def discover(self) -> Discovery:
        discovery = await super().discover()
        tags = dict(discovery.tags)
        tags[SETPOINT] = dataclasses.replace(tags[SETPOINT], is_writable=True)
        return Discovery(tags=tags, handles=discovery.handles)

    async def write(self, target, value: Any, command: Command) -> None:
        if self.refuse is not None:
            raise self.refuse
        self.writes.append((target.handle, value))
        self.commands.append(command)
        self.values[target.handle] = self.applies(value)
        if self.lose_read_back:
            self.fail_reads = True


def write(
    svc, path: str, value: Any = None, *, params: dict | None = None, **command_fields: Any
) -> tuple[int, str, dict | None]:
    handler = svc._writes[(WRITE_CONTRACT, path)]
    command = Command(
        path=path,
        verb=path.rsplit("/", 1)[-1],
        contract=WRITE_CONTRACT,
        params=params if params is not None else {"value": value},
        **command_fields,
    )
    try:
        result = asyncio.run(handler(command))
    except CommandRejected as exc:
        return exc.code, exc.message, exc.result
    assert isinstance(result, CommandResult)
    return 200, result.message, dict(result.result)


@pytest.fixture(autouse=True)
def durable_state(tmp_path, monkeypatch):
    monkeypatch.setenv("COLCA_STATE_DIR", str(tmp_path))


@pytest.fixture
def node() -> FakeNode:
    return FakeNode()


@pytest.fixture
def writable() -> WritableDriver:
    return WritableDriver()


def test_a_bound_signal_is_written_read_back_and_answered_with_what_was_read(node, writable, monkeypatch):
    svc = started(node, writable, monkeypatch)
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")

    code, _message, result = write(svc, PATH, 12.5)

    assert writable.writes == [(SETPOINT, 12.5)]
    assert (code, result) == (200, {"outcome": "applied", "value": 12.5})


def test_a_value_the_source_did_not_keep_is_answered_failed_with_the_value_read(node, writable, monkeypatch):
    svc = started(node, writable, monkeypatch)
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")
    writable.applies = lambda value: min(value, 20.0)  # the PLC clamps

    code, message, result = write(svc, PATH, 99.0)

    assert code == 409 and "read back 20.0" in message
    assert result == {"outcome": "failed", "requested": 99.0, "value": 20.0}


def test_a_refused_write_is_failed_and_a_lost_read_back_is_unknown(node, writable, monkeypatch):
    svc = started(node, writable, monkeypatch)
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")

    writable.refuse = RuntimeError("BadNotWritable")
    code, _message, result = write(svc, PATH, 1.0)
    assert (code, result["outcome"]) == (502, "failed")

    writable.refuse = SourceDisconnectedError("link down")
    code, _message, result = write(svc, PATH, 1.0)
    assert (code, result["outcome"]) == (503, "failed")

    writable.refuse = None
    writable.lose_read_back = True
    code, message, result = write(svc, PATH, 3.0)
    assert (code, result["outcome"]) == (504, "unknown")
    assert "reading it back failed" in message


def test_a_read_only_tag_or_a_command_without_value_is_refused(node, writable, monkeypatch):
    svc = started(node, writable, monkeypatch)
    bind(node, svc, "Axis1/Temperature", path="line1/press3/temp", signal_id="01JSIG2")
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")

    code, _message, result = write(svc, "line1/press3/temp", 5.0)
    assert (code, result) == (422, {"outcome": "refused"})
    code, _message, result = write(svc, PATH, params={})
    assert (code, result) == (422, {"outcome": "refused"})
    assert writable.writes == []


def test_a_driver_that_does_not_write_answers_unsupported(node, monkeypatch):
    class ReadOnly(WritableDriver):
        write = Driver.write  # the default

    driver = ReadOnly()
    svc = started(node, driver, monkeypatch)
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")

    code, message, result = write(svc, PATH, 1.0)

    assert (code, result) == (501, {"outcome": "unsupported"})
    assert "does not write" in message


def test_every_bound_signal_is_announced_and_other_routes_are_kept(node, writable, monkeypatch):
    svc = started(node, writable, monkeypatch)
    svc.announce_commands([("_CmdOperate", "line1/press3/reset")])
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")
    asyncio.run(svc._announce_writes())

    details = [p for t, p in node.published if isinstance(p, ServiceDetails)]
    routes = {(c["contract"], c["path"]) for c in details[-1].__dict__["commands"]}
    assert routes == {("_CmdOperate", "line1/press3/reset"), (WRITE_CONTRACT, PATH)}
    assert all("heartbeat" not in path and "is_connected" not in path for _c, path in routes)


def test_the_write_ledger_survives_a_restart(tmp_path):
    """A write started and not answered is still known after a restart, so the
    executor answers it 504 instead of writing again."""
    from chaski.executor import SqliteLedger

    ledger = SqliteLedger(tmp_path / "writes.sqlite3")
    ledger.command_started("c-1")
    ledger.command_started("c-2")
    ledger.command_answered("c-2", '{"result_code": 200}')
    ledger.close()

    again = SqliteLedger(tmp_path / "writes.sqlite3")
    assert again.command_entry("c-1") == (True, None)
    assert again.command_entry("c-2") == (True, '{"result_code": 200}')
    assert again.command_entry("c-3") == (False, None)
    again.close()


def test_the_driver_gets_the_command_with_its_sender_person_and_operation(node, writable, monkeypatch):
    svc = started(node, writable, monkeypatch)
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")

    code, _message, _result = write(
        svc,
        PATH,
        params={"value": 12.5, "note": "night shift"},
        correlation_id="c-1",
        expires_at=1e15,
        actor_id="01JHMI",
        actor_label="hmi-api",
        actor_kind="service",
        operation_id="op-1",
        on_behalf_of=Actor("sub-anna", "anna"),
    )

    assert code == 200
    (command,) = writable.commands
    assert command.sender == Actor("01JHMI", "hmi-api", "service")
    assert command.on_behalf_of == Actor("sub-anna", "anna", "human")
    assert (command.operation_id, command.correlation_id, command.expires_at) == ("op-1", "c-1", 1e15)
    assert command.params["note"] == "night shift"


class RecipeConnector(ConnectorService):
    """A connector that takes over its writes through the public hook: only
    one sender, a value derived from several command fields, and its own
    field in every answer."""

    async def handle_signal_write(self, write: SignalWrite) -> CommandResult:
        if write.command.sender.label != "hmi-api":
            raise CommandRejected(403, "refused: only the HMI writes", {"outcome": "refused"})
        values = write.command.params.get("values")
        journal = {"operation_id": write.command.operation_id, "by": write.command.on_behalf_of.id}
        try:
            answer = await write.apply(value=sum(values))
        except CommandRejected as rejected:
            raise CommandRejected(
                rejected.code, rejected.message, {**(rejected.result or {}), "journal": journal}
            ) from None
        return CommandResult(answer.message, {**answer.result, "journal": journal})


def test_a_connector_takes_over_a_write_through_the_public_hook(node, writable, monkeypatch):
    svc = started(node, writable, monkeypatch, connector=RecipeConnector)
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")
    hmi = {"actor_id": "01JHMI", "actor_label": "hmi-api", "actor_kind": "service"}
    anna = Actor("sub-anna", "anna")

    code, _message, result = write(
        svc, PATH, params={"values": [5.0, 7.5]}, operation_id="op-1", on_behalf_of=anna, **hmi
    )
    assert (code, result) == (
        200,
        {"outcome": "applied", "value": 12.5, "journal": {"operation_id": "op-1", "by": "sub-anna"}},
    )
    assert writable.writes == [(SETPOINT, 12.5)]

    writable.applies = lambda value: min(value, 10.0)
    code, _message, result = write(svc, PATH, params={"values": [20.0]}, operation_id="op-2", on_behalf_of=anna, **hmi)
    assert code == 409
    assert result == {
        "outcome": "failed",
        "requested": 20.0,
        "value": 10.0,
        "journal": {"operation_id": "op-2", "by": "sub-anna"},
    }

    code, _message, result = write(svc, PATH, params={"values": [1.0]}, actor_label="someone-else")
    assert (code, result) == (403, {"outcome": "refused"})
    assert len(writable.writes) == 2


def test_apply_without_a_value_is_refused_without_writing(node, writable, monkeypatch):
    svc = started(node, writable, monkeypatch)
    bind(node, svc, SETPOINT, path=PATH, signal_id="01JSIG1")

    code, message, result = write(svc, PATH, params={"values": [1.0]})

    assert (code, result) == (422, {"outcome": "refused"})
    assert "no value" in message
    assert writable.writes == []
