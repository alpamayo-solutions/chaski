"""Against a real node: a signal write reaches the connector that holds the
signal's binding, is written and read back, and its answer carries the value
read. The sender waits for that answer; nothing is answered earlier."""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import time

import pytest
from test_child_commands_integration import _element, _enroll
from test_connector_writes import SETPOINT, WritableDriver

import chaski
from chaski.connector import ConnectorService
from chaski.dataops import Producer

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door rather than the unit suite's default fake."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """The integration caller supplies the bundle matching its binary."""


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


@contextlib.contextmanager
def _connector(node: chaski.Node, driver, tmp_path):
    door = chaski.LocalDoor(host="127.0.0.1", http_port=node._ports["api_local"], mqtt_port=node._ports["mqtt_local"])
    svc = ConnectorService("press-conn", "line1", driver=driver, node=door, state_dir=tmp_path / "conn", interval=0.2)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_until_complete, args=(svc.serve(),), daemon=True)
    thread.start()
    try:
        yield svc
    finally:
        asyncio.run_coroutine_threadsafe(svc.stop(), loop).result(timeout=10)
        thread.join(timeout=20)
        loop.close()


def _signal_path(svc: chaski.Service, tag_id: str, timeout: float = 30.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for entry in svc.kv(contract="_Signal"):
            if isinstance(entry.payload, dict) and entry.payload.get("data_tag") == tag_id:
                return entry.path
        time.sleep(0.2)
    raise AssertionError(f"no signal bound to tag {tag_id}")


@contextlib.contextmanager
def _operator_and_signal(tmp_path, driver):
    """A node, a connector holding one writable signal, and an enrolled
    external operator service allowed to write it: ``(operator, path)``."""
    with (
        chaski.Node("writes", data_dir=tmp_path / "node") as node,
        node.service("admin") as admin,
        _connector(node, driver, tmp_path) as connector,
    ):
        deadline = time.monotonic() + 30
        while connector._catalogue is None or connector._catalogue.tag_id(SETPOINT) is None:
            assert time.monotonic() < deadline, "the connector never catalogued its tags"
            time.sleep(0.1)
        tag_id = connector._catalogue.tag_id(SETPOINT)
        # The node binds a new connector's tags itself (autobind on_new_connector).
        path = _signal_path(admin, tag_id)

        api, mqtt = node._ports["api"], node._ports["mqtt"]
        operator = chaski.Service(
            "operator", "ops", node=f"https://127.0.0.1:{api}", api_port=api, mqtt_port=mqtt, state_dir=tmp_path / "op"
        )
        element = _element(admin, "ops")
        _enroll(
            node,
            {
                "ulid": operator.ulid,
                "pubkey": operator.pubkey,
                "kind": "external",
                "element": element,
                "grants": ["cmd:#:param", "read:#", f"write:{element}/#"],
            },
        )
        operator.start()
        try:
            yield operator, path
        finally:
            operator.close()


def _first_answer(operator: chaski.Service, path: str, value, **kwargs) -> dict:
    """The first answer that is not the 404 a write gets before the connector
    announced its route."""
    deadline = time.monotonic() + 30
    while True:
        answer = operator.write_signal(path, value, lifetime=10, **kwargs).wait(15)
        if answer["result_code"] != 404 or time.monotonic() > deadline:
            return answer
        time.sleep(0.5)


def test_a_signal_write_is_answered_after_the_read_back(tmp_path):
    driver = WritableDriver()
    with _operator_and_signal(tmp_path, driver) as (operator, path):
        answer = _first_answer(operator, path, 12.5)
        assert answer["result_code"] == 200, answer
        assert answer["result"] == {"outcome": "applied", "value": 12.5}
        assert driver.writes == [(SETPOINT, 12.5)]

        driver.applies = lambda value: min(value, 20.0)
        clamped = operator.write_signal(path, 99.0, lifetime=10).wait(15)
        assert clamped["result_code"] == 409, clamped
        assert clamped["result"]["value"] == 20.0


def test_a_write_carries_its_sender_person_and_operation_and_is_done_once(tmp_path):
    """The driver sees the node-attested sender, the person it acts for and the
    operation; a retry of the same operation (a new correlation id) is answered
    from the connector's record without writing the source again."""
    driver = WritableDriver()
    anna = {"id": "sub-anna", "label": "anna"}
    with _operator_and_signal(tmp_path, driver) as (operator, path):
        first = _first_answer(operator, path, 12.5, operation_id="op-1", on_behalf_of=anna, params={"note": "n"})
        assert first["result_code"] == 200, first
        assert first["operation_id"] == "op-1"
        assert first["on_behalf_of"] == {**anna, "kind": "human"}
        assert "replayed" not in first

        command = driver.commands[-1]
        assert command.sender.id == operator.ulid
        assert command.sender.kind == "service"
        assert command.on_behalf_of == chaski.Actor("sub-anna", "anna")
        assert (command.operation_id, command.params["note"]) == ("op-1", "n")
        assert command.correlation_id == first["correlation_id"]

        repeat = operator.write_signal(
            path, 12.5, lifetime=10, operation_id="op-1", on_behalf_of=anna, params={"note": "n"}
        ).wait(15)
        assert repeat["correlation_id"] != first["correlation_id"]
        assert (repeat["result_code"], repeat["result"], repeat["replayed"]) == (200, first["result"], True)

        other = operator.write_signal(path, 13.0, lifetime=10, operation_id="op-1", on_behalf_of=anna).wait(15)
        assert (other["result_code"], other["result"]) == (409, {"outcome": "refused"})

        assert driver.writes == [(SETPOINT, 12.5)]
