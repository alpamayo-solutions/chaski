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


def test_a_signal_write_is_answered_after_the_read_back(tmp_path):
    driver = WritableDriver()
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
            deadline = time.monotonic() + 30
            while True:
                answer = operator.write_signal(path, 12.5, lifetime=10).wait(15)
                if answer["result_code"] != 404 or time.monotonic() > deadline:
                    break  # 404 until the connector announced the route
                time.sleep(0.5)
            assert answer["result_code"] == 200, answer
            assert answer["result"] == {"outcome": "applied", "value": 12.5}
            assert driver.writes == [(SETPOINT, 12.5)]

            driver.applies = lambda value: min(value, 20.0)
            clamped = operator.write_signal(path, 99.0, lifetime=10).wait(15)
            assert clamped["result_code"] == 409, clamped
            assert clamped["result"]["value"] == 20.0
        finally:
            operator.close()
