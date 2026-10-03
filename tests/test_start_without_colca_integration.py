"""Against a real node that is not running yet: a connector and a DataOps
service started before their Colca answer their health doors at once with
"not ready" and the reason, do not exit, and start normally once colcad is up.
The connector then publishes, and the DataOps service executes a command it
reads from the node's stream and publishes its output."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import ssl
import threading
import time
import urllib.error
import urllib.request

import pytest
import yaml
from test_connector_refusals_integration import TEMPERATURE, _signal_id, _wait_for_values
from test_connector_service import FakeDriver
from test_connector_writes_integration import _signal_path

import chaski
from chaski.connector import ConnectorService
from chaski.connector import run as run_connector
from chaski.dataops import Command, Producer, SignalOutput, on_command
from chaski.node import Node

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)

SET_SPEED = "line1/line/setSpeed"


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


class Line(Producer):
    name = "line"
    system_element_name = "line1"
    speed = SignalOutput("speed", data_type="float", description="Line speed")

    @on_command(SET_SPEED)
    async def set_speed(self, command: Command) -> str:
        self.speed.publish(float(command.params["value"]))
        return "speed set"


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def fixed_local_doors(monkeypatch):
    """The node's local doors on ports chosen before it starts, so services
    can be pointed at a node that is not up yet: ``(http_port, mqtt_port)``."""
    http_port, mqtt_port = _free_port(), _free_port()
    original = Node._write_config

    def write_config(self):
        original(self)
        doc = yaml.safe_load(self._config_path().read_text(encoding="utf-8"))
        doc["api"]["local_addr"] = f"127.0.0.1:{http_port}"
        doc["mqtt_local"]["addr"] = f"127.0.0.1:{mqtt_port}"
        self._config_path().write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")

    monkeypatch.setattr(Node, "_write_config", write_config)
    return http_port, mqtt_port


def _get(port: int, path: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=2) as response:  # nosec B310
            return response.status, response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def _wait(predicate, timeout: float = 30.0, what: str = "condition"):
    deadline = time.monotonic() + timeout
    while not (result := predicate()):
        assert time.monotonic() < deadline, f"{what} not met in time"
        time.sleep(0.1)
    return result


def _answers(port: int):
    def probe():
        try:
            return _get(port, "/is_healthy")
        except OSError:
            return None

    return probe


@contextlib.contextmanager
def _operator(node: chaski.Node, admin: chaski.Service, tmp_path):
    """An external service enrolled with a grant to send ``param`` commands;
    local services may not send them."""
    ack = admin.command("_CmdConfigure", "element/author", {"path": "ops"}, lifetime=10, timeout=10)
    assert ack["result_code"] == 200, ack
    api, mqtt = node._ports["api"], node._ports["mqtt"]
    operator = chaski.Service(
        "operator", "ops", node=f"https://127.0.0.1:{api}", api_port=api, mqtt_port=mqtt, state_dir=tmp_path / "op"
    )
    body = {
        "ulid": operator.ulid,
        "pubkey": operator.pubkey,
        "kind": "external",
        "element": ack["message"],
        "grants": ["cmd:#:param", "read:#", f"write:{ack['message']}/#"],
    }
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    request = urllib.request.Request(
        f"https://127.0.0.1:{api}/enroll",
        data=json.dumps(body).encode(),
        headers={"X-Colca-Token": node.admin_token, "Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(request, context=context).read()  # noqa: S310  # nosec B310
    with operator.start():
        yield operator


@contextlib.contextmanager
def _running_connector(svc: ConnectorService, health_port: int):
    """The stock entry point, ``chaski.run_connector``, on its own thread."""
    loops: list[asyncio.AbstractEventLoop] = []
    failure: list[BaseException] = []

    def build() -> ConnectorService:
        loops.append(asyncio.get_running_loop())
        return svc

    def run() -> None:
        try:
            run_connector(build, health_port=health_port)
        except BaseException as exc:  # pragma: no cover - reported below
            failure.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        _wait(lambda: loops, what="the connector's loop")
        yield thread
    finally:
        asyncio.run_coroutine_threadsafe(svc.stop(), loops[0]).result(timeout=10)
        thread.join(timeout=20)
    assert not failure, f"the connector ended with {failure[0]!r}"


@contextlib.contextmanager
def _serving_dataops(svc, stop: asyncio.Event):
    loop = asyncio.new_event_loop()
    failure: list[BaseException] = []

    def run() -> None:
        try:
            loop.run_until_complete(svc.serve(stop))
        except BaseException as exc:  # pragma: no cover - reported below
            failure.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        loop.call_soon_threadsafe(stop.set)
        thread.join(timeout=20)
        loop.close()
    assert not failure, f"the DataOps service ended with {failure[0]!r}"


def test_services_started_before_colca_wait_report_not_ready_and_then_run(tmp_path, fixed_local_doors):
    http_port, mqtt_port = fixed_local_doors
    door = chaski.LocalDoor(host="127.0.0.1", http_port=http_port, mqtt_port=mqtt_port)
    connector_health, dataops_health = _free_port(), _free_port()
    connector = ConnectorService(
        "temp-conn",
        "line1",
        driver=FakeDriver(),
        node=door,
        state_dir=tmp_path / "conn",
        interval=0.2,
        heartbeat_interval=1.0,
    )
    dataops = chaski.DataOpsService(
        "dataops",
        node=door,
        state_dir=tmp_path / "dataops-state",
        data_dir=tmp_path / "dataops-data",
        health_port=dataops_health,
    )
    dataops.add(Line)
    node = chaski.Node("late-colca", data_dir=tmp_path / "node")

    # Unwound in reverse: both services stop while their node still runs.
    with contextlib.ExitStack() as stack:
        stack.callback(node.stop)
        connector_thread = stack.enter_context(_running_connector(connector, connector_health))
        dataops_thread = stack.enter_context(_serving_dataops(dataops, asyncio.Event()))

        # Both health doors answer at once: not ready, and why.
        started = time.monotonic()
        status, body = _wait(_answers(connector_health), timeout=5, what="the connector's health door")
        assert status == 503, body
        assert "not ready" in body, body
        status, body = _wait(_answers(dataops_health), timeout=5, what="the DataOps health door")
        assert status == 503, body
        _wait(lambda: "cannot reach Colca" in _get(connector_health, "/is_healthy")[1], timeout=10)
        _wait(lambda: "cannot reach Colca" in _get(dataops_health, "/healthz")[1], timeout=10)
        snapshot = json.loads(_get(dataops_health, "/healthz")[1])
        assert snapshot["ok"] is False and snapshot["ingest"] == "starting", snapshot
        assert f"127.0.0.1 (http :{http_port}, mqtt :{mqtt_port})" in snapshot["not_ready"], snapshot
        assert time.monotonic() - started < 15

        # Colca stays away for several retries; neither process gives up.
        _wait(lambda: connector.readiness.attempts >= 3 and dataops.readiness.attempts >= 3, timeout=30)
        assert connector_thread.is_alive() and dataops_thread.is_alive()
        assert connector.readiness.state == dataops.readiness.state == "waiting"
        assert connector.driver.connects == 0, "the source was touched before the node answered"

        node.start()
        admin = stack.enter_context(node.service("admin"))
        # Both come up within the retry ceiling of the node answering.
        _wait(lambda: _get(connector_health, "/is_healthy")[0] == 200, timeout=30, what="connector healthy")
        _wait(lambda: dataops.instances, timeout=30, what="DataOps producers running")
        _wait(lambda: _get(dataops_health, "/healthz")[0] == 200, timeout=30, what="DataOps healthy")
        assert connector.readiness.ready and dataops.readiness.ready

        # The connector publishes normally.
        _wait(lambda: connector._catalogue is not None and connector._catalogue.tag_id(TEMPERATURE), timeout=30)
        signal_id = _signal_id(admin, _signal_path(admin, connector._catalogue.tag_id(TEMPERATURE)))
        _wait_for_values(admin, signal_id, [42.0], cursor="late")

        # The DataOps service consumes its command stream and publishes.
        with _operator(node, admin, tmp_path) as operator:
            ack = operator.command("_CmdParam", SET_SPEED, {"command": {"value": 7.5}}, lifetime=30, timeout=30)
        assert ack["result_code"] == 200, ack
        speed = _signal_id(admin, _signal_path(admin, dataops.instances[0].speed.tag_id))
        _wait_for_values(admin, speed, [7.5], cursor="speed")

        assert connector_thread.is_alive() and dataops_thread.is_alive()
