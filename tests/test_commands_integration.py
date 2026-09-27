"""Against a real node: a command is executed once and answered once, also
when its answer cannot be published at first and across a restart; a write
without a PUBACK is answered 504."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import ssl
import threading
import time
import urllib.request
from typing import ClassVar

import pytest
from dataops_fakes import FakeRuntime
from franzmq.errors import PublishTimeout

import chaski
from chaski.dataops import Command, Producer, commands, on_command
from chaski.dataops.buffer import Buffer
from chaski.failures import HandlerHealth

PATH = "line1/operator/setDensity"
UNKNOWN = "line1/operator/setSandoff"

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


@pytest.fixture
def fast_answer_backoff(monkeypatch):
    monkeypatch.setattr(commands, "ANSWER_BACKOFF_MAX_S", 0.2)


class Operator(Producer):
    name = "operator"
    system_element_name = "line1"
    runs: ClassVar[list[str]] = []
    before_return: object = None

    @on_command(PATH)
    async def set_density(self, command: Command) -> str:
        Operator.runs.append(command.correlation_id)
        if callable(Operator.before_return):
            Operator.before_return()
        return f"density {command.params.get('value')}"

    @on_command(UNKNOWN)
    async def set_sandoff(self, command: Command) -> str:
        Operator.runs.append(command.correlation_id)
        raise RuntimeError("sandoff not stored") from PublishTimeout("colca/v1/_Metric/x", 10.0)


class Executor:
    """A CommandExecutor on its own event loop thread, as the service runs it."""

    def __init__(self, svc, send, ledger, health=None, cursor=commands.CURSOR) -> None:
        producer = Operator().attach(FakeRuntime(svc._require_http("door"), buffer=None))
        handlers = commands.gather([producer])
        self.executor = commands.CommandExecutor(
            svc._require_http("door"),
            send,
            svc.stream(commands.STREAM, cursor=cursor, contracts=commands.stream_contracts(handlers)),
            handlers,
            svc.node_id,
            ledger=ledger,
            health=health,
        )
        self.loop = asyncio.new_event_loop()
        self.stop = asyncio.Event()
        self.executor.subscribe(svc._started_client, self.loop)
        self.thread = threading.Thread(
            target=self.loop.run_until_complete, args=(self.executor.run_forever(self.stop),)
        )
        self.thread.start()

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.stop.set)
        self.thread.join(timeout=15)
        assert not self.thread.is_alive()
        self.loop.close()


@contextlib.contextmanager
def _sender(node, svc, tmp_path):
    """An external service that may send ``param`` commands: local services
    may not, so it is enrolled at ``line1`` with a cmd grant."""
    ack = svc.command("_CmdConfigure", "element/author", {"path": "line1"}, timeout=10)
    assert ack["result_code"] == 200, ack
    element = ack["message"]
    api, mqtt = node._ports["api"], node._ports["mqtt"]
    sender = chaski.Service(
        "sender", "line1", node=f"https://127.0.0.1:{api}", api_port=api, mqtt_port=mqtt, state_dir=tmp_path / "sender"
    )
    body = {
        "ulid": sender.ulid,
        "pubkey": sender.pubkey,
        "kind": "external",
        "element": element,
        "grants": ["cmd:#:param", f"write:{element}/#"],
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
    sender.start()
    try:
        yield sender
    finally:
        sender.close()


def _command(sender, path: str, result: dict, **fields) -> threading.Thread:
    def send() -> None:
        try:
            result.update(sender.command("_CmdParam", path, {"command": fields}, timeout=45))
        except TimeoutError as exc:
            result["error"] = str(exc)

    thread = threading.Thread(target=send)
    thread.start()
    return thread


def _wait(predicate, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.05)


def test_an_answer_that_fails_is_sent_again_and_the_command_runs_once(tmp_path, fast_answer_backoff):
    Operator.runs = []
    with (
        chaski.Node("cmd-ack-retry", data_dir=tmp_path / "node") as node,
        node.service("executor") as svc,
        _sender(node, svc, tmp_path) as sender,
    ):
        attempts = []

        def send(topic: str, payload: str) -> None:
            attempts.append(topic)
            if len(attempts) <= 3:
                raise PublishTimeout(topic, 10.0)
            svc.send(topic, payload)

        health = HandlerHealth()
        runner = Executor(svc, send, Buffer(tmp_path / "ledger.sqlite"), health)
        try:
            result: dict = {}
            _command(sender, PATH, result, value=2.5).join(timeout=50)
            assert result.get("result_code") == 200, result
            assert result["message"] == "density 2.5"
            assert len(attempts) == 4
            assert len(Operator.runs) == 1
            _wait(lambda: health.status == "ok")
        finally:
            runner.close()


def test_a_restart_answers_the_executed_command_once_and_does_not_run_it_again(tmp_path, fast_answer_backoff):
    Operator.runs = []
    with (
        chaski.Node("cmd-restart", data_dir=tmp_path / "node") as node,
        node.service("executor") as svc,
        _sender(node, svc, tmp_path) as sender,
    ):
        result: dict = {}
        waiting = _command(sender, PATH, result, value=3)

        def dead_link(topic: str, payload: str) -> None:
            raise PublishTimeout(topic, 10.0)

        # Round 1 — the answer never gets out; the process stops.
        first = Executor(svc, dead_link, Buffer(tmp_path / "ledger.sqlite"))
        _wait(lambda: len(Operator.runs) == 1)
        first.close()
        assert "result_code" not in result

        # Round 2 — a restart on the same ledger publishes the recorded answer.
        second = Executor(svc, svc.send, Buffer(tmp_path / "ledger.sqlite"))
        try:
            waiting.join(timeout=50)
            assert result.get("result_code") == 200, result
        finally:
            second.close()
        assert len(Operator.runs) == 1

        # Round 3 — the ledger is lost and a new cursor reads the stream
        # from its start: the answer stored next to the command keeps it
        # from running or being answered again.
        answers: list[str] = []

        def record(topic: str, payload: str) -> None:
            answers.append(payload)
            svc.send(topic, payload)

        third = Executor(svc, record, commands.MemoryLedger(), cursor="commands-reread")
        try:
            reread = svc.stream(commands.STREAM, cursor="commands-reread")
            _wait(lambda: reread.fetch().ack_offset is None)
        finally:
            third.close()
        assert len(Operator.runs) == 1
        assert answers == []


def test_a_write_without_puback_is_answered_504(tmp_path):
    Operator.runs = []
    with (
        chaski.Node("cmd-unknown", data_dir=tmp_path / "node") as node,
        node.service("executor") as svc,
        _sender(node, svc, tmp_path) as sender,
    ):
        runner = Executor(svc, svc.send, Buffer(tmp_path / "ledger.sqlite"))
        try:
            result: dict = {}
            _command(sender, UNKNOWN, result, value=1).join(timeout=50)
            assert result.get("result_code") == 504, result
            assert "outcome unknown" in result["message"]
        finally:
            runner.close()


def test_a_command_right_after_a_broker_blip_is_answered(tmp_path):
    """The executor's MQTT link drops while it answers; the answer arrives."""
    Operator.runs = []
    with (
        chaski.Node("cmd-blip", data_dir=tmp_path / "node") as node,
        node.service("executor") as svc,
        _sender(node, svc, tmp_path) as sender,
    ):
        dropped = threading.Event()

        def drop_link() -> None:
            if not dropped.is_set():
                dropped.set()
                svc._started_client.socket().shutdown(socket.SHUT_RDWR)

        Operator.before_return = drop_link
        runner = Executor(svc, svc.send, Buffer(tmp_path / "ledger.sqlite"))
        try:
            result: dict = {}
            _command(sender, PATH, result, value=7).join(timeout=50)
            assert result.get("result_code") == 200, result
            assert dropped.is_set()
            assert len(Operator.runs) == 1
        finally:
            Operator.before_return = None
            runner.close()
