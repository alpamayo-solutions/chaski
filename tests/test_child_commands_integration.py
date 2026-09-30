"""Against a real hub and edge: a hub service commands a service on the edge.

The command is addressed to the edge node at its mounted path, needs no
expiry, waits at the hub while the edge is down, and is executed once when the
edge comes back. The sender sees it queued and forwarded, and then its outcome.
"""

from __future__ import annotations

import contextlib
import json
import os
import ssl
import time
import urllib.request

import pytest
from test_commands_integration import PATH, Executor, Operator

import chaski
from chaski.dataops import Producer, commands
from chaski.dataops.buffer import Buffer

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)

MOUNT = "edge7"


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


def _enroll(node: chaski.Node, body: dict) -> None:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    request = urllib.request.Request(
        f"https://127.0.0.1:{node._ports['api']}/enroll",
        data=json.dumps(body).encode(),
        headers={"X-Colca-Token": node.admin_token, "Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(request, context=context).read()  # noqa: S310  # nosec B310


def _element(svc: chaski.Service, path: str) -> str:
    ack = svc.command("_CmdConfigure", "element/author", {"path": path}, lifetime=10, timeout=10)
    assert ack["result_code"] == 200, ack
    return ack["message"]


@contextlib.contextmanager
def _edge(hub: chaski.Node, data_dir):
    edge = chaski.Node("edge", parent=(f"https://127.0.0.1:{hub._ports['repl']}", hub.pubkey), data_dir=data_dir)
    edge.start()
    try:
        yield edge
    finally:
        edge.stop()


@contextlib.contextmanager
def _hub_sender(hub: chaski.Node, hub_svc: chaski.Service, tmp_path):
    """An external service at the hub with a cmd grant over the whole tree."""
    api, mqtt = hub._ports["api"], hub._ports["mqtt"]
    sender = chaski.Service(
        "fleet", "fleet", node=f"https://127.0.0.1:{api}", api_port=api, mqtt_port=mqtt, state_dir=tmp_path / "fleet"
    )
    element = _element(hub_svc, "fleet")
    _enroll(
        hub,
        {
            "ulid": sender.ulid,
            "pubkey": sender.pubkey,
            "kind": "external",
            "element": element,
            "grants": ["cmd:#:param", "read:#", f"write:{element}/#"],
        },
    )
    sender.start()
    try:
        yield sender
    finally:
        sender.close()


def _wait(predicate, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.1)


def test_a_hub_command_waits_for_an_offline_edge_and_runs_once(tmp_path):
    Operator.runs = []
    with chaski.Node("hub", data_dir=tmp_path / "hub") as hub, hub.service("hub-admin") as hub_svc:
        mount_element = _element(hub_svc, MOUNT)
        with _edge(hub, tmp_path / "edge") as edge:
            _enroll(hub, {"ulid": edge.ulid, "pubkey": edge.pubkey, "kind": "node", "element": mount_element})
            edge.wait_enrolled(timeout=60)
            edge_id = edge.ulid
        # The edge is down now. The hub accepts and queues the command.
        with _hub_sender(hub, hub_svc, tmp_path) as sender:
            sent = sender.send_command(
                "_CmdParam", f"{MOUNT}/{PATH}", {"command": {"value": 4}}, lifetime=None, node=edge_id, progress=True
            )
            short = sender.send_command(
                "_CmdParam", f"{MOUNT}/{PATH}", {"command": {"value": 9}}, lifetime=1, node=edge_id
            )
            assert sent.expires_at is None
            with pytest.raises(TimeoutError, match="stays queued"):
                sent.wait(3)
            assert [p["stage"] for p in sent.progress] == ["queued"]

            # The edge comes back; its service executes what waited for it.
            with _edge(hub, tmp_path / "edge") as edge, edge.service("executor") as svc:
                runner = Executor(svc, svc.send, Buffer(tmp_path / "ledger.sqlite"))
                try:
                    outcome = sent.wait(60)
                    expired = short.wait(60)
                finally:
                    runner.close()
            assert outcome["result_code"] == 200, outcome
            assert outcome["message"] == "density 4"
            assert expired["result_code"] == commands.ACK_EXPIRED, expired
            assert Operator.runs == [sent.correlation_id]
            _wait(lambda: "forwarded" in [p["stage"] for p in sent.progress])
