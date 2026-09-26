"""The output catalogue across DataOpsService restarts.

A fake node keeps what the service publishes retained and answers the
``_DataTags`` read ``start()`` makes, so start/close/start runs the same path
as a deployment: the run's catalogue is the one the node keeps, and every
output keeps its tag id on the next start.
"""

from __future__ import annotations

import json
from pathlib import Path

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from colca_data_contracts.local_service import LocalServiceIdentity
from colca_data_contracts.payload import DataTags
from dataops_fakes import kv_entry

from chaski.dataops.base import Producer
from chaski.dataops.outputs import SignalOutput
from chaski.dataops.service import DataOpsService


class _ReasonCode:
    is_failure = False


class _Node:
    """The retained records a node keeps, written by the fake MQTT client and
    read back through the fake door."""

    def __init__(self) -> None:
        self.retained: dict[str, dict] = {}
        self.catalogue_publishes = 0

    def catalogue(self) -> dict:
        (payload,) = (p for t, p in self.retained.items() if "/_DataTags/" in t)
        return payload


class _Client:
    def __init__(self, node: _Node) -> None:
        self.node = node
        self.on_connect = None
        self.on_disconnect = None

    def _handle_on_message(self, message) -> None:
        pass

    def loop_start(self) -> None:
        if self.on_connect is not None:
            self.on_connect(self, None, None, _ReasonCode())

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:
        pass

    def unsubscribe(self, topic) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:
        if isinstance(payload, DataTags):
            self.node.catalogue_publishes += 1
            self.node.retained[str(topic)] = json.loads(payload.encode())


class _Door:
    def __init__(self, node: _Node) -> None:
        self.node = node

    def kv(self, prefix: str = "", *, contract=None):
        return [kv_entry(t, p) for t, p in self.node.retained.items() if contract is None or f"/{contract}/" in t]

    def close(self) -> None:
        pass


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


def _producer(**outputs) -> Producer:
    cls = type("_Press", (Producer,), {"name": "press", **outputs})
    return cls()


def _run(node: _Node, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **outputs) -> dict[str, str]:
    """One run of the service: start, catalogue the outputs, close."""
    identity = LocalServiceIdentity(
        service_id="svc-ulid",
        service_name="dataops",
        node_id="n-edge1",
        system_element_id="el-1",
        mount="line1",
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", lambda *a, **k: (_Client(node), identity))
    monkeypatch.setattr("chaski.service.Door", lambda *a, **k: _Door(node))
    service = DataOpsService("dataops", "line1", data_dir=tmp_path, state_dir=tmp_path)
    service.start()
    try:
        return service.bind_outputs([_producer(**outputs).attach(service)])
    finally:
        service.close()


def test_every_output_keeps_its_tag_id_across_restarts(tmp_path, monkeypatch):
    """An output added in a run keeps its id on the next start. The shutdown
    used to republish the catalogue read at start, which did not have it, so
    the next start minted a new id and the node bound a second signal."""
    node = _Node()
    run1 = _run(node, tmp_path, monkeypatch, a=SignalOutput("a", "float"))
    run2 = _run(node, tmp_path, monkeypatch, a=SignalOutput("a", "float"), b=SignalOutput("b", "float"))
    run3 = _run(node, tmp_path, monkeypatch, a=SignalOutput("a", "float"), b=SignalOutput("b", "float"))

    assert run2["press.a"] == run1["press.a"]
    assert run3 == run2, "an output added in run 2 must keep its tag id in run 3"


def test_close_leaves_the_runs_catalogue_on_the_node(tmp_path, monkeypatch):
    node = _Node()
    _run(node, tmp_path, monkeypatch, a=SignalOutput("a", "float"))
    publishes = node.catalogue_publishes
    ids = _run(node, tmp_path, monkeypatch, a=SignalOutput("a", "float"), b=SignalOutput("b", "float"))

    assert node.catalogue_publishes == publishes + 1, "the run publishes its catalogue once, and close() not at all"
    tags = {t["source"]: t for t in node.catalogue()["data_tags"]}
    assert {s: t["id"] for s, t in tags.items()} == ids
    assert not any(t["is_stale"] for t in tags.values()), "a shutdown marks no declared output stale"


def test_an_unchanged_catalogue_is_not_republished_on_restart(tmp_path, monkeypatch):
    node = _Node()
    _run(node, tmp_path, monkeypatch, a=SignalOutput("a", "float", unit="%", semantic_type="availability"))
    _run(node, tmp_path, monkeypatch, a=SignalOutput("a", "float", unit="%", semantic_type="availability"))

    assert node.catalogue_publishes == 1
    (tag,) = node.catalogue()["data_tags"]
    assert tag["meta"] == {"unit": "%", "semantic_type": "availability"}
