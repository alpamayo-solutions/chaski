"""Level-2 pin for ``chaski.DataOpsService`` as a whole (service families
design §3.5): a minimal user-defined service with one
``@on_metric`` producer, fed by a fake door, publishes one computed value —
and it is a :class:`chaski.Service` to the node, not a kind of its own (§3.1).

Hermetic: the MQTT side is the same fake client and monkeypatched connection
functions ``test_service_lifecycle.py`` uses (plus the two paho attributes
the doorbell touches); the HTTP side is a fake door installed in place of
``chaski.service.Door`` that also plays the node's part in the commissioning
act — when the service publishes its ``_DataTags`` catalogue, the fake
authors the bound ``_Signal`` the way ``signal/autobind`` would.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from dataops_fakes import run_async
from colca_data_contracts import container_resource_health_metrics
from colca_data_contracts.local_service import LocalServiceIdentity
from colca_data_contracts.payload import ServiceDetails

from chaski.dataops import DataOpsService, Producer, SignalOutput, SignalRangeInput, on_metric
from chaski.door import KvEntry, Page, Record
from chaski.service import Service

NODE_ID = "n-edge1"


class _FakeReasonCode:
    is_failure = False


class _FakeClient:
    """franzmq.Client stand-in with the two paho-level attributes the
    doorbell uses (``message_callback_add``, ``_handle_on_message``)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, object]] = []
        self.subscriptions: list[str] = []
        self.callbacks: dict[str, object] = {}
        self.on_connect = None
        self.node_id = None

    def loop_start(self) -> None:
        if self.on_connect is not None:
            self.on_connect(self, None, None, _FakeReasonCode())

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:  # noqa: ARG002
        self.subscriptions.append(str(topic))

    def message_callback_add(self, sub: str, callback) -> None:
        self.callbacks[sub] = callback

    def _handle_on_message(self, message) -> None:  # what ring_even_if_undecodable wraps
        pass

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:  # noqa: ARG002
        self.published.append((str(topic), payload))

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:  # noqa: ARG002
        pass


class _FakeNodeDoor:
    """The door AND the node behind it, for one service: KV holds the input
    `_Signal`; a published `_DataTags` catalogue is answered by authoring
    the `_Signal` bound to its tag (autobind); the metrics stream serves one
    input record, once, from a real cursor table."""

    instances: list["_FakeNodeDoor"] = []

    def __init__(self, base_url: str, service: str, *, timeout: float = 10.0, cert=None) -> None:
        self.service = service
        self.entries: list[KvEntry] = [
            KvEntry(
                path="oven/temperature",
                node_id=NODE_ID,
                topic=f"colca/v1/_Signal/{NODE_ID}/oven/temperature",
                payload={"id": "sig-in", "name": "temperature"},
                ts=0.0,
                offset=1,
            ),
        ]
        self.published: list[tuple[str, dict]] = []
        self.records: list[Record] = [
            Record(
                offset=1,
                origin_offset=1,
                topic=f"colca/v1/_Metric/{NODE_ID}/oven/temperature",
                payload={"signal_id": "sig-in", "value": 21.5, "timestamp": 1_700_000_000.0},
                ts=1_700_000_000_000.0,
                written_by="connector",
                actor_id="c",
                actor_label="c",
                actor_kind="local",
            ),
        ]
        self.cursors: dict[str, int] = {}
        self.acks: list[tuple[str, str, int]] = []
        self.fetches: list[tuple[str, str, list[str] | None]] = []
        type(self).instances.append(self)

    def close(self) -> None:
        pass

    def kv(self, prefix="", *, contract=None):
        return list(self.entries)

    def publish(self, topic: str, payload: str) -> None:
        body = json.loads(payload)
        self.published.append((topic, body))
        if "/_DataTags/" in topic:
            # The node's part of the commissioning act: bind the catalogue.
            for tag in body["data_tags"]:
                self.entries.append(
                    KvEntry(
                        path=f"oven/{tag['name']}",
                        node_id=NODE_ID,
                        topic=f"colca/v1/_Signal/{NODE_ID}/oven/{tag['name']}",
                        payload={"id": "sig-out", "name": tag["name"], "data_tag": tag["id"], "is_published": True},
                        ts=0.0,
                        offset=2,
                    )
                )

    def fetch(self, stream, cursor, *, max=1000, signal_ids=None):  # noqa: A002
        self.fetches.append((stream, cursor, signal_ids))
        position = self.cursors.get(cursor, 0)
        records = [
            r
            for r in self.records
            if r.offset > position and (signal_ids is None or r.payload["signal_id"] in signal_ids)
        ][:max]
        return Page(records=records, next=(records[-1].offset + 1) if records else position + 1)

    def ack(self, stream, cursor, offset) -> bool:
        self.acks.append((stream, cursor, offset))
        moved = offset > self.cursors.get(cursor, 0)
        if moved:
            self.cursors[cursor] = offset
        return moved

    def delete_cursor(self, stream, cursor) -> None:
        self.cursors.pop(cursor, None)

    def metrics(self) -> list[tuple[str, dict]]:
        return [(t, p) for t, p in self.published if "/_Metric/" in t]


class Doubler(Producer):
    """The whole of what a user writes."""

    name = "doubler"
    system_element_name = "oven"

    temperature = SignalRangeInput("temperature", window="10m")
    doubled = SignalOutput("doubled", "float", "temperature x 2")

    @on_metric("temperature")
    async def recompute(self, metric) -> None:
        self.doubled.publish(metric.value * 2, metric.timestamp)
        self.advance_watermark(metric.timestamp)


@pytest.fixture(autouse=True)
def _fake_node(monkeypatch):
    identity = LocalServiceIdentity(
        service_id="svc-ulid",
        service_name="dataops",
        node_id=NODE_ID,
        system_element_id="",
        mount="",
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)
    monkeypatch.setattr("chaski.service.Door", _FakeNodeDoor)
    _FakeNodeDoor.instances.clear()
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


def _connect(client: _FakeClient, monkeypatch):
    def _connect_local_mqtt(name, **kwargs):
        return client, kwargs["identity"]

    monkeypatch.setattr("chaski.service.connect_local_mqtt", _connect_local_mqtt)


async def _poll_until(predicate, timeout: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met in time")


@run_async
async def test_one_on_metric_producer_publishes_one_computed_value(tmp_path: Path, monkeypatch):
    client = _FakeClient()
    _connect(client, monkeypatch)
    svc = DataOpsService("dataops", state_dir=tmp_path, data_dir=tmp_path / "data", poll_interval=0.02, health_port=0)
    svc.add(Doubler)

    stop = asyncio.Event()
    task = asyncio.ensure_future(svc.serve(stop))
    try:
        await _poll_until(lambda: _FakeNodeDoor.instances and _FakeNodeDoor.instances[0].metrics())
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10.0)

    door = _FakeNodeDoor.instances[0]

    # The catalogue went out first, once, at the service's own position (unplaced: node root).
    catalogues = [(t, p) for t, p in door.published if "/_DataTags/" in t]
    assert [t for t, _ in catalogues] == [f"colca/v1/_DataTags/{NODE_ID}/dataops"]
    (tag,) = catalogues[0][1]["data_tags"]
    assert (tag["source"], tag["name"], tag["data_type"]) == ("doubler.doubled", "doubled", "float")

    # Exactly one computed value, at the bound Signal's own position, twice the input.
    assert door.metrics() == [
        (
            f"colca/v1/_Metric/{NODE_ID}/oven/doubled",
            {"signal_id": "sig-out", "value": 43.0, "timestamp": 1_700_000_000.0},
        )
    ]

    # It came in through the SDK's consume lane: the generational cursor
    # inside this identity's namespace, filtered to the declared input, and
    # acked after the page was processed.
    generation = door.fetches[0][1].split("ingest-")[1]
    assert door.fetches[0] == ("metrics", f"c/dataops/ingest-{generation}", ["sig-in"])
    assert door.acks[0] == ("metrics", f"c/dataops/ingest-{generation}", 1)

    # The buffer is the only local state: the point landed, the watermark advanced, in data_dir.
    assert (tmp_path / "data" / "buffer.sqlite3").exists()

    # The doorbell rang on the base class's own client, and the service is a
    # plain local service to the node: registration is CONNECTOR-typed, and
    # close() marked it inactive like any Service.
    assert "colca/v1/_Metric/#" in client.subscriptions
    details = [p for _t, p in client.published if isinstance(p, ServiceDetails)]
    assert details[0].name == "dataops" and details[0].service_type.value == "connector"
    assert details[-1].is_active is False


def test_a_dataops_service_registers_byte_identical_to_a_bare_service(tmp_path: Path, monkeypatch):
    """The mechanical half of §3.1: a family is SDK ergonomics, never a
    node-side kind. For the same inputs a DataOpsService and a bare Service
    publish the same `_ServiceDetails`, byte for byte — including when the
    deployment declares container health metrics, which is an input, not a
    family trait."""
    metrics = container_resource_health_metrics()

    bare_client = _FakeClient()
    _connect(bare_client, monkeypatch)
    bare = Service("dataops", state_dir=tmp_path / "bare", health_metrics=metrics)
    bare.start()

    family_client = _FakeClient()
    _connect(family_client, monkeypatch)
    family = DataOpsService("dataops", state_dir=tmp_path / "family", health_metrics=metrics)
    family.start()
    family.close()
    bare.close()

    def details(client):
        return [p.encode() for _t, p in client.published if isinstance(p, ServiceDetails)]

    assert details(bare_client), "denominator: the bare service published a registration"
    assert details(family_client) == details(bare_client)
    assert json.loads(details(bare_client)[0])["health_metrics"], "the declared metrics travelled"


def test_add_refuses_a_non_producer_and_discover_adopts_a_package(tmp_path: Path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "userprods").mkdir()
    (tmp_path / "userprods" / "__init__.py").write_text("")
    (tmp_path / "userprods" / "p1.py").write_text(
        "from chaski.dataops import Producer, every\n"
        "class P1(Producer):\n"
        "    name = 'p1'\n"
        "    system_element_name = 'se'\n"
        "    @every('10s')\n"
        "    async def tick(self):\n"
        "        pass\n"
    )
    svc = DataOpsService("dataops", state_dir=tmp_path / "state")
    with pytest.raises(TypeError):
        svc.add(object)  # type: ignore[arg-type]
    assert svc.discover("userprods") == 1
    assert [p.name for p in svc.producers] == ["p1"]
    assert svc.discover("userprods") == 0, "adopting the same package twice adds nothing"
    assert svc.discover("no.such.package") == 0
