"""``chaski.DataOpsService`` end to end: a service with one ``@on_metric``
producer, fed by a fake door, publishes one computed value and registers at
the node like any :class:`chaski.Service`.

The MQTT side is the fake client from ``test_service_lifecycle.py``. The HTTP
side is a fake door that also plays the node: when the service publishes its
``_DataTags`` catalogue, it writes the bound ``_Signal`` as autobind would.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import ClassVar

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from colca_data_contracts import container_resource_health_metrics
from colca_data_contracts.local_service import LocalServiceIdentity
from colca_data_contracts.payload import ServiceDetails
from dataops_fakes import run_async

from chaski.dataops import DataOpsService, Producer, SignalOutput, SignalRangeInput, on_constant, on_metric, on_signal
from chaski.door import KvEntry, Page, Record
from chaski.service import Service

NODE_ID = "n-edge1"


class _FakeReasonCode:
    is_failure = False


class _FakeMessage:
    """What franzmq hands a ``client.subscribe(..., callback=)`` callback: a
    decoded payload (``None`` for a tombstone), never raw bytes."""

    def __init__(self, payload: object) -> None:
        self.payload = payload


class _FakeClient:
    """franzmq.Client stand-in with the two paho-level attributes the
    doorbell uses (``message_callback_add``, ``_handle_on_message``), plus
    enough of ``subscribe(..., callback=)`` for ``chaski.dataops.watch`` to
    register and fire a typed callback without a real MQTT broker."""

    def __init__(self) -> None:
        self.published: list[tuple[str, object]] = []
        self.subscriptions: list[str] = []
        self.callbacks: dict[str, object] = {}
        self.typed_callbacks: dict[str, object] = {}
        self.on_connect = None
        self.node_id = None

    def loop_start(self) -> None:
        if self.on_connect is not None:
            self.on_connect(self, None, None, _FakeReasonCode())

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:
        self.subscriptions.append(str(topic))
        if callback is not None:
            self.typed_callbacks[str(topic)] = callback

    def deliver(self, topic: str, payload: object) -> None:
        """Simulate a retained/live delivery on ``topic`` to whatever was
        subscribed there with a typed callback (``chaski.dataops.watch``'s
        own subscription style) — ``payload=None`` is a tombstone."""
        self.typed_callbacks[topic](_FakeMessage(payload))

    def message_callback_add(self, sub: str, callback) -> None:
        self.callbacks[sub] = callback

    def message_callback_remove(self, sub: str) -> None:
        self.callbacks.pop(sub, None)

    def unsubscribe(self, topic) -> None:
        self.subscriptions.remove(str(topic))

    def _handle_on_message(self, message) -> None:  # what tolerate_undecodable wraps
        pass

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:
        self.published.append((str(topic), payload))

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:
        pass


class _FakeNodeDoor:
    """The door AND the node behind it, for one service: KV holds the input
    `_Signal`; a published `_DataTags` catalogue is answered by authoring
    the `_Signal` bound to its tag (autobind); the metrics stream serves one
    input record, once, from a real cursor table."""

    instances: ClassVar[list[_FakeNodeDoor]] = []

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

    def fetch(self, stream, cursor, *, max=1000, signal_ids=None):
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


class ReadyAndWatched(Producer):
    """Exercises `on_ready` (publish at startup with no retry loop) and
    `@on_constant`/`@on_signal` (no `@on_metric` equivalent exists for either)."""

    name = "ready_and_watched"
    system_element_name = "oven"

    ready = SignalOutput("ready", "bool", "set once, from on_ready")
    last_constant = SignalOutput("lastConstant", "string", "the constant's own value, or 'retired'")
    last_signal = SignalOutput("lastSignal", "string", "the signal's own id, or 'retired'")

    async def on_ready(self) -> None:
        # Outputs are bound by the time this runs — no RuntimeError, no retry.
        self.ready.publish(True)

    @on_constant("oven/operator/setpoint")
    async def on_setpoint(self, constant) -> None:
        self.last_constant.publish("retired" if constant is None else str(constant.value))

    @on_signal("oven/temperature")
    async def on_temperature_signal(self, signal) -> None:
        self.last_signal.publish("retired" if signal is None else signal.id)


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

    # The ingest wakes on its own input's topic only, on the base class's own
    # client, and the service is a plain local service to the node:
    # registration is CONNECTOR-typed, and close() marked it inactive.
    assert f"colca/v1/_Metric/{NODE_ID}/oven/temperature" in client.subscriptions
    assert not any(t.endswith("/#") and "_Metric" in t for t in client.subscriptions)
    details = [p for _t, p in client.published if isinstance(p, ServiceDetails)]
    assert details[0].name == "dataops" and details[0].service_type.value == "connector"
    assert details[-1].is_active is False


def test_a_dataops_service_registers_byte_identical_to_a_bare_service(tmp_path: Path, monkeypatch):
    """For the same inputs a DataOpsService and a plain Service publish the same
    `_ServiceDetails`, byte for byte, container health metrics included."""
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


def test_discover_adopts_a_producer_built_with_type(tmp_path: Path, monkeypatch):
    # A class built by calling the metaclass names abc as its module. Discovery
    # must still find it, and its code hash must cover the module holding it.
    from chaski.dataops.codehash import compute_code_hash

    monkeypatch.syspath_prepend(str(tmp_path))
    (tmp_path / "builtprods.py").write_text(
        "from chaski.dataops import Producer, every\n"
        "async def tick(self):\n"
        "    pass\n"
        "Built = type(Producer)('Built', (Producer,), {\n"
        "    'name': 'built', 'system_element_name': 'se', 'tick': every('10s')(tick),\n"
        "})\n"
    )
    svc = DataOpsService("dataops", state_dir=tmp_path / "state")
    assert svc.discover("builtprods") == 1
    (built,) = svc.producers
    assert built.name == "built"
    assert built.__module__ == "builtprods"
    assert compute_code_hash(built)


def test_pending_uses_the_buffer_of_unbound_samples(tmp_path: Path) -> None:
    # The SQLite buffer of a DataOpsService is its own attribute, not Service's buffer.
    svc = DataOpsService("dataops", mount="site1", data_dir=tmp_path)
    assert svc.pending() == []


# ─── on_ready: startup compute with outputs already bound ──────────────────


@run_async
async def test_on_ready_runs_after_outputs_are_bound_with_no_retry_needed(tmp_path: Path, monkeypatch):
    client = _FakeClient()
    _connect(client, monkeypatch)
    svc = DataOpsService("dataops", state_dir=tmp_path, data_dir=tmp_path / "data", poll_interval=0.02, health_port=0)
    svc.add(ReadyAndWatched)

    stop = asyncio.Event()
    task = asyncio.ensure_future(svc.serve(stop))
    try:
        await _poll_until(lambda: _FakeNodeDoor.instances and _FakeNodeDoor.instances[0].metrics())
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10.0)

    door = _FakeNodeDoor.instances[0]
    (ready_metric,) = [(t, p) for t, p in door.metrics() if t.endswith("/oven/ready")]
    assert ready_metric[1]["value"] is True


# ─── @on_constant / @on_signal: MQTT-driven, no stream involved ────────────


@run_async
async def test_on_constant_fires_on_write_and_sees_the_tombstone_as_none(tmp_path: Path, monkeypatch):
    from colca_data_contracts.payload import Constant, ConstantDataType

    client = _FakeClient()
    _connect(client, monkeypatch)
    svc = DataOpsService("dataops", state_dir=tmp_path, data_dir=tmp_path / "data", poll_interval=0.02, health_port=0)
    svc.add(ReadyAndWatched)

    topic = f"colca/v1/_Constant/{NODE_ID}/oven/operator/setpoint"
    stop = asyncio.Event()
    task = asyncio.ensure_future(svc.serve(stop))
    try:
        await _poll_until(lambda: topic in client.subscriptions)

        client.deliver(
            topic,
            Constant(id="c-1", name="setpoint", data_type=ConstantDataType.FLOAT64, value=42.0),
        )
        await _poll_until(
            lambda: any(t.endswith("/oven/lastConstant") for t, _ in _FakeNodeDoor.instances[0].metrics())
        )
        first = [p for t, p in _FakeNodeDoor.instances[0].metrics() if t.endswith("/oven/lastConstant")][-1]
        assert first["value"] == "42.0"

        client.deliver(topic, None)  # the wire tombstone: the constant was retired
        await _poll_until(
            lambda: (
                [p for t, p in _FakeNodeDoor.instances[0].metrics() if t.endswith("/oven/lastConstant")][-1]["value"]
                == "retired"
            )
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10.0)

    # Subscribed at qos=1, scoped to this service's own node — and never
    # through the metrics stream: `ReadyAndWatched` declares no
    # `SignalRangeInput`, so nothing was ever fetched at all.
    assert f"colca/v1/_Constant/{NODE_ID}/oven/operator/setpoint" in client.subscriptions
    assert _FakeNodeDoor.instances[0].fetches == []


@run_async
async def test_on_signal_fires_on_binding_change_and_on_release(tmp_path: Path, monkeypatch):
    from colca_data_contracts.payload import Signal as SignalRecord

    client = _FakeClient()
    _connect(client, monkeypatch)
    svc = DataOpsService("dataops", state_dir=tmp_path, data_dir=tmp_path / "data", poll_interval=0.02, health_port=0)
    svc.add(ReadyAndWatched)

    topic = f"colca/v1/_Signal/{NODE_ID}/oven/temperature"
    stop = asyncio.Event()
    task = asyncio.ensure_future(svc.serve(stop))
    try:
        await _poll_until(lambda: topic in client.subscriptions)

        client.deliver(topic, SignalRecord(id="sig-bound", name="temperature"))
        await _poll_until(lambda: any(t.endswith("/oven/lastSignal") for t, _ in _FakeNodeDoor.instances[0].metrics()))
        bound = [p for t, p in _FakeNodeDoor.instances[0].metrics() if t.endswith("/oven/lastSignal")][-1]
        assert bound["value"] == "sig-bound"

        client.deliver(topic, None)  # an integration released the field
        await _poll_until(
            lambda: (
                [p for t, p in _FakeNodeDoor.instances[0].metrics() if t.endswith("/oven/lastSignal")][-1]["value"]
                == "retired"
            )
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10.0)
