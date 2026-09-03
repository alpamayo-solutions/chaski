"""Level-2 pin for Service's signal-binding logic (hermetic — a fake MQTT
client, no broker) — SDK design §9's "buffered sample flushes on bind".

The node's default placement binds a Signal at ``{connector's own
mount}/{tag's sanitized NAME}`` (colca `exec_configure.go` `bindCatalogue`),
which is NOT in general the same string as the ``path`` a caller passed to
``publish()`` — a single-segment source like "temp" gets the connector's
mount prepended. `_on_signal` must match by `signal.data_tag` against the
catalogue, never by comparing the Signal's own topic path to the source.

``connect_local_mqtt``/``resolve_local_identity`` are monkeypatched so the
public ``Service(name, mount, ...)`` constructor (no ``node=`` — the local
door) exercises the real ``start()``/``publish()``/``_on_signal`` code paths
without a real broker or a real ``/self`` HTTP call.
"""

from __future__ import annotations

from pathlib import Path

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from franzmq import Topic
from colca_data_contracts.local_service import LocalServiceIdentity
from colca_data_contracts.payload import DataTags, Metric, Signal

from chaski.service import Service


class _FakeReasonCode:
    is_failure = False


class _FakeClient:
    """A minimal stand-in for franzmq.Client: records publishes, and fires
    subscription callbacks only when the test calls deliver()."""

    def __init__(self) -> None:
        self.published: list[tuple[str, object]] = []
        self.tombstoned: list[str] = []
        self.subscriptions: dict[str, object] = {}
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
        self.subscriptions[str(topic)] = callback

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:  # noqa: ARG002
        self.published.append((str(topic), payload))

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:  # noqa: ARG002
        self.tombstoned.append(str(topic))

    def deliver(self, topic: Topic, payload: object) -> None:
        topic_str = str(topic)
        for filt, callback in self.subscriptions.items():
            prefix = filt[:-1] if filt.endswith("#") else filt
            if topic_str == filt or topic_str.startswith(prefix):
                callback(type("Message", (), {"topic": topic, "payload": payload})())
                return
        raise AssertionError(f"no subscription matches {topic_str!r}: {list(self.subscriptions)}")


def _service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, mount: str = "line1") -> tuple[Service, _FakeClient]:
    client = _FakeClient()
    identity = LocalServiceIdentity(
        service_id="svc-ulid", service_name="svc1", node_id="n-edge1",
        system_element_id="el-1", mount=mount,
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", lambda *a, **k: (client, identity))
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)
    svc = Service("svc1", mount, state_dir=tmp_path)
    svc.start()
    return svc, client


def _minted_tag_id(client: _FakeClient) -> str:
    catalogue = next(p for _t, p in client.published if isinstance(p, DataTags))
    return catalogue.data_tags[0].id


def test_a_single_segment_path_binds_at_the_connectors_mount_not_at_the_source(tmp_path, monkeypatch):
    """The bug this pins: "temp" (no meta.element) binds at "line1/temp" at
    the node — a different string from the source "temp" the catalogue
    carries. Matching must go through signal.data_tag, not the path."""
    svc, client = _service(tmp_path, monkeypatch)

    svc.publish("temp", 42.0)
    tag_id = _minted_tag_id(client)

    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "temp"))
    client.deliver(signal_topic, Signal(id="sig-1", name="temp", data_tag=tag_id))

    metrics = [p for _t, p in client.published if isinstance(p, Metric)]
    assert len(metrics) == 1, client.published
    assert metrics[0].signal_id == "sig-1"
    assert metrics[0].value == 42.0


def test_a_multi_segment_path_binds_under_the_subscribed_mount_and_still_matches(tmp_path, monkeypatch):
    """Denominator check (testing.md): a source naming its own child element
    (meta.element, resolved server-side against an element already placed
    under this service's own mount) must keep matching too — this is the
    case where the Signal's bound path and the catalogue source happen to be
    the identical string."""
    svc, client = _service(tmp_path, monkeypatch)

    svc.publish("press3/temp", 7.0)
    tag_id = _minted_tag_id(client)

    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "press3", "temp"))
    client.deliver(signal_topic, Signal(id="sig-2", name="temp", data_tag=tag_id))

    metrics = [p for _t, p in client.published if isinstance(p, Metric)]
    assert len(metrics) == 1, client.published
    assert metrics[0].signal_id == "sig-2"


def test_publish_after_binding_goes_straight_to_metric(tmp_path, monkeypatch):
    svc, client = _service(tmp_path, monkeypatch)
    svc.publish("temp", 1.0)
    tag_id = _minted_tag_id(client)
    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "temp"))
    client.deliver(signal_topic, Signal(id="sig-1", name="temp", data_tag=tag_id))

    svc.publish("temp", 2.0)

    metrics = [p for _t, p in client.published if isinstance(p, Metric)]
    assert [m.value for m in metrics] == [1.0, 2.0]


def test_a_signal_for_a_tag_this_service_does_not_own_is_ignored(tmp_path, monkeypatch):
    svc, client = _service(tmp_path, monkeypatch)
    svc.publish("temp", 1.0)

    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "someone-elses"))
    client.deliver(signal_topic, Signal(id="sig-x", name="x", data_tag="not-our-tag"))

    assert not [p for _t, p in client.published if isinstance(p, Metric)]


def test_a_signal_tombstone_unbinds(tmp_path, monkeypatch):
    svc, client = _service(tmp_path, monkeypatch)
    svc.publish("temp", 1.0)
    tag_id = _minted_tag_id(client)
    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "temp"))
    client.deliver(signal_topic, Signal(id="sig-1", name="temp", data_tag=tag_id))
    assert tag_id in svc._bindings

    client.deliver(signal_topic, None)

    assert tag_id not in svc._bindings
