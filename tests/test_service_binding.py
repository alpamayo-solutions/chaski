"""Tests for Service's signal binding with a fake MQTT client.

The node binds a Signal at ``{mount}/{tag name}``, which is usually not the
``path`` given to ``publish()``, so `_on_signal` matches by `signal.data_tag`.
``connect_local_mqtt`` and ``resolve_local_identity`` are patched, so the real
``start()``, ``publish()`` and ``_on_signal`` run without a broker.
"""

from __future__ import annotations

from pathlib import Path

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from colca_data_contracts.local_service import LocalServiceIdentity
from colca_data_contracts.payload import DataTags, Metric, Signal
from franzmq import Topic

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

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:
        self.subscriptions[str(topic)] = callback

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:
        self.published.append((str(topic), payload))

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:
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
        service_id="svc-ulid",
        service_name="svc1",
        node_id="n-edge1",
        system_element_id="el-1",
        mount=mount,
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
    """The source "temp" binds at "line1/temp" at the node, so matching goes
    through signal.data_tag."""
    svc, client = _service(tmp_path, monkeypatch)

    svc.publish("temp", 42.0)
    tag_id = _minted_tag_id(client)

    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "temp"))
    client.deliver(signal_topic, Signal(id="sig-1", name="temp", data_tag=tag_id, is_published=True))

    metrics = [p for _t, p in client.published if isinstance(p, Metric)]
    assert len(metrics) == 1, client.published
    assert metrics[0].signal_id == "sig-1"
    assert metrics[0].value == 42.0


def test_a_multi_segment_path_binds_under_the_subscribed_mount_and_still_matches(tmp_path, monkeypatch):
    """A source with its own child element matches too; here the bound path and
    the source happen to be the same string."""
    svc, client = _service(tmp_path, monkeypatch)

    svc.publish("press3/temp", 7.0)
    tag_id = _minted_tag_id(client)

    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "press3", "temp"))
    client.deliver(signal_topic, Signal(id="sig-2", name="temp", data_tag=tag_id, is_published=True))

    metrics = [p for _t, p in client.published if isinstance(p, Metric)]
    assert len(metrics) == 1, client.published
    assert metrics[0].signal_id == "sig-2"


def test_publish_after_binding_goes_straight_to_metric(tmp_path, monkeypatch):
    svc, client = _service(tmp_path, monkeypatch)
    svc.publish("temp", 1.0)
    tag_id = _minted_tag_id(client)
    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "temp"))
    client.deliver(signal_topic, Signal(id="sig-1", name="temp", data_tag=tag_id, is_published=True))

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
    client.deliver(signal_topic, Signal(id="sig-1", name="temp", data_tag=tag_id, is_published=True))
    assert svc._bindings_for(tag_id)

    client.deliver(signal_topic, None)

    assert not svc._bindings_for(tag_id)


def test_a_signal_the_node_switched_off_is_bound_but_publishes_nothing(tmp_path, monkeypatch):
    """``is_published`` is the node's say over whether a Signal's values go
    out (the same flag the connector's poll loop honours). A sample for a
    switched-off Signal is neither published nor kept: buffering would hold
    it for a binding that already exists."""
    svc, client = _service(tmp_path, monkeypatch)
    svc.publish("temp", 1.0)
    tag_id = _minted_tag_id(client)
    signal_topic = Topic(payload_type=Signal, node_id="n-edge1", context=("line1", "temp"))
    client.deliver(signal_topic, Signal(id="sig-1", name="temp", data_tag=tag_id, is_published=False))

    svc.publish("temp", 2.0)

    assert not [p for _t, p in client.published if isinstance(p, Metric)]
    assert svc.pending() == [], "a bound path is not pending, published or not"

    # Denominator: switching it on publishes from then on.
    client.deliver(signal_topic, Signal(id="sig-1", name="temp", data_tag=tag_id, is_published=True))
    svc.publish("temp", 3.0)
    assert [m.value for _t, m in client.published if isinstance(m, Metric)] == [3.0]
