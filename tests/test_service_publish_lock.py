"""Level-2 pin for the one threading rule Service has: never hold its lock
across a waiting publish (`service.py` ``_publish_outside_the_lock``).

The bug this pins, seen first at level 4 (the embedded-node contract): a
qos=1 publish waits for a PUBACK, and the PUBACK is read by the same MQTT
network thread that runs ``_on_signal``. A Signal that lands while
``close()`` (or ``publish()``, ``status()``, ``retire()``) is inside the lock
therefore blocks that thread on the lock the publisher holds, and the
publisher waits out its full ``publish_timeout`` for an acknowledgement that
cannot arrive — surfacing as ``PublishTimeout`` on ``_ServiceDetails``, a
topic with nothing to do with the Signal that caused it.

The fake here reproduces exactly that coupling and nothing else: its
``publish(wait=True)`` needs the network thread to be free before it can
return, and a delivery armed by the test runs ON that thread. With the lock
held across the publish, the two block each other and the publish raises;
with the fix they proceed in either order.
"""

from __future__ import annotations

import concurrent.futures
import threading
from pathlib import Path

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from colca_data_contracts.local_service import LocalServiceIdentity
from colca_data_contracts.payload import DataTags, Metric, Signal
from franzmq import Topic

from chaski.service import Service

_PUBACK_TIMEOUT = 5.0


class _FakeReasonCode:
    is_failure = False


class _NetworkThreadClient:
    """A fake franzmq.Client whose PUBACK comes from the SAME single thread
    that dispatches subscription callbacks — the real client's shape, and the
    only property this test needs.

    ``arm_delivery`` queues a message to be dispatched on that thread at the
    next publish, which is how the test puts a Signal into the exact window
    where a publisher might be holding the lock.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, object]] = []
        self.tombstoned: list[str] = []
        self.subscriptions: dict[str, object] = {}
        self.on_connect = None
        self.node_id = None
        self._net = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="fake-mqtt-net")
        self._armed: list[tuple[Topic, object]] = []

    # -- franzmq surface ------------------------------------------------

    def loop_start(self) -> None:
        if self.on_connect is not None:
            self.on_connect(self, None, None, _FakeReasonCode())

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:
        self.subscriptions[str(topic)] = callback

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True):
        self.published.append((str(topic), payload))
        if wait:
            self._await_network_thread(str(topic))

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:
        self.tombstoned.append(str(topic))
        if wait:
            self._await_network_thread(str(topic))

    # -- test controls --------------------------------------------------

    def arm_delivery(self, topic: Topic, payload: object) -> None:
        """Dispatch this message on the network thread at the next publish."""
        self._armed.append((topic, payload))

    def shutdown(self) -> None:
        self._net.shutdown(wait=True)

    # -- internals ------------------------------------------------------

    def _await_network_thread(self, topic: str) -> None:
        """A waiting publish cannot return before the network thread has
        drained what it owes — that thread is the one that would read the
        PUBACK."""
        armed, self._armed = self._armed, []
        for message_topic, payload in armed:
            self._net.submit(self._dispatch, message_topic, payload)
        drained = self._net.submit(lambda: None)
        try:
            drained.result(timeout=_PUBACK_TIMEOUT)
        except concurrent.futures.TimeoutError:  # pragma: no cover - the bug
            raise AssertionError(
                f"no PUBACK for {topic} within {_PUBACK_TIMEOUT}s: the network thread is "
                "blocked on Service._lock, so the publisher is holding it across a waiting "
                "publish (service.py _publish_outside_the_lock)"
            ) from None

    def _dispatch(self, topic: Topic, payload: object) -> None:
        topic_str = str(topic)
        for filt, callback in self.subscriptions.items():
            prefix = filt[:-1] if filt.endswith("#") else filt
            if topic_str == filt or topic_str.startswith(prefix):
                callback(type("Message", (), {"topic": topic, "payload": payload})())
                return


@pytest.fixture()
def service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    client = _NetworkThreadClient()
    identity = LocalServiceIdentity(
        service_id="svc-ulid",
        service_name="erp",
        node_id="n-edge1",
        system_element_id=None,
        mount="",
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", lambda *a, **k: (client, identity))
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)
    svc = Service("erp", state_dir=tmp_path)
    svc.start()
    try:
        yield svc, client
    finally:
        client.shutdown()


def _minted_tag_id(client: _NetworkThreadClient) -> str:
    catalogue = next(p for _t, p in client.published if isinstance(p, DataTags))
    return catalogue.data_tags[0].id


def _signal_for(client: _NetworkThreadClient) -> tuple[Topic, Signal]:
    tag_id = _minted_tag_id(client)
    return (
        Topic(payload_type=Signal, node_id="n-edge1", context=("orders",)),
        Signal(id="sig-1", name="orders", data_tag=tag_id, is_published=True),
    )


def test_close_completes_when_a_signal_lands_on_the_network_thread(service):
    """The level-4 failure, hermetic: publish buffers (no binding yet), then
    the node's autobound Signal arrives in the window where close() is
    publishing its final ServiceDetails."""
    svc, client = service
    svc.publish("orders", 42)
    client.arm_delivery(*_signal_for(client))

    svc.close()  # raises through the fake if the lock is held across a publish

    metrics = [p for _t, p in client.published if isinstance(p, Metric)]
    assert [m.value for m in metrics] == [42], client.published


def test_publish_completes_when_a_signal_lands_on_the_network_thread(service):
    """Same rule on the hot path: growing the catalogue publishes it, and the
    Signal for the PREVIOUS tag may land while that publish is in flight."""
    svc, client = service
    svc.publish("orders", 1)
    client.arm_delivery(*_signal_for(client))

    svc.publish("shipments", 2)

    assert [t for t, p in client.published if isinstance(p, DataTags)], client.published


def test_status_completes_when_a_signal_lands_on_the_network_thread(service):
    svc, client = service
    svc.publish("orders", 1)
    client.arm_delivery(*_signal_for(client))

    svc.status(ok=False, detail="plc unreachable")

    assert svc.pending() == [], svc.pending()


def test_retire_completes_when_a_signal_lands_on_the_network_thread(service):
    svc, client = service
    svc.publish("orders", 1)
    client.arm_delivery(*_signal_for(client))

    svc.retire()

    assert len(client.tombstoned) == 2, client.tombstoned


def test_the_lock_is_free_while_a_publish_waits(service):
    """The rule itself, stated once rather than only through its symptoms: a
    second thread must be able to take the lock while a waiting publish is in
    flight. Without this, the three tests above could all pass on timing
    alone if the fake's window ever narrowed."""
    svc, client = service
    taken = threading.Event()

    def grab_the_lock() -> None:
        with svc._lock:
            taken.set()

    original = client.publish

    def publish_and_probe(*args, **kwargs):
        threading.Thread(target=grab_the_lock, daemon=True).start()
        assert taken.wait(_PUBACK_TIMEOUT), (
            "Service._lock was held while a publish waited for its PUBACK (service.py _publish_outside_the_lock)"
        )
        return original(*args, **kwargs)

    client.publish = publish_and_probe  # type: ignore[method-assign]
    svc.publish("orders", 1)
