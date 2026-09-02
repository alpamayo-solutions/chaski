"""The shared connector-protocol publisher (SDK design §3, §7 gap 3).

``Service.local`` (inside a deployment, the local door, no credential) and
``Service.external`` (outside it, the published door, a registry-pinned
mTLS key) are ONE implementation differing only in which door they open and
which credential they present — architecture principle 1's one-clause test,
applied to the client side. Both give the same ``publish(path, value, unit=,
timestamp=)``: every distinct path becomes a ``DataTag`` (see
``catalogue.py``), the catalogue is republished when it grows (content-hash
guarded, exactly like the connector), and each publish resolves the Signal
the node minted for that tag — learned from the service's own ``_Signal``
subscription — before writing ``_Metric`` with ``signal_id``. A path with no
Signal yet is buffered (bounded) and flushed the moment one binds.
"""

from __future__ import annotations

import json
import logging
import ssl
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit

import paho.mqtt.client as pahomqtt
from franzmq import Client, Topic
from colca_data_contracts.local_service import connect_local_mqtt
from colca_data_contracts.payload import DataTags, Metric
from colca_data_contracts.payload import Signal as SignalRecord
from colca_data_contracts.service_topics import service_context

from .catalogue import Catalogue

logger = logging.getLogger(__name__)

# How long a not-yet-bound path may keep buffering before its "still
# unbound" line repeats — mirrors colca's own 5-minute reminder shape
# (unbound.go on the node side of this same gap).
_UNBOUND_LOG_INTERVAL = 300.0
# A not-yet-bound path buffers at most this many samples; the newest ones
# survive (a bounded deque with maxlen drops the oldest on overflow) — the
# same "bounded memory over unbounded backlog" rule every publisher in this
# codebase follows ("Every service publishes its log").
_MAX_BUFFERED_PER_PATH = 100
_DEFAULT_EXTERNAL_MQTT_PORT = 8883
_DEFAULT_EXTERNAL_API_PORT = 443


def _epoch(ts: Any) -> float:
    """Accept a datetime, a float/int epoch, or None (-> now). The wire
    contract's timestamp is a number — a datetime would encode as an ISO
    string, which the door refuses (see dataops/src/dataops/outputs.py's own
    _epoch, the same rule restated here because a Service has no door client
    to borrow it from)."""
    if ts is None:
        return time.time()
    if hasattr(ts, "timestamp") and not isinstance(ts, (int, float)):
        return ts.timestamp()
    return float(ts)


def _state_home() -> Path:
    import os

    override = os.environ.get("COLCA_STATE_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".colca" / "state"


def _insecure_ssl_context() -> ssl.SSLContext:
    """No CA anywhere in this door (identity/authz §"Node doors"): trust is
    the registry-pinned key presented as a client certificate, never a chain
    — the same InsecureSkipVerify shape colca-machine (the Go reference
    client) uses when it dials this exact door."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class Service:
    """A publisher on either the local or the external door.

    Construct with :meth:`local` or :meth:`external` — never directly.
    """

    def __init__(
        self,
        *,
        client: Client,
        node_id: str,
        mount: str,
        catalogue_context: tuple[str, ...],
        connector: str,
        state_dir: Path,
    ) -> None:
        self._client = client
        self._node_id = node_id
        self._mount = mount
        self._catalogue_topic = Topic(payload_type=DataTags, node_id=node_id, context=catalogue_context)
        self._lock = threading.RLock()
        self._catalogue = Catalogue(Path(state_dir) / "catalogue.json", connector=connector)
        # tag_id -> (metric Topic, signal id)
        self._bindings: dict[str, tuple[Topic, str]] = {}
        # the SIGNAL's own topic string -> tag_id, so a tombstone (which
        # arrives on the _Signal topic, never the _Metric one _bindings
        # stores) can find what to unbind.
        self._signal_topic_of: dict[str, str] = {}
        self._buffer: dict[str, deque] = {}
        self._unbound_log_at: dict[str, float] = {}
        self._seen: set[str] = set()
        self._closed = False

    # -- construction ----------------------------------------------------

    @classmethod
    def local(
        cls,
        name: str,
        mount: str = "",
        *,
        host: str = "colca",
        http_port: int = 80,
        mqtt_port: int = 1883,
        state_dir: Optional[Path] = None,
    ) -> "Service":
        """A publisher inside this deployment: the local door proves the
        identity, so no credential travels (local-service-trust design)."""
        client, identity = connect_local_mqtt(
            name, host=host, http_port=http_port, mqtt_port=mqtt_port, mount=mount
        )
        svc = cls(
            client=client,
            node_id=identity.node_id,
            mount=identity.mount,
            catalogue_context=identity.hierarchy,
            connector=identity.service_id,
            state_dir=state_dir or _state_home() / f"local-{name}",
        )
        svc._start()
        return svc

    @classmethod
    def external(
        cls,
        url: str,
        cert: str,
        key: str,
        *,
        ulid: str,
        mount: str = "",
        mqtt_port: int = _DEFAULT_EXTERNAL_MQTT_PORT,
        api_port: Optional[int] = None,
        state_dir: Optional[Path] = None,
    ) -> "Service":
        """A publisher outside this deployment: a registry-pinned mTLS key
        is the credential (published door, 8883).

        ``ulid`` is the identity's own id — the same value the operator gave
        ``colca external enroll``, used as the CONNECT username and as the
        catalogue's ``connector`` id. ``cert``/``key`` are the PEM files
        ``colca-keygen -cert`` writes (or their equivalent): ``key`` wraps
        the enrolled ed25519 private key, ``cert`` the self-signed
        certificate presenting it as a TLS client certificate. ``url`` is
        the node's own HTTPS API address — used once, unauthenticated, to
        read the node's ulid off ``GET /healthz`` (every v1 topic needs it
        at level 4), and to derive the MQTT host.
        """
        host, healthz_url = _external_healthz_url(url, api_port)
        node_id = _read_node_id(healthz_url)
        client = _connect_external_mqtt(host, mqtt_port, ulid, cert, key)
        client.node_id = node_id
        svc = cls(
            client=client,
            node_id=node_id,
            mount=mount,
            catalogue_context=service_context(mount, ulid),
            connector=ulid,
            state_dir=state_dir or _state_home() / f"external-{ulid}",
        )
        svc._start()
        return svc

    def _start(self, *, connect_timeout: float = 10.0) -> None:
        mount_parts = tuple(p for p in self._mount.split("/") if p)
        filter_context = (*mount_parts, "#") if mount_parts else ("#",)
        signal_filter = Topic(payload_type=SignalRecord, node_id=self._node_id, context=filter_context)

        # Subscribing straight after connect()/loop_start() races the
        # CONNACK: connect() opens the socket and sends CONNECT but does not
        # wait for the broker's answer, so a SUBSCRIBE sent before it arrives
        # is a protocol violation the broker is free to drop. Wait for the
        # (paho v2) on_connect callback instead — franzmq.Client leaves
        # .on_connect unset unless a caller sets it, so this never collides
        # with local_service.connect_local_mqtt's own optional on_connect.
        connected = threading.Event()

        def _on_connect(_client: Any, _userdata: Any, _flags: Any, reason_code: Any,
                        _properties: Any = None) -> None:
            if not getattr(reason_code, "is_failure", False):
                connected.set()

        self._client.on_connect = _on_connect
        self._client.loop_start()
        if not connected.wait(connect_timeout):
            raise TimeoutError(f"chaski.Service: no CONNACK from the broker within {connect_timeout}s")
        self._client.subscribe(signal_filter, qos=1, callback=self._on_signal)

    # -- publishing --------------------------------------------------------

    def publish(
        self,
        path: str,
        value: Any,
        *,
        unit: Optional[str] = None,
        timestamp: Optional[Any] = None,
    ) -> None:
        """Publish one sample at ``path``.

        The first call for a never-seen ``path`` mints a DataTag and grows
        the catalogue (republished immediately); every call resolves the
        Signal the node minted for that tag from this service's own
        ``_Signal`` subscription. Before that Signal exists, the sample is
        buffered (bounded) and flushed the moment it binds.
        """
        if self._closed:
            raise RuntimeError("chaski.Service is closed")
        with self._lock:
            tag_id, changed = self._catalogue.ensure(path, value, unit)
            self._seen.add(path)
            if changed:
                self._republish_catalogue()
            binding = self._bindings.get(tag_id)
            if binding is None:
                self._buffer_sample(path, value, timestamp)
                return
            topic, signal_id = binding
        self._publish_metric(topic, signal_id, value, timestamp)

    def _publish_metric(
        self, topic: Topic, signal_id: str, value: Any, timestamp: Optional[Any], *, wait: bool = True
    ) -> None:
        metric = Metric(value=value, timestamp=_epoch(timestamp), signal_id=signal_id)
        self._client.publish(topic, metric, qos=1, wait=wait)

    def _buffer_sample(self, path: str, value: Any, timestamp: Optional[Any]) -> None:
        queue = self._buffer.setdefault(path, deque(maxlen=_MAX_BUFFERED_PER_PATH))
        queue.append((value, timestamp))
        now = time.monotonic()
        last = self._unbound_log_at.get(path, 0.0)
        if now - last >= _UNBOUND_LOG_INTERVAL:
            self._unbound_log_at[path] = now
            logger.warning(
                "chaski.Service: %r has no bound Signal yet — buffering (%d queued, capped at %d)",
                path, len(queue), _MAX_BUFFERED_PER_PATH,
            )

    # -- signal binding ------------------------------------------------

    def _on_signal(self, message: Any) -> None:
        """A Signal binds at ``{under}/{leaf}`` — ``under`` is the connector's
        own mount by default, ``leaf`` the tag's sanitized NAME — which is
        NOT in general the same string as the ``path`` a caller gave
        ``publish()`` (a single-segment source like "temp" gets the
        connector's mount prepended; a multi-segment one only matches by
        coincidence). So the match key is ``signal.data_tag`` against this
        service's own catalogue, never the topic's path (``exec_configure.go``
        ``bindCatalogue``).
        """
        topic_str = str(message.topic)
        parts = topic_str.split("/")
        if len(parts) < 5:
            return
        path = "/".join(parts[4:])
        signal = message.payload
        if signal is None:
            # Tombstone: unbind whichever tag_id this exact SIGNAL topic
            # (not the _Metric topic _bindings stores — a different contract,
            # hence the separate reverse index) was bound to.
            with self._lock:
                tag_id = self._signal_topic_of.pop(topic_str, None)
                if tag_id is not None:
                    self._bindings.pop(tag_id, None)
            return
        tag_id = getattr(signal, "data_tag", None)
        if not tag_id:
            return
        with self._lock:
            source = self._catalogue.source_for_tag(tag_id)
            if source is None:
                return  # a _Signal bound to a tag this service does not own
            metric_topic = Topic(payload_type=Metric, node_id=self._node_id, context=tuple(path.split("/")))
            self._bindings[tag_id] = (metric_topic, signal.id)
            self._signal_topic_of[topic_str] = tag_id
            queued = self._buffer.pop(source, None)
        if queued:
            # Always the MQTT network/callback thread here: franzmq.Client's
            # own qos>=1 wait-for-PUBACK is a deadlock on this thread by
            # construction (it is the thread that would have to read the
            # PUBACK), so ask it to fire-and-forget instead of trusting its
            # same-thread detection, which does not appear to catch this
            # dispatch path (observed: a PublishTimeout after the full
            # publish_timeout, not a same-thread skip).
            for value, timestamp in queued:
                self._publish_metric(metric_topic, signal.id, value, timestamp, wait=False)

    # -- catalogue ---------------------------------------------------------

    def _republish_catalogue(self) -> None:
        payload = self._catalogue.payload()
        revision = payload.version
        if revision == self._catalogue.last_published_revision:
            return
        self._client.publish(self._catalogue_topic, payload, qos=1, retain=True)
        self._catalogue.record_published(revision)

    # -- lifecycle -----------------------------------------------------

    def close(self) -> None:
        """Seal the catalogue (any known path not published this run goes
        stale) and disconnect. Idempotent."""
        if self._closed:
            return
        self._closed = True
        with self._lock:
            if self._catalogue.seal(self._seen):
                self._republish_catalogue()
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:  # noqa: BLE001 - best-effort teardown
            logger.debug("chaski.Service: disconnect raised during close()", exc_info=True)

    def __enter__(self) -> "Service":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


def _external_healthz_url(url: str, api_port: Optional[int]) -> tuple[str, str]:
    parsed = urlsplit(url if "://" in url else f"https://{url}")
    host = parsed.hostname
    if not host:
        raise ValueError(f"chaski.Service.external: could not parse a host from {url!r}")
    port = api_port if api_port is not None else (parsed.port or _DEFAULT_EXTERNAL_API_PORT)
    return host, f"https://{host}:{port}/healthz"


def _read_node_id(healthz_url: str, timeout: float = 10.0) -> str:
    request = urllib.request.Request(healthz_url)
    with urllib.request.urlopen(request, timeout=timeout, context=_insecure_ssl_context()) as response:
        payload = json.load(response)
    node_id = payload.get("ulid")
    if not node_id:
        raise RuntimeError(f"{healthz_url} did not report a node ulid")
    return str(node_id)


def _connect_external_mqtt(host: str, port: int, ulid: str, cert: str, key: str) -> Client:
    client = Client(client_id=ulid, protocol=pahomqtt.MQTTv5)
    client.username_pw_set(ulid)
    ctx = _insecure_ssl_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_cert_chain(certfile=cert, keyfile=key)
    client.tls_set_context(ctx)
    client.reconnect_on_failure = True
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    client.connect(host=host, port=port, clean_start=False)
    return client
