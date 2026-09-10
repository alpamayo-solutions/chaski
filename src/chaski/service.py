"""``chaski.Service``: the shared connector-protocol publisher .

ONE constructor, not two classmethods: ``Service(name, mount="", *,
node=None, ...)``. ``node`` says who this service is to Colca —
architecture principle 1's one-clause test, applied to the client side:

* ``node=None`` (the default) — inside a deployment: the local door
  (``colca:80``/``colca:1883``, compose DNS), no credential, self-registered
  by name+mount.
* ``node="https://..."`` — outside a deployment: the published door
  (8883/443), a registry-pinned ed25519 identity this Service loads or
  mints under its own state directory.
* ``node=LocalDoor(...)`` — an embedded node's own local door. An integrator
  never constructs this; it is what ``chaski.Node.service()`` passes.

Whichever door, ``publish(path, value, unit=, timestamp=)`` is the same
implementation: every distinct path becomes a ``DataTag`` (see
``catalogue.py``), the catalogue is republished when it changes
(content-hash guarded), and each publish resolves the Signal the node
minted for that tag — learned from the service's own ``_Signal``
subscription — before writing ``_Metric`` with ``signal_id``. A path with no
Signal yet is buffered (bounded) and flushed the moment one binds.

Construction never connects — it stores configuration and, outside a
deployment, loads-or-mints the identity. ``start()`` (or the context
manager) is what opens the door; see the design's lifecycle table for what
happens at each stage. ``start()`` also reads the catalogue this service
published LAST time back from the node's KV before it subscribes to its
``_Signal`` records: those records name the ids of the previous run, so the
catalogue has to know them before the first one arrives, and the republish
guard is armed from the revision already on record.

**Placement follows the registry, live.** A re-placement (an operator moves
the service's entry) kicks the MQTT session; on the reconnect the service
re-resolves its identity, re-subscribes at its current mount, and
republishes its ``_ServiceDetails`` and catalogue there — a rename or a
reparent needs no redeploy (local-service-trust design §3.2, §6.1).

``ConnectorService`` (``chaski.connector``) is this class plus a poll loop:
a driver discovers tags into the same catalogue and reads them on an
interval; bindings, publishing, registration and the consume lane are all
this class. It re-implements nothing here.

**The consume lane** (service families design §3.3). A
started Service also reads: ``kv(prefix, contract=)`` is a bounded snapshot
of the node's retained state, and ``stream(name)`` a named, durable cursor
over one of its streams — ``metrics``, ``annotations``, ``alarms``, ... —
with the same fetch → process → ack contract the shipped dataops service
follows (``chaski.door.Stream``). Both speak the same door ``publish()``
registers at, with the same identity: ``X-Colca-Service`` on the local door,
the pinned client certificate on the published one.

**A bridge is a Service whose protocol is HTTP** (design §3.6). There is
no ``BridgeService`` class, deliberately. A bridge to an ERP/MES has two
lanes and neither is bridge-shaped: its *reference* lane polls the foreign
system and ``publish()``-es what it learns — a connector whose protocol
happens to be HTTP — and its *writeback* lane follows a stream with
``stream()`` and makes idempotent foreign writes. "Never commands machines"
is not a base class either: an external identity holds the ``write:``
grants its enrollment gave it and no ``cmd:`` — position is authority
(architecture principle 6). What a bridge genuinely owns — the foreign
system's client and its idempotency keys — is shaped by that system, so
start from a plain ``Service`` rather than a class that would wrap a
dictionary.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple, Optional, Union
from urllib.parse import urlsplit

import paho.mqtt.client as pahomqtt
import ulid as ulid_lib
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import NameOID
from franzmq import Client, Topic
from colca_data_contracts.local_service import (
    attach_log_publisher,
    connect_local_mqtt,
    resolve_local_identity,
)
from colca_data_contracts.payload import (
    DataTags,
    HealthMetricDeclaration,
    Metric,
    ServiceDetails,
    ServiceType,
)
from colca_data_contracts.payload import Signal as SignalRecord
from colca_data_contracts.service_topics import service_context

from .catalogue import Catalogue, element_for
from .door import Door, KvEntry, Stream

logger = logging.getLogger(__name__)


class Binding(NamedTuple):
    """One ``_Signal`` record the node authored against a tag of this
    service, keyed in :attr:`Service._bindings` by the SIGNAL's own topic —
    a tag may be read by more than one Signal, and a tombstone arrives on
    exactly that topic."""

    tag_id: str
    #: The ``_Metric`` topic: the Signal's own position, same node, same path.
    topic: Topic
    signal: SignalRecord

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
_DEFAULT_LOCAL_HTTP_PORT = 80
_DEFAULT_LOCAL_MQTT_PORT = 1883


class NotEnrolled(RuntimeError):
    """Raised by :meth:`Service.start` when the node refuses this identity's
    CONNECT — outside a deployment only. The message says how an operator
    enrolls it; also available bare via :meth:`Service.enroll_hint`."""

    def __init__(self, name: str, node_url: str, command: str) -> None:
        super().__init__(
            f"{name} is not enrolled at {node_url}. To enroll it:\n  {command}"
        )
        self.command = command


@dataclass(frozen=True)
class LocalDoor:
    """A node's local door (SDK design §3.1): host, HTTP port, MQTT port. An
    integrator never constructs this directly — pass a ``node=`` URL string
    outside a deployment, or leave ``node`` unset inside one (the compose
    defaults below). ``chaski.Node.service()`` builds one for an embedded
    node, and a containerised connector can build one from its own
    environment."""

    host: str = "colca"
    http_port: int = _DEFAULT_LOCAL_HTTP_PORT
    mqtt_port: int = _DEFAULT_LOCAL_MQTT_PORT


def _epoch(ts: Any) -> float:
    """Accept a datetime, a float/int epoch, or None (-> now). The wire
    contract's timestamp is a number — a datetime would encode as an ISO
    string, which the door refuses. ``chaski.dataops.outputs`` applies the
    same rule to a producer's output (it cannot import this one without
    pulling the MQTT publisher into a module that never uses it)."""
    if ts is None:
        return time.time()
    if hasattr(ts, "timestamp") and not isinstance(ts, (int, float)):
        return ts.timestamp()
    return float(ts)


def _default_state_dir(name: str) -> Path:
    override = os.environ.get("COLCA_STATE_DIR")
    base = Path(override).expanduser() if override else Path.home() / ".colca"
    return base / "services" / name


def _insecure_ssl_context() -> ssl.SSLContext:
    """No CA anywhere in this door (identity/authz §"Node doors"): trust is
    the registry-pinned key presented as a client certificate, never a chain
    — the same InsecureSkipVerify shape colca-machine (the Go reference
    client) uses when it dials this exact door."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _pending_reason(path: str, mount: str, connector_id: str, *, connected: bool) -> str:
    """The reason a buffered path has no bound Signal yet — pure and testable
    without a broker (level 2). Three cases, each independently observable
    from state the SDK already holds:

    * "not enrolled" — outside a deployment, this identity has never
      completed a CONNECT, so nothing at the node can have happened yet.
    * "element not yet authored" — the path names a parent (``element_for``
      is non-empty), so binding first needs the node to author that element
      (``bindCatalogue``/``authorElementAt``) — a step a mount-level path
      (already sitting on an element the enrollment/registration authored)
      does not need.
    * "awaiting binding" — the tag is on an element the node already has;
      it is only waiting for `signal/autobind` (or the node's own
      autobind-on-catalogue-growth trigger) to mint the Signal.
    """
    if not connected:
        return "not enrolled"
    element = element_for(path, mount)
    if element:
        return f"element not yet authored: {element!r}"
    return (
        "awaiting binding — an operator can run "
        f"`colca configure signal/autobind --connector {connector_id}` at the node "
        "if its autobind is off"
    )


def _mint_identity(identity_dir: Path) -> tuple[str, str, Path, Path]:
    """Load this Service's own external identity from ``identity_dir``,
    minting one on first use (idempotent — a second call against the same
    directory returns the identical ulid/pubkey). Mirrors
    ``colca-keygen -cert``: an ed25519 key (PEM PKCS8) plus a self-signed
    certificate wrapping it (cert = key container, trust = registry pinning
    — no CA anywhere at this door). Both files, and the minted ulid, are
    written 0600 — private material, host-local only."""
    identity_dir.mkdir(parents=True, exist_ok=True)
    ulid_path = identity_dir / "ulid"
    key_path = identity_dir / "identity.key"
    cert_path = identity_dir / "identity.key.crt"

    if ulid_path.exists() and key_path.exists() and cert_path.exists():
        ulid = ulid_path.read_text(encoding="utf-8").strip()
        key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    else:
        ulid = str(ulid_lib.new())
        key = Ed25519PrivateKey.generate()
        key_pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        key_path.write_bytes(key_pem)
        key_path.chmod(0o600)

        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, ulid)])
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(hours=1))
            .not_valid_after(now + datetime.timedelta(days=3650))
            .sign(key, None)
        )
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        cert_path.chmod(0o600)

        ulid_path.write_text(ulid, encoding="utf-8")
        ulid_path.chmod(0o600)

    pubkey_hex = key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    ).hex()
    return ulid, pubkey_hex, key_path, cert_path


def _node_admin_base(node_url: str, api_port: Optional[int]) -> tuple[str, str]:
    """(host, base https url) for a node's published API door."""
    parsed = urlsplit(node_url if "://" in node_url else f"https://{node_url}")
    host = parsed.hostname
    if not host:
        raise ValueError(f"chaski.Service: could not parse a host from {node_url!r}")
    port = api_port if api_port is not None else (parsed.port or _DEFAULT_EXTERNAL_API_PORT)
    return host, f"https://{host}:{port}"


def _read_node_id(healthz_url: str, timeout: float = 10.0) -> str:
    request = urllib.request.Request(healthz_url)
    with urllib.request.urlopen(request, timeout=timeout, context=_insecure_ssl_context()) as response:  # noqa: S310
        payload = json.load(response)
    node_id = payload.get("ulid")
    if not node_id:
        raise RuntimeError(f"{healthz_url} did not report a node ulid")
    return str(node_id)


def _connect_external_mqtt(
    host: str, port: int, ulid: str, key_path: Path, cert_path: Path,
    *, will: Optional[tuple[Topic, ServiceDetails]] = None,
    max_queued_messages: int = 0,
) -> Client:
    client = Client(client_id=ulid, protocol=pahomqtt.MQTTv5)
    if max_queued_messages:
        client.max_queued_messages_set(max_queued_messages)
    client.username_pw_set(ulid)
    ctx = _insecure_ssl_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    client.tls_set_context(ctx)
    if will is not None:
        will_topic, will_payload = will
        client.will_set(str(will_topic), will_payload.encode(), qos=1, retain=True)
    client.reconnect_on_failure = True
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    client.connect(host=host, port=port, clean_start=False)
    return client


def _revoke_external(node_url: str, ulid: str, token: str, *, api_port: Optional[int] = None,
                      timeout: float = 15.0) -> None:
    """``DELETE /enroll/{ulid}`` on the node's admin door."""
    _, base = _node_admin_base(node_url, api_port)
    request = urllib.request.Request(
        f"{base}/enroll/{ulid}", method="DELETE", headers={"X-Colca-Token": token},
    )
    try:
        urllib.request.urlopen(request, timeout=timeout, context=_insecure_ssl_context())  # noqa: S310
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"chaski.Service.retire(): revoking {ulid} at {node_url} failed: HTTP {exc.code}"
        ) from exc


class Service:
    """A publisher on either the local or the external door — one implementation,
    one constructor. See the module docstring for the lifecycle."""

    def __init__(
        self,
        name: str,
        mount: str = "",
        *,
        node: Any = None,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        version: Optional[str] = None,
        logs: bool = True,
        state_dir: Optional[Path] = None,
        mqtt_port: Optional[int] = None,
        api_port: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
        architecture_metadata: Optional[dict[str, Any]] = None,
        health_metrics: Optional[Iterable[HealthMetricDeclaration]] = None,
        max_queued_messages: int = 0,
    ) -> None:
        """``node`` says who this Service is to Colca: ``None`` (default,
        inside a deployment), a ``node=`` URL string (outside one — the
        identity is loaded or minted under ``state_dir``), or a
        :class:`LocalDoor` (an embedded node's own local door — internal,
        ``chaski.Node.service()`` only).

        ``mqtt_port``/``api_port`` are for a node whose published
        doors sit behind a non-standard port (a test harness, a port-mapped
        deployment).

        ``metadata`` and ``architecture_metadata`` ride the retained
        ``_ServiceDetails`` record as given: what an editor shows about
        this service beyond its health — protocol, icon, the driver behind
        it. ``architecture_metadata`` is merged under the live
        ``status``/``detail`` :meth:`status` writes.

        ``health_metrics`` declares which Prometheus-scraped metrics describe
        this service's health in the Edit (``_ServiceDetails.
        health_metrics``) — a fact about where the service RUNS, not about
        what kind of service it is: a containerised deployment passes
        ``colca_data_contracts.container_resource_health_metrics()``, a
        host process nothing.

        Construction never touches the network except to load or mint an
        external identity's own key material on disk — it does not dial the
        node. See :meth:`start`.
        """
        self.name = name
        self._max_queued_messages = max_queued_messages
        self._mount = mount
        self.display_name = display_name
        self.description = description
        self.version = version
        self.logs = logs
        self.metadata: dict[str, Any] = dict(metadata or {})
        self.architecture_metadata: dict[str, Any] = dict(architecture_metadata or {})
        self.health_metrics = list(health_metrics or [])
        self._state_dir = Path(state_dir) if state_dir is not None else _default_state_dir(name)
        self._mqtt_port_override = mqtt_port
        self._api_port_override = api_port

        # _publish_outside_the_lock — the one threading rule this class has.
        #
        # This lock guards the catalogue, the bindings and the buffer, and it
        # is taken by TWO threads: the caller's, and the MQTT network thread
        # that runs `_on_signal`. A qos=1 publish waits for its PUBACK, and the
        # PUBACK is read by that same network thread — so holding this lock
        # across a waiting publish deadlocks the pair: the caller waits for a
        # PUBACK the network thread cannot deliver because it is blocked on
        # the lock the caller holds. It costs the full publish_timeout and
        # then surfaces as `PublishTimeout` on an unrelated topic, which is
        # what made it look like a broker fault rather than our own.
        #
        # So: decide under the lock, publish outside it. Every waiting publish
        # in this class (catalogue, ServiceDetails, metric, tombstones) is
        # reached with the lock released, and the helpers that do the
        # publishing say so in their own docstrings.
        self._lock = threading.RLock()
        self._client: Optional[Client] = None
        # The door's HTTP client — the consume lane (kv/stream). Opened by
        # start() beside the MQTT client, on the same door with the same
        # identity, so a read is never made under a name the node has not
        # yet registered at its mount.
        self._http: Optional[Door] = None
        self._connected = False
        self._closed = False
        self._node_id: Optional[str] = None
        self._service_id: Optional[str] = None
        self._system_element_id: Optional[str] = None
        # Where the node currently has this service: the resolved mount and
        # the hierarchy (mount + name) its own records live under. Local
        # services learn it from /self and re-learn it on every reconnect;
        # an external one is placed by its enrollment and keeps the mount
        # it was constructed with.
        self._resolved_mount: str = mount
        self._hierarchy: tuple[str, ...] = ()
        self._catalogue: Optional[Catalogue] = None
        self._catalogue_topic: Optional[Topic] = None
        self._details_topic: Optional[Topic] = None
        self._signal_filter: Optional[Topic] = None
        # The SIGNAL's own topic -> Binding. See Binding for why the key is
        # the signal's topic and not the tag id.
        self._bindings: dict[str, Binding] = {}
        self._buffer: dict[str, deque] = {}
        # First CONNACK: start() waits on this; every later on_connect is a
        # reconnect and re-announces placement instead (see _on_connect).
        self._connected_event = threading.Event()
        self._connect_outcome: Any = None
        self._unbound_log_at: dict[str, float] = {}
        self._seen: set[str] = set()
        self._last_status = "healthy"
        self._last_detail = ""

        if isinstance(node, LocalDoor):
            self._external = False
            self._door: Optional[LocalDoor] = node
            self._node_url: Optional[str] = None
            self.ulid: Optional[str] = None
            self.pubkey: Optional[str] = None
        elif node is None:
            self._external = False
            self._door = LocalDoor()
            self._node_url = None
            self.ulid = None
            self.pubkey = None
        elif isinstance(node, str):
            self._external = True
            self._door = None
            self._node_url = node
            self.ulid, self.pubkey, self._key_path, self._cert_path = _mint_identity(
                self._state_dir / "identity"
            )
        else:
            raise TypeError(
                f"chaski.Service: node= must be None, a URL string, or LocalDoor, got {node!r}"
            )

    # -- lifecycle -----------------------------------------------------

    def start(self, *, connect_timeout: float = 10.0) -> "Service":
        """Connect, self-register (local) or authenticate (external), publish
        the initial retained ``_ServiceDetails``, and subscribe to this
        service's own ``_Signal`` records. Idempotent — a second call on an
        already-started Service is a no-op.

        Outside a deployment, a refused CONNECT (the identity is not yet
        enrolled) raises :class:`NotEnrolled` — see :meth:`wait_enrolled` to
        poll instead of raising once.
        """
        if self._client is not None:
            return self
        if self._external:
            self._start_external(connect_timeout)
        else:
            self._start_local(connect_timeout)
        return self

    def _start_local(self, connect_timeout: float) -> None:
        door = self._door
        assert door is not None  # local mode always carries a door
        identity = resolve_local_identity(
            self.name, host=door.host, http_port=door.http_port, mount=self._mount,
        )
        self._node_id = identity.node_id
        self._service_id = identity.service_id
        self._apply_placement(identity.mount, identity.hierarchy, identity.system_element_id or None)
        self._catalogue = Catalogue(connector=identity.service_id, mount=self._resolved_mount)
        self._http = Door(f"http://{door.host}:{door.http_port}", service=self.name)

        will_payload = self._build_service_details(is_active=False, status="unhealthy")
        try:
            client, _ = connect_local_mqtt(
                self.name, host=door.host, http_port=door.http_port, mqtt_port=door.mqtt_port,
                mount=self._mount, identity=identity, publish_logs=self.logs,
                will=(self._details_topic, will_payload),
                max_queued_messages=self._max_queued_messages,
            )
        except Exception:
            self._reset_after_failed_connect()
            raise
        self._client = client
        try:
            self._connect_and_wait(connect_timeout)
        except Exception:
            self._reset_after_failed_connect()
            raise
        self._after_connect()

    def _start_external(self, connect_timeout: float) -> None:
        host, base = _node_admin_base(self._node_url, self._api_port_override)
        self._node_id = _read_node_id(f"{base}/healthz")
        self._service_id = self.ulid
        self._apply_placement(self._mount, service_context(self._mount, self.ulid), None)
        self._catalogue = Catalogue(connector=self.ulid, mount=self._mount)
        # The published API door authenticates the same pinned key the MQTT
        # door does, presented as a client certificate ("Node
        # doors": API 443, credential "cert").
        self._http = Door(base, service=self.ulid, cert=(self._cert_path, self._key_path))

        will_payload = self._build_service_details(is_active=False, status="unhealthy")
        port = self._mqtt_port_override or _DEFAULT_EXTERNAL_MQTT_PORT
        self._client = _connect_external_mqtt(
            host, port, self.ulid, self._key_path, self._cert_path,
            will=(self._details_topic, will_payload),
            max_queued_messages=self._max_queued_messages,
        )
        try:
            self._connect_and_wait(connect_timeout)
        except Exception:
            self._reset_after_failed_connect()
            raise
        if self.logs:
            attach_log_publisher(self._client, self._hierarchy)
        self._after_connect()

    def _apply_placement(self, mount: str, hierarchy: tuple[str, ...], element_id: Optional[str]) -> None:
        """Take the node's current answer for where this service sits: the
        topics its own records live at, and the ``_Signal`` filter narrowed
        to its own read scope (at or below its bound element — a fully
        wildcarded filter is not authorized under local-service-trust, auth
        §5.3)."""
        self._resolved_mount = mount
        self._hierarchy = tuple(hierarchy)
        self._system_element_id = element_id
        self._catalogue_topic = Topic(payload_type=DataTags, node_id=self._node_id, context=self._hierarchy)
        self._details_topic = Topic(
            payload_type=ServiceDetails, node_id=self._node_id, context=self._hierarchy + ("_service",),
        )
        mount_parts = tuple(p for p in mount.split("/") if p)
        filter_context = (*mount_parts, "#") if mount_parts else ("#",)
        self._signal_filter = Topic(payload_type=SignalRecord, node_id=self._node_id, context=filter_context)

    def _connect_and_wait(self, connect_timeout: float) -> None:
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.loop_start()
        if not self._connected_event.wait(connect_timeout):
            raise TimeoutError(f"chaski.Service: no CONNACK from the broker within {connect_timeout}s")
        reason_code = self._connect_outcome
        if getattr(reason_code, "is_failure", False):
            if self._external:
                raise NotEnrolled(self.name, self._node_url, self.enroll_hint())
            raise RuntimeError(f"chaski.Service: broker refused CONNECT ({reason_code})")
        self._connected = True

    def _on_connect(self, client: Any, _userdata: Any, _flags: Any, reason_code: Any,
                    _properties: Any = None) -> None:
        """paho's CONNACK callback, on its network thread. The first one
        releases :meth:`start`, which finishes the setup on the caller's
        thread (:meth:`_after_connect`). Every later one is a RECONNECT —
        and a re-placement kicks the live session, so a reconnect is exactly
        when placement may have moved: re-resolve it and re-announce
        (:meth:`_reannounce`). Nothing here may wait for a PUBACK: this is
        the thread that would read it."""
        if not self._connected_event.is_set():
            self._connect_outcome = reason_code
            self._connected_event.set()
            return
        if getattr(reason_code, "is_failure", False):
            return
        self._broker_state_changed(True)
        try:
            self._reannounce(client)
        except Exception:  # noqa: BLE001 - a callback that raises is swallowed by paho anyway
            logger.exception("chaski.Service: re-announcing %s after a reconnect failed", self.name)

    def _on_disconnect(self, *_args: Any, **_kwargs: Any) -> None:
        logger.warning("chaski.Service: %s disconnected from the broker (auto-reconnecting)", self.name)
        self._broker_state_changed(False)

    def _broker_state_changed(self, connected: bool) -> None:
        """Hook: the broker link came up (True) or went down (False)."""

    def _reannounce(self, client: Any) -> None:
        """On a reconnect (network thread): follow the registry's CURRENT
        placement — re-subscribe at it, republish ``_ServiceDetails`` there,
        and, if the position moved, forget the catalogue's last published
        revision so it republishes at the new topic. Publishes here go out
        without waiting (franzmq detects the network thread itself)."""
        old_filter = self._signal_filter
        old_topic = str(self._catalogue_topic)
        if not self._external:
            door = self._door
            identity = resolve_local_identity(
                self.name, host=door.host, http_port=door.http_port, mount=self._mount,
            )
            with self._lock:
                self._apply_placement(identity.mount, identity.hierarchy, identity.system_element_id or None)
                if str(self._catalogue_topic) != old_topic:
                    self._catalogue.mount = self._resolved_mount
                    self._catalogue.last_published_revision = None
                    self._catalogue.dirty = True
        if str(old_filter) != str(self._signal_filter):
            client.unsubscribe(old_filter)
        client.subscribe(self._signal_filter, qos=1, callback=self._on_signal)
        with self._lock:
            details = self._build_service_details(is_active=True, status=self._last_status,
                                                  detail=self._last_detail)
        client.publish(self._details_topic, details, qos=1, retain=True, wait=False)
        self._placement_reannounced()

    def _placement_reannounced(self) -> None:
        """Hook: a reconnect re-announced this service (catalogue may be due)."""

    def _previous_catalogue(self) -> Optional[dict[str, Any]]:
        """The retained ``_DataTags`` record this service published last time,
        read back from the node's KV under its own path — the memory that
        lets tag ids survive a restart with no local state (design §6).
        ``None`` when this service has never published one at this topic.
        A transport failure is raised, never read as "no previous
        catalogue": that would mint fresh ids and orphan every binding."""
        prefix = "/".join(self._hierarchy)
        wanted = str(self._catalogue_topic)
        for entry in self._http.kv(prefix, contract="_DataTags"):
            if entry.topic != wanted:
                continue
            payload = entry.payload
            if isinstance(payload, str):
                payload = json.loads(payload)
            return payload or None
        return None

    def _reset_after_failed_connect(self) -> None:
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
        self._client = None
        self._connected = False
        self._connected_event.clear()
        self._connect_outcome = None
        self._close_http()

    def _close_http(self) -> None:
        if self._http is not None:
            self._http.close()
            self._http = None

    def _after_connect(self) -> None:
        # The previous catalogue BEFORE the subscription: the retained
        # _Signal records that arrive on SUBSCRIBE name the ids of the last
        # run, and a binding for an id the catalogue does not know is
        # dropped as someone else's (see _on_signal).
        with self._lock:
            self._catalogue.load_previous(self._previous_catalogue())
        self._client.subscribe(self._signal_filter, qos=1, callback=self._on_signal)
        self._publish_details(self._build_service_details(is_active=True, status="healthy"))

    @property
    def node_id(self) -> Optional[str]:
        """The node this service is registered at — level 4 of every topic
        it writes. ``None`` before :meth:`start`."""
        return self._node_id

    @property
    def service_id(self) -> Optional[str]:
        """This service's identity at the node: the registry ULID minted at
        self-registration (local) or the pinned one (external) — what the
        catalogue's ``connector`` field carries. ``None`` before :meth:`start`."""
        return self._service_id

    @property
    def mount(self) -> str:
        """Where the node currently has this service (re-resolved on every
        reconnect for a local service)."""
        return self._resolved_mount

    def is_broker_connected(self) -> bool:
        """Whether the MQTT door is currently reachable — the fact a health
        endpoint reports. False before :meth:`start`; afterwards the real
        socket state, including an outage the caller is buffering against."""
        return self._client is not None and bool(self._client.is_connected())

    def enroll_hint(self) -> str:
        """The exact one-liner an operator runs to enroll this identity
        (also the text of a raised :class:`NotEnrolled`). Outside a
        deployment only."""
        if not self._external:
            raise RuntimeError(
                "chaski.Service.enroll_hint() only applies outside a deployment (node=<url>)"
            )
        position = f" at {self._mount!r}" if self._mount else ""
        return (
            f"enroll {self.name} at {self._node_url}: author an element{position} there, then "
            f"POST /enroll with the admin token and "
            f'{{"ulid": "{self.ulid}", "kind": "external", "element": "<that element id>", "pubkey": "{self.pubkey}"}}'
        )

    def wait_enrolled(self, timeout: float = 60.0, *, poll_interval: float = 2.0) -> "Service":
        """Poll ``start()`` — reconnecting — until the operator enrolls this
        identity, or ``timeout`` elapses. Outside a deployment only."""
        if not self._external:
            raise RuntimeError(
                "chaski.Service.wait_enrolled() only applies outside a deployment (node=<url>)"
            )
        deadline = time.monotonic() + timeout
        last_exc: Optional[Exception] = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                self.start(connect_timeout=min(10.0, max(1.0, remaining)))
                return self
            except NotEnrolled as exc:
                last_exc = exc
                time.sleep(min(poll_interval, max(0.0, deadline - time.monotonic())))
        raise TimeoutError(
            f"chaski.Service: {self.name} still not enrolled at {self._node_url} "
            f"after {timeout:.0f}s" + (f" ({last_exc})" if last_exc else "")
        )

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
        if self._client is None:
            raise RuntimeError(
                "chaski.Service: call start() (or use `with Service(...) as svc:`) before publish()"
            )
        with self._lock:
            tag_id, _changed = self._catalogue.ensure(path, value, unit)
            self._seen.add(path)
            catalogue = self._catalogue_to_publish()
            bound = self._bindings_for(tag_id)
            targets = [(b.topic, b.signal.id) for b in bound if b.signal.is_published]
            if not bound:
                self._buffer_sample(path, value, timestamp)
        # Outside the lock — see _publish_outside_the_lock. The catalogue goes
        # first either way: it is what makes the node mint the Signal this
        # sample binds to, so a buffered sample still has to publish it.
        if catalogue is not None:
            self._publish_catalogue(catalogue)
        # Bound to a Signal the node has switched off (is_published false):
        # the node said not to publish it, so the sample is neither sent nor
        # kept — buffering would hold it for a binding that already exists.
        for topic, signal_id in targets:
            self._publish_metric(topic, signal_id, value, timestamp)

    def _bindings_for(self, tag_id: str) -> list[Binding]:
        """Under the lock: every Signal currently bound to ``tag_id``."""
        return [b for b in self._bindings.values() if b.tag_id == tag_id]

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

    # -- consuming ---------------------------------------------------------

    def _require_http(self, method: str) -> Door:
        if self._closed:
            raise RuntimeError("chaski.Service is closed")
        if self._http is None:
            raise RuntimeError(
                f"chaski.Service: call start() (or use `with Service(...) as svc:`) before {method}()"
            )
        return self._http

    @property
    def cursor_prefix(self) -> str:
        """The cursor namespace this identity owns at the door — what
        :meth:`stream` prepends to a cursor name. ``c/{name}/`` for a local
        service, ``{ulid}/`` for an external one: the node's own rule
        (``plugins/uns`` ``Entry.CursorPrefix``), restated here only so a
        caller never has to know it — a cursor outside this prefix is
        refused by the door."""
        if self._external:
            return f"{self.ulid}/"
        return f"c/{self.name}/"

    def kv(
        self,
        prefix: str = "",
        *,
        contract: Union[str, Iterable[str], None] = None,
    ) -> list[KvEntry]:
        """A snapshot of the node's retained state under ``prefix`` — every
        page of ``GET /kv`` followed to the end — optionally narrowed to one
        or more uns contracts (``contract="_Signal"``,
        ``contract=["_SystemElement", "_Group"]``), which the node filters
        before decoding any payload. Only entries this identity may read
        are returned. Requires :meth:`start`."""
        return self._require_http("kv").kv(prefix, contract=contract)

    def stream(
        self,
        name: str,
        *,
        cursor: Optional[str] = None,
        max: int = 1000,
        signal_ids: Iterable[str] | None = None,
    ) -> Stream:
        """A named, durable cursor over the node's stream ``name``
        (``metrics``, ``annotations``, ``alarms``, ...) — see
        :class:`chaski.door.Stream` for the fetch → process → ack contract.

        ``cursor`` names the cursor within this service's own namespace
        (:attr:`cursor_prefix`) and defaults to the stream's name, so one
        consumer per stream needs no naming at all; a service following
        the same stream twice, or rebuilding its local state and wanting a
        fresh start (dataops' generational cursor), passes its own —
        ``svc.stream("metrics", cursor="ingest-02")`` — and retires the
        old one with ``Stream.retire()``. ``max`` bounds one page.
        ``signal_ids`` filters the ``metrics`` stream server-side (the door
        refuses it on any other stream). Requires :meth:`start`.
        """
        door = self._require_http("stream")
        return Stream(
            door, name, self.cursor_prefix + (cursor or name), max=max, signal_ids=signal_ids,
        )

    def pending(self) -> list[tuple[str, str]]:
        """``(path, reason)`` for every published path with no bound Signal
        yet — see :func:`_pending_reason` for what each reason means."""
        with self._lock:
            connector_id = self._catalogue.connector if self._catalogue is not None else ""
            return [
                (path, _pending_reason(path, self._resolved_mount, connector_id, connected=self._connected))
                for path in self._buffer
            ]

    # -- health --------------------------------------------------------

    def status(self, ok: bool, detail: str = "") -> None:
        """Republish ``_ServiceDetails`` with ``architecture_metadata.status``
        healthy/unhealthy (+``detail``) — what a health view reads."""
        if self._closed:
            raise RuntimeError("chaski.Service is closed")
        if self._client is None:
            raise RuntimeError(
                "chaski.Service: call start() (or use `with Service(...) as svc:`) before status()"
            )
        self._last_status = "healthy" if ok else "unhealthy"
        self._last_detail = detail
        with self._lock:
            details = self._build_service_details(is_active=True, status=self._last_status, detail=detail)
        self._publish_details(details)

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
        signal = message.payload
        if signal is None:
            # Tombstone: the signal was retired and its binding goes with it.
            with self._lock:
                if self._bindings.pop(topic_str, None) is not None:
                    logger.info("chaski.Service: binding retired: %s", topic_str)
                    self._bindings_changed()
            return
        tag_id = getattr(signal, "data_tag", None)
        with self._lock:
            source = self._catalogue.source_for_tag(tag_id) if tag_id else None
            if source is None:
                # Names a tag this service never minted — someone else's
                # binding, or a signal that was unbound from one of ours.
                if self._bindings.pop(topic_str, None) is not None:
                    self._bindings_changed()
                return
            # The metric belongs at the SIGNAL's own position: same node,
            # same path — no service-local address, no translation (design §6).
            metric_topic = Topic(payload_type=Metric, node_id=parts[3], context=tuple(parts[4:]))
            self._bindings[topic_str] = Binding(tag_id, metric_topic, signal)
            self._bindings_changed()
            # The buffer held samples for a path with no binding; it has one
            # now. Switched off by the node (is_published false), they are
            # dropped rather than kept for a binding that already exists.
            queued = self._buffer.pop(source, None)
            if not signal.is_published:
                queued = None
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

    def _bindings_changed(self) -> None:
        """Hook, under the lock: the binding table changed."""

    # -- catalogue ---------------------------------------------------------

    def _catalogue_to_publish(self) -> Optional[tuple[Any, Any]]:
        """Under the caller's lock: the catalogue that still needs publishing
        (payload and its revision), or None when nothing changed since the
        last decision or the last publish already carried this content.
        Decides only — the publish itself belongs outside the lock
        (:meth:`_publish_catalogue`)."""
        if not self._catalogue.dirty:
            return None
        payload = self._catalogue.payload()
        revision = self._catalogue.revision(payload)
        if revision == self._catalogue.last_published_revision:
            self._catalogue.dirty = False
            logger.debug("chaski.Service: catalogue unchanged (%s), not republished", payload.version[:12])
            return None
        return payload, revision

    def _publish_catalogue(self, prepared: tuple[Any, Any]) -> None:
        """OUTSIDE the lock — see _publish_outside_the_lock."""
        payload, revision = prepared
        self._client.publish(self._catalogue_topic, payload, qos=1, retain=True)
        with self._lock:
            self._catalogue.record_published(revision)

    # -- ServiceDetails --------------------------------------------------

    def _build_service_details(self, *, is_active: bool, status: str, detail: str = "") -> ServiceDetails:
        metadata: dict[str, Any] = dict(self.metadata)
        if self.version:
            metadata["version"] = self.version
        architecture_metadata: dict[str, Any] = {**self.architecture_metadata, "status": status}
        if detail:
            architecture_metadata["detail"] = detail
        else:
            architecture_metadata.pop("detail", None)
        return ServiceDetails(
            id=self._service_id,
            name=self.name,
            service_type=ServiceType.CONNECTOR,
            colca_node_id=self._node_id,
            display_name=self.display_name or "",
            description=self.description or "",
            system_element_id=self._system_element_id,
            hierarchy=list(self._hierarchy),
            is_active=is_active,
            metadata=metadata,
            architecture_metadata=architecture_metadata,
            health_metrics=list(self.health_metrics),
        )

    def _publish_details(self, details: ServiceDetails) -> None:
        """OUTSIDE the lock — see _publish_outside_the_lock."""
        self._client.publish(self._details_topic, details, qos=1, retain=True)

    # -- lifecycle: shutdown -----------------------------------------------

    def close(self) -> None:
        """Seal the catalogue (any known path not published this run goes
        stale), republish ``_ServiceDetails`` with ``is_active=False``, and
        disconnect. Registration and ids stay. Idempotent."""
        if self._closed:
            return
        self._closed = True
        with self._lock:
            client = self._client
            catalogue = None
            if client is not None:
                self._seal_catalogue()
                catalogue = self._catalogue_to_publish()
            details = (
                self._build_service_details(is_active=False, status=self._last_status)
                if client is not None
                else None
            )
        # Outside the lock — see _publish_outside_the_lock.
        if catalogue is not None:
            self._publish_catalogue(catalogue)
        if details is not None:
            self._publish_details(details)
        self._disconnect_client()

    def _seal_catalogue(self) -> None:
        """Under the lock, at close: the ``publish()`` path's end-of-run rule
        — a path not published this run goes stale. A discovery-driven
        catalogue (``ConnectorService``) has no such rule: what is stale
        there is decided by discovery, never by a shutdown."""
        self._catalogue.seal(self._seen)

    def retire(self, token: Optional[str] = None) -> None:
        """A deliberate end, not a restart: tombstone this service's own
        ``_ServiceDetails`` and ``_DataTags`` (empty retained payloads) so
        the tree forgets it entirely. Outside a deployment, also
        revokes the enrollment through the node's admin door
        (``DELETE /enroll``), which needs ``token`` (raises
        without one). Not part of the context manager: this is an explicit
        decision, never implied by ``__exit__``.
        """
        if self._external and not token:
            raise RuntimeError(
                "chaski.Service.retire() outside a deployment needs the node's admin token "
                "(it revokes the enrollment with DELETE /enroll)"
            )
        if self._closed:
            raise RuntimeError("chaski.Service: cannot retire() a closed service")
        self._closed = True
        with self._lock:
            client = self._client
        # Outside the lock — see _publish_outside_the_lock.
        if client is not None:
            client.publish_tombstone(self._details_topic, qos=1)
            client.publish_tombstone(self._catalogue_topic, qos=1)
        self._disconnect_client()
        if self._external and token:
            _revoke_external(self._node_url, self.ulid, token, api_port=self._api_port_override)

    def _disconnect_client(self) -> None:
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:  # noqa: BLE001 - best-effort teardown
                logger.debug("chaski.Service: disconnect raised during teardown", exc_info=True)
        self._connected = False
        self._close_http()

    def __enter__(self) -> "Service":
        return self.start()

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
