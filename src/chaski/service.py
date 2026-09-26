"""``chaski.Service``: publish data to a Colca node.

One constructor, ``Service(name, mount="", *, node=None, ...)``, where
``node`` says who the service is to Colca:

* ``node=None`` (the default): inside a deployment, on the local door
  (``colca:80``/``colca:1883``), without a credential, registered by name and
  mount.
* ``node="https://..."``: outside a deployment, on the published door, with an
  ed25519 identity the service loads or mints under its state directory.
* ``node=LocalDoor(...)``: an embedded node's local door, passed by
  ``chaski.Node.service()``.

``publish(path, value, unit=, timestamp=)`` works the same on every door.
Each path becomes a ``DataTag`` (see ``catalogue.py``), the catalogue is
republished when it changes, and each sample is written as a ``_Metric`` for
the Signal the node bound to its tag. Samples for a path without a Signal are
buffered, up to a limit, and flushed when one binds.

Construction does not connect; ``start()`` or the context manager does.
``start()`` reads the previously published catalogue from the node before
subscribing to the service's ``_Signal`` records, because those records name
the previous run's tag ids.

**Placement follows the registry.** Moving the service's entry reconnects its
MQTT session; the service then re-resolves its position, re-subscribes, and
republishes ``_ServiceDetails`` and its catalogue there, without a redeploy.

``ConnectorService`` (``chaski.connector``) adds a poll loop to this class.

**Reading.** A started service can also read: ``kv(prefix, contract=)``
returns a snapshot of the node's retained state, and ``stream(name)`` a
durable cursor over one of its streams (``chaski.door.Stream``), both with the
service's own identity.

**Records and commands.** Everything a service writes goes over its MQTT
session: ``send(topic, payload)`` publishes any other record at QoS 1 and
waits for the PUBACK, ``retract(topic)`` retires a state record, and
``command(contract, path, fields)`` sends a command to the service's node and
returns its ``_Ack``. HTTP is for reading.

**Bridges.** A bridge to an ERP or MES is a plain ``Service``: it polls the
foreign system and ``publish()``-es what it learns, and follows a stream with
``stream()`` to write back. What it may do comes from its enrollment grants.
"""

from __future__ import annotations

import datetime
import json
import logging
import math
import os
import ssl
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, NamedTuple, cast
from urllib.parse import urlsplit

import paho.mqtt.client as pahomqtt
import ulid as ulid_lib
from colca_data_contracts.local_service import (
    attach_log_publisher,
    connect_local_mqtt,
    resolve_local_identity,
)
from colca_data_contracts.payload import (
    ClockDefinition,
    DataTags,
    HealthMetricDeclaration,
    Metric,
    ServiceDetails,
    ServiceType,
    TimeSync,
)
from colca_data_contracts.payload import Signal as SignalRecord
from colca_data_contracts.root import topic_prefix
from colca_data_contracts.service_topics import service_context
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import NameOID
from franzmq import Client, Topic

from .catalogue import Catalogue, element_for
from .clock import Clock, ClockNotReady
from .command import CommandSender
from .coordination import StepGate
from .door import Door, KvEntry, Stream
from .subscriptions import Subscriptions

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


# How often the "still unbound" line repeats for a buffering path.
_UNBOUND_LOG_INTERVAL = 300.0
# A path without a Signal buffers at most this many samples, dropping the oldest.
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
        super().__init__(f"{name} is not enrolled at {node_url}. To enroll it:\n  {command}")
        self.command = command


@dataclass(frozen=True)
class LocalDoor:
    """A node's local door: host, HTTP port, MQTT port. ``chaski.Node.service()``
    builds one for an embedded node, and a containerised connector can build one
    from its environment; otherwise pass a URL or leave ``node`` unset."""

    host: str = "colca"
    http_port: int = _DEFAULT_LOCAL_HTTP_PORT
    mqtt_port: int = _DEFAULT_LOCAL_MQTT_PORT


def _epoch(ts: Any) -> float:
    """Accept a datetime, an epoch number, or None (now) and return epoch
    seconds; the wire contract wants a number, not an ISO string."""
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
    """TLS for the published door. There is no CA: trust is the pinned key
    presented as a client certificate, so the chain is not checked."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _pending_reason(path: str, mount: str, connector_id: str, *, connected: bool) -> str:
    """Why a buffered path has no bound Signal yet:

    * "not enrolled": outside a deployment, this identity has never connected.
    * "element not yet authored": the path names a parent element the node
      has to create first.
    * "awaiting binding": the element exists and the tag waits for
      `signal/autobind` or the node's autobind.
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
    """Load this service's external identity from ``identity_dir``, minting it
    on first use. Like ``colca-keygen -cert``: an ed25519 key (PEM PKCS8) and a
    self-signed certificate around it. The files and the ulid are written 0600."""
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
        now = datetime.datetime.now(datetime.UTC)
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

    pubkey_hex = (
        key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        .hex()
    )
    return ulid, pubkey_hex, key_path, cert_path


def _node_admin_base(node_url: str, api_port: int | None) -> tuple[str, str]:
    """(host, base https url) for a node's published API door."""
    parsed = urlsplit(node_url if "://" in node_url else f"https://{node_url}")
    host = parsed.hostname
    if not host:
        raise ValueError(f"chaski.Service: could not parse a host from {node_url!r}")
    port = api_port if api_port is not None else (parsed.port or _DEFAULT_EXTERNAL_API_PORT)
    return host, f"https://{host}:{port}"


def _read_node_id(healthz_url: str, timeout: float = 10.0) -> str:
    if not healthz_url.startswith("https://"):
        raise ValueError(f"chaski.Service: {healthz_url!r} is not an https URL")
    request = urllib.request.Request(healthz_url)  # noqa: S310 - https only, checked above
    with urllib.request.urlopen(request, timeout=timeout, context=_insecure_ssl_context()) as response:  # noqa: S310  # nosec B310
        payload = json.load(response)
    node_id = payload.get("ulid")
    if not node_id:
        raise RuntimeError(f"{healthz_url} did not report a node ulid")
    return str(node_id)


def _connect_external_mqtt(
    host: str,
    port: int,
    ulid: str,
    key_path: Path,
    cert_path: Path,
    *,
    will: tuple[Topic, ServiceDetails] | None = None,
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


def tolerate_undecodable(client: Any) -> None:
    """Make ``client`` survive a message franzmq cannot decode.

    franzmq decodes every inbound message on paho's network thread before
    dispatching, and a failed decode kills that thread, and with it every
    subscription on the client. On a failure the raw message goes to the
    matching callbacks instead, with a warning naming the topic. Idempotent.
    """
    typed_dispatch = client._handle_on_message
    if getattr(typed_dispatch, "_tolerates_undecodable", False):
        return
    raw_dispatch = pahomqtt.Client._handle_on_message

    def guarded(message: Any) -> Any:
        try:
            return typed_dispatch(message)
        except Exception as exc:
            logger.warning(
                "undecodable message on %s (%s: %s) — dispatching it undecoded instead",
                getattr(message, "topic", "?"),
                type(exc).__name__,
                exc,
            )
            try:
                return raw_dispatch(client, message)
            except Exception:
                logger.exception("message on %s dropped: its callback failed", getattr(message, "topic", "?"))
                return None

    guarded._tolerates_undecodable = True  # type: ignore[attr-defined]
    client._handle_on_message = guarded


def guard_network_thread(client: Any, name: str) -> None:
    """Keep ``client``'s network thread alive, or end the process with it.

    paho lets an exception from parsing an inbound packet escape its network
    thread, which then ends without a disconnect: the client still looks
    connected, but nothing is read or sent again. A packet that does not parse
    means the stream is misframed, so the connection is dropped instead and
    paho reconnects. If the thread dies anyway, the process exits non-zero so
    its supervisor restarts it. Call before ``loop_start``. Idempotent.
    """
    handle = getattr(client, "_packet_handle", None)
    if handle is None or getattr(handle, "_guarded", False):
        return  # no paho network loop to guard, or guarded already

    def guarded_handle() -> Any:
        try:
            return handle()
        except Exception:
            logger.exception(
                "chaski.Service: %s received an MQTT packet it cannot parse (command 0x%02x) — reconnecting",
                name,
                client._in_packet.get("command", 0),
            )
            return pahomqtt.MQTTErrorCode.MQTT_ERR_PROTOCOL

    guarded_handle._guarded = True  # type: ignore[attr-defined]
    client._packet_handle = guarded_handle

    thread_main = client._thread_main

    def guarded_main() -> None:
        try:
            thread_main()
        except BaseException:
            logger.critical("chaski.Service: the MQTT network thread of %s died — exiting", name, exc_info=True)
            os._exit(70)

    client._thread_main = guarded_main


def _revoke_external(
    node_url: str, ulid: str, token: str, *, api_port: int | None = None, timeout: float = 15.0
) -> None:
    """``DELETE /enroll/{ulid}`` on the node's admin door."""
    _, base = _node_admin_base(node_url, api_port)
    request = urllib.request.Request(  # noqa: S310 - _node_admin_base is always https
        f"{base}/enroll/{ulid}",
        method="DELETE",
        headers={"X-Colca-Token": token},
    )
    try:
        urllib.request.urlopen(request, timeout=timeout, context=_insecure_ssl_context())  # noqa: S310  # nosec B310
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"chaski.Service.retire(): revoking {ulid} at {node_url} failed: HTTP {exc.code}") from exc


class Service:
    """A publisher on either the local or the external door — one implementation,
    one constructor. See the module docstring for the lifecycle."""

    def __init__(
        self,
        name: str,
        mount: str = "",
        *,
        node: Any = None,
        display_name: str | None = None,
        description: str | None = None,
        version: str | None = None,
        logs: bool = True,
        state_dir: Path | None = None,
        mqtt_port: int | None = None,
        api_port: int | None = None,
        metadata: dict[str, Any] | None = None,
        architecture_metadata: dict[str, Any] | None = None,
        health_metrics: Iterable[HealthMetricDeclaration] | None = None,
        max_queued_messages: int = 0,
        clock: Clock | None = None,
        step_dependencies: list[str] | None = None,
        commands: Iterable[tuple[str, str]] | None = None,
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

        ``health_metrics`` names the Prometheus metrics that describe this
        service's health (``_ServiceDetails.health_metrics``); a container
        passes ``colca_data_contracts.container_resource_health_metrics()``.

        ``commands`` are the ``(contract, node-local path)`` pairs this service
        executes, announced in ``_ServiceDetails.commands`` so the node can
        answer a command nobody executes with a 404 ``_Ack``. A path may use
        ``+`` for one segment and a trailing ``#``. A
        :class:`~chaski.dataops.DataOpsService` announces its ``@on_command``
        handlers itself. See :meth:`announce_commands`.

        Construction does not dial the node; it only loads or mints an external
        identity on disk. See :meth:`start`.
        """
        self.name = name
        self.clock = clock or Clock()
        self._clock_subscriptions: set[str] = set()
        self._subscriptions: Subscriptions | None = None
        self._last_clock_report = float("-inf")
        self._processed_at: float | None = None
        self._progress_stop = threading.Event()
        self._progress_thread: threading.Thread | None = None
        self._max_queued_messages = max_queued_messages
        self._mount = mount
        self.display_name = display_name
        self.description = description
        self.version = version
        self.logs = logs
        self.metadata: dict[str, Any] = dict(metadata or {})
        self.architecture_metadata: dict[str, Any] = dict(architecture_metadata or {})
        self.health_metrics = list(health_metrics or [])
        self._announced_commands: list[tuple[str, str]] = sorted(set(commands or []))
        self._state_dir = Path(state_dir) if state_dir is not None else _default_state_dir(name)
        self.step = (
            StepGate(self, step_dependencies, self._state_dir / "clock-progress.json")
            if step_dependencies is not None
            else None
        )
        self._mqtt_port_override = mqtt_port
        self._api_port_override = api_port

        # _publish_outside_the_lock: this lock guards the catalogue, bindings
        # and buffer, and the MQTT network thread takes it too. A QoS 1 publish
        # waits for a PUBACK only that thread can read, so decide under the
        # lock and publish after releasing it.
        self._lock = threading.RLock()
        self._client: Client | None = None
        # HTTP client for kv() and stream(), opened by start() on the same door
        # and identity as the MQTT client.
        self._http: Door | None = None
        self._connected = False
        self._closed = False
        self._node_id: str | None = None
        self._service_id: str | None = None
        self._system_element_id: str | None = None
        # Where the node has this service: the mount, and the hierarchy (mount
        # and name) its records live under. Local services read it from /self
        # on every connect; external ones keep the constructor's mount.
        self._resolved_mount: str = mount
        self._hierarchy: tuple[str, ...] = ()
        self._catalogue: Catalogue | None = None
        self._catalogue_topic: Topic | None = None
        self._details_topic: Topic | None = None
        self._signal_filter: Topic | None = None
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
        self._command_sender: CommandSender | None = None

        if isinstance(node, LocalDoor):
            self._external = False
            self._door: LocalDoor | None = node
            self._node_url: str | None = None
            self.ulid: str | None = None
            self.pubkey: str | None = None
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
            self.ulid, self.pubkey, self._key_path, self._cert_path = _mint_identity(self._state_dir / "identity")
        else:
            raise TypeError(f"chaski.Service: node= must be None, a URL string, or LocalDoor, got {node!r}")

    # -- lifecycle -----------------------------------------------------

    def start(self, *, connect_timeout: float = 10.0) -> Service:
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
        door = self._local_door()
        identity = resolve_local_identity(
            self.name,
            host=door.host,
            http_port=door.http_port,
            mount=self._mount,
        )
        self._node_id = identity.node_id
        self._service_id = identity.service_id
        self._apply_placement(identity.mount, identity.hierarchy, identity.system_element_id or None)
        self._catalogue = Catalogue(connector=identity.service_id, mount=self._resolved_mount)
        self._http = Door(f"http://{door.host}:{door.http_port}", service=self.name)

        will_payload = self._build_service_details(is_active=False, status="unhealthy")
        try:
            client, _ = connect_local_mqtt(
                self.name,
                host=door.host,
                http_port=door.http_port,
                mqtt_port=door.mqtt_port,
                mount=self._mount,
                identity=identity,
                publish_logs=self.logs,
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
        node_url, ulid = self._external_identity()
        host, base = _node_admin_base(node_url, self._api_port_override)
        self._node_id = _read_node_id(f"{base}/healthz")
        self._service_id = ulid
        self._apply_placement(self._mount, service_context(self._mount, ulid), None)
        self._catalogue = Catalogue(connector=ulid, mount=self._mount)
        # The API door checks the same pinned key, as a client certificate.
        self._http = Door(base, service=ulid, cert=(self._cert_path, self._key_path))

        will_payload = self._build_service_details(is_active=False, status="unhealthy")
        port = self._mqtt_port_override or _DEFAULT_EXTERNAL_MQTT_PORT
        self._client = _connect_external_mqtt(
            host,
            port,
            ulid,
            self._key_path,
            self._cert_path,
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

    def _apply_placement(self, mount: str, hierarchy: tuple[str, ...], element_id: str | None) -> None:
        """Apply where the node says this service sits: the topics of its own
        records, and a ``_Signal`` filter at or below its element (a full
        wildcard would not be authorized)."""
        self._resolved_mount = mount
        self._hierarchy = tuple(hierarchy)
        self._system_element_id = element_id
        self._catalogue_topic = Topic(payload_type=DataTags, node_id=self._node_id, context=self._hierarchy)
        self._details_topic = Topic(
            payload_type=ServiceDetails,
            node_id=self._node_id,
            context=(*self._hierarchy, "_service"),
        )
        mount_parts = tuple(p for p in mount.split("/") if p)
        filter_context = (*mount_parts, "#") if mount_parts else ("#",)
        self._signal_filter = Topic(payload_type=SignalRecord, node_id=self._node_id, context=filter_context)

    def _connect_and_wait(self, connect_timeout: float) -> None:
        client = self._started_client
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        # before the loop: a persistent session delivers its queued messages
        # right after CONNACK, ahead of any subscription made in this run
        tolerate_undecodable(client)
        guard_network_thread(client, self.name)
        self._subscriptions = Subscriptions(client)
        client.loop_start()
        if not self._connected_event.wait(connect_timeout):
            raise TimeoutError(f"chaski.Service: no CONNACK from the broker within {connect_timeout}s")
        reason_code = self._connect_outcome
        if getattr(reason_code, "is_failure", False):
            if self._external:
                node_url, _ = self._external_identity()
                raise NotEnrolled(self.name, node_url, self.enroll_hint())
            raise RuntimeError(f"chaski.Service: broker refused CONNECT ({reason_code})")
        self._connected = True

    def _on_connect(self, client: Any, _userdata: Any, flags: Any, reason_code: Any, _properties: Any = None) -> None:
        """paho's CONNACK callback, on its network thread. The first one lets
        :meth:`start` finish on the caller's thread (:meth:`_after_connect`);
        later ones are reconnects, possibly after a re-placement, and
        re-announce (:meth:`_reannounce`). A reconnect to a node that lost the
        session subscribes everything again first. Nothing here may wait for
        a PUBACK."""
        if not self._connected_event.is_set():
            self._connect_outcome = reason_code
            self._connected_event.set()
            return
        if getattr(reason_code, "is_failure", False):
            logger.warning("chaski.Service: %s reconnect refused by the broker (%s)", self.name, reason_code)
            return
        fresh = not getattr(flags, "session_present", False)
        if fresh and self._subscriptions is not None:
            # _reannounce renews its own record's subscription after the announce
            restored = self._subscriptions.restore(later=[str(self._details_topic)])
            logger.info(
                "chaski.Service: %s reconnected on a new session; %d subscription(s) restored", self.name, restored
            )
        else:
            logger.info("chaski.Service: %s reconnected; the broker kept the session", self.name)
        self._broker_state_changed(True)
        try:
            self._reannounce(client, fresh)
        except Exception:
            logger.exception("chaski.Service: re-announcing %s after a reconnect failed", self.name)

    def _on_disconnect(self, *_args: Any, **_kwargs: Any) -> None:
        logger.warning("chaski.Service: %s disconnected from the broker (auto-reconnecting)", self.name)
        self._broker_state_changed(False)

    def _broker_state_changed(self, connected: bool) -> None:
        """Hook: the broker link came up (True) or went down (False)."""

    def _reannounce(self, client: Any, fresh: bool) -> None:
        """On a reconnect, on the network thread: re-subscribe at the current
        placement, republish ``_ServiceDetails``, and if the position moved,
        republish the catalogue at the new topic. Publishes here do not wait."""
        old_filter = self._signal_filter
        old_details = self._details_topic
        old_topic = str(self._catalogue_topic)
        if not self._external:
            door = self._local_door()
            identity = resolve_local_identity(
                self.name,
                host=door.host,
                http_port=door.http_port,
                mount=self._mount,
            )
            with self._lock:
                self._apply_placement(identity.mount, identity.hierarchy, identity.system_element_id or None)
                if str(self._catalogue_topic) != old_topic:
                    catalogue = self._started_catalogue
                    catalogue.mount = self._resolved_mount
                    catalogue.last_published_revision = None
                    catalogue.dirty = True
        if str(old_filter) != str(self._signal_filter):
            client.unsubscribe(old_filter)
            client.subscribe(self._signal_filter, qos=1, callback=self._on_signal)
        self._subscribe_clock()
        with self._lock:
            details = self._build_service_details(is_active=True, status=self._last_status, detail=self._last_detail)
        client.publish(self._details_topic, details, qos=1, retain=True, wait=False)
        if str(old_details) != str(self._details_topic):
            client.unsubscribe(old_details)
            client.subscribe(self._details_topic, qos=1, callback=self._on_own_details)
        elif fresh and self._subscriptions is not None:
            self._subscriptions.renew(str(self._details_topic))
        self._placement_reannounced()

    def _on_own_details(self, message: Any) -> None:
        """This service's own retained ``_ServiceDetails``, on the network
        thread. A last will from a replaced connection can land after the
        reconnect announced, and nothing else would correct it: re-announce
        once when the record reads inactive or deleted while this service is
        up. Its own active echo is a no-op."""
        payload = message.payload
        if isinstance(payload, (bytes, str)) and payload:
            try:
                payload = json.loads(payload)
            except ValueError:
                payload = None
        is_active = payload.get("is_active") if isinstance(payload, dict) else getattr(payload, "is_active", None)
        if is_active is True:
            return
        with self._lock:
            if self._closed:
                return
            logger.warning("chaski.Service: %s read its own record as inactive; re-announcing", self.name)
            details = self._build_service_details(is_active=True, status=self._last_status, detail=self._last_detail)
            # Under the lock, which close() takes after setting _closed: its
            # inactive record then always queues behind this one. No wait, so
            # no PUBACK is awaited under the lock.
            self._started_client.publish(self._details_topic, details, qos=1, retain=True, wait=False)

    def _placement_reannounced(self) -> None:
        """Hook: a reconnect re-announced this service (catalogue may be due)."""

    def _previous_catalogue(self) -> dict[str, Any] | None:
        """The ``_DataTags`` record this service published last time, from the
        node's KV, or ``None`` if there is none. A transport failure raises
        rather than returning ``None``, which would mint new ids and orphan
        every binding."""
        prefix = "/".join(self._hierarchy)
        wanted = str(self._catalogue_topic)
        for entry in self._require_http("kv").kv(prefix, contract="_DataTags"):
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
            except Exception:
                logger.debug("chaski.Service: cleanup after a failed connect raised", exc_info=True)
        self._client = None
        self._connected = False
        self._connected_event.clear()
        self._connect_outcome = None
        self._close_http()

    def _close_http(self) -> None:
        if self._http is not None:
            self._http.close()
            self._http = None

    @property
    def _started_client(self) -> Client:
        if self._client is None:
            raise RuntimeError("chaski.Service: call start() first")
        return self._client

    @property
    def _started_catalogue(self) -> Catalogue:
        if self._catalogue is None:
            raise RuntimeError("chaski.Service: call start() first")
        return self._catalogue

    def _local_door(self) -> LocalDoor:
        if self._door is None:
            raise RuntimeError("chaski.Service: a service outside a deployment has no local door")
        return self._door

    def _external_identity(self) -> tuple[str, str]:
        """The node URL and ulid of a service outside a deployment."""
        if self._node_url is None or self.ulid is None:
            raise RuntimeError("chaski.Service: a service inside a deployment has no external identity")
        return self._node_url, self.ulid

    def _after_connect(self) -> None:
        # Load the previous catalogue before subscribing: retained _Signal
        # records name the last run's ids, and bindings for unknown ids are
        # dropped (see _on_signal).
        self._command_sender = CommandSender(self._started_client, str(self._node_id))
        with self._lock:
            self._started_catalogue.load_previous(self._previous_catalogue())
        self._started_client.subscribe(self._signal_filter, qos=1, callback=self._on_signal)
        self._subscribe_clock()
        self._publish_details(self._build_service_details(is_active=True, status="healthy"))
        # after the announce, so the retained record it reads back is its own
        self._started_client.subscribe(self._details_topic, qos=1, callback=self._on_own_details)

    def _subscribe_clock(self) -> None:
        client = self._started_client
        for topic in self._clock_subscriptions:
            client.unsubscribe(topic)
        self._clock_subscriptions.clear()
        self.clock.reconnect()
        if self.clock.source == "mqtt":
            topic = f"{topic_prefix()}_TimeSync/{self.node_id}"
            client.subscribe(topic, qos=0, callback=self._on_time_sync)
            self._clock_subscriptions.add(topic)
        if self.clock.definition_topic:
            client.subscribe(self.clock.definition_topic, qos=1, callback=self._on_clock_definition)
            self._clock_subscriptions.add(self.clock.definition_topic)
        if self.step is not None:
            self.step.reconnect()
            for topic in self.step.topics():
                client.subscribe(topic, qos=1, callback=self.step.observe)
                self._clock_subscriptions.add(topic)

    def _on_time_sync(self, message: Any) -> None:
        try:
            payload = message.payload
            if isinstance(payload, (bytes, str)):
                payload = json.loads(payload)
            now_ms = payload.now_ms if isinstance(payload, TimeSync) else payload["now_ms"]
            self.clock.apply_time(now_ms, retained=bool(getattr(message, "retain", False)))
        except (ValueError, KeyError, TypeError):
            logger.warning("invalid hub time beacon", exc_info=True)

    def _on_clock_definition(self, message: Any) -> None:
        try:
            payload = message.payload
            if payload is None or payload == b"" or payload == "":
                self.clock.remove_definition()
                return
            if isinstance(payload, (bytes, str)):
                payload = json.loads(payload)
            definition = payload if isinstance(payload, ClockDefinition) else ClockDefinition(**payload)
            self.clock.apply_definition(definition)
        except (ValueError, TypeError):
            self.clock.remove_definition()
            logger.warning("invalid factory clock definition; application time suspended", exc_info=True)

    @property
    def node_id(self) -> str | None:
        """The node this service is registered at — level 4 of every topic
        it writes. ``None`` before :meth:`start`."""
        return self._node_id

    @property
    def service_id(self) -> str | None:
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
            raise RuntimeError("chaski.Service.enroll_hint() only applies outside a deployment (node=<url>)")
        position = f" at {self._mount!r}" if self._mount else ""
        return (
            f"enroll {self.name} at {self._node_url}: author an element{position} there, then "
            f"POST /enroll with the admin token and "
            f'{{"ulid": "{self.ulid}", "kind": "external", "element": "<that element id>", "pubkey": "{self.pubkey}"}}'
        )

    def wait_enrolled(self, timeout: float = 60.0, *, poll_interval: float = 2.0) -> Service:
        """Poll ``start()`` — reconnecting — until the operator enrolls this
        identity, or ``timeout`` elapses. Outside a deployment only."""
        if not self._external:
            raise RuntimeError("chaski.Service.wait_enrolled() only applies outside a deployment (node=<url>)")
        deadline = time.monotonic() + timeout
        last_exc: Exception | None = None
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
        unit: str | None = None,
        timestamp: Any | None = None,
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
            raise RuntimeError("chaski.Service: call start() (or use `with Service(...) as svc:`) before publish()")
        timestamp = self.clock.now() if timestamp is None else _epoch(timestamp)
        with self._lock:
            tag_id, _changed = self._started_catalogue.ensure(path, value, unit)
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
        self, topic: Topic, signal_id: str, value: Any, timestamp: Any | None, *, wait: bool = True
    ) -> None:
        metric = Metric(value=value, timestamp=_epoch(timestamp), signal_id=signal_id)
        self._started_client.publish(topic, metric, qos=1, wait=wait)

    def _buffer_sample(self, path: str, value: Any, timestamp: Any | None) -> None:
        queue = self._buffer.setdefault(path, deque(maxlen=_MAX_BUFFERED_PER_PATH))
        queue.append((value, timestamp))
        now = time.monotonic()
        last = self._unbound_log_at.get(path, 0.0)
        if now - last >= _UNBOUND_LOG_INTERVAL:
            self._unbound_log_at[path] = now
            logger.warning(
                "chaski.Service: %r has no bound Signal yet — buffering (%d queued, capped at %d)",
                path,
                len(queue),
                _MAX_BUFFERED_PER_PATH,
            )

    # -- records and commands ------------------------------------------------

    def _require_client(self, method: str) -> Any:
        if self._closed:
            raise RuntimeError("chaski.Service is closed")
        if self._client is None:
            raise RuntimeError(f"chaski.Service: call start() (or use `with Service(...) as svc:`) before {method}()")
        return self._client

    def send(self, topic: str, payload: str, *, retain: bool = False) -> None:
        """Publish one record at ``topic`` on this service's MQTT session, at
        QoS 1, and wait for the node's PUBACK.

        ``payload`` is the record as a JSON string. ``retain`` for a state
        record. A record the node refuses raises
        ``franzmq.errors.PublishRejected``; no PUBACK in time raises
        ``PublishTimeout``. Must not be called from an MQTT callback, which
        cannot wait for its own PUBACK.
        """
        # franzmq sends ``payload.encode()``, which a JSON string already has.
        self._require_client("send").publish(topic, payload, qos=1, retain=retain)

    def retract(self, topic: str) -> None:
        """Retire the state record at ``topic``: an empty retained payload,
        which the node keeps as a tombstone and drops from its KV."""
        self._require_client("retract").publish_tombstone(topic, qos=1)

    def command(
        self,
        contract: str,
        path: str,
        fields: dict[str, Any] | None = None,
        *,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Send one command to ``path`` on this service's node and return its
        ``_Ack`` as the executor wrote it (``result_code``, ``message``, and
        whatever else it carries, such as ``state_writes``). See
        :meth:`chaski.command.CommandSender.command`."""
        self._require_client("command")
        return cast(CommandSender, self._command_sender).command(contract, path, fields, timeout=timeout)

    # -- consuming ---------------------------------------------------------

    def _require_http(self, method: str) -> Door:
        if self._closed:
            raise RuntimeError("chaski.Service is closed")
        if self._http is None:
            raise RuntimeError(f"chaski.Service: call start() (or use `with Service(...) as svc:`) before {method}()")
        return self._http

    @property
    def cursor_prefix(self) -> str:
        """The cursor namespace this identity owns, which :meth:`stream`
        prepends: ``c/{name}/`` for a local service, ``{ulid}/`` for an external
        one. The door refuses cursors outside it."""
        if self._external:
            return f"{self.ulid}/"
        return f"c/{self.name}/"

    def kv(
        self,
        prefix: str = "",
        *,
        contract: str | Iterable[str] | None = None,
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
        cursor: str | None = None,
        max: int = 1000,
        signal_ids: Iterable[str] | None = None,
        contracts: Iterable[str] | None = None,
    ) -> Stream:
        """A named, durable cursor over the node's stream ``name``
        (``metrics``, ``annotations``, ``alarms``, ...) — see
        :class:`chaski.door.Stream` for the fetch → process → ack contract.

        ``cursor`` names the cursor within :attr:`cursor_prefix` and defaults
        to the stream's name. Pass another name to follow a stream twice or to
        start fresh (``svc.stream("metrics", cursor="ingest-02")``), and retire
        the old one with ``Stream.retire()``. ``max`` bounds one page.
        ``signal_ids`` filters the ``metrics`` stream at the door, ``contracts``
        any stream (colca 0.18+). Requires :meth:`start`.
        """
        door = self._require_http("stream")
        return Stream(
            door,
            name,
            self.cursor_prefix + (cursor or name),
            max=max,
            signal_ids=signal_ids,
            contracts=contracts,
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

    def report_progress(self, processed_at: float, *, force: bool = False) -> bool:
        """Report application progress at most once per real second.

        This is control/health state, not a business or historian fact. A
        simulation must report what it processed, not just its target clock.
        Reporting is best effort: a broker outage must not abort completed work.
        Returns whether the status was published. Business progress must already
        be checkpointed by the caller before calling this method.
        """
        if (
            isinstance(processed_at, bool)
            or not isinstance(processed_at, (int, float))
            or not math.isfinite(processed_at)
        ):
            raise ValueError("processed_at must be finite")
        with self._lock:
            if self._closed:
                return False
            if self._client is None:
                raise RuntimeError("chaski.Service: call start() before report_progress()")
            self._processed_at = processed_at
            if self._progress_thread is None:
                self._progress_thread = threading.Thread(
                    target=self._progress_heartbeat, daemon=True, name=f"{self.name}-clock-health"
                )
                self._progress_thread.start()
        return self._publish_progress(force=force)

    def _progress_heartbeat(self) -> None:
        # Real-time liveness continues while factory work is paused. The last
        # checkpoint does not advance unless the worker reports actual work.
        while not self._progress_stop.wait(5):
            self._publish_progress()

    def _publish_progress(self, *, force: bool = False) -> bool:
        now = time.monotonic()
        with self._lock:
            if self._closed or self._processed_at is None or (not force and now - self._last_clock_report < 1):
                return False
            processed_at = self._processed_at
            self._last_clock_report = now
        status = asdict(self.clock.status())
        status["processed_at"] = processed_at
        try:
            status["observed_at"] = self.clock.real_now()
        except ClockNotReady:
            status["observed_at"] = None
        status["lag_s"] = max(0, status["factory_now"] - processed_at) if status["factory_now"] is not None else None
        with self._lock:
            self.metadata["application_clock"] = status
            details = self._build_service_details(is_active=True, status=self._last_status, detail=self._last_detail)
        try:
            if self.step is not None and status.get("run_id"):
                # This marker travels in-order behind samples. Colca drains
                # priority events before forwarding it to an upstream node.
                marker_topic = str(self._details_topic).replace("/_ServiceDetails/", "/_ClockProgress/", 1)
                self.send(
                    marker_topic, json.dumps({"run_id": status["run_id"], "processed_at": processed_at}), retain=True
                )
            self._publish_details(details)
        except Exception:
            logger.warning("Could not publish application clock progress", exc_info=True)
            return False
        return True

    def status(self, ok: bool, detail: str = "") -> None:
        """Republish ``_ServiceDetails`` with ``architecture_metadata.status``
        healthy/unhealthy (+``detail``) — what a health view reads."""
        if self._closed:
            raise RuntimeError("chaski.Service is closed")
        if self._client is None:
            raise RuntimeError("chaski.Service: call start() (or use `with Service(...) as svc:`) before status()")
        self._last_status = "healthy" if ok else "unhealthy"
        self._last_detail = detail
        with self._lock:
            details = self._build_service_details(is_active=True, status=self._last_status, detail=detail)
        self._publish_details(details)

    # -- signal binding ------------------------------------------------

    def _on_signal(self, message: Any) -> None:
        """Match a ``_Signal`` to this service's tags by ``data_tag``. Its topic
        path is not the ``path`` given to ``publish()``, so it cannot be the key.
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
            source = self._started_catalogue.source_for_tag(tag_id) if tag_id else None
            if not tag_id or source is None:
                # Names a tag this service never minted — someone else's
                # binding, or a signal that was unbound from one of ours.
                if self._bindings.pop(topic_str, None) is not None:
                    self._bindings_changed()
                return
            # The metric goes to the Signal's own position: same node and path.
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
            # This runs on the MQTT network thread, which cannot wait for its own
            # PUBACK, so publish without waiting; franzmq's own detection does not
            # cover this callback path.
            for value, timestamp in queued:
                self._publish_metric(metric_topic, signal.id, value, timestamp, wait=False)

    def _bindings_changed(self) -> None:
        """Hook, under the lock: the binding table changed."""

    # -- catalogue ---------------------------------------------------------

    def _catalogue_to_publish(self) -> tuple[Any, Any] | None:
        """Under the caller's lock: the catalogue that still needs publishing
        (payload and its revision), or None when nothing changed since the
        last decision or the last publish already carried this content.
        Decides only — the publish itself belongs outside the lock
        (:meth:`_publish_catalogue`)."""
        catalogue = self._started_catalogue
        if not catalogue.dirty:
            return None
        payload = catalogue.payload()
        revision = catalogue.revision(payload)
        if revision == catalogue.last_published_revision:
            catalogue.dirty = False
            logger.debug("chaski.Service: catalogue unchanged (%s), not republished", payload.version[:12])
            return None
        return payload, revision

    def _publish_catalogue(self, prepared: tuple[Any, Any]) -> None:
        """OUTSIDE the lock — see _publish_outside_the_lock."""
        payload, revision = prepared
        self._started_client.publish(self._catalogue_topic, payload, qos=1, retain=True)
        with self._lock:
            self._started_catalogue.record_published(revision)

    # -- ServiceDetails --------------------------------------------------

    def announce_commands(self, commands: Iterable[tuple[str, str]]) -> None:
        """Announce the ``(contract, node-local path)`` commands this service
        executes, replacing what it announced before, and republish
        ``_ServiceDetails`` when started. Announce before :meth:`start` where
        possible: the last will is built at connect and carries what was
        announced then, so a crashed service keeps its commands."""
        routes = sorted(set(commands))
        with self._lock:
            changed = routes != self._announced_commands
            self._announced_commands = routes
            details = (
                self._build_service_details(is_active=True, status=self._last_status, detail=self._last_detail)
                if changed and self._client is not None and self._node_id is not None
                else None
            )
        if details is not None:
            self._publish_details(details)

    def _build_service_details(self, *, is_active: bool, status: str, detail: str = "") -> ServiceDetails:
        metadata: dict[str, Any] = dict(self.metadata)
        if self.version:
            metadata["version"] = self.version
        architecture_metadata: dict[str, Any] = {**self.architecture_metadata, "status": status}
        if detail:
            architecture_metadata["detail"] = detail
        else:
            architecture_metadata.pop("detail", None)
        if self._service_id is None or self._node_id is None:
            raise RuntimeError("chaski.Service: call start() first")
        details = ServiceDetails(
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
        if self._announced_commands:
            # colca-data-contracts before 0.18 has no field for it; the record
            # carries it all the same.
            details.__dict__["commands"] = [
                {"contract": contract, "path": path} for contract, path in self._announced_commands
            ]
        return details

    def _publish_details(self, details: ServiceDetails) -> None:
        """OUTSIDE the lock — see _publish_outside_the_lock."""
        self._started_client.publish(self._details_topic, details, qos=1, retain=True)

    # -- lifecycle: shutdown -----------------------------------------------

    def close(self) -> None:
        """Seal the catalogue (any known path not published this run goes
        stale), republish ``_ServiceDetails`` with ``is_active=False``, and
        disconnect. Registration and ids stay. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._stop_progress()
        with self._lock:
            client = self._client
            catalogue = None
            if client is not None:
                self._seal_catalogue()
                catalogue = self._catalogue_to_publish()
            details = (
                self._build_service_details(is_active=False, status=self._last_status) if client is not None else None
            )
        # Outside the lock — see _publish_outside_the_lock.
        if catalogue is not None:
            self._publish_catalogue(catalogue)
        if details is not None:
            self._publish_details(details)
        self._disconnect_client()

    def _stop_progress(self) -> None:
        self._progress_stop.set()
        if self._progress_thread is not None:
            self._progress_thread.join(timeout=10)

    def _seal_catalogue(self) -> None:
        """Under the lock, at close: the ``publish()`` path's end-of-run rule
        — a path not published this run goes stale. A discovery-driven
        catalogue (``ConnectorService``) has no such rule: what is stale
        there is decided by discovery, never by a shutdown."""
        self._started_catalogue.seal(self._seen)

    def retire(self, token: str | None = None) -> None:
        """Remove this service for good: publish empty retained
        ``_ServiceDetails`` and ``_DataTags`` so the tree forgets it. Outside a
        deployment it also revokes the enrollment (``DELETE /enroll``), which
        needs ``token``. The context manager never calls this.
        """
        if self._external and not token:
            raise RuntimeError(
                "chaski.Service.retire() outside a deployment needs the node's admin token "
                "(it revokes the enrollment with DELETE /enroll)"
            )
        if self._closed:
            raise RuntimeError("chaski.Service: cannot retire() a closed service")
        self._closed = True
        self._stop_progress()
        with self._lock:
            client = self._client
        # Outside the lock — see _publish_outside_the_lock.
        if client is not None:
            client.publish_tombstone(self._details_topic, qos=1)
            client.publish_tombstone(self._catalogue_topic, qos=1)
        self._disconnect_client()
        if self._external and token:
            node_url, ulid = self._external_identity()
            _revoke_external(node_url, ulid, token, api_port=self._api_port_override)

    def _disconnect_client(self) -> None:
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                logger.debug("chaski.Service: disconnect raised during teardown", exc_info=True)
        self._connected = False
        self._close_http()

    def __enter__(self) -> Service:
        return self.start()

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
