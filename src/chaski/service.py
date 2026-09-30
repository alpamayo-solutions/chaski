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

import contextlib
import contextvars
import datetime
import json
import logging
import math
import os
import socket
import ssl
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, cast
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

from ._wakeup import Wakeup
from .catalogue import Catalogue, element_for
from .clock import Clock, ClockNotReady
from .command import CommandSender, SentCommand
from .coordination import StepGate
from .door import Door, KvEntry, Record, Stream
from .failures import REJECTED_FINDING, UNHEALTHY, HandlerHealth, Reject, rejection_finding
from .pending import PendingSamples
from .subscriptions import Subscriptions
from .topic_wakeup import TopicFanout, TopicWakeup

if TYPE_CHECKING:
    from .retained_view import RetainedView
    from .stream_changes import StreamChanges

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
# Unbound paths backpressure at this durable queue bound.
_MAX_BUFFERED_PER_PATH = 100
_DEFAULT_EXTERNAL_MQTT_PORT = 8883
_DEFAULT_EXTERNAL_API_PORT = 443
_DEFAULT_LOCAL_HTTP_PORT = 80
_DEFAULT_LOCAL_MQTT_PORT = 1883

#: MQTT 5 DISCONNECT reason code a broker sends to a connection whose client
#: id another connection just claimed: the broker hands the session over.
SESSION_TAKEN_OVER = 0x8E
#: How long after the last takeover the identity conflict stands. Two
#: processes on one identity displace each other every second or two; once
#: none follows for this long, the other process has gone.
IDENTITY_CONFLICT_HOLD_S = 60.0


#: The deadline (unix seconds) writes from the current context must be sent by.
_write_deadline: contextvars.ContextVar[float | None] = contextvars.ContextVar("chaski_write_deadline", default=None)


class NotSent(RuntimeError):
    """A write refused before it was handed to the MQTT client, because its
    deadline had passed or the broker link was down. Nothing was queued, so
    nothing is sent after a reconnect."""


@contextlib.contextmanager
def writes_until(deadline: float | None) -> Iterator[None]:
    """Refuse :meth:`Service.send`/:meth:`Service.retract`/:meth:`Service.command`
    from this context (tasks and ``asyncio.to_thread`` calls inherit it) once
    ``deadline`` (unix seconds) has passed or while the broker link is down,
    instead of queueing the write for after a reconnect. A command sent
    before then expires, and is waited for, no later than ``deadline``.
    ``None`` sets no deadline."""
    token = _write_deadline.set(deadline)
    try:
        yield
    finally:
        _write_deadline.reset(token)


def write_deadline() -> float | None:
    """The deadline (unix seconds) writes from the current context must be
    sent by, as :func:`writes_until` set it; ``None`` without one. In an
    ``@on_command`` handler it is the command's ``expires_at``."""
    return _write_deadline.get()


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
    client.connect(host=host, port=port, clean_start=False)
    return client


#: Longest wait between two reconnect attempts. A broker that is back is
#: reached within this, so commands sent right after an outage do not expire.
RECONNECT_MAX_S = 5.0


def bound_reconnect(client: Any, maximum: float = RECONNECT_MAX_S) -> Any:
    """Make paho wait a jittered, growing delay of at most ``maximum`` seconds
    between reconnect attempts, instead of doubling up to two minutes.
    Returns the :class:`~chaski.retry.Backoff`; reset it on a CONNACK."""
    from .retry import Backoff

    backoff = Backoff(minimum=1.0, maximum=maximum)
    wait = getattr(client, "_reconnect_wait", None)
    if wait is None or not hasattr(client, "reconnect_delay_set"):
        return backoff

    def jittered_wait() -> None:
        delay = backoff.delay()
        client.reconnect_delay_set(min_delay=delay, max_delay=delay)
        wait()

    client._reconnect_wait = jittered_wait
    return backoff


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
        self._reconnect_backoff: Any = None
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
            StepGate(
                self,
                step_dependencies,
                self._state_dir / "clock-progress.json",
                asynchronous=os.environ.get("FACTORY_ASYNC_CONSUMER") == "true",
            )
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
        # Push wake-ups handed out by wake_on(); each rings after a reconnect.
        self._wakeups: list[TopicWakeup] = []
        self._fanout: TopicFanout | None = None
        # HTTP client for kv() and stream(), opened by start() on the same door
        # and identity as the MQTT client.
        self._http: Door | None = None
        self._stream_watches: list[StreamChanges] = []
        self._retained_views: list[RetainedView] = []
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
        self._pending_samples: PendingSamples | None = None
        self._pending_sources: set[str] = set()
        self._pending_wake = Wakeup()
        self._pending_stop = threading.Event()
        self._pending_thread = None
        # First CONNACK: start() waits on this; every later on_connect is a
        # reconnect and re-announces placement instead (see _on_connect).
        self._connected_event = threading.Event()
        self._reannounce_stop = threading.Event()
        self._connect_outcome: Any = None
        self._unbound_log_at: dict[str, float] = {}
        self._seen: set[str] = set()
        self._last_status = "healthy"
        self._last_detail = ""
        # What status() was last told, combined with handler_health into what
        # _ServiceDetails says (see _publish_status).
        self._user_ok = True
        self._user_detail = ""
        self.handler_health = HandlerHealth(on_change=self._handler_health_changed)
        self._rejected = 0
        self._command_sender: CommandSender | None = None
        # The node's cursor_lag finding about this service: its topic, and the
        # summary while it stands (see cursor_lag).
        self._lag_topic: str | None = None
        self._cursor_lag = ""
        # When the broker last handed this identity's session to another
        # connection (monotonic), and the timer that retires the conflict.
        self._taken_over_at: float | None = None
        self._conflict_timer: threading.Timer | None = None

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
        try:
            if self._external:
                self._start_external(connect_timeout)
            else:
                self._start_local(connect_timeout)
        except Exception:
            self._reset_after_failed_connect()
            raise
        if (self._state_dir / "pending-samples.sqlite3").exists():
            with self._lock:
                self._open_pending()
                pending = cast(PendingSamples, self._pending_samples)
                for path in self._pending_sources:
                    _, value, _, unit = pending.page(path, 1)[0]
                    self._started_catalogue.ensure(path, value, unit)
                    self._seen.add(path)
                catalogue = self._catalogue_to_publish()
            if catalogue is not None:
                self._publish_catalogue(catalogue)
            self._pending_wake.notify()
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
        self._reconnect_backoff = bound_reconnect(client)
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
        re-announce (:meth:`_reannounce`). Nothing here may wait for a PUBACK."""
        # Small ordered MQTT packets otherwise wait for TCP delayed ACKs
        # when a subscription acknowledgement precedes a publish. Apply on
        # every connection, including reconnects and TLS sockets.
        sock = client.socket() if hasattr(client, "socket") else None
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if self._reconnect_backoff is not None and not getattr(reason_code, "is_failure", False):
            self._reconnect_backoff.reset()
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
        with self._lock:
            wakeups = list(self._wakeups)
        for wakeup in wakeups:
            wakeup.reconnected()
        self._reannounce_stop.set()
        self._reannounce_stop = threading.Event()
        try:
            self._reannounce(client, fresh)
        except Exception as exc:
            logger.exception("chaski.Service: re-announcing %s after a reconnect failed; retrying", self.name)
            threading.Thread(
                target=self._retry_reannounce,
                args=(client, self._reannounce_stop, exc, fresh),
                daemon=True,
                name=f"{self.name}-registration-retry",
            ).start()

    def _retry_reannounce(self, client: Any, cancelled: threading.Event, error=None, fresh=False) -> None:
        # MQTT may accept connections before the HTTP registration door is ready.
        # Retry only this failed operation; do not wait on the network thread or
        # introduce a recurring registration poll once it succeeds.
        from .retry import Backoff

        retry = Backoff()
        while not cancelled.wait(retry.delay(error)):
            if self._closed:
                return
            try:
                self._reannounce(client, fresh)
                return
            except Exception as exc:
                error = exc
                logger.warning("chaski.Service: registration retry for %s failed", self.name, exc_info=True)

    def _on_disconnect(
        self, _client: Any = None, _userdata: Any = None, _flags: Any = None, reason_code: Any = None, *_rest: Any
    ) -> None:
        self._reannounce_stop.set()
        if getattr(reason_code, "value", reason_code) == SESSION_TAKEN_OVER:
            self._session_taken_over()
        else:
            logger.warning("chaski.Service: %s disconnected from the broker (auto-reconnecting)", self.name)
        self._broker_state_changed(False)

    def _session_taken_over(self) -> None:
        """Another connection claimed this service's MQTT client id: another
        process runs as the same service. The broker hands the one session
        back and forth, and one process's unsubscribes remove the other's
        subscriptions, so consumers stop being woken. Report it; which
        process should stop is the operator's decision."""
        logger.error(
            "chaski.Service: the broker handed %s's session to another connection: another process is "
            "running as %s on this node; both keep displacing each other and consumers may stop being woken "
            "until one of them stops",
            self.name,
            self.name,
        )
        with self._lock:
            if self._closed:
                return
            self._taken_over_at = time.monotonic()
            # The reconnect announces what this composes (see _reannounce).
            self._compose_status()
            if self._conflict_timer is not None:
                self._conflict_timer.cancel()
            timer = threading.Timer(IDENTITY_CONFLICT_HOLD_S, self._identity_conflict_expired)
            timer.daemon = True
            self._conflict_timer = timer
            timer.start()

    def _identity_conflict_expired(self) -> None:
        """No takeover for IDENTITY_CONFLICT_HOLD_S: report healthy again."""
        with self._lock:
            if self._closed or self.identity_conflict:
                return
            self._conflict_timer = None
            self._compose_status()
        if not self.is_broker_connected():
            return  # the reconnect announces the composed status
        try:
            self._publish_status()
        except Exception:
            logger.warning("chaski.Service: could not publish %s's status", self.name, exc_info=True)

    @property
    def identity_conflict(self) -> str:
        """Why another process is taken to run as this service, ``""`` while
        no session takeover happened in the last IDENTITY_CONFLICT_HOLD_S."""
        taken = self._taken_over_at
        if taken is None or time.monotonic() - taken >= IDENTITY_CONFLICT_HOLD_S:
            return ""
        return (
            f"another process is running as {self.name}: the broker handed its session to another connection "
            f"with the same client id (MQTT session taken over)"
        )

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
        self._subscribe_cursor_lag()
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
        for view in self._retained_views:
            view.close()
        self._retained_views.clear()
        for watch in self._stream_watches:
            watch.close()
        self._stream_watches.clear()
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
        self._subscribe_cursor_lag()

    @property
    def cursor_lag(self) -> str:
        """The summary of the node's ``cursor_lag`` finding about this service,
        ``""`` while there is none.

        The node (colca 0.19+) watches every cursor: when a record this
        service reads has waited unread longer than the node's threshold, it
        writes the finding next to the service's own record, and retires it
        once the cursor caught up. Nothing here reads on a timer, so this is
        how a lost wake or a stuck loop shows; a health check fails on it.
        """
        return self._cursor_lag

    def _lag_topic_now(self) -> str:
        return f"{topic_prefix()}_Finding/{self._node_id}/{'/'.join(self._hierarchy)}/cursor_lag"

    def _subscribe_cursor_lag(self) -> None:
        """Follow the node's cursor_lag finding at the current placement."""
        client = self._started_client
        topic = self._lag_topic_now()
        if topic == self._lag_topic:
            return
        if self._lag_topic is not None:
            client.unsubscribe(self._lag_topic)
            self._cursor_lag = ""
        self._lag_topic = topic
        client.subscribe(topic, qos=1, callback=self._on_cursor_lag)

    def _on_cursor_lag(self, message: Any) -> None:
        payload = message.payload
        if isinstance(payload, (bytes, str)):
            try:
                payload = json.loads(payload) if payload else None
            except ValueError:
                payload = {}
        if payload is None:
            summary = ""
        else:
            raw = payload.get("summary") if isinstance(payload, dict) else getattr(payload, "summary", None)
            summary = raw if isinstance(raw, str) and raw else "records wait unread on this service's cursor"
        if summary != self._cursor_lag:
            if summary:
                logger.warning("chaski.Service: %s: the node reports %s", self.name, summary)
            else:
                logger.info("chaski.Service: %s: the node reports its cursors caught up", self.name)
        self._cursor_lag = summary

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
        """Retry refused authentication until enrollment or the deadline.

        An unenrolled identity cannot subscribe yet. ``poll_interval`` is the
        compatibility name for the minimum retry backoff, not an idle poll.
        """
        if not self._external:
            raise RuntimeError("chaski.Service.wait_enrolled() only applies outside a deployment (node=<url>)")
        deadline = time.monotonic() + timeout
        from .retry import Backoff

        retry = Backoff(minimum=min(30.0, max(0.001, poll_interval)))
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
                time.sleep(min(retry.delay(exc), max(0.0, deadline - time.monotonic())))
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
            if not bound or (self._pending_samples is not None and path in self._pending_sources):
                self._buffer_sample(path, value, timestamp, unit)
                targets = []
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

    def _open_pending(self):
        if self._pending_samples is None:
            self._pending_samples = PendingSamples(self._state_dir / "pending-samples.sqlite3")
            self._pending_sources = set(self._pending_samples.sources())
            self._pending_thread = threading.Thread(
                target=self._drain_pending, name=f"{self.name}-pending", daemon=True
            )
            self._pending_thread.start()

    def _buffer_sample(self, path, value, timestamp, unit=None):
        self._open_pending()
        self._pending_samples.append(path, value, timestamp, unit, _MAX_BUFFERED_PER_PATH)
        self._pending_sources.add(path)
        self._pending_wake.notify()
        now = time.monotonic()
        if now - self._unbound_log_at.get(path, 0.0) >= _UNBOUND_LOG_INTERVAL:
            self._unbound_log_at[path] = now
            logger.warning("chaski.Service: %r queued durably pending binding/publication", path)

    def _drain_pending(self):
        from .retry import Backoff

        retry = Backoff()
        while not self._pending_stop.is_set():
            version = self._pending_wake.version
            progressed = False
            try:
                with self._lock:
                    paths = list(self._pending_sources)
                for path in paths:
                    with self._lock:
                        catalogue = self._started_catalogue
                        tag = next((tag for tag in catalogue.data_tags() if tag.source == path), None)
                        bound = self._bindings_for(tag.id) if tag else []
                        if not bound:
                            continue
                        targets = [(b.topic, b.signal.id) for b in bound if b.signal.is_published]
                        rows = self._pending_samples.page(path)
                    for _, value, timestamp, _ in rows:
                        for topic, signal_id in targets:
                            self._publish_metric(topic, signal_id, value, timestamp)
                    # Disabled bindings deliberately suppress publication. Otherwise
                    # every target has acknowledged before these rows are removed.
                    with self._lock:
                        self._pending_samples.ack([row[0] for row in rows])
                        if not self._pending_samples.page(path, 1):
                            self._pending_sources.discard(path)
                    progressed |= bool(rows)
                retry.reset()
            except Exception as exc:
                logger.warning("Pending sample publication failed; retaining rows: %s", type(exc).__name__)
                self._pending_stop.wait(retry.delay(exc))
                continue
            if not progressed:
                self._pending_wake.wait(version)

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
        cannot wait for its own PUBACK. Under :func:`writes_until`, a write
        past the deadline or while the link is down raises :class:`NotSent`.
        """
        # franzmq sends ``payload.encode()``, which a JSON string already has.
        self._writable("send", topic).publish(topic, payload, qos=1, retain=retain)

    def retract(self, topic: str) -> None:
        """Retire the state record at ``topic``: an empty retained payload,
        which the node keeps as a tombstone and drops from its KV."""
        self._writable("retract", topic).publish_tombstone(topic, qos=1)

    def _writable(self, method: str, topic: str) -> Any:
        """The client, unless a write deadline (:func:`writes_until`) refuses
        the write: queued now, it could reach the node after the deadline."""
        client = self._require_client(method)
        deadline = _write_deadline.get()
        if deadline is not None:
            if time.time() >= deadline:
                raise NotSent(f"{topic}: not sent, its deadline had passed")
            if not client.is_connected():
                raise NotSent(f"{topic}: not sent, the broker link is down")
        return client

    def send_command(
        self,
        contract: str,
        path: str,
        fields: dict[str, Any] | None = None,
        *,
        lifetime: float | None,
        node: str | None = None,
        progress: bool = False,
    ) -> SentCommand:
        """Send one command and return once the node accepted it (stored and
        queued); :meth:`SentCommand.wait` waits for its outcome. ``node`` is
        the node that executes it, this service's node when ``None``; ``path``
        is in this node's coordinates, a child's mount included. ``lifetime``
        is seconds until it expires, or ``None`` for a command that never
        expires and is delivered whenever its node is reachable. See
        :class:`chaski.command.CommandSender`. Under :func:`writes_until` the
        command expires by the deadline at the latest; past it or while the
        link is down it is not sent and raises :class:`NotSent`."""
        self._writable("command", f"{contract} {path}")
        deadline = _write_deadline.get()
        if deadline is not None:
            left = deadline - time.time()
            if left <= 0:
                raise NotSent(f"{contract} {path}: not sent, its deadline had passed")
            lifetime = left if lifetime is None else min(lifetime, left)
        return cast(CommandSender, self._command_sender).send(
            contract, path, fields, lifetime=lifetime, node=node, progress=progress
        )

    def command(
        self,
        contract: str,
        path: str,
        fields: dict[str, Any] | None = None,
        *,
        lifetime: float | None,
        node: str | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Send one command and return its outcome, the ``_Ack`` as the
        executor wrote it (``result_code``, ``message``, and whatever else it
        carries, such as ``state_writes``). :meth:`send_command` says what
        ``node``, ``path`` and ``lifetime`` mean. Waiting ends after
        ``timeout`` seconds with :class:`TimeoutError` and leaves the command
        queued. Under :func:`writes_until` it is also waited for until the
        deadline at the latest."""
        deadline = _write_deadline.get()
        if deadline is not None:
            timeout = min(timeout, deadline - time.time())
        return self.send_command(contract, path, fields, lifetime=lifetime, node=node).wait(timeout)

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

    def watch_streams(self, *streams):
        """Subscribe to local durable-stream changes. Closed with this service."""
        from .stream_changes import StreamChanges

        watch = StreamChanges(self._require_http("watch_streams"), streams).start()
        self._stream_watches.append(watch)
        return watch

    def retained_view(self, *, contracts, streams, cursor, scope, on_change=None):
        """Rebuildable retained view, updated from durable stream notifications.

        ``scope`` is a :class:`chaski.ViewScope` naming the paths the view
        reads: its snapshot, its stream drain and its unread count stay inside
        them. ``ViewScope.whole_node()`` reads every path and has to be asked
        for.
        """
        from .retained_view import RetainedView

        view = RetainedView(
            self._require_http("retained_view"),
            contracts,
            streams,
            self.cursor_prefix + cursor,
            scope=scope,
            on_change=on_change,
        ).start()
        self._retained_views.append(view)
        return view

    def stream(
        self,
        name: str,
        *,
        cursor: str | None = None,
        max: int = 1000,
        signal_ids: Iterable[str] | None = None,
        contracts: Iterable[str] | None = None,
        topics: Iterable[str] | None = None,
    ) -> Stream:
        """A named, durable cursor over the node's stream ``name``
        (``metrics``, ``annotations``, ``alarms``, ...) — see
        :class:`chaski.door.Stream` for the fetch → process → ack contract.

        ``cursor`` names the cursor within :attr:`cursor_prefix` and defaults
        to the stream's name. Pass another name to follow a stream twice or to
        start fresh (``svc.stream("metrics", cursor="ingest-02")``), and retire
        the old one with ``Stream.retire()``. ``max`` bounds one page.
        ``signal_ids`` filters the ``metrics`` stream at the door; ``contracts``
        (colca 0.18+) and ``topics`` (MQTT filters, colca 0.19+) any stream.
        Pass what wakes the consumer, so the node counts only those records as
        unread on its cursor. Requires :meth:`start`.
        """
        door = self._require_http("stream")
        return Stream(
            door,
            name,
            self.cursor_prefix + (cursor or name),
            max=max,
            signal_ids=signal_ids,
            contracts=contracts,
            topics=topics,
        )

    def pending(self) -> list[tuple[str, str]]:
        """``(path, reason)`` for every published path with no bound Signal
        yet — see :func:`_pending_reason` for what each reason means."""
        with self._lock:
            connector_id = self._catalogue.connector if self._catalogue is not None else ""
            return [
                (path, _pending_reason(path, self._resolved_mount, connector_id, connected=self._connected))
                for path in self._pending_sources
            ]

    # -- health --------------------------------------------------------

    def report_progress(self, processed_at: float, *, force: bool = False) -> bool:
        """Report application progress immediately when forced; health every five seconds.

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
            if self._closed or self._processed_at is None or (not force and now - self._last_clock_report < 5):
                return False
            processed_at = self._processed_at
            publish_details = now - self._last_clock_report >= 5
            if publish_details:
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
            details = (
                self._build_service_details(is_active=True, status=self._last_status, detail=self._last_detail)
                if publish_details or self.step is None
                else None
            )
        try:
            if self.step is not None and status.get("run_id"):
                # This marker travels in-order behind samples. Colca drains
                # priority events before forwarding it to an upstream node.
                marker_topic = str(self._details_topic).replace("/_ServiceDetails/", "/_ClockProgress/", 1)
                self.send(
                    marker_topic, json.dumps({"run_id": status["run_id"], "processed_at": processed_at}), retain=True
                )
            # Ordered progress is frequent; the full service projection only
            # needs a real-time heartbeat, even during accelerated simulation.
            if details is not None:
                self._publish_details(details)
        except Exception:
            logger.warning("Could not publish application clock progress", exc_info=True)
            return False
        return True

    def status(self, ok: bool, detail: str = "") -> None:
        """Republish ``_ServiceDetails`` with ``architecture_metadata.status``
        healthy/unhealthy (+``detail``) — what a health view reads.

        Combined with :attr:`handler_health`: a service whose handlers are
        failing reports that too, and is unhealthy once one failed
        ``unhealthy_after`` times in a row, whatever ``ok`` says."""
        if self._closed:
            raise RuntimeError("chaski.Service is closed")
        if self._client is None:
            raise RuntimeError("chaski.Service: call start() (or use `with Service(...) as svc:`) before status()")
        self._user_ok, self._user_detail = ok, detail
        self._publish_status()

    def _publish_status(self) -> None:
        with self._lock:
            self._compose_status()
            details = self._build_service_details(is_active=True, status=self._last_status, detail=self._last_detail)
        self._publish_details(details)

    def _compose_status(self) -> None:
        """What ``_ServiceDetails`` says: status() combined with
        :attr:`handler_health` and :attr:`identity_conflict`."""
        handlers = self.handler_health.status
        summary = self.handler_health.summary()
        conflict = self.identity_conflict
        ok = self._user_ok and handlers != UNHEALTHY and not conflict
        detail = "; ".join(
            part for part in (conflict, self._user_detail, f"handlers {handlers}: {summary}" if summary else "") if part
        )
        self._last_status = "healthy" if ok else "unhealthy"
        self._last_detail = detail[:500]

    def _handler_health_changed(self, _status: str, _summary: str) -> None:
        """A consumer started or stopped failing: republish the status, off
        the reporting thread (it may be an event loop or an MQTT callback)."""
        if self._client is None or self._closed:
            return

        def publish() -> None:
            try:
                self._publish_status()
            except Exception:
                logger.warning("chaski.Service: could not publish handler health", exc_info=True)

        threading.Thread(target=publish, name=f"{self.name}-handler-health", daemon=True).start()

    def _rejection_topic(self) -> str:
        return f"{topic_prefix()}_Finding/{self._node_id}/{'/'.join(self._hierarchy)}/{REJECTED_FINDING}"

    def reject(self, consumer: str, subject: dict[str, Any], rejected: Reject) -> None:
        """Record that ``consumer`` set an input aside on purpose, durably:
        the service's ``rejected_input`` ``_Finding``, retained, with the
        node's PUBACK awaited. Runners call this when a handler raises
        :class:`chaski.Reject`, and acknowledge the input only after it
        returned. Every rejection is one more record on the ``entities``
        stream; the retained record is the latest. :meth:`clear_rejections`
        retires it once the inputs were dealt with."""
        with self._lock:
            self._rejected += 1
            count = self._rejected
        payload = rejection_finding(consumer, subject, rejected, rejected=count)
        self.send(self._rejection_topic(), json.dumps(payload), retain=True)
        logger.warning("chaski.Service: %s rejected %s: %s", consumer, subject, rejected.reason)

    def clear_rejections(self) -> None:
        """Retire the ``rejected_input`` finding."""
        self.retract(self._rejection_topic())

    def wake_on(self, topics=()) -> TopicWakeup:
        """A push wake-up on exactly ``topics``, for :meth:`consume`'s ``bell``.

        A consumer that reads a few signals of a busy node drains a scoped
        stream (``stream(..., signal_ids=...)``) and passes
        ``wake_on(<their _Metric topics>).bell``: it wakes when one of them
        changes and at no other time. :meth:`TopicWakeup.rebind` follows a
        changed set; the subscriptions survive reconnects, and every reconnect
        rings once.
        """
        with self._lock:
            # One fanout per client: paho keeps a single callback per topic,
            # and two consumers of this service may want the same one.
            if self._fanout is None:
                self._fanout = TopicFanout(self._started_client)
            fanout = self._fanout
        wakeup = TopicWakeup(fanout, topics)
        with self._lock:
            self._wakeups.append(wakeup)
        return wakeup

    def consume(
        self,
        stream: Stream,
        handler: Callable[[Record], Any],
        *,
        bell: Any = None,
        stop: threading.Event | None = None,
        consumer: str | None = None,
    ) -> None:
        """Run ``handler(record)`` for every record of ``stream`` (from
        :meth:`stream`), now and whenever it grows, until ``stop`` is set.

        A handler that raises is not acknowledged: the cursor moves only past
        the records before it, the same record is retried with bounded,
        jittered backoff, and :attr:`handler_health` reports the consumer
        degraded, then unhealthy. Raise :class:`chaski.Reject` to set a
        record aside instead (see :meth:`reject`). ``bell`` is a
        :class:`chaski.Doorbell` rung by the stream's MQTT topics and on
        reconnect; without one the stream's growth is watched. Blocks.
        """
        from .consume import consume

        consume(
            stream,
            handler,
            health=self.handler_health,
            reject=self.reject,
            bell=bell,
            stop=stop,
            consumer=consumer,
        )

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
            self._pending_wake.notify()

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
        with self._lock:
            if self._conflict_timer is not None:
                self._conflict_timer.cancel()
                self._conflict_timer = None
        self._pending_stop.set()
        self._pending_wake.notify()
        if self._pending_thread is not None:
            self._pending_thread.join(timeout=15)
            if not self._pending_thread.is_alive():
                self._pending_samples.close()
        self._reannounce_stop.set()
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
        if self._subscriptions is not None:
            self._subscriptions.close()
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
