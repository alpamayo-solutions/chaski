"""``chaski.ConnectorService``: a :class:`chaski.Service` that polls a source.

A connector's tags come from discovery rather than ``publish()`` calls.
Everything that is not protocol-specific lives here: the catalogue, learning
which tags are bound to Signals, polling on a fixed cadence, rounding to the
Signal's precision, publishing on change, a heartbeat and a connectivity flag,
buffering through a broker outage and reconnecting with backoff. A connector
for a new protocol only writes a driver.

The driver protocol is four ``async`` methods (:class:`Driver`):

* ``connect()`` — open the source; raise on failure (the loop retries).
* ``discover()`` — the source's tags: ``{source: DataTag}`` plus, per
  source, whatever handle ``read`` needs to read it (an OPC UA node, a
  register descriptor, a JSON pointer).
* ``read(targets)`` — one poll of the bound targets: ``(topic, raw_value,
  signal)`` per reading. Raise :class:`SourceDisconnectedError` when the
  source is gone; the loop flips ``is_connected`` and reconnects. The
  heartbeat keeps its own schedule meanwhile: it never waits for a read.
* ``close()`` — release the source.

and one optional method:

* ``write(target, value, command)`` — write one value to one tag. The
  default raises :class:`WriteUnsupported`; a read-only driver leaves it
  alone. ``command`` is the :class:`~chaski.executor.Command` being executed.

Ids, topics, payloads and timing are the base class's and the loop's; a
driver never sees a tag id, a topic it has to build, or a Metric.

**Timing.** Each poll stamps every reading with one epoch, taken at the top
of the iteration; the next iteration starts ``interval`` after that top
(drift-compensated), and an iteration that overran is counted and the next
one starts immediately. A poll cycle that could not publish (broker down)
journals every metric before publication. It drains that bounded journal before
acquiring again, and preserves it through restart. Queue saturation is visible
backpressure, never eviction.

**Refused samples.** Transport failures and anything the node did not decide
are retried; the journal keeps the samples. A sample the node definitively
refuses (its schema, its topic rules, a missing grant, another producer's
signal; see :func:`refusal_is_final`) can never be admitted, so retrying it
would only block the samples behind it. It is set aside instead: recorded as
the service's retained ``rejected_input`` finding, counted
(:attr:`ConnectorService.refused_samples_total`, :meth:`Telemetry.sample_refused`)
and only then removed from the journal. A non-finite reading (NaN, ±Inf) is
refused the same way before it is journaled: JSON has no such numbers, so no
contract can carry one. Each signal is logged and recorded once per spell of
refusals; the spell ends when one of its samples is admitted again.

**Signal writes.** The standard way to set a signal is a ``_CmdParam``
command at the signal's own path (``<element path>/<signal>``), with
``{"command": {"value": ...}}``. The connector that holds the signal's
binding executes it: it announces a route per bound signal, writes through
``Driver.write``, reads the tag back and answers only then. The ``_Ack``
carries the result: ``200`` with ``result.outcome`` ``applied`` and the
value read back; ``409`` ``failed`` when the value read back differs; ``502``
``failed`` when the source refused the write; ``503`` ``failed`` when the
source is not connected; ``504`` ``unknown`` when the write went out but the
read-back failed; ``501`` ``unsupported`` when the driver cannot write;
``422`` ``refused`` for a read-only tag or a command without a value; and,
from the executor, ``498`` when the command expired before it ran. Who may
write a signal is a colca ``cmd`` grant with the ``param`` class on its path.

**Who wrote it, and once.** The driver gets the whole command as
``Driver.write``'s third argument: the attested sender
(``command.sender``), the person it acts for (``command.on_behalf_of``,
asserted by the sender), the ``operation_id`` and ``correlation_id``,
``expires_at`` and every ``command`` field beside ``value``
(``command.params``). A write sent with an ``operation_id`` is recorded on
disk under it before the driver is called; a repeat is answered from that
record and never reaches the driver (:mod:`chaski.executor`, "Operations").
The ``_Ack`` repeats ``operation_id`` and ``on_behalf_of``.

**Taking over a write.** :meth:`ConnectorService.handle_signal_write` runs
every write, after the connector checked the binding and the tag. Its
default is :meth:`SignalWrite.apply`: write ``command.params["value"]``, read
back, answer. A subclass overrides it to check the sender, to write
something derived from the command (a recipe of several values on one
signal: ``write.apply(value=...)``), or to add to the answer: return a
:class:`~chaski.executor.CommandResult`, or raise
:class:`~chaski.executor.CommandRejected`, with more ``result`` fields.

**Not included.** Metrics exposition: the loop reports to a
:class:`Telemetry`, a no-op by default. Configuration from the
environment: the process that builds a connector reads its own.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import functools
import json
import logging
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from decimal import ROUND_HALF_UP, Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from math import isclose, isfinite, isnan
from types import MappingProxyType
from typing import Any, Literal, NamedTuple

import httpx
from colca_data_contracts.payload import DataTag, Metric
from colca_data_contracts.payload import Signal as SignalRecord
from franzmq import Topic
from franzmq.errors import PublishRejected, PublishTimeout

from .executor import Command, CommandExecutor, CommandRejected, CommandResult, SqliteLedger, parse_topic
from .failures import Reject
from .service import Service

# Synthetic tags have stable source keys; the catalogue mints and reuses their
# ids like any other tag's.
HEARTBEAT_TAG_SOURCE = "__heartbeat__"
HEARTBEAT_TAG_NAME = "heartbeat"
#: Every connector exposes a boolean reflecting its source's reachability:
#: True after a successful read, False after a SourceDisconnectedError or a
#: failed discovery. Published on change, per bound Signal.
IS_CONNECTED_TAG_SOURCE = "__is_connected__"
IS_CONNECTED_TAG_NAME = "is_connected"

#: The contract of a signal write, and the cursor its executor reads with.
WRITE_CONTRACT = "_CmdParam"
WRITE_CURSOR = "signal-writes"

#: How long after a failed startup discovery the loop retries it. The retry
#: is a full connect + discover, not just a reconnect: a browse-based driver
#: that started while its source was still booting has no catalogue at all.
DISCOVERY_RETRY_SECONDS = 15.0

#: The consumer name a refused sample is recorded under in the service's
#: ``rejected_input`` finding.
REFUSED_SAMPLES_CONSUMER = "connector-samples"

#: colca's rejection reasons (``colca_rejected_publishes_total{reason}``) that
#: are a verdict on the record itself: sending it again gets the same answer.
#: ``draining`` is temporary and any reason not listed here is retried.
FINAL_REFUSAL_REASONS = frozenset(
    {
        "validation",
        "grammar",
        "node_id",
        "identity",
        "write_denied",
        "cmd_denied",
        "registry_contract",
        "human_write",
        "time_sync",
        "not_producer",
    }
)

_MISSING = object()


class SourceDisconnectedError(Exception):
    """Raise from a driver's ``read`` (or ``connect``/``discover``) when the
    source connection is lost. The loop publishes ``is_connected=False``,
    reconnects with backoff, and keeps trying on later polls. Don't swallow
    connection errors in the driver, or the flag stays True."""


class WriteUnsupported(Exception):
    """Raised by :meth:`Driver.write` when the driver cannot write (the
    default). The write is answered ``501`` ``unsupported``."""


class MqttDisconnectedError(Exception):
    """The broker is unreachable mid-publish; raised by the publish path for
    the loop's reconnect handling."""


class Target(NamedTuple):
    """One bound, published Signal to read: the Signal record (its id and
    precision), the driver's own handle for the tag it names, and the
    ``_Metric`` topic the reading goes to. A tuple, so a driver may unpack
    it ``for signal, handle, topic in targets``."""

    signal: SignalRecord
    handle: Any
    topic: Topic


class Reading(NamedTuple):
    """What a driver returns per read: the target's topic, the raw value,
    the target's Signal. The loop rounds, deduplicates and stamps it."""

    topic: Topic
    value: Any
    signal: SignalRecord
    timestamp: float | None = None


class Discovery(NamedTuple):
    """What ``Driver.discover`` returns: the tags by ``source`` (``id`` is
    ignored — the catalogue mints and keeps ids) and, by the same source,
    the handle ``read`` needs for that tag."""

    tags: Mapping[str, DataTag]
    handles: Mapping[str, Any]


class Driver:
    """The protocol half of a connector. Subclass it, implement the four
    ``async`` methods, hand an instance to :class:`ConnectorService`."""

    #: Short label for logs and telemetry (``opcua``, ``modbus``, ``http``).
    protocol: str = "unknown"
    #: Merged into the service's ``_ServiceDetails.metadata``.
    metadata: Mapping[str, Any] = MappingProxyType({})
    #: Whether the catalogue needs a connection to the source: True for
    #: browse-based protocols such as OPC UA. Drivers with a configured tag list
    #: (S7, Modbus) set False, so their tags can be bound before the machine is
    #: connected.
    catalogue_requires_connection: bool = True

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(f"{__name__}.{type(self).__name__}")

    async def connect(self) -> None:
        """Open the source. Raise on failure, with a message naming what
        failed (host, port, endpoint) — the loop logs it and retries."""
        raise NotImplementedError

    async def discover(self) -> Discovery:
        """Discover the source's tags. Called after ``connect`` (or without
        it, when ``catalogue_requires_connection`` is False)."""
        raise NotImplementedError

    async def read(self, targets: list[Target]) -> Iterable[tuple[Topic, Any, SignalRecord] | Reading]:
        """One poll of ``targets``. A target a driver could not read is
        simply absent from the result; a lost source raises
        :class:`SourceDisconnectedError`."""
        raise NotImplementedError

    async def close(self) -> None:
        """Release the source. Must tolerate being called on a half-open
        or already-closed connection."""
        raise NotImplementedError

    async def write(self, target: Target, value: Any, command: Command) -> None:
        """Write ``value`` to the tag ``target`` names and return once the
        source accepted it. The connector reads the tag back through
        :meth:`read` before it answers. ``command`` is the signal write being
        executed: who sent it and for whom, its ``operation_id``, its expiry
        and all its fields (module docstring, "Who wrote it, and once").
        Raise :class:`SourceDisconnectedError` when the source is gone and
        any other exception when the source refused the write. The default
        raises :class:`WriteUnsupported`."""
        raise WriteUnsupported(f"the {self.protocol} connector does not write to its source")


class SignalWrite:
    """One signal write as :meth:`ConnectorService.handle_signal_write` gets
    it: the command, the bound ``target`` and its ``tag``, already checked
    (bound here, writable, available at the source)."""

    def __init__(self, connector: ConnectorService, command: Command, target: Target, tag: DataTag) -> None:
        self.connector = connector
        self.command = command
        self.target = target
        self.tag = tag

    @property
    def path(self) -> str:
        return self.command.path

    async def apply(self, value: Any = _MISSING) -> CommandResult:
        """The standard write: ``value`` (the command's ``value`` when not
        given) through ``Driver.write``, read back, compared. Returns the
        ``applied`` answer or raises :class:`~chaski.executor.CommandRejected`
        with the outcome (module docstring, "Signal writes")."""
        if value is _MISSING:
            if "value" not in self.command.params:
                raise CommandRejected(422, "refused: the command carries no value", {"outcome": "refused"})
            value = self.command.params["value"]
        return await self.connector._apply_write(self, value)


class Telemetry:
    """Where the loop reports what it does; a no-op by default. Methods are
    called from the poll loop or the MQTT network thread and must not block."""

    def broker_healthy(self, healthy: bool) -> None: ...

    def source_healthy(self, healthy: bool) -> None: ...

    def published(self, metric: Metric, *, node_id: str) -> None: ...

    def publish_rejected(self, reason_code: int) -> None: ...

    def sample_refused(self, signal_id: str, reason: str) -> None:
        """One sample set aside: the node refused it for good
        (``reason`` is colca's reason, or ``refused`` when the node did not
        name one) or it was ``non_finite``."""

    def poll_completed(self, duration_s: float, *, overrun: bool) -> None: ...


def is_equal(a: Any, b: Any, precision: int | None) -> bool:
    """Value equality for change detection: two floats are equal within the
    Signal's precision (absolute tolerance ``10**-precision``); NaN equals
    NaN; everything else is ``==``."""
    if a is b:
        return True
    if isinstance(a, float) and isinstance(b, float):
        if isnan(a) and isnan(b):
            return True
        tol = 10 ** (-precision) if precision is not None else 0.0
        return isclose(a, b, rel_tol=0.0, abs_tol=tol)
    return a == b


def round_to_precision(value: float, precision: int) -> float:
    """Round half up to ``precision`` decimals; non-finite values pass through."""
    try:
        if not isfinite(value):
            return value
        quantum = Decimal(10) ** -precision
        return float(Decimal(value).quantize(quantum, rounding=ROUND_HALF_UP))
    except Exception:
        return value


def refusal_is_final(result: Mapping[str, Any], *, batch_admitted: bool) -> bool:
    """Whether a ``POST /publish/batch`` result refuses its record for good.

    A result with an offset is no refusal. A result without an ``error`` is
    no verdict either (a reply that lost its offset): retry it. A result that
    names colca's ``reason`` is final when the reason is in
    :data:`FINAL_REFUSAL_REASONS`. colca up to 0.27 names none in a batch;
    then the batch decides: the node judges every record before it writes,
    and writes the admitted records of one stream in one append, so a refusal
    in a batch where another record was admitted (``batch_admitted``) was the
    node's verdict on that record, not a failed write. A connector's batch is
    all ``_Metric``, one stream. With nothing admitted, a refusal may be a
    storage failure and is retried.
    """
    if result.get("offset") is not None or not result.get("error"):
        return False
    reason = result.get("reason")
    if reason:
        return reason in FINAL_REFUSAL_REASONS
    return batch_admitted


def has_non_finite(value: Any) -> bool:
    """Whether ``value`` is or holds a NaN or infinite float, which JSON
    cannot carry."""
    if isinstance(value, float):
        return not isfinite(value)
    if isinstance(value, (list, tuple)):
        return any(has_non_finite(item) for item in value)
    if isinstance(value, dict):
        return any(has_non_finite(item) for item in value.values())
    return False


class RefusalSpell(NamedTuple):
    """A signal whose samples are being refused: since when (unix seconds),
    how many, and the latest reason."""

    since: float
    refused: int
    reason: str


def _health_handler(is_healthy: Callable[[], bool]) -> type[BaseHTTPRequestHandler]:
    """``/is_healthy``: 200 while ``is_healthy`` says so, 503 while it does
    not; see :func:`run` for what it checks."""

    class HealthCheckHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/is_healthy":
                if is_healthy():
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"ok")
                else:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(b"unhealthy")
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            pass

    return HealthCheckHandler


def start_health_server(port: int, is_healthy: Callable[[], bool]) -> HTTPServer:
    """Serve ``GET /is_healthy`` on ``port`` from a daemon thread."""
    server = HTTPServer(("0.0.0.0", port), _health_handler(is_healthy))  # noqa: S104 - a container's own port  # nosec B104
    threading.Thread(target=server.serve_forever, daemon=True, name="colca-health").start()
    return server


def run(build: Callable[[], ConnectorService], *, health_port: int | None = 8888) -> None:
    """Build a connector inside a running event loop and serve it until
    stopped, with ``/is_healthy`` on ``health_port`` (None: no endpoint).
    It takes a factory because some protocol clients, such as pymodbus's
    ``AsyncModbusTcpClient``, need the running loop at construction."""

    async def main() -> None:
        svc = build()
        if health_port is not None:
            # Unhealthy while buffering through a broker outage, and while
            # another process runs as this connector (see Service.identity_conflict).
            start_health_server(health_port, lambda: svc.is_broker_connected() and not svc.identity_conflict)
        await svc.serve()

    asyncio.run(main())


class ConnectorService(Service):
    """A Service that discovers tags through a :class:`Driver` and polls
    them. Construct it like a :class:`~chaski.Service` (the same ``node=``
    rule decides the door), then :meth:`run` it.

    ``interval`` is the poll cadence in seconds; ``heartbeat_interval`` how
    often the synthetic heartbeat flips; ``max_pending`` the bound on
    metrics kept through a broker outage; ``reconnect_retries`` the source
    reconnect attempts per outage before the loop stays alive and tries
    again next poll; ``outage_reminder`` how many seconds a broker outage
    may last before it is logged again (the first failure and the recovery
    are always logged); ``summary_interval`` the cadence of the ``[DATA]``
    throughput line.
    """

    def __init__(
        self,
        name: str,
        mount: str = "",
        *,
        driver: Driver,
        interval: float = 1.0,
        heartbeat_interval: float = 5.0,
        max_pending: int = 10_000,
        reconnect_retries: int = 5,
        outage_reminder: float = 300.0,
        summary_interval: float = 60.0,
        telemetry: Telemetry | None = None,
        timestamp_source: Literal["acquisition", "source"] = "acquisition",
        **service_kwargs: Any,
    ) -> None:
        metadata = {**driver.metadata, **(service_kwargs.pop("metadata", None) or {})}
        if max_pending < 1:
            raise ValueError("max_pending must be positive")
        super().__init__(name, mount, metadata=metadata, max_queued_messages=int(max_pending), **service_kwargs)
        self.driver = driver
        if timestamp_source not in ("acquisition", "source"):
            raise ValueError("timestamp_source must be acquisition or source")
        self.timestamp_source = timestamp_source
        self.interval = float(interval)
        self.heartbeat_interval = float(heartbeat_interval)
        self.max_pending = int(max_pending)
        self.reconnect_retries = int(reconnect_retries)
        self.outage_reminder = float(outage_reminder)
        self.summary_interval = float(summary_interval)
        self.telemetry = telemetry or Telemetry()
        self._log = logging.getLogger(name)
        # The loop's clocks, as attributes so a test can drive time and skip
        # backoffs without patching the interpreter's own modules.
        self._now: Callable[[], float] = time.monotonic
        self._sleep: Callable[[float], Any] = asyncio.sleep

        # Per-tag read handles from the last discovery, keyed by tag id once
        # the catalogue has minted/reused ids (source -> id happens in
        # _declare_discovery). Synthetic tags have none.
        self._handles: dict[str, Any] = {}
        # The bound, published Signals to read, rebuilt under the lock
        # whenever a binding changes; the loop copies it under the lock.
        self._targets: list[Target] = []
        self._latest_by_topic: dict[str, Metric] = {}
        # Last is_connected value successfully published, per metric topic:
        # report-on-change per bound Signal, independent of the per-cycle
        # dedup, so a live source drop surfaces while the broker stays up.
        self._is_connected_published: dict[str, bool] = {}
        self._source_healthy = False
        self._discovered = False
        self._next_discovery_retry = 0.0
        from .retry import Backoff

        self._source_backoff = Backoff()
        self._mqtt_backoff = Backoff()
        self._heartbeat_start = self._now()
        self._metric_queue = None
        self._http_backoff = Backoff()
        self._stopping = asyncio.Event()
        self._loop_for_writes: asyncio.AbstractEventLoop | None = None
        # Signal writes: one handler per bound signal, keyed by (contract,
        # node-local path) and changed in place under the lock when bindings
        # change; the executor reads it at every command. _user_commands are
        # the routes announce_commands was given; the signal routes are added.
        self._writes: dict[tuple[str, str], Callable[..., Any]] = {}
        self._user_commands: list[tuple[str, str]] = list(self._announced_commands)
        self._writes_announced: list[tuple[str, str]] = []
        self._write_executor: CommandExecutor | None = None
        # The driver sees one call at a time: a poll, or a write and its read-back.
        self._source_lock = asyncio.Lock()

        self._source_reconnects_total = 0
        self._mqtt_reconnects_total = 0
        self._mqtt_down_since: float | None = None
        self._mqtt_down_attempts = 0
        self._mqtt_down_last_report = 0.0
        self._summary_published = 0
        self._summary_polls = 0
        self._summary_last = self._now()
        #: Samples set aside since start: refused by the node for good, or
        #: non-finite. Each one is also reported to Telemetry.sample_refused.
        self.refused_samples_total = 0
        # Per signal (its id, or the topic when a sample has none): the spell
        # of refusals it is in. Logged and recorded when one starts.
        self._refusal_spells: dict[str, RefusalSpell] = {}
        self._deferral_reported = False

    # -- the base class's hooks -----------------------------------------

    def _bindings_changed(self) -> None:
        self._update_targets()
        self._update_writes()
        self.clock.changes.notify()

    def _broker_state_changed_writes(self, connected: bool) -> None:
        executor, loop = self._write_executor, self._loop_for_writes
        if executor is not None and loop is not None:
            loop.call_soon_threadsafe(executor.link_changed, connected)

    def announce_commands(self, commands: Iterable[tuple[str, str]]) -> None:
        """:meth:`chaski.Service.announce_commands`, keeping the routes of the
        bound signals this connector writes."""
        with self._lock:
            self._user_commands = sorted(set(commands))
            writes = list(self._writes)
        super().announce_commands([*self._user_commands, *writes])

    def _update_writes(self) -> None:
        """Under the lock: a write handler per bound signal, synthetic tags
        excepted."""
        synthetic = self._synthetic_tag_ids()
        writes: dict[tuple[str, str], Callable[..., Any]] = {}
        for binding in self._bindings.values():
            if binding.tag_id in synthetic:
                continue
            parsed = parse_topic(str(binding.topic))
            if parsed is None:
                continue
            writes[(WRITE_CONTRACT, parsed[1])] = functools.partial(self._write_signal, binding.tag_id, parsed[1])
        self._writes.clear()
        self._writes.update(writes)

    async def _announce_writes(self) -> None:
        """Announce the signal routes when they changed; from the loop, since a
        binding changes on the MQTT thread, which cannot wait for a PUBACK."""
        with self._lock:
            routes = sorted(self._writes)
            if routes == self._writes_announced:
                return
            user = list(self._user_commands)
        try:
            await asyncio.to_thread(super().announce_commands, [*user, *routes])
        except Exception as exc:
            # Announced again on the next iteration; until then the node may
            # answer writes to a new signal 404.
            self._log.warning("signal write routes not announced yet: %s", exc)
            return
        self._writes_announced = routes

    async def _write_signal(self, tag_id: str, path: str, command: Command) -> CommandResult:
        """The route handler of one bound signal: check the binding and the
        tag, then :meth:`handle_signal_write`."""
        with self._lock:
            binding = next((b for b in self._bindings.values() if b.tag_id == tag_id), None)
            source = self._started_catalogue.source_for_tag(tag_id)
            tag = self._started_catalogue.tag(source) if source is not None else None
            handle = self._handles.get(tag_id)
        if binding is None or tag is None:
            raise CommandRejected(404, f"refused: {path} is no longer bound here", {"outcome": "refused"})
        if not tag.is_writable:
            raise CommandRejected(422, f"refused: tag {tag.name} is read-only", {"outcome": "refused"})
        if handle is None:
            raise CommandRejected(503, f"failed: tag {tag.name} is not available at the source", {"outcome": "failed"})
        return await self.handle_signal_write(
            SignalWrite(self, command, Target(binding.signal, handle, binding.topic), tag)
        )

    async def handle_signal_write(self, write: SignalWrite) -> CommandResult:
        """Execute one signal write and answer it. The default is
        :meth:`SignalWrite.apply`. Override it to check who sent the write,
        to write a value derived from the command, or to add to the answer's
        ``result`` (module docstring, "Taking over a write"). A repeat of an
        ``operation_id`` never gets here."""
        return await write.apply()

    async def _apply_write(self, write: SignalWrite, value: Any) -> CommandResult:
        """Write, read back, answer with what was read."""
        target, path, binding_signal = write.target, write.path, write.target.signal
        async with self._source_lock:
            try:
                await self.driver.write(target, value, write.command)
            except WriteUnsupported as exc:
                raise CommandRejected(501, f"unsupported: {exc}", {"outcome": "unsupported"}) from exc
            except SourceDisconnectedError as exc:
                self._set_source_healthy(False)
                raise CommandRejected(
                    503, f"failed: the source is not connected: {exc}", {"outcome": "failed"}
                ) from exc
            except Exception as exc:
                self._log.warning("write %s = %r refused by the source: %s", path, value, exc)
                raise CommandRejected(
                    502, f"failed: the source refused the write: {exc}"[:300], {"outcome": "failed"}
                ) from exc
            try:
                readings = list(await self.driver.read([target]))
            except Exception as exc:
                raise CommandRejected(
                    504,
                    f"unknown: written, but reading it back failed: {exc}"[:300],
                    {"outcome": "unknown", "requested": value},
                ) from exc
        read = next((r[1] for r in readings if str(r[0]) == str(target.topic)), _MISSING)
        if read is _MISSING:
            raise CommandRejected(
                504,
                "unknown: written, but the source returned no value on read-back",
                {"outcome": "unknown", "requested": value},
            )
        if not is_equal(read, value, getattr(binding_signal, "precision", None)):
            raise CommandRejected(
                409,
                f"failed: wrote {value!r}, read back {read!r}",
                {"outcome": "failed", "requested": value, "value": read},
            )
        return CommandResult(f"applied: {path} = {read!r}", {"outcome": "applied", "value": read})

    async def _serve_writes(self) -> None:
        """Execute signal writes until the connector stops. The ledger is on
        disk: a write started before a restart is answered ``504`` (outcome
        unknown) after it, never written again."""
        ledger = SqliteLedger(self._state_dir / "signal-writes.sqlite3")
        executor = CommandExecutor(
            self._require_http("signal writes"),
            self.send,
            self.stream("commands", cursor=WRITE_CURSOR, contracts=[WRITE_CONTRACT, "_Ack"]),
            self._writes,
            str(self._node_id),
            contracts=[WRITE_CONTRACT],
            ledger=ledger,
        )
        self._loop_for_writes = asyncio.get_running_loop()
        self._write_executor = executor
        if not self.is_broker_connected():
            executor.link_changed(False)
        stop = asyncio.Event()

        async def stop_with_connector() -> None:
            await self._stopping.wait()
            stop.set()

        watcher = asyncio.ensure_future(stop_with_connector())
        try:
            await executor.run_forever(stop)
        finally:
            watcher.cancel()
            self._write_executor = None
            ledger.close()

    def _seal_catalogue(self) -> None:
        """Discovery decides what is stale; a shutdown changes nothing."""

    def _broker_state_changed(self, connected: bool) -> None:
        self.telemetry.broker_healthy(connected)
        if connected:
            self._report_mqtt_recovered()
        self._broker_state_changed_writes(connected)

    # -- lifecycle --------------------------------------------------------

    def run(self, *, health_port: int | None = 8888) -> None:
        """Serve until stopped: :func:`run` with this service."""
        run(lambda: self, health_port=health_port)

    async def stop(self) -> None:
        self._stopping.set()
        self.clock.changes.notify()

    async def serve(self) -> None:
        """Register, discover, poll. Returns when :meth:`stop` is called;
        raises if the node cannot be reached at all (a connector without a
        node has nothing to do — the container restarts it)."""
        self._log.info("[STARTUP] Connector starting: protocol=%s, interval=%.1fs", self.driver.protocol, self.interval)
        self.start()
        # Service configures Paho's queue before connecting. Its limit is
        # the same as our pending buffer; Paho refuses changing it afterwards.
        self.telemetry.broker_healthy(True)
        writes: asyncio.Task | None = None
        heartbeat: asyncio.Task | None = None
        try:
            await self._startup_discovery()
            writes = asyncio.ensure_future(self._serve_writes())
            heartbeat = asyncio.ensure_future(self._heartbeat_forever())
            await self._poll_forever()
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
            if writes is not None:
                self._stopping.set()
                try:
                    await asyncio.wait_for(writes, timeout=10.0)
                except Exception:
                    self._log.exception("Signal-write executor did not shut down cleanly")
            await self._teardown()

    async def _startup_discovery(self) -> None:
        """Connect the source and discover once. A failure does not abort
        startup: the connector registers and advertises its synthetic tags, so
        "connector down" and "source down" look different, and the loop retries."""
        source_connected = False
        try:
            await self.driver.connect()
            source_connected = True
        except Exception as exc:
            self._set_source_healthy(False)
            self._log.warning("Source connect failed during startup: %s — staying alive, polling loop will retry.", exc)
        discovery: Discovery | None = None
        if source_connected or not self.driver.catalogue_requires_connection:
            try:
                discovery = await self.driver.discover()
                self._discovered = True
                if source_connected:
                    self._set_source_healthy(True)
            except Exception as exc:
                self._set_source_healthy(False)
                self._log.warning(
                    "Source discovery failed during startup: %s — staying alive, polling loop will retry.", exc
                )
        self._declare_discovery(discovery)

    async def _retry_discovery(self) -> None:
        """Retry a failed startup with a full connect and discover; a reconnect
        alone would leave bound tags without read handles."""
        self._next_discovery_retry = self._now() + DISCOVERY_RETRY_SECONDS
        try:
            try:
                await self.driver.close()
            except Exception as exc:
                self._log.debug("Pre-discovery close failed: %s", exc)
            await self.driver.connect()
            discovery = await self.driver.discover()
            self._declare_discovery(discovery)
            self._discovered = True
            self._set_source_healthy(True)
            self._log.info(
                "[STARTUP-RETRY] Source discovery recovered: %d tags catalogued.", len(self._started_catalogue)
            )
        except Exception as exc:
            self._set_source_healthy(False)
            self._log.warning(
                "Source discovery retry failed: %s — next attempt in %.0fs.", exc, DISCOVERY_RETRY_SECONDS
            )

    def _declare_discovery(self, discovery: Discovery | None) -> None:
        """The discovered tags plus the two synthetic ones become the
        catalogue (ids minted or reused there); the driver's handles are
        re-keyed by tag id for the poll loop."""
        tags: dict[str, DataTag] = dict(discovery.tags) if discovery else {}
        handles: Mapping[str, Any] = discovery.handles if discovery else {}
        tags[HEARTBEAT_TAG_SOURCE] = DataTag(
            id="",
            name=HEARTBEAT_TAG_NAME,
            source=HEARTBEAT_TAG_SOURCE,
            is_writable=False,
            is_readable=True,
            data_type="boolean",
            meta={"synthetic": True, "purpose": "liveness"},
        )
        tags[IS_CONNECTED_TAG_SOURCE] = DataTag(
            id="",
            name=IS_CONNECTED_TAG_NAME,
            source=IS_CONNECTED_TAG_SOURCE,
            is_writable=False,
            is_readable=True,
            data_type="boolean",
            meta={"synthetic": True, "purpose": "source-connectivity"},
        )
        with self._lock:
            self._started_catalogue.declare(tags)
            self._handles = {}
            for source, handle in handles.items():
                tag_id = self._started_catalogue.tag_id(source)
                if tag_id is not None:
                    self._handles[tag_id] = handle
            self._update_targets()

    def _update_targets(self) -> None:
        """Under the lock: the bound, published Signals the loop reads."""
        targets: list[Target] = []
        synthetic = self._synthetic_tag_ids()
        for binding in self._bindings.values():
            if not binding.signal.is_published:
                continue
            if binding.tag_id in synthetic:
                targets.append(Target(binding.signal, None, binding.topic))
                continue
            handle = self._handles.get(binding.tag_id)
            if handle is None:
                # A stale tag: the binding is real, but there is nothing at
                # the source left to read for it.
                continue
            targets.append(Target(binding.signal, handle, binding.topic))
        self._targets = targets

    def _synthetic_tag_ids(self) -> set[str]:
        return {
            tag_id
            for source in (HEARTBEAT_TAG_SOURCE, IS_CONNECTED_TAG_SOURCE)
            if (tag_id := self._started_catalogue.tag_id(source)) is not None
        }

    # -- the poll loop ----------------------------------------------------

    async def _poll_forever(self) -> None:
        while not self._stopping.is_set():
            await self._poll_iteration()

    async def _poll_iteration(self) -> None:
        """One iteration of the loop, including the wait that paces the
        next one — see the module docstring's "Timing"."""
        change_version = self.clock.changes.version
        loop_start_perf = time.perf_counter()
        clock_status = self.clock.status()
        step_target = await asyncio.to_thread(self.step.ready) if self.step is not None else None
        sampling = step_target is not None if self.step is not None else clock_status.ready and clock_status.rate > 0
        try:
            if not self._discovered and self._now() >= self._next_discovery_retry:
                await self._retry_discovery()

            self._publish_catalogue_if_due()
            await self._announce_writes()

            with self._lock:
                targets = list(self._targets)
            if not targets:
                delay = max(0.0, self._next_discovery_retry - self._now()) if not self._discovered else None
                await self.clock.changes.wait_async(change_version, delay, stop=self._stopping)
                return

            synthetic = self._synthetic_tag_ids()
            protocol_targets = [t for t in targets if t.signal.data_tag not in synthetic]

            raw_batch: list[tuple[Topic, Any, SignalRecord] | Reading] = []
            # Re-raised after publishing what was read, for the reconnect path.
            source_lost: SourceDisconnectedError | None = None
            if protocol_targets and sampling:
                if len(protocol_targets) > self.max_pending:
                    raise BufferError("max_pending cannot hold one complete acquisition cycle")
                # Retry durable output before taking another PLC observation.
                # This is backpressure; an outage never evicts older samples.
                try:
                    self._publish_batch([])
                except PublishRejected:
                    # The node answered and refused every sample it was sent,
                    # which is either its verdict or a failed write. The
                    # next cycle's samples tell which (see refusal_is_final),
                    # so acquire while the journal holds one more cycle.
                    if not self._journal_has_room(len(protocol_targets)):
                        raise
                try:
                    async with self._source_lock:
                        raw_batch = list(await self.driver.read(protocol_targets))
                    # The authoritative health signal: the protocol
                    # channel demonstrably answered. connect() succeeding
                    # is not — some clients background the TCP setup.
                    self._set_source_healthy(True)
                except SourceDisconnectedError as exc:
                    source_lost = exc
                    self._set_source_healthy(False)

            # Covers the first publish once a Signal is bound and one deferred by
            # a broker outage; a no-op when nothing changed.
            self._publish_is_connected()

            batch: list[tuple[Topic, Metric]] = []
            for reading in raw_batch:
                topic, raw_value, signal = reading[:3]
                source_timestamp = reading[3] if len(reading) > 3 else None
                value = raw_value
                precision = signal.precision
                if precision is not None and isinstance(value, (int, float)):
                    value = round_to_precision(value, precision)
                key = str(topic)
                timestamp = step_target if self.step is not None else clock_status.factory_now
                if self.timestamp_source == "source" and source_timestamp is not None:
                    if not isfinite(source_timestamp):
                        raise ValueError("source timestamp must be finite")
                    timestamp = source_timestamp
                if has_non_finite(value):
                    self._set_aside(
                        signal.id,
                        signal.id,
                        "non_finite",
                        f"non-finite reading {value!r} at {key}: JSON cannot carry it",
                        {"signal_id": signal.id, "topic": key, "timestamp": timestamp, "value": repr(value)},
                        must_record=False,
                    )
                    continue
                metric = Metric(value=value, timestamp=timestamp, signal_id=signal.id)
                self._latest_by_topic[key] = metric
                batch.append((topic, metric))

            self._publish_batch(batch)
            if self.step is not None and step_target is not None and source_lost is None:
                # Partial protocol reads cannot acknowledge an entire window.
                received = {str(reading[0]) for reading in raw_batch}
                if protocol_targets and all(str(target.topic) in received for target in protocol_targets):
                    await asyncio.to_thread(self.step.complete, step_target)
            self._summary_polls += 1
            self._log_summary(len(targets))

            if source_lost is not None:
                raise source_lost

        except SourceDisconnectedError as exc:
            self._set_source_healthy(False)
            self._log.error("Source disconnected: %s", exc)
            await self._reconnect_source()

        except httpx.HTTPError as exc:
            self.telemetry.broker_healthy(False)
            self._log.warning("Metric batch deferred: %s", type(exc).__name__)
            await self._sleep(self._http_backoff.delay(exc))
            return

        except PublishRejected as exc:
            # Undecided refusals: kept and retried, reported once until the
            # journal drains again.
            if not self._deferral_reported:
                self._deferral_reported = True
                self._log.warning("Metric batch deferred, the node refused every sample it was sent: %s", exc)
            await self._sleep(self._http_backoff.delay(exc))
            return

        except MqttDisconnectedError as exc:
            self.telemetry.broker_healthy(False)
            self._report_mqtt_outage(exc)
            for _retry in range(self.reconnect_retries):
                try:
                    self._started_client.reconnect()
                    self._mqtt_reconnects_total += 1
                    self._report_mqtt_recovered()
                    self._mqtt_backoff.reset()
                    break
                except Exception as error:
                    await self._sleep(self._mqtt_backoff.delay(error))
            await self._sleep(0.001)
            return

        except Exception:
            self._log.exception("Unknown polling loop error. Shutting down.")
            raise

        elapsed = time.perf_counter() - loop_start_perf
        if self.step is not None or not sampling:
            self.telemetry.poll_completed(elapsed, overrun=False)
            # Wake for commits or the discovery deadline. An incomplete PLC
            # read retries on its acquisition cadence.
            delay = None
            if not self._discovered:
                discovery = max(0.0, self._next_discovery_retry - self._now())
                delay = discovery if delay is None else min(delay, discovery)
            if step_target is not None and self.step is not None and self.step.completed_at != step_target:
                delay = self.interval if delay is None else min(delay, self.interval)
            if self.step is None and clock_status.ready and clock_status.factory_now is not None:
                # A future definition can start without another message. Wake
                # at that transition. A paused/completed clock without
                # a scheduled transition returns None, so it stays event-driven.
                clock_delay = self.clock.delay_until(clock_status.factory_now + max(self.interval, 1e-6))
                if clock_delay is not None:
                    delay = clock_delay if delay is None else min(delay, clock_delay)
            deadline = self.step.wait_delay(delay) if self.step is not None else delay
            await self.clock.changes.wait_async(change_version, deadline, stop=self._stopping)
            return
        cadence = self.interval / clock_status.rate
        wait = cadence - elapsed
        self.telemetry.poll_completed(elapsed, overrun=wait <= 0)
        if wait > 0:
            await self._sleep(wait)
        else:
            await self._sleep(0.001)

    async def _reconnect_source(self) -> None:
        """Reconnect the source up to ``reconnect_retries`` times with jittered
        backoff. If all fail it returns unhealthy, and the next poll comes back
        here, so reconnecting never stops."""
        for retry in range(self.reconnect_retries):
            try:
                self._log.info("Reconnecting to source (attempt %d/%d)", retry + 1, self.reconnect_retries)
                try:
                    await self.driver.close()
                except Exception as exc:
                    self._log.warning("Error closing source: %s", exc)
                await self.driver.connect()
                # Not source_healthy=1 here: connect() succeeding is not
                # authoritative (pymodbus backgrounds the TCP setup). The next
                # read sets it once the channel actually answers.
                self._source_reconnects_total += 1
                self._source_backoff.reset()
                return
            except Exception as exc:
                await self._sleep(self._source_backoff.delay(exc))
        self._log.warning("Reconnect retries exhausted; staying alive with source_healthy=0")

    def _log_summary(self, target_count: int) -> None:
        now = self._now()
        if now - self._summary_last < self.summary_interval:
            return
        self._log.info(
            "[DATA] Published %d metrics in %d tags over %d polls (%.0fs)",
            self._summary_published,
            target_count,
            self._summary_polls,
            now - self._summary_last,
        )
        self._summary_published = 0
        self._summary_polls = 0
        self._summary_last = now

    # -- catalogue --------------------------------------------------------

    def _publish_catalogue_if_due(self) -> None:
        """Off the network thread, so a rejection is visible and reported.
        A rejected catalogue is not recorded as published: the next
        iteration tries again rather than believing it is out there."""
        with self._lock:
            prepared = self._catalogue_to_publish()
        if prepared is None:
            return
        if not self._started_client.is_connected():
            raise MqttDisconnectedError("MQTT client not connected (checked before the catalogue publish).")
        payload, _ = prepared
        try:
            self._publish_catalogue(prepared)
        except PublishRejected as exc:
            self._log.error("[SYNC] Catalogue rejected by the broker: %s", exc)
            return
        except (ConnectionError, PublishTimeout) as exc:
            # The broker is gone, not slow: the loop's outage path owns this.
            raise MqttDisconnectedError(str(exc)) from exc
        except Exception as exc:
            self._log.exception("Failed to publish the catalogue: %s", exc)
            return
        self._log.info(
            "[SYNC] Published %d data tags to %s (revision %s)",
            len(payload.data_tags),
            str(self._catalogue_topic),
            payload.version[:12],
        )

    def _placement_reannounced(self) -> None:
        # Nothing to do: a moved catalogue is already dirty with its
        # revision forgotten, and the loop publishes what is due.
        pass

    # -- synthetic tags ---------------------------------------------------

    async def _heartbeat_forever(self) -> None:
        """The heartbeat on its own schedule. It says the connector is alive,
        so it must not wait for a read: a slow or unreachable source holds the
        poll loop for as long as its timeouts and reconnects take."""
        while not self._stopping.is_set():
            self._publish_heartbeat()
            age = self._now() - self._heartbeat_start
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), self.heartbeat_interval - (age % self.heartbeat_interval))

    def _publish_heartbeat(self) -> None:
        """Publish the current heartbeat value to every bound heartbeat
        Signal whose last published value differs. A broker outage is the
        poll loop's to handle; the next flip publishes again."""
        heartbeat_id = self._started_catalogue.tag_id(HEARTBEAT_TAG_SOURCE)
        with self._lock:
            targets = [t for t in self._targets if t.signal.data_tag == heartbeat_id]
        value = self._current_heartbeat_value()
        timestamp = datetime.datetime.now(datetime.UTC).timestamp()
        batch: list[tuple[Topic, Metric]] = []
        for target in targets:
            key = str(target.topic)
            last = self._latest_by_topic.get(key)
            if last is not None and last.value == value:
                continue
            batch.append((target.topic, Metric(value=value, timestamp=timestamp, signal_id=target.signal.id)))
        if not batch:
            return
        try:
            self._publish_batch(batch)
        except (MqttDisconnectedError, httpx.HTTPError, PublishRejected) as exc:
            self._log.debug("Heartbeat deferred: %s", type(exc).__name__)
            return
        for topic, metric in batch:
            self._latest_by_topic[str(topic)] = metric

    def _current_heartbeat_value(self) -> bool:
        """Flips every ``heartbeat_interval`` seconds since startup."""
        elapsed = self._now() - self._heartbeat_start
        return (int(elapsed // self.heartbeat_interval) % 2) == 0

    def _set_source_healthy(self, state: bool) -> None:
        """The only writer of the source-health flag. A change is published at
        once through :meth:`_publish_is_connected`."""
        self.telemetry.source_healthy(state)
        self._source_healthy = state
        self._publish_is_connected()

    def _publish_is_connected(self) -> None:
        """Publish ``is_connected`` for every bound Signal whose last
        successfully published value differs from the current state. A
        target that cannot be published yet (broker down) keeps its entry
        untouched, so the next poll retries."""
        if self._client is None:
            return
        is_connected_id = self._catalogue.tag_id(IS_CONNECTED_TAG_SOURCE) if self._catalogue else None
        with self._lock:
            targets = [t for t in self._targets if t.signal.data_tag == is_connected_id]
        current = {str(t.topic) for t in targets}
        for stale in [k for k in self._is_connected_published if k not in current]:
            del self._is_connected_published[stale]
        if not targets:
            return
        state = self._source_healthy
        timestamp = datetime.datetime.now(datetime.UTC).timestamp()
        for target in targets:
            key = str(target.topic)
            if self._is_connected_published.get(key) == state:
                continue
            metric = Metric(value=state, timestamp=timestamp, signal_id=target.signal.id)
            try:
                self._client.publish(target.topic, metric, qos=1)
            except Exception as exc:
                self._log.warning("[IS_CONNECTED] publish failed for %s, will retry: %s", key, exc)
                continue
            self._is_connected_published[key] = state
            self._log.info("[IS_CONNECTED] %s source connectivity -> %s", key, state)

    # -- publishing -------------------------------------------------------

    def _open_metric_queue(self):
        if self._metric_queue is None:
            from .pending import PendingSamples

            self._metric_queue = PendingSamples(self._state_dir / "connector-samples.sqlite3")
        return self._metric_queue

    @property
    def _pending(self):
        """Diagnostic snapshot; the durable journal owns these records."""
        if self._metric_queue is None:
            return []
        return [
            (Topic.from_str(topic), Metric(**payload))
            for _, topic, payload, _, _ in self._metric_queue.page_all(self.max_pending)
        ]

    def _journal_has_room(self, samples: int) -> bool:
        queue = self._open_metric_queue()
        return queue.count() + samples <= self.max_pending

    def _publish_batch(self, batch: list[tuple[Topic, Metric]]) -> None:
        """One durable acquisition cycle, one broker append per stream.

        The HTTP door uses the same admission and MQTT fanout as individual
        publishes. Per-record results decide which journal entries may retire:
        an admitted sample retires, a final refusal (:func:`refusal_is_final`)
        retires once it is recorded, and anything else stays for replay and
        raises :class:`PublishRejected`. A transport failure or a lost reply
        leaves every entry for replay.
        """
        if not batch and self._metric_queue is None and not (self._state_dir / "connector-samples.sqlite3").exists():
            return
        queue = self._open_metric_queue()
        if batch:
            queue.append_batch(
                [(str(topic), json.loads(metric.encode()), metric.timestamp, None) for topic, metric in batch],
                self.max_pending,
            )
        while rows := queue.page_all(min(self.max_pending, 1000)):
            results = self._require_http("publish metrics").publish_batch(
                [(topic, json.dumps(payload)) for _, topic, payload, _, _ in rows]
            )
            if len(results) != len(rows):
                raise RuntimeError("broker batch receipt length does not match acquisition batch")
            accepted = []
            failed = []
            for row, result in zip(rows, results, strict=True):
                identity, topic, payload, _, _ = row
                if result.get("error") or "offset" not in result:
                    failed.append((row, result))
                else:
                    accepted.append(identity)
                    self._summary_published += 1
                    self.telemetry.published(Metric(**payload), node_id=self._node_id or "")
                    self._refusal_spell_ended(payload.get("signal_id") or topic)
            queue.ack(accepted)
            undecided = []
            for row, result in failed:
                identity, topic, payload, timestamp, _ = row
                if not refusal_is_final(result, batch_admitted=bool(accepted)):
                    undecided.append(PublishRejected(0x80, topic, str(result.get("error", "missing durable offset"))))
                    continue
                signal_id = payload.get("signal_id") or ""
                reason = result.get("reason") or "refused"
                self._set_aside(
                    signal_id,
                    signal_id or topic,
                    reason,
                    f"the node refused a sample of {topic}: {result['error']}",
                    {"signal_id": signal_id, "topic": topic, "timestamp": timestamp, "reason": reason},
                    must_record=True,
                )
                queue.ack([identity])
            if undecided:
                self.telemetry.publish_rejected(undecided[0].reason_code)
                raise undecided[0]
        self._deferral_reported = False
        self._http_backoff.reset()
        self._report_mqtt_recovered()

    def _set_aside(
        self, signal_id: str, key: str, reason: str, summary: str, subject: dict[str, Any], *, must_record: bool
    ) -> None:
        """Count one refused sample. The first of a spell (per ``key``, or
        after the reason changed) is logged and recorded in the
        ``rejected_input`` finding. With ``must_record`` the sample is still in
        the journal and must not leave it unrecorded: a failed record raises.
        """
        spell = self._refusal_spells.get(key)
        if spell is None or spell.reason != reason:
            try:
                self.reject(REFUSED_SAMPLES_CONSUMER, subject, Reject(summary, detail={"reason": reason}))
            except Exception as exc:
                if must_record:
                    if isinstance(exc, (ConnectionError, PublishTimeout)):
                        raise MqttDisconnectedError(f"could not record a refused sample: {exc}") from exc
                    raise
                # A non-finite reading cannot be journaled; it is counted and
                # logged, and the next one of the spell tries the record again.
                self._log.warning("[REFUSED] %s (not recorded in the finding: %s)", summary, exc)
                self._count_refusal(signal_id, reason)
                return
            spell = RefusalSpell(time.time(), 0, reason)
        self._refusal_spells[key] = spell._replace(refused=spell.refused + 1)
        self._count_refusal(signal_id, reason)

    def _count_refusal(self, signal_id: str, reason: str) -> None:
        self.refused_samples_total += 1
        self.telemetry.sample_refused(signal_id, reason)

    def _refusal_spell_ended(self, key: str) -> None:
        spell = self._refusal_spells.pop(key, None)
        if spell is not None:
            self._log.info(
                "[REFUSED] %s is admitted again after %d refused samples (%s) over %.0fs",
                key,
                spell.refused,
                spell.reason,
                time.time() - spell.since,
            )

    def refusing(self) -> dict[str, RefusalSpell]:
        """The signals whose samples are being refused, keyed by signal id
        (or topic, for a sample without one)."""
        return dict(self._refusal_spells)

    def close(self) -> None:
        super().close()
        if self._metric_queue is not None:
            self._metric_queue.close()
            self._metric_queue = None

    # -- outage reporting -------------------------------------------------

    def _report_mqtt_outage(self, error: Exception) -> None:
        """Log a broker outage when it starts and then only every
        ``outage_reminder`` seconds, not on every poll."""
        now = self._now()
        if self._mqtt_down_since is None:
            self._mqtt_down_since = now
            self._mqtt_down_attempts = 1
            self._mqtt_down_last_report = now
            self._log.error("MQTT disconnected: %s — retrying until it answers", error)
            return
        self._mqtt_down_attempts += 1
        if now - self._mqtt_down_last_report < self.outage_reminder:
            return
        self._mqtt_down_last_report = now
        self._log.error(
            "MQTT still disconnected after %.0fs and %d attempts: %s",
            now - self._mqtt_down_since,
            self._mqtt_down_attempts,
            error,
        )

    def _report_mqtt_recovered(self) -> None:
        """The other half: a lane that came back says so, once, with the cost."""
        if self._mqtt_down_since is None:
            return
        down_for = self._now() - self._mqtt_down_since
        attempts = self._mqtt_down_attempts
        self._mqtt_down_since = None
        self._mqtt_down_attempts = 0
        self._log.info("MQTT reconnected after %.0fs and %d attempts", down_for, attempts)

    # -- shutdown ---------------------------------------------------------

    async def _teardown(self) -> None:
        try:
            self.close()
        except Exception:
            self._log.exception("Error during MQTT teardown")
        finally:
            self.telemetry.broker_healthy(False)
        try:
            await self.driver.close()
        except Exception:
            self._log.exception("Error during source teardown")
        finally:
            self.telemetry.source_healthy(False)
            self._source_healthy = False
