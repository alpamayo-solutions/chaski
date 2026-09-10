"""``chaski.ConnectorService``: a :class:`chaski.Service` that polls a source
(service families design §3.4).

A connector is a Service whose tags come from DISCOVERY rather than from
``publish()`` calls, read on an interval from a source a protocol driver
speaks to. Everything a connector does that is not protocol — advertising a
catalogue, learning which of its tags the node bound to Signals, polling on
a fixed cadence, rounding to the Signal's precision, publishing on change,
keeping a liveness heartbeat and a source-connectivity flag, buffering
through a broker outage, reconnecting to the source with backoff — is the
same for OPC UA, Modbus, S7, Jetter and an HTTP status endpoint. That is
this class, and this is where a connector for a new protocol starts.

The driver protocol is four ``async`` methods (:class:`Driver`):

* ``connect()`` — open the source; raise on failure (the loop retries).
* ``discover()`` — the source's tags: ``{source: DataTag}`` plus, per
  source, whatever handle ``read`` needs to read it (an OPC UA node, a
  register descriptor, a JSON pointer).
* ``read(targets)`` — one poll of the bound targets: ``(topic, raw_value,
  signal)`` per reading. Raise :class:`SourceDisconnectedError` when the
  source is gone; the loop flips ``is_connected``, keeps the heartbeat
  going, and reconnects.
* ``close()`` — release the source.

Ids, topics, payloads and timing are the base class's and the loop's; a
driver never sees a tag id, a topic it has to build, or a Metric.

**Timing.** Each poll stamps every reading with one epoch, taken at the top
of the iteration; the next iteration starts ``interval`` after that top
(drift-compensated), and an iteration that overran is counted and the next
one starts immediately. A poll cycle that could not publish (broker down)
keeps its metrics — bounded at ``max_pending``, oldest dropped — and
prepends them to the next cycle's batch.

**What is NOT here.** Prometheus exposition: the loop reports its events to
a :class:`Telemetry` (a no-op by default), where a process can plug in
Prometheus gauges. Durability across a restart: the pending
buffer is memory, by design — the answer for durability is ``chaski.Node``
(design §3.4). Configuration from the environment: this is a library; the
process that builds one reads its own environment.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import secrets
import threading
import time
from decimal import ROUND_HALF_UP, Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from math import isclose, isfinite, isnan
from typing import Any, Callable, Iterable, Mapping, NamedTuple, Optional

from franzmq import Topic
from franzmq.errors import PublishRejected, PublishTimeout
from colca_data_contracts.payload import DataTag, Metric
from colca_data_contracts.payload import Signal as SignalRecord

from .service import Service

# Synthetic tags have stable SOURCE keys like every protocol tag. Their ids
# are ordinary catalogue-minted ULIDs, reused from the previous catalogue; a
# source string is never smuggled into the identity field as a special case.
HEARTBEAT_TAG_SOURCE = "__heartbeat__"
HEARTBEAT_TAG_NAME = "heartbeat"
#: Every connector exposes a boolean reflecting its source's reachability:
#: True after a successful read, False after a SourceDisconnectedError or a
#: failed discovery. Published on change, per bound Signal.
IS_CONNECTED_TAG_SOURCE = "__is_connected__"
IS_CONNECTED_TAG_NAME = "is_connected"

#: How long after a failed startup discovery the loop retries it. The retry
#: is a full connect + discover, not just a reconnect: a browse-based driver
#: that started while its source was still booting has no catalogue at all.
DISCOVERY_RETRY_SECONDS = 15.0


class SourceDisconnectedError(Exception):
    """Raise from a driver's ``read`` (or ``connect``/``discover`` helpers)
    when the source connection is lost. The loop flips the source-health
    flag, publishes ``is_connected=False``, reconnects with backoff and, if
    every retry fails, stays alive and tries again next poll — a source
    that is genuinely offline is not a reason to crash-loop the container.
    A driver that swallows connection errors locally leaves the flag stuck
    at True and spams one log line per tag instead."""


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
    metadata: dict[str, Any] = {}
    #: Whether the catalogue can only be built while connected to the
    #: source. True for browse-based protocols (OPC UA discovers nodes from
    #: the server). False for file-mapped ones (S7, Modbus) whose tag list
    #: comes from the driver's own config — those announce their catalogue
    #: even while the source is unreachable, so the data model can be bound
    #: before the machine is physically connected.
    catalogue_requires_connection: bool = True

    def __init__(self, *, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(f"{__name__}.{type(self).__name__}")

    async def connect(self) -> None:
        """Open the source. Raise on failure, with a message naming what
        failed (host, port, endpoint) — the loop logs it and retries."""
        raise NotImplementedError

    async def discover(self) -> Discovery:
        """Discover the source's tags. Called after ``connect`` (or without
        it, when ``catalogue_requires_connection`` is False)."""
        raise NotImplementedError

    async def read(self, targets: list[Target]) -> Iterable[tuple[Topic, Any, SignalRecord]]:
        """One poll of ``targets``. A target a driver could not read is
        simply absent from the result; a lost source raises
        :class:`SourceDisconnectedError`."""
        raise NotImplementedError

    async def close(self) -> None:
        """Release the source. Must tolerate being called on a half-open
        or already-closed connection."""
        raise NotImplementedError


class Telemetry:
    """Where the loop reports what it does. No-op by default; the shipped
    connector image subclasses it with Prometheus instruments. Every method
    is called from the poll loop or the MQTT network thread and must not
    block."""

    def broker_healthy(self, healthy: bool) -> None: ...

    def source_healthy(self, healthy: bool) -> None: ...

    def published(self, metric: Metric, *, node_id: str) -> None: ...

    def publish_rejected(self, reason_code: int) -> None: ...

    def poll_completed(self, duration_s: float, *, overrun: bool) -> None: ...


def is_equal(a: Any, b: Any, precision: Optional[int]) -> bool:
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
    except Exception:  # noqa: BLE001 - a value Decimal cannot represent is published as-is
        return value


def _health_handler(is_healthy: Callable[[], bool]) -> type[BaseHTTPRequestHandler]:
    """``/is_healthy``: 200 while the broker is reachable, 503 while it is
    not — so a connector silently buffering through an outage is visible
    to ``docker compose ps`` and to whatever polls the endpoint, without a
    second liveness signal beside the one paho already tracks."""

    class HealthCheckHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server's name
            if self.path == "/is_healthy":
                if is_healthy():
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"ok")
                else:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(b"mqtt disconnected")
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - http.server's name
            pass

    return HealthCheckHandler


def start_health_server(port: int, is_healthy: Callable[[], bool]) -> HTTPServer:
    """Serve ``GET /is_healthy`` on ``port`` from a daemon thread."""
    server = HTTPServer(("0.0.0.0", port), _health_handler(is_healthy))  # noqa: S104 - a container's own port
    threading.Thread(target=server.serve_forever, daemon=True, name="colca-health").start()
    return server


def run(build: Callable[[], "ConnectorService"], *, health_port: Optional[int] = 8888) -> None:
    """Build a connector INSIDE a running event loop and serve it until
    stopped, with the ``/is_healthy`` endpoint on ``health_port`` (None: no
    endpoint). A factory rather than an instance because some protocol
    clients need the loop at construction — pymodbus's
    ``AsyncModbusTcpClient`` binds ``asyncio.get_running_loop()`` in its
    ``__init__`` — so a driver built before ``asyncio.run`` would never
    connect. ``ConnectorService.run()`` is this, for a service built where a
    loop already exists or whose driver does not care."""

    async def main() -> None:
        svc = build()
        if health_port is not None:
            start_health_server(health_port, svc.is_broker_connected)
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
        telemetry: Optional[Telemetry] = None,
        **service_kwargs: Any,
    ) -> None:
        metadata = {**driver.metadata, **(service_kwargs.pop("metadata", None) or {})}
        if max_pending < 1:
            raise ValueError("max_pending must be positive")
        super().__init__(name, mount, metadata=metadata, max_queued_messages=int(max_pending), **service_kwargs)
        self.driver = driver
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
        self._heartbeat_start = self._now()
        self._pending: list[tuple[Topic, Metric]] = []
        self._stopping = asyncio.Event()

        self._source_reconnects_total = 0
        self._mqtt_reconnects_total = 0
        self._mqtt_down_since: Optional[float] = None
        self._mqtt_down_attempts = 0
        self._mqtt_down_last_report = 0.0
        self._summary_published = 0
        self._summary_polls = 0
        self._summary_last = self._now()

    # -- the base class's hooks -----------------------------------------

    def _bindings_changed(self) -> None:
        self._update_targets()

    def _seal_catalogue(self) -> None:
        """Discovery decides what is stale; a shutdown changes nothing."""

    def _broker_state_changed(self, connected: bool) -> None:
        self.telemetry.broker_healthy(connected)
        if connected:
            self._report_mqtt_recovered()

    # -- lifecycle --------------------------------------------------------

    def run(self, *, health_port: Optional[int] = 8888) -> None:
        """Serve until stopped: :func:`run` with this service."""
        run(lambda: self, health_port=health_port)

    async def stop(self) -> None:
        self._stopping.set()

    async def serve(self) -> None:
        """Register, discover, poll. Returns when :meth:`stop` is called;
        raises if the node cannot be reached at all (a connector without a
        node has nothing to do — the container restarts it)."""
        self._log.info("[STARTUP] Connector starting: protocol=%s, interval=%.1fs",
                       self.driver.protocol, self.interval)
        self.start()
        # Service configures Paho's queue before connecting. Its limit is
        # the same as our pending buffer; Paho refuses changing it afterwards.
        self.telemetry.broker_healthy(True)
        try:
            await self._startup_discovery()
            await self._poll_forever()
        finally:
            await self._teardown()

    async def _startup_discovery(self) -> None:
        """Connect the source and discover once. Neither failure aborts
        startup: the loop keeps retrying, and meanwhile the connector is
        registered and its synthetic tags are advertised — so a customer
        can tell "connector down" from "source down"."""
        source_connected = False
        try:
            await self.driver.connect()
            source_connected = True
        except Exception as exc:  # noqa: BLE001 - the loop retries; the reason is logged
            self._set_source_healthy(False)
            self._log.warning("Source connect failed during startup: %s — staying alive, "
                              "polling loop will retry.", exc)
        discovery: Optional[Discovery] = None
        if source_connected or not self.driver.catalogue_requires_connection:
            try:
                discovery = await self.driver.discover()
                self._discovered = True
                if source_connected:
                    self._set_source_healthy(True)
            except Exception as exc:  # noqa: BLE001 - same: retried by the loop
                self._set_source_healthy(False)
                self._log.warning("Source discovery failed during startup: %s — staying alive, "
                                  "polling loop will retry.", exc)
        self._declare_discovery(discovery)

    async def _retry_discovery(self) -> None:
        """The startup promise kept: the RETRY is a full connect + discover.
        Reconnecting alone would leave every bound tag without a handle
        forever, silently polling nothing but the synthetic tags."""
        self._next_discovery_retry = self._now() + DISCOVERY_RETRY_SECONDS
        try:
            try:
                await self.driver.close()
            except Exception as exc:  # noqa: BLE001 - a half-open session from the failed connect
                self._log.debug("Pre-discovery close failed: %s", exc)
            await self.driver.connect()
            discovery = await self.driver.discover()
            self._declare_discovery(discovery)
            self._discovered = True
            self._set_source_healthy(True)
            self._log.info("[STARTUP-RETRY] Source discovery recovered: %d tags catalogued.",
                           len(self._catalogue))
        except Exception as exc:  # noqa: BLE001 - reported, retried on the next interval
            self._set_source_healthy(False)
            self._log.warning("Source discovery retry failed: %s — next attempt in %.0fs.",
                              exc, DISCOVERY_RETRY_SECONDS)

    def _declare_discovery(self, discovery: Optional[Discovery]) -> None:
        """The discovered tags plus the two synthetic ones become the
        catalogue (ids minted or reused there); the driver's handles are
        re-keyed by tag id for the poll loop."""
        tags: dict[str, DataTag] = dict(discovery.tags) if discovery else {}
        handles: Mapping[str, Any] = discovery.handles if discovery else {}
        tags[HEARTBEAT_TAG_SOURCE] = DataTag(
            id="", name=HEARTBEAT_TAG_NAME, source=HEARTBEAT_TAG_SOURCE,
            is_writable=False, is_readable=True, data_type="boolean",
            meta={"synthetic": True, "purpose": "liveness"},
        )
        tags[IS_CONNECTED_TAG_SOURCE] = DataTag(
            id="", name=IS_CONNECTED_TAG_NAME, source=IS_CONNECTED_TAG_SOURCE,
            is_writable=False, is_readable=True, data_type="boolean",
            meta={"synthetic": True, "purpose": "source-connectivity"},
        )
        with self._lock:
            self._catalogue.declare(tags)
            self._handles = {}
            for source, handle in handles.items():
                tag_id = self._catalogue.tag_id(source)
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
            if (tag_id := self._catalogue.tag_id(source)) is not None
        }

    # -- the poll loop ----------------------------------------------------

    async def _poll_forever(self) -> None:
        while not self._stopping.is_set():
            await self._poll_iteration()

    async def _poll_iteration(self) -> None:
        """One iteration of the loop, including the wait that paces the
        next one — see the module docstring's "Timing"."""
        loop_start_perf = time.perf_counter()
        # What every metric of this iteration carries: unix seconds
        # (the contract's timestamp is a number; a datetime would encode
        # to an ISO string the door refuses).
        loop_start_epoch = datetime.datetime.now(datetime.timezone.utc).timestamp()
        try:
            if not self._discovered and self._now() >= self._next_discovery_retry:
                await self._retry_discovery()

            self._publish_catalogue_if_due()

            with self._lock:
                targets = list(self._targets)
            if not targets:
                await self._sleep(self.interval)
                return

            heartbeat_id = self._catalogue.tag_id(HEARTBEAT_TAG_SOURCE)
            is_connected_id = self._catalogue.tag_id(IS_CONNECTED_TAG_SOURCE)
            heartbeat_targets = [t for t in targets if t.signal.data_tag == heartbeat_id]
            protocol_targets = [t for t in targets
                                if t.signal.data_tag not in (heartbeat_id, is_connected_id)]

            raw_batch: list[tuple[Topic, Any, SignalRecord]] = []
            # A lost source must not cost the heartbeat: the connector is
            # alive even when its source is not, and the heartbeat is what
            # says so. Re-raised after publishing, for the reconnect path.
            source_lost: Optional[SourceDisconnectedError] = None
            if protocol_targets:
                try:
                    raw_batch = list(await self.driver.read(protocol_targets))
                    # The authoritative health signal: the protocol
                    # channel demonstrably answered. connect() succeeding
                    # is not — some clients background the TCP setup.
                    self._set_source_healthy(True)
                except SourceDisconnectedError as exc:
                    source_lost = exc
                    self._set_source_healthy(False)

            if heartbeat_targets:
                heartbeat = self._current_heartbeat_value()
                for target in heartbeat_targets:
                    raw_batch.append((target.topic, heartbeat, target.signal))

            # is_connected is published on change from _set_source_healthy;
            # this covers the initial publish once a Signal is bound and
            # self-heals a publish deferred by a broker outage. No-op when
            # unchanged.
            self._publish_is_connected()

            batch: list[tuple[Topic, Metric]] = []
            for topic, raw_value, signal in raw_batch:
                value = raw_value
                precision = signal.precision
                if precision is not None and isinstance(value, (int, float)):
                    value = round_to_precision(value, precision)
                key = str(topic)
                last = self._latest_by_topic.get(key)
                if last is not None and is_equal(last.value, value, precision):
                    continue
                metric = Metric(value=value, timestamp=loop_start_epoch, signal_id=signal.id)
                self._latest_by_topic[key] = metric
                batch.append((topic, metric))

            self._publish_batch(batch)
            self._summary_polls += 1
            self._log_summary(len(targets))

            if source_lost is not None:
                raise source_lost

        except SourceDisconnectedError as exc:
            self._set_source_healthy(False)
            self._log.error("Source disconnected: %s", exc)
            await self._reconnect_source()

        except MqttDisconnectedError as exc:
            self.telemetry.broker_healthy(False)
            self._report_mqtt_outage(exc)
            for retry in range(self.reconnect_retries):
                try:
                    self._client.reconnect()
                    self._mqtt_reconnects_total += 1
                    self._report_mqtt_recovered()
                    break
                except Exception:  # noqa: BLE001 - retried with backoff
                    await self._sleep(1 + (self.reconnect_retries - retry) * 5)
            await self._sleep(0.001)
            return

        except Exception:
            self._log.exception("Unknown polling loop error. Shutting down.")
            raise

        elapsed = time.perf_counter() - loop_start_perf
        wait = self.interval - elapsed
        self.telemetry.poll_completed(elapsed, overrun=wait <= 0)
        if wait > 0:
            await self._sleep(wait)
        else:
            await self._sleep(0.001)

    async def _reconnect_source(self) -> None:
        """Reconnect the source, ``reconnect_retries`` times with a jittered
        backoff. Exhausting them leaves source_healthy=0 and returns: the
        next poll raises SourceDisconnectedError again and re-enters here,
        so reconnecting continues indefinitely instead of crash-looping the
        container while the source is genuinely offline."""
        for retry in range(self.reconnect_retries):
            try:
                self._log.info("Reconnecting to source (attempt %d/%d)", retry + 1, self.reconnect_retries)
                try:
                    await self.driver.close()
                except Exception as exc:  # noqa: BLE001 - closing a dead session may fail
                    self._log.warning("Error closing source: %s", exc)
                await self.driver.connect()
                # Not source_healthy=1 here: connect() succeeding is not
                # authoritative (pymodbus backgrounds the TCP setup). The next
                # read sets it once the channel actually answers.
                self._source_reconnects_total += 1
                return
            except Exception:  # noqa: BLE001 - retried with backoff
                # SystemRandom jitter: security-independent, but it also makes
                # the delay unpredictable to a peer forcing reconnects.
                delay = 1 + (self.reconnect_retries - retry) * 5 + secrets.randbelow(501) / 1000
                await self._sleep(delay)
        self._log.warning("Reconnect retries exhausted; staying alive with source_healthy=0")

    def _log_summary(self, target_count: int) -> None:
        now = self._now()
        if now - self._summary_last < self.summary_interval:
            return
        self._log.info("[DATA] Published %d metrics in %d tags over %d polls (%.0fs)",
                       self._summary_published, target_count, self._summary_polls,
                       now - self._summary_last)
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
        if not self._client.is_connected():
            raise MqttDisconnectedError("MQTT client not connected (checked before the catalogue publish).")
        payload, revision = prepared
        try:
            self._publish_catalogue(prepared)
        except PublishRejected as exc:
            self._log.error("[SYNC] Catalogue rejected by the broker: %s", exc)
            return
        except (ConnectionError, PublishTimeout) as exc:
            # The broker is gone, not slow: the loop's outage path owns this.
            raise MqttDisconnectedError(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 - reported; retried next iteration
            self._log.exception("Failed to publish the catalogue: %s", exc)
            return
        self._log.info("[SYNC] Published %d data tags to %s (revision %s)",
                       len(payload.data_tags), str(self._catalogue_topic), payload.version[:12])

    def _placement_reannounced(self) -> None:
        # Nothing to do: a moved catalogue is already dirty with its
        # revision forgotten, and the loop publishes what is due.
        pass

    # -- synthetic tags ---------------------------------------------------

    def _current_heartbeat_value(self) -> bool:
        """Flips every ``heartbeat_interval`` seconds since startup."""
        elapsed = self._now() - self._heartbeat_start
        return (int(elapsed // self.heartbeat_interval) % 2) == 0

    def _set_source_healthy(self, state: bool) -> None:
        """The one writer of the source-health flag: telemetry and the
        in-process mirror never drift, and a change is surfaced at once
        through :meth:`_publish_is_connected` — the per-cycle dedup cannot
        be relied on for a transition while the broker session stays up."""
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
        timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
        for target in targets:
            key = str(target.topic)
            if self._is_connected_published.get(key) == state:
                continue
            metric = Metric(value=state, timestamp=timestamp, signal_id=target.signal.id)
            try:
                self._client.publish(target.topic, metric, qos=1)
            except Exception as exc:  # noqa: BLE001 - left for the next poll to retry
                self._log.warning("[IS_CONNECTED] publish failed for %s, will retry: %s", key, exc)
                continue
            self._is_connected_published[key] = state
            self._log.info("[IS_CONNECTED] %s source connectivity -> %s", key, state)

    # -- publishing -------------------------------------------------------

    def _buffer_pending(self, pending: list[tuple[Topic, Metric]]) -> None:
        """Keep what a cycle could not publish for the next one — bounded
        at ``max_pending``, oldest dropped, so a long outage costs bounded
        memory."""
        if len(pending) > self.max_pending:
            drop = len(pending) - self.max_pending
            self._log.warning("Dropping %d buffered metrics (backpressure). Max pending: %d.",
                              drop, self.max_pending)
            pending = pending[drop:]
        self._pending = pending

    def _publish_batch(self, batch: list[tuple[Topic, Metric]]) -> None:
        """Publish one cycle's metrics, the previous cycle's pending ones
        first. Checks ``is_connected()`` before anything: paho queues a
        QoS>=1 publish while disconnected instead of raising, and franzmq
        would only notice via PublishTimeout after the full timeout, one
        metric at a time — so a broker outage is failed closed here,
        immediately, into the loop's reconnect path."""
        if self._pending:
            batch = self._pending + batch
            self._pending = []
        client = self._client
        if client is None or not client.is_connected():
            self._buffer_pending(batch)
            raise MqttDisconnectedError("MQTT client not connected (checked before publish).")
        # The link answered: an outage that paho's own reconnect ended between
        # two polls is over, and the next one is news again.
        self._report_mqtt_recovered()
        for index, (topic, metric) in enumerate(batch):
            try:
                # QoS 1: durable ingest is the point, and it is the only
                # level at which the broker can say it refused a record.
                client.publish(topic, metric, qos=1)
                self._summary_published += 1
                self.telemetry.published(metric, node_id=self._node_id or "")
            except PublishRejected as exc:
                # The broker judged the record and said no. Retrying would
                # fail identically, so it is dropped — never silently: the
                # reason names what to fix (contract, schema, grant).
                self.telemetry.publish_rejected(exc.reason_code)
                self._log.error("[PUBLISH] %s", exc)
            except (ConnectionError, MqttDisconnectedError, PublishTimeout) as exc:
                # PublishTimeout means paho queued it without delivering: the
                # broker is gone, not slow — a disconnect, not a per-metric
                # failure.
                self._buffer_pending(batch[index:])
                raise MqttDisconnectedError(str(exc)) from exc
            except Exception as exc:  # noqa: BLE001 - one metric's failure is not the batch's
                self._log.error("Publish error to %s: %s", topic, exc)

    # -- outage reporting -------------------------------------------------

    def _report_mqtt_outage(self, error: Exception) -> None:
        """Say the broker is unreachable once, then rarely, not once per
        poll. An outage is a STATE: it begins, it ends, and while it lasts
        the only news is that it still lasts — the shape colca's
        ``linkstate.go`` uses for a replication lane. Logging it per poll
        wrote 698 identical ERROR records into the tree during one
        deployment's startup window."""
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
        self._log.error("MQTT still disconnected after %.0fs and %d attempts: %s",
                        now - self._mqtt_down_since, self._mqtt_down_attempts, error)

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
        except Exception:  # noqa: BLE001 - shutdown must reach the driver regardless
            self._log.exception("Error during MQTT teardown")
        finally:
            self.telemetry.broker_healthy(False)
        try:
            await self.driver.close()
        except Exception:  # noqa: BLE001 - best effort
            self._log.exception("Error during source teardown")
        finally:
            self.telemetry.source_healthy(False)
            self._source_healthy = False
