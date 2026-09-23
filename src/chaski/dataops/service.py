"""``chaski.DataOpsService``: a :class:`chaski.Service` that runs producers.

To the node it is one more local service: it registers, publishes
``_ServiceDetails`` and opens the door like any ``Service``, and adds the
producer runtime.

* **One ingest lane**: :class:`~chaski.dataops.ingest.Ingest` over
  ``self.stream("metrics", cursor="ingest-<generation>", signal_ids=...)``,
  acking after processing. There is no second path to the data.
* **MQTT wakes the ingest for the service's own inputs**:
  :meth:`DataOpsService._wake_on_inputs` subscribes the ``_Metric`` topic of
  each resolved input signal at QoS 0, again whenever the inputs resolve
  anew, and turns a burst of messages into one wake. The payload is not read.
* **The buffer is the only local state**: one SQLite
  :class:`~chaski.dataops.buffer.Buffer` under ``data_dir`` holds input
  windows and watermarks; :func:`trim_buffer` prunes it.
* **The historian is optional and read-only**: pass a
  :class:`~chaski.dataops.inputs.Historian` as ``historian=``. The SDK ships
  no database driver.
* **Outputs are catalogued**: :func:`~chaski.dataops.outputs.build_catalogue`
  publishes a ``_DataTags`` record like a connector's; annotations are
  ``_Annotation`` records.
* **A code change replays**: :func:`replay_changed_producers`, keyed by
  :func:`~chaski.dataops.codehash.compute_code_hash`.
* **``_Constant``/``_Signal`` are watched, not ingested**:
  :mod:`chaski.dataops.watch` subscribes ``@on_constant``/``@on_signal``
  triggers directly on the node's retained records — neither contract has a
  stream to poll or buffer.

Startup order inside :meth:`DataOpsService.serve`:

 1. ``start()``: connect, register, open the door, then open the buffer
 2. instantiate every producer, attach it and run ``setup()``; a producer
    whose ``setup()`` raises is skipped with a logged reason
 3. publish the ``SignalOutput`` catalogue if it changed, and bind every
    output, ``AnnotationOutput`` included
 4. run every producer's ``on_ready()`` — outputs are bound, and no trigger
    has fired yet, so this is where startup compute belongs
 5. check every input window against the broker's metrics retention
 6. build the ``signal_id -> [handler]`` dispatch table and the set of input
    signal ids
 7. replay every producer whose code hash changed, a new one included: reset
    its watermark to the earliest buffered point of its inputs and feed the
    buffered records through the live handlers. Unchanged producers are left
    alone
 8. retire the previous generation's ingest cursor, if one is known
 9. start the ingest task, and a retry loop for inputs that did not resolve
 10. subscribe every ``@on_constant``/``@on_signal`` trigger
     (:mod:`chaski.dataops.watch`); the input topics that wake the ingest
     are subscribed when its stream opens (step 8)
 11. schedule cron and interval ticks, and the periodic buffer trim
 12. open the health door and block until ``stop``, then unwind in reverse
     and ``close()``
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import importlib
import logging
import pkgutil
import signal
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import fields
from pathlib import Path
from typing import Any, cast

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from colca_data_contracts import Metric

from chaski.door import Door, Record, Stream
from chaski.service import Service

from . import codehash, health, resolve, watch
from .base import Producer, Runtime
from .buffer import Buffer
from .ingest import Ingest
from .inputs import Historian, declared_inputs, validate_windows
from .outputs import bind_annotation_outputs, build_catalogue, declared_outputs
from .triggers import CronSpec, IntervalSpec, OnConstantSpec, OnMetricSpec, OnSignalSpec

log = logging.getLogger("chaski.dataops")

#: A burst of input metrics within this window wakes the ingest once.
WAKE_COALESCE_S = 0.2
#: colca's default metrics retention — what a declared window is checked
#: against when the service is given no ``retention=`` of its own.
DEFAULT_RETENTION_S = 14 * 24 * 3600.0
_METRIC_FIELDS = {f.name for f in fields(Metric)}


# ─── discovery ──────────────────────────────────────────────────────────────


def import_package(package: str) -> list[str]:
    """Import ``package`` and every submodule, so their ``Producer`` subclasses
    register. Returns the imported module names; a missing package logs a
    warning and returns an empty list."""
    try:
        pkg = importlib.import_module(package)
    except ModuleNotFoundError:
        log.warning("Producer package %s not found — skipping", package)
        return []
    names = [package]
    if not hasattr(pkg, "__path__"):
        log.debug("Producer package %s is a single module, already imported", package)
        return names
    for module_info in pkgutil.walk_packages(pkg.__path__, prefix=f"{package}."):
        importlib.import_module(module_info.name)
        names.append(module_info.name)
        log.debug("Imported producer module %s", module_info.name)
    return names


def _claim_producers_built_in(module_names: Iterable[str]) -> None:
    """Point a producer built with ``type()`` at the module that holds it.

    Such a class names the module that made the call as its own (``abc``, for
    ``Producer``'s metaclass), so discovery would skip it and its code hash
    would cover the wrong source.
    """
    for module_name in module_names:
        module = sys.modules.get(module_name)
        if module is None:
            continue
        for value in list(vars(module).values()):
            if isinstance(value, type) and issubclass(value, Producer) and not _held_by_own_module(value):
                value.__module__ = module_name


def _held_by_own_module(cls: type) -> bool:
    obj: Any = sys.modules.get(cls.__module__)
    for part in cls.__qualname__.split("."):
        obj = getattr(obj, part, None)
    return obj is cls


def import_directory(path: Path) -> list[str]:
    """Import every top-level ``*.py`` under ``path`` so its ``Producer``
    subclasses register. Returns the imported module names.

    The directory goes on ``sys.path`` and each file is imported by its stem,
    so the modules can import each other (``from machine_base import
    MachineBase``). Files starting with ``_`` and subdirectories are skipped.
    A module that fails to import is logged and skipped.
    """
    path = Path(path)
    if not path.is_dir():
        log.info("No producer directory at %s — nothing imported from it", path)
        return []

    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

    imported: list[str] = []
    for py in sorted(path.glob("*.py")):
        if py.name.startswith("_"):
            continue
        try:
            importlib.import_module(py.stem)
            log.info("Imported producer module %s", py.stem)
            imported.append(py.stem)
        except Exception:
            log.exception("Producer module %s failed to import — skipping", py.stem)
    return imported


# ─── ticks: on the scheduler, off the loop ─────────────────────────────────


def off_loop(method):
    """A producer tick as a job the scheduler runs off the event loop.

    Tick bodies are synchronous (sqlite reads, HTTP publishes) and would block
    every timer if they ran on the loop. As a plain function APScheduler runs
    the job in its thread pool, where this drives the coroutine on a private
    loop; ``max_instances=1`` keeps a slow tick from overlapping itself.

    The tick holds the producer's lock, because the producer's ``@on_metric``
    handlers run on another thread (see :func:`make_handler`).
    """

    @functools.wraps(method)
    def job() -> None:
        with method.__self__._lock:
            asyncio.run(method())

    return job


def schedule_periodic(scheduler: AsyncIOScheduler, instance: Producer) -> int:
    """Wire each (method, cron-or-interval) pair onto the scheduler.

    ``OnMetricSpec`` triggers are not scheduled here; they are in the dispatch
    table from :func:`build_dispatch`. ``OnConstantSpec``/``OnSignalSpec``
    triggers are not scheduled here either; :func:`watch.gather_triggers`
    subscribes them onto the constant/signal MQTT watch. Every job this
    function DOES schedule is wrapped by :func:`off_loop`.
    """
    count = 0
    for method_name, spec in instance.__class__._triggers:
        method = getattr(instance, method_name)
        if isinstance(spec, CronSpec):
            ap_trigger = CronTrigger.from_crontab(spec.expression)
            kind = f"cron({spec.expression!r})"
        elif isinstance(spec, IntervalSpec):
            ap_trigger = IntervalTrigger(seconds=spec.seconds)
            kind = f"every({spec.seconds}s)"
        elif isinstance(spec, (OnMetricSpec, OnConstantSpec, OnSignalSpec)):
            continue  # each owned and scheduled elsewhere (see docstring)
        else:
            log.warning("Unknown trigger spec %r on %s.%s — skipped", spec, instance.name, method_name)
            continue

        job_id = f"{instance.name}.{method_name}::{kind}"
        scheduler.add_job(
            off_loop(method),
            trigger=ap_trigger,
            id=job_id,
            name=job_id,
            replace_existing=True,
            coalesce=True,  # if late, run once not N times
            max_instances=1,  # never overlap a slow method with itself
            misfire_grace_time=60,
        )
        log.info("Scheduled %s.%s with %s", instance.name, method_name, kind)
        count += 1
    return count


# ─── on_metric dispatch (feeds the Ingest loop) ─────────────────────────────


def build_dispatch(
    runtime: Runtime,
    instances: list[Producer],
) -> tuple[dict[str, list], list[str], int]:
    """Resolve one pass's worth of names from a single KV read.

    Nothing is kept between passes, so a rebound signal is picked up on the
    next pass.

    `forget_resolved` runs only when this pass pinned a fresh snapshot. If the
    read fails, inputs keep the ids they resolved before and only inputs that
    never resolved are attempted, so a refused read never looks like every
    signal disappeared.
    """
    with resolve.one_pass(runtime.door) as pinned:
        if pinned:
            forget_resolved(instances)
        else:
            log.warning(
                "Could not pin a KV snapshot for this resolution pass — "
                "keeping every already-resolved input's id rather than risk "
                "narrowing the fetch filter below what is actually bound."
            )
        return _resolve_dispatch(instances)


def forget_resolved(instances: list[Producer]) -> None:
    """Drop every held id, inputs and outputs together, so this pass resolves
    them again. Called only when `build_dispatch` pinned a fresh KV read.
    """
    for instance in instances:
        for _name, declared_input in declared_inputs(instance):
            declared_input.forget()
        for _name, declared_output in declared_outputs(instance):
            declared_output.forget()


def _resolve_dispatch(
    instances: list[Producer],
) -> tuple[dict[str, list], list[str], int]:
    """Walk every producer's declared inputs and ``@on_metric`` triggers, and
    return ``(dispatch, signal_ids, unresolved)``.

    ``dispatch`` maps ``signal_id -> [async handler(record), ...]`` for
    :class:`~chaski.dataops.ingest.Ingest`, built only from ``@on_metric``
    declarations.

    ``signal_ids`` is the union of every declared input's signal id, sent with
    the fetch. It is wider than the dispatch keys, because a producer driven
    only by ticks still needs its inputs in the buffer.

    ``unresolved`` counts inputs that could not be resolved. They are logged,
    left out of both, and retried by :func:`reresolve_loop`; producers often
    start before their signals are commissioned.
    """
    dispatch: dict[str, list] = {}
    signal_ids: dict[str, None] = {}  # ordered de-dup, dict as a set
    unresolved = 0

    for instance in instances:
        cls = type(instance)

        # Resolve every declared SignalRangeInput on this producer exactly
        # once, whatever triggers it (or none at all).
        resolved: dict[str, str] = {}
        for attr_name, input_attr in declared_inputs(instance):
            try:
                signal_id = input_attr.signal_id
            except Exception as exc:
                log.warning(
                    "%s.%s: cannot resolve signal_id for declared input %r yet (%s) — "
                    "excluded from the fetch filter and dispatch until it resolves",
                    instance.name,
                    attr_name,
                    input_attr.signal_name,
                    exc,
                )
                unresolved += 1
                continue
            resolved[attr_name] = signal_id
            signal_ids.setdefault(signal_id, None)

        for method_name, spec in cls._triggers:
            if not isinstance(spec, OnMetricSpec):
                continue
            if spec.input_name not in resolved:
                if getattr(instance, spec.input_name, None) is None:
                    log.error(
                        "%s.%s declares @on_metric(%r) but %r is not a class attribute — skipped",
                        instance.name,
                        method_name,
                        spec.input_name,
                        spec.input_name,
                    )
                # else: a declared input that failed resolution above —
                # already warned there, skip this handler silently.
                continue

            signal_id = resolved[spec.input_name]
            method = getattr(instance, method_name)
            dispatch.setdefault(signal_id, []).append(make_handler(method))
            log.info(
                "Will dispatch %s.%s for signal_id=%s (input %s)",
                instance.name,
                method_name,
                signal_id,
                spec.input_name,
            )

    return dispatch, list(signal_ids), unresolved


async def reresolve_loop(
    runtime: Runtime,
    instances: list[Producer],
    ingest,
    stop: asyncio.Event,
    ensure_running,
    interval_s: float = 15.0,
) -> None:
    """Retry the inputs that did not resolve, until they all do.

    Without this, a producer started before its signals were commissioned
    would keep an empty dispatch table. Stops once everything resolves.
    """
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
            return  # stop was set
        except TimeoutError:
            pass

        # A resolution pass is a KV read over sync httpx — worker thread,
        # not the loop that owns every timer in the process.
        dispatch, signal_ids, unresolved = await asyncio.to_thread(build_dispatch, runtime, instances)
        if signal_ids:
            ingest.rebind(dispatch, signal_ids)
            ensure_running()
        if not unresolved:
            log.info("Every declared input resolved — dispatch now covers %d signal(s).", len(dispatch))
            return


def compute_trim_horizons(instances: list[Producer], retention_s: float) -> dict[str, float]:
    """Per-signal trim horizon: the larger of the signal's widest declared
    window and the broker's metrics retention.

    Keeping the broker's whole retention locally lets a replay start from the
    earliest buffered point without reading the stream again. Unresolved
    inputs are skipped; nothing lands in the buffer for them yet.
    """
    horizons: dict[str, float] = {}
    for instance in instances:
        for attr_name, input_attr in declared_inputs(instance):
            try:
                signal_id = input_attr.signal_id
            except Exception as exc:
                log.debug(
                    "%s.%s: cannot resolve signal_id for declared input %r yet (%s) — "
                    "excluded from the trim horizon until it resolves",
                    instance.name,
                    attr_name,
                    input_attr.signal_name,
                    exc,
                )
                continue
            horizon = max(input_attr.window_s, retention_s)
            if horizon > horizons.get(signal_id, 0.0):
                horizons[signal_id] = horizon
    return horizons


def trim_buffer(buffer: Buffer, instances: list[Producer], retention_s: float) -> None:
    """Periodic job: drop buffered points older than each signal's horizon.

    Horizons are recomputed from the currently resolved inputs on every run,
    so a signal that resolves late is trimmed too; resolved ids are held in
    memory, so this needs no KV scan. A plain function, so APScheduler runs
    the sqlite work off the loop."""
    horizons = compute_trim_horizons(instances, retention_s)
    deleted = buffer.trim(horizons)
    if deleted:
        log.info("Buffer trim: deleted %d point(s) past their per-signal horizon", deleted)
    else:
        log.debug("Buffer trim: nothing past its per-signal horizon")


def make_handler(method):
    """Adapt a producer's ``@on_metric`` method (``async def f(self, metric)``)
    into an Ingest handler (``async def h(record)``) that decodes the payload
    into a ``Metric``. Holds the producer's lock, like :func:`off_loop`."""

    async def _handler(record: Record) -> None:
        metric = decode_metric(record)
        with method.__self__._lock:
            await method(metric)

    return _handler


def decode_metric(record: Record) -> Metric:
    payload = record.payload if isinstance(record.payload, dict) else {}
    fields_only = {k: v for k, v in payload.items() if k in _METRIC_FIELDS}
    # record.ts is in milliseconds; fallback_timestamp_s converts to seconds.
    fields_only.setdefault("timestamp", record.fallback_timestamp_s)
    return Metric(**fields_only)


def synthetic_record(signal_id: str, ts: float, value: Any, *, actor: str = "replay") -> Record:
    """A ``Record`` built from a buffered point, so replay goes through the same
    handlers as live traffic. Offset and topic mean nothing here;
    ``written_by`` names the replaying ``actor``.
    """
    return Record(
        offset=-1,
        origin_offset=-1,
        topic="",
        payload={"signal_id": signal_id, "timestamp": ts, "value": value},
        ts=ts,
        written_by=f"dataops-{actor}",
        actor_id="",
        actor_label="",
        actor_kind=actor,
    )


# ─── hash-triggered replay ────────────────────────────────────────────────


async def replay_changed_producers(runtime: Runtime, instances: list[Producer]) -> None:
    """Replay every producer whose code hash changed, a new producer included.

    Its watermark is reset to the earliest buffered point of its inputs, and
    every buffered record for its ``@on_metric`` signals goes, in timestamp
    order, through the same handlers live traffic uses. Replayed publishes
    overwrite like live ones. A producer with an unchanged hash is skipped, so
    a replay happens once per change; a tick-only producer only gets its
    watermark reset.

    A handler that raises is logged and skipped, as in live ingest, and the
    watermark and hash are stored after the pass regardless, so a broken
    producer replays once instead of on every restart. A pass with failures
    ends with a warning that counts them.
    """
    # One KV read for the whole sweep; the per-producer passes below reuse it.
    with resolve.one_pass(runtime.door):
        await _replay_each(runtime, instances)


async def _replay_each(runtime: Runtime, instances: list[Producer]) -> None:
    buffer = runtime.buffer
    for instance in instances:
        cls = type(instance)
        new_hash = codehash.compute_code_hash(cls)
        old_hash = buffer.code_hash(instance.name)
        if old_hash == new_hash:
            continue

        mini_dispatch, mini_signal_ids, _ = build_dispatch(runtime, [instance])
        earliest = _earliest_across(buffer, mini_signal_ids)
        now = time.time()
        start = earliest if earliest is not None else now

        log.info(
            "%s: code hash changed (%s -> %s) — replaying buffered window [%.3f, %.3f)",
            instance.name,
            (old_hash or "none")[:12],
            new_hash[:12],
            start,
            now,
        )

        rows: list[tuple[float, str, Any]] = []
        for signal_id in mini_signal_ids:
            df = buffer.window(signal_id, start, now)
            for row in df.itertuples():
                rows.append((cast(float, row.ts), signal_id, row.value))
        rows.sort(key=lambda r: r[0])

        failures = 0
        for ts, signal_id, value in rows:
            record = synthetic_record(signal_id, ts, value)
            for handler in mini_dispatch.get(signal_id, []):
                try:
                    await handler(record)
                except Exception:
                    # Like live ingest: log a failing handler and move on.
                    failures += 1
                    log.exception(
                        "%s: on_metric handler failed replaying signal_id=%s at ts=%.3f — continuing",
                        instance.name,
                        signal_id,
                        ts,
                    )

        # Stored even after handler failures, so the same window is not
        # replayed on every restart.
        final_position = rows[-1][0] if rows else start
        buffer.set_watermark(instance.name, final_position, new_hash)
        log_fn = log.warning if failures else log.info
        log_fn(
            "%s: replay complete (%d record(s), %d failure(s)) — watermark=%.3f",
            instance.name,
            len(rows),
            failures,
            final_position,
        )


def _earliest_across(buffer: Buffer, signal_ids: Iterable[str]) -> float | None:
    """``MIN`` of :meth:`Buffer.earliest` across several signals, or
    ``None`` when none of them holds anything yet."""
    values = [e for e in (buffer.earliest(sid) for sid in signal_ids) if e is not None]
    return min(values) if values else None


# ─── MQTT dispatch ──────────────────────────────────────────────────────────


def tolerate_undecodable(client) -> None:
    """Make every subscription on ``client`` survive a message franzmq cannot
    decode — the input wake's own need, and equally
    :mod:`chaski.dataops.watch`'s: a ``_Constant``/``_Signal`` tombstone (an
    empty retained payload — the documented "this record was retired"
    convention) is exactly the kind of message franzmq's typed decode was not
    written to expect.

    franzmq decodes every inbound message on paho's network thread before
    dispatching, and an undecodable one would kill that thread — every
    subscription on this client, input wake and typed alike, since one client
    thread serves them all. On a decode failure the raw, undecoded message
    goes to the matching callbacks instead (their own decoding, if any, is
    on them), with a warning naming the topic.

    Idempotent: patches ``client._handle_on_message`` once per client, so the
    input wake and :func:`chaski.dataops.watch.start` can both call this on the
    same client without wrapping it twice.
    """
    from paho.mqtt.client import Client as PahoClient

    typed_dispatch = client._handle_on_message
    if getattr(typed_dispatch, "_tolerates_undecodable", False):
        return
    raw_dispatch = PahoClient._handle_on_message

    def guarded(message):
        try:
            return typed_dispatch(message)
        except Exception:
            log.warning(
                "undecodable message on %s — dispatching it undecoded instead",
                getattr(message, "topic", "?"),
                exc_info=True,
            )
            return raw_dispatch(client, message)

    guarded._tolerates_undecodable = True  # type: ignore[attr-defined]  # the idempotence marker itself
    client._handle_on_message = guarded


# ─── the service ────────────────────────────────────────────────────────────


class DataOpsService(Service):
    """A :class:`chaski.Service` that runs :class:`~chaski.dataops.Producer`
    classes. Everything the base class does works unchanged (``node=``,
    registration, logs, ``kv()``, ``stream()``); this adds the producer
    runtime described in the module docstring.

    ::

        import chaski
        from chaski.dataops import Producer, SignalRangeInput, SignalOutput, every

        class Oee(Producer):
            name = "oee"
            system_element_name = "press3"
            speed = SignalRangeInput("speed", window="1h")
            oee = SignalOutput("oee", data_type="float", description="Rolling OEE")

            @every("1m")
            async def tick(self):
                frame = self.speed.fetch(self.watermark, now())
                self.oee.publish(compute_oee(frame))
                self.advance_watermark(now())

        svc = chaski.DataOpsService("analytics", mount="site1/line1")
        svc.add(Oee)
        svc.run()

    ``data_dir`` holds the buffer (default: the service's own state
    directory, ``~/.colca/services/<name>``); ``retention`` is the broker's
    metrics retention in seconds, what a declared window is validated
    against; ``historian`` an optional read-only
    :class:`~chaski.dataops.inputs.Historian`; ``poll_interval``/
    ``trim_interval`` the ingest poll and buffer-trim cadences;
    ``health_port`` the health door (``0`` for an ephemeral port). Every
    other keyword is the base class's.
    """

    def __init__(
        self,
        name: str,
        mount: str = "",
        *,
        node: Any = None,
        data_dir: Path | None = None,
        retention: float | None = None,
        historian: Historian | None = None,
        poll_interval: float = 1.0,
        trim_interval: float = 3600.0,
        health_port: int = health.PORT_DEFAULT,
        **service_kw: Any,
    ) -> None:
        super().__init__(name, mount, node=node, **service_kw)
        self._data_dir = Path(data_dir) if data_dir is not None else self._state_dir
        self.retention_s = float(retention) if retention is not None else DEFAULT_RETENTION_S
        self._historian = historian
        self._poll_interval_s = poll_interval
        self._trim_interval_s = trim_interval
        self._health_port = health_port
        self._producers: dict[str, type[Producer]] = {}
        self._local_buffer: Buffer | None = None
        self._ingest: Ingest | None = None
        self._wake_topics: set[str] = set()
        self._wake_pending = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self.instances: list[Producer] = []

    # -- the run list ----------------------------------------------------

    def add(self, producer_cls: type[Producer]) -> DataOpsService:
        """Run ``producer_cls``. Explicit registration — the counterpart of
        :meth:`discover`. A second class with the same ``name`` replaces
        the first (one producer per name, the buffer keys watermarks by it).
        Returns ``self`` for chaining."""
        if not (isinstance(producer_cls, type) and issubclass(producer_cls, Producer)):
            raise TypeError(f"DataOpsService.add() takes a Producer subclass, got {producer_cls!r}")
        name = getattr(producer_cls, "name", None)
        if not name:
            raise ValueError(f"{producer_cls.__name__} has no `name` — every producer needs one")
        if name in self._producers and self._producers[name] is not producer_cls:
            log.warning(
                "Producer name %r added twice — %s replaces %s",
                name,
                producer_cls.__name__,
                self._producers[name].__name__,
            )
        self._producers[name] = producer_cls
        return self

    def discover(self, package: str) -> int:
        """Import ``package`` (and every submodule under it) and run every
        concrete producer it defines. Returns how many were added."""
        modules = set(import_package(package))
        _claim_producers_built_in(modules)
        return self._adopt(lambda cls: cls.__module__ in modules or cls.__module__.startswith(f"{package}."))

    def discover_directory(self, path: Path) -> int:
        """Import every top-level ``*.py`` under ``path`` (see
        :func:`import_directory`) and run every concrete producer those
        files define. Returns how many were added."""
        modules = set(import_directory(path))
        _claim_producers_built_in(modules)
        return self._adopt(lambda cls: cls.__module__ in modules)

    def _adopt(self, owned: Callable[[type[Producer]], bool]) -> int:
        added = 0
        for cls in Producer.all():
            if owned(cls) and self._producers.get(cls.name) is not cls:
                self.add(cls)
                added += 1
        return added

    @property
    def producers(self) -> list[type[Producer]]:
        """The producer classes this service runs, sorted by name."""
        return [self._producers[n] for n in sorted(self._producers)]

    # -- Runtime ---------------------------------------------------------

    @property
    def door(self) -> Door:
        """The door every input resolves and every output publishes
        through — the base class's own, open after :meth:`start`."""
        return self._require_http("door")

    @property
    def buffer(self) -> Buffer:
        """The one local state, open after :meth:`start`."""
        if self._local_buffer is None:
            raise RuntimeError(
                "chaski.DataOpsService: call start() (or run()/serve()) before buffer — "
                "the buffer is opened alongside the door"
            )
        return self._local_buffer

    @property
    def historian(self) -> Historian | None:
        return self._historian

    @property
    def data_dir(self) -> Path:
        return self._data_dir

    # -- lifecycle -------------------------------------------------------

    def start(self, *, connect_timeout: float = 10.0) -> DataOpsService:
        """The base class's :meth:`~chaski.Service.start`, then open the
        buffer. Idempotent."""
        super().start(connect_timeout=connect_timeout)
        if self._local_buffer is None:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._local_buffer = Buffer(self._data_dir / "buffer.sqlite3")
        return self

    def close(self) -> None:
        super().close()
        if self._local_buffer is not None:
            self._local_buffer.close()
            self._local_buffer = None

    def instantiate(self) -> list[Producer]:
        """Instantiate every added producer and attach it to this runtime —
        without running ``setup()``; :meth:`serve` does that. A producer
        whose constructor raises is skipped with a logged reason."""
        instances: list[Producer] = []
        for cls in self.producers:
            try:
                instance = cls().attach(self)
            except Exception:
                log.exception("Producer %s could not be instantiated — skipping", cls.name)
                continue
            instances.append(instance)
        return instances

    def bind_outputs(self, instances: list[Producer]) -> dict[str, str]:
        """Catalogue every declared ``SignalOutput`` and bind every
        ``AnnotationOutput`` on ``instances``. Returns the catalogue's
        ``{source: tag_id}``. Requires :meth:`start`."""
        door = self.door
        node_id, service_ulid = self._node_id, self._service_id
        if node_id is None or service_ulid is None:
            raise RuntimeError("chaski.DataOpsService: call start() before bind_outputs()")
        result = build_catalogue(
            instances,
            door,
            node_id=node_id,
            mount=self._mount,
            service_name=self.name,
            service_ulid=service_ulid,
        )
        bind_annotation_outputs(instances, node_id=node_id, mount=self._mount)
        return result

    def _open_ingest_stream(self, cursor: str, signal_ids: list[str] | None) -> Stream:
        self._wake_on_inputs(signal_ids or [])
        return self.stream("metrics", cursor=cursor, signal_ids=signal_ids)

    def _wake_on_inputs(self, signal_ids: list[str]) -> None:
        """Subscribe the ``_Metric`` topic of each input signal, and drop the
        ones no longer read. Called whenever the ingest stream (re)opens, so a
        late-resolved input starts waking the ingest from then on.

        QoS 0: a lost message costs one wake, which the poll interval covers.
        """
        client = self._client
        if client is None:
            return
        topics = set(resolve.resolve_metric_topics(self.door, signal_ids).values())
        if len(topics) < len(signal_ids):
            log.debug("%d input signal(s) have no known topic yet", len(signal_ids) - len(topics))
        tolerate_undecodable(client)
        for topic in sorted(topics - self._wake_topics):
            client.message_callback_add(topic, self._on_input_metric)
            client.subscribe(topic, qos=0)
        for topic in sorted(self._wake_topics - topics):
            client.message_callback_remove(topic)
            client.unsubscribe(topic)
        self._wake_topics = topics
        log.info("ingest wakes on %d input topic(s)", len(topics))

    def _on_input_metric(self, _client, _userdata, _message) -> None:
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(self._wake_soon)

    def _wake_soon(self) -> None:
        if self._wake_pending or self._loop is None:
            return
        self._wake_pending = True
        self._loop.call_later(WAKE_COALESCE_S, self._wake_now)

    def _wake_now(self) -> None:
        self._wake_pending = False
        if self._ingest is not None:
            self._ingest.wake()

    def _watch_constants_and_signals(self, instances: list[Producer]) -> None:
        """Subscribe every ``@on_constant``/``@on_signal`` trigger declared
        across ``instances`` — see :mod:`chaski.dataops.watch`. A no-op when
        nothing declared either."""
        constant_triggers, signal_triggers = watch.gather_triggers(instances)
        watch.start(
            self._started_client,
            cast(str, self._node_id),
            self.door,
            cast(asyncio.AbstractEventLoop, self._loop),
            constant_triggers,
            signal_triggers,
        )

    async def serve(self, stop: asyncio.Event | None = None) -> None:
        """Run the service on the current event loop until ``stop`` is set
        — see the module docstring for the startup order. :meth:`run` is
        the blocking wrapper with signal handling."""
        stop = stop or asyncio.Event()
        self.start()
        loop = asyncio.get_running_loop()
        self._loop = loop

        producers = self.producers
        if not producers:
            log.warning("No producers added to %s — add() or discover() some before serve().", self.name)
        else:
            log.info("Running %d producer(s): %s", len(producers), [p.name for p in producers])

        # 2) Instantiate + run setup() — best-effort (see module docstring).
        instances: list[Producer] = []
        for instance in self.instantiate():
            try:
                await instance.setup()
            except Exception:
                log.exception("Producer %s setup() failed — skipping", instance.name)
                continue
            instances.append(instance)
        self.instances = instances

        # 3) Catalogue the SignalOutputs and bind the AnnotationOutputs.
        self.bind_outputs(instances)

        # 4) Outputs are bound and no trigger has fired yet: producers do
        #    startup compute here instead of retrying publish() on the
        #    RuntimeError it raises before binding.
        for instance in instances:
            try:
                await instance.on_ready()
            except Exception:
                log.exception("Producer %s on_ready() failed — its triggers are still wired", instance.name)

        # 5) Refuse windows longer than the broker's retention without a historian.
        validate_windows(
            instances,
            retention_s=self.retention_s,
            historian_configured=self._historian is not None,
        )

        # 6) The on_metric dispatch table and the declared input signal ids.
        dispatch, signal_ids, unresolved = build_dispatch(self, instances)

        # 7) Replay producers whose code changed.
        await replay_changed_producers(self, instances)

        # 8) Retire the previous generation's cursor (if any) and build the
        #    ingest loop over this service's own consume lane.
        ingest = Ingest(
            self._open_ingest_stream,
            self.buffer,
            dispatch=dispatch,
            signal_ids=signal_ids or None,
            poll_interval_s=self._poll_interval_s,
            # A lost buffer's generation cannot be recovered, so nothing knows
            # the previous cursor yet and retiring it is a no-op.
            previous_generation=None,
        )
        ingest.retire_previous_generation()
        self._ingest = ingest

        # 9) Start ingest in the background and keep retrying unresolved inputs.
        health_state = health.HealthState(producers=len(instances), generation=self.buffer.generation)
        ingest_task: asyncio.Task | None = None
        if signal_ids:
            ingest_task = asyncio.ensure_future(ingest.run_forever(stop))
            health_state.ingest_task = ingest_task
            log.info("Ingest loop started: cursor=%s signal_ids=%d", ingest.cursor, len(signal_ids))
        else:
            log.warning("No producer declared a resolved input — ingest loop not started (nothing to fetch yet).")

        def _ensure_ingest_running() -> None:
            """Start the loop if nothing resolved at startup and something has now."""
            nonlocal ingest_task
            if ingest_task is None:
                ingest_task = asyncio.ensure_future(ingest.run_forever(stop))
                health_state.ingest_task = ingest_task
                log.info("Ingest loop started after a late resolve: cursor=%s", ingest.cursor)

        reresolve_task: asyncio.Task | None = None
        if unresolved:
            log.info("%d declared input(s) unresolved — retrying until they are commissioned.", unresolved)
            reresolve_task = asyncio.ensure_future(
                reresolve_loop(self, instances, ingest, stop, _ensure_ingest_running)
            )

        # 10) Every @on_constant/@on_signal subscription.
        self._watch_constants_and_signals(instances)

        # 11) Schedule cron/interval triggers (everything except @on_metric),
        #     plus the service's own periodic buffer trim.
        scheduler = AsyncIOScheduler()
        for instance in instances:
            schedule_periodic(scheduler, instance)
        scheduler.add_job(
            trim_buffer,
            args=(self.buffer, instances, self.retention_s),
            trigger=IntervalTrigger(seconds=self._trim_interval_s),
            id=f"{self.name}.buffer-trim",
            name=f"{self.name}.buffer-trim",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=60,
        )
        scheduler.start()
        log.info("Scheduler started (buffer trim every %.0fs).", self._trim_interval_s)

        # 12) The health door runs on this loop on purpose (see chaski.dataops.health).
        health_server = await health.serve(health_state, port=self._health_port)

        try:
            await stop.wait()
        finally:
            log.info("Shutting down...")
            scheduler.shutdown(wait=False)
            if reresolve_task is not None:
                # `stop` is already set, so the loop returns on its next wait;
                # cancel covers the case where it is mid-KV-read.
                reresolve_task.cancel()
            if ingest_task is not None:
                ingest.wake()
                try:
                    await asyncio.wait_for(ingest_task, timeout=10.0)
                except Exception:
                    log.exception("Ingest loop did not shut down cleanly")
            for instance in instances:
                try:
                    await instance.teardown()
                except Exception:
                    log.exception("Error during teardown of %s", instance.name)
            health_server.close()
            await health_server.wait_closed()
            self._ingest = None
            self.close()

    def run(self) -> None:
        """Block: :meth:`serve` on a fresh event loop until SIGINT/SIGTERM."""

        async def _main() -> None:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
            await self.serve(stop)

        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(_main())
