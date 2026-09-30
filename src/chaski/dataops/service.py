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
  declares them into the service's own catalogue, which is published as a
  ``_DataTags`` record like a connector's; annotations are ``_Annotation``
  records.
* **A code change replays**: :func:`replay_changed_producers`, keyed by
  :func:`~chaski.dataops.codehash.compute_code_hash`.
* **``_Constant``/``_Signal`` are watched, not ingested**:
  :mod:`chaski.dataops.watch` subscribes ``@on_constant``/``@on_signal``
  triggers directly on the node's retained records — neither contract has a
  stream to poll or buffer.

Startup order inside :meth:`DataOpsService.serve`:

 1. ``start()``: connect, register, open the door, then open the buffer
 2. instantiate every producer, attach it and run ``setup()``; a failed
    producer stops startup before intake can advance
 3. open the live resolution index (an unplaced local service: one KV read,
    then the node's retained ``_SystemElement``/``_Signal``/``_AnnotationType``
    records over MQTT), publish the ``SignalOutput`` catalogue if it changed,
    and bind every output, ``AnnotationOutput`` included
 4. run every producer's ``on_ready()`` — outputs are bound, and no trigger
    has fired yet, so this is where startup compute belongs
 5. check every input window against the broker's metrics retention
 6. build the ``signal_id -> [handler]`` dispatch table and the set of input
    signal ids
 7. replay every producer whose code hash changed, a new one included: reset
    its watermark to the earliest buffered point of its inputs and feed the
    buffered records through the live handlers. Unchanged producers are left
    alone. A failed replay is retried under :func:`supervise`
 8. retire the previous generation's ingest cursor, if one is known
 9. start the ingest task, and resolve again whenever the live index changes
    (:func:`follow_index`); without one, whenever a ``_Signal`` record under
    the service changes, and after every reconnect
 10. subscribe every ``@on_constant``/``@on_signal`` trigger
     (:mod:`chaski.dataops.watch`); the input topics that wake the ingest
     are subscribed when its stream opens (step 8)
 11. schedule cron and interval ticks, and the periodic buffer trim
 12. open the health door and block until ``stop``, then unwind in reverse
     and ``close()``

The replay of step 7 and every background task of steps 9 to 11 run under
:func:`supervise`: a task
that raises is logged at once, counted in ``handler_health`` (degraded, then
unhealthy) and restarted with jittered backoff. None dies silently.
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
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import fields
from pathlib import Path
from typing import Any, cast

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from colca_data_contracts import Metric

from chaski.door import Door, Record, Stream
from chaski.failures import HandlerHealth, Reject
from chaski.retained_view import ViewScope
from chaski.service import Service

from . import codehash, commands, health, resolve, watch
from .base import Producer, Runtime, restore_checkpoint, runtime_now, save_checkpoint, state_copy
from .buffer import Buffer
from .ingest import Ingest, consumer_name
from .inputs import Historian, declared_inputs, validate_windows
from .outputs import SignalOutput, bind_annotation_outputs, build_catalogue, declared_outputs, resolved_outputs
from .scheduling import record_rejection, run_due, run_periodic, timer_key
from .triggers import CronSpec, IntervalSpec, OnCommandSpec, OnConstantSpec, OnMetricSpec, OnSignalSpec

log = logging.getLogger("chaski.dataops")

#: A burst of input metrics within this window wakes the ingest once.
WAKE_COALESCE_S = 0.2
#: After a failed read of the input topics, the first retry waits this long,
#: doubling per consecutive failure up to :data:`WAKE_RETRY_MAX_S`.
WAKE_RETRY_S = 1.0
WAKE_RETRY_MAX_S = 30.0
#: colca's default metrics retention — what a declared window is checked
#: against when the service is given no ``retention=`` of its own.
DEFAULT_RETENTION_S = 14 * 24 * 3600.0
#: After a change to the resolution index, wait this long for the rest of a
#: burst (a plant model applied, a batch of bindings) before resolving again.
INDEX_SETTLE_S = 0.5
#: A failed seed of the resolution index is retried after this long,
#: doubling per consecutive failure up to :data:`WAKE_RETRY_MAX_S`.
INDEX_RETRY_S = 1.0
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
    and ``@on_constant`` handlers run on the event loop meanwhile (see
    :func:`make_handler`).
    """

    instance = method.__self__
    consumer = f"{instance.name}.{method.__name__}"

    @functools.wraps(method)
    def job() -> None:
        health = getattr(instance._runtime, "handler_health", None)
        try:
            with instance._lock:
                try:
                    asyncio.run(method())
                except Reject as rejected:
                    record_rejection(instance._runtime, consumer, {"timer": consumer, "at": time.time()}, rejected)
        except Exception as exc:
            # APScheduler logs it; the next tick runs the method again.
            if health is not None:
                health.failed(consumer, exc)
            raise
        if health is not None:
            health.succeeded(consumer)

    return job


def schedule_periodic(scheduler: AsyncIOScheduler, instance: Producer) -> int:
    """Wire each (method, cron-or-interval) pair onto the scheduler.

    ``OnMetricSpec`` triggers are not scheduled here; they are in the dispatch
    table from :func:`build_dispatch`. ``OnConstantSpec``/``OnSignalSpec``
    triggers are not scheduled here either; :func:`watch.gather_triggers`
    subscribes them onto the constant/signal MQTT watch. ``OnCommandSpec``
    triggers run on :class:`commands.CommandExecutor`. Every job this
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
        elif isinstance(spec, OnMetricSpec | OnConstantSpec | OnSignalSpec | OnCommandSpec):
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

    A live runtime reuses dispatch while its watched definition index is
    unchanged. Definition events and reconnects force re-resolution. Runtimes
    without a definition subscription retain fresh reads on every pass.

    `forget_resolved` runs only when this pass pinned a fresh snapshot. If the
    read fails, inputs keep the ids they resolved before and only inputs that
    never resolved are attempted, so a refused read never looks like every
    signal disappeared.
    """
    with resolve.one_pass(runtime.door) as pinned:
        index = resolve._pinned_index() if pinned else None
        cache_key = (index, tuple(instances))
        previous = getattr(runtime, "_resolved_dispatch", None)
        watched = getattr(runtime.door, "_dataops_definitions", None) is not None
        if watched and previous is not None and previous[0][0] is index and previous[0][1] == cache_key[1]:
            return previous[1]
        if pinned:
            forget_resolved(instances)
        else:
            log.warning(
                "Could not pin a KV snapshot for this resolution pass — "
                "keeping every already-resolved input's id rather than risk "
                "narrowing the fetch filter below what is actually bound."
            )
        result = _resolve_dispatch(instances)
        for instance in instances:
            for _name, output in declared_outputs(instance):
                if isinstance(output, SignalOutput):
                    output.flush()
        if watched and pinned:
            cast(Any, runtime)._resolved_dispatch = (cache_key, result)
        return result


def forget_resolved(instances: list[Producer]) -> None:
    """Drop every held id, inputs and outputs together, so this pass resolves
    them again. Called only when `build_dispatch` pinned a fresh index.
    """
    for instance in instances:
        for _name, declared_input in declared_inputs(instance):
            declared_input.forget()
        for _name, declared_output in resolved_outputs(instance):
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
    left out of both, and resolved again by :func:`follow_index` once they
    are commissioned; producers often start before their signals are.
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
            domain = getattr(instance, spec.input_name).time_domain
            dispatch.setdefault(signal_id, []).append(make_handler(method, time_domain=domain))
            log.info(
                "Will dispatch %s.%s for signal_id=%s (input %s)",
                instance.name,
                method_name,
                signal_id,
                spec.input_name,
            )

    return dispatch, list(signal_ids), unresolved


#: A restarted task that runs this long without failing counts as recovered.
TASK_STABLE_S = 30.0
#: Upper bound on the backoff before a crashed task is restarted.
TASK_BACKOFF_MAX_S = 30.0


async def supervise(
    name: str,
    start: Callable[[], Awaitable[Any]],
    stop: asyncio.Event,
    health: HandlerHealth,
    *,
    stable_s: float = TASK_STABLE_S,
    backoff_max_s: float = TASK_BACKOFF_MAX_S,
) -> None:
    """Run the coroutine ``start()`` makes until it returns or ``stop`` is set.

    When it raises, the exception is logged at once, ``health`` counts it as
    ``task <name>`` (degraded; unhealthy after repeated crashes), and a new
    coroutine starts after a jittered backoff. A restarted task that runs
    ``stable_s`` without failing clears the count. Cancelling the supervisor
    cancels the task.
    """
    from chaski.retry import Backoff

    consumer = f"task {name}"
    retry = Backoff(minimum=0.5, maximum=backoff_max_s)
    while True:
        task = asyncio.ensure_future(start())
        try:
            if retry.failures:
                done, _pending = await asyncio.wait({task}, timeout=stable_s)
                if not done:
                    health.succeeded(consumer)
                    retry.reset()
            await task
            if retry.failures:
                health.succeeded(consumer)
            return
        except asyncio.CancelledError:
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
            raise
        except Exception as exc:
            if stop.is_set():
                log.error("%s failed while stopping", name, exc_info=exc)
                return
            count = health.failed(consumer, exc)
            delay = retry.delay(exc)
            log.error("%s crashed (%d in a row); restarting it in %.1fs", name, count, delay, exc_info=exc)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
            if stop.is_set():
                return


async def reresolve_loop(runtime, instances, ingest, stop, ensure_running) -> None:
    """Rebind on definition updates, including reconnect and late commissioning."""
    cache = getattr(runtime.door, "_dataops_definitions", None)
    if cache is None:
        raise RuntimeError("DataOps requires its subscribed definition cache")
    from chaski.retry import Backoff

    retry = Backoff()
    active_dispatch = None
    while not stop.is_set():
        version = cache.changes.version
        try:
            dispatch, signal_ids, _unresolved = await asyncio.to_thread(build_dispatch, runtime, instances)
            if dispatch is not active_dispatch:
                ingest.rebind(dispatch, signal_ids or [])
                active_dispatch = dispatch
                if signal_ids:
                    ensure_running()
            retry.reset()
            await cache.changes.wait_async(version, stop=stop)
        except Exception as exc:
            log.exception("Could not refresh definition bindings; retrying")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=retry.delay(exc))


def compute_trim_horizons(
    instances: list[Producer], retention_s: float, *, time_domain: str | None = None
) -> dict[str, float]:
    """Per-signal trim horizon: the larger of the signal's widest declared
    window and the broker's metrics retention.

    Keeping the broker's whole retention locally lets a replay start from the
    earliest buffered point without reading the stream again. Unresolved
    inputs are skipped; nothing lands in the buffer for them yet.
    """
    horizons: dict[str, float] = {}
    for instance in instances:
        for attr_name, input_attr in declared_inputs(instance):
            if time_domain is not None and input_attr.time_domain != time_domain:
                continue
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


def trim_buffer(buffer: Buffer, instances: list[Producer], retention_s: float, clock=None) -> None:
    """Periodic job: drop buffered points older than each signal's horizon.

    Horizons are recomputed from the currently resolved inputs on every run,
    so a signal that resolves late is trimmed too; resolved ids are held in
    memory, so this needs no KV scan. A plain function, so APScheduler runs
    the sqlite work off the loop."""
    if clock is None:
        deleted = buffer.trim(compute_trim_horizons(instances, retention_s))
    else:
        application_now = clock.now()
        definition = getattr(clock, "definition", None)
        if definition is not None:
            # Keep the input window needed by the slowest timer, including a
            # newly started timer. Requested speed must never prune its backlog.
            start = definition.start_at if definition.start_at is not None else definition.factory_anchor
            for instance in instances:
                for method, spec in type(instance)._triggers:
                    if isinstance(spec, CronSpec | IntervalSpec):
                        progress = buffer.watermark(timer_key(instance, method, spec))
                        application_now = min(application_now, progress if progress is not None else start)
        deleted = buffer.trim(
            compute_trim_horizons(instances, retention_s, time_domain="application"), now=application_now
        )
        real_now = clock.real_now() if hasattr(clock, "real_now") else time.time()
        deleted += buffer.trim(compute_trim_horizons(instances, retention_s, time_domain="real"), now=real_now)
    if deleted:
        log.info("Buffer trim: deleted %d point(s) past their per-signal horizon", deleted)
    else:
        log.debug("Buffer trim: nothing past its per-signal horizon")


def make_handler(method, *, time_domain="application"):
    """Adapt a producer's ``@on_metric`` method (``async def f(self, metric)``)
    into an Ingest handler (``async def h(record)``) that decodes the payload
    into a ``Metric``. Holds the producer's lock, like :func:`off_loop`."""

    async def _handler(record: Record) -> None:
        metric = decode_metric(record)
        clock = getattr(method.__self__.runtime, "clock", None)
        instant = (
            clock.at(float(metric.timestamp))
            if clock is not None and time_domain == "application"
            else contextlib.nullcontext()
        )
        instance = method.__self__
        with instance._lock, instant:
            checkpointed = instance.state_version is not None and record.offset >= 0
            key = method.__name__
            if checkpointed and record.offset <= instance._handled_offsets.get(key, -1):
                return
            previous = state_copy(instance) if checkpointed else None
            try:
                await method(metric)
                if checkpointed:
                    offsets = dict(instance._handled_offsets)
                    instance._handled_offsets[key] = record.offset
                    try:
                        save_checkpoint(instance)
                    except BaseException:
                        instance._handled_offsets = offsets
                        raise
            except BaseException:
                if checkpointed:
                    instance.restore_state(previous)
                raise

    _handler.consumer = f"{method.__self__.name}.{method.__name__}"
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

    A handler that raises :class:`chaski.Reject` has the record recorded as
    rejected (the runtime's ``reject``), as live ingest does, and the replay
    goes on. Any other failure stops recovery: the producer's watermark and
    code hash are not marked complete, a checkpointed producer's state is put
    back, and the exception propagates, so the caller retries the replay.
    Handlers must make repeated effects idempotent.
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
        restored = restore_checkpoint(instance)
        if old_hash == new_hash and (instance.state_version is None or restored):
            continue

        mini_dispatch, mini_signal_ids, _ = build_dispatch(runtime, [instance])
        earliest = _earliest_across(buffer, mini_signal_ids)
        now = runtime_now(runtime)
        if getattr(runtime, "step", None) is not None:
            application_inputs = set()
            for _, input_attr in declared_inputs(instance):
                if input_attr.time_domain != "real":
                    # Unresolved inputs have no buffered records.
                    with contextlib.suppress(LookupError):
                        application_inputs.add(input_attr.signal_id)
            pending_start = buffer.pending_input_start(application_inputs)
            if pending_start is not None:
                # Reconstruct only the prefix before pending work. Otherwise a
                # crash/code reload can recreate future producer state and then
                # replay older inbox records against it (backwards intervals).
                now = min(now, pending_start)
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
            rows.extend((ts, signal_id, value) for ts, value in buffer.points(signal_id, start, now))
        rows.sort(key=lambda r: r[0])

        previous = state_copy(instance) if instance.state_version is not None else None
        try:
            for ts, signal_id, value in rows:
                record = synthetic_record(signal_id, ts, value)
                for handler in mini_dispatch.get(signal_id, []):
                    try:
                        await handler(record)
                    except Reject as rejected:
                        subject = {"replay": instance.name, "signal_id": signal_id, "ts": ts}
                        await asyncio.to_thread(record_rejection, runtime, consumer_name(handler), subject, rejected)
        except BaseException:
            if previous is not None:
                instance.restore_state(previous)
            raise

        final_position = rows[-1][0] if rows else start
        save_checkpoint(instance)
        buffer.set_watermark(instance.name, final_position, new_hash)
        log.info(
            "%s: replay complete (%d record(s)) — watermark=%.3f",
            instance.name,
            len(rows),
            final_position,
        )


def _earliest_across(buffer: Buffer, signal_ids: Iterable[str]) -> float | None:
    """``MIN`` of :meth:`Buffer.earliest` across several signals, or
    ``None`` when none of them holds anything yet."""
    values = [e for e in (buffer.earliest(sid) for sid in signal_ids) if e is not None]
    return min(values) if values else None


# ─── MQTT dispatch ──────────────────────────────────────────────────────────


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
    :class:`~chaski.dataops.inputs.Historian`; ``retry_min``/
    ``trim_interval`` the failure backoff minimum and buffer-trim cadence;
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
        retry_min: float = 1.0,
        trim_interval: float = 3600.0,
        health_port: int = health.PORT_DEFAULT,
        **service_kw: Any,
    ) -> None:
        super().__init__(name, mount, node=node, **service_kw)
        self._data_dir = Path(data_dir) if data_dir is not None else self._state_dir
        self.retention_s = float(retention) if retention is not None else DEFAULT_RETENTION_S
        self._historian = historian
        self._retry_min_s = retry_min
        self._trim_interval_s = trim_interval
        self._health_port = health_port
        self._producers: dict[str, type[Producer]] = {}
        self._local_buffer: Buffer | None = None
        self._ingest: Ingest | None = None
        self._wake_topics: set[str] = set()
        self._wake_pending = False
        self._wake_failures = 0
        from ..retry import Backoff

        self._wake_backoff = Backoff(minimum=min(WAKE_RETRY_S, WAKE_RETRY_MAX_S), maximum=WAKE_RETRY_MAX_S)
        self._wake_retry: asyncio.TimerHandle | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._commands: commands.CommandExecutor | None = None
        self.instances: list[Producer] = []
        self._step_loop_last = time.monotonic()
        self._step_waiting = False
        self._definition_cache: resolve.LiveIndex | None = None

    def _after_connect(self) -> None:
        super()._after_connect()
        self._definition_cache = resolve.DefinitionCache(
            self.retained_view(
                contracts=("_Signal", "_SystemElement", "_AnnotationType"),
                streams=("entities", "definitions"),
                cursor="dataops-definitions",
                # Resolution reaches any path a producer names.
                scope=ViewScope.whole_node(),
                on_change=self.clock.changes.notify,
            )
        )
        cast(Any, self.door)._dataops_definitions = self._definition_cache

    def _placement_reannounced(self) -> None:
        super()._placement_reannounced()
        for topic in tuple(self._wake_topics):
            self._started_client.subscribe(topic, qos=1)

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
        """The door every input resolves through — the base class's own,
        open after :meth:`start`. Outputs write with :meth:`send`."""
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
        buffer. Idempotent. Every ``@on_command`` of the added producers is
        announced (:meth:`~chaski.Service.announce_commands`) before the
        service connects, so its last will carries them too."""
        declared = commands.declared_routes(self._producers.values())
        if declared:
            self.announce_commands([*self._announced_commands, *declared])
        super().start(connect_timeout=connect_timeout)
        if self._local_buffer is None:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._local_buffer = Buffer(self._data_dir / "buffer.sqlite3")
        return self

    def close(self) -> None:
        self._cancel_wake_retry()
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
        node_id = self._node_id
        if node_id is None:
            raise RuntimeError("chaski.DataOpsService: call start() before bind_outputs()")
        with self._lock:
            result = build_catalogue(instances, self._started_catalogue)
            prepared = self._catalogue_to_publish()
        # Outside the lock — see _publish_outside_the_lock.
        if prepared is not None:
            self._publish_catalogue(prepared)
        bind_annotation_outputs(instances, node_id=node_id, mount=self._mount)
        return result

    def _seal_catalogue(self) -> None:
        """The declared outputs decide what is stale (:func:`build_catalogue`);
        a shutdown changes nothing, so the run's catalogue stays the node's."""

    def _open_ingest_stream(self, cursor: str, signal_ids: list[str] | None) -> Stream:
        self._wake_on_inputs(signal_ids or [])
        return self.stream("metrics", cursor=cursor, signal_ids=signal_ids)

    def _wake_on_inputs(self, signal_ids: list[str]) -> None:
        """Subscribe the ``_Metric`` topic of each input signal, and drop the
        ones no longer read. Called whenever the ingest stream (re)opens, so a
        late-resolved input starts waking the ingest from then on.

        QoS 1 on the service's persistent session, so a wake is not dropped on
        the way. Nothing reads on a timer behind it: a reconnect wakes the
        ingest, and a consumer that stops reading anyway is reported by the
        node's cursor watchdog.

        The topics come from ``/kv``, which the node rate-limits. A transport
        error there (429, 5xx, a timeout) keeps the topics already
        subscribed and retries with backoff, or sooner if the inputs rebind.
        The ingest is woken once the retry subscribed, for what arrived before.
        """
        client = self._client
        if client is None:
            return
        self._cancel_wake_retry()
        try:
            topics = set(resolve.resolve_metric_topics(self.door, signal_ids).values())
        except httpx.HTTPError as exc:
            self._wake_failures += 1
            delay = self._wake_backoff.delay(exc)
            log.warning(
                "Could not read the input topics to wake on (attempt %d): %s — keeping %d topic(s), retrying in %.0fs",
                self._wake_failures,
                exc,
                len(self._wake_topics),
                delay,
            )
            if self._loop is not None:
                self._wake_retry = self._loop.call_later(delay, self._wake_on_inputs, list(signal_ids))
            return
        self._wake_failures = 0
        self._wake_backoff.reset()
        if len(topics) < len(signal_ids):
            log.debug("%d input signal(s) have no known topic yet", len(signal_ids) - len(topics))
        added = sorted(topics - self._wake_topics)
        for topic in added:
            client.message_callback_add(topic, self._on_input_metric)
            client.subscribe(topic, qos=1)
        for topic in sorted(self._wake_topics - topics):
            client.message_callback_remove(topic)
            client.unsubscribe(topic)
        self._wake_topics = topics
        log.info("ingest wakes on %d input topic(s)", len(topics))
        if added and self._ingest is not None:
            # Records may have arrived before the subscription existed.
            self._ingest.wake()

    def _cancel_wake_retry(self) -> None:
        if self._wake_retry is not None:
            self._wake_retry.cancel()
            self._wake_retry = None

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
            health=self.handler_health,
            reject=self.reject,
        )

    def _execute_commands(self, instances: list[Producer]) -> commands.CommandExecutor | None:
        """Build the executor for every ``@on_command`` declared across
        ``instances`` — see :mod:`chaski.dataops.commands`. ``None``, with no
        stream watch and no cursor, when nothing declared one."""
        handlers = commands.gather(instances)
        if not handlers:
            return None
        node_id = cast(str, self._node_id)
        executor = commands.CommandExecutor(
            self.door,
            self.send,
            self.stream(
                commands.STREAM,
                cursor=commands.CURSOR,
                contracts=commands.stream_contracts(handlers),
                topics=commands.stream_topics(handlers, node_id),
            ),
            handlers,
            node_id,
            ledger=self.buffer,
            health=self.handler_health,
        )
        if not self.is_broker_connected():
            executor.link_changed(False)
        return executor

    def _broker_state_changed(self, connected: bool) -> None:
        """Back on the broker: commands and input metrics sent while the link
        was down only rang a bell nobody heard, so drain both once."""
        super()._broker_state_changed(connected)
        loop, executor = self._loop, self._commands
        if connected and loop is not None and self._ingest is not None:
            loop.call_soon_threadsafe(self._ingest.wake)
        if loop is not None and executor is not None:
            loop.call_soon_threadsafe(executor.link_changed, connected)

    async def serve(self, stop: asyncio.Event | None = None) -> None:
        """Run the service on the current event loop until ``stop`` is set
        — see the module docstring for the startup order. :meth:`run` is
        the blocking wrapper with signal handling."""
        stop = stop or asyncio.Event()
        loop = asyncio.get_running_loop()
        self._loop = loop

        # The infrastructure stays live while a fresh deployment waits for its
        # first timeline/beacon. Producer setup may read application time, so it
        # must not run in a different clock domain or be skipped at startup.
        health_state = health.HealthState(
            ready=False,
            broker_connected=self.is_broker_connected,
            cursor_lag=lambda: self.cursor_lag,
            identity_conflict=lambda: self.identity_conflict,
        )
        health_server = None
        try:
            health_server = await health.serve(health_state, port=self._health_port)
            from ..retry import Backoff

            startup_backoff = Backoff()
            while not stop.is_set():
                try:
                    await asyncio.to_thread(self.start)
                    health_state.generation = self.buffer.generation
                    break
                except (httpx.HTTPError, ConnectionError, TimeoutError) as exc:
                    log.warning("DataOps startup waiting for Colca (%s)", type(exc).__name__)
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), startup_backoff.delay(exc))
            while not stop.is_set():
                version = self.clock.changes.version
                if self.clock.status().ready:
                    break
                await self.clock.changes.wait_async(version, stop=stop)
            if stop.is_set():
                return
            if self._definition_cache is None:
                raise RuntimeError("DataOps definition subscription was not initialized")
            while not stop.is_set():
                version = self._definition_cache.changes.version
                if self._definition_cache.view.available:
                    break
                await self._definition_cache.changes.wait_async(version, stop=stop)
            if stop.is_set():
                return
            health_state.ready = True
            producers = self.producers
            if not producers:
                log.warning("No producers added to %s — add() or discover() some before serve().", self.name)
            else:
                log.info("Running %d producer(s): %s", len(producers), [p.name for p in producers])

            # Every configured producer must initialize before intake can advance.
            instances: list[Producer] = []
            for instance in self.instantiate():
                await instance.setup()
                instances.append(instance)
            self.instances = instances

            # 3) Catalogue the SignalOutputs and bind the AnnotationOutputs,
            #    after opening the live resolution index so the bindings the
            #    node writes for the catalogue arrive on it.
            self.bind_outputs(instances)
            # Catalogue publication commits new Signal bindings. Synchronize
            # once at this causal boundary, before on_ready or durable ingest;
            # ordinary per-signal lookups thereafter use the pushed cache.
            await asyncio.to_thread(self._definition_cache.view.synchronize)

            # 4) Outputs are bound and no trigger has fired yet: producers do
            #    startup compute here instead of retrying publish() on the
            #    RuntimeError it raises before binding.
            for instance in instances:
                await instance.on_ready()

            # 5) Refuse windows longer than the broker's retention without a historian.
            validate_windows(
                instances,
                retention_s=self.retention_s,
                historian_configured=self._historian is not None,
            )

            # 6) The on_metric dispatch table and the declared input signal ids.
            dispatch, signal_ids, unresolved = build_dispatch(self, instances)

            # 7) Replay producers whose code changed. A failure is counted in
            #    handler_health and the replay retried; the health door stays up.
            health_state.handlers = self.handler_health
            await supervise("replay", lambda: replay_changed_producers(self, instances), stop, self.handler_health)
            if stop.is_set():
                return

            # 8) Retire the previous generation's cursor (if any) and build the
            #    ingest loop over this service's own consume lane.
            ingest = Ingest(
                self._open_ingest_stream,
                self.buffer,
                dispatch=dispatch,
                signal_ids=signal_ids or None,
                retry_min_s=self._retry_min_s,
                health=self.handler_health,
                reject=self.reject,
                # A lost buffer's generation cannot be recovered, so nothing knows
                # the previous cursor yet and retiring it is a no-op.
                previous_generation=None,
            )
            ingest.retire_previous_generation()
            self._ingest = ingest

            # 9) Start ingest in the background and keep retrying unresolved inputs.
            health_state.producers = len(instances)
            health_state.last_drain_at = (
                (lambda: self._step_loop_last) if self.step is not None else (lambda: ingest.last_drain_at)
            )
            health_state.stall_after_s = health.STALL_AFTER_MIN_S
            health_state.waiting = (lambda: self._step_waiting) if self.step is not None else (lambda: ingest.waiting)
            health_state.connected = self.is_broker_connected
            ingest_task: asyncio.Task | None = None
            tasks_health = self.handler_health

            def supervised(name: str, start: Callable[[], Awaitable[Any]]) -> asyncio.Task:
                return asyncio.ensure_future(supervise(name, start, stop, tasks_health))

            if self.step is not None:
                ingest_task = supervised("step loop", lambda: self._run_steps(instances, ingest, stop))
                health_state.ingest_task = ingest_task
            elif signal_ids:
                ingest_task = supervised("ingest", lambda: ingest.run_forever(stop))
                health_state.ingest_task = ingest_task
                log.info("Ingest loop started: cursor=%s signal_ids=%d", ingest.cursor, len(signal_ids))
            else:
                log.warning("No producer declared a resolved input — ingest loop not started (nothing to fetch yet).")

            def _ensure_ingest_running() -> None:
                """Start the loop if nothing resolved at startup and something has now."""
                nonlocal ingest_task
                if ingest_task is None:
                    ingest_task = supervised("ingest", lambda: ingest.run_forever(stop))
                    health_state.ingest_task = ingest_task
                    log.info("Ingest loop started after a late resolve: cursor=%s", ingest.cursor)

            reresolve_task: asyncio.Task | None = None
            if self.step is None and (unresolved or self._definition_cache is not None):
                log.info("%d declared input(s) unresolved — retrying until they are commissioned.", unresolved)
                reresolve_task = supervised(
                    "re-resolve", lambda: reresolve_loop(self, instances, ingest, stop, _ensure_ingest_running)
                )
                if unresolved:
                    log.info(
                        "%d declared input(s) unresolved — resolving again when they are commissioned.", unresolved
                    )

            # 10) Every @on_constant/@on_signal subscription, and the @on_command
            #     executor.
            self._watch_constants_and_signals(instances)
            self._commands = self._execute_commands(instances)
            commands_task: asyncio.Task | None = None
            executor = self._commands
            if executor is not None:
                commands_task = supervised("command executor", lambda: executor.run_forever(stop))

            # 11) Schedule cron/interval triggers (everything except @on_metric),
            #     plus the service's own periodic buffer trim.
            scheduler = AsyncIOScheduler()
            factory_tasks = []
            for instance in instances:
                if self.step is not None:
                    continue
                if self.clock.definition_topic:
                    for method_name, spec in instance.__class__._triggers:
                        if isinstance(spec, CronSpec | IntervalSpec):
                            factory_tasks.append(
                                supervised(
                                    f"{instance.name}.{method_name}",
                                    functools.partial(run_periodic, instance, method_name, spec, self.clock),
                                )
                            )
                else:
                    schedule_periodic(scheduler, instance)
            scheduler.add_job(
                trim_buffer,
                args=(self.buffer, instances, self.retention_s, self.clock),
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

            try:
                await stop.wait()
            finally:
                log.info("Shutting down...")
                scheduler.shutdown(wait=False)
                for task in factory_tasks:
                    task.cancel()
                await asyncio.gather(*factory_tasks, return_exceptions=True)
                if reresolve_task is not None:
                    # `stop` is already set, so the loop returns on its next wait;
                    # cancel covers the case where it is mid-KV-read.
                    reresolve_task.cancel()
                if commands_task is not None:
                    try:
                        await asyncio.wait_for(commands_task, timeout=10.0)
                    except Exception:
                        log.exception("Command executor did not shut down cleanly")
                    self._commands = None
                if ingest_task is not None:
                    if self.step is not None:
                        ingest_task.cancel()
                    ingest.wake()
                    try:
                        await asyncio.wait_for(ingest_task, timeout=10.0)
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        log.exception("Ingest loop did not shut down cleanly")
                for instance in instances:
                    try:
                        await instance.teardown()
                    except Exception:
                        log.exception("Error during teardown of %s", instance.name)
                self._ingest = None

        finally:
            if health_server is not None:
                health_server.close()
                await health_server.wait_closed()
            self.close()

    async def _run_steps(self, instances, ingest, stop):
        if self.step is None:
            return
        from ..retry import Backoff

        backoff = Backoff()
        active_dispatch = None
        while not stop.is_set():
            version = self.clock.changes.version
            self._step_waiting = False
            try:
                target = await asyncio.to_thread(self.step.ready)
                if target is not None:
                    dispatch, signal_ids, unresolved = await asyncio.to_thread(build_dispatch, self, instances)
                    if not unresolved:
                        if dispatch is not active_dispatch:
                            ingest.rebind(dispatch, signal_ids or None)
                            active_dispatch = dispatch
                        real_signals = {
                            input_attr.signal_id
                            for instance in instances
                            for _, input_attr in declared_inputs(instance)
                            if input_attr.time_domain == "real"
                        }

                        async def before_sample(at):
                            await run_due(instances, self.clock, at, inclusive=False)

                        async def finish_window(at):
                            await run_due(instances, self.clock, at, inclusive=True)

                        if signal_ids:
                            await ingest.run_window(
                                target, before_sample, finish_window, real_signals=real_signals, clock=self.clock
                            )
                        else:
                            await finish_window(target)
                        await asyncio.to_thread(self.step.complete, target)
            except Exception as exc:
                failed = ingest.take_failure()
                name = failed[0] if failed is not None else "coordinated window"
                count = self.handler_health.failed(name, exc)
                delay = backoff.delay(exc)
                log.error(
                    "Coordinated window failed in %s (%d in a row); progress not acknowledged, retrying in %.1fs",
                    name,
                    count,
                    delay,
                    exc_info=exc,
                )
                await ingest._sleep_or_stop(delay, stop)
                continue
            self.handler_health.succeeded("coordinated window")
            backoff.reset()
            self._step_loop_last = time.monotonic()
            self._step_waiting = True
            await self.clock.changes.wait_async(version, self.step.wait_delay())

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
