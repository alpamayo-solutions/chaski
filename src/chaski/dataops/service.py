"""``chaski.DataOpsService``: a :class:`chaski.Service` that runs producers.

A service family is a base class offering a lane, never a kind the node
knows about (service families design §3.1, architecture
principle 6): a ``DataOpsService`` registers, publishes ``_ServiceDetails``
and opens the door exactly as a bare ``Service`` does — the node sees one
more local service — and adds only the evaluator runtime.

Where each of its invariants lives:

* **One ingest lane** (§3) — :class:`~chaski.dataops.ingest.Ingest`, over
  ``self.stream("metrics", cursor="ingest-<generation>", signal_ids=...)``:
  the SDK's own consume lane, a generational named cursor, ack after
  process. There is no second polling or direct-DB path.
* **MQTT is a doorbell only** — :meth:`DataOpsService._ring_doorbell`
  subscribes ``<root>/v1/_Metric/#`` at qos 0 on the base class's own MQTT
  client for exactly one purpose: waking the ingest loop instead of waiting
  out the poll interval. The payload is never read; reading it would open a
  second lane into data the ingest loop already owns. The same connection
  carries this service's retained ``_ServiceDetails`` and its log, as it
  does for every ``Service``.
* **The buffer is the only local state** (§3) —
  :class:`~chaski.dataops.buffer.Buffer`, one SQLite file under
  ``data_dir``; every declared input window and every producer's watermark
  read and write through it, and :func:`trim_buffer` prunes it on its own
  interval.
* **The historian is optional and read-only** (§7) — the ``historian=``
  argument, a :class:`~chaski.dataops.inputs.Historian` port or ``None``
  (the default). The SDK imports no database driver; a deployment
  implements the port, for example over TimescaleDB, and passes it in.
* **Outputs are catalogue-provisioned** (§5) —
  :func:`~chaski.dataops.outputs.build_catalogue`, a ``_DataTags`` record
  like a connector's, commissioned by ``signal/autobind``; annotations are
  broker-native ``_Annotation`` records (§8).
* **A code change replays** (§10) — :func:`replay_changed_producers`,
  keyed by :func:`~chaski.dataops.codehash.compute_code_hash`.

Startup order inside :meth:`DataOpsService.serve`:

 1. ``start()`` — the base class connects, self-registers and opens the
    door; then the buffer is opened (mints/loads its generation)
 2. instantiate every added producer, attach it to this runtime and run its
    ``setup()`` (best-effort — a producer whose ``setup()`` raises is
    skipped with a logged reason, not fatal)
 3. build + publish (iff changed) the ``SignalOutput`` catalogue and bind
    every declared output to its resolved tag id, and bind every declared
    ``AnnotationOutput`` (design §5, §8) — every output publishes through
    the door from here on, never MQTT
 4. validate every declared input's window against the broker's metrics
    retention (a startup error when it exceeds it with no historian
    configured — design §4.1)
 5. build the ``signal_id -> [handler]`` dispatch table from every
    producer's ``@on_metric`` declarations, and the union of every
    declared input's signal id
 6. hash-triggered broker-window replay (design §10): for every producer
    whose code hash differs from what is stored — including a brand-new
    producer, which has none stored yet — reset its watermark to the
    earliest point the buffer holds for its own declared inputs and replay
    every buffered record for its ``@on_metric``-dispatched signals, in
    order, through the SAME handlers live traffic uses. A producer whose
    hash is UNCHANGED is untouched: no reset, no replay
 7. retire the previous generation's ingest cursor, if any is known
 8. start :class:`~chaski.dataops.ingest.Ingest` as a background task, and
    a retry loop for the inputs that did not resolve yet
 9. ring the MQTT doorbell
 10. schedule cron/interval ticks, plus one periodic job that trims the
     buffer back to each signal's horizon (design §3)
 11. open the health door and block until ``stop``; then unwind everything
     in reverse and ``close()``
"""

from __future__ import annotations

import asyncio
import functools
import importlib
import logging
import pkgutil
import signal
import sys
import time
from dataclasses import fields
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from colca_data_contracts import Metric, topic_prefix

from chaski.door import Door, Record, Stream
from chaski.service import Service

from . import codehash, health, resolve
from .base import Producer, Runtime
from .buffer import Buffer
from .ingest import Ingest
from .inputs import Historian, declared_inputs, validate_windows
from .outputs import bind_annotation_outputs, build_catalogue, declared_outputs
from .triggers import CronSpec, IntervalSpec, OnMetricSpec

log = logging.getLogger("chaski.dataops")

DOORBELL_WILDCARD = f"{topic_prefix()}_Metric/#"
#: colca's default metrics retention — what a declared window is checked
#: against when the service is given no ``retention=`` of its own.
DEFAULT_RETENTION_S = 14 * 24 * 3600.0
_METRIC_FIELDS = {f.name for f in fields(Metric)}


# ─── discovery ──────────────────────────────────────────────────────────────


def import_package(package: str) -> list[str]:
    """Import ``package`` and every submodule under it, so each module's
    ``Producer`` subclasses register via ``__init_subclass__``. Returns the
    imported module names; a package that does not exist is a WARNING and
    an empty list, not an error — the shipped service names optional
    packages through an env var."""
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


def import_directory(path: Path) -> list[str]:
    """Import every top-level ``*.py`` under ``path`` so its ``Producer``
    subclasses register via ``__init_subclass__``. Returns the imported
    module names (the file stems).

    The directory is put on ``sys.path`` and each file is imported by its
    plain module name (``importlib.import_module(stem)``). That single flat
    namespace lets the modules import each other directly — e.g. a
    producer doing ``from machine_base import MachineBase`` reuses the
    exact same module object the loader imported, with no duplicate
    execution. Code that only defines shared base classes / helpers (ABCs,
    compute math) imports cleanly too; being abstract, the ABCs simply
    register nothing.

    Files starting with ``_`` are skipped; subdirectories (e.g. ``tests/``)
    are ignored. A module that fails to import is logged and skipped, never
    fatal — one broken customer file must not take every other producer
    down with it.
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
    """A producer tick as a job the scheduler runs OFF the event loop.

    A producer's ``@every``/``@cron`` method is a coroutine function by
    contract, but its body is synchronous — buffer reads (sqlite), output
    publishes (httpx). Handed to ``AsyncIOScheduler`` as a coroutine it runs
    ON the loop, and every one of those calls blocks every timer in the
    process: the measured cost was a 15-second schedule firing every ~30
    seconds. Handed a plain function instead, APScheduler runs it in its own
    thread pool — so this wraps the coroutine in ``asyncio.run`` on a fresh,
    private loop, in that worker thread. Ticks fire on time and in parallel;
    ``max_instances=1`` still stops a slow tick from overlapping itself.

    Also takes the producer's own lock for the duration of the tick (design
    §4: "handlers and ticks on one producer serialize on the producer's own
    lock") — a tick runs on APScheduler's executor thread while an
    ``@on_metric`` handler for the SAME producer runs on the ingest worker
    thread (see :func:`make_handler`), two real OS threads that can race on
    shared producer state (e.g. ``MachineState._last_emitted``) with no lock.
    """

    @functools.wraps(method)
    def job() -> None:
        with method.__self__._lock:
            asyncio.run(method())

    return job


def schedule_periodic(scheduler: AsyncIOScheduler, instance: Producer) -> int:
    """Wire each (method, cron-or-interval) pair onto the scheduler.

    ``OnMetricSpec`` triggers are NOT scheduled here — they live on the
    ingest dispatch table built by :func:`build_dispatch`. Every job is
    wrapped by :func:`off_loop`: the loop keeps the timers, the executor
    threads do the work.
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
        elif isinstance(spec, OnMetricSpec):
            continue  # handled by the ingest dispatch table
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

    Every `input.signal_id` inside scans KV on its own — twice, for the
    element and then the signal — so a node with thirty declared inputs asked
    colca for sixty full snapshots each time this ran. colca serves `/kv` at
    five a second (it is a SCAN class), refused the rest with HTTP 429, and
    this function reported each refusal as an input that could not be
    resolved and dropped it from dispatch. `reresolve_loop` then retried and
    failed the same way, forever.

    Pinning one read for the pass is the whole fix. Nothing is cached between
    passes: the next call reads KV fresh, so `resolve`'s rule that a rebound
    signal is picked up on the very next call still holds.

    A pass whose own pinned KV read FAILS (a `/kv` 429, colca restarting
    mid-scan) must never look like "every signal vanished". `forget_resolved`
    — the step that makes a real rebind or removal visible — only runs when
    THIS pass actually pinned a fresh snapshot. On a failed pin every input
    keeps whatever id its last successful pass resolved (a plain in-memory
    read, no further KV call), and only inputs that have never yet resolved
    are attempted — and fail exactly as they did before pinning existed.
    Without this, `forget_resolved` ran unconditionally, so one refused scan
    mid-`reresolve_loop` wiped every already-resolved input's cached id;
    each then tried to re-resolve on its own against a door still refusing
    requests, narrowed the fetch filter below what was actually bound, and
    `ingest.rebind` dropped already-flowing signals from the buffer while the
    cursor kept acking — silently, not logged as unresolved.
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
    """Drop every held id so this pass resolves them afresh.

    Inputs and outputs both hold what they resolved — that is what keeps the
    ingest and publish paths off colca's KV door, which serves five scans a
    second. This is the one place their cadence is set, so both are forgotten
    together and neither can quietly diverge from the other. Called only when
    `build_dispatch` actually pinned a fresh KV read for this pass — see its
    docstring.
    """
    for instance in instances:
        for _name, declared_input in declared_inputs(instance):
            declared_input.forget()
        for _name, declared_output in declared_outputs(instance):
            declared_output.forget()


def _resolve_dispatch(
    instances: list[Producer],
) -> tuple[dict[str, list], list[str], int]:
    """Walk every producer's declared inputs and ``@on_metric`` triggers,
    and return ``(dispatch, signal_ids, unresolved)``.

    ``dispatch`` maps ``signal_id -> [async handler(record), ...]``, ready
    to hand straight to :class:`~chaski.dataops.ingest.Ingest`. It is built
    ONLY from ``@on_metric`` declarations — a ``@every``/``@cron`` tick
    never gets a dispatch entry, it reads its inputs on its own schedule
    instead.

    ``unresolved`` counts the declared inputs that could not be resolved on
    this pass. It is what :func:`reresolve_loop` watches: a producer
    commissioned before its signals exist would otherwise stay excluded from
    dispatch for the life of the process and degrade silently to its timers.

    ``signal_ids`` is the deduplicated union of every declared
    ``SignalRangeInput`` on every producer — sent with the fetch so the door
    filters the stream server-side, the buffer only ever holds points some
    producer actually declared. Critically this is NOT the same set as
    ``dispatch``'s keys: a producer driven purely by ``@every``/``@cron``
    (no ``@on_metric`` at all) still needs its declared input's points
    landing in the buffer, or its window reads come back empty (design §3,
    §4.1). Discovery uses :func:`~chaski.dataops.inputs.declared_inputs` —
    the same enumeration :func:`validate_windows` uses — so a ticking-only
    input is counted here exactly like a windowed one is counted there.

    A signal genuinely absent from KV (not yet bound, or a typo in the
    declared name) is logged at WARNING and excluded from both the fetch
    filter and the dispatch table — startup is not aborted, so one
    unresolved input doesn't take every other producer down with it.
    Whatever fails here is retried by :func:`reresolve_loop` until it
    resolves — a signal is commissioned by a separate act (`signal/autobind`,
    or an editor), so a producer routinely starts
    before the tree it reads exists.
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

    Resolution is a KV read (`chaski.dataops.resolve`), and that module's own
    rule is that nothing caches a resolved id — "a signal that moves or is
    rebound is picked up on the very next call". The dispatch table was the
    one place that did cache, and it cached the WORST possible moment:
    startup, before any signal had been
    commissioned. A producer that lost that race kept an empty table for the
    life of the process and fell back to its timers, so its outputs tracked
    its inputs at the timer's cadence instead of the data's.

    Stops as soon as everything resolves — this is a startup race, not a
    steady-state poll, and a resolved input never becomes unresolved.
    """
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
            return  # stop was set
        except asyncio.TimeoutError:
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
    """Per-signal trim horizon (design §3): ``max(the largest declared
    window on it, the broker's own metrics retention)``.

    Keeping at least the broker's full retained window locally is what lets
    the hash-triggered replay (§10) reset a producer to "the earliest point
    the buffer holds" without ever needing to re-read the stream — trimming
    any tighter than that would silently shrink what a future replay can
    see. An input that fails to resolve is skipped exactly like it is at
    dispatch time — unresolved means nothing is landing in the buffer for
    it yet, so there is nothing to trim.
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
    """Periodic job (design §3: "the service's own duty, a periodic
    delete"): drop buffered points older than each signal's horizon.

    Horizons are recomputed from the CURRENTLY resolved inputs on every
    run, not once at startup. They used to be computed once, before
    `reresolve_loop` had resolved anything (a producer routinely starts
    before its signals are commissioned), so a
    signal that resolved later never appeared in the horizons dict —
    `Buffer.trim` keeps every point for a signal absent from it — and grew
    without bound for the life of the volume. Recomputing here costs
    nothing extra for an already-resolved input: `SignalRangeInput.signal_id`
    caches per instance until a resolution pass forgets it, so this is a
    plain in-memory read, not a fresh KV scan, for anything already bound.

    Plain function on purpose: sqlite work has no business on the event
    loop, and APScheduler runs a non-coroutine job in its executor."""
    horizons = compute_trim_horizons(instances, retention_s)
    deleted = buffer.trim(horizons)
    if deleted:
        log.info("Buffer trim: deleted %d point(s) past their per-signal horizon", deleted)
    else:
        log.debug("Buffer trim: nothing past its per-signal horizon")


def make_handler(method):
    """Adapt a producer's ``@on_metric`` method (``async def f(self, metric)``)
    into an Ingest handler (``async def h(record)``) by decoding the
    record's already-JSON-decoded payload into a ``Metric``.

    Takes the producer's own lock for the duration of the call — see
    :func:`off_loop`'s docstring for why: this handler runs on the ingest
    worker thread, a tick for the SAME producer runs on APScheduler's
    executor thread, and nothing else serializes them onto the producer's
    shared state."""

    async def _handler(record: Record) -> None:
        metric = decode_metric(record)
        with method.__self__._lock:
            await method(metric)

    return _handler


def decode_metric(record: Record) -> Metric:
    payload = record.payload if isinstance(record.payload, dict) else {}
    fields_only = {k: v for k, v in payload.items() if k in _METRIC_FIELDS}
    # record.ts is colca's own record timestamp, unix MILLISECONDS — every
    # other timestamp here (and everything the handler goes on to do with
    # it) is unix seconds. record.fallback_timestamp_s is the one place
    # that conversion lives; Ingest._timestamp_of is the other consumer.
    fields_only.setdefault("timestamp", record.fallback_timestamp_s)
    return Metric(**fields_only)


def synthetic_record(signal_id: str, ts: float, value: Any, *, actor: str = "replay") -> Record:
    """A ``Record`` shaped like one a fetch would return, built from a
    BUFFERED (or historised) point rather than a live fetch — used only to
    feed the exact same :func:`make_handler`-wrapped dispatch entries live
    traffic uses, so a replayed point is decoded and handled identically to
    a live one. The non-payload bookkeeping fields (offset, topic, actor_*)
    carry no meaning here; ``written_by``/``actor_kind`` name the replaying
    ``actor`` purely so a handler that happens to log ``record.written_by``
    reads something honest rather than an empty string.
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


# ─── hash-triggered broker-window replay (design §10, requirement #5) ─────


async def replay_changed_producers(runtime: Runtime, instances: list[Producer]) -> None:
    """Per producer: if its code hash differs from what is stored for it —
    including "never stored", a brand-new producer — reset its watermark to
    the earliest point the buffer holds across its own declared inputs and
    replay every buffered record for its ``@on_metric``-dispatched signals,
    in timestamp order, through the SAME dispatch handlers live traffic
    uses (:func:`build_dispatch`, called here scoped to just this one
    producer so its replay never touches another producer's signals).

    Publishes made during replay overwrite exactly like a live publish does
    (design §6) — replaying a point that was already correctly computed is
    harmless. A producer whose hash is UNCHANGED since the last run is
    skipped entirely: no watermark write, no replay — this is what makes
    the reset a one-time event per real change rather than something that
    re-fires on every restart with the same code.

    A tick-only producer (no ``@on_metric`` at all) still gets its watermark
    reset and hash persisted on a genuine change — there is simply nothing
    for :func:`build_dispatch` to hand back to replay, since a tick reads
    the buffer fresh on its own schedule rather than being fed one record
    at a time.

    **A handler that raises is survived, exactly like live traffic.**
    :meth:`~chaski.dataops.ingest.Ingest._process_record` never lets one
    broken ``@on_metric`` handler take the ingest loop down — it logs and
    moves on, then acks the page regardless. Replay matches that: a raising
    handler is logged (``log.exception``) and skipped, replay continues with
    the next handler/record, and the watermark + code hash are still
    persisted once the pass finishes — the same "advance regardless of a
    handler failure" rule the live ack already applies. Without this, the
    SAME producer error that is harmless live was fatal here, and — because
    it happened before ``set_watermark`` — the new hash was never stored, so
    the next restart replayed the identical window and crashed again: an
    unbounded crash-loop. Persisting the watermark/hash unconditionally
    after the pass is what makes a producer that fails on every record
    replay exactly once (not forever) rather than exactly zero times
    (silently "succeeding"). Failures are never silent: each one is
    exception-logged with its ``signal_id``/``ts``, and the completion line
    escalates to WARNING (naming the failure count) whenever the pass
    wasn't clean.
    """
    # One KV read for the whole sweep. `build_dispatch` below opens a pass of
    # its own per producer, and a nested pass reuses this pin rather than
    # reading again — so a node with several changed producers pays one scan
    # here, not one each.
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
                rows.append((row.ts, signal_id, row.value))
        rows.sort(key=lambda r: r[0])

        failures = 0
        for ts, signal_id, value in rows:
            record = synthetic_record(signal_id, ts, value)
            for handler in mini_dispatch.get(signal_id, []):
                try:
                    await handler(record)
                except Exception:
                    # Mirrors Ingest._process_record's per-handler guard: one
                    # broken handler must not take the whole replay pass (or
                    # the service startup it runs during) down — log it and
                    # move on to the next handler/record.
                    failures += 1
                    log.exception(
                        "%s: on_metric handler failed replaying signal_id=%s at ts=%.3f — continuing",
                        instance.name,
                        signal_id,
                        ts,
                    )

        # Persisted unconditionally, exactly like a live page is always
        # acked regardless of a handler failure (Ingest.run_once) — a
        # producer that fails on every record still completes its ONE
        # replay pass rather than crash-looping the process forever on
        # the same unstored hash.
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


# ─── the doorbell ───────────────────────────────────────────────────────────


def ring_even_if_undecodable(client) -> None:
    """Keep the doorbell ringing for a message franzmq cannot decode.

    franzmq decodes EVERY inbound message by its topic's contract before it
    dispatches — including to a raw ``message_callback_add`` callback that
    never looks at the payload — and it does so on paho's network thread
    with nothing catching a failure. One ``_Metric`` on the wire without a
    ``timestamp`` therefore raised ``KeyError`` inside the decode, paho's
    thread died, and the service went deaf: no doorbell, no registration
    refresh, no error (seen on level 4, ``payload.py:150``). The doorbell
    exists to ring, not to read, so a message that will not decode still
    rings it: the typed path is tried first and, when it fails, the raw
    paho dispatch delivers the undecoded message to the callbacks that
    matched. Logged at WARNING with the topic, because a contract violation
    on the bus is worth a line — but never a dead thread.
    """
    from paho.mqtt.client import Client as PahoClient

    typed_dispatch = client._handle_on_message
    raw_dispatch = PahoClient._handle_on_message

    def guarded(message):
        try:
            return typed_dispatch(message)
        except Exception:  # noqa: BLE001 — anything the decode raises, by design
            log.warning(
                "undecodable message on %s — ringing the doorbell without reading it",
                getattr(message, "topic", "?"),
                exc_info=True,
            )
            return raw_dispatch(client, message)

    client._handle_on_message = guarded


# ─── the service ────────────────────────────────────────────────────────────


class DataOpsService(Service):
    """A :class:`chaski.Service` that runs :class:`~chaski.dataops.Producer`
    classes — the evaluator runtime as an SDK family (service families
    design §3.5). Everything the base does, it does unchanged:
    one constructor, ``node=None`` for the local door inside a deployment
    or a URL for the published one, ``_ServiceDetails`` registration, log
    publishing, ``kv()``/``stream()``. What it adds is the runtime a
    producer needs — see the module docstring for where each piece lives.

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
        data_dir: Optional[Path] = None,
        retention: Optional[float] = None,
        historian: Optional[Historian] = None,
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
        self._buffer: Optional[Buffer] = None
        self._ingest: Optional[Ingest] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.instances: list[Producer] = []

    # -- the run list ----------------------------------------------------

    def add(self, producer_cls: type[Producer]) -> "DataOpsService":
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
        return self._adopt(lambda cls: cls.__module__ in modules or cls.__module__.startswith(f"{package}."))

    def discover_directory(self, path: Path) -> int:
        """Import every top-level ``*.py`` under ``path`` (see
        :func:`import_directory`) and run every concrete producer those
        files define. Returns how many were added."""
        modules = set(import_directory(path))
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
        if self._buffer is None:
            raise RuntimeError(
                "chaski.DataOpsService: call start() (or run()/serve()) before buffer — "
                "the buffer is opened alongside the door"
            )
        return self._buffer

    @property
    def historian(self) -> Optional[Historian]:
        return self._historian

    @property
    def data_dir(self) -> Path:
        return self._data_dir

    # -- lifecycle -------------------------------------------------------

    def start(self, *, connect_timeout: float = 10.0) -> "DataOpsService":
        """The base class's :meth:`~chaski.Service.start`, then open the
        buffer. Idempotent."""
        super().start(connect_timeout=connect_timeout)
        if self._buffer is None:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._buffer = Buffer(self._data_dir / "buffer.sqlite3")
        return self

    def close(self) -> None:
        super().close()
        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None

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
        """Catalogue-provision every declared ``SignalOutput`` (design §5)
        and bind every declared ``AnnotationOutput`` (design §8) on
        ``instances``, under this service's own identity at the node.
        Returns the catalogue's ``{source: tag_id}``. Requires
        :meth:`start`."""
        door = self.door
        result = build_catalogue(
            instances,
            door,
            node_id=self._node_id,
            mount=self._mount,
            service_name=self.name,
            service_ulid=self._service_id,
        )
        bind_annotation_outputs(instances, node_id=self._node_id, mount=self._mount)
        return result

    def _open_ingest_stream(self, cursor: str, signal_ids: list[str] | None) -> Stream:
        return self.stream("metrics", cursor=cursor, signal_ids=signal_ids)

    def _ring_doorbell(self) -> None:
        """Subscribe ``colca/v1/_Metric/#`` at qos 0 on the base class's MQTT
        client, purely to wake the ingest loop on delivery. The payload is
        never read. The connection is a persistent session
        (``connect_local_mqtt``: ``clean_start=False``, maximal session
        expiry), so the subscription survives a reconnect at the broker.

        QoS 0, deliberately: at QoS 1 the broker must book every metric into
        this client's inflight store; the moment the metric rate outran the
        acks, mochi dropped each delivery AND warned per message ("client
        store quota reached"), filling the broker's log with a line about
        deliveries this service never needed guaranteed.
        """
        client = self._client
        loop = self._loop

        def _on_doorbell(_client, _userdata, _raw_message) -> None:
            ingest = self._ingest
            if ingest is not None and loop is not None:
                loop.call_soon_threadsafe(ingest.wake)

        ring_even_if_undecodable(client)
        client.message_callback_add(DOORBELL_WILDCARD, _on_doorbell)
        client.subscribe(DOORBELL_WILDCARD, qos=0)
        log.info("doorbell: subscribed %s at qos 0", DOORBELL_WILDCARD)

    async def serve(self, stop: Optional[asyncio.Event] = None) -> None:
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

        # 3) Catalogue-provision every declared SignalOutput (design §5) and
        #    bind every declared AnnotationOutput (design §8). From here on
        #    every producer output publishes through the door — MQTT carries
        #    only the doorbell.
        self.bind_outputs(instances)

        # 4) Startup check: a declared window longer than the broker's own
        #    metrics retention, with no historian to cover the gap, fails loud
        #    here rather than showing up later as a silently shrinking frame.
        validate_windows(
            instances,
            retention_s=self.retention_s,
            historian_configured=self._historian is not None,
        )

        # 5) The on_metric dispatch table + declared input signal ids. The
        #    periodic buffer trim (step 10) recomputes its own per-signal
        #    horizon on every run from these SAME instances, so a signal
        #    that resolves after this point is still trimmed once it does.
        dispatch, signal_ids, unresolved = build_dispatch(self, instances)

        # 6) Hash-triggered broker-window replay (design §10).
        await replay_changed_producers(self, instances)

        # 7) Retire the previous generation's cursor (if any) and build the
        #    ingest loop over this service's own consume lane.
        ingest = Ingest(
            self._open_ingest_stream,
            self.buffer,
            dispatch=dispatch,
            signal_ids=signal_ids or None,
            poll_interval_s=self._poll_interval_s,
            # Buffer today has no way to recover a generation from a buffer
            # file that no longer exists — a genuinely lost/rebuilt buffer
            # leaves its old cursor orphaned. This retires it whenever a
            # caller DOES know the prior generation; today nothing supplies
            # one, so this is a documented no-op until a deliberate
            # buffer-rebuild path exists.
            previous_generation=None,
        )
        ingest.retire_previous_generation()
        self._ingest = ingest

        # 8) Start the ingest loop as a background task, and keep retrying
        #    whatever did not resolve — a signal is commissioned by a
        #    separate act, so a producer routinely starts before the tree it
        #    reads exists.
        health_state = health.HealthState(producers=len(instances), generation=self.buffer.generation)
        ingest_task: Optional[asyncio.Task] = None
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

        reresolve_task: Optional[asyncio.Task] = None
        if unresolved:
            log.info("%d declared input(s) unresolved — retrying until they are commissioned.", unresolved)
            reresolve_task = asyncio.ensure_future(
                reresolve_loop(self, instances, ingest, stop, _ensure_ingest_running)
            )

        # 9) The doorbell.
        self._ring_doorbell()

        # 10) Schedule cron/interval triggers (everything except @on_metric),
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

        # 11) The health door, ON this loop on purpose (see chaski.dataops.health):
        #     the probe's question is "is the loop that owns every timer still
        #     answering?", and only a loop-hosted server answers it truthfully.
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

        try:
            asyncio.run(_main())
        except KeyboardInterrupt:
            pass
