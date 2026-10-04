"""Backfill: run a producer once over history that is older than live intake.

Live intake reads the node's ``metrics`` stream, which holds only the node's
stream retention. A producer that must also cover older history declares it::

    class Cycles(Producer):
        name = "cycles"
        system_element_name = "press"
        backfill = Backfill(horizon="400d", window="6h")

        state = SignalRangeInput("state", window="1h")
        cycle = AnnotationOutput("cycle")

        @on_metric("state")
        async def on_state(self, metric) -> None:
            ...  # the same handler runs live, on replay and in backfill

**The first backfill.** At its first start the service records a job from
``now - horizon`` to the live edge and holds the producer's live triggers
(``@on_metric`` and ``@every``/``@cron``) while it runs. The live ingest goes
on buffering the producer's inputs; it only does not dispatch them. The job
walks history in windows of ``window``, oldest first. In each window the
records of the producer's ``@on_metric`` inputs, read source-transparently
(:meth:`SignalRangeInput.fetch`: the buffer, and the historian for what the
buffer no longer holds), and the instants of its ``@every``/``@cron`` ticks,
go in timestamp order through the same handlers live traffic uses. A handler
sees ``self.now`` at its record's timestamp, a tick at its instant.

**Handover.** When the next window would reach the live edge (the newest
buffered point of the producer's inputs) and the ingest has drained to the
stream head once, the job takes the ingest's page lock, so no page is half
dispatched, processes the rest of the buffer up to that edge, and releases the
producer to live dispatch. Every record the buffer held was handled by the
backfill; every later page is dispatched live, in stream order, as it would
have been without a backfill. One instance sees the producer's whole timeline
in order, so a producer with in-memory state ends the backfill in the state
live processing continues from.

**Independent mode.** ``Backfill(..., mode="independent")`` does not hold
the producer's live triggers: live dispatch runs from the first start, as for
a producer without a backfill. The first job then covers ``[now - horizon,
live start)``; the live start is recorded with the job at that first start, so
neither a restart nor a code change moves it. The job runs on a separate
instance, after ``setup()``, like a repair, beside live dispatch, throttled the
same way. Where history and live overlap (live processing also covers what the
stream still holds), deterministic output ids make the overlap idempotent.

Choose ``hold_live`` (the default) when the producer's in-memory state must
flow continuously from history into live. Choose ``independent`` when its
output does not depend on state carried across the live start, because its
state restarts cleanly from the data (a cycle segmenter that opens a cycle at
the next start condition, a per-sample computation): live output is there at
once instead of after hours of history, and the union of history and live
output equals one live pass. Such a producer writes a result once it is final
(a cycle at its end): the history run stops at the live start with the cycle
open there unfinished, and a write of it as still open would overwrite what
live processing wrote at its end. A producer switched from ``hold_live`` to
``independent`` while its first job runs keeps the job's position; its live
start is fixed at that restart.

**Durable and resumable.** The job's position is committed after each window,
in the same commit as the producer's checkpoint (``state_version``) when the
job holds live. After a restart the job resumes at the last committed window,
with the checkpoint restored; a producer without ``state_version``, and every
job on a separate instance, resumes with the state its ``setup()`` gives it. A window that failed is processed again. A changed code
hash restarts the job from its start.

**Throttled.** Between windows the job sleeps long enough that it runs at most
``backfill_rate`` windows per second and is busy at most ``backfill_busy`` of
the wall time (:class:`~chaski.dataops.DataOpsService`).

**Idempotent output.** Backfill emits through the producer's own outputs:
``AnnotationOutput.write_interval`` derives the annotation id from the
interval, and a ``SignalOutput`` sample is keyed by signal and timestamp, so
processing a record twice (a window redone after a crash, a repair over a range
live processing covered) overwrites rather than duplicates.

**Repair.** :meth:`DataOpsService.request_backfill` records a job for an
explicit range. It runs on a separate instance of the producer, after
``setup()``, so the live instance's state and its live dispatch are left
alone. The same throttle applies. Its ``window`` is the job's own; without
one it is the declaration's, or ``DEFAULT_WINDOW``.

**Progress.** The health door reports ``backfill`` (the job, its range,
position and share done), and the service keeps a retained ``backfill``
``_Finding`` while a job runs, retired when none is left. A running backfill
is progress, not lag: the job reads history through the buffer and the
historian, never through a cursor, so it adds nothing to the node's
``cursor_lag``. While a first job holds live dispatch the door's ``status``
is ``backfilling``. A job with no window finished for ``stall_after`` seconds
(the throttle's own pause not counted) is reported as ``stalled`` with the
reason, and the door's ``status`` is ``degraded``; it stays 200, since live
intake is not affected.

Nothing here reads Colca to find new data: live intake stays push-driven, and
the runner waits on its own doorbell (a new request) and on the ingest's.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import ulid

from chaski.doorbell import Doorbell
from chaski.failures import Reject
from chaski.outage import warn_failure

from . import codehash
from .base import Producer, restore_checkpoint, runtime_now, save_checkpoint, state_copy
from .ingest import consumer_name
from .inputs import SignalRangeInput, declared_inputs
from .outputs import AnnotationOutput, SignalOutput, resolved_outputs
from .scheduling import next_tick, record_rejection
from .triggers import CronSpec, IntervalSpec, parse_duration

if TYPE_CHECKING:
    from .service import DataOpsService

log = logging.getLogger("chaski.dataops.backfill")

#: The job a producer's ``backfill`` declaration starts: from its horizon to
#: the live edge (``hold_live``) or to where live dispatch began (``independent``).
INITIAL = "initial"
#: The first backfill holds live dispatch until it reaches the live edge.
HOLD_LIVE = "hold_live"
#: Live dispatch starts at once; the first backfill covers what lies before.
INDEPENDENT = "independent"
MODES = (HOLD_LIVE, INDEPENDENT)
DEFAULT_WINDOW = "1h"
#: At most this many windows per second.
DEFAULT_RATE = 1.0
#: Busy at most this share of the wall time.
DEFAULT_BUSY = 0.5
#: The retained ``_Finding`` a running backfill keeps, under the service.
FINDING = "backfill"
#: A running job republishes its finding at most this often.
FINDING_INTERVAL_S = 60.0
#: A job with no window finished for this long is reported as stalled.
DEFAULT_STALL_AFTER_S = 600.0


class Backfill:
    """A producer's declaration that it processes history older than live
    intake: ``horizon`` (how far back, ``"400d"`` or seconds), ``window``
    (how much history one step processes) and ``mode``, how the first
    backfill relates to live dispatch:

    * ``"hold_live"`` (the default): live triggers wait until the history is
      processed, and one instance carries its state from history into live.
    * ``"independent"``: live dispatch starts at once, and the history before
      that start runs beside it on a separate instance.

    See the module docstring for when to choose which."""

    __slots__ = ("horizon_s", "mode", "window_s")

    def __init__(self, horizon: str | float, window: str | float = DEFAULT_WINDOW, *, mode: str = HOLD_LIVE) -> None:
        if mode not in MODES:
            raise ValueError(f"Backfill mode must be one of {', '.join(MODES)}, got {mode!r}")
        self.horizon_s = parse_duration(horizon)
        self.window_s = parse_duration(window)
        self.mode = mode

    @property
    def holds_live(self) -> bool:
        return self.mode == HOLD_LIVE

    def __repr__(self) -> str:
        return f"Backfill(horizon={self.horizon_s:g}, window={self.window_s:g}, mode={self.mode!r})"


def tick_instants(spec: CronSpec | IntervalSpec, start: float, end: float) -> list[float]:
    """Every instant in ``[start, end)`` at which ``spec`` fires: interval
    ticks on multiples of the interval since the epoch, cron in UTC. Windows
    that tile a range therefore fire every instant exactly once."""
    if isinstance(spec, IntervalSpec):
        instants = []
        due = math.ceil(start / spec.seconds) * spec.seconds
        while due < end:
            if due >= start:
                instants.append(due)
            due += spec.seconds
        return instants
    fires: list[float] = []
    fire = next_tick(spec, start - 1.0)
    while fire is not None and fire < end:
        if fire >= start:
            fires.append(fire)
        fire = next_tick(spec, fire)
    return fires


@dataclass
class _Plan:
    """What one producer's backfill reads and calls, from one resolution."""

    #: ``(signal_id, input, handlers)`` per ``@on_metric`` input signal.
    metrics: list[tuple[str, SignalRangeInput, list]] = field(default_factory=list)
    #: Every resolved input signal: what the live edge is taken over.
    signal_ids: list[str] = field(default_factory=list)
    #: ``(method_name, spec)`` per ``@every``/``@cron`` trigger.
    ticks: list[tuple[str, CronSpec | IntervalSpec]] = field(default_factory=list)
    declared: int = 0
    unresolved: int = 0


def _plan(instance: Producer) -> _Plan:
    from .service import _resolve_dispatch

    dispatch, signal_ids, unresolved = _resolve_dispatch([instance])
    inputs: dict[str, SignalRangeInput] = {}
    declared = 0
    for _name, declared_input in declared_inputs(instance):
        declared += 1
        with contextlib.suppress(LookupError):
            inputs.setdefault(declared_input.signal_id, declared_input)
    metrics = [(sid, inputs[sid], dispatch[sid]) for sid in signal_ids if dispatch.get(sid)]
    ticks = [(m, s) for m, s in type(instance)._triggers if isinstance(s, CronSpec | IntervalSpec)]
    return _Plan(metrics, list(signal_ids), ticks, declared, unresolved)


def _bind_like(target: Producer, template: Producer) -> None:
    """Give ``target``'s outputs the bindings ``template``'s have, so a repair
    instance writes exactly what the live one does."""
    for name, output in resolved_outputs(template):
        copy = getattr(target, name)
        if isinstance(output, SignalOutput) and output._source is not None and output._tag_id is not None:
            copy.bind(output._source, output._tag_id)
        elif (
            isinstance(output, AnnotationOutput)
            and output._source is not None
            and output._node_id is not None
            and output._topic_prefix is not None
        ):
            copy.bind(output._source, output._node_id, output._topic_prefix)


def _iso(ts: float | None) -> str | None:
    return None if ts is None else datetime.fromtimestamp(ts, UTC).isoformat()


class BackfillRunner:
    """Runs a service's backfill jobs, one window at a time; see the module docstring."""

    def __init__(
        self,
        service: DataOpsService,
        instances: list[Producer],
        *,
        rate: float = DEFAULT_RATE,
        busy: float = DEFAULT_BUSY,
        stall_after: float = DEFAULT_STALL_AFTER_S,
    ) -> None:
        if stall_after <= 0:
            raise ValueError("backfill_stall_after must be positive")
        if rate <= 0:
            raise ValueError("backfill_rate must be positive")
        if not 0 < busy <= 1:
            raise ValueError("backfill_busy must be in (0, 1]")
        self._service = service
        self._instances = {instance.name: instance for instance in instances}
        self._rate = rate
        self._busy = busy
        self._stall_after = stall_after
        #: Rung by :meth:`request`: a new job may be waiting.
        self.bell = Doorbell()
        self._lock = threading.Lock()
        self._held: set[str] = set()
        self._current: dict[str, Any] | None = None
        self._pending = 0
        #: Monotonic time the running job last moved: started, finished a
        #: window, or ended the throttle's pause. ``None`` while none runs.
        self._moved_at: float | None = None
        #: What the running job waits for, ``""`` while it is not waiting.
        self._waiting = ""
        self._finding_up: bool | None = None
        self._finding_at = 0.0

    @property
    def _buffer(self):
        return self._service.buffer

    # -- startup -----------------------------------------------------------

    def plan(self) -> None:
        """Record the first job of every producer that declares a backfill and
        has none yet, and hold the live triggers of each whose first job holds
        live dispatch and is not done.

        An independent first job ends where live dispatch begins: now, at the
        first start. The end is recorded with the job, so a restart keeps it.
        A first job recorded as holding live and not yet done, whose producer
        now declares ``independent``, gets its end here and goes on beside
        live dispatch."""
        now = runtime_now(self._service)
        for name, instance in self._instances.items():
            spec = type(instance).backfill
            if spec is None:
                continue
            created = self._buffer.add_backfill_job(
                name,
                INITIAL,
                now - spec.horizon_s,
                None if spec.holds_live else now,
                codehash.compute_code_hash(type(instance)),
                created_at=time.time(),
            )
            job = self._buffer.backfill_job(name, INITIAL)
            if job is None or job["done"]:
                continue
            if job["end"] is None and not spec.holds_live:
                self._buffer.set_backfill_end(name, INITIAL, now)
                job = {**job, "end": now}
            if job["end"] is None:
                with self._lock:
                    self._held.add(name)
                log.info(
                    "%s: %s backfill from %s to the live edge; live triggers held until it is done",
                    name,
                    "starting" if created else "resuming",
                    _iso(job["position"]),
                )
            else:
                log.info(
                    "%s: %s independent backfill from %s to %s; live dispatch runs meanwhile",
                    name,
                    "starting" if created else "resuming",
                    _iso(job["position"]),
                    _iso(job["end"]),
                )

    def holds(self, producer: str) -> bool:
        """Whether ``producer``'s live triggers are held for its first backfill."""
        with self._lock:
            return producer in self._held

    # -- repair ------------------------------------------------------------

    def request(self, producer: str, start: float, end: float, *, window: Any = None) -> str:
        """Record a job over ``[start, end)`` and wake the runner. Returns its id."""
        job = (
            request(self._buffer, self._instances[producer], start, end, window=window)
            if producer in self._instances
            else None
        )
        if job is None:
            raise KeyError(f"no producer {producer!r} runs in this service")
        self.bell.ring()
        return job

    # -- progress ----------------------------------------------------------

    def status(self) -> dict[str, Any]:
        """The running job and how many are pending, for the health door; empty when idle."""
        with self._lock:
            current = dict(self._current) if self._current is not None else None
            pending = self._pending
            moved_at, waiting = self._moved_at, self._waiting
        if current is None and not pending:
            return {}
        if current is not None and moved_at is not None:
            still = time.monotonic() - moved_at
            if still > self._stall_after:
                current["stalled"] = f"no backfill window finished in {still:.0f} s" + (
                    f"; it waits for {waiting}" if waiting else ""
                )
        return {"running": current, "pending": pending}

    def _moved(self, *, pause: float = 0.0) -> None:
        """The running job moved now; a throttle ``pause`` that follows does
        not count towards a stall."""
        with self._lock:
            self._moved_at = time.monotonic() + pause
            self._waiting = ""

    def _wait_for(self, waiting: str) -> None:
        with self._lock:
            self._waiting = waiting

    def _report(self, job: dict[str, Any], position: float, windows: int, end: float | None) -> None:
        start = job["start"]
        span = (end if end is not None else position) - start
        progress = 1.0 if span <= 0 else min(1.0, max(0.0, (position - start) / span))
        with self._lock:
            self._current = {
                "producer": job["producer"],
                "job": job["job"],
                "from": _iso(start),
                "to": _iso(job["end"]) if job["end"] is not None else "live edge",
                "position": _iso(position),
                "windows": windows,
                "progress": round(progress, 4),
                "holds_live": job["producer"] in self._held,
                "window_s": job["window_s"],
            }

    async def _publish_finding(self, *, force: bool = False) -> None:
        status = self.status()
        if status and not force and time.monotonic() - self._finding_at < FINDING_INTERVAL_S:
            return
        self._finding_at = time.monotonic()
        topic = self._service._backfill_finding_topic()
        try:
            if status:
                running = status["running"] or {}
                summary = (
                    f"{running.get('producer')} backfill {running.get('job')}: "
                    f"{100 * running.get('progress', 0):.0f}% ({running.get('position')} of "
                    f"{running.get('from')} to {running.get('to')}), {status['pending']} job(s) pending"
                )
                payload = {
                    "reason": FINDING,
                    "summary": summary[:300],
                    "observed_at": time.time(),
                    "suggested_severity": "info",
                    "detail": status,
                    "remedy": "None needed: the finding is retired when every backfill is done.",
                }
                await asyncio.to_thread(self._service.send, topic, json.dumps(payload), retain=True)
                self._finding_up = True
            elif self._finding_up or (self._finding_up is None and self._buffer.backfill_jobs()):
                await asyncio.to_thread(self._service.retract, topic)
                self._finding_up = False
        except Exception as exc:
            warn_failure(log, exc, "Could not publish the backfill finding")

    # -- the loop ----------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        """Run every pending job, oldest first; then wait for a request."""
        while not stop.is_set():
            seen = self.bell.generation
            jobs = [
                job for job in await asyncio.to_thread(self._buffer.backfill_jobs, pending_only=True) if self._runs(job)
            ]
            with self._lock:
                self._pending = max(0, len(jobs) - 1)
            if not jobs:
                with self._lock:
                    self._current = None
                    self._moved_at = None
                    self._waiting = ""
                await self._publish_finding(force=True)
                await _until(self.bell.after(seen), stop)
                continue
            await self._run_job(jobs[0], stop)

    def _runs(self, job: dict[str, Any]) -> bool:
        instance = self._instances.get(job["producer"])
        if instance is None:
            return False
        return job["job"] != INITIAL or type(instance).backfill is not None

    async def _run_job(self, job: dict[str, Any], stop: asyncio.Event) -> None:
        name, job_id = job["producer"], job["job"]
        live = self._instances[name]
        cls = type(live)
        initial = job_id == INITIAL
        # A first backfill that holds live dispatch runs on the live instance
        # up to the live edge; every other job on its own instance up to its end.
        holding = initial and job["end"] is None
        window = _window_of(cls, job)
        code_hash = await asyncio.to_thread(codehash.compute_code_hash, cls)
        position, windows = job["position"], job["windows"]
        if windows and job["code_hash"] != code_hash:
            log.info(
                "%s: code changed during backfill %s — starting it again from %s", name, job_id, _iso(job["start"])
            )
            position, windows = job["start"], 0
            await asyncio.to_thread(self._buffer.backfill_progress, name, job_id, position, windows, code_hash)
        # Computing the hash parses the producer's modules: once per job.
        job = {**job, "code_hash": code_hash, "window_s": window}

        if holding:
            instance = live
            if windows:
                restore_checkpoint(instance)
        else:
            instance = cls().attach(self._service)
            await instance.setup()
            _bind_like(instance, live)
        log.info("%s: backfill %s at %s (%d window(s) done)", name, job_id, _iso(position), windows)
        # Reported from the start, so a job that waits before its first
        # window shows, and a stall there too. Holding live, the edge is
        # about now until the plan says where the buffer ends.
        self._report(job, position, windows, job["end"] if job["end"] is not None else runtime_now(self._service))
        self._moved()
        try:
            while not stop.is_set():
                plan = await self._resolved(instance, stop)
                if plan is None:
                    return
                if self._service.historian is None and plan.signal_ids:
                    # Without a historian the buffer is all there is: skip to
                    # its first point, once the ingest has filled it.
                    if initial and not await self._caught_up(plan, stop):
                        continue
                    first = self._first(plan)
                    if first is not None and position < first:
                        position = first
                if holding:
                    edge = self._edge(plan)
                    self._report(job, position, windows, edge)
                    if position + window < edge:
                        position, windows = await self._step(
                            instance, plan, job, position, position + window, windows, stop
                        )
                        continue
                    if await self._hand_over(instance, plan, job, position, windows, code_hash, window, stop):
                        return
                    continue
                end = job["end"]
                self._report(job, position, windows, end)
                if position >= end:
                    await asyncio.to_thread(self._buffer.finish_backfill, name, job_id)
                    log.info("%s: backfill %s done (%d window(s))", name, job_id, windows)
                    return
                position, windows = await self._step(
                    instance, plan, job, position, min(position + window, end), windows, stop
                )
        finally:
            if not holding:
                try:
                    await instance.teardown()
                except Exception as exc:
                    warn_failure(log, exc, f"Teardown of {name}'s backfill instance failed")

    async def _resolved(self, instance: Producer, stop: asyncio.Event) -> _Plan | None:
        """The producer's plan once at least one of its declared inputs
        resolves (at once for a producer without inputs); ``None`` on stop.
        Waits on the service's definition view, which the node pushes."""
        cache = getattr(self._service.door, "_dataops_definitions", None)
        while not stop.is_set():
            version = cache.changes.version if cache is not None else 0
            plan = await asyncio.to_thread(_plan, instance)
            if not plan.declared or plan.signal_ids or cache is None:
                if plan.unresolved:
                    log.warning("%s: %d input(s) unresolved; backfilling the others", instance.name, plan.unresolved)
                self._wait_for("")
                return plan
            log.info("%s: backfill waits for its inputs to be commissioned", instance.name)
            self._wait_for("its inputs to be commissioned")
            await cache.changes.wait_async(version, stop=stop)
        return None

    async def _caught_up(self, plan: _Plan, stop: asyncio.Event) -> bool:
        """Whether the ingest has drained to the stream head once; until then
        the buffer is not where live intake continues from. Waits for it,
        returning False so the caller looks again."""
        ingest = self._service._ingest
        if not plan.signal_ids or ingest is None or ingest.caught_up.generation:
            return True
        self._wait_for("the live ingest to reach the stream head")
        await _until(ingest.caught_up.after(0), stop)
        self._wait_for("")
        return False

    def _first(self, plan: _Plan) -> float | None:
        """The oldest buffered point of the producer's inputs, or ``None``."""
        earliest = [ts for ts in (self._buffer.earliest(sid) for sid in plan.signal_ids) if ts is not None]
        return min(earliest) if earliest else None

    def _edge(self, plan: _Plan) -> float:
        """The newest buffered point of the producer's inputs: what live
        intake has reached. ``now`` while nothing is buffered."""
        latest = [ts for ts in (self._buffer.latest(sid) for sid in plan.signal_ids) if ts is not None]
        return max(latest) if latest else runtime_now(self._service)

    async def _step(
        self,
        instance: Producer,
        plan: _Plan,
        job: dict[str, Any],
        start: float,
        end: float,
        windows: int,
        stop: asyncio.Event,
    ) -> tuple[float, int]:
        """One window, committed, then the throttle's pause."""
        began = time.monotonic()
        await self._window(instance, plan, job, start, end)
        windows += 1
        await asyncio.to_thread(self._commit, instance, job, end, windows)
        elapsed = time.monotonic() - began
        pause = max(1.0 / self._rate - elapsed, elapsed * (1.0 / self._busy - 1.0), 0.0)
        self._moved(pause=pause)
        await self._publish_finding()
        if pause > 0:
            await _until(asyncio.sleep(pause), stop)
        return end, windows

    def _commit(
        self, instance: Producer, job: dict[str, Any], position: float, windows: int, *, done: bool = False
    ) -> None:
        code_hash = job["code_hash"]
        with self._buffer.one_commit():
            if _holds_live(job):
                save_checkpoint(instance)
            self._buffer.backfill_progress(job["producer"], job["job"], position, windows, code_hash)
            if done:
                self._buffer.finish_backfill(job["producer"], job["job"])

    async def _hand_over(
        self,
        instance: Producer,
        plan: _Plan,
        job: dict[str, Any],
        position: float,
        windows: int,
        code_hash: str,
        window: float,
        stop: asyncio.Event,
    ) -> bool:
        """Process the rest of the buffer and release the producer to live
        dispatch, between two ingest pages. False when the live edge moved
        more than a window on meanwhile: the caller walks on first."""
        if not await self._caught_up(plan, stop):
            return False
        ingest = self._service._ingest
        page_lock = ingest.page_lock if ingest is not None else threading.Lock()
        await _acquire(page_lock)
        try:
            edge = self._edge(plan)
            if position + window < edge:
                return False
            end = math.nextafter(max(edge, position), math.inf)
            await self._window(instance, plan, job, position, end)
            await asyncio.to_thread(self._commit, instance, job, end, windows + 1, done=True)
            await asyncio.to_thread(self._buffer.set_watermark, instance.name, edge, code_hash)
            with self._lock:
                self._held.discard(instance.name)
                self._current = None
                self._moved_at = None
                self._waiting = ""
        finally:
            page_lock.release()
        log.info(
            "%s: backfill done at %s (%d window(s)); live dispatch resumes", instance.name, _iso(edge), windows + 1
        )
        await self._publish_finding(force=True)
        return True

    async def _window(self, instance: Producer, plan: _Plan, job: dict[str, Any], start: float, end: float) -> int:
        """Every record and tick of ``[start, end)`` through the producer's
        own handlers, in timestamp order. A checkpointed producer's state is
        put back if the window fails, so the retry starts where it did."""
        from .service import synthetic_record

        historian = self._service.historian
        events: list[tuple[float, int, int, Any, Any]] = []
        for order, (signal_id, declared_input, handlers) in enumerate(plan.metrics):
            low = start
            if historian is None:
                # Without a historian, what the buffer holds is all there is.
                earliest = self._buffer.earliest(signal_id)
                if earliest is None or earliest >= end:
                    continue
                low = max(start, earliest)
            frame = await asyncio.to_thread(declared_input.fetch, low, end)
            for ts, value in zip(frame["ts"].tolist(), frame["value"].tolist(), strict=True):
                events.append((float(ts), 0, order, (signal_id, handlers), value))
        for order, (method_name, spec) in enumerate(plan.ticks):
            events.extend((due, 1, order, method_name, None) for due in tick_instants(spec, start, end))
        events.sort(key=lambda event: event[:3])

        checkpointed = _holds_live(job) and instance.state_version is not None
        previous = state_copy(instance) if checkpointed else None
        try:
            for ts, kind, _order, target, value in events:
                if kind == 0:
                    signal_id, handlers = target
                    record = synthetic_record(signal_id, ts, value, actor="backfill")
                    for handler in handlers:
                        try:
                            await handler(record)
                        except Reject as rejected:
                            subject = {
                                "backfill": job["job"],
                                "producer": instance.name,
                                "signal_id": signal_id,
                                "ts": ts,
                            }
                            await asyncio.to_thread(
                                record_rejection, self._service, consumer_name(handler), subject, rejected
                            )
                else:
                    await asyncio.to_thread(self._tick, instance, target, ts, job)
        except BaseException:
            if previous is not None:
                instance.restore_state(previous)
            raise
        return len(events)

    def _tick(self, instance: Producer, method_name: str, due: float, job: dict[str, Any]) -> None:
        """One tick at its instant, off the loop like a live tick."""
        clock = getattr(self._service, "clock", None)
        with instance._lock, clock.at(due) if clock is not None else contextlib.nullcontext():
            try:
                asyncio.run(getattr(instance, method_name)())
            except Reject as rejected:
                consumer = f"{instance.name}.{method_name}"
                subject = {"backfill": job["job"], "producer": instance.name, "timer": method_name, "due": due}
                record_rejection(self._service, consumer, subject, rejected)


def request(
    buffer: Any, instance_or_cls: Producer | type[Producer], start: Any, end: Any, *, window: Any = None
) -> str:
    """Record a repair job for a producer over ``[start, end)`` (unix
    seconds or datetimes) in ``buffer``; returns the job id. ``window``
    (``"6h"`` or seconds) is how much history one step processes; without
    it the job steps by the producer's ``backfill`` window, or
    ``DEFAULT_WINDOW`` when it declares none."""
    from .outputs import _epoch

    cls = instance_or_cls if isinstance(instance_or_cls, type) else type(instance_or_cls)
    start_s, end_s = _epoch(start), _epoch(end)
    if not math.isfinite(start_s) or not math.isfinite(end_s) or end_s <= start_s:
        raise ValueError(f"a backfill needs start < end, got {start!r} .. {end!r}")
    window_s = None if window is None else parse_duration(window)
    job = f"repair-{ulid.new()}"
    # The runner records the code hash with the first window it commits.
    buffer.add_backfill_job(cls.name, job, start_s, end_s, "", created_at=time.time(), window=window_s)
    log.info("%s: backfill %s requested over %s .. %s", cls.name, job, _iso(start_s), _iso(end_s))
    return job


def _holds_live(job: dict[str, Any]) -> bool:
    """Whether ``job`` is a first backfill that holds live dispatch."""
    return job["job"] == INITIAL and job["end"] is None


def _window_of(cls: type[Producer], job: dict[str, Any]) -> float:
    """How much history one step of ``job`` processes: a first backfill's
    from the declaration, a repair's from the job and else the declaration."""
    if job["job"] != INITIAL and job.get("window") is not None:
        return float(job["window"])
    return cls.backfill.window_s if cls.backfill is not None else parse_duration(DEFAULT_WINDOW)


async def _acquire(lock: threading.Lock) -> None:
    """Take ``lock`` without blocking the loop. Cancelled while waiting, the
    lock is released as soon as the waiting thread gets it."""
    acquiring = asyncio.ensure_future(asyncio.to_thread(lock.acquire))
    try:
        await asyncio.shield(acquiring)
    except asyncio.CancelledError:
        acquiring.add_done_callback(lambda done: lock.release() if not done.cancelled() and done.result() else None)
        raise


async def _until(awaitable: Any, stop: asyncio.Event) -> None:
    """Wait for ``awaitable`` or ``stop``, whichever comes first."""
    task = asyncio.ensure_future(awaitable)
    stopping = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({task, stopping}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for pending in (task, stopping):
            if not pending.done():
                pending.cancel()


__all__ = [
    "DEFAULT_BUSY",
    "DEFAULT_RATE",
    "HOLD_LIVE",
    "INDEPENDENT",
    "INITIAL",
    "Backfill",
    "BackfillRunner",
    "request",
    "tick_instants",
]
