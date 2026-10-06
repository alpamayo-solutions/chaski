"""Factory-time periodic callbacks with durable progress and no skipped ticks."""

from __future__ import annotations

import asyncio
import datetime
import logging
import math
from typing import Any

from apscheduler.triggers.cron import CronTrigger

from chaski.clock import Clock, ClockNotReady
from chaski.failures import Reject
from chaski.outage import Outage, warn_failure
from chaski.retry import Backoff

from .base import Producer
from .triggers import CronSpec, IntervalSpec

log = logging.getLogger(__name__)


def timer_key(instance: Producer, method_name: str, spec: CronSpec | IntervalSpec) -> str:
    return f"__clock__:{instance.name}:{method_name}:{spec!r}"


def first_position(spec: CronSpec | IntervalSpec, definition: Any, now: float) -> float:
    """The tick a timer with no progress in the current clock run counts from.

    At the run's first revision that is the run's start, so a fresh run fires
    every tick. A worker that meets the run later (it joined late, or its
    progress came from a release that did not record the run) starts where it
    enters: the anchor of the definition in force (a coordinated window's
    start), never after ``now``. Ticks of the run before that are skipped
    rather than caught up. Interval ticks stay on the run's grid.
    """
    start = definition.start_at if definition.start_at is not None else definition.factory_anchor
    if definition.revision == 1:
        return start
    entry = max(start, min(definition.factory_anchor, now))
    if isinstance(spec, IntervalSpec):
        return start + math.floor((entry - start) / spec.seconds) * spec.seconds
    return entry


def next_tick(spec: CronSpec | IntervalSpec, previous: float) -> float | None:
    if isinstance(spec, IntervalSpec):
        return previous + spec.seconds
    trigger = CronTrigger.from_crontab(spec.expression, timezone=datetime.UTC)
    date = datetime.datetime.fromtimestamp(previous, datetime.UTC)
    value = trigger.get_next_fire_time(date, date)
    return value.timestamp() if value else None


def record_rejection(runtime: Any, consumer: str, subject: dict[str, Any], rejected: Reject) -> None:
    """Record ``rejected`` through the runtime's ``reject``; a runtime without
    one cannot pass the input, so the rejection counts as a failure."""
    reject = getattr(runtime, "reject", None)
    if reject is None:
        raise RuntimeError(f"{consumer} rejected an input, but this runtime has nowhere to record it") from rejected
    reject(consumer, subject, rejected)


def _call(instance: Producer, method_name: str, key: str, run_id: str, due: float) -> None:
    """Run one tick's callback and commit its progress in clock run ``run_id``;
    a rejected tick is recorded and passed."""
    try:
        asyncio.run(getattr(instance, method_name)())
    except Reject as rejected:
        record_rejection(instance.runtime, f"{instance.name}.{method_name}", {"timer": key, "due": due}, rejected)
    instance.runtime.buffer.set_timer_position(key, run_id, due)


async def run_periodic(instance: Producer, method_name: str, spec: CronSpec | IntervalSpec, clock: Clock) -> None:
    """One sequential loop per trigger; shutdown cancellation stays real-time.

    A callback failure retries the same tick with bounded, jittered backoff and
    counts against the runtime's ``handler_health``. A callback that raises
    :class:`chaski.Reject` has the tick recorded as rejected (the runtime's
    ``reject``) and passed. Callbacks must use idempotent
    output identities: a crash after output but before progress commits replays
    that tick. A bounded yield gives ingest and health work CPU even at 1000x.

    Progress belongs to the clock run it was made in: a restart within the run
    continues after the last committed tick; without progress in the run the
    timer starts at :func:`first_position`.
    """
    key = timer_key(instance, method_name, spec)
    while True:
        version = clock.changes.version
        try:
            definition = clock.definition
            if definition is not None:
                previous = await asyncio.to_thread(instance.runtime.buffer.timer_position, key, definition.run_id)
                if previous is None:
                    previous = first_position(spec, definition, clock.now())
                break
        except ClockNotReady:
            pass
        await clock.changes.wait_async(version)
    run_id = definition.run_id
    consumer = f"{instance.name}.{method_name}"
    health = getattr(instance.runtime, "handler_health", None)
    retry = Backoff()
    outage = Outage(log, consumer, recovered_as="succeeded again")
    while (due := next_tick(spec, previous)) is not None:
        await clock.sleep_until(due)

        def invoke() -> None:
            with instance._lock, clock.at(due):
                _call(instance, method_name, key, run_id, due)

        try:
            worker = asyncio.create_task(asyncio.to_thread(invoke))
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                await worker  # Finish/commit before the runtime closes SQLite.
                raise
        except Exception as exc:
            count = health.failed(consumer, exc) if health is not None else retry.failures + 1
            delay = retry.delay(exc)
            if not outage.failed(exc, delay=delay):
                log.error(
                    "Factory callback %s failed at %.6f (%d in a row); retrying the same tick in %.1fs",
                    consumer,
                    due,
                    count,
                    delay,
                    exc_info=exc,
                )
            await asyncio.sleep(delay)
            continue
        if health is not None:
            health.succeeded(consumer)
        retry.reset()
        outage.recovered()
        previous = due
        report = getattr(instance.runtime, "report_progress", None)
        if report is not None:
            try:
                await asyncio.to_thread(report, due)
            except Exception as exc:
                warn_failure(log, exc, "Could not report factory callback progress")
        await asyncio.sleep(0)


async def run_due(instances: list[Producer], clock: Clock, boundary: float, *, inclusive: bool) -> None:
    """Merge scheduled callbacks in time order around a coordinated input batch."""
    definition = clock.definition
    if definition is None:
        raise ValueError("coordinated callbacks require a clock definition")
    run_id = definition.run_id
    while True:
        pending = []
        for instance in instances:
            for method_name, spec in instance.__class__._triggers:
                if not isinstance(spec, (IntervalSpec, CronSpec)):
                    continue
                key = timer_key(instance, method_name, spec)
                previous = instance.runtime.buffer.timer_position(key, run_id)
                if previous is None:
                    previous = first_position(spec, definition, boundary)
                due = next_tick(spec, previous)
                if due is not None and (due <= boundary if inclusive else due < boundary):
                    pending.append((due, instance.name, method_name, instance, key))
        if not pending:
            return
        due, _, method_name, instance, key = min(pending, key=lambda item: item[:3])

        def invoke(instance=instance, due=due, method_name=method_name, key=key) -> None:
            with instance._lock, clock.at(due):
                _call(instance, method_name, key, run_id, due)

        worker = asyncio.create_task(asyncio.to_thread(invoke))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            await worker
            raise
