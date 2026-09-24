"""Factory-time periodic callbacks with durable progress and no skipped ticks."""

from __future__ import annotations

import asyncio
import datetime
import logging

from apscheduler.triggers.cron import CronTrigger

from chaski.clock import Clock, ClockNotReady

from .base import Producer
from .triggers import CronSpec, IntervalSpec

log = logging.getLogger(__name__)


def timer_key(instance: Producer, method_name: str, spec: CronSpec | IntervalSpec) -> str:
    return f"__clock__:{instance.name}:{method_name}:{spec!r}"


def next_tick(spec: CronSpec | IntervalSpec, previous: float) -> float | None:
    if isinstance(spec, IntervalSpec):
        return previous + spec.seconds
    trigger = CronTrigger.from_crontab(spec.expression, timezone=datetime.UTC)
    date = datetime.datetime.fromtimestamp(previous, datetime.UTC)
    value = trigger.get_next_fire_time(date, date)
    return value.timestamp() if value else None


async def run_periodic(instance: Producer, method_name: str, spec: CronSpec | IntervalSpec, clock: Clock) -> None:
    """One sequential loop per trigger; shutdown cancellation stays real-time.

    A callback failure retries the same tick. Callbacks must use idempotent
    output identities: a crash after output but before progress commits replays
    that tick. A bounded yield gives ingest and health work CPU even at 1000x.
    """
    key = timer_key(instance, method_name, spec)
    buffer = instance.runtime.buffer
    previous = buffer.watermark(key)
    while previous is None:
        try:
            definition = clock.definition
            previous = (
                (definition.start_at if definition.start_at is not None else definition.factory_anchor)
                if definition
                else clock.now()
            )
        except ClockNotReady:
            await asyncio.sleep(0.1)
    method = getattr(instance, method_name)
    while (due := next_tick(spec, previous)) is not None:
        await clock.sleep_until(due)

        def invoke() -> None:
            with instance._lock, clock.at(due):
                asyncio.run(method())
                buffer.set_watermark(key, due, "clock-v1")

        try:
            worker = asyncio.create_task(asyncio.to_thread(invoke))
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                await worker  # Finish/commit before the runtime closes SQLite.
                raise
        except Exception:
            log.exception("Factory callback %s.%s failed at %.6f; will retry", instance.name, method_name, due)
            await asyncio.sleep(1)
            continue
        previous = due
        report = getattr(instance.runtime, "report_progress", None)
        if report is not None:
            try:
                await asyncio.to_thread(report, due)
            except Exception:
                log.warning("Could not report factory callback progress", exc_info=True)
        await asyncio.sleep(0)


async def run_due(instances: list[Producer], clock: Clock, boundary: float, *, inclusive: bool) -> None:
    """Merge scheduled callbacks in time order around a coordinated input batch."""
    definition = clock.definition
    if definition is None:
        raise ValueError("coordinated callbacks require a clock definition")
    start = definition.start_at if definition.start_at is not None else definition.factory_anchor
    while True:
        pending = []
        for instance in instances:
            for method_name, spec in instance.__class__._triggers:
                if not isinstance(spec, (IntervalSpec, CronSpec)):
                    continue
                key = timer_key(instance, method_name, spec)
                previous = instance.runtime.buffer.watermark(key)
                due = next_tick(spec, previous if previous is not None else start)
                if due is not None and (due <= boundary if inclusive else due < boundary):
                    pending.append((due, instance.name, method_name, instance, key))
        if not pending:
            return
        due, _, method_name, instance, key = min(pending, key=lambda item: item[:3])

        def invoke(instance=instance, due=due, method_name=method_name, key=key) -> None:
            with instance._lock, clock.at(due):
                asyncio.run(getattr(instance, method_name)())
                instance.runtime.buffer.set_watermark(key, due, "clock-v1")

        worker = asyncio.create_task(asyncio.to_thread(invoke))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            await worker
            raise
