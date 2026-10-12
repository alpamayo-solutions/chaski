"""Tick arithmetic for ``@every``/``@cron`` triggers and recording a rejected input."""

from __future__ import annotations

import datetime
from typing import Any

from apscheduler.triggers.cron import CronTrigger

from chaski.failures import Reject

from .triggers import CronSpec, IntervalSpec


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
