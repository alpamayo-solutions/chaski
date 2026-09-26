"""Opt-in application time. Network deadlines and credentials always use real time.

A host using NTP selects ``local``; ``mqtt`` uses Colca's existing _TimeSync
beacon instead. No OS clock is changed and the two corrections are never added.
Factory definitions are retained configuration, while time beacons must be live.
"""

from __future__ import annotations

import asyncio
import math
import os
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
from typing import Literal, cast

from colca_data_contracts.payload import ClockDefinition, ClockSegment


def _current_task() -> asyncio.Task | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


class ClockNotReady(RuntimeError):
    """Application time is unavailable; keep infrastructure alive and retry."""


def validate_definition(value: ClockDefinition) -> None:
    if value.previous is not None:
        previous = value.previous if isinstance(value.previous, ClockSegment) else ClockSegment(**value.previous)
        validate_definition(
            ClockDefinition(
                value.id,
                value.run_id,
                value.revision,
                previous.real_anchor,
                previous.factory_anchor,
                previous.rate,
                previous.stop_at,
                previous.catch_up,
            )
        )
        if (
            previous.real_anchor > value.real_anchor
            or abs(project(previous, value.real_anchor) - value.factory_anchor) > 1e-6
        ):
            raise ValueError("previous clock segment must preserve continuity")
    if not isinstance(value.id, str) or not value.id or not isinstance(value.run_id, str) or not value.run_id:
        raise ValueError("clock id and run_id are required")
    if type(value.revision) is not int or value.revision < 1:
        raise ValueError("clock revision must be a positive integer")
    for name in ("real_anchor", "factory_anchor", "rate", "stop_at", "start_at"):
        number = getattr(value, name)
        if name in ("stop_at", "start_at") and number is None:
            continue
        if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number):
            raise ValueError(f"{name} must be finite")
    if value.rate < 0 or value.rate > 1000:
        raise ValueError("rate must be between 0 and 1000")
    if value.stop_at is not None and value.stop_at < value.factory_anchor:
        raise ValueError("stop_at must not precede factory_anchor")
    if value.start_at is not None and value.start_at > value.factory_anchor:
        raise ValueError("start_at must not follow factory_anchor")
    if type(value.catch_up) is not bool:
        raise ValueError("catch_up must be boolean")
    if value.catch_up and (value.factory_anchor > value.real_anchor or 0 < value.rate < 1):
        raise ValueError("catch-up requires a past anchor and rate 0 or >= 1")


def project(definition: ClockDefinition | ClockSegment, real_now: float) -> float:
    """Evaluate a clock definition without state (also useful to controllers)."""
    previous = getattr(definition, "previous", None)
    if previous is not None and real_now < definition.real_anchor:
        if isinstance(previous, dict):
            previous = ClockSegment(**previous)
        return project(previous, real_now)
    result = definition.factory_anchor + max(0.0, real_now - definition.real_anchor) * definition.rate
    if definition.catch_up:
        result = min(result, real_now)
    if definition.stop_at is not None:
        result = min(result, definition.stop_at)
    return result


@dataclass(frozen=True)
class ClockStatus:
    source: str
    ready: bool
    sync_age: float | None
    run_id: str | None
    revision: int | None
    rate: float
    factory_now: float | None
    reason: str = ""


class Clock:
    """One real-time source and an optional named factory timeline.

    With no arguments this is host time, with no subscriptions or dependency on
    NTP. In MQTT mode a fresh beacon is required initially and after reconnect;
    between beacons monotonic elapsed time advances the estimate. After
    ``max_sync_age`` seconds the clock refuses timestamps rather than silently
    switching domains. Callers should surface :meth:`status` as health.

    ``definition_topic`` must name one exact authority, not a wildcard. A
    selected definition is required before application timestamps can be made.
    Existing runs cannot be rewound or replaced by delayed retained messages.
    """

    def __init__(
        self,
        *,
        source: Literal["local", "mqtt"] = "local",
        definition_topic: str | None = None,
        max_sync_age: float = 120.0,
        wall: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if source not in ("local", "mqtt"):
            raise ValueError("clock source must be local or mqtt")
        if not math.isfinite(max_sync_age) or max_sync_age <= 0:
            raise ValueError("max_sync_age must be positive and finite")
        if definition_topic is not None:
            parts = definition_topic.split("/")
            if len(parts) != 5 or parts[2] != "_ClockDefinition" or any(not p for p in parts):
                raise ValueError("definition_topic must address one _ClockDefinition authority and id")
            if "+" in definition_topic or "#" in definition_topic:
                raise ValueError("definition_topic cannot contain wildcards")
        self.source = source
        self.definition_topic = definition_topic
        self.max_sync_age = max_sync_age
        self._wall = wall
        self._monotonic = monotonic
        self._lock = threading.RLock()
        self._sync: tuple[float, float] | None = None
        self._definition: ClockDefinition | None = None
        self._previous_definition: ClockDefinition | None = None
        self._last: float | None = None
        self._definition_available = not bool(definition_topic)
        # The scheduled time, and the task it was set in (None: set outside a loop).
        self._scheduled: ContextVar[tuple[float, asyncio.Task | None] | None] = ContextVar(
            "factory_scheduled_time", default=None
        )

    @classmethod
    def from_env(cls) -> Clock:
        """Deployment seam; plain SDK construction never reads environment."""
        return cls(
            source=cast(Literal["local", "mqtt"], os.environ.get("APPLICATION_TIME_SOURCE", "local")),
            definition_topic=os.environ.get("FACTORY_CLOCK_TOPIC") or None,
            max_sync_age=float(os.environ.get("TIME_SYNC_MAX_AGE_S", "120")),
        )

    def reconnect(self) -> None:
        """Require fresh authority data; keep revision and rollback protection."""
        with self._lock:
            self._sync = None
            self._definition_available = not bool(self.definition_topic)

    def apply_time(self, now_ms: float, *, retained: bool = False) -> None:
        if self.source != "mqtt":
            return  # Host/NTP mode never receives a second offset.
        if retained:
            raise ValueError("time beacons must not be retained")
        if isinstance(now_ms, bool) or not isinstance(now_ms, (int, float)) or not math.isfinite(now_ms):
            raise ValueError("time beacon must contain finite now_ms")
        with self._lock:
            self._sync = (now_ms / 1000, self._monotonic())

    def apply_definition(self, definition: ClockDefinition) -> bool:
        validate_definition(definition)
        if self.definition_topic and definition.id != self.definition_topic.rsplit("/", 1)[1]:
            raise ValueError("clock id does not match selected topic")
        with self._lock:
            previous = self._definition
            if previous:
                if definition.run_id != previous.run_id or definition.id != previous.id:
                    raise ValueError("new clock run requires explicit consumer restart/reset")
                if definition.revision < previous.revision:
                    return False
                if definition.revision == previous.revision:
                    if definition != previous:
                        raise ValueError("conflicting clock definitions at the same revision")
                    self._definition_available = True
                    return False
            self._previous_definition = previous
            self._definition = deepcopy(definition)
            self._definition_available = True
            return True

    def remove_definition(self) -> None:
        """A tombstone stops the timeline; it does not switch to wall time."""
        with self._lock:
            self._definition_available = False

    def real_now(self) -> float:
        with self._lock:
            if self.source == "local":
                return self._wall()
            if self._sync is None:
                raise ClockNotReady("waiting for a live hub-time beacon")
            epoch, received = self._sync
            elapsed = self._monotonic() - received
            if elapsed > self.max_sync_age:
                raise ClockNotReady("hub-time beacon is stale")
            return epoch + elapsed

    def now(self) -> float:
        scheduled = self._scheduled.get()
        if scheduled is not None and (scheduled[1] is None or scheduled[1] is _current_task()):
            return scheduled[0]
        with self._lock:
            if not self._definition_available:
                raise ClockNotReady("waiting for clock definition")
            real = self.real_now()
            definition = self._active(real)
            value = project(definition, real) if definition else real
            # A negative correction must never rewrite already emitted history.
            # Expose the problem until real/factory time catches up; do not make
            # up duplicate timestamps by clamping them to the previous sample.
            if self._last is not None and value < self._last - 1e-6:
                raise ClockNotReady("clock moved behind the last emitted timestamp")
            self._last = value
            return value

    def _active(self, real: float) -> ClockDefinition | ClockSegment | None:
        if self._definition and real < self._definition.real_anchor and self._definition.previous:
            previous = self._definition.previous
            return previous if isinstance(previous, ClockSegment) else ClockSegment(**previous)
        if self._definition and real < self._definition.real_anchor and self._previous_definition:
            return self._previous_definition
        return self._definition

    @contextmanager
    def at(self, timestamp: float) -> Iterator[None]:
        """Evaluate one historical/scheduled callback at its exact event time.

        Context-local: other threads/tasks continue seeing the live clock. The
        scheduler owns the timestamp and progress, not a global clock rewind.
        Entered in a task, it holds for that task only: a task it starts (a
        trailing run, a retry) runs later, on the live clock.
        """
        if not math.isfinite(timestamp):
            raise ValueError("scheduled timestamp must be finite")
        token = self._scheduled.set((timestamp, _current_task()))
        try:
            yield
        finally:
            self._scheduled.reset(token)

    @property
    def definition(self) -> ClockDefinition | None:
        with self._lock:
            return deepcopy(self._definition)

    @property
    def rate(self) -> float:
        with self._lock:
            real = self.real_now()
            definition = self._active(real)
            if definition is None:
                return 1.0
            if real < definition.real_anchor:
                return 0.0
            value = project(definition, real)
            if definition.stop_at is not None and value >= definition.stop_at:
                return 0.0
            if definition.rate and definition.catch_up and value >= real:
                return 1.0
            return definition.rate

    def status(self) -> ClockStatus:
        with self._lock:
            definition = self._definition
            age = self._monotonic() - self._sync[1] if self._sync else None
            try:
                now, rate, reason = self.now(), self.rate, ""
            except ClockNotReady as exc:
                now, rate, reason = None, 0.0, str(exc)
            return ClockStatus(
                self.source,
                not reason,
                age,
                definition.run_id if definition else None,
                definition.revision if definition else None,
                rate,
                now,
                reason,
            )

    def sleep(self, seconds: float, *, stop: threading.Event | None = None) -> None:
        """Blocking factory-time wait for a worker thread; never a socket timeout."""
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("seconds must be non-negative and finite")
        target = None
        while stop is None or not stop.is_set():
            try:
                now = self.now()
                if target is None:
                    target = now + seconds
                remaining = target - now
                if remaining <= 0:
                    return
                rate = self.rate
                delay = min(0.1, remaining / rate) if rate else 0.1
            except ClockNotReady:
                delay = 0.1
            if stop is not None:
                stop.wait(delay)
            else:
                time.sleep(delay)

    async def sleep_until(self, timestamp: float, *, responsiveness: float = 0.1) -> None:
        """Wait in factory seconds, responding to pause, speed and sync changes.

        Does not schedule network deadlines. Cancellation works while paused or
        waiting for authority. A bounded real wait also avoids busy spinning.
        """
        if not math.isfinite(timestamp) or not math.isfinite(responsiveness) or responsiveness <= 0:
            raise ValueError("finite timestamp and positive responsiveness required")
        while True:
            try:
                remaining = timestamp - self.now()
                if remaining <= 0:
                    return
                rate = self.rate
                wait = min(responsiveness, remaining / rate) if rate else responsiveness
            except ClockNotReady:
                wait = responsiveness
            await asyncio.sleep(wait)
