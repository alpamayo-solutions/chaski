"""Durable progress of ``@every``/``@cron`` ticks, and the instants they missed.

Each tick records, in the buffer (``tick_progress``), the last instant it ran
for. A tick that finds instants between that one and its own that it did not
run, and the service at start for the time it was down, has **missed** them:
a stalled producer (a long callback, a blocked worker), a stopped service,
an edge without power.

Instants missed by less than :data:`GRACE_S` are coalesced, as a scheduler
that runs late does (APScheduler's ``coalesce`` and ``misfire_grace_time``):
nothing is reported and nothing runs again. Older ones are reported as the
service's retained ``missed_ticks`` ``_Finding`` and on the health door.

**Catch-up is the producer's choice.** A producer that sets
``catch_up_ticks = True`` has its missed instants run later, each at its
instant (``self.now`` is the instant), through the backfill runner: a job
``ticks-<method>-<first instant>`` over the missed range, on an instance of
its own (like a repair, :mod:`chaski.dataops.backfill`), after the ingest has
caught up with the stream so the windows the ticks read are complete. A tick
that fails during catch-up is reported and the job goes on, as a live tick
that fails is followed by the next. The default is off, because a tick that
publishes at the wall clock, or keeps state from tick to tick, does the wrong
thing when it runs for a past instant: such a tick must use ``self.now`` and
read its state from its windows before it opts in.

Interval ticks fire on multiples of their interval since the epoch, live and
in catch-up alike; cron ticks in the host's time zone, as the scheduler runs
them live.
"""

from __future__ import annotations

import datetime
import json
import logging
import math
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from chaski.outage import warn_failure

from .triggers import CronSpec, IntervalSpec

if TYPE_CHECKING:
    from .base import Producer

log = logging.getLogger("chaski.dataops.ticks")

#: Instants missed by less than this are coalesced: not reported, not run again.
GRACE_S = 60.0
#: The job-id prefix of a catch-up (:mod:`chaski.dataops.backfill`).
TICKS = "ticks-"
#: The retained ``_Finding`` that lists missed ticks.
FINDING = "missed_ticks"
#: A missed span that is not caught up stays in the finding this long.
KEEP_S = 24 * 3600.0

Spec = CronSpec | IntervalSpec


def live_trigger(spec: Spec, now: float | None = None) -> CronTrigger | IntervalTrigger:
    """The scheduler trigger of a live tick: an interval aligned to multiples
    of itself since the epoch, a cron expression in the host's time zone."""
    if isinstance(spec, IntervalSpec):
        now = time.time() if now is None else now
        start = math.floor(now / spec.seconds) * spec.seconds
        return IntervalTrigger(seconds=spec.seconds, start_date=datetime.datetime.fromtimestamp(start, datetime.UTC))
    return CronTrigger.from_crontab(spec.expression)


def instants(spec: Spec, after: float, until: float, *, limit: int | None = None) -> list[float]:
    """Every instant of ``spec`` with ``after < t <= until``, oldest first (at
    most ``limit``, the newest ones kept)."""
    if until <= after:
        return []
    if isinstance(spec, IntervalSpec):
        step = spec.seconds
        first = (math.floor(after / step) + 1) * step
        last = math.floor(until / step) * step
        if last < first:
            return []
        count = round((last - first) / step) + 1
        if limit is not None and count > limit:
            first = last - (limit - 1) * step
            count = limit
        return [first + i * step for i in range(count)]
    trigger = CronTrigger.from_crontab(spec.expression)
    tz = trigger.timezone
    previous = datetime.datetime.fromtimestamp(after, tz)
    fires: list[float] = []
    while True:
        fire = trigger.get_next_fire_time(previous, previous + datetime.timedelta(microseconds=1))
        if fire is None:
            break
        ts = fire.timestamp()
        if ts > until:
            break
        if ts > after:
            fires.append(ts)
            if limit is not None and len(fires) > limit:
                fires.pop(0)
        previous = fire
    return fires


def due(spec: Spec, now: float) -> float:
    """The instant a live tick firing at ``now`` runs for: the newest instant
    of ``spec`` at or before ``now`` (``now`` itself where none is found within
    a day, for a cron expression that fires rarely)."""
    if isinstance(spec, IntervalSpec):
        return math.floor(now / spec.seconds) * spec.seconds
    found = instants(spec, now - 86400.0, now, limit=1)
    return found[-1] if found else now


def catches_up(producer: Producer | type[Producer]) -> bool:
    """Whether ``producer`` runs its missed tick instants later."""
    cls = producer if isinstance(producer, type) else type(producer)
    return bool(getattr(cls, "catch_up_ticks", False))


class MissedTicks:
    """The service's account of missed tick instants: what it reports (the
    ``missed_ticks`` finding, the health door) and the catch-up jobs it
    requests."""

    def __init__(
        self,
        buffer: Any,
        *,
        request: Callable[[str, float, float, str], str] | None = None,
        publish: Callable[[str, dict[str, Any] | None], None] | None = None,
    ) -> None:
        self._buffer = buffer
        self._request = request
        self._publish = publish
        self._lock = threading.Lock()
        #: ``(producer, tick) -> span`` of the latest miss of each tick.
        self._spans: dict[tuple[str, str], dict[str, Any]] = {}
        self._published: str | None = None

    # -- recording ------------------------------------------------------------

    def ran(self, producer: Producer, tick: str, spec: Spec, at: float) -> None:
        """A live tick of ``producer`` ran for the instant ``at``: record it,
        and account for the instants it skipped since the one before."""
        last = self._buffer.tick_progress(producer.name, tick)
        if last is not None and at > last:
            self.account(producer, tick, spec, last, at)
        self._buffer.set_tick_progress(producer.name, tick, at, durable=False)

    def account(self, producer: Producer, tick: str, spec: Spec, after: float, before: float) -> int:
        """The instants of ``spec`` in ``(after, before)`` older than
        :data:`GRACE_S` before ``before`` were missed: report them, and
        record their catch-up if ``producer`` takes one. Returns how many."""
        horizon = before - GRACE_S
        missed = [t for t in instants(spec, after, before) if t < before and t <= horizon]
        if not missed:
            return 0
        name = producer.name
        job = None
        if catches_up(producer) and self._request is not None:
            try:
                job = self._request(name, missed[0], math.nextafter(missed[-1], math.inf), tick)
            except Exception as exc:
                warn_failure(log, exc, f"Could not record the catch-up of {name}.{tick}")
        log.warning(
            "%s.%s missed %d tick(s) from %s to %s; %s",
            name,
            tick,
            len(missed),
            _iso(missed[0]),
            _iso(missed[-1]),
            f"catching up ({job})" if job else "not run again (the producer does not catch up ticks)",
        )
        with self._lock:
            self._spans[(name, tick)] = {
                "producer": name,
                "tick": tick,
                "missed": len(missed),
                "from": _iso(missed[0]),
                "to": _iso(missed[-1]),
                "catch_up": job,
                "observed_at": time.time(),
            }
        self.refresh()
        return len(missed)

    # -- reporting ------------------------------------------------------------

    def status(self) -> list[dict[str, Any]]:
        """The missed spans still reported, for the health door."""
        with self._lock:
            return [dict(span) for span in self._spans.values()]

    def uncaught(self) -> list[str]:
        """``producer.tick`` of each reported miss that is not caught up."""
        with self._lock:
            return sorted(f"{s['producer']}.{s['tick']}" for s in self._spans.values() if not s["catch_up"])

    def refresh(self, now: float | None = None) -> None:
        """Drop spans whose catch-up is done or that are older than
        :data:`KEEP_S`, and publish the finding as it now stands (retired
        when nothing is left)."""
        now = time.time() if now is None else now
        pending = {job["job"] for job in self._buffer.backfill_jobs(pending_only=True)}
        with self._lock:
            for key, span in list(self._spans.items()):
                if (span["catch_up"] and span["catch_up"] not in pending) or (
                    not span["catch_up"] and now - span["observed_at"] > KEEP_S
                ):
                    del self._spans[key]
            spans = [dict(span) for span in self._spans.values()]
        if self._publish is None:
            return
        payload: dict[str, Any] | None = None
        if spans:
            total = sum(span["missed"] for span in spans)
            uncaught = [s for s in spans if not s["catch_up"]]
            payload = {
                "reason": FINDING,
                "summary": (
                    f"{total} tick(s) of {len(spans)} @every/@cron trigger(s) were missed; "
                    f"{len(spans) - len(uncaught)} are being caught up, {len(uncaught)} are not run again"
                )[:300],
                "observed_at": now,
                "suggested_severity": "warning",
                "detail": {"missed": spans},
                "remedy": (
                    "None needed for the ones being caught up. The others: set catch_up_ticks = True on a "
                    "producer whose ticks compute from self.now and their windows, or repair the range with "
                    "a backfill request."
                ),
            }
        compare = json.dumps(spans, sort_keys=True) if spans else None
        if compare == self._published:
            return
        try:
            self._publish(FINDING, payload)
            self._published = compare
        except Exception as exc:
            warn_failure(log, exc, "Could not publish the missed_ticks finding")


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(ts, datetime.UTC).isoformat(timespec="seconds")


__all__ = ["FINDING", "GRACE_S", "TICKS", "MissedTicks", "catches_up", "due", "instants", "live_trigger"]
