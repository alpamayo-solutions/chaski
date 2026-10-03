"""What happens when a handler raises: retry, never acknowledge, report.

A handler that raises is not acknowledged. Its input stays pending, and the
runner retries the same input with bounded, jittered backoff
(:class:`chaski.retry.Backoff`). Each consumer counts its consecutive
failures in :class:`HandlerHealth`: one failure makes the service
``degraded``, ``unhealthy_after`` of them make it ``unhealthy``. The first
success clears the count. A failure is logged with its traceback, unless it is
an expected, retried condition such as Colca being away (:mod:`chaski.outage`).

To pass an input on purpose, a handler raises :class:`Reject`. The runner
records the rejection durably, as the service's ``rejected_input``
``_Finding`` (:func:`rejection_finding`), and only then acknowledges it.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .outage import warn_failure

log = logging.getLogger("chaski.failures")

#: Consecutive failures of one consumer after which the service is unhealthy.
UNHEALTHY_AFTER = 5

#: The ``_Finding`` name a rejection is recorded under, at the service's element.
REJECTED_FINDING = "rejected_input"

OK, DEGRADED, UNHEALTHY = "ok", "degraded", "unhealthy"


class Reject(Exception):
    """Raise from a handler to set its input aside on purpose.

    The runner records the rejection durably (the service's
    ``rejected_input`` finding), then acknowledges the input and goes on with
    the next one. Use it for input that can never succeed, such as a record
    that does not decode; any other exception is retried.
    """

    def __init__(self, reason: str, *, detail: dict[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Failing:
    """One consumer that is failing: how often in a row, the last error, and
    since when (unix seconds)."""

    failures: int
    error: str
    since: float


class HandlerHealth:
    """Consecutive handler failures per consumer, and the status they make.

    Thread-safe. ``on_change(status, summary)`` is called whenever the status
    or the set of failing consumers changes, from the thread that reported it.
    """

    def __init__(
        self,
        *,
        unhealthy_after: int = UNHEALTHY_AFTER,
        on_change: Callable[[str, str], None] | None = None,
    ) -> None:
        if unhealthy_after < 1:
            raise ValueError("unhealthy_after must be at least 1")
        self.unhealthy_after = unhealthy_after
        self.on_change = on_change
        self._lock = threading.Lock()
        self._failing: dict[str, Failing] = {}

    def failed(self, consumer: str, error: BaseException) -> int:
        """Count one more failure of ``consumer``; returns how many in a row."""
        text = f"{type(error).__name__}: {error}"[:200]
        with self._lock:
            before = self._status()
            previous = self._failing.get(consumer)
            count = previous.failures + 1 if previous else 1
            self._failing[consumer] = Failing(count, text, previous.since if previous else time.time())
            after = self._status()
            changed = previous is None or before != after
        if changed:
            self._changed()
        return count

    def succeeded(self, consumer: str) -> None:
        """``consumer`` handled its input; its failure count is cleared."""
        with self._lock:
            if self._failing.pop(consumer, None) is None:
                return
        self._changed()

    def _status(self) -> str:
        if not self._failing:
            return OK
        if max(f.failures for f in self._failing.values()) >= self.unhealthy_after:
            return UNHEALTHY
        return DEGRADED

    @property
    def status(self) -> str:
        """``ok``, ``degraded`` (a consumer is failing) or ``unhealthy`` (one
        failed ``unhealthy_after`` times in a row)."""
        with self._lock:
            return self._status()

    def failing(self) -> dict[str, Failing]:
        with self._lock:
            return dict(self._failing)

    def summary(self) -> str:
        """One line naming the failing consumers, ``""`` while none fails."""
        with self._lock:
            return "; ".join(f"{name} failed {f.failures}x: {f.error}" for name, f in sorted(self._failing.items()))

    def _changed(self) -> None:
        if self.on_change is None:
            return
        try:
            self.on_change(self.status, self.summary())
        except Exception as exc:
            warn_failure(log, exc, "Could not report handler health")


def rejection_finding(consumer: str, subject: dict[str, Any], reject: Reject, *, rejected: int) -> dict[str, Any]:
    """The ``_Finding`` payload that records one rejection.

    ``subject`` says what was rejected (stream, offset, topic, ...).
    ``rejected`` is how many inputs this service has rejected since it
    started, so a reader sees the record is not the only one.
    """
    detail: dict[str, Any] = {"consumer": consumer, "rejected": rejected, **subject}
    if reject.detail:
        detail["reject"] = reject.detail
    return {
        "reason": REJECTED_FINDING,
        "summary": f"{consumer} set an input aside: {reject.reason}"[:300],
        "observed_at": time.time(),
        "suggested_severity": "warning",
        "detail": detail,
        "remedy": "Correct the input at its source; the rejected record stays in the stream until retention.",
    }


def record_subject(record: Any) -> dict[str, Any]:
    """What identifies a stream record in a rejection."""
    return {
        "offset": getattr(record, "offset", None),
        "topic": getattr(record, "topic", None),
        "ts": getattr(record, "ts", None),
    }
