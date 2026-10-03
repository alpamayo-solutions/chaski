"""Logging a retried failure the node causes, without filling the log.

Colca restarts, a node answers ``503`` or ``429`` for a while, a broker link
drops: every retry loop in chaski meets these and retries them. They are
expected, so they are not logged like a defect. :class:`Outage` logs such a
condition as a state:

* one WARNING when it starts, with the reason;
* while it lasts, one DEBUG line per retry and an INFO
  ``still waiting (N attempts, M s)`` line every ``remind_every`` seconds;
* one INFO line when it recovers: ``reached Colca after N failed attempts / M s``
  (a handler or timer says ``succeeded again`` instead: its failure may come
  from something other than Colca).

None of these carries a traceback. Any other exception is not expected:
:meth:`Outage.failed` returns False for it and the caller logs it as before,
at ERROR with its traceback.

What counts as expected is :func:`expected_failure`.
"""

from __future__ import annotations

import errno
import logging
import socket
import threading
import time
import urllib.error
from collections.abc import Callable

import httpx
from franzmq.errors import PublishRejected, PublishTimeout

#: How often a lasting outage logs that it is still waiting.
REMIND_EVERY_S = 60.0


class ColcaUnavailable(RuntimeError):
    """State the node keeps is not available right now (its subscription is
    recovering, the link is down). Retried; logged as an outage."""

    colca_unavailable = True


def expected_failure(exc: BaseException) -> str | None:
    """Why ``exc`` is an expected, retried condition, or None when it is not.

    Expected: Colca cannot be reached (a refused or reset connection, a name
    that does not resolve, a timeout, a dropped stream), it answers
    ``408``/``425``/``429``/``5xx``, a durable queue is full until the node
    takes it again (``BufferError``), or an exception says the node's state is
    unavailable (``colca_unavailable``). An exception raised ``from`` one of
    these counts too. Anything else (a ``4xx``, a bug in a handler) is not.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _expected(current):
            return _describe(current)
        current = current.__cause__
    return None


#: PUBACK reason codes that mean "not now": quota exceeded (a draining
#: destination), server busy.
TRANSIENT_PUBACK = frozenset({0x89, 0x97})

#: OSError numbers that say the node is not reachable over the network.
_UNREACHABLE_ERRNOS = frozenset({errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ENETDOWN, errno.EHOSTDOWN})


def _expected(exc: BaseException) -> bool:
    from .startup import BrokerRefused, transient_status

    if getattr(exc, "colca_unavailable", False):
        return True
    if isinstance(exc, urllib.error.HTTPError):
        return transient_status(exc.code)
    if isinstance(exc, httpx.HTTPStatusError):
        return transient_status(exc.response.status_code)
    if isinstance(exc, BrokerRefused):
        return exc.transient
    if isinstance(exc, PublishTimeout):
        return True  # no PUBACK: the broker is away or too slow
    if isinstance(exc, PublishRejected):
        return exc.reason_code in TRANSIENT_PUBACK
    if isinstance(exc, BufferError):
        return True  # a durable queue is full: backpressure until the node takes it again
    # Not every OSError: a missing file or a refused permission in a handler
    # is a defect, not a node that is away.
    if isinstance(exc, (ConnectionError, TimeoutError, socket.gaierror, urllib.error.URLError, httpx.TransportError)):
        return True
    return isinstance(exc, OSError) and exc.errno in _UNREACHABLE_ERRNOS


def _describe(exc: BaseException) -> str:
    text = str(exc)
    name = type(exc).__name__
    return f"{name}: {text}"[:300] if text else name


def warn_failure(logger: logging.Logger, exc: BaseException, message: str, *args: object) -> None:
    """Log a failed best-effort step (a status publish, a progress report) at
    WARNING: with the reason in one line when it is expected, with the
    traceback when it is not."""
    reason = expected_failure(exc)
    if reason is None:
        logger.warning(message, *args, exc_info=exc)
    else:
        logger.warning(message + ": %s", *args, reason)


class Outage:
    """One retried operation's outage, logged as a state (see the module).

    Call :meth:`failed` after each failed attempt and :meth:`recovered`
    after a success. Thread-safe.
    """

    def __init__(
        self,
        logger: logging.Logger,
        what: str,
        *,
        remind_every: float = REMIND_EVERY_S,
        now: Callable[[], float] = time.monotonic,
        recovered_as: str = "reached Colca",
    ) -> None:
        self.logger = logger
        self.what = what
        self.recovered_as = recovered_as
        self.remind_every = float(remind_every)
        self._now = now
        self._lock = threading.Lock()
        self.since: float | None = None
        self.attempts = 0
        self._reminded_at = 0.0

    @property
    def active(self) -> bool:
        """Whether the condition is still going on."""
        return self.since is not None

    def failed(self, exc: BaseException, *, delay: float | None = None, reason: str | None = None) -> bool:
        """Account for one failed attempt.

        Returns True when ``exc`` is expected (:func:`expected_failure`) and
        was logged here; False when it is not, and the caller logs it with its
        traceback. ``reason`` replaces the exception's text in the log.
        """
        found = expected_failure(exc)
        if found is None:
            return False
        self.note(reason or found, delay=delay)
        return True

    def note(self, reason: str, *, delay: float | None = None) -> None:
        """Account for one failed attempt whose cause is known to be expected."""
        with self._lock:
            now = self._now()
            self.attempts += 1
            if self.since is None:
                self.since = self._reminded_at = now
                level, text = logging.WARNING, f"{reason}; retrying until it answers"
            elif now - self._reminded_at >= self.remind_every:
                self._reminded_at = now
                level, text = (
                    logging.INFO,
                    f"still waiting ({self.attempts} attempts, {now - self.since:.0f} s): {reason}",
                )
            else:
                level = logging.DEBUG
                text = f"attempt {self.attempts} failed: {reason}" + (
                    "" if delay is None else f"; next in {delay:.1f}s"
                )
        self.logger.log(level, "%s: %s", self.what, text)

    def recovered(self) -> None:
        """The operation succeeded; log the recovery once if it was failing."""
        with self._lock:
            if self.since is None:
                return
            attempts, lasted = self.attempts, self._now() - self.since
            self.since, self.attempts = None, 0
        self.logger.info("%s: %s after %d failed attempts / %.0f s", self.what, self.recovered_as, attempts, lasted)
