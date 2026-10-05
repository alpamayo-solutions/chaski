"""``Service.consume``: follow a stream with a handler that may fail.

:meth:`chaski.door.Stream.follow` leaves failures to the caller. This runner
owns them: a handler that raises is not acknowledged, the cursor is moved only
past the records before it, and the same record is retried with bounded,
jittered backoff while :class:`chaski.failures.HandlerHealth` reports the
consumer degraded, then unhealthy. A handler that raises
:class:`chaski.failures.Reject` has its record recorded as rejected before
the cursor passes it.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from .door import Record, Stream, StreamGapError
from .doorbell import IDLE_DRAIN_S
from .failures import HandlerHealth, Reject, record_subject
from .outage import Outage, expected_failure
from .retry import Backoff

log = logging.getLogger("chaski.consume")

RejectFn = Callable[[str, dict[str, Any], Reject], None]


class _HandlerFailed(Exception):
    def __init__(self, record: Record, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.record = record
        self.cause = cause


def consume(
    stream: Stream,
    handler: Callable[[Record], Any],
    *,
    health: HandlerHealth,
    reject: RejectFn,
    bell: Any = None,
    stop: threading.Event | None = None,
    consumer: str | None = None,
    retry: Backoff | None = None,
    idle_drain_s: float | None = IDLE_DRAIN_S,
) -> None:
    """Drain ``stream`` through ``handler`` now and after every ring of
    ``bell`` (a stream watch when omitted), until ``stop`` is set. Setting
    ``stop`` alone ends it; no ring is needed.

    After ``idle_drain_s`` without a ring it drains anyway, so a filtered
    cursor whose topics stay silent still moves past the records it skips
    (:data:`chaski.doorbell.IDLE_DRAIN_S`; ``None`` waits for the bell alone).

    A pruned range raises :class:`chaski.door.StreamGapError`; a failed
    fetch or ack is retried with the same backoff as a failed handler. A
    retry waiting out its backoff runs at once when the node's link comes back
    (the door's ``link_up``), and the backoff starts over.

    Without a ``bell`` each stream-change hint's head bounds the drain, and a
    hint the cursor already passed costs no request
    (:class:`chaski.stream_changes.StreamChange`).
    """
    stop = stop or threading.Event()
    name = consumer or stream.cursor
    backoff = retry or Backoff()
    reading = Outage(log, f"{name}: reading {stream.name}")
    handling = Outage(log, name, recovered_as="succeeded again")
    watch = None
    if bell is None:
        from .stream_changes import StreamChanges

        watch = StreamChanges(stream._door, [stream.name], stop=stop).start()
        bell = watch[stream.name]
    # The node's link coming back ends a retry wait; the backoff spaces
    # retries while it stays up.
    link = getattr(stream._door, "link_up", None)

    def retry_after(delay: float, since: int) -> None:
        if link is None:
            stop.wait(delay)
        elif link.wait_after(since, delay, stop=stop):
            backoff.reset()

    # The subscription the last complete drain started from (see StreamChange.covers).
    drained_on = None
    try:
        while not stop.is_set():
            seen = bell.generation
            link_seen = link.generation if link is not None else 0
            change = watch.latest(stream.name) if watch is not None else None
            if change is not None and change.covers(stream.position, drained_on):
                # The hint's head is behind the cursor already: nothing to fetch.
                bell.wait_after(seen, idle_drain_s, stop=stop)
                continue
            try:
                _drain(stream, handler, stop, health, reject, name, None if change is None else change.head)
            except StreamGapError:
                raise
            except _HandlerFailed as failed:
                count = health.failed(name, failed.cause)
                delay = backoff.delay(failed.cause)
                if not handling.failed(failed.cause, delay=delay):
                    log.error(
                        "%s failed at offset=%d (%d in a row); not acknowledged, retrying in %.1fs",
                        name,
                        failed.record.offset,
                        count,
                        delay,
                        exc_info=failed.cause,
                    )
                retry_after(delay, link_seen)
                continue
            except Exception as exc:
                delay = backoff.delay(exc)
                if not reading.failed(exc, delay=delay):
                    log.error("%s: reading %s failed; retrying in %.1fs", name, stream.name, delay, exc_info=exc)
                retry_after(delay, link_seen)
                continue
            if change is not None:
                drained_on = change.subscription
            backoff.reset()
            reading.recovered()
            handling.recovered()
            if stop.is_set():
                return
            # Setting stop ends the wait too: a consumer rescoped to nothing
            # has a bell that nothing rings any more.
            bell.wait_after(seen, idle_drain_s, stop=stop)
    finally:
        if watch is not None:
            watch.close()


def _drain(
    stream: Stream,
    handler: Callable[[Record], Any],
    stop: threading.Event,
    health: HandlerHealth,
    reject: RejectFn,
    name: str,
    head: int | None = None,
) -> None:
    current: Record | None = None
    try:
        for record in stream.drain(stop=stop, head=head):
            current = record
            try:
                handler(record)
            except Reject as rejected:
                reject(name, {"stream": stream.name, "cursor": stream.cursor, **record_subject(record)}, rejected)
            health.succeeded(name)
            current = None
    except Exception as exc:
        if current is None:
            raise
        failed = _HandlerFailed(current, exc)
        # Everything before the failed record was handled: a restart resumes at it.
        if current.offset > 1:
            try:
                stream.ack(current.offset - 1)
            except Exception as ack_error:
                log.warning(
                    "%s: could not acknowledge up to offset=%d: %s",
                    name,
                    current.offset - 1,
                    ack_error,
                    exc_info=expected_failure(ack_error) is None,
                )
        raise failed from exc
