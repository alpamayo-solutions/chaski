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
from .failures import HandlerHealth, Reject, record_subject
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
) -> None:
    """Drain ``stream`` through ``handler`` now and after every ring of
    ``bell`` (a stream watch when omitted), until ``stop`` is set.

    A pruned range raises :class:`chaski.door.StreamGapError`; a failed
    fetch or ack is retried with the same backoff as a failed handler.
    """
    stop = stop or threading.Event()
    name = consumer or stream.cursor
    backoff = retry or Backoff()
    watch = None
    if bell is None:
        from .stream_changes import StreamChanges

        watch = StreamChanges(stream._door, [stream.name], stop=stop).start()
        bell = watch[stream.name]
    try:
        while not stop.is_set():
            seen = bell.generation
            try:
                _drain(stream, handler, stop, health, reject, name)
            except StreamGapError:
                raise
            except _HandlerFailed as failed:
                count = health.failed(name, failed.cause)
                delay = backoff.delay(failed.cause)
                log.error(
                    "%s failed at offset=%d (%d in a row); not acknowledged, retrying in %.1fs",
                    name,
                    failed.record.offset,
                    count,
                    delay,
                    exc_info=failed.cause,
                )
                stop.wait(delay)
                continue
            except Exception as exc:
                delay = backoff.delay(exc)
                log.warning("%s: reading %s failed (%s); retrying in %.1fs", name, stream.name, exc, delay)
                stop.wait(delay)
                continue
            backoff.reset()
            if stop.is_set():
                return
            bell.wait_after(seen)
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
) -> None:
    current: Record | None = None
    try:
        for record in stream.drain(stop=stop):
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
            except Exception:
                log.warning("%s: could not acknowledge up to offset=%d", name, current.offset - 1, exc_info=True)
        raise failed from exc
