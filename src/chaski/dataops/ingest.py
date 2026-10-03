"""Single-lane stream ingest over colca's ``metrics`` stream.

One durable cursor per buffer generation (``ingest-<generation>`` in the
service's cursor namespace) walks the stream forward, appends every record to
the local :class:`~chaski.dataops.Buffer`, and runs the ``@on_metric`` handlers
bound to its ``signal_id``, in stream order, live and on replay alike. A new
buffer generation starts a new cursor at offset 1, so cold start, recovery and
replay are one code path.

The cursor is a :class:`chaski.door.Stream`, the consume lane every service
uses. The loop opens it again whenever the signal filter changes
(:meth:`Ingest.rebind`); the cursor name, and so its position, stays.

**Crash safety.** Per page: append and run handlers, then ack. A crash before
the ack redelivers the page, and appends are idempotent on
``(signal_id, ts)``. Handlers must be idempotent for the same reason.

**A failed handler is not acknowledged.** The cursor moves only past the
records before it, and :meth:`Ingest.run_forever` retries the same record
with bounded, jittered backoff, reporting the handler to
:class:`chaski.failures.HandlerHealth` (degraded, then unhealthy) and logging
each failure. A handler that raises :class:`chaski.Reject` has the record
recorded as rejected (``reject``) and the page goes on.

**Pacing.** :meth:`Ingest.run_forever` fetches again at once after a full
page, as the loop is behind. After a partial page it waits until
:data:`MIN_FETCH_INTERVAL_S` has passed since the previous fetch, so a busy
stream is read in larger pages instead of more requests.

**Read-ahead.** On a node that reads ahead of a cursor (colca 0.18.2+,
``/fetch?from=``), the page after a full one is fetched while the full one is
processed, and acks go out in the background: a page is still acked only
after it was processed, acks only move forward, and while one is in flight
later pages collapse into a single ack of the newest offset. A failed ack or
fetch, and a :meth:`Ingest.rebind`, restart the read at the acked cursor. On
older nodes the loop fetches, processes and acks in turn.

**Gaps.** When the cursor fell below the stream's low-water mark, colca
returns a ``gap`` with the surviving records. Processing fails visibly rather than acknowledging missing history.

**MQTT only wakes it.** A message on one of the service's input topics calls
:meth:`Ingest.wake` (through
:meth:`chaski.dataops.service.DataOpsService._wake_on_inputs`); the message
itself is not read. So does every reconnect of the broker link. The loop
drains at start, then waits for the next wake. It takes the wake generation
before each drain, so a wake that arrives while it drains leads to one more
drain instead of being lost. A consumer that stops reading anyway is caught by
the node's cursor watchdog, which the health door reports
(:mod:`chaski.dataops.health`).

**A silent filter still moves.** The wake comes from the signals the loop
reads, so while none of them changes it would never drain, and its cursor
would hold the node's retention of the whole stream. After ``idle_drain_s``
(:data:`chaski.doorbell.IDLE_DRAIN_S`) without a wake it drains anyway; the
pages it walks hold none of its signals and are acked past.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from random import SystemRandom
from typing import Any

import httpx

from chaski.door import Gap, Page, Record, Stream, StreamGapError
from chaski.doorbell import IDLE_DRAIN_S, Doorbell
from chaski.failures import HandlerHealth, Reject, record_subject
from chaski.outage import Outage
from chaski.retry import Backoff

from .buffer import Buffer

log = logging.getLogger("chaski.dataops.ingest")

# Backoff jitter; SystemRandom only keeps security scanners quiet.
_jitter = SystemRandom()

STREAM = "metrics"

# A handler receives the raw Record; whoever builds the dispatch table decodes
# ``record.payload``.
Handler = Callable[[Record], Awaitable[None]]

#: ``open_stream(cursor, signal_ids) -> Stream``: the service's own
#: ``stream("metrics", cursor=cursor, signal_ids=signal_ids)``, which puts
#: the cursor inside the identity's namespace (``Service.cursor_prefix``).
OpenStream = Callable[[str, "list[str] | None"], Stream]

#: ``reject(consumer, subject, rejection)``: records a rejection durably and
#: returns only then (:meth:`chaski.Service.reject`).
RejectFn = Callable[[str, "dict[str, Any]", Reject], None]


def consumer_name(handler: Any) -> str:
    """How health and logs name a handler: ``Producer.method`` where known."""
    return getattr(handler, "consumer", None) or getattr(handler, "__qualname__", None) or repr(handler)


#: While pages come back partial, fetches are at least this far apart, so a
#: busy stream is read in larger pages rather than with more requests. It is
#: also the most a caught-up record waits for its fetch.
MIN_FETCH_INTERVAL_S = 0.1


def cursor_name(generation: str) -> str:
    """The generational ingest cursor name for one buffer generation —
    relative to the service's cursor namespace."""
    return f"ingest-{generation}"


class Ingest:
    """Fetch, append, dispatch, ack: the loop over one colca stream.

    Owns the cursor for ``buffer.generation``. ``dispatch`` maps a
    ``signal_id`` to its handlers; ``signal_ids`` are sent with the fetch so
    the door filters, and the buffer only holds declared signals.
    """

    def __init__(
        self,
        open_stream: OpenStream,
        buffer: Buffer,
        *,
        dispatch: dict[str, list[Handler]] | None = None,
        signal_ids: Iterable[str] | None = None,
        previous_generation: str | None = None,
        strict: bool = True,
        min_fetch_interval_s: float = MIN_FETCH_INTERVAL_S,
        retry_min_s: float = 1.0,
        health: HandlerHealth | None = None,
        reject: RejectFn | None = None,
        idle_drain_s: float | None = IDLE_DRAIN_S,
    ) -> None:
        self._retry_min_s = retry_min_s
        self._idle_drain_s = idle_drain_s
        self._health = health or HandlerHealth()
        self._reject = reject
        # (handler, record) of the handler failure that ended the last step.
        self._failed: tuple[str, Record] | None = None
        self.waiting = False
        if not strict:
            raise ValueError("DataOps cannot skip failed handlers; strict=False is no longer supported")
        self._open_stream = open_stream
        self._buffer = buffer
        self._dispatch = dispatch or {}
        self._signal_ids = list(signal_ids) if signal_ids is not None else None
        self._cursor_name = cursor_name(buffer.generation)
        self._stream = open_stream(self._cursor_name, self._signal_ids)
        self._previous_generation = previous_generation
        self._bell = Doorbell()
        self._window_started = time.monotonic()
        self._last_drain_at = self._window_started
        self._window_records = 0
        self._window_drains = 0
        # Read-ahead state (see the module docstring). ``_read_ahead`` is None
        # until the first page says whether the node supports it.
        self._read_ahead: bool | None = None
        self._next_from: int | None = None
        self._prefetch: tuple[Stream, asyncio.Future[Page]] | None = None
        self._ack_want: tuple[Stream, int] | None = None
        self._ack_task: asyncio.Future[None] | None = None
        self._ack_failed = False
        self._ack_error: httpx.HTTPError | BufferError | None = None
        self._behind = False
        self._last_fetch_at = 0.0
        self._min_fetch_interval_s = min_fetch_interval_s
        # Colca away: one warning when the drain starts failing, one line when it recovers.
        self._outage = Outage(log, f"Ingest drain on cursor={self._cursor_name}")
        self._handler_outages: dict[str, Outage] = {}

    def rebind(self, dispatch: dict[str, list], signal_ids: Iterable[str] | None) -> None:
        """Swap what this loop dispatches and fetches, mid-run.

        Producers often start before the signals they read are commissioned.
        Only the table and the filter change; the cursor keeps its position,
        and records a newly resolved signal missed are recovered by replay.
        """
        self._dispatch = dispatch or {}
        self._signal_ids = list(signal_ids) if signal_ids is not None else None
        self._stream = self._open_stream(self._cursor_name, self._signal_ids)
        self._bell.ring()

    @property
    def cursor(self) -> str:
        """The full cursor name at the door (namespace included)."""
        return self._stream.cursor

    @property
    def stream(self) -> Stream:
        return self._stream

    @property
    def last_drain_at(self) -> float:
        """Monotonic time the last drain finished (or the loop was built)."""
        return self._last_drain_at

    # ------------------------------------------------------------------ startup

    def retire_previous_generation(self) -> None:
        """Delete the previous generation's ingest cursor, if any.

        A rebuilt buffer starts a new cursor, and the old one would otherwise
        hold back retention. Deleting a missing cursor succeeds.
        """
        if not self._previous_generation:
            return
        stale = self._open_stream(cursor_name(self._previous_generation), None)
        log.info("Retiring previous-generation cursor %s (stream=%s)", stale.cursor, stale.name)
        stale.retire()

    # ------------------------------------------------------------------ loop

    async def run_once(self) -> int:
        """Fetch one page, process every record, then ack.

        The blocking part of a drain (door HTTP, sqlite) runs in a worker
        thread, which keeps timers, wake and the health door responsive. The
        ``@on_metric`` handlers run on this event loop, the service's own, like
        ``@on_constant`` handlers: whatever a handler schedules there (a
        throttle's trailing run, a retry) outlives the page.

        Returns the number of records processed. A ``gap`` raises
        :class:`StreamGapError`. ``httpx.HTTPError`` propagates for :meth:`run_forever` to
        retry; any other exception ends the task.
        """
        return await asyncio.to_thread(self.drain_once, asyncio.get_running_loop())

    async def run_window(self, target, before_sample, finish_window, *, real_signals=(), clock=None):
        """Durable intake followed by event-time dispatch through a watermark.

        Intake and effects have separate progress. Future samples remain in the
        inbox, so upstream generation need not wait for this consumer. A crash
        after a callback replays that callback; outputs must remain idempotent.
        """
        head = await asyncio.to_thread(self._stream.head)
        while True:
            page = await asyncio.to_thread(self._stream.fetch)
            if page.gap is not None:
                raise StreamGapError(f"input stream has a retention gap: {page.gap}")
            if page.ack_offset is None:
                if page.next <= head:
                    raise RuntimeError("input stream stopped before its captured head")
                break
            await asyncio.to_thread(self._buffer.queue_inputs, page.records, page.ack_offset)
            await asyncio.to_thread(self._stream.ack, page.ack_offset)
            if page.next > head:
                break
        # Infrastructure readings retain real UTC timestamps. Make them
        # available to freshness checks, but dispatch their callbacks at the
        # processed boundary after historical machine records, never in future
        # generation time.
        after = 0
        while records := await asyncio.to_thread(
            self._buffer.input_batch, target, real_signals=real_signals, real=True, after=after
        ):
            for record in records:
                await asyncio.to_thread(self._append, record)
            after = records[-1].offset
        while records := await asyncio.to_thread(self._buffer.input_batch, target, real_signals=real_signals):
            with self._buffer.one_commit():
                for record in records:
                    await before_sample(self._timestamp_of(record))
                    signal_id = await asyncio.to_thread(self._append, record)
                    if signal_id is not None:
                        await self._handle(signal_id, record)
                    await asyncio.to_thread(self._buffer.finish_input, record.offset)
        while records := await asyncio.to_thread(
            self._buffer.input_batch, target, real_signals=real_signals, real=True
        ):
            for record in records:
                signal_id = self._signal_id_of(record)
                if signal_id is not None:
                    with clock.at(target) if clock is not None else contextlib.nullcontext():
                        await self._handle(signal_id, record)
                await asyncio.to_thread(self._buffer.finish_input, record.offset)
        await finish_window(target)

    def drain_once(self, loop: asyncio.AbstractEventLoop) -> int:
        """The thread body of :meth:`run_once`: one page, start to ack. Each
        record's handlers run on ``loop``, one after the other, keeping stream
        order; the page is acked once they are done."""
        stream = self._stream
        page: Page = stream.fetch()
        processed = self._process(page, loop)
        # See Page.ack_offset: the last record, or the gap's bound when nothing
        # survived it.
        ack_offset = page.ack_offset
        if ack_offset is not None:
            stream.ack(ack_offset)
        return processed

    def _process(self, page: Page, loop: asyncio.AbstractEventLoop) -> int:
        """Append every record of ``page`` and run its handlers on ``loop``,
        in stream order. Runs in a worker thread; does not ack."""
        if page.gap is not None:
            raise StreamGapError(f"input stream has a retention gap: {page.gap}")

        self._failed = None
        # One commit per page: the page is acked only after it, and a crash
        # before the ack appends the page again.
        with self._buffer.one_commit():
            for record in page.records:
                signal_id = self._append(record)
                if signal_id is not None and self._dispatch.get(signal_id):
                    asyncio.run_coroutine_threadsafe(self._handle(signal_id, record), loop).result()
        return len(page.records)

    # ------------------------------------------------------------------ read-ahead

    async def _step(self) -> int:
        """One page of :meth:`run_forever`, paced by how full pages come back.

        A full page means the loop is behind: the next fetch follows at once,
        read ahead while this page is processed where the node supports it. A
        partial page means it is caught up: the next fetch waits until
        ``min_fetch_interval_s`` has passed since the previous one, so records
        collect into larger pages instead of more requests.

        Returns the number of records processed, like :meth:`run_once`.
        """
        if self._ack_task is not None and self._ack_task.done():
            self._ack_task.result()  # an ack that failed with more than an HTTP error ends the loop
        stream = self._stream
        if self._ack_failed:
            error = self._ack_error
            await self._restart_from_cursor()
            if error is not None:
                raise error
        elif self._prefetch is not None and self._prefetch[0] is not stream:
            await self._restart_from_cursor()
        loop = asyncio.get_running_loop()

        if self._prefetch is not None:
            pending = self._prefetch[1]
            self._prefetch = None
            page = await pending
        else:
            if not self._behind:
                wait = self._last_fetch_at + self._min_fetch_interval_s - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
            self._last_fetch_at = time.monotonic()
            page = await asyncio.to_thread(stream.fetch, from_offset=self._next_from)

        if page.start is None:
            if self._read_ahead is None:
                log.info(
                    "Node does not read ahead of a cursor; ingest fetches and acks in turn (cursor=%s)", self.cursor
                )
            self._read_ahead = False
        else:
            self._read_ahead = True
            self._next_from = page.next

        ack_offset = page.ack_offset
        if ack_offset is None:
            self._behind = False
            return 0
        # A page of skipped records still moved the read on: read again at once
        # rather than take it for the head.
        self._behind = ack_offset is not None
        if self._behind and self._read_ahead:
            self._last_fetch_at = time.monotonic()
            self._prefetch = (stream, asyncio.ensure_future(asyncio.to_thread(stream.fetch, from_offset=page.next)))
        processed = await asyncio.to_thread(self._process, page, loop)
        if self._read_ahead:
            self._queue_ack(stream, ack_offset)
        else:
            await asyncio.to_thread(stream.ack, ack_offset)
        return processed

    def _queue_ack(self, stream: Stream, offset: int) -> None:
        """Ack ``offset`` in the background. Acks are cumulative, so while one
        is in flight only the newest wanted offset is kept."""
        if self._ack_failed:
            return  # Retry/backoff owns the cursor; do not bypass a refused ack.
        self._ack_want = (stream, offset)
        if self._ack_task is None or self._ack_task.done():
            self._ack_task = asyncio.ensure_future(self._send_acks())

    async def _send_acks(self) -> None:
        while self._ack_want is not None:
            stream, offset = self._ack_want
            self._ack_want = None
            try:
                await asyncio.to_thread(stream.ack, offset)
            except (httpx.HTTPError, BufferError) as exc:
                # A later ack covers this one; until then, reading restarts at
                # the acked cursor so nothing is skipped.
                self._ack_failed = True
                self._ack_error = exc
                self._ack_want = None
                self.wake()
                if not self._outage.failed(exc):
                    log.warning("Ack of offset=%d failed on cursor=%s: %s", offset, stream.cursor, exc)
                return

    async def _restart_from_cursor(self) -> None:
        """Drop the page read ahead and wait for the acks in flight; the next
        fetch reads from the acked cursor."""
        if self._prefetch is not None:
            self._prefetch[1].cancel()
            self._prefetch = None
        if self._ack_task is not None:
            await self._ack_task
        self._next_from = None
        self._ack_failed = False
        self._ack_error = None

    def _append(self, record: Record) -> str | None:
        """Buffer one record; its ``signal_id``, or ``None`` when it has none."""
        signal_id = self._signal_id_of(record)
        if signal_id is None:
            log.warning(
                "Record at offset=%d on %s has no signal_id — cannot buffer or dispatch it", record.offset, record.topic
            )
            return None
        self._buffer.append(signal_id, self._timestamp_of(record), self._value_of(record))
        return signal_id

    async def _handle(self, signal_id: str, record: Record) -> None:
        for handler in self._dispatch.get(signal_id, []):
            name = consumer_name(handler)
            try:
                try:
                    await handler(record)
                except Reject as rejected:
                    subject = {
                        "stream": STREAM,
                        "cursor": self.cursor,
                        "signal_id": signal_id,
                        **record_subject(record),
                    }
                    await asyncio.to_thread(self._record_rejection, name, subject, rejected)
            except Exception:
                self._failed = (name, record)
                raise
            self._health.succeeded(name)

    def _record_rejection(self, name: str, subject: dict[str, Any], rejected: Reject) -> None:
        if self._reject is None:
            raise RuntimeError(f"{name} rejected a record, but this ingest has nowhere to record it") from rejected
        self._reject(name, subject, rejected)

    def take_failure(self) -> tuple[str, Record] | None:
        """The ``(handler, record)`` whose failure ended the last drain, once."""
        failed, self._failed = self._failed, None
        return failed

    async def _handler_failed(self, exc: Exception, retry: Backoff, stop: asyncio.Event) -> bool:
        """Account for a handler failure that ended a step; False when it was not one."""
        failed = self.take_failure()
        if failed is None:
            return False
        name, record = failed
        await self._restart_from_cursor()
        # The records before the failed one were handled: a restart resumes at it.
        if record.offset > 1:
            try:
                await asyncio.to_thread(self._stream.ack, record.offset - 1)
            except (httpx.HTTPError, BufferError) as ack_error:
                if not self._outage.failed(ack_error):
                    log.warning("Could not acknowledge up to offset=%d: %s", record.offset - 1, ack_error)
        count = self._health.failed(name, exc)
        backoff = retry.delay(exc)
        outage = self._handler_outages.get(name)
        if outage is None:
            outage = self._handler_outages[name] = Outage(log, name, recovered_as="succeeded again")
        if not outage.failed(exc, delay=backoff):
            log.error(
                "on_metric handler %s failed at offset=%d (%d in a row); not acknowledged, retrying in %.1fs",
                name,
                record.offset,
                count,
                backoff,
                exc_info=exc,
            )
        await self._sleep_or_stop(backoff, stop)
        return True

    @staticmethod
    def _signal_id_of(record: Record) -> str | None:
        payload = record.payload
        if isinstance(payload, dict):
            return payload.get("signal_id")
        return getattr(payload, "signal_id", None)

    @staticmethod
    def _timestamp_of(record: Record) -> float:
        payload = record.payload
        value: Any = payload.get("timestamp") if isinstance(payload, dict) else getattr(payload, "timestamp", None)
        return float(value) if value is not None else record.fallback_timestamp_s

    @staticmethod
    def _value_of(record: Record) -> Any:
        payload = record.payload
        if isinstance(payload, dict):
            return payload.get("value")
        return getattr(payload, "value", None)

    @staticmethod
    def _log_gap(gap: Gap) -> None:
        log.warning(
            "Gap on stream=%s: offsets %d..%d were pruned (first_ts=%s last_ts=%s approx=%s) — "
            "continuing from the low-water mark; deep-backfill from the historian is the repair tool.",
            gap.stream,
            gap.from_offset,
            gap.to_offset,
            gap.first_ts,
            gap.last_ts,
            gap.approx,
        )

    # ------------------------------------------------------------------ wake

    def wake(self) -> None:
        """New data may be waiting: drain again. Safe from any thread."""
        self._bell.ring()

    #: How often the ingest lane logs what it did, the same cadence as the
    #: connectors' [DATA] line.
    ROLLUP_INTERVAL_S = 60.0

    def _note_drain(self, processed: int, now: float | None = None) -> None:
        """Fold one drain into the rollup, logging when the window closes.

        A window with zero records is logged too; that is how a stalled loop
        shows.
        """
        now = time.monotonic() if now is None else now
        self._last_drain_at = now
        self._window_records += processed
        self._window_drains += 1
        if now - self._window_started < self.ROLLUP_INTERVAL_S:
            return
        log.info(
            "[DATA] Ingested %d records over %d drains (%.0fs) cursor=%s",
            self._window_records,
            self._window_drains,
            now - self._window_started,
            self.cursor,
        )
        self._window_started = now
        self._window_records = 0
        self._window_drains = 0

    #: Upper bound on the backoff after transport errors; a colca restart takes
    #: seconds.
    ERROR_BACKOFF_MAX_S = 30.0

    #: The first backoff after a transport error, doubled per consecutive one.
    ERROR_BACKOFF_S = 1.0

    def _error_backoff_s(self, attempt: int) -> float:
        """Backoff for the ``attempt``-th (1-based) consecutive transport error.

        Exponential from :attr:`ERROR_BACKOFF_S`, capped at
        :attr:`ERROR_BACKOFF_MAX_S`, plus up to 20% jitter so services do not
        retry in lockstep.
        """
        base = min(self.ERROR_BACKOFF_S * (2 ** (attempt - 1)), self.ERROR_BACKOFF_MAX_S)
        return base * (1.0 + _jitter.uniform(0.0, 0.2))

    async def _sleep_or_stop(self, seconds: float, stop: asyncio.Event) -> None:
        """Sleep up to ``seconds``, waking early if ``stop`` is set."""
        stop_task = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({stop_task}, timeout=seconds)
        finally:
            if not stop_task.done():
                stop_task.cancel()

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        """Process pages until ``stop`` is set.

        Drains while fetches return records, paced as the module docstring
        says, then waits for :meth:`wake`. Where the node supports it, the page
        after a full one is read ahead while the full one is processed.

        A failed handler is retried at its record with backoff, never
        acknowledged (see the module docstring). ``httpx.HTTPError`` (colca
        restarting, a timeout, 429, 5xx) is retried with backoff, and logged
        once when it starts and once when it recovers
        (:class:`chaski.outage.Outage`); the unacked page is simply fetched
        again. Any other exception, a
        retention gap included, propagates, ends the task and turns the health
        door to 503.
        """
        stop = stop or asyncio.Event()
        try:
            await self._run(stop)
        finally:
            await self._restart_from_cursor()

    async def _run(self, stop: asyncio.Event) -> None:
        retry = Backoff(minimum=min(self._retry_min_s, self.ERROR_BACKOFF_MAX_S), maximum=self.ERROR_BACKOFF_MAX_S)
        seen = self._bell.generation
        while not stop.is_set():
            if not self._behind:
                # Taken before the fetch: a wake from here on means another drain.
                seen = self._bell.generation
            self.waiting = False
            try:
                processed = await self._step()
            except Exception as exc:
                if await self._handler_failed(exc, retry, stop):
                    continue
                if not isinstance(exc, (httpx.HTTPError, BufferError)):
                    raise
                await self._restart_from_cursor()
                backoff = retry.delay(exc)
                if not self._outage.failed(exc, delay=backoff):
                    log.error(
                        "Ingest drain failed (attempt %d); retrying in %.1fs", retry.failures, backoff, exc_info=exc
                    )
                await self._sleep_or_stop(backoff, stop)
                continue

            retry.reset()
            self._outage.recovered()
            for outage in self._handler_outages.values():
                outage.recovered()
            self._note_drain(processed)
            if processed > 0 or self._behind:
                continue

            self.waiting = True
            wake_task = asyncio.ensure_future(self._bell.after(seen))
            stop_task = asyncio.ensure_future(stop.wait())
            try:
                # A timeout drains too: see "A silent filter still moves".
                await asyncio.wait(
                    {wake_task, stop_task}, timeout=self._idle_drain_s, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                for task in (wake_task, stop_task):
                    if not task.done():
                        task.cancel()
