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

**Gaps.** When the cursor fell below the stream's low-water mark, colca
returns a ``gap`` with the surviving records. It is logged as a warning with
the pruned range, and processing continues.

**MQTT only wakes it.** A message on one of the service's input topics calls
:meth:`Ingest.wake` (through
:meth:`chaski.dataops.service.DataOpsService._wake_on_inputs`), which cuts
the poll sleep short; the message itself is not read.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from random import SystemRandom
from typing import Any

import httpx

from chaski.door import Gap, Page, Record, Stream

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
        poll_interval_s: float = 1.0,
        previous_generation: str | None = None,
    ) -> None:
        self._open_stream = open_stream
        self._buffer = buffer
        self._dispatch = dispatch or {}
        self._signal_ids = list(signal_ids) if signal_ids is not None else None
        self._poll_interval_s = poll_interval_s
        self._cursor_name = cursor_name(buffer.generation)
        self._stream = open_stream(self._cursor_name, self._signal_ids)
        self._previous_generation = previous_generation
        self._wake = asyncio.Event()
        self._window_started = time.monotonic()
        self._window_records = 0
        self._window_drains = 0

    def rebind(self, dispatch: dict[str, list], signal_ids: Iterable[str] | None) -> None:
        """Swap what this loop dispatches and fetches, mid-run.

        Producers often start before the signals they read are commissioned.
        Only the table and the filter change; the cursor keeps its position,
        and records a newly resolved signal missed are recovered by replay.
        """
        self._dispatch = dispatch or {}
        self._signal_ids = list(signal_ids) if signal_ids is not None else None
        self._stream = self._open_stream(self._cursor_name, self._signal_ids)
        self._wake.set()

    @property
    def cursor(self) -> str:
        """The full cursor name at the door (namespace included)."""
        return self._stream.cursor

    @property
    def stream(self) -> Stream:
        return self._stream

    @property
    def poll_interval_s(self) -> float:
        return self._poll_interval_s

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
        """Fetch one page, process every record, then ack, in a worker thread.

        A drain is blocking work (door HTTP, sqlite, sync handler bodies), so it
        runs off the event loop, which keeps timers, wake and the health door
        responsive.

        Returns the number of records processed. A ``gap`` is logged, not
        raised. ``httpx.HTTPError`` propagates for :meth:`run_forever` to
        retry; any other exception ends the task.
        """
        return await asyncio.to_thread(self.drain_once)

    def drain_once(self) -> int:
        """The synchronous body of :meth:`run_once`: one page, start to ack.

        ``@on_metric`` handlers are coroutines with sync bodies, so one private
        loop runs them one after another, keeping stream order.
        """
        stream = self._stream
        page: Page = stream.fetch()

        if page.gap is not None:
            self._log_gap(page.gap)

        if page.records:
            asyncio.run(self._process_records(page.records))

        # See Page.ack_offset: the last record, or the gap's bound when nothing
        # survived it.
        ack_offset = page.ack_offset
        if ack_offset is not None:
            stream.ack(ack_offset)

        return len(page.records)

    async def _process_records(self, records: Iterable[Record]) -> None:
        for record in records:
            await self._process_record(record)

    async def _process_record(self, record: Record) -> None:
        signal_id = self._signal_id_of(record)
        if signal_id is None:
            log.warning(
                "Record at offset=%d on %s has no signal_id — cannot buffer or dispatch it", record.offset, record.topic
            )
            return

        ts = self._timestamp_of(record)
        self._buffer.append(signal_id, ts, self._value_of(record))

        for handler in self._dispatch.get(signal_id, []):
            try:
                await handler(record)
            except Exception:
                # The record is already in the buffer; a failing handler only
                # misses this event.
                log.exception(
                    "on_metric handler failed for signal_id=%s at offset=%d — continuing",
                    signal_id,
                    record.offset,
                )

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
        """Signal that new data may be waiting, cutting the poll sleep short.

        From another thread, call it through ``loop.call_soon_threadsafe``.
        """
        self._wake.set()

    #: How often the ingest lane logs what it did, the same cadence as the
    #: connectors' [DATA] line.
    ROLLUP_INTERVAL_S = 60.0

    def _note_drain(self, processed: int, now: float | None = None) -> None:
        """Fold one drain into the rollup, logging when the window closes.

        A window with zero records is logged too; that is how a stalled loop
        shows.
        """
        now = time.monotonic() if now is None else now
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

    def _error_backoff_s(self, attempt: int) -> float:
        """Backoff for the ``attempt``-th (1-based) consecutive transport error.

        Exponential from the poll interval, capped at
        :attr:`ERROR_BACKOFF_MAX_S`, plus up to 20% jitter so services do not
        retry in lockstep.
        """
        base = min(max(self._poll_interval_s, 0.1) * (2 ** (attempt - 1)), self.ERROR_BACKOFF_MAX_S)
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
        """Run :meth:`run_once` until ``stop`` is set.

        Drains without sleeping while fetches return records, then sleeps up
        to ``poll_interval_s``; :meth:`wake` cuts the sleep short.

        ``httpx.HTTPError`` (colca restarting, a timeout, 429, 5xx) is logged
        and retried with backoff; the unacked page is simply fetched again. Any
        other exception propagates, ends the task and turns the health door
        to 503.
        """
        stop = stop or asyncio.Event()
        consecutive_errors = 0
        while not stop.is_set():
            try:
                processed = await self.run_once()
            except httpx.HTTPError as exc:
                consecutive_errors += 1
                backoff = self._error_backoff_s(consecutive_errors)
                log.warning(
                    "Ingest drain failed (attempt %d): %s — retrying in %.1fs",
                    consecutive_errors,
                    exc,
                    backoff,
                )
                await self._sleep_or_stop(backoff, stop)
                continue

            consecutive_errors = 0
            self._note_drain(processed)
            if processed > 0:
                continue

            self._wake.clear()
            wake_task = asyncio.ensure_future(self._wake.wait())
            stop_task = asyncio.ensure_future(stop.wait())
            try:
                await asyncio.wait(
                    {wake_task, stop_task},
                    timeout=self._poll_interval_s,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in (wake_task, stop_task):
                    if not task.done():
                        task.cancel()
