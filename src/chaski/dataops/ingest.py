"""Single-lane stream ingest loop over colca's ``metrics`` stream.

This is the runtime core of the dataops evaluator
(the dataops evaluator design §3): one
durable, generational cursor (``ingest-<generation>`` inside the service's
own cursor namespace — ``c/dataops/ingest-<generation>`` for the shipped
service) walks the ``metrics`` stream forward, appends every record to the
local :class:`~chaski.dataops.Buffer`, and dispatches any ``@on_metric``
handler bound to that record's ``signal_id`` — in stream order, live and on
replay alike. Cold start, disaster recovery, and replay are the same code
path: a fresh cursor (new buffer generation) simply starts at offset 1 and
walks the whole retained window.

**One lane, and it is the SDK's.** The cursor is a
:class:`chaski.door.Stream` — what ``Service.stream("metrics", cursor=...,
signal_ids=...)`` returns — so the ingest loop reads through exactly the
consume lane every other service uses (service families design
§3.3), not a private door client. The loop asks its ``open_stream``
factory for the stream once, and again whenever the signal filter changes
(:meth:`Ingest.rebind`); the cursor name stays, so its server-side position
does.

**Crash-safety contract.** Per page: append every record to the buffer AND
run its handlers, THEN ack. A crash between processing and the ack simply
re-delivers the same page on the next fetch — :meth:`Buffer.append` is
``INSERT OR REPLACE`` on ``(signal_id, ts)``, so reprocessing a page never
duplicates a row. Handlers are expected to be idempotent for the same
reason (metric writes overwrite on exact ``(signal_id, timestamp)`` match —
design §6).

**Gap honesty.** If a page's cursor position has fallen below the stream's
low-water mark, colca returns a ``gap`` object alongside whatever records
survive at/after the LWM. This is logged at WARNING (an operator-visible
level) with the pruned range's bounds and is NOT an error — processing
continues from what the page returned. The deep-backfill CLI is the repair
tool for data lost to retention.

**MQTT is a doorbell only.** The service subscribes to ``colca/v1/_Metric/#``
and calls :meth:`Ingest.wake` on any delivery — never reads the message
payload. That wake short-circuits :meth:`Ingest.run_forever`'s
poll-interval sleep so live data is picked up promptly without polling
tightly. Data enters through exactly one lane: this fetch loop.
"""

from __future__ import annotations

import asyncio
import logging
import time
from random import SystemRandom
from typing import Any, Awaitable, Callable, Iterable

import httpx

from chaski.door import Gap, Page, Record, Stream

from .buffer import Buffer

log = logging.getLogger("chaski.dataops.ingest")

# Jitter on the error backoff, not a security-sensitive use — SystemRandom
# (os.urandom-backed) purely because it doesn't trip a "non-cryptographic
# random" scanner finding for what is, in fact, a non-cryptographic use.
_jitter = SystemRandom()

STREAM = "metrics"

# A handler receives the raw stream Record (not a decoded contract type —
# Ingest stays contract-agnostic, same boundary Door itself draws). The
# caller wiring the dispatch table decides how to decode ``record.payload``.
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
    """Fetch → append → dispatch → ack loop over one colca stream.

    One :class:`Ingest` owns one generational cursor, derived from
    ``buffer.generation`` at construction time. ``dispatch`` maps a
    ``signal_id`` to the handlers that should fire for it; ``signal_ids``
    is the union of every declared input's signal id, sent with the fetch
    so the door itself filters the stream server-side — the buffer only
    ever holds points for signals some producer declared (design §3).
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

        A producer's inputs are resolved to signal ids at startup, and a
        signal is commissioned by a separate act — so a producer routinely
        comes up before the signals it reads exist. Without this the loop
        would keep the empty table it was born with and the producer would
        run on its timers alone, reading a buffer nothing was filtering into.

        Only the table and the filter change. The cursor does not: records
        already consumed under the old filter stay consumed, and the ones a
        newly resolved signal missed are what the producer's own replay
        (design §10) exists to recover.
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

        Colca cursors only move forward, so a rebuilt buffer (a fresh
        generation) starts a brand new cursor at offset 1 — the old one
        is retired so it neither litters the cursor namespace nor holds
        retention hostage (design §5). A cursor delete is idempotent
        success even when the cursor is already gone, so this never needs
        to know whether it "really" existed. When no previous generation
        is known, this is a no-op.
        """
        if not self._previous_generation:
            return
        stale = self._open_stream(cursor_name(self._previous_generation), None)
        log.info("Retiring previous-generation cursor %s (stream=%s)", stale.cursor, stale.name)
        stale.retire()

    # ------------------------------------------------------------------ loop

    async def run_once(self) -> int:
        """Fetch one page, process every record, then ack — OFF the loop.

        Everything in a drain is blocking work (door HTTP, sqlite appends,
        producer handlers whose bodies are sync), so the whole of it runs in
        a worker thread via ``asyncio.to_thread``. Awaiting it on the loop
        directly is how a 15-second producer schedule once fired every ~30
        seconds: the loop that owns every timer spent its time inside
        synchronous httpx calls. The loop's job here is coordination only —
        wake, stop, and the health door stay responsive while a drain runs.

        Returns the number of records processed. Never raises on a
        ``gap`` — it is logged and processing continues from whatever the
        page returned. Fetch/ack transport errors (``httpx.HTTPError``)
        propagate to the caller; :meth:`run_forever` catches exactly that
        class and retries with backoff (a colca restart, a read timeout, a
        429, a 5xx are all transient). Any other exception is a genuine bug
        and is left to kill the task, which is what turns the health door
        503 — see :meth:`run_forever`.
        """
        return await asyncio.to_thread(self.drain_once)

    def drain_once(self) -> int:
        """The synchronous body of :meth:`run_once` — one page, start to ack.

        Runs in a worker thread. The ``@on_metric`` handlers are coroutine
        functions by contract (the producer API), but their bodies are sync
        — so one short-lived private loop drives the whole page, keeping
        records in stream order (order inside a stream is never changed;
        parallelising the handlers here would change it).
        """
        stream = self._stream
        page: Page = stream.fetch()

        if page.gap is not None:
            self._log_gap(page.gap)

        if page.records:
            asyncio.run(self._process_records(page.records))

        # The last record's offset; a gap's own bound when nothing survived
        # at/after the low-water mark (otherwise the next fetch reports the
        # identical gap forever); nothing for an empty page — the one rule
        # `Page.ack_offset` owns, shared with `Stream.drain`.
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
                # One broken handler must not take the whole ingest loop
                # down — the record already landed in the buffer, so
                # nothing here is lost; the failing producer just misses
                # this event.
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
        value: Any
        if isinstance(payload, dict):
            value = payload.get("timestamp")
        else:
            value = getattr(payload, "timestamp", None)
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

    # ------------------------------------------------------------------ doorbell

    def wake(self) -> None:
        """Signal that new data may be waiting — short-circuits the next
        poll-interval sleep in :meth:`run_forever`.

        Safe to call from any thread via
        ``loop.call_soon_threadsafe(ingest.wake)`` — setting an
        ``asyncio.Event`` is the one asyncio primitive documented as
        thread-safe to call this way.
        """
        self._wake.set()

    #: How often the ingest lane says what it did — the same 60 s cadence the
    #: connectors use for their [DATA] rollup, so one grep shows the whole
    #: data path's pulse.
    ROLLUP_INTERVAL_S = 60.0

    def _note_drain(self, processed: int, now: float | None = None) -> None:
        """Fold one drain into the rollup, logging when the window closes.

        Logged even when the window ingested nothing: on a node whose
        connectors publish every second, "0 records" IS the finding — a
        silent ingest lane was exactly what a stalled loop looked like, and
        there was no line to see it by.
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

    #: Bounds on the backoff after a transport error — a colca restart
    #: measured in seconds, not minutes, so the ceiling stays short even
    #: though it grows exponentially per consecutive failure.
    ERROR_BACKOFF_MAX_S = 30.0

    def _error_backoff_s(self, attempt: int) -> float:
        """Bounded, jittered backoff for the ``attempt``-th (1-based)
        consecutive transport error.

        Exponential from the poll interval, capped at
        :attr:`ERROR_BACKOFF_MAX_S`, plus up to 20% jitter — bounded so a
        wedged door is retried within seconds, jittered so a fleet of
        producers recovering from the same colca restart doesn't all
        hammer it back on the same tick.
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
        """Run :meth:`run_once` forever until ``stop`` is set.

        Drains without sleeping while a fetch returns records (there may
        be more waiting); once a fetch comes back empty, sleeps up to
        ``poll_interval_s`` — a :meth:`wake` call (the MQTT doorbell)
        cuts that sleep short.

        A ``httpx.HTTPError`` out of :meth:`run_once` (colca restarting, a
        read timeout, a 429, a 5xx on either ``/fetch`` or ``/ack``) is
        transient: it is logged, backed off (:meth:`_error_backoff_s`), and
        the loop continues — the un-acked cursor position means the next
        fetch simply re-delivers whatever page was in flight, which
        ``Buffer.append``'s idempotency makes safe to reprocess. Any other
        exception is a genuine bug and is left to propagate, which ends
        this task and turns the health door 503 (design: a dead ingest
        task is the one thing that should).
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
