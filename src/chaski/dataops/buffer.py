"""SQLite-backed input buffer, a DataOps service's only local state.

One file under the service's data directory. ``points`` (the
retained window per input signal), ``watermarks`` (replay progress per
producer), ``meta`` (the store's ``generation``), and ``emitted_annotations``
(the ids each ``AnnotationOutput`` published, so ``clear_window`` only deletes
its own), ``backfill_jobs`` (each backfill's range and committed position, see
:mod:`chaski.dataops.backfill`), ``tick_progress`` (the last instant each
``@every``/``@cron`` tick ran for), ``pending_outputs`` (computed samples
waiting for a signal binding or a successful publish), and ``command_ledger``
(the commands this service started executing and the answer each got, so a
restart neither runs a command twice nor answers it twice).

:meth:`Buffer.append` is idempotent on ``(signal_id, ts)``, so a record
processed again after a crash rewrites the same row. Cold start, recovery and
replay are therefore the same code path.

**One writer, explicit transactions.** Every write runs on the one write
connection, held by one thread at a time, as a transaction of its own
(``BEGIN IMMEDIATE`` ... ``COMMIT``) that is committed before the call
returns. Writes that belong together go in one call (:meth:`Buffer.atomically`).
No write ever lands in a transaction someone else opened, and nothing is
acknowledged before its write is committed. A long maintenance task
(:meth:`Buffer.trim`) is cut into short transactions with pauses between,
which other writes take turns with.

**Readers do not wait for the writer.** The file is in WAL mode; every thread
reads on a read-only connection of its own and sees the last committed state.
"""

from __future__ import annotations

import contextlib
import json
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

import ulid

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger("chaski.dataops.buffer")

T = TypeVar("T")

#: Rows the first statement of a :meth:`Buffer.trim` deletes; later
#: statements size themselves to :data:`TRIM_BATCH_S`.
TRIM_CHUNK = 2000
#: How long one trim statement may hold the writer: what a write that queues
#: behind it waits at most.
TRIM_BATCH_S = 0.025
#: Bytes the WAL file is cut back to after a checkpoint: one large
#: transaction does not leave a WAL of its size behind for good.
WAL_SIZE_LIMIT = 64 * 1024 * 1024
#: Page cache of each read connection (KiB). There is one per reading thread.
READ_CACHE_KIB = 1024
#: Page cache of the write connection (KiB).
WRITE_CACHE_KIB = 8192

_SCHEMA = """
CREATE TABLE IF NOT EXISTS producer_checkpoints (
    producer TEXT PRIMARY KEY, code_hash TEXT NOT NULL,
    version INTEGER NOT NULL, state TEXT NOT NULL, positions TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_outputs (
    source TEXT NOT NULL, ts REAL NOT NULL, value TEXT NOT NULL,
    PRIMARY KEY (source, ts)
);

CREATE TABLE IF NOT EXISTS points (
    signal_id TEXT NOT NULL,
    ts        REAL NOT NULL,
    value     TEXT NOT NULL,
    PRIMARY KEY (signal_id, ts)
);

CREATE TABLE IF NOT EXISTS watermarks (
    producer  TEXT PRIMARY KEY,
    position  REAL,
    code_hash TEXT
);

CREATE TABLE IF NOT EXISTS command_ledger (
    correlation_id TEXT PRIMARY KEY,
    started_at     REAL NOT NULL,
    answer         TEXT
);

CREATE TABLE IF NOT EXISTS backfill_jobs (
    producer    TEXT NOT NULL,
    job         TEXT NOT NULL,
    start       REAL NOT NULL,
    "end"       REAL,
    position    REAL NOT NULL,
    windows     INTEGER NOT NULL DEFAULT 0,
    code_hash   TEXT NOT NULL,
    done        INTEGER NOT NULL DEFAULT 0,
    created_at  REAL NOT NULL,
    window_s    REAL,
    PRIMARY KEY (producer, job)
);

CREATE TABLE IF NOT EXISTS tick_progress (
    producer  TEXT NOT NULL,
    tick      TEXT NOT NULL,
    due       REAL NOT NULL,
    PRIMARY KEY (producer, tick)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS emitted_annotations (
    source        TEXT NOT NULL,
    annotation_id TEXT NOT NULL,
    time_start    REAL NOT NULL,
    PRIMARY KEY (source, annotation_id)
);
CREATE INDEX IF NOT EXISTS idx_emitted_annotations_source_start
    ON emitted_annotations (source, time_start);
"""


class _Writer:
    """The buffer's one writer: the write connection, used by one thread at a
    time, one explicit transaction at a time.

    The calling thread runs its transaction itself while it holds the writer
    (no hop to another thread). A write made while the same thread already
    holds it (inside :meth:`Buffer.atomically`) runs inside the transaction
    already open."""

    def __init__(self, path: Path) -> None:
        self._lock = threading.RLock()
        self._owner: threading.Thread | None = None
        self._depth = 0
        self._closed = False
        conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(f"PRAGMA journal_size_limit={WAL_SIZE_LIMIT}")
            conn.execute(f"PRAGMA cache_size=-{WRITE_CACHE_KIB}")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA synchronous=NORMAL")
        except BaseException:
            conn.close()
            raise
        self.conn = conn

    def _transaction(self, fn: Callable[[sqlite3.Connection], T], *, durable: bool) -> T:
        conn = self.conn
        if self._depth:
            return fn(conn)
        if durable:
            # synchronous=FULL syncs the WAL at this commit, with every frame
            # committed before it under NORMAL.
            conn.execute("PRAGMA synchronous=FULL")
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._depth = 1
            try:
                result = fn(conn)
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                self._depth = 0
        finally:
            if durable:
                conn.execute("PRAGMA synchronous=NORMAL")
        return result

    def on_thread(self) -> bool:
        """Whether the calling thread holds the writer (inside a write)."""
        return self._owner is threading.current_thread()

    def call(self, fn: Callable[[sqlite3.Connection], T], *, bare: bool = False, durable: bool = True) -> T:
        """Run ``fn(conn)`` as one committed transaction and return its result.

        ``durable`` (the default) syncs the commit to disk before returning, with
        every commit before it; otherwise the commit survives a crash of the
        process but may be lost with the machine's power until the next
        durable one. ``bare`` runs ``fn`` with no transaction around it, for
        statements that commit on their own."""
        with self._lock:
            if self._closed:
                raise sqlite3.ProgrammingError("Cannot write to a closed buffer.")
            outer = self._owner
            self._owner = threading.current_thread()
            try:
                if bare and not self._depth:
                    return fn(self.conn)
                return self._transaction(fn, durable=durable)
            finally:
                self._owner = outer

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self.conn.close()


class Buffer:
    """One SQLite file holding a DataOps service's only local state.

    Boundary/round-trip decisions pinned here:

    * :meth:`window` is **half-open**: ``start <= ts < end``. This matches
      the historian-query convention (``timestamp >= %s AND timestamp <
      %s``), so a caller tiling consecutive windows never double-counts the
      shared boundary point.
    * :meth:`latest_before` is **inclusive** of ``before`` itself
      (``ts <= before``): "the value in force at this instant" naturally
      includes that instant.
    * Values round-trip through JSON (``json.dumps``/``json.loads``).
      SQLite has no native boolean type, but JSON does, and it tells a
      ``bool`` apart from an ``int``/``float``/``str``/object on the way
      back out — which is what keeps ``True`` from coming back as ``1``.

    Every write is committed when the call returns (module docstring, "One
    writer"); a read sees every write that returned before it started.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._readers_lock = threading.Lock()
        self._readers: list[sqlite3.Connection] = []
        self._local = threading.local()
        self._closed = False
        self._writer = _Writer(self._path)
        self._setup_schema()
        self.generation: str = self._load_or_mint_generation()

    # ------------------------------------------------------------------ plumbing

    def _setup_schema(self) -> None:
        def setup(conn: sqlite3.Connection) -> None:
            conn.executescript(_SCHEMA)  # commits on its own
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._upgrade(conn)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

        self._writer.call(setup, bare=True)

    def _upgrade(self, conn: sqlite3.Connection) -> None:
        """Bring a buffer written by an older release to the current schema."""
        columns = {row[1] for row in conn.execute("PRAGMA table_info(backfill_jobs)")}
        if "window_s" not in columns:
            conn.execute("ALTER TABLE backfill_jobs ADD COLUMN window_s REAL")
        # Application time (chaski 0.25 and older) kept factory-callback
        # progress in ``timers`` and ``__clock__:`` watermarks, and a
        # coordinated input inbox in ``pending_inputs``; nothing reads them now.
        conn.execute("DROP TABLE IF EXISTS timers")
        conn.execute("DROP TABLE IF EXISTS pending_inputs")
        conn.execute("DELETE FROM watermarks WHERE substr(producer, 1, 10) = '__clock__:'")
        conn.execute("DELETE FROM meta WHERE key = 'input_intake'")
        # chaski 0.27 and older also indexed ``points`` on (signal_id, ts), a
        # copy of its primary key. It stays: dropping it frees every page of
        # it in one transaction, and with SQLite's secure_delete (Debian's
        # default) each freed page is rewritten into the WAL, a gigabyte on a
        # large buffer, more than a small container's memory. Startup only
        # makes writes whose size does not grow with the buffer.

    def _read(self) -> sqlite3.Connection:
        """This thread's read connection (the write connection on the writer
        thread, so a read inside :meth:`atomically` sees its own writes)."""
        if self._writer.on_thread():
            return self._writer.conn
        conn = getattr(self._local, "conn", None)
        if conn is None:
            if self._closed:
                raise sqlite3.ProgrammingError("Cannot read from a closed buffer.")
            conn = sqlite3.connect(str(self._path), isolation_level=None, check_same_thread=False)
            conn.execute("PRAGMA query_only=ON")
            conn.execute(f"PRAGMA cache_size=-{READ_CACHE_KIB}")
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
            with self._readers_lock:
                self._readers.append(conn)
        return conn

    def _write(self, fn: Callable[[sqlite3.Connection], T], *, durable: bool = True) -> T:
        return self._writer.call(fn, durable=durable)

    def sync(self) -> None:
        """Make every write so far durable (one sync of the log): what an
        ingest page committed is on disk before the page is acked."""
        self._write(lambda conn: conn.execute("UPDATE meta SET value = value WHERE key = 'generation'"))

    def atomically(self, fn: Callable[[], T]) -> T:
        """Run ``fn()`` holding the writer, as one transaction: every write it
        makes through this buffer commits together, or none does, and its
        reads see its own writes. ``fn`` must not wait for anything but the
        buffer."""
        return self._writer.call(lambda _conn: fn())

    def close(self) -> None:
        self._closed = True
        self._writer.close()
        with self._readers_lock:
            readers, self._readers = self._readers, []
        for conn in readers:
            with contextlib.suppress(sqlite3.Error):
                conn.close()

    def __enter__(self) -> Buffer:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ checkpoints

    def checkpoint(self, producer: str):
        """Return a producer's versioned JSON state and handled input offsets."""
        row = (
            self._read()
            .execute(
                "SELECT code_hash, version, state, positions FROM producer_checkpoints WHERE producer=?",
                (producer,),
            )
            .fetchone()
        )
        return (row[0], row[1], json.loads(row[2]), json.loads(row[3])) if row else None

    def save_checkpoint(self, producer, code_hash, version, state, positions, *, durable: bool = True):
        """Persist a producer's state and handled offsets, committed when this
        returns (``durable=False``: within an ingest page, which syncs before
        its ack)."""
        self.save_checkpoints([(producer, code_hash, version, state, positions)], durable=durable)

    def save_checkpoints(self, rows: list[tuple[str, str, int, Any, Any]], *, durable: bool = True) -> None:
        """:meth:`save_checkpoint` for several producers in one transaction.
        ``durable=False`` within an ingest page, which syncs before its ack."""
        encoded = [
            (producer, code_hash, version, json.dumps(state, allow_nan=False), json.dumps(positions, allow_nan=False))
            for producer, code_hash, version, state, positions in rows
        ]
        if not encoded:
            return
        self._write(
            lambda conn: conn.executemany("INSERT OR REPLACE INTO producer_checkpoints VALUES (?,?,?,?,?)", encoded),
            durable=durable,
        )

    def queue_output(self, source, timestamp, value, *, limit=10000):
        """Persist unbound samples; backpressure instead of evicting history."""
        encoded = json.dumps(value, allow_nan=False)

        def write(conn: sqlite3.Connection) -> None:
            exists = conn.execute(
                "SELECT 1 FROM pending_outputs WHERE source=? AND ts=?", (source, timestamp)
            ).fetchone()
            if (
                not exists
                and conn.execute("SELECT COUNT(*) FROM pending_outputs WHERE source=?", (source,)).fetchone()[0]
                >= limit
            ):
                raise BufferError(f"unbound output {source!r} reached its durable queue limit")
            conn.execute(
                "INSERT INTO pending_outputs VALUES (?,?,?) ON CONFLICT(source,ts) DO UPDATE SET value=excluded.value",
                (source, timestamp, encoded),
            )

        self._write(write)

    def pending_outputs(self, source, *, limit=1000):
        return [
            (ts, json.loads(value))
            for ts, value in self._read().execute(
                "SELECT ts,value FROM pending_outputs WHERE source=? ORDER BY ts LIMIT ?", (source, limit)
            )
        ]

    def outputs_sent(self, source: str, timestamps: list[float]) -> None:
        """Drop the queued samples of ``source`` the node confirmed, in one commit."""
        rows = [(source, ts) for ts in timestamps]
        self._write(lambda conn: conn.executemany("DELETE FROM pending_outputs WHERE source=? AND ts=?", rows))

    # ------------------------------------------------------------------ identity

    def _load_or_mint_generation(self) -> str:
        def load(conn: sqlite3.Connection) -> tuple[str, bool]:
            row = conn.execute("SELECT value FROM meta WHERE key = 'generation'").fetchone()
            if row is not None:
                return row[0], False
            generation = str(ulid.new())
            conn.execute("INSERT INTO meta (key, value) VALUES ('generation', ?)", (generation,))
            return generation, True

        generation, minted = self._write(load)
        if minted:
            log.info("Minted new buffer generation %s at %s", generation, self._path)
        return generation

    def accepted_gap(self) -> int:
        """The stream offset through which an operator accepted lost input
        records (:meth:`accept_gap`), 0 when none was accepted."""
        row = self._read().execute("SELECT value FROM meta WHERE key = 'accepted_gap_through'").fetchone()
        return int(row[0]) if row is not None else 0

    def accept_gap(self, through_offset: int) -> None:
        """Record, durably, that the input records pruned up to
        ``through_offset`` are accepted as lost."""

        def write(conn: sqlite3.Connection) -> None:
            row = conn.execute("SELECT value FROM meta WHERE key = 'accepted_gap_through'").fetchone()
            value = max(through_offset, int(row[0]) if row is not None else 0)
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('accepted_gap_through', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(value),),
            )

        self._write(write)

    # ------------------------------------------------------------------ points

    def append(self, signal_id: str, ts: float, value: Any) -> None:
        """Insert or overwrite one point. Idempotent on ``(signal_id, ts)``.
        Committed when this returns."""
        self.append_many([(signal_id, ts, value)])

    def append_many(self, points: list[tuple[str, float, Any]], *, durable: bool = True) -> None:
        """:meth:`append` for several points in one transaction. An ingest
        page appends with ``durable=False`` and makes the page durable once,
        before its ack (:meth:`sync`)."""
        values = [(signal, ts, json.dumps(value)) for signal, ts, value in points]
        if not values:
            return
        self._write(
            lambda conn: conn.executemany(
                "INSERT OR REPLACE INTO points (signal_id, ts, value) VALUES (?, ?, ?)", values
            ),
            durable=durable,
        )

    # ─── command ledger ───────────────────────────────────────────────

    def command_entry(self, correlation_id: str) -> tuple[bool, str | None]:
        """``(started, answer)`` of a command: whether this service started
        executing it, and the answer it recorded (JSON), if any."""
        row = (
            self._read()
            .execute("SELECT answer FROM command_ledger WHERE correlation_id=?", (correlation_id,))
            .fetchone()
        )
        return (row is not None, row[0] if row is not None else None)

    def command_started(self, correlation_id: str, *, keep_s: float = 3600.0) -> None:
        """Record, durably and before the handler runs, that a command is
        being executed. Entries older than ``keep_s`` are dropped."""
        now = time.time()

        def write(conn: sqlite3.Connection) -> None:
            conn.execute("DELETE FROM command_ledger WHERE started_at < ?", (now - keep_s,))
            conn.execute(
                "INSERT OR IGNORE INTO command_ledger (correlation_id, started_at) VALUES (?, ?)",
                (correlation_id, now),
            )

        self._write(write)

    def command_answered(self, correlation_id: str, answer: str) -> None:
        """Record a command's answer before it is published."""
        now = time.time()
        self._write(
            lambda conn: conn.execute(
                "INSERT INTO command_ledger (correlation_id, started_at, answer) VALUES (?, ?, ?) "
                "ON CONFLICT(correlation_id) DO UPDATE SET answer=excluded.answer",
                (correlation_id, now, answer),
            )
        )

    def window(self, signal_id: str, start: float, end: float) -> pd.DataFrame:
        """Points for ``signal_id`` with ``start <= ts < end``, ordered by ts.

        Returns a DataFrame with columns ``ts`` (float) and ``value`` (the
        original Python type, decoded from JSON). Empty — but correctly
        columned — when nothing matches.
        """
        # pandas costs a process ~60 MB, so it loads with the first frame.
        import pandas as pd

        rows = self.points(signal_id, start, end)
        return pd.DataFrame(
            {"ts": [ts for ts, _ in rows], "value": [value for _, value in rows]},
            columns=["ts", "value"],
        )

    def points(self, signal_id: str, start: float, end: float) -> list[tuple[float, Any]]:
        """``(ts, value)`` for ``signal_id`` with ``start <= ts < end``, ordered by ts."""
        rows = (
            self._read()
            .execute(
                "SELECT ts, value FROM points WHERE signal_id = ? AND ts >= ? AND ts < ? ORDER BY ts ASC",
                (signal_id, start, end),
            )
            .fetchall()
        )
        return [(ts, json.loads(value)) for ts, value in rows]

    def latest_before(self, signal_id: str, before: float) -> tuple[float, Any] | None:
        """Latest point for ``signal_id`` with ``ts <= before``, or ``None``."""
        row = (
            self._read()
            .execute(
                "SELECT ts, value FROM points WHERE signal_id = ? AND ts <= ? ORDER BY ts DESC LIMIT 1",
                (signal_id, before),
            )
            .fetchone()
        )
        if row is None:
            return None
        return (row[0], json.loads(row[1]))

    def earliest(self, signal_id: str) -> float | None:
        """``MIN(ts)`` for ``signal_id``, or ``None`` if the buffer holds nothing for it."""
        row = self._read().execute("SELECT MIN(ts) FROM points WHERE signal_id = ?", (signal_id,)).fetchone()
        return row[0] if row and row[0] is not None else None

    def next_point(self, signal_id: str, at: float) -> float | None:
        """The oldest ``ts >= at`` for ``signal_id``, or ``None``."""
        row = (
            self._read()
            .execute("SELECT MIN(ts) FROM points WHERE signal_id = ? AND ts >= ?", (signal_id, at))
            .fetchone()
        )
        return row[0] if row and row[0] is not None else None

    def latest(self, signal_id: str) -> float | None:
        """``MAX(ts)`` for ``signal_id``, or ``None`` if the buffer holds nothing for it."""
        row = self._read().execute("SELECT MAX(ts) FROM points WHERE signal_id = ?", (signal_id,)).fetchone()
        return row[0] if row and row[0] is not None else None

    def trim(
        self,
        horizons: dict[str, float],
        *,
        now: float | None = None,
        chunk: int = TRIM_CHUNK,
        batch_s: float = TRIM_BATCH_S,
        pause: Callable[[float], None] = time.sleep,
    ) -> int:
        """Trim history while retaining the value in force at each window start.

        Keep the newest point before the cutoff as an anchor for latest-value
        reads and change-only signals. That is at most one extra row per signal.
        Only signals present as keys in ``horizons`` are touched — a
        signal absent from the dict keeps every point it has. Never
        touches ``watermarks`` or ``meta``. Returns the total rows deleted.

        A trim is background work and gives way to everything else that writes:
        each statement is a transaction of its own on the writer, sized so it
        holds the writer for about ``batch_s`` (starting at ``chunk`` rows), and
        after each one the trim waits (``pause``) as long as it held it, so it
        keeps the writer busy at most half the time. A write that arrives
        meanwhile waits for one statement at most, never for the whole trim.
        Call it from a thread of its own, never from the writer thread.
        """
        now = time.time() if now is None else now
        deleted = 0
        size = max(1, chunk)
        for signal_id, horizon in horizons.items():
            cutoff = now - horizon
            row = (
                self._read()
                .execute("SELECT MAX(ts) FROM points WHERE signal_id = ? AND ts < ?", (signal_id, cutoff))
                .fetchone()
            )
            anchor = row[0] if row is not None else None
            if anchor is None:
                continue
            while True:
                limit = size

                def delete(
                    conn: sqlite3.Connection, limit: int = limit, anchor: float = anchor, signal_id: str = signal_id
                ) -> tuple[int, float]:
                    began = time.monotonic()
                    cur = conn.execute(
                        "DELETE FROM points WHERE rowid IN "
                        "(SELECT rowid FROM points WHERE signal_id = ? AND ts < ? ORDER BY ts LIMIT ?)",
                        (signal_id, anchor, limit),
                    )
                    return cur.rowcount, time.monotonic() - began

                count, held = self._write(delete)
                deleted += count
                if count < limit:
                    break
                # Aim the next statement at batch_s, within a factor of two per step.
                if held > 0:
                    size = max(100, min(size * 2, int(size * batch_s / held)))
                pause(held)
        return deleted

    # ------------------------------------------------------------------ watermarks

    def watermark(self, producer: str) -> float | None:
        """The producer's last-processed position, or ``None`` if never set."""
        row = self._read().execute("SELECT position FROM watermarks WHERE producer = ?", (producer,)).fetchone()
        return row[0] if row is not None else None

    def code_hash(self, producer: str) -> str | None:
        """The code hash last stored for the producer, or ``None`` if never set."""
        row = self._read().execute("SELECT code_hash FROM watermarks WHERE producer = ?", (producer,)).fetchone()
        return row[0] if row is not None else None

    def set_watermark(self, producer: str, position: float, code_hash: str) -> None:
        """Persist a producer's progress and the code hash it was computed with."""
        self._write(
            lambda conn: conn.execute(
                "INSERT INTO watermarks (producer, position, code_hash) VALUES (?, ?, ?) "
                "ON CONFLICT(producer) DO UPDATE SET "
                "position = excluded.position, code_hash = excluded.code_hash",
                (producer, position, code_hash),
            )
        )

    # ------------------------------------------------------------------ ticks

    def tick_progress(self, producer: str, tick: str) -> float | None:
        """The last instant ``producer``'s tick ``tick`` ran for, or ``None``."""
        row = (
            self._read()
            .execute("SELECT due FROM tick_progress WHERE producer = ? AND tick = ?", (producer, tick))
            .fetchone()
        )
        return row[0] if row is not None else None

    def set_tick_progress(self, producer: str, tick: str, due: float, *, durable: bool = True) -> None:
        """Record that ``producer``'s tick ``tick`` ran for ``due``; never moves
        back. ``durable=False``: synced with the next durable write."""
        self._write(
            lambda conn: conn.execute(
                "INSERT INTO tick_progress (producer, tick, due) VALUES (?, ?, ?) "
                "ON CONFLICT(producer, tick) DO UPDATE SET due = MAX(due, excluded.due)",
                (producer, tick, due),
            ),
            durable=durable,
        )

    # ------------------------------------------------------------------ annotations

    def record_emitted_annotation(self, source: str, annotation_id: str, time_start: float) -> None:
        """Record that ``source`` (one ``AnnotationOutput``) published
        ``annotation_id`` with this ``time_start``.

        :meth:`AnnotationOutput.clear_window` only deletes ids read from this
        table, so a producer cannot delete an annotation it did not emit.
        Idempotent on ``(source, annotation_id)``.
        """
        self._write(
            lambda conn: conn.execute(
                "INSERT OR REPLACE INTO emitted_annotations (source, annotation_id, time_start) VALUES (?, ?, ?)",
                (source, annotation_id, time_start),
            )
        )

    def emitted_annotations_in_window(self, source: str, start: float, end: float) -> list[tuple[str, float]]:
        """``(annotation_id, time_start)`` pairs ``source`` itself recorded
        emitting, with ``start <= time_start < end`` (half-open, matching
        :meth:`window`), ordered by ``time_start``."""
        rows = (
            self._read()
            .execute(
                "SELECT annotation_id, time_start FROM emitted_annotations "
                "WHERE source = ? AND time_start >= ? AND time_start < ? ORDER BY time_start ASC",
                (source, start, end),
            )
            .fetchall()
        )
        return [(r[0], r[1]) for r in rows]

    # ------------------------------------------------------------------ backfill jobs

    def backfill_jobs(self, *, pending_only: bool = False) -> list[dict[str, Any]]:
        """Every backfill job, oldest first: ``producer``, ``job``, ``start``,
        ``end`` (``None`` for a first backfill that holds live dispatch and
        ends at the live edge; an independent one ends at its recorded
        boundary), ``position``, ``windows``, ``code_hash``, ``done`` and
        ``window`` (seconds per step, ``None`` when the job did not set one)."""
        rows = (
            self._read()
            .execute(
                'SELECT producer, job, start, "end", position, windows, code_hash, done, created_at, window_s '
                "FROM backfill_jobs WHERE done = 0 OR ? ORDER BY created_at, producer, job",
                (0 if pending_only else 1,),
            )
            .fetchall()
        )
        keys = ("producer", "job", "start", "end", "position", "windows", "code_hash", "done", "created_at", "window")
        return [{**dict(zip(keys, row, strict=True)), "done": bool(row[7])} for row in rows]

    def backfill_job(self, producer: str, job: str) -> dict[str, Any] | None:
        """One job (see :meth:`backfill_jobs`), or ``None``."""
        return next((j for j in self.backfill_jobs() if j["producer"] == producer and j["job"] == job), None)

    def add_backfill_job(
        self,
        producer: str,
        job: str,
        start: float,
        end: float | None,
        code_hash: str,
        *,
        created_at: float,
        window: float | None = None,
    ) -> bool:
        """Record a job at its start. A job that exists is left as it is, its
        end included; returns whether this call created it."""
        return self._write(
            lambda conn: (
                conn.execute(
                    'INSERT OR IGNORE INTO backfill_jobs (producer, job, start, "end", position, windows, code_hash, '
                    "done, created_at, window_s) VALUES (?, ?, ?, ?, ?, 0, ?, 0, ?, ?)",
                    (producer, job, start, end, start, code_hash, created_at, window),
                ).rowcount
                > 0
            )
        )

    def restart_backfill(self, producer: str, job: str, start: float, code_hash: str, *, created_at: float) -> None:
        """Record ``job`` anew from ``start``, ending at the live edge: a
        replay that starts over, whatever it was before."""
        self._write(
            lambda conn: conn.execute(
                'INSERT OR REPLACE INTO backfill_jobs (producer, job, start, "end", position, windows, code_hash, '
                "done, created_at, window_s) VALUES (?, ?, ?, NULL, ?, 0, ?, 0, ?, NULL)",
                (producer, job, start, start, code_hash, created_at),
            )
        )

    def set_backfill_end(self, producer: str, job: str, end: float) -> None:
        """Fix the end of a job that had none: a first backfill that no longer
        holds live dispatch ends where live dispatch began."""
        self._write(
            lambda conn: conn.execute(
                'UPDATE backfill_jobs SET "end" = ? WHERE producer = ? AND job = ? AND "end" IS NULL',
                (end, producer, job),
            )
        )

    def backfill_progress(self, producer: str, job: str, position: float, windows: int, code_hash: str) -> None:
        """Record the end of the last window a job completed. Inside
        :meth:`atomically` it commits with the rest of the block (the
        producer's checkpoint goes in the same commit)."""
        self._write(
            lambda conn: conn.execute(
                "UPDATE backfill_jobs SET position = ?, windows = ?, code_hash = ? WHERE producer = ? AND job = ?",
                (position, windows, code_hash, producer, job),
            )
        )

    def finish_backfill(self, producer: str, job: str) -> None:
        """Mark a job done (inside :meth:`atomically`, with the rest of the block)."""
        self._write(
            lambda conn: conn.execute(
                "UPDATE backfill_jobs SET done = 1 WHERE producer = ? AND job = ?", (producer, job)
            )
        )
