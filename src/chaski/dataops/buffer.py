"""SQLite-backed input buffer, a DataOps service's only local state.

One file under the service's data directory. ``points`` (the
retained window per input signal), ``watermarks`` (replay progress per
producer), ``meta`` (the store's ``generation``), and ``emitted_annotations``
(the ids each ``AnnotationOutput`` published, so ``clear_window`` only deletes
its own), ``backfill_jobs`` (each backfill's range and committed position, see
:mod:`chaski.dataops.backfill`), and ``pending_outputs`` (computed samples waiting for a signal binding
or a successful publish), and ``command_ledger`` (the commands this service
started executing and the answer each got, so a restart neither runs a command
twice nor answers it twice).

:meth:`Buffer.append` is idempotent on ``(signal_id, ts)``, so a record
processed again after a crash rewrites the same row. Cold start, recovery and
replay are therefore the same code path.
"""

from __future__ import annotations

import contextlib
import json
import logging
import sqlite3
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ulid

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger("chaski.dataops.buffer")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS producer_checkpoints (
    producer TEXT PRIMARY KEY, code_hash TEXT NOT NULL,
    version INTEGER NOT NULL, state TEXT NOT NULL, positions TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_inputs (
    offset INTEGER PRIMARY KEY, ts REAL NOT NULL, record TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pending_inputs_time ON pending_inputs(ts, offset);

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
CREATE INDEX IF NOT EXISTS idx_points_signal_ts ON points (signal_id, ts);

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
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._upgrade()
        self._conn.commit()
        self._deferred = 0
        self.generation: str = self._load_or_mint_generation()

    def _upgrade(self) -> None:
        """Add the columns a buffer written by an older release lacks."""
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(backfill_jobs)")}
        if "window_s" not in columns:
            self._conn.execute("ALTER TABLE backfill_jobs ADD COLUMN window_s REAL")

    def checkpoint(self, producer: str):
        """Return a producer's versioned JSON state and handled input offsets."""
        with self._lock:
            row = self._conn.execute(
                "SELECT code_hash, version, state, positions FROM producer_checkpoints WHERE producer=?",
                (producer,),
            ).fetchone()
        return (row[0], row[1], json.loads(row[2]), json.loads(row[3])) if row else None

    def save_checkpoint(self, producer, code_hash, version, state, positions):
        """Persist state before input acknowledgement, in the enclosing batch commit."""
        encoded = json.dumps(state, allow_nan=False)
        offsets = json.dumps(positions, allow_nan=False)
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO producer_checkpoints VALUES (?,?,?,?,?)",
                (producer, code_hash, version, encoded, offsets),
            )
            if not self._deferred:
                self._conn.commit()

    def queue_inputs(self, records, scanned_to, *, limit=100000):
        """Commit the coordinated inbox and intake offset before broker ack.

        An acknowledgement retry cannot reinsert already dispatched records.
        The separate processing watermark advances only after callbacks finish.
        """
        from dataclasses import asdict

        from .ingest import Ingest

        with self._lock, self._conn:
            row = self._conn.execute("SELECT value FROM meta WHERE key='input_intake'").fetchone()
            previous = int(row[0]) if row else 0
            fresh = [r for r in records if r.offset > previous]
            count = self._conn.execute("SELECT COUNT(*) FROM pending_inputs").fetchone()[0]
            if count + len(fresh) > limit:
                raise BufferError("coordinated input inbox reached its durable queue limit")
            self._conn.executemany(
                "INSERT OR IGNORE INTO pending_inputs VALUES (?,?,?)",
                [(r.offset, Ingest._timestamp_of(r), json.dumps(asdict(r), allow_nan=False)) for r in fresh],
            )
            self._conn.execute(
                "INSERT INTO meta VALUES ('input_intake',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(max(previous, scanned_to)),),
            )

    def input_batch(self, through, *, real_signals=(), real=False, after=0, limit=1000):
        """Read one bounded inbox batch in its declared clock domain."""
        from chaski.door import Record

        if real and not real_signals:
            return []
        query = (
            "SELECT record FROM pending_inputs WHERE offset>? "
            "AND json_extract(record,'$.payload.signal_id') IN (SELECT value FROM json_each(?)) "
            "ORDER BY offset LIMIT ?"
            if real
            else "SELECT record FROM pending_inputs WHERE ts<=? "
            "AND json_extract(record,'$.payload.signal_id') NOT IN (SELECT value FROM json_each(?)) "
            "ORDER BY ts,offset LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(
                query,
                (after if real else through, json.dumps(list(real_signals)), limit),
            ).fetchall()
            return [Record(**json.loads(row[0])) for row in rows]

    def pending_input_start(self, signal_ids):
        """Earliest queued event for these inputs, excluded from state replay."""
        if not signal_ids:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(ts) FROM pending_inputs WHERE "
                "json_extract(record,'$.payload.signal_id') IN (SELECT value FROM json_each(?))",
                (json.dumps(list(signal_ids)),),
            ).fetchone()
            return row[0]

    def finish_input(self, offset):
        """Delete only after the callback's effects have completed durably."""
        with self._lock:
            self._conn.execute("DELETE FROM pending_inputs WHERE offset=?", (offset,))
            if not self._deferred:
                self._conn.commit()

    def queue_output(self, source, timestamp, value, *, limit=10000):
        """Persist unbound samples; backpressure instead of evicting history."""
        encoded = json.dumps(value, allow_nan=False)
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM pending_outputs WHERE source=? AND ts=?", (source, timestamp)
            ).fetchone()
            if (
                not exists
                and self._conn.execute("SELECT COUNT(*) FROM pending_outputs WHERE source=?", (source,)).fetchone()[0]
                >= limit
            ):
                raise BufferError(f"unbound output {source!r} reached its durable queue limit")
            self._conn.execute(
                "INSERT INTO pending_outputs VALUES (?,?,?) ON CONFLICT(source,ts) DO UPDATE SET value=excluded.value",
                (source, timestamp, encoded),
            )

    def pending_outputs(self, source, *, limit=1000):
        with self._lock:
            return [
                (ts, json.loads(value))
                for ts, value in self._conn.execute(
                    "SELECT ts,value FROM pending_outputs WHERE source=? ORDER BY ts LIMIT ?", (source, limit)
                )
            ]

    def output_sent(self, source, timestamp):
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM pending_outputs WHERE source=? AND ts=?", (source, timestamp))

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> Buffer:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ identity

    def _load_or_mint_generation(self) -> str:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = 'generation'").fetchone()
            if row is not None:
                return row[0]
            generation = str(ulid.new())
            self._conn.execute("INSERT INTO meta (key, value) VALUES ('generation', ?)", (generation,))
            self._conn.commit()
            log.info("Minted new buffer generation %s at %s", generation, self._path)
            return generation

    # ------------------------------------------------------------------ points

    def append(self, signal_id: str, ts: float, value: Any) -> None:
        """Insert or overwrite one point. Idempotent on ``(signal_id, ts)``.

        Committed at once, or at the end of the enclosing :meth:`one_commit`.
        """
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO points (signal_id, ts, value) VALUES (?, ?, ?)",
                (signal_id, ts, json.dumps(value)),
            )
            if not self._deferred:
                self._conn.commit()

    # ─── command ledger ───────────────────────────────────────────────

    def command_entry(self, correlation_id: str) -> tuple[bool, str | None]:
        """``(started, answer)`` of a command: whether this service started
        executing it, and the answer it recorded (JSON), if any."""
        with self._lock:
            row = self._conn.execute(
                "SELECT answer FROM command_ledger WHERE correlation_id=?", (correlation_id,)
            ).fetchone()
        return (row is not None, row[0] if row is not None else None)

    def command_started(self, correlation_id: str, *, keep_s: float = 3600.0) -> None:
        """Record, durably and before the handler runs, that a command is
        being executed. Entries older than ``keep_s`` are dropped."""
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM command_ledger WHERE started_at < ?", (now - keep_s,))
            self._conn.execute(
                "INSERT OR IGNORE INTO command_ledger (correlation_id, started_at) VALUES (?, ?)",
                (correlation_id, now),
            )

    def command_answered(self, correlation_id: str, answer: str) -> None:
        """Record a command's answer before it is published."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO command_ledger (correlation_id, started_at, answer) VALUES (?, ?, ?) "
                "ON CONFLICT(correlation_id) DO UPDATE SET answer=excluded.answer",
                (correlation_id, time.time(), answer),
            )

    @contextlib.contextmanager
    def one_commit(self) -> Iterator[None]:
        """Commit the :meth:`append` calls inside the block once, when it ends.

        Reads inside the block already see the points. A crash inside it loses
        them, so use it only where they are appended again after a crash, as
        for a page that is acked after the block.
        """
        with self._lock:
            self._deferred += 1
        try:
            yield
        finally:
            with self._lock:
                self._deferred -= 1
                if not self._deferred:
                    self._conn.commit()

    def append_many(self, points: list[tuple[str, float, Any]]) -> None:
        """Commit adjacent input-only records together, before any handler runs."""
        values = [(signal, ts, json.dumps(value)) for signal, ts, value in points]
        if not values:
            return
        with self._lock, self._conn:
            self._conn.executemany("INSERT OR REPLACE INTO points (signal_id, ts, value) VALUES (?, ?, ?)", values)

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
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, value FROM points WHERE signal_id = ? AND ts >= ? AND ts < ? ORDER BY ts ASC",
                (signal_id, start, end),
            ).fetchall()
        return [(ts, json.loads(value)) for ts, value in rows]

    def latest_before(self, signal_id: str, before: float) -> tuple[float, Any] | None:
        """Latest point for ``signal_id`` with ``ts <= before``, or ``None``."""
        with self._lock:
            row = self._conn.execute(
                "SELECT ts, value FROM points WHERE signal_id = ? AND ts <= ? ORDER BY ts DESC LIMIT 1",
                (signal_id, before),
            ).fetchone()
        if row is None:
            return None
        return (row[0], json.loads(row[1]))

    def earliest(self, signal_id: str) -> float | None:
        """``MIN(ts)`` for ``signal_id``, or ``None`` if the buffer holds nothing for it."""
        with self._lock:
            row = self._conn.execute("SELECT MIN(ts) FROM points WHERE signal_id = ?", (signal_id,)).fetchone()
        return row[0] if row and row[0] is not None else None

    def latest(self, signal_id: str) -> float | None:
        """``MAX(ts)`` for ``signal_id``, or ``None`` if the buffer holds nothing for it."""
        with self._lock:
            row = self._conn.execute("SELECT MAX(ts) FROM points WHERE signal_id = ?", (signal_id,)).fetchone()
        return row[0] if row and row[0] is not None else None

    def trim(self, horizons: dict[str, float], *, now: float | None = None) -> int:
        """Trim history while retaining the value in force at each window start.

        Keep the newest point before the cutoff as an anchor for latest-value
        reads and change-only signals. That is at most one extra row per signal.
        Only signals present as keys in ``horizons`` are touched — a
        signal absent from the dict keeps every point it has. Never
        touches ``watermarks`` or ``meta``. Returns the total rows deleted.
        """
        now = time.time() if now is None else now
        deleted = 0
        with self._lock:
            for signal_id, horizon in horizons.items():
                cutoff = now - horizon
                cur = self._conn.execute(
                    "DELETE FROM points WHERE signal_id = ? AND ts < "
                    "(SELECT MAX(ts) FROM points WHERE signal_id = ? AND ts < ?)",
                    (signal_id, signal_id, cutoff),
                )
                deleted += cur.rowcount
            self._conn.commit()
        return deleted

    # ------------------------------------------------------------------ watermarks

    def watermark(self, producer: str) -> float | None:
        """The producer's last-processed position, or ``None`` if never set."""
        with self._lock:
            row = self._conn.execute("SELECT position FROM watermarks WHERE producer = ?", (producer,)).fetchone()
        return row[0] if row is not None else None

    def code_hash(self, producer: str) -> str | None:
        """The code hash last stored for the producer, or ``None`` if never set."""
        with self._lock:
            row = self._conn.execute("SELECT code_hash FROM watermarks WHERE producer = ?", (producer,)).fetchone()
        return row[0] if row is not None else None

    def set_watermark(self, producer: str, position: float, code_hash: str) -> None:
        """Persist a producer's progress and the code hash it was computed with."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO watermarks (producer, position, code_hash) VALUES (?, ?, ?) "
                "ON CONFLICT(producer) DO UPDATE SET "
                "position = excluded.position, code_hash = excluded.code_hash",
                (producer, position, code_hash),
            )
            self._conn.commit()

    # ------------------------------------------------------------------ emitted annotations

    def record_emitted_annotation(self, source: str, annotation_id: str, time_start: float) -> None:
        """Record that ``source`` (one ``AnnotationOutput``) published
        ``annotation_id`` with this ``time_start``.

        :meth:`AnnotationOutput.clear_window` only deletes ids read from this
        table, so a producer cannot delete an annotation it did not emit.
        Idempotent on ``(source, annotation_id)``.
        """
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO emitted_annotations (source, annotation_id, time_start) VALUES (?, ?, ?)",
                (source, annotation_id, time_start),
            )
            self._conn.commit()

    def emitted_annotations_in_window(self, source: str, start: float, end: float) -> list[tuple[str, float]]:
        """``(annotation_id, time_start)`` pairs ``source`` itself recorded
        emitting, with ``start <= time_start < end`` (half-open, matching
        :meth:`window`), ordered by ``time_start``."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT annotation_id, time_start FROM emitted_annotations "
                "WHERE source = ? AND time_start >= ? AND time_start < ? ORDER BY time_start ASC",
                (source, start, end),
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    # ------------------------------------------------------------------ backfill jobs

    def backfill_jobs(self, *, pending_only: bool = False) -> list[dict[str, Any]]:
        """Every backfill job, oldest first: ``producer``, ``job``, ``start``,
        ``end`` (``None`` for a first backfill that holds live dispatch and
        ends at the live edge; an independent one ends at its recorded
        boundary), ``position``, ``windows``, ``code_hash``, ``done`` and
        ``window`` (seconds per step, ``None`` when the job did not set one)."""
        with self._lock:
            rows = self._conn.execute(
                'SELECT producer, job, start, "end", position, windows, code_hash, done, created_at, window_s '
                "FROM backfill_jobs WHERE done = 0 OR ? ORDER BY created_at, producer, job",
                (0 if pending_only else 1,),
            ).fetchall()
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
        with self._lock, self._conn:
            cur = self._conn.execute(
                'INSERT OR IGNORE INTO backfill_jobs (producer, job, start, "end", position, windows, code_hash, '
                "done, created_at, window_s) VALUES (?, ?, ?, ?, ?, 0, ?, 0, ?, ?)",
                (producer, job, start, end, start, code_hash, created_at, window),
            )
            return cur.rowcount > 0

    def set_backfill_end(self, producer: str, job: str, end: float) -> None:
        """Fix the end of a job that had none: a first backfill that no longer
        holds live dispatch ends where live dispatch began."""
        with self._lock, self._conn:
            self._conn.execute(
                'UPDATE backfill_jobs SET "end" = ? WHERE producer = ? AND job = ? AND "end" IS NULL',
                (end, producer, job),
            )

    def backfill_progress(self, producer: str, job: str, position: float, windows: int, code_hash: str) -> None:
        """Record the end of the last window a job completed. Committed at
        once, or with the enclosing :meth:`one_commit` (the producer's
        checkpoint goes in the same commit)."""
        with self._lock:
            self._conn.execute(
                "UPDATE backfill_jobs SET position = ?, windows = ?, code_hash = ? WHERE producer = ? AND job = ?",
                (position, windows, code_hash, producer, job),
            )
            if not self._deferred:
                self._conn.commit()

    def finish_backfill(self, producer: str, job: str) -> None:
        """Mark a job done. Committed at once, or with the enclosing :meth:`one_commit`."""
        with self._lock:
            self._conn.execute("UPDATE backfill_jobs SET done = 1 WHERE producer = ? AND job = ?", (producer, job))
            if not self._deferred:
                self._conn.commit()
