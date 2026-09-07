"""SQLite-backed input buffer — a DataOps service's only local state.

Kept in one file under the service's data directory
(the dataops evaluator design §3): a
``points`` table holding the retained window per declared input signal, a
``watermarks`` table tracking per-producer replay progress, a ``meta``
table holding the store's own identity (its ``generation``), and an
``emitted_annotations`` table recording the ids each ``AnnotationOutput``
has itself published (design §10) — the record that lets
``clear_window`` delete only annotations a producer actually emitted.

Every :meth:`Buffer.append` is idempotent on ``(signal_id, ts)`` — a crash
between an append and the corresponding cursor ack simply re-writes the
same row when the ingest loop re-fetches the record. That is what makes
cold start, disaster recovery, and replay the same code path (design §3,
§6): re-processing a record is always safe.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd
import ulid

log = logging.getLogger("chaski.dataops.buffer")

_SCHEMA = """
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
        self._conn.commit()
        self.generation: str = self._load_or_mint_generation()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "Buffer":
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
        """Insert or overwrite one point. Idempotent on ``(signal_id, ts)``."""
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO points (signal_id, ts, value) VALUES (?, ?, ?)",
                (signal_id, ts, json.dumps(value)),
            )
            self._conn.commit()

    def window(self, signal_id: str, start: float, end: float) -> pd.DataFrame:
        """Points for ``signal_id`` with ``start <= ts < end``, ordered by ts.

        Returns a DataFrame with columns ``ts`` (float) and ``value`` (the
        original Python type, decoded from JSON). Empty — but correctly
        columned — when nothing matches.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, value FROM points WHERE signal_id = ? AND ts >= ? AND ts < ? ORDER BY ts ASC",
                (signal_id, start, end),
            ).fetchall()
        return pd.DataFrame(
            {"ts": [r[0] for r in rows], "value": [json.loads(r[1]) for r in rows]},
            columns=["ts", "value"],
        )

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

    def trim(self, horizons: dict[str, float]) -> int:
        """Delete points older than each signal's declared horizon (seconds).

        Only signals present as keys in ``horizons`` are touched — a
        signal absent from the dict keeps every point it has. Never
        touches ``watermarks`` or ``meta``. Returns the total rows deleted.
        """
        now = time.time()
        deleted = 0
        with self._lock:
            for signal_id, horizon in horizons.items():
                cutoff = now - horizon
                cur = self._conn.execute(
                    "DELETE FROM points WHERE signal_id = ? AND ts < ?", (signal_id, cutoff)
                )
                deleted += cur.rowcount
            self._conn.commit()
        return deleted

    # ------------------------------------------------------------------ watermarks

    def watermark(self, producer: str) -> float | None:
        """The producer's last-processed position, or ``None`` if never set."""
        with self._lock:
            row = self._conn.execute(
                "SELECT position FROM watermarks WHERE producer = ?", (producer,)
            ).fetchone()
        return row[0] if row is not None else None

    def code_hash(self, producer: str) -> str | None:
        """The code hash last stored for the producer, or ``None`` if never set."""
        with self._lock:
            row = self._conn.execute(
                "SELECT code_hash FROM watermarks WHERE producer = ?", (producer,)
            ).fetchone()
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
        """Record that ``source`` (one ``AnnotationOutput``, identified the
        same way a catalogue entry is — design §5, §10) itself published
        ``annotation_id`` with this ``time_start``.

        This is what makes :meth:`AnnotationOutput.clear_window` safe: it
        only ever deletes ids read back from THIS table, so a producer
        structurally cannot delete an annotation it never emitted — there is
        no query that lets it name an arbitrary id. Idempotent on
        ``(source, annotation_id)``: re-emitting the same logical interval
        (same derived id) just re-writes the same row.
        """
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO emitted_annotations (source, annotation_id, time_start) "
                "VALUES (?, ?, ?)",
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
