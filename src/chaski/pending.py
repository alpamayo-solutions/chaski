"""Durable, bounded samples waiting for output binding and broker acceptance."""

import json
import sqlite3
import threading


class PendingSamples:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS samples (id INTEGER PRIMARY KEY, source TEXT, value TEXT, ts REAL, unit TEXT)"
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS samples_source ON samples(source,id)")
        self.db.commit()

    def append(self, source, value, ts, unit, limit):
        encoded = json.dumps(value, allow_nan=False)
        with self.lock, self.db:
            if self.db.execute("SELECT COUNT(*) FROM samples WHERE source=?", (source,)).fetchone()[0] >= limit:
                raise BufferError(f"chaski.Service: output {source!r} buffer is full")
            self.db.execute("INSERT INTO samples(source,value,ts,unit) VALUES (?,?,?,?)", (source, encoded, ts, unit))

    def append_batch(self, rows, limit):
        """Atomically accept a whole acquisition cycle, never evict samples."""
        encoded = [(source, json.dumps(value, allow_nan=False), ts, unit) for source, value, ts, unit in rows]
        with self.lock, self.db:
            count = self.db.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
            if count + len(encoded) > limit:
                raise BufferError("connector durable sample queue is full; acquisition must wait")
            self.db.executemany("INSERT INTO samples(source,value,ts,unit) VALUES (?,?,?,?)", encoded)

    def page_all(self, limit=1000):
        with self.lock:
            return [
                (i, source, json.loads(value), ts, unit)
                for i, source, value, ts, unit in self.db.execute(
                    "SELECT id,source,value,ts,unit FROM samples ORDER BY id LIMIT ?", (limit,)
                )
            ]

    def sources(self):
        with self.lock:
            return [row[0] for row in self.db.execute("SELECT DISTINCT source FROM samples")]

    def page(self, source, limit=200):
        with self.lock:
            return [
                (i, json.loads(value), ts, unit)
                for i, value, ts, unit in self.db.execute(
                    "SELECT id,value,ts,unit FROM samples WHERE source=? ORDER BY id LIMIT ?", (source, limit)
                )
            ]

    def ack(self, ids):
        with self.lock, self.db:
            self.db.executemany("DELETE FROM samples WHERE id=?", ((i,) for i in ids))

    def close(self):
        with self.lock:
            self.db.close()
