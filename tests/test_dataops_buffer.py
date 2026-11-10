"""Tests for chaski.dataops.buffer.Buffer, a DataOps service's local SQLite state."""

from __future__ import annotations

import sqlite3

import pytest

from chaski.dataops.buffer import Buffer


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "buffer.sqlite3"


@pytest.fixture
def buffer(db_path):
    b = Buffer(db_path)
    try:
        yield b
    finally:
        b.close()


# ------------------------------------------------------------------ append / window


def test_append_and_window_returns_points_ordered_by_ts(buffer):
    buffer.append("sig-1", 20.0, 2)
    buffer.append("sig-1", 10.0, 1)
    buffer.append("sig-1", 30.0, 3)

    df = buffer.window("sig-1", 0.0, 100.0)

    assert list(df["ts"]) == [10.0, 20.0, 30.0]
    assert list(df["value"]) == [1, 2, 3]


def test_window_is_half_open_start_inclusive_end_exclusive(buffer):
    buffer.append("sig-1", 10.0, "at-start")
    buffer.append("sig-1", 15.0, "in-middle")
    buffer.append("sig-1", 20.0, "at-end")

    df = buffer.window("sig-1", 10.0, 20.0)

    assert list(df["value"]) == ["at-start", "in-middle"]


def test_window_excludes_other_signals_and_out_of_range_points(buffer):
    buffer.append("sig-1", 10.0, "in")
    buffer.append("sig-2", 10.0, "wrong-signal")
    buffer.append("sig-1", 999.0, "out-of-range")

    df = buffer.window("sig-1", 0.0, 100.0)

    assert list(df["value"]) == ["in"]


def test_window_returns_empty_dataframe_with_expected_columns_when_no_points(buffer):
    df = buffer.window("sig-missing", 0.0, 100.0)

    assert list(df.columns) == ["ts", "value"]
    assert len(df) == 0


def test_append_is_idempotent_on_signal_id_and_ts(buffer):
    buffer.append("sig-1", 10.0, "first")
    buffer.append("sig-1", 10.0, "second")

    df = buffer.window("sig-1", 0.0, 100.0)

    assert len(df) == 1
    assert df["value"].iloc[0] == "second"


# ------------------------------------------------------------------ value round-trip


def test_value_round_trip_preserves_python_types(buffer):
    buffer.append("sig-bool", 1.0, True)
    buffer.append("sig-int", 1.0, 1)
    buffer.append("sig-float", 1.0, 1.5)
    buffer.append("sig-str", 1.0, "hello")
    buffer.append("sig-json", 1.0, {"nested": [1, 2, 3]})

    assert buffer.latest_before("sig-bool", 2.0) == (1.0, True)
    assert buffer.latest_before("sig-int", 2.0) == (1.0, 1)
    assert buffer.latest_before("sig-float", 2.0) == (1.0, 1.5)
    assert buffer.latest_before("sig-str", 2.0) == (1.0, "hello")
    assert buffer.latest_before("sig-json", 2.0) == (1.0, {"nested": [1, 2, 3]})

    # SQLite has no bool type, so values go through JSON. `is True` checks
    # exactly, where `==` would accept 1.
    bool_value = buffer.latest_before("sig-bool", 2.0)[1]
    assert bool_value is True
    int_value = buffer.latest_before("sig-int", 2.0)[1]
    assert int_value == 1
    assert int_value is not True


# ------------------------------------------------------------------ latest_before / earliest


def test_latest_before_is_inclusive_of_the_boundary(buffer):
    buffer.append("sig-1", 10.0, "at-boundary")
    buffer.append("sig-1", 20.0, "after")

    assert buffer.latest_before("sig-1", 10.0) == (10.0, "at-boundary")


def test_latest_before_returns_none_when_nothing_qualifies(buffer):
    buffer.append("sig-1", 100.0, "too-late")

    assert buffer.latest_before("sig-1", 10.0) is None


def test_latest_before_returns_most_recent_qualifying_point(buffer):
    buffer.append("sig-1", 10.0, "old")
    buffer.append("sig-1", 15.0, "newer")
    buffer.append("sig-1", 999.0, "future")

    assert buffer.latest_before("sig-1", 20.0) == (15.0, "newer")


def test_earliest_returns_min_ts_or_none(buffer):
    assert buffer.earliest("sig-missing") is None

    buffer.append("sig-1", 30.0, "a")
    buffer.append("sig-1", 10.0, "b")
    buffer.append("sig-1", 20.0, "c")

    assert buffer.earliest("sig-1") == 10.0


# ------------------------------------------------------------------ watermarks


def test_watermark_and_code_hash_round_trip(buffer):
    assert buffer.watermark("machine_state") is None
    assert buffer.code_hash("machine_state") is None

    buffer.set_watermark("machine_state", 123.5, "hash-abc")

    assert buffer.watermark("machine_state") == 123.5
    assert buffer.code_hash("machine_state") == "hash-abc"


def test_set_watermark_overwrites_previous_value(buffer):
    buffer.set_watermark("machine_state", 1.0, "hash-1")
    buffer.set_watermark("machine_state", 2.0, "hash-2")

    assert buffer.watermark("machine_state") == 2.0
    assert buffer.code_hash("machine_state") == "hash-2"


def test_watermark_persists_across_reopen(db_path):
    b1 = Buffer(db_path)
    b1.set_watermark("machine_state", 42.0, "hash-x")
    b1.close()

    b2 = Buffer(db_path)
    try:
        assert b2.watermark("machine_state") == 42.0
        assert b2.code_hash("machine_state") == "hash-x"
    finally:
        b2.close()


# ------------------------------------------------------------------ generation


def test_generation_is_stable_across_reopen(db_path):
    b1 = Buffer(db_path)
    generation = b1.generation
    b1.close()

    b2 = Buffer(db_path)
    try:
        assert b2.generation == generation
    finally:
        b2.close()


def test_generation_is_fresh_after_the_file_is_deleted(db_path):
    b1 = Buffer(db_path)
    generation = b1.generation
    b1.close()

    db_path.unlink()

    b2 = Buffer(db_path)
    try:
        assert b2.generation != generation
    finally:
        b2.close()


# ------------------------------------------------------------------ trim


def test_trim_removes_only_past_horizon_rows(buffer):
    import time

    now = time.time()
    buffer.append("sig-1", now - 200.0, "obsolete")
    buffer.append("sig-1", now - 100.0, "old")
    buffer.append("sig-1", now - 1.0, "recent")

    deleted = buffer.trim({"sig-1": 10.0})

    assert deleted == 1
    df = buffer.window("sig-1", 0.0, now + 1.0)
    assert list(df["value"]) == ["old", "recent"]
    assert buffer.latest_before("sig-1", now - 10)[1] == "old"


def test_trim_leaves_signals_absent_from_horizons_untouched(buffer):
    import time

    now = time.time()
    buffer.append("sig-1", now - 1000.0, "ancient")
    buffer.append("sig-2", now - 1000.0, "also-ancient")

    deleted = buffer.trim({"sig-1": 10.0})

    assert deleted == 0
    assert len(buffer.window("sig-1", 0.0, now + 1.0)) == 1
    assert len(buffer.window("sig-2", 0.0, now + 1.0)) == 1


def test_trim_does_not_touch_watermarks_or_meta(buffer):
    import time

    buffer.set_watermark("machine_state", 5.0, "hash-1")
    generation_before = buffer.generation

    buffer.append("sig-1", time.time() - 1000.0, "old")
    buffer.trim({"sig-1": 10.0})

    assert buffer.watermark("machine_state") == 5.0
    assert buffer.code_hash("machine_state") == "hash-1"
    assert buffer.generation == generation_before


def _old_points(buffer, signals: int, per_signal: int) -> None:
    buffer.append_many([(f"sig-{s}", float(ts), 1.0) for s in range(signals) for ts in range(per_signal)])


def _writer_statements(buffer) -> list[str]:
    """Record every statement the writer connection runs, in order."""
    statements: list[str] = []
    buffer._writer.conn.set_trace_callback(statements.append)
    return statements


def test_every_write_is_committed_when_it_returns(db_path):
    """No write waits for a later commit: another connection sees it at once."""
    buffer = Buffer(db_path)
    other = sqlite3.connect(str(db_path))
    try:
        buffer.append_many([("sig-1", 1.0, 1.0), ("sig-1", 2.0, 2.0)])
        assert other.execute("SELECT COUNT(*) FROM points").fetchone()[0] == 2
        buffer.append("sig-1", 3.0, 3.0)
        buffer.save_checkpoint("p", "h", 1, {"n": 1}, {"m": 3})
        assert other.execute("SELECT COUNT(*) FROM points").fetchone()[0] == 3
        assert other.execute("SELECT COUNT(*) FROM producer_checkpoints").fetchone()[0] == 1
        assert not buffer._writer.conn.in_transaction
    finally:
        other.close()
        buffer.close()


def test_atomically_commits_its_writes_together_or_not_at_all(db_path):
    buffer = Buffer(db_path)
    other = sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        buffer.add_backfill_job("p", "repair-1", 0.0, 100.0, "", created_at=1.0)

        def commit() -> None:
            buffer.save_checkpoint("p", "h", 1, {"n": 1}, {})
            # Reads inside the block see its own writes.
            assert buffer.checkpoint("p") == ("h", 1, {"n": 1}, {})
            assert other.execute("SELECT COUNT(*) FROM producer_checkpoints").fetchone()[0] == 0
            buffer.backfill_progress("p", "repair-1", 50.0, 1, "h")

        buffer.atomically(commit)
        assert other.execute("SELECT COUNT(*) FROM producer_checkpoints").fetchone()[0] == 1
        assert buffer.backfill_job("p", "repair-1")["position"] == 50.0

        def fail() -> None:
            buffer.save_checkpoint("p", "h", 1, {"n": 2}, {})
            buffer.backfill_progress("p", "repair-1", 60.0, 2, "h")
            raise RuntimeError("the window failed")

        with pytest.raises(RuntimeError):
            buffer.atomically(fail)
        assert buffer.checkpoint("p")[2] == {"n": 1}
        assert buffer.backfill_job("p", "repair-1")["position"] == 50.0
    finally:
        other.close()
        buffer.close()


def test_writes_from_many_threads_each_land_in_a_transaction_of_their_own(db_path):
    """The buffer had one connection, so one transaction, and a shared
    counter decided when to commit: a write from one thread could commit
    another thread's half-done work, or stay uncommitted after its caller
    acked. Now every write is its own transaction on the one writer."""
    import threading

    buffer = Buffer(db_path)
    statements = _writer_statements(buffer)
    try:
        threads = [
            threading.Thread(target=lambda n=n: [buffer.append(f"sig-{n}", float(i), i) for i in range(50)])
            for n in range(4)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        begins = [s for s in statements if s.startswith("BEGIN")]
        commits = [s for s in statements if s == "COMMIT"]
        assert len(begins) == len(commits) == 200
        assert sum(len(buffer.points(f"sig-{n}", 0, 100)) for n in range(4)) == 200
    finally:
        buffer.close()


def test_trim_runs_each_statement_as_a_transaction_of_its_own(db_path):
    """A trim during a catch-up page used to join the page's transaction and
    commit only with it (a 1 GB WAL on a plant edge). Each trim statement is
    now committed on its own."""
    buffer = Buffer(db_path)
    try:
        _old_points(buffer, 3, 25)
        statements = _writer_statements(buffer)

        deleted = buffer.trim({"sig-0": 10.0, "sig-1": 10.0, "sig-2": 10.0}, now=35.0, chunk=10, pause=lambda _s: None)

        # The cutoff is 25; the newest point before it (24) stays as the anchor.
        assert deleted == 3 * 24
        assert [ts for ts, _ in buffer.points("sig-1", 0.0, 100.0)] == [24.0]
        deletes = [i for i, s in enumerate(statements) if s.startswith("DELETE FROM points")]
        assert deletes
        for i in deletes:
            assert statements[i - 1].startswith("BEGIN") and statements[i + 1] == "COMMIT"
    finally:
        buffer.close()


def test_trim_pauses_after_each_statement_for_as_long_as_it_held_the_writer(buffer):
    _old_points(buffer, 2, 40)
    pauses: list[float] = []

    deleted = buffer.trim({"sig-0": 10.0, "sig-1": 10.0}, now=50.0, chunk=10, pause=pauses.append)

    assert deleted == 2 * 39
    assert pauses
    assert all(p >= 0.0 for p in pauses)


def test_a_slow_trim_lets_a_page_write_in_after_one_statement(db_path):
    """A trim of 6.8 M points after an outage held an edge's ingest for six
    minutes: it re-took the shared lock between chunks while the page ran
    handlers. Writes now queue on the writer between trim statements, each
    sized to ``batch_s``: a page's write waits for one statement at most."""
    import threading
    import time

    buffer = Buffer(db_path)
    try:
        _old_points(buffer, 8, 6000)

        def slow() -> int:
            time.sleep(0.0005)
            return 0

        buffer._writer.conn.set_progress_handler(slow, 2000)
        trim = threading.Thread(
            target=buffer.trim,
            args=({f"sig-{s}": 10.0 for s in range(8)},),
            kwargs={"now": 10_000.0, "chunk": 500, "batch_s": 0.02},
            name="trim",
        )
        trim.start()
        time.sleep(0.1)
        waits = []
        for i in range(20):
            began = time.monotonic()
            buffer.append_many([("live", 10_000.0 + i, 1.0)])
            waits.append(time.monotonic() - began)
            time.sleep(0.005)  # the page's handlers
        trim_running = trim.is_alive()
        trim.join(120)
        assert trim_running, "the trim ended before the page"
        assert max(waits) < 0.25, f"a page write waited {max(waits):.2f} s beside the trim"
    finally:
        buffer.close()


def test_a_reader_does_not_wait_for_a_long_write(db_path):
    """Readers have connections of their own (WAL): a tick reading its window
    is not held up by the writer, and sees the last committed state."""
    import threading
    import time

    buffer = Buffer(db_path)
    try:
        buffer.append("sig", 1.0, 1.0)
        started = threading.Event()

        def long_write(conn) -> None:
            conn.execute("INSERT INTO points VALUES ('sig', 2.0, '2')")
            started.set()
            time.sleep(0.5)

        writer = threading.Thread(target=buffer._write, args=(long_write,))
        writer.start()
        started.wait(5)
        began = time.monotonic()
        assert buffer.points("sig", 0, 10) == [(1.0, 1.0)]
        assert time.monotonic() - began < 0.2
        writer.join()
        assert buffer.points("sig", 0, 10) == [(1.0, 1.0), (2.0, 2.0)]
    finally:
        buffer.close()


def test_opening_an_old_buffer_drops_the_duplicate_points_index(db_path):
    old = sqlite3.connect(db_path)
    old.executescript(
        """
        CREATE TABLE points (signal_id TEXT NOT NULL, ts REAL NOT NULL, value TEXT NOT NULL,
                             PRIMARY KEY (signal_id, ts));
        CREATE INDEX idx_points_signal_ts ON points (signal_id, ts);
        INSERT INTO points VALUES ('sig-1', 10, '1');
        """
    )
    old.commit()
    old.close()
    buffer = Buffer(db_path)
    try:
        indexes = {row[0] for row in buffer._read().execute("SELECT name FROM sqlite_master WHERE type='index'")}
        assert "idx_points_signal_ts" not in indexes
        assert buffer.latest("sig-1") == 10
    finally:
        buffer.close()


def test_change_only_plan_survives_many_retention_intervals(buffer):
    plan = {"planned_work_order": "WO-1", "planned_job_card": "JC-1"}
    buffer.append("plan", 100, plan)
    for now in (1000, 2000, 10000):
        buffer.trim({"plan": 900}, now=now)
        assert buffer.latest_before("plan", now) == (100, plan)
    buffer.append("plan", 10100, {"planned_work_order": "WO-2"})
    buffer.trim({"plan": 900}, now=12000)
    assert buffer.points("plan", 0, 12001) == [(10100, {"planned_work_order": "WO-2"})]


# ------------------------------------------------------------------ upgrade


def test_a_buffer_from_the_application_time_releases_loses_its_clock_state(db_path):
    """chaski 0.25 kept factory-callback progress (``timers``, ``__clock__:``
    watermarks) and a coordinated inbox (``pending_inputs``, ``input_intake``).
    Opening such a buffer drops them once; everything else stays."""
    old = sqlite3.connect(db_path)
    old.executescript(
        """
        CREATE TABLE timers (timer TEXT PRIMARY KEY, run_id TEXT NOT NULL, position REAL NOT NULL);
        INSERT INTO timers VALUES ('__clock__:p:tick:x', 'run', 1060);
        CREATE TABLE pending_inputs (offset INTEGER PRIMARY KEY, ts REAL NOT NULL, record TEXT NOT NULL);
        INSERT INTO pending_inputs VALUES (7, 1010, '{}');
        CREATE TABLE watermarks (producer TEXT PRIMARY KEY, position REAL, code_hash TEXT);
        INSERT INTO watermarks VALUES ('__clock__:p:tick:x', 1060, NULL);
        INSERT INTO watermarks VALUES ('kept', 42, 'hash');
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO meta VALUES ('input_intake', '7');
        INSERT INTO meta VALUES ('generation', 'gen-1');
        CREATE TABLE points (signal_id TEXT NOT NULL, ts REAL NOT NULL, value TEXT NOT NULL,
                             PRIMARY KEY (signal_id, ts));
        INSERT INTO points VALUES ('sig-1', 10, '1');
        """
    )
    old.commit()
    old.close()

    buffer = Buffer(db_path)
    try:
        tables = {row[0] for row in buffer._read().execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert "timers" not in tables
        assert "pending_inputs" not in tables
        assert buffer.watermark("__clock__:p:tick:x") is None
        assert buffer.watermark("kept") == 42
        assert buffer.generation == "gen-1"
        assert buffer._read().execute("SELECT value FROM meta WHERE key='input_intake'").fetchone() is None
        assert buffer.latest("sig-1") == 10
    finally:
        buffer.close()

    # The second open finds nothing to drop and changes nothing.
    again = Buffer(db_path)
    try:
        assert again.watermark("kept") == 42
        assert again.generation == "gen-1"
    finally:
        again.close()
