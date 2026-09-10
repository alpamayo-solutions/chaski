"""Tests for chaski.dataops.buffer.Buffer, a DataOps service's local SQLite state."""

from __future__ import annotations

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
    buffer.append("sig-1", now - 100.0, "old")
    buffer.append("sig-1", now - 1.0, "recent")

    deleted = buffer.trim({"sig-1": 10.0})

    assert deleted == 1
    df = buffer.window("sig-1", 0.0, now + 1.0)
    assert list(df["value"]) == ["recent"]


def test_trim_leaves_signals_absent_from_horizons_untouched(buffer):
    import time

    now = time.time()
    buffer.append("sig-1", now - 1000.0, "ancient")
    buffer.append("sig-2", now - 1000.0, "also-ancient")

    deleted = buffer.trim({"sig-1": 10.0})

    assert deleted == 1
    assert len(buffer.window("sig-1", 0.0, now + 1.0)) == 0
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
