"""Unit tests for chaski.dataops.inputs — buffer-backed, KV-resolved declared inputs.

No live colca and no real Postgres: a fake ``Door`` (KV only) plus a real
``Buffer`` over a tmp SQLite file stand in for the runtime, and a small
stand-in implements the :class:`Historian` port where a test needs
"historian configured". Inputs are read the way a producer reads them —
through an instance attached to a runtime — because that is the only way
they reach a door or a buffer now (no module globals).
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from dataops_fakes import FakeDoor, FakeRuntime, kv_entry

from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.inputs import SignalRangeInput, WindowExceedsRetentionError, validate_windows


class _Holder(Producer):
    """The producer an input under test is declared on."""

    name = "holder"
    heartbeat = SignalRangeInput("heartbeat")
    missing = SignalRangeInput("no_such_signal")


class _FakeHistorian:
    def __init__(self, window_rows=None, latest=None) -> None:
        self.window_rows = window_rows or []
        self.latest = latest
        self.window_calls: list[tuple[str, float, float]] = []

    def window(self, signal_id, start, end):
        self.window_calls.append((signal_id, start, end))
        return pd.DataFrame({"ts": [r[0] for r in self.window_rows], "value": [r[1] for r in self.window_rows]},
                            columns=["ts", "value"])

    def latest_before(self, signal_id, before):
        return self.latest


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


@pytest.fixture
def door():
    return FakeDoor([kv_entry("colca/v1/_Signal/n-1/line1/heartbeat", {"id": "sig-1", "name": "heartbeat"})])


@pytest.fixture
def runtime(door, buffer):
    return FakeRuntime(door, buffer)


def _holder(runtime) -> _Holder:
    return _Holder().attach(runtime)


# ─── signal_id resolution ────────────────────────────────────────────────


def test_signal_id_resolves_via_kv(runtime):
    assert _holder(runtime).heartbeat.signal_id == "sig-1"


def test_signal_id_raises_lookup_error_when_unresolved(runtime):
    with pytest.raises(LookupError):
        _ = _holder(runtime).missing.signal_id


def test_an_input_read_on_the_class_is_the_declaration_not_a_reader():
    """``Holder.heartbeat`` is what validate_windows and the dispatch
    builder walk; reading a value through it has no runtime to reach."""
    assert isinstance(_Holder.heartbeat, SignalRangeInput)
    with pytest.raises(RuntimeError, match="producer instance"):
        _ = _Holder.heartbeat.signal_id


def test_each_instance_reads_through_its_own_runtime(door, buffer, tmp_path):
    """Per-instance state: two instances of one producer class attached to
    two runtimes resolve against two doors — the module-global binding this
    replaced would have made them share one."""
    other_door = FakeDoor([kv_entry("colca/v1/_Signal/n-2/heartbeat", {"id": "sig-other", "name": "heartbeat"})])
    other_buffer = Buffer(tmp_path / "other.sqlite3")
    try:
        a = _Holder().attach(FakeRuntime(door, buffer))
        b = _Holder().attach(FakeRuntime(other_door, other_buffer))
        assert a.heartbeat.signal_id == "sig-1"
        assert b.heartbeat.signal_id == "sig-other"
        assert a.heartbeat is a.heartbeat, "the per-instance copy is created once and kept"
        assert a.heartbeat is not b.heartbeat
    finally:
        other_buffer.close()


# ─── fetch(): buffer / historian source-transparency ────────────────────


def test_fetch_reads_from_buffer_when_range_is_covered(buffer, runtime):
    buffer.append("sig-1", 100.0, 1)
    buffer.append("sig-1", 110.0, 2)
    buffer.append("sig-1", 200.0, 3)

    df = _holder(runtime).heartbeat.fetch(100.0, 200.0)

    assert list(df["ts"]) == [100.0, 110.0]
    assert list(df["value"]) == [1, 2]


def test_fetch_before_buffer_without_historian_raises(buffer, runtime):
    buffer.append("sig-1", 100.0, "first")

    with pytest.raises(RuntimeError, match="no historian"):
        _holder(runtime).heartbeat.fetch(0.0, 200.0)


def test_fetch_before_buffer_with_historian_merges(buffer, door):
    buffer.append("sig-1", 100.0, "from-buffer")
    historian = _FakeHistorian(window_rows=[(50.0, "from-historian")])
    holder = _holder(FakeRuntime(door, buffer, historian))

    df = holder.heartbeat.fetch(0.0, 200.0)

    assert list(df["value"]) == ["from-historian", "from-buffer"]
    assert historian.window_calls == [("sig-1", 0.0, 100.0)], "the historian serves only the part before the buffer"


def test_fetch_returns_empty_when_buffer_empty_and_no_historian(runtime):
    df = _holder(runtime).heartbeat.fetch(0.0, 200.0)
    assert list(df.columns) == ["ts", "value"]
    assert len(df) == 0


# ─── latest_value_before / latest_timestamp_before / is_fresh ───────────


def test_latest_value_before_reads_buffer(buffer, runtime):
    buffer.append("sig-1", 10.0, "old")
    buffer.append("sig-1", 20.0, "new")

    holder = _holder(runtime)
    assert holder.heartbeat.latest_value_before(25.0) == "new"
    assert holder.heartbeat.latest_timestamp_before(25.0) == 20.0


def test_latest_value_before_falls_back_to_historian(buffer, door):
    holder = _holder(FakeRuntime(door, buffer, _FakeHistorian(latest=(5.0, 42.0))))

    assert holder.heartbeat.latest_value_before(0.0) == 42.0


def test_latest_value_before_returns_none_without_historian(runtime):
    assert _holder(runtime).heartbeat.latest_value_before(0.0) is None


def test_is_fresh_true_within_window(buffer, runtime):
    now = time.time()
    buffer.append("sig-1", now - 5.0, "v")

    assert _holder(runtime).heartbeat.is_fresh(10.0, now=now) is True


def test_is_fresh_false_outside_window(buffer, runtime):
    now = time.time()
    buffer.append("sig-1", now - 50.0, "v")

    assert _holder(runtime).heartbeat.is_fresh(10.0, now=now) is False


def test_is_fresh_false_when_never_seen(runtime):
    assert _holder(runtime).heartbeat.is_fresh(10.0) is False


def test_latest_value_property_reads_the_current_value(buffer, runtime):
    now = time.time()
    buffer.append("sig-1", now - 1.0, "current")

    assert _holder(runtime).heartbeat.latest_value == "current"


# ─── window parsing ──────────────────────────────────────────────────────


def test_window_accepts_duration_strings():
    assert SignalRangeInput("x", window="24h").window_s == 24 * 3600
    assert SignalRangeInput("x", window="30d").window_s == 30 * 86400
    assert SignalRangeInput("x").window_s == 3600  # default "1h"


# ─── validate_windows (design §4.1) ──────────────────────────────────────


def test_validate_windows_passes_when_historian_configured():
    class P:
        name = "p"
        x = SignalRangeInput("x", window="30d")

    validate_windows([P], retention_s=14 * 86400, historian_configured=True)  # must not raise


def test_validate_windows_passes_when_window_within_retention():
    class P:
        name = "p"
        x = SignalRangeInput("x", window="1h")

    validate_windows([P], retention_s=14 * 86400, historian_configured=False)  # must not raise


def test_validate_windows_raises_named_error_over_retention():
    class ThirtyDayProducer:
        name = "thirty_day_producer"
        my_input = SignalRangeInput("part_counter", window="30d")

    retention_s = 14 * 86400.0

    with pytest.raises(WindowExceedsRetentionError) as exc_info:
        validate_windows([ThirtyDayProducer], retention_s=retention_s, historian_configured=False)

    message = str(exc_info.value)
    assert "ThirtyDayProducer" in message
    assert "my_input" in message
    assert "part_counter" in message
    assert str(int(30 * 86400)) in message
    assert str(int(retention_s)) in message


def test_validate_windows_checks_inherited_inputs():
    class Base:
        heartbeat = SignalRangeInput("heartbeat", window="30d")

    class Concrete(Base):
        name = "concrete"

    with pytest.raises(WindowExceedsRetentionError, match="heartbeat"):
        validate_windows([Concrete], retention_s=14 * 86400, historian_configured=False)


def test_validate_windows_accepts_instances_too():
    class P:
        name = "p"
        x = SignalRangeInput("x", window="30d")

    with pytest.raises(WindowExceedsRetentionError):
        validate_windows([P()], retention_s=14 * 86400, historian_configured=False)


def test_the_ingest_hot_path_does_not_re_read_kv(door, runtime):
    """`signal_id` is read per METRIC, and it used to scan KV on every read.

    Twice over, since `resolve_signal` looks up the element first. colca
    serves /kv at five a second because it is a SCAN class, so a node
    ingesting a few hundred metrics a second answered most of those reads
    with HTTP 429 — and each one surfaced as an `on_metric` handler failing
    on a signal it had already resolved successfully at startup.
    """
    signal = _holder(runtime).heartbeat

    first = signal.signal_id
    for _ in range(200):
        assert signal.signal_id == first

    assert door.kv_calls == 1, (
        f"{door.kv_calls} KV scans for 201 reads of one id — colca allows 5 per second"
    )


def test_forget_makes_the_next_read_resolve_again(door, runtime):
    """The denominator for the test above: the id is held, not frozen.

    The service calls `forget()` at the top of every resolution pass, so a
    rebound signal is picked up at that cadence — which is the same cadence
    the dispatch table it builds is rebuilt on.
    """
    signal = _holder(runtime).heartbeat
    assert signal.signal_id == "sig-1"

    door.entries = [kv_entry("colca/v1/_Signal/n-1/line1/heartbeat", {"id": "sig-2", "name": "heartbeat"})]
    assert signal.signal_id == "sig-1", "an unforced read must not pay for a scan"

    signal.forget()
    assert signal.signal_id == "sig-2"
