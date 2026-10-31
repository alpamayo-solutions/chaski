"""The source-clock estimate: the skew math, the delay rule, the window and
what does not count (chaski.source_clock)."""

from __future__ import annotations

import math

import pytest

from chaski.source_clock import EARLIEST_PLAUSIBLE, SourceClock

T0 = 1_800_000_000.0  # 2027-01-15, a node wall-clock time


def feed(clock: SourceClock, *, minutes: int, offset: float, delays=(0.0,), topic="t", start=T0) -> float:
    """One new source timestamp per second per delay, for ``minutes``: the
    source observed at ``received - delay`` on a clock ``offset`` ahead.
    Returns the last receive time."""
    received = start
    for second in range(minutes * 60):
        received = start + second
        for i, delay in enumerate(delays):
            clock.observe(f"{topic}{i}", received - delay + offset, received)
    return received


def test_a_source_ahead_reads_as_positive_skew_and_behind_as_negative():
    for offset in (2.0, -3.5):
        clock = SourceClock(live=True)
        now = feed(clock, minutes=5, offset=offset)
        status = clock.status(now)
        assert status.skew_s == pytest.approx(offset)
        assert status.jitter_s == pytest.approx(0.0)
        assert status.minutes >= 3


def test_delivery_delay_is_not_counted_as_skew():
    """Every reading arrives late, most by seconds; one per minute arrives at
    once. The estimate is the clock offset, not an average of the delays."""
    clock = SourceClock(live=True)
    now = feed(clock, minutes=10, offset=0.25, delays=(0.0, 1.5, 4.0, 30.0))
    assert clock.status(now).skew_s == pytest.approx(0.25)


def test_a_uniformly_late_source_reads_behind_by_its_smallest_delay_never_ahead():
    clock = SourceClock(live=True)
    now = feed(clock, minutes=5, offset=0.0, delays=(0.8, 2.0))
    skew = clock.status(now).skew_s
    assert skew == pytest.approx(-0.8)
    assert skew <= 0.0


def test_one_glitched_minute_each_way_does_not_move_the_estimate():
    clock = SourceClock(live=True)
    now = feed(clock, minutes=6, offset=1.0)
    clock.observe("glitch", T0, T0)  # baseline for the topic
    clock.observe("glitch", now + 3600, now)  # an hour ahead, in the last minute
    status = clock.status(now)
    assert status.skew_s == pytest.approx(1.0)
    assert status.invalid == 0


def test_jitter_is_the_spread_of_the_per_minute_estimates_without_the_extremes():
    clock = SourceClock(live=True)
    received = T0
    for minute, offset in enumerate([0.0, 0.1, 0.2, 0.3, 0.4, 5.0]):
        for second in range(60):
            received = T0 + minute * 60 + second
            clock.observe("t", received + offset, received)
    status = clock.status(received)
    # 0.0 and 5.0 are discarded; 0.1..0.4 remain.
    assert status.skew_s == pytest.approx(0.4)
    assert status.jitter_s == pytest.approx(0.3)


def test_the_window_forgets_minutes_older_than_fifteen_minutes():
    clock = SourceClock(live=True)
    last = feed(clock, minutes=20, offset=9.0)
    now = feed(clock, minutes=15, offset=1.0, start=last + 1)
    status = clock.status(now)
    assert status.skew_s == pytest.approx(1.0)
    assert status.minutes <= 15
    assert len(clock._buckets) <= 16


def test_memory_is_fixed_whatever_the_reading_rate():
    clock = SourceClock(live=True)
    feed(clock, minutes=60, offset=0.0, delays=tuple(0.01 * i for i in range(10)))
    assert len(clock._buckets) <= 16
    assert len(clock._last_stamp) == 10


def test_a_quiet_window_has_no_estimate():
    clock = SourceClock(live=True)
    now = feed(clock, minutes=5, offset=1.0)
    status = clock.status(now + 16 * 60)
    assert status.skew_s is None and status.jitter_s is None
    assert status.stamped == status.measured == 0


def test_fewer_than_three_minutes_with_a_new_timestamp_is_no_estimate():
    clock = SourceClock(live=True)
    now = feed(clock, minutes=2, offset=1.0)
    status = clock.status(now)
    assert status.minutes == 2
    assert status.skew_s is None and status.jitter_s is None
    assert status.measured > 0


# ── what does not count ──────────────────────────────────────────────────


def test_a_reading_without_a_timestamp_counts_as_unstamped_and_nothing_else():
    clock = SourceClock(live=True)
    for second in range(600):
        clock.observe("t", None, T0 + second)
    status = clock.status(T0 + 600)
    assert status.unstamped == 600
    assert status.stamped == status.measured == status.invalid == 0
    assert status.skew_s is None and status.jitter_s is None


def test_a_source_that_stamps_only_some_readings_is_measured_from_those():
    clock = SourceClock(live=True)
    received = T0
    for second in range(300):
        received = T0 + second
        clock.observe("stamped", received + 0.5, received)
        clock.observe("bare", None, received)
    status = clock.status(received)
    assert status.skew_s == pytest.approx(0.5)
    assert status.stamped == status.unstamped == 300


def test_a_repeated_timestamp_is_a_steady_value_not_an_observation():
    """MTConnect current: a value that has not changed for three days keeps
    its old timestamp. It is neither skew nor invalid."""
    clock = SourceClock(live=True)
    old = T0 - 3 * 86_400
    for second in range(600):
        clock.observe("steady", old, T0 + second)
    status = clock.status(T0 + 600)
    assert status.stamped == 600
    assert status.measured == status.invalid == 0
    assert status.skew_s is None


def test_the_first_timestamp_of_a_topic_counts_neither_way():
    clock = SourceClock(live=True)
    clock.observe("t", T0 - 7200, T0)
    status = clock.status(T0)
    assert status.stamped == 1 and status.measured == 0 and status.invalid == 0


@pytest.mark.parametrize(
    "stamp",
    [0.0, 1.0, EARLIEST_PLAUSIBLE - 1, math.nan, math.inf, -math.inf],
    ids=["epoch-0", "1970", "before-2000", "nan", "inf", "-inf"],
)
def test_an_unset_or_broken_source_clock_is_invalid_even_when_repeated(stamp):
    clock = SourceClock(live=True)
    now = feed(clock, minutes=5, offset=0.2, topic="good")
    for second in range(120):
        clock.observe("plc", stamp, now - 120 + second)
    status = clock.status(now)
    assert status.invalid == 120
    assert status.skew_s == pytest.approx(0.2)


def test_far_future_is_invalid_every_time_and_far_past_only_when_new():
    clock = SourceClock(live=True)
    received = T0
    for second in range(300):
        received = T0 + second
        clock.observe("future", received + 2 * 86_400, received)
        clock.observe("past", received - 2 * 86_400, received)  # new each time
    status = clock.status(received)
    assert status.invalid == 300 + 299  # the first "past" stamp has no baseline
    assert status.measured == 0
    assert status.skew_s is None


def test_a_driver_with_past_timestamps_is_never_measured():
    """Energy slots delivered after the slot ends: every reading is at least
    a slot late. Counted, never turned into a skew."""
    clock = SourceClock(live=False)
    received = T0
    for slot in range(10):
        received = T0 + slot * 60
        clock.observe("slot", received - 900 - slot % 7, received)
    status = clock.status(received)
    assert status.live is False
    assert status.stamped == 10 and status.measured == 0
    assert status.skew_s is None and status.jitter_s is None


def test_forget_drops_topics_no_longer_bound():
    clock = SourceClock(live=True)
    clock.observe("a", T0, T0)
    clock.observe("b", T0, T0)
    clock.forget({"a"})
    assert set(clock._last_stamp) == {"a"}


@pytest.mark.parametrize(
    "kwargs",
    [{"window": 0}, {"bucket": 0}, {"window": 30, "bucket": 60}, {"min_minutes": 2}],
)
def test_invalid_settings_are_refused(kwargs):
    with pytest.raises(ValueError):
        SourceClock(live=True, **kwargs)
