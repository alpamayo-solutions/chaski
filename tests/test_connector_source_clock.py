"""ConnectorService measures its source's clock under timestamp_source="source"
and reports it to Telemetry.source_clock (chaski.source_clock)."""

from __future__ import annotations

import pytest
from test_connector_service import FakeDriver, FakeNode, RecordingTelemetry, bind, poll, started

import chaski
from chaski.connector import Reading
from chaski.source_clock import SourceClockStatus

T0 = 1_800_000_000.0


@pytest.fixture(autouse=True)
def durable_state(tmp_path, monkeypatch):
    monkeypatch.setenv("COLCA_STATE_DIR", str(tmp_path))


@pytest.fixture
def node() -> FakeNode:
    return FakeNode()


class Wall:
    """The connector's wall clock, advanced by the test."""

    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> float:
        return self.now


class StampingDriver(FakeDriver):
    """Stamps each reading at the wall clock plus ``offset`` minus ``delay``;
    ``stamp_topics`` limits stamping to those sources (None: all)."""

    source_timestamps = "live"

    def __init__(self, wall: Wall, *, offset: float = 0.0, delay: float = 0.0) -> None:
        super().__init__()
        self.wall = wall
        self.offset = offset
        self.delay = delay
        self.stamp_sources: set[str] | None = None
        self.tags["Axis2/Speed"] = {"name": "Speed", "data_type": "float"}
        self.values["Axis2/Speed"] = 1.0

    async def read(self, targets):
        readings = []
        for signal, handle, topic in targets:
            if handle not in self.values:
                continue
            stamped = self.stamp_sources is None or handle in self.stamp_sources
            stamp = self.wall.now + self.offset - self.delay if stamped else None
            readings.append(Reading(topic, self.values[handle], signal, stamp))
        return readings


class ClockTelemetry(RecordingTelemetry):
    def __init__(self) -> None:
        super().__init__()
        self.clock: list[SourceClockStatus] = []

    def source_clock(self, status: SourceClockStatus) -> None:
        self.clock.append(status)


def run(node: FakeNode, driver: FakeDriver, monkeypatch, wall: Wall, *, minutes: int = 5, **kwargs):
    telemetry = ClockTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry, **kwargs)
    svc._wall = wall
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    bind(node, svc, "Axis2/Speed", path="line1/speed", signal_id="s-speed")
    for _ in range(minutes * 6):
        poll(svc)
        wall.now += 10.0
    return svc, telemetry


def test_a_source_two_seconds_ahead_is_reported_to_telemetry(node, monkeypatch):
    wall = Wall()
    driver = StampingDriver(wall, offset=2.0)
    svc, telemetry = run(node, driver, monkeypatch, wall, timestamp_source="source")
    last = telemetry.clock[-1]
    assert last.skew_s == pytest.approx(2.0)
    assert last.jitter_s == pytest.approx(0.0)
    assert last.live and last.unstamped == 0 and last.invalid == 0
    assert svc.source_clock_status().skew_s == pytest.approx(2.0)
    # Reported after every poll, the same object the accessor computes.
    assert len(telemetry.clock) == 30


def test_late_delivery_from_a_correct_clock_is_not_skew(node, monkeypatch):
    """Readings arrive 0..9 s after the source saw them; the source's clock is
    right. The estimate sits at the smallest delay, never at the average."""
    wall = Wall()
    driver = StampingDriver(wall)
    telemetry = ClockTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry, timestamp_source="source")
    svc._wall = wall
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    for i in range(300):
        driver.delay = (i * 7) % 10  # 0, 7, 4, 1, 8, ... one zero per 10 polls
        poll(svc)
        wall.now += 1.0
    assert telemetry.clock[-1].skew_s == pytest.approx(0.0)


def test_acquisition_mode_measures_and_reports_nothing(node, monkeypatch):
    wall = Wall()
    driver = StampingDriver(wall, offset=2.0)
    svc, telemetry = run(node, driver, monkeypatch, wall)
    assert telemetry.clock == []
    assert svc.source_clock_status() is None
    assert svc._source_clock is None


def test_a_source_mode_reading_without_a_timestamp_is_written_at_the_poll_time_and_not_counted(node, monkeypatch):
    wall = Wall()
    driver = StampingDriver(wall, offset=2.0)
    driver.stamp_sources = set()
    _svc, telemetry = run(node, driver, monkeypatch, wall, timestamp_source="source")
    # Written at the poll's own time, as before this feature.
    stamps = {m.timestamp for _t, m in node.metrics() if m.signal_id in ("s-temp", "s-speed")}
    assert stamps and all(T0 <= s < wall.now for s in stamps)
    assert all(s == T0 + 10.0 * round((s - T0) / 10.0) for s in stamps)
    last = telemetry.clock[-1]
    assert last.skew_s is None and last.jitter_s is None
    assert last.stamped == 0 and last.measured == 0 and last.unstamped > 0


def test_a_driver_that_never_stamps_is_counted_as_unstamped_only(node, monkeypatch):
    wall = Wall()
    driver = FakeDriver()  # plain (topic, value, signal) tuples
    telemetry = ClockTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry, timestamp_source="source")
    svc._wall = wall
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    for _ in range(60):
        poll(svc)
        wall.now += 10.0
    last = telemetry.clock[-1]
    assert last.skew_s is None and last.jitter_s is None
    assert last.stamped == 0 and last.unstamped > 0
    assert last.live is False  # FakeDriver keeps the default "past"


def test_a_driver_stamping_only_some_tags_is_measured_from_those(node, monkeypatch):
    wall = Wall()
    driver = StampingDriver(wall, offset=-1.5)
    driver.stamp_sources = {"Axis1/Temperature"}
    _svc, telemetry = run(node, driver, monkeypatch, wall, timestamp_source="source")
    last = telemetry.clock[-1]
    assert last.skew_s == pytest.approx(-1.5)
    assert last.stamped == last.unstamped > 0


def test_an_unset_plc_clock_is_counted_invalid_and_written_as_stamped(node, monkeypatch):
    """A PLC at epoch 0: its samples are still written at the time it stamps
    (that is what source mode asks for), counted invalid, and no skew."""
    wall = Wall()
    driver = StampingDriver(wall, offset=-T0)
    _svc, telemetry = run(node, driver, monkeypatch, wall, timestamp_source="source")
    last = telemetry.clock[-1]
    assert last.invalid == last.stamped > 0
    assert last.skew_s is None


def test_a_past_timestamps_driver_is_not_measured(node, monkeypatch):
    wall = Wall()
    driver = StampingDriver(wall, delay=900.0)
    driver.source_timestamps = "past"
    _svc, telemetry = run(node, driver, monkeypatch, wall, timestamp_source="source")
    last = telemetry.clock[-1]
    assert last.live is False and last.stamped > 0
    assert last.skew_s is None


def test_an_unknown_source_timestamps_declaration_is_refused(node, monkeypatch):
    driver = FakeDriver()
    driver.source_timestamps = "sometimes"
    with pytest.raises(ValueError, match="source_timestamps"):
        started(node, driver, monkeypatch, timestamp_source="source")


def test_the_status_type_is_public():
    assert chaski.SourceClockStatus is SourceClockStatus
    assert "SourceClockStatus" in chaski.__all__
