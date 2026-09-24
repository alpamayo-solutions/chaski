import asyncio
from dataclasses import replace

import pytest
from colca_data_contracts.payload import ClockDefinition

from chaski.clock import Clock, ClockNotReady, project, validate_definition

TOPIC = "colca/v1/_ClockDefinition/hub/factory"


class Time:
    wall = 10_000.0
    mono = 0.0

    def clock(self, **kw):
        return Clock(wall=lambda: self.wall, monotonic=lambda: self.mono, **kw)

    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds


def definition(**kw):
    return replace(ClockDefinition("factory", "run1", 1, 10000, 1000, 100), **kw)


def test_default_needs_no_ntp_or_broker_and_ignores_beacons():
    time = Time()
    clock = time.clock()
    clock.apply_time(90000000)
    assert clock.now() == time.wall
    assert clock.status().ready


def test_mqtt_uses_monotonic_not_local_wall_and_requires_fresh_beacon():
    time = Time()
    clock = time.clock(source="mqtt", max_sync_age=30)
    with pytest.raises(ClockNotReady):
        clock.now()
    clock.apply_time(2000000)
    time.wall = -500
    time.mono = 20
    assert clock.now() == 2020
    time.mono = 31
    assert not clock.status().ready
    clock.apply_time(2031000)
    assert clock.now() == 2031
    clock.reconnect()
    assert not clock.status().ready
    with pytest.raises(ValueError, match="retained"):
        clock.apply_time(2031000, retained=True)


def test_accelerate_pause_resume_and_revision_replay():
    time = Time()
    clock = time.clock(definition_topic=TOPIC)
    assert not clock.status().ready
    first = definition()
    clock.apply_definition(first)
    time.advance(5)
    assert clock.now() == 1500
    paused = definition(revision=2, real_anchor=time.wall, factory_anchor=1500, rate=0)
    clock.apply_definition(paused)
    time.advance(50)
    assert clock.now() == 1500
    assert not clock.apply_definition(first)
    clock.reconnect()
    assert not clock.status().ready
    assert not clock.apply_definition(paused)
    assert clock.now() == 1500
    clock.apply_definition(definition(revision=3, real_anchor=time.wall, factory_anchor=1500, rate=10))
    time.advance(2)
    assert clock.now() == 1520


def test_definition_conflict_run_change_and_tombstone_do_not_rewind():
    time = Time()
    clock = time.clock(definition_topic=TOPIC)
    clock.apply_definition(definition())
    assert clock.now() == 1000
    for value in [definition(rate=2), definition(run_id="run2"), definition(id="other")]:
        with pytest.raises(ValueError):
            clock.apply_definition(value)
    clock.remove_definition()
    with pytest.raises(ClockNotReady):
        clock.now()
    clock.apply_definition(definition(revision=2, factory_anchor=500))
    assert "behind" in clock.status().reason


def test_stop_and_catch_up_do_not_overshoot():
    time = Time()
    clock = time.clock()
    clock.apply_definition(definition(rate=1000, catch_up=True))
    time.advance(20)
    assert clock.now() == time.wall
    assert clock.rate == 1
    time.advance(20)
    assert clock.now() == time.wall
    clock = time.clock()
    clock.apply_definition(definition(rate=1000, stop_at=2000))
    assert clock.now() == 2000
    assert clock.rate == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": 1.1},
        {"revision": True},
        {"rate": -1},
        {"rate": 1001},
        {"real_anchor": float("nan")},
        {"rate": float("inf")},
        {"stop_at": 999},
        {"run_id": ""},
        {"run_id": 1},
        {"id": True},
        {"catch_up": "false"},
        {"catch_up": True, "rate": 0.5},
    ],
)
def test_invalid_definitions(changes):
    with pytest.raises(ValueError):
        validate_definition(definition(**changes))


def test_wait_responds_to_pause_resume_and_is_cancellable():
    async def run():
        time = Time()
        clock = time.clock()
        clock.apply_definition(definition(rate=0))
        pending = asyncio.create_task(clock.sleep_until(1002, responsiveness=0.001))
        await asyncio.sleep(0.005)
        assert not pending.done()
        clock.apply_definition(definition(revision=2, rate=2))
        time.advance(1)
        await asyncio.wait_for(pending, 0.1)
        pending = asyncio.create_task(clock.sleep_until(999999))
        await asyncio.sleep(0)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

    asyncio.run(run())


def test_projection_is_reproducible_for_independent_consumers():
    clock = definition(rate=1000)
    assert [project(clock, t) for t in [10000, 10001, 10002]] == [1000, 2000, 3000]


def test_projection_matches_the_brokers_shared_vectors():
    import json
    from importlib.resources import files

    from colca_data_contracts.payload import ClockDefinition

    cases = json.loads(files("colca_data_contracts").joinpath("vectors/application_time.json").read_text())
    for case in cases:
        clock = ClockDefinition(**case["clock"])
        validate_definition(clock)
        assert project(clock, case["real"]) == case["expected"], case["name"]


def test_future_effective_pause_uses_previous_segment_until_transition():
    time = Time()
    clock = time.clock()
    clock.apply_definition(definition())
    clock.apply_definition(definition(revision=2, real_anchor=10005, factory_anchor=1500, rate=0))
    time.advance(2)
    assert clock.now() == 1200
    assert clock.rate == 100
    time.advance(3)
    assert clock.now() == 1500
    assert clock.rate == 0


def test_future_initial_start_does_not_poll_early():
    time = Time()
    clock = time.clock()
    clock.apply_definition(definition(real_anchor=10005))
    assert clock.now() == 1000
    assert clock.rate == 0


def test_reconnecting_before_scheduled_pause_reconstructs_previous_segment():
    from colca_data_contracts.payload import ClockSegment

    time = Time()
    clock = time.clock(definition_topic=TOPIC)
    # A fresh process has only the latest retained record, no in-memory past.
    clock.apply_definition(
        definition(revision=2, real_anchor=10005, factory_anchor=1500, rate=0, previous=ClockSegment(10000, 1000, 100))
    )
    time.advance(2)
    assert clock.now() == 1200
    assert clock.rate == 100
    time.advance(4)
    assert clock.now() == 1500
    assert clock.rate == 0


def test_retained_definition_cannot_be_mutated_by_caller():
    from colca_data_contracts.payload import ClockSegment

    time = Time()
    clock = time.clock()
    value = definition(
        revision=2, real_anchor=10005, factory_anchor=1500, rate=0, previous=ClockSegment(10000, 1000, 100)
    )
    clock.apply_definition(value)
    value.previous.rate = 500
    clock.definition.previous.rate = 900
    time.advance(2)
    assert clock.now() == 1200
