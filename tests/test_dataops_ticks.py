"""Durable tick progress and missed ticks (chaski.dataops.ticks)."""

from __future__ import annotations

import datetime
import time

import pytest
from dataops_fakes import FakeDoor, FakeRuntime

from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.service import off_loop
from chaski.dataops.ticks import GRACE_S, MissedTicks, due, instants, live_trigger
from chaski.dataops.triggers import CronSpec, IntervalSpec, cron, every
from chaski.dataops.worker import ProducerWorker


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


def test_interval_instants_are_multiples_of_the_interval():
    spec = IntervalSpec(seconds=30.0)
    assert instants(spec, 100.0, 200.0) == [120.0, 150.0, 180.0]
    assert instants(spec, 120.0, 180.0) == [150.0, 180.0]
    assert instants(spec, 100.0, 200.0, limit=2) == [150.0, 180.0]
    assert due(spec, 179.9) == 150.0


def test_cron_instants_follow_the_expression():
    spec = CronSpec(expression="0 * * * *")
    start = datetime.datetime(2026, 11, 5, 0, 30, tzinfo=datetime.UTC).timestamp()
    found = instants(spec, start, start + 3 * 3600)
    assert len(found) == 3
    assert all(f % 60 == 0 for f in found)


def test_the_live_interval_trigger_fires_on_the_same_instants():
    spec = IntervalSpec(seconds=30.0)
    now = 1_700_000_017.0
    trigger = live_trigger(spec, now)
    first = trigger.get_next_fire_time(None, datetime.datetime.fromtimestamp(now, datetime.UTC))
    assert first.timestamp() % 30.0 == 0.0


class Oee(Producer):
    name = "oee"
    system_element_name = "press"

    @every("30s")
    async def tick(self) -> None:
        self.ran.append(self.now)

    def __init__(self) -> None:
        super().__init__()
        self.ran: list[float] = []


class CatchingUp(Oee):
    name = "oee_catching_up"

    @every("30s", catch_up=True)
    async def tick(self) -> None:
        self.ran.append(self.now)


class Derived(Oee):
    """Inherits ``tick`` (no catch-up) and flags only a tick of its own."""

    name = "oee_derived"

    @every("1m", catch_up=True)
    async def own(self) -> None:
        self.ran.append(self.now)


def _spec(cls: type[Producer], method: str) -> IntervalSpec | CronSpec:
    (spec,) = [spec for name, spec in cls._triggers if name == method]
    return spec


def _missed(buffer, requests, findings):
    def request(producer: str, start: float, end: float, tick: str) -> str:
        requests.append((producer, start, end, tick))
        buffer.add_backfill_job(producer, f"ticks-{tick}", start, end, "", created_at=time.time())
        return f"ticks-{tick}"

    return MissedTicks(buffer, request=request, publish=lambda reason, payload: findings.append((reason, payload)))


def test_a_tick_records_its_instant_and_coalesces_a_short_delay(buffer):
    requests, findings = [], []
    missed = _missed(buffer, requests, findings)
    producer = CatchingUp()
    spec = _spec(CatchingUp, "tick")
    missed.ran(producer, "tick", spec, 1_000_020.0)
    # One instant late, within the grace: coalesced, as before.
    missed.ran(producer, "tick", spec, 1_000_080.0)
    assert buffer.tick_progress("oee_catching_up", "tick") == 1_000_080.0
    assert requests == [] and missed.status() == []


def test_a_stall_is_reported_and_caught_up_for_a_tick_that_opts_in(buffer):
    requests, findings = [], []
    missed = _missed(buffer, requests, findings)
    spec = _spec(CatchingUp, "tick")
    producer = CatchingUp()
    missed.ran(producer, "tick", spec, 1_000_020.0)
    missed.ran(producer, "tick", spec, 1_000_020.0 + 3600.0)
    ((name, start, end, tick),) = requests
    assert (name, tick) == ("oee_catching_up", "tick")
    assert start == 1_000_050.0
    assert end > 1_000_020.0 + 3600.0 - GRACE_S - 30.0
    (span,) = missed.status()
    assert span["missed"] == len(instants(spec, 1_000_020.0, 1_000_020.0 + 3600.0 - GRACE_S))
    assert span["catch_up"] == "ticks-tick"
    assert findings[-1][0] == "missed_ticks" and findings[-1][1]["suggested_severity"] == "warning"


def test_a_tick_that_does_not_opt_in_is_reported_and_not_run_again(buffer):
    requests, findings = [], []
    missed = _missed(buffer, requests, findings)
    spec = _spec(Oee, "tick")
    producer = Oee()
    missed.ran(producer, "tick", spec, 1_000_020.0)
    missed.ran(producer, "tick", spec, 1_000_020.0 + 3 * 86400.0)
    assert requests == []
    assert missed.uncaught() == ["oee.tick"]
    assert "not run again" in findings[-1][1]["summary"]


def test_a_subclass_catches_up_only_the_ticks_it_flags_not_the_ones_it_inherits(buffer):
    """A producer built on another (a machine on MachineState) flags its own
    ticks; the inherited tick that keeps state between ticks stays live-only,
    as declared where it is written."""
    assert not _spec(Derived, "tick").catch_up
    assert _spec(Derived, "own").catch_up
    requests, findings = [], []
    missed = _missed(buffer, requests, findings)
    producer = Derived()
    for tick in ("tick", "own"):
        spec = _spec(Derived, tick)
        missed.ran(producer, tick, spec, 1_000_020.0)
        missed.ran(producer, tick, spec, 1_000_020.0 + 3600.0)
    assert [tick for _, _, _, tick in requests] == ["own"]
    assert missed.uncaught() == ["oee_derived.tick"]
    caught = {span["tick"]: span["catch_up"] for span in missed.status()}
    assert caught == {"tick": None, "own": "ticks-own"}


def test_catch_up_is_declared_on_cron_ticks_too():
    class Nightly(Producer):
        name = "nightly"
        system_element_name = "press"

        @cron("0 2 * * *", catch_up=True)
        async def report(self) -> None:
            pass

        @cron("0 3 * * *")
        async def cleanup(self) -> None:
            pass

    assert _spec(Nightly, "report") == CronSpec("0 2 * * *", catch_up=True)
    assert not _spec(Nightly, "cleanup").catch_up


def test_the_finding_is_retired_when_the_catch_up_is_done(buffer):
    requests, findings = [], []
    missed = _missed(buffer, requests, findings)
    spec = _spec(CatchingUp, "tick")
    producer = CatchingUp()
    missed.ran(producer, "tick", spec, 1_000_020.0)
    missed.ran(producer, "tick", spec, 1_000_020.0 + 3600.0)
    buffer.finish_backfill("oee_catching_up", "ticks-tick")
    missed.refresh()
    assert missed.status() == []
    assert findings[-1] == ("missed_ticks", None)


def test_a_live_tick_runs_on_the_worker_and_records_its_progress(buffer):
    runtime = FakeRuntime(FakeDoor([]), buffer)
    requests, findings = [], []
    runtime.missed_ticks = _missed(buffer, requests, findings)
    producer = Oee().attach(runtime)
    producer._worker = ProducerWorker(producer.name)
    try:
        spec = IntervalSpec(seconds=30.0)
        off_loop(producer.tick, spec)()
        assert len(producer.ran) == 1
        assert buffer.tick_progress("oee", "tick") == due(spec, time.time())
    finally:
        producer._worker.stop()
