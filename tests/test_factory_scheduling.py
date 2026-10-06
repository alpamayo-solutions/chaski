import asyncio
import threading
from types import SimpleNamespace

from colca_data_contracts.payload import ClockDefinition

from chaski.clock import Clock
from chaski.dataops.buffer import Buffer
from chaski.dataops.scheduling import next_tick, run_periodic
from chaski.dataops.triggers import CronSpec, IntervalSpec


def test_accelerated_callbacks_keep_exact_times_and_resume_without_skipping(tmp_path):
    async def run():
        clock = Clock(wall=lambda: 10005)
        clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 10))
        buffer = Buffer(tmp_path / "state.db")
        called = []

        class Producer:
            name = "scheduled"
            _lock = threading.RLock()
            runtime = SimpleNamespace(buffer=buffer)

            async def tick(self):
                called.append(clock.now())

        def offline_progress(timestamp):
            raise ConnectionError("broker unavailable")

        Producer.runtime.report_progress = offline_progress

        producer = Producer()
        task = asyncio.create_task(run_periodic(producer, "tick", IntervalSpec(10), clock))
        while len(called) < 5:
            await asyncio.sleep(0.001)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert called == [1010, 1020, 1030, 1040, 1050]
        assert clock.now() == 1050
        task = asyncio.create_task(run_periodic(producer, "tick", IntervalSpec(10), clock))
        await asyncio.sleep(0.01)
        assert len(called) == 5
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        buffer.close()

    asyncio.run(asyncio.wait_for(run(), 3))


def test_cron_uses_factory_utc_calendar():
    assert next_tick(CronSpec("* * * * *"), 1000) == 1020


def test_buffer_retains_historical_application_window(tmp_path):
    buffer = Buffer(tmp_path / "buffer.db")
    buffer.append("signal", 900, 0)
    buffer.append("signal", 1000, 1)
    buffer.append("signal", 1100, 2)
    assert buffer.trim({"signal": 100}, now=1150) == 1
    assert buffer.latest_before("signal", 1050) == (1000, 1)
    assert buffer.latest_before("signal", 1150) == (1100, 2)
    buffer.close()


def test_coordinated_callbacks_use_old_state_before_new_boundary_sample(tmp_path):
    from chaski.dataops.scheduling import run_due

    async def run():
        clock = Clock(wall=lambda: 10001)
        clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 1000, 1010, start_at=1000))
        buffer = Buffer(tmp_path / "state.db")
        called, value = [], [1]

        class Scheduled:
            name = "scheduled"
            _triggers = (("tick", IntervalSpec(5)),)
            _lock = threading.RLock()
            runtime = SimpleNamespace(buffer=buffer)

            async def tick(self):
                called.append((clock.now(), value[0]))

        producer = Scheduled()
        await run_due([producer], clock, 1010, inclusive=False)
        value[0] = 2  # Ingest the new machine sample at the boundary.
        await run_due([producer], clock, 1010, inclusive=True)
        await run_due([producer], clock, 1010, inclusive=True)
        assert called == [(1005, 1), (1010, 2)]
        buffer.close()

    asyncio.run(run())


def test_a_rejected_factory_tick_is_recorded_and_passed(tmp_path):
    from chaski import Reject

    async def run():
        clock = Clock(wall=lambda: 10005)
        clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 10))
        buffer = Buffer(tmp_path / "state.db")
        called, rejected = [], []

        class Producer:
            name = "scheduled"
            _lock = threading.RLock()
            runtime = SimpleNamespace(
                buffer=buffer, reject=lambda consumer, subject, r: rejected.append((consumer, subject, r.reason))
            )

            async def tick(self):
                called.append(clock.now())
                if clock.now() == 1010:
                    raise Reject("no input for this tick")

        task = asyncio.create_task(run_periodic(Producer(), "tick", IntervalSpec(10), clock))
        while len(called) < 3:
            await asyncio.sleep(0.001)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert called[:3] == [1010, 1020, 1030]
        assert rejected == [
            (
                "scheduled.tick",
                {"timer": "__clock__:scheduled:tick:IntervalSpec(seconds=10)", "due": 1010},
                "no input for this tick",
            )
        ]
        buffer.close()

    asyncio.run(asyncio.wait_for(run(), 3))


def _coordinated(tmp_path, called, clock):
    buffer = Buffer(tmp_path / "state.db")

    class Scheduled:
        name = "scheduled"
        _triggers = (("tick", IntervalSpec(5)),)
        _lock = threading.RLock()
        runtime = SimpleNamespace(buffer=buffer)

        async def tick(self):
            called.append(clock.now())

    return Scheduled(), buffer


def _new_run():
    clock = Clock(wall=lambda: 10001)
    clock.apply_definition(ClockDefinition("factory", "new-run", 1, 10000, 1000, 1000, 1015, start_at=1000))
    return clock


def test_a_new_clock_run_does_not_replay_the_previous_runs_timer_progress(tmp_path):
    from chaski.dataops.scheduling import run_due, timer_key

    async def run():
        clock, called = _new_run(), []
        producer, buffer = _coordinated(tmp_path, called, clock)
        # The previous run stopped long before this run's start.
        buffer.set_timer_position(timer_key(producer, "tick", IntervalSpec(5)), "old-run", 500)
        await run_due([producer], clock, 1015, inclusive=True)
        assert called == [1005, 1010, 1015]
        buffer.close()

    asyncio.run(asyncio.wait_for(run(), 3))


def test_a_new_clock_run_fires_timers_the_previous_run_left_in_the_future(tmp_path):
    from chaski.dataops.scheduling import run_due, timer_key

    async def run():
        clock, called = _new_run(), []
        producer, buffer = _coordinated(tmp_path, called, clock)
        buffer.set_timer_position(timer_key(producer, "tick", IntervalSpec(5)), "old-run", 5000)
        await run_due([producer], clock, 1015, inclusive=True)
        assert called == [1005, 1010, 1015]
        buffer.close()

    asyncio.run(asyncio.wait_for(run(), 3))


def test_a_restart_within_the_same_clock_run_continues_its_timers(tmp_path):
    from chaski.dataops.scheduling import run_due, timer_key

    async def run():
        clock, called = _new_run(), []
        producer, buffer = _coordinated(tmp_path, called, clock)
        await run_due([producer], clock, 1005, inclusive=True)
        buffer.close()
        producer, buffer = _coordinated(tmp_path, called, clock)  # restart
        await run_due([producer], clock, 1015, inclusive=True)
        assert called == [1005, 1010, 1015]
        assert buffer.timer_position(timer_key(producer, "tick", IntervalSpec(5)), "new-run") == 1015
        buffer.close()

    asyncio.run(asyncio.wait_for(run(), 3))


def test_timer_progress_without_a_recorded_run_is_not_continued(tmp_path):
    from chaski.dataops.scheduling import run_due, timer_key

    async def run():
        clock, called = _new_run(), []
        producer, buffer = _coordinated(tmp_path, called, clock)
        key = timer_key(producer, "tick", IntervalSpec(5))
        buffer.set_watermark(key, 500, "clock-v1")  # as releases before run-scoped timers stored it
        buffer.set_watermark("scheduled", 900, "producer-hash")
        buffer.close()
        producer, buffer = _coordinated(tmp_path, called, clock)
        assert buffer.watermark(key) is None
        assert buffer.watermark("scheduled") == 900
        await run_due([producer], clock, 1015, inclusive=True)
        assert called == [1005, 1010, 1015]
        buffer.close()

    asyncio.run(asyncio.wait_for(run(), 3))


def test_a_periodic_timer_starts_a_new_clock_run_at_its_start(tmp_path):
    async def run():
        clock = Clock(wall=lambda: 10005)
        clock.apply_definition(ClockDefinition("factory", "new-run", 1, 10000, 1000, 10))
        buffer = Buffer(tmp_path / "state.db")
        called = []

        class Producer:
            name = "scheduled"
            _lock = threading.RLock()
            runtime = SimpleNamespace(buffer=buffer)

            async def tick(self):
                called.append(clock.now())

        from chaski.dataops.scheduling import timer_key

        buffer.set_timer_position(timer_key(Producer(), "tick", IntervalSpec(10)), "old-run", 500)
        task = asyncio.create_task(run_periodic(Producer(), "tick", IntervalSpec(10), clock))
        while len(called) < 2:
            await asyncio.sleep(0.001)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert called[:2] == [1010, 1020]
        buffer.close()

    asyncio.run(asyncio.wait_for(run(), 3))
