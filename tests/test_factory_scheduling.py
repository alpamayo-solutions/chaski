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
    buffer.append("signal", 1000, 1)
    buffer.append("signal", 1100, 2)
    assert buffer.trim({"signal": 100}, now=1150) == 1
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
