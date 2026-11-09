"""Tests for chaski.dataops.worker: one thread and event loop per producer."""

from __future__ import annotations

import asyncio
import contextvars
import threading
import time

import pytest
from dataops_fakes import run_async

from chaski.dataops.worker import ProducerWorker, call_on_worker, on_worker


@pytest.fixture
def worker():
    w = ProducerWorker("p")
    try:
        yield w
    finally:
        w.stop()


def test_work_runs_one_piece_at_a_time_in_the_order_handed_over(worker):
    ran: list[tuple[str, int]] = []
    active = [0]

    def piece(n: int):
        async def run() -> int:
            active[0] += 1
            assert active[0] == 1, "two pieces of work overlapped"
            ran.append(("start", n))
            await asyncio.sleep(0.001)
            ran.append(("end", n))
            active[0] -= 1
            return n

        return run

    futures = [worker.submit(piece(n)) for n in range(20)]
    assert [f.result(5) for f in futures] == list(range(20))
    assert ran == [(edge, n) for n in range(20) for edge in ("start", "end")]


def test_work_runs_on_the_workers_thread_and_one_loop_for_its_life(worker):
    async def where() -> tuple[int, int]:
        return threading.get_ident(), id(asyncio.get_running_loop())

    first = worker.call(where)
    second = worker.call(where)
    assert first == second
    assert first[0] != threading.get_ident()


def test_what_a_callback_starts_keeps_running_between_callbacks(worker):
    """A trailing run or retry a producer schedules runs on its loop without
    waiting for the next callback (a loop run per callback would freeze it)."""
    done = threading.Event()

    async def schedule() -> None:
        asyncio.get_running_loop().call_later(0.01, done.set)

    worker.call(schedule)
    assert done.wait(2), "the call_later never ran"


def test_an_asyncio_object_made_in_setup_is_usable_in_every_later_callback(worker):
    holder: dict[str, asyncio.Lock] = {}

    async def setup() -> None:
        holder["lock"] = asyncio.Lock()

    async def use() -> bool:
        async with holder["lock"]:
            return True

    worker.call(setup)
    assert worker.call(use) and worker.call(use)


def test_work_sees_the_context_of_whoever_handed_it_over(worker):
    deadline: contextvars.ContextVar[float | None] = contextvars.ContextVar("deadline", default=None)

    async def read() -> float | None:
        return deadline.get()

    token = deadline.set(42.0)
    try:
        assert worker.call(read) == 42.0
    finally:
        deadline.reset(token)
    assert worker.call(read) is None


def test_a_failure_reaches_the_caller_and_the_worker_goes_on(worker):
    async def fail() -> None:
        raise ValueError("boom")

    async def ok() -> str:
        return "ok"

    with pytest.raises(ValueError, match="boom"):
        worker.call(fail)
    assert worker.call(ok) == "ok"


def test_call_from_the_workers_own_thread_is_refused(worker):
    async def nested() -> None:
        worker.call(_noop)

    with pytest.raises(RuntimeError, match="wait for itself"):
        worker.call(nested)


async def _noop() -> None:
    return None


def test_stop_finishes_queued_work_and_cancels_what_the_producer_left_running():
    worker = ProducerWorker("p")
    cancelled = threading.Event()

    async def forever() -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    background: list[asyncio.Future] = []

    async def start_background() -> None:
        background.append(asyncio.ensure_future(forever()))

    worker.call(start_background)
    queued = worker.submit(_slow)
    worker.stop()
    assert queued.result(1) == "slow done"
    assert cancelled.is_set()
    later = worker.submit(_noop)
    with pytest.raises(RuntimeError, match="stopped"):
        later.result(1)


async def _slow() -> str:
    await asyncio.sleep(0.05)
    return "slow done"


@run_async
async def test_on_worker_awaits_without_blocking_the_callers_loop():
    worker = ProducerWorker("p")
    try:
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.005)
                ticks += 1

        async def blocking() -> None:
            time.sleep(0.2)  # a producer doing synchronous work

        task = asyncio.ensure_future(ticker())
        await on_worker(_Producer(worker), blocking)
        task.cancel()
        assert ticks >= 10, "the caller's loop was blocked by the producer's work"
    finally:
        worker.stop()


def test_without_a_worker_the_work_runs_directly():
    async def where() -> int:
        return threading.get_ident()

    assert call_on_worker(_Producer(None), where) == threading.get_ident()


class _Producer:
    def __init__(self, worker: ProducerWorker | None) -> None:
        self._worker = worker
