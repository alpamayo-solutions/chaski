"""A background task that raises is logged, reported and restarted; the MQTT
client reconnects within seconds."""

from __future__ import annotations

import asyncio
import logging

from dataops_fakes import run_async

from chaski.dataops.service import supervise
from chaski.failures import HandlerHealth
from chaski.service import RECONNECT_MAX_S, bound_reconnect


@run_async
async def test_a_crashed_task_restarts_and_its_health_recovers(caplog):
    health = HandlerHealth(unhealthy_after=2)
    stop = asyncio.Event()
    runs: list[int] = []

    async def executor() -> None:
        runs.append(len(runs) + 1)
        if len(runs) <= 2:
            raise RuntimeError("PublishTimeout on the _Ack")
        await stop.wait()

    with caplog.at_level(logging.ERROR, logger="chaski.dataops.service"):
        task = asyncio.ensure_future(supervise("command executor", executor, stop, health, stable_s=0.05))
        for _ in range(200):
            await asyncio.sleep(0.01)
            if len(runs) == 3:
                break
    assert len(runs) == 3
    assert "command executor crashed (1 in a row)" in caplog.text, "the exception is logged when it happens"
    assert health.status == "unhealthy"
    assert "task command executor failed 2x" in health.summary()
    for _ in range(100):
        await asyncio.sleep(0.01)
        if health.status == "ok":
            break
    assert health.status == "ok", "a restart that keeps running clears the failure"
    assert not task.done()
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)


@run_async
async def test_a_task_that_returns_is_not_restarted():
    health = HandlerHealth()
    runs: list[int] = []

    async def once() -> None:
        runs.append(1)

    await asyncio.wait_for(supervise("once", once, asyncio.Event(), health), timeout=1.0)
    assert runs == [1]
    assert health.status == "ok"


@run_async
async def test_cancelling_the_supervisor_cancels_the_task():
    cancelled = asyncio.Event()

    async def forever() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = asyncio.ensure_future(supervise("forever", forever, asyncio.Event(), HandlerHealth()))
    await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert cancelled.is_set()


class _PahoLike:
    def __init__(self) -> None:
        self.delays: list[tuple[float, float]] = []

    def reconnect_delay_set(self, min_delay, max_delay) -> None:
        self.delays.append((min_delay, max_delay))

    def _reconnect_wait(self) -> None:
        pass


def test_reconnect_waits_at_most_five_seconds_with_jitter():
    client = _PahoLike()
    backoff = bound_reconnect(client)
    for _ in range(20):
        client._reconnect_wait()
    waits = [low for low, high in client.delays if low == high]
    assert len(waits) == 20
    assert all(0 < w <= RECONNECT_MAX_S == 5.0 for w in waits)
    assert len(set(waits)) > 1, "jittered, not a fixed delay"
    backoff.reset()
    client._reconnect_wait()
    assert client.delays[-1][0] <= 1.0, "a CONNACK starts the backoff over"
