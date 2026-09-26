"""The health door answers for the running service, from its event loop.

Its 200 or 503 follows the ingest task. Being on the loop, it cannot answer at
all while the loop is blocked; that is not timed here.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request

from chaski.dataops.health import HealthState, serve


def _get(port: int):
    """One probe, exactly the shape the container healthcheck sends."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as refused:
        return refused.code, json.loads(refused.read())


def _free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run_async(fn):
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    wrapper.__name__ = fn.__name__
    return wrapper


@run_async
async def test_a_running_ingest_answers_200_with_the_service_facts():
    state = HealthState(producers=3, generation="01GEN")
    state.ingest_task = asyncio.ensure_future(asyncio.sleep(30))
    port = _free_port()
    server = await serve(state, port=port)
    try:
        status, body = await asyncio.to_thread(_get, port)
        assert status == 200, body
        assert body["ok"] is True
        assert body["ingest"] == "running"
        assert body["producers"] == 3
        assert body["generation"] == "01GEN"
    finally:
        state.ingest_task.cancel()
        server.close()
        await server.wait_closed()


@run_async
async def test_a_dead_ingest_loop_turns_the_probe_503():
    """A dead ingest task makes the service unhealthy; one that never started
    does not."""

    async def _dies():
        raise RuntimeError("boom")

    state = HealthState()
    state.ingest_task = asyncio.ensure_future(_dies())
    await asyncio.sleep(0)  # let it die
    port = _free_port()
    server = await serve(state, port=port)
    try:
        status, body = await asyncio.to_thread(_get, port)
        assert status == 503, body
        assert body["ok"] is False
        assert body["ingest"] == "dead"

        state.ingest_task = None  # never-started: healthy by design
        status, body = await asyncio.to_thread(_get, port)
        assert status == 200, body
        assert body["ingest"] == "not-started"
    finally:
        server.close()
        await server.wait_closed()


@run_async
async def test_an_ingest_loop_that_stopped_draining_turns_the_probe_503():
    """Alive is not enough: a loop stuck on a door that never answers, or
    retrying the same error for ever, has stopped finishing drains."""
    last_drain = [time.monotonic() - 301.0]
    state = HealthState(last_drain_at=lambda: last_drain[0], stall_after_s=300.0)
    state.ingest_task = asyncio.ensure_future(asyncio.sleep(30))
    port = _free_port()
    server = await serve(state, port=port)
    try:
        status, body = await asyncio.to_thread(_get, port)
        assert status == 503, body
        assert body["ok"] is False
        assert body["ingest"] == "stalled"
        assert body["since_drain_s"] >= 300

        last_drain[0] = time.monotonic()  # it drained again
        status, body = await asyncio.to_thread(_get, port)
        assert status == 200, body
        assert body["ingest"] == "running"
    finally:
        state.ingest_task.cancel()
        server.close()
        await server.wait_closed()


def test_a_broker_link_down_past_the_grace_period_fails_the_probe(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr("chaski.dataops.health.time.monotonic", lambda: now[0])
    connected = [False]
    state = HealthState(broker_connected=lambda: connected[0], broker_grace_s=60.0)

    assert state.healthy() and state.snapshot()["broker"] == "reconnecting"
    now[0] += 59
    assert state.healthy()
    now[0] += 2
    assert not state.healthy() and state.snapshot()["broker"] == "down"

    connected[0] = True
    assert state.healthy() and state.snapshot()["broker"] == "connected"
    connected[0] = False
    assert state.healthy(), "a new outage starts its own grace period"
