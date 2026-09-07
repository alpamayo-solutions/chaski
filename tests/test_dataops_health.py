"""The health door answers for the RUNNING service, from its event loop.

The old container healthcheck (`python -c "import dataops.service"`) spawned
a fresh interpreter: it could not see a dead ingest loop, and under load its
own import cost exceeded the probe timeout. These tests pin the replacement:
a loop-hosted server whose 200/503 is derived from the one task that makes
this service a service — and which, being ON the loop, cannot answer at all
when the loop is blocked (that silence is the probe's honest failure mode;
timing it is not a job for a unit test).
"""

from __future__ import annotations

import asyncio
import json
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
    """The one condition that makes an answering service unhealthy: its
    single data lane died. An ingest that was never started is NOT that —
    a fresh node waiting to be commissioned is healthy."""

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
