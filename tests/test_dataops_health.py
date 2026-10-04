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
async def test_a_coordinated_step_loop_that_stopped_turns_the_probe_503():
    """In coordinated step mode alive is not enough: a step loop stuck on a
    door that never answers has stopped finishing steps."""
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


@run_async
async def test_the_nodes_cursor_lag_finding_turns_the_probe_503_and_an_idle_stream_does_not():
    """Records this service reads that wait unread past the node's threshold
    fail the probe; an ingest that has had nothing to read for an hour is
    healthy, because only waiting records count."""
    lag = ["historian has records on metrics waiting 75 s that it has not read"]
    state = HealthState(cursor_lag=lambda: lag[0])
    state.ingest_task = asyncio.ensure_future(asyncio.sleep(30))
    port = _free_port()
    server = await serve(state, port=port)
    try:
        status, body = await asyncio.to_thread(_get, port)
        assert status == 503, body
        assert body["ok"] is False
        assert body["cursor_lag"] == lag[0]

        lag[0] = ""  # the node retired the finding: caught up, or simply idle
        status, body = await asyncio.to_thread(_get, port)
        assert status == 200, body
        assert body["ingest"] == "running"
        assert "cursor_lag" not in body and "since_drain_s" not in body
    finally:
        state.ingest_task.cancel()
        server.close()
        await server.wait_closed()


@run_async
async def test_an_identity_conflict_turns_the_probe_503():
    """Another process uses this service's identity: the broker hands the
    session back and forth and consumers stop being woken."""
    conflict = ["another process is connected as svc1"]
    state = HealthState(identity_conflict=lambda: conflict[0])
    port = _free_port()
    server = await serve(state, port=port)
    try:
        status, body = await asyncio.to_thread(_get, port)
        assert status == 503, body
        assert body["ok"] is False
        assert body["identity_conflict"] == conflict[0]

        conflict[0] = ""
        status, body = await asyncio.to_thread(_get, port)
        assert status == 200, body
        assert "identity_conflict" not in body
    finally:
        server.close()
        await server.wait_closed()


# ─── health per backfill mode ──────────────────────────────────────────────


def _running(*, holds_live: bool, stalled: str = "") -> dict:
    running = {"producer": "cycles", "job": "initial", "progress": 0.03, "holds_live": holds_live}
    if stalled:
        running["stalled"] = stalled
    return {"running": running, "pending": 0}


def _state(backfill: dict, lag: str = "") -> HealthState:
    state = HealthState(backfill=lambda: backfill, cursor_lag=lambda: lag)
    state.ingest_task = asyncio.ensure_future(asyncio.sleep(30))
    return state


@run_async
async def test_without_a_backfill_a_running_ingest_reports_ok():
    state = _state({})
    try:
        body = state.snapshot()
        assert state.healthy() and body["status"] == "ok"
        assert "backfill" not in body and "degraded" not in body
    finally:
        state.ingest_task.cancel()


@run_async
async def test_an_independent_backfill_is_progress_and_the_service_reports_ok():
    """Live dispatch runs beside the history: the door answers 200 with the
    job's progress, and only live lag fails it."""
    state = _state(_running(holds_live=False))
    try:
        body = state.snapshot()
        assert state.healthy() and body["ok"] and body["status"] == "ok"
        assert body["backfill"]["running"]["holds_live"] is False
        assert "cursor_lag" not in body
    finally:
        state.ingest_task.cancel()


@run_async
async def test_a_backfill_that_holds_live_reports_backfilling_and_stays_200():
    """Live triggers are held on purpose; the ingest goes on buffering, so the
    service is neither not-ready nor degraded."""
    state = _state(_running(holds_live=True))
    try:
        body = state.snapshot()
        assert state.healthy() and body["ok"]
        assert body["status"] == "backfilling"
        assert "not_ready" not in body and "degraded" not in body
    finally:
        state.ingest_task.cancel()


@run_async
async def test_live_lag_during_a_backfill_still_fails_the_probe_in_either_mode():
    lag = "dataops has records on metrics waiting 75 s that it has not read"
    for holds_live in (False, True):
        state = _state(_running(holds_live=holds_live), lag=lag)
        try:
            body = state.snapshot()
            assert not state.healthy() and not body["ok"]
            assert body["status"] == "unhealthy" and body["cursor_lag"] == lag
            assert body["backfill"]["running"]["holds_live"] is holds_live
        finally:
            state.ingest_task.cancel()


@run_async
async def test_a_stalled_backfill_is_degraded_with_its_reason_and_stays_200():
    reason = "no backfill window finished in 900 s; it waits for its inputs to be commissioned"
    for holds_live in (False, True):
        state = _state(_running(holds_live=holds_live, stalled=reason))
        try:
            body = state.snapshot()
            assert state.healthy() and body["ok"]
            assert body["status"] == "degraded"
            assert body["degraded"] == [reason]
        finally:
            state.ingest_task.cancel()


@run_async
async def test_a_retried_handler_is_degraded_and_names_it():
    from chaski.failures import HandlerHealth

    handlers = HandlerHealth()
    handlers.failed("cycles.on_state", RuntimeError("boom"))
    state = _state({})
    state.handlers = handlers
    try:
        body = state.snapshot()
        assert state.healthy() and body["status"] == "degraded"
        assert body["degraded"] == ["cycles.on_state failed 1x, retried"]
    finally:
        state.ingest_task.cancel()


def test_before_startup_is_done_the_status_is_starting():
    state = HealthState(ready=False, backfill=lambda: _running(holds_live=True))
    body = state.snapshot()
    assert not state.healthy() and body["status"] == "starting"
