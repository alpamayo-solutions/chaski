"""Starting while the node cannot be reached: which failures are retried,
the backoff between attempts, and the readiness a health door reports."""

from __future__ import annotations

import asyncio
import email.message
import io
import json
import socket
import urllib.error
import urllib.request

import httpx
import pytest
from ports import reserved_port

import chaski.service as service_module
from chaski.connector import ConnectorService, start_health_server
from chaski.dataops import health as health_module
from chaski.dataops.health import HealthState
from chaski.retry import retry_after
from chaski.service import Service
from chaski.startup import STARTUP_RETRY_MAX_S, BrokerRefused, Readiness, colca_unreachable


def _http_error(code: int, retry_after_s: str | None = None) -> urllib.error.HTTPError:
    headers = email.message.Message()
    if retry_after_s is not None:
        headers["Retry-After"] = retry_after_s
    return urllib.error.HTTPError("http://colca:80/self", code, "", headers, io.BytesIO(b"{}"))


def _status_error(code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://colca/kv")
    return httpx.HTTPStatusError("", request=request, response=httpx.Response(code, request=request))


class _Code:
    def __init__(self, value: int) -> None:
        self.value = value


@pytest.mark.parametrize(
    "exc",
    [
        urllib.error.URLError(ConnectionRefusedError(61, "Connection refused")),
        urllib.error.URLError(socket.gaierror(-2, "Name or service not known")),
        ConnectionRefusedError(61, "Connection refused"),
        TimeoutError("no CONNACK from the broker within 10s"),
        httpx.ConnectError("connection refused"),
        _http_error(503),
        _http_error(429),
        _status_error(502),
        BrokerRefused(_Code(0x88)),
    ],
)
def test_an_unreachable_or_busy_node_is_retried(exc):
    assert colca_unreachable(exc)


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("Colca /self resolved service 'other', expected 'temp-conn'"),
        ValueError("chaski.Service: could not parse a host from ''"),
        _http_error(409),
        _http_error(401),
        _status_error(403),
        BrokerRefused(_Code(0x87)),
    ],
)
def test_a_configuration_error_is_not_retried(exc):
    assert not colca_unreachable(exc)


def test_retry_after_is_read_from_urllibs_http_error():
    assert retry_after(_http_error(429, "7")) == 7.0
    assert retry_after(_http_error(503, "7")) is None


class _Stop:
    """A stop event that records every wait instead of sleeping, and is set
    after ``waits`` waits."""

    def __init__(self, waits: int = 1_000) -> None:
        self.delays: list[float] = []
        self.waits = waits

    def is_set(self) -> bool:
        return len(self.delays) >= self.waits

    def wait(self, delay: float) -> bool:
        self.delays.append(delay)
        return self.is_set()


def _service(monkeypatch, failures: list[BaseException]) -> Service:
    """A local service whose start attempts raise ``failures`` in order, then
    succeed."""
    svc = Service("temp-conn", "line1")
    attempts: list[int] = []

    def start_local(_timeout: float) -> None:
        attempts.append(1)
        if failures:
            raise failures.pop(0)

    monkeypatch.setattr(svc, "_start_local", start_local)
    svc.attempts = attempts  # type: ignore[attr-defined]
    return svc


def test_readiness_before_start_says_so():
    svc = Service("temp-conn")
    assert svc.readiness == Readiness("not-started", "start() has not been called")
    assert not svc.readiness.ready


def test_start_retries_an_unreachable_node_and_reports_why_until_it_answers(monkeypatch):
    refused = urllib.error.URLError(ConnectionRefusedError(61, "Connection refused"))
    svc = _service(monkeypatch, [refused, refused, refused])
    seen: list[Readiness] = []
    stop = _Stop()
    original_wait = stop.wait

    def wait(delay: float) -> bool:
        seen.append(svc.readiness)
        return original_wait(delay)

    stop.wait = wait  # type: ignore[method-assign]

    assert svc.start_when_reachable(stop) is True  # type: ignore[arg-type]

    assert len(svc.attempts) == 4  # type: ignore[attr-defined]
    assert [r.state for r in seen] == ["waiting"] * 3
    assert [r.attempts for r in seen] == [1, 2, 3]
    assert seen[0].reason.startswith("cannot reach Colca at colca (http :80, mqtt :1883): URLError")
    assert svc.readiness == Readiness("ready", "", 4)


def test_the_backoff_grows_and_stays_bounded(monkeypatch):
    svc = _service(monkeypatch, [ConnectionRefusedError(61, "refused")] * 40)
    stop = _Stop()
    assert svc.start_when_reachable(stop) is True  # type: ignore[arg-type]
    assert len(stop.delays) == 40
    assert max(stop.delays) <= STARTUP_RETRY_MAX_S
    assert max(stop.delays[:2]) < min(stop.delays[-10:]), "the wait does not grow"


def test_a_node_asking_to_retry_later_is_honoured(monkeypatch):
    svc = _service(monkeypatch, [_http_error(429, "20")])
    stop = _Stop()
    assert svc.start_when_reachable(stop) is True  # type: ignore[arg-type]
    assert stop.delays and stop.delays[0] >= 20.0


def test_a_configuration_error_ends_startup_at_once(monkeypatch):
    svc = _service(monkeypatch, [RuntimeError("Colca /self resolved service 'x', expected 'temp-conn'")])
    stop = _Stop()
    with pytest.raises(RuntimeError, match="expected 'temp-conn'"):
        svc.start_when_reachable(stop)  # type: ignore[arg-type]
    assert stop.delays == []
    assert svc.readiness.state == "failed"
    assert "expected 'temp-conn'" in svc.readiness.reason


def test_stopping_while_waiting_returns_without_starting(monkeypatch):
    svc = _service(monkeypatch, [ConnectionRefusedError(61, "refused")] * 10)
    assert svc.start_when_reachable(_Stop(waits=2)) is False  # type: ignore[arg-type]
    assert svc.readiness.state == "waiting"
    assert svc._client is None


def test_the_async_form_is_stopped_by_an_asyncio_event(monkeypatch):
    monkeypatch.setattr(service_module, "STARTUP_RETRY_MIN_S", 0.01)
    monkeypatch.setattr(service_module, "STARTUP_RETRY_MAX_S", 0.02)
    svc = _service(monkeypatch, [ConnectionRefusedError(61, "refused")] * 1_000)

    async def main() -> bool:
        stop = asyncio.Event()
        task = asyncio.ensure_future(svc.start_when_reachable_async(stop))
        while svc.readiness.attempts < 3:
            await asyncio.sleep(0.01)
        stop.set()
        return await asyncio.wait_for(task, 5)

    assert asyncio.run(main()) is False


def test_the_dataops_health_door_names_what_startup_waits_for():
    state = HealthState(ready=False, not_ready=lambda: "cannot reach Colca at colca (http :80, mqtt :1883): boom")
    body = state.snapshot()
    assert not state.healthy()
    assert body["ok"] is False and body["ingest"] == "starting"
    assert body["not_ready"].startswith("cannot reach Colca")
    state.ready = True
    assert "not_ready" not in state.snapshot()


def test_the_connector_health_door_answers_not_ready_with_the_reason_before_start():
    from test_connector_service import FakeDriver

    svc = ConnectorService("temp-conn", "line1", driver=FakeDriver())
    assert svc.health_problem() == "not ready: start() has not been called"
    server = start_health_server(0, lambda: not svc.health_problem(), reason=svc.health_problem)
    try:
        port = server.server_address[1]
        with pytest.raises(urllib.error.HTTPError) as answer:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/is_healthy", timeout=5)  # nosec B310
        assert answer.value.code == 503
        assert answer.value.read().decode() == "unhealthy: not ready: start() has not been called"
    finally:
        server.shutdown()
        server.server_close()


def test_a_dataops_service_health_door_answers_while_colca_is_away(monkeypatch, tmp_path):
    """The DataOps door is up before the node answers and reports why."""
    from chaski.dataops import DataOpsService

    # Nothing listens there, and no other test's listener on port 0 can take it.
    closed_port = reserved_port()
    door = service_module.LocalDoor(host="127.0.0.1", http_port=closed_port, mqtt_port=closed_port)
    svc = DataOpsService("dataops", node=door, state_dir=tmp_path, health_port=0)
    ports: list[int] = []
    original_serve = health_module.serve

    async def serve(state, port):
        server = await original_serve(state, port)
        ports.append(server.sockets[0].getsockname()[1])
        return server

    monkeypatch.setattr(health_module, "serve", serve)

    async def main() -> dict:
        stop = asyncio.Event()
        task = asyncio.ensure_future(svc.serve(stop))
        try:
            while svc.readiness.state != "waiting":
                await asyncio.sleep(0.02)
            reader, writer = await asyncio.open_connection("127.0.0.1", ports[0])
            writer.write(b"GET /healthz HTTP/1.1\r\n\r\n")
            await writer.drain()
            response = await reader.read()
            writer.close()
            assert response.startswith(b"HTTP/1.1 503"), response
            return json.loads(response.split(b"\r\n\r\n", 1)[1])
        finally:
            stop.set()
            await asyncio.wait_for(task, 15)

    body = asyncio.run(main())
    assert body["ingest"] == "starting"
    assert body["not_ready"].startswith(f"cannot reach Colca at 127.0.0.1 (http :{closed_port}")
