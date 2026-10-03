"""Against a real node: a sample the node refuses, or one it could never
admit, does not block the connector's durable queue. The samples after it
arrive, the queue drains, and the refusal is a retained ``rejected_input``
finding. A transport failure is still retried and loses nothing."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import threading
import time

import httpx
import pytest
from test_connector_service import FakeDriver
from test_connector_writes_integration import _signal_path

import chaski
from chaski.connector import ConnectorService
from chaski.dataops import Producer

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)

TEMPERATURE = "Axis1/Temperature"


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door rather than the unit suite's default fake."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """The integration caller supplies the bundle matching its binary."""


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


class SequenceDriver(FakeDriver):
    """Reads ``readings`` in order, one per poll, then repeats the last."""

    def __init__(self, readings: list[float]) -> None:
        super().__init__()
        self.readings = list(readings)

    async def read(self, targets):
        if len(self.readings) > 1:
            self.values[TEMPERATURE] = self.readings.pop(0)
        else:
            self.values[TEMPERATURE] = self.readings[0]
        return await super().read(targets)


@contextlib.contextmanager
def _serving(svc: ConnectorService):
    loop = asyncio.new_event_loop()
    failure: list[BaseException] = []

    def run() -> None:
        try:
            loop.run_until_complete(svc.serve())
        except BaseException as exc:  # pragma: no cover - reported below
            failure.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        asyncio.run_coroutine_threadsafe(svc.stop(), loop).result(timeout=10)
        thread.join(timeout=20)
        loop.close()
    assert not failure, f"the connector's loop ended with {failure[0]!r}"


def _connector(node: chaski.Node, driver, tmp_path) -> ConnectorService:
    door = chaski.LocalDoor(host="127.0.0.1", http_port=node._ports["api_local"], mqtt_port=node._ports["mqtt_local"])
    return ConnectorService(
        "temp-conn",
        "line1",
        driver=driver,
        node=door,
        state_dir=tmp_path / "conn",
        interval=0.2,
        heartbeat_interval=1.0,
    )


def _signal_id(admin: chaski.Service, path: str) -> str:
    entry = next(e for e in admin.kv(contract="_Signal") if e.path == path)
    return entry.payload["id"]


def _values(admin: chaski.Service, signal_id: str, cursor: str) -> list:
    return [record.payload["value"] for record in admin.stream("metrics", cursor=cursor, signal_ids=[signal_id])]


def _wait_for_values(admin, signal_id, expected: list, cursor: str, timeout: float = 30.0) -> list:
    seen: list = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        seen += _values(admin, signal_id, cursor)
        if all(value in seen for value in expected):
            return seen
        time.sleep(0.2)
    raise AssertionError(f"expected {expected} at the node, saw {seen}")


def _rejection_finding(admin: chaski.Service) -> dict:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        for entry in admin.kv(contract="_Finding"):
            if entry.path.endswith("rejected_input") and isinstance(entry.payload, dict):
                return entry.payload
        time.sleep(0.2)
    raise AssertionError("no rejected_input finding")


def _wait_drained(svc: ConnectorService, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while svc._pending:
        assert time.monotonic() < deadline, f"the queue never drained: {svc._pending}"
        time.sleep(0.1)


def test_a_nan_reading_is_refused_and_the_readings_after_it_arrive(tmp_path, caplog):
    driver = SequenceDriver([math.nan, 1.0, 2.0, 3.0])
    with (
        caplog.at_level(logging.INFO),
        chaski.Node("refusals", data_dir=tmp_path / "node") as node,
        node.service("admin") as admin,
    ):
        svc = _connector(node, driver, tmp_path)
        with _serving(svc) as thread:
            deadline = time.monotonic() + 30
            while svc._catalogue is None or svc._catalogue.tag_id(TEMPERATURE) is None:
                assert time.monotonic() < deadline, "the connector never catalogued its tags"
                time.sleep(0.1)
            path = _signal_path(admin, svc._catalogue.tag_id(TEMPERATURE))
            signal_id = _signal_id(admin, path)

            seen = _wait_for_values(admin, signal_id, [1.0, 2.0, 3.0], cursor="nan")
            _wait_drained(svc)
            assert thread.is_alive()
            assert not any(isinstance(v, float) and not math.isfinite(v) for v in seen)
            assert svc.refused_samples_total == 1
            finding = _rejection_finding(admin)
            assert finding["detail"]["signal_id"] == signal_id
            assert "non-finite" in finding["summary"]

        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert not any("rejected" in m or "Unknown polling loop error" in m for m in errors), errors
        refusal_logs = [r.getMessage() for r in caplog.records if "connector-samples rejected" in r.getMessage()]
        assert len(refusal_logs) == 1, refusal_logs


def test_a_sample_the_node_refuses_is_dropped_visibly_and_does_not_block_the_queue(tmp_path, caplog):
    """A sample queued before this release (or by a bug) that the node's
    schema refuses: the node admits the samples after it in the same batch,
    so the refusal is final. It is recorded and removed; nothing retries it."""
    driver = SequenceDriver([1.0, 2.0, 3.0])
    with (
        caplog.at_level(logging.INFO),
        chaski.Node("refusals", data_dir=tmp_path / "node") as node,
        node.service("admin") as admin,
    ):
        svc = _connector(node, driver, tmp_path)
        # The node's own id puts the refused sample past the topic checks; the
        # empty signal_id is what its schema refuses.
        node_id = admin._node_id
        refused_topic = f"colca/v1/_Metric/{node_id}/line1/temp-conn/broken"
        svc._open_metric_queue().append_batch(
            [(refused_topic, {"value": 1.0, "timestamp": time.time(), "signal_id": ""}, time.time(), None)],
            svc.max_pending,
        )
        with _serving(svc) as thread:
            deadline = time.monotonic() + 30
            while svc._catalogue is None or svc._catalogue.tag_id(TEMPERATURE) is None:
                assert time.monotonic() < deadline, "the connector never catalogued its tags"
                time.sleep(0.1)
            path = _signal_path(admin, svc._catalogue.tag_id(TEMPERATURE))
            signal_id = _signal_id(admin, path)

            _wait_for_values(admin, signal_id, [1.0, 2.0, 3.0], cursor="refused")
            _wait_drained(svc)
            assert thread.is_alive()
            assert svc.refused_samples_total == 1
            finding = _rejection_finding(admin)
            assert finding["detail"]["topic"] == refused_topic

        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert not any("Unknown polling loop error" in m for m in errors), errors


class CountingDriver(FakeDriver):
    """Reads 1.0, 2.0, 3.0, ... one per poll."""

    def __init__(self) -> None:
        super().__init__()
        self.count = 0

    async def read(self, targets):
        self.count += 1
        self.values[TEMPERATURE] = float(self.count)
        return await super().read(targets)


def test_a_transport_failure_is_retried_and_loses_nothing(tmp_path):
    driver = CountingDriver()
    with chaski.Node("refusals", data_dir=tmp_path / "node") as node, node.service("admin") as admin:
        svc = _connector(node, driver, tmp_path)
        failures = {"left": 0}
        with _serving(svc) as thread:
            deadline = time.monotonic() + 30
            while svc._catalogue is None or svc._catalogue.tag_id(TEMPERATURE) is None or svc._http is None:
                assert time.monotonic() < deadline, "the connector never catalogued its tags"
                time.sleep(0.1)
            path = _signal_path(admin, svc._catalogue.tag_id(TEMPERATURE))
            signal_id = _signal_id(admin, path)
            seen = _wait_for_values(admin, signal_id, [1.0], cursor="transport")

            publish = svc._http.publish_batch

            def flaky(records):
                if failures["left"] > 0:
                    failures["left"] -= 1
                    raise httpx.ConnectError("connection refused")
                return publish(records)

            failures["left"] = 3
            svc._http.publish_batch = flaky
            deadline = time.monotonic() + 30
            while failures["left"] > 0:
                assert time.monotonic() < deadline, "the connector stopped publishing"
                time.sleep(0.1)
            last = float(driver.count + 2)
            seen += _wait_for_values(admin, signal_id, [last], cursor="transport")
            _wait_drained(svc)
            assert thread.is_alive()

        assert sorted(seen) == [float(n) for n in range(1, int(max(seen)) + 1)], "a sample went missing"
        assert svc.refused_samples_total == 0
