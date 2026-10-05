"""The DataOps health door while a backfill runs, against a real node.

A service started from fresh state (a new buffer generation) runs a long
independent backfill beside live dispatch. Its health door stays 200 while the
history runs and live output is current, also when the node watched the
previous generation's ingest cursor. A live ingest that stops reading still
fails the door, through the node's cursor_lag finding.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid

import pytest

import chaski
from chaski.dataops import Backfill, Producer, SignalOutput, SignalRangeInput, on_metric
from chaski.dataops.backfill import INITIAL
from chaski.service import LocalDoor

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)


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


class EmptyHistorian:
    """A historian with nothing older than the stream: every window is empty,
    as on the rig, where a 400-day history ran mostly empty 6 h windows."""

    def window(self, signal_id, start, end):
        import pandas as pd

        return pd.DataFrame({"ts": [], "value": []}, columns=["ts", "value"])

    def latest_before(self, signal_id, before):
        return None


class Doubler(Producer):
    name = "doubler"
    system_element_name = "Line/M1"
    backfill = Backfill(horizon="400d", window="6h", mode="independent")

    speed = SignalRangeInput("speed", window="1h")
    doubled = SignalOutput("doubled", "float", "speed x 2")

    @on_metric("speed")
    async def on_speed(self, metric) -> None:
        self.doubled.publish(2 * metric.value, metric.timestamp)


def _wait(predicate, timeout: float = 60.0, interval: float = 0.2):
    deadline = time.monotonic() + timeout
    while not (result := predicate()):
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(interval)
    return result


def _health(port: int) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


@contextlib.contextmanager
def _serving(node, data_dir, **kwargs):
    kwargs.setdefault("health_port", 0)
    door = LocalDoor(host="127.0.0.1", http_port=node._ports["api_local"], mqtt_port=node._ports["mqtt_local"])
    svc = chaski.DataOpsService(
        "dataops", node=door, state_dir=node.data_dir / "services" / "dataops", data_dir=data_dir, **kwargs
    )
    svc.add(Doubler)
    loop = asyncio.new_event_loop()
    stop = asyncio.Event()
    failed: list[BaseException] = []

    def serve():
        try:
            loop.run_until_complete(svc.serve(stop))
        except BaseException as exc:
            failed.append(exc)

    thread = threading.Thread(target=serve)
    thread.start()
    try:
        _wait(lambda: svc._ingest is not None or failed)
        assert not failed, failed
        yield svc
    finally:
        loop.call_soon_threadsafe(stop.set)
        thread.join(timeout=30)
        svc.close()
        loop.close()


def _publish(machine, value: float) -> float:
    _wait(lambda: not machine._pending_sources, interval=0.05)
    ts = time.time()
    machine.publish("speed", value, timestamp=ts)
    return ts


def _latest_output(reader) -> float | None:
    """The timestamp of the newest ``doubled`` sample in the node's metrics stream."""
    signal_id = None
    for row in reader.kv("", contract="_Signal"):
        if row.payload and row.payload.get("name") == "doubled":
            signal_id = row.payload["id"]
    if signal_id is None:
        return None
    stream = reader.stream("metrics", cursor=f"read-{uuid.uuid4().hex[:8]}", signal_ids=[signal_id])
    records = list(stream)
    stream.retire()
    return max((float(r.payload["timestamp"]) for r in records), default=None)


def test_a_long_independent_backfill_after_a_fresh_start_keeps_health_ok_and_a_stalled_ingest_fails_it(
    tmp_path, fast_lag_alarm, health_doors
):
    with (
        chaski.Node("health-backfill", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("reader") as reader,
    ):
        _publish(machine, 1.0)
        # A first run reads the stream on its generation's cursor, then stops.
        with _serving(node, tmp_path / "first", historian=EmptyHistorian()) as first:
            old_cursor = first._ingest.cursor
            _wait(lambda: first._ingest.caught_up.generation)
            _wait(lambda: _latest_output(reader) is not None)
        # Records the old cursor reads arrive while no one runs.
        _publish(machine, 2.0)

        # Fresh state: a new buffer generation, so a new ingest cursor, and a
        # long independent first backfill beside live dispatch.
        with _serving(node, tmp_path / "second", historian=EmptyHistorian(), backfill_rate=0.5) as svc:
            port = health_doors.dataops[-1]
            assert svc._ingest.cursor != old_cursor
            _wait(lambda: svc._backfill.status().get("running"))

            # Past the node's lag threshold (1 s) several times over, with live
            # records flowing: the door stays 200, reports the backfill as
            # progress, and live output is current.
            deadline = time.monotonic() + 8.0
            answers = []
            while time.monotonic() < deadline:
                sent = _publish(machine, 3.0)
                _wait(lambda sent=sent: (_latest_output(reader) or 0) >= sent, timeout=10.0)
                answers.append(_health(port))
                time.sleep(0.5)
            assert all(status == 200 for status, _ in answers), [body for _, body in answers]
            _, body = answers[-1]
            assert body["ok"] and "cursor_lag" not in body
            assert body["backfill"]["running"]["holds_live"] is False
            assert body["status"] == "ok"
            job = svc.buffer.backfill_job("doubler", INITIAL)
            assert job is not None and not job["done"], "the backfill finished before health was checked"

            # The previous generation's cursor was retired at the node.
            cursors = {row["cursor"] for row in svc._require_http("backlog").backlog([svc.cursor_prefix + "ingest-"])}
            assert cursors == {svc._ingest.cursor}

            # A live ingest that stops reading still fails the door.
            svc._ingest.page_lock.acquire()
            try:
                _publish(machine, 4.0)
                _, body = _wait(lambda: (answer := _health(port))[0] == 503 and answer, timeout=30.0)
                assert body["cursor_lag"] and not body["ok"]
                assert body["status"] == "unhealthy"
            finally:
                svc._ingest.page_lock.release()
            _wait(lambda: _health(port)[0] == 200, timeout=30.0)
