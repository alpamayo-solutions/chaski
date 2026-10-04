"""The DataOps backfill against a real node.

The machine history is seeded through a real colcad: recent samples are
published to the node and reach the DataOps service through its metrics stream,
as live data does. Older samples, beyond what the stream holds, sit in a fake
historian: chaski ships no ``Historian`` implementation and a real one needs a
TimescaleDB, so the port is an in-memory stand-in holding what a historian
would have recorded (the archive, plus the recent samples).

The reference is the same producer run on a second node that holds the whole
history in its stream, processed live.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import time
import uuid

import pandas as pd
import pytest
from node_api_fake import CLIENT_ID, CLIENT_SECRET, FakeNodeApi

import chaski
from chaski.dataops import Backfill, NodeHistorian, Producer, SignalOutput, SignalRangeInput, on_metric
from chaski.dataops.backfill import INITIAL
from chaski.service import LocalDoor

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)

DAY = 86400.0


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


class FakeHistorian:
    """The ``Historian`` port over the points a historian would hold."""

    def __init__(self) -> None:
        self.points: dict[str, list[tuple[float, object]]] = {}

    def add(self, signal_id: str, rows) -> None:
        self.points[signal_id] = sorted([*self.points.get(signal_id, []), *rows])

    def window(self, signal_id, start, end):
        rows = [(ts, v) for ts, v in self.points.get(signal_id, []) if start <= ts < end]
        return pd.DataFrame({"ts": [r[0] for r in rows], "value": [r[1] for r in rows]}, columns=["ts", "value"])

    def latest_before(self, signal_id, before):
        rows = [(ts, v) for ts, v in self.points.get(signal_id, []) if ts <= before]
        return rows[-1] if rows else None


def cycles(backfill: Backfill | None):
    """A cycle segmenter: in-memory state, checkpointed, reading its input
    source-transparently, emitting each cycle's duration at its end."""

    class Cycles(Producer):
        name = "cycles"
        system_element_name = "Line/M1"
        state_version = 1

        state = SignalRangeInput("state", window="1h")
        cycle_seconds = SignalOutput("cycle_seconds", "float", "duration of each machine cycle")

        def __init__(self) -> None:
            super().__init__()
            self.open: float | None = None

        def snapshot_state(self):
            return {"open": self.open}

        def restore_state(self, state) -> None:
            self.open = state["open"]

        @on_metric("state")
        async def on_state(self, metric) -> None:
            previous = self.state.latest_value_before(metric.timestamp - 1e-6)
            if metric.value and not previous:
                self.open = metric.timestamp
            elif not metric.value and previous and self.open is not None:
                self.cycle_seconds.publish(metric.timestamp - self.open, metric.timestamp)
                self.open = None

    Cycles.backfill = backfill
    return Cycles


def _history(start: float, count: int) -> list[tuple[float, int]]:
    """Machine state samples about ten minutes apart: stopped, running, running."""
    return [(start + i * 600.0 + (i * 37) % 120, 0 if i % 3 == 0 else 1) for i in range(count)]


def _expected(history) -> dict[float, float]:
    out, previous, opened = {}, None, None
    for ts, value in history:
        if value and not previous:
            opened = ts
        elif not value and previous and opened is not None:
            out[ts] = ts - opened
            opened = None
        previous = value
    return out


def _wait(predicate, timeout: float = 60.0, interval: float = 0.2):
    deadline = time.monotonic() + timeout
    while not (result := predicate()):
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(interval)
    return result


@contextlib.contextmanager
def _serving(node, data_dir, producer_cls, **kwargs):
    """A DataOps service on the node's local door, served on its own loop."""
    door = LocalDoor(host="127.0.0.1", http_port=node._ports["api_local"], mqtt_port=node._ports["mqtt_local"])
    svc = chaski.DataOpsService(
        "dataops", node=door, state_dir=node.data_dir / "services" / "dataops", data_dir=data_dir, **kwargs
    )
    svc.add(producer_cls)
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
        # Started up to the point where backfills are planned.
        _wait(lambda: svc._backfill is not None or failed)
        assert not failed, failed
        yield svc
    finally:
        loop.call_soon_threadsafe(stop.set)
        thread.join(timeout=30)
        svc.close()
        loop.close()


def _publish(machine, rows) -> None:
    """Publish each sample once its signal is bound: until then the machine
    service queues only a few."""
    for ts, value in rows:
        _wait(lambda: not machine._pending_sources, interval=0.05)
        machine.publish("state", value, timestamp=ts)


def _signal_id(reader, name: str) -> str:
    def find():
        for row in reader.kv("", contract="_Signal"):
            if row.payload and row.payload.get("name") == name:
                return row.payload["id"]
        return None

    return _wait(find)


def _outputs(reader) -> list[tuple[float, float]]:
    """Every ``cycle_seconds`` sample in the node's metrics stream, in stream order."""
    signal_id = _signal_id(reader, "cycle_seconds")
    stream = reader.stream("metrics", cursor=f"read-{uuid.uuid4().hex[:8]}", signal_ids=[signal_id])
    records = list(stream)
    stream.retire()
    return [(float(r.payload["timestamp"]), float(r.payload["value"])) for r in records]


def _until_outputs(reader, count: int) -> list[tuple[float, float]]:
    def enough():
        rows = _outputs(reader)
        return rows if len({ts for ts, _ in rows}) >= count else None

    return _wait(enough)


def _seed(node, machine, reader, archive, recent) -> FakeHistorian:
    """Recent samples go to the node; the historian holds archive and recent."""
    _publish(machine, recent)
    signal_id = _signal_id(reader, "state")
    historian = FakeHistorian()
    historian.add(signal_id, [*archive, *recent])
    return historian


def test_backfill_then_live_equals_live_processing_of_the_whole_history(tmp_path):
    now = time.time()
    history = _history(now - 3 * DAY, 3 * 144)
    archive = [row for row in history if row[0] < now - DAY]
    recent = [row for row in history if row[0] >= now - DAY]
    later = _history(now + 60.0, 12)
    expected = _expected([*history, *later])

    # The reference: one node whose stream holds the whole history, processed live.
    with (
        chaski.Node("backfill-reference", data_dir=tmp_path / "ref") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("reader") as reader,
        _serving(node, tmp_path / "ref-data", cycles(None)),
    ):
        _publish(machine, history)
        _until_outputs(reader, len(_expected(history)))
        _publish(machine, later)
        reference = _until_outputs(reader, len(expected))

    assert dict(reference) == pytest.approx(expected)

    with (
        chaski.Node("backfill", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("reader") as reader,
    ):
        historian = _seed(node, machine, reader, archive, recent)
        began = time.monotonic()
        with _serving(
            node,
            tmp_path / "data",
            cycles(Backfill(horizon="4d", window="6h")),
            historian=historian,
            backfill_rate=10.0,
        ) as svc:
            assert svc.backfill_holds("cycles")
            _wait(lambda: not svc.backfill_holds("cycles"))
            elapsed = time.monotonic() - began
            job = svc.buffer.backfill_job("cycles", INITIAL)
            assert job is not None and job["done"]
            # Throttled: 10 windows per second at most, the handover window aside.
            assert elapsed >= (job["windows"] - 1) / 10.0

            backfilled = _until_outputs(reader, len(_expected(history)))
            # Handed over without a gap: live samples after the handover are
            # processed live, by the same instance.
            _publish(machine, later)
            outputs = _until_outputs(reader, len(expected))
            assert backfilled == outputs[: len(backfilled)]

            # Each cycle emitted once: no double emission at the handover.
            assert len(outputs) == len({ts for ts, _ in outputs})
            assert sorted(outputs) == sorted(reference)

            # A repair over the whole range, live output included, writes the
            # same samples again: by key, nothing changes.
            repair = svc.request_backfill("cycles", history[0][0], time.time() + 3600)
            _wait(lambda: (svc.buffer.backfill_job("cycles", repair) or {}).get("done"))
            repaired = _outputs(reader)
            assert len(repaired) > len(outputs)
            assert dict(repaired) == dict(outputs)


def test_a_restarted_service_resumes_its_backfill(tmp_path):
    now = time.time()
    history = _history(now - 4 * DAY, 4 * 144)
    archive = [row for row in history if row[0] < now - DAY]
    recent = [row for row in history if row[0] >= now - DAY]
    expected = _expected(history)
    producer = cycles(Backfill(horizon="5d", window="2h"))

    with (
        chaski.Node("backfill-restart", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("reader") as reader,
    ):
        historian = _seed(node, machine, reader, archive, recent)
        with _serving(node, tmp_path / "data", producer, historian=historian, backfill_rate=8.0) as svc:
            _wait(lambda: (svc.buffer.backfill_job("cycles", INITIAL) or {}).get("windows", 0) >= 8)
        # Stopped mid-backfill.
        with _serving(node, tmp_path / "data", producer, historian=historian, backfill_rate=1000.0) as svc:
            job = svc.buffer.backfill_job("cycles", INITIAL)
            assert job is not None and job["windows"] >= 8
            stopped_at = job["position"]
            assert svc.backfill_holds("cycles"), "the restart began the backfill again or skipped it"
            _wait(lambda: not svc.backfill_holds("cycles"))
            assert svc.buffer.backfill_job("cycles", INITIAL)["position"] > stopped_at
            outputs = _until_outputs(reader, len(expected))

    # A window cut short by the stop may be emitted twice: by key, the result
    # is that of one uninterrupted pass.
    assert dict(outputs) == pytest.approx(expected)


def test_a_backfill_reads_history_through_the_node_api_page_by_page(tmp_path, monkeypatch):
    """The historian is the node's API, over HTTP: the service finds it in its
    service account's environment, as in a PREKIT deployment, and pages every
    6 h window (36 samples) five rows at a time. The outcome is that of live
    processing over the whole history."""
    now = time.time()
    # Microsecond timestamps, what the historian stores and the API returns.
    history = [(round(ts, 6), value) for ts, value in _history(now - 3 * DAY, 3 * 144)]
    archive = [row for row in history if row[0] < now - DAY]
    recent = [row for row in history if row[0] >= now - DAY]
    expected = _expected(history)
    api = FakeNodeApi()

    with (
        chaski.Node("backfill-node-api", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("reader") as reader,
        api.serve() as origin,
    ):
        seeded = _seed(node, machine, reader, archive, recent)
        for signal_id, rows in seeded.points.items():
            api.add(signal_id, rows)
        monkeypatch.setenv("PREKIT_URL", origin)
        monkeypatch.setenv("PREKIT_CLIENT_ID", CLIENT_ID)
        monkeypatch.setenv("PREKIT_CLIENT_SECRET", CLIENT_SECRET)
        monkeypatch.setenv("DATAOPS_HISTORIAN_PAGE_SIZE", "5")
        with _serving(node, tmp_path / "data", cycles(Backfill(horizon="4d", window="6h")), backfill_rate=100.0) as svc:
            assert isinstance(svc.historian, NodeHistorian)
            _wait(lambda: not svc.backfill_holds("cycles"))
            outputs = _until_outputs(reader, len(expected))

    assert dict(outputs) == pytest.approx(expected)
    assert len(outputs) == len({ts for ts, _ in outputs})
    assert api.tokens_issued == 1
    assert any("cursor" in request for request in api.metric_requests)
    assert all(request["limit"] == 5 for request in api.metric_requests)
