"""The DataOps runtime against a real node: the failure modes of November 2026.

Each test runs a ``DataOpsService`` on a real colcad (``COLCAD_BINARY``):

* a tick of a producer whose handler is stuck in a page holds up nothing but
  that producer (the 3.5-day stall of the chocolate edge, 2026-11-05);
* a large first trim after an outage runs while the ingest is live, without
  holding it up (6.77 M rows and a six-minute stop, 2026-11-09; scaled down);
* every page is committed before its ack, also while a backfill commits its
  windows (ack before commit through the shared commit counter);
* ticks missed in a three-day stall are run afterwards, each at its instant,
  for a producer that catches up (the OEE hole of 2026-11-05 to 11-09).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import sqlite3
import threading
import time

import pytest
from test_dataops_backfill_integration import _publish, _signal_id, _wait

import chaski
from chaski.dataops import Producer, SignalOutput, SignalRangeInput, every, on_metric
from chaski.dataops.buffer import Buffer
from chaski.door import Stream
from chaski.service import LocalDoor

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"),
    reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE",
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


def _loop_latency(svc) -> float:
    """How long the service's loop takes to run a callback handed to it."""
    loop = svc._loop
    done = threading.Event()
    began = time.monotonic()
    loop.call_soon_threadsafe(done.set)
    assert done.wait(10), "the service's loop did not answer in 10 s"
    return time.monotonic() - began


# ─── a tick blocked behind a page ───────────────────────────────────────────


@contextlib.contextmanager
def _serving(node, data_dir, *classes, **kwargs):
    """A DataOps service running ``classes`` on the node's local door."""
    door = LocalDoor(host="127.0.0.1", http_port=node._ports["api_local"], mqtt_port=node._ports["mqtt_local"])
    svc = chaski.DataOpsService(
        "dataops",
        node=door,
        state_dir=node.data_dir / "services" / "dataops",
        data_dir=data_dir,
        health_port=0,
        **kwargs,
    )
    for cls in classes:
        svc.add(cls)
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
        _wait(lambda: svc._backfill is not None or failed)
        assert not failed, failed
        yield svc
    finally:
        loop.call_soon_threadsafe(stop.set)
        thread.join(timeout=30)
        svc.close()
        loop.close()


def test_a_producer_stuck_in_a_page_holds_up_only_itself(tmp_path):
    """One producer's handler blocks its worker for seconds inside a page.
    Its own tick waits and runs after it, never beside it; another producer's
    ticks, the service's loop and the other producer's records go on."""
    stuck = threading.Event()
    release = threading.Event()
    log: list[tuple[str, float]] = []

    class Stuck(Producer):
        name = "stuck"
        system_element_name = "Line/M1"
        state = SignalRangeInput("state", window="1h")

        @on_metric("state")
        async def on_state(self, metric) -> None:
            if metric.value == 99:
                stuck.set()
                release.wait(20)  # synchronous: the worker thread is blocked
            log.append(("stuck.state", time.monotonic()))

        @every("1s")
        async def tick(self) -> None:
            log.append(("stuck.tick", time.monotonic()))

    class Free(Producer):
        name = "free"
        system_element_name = "Line/M2"
        speed = SignalRangeInput("speed", window="1h")

        @on_metric("speed")
        async def on_speed(self, metric) -> None:
            log.append(("free.speed", time.monotonic()))

        @every("1s")
        async def tick(self) -> None:
            log.append(("free.tick", time.monotonic()))

    with (
        chaski.Node("stuck-page", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("machine2", mount="Line/M2") as machine2,
        _serving(node, tmp_path / "data", Stuck, Free) as svc,
    ):
        _publish(machine, [(time.time(), 1)])
        _wait(lambda: any(name == "stuck.state" for name, _ in log))
        _publish(machine, [(time.time(), 99)])
        assert stuck.wait(30)
        blocked_at = time.monotonic()
        try:
            time.sleep(3.5)
            assert _loop_latency(svc) < 0.5
            machine2.publish("speed", 3.0, timestamp=time.time())
            time.sleep(1.5)
            during = [name for name, at in log if at > blocked_at]
            assert "stuck.tick" not in during, "a tick ran beside its producer's page"
            assert during.count("free.tick") >= 3, "another producer's ticks stopped"
        finally:
            release.set()
        _wait(lambda: any(name == "stuck.tick" and at > blocked_at for name, at in log))
        # The other producer's record waited at most for the page of the stuck one.
        _wait(lambda: any(name == "free.speed" for name, _ in log), timeout=30)


# ─── a large trim while the ingest is live ─────────────────────────────────


def test_a_large_first_trim_runs_beside_live_ingest(tmp_path):
    """An edge back from a 3.4-day stop: its buffer holds far more than the
    horizon. The trim deletes it in short statements while live records keep
    being handled within a second."""
    seen: dict[float, float] = {}

    class Live(Producer):
        name = "live"
        system_element_name = "Line/M1"
        state = SignalRangeInput("state", window="10m")

        @on_metric("state")
        async def on_state(self, metric) -> None:
            seen[float(metric.timestamp)] = time.monotonic()

    with (
        chaski.Node("trim-live", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("reader") as reader,
    ):
        _publish(machine, [(time.time(), 0)])
        signal_id = _signal_id(reader, "state")
        data = tmp_path / "data"
        data.mkdir()
        old = time.time() - 4 * DAY
        rows = 300_000
        with Buffer(data / "buffer.sqlite3") as buffer:
            for start in range(0, rows, 50_000):
                buffer.append_many([(signal_id, old + i, i) for i in range(start, start + 50_000)])
            # The producer ran before the outage: no replay of its new code.
            from chaski.dataops.codehash import compute_code_hash

            buffer.set_watermark("live", old, compute_code_hash(Live))
        with _serving(node, data, Live, buffer_floor=0.0, trim_interval=1.0) as svc:
            path = data / "buffer.sqlite3"
            latencies = []
            for _ in range(20):
                ts = time.time()
                published = time.monotonic()
                _publish(machine, [(ts, 1)])
                _wait(lambda ts=ts: ts in seen, timeout=10, interval=0.01)
                latencies.append(seen[ts] - published)
                time.sleep(0.2)
            _wait(lambda: svc.buffer.earliest(signal_id) > old + rows - 2, timeout=120)
            assert max(latencies) < 1.0, f"a live record waited {max(latencies):.2f} s beside the trim"
            wal = path.with_name(path.name + "-wal")
            assert not wal.exists() or wal.stat().st_size < 80 * 2**20


# ─── a page is committed before its ack ────────────────────────────────────


def test_every_ack_follows_the_commit_of_its_page_while_backfills_commit(tmp_path, monkeypatch):
    """A crash right after any ack finds the page's checkpoint on disk, also
    while repair backfills commit their windows beside the pages (the shared
    commit counter let a page end without a commit and be acked)."""

    class Counter(Producer):
        name = "counter"
        system_element_name = "Line/M1"
        state_version = 1
        state = SignalRangeInput("state", window="1h")

        def __init__(self) -> None:
            super().__init__()
            self.total = 0.0

        def snapshot_state(self):
            return {"total": self.total}

        def restore_state(self, state) -> None:
            self.total = state["total"]

        @on_metric("state")
        async def on_state(self, metric) -> None:
            self.total += float(metric.value)

    checks: list[tuple[int, int]] = []
    paths: list = []
    original = Stream.ack

    def ack(self, offset, *args, **kwargs):
        if paths and "ingest-" in self.cursor:
            # What a crash right now would leave: the committed state only.
            conn = sqlite3.connect(f"file:{paths[0]}?mode=ro", uri=True)
            try:
                row = conn.execute("SELECT positions FROM producer_checkpoints WHERE producer='counter'").fetchone()
            finally:
                conn.close()
            handled = json.loads(row[0]).get("on_state", -1) if row else -1
            checks.append((offset, handled))
        return original(self, offset, *args, **kwargs)

    monkeypatch.setattr(Stream, "ack", ack)
    # A backfill's commit that takes a while, as on a busy edge disk: pages end
    # while one is open.
    progress = Buffer.backfill_progress

    def slow_progress(self, *args, **kwargs):
        time.sleep(0.05)
        return progress(self, *args, **kwargs)

    monkeypatch.setattr(Buffer, "backfill_progress", slow_progress)
    with (
        chaski.Node("ack-commit", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        _serving(node, tmp_path / "data", Counter, historian=_EmptyHistorian(), backfill_rate=1000.0) as svc,
    ):
        paths.append(tmp_path / "data" / "buffer.sqlite3")
        stop = threading.Event()

        def repairs():
            while not stop.is_set():
                now = time.time()
                svc.request_backfill("counter", now - 3600, now, window="5m")
                time.sleep(0.05)

        repairing = threading.Thread(target=repairs)
        repairing.start()
        try:
            now = time.time()
            for i in range(300):
                _publish(machine, [(now + i * 0.01, 1)])
                time.sleep(0.01)
            try:
                _wait(lambda: svc.instances[0].total >= 300, timeout=60)
            except AssertionError:
                raise AssertionError(f"total={svc.instances[0].total} checks={checks[-5:]}") from None
        finally:
            stop.set()
            repairing.join()
    assert checks
    late = [(offset, handled) for offset, handled in checks if handled < offset]
    # The acked offset can be a record of another stream entry the filter
    # skipped (no state) only at its very start; every other ack is covered.
    assert len(late) <= 1, late


class _EmptyHistorian:
    def window(self, signal_id, start, end):
        import pandas as pd

        return pd.DataFrame({"ts": [], "value": []}, columns=["ts", "value"])

    def latest_before(self, signal_id, before):
        return None


# ─── three days missed, then caught up ─────────────────────────────────────


def test_three_days_of_missed_ticks_are_caught_up_at_their_instants(tmp_path):
    """An edge stalled for three days, with its inputs still in the stream:
    after the restart the ingest catches up, and every hourly tick missed runs
    at its instant over the input as it was then, published at that instant.
    The missed_ticks finding stands until the catch-up is done."""
    now = time.time()
    start = now - 3 * DAY
    samples = [(start - 3600.0 + i * 600.0, float(i % 6)) for i in range(int((now - start + 3600.0) / 600.0))]

    class Oee(Producer):
        name = "oee"
        system_element_name = "Line/M1"
        state = SignalRangeInput("state", window="2h")
        mean = SignalOutput("hourly_mean", "float", "mean state over the hour")

        @every("1h", catch_up=True)
        async def tick(self) -> None:
            frame = self.state.fetch(self.now - 3600.0, self.now)
            if len(frame):
                self.mean.publish(float(frame["value"].mean()), self.now)

    with (
        chaski.Node("catch-up", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("reader") as reader,
    ):
        _publish(machine, samples)
        data = tmp_path / "data"
        data.mkdir()
        last_run = math.floor(start / 3600.0) * 3600.0
        with Buffer(data / "buffer.sqlite3") as buffer:
            # The service ran this producer until three days ago.
            buffer.set_tick_progress("oee", "tick", last_run)
            from chaski.dataops.codehash import compute_code_hash

            buffer.set_watermark("oee", last_run, compute_code_hash(Oee))
        with _serving(node, data, Oee, backfill_rate=1000.0) as svc:
            _wait(lambda: svc.missed_ticks is not None and svc.missed_ticks.status(), timeout=30)
            (span,) = svc.missed_ticks.status()
            assert span["catch_up"], span
            _wait(lambda: (svc.buffer.backfill_job("oee", span["catch_up"]) or {}).get("done"), timeout=120)
            _wait(lambda: not svc.missed_ticks.status(), timeout=90)

        mean_id = _signal_id(reader, "hourly_mean")
        stream = reader.stream("metrics", cursor="read-catch-up", signal_ids=[mean_id])
        published = {float(r.payload["timestamp"]): float(r.payload["value"]) for r in stream}

    expected_instants = [last_run + 3600.0 * k for k in range(1, 100) if last_run + 3600.0 * k <= now - 60.0]
    assert len(published) >= len(expected_instants) - 1
    for instant in expected_instants[:-1]:
        values = [v for ts, v in samples if instant - 3600.0 <= ts < instant]
        assert published[instant] == pytest.approx(sum(values) / len(values))
