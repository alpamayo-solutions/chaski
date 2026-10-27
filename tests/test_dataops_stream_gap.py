"""Input records the stream pruned before the ingest read them.

A gap used to raise in the ingest, which its supervisor restarted, which met
the same gap: a crash loop with no way out. Now a service with a historian
records a repair backfill over the gap and goes on; one without waits, with
a finding and a 503, until an operator accepts the loss with a command.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from dataops_fakes import FakeDoor, run_async, stream_opener

from chaski.dataops import DataOpsService, Producer, SignalRangeInput, on_metric
from chaski.dataops.backfill import GAP_REPAIR
from chaski.dataops.buffer import Buffer
from chaski.dataops.commands import Command, CommandRejected
from chaski.dataops.health import HealthState
from chaski.dataops.ingest import Ingest
from chaski.door import Gap, Page, Record

GAP = Gap(
    stream="metrics", from_offset=1, to_offset=40, first_ts=1_700_000_000_000, last_ts=1_700_003_600_000, approx=False
)


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


def _record(offset: int, ts: float) -> Record:
    return Record(
        offset=offset,
        origin_offset=offset,
        topic="colca/v1/_Metric/n-1/x/a",
        payload={"signal_id": "sig-a", "value": 1.0, "timestamp": ts},
        ts=ts * 1000,
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )


# ─── the ingest ────────────────────────────────────────────────────────────


@run_async
async def test_the_ingest_waits_at_a_gap_it_may_not_pass_and_goes_on_once_woken(tmp_path):
    door = FakeDoor()
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    allowed = [False]
    asked: list[Gap] = []

    def on_gap(gap, survivors):
        asked.append(gap)
        return allowed[0]

    page = Page(records=[_record(41, 1_700_003_700.0)], next=42, gap=GAP)
    for _ in range(5):
        door.queue(page)
    ingest = Ingest(stream_opener(door), buffer, signal_ids=["sig-a"], on_gap=on_gap, idle_drain_s=None)
    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        while not ingest.gap_waiting:
            assert not task.done(), task
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.05)
        # Waiting, not retrying: asked once, nothing appended or acked.
        assert len(asked) == 1 and not task.done()
        assert door.acked == [] and buffer.points("sig-a", 0, 2e9) == []
        assert "offsets 1..40" in ingest.gap_waiting

        allowed[0] = True
        ingest.wake()
        while not door.acked:
            await asyncio.sleep(0.005)
        assert door.acked[0][2] == 41
        assert ingest.gap_waiting == ""
        assert buffer.points("sig-a", 0, 2e9) == [(1_700_003_700.0, 1.0)]
    finally:
        stop.set()
        ingest.wake()
        await asyncio.wait_for(task, 5)
        buffer.close()


def test_the_health_door_fails_while_the_ingest_waits_at_a_gap():
    waiting = ["offsets 1..40 of metrics were pruned before they were read"]
    state = HealthState(gap=lambda: waiting[0])
    ok, body = state.answer()
    assert not ok and body["stream_gap"] == waiting[0] and body["status"] == "unhealthy"
    waiting[0] = ""
    ok, body = state.answer()
    assert ok and "stream_gap" not in body


# ─── the service ───────────────────────────────────────────────────────────


class Reader(Producer):
    name = "reader"
    system_element_name = "press"
    a = SignalRangeInput("a", window="1h")

    @on_metric("a")
    async def on_a(self, metric) -> None:
        pass


class _Historian:
    def window(self, signal_id, start, end):  # pragma: no cover - not read here
        raise AssertionError

    def latest_before(self, signal_id, before):  # pragma: no cover
        return None


def _service(tmp_path, *, historian):
    svc = DataOpsService("dataops", state_dir=tmp_path, data_dir=tmp_path / "data", historian=historian)
    svc._node_id = "n-1"
    svc._hierarchy = ("dataops",)
    svc._local_buffer = Buffer(tmp_path / "buffer.sqlite3")
    svc.add(Reader)
    svc.instances = [Reader().attach(svc)]
    sent: list[tuple[str, str]] = []
    svc.send = lambda topic, payload, retain=False: sent.append((topic, payload))  # type: ignore[method-assign]
    svc.retract = lambda topic: sent.append((topic, ""))  # type: ignore[method-assign]
    return svc, sent


def test_with_a_historian_a_gap_is_repaired_once_and_the_ingest_goes_on(tmp_path):
    svc, sent = _service(tmp_path, historian=_Historian())
    try:
        survivors = [_record(41, 1_700_003_700.0)]
        assert svc._on_gap(GAP, survivors) is True
        assert svc._on_gap(GAP, survivors) is True  # the page redelivered after a crash
        jobs = svc.buffer.backfill_jobs(pending_only=True)
        assert [(j["producer"], j["job"]) for j in jobs] == [("reader", f"{GAP_REPAIR}1-40")]
        (job,) = jobs
        assert job["start"] <= 1_700_000_000.0 - 3600 + 1 and job["end"] >= 1_700_003_600.0 + 3600 - 1
        topic, payload = sent[0]
        assert topic == "colca/v1/_Finding/n-1/dataops/stream_gap"
        finding = json.loads(payload)
        assert finding["detail"]["repair"] == f"{GAP_REPAIR}1-40" and finding["suggested_severity"] == "warning"

        svc.buffer.finish_backfill("reader", f"{GAP_REPAIR}1-40")
        svc._gap_repaired()
        assert sent[-1] == ("colca/v1/_Finding/n-1/dataops/stream_gap", "")
    finally:
        svc.buffer.close()


@run_async
async def test_without_a_historian_the_ingest_waits_until_an_operator_accepts_the_loss(tmp_path):
    svc, sent = _service(tmp_path, historian=None)
    try:
        assert svc._on_gap(GAP, []) is False
        assert svc._on_gap(GAP, []) is False
        (finding,) = [json.loads(p) for t, p in sent if t.endswith("/stream_gap") and p]
        assert finding["suggested_severity"] == "error"
        assert finding["detail"]["accept"] == {"contract": "_CmdMaintain", "path": "dataops/acceptGap"}

        command = Command(path="dataops/acceptGap", verb="acceptGap", contract="_CmdMaintain", actor_label="Ana")
        message = await svc._accept_gap(command)
        assert "1..40" in message
        # Durable: a restart goes on past it too.
        again = Buffer(tmp_path / "buffer.sqlite3")
        assert again.accepted_gap() == 40
        again.close()
        assert svc._on_gap(GAP, []) is True
        assert sent[-1] == ("colca/v1/_Finding/n-1/dataops/stream_gap", "")

        with pytest.raises(CommandRejected):
            await svc._accept_gap(command)
    finally:
        svc.buffer.close()


def test_a_later_gap_beyond_the_accepted_one_waits_again(tmp_path):
    svc, _sent = _service(tmp_path, historian=None)
    try:
        svc.buffer.accept_gap(40)
        later = Gap(stream="metrics", from_offset=41, to_offset=90, first_ts=None, last_ts=None, approx=True)
        assert svc._on_gap(GAP, []) is True
        assert svc._on_gap(later, []) is False
    finally:
        svc.buffer.close()
