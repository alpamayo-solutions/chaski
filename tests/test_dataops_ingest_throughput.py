"""Catch-up throughput of the ingest lane, as a regression guard.

A plant hub's DataOps service once caught up a backlog at ~50 records/s
while the door served the same pages in 0.1 s: the time went into chaski's
own per-record work. This drives full pages of 1,000 records for 237 signals
through a real :class:`~chaski.dataops.ingest.Ingest`, a real buffer and the
service's own dispatch (:func:`~chaski.dataops.service.build_dispatch`) into
a site-meter-like producer: one ``SignalRangeInput`` and one checkpointed
``@on_metric`` handler per signal, keeping a few readings per signal.

The threshold is far below what the lane does on a laptop, so a slow CI
runner passes; per-record checkpoints, commits or loop hops fail it.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque

import pytest
from dataops_fakes import FakeDoor, FakeRuntime, signal_entry, stream_opener

from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.ingest import Ingest
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.service import build_dispatch
from chaski.dataops.triggers import on_metric
from chaski.door import Page, Record

SIGNALS = 237
PAGE = 1000
PAGES = 10
#: Records per second the lane must at least keep up while catching up.
MIN_RECORDS_PER_S = 5000


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


def _handler(attr: str):
    @on_metric(attr)
    async def handle(self, metric) -> None:
        seen = self.readings.setdefault(attr, deque(maxlen=4))
        if (metric.timestamp, metric.value) not in seen:
            seen.append((metric.timestamp, metric.value))
        self.handled += 1

    handle.__name__ = f"on_{attr}"
    return handle


class _MeterBase(Producer):
    state_version = 1

    def __init__(self) -> None:
        super().__init__()
        self.readings: dict[str, deque] = {}
        self.handled = 0

    def snapshot_state(self):
        return {attr: [list(r) for r in seen] for attr, seen in self.readings.items()}

    def restore_state(self, state) -> None:
        self.readings = {attr: deque((tuple(r) for r in seen), maxlen=4) for attr, seen in state.items()}


def meter_class(signals: int = SIGNALS) -> type[Producer]:
    attrs: dict = {"name": "bench_meter", "system_element_name": "GridConnection", "__module__": __name__}
    for i in range(signals):
        attr = f"in_{i}"
        attrs[attr] = SignalRangeInput(f"signal_{i}", window="15m")
        attrs[f"on_{attr}"] = _handler(attr)
    return type("BenchMeter", (_MeterBase,), attrs)


def _record(offset: int, signal: int, ts: float) -> Record:
    return Record(
        offset=offset,
        origin_offset=offset,
        topic=f"colca/v1/_Metric/n-1/m/{signal}",
        payload={"signal_id": f"sig-{signal}", "value": float(offset % 97), "timestamp": ts},
        ts=ts * 1000,
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )


def pages(count: int = PAGES, size: int = PAGE, signals: int = SIGNALS) -> list[Page]:
    out = []
    offset = 1
    for _ in range(count):
        records = [_record(offset + i, (offset + i) % signals, 1_700_000_000.0 + offset + i) for i in range(size)]
        offset += size
        out.append(Page(records=records, next=offset))
    return out


def build(tmp_path, signals: int = SIGNALS):
    door = FakeDoor([signal_entry(f"sig-{i}", f"signal_{i}") for i in range(signals)])
    buffer = Buffer(tmp_path / "buffer.sqlite3")
    runtime = FakeRuntime(door, buffer)
    producer = meter_class(signals)().attach(runtime)
    dispatch, signal_ids, _ = build_dispatch(runtime, [producer])
    ingest = Ingest(stream_opener(door), buffer, dispatch=dispatch, signal_ids=signal_ids)
    return door, buffer, producer, ingest


async def drain(ingest: Ingest, door: FakeDoor, queued: list[Page]) -> float:
    """Seconds to process ``queued`` page by page, as the catch-up loop does."""
    for page in queued:
        door.queue(page)
    started = time.perf_counter()
    for _ in queued:
        await ingest.run_once()
    return time.perf_counter() - started


def test_catch_up_keeps_the_minimum_rate(tmp_path):
    door, buffer, producer, ingest = build(tmp_path)
    try:
        elapsed = asyncio.run(drain(ingest, door, pages()))
        assert producer.handled == PAGES * PAGE
        rate = PAGES * PAGE / elapsed
        print(f"ingest catch-up: {rate:,.0f} records/s ({elapsed / PAGES * 1000:.0f} ms per page of {PAGE})")
        assert rate >= MIN_RECORDS_PER_S, f"{rate:,.0f} records/s"
    finally:
        buffer.close()
