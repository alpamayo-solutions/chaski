"""Tests for chaski.dataops.service's dispatch, trim and tick machinery.

The fetch filter (`signal_ids`) says which points are buffered; the dispatch
table (`dispatch`) says which methods fire for a buffered point. A producer
driven only by ticks is not in `dispatch`, but its inputs must be in
`signal_ids`, or its window reads stay empty.

A fake `Door` (KV only) stands in for resolution.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from unittest.mock import patch

import pytest
from dataops_fakes import FakeDoor, FakeRuntime, run_async, signal_entry

from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.service import (
    build_dispatch,
    compute_trim_horizons,
    decode_metric,
    make_handler,
    off_loop,
    schedule_periodic,
    trim_buffer,
)
from chaski.dataops.triggers import every, on_constant, on_metric, on_signal
from chaski.door import Record


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


@pytest.fixture
def door():
    return FakeDoor(
        [
            signal_entry("sig-tick", "tick_only_signal"),
            signal_entry("sig-event", "on_metric_signal"),
        ]
    )


@pytest.fixture
def runtime(door, buffer):
    return FakeRuntime(door, buffer)


@pytest.fixture(autouse=True)
def _isolate_registry():
    """Defining test-only Producer subclasses here must never leak into
    other test modules."""
    saved_registry = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved_registry)


# ─── the producers under test ────────────────────────────────────────────


class TickOnlyProducer(Producer):
    """Declares an input but is driven ONLY by @every/@cron — no @on_metric
    at all. Its input's signal id must still land in the fetch filter, or
    the buffer never holds its points."""

    name = "tick_only"
    system_element_name = "SE-Tick"

    tick_input = SignalRangeInput("tick_only_signal", window="1h")

    @every("30s")
    async def tick(self) -> None:
        pass


class EventDrivenProducer(Producer):
    """Declares an input wired to @on_metric."""

    name = "event_driven"
    system_element_name = "SE-Event"

    event_input = SignalRangeInput("on_metric_signal", window="1h")

    @on_metric("event_input")
    async def on_event(self, metric) -> None:
        pass


def _instantiate(runtime, *classes):
    return [cls().attach(runtime) for cls in classes]


# ─── signal_ids: the fetch filter ────────────────────────────────────────


def test_tick_only_producer_contributes_its_signal_to_the_filter(runtime):
    """A @every/@cron-only producer's declared input was once never added
    to signal_ids, so its points never entered the buffer."""
    instances = _instantiate(runtime, TickOnlyProducer)
    _dispatch, signal_ids, _unresolved = build_dispatch(runtime, instances)
    assert "sig-tick" in signal_ids


def test_filter_is_the_union_of_tick_only_and_on_metric_inputs(runtime):
    """Both kinds of inputs are in the filter."""
    instances = _instantiate(runtime, TickOnlyProducer, EventDrivenProducer)
    _dispatch, signal_ids, _unresolved = build_dispatch(runtime, instances)
    assert set(signal_ids) == {"sig-tick", "sig-event"}


# ─── dispatch: @on_metric only ───────────────────────────────────────────


def test_dispatch_table_has_no_entry_for_a_tick_only_producer(runtime):
    """Buffering != dispatching. A tick-only producer's signal belongs in
    the filter, but nothing should call it per-record — it reads its input
    on its own schedule."""
    instances = _instantiate(runtime, TickOnlyProducer)
    dispatch, _signal_ids, _unresolved = build_dispatch(runtime, instances)
    assert dispatch == {}


def test_dispatch_table_still_has_the_on_metric_entry(runtime):
    instances = _instantiate(runtime, TickOnlyProducer, EventDrivenProducer)
    dispatch, _signal_ids, _unresolved = build_dispatch(runtime, instances)
    assert list(dispatch.keys()) == ["sig-event"]
    assert len(dispatch["sig-event"]) == 1


# ─── unresolved input: startup does not raise ────────────────────────────


class UnresolvableProducer(Producer):
    """Declares an input for a signal that doesn't exist in KV yet."""

    name = "unresolvable"
    system_element_name = "SE-Missing"

    missing_input = SignalRangeInput("no_such_signal", window="1h")

    @every("1m")
    async def tick(self) -> None:
        pass


def test_unresolvable_input_is_skipped_not_raised(runtime):
    instances = _instantiate(runtime, UnresolvableProducer)
    dispatch, signal_ids, unresolved = build_dispatch(runtime, instances)  # must not raise
    assert dispatch == {}
    assert signal_ids == []
    # Counted, not just skipped: this is what tells the service to keep
    # retrying instead of running on its timers for the rest of the process.
    assert unresolved == 1


@run_async
async def test_a_late_commissioned_signal_reaches_dispatch_without_a_restart(runtime):
    """A signal commissioned after the producer started reaches dispatch
    without a restart.
    """
    import chaski.dataops.service as service_module

    class _Ingest:
        def __init__(self):
            self.bound: list[tuple[dict, list]] = []

        def rebind(self, dispatch, signal_ids):
            self.bound.append((dispatch, list(signal_ids or [])))

    ingest = _Ingest()
    stop = asyncio.Event()
    started: list[str] = []
    calls = {"n": 0}

    # First pass resolves nothing (the signals do not exist yet); the second
    # resolves — exactly what applying a plant model looks like from here.
    def fake_build(runtime, instances):
        calls["n"] += 1
        if calls["n"] == 1:
            return {}, [], 1
        return {"sig-late": ["handler"]}, ["sig-late"], 0

    with patch.object(service_module, "build_dispatch", fake_build):
        await service_module.reresolve_loop(
            runtime,
            [],
            ingest,
            stop,
            lambda: started.append("ingest"),
            interval_s=0.01,
        )

    assert ingest.bound == [({"sig-late": ["handler"]}, ["sig-late"])]
    assert started == ["ingest"], "the loop must start if nothing resolved at startup"


@run_async
async def test_the_retry_stops_once_everything_resolves(runtime):
    """The retry loop stops reading KV once everything resolved."""
    import chaski.dataops.service as service_module

    class _Ingest:
        def rebind(self, dispatch, signal_ids):
            pass

    calls = {"n": 0}

    def fake_build(runtime, instances):
        calls["n"] += 1
        return {"s": ["h"]}, ["s"], 0

    with patch.object(service_module, "build_dispatch", fake_build):
        await asyncio.wait_for(
            service_module.reresolve_loop(
                runtime,
                [],
                _Ingest(),
                asyncio.Event(),
                lambda: None,
                interval_s=0.01,
            ),
            timeout=2,
        )

    assert calls["n"] == 1


# ─── trim horizons and the periodic trim ─────────────────────────────────
#
# The horizon per signal is the larger of its widest declared window and the
# broker's metrics retention.


class ShortWindowProducer(Producer):
    """Declares a window shorter than the broker's own retention."""

    name = "short_window"
    system_element_name = "SE-Short"

    short_input = SignalRangeInput("on_metric_signal", window="10s")

    @on_metric("short_input")
    async def on_event(self, metric) -> None:
        pass


class LongWindowProducer(Producer):
    """Declares a LONGER window on the SAME signal as ShortWindowProducer."""

    name = "long_window"
    system_element_name = "SE-Long"

    long_input = SignalRangeInput("on_metric_signal", window="2h")

    @on_metric("long_input")
    async def on_event(self, metric) -> None:
        pass


def test_horizon_is_the_largest_declared_window_across_producers_sharing_a_signal(runtime):
    """Two producers declare different windows on the SAME signal — the
    horizon must be the larger of the two, not whichever producer happened
    to be walked last."""
    instances = _instantiate(runtime, ShortWindowProducer, LongWindowProducer)
    horizons = compute_trim_horizons(instances, retention_s=1.0)
    assert horizons == {"sig-event": 2 * 3600.0}


def test_horizon_falls_back_to_broker_retention_when_it_exceeds_every_declared_window(runtime):
    """A short window never trims tighter than the broker's retention, which
    replay needs."""
    instances = _instantiate(runtime, ShortWindowProducer)
    horizons = compute_trim_horizons(instances, retention_s=999_999.0)
    assert horizons == {"sig-event": 999_999.0}


def test_horizon_uses_the_declared_window_when_it_exceeds_retention(runtime):
    instances = _instantiate(runtime, ShortWindowProducer)
    horizons = compute_trim_horizons(instances, retention_s=1.0)
    assert horizons == {"sig-event": 10.0}


def test_horizon_skips_an_unresolved_input_rather_than_raising(runtime):
    instances = _instantiate(runtime, UnresolvableProducer)
    horizons = compute_trim_horizons(instances, retention_s=3600.0)  # must not raise
    assert horizons == {}


def test_trim_buffer_deletes_only_points_past_the_computed_horizon(buffer, runtime):
    """A producer's declared window flows through trim_buffer: a point older
    than the horizon is deleted, one inside it survives."""
    now = time.time()
    buffer.append("sig-event", now - 20.0, "old")  # older than the 10s window
    buffer.append("sig-event", now - 1.0, "recent")  # inside the 10s window

    instances = _instantiate(runtime, ShortWindowProducer)

    # retention_s shorter than the declared window; trim_buffer computes
    # its own horizons from the live instances on every call.
    trim_buffer(buffer, instances, retention_s=1.0)

    df = buffer.window("sig-event", 0.0, now + 1.0)
    assert list(df["value"]) == ["recent"], "trim must delete the point past the horizon and keep the one inside it"


class LateResolvedProducer(Producer):
    """Declares an input for a signal that appears in KV only later; its own
    signal name keeps the seeded buffer apart from other tests."""

    name = "late_resolved"
    system_element_name = "SE-Late"

    late_input = SignalRangeInput("late_signal", window="10s")

    @on_metric("late_input")
    async def on_event(self, metric) -> None:
        pass


def test_trim_buffer_recomputes_horizons_so_a_late_resolved_input_gets_trimmed(buffer, door, runtime):
    """The trim job recomputes horizons on every run, so a signal that resolves
    late is trimmed too."""
    instances = _instantiate(runtime, LateResolvedProducer)
    now = time.time()
    buffer.append("sig-late", now - 20.0, "old")  # older than the 10s window, once it resolves

    # At "startup" the signal is not commissioned in KV yet — unresolved,
    # so nothing can be trimmed for it.
    trim_buffer(buffer, instances, retention_s=1.0)
    df = buffer.window("sig-late", 0.0, now + 1.0)
    assert list(df["value"]) == ["old"], "an unresolved input must not be trimmed"

    # The signal is commissioned later: KV gains the _Signal entry (mirrors
    # a commissioning while the service keeps running).
    door.entries.append(signal_entry("sig-late", "late_signal"))

    trim_buffer(buffer, instances, retention_s=1.0)
    df = buffer.window("sig-late", 0.0, now + 1.0)
    assert list(df["value"]) == [], "a late-resolved input must be trimmed on the very next trim run, without a restart"


def test_the_doorbell_rings_for_a_metric_franzmq_cannot_decode():
    """An undecodable message still rings the doorbell; a well-formed one takes
    the typed path."""
    import json

    import franzmq
    from paho.mqtt.client import MQTTMessage

    from chaski.dataops.service import DOORBELL_WILDCARD, ring_even_if_undecodable

    client = franzmq.Client(client_id="doorbell-test")
    rings: list[str] = []
    client.message_callback_add(DOORBELL_WILDCARD, lambda c, u, m: rings.append(m.topic))
    ring_even_if_undecodable(client)

    def deliver(payload: dict) -> None:
        message = MQTTMessage(mid=1, topic=b"colca/v1/_Metric/n1/line1/temp")
        message.payload = json.dumps(payload).encode()
        client._handle_on_message(message)

    deliver({"v": 1.0, "value": 1.0, "signal_id": "s"})  # no timestamp: the decode raises
    deliver({"v": 2.0, "value": 2.0, "signal_id": "s", "timestamp": 1.0})  # decodes fine
    assert rings == ["colca/v1/_Metric/n1/line1/temp"] * 2, rings


def test_a_periodic_tick_is_scheduled_as_a_plain_function_not_a_coroutine():
    """`off_loop` turns an async tick into a plain function, which
    AsyncIOScheduler runs in its thread pool, and still runs the coroutine
    body, exceptions included. It wraps a bound Producer method, as in
    production."""
    import inspect

    class _TickProducer(Producer):
        name = "off_loop_plain_function_test"
        system_element_name = "SE-OffLoop"

        def __init__(self):
            super().__init__()
            self.ran = []

        @every("10s")
        async def tick(self):
            self.ran.append(True)

        @every("10s")
        async def broken(self):
            raise RuntimeError("tick failed")

    inst = _TickProducer()

    job = off_loop(inst.tick)
    assert not inspect.iscoroutinefunction(job), (
        "off_loop must hand APScheduler a plain function — a coroutine job "
        "would run on the event loop and block every timer in the process"
    )
    job()
    assert inst.ran == [True]

    try:
        off_loop(inst.broken)()
    except RuntimeError as exc:
        assert "tick failed" in str(exc)
    else:
        raise AssertionError("a failing tick must propagate, not vanish")


def test_schedule_periodic_ignores_on_constant_and_on_signal_specs_without_warning(caplog):
    """`OnConstantSpec`/`OnSignalSpec` triggers are scheduled by
    `watch.gather_triggers`, not here — `schedule_periodic` must skip them
    silently, the same as it already silently skips `OnMetricSpec` (handled
    by the ingest dispatch table), not log them as an unrecognised spec.

    Regression for the dataops image logging "Unknown trigger spec
    OnConstantSpec/OnSignalSpec ... skipped" at startup for every producer
    using either decorator, even though `watch.gather_triggers` already
    wires them up correctly — `schedule_periodic` just did not know about
    the two spec types yet.
    """
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    class _MixedTriggerProducer(Producer):
        name = "mixed_trigger_test"
        system_element_name = "SE-Mixed"

        @every("10s")
        async def tick(self) -> None:
            pass

        @on_constant("line1/operator/activeRecipeId")
        async def on_recipe_changed(self, constant) -> None:
            pass

        @on_signal("line1/mas2/sta1/aggos/grit")
        async def on_grit_binding_changed(self, signal) -> None:
            pass

    scheduler = AsyncIOScheduler()
    instance = _MixedTriggerProducer()

    with caplog.at_level(logging.WARNING, logger="chaski.dataops"):
        count = schedule_periodic(scheduler, instance)

    assert count == 1, "only the @every tick is this function's own job to schedule"
    assert [job.id for job in scheduler.get_jobs()] == ["mixed_trigger_test.tick::every(10.0s)"]
    assert "Unknown trigger spec" not in caplog.text


# ─── timestamp fallback: colca's record.ts is milliseconds ───────────────


def _metric_record(payload: dict, ts: float) -> Record:
    return Record(
        offset=1,
        origin_offset=1,
        topic="colca/v1/_Metric/n-1/line1/x",
        payload=payload,
        ts=ts,
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )


def test_decode_metric_falls_back_to_colca_record_ts_converted_to_seconds():
    """A payload without a timestamp gets record.ts converted from milliseconds
    to seconds."""
    record = _metric_record({"signal_id": "sig-1", "value": 1.0}, ts=1_700_000_000_000.0)

    metric = decode_metric(record)

    assert metric.timestamp == pytest.approx(1_700_000_000.0)


def test_decode_metric_prefers_the_payloads_own_timestamp():
    """A payload's own timestamp is kept unconverted."""
    record = _metric_record({"signal_id": "sig-1", "value": 1.0, "timestamp": 42.0}, ts=1_700_000_000_000.0)

    metric = decode_metric(record)

    assert metric.timestamp == 42.0


# ─── a failed pinned KV read must not drop already-resolved signals ────────


class FlakyKvDoor(FakeDoor):
    """Like FakeDoor (KV only), but can be told to refuse KV reads on
    demand — simulates a colca `/kv` 429 mid resolution pass."""

    def __init__(self, entries):
        super().__init__(entries)
        self.fail = False

    def kv(self, prefix="", *, contract=None):
        if self.fail:
            raise RuntimeError("429 Too Many Requests")
        return super().kv(prefix, contract=contract)


def test_a_failed_pinned_read_keeps_previously_resolved_ids_bound(buffer):
    """When the pass's KV read fails, inputs keep the ids they resolved before
    and the fetch filter does not narrow."""
    door = FlakyKvDoor([signal_entry("sig-event", "on_metric_signal")])
    runtime = FakeRuntime(door, buffer)

    instances = _instantiate(runtime, EventDrivenProducer)

    # The first pass resolves normally.
    _, signal_ids1, unresolved1 = build_dispatch(runtime, instances)
    assert signal_ids1 == ["sig-event"]
    assert unresolved1 == 0

    # KV starts refusing on the next pass's pinned read.
    door.fail = True
    dispatch2, signal_ids2, _ = build_dispatch(runtime, instances)

    assert signal_ids2 == ["sig-event"], "an already-resolved signal must not be dropped by a failed pass"
    assert list(dispatch2.keys()) == ["sig-event"]


# ─── handlers and ticks serialize on the producer's lock ─────────────────


def test_handler_and_tick_on_the_same_producer_serialize_on_its_lock():
    """A handler (through `make_handler`) and a tick (through `off_loop`) on the
    same producer never overlap. The producer overrides `__init__` without
    calling super(); the lock exists anyway."""

    class _LockedProducer(Producer):
        name = "locked_producer_test"
        system_element_name = "SE-Locked"

        def __init__(self):
            self.active = 0
            self.max_active = 0

        def _critical_section(self):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            time.sleep(0.1)
            self.active -= 1

        @on_metric("dummy_input")
        async def on_event(self, metric):
            self._critical_section()

        @every("10s")
        async def tick(self):
            self._critical_section()

    inst = _LockedProducer()
    handler = make_handler(inst.on_event)
    tick_job = off_loop(inst.tick)
    record = _metric_record({"signal_id": "s", "value": 1.0, "timestamp": 1.0}, ts=1000.0)

    barrier = threading.Barrier(2)

    def run_handler():
        barrier.wait()
        asyncio.run(handler(record))

    def run_tick():
        barrier.wait()
        tick_job()

    t1 = threading.Thread(target=run_handler)
    t2 = threading.Thread(target=run_tick)
    t1.start()
    t2.start()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    assert inst.max_active == 1, "handler and tick ran concurrently — the producer's lock did not serialize them"
