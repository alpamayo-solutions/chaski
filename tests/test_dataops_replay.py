"""Tests for chaski.dataops.service.replay_changed_producers.

A fake Door (KV only) resolves inputs; a real Buffer on a temporary SQLite file
holds watermarks, code hashes and the points replay reads.
"""

from __future__ import annotations

import pytest
from dataops_fakes import FakeDoor, FakeRuntime, run_async, signal_entry

from chaski import Reject
from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.service import replay_changed_producers
from chaski.dataops.triggers import every, on_metric


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


@pytest.fixture
def runtime(buffer):
    return FakeRuntime(
        FakeDoor(
            [
                signal_entry("sig-event", "on_metric_signal"),
                signal_entry("sig-tick", "tick_only_signal"),
            ]
        ),
        buffer,
    )


@pytest.fixture(autouse=True)
def _isolate_registry():
    """Never leak the test-only Producer registrations into another test."""
    saved_registry = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved_registry)


# ─── producers under test ───────────────────────────────────────────────────


class RecordingProducer(Producer):
    """An @on_metric-driven producer that records every (timestamp, value)
    it is handed, so replay can be verified by inspecting what actually
    reached the handler — not just that the watermark moved."""

    name = "recording"
    system_element_name = "SE-1"

    event_input = SignalRangeInput("on_metric_signal", window="1h")

    def __init__(self) -> None:
        super().__init__()
        self.received: list[tuple[float, object]] = []

    @on_metric("event_input")
    async def on_event(self, metric) -> None:
        self.received.append((metric.timestamp, metric.value))


class TickOnlyProducer(Producer):
    """Declares an input but no @on_metric handler: nothing to replay, but its
    hash and watermark are still kept."""

    name = "tick_only"
    system_element_name = "SE-2"

    tick_input = SignalRangeInput("tick_only_signal", window="1h")

    @every("30s")
    async def tick(self) -> None:
        pass


# ─── first run: no prior hash recorded ─────────────────────────────────────


@run_async
async def test_first_run_replays_every_buffered_point_in_order(buffer, runtime):
    buffer.append("sig-event", 100.0, "a")
    buffer.append("sig-event", 300.0, "c")
    buffer.append("sig-event", 200.0, "b")  # inserted out of ts order on purpose

    instance = RecordingProducer().attach(runtime)
    await replay_changed_producers(runtime, [instance])

    assert instance.received == [(100.0, "a"), (200.0, "b"), (300.0, "c")]


@run_async
async def test_first_run_persists_the_current_hash_and_advances_watermark(buffer, runtime):
    from chaski.dataops import codehash

    buffer.append("sig-event", 100.0, "a")
    instance = RecordingProducer().attach(runtime)

    await replay_changed_producers(runtime, [instance])

    assert buffer.code_hash("recording") == codehash.compute_code_hash(RecordingProducer)
    assert buffer.watermark("recording") == 100.0


# ─── the exactly-once contract ──────────────────────────────────────────────


@run_async
async def test_unchanged_hash_does_not_reset_or_replay_again(buffer, runtime):
    """The denominator: a second startup with the SAME code must not fire
    the handler again, and must not move the watermark."""
    buffer.append("sig-event", 100.0, "a")
    instance = RecordingProducer().attach(runtime)

    await replay_changed_producers(runtime, [instance])
    assert len(instance.received) == 1
    watermark_after_first = buffer.watermark("recording")

    # A brand new point arrives in the buffer between the two calls — if
    # the second call replayed again it WOULD see it (proving this isn't
    # green only because there's nothing left to replay).
    buffer.append("sig-event", 200.0, "b")
    instance.received.clear()

    await replay_changed_producers(runtime, [instance])

    assert instance.received == [], "an unchanged hash must not replay again"
    assert buffer.watermark("recording") == watermark_after_first, "an unchanged hash must not reset the watermark"


@run_async
async def test_a_second_real_code_change_replays_again(buffer, runtime, monkeypatch):
    """Replay happens once per change: a second code change, simulated by
    patching compute_code_hash, replays again."""
    from chaski.dataops import service

    buffer.append("sig-event", 100.0, "a")
    instance = RecordingProducer().attach(runtime)
    await replay_changed_producers(runtime, [instance])
    assert len(instance.received) == 1

    buffer.append("sig-event", 200.0, "b")
    instance.received.clear()

    monkeypatch.setattr(service.codehash, "compute_code_hash", lambda cls: "a-genuinely-different-hash")
    await replay_changed_producers(runtime, [instance])

    assert instance.received == [(100.0, "a"), (200.0, "b")], "a genuine second change must replay again"


# ─── tick-only producers: no dispatch, same bookkeeping ────────────────────


@run_async
async def test_tick_only_producer_gets_hash_persisted_with_no_dispatch(buffer, runtime):
    from chaski.dataops import codehash

    instance = TickOnlyProducer().attach(runtime)
    await replay_changed_producers(runtime, [instance])

    assert buffer.code_hash("tick_only") == codehash.compute_code_hash(TickOnlyProducer)


@run_async
async def test_tick_only_producer_is_untouched_on_the_second_call(buffer, runtime):
    instance = TickOnlyProducer().attach(runtime)
    await replay_changed_producers(runtime, [instance])
    hash_after_first = buffer.code_hash("tick_only")
    watermark_after_first = buffer.watermark("tick_only")

    await replay_changed_producers(runtime, [instance])

    assert buffer.code_hash("tick_only") == hash_after_first
    assert buffer.watermark("tick_only") == watermark_after_first


# ─── a producer error is survived on replay, as it is live ─────────────────


class FailingProducer(Producer):
    """An @on_metric handler that always raises."""

    name = "failing"

    event_input = SignalRangeInput("on_metric_signal", window="1h")

    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    @on_metric("event_input")
    async def on_event(self, metric) -> None:
        self.calls += 1
        raise RuntimeError("boom")


@run_async
async def test_failed_replay_is_not_marked_complete_and_retries(buffer, runtime):
    buffer.append("sig-event", 100.0, "a")
    instance = FailingProducer().attach(runtime)
    for attempt in range(2):
        with pytest.raises(RuntimeError, match="boom"):
            await replay_changed_producers(runtime, [instance])
        assert buffer.code_hash("failing") is None
        assert buffer.watermark("failing") is None
        assert instance.calls == attempt + 1


@run_async
async def test_coordinated_replay_stops_before_pending_inputs(buffer, runtime):
    from types import SimpleNamespace

    from chaski.dataops.service import synthetic_record

    runtime.step = SimpleNamespace()
    for ts, value in [(100, "committed"), (200, "pending"), (300, "later pending")]:
        buffer.append("sig-event", ts, value)
    from dataclasses import replace

    records = [
        replace(synthetic_record("sig-event", ts, value), offset=i)
        for i, ts, value in [(1, 200, "pending"), (2, 300, "later pending")]
    ]
    buffer.queue_inputs(records, 2)
    instance = RecordingProducer().attach(runtime)
    await replay_changed_producers(runtime, [instance])
    assert instance.received == [(100, "committed")]
    assert len(buffer.input_batch(300)) == 2


class CheckpointProducer(RecordingProducer):
    name = "checkpointed"
    state_version = 1

    def snapshot_state(self):
        return self.received

    def restore_state(self, state):
        self.received = [tuple(item) for item in state]


@run_async
async def test_restart_restores_state_and_redelivered_page_does_not_repeat_effects(buffer, runtime):
    from dataclasses import replace

    from chaski.dataops.service import make_handler, synthetic_record

    first = CheckpointProducer().attach(runtime)
    await replay_changed_producers(runtime, [first])
    record = replace(synthetic_record("sig-event", 100, "opened"), offset=12)
    await make_handler(first.on_event)(record)
    second = CheckpointProducer().attach(runtime)
    await replay_changed_producers(runtime, [second])
    assert second.received == [(100, "opened")]
    await make_handler(second.on_event)(record)
    assert second.received == [(100, "opened")]
    await make_handler(second.on_event)(
        replace(record, offset=13, payload={"signal_id": "sig-event", "timestamp": 200, "value": "closed"})
    )
    assert second.received == [(100, "opened"), (200, "closed")]


@run_async
async def test_checkpoint_failure_restores_memory_and_preserves_retry(buffer, runtime, monkeypatch):
    from dataclasses import replace

    from chaski.dataops.service import make_handler, synthetic_record

    instance = CheckpointProducer().attach(runtime)
    await replay_changed_producers(runtime, [instance])
    save = buffer.save_checkpoint

    def broken(*args):
        raise OSError("disk full")

    monkeypatch.setattr(buffer, "save_checkpoint", broken)
    record = replace(synthetic_record("sig-event", 100, "opened"), offset=12)
    with pytest.raises(OSError, match="disk full"):
        await make_handler(instance.on_event)(record)
    assert instance.received == []
    monkeypatch.setattr(buffer, "save_checkpoint", save)
    await make_handler(instance.on_event)(record)
    assert instance.received == [(100, "opened")]


class OwnCheckpointProducer(CheckpointProducer):
    """Keeps a checkpoint of its own under names chaski once used itself."""

    name = "own-checkpoint"

    def __init__(self) -> None:
        super().__init__()
        self.own_calls: list[str] = []

    def _save_checkpoint(self):
        self.own_calls.append("save")
        return self._write()

    async def _write(self) -> None:
        pass

    def _restore_checkpoint(self):
        self.own_calls.append("restore")

    def _state_copy(self):
        self.own_calls.append("copy")


@run_async
async def test_a_producer_method_named_like_chaskis_checkpoint_is_not_called(buffer, runtime):
    """A producer's own async ``_save_checkpoint`` was called without await
    after a replay, and chaski's checkpoint was not saved."""
    from dataclasses import replace

    from chaski.dataops.service import make_handler, synthetic_record

    buffer.append("sig-event", 100.0, "a")
    first = OwnCheckpointProducer().attach(runtime)
    await replay_changed_producers(runtime, [first])
    assert buffer.checkpoint(first.name)[2] == [[100.0, "a"]]
    await make_handler(first.on_event)(replace(synthetic_record("sig-event", 200, "b"), offset=5))
    assert buffer.checkpoint(first.name)[2] == [[100.0, "a"], [200, "b"]]

    second = OwnCheckpointProducer().attach(runtime)
    await replay_changed_producers(runtime, [second])
    assert second.received == [(100.0, "a"), (200, "b")]
    assert first.own_calls == second.own_calls == []


def test_checkpoint_requires_both_state_hooks():
    class Incomplete(Producer):
        state_version = 1

    with pytest.raises(TypeError, match="requires snapshot_state"):
        Incomplete()


# ─── a handler that rejects or fails during replay ─────────────────────────


class RejectingProducer(Producer):
    """Sets negative values aside on purpose; fails on ``None``."""

    name = "rejecting"
    system_element_name = "SE-1"

    event_input = SignalRangeInput("on_metric_signal", window="1h")

    def __init__(self) -> None:
        super().__init__()
        self.received: list[tuple[float, object]] = []

    @on_metric("event_input")
    async def on_event(self, metric) -> None:
        if metric.value is None:
            raise RuntimeError("store unavailable")
        if metric.value < 0:
            raise Reject("negative", detail={"value": metric.value})
        self.received.append((metric.timestamp, metric.value))


class RecordingRuntime(FakeRuntime):
    def __init__(self, door, buffer) -> None:
        super().__init__(door, buffer)
        self.rejected: list[tuple[str, dict, str]] = []

    def reject(self, consumer: str, subject: dict, rejected: Reject) -> None:
        self.rejected.append((consumer, subject, rejected.reason))


@run_async
async def test_a_rejected_record_is_recorded_and_the_replay_completes(buffer, runtime):
    from chaski.dataops import codehash

    runtime = RecordingRuntime(runtime.door, buffer)
    buffer.set_watermark("rejecting", 0.0, "old-hash")
    buffer.append("sig-event", 100.0, 1)
    buffer.append("sig-event", 200.0, -1)
    buffer.append("sig-event", 300.0, 3)

    instance = RejectingProducer().attach(runtime)
    await replay_changed_producers(runtime, [instance])

    assert instance.received == [(100.0, 1), (300.0, 3)]
    assert runtime.rejected == [
        ("rejecting.on_event", {"replay": "rejecting", "signal_id": "sig-event", "ts": 200.0}, "negative")
    ]
    assert buffer.code_hash("rejecting") == codehash.compute_code_hash(RejectingProducer)
    assert buffer.watermark("rejecting") == 300.0


@run_async
async def test_a_rejection_that_cannot_be_recorded_stops_the_replay(buffer, runtime):
    buffer.set_watermark("rejecting", 0.0, "old-hash")
    buffer.append("sig-event", 100.0, -1)

    instance = RejectingProducer().attach(runtime)
    with pytest.raises(RuntimeError, match="nowhere to record"):
        await replay_changed_producers(runtime, [instance])

    assert buffer.code_hash("rejecting") == "old-hash"


@run_async
async def test_a_failed_replay_leaves_the_code_hash_unwritten(buffer, runtime):
    runtime = RecordingRuntime(runtime.door, buffer)
    buffer.set_watermark("rejecting", 0.0, "old-hash")
    buffer.append("sig-event", 100.0, 1)
    buffer.append("sig-event", 200.0, None)

    instance = RejectingProducer().attach(runtime)
    with pytest.raises(RuntimeError, match="store unavailable"):
        await replay_changed_producers(runtime, [instance])

    assert buffer.code_hash("rejecting") == "old-hash"
    assert buffer.watermark("rejecting") == 0.0
