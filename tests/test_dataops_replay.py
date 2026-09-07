"""Unit tests for chaski.dataops.service.replay_changed_producers —
hash-triggered broker-window replay (design §10, requirement #5).

No live colca: a fake Door (KV only) resolves declared inputs, same
pattern as test_dataops_service.py. A real Buffer over a tmp SQLite file
provides watermark/code_hash persistence and the buffered points replay
reads from.
"""

from __future__ import annotations

import pytest
from dataops_fakes import FakeDoor, FakeRuntime, run_async, signal_entry

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
    return FakeRuntime(FakeDoor([
        signal_entry("sig-event", "on_metric_signal"),
        signal_entry("sig-tick", "tick_only_signal"),
    ]), buffer)


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
    """Declares an input but has no @on_metric handler at all — replay has
    nothing to dispatch for it, but its hash/watermark bookkeeping must
    still work uniformly (design: no special-casing by trigger shape)."""

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
    """Denominator for 'exactly once': it means once PER CHANGE, not a
    permanent lockout. Simulates a second genuine code change by monkey-
    patching compute_code_hash to return a fresh value on the second call."""
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


# ─── a producer error must be as survivable on replay as it is live ─────────
#
# Ingest._process_record wraps every @on_metric handler call in its own
# try/except so one broken handler never takes the ingest loop down. Before
# this guard existed here, the SAME error was fatal on replay, and — because
# it happened before set_watermark — the hash was never persisted, so the
# next restart replayed the identical window and crashed again. Unbounded
# crash-loop.


class FailingProducer(Producer):
    """An @on_metric handler that always raises — the shape that crash-
    looped the service on replay."""

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
async def test_a_handler_that_raises_on_replay_does_not_crash_the_service(buffer, runtime):
    """Replay must survive a producer error exactly like live traffic does —
    no exception escapes replay_changed_producers."""
    buffer.append("sig-event", 100.0, "a")
    instance = FailingProducer().attach(runtime)

    await replay_changed_producers(runtime, [instance])  # must not raise

    assert instance.calls == 1, "the handler must still have been invoked (and its failure survived)"


@run_async
async def test_a_failing_producer_still_gets_its_watermark_and_hash_persisted(buffer, runtime):
    """The rule for the watermark/hash: persisted exactly once per replay
    pass regardless of handler failures — same as a live page always being
    acked regardless of a handler failure. Otherwise a producer that fails
    on EVERY record would replay (and crash-loop) forever, since the hash
    triggering replay would never get stored."""
    from chaski.dataops import codehash

    buffer.append("sig-event", 100.0, "a")
    instance = FailingProducer().attach(runtime)

    await replay_changed_producers(runtime, [instance])

    assert buffer.code_hash("failing") == codehash.compute_code_hash(FailingProducer)
    assert buffer.watermark("failing") == 100.0

    # Denominator half of "not forever": with the hash now stored, a second
    # startup with the SAME (still-failing) code must not replay again —
    # exactly the same one-time-per-change contract a healthy producer gets.
    instance.calls = 0
    await replay_changed_producers(runtime, [instance])
    assert instance.calls == 0, "an unchanged hash must not replay a failing producer again either"


@run_async
async def test_a_failing_producer_does_not_block_a_later_producer_in_the_same_pass(buffer, runtime):
    """An unhandled exception from one producer's handler must not abort
    bookkeeping for every producer after it in the same startup pass —
    denominator: a healthy producer later in the list still replays and
    advances."""
    buffer.append("sig-event", 100.0, "a")
    failing = FailingProducer().attach(runtime)
    healthy = RecordingProducer().attach(runtime)

    await replay_changed_producers(runtime, [failing, healthy])

    assert healthy.received == [(100.0, "a")], "a later healthy producer must still replay"
    assert buffer.watermark("recording") == 100.0, "a later healthy producer must still advance its watermark"
