"""The live resolution index: one KV read, then kept current by the node's
retained ``_SystemElement``, ``_Signal`` and ``_AnnotationType`` records.

Covered: lookups read no KV once it is live, MQTT updates (decoded, raw,
tombstones) move it, a record written during the seed read is not lost, a
dropped broker link falls back to KV reads until the next seed, path inputs,
the annotation type id kept on the output, and re-resolving on a change
instead of a timer.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from colca_data_contracts.payload import Signal
from dataops_fakes import NODE_ID, FakeDoor, FakeRuntime, kv_entry, run_async

from chaski.dataops import resolve
from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.outputs import AnnotationOutput, bind_annotation_outputs

SIGNAL = f"colca/v1/_Signal/{NODE_ID}"


class CountingDoor(FakeDoor):
    """Counts the reads that ask for the index contracts."""

    def __init__(self, entries=None) -> None:
        super().__init__(entries)
        self.index_reads = 0
        self.during_read = None

    def kv(self, prefix: str = "", *, contract=None):
        if contract is not None and "_Signal" in contract:
            self.index_reads += 1
        entries = super().kv(prefix, contract=contract)
        if self.during_read is not None:
            hook, self.during_read = self.during_read, None
            hook()
        return entries


@pytest.fixture
def door():
    return CountingDoor(
        [
            kv_entry(f"colca/v1/_SystemElement/{NODE_ID}/line1", {"id": "el-1", "name": "line1"}),
            kv_entry(f"{SIGNAL}/line1/speed", {"id": "sig-speed", "name": "speed", "system_element_id": "el-1"}),
        ]
    )


@pytest.fixture
def live(door):
    index = resolve.LiveIndex(door)
    index.seed()
    resolve.attach(door, index)
    try:
        yield index
    finally:
        resolve.attach(door, None)


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


def test_a_live_index_answers_every_lookup_without_reading_kv(door, live):
    assert door.index_reads == 1  # the seed

    for _ in range(50):
        assert resolve.resolve_signal(door, "speed", "line1") == "sig-speed"
        with resolve.one_pass(door):
            assert resolve.resolve_system_element(door, "line1") == "el-1"
        with resolve.lazy_pass(resolve.Snapshot(door)):
            assert resolve.resolve_signal_path(door, "line1/speed") == "sig-speed"
    assert resolve.resolve_metric_topics(door, ["sig-speed"]) == {
        "sig-speed": f"colca/v1/_Metric/{NODE_ID}/line1/speed"
    }

    assert door.index_reads == 1
    assert door.kv_calls == 1


def test_a_record_over_mqtt_moves_the_index(door, live):
    changes = []
    live.add_listener(lambda: changes.append(1))

    # Commissioned: a decoded contract, as franzmq hands it over.
    live.observe(f"{SIGNAL}/line1/temp", Signal(id="sig-temp", name="temp", system_element_id="el-1"))
    assert resolve.resolve_signal(door, "temp", "line1") == "sig-temp"

    # Rebound to a new id, as raw bytes (franzmq could not decode it).
    live.observe(f"{SIGNAL}/line1/temp", b'{"id": "sig-temp-2", "name": "temp", "system_element_id": "el-1"}')
    assert resolve.resolve_signal_path(door, "line1/temp") == "sig-temp-2"

    # Retired.
    live.observe(f"{SIGNAL}/line1/temp", None)
    assert resolve.resolve_signal(door, "temp") is None

    assert len(changes) == 3
    assert door.index_reads == 1


def test_a_retained_record_delivered_again_is_not_a_change(live):
    changes = []
    live.add_listener(lambda: changes.append(1))

    # The retained replay after a subscribe: same fields, decoded with defaults.
    live.observe(f"{SIGNAL}/line1/speed", Signal(id="sig-speed", name="speed", system_element_id="el-1"))
    live.observe(f"colca/v1/_Constant/{NODE_ID}/line1/setpoint", b'{"value": 1}')  # not an index contract

    assert changes == []


def test_a_record_written_during_the_seed_read_is_not_lost(door):
    index = resolve.LiveIndex(door)
    # The KV read returns the old state; the write reaches MQTT meanwhile.
    door.during_read = lambda: index.observe(f"{SIGNAL}/line1/speed", Signal(id="sig-new", name="speed"))

    index.seed()
    resolve.attach(door, index)
    try:
        assert resolve.resolve_signal(door, "speed") == "sig-new"
    finally:
        resolve.attach(door, None)


def test_a_dropped_link_reads_kv_until_the_next_seed(door, live):
    live.suspend()
    live.observe(f"{SIGNAL}/line1/temp", Signal(id="sig-temp", name="temp"))  # held for the seed

    assert live.current() is None
    assert resolve.resolve_signal(door, "speed") == "sig-speed"
    assert door.index_reads == 2  # read, as without an index

    live.seed()
    assert resolve.resolve_signal(door, "temp") == "sig-temp"
    assert resolve.resolve_signal(door, "speed") == "sig-speed"
    assert door.index_reads == 3


def test_without_an_index_a_pass_reads_only_the_index_contracts(door):
    with resolve.one_pass(door):
        resolve.resolve_signal(door, "speed")
    assert door.index_reads == 1


# ─── path inputs ─────────────────────────────────────────────────────────────


def _attached(runtime, **attrs):
    cls = type("_PathProducer", (Producer,), {"name": "path_producer", **attrs})
    return cls().attach(runtime)


def test_a_path_input_resolves_the_signal_at_that_position(door, live, buffer):
    producer = _attached(FakeRuntime(door, buffer), speed=SignalRangeInput(path="/line1/speed"))

    assert producer.speed.signal_id == "sig-speed"
    assert producer.speed.signal_name == "line1/speed"


def test_a_path_input_that_does_not_resolve_names_the_path(door, live, buffer):
    producer = _attached(FakeRuntime(door, buffer), speed=SignalRangeInput(path="line1/missing"))

    with pytest.raises(LookupError, match="line1/missing"):
        _ = producer.speed.signal_id


def test_a_path_input_takes_a_path_or_a_name_not_both():
    with pytest.raises(ValueError, match="not both"):
        SignalRangeInput("speed", path="line1/speed")
    with pytest.raises(ValueError, match="needs a signal name or a path"):
        SignalRangeInput()


# ─── the annotation type id ──────────────────────────────────────────────────


def test_an_annotation_output_keeps_its_type_id_until_the_next_pass(buffer):
    door = CountingDoor([kv_entry(f"colca/v1/_AnnotationType/{NODE_ID}/downtime", {"id": "at-1", "name": "downtime"})])
    producer = _attached(FakeRuntime(door, buffer), downtime=AnnotationOutput("downtime"))
    bind_annotation_outputs([producer], node_id=NODE_ID, mount="line1")

    for start in range(5):
        producer.downtime.write_interval(float(start), float(start) + 1)
    assert door.index_reads == 1

    door.entries = [kv_entry(f"colca/v1/_AnnotationType/{NODE_ID}/downtime", {"id": "at-2", "name": "downtime"})]
    producer.downtime.forget()
    assert producer.downtime.type_id == "at-2"


# ─── re-resolving on a change ───────────────────────────────────────────────


@run_async
async def test_a_change_resolves_again_and_rebinds_only_what_changed():
    import chaski.dataops.service as service_module

    class _Ingest:
        def __init__(self):
            self.bound = []

        def rebind(self, dispatch, signal_ids):
            self.bound.append(list(signal_ids or []))

    passes = iter(
        [
            ({"s1": ["h"]}, ["s1"], 0),  # nothing that matters changed
            ({"s1": ["h"], "s2": ["h"]}, ["s1", "s2"], 0),  # a new signal resolved
        ]
    )
    ingest, stop, changed, started = _Ingest(), asyncio.Event(), asyncio.Event(), []
    bound = service_module._bound_shape({"s1": ["h"]}, ["s1"])

    with patch.object(service_module, "build_dispatch", lambda runtime, instances: next(passes)):
        task = asyncio.ensure_future(
            service_module.follow_index(
                None, [], ingest, stop, lambda: started.append(1), changed, bound, settle_s=0.01
            )
        )
        changed.set()
        await asyncio.sleep(0.1)
        assert ingest.bound == []
        changed.set()
        await asyncio.sleep(0.1)
        stop.set()
        await asyncio.wait_for(task, timeout=2)

    assert ingest.bound == [["s1", "s2"]]
    assert started == [1]
