"""Resolution shares the ordered retained view, including recovery and tombstones."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from dataops_fakes import NODE_ID, FakeDoor, FakeRuntime, kv_entry

from chaski.dataops import resolve
from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.outputs import AnnotationOutput, bind_annotation_outputs
from chaski.door import Page
from chaski.retained_view import RetainedView, ViewScope

SIGNAL = f"colca/v1/_Signal/{NODE_ID}"


class CountingDoor(FakeDoor):
    def __init__(self, entries=None):
        super().__init__(entries)
        self.index_reads = 0
        self.during_read = None
        self.records = {"entities": [], "definitions": []}
        self.positions = {"entities": 0, "definitions": 0}
        for entry in list(self.entries):
            self.put(entry.topic, entry.payload)

    def put(self, topic, payload):
        stream = "definitions" if "/_AnnotationType/" in topic else "entities"
        offset = len(self.records[stream]) + 1
        self.records[stream].append(SimpleNamespace(topic=topic, payload=payload, offset=offset, ts=0))
        self.entries = [e for e in self.entries if e.topic != topic]
        if payload is not None:
            self.entries.append(replace(kv_entry(topic, payload), offset=offset))

    def kv(self, prefix="", *, contract=None, depth=None):
        self.index_reads += 1
        entries = super().kv(prefix, contract=contract)
        if self.during_read:
            hook, self.during_read = self.during_read, None
            hook()
        return entries

    def fetch(self, stream, cursor, *, max=1000, tail=False, contracts=None, topics=None):
        rows = self.records[stream]
        if tail:
            return Page([], len(rows) + 1)
        start = self.positions[stream]
        page = rows[start : start + max]
        return Page(page, start + len(page) + 1, start=start + 1)

    def ack(self, stream, cursor, offset):
        self.positions[stream] = offset
        return True


@pytest.fixture
def door():
    return CountingDoor(
        [
            kv_entry(f"colca/v1/_SystemElement/{NODE_ID}/line1", {"id": "el-1", "name": "line1"}),
            kv_entry(f"{SIGNAL}/line1/speed", {"id": "sig-speed", "name": "speed", "system_element_id": "el-1"}),
        ]
    )


def index_for(door):
    return resolve.LiveIndex(
        RetainedView(door, resolve.INDEX_CONTRACTS, ("entities", "definitions"), "index", scope=ViewScope.whole_node())
    )


@pytest.fixture
def live(door):
    index = index_for(door)
    index.seed()
    resolve.attach(door, index)
    yield index
    resolve.attach(door, None)


@pytest.fixture
def buffer(tmp_path):
    with Buffer(tmp_path / "buffer.sqlite3") as value:
        yield value


def test_a_live_index_answers_every_lookup_without_reading_kv(door, live):
    for _ in range(50):
        with resolve.one_pass(door):
            assert resolve.resolve_signal(door, "speed", "line1") == "sig-speed"
            assert resolve.resolve_signal_path(door, "line1/speed") == "sig-speed"
    assert door.index_reads == 1


def test_durable_changes_and_tombstones_update_all_resolvers(door, live):
    changes = []
    live.add_listener(lambda: changes.append(1))
    topic = f"{SIGNAL}/line1/temp"
    for identifier in ("sig-temp", "sig-temp-2", None):
        door.put(topic, {"id": identifier, "name": "temp"} if identifier else None)
        live.view.synchronize()
        assert resolve.resolve_signal(door, "temp") == identifier
    assert len(changes) == 3
    assert door.index_reads == 1


def test_unversioned_mqtt_hint_cannot_overwrite_newer_durable_state(door, live):
    live.observe(f"{SIGNAL}/line1/speed", {"id": "old-id", "name": "speed"})
    live.view.synchronize()
    assert resolve.resolve_signal(door, "speed") == "sig-speed"
    assert door.index_reads == 1


def test_a_write_during_hydration_is_not_lost(door):
    index = index_for(door)
    door.during_read = lambda: door.put(f"{SIGNAL}/line1/speed", {"id": "new-id", "name": "speed"})
    index.seed()
    assert index.index().signal_id_by_path["line1/speed"] == "new-id"


def test_a_dropped_link_blocks_lookups_without_http_fallback(door, live):
    live.suspend()
    with pytest.raises(RuntimeError, match="unavailable"):
        resolve.resolve_signal(door, "speed")
    assert door.index_reads == 1
    door.put(f"{SIGNAL}/line1/temp", {"id": "sig-temp", "name": "temp"})
    live.seed()
    assert resolve.resolve_signal(door, "temp") == "sig-temp"
    assert door.index_reads == 1


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
