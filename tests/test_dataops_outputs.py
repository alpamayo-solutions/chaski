"""Tests for chaski.dataops.outputs: SignalOutput and AnnotationOutput.

A fake ``Door`` records publishes. Outputs are declared on a class and used
through an instance attached to a runtime, as a producer uses them.
"""

from __future__ import annotations

import json

import pytest
from colca_data_contracts import derive_annotation_id
from dataops_fakes import NODE_ID, FakeDoor, FakeRuntime, kv_entry

from chaski.dataops import outputs
from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.outputs import AnnotationOutput, SignalOutput, bind_annotation_outputs, build_catalogue


def _signal_entry(topic: str, *, signal_id: str, data_tag: str, is_published: bool = True):
    return kv_entry(topic, {"id": signal_id, "data_tag": data_tag, "is_published": is_published})


def _annotation_type_entry(name: str, type_id: str):
    return kv_entry(f"colca/v1/_AnnotationType/{NODE_ID}/{name}", {"id": type_id, "name": name})


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


def _producer(name: str, runtime, **outs) -> Producer:
    """A minimal, unregistered producer with each output as a class attribute,
    which is where build_catalogue and bind_annotation_outputs look."""
    cls = type(f"_TestProducer_{name}", (Producer,), {"name": name, **outs})
    return cls().attach(runtime)


def _bound_output(door, *, buffer=None, source="producer.computed", tag_id="tag-1") -> SignalOutput:
    producer = _producer("producer", FakeRuntime(door, buffer), computed=SignalOutput("computed", "float"))
    producer.computed.bind(source, tag_id)
    return producer.computed


# ─── SignalOutput.publish: bound / unbound ─────────────────────────────────


def test_unbound_output_publishes_nothing():
    door = FakeDoor()  # no _Signal entries at all
    out = _bound_output(door)

    out.publish(1.0)

    assert door.published == []


def test_unbound_output_logs_at_warning(caplog):
    door = FakeDoor()
    out = _bound_output(door)

    with caplog.at_level("WARNING", logger="chaski.dataops.outputs"):
        out.publish(1.0)

    assert any("computed" in r.message and "tag-1" in r.message for r in caplog.records)


def test_unbound_output_log_is_rate_limited(caplog, monkeypatch):
    door = FakeDoor()
    out = _bound_output(door)

    clock = [1000.0]
    monkeypatch.setattr(outputs.time, "time", lambda: clock[0])

    with caplog.at_level("WARNING", logger="chaski.dataops.outputs"):
        out.publish(1.0)
        clock[0] += 1.0  # well inside the 60s floor
        out.publish(1.0)

    assert len(caplog.records) == 1, "a second publish within the rate-limit window must not log again"

    caplog.clear()
    clock[0] += outputs._UNBOUND_LOG_INTERVAL_S + 1.0
    with caplog.at_level("WARNING", logger="chaski.dataops.outputs"):
        out.publish(1.0)
    assert len(caplog.records) == 1, "a publish past the rate-limit window must log again"


def test_binding_not_marked_is_published_counts_as_unbound():
    door = FakeDoor(
        [
            _signal_entry(
                "colca/v1/_Signal/n-1/line1/computed", signal_id="sig-1", data_tag="tag-1", is_published=False
            ),
        ]
    )
    out = _bound_output(door)

    out.publish(1.0)

    assert door.published == []


def test_bound_output_publishes_metric_at_the_bound_position():
    door = FakeDoor(
        [
            _signal_entry("colca/v1/_Signal/n-1/line1/computed", signal_id="sig-99", data_tag="tag-1"),
            # A binding for a DIFFERENT tag must not be picked up.
            _signal_entry("colca/v1/_Signal/n-1/line1/other", signal_id="sig-other", data_tag="tag-2"),
        ]
    )
    out = _bound_output(door)

    out.publish(3.14, timestamp=1000.0)

    assert len(door.published) == 1
    topic, payload_json = door.published[0]
    assert topic == "colca/v1/_Metric/n-1/line1/computed"
    payload = json.loads(payload_json)
    assert payload["signal_id"] == "sig-99"
    assert payload["value"] == 3.14
    assert payload["timestamp"] == 1000.0


def test_rebind_is_picked_up_on_the_next_resolution_pass():
    """A held binding is refreshed by `forget`, so an output does not keep
    writing to a retired signal.
    """
    door = FakeDoor(
        [
            _signal_entry("colca/v1/_Signal/n-1/line1/computed", signal_id="sig-old", data_tag="tag-1"),
        ]
    )
    out = _bound_output(door)
    out.publish(1.0)
    assert json.loads(door.published[-1][1])["signal_id"] == "sig-old"

    door.entries = [
        kv_entry("colca/v1/_Signal/n-1/line1/computed", {}),  # tombstoned
        _signal_entry("colca/v1/_Signal/n-1/line1/computed-2", signal_id="sig-new", data_tag="tag-1"),
    ]
    out.publish(2.0)
    assert json.loads(door.published[-1][1])["signal_id"] == "sig-old", (
        "a bound output re-read KV on a publish — that is the scan per value this replaced"
    )

    out.forget()
    out.publish(3.0)
    assert json.loads(door.published[-1][1])["signal_id"] == "sig-new"


def test_an_unbound_output_keeps_looking():
    """A miss is not held, because nothing tells the service when it is bound."""
    door = FakeDoor()  # no _Signal entries at all
    out = _bound_output(door)

    out.publish(1.0)
    assert door.published == []

    door.entries = [
        _signal_entry("colca/v1/_Signal/n-1/line1/computed", signal_id="sig-1", data_tag="tag-1"),
    ]
    out.publish(2.0)
    assert json.loads(door.published[-1][1])["signal_id"] == "sig-1"


# ─── build_catalogue ────────────────────────────────────────────────────────


def _catalogue_topic() -> str:
    return "colca/v1/_DataTags/n-1/dataops"


def _build(door, producer):
    return build_catalogue([producer], door, node_id=NODE_ID, mount="", service_name="dataops", service_ulid="svc-1")


def test_build_catalogue_mints_ids_and_publishes_when_none_retained():
    door = FakeDoor()
    producer = _producer("press", FakeRuntime(door, None), computed=SignalOutput("computed", "float"))

    result = _build(door, producer)

    assert len(door.published) == 1
    topic, payload_json = door.published[0]
    assert topic == _catalogue_topic()
    payload = json.loads(payload_json)
    assert len(payload["data_tags"]) == 1
    assert payload["data_tags"][0]["source"] == "press.computed"
    assert result == {"press.computed": payload["data_tags"][0]["id"]}
    # bind() actually ran, on the INSTANCE's copy:
    assert producer.computed.tag_id == result["press.computed"]
    with pytest.raises(RuntimeError):
        _ = type(producer).computed.tag_id


def test_build_catalogue_reuses_ids_and_skips_republish_across_a_simulated_restart():
    """Two independent builds with the same outputs, as after a restart, mint the
    same tag id, so the second build publishes nothing."""
    door1 = FakeDoor()
    producer1 = _producer("press", FakeRuntime(door1, None), computed=SignalOutput("computed", "float"))
    result1 = _build(door1, producer1)
    assert len(door1.published) == 1
    published_topic, published_json = door1.published[0]

    # A restart: a new Door whose KV holds what run 1 published, and new
    # producer instances.
    door2 = FakeDoor([kv_entry(published_topic, json.loads(published_json))])
    producer2 = _producer("press", FakeRuntime(door2, None), computed=SignalOutput("computed", "float"))
    result2 = _build(door2, producer2)

    assert result2 == result1, "the same declared output must reuse the same tag id across a restart"
    assert door2.published == [], "an unchanged catalogue must not be republished"


def test_build_catalogue_republishes_when_declared_outputs_actually_change():
    """Denominator for the skip-when-unchanged test above: prove the guard
    can also say yes."""
    door1 = FakeDoor()
    producer1 = _producer("press", FakeRuntime(door1, None), computed=SignalOutput("computed", "float"))
    _build(door1, producer1)
    published_topic, published_json = door1.published[0]

    door2 = FakeDoor([kv_entry(published_topic, json.loads(published_json))])
    producer2 = _producer(
        "press",
        FakeRuntime(door2, None),
        computed=SignalOutput("computed", "float"),
        extra=SignalOutput("extra", "int"),  # a genuinely new declared output
    )
    _build(door2, producer2)

    assert len(door2.published) == 1, "adding a declared output must republish the catalogue"


def test_build_catalogue_carries_a_removed_source_forward_as_stale():
    door1 = FakeDoor()
    producer1 = _producer("press", FakeRuntime(door1, None), a=SignalOutput("a", "float"), b=SignalOutput("b", "float"))
    result1 = _build(door1, producer1)
    published_topic, published_json = door1.published[0]

    # Run 2 only declares "a" — "b" is gone.
    door2 = FakeDoor([kv_entry(published_topic, json.loads(published_json))])
    producer2 = _producer("press", FakeRuntime(door2, None), a=SignalOutput("a", "float"))
    _build(door2, producer2)

    assert len(door2.published) == 1
    tags = json.loads(door2.published[0][1])["data_tags"]
    by_source = {t["source"]: t for t in tags}
    assert by_source["press.a"]["is_stale"] is False
    assert by_source["press.b"]["is_stale"] is True
    assert by_source["press.b"]["id"] == result1["press.b"], "a stale tag keeps its old id"


# ─── AnnotationOutput ───────────────────────────────────────────────────────


def _bound_annotation_output(door, buffer, *, name="downtime", producer_name="press") -> AnnotationOutput:
    producer = _producer(producer_name, FakeRuntime(door, buffer), downtime=AnnotationOutput(name))
    bind_annotation_outputs([producer], node_id=NODE_ID, mount="line1")
    return producer.downtime


def test_write_interval_derives_a_deterministic_id_and_publishes(buffer):
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    out = _bound_annotation_output(door, buffer)

    expected_id = derive_annotation_id("at-1", "dataops/press", 1000.0, [])
    got_id = out.write_interval(1000.0, 1010.0, value="jam")

    assert got_id == expected_id
    assert len(door.published) == 1
    topic, payload_json = door.published[0]
    assert topic == f"colca/v1/_Annotation/{NODE_ID}/line1/press/downtime/{expected_id}"
    payload = json.loads(payload_json)
    assert payload["annotation_id"] == expected_id
    assert payload["annotation_type_id"] == "at-1"
    assert payload["time_start"] == 1000.0
    assert payload["time_end"] == 1010.0
    assert payload["value"] == "jam"
    assert payload["source"] == "dataops/press"
    assert payload["deleted"] is False


def test_same_interval_republished_updates_in_place_not_duplicates(buffer):
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    out = _bound_annotation_output(door, buffer)

    id1 = out.write_interval(1000.0, value="jam")
    id2 = out.write_interval(1000.0, time_end=1020.0, value="jam")  # same start, now with an end

    assert id1 == id2
    assert len(door.published) == 2
    assert door.published[0][0] == door.published[1][0], "an update republishes at the SAME topic"


def test_id_and_topic_are_identical_across_a_simulated_restart(tmp_path):
    """Two independently bound AnnotationOutputs, as after a restart, derive the
    same id and topic for the same type, source and start."""
    db_path = tmp_path / "buffer.sqlite3"

    door1 = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    b1 = Buffer(db_path)
    try:
        out1 = _bound_annotation_output(door1, b1)
        id1 = out1.write_interval(1000.0, value="jam")
    finally:
        b1.close()
    topic1 = door1.published[0][0]

    # Fresh Door, fresh AnnotationOutput/Producer, buffer re-opened from the
    # same file — a new process, same declared identity.
    door2 = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    b2 = Buffer(db_path)
    try:
        out2 = _bound_annotation_output(door2, b2)
        id2 = out2.write_interval(1000.0, value="jam")
    finally:
        b2.close()
    topic2 = door2.published[0][0]

    assert id1 == id2
    assert topic1 == topic2


def test_clear_window_only_deletes_ids_this_output_itself_emitted(buffer):
    """clear_window only deletes ids this output recorded emitting."""
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    runtime = FakeRuntime(door, buffer)

    press_producer = _producer("press", runtime, downtime=AnnotationOutput("downtime"))
    other_producer = _producer("other_machine", runtime, downtime=AnnotationOutput("downtime"))

    bind_annotation_outputs([press_producer, other_producer], node_id=NODE_ID, mount="")
    press, other = press_producer.downtime, other_producer.downtime

    press_id = press.write_interval(1000.0, value="a")
    other_id = other.write_interval(1005.0, value="b")  # same window, different source
    door.published.clear()

    deleted = press.clear_window(0.0, 2000.0)

    assert deleted == 1
    assert len(door.published) == 1
    payload = json.loads(door.published[0][1])
    assert payload["annotation_id"] == press_id
    assert payload["deleted"] is True
    assert other_id not in [json.loads(p)["annotation_id"] for _t, p in door.published]


def test_clear_window_delete_marker_is_a_new_append_not_a_republish_of_the_create(buffer):
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    out = _bound_annotation_output(door, buffer)
    out.write_interval(1000.0, value="jam")
    door.published.clear()

    n = out.clear_window(0.0, 2000.0)

    assert n == 1
    payload = json.loads(door.published[0][1])
    assert payload["deleted"] is True
    assert payload["value"] is None  # a delete marker carries no value


def test_clear_window_outside_the_range_deletes_nothing(buffer):
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    out = _bound_annotation_output(door, buffer)
    out.write_interval(1000.0, value="jam")
    door.published.clear()

    n = out.clear_window(2000.0, 3000.0)

    assert n == 0
    assert door.published == []


def test_a_refused_scan_idles_the_publish_instead_of_killing_the_tick():
    """A 429 on the KV scan idles the value instead of raising; a 500 still raises."""
    import httpx

    class RefusingDoor(FakeDoor):
        def __init__(self, status: int) -> None:
            super().__init__([])
            self.status = status

        def kv(self, prefix="", *, contract=None):
            request = httpx.Request("GET", "http://colca/kv")
            raise httpx.HTTPStatusError(
                f"{self.status}",
                request=request,
                response=httpx.Response(self.status, request=request),
            )

    out = _bound_output(RefusingDoor(429), source="p.availability")
    out.publish(1.0)  # must not raise

    out500 = _bound_output(RefusingDoor(500), source="p.availability")
    try:
        out500.publish(1.0)
    except httpx.HTTPStatusError:
        pass
    else:
        raise AssertionError("a non-429 refusal must propagate, not idle")


def test_delete_removes_one_annotation_not_another_that_started_at_the_same_instant(buffer):
    """Two machines' annotations from one producer start at the same instant:
    clear_window over that instant would take out both, delete names one."""
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    out = _bound_annotation_output(door, buffer)
    press_a = out.write_interval(1000.0, value="jam", signal_ids=["sig-a"])
    press_b = out.write_interval(1000.0, value="jam", signal_ids=["sig-b"])
    assert press_a != press_b
    door.published.clear()

    deleted = out.delete(1000.0, signal_ids=["sig-a"])

    assert deleted == press_a
    assert len(door.published) == 1
    topic, payload_json = door.published[0]
    assert topic == f"colca/v1/_Annotation/{NODE_ID}/line1/press/downtime/{press_a}"
    payload = json.loads(payload_json)
    assert payload["annotation_id"] == press_a
    assert payload["deleted"] is True
    assert payload["signal_ids"] == ["sig-a"]


def test_delete_needs_no_buffer_record_of_the_write(tmp_path):
    """A producer that keeps no state deletes from the event alone: the id is
    derived, so an emptied buffer (a new generation) still names it."""
    door1 = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    b1 = Buffer(tmp_path / "first.sqlite3")
    try:
        written = _bound_annotation_output(door1, b1).write_interval(1000.0, value="jam", signal_ids=["sig-a"])
    finally:
        b1.close()

    door2 = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    b2 = Buffer(tmp_path / "second.sqlite3")
    try:
        out = _bound_annotation_output(door2, b2)
        assert out.clear_window(0.0, 2000.0) == 0, "the fresh buffer holds no record of the write"
        deleted = out.delete(1000.0, signal_ids=["sig-a"])
    finally:
        b2.close()

    assert deleted == written
    assert json.loads(door2.published[-1][1])["annotation_id"] == written


def test_an_annotation_is_updated_by_the_id_its_write_returned(buffer):
    """The case re-deriving cannot serve: an annotation whose signal set grew
    and whose start was corrected while it was open. The producer kept the id
    the create returned, and closes THAT annotation."""
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    out = _bound_annotation_output(door, buffer)

    opened = out.write_interval(1000.0, value="jam", signal_ids=["sig-a"])
    closed = out.write_interval(
        990.0,
        1200.0,
        value="jam",
        signal_ids=["sig-a", "sig-b"],
        annotation_id=opened,
    )

    assert closed == opened
    assert [t for t, _p in door.published] == [door.published[0][0]] * 2, "an update republishes at the SAME topic"
    payload = json.loads(door.published[-1][1])
    assert (payload["time_start"], payload["time_end"]) == (990.0, 1200.0)
    assert payload["signal_ids"] == ["sig-a", "sig-b"]
    assert derive_annotation_id("at-1", "dataops/press", 990.0, ["sig-a", "sig-b"]) != closed, (
        "the point of passing the id: re-deriving from the moved start and signals names another annotation"
    )


def test_delete_by_id_needs_no_start_time(buffer):
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    out = _bound_annotation_output(door, buffer)
    opened = out.write_interval(1000.0, value="jam", signal_ids=["sig-a"])
    door.published.clear()

    deleted = out.delete(annotation_id=opened)

    assert deleted == opened
    payload = json.loads(door.published[0][1])
    assert payload["annotation_id"] == opened
    assert payload["deleted"] is True


def test_delete_with_neither_a_start_nor_an_id_is_refused(buffer):
    door = FakeDoor([_annotation_type_entry("downtime", "at-1")])
    out = _bound_annotation_output(door, buffer)

    with pytest.raises(TypeError, match="time_start, or the annotation_id"):
        out.delete()
