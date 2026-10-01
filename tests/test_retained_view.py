import threading
from types import SimpleNamespace

import pytest

from chaski.door import Gap, KvEntry, Page
from chaski.retained_view import RetainedView, ViewScope

TOPIC = "colca/v1/_Metric/node/temperature"


class Door:
    def __init__(self):
        self.records = []
        self.position = 0
        self.snapshots = 0
        self.during_snapshot = lambda: None
        self.fail = False
        self.gap = False

    def put(self, value):
        self.records.append(SimpleNamespace(topic=TOPIC, payload=value, offset=len(self.records) + 1, ts=0))

    def fetch(self, stream, cursor, *, max, tail=False, contracts=None, topics=None):
        if self.fail:
            raise ConnectionError("disconnected")
        if tail:
            return Page(self.records[-1:], len(self.records) + 1)
        if self.gap:
            self.gap = False
            return Page([], len(self.records) + 1, Gap(stream, 0, len(self.records), None, None, False))
        rows = self.records[self.position : self.position + max]
        return Page(rows, (rows[-1].offset + 1) if rows else self.position + 1)

    def ack(self, stream, cursor, offset):
        self.position = offset
        return True

    def kv(self, prefix, *, contract, depth=None):
        self.snapshots += 1
        self.during_snapshot()
        if not self.records or self.records[-1].payload is None:
            return []
        r = self.records[-1]
        return [KvEntry("temperature", "node", TOPIC, r.payload, 0, r.offset)]


def view(door):
    return RetainedView(door, ["_Metric"], ["metrics"], "c/worker/view", scope=ViewScope.whole_node())


def test_snapshot_race_and_incremental_tombstone_do_not_resurrect_older_values():
    door = Door()
    door.put({"value": 1})
    door.during_snapshot = lambda: (door.put({"value": 2}), door.put({"value": 3}))
    current = view(door)
    assert current.synchronize()[1][0].payload == {"value": 3}
    assert door.snapshots == 1
    door.put(None)
    assert current.synchronize()[1] == []
    door.put({"value": 4})
    assert current.synchronize()[1][0].payload == {"value": 4}
    assert door.snapshots == 1


def test_disconnect_refuses_cached_state_and_gap_rebuilds_from_snapshot():
    door = Door()
    door.put({"value": 1})
    current = view(door)
    assert current.synchronize()[1][0].payload["value"] == 1
    door.fail = True
    with pytest.raises(ConnectionError):
        current.synchronize()[1]
    assert not current.available
    door.fail = False
    door.put({"value": 2})
    assert current.synchronize()[1][0].payload["value"] == 2
    door.put(None)
    door.gap = True
    assert current.synchronize()[1] == []
    assert door.snapshots == 2
    door.put({"value": 3})
    assert view(door).read()[0].payload["value"] == 3


def test_returned_view_cannot_modify_retained_state():
    door = Door()
    door.put({"value": 1})
    current = view(door)
    current.synchronize()[1][0].payload["value"] = 99
    assert current.synchronize()[1][0].payload["value"] == 1


def test_cached_reads_do_not_fetch_and_disconnect_refuses_them():
    door = Door()
    door.put({"value": 1})
    current = view(door)
    current.synchronize()
    door.fail = True  # Any accidental fetch would fail this test.
    for _ in range(100):
        assert current.read()[0].payload == {"value": 1}
    current._unavailable()
    with pytest.raises(RuntimeError, match="unavailable"):
        current.read()
    door.fail = False
    door.put({"value": 2})
    current.synchronize()
    assert current.read()[0].payload == {"value": 2}


def test_metric_hint_does_not_read_unchanged_definition_streams():
    class MultiDoor(Door):
        def __init__(self):
            super().__init__()
            self.fetches = []

        def fetch(self, stream, *args, **kwargs):
            self.fetches.append(stream)
            return Page([], 1)

    door = MultiDoor()
    current = RetainedView(
        door, ["_Metric", "_Signal"], ["metrics", "entities"], "c/worker/view", scope=ViewScope.whole_node()
    )
    current._refresh_changed()
    door.fetches.clear()
    current.watch["metrics"].notify()
    current._refresh_changed()
    assert set(door.fetches) == {"metrics"}
    door.fetches.clear()
    current._refresh_changed()
    assert door.fetches == []


def test_a_view_at_the_head_still_fetches_with_its_contracts():
    """The node counts a cursor's unread records by its last fetch filter; a
    tail read or an ack does not replace it, so a view that starts at the head
    fetches once with its contracts anyway."""

    class FilterDoor(Door):
        def __init__(self):
            super().__init__()
            self.filtered = []

        def fetch(self, stream, cursor, *, max, tail=False, contracts=None, topics=None):
            if not tail:
                self.filtered.append((stream, contracts))
            return super().fetch(stream, cursor, max=max, tail=tail, contracts=contracts, topics=topics)

    door = FilterDoor()
    door.put({"value": 1})
    current = view(door)
    current.synchronize()
    assert door.filtered == [("metrics", ["_Metric"])]
    door.filtered.clear()
    current.synchronize()
    assert door.filtered == [("metrics", ["_Metric"])]


def test_a_failed_synchronize_is_retried_without_waiting_for_a_new_record():
    # A caller's synchronize() failed once (a 429 from Colca) and marked the
    # view unavailable. The subscription loop retries only when a stream
    # changes, so on a quiet stream the view stayed unavailable for good and
    # every read failed until the process restarted.
    import time

    door = Door()
    door.put({"value": 1})
    current = view(door)
    current.synchronize()
    current.thread.start()  # the subscription loop, without a real watch door
    try:
        door.fail = True
        with pytest.raises(ConnectionError):
            current.synchronize()
        assert not current.available
        door.fail = False  # Colca recovers; nothing new is written
        deadline = time.monotonic() + 10
        while not current.available and time.monotonic() < deadline:
            time.sleep(0.05)
        assert current.available
        assert current.read()[0].payload == {"value": 1}
    finally:
        current.stop.set()
        current.watch.changes.notify()
        current.thread.join(timeout=5)


# ─── scope ───────────────────────────────────────────────────────────────────


class ScopedDoor:
    """A node holding _Signal records at several paths, one entities stream."""

    def __init__(self):
        self.records = []
        self.position = 0
        self.kv_calls = []
        self.fetch_topics = []

    def put(self, path, value):
        topic = f"colca/v1/_Signal/node/{path}"
        self.records.append(SimpleNamespace(topic=topic, payload=value, offset=len(self.records) + 1, ts=0))

    def fetch(self, stream, cursor, *, max, tail=False, contracts=None, topics=None):
        if tail:
            return Page(self.records[-1:], len(self.records) + 1)
        self.fetch_topics.append(topics)
        # The door returns every record: an older node ignores the topic filter.
        rows = self.records[self.position : self.position + max]
        return Page(rows, (rows[-1].offset + 1) if rows else self.position + 1)

    def ack(self, stream, cursor, offset):
        self.position = offset
        return True

    def kv(self, prefix, *, contract, depth=None):
        self.kv_calls.append((prefix, tuple(contract), depth))
        latest = {}
        for r in self.records:
            latest[r.topic] = r
        return [
            KvEntry(r.topic.split("/", 4)[4], "node", r.topic, r.payload, 0, r.offset)
            for r in latest.values()
            # /kv matches its prefix as a string, like the node.
            if r.payload is not None and r.topic.split("/", 4)[4].startswith(prefix)
        ]


def scoped(door, scope):
    return RetainedView(door, ["_Signal"], ["entities"], "c/worker/view", scope=scope)


def paths(view):
    return sorted(e.path for e in view.synchronize()[1])


def test_the_snapshot_reads_only_the_scope_and_whole_segments():
    door = ScopedDoor()
    door.put("line1/press3", {"id": "a"})
    door.put("line1/press3/temp", {"id": "b"})
    door.put("line1/press30/temp", {"id": "c"})
    door.put("line2/temp", {"id": "d"})
    current = scoped(door, ViewScope(["line1/press3"]))
    assert paths(current) == ["line1/press3", "line1/press3/temp"]
    assert door.kv_calls == [("line1/press3", ("_Signal",), None)]


def test_stream_records_outside_the_scope_are_never_applied():
    door = ScopedDoor()
    door.put("line1/temp", {"id": "a"})
    current = scoped(door, ViewScope(["line1"]))
    assert paths(current) == ["line1/temp"]
    door.put("line2/temp", {"id": "b"})
    door.put("line1/pressure", {"id": "c"})
    door.put("line1/temp", None)
    assert paths(current) == ["line1/pressure"]
    # The drain went past the out-of-scope record: nothing stays unread.
    assert door.position == len(door.records)
    assert door.fetch_topics[-1] == ["colca/v1/_Signal/+/line1/#"]


def test_depth_limits_the_snapshot_the_stream_and_the_topic_filters():
    door = ScopedDoor()
    door.put("line1", {"id": "a"})
    door.put("line1/press3", {"id": "b"})
    door.put("line1/press3/temp", {"id": "c"})
    current = scoped(door, ViewScope(["line1"], depth=1))
    assert door.kv_calls == []
    assert paths(current) == ["line1", "line1/press3"]
    assert door.kv_calls == [("line1", ("_Signal",), 1)]
    door.put("line1/press4", {"id": "d"})
    door.put("line1/press4/temp", {"id": "e"})
    assert paths(current) == ["line1", "line1/press3", "line1/press4"]
    assert door.fetch_topics[-1] == ["colca/v1/_Signal/+/line1", "colca/v1/_Signal/+/line1/+"]


def test_several_prefixes_are_one_view():
    door = ScopedDoor()
    door.put("line1/temp", {"id": "a"})
    door.put("line2/temp", {"id": "b"})
    door.put("line3/temp", {"id": "c"})
    current = scoped(door, ViewScope(["line1", "line3/"]))
    assert paths(current) == ["line1/temp", "line3/temp"]
    assert [call[0] for call in door.kv_calls] == ["line1", "line3"]


def test_the_whole_node_is_asked_for_explicitly():
    door = ScopedDoor()
    door.put("line1/temp", {"id": "a"})
    door.put("line2/temp", {"id": "b"})
    current = scoped(door, ViewScope.whole_node())
    assert paths(current) == ["line1/temp", "line2/temp"]
    # No topic filter: the contract filter is the whole scope.
    assert door.fetch_topics[-1] is None
    with pytest.raises(TypeError, match="ViewScope"):
        RetainedView(door, ["_Signal"], ["entities"], "c/worker/view", scope=None)
    with pytest.raises(TypeError):
        RetainedView(door, ["_Signal"], ["entities"], "c/worker/view")


@pytest.mark.parametrize("prefixes", [[], [""], ["/"], ["line1/+"], ["a//b"], ["#"]])
def test_an_empty_or_wildcard_scope_is_refused(prefixes):
    with pytest.raises(ValueError):
        ViewScope(prefixes)


def test_a_single_string_is_not_a_list_of_prefixes():
    with pytest.raises(TypeError):
        ViewScope("line1")


@pytest.mark.parametrize("depth", [0, -1, True, 1.5])
def test_depth_must_be_a_positive_segment_count(depth):
    with pytest.raises(ValueError):
        ViewScope(["line1"], depth=depth)


def test_a_scope_that_needs_too_many_topic_filters_is_refused():
    with pytest.raises(ValueError, match="topic filters"):
        RetainedView(
            ScopedDoor(),
            ["_Signal"],
            ["entities"],
            "c/worker/view",
            scope=ViewScope([f"line{i}" for i in range(400)], depth=2),
        )


# ─── read after write: position and wait_caught_up ───────────────────────────


def test_position_is_the_acknowledged_offset_and_needs_no_network():
    door = Door()
    door.put({"value": 1})
    current = view(door)
    assert current.position() == 0
    current.synchronize()
    assert current.position() == current.position("metrics") == 1
    door.put({"value": 2})
    door.fail = True  # position() never reads the node.
    assert current.position() == 1
    door.fail = False
    assert current.heads() == {"metrics": 2}
    current.synchronize()
    assert current.position() == 2


def test_wait_caught_up_returns_once_the_drain_applied_the_head():
    door = Door()
    door.put({"value": 1})
    current = view(door)
    current.synchronize()
    door.put({"value": 2})
    head = current.heads()
    assert current.wait_caught_up(head, timeout=0) is False

    result = []
    waiter = threading.Thread(target=lambda: result.append(current.wait_caught_up(head, timeout=10)))
    waiter.start()
    current.synchronize()  # What the view's own drain does on a stream hint.
    waiter.join(timeout=10)
    assert result == [True]
    assert current.read()[0].payload == {"value": 2}
    assert current.wait_caught_up(2, timeout=0) is True
    assert current.wait_caught_up(timeout=0) is True, "None captures the heads now"


def test_records_outside_the_scope_still_move_the_position():
    door = ScopedDoor()
    door.put("line1/temp", {"id": "a"})
    current = scoped(door, ViewScope(["line1"]))
    current.synchronize()
    revision = current.revision
    door.put("line2/temp", {"id": "b"})
    current.synchronize()
    assert current.revision == revision, "no in-scope change"
    assert current.wait_caught_up({"entities": 2}, timeout=0) is True


def test_wait_caught_up_gives_up_when_the_view_closes():
    door = Door()
    door.put({"value": 1})
    current = view(door)
    current.synchronize()
    result = []
    waiter = threading.Thread(target=lambda: result.append(current.wait_caught_up(5, timeout=10)))
    waiter.start()
    current.stop.set()
    current._applied.ring()
    waiter.join(timeout=10)
    assert result == [False]


def test_wait_caught_up_names_streams_the_view_reads():
    door = Door()
    multi = RetainedView(door, ["_Metric"], ["metrics", "entities"], "c/w/v", scope=ViewScope.whole_node())
    with pytest.raises(ValueError, match="name one of"):
        multi.position()
    with pytest.raises(ValueError, match="name one of"):
        multi.wait_caught_up(3, timeout=0)
    with pytest.raises(ValueError, match="does not read stream 'alarms'"):
        multi.wait_caught_up({"alarms": 1}, timeout=0)
    with pytest.raises(TypeError):
        multi.wait_caught_up("3", timeout=0)
