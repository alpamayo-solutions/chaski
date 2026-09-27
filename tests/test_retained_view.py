from types import SimpleNamespace

import pytest

from chaski.door import Gap, KvEntry, Page
from chaski.retained_view import RetainedView

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

    def fetch(self, stream, cursor, *, max, tail=False):
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

    def kv(self, prefix, *, contract):
        self.snapshots += 1
        self.during_snapshot()
        if not self.records or self.records[-1].payload is None:
            return []
        r = self.records[-1]
        return [KvEntry("temperature", "node", TOPIC, r.payload, 0, r.offset)]


def view(door):
    return RetainedView(door, ["_Metric"], ["metrics"], "c/worker/view")


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
    current = RetainedView(door, ["_Metric", "_Signal"], ["metrics", "entities"], "c/worker/view")
    current._refresh_changed()
    door.fetches.clear()
    current.watch["metrics"].notify()
    current._refresh_changed()
    assert door.fetches == ["metrics"]
    door.fetches.clear()
    current._refresh_changed()
    assert door.fetches == []
