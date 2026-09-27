"""Definition events refresh resolution without per-record broker snapshots."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from dataops_fakes import FakeDoor, FakeRuntime, signal_entry

from chaski.dataops import resolve
from chaski.dataops.base import Producer
from chaski.dataops.inputs import SignalRangeInput
from chaski.dataops.service import build_dispatch


class View:
    def __init__(self, door):
        from chaski._wakeup import Wakeup

        self.door = door
        self.changes = Wakeup()
        self.revision = 0
        self.available = True

    def changed(self):
        self.revision += 1
        self.changes.notify()

    def snapshot(self):
        self.door.kv_calls += 1
        return self.revision, list(self.door.entries)


def watched(entries=()):
    door = FakeDoor(entries)
    cache = resolve.DefinitionCache(View(door))
    door._dataops_definitions = cache
    return door, cache


def test_concurrent_lookups_share_only_definition_reads():
    door, cache = watched([signal_entry("sig-1", "speed")])
    with ThreadPoolExecutor(8) as pool:
        assert list(pool.map(lambda _: resolve.resolve_signal(door, "speed"), range(100))) == ["sig-1"] * 100
    assert door.kv_calls == 1
    door.entries = [signal_entry("sig-2", "speed")]
    cache.view.changed()  # definition event (including a tombstone)
    assert resolve.resolve_signal(door, "speed") == "sig-2"
    assert door.kv_calls == 2
    door.entries = []
    cache.view.changed()
    assert resolve.resolve_signal(door, "speed") is None


def test_invalidation_during_read_is_not_lost():
    door, cache = watched([signal_entry("sig-old", "speed")])
    original = cache.view.snapshot
    read, release = threading.Event(), threading.Event()

    def blocked(*args, **kwargs):
        result = original(*args, **kwargs)
        read.set()
        assert release.wait(2)
        return result

    cache.view.snapshot = blocked
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(resolve.resolve_signal, door, "speed")
        assert read.wait(2)
        door.entries = [signal_entry("sig-new", "speed")]
        cache.view.changed()
        release.set()
        assert future.result() == "sig-old"
    assert resolve.resolve_signal(door, "speed") == "sig-new"


def test_failed_refresh_is_retried_and_not_cached_as_empty():
    door, cache = watched([signal_entry("sig-1", "speed")])
    original = cache.view.snapshot
    cache.view.snapshot = lambda *a, **k: (_ for _ in ()).throw(ConnectionError("broker restarting"))
    with pytest.raises(ConnectionError):
        resolve.resolve_signal(door, "speed")
    cache.view.snapshot = original
    assert resolve.resolve_signal(door, "speed") == "sig-1"


def test_dispatch_reuses_bindings_until_definition_event_including_late_input():
    class Input(Producer):
        name = "cache-test-input"
        speed = SignalRangeInput("speed", window="1h")

    door, cache = watched()
    runtime = FakeRuntime(door, None)
    instances = [Input().attach(runtime)]
    result = build_dispatch(runtime, instances)
    assert result[2] == 1
    assert build_dispatch(runtime, instances) is result
    door.entries = [signal_entry("sig-1", "speed")]
    cache.view.changed()
    result = build_dispatch(runtime, instances)
    assert result[1:] == (["sig-1"], 0)
    for _ in range(100):
        assert build_dispatch(runtime, instances) is result
    assert door.kv_calls == 2
    door.entries = [signal_entry("sig-2", "speed")]
    cache.view.changed()
    assert build_dispatch(runtime, instances)[1] == ["sig-2"]


def test_unavailable_view_cannot_return_a_cached_definition():
    door, cache = watched([signal_entry("sig-1", "speed")])
    assert resolve.resolve_signal(door, "speed") == "sig-1"
    cache.view.available = False
    cache.view.snapshot = lambda: (_ for _ in ()).throw(ConnectionError("disconnected"))
    with pytest.raises(ConnectionError):
        resolve.resolve_signal(door, "speed")
