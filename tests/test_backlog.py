import threading
import time

import pytest

from chaski._wakeup import Wakeup
from chaski.backlog import BacklogView


class Door:
    def __init__(self):
        self.hints = Wakeup()
        self.reads = 0
        self.entered = threading.Event()
        self.release = threading.Event()

    def backlog(self, prefixes):
        assert prefixes == ["c/projector/"]
        self.reads += 1
        self.entered.set()
        assert self.release.wait(3)
        return [{"cursor": "projector", "position": 10, "head": 10}]

    def watch_backlog(self, stop):
        while not stop():
            version = self.hints.version
            yield True
            self.hints.wait(version, 0.1)  # test transport heartbeat
            if self.hints.version == version:
                while not stop() and self.hints.version == version:
                    yield False
                    self.hints.wait(version, 0.1)


def test_push_only_reads_and_causal_final_fence():
    door = Door()
    changes = Wakeup()
    monitor = BacklogView(door, ["c/projector/"], changes, interval=0.01).start()
    try:
        assert door.entered.wait(1)
        with pytest.raises(RuntimeError, match="unavailable"):
            monitor()
        version = changes.version
        door.release.set()
        changes.wait(version, 1)
        assert monitor()[0]["position"] == 10
        # Subscription heartbeats do not cause storage reads or consumer wakeups.
        time.sleep(0.2)
        reads = door.reads
        version = changes.version
        changes.wait(version, 0.3)
        assert changes.version == version
        assert door.reads == reads
        with pytest.raises(RuntimeError, match="final"):
            monitor.fence_snapshot()
        changes.wait(version, 1)
        assert monitor.fence_snapshot()[0]["head"] == 10
        version = changes.version
        door.hints.notify()
        changes.wait(version, 1)
        assert door.reads > reads
        with monitor.lock:
            monitor.connected = False
        with pytest.raises(RuntimeError, match="stale"):
            monitor()
    finally:
        monitor.close()
        assert not monitor.thread.is_alive()
        assert not monitor.subscription.is_alive()


def test_backlog_hints_cannot_bypass_retry_after(monkeypatch):
    import httpx

    class RateLimited(Door):
        def backlog(self, prefixes):
            self.reads += 1
            monitor.wake.notify()
            response = httpx.Response(
                429, headers={"Retry-After": "45"}, request=httpx.Request("GET", "http://colca/backlog")
            )
            response.raise_for_status()

    door = RateLimited()
    monitor = BacklogView(door, [], Wakeup())
    delays = []
    original_wait = monitor.stop.wait

    def wait(delay):
        delays.append(delay)
        monitor.stop.set()
        return original_wait(0)

    monkeypatch.setattr(monitor.stop, "wait", wait)
    monitor.run()
    assert door.reads == 1
    assert 45 <= delays[0] <= 54
