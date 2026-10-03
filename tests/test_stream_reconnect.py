"""A stream subscription that failed resumes when the node's link comes back,
not after its own backoff; while the link stays up the backoff spaces retries."""

from __future__ import annotations

import threading
import time

import httpx

from chaski.doorbell import Doorbell
from chaski.stream_changes import StreamChanges


class _Door:
    """A node that refuses /watch until ``up`` is set, then streams one hint."""

    def __init__(self) -> None:
        self.link_up = Doorbell()
        self.up = threading.Event()
        self.attempts: list[float] = []

    def watch(self, streams, *, stop, contracts=(), interval_ms=None):
        self.attempts.append(time.monotonic())
        if not self.up.is_set():
            raise httpx.ConnectError("connection refused")
        yield type("Hint", (), {"streams": list(streams)})()
        while not stop():
            time.sleep(0.01)


def _wait(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.005)


def test_a_failed_subscription_reconnects_when_the_link_comes_back():
    door = _Door()
    watch = StreamChanges(door, ["metrics"]).start()
    try:
        _wait(lambda: len(door.attempts) >= 3, timeout=10)  # 1st try, then backoff ~0.5-1 s, ~1-2 s
        time.sleep(0.2)
        before = len(door.attempts)
        reconnected = watch.reconnected.version
        door.up.set()
        rang = time.monotonic()
        door.link_up.ring()
        assert watch.reconnected.wait(reconnected, 1.0), "no reconnect on the link coming back"
        assert door.attempts[before] - rang < 0.2
        assert watch.connected
    finally:
        watch.close()


def test_failures_while_the_link_stays_up_back_off():
    door = _Door()
    watch = StreamChanges(door, ["metrics"]).start()
    try:
        time.sleep(1.2)
        # Backoff 0.5-1 s after the first failure, then 1-2 s: at most two attempts.
        assert 1 <= len(door.attempts) <= 2, door.attempts
    finally:
        watch.close()
