"""Local Colca stream hints with fixed-window batching and durable catch-up."""

from __future__ import annotations

import logging
import threading
import time

from ._wakeup import Wakeup
from .retry import Backoff

log = logging.getLogger(__name__)


class StreamChanges:
    """One subscription fans out to independent durable consumers.

    Capture ``signal.version`` before draining, then call ``wait``. Hints may
    coalesce freely; fetching and acknowledging remain the consumer's job.
    """

    def __init__(self, door, streams, *, stop=None, disconnected=None, contracts=(), on_change=None):
        self.on_change = on_change
        self.door = door
        self.contracts = tuple(contracts)
        self.connected = False
        self.disconnected = disconnected
        self.signals = {name: Wakeup() for name in streams}
        self.changes = Wakeup()
        self.stop = stop or threading.Event()
        self.thread = threading.Thread(target=self._run, name="stream-changes", daemon=True)

    def start(self):
        self.thread.start()
        return self

    def __getitem__(self, stream):
        return self.signals[stream]

    def _run(self):
        backoff = Backoff()
        while not self.stop.is_set():
            error = None
            connected_at = time.monotonic()
            try:
                for hint in self.door.watch(
                    self.signals, stop=self.stop.is_set, contracts=self.contracts, interval_ms=0
                ):
                    self.connected = True
                    if time.monotonic() - connected_at >= 30:
                        backoff.reset()
                    for stream in hint.streams:
                        if stream in self.signals:
                            self.signals[stream].notify()
                    self.changes.notify()
                    if self.on_change is not None:
                        self.on_change()
            except Exception as exc:
                error = exc
                log.warning("Stream subscription disconnected (%s); reconnecting", type(exc).__name__)
            self.connected = False
            if self.disconnected is not None:
                self.disconnected()
            if not self.stop.is_set():
                self.stop.wait(backoff.delay(error))
        self.changes.notify()
        for signal in self.signals.values():
            signal.notify()

    def close(self):
        self.stop.set()
        self.changes.notify()
        for signal in self.signals.values():
            signal.notify()
        self.thread.join(timeout=16)


class BatchWait:
    """Minimum spacing between drains; pages within a drain run immediately."""

    def __init__(self, signal, interval=0.1, stop=None):
        if not 0 <= interval <= 30:
            raise ValueError("batch interval must be between 0 and 30 seconds")
        self.signal, self.interval = signal, interval
        self.stop = stop or threading.Event()
        self.started = time.monotonic()

    @property
    def version(self):
        return self.signal.version

    def wait(self, version, *, retry=None):
        if retry is not None:
            self.stop.wait(retry)  # Busy hints cannot bypass server backpressure.
        else:
            self.signal.wait(version)
        self.stop.wait(max(0, self.started + self.interval - time.monotonic()))
        self.started = time.monotonic()
