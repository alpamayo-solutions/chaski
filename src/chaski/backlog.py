"""Push-maintained queue telemetry with a causal final-drain fence."""

import logging
import threading
import time

from ._wakeup import Wakeup
from .retry import Backoff

log = logging.getLogger(__name__)


class BacklogView:
    def __init__(self, door, prefixes, changes, *, interval=1.0, max_age=15.0):
        self.door, self.prefixes, self.changes = door, prefixes, changes
        self.interval, self.max_age = interval, max_age
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.wake = Wakeup()
        self.rows = None
        self.sample_started = self.last_hint = 0.0
        self.connected = False
        self.fence_after = None
        self.thread = threading.Thread(target=self.run, name="queue-snapshot", daemon=True)
        self.subscription = threading.Thread(target=self.watch, name="queue-watch", daemon=True)

    def start(self):
        self.subscription.start()
        self.thread.start()
        return self

    def __call__(self):
        with self.lock:
            if self.rows is None or not self.connected or time.monotonic() - self.last_hint > self.max_age:
                raise RuntimeError("Queue telemetry unavailable or stale")
            return [dict(row) for row in self.rows]

    def fence_snapshot(self):
        """Request a read started after the caller observed final worker completion."""
        with self.lock:
            if self.fence_after is None:
                self.fence_after = time.monotonic()
                self.wake.notify()
            if self.sample_started < self.fence_after:
                raise RuntimeError("Waiting for final queue snapshot")
        return self()

    def watch(self):
        backoff = Backoff()
        while not self.stop.is_set():
            error = None
            connected_at = time.monotonic()
            try:
                for changed in self.door.watch_backlog(self.stop.is_set):
                    if time.monotonic() - connected_at >= 30:
                        backoff.reset()
                    with self.lock:
                        reconnected = not self.connected
                        self.last_hint = time.monotonic()
                        self.connected = True
                    if changed or reconnected:
                        self.wake.notify()
            except Exception as exc:
                error = exc
                log.warning("Queue subscription unavailable (%s)", type(exc).__name__)
            with self.lock:
                self.connected = False
            self.changes.notify()
            self.stop.wait(backoff.delay(error))

    def run(self):
        backoff = Backoff()
        while not self.stop.is_set():
            version = self.wake.version
            started = time.monotonic()
            try:
                rows = self.door.backlog(self.prefixes)
                if not rows:
                    raise RuntimeError("Queue telemetry is empty")
                retry = None
                backoff.reset()
            except Exception as exc:
                rows, retry = None, backoff.delay(exc)
            with self.lock:
                self.rows, self.sample_started = rows, started
            self.changes.notify()
            if retry is not None:
                # Backlog hints cannot bypass server backpressure.
                self.stop.wait(retry)
            else:
                self.wake.wait(version)
            self.stop.wait(max(0, started + self.interval - time.monotonic()))

    def close(self):
        self.stop.set()
        self.wake.notify()
        self.thread.join(timeout=16)
        self.subscription.join(timeout=16)
