"""Local Colca stream hints with fixed-window batching and durable catch-up."""

from __future__ import annotations

import logging
import threading
import time

from ._wakeup import Wakeup
from .outage import Outage
from .retry import Backoff

log = logging.getLogger(__name__)


class StreamChanges:
    """One subscription fans out to independent durable consumers.

    Capture ``signal.version`` before draining, then call ``wait``. Hints may
    coalesce freely; fetching and acknowledging remain the consumer's job.

    ``contracts`` narrows the hints to growth by those contracts. A filtered
    consumer woken only by them leaves its cursor behind the records it skips
    between hints, which holds back the node's pruner; leave it empty unless
    something else wakes the consumer past them.
    """

    def __init__(self, door, streams, *, stop=None, disconnected=None, contracts=(), on_change=None):
        self.on_change = on_change
        self.door = door
        self.contracts = tuple(contracts)
        self.connected = False
        self.disconnected = disconnected
        self.signals = {name: Wakeup() for name in streams}
        self.changes = Wakeup()
        #: Rung each time the subscription is established again after it failed.
        self.reconnected = Wakeup()
        self.stop = stop or threading.Event()
        self.thread = threading.Thread(target=self._run, name="stream-changes", daemon=True)

    def start(self):
        self.thread.start()
        return self

    def __getitem__(self, stream):
        return self.signals[stream]

    def _run(self):
        backoff = Backoff()
        outage = Outage(log, f"Stream subscription {', '.join(self.signals)}")
        # The node's link coming back is the event a failed subscription
        # resumes on; the backoff spaces retries while the link stays up.
        link = getattr(self.door, "link_up", None)
        failed = False
        while not self.stop.is_set():
            error = None
            connected_at = time.monotonic()
            link_seen = link.generation if link is not None else 0
            try:
                for hint in self.door.watch(
                    self.signals, stop=self.stop.is_set, contracts=self.contracts, interval_ms=0
                ):
                    if failed:
                        failed = False
                        outage.recovered()
                        self.reconnected.notify()
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
                failed = True
            self.connected = False
            if self.disconnected is not None:
                self.disconnected()
            if not self.stop.is_set():
                delay = backoff.delay(error)
                if error is not None and not outage.failed(error, delay=delay):
                    log.error("Stream subscription disconnected; reconnecting in %.1fs", delay, exc_info=error)
                if link is None:
                    self.stop.wait(delay)
                elif link.wait_after(link_seen, delay, stop=self.stop):
                    backoff.reset()  # the link is back: reconnect now, back off afresh
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

    def wait(self, version, *, retry=None, resume=None, resume_since=0):
        """Wait for the next drain. After a failure (``retry`` seconds) only
        the backoff or ``resume`` ringing after ``resume_since`` (the
        subscription re-established after an outage) ends the wait: busy hints
        cannot bypass server backpressure. True when ``resume`` ended it."""
        resumed = False
        if retry is not None:
            if resume is None:
                self.stop.wait(retry)
            else:
                resumed = resume.wait_after(resume_since, retry, stop=self.stop)
        else:
            self.signal.wait(version)
        self.stop.wait(max(0, self.started + self.interval - time.monotonic()))
        self.started = time.monotonic()
        return resumed
