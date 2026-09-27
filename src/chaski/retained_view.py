"""A retained snapshot followed by durable, ordered updates.

Capture stream positions before the snapshot; catch up from those positions
before exposing it. Per-topic offsets reject records older than the snapshot.
Tombstones are applied too. This view is rebuildable, not a delivery ledger.
It fetches only its contracts, and fetches on every drain, so other records
on the same stream never count as unread on its cursor.
"""

import copy
import logging
import threading

from ._wakeup import Wakeup
from .door import KvEntry
from .retry import Backoff
from .stream_changes import BatchWait, StreamChanges

log = logging.getLogger(__name__)


class RetainedView:
    def __init__(self, door, contracts, streams, cursor, on_change=None):
        self.door, self.contracts = door, frozenset(contracts)
        self.streams, self.cursor = tuple(streams), cursor
        self.lock, self.stop = threading.RLock(), threading.Event()
        self.entries, self.offsets = {}, {}
        self.positions = {}
        self._stream_versions = {}
        self.initialized = False
        self.available = False
        self.revision = 0
        self.changes = Wakeup()
        self.on_change = on_change
        self.watch = StreamChanges(door, self.streams, disconnected=self._unavailable, contracts=self.contracts)
        self.thread = threading.Thread(target=self._run, daemon=True, name="retained-view")

    def start(self):
        self.watch.start()
        self.thread.start()
        return self

    def _bootstrap(self):
        heads = {s: max(0, self.door.fetch(s, self.cursor, max=1, tail=True).next - 1) for s in self.streams}
        entries = self.door.kv("", contract=self.contracts)
        self.entries = {e.topic: e for e in entries}
        self.offsets = {e.topic: e.offset for e in entries}
        for stream, head in heads.items():
            if head:
                self.door.ack(stream, self.cursor, head)
        self.positions = heads
        self.initialized = True
        self.revision += 1

    def read(self):
        return self.snapshot()[1]

    def snapshot(self):
        """Read the push-maintained view without performing network I/O.

        Initial hydration may be done by the first caller. Once initialized,
        the subscription owns refresh and recovery; readers never defeat its
        batching or backoff. Use synchronize once at a causal boundary when a
        committed upstream effect must be visible before processing input.
        """
        with self.lock:
            if not self.initialized:
                return self.synchronize()
            if not self.available:
                raise RuntimeError("Retained view unavailable; waiting for subscription recovery")
            return self.revision, copy.deepcopy(list(self.entries.values()))

    def synchronize(self):
        """Drain through the current durable heads before returning state."""
        try:
            return self._snapshot()
        except Exception:
            self._unavailable()
            raise

    def _unavailable(self):
        with self.lock:
            if self.available:
                self.available = False
                self.revision += 1
                self.changes.notify()
                if self.on_change is not None:
                    self.on_change()

    def _snapshot(self, streams=None, *, copy_result=True):
        """Flush available updates before a causal read, then return a copy.

        Background wakeups usually drain first. This final drain also covers a
        dependency completion arriving ahead of its stream-change hint.
        """
        with self.lock:
            previous_revision = self.revision
            selected = tuple(self.streams if streams is None else streams)
            versions = {s: self.watch[s].version for s in selected}
            for _ in range(3):
                if not self.initialized:
                    selected = self.streams
                    versions = {s: self.watch[s].version for s in selected}
                    self._bootstrap()
                gap = False
                for stream in selected:
                    # Fetch at least once, also at the head: the node counts
                    # unread records by the cursor's last fetch filter, and a
                    # tail read or an ack does not replace an earlier one.
                    head = self.door.fetch(stream, self.cursor, max=1, tail=True).next - 1
                    while not self.stop.is_set():
                        page = self.door.fetch(stream, self.cursor, max=1000, contracts=sorted(self.contracts))
                        if page.gap is not None:
                            self.initialized = False
                            gap = True
                            break
                        for record in page.records:
                            parts = record.topic.split("/")
                            if len(parts) < 4 or parts[2] not in self.contracts:
                                continue
                            if record.offset <= self.offsets.get(record.topic, -1):
                                continue
                            self.offsets[record.topic] = record.offset
                            self.revision += 1
                            if record.payload is None or record.payload == b"":
                                self.entries.pop(record.topic, None)
                            else:
                                self.entries[record.topic] = KvEntry(
                                    "/".join(parts[4:]),
                                    parts[3],
                                    record.topic,
                                    record.payload,
                                    record.ts,
                                    record.offset,
                                )
                        ack_offset = page.ack_offset
                        if ack_offset is None:
                            if page.next <= head:
                                raise RuntimeError(f"{stream} stopped before its captured head")
                            break
                        self.door.ack(stream, self.cursor, ack_offset)
                        self.positions[stream] = ack_offset
                        if ack_offset >= head:
                            break
                    if gap:
                        break
                if not gap and not self.stop.is_set():
                    if not self.available:
                        self.revision += 1
                    self.available = True
                    if self.revision != previous_revision:
                        self.changes.notify()
                        if self.on_change is not None:
                            self.on_change()
                    self._stream_versions.update(versions)
                    return (self.revision, copy.deepcopy(list(self.entries.values()))) if copy_result else None
            raise RuntimeError("Retained view could not catch up; refusing stale state")

    def _refresh_changed(self):
        with self.lock:
            streams = [
                s for s in self.streams if not self.available or self.watch[s].version != self._stream_versions.get(s)
            ]
            if streams:
                self._snapshot(streams, copy_result=False)

    def _run(self):
        backoff = Backoff()
        batch = BatchWait(self.watch.changes, interval=0.1, stop=self.stop)
        while not self.stop.is_set():
            version = self.watch.changes.version
            try:
                self._refresh_changed()
                backoff.reset()
                retry = None
            except Exception as exc:
                self._unavailable()
                log.warning("Retained view unavailable (%s)", type(exc).__name__)
                retry = backoff.delay(exc)
            batch.wait(version, retry=retry)

    def close(self):
        self.stop.set()
        self.watch.changes.notify()
        self.watch.close()
        if self.thread.is_alive():
            self.thread.join(timeout=16)
