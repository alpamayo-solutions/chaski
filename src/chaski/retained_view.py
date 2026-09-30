"""A retained snapshot followed by durable, ordered updates.

Capture stream positions before the snapshot; catch up from those positions
before exposing it. Per-topic offsets reject records older than the snapshot.
Tombstones are applied too. This view is rebuildable, not a delivery ledger.

A view holds its contracts within one :class:`ViewScope`: the paths it reads.
The snapshot reads only those paths, and the drain fetches only its contracts
and paths, on every drain, so other records on the same stream never count as
unread on its cursor. Records outside the scope are never applied.
"""

from __future__ import annotations

import copy
import logging
import threading
from collections.abc import Iterable
from dataclasses import dataclass

from colca_data_contracts.root import topic_prefix

from ._wakeup import Wakeup
from .door import KvEntry
from .retry import Backoff
from .stream_changes import BatchWait, StreamChanges

log = logging.getLogger(__name__)

# The node accepts at most this many topic filters on one fetch.
MAX_TOPIC_FILTERS = 1000


@dataclass(frozen=True, init=False)
class ViewScope:
    """The paths a retained view reads.

    ``prefixes`` are hierarchy paths in whole segments, without the node id:
    ``"line1/press3"`` holds ``line1/press3`` itself and everything below it,
    not ``line1/press30``. ``depth`` keeps entries at most that many segments
    below a prefix (``None``: no limit). There is no empty scope: a view of
    every path on the node is :meth:`whole_node`, said explicitly.
    """

    prefixes: tuple[str, ...]
    depth: int | None = None

    def __init__(self, prefixes: Iterable[str], depth: int | None = None):
        if isinstance(prefixes, str):
            raise TypeError("ViewScope prefixes is a collection of paths, not one string")
        normalized = tuple(dict.fromkeys(self._path(p) for p in prefixes))
        if not normalized:
            raise ValueError("ViewScope needs at least one prefix; use ViewScope.whole_node() for every path")
        if depth is not None and (isinstance(depth, bool) or not isinstance(depth, int) or depth < 1):
            raise ValueError("ViewScope depth must be a positive number of path segments")
        object.__setattr__(self, "prefixes", normalized)
        object.__setattr__(self, "depth", depth)

    @staticmethod
    def _path(prefix: str) -> str:
        path = str(prefix).strip("/")
        if not path and prefix != "":
            raise ValueError(f"ViewScope prefix {prefix!r} names no path")
        if not path:
            raise ValueError("ViewScope prefix '' is every path; use ViewScope.whole_node()")
        if any(not segment or segment in ("+", "#") for segment in path.split("/")):
            raise ValueError(f"ViewScope prefix {prefix!r} must be whole path segments without wildcards")
        return path

    @classmethod
    def whole_node(cls, depth: int | None = None) -> ViewScope:
        """Every path on the node, to ``depth`` segments."""
        scope = object.__new__(cls)
        if depth is not None and (isinstance(depth, bool) or not isinstance(depth, int) or depth < 1):
            raise ValueError("ViewScope depth must be a positive number of path segments")
        object.__setattr__(scope, "prefixes", ("",))
        object.__setattr__(scope, "depth", depth)
        return scope

    @property
    def is_whole_node(self) -> bool:
        return self.prefixes == ("",)

    def contains(self, path: str) -> bool:
        """Whether an entry at ``path`` (without the node id) is in scope."""
        for prefix in self.prefixes:
            if prefix == "":
                below = path
            elif path == prefix:
                below = ""
            elif path.startswith(prefix + "/"):
                below = path[len(prefix) + 1 :]
            else:
                continue
            if self.depth is None or (below.count("/") + 1 if below else 0) <= self.depth:
                return True
        return False

    def topic_filters(self, contracts: Iterable[str]) -> list[str] | None:
        """MQTT filters for this scope's records of ``contracts``, for a fetch.

        ``None`` for the whole node without a depth: the contract filter is the
        whole scope.
        """
        if self.is_whole_node and self.depth is None:
            return None
        filters = []
        for contract in sorted(contracts):
            for prefix in self.prefixes:
                base = f"{topic_prefix()}{contract}/+" + (f"/{prefix}" if prefix else "")
                if self.depth is None:
                    filters.append(f"{base}/#")
                    continue
                filters.append(base)
                filters.extend(base + "/+" * level for level in range(1, self.depth + 1))
        if len(filters) > MAX_TOPIC_FILTERS:
            raise ValueError(
                f"this scope needs {len(filters)} topic filters, more than the node accepts "
                f"({MAX_TOPIC_FILTERS}); use fewer prefixes, contracts or a smaller depth"
            )
        return filters


class RetainedView:
    def __init__(self, door, contracts, streams, cursor, *, scope: ViewScope, on_change=None):
        if not isinstance(scope, ViewScope):
            raise TypeError("RetainedView needs a ViewScope; ViewScope.whole_node() reads every path")
        self.door, self.contracts = door, frozenset(contracts)
        self.scope = scope
        self.topics = scope.topic_filters(self.contracts)
        # Only a narrower scope adds parameters to the door calls.
        self._depth: dict[str, int] = {} if scope.depth is None else {"depth": scope.depth}
        self._topics: dict[str, list[str]] = {} if self.topics is None else {"topics": self.topics}
        self.streams, self.cursor = tuple(streams), cursor
        self.lock, self.stop = threading.RLock(), threading.Event()
        self.entries: dict[str, KvEntry] = {}
        self.offsets: dict[str, int] = {}
        self.positions: dict[str, int] = {}
        self._stream_versions: dict[str, int] = {}
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
        entries = [
            e
            for prefix in self.scope.prefixes
            for e in self.door.kv(prefix, contract=sorted(self.contracts), **self._depth)
            # /kv matches the prefix as a string; the scope is whole segments.
            if self.scope.contains(e.path)
        ]
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
            # The subscription loop retries only when a stream changes. On a
            # quiet stream nothing would wake it, and the view a caller just
            # failed stayed unavailable for good; wake it to recover with its
            # own backoff.
            self.watch.changes.notify()
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
                        page = self.door.fetch(
                            stream, self.cursor, max=1000, contracts=sorted(self.contracts), **self._topics
                        )
                        if page.gap is not None:
                            self.initialized = False
                            gap = True
                            break
                        for record in page.records:
                            parts = record.topic.split("/")
                            if len(parts) < 4 or parts[2] not in self.contracts:
                                continue
                            # A node older than colca 0.19 ignores the topic
                            # filter; out-of-scope records are never applied.
                            if not self.scope.contains("/".join(parts[4:])):
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
