"""A retained snapshot followed by durable, ordered updates.

Capture stream positions before the snapshot; catch up from those positions
before exposing it. Per-topic offsets reject records older than the snapshot.
Tombstones are applied too. This view is rebuildable, not a delivery ledger.

A view holds its contracts within one :class:`ViewScope`: the paths it reads.
The snapshot reads only those paths, and the drain fetches only its contracts
and paths, on every drain, so other records on the same stream never count as
unread on its cursor. Records outside the scope are never applied.

The view wakes on every growth of its streams, not only on its own contracts.
A drain that finds nothing of its own still acks the offset it scanned to, so
the cursor follows the stream head. Woken only for its contracts, the cursor
stood still between relevant changes: the node's pruner, which never cuts
below the lowest cursor, kept everything after it, and the cursor read as lag.
Drains that found nothing of the view's start at most :data:`WALK_INTERVAL_S`
apart, so a stream busy with other contracts costs a bounded request rate.
"""

from __future__ import annotations

import copy
import logging
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from colca_data_contracts.root import topic_prefix

from ._wakeup import Wakeup
from .door import KvEntry
from .doorbell import Doorbell
from .outage import ColcaUnavailable, Outage
from .retry import Backoff
from .stream_changes import BatchWait, StreamChanges

log = logging.getLogger(__name__)

# The node accepts at most this many topic filters on one fetch.
MAX_TOPIC_FILTERS = 1000

#: Minimum spacing of drain starts after a drain that changed the view.
DRAIN_INTERVAL_S = 0.1
#: Minimum spacing of drain starts after a drain that only walked the cursor
#: past records of other contracts or paths. A change of the view's own waits
#: at most this long, and only while the stream is busy with others.
WALK_INTERVAL_S = 1.0


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
        # Per stream, the subscription of the hint its last complete drain
        # started from (see StreamChange.covers).
        self._drained_on: dict[str, int] = {}
        self.initialized = False
        self.available = False
        self.revision = 0
        self.changes = Wakeup()
        # Rung whenever a stream position moves, also without a state change
        # (records outside the scope still advance the cursor).
        self._applied = Doorbell()
        self.on_change = on_change
        # Logs a failing refresh once, and its recovery once, whichever caller recovers it.
        self._outage = Outage(log, f"Retained view {cursor}")
        # Every growth of the streams wakes the view (see the module docstring).
        self.watch = StreamChanges(door, self.streams, disconnected=self._unavailable)
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
        self._drained_on = {}
        self.initialized = True
        self.revision += 1
        self._applied.ring()

    def read(self):
        return self.snapshot()[1]

    def _stream(self, stream):
        if stream is None:
            if len(self.streams) != 1:
                raise ValueError(f"this view reads {len(self.streams)} streams; name one of {list(self.streams)}")
            return self.streams[0]
        if stream not in self.streams:
            raise ValueError(f"this view does not read stream {stream!r}; it reads {list(self.streams)}")
        return stream

    def position(self, stream=None):
        """The last offset of ``stream`` this view applied and acknowledged.

        Every record up to it is in :meth:`snapshot`, or was outside the
        view's scope. ``stream`` may be omitted when the view reads one
        stream. 0 before the view first hydrated. No network I/O.
        """
        stream = self._stream(stream)
        with self.lock:
            return self.positions.get(stream, 0)

    def heads(self):
        """The last admitted offset of every stream this view reads, now.

        One tail read per stream with the view's cursor; it moves nothing and
        does not change the cursor's filter. Pass the result to
        :meth:`wait_caught_up` for a read that must include every record
        admitted before it arrived.
        """
        return {s: max(0, self.door.fetch(s, self.cursor, max=1, tail=True).next - 1) for s in self.streams}

    def wait_caught_up(self, heads=None, timeout=None):
        """Block until this view applied every stream through ``heads``.

        ``heads`` maps stream names to offsets (from :meth:`heads`, or a head
        the caller captured elsewhere); an ``int`` is the head of the one
        stream a single-stream view reads; ``None`` captures :meth:`heads`
        now. Waits on the view's own drain, which runs on stream-change
        hints; nothing is read on a timer and nothing is drained here.

        True once :meth:`position` reached every head; False when ``timeout``
        (seconds) passed first or the view is closed. A following
        :meth:`snapshot` then holds every record up to those heads.
        """
        if heads is None:
            heads = self.heads()
        elif isinstance(heads, Mapping):
            heads = {self._stream(stream): int(offset) for stream, offset in heads.items()}
        elif isinstance(heads, int) and not isinstance(heads, bool):
            heads = {self._stream(None): heads}
        else:
            raise TypeError("heads is a {stream: offset} mapping, one offset or None")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            seen = self._applied.generation
            if self.stop.is_set():
                return False
            with self.lock:
                if all(self.positions.get(stream, 0) >= head for stream, head in heads.items()):
                    return True
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return False
            self._applied.wait_after(seen, remaining)

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
                raise ColcaUnavailable("Retained view unavailable; waiting for subscription recovery")
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

    def _snapshot(self, streams=None, *, copy_result=True, hinted=False):
        """Flush available updates before a causal read, then return a copy.

        Background wakeups usually drain first. This final drain also covers a
        dependency completion arriving ahead of its stream-change hint.

        ``hinted`` (the background drain) bounds each stream by the head of its
        newest hint instead of reading the tail, and skips a stream whose hint
        the view already applied. A causal read keeps reading the tail.
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
                    change = self.watch.latest(stream) if hinted else None
                    if change is not None and change.covers(
                        self.positions.get(stream, 0), self._drained_on.get(stream)
                    ):
                        continue
                    # Fetch at least once, also at the head: the node counts
                    # unread records by the cursor's last fetch filter, and a
                    # tail read or an ack does not replace an earlier one. A
                    # new subscription is drained once, so this holds.
                    if change is not None and change.head is not None:
                        head = change.head
                    else:
                        head = self.door.fetch(stream, self.cursor, max=1, tail=True).next - 1
                    first = True
                    while not self.stop.is_set():
                        page = self.door.fetch(
                            stream, self.cursor, max=1000, contracts=sorted(self.contracts), **self._topics
                        )
                        reset = first and page.start is not None and page.start - 1 < self.positions.get(stream, 0)
                        first = False
                        if reset:
                            # The node lost acknowledged records (a reset or
                            # restore). Entries and per-topic offsets from
                            # before it would hide newer records at lower
                            # offsets and keep deleted paths: rebuild.
                            log.warning(
                                "Retained view %s: %s restarted below the applied position; rebuilding",
                                self.cursor,
                                stream,
                            )
                        if reset or page.gap is not None:
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
                                raise ColcaUnavailable(f"{stream} stopped before its captured head")
                            if not page.records and page.next - 1 > self.positions.get(stream, 0):
                                # Nothing after the cursor: it already stands at the head.
                                self.positions[stream] = page.next - 1
                                self._applied.ring()
                            break
                        self.door.ack(stream, self.cursor, ack_offset)
                        self.positions[stream] = ack_offset
                        self._applied.ring()
                        if ack_offset >= head:
                            break
                    if gap:
                        break
                    if change is not None and not self.stop.is_set():
                        self._drained_on[stream] = change.subscription
                if not gap and not self.stop.is_set():
                    if not self.available:
                        self.revision += 1
                    self.available = True
                    self._outage.recovered()
                    if self.revision != previous_revision:
                        self.changes.notify()
                        if self.on_change is not None:
                            self.on_change()
                    self._stream_versions.update(versions)
                    return (self.revision, copy.deepcopy(list(self.entries.values()))) if copy_result else None
            raise ColcaUnavailable("Retained view could not catch up; refusing stale state")

    def _refresh_changed(self):
        with self.lock:
            streams = [
                s for s in self.streams if not self.available or self.watch[s].version != self._stream_versions.get(s)
            ]
            if streams:
                self._snapshot(streams, copy_result=False, hinted=True)

    def _run(self):
        backoff = Backoff()
        batch = BatchWait(self.watch.changes, interval=DRAIN_INTERVAL_S, stop=self.stop)
        while not self.stop.is_set():
            version = self.watch.changes.version
            resumed_seen = self.watch.reconnected.version
            revision = self.revision
            try:
                self._refresh_changed()
                backoff.reset()
                retry = None
                batch.interval = DRAIN_INTERVAL_S if self.revision != revision else WALK_INTERVAL_S
            except Exception as exc:
                self._unavailable()
                if self.stop.is_set():
                    break  # closing: a drain cut short is not an outage
                retry = backoff.delay(exc)
                if not self._outage.failed(exc, delay=retry):
                    log.error("Retained view %s unavailable; retrying in %.1fs", self.cursor, retry, exc_info=exc)
            if batch.wait(version, retry=retry, resume=self.watch.reconnected, resume_since=resumed_seen):
                backoff.reset()  # the subscription is back: refresh now, back off afresh

    def close(self):
        self.stop.set()
        self._applied.ring()
        self.watch.changes.notify()
        self.watch.close()
        if self.thread.is_alive():
            self.thread.join(timeout=16)
