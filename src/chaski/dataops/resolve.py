"""KV-based identity resolution for dataops.

Resolves colca identities from the node's retained KV: signal name and element
to Signal ULID, a signal's node-local path to its ULID, element name to ULID,
annotation type name to ULID, and a catalogue tag id to the ``_Signal`` bound
to it (:func:`resolve_output_binding`). There is no database access here.

Two sources answer a lookup:

* a :class:`LiveIndex`, when the service keeps one: one KV read at start and
  after a reconnect, then kept current by the node's retained
  ``_SystemElement``, ``_Signal`` and ``_AnnotationType`` records over MQTT.
  A lookup then reads nothing, and a signal that moves or rebinds resolves to
  its new id as soon as the node says so.
* otherwise a KV read of those three contracts. :func:`one_pass` lets a
  caller resolving many names share one read for the length of a block;
  ``/kv`` is rate-limited, and one read per lookup ran into 429s.
"""

from __future__ import annotations

import contextvars
import json
import logging
import threading
import weakref
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

    from chaski.door import Door

log = logging.getLogger("chaski.dataops.resolve")

#: The contracts an index is built from, and all a resolution read asks for.
INDEX_CONTRACTS = ("_SystemElement", "_Signal", "_AnnotationType")

# Contract markers, slash-delimited so ``_Signal`` cannot match ``_SystemElement``.
_SIGNAL_TOPIC = "/_Signal/"
_SYSTEM_ELEMENT_TOPIC = "/_SystemElement/"
_ANNOTATION_TYPE_TOPIC = "/_AnnotationType/"

#: What an index reads of each record. A live index keeps only these fields,
#: so a record rewritten with the same values is not a change.
_FIELDS = {
    "_SystemElement": ("id", "name"),
    "_Signal": ("id", "name", "system_element_id", "data_tag", "is_published"),
    "_AnnotationType": ("id", "name"),
}


def _payload_of(entry: Any) -> dict[str, Any] | None:
    """The entry's payload as a dict, or None for a tombstone or malformed entry."""
    payload = entry.payload
    if not isinstance(payload, dict) or not payload:
        return None
    return payload


#: One KV read, pinned only for the length of a resolution pass; not a cache.
@dataclass(frozen=True, slots=True)
class _Index:
    """One KV snapshot, indexed by what the resolvers actually ask for.

    Built once per snapshot so a pass over a producer's declared inputs costs
    one dict build rather than a walk per lookup.
    """

    element_id_by_name: dict[str, str]
    annotation_type_id_by_name: dict[str, str]
    signals_by_name: dict[str, list[dict[str, Any]]]
    binding_by_tag: dict[str, tuple[str, str]]
    metric_topic_by_signal_id: dict[str, str]
    element_path_by_id: dict[str, str]
    signal_path_by_id: dict[str, str]
    signal_id_by_path: dict[str, str]


def _build_index(entries: Iterable[Any]) -> _Index:
    return _index_of((entry.topic, _payload_of(entry)) for entry in entries)


def _index_of(records: Iterable[tuple[str, dict[str, Any] | None]]) -> _Index:
    """Index ``(topic, payload)`` pairs; a ``None`` payload is a tombstone."""
    elements: dict[str, str] = {}
    annotation_types: dict[str, str] = {}
    signals: dict[str, list[dict[str, Any]]] = {}
    bindings: dict[str, tuple[str, str]] = {}
    metric_topics: dict[str, str] = {}
    element_paths: dict[str, str] = {}
    signal_paths: dict[str, str] = {}
    signal_ids: dict[str, str] = {}

    for topic, payload in records:
        if payload is None:
            # A tombstone: a retired record is simply not in the index.
            continue
        if _SYSTEM_ELEMENT_TOPIC in topic:
            name, identifier = payload.get("name"), payload.get("id")
            # The first match in snapshot order wins.
            if name is not None and identifier and name not in elements:
                elements[name] = identifier
            if identifier:
                element_paths[identifier] = _path_of(topic)
        elif _ANNOTATION_TYPE_TOPIC in topic:
            name, identifier = payload.get("name"), payload.get("id")
            if name is not None and identifier and name not in annotation_types:
                annotation_types[name] = identifier
        elif _SIGNAL_TOPIC in topic:
            name = payload.get("name")
            if name is not None:
                signals.setdefault(name, []).append(payload)
            tag = payload.get("data_tag")
            identifier = payload.get("id")
            if identifier:
                metric_topics[identifier] = _metric_topic_for(topic)
                signal_paths[identifier] = _path_of(topic)
                signal_ids.setdefault(_path_of(topic), identifier)
            if tag and identifier and payload.get("is_published", False) and tag not in bindings:
                bindings[tag] = (_metric_topic_for(topic), identifier)

    return _Index(elements, annotation_types, signals, bindings, metric_topics, element_paths, signal_paths, signal_ids)


def _read(door: Door) -> _Index:
    """One KV read of the contracts an index is built from."""
    return _build_index(door.kv("", contract=list(INDEX_CONTRACTS)))


# ─── the live index ──────────────────────────────────────────────────────────


def _contract_of(topic: str) -> str | None:
    """``colca/v1/{contract}/{node}/{path…}`` -> ``{contract}``."""
    parts = topic.split("/", 3)
    return parts[2] if len(parts) > 3 else None


def _as_dict(payload: Any) -> dict[str, Any] | None:
    """A record's payload as a dict, from KV (a dict) or MQTT (a decoded
    contract, or raw bytes when franzmq could not decode it); ``None`` for a
    tombstone or something that is not a record."""
    if payload is None:
        return None
    if isinstance(payload, dict):
        return payload or None
    try:
        if isinstance(payload, (bytes, bytearray, str)):
            decoded = json.loads(payload) if payload else None
        else:
            decoded = json.loads(payload.encode())
    except (ValueError, TypeError, AttributeError):
        return None
    return decoded if isinstance(decoded, dict) and decoded else None


def _projection(contract: str, payload: Any) -> dict[str, Any] | None:
    record = _as_dict(payload)
    if record is None:
        return None
    kept = {name: record.get(name) for name in _FIELDS[contract]}
    if "is_published" in kept:
        kept["is_published"] = bool(kept["is_published"])
    return kept


class LiveIndex:
    """The resolution index, kept current by the node's retained records.

    :meth:`seed` reads KV once; :meth:`observe` applies every retained
    ``_SystemElement``, ``_Signal`` and ``_AnnotationType`` record the service
    receives over MQTT from then on, tombstones included. Until the first seed
    succeeds, and from :meth:`suspend` (the broker link dropped, so changes may
    be missed) until the next one, :meth:`current` is ``None`` and resolution
    reads KV as it would without an index.

    Records received while a seed is outstanding are applied after it, in the
    order they arrived, so a write that lands during the read is not lost.

    ``on_change`` listeners run on the thread that applied the change, outside
    the lock, whenever what the index answers changed; a retained record
    delivered again with the same fields is not a change.
    """

    #: Records held while a seed is outstanding; past this the seed is redone.
    MAX_PENDING = 100_000

    def __init__(self, door: Door) -> None:
        self._door = door
        self._lock = threading.Lock()
        self._records: dict[str, dict[str, Any]] = {}
        self._pending: list[tuple[str, dict[str, Any] | None]] | None = []
        self._overflowed = False
        self._index: _Index | None = None
        self._listeners: list[Callable[[], None]] = []

    @property
    def live(self) -> bool:
        with self._lock:
            return self._pending is None

    def add_listener(self, listener: Callable[[], None]) -> None:
        self._listeners.append(listener)

    def observe(self, topic: str, payload: Any) -> None:
        """Apply one retained record as MQTT delivered it (``None`` is a
        tombstone). Records of other contracts are ignored."""
        topic = str(topic)
        contract = _contract_of(topic)
        if contract not in _FIELDS:
            return
        record = _projection(contract, payload)
        with self._lock:
            if self._pending is not None:
                if len(self._pending) >= self.MAX_PENDING:
                    self._pending.clear()
                    self._overflowed = True
                self._pending.append((topic, record))
                return
            changed = _apply(self._records, topic, record)
            if changed:
                self._index = None
        if changed:
            self._notify()

    def seed(self) -> None:
        """Read KV and go live. Raises what the read raised; the index then
        stays as it was (not live) and resolution keeps reading KV."""
        while True:
            entries = self._door.kv("", contract=list(INDEX_CONTRACTS))
            records: dict[str, dict[str, Any]] = {}
            for entry in entries:
                contract = _contract_of(entry.topic)
                if contract in _FIELDS:
                    _apply(records, entry.topic, _projection(contract, entry.payload))
            with self._lock:
                if self._overflowed:
                    # Records were dropped while the read ran: read again.
                    self._overflowed = False
                    self._pending = []
                    continue
                for topic, record in self._pending or ():
                    _apply(records, topic, record)
                changed = records != self._records
                self._records = records
                self._pending = None
                self._index = None
            log.info("resolution index live: %d record(s)", len(records))
            if changed:
                self._notify()
            return

    def suspend(self) -> None:
        """Stop answering until the next :meth:`seed`, and hold what arrives
        meanwhile for it."""
        with self._lock:
            if self._pending is None:
                self._pending = []
            self._index = None

    def current(self) -> _Index | None:
        """The index as it stands, or ``None`` while it is not live."""
        with self._lock:
            if self._pending is not None:
                return None
            if self._index is None:
                self._index = _index_of(self._records.items())
            return self._index

    def _notify(self) -> None:
        for listener in list(self._listeners):
            try:
                listener()
            except Exception:
                log.exception("resolution index listener failed")


def _apply(records: dict[str, dict[str, Any]], topic: str, record: dict[str, Any] | None) -> bool:
    """Set or retire one record; whether anything changed."""
    if record is None:
        return records.pop(topic, None) is not None
    if records.get(topic) == record:
        return False
    records[topic] = record
    return True


_live: weakref.WeakKeyDictionary[Any, LiveIndex] = weakref.WeakKeyDictionary()


def attach(door: Door, index: LiveIndex | None) -> None:
    """Resolve through ``index`` for every lookup on ``door``; ``None`` detaches."""
    if index is None:
        _live.pop(door, None)
    else:
        _live[door] = index


def _live_index(door: Door) -> _Index | None:
    try:
        index = _live.get(door)
    except TypeError:  # a door that cannot be weakly referenced has no index
        return None
    return index.current() if index is not None else None


# ─── one read per pass ──────────────────────────────────────────────────────


class Snapshot:
    """One index, taken on first use and then shared by every pass it is
    entered in: the live index as it stands, or else one KV read. A failed
    read is remembered: resolvers then read on their own, as they would
    outside a pass."""

    def __init__(self, door: Door) -> None:
        self._door = door
        self._index: _Index | None = None
        self._failed = False

    def index(self) -> _Index | None:
        if self._index is None and not self._failed:
            live = _live_index(self._door)
            if live is not None:
                self._index = live
                return live
            try:
                self._index = _read(self._door)
            except Exception as exc:
                self._failed = True
                log.debug("could not pin a KV snapshot for this pass (%s); resolving one at a time", exc)
        return self._index


_pinned: contextvars.ContextVar[Snapshot | None] = contextvars.ContextVar(
    "colca_dataops_resolve_pinned_snapshot", default=None
)


def _pinned_index() -> _Index | None:
    pinned = _pinned.get()
    return pinned.index() if pinned is not None else None


@contextmanager
def one_pass(door: Door) -> Iterator[bool]:
    """Resolve everything inside this block from one KV read.

    Nesting reuses the outer read. Yields ``True`` when a snapshot is pinned
    and ``False`` when the read failed; a failed read does not raise, and each
    resolver then reads on its own as it would outside a pass. Use the yielded
    value to avoid treating a failed read as proof that an id is gone.

    The pin lives in a ContextVar, so concurrent tasks neither see nor wait
    for it.
    """
    if _pinned_index() is not None:
        yield True
        return
    snapshot = Snapshot(door)
    if snapshot.index() is None:
        yield False
        return
    with lazy_pass(snapshot):
        yield True


@contextmanager
def lazy_pass(snapshot: Snapshot) -> Iterator[None]:
    """Like :func:`one_pass`, but the read happens only when a resolver first
    needs it, and ``snapshot`` can be shared by several blocks."""
    token = _pinned.set(snapshot)
    try:
        yield
    finally:
        _pinned.reset(token)


def _snapshot(door: Door) -> _Index:
    """The pass's pinned index, else the live index, else a fresh read."""
    pinned = _pinned_index()
    if pinned is not None:
        return pinned
    live = _live_index(door)
    return live if live is not None else _read(door)


def resolve_metric_topics(door: Door, signal_ids: list[str]) -> dict[str, str]:
    """``{signal_id: its _Metric topic}`` for the ids a snapshot knows.
    Without a pass or a live index, only the ``_Signal`` records are read."""
    index = _pinned_index() or _live_index(door) or _build_index(door.kv("", contract="_Signal"))
    known = index.metric_topic_by_signal_id
    return {sid: known[sid] for sid in signal_ids if sid in known}


def resolve_signal_path(door: Door, path: str) -> str | None:
    """Return the ULID of the ``_Signal`` at node-local ``path``
    (``Plant/Line1/M2/speed``), or ``None``."""
    return _snapshot(door).signal_id_by_path.get(path.strip("/"))


def resolve_system_element(door: Door, name: str) -> str | None:
    """Return a SystemElement's ULID by exact name match, or ``None``."""
    return _snapshot(door).element_id_by_name.get(name)


def resolve_signal(door: Door, name: str, system_element_name: str | None = None) -> str | None:
    """Return a Signal's ULID by ``(name, system_element_name)``, or ``None``.

    Scoped to a SystemElement when given — required whenever the same
    signal name lives on multiple SystemElements. Unscoped (matches on name
    alone) when ``system_element_name`` is ``None``. When a scope is given
    but no SystemElement of that name exists, resolution fails outright
    (``None``) rather than silently falling back to an unscoped match.
    """
    index = _snapshot(door)

    element_id: str | None = None
    if system_element_name is not None:
        element_id = index.element_id_by_name.get(system_element_name)
        if element_id is None:
            return None

    for payload in index.signals_by_name.get(name, ()):
        if element_id is not None and payload.get("system_element_id") != element_id:
            continue
        return payload.get("id")
    return None


def signals_outside_element(door: Door, system_element_id: str, signal_ids: Iterable[str]) -> list[str]:
    """The ids among ``signal_ids`` that the snapshot places outside the
    subtree of ``system_element_id``, in the order given.

    An element or signal the snapshot does not know is not reported: this
    finds the placements that are known to be wrong, not the ones that cannot
    be checked yet.
    """
    index = _snapshot(door)
    root = index.element_path_by_id.get(system_element_id)
    if root is None:
        return []
    outside = []
    for signal_id in signal_ids:
        path = index.signal_path_by_id.get(signal_id)
        if path is not None and not (root == "" or path.startswith(root + "/")):
            outside.append(signal_id)
    return outside


def resolve_annotation_type(door: Door, name: str) -> str | None:
    """Return an AnnotationType's ULID by exact name match, or ``None``."""
    return _snapshot(door).annotation_type_id_by_name.get(name)


def resolve_output_binding(door: Door, tag_id: str) -> tuple[str, str] | None:
    """Find the retained ``_Signal`` whose ``data_tag`` is ``tag_id``, one of
    this service's catalogue tags.

    Returns ``(metric_topic, signal_id)``, or ``None`` when no ``_Signal`` names
    ``tag_id`` yet or the one that does is not ``is_published``. Without a
    live index this reads KV on every call, so a new binding is picked up on
    the next publish either way.
    """
    return _snapshot(door).binding_by_tag.get(tag_id)


def _path_of(topic: str) -> str:
    """``colca/v1/{contract}/{node}/{path…}`` -> ``{path…}``."""
    return "/".join(topic.split("/")[4:])


def _metric_topic_for(signal_topic: str) -> str:
    """``colca/v1/_Signal/{node}/{path…}`` -> the ``_Metric`` topic at the same
    node and path."""
    parts = signal_topic.split("/")
    parts[2] = "_Metric"
    return "/".join(parts)
