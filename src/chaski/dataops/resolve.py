"""KV-based identity resolution for dataops.

Resolves colca identities from the node's retained KV: signal name and element
to Signal ULID, a signal's node-local path to its ULID, element name to ULID,
annotation type name to ULID, and a catalogue tag id to the ``_Signal`` bound
to it (:func:`resolve_output_binding`). There is no database access here.

Services resolve through one push-maintained :class:`LiveIndex`, backed by
:class:`chaski.retained_view.RetainedView`. Direct one-shot utilities may read a
scoped snapshot; a service never switches to HTTP lookup while recovering.
"""

from __future__ import annotations

import contextvars
import logging
import threading
import weakref
from collections.abc import Iterable
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
    disabled_tags: set[str]
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
    disabled_tags: set[str] = set()
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
            if tag and not payload.get("is_published", False):
                disabled_tags.add(tag)
            if tag and identifier and payload.get("is_published", False) and tag not in bindings:
                bindings[tag] = (_metric_topic_for(topic), identifier)

    return _Index(
        elements,
        annotation_types,
        signals,
        bindings,
        metric_topics,
        element_paths,
        signal_paths,
        disabled_tags - bindings.keys(),
        signal_ids,
    )


def _read(door: Door) -> _Index:
    """One KV read of the contracts an index is built from."""
    return _build_index(door.kv("", contract=list(INDEX_CONTRACTS)))


# ─── the live index ──────────────────────────────────────────────────────────


class LiveIndex:
    """Resolution over the SDK's ordered retained view, with no HTTP fallback.

    Subscription starts before hydration. Reconnects and retention gaps recover
    through the same durable view; lookups wait while that view is unavailable.
    A revision change rebuilds the lookup maps once, shared by every resolver.
    """

    def __init__(self, door_or_view):
        from chaski.retained_view import RetainedView, ViewScope

        self._owns_view = not hasattr(door_or_view, "snapshot")
        # Resolution reaches any path a producer names, so the index is the
        # node's, not one subtree's.
        self.view = (
            RetainedView(
                door_or_view,
                INDEX_CONTRACTS,
                ("entities", "definitions"),
                "dataops-definitions",
                scope=ViewScope.whole_node(),
            )
            if self._owns_view
            else door_or_view
        )
        self.changes = self.view.changes
        self._read_lock = threading.Lock()
        self._revision = None
        self._index = None
        self._listeners = []
        prior = getattr(self.view, "on_change", None)

        def changed():
            if prior is not None:
                prior()
            for listener in tuple(self._listeners):
                listener()

        self.view.on_change = changed
        self._started = not self._owns_view

    @property
    def live(self):
        return self.view.available

    def add_listener(self, listener):
        self._listeners.append(listener)

    def seed(self):
        if not self._started:
            self.view.start()
            self._started = True
        self.view.synchronize()

    def suspend(self):
        self.view._unavailable()

    def observe(self, topic, payload):
        # MQTT values have no durable offset: they may wake, never overwrite
        # a newer snapshot. The common view drains the authoritative records.
        for signal in self.view.watch.signals.values():
            signal.notify()
        self.view.watch.changes.notify()

    def current(self):
        return self.index() if self.view.available else None

    def index(self):
        with self._read_lock:
            if self.view.available and self._index is not None and self._revision == self.view.revision:
                return self._index
            revision, entries = self.view.snapshot()
            self._index = _build_index(entries)
            self._revision = revision
            return self._index

    def close(self):
        if self._owns_view and self._started:
            self.view.close()


# Compatibility with callers that already supply their shared retained view.
DefinitionCache = LiveIndex


def _read_index(door):
    cache = getattr(door, "_dataops_definitions", None)
    return cache.index() if cache is not None else _read(door)


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
    if index is None:
        return None
    current = index.current()
    if current is None:
        raise RuntimeError("Resolution index unavailable; waiting for subscription recovery")
    return current


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
                self._index = _read_index(self._door)
            except Exception as exc:
                if getattr(self._door, "_dataops_definitions", None) is not None:
                    raise
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
    return live if live is not None else _read_index(door)


def resolve_metric_topics(door: Door, signal_ids: list[str]) -> dict[str, str]:
    """``{signal_id: its _Metric topic}`` for the ids a snapshot knows.
    Without a pass or a live index, only the ``_Signal`` records are read."""
    index = _pinned_index() or _live_index(door) or _read_index(door)
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


def output_disabled(door: Door, tag_id: str) -> bool:
    """Explicit operator publication setting, distinct from late commissioning."""
    return tag_id in _snapshot(door).disabled_tags


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
