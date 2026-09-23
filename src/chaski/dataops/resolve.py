"""KV-based identity resolution for dataops.

Resolves colca identities from the node's retained KV: signal name and element
to Signal ULID, element name to ULID, annotation type name to ULID, and a
catalogue tag id to the ``_Signal`` bound to it
(:func:`resolve_output_binding`). There is no database access here.

Every ``resolve_*`` call reads KV fresh and nothing is remembered, so a signal
that moves or rebinds resolves to its new id on the next call. :func:`one_pass`
lets a caller resolving many names share one KV read for the length of a
block; ``/kv`` is rate-limited, and one read per lookup ran into 429s.
"""

from __future__ import annotations

import contextvars
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

if TYPE_CHECKING:
    from chaski.door import Door

log = logging.getLogger("chaski.dataops.resolve")

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


def _build_index(entries: list[Any]) -> _Index:
    elements: dict[str, str] = {}
    annotation_types: dict[str, str] = {}
    signals: dict[str, list[dict[str, Any]]] = {}
    bindings: dict[str, tuple[str, str]] = {}
    metric_topics: dict[str, str] = {}
    element_paths: dict[str, str] = {}
    signal_paths: dict[str, str] = {}

    for entry in entries:
        topic = entry.topic
        payload = _payload_of(entry)
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
            if tag and identifier and payload.get("is_published", False) and tag not in bindings:
                bindings[tag] = (_metric_topic_for(topic), identifier)

    return _Index(elements, annotation_types, signals, bindings, metric_topics, element_paths, signal_paths)


_pinned_index: contextvars.ContextVar[_Index | None] = contextvars.ContextVar(
    "colca_dataops_resolve_pinned_index", default=None
)


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
    if _pinned_index.get() is not None:
        yield True
        return
    try:
        index = _build_index(door.kv(""))
    except Exception as exc:
        log.debug("could not pin a KV snapshot for this pass (%s); resolving one at a time", exc)
        yield False
        return
    token = _pinned_index.set(index)
    try:
        yield True
    finally:
        _pinned_index.reset(token)


def _snapshot(door: Door) -> _Index:
    """The pass's pinned index, or a fresh read when no pass is active."""
    pinned = _pinned_index.get()
    return pinned if pinned is not None else _build_index(door.kv(""))


def resolve_metric_topics(door: Door, signal_ids: list[str]) -> dict[str, str]:
    """``{signal_id: its _Metric topic}`` for the ids a snapshot knows. Outside
    a pass, only the ``_Signal`` records are read."""
    pinned = _pinned_index.get()
    index = pinned if pinned is not None else _build_index(door.kv("", contract="_Signal"))
    known = index.metric_topic_by_signal_id
    return {sid: known[sid] for sid in signal_ids if sid in known}


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
    ``tag_id`` yet or the one that does is not ``is_published``. Reads KV on
    every call, so a new binding is picked up on the next publish.
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
