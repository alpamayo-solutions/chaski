"""KV-based identity resolution for dataops (design §4.2, §5, §8).

Resolves colca identities by scanning the node's retained KV projection —
``_Signal`` records for (signal name, system element name) -> signal ULID,
``_SystemElement`` records for element name -> ULID, ``_AnnotationType``
records for annotation-type name -> ULID, and (the reverse direction,
design §5) a service's own catalogue tag ULID -> the ``_Signal`` record
bound to it, via :func:`resolve_output_binding`. This is the ONLY
resolution path: no database access, no historian involvement (the
optional :class:`~chaski.dataops.inputs.Historian` is a different concern
entirely — historised metric *values*, not topology identities).

Every ``resolve_*`` call reads KV fresh, and nothing here remembers a
resolved id — a signal that moves or rebinds resolves to its new id on the
very next call. That rule is load-bearing and unchanged.

What :func:`one_pass` adds is narrower: a caller resolving MANY names at
once can pin a single KV read for the duration of that pass. colca serves
``/kv`` at five requests a second (it is a SCAN class), and a pass over
thirty declared inputs asked for sixty full snapshots — ``resolve_signal``
alone asks twice, once for the element and once for the signal. colca
refused most of them with HTTP 429, and each refusal read as an input that
could not be resolved, which excluded it from dispatch until the next pass
failed the same way. That was the demo's entire log noise.

A snapshot that outlives its pass is a different thing and was tried: it
made the level-4 dataops contract fail while every unit test passed, and
the same index with the pin disabled passes, so the resolvers were right
and the LIFETIME was wrong. Hence a pin scoped to a block a caller opens
deliberately, and nothing at all between them.
"""

from __future__ import annotations

import contextvars
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

if TYPE_CHECKING:
    from chaski.door import Door

log = logging.getLogger("chaski.dataops.resolve")

# Contract markers: a KV entry's ``topic`` is ``colca/v1/_{Contract}/{node}/{path...}``
# ("State Topic Structure") — slash-delimited on both sides, so a
# substring check can't collide between e.g. ``_Signal`` and ``_SystemElement``.
_SIGNAL_TOPIC = "/_Signal/"
_SYSTEM_ELEMENT_TOPIC = "/_SystemElement/"
_ANNOTATION_TYPE_TOPIC = "/_AnnotationType/"


def _payload_of(entry: Any) -> dict[str, Any] | None:
    """The entry's payload as a dict, or None for a tombstone/malformed entry.

    A retired SystemElement/Signal/AnnotationType is a retained EMPTY
    payload (the tombstone convention for ``_Signal``/
    ``_SystemElement``) — that must never resolve as a match.
    """
    payload = entry.payload
    if not isinstance(payload, dict) or not payload:
        return None
    return payload


#: One KV read, pinned for the duration of a resolution PASS.
#:
#: NOT a cache with a lifetime. A snapshot that outlived the call that took it
#: broke the level-4 dataops contract: the resolvers were right — the same
#: index with the pin disabled passes — but an answer surviving into unrelated
#: later calls did not. So this holds a snapshot only while a caller explicitly
#: says "these lookups are one pass", and nothing is remembered afterwards.
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


def _build_index(entries: list[Any]) -> _Index:
    elements: dict[str, str] = {}
    annotation_types: dict[str, str] = {}
    signals: dict[str, list[dict[str, Any]]] = {}
    bindings: dict[str, tuple[str, str]] = {}

    for entry in entries:
        topic = entry.topic
        payload = _payload_of(entry)
        if payload is None:
            # A tombstone. Skipped rather than indexed as an absence: the
            # snapshot IS the current state, so a retired record simply is
            # not in it.
            continue
        if _SYSTEM_ELEMENT_TOPIC in topic:
            name, identifier = payload.get("name"), payload.get("id")
            # First wins, matching the scan this replaced: it returned the
            # first match in snapshot order and stopped.
            if name is not None and identifier and name not in elements:
                elements[name] = identifier
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
            if tag and identifier and payload.get("is_published", False) and tag not in bindings:
                bindings[tag] = (_metric_topic_for(topic), identifier)

    return _Index(elements, annotation_types, signals, bindings)


_pinned_index: contextvars.ContextVar[_Index | None] = contextvars.ContextVar(
    "colca_dataops_resolve_pinned_index", default=None
)


@contextmanager
def one_pass(door: Door) -> Iterator[bool]:
    """Resolve everything inside this block from ONE KV read.

    For a caller resolving many names at once — a producer's declared inputs,
    of which there can be dozens. Each ``resolve_*`` call reads KV on its own,
    and ``resolve_signal`` reads twice (element, then signal), so a pass over
    thirty inputs asked colca for sixty full snapshots. colca serves ``/kv`` at
    five a second (it is a SCAN), refused the rest with HTTP 429, and every
    refusal read as an input that could not be resolved — excluded from
    dispatch until the next pass failed the same way.

    Nesting reuses the outer pin rather than reading again, so a caller that
    opens a pass per producer inside a pass over all of them still costs one
    read, not one per producer.

    Yields ``True`` when a snapshot was actually pinned (including a nested
    pass reusing the outer pin), ``False`` when the read failed. A read that
    FAILS does not pin and does not raise — the whole point of the pass is to
    spare the door, so it must not become a new way to lose: every resolver
    then reads on its own and fails exactly where it failed before, inside
    whatever per-input handling the caller already has. Raising here instead
    took the entire service down on one refused scan. A caller that needs to
    tell "pinned and resolved cleanly" apart from "pin failed, resolving
    degraded" — e.g. so it never treats a failed read as proof a previously
    resolved id is now gone — reads the yielded value.

    A ContextVar rather than a module global: the pin belongs to the call that
    took it, so a concurrent task neither sees it nor is blocked by it. Outside
    the block every resolver reads KV fresh, exactly as before — nothing here
    caches across calls.
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


def resolve_annotation_type(door: Door, name: str) -> str | None:
    """Return an AnnotationType's ULID by exact name match, or ``None``."""
    return _snapshot(door).annotation_type_id_by_name.get(name)


def resolve_output_binding(door: Door, tag_id: str) -> tuple[str, str] | None:
    """Find the retained ``_Signal`` record bound to one of THIS service's
    own catalogue tags (design §5), the same direction a connector binds:
    a ``_Signal``'s ``data_tag`` names a tag ULID from the publisher's own
    catalogue, never the other way around.

    Returns ``(metric_topic, signal_id)`` — the position and Signal ULID a
    ``_Metric`` publish for this binding belongs at — or ``None`` when no
    ``_Signal`` names ``tag_id`` yet (unbound), or the one that does hasn't
    been marked ``is_published`` (mirrors the connector's own
    ``_update_publish_targets`` filter: a binding exists but publication
    wasn't asked for).

    Fresh KV read every call, like every other ``resolve_*`` function here —
    nothing caches a binding, so a rebind (or the FIRST bind, arriving after
    this service already started) is picked up on the very next publish. An
    unbound output idles meanwhile, which is what it already does.
    """
    return _snapshot(door).binding_by_tag.get(tag_id)


def _metric_topic_for(signal_topic: str) -> str:
    """``colca/v1/_Signal/{node}/{path…}`` -> the ``_Metric`` topic at the
    SAME node and the SAME path — a metric for a binding belongs at exactly
    the position its ``_Signal`` record occupies (mirrors the connector's
    own ``_metric_topic_for``)."""
    parts = signal_topic.split("/")
    parts[2] = "_Metric"
    return "/".join(parts)
