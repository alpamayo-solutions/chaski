"""Output primitives for Producer classes (design §5, §8).

Two output types, both publishing through the local door — never MQTT
directly, never a database:

* ``SignalOutput`` — one computed signal. It is NOT resolved/created by the
  producer: the service catalogues every declared ``SignalOutput`` as a
  ``DataTags`` entry, exactly like a connector catalogues its source tags,
  and binds each one to its minted tag id via :func:`build_catalogue`. From
  there the output is commissioned through the SAME command flow as a
  connector tag (``signal/autobind``, or an editor) — the node
  authors the ``_Signal`` record and mints ITS OWN ULID, which the output
  discovers by scanning KV for a ``_Signal`` whose ``data_tag`` names the
  tag id it was bound to (:func:`chaski.dataops.resolve.resolve_output_binding`).
  ``publish`` builds one ``Metric`` and publishes it at the bound position;
  an output with no ``_Signal`` bound to it yet idles — no write, no
  exception — logging at a rate limit rather than on every call.

* ``AnnotationOutput`` — publishes ``_Annotation`` records on the broker's
  own ``annotations`` stream (never a database — that stream's sink is the
  only writer of the projected annotation table, and it is not this
  service). ``write_interval`` derives the annotation's id deterministically
  from ``(annotation_type_id, source, time_start, signal_ids)``
  (``colca_data_contracts.derive_annotation_id``): the same logical
  interval republished — to set ``time_end``, to correct a value — always
  lands on the SAME id, so replay is naturally idempotent with no local
  bookkeeping beyond what :meth:`clear_window` needs (below), while the
  same interval about a different machine is a different annotation.
  ``clear_window`` issues delete-marker appends (``deleted=True``) for ids
  in ``[start, end)`` — but ONLY ids this exact output previously recorded
  emitting in the buffer's ``emitted_annotations`` table
  (:meth:`chaski.dataops.Buffer.record_emitted_annotation`). There is no
  query that lets a producer name an arbitrary id to delete: the set it can
  ever act on is exactly the set it already wrote, so "a producer cannot
  delete an annotation it never emitted" holds structurally, not by
  convention.

Both are declared as class attributes and used through the instance
(``self.oee.publish(...)``) — per-instance copies, exactly like
:class:`~chaski.dataops.inputs.SignalRangeInput`, reaching the door and the
buffer through the producer's runtime (``self.runtime``). They are bound once
by the service runtime after every producer's ``setup()`` has run
(:func:`build_catalogue` for ``SignalOutput``,
:func:`bind_annotation_outputs` for ``AnnotationOutput``): the framework owns
the wiring, a producer's own code never touches a ``Door`` directly.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import httpx
import ulid
from colca_data_contracts import (
    AnnotationPayload,
    DataTag,
    DataTags,
    Metric,
    derive_annotation_id,
)
from franzmq import Topic

from . import resolve
from .inputs import _PerInstance

if TYPE_CHECKING:
    from chaski.door import Door

log = logging.getLogger("chaski.dataops.outputs")

# How often an unbound output is allowed to log its idle status. It is
# checked on every publish() call (potentially once per tick), so without a
# floor an unbound output would spam the log at whatever cadence its
# producer runs at.
_UNBOUND_LOG_INTERVAL_S = 60.0


def _epoch(ts: Any) -> float:
    """Accept a ``datetime``, a float/int epoch, or ``None`` (-> now)."""
    if ts is None:
        return time.time()
    if hasattr(ts, "timestamp") and not isinstance(ts, (int, float)):
        return ts.timestamp()
    return float(ts)


def _catalogue_meta(output: SignalOutput) -> dict[str, Any]:
    """What a catalogue entry says about an output beyond its name and type.

    ``element`` is the node-local path of the element the output belongs
    under. The node's catalogue lifecycle places the signal there instead of
    at this service's own mount — one dataops service computes for several
    machines, and every machine's ``oee`` must land on ITS element, not at
    the node root under one name. Left out when the output names none.
    """
    meta: dict[str, Any] = {}
    if output.description:
        meta["description"] = output.description
    if output.system_element_name:
        meta["element"] = output.system_element_name
    return meta


class SignalOutput(_PerInstance):
    """Output bound to one catalogued signal.

    ``system_element_name`` is the node-local PATH of the element this output
    belongs under (``Roasting/drum-roaster-01``). It travels in the catalogue
    entry as ``meta.element`` and is where the node places the signal when it
    binds the catalogue; it is not used to resolve anything on this side —
    the bound ``_Signal`` is whatever the commissioning act actually bound
    this tag to.
    """

    def __init__(self, signal_name: str, data_type: str, description: str = "", system_element_name: str | None = None):
        self.signal_name = signal_name
        self.data_type = data_type
        self.description = description
        self.system_element_name = system_element_name
        self._source: str | None = None
        self._tag_id: str | None = None
        self._last_unbound_log_ts: float = 0.0
        self._binding: tuple[str, str] | None = None

    def forget(self) -> None:
        """Drop the resolved binding so the next publish resolves again.

        Called on the same resolution pass that re-resolves inputs, which is
        this service's re-resolution cadence for everything.
        """
        self._binding = None

    # ─── binding (set once by build_catalogue) ─────────────────────────

    def bind(self, source: str, tag_id: str) -> None:
        """Attach this run's catalogue identity. Called once by
        :func:`build_catalogue` after it has minted or reused ``tag_id`` for
        this output's ``source`` — never by producer code directly. The door
        it publishes through is the owning producer's runtime's."""
        self._source = source
        self._tag_id = tag_id

    @property
    def source(self) -> str:
        """The catalogue natural key this output was bound under."""
        if self._source is None:
            raise RuntimeError(
                f"SignalOutput[{self.signal_name!r}] is not bound yet — "
                "the service runtime binds every declared SignalOutput via "
                "build_catalogue() before any producer method runs"
            )
        return self._source

    @property
    def tag_id(self) -> str:
        if self._tag_id is None:
            raise RuntimeError(
                f"SignalOutput[{self.signal_name!r}] has no catalogue tag id — not bound yet (see build_catalogue())"
            )
        return self._tag_id

    # ─── publish: one Metric at the bound position ─────────────────────

    def publish(self, value: Any, timestamp: Any = None) -> None:
        """Publish a single ``Metric`` at this output's BOUND position.

        Looks up the current binding fresh (design §4.2's no-cache rule,
        applied to outputs too): a ``_Signal`` naming ``self.tag_id`` via
        ``data_tag`` and marked ``is_published``. When none exists yet —
        the catalogue was published but nobody has commissioned this tag
        through ``signal/autobind`` or an editor — this is a
        no-op: no write, no exception, just a rate-limited idle log (design
        §5: "an unbound output idles its producer's writes with a clear,
        rate-limited log status. No fallback path.").
        """
        door = self._runtime().door
        # A found binding is held; a MISS never is. An unbound output has to
        # keep looking — it becomes bound by a separate act (`signal/autobind`,
        # or an editor) that this service does not perform and
        # cannot be notified of — while a bound one has nothing left to learn
        # until the next resolution pass calls `forget`.
        #
        # Resolving on every publish is what made this the demo's remaining
        # 429 source: each call is a full KV scan, colca serves /kv at five a
        # second, and a node publishing ten computed signals per machine asked
        # for one scan per value.
        try:
            binding = self._binding or resolve.resolve_output_binding(door, self.tag_id)
        except httpx.HTTPStatusError as refused:
            if refused.response.status_code != 429:
                raise
            # 429 is the door WORKING — /kv is a scan class served five a
            # second, and the answer is "ask again later". For a publish that
            # means: unbound for now, idle this one value; the next publish
            # asks again. Raising here killed the producer's whole tick
            # mid-method, taking every later output down with one refused
            # scan — the demo plant's _oee ticks died that way.
            self._log_unbound()
            return
        if binding is None:
            self._log_unbound()
            return
        self._binding = binding
        topic, signal_id = binding

        metric = Metric(value=value, timestamp=_epoch(timestamp), signal_id=signal_id)
        door.publish(topic, metric.encode())
        log.debug("SignalOutput[%s]: published %r @ %s on %s", self.signal_name, value, metric.timestamp, topic)

    def _log_unbound(self) -> None:
        now = time.time()
        if now - self._last_unbound_log_ts < _UNBOUND_LOG_INTERVAL_S:
            return
        self._last_unbound_log_ts = now
        log.warning(
            "SignalOutput[%s] (source=%s, tag=%s) has no bound _Signal yet — publish is idle until it is commissioned",
            self.signal_name,
            self._source,
            self._tag_id,
        )


class AnnotationOutput(_PerInstance):
    """Interval-annotation output, publishing ``_Annotation`` records
    (design §8) rather than writing a database.

    For non-interval (point) annotations, pass ``time_end=time_start``.
    """

    def __init__(self, annotation_name: str, data_type: str = "str", description: str = ""):
        self.annotation_name = annotation_name
        self.data_type = data_type
        self.description = description
        self._source: str | None = None
        self._node_id: str | None = None
        self._topic_prefix: tuple[str, ...] | None = None

    # ─── binding (set once by bind_annotation_outputs) ─────────────────

    def bind(self, source: str, node_id: str, topic_prefix: tuple[str, ...]) -> None:
        """Attach this output's identity. Called once by
        :func:`bind_annotation_outputs` — never by producer code directly.
        ``node_id``/``topic_prefix`` are bound once rather than re-resolved
        via ``door.self_info()`` on every publish — annotations can be
        frequent (design §8: ~1/30s per machine), so a per-call identity
        lookup would add an HTTP round trip to every ``write_interval``/
        ``clear_window``. The door and buffer are the owning producer's
        runtime's.
        """
        self._source = source
        self._node_id = node_id
        self._topic_prefix = topic_prefix

    def _require_source(self) -> str:
        if self._source is None:
            raise RuntimeError(
                f"AnnotationOutput[{self.annotation_name!r}] is not bound yet — "
                "the service runtime binds every declared AnnotationOutput via "
                "bind_annotation_outputs() before any producer method runs"
            )
        return self._source

    @property
    def type_id(self) -> str:
        """This annotation type's ULID, resolved fresh via KV every call —
        the same no-cache rule ``SignalRangeInput.signal_id`` follows
        between resolution passes. Raises if no ``_AnnotationType`` named
        ``self.annotation_name`` exists yet; annotation types are colca
        definitions provisioned by the data-model tooling, not minted here.
        """
        type_id = resolve.resolve_annotation_type(self._runtime().door, self.annotation_name)
        if type_id is None:
            raise LookupError(f"AnnotationType not found: {self.annotation_name!r}")
        return type_id

    def _topic(self, annotation_id: str) -> str:
        if self._node_id is None or self._topic_prefix is None:
            raise RuntimeError(f"AnnotationOutput[{self.annotation_name!r}] is not bound yet")
        return str(
            Topic(
                payload_type=AnnotationPayload,
                node_id=self._node_id,
                context=(*self._topic_prefix, annotation_id),
            )
        )

    # ─── write path ─────────────────────────────────────────────────────

    def write_interval(
        self,
        time_start: Any,
        time_end: Any = None,
        value: Any = None,
        signal_ids: Iterable[str] | None = None,
    ) -> str:
        """Publish (create or update) one annotation instance.

        The annotation's id is DERIVED from ``(annotation_type_id, source,
        time_start, signal_ids)`` — never chosen by the caller — so
        republishing the same logical interval (e.g. to set ``time_end`` once
        it is known) always lands on the same id and topic (design §8), and
        the same interval about a DIFFERENT machine is a different annotation.
        Returns the derived id.
        """
        runtime = self._runtime()
        source = self._require_source()

        ts_start = _epoch(time_start)
        ts_end = _epoch(time_end) if time_end is not None else None
        signals = list(signal_ids or [])
        type_id = self.type_id
        annotation_id = derive_annotation_id(type_id, source, ts_start, signals)

        payload = AnnotationPayload(
            annotation_id=annotation_id,
            annotation_type_id=type_id,
            time_start=ts_start,
            time_end=ts_end,
            value=value,
            signal_ids=signals,
            source=source,
        )
        runtime.door.publish(self._topic(annotation_id), payload.encode())
        runtime.buffer.record_emitted_annotation(source, annotation_id, ts_start)
        log.debug("AnnotationOutput[%s]: published %s @ %s..%s", self.annotation_name, annotation_id, ts_start, ts_end)
        return annotation_id

    def clear_window(self, start: Any, end: Any) -> int:
        """Delete-marker append (``deleted=True``) for every annotation
        THIS output previously emitted with ``time_start`` in
        ``[start, end)`` — read back from the buffer's own
        ``emitted_annotations`` record (see module docstring: this is what
        makes it structurally impossible to delete an id this output never
        emitted). Returns the number of delete markers published.
        """
        runtime = self._runtime()
        source = self._require_source()
        type_id = self.type_id

        start_s, end_s = _epoch(start), _epoch(end)
        pairs = runtime.buffer.emitted_annotations_in_window(source, start_s, end_s)
        for annotation_id, ts_start in pairs:
            payload = AnnotationPayload(
                annotation_id=annotation_id,
                annotation_type_id=type_id,
                time_start=ts_start,
                source=source,
                deleted=True,
            )
            runtime.door.publish(self._topic(annotation_id), payload.encode())
        if pairs:
            log.info(
                "AnnotationOutput[%s]: cleared %d annotation(s) in %s..%s",
                self.annotation_name,
                len(pairs),
                start_s,
                end_s,
            )
        return len(pairs)


# ─── catalogue: SignalOutput provisioning (design §5) ──────────────────────


def _iter_signal_outputs(instances: Iterable[Any]) -> Iterable[tuple[str, SignalOutput, Any]]:
    """Yield ``(source, bound_output, instance)`` for every ``SignalOutput``
    class attribute declared on any of ``instances``' classes.

    ``source`` is ``"{producer.name}.{attr_name}"`` — the same natural-key
    role a connector's ``DataTag.source`` plays (design §5, connector's
    ``(connector, source)`` natural key): stable across a restart as long
    as the producer's name and the attribute name don't change, which is
    exactly what lets catalogue tag ids be reused rather than reminted on
    every restart.
    """
    for instance in instances:
        cls = type(instance)
        for attr_name in dir(cls):
            class_attr = getattr(cls, attr_name, None)
            if not isinstance(class_attr, SignalOutput):
                continue
            yield (f"{instance.name}.{attr_name}", getattr(instance, attr_name), instance)


def declared_outputs(instance: Any) -> Iterable[tuple[str, SignalOutput]]:
    """``(attr_name, bound_output)`` for every ``SignalOutput`` declared on
    ``instance``'s class — what a resolution pass forgets alongside the
    inputs."""
    cls = type(instance)
    for attr_name in dir(cls):
        class_attr = getattr(cls, attr_name, None)
        if isinstance(class_attr, SignalOutput):
            yield attr_name, getattr(instance, attr_name)


def _read_previous_catalogue(door: Door, catalogue_topic: str) -> dict:
    """This service's own previously-published ``DataTags`` payload dict,
    read back from KV, or ``{}`` if it has never published one — the memory
    that lets tag-id minting AND the republish guard both survive a
    restart with no dedicated local state of their own (design §5, mirrors
    connector's ``node_client.previous_catalogue``)."""
    for entry in door.kv(""):
        if entry.topic == catalogue_topic:
            payload = entry.payload
            return payload if isinstance(payload, dict) else {}
    return {}


def build_catalogue(
    instances: Iterable[Any],
    door: Door,
    *,
    node_id: str,
    mount: str,
    service_name: str,
    service_ulid: str,
) -> dict[str, str]:
    """Build, mint/reuse ids for, and (iff changed) publish this run's
    output ``DataTags`` catalogue, then :meth:`SignalOutput.bind` every
    declared output to its resolved tag id.

    Tag-id minting/reuse mirrors a connector's ``_finalize_catalogue``
    exactly: an output whose ``source`` appears in the previous catalogue
    keeps that catalogue entry's id; a new ``source`` mints a fresh ULID;
    a ``source`` that no longer appears (e.g. a producer removed) carries
    its old tag forward marked ``is_stale`` rather than being dropped, so a
    ``_Signal`` still bound to it does not silently rebind or vanish.

    The republish guard is content-hash-guarded like any catalogue — but sourced from KV rather than in-process
    memory (a DataOps service has no other durable catalogue-state store,
    and every other resolve/bind path already reads KV fresh rather than
    caching — :mod:`chaski.dataops.resolve`): the newly-built payload's
    ``(topic, connector, version)`` is compared against the SAME tuple
    read back from the previously retained catalogue, and the publish is
    skipped when they match. Because ids are reused deterministically for
    an unchanged declared-output set, this is also what "catalogue content
    hash is stable across restarts" cashes out as structurally: same
    inputs -> same ids -> same hash -> no wasted re-append/re-replicate.

    Returns ``{source: tag_id}`` for every declared output (bound or not).
    """
    mount_parts = tuple(p for p in mount.split("/") if p)
    catalogue_topic = str(
        Topic(
            payload_type=DataTags,
            node_id=node_id,
            context=(*mount_parts, service_name),
        )
    )

    previous_payload = _read_previous_catalogue(door, catalogue_topic)
    previous = {t.get("source"): t for t in (previous_payload.get("data_tags") or []) if t.get("source")}

    declared = list(_iter_signal_outputs(instances))
    seen_sources: set[str] = set()
    tags: dict[str, DataTag] = {}
    result: dict[str, str] = {}

    for source, output_attr, _instance in declared:
        seen_sources.add(source)
        old = previous.get(source)
        tag_id = old["id"] if old else str(ulid.new())
        tags[tag_id] = DataTag(
            id=tag_id,
            name=output_attr.signal_name,
            source=source,
            # A computed output is never write-accepting from outside — the
            # only writer of its value is the producer's own compute step
            # (there is no reverse "source" to accept an external write) —
            # and it is always readable: the producer always has a current
            # value to offer once it runs.
            is_writable=False,
            is_readable=True,
            data_type=output_attr.data_type,
            is_stale=False,
            meta=_catalogue_meta(output_attr),
        )
        result[source] = tag_id

    for source, old in previous.items():
        if source in seen_sources:
            continue
        old_id = old["id"]
        tags[old_id] = DataTag(
            id=old_id,
            name=old.get("name", ""),
            source=source,
            is_writable=old.get("is_writable", False),
            is_readable=old.get("is_readable", True),
            data_type=old.get("data_type"),
            is_stale=True,
            meta=old.get("meta") or {},
        )

    payload = DataTags(data_tags=list(tags.values()), connector=service_ulid)
    new_state = (catalogue_topic, payload.connector, payload.version)
    old_state = (
        (catalogue_topic, previous_payload.get("connector"), previous_payload.get("version"))
        if previous_payload
        else None
    )

    if new_state != old_state:
        door.publish(catalogue_topic, payload.encode())
        log.info(
            "Published output catalogue to %s: %d tag(s), revision %s", catalogue_topic, len(tags), payload.version[:12]
        )
    else:
        log.debug("Output catalogue unchanged (%s), not republished", payload.version[:12])

    for source, output_attr, _instance in declared:
        output_attr.bind(source, result[source])

    return result


# ─── AnnotationOutput binding (design §8) ──────────────────────────────────


def bind_annotation_outputs(
    instances: Iterable[Any],
    *,
    node_id: str,
    mount: str,
) -> None:
    """Bind every declared ``AnnotationOutput`` on ``instances`` to a stable
    ``source`` + ``_Annotation`` topic prefix.

    ``source`` — the producing identity carried on the wire for audit
    (design §8) — is ``"dataops/{producer.name}"``: stable across a
    restart, which is exactly what :func:`colca_data_contracts.
    derive_annotation_id` needs to derive the same id for the same logical
    interval every time. The topic prefix is this run's mount plus the
    producer's own name and the output's attribute name — a fixed, human-
    readable position under this (unplaced, node-wide-write) local
    service's own subtree; annotations are never KV-projected or retained,
    so nothing downstream depends on this path meaning anything more
    specific than "where this producer's annotations live".
    """
    mount_parts = tuple(p for p in mount.split("/") if p)
    for instance in instances:
        cls = type(instance)
        for attr_name in dir(cls):
            class_attr = getattr(cls, attr_name, None)
            if not isinstance(class_attr, AnnotationOutput):
                continue
            output_attr = getattr(instance, attr_name)
            source = f"dataops/{instance.name}"
            topic_prefix = (*mount_parts, instance.name, attr_name)
            output_attr.bind(source, node_id, topic_prefix)
