"""Output primitives for Producer classes.

Both publish through the local door, never MQTT directly and never a database.

* ``SignalOutput``: one computed signal. The service catalogues every declared
  output as a ``DataTags`` entry, like a connector's source tags
  (:func:`build_catalogue`), and it is commissioned the same way
  (``signal/autobind`` or an editor). The node writes the ``_Signal`` and mints
  its id, which the output finds by its ``data_tag``
  (:func:`chaski.dataops.resolve.resolve_output_binding`). Until then
  ``publish`` idles with a rate-limited log.

* ``AnnotationOutput``: publishes ``_Annotation`` records on the
  ``annotations`` stream. ``write_interval`` derives the id from
  ``(annotation_type_id, source, time_start, signal_ids)``, so publishing an
  interval again updates it, and ``delete`` removes it. Both also take the
  ``annotation_id`` a write returned, which is how a producer keeping that id
  updates an annotation whose start or signal set has moved since.
  ``clear_window`` publishes delete markers only for
  ids this output recorded emitting in the buffer, so a producer cannot delete
  annotations it did not write.

Both are declared as class attributes and used through the instance
(``self.oee.publish(...)``), like inputs. The service binds them after every
producer's ``setup()`` (:func:`build_catalogue`,
:func:`bind_annotation_outputs`).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import httpx
from colca_data_contracts import (
    AnnotationPayload,
    DataTag,
    Metric,
    derive_annotation_id,
)
from franzmq import Topic

from . import resolve
from .inputs import _PerInstance

if TYPE_CHECKING:
    from chaski.catalogue import Catalogue

log = logging.getLogger("chaski.dataops.outputs")

# How often an unbound output logs that it is idle; publish() may run every tick.
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

    ``element`` is the node-local path the node places the signal under, so
    one service can compute for several machines. ``unit``,
    ``semantic_type`` and ``description`` are applied to the signal the node
    binds to the tag, and updated there when they change. Each is left out
    when the output names none.
    """
    meta: dict[str, Any] = {}
    if output.description:
        meta["description"] = output.description
    if output.system_element_name:
        meta["element"] = output.system_element_name
    if output.unit:
        meta["unit"] = output.unit
    if output.semantic_type:
        meta["semantic_type"] = output.semantic_type
    return meta


class SignalOutput(_PerInstance):
    """Output bound to one catalogued signal.

    ``system_element_name`` is the node-local path of the element the output
    belongs under (``Roasting/drum-roaster-01``). It travels as
    ``meta.element`` and tells the node where to place the signal; it is not
    used to resolve anything here.

    ``unit``, ``semantic_type`` (the name of a semantic tag the node knows,
    such as ``availability``) and ``description`` travel in the catalogue
    entry. The node applies them to the signal it binds to this output and
    follows later changes: the producer owns its outputs' metadata.
    """

    def __init__(
        self,
        signal_name: str,
        data_type: str,
        description: str = "",
        system_element_name: str | None = None,
        *,
        unit: str | None = None,
        semantic_type: str | None = None,
    ):
        self.signal_name = signal_name
        self.data_type = data_type
        self.description = description
        self.system_element_name = system_element_name
        self.unit = unit
        self.semantic_type = semantic_type
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
        """Attach this run's catalogue identity. Called by
        :func:`build_catalogue`, not by producer code."""
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
        """Publish one ``Metric`` at the position this output is bound to.

        The binding is a published ``_Signal`` whose ``data_tag`` is
        ``self.tag_id``. Until one exists this is a no-op with a rate-limited
        log.
        """
        door = self._runtime().door
        # A found binding is kept until the next pass calls forget(). A miss is
        # not kept: binding happens elsewhere and nothing notifies us.
        try:
            binding = self._binding or resolve.resolve_output_binding(door, self.tag_id)
        except httpx.HTTPStatusError as refused:
            if refused.response.status_code != 429:
                raise
            # A 429 means ask again later: skip this value and try on the next
            # publish instead of failing the producer's tick.
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
    """Interval annotation output, publishing ``_Annotation`` records.

    For a point annotation, pass ``time_end=time_start``.
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
        """Attach this output's identity. Called by
        :func:`bind_annotation_outputs`, not by producer code; binding once
        saves a lookup on every write.
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
        """This annotation type's ULID, resolved from KV on every call. Raises
        if no ``_AnnotationType`` named ``self.annotation_name`` exists; types
        are definitions and are not created here.
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
        annotation_id: str | None = None,
        system_element_id: str | None = None,
        related_annotation_ids: Iterable[str] | None = None,
    ) -> str:
        """Publish (create or update) one annotation and return its id.

        The id is derived from ``(annotation_type_id, source, time_start,
        signal_ids)``, so publishing the same interval again, for example to
        set ``time_end``, updates it.

        ``annotation_id`` addresses an annotation this output already wrote,
        by the id that write returned, and is how a producer updates one
        whose start or signal set has moved since — both are part of the
        derivation, so re-deriving would name a different annotation and
        leave the original as it was. A producer keeping the returned id
        with whatever it is projecting (a session, an order) never has to
        reproduce the derivation's inputs at all.

        ``signal_ids`` are what the annotation was computed from.
        ``system_element_id`` is where it belongs, and every signal must lie
        below that element: a signal the node's KV places elsewhere raises
        ``ValueError`` and nothing is published. ``related_annotation_ids``
        are the annotations this one belongs to, such as the panel a head
        pass is part of. Neither is part of the id.
        """
        runtime = self._runtime()
        source = self._require_source()

        ts_start = _epoch(time_start)
        ts_end = _epoch(time_end) if time_end is not None else None
        signals = list(signal_ids or [])
        type_id = self.type_id
        if system_element_id is not None and signals:
            outside = resolve.signals_outside_element(runtime.door, system_element_id, signals)
            if outside:
                raise ValueError(
                    f"AnnotationOutput[{self.annotation_name!r}]: signal(s) {outside} are not below "
                    f"system element {system_element_id!r}"
                )
        if annotation_id is None:
            annotation_id = derive_annotation_id(type_id, source, ts_start, signals)

        payload = AnnotationPayload(
            annotation_id=annotation_id,
            annotation_type_id=type_id,
            time_start=ts_start,
            time_end=ts_end,
            value=value,
            signal_ids=signals,
            source=source,
            system_element_id=system_element_id,
            related_annotation_ids=list(related_annotation_ids or []),
        )
        runtime.door.publish(self._topic(annotation_id), payload.encode())
        runtime.buffer.record_emitted_annotation(source, annotation_id, ts_start)
        log.debug("AnnotationOutput[%s]: published %s @ %s..%s", self.annotation_name, annotation_id, ts_start, ts_end)
        return annotation_id

    def delete(
        self,
        time_start: Any = None,
        signal_ids: Iterable[str] | None = None,
        annotation_id: str | None = None,
    ) -> str:
        """Publish a delete marker for one annotation — the one
        ``write_interval`` wrote for this ``time_start`` and signal set, or
        the one ``annotation_id`` names — and return its id.

        Deriving the id takes this output's own ``source`` as half of it, so
        only an annotation this producer wrote can be named that way, and an
        id it returned is its own by the same argument. Unlike
        ``clear_window`` neither form needs a buffer record, and neither
        takes out every other annotation that started in the same window.
        """
        runtime = self._runtime()
        source = self._require_source()
        type_id = self.type_id

        if time_start is None and annotation_id is None:
            raise TypeError(
                "AnnotationOutput.delete needs the annotation's time_start, or the annotation_id it was written under"
            )
        # A marker's own time_start is not read by any consumer — the id is
        # the identity — so deleting by id need not know when it started.
        ts_start = _epoch(time_start) if time_start is not None else 0.0
        signals = list(signal_ids or [])
        if annotation_id is None:
            annotation_id = derive_annotation_id(type_id, source, ts_start, signals)
        payload = AnnotationPayload(
            annotation_id=annotation_id,
            annotation_type_id=type_id,
            time_start=ts_start,
            signal_ids=signals,
            source=source,
            deleted=True,
        )
        runtime.door.publish(self._topic(annotation_id), payload.encode())
        log.debug("AnnotationOutput[%s]: deleted %s @ %s", self.annotation_name, annotation_id, ts_start)
        return annotation_id

    def clear_window(self, start: Any, end: Any) -> int:
        """Publish a delete marker for every annotation this output emitted with
        ``time_start`` in ``[start, end)``, as recorded in the buffer. Returns
        the number of markers published.
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


# ─── catalogue: SignalOutput provisioning ──────────────────────────────────


def _iter_signal_outputs(instances: Iterable[Any]) -> Iterable[tuple[str, SignalOutput, Any]]:
    """Yield ``(source, bound_output, instance)`` for every ``SignalOutput``
    declared on the classes of ``instances``.

    ``source`` is ``"{producer.name}.{attr_name}"``, stable across restarts so
    tag ids are reused.
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


def build_catalogue(instances: Iterable[Any], catalogue: Catalogue) -> dict[str, str]:
    """Declare every ``SignalOutput`` on ``instances`` into the service's
    catalogue and bind each output to its tag id.

    ``catalogue`` is the one the service loaded from the node at start, so ids
    are kept by ``source`` across restarts, new sources mint, and sources no
    longer declared stay in the catalogue marked ``is_stale`` so a signal bound
    to one stays bound. Publishing is the service's
    (:meth:`chaski.DataOpsService.bind_outputs`).

    Returns ``{source: tag_id}`` for every declared output.
    """
    declared = list(_iter_signal_outputs(instances))
    catalogue.declare(
        {
            source: DataTag(
                id="",
                name=output_attr.signal_name,
                source=source,
                # Only the producer writes a computed output; anyone may read it.
                is_writable=False,
                is_readable=True,
                data_type=output_attr.data_type,
                is_stale=False,
                meta=_catalogue_meta(output_attr),
            )
            for source, output_attr, _instance in declared
        }
    )
    result: dict[str, str] = {}
    for source, output_attr, _instance in declared:
        tag_id = catalogue.tag_id(source)
        if tag_id is None:  # declare() keeps every declared source
            raise RuntimeError(f"chaski: {source} is missing from the catalogue it was just declared into")
        output_attr.bind(source, tag_id)
        result[source] = tag_id
    return result


# ─── AnnotationOutput binding ──────────────────────────────────────────────


def bind_annotation_outputs(
    instances: Iterable[Any],
    *,
    node_id: str,
    mount: str,
) -> None:
    """Bind every declared ``AnnotationOutput`` on ``instances`` to a stable
    ``source`` and ``_Annotation`` topic prefix.

    ``source`` is ``"dataops/{producer.name}"``, stable across restarts so
    annotation ids are too. The topic prefix is the mount, the producer's name
    and the attribute name.
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
