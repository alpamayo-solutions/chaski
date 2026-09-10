"""Input descriptors for Producer classes.

Inputs read only from the runtime's buffer (:class:`chaski.dataops.Buffer`),
never from a live MQTT cache, so ticks and ``@on_metric`` handlers see the same
data live, on replay and in backfill. Names resolve to Signal ULIDs through
:mod:`chaski.dataops.resolve` on every resolution pass.

An input is declared as a class attribute and used through the instance::

    class MyMachine(Producer):
        part_counter = SignalRangeInput("part_counter", window="1h")

        @on_metric("part_counter")
        async def recompute(self, metric):
            if self.part_counter.is_fresh(120):
                ...

        # backfill / point-in-time reads use the exact same input:
        def compute_status_at(self, t):
            return self.part_counter.latest_value_before(t)

``self.part_counter`` is a per-instance copy of the declaration, created on
first access, that reaches the door, buffer and historian through
``self.runtime``. On the class (``MyMachine.part_counter``) it is the
declaration itself.

:class:`Historian` is an optional, read-only port for points older than the
buffer. The SDK ships no database driver; with ``historian=None`` every read
comes from the buffer.
"""

from __future__ import annotations

import copy
import logging
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import pandas as pd

from . import resolve
from .triggers import parse_duration

if TYPE_CHECKING:
    from .base import Producer, Runtime

log = logging.getLogger("chaski.dataops.inputs")

DEFAULT_WINDOW = "1h"


@runtime_checkable
class Historian(Protocol):
    """An optional, read-only source of points older than the buffer holds.
    Timestamps are unix seconds; frames have columns ``ts`` and ``value``, like
    :meth:`chaski.dataops.Buffer.window`.
    """

    def window(self, signal_id: str, start: float, end: float) -> pd.DataFrame:
        """Points with ``start <= ts < end`` (half-open), ordered by ts."""
        ...

    def latest_before(self, signal_id: str, before: float) -> tuple[float, Any] | None:
        """``(ts, value)`` of the most recent point with ``ts <= before``, or ``None``."""
        ...


class _PerInstance:
    """Descriptor shared by inputs and outputs: on the class it is the
    declaration, on an instance a copy kept in the instance's ``__dict__``.
    A test can replace it by plain assignment.
    """

    _attr_name: str | None = None
    _owner: Producer | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        self._attr_name = name

    def __get__(self, instance: Any, owner: type | None = None):
        if instance is None:
            return self
        name = self._attr_name or _find_attr_name(owner or type(instance), self)
        bound = copy.copy(self)
        bound._owner = instance
        instance.__dict__[name] = bound
        return bound

    def _runtime(self) -> Runtime:
        owner = self._owner
        if owner is None:
            raise RuntimeError(
                f"{type(self).__name__} is the class-level declaration — read it through a "
                "producer instance (self.<attr>) so it can reach that producer's runtime"
            )
        return owner.runtime


def _find_attr_name(owner: type, descriptor: Any) -> str:
    """The attribute a descriptor was assigned to when ``__set_name__``
    never ran (assigned after class creation with ``setattr``)."""
    for klass in owner.__mro__:
        for name, value in vars(klass).items():
            if value is descriptor:
                return name
    raise AttributeError(f"{descriptor!r} is not a class attribute of {owner.__name__}")


class SignalRangeInput(_PerInstance):
    """Read one signal's buffered values, resolved by name and optional element.

    Pass ``system_element_name`` when the same signal name exists on several
    elements.

    ``window`` is how far back the input reaches, in seconds or as ``"24h"``,
    ``"7d"``. It sizes the buffer horizon, and :func:`validate_windows` refuses
    at startup a window longer than broker retention without a historian.
    """

    def __init__(
        self,
        signal_name: str,
        system_element_name: str | None = None,
        window: str | float = DEFAULT_WINDOW,
    ) -> None:
        self.signal_name = signal_name
        self.system_element_name = system_element_name
        self.window_s = parse_duration(window)
        self._resolved_id: str | None = None

    def forget(self) -> None:
        """Drop the resolved id so the next read resolves again.

        The service calls this at the start of every resolution pass, so a
        rebind is picked up on the next pass.
        """
        self._resolved_id = None

    @property
    def signal_id(self) -> str:
        """This input's Signal ULID, resolved once per resolution pass.

        It is read for every record, and resolving is a rate-limited KV scan,
        so it is not resolved again until :meth:`forget`.
        """
        if self._resolved_id is not None:
            return self._resolved_id
        door = self._runtime().door
        sid = resolve.resolve_signal(door, self.signal_name, self.system_element_name)
        if sid is None:
            scope = f" on SE={self.system_element_name!r}" if self.system_element_name else ""
            raise LookupError(f"Signal not found: {self.signal_name!r}{scope}")
        self._resolved_id = sid
        return sid

    # ─── source-transparent reads: buffer, historian iff configured ────

    def fetch(self, start: float, end: float) -> pd.DataFrame:
        """Points with ``start <= ts < end``, like :meth:`Buffer.window`.

        The part of the range older than the buffer's earliest point comes
        from the historian if there is one. Without a historian, an empty
        buffer gives an empty frame, and a range reaching before the buffer's
        earliest point raises instead of returning an incomplete frame.
        """
        runtime = self._runtime()
        buffer = runtime.buffer
        historian = runtime.historian
        signal_id = self.signal_id
        buf_earliest = buffer.earliest(signal_id)

        if buf_earliest is None:
            if historian is not None:
                return historian.window(signal_id, start, end)
            return pd.DataFrame(columns=["ts", "value"])

        if start < buf_earliest:
            if historian is None:
                raise RuntimeError(
                    f"fetch({self.signal_name!r}): requested range starts at {start}, "
                    f"before the buffer's earliest retained point ({buf_earliest}) for "
                    "this signal, and no historian is configured to serve the gap"
                )
            hist_end = min(end, buf_earliest)
            hist_df = historian.window(signal_id, start, hist_end)
            if buf_earliest < end:
                buf_df = buffer.window(signal_id, buf_earliest, end)
                return pd.concat([hist_df, buf_df], ignore_index=True)
            return hist_df

        return buffer.window(signal_id, start, end)

    def latest_value_before(self, before: float) -> Any:
        """Value of the most recent point with ``ts <= before``, from the buffer
        or else the historian; ``None`` if neither has one."""
        row = self._latest_before(before)
        return row[1] if row is not None else None

    def latest_timestamp_before(self, before: float) -> float | None:
        """Timestamp of the most recent point with ``ts <= before``, or
        ``None``. Same source-transparent rule as :meth:`latest_value_before`."""
        row = self._latest_before(before)
        return row[0] if row is not None else None

    def _latest_before(self, before: float) -> tuple[float, Any] | None:
        runtime = self._runtime()
        signal_id = self.signal_id
        row = runtime.buffer.latest_before(signal_id, before)
        if row is not None:
            return row
        if runtime.historian is None:
            return None
        return runtime.historian.latest_before(signal_id, before)

    @property
    def earliest_timestamp(self) -> float | None:
        """The oldest point the buffer holds for this signal, or ``None``.

        Without a historian, clamp a window's start to this, since
        :meth:`fetch` refuses a range that reaches further back.
        """
        return self._runtime().buffer.earliest(self.signal_id)

    @property
    def latest_value(self) -> Any:
        """The value in force right now (``latest_value_before(now)``)."""
        return self.latest_value_before(time.time())

    @property
    def latest_timestamp(self) -> float | None:
        """The timestamp of :attr:`latest_value`, or ``None``."""
        return self.latest_timestamp_before(time.time())

    def is_fresh(self, freshness_s: float, now: float | None = None) -> bool:
        """True iff the most recent point at/before ``now`` is at most
        ``freshness_s`` seconds old. Used by state-machine producers to
        gate decisions like "heartbeat is alive"."""
        now = now if now is not None else time.time()
        ts = self.latest_timestamp_before(now)
        if ts is None:
            return False
        return (now - ts) <= freshness_s


# ─── startup validation ─────────────────────────────────────────────────────


class WindowExceedsRetentionError(RuntimeError):
    """A declared input window exceeds broker retention and no historian covers
    the gap. Raised at startup.
    """


def validate_windows(
    producers: Iterable[type | Any],
    retention_s: float,
    historian_configured: bool,
) -> None:
    """Raise :class:`WindowExceedsRetentionError` for the first declared
    input whose ``window`` exceeds ``retention_s``, unless a historian is
    configured to cover the gap.

    ``producers`` is an iterable of ``Producer`` subclasses (or instances —
    either works). Every ``SignalRangeInput`` reachable through the class's
    MRO is checked, including ones declared on an abstract base (e.g.
    ``MachineState.heartbeat``) and inherited by a concrete subclass.
    """
    if historian_configured:
        return
    for producer in producers:
        cls = producer if isinstance(producer, type) else type(producer)
        for attr_name in dir(cls):
            attr = getattr(cls, attr_name, None)
            if not isinstance(attr, SignalRangeInput):
                continue
            if attr.window_s > retention_s:
                raise WindowExceedsRetentionError(
                    f"{cls.__name__}.{attr_name} (signal={attr.signal_name!r}) declares "
                    f"window={attr.window_s:.0f}s, but the broker retains only "
                    f"{retention_s:.0f}s of metrics and no historian is configured to "
                    "cover the gap"
                )


def declared_inputs(instance: Any) -> Iterable[tuple[str, SignalRangeInput]]:
    """``(attr_name, bound_input)`` for every ``SignalRangeInput`` declared on
    ``instance``'s class."""
    cls = type(instance)
    for attr_name in dir(cls):
        class_attr = getattr(cls, attr_name, None)
        if isinstance(class_attr, SignalRangeInput):
            yield attr_name, getattr(instance, attr_name)
