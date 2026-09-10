"""Input descriptors for Producer classes.

Declared inputs read exclusively from the runtime's own buffer
(:class:`chaski.dataops.Buffer`) — never from a live MQTT cache
(the dataops evaluator design §3): ticks and
``@on_metric`` handlers alike see the same source-transparent view, live,
on replay, and in deep backfill. Resolution (signal name + optional system
element -> Signal ULID) reads colca's KV projection fresh on every
resolution pass via :mod:`chaski.dataops.resolve` — see that module's
docstring for why nothing here caches a resolved id across passes.

**Per-instance, through the producer's runtime.** An input is declared as
a class attribute and read through the instance::

    class MyMachine(Producer):
        part_counter = SignalRangeInput("part_counter", window="1h")

        @on_metric("part_counter")
        async def recompute(self, metric):
            if self.part_counter.is_fresh(120):
                ...

        # backfill / point-in-time reads use the exact same input:
        def compute_status_at(self, t):
            return self.part_counter.latest_value_before(t)

``self.part_counter`` is a per-instance copy of the declaration (a plain
descriptor: the first access on an instance stores the copy in that
instance's ``__dict__``), and it reaches the door, the buffer and the
optional historian through ``self.runtime`` — the
:class:`~chaski.dataops.base.Runtime` the producer is attached to. This
replaces the module-level ``bind(door, buffer)`` the shipped ``dataops``
service used: correct for one container, wrong for an SDK where two
services may share a process (service families design §3.5).
Read on the class (``MyMachine.part_counter``) the descriptor is the
declaration itself — what :func:`validate_windows` and the dispatch
builder walk.

**The historian is a port, not a dependency.** :class:`Historian` names the
two reads an optional, read-only historian must answer (design §7). The
SDK never imports a database driver: the shipped ``dataops`` image
implements this port over TimescaleDB (``dataops/historian.py``), and a
runtime with ``historian=None`` — the default — serves every read from the
buffer alone.
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
    """An optional, READ-ONLY source of historised points older than the
    buffer holds (evaluator design §7). Timestamps are unix seconds, the
    same unit as every buffer read; a frame has columns ``ts`` and
    ``value``, the same shape as :meth:`chaski.dataops.Buffer.window`.
    """

    def window(self, signal_id: str, start: float, end: float) -> pd.DataFrame:
        """Points with ``start <= ts < end`` (half-open), ordered by ts."""
        ...

    def latest_before(self, signal_id: str, before: float) -> tuple[float, Any] | None:
        """``(ts, value)`` of the most recent point with ``ts <= before``, or ``None``."""
        ...


class _PerInstance:
    """The descriptor half every declared input/output shares: read on the
    class it is the declaration; read on an instance it is that instance's
    own copy, created once and kept in the instance's ``__dict__`` under
    the declared attribute name (a non-data descriptor, so the instance
    entry wins on every later access — and a test may replace it with a
    stand-in by plain assignment).
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
    """Read one signal's buffered values, resolved by name (+ optional element).

    Pass ``system_element_name`` to scope resolution to a specific
    SystemElement — required on multi-machine hubs where the same signal
    name lives on multiple SEs.

    ``window`` declares how far back this input needs to reach (a plain
    number of seconds, or a string like ``"24h"``, ``"7d"``) — it sizes the
    per-signal horizon the service trims the buffer to, and is checked at
    startup by :func:`validate_windows`: a window longer than the broker's
    own retention, with no historian configured to cover the gap, is a
    startup error rather than a producer silently seeing an emptier and
    emptier frame over time.
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

        Called by the service at the top of every resolution pass, which is
        what keeps a rebind visible: the pass runs at startup and then on
        `reresolve_loop`'s cadence, and that IS this service's re-resolution
        rate — the dispatch table it builds is keyed by the ids it resolved,
        so nothing downstream could act on a fresher answer anyway.
        """
        self._resolved_id = None

    @property
    def signal_id(self) -> str:
        """This input's Signal ULID, resolved once per resolution pass.

        It used to resolve on EVERY read, and this is read per metric: the
        ingest loop asks for it for each record it applies, and each ask is a
        full KV scan — twice over, since `resolve_signal` looks up the element
        first. colca serves /kv at five a second because it is a SCAN, so a
        node ingesting a few hundred metrics a second answered most of them
        with HTTP 429, and every one surfaced as a handler that failed on a
        signal it had already resolved successfully at startup.

        The dispatch table is keyed by exactly this id, so re-deriving it per
        record could not have changed any routing decision — it was work with
        no possible effect but to fail.
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
        """Points with ``start <= ts < end`` (half-open, matching
        :meth:`Buffer.window`).

        Served from the buffer. The portion of the range older than the
        buffer's earliest retained point for this signal is served from
        the historian when one is configured; when the buffer holds
        nothing at all for this signal, the historian is consulted if
        configured, otherwise the honest answer is an empty frame — buffer
        state alone can't tell "nothing was ever produced" apart from
        "not ingested yet". A range that demonstrably predates the
        buffer's own retained window, with no historian configured to
        cover it, is a clear error rather than a silently incomplete
        frame (design §4.3, §7).
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
        """Value of the most recent point with ``ts <= before``, source-
        transparent over buffer then historian. ``None`` if neither holds
        one — never an error (unlike :meth:`fetch`, there is no declared
        range to be provably incomplete against)."""
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

        What a producer clamps a trailing window's start to on a node with
        no historian: :meth:`fetch` refuses a range that reaches before this
        point rather than silently serving less than was asked for, so a
        window-based computation (an OEE over the last hour, on a service
        that started ten minutes ago) asks first and computes over what
        there is.
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


# ─── startup validation (design §4.1) ──────────────────────────────────────


class WindowExceedsRetentionError(RuntimeError):
    """A declared input window exceeds broker retention with no historian
    configured to cover the gap.

    Raised at startup, never mid-run: the alternative is a producer whose
    windowed reads silently grow emptier as the buffer's actual retained
    window falls short of what it declared, with nothing telling the
    operator why.
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
    """``(attr_name, bound_input)`` for every ``SignalRangeInput`` declared
    on ``instance``'s class — the one enumeration the dispatch builder, the
    trim-horizon computation and the resolution pass share, so a ticking-
    only input is counted everywhere a windowed one is."""
    cls = type(instance)
    for attr_name in dir(cls):
        class_attr = getattr(cls, attr_name, None)
        if isinstance(class_attr, SignalRangeInput):
            yield attr_name, getattr(instance, attr_name)
