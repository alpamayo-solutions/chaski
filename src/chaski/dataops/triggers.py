"""Trigger decorators for Producer methods.

A trigger is attached to a method via decorator. The decorator only stores
metadata on the method (``__colca_triggers__``); actual scheduling happens
in the service runtime when the producer is instantiated.

Multiple triggers can stack on the same method — the method fires on
whichever event arrives first.

    @trigger.cron("*/15 * * * *")
    async def compute(self):
        ...

    @trigger.every("30s")
    @trigger.cron("0 6 * * *")
    async def heartbeat(self):
        ...
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class CronSpec:
    """Cron-style trigger spec. Five fields: min hour dom month dow."""

    expression: str


@dataclass(frozen=True)
class IntervalSpec:
    """Fixed-interval trigger spec, e.g. every 30 seconds."""

    seconds: float


@dataclass(frozen=True)
class OnMetricSpec:
    """Event-driven trigger: fire for every new Metric of one of the producer's
    declared inputs.

    ``input_name`` is the attribute name of the ``SignalRangeInput`` on the
    producer class (e.g. ``"part_counter"``). The decorated method receives the
    decoded Metric. ``@on_metric`` decorators may stack on one method.
    """

    input_name: str


@dataclass(frozen=True)
class OnConstantSpec:
    """Event-driven trigger: fire for every ``_Constant`` record at a
    node-local path matching ``path_or_pattern``.

    ``path_or_pattern`` is an MQTT topic filter over the node-local path: an
    exact path (``"line1/operator/activeRecipeId"``) matches only itself;
    ``+`` matches exactly one path segment and ``#`` matches it and
    everything under it (``"catalog/#"``), the same wildcards any MQTT
    subscription accepts. The node retains the current record at its own
    topic, so subscribing also delivers whatever is already there — a
    producer sees a constant that was set before it started without waiting
    for a change.

    The decorated method receives the decoded
    :class:`colca_data_contracts.Constant`, or ``None`` when the record was
    retired: an empty retained payload is the wire tombstone for "this path
    has no value anymore", the same convention ``_Metric``/``_Signal`` use.
    ``@on_constant`` decorators may stack on one method, like ``@on_metric``.
    """

    path_or_pattern: str


@dataclass(frozen=True)
class OnSignalSpec:
    """Event-driven trigger: fire whenever a ``_Signal`` binding appears,
    changes or disappears at a node-local path matching ``path_or_pattern`` —
    an integration taking over a field, changing what it reads from, or
    releasing it back to operator/catalog input. Same path/pattern semantics
    as :class:`OnConstantSpec`.

    The decorated method receives the decoded
    :class:`colca_data_contracts.Signal`, or ``None`` when the binding was
    retired (the wire tombstone).
    """

    path_or_pattern: str


# Methods carry ``__colca_triggers__: list[TriggerSpec]`` once decorated.
TriggerSpec = CronSpec | IntervalSpec | OnMetricSpec | OnConstantSpec | OnSignalSpec
_TRIGGERS_ATTR = "__colca_triggers__"


def _attach(method: Callable, spec: TriggerSpec) -> Callable:
    """Append a trigger spec to the method's marker list (creating it on first use)."""
    existing = getattr(method, _TRIGGERS_ATTR, None)
    if existing is None:
        existing = []
        setattr(method, _TRIGGERS_ATTR, existing)
    existing.append(spec)
    return method


def cron(expression: str) -> Callable[[Callable], Callable]:
    """Schedule a method on a cron expression. Five fields: ``min hour dom month dow``.

    Example:
        ``@cron("*/15 * * * *")`` — every quarter hour, on the quarter
        ``@cron("0 6 * * 1-5")`` — 06:00 on weekdays
    """
    # Cheap sanity check, real parsing happens in apscheduler
    if len(expression.split()) != 5:
        raise ValueError(f"Cron expression must have 5 fields, got {expression!r}")

    def decorator(fn: Callable) -> Callable:
        return _attach(fn, CronSpec(expression=expression))

    return decorator


_INTERVAL_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)?\s*$")
_UNIT_TO_SECONDS = {"ms": 1 / 1000, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(duration: str | float | int) -> float:
    """Parse a duration into seconds.

    Accepts a numeric seconds value or a string like ``"30s"``, ``"15m"``,
    ``"2h"``, ``"500ms"``, ``"7d"``. Used by :func:`every` and by
    ``SignalRangeInput``'s ``window``.
    """
    if isinstance(duration, bool):
        raise TypeError(f"`duration` must be str or number, got {type(duration).__name__}")
    if isinstance(duration, (int, float)):
        seconds = float(duration)
    elif isinstance(duration, str):
        match = _INTERVAL_RE.match(duration)
        if not match:
            raise ValueError(f"Invalid duration {duration!r} (expected '30s', '15m', '2h', '7d', ...)")
        value, unit = match.group(1), match.group(2) or "s"
        seconds = float(value) * _UNIT_TO_SECONDS[unit]
    else:
        raise TypeError(f"`duration` must be str or number, got {type(duration).__name__}")

    if seconds <= 0:
        raise ValueError(f"Duration must be positive, got {seconds}")

    return seconds


def every(duration: str | float | int) -> Callable[[Callable], Callable]:
    """Schedule a method at a fixed interval.

    ``duration`` accepts a numeric seconds value or a string like ``"30s"``,
    ``"15m"``, ``"2h"``, ``"500ms"``.
    """
    seconds = parse_duration(duration)

    def decorator(fn: Callable) -> Callable:
        return _attach(fn, IntervalSpec(seconds=seconds))

    return decorator


def on_metric(input_name: str) -> Callable[[Callable], Callable]:
    """Fire whenever the named declared input receives a new Metric.

    The argument is the attribute name of a ``SignalRangeInput`` on the
    producer class (not the underlying signal name). Stacks with other
    triggers — the method runs on any matching event.

    Example::

        class MyMachine(Producer):
            part_counter = SignalRangeInput(...)
            error_code   = SignalRangeInput(...)

            @on_metric("part_counter")
            @on_metric("error_code")
            async def recompute(self, metric):
                ...

    The handler receives the decoded :class:`colca_data_contracts.Metric` as
    its only positional argument. The record is already in the buffer when the
    handler runs.
    """
    if not input_name or not isinstance(input_name, str):
        raise ValueError(f"input_name must be a non-empty string, got {input_name!r}")

    def decorator(fn: Callable) -> Callable:
        return _attach(fn, OnMetricSpec(input_name=input_name))

    return decorator


def _check_path_or_pattern(path_or_pattern: str) -> str:
    if not path_or_pattern or not isinstance(path_or_pattern, str):
        raise ValueError(f"path_or_pattern must be a non-empty string, got {path_or_pattern!r}")
    return path_or_pattern


def on_constant(path_or_pattern: str) -> Callable[[Callable], Callable]:
    """Fire whenever a ``_Constant`` record at ``path_or_pattern`` is written
    or retired — an operator or catalog value chaski has no declared-input
    abstraction for (a ``_Constant`` is not read as a windowed input the way
    a ``_Signal``'s ``_Metric`` history is).

    ``path_or_pattern`` is a node-local path, or an MQTT filter over one
    (``+``/``#``) — see :class:`~chaski.dataops.triggers.OnConstantSpec`.
    Stacks with other triggers, ``@on_constant`` included, so one method can
    watch several paths.

    Example::

        class Recipe(Producer):
            @on_constant("line1/operator/activeRecipeId")
            @on_constant("catalog/recipes/#")
            async def recompute(self, constant):
                ...  # `constant` is None when the record was retired

    Delivery is retained: subscribing also delivers whatever is already set,
    so a producer sees today's operator/catalog state without waiting for
    the next write.
    """
    spec = OnConstantSpec(path_or_pattern=_check_path_or_pattern(path_or_pattern))

    def decorator(fn: Callable) -> Callable:
        return _attach(fn, spec)

    return decorator


def on_signal(path_or_pattern: str) -> Callable[[Callable], Callable]:
    """Fire whenever a ``_Signal`` binding at ``path_or_pattern`` appears,
    changes or disappears — an integration taking over a field or releasing
    it, which changes who owns a value without chaski ever seeing a new
    ``_Metric``.

    Same path/pattern and retained-delivery semantics as :func:`on_constant`;
    see :class:`~chaski.dataops.triggers.OnSignalSpec`. The decorated method
    receives the decoded ``Signal``, or ``None`` when the binding was
    retired.
    """
    spec = OnSignalSpec(path_or_pattern=_check_path_or_pattern(path_or_pattern))

    def decorator(fn: Callable) -> Callable:
        return _attach(fn, spec)

    return decorator
