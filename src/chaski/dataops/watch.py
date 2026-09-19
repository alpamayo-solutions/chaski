"""``@on_constant``/``@on_signal`` dispatch — the MQTT-driven complement to
``@on_metric``'s stream-driven dispatch (:mod:`chaski.dataops.service`).

Neither ``_Constant`` nor ``_Signal`` has a durable stream like ``metrics``
(:meth:`chaski.door.Door.fetch` only serves ``stream="metrics"``), so there is
nothing here to poll, buffer or replay. The node retains the current record at
its own topic instead, and MQTT's own retained-message rule means a fresh
subscription is handed that record immediately, and every later write again,
a retired record's empty payload included. A subscription is the whole
mechanism: no cursor, no ack, no local buffer.

:func:`start` turns every producer's ``@on_constant``/``@on_signal`` triggers
(:class:`~chaski.dataops.triggers.OnConstantSpec`,
:class:`~chaski.dataops.triggers.OnSignalSpec`) into one MQTT subscription per
``(contract, path_or_pattern, handler)``, scoped to the service's own node.
franzmq decodes each message against the declared contract and hands the
tombstone case to the handler as ``None``, so this module does no decoding of
its own — the crash a hand-rolled subscription like this used to risk (an
unconditional ``json.loads`` on a tombstone's empty payload, killing the whole
MQTT client) is guarded once, for every subscription this service makes,
by :func:`chaski.dataops.service.ring_even_if_undecodable`.

Delivery happens on franzmq's own callback thread (:class:`franzmq.Client`
runs a message's registered callbacks off the network thread already); each
match is handed to the producer's event loop with ``call_soon_threadsafe``,
run under the producer's lock and a pinned :func:`chaski.dataops.resolve.one_pass`
snapshot — the same two guarantees
:func:`chaski.dataops.service.make_handler` gives ``@on_metric``, so a handler
that publishes several outputs costs one KV read, not one per output.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from typing import Any

from colca_data_contracts.payload import Constant, Signal
from franzmq import Topic

from . import resolve
from .base import Producer
from .triggers import OnConstantSpec, OnSignalSpec

log = logging.getLogger("chaski.dataops.watch")

#: qos=1: a missed `_Constant`/`_Signal` write is a missed decision, unlike
#: the doorbell (qos=0), which only wakes a poll that runs again regardless.
_QOS = 1


def gather_triggers(instances: Iterable[Producer]) -> tuple[list[tuple[str, Callable]], list[tuple[str, Callable]]]:
    """``(constant_triggers, signal_triggers)``: every ``(path_or_pattern,
    bound_handler)`` declared across ``instances`` via ``@on_constant``/
    ``@on_signal``. Bound to each producer instance, like
    :func:`chaski.dataops.service.build_dispatch` binds ``@on_metric``
    handlers."""
    constant_triggers: list[tuple[str, Callable]] = []
    signal_triggers: list[tuple[str, Callable]] = []
    for instance in instances:
        for method_name, spec in type(instance)._triggers:
            if isinstance(spec, OnConstantSpec):
                constant_triggers.append((spec.path_or_pattern, getattr(instance, method_name)))
            elif isinstance(spec, OnSignalSpec):
                signal_triggers.append((spec.path_or_pattern, getattr(instance, method_name)))
    return constant_triggers, signal_triggers


def start(
    client: Any,
    node_id: str,
    door: Any,
    loop: asyncio.AbstractEventLoop,
    constant_triggers: list[tuple[str, Callable]],
    signal_triggers: list[tuple[str, Callable]],
) -> int:
    """Subscribe every ``(path_or_pattern, handler)`` pair on ``client``,
    scoped to ``node_id``. Returns how many subscriptions were made.

    Safe to call with two empty lists — nothing subscribes, so a service with
    no ``@on_constant``/``@on_signal`` producer opens no extra subscription.
    ``door`` is read fresh (:func:`chaski.dataops.resolve.one_pass`) for every
    dispatched message, not held onto otherwise.
    """
    if not constant_triggers and not signal_triggers:
        return 0
    # Imported here, not at module level: chaski.dataops.service imports this
    # module to call start() from DataOpsService.serve(), so a top-level
    # import back the other way would be circular.
    from .service import ring_even_if_undecodable

    ring_even_if_undecodable(client)
    count = 0
    for path_or_pattern, handler in constant_triggers:
        _subscribe(client, Constant, node_id, path_or_pattern, handler, door, loop)
        count += 1
    for path_or_pattern, handler in signal_triggers:
        _subscribe(client, Signal, node_id, path_or_pattern, handler, door, loop)
        count += 1
    log.info(
        "watch: %d @on_constant and %d @on_signal subscription(s) on node=%s",
        len(constant_triggers),
        len(signal_triggers),
        node_id,
    )
    return count


def _subscribe(
    client: Any,
    payload_type: type,
    node_id: str,
    path_or_pattern: str,
    handler: Callable,
    door: Any,
    loop: asyncio.AbstractEventLoop,
) -> None:
    topic = Topic(payload_type=payload_type, node_id=node_id, context=tuple(path_or_pattern.split("/")))
    callback = _callback(handler, door, loop)
    client.subscribe(topic, qos=_QOS, callback=callback)
    log.debug("watching %s for %s", topic, getattr(handler, "__qualname__", handler))


def _callback(handler: Callable, door: Any, loop: asyncio.AbstractEventLoop) -> Callable[[Any], None]:
    """franzmq calls this off the event loop (its own callback thread); hand
    the dispatch back to the loop the way :meth:`chaski.service.Service._ring_doorbell`
    hands the ingest wake-up back."""

    def _on_message(message: Any) -> None:
        record = message.payload  # already decoded; None is the tombstone
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_dispatch(handler, record, door)))

    return _on_message


async def _dispatch(handler: Any, record: Any, door: Any) -> None:
    """``handler`` is a bound Producer method — typed ``Any`` because
    ``Callable`` carries no ``__self__``, which this needs for the
    producer's own lock and name."""
    producer = handler.__self__
    with resolve.one_pass(door):
        try:
            with producer._lock:
                await handler(record)
        except Exception:
            log.exception(
                "%s.%s failed handling %s",
                producer.name,
                getattr(handler, "__name__", handler),
                "a retired record" if record is None else type(record).__name__,
            )
