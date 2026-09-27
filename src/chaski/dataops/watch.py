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
by :func:`chaski.service.tolerate_undecodable`.

Delivery happens on franzmq's own callback thread (:class:`franzmq.Client`
runs a message's registered callbacks off the network thread already); each
match is handed to the producer's event loop with ``call_soon_threadsafe``
and run under the producer's lock and a pinned KV snapshot, as
:func:`chaski.dataops.service.make_handler` does for ``@on_metric``. Every
message handed over before the loop gets to run them is one burst, and a burst
shares one lazy :class:`chaski.dataops.resolve.Snapshot`: one ``/kv`` read for
the burst when a handler resolves something, none when no handler does.

**A failed handler is retried, not dropped.** Its record is handed to the
handler again with bounded, jittered backoff, counted against the service's
:class:`chaski.failures.HandlerHealth` and logged each time, until it
succeeds or a newer record at the same topic replaces it (the record is
current state, so only the newest one matters). A handler that raises
:class:`chaski.Reject` has the record recorded as rejected instead.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable, Iterable
from typing import Any

from colca_data_contracts.payload import Constant, Signal
from franzmq import Topic

from chaski.failures import HandlerHealth, Reject
from chaski.retry import Backoff

from . import resolve
from .base import Producer
from .triggers import OnConstantSpec, OnSignalSpec

log = logging.getLogger("chaski.dataops.watch")

#: qos=1: a missed `_Constant`/`_Signal` write is a missed decision, unlike
#: the input wake (qos=0), which only wakes a poll that runs again regardless.
_QOS = 1
#: The first retry of a failed handler; doubles per failure up to 30 s.
RETRY_MIN_S = 1.0


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
    *,
    health: HandlerHealth | None = None,
    reject: Callable[[str, dict[str, Any], Reject], None] | None = None,
) -> int:
    """Subscribe every ``(path_or_pattern, handler)`` pair on ``client``,
    scoped to ``node_id``. Returns how many subscriptions were made.

    Safe to call with two empty lists — nothing subscribes, so a service with
    no ``@on_constant``/``@on_signal`` producer opens no extra subscription.
    ``door`` is read at most once per burst of messages, and only when a
    handler resolves something.
    """
    if not constant_triggers and not signal_triggers:
        return 0
    bursts = _Bursts(door, loop, health or HandlerHealth(), reject)
    count = 0
    for path_or_pattern, handler in constant_triggers:
        _subscribe(client, Constant, node_id, path_or_pattern, handler, bursts)
        count += 1
    for path_or_pattern, handler in signal_triggers:
        _subscribe(client, Signal, node_id, path_or_pattern, handler, bursts)
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
    bursts: _Bursts,
) -> None:
    topic = Topic(payload_type=payload_type, node_id=node_id, context=tuple(path_or_pattern.split("/")))
    client.subscribe(topic, qos=_QOS, callback=bursts.callback(handler))
    log.debug("watching %s for %s", topic, getattr(handler, "__qualname__", handler))


class _Bursts:
    """Hands messages from franzmq's callback thread to the loop, in bursts.

    Messages queue up until the loop runs :meth:`_drain`; everything drained
    together is dispatched under one lazy snapshot. Handlers still run as
    separate tasks, as before.
    """

    def __init__(
        self,
        door: Any,
        loop: asyncio.AbstractEventLoop,
        health: HandlerHealth,
        reject: Callable[[str, dict[str, Any], Reject], None] | None,
    ) -> None:
        self._door = door
        self._loop = loop
        self._health = health
        self._reject = reject
        self._lock = threading.Lock()
        self._pending: list[tuple[Any, Any, str, int]] = []
        self._tasks: set[asyncio.Future[None]] = set()  # held so a running dispatch is not collected
        # (handler, topic) -> generation of the newest record handed over.
        self._latest: dict[tuple[Any, str], int] = {}
        self._generation = 0

    def callback(self, handler: Callable) -> Callable[[Any], None]:
        def _on_message(message: Any) -> None:
            record = message.payload  # already decoded; None is the tombstone
            topic = str(getattr(message, "topic", ""))
            with self._lock:
                first = not self._pending
                self._generation += 1
                self._latest[(handler, topic)] = self._generation
                self._pending.append((handler, record, topic, self._generation))
            if first:
                self._loop.call_soon_threadsafe(self._drain)

        return _on_message

    def superseded(self, handler: Any, topic: str, generation: int) -> bool:
        with self._lock:
            return self._latest.get((handler, topic), generation) != generation

    def _drain(self) -> None:
        with self._lock:
            burst, self._pending = self._pending, []
        snapshot = resolve.Snapshot(self._door)
        for handler, record, topic, generation in burst:
            task = asyncio.ensure_future(_dispatch(self, handler, record, topic, generation, snapshot))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)


async def _dispatch(
    bursts: _Bursts, handler: Any, record: Any, topic: str, generation: int, snapshot: resolve.Snapshot
) -> None:
    """``handler`` is a bound Producer method — typed ``Any`` because
    ``Callable`` carries no ``__self__``, which this needs for the
    producer's own lock and name."""
    producer = handler.__self__
    name = f"{producer.name}.{getattr(handler, '__name__', handler)}"
    retry = Backoff(minimum=RETRY_MIN_S)
    while True:
        try:
            with resolve.lazy_pass(snapshot):
                try:
                    with producer._lock:
                        await handler(record)
                except Reject as rejected:
                    if bursts._reject is None:
                        raise RuntimeError(f"{name} rejected a record, but there is nowhere to record it") from rejected
                    subject = {"topic": topic, "retired": record is None}
                    await asyncio.to_thread(bursts._reject, name, subject, rejected)
        except Exception as exc:
            count = bursts._health.failed(name, exc)
            delay = retry.delay(exc)
            log.error(
                "%s failed handling %s at %s (%d in a row); retrying in %.1fs unless a newer record replaces it",
                name,
                "a retired record" if record is None else type(record).__name__,
                topic,
                count,
                delay,
                exc_info=exc,
            )
            await asyncio.sleep(delay)
            if bursts.superseded(handler, topic, generation):
                return  # a newer record at this topic took over; its dispatch reports health
            # A retry resolves against the node as it is now.
            snapshot = resolve.Snapshot(bursts._door)
            continue
        bursts._health.succeeded(name)
        return
