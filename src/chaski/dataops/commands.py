"""``@on_command`` execution: a producer answers commands with an ``_Ack``.

A person may send commands, never data. A producer that owns a value can
therefore offer a command that sets it: it checks the request, writes what
follows from it, and acknowledges.

**Who sent it comes from the stream.** Over MQTT a subscriber sees only the
payload, and a name carried inside it would be whatever the sender claimed.
The node writes the verified identity onto the stored record, so the
executor reads commands from the node's ``commands`` stream through a durable
cursor of its own (``commands`` in the service's namespace) and hands the
record's ``actor_id``/``actor_label``/``actor_kind`` to the handler.

**The stream rings the bell.** The ``commands`` stream carries every command
at the node. The executor follows its growth over ``GET /watch``
(:meth:`CommandExecutor.run_forever`), filtered by :func:`stream_contracts`,
and drains once at startup, once per hint and once after the broker link
comes back. There is no timed poll.

**It wakes on everything it reads.** A drain reads only the executor's own
command and ``_Ack`` topics (:func:`stream_topics`); the node counts records
matching that filter as unread on its cursor. Its own answers land after the
head a drain captured, so the watch includes ``_Ack``: otherwise the answer
would stay unread with nothing to wake the executor past it. Answers at other
paths wake a drain that reads nothing and moves the cursor to the end of what
the node scanned (:attr:`Page.ack_offset`). A drain ends at the head it
captured.

**Answers.** Each command is answered over MQTT at ``_Ack/<node>/<path>``
with ``{correlation_id, result_code, message, performed_at}``:

======  =====================================================
200     the handler returned; its string is the message
4xx     the handler raised :class:`CommandRejected`
498     ``expires_at`` (unix ms) had passed, also while the executor
        waited for its broker link; the handler is not run
500     the handler raised anything else; nothing it sent is pending
504     outcome unknown: the handler sent a write the node did not
        confirm in time (``PublishTimeout``), or the service stopped
        while the handler ran. The write may still take effect.
======  =====================================================

A command without a ``correlation_id`` cannot be matched by its sender and is
not answered.

**A command may wait long.** A command without ``expires_at`` never expires:
it may reach the executor days after it was sent, when its node was cut off
from the sender's. A handler whose effect must not happen late checks the age
itself (``Command.ts`` is when the sender's node stored it) or relies on the
sender setting a lifetime. A handler must not repeat an effect that is not
idempotent: the same command can be handed to it again only after the
answer was lost, and the ledger below answers it without running it.

**A handler runs only while the broker link is up.** Its effects are MQTT
writes. While the link is down the executor waits for it, until the
command's deadline at most, and a command that expires meanwhile is answered
498 without running. While the handler runs, a write after the deadline or
while the link is down is refused (:class:`chaski.service.NotSent`) rather
than queued for after a reconnect, where it would land late; the command is
then answered 500.

**504, not 500, for an unconfirmed write.** A publish without a PUBACK stays
queued in the MQTT client and goes out after a reconnect, possibly after the
command was answered. Answering 500 ("nothing was changed") would then be
false. The executor answers 504 instead: the sender must read the state back
before it relies on either outcome.

**An answer is published before the cursor passes its command.** A failed
``_Ack`` publish (no PUBACK, broker away) does not end the executor: the answer
stays pending and is sent again, once the broker link is back, with bounded
and jittered backoff (:data:`ANSWER_BACKOFF_MAX_S`). The cursor is acked only
after every answer on the page was confirmed. The same answer may therefore
arrive more than once; it is always identical.

**One execution, one answer.** The executor reads the ``_Ack`` records of its
own paths from the ``commands`` stream next to the commands, and a command
whose answer is already there is not executed again. With a ledger (the
DataOps service's buffer, :meth:`~chaski.dataops.buffer.Buffer.command_started`)
a command is recorded before its handler runs and its answer before it is
published: after a restart a recorded answer is published again unchanged,
and a command that was started but not answered gets 504. It is never run a
second time, and never answered 500 first and 498 later.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from colca_data_contracts import topic_prefix

from chaski import executor
from chaski.executor import (
    ACK_BUSY,
    ACK_CONTRACT,
    ACK_EXPIRED,
    ACK_FAILED,
    ACK_OK,
    ACK_UNKNOWN,
    ANSWER_BACKOFF_MAX_S,
    ANSWER_CONSUMER,
    CURSOR,
    STREAM,
    WATCH_INTERVAL_MS,
    Command,
    CommandRejected,
    CommandResult,
    MemoryLedger,
    expired,
    parse_topic,
    unconfirmed_write,
)

from . import resolve
from .base import Producer
from .triggers import OnCommandSpec

__all__ = [
    "ACK_BUSY",
    "ACK_CONTRACT",
    "ACK_EXPIRED",
    "ACK_FAILED",
    "ACK_OK",
    "ACK_UNKNOWN",
    "ANSWER_BACKOFF_MAX_S",
    "ANSWER_CONSUMER",
    "CURSOR",
    "STREAM",
    "WATCH_INTERVAL_MS",
    "Command",
    "CommandExecutor",
    "CommandRejected",
    "CommandResult",
    "MemoryLedger",
    "contracts",
    "declared_routes",
    "expired",
    "gather",
    "parse_topic",
    "stream_contracts",
    "stream_topics",
    "unconfirmed_write",
]


def contracts(handlers: dict[tuple[str, str], Callable]) -> list[str]:
    """The command contracts ``handlers`` execute."""
    return sorted({contract for contract, _path in handlers})


def stream_contracts(handlers: dict[tuple[str, str], Callable]) -> list[str]:
    """What the executor's stream reads and what wakes it: its command
    contracts and ``_Ack``, so it sees which of its commands were answered
    already and is woken past its own answers."""
    return sorted({*contracts(handlers), ACK_CONTRACT})


def stream_topics(handlers: dict[tuple[str, str], Callable], node_id: str) -> list[str]:
    """The topics the executor's stream reads: its commands and their answers. ``_Ack`` records at other paths are not its business
    and must not count as unread on its cursor."""
    prefix = topic_prefix()
    return sorted(
        {f"{prefix}{contract}/{node_id}/{path}" for contract, path in handlers}
        | {f"{prefix}{ACK_CONTRACT}/{node_id}/{path}" for _contract, path in handlers}
    )


def gather(instances: Iterable[Producer]) -> dict[tuple[str, str], Callable]:
    """``{(contract, path): bound handler}`` across ``instances``. One path
    has one executor; a second declaration of the same command is an error."""
    handlers: dict[tuple[str, str], Callable] = {}
    for instance in instances:
        for method_name, spec in type(instance)._triggers:
            if not isinstance(spec, OnCommandSpec):
                continue
            key = (spec.contract, spec.path)
            if key in handlers:
                raise ValueError(f"{spec.contract} {spec.path} is declared by two @on_command handlers")
            handlers[key] = getattr(instance, method_name)
    return handlers


def declared_routes(producer_classes: Iterable[type[Producer]]) -> list[tuple[str, str]]:
    """``(contract, path)`` of every ``@on_command`` the producer classes
    declare: what their service announces it executes."""
    routes: set[tuple[str, str]] = set()
    for cls in producer_classes:
        for _method_name, spec in cls._triggers:
            if isinstance(spec, OnCommandSpec):
                routes.add((spec.contract, spec.path))
    return sorted(routes)


class CommandExecutor(executor.CommandExecutor):
    """The executor for ``@on_command`` handlers of DataOps producers. A
    handler runs under its producer's lock, and its resolutions share one KV
    read taken only when the first of them needs it: a full read per command
    ran into the node's rate limit when commands came fast."""

    async def _invoke(self, handler: Any, command: Command) -> Any:
        producer = handler.__self__
        with resolve.lazy_pass(resolve.Snapshot(self._door)), producer._lock:
            return await handler(command)
