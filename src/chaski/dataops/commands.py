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

**MQTT only rings the bell.** Each declared command topic is subscribed at
QoS 1; a message wakes a drain of the stream and is not read itself. The
executor drains once at startup, once per wake and once after the broker link
comes back. There is no timed poll.

**Answers.** Each command is answered over MQTT at ``_Ack/<node>/<path>``
with ``{correlation_id, result_code, message, performed_at}``:

======  =====================================================
200     the handler returned; its string is the message
4xx     the handler raised :class:`CommandRejected`
400     the command asks to live longer than
        :data:`chaski.command.MAX_LIFETIME_S`, counted
        from its arrival at the node or from its ``created_at``, or says it
        was created after it arrived; the handler is not run
498     ``expires_at`` (unix ms) had passed; the handler is not run
500     the handler raised anything else
======  =====================================================

A command without a ``correlation_id`` cannot be matched by its sender and is
not answered.

**Crash safety.** The cursor is acked per page, after every command on it was
handled. A crash before that repeats the page, so handlers must be
idempotent — setting a value is.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import httpx
from colca_data_contracts import topic_prefix

from chaski.command import lifetime_refusal
from chaski.door import Page, Record, Stream

from . import resolve
from .base import Producer
from .triggers import OnCommandSpec

log = logging.getLogger("chaski.dataops.commands")

STREAM = "commands"
CURSOR = "commands"
ACK_OK = 200
ACK_REFUSED = 400
ACK_EXPIRED = 498
ACK_FAILED = 500
ACK_BUSY = 503
#: QoS 1: a lost wake would leave a command waiting for the next one.
_QOS = 1
#: Upper bound on the retry backoff after the door failed.
_ERROR_BACKOFF_MAX_S = 30.0


@dataclass(frozen=True)
class Command:
    """One command as a handler sees it. The actor fields are the node's
    attestation from the stored record, never the payload's."""

    path: str
    verb: str
    contract: str
    params: dict[str, Any] = field(default_factory=dict)
    correlation_id: str = ""
    expires_at: float | None = None
    actor_id: str = ""
    actor_label: str = ""
    actor_kind: str = ""
    ts: float = 0.0
    offset: int = 0


class CommandRejected(Exception):
    """Raise from an ``@on_command`` handler to answer with ``code`` (a 4xx)
    and ``message`` instead of 200."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = int(code)
        self.message = message


Handler = Callable[[Command], Any]


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


def parse_topic(topic: str) -> tuple[str, str] | None:
    """``(contract, path)`` of ``<root>/v1/<contract>/<owner>/<path...>``,
    or ``None`` for anything shorter."""
    parts = topic.split("/")
    if len(parts) < 5:
        return None
    return parts[2], "/".join(parts[4:])


def expired(expires_at: Any, now_ms: float) -> bool:
    """``expires_at`` is unix milliseconds. Absent or unusable is not
    expired: a missing deadline must not turn into a rejection."""
    try:
        return float(expires_at) < now_ms
    except (TypeError, ValueError):
        return False


def _deadline(raw: Any) -> float | None:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


class CommandExecutor:
    """Drains the ``commands`` stream for the declared commands and answers
    them. ``handlers`` comes from :func:`gather`; ``send`` publishes the
    ack (the service's :meth:`~chaski.Service.send`)."""

    def __init__(
        self,
        door: Any,
        send: Callable[[str, str], None],
        stream: Stream,
        handlers: dict[tuple[str, str], Callable],
        node_id: str,
    ) -> None:
        self._door = door
        self._send = send
        self._stream = stream
        self._handlers = handlers
        self._node_id = node_id
        self._wake = asyncio.Event()

    # -- topics -------------------------------------------------------------

    def command_topics(self) -> list[str]:
        return sorted(f"{topic_prefix()}{contract}/{self._node_id}/{path}" for contract, path in self._handlers)

    def ack_topic(self, path: str) -> str:
        return f"{topic_prefix()}_Ack/{self._node_id}/{path}"

    def subscribe(self, client: Any, loop: asyncio.AbstractEventLoop) -> int:
        """Subscribe every declared command topic as a wake-up. The payload
        is not decoded: the stream record is what gets executed. A refused
        SUBACK is retried by the service's :class:`chaski.subscriptions.Subscriptions`."""

        def _ring(_client: Any, _userdata: Any, _message: Any) -> None:
            loop.call_soon_threadsafe(self.wake)

        topics = self.command_topics()
        for topic in topics:
            client.message_callback_add(topic, _ring)
            client.subscribe(topic, qos=_QOS)
        log.info("commands: executing %d command(s) on node=%s", len(topics), self._node_id)
        return len(topics)

    def wake(self) -> None:
        """New commands may be waiting. From another thread, call it through
        ``loop.call_soon_threadsafe``."""
        self._wake.set()

    # -- loop -----------------------------------------------------------------

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Drain now, then once per wake, until ``stop``. A door failure is
        retried with backoff; the unacked page is read again."""
        errors = 0
        self._wake.set()
        while not stop.is_set():
            wake_task = asyncio.ensure_future(self._wake.wait())
            stop_task = asyncio.ensure_future(stop.wait())
            try:
                await asyncio.wait({wake_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in (wake_task, stop_task):
                    if not task.done():
                        task.cancel()
            if stop.is_set():
                return
            self._wake.clear()
            try:
                await self.drain()
                errors = 0
            except httpx.HTTPError as exc:
                errors += 1
                backoff = min(0.5 * (2 ** (errors - 1)), _ERROR_BACKOFF_MAX_S)
                log.warning("commands: drain failed (attempt %d): %s — retrying in %.1fs", errors, exc, backoff)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                self._wake.set()

    async def drain(self) -> int:
        """Handle every command up to the head of the stream, acking page by
        page. Returns how many records were read."""
        seen = 0
        while True:
            page: Page = await asyncio.to_thread(self._stream.fetch)
            if page.gap is not None:
                log.warning(
                    "commands: offsets %d..%d were pruned before this service read them",
                    page.gap.from_offset,
                    page.gap.to_offset,
                )
            for record in page.records:
                await self.handle(record)
            seen += len(page.records)
            ack_offset = page.ack_offset
            if ack_offset is None:
                return seen
            await asyncio.to_thread(self._stream.ack, ack_offset)

    async def handle(self, record: Record) -> None:
        """Execute one record if it is a declared command, and answer it."""
        parsed = parse_topic(record.topic)
        if parsed is None:
            return
        handler = self._handlers.get(parsed)
        if handler is None:
            return
        contract, path = parsed
        payload = record.payload if isinstance(record.payload, dict) else {}
        params = payload.get("command")
        command = Command(
            path=path,
            verb=path.rsplit("/", 1)[-1],
            contract=contract,
            params=params if isinstance(params, dict) else {},
            correlation_id=str(payload.get("correlation_id") or ""),
            expires_at=_deadline(payload.get("expires_at")),
            actor_id=record.actor_id,
            actor_label=record.actor_label,
            actor_kind=record.actor_kind,
            ts=record.ts,
            offset=record.offset,
        )
        refusal = lifetime_refusal(payload.get("expires_at"), payload.get("created_at"), record.ts)
        if refusal is not None:
            code, message = ACK_REFUSED, refusal
        elif expired(payload.get("expires_at"), time.time() * 1000.0):
            code, message = ACK_EXPIRED, "expired before it was executed"
        else:
            code, message = await self._run(handler, command)
        who = command.actor_label or command.actor_id or "?"
        log.info("command %s %s by %s -> %d %s", contract, path, who, code, message)
        if not command.correlation_id:
            log.warning("command %s %s has no correlation_id — not answered", contract, path)
            return
        answer = {
            "correlation_id": command.correlation_id,
            "result_code": code,
            "message": message,
            "performed_at": time.time(),
        }
        await asyncio.to_thread(self._send, self.ack_topic(path), json.dumps(answer))

    async def _run(self, handler: Any, command: Command) -> tuple[int, str]:
        """Run one command. Its resolutions share one KV read, taken only when the
        first of them needs it: a full read per command ran into the node's rate
        limit when commands came fast."""
        producer = handler.__self__
        with resolve.lazy_pass(resolve.Snapshot(self._door)):
            try:
                with producer._lock:
                    result = await handler(command)
            except CommandRejected as exc:
                return exc.code, exc.message
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    return ACK_BUSY, "the node is busy; send the command again"
                log.exception("%s.%s failed on %s", producer.name, getattr(handler, "__name__", handler), command.path)
                return ACK_FAILED, f"the command failed: the node answered {exc.response.status_code}"
            except Exception as exc:
                log.exception("%s.%s failed on %s", producer.name, getattr(handler, "__name__", handler), command.path)
                return ACK_FAILED, f"the command failed: {type(exc).__name__}: {exc}"[:200]
        return ACK_OK, "" if result is None else str(result)
