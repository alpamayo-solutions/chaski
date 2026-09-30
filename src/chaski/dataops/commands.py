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

import asyncio
import contextlib
import json
import logging
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

import httpx
from colca_data_contracts import topic_prefix
from franzmq.errors import PublishTimeout

from chaski.command import is_progress
from chaski.door import Page, Record, Stream, StreamGapError
from chaski.service import NotSent, writes_until

from . import resolve
from .base import Producer
from .triggers import OnCommandSpec

log = logging.getLogger("chaski.dataops.commands")

STREAM = "commands"
CURSOR = "commands"
ACK_OK = 200
ACK_EXPIRED = 498
ACK_FAILED = 500
ACK_BUSY = 503
ACK_UNKNOWN = 504
#: The contract of an answer; the executor reads its own back from the stream.
ACK_CONTRACT = "_Ack"
#: Upper bound on the backoff between attempts to publish one answer.
ANSWER_BACKOFF_MAX_S = 5.0
#: What a failing answer publish is reported as in the handler health.
ANSWER_CONSUMER = "commands: answer"
#: How many answered correlation ids the executor remembers in memory.
_ANSWERED_LIMIT = 4096
#: Upper bound on the retry backoff after the door failed.
_ERROR_BACKOFF_MAX_S = 30.0
#: How far apart the node sends stream-growth hints at most; what grows in
#: between is one hint.
WATCH_INTERVAL_MS = 1000


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


class MemoryLedger:
    """The ledger of a process without a buffer: it does not survive a
    restart. The DataOps service passes its durable buffer instead."""

    def __init__(self) -> None:
        self._entries: OrderedDict[str, str | None] = OrderedDict()

    def command_entry(self, correlation_id: str) -> tuple[bool, str | None]:
        if correlation_id not in self._entries:
            return False, None
        return True, self._entries[correlation_id]

    def command_started(self, correlation_id: str) -> None:
        self._entries.setdefault(correlation_id, None)
        while len(self._entries) > _ANSWERED_LIMIT:
            self._entries.popitem(last=False)

    def command_answered(self, correlation_id: str, answer: str) -> None:
        self._entries[correlation_id] = answer


def unconfirmed_write(exc: BaseException) -> bool:
    """Whether ``exc`` (or what caused it) is a publish that got no PUBACK:
    the write may still reach the node."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, PublishTimeout):
            return True
        if isinstance(current, NotSent):
            return False
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False


class _Stopping(Exception):
    """The service stops while an answer is still pending."""


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
    ack (the service's :meth:`~chaski.Service.send`). ``stream`` should read
    :func:`stream_contracts`. ``ledger`` records started and answered
    commands (:class:`MemoryLedger` when omitted); ``health`` counts failing
    answer publishes under :data:`ANSWER_CONSUMER`."""

    def __init__(
        self,
        door: Any,
        send: Callable[[str, str], None],
        stream: Stream,
        handlers: dict[tuple[str, str], Callable],
        node_id: str,
        *,
        ledger: Any = None,
        health: Any = None,
    ) -> None:
        self._door = door
        self._send = send
        self._stream = stream
        self._handlers = handlers
        self._node_id = node_id
        self._ledger = ledger if ledger is not None else MemoryLedger()
        self._health = health
        self._wake = asyncio.Event()
        # Set while the broker link is up; an answer waits on it.
        self._link = asyncio.Event()
        self._link.set()
        self._stop = asyncio.Event()
        self._answered: OrderedDict[str, None] = OrderedDict()
        self._ack_topics = {self.ack_topic(path) for _contract, path in handlers}

    # -- topics -------------------------------------------------------------

    def ack_topic(self, path: str) -> str:
        return f"{topic_prefix()}_Ack/{self._node_id}/{path}"

    def wake(self) -> None:
        """New commands may be waiting. From another thread, call it through
        ``loop.call_soon_threadsafe``."""
        self._wake.set()

    def link_changed(self, connected: bool) -> None:
        """The broker link went down or came back; on the loop's thread. A
        pending answer is sent again once it is back."""
        if connected:
            self._link.set()
            self._wake.set()
        else:
            self._link.clear()

    # -- loop -----------------------------------------------------------------

    async def run_forever(self, stop: asyncio.Event) -> None:
        from chaski.stream_changes import StreamChanges

        loop = asyncio.get_running_loop()
        watch = StreamChanges(
            self._door,
            [STREAM],
            contracts=stream_contracts(self._handlers),
            on_change=lambda: loop.call_soon_threadsafe(self.wake),
        ).start()
        try:
            await self._serve(stop)
        finally:
            await asyncio.to_thread(watch.close)

    async def _serve(self, stop: asyncio.Event) -> None:
        from chaski.retry import Backoff

        self._stop = stop
        retry = Backoff(minimum=0.5, maximum=_ERROR_BACKOFF_MAX_S)
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
                retry.reset()
            except _Stopping:
                return
            except httpx.HTTPError as exc:
                backoff = retry.delay(exc)
                log.warning("commands: drain failed (attempt %d): %s — retrying in %.1fs", retry.failures, exc, backoff)
                if await self._stopped_within(backoff):
                    return
                self._wake.set()

    async def _stopped_within(self, seconds: float) -> bool:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        return self._stop.is_set()

    async def drain(self) -> int:
        """Handle every command up to the head of the stream, acking page by
        page once every answer on the page is published. Returns how many
        records were read."""
        seen = 0
        head = await asyncio.to_thread(self._stream.head)
        while True:
            page: Page = await asyncio.to_thread(self._stream.fetch)
            if page.gap is not None:
                raise StreamGapError(
                    f"commands: offsets {page.gap.from_offset}..{page.gap.to_offset} were pruned; explicit recovery required"
                )
            self._note_answers(page.records)
            scanned_ahead = False
            for record in page.records:
                if not scanned_ahead and self._needs_look_ahead(record, page):
                    await asyncio.to_thread(self._scan_ahead, page.next)
                    scanned_ahead = True
                await self.handle(record)
            seen += len(page.records)
            ack_offset = page.ack_offset
            if ack_offset is None:
                if page.next <= head:
                    raise RuntimeError(f"commands: the stream stopped at {page.next} before its head {head}")
                return seen
            await asyncio.to_thread(self._stream.ack, ack_offset)
            if ack_offset >= head:
                return seen

    # -- answered commands ----------------------------------------------------

    def _note_answers(self, records: Iterable[Record]) -> None:
        for record in records:
            if record.topic not in self._ack_topics or not isinstance(record.payload, dict):
                continue
            if is_progress(record.payload):
                continue  # a 202 says a node queued or forwarded it, not that it was answered
            correlation_id = str(record.payload.get("correlation_id") or "")
            if correlation_id:
                self._remember(correlation_id)

    def _remember(self, correlation_id: str) -> None:
        self._answered[correlation_id] = None
        self._answered.move_to_end(correlation_id)
        while len(self._answered) > _ANSWERED_LIMIT:
            self._answered.popitem(last=False)

    def _needs_look_ahead(self, record: Record, page: Page) -> bool:
        """A full page may end before the answer to one of its commands: read
        ahead for answers before executing it."""
        if len(page.records) < self._stream.page_size or record.topic in self._ack_topics:
            return False
        payload = record.payload if isinstance(record.payload, dict) else {}
        correlation_id = str(payload.get("correlation_id") or "")
        return bool(correlation_id) and correlation_id not in self._answered

    def _scan_ahead(self, from_offset: int) -> None:
        """Note the answers after ``from_offset`` without moving the cursor.
        A node that cannot read ahead (no ``Page.start``) ends the scan."""
        offset = from_offset
        while True:
            page = self._stream.fetch(from_offset=offset)
            if page.start is None:
                return
            self._note_answers(page.records)
            if len(page.records) < self._stream.page_size or page.next <= offset:
                return
            offset = page.next

    # -- one command ------------------------------------------------------------

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
        correlation_id = command.correlation_id
        who = command.actor_label or command.actor_id or "?"
        if correlation_id in self._answered:
            log.info("command %s %s by %s was answered already; not executed again", contract, path, who)
            return
        if correlation_id:
            started, recorded = await asyncio.to_thread(self._ledger.command_entry, correlation_id)
            if started:
                if recorded is None:
                    recorded = self._answer_body(
                        correlation_id,
                        ACK_UNKNOWN,
                        "outcome unknown: the service stopped while executing it; it may have taken effect",
                    )
                    await asyncio.to_thread(self._ledger.command_answered, correlation_id, recorded)
                log.info(
                    "command %s %s by %s was executed before; answering it again, not re-executing", contract, path, who
                )
                await self._publish_answer(self.ack_topic(path), correlation_id, recorded)
                return

        if expired(payload.get("expires_at"), time.time() * 1000.0):
            code, message = ACK_EXPIRED, "expired before it was executed"
        elif not await self._link_before(command.expires_at):
            code, message = ACK_EXPIRED, "expired while the broker link was down; not executed"
        else:
            if correlation_id:
                await asyncio.to_thread(self._ledger.command_started, correlation_id)
            code, message = await self._run(handler, command)
        log.info("command %s %s by %s -> %d %s", contract, path, who, code, message)
        if not correlation_id:
            log.warning("command %s %s has no correlation_id — not answered", contract, path)
            return
        answer = self._answer_body(correlation_id, code, message)
        await asyncio.to_thread(self._ledger.command_answered, correlation_id, answer)
        await self._publish_answer(self.ack_topic(path), correlation_id, answer)

    @staticmethod
    def _answer_body(correlation_id: str, code: int, message: str) -> str:
        return json.dumps(
            {"correlation_id": correlation_id, "result_code": code, "message": message, "performed_at": time.time()}
        )

    async def _publish_answer(self, topic: str, correlation_id: str, answer: str) -> None:
        """Publish ``answer`` until the node confirms it. While the broker is
        away it waits for the link; a failed attempt is retried with jittered
        backoff. Raises :class:`_Stopping` when the service stops first: the
        cursor then stays before the command, and the ledger holds the answer."""
        from chaski.retry import Backoff

        retry = Backoff(minimum=min(0.5, ANSWER_BACKOFF_MAX_S), maximum=ANSWER_BACKOFF_MAX_S)
        while True:
            if not self._link.is_set():
                await self._wait_for_link()
            try:
                await asyncio.to_thread(self._send, topic, answer)
            except Exception as exc:
                delay = retry.delay(exc)
                count = self._health.failed(ANSWER_CONSUMER, exc) if self._health is not None else retry.failures
                log.error(
                    "commands: answer %s on %s not confirmed (%d in a row): %s — sending it again in %.1fs",
                    correlation_id,
                    topic,
                    count,
                    exc,
                    delay,
                )
                if await self._stopped_within(delay):
                    raise _Stopping from exc
                continue
            if retry.failures and self._health is not None:
                self._health.succeeded(ANSWER_CONSUMER)
            self._remember(correlation_id)
            return

    async def _link_before(self, expires_at: float | None) -> bool:
        """Wait for the broker link, until ``expires_at`` (unix ms) at most.
        False when the command expired first; raises :class:`_Stopping` when
        the service stops first."""
        if self._link.is_set():
            return True
        timeout = None if expires_at is None else max(0.0, expires_at / 1000.0 - time.time())
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._wait_for_link(), timeout)
        return self._link.is_set() and not expired(expires_at, time.time() * 1000.0)

    async def _wait_for_link(self) -> None:
        link_task = asyncio.ensure_future(self._link.wait())
        stop_task = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({link_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (link_task, stop_task):
                if not task.done():
                    task.cancel()
        if self._stop.is_set():
            raise _Stopping

    async def _run(self, handler: Any, command: Command) -> tuple[int, str]:
        """Run one command. Its resolutions share one KV read, taken only when the
        first of them needs it: a full read per command ran into the node's rate
        limit when commands came fast."""
        producer = handler.__self__
        name = getattr(handler, "__name__", handler)
        deadline = None if command.expires_at is None else command.expires_at / 1000.0
        with resolve.lazy_pass(resolve.Snapshot(self._door)), writes_until(deadline):
            try:
                with producer._lock:
                    result = await handler(command)
            except CommandRejected as exc:
                return exc.code, exc.message
            except NotSent as exc:
                log.warning("%s.%s on %s: a write was refused: %s", producer.name, name, command.path, exc)
                return ACK_FAILED, f"the command failed: {exc}"[:200]
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    return ACK_BUSY, "the node is busy; send the command again"
                log.exception("%s.%s failed on %s", producer.name, name, command.path)
                return ACK_FAILED, f"the command failed: the node answered {exc.response.status_code}"
            except Exception as exc:
                if unconfirmed_write(exc):
                    log.warning("%s.%s on %s: a write was not confirmed: %s", producer.name, name, command.path, exc)
                    return (
                        ACK_UNKNOWN,
                        "outcome unknown: a write was sent but the node did not confirm it in time; it may still take effect",
                    )
                log.exception("%s.%s failed on %s", producer.name, name, command.path)
                return ACK_FAILED, f"the command failed: {type(exc).__name__}: {exc}"[:200]
        return ACK_OK, "" if result is None else str(result)
