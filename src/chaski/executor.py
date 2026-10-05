"""Execute commands from the node's ``commands`` stream and answer them.

:class:`CommandExecutor` is the mechanism behind ``@on_command`` (DataOps) and a
connector's signal writes (:class:`chaski.ConnectorService`): it drains the
stream through a durable cursor, runs each command's handler once, and
answers with an ``_Ack`` at the command's position. The answer is recorded in
a ledger before it is published, so a restart answers again instead of
running again. :mod:`chaski.dataops.commands` documents the answer codes.

A handler returns a message string, or a :class:`CommandResult` whose
``result`` mapping is carried in the ``_Ack`` as ``result``;
:class:`CommandRejected` answers its own code (and optional ``result``).

**Operations.** A command that carries an ``operation_id``
(:mod:`chaski.command`, "Who and which operation") is also recorded in the
ledger under ``(attested sender, operation_id)`` before its handler runs, and
its outcome after. A later command with the same key is not executed: it is
answered with the recorded outcome and ``"replayed": true``, or ``504`` when
the first one was started and never answered (the service stopped mid-way),
or ``409`` when its contract, path, ``command`` or ``on_behalf_of`` differ
from the recorded one. The ledger keeps entries for its retention (a
:class:`SqliteLedger` seven days), which is how long a repeat is recognised.
An ``_Ack`` repeats the command's ``operation_id`` and ``on_behalf_of``.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from colca_data_contracts import topic_prefix
from franzmq.errors import PublishTimeout

from chaski.command import Actor, EnvelopeError, check_operation_id, is_progress
from chaski.door import Page, Record, Stream, StreamGapError
from chaski.service import NotSent, writes_until

log = logging.getLogger("chaski.executor")

STREAM = "commands"
CURSOR = "commands"
ACK_OK = 200
ACK_FORBIDDEN = 403
ACK_CONFLICT = 409
ACK_INVALID = 422
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


class SqliteLedger:
    """A ledger in a SQLite file, for a process without a DataOps buffer (a
    connector): a command started before a restart and not answered is
    answered ``504`` after it, never run again. Entries go ``keep_s`` after
    their command started."""

    def __init__(self, path: str | os.PathLike[str], *, keep_s: float = 7 * 86400.0) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._keep_s = keep_s
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS command_ledger "
            "(correlation_id TEXT PRIMARY KEY, started_at REAL NOT NULL, answer TEXT)"
        )

    def command_entry(self, correlation_id: str) -> tuple[bool, str | None]:
        with self._lock:
            row = self._conn.execute(
                "SELECT answer FROM command_ledger WHERE correlation_id=?", (correlation_id,)
            ).fetchone()
        return (row is not None, row[0] if row is not None else None)

    def command_started(self, correlation_id: str) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute("DELETE FROM command_ledger WHERE started_at < ?", (now - self._keep_s,))
            self._conn.execute(
                "INSERT OR IGNORE INTO command_ledger (correlation_id, started_at) VALUES (?, ?)", (correlation_id, now)
            )

    def command_answered(self, correlation_id: str, answer: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO command_ledger (correlation_id, started_at, answer) VALUES (?, ?, ?) "
                "ON CONFLICT(correlation_id) DO UPDATE SET answer=excluded.answer",
                (correlation_id, time.time(), answer),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()


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
    """One command as a handler sees it. The ``actor_*`` fields are the node's
    attestation of the sender from the stored record, never the payload's.
    ``on_behalf_of`` is the person the sender says it acts for, asserted by
    that sender (:mod:`chaski.command`); ``operation_id`` its idempotency key,
    ``""`` when it sent none. ``expires_at`` is unix milliseconds."""

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
    operation_id: str = ""
    on_behalf_of: Actor | None = None

    @property
    def sender(self) -> Actor:
        """The attested sender."""
        return Actor(self.actor_id, self.actor_label, self.actor_kind)


class CommandRejected(Exception):
    """Raise from a command handler to answer with ``code`` and ``message``
    instead of 200; ``result`` is carried in the ``_Ack`` as ``result``."""

    def __init__(self, code: int, message: str, result: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = int(code)
        self.message = message
        self.result = dict(result) if result is not None else None


@dataclass(frozen=True)
class CommandResult:
    """What a handler may return instead of a message: the message and a
    ``result`` mapping the ``_Ack`` carries, such as a value read back."""

    message: str = ""
    result: Mapping[str, Any] = field(default_factory=dict)


Handler = Callable[[Command], Any]


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
    """Drains the ``commands`` stream for the handled commands and answers
    them. ``handlers`` maps ``(contract, node-local path)`` to an async
    handler; it is read at every record, so a caller may change it while the
    executor runs (a connector's bindings). ``send`` publishes the ack (the
    service's :meth:`~chaski.Service.send`). ``stream`` must read the
    handled contracts and ``_Ack``; ``contracts`` names the contracts whose
    growth wakes the executor, the handlers' own when omitted. ``ledger``
    records started and answered commands (:class:`MemoryLedger` when
    omitted); ``health`` counts failing answer publishes under
    :data:`ANSWER_CONSUMER`."""

    def __init__(
        self,
        door: Any,
        send: Callable[[str, str], None],
        stream: Stream,
        handlers: Mapping[tuple[str, str], Callable],
        node_id: str,
        *,
        ledger: Any = None,
        health: Any = None,
        contracts: Iterable[str] | None = None,
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
        # The stream watch while running, and the subscription of the hint the
        # last complete drain started from (see StreamChange.covers).
        self._watch: Any = None
        self._drained_on: int | None = None
        self._answered: OrderedDict[str, None] = OrderedDict()
        self._contracts = sorted({*(contracts if contracts is not None else (c for c, _p in handlers)), ACK_CONTRACT})

    @property
    def _ack_topics(self) -> set[str]:
        return {self.ack_topic(path) for _contract, path in list(self._handlers)}

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
            contracts=self._contracts,
            on_change=lambda _changes: loop.call_soon_threadsafe(self.wake),
        ).start()
        self._watch = watch
        try:
            await self._serve(stop)
        finally:
            self._watch = None
            await asyncio.to_thread(watch.close)

    async def _serve(self, stop: asyncio.Event) -> None:
        from chaski.outage import Outage
        from chaski.retry import Backoff

        self._stop = stop
        retry = Backoff(minimum=0.5, maximum=_ERROR_BACKOFF_MAX_S)
        outage = Outage(log, "commands: drain")
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
            link = getattr(self._door, "link_up", None)
            link_seen = link.generation if link is not None else 0
            try:
                await self.drain()
                retry.reset()
                outage.recovered()
            except _Stopping:
                return
            except httpx.HTTPError as exc:
                backoff = retry.delay(exc)
                if not outage.failed(exc, delay=backoff):
                    log.error(
                        "commands: drain failed (attempt %d); retrying in %.1fs", retry.failures, backoff, exc_info=exc
                    )
                # The node's link coming back ends the wait; the backoff
                # spaces retries while it stays up.
                if await self._stopped_within(backoff, link, link_seen):
                    return
                if link is not None and link.generation != link_seen:
                    retry.reset()
                self._wake.set()

    async def _stopped_within(self, seconds: float, wake: Any = None, since: int = 0) -> bool:
        """Wait up to ``seconds``, or until ``wake`` (a :class:`chaski.Doorbell`)
        rang after ``since``. True when the service stopped."""
        if wake is not None:
            stop_task = asyncio.ensure_future(self._stop.wait())
            wake_task = asyncio.ensure_future(wake.after(since))
            try:
                await asyncio.wait({stop_task, wake_task}, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in (stop_task, wake_task):
                    if not task.done():
                        task.cancel()
            return self._stop.is_set()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        return self._stop.is_set()

    async def drain(self) -> int:
        """Handle every command up to the head of the stream, acking page by
        page once every answer on the page is published. Returns how many
        records were read.

        The head of the newest stream-change hint bounds it, and a hint the
        cursor already passed costs no request; without one it reads the
        head first."""
        seen = 0
        change = self._watch.latest(self._stream.name) if self._watch is not None else None
        if change is not None and change.covers(self._stream.position, self._drained_on):
            return 0
        if change is not None and change.head is not None:
            head = change.head
        else:
            head = await asyncio.to_thread(self._stream.head)
        first = True
        while True:
            before = self._stream.position
            page: Page = await asyncio.to_thread(self._stream.fetch)
            if first:
                self._stream.observe(page, before)
                first = False
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
                if not page.records and page.gap is None:
                    # Nothing after the cursor: it already stands at the head.
                    self._stream._advance(page.next - 1)
                break
            await asyncio.to_thread(self._stream.ack, ack_offset)
            if ack_offset >= head:
                break
        if change is not None:
            self._drained_on = change.subscription
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
        envelope_error: tuple[int, str] | None = None
        operation_id = ""
        on_behalf_of: Actor | None = None
        try:
            if payload.get("operation_id") is not None:
                operation_id = check_operation_id(payload["operation_id"])
            if payload.get("on_behalf_of") is not None:
                if not isinstance(payload["on_behalf_of"], dict):
                    raise EnvelopeError("on_behalf_of must be an object with an id")
                on_behalf_of = Actor.coerce(payload["on_behalf_of"])
        except EnvelopeError as exc:
            envelope_error = (ACK_INVALID, f"refused: {exc}")
        if on_behalf_of is not None and record.actor_kind == "human" and on_behalf_of.id != record.actor_id:
            envelope_error = (ACK_FORBIDDEN, "refused: a person cannot send a command on behalf of someone else")
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
            operation_id=operation_id,
            on_behalf_of=on_behalf_of,
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

        result: Mapping[str, Any] | None = None
        replayed = False
        operation_key = self._operation_key(command) if not envelope_error else None
        digest = self._operation_digest(command)
        recorded_operation = (
            await asyncio.to_thread(self._ledger.command_entry, operation_key) if operation_key else (False, None)
        )
        if envelope_error is not None:
            code, message = envelope_error
            result = {"outcome": "refused"}
        elif recorded_operation[0]:
            code, message, result, replayed = await self._replay(operation_key, digest, recorded_operation[1])
        elif expired(payload.get("expires_at"), time.time() * 1000.0):
            code, message = ACK_EXPIRED, "expired before it was executed"
        elif not await self._link_before(command.expires_at):
            code, message = ACK_EXPIRED, "expired while the broker link was down; not executed"
        else:
            if correlation_id:
                await asyncio.to_thread(self._ledger.command_started, correlation_id)
            if operation_key:
                await asyncio.to_thread(self._ledger.command_started, operation_key)
            code, message, result = await self._run(handler, command)
            if operation_key:
                await asyncio.to_thread(
                    self._ledger.command_answered, operation_key, self._operation_record(digest, code, message, result)
                )
        log.info(
            "command %s %s by %s%s -> %d %s%s",
            contract,
            path,
            who,
            f" for {command.on_behalf_of.label or command.on_behalf_of.id}" if command.on_behalf_of else "",
            code,
            message,
            " (replayed)" if replayed else "",
        )
        if not correlation_id:
            log.warning("command %s %s has no correlation_id — not answered", contract, path)
            return
        answer = self._answer_body(correlation_id, code, message, result, command=command, replayed=replayed)
        await asyncio.to_thread(self._ledger.command_answered, correlation_id, answer)
        await self._publish_answer(self.ack_topic(path), correlation_id, answer)

    # -- operations -------------------------------------------------------------

    @staticmethod
    def _operation_key(command: Command) -> str | None:
        """The ledger key of a command's operation: its id, scoped to the
        attested sender, so one sender cannot replay or block another's."""
        if not command.operation_id:
            return None
        return "op\x1f" + json.dumps([command.actor_id, command.operation_id])

    @staticmethod
    def _operation_digest(command: Command) -> str:
        """What a repeat of the operation must match: everything it asks for,
        not when it expires or how it is correlated."""
        material = {
            "contract": command.contract,
            "path": command.path,
            "command": command.params,
            "on_behalf_of": command.on_behalf_of.envelope() if command.on_behalf_of else None,
        }
        return hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode()).hexdigest()

    @staticmethod
    def _operation_record(digest: str, code: int, message: str, result: Mapping[str, Any] | None) -> str:
        return json.dumps(
            {"digest": digest, "result_code": code, "message": message, "result": dict(result) if result else None}
        )

    async def _replay(
        self, operation_key: str | None, digest: str, recorded: str | None
    ) -> tuple[int, str, Mapping[str, Any] | None, bool]:
        """The answer to a repeated operation, from what the ledger holds."""
        if recorded is None:
            # Started and never answered: the service stopped while executing
            # it. Recorded, so every further repeat gets the same answer.
            message = "outcome unknown: the service stopped while executing this operation; it may have taken effect"
            if operation_key:
                await asyncio.to_thread(
                    self._ledger.command_answered,
                    operation_key,
                    self._operation_record(digest, ACK_UNKNOWN, message, None),
                )
            return ACK_UNKNOWN, message, None, True
        entry = json.loads(recorded)
        if entry.get("digest") != digest:
            return (
                ACK_CONFLICT,
                "refused: this operation_id was already used for a different command; not executed",
                # A connector's 409 is also a write read back different; the
                # outcome tells the two apart.
                {"outcome": "refused"},
                False,
            )
        return int(entry["result_code"]), str(entry.get("message") or ""), entry.get("result"), True

    @staticmethod
    def _answer_body(
        correlation_id: str,
        code: int,
        message: str,
        result: Mapping[str, Any] | None = None,
        *,
        command: Command | None = None,
        replayed: bool = False,
    ) -> str:
        body: dict[str, Any] = {
            "correlation_id": correlation_id,
            "result_code": code,
            "message": message,
            "performed_at": time.time(),
        }
        if result:
            body["result"] = dict(result)
        if command is not None and command.operation_id:
            body["operation_id"] = command.operation_id
        if command is not None and command.on_behalf_of is not None:
            body["on_behalf_of"] = command.on_behalf_of.envelope()
        if replayed:
            body["replayed"] = True
        return json.dumps(body)

    async def _publish_answer(self, topic: str, correlation_id: str, answer: str) -> None:
        """Publish ``answer`` until the node confirms it. While the broker is
        away it waits for the link; a failed attempt is retried with jittered
        backoff. Raises :class:`_Stopping` when the service stops first: the
        cursor then stays before the command, and the ledger holds the answer."""
        from chaski.outage import Outage
        from chaski.retry import Backoff

        retry = Backoff(minimum=min(0.5, ANSWER_BACKOFF_MAX_S), maximum=ANSWER_BACKOFF_MAX_S)
        outage = Outage(log, f"commands: answer {correlation_id} on {topic}")
        while True:
            if not self._link.is_set():
                await self._wait_for_link()
            try:
                await asyncio.to_thread(self._send, topic, answer)
            except Exception as exc:
                delay = retry.delay(exc)
                count = self._health.failed(ANSWER_CONSUMER, exc) if self._health is not None else retry.failures
                if not outage.failed(exc, delay=delay):
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
            outage.recovered()
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

    async def _invoke(self, handler: Any, command: Command) -> Any:
        """Call one handler; a subclass adds what its handlers need around it."""
        return await handler(command)

    async def _run(self, handler: Any, command: Command) -> tuple[int, str, Mapping[str, Any] | None]:
        """Run one command and map what happened to an answer."""
        name = getattr(handler, "__qualname__", handler)
        deadline = None if command.expires_at is None else command.expires_at / 1000.0
        with writes_until(deadline):
            try:
                result = await self._invoke(handler, command)
            except CommandRejected as exc:
                return exc.code, exc.message, exc.result
            except NotSent as exc:
                log.warning("%s on %s: a write was refused: %s", name, command.path, exc)
                return ACK_FAILED, f"the command failed: {exc}"[:200], None
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    return ACK_BUSY, "the node is busy; send the command again", None
                log.exception("%s failed on %s", name, command.path)
                return ACK_FAILED, f"the command failed: the node answered {exc.response.status_code}", None
            except Exception as exc:
                if unconfirmed_write(exc):
                    log.warning("%s on %s: a write was not confirmed: %s", name, command.path, exc)
                    return (
                        ACK_UNKNOWN,
                        "outcome unknown: a write was sent but the node did not confirm it in time; it may still take effect",
                        None,
                    )
                log.exception("%s failed on %s", name, command.path)
                return ACK_FAILED, f"the command failed: {type(exc).__name__}: {exc}"[:200], None
        if isinstance(result, CommandResult):
            return ACK_OK, result.message, result.result
        return ACK_OK, "" if result is None else str(result), None
