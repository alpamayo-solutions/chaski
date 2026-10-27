"""Pipelined publishes: send now, confirm before the commit that depends on them.

A QoS 1 publish is confirmed by the node's PUBACK. Waiting for it after every
value caps a producer at one round trip per value. Inside a
:class:`PublishBatch` a service's writes (:meth:`chaski.Service.send`,
:meth:`~chaski.Service.retract`, :meth:`~chaski.Service.publish`) are handed
to the MQTT client and the batch keeps them unconfirmed, at most
:data:`PUBLISH_WINDOW` at a time: the next write waits for the oldest. Whoever
opened the batch calls :meth:`PublishBatch.settle` before it commits anything
that depends on the writes (an ingest page's ack, a backfill window's
position, a tick's watermark), and a refused or unconfirmed write raises
there as it would have from the write itself.

Outside a batch, and in any other task or thread than the one that opened it,
a write waits for its PUBACK as before.

The node's verdict is read from the client's public ``on_publish`` callback
(:class:`Pubacks`), which hands over each PUBACK's reason code.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Iterator
from contextvars import ContextVar
from typing import Any

from franzmq.errors import PublishRejected, PublishTimeout

#: Unconfirmed writes a batch keeps before the next one waits for the oldest.
PUBLISH_WINDOW = 64

#: PUBACKs that arrived before their write was registered, kept per mid. The
#: window is a few instructions wide; the bound only keeps the PUBACKs of
#: writes nobody registers (waited publishes) from piling up.
_EARLY_LIMIT = 256


def _current_task() -> asyncio.Task | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


class Unconfirmed:
    """One write handed to the client, waiting for its PUBACK."""

    __slots__ = ("_acks", "_code", "_done", "deadline", "mid", "origin", "timeout", "topic")

    def __init__(self, acks: Pubacks, mid: int, topic: str, timeout: float) -> None:
        self._acks = acks
        self.mid = mid
        self.topic = topic
        self.timeout = timeout
        self.deadline = time.monotonic() + timeout
        self._done = threading.Event()
        self._code = 0
        #: Who wrote it, for the failure a batch reports (an ingest handler).
        self.origin: str | None = None

    def _resolve(self, code: int) -> None:
        self._code = code
        self._done.set()

    @property
    def done(self) -> bool:
        return self._done.is_set()

    def wait(self) -> None:
        """Return once the node confirmed the write; raise ``PublishRejected``
        when it refused it, ``PublishTimeout`` when no PUBACK came in time."""
        if not self._done.wait(max(0.0, self.deadline - time.monotonic())):
            self._acks.forget(self)
            raise PublishTimeout(self.topic, self.timeout)
        if self._code >= 0x80:
            raise PublishRejected(self._code, self.topic)


class Pubacks:
    """Matches each PUBACK to the write it confirms, through the client's
    public ``on_publish`` callback (MQTT 5: it carries the reason code).
    Installed once per client, before its network loop starts."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._lock = threading.Lock()
        self._waiting: dict[int, Unconfirmed] = {}
        self._early: OrderedDict[int, int] = OrderedDict()
        client.on_publish = self._on_publish

    def _on_publish(self, _client: Any, _userdata: Any, mid: int, reason_code: Any = None, _props: Any = None) -> None:
        # On the network thread, inside paho's lock of outgoing messages:
        # only this tracker's own lock is taken here.
        code = int(getattr(reason_code, "value", reason_code) or 0)
        with self._lock:
            pending = self._waiting.pop(mid, None)
            if pending is None:
                self._early[mid] = code
                while len(self._early) > _EARLY_LIMIT:
                    self._early.popitem(last=False)
        if pending is not None:
            pending._resolve(code)

    def send(self, topic: str, payload: Any, *, retain: bool = False, tombstone: bool = False) -> Unconfirmed:
        """Hand one QoS 1 write to the client without waiting for its PUBACK."""
        client = self._client
        if tombstone:
            info = client.publish_tombstone(topic, qos=1, wait=False)
        else:
            info = client.publish(topic, payload, qos=1, retain=retain, wait=False)
        timeout = float(getattr(client, "publish_timeout", 10.0))
        pending = Unconfirmed(self, info.mid, str(topic), timeout)
        with self._lock:
            early = self._early.pop(info.mid, None)
            if early is None:
                self._waiting[info.mid] = pending
        if early is not None:
            pending._resolve(early)
        return pending

    def forget(self, pending: Unconfirmed) -> None:
        with self._lock:
            if self._waiting.get(pending.mid) is pending:
                del self._waiting[pending.mid]


_batch: ContextVar[tuple[PublishBatch, asyncio.Task | None] | None] = ContextVar("chaski_publish_batch", default=None)


class PublishBatch:
    """Writes made in one block, confirmed together; see the module docstring."""

    def __init__(self, window: int = PUBLISH_WINDOW) -> None:
        if window < 1:
            raise ValueError("a publish window holds at least one write")
        self._window = window
        self._pending: deque[Unconfirmed] = deque()
        #: Who the next writes are made for, kept with each (``Unconfirmed.origin``).
        self.origin: str | None = None
        #: ``origin`` of the write whose failure :meth:`settle` raised last.
        self.failed_origin: str | None = None
        self.sent = 0

    @staticmethod
    def current() -> PublishBatch | None:
        """The batch the current task (or thread, outside a loop) opened, if any."""
        active = _batch.get()
        if active is None or active[1] is not _current_task():
            return None
        return active[0]

    @contextlib.contextmanager
    def active(self) -> Iterator[PublishBatch]:
        """Collect this task's writes until the block ends. Does not settle."""
        token = _batch.set((self, _current_task()))
        try:
            yield self
        finally:
            _batch.reset(token)

    def add(self, pending: Unconfirmed) -> None:
        """Keep ``pending``; with the window full, wait for the oldest first.
        A failure of a write waited for here raises at once."""
        pending.origin = self.origin
        self._pending.append(pending)
        self.sent += 1
        while self._pending and (self._pending[0].done or len(self._pending) > self._window):
            self._pending.popleft().wait()

    def settle(self) -> None:
        """Wait for every write still unconfirmed. Raises the first failure
        after waiting for all; :attr:`failed_origin` names who made that write."""
        failure: BaseException | None = None
        while self._pending:
            pending = self._pending.popleft()
            try:
                pending.wait()
            except Exception as exc:
                if failure is None:
                    self.failed_origin = pending.origin
                    failure = exc
        if failure is not None:
            raise failure

    def __len__(self) -> int:
        return len(self._pending)


def settle_current() -> None:
    """Settle the batch the current task opened, if any: what a producer's
    durable progress (a watermark, a checkpoint) depends on is confirmed first."""
    batch = PublishBatch.current()
    if batch is not None:
        batch.settle()


__all__ = ["PUBLISH_WINDOW", "Pubacks", "PublishBatch", "Unconfirmed", "settle_current"]
