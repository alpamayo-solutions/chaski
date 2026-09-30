"""Send a command over an MQTT session and wait for its ``_Ack``.

:class:`CommandSender` works on any franzmq client connected to a node, so a
process that keeps its own session (a bridge with its own catalogue) sends
commands the same way :meth:`chaski.Service.command` does.

**Where it goes.** A command is addressed to a node and a path in the
sender's own coordinates: ``node`` is the node that executes it (this node by
default, or a node below it) and ``path`` is the position as the sender's node
sees it, the child's mount included. The executor answers at
``_Ack/<node>/<path>``, and that is also where the answer arrives at the
sender's node. The sender subscribes there on first use and keeps the
subscription; it goes out before the command on the same session, so the
broker holds it before the answer exists.

**How long it lives.** ``lifetime`` is the sender's decision and has no
default. ``None`` sends a command without ``expires_at``: it never expires and
is delivered whenever its target is reachable, also after days, as long as the
node's retention keeps it. A number of seconds sends ``expires_at`` (unix
milliseconds) that far ahead: once it passed, the executor answers ``498``
without running it. A command that acts on a physical machine should carry a
short lifetime.

**Accepted is not done.** :meth:`CommandSender.send` returns once the node
confirmed the publish (PUBACK): the command is stored and queued.
:meth:`SentCommand.wait` waits for its outcome, the first ``_Ack`` that is not
a ``202`` progress ack. A wait that times out raises :class:`TimeoutError` and
leaves the command queued; it is not cancelled. A sender that must learn an
outcome that may come days later reads the ``_Ack`` records from its node's
``commands`` stream with a cursor of its own, by correlation id
(:func:`is_progress` tells progress from outcome).
"""

from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import ulid as ulid_lib
from colca_data_contracts import topic_prefix

#: ``result_code`` of a progress ack: the command was queued or forwarded, it
#: has no outcome yet.
PROGRESS = 202
#: How many sent commands a sender keeps a waiter for. The oldest is forgotten
#: first; waiting on it then times out.
_WAITER_LIMIT = 4096


def is_progress(ack: Mapping[str, Any]) -> bool:
    """Whether ``ack`` is a ``202`` progress ack (``stage`` ``queued`` or
    ``forwarded``) rather than the command's outcome."""
    return ack.get("result_code") == PROGRESS


@dataclass
class _Waiter:
    event: threading.Event = field(default_factory=threading.Event)
    ack: dict[str, Any] = field(default_factory=dict)
    progress: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class SentCommand:
    """A command the node accepted. :meth:`wait` returns its outcome."""

    correlation_id: str
    topic: str
    ack_topic: str
    #: Unix milliseconds, or ``None`` for a command that never expires.
    expires_at: int | None
    _sender: CommandSender = field(repr=False)
    _waiter: _Waiter = field(repr=False)

    def wait(self, timeout: float) -> dict[str, Any]:
        """The command's outcome: its first ``_Ack`` that is not a progress ack,
        as the executor (or a node that answered it) wrote it. A
        ``result_code`` other than 200 is returned, not raised. Raises
        :class:`TimeoutError` when none arrived within ``timeout`` seconds; the
        command stays queued and may still run, and ``wait`` may be called
        again. Must not be called from an MQTT callback."""
        if not self._waiter.event.wait(timeout):
            raise TimeoutError(
                f"no outcome on {self.ack_topic} within {timeout:.0f}s; the command stays queued and its result is "
                "unknown: read the current state before relying on either outcome"
            )
        self._sender._forget(self.correlation_id)
        return self._waiter.ack

    @property
    def progress(self) -> list[dict[str, Any]]:
        """The ``202`` progress acks seen so far, in arrival order. A node
        writes them only for a command sent with ``progress=True``."""
        with self._sender._lock:
            return list(self._waiter.progress)


class CommandSender:
    """Commands from one franzmq session, addressed to its node or a node below.

    The client must survive a message franzmq cannot decode
    (:func:`chaski.service.tolerate_undecodable`): a configure ack carries
    ``state_writes``, which franzmq's ``Ack`` type does not have.
    """

    def __init__(self, client: Any, node_id: str) -> None:
        self._client = client
        self._node_id = node_id
        self._lock = threading.Lock()
        self._topics: set[str] = set()
        self._waiters: OrderedDict[str, _Waiter] = OrderedDict()

    def send(
        self,
        contract: str,
        path: str,
        fields: dict[str, Any] | None = None,
        *,
        lifetime: float | None,
        node: str | None = None,
        progress: bool = False,
    ) -> SentCommand:
        """Send ``fields`` as ``contract`` to ``path`` at ``node`` (this node
        when ``None``) and return once the node accepted it. ``lifetime`` is
        seconds until it expires, or ``None`` for a command that never
        expires. ``progress`` asks the nodes for ``202`` acks while it is
        queued and forwarded. ``path`` is in this node's coordinates: for a
        node below, it starts with that node's mount."""
        if lifetime is not None and lifetime <= 0:
            raise ValueError(f"lifetime must be positive or None, got {lifetime}")
        target = node or self._node_id
        topic = f"{topic_prefix()}{contract}/{target}/{path}"
        ack_topic = f"{topic_prefix()}_Ack/{target}/{path}"
        correlation_id = str(ulid_lib.new())
        expires_at = None if lifetime is None else int((time.time() + lifetime) * 1000)
        waiter = _Waiter()
        with self._lock:
            self._waiters[correlation_id] = waiter
            while len(self._waiters) > _WAITER_LIMIT:
                self._waiters.popitem(last=False)
            subscribe = ack_topic not in self._topics
            self._topics.add(ack_topic)
        if subscribe:
            self._client.message_callback_add(ack_topic, self._on_ack)
            self._client.subscribe(ack_topic, qos=1)
        payload: dict[str, Any] = {**(fields or {}), "correlation_id": correlation_id}
        if expires_at is not None:
            payload["expires_at"] = expires_at
        if progress:
            payload["progress"] = True
        try:
            # franzmq sends ``payload.encode()``, which a JSON string already has.
            self._client.publish(topic, json.dumps(payload), qos=1)
        except BaseException:
            self._forget(correlation_id)
            raise
        return SentCommand(correlation_id, topic, ack_topic, expires_at, self, waiter)

    def command(
        self,
        contract: str,
        path: str,
        fields: dict[str, Any] | None = None,
        *,
        lifetime: float | None,
        node: str | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """:meth:`send` and :meth:`SentCommand.wait` in one call: the outcome,
        or :class:`TimeoutError` after ``timeout`` seconds, which leaves the
        command queued. Must not be called from an MQTT callback, which cannot
        wait for its own PUBACK."""
        return self.send(contract, path, fields, lifetime=lifetime, node=node).wait(timeout)

    def _forget(self, correlation_id: str) -> None:
        with self._lock:
            self._waiters.pop(correlation_id, None)

    def _on_ack(self, _client: Any, _userdata: Any, message: Any) -> None:
        """franzmq hands the ack decoded, or raw when it cannot decode it."""
        payload = message.payload
        if isinstance(payload, (bytes, bytearray)):
            try:
                payload = json.loads(payload)
            except ValueError:
                return
        elif payload is not None and not isinstance(payload, dict):
            payload = dict(vars(payload))
        if not isinstance(payload, dict):
            return
        with self._lock:
            waiter = self._waiters.get(str(payload.get("correlation_id") or ""))
            if waiter is None or waiter.event.is_set():
                return
            if is_progress(payload):
                waiter.progress.append(payload)
                return
            waiter.ack = payload
        waiter.event.set()
