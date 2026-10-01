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

**Who and which operation.** Two optional envelope fields sit beside
``correlation_id`` and ``expires_at``:

* ``operation_id`` — the sender's idempotency key for one logical operation,
  kept the same when the sender retries it (an HTTP request repeated by a
  browser, a resend after a timeout). The executor records the outcome under
  ``(attested sender, operation_id)``; a repeat is answered with that recorded
  outcome and ``"replayed": true``, and is never executed twice. A repeat
  whose contract, path, ``command`` or ``on_behalf_of`` differ is answered
  ``409``. Whatever the first execution answered is what a repeat gets, a
  failure included: a new attempt after a failure is a new operation with a
  new id. A command that expired, or was refused for its envelope, was not
  executed and is not recorded. Each send still gets its own
  ``correlation_id``. At most
  :data:`OPERATION_ID_MAX` characters, no ``/`` and no control characters.
* ``on_behalf_of`` — the person (or system) the sender acts for, as
  ``{"id": ..., "label": ..., "kind": ...}`` (:class:`Actor`). ``kind``
  defaults to ``human``.

Trust: the node attests the sender. It authenticated the session that
published the command and stamps that identity on the stored record; the
executor reads the sender from there, never from the payload. ``on_behalf_of``
is not attested by the node: it is the sender's claim, and it is exactly as
trustworthy as the sender that made it. Grant the command class only to
services that authenticate the people they act for. A reader shows it as
"<on_behalf_of> via <sender>", never as the sender. A person (a sender of kind
``human``) cannot act on behalf of anyone else: the executor refuses that
``403``.
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

#: The longest ``operation_id`` a command may carry.
OPERATION_ID_MAX = 128
#: The longest ``on_behalf_of`` id or label.
ACTOR_FIELD_MAX = 255
#: The ``on_behalf_of`` kinds a sender may assert.
ACTOR_KINDS = frozenset({"human", "service", "system"})


class EnvelopeError(ValueError):
    """An ``operation_id`` or ``on_behalf_of`` a command must not carry."""


@dataclass(frozen=True)
class Actor:
    """Someone a command names: the attested sender (``Command.sender``) or
    the person it acts for (``Command.on_behalf_of``)."""

    id: str
    label: str = ""
    kind: str = "human"

    def envelope(self) -> dict[str, str]:
        """The ``on_behalf_of`` object on the wire."""
        body = {"id": self.id, "kind": self.kind}
        if self.label:
            body["label"] = self.label
        return body

    @classmethod
    def coerce(cls, value: Actor | Mapping[str, Any] | str) -> Actor:
        """An :class:`Actor` from an id, a wire object or an Actor, validated.
        Raises :class:`EnvelopeError`."""
        if isinstance(value, Actor):
            actor = value
        elif isinstance(value, str):
            actor = cls(value)
        elif isinstance(value, Mapping):
            unknown = set(value) - {"id", "label", "kind"}
            if unknown:
                raise EnvelopeError(f"on_behalf_of: unknown field(s) {sorted(unknown)}")
            fields = {key: value[key] for key in ("id", "label", "kind") if value.get(key) is not None}
            if not all(isinstance(item, str) for item in fields.values()):
                raise EnvelopeError("on_behalf_of: id, label and kind are strings")
            actor = cls(**fields) if "id" in fields else cls("")
        else:
            raise EnvelopeError("on_behalf_of must be an object with an id")
        if not actor.id.strip():
            raise EnvelopeError("on_behalf_of needs an id")
        if len(actor.id) > ACTOR_FIELD_MAX or len(actor.label) > ACTOR_FIELD_MAX:
            raise EnvelopeError(f"on_behalf_of: id and label are at most {ACTOR_FIELD_MAX} characters")
        if actor.kind not in ACTOR_KINDS:
            raise EnvelopeError(f"on_behalf_of: kind must be one of {sorted(ACTOR_KINDS)}")
        if _has_control(actor.id) or _has_control(actor.label):
            raise EnvelopeError("on_behalf_of: control characters are not allowed")
        return actor


def _has_control(text: str) -> bool:
    return any(ord(char) < 0x20 or ord(char) == 0x7F for char in text)


def check_operation_id(value: Any) -> str:
    """``value`` as an ``operation_id``, or :class:`EnvelopeError`."""
    if not isinstance(value, str) or not value:
        raise EnvelopeError("operation_id must be a non-empty string")
    if len(value) > OPERATION_ID_MAX:
        raise EnvelopeError(f"operation_id is longer than {OPERATION_ID_MAX} characters")
    if "/" in value or _has_control(value):
        raise EnvelopeError("operation_id must not contain '/' or control characters")
    return value


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
        operation_id: str | None = None,
        on_behalf_of: Actor | Mapping[str, Any] | str | None = None,
    ) -> SentCommand:
        """Send ``fields`` as ``contract`` to ``path`` at ``node`` (this node
        when ``None``) and return once the node accepted it. ``lifetime`` is
        seconds until it expires, or ``None`` for a command that never
        expires. ``progress`` asks the nodes for ``202`` acks while it is
        queued and forwarded. ``path`` is in this node's coordinates: for a
        node below, it starts with that node's mount. ``operation_id`` and
        ``on_behalf_of`` are the envelope fields the module describes (an
        ``on_behalf_of`` string is the person's id)."""
        if lifetime is not None and lifetime <= 0:
            raise ValueError(f"lifetime must be positive or None, got {lifetime}")
        envelope: dict[str, Any] = {}
        if operation_id is not None:
            envelope["operation_id"] = check_operation_id(operation_id)
        if on_behalf_of is not None:
            envelope["on_behalf_of"] = Actor.coerce(on_behalf_of).envelope()
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
        payload: dict[str, Any] = {**(fields or {}), **envelope, "correlation_id": correlation_id}
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
        operation_id: str | None = None,
        on_behalf_of: Actor | Mapping[str, Any] | str | None = None,
    ) -> dict[str, Any]:
        """:meth:`send` and :meth:`SentCommand.wait` in one call: the outcome,
        or :class:`TimeoutError` after ``timeout`` seconds, which leaves the
        command queued. Must not be called from an MQTT callback, which cannot
        wait for its own PUBACK."""
        return self.send(
            contract, path, fields, lifetime=lifetime, node=node, operation_id=operation_id, on_behalf_of=on_behalf_of
        ).wait(timeout)

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
