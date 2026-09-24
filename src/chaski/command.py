"""Send a command over an MQTT session and wait for its ``_Ack``.

:class:`CommandSender` works on any franzmq client connected to a node, so a
process that keeps its own session (a bridge with its own catalogue) sends
commands the same way :meth:`chaski.Service.command` does.

The executor answers at ``_Ack/<node>/<path>``, the command's own position.
The sender subscribes there on first use and keeps the subscription; it goes
out before the command on the same session, so the broker holds it before
the ack exists. The command carries ``correlation_id`` and ``expires_at``
(unix milliseconds) at the top level, next to the verb's own fields, which
is where the node reads them.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import ulid as ulid_lib
from colca_data_contracts import topic_prefix


@dataclass
class _Waiter:
    event: threading.Event = field(default_factory=threading.Event)
    ack: dict[str, Any] = field(default_factory=dict)


class CommandSender:
    """Commands to one node on one franzmq session.

    The client must survive a message franzmq cannot decode
    (:func:`chaski.service.tolerate_undecodable`): a configure ack carries
    ``state_writes``, which franzmq's ``Ack`` type does not have.
    """

    def __init__(self, client: Any, node_id: str) -> None:
        self._client = client
        self._node_id = node_id
        self._lock = threading.Lock()
        self._topics: set[str] = set()
        self._waiters: dict[str, _Waiter] = {}

    def command(
        self,
        contract: str,
        path: str,
        fields: dict[str, Any] | None = None,
        *,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Send ``fields`` as ``contract`` to ``path`` on the node and return
        the ``_Ack`` as the executor wrote it. The command expires
        ``timeout`` from now, so one nobody executed in time is not executed
        later. Raises ``TimeoutError`` without an ack; a ``result_code``
        other than 200 is returned, not raised. Must not be called from an
        MQTT callback, which cannot wait for its own PUBACK."""
        topic = f"{topic_prefix()}{contract}/{self._node_id}/{path}"
        ack_topic = f"{topic_prefix()}_Ack/{self._node_id}/{path}"
        correlation_id = str(ulid_lib.new())
        waiter = _Waiter()
        with self._lock:
            self._waiters[correlation_id] = waiter
            subscribe = ack_topic not in self._topics
            self._topics.add(ack_topic)
        try:
            if subscribe:
                self._client.message_callback_add(ack_topic, self._on_ack)
                self._client.subscribe(ack_topic, qos=1)
            payload = {
                **(fields or {}),
                "correlation_id": correlation_id,
                "expires_at": int((time.time() + timeout) * 1000),
            }
            # franzmq sends ``payload.encode()``, which a JSON string already has.
            self._client.publish(topic, json.dumps(payload), qos=1)
            if not waiter.event.wait(timeout):
                raise TimeoutError(f"no _Ack on {ack_topic} within {timeout:.0f}s")
            return waiter.ack
        finally:
            with self._lock:
                self._waiters.pop(correlation_id, None)

    def resubscribe(self, client: Any) -> None:
        """Subscribe every ack topic again, after a reconnect that lost the
        session. Nothing here waits, so it may run on the network thread."""
        with self._lock:
            topics = sorted(self._topics)
        for topic in topics:
            client.subscribe(topic, qos=1)

    def _on_ack(self, _client: Any, _userdata: Any, message: Any) -> None:
        """franzmq hands the ack decoded when its ``Ack`` type fits, and raw
        when the ack carries more."""
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
        if waiter is not None:
            waiter.ack = payload
            waiter.event.set()
