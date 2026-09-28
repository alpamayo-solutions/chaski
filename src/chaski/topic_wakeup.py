"""``chaski.TopicWakeup``: a push wake-up scoped to exactly the topics a
consumer reads.

A consumer that needs a few signals out of a busy node must neither read the
whole stream nor be woken by all of it. It drains a stream scoped server side
(``Service.stream(..., signal_ids=...)``) and is woken by an MQTT subscription
on exactly the ``_Metric`` topics of those signals: a change anywhere else
costs it nothing. This is the pattern DataOps uses for its inputs, available
to any service.

The payload is not read; the bell only says the scoped stream may have grown.
New topics and a reconnect ring once, for what arrived while nothing listened.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable
from typing import Any

from .doorbell import Doorbell

QOS = 1


class TopicWakeup:
    """See the module docstring. Obtain one from :meth:`chaski.Service.wake_on`."""

    def __init__(self, client: Any, topics: Iterable[str] = ()) -> None:
        self.bell = Doorbell()
        self._client = client
        self._lock = threading.Lock()
        self._topics: set[str] = set()
        self._closed = False
        self.rebind(topics)

    @property
    def topics(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._topics)

    def rebind(self, topics: Iterable[str]) -> None:
        """Subscribe exactly ``topics``, dropping the ones no longer read."""
        wanted = {str(t) for t in topics}
        with self._lock:
            if self._closed:
                return
            added = sorted(wanted - self._topics)
            removed = sorted(self._topics - wanted)
            self._topics = wanted
        for topic in added:
            self._client.message_callback_add(topic, self._on_message)
            self._client.subscribe(topic, qos=QOS)
        for topic in removed:
            self._client.message_callback_remove(topic)
            self._client.unsubscribe(topic)
        if added:
            # Records may have arrived before the subscription existed.
            self.bell.ring()

    def reconnected(self) -> None:
        """The connection was down; what arrived meanwhile was not heard."""
        self.bell.ring()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            topics, self._topics = sorted(self._topics), set()
        for topic in topics:
            self._client.message_callback_remove(topic)
            self._client.unsubscribe(topic)

    def _on_message(self, _client: Any, _userdata: Any, _message: Any) -> None:
        self.bell.ring()
