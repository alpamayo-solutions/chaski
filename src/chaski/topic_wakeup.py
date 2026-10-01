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

A topic may be an MQTT filter with ``+`` and ``#`` (``colca/v1/_Signal/+/Line/#``):
the bell then rings for every message whose topic the filter matches, for a
consumer that wakes when anything under its scope changes.

Several wake-ups of one service may want the same topic. paho keeps one
callback per topic, so they share one :class:`TopicFanout`: it subscribes a
topic once, rings every wake-up that wants it, and unsubscribes it only when
the last of them lets go.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable
from typing import Any

from paho.mqtt.client import topic_matches_sub

from .doorbell import Doorbell

QOS = 1


class TopicFanout:
    """One MQTT subscription and callback per topic, shared by wake-ups."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._lock = threading.Lock()
        self._wanted: dict[str, set[TopicWakeup]] = {}

    def add(self, topic: str, wakeup: TopicWakeup) -> None:
        with self._lock:
            holders = self._wanted.setdefault(topic, set())
            first = not holders
            holders.add(wakeup)
        if first:
            self._client.message_callback_add(topic, self._on_message)
            self._client.subscribe(topic, qos=QOS)

    def remove(self, topic: str, wakeup: TopicWakeup) -> None:
        with self._lock:
            holders = self._wanted.get(topic)
            if not holders:
                return
            holders.discard(wakeup)
            last = not holders
            if last:
                del self._wanted[topic]
        if last:
            self._client.message_callback_remove(topic)
            self._client.unsubscribe(topic)

    def _on_message(self, _client: Any, _userdata: Any, message: Any) -> None:
        # A message carries its concrete topic; the wake-ups are keyed by the
        # filter they subscribed, which may hold ``+`` or ``#``.
        topic = str(getattr(message, "topic", ""))
        with self._lock:
            if not topic:
                holders = {w for s in self._wanted.values() for w in s}
            else:
                holders = {
                    w
                    for wanted, subscribed in self._wanted.items()
                    if wanted == topic or (_is_filter(wanted) and topic_matches_sub(wanted, topic))
                    for w in subscribed
                }
        for wakeup in holders:
            wakeup.bell.ring()


def _is_filter(topic: str) -> bool:
    return "+" in topic or "#" in topic


class TopicWakeup:
    """See the module docstring. Obtain one from :meth:`chaski.Service.wake_on`."""

    def __init__(self, fanout: Any, topics: Iterable[str] = ()) -> None:
        self.bell = Doorbell()
        # A bare client (tests, single consumers) gets a fanout of its own.
        self._fanout = fanout if isinstance(fanout, TopicFanout) else TopicFanout(fanout)
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
            self._fanout.add(topic, self)
        for topic in removed:
            self._fanout.remove(topic, self)
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
            self._fanout.remove(topic, self)
