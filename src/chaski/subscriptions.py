"""Every subscription a client holds, restored after a reconnect.

A node that restarts forgets the MQTT session, and with it every subscription
the service made, however long the session expiry was. The service reconnects
and still publishes, but no command, wake-up or watched record reaches it
again. :class:`Subscriptions` records each ``subscribe``/``unsubscribe`` on
the client and subscribes the whole set again on :meth:`restore`.

It also retries a refused SUBACK: the broker answers "packet identifier in
use" to a SUBSCRIBE whose id matches one of its own in-flight QoS 1
deliveries (mochi shares one id space for both directions), which happens
while a fresh session receives its retained records.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable
from typing import Any

from .retry import Backoff

logger = logging.getLogger(__name__)

RETRY_S = 1.0


def _refused(code: Any) -> bool:
    """A SUBACK reason code at or above 0x80 refuses the subscription."""
    value = getattr(code, "value", code)
    try:
        return int(value) >= 0x80
    except (TypeError, ValueError):
        return bool(getattr(code, "is_failure", False))


class Subscriptions:
    """Install on a client before its loop starts; see the module docstring."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._held: dict[str, int] = {}
        self._pending: dict[int, str] = {}
        self._retry: dict[str, Backoff] = {}
        self._timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()
        subscribe, unsubscribe = client.subscribe, client.unsubscribe

        def tracked_subscribe(topic: Any, qos: int = 0, *args: Any, **kwargs: Any) -> Any:
            with self._lock:
                self._held[str(topic)] = qos
            result = subscribe(topic, qos, *args, **kwargs)
            self._sent(str(topic), result)
            return result

        def tracked_unsubscribe(topic: Any, *args: Any, **kwargs: Any) -> Any:
            with self._lock:
                self._held.pop(str(topic), None)
                self._retry.pop(str(topic), None)
                if timer := self._timers.pop(str(topic), None):
                    timer.cancel()
            return unsubscribe(topic, *args, **kwargs)

        client.subscribe = tracked_subscribe
        client.unsubscribe = tracked_unsubscribe
        client.on_subscribe = self._on_subscribe

    def close(self) -> None:
        """Cancel refused-subscription retries when their client closes."""
        with self._lock:
            self._held.clear()
            self._pending.clear()
            self._retry.clear()
            for timer in self._timers.values():
                timer.cancel()
            self._timers.clear()

    def topics(self) -> dict[str, int]:
        with self._lock:
            return dict(self._held)

    def restore(self, *, later: Iterable[str] = ()) -> int:
        """Subscribe every held topic again, except those the caller renews
        itself ``later``. Callbacks stay registered on the client, so none is
        passed. Nothing here waits."""
        held = set(self.topics()) - set(later)
        for topic in sorted(held):
            self.renew(topic)
        return len(held)

    def renew(self, topic: str) -> None:
        """Subscribe one held topic again; a topic no longer held is skipped."""
        with self._lock:
            qos = self._held.get(topic)
            if timer := self._timers.pop(topic, None):
                timer.cancel()
        if qos is None:
            return
        self._client.subscribe(topic, qos)

    def _sent(self, topic: str, result: Any) -> None:
        mid = result[1] if isinstance(result, tuple) and len(result) > 1 else None
        if mid is not None:
            with self._lock:
                self._pending[mid] = topic

    def _on_subscribe(self, _client: Any, _userdata: Any, mid: int, reason_codes: Any, _properties: Any = None) -> None:
        with self._lock:
            topic = self._pending.pop(mid, None)
            held = topic in self._held
        if topic is None or not held:
            return
        if not any(_refused(code) for code in reason_codes or ()):
            with self._lock:
                self._retry.pop(topic, None)
                if timer := self._timers.pop(topic, None):
                    timer.cancel()
            return
        logger.warning(
            "subscription to %s refused (%s) — retrying",
            topic,
            ", ".join(str(code) for code in reason_codes),
        )
        with self._lock:
            if topic not in self._held or topic in self._timers:
                return
            backoff = self._retry.setdefault(topic, Backoff(minimum=RETRY_S))
            timer = threading.Timer(backoff.delay(), self.renew, (topic,))
            timer.daemon = True
            self._timers[topic] = timer
            timer.start()
