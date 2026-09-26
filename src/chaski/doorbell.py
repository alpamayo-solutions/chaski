"""``chaski.Doorbell``: the wake-up of a consumer that drains a stream.

A consumer reads nothing on a timer. Whatever says the stream may have grown —
an MQTT message on the topics it reads, a ``/watch`` hint, a reconnect — rings
the bell, and the consumer drains from its cursor until the stream is empty.

The consumer takes :attr:`Doorbell.generation` **before** it drains and then
waits for a newer one. A ring that arrives while it drains therefore leads to
one more drain instead of being lost, and many rings during one drain cost one
drain. Thread-safe: MQTT callbacks ring from paho's network thread.
"""

from __future__ import annotations

import asyncio
import threading


class Doorbell:
    """A generation counter that consumers wait on; see the module docstring."""

    def __init__(self) -> None:
        self._generation = 0
        self._cond = threading.Condition()
        self._async: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []

    @property
    def generation(self) -> int:
        """Increases with every :meth:`ring`."""
        with self._cond:
            return self._generation

    def ring(self) -> None:
        """The stream may have grown. Safe from any thread."""
        with self._cond:
            self._generation += 1
            self._cond.notify_all()
            waiters, self._async = self._async, []
        for loop, future in waiters:
            loop.call_soon_threadsafe(_resolve, future)

    def wait_after(self, since: int, timeout: float | None = None) -> bool:
        """Block until the bell rang after ``since`` was taken (at once when it
        already has). False when ``timeout`` passed first."""
        with self._cond:
            return self._cond.wait_for(lambda: self._generation != since, timeout)

    async def after(self, since: int) -> None:
        """Wait on the running loop until the bell rang after ``since``."""
        loop = asyncio.get_running_loop()
        with self._cond:
            if self._generation != since:
                return
            future: asyncio.Future[None] = loop.create_future()
            self._async.append((loop, future))
        await future


def _resolve(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)
