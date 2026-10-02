"""``chaski.Doorbell``: the wake-up of a consumer that drains a stream.

Whatever says the stream may have grown — an MQTT message on the topics it
reads, a ``/watch`` hint, a reconnect — rings the bell, and the consumer drains
from its cursor until the stream is empty.

The bell rings only for what the consumer reads. A filtered consumer whose
topics stay silent would never drain, so its cursor would stand still while the
stream grows past it, and the node's pruner, which never cuts below the lowest
cursor, would keep everything. So a consumer also drains after
:data:`IDLE_DRAIN_S` without a ring: the drain finds nothing to hand out but
acks the filtered pages it walked, moving the cursor over records that are
not its own. That is the only work done on a timer.

The consumer takes :attr:`Doorbell.generation` **before** it drains and then
waits for a newer one. A ring that arrives while it drains therefore leads to
one more drain instead of being lost, and many rings during one drain cost one
drain. Thread-safe: MQTT callbacks ring from paho's network thread.

A consumer waits on its bell and its ``stop`` together
(``wait_after(seen, stop=stop)``): setting ``stop`` ends the wait without a
ring, so a consumer whose bell nothing can ring any more (rescoped to no
topics) still notices it is to stop.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable

#: How long a consumer waits on a silent bell before it drains anyway, to walk
#: its cursor past records its filter skips (see the module docstring).
IDLE_DRAIN_S = 300.0


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
            if not loop.is_closed():
                loop.call_soon_threadsafe(_resolve, future)

    def wait_after(self, since: int, timeout: float | None = None, *, stop: threading.Event | None = None) -> bool:
        """Block until the bell rang after ``since`` was taken (at once when it
        already has). False when ``timeout`` passed or ``stop`` was set first."""
        if stop is None:
            with self._cond:
                return self._cond.wait_for(lambda: self._generation != since, timeout)
        release = _when_set(stop, self._wake_waiters)
        try:
            with self._cond:
                self._cond.wait_for(lambda: self._generation != since or stop.is_set(), timeout)
                return self._generation != since
        finally:
            release()

    def _wake_waiters(self) -> None:
        with self._cond:
            self._cond.notify_all()

    async def after(self, since: int) -> None:
        """Wait on the running loop until the bell rang after ``since``."""
        loop = asyncio.get_running_loop()
        with self._cond:
            if self._generation != since:
                return
            future: asyncio.Future[None] = loop.create_future()
            self._async.append((loop, future))
        try:
            await future
        finally:
            with self._cond:
                if (loop, future) in self._async:
                    self._async.remove((loop, future))


_hooks_lock = threading.Lock()
_HOOKS = "_chaski_when_set"


def _when_set(event: threading.Event, callback: Callable[[], None]) -> Callable[[], None]:
    """Call ``callback`` when ``event`` is set; returns the function that
    stops that. ``threading.Event`` has no hook of its own, so the instance's
    ``set`` is wrapped once and calls the registered callbacks after setting
    the flag."""
    with _hooks_lock:
        hooks: list[Callable[[], None]] | None = event.__dict__.get(_HOOKS)
        if hooks is None:
            hooks = []
            original = event.set
            registered = hooks

            def set_and_notify() -> None:
                original()
                with _hooks_lock:
                    pending = list(registered)
                for hook in pending:
                    hook()

            setattr(event, _HOOKS, hooks)
            event.set = set_and_notify  # type: ignore[method-assign]
        hooks.append(callback)

    def release() -> None:
        with _hooks_lock:
            if callback in hooks:
                hooks.remove(callback)

    return release


def _resolve(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)
