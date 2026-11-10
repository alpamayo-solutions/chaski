"""One worker per producer: the thread and event loop all its code runs on.

Every callback of a producer runs on its worker, one at a time, in the order
it was handed over: ``setup``/``on_ready``/``teardown``, ``@on_metric``
handlers (a whole ingest page's worth at a time, :mod:`chaski.dataops.ingest`),
``@every``/``@cron`` ticks, ``@on_constant``/``@on_signal`` handlers,
``@on_command`` handlers and backfill windows. Nothing of a producer runs on
the service's event loop, and no thread waits on a producer lock: the loop,
the ingest, the scheduler and the backfill runner hand work over and wait for
its result (:meth:`ProducerWorker.run`, :meth:`ProducerWorker.call`).

The worker's loop runs for the producer's lifetime, so what a callback starts
(a trailing run, a retry, a ``call_later``) runs on it between callbacks, and
asyncio objects a producer keeps (a ``Lock``, an ``Event``) belong to one loop.
A callback that blocks (SQLite, an HTTP read, waiting for a publish
confirmation) holds up only its own producer.

Each piece of work runs as a task of its own, in a copy of the context of
whoever handed it over: context variables such as a command's write deadline
reach the producer's code as they did when it ran in the caller's task.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

log = logging.getLogger("chaski.dataops.worker")

T = TypeVar("T")

#: How long :meth:`ProducerWorker.stop` waits for the thread to end.
STOP_TIMEOUT_S = 10.0

_Work = tuple[Callable[[], Awaitable[Any]], concurrent.futures.Future, contextvars.Context]


class ProducerWorker:
    """A thread running one event loop, and the queue of work it runs on it
    one piece at a time (see the module docstring)."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[_Work | None] | None = None
        self._stopped = False
        self._serving: asyncio.Task[None] | None = None
        #: How many pieces of work wait or run; read by the health door.
        self.pending = 0
        self._pending_lock = threading.Lock()
        self._thread = threading.Thread(target=self._main, name=f"dataops-{name}", daemon=True)
        self._thread.start()
        self._ready.wait()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        loop = self._loop
        if loop is None:
            raise RuntimeError(f"the worker of {self.name} is not running")
        return loop

    def on_thread(self) -> bool:
        """Whether the caller runs on this worker's thread."""
        return threading.current_thread() is self._thread

    # -- the thread ---------------------------------------------------------

    def _main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._queue = asyncio.Queue()
        self._serving = loop.create_task(self._serve())
        self._serving.add_done_callback(lambda _task: loop.stop())
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            try:
                tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
                for task in tasks:
                    task.cancel()
                if tasks:
                    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
            finally:
                loop.close()

    async def _serve(self) -> None:
        queue = cast(asyncio.Queue, self._queue)  # set before the loop runs
        while True:
            work = await queue.get()
            if work is None:
                return
            fn, future, context = work
            try:
                if not future.set_running_or_notify_cancel():
                    continue
                task = asyncio.get_running_loop().create_task(_call(fn), context=context)
                try:
                    result = await task
                except asyncio.CancelledError as exc:
                    future.set_exception(concurrent.futures.CancelledError())
                    if task.cancelled() and not cast(asyncio.Task, asyncio.current_task()).cancelling():
                        continue  # the producer's own work was cancelled; go on
                    raise exc
                except BaseException as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)
            finally:
                with self._pending_lock:
                    self.pending -= 1

    # -- handing work over ----------------------------------------------------

    def submit(self, fn: Callable[[], Awaitable[T]]) -> concurrent.futures.Future[T]:
        """Queue ``fn()`` to run on this worker; its result, or exception, in
        the returned future. Safe from any thread."""
        future: concurrent.futures.Future[T] = concurrent.futures.Future()
        if self._stopped:
            future.set_exception(RuntimeError(f"the worker of {self.name} has stopped"))
            return future
        context = contextvars.copy_context()
        with self._pending_lock:
            self.pending += 1
        queue = cast(asyncio.Queue, self._queue)  # set before the thread signals ready
        try:
            self.loop.call_soon_threadsafe(queue.put_nowait, (fn, future, context))
        except RuntimeError:
            with self._pending_lock:
                self.pending -= 1
            future.set_exception(RuntimeError(f"the worker of {self.name} has stopped"))
        return future

    async def run(self, fn: Callable[[], Awaitable[T]]) -> T:
        """Run ``fn()`` on this worker and await its result from another loop.

        Called on the worker itself (a callback handing more work to its own
        producer), ``fn()`` runs at once, inside the current piece of work."""
        if self.on_thread():
            return await fn()
        future = self.submit(fn)
        try:
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            future.cancel()
            raise

    def call(self, fn: Callable[[], Awaitable[T]], timeout: float | None = None) -> T:
        """Run ``fn()`` on this worker and block the calling thread for its
        result. Never call it from this worker's own thread."""
        if self.on_thread():
            raise RuntimeError(f"{self.name}: call() from its own worker would wait for itself")
        return self.submit(fn).result(timeout)

    def stop(self, timeout: float = STOP_TIMEOUT_S) -> None:
        """Let the work already queued finish, cancel what the producer left
        running, and end the thread."""
        if self._stopped:
            return
        self._stopped = True
        loop, queue = self._loop, self._queue
        if loop is None or queue is None or loop.is_closed():
            return

        try:
            # The serve task ends after the work queued before this, and its
            # end stops the loop.
            loop.call_soon_threadsafe(queue.put_nowait, None)
        except RuntimeError:
            return
        if not self.on_thread():
            self._thread.join(timeout)
            if self._thread.is_alive():
                log.warning("The worker of %s did not stop within %.0f s", self.name, timeout)


async def _call(fn: Callable[[], Awaitable[T]]) -> T:
    return await fn()


def worker_of(producer: Any) -> ProducerWorker | None:
    """The worker ``producer`` runs on, ``None`` when it has none (a producer
    a test or a standalone executor drives directly)."""
    return getattr(producer, "_worker", None)


async def on_worker(producer: Any, fn: Callable[[], Awaitable[T]]) -> T:
    """Run ``fn()`` on ``producer``'s worker and await it; directly when it
    has none, or when the caller already runs on it."""
    worker = worker_of(producer)
    if worker is None:
        return await fn()
    return await worker.run(fn)


def call_on_worker(producer: Any, fn: Callable[[], Awaitable[T]]) -> T:
    """:func:`on_worker` for a thread that is not an event loop: block for the
    result. Without a worker, run ``fn()`` on a private loop."""
    worker = worker_of(producer)
    if worker is None:
        return asyncio.run(_call(fn))
    return worker.call(fn)


__all__ = ["ProducerWorker", "call_on_worker", "on_worker", "worker_of"]
