"""A drain that failed because the node was away retries as soon as the node's
link comes back (the door's ``link_up``), not after its backoff; while the
link stays up, the backoff spaces retries. One test pair per drain loop:
``Service.consume``, the DataOps ingest and the command executor."""

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace

import httpx
from dataops_fakes import run_async
from test_handler_failures import CursorDoor, _opener, _record, buffer  # noqa: F401  # fixture

from chaski.consume import consume
from chaski.dataops.ingest import Ingest
from chaski.door import Stream
from chaski.doorbell import Doorbell
from chaski.executor import CommandExecutor
from chaski.failures import HandlerHealth
from chaski.retry import Backoff

#: Far longer than any test waits: a retry before it can only come from link_up.
BACKOFF_S = 30.0


class AwayDoor(CursorDoor):
    """A node that refuses every fetch until ``up`` is set."""

    def __init__(self, records) -> None:
        super().__init__(records)
        self.link_up = Doorbell()
        self.up = threading.Event()
        self.fetches = 0

    def fetch(self, *args, **kwargs):
        self.fetches += 1
        if not self.up.is_set():
            raise httpx.ConnectError("connection refused")
        return super().fetch(*args, **kwargs)


def _wait(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.005)


async def _until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        await asyncio.sleep(0.005)


# ─── Service.consume ─────────────────────────────────────────────────────


def _consuming(door, handled, stop):
    worker = threading.Thread(
        target=consume,
        args=(Stream(door, "metrics", "c/consumer"), lambda record: handled.append(record.offset)),
        kwargs={
            "health": HandlerHealth(),
            "reject": lambda *_: None,
            "bell": Doorbell(),
            "stop": stop,
            "retry": Backoff(minimum=BACKOFF_S, maximum=BACKOFF_S),
            "idle_drain_s": None,
        },
        daemon=True,
    )
    worker.start()
    return worker


def test_consume_retries_when_the_link_comes_back():
    door = AwayDoor([_record(1)])
    handled: list[int] = []
    stop = threading.Event()
    worker = _consuming(door, handled, stop)
    try:
        _wait(lambda: door.fetches >= 1)
        door.up.set()
        rang = time.monotonic()
        door.link_up.ring()
        _wait(lambda: handled == [1], timeout=1.0)
        assert time.monotonic() - rang < 0.5
    finally:
        stop.set()
        worker.join(5)


def test_consume_backs_off_while_the_link_stays_up():
    door = AwayDoor([_record(1)])
    stop = threading.Event()
    worker = _consuming(door, [], stop)
    try:
        _wait(lambda: door.fetches >= 1)
        time.sleep(0.3)
        assert door.fetches == 1, "a failure with the link up waits out its backoff"
    finally:
        stop.set()
        worker.join(5)


# ─── DataOps ingest ──────────────────────────────────────────────────────


def _ingest(door, buffer, handled):  # noqa: F811
    async def handler(record):
        handled.append(record.offset)

    ingest = Ingest(_opener(door), buffer, dispatch={"sig-1": [handler]}, signal_ids=["sig-1"], idle_drain_s=None)
    ingest._retry_min_s = BACKOFF_S
    ingest.ERROR_BACKOFF_MAX_S = BACKOFF_S
    ingest._min_fetch_interval_s = 0.0
    return ingest


@run_async
async def test_ingest_retries_when_the_link_comes_back(buffer):  # noqa: F811
    door = AwayDoor([_record(1)])
    handled: list[int] = []
    ingest = _ingest(door, buffer, handled)
    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        await _until(lambda: door.fetches >= 1)
        door.up.set()
        rang = time.monotonic()
        door.link_up.ring()
        await _until(lambda: handled == [1], timeout=1.0)
        assert time.monotonic() - rang < 0.5
    finally:
        stop.set()
        ingest.wake()
        await asyncio.wait_for(task, 5)


@run_async
async def test_ingest_backs_off_while_the_link_stays_up(buffer):  # noqa: F811
    door = AwayDoor([_record(1)])
    ingest = _ingest(door, buffer, [])
    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        await _until(lambda: door.fetches >= 1)
        fetched = door.fetches
        await asyncio.sleep(0.3)
        assert door.fetches == fetched, "a failure with the link up waits out its backoff"
    finally:
        stop.set()
        ingest.wake()
        await asyncio.wait_for(task, 5)


# ─── command executor ────────────────────────────────────────────────────


def _executor(door, calls, up):
    executor = CommandExecutor(door, lambda *_: None, SimpleNamespace(), {}, "n-1")

    async def drain():
        calls.append(time.monotonic())
        if not up.is_set():
            raise httpx.ConnectError("connection refused")
        return 0

    executor.drain = drain  # type: ignore[method-assign]
    return executor


@run_async
async def test_the_command_executor_retries_when_the_link_comes_back(monkeypatch):
    monkeypatch.setattr("chaski.executor._ERROR_BACKOFF_MAX_S", BACKOFF_S)
    monkeypatch.setattr(Backoff, "delay", lambda self, error=None: BACKOFF_S)
    door = SimpleNamespace(link_up=Doorbell())
    calls: list[float] = []
    up = threading.Event()
    executor = _executor(door, calls, up)
    stop = asyncio.Event()
    task = asyncio.ensure_future(executor._serve(stop))
    try:
        await _until(lambda: len(calls) >= 1)
        up.set()
        rang = time.monotonic()
        door.link_up.ring()
        await _until(lambda: len(calls) >= 2, timeout=1.0)
        assert calls[1] - rang < 0.5
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)


@run_async
async def test_the_command_executor_backs_off_while_the_link_stays_up(monkeypatch):
    monkeypatch.setattr(Backoff, "delay", lambda self, error=None: BACKOFF_S)
    door = SimpleNamespace(link_up=Doorbell())
    calls: list[float] = []
    executor = _executor(door, calls, threading.Event())
    stop = asyncio.Event()
    task = asyncio.ensure_future(executor._serve(stop))
    try:
        await _until(lambda: len(calls) >= 1)
        executor.wake()  # a stream hint does not bypass the backoff
        await asyncio.sleep(0.3)
        assert len(calls) == 1, "a failure with the link up waits out its backoff"
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)
