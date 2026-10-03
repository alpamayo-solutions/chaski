"""An expected, retried failure (Colca away, 5xx, 429) is logged as a state:
one WARNING when it starts, quiet retries, one INFO when it recovers, never a
traceback. Anything else is still an ERROR with its traceback."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from dataops_fakes import run_async
from franzmq.errors import PublishRejected, PublishTimeout
from test_handler_failures import CursorDoor, _record

from chaski import ColcaUnavailable, Doorbell, NotSent
from chaski.consume import consume
from chaski.dataops import Producer, every
from chaski.door import Stream
from chaski.failures import HandlerHealth
from chaski.outage import Outage, expected_failure, warn_failure
from chaski.retry import Backoff
from chaski.startup import BrokerRefused

OUTAGE = 5


def _status(code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://colca/fetch")
    return httpx.HTTPStatusError("", request=request, response=httpx.Response(code, request=request))


class _Code:
    def __init__(self, value: int) -> None:
        self.value = value


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


@pytest.fixture
def no_backoff(monkeypatch):
    """Retries without waiting, so an outage of several attempts is quick."""
    original = Backoff.delay

    def delay(self, error=None):
        original(self, error)
        return 0.001

    monkeypatch.setattr(Backoff, "delay", delay)


def _tracebacks(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.exc_info]


def _at(caplog, level: int, text: str) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == level and text in r.getMessage()]


# ─── what is expected ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("connection refused"),
        httpx.RemoteProtocolError("peer closed connection"),
        httpx.ReadTimeout("timed out"),
        ConnectionRefusedError(61, "Connection refused"),
        TimeoutError("no CONNACK"),
        _status(503),
        _status(429),
        ColcaUnavailable("Retained view unavailable; waiting for subscription recovery"),
        NotSent("x: not sent, the broker link is down", link_down=True),
        PublishTimeout("colca/v1/_Metric/x", 10.0),
        PublishRejected(0x89, "colca/v1/_Metric/x"),
        BrokerRefused(_Code(0x88)),
        BufferError("connector durable sample queue is full; acquisition must wait"),
    ],
)
def test_a_node_that_is_away_or_busy_is_expected(exc):
    assert expected_failure(exc)


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("bug"),
        ValueError("bad payload"),
        KeyError("signal_id"),
        _status(404),
        _status(403),
        FileNotFoundError(2, "No such file", "/data/x.sqlite3"),
        NotSent("x: not sent, its deadline had passed"),
        PublishRejected(0x87, "colca/v1/_Metric/x"),
    ],
)
def test_anything_else_is_not_expected(exc):
    assert expected_failure(exc) is None


def test_an_error_raised_from_an_outage_is_expected():
    try:
        try:
            raise httpx.ConnectError("connection refused")
        except httpx.ConnectError as cause:
            raise RuntimeError("could not write the interval") from cause
    except RuntimeError as exc:
        assert expected_failure(exc) is not None


# ─── the Outage itself ───────────────────────────────────────────────────


def test_an_outage_of_n_retries_logs_one_warning_no_traceback_and_one_recovery(caplog):
    clock = Clock()
    outage = Outage(logging.getLogger("chaski.test"), "Definition bindings", now=clock)
    with caplog.at_level(logging.DEBUG, logger="chaski.test"):
        for _ in range(OUTAGE):
            assert outage.failed(ColcaUnavailable("Retained view unavailable"), delay=1.0)
            clock.now += 2.0
        outage.recovered()
        outage.recovered()  # a success without an outage logs nothing

    warnings = _at(caplog, logging.WARNING, "")
    assert warnings == ["Definition bindings: ColcaUnavailable: Retained view unavailable; retrying until it answers"]
    assert len(_at(caplog, logging.DEBUG, "Definition bindings: attempt")) == OUTAGE - 1
    assert _at(caplog, logging.INFO, "") == ["Definition bindings: reached Colca after 5 failed attempts / 10 s"]
    assert not _tracebacks(caplog)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_a_long_outage_says_it_is_still_waiting_once_per_interval(caplog):
    clock = Clock()
    outage = Outage(logging.getLogger("chaski.test"), "Ingest drain", remind_every=60.0, now=clock)
    with caplog.at_level(logging.INFO, logger="chaski.test"):
        for _ in range(130):
            outage.failed(httpx.ConnectError("connection refused"))
            clock.now += 1.0
    assert len(_at(caplog, logging.WARNING, "Ingest drain")) == 1
    assert _at(caplog, logging.INFO, "still waiting") == [
        "Ingest drain: still waiting (61 attempts, 60 s): ConnectError: connection refused",
        "Ingest drain: still waiting (121 attempts, 120 s): ConnectError: connection refused",
    ]


def test_an_unexpected_error_is_left_to_the_caller(caplog):
    outage = Outage(logging.getLogger("chaski.test"), "Definition bindings")
    with caplog.at_level(logging.DEBUG, logger="chaski.test"):
        assert not outage.failed(RuntimeError("bug"))
        outage.recovered()
    assert not caplog.records and not outage.active


def test_a_best_effort_step_shows_a_traceback_only_for_a_defect(caplog):
    log = logging.getLogger("chaski.test")
    with caplog.at_level(logging.WARNING, logger="chaski.test"):
        warn_failure(log, PublishTimeout("colca/v1/_ServiceDetails/x", 10.0), "could not publish %s's status", "svc")
        warn_failure(log, RuntimeError("bug"), "could not publish %s's status", "svc")
    first, second = caplog.records
    assert (
        first.getMessage()
        == "could not publish svc's status: PublishTimeout: no PUBACK for colca/v1/_ServiceDetails/x within 10.0s"
    )
    assert first.exc_info is None
    assert second.exc_info is not None


# ─── the loops ───────────────────────────────────────────────────────────


@run_async
async def test_definition_bindings_log_an_outage_once_and_its_recovery(no_backoff, caplog):
    """The loop that logged an ERROR with a traceback on every retry while
    Colca was away."""
    import chaski.dataops.service as service_module
    from chaski._wakeup import Wakeup

    door = SimpleNamespace(_dataops_definitions=SimpleNamespace(changes=Wakeup()))
    runtime = SimpleNamespace(door=door)
    calls: list[int] = []
    bound = asyncio.Event()

    def build(*_):
        calls.append(1)
        if len(calls) <= OUTAGE:
            raise ColcaUnavailable("Retained view unavailable; waiting for subscription recovery")
        return {}, [], 0

    stop = asyncio.Event()
    with (
        caplog.at_level(logging.DEBUG, logger="chaski.dataops"),
        patch.object(service_module, "build_dispatch", build),
    ):
        ingest = SimpleNamespace(rebind=lambda *_: bound.set())
        task = asyncio.create_task(service_module.reresolve_loop(runtime, [], ingest, stop, lambda: None))
        await asyncio.wait_for(bound.wait(), 5)
        stop.set()
        await asyncio.wait_for(task, 5)

    assert len(_at(caplog, logging.WARNING, "Definition bindings")) == 1
    assert _at(caplog, logging.INFO, "Definition bindings: reached Colca after 5 failed attempts")
    assert not _tracebacks(caplog)


@run_async
async def test_definition_bindings_still_log_a_defect_with_its_traceback(no_backoff, caplog):
    import chaski.dataops.service as service_module
    from chaski._wakeup import Wakeup

    door = SimpleNamespace(_dataops_definitions=SimpleNamespace(changes=Wakeup()))
    calls: list[int] = []
    bound = asyncio.Event()

    def build(*_):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("bug in a producer's input declaration")
        return {}, [], 0

    stop = asyncio.Event()
    with (
        caplog.at_level(logging.DEBUG, logger="chaski.dataops"),
        patch.object(service_module, "build_dispatch", build),
    ):
        ingest = SimpleNamespace(rebind=lambda *_: bound.set())
        task = asyncio.create_task(
            service_module.reresolve_loop(SimpleNamespace(door=door), [], ingest, stop, lambda: None)
        )
        await asyncio.wait_for(bound.wait(), 5)
        stop.set()
        await asyncio.wait_for(task, 5)

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None
    assert "bug in a producer's input declaration" in str(errors[0].exc_info[1])


def test_a_timer_tick_that_fails_because_colca_is_away_logs_no_traceback(caplog):
    """APScheduler logged 'Job ... raised an exception' with a traceback for
    every tick while Colca was away. The tick's failure still counts in health."""
    from chaski.dataops.service import off_loop

    class Flush(Producer):
        name = "usage"
        system_element_name = "SE-Usage"
        away = True

        @every("60s")
        async def flush(self):
            if self.away:
                raise ColcaUnavailable("Retained view unavailable; waiting for subscription recovery")

        @every("60s")
        async def broken(self):
            raise RuntimeError("bug")

    producer = Flush()
    producer._runtime = SimpleNamespace(handler_health=HandlerHealth())
    tick = off_loop(producer.flush)

    with caplog.at_level(logging.DEBUG, logger="chaski.dataops"):
        for _ in range(OUTAGE):
            tick()  # does not raise: APScheduler has nothing to log
        assert producer._runtime.handler_health.failing()["usage.flush"].failures == OUTAGE
        producer.away = False
        tick()
    assert producer._runtime.handler_health.status == "ok"
    assert len(_at(caplog, logging.WARNING, "usage.flush")) == 1
    assert _at(caplog, logging.INFO, "usage.flush: succeeded again after 5 failed attempts")
    assert not _tracebacks(caplog)

    with pytest.raises(RuntimeError, match="bug"):
        off_loop(producer.broken)()  # APScheduler logs it with its traceback


def _consume(stream, handler, stop):
    worker = threading.Thread(
        target=consume,
        args=(stream, handler),
        kwargs={
            "health": HandlerHealth(),
            "reject": lambda *_: None,
            "bell": Doorbell(),
            "stop": stop,
            "retry": Backoff(minimum=0.001, maximum=0.002),
        },
        daemon=True,
    )
    worker.start()
    return worker


def _wait(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.005)


def test_a_consumer_whose_handler_cannot_reach_colca_logs_once(caplog):
    door = CursorDoor([_record(1), _record(2)])
    stream = Stream(door, "metrics", "c/consumer")
    attempts: list[int] = []

    def handler(record):
        if record.offset == 2:
            attempts.append(1)
            if len(attempts) <= OUTAGE:
                raise httpx.ConnectError("connection refused")

    stop = threading.Event()
    with caplog.at_level(logging.DEBUG, logger="chaski.consume"):
        worker = _consume(stream, handler, stop)
        _wait(lambda: door.cursors.get("c/consumer") == 2)
        stop.set()
        worker.join(5)
    assert len(_at(caplog, logging.WARNING, "c/consumer")) == 1
    assert _at(caplog, logging.INFO, "c/consumer: succeeded again after 5 failed attempts")
    assert not _tracebacks(caplog)


def test_a_consumer_whose_handler_has_a_bug_logs_its_traceback(caplog):
    door = CursorDoor([_record(1)])
    stream = Stream(door, "metrics", "c/consumer")
    attempts: list[int] = []

    def handler(record):
        attempts.append(1)
        if len(attempts) == 1:
            raise KeyError("signal_id")

    stop = threading.Event()
    with caplog.at_level(logging.DEBUG, logger="chaski.consume"):
        worker = _consume(stream, handler, stop)
        _wait(lambda: door.cursors.get("c/consumer") == 1)
        stop.set()
        worker.join(5)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and errors[0].exc_info is not None


class _View:
    """A definition cache whose view goes away and comes back, like
    :class:`chaski.dataops.resolve.LiveIndex` over its retained view."""

    def __init__(self) -> None:
        from chaski._wakeup import Wakeup

        self.changes = Wakeup()
        self.live = True

    def set_live(self, live: bool) -> None:
        self.live = live
        self.changes.notify()


@run_async
async def test_definition_bindings_rebind_as_soon_as_the_view_is_available_again():
    """Resume on the prerequisite's event: the backoff step reached during the
    outage does not delay the rebind once the view is available."""
    import chaski.dataops.service as service_module

    view = _View()
    runtime = SimpleNamespace(door=SimpleNamespace(_dataops_definitions=view))
    calls: list[float] = []
    rebound = asyncio.Event()

    def build(*_):
        calls.append(time.monotonic())
        if not view.live:
            raise ColcaUnavailable("Retained view unavailable; waiting for subscription recovery")
        return {}, [], 0

    def rebind(*_):
        if len(calls) > 1:
            rebound.set()

    stop = asyncio.Event()
    with patch.object(service_module, "build_dispatch", build):
        task = asyncio.create_task(
            service_module.reresolve_loop(runtime, [], SimpleNamespace(rebind=rebind), stop, lambda: None)
        )
        await asyncio.sleep(0.05)
        view.set_live(False)  # Colca away: the next pass fails
        await asyncio.sleep(0.05)
        assert len(calls) == 2, "a pass while the view is down fails once, then waits for the view"
        await asyncio.sleep(2.0)  # longer than the first backoff steps
        assert len(calls) == 2, "no retries on a timer while the view is down"
        back = time.monotonic()
        view.set_live(True)
        await asyncio.wait_for(rebound.wait(), 1.0)
        stop.set()
        await asyncio.wait_for(task, 5)
    assert calls[-1] - back < 0.5


@run_async
async def test_definition_bindings_back_off_while_the_view_is_available():
    import chaski.dataops.service as service_module

    view = _View()
    runtime = SimpleNamespace(door=SimpleNamespace(_dataops_definitions=view))
    calls: list[float] = []

    def build(*_):
        calls.append(time.monotonic())
        raise RuntimeError("bug in a producer's input declaration")

    stop = asyncio.Event()
    with patch.object(service_module, "build_dispatch", build):
        task = asyncio.create_task(
            service_module.reresolve_loop(runtime, [], SimpleNamespace(rebind=lambda *_: None), stop, lambda: None)
        )
        await asyncio.sleep(0.3)
        assert len(calls) == 1, "a failure while the view is up waits out its backoff (at least 0.5 s)"
        view.changes.notify()  # a definition change retries at once
        await asyncio.sleep(0.1)
        assert len(calls) == 2
        stop.set()
        await asyncio.wait_for(task, 5)
