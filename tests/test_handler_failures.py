"""A failed handler is never acknowledged: it is retried, health degrades,
and only an explicit ``Reject`` lets a record pass, after it was recorded."""

from __future__ import annotations

import asyncio
import json
import threading
import urllib.error
import urllib.request

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import franzmq
import pytest
from colca_data_contracts.payload import Constant, ConstantDataType
from dataops_fakes import FakeDoor, FakeRuntime, run_async
from paho.mqtt.client import MQTTMessage

from chaski import Doorbell, Reject
from chaski.consume import consume
from chaski.dataops import watch
from chaski.dataops.base import Producer
from chaski.dataops.buffer import Buffer
from chaski.dataops.health import HealthState, serve
from chaski.dataops.ingest import Ingest
from chaski.dataops.triggers import on_constant
from chaski.door import Page, Record, Stream
from chaski.failures import DEGRADED, OK, UNHEALTHY, HandlerHealth, rejection_finding

NODE_ID = "n-1"


def _record(offset: int, signal_id: str = "sig-1", value: float = 1.0) -> Record:
    return Record(
        offset=offset,
        origin_offset=offset,
        topic=f"colca/v1/_Metric/{NODE_ID}/line1/x",
        payload={"signal_id": signal_id, "value": value, "timestamp": float(offset)},
        ts=float(offset),
        written_by="connector",
        actor_id="svc-1",
        actor_label="connector",
        actor_kind="local",
    )


class CursorDoor(FakeDoor):
    """A door with a real cursor: ``fetch`` reads after the acked offset, so
    a record that was not acknowledged is served again."""

    def __init__(self, records: list[Record], *, page_size: int = 100) -> None:
        super().__init__()
        self.records = records
        self.page_size = page_size
        self.cursors: dict[str, int] = {}

    def fetch(self, stream, cursor, *, max=1000, from_offset=None, tail=False, **_scope):
        if tail:
            return Page(records=[], next=(self.records[-1].offset + 1) if self.records else 1)
        start = self.cursors.get(cursor, 0) + 1
        if from_offset is not None:
            start = builtins_max(start, from_offset)
        page = [r for r in self.records if r.offset >= start][: min(max, self.page_size)]
        return Page(records=page, next=(page[-1].offset + 1) if page else start, start=start)

    def ack(self, stream, cursor, offset) -> bool:
        self.acked.append((stream, cursor, offset))
        moved = offset > self.cursors.get(cursor, 0)
        self.cursors[cursor] = builtins_max(self.cursors.get(cursor, 0), offset)
        return moved


builtins_max = max


@pytest.fixture
def buffer(tmp_path):
    b = Buffer(tmp_path / "buffer.sqlite3")
    try:
        yield b
    finally:
        b.close()


def _opener(door):
    return lambda cursor, signal_ids: Stream(door, "metrics", "c/" + cursor, signal_ids=signal_ids)


async def _until(predicate, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


# ─── HandlerHealth ─────────────────────────────────────────────────────────


def test_health_degrades_on_the_first_failure_and_turns_unhealthy_after_n():
    changes = []
    health = HandlerHealth(unhealthy_after=3, on_change=lambda status, summary: changes.append(status))
    assert health.status == OK
    assert health.failed("p.h", RuntimeError("x")) == 1
    assert health.status == DEGRADED
    health.failed("p.h", RuntimeError("x"))
    assert health.status == DEGRADED
    assert health.failed("p.h", RuntimeError("x")) == 3
    assert health.status == UNHEALTHY
    assert "p.h failed 3x: RuntimeError: x" in health.summary()
    health.succeeded("p.h")
    assert health.status == OK
    assert health.summary() == ""
    assert changes == [DEGRADED, UNHEALTHY, OK]


def test_a_rejection_finding_says_what_was_set_aside_and_why():
    payload = rejection_finding(
        "p.h", {"offset": 7, "topic": "t"}, Reject("bad unit", detail={"unit": "?"}), rejected=2
    )
    assert payload["reason"] == "rejected_input"
    assert payload["detail"] == {"consumer": "p.h", "rejected": 2, "offset": 7, "topic": "t", "reject": {"unit": "?"}}
    assert "bad unit" in payload["summary"]


# ─── @on_metric ingest ─────────────────────────────────────────────────────


@run_async
async def test_a_failed_metric_handler_is_retried_at_its_record_and_never_acked(buffer):
    door = CursorDoor([_record(1), _record(2), _record(3)])
    health = HandlerHealth(unhealthy_after=3)
    attempts: list[int] = []
    healthy = threading.Event()

    async def handler(record):
        attempts.append(record.offset)
        if record.offset == 2 and not healthy.is_set():
            raise RuntimeError("downstream refused")

    ingest = Ingest(_opener(door), buffer, dispatch={"sig-1": [handler]}, signal_ids=["sig-1"], health=health)
    ingest.ERROR_BACKOFF_MAX_S = 0.02
    ingest._retry_min_s = 0.01
    stop = asyncio.Event()
    task = asyncio.ensure_future(ingest.run_forever(stop))
    try:
        await _until(lambda: health.status == UNHEALTHY)
        # Everything before the failed record is acknowledged, the failed one never.
        assert max(offset for _s, _c, offset in door.acked) == 1
        assert attempts.count(2) >= 3
        assert attempts[:1] == [1] and 3 not in attempts
        healthy.set()
        ingest.wake()
        await _until(lambda: 3 in attempts)
        await _until(lambda: door.cursors.get("c/ingest-" + buffer.generation) == 3)
        assert health.status == OK
    finally:
        stop.set()
        ingest.wake()
        await asyncio.wait_for(task, timeout=2)


@run_async
async def test_a_restart_resumes_at_the_failed_record(buffer):
    door = CursorDoor([_record(1), _record(2), _record(3)])

    async def broken(record):
        if record.offset == 2:
            raise RuntimeError("boom")

    first = Ingest(_opener(door), buffer, dispatch={"sig-1": [broken]}, signal_ids=["sig-1"])
    first._retry_min_s = 0.01
    stop = asyncio.Event()
    task = asyncio.ensure_future(first.run_forever(stop))
    await _until(lambda: door.acked)
    stop.set()
    first.wake()
    await asyncio.wait_for(task, timeout=2)

    seen: list[int] = []

    async def fixed(record):
        seen.append(record.offset)

    second = Ingest(_opener(door), buffer, dispatch={"sig-1": [fixed]}, signal_ids=["sig-1"])
    await second.run_once()
    assert seen == [2, 3]


@run_async
async def test_a_rejected_metric_is_recorded_before_the_page_is_acked(buffer):
    door = CursorDoor([_record(1), _record(2), _record(3)])
    order: list[str] = []
    rejected: list[tuple[str, dict, str]] = []

    def record_rejection(consumer, subject, reject):
        order.append("recorded")
        rejected.append((consumer, subject, reject.reason))

    original_ack = door.ack

    def ack(stream, cursor, offset):
        order.append(f"ack {offset}")
        return original_ack(stream, cursor, offset)

    door.ack = ack
    seen: list[int] = []

    async def handler(record):
        if record.offset == 2:
            raise Reject("value out of range")
        seen.append(record.offset)

    handler.consumer = "Panels.on_edge"
    health = HandlerHealth()
    ingest = Ingest(
        _opener(door),
        buffer,
        dispatch={"sig-1": [handler]},
        signal_ids=["sig-1"],
        health=health,
        reject=record_rejection,
    )
    await ingest.run_once()
    assert seen == [1, 3]
    assert order == ["recorded", "ack 3"]
    consumer, subject, reason = rejected[0]
    assert (consumer, subject["offset"], subject["signal_id"], reason) == (
        "Panels.on_edge",
        2,
        "sig-1",
        "value out of range",
    )
    assert health.status == OK


@run_async
async def test_a_rejection_that_cannot_be_recorded_is_a_failure_not_an_ack(buffer):
    door = CursorDoor([_record(1)])

    def unreachable(consumer, subject, reject):
        raise ConnectionError("node away")

    async def handler(record):
        raise Reject("poison")

    ingest = Ingest(_opener(door), buffer, dispatch={"sig-1": [handler]}, signal_ids=["sig-1"], reject=unreachable)
    with pytest.raises(ConnectionError):
        await ingest.run_once()
    assert door.acked == []


# ─── health door ───────────────────────────────────────────────────────────


def _get(port: int):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as refused:
        return refused.code, json.loads(refused.read())


@run_async
async def test_the_health_door_says_degraded_then_fails_when_unhealthy():
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    health = HandlerHealth(unhealthy_after=2)
    state = HealthState(handlers=health)
    server = await serve(state, port=port)
    try:
        health.failed("Panels.on_edge", RuntimeError("boom"))
        status, body = await asyncio.to_thread(_get, port)
        assert status == 200 and body["handlers"] == "degraded"
        assert body["failing"]["Panels.on_edge"]["failures"] == 1
        health.failed("Panels.on_edge", RuntimeError("boom"))
        status, body = await asyncio.to_thread(_get, port)
        assert status == 503 and body["ok"] is False and body["handlers"] == "unhealthy"
        health.succeeded("Panels.on_edge")
        status, body = await asyncio.to_thread(_get, port)
        assert status == 200 and body["handlers"] == "ok" and "failing" not in body
    finally:
        server.close()
        await server.wait_closed()


# ─── @on_constant / @on_signal ─────────────────────────────────────────────


def _deliver(client: franzmq.Client, topic: str, value: float) -> None:
    message = MQTTMessage(mid=1, topic=topic.encode())
    message.payload = Constant(id="c-1", name="sp", data_type=ConstantDataType.FLOAT64, value=value).encode()
    client._handle_on_message(message)


class Flaky(Producer):
    name = "flaky"
    system_element_name = "SE-Flaky"

    def __init__(self) -> None:
        super().__init__()
        self.fail_on: set[float] = set()
        self.reject_on: set[float] = set()
        self.calls: list[float] = []
        self.done: list[float] = []

    @on_constant("line1/operator/setpoint")
    async def on_setpoint(self, constant) -> None:
        self.calls.append(constant.value)
        if constant.value in self.reject_on:
            raise Reject("unknown recipe")
        if constant.value in self.fail_on:
            raise RuntimeError("not yet")
        self.done.append(constant.value)


@pytest.fixture
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


def _watching(client_id, *, reject=None):
    door = FakeDoor()
    instance = Flaky().attach(FakeRuntime(door, buffer=None))
    client = franzmq.Client(client_id=client_id)
    health = HandlerHealth(unhealthy_after=3)
    constants, signals = watch.gather_triggers([instance])
    watch.start(client, NODE_ID, door, asyncio.get_running_loop(), constants, signals, health=health, reject=reject)
    return instance, client, health


TOPIC = f"colca/v1/_Constant/{NODE_ID}/line1/operator/setpoint"


@run_async
async def test_a_failed_constant_handler_is_retried_until_it_succeeds(_isolate_registry, monkeypatch):
    monkeypatch.setattr(watch, "RETRY_MIN_S", 0.01)
    instance, client, health = _watching("flaky-retry")
    instance.fail_on.add(5.0)
    _deliver(client, TOPIC, 5.0)
    await _until(lambda: health.status == UNHEALTHY)
    assert instance.calls.count(5.0) >= 3
    instance.fail_on.clear()
    await _until(lambda: instance.done == [5.0])
    assert health.status == OK


@run_async
async def test_a_newer_record_at_the_topic_ends_the_retry_of_the_older_one(_isolate_registry, monkeypatch):
    monkeypatch.setattr(watch, "RETRY_MIN_S", 0.05)
    instance, client, health = _watching("flaky-supersede")
    instance.fail_on.add(5.0)
    _deliver(client, TOPIC, 5.0)
    await _until(lambda: instance.calls.count(5.0) >= 1)
    _deliver(client, TOPIC, 6.0)
    await _until(lambda: instance.done == [6.0])
    retries = instance.calls.count(5.0)
    await asyncio.sleep(0.3)
    assert instance.calls.count(5.0) <= retries + 1
    assert health.status == OK


@run_async
async def test_a_rejected_constant_is_recorded_not_retried(_isolate_registry, monkeypatch):
    monkeypatch.setattr(watch, "RETRY_MIN_S", 0.01)
    recorded = []
    instance, client, health = _watching("flaky-reject", reject=lambda c, s, r: recorded.append((c, s, r.reason)))
    instance.reject_on.add(7.0)
    _deliver(client, TOPIC, 7.0)
    await _until(lambda: recorded)
    await asyncio.sleep(0.1)
    assert instance.calls == [7.0]
    assert recorded == [("flaky.on_setpoint", {"topic": TOPIC, "retired": False}, "unknown recipe")]
    assert health.status == OK


# ─── Service.consume over a plain Stream ───────────────────────────────────


def test_consume_retries_the_failed_record_and_records_a_rejection():
    records = [_record(1), _record(2), _record(3), _record(4)]
    door = CursorDoor(records)
    stream = Stream(door, "metrics", "c/consumer")
    health = HandlerHealth(unhealthy_after=2)
    rejected: list[int] = []
    handled: list[int] = []
    fixed = threading.Event()
    stop = threading.Event()
    bell = Doorbell()

    def handler(record):
        if record.offset == 2 and not fixed.is_set():
            raise RuntimeError("database away")
        if record.offset == 3:
            raise Reject("undecodable")
        handled.append(record.offset)

    def reject(consumer, subject, rejection):
        rejected.append(subject["offset"])

    from chaski.retry import Backoff

    worker = threading.Thread(
        target=consume,
        args=(stream, handler),
        kwargs={
            "health": health,
            "reject": reject,
            "bell": bell,
            "stop": stop,
            "consumer": "sink",
            "retry": Backoff(0.01, 0.02),
        },
    )
    worker.start()
    try:
        _wait(lambda: health.status == UNHEALTHY)
        assert door.cursors["c/consumer"] == 1, "only the record before the failed one is acknowledged"
        assert handled == [1]
        fixed.set()
        _wait(lambda: door.cursors["c/consumer"] == 4)
        assert handled == [1, 2, 4]
        assert rejected == [3]
        assert health.status == OK
    finally:
        stop.set()
        bell.ring()
        worker.join(timeout=5)
    assert not worker.is_alive()


def _wait(predicate, timeout: float = 5.0) -> None:
    import time

    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.01)


# ─── periodic ticks ────────────────────────────────────────────────────────


def test_a_failed_tick_counts_against_health_and_still_reaches_the_scheduler(_isolate_registry):
    from chaski.dataops.service import off_loop
    from chaski.dataops.triggers import every

    class Ticker(Producer):
        name = "ticker"
        system_element_name = "SE-Tick"
        broken = True

        @every("1s")
        async def tick(self) -> None:
            if self.broken:
                raise RuntimeError("tick failed")

    runtime = FakeRuntime(FakeDoor(), buffer=None)
    runtime.handler_health = HandlerHealth()
    instance = Ticker().attach(runtime)
    job = off_loop(instance.tick)
    with pytest.raises(RuntimeError):
        job()
    assert runtime.handler_health.failing()["ticker.tick"].failures == 1
    instance.broken = False
    job()
    assert runtime.handler_health.status == OK


def test_consume_ends_when_stop_is_set_without_a_ring():
    # A view rescoped its consumer to no topics: nothing could ring the bell,
    # and consume waited on it forever after stop was set (Hygentile rig).
    door = CursorDoor([_record(1)])
    stream = Stream(door, "metrics", "c/consumer")
    stop, bell = threading.Event(), Doorbell()
    handled: list[int] = []
    worker = threading.Thread(
        target=consume,
        args=(stream, lambda record: handled.append(record.offset)),
        kwargs={"health": HandlerHealth(), "reject": lambda *_: None, "bell": bell, "stop": stop},
        daemon=True,
    )
    worker.start()
    _wait(lambda: door.cursors.get("c/consumer") == 1)
    stop.set()
    worker.join(timeout=5)
    assert not worker.is_alive(), "consume kept waiting on a bell nobody rings"
    assert handled == [1]
