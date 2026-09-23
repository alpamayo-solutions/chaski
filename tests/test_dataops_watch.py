"""Tests for chaski.dataops.watch: the `@on_constant`/`@on_signal` dispatch.

A real `franzmq.Client` plays the MQTT side — subscribing and delivering a
message through it, rather than a hand-rolled stand-in, is what proves the
tombstone case actually decodes to `None` instead of crashing the client
(the gap `chaski.dataops.service.tolerate_undecodable` exists for).
"""

from __future__ import annotations

import asyncio

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import franzmq
import pytest
from colca_data_contracts.payload import Constant, ConstantDataType
from colca_data_contracts.payload import Signal as SignalRecord
from dataops_fakes import FakeDoor, FakeRuntime, run_async
from paho.mqtt.client import MQTTMessage

from chaski.dataops import watch
from chaski.dataops.base import Producer
from chaski.dataops.triggers import on_constant, on_signal

NODE_ID = "n-1"


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


class Watcher(Producer):
    """Declares both kinds of trigger, stacked once each, so `gather_triggers`
    has something real to walk."""

    name = "watcher"
    system_element_name = "SE-Watch"

    def __init__(self) -> None:
        super().__init__()
        self.constants: list[Constant | None] = []
        self.signals: list[SignalRecord | None] = []
        self.catalog: list[Constant | None] = []

    @on_constant("line1/operator/setpoint")
    async def on_setpoint(self, constant: Constant | None) -> None:
        self.constants.append(constant)

    @on_constant("catalog/#")
    async def on_catalog(self, constant: Constant | None) -> None:
        self.catalog.append(constant)

    @on_signal("line1/temperature")
    async def on_temperature(self, signal: SignalRecord | None) -> None:
        self.signals.append(signal)


def _attach(*, door=None):
    door = door or FakeDoor()
    return Watcher().attach(FakeRuntime(door, buffer=None)), door


# ─── gather_triggers ─────────────────────────────────────────────────────


def test_gather_triggers_splits_constants_from_signals():
    instance, _door = _attach()
    constant_triggers, signal_triggers = watch.gather_triggers([instance])

    assert {p for p, _h in constant_triggers} == {"line1/operator/setpoint", "catalog/#"}
    assert [p for p, _h in signal_triggers] == ["line1/temperature"]
    # Bound to the instance, not the class — calling it needs no self= argument.
    (_path, handler) = signal_triggers[0]
    assert handler.__self__ is instance


def test_gather_triggers_is_empty_for_a_producer_with_neither_trigger():
    from chaski.dataops.triggers import every

    class TickOnly(Producer):
        name = "tick_only_watch_test"
        system_element_name = "SE-1"

        @every("10s")
        async def tick(self) -> None:
            pass

    inst = TickOnly().attach(FakeRuntime(FakeDoor(), buffer=None))
    constant_triggers, signal_triggers = watch.gather_triggers([inst])
    assert constant_triggers == []
    assert signal_triggers == []


# ─── start(): subscribing ────────────────────────────────────────────────


def test_start_is_a_noop_with_no_triggers():
    client = franzmq.Client(client_id="watch-noop")
    n = watch.start(client, NODE_ID, FakeDoor(), asyncio.new_event_loop(), [], [])
    assert n == 0
    assert client.subscribed_topics == set()


def test_start_subscribes_one_topic_per_declared_path_scoped_to_the_node():
    instance, door = _attach()
    constant_triggers, signal_triggers = watch.gather_triggers([instance])
    client = franzmq.Client(client_id="watch-subscribe")
    loop = asyncio.new_event_loop()

    n = watch.start(client, NODE_ID, door, loop, constant_triggers, signal_triggers)

    assert n == 3
    assert client.subscribed_topics == {
        f"colca/v1/_Constant/{NODE_ID}/line1/operator/setpoint",
        f"colca/v1/_Constant/{NODE_ID}/catalog/#",
        f"colca/v1/_Signal/{NODE_ID}/line1/temperature",
    }


# ─── dispatch: decode, tombstones, matching, locking, pinning ──────────────


def _deliver(client: franzmq.Client, topic: str, payload: bytes) -> None:
    message = MQTTMessage(mid=1, topic=topic.encode())
    message.payload = payload
    client._handle_on_message(message)


@run_async
async def test_a_write_reaches_the_matching_handler_decoded():
    instance, door = _attach()
    constant_triggers, signal_triggers = watch.gather_triggers([instance])
    client = franzmq.Client(client_id="watch-decode")
    loop = asyncio.get_running_loop()
    watch.start(client, NODE_ID, door, loop, constant_triggers, signal_triggers)

    payload = Constant(id="c-1", name="setpoint", data_type=ConstantDataType.FLOAT64, value=88.0).encode()
    _deliver(client, f"colca/v1/_Constant/{NODE_ID}/line1/operator/setpoint", payload)
    await asyncio.sleep(0.05)

    assert len(instance.constants) == 1
    assert instance.constants[0].value == 88.0
    assert instance.catalog == [], "a different declared path must not also fire"


@run_async
async def test_a_tombstone_is_delivered_as_none_not_a_crash():
    instance, door = _attach()
    constant_triggers, signal_triggers = watch.gather_triggers([instance])
    client = franzmq.Client(client_id="watch-tombstone")
    loop = asyncio.get_running_loop()
    watch.start(client, NODE_ID, door, loop, constant_triggers, signal_triggers)

    # An empty retained payload: the documented wire tombstone for "this
    # record was retired" — franzmq's own typed decode was not written to
    # expect it (chaski.dataops.service.tolerate_undecodable's own gap).
    _deliver(client, f"colca/v1/_Constant/{NODE_ID}/line1/operator/setpoint", b"")
    await asyncio.sleep(0.05)

    assert instance.constants == [None]


@run_async
async def test_a_multi_level_wildcard_matches_everything_under_it():
    instance, door = _attach()
    constant_triggers, signal_triggers = watch.gather_triggers([instance])
    client = franzmq.Client(client_id="watch-wildcard")
    loop = asyncio.get_running_loop()
    watch.start(client, NODE_ID, door, loop, constant_triggers, signal_triggers)

    payload = Constant(id="c-2", name="widthMm", data_type=ConstantDataType.FLOAT64, value=1250.0).encode()
    _deliver(client, f"colca/v1/_Constant/{NODE_ID}/catalog/products/sku-1", payload)
    await asyncio.sleep(0.05)

    assert len(instance.catalog) == 1
    assert instance.catalog[0].value == 1250.0
    assert instance.constants == [], "the exact-path trigger must not also fire on a different path"


@run_async
async def test_on_signal_decodes_the_signal_contract():
    instance, door = _attach()
    constant_triggers, signal_triggers = watch.gather_triggers([instance])
    client = franzmq.Client(client_id="watch-signal")
    loop = asyncio.get_running_loop()
    watch.start(client, NODE_ID, door, loop, constant_triggers, signal_triggers)

    payload = SignalRecord(id="sig-9", name="temperature").encode()
    _deliver(client, f"colca/v1/_Signal/{NODE_ID}/line1/temperature", payload)
    await asyncio.sleep(0.05)

    assert instance.signals == [SignalRecord(id="sig-9", name="temperature")]


@run_async
async def test_dispatch_pins_one_kv_snapshot_per_message():
    """A handler that resolves several output bindings costs one KV read,
    not one per resolution — the same guarantee build_dispatch's own
    resolve.one_pass gives @on_metric."""

    class MultiPublish(Producer):
        name = "multi_publish"
        system_element_name = "SE-Multi"

        def __init__(self) -> None:
            super().__init__()
            self.kv_calls_seen: list[int] = []

        @on_constant("line1/operator/setpoint")
        async def on_setpoint(self, constant) -> None:
            from chaski.dataops import resolve as resolve_module

            door = self.runtime.door
            # Several lookups inside one handler call share the pinned pass.
            resolve_module.resolve_signal(door, "a")
            resolve_module.resolve_signal(door, "b")
            self.kv_calls_seen.append(door.kv_calls)

    door = FakeDoor()
    instance = MultiPublish().attach(FakeRuntime(door, buffer=None))
    constant_triggers, signal_triggers = watch.gather_triggers([instance])
    client = franzmq.Client(client_id="watch-onepass")
    loop = asyncio.get_running_loop()
    watch.start(client, NODE_ID, door, loop, constant_triggers, signal_triggers)

    payload = Constant(id="c-3", name="setpoint", data_type=ConstantDataType.FLOAT64, value=1.0).encode()
    _deliver(client, f"colca/v1/_Constant/{NODE_ID}/line1/operator/setpoint", payload)
    await asyncio.sleep(0.05)

    assert door.kv_calls == 1, "two resolve_signal() calls inside one handler must share one pinned KV read"
    assert instance.kv_calls_seen == [1]


@run_async
async def test_handler_exception_is_logged_and_does_not_stop_the_client(caplog):
    class Broken(Producer):
        name = "broken_watch"
        system_element_name = "SE-Broken"

        @on_constant("line1/operator/setpoint")
        async def on_setpoint(self, constant) -> None:
            raise RuntimeError("boom")

    door = FakeDoor()
    instance = Broken().attach(FakeRuntime(door, buffer=None))
    constant_triggers, signal_triggers = watch.gather_triggers([instance])
    client = franzmq.Client(client_id="watch-broken")
    loop = asyncio.get_running_loop()
    watch.start(client, NODE_ID, door, loop, constant_triggers, signal_triggers)

    payload = Constant(id="c-4", name="setpoint", data_type=ConstantDataType.FLOAT64, value=1.0).encode()
    with caplog.at_level("ERROR", logger="chaski.dataops.watch"):
        _deliver(client, f"colca/v1/_Constant/{NODE_ID}/line1/operator/setpoint", payload)
        await asyncio.sleep(0.05)

    assert any("boom" in r.message or "boom" in str(r.exc_info) for r in caplog.records)
