"""Tests for ``chaski.ConnectorService`` with a fake driver, a fake node behind
both doors (MQTT and KV), and the loop driven one iteration at a time.

Covered: the catalogue is published once and again only when it changes, also
across restarts; tag ids outlive discovery; bindings follow ``data_tag``,
including tombstones and other services' tags; values are published on change
at the Signal's topic and precision; the heartbeat survives a lost source;
``is_connected`` is published on change; a broker outage fills a bounded buffer
that is retried in order; the source reconnects with backoff; and an outage is
logged once, not per poll.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import threading
import time
from types import SimpleNamespace
from typing import Any

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from colca_data_contracts.local_service import LocalServiceIdentity
from colca_data_contracts.payload import DataTag, DataTags, Metric, ServiceDetails, Signal
from franzmq import Topic
from franzmq.errors import PublishRejected, PublishTimeout

from chaski.connector import (
    HEARTBEAT_TAG_SOURCE,
    IS_CONNECTED_TAG_SOURCE,
    ConnectorService,
    Discovery,
    Driver,
    MqttDisconnectedError,
    Reading,
    SourceDisconnectedError,
    Telemetry,
    has_non_finite,
    refusal_is_final,
)
from chaski.door import KvEntry

NODE = "n-edge1"
NAME = "connector-opcua"
ULID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}")


class _ReasonCode:
    is_failure = False


class FakeNode:
    """A colca node's local doors in memory: retained state, a publish log,
    subscriptions with callbacks and a connectivity switch. One object plays
    both the MQTT client and the ``Door``."""

    def __init__(self) -> None:
        self.retained: dict[str, dict] = {}
        self.published: list[tuple[str, Any]] = []
        self.subscriptions: dict[str, Any] = {}
        self.unsubscribed: list[str] = []
        self.connected = True
        self.reject: Exception | None = None
        self.reconnects = 0
        self.on_connect = None
        self.on_disconnect = None
        self.node_id = None
        self.queue_limit: int | None = None

    # -- the Door half --
    def door(self, base_url: str, service: str, *, timeout: float = 10.0, cert=None):
        return self

    def close(self) -> None:
        pass

    def kv(self, prefix: str = "", *, contract=None) -> list[KvEntry]:
        out = []
        for topic, payload in self.retained.items():
            path = "/".join(topic.split("/")[4:])
            if not path.startswith(prefix):
                continue
            if contract and not topic.startswith(f"colca/v1/{contract}/"):
                continue
            out.append(KvEntry(path=path, node_id=NODE, topic=topic, payload=payload, ts=0, offset=0))
        return out

    def publish_batch(self, records):
        results = []
        for topic, payload in records:
            try:
                self.publish(topic, Metric(**json.loads(payload)), qos=1)
            except PublishRejected as exc:
                results.append({"error": str(exc)})
            except PublishTimeout as exc:
                raise MqttDisconnectedError(str(exc)) from exc
            else:
                results.append({"stream": "metrics", "offset": len(self.published)})
        return results

    # -- the MQTT half --
    def _handle_on_message(self, message) -> None:
        pass

    def loop_start(self) -> None:
        if self.on_connect is not None:
            self.on_connect(self, None, None, _ReasonCode())

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def reconnect(self) -> None:
        self.reconnects += 1
        if not self.connected:
            raise ConnectionError("broker still down")

    def is_connected(self) -> bool:
        return self.connected

    def max_queued_messages_set(self, limit: int) -> None:
        self.queue_limit = limit

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:
        self.subscriptions[str(topic)] = callback

    def unsubscribe(self, topic) -> None:
        self.unsubscribed.append(str(topic))
        self.subscriptions.pop(str(topic), None)

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:
        if self.reject is not None:
            raise self.reject
        if not self.connected:
            raise PublishTimeout(str(topic), 10.0)
        self.published.append((str(topic), payload))
        if retain:
            self.retained[str(topic)] = json.loads(payload) if isinstance(payload, str) else payload.__dict__

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:
        pass

    # -- what a test drives --
    def deliver_signal(
        self, *, path: str, signal_id: str, data_tag: str, is_published: bool = True, precision: int | None = None
    ) -> str:
        topic = f"colca/v1/_Signal/{NODE}/{path}"
        signal = Signal(
            id=signal_id,
            name=path.rsplit("/", 1)[-1],
            data_tag=data_tag,
            is_published=is_published,
            precision=precision,
        )
        self._deliver(topic, signal)
        return topic

    def deliver_tombstone(self, topic: str) -> None:
        self._deliver(topic, None)

    def _deliver(self, topic: str, payload: Any) -> None:
        for filt, callback in self.subscriptions.items():
            prefix = filt[:-1] if filt.endswith("#") else filt
            if topic == filt or topic.startswith(prefix):
                callback(SimpleNamespace(topic=topic, payload=payload))
                return
        raise AssertionError(f"no subscription matches {topic!r}: {list(self.subscriptions)}")

    # -- observation --
    def catalogues(self) -> list[DataTags]:
        return [p for _t, p in self.published if isinstance(p, DataTags)]

    def metrics(self) -> list[tuple[str, Metric]]:
        return [(t, p) for t, p in self.published if isinstance(p, Metric)]

    def details(self) -> list[ServiceDetails]:
        return [p for _t, p in self.published if isinstance(p, ServiceDetails)]

    def findings(self) -> list[dict]:
        return [json.loads(p) for t, p in self.published if "/_Finding/" in t]


class FakeDriver(Driver):
    """A source with tags keyed by natural address and a value per tag.
    ``fail_reads`` makes every read raise SourceDisconnectedError;
    ``fail_connects`` counts down connect failures."""

    protocol = "fake"

    def __init__(self, *, requires_connection: bool = True) -> None:
        super().__init__(logger=logging.getLogger("fake-driver"))
        self.metadata = {"protocol": "FAKE"}
        self.catalogue_requires_connection = requires_connection
        self.tags: dict[str, dict] = {"Axis1/Temperature": {"name": "Temperature", "data_type": "float"}}
        self.values: dict[str, Any] = {"Axis1/Temperature": 42.0}
        self.connects = 0
        self.closes = 0
        self.reads: list[list[str]] = []
        self.fail_reads = False
        self.fail_connects = 0
        self.fail_discover = False

    async def connect(self) -> None:
        self.connects += 1
        if self.fail_connects > 0:
            self.fail_connects -= 1
            raise RuntimeError("source unreachable")

    async def discover(self) -> Discovery:
        if self.fail_discover:
            raise RuntimeError("browse failed")
        tags = {
            source: DataTag(
                id="",
                name=spec["name"],
                source=source,
                is_writable=False,
                is_readable=True,
                data_type=spec["data_type"],
                meta={},
            )
            for source, spec in self.tags.items()
        }
        return Discovery(tags=tags, handles={source: source for source in self.tags})

    async def read(self, targets):
        if self.fail_reads:
            raise SourceDisconnectedError("link down")
        self.reads.append([handle for _s, handle, _t in targets])
        return [(topic, self.values[handle], signal) for signal, handle, topic in targets if handle in self.values]

    async def close(self) -> None:
        self.closes += 1


class RecordingTelemetry(Telemetry):
    def __init__(self) -> None:
        self.events: list[tuple] = []

    def broker_healthy(self, healthy: bool) -> None:
        self.events.append(("broker", healthy))

    def source_healthy(self, healthy: bool) -> None:
        self.events.append(("source", healthy))

    def published(self, metric: Metric, *, node_id: str) -> None:
        self.events.append(("published", metric.signal_id, node_id))

    def publish_rejected(self, reason_code: int) -> None:
        self.events.append(("rejected", reason_code))

    def sample_refused(self, signal_id: str, reason: str) -> None:
        self.events.append(("refused", signal_id, reason))

    def poll_completed(self, duration_s: float, *, overrun: bool) -> None:
        self.events.append(("poll", overrun))


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture(autouse=True)
def durable_state(tmp_path, monkeypatch):
    monkeypatch.setenv("COLCA_STATE_DIR", str(tmp_path))


@pytest.fixture
def node() -> FakeNode:
    return FakeNode()


@pytest.fixture
def driver() -> FakeDriver:
    return FakeDriver()


def make_service(node: FakeNode, driver: Driver, monkeypatch, *, mount: str = "", **kwargs) -> ConnectorService:
    identity = LocalServiceIdentity(
        service_id="01J00000000000000000000000",
        service_name=NAME,
        node_id=NODE,
        system_element_id="el-1" if mount else "",
        mount=mount,
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)

    def connect(*args, **kwargs):
        node.max_queued_messages_set(kwargs["max_queued_messages"])
        return node, identity

    monkeypatch.setattr("chaski.service.connect_local_mqtt", connect)
    monkeypatch.setattr("chaski.service.Door", node.door)
    clock = kwargs.pop("clock", None) or Clock()
    connector = kwargs.pop("connector", ConnectorService)
    svc = connector(NAME, mount, driver=driver, interval=0.0, **kwargs)
    svc._now = clock
    slept: list[float] = []

    async def no_sleep(seconds: float) -> None:
        slept.append(seconds)

    svc._sleep = no_sleep

    async def no_wait(version, timeout=None, **kwargs):
        if timeout is not None:
            await no_sleep(timeout)

    svc.clock.changes.wait_async = no_wait
    svc.slept = slept  # type: ignore[attr-defined]
    return svc


def started(node: FakeNode, driver: Driver, monkeypatch, **kwargs) -> ConnectorService:
    """A service through start() and its startup discovery — what serve()
    does before the first poll."""
    svc = make_service(node, driver, monkeypatch, **kwargs)
    svc.start()
    asyncio.run(svc._startup_discovery())
    return svc


def poll(svc: ConnectorService, times: int = 1) -> None:
    for _ in range(times):
        asyncio.run(svc._poll_iteration())


def tag_ids(catalogue: DataTags) -> dict[str, str]:
    return {t.source: t.id for t in catalogue.data_tags}


def bind(node: FakeNode, svc: ConnectorService, source: str, *, path: str, signal_id: str, **kw) -> str:
    return node.deliver_signal(path=path, signal_id=signal_id, data_tag=svc._catalogue.tag_id(source), **kw)


def test_factory_clock_accelerates_reads_and_pause_keeps_heartbeat(node, driver, monkeypatch):
    from colca_data_contracts.payload import ClockDefinition

    from chaski.clock import Clock as ApplicationClock

    real = Clock()
    svc = started(node, driver, monkeypatch, clock=real)
    svc.interval = 10
    wait = svc.clock.changes.wait_async
    svc.clock = ApplicationClock(wall=real)
    svc.clock.changes.wait_async = wait
    svc.clock.apply_definition(ClockDefinition("factory", "run", 1, 1000, 100, 100))
    bind(node, svc, "Axis1/Temperature", path="temperature", signal_id="temp")
    bind(node, svc, HEARTBEAT_TAG_SOURCE, path="heartbeat", signal_id="heartbeat")
    poll(svc)
    assert len(driver.reads) == 1
    assert 0 < svc.slept[-1] <= 0.1
    assert next(m for _, m in node.metrics() if m.signal_id == "temp").timestamp == 100

    svc.clock.apply_definition(ClockDefinition("factory", "run", 2, 1000, 100, 0))
    real.now += 5
    poll(svc)
    assert len(driver.reads) == 1
    # The heartbeat runs on its own schedule, paused clock or not.
    svc._publish_heartbeat()
    heartbeats = [m for _, m in node.metrics() if m.signal_id == "heartbeat"]
    assert len(heartbeats) == 1
    assert all(m.timestamp > 1_000_000_000 for m in heartbeats)

    svc.clock.apply_definition(ClockDefinition("factory", "run", 3, 1005, 100, 10))
    real.now += 1
    driver.values["Axis1/Temperature"] = 43
    poll(svc)
    assert len(driver.reads) == 2
    assert [m.timestamp for _, m in node.metrics() if m.signal_id == "temp"] == [100, 110]
    assert 0 < svc.slept[-1] <= 1


@pytest.mark.parametrize("heartbeat", [False, True])
def test_future_start_wakes_acquisition_and_nothing_else(node, driver, monkeypatch, heartbeat):
    from colca_data_contracts.payload import ClockDefinition

    from chaski.clock import Clock as ApplicationClock

    real = Clock()
    svc = started(node, driver, monkeypatch, clock=real)
    svc._heartbeat_start = real.now
    svc.interval = 1
    wait = svc.clock.changes.wait_async
    svc.clock = ApplicationClock(wall=real)
    svc.clock.changes.wait_async = wait
    svc.clock.apply_definition(ClockDefinition("factory", "run", 1, 1001, 100, 100, stop_at=400))
    bind(node, svc, "Axis1/Temperature", path="temperature", signal_id="temp")
    if heartbeat:
        bind(node, svc, HEARTBEAT_TAG_SOURCE, path="heartbeat", signal_id="heartbeat")

    poll(svc)
    assert driver.reads == []
    assert svc.slept[-1] == pytest.approx(1)
    real.now += svc.slept[-1]
    poll(svc)
    assert len(driver.reads) == 1
    assert next(m for _, m in node.metrics() if m.signal_id == "temp").timestamp == 100

    # Once the bounded run is over, do not replace idle waiting with polling;
    # a bound heartbeat does not wake the loop either, it has its own schedule.
    real.now += 3
    svc.slept.clear()
    poll(svc)
    assert len(driver.reads) == 1
    assert not svc.slept


def test_missing_clock_stops_sampling_without_stopping_health(node, driver, monkeypatch):
    from chaski.clock import Clock as ApplicationClock

    svc = started(node, driver, monkeypatch)
    wait = svc.clock.changes.wait_async
    svc.clock = ApplicationClock(source="mqtt")
    svc.clock.changes.wait_async = wait
    bind(node, svc, "Axis1/Temperature", path="temperature", signal_id="temp")
    bind(node, svc, HEARTBEAT_TAG_SOURCE, path="heartbeat", signal_id="heartbeat")
    poll(svc)
    svc._publish_heartbeat()
    assert driver.reads == []
    assert [m.signal_id for _, m in node.metrics()] == ["heartbeat"]


# ── the catalogue ──────────────────────────────────────────────────────


def test_discovery_publishes_the_catalogue_once_and_not_again_unchanged(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    poll(svc, 3)

    catalogues = node.catalogues()
    assert len(catalogues) == 1, (
        "an unchanged catalogue must not be republished: one fat record costs a "
        "full re-append and re-replication every time"
    )
    topic = next(t for t, p in node.published if isinstance(p, DataTags))
    assert topic == f"colca/v1/_DataTags/{NODE}/{NAME}", "unplaced: the catalogue sits at the node root + name"
    assert catalogues[0].connector == "01J00000000000000000000000", "the record carries the registry ULID, not the name"
    ids = tag_ids(catalogues[0])
    assert set(ids) == {"Axis1/Temperature", HEARTBEAT_TAG_SOURCE, IS_CONNECTED_TAG_SOURCE}, (
        "the two synthetic tags are part of the same catalogue as the protocol tags"
    )
    assert all(ULID.fullmatch(i) for i in ids.values())


def test_the_catalogue_goes_to_the_connectors_own_mount(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch, mount="line1/press3")
    poll(svc)
    topic = next(t for t, p in node.published if isinstance(p, DataTags))
    assert topic == f"colca/v1/_DataTags/{NODE}/line1/press3/{NAME}"
    assert str(svc._signal_filter) == f"colca/v1/_Signal/{NODE}/line1/press3/#", (
        "the _Signal subscription is narrowed to the connector's own read scope"
    )


def test_a_changed_catalogue_is_republished_with_a_new_revision(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    poll(svc)
    driver.tags["Axis2/Temperature"] = {"name": "Temperature", "data_type": "float"}
    svc._declare_discovery(asyncio.run(driver.discover()))
    poll(svc)

    first, second = node.catalogues()
    assert first.version != second.version
    assert tag_ids(second)["Axis1/Temperature"] == tag_ids(first)["Axis1/Temperature"]
    assert tag_ids(second)["Axis2/Temperature"] not in tag_ids(first).values()


def test_a_rejected_catalogue_is_not_recorded_as_published(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    node.reject = PublishRejected(0x99, "catalogue")
    poll(svc)
    assert node.catalogues() == []
    assert svc._catalogue.last_published_revision is None, "the broker refused it; the next attempt must try again"

    node.reject = None
    poll(svc)
    assert len(node.catalogues()) == 1


def test_a_restart_reuses_every_tag_id_and_republishes_nothing(node, driver, monkeypatch):
    """A restart is a fresh object against the same node; the ids and the
    republish guard come back from the node's retained record."""
    first = started(node, driver, monkeypatch)
    poll(first)
    before = tag_ids(node.catalogues()[0])
    assert len(node.catalogues()) == 1, "sanity: the first run does publish"

    second = started(node, FakeDriver(), monkeypatch)
    poll(second, 2)

    assert len(node.catalogues()) == 1, (
        "an unchanged catalogue republished after a restart with nothing on the source having changed"
    )
    assert {s: second._catalogue.tag_id(s) for s in before} == before, (
        "rediscovery minted new ids; every bound signal just broke"
    )


def test_a_vanished_tag_keeps_its_id_stale_and_a_returning_one_is_revived(node, driver, monkeypatch):
    first = started(node, driver, monkeypatch)
    poll(first)
    before = tag_ids(node.catalogues()[0])

    gone = FakeDriver()
    gone.tags.clear()
    second = started(node, gone, monkeypatch)
    poll(second)
    catalogue = node.catalogues()[-1]
    stale = next(t for t in catalogue.data_tags if t.source == "Axis1/Temperature")
    assert stale.id == before["Axis1/Temperature"] and stale.is_stale is True

    third = started(node, FakeDriver(), monkeypatch)
    poll(third)
    back = next(t for t in node.catalogues()[-1].data_tags if t.source == "Axis1/Temperature")
    assert back.id == before["Axis1/Temperature"] and back.is_stale is False


def test_a_file_mapped_driver_advertises_its_catalogue_while_the_source_is_down(node, monkeypatch):
    """S7/Modbus derive their catalogue from config, so signals can be bound
    before the machine is physically connected."""
    driver = FakeDriver(requires_connection=False)
    driver.fail_connects = 10
    svc = started(node, driver, monkeypatch)
    poll(svc)
    assert "Axis1/Temperature" in tag_ids(node.catalogues()[0])
    assert svc._source_healthy is False


def test_a_browse_driver_that_started_with_its_source_down_discovers_on_the_retry(node, driver, monkeypatch):
    """The retry is a full connect and discover. Until it succeeds only the
    synthetic tags are advertised, and known ids are carried forward."""
    clock = Clock()
    # Startup fails, and so does the first poll's retry (it is immediate);
    # the next retry is due DISCOVERY_RETRY_SECONDS later.
    driver.fail_connects = 2
    svc = started(node, driver, monkeypatch, clock=clock)
    poll(svc)
    assert set(tag_ids(node.catalogues()[0])) == {HEARTBEAT_TAG_SOURCE, IS_CONNECTED_TAG_SOURCE}
    clock.now += 1.0
    poll(svc)
    assert driver.connects == 2, "not due yet: no third attempt after one second"

    clock.now += 15.0
    poll(svc)
    assert "Axis1/Temperature" in tag_ids(node.catalogues()[-1])
    assert driver.connects == 3 and driver.closes == 2


# ── bindings ────────────────────────────────────────────────────────────


def test_a_binding_makes_the_tag_polled_at_the_signals_own_topic(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    poll(svc)
    assert driver.reads == [], "nothing bound, nothing read"

    bind(node, svc, "Axis1/Temperature", path="line1/press3/temp", signal_id="01JSIG1")
    poll(svc)

    assert driver.reads == [["Axis1/Temperature"]]
    topic, metric = node.metrics()[0]
    assert topic == f"colca/v1/_Metric/{NODE}/line1/press3/temp", (
        "the metric belongs at the SIGNAL's own path, not one derived from the tag"
    )
    assert metric.signal_id == "01JSIG1" and metric.value == 42.0
    assert isinstance(metric.timestamp, float)


def test_a_signal_naming_a_tag_this_connector_never_minted_is_ignored(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    node.deliver_signal(path="line1/press9/other", signal_id="01JOTHER", data_tag="not-ours")
    poll(svc)
    assert svc._bindings == {} and driver.reads == [] and node.metrics() == []


def test_an_unpublished_signal_produces_no_target_and_a_tombstone_removes_one(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    off = bind(node, svc, "Axis1/Temperature", path="line1/off", signal_id="s-off", is_published=False)
    assert len(svc._bindings) == 1 and svc._targets == [], "bound, but the node said not to publish it"

    on = bind(node, svc, "Axis1/Temperature", path="line1/on", signal_id="s-on")
    assert [t.signal.id for t in svc._targets] == ["s-on"]

    node.deliver_tombstone(on)
    node.deliver_tombstone(off)
    assert svc._bindings == {} and svc._targets == [], "a retired signal must stop being published"


def test_a_stale_tag_is_still_a_known_binding_but_publishes_nothing(node, driver, monkeypatch):
    first = started(node, driver, monkeypatch)
    poll(first)
    gone = FakeDriver()
    gone.tags.clear()
    svc = started(node, gone, monkeypatch)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s1")
    assert len(svc._bindings) == 1, "the binding is still ours — the tag is stale, not unknown"
    assert svc._targets == []


def test_two_signals_may_read_the_same_tag(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    bind(node, svc, "Axis1/Temperature", path="line1/a", signal_id="s-a")
    bind(node, svc, "Axis1/Temperature", path="line1/b", signal_id="s-b")
    poll(svc)
    assert sorted(m.signal_id for _t, m in node.metrics()) == ["s-a", "s-b"]


# ── publish on change, precision, heartbeat ────────────────────────────


def test_unchanged_measurements_are_preserved_and_heartbeat_only_flips_on_its_interval(node, driver, monkeypatch):
    clock = Clock()
    svc = started(node, driver, monkeypatch, clock=clock, heartbeat_interval=5.0)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    bind(node, svc, HEARTBEAT_TAG_SOURCE, path="line1/heartbeat", signal_id="s-hb")

    poll(svc)
    svc._publish_heartbeat()
    assert [m.signal_id for _t, m in node.metrics()] == ["s-temp", "s-hb"]

    poll(svc)
    svc._publish_heartbeat()
    assert [m.signal_id for _t, m in node.metrics()[2:]] == ["s-temp"], (
        "the stable measurement is a new observation; the heartbeat is unchanged"
    )

    clock.now += 5.0
    poll(svc)
    svc._publish_heartbeat()
    assert [m.signal_id for _t, m in node.metrics()[3:]] == ["s-temp", "s-hb"]
    assert node.metrics()[-1][1].value != node.metrics()[1][1].value

    driver.values["Axis1/Temperature"] = 43.0
    poll(svc)
    assert node.metrics()[-1][1].signal_id == "s-temp" and node.metrics()[-1][1].value == 43.0


def test_precision_rounds_values_without_suppressing_observations(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s", precision=1)
    driver.values["Axis1/Temperature"] = 42.04
    poll(svc)
    driver.values["Axis1/Temperature"] = 42.06  # rounds to 42.1: a change at precision 1
    poll(svc)
    driver.values["Axis1/Temperature"] = 42.14  # still 42.1: not a change
    poll(svc)
    assert [m.value for _t, m in node.metrics()] == [42.0, 42.1, 42.1]


def test_every_metric_of_one_poll_carries_the_same_timestamp(node, driver, monkeypatch):
    driver.tags["Axis2/Speed"] = {"name": "Speed", "data_type": "float"}
    driver.values["Axis2/Speed"] = 7.0
    svc = started(node, driver, monkeypatch)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s1")
    bind(node, svc, "Axis2/Speed", path="line1/speed", signal_id="s2")
    poll(svc)
    stamps = {m.timestamp for _t, m in node.metrics()}
    assert len(node.metrics()) == 2 and len(stamps) == 1


def test_the_loop_paces_itself_on_the_interval_and_counts_an_overrun(node, driver, monkeypatch):
    telemetry = RecordingTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry)
    svc.interval = 60.0
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s1")
    poll(svc)
    assert svc.slept and 59.0 < svc.slept[-1] <= 60.0, "waits out the remainder of the interval"
    assert ("poll", False) in telemetry.events

    svc.interval = 0.0
    poll(svc)
    assert svc.slept[-1] == 0.001 and ("poll", True) in telemetry.events


# ── is_connected ────────────────────────────────────────────────────────


def test_is_connected_is_published_on_change_per_bound_signal(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    bind(node, svc, IS_CONNECTED_TAG_SOURCE, path="line1/conn", signal_id="s-conn")
    poll(svc)
    values = [m.value for _t, m in node.metrics() if m.signal_id == "s-conn"]
    assert values == [True], "the initial sample once a Signal is bound"

    driver.fail_reads = True
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    poll(svc)
    values = [m.value for _t, m in node.metrics() if m.signal_id == "s-conn"]
    assert values == [True, False], "a live source drop surfaces at once, while the broker stays up"

    poll(svc)
    values = [m.value for _t, m in node.metrics() if m.signal_id == "s-conn"]
    assert values == [True, False], "report-on-change: no spam while nothing changes"

    # A second binding for the same synthetic tag gets its own initial sample.
    bind(node, svc, IS_CONNECTED_TAG_SOURCE, path="line1/conn2", signal_id="s-conn2")
    poll(svc)
    assert [m.value for _t, m in node.metrics() if m.signal_id == "s-conn2"] == [False]


def test_a_failed_is_connected_publish_is_retried_next_poll(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    bind(node, svc, IS_CONNECTED_TAG_SOURCE, path="line1/conn", signal_id="s-conn")
    node.reject = RuntimeError("broker hiccup")
    poll(svc)
    assert svc._is_connected_published == {}, "unpublished: retried, not recorded"
    node.reject = None
    poll(svc)
    assert [m.value for _t, m in node.metrics() if m.signal_id == "s-conn"] == [True]


# ── a lost source ───────────────────────────────────────────────────────


def test_a_lost_source_keeps_the_heartbeat_and_reconnects_with_backoff(node, driver, monkeypatch):
    telemetry = RecordingTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry, reconnect_retries=3)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    bind(node, svc, HEARTBEAT_TAG_SOURCE, path="line1/hb", signal_id="s-hb")
    driver.fail_reads = True
    driver.fail_connects = 2

    poll(svc)
    svc._publish_heartbeat()

    assert [m.signal_id for _t, m in node.metrics()] == ["s-hb"], (
        "the connector is alive even when its source is not; the heartbeat says so"
    )
    assert ("source", False) in telemetry.events
    # Two failed reconnects with bounded exponential jitter, then success.
    assert driver.connects == 1 + 3 and driver.closes == 3
    backoffs = [s for s in svc.slept if s >= 0.5]
    assert len(backoffs) == 2 and 0.5 <= backoffs[0] <= 1.0 and 1.0 <= backoffs[1] <= 2.0, svc.slept

    driver.fail_reads = False
    poll(svc)
    assert node.metrics()[-1][1].signal_id == "s-temp"
    assert ("source", True) in telemetry.events[-4:]


def test_the_heartbeat_keeps_its_schedule_while_a_read_hangs(node, driver, monkeypatch):
    """A slow or unreachable source holds the poll loop for its timeouts and
    reconnects (a Modbus device that is off: tens of seconds per attempt). The
    heartbeat says the connector is alive, so it must not wait for the read."""
    clock = Clock()
    svc = started(node, driver, monkeypatch, clock=clock, heartbeat_interval=0.05)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    bind(node, svc, HEARTBEAT_TAG_SOURCE, path="line1/hb", signal_id="s-hb")

    async def scenario() -> list[str]:
        released = asyncio.Event()
        read = driver.read

        async def hanging_read(targets):
            await released.wait()
            return await read(targets)

        driver.read = hanging_read
        polling = asyncio.ensure_future(svc._poll_iteration())
        beating = asyncio.ensure_future(svc._heartbeat_forever())
        for _ in range(3):
            await asyncio.sleep(0.06)
            clock.now += svc.heartbeat_interval
        hung = [m.signal_id for _t, m in node.metrics()]
        assert not polling.done(), "the read is still hanging"
        released.set()
        await polling
        svc._stopping.set()
        await beating
        return hung

    hung = asyncio.run(scenario())
    assert hung.count("s-hb") >= 2 and "s-temp" not in hung, hung
    assert "s-temp" in [m.signal_id for _t, m in node.metrics()]


def test_exhausted_reconnects_stay_alive_and_try_again_next_poll(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch, reconnect_retries=2)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    driver.fail_reads = True
    driver.fail_connects = 100

    poll(svc)
    assert driver.connects == 3
    poll(svc)
    assert driver.connects == 5, "the next poll re-enters the reconnect path; no crash, no exit"
    assert svc._source_healthy is False


# ── a lost broker ───────────────────────────────────────────────────────


def _batch(n: int) -> list[tuple[Topic, Metric]]:
    return [
        (
            Topic(payload_type=Metric, node_id=NODE, context=("line1", f"tag{i}")),
            Metric(value=float(i), timestamp=0.0, signal_id=f"sig-{i}"),
        )
        for i in range(n)
    ]


def test_a_disconnected_broker_is_detected_before_publishing_and_does_not_stall(node, driver, monkeypatch):
    """paho queues publishes while disconnected, so the loop checks
    is_connected() first instead of waiting out one timeout per metric."""
    svc = started(node, driver, monkeypatch)
    node.connected = False
    batch = _batch(50)
    started_at = time.perf_counter()
    with pytest.raises(MqttDisconnectedError):
        svc._publish_batch(batch)
    assert time.perf_counter() - started_at < 1.0
    assert node.metrics() == [] and svc._pending == batch


def test_a_publish_timeout_mid_batch_buffers_the_rest_and_pending_goes_out_first(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    first = _batch(2)
    svc._publish_batch(first)
    node.published.clear()

    node.reject = PublishTimeout("some/topic", 10.0)
    second = _batch(3)
    with pytest.raises(MqttDisconnectedError):
        svc._publish_batch(second)
    assert svc._pending == second, "the failing metric and everything after it must be buffered"

    node.reject = None
    third = _batch(1)
    svc._publish_batch(third)
    assert [m for _t, m in node.metrics()] == [m for _t, m in second] + [m for _t, m in third], (
        "carried-over pending metrics publish before this cycle's own batch"
    )
    assert svc._pending == []


def test_backpressure_refuses_an_oversized_cycle_without_eviction(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch, max_pending=100)
    node.connected = False
    batch = _batch(125)
    with pytest.raises(BufferError):
        svc._publish_batch(batch)
    assert svc._pending == []
    with pytest.raises(MqttDisconnectedError):
        svc._publish_batch(batch[:100])
    with pytest.raises(BufferError):
        svc._publish_batch(batch[100:])
    assert svc._pending == batch[:100]
    assert node.queue_limit == 100, "paho's own queue is capped at the same bound — one bound, not two"


def test_a_broker_outage_in_the_loop_reconnects_and_flushes_when_it_returns(node, driver, monkeypatch, caplog):
    telemetry = RecordingTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry, reconnect_retries=2)
    poll(svc)  # the catalogue is out; the outage starts after it
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    node.connected = False
    with caplog.at_level(logging.INFO):
        poll(svc)
        assert node.reconnects == 2 and ("broker", False) in telemetry.events
        assert len(svc._pending) == 1
        node.connected = True
        driver.values["Axis1/Temperature"] = 43.0
        poll(svc)
    assert [m.value for _t, m in node.metrics()] == [42.0, 43.0], "pending first, then this cycle's"
    messages = [r.getMessage() for r in caplog.records if r.levelname in ("ERROR", "INFO")]
    assert any("MQTT disconnected" in m for m in messages)
    assert any("MQTT reconnected after" in m for m in messages)


def test_a_refusal_with_nothing_admitted_remains_durable_and_fails_visibly(node, driver, monkeypatch):
    """Nothing in the batch was admitted, so the refusal may be a failed write
    at the node: the sample stays and the failure is raised, not dropped."""
    telemetry = RecordingTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry)
    node.reject = PublishRejected(0x99, "colca/v1/_Metric/x")
    with pytest.raises(PublishRejected):
        svc._publish_batch(_batch(1))
    assert svc._pending == _batch(1)
    assert ("rejected", 0x80) in telemetry.events
    assert svc.refused_samples_total == 0 and node.findings() == []


# ── an outage is a state ────────────────────────────────────────────────


def test_a_long_outage_reports_once_then_on_the_interval(node, driver, monkeypatch, caplog):
    clock = Clock()
    svc = make_service(node, driver, monkeypatch, clock=clock, outage_reminder=300.0)
    error = RuntimeError("MQTT client not connected (checked before publish).")
    with caplog.at_level(logging.INFO):
        svc._report_mqtt_outage(error)
        for _ in range(301):
            clock.now += 1.0
            svc._report_mqtt_outage(error)
    reported = [(r.levelname, r.getMessage()) for r in caplog.records]
    assert len(reported) == 2, reported
    assert reported[0][0] == "ERROR" and "retrying until it answers" in reported[0][1]
    assert "still disconnected" in reported[1][1] and "301 attempts" in reported[1][1]


def test_recovery_is_reported_once_with_what_it_cost_and_a_second_outage_is_news_again(
    node,
    driver,
    monkeypatch,
    caplog,
):
    clock = Clock()
    svc = make_service(node, driver, monkeypatch, clock=clock)
    svc._report_mqtt_outage(RuntimeError("down"))
    clock.now += 42.0
    svc._report_mqtt_outage(RuntimeError("down"))
    caplog.clear()
    with caplog.at_level(logging.INFO):
        svc._report_mqtt_recovered()
        svc._report_mqtt_recovered()
        clock.now += 5.0
        svc._report_mqtt_outage(RuntimeError("down again"))
    reported = [(r.levelname, r.getMessage()) for r in caplog.records]
    assert len(reported) == 2, reported
    assert reported[0][0] == "INFO" and "42s" in reported[0][1] and "2 attempts" in reported[0][1]
    assert "down again" in reported[1][1]


# ── registration and placement ──────────────────────────────────────────


def test_registration_carries_the_drivers_protocol_and_the_given_presentation(node, driver, monkeypatch):
    health = [SimpleNamespace(key="source_healthy")]
    svc = started(
        node,
        driver,
        monkeypatch,
        metadata={"demo": "line1"},
        architecture_metadata={"icon": "svc-opcua.webp", "status": "stale"},
        health_metrics=health,
    )
    details = node.details()[-1]
    assert details.id == "01J00000000000000000000000" and details.name == NAME and details.colca_node_id == NODE
    assert details.metadata == {"protocol": "FAKE", "demo": "line1"}
    assert details.architecture_metadata == {"icon": "svc-opcua.webp", "status": "healthy"}, (
        "the live status wins over anything the presentation carried"
    )
    assert details.health_metrics == health and details.is_active is True
    svc.close()
    assert node.details()[-1].is_active is False
    assert len(node.catalogues()) == 0 or all(not t.is_stale for t in node.catalogues()[-1].data_tags), (
        "closing a connector stales nothing: discovery decides what is stale"
    )


def test_a_reconnect_re_resolves_placement_and_republishes_at_the_new_position(node, driver, monkeypatch):
    """A re-placement kicks the live session; the reconnect must follow the
    registry's CURRENT mount — new subscription, details and catalogue at
    the new topic — through the client the callback is handed."""
    svc = started(node, driver, monkeypatch)
    poll(svc)
    assert len(node.catalogues()) == 1
    moved = LocalServiceIdentity(
        service_id="01J00000000000000000000000",
        service_name=NAME,
        node_id=NODE,
        system_element_id="el-press3",
        mount="line1/press3",
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: moved)

    svc._on_connect(node, None, None, _ReasonCode())  # the reconnect

    assert node.unsubscribed == [
        f"colca/v1/_Signal/{NODE}/#",
        f"colca/v1/_ServiceDetails/{NODE}/{NAME}/_service",
        f"colca/v1/_Finding/{NODE}/{NAME}/cursor_lag",
    ]
    assert f"colca/v1/_Signal/{NODE}/line1/press3/#" in node.subscriptions
    assert f"colca/v1/_ServiceDetails/{NODE}/line1/press3/{NAME}/_service" in node.subscriptions
    # The node's cursor_lag finding follows the service to its new position.
    assert f"colca/v1/_Finding/{NODE}/line1/press3/{NAME}/cursor_lag" in node.subscriptions
    assert next(t for t, p in reversed(node.published) if isinstance(p, ServiceDetails)) == (
        f"colca/v1/_ServiceDetails/{NODE}/line1/press3/{NAME}/_service"
    )
    poll(svc)
    topics = [t for t, p in node.published if isinstance(p, DataTags)]
    assert topics[-1] == f"colca/v1/_DataTags/{NODE}/line1/press3/{NAME}"
    assert tag_ids(node.catalogues()[-1]) == tag_ids(node.catalogues()[0]), "a move mints nothing"


def test_the_local_door_is_opened_with_the_log_publisher_attached(node, driver, monkeypatch):
    """The connector publishes its log through connect_local_mqtt, like every
    local service."""
    calls: dict = {}

    def connect(name, **kwargs):
        calls.update(kwargs, name=name)
        return node, kwargs["identity"]

    identity = LocalServiceIdentity(
        service_id="svc-ulid", service_name=NAME, node_id=NODE, system_element_id="", mount=""
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", connect)
    monkeypatch.setattr("chaski.service.Door", node.door)
    ConnectorService(NAME, driver=driver).start()
    assert calls["name"] == NAME and calls["publish_logs"] is True and calls["will"] is not None


# ── the whole loop, end to end ──────────────────────────────────────────


def test_serve_runs_discovery_binding_and_polling_until_stopped(node, driver, monkeypatch):
    svc = make_service(node, driver, monkeypatch)
    svc.interval = 0.01
    svc._sleep = asyncio.sleep
    loop_ready = threading.Event()
    loop_holder: dict = {}

    async def serve() -> None:
        loop_holder["loop"] = asyncio.get_running_loop()
        loop_ready.set()
        await svc.serve()

    thread = threading.Thread(target=lambda: asyncio.run(serve()), daemon=True)
    thread.start()
    assert loop_ready.wait(5)

    def eventually(check, description: str, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if check():
                return
            time.sleep(0.02)
        raise AssertionError(f"timed out waiting for {description}; published={node.published}")

    eventually(lambda: node.catalogues(), "the catalogue")
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    eventually(lambda: any(m.signal_id == "s-temp" for _t, m in node.metrics()), "a metric")
    assert svc.is_broker_connected()

    asyncio.run_coroutine_threadsafe(svc.stop(), loop_holder["loop"]).result(5)
    thread.join(5)
    assert not thread.is_alive()
    assert node.details()[-1].is_active is False and driver.closes >= 1


def test_connector_outage_journal_survives_process_restart(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    samples = _batch(3)
    node.connected = False
    with pytest.raises(MqttDisconnectedError):
        svc._publish_batch(samples)
    node.connected = True
    svc.close()
    resumed = started(node, driver, monkeypatch)
    calls = []
    publish = node.publish_batch

    def record_batch(rows):
        calls.append(len(rows))
        return publish(rows)

    node.publish_batch = record_batch
    resumed._publish_batch([])
    assert calls == [3]
    assert [m for _, m in node.metrics()] == [m for _, m in samples]
    assert resumed._pending == []
    resumed.close()


def _refusing(node: FakeNode, refused: set[int], reason: str | None = None):
    """``node.publish_batch`` refusing the samples at the given positions."""
    publish = node.publish_batch

    def partial(rows):
        results = []
        for i, row in enumerate(rows):
            if i in refused:
                result = {"error": "_Metric: payload failed the contract schema"}
                if reason is not None:
                    result["reason"] = reason
                results.append(result)
            else:
                results += publish([row])
        return results

    return partial


def test_a_refusal_beside_an_admitted_sample_is_final_and_set_aside_once(node, driver, monkeypatch, caplog):
    """The node admitted the rest of the batch, so it judged the refused
    sample and refuses it for good: recorded, counted, removed. A second
    refusal of the same signal in the same spell is counted, not recorded
    again; an admitted sample of that signal ends the spell."""
    telemetry = RecordingTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry)
    publish = node.publish_batch
    node.publish_batch = _refusing(node, {1})
    with caplog.at_level(logging.INFO):
        svc._publish_batch(_batch(3))
        assert svc._pending == []
        assert [m.value for _, m in node.metrics()] == [0.0, 2.0]
        assert svc.refused_samples_total == 1
        assert ("refused", "sig-1", "refused") in telemetry.events
        [finding] = node.findings()
        assert finding["reason"] == "rejected_input"
        assert finding["detail"]["signal_id"] == "sig-1"
        assert "contract schema" in finding["summary"]

        svc._publish_batch(_batch(3))
        assert svc._pending == [] and svc.refused_samples_total == 2
        assert len(node.findings()) == 1, "once per signal per spell"
        assert svc.refusing()["sig-1"].refused == 2

        node.publish_batch = publish
        svc._publish_batch(_batch(3))
    assert "sig-1" not in svc.refusing()
    assert any("admitted again after 2 refused samples" in r.getMessage() for r in caplog.records)
    rejected = [r for r in caplog.records if "connector-samples rejected" in r.getMessage()]
    assert len(rejected) == 1


def test_a_refused_sample_that_cannot_be_recorded_stays_in_the_journal(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    node.publish_batch = _refusing(node, {1})
    real_publish = node.publish

    def no_findings(topic, payload, **kwargs):
        if "/_Finding/" in str(topic):
            raise PublishTimeout(str(topic), 10.0)
        return real_publish(topic, payload, **kwargs)

    node.publish = no_findings
    with pytest.raises(MqttDisconnectedError):
        svc._publish_batch(_batch(3))
    assert svc._pending == _batch(3)[1:2], "admitted samples retire; the unrecorded refusal stays"
    assert svc.refused_samples_total == 0


def test_a_named_reason_decides_even_with_nothing_admitted(node, driver, monkeypatch):
    svc = started(node, driver, monkeypatch)
    node.publish_batch = _refusing(node, {0}, reason="not_producer")
    svc._publish_batch(_batch(1))
    assert svc._pending == [] and svc.refused_samples_total == 1

    node.publish_batch = _refusing(node, {0, 1}, reason="draining")
    with pytest.raises(PublishRejected):
        svc._publish_batch(_batch(2))
    assert svc._pending == _batch(2)


def test_not_written_is_retried_even_beside_an_admitted_sample(node, driver, monkeypatch):
    """A named reason wins over the batch fallback: not_written beside an
    admitted sample is the node's failed write, kept and sent again."""
    svc = started(node, driver, monkeypatch)
    publish = node.publish_batch
    node.publish_batch = _refusing(node, {1}, reason="not_written")
    with pytest.raises(PublishRejected):
        svc._publish_batch(_batch(3))
    assert svc._pending == _batch(3)[1:2] and svc.refused_samples_total == 0
    node.publish_batch = publish
    svc._publish_batch([])
    assert [m.value for _, m in node.metrics()] == [0.0, 2.0, 1.0]


@pytest.mark.parametrize(
    ("result", "batch_admitted", "final"),
    [
        ({"stream": "metrics", "offset": 3}, False, False),
        ({"stream": "metrics", "offset": 3}, True, False),
        ({}, True, False),
        ({"error": "_Metric: field value missing"}, True, True),
        ({"error": "_Metric: field value missing"}, False, False),
        ({"error": "pebble: disk full"}, False, False),
        ({"error": "x", "reason": "validation"}, False, True),
        ({"error": "x", "reason": "grammar"}, False, True),
        ({"error": "x", "reason": "node_id"}, False, True),
        ({"error": "x", "reason": "identity"}, False, True),
        ({"error": "x", "reason": "write_denied"}, False, True),
        ({"error": "x", "reason": "not_producer"}, False, True),
        ({"error": "x", "reason": "registry_contract"}, False, True),
        ({"error": "x", "reason": "time_sync"}, False, True),
        ({"error": "x", "reason": "too_large"}, False, True),
        ({"error": "x", "reason": "not_written"}, True, False),
        ({"error": "x", "reason": "draining"}, True, False),
        ({"error": "x", "reason": "something_new"}, True, False),
    ],
)
def test_refusal_classification(result, batch_admitted, final):
    assert refusal_is_final(result, batch_admitted=batch_admitted) is final


@pytest.mark.parametrize(
    ("value", "non_finite"),
    [
        (1.5, False),
        (0, False),
        (True, False),
        ("nan", False),
        (None, False),
        (math.nan, True),
        (math.inf, True),
        (-math.inf, True),
        ([1.0, math.nan], True),
        ({"a": {"b": -math.inf}}, True),
        ([1.0, {"a": 2.0}], False),
    ],
)
def test_non_finite_detection(value, non_finite):
    assert has_non_finite(value) is non_finite


def test_a_non_finite_reading_is_refused_before_the_journal(node, driver, monkeypatch, caplog):
    """NaN and ±Inf cannot be written as JSON: the reading is set aside at
    acquisition, once per spell, and the readings after it go out."""
    telemetry = RecordingTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry)
    poll(svc)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    with caplog.at_level(logging.INFO):
        for value in (math.nan, math.inf, -math.inf):
            driver.values["Axis1/Temperature"] = value
            poll(svc)
        assert svc._pending == [] and node.metrics() == []
        assert svc.refused_samples_total == 3
        assert telemetry.events.count(("refused", "s-temp", "non_finite")) == 3
        [finding] = node.findings()
        assert finding["detail"]["signal_id"] == "s-temp" and "non-finite" in finding["summary"]

        driver.values["Axis1/Temperature"] = 42.5
        poll(svc)
    assert [m.value for _, m in node.metrics()] == [42.5]
    assert svc.refusing() == {}
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)


class SourceTimestampDriver(FakeDriver):
    """Returns each reading with the source timestamp in ``stamp``."""

    def __init__(self) -> None:
        super().__init__()
        self.stamp = 1000.0

    async def read(self, targets):
        return [Reading(topic, value, signal, self.stamp) for topic, value, signal in await super().read(targets)]


def test_a_non_finite_source_timestamp_is_refused_before_the_journal(node, monkeypatch, caplog):
    driver = SourceTimestampDriver()
    telemetry = RecordingTelemetry()
    svc = started(node, driver, monkeypatch, telemetry=telemetry, timestamp_source="source")
    poll(svc)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    with caplog.at_level(logging.INFO):
        for stamp in (math.nan, math.inf):
            driver.stamp = stamp
            poll(svc)
        assert svc._pending == [] and node.metrics() == []
        assert svc.refused_samples_total == 2
        assert telemetry.events.count(("refused", "s-temp", "non_finite_timestamp")) == 2
        [finding] = node.findings()
        assert "non-finite source timestamp" in finding["summary"]

        driver.stamp = 1234.5
        poll(svc)
    assert [m.timestamp for _, m in node.metrics()] == [1234.5]
    assert svc.refusing() == {}
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)


def test_a_journal_holding_only_refusals_still_acquires_so_the_next_sample_decides(node, driver, monkeypatch):
    """Everything the node was sent was refused: undecided. The loop still
    acquires one more cycle; the node admits that sample, which makes the
    older refusal final."""
    svc = started(node, driver, monkeypatch)
    poll(svc)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    stale = (Topic(payload_type=Metric, node_id=NODE, context=("line1", "gone")), Metric(1.0, 0.0, signal_id="gone"))
    svc._open_metric_queue().append_batch([(str(stale[0]), json.loads(stale[1].encode()), 0.0, None)], 10)
    publish = node.publish_batch

    def refuse_gone(rows):
        results = []
        for row in rows:
            if json.loads(row[1])["signal_id"] == "gone":
                results.append({"error": "_Metric: payload failed the contract schema"})
            else:
                results += publish([row])
        return results

    node.publish_batch = refuse_gone
    poll(svc)
    assert svc._pending == []
    assert [m.value for _, m in node.metrics()] == [42.0]
    assert svc.refused_samples_total == 1


def test_batch_throttling_preserves_samples_and_server_retry_deadline(node, driver, monkeypatch):
    import httpx

    svc = started(node, driver, monkeypatch)
    bind(node, svc, "Axis1/Temperature", path="line1/temp", signal_id="s-temp")
    publish = node.publish_batch

    def throttled(rows):
        response = httpx.Response(
            429, headers={"Retry-After": "7"}, request=httpx.Request("POST", "http://colca/publish/batch")
        )
        response.raise_for_status()

    node.publish_batch = throttled
    poll(svc)
    assert 7 <= svc.slept[-1] <= 8.4
    assert len(svc._pending) == 1
    node.publish_batch = publish
    svc._publish_batch([])
    assert svc._pending == []
