"""Registration stays stable while ordered readiness crosses the real broker."""

import os
import socket
import threading
import time

import pytest
from colca_data_contracts.payload import ClockDefinition

from chaski import Clock, Node

pytestmark = pytest.mark.skipif(not os.environ.get("COLCAD_BINARY"), reason="requires matching colcad and bundle")


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """Use the caller's matching contract bundle."""


def _ready(service, timeout=10):
    deadline = time.monotonic() + timeout
    while True:
        version = service.step.changes.version
        ready = service.step.ready()
        if ready is not None:
            return ready
        remaining = deadline - time.monotonic()
        assert remaining > 0, "ordered live readiness never arrived"
        service.step.changes.wait(version, remaining)


def test_paused_control_and_reconnect_do_not_republish_registration(tmp_path):
    real = time.time()
    definition = ClockDefinition("factory", "run", 1, real, 1000, 0, 1000)
    source_clock, sink_clock = Clock(), Clock()
    source_clock.apply_definition(definition)
    sink_clock.apply_definition(definition)
    with (
        Node("discovery-control", data_dir=tmp_path / "node") as node,
        node.service("source", clock=source_clock, step_dependencies=[]) as source,
        node.service("sink", clock=sink_clock, step_dependencies=[str(source._details_topic)]) as sink,
    ):
        entities = source.stream("entities", cursor="registration-test")
        before = entities.fetch().next
        assert source.report_progress(1000, force=True)
        assert _ready(sink) == 1000
        # Force a paused readiness refresh without waiting on a timer.
        source._last_clock_report = float("-inf")
        assert source._publish_progress()
        assert entities.fetch().next == before
        reconnect = threading.Event()
        original = sink._reannounce

        def reannounce(client, fresh=False):
            original(client, fresh)
            reconnect.set()

        sink._reannounce = reannounce
        sink._started_client.socket().shutdown(socket.SHUT_RDWR)
        assert reconnect.wait(10)
        assert _ready(sink) == 1000
        # Reconnect can change discovery; subsequent control cannot.
        source_registrations = sum(row.topic == str(source._details_topic) for row in entities.fetch().records)
        source._last_clock_report = float("-inf")
        assert source._publish_progress()
        assert sum(row.topic == str(source._details_topic) for row in entities.fetch().records) == source_registrations
