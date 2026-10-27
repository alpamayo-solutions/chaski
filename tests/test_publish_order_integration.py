"""Publish order across a reconnect, against a real colcad.

A publisher keeps several QoS 1 writes on the wire (franzmq's in-flight
window). When the link drops mid-window, the unacknowledged ones are resent
after the reconnect, and a write made meanwhile must not overtake them: a
node keeps the record that arrived last on a retained topic, and a stream
reader handles records in arrival order. Duplicates are allowed (QoS 1 resends
what the node stored but did not acknowledge), loss and overtaking are not.
"""

from __future__ import annotations

import os
import socket
import time
import uuid

import pytest
from colca_data_contracts.payload import Metric

import chaski

pytestmark = [
    pytest.mark.skipif(
        not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
    ),
]

COUNT = 3000


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door rather than the unit suite's default fake."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """The integration caller supplies the bundle matching its binary."""


def _wait(predicate, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while not (result := predicate()):
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.05)
    return result


def test_a_reconnect_mid_window_loses_nothing_and_nothing_overtakes_an_older_write(tmp_path):
    with (
        chaski.Node("order", data_dir=tmp_path / "node") as node,
        node.service("writer", mount="Line/M1") as writer,
        node.service("reader") as reader,
    ):
        writer.publish("seq", 0.0)
        _wait(lambda: writer._bindings and not writer._pending_sources)
        binding = next(iter(writer._bindings.values()))
        topic, signal_id = binding.topic, binding.signal.id
        client = writer._started_client
        reconnects = []
        on_connect = client.on_connect

        def counting(*args, **kwargs):
            reconnects.append(time.monotonic())
            return on_connect(*args, **kwargs)

        client.on_connect = counting
        base = 1_700_000_000.0
        for seq in range(1, COUNT + 1):
            # Paced, so writes go on while the link is down and while it
            # comes back, where an overtaking write would come from.
            time.sleep(0.001)
            client.publish(
                topic, Metric(value=float(seq), timestamp=base + seq, signal_id=signal_id), qos=1, wait=False
            )
            if seq == COUNT // 3:
                # The link drops with a window of writes unacknowledged.
                client.socket().shutdown(socket.SHUT_RDWR)
        # paho's wait_for_publish refuses a message published while the link
        # was down, though it is sent after the reconnect: wait until its
        # store of unacknowledged messages is empty.
        _wait(lambda: not client._out_messages, timeout=60)
        assert reconnects, "the link did not drop and come back"

        stream = reader.stream("metrics", cursor=f"order-{uuid.uuid4().hex[:8]}", signal_ids=[signal_id])
        values = [float(r.payload["value"]) for r in stream if r.payload.get("signal_id") == signal_id]

    seen: set[float] = set()
    highest = 0.0
    overtaken = []
    for value in values:
        if value not in seen and value < highest:
            overtaken.append((value, highest))
        seen.add(value)
        highest = max(highest, value)
    assert not overtaken, f"{len(overtaken)} write(s) arrived after a newer one, first {overtaken[:3]}"
    assert {float(s) for s in range(COUNT + 1)} <= seen, "a write was lost"
    assert values[-1] == float(COUNT), "the last write is not the last record"
