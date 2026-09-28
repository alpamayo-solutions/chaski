"""A scoped consumer against a real node: woken by its own topics only, and
reading only its own signals."""

import os
import time

import pytest


@pytest.fixture(autouse=True)
def _no_node_door():
    pass


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    pass


def test_a_scoped_consumer_wakes_on_and_reads_only_its_signals(tmp_path):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires real colcad binary and contracts bundle")
    from chaski import Node

    with (
        Node("scoped", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("reader") as reader,
    ):
        machine.publish("state", 1)
        machine.publish("noise", 0)

        def signal(name):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                for row in reader.kv("", contract="_Signal"):
                    if row.path.endswith("/" + name):
                        return row
                time.sleep(0.1)
            raise AssertionError(f"no _Signal for {name}")

        state = signal("state")
        signal("noise")
        topic = state.topic.replace("/_Signal/", "/_Metric/", 1)
        wake = reader.wake_on([topic])
        stream = reader.stream("metrics", cursor="scoped", signal_ids=[state.payload["id"]])
        # The first sample reaches the metrics stream asynchronously, possibly
        # after a head read; consume it rather than ack a head that misses it.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            records = stream.fetch().records
            if records:
                stream.ack(records[-1])
            if any(r.payload["value"] == 1 for r in records):
                break
            time.sleep(0.1)
        else:
            raise AssertionError("first sample never reached the scoped stream")

        # Subscribing rings once, and the broker then delivers the retained
        # value; both land asynchronously. Wait for the bell to go quiet.
        seen = wake.bell.generation
        while wake.bell.wait_after(seen, timeout=0.5):
            seen = wake.bell.generation
        for i in range(20):
            machine.publish("noise", i)
        assert not wake.bell.wait_after(seen, timeout=1.5), "woken by a signal it does not read"

        machine.publish("state", 4)
        assert wake.bell.wait_after(seen, timeout=10), "not woken by its own signal"
        records = stream.fetch().records
        assert [r.payload["value"] for r in records] == [4]
        wake.close()


def test_two_consumers_of_one_service_both_wake_on_a_shared_topic(tmp_path):
    # paho keeps one callback per topic; the second wake_on replaced the first
    # and one consumer never woke again (tcdb-api's PLC consumer and its value
    # view both read a machine's machine_state).
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires real colcad binary and contracts bundle")
    from chaski import Node

    with (
        Node("shared", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="M") as machine,
        node.service("reader") as reader,
    ):
        machine.publish("state", 1)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            rows = [r for r in reader.kv("", contract="_Signal") if r.path.endswith("/state")]
            if rows:
                break
            time.sleep(0.1)
        topic = rows[0].topic.replace("/_Signal/", "/_Metric/", 1)
        first, second = reader.wake_on([topic]), reader.wake_on([topic])
        seen = first.bell.generation, second.bell.generation
        machine.publish("state", 4)
        assert first.bell.wait_after(seen[0], timeout=10)
        assert second.bell.wait_after(seen[1], timeout=10)
        first.close()
        seen = second.bell.generation
        machine.publish("state", 3)
        assert second.bell.wait_after(seen, timeout=10), "closing one consumer silenced the other"
        second.close()
