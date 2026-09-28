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

    with Node("scoped", data_dir=tmp_path / "node") as node, node.service("machine", mount="Line/M1") as machine, \
            node.service("reader") as reader:
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
        stream.ack(stream.head() if callable(stream.head) else stream.head)

        seen = wake.bell.generation
        for i in range(20):
            machine.publish("noise", i)
        assert not wake.bell.wait_after(seen, timeout=1.5), "woken by a signal it does not read"

        machine.publish("state", 4)
        assert wake.bell.wait_after(seen, timeout=10), "not woken by its own signal"
        page = stream.fetch()
        records = getattr(page, "records", page)
        assert [r.payload["value"] for r in records] == [4]
        wake.close()
