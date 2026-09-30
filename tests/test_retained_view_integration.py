"""Real broker snapshot, notification, cursor and restart boundary."""

import os
import time

import pytest

import chaski


@pytest.fixture(autouse=True)
def _no_node_door():
    pass


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    pass


def test_retained_view_follows_real_metrics_and_rebuilds_after_restart(tmp_path):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires real colcad binary and contracts bundle")
    from chaski import Node

    with Node("retained-view", data_dir=tmp_path / "node") as node, node.service("worker") as service:
        current = service.retained_view(
            contracts=["_Metric"], streams=["metrics"], cursor="view", scope=chaski.ViewScope.whole_node()
        )

        def observed(value):
            deadline = time.monotonic() + 10
            while True:
                version = current.changes.version
                rows = current.read()
                if any(e.path.split("/")[-1] == "temperature" and e.payload.get("value") == value for e in rows):
                    return
                remaining = deadline - time.monotonic()
                assert remaining > 0, [(e.path, e.payload) for e in rows]
                current.changes.wait(version, remaining)

        service.publish("temperature", 21)
        observed(21)
        service.publish("temperature", 22)
        observed(22)
        current.close()
        current = service.retained_view(
            contracts=["_Metric"], streams=["metrics"], cursor="view", scope=chaski.ViewScope.whole_node()
        )
        observed(22)
