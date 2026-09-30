"""Against a real node: a scoped retained view holds only its paths, and records
outside them never count as unread on its cursor."""

import os
import time

import pytest

import chaski


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door rather than the unit suite's default fake."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """The integration caller supplies the bundle matching its binary."""


def _wait(predicate, timeout: float = 20.0):
    deadline = time.monotonic() + timeout
    while not (result := predicate()):
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.2)
    return result


def test_a_scoped_view_reads_and_follows_only_its_paths(tmp_path, fast_lag_alarm):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with chaski.Node("view-scope", data_dir=tmp_path / "node") as node, node.service("worker") as svc:
        svc.publish("line1/before", 1)
        svc.publish("line10/before", 1)
        svc.publish("line2/before", 1)
        _wait(lambda: len(svc.kv(contract="_Signal")) >= 3)

        view = svc.retained_view(
            contracts=["_Signal"], streams=["entities"], cursor="view", scope=chaski.ViewScope(["line1"])
        )
        # The snapshot holds line1 only: not line10, whose path starts alike.
        assert {e.path for e in view.read()} == {"line1/before"}

        control = svc.stream("entities", cursor="control")
        list(control)

        # Out of scope, on the same stream.
        for i in range(20):
            svc.publish(f"line2/sig-{i}", i)
        # In scope: followed from the stream.
        svc.publish("line1/after", 2)
        _wait(lambda: {e.path for e in view.read()} == {"line1/before", "line1/after"})

        def lagging():
            for entry in svc.kv(contract="_Finding"):
                if entry.path.endswith("cursor_lag"):
                    names = {c["cursor"] for c in entry.payload["detail"]["cursors"]}
                    if control.cursor in names:
                        return names
            return None

        names = _wait(lagging)
        assert view.cursor not in names, names
        assert {e.path for e in view.read()} == {"line1/before", "line1/after"}
        view.close()
