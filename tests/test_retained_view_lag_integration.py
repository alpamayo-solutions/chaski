"""Against a real node: a retained view reads only its contracts, so records of
other contracts on the same stream never count as unread on its cursor."""

import json
import os
import time

import pytest
from colca_data_contracts.root import topic_prefix

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
        time.sleep(0.5)
    return result


def test_other_contracts_on_the_stream_are_not_unread_for_the_view(tmp_path, fast_lag_alarm):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with chaski.Node("view-lag", data_dir=tmp_path / "node") as node, node.service("worker") as svc:
        view = svc.retained_view(contracts=["_Signal"], streams=["entities"], cursor="view")
        view.read()

        # A consumer of every entities record, drained to the head: the control.
        control = svc.stream("entities", cursor="control")
        list(control)

        # An indexed record wakes the view, which drains it.
        svc.publish("temperature", 21)
        _wait(lambda: any(e.path.split("/")[-1] == "temperature" for e in view.read()))

        # Records of another contract follow; nothing wakes the view for them.
        base = f"{topic_prefix()}_Finding/{svc.node_id}/{'/'.join(svc._hierarchy)}"
        for i in range(20):
            finding = {"reason": "test", "summary": str(i), "observed_at": time.time(), "suggested_severity": "info"}
            svc.send(f"{base}/other-{i}", json.dumps(finding), retain=True)

        def lagging():
            for entry in svc.kv(contract="_Finding"):
                if entry.path.endswith("cursor_lag"):
                    names = {c["cursor"] for c in entry.payload["detail"]["cursors"]}
                    if control.cursor in names:
                        return names
            return None

        names = _wait(lagging)
        assert view.cursor not in names, names
        view.close()
