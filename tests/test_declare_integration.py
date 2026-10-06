"""Against a real node: a path declared before the service's first sample is in
its ``_DataTags`` at start, the node binds a signal to it before any value
exists, and the first ``publish()`` lands on that same signal."""

from __future__ import annotations

import os
import time

import pytest
from test_connector_refusals_integration import _wait_for_values

import chaski
from chaski.service import LocalDoor

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)

PATH = "events/order_closed"


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door rather than the unit suite's default fake."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """The integration caller supplies the bundle matching its binary."""


def _eventually(probe, what: str, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while not (result := probe()):
        assert time.monotonic() < deadline, f"{what} not seen in time"
        time.sleep(0.1)
    return result


def _declared_tag(svc: chaski.Service) -> dict | None:
    for entry in svc.kv(contract="_DataTags"):
        if isinstance(entry.payload, dict) and entry.payload.get("connector") == svc.service_id:
            for tag in entry.payload.get("data_tags") or []:
                if tag.get("source") == PATH:
                    return tag
    return None


def _bound_signal(svc: chaski.Service, tag_id: str) -> dict | None:
    for entry in svc.kv(contract="_Signal"):
        if isinstance(entry.payload, dict) and entry.payload.get("data_tag") == tag_id:
            return entry.payload
    return None


def test_a_declared_path_is_bound_before_its_first_sample_and_the_sample_lands_on_that_signal(tmp_path):
    with chaski.Node("declare-node", data_dir=tmp_path / "node") as node:
        door = LocalDoor(host="127.0.0.1", http_port=node._ports["api_local"], mqtt_port=node._ports["mqtt_local"])
        svc = chaski.Service("events", node=door, state_dir=tmp_path / "svc")
        svc.declare(PATH, data_type="string", description="an order closed")
        with svc.start():
            tag = _eventually(lambda: _declared_tag(svc), "the declared tag in _DataTags")
            assert (tag["data_type"], tag["is_stale"]) == ("string", False)
            assert tag["meta"].get("description") == "an order closed"

            signal = _eventually(lambda: _bound_signal(svc, tag["id"]), "a signal bound to the declared tag")
            assert signal["data_type"] == "string"
            assert signal.get("description") == "an order closed"

            svc.publish(PATH, '{"order": 1}')

            _wait_for_values(svc, signal["id"], ['{"order": 1}'], cursor="declare-test")
            assert _declared_tag(svc)["id"] == tag["id"], "the publish reused the declared tag"
