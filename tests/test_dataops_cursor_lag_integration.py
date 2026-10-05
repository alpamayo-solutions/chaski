"""Against a real node: records a DataOps service does not read never count as
unread on its cursors, so the node raises no cursor_lag for it, also when the
node still remembers an unfiltered read of a cursor from an earlier run."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import threading
import time

import pytest
from colca_data_contracts.root import topic_prefix

import chaski
from chaski.dataops import Command, Producer, SignalOutput, on_command
from chaski.service import LocalDoor

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door rather than the unit suite's default fake."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """The integration caller supplies the bundle matching its binary."""


@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(Producer._registry)
    yield
    Producer._registry.clear()
    Producer._registry.update(saved)


class Line(Producer):
    name = "line"
    system_element_name = "line1"
    speed = SignalOutput("speed", data_type="float", description="Line speed")

    @on_command("line1/line/setSpeed")
    async def set_speed(self, command: Command) -> str:
        return "ok"


def _wait(predicate, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while not (result := predicate()):
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.2)
    return result


@contextlib.contextmanager
def _serving(node, tmp_path):
    """A DataOps service on the node's local door, served on its own loop."""
    door = LocalDoor(host="127.0.0.1", http_port=node._ports["api_local"], mqtt_port=node._ports["mqtt_local"])
    svc = chaski.DataOpsService(
        "dataops",
        node=door,
        state_dir=node.data_dir / "services" / "dataops",
        data_dir=tmp_path / "data",
        health_port=0,
    )
    svc.add(Line)
    loop = asyncio.new_event_loop()
    stop = asyncio.Event()
    thread = threading.Thread(target=loop.run_until_complete, args=(svc.serve(stop),))
    thread.start()
    try:
        _wait(lambda: svc.instances)
        yield svc
    finally:
        loop.call_soon_threadsafe(stop.set)
        thread.join(timeout=20)
        svc.close()
        loop.close()


def test_foreign_entity_records_are_not_unread_for_a_dataops_service(tmp_path, fast_lag_alarm):
    with chaski.Node("dataops-lag", data_dir=tmp_path / "node") as node, node.service("other") as other:
        # A first run commissions the outputs, so the restart below finds
        # nothing new on its definition streams.
        with _serving(node, tmp_path):
            pass
        # An earlier version read the definition cursor without a filter; the
        # node remembers that until the cursor fetches again.
        with node.service("dataops") as earlier:
            for stream in ("entities", "definitions"):
                list(earlier.stream(stream, cursor="dataops-definitions"))
            list(earlier.stream("commands", cursor="commands"))

        with _serving(node, tmp_path) as svc:
            # The definition subscription is up and its first hint was handled,
            # so no later wake-up refreshes the view.
            view = svc._definition_cache.view
            entities = view.watch["entities"]
            _wait(
                lambda: (
                    view.watch.connected
                    and entities.version
                    and view._stream_versions.get("entities") == entities.version
                )
            )
            # A consumer of every entities record, drained to the head: the control.
            control = svc.stream("entities", cursor="control")
            list(control)

            base = f"{topic_prefix()}_Finding/{other.node_id}/{'/'.join(other._hierarchy) or 'other'}"
            for i in range(20):
                finding = {
                    "reason": "test",
                    "summary": str(i),
                    "observed_at": time.time(),
                    "suggested_severity": "info",
                }
                other.send(f"{base}/foreign-{i}", json.dumps(finding), retain=True)

            def lagging():
                for entry in svc.kv(contract="_Finding"):
                    if entry.path.endswith("cursor_lag") and entry.payload:
                        names = {c["cursor"] for c in entry.payload["detail"]["cursors"]}
                        if control.cursor in names:
                            return names
                return None

            names = _wait(lagging)
            assert names == {control.cursor}, names
