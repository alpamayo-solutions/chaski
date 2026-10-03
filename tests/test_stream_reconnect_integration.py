"""Against a real node that stops and comes back: a retained view is available
again within about a second of colcad answering, because the service's MQTT
link coming back reconnects the view's stream subscription at once instead of
after its own backoff."""

from __future__ import annotations

import os
import time

import pytest
from test_start_without_colca_integration import (  # noqa: F401  # fixtures
    _contracts_bundle_env,
    _no_node_door,
    fixed_local_doors,
)

import chaski

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)

#: Long enough that every backoff (stream subscription, view, MQTT) grew past
#: its first steps: 1, 2, 4 s and more.
OUTAGE_S = 12.0


def _until(predicate, timeout: float) -> float | None:
    """Monotonic time ``predicate`` first held, or None after ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return time.monotonic()
        time.sleep(0.01)
    return None


def test_a_view_is_available_again_within_a_second_of_colcad_answering(tmp_path, fixed_local_doors):  # noqa: F811
    node = chaski.Node("reconnect", data_dir=tmp_path / "node")
    node.start()
    try:
        with node.service("worker") as service:
            view = service.retained_view(
                contracts=["_Metric"], streams=["metrics"], cursor="view", scope=chaski.ViewScope.whole_node()
            )
            service.publish("temperature", 21)
            assert _until(lambda: view.available and view.read(), 10), "the view never became available"

            node.stop()
            assert _until(lambda: not view.available, 10), "the view did not notice the node going away"
            time.sleep(OUTAGE_S)
            node.start()  # returns once colcad answers its health check
            answered = time.monotonic()

            linked = _until(service.is_broker_connected, 10)
            available = _until(lambda: view.available, 10)
            assert linked is not None and available is not None, (linked, available)
            # The MQTT link is the push event; the view follows it at once.
            assert available - linked < 1.0, (linked - answered, available - answered)
            assert available - answered < 1.5, (linked - answered, available - answered)

            service.publish("temperature", 22)
            assert _until(lambda: any(e.payload.get("value") == 22 for e in view.read()), 10), (
                "the view stopped following the stream"
            )
            view.close()
    finally:
        node.stop()
