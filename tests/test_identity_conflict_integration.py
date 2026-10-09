"""Two processes running as one service against a real node: the broker hands
their one MQTT session back and forth (MQTT 5 DISCONNECT 0x8E), and both say
so instead of flapping silently."""

import logging
import os
import time

import pytest


@pytest.fixture(autouse=True)
def _no_node_door():
    pass


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    pass


def test_a_second_process_on_one_identity_is_an_error_and_unhealthy(tmp_path, caplog, monkeypatch):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires real colcad binary and contracts bundle")
    from chaski import Node
    from chaski.telemetry import ServiceTelemetry

    class Capture(ServiceTelemetry):
        def __init__(self):
            self.healthy = []

        def service_health(self, healthy):
            self.healthy.append(healthy)

    monkeypatch.setattr("chaski.service.ServiceTelemetry", Capture)

    with Node("identity", data_dir=tmp_path / "node") as node:
        first = node.service("worker")
        assert first.telemetry.healthy[-1] is True
        second = None
        try:
            with caplog.at_level(logging.ERROR, logger="chaski.service"):
                second = node.service("worker")
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline and not (
                    first.identity_conflict
                    and second.identity_conflict
                    and first.telemetry.healthy[-1:] == [False]
                    and second.telemetry.healthy[-1:] == [False]
                ):
                    time.sleep(0.1)
            assert "another process" in first.identity_conflict
            assert "another process" in second.identity_conflict
            assert first.telemetry.healthy[-1] is False
            assert second.telemetry.healthy[-1] is False
            errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
            assert any("another process is running as worker" in message for message in errors), errors

            # Registration remains discoverable; the runtime failure belongs
            # to telemetry and the diagnostic log, not retained entity state.
            registrations = [
                row for row in first.kv("", contract="_ServiceDetails") if row.path.endswith("worker/_service")
            ]
            assert len(registrations) == 1
            metadata = registrations[0].payload.get("architecture_metadata") or {}
            assert not {"status", "detail", "metrics", "application_clock"} & metadata.keys()
        finally:
            if second is not None:
                second.close()
            first.close()
