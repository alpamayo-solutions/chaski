"""Real MQTT reconnect with the HTTP registration door temporarily unavailable."""

import os
import socket
import threading
import time

import pytest


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door rather than the unit suite's default fake."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """The integration caller supplies the bundle matching its binary."""


def test_registration_retry_restores_clock_after_broker_reconnect(tmp_path, monkeypatch):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    import chaski.service as module
    from chaski import Node
    from chaski.clock import Clock

    disconnected, recovered = threading.Event(), threading.Event()
    with (
        Node("registration-recovery", data_dir=tmp_path / "node") as node,
        node.service("worker", clock=Clock(source="mqtt")) as service,
    ):
        service._broker_state_changed = lambda connected: None if connected else disconnected.set()
        original_resolve = module.resolve_local_identity
        original_reannounce = service._reannounce
        attempts = []

        def resolve(*args, **kwargs):
            attempts.append(1)
            if len(attempts) == 1:
                raise ConnectionError("HTTP listener not ready after MQTT CONNACK")
            return original_resolve(*args, **kwargs)

        def reannounce(client, fresh=False):
            original_reannounce(client, fresh)
            recovered.set()

        monkeypatch.setattr(module, "resolve_local_identity", resolve)
        monkeypatch.setattr(service, "_reannounce", reannounce)
        # Drop the real MQTT socket. Node.start() intentionally allocates
        # new ports, so restarting the test Node would change the endpoint.
        service._started_client.socket().shutdown(socket.SHUT_RDWR)
        assert disconnected.wait(5)
        assert recovered.wait(10), "MQTT recovered but registration was never retried"
        assert len(attempts) >= 2
        deadline = time.monotonic() + 10
        while True:
            version = service.clock.changes.version
            if service.clock.status().ready:
                break
            remaining = deadline - time.monotonic()
            assert remaining > 0, "registration recovered but clock subscription did not"
            service.clock.changes.wait(version, remaining)
        assert abs(service.clock.real_now() - time.time()) < 2
