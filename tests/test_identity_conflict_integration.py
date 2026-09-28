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


def test_a_second_process_on_one_identity_is_an_error_and_unhealthy(tmp_path, caplog):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires real colcad binary and contracts bundle")
    from chaski import Node

    with Node("identity", data_dir=tmp_path / "node") as node:
        first = node.service("worker")
        second = None
        try:
            with caplog.at_level(logging.ERROR, logger="chaski.service"):
                second = node.service("worker")
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline and not (first.identity_conflict and second.identity_conflict):
                    time.sleep(0.1)
            assert "another process" in first.identity_conflict
            assert "another process" in second.identity_conflict
            errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
            assert any("another process is running as worker" in message for message in errors), errors

            # The node's record of the service carries it too.
            detail = ""
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                for row in first.kv("", contract="_ServiceDetails"):
                    if row.path.endswith("worker/_service"):
                        meta = row.payload.get("architecture_metadata") or {}
                        if meta.get("status") == "unhealthy":
                            detail = meta.get("detail", "")
                if "another process" in detail:
                    break
                time.sleep(0.2)
            assert "another process" in detail
        finally:
            if second is not None:
                second.close()
            first.close()
