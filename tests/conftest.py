from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(autouse=True)
def _contracts_bundle_env(tmp_path_factory, monkeypatch):
    """Node configs need a contracts bundle; tests not about the bundle get a
    placeholder through the env override."""
    bundle = tmp_path_factory.mktemp("bundle") / "contracts-bundle.json"
    bundle.write_text("{}")
    monkeypatch.setenv("COLCAD_CONTRACTS_BUNDLE", str(bundle))


class _NoNodeDoor:
    """The door of a node that holds nothing, so ``Service.start()`` finds no
    previous catalogue. Consume-lane tests install their own fake on top."""

    def __init__(self, base_url: str, service: str, *, timeout: float = 10.0, cert=None) -> None:
        self.base_url = base_url
        self.service = service
        self.cert = cert
        from chaski.doorbell import Doorbell

        self.link_up = Doorbell()

    def close(self) -> None:
        pass

    def kv(self, prefix: str = "", *, contract=None):
        return []


@pytest.fixture(autouse=True)
def _no_node_door(monkeypatch):
    monkeypatch.setattr("chaski.service.Door", _NoNodeDoor)


@pytest.fixture
def fast_lag_alarm(monkeypatch):
    """Let an embedded node's cursor watchdog report unread records after one second."""
    import json

    from chaski.node import Node

    original = Node._write_config

    def write_config(self):
        original(self)
        doc = self._load_existing()
        doc["cursors"] = {"lag_alarm_after": "1s"}
        self._config_path().write_text(json.dumps(doc), encoding="utf-8")

    monkeypatch.setattr(Node, "_write_config", write_config)
