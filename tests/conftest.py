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

    def close(self) -> None:
        pass

    def kv(self, prefix: str = "", *, contract=None):
        return []


@pytest.fixture(autouse=True)
def _no_node_door(monkeypatch):
    monkeypatch.setattr("chaski.service.Door", _NoNodeDoor)
