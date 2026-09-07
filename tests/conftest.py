from __future__ import annotations

import pytest

from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(autouse=True)
def _contracts_bundle_env(tmp_path_factory, monkeypatch):
    """Every Node config needs a contracts bundle (node.py
    _resolve_contracts_bundle fails loudly without one); tests that are not
    about the bundle get a placeholder file through the env override."""
    bundle = tmp_path_factory.mktemp("bundle") / "contracts-bundle.json"
    bundle.write_text("{}")
    monkeypatch.setenv("COLCAD_CONTRACTS_BUNDLE", str(bundle))


class _NoNodeDoor:
    """The door of a node that holds nothing: ``Service.start()`` reads the
    previous catalogue through it and finds none. Hermetic Service tests
    never reach a real ``/kv``; a test about the consume lane installs its
    own fake with a cursor table (``test_service_consume.py``) on top of
    this one."""

    def __init__(self, base_url: str, service: str, *, timeout: float = 10.0, cert=None) -> None:
        self.base_url = base_url
        self.service = service
        self.cert = cert

    def close(self) -> None:
        pass

    def kv(self, prefix: str = "", *, contract=None):  # noqa: ARG002
        return []


@pytest.fixture(autouse=True)
def _no_node_door(monkeypatch):
    monkeypatch.setattr("chaski.service.Door", _NoNodeDoor)
