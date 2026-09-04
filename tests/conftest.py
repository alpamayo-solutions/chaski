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
