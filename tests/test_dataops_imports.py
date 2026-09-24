"""Importing chaski and chaski.dataops must not load pandas or numpy."""

from __future__ import annotations

import subprocess
import sys


def test_import_does_not_load_pandas_or_numpy():
    code = (
        "import sys, chaski, chaski.dataops, chaski.dataops.service\n"
        "print(sorted(m for m in ('pandas', 'numpy') if m in sys.modules))"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)  # noqa: S603
    assert result.stdout.strip() == "[]"
