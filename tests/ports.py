"""Ports for test listeners while the suite runs in parallel (pytest-xdist).

A listener a test starts itself binds port 0 and the test reads the port back.
This module is for the rest: a node's door that services must be pointed at
before the node is up.
"""

from __future__ import annotations

import itertools
import os
import socket

# Below the ephemeral ranges of Linux (32768+) and macOS (49152+), so nothing
# that binds port 0 meanwhile can take a port handed out here.
_FIRST = 20000
_PER_WORKER = 500
_handed_out = itertools.count()


def _worker() -> int:
    return int(os.environ.get("PYTEST_XDIST_WORKER", "gw0").removeprefix("gw") or 0)


def reserved_port() -> int:
    """A free port in a block of its own per xdist worker, never the same
    twice in one worker, so parallel tests cannot pick the same one."""
    first = _FIRST + _worker() * _PER_WORKER
    for _ in range(_PER_WORKER):
        port = first + next(_handed_out) % _PER_WORKER
        with socket.socket() as probe:
            # As the listeners bind: a port in TIME_WAIT from an earlier test is usable.
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
        return port
    raise RuntimeError(f"no free port in {first}..{first + _PER_WORKER - 1}")
