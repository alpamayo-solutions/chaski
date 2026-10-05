"""Against a real node: a consumer woken by /watch stops at the hint's head
without reading the tail, does not fetch for a hint it already passed, and
drains once more after the node comes back."""

from __future__ import annotations

import os
import threading
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


def _until(predicate, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.02)


def _temperature(record, values: list) -> None:
    if record.topic.endswith("/temperature"):
        values.append(record.payload.get("value"))


class _Counting:
    """Logs the consumer's own cursor reads on a shared door."""

    def __init__(self, door, cursor: str) -> None:
        self.reads: list[str] = []
        fetch = door.fetch

        def counted(stream, cursor_name, *args, **kwargs):
            if cursor_name == cursor:
                self.reads.append("tail" if kwargs.get("tail") else "page")
            return fetch(stream, cursor_name, *args, **kwargs)

        door.fetch = counted

    def quiet(self, seconds: float) -> list[str]:
        before = len(self.reads)
        time.sleep(seconds)
        return self.reads[before:]


def test_consume_follows_hint_heads_and_drains_once_after_the_node_returns(tmp_path, fixed_local_doors):  # noqa: F811
    node = chaski.Node("stream-heads", data_dir=tmp_path / "node")
    node.start()
    try:
        with node.service("worker") as service:
            stream = service.stream("metrics", cursor="heads")
            reads = _Counting(stream._door, stream.cursor)
            values: list = []
            stop = threading.Event()
            worker = threading.Thread(
                target=service.consume,
                args=(stream, lambda record: _temperature(record, values)),
                kwargs={"stop": stop, "idle_drain_s": 0.2},
            )
            worker.start()
            try:
                # The drain at start may run before the first hint: it reads the tail.
                _until(lambda: len(reads.reads) >= 1)
                _until(lambda: reads.quiet(0.5) == [])
                del reads.reads[:]
                for value in (1, 2, 3):
                    service.publish("temperature", value)
                _until(lambda: values[-1:] == [3])
                _until(lambda: reads.quiet(0.5) == [])
                assert "tail" not in reads.reads

                # Idle drains on a hint the cursor passed read nothing.
                assert reads.quiet(1.0) == []

                service.publish("temperature", 4)
                _until(lambda: values[-1:] == [4])
                assert "tail" not in reads.reads

                # The node goes away and comes back: one drain from the cursor.
                settled = len(reads.reads)
                node.stop()
                time.sleep(1.0)
                node.start()
                _until(lambda: len(reads.reads) > settled)
                _until(lambda: reads.quiet(0.5) == [])
                assert values == [1, 2, 3, 4]

                service.publish("temperature", 5)
                _until(lambda: values[-1:] == [5])
                assert "tail" not in reads.reads
            finally:
                stop.set()
                worker.join(10)
                assert not worker.is_alive()
    finally:
        node.stop()
