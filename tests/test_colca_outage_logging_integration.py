"""Against a real node that stops for a while under a running DataOps service:
the outage is logged as a state (one WARNING per retry loop when it starts, one
INFO when it recovers), never as an ERROR with a traceback, and the service
works again afterwards."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import threading
import time

import pytest
from test_start_without_colca_integration import (  # noqa: F401  # fixtures
    _contracts_bundle_env,
    _free_port,
    _get,
    _isolate_registry,
    _no_node_door,
    _serving_dataops,
    _wait,
    fixed_local_doors,
)

import chaski
from chaski.dataops import Producer, SignalOutput, every, resolve

pytestmark = pytest.mark.skipif(
    not os.environ.get("COLCAD_BINARY"), reason="requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE"
)

TICKS: list[float] = []
TICKED = threading.Event()


class Ticker(Producer):
    """A timer that reads the node's definitions on every tick, as a
    producer writing an interval output does."""

    name = "ticker"
    system_element_name = "line1"
    speed = SignalOutput("speed", data_type="float", description="Line speed")

    @every(0.3)
    async def tick(self) -> None:
        resolve.output_disabled(self.runtime.door, self.speed.tag_id)
        TICKS.append(time.monotonic())
        TICKED.set()


def _healthy(port: int) -> bool:
    try:
        return _get(port, "/healthz")[0] == 200
    except OSError:
        return False


#: This service's retry loops that read the node. Threads other tests left
#: behind may log their own outages into the same capture.
LOOPS = {"ticker.tick", "Definition bindings", "Retained view c/dataops/dataops-definitions"}


def _started(records) -> set[str]:
    """This service's loops that logged an outage starting."""
    return LOOPS & {r.getMessage().split(":")[0] for r in records if "retrying until it answers" in r.getMessage()}


def _recovered(records) -> set[str]:
    """This service's loops that logged their recovery."""
    return LOOPS & {
        r.getMessage().split(":")[0]
        for r in records
        if r.levelno == logging.INFO and (" after " in r.getMessage() and "failed attempts" in r.getMessage())
    }


def _chaski(record: logging.LogRecord) -> bool:
    return record.name.startswith(("chaski", "apscheduler"))


def test_a_colca_outage_logs_one_warning_per_loop_and_no_traceback(tmp_path, fixed_local_doors, caplog):  # noqa: F811
    http_port, mqtt_port = fixed_local_doors
    door = chaski.LocalDoor(host="127.0.0.1", http_port=http_port, mqtt_port=mqtt_port)
    health_port = _free_port()
    dataops = chaski.DataOpsService(
        "dataops",
        node=door,
        state_dir=tmp_path / "dataops-state",
        data_dir=tmp_path / "dataops-data",
        health_port=health_port,
    )
    dataops.add(Ticker)
    node = chaski.Node("outage", data_dir=tmp_path / "node")
    node.start()

    with contextlib.ExitStack() as stack:
        stack.callback(node.stop)
        stack.enter_context(_serving_dataops(dataops, asyncio.Event()))
        _wait(lambda: _healthy(health_port), timeout=30, what="DataOps healthy")
        assert TICKED.wait(10), "the timer never ran"

        with caplog.at_level(logging.DEBUG):
            first = len(caplog.records)
            node.stop()
            # Several retries of every loop while the node is away.
            time.sleep(6)
            node.start()
            _wait(lambda: _healthy(health_port), timeout=60, what="DataOps healthy again")
            TICKED.clear()
            assert TICKED.wait(20), "the timer did not run again after the node came back"
            # Every loop that warned logs its recovery once it succeeds again;
            # the slowest retries after its backoff (at most 30 s).
            _wait(
                lambda: _started(caplog.records[first:]) <= _recovered(caplog.records[first:]),
                timeout=45,
                what="a recovery line for every loop that warned",
            )
            # From the node stopping until every loop recovered; teardown is not part of it.
            window = caplog.records[first:]

    records = [r for r in window if _chaski(r)]
    defects = [(r.name, r.levelname, r.getMessage()) for r in records if r.levelno >= logging.ERROR or r.exc_info]
    assert not defects, defects

    def warnings(prefix: str) -> list[str]:
        return [r.getMessage() for r in records if r.levelno == logging.WARNING and r.getMessage().startswith(prefix)]

    # The two loops that logged a traceback per retry before: one warning each.
    assert len(warnings("ticker.tick:")) == 1, warnings("ticker.tick:")
    assert len(warnings("Definition bindings:")) <= 1, warnings("Definition bindings:")
    # Every loop that warned said when it worked again, the timer included.
    assert "ticker.tick" in _started(records) <= _recovered(records), (_started(records), _recovered(records))
