import json
from types import SimpleNamespace

import pytest
from colca_data_contracts.payload import ClockDefinition

from chaski.clock import Clock
from chaski.coordination import StepGate

TOPIC = "colca/v1/_ServiceDetails/node/source"


def test_completion_waits_for_exact_upstream_run_and_durable_commit(tmp_path):
    now = [10001.0]
    clock = Clock(wall=lambda: now[0])
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 1000, 1010))
    rows, reports = [], []
    service = SimpleNamespace(
        clock=clock, kv=lambda *args, **kw: rows, report_progress=lambda at, **kw: reports.append(at)
    )
    gate = StepGate(service, [TOPIC], tmp_path / "progress.json")
    assert gate.ready() is None
    payload = {
        "is_active": True,
        "metadata": {
            "application_clock": {"run_id": "wrong", "ready": True, "processed_at": 1010, "observed_at": 10001}
        },
    }
    gate.observe(SimpleNamespace(topic=TOPIC, payload=payload))
    assert gate.ready() is None
    payload["metadata"]["application_clock"]["run_id"] = "run"
    # The priority status lane may arrive before the sample lane.
    assert gate.ready() is None
    gate.observe(
        SimpleNamespace(
            topic=TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/"),
            payload={"run_id": "run", "processed_at": 1010},
        )
    )
    assert gate.ready() == 1010
    payload["is_active"] = False
    assert gate.ready() is None
    payload["is_active"] = True
    now[0] += 16
    assert gate.ready() is None
    now[0] -= 16
    with pytest.raises(ValueError, match="boundary"):
        gate.complete(1009)
    gate.complete(1010)
    assert json.loads((tmp_path / "progress.json").read_text())["completed_at"] == 1010
    restarted = StepGate(service, [TOPIC], tmp_path / "progress.json")
    assert restarted.ready() is None
    assert reports == [1010, 1010]


def test_moving_clock_does_not_expose_a_partial_window(tmp_path):
    clock = Clock(wall=lambda: 10000.005)
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 1000, 1010))
    service = SimpleNamespace(clock=clock)
    assert StepGate(service, [], tmp_path / "progress.json").boundary() is None
