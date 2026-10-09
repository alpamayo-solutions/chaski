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
    gate = StepGate(service, [TOPIC], tmp_path / "progress.json", monotonic=lambda: now[0])
    assert gate.ready() is None
    payload = {
        "is_active": True,
        "metadata": {
            "application_clock": {"run_id": "wrong", "ready": True, "processed_at": 1010, "observed_at": 10001}
        },
    }
    gate.observe(SimpleNamespace(topic=TOPIC, payload=payload))
    assert gate.ready() is None
    # Registration cannot grant completion or readiness.
    # The priority status lane may arrive before the sample lane.
    assert gate.ready() is None
    gate.observe(
        SimpleNamespace(
            topic=TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/"),
            payload={"run_id": "run", "processed_at": 1010, "ready": True, "observed_at": 10001},
        )
    )
    assert gate.ready() == 1010
    # A newer durable marker does not require republishing full service details.
    payload["metadata"]["application_clock"]["processed_at"] = 1000
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


def test_wait_ready_wakes_on_commit_without_rechecking_idle_state(tmp_path):
    import asyncio

    async def run():
        clock = Clock(wall=lambda: 10001)
        clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1010))
        service = SimpleNamespace(clock=clock, report_progress=lambda *a, **kw: None)
        gate = StepGate(service, [TOPIC], tmp_path / "progress.json")
        checks = []
        ready = gate.ready

        def counted():
            checks.append(True)
            return ready()

        gate.ready = counted
        task = asyncio.create_task(gate.wait_ready())
        await asyncio.sleep(0.04)
        assert len(checks) == 1
        gate.observe(
            SimpleNamespace(
                topic=TOPIC,
                payload={
                    "is_active": True,
                    "metadata": {"application_clock": {"ready": True, "run_id": "run", "observed_at": 10001}},
                },
            )
        )
        gate.observe(
            SimpleNamespace(
                topic=TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/"),
                payload={"run_id": "run", "processed_at": 1010, "ready": True, "observed_at": 10001},
            )
        )
        assert await asyncio.wait_for(task, 0.2) == 1010
        gate.reconnect()
        waiting = asyncio.create_task(gate.wait_ready())
        await asyncio.sleep(0.02)
        assert not waiting.done()  # retained state must be reacquired
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

    asyncio.run(run())


def test_wait_ready_schedules_boundary_without_another_message(tmp_path):
    import asyncio
    import time

    async def run():
        clock = Clock()
        clock.apply_definition(ClockDefinition("factory", "run", 1, time.time(), 1000, 100, 1010))
        service = SimpleNamespace(clock=clock, report_progress=lambda *a, **kw: None)
        gate = StepGate(service, [], tmp_path / "progress.json")
        assert await asyncio.wait_for(gate.wait_ready(), 1) == 1010

    asyncio.run(run())


def test_async_consumer_finishes_an_issued_older_window_after_clock_advances(tmp_path):
    now = [10001.0]
    clock = Clock(wall=lambda: now[0])
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1030))
    reports = []
    service = SimpleNamespace(clock=clock, report_progress=lambda at, **kw: reports.append(at))
    gate = StepGate(service, [TOPIC], tmp_path / "progress.json", asynchronous=True)
    gate.observe(
        SimpleNamespace(
            topic=TOPIC,
            payload={
                "is_active": True,
                "metadata": {"application_clock": {"ready": True, "run_id": "run", "observed_at": now[0]}},
            },
        )
    )
    gate.observe(
        SimpleNamespace(
            topic=TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/"),
            payload={"run_id": "run", "processed_at": 1010, "ready": True, "observed_at": 10001},
        )
    )
    assert gate.ready() == 1010
    clock.apply_definition(ClockDefinition("factory", "run", 2, 10001, 1030, 100, 1050))
    now[0] += 0.3
    gate.complete(1010)
    assert reports == [1010]
    with pytest.raises(ValueError, match="issued"):
        gate.complete(1040)


def test_async_gate_never_invents_a_partial_boundary_from_current_time(tmp_path):
    clock = Clock(wall=lambda: 10000.05)
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1030))
    gate = StepGate(SimpleNamespace(clock=clock), [TOPIC], tmp_path / "progress.json", asynchronous=True)
    gate.observe(
        SimpleNamespace(
            topic=TOPIC,
            payload={
                "is_active": True,
                "metadata": {"application_clock": {"ready": True, "run_id": "run", "observed_at": 10000.05}},
            },
        )
    )
    gate.observe(
        SimpleNamespace(
            topic=TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/"),
            payload={"run_id": "run", "processed_at": 1010, "ready": True, "observed_at": 10001},
        )
    )
    assert gate.ready() is None  # 1005 is not an acquired/committed boundary.


def test_local_alias_wakes_only_for_its_dependency_and_handles_retirement(tmp_path):
    clock = Clock(wall=lambda: 10001.0)
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1010))
    service = SimpleNamespace(clock=clock, node_id="node")
    gate = StepGate(service, ["./source"], tmp_path / "progress.json")
    from colca_data_contracts.root import topic_prefix

    root = topic_prefix() + "_ServiceDetails/node/"
    source, other = root + "placement/source/_service", root + "placement/other/_service"

    def progress(topic):
        gate.observe(
            SimpleNamespace(
                topic=topic.replace("/_ServiceDetails/", "/_ClockProgress/"),
                payload={"run_id": "run", "processed_at": 1010, "ready": True, "observed_at": 10001},
            )
        )

    def details(topic, name):
        gate.observe(
            SimpleNamespace(
                topic=topic,
                payload={
                    "name": name,
                    "is_active": True,
                    "metadata": {"application_clock": {"ready": True, "run_id": "run", "observed_at": 10001}},
                },
            )
        )

    version = clock.changes.version
    progress(source)  # Can precede retained service identity after reconnect.
    progress(other)
    details(other, "other")
    progress(other)
    assert clock.changes.version == version
    assert gate.ready() is None
    details(source, "source")
    assert gate.ready() == 1010
    version = clock.changes.version
    progress(source)  # QoS duplicate/heartbeat carries no new progress.
    assert clock.changes.version == version
    gate.observe(SimpleNamespace(topic=source, payload=None))
    assert gate.ready() is None
    assert clock.changes.version > version
    gate.reconnect()
    progress(source)
    assert gate.ready() is None
    details(source, "source")
    assert gate.ready() == 1010


def test_exact_dependencies_ignore_unrelated_progress(tmp_path):
    clock = Clock(wall=lambda: 10001.0)
    gate = StepGate(SimpleNamespace(clock=clock), [TOPIC], tmp_path / "progress.json")
    version = clock.changes.version
    gate.observe(
        SimpleNamespace(
            topic=TOPIC.replace("source", "other").replace("/_ServiceDetails/", "/_ClockProgress/"),
            payload={"run_id": "run", "processed_at": 1010, "ready": True, "observed_at": 10001},
        )
    )
    assert clock.changes.version == version


def test_progress_snapshot_does_not_copy_catalogues_and_is_detached(tmp_path):
    class Catalogue:
        def __deepcopy__(self, memo):
            raise AssertionError("The progress hot path must not traverse catalogues")

    clock = Clock(wall=lambda: 10001.0)
    gate = StepGate(SimpleNamespace(clock=clock), [TOPIC], tmp_path / "progress.json")
    gate.observe(
        SimpleNamespace(
            topic=TOPIC,
            payload={
                "id": "source",
                "name": "source",
                "is_active": True,
                "metadata": {
                    "catalogue": Catalogue(),
                    "application_clock": {"run_id": "run", "observed_at": 10001, "ready": True},
                },
            },
        )
    )
    gate.observe(
        SimpleNamespace(
            topic=TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/"),
            payload={"run_id": "run", "processed_at": 1010, "ready": True, "observed_at": 10001},
        )
    )
    snapshot = gate.records(progress_only=True)
    assert snapshot[TOPIC]["clock_progress"]["processed_at"] == 1010
    snapshot[TOPIC]["clock_progress"]["ready"] = False
    assert gate.records(progress_only=True)[TOPIC]["clock_progress"]["ready"]


def test_health_uses_control_receipt_age_and_rejects_retained_and_duplicates(tmp_path):
    received = [10.0]
    clock = Clock(wall=lambda: 10001)
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1010))
    gate = StepGate(SimpleNamespace(clock=clock), [TOPIC], tmp_path / "progress.json", monotonic=lambda: received[0])
    gate.observe(SimpleNamespace(topic=TOPIC, payload={"is_active": True}, retain=True))
    topic = TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/")
    marker = {"run_id": "run", "ready": True, "observed_at": 900000, "processed_at": 1010}
    gate.observe(SimpleNamespace(topic=topic, payload=marker, retain=True))
    assert gate.ready() is None
    gate.observe(SimpleNamespace(topic=topic, payload=marker, retain=False))
    assert gate.ready() == 1010  # Receipt age tolerates producer wall-clock skew.
    received[0] += 16
    gate.observe(SimpleNamespace(topic=topic, payload=marker, retain=False))
    assert gate.ready() is None  # Duplicate control does not renew readiness.
    marker["observed_at"] += 5
    gate.observe(SimpleNamespace(topic=topic, payload=marker, retain=False))
    assert gate.ready() == 1010
    assert gate.records(progress_only=True)[TOPIC]["clock_progress"]["age_s"] == 0
    gate.reconnect()
    gate.observe(SimpleNamespace(topic=TOPIC, payload={"is_active": True}, retain=True))
    gate.observe(SimpleNamespace(topic=topic, payload=marker, retain=True))
    assert gate.ready() is None
    marker["observed_at"] += 5
    gate.observe(SimpleNamespace(topic=topic, payload=marker, retain=False))
    assert gate.ready() == 1010


def test_registration_runtime_metadata_cannot_grant_readiness(tmp_path):
    clock = Clock(wall=lambda: 10001)
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1010))
    gate = StepGate(SimpleNamespace(clock=clock), [TOPIC], tmp_path / "progress.json")
    gate.observe(
        SimpleNamespace(
            topic=TOPIC,
            payload={
                "is_active": True,
                "metadata": {
                    "application_clock": {"run_id": "run", "ready": True, "processed_at": 1010, "observed_at": 10001}
                },
            },
        )
    )
    gate.observe(
        SimpleNamespace(
            topic=TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/"),
            payload={"run_id": "run", "processed_at": 1010},
        )
    )
    assert gate.ready() is None


def test_live_replay_of_retained_control_wakes_readiness_waiter(tmp_path):
    clock = Clock(wall=lambda: 10001)
    clock.apply_definition(ClockDefinition("factory", "run", 1, 10000, 1000, 100, 1010))
    gate = StepGate(SimpleNamespace(clock=clock), [TOPIC], tmp_path / "progress.json")
    gate.observe(SimpleNamespace(topic=TOPIC, payload={"is_active": True}, retain=True))
    marker = {"run_id": "run", "processed_at": 1010, "ready": True, "observed_at": 10001}
    topic = TOPIC.replace("/_ServiceDetails/", "/_ClockProgress/")
    gate.observe(SimpleNamespace(topic=topic, payload=marker, retain=True))
    before = clock.changes.version
    gate.observe(SimpleNamespace(topic=topic, payload=marker, retain=False))
    assert clock.changes.version > before
    assert gate.ready() == 1010
