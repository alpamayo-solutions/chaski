"""Against a real node: a failed handler leaves its record unacknowledged, a
restart resumes at it, and a rejection is recorded before the cursor passes."""

import json
import os
import threading
import time

import pytest
from colca_data_contracts.root import topic_prefix

import chaski


@pytest.fixture(autouse=True)
def _no_node_door():
    """Use the real HTTP door rather than the unit suite's default fake."""


@pytest.fixture(autouse=True)
def _contracts_bundle_env():
    """The integration caller supplies the bundle matching its binary."""


def _wait(predicate, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.05)


def _run(svc, stream, handler, stop, bell):
    worker = threading.Thread(target=svc.consume, args=(stream, handler), kwargs={"bell": bell, "stop": stop})
    worker.start()
    return worker


def _stop(worker, stop, bell):
    stop.set()
    bell.ring()
    worker.join(timeout=10)
    assert not worker.is_alive()


def test_failed_record_is_not_acked_restart_resumes_there_and_rejection_is_durable(tmp_path):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with chaski.Node("consume-recovery", data_dir=tmp_path / "node") as node, node.service("worker") as svc:
        base = f"{topic_prefix()}_Finding/{svc.node_id}/{'/'.join(svc._hierarchy)}"
        for name in ("a", "b", "c"):
            finding = {"reason": "test", "summary": name, "observed_at": time.time(), "suggested_severity": "info"}
            svc.send(f"{base}/case-{name}", json.dumps(finding), retain=True)

        def name_of(record):
            last = record.topic.rsplit("/", 1)[-1]
            return last[5:] if last.startswith("case-") else None

        # Round 1: "b" keeps failing.
        handled: list[str] = []

        def failing(record):
            name = name_of(record)
            if name == "b":
                raise RuntimeError("b cannot be stored yet")
            if name:
                handled.append(name)

        stream = svc.stream("entities", cursor="consume-test", contracts=["_Finding"])
        stop, bell = threading.Event(), chaski.Doorbell()
        worker = _run(svc, stream, failing, stop, bell)
        try:
            _wait(lambda: svc.handler_health.failing().get(stream.cursor, None) is not None)
            _wait(lambda: svc.handler_health.failing()[stream.cursor].failures >= 2)
            assert svc.handler_health.status == "degraded"
        finally:
            _stop(worker, stop, bell)
        assert handled == ["a"]

        # The node's cursor stands right before "b": a fresh reader starts there.
        page = svc.stream("entities", cursor="consume-test", contracts=["_Finding"]).fetch()
        names = [n for n in map(name_of, page.records) if n]
        assert names[:1] == ["b"], names
        assert "a" not in names

        # Round 2, after a restart: "b" works now, "c" is poison.
        def fixed(record):
            name = name_of(record)
            if name == "c":
                raise chaski.Reject("c does not decode", detail={"why": "test"})
            if name:
                handled.append(name)

        stop, bell = threading.Event(), chaski.Doorbell()
        worker = _run(svc, svc.stream("entities", cursor="consume-test", contracts=["_Finding"]), fixed, stop, bell)

        def unread():
            page = svc.stream("entities", cursor="consume-test", contracts=["_Finding"]).fetch()
            return [n for n in map(name_of, page.records) if n]

        try:
            _wait(lambda: handled == ["a", "b"])
            _wait(lambda: any(e.path.endswith("rejected_input") for e in svc.kv(contract="_Finding")))
            # The page is acked after its rejection was recorded; stopping
            # before that leaves it for the next start, by design.
            _wait(lambda: not unread())
        finally:
            _stop(worker, stop, bell)
        assert svc.handler_health.status == "ok"

        rejection = next(e for e in svc.kv(contract="_Finding") if e.path.endswith("rejected_input"))
        assert rejection.payload["reason"] == "rejected_input"
        assert rejection.payload["detail"]["topic"] == f"{base}/case-c"
        assert rejection.payload["detail"]["reject"] == {"why": "test"}

        assert not unread(), "c was acknowledged after its rejection"

        svc.clear_rejections()
        _wait(lambda: not any(e.path.endswith("rejected_input") for e in svc.kv(contract="_Finding")))


def test_a_consumer_rescoped_before_its_signals_exist_stops_and_restarts_on_them(tmp_path):
    # The rig's HMI API saw its machine element before the element's signals:
    # its wake-up subscribed no topic. When the signals came, the view set
    # stop to rebind, but consume kept waiting on a bell nothing could ring
    # and the API never read a value until it was restarted.
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with (
        chaski.Node("consume-rescope", data_dir=tmp_path / "node") as node,
        node.service("machine", mount="Line/M1") as machine,
        node.service("view") as view,
    ):
        machine.publish("state", 7)
        _wait(lambda: any(r.path.endswith("/state") for r in view.kv("", contract="_Signal")))
        signal = next(r for r in view.kv("", contract="_Signal") if r.path.endswith("/state"))
        # The metrics stream is not empty, so a drain of any scope ends at a head > 0.
        probe = view.stream("metrics", cursor="probe", signal_ids=[signal.payload["id"]])
        _wait(lambda: bool(probe.fetch().records))

        # The view does not know the signal yet: no topic, a scope nothing matches.
        wake = view.wake_on([])
        stream = view.stream("metrics", cursor="rescope", signal_ids=["not-yet-known"])
        stop = threading.Event()
        worker = threading.Thread(
            target=view.consume,
            args=(stream, lambda _record: None),
            kwargs={"bell": wake.bell, "stop": stop},
            daemon=True,  # on main it never returns; let the failure report
        )
        worker.start()
        _wait(lambda: stream.position > 0)  # drained to the head; now waiting on the bell
        stop.set()  # rescope: no ring
        worker.join(timeout=10)
        assert not worker.is_alive(), "consume did not notice stop while its bell had no topic"

        # The rebound consumer reads the signal it now knows.
        wake.rebind([signal.topic.replace("/_Signal/", "/_Metric/", 1)])
        values: list[float] = []
        stream = view.stream("metrics", cursor="rescope-2", signal_ids=[signal.payload["id"]])
        stop = threading.Event()
        worker = threading.Thread(
            target=view.consume,
            args=(stream, lambda record: values.append(record.payload["value"])),
            kwargs={"bell": wake.bell, "stop": stop},
        )
        worker.start()
        try:
            _wait(lambda: 7 in values)
            machine.publish("state", 8)
            _wait(lambda: 8 in values)
        finally:
            stop.set()
            worker.join(timeout=10)
        assert not worker.is_alive()
        wake.close()
