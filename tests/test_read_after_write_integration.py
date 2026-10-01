"""Against a real node: a read that waits for the head it captured sees every
record admitted before it, through a retained view and through a consumer."""

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


def _admitted_after(head_of, before: int, timeout: float = 20.0) -> int:
    """The head once a record published over MQTT was admitted after ``before``."""
    deadline = time.monotonic() + timeout
    while (head := head_of()) <= before:
        assert time.monotonic() < deadline, "the record was not admitted in time"
        time.sleep(0.02)
    return head


def test_a_retained_view_read_after_wait_caught_up_holds_the_captured_head(tmp_path):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with chaski.Node("view-position", data_dir=tmp_path / "node") as node, node.service("worker") as service:
        view = service.retained_view(
            contracts=["_Metric"], streams=["metrics"], cursor="view", scope=chaski.ViewScope.whole_node()
        )
        view.synchronize()
        for value in (21, 22, 23):
            before = view.heads()["metrics"]
            service.publish("temperature", value)
            head = _admitted_after(lambda: view.heads()["metrics"], before)
            assert view.wait_caught_up({"metrics": head}, timeout=10)
            assert view.position() >= head
            temperature = [e.payload["value"] for e in view.read() if e.path.split("/")[-1] == "temperature"]
            assert temperature == [value]
        assert view.wait_caught_up(timeout=10), "the current heads were applied"
        view.close()
        assert not view.wait_caught_up(view.position() + 1, timeout=10), "a closed view stops waiting"


def test_a_consumer_stream_reports_how_far_it_handled(tmp_path):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with chaski.Node("stream-position", data_dir=tmp_path / "node") as node, node.service("worker") as svc:
        base = f"{topic_prefix()}_Finding/{svc.node_id}/{'/'.join(svc._hierarchy)}"
        handled: list[str] = []

        def handle(record):
            if "/case-" in record.topic:
                handled.append(record.topic.rsplit("/case-", 1)[1])

        # Rung by the node's stream-change hints; also rung to end a consumer.
        bell = svc.watch_streams("entities")["entities"]

        def run(stream):
            stop = threading.Event()
            worker = threading.Thread(target=svc.consume, args=(stream, handle), kwargs={"stop": stop, "bell": bell})
            worker.start()
            return stop, worker

        def end(stop, worker):
            stop.set()
            bell.ring()
            worker.join(timeout=20)
            assert not worker.is_alive()

        stream = svc.stream("entities", cursor="position-test", contracts=["_Finding"])
        stop, worker = run(stream)
        try:
            for name in ("a", "b"):
                before = stream.head()
                finding = {"reason": "test", "summary": name, "observed_at": time.time(), "suggested_severity": "info"}
                svc.send(f"{base}/case-{name}", json.dumps(finding), retain=True)
                head = _admitted_after(stream.head, before)
                assert stream.wait_caught_up(head, timeout=10)
                assert handled[-1] == name
        finally:
            end(stop, worker)

        # A restarted consumer whose cursor already stands at the head knows it.
        restarted = svc.stream("entities", cursor="position-test", contracts=["_Finding"])
        head = restarted.head()
        assert restarted.position == 0
        stop, worker = run(restarted)
        try:
            assert restarted.wait_caught_up(head, timeout=10)
        finally:
            end(stop, worker)
        assert handled == ["a", "b"]
