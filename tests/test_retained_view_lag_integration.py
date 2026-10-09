"""Against a real node: a retained view reads only its contracts, so records of
other contracts on the same stream never count as unread on its cursor."""

import json
import os
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


def _wait(predicate, timeout: float = 20.0):
    deadline = time.monotonic() + timeout
    while not (result := predicate()):
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.5)
    return result


def test_other_contracts_on_the_stream_are_not_unread_for_the_view(tmp_path, fast_lag_alarm):
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with chaski.Node("view-lag", data_dir=tmp_path / "node") as node, node.service("worker") as svc:
        view = svc.retained_view(
            contracts=["_Signal"], streams=["entities"], cursor="view", scope=chaski.ViewScope.whole_node()
        )
        view.read()

        # A consumer of every entities record, drained to the head: the control.
        control = svc.stream("entities", cursor="control")
        list(control)

        # An indexed record wakes the view, which drains it.
        svc.publish("temperature", 21)
        _wait(lambda: any(e.path.split("/")[-1] == "temperature" for e in view.read()))

        # Records of another contract follow; nothing wakes the view for them.
        base = f"{topic_prefix()}_Finding/{svc.node_id}/{'/'.join(svc._hierarchy)}"
        for i in range(20):
            finding = {"reason": "test", "summary": str(i), "observed_at": time.time(), "suggested_severity": "info"}
            svc.send(f"{base}/other-{i}", json.dumps(finding), retain=True)

        def lagging():
            for entry in svc.kv(contract="_Finding"):
                if entry.path.endswith("cursor_lag"):
                    names = {c["cursor"] for c in entry.payload["detail"]["cursors"]}
                    if control.cursor in names:
                        return names
            return None

        names = _wait(lagging)
        assert view.cursor not in names, names
        view.close()


def test_a_restarted_view_replaces_the_filter_the_node_remembers_for_its_cursor(tmp_path, fast_lag_alarm):
    """The node keeps a cursor's last fetch filter until the cursor fetches
    again. A view that starts at the head must still fetch with its contracts,
    or an earlier unfiltered read keeps counting other records as unread."""
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with chaski.Node("view-restart", data_dir=tmp_path / "node") as node, node.service("worker") as svc:
        # An earlier consumer on the same cursor read every record.
        list(svc.stream("entities", cursor="view"))
        view = svc.retained_view(
            contracts=["_Signal"], streams=["entities"], cursor="view", scope=chaski.ViewScope.whole_node()
        )
        view.read()
        control = svc.stream("entities", cursor="control")
        list(control)
        # The view's subscription is up and its first hint was handled, so no
        # later wake-up refreshes it.
        entities = view.watch["entities"]
        _wait(
            lambda: (
                view.watch.connected and entities.version and view._stream_versions.get("entities") == entities.version
            )
        )

        base = f"{topic_prefix()}_Finding/{svc.node_id}/{'/'.join(svc._hierarchy)}"
        for i in range(20):
            finding = {"reason": "test", "summary": str(i), "observed_at": time.time(), "suggested_severity": "info"}
            svc.send(f"{base}/other-{i}", json.dumps(finding), retain=True)

        def lagging():
            for entry in svc.kv(contract="_Finding"):
                if entry.path.endswith("cursor_lag"):
                    names = {c["cursor"] for c in entry.payload["detail"]["cursors"]}
                    if control.cursor in names:
                        return names
            return None

        names = _wait(lagging)
        assert view.cursor not in names, names
        view.close()


@pytest.fixture
def fast_entities_retention(monkeypatch):
    """Prune the entities stream every second down to records a second old."""
    from chaski.node import Node

    original = Node._write_config

    def write_config(self):
        original(self)
        doc = self._load_existing()
        doc["retention"] = {"interval": "1s", "streams": {"entities": {"max_age": "1s"}}}
        self._config_path().write_text(json.dumps(doc), encoding="utf-8")

    monkeypatch.setattr(Node, "_write_config", write_config)


def test_records_of_other_contracts_walk_the_cursor_and_let_the_stream_be_pruned(tmp_path, fast_entities_retention):
    """Records of other contracts wake the view too. Its drain finds nothing of
    its own and acks the offset it scanned to, so the cursor follows the head:
    it shows no lag and does not hold back the pruner."""
    if not os.environ.get("COLCAD_BINARY"):
        pytest.skip("requires COLCAD_BINARY and matching COLCAD_CONTRACTS_BUNDLE")
    with chaski.Node("view-walk", data_dir=tmp_path / "node") as node, node.service("worker") as svc:
        door = svc._require_http("door")
        view = svc.retained_view(
            contracts=["_Signal"], streams=["entities"], cursor="view", scope=chaski.ViewScope.whole_node()
        )
        view.read()
        svc.publish("temperature", 21)
        _wait(lambda: any(e.path.split("/")[-1] == "temperature" for e in view.read()))

        def pruned_to():
            # A new cursor starts at the LWM without a historical gap. Supply
            # the explicit old position to observe the prefix retention cut.
            page = door.fetch("entities", "c/worker/probe", max=1, from_offset=1)
            return page.gap.to_offset if page.gap is not None else 0

        # The pruner removed everything below the view's cursor and stops
        # there; a moving low-water mark would wake the view on its own.
        position = view.position("entities")
        _wait(lambda: pruned_to() >= position, timeout=30)
        time.sleep(2.5)
        settled = pruned_to()
        assert view.position("entities") == position

        base = f"{topic_prefix()}_Finding/{svc.node_id}/{'/'.join(svc._hierarchy)}"
        for i in range(20):
            finding = {"reason": "test", "summary": str(i), "observed_at": time.time(), "suggested_severity": "info"}
            svc.send(f"{base}/other-{i}", json.dumps(finding), retain=True)
        head = _wait(lambda: (h := view.heads()["entities"]) >= settled + 20 and h)

        # No record of the view's own arrives; its cursor still reaches the head
        # and shows no lag.
        assert view.wait_caught_up({"entities": head}, timeout=20), (view.position("entities"), head)
        rows = {(row["cursor"], row["stream"]): row for row in door.backlog([view.cursor])}
        assert rows[(view.cursor, "entities")]["position"] > head, rows

        # The pruner passes the records the view skipped.
        _wait(lambda: pruned_to() >= settled + 20, timeout=30)
        view.close()
