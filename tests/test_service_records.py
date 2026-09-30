"""Service.send, retract and command: every write goes over the MQTT session.

The fake client plays the broker and the node: it records what was sent, in
order, and answers a command on its ack topic the way the node does, raw
when the ack carries more than franzmq's ``Ack`` type (a configure ack's
``state_writes``) and decoded otherwise.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from colca_data_contracts.local_service import LocalServiceIdentity
from franzmq.data_contracts.base import Ack

from chaski import CommandSender
from chaski.service import Service

NODE = "n-edge1"


class _ReasonCode:
    is_failure = False


class _Message:
    def __init__(self, payload: object) -> None:
        self.payload = payload


class _Client:
    def __init__(self, *, answer: str | None = "raw") -> None:
        # (kind, topic, payload, retain) in the order the session sent them.
        self.sent: list[tuple[str, str, object, bool]] = []
        self.callbacks: dict[str, object] = {}
        self.answer = answer
        self.state_writes = [{"stream": "state", "offset": 7, "topic": "t"}]
        self.on_connect = None

    def loop_start(self) -> None:
        self.on_connect(self, None, None, _ReasonCode())

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def _handle_on_message(self, message) -> None:
        pass

    def message_callback_add(self, topic: str, callback) -> None:
        self.callbacks[topic] = callback

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:
        self.sent.append(("subscribe", str(topic), None, False))

    def unsubscribe(self, topic) -> None:
        pass

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:
        self.sent.append(("tombstone", str(topic), None, True))

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:
        topic = str(topic)
        self.sent.append(("publish", topic, payload, retain))
        if "/_CmdConfigure/" not in topic or self.answer is None:
            return
        command = json.loads(payload)
        ack_topic = topic.replace("/_CmdConfigure/", "/_Ack/")
        if self.answer == "raw":
            body = {
                "correlation_id": command["correlation_id"],
                "result_code": 200,
                "message": "ok",
                "state_writes": self.state_writes,
            }
            message = _Message(json.dumps(body).encode())
        else:
            message = _Message(Ack(correlation_id=command["correlation_id"], result_code=409, message="taken"))
        # Another command's ack on the same topic first: it must not answer this one.
        self.callbacks[ack_topic](self, None, _Message(json.dumps({"correlation_id": "other"}).encode()))
        self.callbacks[ack_topic](self, None, message)

    def of(self, kind: str) -> list[tuple[str, str, object, bool]]:
        return [entry for entry in self.sent if entry[0] == kind]


def _service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, client: _Client) -> Service:
    identity = LocalServiceIdentity(
        service_id="svc-ulid", service_name="svc1", node_id=NODE, system_element_id="", mount=""
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", lambda *a, **k: (client, identity))
    return Service("svc1", state_dir=tmp_path).start()


def test_send_publishes_the_record_as_given(tmp_path, monkeypatch):
    client = _Client()
    svc = _service(tmp_path, monkeypatch, client)

    svc.send(f"colca/v1/_Finding/{NODE}/line1/f1", '{"severity": "warning"}', retain=True)
    svc.send(f"colca/v1/_Metric/{NODE}/line1/speed", '{"value": 1}')

    assert client.of("publish")[-2:] == [
        ("publish", f"colca/v1/_Finding/{NODE}/line1/f1", '{"severity": "warning"}', True),
        ("publish", f"colca/v1/_Metric/{NODE}/line1/speed", '{"value": 1}', False),
    ]


def test_retract_sends_a_tombstone(tmp_path, monkeypatch):
    client = _Client()
    svc = _service(tmp_path, monkeypatch, client)

    svc.retract(f"colca/v1/_Finding/{NODE}/line1/f1")

    assert client.of("tombstone") == [("tombstone", f"colca/v1/_Finding/{NODE}/line1/f1", None, True)]


def test_writes_need_a_started_open_service(tmp_path, monkeypatch):
    with pytest.raises(RuntimeError, match="start"):
        Service("svc1", state_dir=tmp_path).send("t", "{}")
    svc = _service(tmp_path, monkeypatch, _Client())
    svc.close()
    with pytest.raises(RuntimeError, match="closed"):
        svc.command("_CmdConfigure", "element/upsert", {}, lifetime=5)


def test_command_subscribes_to_its_ack_before_sending_and_returns_it(tmp_path, monkeypatch):
    client = _Client()
    svc = _service(tmp_path, monkeypatch, client)
    before_ms = time.time() * 1000

    ack = svc.command("_CmdConfigure", "element/upsert", {"elements": [{"path": "a"}]}, lifetime=5, timeout=5)

    assert ack["result_code"] == 200
    assert ack["state_writes"] == client.state_writes
    ack_topic = f"colca/v1/_Ack/{NODE}/element/upsert"
    command_topic = f"colca/v1/_CmdConfigure/{NODE}/element/upsert"
    order = [(kind, topic) for kind, topic, _p, _r in client.sent if topic in (ack_topic, command_topic)]
    assert order == [("subscribe", ack_topic), ("publish", command_topic)]
    payload = json.loads(client.of("publish")[-1][2])
    assert payload["elements"] == [{"path": "a"}]
    assert payload["correlation_id"] == ack["correlation_id"]
    assert before_ms + 4000 <= payload["expires_at"] <= time.time() * 1000 + 5000
    assert "progress" not in payload


def test_a_command_without_a_lifetime_carries_no_expiry(tmp_path, monkeypatch):
    client = _Client()
    svc = _service(tmp_path, monkeypatch, client)

    ack = svc.command("_CmdConfigure", "element/upsert", {"elements": []}, lifetime=None, timeout=5)

    assert ack["result_code"] == 200
    payload = json.loads(client.of("publish")[-1][2])
    assert "expires_at" not in payload


def test_the_lifetime_is_the_senders_choice(tmp_path, monkeypatch):
    svc = _service(tmp_path, monkeypatch, _Client())
    with pytest.raises(TypeError, match="lifetime"):
        svc.command("_CmdConfigure", "element/upsert", {"elements": []}, timeout=5)  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="lifetime"):
        svc.command("_CmdConfigure", "element/upsert", {"elements": []}, lifetime=0, timeout=5)


def test_a_command_to_a_node_below_is_addressed_to_it_at_its_mounted_path(tmp_path, monkeypatch):
    """A hub service commands a service on an edge: the topic names the edge,
    the path is the hub's (the edge's mount first), and the answer is awaited
    where it arrives at the hub."""
    client = _Client(answer=None)
    svc = _service(tmp_path, monkeypatch, client)

    sent = svc.send_command(
        "_CmdParam", "edge7/fleet/apply", {"command": {"x": 1}}, lifetime=None, node="n-edge7", progress=True
    )

    assert client.of("publish")[-1][1] == "colca/v1/_CmdParam/n-edge7/edge7/fleet/apply"
    assert sent.ack_topic == "colca/v1/_Ack/n-edge7/edge7/fleet/apply"
    assert [t for _k, t, _p, _r in client.of("subscribe")][-1] == sent.ack_topic
    payload = json.loads(client.of("publish")[-1][2])
    assert payload["progress"] is True and "expires_at" not in payload

    # Progress acks are not the outcome; the executor's answer is.
    for body in (
        {"correlation_id": sent.correlation_id, "result_code": 202, "stage": "queued"},
        {"correlation_id": sent.correlation_id, "result_code": 202, "stage": "forwarded"},
        {"correlation_id": sent.correlation_id, "result_code": 200, "message": "applied"},
    ):
        client.callbacks[sent.ack_topic](client, None, _Message(json.dumps(body).encode()))
    assert [p["stage"] for p in sent.progress] == ["queued", "forwarded"]
    assert sent.wait(1)["message"] == "applied"


def test_a_decoded_ack_is_returned_too_and_a_refusal_is_not_raised(tmp_path, monkeypatch):
    client = _Client(answer="decoded")
    svc = _service(tmp_path, monkeypatch, client)

    ack = svc.command("_CmdConfigure", "signal/autobind", {"connector": "c"}, lifetime=5, timeout=5)

    assert (ack["result_code"], ack["message"]) == (409, "taken")


def test_the_ack_topic_is_subscribed_once_and_again_after_a_reconnect(tmp_path, monkeypatch):
    client = _Client()
    svc = _service(tmp_path, monkeypatch, client)
    ack_topic = f"colca/v1/_Ack/{NODE}/constant/upsert"

    svc.command("_CmdConfigure", "constant/upsert", {"constants": []}, lifetime=5, timeout=5)
    svc.command("_CmdConfigure", "constant/upsert", {"constants": []}, lifetime=5, timeout=5)
    assert [t for _k, t, _p, _r in client.of("subscribe")].count(ack_topic) == 1

    svc._on_connect(client, None, None, _ReasonCode())  # a reconnect
    assert [t for _k, t, _p, _r in client.of("subscribe")].count(ack_topic) == 2


def test_no_outcome_in_time_raises_and_leaves_the_command_waitable(tmp_path, monkeypatch):
    client = _Client(answer=None)
    svc = _service(tmp_path, monkeypatch, client)

    sent = svc.send_command("_CmdConfigure", "element/upsert", {"elements": []}, lifetime=None)
    with pytest.raises(TimeoutError, match="stays queued"):
        sent.wait(0.05)

    # The outcome comes later; the same command is waited for again.
    ack = {"correlation_id": sent.correlation_id, "result_code": 200, "message": "late"}
    client.callbacks[sent.ack_topic](client, None, _Message(json.dumps(ack).encode()))
    assert sent.wait(0.05)["message"] == "late"
    assert svc._command_sender._waiters == {}


def test_a_command_under_a_write_deadline_expires_and_is_waited_for_by_it(tmp_path, monkeypatch):
    """A command sent from a command handler must not outlive the command
    being handled."""
    from chaski import write_deadline
    from chaski.service import writes_until

    client = _Client(answer=None)
    client.is_connected = lambda: True
    svc = _service(tmp_path, monkeypatch, client)
    assert write_deadline() is None
    deadline = time.time() + 0.3

    with writes_until(deadline):
        assert write_deadline() == deadline
        started = time.monotonic()
        with pytest.raises(TimeoutError, match="_Ack"):
            svc.command("_CmdConfigure", "element/upsert", {"elements": []}, lifetime=5, timeout=30)
        assert time.monotonic() - started < 2

    payload = json.loads(client.of("publish")[-1][2])
    assert payload["expires_at"] <= deadline * 1000


def test_a_command_past_its_write_deadline_or_without_a_link_is_not_sent(tmp_path, monkeypatch):
    from chaski import NotSent
    from chaski.service import writes_until

    client = _Client()
    client.is_connected = lambda: True
    svc = _service(tmp_path, monkeypatch, client)
    sent = len(client.of("publish"))

    with writes_until(time.time() - 1), pytest.raises(NotSent, match="deadline"):
        svc.command("_CmdConfigure", "element/upsert", {"elements": []}, lifetime=5, timeout=5)
    client.is_connected = lambda: False
    with writes_until(time.time() + 30), pytest.raises(NotSent, match="link is down"):
        svc.command("_CmdConfigure", "element/upsert", {"elements": []}, lifetime=5, timeout=5)
    assert len(client.of("publish")) == sent


def test_a_command_sender_works_on_any_session():
    client = _Client()
    sender = CommandSender(client, NODE)

    ack = sender.command("_CmdConfigure", "signal/autobind", {"connector": "c"}, lifetime=5, timeout=5)

    assert ack["result_code"] == 200
    assert [t for _k, t, _p, _r in client.of("subscribe")] == [f"colca/v1/_Ack/{NODE}/signal/autobind"]
