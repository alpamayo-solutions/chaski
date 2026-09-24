"""Tests for Service's lifecycle with a fake MQTT client and real identity files.

Covered: ServiceDetails and its is_active and status changes, NotEnrolled for a
refused CONNACK, the enroll hint, pending()'s three reasons, retire() needing a
token outside a deployment, and idempotent identity minting with 0600 files.
"""

from __future__ import annotations

from pathlib import Path

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
import pytest
from colca_data_contracts.local_service import LocalServiceIdentity
from colca_data_contracts.payload import ServiceDetails

from chaski.service import NotEnrolled, Service, _pending_reason


class _FakeReasonCode:
    def __init__(self, is_failure: bool = False) -> None:
        self.is_failure = is_failure


class _FakeClient:
    """A minimal franzmq.Client stand-in with a configurable CONNACK outcome
    that records tombstones."""

    def __init__(self, *, reason_code: _FakeReasonCode | None = None) -> None:
        self.published: list[tuple[str, object]] = []
        self.tombstoned: list[str] = []
        self.subscriptions: dict[str, object] = {}
        self.on_connect = None
        self.node_id = None
        self._reason_code = reason_code or _FakeReasonCode()
        self.disconnected = False

    def _handle_on_message(self, message) -> None:
        pass

    def loop_start(self) -> None:
        if self.on_connect is not None:
            self.on_connect(self, None, None, self._reason_code)

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        self.disconnected = True

    def subscribe(self, topic, qos: int = 0, callback=None) -> None:
        self.subscriptions[str(topic)] = callback

    def publish(self, topic, payload, qos: int = 0, retain: bool = False, wait: bool = True) -> None:
        self.published.append((str(topic), payload))

    def publish_tombstone(self, topic, qos: int = 0, wait: bool = True) -> None:
        self.tombstoned.append(str(topic))


def _local_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, mount: str = "line1", **kwargs
) -> tuple[Service, _FakeClient]:
    client = _FakeClient()
    identity = LocalServiceIdentity(
        service_id="svc-ulid",
        service_name="svc1",
        node_id="n-edge1",
        system_element_id="el-1",
        mount=mount,
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", lambda *a, **k: (client, identity))
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)
    svc = Service("svc1", mount, state_dir=tmp_path, **kwargs)
    svc.start()
    return svc, client


def test_clock_progress_survives_loss_of_sync_and_broker(tmp_path, monkeypatch):
    from chaski.clock import Clock

    svc, client = _local_service(tmp_path, monkeypatch, clock=Clock(source="mqtt"))
    assert svc.report_progress(1000)
    details = client.published[-1][1]
    progress = details.metadata["application_clock"]
    assert progress["processed_at"] == 1000
    assert progress["ready"] is False
    assert progress["observed_at"] is None
    assert progress["lag_s"] is None

    def disconnected(*args, **kwargs):
        raise ConnectionError("broker unavailable")

    monkeypatch.setattr(client, "publish", disconnected)
    svc._last_clock_report = 0
    assert svc.report_progress(1000) is False
    assert svc.metadata["application_clock"]["processed_at"] == 1000
    with pytest.raises(ValueError, match="finite"):
        svc.report_progress(float("nan"))
    monkeypatch.undo()
    svc.close()


def test_progress_liveness_continues_while_factory_time_is_paused(tmp_path, monkeypatch):
    from colca_data_contracts.payload import ClockDefinition

    from chaski.clock import Clock

    real = [10000.0]
    clock = Clock(wall=lambda: real[0])
    clock.apply_definition(ClockDefinition("factory", "run", 1, real[0], 1000, 0))
    svc, client = _local_service(tmp_path, monkeypatch, clock=clock)
    assert svc.report_progress(1000)
    real[0] += 6
    svc._last_clock_report = float("-inf")
    assert svc._publish_progress()
    progress = client.published[-1][1].metadata["application_clock"]
    assert progress["processed_at"] == progress["factory_now"] == 1000
    assert progress["observed_at"] == 10006
    svc.close()
    assert not svc._progress_thread.is_alive()


def _external_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    mount: str = "site1/erp",
    fails: bool = False,
) -> tuple[Service, _FakeClient]:
    client = _FakeClient(reason_code=_FakeReasonCode(is_failure=fails))
    monkeypatch.setattr("chaski.service._read_node_id", lambda url, timeout=10.0: "n-ext")
    monkeypatch.setattr("chaski.service._connect_external_mqtt", lambda *a, **k: client)
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)
    svc = Service("erp-bridge", mount, node="https://edge1.example", state_dir=tmp_path)
    return svc, client


def _details(client: _FakeClient) -> list[ServiceDetails]:
    return [p for _t, p in client.published if isinstance(p, ServiceDetails)]


# -- ServiceDetails shape and is_active/status transitions ------------------


def test_start_publishes_one_active_healthy_service_details(tmp_path, monkeypatch):
    _, client = _local_service(
        tmp_path,
        monkeypatch,
        display_name="ERP Bridge",
        description="pushes ERP orders",
        version="1.2.3",
    )
    details = _details(client)
    assert len(details) == 1
    d = details[0]
    assert d.is_active is True
    assert d.architecture_metadata == {"status": "healthy"}
    assert d.display_name == "ERP Bridge"
    assert d.description == "pushes ERP orders"
    assert d.metadata["version"] == "1.2.3"
    assert d.id == "svc-ulid"
    assert d.name == "svc1"
    assert d.colca_node_id == "n-edge1"


def test_close_republishes_inactive(tmp_path, monkeypatch):
    svc, client = _local_service(tmp_path, monkeypatch)
    svc.close()
    details = _details(client)
    assert details[-1].is_active is False
    assert client.disconnected is True


def test_close_is_idempotent(tmp_path, monkeypatch):
    svc, client = _local_service(tmp_path, monkeypatch)
    svc.close()
    published_after_first_close = len(client.published)
    svc.close()
    assert len(client.published) == published_after_first_close


def test_status_transitions_health_without_deactivating(tmp_path, monkeypatch):
    svc, client = _local_service(tmp_path, monkeypatch)
    svc.status(False, "source unreachable")
    latest = _details(client)[-1]
    assert latest.is_active is True
    assert latest.architecture_metadata == {"status": "unhealthy", "detail": "source unreachable"}

    svc.status(True)
    latest = _details(client)[-1]
    assert latest.is_active is True
    assert latest.architecture_metadata == {"status": "healthy"}


def test_local_start_sets_a_last_will_before_connect(tmp_path, monkeypatch):
    captured: dict = {}
    identity = LocalServiceIdentity(
        service_id="svc-ulid",
        service_name="svc1",
        node_id="n-edge1",
        system_element_id="el-1",
        mount="line1",
    )
    client = _FakeClient()

    def fake_connect(_name, **kwargs):
        captured.update(kwargs)
        return client, identity

    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", fake_connect)
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)

    svc = Service("svc1", "line1", state_dir=tmp_path)
    svc.start()

    assert "will" in captured
    will_topic, will_payload = captured["will"]
    assert str(will_topic) == "colca/v1/_ServiceDetails/n-edge1/line1/svc1/_service"
    assert will_payload.is_active is False


# -- NotEnrolled / enroll_hint / wait_enrolled -------------------------------


def test_external_connect_refused_raises_not_enrolled_with_the_exact_command(tmp_path, monkeypatch):
    svc, _ = _external_service(tmp_path, monkeypatch, fails=True)

    with pytest.raises(NotEnrolled) as excinfo:
        svc.start()

    expected_command = (
        f"enroll {svc.name} at https://edge1.example: author an element at 'site1/erp' there, then "
        f"POST /enroll with the admin token and "
        f'{{"ulid": "{svc.ulid}", "kind": "external", "element": "<that element id>", "pubkey": "{svc.pubkey}"}}'
    )
    assert svc.enroll_hint() == expected_command
    assert expected_command in str(excinfo.value)
    assert "erp-bridge is not enrolled at https://edge1.example" in str(excinfo.value)


def test_enroll_hint_only_applies_outside_a_deployment(tmp_path, monkeypatch):
    svc, _client = _local_service(tmp_path, monkeypatch)
    with pytest.raises(RuntimeError):
        svc.enroll_hint()


def test_wait_enrolled_retries_start_until_accepted(tmp_path, monkeypatch):
    attempts = {"n": 0}
    failing = _FakeClient(reason_code=_FakeReasonCode(is_failure=True))
    accepted = _FakeClient(reason_code=_FakeReasonCode(is_failure=False))

    def fake_connect(*_a, **_k):
        attempts["n"] += 1
        return failing if attempts["n"] == 1 else accepted

    monkeypatch.setattr("chaski.service._read_node_id", lambda url, timeout=10.0: "n-ext")
    monkeypatch.setattr("chaski.service._connect_external_mqtt", fake_connect)
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)

    svc = Service("erp-bridge", "site1/erp", node="https://edge1.example", state_dir=tmp_path)
    svc.wait_enrolled(timeout=5.0, poll_interval=0.01)

    assert attempts["n"] == 2
    assert svc._client is accepted


def test_wait_enrolled_only_applies_outside_a_deployment(tmp_path, monkeypatch):
    svc, _client = _local_service(tmp_path, monkeypatch)
    with pytest.raises(RuntimeError):
        svc.wait_enrolled(timeout=0.1)


# -- retire() -----------------------------------------------------------


def test_retire_outside_refuses_without_a_token(tmp_path, monkeypatch):
    svc, _client = _external_service(tmp_path, monkeypatch)
    svc.start()
    with pytest.raises(RuntimeError):
        svc.retire()


def test_retire_outside_tombstones_and_revokes_with_a_token(tmp_path, monkeypatch):
    svc, client = _external_service(tmp_path, monkeypatch)
    svc.start()
    revoked: dict = {}
    monkeypatch.setattr(
        "chaski.service._revoke_external",
        lambda url, ulid, token, **k: revoked.update(url=url, ulid=ulid, token=token),
    )

    svc.retire("admin-token")

    assert str(svc._details_topic) in client.tombstoned
    assert str(svc._catalogue_topic) in client.tombstoned
    assert revoked == {"url": "https://edge1.example", "ulid": svc.ulid, "token": "admin-token"}
    assert client.disconnected is True


def test_retire_local_tombstones_without_a_token(tmp_path, monkeypatch):
    svc, client = _local_service(tmp_path, monkeypatch)
    svc.retire()
    assert str(svc._details_topic) in client.tombstoned
    assert str(svc._catalogue_topic) in client.tombstoned


# -- pending() ------------------------------------------------------------


def test_pending_reason_is_not_enrolled_when_disconnected():
    assert _pending_reason("temp", "line1", "conn-1", connected=False) == "not enrolled"


def test_pending_reason_names_the_unauthored_element_for_a_multi_segment_path():
    reason = _pending_reason("press3/temp", "line1", "conn-1", connected=True)
    assert reason.startswith("element not yet authored")
    assert "line1/press3" in reason


def test_pending_reason_is_awaiting_binding_for_a_mount_level_path_and_names_the_operator_command():
    reason = _pending_reason("temp", "line1", "conn-1", connected=True)
    assert reason.startswith("awaiting binding")
    assert "signal/autobind --connector conn-1" in reason


def test_pending_lists_every_unbound_published_path_with_its_reason(tmp_path, monkeypatch):
    svc, _client = _local_service(tmp_path, monkeypatch)
    svc.publish("temp", 1.0)
    svc.publish("press3/pressure", 2.0)

    pending = dict(svc.pending())

    assert set(pending) == {"temp", "press3/pressure"}
    assert pending["temp"].startswith("awaiting binding")
    assert pending["press3/pressure"].startswith("element not yet authored")


# -- identity minting -----------------------------------------------------


def test_identity_minting_is_idempotent_and_files_are_0600(tmp_path):
    first = Service("erp-bridge", "site1/erp", node="https://edge1.example", state_dir=tmp_path)
    second = Service("erp-bridge", "site1/erp", node="https://edge1.example", state_dir=tmp_path)

    assert first.ulid == second.ulid
    assert first.pubkey == second.pubkey
    assert len(first.pubkey) == 64
    assert all(c in "0123456789abcdef" for c in first.pubkey)

    identity_dir = tmp_path / "identity"
    for name in ("identity.key", "identity.key.crt", "ulid"):
        path = identity_dir / name
        assert path.exists(), f"{name} was not written"
        mode = path.stat().st_mode & 0o777
        assert mode == 0o600, f"{path} has mode {oct(mode)}, expected 0600"


def test_two_different_names_mint_different_identities(tmp_path):
    a = Service("erp-bridge", "site1/erp", node="https://edge1.example", state_dir=tmp_path / "a")
    b = Service("mes-bridge", "site1/mes", node="https://edge1.example", state_dir=tmp_path / "b")
    assert a.ulid != b.ulid
    assert a.pubkey != b.pubkey


def test_the_client_tolerates_undecodable_messages_before_its_loop_starts(tmp_path, monkeypatch):
    # clean_start=False: the broker delivers the session's queued messages right
    # after CONNACK, before this run subscribes anything
    guarded: list[bool] = []

    class _Client(_FakeClient):
        def loop_start(self) -> None:
            guarded.append(getattr(self._handle_on_message, "_tolerates_undecodable", False))
            super().loop_start()

    client = _Client()
    identity = LocalServiceIdentity(
        service_id="svc-ulid", service_name="svc1", node_id="n-edge1", system_element_id="el-1", mount="line1"
    )
    monkeypatch.setattr("chaski.service.resolve_local_identity", lambda *a, **k: identity)
    monkeypatch.setattr("chaski.service.connect_local_mqtt", lambda *a, **k: (client, identity))
    monkeypatch.setattr("chaski.service.attach_log_publisher", lambda *a, **k: None)
    Service("svc1", "line1", state_dir=tmp_path).start()
    assert guarded == [True]
