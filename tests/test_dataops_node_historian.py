"""``NodeHistorian``: the Historian port over a PREKIT node's API, against a
fake of the two API reads it uses (``node_api_fake.FakeNodeApi``)."""

from __future__ import annotations

from itertools import pairwise

import pytest
from node_api_fake import CLIENT_ID, CLIENT_SECRET, FakeNodeApi

import chaski
from chaski.dataops import NodeHistorian, NodeHistorianError
from chaski.dataops.inputs import Historian

SIGNAL = "01HSIGNAL00000000000000001"
OTHER = "01HSIGNAL00000000000000002"
T0 = 1_780_000_000.0


def _historian(api: FakeNodeApi, *, page_size: int = 5, attempts: int = 6, sleeps: list | None = None):
    return NodeHistorian(
        "http://node.test",
        CLIENT_ID,
        CLIENT_SECRET,
        page_size=page_size,
        attempts=attempts,
        transport=api.transport(),
        sleep=(sleeps.append if sleeps is not None else lambda _s: None),
    )


@pytest.fixture
def api():
    fake = FakeNodeApi()
    # Fractional, irregular timestamps: a page boundary must not lose precision.
    fake.add(SIGNAL, [(T0 + i * 1.25 + (i % 3) * 0.000_123, float(i)) for i in range(23)])
    return fake


def test_it_is_a_historian():
    assert isinstance(_historian(FakeNodeApi()), Historian)


@pytest.mark.parametrize("page_size", [1, 4, 5, 22, 23, 24, 10_000])
def test_a_window_is_read_page_by_page_exactly_once(api, page_size):
    historian = _historian(api, page_size=page_size)

    frame = historian.window(SIGNAL, T0, T0 + 3600)

    expected = api.points[SIGNAL]
    assert frame["ts"].tolist() == pytest.approx([ts for ts, _ in expected], abs=1e-6)
    assert frame["value"].tolist() == [value for _, value in expected]
    assert list(frame.columns) == ["ts", "value"]
    # Bounded: every request asks for one page, and as many as the window needs.
    assert all(request["limit"] == page_size and request["raw"] is True for request in api.metric_requests)
    assert len(api.metric_requests) == max(1, -(-len(expected) // page_size))
    assert "cursor" not in api.metric_requests[0]
    assert all("cursor" in request for request in api.metric_requests[1:])


def test_the_window_is_half_open(api):
    stamps = [ts for ts, _ in api.points[SIGNAL]]

    frame = _historian(api).window(SIGNAL, stamps[3], stamps[10])

    assert frame["value"].tolist() == [float(i) for i in range(3, 10)]


def test_adjacent_windows_tile_the_history(api):
    historian = _historian(api, page_size=3)
    stamps = [ts for ts, _ in api.points[SIGNAL]]
    edges = [T0, stamps[7], stamps[8], T0 + 20.0, T0 + 3600]

    values = []
    for start, end in pairwise(edges):
        values.extend(historian.window(SIGNAL, start, end)["value"].tolist())

    assert values == [float(i) for i in range(23)]


def test_rows_sharing_a_timestamp_across_a_page_boundary_come_once_each():
    api = FakeNodeApi()
    api.add(SIGNAL, [(T0 + (i // 3), float(i)) for i in range(12)])

    frame = _historian(api, page_size=2).window(SIGNAL, T0, T0 + 10)

    assert frame["value"].tolist() == [float(i) for i in range(12)]


def test_values_keep_their_type():
    api = FakeNodeApi()
    api.add(SIGNAL, [(T0, True), (T0 + 1, "running"), (T0 + 2, {"speed": 3}), (T0 + 3, 7)])

    frame = _historian(api).window(SIGNAL, T0, T0 + 10)

    assert frame["value"].tolist() == [True, "running", {"speed": 3}, 7.0]


def test_an_empty_window_is_an_empty_frame(api):
    historian = _historian(api)

    frame = historian.window(SIGNAL, T0 - 100, T0 - 50)
    inverted = historian.window(SIGNAL, T0 + 10, T0)

    assert frame.empty and list(frame.columns) == ["ts", "value"]
    assert inverted.empty
    assert len(api.metric_requests) == 1


def test_a_signal_the_api_leaves_out_fails_rather_than_reading_as_no_history(api):
    historian = _historian(api)

    with pytest.raises(NodeHistorianError, match="no history for signal"):
        historian.window(OTHER, T0, T0 + 100)
    with pytest.raises(NodeHistorianError, match="no history for signal"):
        historian.latest_before(OTHER, T0 + 100)


def test_a_node_that_cannot_page_is_refused_loudly(api, monkeypatch):
    original = api._metrics

    def unpaged(request):
        status, headers, series = original({k: v for k, v in request.items() if k not in ("limit", "cursor")})
        return status, headers, series

    monkeypatch.setattr(api, "_metrics", unpaged)

    with pytest.raises(NodeHistorianError, match="too old to page"):
        _historian(api).window(SIGNAL, T0, T0 + 100)


# ─── latest before ──────────────────────────────────────────────────────────


def test_latest_before_is_inclusive_and_none_before_the_first_point(api):
    historian = _historian(api)
    stamps = [ts for ts, _ in api.points[SIGNAL]]

    assert historian.latest_before(SIGNAL, stamps[5]) == pytest.approx((stamps[5], 5.0))
    assert historian.latest_before(SIGNAL, (stamps[5] + stamps[6]) / 2)[1] == 5.0
    assert historian.latest_before(SIGNAL, T0 - 1) is None
    assert api.latest_requests[0]["signal_ids"] == [SIGNAL]


# ─── credentials, retries ───────────────────────────────────────────────────


def test_one_token_serves_every_page_until_it_expires(api):
    historian = _historian(api, page_size=2)

    historian.window(SIGNAL, T0, T0 + 3600)
    historian.latest_before(SIGNAL, T0 + 3600)

    assert api.tokens_issued == 1
    assert len(api.metric_requests) == 12


def test_a_token_refused_mid_read_is_renewed_once_and_the_read_continues(api):
    historian = _historian(api, page_size=4)
    historian.window(SIGNAL, T0, T0 + 1)
    api.revoke_next_token = True

    frame = historian.window(SIGNAL, T0, T0 + 3600)

    assert len(frame) == 23
    assert api.tokens_issued == 2


def test_wrong_credentials_fail_at_once_naming_the_client(api):
    historian = NodeHistorian("http://node.test", CLIENT_ID, "wrong", transport=api.transport(), sleep=lambda _s: None)

    with pytest.raises(NodeHistorianError, match="refused the 'cycles' service account"):
        historian.window(SIGNAL, T0, T0 + 100)


@pytest.mark.parametrize("status", [429, 503])
def test_retry_after_is_honoured(api, status):
    sleeps: list[float] = []
    api.refusals = [(status, {"Retry-After": "7"}), (status, {"Retry-After": "2"})]

    frame = _historian(api, sleeps=sleeps).window(SIGNAL, T0, T0 + 3600)

    assert len(frame) == 23
    assert len(sleeps) == 2
    assert sleeps[0] >= 7.0 and sleeps[1] >= 2.0


def test_a_huge_retry_after_is_capped(api):
    sleeps: list[float] = []
    api.refusals = [(429, {"Retry-After": "86400"})]

    _historian(api, sleeps=sleeps).window(SIGNAL, T0, T0 + 3600)

    assert sleeps[0] <= 60.0 * 1.2 + 30.0


def test_a_node_that_stays_unavailable_fails_after_the_attempts(api):
    sleeps: list[float] = []
    api.refusals = [(503, {})] * 10

    with pytest.raises(NodeHistorianError, match="did not answer after 3 attempt"):
        _historian(api, attempts=3, sleeps=sleeps).window(SIGNAL, T0, T0 + 3600)

    assert len(sleeps) == 2


def test_a_refused_request_is_not_retried(api):
    sleeps: list[float] = []
    api.refusals = [(400, {})]

    with pytest.raises(NodeHistorianError, match="HTTP 400"):
        _historian(api, sleeps=sleeps).window(SIGNAL, T0, T0 + 3600)

    assert sleeps == []


# ─── configuration ──────────────────────────────────────────────────────────

ENV = {
    "PREKIT_URL": "https://reverse-proxy/api/v1/",
    "PREKIT_CLIENT_ID": "cycles",
    "PREKIT_CLIENT_SECRET": "secret",
}


@pytest.mark.parametrize("missing", sorted(ENV))
def test_from_env_needs_the_api_and_the_credential(missing):
    assert NodeHistorian.from_env({k: v for k, v in ENV.items() if k != missing}) is None


def test_from_env_reads_a_service_accounts_environment():
    historian = NodeHistorian.from_env({**ENV, "DATAOPS_HISTORIAN_PAGE_SIZE": "2500"})

    assert historian is not None
    assert historian.url == "https://reverse-proxy"
    assert historian.token_url == "https://reverse-proxy/auth/realms/prekit/protocol/openid-connect/token"  # noqa: S105
    assert historian.page_size == 2500
    overridden = NodeHistorian.from_env({**ENV, "PREKIT_TOKEN_URL": "https://kc.test/token", "PREKIT_REALM": "x"})
    assert overridden is not None
    assert overridden.token_url == "https://kc.test/token"  # noqa: S105


def test_from_env_trusts_the_deployment_ca(tmp_path, monkeypatch):
    seen = {}
    real_client = chaski.dataops.node_historian.httpx.Client

    def client(**kwargs):
        seen.update(kwargs)
        return real_client(**{**kwargs, "verify": True})

    monkeypatch.setattr(chaski.dataops.node_historian.httpx, "Client", client)

    NodeHistorian.from_env({**ENV, "PREKIT_CA_CERT": "/etc/certs/prekit-ca.crt"})

    assert seen["verify"] == "/etc/certs/prekit-ca.crt"


def test_a_dataops_service_picks_the_node_historian_up_from_its_environment(monkeypatch, tmp_path):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)

    automatic = chaski.DataOpsService("cycles", data_dir=tmp_path / "a")
    explicit = object()
    chosen = chaski.DataOpsService("cycles", data_dir=tmp_path / "b", historian=explicit)

    assert isinstance(automatic.historian, NodeHistorian)
    assert automatic.historian.client_id == "cycles"
    assert chosen.historian is explicit
    automatic.close()


def test_without_the_environment_a_dataops_service_has_no_historian(monkeypatch, tmp_path):
    for key in ENV:
        monkeypatch.delenv(key, raising=False)

    assert chaski.DataOpsService("cycles", data_dir=tmp_path).historian is None
