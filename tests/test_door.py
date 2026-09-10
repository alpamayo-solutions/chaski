"""Level-2 pin for ``chaski.door.Door`` against a stub local-door HTTP server
— the wire envelope of ``/fetch``, ``/ack``, ``/kv`` (paging + contract
filter), ``/self`` and ``/publish``, exactly as colcad's httpapi answers
them. Moved here from ``dataops/tests/test_door.py`` with the client itself
(service families design D7); the dataops suite exercises the
same class through its ingest/resolve/output tests."""

from __future__ import annotations

import http.server
import json
import threading
from urllib.parse import parse_qsl, urlsplit

import httpx
import pytest

from chaski.door import Door, Gap, Page


class _StubHandler(http.server.BaseHTTPRequestHandler):
    """Records every request and answers with a canned response per path.

    Class-level state (``responses``/``requests``) is reset per test by the
    ``stub_server`` fixture below, since ``ThreadingHTTPServer`` builds a
    fresh handler instance per request.
    """

    responses: dict[str, tuple[int, dict]] = {}
    requests: list[dict] = []

    def _record(self, method: str) -> None:
        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length) if length else b""
        parsed = urlsplit(self.path)
        type(self).requests.append(
            {
                "method": method,
                "path": parsed.path,
                "query": parse_qsl(parsed.query, keep_blank_values=True),
                "headers": dict(self.headers.items()),
                "body": json.loads(raw_body) if raw_body else None,
            }
        )

    def _respond(self) -> None:
        path = urlsplit(self.path).path
        stub = type(self).responses.get(path, (404, {"error": "no stub for " + path}))
        # A list of stubs answers successive requests to the same path in
        # order (the last one repeats) — what a paged /kv needs.
        if isinstance(stub, list):
            calls = sum(1 for r in type(self).requests if r["path"] == path)
            stub = stub[min(calls, len(stub)) - 1]
        status, payload = stub
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler naming)
        self._record("GET")
        self._respond()

    def do_POST(self) -> None:  # noqa: N802
        self._record("POST")
        self._respond()

    def log_message(self, *args: object) -> None:  # silence default stderr logging
        pass


@pytest.fixture
def stub_server():
    _StubHandler.responses = {}
    _StubHandler.requests = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, _StubHandler
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture
def door(stub_server):
    server, _ = stub_server
    port = server.server_address[1]
    d = Door(f"http://127.0.0.1:{port}", service="dataops")
    try:
        yield d
    finally:
        d.close()


def _last_request(handler_cls) -> dict:
    assert handler_cls.requests, "expected the stub server to have received a request"
    return handler_cls.requests[-1]


# ------------------------------------------------------------------ fetch


def test_fetch_sets_service_header_and_builds_repeatable_signal_id_query(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/fetch"] = (200, {"records": [], "next": 1})

    door.fetch("metrics", "c/dataops/ingest-01", max=50, signal_ids=["sig-1", "sig-2"])

    req = _last_request(handler_cls)
    assert req["method"] == "GET"
    assert req["path"] == "/fetch"
    assert req["headers"]["X-Colca-Service"] == "dataops"
    query = dict(req["query"])
    assert query["stream"] == "metrics"
    assert query["cursor"] == "c/dataops/ingest-01"
    assert query["max"] == "50"
    signal_id_values = [v for k, v in req["query"] if k == "signal_id"]
    assert signal_id_values == ["sig-1", "sig-2"]


def test_fetch_parses_records_next_and_no_gap(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/fetch"] = (
        200,
        {
            "records": [
                {
                    "offset": 5,
                    "origin_offset": 5,
                    "topic": "colca/v1/_Metric/n-1/line1/press1",
                    "payload": {"signal_id": "sig-1", "value": 42.5, "timestamp": 1000.0},
                    "ts": 1000.0,
                    "written_by": "connector",
                    "actor_id": "svc-1",
                    "actor_label": "connector",
                    "actor_kind": "local",
                }
            ],
            "next": 6,
        },
    )

    page = door.fetch("metrics", "c/dataops/ingest-01")

    assert isinstance(page, Page)
    assert page.next == 6
    assert page.gap is None
    assert len(page.records) == 1
    record = page.records[0]
    assert record.offset == 5
    assert record.origin_offset == 5
    assert record.topic == "colca/v1/_Metric/n-1/line1/press1"
    # payload stays a plain decoded dict — the Door never types it
    assert record.payload == {"signal_id": "sig-1", "value": 42.5, "timestamp": 1000.0}
    assert record.ts == 1000.0
    assert record.written_by == "connector"
    assert record.actor_id == "svc-1"
    assert record.actor_label == "connector"
    assert record.actor_kind == "local"


def test_fetch_parses_gap_object(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/fetch"] = (
        200,
        {
            "records": [],
            "next": 500,
            "gap": {
                "stream": "metrics",
                "from_offset": 1,
                "to_offset": 499,
                "first_ts": 900.0,
                "last_ts": 1200.0,
                "approx": True,
            },
        },
    )

    page = door.fetch("metrics", "c/dataops/ingest-01")

    assert page.gap == Gap(stream="metrics", from_offset=1, to_offset=499, first_ts=900.0, last_ts=1200.0, approx=True)


def test_fetch_default_max_is_sent_as_query_param(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/fetch"] = (200, {"records": [], "next": 1})

    door.fetch("metrics", "c/dataops/ingest-01")

    query = dict(_last_request(handler_cls)["query"])
    assert query["max"] == "1000"


def test_fetch_raises_on_http_error(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/fetch"] = (400, {"error": "unknown stream"})

    with pytest.raises(httpx.HTTPStatusError):
        door.fetch("bogus", "c/dataops/ingest-01")


def test_record_fallback_timestamp_converts_colca_millis_to_seconds(stub_server, door):
    """colca's own record ``ts`` is unix MILLISECONDS
    (``colca/internal/store/store.go``'s ``UnixMilli`` cutoff comparison);
    every other timestamp in dataops is unix SECONDS. A record's raw ``ts``
    must stay untouched (it is passed through verbatim from the wire, and
    other tests pin that), but ``fallback_timestamp_s`` — the one place both
    ``Ingest._timestamp_of`` and ``service._decode_metric`` get their
    no-payload-timestamp fallback from — must convert it."""
    _, handler_cls = stub_server
    handler_cls.responses["/fetch"] = (
        200,
        {
            "records": [
                {
                    "offset": 1,
                    "origin_offset": 1,
                    "topic": "colca/v1/_Metric/n-1/line1/press1",
                    "payload": {"signal_id": "sig-1", "value": 1.0},
                    "ts": 1_700_000_000_000.0,  # milliseconds, as colca sends it
                    "written_by": "connector",
                    "actor_id": "svc-1",
                    "actor_label": "connector",
                    "actor_kind": "local",
                }
            ],
            "next": 2,
        },
    )

    page = door.fetch("metrics", "c/dataops/ingest-01")
    record = page.records[0]

    assert record.ts == 1_700_000_000_000.0, "the raw wire ts must pass through untouched"
    assert record.fallback_timestamp_s == pytest.approx(1_700_000_000.0)


def test_fetch_raises_on_connection_error(stub_server):
    server, _ = stub_server
    port = server.server_address[1]
    server.shutdown()  # stop the server so the connection genuinely fails
    d = Door(f"http://127.0.0.1:{port}", service="dataops", timeout=1.0)
    try:
        with pytest.raises(httpx.HTTPError):
            d.fetch("metrics", "c/dataops/ingest-01")
    finally:
        d.close()


# ------------------------------------------------------------------ ack / delete_cursor


def test_ack_sends_offset_body_and_returns_moved(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/ack"] = (200, {"moved": True})

    moved = door.ack("metrics", "c/dataops/ingest-01", 41)

    req = _last_request(handler_cls)
    assert req["method"] == "POST"
    assert req["path"] == "/ack"
    assert req["body"] == {"cursor": "c/dataops/ingest-01", "stream": "metrics", "offset": 41}
    assert moved is True


def test_ack_returns_false_when_the_ack_did_not_move_the_cursor(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/ack"] = (200, {"moved": False})

    assert door.ack("metrics", "c/dataops/ingest-01", 5) is False


def test_delete_cursor_sends_delete_body_and_returns_none(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/ack"] = (200, {"deleted": True})

    result = door.delete_cursor("metrics", "c/dataops/ingest-00")

    req = _last_request(handler_cls)
    assert req["body"] == {"cursor": "c/dataops/ingest-00", "stream": "metrics", "delete": True}
    assert "offset" not in req["body"]
    assert result is None


def test_ack_raises_on_http_error(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/ack"] = (403, {"error": "cursor not owned"})

    with pytest.raises(httpx.HTTPStatusError):
        door.ack("metrics", "n-other/foo", 1)


# ------------------------------------------------------------------ kv / self / publish


def test_kv_builds_prefix_query_and_parses_entries(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/kv"] = (
        200,
        {
            "entries": [
                {
                    "path": "line1/press1",
                    "node_id": "n-1",
                    "topic": "colca/v1/_SystemElement/n-1/line1/press1",
                    "payload": {"name": "press1"},
                    "ts": 100.0,
                    "offset": 3,
                }
            ]
        },
    )

    entries = door.kv("line1")

    query = dict(_last_request(handler_cls)["query"])
    assert query["prefix"] == "line1"
    assert query["max"] == "10000"
    assert "contract" not in query, "no filter asked for, none sent — absent means every contract"
    assert len(entries) == 1
    assert entries[0].path == "line1/press1"
    assert entries[0].node_id == "n-1"
    assert entries[0].payload == {"name": "press1"}
    assert entries[0].offset == 3


def _kv_entry(path: str, contract: str = "_SystemElement") -> dict:
    return {
        "path": path,
        "node_id": "n-1",
        "topic": f"colca/v1/{contract}/n-1/{path}",
        "payload": {"name": path},
        "ts": 1.0,
        "offset": 1,
    }


def test_kv_follows_next_page_tokens_until_the_door_returns_none(stub_server, door):
    """One call, every page: the door hands back ``next`` as an opaque
    token and the client resends it as ``after`` until it comes back
    empty — the entries of all three pages arrive as one list, in order."""
    _, handler_cls = stub_server
    handler_cls.responses["/kv"] = [
        (200, {"entries": [_kv_entry("a"), _kv_entry("b")], "next": "tok-1"}),
        (200, {"entries": [_kv_entry("c")], "next": "tok-2"}),
        (200, {"entries": [_kv_entry("d")], "next": ""}),
    ]

    entries = door.kv("")

    assert [e.path for e in entries] == ["a", "b", "c", "d"]
    kv_requests = [r for r in handler_cls.requests if r["path"] == "/kv"]
    assert [dict(r["query"]).get("after") for r in kv_requests] == [None, "tok-1", "tok-2"]


def test_kv_refuses_a_repeated_page_token(stub_server, door):
    """A door that answers the same ``next`` twice would loop this client
    forever; it raises instead of spinning."""
    _, handler_cls = stub_server
    handler_cls.responses["/kv"] = (200, {"entries": [_kv_entry("a")], "next": "tok-1"})

    with pytest.raises(RuntimeError, match="repeated page token"):
        door.kv("")


def test_kv_sends_one_contract_filter_as_a_repeatable_query_param(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/kv"] = (200, {"entries": [_kv_entry("g1", "_Group")], "next": ""})

    entries = door.kv("", contract="_Group")

    req = _last_request(handler_cls)
    assert [v for k, v in req["query"] if k == "contract"] == ["_Group"]
    assert [e.topic for e in entries] == ["colca/v1/_Group/n-1/g1"]


def test_kv_sends_several_contracts_as_repeated_params_on_every_page(stub_server, door):
    """The filter belongs to the scan, not to its first page — a paged read
    that dropped it on page two would silently widen to every contract."""
    _, handler_cls = stub_server
    handler_cls.responses["/kv"] = [
        (200, {"entries": [_kv_entry("s1", "_Signal")], "next": "tok-1"}),
        (200, {"entries": [_kv_entry("e1", "_SystemElement")], "next": ""}),
    ]

    door.kv("line1", contract=["_Signal", "_SystemElement"])

    kv_requests = [r for r in handler_cls.requests if r["path"] == "/kv"]
    assert len(kv_requests) == 2
    for req in kv_requests:
        assert [v for k, v in req["query"] if k == "contract"] == ["_Signal", "_SystemElement"]
        assert dict(req["query"])["prefix"] == "line1"


def test_kv_unknown_contract_is_the_doors_400_not_an_empty_list(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/kv"] = (400, {"error": 'unknown contract: "_Bogus"'})

    with pytest.raises(httpx.HTTPStatusError):
        door.kv("", contract="_Bogus")


# ------------------------------------------------------------------ Page.ack_offset


def _rec(offset: int) -> dict:
    return {"offset": offset, "origin_offset": offset, "topic": "t", "payload": {}, "ts": 0.0}


def test_page_ack_offset_is_the_last_record_the_gap_bound_or_nothing(stub_server, door):
    """The one rule the dataops ingest loop and ``Stream`` share: after a
    page is fully processed, ack its last record; when nothing survived
    at/after the low-water mark, ack the gap's bound (or the next fetch
    reports the identical gap forever); an empty page acks nothing."""
    _, handler_cls = stub_server
    gap = {"stream": "metrics", "from_offset": 1, "to_offset": 40, "first_ts": None, "last_ts": None}
    handler_cls.responses["/fetch"] = [
        (200, {"records": [_rec(41), _rec(42)], "next": 43, "gap": gap}),
        (200, {"records": [], "next": 41, "gap": gap}),
        (200, {"records": [], "next": 43}),
    ]

    with_records = door.fetch("metrics", "c/svc/x")
    gap_only = door.fetch("metrics", "c/svc/x")
    empty = door.fetch("metrics", "c/svc/x")

    assert with_records.ack_offset == 42, "records win over the gap they arrived beside"
    assert gap_only.ack_offset == 40
    assert empty.ack_offset is None


def test_self_info_returns_raw_dict(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/self"] = (
        200,
        {"ulid": "01ABC", "name": "dataops", "node": "n-1", "element": "el-1", "mount": "line1"},
    )

    info = door.self_info()

    assert info == {"ulid": "01ABC", "name": "dataops", "node": "n-1", "element": "el-1", "mount": "line1"}


def test_publish_embeds_payload_as_json_value_not_a_double_encoded_string(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/publish"] = (200, {"stream": "metrics", "offset": 9, "topic": "t"})

    door.publish("colca/v1/_Metric/n-1/line1/press1", json.dumps({"signal_id": "sig-1", "value": 1}))

    req = _last_request(handler_cls)
    assert req["path"] == "/publish"
    assert req["body"] == {
        "topic": "colca/v1/_Metric/n-1/line1/press1",
        "payload": {"signal_id": "sig-1", "value": 1},
    }


def test_publish_raises_on_http_error(stub_server, door):
    _, handler_cls = stub_server
    handler_cls.responses["/publish"] = (500, {"error": "boom"})

    with pytest.raises(httpx.HTTPStatusError):
        door.publish("colca/v1/_Metric/n-1/line1/press1", json.dumps({"value": 1}))
