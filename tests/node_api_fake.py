"""A stand-in for the two PREKIT node API reads ``NodeHistorian`` uses.

It answers like the node does (PREKIT ``api/src/edge/workbench/grafana_reads.py``):
the token endpoint for one service account, the raw metric read paged by
``limit`` and an opaque ``cursor`` in (timestamp, row) order with
``next_cursor`` on a paged series, and the latest-before read. A signal it
does not hold is left out of the answer, as an unreadable one is.

The same object serves an ``httpx.MockTransport`` (unit tests) and a real
HTTP server on a local port (integration tests).
"""

from __future__ import annotations

import base64
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs

import httpx

CLIENT_ID = "cycles"
CLIENT_SECRET = "s3cret"  # noqa: S105 - a test fixture's credential
TOKEN_PATH = "/auth/realms/prekit/protocol/openid-connect/token"  # noqa: S105 - a path, not a secret
METRICS_PATH = "/api/v1/workbench/grafana/metrics/"
LATEST_PATH = "/api/v1/workbench/grafana/metrics/latest-before/"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat().replace("+00:00", "Z")


def _epoch(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


def _point(ts: float, value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        kind = "boolean"
    elif isinstance(value, int | float):
        kind = "number"
    elif isinstance(value, str):
        kind = "string"
    else:
        kind = "json"
    return {"timestamp": _iso(ts), "value_type": kind, "value": value}


class FakeNodeApi:
    def __init__(self, *, token_lifetime: float = 300.0) -> None:
        #: signal id -> [(ts, value)], kept ordered.
        self.points: dict[str, list[tuple[float, Any]]] = {}
        self.token_lifetime = token_lifetime
        self.tokens_issued = 0
        self.metric_requests: list[dict[str, Any]] = []
        self.latest_requests: list[dict[str, Any]] = []
        #: Answers to give before serving normally: (status, headers).
        self.refusals: list[tuple[int, dict[str, str]]] = []
        self.revoke_next_token = False
        self._valid: set[str] = set()
        self._lock = threading.Lock()

    def add(self, signal_id: str, rows) -> None:
        self.points[signal_id] = sorted([*self.points.get(signal_id, []), *rows], key=lambda row: row[0])

    # -- dispatch ----------------------------------------------------------

    def handle(self, method: str, path: str, headers: dict[str, str], body: bytes) -> tuple[int, dict[str, str], Any]:
        with self._lock:
            if method == "POST" and path == TOKEN_PATH:
                return self._token(parse_qs(body.decode()))
            if method != "POST" or path not in (METRICS_PATH, LATEST_PATH):
                return 404, {}, {"detail": "not found"}
            token = headers.get("authorization", "").removeprefix("Bearer ")
            if token not in self._valid:
                return 401, {}, {"detail": "unauthenticated"}
            if self.revoke_next_token:
                self.revoke_next_token = False
                self._valid.discard(token)
                return 401, {}, {"detail": "token expired"}
            if self.refusals:
                status, extra = self.refusals.pop(0)
                return status, extra, {"detail": "try later"}
            request = json.loads(body)
            if path == METRICS_PATH:
                self.metric_requests.append(request)
                return self._metrics(request)
            self.latest_requests.append(request)
            return self._latest(request)

    def _token(self, form: dict[str, list[str]]) -> tuple[int, dict[str, str], Any]:
        if (
            form.get("grant_type") != ["client_credentials"]
            or form.get("client_id") != [CLIENT_ID]
            or form.get("client_secret") != [CLIENT_SECRET]
        ):
            return 401, {}, {"error": "invalid_client"}
        self.tokens_issued += 1
        token = f"token-{self.tokens_issued}"
        self._valid.add(token)
        return 200, {}, {"access_token": token, "expires_in": self.token_lifetime, "token_type": "Bearer"}

    def _metrics(self, request: dict[str, Any]) -> tuple[int, dict[str, str], Any]:
        start, end = _epoch(request["from"]), _epoch(request["to"])
        limit = request.get("limit")
        cursor = request.get("cursor")
        paging = limit is not None or cursor is not None
        after = int(base64.b64decode(cursor)) if cursor else -1
        series = []
        for signal_id in request["signal_ids"]:
            rows = [
                (index, ts, value)
                for index, (ts, value) in enumerate(self.points.get(signal_id, []))
                if start <= ts < end and index > after
            ]
            cap = limit or 100_000
            page, truncated = rows[:cap], len(rows) > cap
            entry: dict[str, Any] = {
                "signal_id": signal_id,
                "agg_func": "raw",
                "range": {"from": request["from"], "to": request["to"], "bucket_seconds": 0},
                "points": [_point(ts, value) for _index, ts, value in page],
                "truncated": truncated,
            }
            if paging:
                entry["next_cursor"] = base64.b64encode(str(page[-1][0]).encode()).decode() if truncated else None
            if signal_id in self.points:
                series.append(entry)
        return 200, {}, series

    def _latest(self, request: dict[str, Any]) -> tuple[int, dict[str, str], Any]:
        before = _epoch(request["before"])
        out = []
        for signal_id in request["signal_ids"]:
            if signal_id not in self.points:
                continue
            earlier = [(ts, value) for ts, value in self.points[signal_id] if ts <= before]
            out.append({"signal_id": signal_id, "point": _point(*earlier[-1]) if earlier else None})
        return 200, {}, out

    # -- transports --------------------------------------------------------

    def transport(self) -> httpx.MockTransport:
        def respond(request: httpx.Request) -> httpx.Response:
            status, headers, payload = self.handle(
                request.method, request.url.path, dict(request.headers), request.read()
            )
            return httpx.Response(status, headers=headers, json=payload)

        return httpx.MockTransport(respond)

    @contextmanager
    def serve(self) -> Iterator[str]:
        """Serve on a local port; yields the origin, e.g. ``http://127.0.0.1:5123``."""
        api = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                status, headers, payload = api.handle(
                    "POST", self.path, {k.lower(): v for k, v in self.headers.items()}, self.rfile.read(length)
                )
                data = json.dumps(payload).encode()
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args: Any) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}"
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
