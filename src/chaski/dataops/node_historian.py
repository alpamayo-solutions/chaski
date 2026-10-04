"""``NodeHistorian``: the :class:`~chaski.dataops.Historian` port over a PREKIT node's API.

A DataOps service that is not PREKIT's own reads node history (the edge
historian, typically 400 days) through the node's API instead of the
database: it never holds the database password and reads only what its
identity may read.

It authenticates as the service's Keycloak service account
(``client_credentials``). A PREKIT deployment hands a service declared with
``service_account:`` everything this needs:

=======================  ===============================================
``PREKIT_URL``           the node's API (``https://reverse-proxy``)
``PREKIT_CLIENT_ID``     the service account's client id
``PREKIT_CLIENT_SECRET`` its secret
``PREKIT_CA_CERT``       the deployment's CA chain, to verify the API
=======================  ===============================================

so :class:`~chaski.dataops.DataOpsService` builds one by itself
(:meth:`NodeHistorian.from_env`) when no ``historian=`` is passed and these
are set. ``PREKIT_TOKEN_URL`` overrides the token endpoint, which defaults to
the realm (``PREKIT_REALM``, default ``prekit``) behind the same origin.

The two reads:

* :meth:`~NodeHistorian.window` pages the raw metric read
  (``POST /api/v1/workbench/grafana/metrics/`` with ``raw``, ``limit`` and
  ``cursor``), ``page_size`` rows per request, so one response never holds
  more than a page. The frame it returns holds the window, so memory is
  bounded by the caller's window (a backfill's ``window``), not the horizon.
* :meth:`~NodeHistorian.latest_before` asks
  ``POST /api/v1/workbench/grafana/metrics/latest-before/``.

Failures are visible, never an empty answer: a signal the API leaves out
(unreadable, or the node does not expose its historian), a refused request
or a node too old to page raises :class:`NodeHistorianError`, which fails the
backfill window so it is retried and reported. ``429`` and ``503`` are
retried after ``Retry-After``; other server and transport errors with
bounded, jittered backoff; ``401`` once with a fresh token.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any

import httpx

from chaski.retry import RETRY_AFTER_MAX_S, Backoff

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger("chaski.dataops.node_historian")

METRICS_PATH = "/api/v1/workbench/grafana/metrics/"
LATEST_BEFORE_PATH = "/api/v1/workbench/grafana/metrics/latest-before/"
#: Rows per request. The API accepts up to 100,000.
DEFAULT_PAGE_SIZE = 10_000
DEFAULT_REALM = "prekit"
#: Tries per request, the first included, before :class:`NodeHistorianError`.
DEFAULT_ATTEMPTS = 6
#: Statuses retried after a wait: rate limited, or the node briefly unavailable.
RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
#: A token is renewed this long before it expires.
TOKEN_MARGIN_S = 30.0


class NodeHistorianError(RuntimeError):
    """The node's API could not answer a history read."""


class NodeHistorian:
    """:class:`chaski.dataops.Historian` over a PREKIT node's API; see the module docstring."""

    def __init__(
        self,
        url: str,
        client_id: str,
        client_secret: str,
        *,
        token_url: str | None = None,
        realm: str = DEFAULT_REALM,
        verify: bool | str = True,
        page_size: int = DEFAULT_PAGE_SIZE,
        timeout: float = 30.0,
        attempts: int = DEFAULT_ATTEMPTS,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not 1 <= page_size <= 100_000:
            raise ValueError("page_size must be between 1 and 100000")
        if attempts < 1:
            raise ValueError("attempts must be at least 1")
        origin = url.rstrip("/").removesuffix("/api/v1")
        self.url = origin
        self.client_id = client_id
        self._client_secret = client_secret
        self.token_url = token_url or f"{origin}/auth/realms/{realm}/protocol/openid-connect/token"
        self.page_size = page_size
        self._attempts = attempts
        self._sleep = sleep
        self._http = httpx.Client(base_url=origin, verify=verify, timeout=timeout, transport=transport)
        self._token_lock = threading.Lock()
        self._token: str | None = None
        self._token_expires = 0.0

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> NodeHistorian | None:
        """A historian from ``PREKIT_URL``, ``PREKIT_CLIENT_ID`` and
        ``PREKIT_CLIENT_SECRET`` (plus the optional ``PREKIT_CA_CERT``,
        ``PREKIT_TOKEN_URL``, ``PREKIT_REALM`` and
        ``DATAOPS_HISTORIAN_PAGE_SIZE``), or ``None`` when any of the three
        is missing."""
        env = os.environ if environ is None else environ
        url, client_id, secret = (
            env.get(key, "").strip() for key in ("PREKIT_URL", "PREKIT_CLIENT_ID", "PREKIT_CLIENT_SECRET")
        )
        if not (url and client_id and secret):
            return None
        page_size = env.get("DATAOPS_HISTORIAN_PAGE_SIZE", "").strip()
        return cls(
            url,
            client_id,
            secret,
            token_url=env.get("PREKIT_TOKEN_URL", "").strip() or None,
            realm=env.get("PREKIT_REALM", "").strip() or DEFAULT_REALM,
            verify=env.get("PREKIT_CA_CERT", "").strip() or True,
            page_size=int(page_size) if page_size else DEFAULT_PAGE_SIZE,
        )

    def __repr__(self) -> str:
        return f"NodeHistorian({self.url!r}, client_id={self.client_id!r})"

    def close(self) -> None:
        self._http.close()

    # -- the port ----------------------------------------------------------

    def window(self, signal_id: str, start: float, end: float) -> pd.DataFrame:
        """Points with ``start <= ts < end``, ordered by ts, read page by page."""
        import pandas as pd

        timestamps: list[float] = []
        values: list[Any] = []
        if end > start:
            body: dict[str, Any] = {
                "signal_ids": [signal_id],
                "from": _iso(start),
                "to": _iso(end),
                "raw": True,
                "limit": self.page_size,
            }
            seen: set[str] = set()
            while True:
                series = self._series(self._post(METRICS_PATH, body), signal_id)
                for point in series.get("points", []):
                    timestamps.append(_epoch(point["timestamp"]))
                    values.append(_value(point))
                if "next_cursor" not in series:
                    raise NodeHistorianError(
                        f"{self.url} answered a paged read without next_cursor: the node's PREKIT "
                        "is too old to page raw metric reads"
                    )
                cursor = series["next_cursor"]
                if cursor is None:
                    break
                if cursor in seen:
                    raise NodeHistorianError(f"{self.url} repeated a page cursor for signal {signal_id}")
                seen.add(cursor)
                body["cursor"] = cursor
        return pd.DataFrame({"ts": timestamps, "value": values}, columns=["ts", "value"])

    def latest_before(self, signal_id: str, before: float) -> tuple[float, Any] | None:
        """``(ts, value)`` of the most recent point with ``ts <= before``, or ``None``."""
        rows = self._post(LATEST_BEFORE_PATH, {"signal_ids": [signal_id], "before": _iso(before)})
        row = next((row for row in rows if isinstance(row, dict) and row.get("signal_id") == signal_id), None)
        if row is None:
            raise _not_served(self.url, signal_id)
        point = row.get("point")
        if point is None:
            return None
        return _epoch(point["timestamp"]), _value(point)

    # -- transport ---------------------------------------------------------

    def _series(self, payload: Any, signal_id: str) -> dict[str, Any]:
        for series in payload if isinstance(payload, list) else []:
            if isinstance(series, dict) and series.get("signal_id") == signal_id:
                return series
        raise _not_served(self.url, signal_id)

    def _post(self, path: str, body: dict[str, Any]) -> Any:
        """POST ``body`` as the service account; the decoded JSON answer.
        Retries what a retry can fix, raises :class:`NodeHistorianError`
        for the rest and once the attempts are spent."""
        backoff = Backoff(minimum=1.0, maximum=30.0)
        renewed = False
        last: str = ""
        for attempt in range(1, self._attempts + 1):
            try:
                response = self._http.post(path, json=body, headers={"Authorization": f"Bearer {self._bearer()}"})
            except (httpx.TransportError, _TokenUnavailable) as exc:
                last = f"{type(exc).__name__}: {exc}"
                if attempt < self._attempts:
                    self._wait(backoff.delay(), last)
                continue
            if response.status_code == 401 and not renewed:
                # Expired or revoked early: one fresh token, at once.
                renewed = True
                last = "HTTP 401"
                self._forget_token()
                continue
            if response.status_code in RETRYABLE_STATUS:
                last = f"HTTP {response.status_code}"
                if attempt < self._attempts:
                    error = httpx.HTTPStatusError(last, request=response.request, response=response)
                    self._wait(max(backoff.delay(error), _retry_after(response)), last)
                continue
            if response.is_error:
                raise NodeHistorianError(
                    f"{self.url}{path} refused the read: HTTP {response.status_code} {response.text[:300]}"
                )
            return response.json()
        raise NodeHistorianError(f"{self.url}{path} did not answer after {self._attempts} attempt(s): {last}")

    def _wait(self, seconds: float, reason: str) -> None:
        log.info("Node historian: %s; retrying in %.1f s", reason, seconds)
        self._sleep(seconds)

    def _bearer(self) -> str:
        with self._token_lock:
            if self._token is not None and time.monotonic() < self._token_expires:
                return self._token
            try:
                response = self._http.post(
                    self.token_url,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self.client_id,
                        "client_secret": self._client_secret,
                    },
                )
            except httpx.TransportError as exc:
                raise _TokenUnavailable(f"token endpoint {self.token_url}: {exc}") from exc
            if response.status_code in RETRYABLE_STATUS:
                raise _TokenUnavailable(f"token endpoint {self.token_url}: HTTP {response.status_code}")
            if response.is_error:
                raise NodeHistorianError(
                    f"{self.token_url} refused the {self.client_id!r} service account: HTTP "
                    f"{response.status_code}. Check its client and secret (`prekit auth apply`)."
                )
            payload = response.json()
            lifetime = float(payload.get("expires_in", 60))
            self._token = str(payload["access_token"])
            self._token_expires = time.monotonic() + max(0.0, lifetime - min(TOKEN_MARGIN_S, lifetime / 2))
            return self._token

    def _forget_token(self) -> None:
        with self._token_lock:
            self._token = None


class _TokenUnavailable(Exception):
    """The token endpoint could not be reached or was briefly unavailable."""


def _not_served(url: str, signal_id: str) -> NodeHistorianError:
    return NodeHistorianError(
        f"{url} returned no history for signal {signal_id}: this identity may not read it, the "
        "signal does not exist, or the node does not expose its historian (EXPOSE_HISTORIAN)"
    )


def _retry_after(response: httpx.Response) -> float:
    """Seconds ``Retry-After`` asks for, capped at ``RETRY_AFTER_MAX_S``; 0 without one."""
    value = response.headers.get("Retry-After", "")
    try:
        delay = float(value)
    except ValueError:
        try:
            delay = (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return 0.0
    return min(delay, RETRY_AFTER_MAX_S) if math.isfinite(delay) and delay > 0 else 0.0


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat()


def _epoch(value: str) -> float:
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return stamp.timestamp()


def _value(point: Mapping[str, Any]) -> Any:
    """A point's value as the buffer holds it: a number as float, a boolean
    as bool, a string as str, JSON as decoded."""
    value = point.get("value")
    kind = point.get("value_type")
    if value is None:
        return None
    if kind == "number":
        return float(value)
    if kind == "boolean":
        return bool(value)
    if kind == "string":
        return str(value)
    return value


__all__ = ["DEFAULT_PAGE_SIZE", "NodeHistorian", "NodeHistorianError"]
