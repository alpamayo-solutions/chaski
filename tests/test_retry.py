from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace

import httpx
import pytest

from chaski.retry import RETRY_AFTER_MAX_S, Backoff


def _refused(status, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    request = httpx.Request("GET", "http://colca/fetch")
    return httpx.HTTPStatusError(
        str(status), request=request, response=httpx.Response(status, headers=headers, request=request)
    )


def test_429_waits_at_least_retry_after_with_jitter():
    delays = [Backoff().delay(_refused(429, "5")) for _ in range(200)]
    assert all(5 <= delay <= 6 for delay in delays)
    # Callers refused together must not all come back at the same instant.
    assert len(set(delays)) > 1


def test_projector_retry_after_is_honoured_like_the_header():
    delays = [Backoff().delay(SimpleNamespace(status=429, retry_after=5)) for _ in range(50)]
    assert all(5 <= delay <= 6 for delay in delays)


def test_retry_after_does_not_shorten_the_backoff_of_repeated_refusals():
    backoff = Backoff(minimum=1.0, maximum=30.0)
    for _ in range(5):
        backoff.delay(_refused(429, "1"))
    # The sixth refusal in a row backs off 15-30 s, not the node's 1 s.
    assert 15 <= backoff.delay(_refused(429, "1")) <= 30


def test_retry_after_beyond_the_backoff_maximum_is_honoured():
    delays = [Backoff(maximum=30.0).delay(_refused(429, "45")) for _ in range(50)]
    assert all(45 <= delay <= 54 for delay in delays)


# Named ids: an id carrying the date would differ between the processes of a parallel run.
@pytest.mark.parametrize(
    "value",
    ["3600", format_datetime(datetime.now(UTC) + timedelta(days=2), usegmt=True)],
    ids=["seconds", "http-date"],
)
def test_absurd_retry_after_is_capped(value):
    delays = [Backoff().delay(_refused(429, value)) for _ in range(50)]
    assert all(RETRY_AFTER_MAX_S <= delay <= RETRY_AFTER_MAX_S * 1.2 for delay in delays)


def test_http_date_retry_after_is_honoured():
    when = format_datetime(datetime.now(UTC) + timedelta(seconds=20), usegmt=True)
    assert 15 <= Backoff().delay(_refused(429, when)) <= 24


def test_429_without_retry_after_uses_the_normal_backoff():
    backoff = Backoff()
    assert 0.5 <= backoff.delay(_refused(429)) <= 1
    assert 1 <= backoff.delay(_refused(429)) <= 2


def test_invalid_retry_after_uses_bounded_jitter():
    for value in ("NaN", "inf", "invalid", "-1", "0"):
        assert 0.5 <= Backoff().delay(_refused(429, value)) <= 1


def test_retry_after_on_other_statuses_is_ignored():
    for status in (500, 503):
        backoff = Backoff()
        assert 0.5 <= backoff.delay(_refused(status, "20")) <= 1
    assert 0.5 <= Backoff().delay(httpx.ConnectError("refused")) <= 1
    assert 0.5 <= Backoff().delay() <= 1
