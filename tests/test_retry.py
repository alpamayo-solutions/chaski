from types import SimpleNamespace

from chaski.retry import Backoff


def test_retry_after_from_http_and_projector_is_not_capped():
    backoff = Backoff()
    assert backoff.delay(SimpleNamespace(status=429, retry_after=90)) == 90
    assert (
        backoff.delay(SimpleNamespace(response=SimpleNamespace(status_code=429, headers={"Retry-After": "120"}))) == 120
    )


def test_invalid_retry_after_uses_bounded_jitter():
    for value in ("NaN", "inf", "invalid", "-1"):
        backoff = Backoff()
        delay = backoff.delay(
            SimpleNamespace(response=SimpleNamespace(status_code=429, headers={"Retry-After": value}))
        )
        assert 0.5 <= delay <= 1
