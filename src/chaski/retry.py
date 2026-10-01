"""Backoff for failed transport operations; never an idle refresh cadence."""

import math
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from random import SystemRandom

_jitter = SystemRandom()

#: The longest a node's ``Retry-After`` holds a retry back. A larger value
#: (a misconfigured limiter, an HTTP date days ahead) is cut to this, so one
#: bad header cannot park a consumer for hours.
RETRY_AFTER_MAX_S = 60.0

#: Spread added on top of ``Retry-After``: callers refused together must not
#: all come back in the same instant and trip the limit again.
RETRY_AFTER_JITTER = 0.2


def retry_after(error) -> float | None:
    """Seconds a 429 in ``error`` asks the caller to wait, or None."""
    explicit = getattr(error, "retry_after", None)
    if (
        getattr(error, "status", None) == 429
        and isinstance(explicit, (int, float))
        and math.isfinite(explicit)
        and explicit > 0
    ):
        return float(explicit)
    response = getattr(error, "response", None)
    if response is None or response.status_code != 429:
        return None
    value = response.headers.get("Retry-After", "")
    try:
        delay = float(value)
    except ValueError:
        try:
            delay = (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None
    return delay if math.isfinite(delay) and delay > 0 else None


class Backoff:
    """The wait between retries of one failed operation.

    Call :meth:`delay` after each consecutive failure and wait that long;
    call :meth:`reset` after a success. It spaces retries of an operation
    that failed; it is not a cadence for reading a node to find new work.
    """

    def __init__(self, minimum: float = 1.0, maximum: float = 30.0) -> None:
        if not 0 < minimum <= maximum:
            raise ValueError("backoff requires 0 < minimum <= maximum")
        self.minimum, self.maximum, self.failures = minimum, maximum, 0

    def reset(self) -> None:
        """Start again at ``minimum``: the operation succeeded."""
        self.failures = 0

    def delay(self, error: BaseException | None = None) -> float:
        """Seconds to wait before retrying after ``error``.

        Exponential with jitter per consecutive failure. A 429 carrying
        ``Retry-After`` waits at least that long (capped at
        :data:`RETRY_AFTER_MAX_S`, plus up to 20% jitter), and never less
        than the backoff already reached, so repeated refusals still slow
        down instead of retrying at the node's minimum every time.
        """
        self.failures += 1
        ceiling = min(self.maximum, self.minimum * 2 ** min(self.failures - 1, 30))
        delay = _jitter.uniform(ceiling / 2, ceiling)
        asked = retry_after(error)
        if asked is not None:
            floor = min(asked, RETRY_AFTER_MAX_S)
            delay = max(delay, floor * _jitter.uniform(1.0, 1.0 + RETRY_AFTER_JITTER))
        return delay
