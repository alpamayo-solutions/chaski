"""Backoff for failed transport operations; never an idle refresh cadence."""

import math
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from random import SystemRandom

_jitter = SystemRandom()


class Backoff:
    def __init__(self, minimum=1.0, maximum=30.0):
        if not 0 < minimum <= maximum:
            raise ValueError("backoff requires 0 < minimum <= maximum")
        self.minimum, self.maximum, self.failures = minimum, maximum, 0

    def reset(self):
        self.failures = 0

    def delay(self, error=None):
        self.failures += 1
        explicit = getattr(error, "retry_after", None)
        if (
            getattr(error, "status", None) == 429
            and isinstance(explicit, (int, float))
            and math.isfinite(explicit)
            and explicit > 0
        ):
            return explicit
        response = getattr(error, "response", None)
        if response is not None and response.status_code == 429:
            value = response.headers.get("Retry-After", "")
            try:
                delay = float(value)
            except ValueError:
                try:
                    delay = (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds()
                except (TypeError, ValueError, OverflowError):
                    delay = 0
            if math.isfinite(delay) and delay > 0:
                return delay
        ceiling = min(self.maximum, self.minimum * 2 ** min(self.failures - 1, 30))
        return _jitter.uniform(ceiling / 2, ceiling)
