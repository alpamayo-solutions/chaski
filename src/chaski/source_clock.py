"""How far a data source's clock is off, from the timestamps it stamps.

A connector with ``timestamp_source="source"`` writes each sample at the
source's own time (``Reading.timestamp``). Nothing else checks that clock: a
PLC two seconds ahead puts every sample two seconds into the future.
:class:`SourceClock` compares each source timestamp with the time the
connector received the reading, on the node's clock::

    skew = source_ts - received_at

**Delay is not skew.** ``received_at`` is taken after the driver's read
returned, so for a source whose clock is right ``skew = -delay <= 0``: the
time between the source observing the value and the connector receiving it
(the poll phase, transport, the source's own buffering). With an offset
``o``, ``skew = o - delay``, and ``delay >= 0`` always. So the largest skew
seen is the delay-free estimate: ``o >= max(skew)``, and the two are equal
as soon as one reading arrived without delay. Averages and medians of the
raw skews would report the delay as a clock error; the maximum does not.

**Only new observations count.** A source that reports a value with the time
it last changed (MTConnect ``current``) returns the same timestamp poll after
poll, hours old for a steady value. Such a repeat is not an observation made
now, so a reading counts only when its timestamp differs from the previous
one seen for its topic. The first reading of a topic has nothing to compare
with and counts neither way.

**Only live timestamps count.** A driver whose timestamps name the time the
data refers to rather than the moment the source saw it (energy slots
delivered after the slot ends, batches, backfill: EnEffCo) has a delay of at
least one slot on every reading, and no maximum removes that. Such a driver
is never measured. A driver opts in with ``Driver.source_timestamps =
"live"``; the default, ``"past"``, is never measured.

**The window.** Fresh readings fall into one-minute buckets on the node clock;
a bucket keeps the largest skew it saw and four counters. Buckets older than
``window`` (15 minutes) are dropped: memory is fixed, whatever the reading
rate. Over the buckets that hold a measurement the highest and the lowest
estimate are discarded (one bad minute each way), and of the rest:

* ``skew_s`` is the largest: the delay-free estimate, robust to one minute
  with a glitched timestamp;
* ``jitter_s`` is the largest minus the smallest: how far the per-minute
  estimate moves inside the window. A stepping or drifting source clock
  shows here, and so does a source that changes rarely (each minute's
  estimate then carries that minute's smallest delay).

Both need ``min_minutes`` (3) minutes with a measurement; until then they are
None, and so is a source that never stamps.

**Implausible timestamps** are counted as ``invalid`` and kept out of the
estimate: one before 2000-01-01 (an unset PLC clock, epoch 0), one more than
``plausible`` (one day) ahead of the node, and a new one more than a day
behind it. A repeated old timestamp is a steady value, not an invalid one.

**Limits.** The estimate can only be too low, never too high: a source whose
readings all arrive late reads behind by its smallest delay. With a source
that changes often (several times per poll interval) that delay is a few
milliseconds; a source that changes about once a minute reads up to one poll
interval behind. A source that does not change for several minutes is not
measured at all. A node clock that is itself off shifts every estimate by
its own error: the node's clock alarms say which one is wrong.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import floor, isfinite

#: 2000-01-01T00:00:00Z. A source timestamp before it is an unset clock.
EARLIEST_PLAUSIBLE = 946_684_800.0


@dataclass(frozen=True)
class SourceClockStatus:
    """The source clock over the window, as :meth:`SourceClock.status` gives it.

    ``skew_s`` and ``jitter_s`` are seconds, positive when the source is
    ahead of the node; None when there is no estimate (nothing stamped, a
    driver that is not ``live``, too few minutes with a new timestamp). The
    counters are readings received in the window: ``stamped`` carried a
    source timestamp, ``unstamped`` did not (written at the receive time),
    ``invalid`` carried an implausible one, ``measured`` fed the estimate.
    """

    skew_s: float | None
    jitter_s: float | None
    stamped: int
    unstamped: int
    invalid: int
    measured: int
    minutes: int
    live: bool
    window_s: float


class _Bucket:
    __slots__ = ("invalid", "max_skew", "measured", "stamped", "unstamped")

    def __init__(self) -> None:
        self.max_skew: float | None = None
        self.measured = 0
        self.stamped = 0
        self.unstamped = 0
        self.invalid = 0


class SourceClock:
    """A bounded rolling estimate of one connector's source clock offset.

    Not thread-safe; the connector's poll loop is its only caller. Memory is
    one bucket per minute of ``window`` plus the last timestamp per topic
    (one float per bound signal, like the connector's own dedup state).
    """

    def __init__(
        self,
        *,
        live: bool,
        window: float = 900.0,
        bucket: float = 60.0,
        min_minutes: int = 3,
        plausible: float = 86_400.0,
    ) -> None:
        if window <= 0 or bucket <= 0 or window < bucket:
            raise ValueError("window and bucket must be positive, window at least one bucket")
        if min_minutes < 3:
            # One minute is discarded at each end; at least one must remain.
            raise ValueError("min_minutes must be at least 3")
        self.live = bool(live)
        self.window = float(window)
        self.bucket = float(bucket)
        self.min_minutes = int(min_minutes)
        self.plausible = float(plausible)
        self._buckets: dict[int, _Bucket] = {}
        self._last_stamp: dict[str, float] = {}

    def _bucket_at(self, received_at: float) -> _Bucket:
        key = floor(received_at / self.bucket)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = self._buckets[key] = _Bucket()
            self._expire(received_at)
        return bucket

    def _expire(self, now: float) -> None:
        oldest = floor((now - self.window) / self.bucket)
        for key in [k for k in self._buckets if k <= oldest]:
            del self._buckets[key]

    def observe(self, topic: str, source_ts: float | None, received_at: float) -> None:
        """One reading: ``source_ts`` its source timestamp (None when the
        driver did not stamp it), ``received_at`` the node's wall clock when
        the read returned."""
        bucket = self._bucket_at(received_at)
        if source_ts is None:
            bucket.unstamped += 1
            return
        bucket.stamped += 1
        previous = self._last_stamp.get(topic)
        if not isfinite(source_ts) or source_ts < EARLIEST_PLAUSIBLE:
            bucket.invalid += 1
            return
        skew = source_ts - received_at
        self._last_stamp[topic] = source_ts
        if skew > self.plausible:
            bucket.invalid += 1
            return
        if previous is None or source_ts == previous:
            return  # nothing to compare with, or a repeat: not observed now
        if skew < -self.plausible:
            bucket.invalid += 1
            return
        if not self.live:
            return
        bucket.measured += 1
        if bucket.max_skew is None or skew > bucket.max_skew:
            bucket.max_skew = skew

    def forget(self, keep: set[str]) -> None:
        """Drop the last timestamp of every topic not in ``keep`` (no longer bound)."""
        for topic in [t for t in self._last_stamp if t not in keep]:
            del self._last_stamp[topic]

    def status(self, now: float) -> SourceClockStatus:
        """The window ending at ``now`` (node wall clock)."""
        self._expire(now)
        buckets = list(self._buckets.values())
        estimates = sorted(b.max_skew for b in buckets if b.max_skew is not None)
        skew = jitter = None
        if len(estimates) >= self.min_minutes:
            kept = estimates[1:-1]
            skew, jitter = kept[-1], kept[-1] - kept[0]
        return SourceClockStatus(
            skew_s=skew,
            jitter_s=jitter,
            stamped=sum(b.stamped for b in buckets),
            unstamped=sum(b.unstamped for b in buckets),
            invalid=sum(b.invalid for b in buckets),
            measured=sum(b.measured for b in buckets),
            minutes=len(estimates),
            live=self.live,
            window_s=self.window,
        )
