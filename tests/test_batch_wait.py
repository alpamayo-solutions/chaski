"""``BatchWait.wait`` is bounded: a ring that never comes does not stall the
consumer, and the result says why the wait ended."""

import inspect
import threading
import time

from chaski._wakeup import Wakeup
from chaski.doorbell import IDLE_DRAIN_S
from chaski.stream_changes import BatchWait, Woke


def _waiting(batch, version, **kwargs):
    result: list[Woke] = []
    thread = threading.Thread(target=lambda: result.append(batch.wait(version, **kwargs)), daemon=True)
    thread.start()
    return thread, result


def test_a_wait_without_a_ring_times_out_within_the_bound():
    batch = BatchWait(Wakeup(), interval=0)
    started = time.monotonic()
    thread, result = _waiting(batch, batch.version, timeout=0.05)
    thread.join(timeout=5)
    assert result == [Woke.TIMED_OUT]
    assert 0.05 <= time.monotonic() - started < 2


def test_a_ring_ends_the_wait_as_rang():
    signal = Wakeup()
    batch = BatchWait(signal, interval=0)
    thread, result = _waiting(batch, batch.version, timeout=5)
    signal.notify()
    thread.join(timeout=5)
    assert result == [Woke.RANG]


def test_a_ring_before_the_wait_is_not_lost():
    signal = Wakeup()
    batch = BatchWait(signal, interval=0)
    version = batch.version
    signal.notify()
    assert batch.wait(version, timeout=5) is Woke.RANG


def test_the_plain_call_is_bounded_by_the_documented_default():
    default = inspect.signature(BatchWait.wait).parameters["timeout"].default
    assert default == IDLE_DRAIN_S
    assert 0 < IDLE_DRAIN_S < float("inf")
    # The call every consumer written before the bound makes still works.
    signal = Wakeup()
    batch = BatchWait(signal, interval=0)
    version = batch.version
    signal.notify()
    assert batch.wait(version) is Woke.RANG


def test_setting_stop_ends_a_wait_that_asked_for_no_bound():
    stop = threading.Event()
    batch = BatchWait(Wakeup(), interval=0, stop=stop)
    thread, result = _waiting(batch, batch.version, timeout=None)
    assert not stop.wait(0.1)
    assert thread.is_alive(), "timeout=None waits for the ring alone"
    stop.set()
    thread.join(timeout=5)
    assert result == [Woke.STOPPED]


def test_only_a_resumed_subscription_is_true_as_the_bool_was():
    signal, resume = Wakeup(), Wakeup()
    batch = BatchWait(signal, interval=0)
    since = resume.version
    assert not batch.wait(batch.version, timeout=0.01)  # timed out
    assert batch.wait(batch.version, retry=0.01, resume=resume, resume_since=since) is Woke.TIMED_OUT
    resume.notify()
    woke = batch.wait(batch.version, retry=30, resume=resume, resume_since=since)
    assert woke is Woke.RESUMED and woke
    version = batch.version
    signal.notify()
    assert not batch.wait(version, timeout=5)  # rang, which the bool never reported
    assert [bool(member) for member in Woke] == [False, False, True, False]
