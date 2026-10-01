import threading

from test_topic_wakeup import A, Client

from chaski import Doorbell
from chaski.topic_wakeup import TopicWakeup


def _waiting(bell, since, stop):
    result: list[bool] = []
    thread = threading.Thread(target=lambda: result.append(bell.wait_after(since, stop=stop)), daemon=True)
    thread.start()
    return thread, result


def test_setting_stop_ends_a_wait_without_a_ring():
    bell, stop = Doorbell(), threading.Event()
    thread, result = _waiting(bell, bell.generation, stop)
    assert not stop.wait(0.1)
    stop.set()
    thread.join(timeout=5)
    assert not thread.is_alive() and result == [False]


def test_a_ring_still_ends_a_wait_that_watches_stop():
    bell, stop = Doorbell(), threading.Event()
    thread, result = _waiting(bell, bell.generation, stop)
    bell.ring()
    thread.join(timeout=5)
    assert result == [True] and not stop.is_set()


def test_a_stop_set_before_the_wait_returns_at_once():
    bell, stop = Doorbell(), threading.Event()
    stop.set()
    assert bell.wait_after(bell.generation, timeout=5, stop=stop) is False


def test_one_stop_shared_by_several_waits_ends_them_all():
    stop = threading.Event()
    bells = [Doorbell(), Doorbell()]
    waits = [_waiting(bell, bell.generation, stop) for bell in bells]
    stop.set()
    for thread, result in waits:
        thread.join(timeout=5)
        assert result == [False]


def test_a_rescope_rings_even_when_it_only_drops_topics():
    wake = TopicWakeup(Client(), [A])
    seen = wake.bell.generation
    wake.rebind([])
    assert wake.bell.generation > seen
