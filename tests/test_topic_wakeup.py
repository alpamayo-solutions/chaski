from chaski.topic_wakeup import TopicWakeup


class Client:
    def __init__(self):
        self.subscribed, self.callbacks = {}, {}

    def subscribe(self, topic, qos=0):
        self.subscribed[topic] = qos

    def unsubscribe(self, topic):
        self.subscribed.pop(topic, None)

    def message_callback_add(self, topic, callback):
        self.callbacks[topic] = callback

    def message_callback_remove(self, topic):
        self.callbacks.pop(topic, None)


A = "prekit/v1/_Metric/N/Machine/machine_state"
B = "prekit/v1/_Metric/N/Machine/pressure"


def test_it_subscribes_exactly_the_topics_it_is_given():
    client = Client()
    wake = TopicWakeup(client, [A, B])
    assert client.subscribed == {A: 1, B: 1}
    wake.rebind([A])
    assert client.subscribed == {A: 1}
    assert set(client.callbacks) == {A}


def test_a_message_on_a_topic_rings_and_nothing_else_does():
    client = Client()
    wake = TopicWakeup(client, [A])
    seen = wake.bell.generation
    client.callbacks[A](client, None, object())
    assert wake.bell.generation > seen


def test_new_topics_and_a_reconnect_ring_for_what_arrived_unheard():
    client = Client()
    wake = TopicWakeup(client, [])
    seen = wake.bell.generation
    wake.rebind([A])
    assert wake.bell.generation > seen
    seen = wake.bell.generation
    wake.rebind([A])  # unchanged: nothing new to catch up on
    assert wake.bell.generation == seen
    wake.reconnected()
    assert wake.bell.generation > seen


def test_close_drops_every_subscription():
    client = Client()
    wake = TopicWakeup(client, [A, B])
    wake.close()
    assert client.subscribed == {} and client.callbacks == {}
    wake.rebind([A])
    assert client.subscribed == {}


def _hub_client():
    from chaski.topic_wakeup import TopicFanout

    client = Client()
    return client, TopicFanout(client)


def test_two_wakeups_on_one_topic_both_ring():
    # paho keeps one callback per topic: a second message_callback_add replaced
    # the first, and one consumer of a service silently stopped waking.
    client, fanout = _hub_client()
    first, second = TopicWakeup(fanout, [A]), TopicWakeup(fanout, [A, B])
    seen = first.bell.generation, second.bell.generation
    client.callbacks[A](client, None, object())
    assert first.bell.generation > seen[0] and second.bell.generation > seen[1]


def test_closing_one_wakeup_keeps_the_others_subscription():
    client, fanout = _hub_client()
    first, second = TopicWakeup(fanout, [A]), TopicWakeup(fanout, [A])
    first.close()
    assert client.subscribed == {A: 1}
    seen = second.bell.generation
    client.callbacks[A](client, None, object())
    assert second.bell.generation > seen
    second.close()
    assert client.subscribed == {} and client.callbacks == {}


class Message:
    def __init__(self, topic):
        self.topic = topic


def test_a_wildcard_filter_rings_for_every_topic_it_matches():
    # The fanout looked the message's concrete topic up among the subscribed
    # filters; a filter with + or # never matched and never rang.
    client, fanout = _hub_client()
    scope = "prekit/v1/_Signal/+/Hygentile/#"
    wake, other = TopicWakeup(fanout, [scope]), TopicWakeup(fanout, [A])
    assert client.subscribed == {scope: 1, A: 1}
    for topic in ("prekit/v1/_Signal/edge-2/Hygentile/M2/state", "prekit/v1/_Signal/hub/Hygentile"):
        seen, quiet = wake.bell.generation, other.bell.generation
        client.callbacks[scope](client, None, Message(topic))
        assert wake.bell.generation > seen, topic
        assert other.bell.generation == quiet, topic


def test_a_wildcard_filter_does_not_ring_for_a_topic_outside_it():
    client, fanout = _hub_client()
    scope, plus = "prekit/v1/_Signal/+/Hygentile/#", "prekit/v1/_Metric/+/M/state"
    wake = TopicWakeup(fanout, [scope, plus])
    seen = wake.bell.generation
    for topic in ("prekit/v1/_Signal/edge-2/Other/M2/state", "prekit/v1/_Metric/N/M/sub/state"):
        client.callbacks[scope](client, None, Message(topic))
    assert wake.bell.generation == seen
    client.callbacks[plus](client, None, Message("prekit/v1/_Metric/N/M/state"))
    assert wake.bell.generation > seen
