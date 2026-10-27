"""Pipelined writes (:mod:`chaski.publishing`): sent at once, confirmed together."""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time

import pytest
from dataops_fakes import AckingClient
from franzmq.errors import PublishRejected, PublishTimeout

from chaski.publishing import Pubacks, PublishBatch, Unconfirmed, settle_current
from chaski.service import Service


@pytest.fixture
def client():
    c = AckingClient(latency=0.005)
    try:
        yield c
    finally:
        c.close()


def test_a_batch_sends_every_write_before_any_is_confirmed_and_settle_waits_for_all(client):
    acks = Pubacks(client)
    batch = PublishBatch(window=100)
    started = time.monotonic()
    with batch.active():
        for i in range(50):
            batch.add(acks.send(f"t/{i}", "{}"))
    handed_over = time.monotonic() - started
    batch.settle()

    assert len(client.sent) == 50 and client.waited == 0
    # 50 round trips one after the other take 250 ms; handing them over does not.
    assert handed_over < 0.1
    assert len(batch) == 0


def test_settle_raises_the_refusal_after_waiting_for_the_rest_and_names_who_wrote_it(client):
    client.refuse = {"t/refused"}
    acks = Pubacks(client)
    batch = PublishBatch()
    with batch.active():
        batch.origin = "first"
        batch.add(acks.send("t/ok", "{}"))
        batch.origin = "second"
        batch.add(acks.send("t/refused", "{}"))
        batch.origin = "third"
        third = acks.send("t/later", "{}")
        batch.add(third)

    with pytest.raises(PublishRejected) as refused:
        batch.settle()

    assert refused.value.reason_code == 0x99 and refused.value.topic == "t/refused"
    assert batch.failed_origin == "second"
    assert third.done and len(batch) == 0


def test_a_write_without_a_puback_times_out(client):
    client.silent = {"t/lost"}
    client.publish_timeout = 0.05
    acks = Pubacks(client)
    batch = PublishBatch()
    with batch.active():
        batch.add(acks.send("t/lost", "{}"))
    with pytest.raises(PublishTimeout):
        batch.settle()
    assert acks._waiting == {}


def test_a_full_window_waits_for_the_oldest_write(client):
    client.latency = 0.02
    acks = Pubacks(client)
    batch = PublishBatch(window=4)
    with batch.active():
        for i in range(4):
            batch.add(acks.send(f"t/{i}", "{}"))
        assert len(batch) == 4
        batch.add(acks.send("t/4", "{}"))
        # The fifth write waited for the first.
        assert len(batch) <= 4
    batch.settle()


def test_a_puback_that_arrives_before_the_write_is_registered_still_counts():
    class Instant:
        publish_timeout = 1.0
        on_publish = None

        def publish(self, topic, payload, qos=0, retain=False, wait=True):
            # The network thread read the PUBACK before publish() returned.
            self.on_publish(self, None, 7, 0x87, None)
            return type("Sent", (), {"mid": 7})()

    client = Instant()
    acks = Pubacks(client)
    pending = acks.send("t/x", "{}")
    assert pending.done
    with pytest.raises(PublishRejected):
        pending.wait()


def test_only_the_task_that_opened_a_batch_writes_into_it(client):
    acks = Pubacks(client)

    async def main():
        batch = PublishBatch()
        with batch.active():
            assert PublishBatch.current() is batch

            async def elsewhere():
                return PublishBatch.current()

            # A task the handler starts outlives the page: it waits for its own writes.
            assert await asyncio.ensure_future(elsewhere()) is None
            assert await asyncio.to_thread(PublishBatch.current) is None
            batch.add(acks.send("t/x", "{}"))
        assert PublishBatch.current() is None
        await asyncio.to_thread(batch.settle)

    asyncio.run(main())


def test_settle_current_settles_the_open_batch_only(client):
    acks = Pubacks(client)
    settle_current()  # nothing open: nothing to do
    batch = PublishBatch()
    with batch.active():
        batch.add(acks.send("t/x", "{}"))
        settle_current()
        assert len(batch) == 0


def test_service_writes_are_pipelined_inside_a_batch_and_waited_for_outside(client, tmp_path):
    svc = Service("svc", state_dir=tmp_path)
    svc._client = client
    svc._pubacks = Pubacks(client)

    svc.send("t/waited", "{}")
    assert client.waited == 1

    with svc.pipelined():
        svc.send("t/a", "{}")
        svc.retract("t/b")
        assert client.waited == 1
    assert client.waited == 1
    assert [topic for topic, _ in client.sent] == ["t/waited", "t/a", "t/b"]


def test_the_pipelined_block_raises_a_refusal_when_it_ends(client, tmp_path):
    client.refuse = {"t/refused"}
    svc = Service("svc", state_dir=tmp_path)
    svc._client = client
    svc._pubacks = Pubacks(client)

    with pytest.raises(PublishRejected), svc.pipelined():
        svc.send("t/refused", "{}")


def test_a_service_publish_finds_its_bindings_by_tag_without_a_scan(tmp_path):
    from colca_data_contracts.payload import Signal as SignalRecord
    from franzmq import Topic

    from chaski.service import Binding

    svc = Service("svc", state_dir=tmp_path)
    for i in range(3):
        signal = SignalRecord(id=f"sig-{i}", name=f"s{i}", data_tag=f"tag-{i % 2}")
        svc._set_binding(
            f"colca/v1/_Signal/n/x/{i}", Binding(f"tag-{i % 2}", Topic.from_str("colca/v1/_Metric/n/x"), signal)
        )
    assert [b.signal.id for b in svc._bindings_for("tag-0")] == ["sig-0", "sig-2"]
    svc._set_binding("colca/v1/_Signal/n/x/0", None)
    assert [b.signal.id for b in svc._bindings_for("tag-0")] == ["sig-2"]
    assert svc._bindings_for("tag-9") == []


def test_settling_from_another_thread_does_not_lose_a_puback(client):
    client.latency = 0.0
    acks = Pubacks(client)
    batch = PublishBatch(window=1000)
    sent = threading.Event()

    def write():
        with batch.active():
            for i in range(500):
                batch.add(acks.send(f"t/{i}", "{}"))
        sent.set()

    thread = threading.Thread(target=write)
    thread.start()
    thread.join()
    assert sent.is_set()
    batch.settle()


def test_franzmqs_client_hands_each_pubacks_reason_code_to_on_publish():
    """The verdict on a pipelined write comes from the public callback; the
    client's own PUBACK hook must keep calling it."""
    from franzmq import Client
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.reasoncodes import ReasonCode

    client = Client(client_id="t")
    acks = Pubacks(client)
    pending = Unconfirmed(acks, 5, "t/x", 1.0)
    acks._waiting[5] = pending
    # paho then drops the message from its store; this client never sent one.
    with contextlib.suppress(KeyError):
        client._do_on_publish(5, ReasonCode(PacketTypes.PUBACK, identifier=0x87), None)
    assert pending.done
    with pytest.raises(PublishRejected):
        pending.wait()
