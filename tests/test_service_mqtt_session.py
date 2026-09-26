"""The MQTT session outlives what the broker does to it: a packet it cannot
parse, a restart that forgets every subscription, a refused SUBACK. When the
network thread dies anyway, the process goes with it."""

from __future__ import annotations

import os
import socket
import struct
import threading
import time

import franzmq
import paho.mqtt.client as pahomqtt
from paho.mqtt.client import MQTTMessage

from chaski.service import guard_network_thread, tolerate_undecodable
from chaski.subscriptions import RETRY_S, Subscriptions

CONNACK = bytes([0x20, 0x03, 0x00, 0x00, 0x00])


def _remaining_length(length: int) -> bytes:
    out = b""
    while True:
        byte, length = length % 128, length // 128
        out += bytes([byte | (0x80 if length else 0)])
        if not length:
            return out


def _misframed_publish(topic: str) -> bytes:
    """A PUBLISH whose topic is longer than MQTT allows, framed the way a broker
    that writes the length as a plain uint16 frames it: the length wraps, and
    the rest of the topic is read as the properties."""
    encoded = topic.encode()
    body = struct.pack("!H", len(encoded) & 0xFFFF) + encoded + b"\x00" + b"{}"
    return bytes([0x30]) + _remaining_length(len(body)) + body


def _read_packet(conn: socket.socket) -> tuple[int, bytes] | None:
    header = conn.recv(1)
    if not header:
        return None
    length, shift = 0, 0
    while True:
        byte = conn.recv(1)[0]
        length |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            break
    body = b""
    while len(body) < length:
        chunk = conn.recv(length - len(body))
        if not chunk:
            return None
        body += chunk
    return header[0], body


class _Broker:
    """An MQTT 5 broker just big enough for these tests. Every connection gets
    a fresh session. ``first_connection`` runs on the first one only."""

    def __init__(self, first_connection=None, suback_codes=None) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.connections: list[socket.socket] = []
        self.subscribed: list[list[tuple[str, int]]] = []
        self.suback_codes = list(suback_codes or [])
        self.changed = threading.Condition()
        self._first_connection = first_connection
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self.listener.accept()
            except OSError:
                return
            with self.changed:
                self.connections.append(conn)
                self.subscribed.append([])
                index = len(self.connections) - 1
                self.changed.notify_all()
            threading.Thread(target=self._session, args=(conn, index), daemon=True).start()

    def _session(self, conn: socket.socket, index: int) -> None:
        try:
            while (packet := _read_packet(conn)) is not None:
                kind, body = packet
                if kind >> 4 == 1:  # CONNECT
                    conn.sendall(CONNACK)
                    if index == 0 and self._first_connection is not None:
                        self._first_connection(conn)
                elif kind >> 4 == 8:  # SUBSCRIBE
                    self._subscribe(conn, index, body)
        except OSError:
            return

    def _subscribe(self, conn: socket.socket, index: int, body: bytes) -> None:
        packet_id = body[:2]
        pos = 3  # an empty property block
        granted = []
        while pos < len(body):
            (length,) = struct.unpack("!H", body[pos : pos + 2])
            topic = body[pos + 2 : pos + 2 + length].decode()
            qos = body[pos + 2 + length] & 0x03
            pos += 3 + length
            with self.changed:
                self.subscribed[index].append((topic, qos))
                self.changed.notify_all()
            granted.append(self.suback_codes.pop(0) if self.suback_codes else qos)
        suback = packet_id + b"\x00" + bytes(granted)
        conn.sendall(bytes([0x90]) + _remaining_length(len(suback)) + suback)

    def wait(self, predicate, timeout: float = 10.0) -> bool:
        with self.changed:
            return self.changed.wait_for(predicate, timeout)

    def drop(self, index: int) -> None:
        self.connections[index].shutdown(socket.SHUT_RDWR)
        self.connections[index].close()

    def close(self) -> None:
        self.listener.close()
        for conn in self.connections:
            conn.close()


def _client(broker: _Broker, name: str) -> franzmq.Client:
    client = franzmq.Client(client_id=name, protocol=pahomqtt.MQTTv5)
    client.reconnect_delay_set(min_delay=1, max_delay=1)
    guard_network_thread(client, name)
    return client


def test_an_unparseable_packet_reconnects_instead_of_killing_the_thread():
    broker = _Broker(
        first_connection=lambda conn: conn.sendall(_misframed_publish("colca/v1/_Constant/n/" + "X" * 70000))
    )
    client = _client(broker, "guard-test")
    client.connect("127.0.0.1", broker.port)
    client.loop_start()
    try:
        assert broker.wait(lambda: len(broker.connections) >= 2), "the client never reconnected after the bad packet"
        assert client._thread is not None and client._thread.is_alive()
    finally:
        client.loop_stop()
        broker.close()


def test_a_broker_that_forgot_the_session_gets_every_subscription_back():
    broker = _Broker()
    client = _client(broker, "restore-test")
    subscriptions = Subscriptions(client)
    received: list[str] = []
    connects: list[bool] = []

    def on_connect(_c, _u, flags, _rc, _p=None) -> None:
        if connects:
            subscriptions.restore()
        connects.append(flags.session_present)

    client.on_connect = on_connect
    client.connect("127.0.0.1", broker.port)
    client.loop_start()
    try:
        assert broker.wait(lambda: len(broker.connections) == 1)
        client.subscribe("colca/v1/_CmdParam/n/line1/operator/setProduct", qos=1, callback=lambda m: received.append(m))
        client.message_callback_add("colca/v1/_Metric/n/line1/temp", lambda *_: None)
        client.subscribe("colca/v1/_Metric/n/line1/temp", qos=0)
        client.subscribe("colca/v1/_Constant/n/gone", qos=1)
        client.unsubscribe("colca/v1/_Constant/n/gone")
        assert broker.wait(lambda: len(broker.subscribed[0]) == 3)

        broker.drop(0)  # the node restarts

        expected = {("colca/v1/_CmdParam/n/line1/operator/setProduct", 1), ("colca/v1/_Metric/n/line1/temp", 0)}
        assert broker.wait(lambda: len(broker.subscribed) == 2 and set(broker.subscribed[1]) == expected)
        assert connects == [False, False]
    finally:
        client.loop_stop()
        broker.close()


def test_a_refused_subscription_is_sent_again():
    """mochi refuses a SUBSCRIBE whose id collides with one of its in-flight
    deliveries ("packet identifier in use", 0x91)."""
    broker = _Broker(suback_codes=[0x91])
    client = _client(broker, "refused-test")
    Subscriptions(client)
    client.connect("127.0.0.1", broker.port)
    client.loop_start()
    try:
        assert broker.wait(lambda: len(broker.connections) == 1)
        started = time.monotonic()
        client.subscribe("colca/v1/_CmdParam/n/line1/operator/setRecipe", qos=1)
        assert broker.wait(lambda: len(broker.subscribed[0]) == 2)
        assert time.monotonic() - started >= RETRY_S * 0.9
    finally:
        client.loop_stop()
        broker.close()


def test_the_process_exits_when_the_network_thread_dies(monkeypatch):
    exits: list[int] = []
    monkeypatch.setattr(os, "_exit", exits.append)
    client = franzmq.Client(client_id="guard-exit", protocol=pahomqtt.MQTTv5)

    def dies() -> None:
        raise RuntimeError("network thread bug")

    client._thread_main = dies
    guard_network_thread(client, "guard-exit")
    client._thread_main()

    assert exits == [70]


def test_a_normal_loop_stop_does_not_exit(monkeypatch):
    exits: list[int] = []
    monkeypatch.setattr(os, "_exit", exits.append)
    client = franzmq.Client(client_id="guard-stop", protocol=pahomqtt.MQTTv5)
    client._thread_main = lambda: None
    guard_network_thread(client, "guard-stop")
    client._thread_main()

    assert exits == []


def test_a_failing_callback_drops_the_message_not_the_thread():
    topic = "colca/v1/_Metric/n1/line1/temp"
    client = franzmq.Client(client_id="callback-test")
    seen: list[bytes] = []

    def callback(_c, _u, message) -> None:
        seen.append(message.payload)
        if message.payload == b"bad":
            raise ValueError("handler bug")

    client.message_callback_add(topic, callback)
    tolerate_undecodable(client)

    for payload in (b"bad", b"good"):
        message = MQTTMessage(mid=1, topic=topic.encode())
        message.payload = payload
        client._handle_on_message(message)

    assert seen[-1] == b"good"
