"""Wait for the durable output effect, rather than MQTT callback scheduling."""

import threading
import time


def drained(service, timeout=3):
    deadline = time.monotonic() + timeout
    while service.pending():
        assert time.monotonic() < deadline, service.pending()
        threading.Event().wait(0.005)
