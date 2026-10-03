"""A DataOps service's health door: an HTTP server on the event loop, port 8888.

It runs on the service's own event loop on purpose. The loop schedules every
producer tick and ingest drain, so a loop that answers in time is what the
container wants to know, and a blocked loop fails the probe. Blocking work runs
off the loop (see ``Ingest.run_once`` and ``service.off_loop``).

Standard library only: HTTP/1.1 with ``Connection: close`` and one route,
``/healthz``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from chaski.failures import UNHEALTHY, HandlerHealth

log = logging.getLogger("chaski.dataops.health")

PORT_DEFAULT = 8888
#: In coordinated step mode, no finished step for this long is a stall.
STALL_AFTER_MIN_S = 300.0
#: How long the broker may be away before the probe fails: paho reconnects
#: within seconds of a node restart, so this is a reconnect that never comes.
BROKER_GRACE_S = 60.0


@dataclass
class HealthState:
    """What the service knows about itself, set by ``run()``.

    An ingest task that died with an exception turns the answer into a 503,
    because the service is up but doing nothing. So does the node's
    ``cursor_lag`` finding about this service (``cursor_lag``): records it
    reads have waited unread past the node's threshold, a lost wake or a stuck
    loop. An idle stream is healthy however old its last record is: the ingest
    reads when woken (and walks a silent filter forward now and then), and
    only records that wait count. An
    ingest that never started (nothing resolved yet) is healthy: a fresh node
    waiting to be commissioned. A broker link that stays down for
    ``broker_grace_s`` fails the probe too: without it no command, wake-up or
    watched record arrives. In coordinated step mode ``last_drain_at`` is the
    step loop's, and ``stall_after_s`` without a step is a stall. So does
    another process running as this service (``identity_conflict``): the
    broker hands their one session back and forth and wake-ups get lost.

    ``handlers`` counts failing handlers: while one is retried the answer
    says ``degraded`` and names it; once one failed ``unhealthy_after`` times
    in a row it is a 503.

    Until startup is done (``ready``) the answer is a 503 with ``ingest``
    ``starting`` and ``not_ready`` naming what startup waits for: a node that
    cannot be reached (``cannot reach Colca at ...``), its clock, its
    definitions. The door answers from the first moment, before the node does.
    """

    started_at: float = field(default_factory=time.time)
    ingest_task: asyncio.Task | None = None
    #: Monotonic time of the last finished drain, from the ingest loop.
    last_drain_at: Callable[[], float] | None = None
    waiting: Callable[[], bool] | None = None
    connected: Callable[[], bool] | None = None
    stall_after_s: float = STALL_AFTER_MIN_S
    ready: bool = True
    producers: int = 0
    generation: str = ""
    broker_connected: Callable[[], bool] | None = None
    broker_grace_s: float = BROKER_GRACE_S
    #: The node's cursor_lag finding about this service, "" while none stands.
    cursor_lag: Callable[[], str] | None = None
    #: Another process runs as this service, "" while none does.
    identity_conflict: Callable[[], str] | None = None
    handlers: HandlerHealth | None = None
    #: What startup waits for while not ``ready``.
    not_ready: Callable[[], str] | None = None
    _broker_down_since: float | None = field(default=None, repr=False)

    def _ingest(self) -> str:
        if not self.ready:
            return "starting"
        task = self.ingest_task
        if task is None:
            return "not-started"
        if task.done():
            return "dead"
        if self.connected is not None and not self.connected():
            return "disconnected"
        if self.waiting is not None and self.waiting():
            return "running"
        if self._since_drain() > self.stall_after_s:
            return "stalled"
        return "running"

    def _since_drain(self) -> float:
        if self.last_drain_at is None:
            return 0.0
        return time.monotonic() - self.last_drain_at()

    def _broker(self) -> str:
        if self.broker_connected is None or self.broker_connected():
            self._broker_down_since = None
            return "connected"
        now = time.monotonic()
        if self._broker_down_since is None:
            self._broker_down_since = now
        return "reconnecting" if now - self._broker_down_since < self.broker_grace_s else "down"

    def _lag(self) -> str:
        return self.cursor_lag() if self.cursor_lag is not None else ""

    def _conflict(self) -> str:
        return self.identity_conflict() if self.identity_conflict is not None else ""

    def _handlers(self) -> str:
        return self.handlers.status if self.handlers is not None else "ok"

    def healthy(self) -> bool:
        return (
            self._ingest() in ("not-started", "running")
            and self._broker() != "down"
            and not self._lag()
            and not self._conflict()
            and self._handlers() != UNHEALTHY
        )

    def snapshot(self) -> dict:
        ingest = self._ingest()
        broker = self._broker()
        lag = self._lag()
        conflict = self._conflict()
        handlers = self._handlers()
        body = {
            "ok": ingest in ("not-started", "running")
            and broker != "down"
            and not lag
            and not conflict
            and handlers != UNHEALTHY,
            "ingest": ingest,
            "broker": broker,
            "handlers": handlers,
            "producers": self.producers,
            "generation": self.generation,
            "uptime_s": round(time.time() - self.started_at, 1),
        }
        if not self.ready and self.not_ready is not None:
            body["not_ready"] = self.not_ready()
        if self.ingest_task is not None and self.last_drain_at is not None:
            body["since_drain_s"] = round(self._since_drain(), 1)
        if lag:
            body["cursor_lag"] = lag
        if conflict:
            body["identity_conflict"] = conflict
        if self.handlers is not None and handlers != "ok":
            body["failing"] = {
                name: {"failures": f.failures, "error": f.error, "since": round(f.since, 3)}
                for name, f in self.handlers.failing().items()
            }
        return body


async def serve(state: HealthState, port: int = PORT_DEFAULT) -> asyncio.AbstractServer:
    """Start the health server on the running loop and return it."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.read(2048)  # one small request; the path does not matter
            body = json.dumps(state.snapshot()).encode()
            status = b"200 OK" if state.healthy() else b"503 Service Unavailable"
            writer.write(
                b"HTTP/1.1 " + status + b"\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + body
            )
            await writer.drain()
        except Exception:
            log.debug("health request failed", exc_info=True)
        finally:
            writer.close()

    # Binding all interfaces is the point: the probe (and an operator's
    # curl) reach a container-internal door; nothing publishes 8888.
    server = await asyncio.start_server(handle, "0.0.0.0", port)  # noqa: S104 # nosec B104
    log.info("Health server on :%d (served from the event loop on purpose)", port)
    return server
