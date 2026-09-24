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

log = logging.getLogger("chaski.dataops.health")

PORT_DEFAULT = 8888
#: No finished drain for this long, or ten poll intervals if longer, is a stall.
#: Transport errors back off to at most 30 s, so a node restart stays under it.
STALL_AFTER_MIN_S = 300.0


@dataclass
class HealthState:
    """What the service knows about itself, set by ``run()``.

    An ingest task that died with an exception turns the answer into a 503,
    because the service is up but doing nothing. So does one that is alive but
    has not finished a drain for ``stall_after_s``: a healthy loop drains at
    least once per poll interval, even with nothing to read. An ingest that
    never started (nothing resolved yet) is healthy: a fresh node waiting to be
    commissioned.
    """

    started_at: float = field(default_factory=time.time)
    ingest_task: asyncio.Task | None = None
    #: Monotonic time of the last finished drain, from the ingest loop.
    last_drain_at: Callable[[], float] | None = None
    stall_after_s: float = STALL_AFTER_MIN_S
    producers: int = 0
    generation: str = ""

    def _ingest(self) -> str:
        task = self.ingest_task
        if task is None:
            return "not-started"
        if task.done():
            return "dead"
        if self._since_drain() > self.stall_after_s:
            return "stalled"
        return "running"

    def _since_drain(self) -> float:
        if self.last_drain_at is None:
            return 0.0
        return time.monotonic() - self.last_drain_at()

    def healthy(self) -> bool:
        return self._ingest() in ("not-started", "running")

    def snapshot(self) -> dict:
        ingest = self._ingest()
        body = {
            "ok": ingest in ("not-started", "running"),
            "ingest": ingest,
            "producers": self.producers,
            "generation": self.generation,
            "uptime_s": round(time.time() - self.started_at, 1),
        }
        if self.ingest_task is not None and self.last_drain_at is not None:
            body["since_drain_s"] = round(self._since_drain(), 1)
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
