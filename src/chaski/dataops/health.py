"""A DataOps service's health door: an HTTP server ON the event loop, port 8888.

Every Colca Python service answers health on 8888. The shipped dataops
service once had no server at all — its container healthcheck spawned a
fresh interpreter that imported the whole app and exited: it tested nothing
about the RUNNING service, and under load the import alone exceeded the
probe's own timeout. A check that cannot see a real failure and can report
a false one is worse than none.

This one is deliberately served from the service's own event loop, with no
thread to hide behind: the loop is the thing that schedules every producer
tick and every ingest drain, so "the loop answered within the probe's
timeout" is the very fact the container wants to know. A wedged or blocked
loop fails the probe honestly. (The blocking work itself — door HTTP,
sqlite — runs OFF the loop; see ``Ingest.run_once`` and
``service.off_loop``. This server is how that stays true.)

Stdlib only, HTTP/1.1 with ``Connection: close`` — one tiny GET, no routes
beyond ``/healthz``, no framework for a service that otherwise needs none.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field

log = logging.getLogger("chaski.dataops.health")

PORT_DEFAULT = 8888


@dataclass
class HealthState:
    """What the service knows about itself, written by ``run()``'s wiring.

    ``ingest_task`` is the one thing that can make the answer a 503 while
    the loop still runs: the ingest loop is the service's single data lane,
    so a task that DIED (done, with an exception) means the service is up
    but doing nothing — the container should say so. An ingest that was
    never started (nothing resolved yet) is healthy: that is a fresh node
    waiting to be commissioned, not a failure.
    """

    started_at: float = field(default_factory=time.time)
    ingest_task: asyncio.Task | None = None
    producers: int = 0
    generation: str = ""

    def healthy(self) -> bool:
        task = self.ingest_task
        return task is None or not task.done()

    def snapshot(self) -> dict:
        task = self.ingest_task
        if task is None:
            ingest = "not-started"
        elif not task.done():
            ingest = "running"
        else:
            ingest = "dead"
        return {
            "ok": self.healthy(),
            "ingest": ingest,
            "producers": self.producers,
            "generation": self.generation,
            "uptime_s": round(time.time() - self.started_at, 1),
        }


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
