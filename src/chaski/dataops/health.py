"""A DataOps service's health door: an HTTP server on port 8888.

It answers from its own thread, never from the event loop. The loop runs every
``@on_metric`` handler, record by record, so while the ingest lane catches up
a long backlog it is busy for seconds at a time (a handler that waits for its
producer's lock behind a long tick, one that writes its checkpoint). A door on
the loop could not answer then, and the container's probe (4 s) read a
service that was working through its backlog as one that was dead.

Whether the loop moves is still the door's question, asked without needing
the loop to answer it: a beat on the loop (:func:`serve` starts it) stamps
the time every ``LOOP_BEAT_S``. A loop late by ``loop_busy_s`` is busy, which
the body reports as ``degraded`` (200; it is catching up); one that has not
beaten for ``stall_after_s`` is stalled (503), as an ingest that finished no
drain for that long is.

Standard library only: HTTP/1.1 with ``Connection: close``; every path
answers like ``/healthz``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from chaski.failures import UNHEALTHY, HandlerHealth

log = logging.getLogger("chaski.dataops.health")

PORT_DEFAULT = 8888
#: An ingest busy (not waiting) without finishing a drain for this long is a stall.
STALL_AFTER_MIN_S = 300.0
#: How long the broker may be away before the probe fails: paho reconnects
#: within seconds of a node restart, so this is a reconnect that never comes.
BROKER_GRACE_S = 60.0
#: How often the beat on the event loop stamps the time.
LOOP_BEAT_S = 0.5
#: A beat this late means the loop is busy: degraded, not failed.
LOOP_BUSY_S = 2.0


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
    watched record arrives. An ingest that is busy, not waiting, and has not
    finished a drain for ``stall_after_s`` is stalled. So does
    another process running as this service (``identity_conflict``): the
    broker hands their one session back and forth and wake-ups get lost.

    ``handlers`` counts failing handlers: while one is retried the answer
    says ``degraded`` and names it; once one failed ``unhealthy_after`` times
    in a row it is a 503.

    Until startup is done (``ready``) the answer is a 503 with ``ingest``
    ``starting`` and ``not_ready`` naming what startup waits for: a node that
    cannot be reached (``cannot reach Colca at ...``), its
    definitions. The door answers from the first moment, before the node does.

    ``backfill`` reports a running backfill (:mod:`chaski.dataops.backfill`):
    the job, its range, position and share done. A backfill in progress is
    healthy: it reads history through the buffer and the historian, not a
    cursor, so it adds nothing to ``cursor_lag``, which stays the lag of live
    intake. One that fails is counted in ``handlers`` like any task.

    ``loop`` says whether the event loop moves (see the module docstring):
    ``ok``, ``busy`` (late by ``loop_busy_s``: degraded, with ``loop_late_s``)
    or ``stalled`` (no beat for ``stall_after_s``: unhealthy). Before the beat
    has started it is ``ok``.

    ``status`` sums the answer up in one word:

    * ``starting``: startup is not done (503);
    * ``unhealthy``: any of the failures above (503);
    * ``degraded``: a handler is being retried, the event loop is busy, or a
      running backfill finished no window for its ``stall_after`` bound;
      ``degraded`` lists why (200, since live intake goes on);
    * ``backfilling``: a first backfill holds the producer's live triggers on
      purpose until it reaches the live edge (200; the ingest goes on
      buffering, so its cursor does not lag);
    * ``ok`` otherwise, an independent backfill running beside live dispatch
      included (200; ``backfill`` shows its progress).
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
    #: The running backfill and how many are pending, ``{}`` while none is.
    backfill: Callable[[], dict] | None = None
    #: Monotonic time of the last beat on the event loop, None before the first.
    loop_beat_at: float | None = None
    loop_busy_s: float = LOOP_BUSY_S
    _broker_down_since: float | None = field(default=None, repr=False)
    #: The door answers from a thread per request; _broker() keeps state.
    _answer_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

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

    def _loop_late(self) -> float:
        if self.loop_beat_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self.loop_beat_at)

    def _loop(self) -> str:
        late = self._loop_late()
        if late > self.stall_after_s:
            return "stalled"
        if late > self.loop_busy_s:
            return "busy"
        return "ok"

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
            and self._loop() != "stalled"
            and not self._lag()
            and not self._conflict()
            and self._handlers() != UNHEALTHY
        )

    def _degraded(self, backfill: dict, loop: str) -> list[str]:
        reasons: list[str] = []
        if loop == "busy":
            reasons.append(f"event loop busy for {self._loop_late():.0f} s (catching up)")
        if self.handlers is not None:
            reasons.extend(
                f"{name} failed {f.failures}x, retried" for name, f in sorted(self.handlers.failing().items())
            )
        stalled = (backfill.get("running") or {}).get("stalled")
        if stalled:
            reasons.append(stalled)
        return reasons

    def snapshot(self) -> dict:
        ingest = self._ingest()
        broker = self._broker()
        lag = self._lag()
        conflict = self._conflict()
        handlers = self._handlers()
        loop = self._loop()
        body = {
            "ok": ingest in ("not-started", "running")
            and broker != "down"
            and loop != "stalled"
            and not lag
            and not conflict
            and handlers != UNHEALTHY,
            "ingest": ingest,
            "broker": broker,
            "handlers": handlers,
            "loop": loop,
            "producers": self.producers,
            "generation": self.generation,
            "uptime_s": round(time.time() - self.started_at, 1),
        }
        if not self.ready and self.not_ready is not None:
            body["not_ready"] = self.not_ready()
        if self.ingest_task is not None and self.last_drain_at is not None:
            body["since_drain_s"] = round(self._since_drain(), 1)
        if loop != "ok":
            body["loop_late_s"] = round(self._loop_late(), 1)
        backfill = self.backfill() if self.backfill is not None else {}
        if backfill:
            body["backfill"] = backfill
        degraded = self._degraded(backfill, loop) if body["ok"] else []
        if not self.ready:
            body["status"] = "starting"
        elif not body["ok"]:
            body["status"] = "unhealthy"
        elif degraded:
            body["status"] = "degraded"
            body["degraded"] = degraded
        elif (backfill.get("running") or {}).get("holds_live"):
            body["status"] = "backfilling"
        else:
            body["status"] = "ok"
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

    def answer(self) -> tuple[bool, dict]:
        """Whether the service is healthy, and the body that says so: one
        consistent answer per request, whichever thread asks."""
        with self._answer_lock:
            body = self.snapshot()
            return body["ok"], body


class HealthDoor:
    """The running health door: its thread-served HTTP server and the beat on
    the event loop. :meth:`close` stops both."""

    def __init__(self, server: ThreadingHTTPServer, beat: asyncio.Task) -> None:
        self._server = server
        self._beat = beat

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def close(self) -> None:
        self._beat.cancel()
        self._server.shutdown()
        self._server.server_close()

    async def wait_closed(self) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await self._beat


async def _beat(state: HealthState) -> None:
    while True:
        state.loop_beat_at = time.monotonic()
        await asyncio.sleep(LOOP_BEAT_S)


def _handler(state: HealthState) -> type[BaseHTTPRequestHandler]:
    class HealthHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            healthy, body = state.answer()
            payload = json.dumps(body).encode()
            self.send_response(200 if healthy else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args) -> None:
            pass

    return HealthHandler


async def serve(state: HealthState, port: int = PORT_DEFAULT) -> HealthDoor:
    """Start the health door: the HTTP server on its own thread, and the beat
    on the running loop that tells it whether the loop moves."""
    beat = asyncio.ensure_future(_beat(state))
    # Binding all interfaces is the point: the probe (and an operator's
    # curl) reach a container-internal door; nothing publishes 8888.
    server = ThreadingHTTPServer(("0.0.0.0", port), _handler(state))  # noqa: S104 # nosec B104
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True, name="dataops-health").start()
    log.info("Health server on :%d (served off the event loop)", server.server_address[1])
    return HealthDoor(server, beat)
