"""Starting a service while its node cannot be reached yet.

A container starts in any order: a connector or a DataOps service may be up
before the node's Colca is, or Colca may restart under it. Such a service
does not exit. It keeps its health door answering, reports itself not ready
with the reason, and retries the connection with a bounded, jittered backoff
that honours a node's ``Retry-After``. Once the node answers it starts as
usual.

What is retried is a node that cannot be reached or says "later": a refused
or reset connection, a name that does not resolve yet, a timeout, an HTTP
``408``/``425``/``429``/``5xx``, and an MQTT CONNACK refusing for load
(:data:`TRANSIENT_CONNACK`). Anything else (a wrong service name, a refused
identity, an invalid configuration) is a configuration error, and
:meth:`chaski.Service.start_when_reachable` raises it.
"""

from __future__ import annotations

import urllib.error
from dataclasses import dataclass
from typing import Literal

import httpx

#: Shortest and longest wait between two startup attempts. The ceiling is
#: low on purpose: a node that is back is reached within it.
STARTUP_RETRY_MIN_S = 0.5
STARTUP_RETRY_MAX_S = 5.0

#: How often a service still waiting for its node logs "still waiting" (INFO).
STARTUP_LOG_REMINDER_S = 60.0

#: HTTP statuses that mean "not now" rather than "never".
TRANSIENT_HTTP = frozenset({408, 425, 429})

#: MQTT 5 CONNACK reason codes that mean "not now": server unavailable,
#: server busy, quota exceeded, connection rate exceeded.
TRANSIENT_CONNACK = frozenset({0x88, 0x89, 0x97, 0x9F})


class BrokerRefused(RuntimeError):
    """The broker answered CONNECT with a failure reason code."""

    def __init__(self, reason_code: object) -> None:
        super().__init__(f"chaski.Service: broker refused CONNECT ({reason_code})")
        self.reason_code = reason_code

    @property
    def transient(self) -> bool:
        return getattr(self.reason_code, "value", self.reason_code) in TRANSIENT_CONNACK


def transient_status(status: int) -> bool:
    """Whether an HTTP ``status`` means "not now" rather than "never"."""
    return status in TRANSIENT_HTTP or status >= 500


def colca_unreachable(exc: BaseException) -> bool:
    """Whether ``exc`` from :meth:`chaski.Service.start` says the node cannot
    be reached yet (retry) rather than that the service is misconfigured."""
    if isinstance(exc, urllib.error.HTTPError):
        return transient_status(exc.code)
    if isinstance(exc, httpx.HTTPStatusError):
        return transient_status(exc.response.status_code)
    if isinstance(exc, BrokerRefused):
        return exc.transient
    # OSError covers urllib's URLError, refused and reset connections, a name
    # that does not resolve yet (socket.gaierror) and TimeoutError (no CONNACK).
    return isinstance(exc, (OSError, httpx.TransportError))


State = Literal["not-started", "connecting", "waiting", "ready", "failed"]


@dataclass(frozen=True)
class Readiness:
    """Whether a service has reached its node, and why not.

    * ``not-started``: :meth:`~chaski.Service.start` was not called yet.
    * ``connecting``: the first attempt is under way.
    * ``waiting``: the node cannot be reached; the service retries.
    * ``ready``: started.
    * ``failed``: the last attempt failed for a reason retrying cannot fix.

    ``reason`` says why the service is not ready, ``""`` once it is.
    ``attempts`` counts the startup attempts made so far.
    """

    state: State
    reason: str = ""
    attempts: int = 0

    @property
    def ready(self) -> bool:
        return self.state == "ready"
