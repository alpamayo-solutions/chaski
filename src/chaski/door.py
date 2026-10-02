"""The colca door's HTTP client, the SDK's consume lane.

One implementation of ``GET /fetch`` (named cursors), ``POST /ack``,
``GET /kv`` (paged, contract-filtered), ``GET /watch`` (stream change
hints), ``GET /self`` and ``POST /publish``, used by
``chaski.Service.stream()`` and ``chaski.Service.kv()``.

On the local door (plain HTTP inside the deployment's network) there is no
credential: the caller names itself with ``X-Colca-Service`` and the door
registers it on first sight. On the published API door (HTTPS) the caller
presents its pinned identity as a TLS client certificate (``cert=``) and the
name header is ignored. ``base_url`` and ``cert`` decide which door a
:class:`Door` talks to.

The client decodes the HTTP/JSON envelope but returns each record's
``payload`` as plain JSON values; decoding it into a contract type is the
caller's job. Errors are ``httpx`` exceptions, raised unchanged:
``httpx.HTTPStatusError`` for a non-2xx response, a transport exception
otherwise. Both are ``httpx.HTTPError``.
"""

from __future__ import annotations

import json
import logging
import ssl
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from chaski.doorbell import IDLE_DRAIN_S, Doorbell

log = logging.getLogger("chaski.door")


@dataclass(frozen=True)
class Record:
    """One record returned by ``GET /fetch`` (or held inside a :class:`Page`).

    ``ts`` is colca's record timestamp in unix milliseconds, while every other
    timestamp is in seconds. Use :attr:`fallback_timestamp_s` when the record
    timestamp stands in for a payload without one.
    """

    offset: int
    origin_offset: int
    topic: str
    payload: Any
    ts: float
    written_by: str
    actor_id: str
    actor_label: str
    actor_kind: str

    @property
    def fallback_timestamp_s(self) -> float:
        """``ts`` converted from milliseconds to unix seconds."""
        return self.ts / 1000.0


@dataclass(frozen=True)
class Gap:
    """A pruned-range marker riding alongside a ``/fetch`` response.

    Present only when the cursor's position is below the stream's
    low-water mark. Never moves the cursor by itself — the caller clears it
    by acking ``to_offset``.
    """

    stream: str
    from_offset: int
    to_offset: int
    first_ts: float | None
    last_ts: float | None
    approx: bool


@dataclass(frozen=True)
class Page:
    """One ``GET /fetch`` response.

    ``records`` are in stream order. ``next`` is the offset to fetch from
    next time — fetch itself never moves the cursor, only ``ack`` does.
    ``gap`` is set only when the cursor's position has fallen below the
    stream's low-water mark. ``start`` is the offset the page was read from;
    nodes before colca 0.18.2 do not report it and cannot read ahead.
    """

    records: list[Record]
    next: int
    gap: Gap | None = None
    start: int | None = None

    @property
    def ack_offset(self) -> int | None:
        """Last scanned offset after processing the page's matching records.

        The last offset the node scanned for the page (``next - 1``): a
        filtered fetch (``signal_ids``, ``contracts``) moves ``next`` past the
        records it skips, and acking only the last returned record left the
        cursor behind every skipped one, where its lag and age count them as
        unread. The gap's bound when nothing was scanned past it; ``None`` when
        the page moved nothing (an empty page from a node that does not report
        ``start``, or one whose ``next`` is where it started).
        """
        if self.records:
            return max(self.records[-1].offset, self.next - 1)
        if self.start is not None and self.next > self.start:
            return self.next - 1 if self.gap is None else max(self.gap.to_offset, self.next - 1)
        return self.gap.to_offset if self.gap is not None else None


@dataclass(frozen=True)
class Hint:
    """One line of ``GET /watch``: the streams that grew since the previous
    hint, and each one's next offset."""

    streams: list[str]
    next: dict[str, int]


#: A watch writes at least a heartbeat every 5 s; this long without a line is a
#: dead connection.
WATCH_SILENCE_S = 15.0


@dataclass(frozen=True)
class KvEntry:
    """One entry from ``GET /kv``."""

    path: str
    node_id: str
    topic: str
    payload: Any
    ts: float
    offset: int


def _external_ssl_context(cert_path: Path, key_path: Path) -> ssl.SSLContext:
    """TLS context for the published API door. There is no CA: trust is the
    pinned key presented as a client certificate, so the chain is not checked."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    return ctx


class Door:
    """HTTP client for one colca node's door.

    ``base_url`` is the door's origin: ``http://colca`` (the local door,
    plain HTTP, port 80 inside the compose network) or ``https://node:443``
    (the published API door). ``service`` names this caller for
    ``X-Colca-Service`` on the local door and therefore also for the
    ``c/{service}/...`` cursor namespace it owns. ``cert`` — a
    ``(cert_path, key_path)`` pair — switches the client to the published
    door's credential: the identity is the certificate, and the cursor
    namespace is ``{ulid}/...``.
    """

    def __init__(
        self,
        base_url: str,
        service: str,
        *,
        timeout: float = 10.0,
        cert: tuple[Path, Path] | None = None,
    ) -> None:
        self._service = service
        kwargs: dict[str, Any] = {
            "base_url": base_url.rstrip("/"),
            "headers": {"X-Colca-Service": service},
            "timeout": timeout,
        }
        if cert is not None:
            kwargs["verify"] = _external_ssl_context(*cert)
        self._client = httpx.Client(**kwargs)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Door:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ reads

    def backlog(self, prefixes):
        """Bounded local queue telemetry. Positions are next offsets to consume."""
        response = self._client.get("/backlog", params=[("prefix", p) for p in prefixes])
        response.raise_for_status()
        return response.json()["queues"]

    def watch_uplink(self, stop, *, timeout=15):
        """Local node lifecycle transitions; heartbeat messages yield None."""
        with self._client.stream("GET", "/watch", params={"uplink": "1"}, timeout=timeout) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if stop():
                    return
                if line:
                    yield json.loads(line).get("uplink")

    def watch_backlog(self, stop):
        """Yield queue-change hints and False transport heartbeats; no record reads."""
        with self._client.stream("GET", "/watch", params={"backlog": "1"}, timeout=15) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if stop():
                    return
                if line:
                    yield bool(json.loads(line)["backlog_changed"])

    def fetch(
        self,
        stream: str,
        cursor: str,
        *,
        max: int = 1000,
        signal_ids: list[str] | None = None,
        tail: bool = False,
        contracts: Iterable[str] | None = None,
        topics: Iterable[str] | None = None,
        from_offset: int | None = None,
    ) -> Page:
        """``GET /fetch`` — read FORWARD from ``cursor``'s stored position.

        Never moves the cursor; only :meth:`ack` does. ``signal_ids`` is
        sent as repeated ``signal_id`` query params and is valid only when
        ``stream == "metrics"`` (the door rejects it otherwise).
        ``contracts`` keeps only records of those contracts (colca 0.18+);
        ``next`` still moves past the others. ``topics`` keeps records whose
        topic matches one of the MQTT filters (colca 0.19+; older nodes ignore
        it and return every record). ``from_offset`` reads ahead of
        the cursor, never behind it (colca 0.18.2+; older nodes ignore it and
        leave :attr:`Page.start` unset).
        """
        params: list[tuple[str, str | int | float | bool | None]] = [
            ("stream", stream),
            ("cursor", cursor),
            ("max", str(max)),
        ]
        if tail:
            params.append(("tail", 1))
        if from_offset is not None:
            params.append(("from", str(from_offset)))
        for signal_id in signal_ids or []:
            params.append(("signal_id", signal_id))
        for contract in contracts or []:
            params.append(("contract", contract))
        for topic in topics or []:
            params.append(("topic", topic))
        resp = self._client.get("/fetch", params=params)
        resp.raise_for_status()
        body = resp.json()

        records = [
            Record(
                offset=r["offset"],
                origin_offset=r["origin_offset"],
                topic=r["topic"],
                payload=r["payload"],
                ts=r["ts"],
                written_by=r.get("written_by", ""),
                actor_id=r.get("actor_id", ""),
                actor_label=r.get("actor_label", ""),
                actor_kind=r.get("actor_kind", ""),
            )
            for r in body.get("records", [])
        ]

        gap: Gap | None = None
        if "gap" in body:
            g = body["gap"]
            gap = Gap(
                stream=g["stream"],
                from_offset=g["from_offset"],
                to_offset=g["to_offset"],
                first_ts=g.get("first_ts"),
                last_ts=g.get("last_ts"),
                approx=g.get("approx", False),
            )

        return Page(records=records, next=body["next"], gap=gap, start=body.get("from"))

    def kv(
        self,
        prefix: str = "",
        *,
        contract: str | Iterable[str] | None = None,
        depth: int | None = None,
    ) -> list[KvEntry]:
        """``GET /kv?prefix=...`` — a snapshot of retained KV entries under
        ``prefix``, every page followed until the door returns an empty
        ``next``.

        ``contract`` narrows the scan to one or more contracts
        (``?contract=_Group&contract=_MetadataType``); the node filters before
        decoding payloads. An unknown contract name is a 400. ``depth`` keeps
        entries at most that many path segments below ``prefix`` (colca
        0.18+), so a tree view reads one level at a time.
        """
        contracts = [contract] if isinstance(contract, str) else list(contract or [])
        entries: list[KvEntry] = []
        after = ""
        while True:
            params: list[tuple[str, str | int | float | bool | None]] = [("prefix", prefix), ("max", "10000")]
            params.extend(("contract", name) for name in contracts)
            if depth is not None:
                params.append(("depth", str(depth)))
            if after:
                params.append(("after", after))
            resp = self._client.get("/kv", params=params)
            resp.raise_for_status()
            body = resp.json()
            entries.extend(
                KvEntry(
                    path=e["path"],
                    node_id=e["node_id"],
                    topic=e["topic"],
                    payload=e["payload"],
                    ts=e["ts"],
                    offset=e["offset"],
                )
                for e in body.get("entries", [])
            )
            next_token = body.get("next", "")
            if not next_token:
                return entries
            if next_token == after:
                raise RuntimeError("GET /kv: server repeated page token")
            after = next_token

    def watch(
        self,
        streams: Iterable[str],
        *,
        interval_ms: int | None = None,
        contracts: Iterable[str] = (),
        stop: Callable[[], bool] | None = None,
    ) -> Iterator[Hint]:
        """``GET /watch`` (colca 0.18+): yield a :class:`Hint` whenever one of
        ``streams`` grows, instead of polling :meth:`fetch` on an idle stream.

        The first hint names every stream, so draining each named stream on
        every hint misses nothing across a reconnect. Hints are at least
        ``interval_ms`` apart (the node's default is 100); what grows in
        between is merged. Heartbeats are not yielded. The generator ends
        with an ``httpx`` error when the connection fails or stays silent for
        :data:`WATCH_SILENCE_S`; reconnecting is the caller's.
        """
        params: list[tuple[str, str | int | float | bool | None]] = [("stream", name) for name in streams]
        params.extend(("contract", name) for name in contracts)
        if interval_ms is not None:
            params.append(("interval_ms", str(interval_ms)))
        timeout = httpx.Timeout(self._client.timeout.connect, read=WATCH_SILENCE_S)
        with self._client.stream("GET", "/watch", params=params, timeout=timeout) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if stop is not None and stop():
                    return
                if not line:
                    continue
                body = json.loads(line)
                named = body.get("streams") or []
                if named:
                    yield Hint(streams=list(named), next={k: int(v) for k, v in (body.get("next") or {}).items()})

    def self_info(self) -> dict:
        """``GET /self`` — this service's minted identity: ulid, name, node, element, mount."""
        resp = self._client.get("/self")
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ writes

    def ack(self, stream: str, cursor: str, offset: int) -> bool:
        """``POST /ack`` — ack the LAST PROCESSED offset (the store advances to ``offset + 1``).

        Monotonic: the door never moves a cursor backward. Returns whether
        the cursor actually moved (``False`` for a stale/duplicate ack).
        """
        resp = self._client.post("/ack", json={"cursor": cursor, "stream": stream, "offset": offset})
        resp.raise_for_status()
        return bool(resp.json()["moved"])

    def delete_cursor(self, stream: str, cursor: str) -> None:
        """``POST /ack`` with ``delete: true``: retire a cursor.

        Succeeds also when the cursor does not exist; raises only on a
        transport or HTTP error.
        """
        resp = self._client.post("/ack", json={"cursor": cursor, "stream": stream, "delete": True})
        resp.raise_for_status()

    def publish(self, topic: str, payload: str) -> dict | None:
        """``POST /publish``: publish one record under this service's identity.

        For a caller without an MQTT session. A :class:`chaski.Service` writes
        over its session instead (``send``, ``retract``, ``command``).

        ``payload`` is a JSON string. It is embedded as a JSON value, not as a
        string, so the stored record matches what an MQTT publisher sends.

        Returns the door's response body, ``None`` when it is empty. A command
        the node executes itself (``_CmdConfigure``) is answered in it: the
        ``_Ack`` is under ``"command"``.
        """
        resp = self._client.post("/publish", json={"topic": topic, "payload": json.loads(payload)})
        resp.raise_for_status()
        if not resp.content:
            return None
        body = resp.json()
        return body if isinstance(body, dict) else None

    def publish_batch(self, records: Iterable[tuple[str, str]]) -> list[dict]:
        """``POST /publish/batch`` (colca 0.19+): many ``(topic, payload)``
        records in one request, at most 5000. Each is judged as
        :meth:`publish` would be, and the admitted ones are written with one
        append per stream. Returns one result per record, in order:
        ``{"stream", "offset"}`` or ``{"error"}``; a refused record does not
        stop the others. ``payload`` is a JSON string, as for :meth:`publish`.
        Commands are refused in a batch."""
        body = {"records": [{"topic": topic, "payload": json.loads(payload)} for topic, payload in records]}
        resp = self._client.post("/publish/batch", json=body)
        resp.raise_for_status()
        return list(resp.json()["results"])

    def retire(self, topic: str) -> None:
        """``POST /publish`` with NO payload — the tombstone.

        Not ``publish(topic, "{}")`` and not ``"null"``: both are a payload,
        which the door validates against the contract's schema and refuses.
        Retiring means the key is absent — for a retained contract, what is not
        in the node's KV is not standing.
        """
        resp = self._client.post("/publish", json={"topic": topic})
        resp.raise_for_status()


class StreamGapError(RuntimeError):
    """Consumer history was pruned; effects and cursor remain unchanged."""


class Stream:
    """A named cursor over one colca stream, as ``Service.stream()`` returns it.

    The cursor lives at the door: ``/fetch`` reads from its stored position
    and only ``/ack`` moves it. Reopening the same name resumes where it was
    acked; a new name starts at the stream's first retained record.

    **Iterating drains the stream, acking page by page.** ``for record in
    stream:`` yields records in stream order and acks a page only when the
    consumer asks for the record after its last one. A consumer that raises or
    stops mid-page gets that page again next time, so handlers must be
    idempotent. Iteration stops at the first empty page.

    :meth:`ack` commits before the page boundary if needed. :meth:`follow`
    repeats the drain whenever a :class:`chaski.Doorbell` rings, and after a
    silent :data:`chaski.doorbell.IDLE_DRAIN_S` so a filtered cursor keeps
    moving. A pruned range (``Page.gap``) raises :class:`StreamGapError`.
    """

    def __init__(
        self,
        door: Door,
        name: str,
        cursor: str,
        *,
        max: int = 1000,
        signal_ids: Iterable[str] | None = None,
        contracts: Iterable[str] | None = None,
        topics: Iterable[str] | None = None,
    ) -> None:
        self._door = door
        self.name = name
        self.cursor = cursor
        self._max = max
        self._acknowledged = 0
        self._moved = Doorbell()
        self._signal_ids = list(signal_ids) if signal_ids is not None else None
        self._contracts = sorted(contracts) if contracts is not None else None
        self._topics = list(topics) if topics is not None else None

    @property
    def page_size(self) -> int:
        """The most records one :meth:`fetch` asks for."""
        return self._max

    def fetch(self, *, from_offset: int | None = None) -> Page:
        """One page from the cursor's stored position, or from ``from_offset``
        when that lies ahead of it. Never moves the cursor."""
        scope: dict[str, Any] = {"signal_ids": self._signal_ids}
        if self._contracts is not None:
            scope["contracts"] = self._contracts
        if self._topics is not None:
            scope["topics"] = self._topics
        if from_offset is not None:
            scope["from_offset"] = from_offset
        return self._door.fetch(self.name, self.cursor, max=self._max, **scope)

    def head(self) -> int:
        """Capture the last admitted offset without moving/changing this cursor.

        A coordinated drain must stop at a fixed boundary: a live producer may
        never leave the stream empty. Tail reads do not replace cursor filters.
        """
        return self._door.fetch(self.name, self.cursor, max=1, tail=True).next - 1

    def ack(self, upto: Record | int) -> bool:
        """Ack ``upto`` (a record, or its offset) as the last PROCESSED
        position. Returns whether the cursor moved."""
        offset = upto.offset if isinstance(upto, Record) else int(upto)
        if offset <= self._acknowledged:
            return False
        moved = self._door.ack(self.name, self.cursor, offset)
        self._advance(offset)
        return moved

    def _advance(self, offset: int) -> None:
        if offset > self._acknowledged:
            self._acknowledged = offset
            self._moved.ring()

    @property
    def position(self) -> int:
        """The last offset this cursor passed, as far as this object knows.

        Moves with every :meth:`ack` (also the page acks of :meth:`drain`,
        :meth:`follow` and ``Service.consume``), and to the head when a drain
        finds the cursor already there. Every record up to it was handled or
        skipped by the cursor's filter. 0 until the first ack or drain in this
        process; no network I/O.
        """
        return self._acknowledged

    def wait_caught_up(self, head: int, timeout: float | None = None) -> bool:
        """Block until :attr:`position` reached ``head`` (e.g. from
        :meth:`head`, captured when a request arrived).

        Waits on this stream's own acknowledgements from whoever drains it;
        nothing is read here. False when ``timeout`` (seconds) passed first.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            seen = self._moved.generation
            if self._acknowledged >= head:
                return True
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return False
            self._moved.wait_after(seen, remaining)

    def retire(self) -> None:
        """Delete this cursor at the door — idempotent, also when it never
        existed. A later fetch under the same name starts over."""
        self._door.delete_cursor(self.name, self.cursor)
        self._acknowledged = 0
        self._moved.ring()

    def __iter__(self) -> Iterator[Record]:
        return self.drain()

    def drain(self, *, stop: threading.Event | None = None) -> Iterator[Record]:
        """Yield every record from the cursor's position to the head, page
        by page, acking each page after its records were consumed. Capture a
        finite head so continuous producers cannot keep this call open forever.
        Cancellation leaves a partially consumed page unacknowledged for replay.
        The final page may include records admitted after the captured head.
        """
        if stop is not None and stop.is_set():
            return
        head = self.head()
        while stop is None or not stop.is_set():
            page = self.fetch()
            if page.gap is not None:
                raise StreamGapError(
                    f"stream={self.name} cursor={self.cursor}: retained offsets "
                    f"{page.gap.from_offset}..{page.gap.to_offset} were pruned; "
                    "rebuild the consumer state before advancing its cursor"
                )
            for record in page.records:
                if stop is not None and stop.is_set():
                    return
                yield record
            if stop is not None and stop.is_set():
                return
            ack_offset = page.ack_offset
            if ack_offset is None:
                if page.next <= head:
                    raise RuntimeError("stream stopped before its captured head")
                if not page.records and page.gap is None:
                    # Nothing after the cursor: it already stands at the head.
                    self._advance(page.next - 1)
                return
            self.ack(ack_offset)
            if ack_offset >= head:
                return

    def follow(
        self,
        bell: Doorbell | None = None,
        *,
        stop: threading.Event | None = None,
        idle_drain_s: float | None = IDLE_DRAIN_S,
    ) -> Iterator[Record]:
        """:meth:`drain` now, then again after every ring of ``bell``, until
        ``stop`` is set. Ring the bell from the MQTT subscription to the topics
        this stream reads and on every reconnect. The generation is taken
        before each drain, so a ring during a drain is not lost. Setting
        ``stop`` ends it.

        After ``idle_drain_s`` without a ring it drains anyway: a filtered
        stream whose topics stay silent still walks its cursor past the
        records it skips, so it does not hold the node's retention (``None``
        waits for the bell alone)."""
        stop = stop or threading.Event()
        if bell is None:
            from .stream_changes import StreamChanges

            watch = StreamChanges(self._door, [self.name], stop=stop).start()
            try:
                yield from self.follow(watch[self.name], stop=stop, idle_drain_s=idle_drain_s)
            finally:
                watch.close()
            return
        while not stop.is_set():
            seen = bell.generation
            yield from self.drain(stop=stop)
            if stop.is_set():
                return
            bell.wait_after(seen, idle_drain_s, stop=stop)
