"""The colca door's HTTP client — the SDK's consume lane (service families
design 2026-09-07 §3.3, decision D7).

One implementation of ``GET /fetch`` (named cursors), ``POST /ack``,
``GET /kv`` (paged, contract-filtered), ``GET /self`` and ``POST /publish``,
used by ``chaski.Service.stream()`` / ``chaski.Service.kv()`` and directly by
the shipped ``dataops`` service. It used to live in ``dataops/door.py`` and
was vendored by hand into every bridge that needed to follow a stream — the
duplication architecture principle 1 forbids, deleted by moving it here.

Two doors, one client. On the LOCAL door (plain HTTP, port 80 inside the
deployment's own compose network) there is no credential — reachability IS
the credential — and the caller names itself with ``X-Colca-Service`` so the
door can self-register and bind it on first sight ("Node doors",
the local service trust design). On the
published API door (HTTPS, 443) the caller presents its registry-pinned
identity as a TLS client certificate instead (``cert=``), and the name
header is ignored. Which door a :class:`Door` speaks to is decided entirely
by its ``base_url`` and whether ``cert`` is given.

This client stays contract-agnostic: it decodes the HTTP/JSON envelope
(records, cursors, gaps, KV entries) but hands each record's ``payload``
back as whatever native Python value ``json`` produced (typically a
``dict``). Turning that into a ``colca_data_contracts`` type (``Metric``,
``Annotation``, ...) is the caller's job.

Errors surface as exceptions from the underlying ``httpx`` client: a
non-2xx response raises ``httpx.HTTPStatusError`` (via
``response.raise_for_status()``); a connection failure, timeout, or other
transport problem raises the corresponding ``httpx`` exception directly.
Nothing here wraps or swallows either kind — a consumer's retry loop
catches ``httpx.HTTPError``, the class both kinds share.
"""

from __future__ import annotations

import json
import logging
import ssl
import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

import httpx

log = logging.getLogger("chaski.door")


@dataclass(frozen=True)
class Record:
    """One record returned by ``GET /fetch`` (or held inside a :class:`Page`).

    ``ts`` is passed through verbatim from the wire — colca's own record
    timestamp, unix MILLISECONDS (``colca/internal/store/store.go``:
    ``cutoff := now.UnixMilli() - maxAge.Milliseconds()`` is compared
    against it). Every other timestamp a consumer handles (buffered points,
    a payload's own ``timestamp`` field, watermarks) is unix SECONDS. Use
    :attr:`fallback_timestamp_s`, never ``ts`` directly, wherever a
    record's own timestamp stands in for a payload that carries none of
    its own.
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
        """``ts`` converted from colca's wire unit (milliseconds) to the
        unix-seconds convention every other timestamp uses.

        The ONE place a consumer gets the conversion from, so it can be
        wrong in at most one place. A payload without a ``timestamp`` field
        once buffered this record's raw millisecond ``ts`` as if it were
        seconds, landing the point ~50,000 years in the future and wedging
        every freshness check for that signal forever.
        """
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
    stream's low-water mark.
    """

    records: list[Record]
    next: int
    gap: Gap | None = None

    @property
    def ack_offset(self) -> int | None:
        """The offset to ack once EVERY record of this page is processed.

        The last record's offset when the page carries records; the gap's
        own bound when nothing survived at/after the low-water mark
        (otherwise the next fetch reports the identical gap forever); and
        ``None`` for an empty page, which acks nothing. This is the rule
        the dataops ingest loop and :class:`Stream` share — one owner.
        """
        if self.records:
            return self.records[-1].offset
        if self.gap is not None:
            return self.gap.to_offset
        return None


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
    """No CA anywhere at the published API door ("Node doors"):
    trust is the registry-pinned key presented as a client certificate,
    never a chain — the same InsecureSkipVerify shape colca-machine (the Go
    reference client) and ``chaski.Service``'s MQTT side use."""
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
        cert: Optional[tuple[Path, Path]] = None,
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

    def __enter__(self) -> "Door":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ reads

    def fetch(
        self,
        stream: str,
        cursor: str,
        *,
        max: int = 1000,
        signal_ids: list[str] | None = None,
    ) -> Page:
        """``GET /fetch`` — read FORWARD from ``cursor``'s stored position.

        Never moves the cursor; only :meth:`ack` does. ``signal_ids`` is
        sent as repeated ``signal_id`` query params and is valid only when
        ``stream == "metrics"`` (the door rejects it otherwise).
        """
        params: list[tuple[str, str]] = [("stream", stream), ("cursor", cursor), ("max", str(max))]
        for signal_id in signal_ids or []:
            params.append(("signal_id", signal_id))
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

        return Page(records=records, next=body["next"], gap=gap)

    def kv(
        self,
        prefix: str = "",
        *,
        contract: Union[str, Iterable[str], None] = None,
    ) -> list[KvEntry]:
        """``GET /kv?prefix=...`` — a snapshot of retained KV entries under
        ``prefix``, every page followed until the door returns an empty
        ``next``.

        ``contract`` narrows the scan to a set of uns contracts — one name
        or several, sent as the repeatable ``contract`` query parameter
        (``?contract=_Group&contract=_MetadataType``). The filter is applied
        inside colcad's scan, before any entry's payload is decoded, so a
        narrowed read stays bounded regardless of how much other state the
        node holds ("Element-Scoped Authorization"). An unknown
        contract name is a 400 from the door, never a silent empty result.
        """
        contracts = [contract] if isinstance(contract, str) else list(contract or [])
        entries: list[KvEntry] = []
        after = ""
        while True:
            params: list[tuple[str, str]] = [("prefix", prefix), ("max", "10000")]
            params.extend(("contract", name) for name in contracts)
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
        """``POST /ack`` with ``delete: true`` — retire a cursor.

        The door answers ``{"deleted": true}`` unconditionally on success,
        including when the cursor was already absent (idempotent), so there
        is no meaningful boolean to hand back — this raises only on a
        transport or HTTP error and otherwise returns ``None``.
        """
        resp = self._client.post("/ack", json={"cursor": cursor, "stream": stream, "delete": True})
        resp.raise_for_status()

    def publish(self, topic: str, payload: str) -> None:
        """``POST /publish`` — publish one record under this service's identity.

        ``payload`` is a pre-serialized JSON string (typically
        ``json.dumps(...)`` of a contract payload dict). It is decoded and
        re-embedded as a native JSON value in the request body — never
        double-encoded as a JSON string containing JSON text — so the
        record colca stores matches what any other publisher (e.g. a
        connector's ``_Metric``) would produce.
        """
        resp = self._client.post("/publish", json={"topic": topic, "payload": json.loads(payload)})
        resp.raise_for_status()


class Stream:
    """A named cursor over one colca stream — what ``Service.stream()`` returns.

    The cursor is durable and server-side: ``/fetch`` reads from wherever
    the door last stored it, and only ``/ack`` moves it. A process that
    restarts under the same cursor name resumes exactly where it acked;
    one that opens a NEW cursor name starts from the stream's first
    retained record — that is how the dataops ingest loop treats a rebuilt
    buffer (a fresh "generation" gets a fresh cursor and :meth:`retire`
    deletes the previous one), and the same contract holds here.

    **Iterating is one drain, page-acked.** ``for record in stream:``
    fetches page after page from the cursor's position, yields every
    record in stream order, and acks a page — its last offset, or a gap's
    bound when nothing survived at/after the low-water mark — only once the
    consumer has come back for more after the page's last record, i.e.
    after processing it. It stops at the first empty page. This is the
    ingest loop's crash-safety contract verbatim (``dataops/ingest.py``):
    a consumer that raises, breaks, or dies mid-page never acks that page,
    so the next drain re-delivers it — at-least-once, page-granular.
    Handlers must therefore be idempotent for the same record.

    :meth:`ack` is still public for a consumer that wants to commit earlier
    than the page boundary (the page's own ack afterwards is a harmless
    ``moved=False``). :meth:`follow` is the same drain repeated forever,
    sleeping ``poll_interval`` after each empty page.

    A pruned range (``Page.gap``) is logged at WARNING — operator-visible,
    not an error — and the drain continues from what survived.
    """

    def __init__(
        self,
        door: Door,
        name: str,
        cursor: str,
        *,
        max: int = 1000,
        signal_ids: Iterable[str] | None = None,
    ) -> None:
        self._door = door
        self.name = name
        self.cursor = cursor
        self._max = max
        self._signal_ids = list(signal_ids) if signal_ids is not None else None

    def fetch(self) -> Page:
        """One page from the cursor's stored position. Never moves the cursor."""
        return self._door.fetch(self.name, self.cursor, max=self._max, signal_ids=self._signal_ids)

    def ack(self, upto: Union[Record, int]) -> bool:
        """Ack ``upto`` (a record, or its offset) as the last PROCESSED
        position. Returns whether the cursor moved."""
        offset = upto.offset if isinstance(upto, Record) else int(upto)
        return self._door.ack(self.name, self.cursor, offset)

    def retire(self) -> None:
        """Delete this cursor at the door — idempotent, also when it never
        existed. A later fetch under the same name starts over."""
        self._door.delete_cursor(self.name, self.cursor)

    def __iter__(self) -> Iterator[Record]:
        return self.drain()

    def drain(self) -> Iterator[Record]:
        """Yield every record from the cursor's position to the head, page
        by page, acking each page after its records were consumed (see the
        class docstring). Stops at the first empty page."""
        while True:
            page = self.fetch()
            if page.gap is not None:
                log.warning(
                    "Gap on stream=%s cursor=%s: offsets %d..%d were pruned (first_ts=%s last_ts=%s "
                    "approx=%s) — continuing from the low-water mark",
                    self.name, self.cursor, page.gap.from_offset, page.gap.to_offset,
                    page.gap.first_ts, page.gap.last_ts, page.gap.approx,
                )
            yield from page.records
            ack_offset = page.ack_offset
            if ack_offset is None:
                return
            self._door.ack(self.name, self.cursor, ack_offset)

    def follow(
        self, *, poll_interval: float = 1.0, stop: Optional[threading.Event] = None
    ) -> Iterator[Record]:
        """:meth:`drain` forever — after an empty page, sleep ``poll_interval``
        (waking early when ``stop`` is set) and drain again. Ends when
        ``stop`` is set."""
        # A plain sleep between drains. A consumer that wants an MQTT
        # doorbell instead (dataops' shape) wires its own wake-up and
        # calls drain() itself.
        stop = stop or threading.Event()
        while not stop.is_set():
            yield from self.drain()
            if stop.wait(poll_interval):
                return
