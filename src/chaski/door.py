"""The colca door's HTTP client, the SDK's consume lane.

One implementation of ``GET /fetch`` (named cursors), ``POST /ack``,
``GET /kv`` (paged, contract-filtered), ``GET /self`` and ``POST /publish``,
used by ``chaski.Service.stream()`` and ``chaski.Service.kv()``.

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
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

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
    stream's low-water mark.
    """

    records: list[Record]
    next: int
    gap: Gap | None = None

    @property
    def ack_offset(self) -> int | None:
        """The offset to ack once every record of this page is processed.

        The last record's offset; the gap's bound when no record survived the
        low-water mark, so the same gap is not reported again; ``None`` for an
        empty page.
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
        params: list[tuple[str, str | int | float | bool | None]] = [
            ("stream", stream),
            ("cursor", cursor),
            ("max", str(max)),
        ]
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
        contract: str | Iterable[str] | None = None,
    ) -> list[KvEntry]:
        """``GET /kv?prefix=...`` — a snapshot of retained KV entries under
        ``prefix``, every page followed until the door returns an empty
        ``next``.

        ``contract`` narrows the scan to one or more contracts
        (``?contract=_Group&contract=_MetadataType``); the node filters before
        decoding payloads. An unknown contract name is a 400.
        """
        contracts = [contract] if isinstance(contract, str) else list(contract or [])
        entries: list[KvEntry] = []
        after = ""
        while True:
            params: list[tuple[str, str | int | float | bool | None]] = [("prefix", prefix), ("max", "10000")]
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
        """``POST /ack`` with ``delete: true``: retire a cursor.

        Succeeds also when the cursor does not exist; raises only on a
        transport or HTTP error.
        """
        resp = self._client.post("/ack", json={"cursor": cursor, "stream": stream, "delete": True})
        resp.raise_for_status()

    def publish(self, topic: str, payload: str) -> None:
        """``POST /publish``: publish one record under this service's identity.

        ``payload`` is a JSON string. It is embedded as a JSON value, not as a
        string, so the stored record matches what an MQTT publisher sends.
        """
        resp = self._client.post("/publish", json={"topic": topic, "payload": json.loads(payload)})
        resp.raise_for_status()

    def retire(self, topic: str) -> None:
        """``POST /publish`` with NO payload — the tombstone.

        Not ``publish(topic, "{}")`` and not ``"null"``: both are a payload,
        which the door validates against the contract's schema and refuses.
        Retiring means the key is absent — for a retained contract, what is not
        in the node's KV is not standing.
        """
        resp = self._client.post("/publish", json={"topic": topic})
        resp.raise_for_status()


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
    repeats the drain, sleeping ``poll_interval`` after an empty page. A pruned
    range (``Page.gap``) is logged as a warning.
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

    def ack(self, upto: Record | int) -> bool:
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
                    self.name,
                    self.cursor,
                    page.gap.from_offset,
                    page.gap.to_offset,
                    page.gap.first_ts,
                    page.gap.last_ts,
                    page.gap.approx,
                )
            yield from page.records
            ack_offset = page.ack_offset
            if ack_offset is None:
                return
            self._door.ack(self.name, self.cursor, ack_offset)

    def follow(self, *, poll_interval: float = 1.0, stop: threading.Event | None = None) -> Iterator[Record]:
        """:meth:`drain` forever — after an empty page, sleep ``poll_interval``
        (waking early when ``stop`` is set) and drain again. Ends when
        ``stop`` is set."""
        # A plain sleep between drains; a consumer with its own wake-up calls
        # drain() itself.
        stop = stop or threading.Event()
        while not stop.is_set():
            yield from self.drain()
            if stop.wait(poll_interval):
                return
