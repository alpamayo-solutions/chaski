"""Shared stand-ins for the ``chaski.dataops`` tests.

A :class:`FakeDoor` answers ``kv()`` from a canned snapshot, serves
``fetch``/``ack`` from queued pages and keeps in ``published`` what was sent
to the node. A :class:`FakeRuntime` gives a producer the fake door, a
``send`` that lands there, a real
:class:`~chaski.dataops.Buffer` on a temporary SQLite file, and an optional
historian stand-in.
"""

from __future__ import annotations

import asyncio
import functools
import json
from typing import Any

from chaski.door import KvEntry, Page, Stream

NODE_ID = "n-1"


def run_async(coro_fn):
    """No pytest-asyncio dependency: drive the coroutine test body through a
    plain asyncio.run() call under a normal (sync) pytest test."""

    @functools.wraps(coro_fn)
    def _wrapper(*args, **kwargs):
        return asyncio.run(coro_fn(*args, **kwargs))

    return _wrapper


def kv_entry(topic: str, payload: dict | None, *, node_id: str = NODE_ID) -> KvEntry:
    return KvEntry(path="p", node_id=node_id, topic=topic, payload=payload, ts=0.0, offset=1)


def signal_entry(signal_id: str, name: str, *, path: str | None = None, node_id: str = NODE_ID) -> KvEntry:
    return kv_entry(
        f"colca/v1/_Signal/{node_id}/{path or name}",
        {"id": signal_id, "name": name},
        node_id=node_id,
    )


class FakeDoor:
    """A Door stand-in: canned ``kv()`` snapshot (mutate ``.entries`` between
    calls to simulate a KV change), the records sent to the node, fixed
    ``self_info()``, and queued pages for ``fetch``/``ack``/``delete_cursor``."""

    def __init__(
        self, entries: list[KvEntry] | None = None, *, ulid: str = "svc-1", name: str = "dataops", mount: str = ""
    ) -> None:
        self.entries = list(entries or [])
        self.published: list[tuple[str, str]] = []
        self.kv_calls = 0
        self.fetch_calls: list[dict] = []
        self.acked: list[tuple[str, str, int]] = []
        self.deleted: list[tuple[str, str]] = []
        self._pages: list[Page] = []
        self._self_info = {"ulid": ulid, "name": name, "node": NODE_ID, "element": "", "mount": mount}

    # -- kv / self --------------------------------------------------------

    def kv(self, prefix: str = "", *, contract: Any = None) -> list[KvEntry]:
        self.kv_calls += 1
        return list(self.entries)

    def self_info(self) -> dict:
        return dict(self._self_info)

    def close(self) -> None:
        pass

    # -- fetch / ack --------------------------------------------------------

    def queue(self, page: Page) -> None:
        self._pages.append(page)

    def fetch(self, stream, cursor, *, max=1000, signal_ids=None, contracts=None, topics=None, from_offset=None):
        call = {"stream": stream, "cursor": cursor, "max": max, "signal_ids": signal_ids}
        if contracts is not None:
            call["contracts"] = contracts
        if topics is not None:
            call["topics"] = topics
        if from_offset is not None:
            call["from_offset"] = from_offset
        self.fetch_calls.append(call)
        if self._pages:
            return self._pages.pop(0)
        return Page(records=[], next=1)

    def watch(self, streams, *, interval_ms=None):
        """No hints: the connection ends at once (tests ring by hand)."""
        return iter(())

    def ack(self, stream, cursor, offset) -> bool:
        self.acked.append((stream, cursor, offset))
        return True

    def delete_cursor(self, stream, cursor) -> None:
        self.deleted.append((stream, cursor))


def stream_opener(door: FakeDoor, *, prefix: str = "c/dataops/"):
    """What ``DataOpsService._open_ingest_stream`` does, over a fake door:
    a real :class:`chaski.door.Stream` whose cursor sits inside the
    service's namespace."""

    def _open(cursor: str, signal_ids):
        return Stream(door, "metrics", prefix + cursor, signal_ids=signal_ids)

    return _open


class FakeRuntime:
    """The :class:`chaski.dataops.Runtime` a producer under test is attached to."""

    def __init__(self, door: FakeDoor, buffer, historian: Any = None) -> None:
        self.door = door
        self.buffer = buffer
        self.historian = historian

    def send(self, topic: str, payload: str, *, retain: bool = False) -> None:
        self.door.published.append((topic, payload))

    def retract(self, topic: str) -> None:
        self.door.published.append((topic, ""))

    def command(self, contract: str, path: str, fields: dict | None = None, *, timeout: float = 30.0) -> dict:
        self.door.published.append((f"colca/v1/{contract}/{NODE_ID}/{path}", json.dumps(fields or {})))
        return {"result_code": 200, "message": ""}
