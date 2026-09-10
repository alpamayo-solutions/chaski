"""The one DataTag catalogue every :class:`chaski.Service` publishes (SDK
design §3, §7 gap 3; service families design §3.4 and §4).

A catalogue is the set of sources a service offers, each with an id that
never changes for as long as the source is known. Two things grow it and
they are the only two:

* ``ensure(path, value, unit)`` — the ``publish()`` path: a never-seen path
  mints a tag, a stale one is revived.
* ``declare(tags)`` — the discovery path (``ConnectorService``): the whole
  discovered set at once. Ids are reused by ``source``, new sources mint,
  and a source that vanished is carried forward marked ``is_stale`` rather
  than dropped, so a Signal bound to it stays bound instead of silently
  rebinding (local-service-trust design §6).

**The memory is the node, not a local file.** A service holds no catalogue
state of its own: the retained ``_DataTags`` record it published last time
is already in the node's KV, readable through the same door it publishes at,
and ``load_previous`` seeds the catalogue from it before the service
subscribes to its ``_Signal`` records — the bindings that arrive on that
subscription name the ids of the PREVIOUS run, so the catalogue must know
them first. That seed is also what arms the republish guard: an unchanged
catalogue after a restart hashes to the revision already on record and is
not re-appended and re-replicated.

This used to exist twice — here as a JSON file next to the service's state
directory, and in ``connector/src/reader.py`` reading the node — with the
content-hash guard implemented in both. There is one now.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import ulid as ulid_lib
from colca_data_contracts.payload import DataTag, DataTags


def infer_data_type(value: Any) -> str:
    """The DataTag.data_type inferred from a value's Python type — bool
    before int (bool is an int subclass in Python)."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def element_for(path: str, mount: str = "") -> str:
    """The path's parent, joined onto ``mount`` to make it node-local (SDK
    design §3 rule 2): the node resolves ``meta.element`` as a node-local
    path (``exec_configure.go`` ``elementAt``/``authorElementAt``), so a
    mount-relative parent sent as-is would miss for every service that is
    not bound at the node's root. "" for a top-level path — no parent means
    no ``meta.element`` at all, which leaves the tag placed at the service's
    own mount, whatever that is (architecture principle 6); joining an empty
    parent with a mount would wrongly turn "no parent" into "the mount
    itself"."""
    if "/" not in path:
        return ""
    parent = path.rsplit("/", 1)[0]
    if not mount:
        return parent
    return f"{mount}/{parent}"


def _tag_from_record(raw: Mapping[str, Any]) -> DataTag:
    return DataTag(
        id=str(raw["id"]),
        name=str(raw.get("name", "")),
        source=str(raw["source"]),
        is_writable=bool(raw.get("is_writable", False)),
        is_readable=bool(raw.get("is_readable", False)),
        data_type=raw.get("data_type"),
        is_stale=bool(raw.get("is_stale", False)),
        meta=dict(raw.get("meta") or {}),
    )


class Catalogue:
    """The source -> DataTag mapping for one service.

    ``connector`` is the identity the published ``DataTags.connector`` field
    carries — the node-minted registry ULID of a local service, the pinned
    ULID of an external one. ``mount`` is where the service sits, used only
    to make a ``publish()``-path parent node-local (:func:`element_for`).
    """

    def __init__(self, *, connector: str, mount: str = "") -> None:
        self.connector = connector
        self.mount = mount
        self._tags: dict[str, DataTag] = {}
        #: The revision (:meth:`revision`) the last successful publish
        #: carried, or the one seeded from the node's retained record.
        self.last_published_revision: str | None = None
        #: Whether anything changed since the last publish decision. A
        #: dirty catalogue is hashed once and compared against
        #: ``last_published_revision``; a clean one is not hashed at all.
        self.dirty = False

    # -- memory --------------------------------------------------------

    def load_previous(self, payload: Mapping[str, Any] | None) -> None:
        """Seed from the retained ``_DataTags`` payload the node holds for
        this service (as ``GET /kv`` returns it: ``data_tags``, ``connector``,
        ``version``). Nothing to seed from is a valid first run."""
        if not payload:
            return
        for raw in payload.get("data_tags") or []:
            if not raw.get("source") or not raw.get("id"):
                continue
            tag = _tag_from_record(raw)
            self._tags[tag.source] = tag
        version = payload.get("version")
        if version:
            self.last_published_revision = f"{payload.get('connector')}\x00{version}"
        self.dirty = False

    # -- the two mutators ------------------------------------------------

    def ensure(self, path: str, value: Any, unit: str | None = None) -> tuple[str, bool]:
        """The ``publish()`` path: return ``(tag_id, changed)`` for ``path``,
        minting a tag the first time the path is seen or reviving a stale
        one. ``data_type`` is inferred from the first value; ``unit`` lands
        in ``meta.unit``; a multi-segment path's parent becomes
        ``meta.element`` (node-local)."""
        tag = self._tags.get(path)
        if tag is None:
            meta: dict[str, Any] = {}
            element = element_for(path, self.mount)
            if element:
                meta["element"] = element
            if unit is not None:
                meta["unit"] = unit
            tag = DataTag(
                id=str(ulid_lib.new()),
                name=path.rsplit("/", 1)[-1],
                source=path,
                is_writable=False,
                is_readable=True,
                data_type=infer_data_type(value),
                is_stale=False,
                meta=meta,
            )
            self._tags[path] = tag
            self.dirty = True
            return tag.id, True
        changed = tag.is_stale
        data_type = tag.data_type
        if data_type is None:
            data_type = infer_data_type(value)
            changed = True
        meta = dict(tag.meta)
        if unit is not None and meta.get("unit") != unit:
            meta["unit"] = unit
            changed = True
        if changed:
            self._tags[path] = replace(tag, is_stale=False, data_type=data_type, meta=meta)
            self.dirty = True
        return tag.id, changed

    def declare(self, tags: Mapping[str, DataTag]) -> None:
        """The discovery path: the complete set of sources a discovery
        found, keyed by ``source``. Each keeps the id it already had (or
        mints one); every known source NOT in ``tags`` is carried forward
        marked stale. The ``id`` on the given tags is ignored — the
        catalogue is the one place ids are minted."""
        merged: dict[str, DataTag] = {}
        for source, raw in tags.items():
            old = self._tags.get(source)
            merged[source] = DataTag(
                id=old.id if old is not None else str(ulid_lib.new()),
                name=raw.name,
                source=source,
                is_writable=raw.is_writable,
                is_readable=raw.is_readable,
                data_type=raw.data_type,
                is_stale=False,
                meta=raw.meta,
            )
        for source, old in self._tags.items():
            if source not in merged:
                merged[source] = old if old.is_stale else replace(old, is_stale=True)
        self._tags = merged
        self.dirty = True

    def seal(self, seen: set[str]) -> bool:
        """Mark every known path NOT in ``seen`` stale — the ``publish()``
        path's end-of-run rule (a path not published this run is gone).
        True if anything changed."""
        changed = False
        for source, tag in list(self._tags.items()):
            if source not in seen and not tag.is_stale:
                self._tags[source] = replace(tag, is_stale=True)
                changed = True
        if changed:
            self.dirty = True
        return changed

    # -- reads -----------------------------------------------------------

    def tag_id(self, source: str) -> str | None:
        tag = self._tags.get(source)
        return tag.id if tag else None

    def source_for_tag(self, tag_id: str) -> str | None:
        for source, tag in self._tags.items():
            if tag.id == tag_id:
                return source
        return None

    def tag(self, source: str) -> DataTag | None:
        return self._tags.get(source)

    def __contains__(self, tag_id: object) -> bool:
        return any(tag.id == tag_id for tag in self._tags.values())

    def __len__(self) -> int:
        return len(self._tags)

    def data_tags(self) -> list[DataTag]:
        return list(self._tags.values())

    def payload(self) -> DataTags:
        return DataTags(data_tags=self.data_tags(), connector=self.connector)

    def revision(self, payload: DataTags | None = None) -> str:
        """What the republish guard compares: the identity the record is
        published under plus the content hash of its tags. A re-registered
        service (new ULID, same tags) republishes; a restart that
        rediscovers the same source does not."""
        payload = payload if payload is not None else self.payload()
        return f"{self.connector}\x00{payload.version}"

    def record_published(self, revision: str) -> None:
        self.last_published_revision = revision
        self.dirty = False
