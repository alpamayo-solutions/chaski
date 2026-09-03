"""Stable id-per-path catalogue for :class:`chaski.Service` (SDK design §3,
§7 gap 3).

Mirrors ``connector/src/reader.py``'s ``_finalize_catalogue`` /
``publish_catalogue`` id-stability and content-hash republish guard, but
keyed off explicit ``publish()`` calls rather than protocol discovery, and
persisted to a small local JSON file (rather than re-derived from the node's
retained catalogue) so a restarted ``Service`` process reuses the same
``DataTag`` id for the same path without needing to reach the node first.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

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


@dataclass
class _Entry:
    id: str
    data_type: Optional[str] = None
    is_stale: bool = False
    unit: Optional[str] = None


class Catalogue:
    """The path -> DataTag mapping for one Service, persisted at ``path``.

    ``ensure(path, value, unit)`` is the mutator ``publish()`` needs: it
    mints a new id the first time a path is seen — a path's id never changes
    afterward, the same natural-key rule the connector's own catalogue keeps
    (``source`` is the natural key) — and revives a path that had gone
    stale. ``seal(seen)`` is the restart-shaped half: any known path NOT in
    ``seen`` (not published at all during this process's lifetime) is marked
    stale, carrying it forward rather than deleting it — exactly what
    ``_finalize_catalogue`` does with a vanished connector tag.
    """

    def __init__(self, path: Path, *, connector: str, mount: str = "") -> None:
        self._path = path
        self.connector = connector
        self._mount = mount
        self._entries: dict[str, _Entry] = {}
        self.last_published_revision: Optional[str] = None
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text(encoding="utf-8"))
        for source, item in raw.get("tags", {}).items():
            self._entries[source] = _Entry(
                id=item["id"],
                data_type=item.get("data_type"),
                is_stale=bool(item.get("is_stale", False)),
                unit=item.get("unit"),
            )
        self.last_published_revision = raw.get("last_published_revision")

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "tags": {
                source: {
                    "id": e.id,
                    "data_type": e.data_type,
                    "is_stale": e.is_stale,
                    "unit": e.unit,
                }
                for source, e in self._entries.items()
            },
            "last_published_revision": self.last_published_revision,
        }
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self._path)

    def ensure(self, path: str, value: Any, unit: Optional[str] = None) -> tuple[str, bool]:
        """Return (tag_id, changed) for `path`, minting or reviving it."""
        entry = self._entries.get(path)
        if entry is None:
            entry = _Entry(id=str(ulid_lib.new()), data_type=infer_data_type(value), unit=unit)
            self._entries[path] = entry
            self._save()
            return entry.id, True
        changed = entry.is_stale
        entry.is_stale = False
        if entry.data_type is None:
            entry.data_type = infer_data_type(value)
            changed = True
        if unit is not None and entry.unit != unit:
            entry.unit = unit
            changed = True
        if changed:
            self._save()
        return entry.id, changed

    def seal(self, seen: set[str]) -> bool:
        """Mark every known path NOT in `seen` stale. True if anything changed."""
        changed = False
        for source, entry in self._entries.items():
            if source not in seen and not entry.is_stale:
                entry.is_stale = True
                changed = True
        if changed:
            self._save()
        return changed

    def tag_id(self, path: str) -> Optional[str]:
        entry = self._entries.get(path)
        return entry.id if entry else None

    def source_for_tag(self, tag_id: str) -> Optional[str]:
        for source, entry in self._entries.items():
            if entry.id == tag_id:
                return source
        return None

    def data_tags(self) -> list[DataTag]:
        tags = []
        for source, entry in self._entries.items():
            leaf = source.rsplit("/", 1)[-1]
            meta: dict[str, Any] = {}
            element = element_for(source, self._mount)
            if element:
                meta["element"] = element
            if entry.unit is not None:
                meta["unit"] = entry.unit
            tags.append(
                DataTag(
                    id=entry.id,
                    name=leaf,
                    source=source,
                    is_writable=False,
                    is_readable=True,
                    data_type=entry.data_type,
                    is_stale=entry.is_stale,
                    meta=meta,
                )
            )
        return tags

    def payload(self) -> DataTags:
        return DataTags(data_tags=self.data_tags(), connector=self.connector)

    def record_published(self, revision: str) -> None:
        self.last_published_revision = revision
        self._save()
