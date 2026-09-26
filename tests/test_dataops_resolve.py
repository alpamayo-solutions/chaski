"""Tests for chaski.dataops.resolve.

A fake ``Door`` returns a canned KV snapshot. Covered: matching by name and
element, tombstones never matching, and a moved or rebound signal resolving to
its new ULID on the next call.
"""

from __future__ import annotations

import pytest

from chaski.dataops import resolve
from chaski.door import KvEntry


def _entry(topic: str, payload: dict | None) -> KvEntry:
    return KvEntry(path="p", node_id="n-1", topic=topic, payload=payload, ts=0.0, offset=1)


class FakeDoor:
    """A Door stand-in whose ``kv()`` always returns the current snapshot —
    mutate ``.entries`` between calls to simulate a KV change."""

    def __init__(self, entries: list[KvEntry]):
        self.entries = entries
        self.calls = 0

    def kv(self, prefix: str, *, contract=None) -> list[KvEntry]:
        self.calls += 1
        return list(self.entries)


# ─── resolve_signal: unscoped ───────────────────────────────────────────


def test_resolve_signal_unscoped_matches_by_name():
    door = FakeDoor(
        [
            _entry("colca/v1/_Signal/n-1/line1/heartbeat", {"id": "sig-1", "name": "heartbeat"}),
            _entry("colca/v1/_Signal/n-1/line1/part_counter", {"id": "sig-2", "name": "part_counter"}),
        ]
    )

    assert resolve.resolve_signal(door, "heartbeat") == "sig-1"
    assert resolve.resolve_signal(door, "part_counter") == "sig-2"


def test_resolve_signal_returns_none_when_no_match():
    door = FakeDoor([_entry("colca/v1/_Signal/n-1/line1/heartbeat", {"id": "sig-1", "name": "heartbeat"})])

    assert resolve.resolve_signal(door, "does_not_exist") is None


def test_resolve_signal_ignores_non_signal_entries():
    door = FakeDoor(
        [
            # Same "name" field, but not a _Signal record — must not match.
            _entry("colca/v1/_Metric/n-1/line1/heartbeat", {"signal_id": "sig-1", "name": "heartbeat"}),
            _entry("colca/v1/_SystemElement/n-1/line1", {"id": "se-1", "name": "heartbeat"}),
        ]
    )

    assert resolve.resolve_signal(door, "heartbeat") is None


def test_resolve_signal_skips_tombstones():
    # A retired _Signal is a retained EMPTY payload — must never match.
    door = FakeDoor(
        [
            _entry("colca/v1/_Signal/n-1/line1/heartbeat", {}),
            _entry("colca/v1/_Signal/n-1/line1/heartbeat2", {"id": "sig-2", "name": "heartbeat"}),
        ]
    )

    assert resolve.resolve_signal(door, "heartbeat") == "sig-2"


# ─── resolve_signal: scoped by system element ───────────────────────────


def test_resolve_signal_scoped_to_system_element():
    door = FakeDoor(
        [
            _entry("colca/v1/_SystemElement/n-1/press01", {"id": "se-1", "name": "Press01"}),
            _entry("colca/v1/_SystemElement/n-1/press02", {"id": "se-2", "name": "Press02"}),
            _entry(
                "colca/v1/_Signal/n-1/press01/machine_status",
                {"id": "sig-p1", "name": "machine_status", "system_element_id": "se-1"},
            ),
            _entry(
                "colca/v1/_Signal/n-1/press02/machine_status",
                {"id": "sig-p2", "name": "machine_status", "system_element_id": "se-2"},
            ),
        ]
    )

    assert resolve.resolve_signal(door, "machine_status", "Press01") == "sig-p1"
    assert resolve.resolve_signal(door, "machine_status", "Press02") == "sig-p2"


def test_resolve_signal_scoped_to_unknown_element_returns_none():
    door = FakeDoor(
        [
            _entry(
                "colca/v1/_Signal/n-1/press01/machine_status",
                {"id": "sig-p1", "name": "machine_status", "system_element_id": "se-1"},
            ),
        ]
    )

    assert resolve.resolve_signal(door, "machine_status", "NoSuchElement") is None


# ─── the whole point: re-resolution after a KV change ───────────────────


def test_a_rebind_is_reflected_on_the_very_next_call():
    """The rule the whole module exists for: no resolved id is remembered."""
    door = FakeDoor(
        [
            _entry("colca/v1/_Signal/n-1/line1/heartbeat", {"id": "sig-old", "name": "heartbeat"}),
        ]
    )

    # First confirm the old id resolves.
    assert resolve.resolve_signal(door, "heartbeat") == "sig-old"

    door.entries = [
        _entry("colca/v1/_Signal/n-1/line1/heartbeat", {}),  # tombstoned
        _entry("colca/v1/_Signal/n-1/line1/heartbeat-2", {"id": "sig-new", "name": "heartbeat"}),
    ]

    assert resolve.resolve_signal(door, "heartbeat") == "sig-new"
    assert door.calls == 2, "each call must re-read KV, not serve a remembered id"


def test_one_pass_over_many_inputs_costs_one_scan():
    """Resolving many inputs inside one pass reads KV once."""
    entries = [_entry("colca/v1/_SystemElement/n-1/line1", {"id": "el-1", "name": "line1"})]
    for index in range(30):
        entries.append(
            _entry(
                f"colca/v1/_Signal/n-1/line1/sig{index}",
                {"id": f"sig-{index}", "name": f"sig{index}", "system_element_id": "el-1"},
            )
        )
    door = FakeDoor(entries)

    with resolve.one_pass(door):
        resolved = [resolve.resolve_signal(door, f"sig{index}", "line1") for index in range(30)]

    assert resolved == [f"sig-{index}" for index in range(30)]
    assert door.calls == 1, f"{door.calls} scans for one pass — colca allows 5 per second"


def test_a_pass_that_cannot_read_is_not_fatal():
    """A failed read does not raise: the pass does not pin, and each resolver
    reads on its own.
    """

    class RefusingDoor(FakeDoor):
        def kv(self, prefix, *, contract=None):
            self.calls += 1
            raise RuntimeError("429 Too Many Requests")

    door = RefusingDoor([])

    # The pass itself must not raise, and the resolver still reports the failure.
    with resolve.one_pass(door), pytest.raises(RuntimeError):
        resolve.resolve_signal(door, "heartbeat")


def test_a_nested_pass_reuses_the_outer_read():
    # Replay opens a pass over all producers and dispatch one per producer
    # inside it; the inner passes must reuse the outer read.
    door = FakeDoor([_entry("colca/v1/_Signal/n-1/l/s", {"id": "sig-1", "name": "s"})])

    with resolve.one_pass(door):
        for _ in range(5):
            with resolve.one_pass(door):
                assert resolve.resolve_signal(door, "s") == "sig-1"

    assert door.calls == 1, f"{door.calls} scans for one outer pass with five nested"


def test_the_pin_does_not_outlive_its_pass():
    """After the block, a lookup reads KV again and sees what changed."""
    door = FakeDoor([_entry("colca/v1/_Signal/n-1/l/s", {"id": "sig-old", "name": "s"})])

    with resolve.one_pass(door):
        assert resolve.resolve_signal(door, "s") == "sig-old"
        door.entries = [_entry("colca/v1/_Signal/n-1/l/s", {"id": "sig-new", "name": "s"})]
        # Inside the pass, the pinned read still answers — that IS the pin.
        assert resolve.resolve_signal(door, "s") == "sig-old"

    assert resolve.resolve_signal(door, "s") == "sig-new"
    assert door.calls == 2, "the pass took one read, and the call after it took another"


# ─── resolve_annotation_type ────────────────────────────────────────────


def test_resolve_annotation_type_matches_by_name():
    door = FakeDoor(
        [
            _entry("colca/v1/_AnnotationType/n-1/downtime", {"id": "at-1", "name": "downtime"}),
        ]
    )

    assert resolve.resolve_annotation_type(door, "downtime") == "at-1"


def test_resolve_annotation_type_returns_none_when_no_match():
    door = FakeDoor([])

    assert resolve.resolve_annotation_type(door, "downtime") is None
