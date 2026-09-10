"""The catalogue every Service publishes: a source keeps its id for as long as
it is known, also across restarts through the node's retained record; a
vanished source stays marked stale; and an unchanged catalogue is not
republished."""

from __future__ import annotations

import re

import colca_data_contracts  # noqa: F401 - installs the UNS "prefix=colca" patch
from colca_data_contracts.payload import DataTag

from chaski.catalogue import Catalogue, element_for, infer_data_type

ULID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}")


def _tag(name: str, source: str | None = None, data_type: str = "float", **meta) -> DataTag:
    return DataTag(
        id="", name=name, source=source or name, is_writable=False, is_readable=True, data_type=data_type, meta=meta
    )


def _retained(cat: Catalogue) -> dict:
    """What the node hands back from ``/kv`` for the record ``cat`` published:
    the payload's own dict form, ``version`` included."""
    return cat.payload().__dict__


def test_infer_data_type_orders_bool_before_int():
    # bool is an int subclass in Python — this must not misreport a flag as
    # a number.
    assert infer_data_type(True) == "bool"
    assert infer_data_type(3) == "int"
    assert infer_data_type(3.5) == "float"
    assert infer_data_type("running") == "string"


def test_element_for_is_the_paths_parent_or_empty_for_a_top_level_path():
    assert element_for("line1/press3/temp") == "line1/press3"
    assert element_for("temp") == ""


def test_element_for_joins_the_mount_to_make_the_parent_node_local():
    # The node resolves meta.element as a node-local path, so the parent is
    # joined onto the mount.
    assert element_for("press3/temp", mount="line1") == "line1/press3"
    # A top-level path has no parent at all — no meta.element, not "the
    # mount itself" — so nothing is joined.
    assert element_for("temp", mount="line1") == ""
    # An unplaced service (mount "") is the identity join: joinPath("", x) == x.
    assert element_for("press3/temp", mount="") == "press3"


# -- the publish() path: ensure ------------------------------------------


def test_ensure_emits_the_mount_joined_element_and_the_unit_for_a_published_path():
    cat = Catalogue(connector="svc1", mount="line1")
    cat.ensure("press3/temp", 21.5, unit="°C")
    tag = cat.data_tags()[0]
    assert tag.meta == {"element": "line1/press3", "unit": "°C"}
    assert tag.name == "temp" and tag.source == "press3/temp" and tag.data_type == "float"


def test_ensure_emits_no_element_for_a_top_level_path_on_a_mounted_catalogue():
    # Without a parent there is no meta.element; the node places the tag at
    # the service's mount by default.
    cat = Catalogue(connector="svc1", mount="line1")
    cat.ensure("temp", 21.5)
    assert "element" not in cat.data_tags()[0].meta


def test_the_catalogue_grows_monotonically_as_new_paths_are_published():
    cat = Catalogue(connector="svc1")
    id_a, grew_a = cat.ensure("a", 1)
    id_b, grew_b = cat.ensure("b", 2)
    assert grew_a and grew_b
    assert id_a != id_b and ULID.fullmatch(id_a) and ULID.fullmatch(id_b)
    assert {tag.source for tag in cat.data_tags()} == {"a", "b"}
    same_id_a, grew_again = cat.ensure("a", 1)
    assert same_id_a == id_a
    assert grew_again is False
    assert {tag.source for tag in cat.data_tags()} == {"a", "b"}


def test_seal_marks_a_path_not_seen_this_run_stale_and_a_seen_one_survives():
    cat = Catalogue(connector="svc1")
    cat.ensure("kept", 1)
    cat.ensure("vanished", 2)

    assert cat.seal(seen={"kept"}) is True

    tags = {tag.source: tag for tag in cat.data_tags()}
    # "kept" must not be stale, or the check on "vanished" proves nothing.
    assert tags["kept"].is_stale is False
    assert tags["vanished"].is_stale is True
    assert cat.seal(seen={"kept"}) is False, "sealing again with nothing new to stale changes nothing"


def test_a_revived_path_is_un_staled_and_reported_as_changed():
    cat = Catalogue(connector="svc1")
    cat.ensure("flaky", 1)
    cat.seal(seen=set())
    assert cat.data_tags()[0].is_stale is True

    tag_id, changed = cat.ensure("flaky", 2)

    assert changed is True
    assert cat.data_tags()[0].is_stale is False
    assert cat.tag_id("flaky") == tag_id


# -- the discovery path: declare -----------------------------------------


def test_declare_mints_an_id_per_source_and_the_id_is_reused_by_a_later_declare():
    cat = Catalogue(connector="svc1")
    cat.declare({"Axis1/Temperature": _tag("Temperature", "Axis1/Temperature")})
    first = cat.tag_id("Axis1/Temperature")
    assert first and ULID.fullmatch(first)

    cat.declare(
        {
            "Axis1/Temperature": _tag("Temperature", "Axis1/Temperature"),
            "Axis2/Temperature": _tag("Temperature", "Axis2/Temperature"),
        }
    )
    assert cat.tag_id("Axis1/Temperature") == first, "rediscovery minted a new id; every bound signal just broke"
    assert cat.tag_id("Axis2/Temperature") not in (None, first)


def test_declare_carries_a_vanished_source_forward_stale_and_revives_it_when_it_returns():
    cat = Catalogue(connector="svc1")
    cat.declare({"a": _tag("a"), "b": _tag("b")})
    id_b = cat.tag_id("b")

    cat.declare({"a": _tag("a")})
    tags = {t.source: t for t in cat.data_tags()}
    assert tags["a"].is_stale is False
    assert tags["b"].is_stale is True and tags["b"].id == id_b, "a vanished tag must keep its identity"

    cat.declare({"a": _tag("a"), "b": _tag("b")})
    back = cat.tag("b")
    assert back.id == id_b and back.is_stale is False, "coming back must not mint a new id either"


def test_declare_ignores_the_id_a_driver_puts_on_a_tag():
    """The catalogue is the one place ids are minted: a driver that
    restates a tag with some id of its own does not get to change the
    identity a Signal is bound to."""
    cat = Catalogue(connector="svc1")
    cat.declare({"a": _tag("a")})
    minted = cat.tag_id("a")
    forged = _tag("a")
    forged.id = "01FORGED00000000000000000"
    cat.declare({"a": forged})
    assert cat.tag_id("a") == minted


# -- the memory: the node's retained record ------------------------------


def test_a_restart_reuses_every_id_from_the_retained_record_and_the_guard_is_armed():
    """A fresh process seeded from the node keeps every id and matches the
    revision on record, so an unchanged catalogue is not republished."""
    first = Catalogue(connector="svc1")
    first.declare({"a": _tag("a", meta_key=1), "b": _tag("b", data_type="int")})
    first.declare({"a": _tag("a", meta_key=1)})  # b vanishes -> stale, carried forward
    first.record_published(first.revision())
    retained = _retained(first)
    assert set(retained) >= {"data_tags", "connector", "version"}

    second = Catalogue(connector="svc1")
    second.load_previous(retained)
    assert second.dirty is False
    assert {t.source: t.id for t in second.data_tags()} == {t.source: t.id for t in first.data_tags()}
    assert second.tag("b").is_stale is True

    # The same discovery again: same ids, same content, same revision.
    second.declare({"a": _tag("a", meta_key=1)})
    assert second.dirty is True
    assert second.revision() == second.last_published_revision == first.last_published_revision

    # Denominator: a genuinely changed catalogue does NOT match.
    second.declare({"a": _tag("a", meta_key=2)})
    assert second.revision() != second.last_published_revision


def test_the_revision_carries_the_publishing_identity_so_a_re_registered_service_republishes():
    """The content hash alone is not the guard: the record's ``connector``
    field is the service's identity, and a re-registration that minted a
    new ULID must publish again even with identical tags."""
    old = Catalogue(connector="ulid-old")
    old.declare({"a": _tag("a")})
    old.record_published(old.revision())

    new = Catalogue(connector="ulid-new")
    new.load_previous(_retained(old))
    new.declare({"a": _tag("a")})
    assert new.revision() != new.last_published_revision
    assert new.payload().connector == "ulid-new"


def test_loading_nothing_is_a_valid_first_run():
    cat = Catalogue(connector="svc1")
    cat.load_previous(None)
    cat.load_previous({})
    assert len(cat) == 0 and cat.last_published_revision is None and cat.dirty is False
