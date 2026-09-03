"""Level-2 pin for SDK design §9: the synthesised catalogue is stable across
restarts (same source -> same tag id), grows monotonically, and marks
vanished paths is_stale."""

from __future__ import annotations

from chaski.catalogue import Catalogue, element_for, infer_data_type


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
    # The node resolves meta.element as a NODE-LOCAL path (exec_configure.go
    # elementAt/authorElementAt), so a mounted service's parent must be
    # joined onto its mount before it is sent, or the node looks for a
    # root-level element that was never meant (SDK design §3 gap 1).
    assert element_for("press3/temp", mount="line1") == "line1/press3"
    # A top-level path has no parent at all — no meta.element, not "the
    # mount itself" — so nothing is joined.
    assert element_for("temp", mount="line1") == ""
    # An unplaced service (mount "") is the identity join: joinPath("", x) == x.
    assert element_for("press3/temp", mount="") == "press3"


def test_data_tags_emit_the_mount_joined_element_for_a_mounted_catalogue(tmp_path):
    cat = Catalogue(tmp_path / "catalogue.json", connector="svc1", mount="line1")
    cat.ensure("press3/temp", 21.5)

    tag = cat.data_tags()[0]
    assert tag.meta["element"] == "line1/press3"


def test_data_tags_emit_no_element_for_a_top_level_path_on_a_mounted_catalogue(tmp_path):
    # No parent means the tag stays placed at the service's own mount, which
    # the node already does by default when meta.element is absent —
    # sending "line1" here would wrongly narrow it to the mount itself.
    cat = Catalogue(tmp_path / "catalogue.json", connector="svc1", mount="line1")
    cat.ensure("temp", 21.5)

    tag = cat.data_tags()[0]
    assert "element" not in tag.meta


def test_a_new_path_mints_a_stable_id_reused_across_restarts(tmp_path):
    path = tmp_path / "catalogue.json"

    first = Catalogue(path, connector="svc1")
    tag_id, changed = first.ensure("orders/open", 42)
    assert changed is True

    # "restart": a fresh Catalogue instance reading the same state file.
    second = Catalogue(path, connector="svc1")
    same_id, changed_again = second.ensure("orders/open", 43)
    assert same_id == tag_id
    assert changed_again is False  # already known, not stale: nothing changed


def test_the_catalogue_grows_monotonically_as_new_paths_are_published(tmp_path):
    cat = Catalogue(tmp_path / "catalogue.json", connector="svc1")
    id_a, grew_a = cat.ensure("a", 1)
    id_b, grew_b = cat.ensure("b", 2)
    assert grew_a and grew_b
    assert id_a != id_b
    assert {tag.source for tag in cat.data_tags()} == {"a", "b"}
    # Publishing "a" again does not remint or drop it.
    same_id_a, grew_again = cat.ensure("a", 1)
    assert same_id_a == id_a
    assert grew_again is False
    assert {tag.source for tag in cat.data_tags()} == {"a", "b"}


def test_seal_marks_a_path_not_seen_this_run_stale_and_a_seen_one_survives(tmp_path):
    path = tmp_path / "catalogue.json"
    cat = Catalogue(path, connector="svc1")
    cat.ensure("kept", 1)
    cat.ensure("vanished", 2)

    changed = cat.seal(seen={"kept"})

    assert changed is True
    tags = {tag.source: tag for tag in cat.data_tags()}
    # Presence check first (testing.md: an absence assertion needs a
    # denominator) — "kept" must NOT be stale before trusting "vanished" is.
    assert tags["kept"].is_stale is False
    assert tags["vanished"].is_stale is True


def test_a_revived_path_is_un_staled_and_reported_as_changed(tmp_path):
    path = tmp_path / "catalogue.json"
    cat = Catalogue(path, connector="svc1")
    cat.ensure("flaky", 1)
    cat.seal(seen=set())  # nothing published this "run" -> flaky goes stale
    assert cat.data_tags()[0].is_stale is True

    tag_id, changed = cat.ensure("flaky", 2)

    assert changed is True
    assert cat.data_tags()[0].is_stale is False
    assert cat.tag_id("flaky") == tag_id


def test_seal_with_nothing_vanished_reports_no_change(tmp_path):
    cat = Catalogue(tmp_path / "catalogue.json", connector="svc1")
    cat.ensure("a", 1)
    assert cat.seal(seen={"a"}) is False


def test_record_published_persists_the_republish_guard_across_restarts(tmp_path):
    path = tmp_path / "catalogue.json"
    first = Catalogue(path, connector="svc1")
    first.ensure("a", 1)
    revision = first.payload().version
    first.record_published(revision)

    second = Catalogue(path, connector="svc1")
    assert second.last_published_revision == revision
