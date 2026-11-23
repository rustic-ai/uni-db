# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Phase 7 — sync Python bindings for fork diff & promote.

Covers ``Uni.diff_fork_primary``, ``Uni.diff_forks``, and
``Uni.promote_from_fork`` plus the ``PromotePattern`` builder and the
``ForkDiff`` / ``PromoteReport`` types.

Mirrors the Rust integration tests in
``crates/uni/tests/fork_diff.rs``, ``fork_promote.rs``, and
``fork_promote_edges.rs``.
"""

from __future__ import annotations

import uni_db


def _make_db():
    return uni_db.Uni.builder().disable_fork_sweeper(True).build()


def _seed_person_schema(db):
    db.schema().label("Person").property("name", "string").apply()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'Alice'})")
    tx.commit()
    db.flush()
    return s


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


def test_diff_fork_primary_empty_on_fresh_fork():
    db = _make_db()
    primary = _seed_person_schema(db)
    fork = primary.fork("audit").build()
    del fork  # release the session
    diff = db.diff_fork_primary("audit")
    assert diff.is_empty()
    assert diff.total_rows() == 0


def test_diff_fork_primary_reports_added_rows():
    db = _make_db()
    primary = _seed_person_schema(db)
    fork = primary.fork("staging").build()
    tx = fork.tx()
    tx.execute("CREATE (:Person {name: 'Bob'})")
    tx.execute("CREATE (:Person {name: 'Carol'})")
    tx.commit()
    del fork

    diff = db.diff_fork_primary("staging")
    assert len(diff.vertices.added) == 2, (
        f"expected 2 fork-only adds, got {[v.properties for v in diff.vertices.added]}"
    )
    assert len(diff.vertices.deleted) == 0
    assert len(diff.vertices.changed) == 0

    # UID is exposed as a string; VID is an int (or None).
    for v in diff.vertices.added:
        assert isinstance(v.uid, str) and len(v.uid) > 0
        assert v.vid is None or isinstance(v.vid, int)
        assert "name" in v.properties

    # Inversion swaps added/deleted.
    inverted = diff.invert()
    assert len(inverted.vertices.added) == 0
    assert len(inverted.vertices.deleted) == 2


def test_diff_forks_between_two_siblings():
    db = _make_db()
    primary = _seed_person_schema(db)

    a = primary.fork("left").build()
    tx = a.tx()
    tx.execute("CREATE (:Person {name: 'L-only'})")
    tx.commit()
    del a

    b = primary.fork("right").build()
    tx = b.tx()
    tx.execute("CREATE (:Person {name: 'R-only'})")
    tx.commit()
    del b

    diff = db.diff_forks("left", "right")
    assert len(diff.vertices.added) == 1, "R-only should be added in diff(left, right)"
    assert len(diff.vertices.deleted) == 1, "L-only should be deleted"


# ---------------------------------------------------------------------------
# Promote
# ---------------------------------------------------------------------------


def test_promote_inserts_fork_only_vertices():
    db = _make_db()
    primary = _seed_person_schema(db)

    fork = primary.fork("publish").build()
    tx = fork.tx()
    tx.execute("CREATE (:Person {name: 'NewKid-1'})")
    tx.execute("CREATE (:Person {name: 'NewKid-2'})")
    tx.commit()
    del fork

    report = db.promote_from_fork(
        "publish",
        [uni_db.PromotePattern.label("Person")],
    )
    assert report.vertices_inserted >= 2, repr(report)
    assert report.edges_inserted == 0

    # Primary now has Alice + the two new kids.
    names = sorted(
        r.get("name")
        for r in primary.query("MATCH (p:Person) RETURN p.name AS name").rows
    )
    assert "Alice" in names
    assert "NewKid-1" in names
    assert "NewKid-2" in names


def test_promote_pattern_constructors():
    """Both PromotePattern.label and PromotePattern.edge_type build, and
    the where_clause kwarg attaches a Cypher predicate."""
    v = uni_db.PromotePattern.label("Person")
    assert v.kind == "vertex"
    v_with_where = uni_db.PromotePattern.label("Person", where_clause="n.age > 18")
    assert v_with_where.kind == "vertex"
    e = uni_db.PromotePattern.edge_type("KNOWS")
    assert e.kind == "edge"
    e_with_where = uni_db.PromotePattern.edge_type(
        "KNOWS", where_clause="r.since > 2020"
    )
    assert e_with_where.kind == "edge"


def test_promote_edges_lands_both_endpoints_and_edge():
    db = _make_db()
    db.schema().label("Person").property("name", "string").apply()
    db.schema().edge_type("KNOWS", ["Person"], ["Person"]).property(
        "since", "int64"
    ).apply()

    primary = db.session()
    tx = primary.tx()
    tx.execute("CREATE (:Person {name: 'Anchor'})")
    tx.commit()
    db.flush()

    fork = primary.fork("rel").build()
    tx = fork.tx()
    tx.execute(
        "CREATE (:Person {name: 'A'})-[:KNOWS {since: 2020}]->(:Person {name: 'B'})"
    )
    tx.commit()
    del fork

    report = db.promote_from_fork(
        "rel",
        [
            uni_db.PromotePattern.label("Person"),
            uni_db.PromotePattern.edge_type("KNOWS"),
        ],
    )
    assert report.vertices_inserted >= 2, repr(report)
    assert report.edges_inserted == 1, repr(report)
    assert report.edges_skipped_no_endpoint == 0
    assert report.edges_skipped_duplicate == 0

    # Primary sees the promoted edge.
    rows = primary.query(
        "MATCH (a:Person {name: 'A'})-[r:KNOWS]->(b:Person {name: 'B'}) "
        "RETURN r.since AS since"
    ).rows
    assert len(rows) == 1
    assert rows[0].get("since") == 2020


# ---------------------------------------------------------------------------
# Promote with options (M4 merge: upsert / delete-promotion / conflict)
# ---------------------------------------------------------------------------


def test_promote_options_and_conflict_policy():
    """PromoteOptions + ConflictPolicy construct and expose their fields."""
    o = uni_db.PromoteOptions(upsert=True)
    assert o.upsert is True
    assert o.delete_promotion is False

    m = uni_db.PromoteOptions(delete_promotion=True)
    assert m.upsert is True  # delete implies the ext_id resolution
    assert m.delete_promotion is True

    assert uni_db.ConflictPolicy.Skip != uni_db.ConflictPolicy.Overwrite
    uni_db.PromoteOptions(
        upsert=True,
        delete_promotion=True,
        on_conflict=uni_db.ConflictPolicy.Overwrite,
    )


def test_promote_with_options_reports_new_fields():
    """promote_from_fork_with_options is callable and the report exposes the
    new merge counters."""
    db = _make_db()
    primary = _seed_person_schema(db)

    fork = primary.fork("publish").build()
    tx = fork.tx()
    tx.execute("CREATE (:Person {name: 'NewKid'})")
    tx.commit()
    del fork

    report = db.promote_from_fork_with_options(
        "publish",
        [uni_db.PromotePattern.label("Person")],
        uni_db.PromoteOptions(upsert=True),
    )
    assert report.vertices_inserted >= 1, repr(report)
    # New M4/M5 counters are accessible.
    assert report.vertices_updated == 0
    assert report.vertices_skipped_no_op == 0
    assert report.vertices_inserted_unverified == 0
    assert report.vertices_deleted == 0
    assert report.vertices_conflicting == 0


def test_promote_merge_delete_promotion(tmp_path):
    """Full merge: a fork deletion propagates to primary, a primary-only row
    the fork never saw survives (anti-spurious-delete)."""
    db = uni_db.Uni.open(str(tmp_path / "db"))
    db.schema().label("Person").property("name", "string").apply()
    primary = db.session()
    tx = primary.tx()
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice'})")
    tx.execute("CREATE (:Person {ext_id: 'p2', name: 'Bob'})")
    tx.commit()
    db.flush()

    # Fork deletes the inherited p1.
    fork = primary.fork("del").build()
    ftx = fork.tx()
    ftx.execute("MATCH (n:Person {name: 'Alice'}) DETACH DELETE n")
    ftx.commit()
    del fork

    # Primary adds p3 after the fork — never seen by the fork.
    tx = primary.tx()
    tx.execute("CREATE (:Person {ext_id: 'p3', name: 'Carol'})")
    tx.commit()
    db.flush()

    report = db.promote_from_fork_with_options(
        "del",
        [uni_db.PromotePattern.label("Person")],
        uni_db.PromoteOptions(delete_promotion=True),
    )
    assert report.vertices_deleted == 1, repr(report)

    names = sorted(
        r.get("name")
        for r in primary.query("MATCH (p:Person) RETURN p.name AS name").rows
    )
    assert names == ["Bob", "Carol"], names  # Alice deleted; Carol survives


def _seed_doc_with_ext_id(db):
    db.schema().label("Doc").property("title", "string").apply()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Doc {ext_id: 'd1', title: 'Draft'})")
    tx.commit()
    db.flush()
    return s


def test_diff_pairs_edited_vertex_into_changed():
    db = _make_db()
    primary = _seed_doc_with_ext_id(db)
    fork = primary.fork("edit").build()
    tx = fork.tx()
    tx.execute("MATCH (d:Doc {ext_id: 'd1'}) SET d.title = 'Final'")
    tx.commit()
    del fork

    diff = db.diff_fork_primary("edit")
    assert diff.vertices.added == []
    assert diff.vertices.deleted == []
    assert len(diff.vertices.changed) == 1
    change = diff.vertices.changed[0]
    assert change.label == "Doc"
    assert change.ext_id == "d1"
    assert change.vid is not None
    assert change.uid != change.after_uid
    title = next(c for c in change.changes if c.key == "title")
    assert (title.before, title.after) == ("Draft", "Final")

    inverse = diff.invert().vertices.changed[0]
    title = next(c for c in inverse.changes if c.key == "title")
    assert (title.before, title.after) == ("Final", "Draft")
    assert inverse.uid == change.after_uid


def test_promote_report_has_no_vertices_skipped_no_uid():
    db = _make_db()
    _seed_person_schema(db)
    fork = db.session().fork("p").build()
    del fork
    report = db.promote_from_fork("p", [uni_db.PromotePattern.label("Person")])
    assert not hasattr(report, "vertices_skipped_no_uid")
