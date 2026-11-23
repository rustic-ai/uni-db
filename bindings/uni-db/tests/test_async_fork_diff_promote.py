# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Phase 7c — async Python bindings for fork diff & promote.

Mirrors `test_fork_diff_promote.py` against ``AsyncUni`` to close the
async surface coverage gap. The ``PromotePattern.label`` /
``edge_type`` constructors are class methods on a shared pyclass, so
they work identically from async-context tests.
"""

from __future__ import annotations

import uni_db


async def _make_db(**config_kwargs):
    builder = uni_db.AsyncUni.builder()
    for k, v in config_kwargs.items():
        builder = getattr(builder, k)(v)
    return await builder.build()


async def _seed_person_schema(db):
    schema = db.schema()
    await schema.label("Person").property("name", "string").apply()
    s = db.session()
    tx = await s.tx()
    await tx.execute("CREATE (:Person {name: 'Alice'})")
    await tx.commit()
    await db.flush()
    return s


async def test_async_diff_fork_primary_empty_on_fresh_fork():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)
    fork = await primary.fork("audit").build()
    del fork
    diff = await db.diff_fork_primary("audit")
    assert diff.is_empty()
    assert diff.total_rows() == 0


async def test_async_diff_fork_primary_reports_added_rows():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)
    fork = await primary.fork("staging").build()
    tx = await fork.tx()
    await tx.execute("CREATE (:Person {name: 'Bob'})")
    await tx.execute("CREATE (:Person {name: 'Carol'})")
    await tx.commit()
    del fork

    diff = await db.diff_fork_primary("staging")
    assert len(diff.vertices.added) == 2, repr(
        [v.properties for v in diff.vertices.added]
    )
    assert len(diff.vertices.deleted) == 0

    for v in diff.vertices.added:
        assert isinstance(v.uid, str) and len(v.uid) > 0
        assert "name" in v.properties

    inverted = diff.invert()
    assert len(inverted.vertices.added) == 0
    assert len(inverted.vertices.deleted) == 2


async def test_async_diff_forks_between_siblings():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)

    a = await primary.fork("left").build()
    tx = await a.tx()
    await tx.execute("CREATE (:Person {name: 'L-only'})")
    await tx.commit()
    del a

    b = await primary.fork("right").build()
    tx = await b.tx()
    await tx.execute("CREATE (:Person {name: 'R-only'})")
    await tx.commit()
    del b

    diff = await db.diff_forks("left", "right")
    assert len(diff.vertices.added) == 1
    assert len(diff.vertices.deleted) == 1


async def test_async_promote_inserts_fork_only_vertices():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)

    fork = await primary.fork("publish").build()
    tx = await fork.tx()
    await tx.execute("CREATE (:Person {name: 'NewKid-1'})")
    await tx.execute("CREATE (:Person {name: 'NewKid-2'})")
    await tx.commit()
    del fork

    report = await db.promote_from_fork(
        "publish", [uni_db.PromotePattern.label("Person")]
    )
    assert report.vertices_inserted >= 2, repr(report)
    assert report.edges_inserted == 0


async def test_async_promote_edges_lands_both_endpoints_and_edge():
    db = await _make_db(disable_fork_sweeper=True)
    await db.schema().label("Person").property("name", "string").apply()
    await (
        db.schema()
        .edge_type("KNOWS", ["Person"], ["Person"])
        .property("since", "int64")
        .apply()
    )

    primary = db.session()
    tx = await primary.tx()
    await tx.execute("CREATE (:Person {name: 'Anchor'})")
    await tx.commit()
    await db.flush()

    fork = await primary.fork("rel").build()
    tx = await fork.tx()
    await tx.execute(
        "CREATE (:Person {name: 'A'})-[:KNOWS {since: 2020}]->(:Person {name: 'B'})"
    )
    await tx.commit()
    del fork

    report = await db.promote_from_fork(
        "rel",
        [
            uni_db.PromotePattern.label("Person"),
            uni_db.PromotePattern.edge_type("KNOWS"),
        ],
    )
    assert report.vertices_inserted >= 2, repr(report)
    assert report.edges_inserted == 1, repr(report)

    # Primary now sees the promoted edge end-to-end.
    rs = await primary.query(
        "MATCH (a:Person {name: 'A'})-[r:KNOWS]->(b:Person {name: 'B'}) "
        "RETURN r.since AS since"
    )
    rows = rs.rows
    assert len(rows) == 1
    assert rows[0].get("since") == 2020


async def test_async_promote_with_options_reports_new_fields():
    """Async promote_from_fork_with_options exposes the new merge counters."""
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)

    fork = await primary.fork("publish").build()
    tx = await fork.tx()
    await tx.execute("CREATE (:Person {name: 'NewKid'})")
    await tx.commit()
    del fork

    report = await db.promote_from_fork_with_options(
        "publish",
        [uni_db.PromotePattern.label("Person")],
        uni_db.PromoteOptions(upsert=True),
    )
    assert report.vertices_inserted >= 1, repr(report)
    assert report.vertices_updated == 0
    assert report.vertices_conflicting == 0
    assert report.vertices_deleted == 0


async def test_async_diff_pairs_edited_vertex_into_changed():
    db = await _make_db(disable_fork_sweeper=True)
    await db.schema().label("Doc").property("title", "string").apply()
    s = db.session()
    tx = await s.tx()
    await tx.execute("CREATE (:Doc {ext_id: 'd1', title: 'Draft'})")
    await tx.commit()
    await db.flush()
    fork = await s.fork("edit").build()
    tx = await fork.tx()
    await tx.execute("MATCH (d:Doc {ext_id: 'd1'}) SET d.title = 'Final'")
    await tx.commit()
    del fork

    diff = await db.diff_fork_primary("edit")
    assert diff.vertices.added == []
    assert diff.vertices.deleted == []
    assert len(diff.vertices.changed) == 1
    change = diff.vertices.changed[0]
    assert change.ext_id == "d1"
    title = next(c for c in change.changes if c.key == "title")
    assert (title.before, title.after) == ("Draft", "Final")
