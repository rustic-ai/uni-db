# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Phase 4b — sync fork API tests.

Mirrored by `test_async_fork.py` for the async surface. Both files set
`disable_fork_sweeper=True` unless the test specifically exercises the
sweeper, so timing-sensitive assertions stay deterministic.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_db(**config_kwargs):
    """Build a temporary in-memory Uni with the given UniConfig overrides.

    `Uni.builder()` returns a temporary-mode `DatabaseBuilder`; chain
    setters then `.build()`. Direct `Uni.in_memory()` would skip the
    config plumbing entirely.
    """
    builder = uni_db.Uni.builder()
    for k, v in config_kwargs.items():
        builder = getattr(builder, k)(v)
    return builder.build()


def _seed_person_schema(db):
    db.schema().label("Person").property("name", "string").apply()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'seed'})")
    tx.commit()
    db.flush()
    return s


def _person_schema_no_flush(db):
    """Declare the Person schema and return a session — but DO NOT seed
    or flush. #97 regression tests commit their own data and must never
    flush before forking; flushing would mask the bug.
    """
    db.schema().label("Person").property("name", "string").apply()
    return db.session()


def _count(scope):
    return scope.query("MATCH (n:Person) RETURN count(n) AS c")[0]["c"]


# ---------------------------------------------------------------------------
# Basic lifecycle
# ---------------------------------------------------------------------------


def test_fork_basic_create_drop():
    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)

    fork = primary.fork("scenario_1").build()
    assert fork.is_forked()
    assert not primary.is_forked()

    info = db.fork_info("scenario_1")
    assert info is not None
    assert info.name == "scenario_1"
    assert info.status == uni_db.ForkStatus.Active
    assert info.parent_fork_id is None

    # Drop with the fork session still alive should error.
    with pytest.raises(uni_db.UniForkInUseError) as excinfo:
        db.drop_fork("scenario_1")
    assert excinfo.value.holder_count >= 1

    # Release and retry.
    del fork
    db.drop_fork("scenario_1")
    assert db.fork_info("scenario_1") is None


def test_fork_isolation_from_primary():
    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)

    fork = primary.fork("iso").build()
    tx = fork.tx()
    tx.execute("CREATE (:Person {name: 'fork-only'})")
    tx.commit()

    # Fork sees primary seed + its own write.
    fork_rows = fork.query("MATCH (p:Person) RETURN p.name AS name")
    fork_names = sorted(r["name"] for r in fork_rows)
    assert fork_names == ["fork-only", "seed"]

    # Primary unchanged.
    primary_rows = primary.query("MATCH (p:Person) RETURN p.name AS name")
    primary_names = sorted(r["name"] for r in primary_rows)
    assert primary_names == ["seed"]

    del fork
    db.drop_fork("iso")


def test_fork_open_or_create():
    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)

    a = primary.fork("dup").build()
    b = primary.fork("dup").build()  # open-or-create returns the same fork
    assert a.is_forked() and b.is_forked()
    del a
    del b

    # `.new_()` requires fresh creation.
    primary.fork("uniq").new_().build()
    with pytest.raises(uni_db.UniForkAlreadyExistsError):
        primary.fork("uniq").new_().build()


# ---------------------------------------------------------------------------
# Nested forks
# ---------------------------------------------------------------------------


def test_nested_fork_chain():
    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)

    a = primary.fork("a").build()
    tx = a.tx()
    tx.execute("CREATE (:Person {name: 'A-only'})")
    tx.commit()

    b = a.fork("b").build()
    tx = b.tx()
    tx.execute("CREATE (:Person {name: 'B-only'})")
    tx.commit()

    # B sees seed + A-only + B-only via Lance base_paths chain.
    b_names = sorted(
        r["name"] for r in b.query("MATCH (p:Person) RETURN p.name AS name")
    )
    assert b_names == ["A-only", "B-only", "seed"]

    # A does NOT see B-only.
    a_names = sorted(
        r["name"] for r in a.query("MATCH (p:Person) RETURN p.name AS name")
    )
    assert "B-only" not in a_names

    # parent_fork_id round-trips.
    b_info = db.fork_info("b")
    a_info = db.fork_info("a")
    assert b_info.parent_fork_id == a_info.id

    del a, b
    db.drop_fork_cascade("a")


def test_drop_fork_refuses_with_children():
    import gc

    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)
    a = primary.fork("a").build()
    _b = a.fork("b").build()
    del a, _b
    # Force pyclass refcount drops to fire on slower runners (CI under
    # pytest-xdist sometimes leaves the Py<T> wrappers reachable past
    # `del` until the next GC pass).
    gc.collect()

    with pytest.raises(uni_db.UniForkHasChildrenError) as excinfo:
        db.drop_fork("a")
    assert "b" in excinfo.value.children

    db.drop_fork_cascade("a")


def test_cascade_subtree_in_use():
    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)
    a = primary.fork("a").build()
    b = a.fork("b").build()  # keep alive
    del a

    with pytest.raises(uni_db.UniForkSubtreeInUseError) as excinfo:
        db.drop_fork_cascade("a")
    blockers = excinfo.value.blockers
    assert any("b" in entry for entry in blockers)

    del b
    db.drop_fork_cascade("a")


# ---------------------------------------------------------------------------
# TTL + sweeper
# ---------------------------------------------------------------------------


def test_fork_ttl_sweeper_drops_expired():
    import gc
    import time

    db = _make_db(
        disable_fork_sweeper=False,
        fork_sweeper_interval=timedelta(milliseconds=100),
    )
    primary = _seed_person_schema(db)

    fork = primary.fork("ephemeral").ttl(timedelta(milliseconds=200)).build()
    del fork
    # Force pyclass refcount drops so the underlying session releases
    # and the sweeper can drop the fork.
    gc.collect()

    # Poll instead of relying on a fixed sleep so slower runners don't
    # flake. TTL is 200ms; sweeper interval is 100ms; we wait up to
    # 5s, polling every 100ms.
    deadline = 5.0
    elapsed = 0.0
    poll_interval = 0.1
    remaining = [f.name for f in db.list_forks()]
    while "ephemeral" in remaining and elapsed < deadline:
        time.sleep(poll_interval)
        elapsed += poll_interval
        remaining = [f.name for f in db.list_forks()]

    assert "ephemeral" not in remaining, (
        f"fork 'ephemeral' was not dropped by the TTL sweeper within {deadline}s "
        f"(remaining = {remaining!r})"
    )


def test_fork_without_ttl_survives_sweeper():
    db = _make_db(
        disable_fork_sweeper=False,
        fork_sweeper_interval=timedelta(milliseconds=100),
    )
    primary = _seed_person_schema(db)

    fork = primary.fork("permanent").build()
    del fork

    import time

    time.sleep(0.4)

    remaining = [f.name for f in db.list_forks()]
    assert "permanent" in remaining
    db.drop_fork("permanent")


# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------


def test_fork_budget_blocks_at_cap():
    db = _make_db(disable_fork_sweeper=True, max_forks=2)
    primary = _seed_person_schema(db)

    _a = primary.fork("a").build()
    _b = primary.fork("b").build()

    with pytest.raises(uni_db.UniForkBudgetExceededError) as excinfo:
        primary.fork("c").build()
    assert excinfo.value.max == 2
    assert excinfo.value.current == 2


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def test_fork_tag_roundtrip():
    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)

    fork = primary.fork("audit").build()
    tx = fork.tx()
    tx.execute("CREATE (:Person {name: 'in-fork'})")
    tx.commit()
    fork.flush()
    del fork

    db.tag_fork("audit", "2026-q1")
    tags = db.list_fork_tags("audit")
    assert "2026-q1" in tags

    db.untag_fork("audit", "2026-q1")
    tags = db.list_fork_tags("audit")
    assert "2026-q1" not in tags

    # Idempotent on re-untag.
    db.untag_fork("audit", "2026-q1")

    db.drop_fork("audit")


def test_drop_of_tagged_fork_raises_fork_tagged():
    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)
    fork = primary.fork("held").build()
    tx = fork.tx()
    tx.execute("CREATE (:Person {name: 'in-fork'})")
    tx.commit()
    del fork  # unflushed: tag_fork flushes it

    db.tag_fork("held", "keep")
    with pytest.raises(uni_db.UniForkTaggedError) as excinfo:
        db.drop_fork("held")
    assert excinfo.value.name == "held"
    assert excinfo.value.tags == ["keep"]
    # Nothing was dropped.
    assert db.list_fork_tags("held") == ["keep"]
    with pytest.raises(uni_db.UniForkTaggedError):
        db.drop_fork_cascade("held")

    db.untag_fork("held", "keep")
    db.drop_fork("held")


def test_fork_tags_are_per_fork():
    db = _make_db(disable_fork_sweeper=True)
    primary = _seed_person_schema(db)
    for name in ("a", "b"):
        fork = primary.fork(name).build()
        tx = fork.tx()
        tx.execute(f"CREATE (:Person {{name: '{name}'}})")
        tx.commit()
        del fork
        db.tag_fork(name, "audit")
    db.untag_fork("a", "audit")
    assert db.list_fork_tags("a") == []
    assert db.list_fork_tags("b") == ["audit"]
    db.untag_fork("b", "audit")


def test_tag_unknown_fork_raises_not_found():
    db = _make_db(disable_fork_sweeper=True)
    with pytest.raises(uni_db.UniForkNotFoundError):
        db.tag_fork("missing", "v1")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_fork_schema_label_strict():
    cfg_db = _make_db(disable_fork_sweeper=True, strict_schema=True)
    cfg_db.schema().label("Item").property("kind", "string").apply()
    primary = cfg_db.session()
    tx = primary.tx()
    tx.execute("CREATE (:Item {kind: 'seed'})")
    tx.commit()
    cfg_db.flush()

    forked = primary.fork("scenario").build()
    forked.fork_schema().label("OnlyOnFork", description="fork-local").apply()

    tx = forked.tx()
    tx.execute("CREATE (:OnlyOnFork)")
    tx.commit()

    rows = forked.query("MATCH (n:OnlyOnFork) RETURN count(n) AS c")
    assert rows[0]["c"] == 1

    del forked
    cfg_db.drop_fork("scenario")


def test_fork_schema_on_primary_session_errors():
    db = _make_db(disable_fork_sweeper=True)
    db.schema().label("X").apply()
    primary = db.session()
    with pytest.raises(uni_db.UniInvalidArgumentError):
        primary.fork_schema().label("Bad").apply()


# ---------------------------------------------------------------------------
# fork_info ergonomics
# ---------------------------------------------------------------------------


def test_fork_info_returns_none_for_missing():
    db = _make_db(disable_fork_sweeper=True)
    assert db.fork_info("does-not-exist") is None


# ---------------------------------------------------------------------------
# #97 — fork inherits the parent's committed-but-unflushed (L0) writes.
# These tests must NOT flush before forking; that is the whole point. Each
# "sees base data" test asserts the fork sees inherited rows BEFORE the
# fork mutates anything (a delete-then-assert-zero shape passes trivially
# under the bug, so it cannot detect it).
# ---------------------------------------------------------------------------


def test_fork_inherits_unflushed_single_node():
    db = _make_db(disable_fork_sweeper=True)
    s = _person_schema_no_flush(db)
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'Alice'})")
    tx.commit()  # no db.flush()

    assert _count(s) == 1
    fork = s.fork("scn").build()
    assert _count(fork) == 1, "fork must inherit the parent's unflushed L0 write"

    del fork
    db.drop_fork("scn")


def test_fork_inherits_unflushed_many_nodes():
    db = _make_db(disable_fork_sweeper=True)
    s = _person_schema_no_flush(db)
    tx = s.tx()
    tx.execute("UNWIND range(1, 25) AS i CREATE (:Person {name: toString(i)})")
    tx.commit()

    fork = s.fork("scn").build()
    assert _count(fork) == 25

    del fork
    db.drop_fork("scn")


def test_fork_inherits_unflushed_relationship():
    db = _make_db(disable_fork_sweeper=True)
    db.schema().label("Person").property("name", "string").apply()
    db.schema().edge_type("KNOWS", ["Person"], ["Person"]).apply()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'A'})-[:KNOWS]->(:Person {name: 'B'})")
    tx.commit()  # no flush

    fork = s.fork("scn").build()
    assert _count(fork) == 2, "fork must see both endpoints"
    rel = fork.query("MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r) AS c")
    assert rel[0]["c"] == 1, "fork must inherit the unflushed relationship"

    del fork
    db.drop_fork("scn")


def test_fork_with_block_tx_unflushed():
    db = _make_db(disable_fork_sweeper=True)
    s = _person_schema_no_flush(db)

    # with-block tx: rolls back on exit, so commit explicitly inside.
    with s.tx() as tx:
        tx.execute("CREATE (:Person {name: 'WithBlock'})")
        tx.commit()
    # Raw tx variant in the same DB.
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'Raw'})")
    tx.commit()

    fork = s.fork("scn").build()
    assert _count(fork) == 2, (
        "fork must inherit both with-block and raw unflushed writes"
    )

    del fork
    db.drop_fork("scn")


def test_parent_writes_after_fork_invisible_no_flush():
    db = _make_db(disable_fork_sweeper=True)
    s = _person_schema_no_flush(db)
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'Alice'})")
    tx.commit()

    fork = s.fork("scn").build()
    # Parent commits more AFTER the fork — must not leak in.
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'Bob'})")
    tx.commit()

    assert _count(fork) == 1, "fork sees only the fork-point row"
    assert _count(s) == 2, "parent sees both rows"

    del fork
    db.drop_fork("scn")


def test_fork_the_writing_session_unflushed():
    db = _make_db(disable_fork_sweeper=True)
    s = _person_schema_no_flush(db)
    tx = s.tx()
    tx.execute("CREATE (:Person {name: 'Writer'})")
    tx.commit()

    # Fork the very session that performed the writes.
    fork = s.fork("self").build()
    rows = fork.query("MATCH (n:Person) RETURN n.name AS name")
    assert [r["name"] for r in rows] == ["Writer"]

    del fork
    db.drop_fork("self")


def test_nested_unflushed_chain():
    db = _make_db(disable_fork_sweeper=True)
    primary = _person_schema_no_flush(db)
    tx = primary.tx()
    tx.execute("CREATE (:Person {name: 'P'})")
    tx.commit()  # no flush anywhere

    a = primary.fork("a").build()
    tx = a.tx()
    tx.execute("CREATE (:Person {name: 'a'})")
    tx.commit()  # no flush

    b = a.fork("b").build()
    assert _count(b) == 2, "B must see P + a via the unflushed chain before writing"

    del b
    del a
    db.drop_fork_cascade("a")


# ---------------------------------------------------------------------------
# #99 — on a fork, a relationship pattern with a TARGET-endpoint label must
# match. Follow-up to #97: a fork's inherited data lives only in Lance, and the
# schemaless traverse built the target labels column from L0 only, so
# `hasLabel(b, "B")` (how `b:B` is compiled) evaluated false on the fork. This
# is the issue's exact (schemaless) shape — no schema is declared.
# ---------------------------------------------------------------------------


def _ab_edge_no_schema_no_flush(db):
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:A {k: 1})-[:R]->(:B {k: 2})")
    tx.commit()  # no schema, no flush — routes through the schemaless path
    return s


def _rel(scope, cypher):
    return scope.query(cypher)[0]["c"]


def test_fork_matches_target_endpoint_label_schemaless():
    db = _make_db(disable_fork_sweeper=True)
    s = _ab_edge_no_schema_no_flush(db)

    # Base session matches the target-endpoint label.
    assert _rel(s, "MATCH (a)-[r:R]->(b:B) RETURN count(r) AS c") == 1

    fork = s.fork("scn").build()
    # Sibling forms guard against a broken traversal masking the fix.
    assert _rel(fork, "MATCH (a)-[r:R]->(b) RETURN count(r) AS c") == 1
    assert _rel(fork, "MATCH (a:A)-[r:R]->(b) RETURN count(r) AS c") == 1
    assert _rel(fork, "MATCH (b:B) RETURN count(b) AS c") == 1
    # The #99 bug: target-endpoint label returned 0 on the fork.
    assert _rel(fork, "MATCH (a)-[r:R]->(b:B) RETURN count(r) AS c") == 1, (
        "fork must match the target-endpoint label (GitHub #99)"
    )

    del fork
    db.drop_fork("scn")
