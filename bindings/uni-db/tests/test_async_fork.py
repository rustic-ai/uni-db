# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Phase 4b — async fork API tests.

Mirrors `test_fork.py` against `AsyncUni` / `AsyncSession`.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

import uni_db

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
    await tx.execute("CREATE (:Person {name: 'seed'})")
    await tx.commit()
    await db.flush()
    return s


async def _person_schema_no_flush(db):
    """Declare the Person schema and return a session — no seed, no flush.
    #97 regression tests must never flush before forking.
    """
    await db.schema().label("Person").property("name", "string").apply()
    return db.session()


async def _count(scope):
    rows = await scope.query("MATCH (n:Person) RETURN count(n) AS c")
    return rows[0]["c"]


# ---------------------------------------------------------------------------
# Basic lifecycle
# ---------------------------------------------------------------------------


async def test_fork_basic_create_drop():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)

    fork = await primary.fork("scenario_1").build()
    assert await fork.is_forked()
    assert not await primary.is_forked()

    info = await db.fork_info("scenario_1")
    assert info is not None
    assert info.name == "scenario_1"

    with pytest.raises(uni_db.UniForkInUseError):
        await db.drop_fork("scenario_1")

    del fork
    await db.drop_fork("scenario_1")
    assert await db.fork_info("scenario_1") is None


async def test_fork_isolation_from_primary():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)

    fork = await primary.fork("iso").build()
    tx = await fork.tx()
    await tx.execute("CREATE (:Person {name: 'fork-only'})")
    await tx.commit()

    fork_rows = await fork.query("MATCH (p:Person) RETURN p.name AS name")
    fork_names = sorted(r["name"] for r in fork_rows)
    assert fork_names == ["fork-only", "seed"]

    primary_rows = await primary.query("MATCH (p:Person) RETURN p.name AS name")
    primary_names = sorted(r["name"] for r in primary_rows)
    assert primary_names == ["seed"]

    del fork
    await db.drop_fork("iso")


# ---------------------------------------------------------------------------
# Nested forks
# ---------------------------------------------------------------------------


async def test_nested_fork_chain():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)

    a = await primary.fork("a").build()
    tx = await a.tx()
    await tx.execute("CREATE (:Person {name: 'A-only'})")
    await tx.commit()

    b = await a.fork("b").build()
    tx = await b.tx()
    await tx.execute("CREATE (:Person {name: 'B-only'})")
    await tx.commit()

    b_names = sorted(
        r["name"] for r in await b.query("MATCH (p:Person) RETURN p.name AS name")
    )
    assert b_names == ["A-only", "B-only", "seed"]

    a_names = sorted(
        r["name"] for r in await a.query("MATCH (p:Person) RETURN p.name AS name")
    )
    assert "B-only" not in a_names

    del a, b
    await db.drop_fork_cascade("a")


async def test_drop_fork_refuses_with_children():
    import gc

    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)
    a = await primary.fork("a").build()
    _b = await a.fork("b").build()
    del a, _b
    # Force pyclass refcount drops to fire on slower runners (CI under
    # pytest-xdist sometimes leaves the Py<T> wrappers reachable past
    # `del` until the next GC pass).
    gc.collect()

    with pytest.raises(uni_db.UniForkHasChildrenError) as excinfo:
        await db.drop_fork("a")
    assert "b" in excinfo.value.children

    await db.drop_fork_cascade("a")


# ---------------------------------------------------------------------------
# TTL
# ---------------------------------------------------------------------------


async def test_fork_ttl_sweeper_drops_expired():
    import gc

    db = await _make_db(
        disable_fork_sweeper=False,
        fork_sweeper_interval=timedelta(milliseconds=100),
    )
    primary = await _seed_person_schema(db)

    fork = await primary.fork("ephemeral").ttl(timedelta(milliseconds=200)).build()
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
    while elapsed < deadline:
        remaining = [f.name for f in await db.list_forks()]
        if "ephemeral" not in remaining:
            return
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    pytest.fail(
        f"fork 'ephemeral' was not dropped by the TTL sweeper within {deadline}s "
        f"(remaining = {remaining!r})"
    )


# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------


async def test_fork_budget_blocks_at_cap():
    db = await _make_db(disable_fork_sweeper=True, max_forks=2)
    primary = await _seed_person_schema(db)

    _a = await primary.fork("a").build()
    _b = await primary.fork("b").build()

    with pytest.raises(uni_db.UniForkBudgetExceededError) as excinfo:
        await primary.fork("c").build()
    assert excinfo.value.max == 2


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


async def test_fork_tag_roundtrip():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)

    fork = await primary.fork("audit").build()
    tx = await fork.tx()
    await tx.execute("CREATE (:Person {name: 'in-fork'})")
    await tx.commit()
    await fork.flush()
    del fork

    await db.tag_fork("audit", "2026-q1")
    tags = await db.list_fork_tags("audit")
    assert "2026-q1" in tags

    await db.untag_fork("audit", "2026-q1")
    tags = await db.list_fork_tags("audit")
    assert "2026-q1" not in tags

    await db.drop_fork("audit")


async def test_drop_of_tagged_fork_raises_fork_tagged():
    db = await _make_db(disable_fork_sweeper=True)
    primary = await _seed_person_schema(db)
    fork = await primary.fork("held").build()
    tx = await fork.tx()
    await tx.execute("CREATE (:Person {name: 'in-fork'})")
    await tx.commit()
    del fork

    await db.tag_fork("held", "keep")
    with pytest.raises(uni_db.UniForkTaggedError) as excinfo:
        await db.drop_fork("held")
    assert excinfo.value.name == "held"
    assert excinfo.value.tags == ["keep"]
    assert await db.list_fork_tags("held") == ["keep"]

    await db.untag_fork("held", "keep")
    await db.drop_fork("held")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


async def test_fork_schema_label_strict():
    db = await _make_db(disable_fork_sweeper=True, strict_schema=True)
    schema = db.schema()
    await schema.label("Item").property("kind", "string").apply()
    primary = db.session()
    tx = await primary.tx()
    await tx.execute("CREATE (:Item {kind: 'seed'})")
    await tx.commit()
    await db.flush()

    forked = await primary.fork("scenario").build()
    await forked.fork_schema().label("OnlyOnFork", description="fork-local").apply()

    tx = await forked.tx()
    await tx.execute("CREATE (:OnlyOnFork)")
    await tx.commit()

    rows = await forked.query("MATCH (n:OnlyOnFork) RETURN count(n) AS c")
    assert rows[0]["c"] == 1

    del forked
    await db.drop_fork("scenario")


# ---------------------------------------------------------------------------
# #97 — fork inherits the parent's committed-but-unflushed (L0) writes.
# No flush before forking; assert the fork sees inherited rows before it
# mutates anything.
# ---------------------------------------------------------------------------


async def test_async_fork_inherits_unflushed_single_node():
    db = await _make_db(disable_fork_sweeper=True)
    s = await _person_schema_no_flush(db)
    tx = await s.tx()
    await tx.execute("CREATE (:Person {name: 'Alice'})")
    await tx.commit()  # no flush

    assert await _count(s) == 1
    fork = await s.fork("scn").build()
    assert await _count(fork) == 1, "fork must inherit the parent's unflushed L0 write"

    del fork
    await db.drop_fork("scn")


async def test_async_fork_inherits_unflushed_relationship():
    db = await _make_db(disable_fork_sweeper=True)
    await db.schema().label("Person").property("name", "string").apply()
    await db.schema().edge_type("KNOWS", ["Person"], ["Person"]).apply()
    s = db.session()
    tx = await s.tx()
    await tx.execute("CREATE (:Person {name: 'A'})-[:KNOWS]->(:Person {name: 'B'})")
    await tx.commit()  # no flush

    fork = await s.fork("scn").build()
    assert await _count(fork) == 2, "fork must see both endpoints"
    rel = await fork.query("MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r) AS c")
    assert rel[0]["c"] == 1, "fork must inherit the unflushed relationship"

    del fork
    await db.drop_fork("scn")


async def test_async_parent_writes_after_fork_invisible_no_flush():
    db = await _make_db(disable_fork_sweeper=True)
    s = await _person_schema_no_flush(db)
    tx = await s.tx()
    await tx.execute("CREATE (:Person {name: 'Alice'})")
    await tx.commit()

    fork = await s.fork("scn").build()
    tx = await s.tx()
    await tx.execute("CREATE (:Person {name: 'Bob'})")
    await tx.commit()

    assert await _count(fork) == 1, "fork sees only the fork-point row"
    assert await _count(s) == 2, "parent sees both rows"

    del fork
    await db.drop_fork("scn")
