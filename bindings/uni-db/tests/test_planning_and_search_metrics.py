# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Planning, sub-plan and search counters reach Python on every surface.

The Rust ``QueryMetrics`` grew ``physical_plans``, ``physical_plan_time``,
``shortest_path_searches``, ``subquery_executions`` and ``lance_iops``, and
``ProfileOutput`` grew ``logical_planning_ms``, ``physical_planning_ms`` and
``physical_plans``; none of them was bound. These tests assert values, not just
presence: each counter is driven by a query whose expected count is known, so a
field bound to a constant zero (or to the wrong Rust field) fails.

The Rust-to-Python field axis is also guarded statically by
``scripts/ci/check_rust_python_parity.py``.
"""

from __future__ import annotations

import pytest

import uni_db

N = 10

# One source, five reachable targets (ids 5..9 on a 0 -> 1 -> ... -> 9 chain).
SHORTEST_ONE_SOURCE = (
    "MATCH (a:N) WHERE a.id = 0 "
    "MATCH p = shortestPath((a)-[:R*]->(b:N)) WHERE b.id >= 5 "
    "RETURN count(p) AS paths"
)
SHORTEST_THREE_SOURCES = (
    "MATCH (a:N) WHERE a.id < 3 "
    "MATCH p = shortestPath((a)-[:R*]->(b:N)) WHERE b.id >= 5 "
    "RETURN count(p) AS paths"
)
# Correlated: the body reads `n`, so it genuinely runs once per outer row.
CORRELATED = (
    "MATCH (n:N) RETURN n.id AS id, [(a:N)-[:R]->(b:N) WHERE a.id < n.id | a.id] AS l"
)
# Uncorrelated: evaluated once and broadcast (#206).
UNCORRELATED = "MATCH (n:N) RETURN [(a:N)-[:R]->(b:N) | a.id] AS l"

REACH_PROGRAM = """
CREATE RULE reach AS
  MATCH (a:N)-[:R]->(b:N)
  YIELD KEY a, KEY b
CREATE RULE reach AS
  MATCH (a:N)-[:R]->(m:N) WHERE m IS reach TO b
  YIELD KEY a, KEY b
"""

POPULATE = [
    "UNWIND range(0, 9) AS i CREATE (:N {id: i})",
    "MATCH (a:N), (b:N) WHERE b.id = a.id + 1 CREATE (a)-[:R]->(b)",
]


def _schema(builder):
    return (
        builder.label("N")
        .property("id", "int")
        .done()
        .edge_type("R", ["N"], ["N"])
        .done()
    )


@pytest.fixture
def chain_db():
    """A flushed 10-node chain `0 -> 1 -> ... -> 9`."""
    db = uni_db.UniBuilder.temporary().build()
    _schema(db.schema()).apply()
    tx = db.session().tx()
    for q in POPULATE:
        tx.execute(q)
    tx.commit()
    db.flush()
    return db


@pytest.fixture
async def async_chain_db():
    db = await uni_db.AsyncUni.temporary()
    await _schema(db.schema()).apply()
    tx = await db.session().tx()
    for q in POPULATE:
        await tx.execute(q)
    await tx.commit()
    await db.flush()
    return db


def _metric_names() -> set[str]:
    return {
        name
        for name in dir(uni_db.QueryMetrics)
        if not name.startswith("_")
        and not callable(getattr(uni_db.QueryMetrics, name, None))
    }


# --- QueryMetrics -----------------------------------------------------------


def test_query_metrics_count_physical_planning(chain_db):
    m = chain_db.session().query("MATCH (n:N) RETURN count(n) AS c").metrics
    assert m.physical_plans >= 1, "the query's own plan was not counted"
    assert m.physical_plan_time_ms > 0.0
    assert m.physical_plan_time_ms <= m.exec_time_ms, (
        "physical planning is measured inside execution"
    )


def test_subquery_executions_track_correlation(chain_db):
    session = chain_db.session()
    correlated = session.query(CORRELATED)
    assert len(correlated) == N
    assert correlated.metrics.subquery_executions == N, (
        "a correlated comprehension runs once per outer row"
    )
    uncorrelated = session.query(UNCORRELATED)
    assert len(uncorrelated) == N
    assert uncorrelated.metrics.subquery_executions == 1, (
        "an uncorrelated comprehension is evaluated once and broadcast"
    )


def test_shortest_path_searches_count_sources(chain_db):
    session = chain_db.session()
    one = session.query(SHORTEST_ONE_SOURCE)
    assert one[0]["paths"] == 5
    assert one.metrics.shortest_path_searches == 1, (
        "five targets of one source share one search"
    )
    three = session.query(SHORTEST_THREE_SOURCES)
    assert three[0]["paths"] == 15
    assert three.metrics.shortest_path_searches == 3


def test_shortest_path_adjacency_reads_bounded_by_the_search(chain_db):
    """Building five paths (35 hops) reads no adjacency beyond the search's own
    one lookup per vertex expanded; re-reading each hop's edge for the output
    used to add two reads per hop."""
    m = chain_db.session().query(SHORTEST_ONE_SOURCE).metrics
    assert 0 < m.shortest_path_adjacency_reads <= N, m.shortest_path_adjacency_reads


def test_shortest_path_searches_zero_without_shortest_path(chain_db):
    """The control: a counter stuck at a constant would pass neither test."""
    m = chain_db.session().query("MATCH (n:N) RETURN count(n) AS c").metrics
    assert m.shortest_path_searches == 0
    assert m.shortest_path_adjacency_reads == 0
    assert m.subquery_executions == 0


def test_lance_iops_count_flushed_reads(chain_db):
    m = chain_db.session().query("MATCH (n:N) RETURN n.id AS id").metrics
    assert m.scans_reported > 0, "the Lance stats callback did not fire"
    assert m.lance_iops > 0, "a scan of flushed data issued no Lance I/O"


def test_vector_search_reports_through_python(ecommerce_db_populated):
    m = (
        ecommerce_db_populated.session()
        .query(
            "CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3) "
            "YIELD vid RETURN vid"
        )
        .metrics
    )
    assert m.searches_reported >= 1, "the vector search stats callback did not fire"
    assert m.vector_index_scans <= m.searches_reported
    assert m.fts_index_scans == 0


def test_execute_result_metrics_dict_matches_the_class(chain_db):
    """`ExecuteResult.metrics` is a dict built by a second converter."""
    tx = chain_db.session().tx()
    result = tx.execute("CREATE (:N {id: 100})")
    tx.rollback()
    assert set(result.metrics) == _metric_names()
    assert result.metrics["physical_plans"] >= 1


# --- ProfileOutput ----------------------------------------------------------


def test_profile_reports_planning(chain_db):
    result, profile = chain_db.session().query_with(CORRELATED).profile()
    assert len(result) == N
    assert profile.logical_planning_ms > 0.0
    assert profile.physical_planning_ms > 0.0
    assert profile.physical_plans >= 1
    assert profile.physical_plans == result.metrics.physical_plans


def test_tx_profile_reports_planning(chain_db):
    tx = chain_db.session().tx()
    _, profile = tx.query_with("MATCH (n:N) RETURN count(n) AS c").profile()
    assert profile.physical_plans >= 1
    assert profile.physical_planning_ms > 0.0


# --- Locy per-iteration planning --------------------------------------------


def _iterations(profile):
    return [
        it
        for stratum in profile.strata
        for rule in stratum["rules"]
        for it in rule["iterations"]
    ]


def _assert_locy_planning(profile):
    recursive = [s for s in profile.strata if s["recursive"]]
    assert recursive and recursive[0]["iterations"] >= 2
    iterations = _iterations(profile)
    assert iterations
    for it in iterations:
        assert it["plans"] >= 1, f"iteration {it['iteration']} built no plan"
        assert it["planning_ms"] > 0.0


def test_locy_profile_iterations_carry_planning(chain_db):
    result, profile = chain_db.session().locy_with(REACH_PROGRAM).profile()
    assert len(result.derived["reach"]) == N * (N - 1) // 2
    _assert_locy_planning(profile)
    # Every per-iteration plan is one of the evaluation's physical plans.
    assert result.metrics.physical_plans >= sum(
        it["plans"] for it in _iterations(profile)
    )


# --- async twins ------------------------------------------------------------


async def test_async_query_metrics(async_chain_db):
    session = async_chain_db.session()
    correlated = await session.query(CORRELATED)
    assert correlated.metrics.subquery_executions == N
    assert correlated.metrics.physical_plans >= 1
    assert correlated.metrics.physical_plan_time_ms > 0.0
    sp = await session.query(SHORTEST_THREE_SOURCES)
    assert sp.metrics.shortest_path_searches == 3
    assert sp.metrics.lance_iops > 0


async def test_async_profile_reports_planning(async_chain_db):
    result, profile = await async_chain_db.session().query_with(CORRELATED).profile()
    assert profile.logical_planning_ms > 0.0
    assert profile.physical_planning_ms > 0.0
    assert profile.physical_plans == result.metrics.physical_plans


async def test_async_execute_result_metrics_dict(async_chain_db):
    tx = await async_chain_db.session().tx()
    result = await tx.execute("CREATE (:N {id: 100})")
    await tx.rollback()
    assert set(result.metrics) == _metric_names()


async def test_async_locy_profile_iterations_carry_planning(async_chain_db):
    _, profile = await async_chain_db.session().locy_with(REACH_PROGRAM).profile()
    _assert_locy_planning(profile)


def test_ephemeral_id_expressions_use_signed_identity():
    db = uni_db.Uni.in_memory()
    result = db.session().query(
        "CALL uni.create.vNode(['L'], '{}') YIELD node "
        "RETURN id(node) AS id, id(node) < 0 AS negative, "
        "elementId(node) = toString(id(node)) AS same_string"
    )
    assert result[0]["id"] < 0
    assert result[0]["negative"] is True
    assert result[0]["same_string"] is True


def test_declared_procedure_inherits_scan_counters():
    db = uni_db.Uni.in_memory()
    session = db.session()
    tx = session.tx()
    tx.execute("UNWIND range(1, 12) AS i CREATE (:CounterProbe {i:i})")
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.scan_counters', "
        "'MATCH (n:CounterProbe) RETURN n.i AS i', 'READ', '[]', '[]')"
    )
    tx.commit()
    direct = session.query("MATCH (n:CounterProbe) RETURN n.i AS i")
    wrapped = session.query("CALL t.scan_counters() YIELD i RETURN i")
    assert len(direct) == len(wrapped) == 12
    assert direct.metrics.rows_scanned == 12
    assert wrapped.metrics.rows_scanned == direct.metrics.rows_scanned
    assert wrapped.metrics.l0_reads == direct.metrics.l0_reads


def test_schemaless_fts_uses_document_statistics():
    db = uni_db.Uni.in_memory()
    session = db.session()
    tx = session.tx()
    for i, text in enumerate(
        ["supplier", "supplier supplier", "supplier site code", "customer order"]
    ):
        tx.execute("CREATE (:TextProbe {i:$i, text:$text})", {"i": i, "text": text})
    tx.commit()
    query = (
        "CALL uni.fts.query('TextProbe', 'text', 'supplier', 10) "
        "YIELD node, score RETURN node.i AS i, score ORDER BY score DESC, i"
    )
    before = [row.to_dict() for row in session.query(query)]
    assert len(before) == 3
    assert len({row["score"] for row in before}) > 1
    db.flush()
    after = [row.to_dict() for row in session.query(query)]
    assert [r["i"] for r in after] == [r["i"] for r in before]
    assert [r["score"] for r in after] == pytest.approx([r["score"] for r in before])
