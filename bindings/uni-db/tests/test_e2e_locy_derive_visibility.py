# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for DERIVE visibility in trailing Cypher commands.

These tests verify that trailing Cypher queries within a Locy program
can see edges materialized by preceding DERIVE commands.

Bug context: session.locy() with DERIVE + trailing Cypher returns 0 rows
because DERIVE mutations are collected but not applied to any L0 buffer
visible to execute_cypher_read().
"""

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def graph_db():
    """Temporary database with schema and a simple A -[:R]-> B graph."""
    db = uni_db.UniBuilder.temporary().build()
    (
        db.schema()
        .label("X")
        .property("name", "string")
        .done()
        .edge_type("R", ["X"], ["X"])
        .done()
        .edge_type("LINKED", ["X"], ["X"])
        .done()
        .edge_type("D", ["X"], ["X"])
        .done()
        .apply()
    )
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
    tx.commit()
    return db


@pytest.fixture
def chain_db():
    """Temporary database with schema and a chain A -[:R]-> B -[:R]-> C."""
    db = uni_db.UniBuilder.temporary().build()
    (
        db.schema()
        .label("X")
        .property("name", "string")
        .done()
        .edge_type("R", ["X"], ["X"])
        .done()
        .edge_type("LINKED", ["X"], ["X"])
        .done()
        .apply()
    )
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})-[:R]->(:X {name: 'C'})")
    tx.commit()
    return db


# ---------------------------------------------------------------------------
# Group 1: Session-level — trailing Cypher sees DERIVE edges
# ---------------------------------------------------------------------------

DERIVE_WITH_TRAILING_CYPHER = """
CREATE RULE link AS
  MATCH (a:X)-[:R]->(b:X)
  DERIVE (a)-[:LINKED]->(b)
DERIVE link
MATCH (a:X)-[:LINKED]->(b:X)
RETURN a.name AS src, b.name AS dst
"""


def test_session_trailing_cypher_sees_derive_edges(graph_db):
    """Trailing Cypher after DERIVE should see derived :LINKED edges."""
    session = graph_db.session()
    result = session.locy(DERIVE_WITH_TRAILING_CYPHER)

    assert len(result.command_results) >= 2

    # Last command is the trailing Cypher
    last_cmd = result.command_results[-1]
    assert last_cmd.command_type in ("cypher",), (
        f"expected cypher command, got {last_cmd.command_type}"
    )
    assert len(last_cmd.rows) > 0, (
        "trailing Cypher after DERIVE should see derived edges (got 0 rows)"
    )
    assert last_cmd.rows[0]["src"] == "A"
    assert last_cmd.rows[0]["dst"] == "B"


DERIVE_WITH_COUNT = """
CREATE RULE link AS
  MATCH (a:X)-[:R]->(b:X)
  DERIVE (a)-[:LINKED]->(b)
DERIVE link
MATCH ()-[r:LINKED]->() RETURN count(r) AS cnt
"""


def test_session_trailing_cypher_after_derive_counts_edges(chain_db):
    """Trailing Cypher count() should reflect derived edges."""
    session = chain_db.session()
    result = session.locy(DERIVE_WITH_COUNT)

    last_cmd = result.command_results[-1]
    assert len(last_cmd.rows) > 0
    # A->B and B->C produce 2 :LINKED edges
    assert last_cmd.rows[0]["cnt"] == 2, (
        f"expected 2 derived :LINKED edges, got {last_cmd.rows[0]['cnt']}"
    )


DERIVE_WITH_JOIN = """
CREATE RULE link AS
  MATCH (a:X)-[:R]->(b:X)
  DERIVE (a)-[:LINKED]->(b)
DERIVE link
MATCH (a:X)-[:LINKED]->(b:X)-[:R]->(c:X)
RETURN a.name AS src, c.name AS dst
"""


def test_session_trailing_cypher_joins_derived_and_existing(chain_db):
    """Trailing Cypher should join derived :LINKED with existing :R edges."""
    session = chain_db.session()
    result = session.locy(DERIVE_WITH_JOIN)

    last_cmd = result.command_results[-1]
    assert len(last_cmd.rows) > 0, (
        "trailing Cypher should join derived and existing edges"
    )
    # A -[:LINKED]-> B -[:R]-> C
    assert last_cmd.rows[0]["src"] == "A"
    assert last_cmd.rows[0]["dst"] == "C"


QUERY_THEN_DERIVE_THEN_CYPHER = """
CREATE RULE linked AS
  MATCH (a:X)-[:R]->(b:X)
  YIELD KEY a, KEY b
CREATE RULE derive_d AS
  MATCH (a:X)-[:R]->(b:X)
  DERIVE (a)-[:D]->(b)
QUERY linked WHERE a = a RETURN a.name AS n
DERIVE derive_d
MATCH ()-[r:D]->() RETURN count(r) AS cnt
"""


def test_session_query_then_derive_then_cypher(graph_db):
    """Full interleaving: QUERY + DERIVE + trailing Cypher."""
    session = graph_db.session()
    result = session.locy(QUERY_THEN_DERIVE_THEN_CYPHER)

    assert len(result.command_results) >= 3

    # QUERY result (command 0)
    query_cmd = result.command_results[0]
    assert query_cmd.command_type == "query"
    assert len(query_cmd.rows) > 0

    # Trailing Cypher (last) sees derived :D edges
    last_cmd = result.command_results[-1]
    assert last_cmd.command_type == "cypher"
    assert len(last_cmd.rows) > 0, "trailing Cypher should see derived :D edges"
    assert last_cmd.rows[0]["cnt"] > 0


# ---------------------------------------------------------------------------
# Group 2: Transaction-level — verify DERIVE visibility in tx.locy()
# ---------------------------------------------------------------------------


def test_tx_trailing_cypher_sees_derive_edges(graph_db):
    """tx.locy() trailing Cypher after DERIVE should see derived edges."""
    session = graph_db.session()
    tx = session.tx()

    result = tx.locy(DERIVE_WITH_TRAILING_CYPHER)

    last_cmd = result.command_results[-1]
    assert len(last_cmd.rows) > 0, (
        "tx trailing Cypher after DERIVE should see derived edges (got 0 rows)"
    )

    tx.commit()

    # After commit, edges should persist
    check = session.query("MATCH ()-[:LINKED]->() RETURN count(*) AS cnt")
    assert check[0]["cnt"] == 1, "committed DERIVE edges should persist"


# ---------------------------------------------------------------------------
# Group 3: Session DERIVE + tx.apply() roundtrip
# ---------------------------------------------------------------------------

DERIVE_ONLY = """
CREATE RULE link AS
  MATCH (a:X)-[:R]->(b:X)
  DERIVE (a)-[:LINKED]->(b)
DERIVE link
"""


def test_session_derive_apply_then_query_sees_edges(graph_db):
    """Session DERIVE + tx.apply() + commit should persist edges."""
    session = graph_db.session()
    result = session.locy(DERIVE_ONLY)

    assert result.derived_fact_set is not None
    assert not result.derived_fact_set.is_empty()

    tx = session.tx()
    apply_result = tx.apply(result.derived_fact_set)
    assert apply_result.facts_applied > 0
    tx.commit()

    check = session.query("MATCH ()-[:LINKED]->() RETURN count(*) AS cnt")
    assert check[0]["cnt"] == 1, "applied DERIVE edges should be visible"


def test_session_derive_without_apply_does_not_persist(graph_db):
    """Session DERIVE without tx.apply() should not persist edges."""
    session = graph_db.session()
    result = session.locy(DERIVE_ONLY)

    assert result.derived_fact_set is not None

    # No tx.apply — edges should NOT be in graph
    check = session.query("MATCH ()-[:LINKED]->() RETURN count(*) AS cnt")
    assert check[0]["cnt"] == 0, "DERIVE without apply should not persist edges"


# ---------------------------------------------------------------------------
# Group 5: Top-level commands run in sequence
# ---------------------------------------------------------------------------

LINKED_RULES = """
CREATE RULE link AS
  MATCH (a:X)-[:R]->(b:X)
  DERIVE (a)-[:LINKED]->(b)
CREATE RULE linked AS
  MATCH (a:X)-[:LINKED]->(b:X)
  YIELD KEY a, KEY b
"""

WRITE_LINKED = "MATCH (a:X {name: 'A'}), (b:X {name: 'B'}) CREATE (a)-[:LINKED]->(b)"


def _query_names(cmd):
    return sorted((r["src"], r["dst"]) for r in cmd.rows)


@pytest.mark.parametrize("level", ["session", "tx"])
def test_query_after_derive_reads_the_derived_edges(graph_db, level):
    """A QUERY after a DERIVE in the same program sees the derived edges."""
    session = graph_db.session()
    program = (
        LINKED_RULES
        + "QUERY linked RETURN a.name AS src, b.name AS dst\n"
        + "DERIVE link\n"
        + "QUERY linked RETURN a.name AS src, b.name AS dst\n"
    )
    runner = session if level == "session" else session.tx()
    result = runner.locy(program)
    before, derive, after = result.command_results
    assert _query_names(before) == []
    assert derive.affected == 1
    assert _query_names(after) == [("A", "B")]
    assert result.stats.sequenced_strata_evaluated > 0


@pytest.mark.parametrize("level", ["session", "tx"])
def test_cypher_write_command_then_query(graph_db, level):
    """A Cypher write is a command: it works, and a later QUERY reads it.

    It used to fail with "Mutation context not set". At the session level the
    write is returned in the DerivedFactSet; in a transaction it lands there.
    """
    session = graph_db.session()
    program = (
        LINKED_RULES
        + WRITE_LINKED
        + "\nQUERY linked RETURN a.name AS src, b.name AS dst\n"
    )
    if level == "tx":
        tx = session.tx()
        result = tx.locy(program)
        tx.commit()
    else:
        result = session.locy(program)
        assert (
            session.query("MATCH ()-[r:LINKED]->() RETURN count(r) AS n")[0]["n"] == 0
        )
        assert not result.derived_fact_set.is_empty()
        tx = session.tx()
        tx.apply(result.derived_fact_set)
        tx.commit()
    assert _query_names(result.command_results[1]) == [("A", "B")]
    assert session.query("MATCH ()-[r:LINKED]->() RETURN count(r) AS n")[0]["n"] == 1
