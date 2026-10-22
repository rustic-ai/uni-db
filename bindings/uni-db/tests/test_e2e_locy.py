# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Sync E2E tests for the locy binding."""

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FRIENDS_PROGRAM = """
CREATE RULE friends AS
  MATCH (a:Person)-[:KNOWS]->(b:Person)
  YIELD KEY a, KEY b
"""

REACHABLE_PROGRAM = """
CREATE RULE reachable AS
  MATCH (a:Person)-[:KNOWS]->(b:Person)
  YIELD KEY a, KEY b
CREATE RULE reachable AS
  MATCH (a:Person)-[:KNOWS]->(mid:Person)
  WHERE mid IS reachable TO b
  YIELD KEY a, KEY b
"""

MULTI_RULE_PROGRAM = """
CREATE RULE friends AS
  MATCH (a:Person)-[:KNOWS]->(b:Person)
  YIELD KEY a, KEY b
CREATE RULE popular AS
  MATCH (n:Person)
  WHERE n IS friends TO m
  YIELD KEY n
"""

QUERY_PROGRAM = """
CREATE RULE friends AS
  MATCH (a:Person)-[:KNOWS]->(b:Person)
  YIELD KEY a, KEY b
QUERY friends WHERE a.name = 'Alice' RETURN b.name AS to_name
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_basic_rule_evaluation(social_db_populated):
    """A simple rule derives facts from graph edges."""
    db = social_db_populated
    session = db.session()
    result = session.locy(FRIENDS_PROGRAM)

    assert hasattr(result, "derived")
    assert hasattr(result, "stats")
    assert hasattr(result, "command_results")

    # friends rule captures 4 KNOWS edges
    friends_rows = result.derived.get("friends", [])
    assert len(friends_rows) == 4, f"Expected 4 friend pairs, got {len(friends_rows)}"

    # Each row has keys 'a' and 'b'
    for row in friends_rows:
        assert "a" in row
        assert "b" in row


def test_stats_returned(social_db_populated):
    """Result includes a LocyStats object with expected fields."""
    db = social_db_populated
    session = db.session()
    result = session.locy(FRIENDS_PROGRAM)

    stats = result.stats
    assert isinstance(stats, uni_db.LocyStats)
    assert stats.strata_evaluated >= 1
    # Non-recursive strata may complete in 0 fixpoint iterations
    assert stats.total_iterations >= 0
    assert stats.evaluation_time_secs >= 0.0
    assert stats.queries_executed >= 0
    assert stats.mutations_executed >= 0
    assert stats.peak_memory_bytes >= 0


def test_query_command(social_db_populated):
    """QUERY command result appears in command_results."""
    db = social_db_populated
    session = db.session()
    result = session.locy(QUERY_PROGRAM)

    cmd_results = result.command_results
    assert len(cmd_results) >= 1

    query_result = cmd_results[0]
    assert query_result["type"] == "query"
    rows = query_result["rows"]
    # Alice is connected to Bob and Charlie
    assert len(rows) >= 1
    to_names = {r["to_name"] for r in rows}
    assert "Bob" in to_names


def test_recursive_rule(social_db_populated):
    """Recursive rule computes transitive closure."""
    db = social_db_populated
    session = db.session()
    result = session.locy(REACHABLE_PROGRAM)

    reachable_rows = result.derived.get("reachable", [])
    # Alice->Bob, Bob->Charlie, Alice->Charlie (base), plus transitive pairs
    assert len(reachable_rows) >= 3


def test_profile_recursive(social_db_populated):
    """profile() returns the result plus a structured execution profile."""
    db = social_db_populated
    session = db.session()
    result, profile = session.locy_with(REACHABLE_PROGRAM).profile()

    # Same derived facts as run().
    assert len(result.derived.get("reachable", [])) >= 3

    # Scalar summaries.
    assert profile.total_time_ms >= 0.0
    assert profile.peak_memory_bytes >= 0
    assert isinstance(profile.plan_text, str) and profile.plan_text

    # Structured strata tree: list of dicts down to per-operator metrics.
    strata = profile.strata
    assert isinstance(strata, list) and len(strata) >= 1
    recursive = [s for s in strata if s["recursive"]]
    assert recursive, "expected a recursive stratum"
    s = recursive[0]
    assert s["iterations"] >= 2, "transitive closure needs multiple passes"
    assert s["rules"], "stratum should have rules"

    rule = s["rules"][0]
    iters = rule["iterations"]
    assert len(iters) == s["iterations"], "per-rule iterations match the stratum"
    total_ops = sum(len(it["operators"]) for it in iters)
    assert total_ops > 0, "per-iteration operator trees should be retained"

    # Operator entries carry the Cypher-style metric fields.
    op = next(op for it in iters for op in it["operators"])
    assert "operator" in op and "actual_rows" in op and "time_ms" in op

    # __str__ renders a readable report.
    assert "Locy Profile" in str(profile)


def test_multi_rule_program(social_db_populated):
    """Multiple rules can be defined in one program."""
    db = social_db_populated
    session = db.session()
    result = session.locy(MULTI_RULE_PROGRAM)

    assert "friends" in result.derived
    assert "popular" in result.derived

    # All 5 people in the social DB have outgoing KNOWS edges (except Eve)
    popular_rows = result.derived["popular"]
    assert len(popular_rows) >= 1


def test_config_override(social_db_populated):
    """Custom config is respected (max_iterations)."""
    db = social_db_populated
    session = db.session()
    # Non-recursive program should succeed even with max_iterations=1
    result = session.locy_with(FRIENDS_PROGRAM).max_iterations(1).run()
    assert len(result.derived.get("friends", [])) == 4


def test_empty_program_raises(social_db_populated):
    """Empty/blank program raises UniParseError (Locy requires at least one statement)."""
    db = social_db_populated
    session = db.session()
    with pytest.raises(uni_db.UniParseError):
        session.locy("")


def test_error_on_invalid_program(social_db_populated):
    """Invalid Locy program text raises UniParseError."""
    db = social_db_populated
    session = db.session()
    with pytest.raises(uni_db.UniParseError):
        session.locy("THIS IS COMPLETELY INVALID LOCY SYNTAX !!!!")


def test_derived_facts_structure(social_db_populated):
    """Derived fact rows are Python dicts with string keys."""
    db = social_db_populated
    session = db.session()
    result = session.locy(FRIENDS_PROGRAM)

    for row in result.derived.get("friends", []):
        assert isinstance(row, dict)
        for key in row:
            assert isinstance(key, str)


def test_param_binding_query_where(social_db_populated):
    """$param in QUERY WHERE resolves from top-level params kwarg."""
    db = social_db_populated
    session = db.session()
    program = """
CREATE RULE persons AS MATCH (p:Person) YIELD KEY p, p.name AS nm
QUERY persons WHERE nm = $target RETURN nm
"""
    result = session.locy(program, params={"target": "Alice"})
    rows = result.command_results[0]["rows"]
    assert len(rows) == 1
    assert rows[0]["nm"] == "Alice"


def test_param_binding_rule_where(social_db_populated):
    """$param in rule MATCH WHERE scopes the derived relation."""
    db = social_db_populated
    session = db.session()
    program = """
CREATE RULE named AS
  MATCH (p:Person)
  WHERE p.name = $target
  YIELD KEY p, p.name AS nm
QUERY named RETURN nm
"""
    result = session.locy(program, params={"target": "Bob"})
    rows = result.command_results[0]["rows"]
    assert len(rows) == 1
    assert rows[0]["nm"] == "Bob"


def test_param_binding_integer(social_db_populated):
    """Integer $param resolves correctly in WHERE comparison."""
    db = social_db_populated
    session = db.session()
    program = """
CREATE RULE adults AS MATCH (p:Person) YIELD KEY p, p.age AS age, p.name AS nm
QUERY adults WHERE age > $min_age RETURN nm
"""
    result = session.locy(program, params={"min_age": 30})
    rows = result.command_results[0]["rows"]
    names = {r["nm"] for r in rows}
    assert "Charlie" in names  # age 35
    assert "Bob" not in names  # age 25


def test_param_binding_with_config(social_db_populated):
    """params kwarg and config can be used together via locy_with builder."""
    db = social_db_populated
    session = db.session()
    program = """
CREATE RULE persons AS MATCH (p:Person) YIELD KEY p, p.name AS nm
QUERY persons WHERE nm = $target RETURN nm
"""
    result = (
        session.locy_with(program).param("target", "Alice").max_iterations(10).run()
    )
    rows = result.command_results[0]["rows"]
    assert len(rows) == 1
    assert rows[0]["nm"] == "Alice"


# ---------------------------------------------------------------------------
# Probabilistic Engine Verification Tests
# ---------------------------------------------------------------------------


def _prob_db():
    """Create a temporary database with schema for probabilistic tests."""
    db = uni_db.UniBuilder.temporary().build()
    (
        db.schema()
        .label("Node")
        .property("name", "string")
        .done()
        .label("Drug")
        .property("name", "string")
        .done()
        .label("Disease")
        .property("name", "string")
        .done()
        .label("SE")
        .property("name", "string")
        .property("sev", "float64")
        .done()
        .label("Risk")
        .property("score", "float64")
        .done()
        .edge_type("CAUSE", ["Node"], ["Node"])
        .property("prob", "float64")
        .done()
        .edge_type("CHECK", ["Node"], ["Node"])
        .property("conf", "float64")
        .done()
        .edge_type("HAS_RISK", ["Node"], ["Risk"])
        .done()
        .edge_type("SIG", ["Drug"], ["Disease"])
        .property("s", "float64")
        .done()
        .edge_type("IND", ["Drug"], ["Disease"])
        .done()
        .edge_type("ADR", ["Drug"], ["SE"])
        .property("freq", "float64")
        .done()
        .apply()
    )
    return db


def test_mnor_basic():
    """MNOR(0.3, 0.5) = 1 - (0.7)(0.5) = 0.65"""
    db = _prob_db()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Node {name: 'A'})")
    tx.execute("CREATE (:Node {name: 'B'})")
    tx.execute(
        "MATCH (a:Node {name:'A'}),(b:Node {name:'B'}) CREATE (a)-[:CAUSE {prob:0.3}]->(b)"
    )
    tx.execute(
        "MATCH (a:Node {name:'A'}),(b:Node {name:'B'}) CREATE (a)-[:CAUSE {prob:0.5}]->(b)"
    )
    tx.commit()
    result = session.locy("""
CREATE RULE risk AS MATCH (a:Node)-[e:CAUSE]->(b:Node) FOLD p=MNOR(e.prob) YIELD KEY a, KEY b, p
""")
    facts = result.derived.get("risk", [])
    assert len(facts) == 1
    assert facts[0]["p"] == pytest.approx(0.65, abs=1e-6)


def test_mprod_basic():
    """MPROD(0.8, 0.9) = 0.72"""
    db = _prob_db()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Node {name: 'A'})")
    tx.execute("CREATE (:Node {name: 'B'})")
    tx.execute(
        "MATCH (a:Node {name:'A'}),(b:Node {name:'B'}) CREATE (a)-[:CHECK {conf:0.8}]->(b)"
    )
    tx.execute(
        "MATCH (a:Node {name:'A'}),(b:Node {name:'B'}) CREATE (a)-[:CHECK {conf:0.9}]->(b)"
    )
    tx.commit()
    result = session.locy("""
CREATE RULE cov AS MATCH (a:Node)-[e:CHECK]->(b:Node) FOLD p=MPROD(e.conf) YIELD KEY a, KEY b, p
""")
    facts = result.derived.get("cov", [])
    assert len(facts) == 1
    assert facts[0]["p"] == pytest.approx(0.72, abs=1e-6)


def test_is_not_prob_complement():
    """IS NOT on PROB rule gives 1-p complement."""
    db = _prob_db()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Node {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.7})")
    tx.execute("CREATE (:Node {name: 'Bob'})-[:HAS_RISK]->(:Risk {score: 0.3})")
    tx.execute("CREATE (:Node {name: 'Charlie'})")
    tx.commit()
    result = session.locy("""
CREATE RULE risky AS MATCH (n:Node)-[:HAS_RISK]->(r:Risk) YIELD KEY n, r.score AS rs PROB
CREATE RULE safe AS MATCH (n:Node) WHERE n IS NOT risky YIELD KEY n, 1.0 AS safety PROB
""")
    safe = result.derived.get("safe", [])
    by_name = {r["n"].properties["name"]: r["safety"] for r in safe}
    assert by_name["Alice"] == pytest.approx(0.3, abs=1e-6)
    assert by_name["Bob"] == pytest.approx(0.7, abs=1e-6)
    assert by_name["Charlie"] == pytest.approx(1.0, abs=1e-6)


def test_fold_query_node_properties():
    """FOLD QUERY returns node names, not None."""
    db = _prob_db()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Node {name: 'A'})")
    tx.execute("CREATE (:Node {name: 'B'})")
    tx.execute(
        "MATCH (a:Node {name:'A'}),(b:Node {name:'B'}) CREATE (a)-[:CAUSE {prob:0.7}]->(b)"
    )
    tx.commit()
    result = session.locy("""
CREATE RULE risk AS MATCH (a:Node)-[e:CAUSE]->(b:Node) FOLD p=MNOR(e.prob) YIELD KEY a, KEY b, p
QUERY risk WHERE a = a RETURN a.name AS name, p
""")
    rows = result.command_results[0].rows
    assert len(rows) == 1
    assert rows[0]["name"] == "A"
    assert rows[0]["p"] == pytest.approx(0.7, abs=1e-6)


def test_composite_key_is_not():
    """Composite-key IS NOT filters by (drug, disease) pair, not drug alone."""
    db = _prob_db()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Drug {name: 'A'})")
    tx.execute("CREATE (:Disease {name: 'Flu'})")
    tx.execute("CREATE (:Disease {name: 'Cold'})")
    tx.execute(
        "MATCH (d:Drug {name:'A'}),(dis:Disease {name:'Flu'}) CREATE (d)-[:IND]->(dis)"
    )
    tx.execute(
        "MATCH (d:Drug {name:'A'}),(dis:Disease {name:'Flu'}) CREATE (d)-[:SIG {s:0.8}]->(dis)"
    )
    tx.execute(
        "MATCH (d:Drug {name:'A'}),(dis:Disease {name:'Cold'}) CREATE (d)-[:SIG {s:0.6}]->(dis)"
    )
    tx.commit()
    result = session.locy("""
CREATE RULE signal AS MATCH (d:Drug)-[e:SIG]->(dis:Disease) FOLD ev=MNOR(e.s) YIELD KEY d, KEY dis, ev
CREATE RULE known AS MATCH (d:Drug)-[:IND]->(dis:Disease) YIELD KEY d, KEY dis
CREATE RULE novel AS MATCH (d:Drug) WHERE d IS signal TO dis, d IS NOT known TO dis YIELD KEY d, KEY dis, ev AS score
QUERY novel WHERE d = d RETURN d.name AS drug, dis.name AS disease, score
""")
    novel = result.derived.get("novel", [])
    assert len(novel) == 1
    dis_name = novel[0]["dis"].properties["name"]
    assert dis_name == "Cold"
    # QUERY should also return Cold
    rows = result.command_results[0].rows
    assert len(rows) == 1
    assert rows[0]["disease"] == "Cold"


def test_mnor_edge_cases():
    """MNOR(0.0)=0.0 and MNOR(1.0)=1.0."""
    db = _prob_db()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Node {name: 'A'})-[:CAUSE {prob: 0.0}]->(:Node {name: 'B'})")
    tx.execute("CREATE (:Node {name: 'C'})-[:CAUSE {prob: 1.0}]->(:Node {name: 'D'})")
    tx.commit()
    result = session.locy("""
CREATE RULE risk AS MATCH (a:Node)-[e:CAUSE]->(b:Node) FOLD p=MNOR(e.prob) YIELD KEY a, KEY b, p
""")
    facts = result.derived.get("risk", [])
    by_src = {r["a"].properties["name"]: r["p"] for r in facts}
    assert by_src["A"] == pytest.approx(0.0, abs=1e-10)
    assert by_src["C"] == pytest.approx(1.0, abs=1e-10)


def test_assume_fold_query():
    """ASSUME mutation + FOLD MNOR re-evaluation returns correct values."""
    db = _prob_db()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Node {name: 'A'})")
    tx.execute("CREATE (:Node {name: 'B'})")
    tx.execute(
        "MATCH (a:Node {name:'A'}),(b:Node {name:'B'}) CREATE (a)-[:CAUSE {prob:0.4}]->(b)"
    )
    tx.commit()
    result = (
        session.locy_with("""
CREATE RULE risk AS MATCH (a:Node)-[e:CAUSE]->(b:Node) FOLD p=MNOR(e.prob) YIELD KEY a, KEY b, p
ASSUME { MATCH (a:Node {name:'A'})-[e:CAUSE]->(b:Node {name:'B'}) SET e.prob = 0.9 }
THEN { QUERY risk WHERE a.name = 'A' RETURN a.name AS name, p }
""")
        .max_iterations(200)
        .run()
    )
    # Baseline should be 0.4
    baseline = result.derived.get("risk", [])
    assert baseline[0]["p"] == pytest.approx(0.4, abs=1e-6)
    # ASSUME should show 0.9
    assume_cmd = next(c for c in result.command_results if c.command_type == "assume")
    assert assume_cmd.rows[0]["p"] == pytest.approx(0.9, abs=1e-6)


def _chain_session():
    """A -> B -> C over KNOWS, schemaless."""
    db = uni_db.Uni.in_memory()
    session = db.session()
    tx = session.tx()
    tx.execute(
        "CREATE (:Person {name: 'A'})-[:KNOWS]->(:Person {name: 'B'})"
        "-[:KNOWS]->(:Person {name: 'C'})"
    )
    tx.commit()
    return db, session


ASSUME_SEQUENCE_PROGRAM = """
CREATE RULE knows AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b
CREATE RULE link AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (a)-[:LINK]->(b)
CREATE RULE linked AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b
ASSUME { MATCH (c:Person {name: 'C'}) CREATE (c)-[:KNOWS]->(:Person {name: 'D'}) }
THEN {
  QUERY linked RETURN a.name AS a, b.name AS b
  DERIVE link
  QUERY linked RETURN a.name AS a, b.name AS b
}
"""


def _pairs(rows):
    return sorted((r["a"], r["b"]) for r in rows)


def test_assume_returns_one_result_per_body_command():
    """QUERY; DERIVE; QUERY in a body returns three results, not flat rows."""
    _db, session = _chain_session()
    result = session.locy(ASSUME_SEQUENCE_PROGRAM)
    (assume_cmd,) = result.command_results
    assert isinstance(assume_cmd, uni_db.AssumeCommandResult)
    first, derive, second = assume_cmd.results
    assert first.command_type == "query" and first.rows == []
    assert derive.command_type == "derive" and derive.affected == 3
    assert second.command_type == "query"
    assert _pairs(second.rows) == [("A", "B"), ("B", "C"), ("C", "D")]
    # `derived` is the state the first body command reads: the mutation's
    # KNOWS edge is in it, the body's DERIVE is not.
    assert len(assume_cmd.derived["knows"]) == 3
    assert assume_cmd.derived["linked"] == []
    # `rows` is the first row-producing body result's rows: the empty QUERY.
    assert assume_cmd.rows == []
    assert result.rows() == []
    # The stored graph is untouched.
    assert len(result.derived["knows"]) == 2


def test_assume_rows_and_locy_result_rows_descend_depth_first():
    """A DERIVE-only ASSUME has no rows; LocyResult.rows() moves past it."""
    _db, session = _chain_session()
    result = session.locy("""
CREATE RULE link AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (a)-[:LINK]->(b)
CREATE RULE linked AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b
ASSUME { CREATE (:Marker {name: 'm'}) } THEN { DERIVE link }
ASSUME { CREATE (:Marker {name: 'm'}) } THEN {
  DERIVE link
  ASSUME { CREATE (:Marker {name: 'n'}) } THEN { QUERY linked RETURN a.name AS a, b.name AS b }
}
""")
    derive_only, nested = result.command_results
    assert derive_only.rows == []
    assert [r.affected for r in derive_only.results] == [2]
    inner = nested.results[1]
    assert isinstance(inner, uni_db.AssumeCommandResult)
    assert _pairs(inner.results[0].rows) == [("A", "B"), ("B", "C")]
    assert _pairs(nested.rows) == [("A", "B"), ("B", "C")]
    assert _pairs(result.rows()) == [("A", "B"), ("B", "C")]
