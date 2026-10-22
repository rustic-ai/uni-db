# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for the locy_evaluate binding on AsyncUni."""

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Helpers (same programs as sync tests)
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


async def test_basic_rule_evaluation(async_social_db_populated):
    """Async: a simple rule derives facts from graph edges."""
    db = async_social_db_populated
    session = db.session()
    result = await session.locy(FRIENDS_PROGRAM)

    assert hasattr(result, "derived")
    assert hasattr(result, "stats")
    assert hasattr(result, "command_results")

    friends_rows = result.derived.get("friends", [])
    assert len(friends_rows) == 4, f"Expected 4 friend pairs, got {len(friends_rows)}"

    for row in friends_rows:
        assert "a" in row
        assert "b" in row


async def test_stats_returned(async_social_db_populated):
    """Async: result includes a LocyStats object with expected fields."""
    db = async_social_db_populated
    session = db.session()
    result = await session.locy(FRIENDS_PROGRAM)

    stats = result.stats
    assert isinstance(stats, uni_db.LocyStats)
    assert stats.strata_evaluated >= 1
    # Non-recursive strata may complete in 0 fixpoint iterations
    assert stats.total_iterations >= 0
    assert stats.evaluation_time_secs >= 0.0
    assert stats.queries_executed >= 0
    assert stats.mutations_executed >= 0
    assert stats.peak_memory_bytes >= 0


async def test_query_command(async_social_db_populated):
    """Async: QUERY command result appears in command_results."""
    db = async_social_db_populated
    session = db.session()
    result = await session.locy(QUERY_PROGRAM)

    cmd_results = result.command_results
    assert len(cmd_results) >= 1

    query_result = cmd_results[0]
    assert query_result["type"] == "query"
    rows = query_result["rows"]
    # Alice is connected to Bob and Charlie
    assert len(rows) >= 1
    to_names = {r["to_name"] for r in rows}
    assert "Bob" in to_names


async def test_recursive_rule(async_social_db_populated):
    """Async: recursive rule computes transitive closure."""
    db = async_social_db_populated
    session = db.session()
    result = await session.locy(REACHABLE_PROGRAM)

    reachable_rows = result.derived.get("reachable", [])
    assert len(reachable_rows) >= 3


async def test_multi_rule_program(async_social_db_populated):
    """Async: multiple rules can be defined in one program."""
    db = async_social_db_populated
    session = db.session()
    result = await session.locy(MULTI_RULE_PROGRAM)

    assert "friends" in result.derived
    assert "popular" in result.derived
    assert len(result.derived["popular"]) >= 1


async def test_config_override(async_social_db_populated):
    """Async: custom config is respected (max_iterations)."""
    db = async_social_db_populated
    session = db.session()
    result = await session.locy_with(FRIENDS_PROGRAM).max_iterations(1).run()
    assert len(result.derived.get("friends", [])) == 4


async def test_empty_program_raises(async_social_db_populated):
    """Async: empty program raises UniParseError (Locy requires at least one statement)."""
    db = async_social_db_populated
    session = db.session()
    with pytest.raises(uni_db.UniParseError):
        await session.locy("")


async def test_error_on_invalid_program(async_social_db_populated):
    """Async: invalid Locy program text raises UniParseError."""
    db = async_social_db_populated
    session = db.session()
    with pytest.raises(uni_db.UniParseError):
        await session.locy("THIS IS COMPLETELY INVALID LOCY SYNTAX !!!!")


async def test_derived_facts_structure(async_social_db_populated):
    """Async: derived fact rows are Python dicts with string keys."""
    db = async_social_db_populated
    session = db.session()
    result = await session.locy(FRIENDS_PROGRAM)

    for row in result.derived.get("friends", []):
        assert isinstance(row, dict)
        for key in row:
            assert isinstance(key, str)


async def test_param_binding_query_where(async_social_db_populated):
    """Async: $param in QUERY WHERE resolves from top-level params kwarg."""
    db = async_social_db_populated
    session = db.session()
    program = """
CREATE RULE persons AS MATCH (p:Person) YIELD KEY p, p.name AS nm
QUERY persons WHERE nm = $target RETURN nm
"""
    result = await session.locy(program, params={"target": "Alice"})
    rows = result.command_results[0]["rows"]
    assert len(rows) == 1
    assert rows[0]["nm"] == "Alice"


async def test_param_binding_integer(async_social_db_populated):
    """Async: integer $param resolves correctly in WHERE comparison."""
    db = async_social_db_populated
    session = db.session()
    program = """
CREATE RULE adults AS MATCH (p:Person) YIELD KEY p, p.age AS age, p.name AS nm
QUERY adults WHERE age > $min_age RETURN nm
"""
    result = await session.locy(program, params={"min_age": 30})
    rows = result.command_results[0]["rows"]
    names = {r["nm"] for r in rows}
    assert "Charlie" in names  # age 35
    assert "Bob" not in names  # age 25


async def test_session_profile(async_social_db_populated):
    """Async: `session.locy_with(prog).profile()` returns (result, profile).

    Mirrors the sync `test_profile_recursive`; guards the async Locy
    profile() parity gap (sync builders had profile(), async did not).
    """
    db = async_social_db_populated
    session = db.session()
    result, profile = await session.locy_with(REACHABLE_PROGRAM).profile()

    # Same derived facts as run().
    assert len(result.derived.get("reachable", [])) >= 3

    # Structured profile down to a recursive stratum.
    assert profile.total_time_ms >= 0.0
    recursive = [s for s in profile.strata if s["recursive"]]
    assert recursive, "expected a recursive stratum"
    assert recursive[0]["iterations"] >= 2
    assert "Locy Profile" in str(profile)


async def test_tx_profile(async_social_db_populated):
    """Async: `tx.locy_with(prog).profile()` returns (result, profile)."""
    db = async_social_db_populated
    session = db.session()
    tx = await session.tx()
    result, profile = await tx.locy_with(REACHABLE_PROGRAM).profile()

    assert len(result.derived.get("reachable", [])) >= 3
    assert profile.total_time_ms >= 0.0
    assert isinstance(profile.strata, list) and len(profile.strata) >= 1


# ---------------------------------------------------------------------------
# Top-level commands run in sequence
# ---------------------------------------------------------------------------

BACK_PROGRAM = """
CREATE RULE knows AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b
CREATE RULE back AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (b)-[:KNOWS]->(a)
QUERY knows RETURN a.name AS src
DERIVE back
QUERY knows RETURN a.name AS src
"""


@pytest.mark.parametrize("level", ["session", "tx"])
async def test_query_after_derive_reads_the_derived_edges(
    async_social_db_populated, level
):
    """Async: a QUERY after a DERIVE in the same program sees the derived edges."""
    session = async_social_db_populated.session()
    if level == "tx":
        tx = await session.tx()
        result = await tx.locy(BACK_PROGRAM)
    else:
        result = await session.locy(BACK_PROGRAM)
    before, derive, after = result.command_results
    # 4 KNOWS edges, then their 4 derived reverses on top.
    assert len(before["rows"]) == 4
    assert derive["affected"] == 4
    assert len(after["rows"]) == 8
    assert result.stats.sequenced_strata_evaluated > 0
