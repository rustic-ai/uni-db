# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""`LocyConfig.goal_directed` (the magic-sets rewrite) from Python.

Mirrors `crates/uni/tests/common/locy/locy_goal_directed.rs`:

* answers: a goal-directed QUERY returns the same rows with the rewrite
  "auto" (the default), "off", "on" and "shadow";
* `derived`: in "on" mode a rule evaluated only for a rewritten goal is absent
  (never partial), an unrelated rule is still published, and no generated
  `__locy_gd*` rule leaks; "auto" and "shadow" publish exactly what "off"
  does, "auto" deriving the left-out rule on the first `derived` read,
  against the snapshot the evaluation read;
* an unknown mode is a `ValueError`, not a silently ignored value.

The `derived` assertions are what make these tests discriminate: an extractor
that dropped the setting would still return equal answers, but would publish
`reach` in "on" mode.

Both config forms (dict and `LocyConfig`) and both APIs (sync and async) are
covered, since each form has its own converter.
"""

import inspect

import pytest

import uni_db

NODES = ["a", "b", "c", "d", "x", "y", "z"]
EDGES = [("a", "b"), ("b", "c"), ("c", "d"), ("d", "a"), ("x", "y"), ("y", "z")]

TC_RIGHT = (
    "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n"
    "CREATE RULE reach AS MATCH (a:N)-[:E]->(m:N) WHERE m IS reach TO b "
    "YIELD KEY a, KEY b\n"
)
QUERY = "QUERY reach WHERE a.name = 'a' RETURN b.name AS n"
PROGRAM = TC_RIGHT + QUERY
WITH_HUB = TC_RIGHT + "CREATE RULE hub AS MATCH (v:N)-[:E]->(:N) YIELD KEY v\n" + QUERY

MODES = ["off", "on", "shadow", "auto"]
# From `a`, the cycle a->b->c->d->a reaches every node of its component.
EXPECTED = ["a", "b", "c", "d"]

DDL = ["CREATE LABEL N (name STRING)", "CREATE EDGE TYPE E () FROM N TO N"]
CREATES = [f"CREATE (:N {{name: '{n}'}})" for n in NODES] + [
    f"MATCH (s:N {{name: '{s}'}}), (d:N {{name: '{d}'}}) CREATE (s)-[:E]->(d)"
    for s, d in EDGES
]


def as_dict(mode):
    return {"goal_directed": mode}


def as_object(mode):
    return uni_db.LocyConfig(goal_directed=mode)


CONFIG_FORMS = [pytest.param(as_dict, id="dict"), pytest.param(as_object, id="object")]


def answers(result):
    queries = [c for c in result.command_results if c["type"] == "query"]
    assert len(queries) == 1, result.command_results
    return sorted(r["n"] for r in queries[0]["rows"])


def derived_sizes(result):
    return {k: len(v) for k, v in result.derived.items()}


def cell(v):
    # A node's identity is its name here; other cells compare as they are.
    return v["name"] if isinstance(v, uni_db.Node) else v


def derived_facts(result):
    """Every rule's facts as sorted tuples, independent of row order."""
    return {
        rule: sorted(
            tuple(sorted((k, repr(cell(v))) for k, v in row.items())) for row in rows
        )
        for rule, rows in result.derived.items()
    }


def assert_on_mode_contract(on, off, shadow, auto=None):
    assert answers(on) == answers(off) == answers(shadow) == EXPECTED
    if auto is not None:
        assert answers(auto) == EXPECTED
        assert derived_facts(auto) == derived_facts(off)
    assert "reach" in off.derived, off.derived.keys()
    assert "reach" not in on.derived, on.derived.keys()
    assert "hub" in on.derived, "an unrelated rule is still published"
    assert not [k for k in on.derived if k.startswith("__locy_gd")], on.derived.keys()
    assert len(on.derived["hub"]) == len(off.derived["hub"])
    assert derived_sizes(shadow) == derived_sizes(off)


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def db():
    db = uni_db.UniBuilder.temporary().build()
    session = db.session()
    for stmts in (DDL, CREATES):
        tx = session.tx()
        for s in stmts:
            tx.execute(s)
        tx.commit()
    return db


def run(db, program, config):
    return db.session().locy_with(program).with_config(config).run()


@pytest.mark.parametrize("form", CONFIG_FORMS)
def test_answers_equal_across_modes(db, form):
    for mode in MODES:
        assert answers(run(db, PROGRAM, form(mode))) == EXPECTED, mode


@pytest.mark.parametrize("form", CONFIG_FORMS)
def test_on_mode_omits_rules_only_goals_needed(db, form):
    off, on, shadow, auto = (run(db, WITH_HUB, form(m)) for m in MODES)
    assert_on_mode_contract(on, off, shadow, auto)


def test_default_derived_equals_off(db):
    off = run(db, WITH_HUB, {"goal_directed": "off"})
    default = db.session().locy(WITH_HUB)
    assert answers(default) == EXPECTED
    assert "reach" in default.derived
    assert derived_facts(default) == derived_facts(off)
    assert default.derived_facts("reach") is not None
    # The same dict on every read: the left-out rule was added once.
    assert default.derived is default.derived


def test_default_derives_left_out_rules_in_the_evaluated_snapshot():
    db = uni_db.UniBuilder.temporary().build()
    session = db.session()
    for stmts in (DDL, CREATES):
        tx = session.tx()
        for s in stmts:
            tx.execute(s)
        tx.commit()
    program = TC_RIGHT + "QUERY reach WHERE a.name = 'x' RETURN b.name AS n"
    before = derived_facts(run(db, program, {"goal_directed": "off"}))
    default = session.locy(program)
    # `z -> a` joins the two components after the evaluation.
    tx = session.tx()
    tx.execute("MATCH (z:N {name: 'z'}), (a:N {name: 'a'}) CREATE (z)-[:E]->(a)")
    tx.commit()
    after = derived_facts(run(db, program, {"goal_directed": "off"}))
    assert after != before
    assert derived_facts(default) == before


@pytest.mark.parametrize("form", CONFIG_FORMS)
def test_mode_is_case_insensitive(db, form):
    on = run(db, WITH_HUB, form("ON"))
    shadow = run(db, WITH_HUB, form("Shadow"))
    assert "reach" not in on.derived
    assert "reach" in shadow.derived


def test_locy_config_reports_the_mode():
    assert uni_db.LocyConfig().goal_directed == "auto"
    assert uni_db.LocyConfig(goal_directed="Auto").goal_directed == "auto"
    assert uni_db.LocyConfig(goal_directed="off").goal_directed == "off"
    assert uni_db.LocyConfig(goal_directed="ON").goal_directed == "on"
    assert uni_db.LocyConfig(goal_directed="shadow").goal_directed == "shadow"


@pytest.mark.parametrize("bad", ["yes", "", "magic", "of"])
def test_unknown_mode_is_rejected(db, bad):
    with pytest.raises(ValueError, match="goal_directed"):
        uni_db.LocyConfig(goal_directed=bad)
    with pytest.raises(ValueError, match="goal_directed"):
        db.session().locy_with(PROGRAM).with_config({"goal_directed": bad})


def test_non_string_mode_is_rejected(db):
    with pytest.raises(TypeError):
        uni_db.LocyConfig(goal_directed=True)
    with pytest.raises(TypeError):
        db.session().locy_with(PROGRAM).with_config({"goal_directed": True})


def test_tx_locy_builder_honours_the_mode(db):
    session = db.session()
    tx = session.tx()
    try:
        on = tx.locy_with(WITH_HUB).with_config({"goal_directed": "on"}).run()
        off = tx.locy_with(WITH_HUB).with_config({"goal_directed": "off"}).run()
    finally:
        tx.rollback()
    assert answers(on) == answers(off) == EXPECTED
    assert "reach" not in on.derived
    assert "reach" in off.derived


def test_tx_locy_default_publishes_every_rule(db):
    session = db.session()
    tx = session.tx()
    try:
        default = tx.locy(WITH_HUB)
        off = tx.locy_with(WITH_HUB).with_config({"goal_directed": "off"}).run()
    finally:
        tx.rollback()
    assert answers(default) == EXPECTED
    assert derived_facts(default) == derived_facts(off)


# ---------------------------------------------------------------------------
# Async
# ---------------------------------------------------------------------------


@pytest.fixture
async def async_db():
    db = await uni_db.AsyncUni.temporary()
    session = db.session()
    for stmts in (DDL, CREATES):
        tx = await session.tx()
        for s in stmts:
            await tx.execute(s)
        await tx.commit()
    return db


async def async_run(db, program, config):
    return await db.session().locy_with(program).with_config(config).run()


@pytest.mark.parametrize("form", CONFIG_FORMS)
async def test_async_answers_equal_across_modes(async_db, form):
    for mode in MODES:
        result = await async_run(async_db, PROGRAM, form(mode))
        assert answers(result) == EXPECTED, mode


@pytest.mark.parametrize("form", CONFIG_FORMS)
async def test_async_on_mode_omits_rules_only_goals_needed(async_db, form):
    off = await async_run(async_db, WITH_HUB, form("off"))
    on = await async_run(async_db, WITH_HUB, form("on"))
    shadow = await async_run(async_db, WITH_HUB, form("shadow"))
    auto = await async_run(async_db, WITH_HUB, form("auto"))
    assert_on_mode_contract(on, off, shadow, auto)


async def test_async_default_derived_equals_off(async_db):
    off = await async_run(async_db, WITH_HUB, {"goal_directed": "off"})
    default = await async_db.session().locy(WITH_HUB)
    assert answers(default) == EXPECTED
    assert "reach" in default.derived
    assert derived_facts(default) == derived_facts(off)


async def test_async_unknown_mode_is_rejected(async_db):
    with pytest.raises(ValueError, match="goal_directed"):
        async_db.session().locy_with(PROGRAM).with_config({"goal_directed": "bogus"})


async def test_async_tx_locy_builder_honours_the_mode(async_db):
    session = async_db.session()
    tx = await session.tx()
    try:
        on = await tx.locy_with(WITH_HUB).with_config({"goal_directed": "on"}).run()
        off = await tx.locy_with(WITH_HUB).with_config({"goal_directed": "off"}).run()
    finally:
        await tx.rollback()
    assert answers(on) == answers(off) == EXPECTED
    assert "reach" not in on.derived
    assert "reach" in off.derived


async def test_async_tx_locy_default_publishes_every_rule(async_db):
    session = async_db.session()
    tx = await session.tx()
    try:
        default = await tx.locy(WITH_HUB)
        off = await tx.locy_with(WITH_HUB).with_config({"goal_directed": "off"}).run()
    finally:
        await tx.rollback()
    assert answers(default) == EXPECTED
    assert derived_facts(default) == derived_facts(off)


# ---------------------------------------------------------------------------
# Warnings of left-out rules, and the awaitable `derived_async()`
# ---------------------------------------------------------------------------

SHARED_DDL = [
    "CREATE LABEL P (name STRING)",
    "CREATE EDGE TYPE Q (p FLOAT) FROM P TO P",
]
SHARED_CREATES = [f"CREATE (:P {{name: '{n}'}})" for n in "ABCD"] + [
    f"MATCH (x:P {{name: '{a}'}}), (y:P {{name: '{b}'}}) CREATE (x)-[:Q {{p: {p}}}]->(y)"
    for a, b, p in [("A", "B", 0.5), ("A", "C", 0.3), ("B", "C", 0.4), ("C", "D", 0.6)]
]
# `reach` warns SharedProbabilisticDependency (two proofs of A -> D share
# C -> D); `link` reads it and is left out by its goal, so deriving it later
# evaluates `reach` again.
SHARED_PROGRAM = (
    "CREATE RULE reach AS MATCH (a:P)-[e:Q]->(b:P) "
    "FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob\n"
    "CREATE RULE reach AS MATCH (a:P)-[e:Q]->(mid:P) WHERE mid IS reach TO b "
    "FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob\n"
    "CREATE RULE link AS MATCH (a:P)-[:Q]->(b:P) WHERE b IS reach TO c "
    "YIELD KEY a, KEY c\n"
    "QUERY reach RETURN a.name AS src, b.name AS dst, prob\n"
    "QUERY link WHERE a.name = 'A' RETURN c.name AS n"
)


def warning_keys(result):
    return sorted((w["code"], w["rule_name"]) for w in result.warnings)


def test_default_reports_off_warnings_once_derived():
    db = uni_db.UniBuilder.temporary().build()
    session = db.session()
    for stmts in (SHARED_DDL, SHARED_CREATES):
        tx = session.tx()
        for s in stmts:
            tx.execute(s)
        tx.commit()
    off = run(db, SHARED_PROGRAM, {"goal_directed": "off"})
    default = session.locy(SHARED_PROGRAM)
    want = warning_keys(off)
    assert ("shared_probabilistic_dependency", "reach") in want, want
    assert derived_facts(default) == derived_facts(off)
    assert warning_keys(default) == want


async def test_async_derived_async_returns_every_rule(async_db):
    off = await async_run(async_db, WITH_HUB, {"goal_directed": "off"})
    default = await async_db.session().locy(WITH_HUB)
    pending = default.derived_async()
    assert inspect.isawaitable(pending)
    derived = await pending
    assert "reach" in derived
    assert derived is default.derived
    assert derived_facts(default) == derived_facts(off)
    # Already derived: awaiting again returns the same dict.
    assert await default.derived_async() is derived
