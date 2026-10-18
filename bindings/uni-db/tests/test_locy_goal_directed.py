# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""`LocyConfig.goal_directed` (the magic-sets rewrite) from Python.

Mirrors `crates/uni/tests/common/locy/locy_goal_directed.rs`:

* answers: a goal-directed QUERY returns the same rows with the rewrite
  "off", "on" and "shadow";
* `derived`: in "on" mode a rule evaluated only for a rewritten goal is absent
  (never partial), an unrelated rule is still published, and no generated
  `__locy_gd*` rule leaks; "shadow" publishes exactly what "off" does;
* an unknown mode is a `ValueError`, not a silently ignored value.

The `derived` assertions are what make these tests discriminate: an extractor
that dropped the setting would still return equal answers, but would publish
`reach` in "on" mode.

Both config forms (dict and `LocyConfig`) and both APIs (sync and async) are
covered, since each form has its own converter.
"""

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

MODES = ["off", "on", "shadow"]
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


def assert_on_mode_contract(on, off, shadow):
    assert answers(on) == answers(off) == answers(shadow) == EXPECTED
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
    off, on, shadow = (run(db, WITH_HUB, form(m)) for m in MODES)
    assert_on_mode_contract(on, off, shadow)


@pytest.mark.parametrize("form", CONFIG_FORMS)
def test_mode_is_case_insensitive(db, form):
    on = run(db, WITH_HUB, form("ON"))
    shadow = run(db, WITH_HUB, form("Shadow"))
    assert "reach" not in on.derived
    assert "reach" in shadow.derived


def test_locy_config_reports_the_mode():
    assert uni_db.LocyConfig().goal_directed == "off"
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
    assert_on_mode_contract(on, off, shadow)


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
