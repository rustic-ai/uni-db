# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Issue #289: `locy_with` timeouts and the type of a cost refusal.

Two defects, both on the Locy path only:

1. An explicit `locy_with(q).timeout(t)` above the database `query_timeout`
   was silently capped at it. From Python there was a second way to lose it:
   the builder applied `.with_config(..)` *after* `.timeout(..)`, and
   `with_config` replaces the whole `LocyConfig`.
2. A program refused for cost (deadline, memory) raised `UniQueryError` -- the
   class an ordinary broken program raises -- so only the message told them
   apart. The Cypher path already raised `UniTimeoutError`.
"""

import pytest

import uni_db

CARTESIAN = "MATCH (a:Entity),(b:Entity),(c:Entity) RETURN count(*) AS n"
PATHS = "MATCH p=(a:Entity)-[:OWNS*]->(b:Entity) RETURN count(p) AS n"


def _entities(n, query_timeout, cyclic=False):
    db = uni_db.UniBuilder.temporary().config({"query_timeout": query_timeout}).build()
    (
        db.schema()
        .label("Entity")
        .property("uid", "string")
        .done()
        .edge_type("OWNS", ["Entity"], ["Entity"])
        .done()
        .apply()
    )
    session = db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()
    vids = list(writer.insert_vertices("Entity", [{"uid": f"e{i}"} for i in range(n)]))
    if cyclic:
        edges = [
            (vids[i], vids[(i * 7 + k * 13 + 1) % n], {})
            for i in range(n)
            for k in range(3)
        ]
        writer.insert_edges("OWNS", edges)
    writer.commit()
    tx.commit()
    return db


@pytest.fixture(scope="module")
def slow_db():
    # 250^3 rows to count: comfortably past a 250 ms database default.
    return _entities(250, query_timeout=0.25)


def test_the_database_default_still_bounds_an_unconfigured_program(slow_db):
    """Control: without a Locy timeout the database default binds.

    This is also what shows the program outlasts 250 ms, so the tests below
    completing is the explicit timeout being honoured.
    """
    with pytest.raises(uni_db.UniTimeoutError):
        slow_db.session().locy_with(CARTESIAN).run()


def test_an_explicit_timeout_above_the_database_default_is_honoured(slow_db):
    result = slow_db.session().locy_with(CARTESIAN).timeout(120.0).run()
    assert result is not None


def test_with_config_does_not_discard_an_explicit_timeout(slow_db):
    """`.with_config(..)` used to be applied last and overwrite `.timeout(..)`."""
    result = (
        slow_db.session()
        .locy_with(CARTESIAN)
        .timeout(120.0)
        .with_config({"max_iterations": 50})
        .run()
    )
    assert result is not None


def test_a_timeout_given_in_the_config_is_honoured(slow_db):
    result = (
        slow_db.session().locy_with(CARTESIAN).with_config({"timeout": 120.0}).run()
    )
    assert result is not None


def test_a_locy_timeout_is_uni_timeout_error(slow_db):
    with pytest.raises(uni_db.UniTimeoutError):
        slow_db.session().locy_with(CARTESIAN).timeout(0.1).run()


def test_a_locy_memory_refusal_is_uni_memory_limit_exceeded_error():
    db = _entities(30, query_timeout=30.0, cyclic=True)
    with pytest.raises(uni_db.UniMemoryLimitExceededError):
        db.session().locy_with(PATHS).max_memory(1024 * 1024).run()


def test_a_broken_program_is_still_uni_query_error(slow_db):
    """Control: the refusal types must not swallow ordinary failures."""
    with pytest.raises(uni_db.UniQueryError):
        slow_db.session().locy("MATCH (e:Entity) RETURN nosuchfn(e.uid) AS x LIMIT 1")
