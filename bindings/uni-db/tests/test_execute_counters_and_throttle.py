# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""ExecuteResult side-effect counters, write-throttle metrics, and the typed
errors (endpoint, missing parameter, uncommitted writes, stalled writes),
through the Python surface.

The counters follow the openCypher TCK side effects: an update is not a
create, a property is (entity, key, value), and labels count per node whose
label set changes.
"""

import pytest

import uni_db


def _db():
    db = uni_db.Uni.in_memory()
    db.schema().label("T").property("k", "int").property_nullable(
        "v", "int"
    ).done().label("Extra").apply()
    return db


def _exec(session, cypher):
    tx = session.tx()
    r = tx.execute(cypher)
    tx.commit()
    return (
        r.nodes_created,
        r.nodes_deleted,
        r.relationships_created,
        r.relationships_deleted,
        r.properties_set,
        r.properties_removed,
        r.labels_added,
        r.labels_removed,
    )


def test_counters_follow_tck_side_effects():
    db = _db()
    s = db.session()
    #           +n -n +r -r +p -p +l -l
    assert _exec(s, "CREATE (:T {k: 1})") == (1, 0, 0, 0, 1, 0, 1, 0)
    assert _exec(s, "MATCH (n:T {k: 1}) SET n.v = 7") == (0, 0, 0, 0, 1, 0, 0, 0)
    assert _exec(s, "MATCH (n:T {k: 1}) SET n.v = 8") == (0, 0, 0, 0, 1, 1, 0, 0)
    assert _exec(s, "MATCH (n:T {k: 1}) SET n.v = 8") == (0, 0, 0, 0, 0, 0, 0, 0)
    assert _exec(s, "MERGE (n:T {k: 2}) ON CREATE SET n.v = 1") == (
        1,
        0,
        0,
        0,
        2,
        0,
        1,
        0,
    )
    assert _exec(s, "MATCH (n:T {k: 2}) SET n:Extra") == (0, 0, 0, 0, 0, 0, 1, 0)
    assert _exec(s, "MATCH (n:T {k: 2}) REMOVE n:Extra") == (0, 0, 0, 0, 0, 0, 0, 1)
    assert _exec(s, "MATCH (n:T {k: 2}) REMOVE n.v") == (0, 0, 0, 0, 0, 1, 0, 0)


def test_throttle_metrics_are_exposed():
    db = _db()
    m = db.metrics()
    assert m.write_throttle_pressure == 0.0
    assert m.write_throttle_waits == 0
    assert m.write_throttle_wait_time_secs == 0.0
    assert m.writes_stalled == 0


def test_edge_endpoint_violation_is_typed():
    db = uni_db.Uni.in_memory()
    db.schema().label("A").done().label("B").done().edge_type("R", ["A"], ["B"]).apply()
    s = db.session()
    tx = s.tx()
    with pytest.raises(uni_db.UniEdgeEndpointError) as info:
        tx.execute("CREATE (:A)-[:R]->(:A)")
    assert isinstance(info.value, uni_db.UniConstraintError)
    assert info.value.edge_type == "R"
    assert info.value.endpoint == "target"
    assert info.value.labels == ["A"]
    assert info.value.allowed == ["B"]


def test_parameter_missing_is_typed():
    db = uni_db.Uni.in_memory()
    with pytest.raises(uni_db.UniParameterMissingError) as ei:
        db.session().query("RETURN $missing AS x")
    assert ei.value.names == ["missing"]
    # A missing parameter is a query error, so existing handlers still match.
    assert isinstance(ei.value, uni_db.UniQueryError)
    tx = db.session().tx()
    with pytest.raises(uni_db.UniParameterMissingError):
        tx.execute("CREATE (:T {k: $absent})")
    tx.rollback()


def test_parameter_missing_names_every_parameter():
    db = uni_db.Uni.in_memory()
    s = db.session()
    with pytest.raises(uni_db.UniParameterMissingError) as ei:
        s.query("RETURN $a, $b, $a")
    assert ei.value.names == ["a", "b"]
    with pytest.raises(uni_db.UniParameterMissingError) as ei:
        s.query("RETURN $a, $b", {"a": 1})
    assert ei.value.names == ["b"]
    tx = s.tx()
    with pytest.raises(uni_db.UniParameterMissingError) as ei:
        tx.execute("CREATE (:T {k: $k, v: $v})")
    assert ei.value.names == ["k", "v"]
    tx.rollback()
    with pytest.raises(uni_db.UniParameterMissingError) as ei:
        s.locy(
            "CREATE RULE r AS MATCH (n:T) WHERE n.k = $x YIELD KEY n "
            "QUERY r WHERE n.v = $y RETURN n.k AS k"
        )
    assert ei.value.names == ["x", "y"]


def test_uncommitted_writes_in_transaction_is_typed():
    db = uni_db.Uni.in_memory()
    db.schema().label("Person").property("name", "string").done().edge_type(
        "KNOWS", ["Person"], ["Person"]
    ).apply()
    tx = db.session().tx()
    tx.execute("CREATE (:Person {name: 'b'})-[:KNOWS]->(:Person {name: 'c'})")
    with pytest.raises(uni_db.UniUncommittedWritesError) as ei:
        tx.query(
            "CALL uni.graph.project('g', {nodeLabels: ['Person'], "
            "edgeTypes: ['KNOWS']}, {}) YIELD node_count RETURN node_count"
        )
    assert ei.value.operation == "uni.graph.project"
    assert ei.value.reason
    assert isinstance(ei.value, uni_db.UniTransactionError)
    tx.rollback()


def test_write_stalled_with_compaction_disabled(tmp_path):
    db = (
        uni_db.UniBuilder.open(str(tmp_path / "db"))
        .config(
            {
                "compaction": {"enabled": False},
                "throttle": {"soft_limit": 1, "hard_limit": 2, "base_delay": 10.0},
            }
        )
        .build()
    )
    cfg = db.config()
    assert cfg["compaction"]["enabled"] is False
    assert cfg["throttle"]["hard_limit"] == 2
    db.schema().label("Item").property("n", "int").apply()
    s = db.session()
    for n in (1, 2):
        tx = s.tx()
        tx.execute(f"CREATE (:Item {{n: {n}}})")
        tx.commit()
        db.flush()
    tx = s.tx()
    with pytest.raises(uni_db.UniWriteStalledError) as ei:
        tx.execute("CREATE (:Item {n: 3})")
    assert (ei.value.l1_runs, ei.value.hard_limit) == (2, 2)
    assert "uni.admin.compact()" in str(ei.value)
    tx.rollback()
    assert db.metrics().writes_stalled >= 1
    # The way out the message names.
    s.run("CALL uni.admin.compact() YIELD tables_optimized RETURN tables_optimized")
    tx = s.tx()
    tx.execute("CREATE (:Item {n: 3})")
    tx.commit()
    assert s.query("MATCH (n:Item) RETURN count(n) AS c")[0]["c"] == 3


def test_nested_config_rejects_unknown_keys():
    with pytest.raises(ValueError, match="unknown compaction config key 'enabeld'"):
        uni_db.UniBuilder.temporary().config({"compaction": {"enabeld": False}})
    with pytest.raises(ValueError, match="unknown throttle config key"):
        uni_db.UniBuilder.temporary().config({"throttle": {"hard": 1}})
