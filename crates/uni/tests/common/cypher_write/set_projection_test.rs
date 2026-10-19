// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression tests for SET-target scan projection — the bisect-net for the
// Part B narrow-projection sentinel refactor (see plan
// /home/rohit/.claude/plans/plan-and-implement-a-valiant-flame.md).
//
// Every test here:
//   1. Performs a SET (potentially with extra clauses around it).
//   2. Reads back the affected entity.
//   3. Asserts the SET values landed AND any untouched columns survived.
//
// The Round 4 Fix-3 attempt reported a 7× speedup that was partly a silent
// no-op — `MutationSetExec` reported rows>0 / time_ms>0 while writing
// nothing. These tests catch that class of regression in ~3 seconds each.
// Rust guideline compliant

use anyhow::Result;
use uni_db::api::schema::{IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
use uni_db::{DataType, ScalarType, Uni, Value};

/// F1a — single property SET on a simple schema round-trips correctly.
#[tokio::test]
async fn f1a_set_single_property_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1, y: 2})")
        .await?;
    tx.commit().await?;

    let vid: i64 = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid")
        .await?
        .rows()[0]
        .get("nid")
        .unwrap();

    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (n:Entity) WHERE id(n) = $v SET n.x = 42")
        .param("v", Value::Int(vid))
        .run()
        .await?;
    tx.commit().await?;

    let row = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(
        row.rows()[0].get::<i64>("x").unwrap(),
        42,
        "SET n.x did not apply"
    );
    assert_eq!(
        row.rows()[0].get::<i64>("y").unwrap(),
        2,
        "untouched n.y was clobbered"
    );
    Ok(())
}

/// F1b — multi-property SET on a wide schema. Verifies set columns updated
/// AND untouched columns preserved.
#[tokio::test]
async fn f1b_set_multi_property_wide_schema_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("a", DataType::Int64)
        .property_nullable("b", DataType::Int64)
        .property_nullable("c", DataType::Int64)
        .property_nullable("d", DataType::Int64)
        .property_nullable("e", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', a: 10, b: 20, c: 30, d: 40, e: 50})")
        .await?;
    tx.commit().await?;

    let vid: i64 = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid")
        .await?
        .rows()[0]
        .get("nid")
        .unwrap();

    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (n:Entity) WHERE id(n) = $v SET n.a = 100, n.c = 300")
        .param("v", Value::Int(vid))
        .run()
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.a AS a, n.b AS b, n.c AS c, n.d AS d, n.e AS e")
        .await?;
    let row = &r.rows()[0];
    assert_eq!(row.get::<i64>("a").unwrap(), 100);
    assert_eq!(row.get::<i64>("b").unwrap(), 20, "n.b clobbered");
    assert_eq!(row.get::<i64>("c").unwrap(), 300);
    assert_eq!(row.get::<i64>("d").unwrap(), 40, "n.d clobbered");
    assert_eq!(row.get::<i64>("e").unwrap(), 50, "n.e clobbered");
    Ok(())
}

/// F1c — production-shape SET (vector embedding + HnswSq index in schema).
/// Partial SET on scalar columns must preserve the embedding column.
/// This is the test most analogous to the issue #72 production workload.
#[tokio::test]
async fn f1c_set_preserves_embedding_under_partial_update() -> Result<()> {
    const EMBED_DIM: usize = 8;
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .property_nullable("confidence", DataType::Float64)
        .vector("embedding", EMBED_DIM)
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::HnswSq {
                    m: 8,
                    ef_construction: 32,
                    partitions: None,
                },
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .done()
        .apply()
        .await?;

    let embedding_vec: Vec<Value> = (0..EMBED_DIM)
        .map(|i| Value::Float((i as f64) * 0.1 + 0.05))
        .collect();
    let tx = db.session().tx().await?;
    tx.execute_with(
        "CREATE (:Entity {entity_id: 'e1', frequency: 1, confidence: 0.5, embedding: $emb})",
    )
    .param("emb", Value::List(embedding_vec.clone()))
    .run()
    .await?;
    tx.commit().await?;

    let vid: i64 = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid")
        .await?
        .rows()[0]
        .get("nid")
        .unwrap();

    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (n:Entity) WHERE id(n) = $v SET n.frequency = 99, n.confidence = 0.95")
        .param("v", Value::Int(vid))
        .run()
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.frequency AS f, n.confidence AS c, n.embedding AS emb")
        .await?;
    let row = &r.rows()[0];
    assert_eq!(row.get::<i64>("f").unwrap(), 99);
    assert!((row.get::<f64>("c").unwrap() - 0.95).abs() < 1e-9);

    // Verify embedding survived. A `VECTOR` column round-trips with type fidelity
    // as `Value::Vector` (not a generic `Value::List` of floats).
    let emb = row.value("emb").expect("emb missing");
    let Value::Vector(raw) = emb else {
        panic!("RETURN of a VECTOR column must yield Value::Vector, got {emb:?}");
    };
    assert_eq!(raw.len(), EMBED_DIM, "embedding clobbered by partial SET");

    // Verify element-wise that the embedding values weren't replaced
    // with defaults (e.g., all-zeros from a null-coalesce path).
    let preserved: Vec<f64> = raw.iter().map(|f| *f as f64).collect();
    assert_eq!(preserved.len(), EMBED_DIM);
    for (i, got) in preserved.iter().enumerate() {
        let want = (i as f64) * 0.1 + 0.05;
        assert!(
            (got - want).abs() < 1e-4,
            "embedding[{i}] changed by partial SET (got {got}, want {want})"
        );
    }
    // Drop vid to silence unused-binding warning.
    let _ = vid;
    Ok(())
}

/// F1d — SET on an edge round-trips. The Round 4 Fix-3 regression manifested
/// on edges first (`test_edge_created_updated_at`). This test would catch it.
#[tokio::test]
async fn f1d_set_edge_property_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("nid", DataType::String)
        .done()
        .edge_type("REL", &["Node"], &["Node"])
        .property_nullable("since", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Node {nid: 'a'})-[:REL {since: 2020}]->(b:Node {nid: 'b'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Node {nid: 'a'})-[r:REL]->(b:Node {nid: 'b'}) SET r.since = 2025")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:REL]->() RETURN r.since AS s")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("s").unwrap(),
        2025,
        "edge SET did not apply"
    );
    Ok(())
}

/// F1e — SET on a TRAVERSE-bound target (audit item B.7). The variable being
/// SET comes through `plan_traverse`, not a direct scan. This is the highest-
/// risk path for the sentinel refactor because target structural projection
/// is conditional.
#[tokio::test]
async fn f1e_set_traverse_bound_target_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("nid", DataType::String)
        .property_nullable("score", DataType::Int64)
        .done()
        .edge_type("REL", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Node {nid: 'a', score: 1})-[:REL]->(b:Node {nid: 'b', score: 2})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Node {nid: 'a'})-[:REL]->(b:Node) SET b.score = 99")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Node {nid: 'b'}) RETURN n.score AS s")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("s").unwrap(),
        99,
        "SET on traverse-bound b did not apply"
    );

    let r2 = db
        .session()
        .query("MATCH (n:Node {nid: 'a'}) RETURN n.score AS s")
        .await?;
    assert_eq!(
        r2.rows()[0].get::<i64>("s").unwrap(),
        1,
        "untouched source a was clobbered"
    );
    Ok(())
}

/// F3 — mixed Property + Labels in a single SET. Variable's HashSet will be
/// `{sentinel, "*"}`; "*" must dominate so labels variant gets full record.
#[tokio::test]
async fn f3_set_mixed_property_and_label() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .label("Extra")
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.x = 99, n:Extra").await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, labels(n) AS lbls")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 99);
    let lbls = r.rows()[0].value("lbls").expect("labels missing");
    match lbls {
        Value::List(l) => {
            let names: Vec<String> = l
                .iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            assert!(
                names.contains(&"Entity".to_string()),
                "Entity label missing"
            );
            assert!(
                names.contains(&"Extra".to_string()),
                "Extra label not added"
            );
        }
        other => panic!("unexpected labels shape: {other:?}"),
    }
    Ok(())
}

/// F4a — `SET n.x = 1, n = {y: 2}`. Property + Variable-replace. "*" must
/// dominate the sentinel; the final result is the map (n.x absent).
#[tokio::test]
async fn f4a_set_property_then_variable_replace() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 0, y: 0})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.x = 7, n = {entity_id: 'e1', y: 99}")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    // n = {...} REPLACES properties; n.x must now be null/absent.
    assert!(
        r.rows()[0]
            .value("x")
            .is_none_or(|v| matches!(v, Value::Null))
    );
    assert_eq!(r.rows()[0].get::<i64>("y").unwrap(), 99);
    Ok(())
}

/// F4b — `SET n.x = 1, n += {y: 2}`. Property + Variable-merge. "*" dominates;
/// both keys end up present.
#[tokio::test]
async fn f4b_set_property_then_variable_merge() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 0, y: 0})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.x = 7, n += {y: 99}")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 7);
    assert_eq!(r.rows()[0].get::<i64>("y").unwrap(), 99);
    Ok(())
}

/// F5a — `SET n.x = 1 RETURN n`. RETURN's bare-variable "*" must dominate
/// the SET's sentinel and pull the full record so the returned `n` is
/// complete.
#[tokio::test]
async fn f5a_set_then_return_bare_node() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1, y: 2})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx.query("MATCH (n:Entity) SET n.x = 99 RETURN n").await?;
    tx.commit().await?;
    assert_eq!(res.rows().len(), 1);
    // The returned `n` should be a Map/Node containing both x (updated)
    // and y (preserved).
    let n = res.rows()[0].value("n").expect("n missing");
    // Returned value may be Node (preferred) or Map (legacy). Both must
    // contain the full property set including the SET'd x and the
    // untouched y.
    let props: &std::collections::HashMap<String, Value> = match n {
        Value::Map(m) => m,
        Value::Node(node) => &node.properties,
        other => panic!("expected node, got {other:?}"),
    };
    assert_eq!(props.get("x"), Some(&Value::Int(99)));
    assert_eq!(props.get("y"), Some(&Value::Int(2)));
    Ok(())
}

/// F5b — `SET n.x = 1 RETURN n.x, n.y`. Explicit dotted-prop RETURN does not
/// trigger "*"; the sentinel-only path runs. Both props must still be
/// available.
#[tokio::test]
async fn f5b_set_then_return_explicit_props() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1, y: 2})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query("MATCH (n:Entity) SET n.x = 99 RETURN n.x AS x, n.y AS y")
        .await?;
    tx.commit().await?;
    assert_eq!(res.rows()[0].get::<i64>("x").unwrap(), 99);
    assert_eq!(res.rows()[0].get::<i64>("y").unwrap(), 2);
    Ok(())
}

/// F6a — `MERGE ... ON MATCH SET`. SetItem propagation through
/// `LogicalPlan::Merge`'s on_match branch.
#[tokio::test]
async fn f6a_merge_on_match_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("key", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {key: 'k1', x: 1})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute_with("MERGE (n:Entity {key: $k}) ON MATCH SET n.x = 99")
        .param("k", Value::String("k1".to_string()))
        .run()
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity {key: 'k1'}) RETURN n.x AS x")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 99);
    Ok(())
}

/// F6b — `MERGE ... ON CREATE SET`. SetItem propagation through on_create.
#[tokio::test]
async fn f6b_merge_on_create_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("key", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute_with("MERGE (n:Entity {key: $k}) ON CREATE SET n.x = 77")
        .param("k", Value::String("new_k".to_string()))
        .run()
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity {key: 'new_k'}) RETURN n.x AS x")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 77);
    Ok(())
}

/// F9 — SET on a multi-label vertex `(n:A:B)`. Goes through
/// `plan_multi_label_scan` → `resolve_schemaless_properties`.
#[tokio::test]
async fn f9_set_multi_label_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("aid", DataType::String)
        .property_nullable("a_prop", DataType::Int64)
        .done()
        .label("B")
        .property_nullable("b_prop", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A:B {aid: 'x', a_prop: 0, b_prop: 0})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:A:B) SET n.a_prop = 11, n.b_prop = 22")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:A:B) RETURN n.a_prop AS a, n.b_prop AS b")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("a").unwrap(), 11);
    assert_eq!(r.rows()[0].get::<i64>("b").unwrap(), 22);
    Ok(())
}

/// F11 — SET against an empty match. No rows, no panic, no corruption.
#[tokio::test]
async fn f11_set_empty_match_no_panic() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .execute_with("MATCH (n:Entity) WHERE id(n) = $v SET n.x = 999")
        .param("v", Value::Int(99_999_999))
        .run()
        .await?;
    tx.commit().await?;
    assert_eq!(
        res.affected_rows(),
        0,
        "expected zero rows for non-matching id()"
    );

    // Sanity: original entity untouched.
    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 1);
    Ok(())
}

/// F2 — SET RHS reads input row state, NOT intra-statement modifications.
///
/// This documents standard openCypher semantics: every SetItem in a single
/// SET clause is evaluated against the row as it entered the SET, not
/// against the post-write state of preceding SetItems. The Round 3 Fix 2
/// coalescing did NOT change this — earlier writes are accumulated and
/// flushed together, but RHS evaluation continues to read input values.
///
/// Catches regressions in either direction: if a future refactor made
/// intra-statement reads "see" earlier writes, this test would fail. If a
/// regression broke RHS evaluation of cross-property references entirely
/// (`n.y + 1` returning null), this would also fail.
#[tokio::test]
async fn f2_set_rhs_reads_input_row_not_intra_statement_writes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .property_nullable("z", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 10, y: 5, z: 0})")
        .await?;
    tx.commit().await?;

    // Input: x=10, y=5, z=0.
    // SET: x = 100, y = n.x + 1, z = n.y * 2
    // RHS of y reads input x (10) → y becomes 11.
    // RHS of z reads input y (5)  → z becomes 10.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.x = 100, n.y = n.x + 1, n.z = n.y * 2")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y, n.z AS z")
        .await?;
    let row = &r.rows()[0];
    assert_eq!(row.get::<i64>("x").unwrap(), 100, "SET n.x did not apply");
    assert_eq!(
        row.get::<i64>("y").unwrap(),
        11,
        "SET n.y RHS should evaluate against input n.x (10), got {}",
        row.get::<i64>("y").unwrap()
    );
    assert_eq!(
        row.get::<i64>("z").unwrap(),
        10,
        "SET n.z RHS should evaluate against input n.y (5), got {}",
        row.get::<i64>("z").unwrap()
    );
    Ok(())
}

/// F-overflow — SET an overflow property (not declared in schema).
/// Schema is partially declared: `entity_id` is in schema, `extra_prop` is
/// not. The L0/Lance path stores overflow properties in a JSON blob.
/// Sentinel skips `_all_props`/`overflow_json` injection on the scan, but
/// the executor's `get_all_vertex_props_with_ctx` should still route the
/// write correctly through property_manager.
#[tokio::test]
async fn f_overflow_set_undeclared_property_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', initial_overflow: 1})")
        .await?;
    tx.commit().await?;

    // SET a NEW overflow property (not in schema, not previously set).
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.new_overflow = 42")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.new_overflow AS no, n.initial_overflow AS io")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("no").unwrap(),
        42,
        "SET on undeclared overflow property did not apply"
    );
    assert_eq!(
        r.rows()[0].get::<i64>("io").unwrap(),
        1,
        "initial overflow property was clobbered"
    );
    Ok(())
}

/// F10 — SET on a fully schemaless label (no properties declared in schema).
/// Exercises `resolve_schemaless_properties` which we changed in Part B.
#[tokio::test]
async fn f10_set_schemaless_label_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").apply().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30, city: 'NYC'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (p:Person) SET p.age = 31, p.city = 'SF'")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.city AS city")
        .await?;
    let row = &r.rows()[0];
    assert_eq!(
        row.get::<String>("name").unwrap(),
        "Alice",
        "name clobbered"
    );
    assert_eq!(row.get::<i64>("age").unwrap(), 31);
    assert_eq!(row.get::<String>("city").unwrap(), "SF");
    Ok(())
}

/// F-id-rhs — `SET n.tracker = id(n)`. The `id()` function lowers to a
/// reference to `_vid`. With the sentinel narrowing scan projection, _vid
/// must still flow through so the RHS evaluates correctly.
#[tokio::test]
async fn f_id_in_rhs_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("tracker", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1'})").await?;
    tx.commit().await?;

    let vid: i64 = db
        .session()
        .query("MATCH (n:Entity) RETURN id(n) AS nid")
        .await?
        .rows()[0]
        .get("nid")
        .unwrap();

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.tracker = id(n)").await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.tracker AS t")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("t").unwrap(),
        vid,
        "SET n.tracker = id(n) did not capture the vid"
    );
    Ok(())
}

/// F-computed-rhs — `SET n.x = n.y + 1` where `y` is a schema property the
/// SET clause does not write to. The explicit `n.y` reference must be
/// collected into the scan projection alongside the sentinel, so the RHS
/// sees the actual stored value.
#[tokio::test]
async fn f_computed_rhs_reads_unrelated_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 0, y: 41})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.x = n.y + 1").await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        42,
        "RHS `n.y + 1` did not evaluate against the stored y"
    );
    assert_eq!(
        r.rows()[0].get::<i64>("y").unwrap(),
        41,
        "n.y was clobbered"
    );
    Ok(())
}

/// F13 — combined property-existence filter and SET. WHERE collects an
/// explicit property; SET adds the sentinel. Union of both must work.
#[tokio::test]
async fn f13_set_with_property_existence_filter() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("optional_prop", DataType::Int64)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'has', optional_prop: 1, x: 0})")
        .await?;
    tx.execute("CREATE (:Entity {entity_id: 'lacks', x: 0})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) WHERE n.optional_prop IS NOT NULL SET n.x = 42")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.entity_id AS id, n.x AS x ORDER BY id")
        .await?;
    let by_id: std::collections::HashMap<String, i64> = r
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("id").unwrap(),
                row.get::<i64>("x").unwrap(),
            )
        })
        .collect();
    assert_eq!(by_id["has"], 42, "SET on filtered-in row did not apply");
    assert_eq!(by_id["lacks"], 0, "SET applied to filtered-out row");
    Ok(())
}

// =============================================================================
// Round 6 — HIGH tier
// =============================================================================

/// H1a — SET → commit → flush → re-query (same session, persistent db).
/// Confirms L0 → Lance flush preserves a sentinel-narrowed write.
#[tokio::test]
async fn h1a_set_then_flush_round_trips() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Uni::open(temp_dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1, y: 2})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.x = 99").await?;
    tx.commit().await?;

    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        99,
        "SET lost across flush"
    );
    assert_eq!(
        r.rows()[0].get::<i64>("y").unwrap(),
        2,
        "untouched y lost across flush"
    );
    Ok(())
}

/// H1b — SET → commit → flush → close → reopen → query.
/// Confirms the write survives full persistence cycle (Lance MVCC + L1 read).
#[tokio::test]
async fn h1b_set_then_flush_then_reopen_round_trips() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Entity")
            .property("entity_id", DataType::String)
            .property_nullable("x", DataType::Int64)
            .property_nullable("y", DataType::Int64)
            .done()
            .apply()
            .await?;

        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1, y: 2})")
            .await?;
        tx.commit().await?;

        let tx = db.session().tx().await?;
        tx.execute("MATCH (n:Entity) SET n.x = 99").await?;
        tx.commit().await?;
        db.flush().await?;
    } // db dropped, ensures persistence

    let db = Uni::open(&path).build().await?;
    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(r.len(), 1, "vertex lost across reopen");
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        99,
        "SET lost across reopen"
    );
    assert_eq!(
        r.rows()[0].get::<i64>("y").unwrap(),
        2,
        "untouched y lost across reopen"
    );
    Ok(())
}

/// H2 — SET combined with hash-indexed-property pushdown predicate.
/// Pushdown injects extra physical filters BEFORE structural projection;
/// must not break sentinel-narrowed scan.
#[tokio::test]
async fn h2_set_with_indexed_property_pushdown_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property("email", DataType::String)
        .index("email", IndexType::Scalar(ScalarType::Hash))
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..50 {
        tx.execute_with("CREATE (:Entity {entity_id: $id, email: $em, x: 0})")
            .param("id", format!("e:{i}"))
            .param("em", format!("u{i}@example.com"))
            .run()
            .await?;
    }
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (n:Entity) WHERE n.email = $em SET n.x = 42")
        .param("em", "u17@example.com")
        .run()
        .await?;
    tx.commit().await?;

    let target = db
        .session()
        .query_with("MATCH (n:Entity) WHERE n.email = $em RETURN n.x AS x")
        .param("em", "u17@example.com")
        .fetch_all()
        .await?;
    assert_eq!(target.len(), 1);
    assert_eq!(
        target.rows()[0].get::<i64>("x").unwrap(),
        42,
        "indexed-pushdown SET did not apply"
    );

    let untouched = db
        .session()
        .query("MATCH (n:Entity) WHERE n.x = 0 RETURN count(n) AS c")
        .await?;
    assert_eq!(
        untouched.rows()[0].get::<i64>("c").unwrap(),
        49,
        "indexed-pushdown SET leaked to other rows"
    );
    Ok(())
}

/// H3a — SET a CypherValue (JSON-blob) property.
#[tokio::test]
async fn h3a_set_cypher_value_property_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property_nullable("metadata", DataType::CypherValue)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (i:Item {name: 'A', metadata: {valid: true, count: 1}})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (i:Item) SET i.metadata = {valid: false, count: 99}")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (i:Item) RETURN i.metadata AS meta, i.name AS name")
        .await?;
    assert_eq!(
        r.rows()[0].get::<String>("name").unwrap(),
        "A",
        "name clobbered"
    );
    let meta = r.rows()[0].value("meta").expect("metadata missing");
    if let Value::Map(m) = meta {
        assert_eq!(m.get("valid"), Some(&Value::Bool(false)));
        assert_eq!(m.get("count"), Some(&Value::Int(99)));
    } else {
        panic!("expected CypherValue Map, got {meta:?}");
    }
    Ok(())
}

/// H3b — SET a Vector property directly (the vector is the SET target).
#[tokio::test]
async fn h3b_set_vector_property_directly_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: 3 })
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (i:Item {name: 'A', embedding: [0.1, 0.2, 0.3]})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (i:Item) SET i.embedding = [0.7, 0.8, 0.9]")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (i:Item) RETURN i.embedding AS emb, i.name AS name")
        .await?;
    assert_eq!(r.rows()[0].get::<String>("name").unwrap(), "A");
    let emb = r.rows()[0].value("emb").expect("embedding missing");
    // A `VECTOR` column must round-trip with type fidelity as `Value::Vector`,
    // not a generic `Value::List` of floats (parity with SparseVector/Btic).
    let Value::Vector(raw) = emb else {
        panic!("RETURN of a VECTOR column must yield Value::Vector, got {emb:?}");
    };
    let v: Vec<f64> = raw.iter().map(|f| *f as f64).collect();
    assert_eq!(v.len(), 3);
    let expected = [0.7, 0.8, 0.9];
    for (i, (got, want)) in v.iter().zip(expected.iter()).enumerate() {
        assert!(
            (got - want).abs() < 1e-4,
            "vector[{i}] not updated: got {got}, want {want}"
        );
    }
    Ok(())
}

/// H4a — SET violates NOT NULL constraint. Expect error; no partial writes.
#[tokio::test]
async fn h4a_set_violates_not_null_rolls_back() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (entity_id STRING NOT NULL, x INT NOT NULL, y INT)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1, y: 2})")
        .await?;
    tx.commit().await?;

    // Attempt: set y to 99 AND x to null — should fail; y should remain 2.
    let tx = db.session().tx().await?;
    let res = tx
        .execute("MATCH (n:Entity) SET n.y = 99, n.x = null")
        .await;
    // Either the execute errors, or the commit errors.
    let final_err = if res.is_err() {
        true
    } else {
        tx.commit().await.is_err()
    };
    assert!(final_err, "SET to null on NOT NULL column must error");

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        1,
        "x corrupted on rollback"
    );
    assert_eq!(
        r.rows()[0].get::<i64>("y").unwrap(),
        2,
        "y partially-written despite NOT NULL violation"
    );
    Ok(())
}

/// H4b — SET violates UNIQUE constraint. Expect error; rows unchanged.
#[tokio::test]
async fn h4b_set_violates_unique_rolls_back() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (key STRING NOT NULL UNIQUE, x INT)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {key: 'k1', x: 1})").await?;
    tx.execute("CREATE (:Entity {key: 'k2', x: 2})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .execute("MATCH (n:Entity {key: 'k1'}) SET n.x = 99, n.key = 'k2'")
        .await;
    let final_err = if res.is_err() {
        true
    } else {
        tx.commit().await.is_err()
    };
    assert!(final_err, "SET duplicate UNIQUE key must error");

    let r = db
        .session()
        .query("MATCH (n:Entity {key: 'k1'}) RETURN n.x AS x")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        1,
        "x partially-written despite UNIQUE violation"
    );
    Ok(())
}

/// H4c — SET violates the declared-type guard (complex value into scalar
/// column). Expect error; rows unchanged.
///
/// Since issue #68, `coerce_and_validate_property_value` checks every written
/// value against the column's declared `DataType` at SET/CREATE time: a Map /
/// Node / Edge / Path (or nested-List) into a scalar column is rejected, as is
/// a genuine primitive mismatch (e.g. String → Int). A `Value::String` into a
/// temporal column is the one coercion (parsed like `datetime()`); everything
/// else that can't be stored losslessly errors at the call site rather than
/// being silently nulled at Lance-flush time.
#[tokio::test]
async fn h4c_set_violates_complex_value_into_scalar_rolls_back() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (entity_id STRING NOT NULL, count INT, name STRING)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', count: 10, name: 'before'})")
        .await?;
    tx.commit().await?;

    // Assigning a Map to a scalar INT column trips the declared-type guard.
    let tx = db.session().tx().await?;
    let res = tx
        .execute("MATCH (n:Entity) SET n.name = 'after', n.count = {nested: 1}")
        .await;
    let final_err = if res.is_err() {
        true
    } else {
        tx.commit().await.is_err()
    };
    assert!(final_err, "SET Map into scalar column must error");

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.count AS c, n.name AS n")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("c").unwrap(), 10, "count corrupted");
    assert_eq!(
        r.rows()[0].get::<String>("n").unwrap(),
        "before",
        "name partially-written despite complex-value violation"
    );
    Ok(())
}

// =============================================================================
// Round 6 — MEDIUM tier
// =============================================================================

/// M1 — Same SET query executed twice in the same session should be a
/// plan-cache hit on the second call and still apply correctly.
#[tokio::test]
async fn m1_set_plan_cache_reuse_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'a', x: 0})")
        .await?;
    tx.execute("CREATE (:Entity {entity_id: 'b', x: 0})")
        .await?;
    tx.commit().await?;

    let session = db.session();
    // First call — plan compiled cold.
    let tx = session.tx().await?;
    tx.execute_with("MATCH (n:Entity {entity_id: $id}) SET n.x = $v")
        .param("id", "a")
        .param("v", 11i64)
        .run()
        .await?;
    tx.commit().await?;

    // Second call — same query string, different params. Should hit cache.
    let tx = session.tx().await?;
    tx.execute_with("MATCH (n:Entity {entity_id: $id}) SET n.x = $v")
        .param("id", "b")
        .param("v", 22i64)
        .run()
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.entity_id AS id, n.x AS x ORDER BY id")
        .await?;
    let by_id: std::collections::HashMap<String, i64> = r
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("id").unwrap(),
                row.get::<i64>("x").unwrap(),
            )
        })
        .collect();
    assert_eq!(by_id["a"], 11, "first SET (cold plan) did not apply");
    assert_eq!(by_id["b"], 22, "second SET (cache hit) did not apply");
    Ok(())
}

/// M2 — `SET n.x = 1 REMOVE n.y` in same statement. REMOVE inserts "*" so
/// the SET's sentinel is dominated; full schema flows. Both writes apply.
#[tokio::test]
async fn m2_set_and_remove_same_statement_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1, y: 2})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) SET n.x = 42 REMOVE n.y")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        42,
        "SET did not apply"
    );
    assert!(
        r.rows()[0]
            .value("y")
            .is_none_or(|v| matches!(v, Value::Null)),
        "REMOVE did not clear y"
    );
    Ok(())
}

/// M3 — OPTIONAL MATCH with no rows feeding SET — must not panic, must
/// not write anything.
#[tokio::test]
async fn m3_optional_match_null_target_set_no_panic() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .label("Missing")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0})").await?;
    tx.commit().await?;

    // OPTIONAL MATCH on a label with zero rows; SET on the null binding
    // should no-op gracefully.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (e:E) OPTIONAL MATCH (m:Missing) SET m.x = 99")
        .await?;
    tx.commit().await?;

    let r = db.session().query("MATCH (e:E) RETURN e.x AS x").await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        0,
        "OPTIONAL SET clobbered e"
    );

    let r2 = db
        .session()
        .query("MATCH (m:Missing) RETURN count(m) AS c")
        .await?;
    assert_eq!(
        r2.rows()[0].get::<i64>("c").unwrap(),
        0,
        "OPTIONAL SET phantom-created a Missing"
    );
    Ok(())
}

/// M4 — `MATCH (n:E) WITH n AS m SET m.x = 1`. Variable rebound through
/// WITH; SET-target name `m` differs from original `n`. The structural
/// projection must follow the rebinding.
#[tokio::test]
async fn m4_with_rebinding_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'e1', x: 1, y: 2})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) WITH n AS m SET m.x = 99")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        99,
        "rebound SET did not apply"
    );
    assert_eq!(
        r.rows()[0].get::<i64>("y").unwrap(),
        2,
        "untouched y clobbered"
    );
    Ok(())
}

/// M5 — Aggregation followed by SET: `MATCH (n) WITH n, count(n) AS c SET n.cnt = c`.
/// After aggregation, `n` survives as a node-bound variable; SET must
/// still find the structural Map.
#[tokio::test]
async fn m5_aggregation_then_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property_nullable("cnt", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {entity_id: 'a'})").await?;
    tx.execute("CREATE (:Entity {entity_id: 'b'})").await?;
    tx.commit().await?;

    // count(n) per-row over a 1-row GROUP BY (each n is unique) → c=1 per vid.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity) WITH n, count(n) AS c SET n.cnt = c")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.cnt AS cnt ORDER BY n.entity_id")
        .await?;
    assert_eq!(r.len(), 2);
    for row in r.rows() {
        assert_eq!(
            row.get::<i64>("cnt").unwrap(),
            1,
            "aggregation-then-SET did not apply"
        );
    }
    Ok(())
}

// =============================================================================
// Round 6 — LOW tier
// =============================================================================

/// L1 — UNWIND a list of vid params, then SET. The iterator variable `u`
/// is row-bound by UNWIND, not by scan; the actual SET target is bound
/// by MATCH inside the loop. This is the supported analogue of the
/// (unimplemented in uni) Cypher FOREACH clause. Same shape used by the
/// production issue #72 workload, so we know it works — this test locks
/// it in explicitly.
#[tokio::test]
async fn l1_unwind_then_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("p", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'a', p: 0})").await?;
    tx.execute("CREATE (:E {id: 'b', p: 0})").await?;
    tx.execute("CREATE (:E {id: 'c', p: 0})").await?;
    tx.commit().await?;

    let vids: Vec<i64> = db
        .session()
        .query("MATCH (n:E) RETURN id(n) AS nid ORDER BY n.id")
        .await?
        .into_iter()
        .map(|r| r.get::<i64>("nid").unwrap())
        .collect();

    let tx = db.session().tx().await?;
    tx.execute_with("UNWIND $vids AS v MATCH (n:E) WHERE id(n) = v SET n.p = 99")
        .param(
            "vids",
            Value::List(vids.iter().map(|v| Value::Int(*v)).collect()),
        )
        .run()
        .await?;
    tx.commit().await?;

    let r = db.session().query("MATCH (n:E) RETURN n.p AS p").await?;
    assert_eq!(r.len(), 3);
    for row in r.rows() {
        assert_eq!(
            row.get::<i64>("p").unwrap(),
            99,
            "UNWIND-driven SET did not apply"
        );
    }
    Ok(())
}

/// L2 — `SET` inside `CALL { ... }` subquery round-trips. Threading the
/// outer MutationContext into the per-row sub-planner enables writes
/// inside `Apply` / `SubqueryCall` plan nodes.
#[tokio::test]
async fn l2_call_subquery_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E) CALL { WITH n SET n.x = 77 } RETURN n.x")
        .await?;
    tx.commit().await?;

    let r = db.session().query("MATCH (n:E) RETURN n.x AS x").await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        77,
        "CALL subquery SET did not apply"
    );
    Ok(())
}

/// L2b — `CREATE` inside CALL subquery creates the new vertex.
#[tokio::test]
async fn l2b_call_subquery_create_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'seed'})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query("MATCH (n:E {id: 'seed'}) CALL { CREATE (m:E {id: 'fresh'}) RETURN m } RETURN m.id AS mid")
        .await?;
    tx.commit().await?;
    assert_eq!(
        res.rows()[0].get::<String>("mid").unwrap(),
        "fresh",
        "subquery RETURN m did not project the newly-created node"
    );

    let r = db
        .session()
        .query("MATCH (n:E) RETURN n.id AS id ORDER BY id")
        .await?;
    let ids: Vec<String> = r
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").unwrap())
        .collect();
    assert_eq!(ids, vec!["fresh".to_string(), "seed".to_string()]);
    Ok(())
}

/// L2c — `MERGE ... ON CREATE SET` inside CALL subquery on a non-existing key.
#[tokio::test]
async fn l2c_call_subquery_merge_on_create_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("fresh", DataType::Bool)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'anchor'})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'anchor'}) \
             CALL { MERGE (m:E {id: 'new_via_merge'}) ON CREATE SET m.fresh = true RETURN m } \
             RETURN m.fresh AS fresh",
        )
        .await?;
    tx.commit().await?;
    let inner = res.rows()[0].value("fresh").expect("inner fresh missing");
    assert!(
        matches!(inner, Value::Bool(true)),
        "subquery RETURN m did not surface ON CREATE SET in outer projection"
    );

    let r = db
        .session()
        .query("MATCH (m:E {id: 'new_via_merge'}) RETURN m.fresh AS fresh")
        .await?;
    assert_eq!(r.len(), 1);
    let fresh = r.rows()[0].value("fresh").expect("fresh missing");
    assert!(
        matches!(fresh, Value::Bool(true)),
        "ON CREATE SET did not fire"
    );
    Ok(())
}

/// L2c2 — `MERGE ... ON MATCH SET` inside CALL subquery on an EXISTING key.
#[tokio::test]
async fn l2c2_call_subquery_merge_on_match_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("hits", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'anchor'})").await?;
    tx.execute("CREATE (:E {id: 'existing', hits: 1})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'anchor'}) \
             CALL { MERGE (m:E {id: 'existing'}) ON MATCH SET m.hits = 99 RETURN m } \
             RETURN m.hits AS hits",
        )
        .await?;
    tx.commit().await?;
    assert_eq!(
        res.rows()[0].get::<i64>("hits").unwrap(),
        99,
        "subquery RETURN m did not surface ON MATCH SET in outer projection"
    );

    let r = db
        .session()
        .query("MATCH (m:E {id: 'existing'}) RETURN m.hits AS hits")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("hits").unwrap(),
        99,
        "ON MATCH SET did not fire"
    );
    Ok(())
}

/// L2d — `DELETE` inside CALL subquery removes the vertex.
#[tokio::test]
async fn l2d_call_subquery_delete_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'keep'})").await?;
    tx.execute("CREATE (:E {id: 'doomed'})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'doomed'}) CALL { WITH n DELETE n } RETURN 1 AS dummy")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:E) RETURN count(n) AS c")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("c").unwrap(),
        1,
        "DELETE in subquery did not remove vertex"
    );
    let r2 = db.session().query("MATCH (n:E) RETURN n.id AS id").await?;
    assert_eq!(r2.rows()[0].get::<String>("id").unwrap(), "keep");
    Ok(())
}

/// L2e — nested `CALL { CALL { ... SET ... } }` — verifies mutation_ctx
/// propagates through nested GraphApplyExec instances.
#[tokio::test]
async fn l2e_call_subquery_nested_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E) CALL { WITH n CALL { WITH n SET n.x = 42 } } RETURN n.x")
        .await?;
    tx.commit().await?;

    let r = db.session().query("MATCH (n:E) RETURN n.x AS x").await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        42,
        "nested CALL SET did not apply"
    );
    Ok(())
}

/// L2f — edge SET inside CALL subquery.
#[tokio::test]
async fn l2f_call_subquery_edge_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("R", &["N"], &["N"])
        .property_nullable("flag", DataType::Bool)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 'a'})-[:R]->(b:N {id: 'b'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:N)-[r:R]->(b:N) CALL { WITH r SET r.flag = true } RETURN a.id")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:R]->() RETURN r.flag AS f")
        .await?;
    let f = r.rows()[0].value("f").expect("flag missing");
    assert!(
        matches!(f, Value::Bool(true)),
        "edge SET in subquery did not apply"
    );
    Ok(())
}

/// L2g — RHS in subquery reads a correlated outer variable's property.
#[tokio::test]
async fn l2g_call_subquery_correlated_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("source", DataType::Int64)
        .property_nullable("copy", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', source: 42, copy: 0})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E) CALL { WITH n SET n.copy = n.source } RETURN n.id")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:E) RETURN n.source AS s, n.copy AS c")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("s").unwrap(), 42);
    assert_eq!(
        r.rows()[0].get::<i64>("c").unwrap(),
        42,
        "correlated subquery RHS did not read outer-bound n.source"
    );
    Ok(())
}

/// L2h — multi-input CALL subquery: outer MATCH yields N rows; subquery
/// fires N times; all writes land.
#[tokio::test]
async fn l2h_call_subquery_multi_input_writes_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("touched", DataType::Bool)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute_with("CREATE (:E {id: $id, touched: false})")
            .param("id", format!("e{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E) CALL { WITH n SET n.touched = true } RETURN n.id")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:E) WHERE n.touched = true RETURN count(n) AS c")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("c").unwrap(),
        5,
        "expected 5 touched rows from multi-input subquery"
    );
    Ok(())
}

/// L2i — subquery contains BOTH read (MATCH traverse) and write (SET).
#[tokio::test]
async fn l2i_call_subquery_mixed_read_write_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .property_nullable("touched", DataType::Bool)
        .done()
        .edge_type("LINK", &["N"], &["N"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 'a'})-[:LINK]->(b:N {id: 'b'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (n:N {id: 'a'}) \
         CALL { WITH n MATCH (n)-[:LINK]->(m) SET m.touched = true } \
         RETURN n.id",
    )
    .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (m:N {id: 'b'}) RETURN m.touched AS t")
        .await?;
    let t = r.rows()[0].value("t").expect("touched missing");
    assert!(
        matches!(t, Value::Bool(true)),
        "mixed read+write subquery did not apply"
    );

    let r2 = db
        .session()
        .query("MATCH (a:N {id: 'a'}) RETURN a.touched AS t")
        .await?;
    let t2 = r2.rows()[0].value("t");
    assert!(
        t2.is_none() || matches!(t2.unwrap(), Value::Null | Value::Bool(false)),
        "outer-only n was touched by mistake"
    );
    Ok(())
}

/// L2j — empty input on a correlated CALL subquery: no phantom writes.
///
/// uni's `run_apply` invokes the empty-input branch even for correlated
/// subqueries; the `WITH n` then errors because `n` is unbound. Either
/// outcome is acceptable for this test: the assertion is that no
/// phantom write lands on the seed row.
#[tokio::test]
async fn l2j_call_subquery_empty_input_no_phantom_writes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'kept', x: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    // Outer WHERE excludes all rows; subquery should not run successfully.
    // Whether this errors or no-ops, the kept vertex must remain at x=0.
    let res = tx
        .execute_with("MATCH (n:E) WHERE id(n) = $v CALL { WITH n SET n.x = 999 } RETURN n.id")
        .param("v", Value::Int(99_999_999))
        .run()
        .await;
    if res.is_ok() {
        // If exec succeeded, commit; otherwise drop the tx.
        let _ = tx.commit().await;
    } else {
        drop(tx);
    }

    let r = db.session().query("MATCH (n:E) RETURN n.x AS x").await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        0,
        "phantom write from empty-input CALL subquery"
    );
    Ok(())
}

/// L2k — constraint violation inside the subquery errors; outer state is
/// unchanged.
#[tokio::test]
async fn l2k_call_subquery_constraint_violation_rolls_back() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (key STRING NOT NULL UNIQUE, marker INT)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {key: 'a', marker: 1})").await?;
    tx.execute("CREATE (:E {key: 'b', marker: 2})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .execute(
            "MATCH (n:E {key: 'a'}) CALL { WITH n SET n.marker = 99, n.key = 'b' } RETURN n.key",
        )
        .await;
    let err = if res.is_err() {
        true
    } else {
        tx.commit().await.is_err()
    };
    assert!(
        err,
        "expected UNIQUE constraint violation inside CALL subquery to error"
    );

    let r = db
        .session()
        .query("MATCH (n:E {key: 'a'}) RETURN n.marker AS m")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("m").unwrap(),
        1,
        "outer row partially-written despite UNIQUE violation in subquery"
    );
    Ok(())
}

/// L2m — write inside CALL subquery is visible to a subsequent read in
/// the same transaction (cross-statement visibility within tx).
#[tokio::test]
async fn l2m_call_subquery_write_visible_to_later_read_in_tx() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E) CALL { WITH n SET n.x = 88 } RETURN n.id")
        .await?;
    // Same transaction: read back via tx.query.
    let r = tx.query("MATCH (n:E) RETURN n.x AS x").await?;
    tx.commit().await?;

    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        88,
        "later read in same tx did not see inner subquery SET"
    );
    Ok(())
}

/// L2o — `CALL { CREATE (m:E {x: 99}) RETURN m } RETURN m.x` projects a
/// freshly-bound node through the subquery's RETURN. This exercises the
/// per-row LargeBinary encoding path in `value_to_single_row_array` (the
/// column carrying the bare-node Value through cross-join) end-to-end —
/// the schema-merge bug used to fail this with "expected LargeBinary but
/// found Utf8".
#[tokio::test]
async fn l2o_call_subquery_return_created_node_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'seed'})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'seed'}) \
             CALL { CREATE (m:E {id: 'fresh', x: 99}) RETURN m } \
             RETURN m.x AS x",
        )
        .await?;
    tx.commit().await?;

    assert_eq!(
        res.rows()[0].get::<i64>("x").unwrap(),
        99,
        "outer projection over subquery's RETURN m did not surface created node's x"
    );
    Ok(())
}

/// L2q — `CALL { WITH n SET n.x = 99 RETURN n }` then outer `RETURN n.x`
/// must surface the post-SET value. Round 9 regression: previously the
/// Apply schema merge kept the outer (pre-SET) `n` column and dropped the
/// subquery's RETURN `n`, so outer projections saw stale data.
#[tokio::test]
async fn l2q_call_subquery_set_then_return_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'e1'}) \
             CALL { WITH n SET n.x = 99 RETURN n } \
             RETURN n.x AS x",
        )
        .await?;
    tx.commit().await?;

    assert_eq!(
        res.rows()[0].get::<i64>("x").unwrap(),
        99,
        "outer RETURN n.x surfaced pre-SET row binding"
    );
    Ok(())
}

/// L2r — coalesced two-property SET inside a CALL subquery, surfaced via
/// outer dotted projections. Exercises Round-3 coalescing + Round-9
/// Apply-boundary override in concert.
#[tokio::test]
async fn l2r_call_subquery_set_two_props_then_return() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0, y: 'old'})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'e1'}) \
             CALL { WITH n SET n.x = 7, n.y = 'fresh' RETURN n } \
             RETURN n.x AS x, n.y AS y",
        )
        .await?;
    tx.commit().await?;

    let row = &res.rows()[0];
    assert_eq!(row.get::<i64>("x").unwrap(), 7);
    assert_eq!(row.get::<String>("y").unwrap(), "fresh");
    Ok(())
}

/// L2s — edge SET inside a CALL subquery, surfaced via outer projection.
#[tokio::test]
async fn l2s_call_subquery_set_edge_then_return() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("LINKS", &["N"], &["N"])
        .property_nullable("flag", DataType::Bool)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 'a'})-[:LINKS {flag: false}]->(b:N {id: 'b'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (a:N)-[r:LINKS]->(b:N) \
             CALL { WITH r SET r.flag = true RETURN r } \
             RETURN r.flag AS flag",
        )
        .await?;
    tx.commit().await?;

    let val = res.rows()[0].value("flag").unwrap();
    assert!(
        matches!(val, Value::Bool(true)),
        "outer RETURN r.flag did not surface post-SET edge value: {val:?}"
    );
    Ok(())
}

/// L2t — `CALL { WITH n SET n.x = 5 }` with NO inner RETURN, then outer
/// `RETURN n.x`. Per openCypher "unit subquery" semantics, a CALL without
/// RETURN passes input rows through unchanged. The Round-10 fix detects
/// the zero-field subquery output schema in `GraphApplyExec` and emits
/// the input row regardless of subquery row count, while side effects
/// (the SET) flush to L0.
///
/// **Two assertions:** (1) writes land (verified via a separate session
/// query post-commit) and (2) the input row passes through (the inline
/// query returns one row). The inline `n.x` value is unspecified by this
/// test — the unit-subquery passthrough emits the pre-SET row binding
/// since there is no inner RETURN to re-emit `n`; refreshing the dotted
/// column would require re-reading from L0 inside `GraphApplyExec`,
/// which is a larger follow-up (see l2t_inline_stale_binding).
#[tokio::test]
async fn l2t_call_subquery_set_no_inner_return_outer_sees_writes() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'e1'}) \
             CALL { WITH n SET n.x = 5 } \
             RETURN n.id AS id",
        )
        .await?;
    tx.commit().await?;

    assert_eq!(res.rows().len(), 1, "unit subquery dropped the outer row");
    assert_eq!(res.rows()[0].get::<String>("id").unwrap(), "e1");

    // Writes must land regardless: a fresh session query sees the SET.
    let r = db
        .session()
        .query("MATCH (n:E {id: 'e1'}) RETURN n.x AS x")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        5,
        "SET in unit subquery did not flush to L0"
    );
    Ok(())
}

/// L2t2 — inline RETURN after unit-subquery SET must surface post-SET
/// values via the per-row sub_row refresh in `append_cross_join_row`
/// (unit-subquery branch).
#[tokio::test]
async fn l2t2_call_subquery_unit_inline_return_post_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'e1'}) \
             CALL { WITH n SET n.x = 5 } \
             RETURN n.x AS x",
        )
        .await?;
    tx.commit().await?;

    assert_eq!(
        res.rows()[0].get::<i64>("x").unwrap(),
        5,
        "inline RETURN after unit-subquery SET surfaced stale row binding"
    );
    Ok(())
}

/// L2u — multi-row unit-subquery: each input row should produce exactly one
/// output row and the SET must apply to all of them. Exercises the batched
/// hash-join code path (≥2 rows + correlated VID).
#[tokio::test]
async fn l2u_unit_subquery_multi_row_passthrough() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute_with("CREATE (:E {id: $id, x: 0})")
            .param("id", Value::String(format!("e{i}")))
            .run()
            .await?;
    }
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E) \
             CALL { WITH n SET n.x = 42 } \
             RETURN n.id AS id ORDER BY id",
        )
        .await?;
    tx.commit().await?;

    assert_eq!(res.rows().len(), 5, "unit subquery dropped input rows");

    // Verify ALL five writes landed via a fresh session query.
    let r = db
        .session()
        .query("MATCH (n:E) RETURN n.id AS id, n.x AS x ORDER BY id")
        .await?;
    assert_eq!(r.rows().len(), 5);
    for row in r.rows() {
        assert_eq!(row.get::<i64>("x").unwrap(), 42, "SET on row did not land");
    }
    Ok(())
}

/// L2v — unit subquery containing a CREATE inside `CALL { CREATE (m:E) }`
/// (no inner RETURN). The CREATE must run as a side effect, and the outer
/// input row must still pass through.
#[tokio::test]
async fn l2v_unit_subquery_create_side_effect() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'seed'})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'seed'}) \
             CALL { CREATE (:E {id: 'fresh'}) } \
             RETURN n.id AS id",
        )
        .await?;
    tx.commit().await?;
    assert_eq!(res.rows().len(), 1, "unit subquery dropped the outer row");
    assert_eq!(res.rows()[0].get::<String>("id").unwrap(), "seed");

    let r = db
        .session()
        .query("MATCH (n:E) RETURN count(n) AS c")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("c").unwrap(),
        2,
        "CREATE side effect did not land"
    );
    Ok(())
}

/// L2w — empty-input × unit subquery: outer MATCH returns 0 rows, so the
/// subquery must not be invoked and no phantom output rows appear.
#[tokio::test]
async fn l2w_unit_subquery_empty_input() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query(
            "MATCH (n:E {id: 'never'}) \
             CALL { WITH n SET n.x = 1 } \
             RETURN n.id AS id",
        )
        .await?;
    tx.commit().await?;

    assert_eq!(
        res.rows().len(),
        0,
        "unit subquery produced phantom rows on empty input"
    );
    Ok(())
}

/// L2x — non-unit subquery with no matches still drops the row (inner-join
/// semantics preserved). Guard against the unit-subquery fix bleeding into
/// the data-subquery path.
#[tokio::test]
async fn l2x_non_unit_subquery_no_match_drops_input() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .done()
        .edge_type("LINKS", &["E"], &["E"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'lonely'})").await?;
    tx.commit().await?;

    let res = db
        .session()
        .query(
            "MATCH (n:E {id: 'lonely'}) \
             CALL { WITH n MATCH (n)-[:LINKS]->(m) RETURN m } \
             RETURN n.id AS id",
        )
        .await?;
    assert_eq!(
        res.rows().len(),
        0,
        "data subquery with no matches should drop input row (inner-join semantics)"
    );
    Ok(())
}

/// L4 — Vector KNN feeding SET. Use `CALL uni.vector.query` to bind the
/// target, then SET on it.
#[tokio::test]
async fn l4_vector_knn_then_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property_nullable("matched", DataType::Bool)
        .vector("embedding", 3)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'A', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'B', embedding: [0.0, 1.0, 0.0]})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0, 0.0], 1) YIELD node AS d \
         SET d.matched = true",
    )
    .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (d:Doc) RETURN d.title AS t, d.matched AS m ORDER BY t")
        .await?;
    let matched: std::collections::HashMap<String, bool> = r
        .rows()
        .iter()
        .map(|row| {
            let t = row.get::<String>("t").unwrap();
            let m = row
                .value("m")
                .map(|v| matches!(v, Value::Bool(true)))
                .unwrap_or(false);
            (t, m)
        })
        .collect();
    assert!(matched["A"], "KNN-matched doc did not get SET");
    assert!(!matched["B"], "non-KNN-matched doc was clobbered by SET");
    Ok(())
}

/// L6 — bidirectional pattern: `MATCH (a)-[r]-(b) SET r.x = 1`.
#[tokio::test]
async fn l6_bidirectional_pattern_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("E", &["N"], &["N"])
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 'a'})-[:E {x: 0}]->(b:N {id: 'b'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:N {id: 'a'})-[r:E]-(b:N {id: 'b'}) SET r.x = 99")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:E]->() RETURN r.x AS x")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        99,
        "undirected SET did not apply"
    );
    Ok(())
}

/// L7 — `CREATE (n:E {a:1}) SET n.b = 2` in same statement. Variable bound
/// by CREATE, not scan.
#[tokio::test]
async fn l7_create_then_set_same_statement_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("entity_id", DataType::String)
        .property_nullable("a", DataType::Int64)
        .property_nullable("b", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (n:E {entity_id: 'e1', a: 1}) SET n.b = 2")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:E) RETURN n.a AS a, n.b AS b")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("a").unwrap(), 1, "CREATE prop lost");
    assert_eq!(
        r.rows()[0].get::<i64>("b").unwrap(),
        2,
        "CREATE-then-SET did not apply"
    );
    Ok(())
}

/// L8 — Traverse with multiple edge types feeding SET.
#[tokio::test]
async fn l8_multi_edge_type_traverse_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property_nullable("since", DataType::Int64)
        .done()
        .edge_type("FOLLOWS", &["Person"], &["Person"])
        .property_nullable("since", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Person {name: 'a'})-[:KNOWS {since: 2010}]->(b:Person {name: 'b'})")
        .await?;
    tx.execute("CREATE (c:Person {name: 'c'})-[:FOLLOWS {since: 2015}]->(d:Person {name: 'd'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (x:Person)-[r:KNOWS|FOLLOWS]->(y:Person) SET r.since = 2025")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:KNOWS|FOLLOWS]->() RETURN r.since AS s")
        .await?;
    assert_eq!(r.len(), 2);
    for row in r.rows() {
        assert_eq!(
            row.get::<i64>("s").unwrap(),
            2025,
            "multi-edge-type SET did not apply"
        );
    }
    Ok(())
}

/// L9 — Two SET clauses across a WITH boundary on the same variable.
#[tokio::test]
async fn l9_cross_with_two_sets_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'e1', x: 0, y: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E) SET n.x = 42 WITH n SET n.y = 99")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:E) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        42,
        "first SET did not apply"
    );
    assert_eq!(
        r.rows()[0].get::<i64>("y").unwrap(),
        99,
        "second SET across WITH did not apply"
    );
    Ok(())
}

/// L3 — `MATCH (a)-[r*1..2]->(b) SET b.x = 99`. Variable-length-path
/// target feeding SET. Confirmed working: each path target receives the
/// SET. The source `a` is unmodified.
#[tokio::test]
async fn l3_vlp_target_set_documents_behavior() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .edge_type("LINK", &["N"], &["N"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:N {id: 'a', x: 0})-[:LINK]->(b:N {id: 'b', x: 0})-[:LINK]->(c:N {id: 'c', x: 0})",
    )
    .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:N {id: 'a'})-[:LINK*1..2]->(b:N) SET b.x = 99")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:N) RETURN n.id AS id, n.x AS x ORDER BY id")
        .await?;
    let xs: std::collections::HashMap<String, i64> = r
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("id").unwrap(),
                row.get::<i64>("x").unwrap(),
            )
        })
        .collect();

    assert_eq!(xs["a"], 0, "a (source) should not be modified");
    assert_eq!(xs["b"], 99, "b (1-hop VLP target) was not SET");
    assert_eq!(xs["c"], 99, "c (2-hop VLP target) was not SET");
    Ok(())
}

/// L3b — VLP target SET with `RETURN b.x` to verify the inline binding
/// surfaces the post-SET value (Round-9 dotted-refresh shape on a VLP
/// target). Exercises the same code path as l3, plus the dotted-column
/// refresh in `append_cross_join_row`.
#[tokio::test]
async fn l3b_vlp_target_set_inline_return() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .edge_type("LINK", &["N"], &["N"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:N {id: 'a', x: 0})-[:LINK]->(b:N {id: 'b', x: 0})-[:LINK]->(c:N {id: 'c', x: 0})",
    )
    .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query("MATCH (a:N {id: 'a'})-[:LINK*1..2]->(b:N) SET b.x = 77 RETURN b.id AS id, b.x AS x ORDER BY id")
        .await?;
    tx.commit().await?;

    let xs: std::collections::HashMap<String, i64> = res
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("id").unwrap(),
                row.get::<i64>("x").unwrap(),
            )
        })
        .collect();
    assert_eq!(
        xs["b"], 77,
        "inline RETURN b.x surfaced stale binding for 1-hop target"
    );
    assert_eq!(
        xs["c"], 77,
        "inline RETURN b.x surfaced stale binding for 2-hop target"
    );
    Ok(())
}

/// L3c — VLP target SET on multi-property: ensures Round-3 coalescing
/// (one read+write per target) runs correctly for VLP-bound vertices.
#[tokio::test]
async fn l3c_vlp_target_set_multi_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::String)
        .done()
        .edge_type("LINK", &["N"], &["N"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 'a'})-[:LINK]->(b:N {id: 'b'})-[:LINK]->(c:N {id: 'c'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:N {id: 'a'})-[:LINK*1..2]->(b:N) SET b.x = 11, b.y = 'tagged'")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:N) RETURN n.id AS id, n.x AS x, n.y AS y ORDER BY id")
        .await?;
    let rows: Vec<_> = r.rows().iter().collect();
    // a is the source; b, c are targets.
    assert!(
        rows[0]
            .value("x")
            .map(|v| matches!(v, Value::Null))
            .unwrap_or(true),
        "a.x should be unset"
    );
    for row in &rows[1..] {
        assert_eq!(row.get::<i64>("x").unwrap(), 11);
        assert_eq!(row.get::<String>("y").unwrap(), "tagged");
    }
    Ok(())
}

/// L3d — VLP target SET with a step that returns ZERO matches must not
/// panic and must not mutate anything.
#[tokio::test]
async fn l3d_vlp_target_set_no_matches() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .edge_type("LINK", &["N"], &["N"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {id: 'lonely', x: 0})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:N {id: 'lonely'})-[:LINK*1..3]->(b:N) SET b.x = 999")
        .await?;
    tx.commit().await?;

    let r = db.session().query("MATCH (n:N) RETURN n.x AS x").await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        0,
        "no-match VLP wrote phantom data"
    );
    Ok(())
}

/// L3e — VLP edge SET via UNWIND on the bound path: `MATCH
/// (a)-[r*1..2]->(b) UNWIND r AS e SET e.flag = true`. Two defects stood in
/// the way: an Arrow schema mismatch (`List<Struct>` vs `LargeBinary`) at the
/// mutation boundary, fixed by the recursive normalization in
/// `normalize_mutation_schema`, and then a SET that ran without error but
/// silently wrote nothing to the UNWIND-bound edges. Both are fixed: the test
/// asserts that both path edges carry the flag after the SET commits. The edges are created without `flag`, so a no-op leaves
/// `count(e.flag)` at 0 and fails the assertion. (Direct VLP target SET is
/// covered by l3/l3b/l3c/l3d.)
#[tokio::test]
async fn l3e_vlp_path_edges_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .done()
        .edge_type("LINK", &["N"], &["N"])
        .property_nullable("flag", DataType::Bool)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 'a'})-[:LINK]->(b:N {id: 'b'})-[:LINK]->(c:N {id: 'c'})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (a:N {id: 'a'})-[r:LINK*1..2]->(c:N {id: 'c'}) \
         UNWIND r AS e SET e.flag = true",
    )
    .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH ()-[e:LINK]->() RETURN count(e) AS n, count(e.flag) AS flagged")
        .await?;
    let n = r.rows()[0].get::<i64>("n").unwrap();
    let flagged = r.rows()[0].get::<i64>("flagged").unwrap();
    assert_eq!(n, 2, "edge count");
    assert_eq!(flagged, 2, "both VLP edges should have flag=true");
    Ok(())
}

/// L2n — RecursiveCTE write path: SET inside the anchor of a
/// `WITH RECURSIVE ...` clause must execute and the recursive
/// expansion must see post-SET values. Round-7 follow-up for the
/// MutationContext propagation through RecursiveCTE.
#[tokio::test]
async fn l2n_recursive_cte_anchor_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .property_nullable("visited", DataType::Bool)
        .done()
        .edge_type("CHILD", &["Item"], &["Item"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..3 {
        tx.execute_with("CREATE (:Item {id: $id})")
            .param("id", Value::Int(i))
            .run()
            .await?;
    }
    tx.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    tx.execute("MATCH (a:Item {id: 1}), (b:Item {id: 2}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    tx.commit().await?;

    // Anchor mutates root.visited; recursive walks the chain.
    let tx = db.session().tx().await?;
    tx.execute(
        "WITH RECURSIVE hierarchy AS (
             MATCH (root:Item {id: 0}) SET root.visited = true RETURN root
             UNION
             MATCH (parent:Item)-[:CHILD]->(child:Item) WHERE parent IN hierarchy RETURN child
         )
         MATCH (n:Item) WHERE n IN hierarchy RETURN n.id AS id ORDER BY id",
    )
    .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Item {id: 0}) RETURN n.visited AS v")
        .await?;
    let val = r.rows()[0].value("v").unwrap();
    assert!(
        matches!(val, Value::Bool(true)),
        "anchor SET inside RecursiveCTE did not persist: {val:?}"
    );
    Ok(())
}

/// L2n2 — RecursiveCTE recursive-side write: SET inside the recursive
/// arm should fire for each new child the recursion discovers, and the
/// writes must persist.
#[tokio::test]
async fn l2n2_recursive_cte_recursive_set_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .property_nullable("touched", DataType::Bool)
        .done()
        .edge_type("CHILD", &["Item"], &["Item"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..4 {
        tx.execute_with("CREATE (:Item {id: $id})")
            .param("id", Value::Int(i))
            .run()
            .await?;
    }
    // chain 0 → 1 → 2 → 3
    for (a, b) in [(0, 1), (1, 2), (2, 3)] {
        tx.execute_with("MATCH (a:Item {id: $a}), (b:Item {id: $b}) CREATE (a)-[:CHILD]->(b)")
            .param("a", Value::Int(a))
            .param("b", Value::Int(b))
            .run()
            .await?;
    }
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "WITH RECURSIVE hierarchy AS (
             MATCH (root:Item {id: 0}) RETURN root
             UNION
             MATCH (parent:Item)-[:CHILD]->(child:Item) WHERE parent IN hierarchy
             SET child.touched = true RETURN child
         )
         MATCH (n:Item) WHERE n IN hierarchy RETURN n.id AS id ORDER BY id",
    )
    .await?;
    tx.commit().await?;

    // All descendants (ids 1, 2, 3) should be touched; root (id 0) should not.
    let r = db
        .session()
        .query("MATCH (n:Item) RETURN n.id AS id, n.touched AS t ORDER BY id")
        .await?;
    let rows = r.rows();
    assert_eq!(rows.len(), 4);
    assert!(
        matches!(
            rows[0].value("t").unwrap(),
            Value::Null | Value::Bool(false)
        ),
        "root should not be touched: {:?}",
        rows[0].value("t")
    );
    for row in &rows[1..] {
        let v = row.value("t").unwrap();
        assert!(
            matches!(v, Value::Bool(true)),
            "descendant id={} should be touched: {v:?}",
            row.get::<i32>("id").unwrap()
        );
    }
    Ok(())
}

/// L2n3 — read-only RecursiveCTE should not regress when MutationContext
/// is None. Regression guard for the Round-7 propagation work.
#[tokio::test]
async fn l2n3_recursive_cte_read_only_unaffected() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .done()
        .edge_type("CHILD", &["Item"], &["Item"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..3 {
        tx.execute_with("CREATE (:Item {id: $id})")
            .param("id", Value::Int(i))
            .run()
            .await?;
    }
    tx.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    tx.execute("MATCH (a:Item {id: 1}), (b:Item {id: 2}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query(
            "WITH RECURSIVE chain AS (
                 MATCH (root:Item {id: 0}) RETURN root
                 UNION
                 MATCH (parent:Item)-[:CHILD]->(child:Item) WHERE parent IN chain RETURN child
             )
             MATCH (n:Item) WHERE n IN chain RETURN n.id AS id ORDER BY id",
        )
        .await?;
    let ids: Vec<i32> = r
        .rows()
        .iter()
        .map(|row| row.get::<i32>("id").unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![0, 1, 2],
        "read-only RecursiveCTE expansion broken"
    );
    Ok(())
}
