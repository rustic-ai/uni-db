// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests T1–T9 for the partial-column Lance writes workstream
//! (Round 11). See the Round-11 section of
//! `/home/rohit/.claude/plans/plan-and-implement-a-valiant-flame.md`.
//!
//! The soundness probe (`crates/uni-store/tests/common/storage/
//! lance_merge_insert_probe.rs`) already verifies the Lance API behavior;
//! these tests verify the end-to-end Cypher SET path with the flag on.

// Rust guideline compliant

use anyhow::Result;
use uni_common::UniConfig;
use uni_db::{DataType, Uni, Value};

fn flag_on_config() -> UniConfig {
    UniConfig {
        #[expect(deprecated, reason = "exercises the deprecated no-op flag")]
        partial_lance_writes: true,
        ..UniConfig::default()
    }
}

/// T1 — Partial SET preserves embedding + non-touched columns. The
/// production hot-path shape: wide schema with a vector column, SET
/// touches only scalar columns, embedding stays intact and KNN still
/// returns the queried row post-flush.
#[tokio::test]
async fn t1_partial_set_preserves_embedding_and_other_columns() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("Entity")
        .property("id", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .property_nullable("confidence", DataType::Float64)
        .property_nullable("untouched", DataType::String)
        .vector("embedding", 8)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..10 {
        let emb: Vec<Value> = (0..8)
            .map(|j| Value::Float(i as f64 + j as f64 * 0.01))
            .collect();
        tx.execute_with(
            "CREATE (:Entity {id: $id, frequency: 0, confidence: 0.0, untouched: $u, embedding: $e})",
        )
        .param("id", Value::String(format!("e{i}")))
        .param("u", Value::String(format!("orig{i}")))
        .param("e", Value::List(emb))
        .run()
        .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    // SET only frequency + confidence on rows e0..=e4 (5 rows).
    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute_with("MATCH (n:Entity {id: $id}) SET n.frequency = $f, n.confidence = $c")
            .param("id", Value::String(format!("e{i}")))
            .param("f", Value::Int(100 + i as i64))
            .param("c", Value::Float(0.9))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    // Verify: all 10 rows present, e0..=e4 updated, e5..=e9 untouched,
    // embedding/untouched preserved on all rows.
    let r = db
        .session()
        .query(
            "MATCH (n:Entity) RETURN n.id AS id, n.frequency AS f, n.confidence AS c, \
             n.untouched AS u, n.embedding AS e ORDER BY id",
        )
        .await?;
    assert_eq!(r.rows().len(), 10);
    for (i, row) in r.rows().iter().enumerate() {
        let id = row.get::<String>("id").unwrap();
        assert_eq!(id, format!("e{i}"));
        let untouched = row.get::<String>("u").unwrap();
        assert_eq!(untouched, format!("orig{i}"), "row {id}: untouched mutated");
        let emb = row.value("e").unwrap();
        // A dense `Vector` column projects back as `Value::Vector` (the typed
        // representation), not `Value::List`.
        if let Value::Vector(items) = emb {
            assert_eq!(items.len(), 8, "row {id}: embedding length wrong");
            for (j, f) in items.iter().enumerate() {
                let want = i as f64 + j as f64 * 0.01;
                // f32 epsilon: embedding round-trips through Lance's Float32 encoding.
                assert!(
                    (*f as f64 - want).abs() < 1e-5,
                    "row {id}: embedding[{j}] = {f}, want {want}"
                );
            }
        } else {
            panic!("row {id}: embedding not preserved as a vector: {emb:?}");
        }
        if i < 5 {
            assert_eq!(
                row.get::<i64>("f").unwrap(),
                100 + i as i64,
                "row {id} freq"
            );
            let c = row.value("c").unwrap();
            if let Value::Float(f) = c {
                assert!((f - 0.9).abs() < 1e-9);
            }
        } else {
            assert_eq!(row.get::<i64>("f").unwrap(), 0);
        }
    }
    Ok(())
}

/// T1b — #121 regression: same as T1 but CREATE and the scalar SET coalesce in
/// ONE L0 window (no flush between), so the row is never flushed before the partial
/// SET. The untouched `embedding` (a `FixedSizeList`) must still read back intact —
/// the partial MergeInsert preserves columns absent from its sub-schema source.
#[tokio::test]
async fn t1b_partial_set_preserves_embedding_no_intervening_flush() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("Entity")
        .property("id", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .vector("embedding", 8)
        .done()
        .apply()
        .await?;

    // CREATE then SET in the SAME L0 window — no flush between.
    let tx = db.session().tx().await?;
    for i in 0..5 {
        let emb: Vec<Value> = (0..8)
            .map(|j| Value::Float(i as f64 + j as f64 * 0.01))
            .collect();
        tx.execute_with("CREATE (:Entity {id: $id, frequency: 0, embedding: $e})")
            .param("id", Value::String(format!("e{i}")))
            .param("e", Value::List(emb))
            .run()
            .await?;
    }
    tx.commit().await?;
    let tx = db.session().tx().await?;
    for i in 0..3 {
        tx.execute_with("MATCH (n:Entity {id: $id}) SET n.frequency = $f")
            .param("id", Value::String(format!("e{i}")))
            .param("f", Value::Int(100 + i as i64))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?; // single flush over the coalesced CREATE+SET

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.id AS id, n.embedding AS e ORDER BY id")
        .await?;
    assert_eq!(r.rows().len(), 5);
    for (i, row) in r.rows().iter().enumerate() {
        let emb = row.value("e").unwrap();
        if let Value::Vector(items) = emb {
            assert_eq!(items.len(), 8, "row e{i}: embedding length wrong");
            assert!(
                (items[0] as f64 - i as f64).abs() < 1e-5,
                "row e{i}: embedding[0] = {}",
                items[0]
            );
        } else {
            panic!("row e{i}: embedding lost on no-intervening-flush partial SET: {emb:?}");
        }
    }
    Ok(())
}

/// T1c — #121 regression: a non-touched multivector `List<Vector>` column (an Arrow
/// `List`) is preserved across a scalar-only partial SET.
#[tokio::test]
async fn t1c_partial_set_preserves_multivector() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: 4 })),
        )
        .apply()
        .await?;

    let tokens = |i: usize| {
        Value::List(
            (0..3)
                .map(|t| {
                    Value::List(
                        (0..4)
                            .map(|d| Value::Float((i * 10 + t * 4 + d) as f64))
                            .collect(),
                    )
                })
                .collect(),
        )
    };

    let tx = db.session().tx().await?;
    for i in 0..3 {
        tx.execute_with("CREATE (:Doc {id: $id, frequency: 0, tokens: $tk})")
            .param("id", Value::String(format!("d{i}")))
            .param("tk", tokens(i))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Doc {id: 'd0'}) SET n.frequency = 7")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:Doc) RETURN n.id AS id, n.tokens AS tk ORDER BY id")
        .await?;
    assert_eq!(r.rows().len(), 3);
    for (i, row) in r.rows().iter().enumerate() {
        match row.value("tk").unwrap() {
            Value::List(toks) => assert_eq!(
                toks.len(),
                3,
                "row d{i}: multivector token count lost on partial SET"
            ),
            other => panic!("row d{i}: tokens not a list (lost on partial SET): {other:?}"),
        }
    }
    Ok(())
}

/// T1d — #121 regression: a non-touched `SparseVector` column (an Arrow `Struct`)
/// is preserved across a scalar-only partial SET, indices + values intact.
#[tokio::test]
async fn t1d_partial_set_preserves_sparse_vector() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .property_nullable("emb", DataType::SparseVector { dimensions: 100 })
        .apply()
        .await?;

    let sv = |base: u32| Value::SparseVector {
        indices: vec![base, base + 1, base + 2],
        values: vec![1.0, 2.0, 3.0],
    };

    let tx = db.session().tx().await?;
    for i in 0..3u32 {
        tx.execute_with("CREATE (:Doc {id: $id, frequency: 0, emb: $e})")
            .param("id", Value::String(format!("d{i}")))
            .param("e", sv(i * 10))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Doc {id: 'd0'}) SET n.frequency = 7")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:Doc) RETURN n.id AS id, n.emb AS e ORDER BY id")
        .await?;
    assert_eq!(r.rows().len(), 3);
    for (i, row) in r.rows().iter().enumerate() {
        match row.value("e").unwrap() {
            Value::SparseVector { indices, values } => {
                let base = i as u32 * 10;
                assert_eq!(
                    indices,
                    &vec![base, base + 1, base + 2],
                    "row d{i}: sparse indices lost on partial SET"
                );
                assert_eq!(values, &vec![1.0f32, 2.0, 3.0], "row d{i}: sparse values");
            }
            other => panic!("row d{i}: emb not SparseVector (lost on partial SET): {other:?}"),
        }
    }
    Ok(())
}

/// T2 — Flag-off equivalence: with `partial_lance_writes = false`
/// (default), the SET behaves bit-for-bit as it did pre-Round-11.
/// This is the central regression guard that Stage 1 ships under.
#[tokio::test]
async fn t2_partial_set_flag_off_equivalence() -> Result<()> {
    // Default UniConfig has partial_lance_writes = false.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Entity")
        .property("id", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .property_nullable("untouched", DataType::String)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {id: 'k', frequency: 0, untouched: 'keep'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity {id: 'k'}) SET n.frequency = 42")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity {id: 'k'}) RETURN n.frequency AS f, n.untouched AS u")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("f").unwrap(), 42);
    assert_eq!(r.rows()[0].get::<String>("u").unwrap(), "keep");
    Ok(())
}

/// T3 — Two SETs on the same vertex in one tx (different keys) must
/// both land via a single MergeInsert source row.
#[tokio::test]
async fn t3_partial_set_two_keys_same_vertex_one_tx() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("Entity")
        .property("id", DataType::String)
        .property_nullable("a", DataType::Int64)
        .property_nullable("b", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Entity {id: 'k', a: 0, b: 0})").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Entity {id: 'k'}) SET n.a = 1").await?;
    tx.execute("MATCH (n:Entity {id: 'k'}) SET n.b = 2").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity {id: 'k'}) RETURN n.a AS a, n.b AS b")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("a").unwrap(), 1);
    assert_eq!(r.rows()[0].get::<i64>("b").unwrap(), 2);
    Ok(())
}

/// T4 — Partial SET followed by Variable replace (`SET n = {...}`)
/// on the same vid in the same tx: the Variable replace forces a
/// full-row Append, and the partial state is superseded (no partial
/// batch emitted for this vid).
#[tokio::test]
async fn t4_partial_then_variable_replace() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("a", DataType::Int64)
        .property_nullable("b", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'k', a: 0, b: 0})").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'k'}) SET n.a = 1").await?;
    tx.execute("MATCH (n:E {id: 'k'}) SET n = {id: 'k', a: 99, b: 99}")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:E {id: 'k'}) RETURN n.a AS a, n.b AS b")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("a").unwrap(),
        99,
        "Variable replace lost"
    );
    assert_eq!(r.rows()[0].get::<i64>("b").unwrap(), 99);
    Ok(())
}

/// T6 — Partial SET on a HASH-indexed scalar column. After the merge,
/// the property is queryable through the index (a simple read-back is
/// sufficient — the index lookup path runs).
#[tokio::test]
async fn t6_partial_set_with_hash_index() -> Result<()> {
    use uni_db::api::schema::{IndexType, ScalarType};
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("U")
        .property("id", DataType::String)
        .property_nullable("email", DataType::String)
        .index("email", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:U {id: 'u1', email: 'old@example.com'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:U {id: 'u1'}) SET n.email = 'new@example.com'")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query_with("MATCH (n:U) WHERE n.email = $e RETURN n.id AS id")
        .param("e", Value::String("new@example.com".to_string()))
        .fetch_all()
        .await?;
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].get::<String>("id").unwrap(), "u1");
    Ok(())
}

/// T8 — Partial SET, then DELETE in a later tx: deletion supersedes
/// the partial state.
#[tokio::test]
async fn t8_partial_set_then_delete() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'k', x: 0})").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'k'}) SET n.x = 5").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'k'}) DELETE n").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:E) RETURN count(n) AS c")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("c").unwrap(), 0);
    Ok(())
}

/// C1 — Generated column recomputes when a partial SET changes the
/// underlying property. Schema has a generated `_gen_LOWER_email`
/// derived from `lower(email)`; SET email under flag-on; read back:
/// generated column reflects the new lower-cased value.
#[tokio::test]
async fn c1_partial_set_with_generated_column_recomputes() -> Result<()> {
    use uni_common::UniConfig;
    let cfg = UniConfig {
        #[expect(deprecated, reason = "exercises the deprecated no-op flag")]
        partial_lance_writes: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;

    // Set up schema + expression index via DDL (creates the generated
    // column automatically).
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (id STRING, email STRING)")
        .await?;
    tx.execute("CREATE INDEX lower_email FOR (u:User) ON (lower(u.email))")
        .await?;
    tx.commit().await?;

    let gen_col = uni_db::core::schema::SchemaManager::generated_column_name("lower(email)");

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:User {id: 'u1', email: 'Alice@Example.com'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Sanity: generated column populated from CREATE.
    let r0 = db
        .session()
        .query(&format!(
            "MATCH (u:User {{id: 'u1'}}) RETURN u.email AS e, u.{gen_col} AS g"
        ))
        .await?;
    assert_eq!(
        r0.rows()[0].get::<String>("g").unwrap(),
        "alice@example.com"
    );

    // Partial-flag SET on email: generator must recompute.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (u:User {id: 'u1'}) SET u.email = 'BOB@example.com'")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r1 = db
        .session()
        .query(&format!(
            "MATCH (u:User {{id: 'u1'}}) RETURN u.email AS e, u.{gen_col} AS g"
        ))
        .await?;
    assert_eq!(r1.rows()[0].get::<String>("e").unwrap(), "BOB@example.com");
    assert_eq!(
        r1.rows()[0].get::<String>("g").unwrap(),
        "bob@example.com",
        "generated column did not recompute under partial flush"
    );
    Ok(())
}

/// C2 — Generated column depends on `email`. SET an UNRELATED
/// property; the generator harmlessly recomputes against the unchanged
/// `email`, producing the same value (idempotency check).
#[tokio::test]
async fn c2_partial_set_untouched_generator_dependency() -> Result<()> {
    use uni_common::UniConfig;
    let cfg = UniConfig {
        #[expect(deprecated, reason = "exercises the deprecated no-op flag")]
        partial_lance_writes: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (id STRING, email STRING, age INT)")
        .await?;
    tx.execute("CREATE INDEX lower_email FOR (p:Person) ON (lower(p.email))")
        .await?;
    tx.commit().await?;
    let gen_col = uni_db::core::schema::SchemaManager::generated_column_name("lower(email)");

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {id: 'p1', email: 'Carol@x.com', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // SET an unrelated column (age).
    let tx = db.session().tx().await?;
    tx.execute("MATCH (p:Person {id: 'p1'}) SET p.age = 31")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query(&format!(
            "MATCH (p:Person {{id: 'p1'}}) RETURN p.age AS a, p.email AS e, p.{gen_col} AS g"
        ))
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("a").unwrap(), 31);
    assert_eq!(r.rows()[0].get::<String>("e").unwrap(), "Carol@x.com");
    assert_eq!(r.rows()[0].get::<String>("g").unwrap(), "carol@x.com");
    Ok(())
}

/// C3 — Two generators on the same label both recompute when their
/// shared input is updated via a partial SET.
#[tokio::test]
async fn c3_partial_set_with_multiple_generators() -> Result<()> {
    use uni_common::UniConfig;
    let cfg = UniConfig {
        #[expect(deprecated, reason = "exercises the deprecated no-op flag")]
        partial_lance_writes: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Note (id STRING, body STRING)")
        .await?;
    tx.execute("CREATE INDEX lower_body FOR (n:Note) ON (lower(n.body))")
        .await?;
    tx.execute("CREATE INDEX upper_body FOR (n:Note) ON (upper(n.body))")
        .await?;
    tx.commit().await?;
    let gen_lo = uni_db::core::schema::SchemaManager::generated_column_name("lower(body)");
    let gen_up = uni_db::core::schema::SchemaManager::generated_column_name("upper(body)");

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Note {id: 'n1', body: 'Hello'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Note {id: 'n1'}) SET n.body = 'World'")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query(&format!(
            "MATCH (n:Note {{id: 'n1'}}) RETURN n.body AS b, n.{gen_lo} AS lo, n.{gen_up} AS up"
        ))
        .await?;
    assert_eq!(r.rows()[0].get::<String>("b").unwrap(), "World");
    assert_eq!(r.rows()[0].get::<String>("lo").unwrap(), "world");
    assert_eq!(r.rows()[0].get::<String>("up").unwrap(), "WORLD");
    Ok(())
}

/// T9 — Cross-tx partial SETs accumulate: after the first SET, a
/// follow-up SET on a different column merges correctly (the per-VID
/// dirty-key set unions across transactions before the flush).
#[tokio::test]
async fn t9_partial_set_cross_tx_dirty_key_union() -> Result<()> {
    let db = Uni::in_memory().config(flag_on_config()).build().await?;
    db.schema()
        .label("E")
        .property("id", DataType::String)
        .property_nullable("a", DataType::Int64)
        .property_nullable("b", DataType::Int64)
        .property_nullable("untouched", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {id: 'k', a: 0, b: 0, untouched: 'keep'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Tx1: SET a only.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'k'}) SET n.a = 11").await?;
    tx.commit().await?;
    // (no flush — second tx joins in same L0)
    // Tx2: SET b only.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:E {id: 'k'}) SET n.b = 22").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:E {id: 'k'}) RETURN n.a AS a, n.b AS b, n.untouched AS u")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("a").unwrap(), 11);
    assert_eq!(r.rows()[0].get::<i64>("b").unwrap(), 22);
    assert_eq!(r.rows()[0].get::<String>("u").unwrap(), "keep");
    Ok(())
}
