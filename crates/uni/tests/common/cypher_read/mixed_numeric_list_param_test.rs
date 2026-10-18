// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A list mixing integers and floats keeps each element's type, whether it
//! arrives as a parameter or a literal — and numeric lists still work as
//! query vectors.
//!
//! Measured before the fix: `RETURN $l` with `$l = [1, 1.5]` gave
//! `[1.0, 1.5]` (so did the literal `[1, 1.5]`), and with
//! `$l = [2^53 + 1, 1.5]` both `9007199254740992 IN $l` and
//! `MATCH (n) WHERE n.x IN $l` (for `x = 2^53`) matched, because widening to
//! `Float64` rounded `2^53 + 1` down to `2^53`.
//!
//! The fix encodes such a list as one CypherValue, the form every other
//! heterogeneous list takes. A numeric list is also how a query vector
//! arrives, so the second half checks each vector consumer gives the same
//! answer for `[1, 0.5, 0]` (mixed) as for `[1.0, 0.5, 0.0]` (all floats).

use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

const TWO_53: i64 = 9_007_199_254_740_992;

fn mixed() -> Value {
    Value::List(vec![Value::Int(1), Value::Float(1.5)])
}

fn big_mixed() -> Value {
    Value::List(vec![Value::Int(TWO_53 + 1), Value::Float(1.5)])
}

async fn one(db: &Uni, q: &str, l: Value) -> Value {
    let r = db
        .session()
        .query_with(q)
        .param("l", l)
        .fetch_all()
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    r.rows()[0].value("r").cloned().unwrap()
}

/// Element types survive a round trip, as a parameter and as a literal.
#[tokio::test]
async fn mixed_list_keeps_element_types() {
    let db = Uni::in_memory().build().await.unwrap();
    let want = Value::List(vec![Value::Int(1), Value::Float(1.5)]);
    assert_eq!(one(&db, "RETURN $l AS r", mixed()).await, want);
    assert_eq!(one(&db, "RETURN [1, 1.5] AS r", Value::Null).await, want);
    assert_eq!(
        one(&db, "RETURN [x IN $l | x] AS r", mixed()).await,
        want,
        "comprehension over the parameter"
    );
    assert_eq!(
        one(&db, "RETURN $l[0] AS r", mixed()).await,
        Value::Int(1),
        "indexing the parameter"
    );
    assert_eq!(
        one(&db, "RETURN [9007199254740993, 1.5] AS r", Value::Null).await,
        big_mixed()
    );
}

/// Membership compares with the element's own type: `2^53 = 2^53 + 1` is
/// false for two integers.
#[tokio::test]
async fn membership_in_a_mixed_list_is_exact() {
    let db = Uni::in_memory().build().await.unwrap();
    for (q, want) in [
        ("RETURN 9007199254740992 IN $l AS r", false),
        ("RETURN 9007199254740993 IN $l AS r", true),
        ("RETURN 1.5 IN $l AS r", true),
        (
            "RETURN 9007199254740992 IN [9007199254740993, 1.5] AS r",
            false,
        ),
        (
            "RETURN 9007199254740993 IN [9007199254740993, 1.5] AS r",
            true,
        ),
    ] {
        assert_eq!(one(&db, q, big_mixed()).await, Value::Bool(want), "{q}");
    }

    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property_nullable("x", DataType::Int64)
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute(&format!(
        "CREATE (:N {{id: 1, x: {TWO_53}}}), (:N {{id: 2, x: {}}}), (:N {{id: 3, x: 1}})",
        TWO_53 + 1
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    for flush in [false, true] {
        if flush {
            db.flush().await.unwrap();
        }
        let r = one(
            &db,
            "MATCH (n:N) WHERE n.x IN $l RETURN collect(n.id) AS r",
            big_mixed(),
        )
        .await;
        assert_eq!(r, Value::List(vec![Value::Int(2)]), "flush={flush}");
        let r = one(
            &db,
            "MATCH (n:N) WHERE n.x IN $l RETURN collect(n.id) AS r",
            Value::List(vec![Value::Int(1), Value::Float(2.5)]),
        )
        .await;
        assert_eq!(r, Value::List(vec![Value::Int(3)]), "flush={flush}");
    }
}

fn floats(v: &[f64]) -> Value {
    Value::List(v.iter().copied().map(Value::Float).collect())
}

/// `[1, 0.5, 0]`: two integers and a float.
fn mixed_vec() -> Value {
    Value::List(vec![Value::Int(1), Value::Float(0.5), Value::Int(0)])
}

fn float_vec() -> Value {
    floats(&[1.0, 0.5, 0.0])
}

async fn vector_db() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("V")
        .property("id", DataType::Int64)
        .property_nullable("emb", DataType::Vector { dimensions: 3 })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for (id, e) in [
        (1, [1.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0]),
        (3, [0.0, 0.0, 1.0]),
        (4, [1.0, 0.5, 0.0]),
    ] {
        tx.execute_with("CREATE (:V {id: $id, emb: $e})")
            .param("id", Value::Int(id))
            .param("e", floats(&e))
            .run()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db
}

async fn rows(db: &Uni, q: &str, v: Value) -> Vec<String> {
    let r = db
        .session()
        .query_with(q)
        .param("q", v.clone())
        .fetch_all()
        .await
        .unwrap_or_else(|e| panic!("{q} with $q = {v:?}: {e}"));
    r.rows().iter().map(|row| format!("{row:?}")).collect()
}

/// Every reader of a query vector answers the same for a mixed list as for
/// an all-float one.
#[tokio::test]
async fn vector_readers_accept_a_mixed_numeric_list() {
    let db = vector_db().await;
    for flush in [false, true] {
        if flush {
            db.flush().await.unwrap();
        }
        for q in [
            "MATCH (n:V) RETURN n.id AS id, vector_similarity(n.emb, $q) AS s ORDER BY id",
            "MATCH (n:V) RETURN n.id AS id, vector_distance(n.emb, $q) AS d ORDER BY id",
            "MATCH (n:V) RETURN n.id AS id, similar_to(n.emb, $q) AS s ORDER BY id",
            "MATCH (n:V) WHERE n.emb ~= $q RETURN n.id AS id LIMIT 2",
            "CALL uni.vector.query('V', 'emb', $q, 2, null, null, {}) \
             YIELD node, score RETURN node.id AS id, score",
            "RETURN vector_similarity($q, [1.0, 0.0, 0.0]) AS s",
        ] {
            let want = rows(&db, q, float_vec()).await;
            assert!(!want.is_empty(), "flush={flush}: {q} returned nothing");
            assert_eq!(rows(&db, q, mixed_vec()).await, want, "flush={flush}: {q}");
        }
    }
}

/// A VECTOR property written from a mixed list stores the same vector as one
/// written from floats, through CREATE, SET and an UNWIND of maps.
#[tokio::test]
async fn vector_writes_accept_a_mixed_numeric_list() {
    let db = vector_db().await;
    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute_with("CREATE (:V {id: 10, emb: $q})")
        .param("q", mixed_vec())
        .run()
        .await
        .unwrap();
    tx.execute_with("CREATE (:V {id: 11, emb: [1.0, 1.0, 1.0]})")
        .run()
        .await
        .unwrap();
    tx.execute_with("MATCH (n:V {id: 11}) SET n.emb = $q")
        .param("q", mixed_vec())
        .run()
        .await
        .unwrap();
    tx.execute_with("UNWIND $rows AS r CREATE (:V {id: r.id, emb: r.emb})")
        .param(
            "rows",
            Value::List(vec![Value::Map(
                [
                    ("id".to_string(), Value::Int(12)),
                    ("emb".to_string(), mixed_vec()),
                ]
                .into_iter()
                .collect(),
            )]),
        )
        .run()
        .await
        .unwrap();
    tx.execute("CREATE (:V {id: 13, emb: [1, 0.5, 0]})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for flush in [false, true] {
        if flush {
            db.flush().await.unwrap();
        }
        let r = db
            .session()
            .query("MATCH (n:V) WHERE n.id IN [4, 10, 11, 12, 13] RETURN n.id AS id, n.emb AS e ORDER BY id")
            .await
            .unwrap();
        let reference = r.rows()[0].value("e").cloned().unwrap();
        assert_eq!(r.rows().len(), 5, "flush={flush}");
        for row in r.rows() {
            assert_eq!(
                row.value("e").unwrap(),
                &reference,
                "flush={flush}: id {:?}",
                row.value("id")
            );
        }
    }
}
