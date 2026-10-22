// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A relationship-type disjunction mixing a declared type with an undeclared
//! one matches either type.
//!
//! `[e:R|Q]` with `R` declared and `Q` schemaless was refused with "Mixed known
//! and unknown edge types not yet supported", in a plain pattern, a
//! variable-length one and a Locy rule body (where the cause was also dropped
//! from the error). openCypher matches a relationship of any listed type
//! (TCK `Match2.feature` [6], "Matching a relationship pattern using a label
//! predicate" family); whether a type is declared is not part of the pattern's
//! meaning.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(mixed_declared_undeclared_edge_types)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("A")
        .property("id", DataType::Int)
        .label("B")
        .property("id", DataType::Int)
        .edge_type("R", &["A"], &["B"])
        .property("w", DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    // a -R-> b2, a -Q-> b3, b2 -Q-> b3.
    tx.execute(
        "CREATE (a:A {id: 1}), (b2:B {id: 2}), (b3:B {id: 3}), \
                (a)-[:R {w: 5}]->(b2), (a)-[:Q {w: 7}]->(b3), (b2)-[:Q {w: 9}]->(b3)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn rows(db: &Uni, cypher: &str) -> Result<Vec<Vec<Value>>> {
    let result = db
        .session()
        .query(cypher)
        .await
        .map_err(|e| anyhow::anyhow!("{cypher}: {e}"))?;
    Ok(result.rows().iter().map(|r| r.values().to_vec()).collect())
}

fn s(v: &str) -> Value {
    Value::String(v.into())
}

async fn assert_mixed(db: &Uni, phase: &str) -> Result<()> {
    // Every edge of either type, once.
    assert_eq!(
        rows(db, "MATCH ()-[e:R|Q]->() RETURN type(e) AS t ORDER BY t").await?,
        vec![vec![s("Q")], vec![s("Q")], vec![s("R")]],
        "{phase}: anonymous endpoints"
    );
    // Declared and undeclared edges with their properties, from a bound source.
    assert_eq!(
        rows(
            db,
            "MATCH (a:A)-[e:R|Q]->(b) RETURN b.id AS id, type(e) AS t, e.w AS w ORDER BY id"
        )
        .await?,
        vec![
            vec![Value::Int(2), s("R"), Value::Int(5)],
            vec![Value::Int(3), s("Q"), Value::Int(7)],
        ],
        "{phase}: bound source"
    );
    // The listed order does not matter.
    assert_eq!(
        rows(db, "MATCH (a:A)-[e:Q|R]->(b) RETURN b.id AS id ORDER BY id").await?,
        vec![vec![Value::Int(2)], vec![Value::Int(3)]],
        "{phase}: reversed disjunction"
    );
    // Incoming.
    assert_eq!(
        rows(
            db,
            "MATCH (b:B {id: 3})<-[e:R|Q]-(x) RETURN x.id AS id, type(e) AS t ORDER BY id"
        )
        .await?,
        vec![vec![Value::Int(1), s("Q")], vec![Value::Int(2), s("Q")]],
        "{phase}: incoming"
    );
    // A whole relationship value keeps its type and properties.
    let r = rows(db, "MATCH (:A)-[e:R|Q]->(:B {id: 2}) RETURN e").await?;
    match &r[..] {
        [row] => match &row[0] {
            Value::Edge(e) => {
                assert_eq!(e.edge_type, "R", "{phase}: {e:?}");
                assert_eq!(
                    e.properties.get("w"),
                    Some(&Value::Int(5)),
                    "{phase}: {e:?}"
                );
            }
            other => panic!("{phase}: expected an edge, got {other:?}"),
        },
        other => panic!("{phase}: one row expected, got {other:?}"),
    }
    // Variable length: a-R->b2, a-Q->b3, a-R->b2-Q->b3.
    assert_eq!(
        rows(
            db,
            "MATCH (a:A)-[:R|Q*1..2]->(b) RETURN b.id AS id ORDER BY id"
        )
        .await?,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(3)]
        ],
        "{phase}: variable length"
    );
    // A Locy rule body.
    let result = db
        .session()
        .locy("CREATE RULE hop AS MATCH (x:A)-[:R|Q]->(y) YIELD KEY x, KEY y QUERY hop RETURN y.id AS id")
        .await
        .map_err(|e| anyhow::anyhow!("{phase}: Locy: {e}"))?;
    let query = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .expect("a QUERY");
    let mut ids: Vec<i64> = query
        .iter()
        .map(|r| r.get("id").and_then(Value::as_i64).expect("id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 3], "{phase}: Locy");
    Ok(())
}

#[tokio::test]
async fn mixed_declared_undeclared_edge_types_unflushed() -> Result<()> {
    let db = open().await?;
    assert_mixed(&db, "L0").await
}

#[tokio::test]
async fn mixed_declared_undeclared_edge_types_flushed() -> Result<()> {
    let db = open().await?;
    db.flush().await?;
    assert_mixed(&db, "flushed").await
}

/// A Locy planning failure keeps its cause in the error text.
#[tokio::test]
async fn mixed_declared_undeclared_edge_types_locy_error_keeps_cause() -> Result<()> {
    let db = open().await?;
    // The edge variable reused as the target node is a planning error with a
    // specific cause; the wrapper used to print only its outermost context.
    let err = db
        .session()
        .locy("CREATE RULE bad AS MATCH (x:A)-[r:R]->(r) YIELD KEY x QUERY bad RETURN x.id AS id")
        .await
        .expect_err("an invalid rule body must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("VariableTypeConflict"),
        "the Locy planning error must carry its cause, got: {msg}"
    );
    Ok(())
}
