// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A declared `VECTOR` property indexes and slices like the list it was
//! written as, on both expression evaluators.
//!
//! A `VECTOR` column reads back as `Value::Vector`. The DataFusion path
//! (reads, `RETURN n.e[0]`) indexed it but could not slice it; the row
//! evaluator (write expressions such as `SET n.x = n.e[0]`) could do neither,
//! failing with "cannot index into Vector" / "Cannot slice Vector".
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(vector_index_slice)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

async fn db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("k", DataType::Int)
        .property("e", DataType::Vector { dimensions: 3 })
        .property_nullable("x", DataType::Float)
        .property_nullable("l", DataType::List(Box::new(DataType::Float)))
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {k: 1, e: [1.0, 2.0, 3.0]})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

fn floats(v: &Value) -> Vec<f64> {
    match v {
        Value::List(items) => items
            .iter()
            .map(|i| match i {
                Value::Float(f) => *f,
                other => panic!("expected a float element, got {other:?}"),
            })
            .collect(),
        Value::Vector(items) => items.iter().map(|f| f64::from(*f)).collect(),
        other => panic!("expected a list, got {other:?}"),
    }
}

/// Reads: the DataFusion evaluator.
async fn check_reads(db: &Uni, phase: &str) -> Result<()> {
    let r = db
        .session()
        .query(
            "MATCH (n:Doc) RETURN n.e[0] AS a, n.e[-1] AS b, n.e[5] AS c, \
             n.e[0..2] AS s, n.e[1..] AS t, n.e[..-1] AS u, n.e[2..1] AS w",
        )
        .await?;
    let row = r.rows()[0].values().to_vec();
    assert_eq!(row[0], Value::Float(1.0), "{phase}: e[0]");
    assert_eq!(row[1], Value::Float(3.0), "{phase}: e[-1]");
    assert_eq!(row[2], Value::Null, "{phase}: e[5]");
    assert_eq!(floats(&row[3]), vec![1.0, 2.0], "{phase}: e[0..2]");
    assert_eq!(floats(&row[4]), vec![2.0, 3.0], "{phase}: e[1..]");
    assert_eq!(floats(&row[5]), vec![1.0, 2.0], "{phase}: e[..-1]");
    assert_eq!(floats(&row[6]), Vec::<f64>::new(), "{phase}: e[2..1]");
    Ok(())
}

/// Writes: the row evaluator computes the SET expressions.
async fn check_writes(db: &Uni, phase: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Doc {k: 1}) SET n.x = n.e[1], n.l = n.e[1..3]")
        .await?;
    tx.commit().await?;
    let r = db
        .session()
        .query("MATCH (n:Doc {k: 1}) RETURN n.x AS x, n.l AS l")
        .await?;
    let row = r.rows()[0].values().to_vec();
    assert_eq!(row[0], Value::Float(2.0), "{phase}: SET from e[1]");
    assert_eq!(floats(&row[1]), vec![2.0, 3.0], "{phase}: SET from e[1..3]");
    Ok(())
}

#[tokio::test]
async fn vector_index_slice_on_both_evaluators() -> Result<()> {
    let db = db().await?;
    check_reads(&db, "L0").await?;
    check_writes(&db, "L0").await?;
    db.flush().await?;
    check_reads(&db, "flushed").await?;
    check_writes(&db, "flushed").await
}
