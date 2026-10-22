// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `SET` of a declared property inside a unit `CALL { }` subquery, for every
//! declared type.
//!
//! The subquery re-emits the outer row with the post-`SET` entity, rebuilding
//! each column as a one-row Arrow array of its declared type. That conversion
//! knew only a handful of Arrow types, so a declared `MAP` property (Arrow
//! `List<Struct<key, value>>`) hit an `unhandled DataType` debug assertion --
//! and in a release build a `Utf8` column where the batch expected the declared
//! type. `List<Int>`, `Vector`, the temporal types and `Float32` reach the same
//! fallback. Each type is checked here against the same `SET` run without the
//! subquery.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(call_subquery_set_declared_types)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

/// `(property, declared type, Cypher literal)` -- one per Arrow shape.
fn cases() -> Vec<(&'static str, DataType, &'static str)> {
    vec![
        (
            "m",
            DataType::Map(Box::new(DataType::String), Box::new(DataType::Int64)),
            "{a: 2, b: 3}",
        ),
        ("li", DataType::List(Box::new(DataType::Int64)), "[4, 5, 6]"),
        (
            "lf",
            DataType::List(Box::new(DataType::Float64)),
            "[1.5, 2.5]",
        ),
        (
            "ls",
            DataType::List(Box::new(DataType::String)),
            "['x', 'y']",
        ),
        ("vec", DataType::Vector { dimensions: 2 }, "[0.5, 0.25]"),
        ("dt", DataType::DateTime, "datetime('2024-01-02T03:04:05Z')"),
        ("d", DataType::Date, "date('2024-01-02')"),
        ("t", DataType::Time, "time('10:11:12+01:00')"),
        ("dur", DataType::Duration, "duration('P1DT2H')"),
        ("f32", DataType::Float32, "1.25"),
        ("i32", DataType::Int32, "7"),
        ("b", DataType::Bool, "true"),
        ("s", DataType::String, "'text'"),
    ]
}

async fn open() -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    let mut label = db.schema().label("F").property("k", DataType::Int);
    for (name, ty, _) in cases() {
        label = label.property_nullable(name, ty);
    }
    label.apply().await?;
    let tx = db.session().tx().await?;
    // k = i: set inside a CALL subquery; k = 100 + i: the plain-SET control.
    for (i, _) in cases().iter().enumerate() {
        tx.execute(&format!("CREATE (:F {{k: {i}}}), (:F {{k: {}}})", 100 + i))
            .await?;
    }
    tx.commit().await?;
    Ok(db)
}

async fn single(db: &Uni, cypher: &str) -> Result<Value> {
    let tx = db.session().tx().await?;
    let result = tx
        .query(cypher)
        .await
        .map_err(|e| anyhow::anyhow!("{cypher}: {e}"))?;
    tx.commit().await?;
    assert_eq!(result.rows().len(), 1, "{cypher}");
    Ok(result.rows()[0].values()[0].clone())
}

fn prop(node: &Value, name: &str, ctx: &str) -> Value {
    match node {
        Value::Node(n) => n.properties.get(name).cloned().unwrap_or(Value::Null),
        other => panic!("{ctx}: expected a node, got {other:?}"),
    }
}

async fn assert_every_type(db: &Uni, phase: &str) -> Result<()> {
    for (i, (name, _, literal)) in cases().into_iter().enumerate() {
        let ctx = format!("{phase}/{name}");
        let control = single(
            db,
            &format!(
                "MATCH (n:F {{k: {}}}) SET n.{name} = {literal} RETURN n.{name} AS v",
                100 + i
            ),
        )
        .await?;
        assert_ne!(
            control,
            Value::Null,
            "{ctx}: control SET must store a value"
        );

        let via_call = single(
            db,
            &format!(
                "MATCH (n:F {{k: {i}}}) CALL {{ WITH n SET n.{name} = {literal} }} \
                 RETURN n.{name} AS v"
            ),
        )
        .await?;
        assert_eq!(
            via_call, control,
            "{ctx}: property read after CALL {{ SET }}"
        );

        // The whole entity crosses the same boundary as a struct column.
        let node = single(
            db,
            &format!("MATCH (n:F {{k: {i}}}) CALL {{ WITH n SET n.{name} = {literal} }} RETURN n"),
        )
        .await?;
        assert_eq!(
            prop(&node, name, &ctx),
            control,
            "{ctx}: entity after CALL {{ SET }}"
        );

        let stored = single(db, &format!("MATCH (n:F {{k: {i}}}) RETURN n.{name} AS v")).await?;
        assert_eq!(stored, control, "{ctx}: stored value");
    }
    Ok(())
}

#[tokio::test]
async fn call_subquery_set_declared_types_unflushed() -> Result<()> {
    let db = open().await?;
    assert_every_type(&db, "L0").await
}

#[tokio::test]
async fn call_subquery_set_declared_types_flushed() -> Result<()> {
    let db = open().await?;
    db.flush().await?;
    assert_every_type(&db, "flushed").await
}
