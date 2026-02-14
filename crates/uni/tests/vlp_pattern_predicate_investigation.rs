// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Investigation tests for VLP pattern predicates in WHERE clauses.
//! These document current behavior and pin expected semantics for fixes.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn control_single_hop_pattern_predicate_schema() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("A").apply().await?;
    db.schema().label("B").apply().await?;
    db.schema().edge_type("REL", &[], &[]).apply().await?;
    db.execute("CREATE (a:A)-[:REL]->(b:B)").await?;

    let rows = db
        .query("MATCH (n), (m) WHERE (n)-[:REL]->(m) RETURN n, m")
        .await?;
    assert_eq!(
        rows.len(),
        1,
        "single-hop schema predicate should match A->B"
    );
    Ok(())
}

#[tokio::test]
async fn regression_vlp_pattern_predicate_schema_bound_target() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("A").apply().await?;
    db.schema().label("B").apply().await?;
    db.schema().label("C").apply().await?;
    db.schema().edge_type("REL", &[], &[]).apply().await?;
    db.execute("CREATE (a:A)-[:REL]->(b:B)-[:REL]->(c:C)")
        .await?;

    let rows = db
        .query("MATCH (n), (m) WHERE (n)-[:REL*1..2]->(m) RETURN n, m")
        .await?;
    assert_eq!(rows.len(), 3, "expected A->B, B->C, A->C");
    Ok(())
}

#[tokio::test]
async fn regression_vlp_pattern_predicate_schemaless_bound_target() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:A)-[:REL]->(b:B)-[:REL]->(c:C)")
        .await?;

    let rows = db
        .query("MATCH (n), (m) WHERE (n)-[:REL*1..2]->(m) RETURN n, m")
        .await?;
    assert_eq!(rows.len(), 3, "expected A->B, B->C, A->C");
    Ok(())
}

#[tokio::test]
async fn regression_vlp_pattern_predicate_type_filter() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("A").apply().await?;
    db.schema().label("B").apply().await?;
    db.schema()
        .label("C")
        .property("dummy", DataType::Int64)
        .apply()
        .await?;
    db.schema().edge_type("REL1", &[], &[]).apply().await?;
    db.schema().edge_type("REL2", &[], &[]).apply().await?;
    db.execute("CREATE (a:A)-[:REL1]->(b:B), (a)-[:REL2]->(c:C {dummy: 1})")
        .await?;

    let rows = db
        .query("MATCH (n), (m) WHERE (n)-[:REL1*1..1]->(m) RETURN n, m")
        .await?;
    assert_eq!(rows.len(), 1, "only A->B should match REL1");
    Ok(())
}
