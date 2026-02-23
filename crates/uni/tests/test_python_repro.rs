// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Reproduction tests for Rust-side bugs exposed by Python bindings.
//!
//! These tests reproduce 3 bugs found through 71 Python binding test failures:
//!
//! 1. `CREATE ... RETURN id(n)` fails with DataFusion schema error
//! 2. `execute()` returns 0 affected rows for mutations without RETURN
//! 3. `list_labels()` ignores schema-registered labels without vertices

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

// ---------------------------------------------------------------------------
// Bug 1: CREATE ... RETURN id(n) fails with DataFusion schema error
//
// The expression translator in df_expr.rs converts id(n) to
// DfExpr::Column("n._vid"), but CREATE's output schema only has a bare `n`
// column. This causes: "Schema error: No field named n._vid"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_create_node_return_id() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Person (name STRING)").await?;

    let result = db
        .query("CREATE (n:Person {name: 'Alice'}) RETURN id(n) AS vid")
        .await?;

    assert_eq!(result.len(), 1, "Should return exactly 1 row");
    let vid = result.rows()[0].get::<i64>("vid")?;
    assert!(vid >= 0, "VID should be a non-negative integer, got {vid}");

    Ok(())
}

#[tokio::test]
async fn test_create_edge_return_id() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Person (name STRING)").await?;
    db.execute("CREATE EDGE TYPE KNOWS FROM Person TO Person")
        .await?;

    let result = db
        .query(
            "CREATE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) \
             RETURN id(r) AS eid",
        )
        .await?;

    assert_eq!(result.len(), 1, "Should return exactly 1 row");
    let eid = result.rows()[0].get::<i64>("eid")?;
    assert!(eid >= 0, "EID should be a non-negative integer, got {eid}");

    Ok(())
}

#[tokio::test]
async fn test_create_node_return_id_and_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Person (name STRING)").await?;

    let result = db
        .query("CREATE (n:Person {name: 'Alice'}) RETURN id(n) AS vid, n.name AS name")
        .await?;

    assert_eq!(result.len(), 1, "Should return exactly 1 row");
    let vid = result.rows()[0].get::<i64>("vid")?;
    assert!(vid >= 0, "VID should be a non-negative integer, got {vid}");
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");

    Ok(())
}

// ---------------------------------------------------------------------------
// Bug 2: execute() returns 0 affected rows for mutations
//
// execute() sets affected_rows = result.len() (row count from QueryResult),
// but mutations without RETURN produce 0 result rows. The affected_rows field
// should reflect the number of entities actually created/updated/deleted.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_execute_create_returns_affected_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Person (name STRING)").await?;

    let result = db.execute("CREATE (:Person {name: 'Alice'})").await?;

    assert!(
        result.affected_rows >= 1,
        "CREATE of 1 node should report affected_rows >= 1, got {}",
        result.affected_rows,
    );

    Ok(())
}

#[tokio::test]
async fn test_execute_create_multiple_returns_affected_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Person (name STRING)").await?;

    let result = db
        .execute("CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})")
        .await?;

    assert!(
        result.affected_rows >= 2,
        "CREATE of 2 nodes should report affected_rows >= 2, got {}",
        result.affected_rows,
    );

    Ok(())
}

#[tokio::test]
async fn test_execute_delete_returns_affected_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Person (name STRING)").await?;
    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.execute("CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;

    let result = db.execute("MATCH (n:Person) DELETE n").await?;

    assert!(
        result.affected_rows >= 1,
        "DELETE should report affected_rows >= 1, got {}",
        result.affected_rows,
    );

    Ok(())
}

#[tokio::test]
async fn test_execute_set_returns_affected_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .await?;
    db.flush().await?;

    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 30")
        .await?;

    assert!(
        result.affected_rows >= 1,
        "SET should report affected_rows >= 1, got {}",
        result.affected_rows,
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Bug 3: list_labels() ignores schema-registered labels
//
// list_labels() runs MATCH (n) RETURN DISTINCT labels(n), which only returns
// labels with existing vertices. After schema().label("Person").apply() with
// no vertices created, list_labels() returns an empty list. In contrast,
// list_edge_types() reads directly from the schema and returns all registered
// types regardless of whether edges exist.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_labels_includes_schema_labels() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Register a label via the schema builder without creating any vertices.
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    let labels = db.list_labels().await?;

    assert!(
        labels.contains(&"Person".to_string()),
        "list_labels() should include schema-registered label 'Person', got {labels:?}",
    );

    Ok(())
}

#[tokio::test]
async fn test_list_labels_after_schema_then_create() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Step 1: Register the label — should appear even without vertices.
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    let labels = db.list_labels().await?;
    assert!(
        labels.contains(&"Person".to_string()),
        "After schema registration, 'Person' should appear in list_labels(), got {labels:?}",
    );

    // Step 2: Create a vertex — label should still appear.
    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.flush().await?;

    let labels = db.list_labels().await?;
    assert!(
        labels.contains(&"Person".to_string()),
        "After creating a vertex, 'Person' should appear in list_labels(), got {labels:?}",
    );

    Ok(())
}

#[tokio::test]
async fn test_list_edge_types_vs_list_labels_consistency() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Register both a label and an edge type with no data created.
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    // list_edge_types() reads from schema — should include "KNOWS".
    let edge_types = db.list_edge_types().await?;
    assert!(
        edge_types.contains(&"KNOWS".to_string()),
        "list_edge_types() should include 'KNOWS', got {edge_types:?}",
    );

    // list_labels() should be consistent and include "Person".
    let labels = db.list_labels().await?;
    assert!(
        labels.contains(&"Person".to_string()),
        "list_labels() should be consistent with list_edge_types() and include schema-registered \
         labels, got {labels:?}",
    );

    Ok(())
}
