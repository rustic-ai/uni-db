// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for procedure calls routed through the DataFusion engine.
//!
//! These tests exercise composite queries where a `CALL` procedure is followed
//! by `MATCH` or other clauses, ensuring the `GraphProcedureCallExec` custom
//! DataFusion execution plan works correctly.

use anyhow::Result;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_db::Uni;

// ---------------------------------------------------------------------------
// Happy path: uni.schema.labels() in composite queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_schema_labels_composite_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").label("Animal").apply().await?;
    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.execute("CREATE (:Animal {name: 'Rex'})").await?;

    // CALL uni.schema.labels() YIELD label MATCH (n:Person) WHERE label = 'Person'
    let result = db
        .query(
            "CALL uni.schema.labels() YIELD label
             MATCH (n:Person) WHERE label = 'Person'
             RETURN n.name AS name, label",
        )
        .await?;

    assert_eq!(result.len(), 1, "Only Person label matches the filter");
    let name: String = result.rows[0].get("name")?;
    assert_eq!(name, "Alice");
    let label: String = result.rows[0].get("label")?;
    assert_eq!(label, "Person");

    Ok(())
}

#[tokio::test]
async fn test_schema_labels_composite_no_filter() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").label("Animal").apply().await?;
    db.execute("CREATE (:Person {name: 'Alice'})").await?;

    // Cross join: every label x every Person node
    let result = db
        .query(
            "CALL uni.schema.labels() YIELD label
             MATCH (n:Person)
             RETURN n.name AS name, label",
        )
        .await?;

    // Two labels (Person, Animal) x 1 Person node = 2 rows
    assert_eq!(result.len(), 2);
    let mut labels: Vec<String> = result
        .rows
        .iter()
        .map(|r| r.get::<String>("label").unwrap())
        .collect();
    labels.sort();
    assert_eq!(labels, vec!["Animal", "Person"]);

    Ok(())
}

#[tokio::test]
async fn test_schema_labels_yield_alias_composite() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").apply().await?;
    db.execute("CREATE (:Person {name: 'Bob'})").await?;

    // Test YIELD aliasing in composite query
    let result = db
        .query(
            "CALL uni.schema.labels() YIELD label AS lbl
             MATCH (n:Person) WHERE lbl = 'Person'
             RETURN n.name AS name, lbl",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let lbl: String = result.rows[0].get("lbl")?;
    assert_eq!(lbl, "Person");

    Ok(())
}

#[tokio::test]
async fn test_schema_labels_composite_empty_result() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").apply().await?;
    db.execute("CREATE (:Person {name: 'Alice'})").await?;

    // Filter on a label that doesn't exist: cross join produces rows
    // but WHERE filters them all out
    let result = db
        .query(
            "CALL uni.schema.labels() YIELD label
             MATCH (n:Person) WHERE label = 'NonExistent'
             RETURN n.name, label",
        )
        .await?;

    assert_eq!(result.len(), 0, "No label matches 'NonExistent'");

    Ok(())
}

#[tokio::test]
async fn test_schema_labels_multiple_yield_columns() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").apply().await?;
    db.execute("CREATE (:Person {name: 'Alice'})").await?;

    // Yield multiple columns from schema procedure in a composite query
    let result = db
        .query(
            "CALL uni.schema.labels() YIELD label, nodeCount
             MATCH (n:Person) WHERE label = 'Person'
             RETURN label, nodeCount",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let label: String = result.rows[0].get("label")?;
    assert_eq!(label, "Person");
    // nodeCount column exists and is an integer (may be 0 for in-memory unflushed data)
    let count: i64 = result.rows[0].get("nodeCount")?;
    assert!(
        count >= 0,
        "nodeCount should be non-negative, got {}",
        count
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Happy path: uni.schema.edgeTypes() in composite queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_schema_edge_types_composite() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    db.execute("CREATE (a:Person {name: 'Alice'})").await?;
    db.execute("CREATE (b:Person {name: 'Bob'})").await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;

    // CALL schema.edgeTypes() YIELD type MATCH on that edge type
    let result = db
        .query(
            "CALL uni.schema.edgeTypes() YIELD type AS edgeType
             MATCH (a:Person)-[:KNOWS]->(b:Person)
             WHERE edgeType = 'KNOWS'
             RETURN a.name AS src, b.name AS dst, edgeType",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let src: String = result.rows[0].get("src")?;
    let dst: String = result.rows[0].get("dst")?;
    assert_eq!(src, "Alice");
    assert_eq!(dst, "Bob");

    Ok(())
}

// ---------------------------------------------------------------------------
// Happy path: uni.vector.query() in composite queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_vector_query_composite_with_match() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc")?;
    schema_manager.add_property("Doc", "title", DataType::String, false)?;
    schema_manager.add_property(
        "Doc",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.add_label("Tag")?;
    schema_manager.add_property("Tag", "name", DataType::String, false)?;
    schema_manager.add_edge_type("TAGGED", vec!["Doc".into()], vec!["Tag".into()])?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.execute("CREATE (d:Doc {title: 'ML Paper', embedding: [1.0, 0.0]})")
        .await?;
    db.execute("CREATE (t:Tag {name: 'machine-learning'})")
        .await?;
    db.execute(
        "MATCH (d:Doc {title: 'ML Paper'}), (t:Tag {name: 'machine-learning'}) CREATE (d)-[:TAGGED]->(t)",
    )
    .await?;
    db.flush().await?;

    // Composite: vector search + graph traversal
    let result = db
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 5) YIELD node, distance
             MATCH (node:Doc)-[:TAGGED]->(t:Tag)
             RETURN node.title AS title, t.name AS tag, distance",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let title: String = result.rows[0].get("title")?;
    let tag: String = result.rows[0].get("tag")?;
    assert_eq!(title, "ML Paper");
    assert_eq!(tag, "machine-learning");

    Ok(())
}

#[tokio::test]
async fn test_vector_query_composite_yield_alias() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Item")?;
    schema_manager.add_property("Item", "name", DataType::String, false)?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.add_label("Category")?;
    schema_manager.add_property("Category", "name", DataType::String, false)?;
    schema_manager.add_edge_type("IN_CATEGORY", vec!["Item".into()], vec!["Category".into()])?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.execute("CREATE (i:Item {name: 'Widget', embedding: [1.0, 0.0]})")
        .await?;
    db.execute("CREATE (c:Category {name: 'Gadgets'})").await?;
    db.execute(
        "MATCH (i:Item {name: 'Widget'}), (c:Category {name: 'Gadgets'}) CREATE (i)-[:IN_CATEGORY]->(c)",
    )
    .await?;
    db.flush().await?;

    // YIELD with alias: p instead of node, dist instead of distance
    let result = db
        .query(
            "CALL uni.vector.query('Item', 'embedding', [1.0, 0.0], 5) YIELD p, dist
             MATCH (p:Item)-[:IN_CATEGORY]->(c:Category)
             RETURN p.name AS item_name, c.name AS cat_name, dist",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let item_name: String = result.rows[0].get("item_name")?;
    let cat_name: String = result.rows[0].get("cat_name")?;
    assert_eq!(item_name, "Widget");
    assert_eq!(cat_name, "Gadgets");

    Ok(())
}

#[tokio::test]
async fn test_vector_query_composite_multiple_yields() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc")?;
    schema_manager.add_property("Doc", "title", DataType::String, false)?;
    schema_manager.add_property(
        "Doc",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.execute("CREATE (d:Doc {title: 'Close', embedding: [0.99, 0.01]})")
        .await?;
    db.execute("CREATE (d:Doc {title: 'Far', embedding: [0.0, 1.0]})")
        .await?;
    db.flush().await?;

    // Yield both node and distance together, then filter in WHERE
    let result = db
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 10) YIELD node, distance
             MATCH (node:Doc)
             WHERE distance < 0.5
             RETURN node.title AS title, distance",
        )
        .await?;

    // Only the close doc should pass the distance filter
    assert!(!result.is_empty());
    for row in result.rows() {
        let dist: f64 = row.get("distance")?;
        assert!(dist < 0.5, "Distance should be < 0.5, got {}", dist);
    }

    Ok(())
}

#[tokio::test]
async fn test_vector_query_composite_empty_results() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc")?;
    schema_manager.add_property("Doc", "title", DataType::String, false)?;
    schema_manager.add_property(
        "Doc",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.add_label("Tag")?;
    schema_manager.add_property("Tag", "name", DataType::String, false)?;
    schema_manager.add_edge_type("HAS_TAG", vec!["Doc".into()], vec!["Tag".into()])?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    // Insert a doc WITHOUT any TAGGED edges
    db.execute("CREATE (d:Doc {title: 'Orphan', embedding: [1.0, 0.0]})")
        .await?;
    db.flush().await?;

    // Vector search finds the doc, but MATCH for edges yields nothing
    let result = db
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 5) YIELD node
             MATCH (node:Doc)-[:HAS_TAG]->(t:Tag)
             RETURN node.title AS title, t.name AS tag",
        )
        .await?;

    assert_eq!(result.len(), 0, "No edges means empty result");

    Ok(())
}

// ---------------------------------------------------------------------------
// Sad path: error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_unknown_procedure_in_composite_query() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").apply().await?;
    db.execute("CREATE (:Person {name: 'Alice'})").await?;

    let result = db
        .query(
            "CALL uni.nonexistent.procedure() YIELD x
             MATCH (n:Person)
             RETURN n.name, x",
        )
        .await;

    assert!(result.is_err(), "Unknown procedure should fail");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not supported")
            || err_msg.contains("not found")
            || err_msg.contains("Unknown"),
        "Error should mention unknown procedure, got: {}",
        err_msg
    );

    Ok(())
}

#[tokio::test]
async fn test_schema_labels_no_labels_composite() -> Result<()> {
    // Fresh DB with no labels at all
    let db = Uni::in_memory().build().await?;

    let result = db
        .query(
            "CALL uni.schema.labels() YIELD label
             RETURN label",
        )
        .await?;

    // No labels means empty result (but no error)
    assert_eq!(result.len(), 0);

    Ok(())
}

// ---------------------------------------------------------------------------
// Edge case: standalone CALL still works (regression check)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_standalone_call_still_works() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").apply().await?;

    // Standalone: goes through legacy executor, not DataFusion
    let result = db
        .query("CALL uni.schema.labels() YIELD label RETURN label")
        .await?;

    assert!(!result.is_empty());
    let labels: Vec<String> = result
        .rows
        .iter()
        .map(|r| r.get::<String>("label").unwrap())
        .collect();
    assert!(labels.contains(&"Person".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_standalone_call_with_alias_still_works() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").apply().await?;

    let result = db
        .query("CALL uni.schema.labels() YIELD label AS l RETURN l")
        .await?;

    assert!(!result.is_empty());
    assert!(result.columns().contains(&"l".to_string()));

    Ok(())
}

// ---------------------------------------------------------------------------
// Edge case: multiple labels with nodeCount
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_schema_labels_composite_with_counts() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Person").label("Company").apply().await?;
    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.execute("CREATE (:Person {name: 'Bob'})").await?;
    db.execute("CREATE (:Company {name: 'Acme'})").await?;

    // Use multiple yield columns and filter
    let result = db
        .query(
            "CALL uni.schema.labels() YIELD label, nodeCount
             MATCH (n:Person)
             WHERE label = 'Person'
             RETURN DISTINCT label, nodeCount",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let label: String = result.rows[0].get("label")?;
    assert_eq!(label, "Person");

    Ok(())
}
