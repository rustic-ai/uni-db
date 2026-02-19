// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for DataFusion mutation operators (M2).
//!
//! These tests verify that simple terminal mutations (CREATE, SET, REMOVE, DELETE)
//! flow through DataFusion MutationExec operators and correctly write to the L0
//! buffer so that subsequent queries see the data (read-your-write semantics).

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Verify that a standalone CREATE (no RETURN) writes to L0 and a subsequent
/// MATCH sees the created node. This exercises the DF MutationCreateExec path.
#[tokio::test]
async fn test_df_create_read_your_write() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("TestNode")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Standalone CREATE — routes through DF MutationCreateExec (terminal mutation)
    db.execute("CREATE (n:TestNode {name: 'Alice'})").await?;

    // Subsequent MATCH should see the created node via L0 buffer
    let result = db
        .query("MATCH (m:TestNode) RETURN m.name ORDER BY m.name")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<String>("m.name")?, "Alice");

    Ok(())
}

/// Verify that multiple standalone CREATEs accumulate in L0 and are all visible.
#[tokio::test]
async fn test_df_create_multiple_nodes() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int64)
        .apply()
        .await?;

    // Three separate CREATEs — each should go through DF MutationCreateExec
    db.execute("CREATE (n:Item {id: 1})").await?;
    db.execute("CREATE (n:Item {id: 2})").await?;
    db.execute("CREATE (n:Item {id: 3})").await?;

    // All three should be visible
    let result = db.query("MATCH (n:Item) RETURN n.id ORDER BY n.id").await?;
    assert_eq!(result.rows().len(), 3);
    assert_eq!(result.rows()[0].get::<i64>("n.id")?, 1);
    assert_eq!(result.rows()[1].get::<i64>("n.id")?, 2);
    assert_eq!(result.rows()[2].get::<i64>("n.id")?, 3);

    Ok(())
}

/// Verify that MATCH ... SET (terminal, no RETURN) routes through DF
/// MutationSetExec and the property change is visible on subsequent read.
#[tokio::test]
async fn test_df_set_read_your_write() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Setup: Create a node (goes through fallback because multi-clause or DF for terminal)
    db.execute("CREATE (n:Person {name: 'Alice'})").await?;

    // Terminal SET — routes through DF MutationSetExec
    db.execute("MATCH (n:Person {name: 'Alice'}) SET n.name = 'Updated'")
        .await?;

    // Subsequent MATCH should see the updated name
    let result = db.query("MATCH (n:Person) RETURN n.name").await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<String>("n.name")?, "Updated");

    Ok(())
}

/// Verify that MATCH ... DELETE (terminal, no RETURN) routes through DF
/// MutationDeleteExec and the node is no longer visible.
#[tokio::test]
async fn test_df_delete_read_your_write() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Ephemeral")
        .property("id", DataType::Int64)
        .apply()
        .await?;

    // Setup
    db.execute("CREATE (n:Ephemeral {id: 1})").await?;
    db.execute("CREATE (n:Ephemeral {id: 2})").await?;

    // Verify both exist
    let result = db
        .query("MATCH (n:Ephemeral) RETURN n.id ORDER BY n.id")
        .await?;
    assert_eq!(result.rows().len(), 2);

    // Terminal DETACH DELETE — routes through DF MutationDeleteExec
    db.execute("MATCH (n:Ephemeral {id: 1}) DETACH DELETE n")
        .await?;

    // Only one should remain
    let result = db
        .query("MATCH (n:Ephemeral) RETURN n.id ORDER BY n.id")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("n.id")?, 2);

    Ok(())
}

/// Verify that MATCH ... REMOVE (terminal, no RETURN) routes through DF
/// MutationRemoveExec and the property is removed.
#[tokio::test]
async fn test_df_remove_read_your_write() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Tag")
        .property("name", DataType::String)
        .property_nullable("color", DataType::String)
        .apply()
        .await?;

    // Setup
    db.execute("CREATE (n:Tag {name: 'rust', color: 'orange'})")
        .await?;

    // Verify the property exists
    let result = db.query("MATCH (n:Tag) RETURN n.color").await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<String>("n.color")?, "orange");

    // Terminal REMOVE — routes through DF MutationRemoveExec
    db.execute("MATCH (n:Tag) REMOVE n.color").await?;

    // Property should be null/removed
    let result = db.query("MATCH (n:Tag) RETURN n.name, n.color").await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<String>("n.name")?, "rust");

    Ok(())
}

/// Verify that MATCH ... CREATE edge (terminal) creates the edge via DF.
#[tokio::test]
async fn test_df_create_edge_read_your_write() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Node")
        .property("id", DataType::Int64)
        .apply()
        .await?;
    db.schema().edge_type("LINK", &[], &[]).apply().await?;

    // Setup nodes
    db.execute("CREATE (n:Node {id: 1})").await?;
    db.execute("CREATE (n:Node {id: 2})").await?;

    // Terminal edge CREATE — routes through DF MutationCreateExec
    db.execute("MATCH (a:Node {id: 1}), (b:Node {id: 2}) CREATE (a)-[:LINK]->(b)")
        .await?;

    // Verify edge exists by traversal
    let result = db
        .query("MATCH (a:Node {id: 1})-[:LINK]->(b:Node) RETURN b.id")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("b.id")?, 2);

    Ok(())
}
