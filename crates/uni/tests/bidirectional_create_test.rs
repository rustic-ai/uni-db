// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_incoming_relationship_create() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema
    db.execute("CREATE LABEL A (name STRING)").await?;
    db.execute("CREATE LABEL B (name STRING)").await?;
    db.execute("CREATE EDGE TYPE KNOWS (since INT) FROM B TO A")
        .await?;

    // Create pattern with incoming relationship: (a)<-[:KNOWS]-(b)
    // This should create edge from b -> a
    db.execute("CREATE (a:A {name: 'Alice'})<-[:KNOWS {since: 2020}]-(b:B {name: 'Bob'})")
        .await?;

    // Query in outgoing direction: Bob -> Alice
    let result = db
        .query("MATCH (b:B)-[:KNOWS]->(a:A) RETURN b.name AS from, a.name AS to")
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("from")?, "Bob");
    assert_eq!(result.rows()[0].get::<String>("to")?, "Alice");

    Ok(())
}

#[tokio::test]
async fn test_mixed_directions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create labels only - use schemaless edge types
    db.execute("CREATE LABEL A (id INT)").await?;
    db.execute("CREATE LABEL B (id INT)").await?;
    db.execute("CREATE LABEL C (id INT)").await?;

    // Create pattern with mixed directions: (a)<-[:ADMIN]-(b)-[:ADMIN]->(c)
    // Should create: b -> a and b -> c
    db.execute("CREATE (a:A {id: 0})<-[:ADMIN]-(b:B {id: 1})-[:ADMIN]->(c:C {id: 2})")
        .await?;

    // Verify both edges exist
    let result = db
        .query("MATCH (b:B)-[:ADMIN]->(x) RETURN x.id AS target_id")
        .await?;

    assert_eq!(result.len(), 2);
    // Collect target IDs
    let mut ids: Vec<i64> = result
        .rows()
        .iter()
        .map(|r| r.get::<i64>("target_id").unwrap())
        .collect();
    ids.sort();
    // Verify we have edges to both a (id=0) and c (id=2)
    assert_eq!(ids, vec![0, 2]);

    Ok(())
}

#[tokio::test]
async fn test_incoming_with_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema
    db.execute("CREATE LABEL Person (name STRING)").await?;
    db.execute("CREATE EDGE TYPE FOLLOWS (since INT) FROM Person TO Person")
        .await?;

    // Create incoming relationship with properties
    db.execute(
        "CREATE (:Person {name: 'Alice'})<-[:FOLLOWS {since: 2021}]-(:Person {name: 'Bob'})",
    )
    .await?;

    // Query to verify edge direction and properties
    let result = db
        .query("MATCH (follower:Person)-[r:FOLLOWS]->(followed:Person) RETURN follower.name, followed.name, r.since")
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("follower.name")?, "Bob");
    assert_eq!(result.rows()[0].get::<String>("followed.name")?, "Alice");
    assert_eq!(result.rows()[0].get::<i64>("r.since")?, 2021);

    Ok(())
}
