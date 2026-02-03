// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_snapshots_and_time_travel() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // State 1: Alice
    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.flush().await?;
    let snap1_id = db.create_snapshot(Some("alice-only")).await?;

    // State 2: Alice + Bob
    db.execute("CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;
    let _snap2_id = db.create_snapshot(Some("alice-and-bob")).await?;

    // Verify State 2
    let res2 = db.query("MATCH (n:Person) RETURN n.name AS name").await?;
    assert_eq!(res2.len(), 2);

    // Time Travel to State 1 using VERSION AS OF
    let res1 = db
        .query(&format!(
            "MATCH (n:Person) RETURN n.name AS name VERSION AS OF '{}'",
            snap1_id
        ))
        .await?;
    assert_eq!(res1.len(), 1);
    assert_eq!(res1.rows()[0].get::<String>("name")?, "Alice");

    // Verify snapshot list procedure
    let list_res = db.query("CALL uni.admin.snapshot.list()").await?;
    assert!(list_res.len() >= 2);

    let mut found_alice = false;
    for row in list_res.rows() {
        if row.get::<String>("name").is_ok() && row.get::<String>("name")? == "alice-only" {
            found_alice = true;
            assert_eq!(row.get::<String>("snapshot_id")?, snap1_id);
        }
    }
    assert!(found_alice);

    // Test Restore
    db.execute(&format!("CALL uni.admin.snapshot.restore('{}')", snap1_id))
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_snapshot_edge_isolation() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    // State 1: Alice -> Bob
    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await?;
    db.flush().await?;
    let snap1 = db.create_snapshot(Some("knows-bob")).await?;

    // State 2: Add Alice -> Charlie
    db.execute("MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(:Person {name: 'Charlie'})")
        .await?;
    db.flush().await?;

    // Current state should see 2 friends
    let current = db
        .query("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(f) RETURN f.name AS friend_name")
        .await?;
    assert_eq!(current.len(), 2);

    // Time travel to snap1 should see only 1 friend
    let past = db
        .query(&format!(
            "MATCH (a:Person {{name: 'Alice'}})-[:KNOWS]->(f) RETURN f.name AS friend_name VERSION AS OF '{}'",
            snap1
        ))
        .await?;
    assert_eq!(past.len(), 1);
    assert_eq!(past.rows()[0].get::<String>("friend_name")?, "Bob");

    Ok(())
}

#[tokio::test]
async fn test_snapshot_property_isolation() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    // State 1: Alice age 30
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;
    db.flush().await?;
    let snap1 = db.create_snapshot(Some("alice-30")).await?;

    // State 2: Update age to 31
    db.execute("MATCH (n:Person {name: 'Alice'}) SET n.age = 31")
        .await?;
    db.flush().await?;

    // Current query should see age 31
    let current = db
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.age AS age")
        .await?;
    assert_eq!(current.rows()[0].get::<i64>("age")?, 31);

    // Time travel should see age 30
    let past = db
        .query(&format!(
            "MATCH (n:Person {{name: 'Alice'}}) RETURN n.age AS age VERSION AS OF '{}'",
            snap1
        ))
        .await?;
    assert_eq!(past.rows()[0].get::<i64>("age")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_time_travel_rejects_writes() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.flush().await?;
    let snap = db.create_snapshot(Some("snap1")).await?;

    // CREATE with VERSION AS OF should fail
    let result = db
        .execute(&format!(
            "CREATE (:Person {{name: 'Eve'}}) VERSION AS OF '{}'",
            snap
        ))
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Write clauses"),
        "Expected write clause error, got: {}",
        err_msg
    );

    Ok(())
}

#[tokio::test]
async fn test_time_travel_no_snapshot_error() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // TIMESTAMP AS OF a time before any snapshots should fail
    let result = db
        .query("MATCH (n:Person) RETURN n TIMESTAMP AS OF '2020-01-01T00:00:00Z'")
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("No snapshot found"),
        "Expected no-snapshot error, got: {}",
        err_msg
    );

    Ok(())
}

#[tokio::test]
async fn test_timestamp_as_of_happy_path() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // State 1: Alice
    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.flush().await?;
    let _snap1 = db.create_snapshot(Some("snap1")).await?;

    // Record a timestamp after snap1 but before snap2
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let between = chrono::Utc::now().to_rfc3339();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // State 2: Alice + Bob
    db.execute("CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;
    let _snap2 = db.create_snapshot(Some("snap2")).await?;

    // Current state sees both
    let current = db.query("MATCH (n:Person) RETURN n.name AS name").await?;
    assert_eq!(current.len(), 2);

    // TIMESTAMP AS OF between snap1 and snap2 should resolve to snap1 (Alice only)
    let past = db
        .query(&format!(
            "MATCH (n:Person) RETURN n.name AS name TIMESTAMP AS OF '{}'",
            between
        ))
        .await?;
    assert_eq!(past.len(), 1, "Expected 1 row at snap1, got {}", past.len());
    assert_eq!(past.rows()[0].get::<String>("name")?, "Alice");

    Ok(())
}
