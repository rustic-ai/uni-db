// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for bulk loading API including vertices and edges.

use anyhow::Result;
use std::collections::HashMap;
use uni_db::Uni;
use uni_db::api::bulk::EdgeData;
use uni_db::unival;

const SCHEMA_JSON: &str = r#"{
    "schema_version": 1,
    "labels": {
        "Person": {
            "id": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "state": "Active"
        },
        "Company": {
            "id": 2,
            "created_at": "2024-01-01T00:00:00Z",
            "state": "Active"
        }
    },
    "edge_types": {
        "KNOWS": {
            "id": 1,
            "src_labels": ["Person"],
            "dst_labels": ["Person"],
            "state": "Active"
        },
        "WORKS_AT": {
            "id": 2,
            "src_labels": ["Person"],
            "dst_labels": ["Company"],
            "state": "Active"
        }
    },
    "properties": {
        "Person": {
            "name": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" },
            "age": { "type": "Int32", "nullable": true, "added_in": 1, "state": "Active" }
        },
        "Company": {
            "name": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" }
        },
        "KNOWS": {
            "since": { "type": "Int32", "nullable": true, "added_in": 1, "state": "Active" }
        },
        "WORKS_AT": {
            "role": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" }
        }
    },
    "indexes": []
}"#;

async fn setup_db() -> Result<(Uni, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    let schema_path = path.join("schema.json");
    tokio::fs::write(&schema_path, SCHEMA_JSON).await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.load_schema(&schema_path).await?;

    Ok((db, temp_dir))
}

#[tokio::test]
async fn test_bulk_insert_vertices() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(100).build()?;

    // Insert 250 vertices (will trigger multiple flushes with batch_size=100)
    let mut props = Vec::new();
    for i in 0..250 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("Person_{}", i)));
        p.insert("age".to_string(), unival!(i % 100));
        props.push(p);
    }

    let vids = bulk.insert_vertices("Person", props).await?;
    assert_eq!(vids.len(), 250);

    let stats = bulk.commit().await?;
    assert_eq!(stats.vertices_inserted, 250);
    drop(tx);

    // Verify data was persisted
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("c")?, 250);

    Ok(())
}

#[tokio::test]
async fn test_bulk_insert_edges() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    // First create some vertices
    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(100).build()?;

    let mut person_props = Vec::new();
    for i in 0..100 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("Person_{}", i)));
        p.insert("age".to_string(), unival!(20 + i % 50));
        person_props.push(p);
    }
    let person_vids = bulk.insert_vertices("Person", person_props).await?;

    let mut company_props = Vec::new();
    for i in 0..10 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("Company_{}", i)));
        company_props.push(p);
    }
    let company_vids = bulk.insert_vertices("Company", company_props).await?;

    // Create KNOWS edges (person -> person)
    let mut knows_edges = Vec::new();
    for i in 0..50 {
        let mut props = HashMap::new();
        props.insert("since".to_string(), unival!(2020 + (i % 5)));
        knows_edges.push(EdgeData::new(
            person_vids[i],
            person_vids[(i + 1) % 100],
            props,
        ));
    }
    let knows_eids = bulk.insert_edges("KNOWS", knows_edges).await?;
    assert_eq!(knows_eids.len(), 50);

    // Create WORKS_AT edges (person -> company)
    let mut works_edges = Vec::new();
    for i in 0..100 {
        let mut props = HashMap::new();
        props.insert("role".to_string(), unival!(format!("Role_{}", i % 5)));
        works_edges.push(EdgeData::new(person_vids[i], company_vids[i % 10], props));
    }
    let works_eids = bulk.insert_edges("WORKS_AT", works_edges).await?;
    assert_eq!(works_eids.len(), 100);

    let stats = bulk.commit().await?;
    assert_eq!(stats.vertices_inserted, 110); // 100 persons + 10 companies
    assert_eq!(stats.edges_inserted, 150); // 50 KNOWS + 100 WORKS_AT

    Ok(())
}

#[tokio::test]
async fn test_bulk_abort_clears_buffers() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(1000).build()?; // Large batch to avoid flush

    // Insert vertices (won't be flushed due to large batch size)
    let mut props = Vec::new();
    for i in 0..50 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("Person_{}", i)));
        props.push(p);
    }
    let _vids = bulk.insert_vertices("Person", props).await?;

    // Abort instead of commit
    bulk.abort().await?;
    drop(tx);

    // Verify no data was persisted (buffers were cleared before flush)
    // When no dataset exists yet, MATCH returns no rows
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    if result.is_empty() {
        // No dataset exists - abort worked correctly
    } else {
        // Dataset exists but should have 0 rows
        assert_eq!(result.rows()[0].get::<i64>("c")?, 0);
    }

    Ok(())
}

#[tokio::test]
async fn test_bulk_progress_callback() -> Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (db, _temp) = setup_db().await?;

    let progress_count = Arc::new(AtomicUsize::new(0));
    let progress_count_clone = progress_count.clone();

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx
        .bulk_writer()
        .batch_size(50)
        .on_progress(move |_progress| {
            progress_count_clone.fetch_add(1, Ordering::SeqCst);
        })
        .build()?;

    // Insert enough to trigger multiple progress callbacks
    let mut props = Vec::new();
    for i in 0..200 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("Person_{}", i)));
        props.push(p);
    }
    bulk.insert_vertices("Person", props).await?;
    bulk.commit().await?;

    // Should have received multiple progress callbacks
    assert!(progress_count.load(Ordering::SeqCst) > 0);

    Ok(())
}

#[tokio::test]
async fn test_bulk_edge_with_properties() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().build()?;

    // Create two persons
    let p1_props = vec![{
        let mut p = HashMap::new();
        p.insert("name".to_string(), unival!("Alice"));
        p.insert("age".to_string(), unival!(30));
        p
    }];
    let p2_props = vec![{
        let mut p = HashMap::new();
        p.insert("name".to_string(), unival!("Bob"));
        p.insert("age".to_string(), unival!(25));
        p
    }];

    let p1_vids = bulk.insert_vertices("Person", p1_props).await?;
    let p2_vids = bulk.insert_vertices("Person", p2_props).await?;

    // Create edge with properties
    let mut edge_props = HashMap::new();
    edge_props.insert("since".to_string(), unival!(2020));

    let edges = vec![EdgeData::new(p1_vids[0], p2_vids[0], edge_props)];
    let eids = bulk.insert_edges("KNOWS", edges).await?;
    assert_eq!(eids.len(), 1);

    bulk.commit().await?;
    drop(tx);

    // Verify edge exists with property
    // Note: Edge property queries may require specific query patterns
    let result = db
        .session()
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name")
        .await?;
    assert_eq!(result.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_bulk_async_indexes_returns_immediately() -> Result<()> {
    use uni_store::storage::IndexRebuildStatus;

    let (db, _temp) = setup_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx
        .bulk_writer()
        .async_indexes(true)
        .batch_size(100)
        .build()?;

    // Insert vertices
    let mut props = Vec::new();
    for i in 0..100 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("Person_{}", i)));
        p.insert("age".to_string(), unival!(i % 100));
        props.push(p);
    }
    bulk.insert_vertices("Person", props).await?;

    let stats = bulk.commit().await?;
    drop(tx);

    // In async mode, indexes_pending should be true
    assert!(stats.indexes_pending);

    // Data should be queryable immediately (via full scan)
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("c")?, 100);

    // Check initial status - should have pending or in-progress tasks
    let status = db.indexes().rebuild_status().await?;
    // Status may be empty if no indexes defined, or have tasks if indexes exist
    if !status.is_empty() {
        // Verify task structure is correct
        for task in &status {
            assert!(!task.label.is_empty());
            assert!(
                task.status == IndexRebuildStatus::Pending
                    || task.status == IndexRebuildStatus::InProgress
                    || task.status == IndexRebuildStatus::Completed
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_bulk_sync_indexes_blocks() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx
        .bulk_writer()
        .async_indexes(false) // Default behavior
        .batch_size(100)
        .build()?;

    // Insert vertices
    let mut props = Vec::new();
    for i in 0..50 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("Person_{}", i)));
        props.push(p);
    }
    bulk.insert_vertices("Person", props).await?;

    let stats = bulk.commit().await?;
    drop(tx);

    // In sync mode, indexes_pending should be false
    assert!(!stats.indexes_pending);
    assert!(stats.index_task_ids.is_empty());

    // Data should be queryable
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("c")?, 50);

    Ok(())
}

#[tokio::test]
async fn test_index_rebuild_status_tracking() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    // Initially, there should be no tasks
    let status = db.indexes().rebuild_status().await?;
    // After a fresh DB, status may be empty or have loaded state
    let _initial_count = status.len();

    // Use rebuild_indexes to trigger a task
    let task_id = db.indexes().rebuild("Person", true).await?;

    // If a task was created, verify we can track it
    if let Some(tid) = task_id {
        let status = db.indexes().rebuild_status().await?;
        let found = status.iter().any(|t| t.id == tid);
        assert!(found, "Task {} should be in status list", tid);
    }

    Ok(())
}

#[tokio::test]
async fn test_is_index_building() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    // Initially, no indexes should be building
    let status = db.indexes().rebuild_status().await?;
    let is_building = status.iter().any(|t| {
        matches!(
            t.status,
            uni_store::storage::IndexRebuildStatus::Pending
                | uni_store::storage::IndexRebuildStatus::InProgress
        )
    });
    assert!(!is_building);

    // Trigger an async rebuild
    let _task_id = db.indexes().rebuild("Person", true).await?;

    // Note: The task may complete very quickly for an empty dataset
    // So we just verify the API works without asserting the value

    Ok(())
}

// Schema with NOT NULL constraint for constraint tests
const SCHEMA_WITH_CONSTRAINTS: &str = r#"{
    "schema_version": 1,
    "labels": {
        "Person": {
            "id": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "state": "Active"
        }
    },
    "edge_types": {},
    "properties": {
        "Person": {
            "name": { "type": "String", "nullable": false, "added_in": 1, "state": "Active" },
            "email": { "type": "String", "nullable": true, "added_in": 1, "state": "Active" }
        }
    },
    "constraints": [
        {
            "name": "unique_email",
            "target": { "Label": "Person" },
            "constraint_type": { "Unique": { "properties": ["email"] } },
            "enabled": true
        }
    ],
    "indexes": []
}"#;

async fn setup_db_with_constraints() -> Result<(Uni, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();

    let schema_path = path.join("schema.json");
    tokio::fs::write(&schema_path, SCHEMA_WITH_CONSTRAINTS).await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.load_schema(&schema_path).await?;

    Ok((db, temp_dir))
}

#[tokio::test]
async fn test_bulk_not_null_constraint() -> Result<()> {
    let (db, _temp) = setup_db_with_constraints().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().validate_constraints(true).build()?;

    // Try to insert without required "name" field - should fail
    let props = vec![{
        let mut p = HashMap::new();
        // Missing "name" which is NOT NULL
        p.insert("email".to_string(), unival!("test@example.com"));
        p
    }];

    let result = bulk.insert_vertices("Person", props).await;
    assert!(result.is_err(), "Expected NOT NULL constraint violation");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("NOT NULL") || err_msg.contains("cannot be null"),
        "Error should mention NOT NULL: {}",
        err_msg
    );

    Ok(())
}

#[tokio::test]
async fn test_bulk_not_null_constraint_with_explicit_null() -> Result<()> {
    let (db, _temp) = setup_db_with_constraints().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().validate_constraints(true).build()?;

    // Try to insert with explicit null for required field
    let props = vec![{
        let mut p = HashMap::new();
        p.insert("name".to_string(), uni_db::Value::Null); // Explicit null
        p.insert("email".to_string(), unival!("test@example.com"));
        p
    }];

    let result = bulk.insert_vertices("Person", props).await;
    assert!(result.is_err(), "Expected NOT NULL constraint violation");

    Ok(())
}

#[tokio::test]
async fn test_bulk_unique_constraint_in_batch() -> Result<()> {
    let (db, _temp) = setup_db_with_constraints().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().validate_constraints(true).build()?;

    // Insert batch with duplicate emails - should fail
    let props = vec![
        {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!("Alice"));
            p.insert("email".to_string(), unival!("same@example.com"));
            p
        },
        {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!("Bob"));
            p.insert("email".to_string(), unival!("same@example.com")); // Duplicate!
            p
        },
    ];

    let result = bulk.insert_vertices("Person", props).await;
    assert!(result.is_err(), "Expected UNIQUE constraint violation");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("UNIQUE") || err_msg.contains("duplicate"),
        "Error should mention UNIQUE: {}",
        err_msg
    );

    Ok(())
}

#[tokio::test]
async fn test_bulk_unique_constraint_across_batches() -> Result<()> {
    let (db, _temp) = setup_db_with_constraints().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().validate_constraints(true).build()?;

    // First batch succeeds
    let props1 = vec![{
        let mut p = HashMap::new();
        p.insert("name".to_string(), unival!("Alice"));
        p.insert("email".to_string(), unival!("alice@example.com"));
        p
    }];
    bulk.insert_vertices("Person", props1).await?;

    // Second batch with same email should fail (conflicts with buffered data)
    let props2 = vec![{
        let mut p = HashMap::new();
        p.insert("name".to_string(), unival!("Bob"));
        p.insert("email".to_string(), unival!("alice@example.com")); // Same as first batch
        p
    }];

    let result = bulk.insert_vertices("Person", props2).await;
    assert!(
        result.is_err(),
        "Expected UNIQUE violation against buffered data"
    );

    Ok(())
}

#[tokio::test]
async fn test_bulk_abort_after_flush_rollback() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?; // Small batch to force flush

    // Insert enough data to trigger a flush (batch_size=10)
    let mut props = Vec::new();
    for i in 0..25 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("Person_{}", i)));
        p.insert("age".to_string(), unival!(i));
        props.push(p);
    }
    let _vids = bulk.insert_vertices("Person", props).await?;
    // At this point, at least 20 rows should have been flushed to LanceDB

    // Abort the bulk load - should rollback the flushed data via LanceDB version
    bulk.abort().await?;
    drop(tx);

    // Verify no data remains
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    if result.is_empty() {
        // Dataset was dropped - abort worked correctly
    } else {
        // Dataset exists but should have 0 rows (rollback worked)
        assert_eq!(
            result.rows()[0].get::<i64>("c")?,
            0,
            "Abort should rollback all flushed data"
        );
    }

    Ok(())
}

/// Pins how a bulk load relates to the transaction it was created from.
///
/// It is not part of that transaction. `bulk.commit()` commits the load on its
/// own: other sessions see it at once, before the transaction commits, and a
/// rollback of the transaction does not undo it. The transaction's own reads
/// never see it, before `bulk.commit()` or after — they read the snapshot the
/// transaction started from, plus its own Cypher writes. Ten rows go through
/// the writer with `batch_size(3)`, so three batches are staged in storage
/// before the commit, not just held in memory.
#[tokio::test]
async fn bulk_load_is_not_part_of_its_hosting_transaction() -> Result<()> {
    let (db, _temp) = setup_db().await?;
    let count = "MATCH (p:Person) RETURN count(p) AS c";
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:Person {name: 'via_cypher'})").await?;
    let mut bulk = tx.bulk_writer().batch_size(3).build()?;
    let props: Vec<HashMap<String, uni_db::Value>> = (0..10)
        .map(|i| HashMap::from([("name".to_string(), unival!(format!("P{i}")))]))
        .collect();
    bulk.insert_vertices("Person", props).await?;

    let tx_sees = tx.query(count).await?.rows()[0].get::<i64>("c")?;
    let others_see = db.session().query(count).await?.rows()[0].get::<i64>("c")?;
    assert_eq!(
        (tx_sees, others_see),
        (1, 0),
        "before bulk.commit(): the transaction sees only its Cypher row, others nothing"
    );

    bulk.commit().await?;
    let tx_sees = tx.query(count).await?.rows()[0].get::<i64>("c")?;
    let others_see = db.session().query(count).await?.rows()[0].get::<i64>("c")?;
    assert_eq!(
        (tx_sees, others_see),
        (1, 10),
        "after bulk.commit(): others see the load before the transaction commits; \
         the transaction itself still does not"
    );

    tx.rollback();
    let others_see = db.session().query(count).await?.rows()[0].get::<i64>("c")?;
    assert_eq!(
        others_see, 10,
        "rolling the transaction back drops its Cypher row and keeps the load"
    );
    Ok(())
}

#[tokio::test]
async fn test_bulk_buffer_limit_checkpoint() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    // Set a very small buffer limit (10KB) and large batch size
    // This forces checkpoint based on buffer size, not batch count
    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx
        .bulk_writer()
        .batch_size(100_000) // Won't flush based on count
        .max_buffer_size_bytes(10 * 1024) // 10KB limit
        .build()?;

    // Insert data that will exceed 10KB when serialized
    // Each vertex with a 500-char name is roughly 500+ bytes
    let mut props = Vec::new();
    for i in 0..100 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        let long_name = format!("Person_{}_with_a_very_long_name_{}", i, "x".repeat(500));
        p.insert("name".to_string(), unival!(long_name));
        p.insert("age".to_string(), unival!(i));
        props.push(p);
    }

    let vids = bulk.insert_vertices("Person", props).await?;
    assert_eq!(vids.len(), 100);

    // Commit to finalize
    let stats = bulk.commit().await?;
    assert_eq!(stats.vertices_inserted, 100);
    drop(tx);

    // Verify all data was persisted
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("c")?, 100);

    Ok(())
}

#[tokio::test]
async fn test_bulk_constraint_validation_disabled() -> Result<()> {
    let (db, _temp) = setup_db_with_constraints().await?;

    // With validation disabled, UNIQUE constraint violations should not fail at insert time
    // Note: Arrow/LanceDB still enforces schema-level nullability, so we can't skip NOT NULL
    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().validate_constraints(false).build()?;

    // Insert duplicate emails - should succeed when UNIQUE validation disabled
    let props = vec![
        {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!("Alice"));
            p.insert("email".to_string(), unival!("same@example.com"));
            p
        },
        {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!("Bob"));
            p.insert("email".to_string(), unival!("same@example.com")); // Duplicate email
            p
        },
    ];

    // This should succeed because UNIQUE validation is disabled
    let result = bulk.insert_vertices("Person", props).await;
    assert!(
        result.is_ok(),
        "Should succeed with validation disabled: {:?}",
        result.err()
    );

    bulk.commit().await?;
    drop(tx);

    // Both rows should exist (constraint was bypassed)
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("c")?, 2);

    Ok(())
}

/// H8: a UNIQUE constraint must hold across buffer flushes. With a small
/// batch_size, the first batch flushes (draining the in-memory buffer); a
/// duplicate key reintroduced in a later batch must still be rejected — the old
/// check only compared against the (now-drained) buffer and let it through.
#[tokio::test]
async fn test_bulk_unique_constraint_spans_buffer_flushes() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let db = Uni::open(temp.path().to_str().unwrap()).build().await?;

    // Define a label with a UNIQUE constraint on `email`.
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (email STRING UNIQUE, name STRING)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    // batch_size = 2 so the first insert flushes and drains the buffer.
    let mut bulk = tx.bulk_writer().batch_size(2).build()?;

    let mk = |email: &str| {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("email".to_string(), unival!(email));
        p.insert("name".to_string(), unival!("n"));
        p
    };

    // First batch of 2 reaches batch_size → flushes → buffer drained.
    bulk.insert_vertices("User", vec![mk("a@x"), mk("b@x")])
        .await?;

    // Second batch reintroduces 'a@x' AFTER the first batch was flushed.
    let result = bulk
        .insert_vertices("User", vec![mk("c@x"), mk("a@x")])
        .await;

    assert!(
        result.is_err(),
        "a duplicate UNIQUE key spanning two flushes must be rejected"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .to_lowercase()
            .contains("unique"),
        "error should mention the UNIQUE violation"
    );

    Ok(())
}

/// H9: a bulk load publishes the per-label table and the main table as
/// separate Lance commits. A failure between them, after the commit marker is
/// durable, used to return a plain `Err` while the load was decided: the next
/// open completed it, so a caller who retried the "failed" load held it twice,
/// and live readers saw the per-label table without `vertices` until then.
/// `commit` now completes the publish in-process from the marker's record and
/// returns `Ok`, reporting the failed first attempt in `warnings`.
#[tokio::test]
async fn bulk_partial_publish_completed_on_reopen() -> Result<()> {
    use std::sync::atomic::Ordering::SeqCst;

    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().to_str().unwrap().to_string();

    let load_id;
    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Person")
            .property("name", uni_db::DataType::String)
            .apply()
            .await?;

        // Arm the fault: the publish commits the per-label `vertices_Person`
        // table, then errors before the main `vertices` table.
        uni_db::api::bulk::FAIL_AFTER_PERLABEL_WRITE.store(true, SeqCst);

        let s = db.session();
        let tx = s.tx().await?;
        let mut bulk = tx.bulk_writer().batch_size(1000).build()?;
        load_id = bulk.load_id().to_string();
        let mut props = Vec::new();
        for i in 0..5 {
            let mut p: HashMap<String, uni_db::Value> = HashMap::new();
            p.insert("name".to_string(), unival!(format!("item_{i}")));
            props.push(p);
        }
        bulk.insert_vertices("Person", props).await?;
        let commit_result = bulk.commit().await;
        uni_db::api::bulk::FAIL_AFTER_PERLABEL_WRITE.store(false, SeqCst);
        let stats = commit_result.map_err(|e| {
            anyhow::anyhow!(
                "a publish failure after the commit point must be completed in-process: {e:#}"
            )
        })?;
        assert_eq!(stats.load_id, load_id);
        assert!(
            stats
                .warnings
                .iter()
                .any(|w| w.contains("injected failure after per-label vertex commit")),
            "the failed first publish attempt must be reported: {:?}",
            stats.warnings
        );
        assert!(db.bulk_load_applied(&load_id).await?, "applied, live");

        // Live readers see all of it: the label scan and the label-less scan
        // agree.
        for q in [
            "MATCH (p:Person) RETURN count(p) AS c",
            "MATCH (p) RETURN count(p) AS c",
        ] {
            let result = db.session().query(q).await?;
            assert_eq!(result.rows()[0].get::<i64>("c")?, 5, "live: {q}");
        }
        drop(tx);
        drop(db);
    }

    // Reopen: the per-label and main tables are consistent (both hold the
    // five rows, once).
    let db = Uni::open(&path).build().await?;
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(
        result.rows()[0].get::<i64>("c")?,
        5,
        "the load must be present exactly once after reopen (per-label table)"
    );
    let storage = db.storage();
    for table in [
        uni_store::backend::table_names::vertex_table_name("Person"),
        uni_store::backend::table_names::main_vertex_table_name().to_string(),
    ] {
        assert_eq!(
            storage.backend().count_rows(&table, None).await?,
            5,
            "the load must be present exactly once after reopen ({table})"
        );
    }
    assert!(
        db.bulk_load_applied(&load_id).await?,
        "applied, after reopen"
    );

    // And the database is healthy: a clean load yields exactly its rows on top,
    // proving no row was published twice.
    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(1000).build()?;
    let mut props = Vec::new();
    for i in 0..3 {
        let mut p: HashMap<String, uni_db::Value> = HashMap::new();
        p.insert("name".to_string(), unival!(format!("clean_{i}")));
        props.push(p);
    }
    bulk.insert_vertices("Person", props).await?;
    let stats = bulk.commit().await?;
    assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);
    drop(tx);

    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(
        result.rows()[0].get::<i64>("c")?,
        8,
        "clean load after recovery must yield exactly its rows"
    );

    Ok(())
}

/// A load's id is exposed before and after commit, and `bulk_load_applied`
/// answers for it: `false` before the commit point, `true` after it, still
/// `true` after a reopen (from the receipt, once the marker is gone), and
/// `false` for a load that was aborted or never existed.
#[tokio::test]
async fn bulk_load_id_and_applied_check() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().to_str().unwrap().to_string();
    let (applied_id, aborted_id);
    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Person")
            .property("name", uni_db::DataType::String)
            .apply()
            .await?;
        let rows = || {
            (0..4)
                .map(|i| HashMap::from([("name".to_string(), unival!(format!("p{i}")))]))
                .collect::<Vec<_>>()
        };

        let tx = db.session().tx().await?;
        let mut bulk = tx.bulk_writer().batch_size(2).build()?;
        applied_id = bulk.load_id().to_string();
        uuid::Uuid::parse_str(&applied_id)?;
        bulk.insert_vertices("Person", rows()).await?;
        assert!(
            !db.bulk_load_applied(&applied_id).await?,
            "staged but before the commit point: not applied"
        );
        let stats = bulk.commit().await?;
        assert_eq!(stats.load_id, applied_id);
        assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);
        assert!(db.bulk_load_applied(&applied_id).await?, "after commit");

        let mut bulk = tx.bulk_writer().batch_size(2).build()?;
        aborted_id = bulk.load_id().to_string();
        assert_ne!(aborted_id, applied_id, "each load has its own id");
        bulk.insert_vertices("Person", rows()).await?;
        bulk.abort().await?;
        assert!(!db.bulk_load_applied(&aborted_id).await?, "aborted");
        tx.commit().await?;
    }

    let db = Uni::open(&path).build().await?;
    assert!(db.bulk_load_applied(&applied_id).await?, "after reopen");
    assert!(
        !db.bulk_load_applied(&aborted_id).await?,
        "aborted, after reopen"
    );
    assert!(
        !db.bulk_load_applied(&uuid::Uuid::new_v4().to_string())
            .await?,
        "an unknown load"
    );
    let invalid = db.bulk_load_applied("../schema").await;
    assert!(
        matches!(invalid, Err(uni_db::UniError::InvalidArgument { .. })),
        "a non-id must be refused, not answered: {invalid:?}"
    );
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("c")?, 4);
    Ok(())
}

// ---------------------------------------------------------------------------
// Vector indexes via the bulk loader (issue #96, creation surface E)
//
// The bulk loader rebuilds indexes through `rebuild_indexes_for_label` ->
// `create_vector_index(...).with_backend(...)` (bulk.rs sync-commit path). That rebuild
// path-class is where an earlier MUVERA backfill bug hid (a missing `with_backend` left the
// FDE column registered in schema but not materialised on disk), and NO test exercised it
// via bulk. These two tests close that gap and assert results against an independent
// brute-force MaxSim oracle, not just "non-empty".
// ---------------------------------------------------------------------------

const MV_DIM: usize = 8;

fn mv_basis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; MV_DIM];
    v[i] = 1.0;
    v
}

fn mv_unit(state: &mut u64) -> Vec<f32> {
    let mut v: Vec<f32> = (0..MV_DIM)
        .map(|_| {
            let mut x = *state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *state = x;
            ((x >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
        })
        .collect();
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut v {
        *x /= n;
    }
    v
}

fn mv_query() -> Vec<Vec<f32>> {
    vec![mv_basis(0), mv_basis(1)]
}

/// Planted corpus: doc `n/2` titled `target` has tokens == the query (MaxSim 2.0); the
/// rest are random unit-vector docs. Returned so the test can compute ground truth.
fn mv_corpus(n: usize, seed: u64) -> Vec<(String, Vec<Vec<f32>>)> {
    let mut state = seed;
    (0..n)
        .map(|i| {
            if i == n / 2 {
                ("target".to_string(), mv_query())
            } else {
                (
                    format!("doc{i}"),
                    (0..3).map(|_| mv_unit(&mut state)).collect(),
                )
            }
        })
        .collect()
}

fn mv_to_value(tokens: &[Vec<f32>]) -> uni_db::Value {
    uni_db::Value::List(
        tokens
            .iter()
            .map(|t| {
                uni_db::Value::List(t.iter().map(|&x| uni_db::Value::Float(x as f64)).collect())
            })
            .collect(),
    )
}

fn mv_lit(tokens: &[Vec<f32>]) -> String {
    let toks: Vec<String> = tokens
        .iter()
        .map(|t| {
            let nums: Vec<String> = t.iter().map(|x| format!("{x:?}")).collect();
            format!("[{}]", nums.join(","))
        })
        .collect();
    format!("[{}]", toks.join(","))
}

/// Cosine MaxSim `Σ_q max_d cos(q,d)` (empty doc token contributes 0) — matches
/// `uni_query_functions::similar_to::maxsim` under the default Cosine metric.
fn mv_cosine_maxsim(query: &[Vec<f32>], doc: &[Vec<f32>]) -> f64 {
    let cos = |a: &[f32], b: &[f32]| -> f64 {
        let dot: f64 = a.iter().zip(b).map(|(&x, &y)| x as f64 * y as f64).sum();
        let na = a.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
        let nb = b.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
        if na == 0.0 || nb == 0.0 {
            0.0
        } else {
            dot / (na * nb)
        }
    };
    query
        .iter()
        .map(|q| {
            doc.iter()
                .map(|d| cos(q, d))
                .fold(None, |acc: Option<f64>, s| {
                    Some(acc.map_or(s, |b| b.max(s)))
                })
                .unwrap_or(0.0)
        })
        .sum()
}

async fn mv_query_results(db: &Uni, k: usize) -> Result<Vec<(String, f64)>> {
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {}, {k}, null, null, {{}}) \
         YIELD node, score RETURN node.title AS title, score",
        mv_lit(&mv_query())
    );
    let res = db.session().query(&cypher).await?;
    Ok(res
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
            )
        })
        .collect())
}

fn mv_assert_oracle(engine: &[(String, f64)], corpus: &[(String, Vec<Vec<f32>>)], full_set: bool) {
    const EPS: f64 = 1e-4;
    let q = mv_query();
    let oracle: HashMap<&str, f64> = corpus
        .iter()
        .map(|(t, toks)| (t.as_str(), mv_cosine_maxsim(&q, toks)))
        .collect();
    for (title, score) in engine {
        let want = oracle
            .get(title.as_str())
            .unwrap_or_else(|| panic!("engine returned unknown title {title:?}"));
        assert!(
            (score - want).abs() < EPS,
            "exact-MaxSim score mismatch for {title:?}: engine={score} oracle={want}"
        );
    }
    for w in engine.windows(2) {
        assert!(
            w[0].1 >= w[1].1 - EPS,
            "results not sorted by score desc: {engine:?}"
        );
    }
    if full_set {
        let got: std::collections::HashSet<&str> = engine.iter().map(|(t, _)| t.as_str()).collect();
        let want: std::collections::HashSet<&str> = oracle.keys().copied().collect();
        assert_eq!(
            got, want,
            "returned set != full bulk-loaded corpus (recall gap)"
        );
        assert_eq!(
            engine.first().map(|(t, _)| t.as_str()),
            Some("target"),
            "exact-match target must rank first: {engine:?}"
        );
    }
}

fn mv_props(corpus: &[(String, Vec<Vec<f32>>)]) -> Vec<HashMap<String, uni_db::Value>> {
    corpus
        .iter()
        .map(|(title, tokens)| {
            let mut p: HashMap<String, uni_db::Value> = HashMap::new();
            p.insert("title".to_string(), unival!(title.clone()));
            p.insert("tokens".to_string(), mv_to_value(tokens));
            p
        })
        .collect()
}

async fn mv_define_doc_schema(db: &Uni) -> Result<()> {
    use uni_db::DataType;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: MV_DIM })),
        )
        .apply()
        .await?;
    Ok(())
}

/// Bulk-load the corpus, THEN `CREATE VECTOR INDEX ... muvera`: the index backfill must
/// read the BULK-written rows correctly (different write path than tx CREATE) and the
/// flat-inner MUVERA query must reproduce the brute-force ranking over the whole corpus.
#[tokio::test]
async fn test_bulk_then_create_muvera_index() -> Result<()> {
    let db = Uni::temporary().build().await?;
    mv_define_doc_schema(&db).await?;

    let corpus = mv_corpus(60, 0xB17C_0DE5);
    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(100).build()?;
    bulk.insert_vertices("Doc", mv_props(&corpus)).await?;
    bulk.commit().await?;
    drop(tx);

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE VECTOR INDEX tok_idx FOR (d:Doc) ON (d.tokens) \
         OPTIONS {type:'muvera', k_sim:4, reps:8, d_proj:8, inner:'flat'}",
    )
    .await?;
    tx.commit().await?;

    let results = mv_query_results(&db, corpus.len()).await?;
    mv_assert_oracle(&results, &corpus, true);
    Ok(())
}

/// Declare the MUVERA index up front, THEN bulk-load + sync-commit: the bulk commit's index
/// rebuild (`rebuild_indexes_for_label` -> `create_vector_index(...).with_backend(...)`,
/// the bulk.rs path) must materialise the FDE column over the bulk-loaded rows. This is the
/// exact rebuild path-class the earlier missing-`with_backend` MUVERA bug lived in.
#[tokio::test]
async fn test_bulk_commit_rebuilds_declared_muvera_index() -> Result<()> {
    use uni_db::{VectorAlgo, VectorIndexCfg, VectorMetric};
    let db = Uni::temporary().build().await?;
    mv_define_doc_schema(&db).await?;

    db.schema()
        .label("Doc")
        .index(
            "tokens",
            uni_db::IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Muvera {
                    k_sim: 4,
                    reps: 8,
                    d_proj: 8,
                    seed: uni_db::api::schema::DEFAULT_FDE_SEED,
                    inner: Box::new(VectorAlgo::Flat),
                },
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    let corpus = mv_corpus(60, 0x5EED_1234);
    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx
        .bulk_writer()
        .batch_size(100)
        .async_indexes(false)
        .build()?;
    bulk.insert_vertices("Doc", mv_props(&corpus)).await?;
    bulk.commit().await?;
    drop(tx);

    let results = mv_query_results(&db, corpus.len()).await?;
    mv_assert_oracle(&results, &corpus, true);
    Ok(())
}
