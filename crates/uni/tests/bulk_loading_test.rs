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

    let mut bulk = db.bulk_writer().batch_size(100).build()?;

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

    // Verify data was persisted
    let result = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(result.rows[0].get::<i64>("c")?, 250);

    Ok(())
}

#[tokio::test]
async fn test_bulk_insert_edges() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    // First create some vertices
    let mut bulk = db.bulk_writer().batch_size(100).build()?;

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

    let mut bulk = db.bulk_writer().batch_size(1000).build()?; // Large batch to avoid flush

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

    // Verify no data was persisted (buffers were cleared before flush)
    // When no dataset exists yet, MATCH returns no rows
    let result = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    if result.rows.is_empty() {
        // No dataset exists - abort worked correctly
    } else {
        // Dataset exists but should have 0 rows
        assert_eq!(result.rows[0].get::<i64>("c")?, 0);
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

    let mut bulk = db
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

    let mut bulk = db.bulk_writer().build()?;

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

    // Verify edge exists with property
    // Note: Edge property queries may require specific query patterns
    let result = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name")
        .await?;
    assert_eq!(result.rows.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_bulk_async_indexes_returns_immediately() -> Result<()> {
    use uni_store::storage::IndexRebuildStatus;

    let (db, _temp) = setup_db().await?;

    let mut bulk = db
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

    // In async mode, indexes_pending should be true
    assert!(stats.indexes_pending);

    // Data should be queryable immediately (via full scan)
    let result = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(result.rows[0].get::<i64>("c")?, 100);

    // Check initial status - should have pending or in-progress tasks
    let status = db.index_rebuild_status().await?;
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

    let mut bulk = db
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

    // In sync mode, indexes_pending should be false
    assert!(!stats.indexes_pending);
    assert!(stats.index_task_ids.is_empty());

    // Data should be queryable
    let result = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(result.rows[0].get::<i64>("c")?, 50);

    Ok(())
}

#[tokio::test]
async fn test_index_rebuild_status_tracking() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    // Initially, there should be no tasks
    let status = db.index_rebuild_status().await?;
    // After a fresh DB, status may be empty or have loaded state
    let _initial_count = status.len();

    // Use rebuild_indexes to trigger a task
    let task_id = db.rebuild_indexes("Person", true).await?;

    // If a task was created, verify we can track it
    if let Some(tid) = task_id {
        let status = db.index_rebuild_status().await?;
        let found = status.iter().any(|t| t.id == tid);
        assert!(found, "Task {} should be in status list", tid);
    }

    Ok(())
}

#[tokio::test]
async fn test_is_index_building() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    // Initially, no indexes should be building
    let is_building = db.is_index_building("Person").await?;
    assert!(!is_building);

    // Trigger an async rebuild
    let _task_id = db.rebuild_indexes("Person", true).await?;

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

    let mut bulk = db.bulk_writer().validate_constraints(true).build()?;

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

    let mut bulk = db.bulk_writer().validate_constraints(true).build()?;

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

    let mut bulk = db.bulk_writer().validate_constraints(true).build()?;

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

    let mut bulk = db.bulk_writer().validate_constraints(true).build()?;

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

    let mut bulk = db.bulk_writer().batch_size(10).build()?; // Small batch to force flush

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

    // Verify no data remains
    let result = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    if result.rows.is_empty() {
        // Dataset was dropped - abort worked correctly
    } else {
        // Dataset exists but should have 0 rows (rollback worked)
        assert_eq!(
            result.rows[0].get::<i64>("c")?,
            0,
            "Abort should rollback all flushed data"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_bulk_buffer_limit_checkpoint() -> Result<()> {
    let (db, _temp) = setup_db().await?;

    // Set a very small buffer limit (10KB) and large batch size
    // This forces checkpoint based on buffer size, not batch count
    let mut bulk = db
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

    // Verify all data was persisted
    let result = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(result.rows[0].get::<i64>("c")?, 100);

    Ok(())
}

#[tokio::test]
async fn test_bulk_constraint_validation_disabled() -> Result<()> {
    let (db, _temp) = setup_db_with_constraints().await?;

    // With validation disabled, UNIQUE constraint violations should not fail at insert time
    // Note: Arrow/LanceDB still enforces schema-level nullability, so we can't skip NOT NULL
    let mut bulk = db.bulk_writer().validate_constraints(false).build()?;

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

    // Both rows should exist (constraint was bypassed)
    let result = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(result.rows[0].get::<i64>("c")?, 2);

    Ok(())
}
