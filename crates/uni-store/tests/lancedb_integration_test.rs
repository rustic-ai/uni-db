// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for LanceDB-based storage path.

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::config::UniConfig;
use uni_common::core::schema::SchemaManager;
use uni_store::lancedb::LanceDbStore;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn test_lancedb_flush_vertices() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", uni_common::DataType::String, false)?;
    schema_manager.add_property("Person", "age", uni_common::DataType::Int64, true)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    // 2. Create StorageManager (LanceDB is always enabled)
    let storage =
        StorageManager::new_with_config(storage_str, schema_manager.clone(), UniConfig::default())
            .await?;
    let storage = Arc::new(storage);

    // 3. Create Writer and insert vertices
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0).await?;

    let vid1 = writer.next_vid().await?;
    let vid2 = writer.next_vid().await?;

    let mut props1 = HashMap::new();
    props1.insert(
        "name".to_string(),
        uni_common::Value::String("Alice".to_string()),
    );
    props1.insert("age".to_string(), uni_common::Value::Int(30));

    let mut props2 = HashMap::new();
    props2.insert(
        "name".to_string(),
        uni_common::Value::String("Bob".to_string()),
    );
    props2.insert("age".to_string(), uni_common::Value::Int(25));

    writer
        .insert_vertex_with_labels(vid1, props1, vec!["Person".to_string()])
        .await?;
    writer
        .insert_vertex_with_labels(vid2, props2, vec!["Person".to_string()])
        .await?;

    // 4. Flush to L1 (this should use LanceDB)
    let snapshot_id = writer
        .flush_to_l1(Some("test_snapshot".to_string()))
        .await?;
    assert!(!snapshot_id.is_empty());

    // 5. Verify LanceDB table exists and has data
    let lancedb_store = storage.lancedb_store();
    let table_name = LanceDbStore::vertex_table_name("Person");

    assert!(lancedb_store.table_exists(&table_name).await?);

    let table = lancedb_store.open_table(&table_name).await?;
    let count = table.count_rows(None).await?;
    assert_eq!(count, 2, "Should have 2 vertices in the table");

    Ok(())
}

#[tokio::test]
async fn test_lancedb_flush_edges() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    // 2. Create StorageManager (LanceDB is always enabled)
    let storage =
        StorageManager::new_with_config(storage_str, schema_manager.clone(), UniConfig::default())
            .await?;
    let storage = Arc::new(storage);

    // 3. Create Writer and insert edges
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0).await?;

    let vid1 = writer.next_vid().await?;
    let vid2 = writer.next_vid().await?;
    let vid3 = writer.next_vid().await?;

    // Insert vertices first (with labels)
    writer
        .insert_vertex_with_labels(vid1, HashMap::new(), vec!["Person".to_string()])
        .await?;
    writer
        .insert_vertex_with_labels(vid2, HashMap::new(), vec!["Person".to_string()])
        .await?;
    writer
        .insert_vertex_with_labels(vid3, HashMap::new(), vec!["Person".to_string()])
        .await?;

    // Insert edges: vid1 -> vid2, vid1 -> vid3
    let eid1 = writer.next_eid(1).await?;
    let eid2 = writer.next_eid(1).await?;

    writer
        .insert_edge(vid1, vid2, 1, eid1, HashMap::new())
        .await?;
    writer
        .insert_edge(vid1, vid3, 1, eid2, HashMap::new())
        .await?;

    // 4. Flush to L1 (this should use LanceDB)
    let snapshot_id = writer.flush_to_l1(None).await?;
    assert!(!snapshot_id.is_empty());

    // 5. Verify LanceDB delta tables exist
    let lancedb_store = storage.lancedb_store();

    let fwd_table_name = LanceDbStore::delta_table_name("knows", "fwd");
    let bwd_table_name = LanceDbStore::delta_table_name("knows", "bwd");

    assert!(
        lancedb_store.table_exists(&fwd_table_name).await?,
        "FWD delta table should exist"
    );
    assert!(
        lancedb_store.table_exists(&bwd_table_name).await?,
        "BWD delta table should exist"
    );

    let fwd_table = lancedb_store.open_table(&fwd_table_name).await?;
    let fwd_count = fwd_table.count_rows(None).await?;
    assert_eq!(fwd_count, 2, "Should have 2 edges in FWD delta table");

    let bwd_table = lancedb_store.open_table(&bwd_table_name).await?;
    let bwd_count = bwd_table.count_rows(None).await?;
    assert_eq!(bwd_count, 2, "Should have 2 edges in BWD delta table");

    Ok(())
}

#[tokio::test]
async fn test_lancedb_table_naming() {
    // Verify table naming conventions
    assert_eq!(LanceDbStore::vertex_table_name("Person"), "vertices_Person");
    assert_eq!(LanceDbStore::vertex_table_name("User"), "vertices_User");

    assert_eq!(
        LanceDbStore::delta_table_name("knows", "fwd"),
        "deltas_knows_fwd"
    );
    assert_eq!(
        LanceDbStore::delta_table_name("LIKES", "bwd"),
        "deltas_LIKES_bwd"
    );

    assert_eq!(
        LanceDbStore::adjacency_table_name("knows", "fwd"),
        "adjacency_knows_fwd"
    );
}
