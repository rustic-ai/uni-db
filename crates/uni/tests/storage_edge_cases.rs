// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::storage::delta::Op;
use uni_db::storage::manager::StorageManager;
use uni_db::unival;

#[tokio::test]
async fn test_vertex_dataset_batch_writes() -> Result<()> {
    let dir = tempdir()?;
    let base_path = dir.path().to_str().unwrap();
    let schema_path = dir.path().join("schema.json");

    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _label_id = schema_manager.add_label("User")?;
    schema_manager.add_property("User", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(base_path, schema_manager.clone()).await?;
    let lancedb_store = storage.lancedb_store();
    let ds = storage.vertex_dataset("User")?;

    // Write a batch
    let vid1 = Vid::new(1);
    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), unival!("Alice"));

    let schema = schema_manager.schema();
    let batch = ds.build_record_batch(
        &[(vid1, vec!["User".to_string()], props1)],
        &[false],
        &[1],
        &schema,
    )?;
    ds.write_batch_lancedb(lancedb_store, batch, &schema)
        .await?;

    let vid2 = Vid::new(2);
    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), unival!("Bob"));
    let batch2 = ds.build_record_batch(
        &[(vid2, vec!["User".to_string()], props2)],
        &[false],
        &[1],
        &schema,
    )?;
    ds.write_batch_lancedb(lancedb_store, batch2, &schema)
        .await?;

    // Verify count using LanceDB
    let table = ds.open_lancedb(lancedb_store).await?;
    assert_eq!(table.count_rows(None).await?, 2);

    Ok(())
}

#[tokio::test]
async fn test_delta_dataset_merging() -> Result<()> {
    let dir = tempdir()?;
    let base_path = dir.path().to_str().unwrap();
    let schema_path = dir.path().join("schema.json");

    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _tid = schema_manager.add_edge_type("KNOWS", vec!["User".into()], vec!["User".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(base_path, schema_manager.clone()).await?;
    let lancedb_store = storage.lancedb_store();
    let delta_ds = storage.delta_dataset("KNOWS", "fwd")?;

    // Write multiple runs using LanceDB
    let schema = schema_manager.schema();

    // Run 1: Insert E1 (ver 1)
    let entry1 = uni_db::storage::delta::L1Entry {
        src_vid: Vid::new(1),
        dst_vid: Vid::new(2),
        eid: uni_db::core::id::Eid::new(1),
        op: Op::Insert,
        version: 1,
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    };
    let batch1 = delta_ds.build_record_batch(std::slice::from_ref(&entry1), &schema)?;
    delta_ds.write_run_lancedb(lancedb_store, batch1).await?;

    // Run 2: Delete E1 (ver 2)
    let entry2 = uni_db::storage::delta::L1Entry {
        src_vid: Vid::new(1),
        dst_vid: Vid::new(2),
        eid: uni_db::core::id::Eid::new(1),
        op: Op::Delete,
        version: 2,
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    };
    let batch2 = delta_ds.build_record_batch(std::slice::from_ref(&entry2), &schema)?;
    delta_ds.write_run_lancedb(lancedb_store, batch2).await?;

    // Read deltas for src_vid using LanceDB
    let deltas = delta_ds
        .read_deltas_lancedb(lancedb_store, Vid::new(1), &schema, None)
        .await?;

    // Should get both? Or merged? read_deltas returns raw entries from runs.
    // Logic in manager merges them.
    assert_eq!(deltas.len(), 2);
    // Find by version since LanceDB may return in different order
    let v1 = deltas
        .iter()
        .find(|d| d.version == 1)
        .expect("version 1 not found");
    let v2 = deltas
        .iter()
        .find(|d| d.version == 2)
        .expect("version 2 not found");
    assert_eq!(v1.op, Op::Insert);
    assert_eq!(v2.op, Op::Delete);

    Ok(())
}
