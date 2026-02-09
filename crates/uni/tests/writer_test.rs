// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;
use uni_db::unival;

#[tokio::test]
async fn test_writer_flush() -> anyhow::Result<()> {
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

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);

    // 2. Initialize Writer
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    // 3. Insert Edge
    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);
    let eid = Eid::new(100);
    writer
        .insert_edge(vid_a, vid_b, 1, eid, HashMap::new())
        .await?;

    // 4. Flush to L1
    writer.flush_to_l1(None).await?;

    // 5. Verify L1 Delta Dataset via LanceDB
    let delta_ds = storage.delta_dataset("knows", "fwd")?;
    let lancedb_store = storage.lancedb_store();
    let table = delta_ds.open_lancedb(lancedb_store).await?;
    let count = table.count_rows(None).await?;

    assert_eq!(count, 1);

    Ok(())
}

#[tokio::test]
async fn test_writer_vertex_flush() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);

    // 2. Initialize Writer
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    // 3. Insert Vertex
    let vid = Vid::new(10);
    let mut props = HashMap::new();
    props.insert("name".to_string(), unival!("Alice"));
    writer
        .insert_vertex_with_labels(vid, props, vec!["Person".to_string()])
        .await?;

    // 4. Flush to L1
    writer.flush_to_l1(None).await?;

    // 5. Verify Vertex Dataset via LanceDB
    let ds = storage.vertex_dataset("Person")?;
    let lancedb_store = storage.lancedb_store();
    let table = ds.open_lancedb(lancedb_store).await?;
    let count = table.count_rows(None).await?;
    assert_eq!(count, 1);

    Ok(())
}
