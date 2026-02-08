// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! # Persistence Restart Integration Test
//!
//! Verifies that data written to the database actually persists to disk
//! and can be recovered after a complete "restart" (dropping all memory state).

use anyhow::Result;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_data_survives_restart() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // --- PHASE 1: Write and Flush ---
    {
        // 1. Setup Schema
        let schema_manager = SchemaManager::load(&schema_path).await?;
        let _label_id = schema_manager.add_label("Person")?;
        schema_manager.add_property("Person", "name", DataType::String, false)?;
        schema_manager.save().await?;
        let schema_manager = Arc::new(schema_manager);

        let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap();

        // 2. Insert Data
        let vid = Vid::new(42);
        let mut props = HashMap::new();
        props.insert("name".to_string(), serde_json::json!("PersistenceCheck").into());
        writer
            .insert_vertex_with_labels(vid, props, vec!["Person".to_string()])
            .await?;

        // 3. Flush to Disk
        writer.flush_to_l1(None).await?;

        // 4. Verify explicit file existence (White-box check)
        // LanceDB creates a directory with .lance extension
        let vertex_path = storage_path.join("vertices_Person.lance");
        if !vertex_path.exists() {
            println!("Storage content ({:?}):", storage_path);
            if let Ok(entries) = std::fs::read_dir(&storage_path) {
                for entry in entries {
                    println!("{:?}", entry.unwrap().path());
                }
            } else {
                println!("Could not read storage dir");
            }
        }
        assert!(
            vertex_path.exists(),
            "Vertex dataset directory should exist at {:?}",
            vertex_path
        );
        assert!(
            vertex_path.join("data").exists(),
            "Data directory should exist"
        );
    }
    // `writer`, `storage`, `schema_manager` are dropped here, simulating shutdown.

    // --- PHASE 2: Restart and Verify ---
    {
        // 1. Re-load Schema
        let schema_manager = SchemaManager::load(&schema_path).await?;
        let schema_manager = Arc::new(schema_manager);

        // 2. Re-initialize Storage (Cold Start)
        let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);

        // 3. Verify Data via LanceDB API
        let ds = storage.vertex_dataset("Person")?;
        let lancedb_store = storage.lancedb_store();
        let table = ds.open_lancedb(lancedb_store).await?;
        let count = table.count_rows(None).await?;
        assert_eq!(count, 1, "Data should persist after restart");

        // 4. Verify Data Content via LanceDB query
        let query = table.query().select(Select::Columns(vec!["name".into()]));
        let batches: Vec<arrow_array::RecordBatch> = query.execute().await?.try_collect().await?;
        assert!(!batches.is_empty(), "Should have at least one batch");
        let batch = &batches[0];
        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "PersistenceCheck");
    }

    Ok(())
}
