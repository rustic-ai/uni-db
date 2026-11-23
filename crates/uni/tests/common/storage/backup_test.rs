// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_backup_and_restore() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let storage_path = path.join("storage");
    let backup_path = path.join("backup");

    // 1. Setup Data
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _label_id = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(storage_path.to_str().unwrap(), schema_manager.clone()).await?,
    );

    // Use Writer to ensure snapshot is created on flush
    let writer = Arc::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    );

    // Insert data directly via Writer to simulate real usage
    {
        let w: &uni_store::Writer = writer.as_ref();
        let vid = Vid::new(1);
        let mut props = std::collections::HashMap::new();
        props.insert("name".to_string(), serde_json::json!("Alice").into());
        w.insert_vertex_with_labels(vid, props, &["Person".to_string()], None)
            .await?;
        w.flush_to_l1(None).await?; // This creates a snapshot
    }

    // 2. Execute BACKUP
    let query = format!("BACKUP TO '{}'", backup_path.to_str().unwrap());

    let ast = uni_cypher::parse(&query)?;
    let planner = QueryPlanner::new(schema_manager.schema());
    let plan = planner.plan(ast)?;
    let mut executor = Executor::new(storage.clone());
    executor.set_writer(writer.clone());
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let result = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].get("status").unwrap().as_str().unwrap(),
        "Backup completed"
    );

    // 3. Verify Backup
    assert!(backup_path.exists());
    assert!(backup_path.join("catalog").exists());
    // A backup has the standard layout: tables under `storage/`.
    assert!(backup_path.join("storage/vertices_Person.lance").exists());

    // 4. Restore (Open new Uni from backup)
    let restore_schema_manager =
        SchemaManager::load(&backup_path.join("catalog/schema.json")).await?;
    let restore_schema_manager = Arc::new(restore_schema_manager);
    let restore_storage = Arc::new(
        StorageManager::new(
            backup_path.join("storage").to_str().unwrap(),
            restore_schema_manager.clone(),
        )
        .await?,
    );

    // Check data via storage backend
    let vtable_name = uni_db::store::backend::table_names::vertex_table_name("Person");
    let count = restore_storage
        .backend()
        .count_rows(&vtable_name, None)
        .await?;
    assert_eq!(count, 1);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&std::fs::read(
            backup_path.join("catalog/layout.json")
        )?)?["layout"],
        serde_json::json!("standard")
    );

    Ok(())
}
