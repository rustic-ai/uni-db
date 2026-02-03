// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_delete_vertex_persistence() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    // 2. Insert Vertex
    let vid = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), json!("Alice"));
    writer
        .insert_vertex_with_labels(vid, props, vec!["Person".to_string()])
        .await?;
    writer.flush_to_l1(None).await?;

    // 2. Delete vertex
    writer.delete_vertex(vid, None).await?;
    writer.flush_to_l1(None).await?;

    // 3. Verify vertex is deleted via PropertyManager
    // If vertex is deleted, getting its property should return null
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let result = prop_mgr.get_vertex_prop(vid, "name").await?;

    assert!(
        result.is_null(),
        "Deleted vertex property should return null, got: {:?}",
        result
    );

    Ok(())
}
