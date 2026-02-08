// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::UniConfig;
use uni_db::core::schema::SchemaManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_l0_auto_flush_threshold() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);

    // 2. Initialize Writer with small threshold
    let config = UniConfig {
        auto_flush_threshold: 3,
        ..Default::default()
    };
    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        config,
        None,
        None,
    )
    .await?;

    // 3. Initial state: No snapshots
    assert!(
        storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?
            .is_none()
    );

    // 4. Perform mutations below threshold
    let v1 = writer.next_vid().await?;
    let mut p1 = HashMap::new();
    p1.insert("name".to_string(), serde_json::json!("v1").into());
    writer
        .insert_vertex_with_labels(v1, p1, vec!["Person".to_string()])
        .await?;
    let v2 = writer.next_vid().await?;
    let mut p2 = HashMap::new();
    p2.insert("name".to_string(), serde_json::json!("v2").into());
    writer
        .insert_vertex_with_labels(v2, p2, vec!["Person".to_string()])
        .await?;

    // Still no snapshot
    assert!(
        storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?
            .is_none()
    );

    // 5. Perform the 3rd mutation (Trigger threshold)
    let v3 = writer.next_vid().await?;
    let mut p3 = HashMap::new();
    p3.insert("name".to_string(), serde_json::json!("v3").into());
    writer
        .insert_vertex_with_labels(v3, p3, vec!["Person".to_string()])
        .await?;

    // Snapshot should be created automatically
    let snapshot = storage.snapshot_manager().load_latest_snapshot().await?;
    assert!(
        snapshot.is_some(),
        "Snapshot should be created automatically when threshold reached"
    );

    let manifest = snapshot.unwrap();
    assert_eq!(manifest.vertices.get("Person").unwrap().count, 3);

    // 6. Verify L0 is rotated (mutation count reset)
    {
        let l0 = writer.l0_manager.get_current();
        let l0_guard = l0.read();
        assert_eq!(l0_guard.mutation_count, 0, "L0 should be rotated and reset");
    }

    Ok(())
}
