// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::schema::SchemaManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_snapshot_creation_on_flush() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    let knows_type =
        schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    // 2. Initial state: No snapshots
    assert!(
        storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?
            .is_none()
    );

    // 3. Write Data
    let vid_a = writer.next_vid().await?;
    let vid_b = writer.next_vid().await?;
    let eid = writer.next_eid(knows_type).await?;

    let mut p1 = HashMap::new();
    p1.insert("name".to_string(), serde_json::json!("Alice").into());
    writer
        .insert_vertex_with_labels(vid_a, p1, vec!["Person".to_string()])
        .await?;

    let mut p2 = HashMap::new();
    p2.insert("name".to_string(), serde_json::json!("Bob").into());
    writer
        .insert_vertex_with_labels(vid_b, p2, vec!["Person".to_string()])
        .await?;

    writer
        .insert_edge(vid_a, vid_b, knows_type, eid, HashMap::new())
        .await?;

    // 4. Flush
    writer.flush_to_l1(None).await?;

    // 5. Verify Snapshot Created
    let manifest_opt = storage.snapshot_manager().load_latest_snapshot().await?;
    assert!(
        manifest_opt.is_some(),
        "Snapshot should be created after flush"
    );
    let manifest = manifest_opt.unwrap();

    // Check Vertices Snapshot
    let person_snap = manifest
        .vertices
        .get("Person")
        .expect("Person snapshot missing");
    assert!(person_snap.count > 0, "Vertex count should be > 0");
    // Note: With LanceDB, lance_version is not tracked (set to 0)
    // as LanceDB tables don't expose internal version numbers.

    // Check Edges Snapshot
    let knows_snap = manifest.edges.get("knows").expect("Knows snapshot missing");
    assert!(knows_snap.count > 0, "Edge count should be > 0");

    let first_snap_id = manifest.snapshot_id.clone();

    // 6. Write More Data
    let vid_c = writer.next_vid().await?;
    let eid2 = writer.next_eid(knows_type).await?;
    let mut p3 = HashMap::new();
    p3.insert("name".to_string(), serde_json::json!("Charlie").into());
    writer
        .insert_vertex_with_labels(vid_c, p3, vec!["Person".to_string()])
        .await?;
    writer
        .insert_edge(vid_b, vid_c, knows_type, eid2, HashMap::new())
        .await?;

    // 7. Flush Again
    writer.flush_to_l1(None).await?;

    // 8. Verify New Snapshot
    let manifest2 = storage
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .unwrap();
    assert_ne!(manifest2.snapshot_id, first_snap_id);

    let person_snap2 = manifest2.vertices.get("Person").unwrap();
    assert!(
        person_snap2.count > person_snap.count,
        "Vertex count should increase"
    );
    // Note: With LanceDB, lance_version is not tracked (set to 0)
    // as LanceDB tables don't expose internal version numbers.

    Ok(())
}
