// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{UniId, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_uid_indexing() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _label_id = schema_manager.add_label("Person")?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(storage_str, schema_manager.clone()).await?;

    // 2. Prepare Data
    let vid = Vid::new(100);
    // Create a valid multibase string for UID (SHA3-256 = 32 bytes)
    // "b" + base32Lower
    // Let's use 32 bytes of zeros
    let bytes = [0u8; 32];
    let uid = UniId::from_bytes(bytes);

    // 3. Insert Mapping
    storage.insert_vertex_with_uid("Person", vid, uid).await?;

    // 4. Lookup
    let found_vid = storage.get_vertex_by_uid(&uid, "Person").await?;

    assert_eq!(found_vid, Some(vid));

    // 5. Lookup Non-existent
    let bytes2 = [1u8; 32];
    let uid2 = UniId::from_bytes(bytes2);

    let found_vid2 = storage.get_vertex_by_uid(&uid2, "Person").await?;
    assert_eq!(found_vid2, None);

    Ok(())
}
