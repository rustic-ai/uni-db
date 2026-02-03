// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::runtime::wal::WriteAheadLog;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_wal_preservation_after_flush() -> anyhow::Result<()> {
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

    // 2. Initialize Writer and attach WAL
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    let wal_store = Arc::new(LocalFileSystem::new_with_prefix(path)?);
    let wal_prefix = ObjectStorePath::from("wal");
    let wal = Arc::new(WriteAheadLog::new(wal_store.clone(), wal_prefix.clone()));

    writer.l0_manager.get_current().write().wal = Some(wal.clone());

    // 3. Insert Edge
    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);
    let eid = Eid::new(100);
    writer
        .insert_edge(vid_a, vid_b, 1, eid, HashMap::new())
        .await?;

    // 4. Flush WAL manually to persist buffer
    wal.flush().await?;

    // Verify WAL has data
    let mutations = wal.replay().await?;
    assert!(
        !mutations.is_empty(),
        "WAL should not be empty after insert and flush"
    );

    // 5. Flush to L1
    writer.flush_to_l1(None).await?;

    // 6. Verify WAL is truncated
    let mutations_after_flush = wal.replay().await?;
    assert!(
        mutations_after_flush.is_empty(),
        "WAL should be empty after L1 flush"
    );

    // 7. Insert another edge
    let eid2 = Eid::new(101);
    writer
        .insert_edge(vid_a, vid_b, 1, eid2, HashMap::new())
        .await?;
    wal.flush().await?;

    // 8. Verify WAL has new data (WAL was preserved)
    let mutations_final = wal.replay().await?;
    assert!(
        !mutations_final.is_empty(),
        "WAL should contain new data after flush and new insert"
    );

    Ok(())
}
