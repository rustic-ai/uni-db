// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Basic tests for StorageManager lifecycle and compaction status.

use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::storage::manager::StorageManager;

async fn setup_storage(path: &std::path::Path) -> (Arc<StorageManager>, Arc<SchemaManager>) {
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_property("Person", "name", DataType::String, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await
        .unwrap(),
    );

    (storage, schema_manager)
}

/// Construction on a clean directory should succeed (exercises recover_all_staging_tables).
#[tokio::test]
async fn test_recover_staging_clean_directory() {
    let dir = tempdir().unwrap();
    let (storage, _schema) = setup_storage(dir.path()).await;

    // Just verify it didn't panic during construction
    assert!(
        storage
            .schema_manager_arc()
            .schema()
            .labels
            .contains_key("Person")
    );
}

/// Fresh database should report zero compaction state.
#[tokio::test]
async fn test_compaction_status_default() {
    let dir = tempdir().unwrap();
    let (storage, _schema) = setup_storage(dir.path()).await;

    let status = storage.compaction_status().unwrap();
    assert!(!status.compaction_in_progress);
    assert_eq!(status.total_compactions, 0);
    assert_eq!(status.total_bytes_reclaimed, 0);
    // The denominator is what makes the "fresh database" claim mean something:
    // without it, every field above reads zero whether it was observed or not.
    assert_eq!(status.status_refreshes, 0);
}

/// Compaction on an empty database should succeed without error.
#[tokio::test]
async fn test_compact_empty_database() {
    let dir = tempdir().unwrap();
    let (storage, _schema) = setup_storage(dir.path()).await;

    let stats = storage.compact().await;
    assert!(
        stats.is_ok(),
        "Compaction should succeed on empty DB: {:?}",
        stats
    );
}
