// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use uni_common::config::UniConfig;
use uni_common::core::schema::SchemaManager;
use uni_store::Writer;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn test_compaction_configuration() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();
    let db_path_str = db_path.to_str().unwrap();

    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.max_l1_runs = 2; // Low threshold for testing
    config.compaction.check_interval = Duration::from_millis(100);

    let store = Arc::new(LocalFileSystem::new_with_prefix(db_path).unwrap());
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store.clone(), &schema_path)
            .await
            .unwrap(),
    );

    let storage = Arc::new(
        StorageManager::new_with_config(db_path_str, schema_manager, config.clone())
            .await
            .unwrap(),
    );

    // Test that configuration is correctly propagated
    let status = storage.compaction_status();
    assert_eq!(status.l1_runs, 0);
    assert!(!status.compaction_in_progress);
}

#[tokio::test]
async fn test_write_throttling_config() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();
    let db_path_str = db_path.to_str().unwrap();

    let mut config = UniConfig::default();
    config.throttle.soft_limit = 2;
    config.throttle.hard_limit = 4;
    config.throttle.base_delay = Duration::from_millis(50);

    let store = Arc::new(LocalFileSystem::new_with_prefix(db_path).unwrap());
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store.clone(), &schema_path)
            .await
            .unwrap(),
    );
    let storage = Arc::new(
        StorageManager::new_with_config(db_path_str, schema_manager.clone(), config.clone())
            .await
            .unwrap(),
    );
    let _writer = Writer::new_with_config(storage.clone(), schema_manager, 0, config, None, None)
        .await
        .unwrap();

    // Ideally we would mock the storage state to simulate high L1 runs,
    // but for now we just verify the Writer can be created with the config.
    // In a real scenario, we would inject a mock storage or manually force L1 runs.

    // Asserting the writer is alive (this is a smoke test until we implement the throttling logic)
}

#[tokio::test]
async fn test_manual_compaction_trigger() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();
    let db_path_str = db_path.to_str().unwrap();

    let config = UniConfig::default();
    let store = Arc::new(LocalFileSystem::new_with_prefix(db_path).unwrap());
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store.clone(), &schema_path)
            .await
            .unwrap(),
    );
    let storage = Arc::new(
        StorageManager::new_with_config(db_path_str, schema_manager, config)
            .await
            .unwrap(),
    );

    // Should return result, even if empty
    let result = storage.compact().await;
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.files_compacted, 0); // Empty DB
}
