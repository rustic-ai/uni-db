// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
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
    let status = storage.compaction_status().unwrap();
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
    assert_eq!(stats.tables_optimized, 0); // Empty DB
}

/// Helper: create schema with Person label and KNOWS edge type.
async fn setup_schema_and_storage(
    db_path_str: &str,
    config: UniConfig,
) -> (Arc<StorageManager>, Arc<SchemaManager>, u32) {
    let store = Arc::new(LocalFileSystem::new_with_prefix(db_path_str).unwrap());
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &schema_path)
            .await
            .unwrap(),
    );
    let _label_id = schema_manager.add_label("Person").unwrap();
    let edge_type_id = schema_manager
        .add_edge_type(
            "KNOWS",
            vec!["Person".to_string()],
            vec!["Person".to_string()],
        )
        .unwrap();
    schema_manager.save().await.unwrap();

    let storage = Arc::new(
        StorageManager::new_with_config(db_path_str, schema_manager.clone(), config.clone())
            .await
            .unwrap(),
    );
    (storage, schema_manager, edge_type_id)
}

/// Helper: insert two Person vertices and a KNOWS edge, then flush.
async fn write_and_flush(
    storage: &Arc<StorageManager>,
    schema_manager: &Arc<SchemaManager>,
    edge_type_id: u32,
    config: UniConfig,
) {
    let writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        config,
        None,
        None,
    )
    .await
    .unwrap();

    let v1 = writer.next_vid().await.unwrap();
    let v2 = writer.next_vid().await.unwrap();
    writer
        .insert_vertex_with_labels(v1, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();
    writer
        .insert_vertex_with_labels(v2, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();

    let eid = writer.next_eid(edge_type_id).await.unwrap();
    writer
        .insert_edge(v1, v2, edge_type_id, eid, HashMap::new(), None, None)
        .await
        .unwrap();

    writer.flush_to_l1(None).await.unwrap();
}

/// Helper: run background compaction for a given duration, then shut it down
/// and return the final compaction status.
/// Run the background compaction loop for `run_duration` of **virtual** time.
///
/// Every caller is a `#[tokio::test(start_paused = true)]`, so the clock only
/// advances when this function advances it — and only when every task is idle.
/// That makes tick delivery deterministic and costs no wall-clock: the same
/// coverage that used to take a 2-second real sleep now takes milliseconds.
///
/// The stepping is load-bearing. A single bulk `advance(run_duration)` fires
/// the timers and returns before the loop has polled them, so the shutdown
/// below wins the `select!` and no compaction ever runs. Advancing in small
/// steps and yielding between lets each tick's work — which is real Lance IO
/// through `spawn_blocking` — finish before the clock moves again.
async fn run_compaction_cycle(
    storage: &Arc<StorageManager>,
    run_duration: Duration,
) -> uni_store::compaction::CompactionStatus {
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let handle = storage.clone().start_background_compaction(shutdown_rx);

    let step = Duration::from_millis(50);
    let steps = (run_duration.as_millis() / step.as_millis()).max(1);
    for _ in 0..steps {
        tokio::time::advance(step).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    let _ = shutdown_tx.send(());
    handle.await.unwrap();

    storage.compaction_status().unwrap()
}

/// Verify that background compaction runs Tier 2 semantic compaction
/// (L1 deltas cleared, total_compactions >= 1).
#[tokio::test(start_paused = true)]
async fn test_background_compaction_runs_semantic() {
    let dir = tempdir().unwrap();
    let db_path_str = dir.path().to_str().unwrap();

    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.max_l1_runs = 1; // Low threshold to trigger immediately
    config.compaction.check_interval = Duration::from_millis(200);

    let (storage, schema_manager, edge_type_id) =
        setup_schema_and_storage(db_path_str, config.clone()).await;

    write_and_flush(&storage, &schema_manager, edge_type_id, config).await;

    // Verify L1 has data before background compaction
    let fwd_table_name = uni_store::backend::table_names::delta_table_name("KNOWS", "fwd");
    let pre_count = storage
        .backend()
        .count_rows(&fwd_table_name, None)
        .await
        .unwrap();
    assert!(
        pre_count > 0,
        "Delta table should have rows before compaction"
    );

    let status = run_compaction_cycle(&storage, Duration::from_secs(2)).await;

    assert!(
        status.total_compactions >= 1,
        "Background compaction should have run at least once, got {}",
        status.total_compactions
    );
    assert_eq!(
        status.l1_runs, 0,
        "l1_runs should be 0 after semantic compaction cleared deltas"
    );
}

/// Verify that the BySize trigger fires when max_l1_size_bytes is exceeded.
#[tokio::test(start_paused = true)]
async fn test_compaction_by_size_trigger() {
    let dir = tempdir().unwrap();
    let db_path_str = dir.path().to_str().unwrap();

    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.max_l1_runs = 100; // High -- won't trigger by run count
    config.compaction.max_l1_size_bytes = 1; // Very low -- triggers on any data
    config.compaction.check_interval = Duration::from_millis(200);

    let (storage, schema_manager, edge_type_id) =
        setup_schema_and_storage(db_path_str, config.clone()).await;

    write_and_flush(&storage, &schema_manager, edge_type_id, config).await;

    let status = run_compaction_cycle(&storage, Duration::from_secs(2)).await;

    assert!(
        status.total_compactions >= 1,
        "BySize trigger should have fired, total_compactions={}",
        status.total_compactions
    );
}

/// Verify that the ByAge trigger fires when max_l1_age is exceeded.
#[tokio::test(start_paused = true)]
async fn test_compaction_by_age_trigger() {
    let dir = tempdir().unwrap();
    let db_path_str = dir.path().to_str().unwrap();

    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.max_l1_runs = 100; // High -- won't trigger by run count
    config.compaction.max_l1_size_bytes = u64::MAX; // High -- won't trigger by size
    config.compaction.max_l1_age = Duration::from_millis(100); // Low -- triggers on any aged data
    config.compaction.check_interval = Duration::from_millis(200);

    let (storage, schema_manager, edge_type_id) =
        setup_schema_and_storage(db_path_str, config.clone()).await;

    write_and_flush(&storage, &schema_manager, edge_type_id, config).await;

    // Wait for data to age past the threshold
    // A REAL sleep, deliberately. `oldest_l1_age` is computed from
    // `SystemTime::now()` (`storage/manager.rs`), not from tokio's clock, so a
    // paused-clock `tokio::time::sleep` would auto-advance instantly and the
    // data would never age past `max_l1_age`. Only the loop's ticking below is
    // virtual; the aging itself has to be real.
    std::thread::sleep(Duration::from_millis(200));

    let status = run_compaction_cycle(&storage, Duration::from_secs(2)).await;

    assert!(
        status.total_compactions >= 1,
        "ByAge trigger should have fired, total_compactions={}",
        status.total_compactions
    );
}

/// Verify that l1_runs only counts non-empty delta tables.
/// After semantic compaction clears deltas, l1_runs should be 0
/// even though the tables still exist.
#[tokio::test(start_paused = true)]
async fn test_l1_runs_counts_non_empty_only() {
    let dir = tempdir().unwrap();
    let db_path_str = dir.path().to_str().unwrap();

    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.max_l1_runs = 1;
    config.compaction.check_interval = Duration::from_millis(200);

    let (storage, schema_manager, edge_type_id) =
        setup_schema_and_storage(db_path_str, config.clone()).await;

    write_and_flush(&storage, &schema_manager, edge_type_id, config).await;

    let status = run_compaction_cycle(&storage, Duration::from_secs(2)).await;

    // If the delta table still exists after compaction, it should be empty
    let fwd_table_name = uni_store::backend::table_names::delta_table_name("KNOWS", "fwd");
    if let Ok(count) = storage.backend().count_rows(&fwd_table_name, None).await {
        assert_eq!(
            count, 0,
            "Delta table should be empty after semantic compaction"
        );
    }

    assert_eq!(
        status.l1_runs, 0,
        "l1_runs should be 0 -- empty tables should not be counted"
    );
}

/// Verify l1_estimated_bytes is computed from row counts, not hardcoded to 0.
#[tokio::test(start_paused = true)]
async fn test_compaction_status_tracks_data_size() {
    let dir = tempdir().unwrap();
    let db_path_str = dir.path().to_str().unwrap();

    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.max_l1_runs = 100; // High -- prevent triggering compaction
    config.compaction.max_l1_size_bytes = u64::MAX;
    config.compaction.check_interval = Duration::from_millis(200);

    let (storage, schema_manager, edge_type_id) =
        setup_schema_and_storage(db_path_str, config.clone()).await;

    write_and_flush(&storage, &schema_manager, edge_type_id, config).await;

    // Run just long enough for a status update cycle (no compaction will trigger)
    let status = run_compaction_cycle(&storage, Duration::from_millis(500)).await;

    assert!(
        status.l1_estimated_bytes > 0,
        "l1_estimated_bytes should be non-zero after writing data, got {}",
        status.l1_estimated_bytes
    );
    assert!(
        status.l1_runs > 0,
        "l1_runs should be non-zero before compaction"
    );
}

/// Verify that background compaction handles an empty DB gracefully
/// (no crash, no panic, 0 total_compactions).
#[tokio::test(start_paused = true)]
async fn test_background_compaction_handles_empty_db() {
    let dir = tempdir().unwrap();
    let db_path_str = dir.path().to_str().unwrap();

    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.max_l1_runs = 1;
    config.compaction.check_interval = Duration::from_millis(200);

    let (storage, _schema_manager, _edge_type_id) =
        setup_schema_and_storage(db_path_str, config).await;

    let status = run_compaction_cycle(&storage, Duration::from_secs(1)).await;

    assert_eq!(
        status.total_compactions, 0,
        "Empty DB should not trigger compaction"
    );
    assert_eq!(status.l1_runs, 0);
    assert_eq!(status.l1_estimated_bytes, 0);
}
