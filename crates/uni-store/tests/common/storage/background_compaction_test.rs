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
    let _writer =
        Writer::new_with_config(storage.clone(), schema_manager, 0, config, None, None, None)
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

/// A bare `StorageManager` over a fresh directory with compaction enabled.
async fn lock_test_storage(dir: &std::path::Path) -> Arc<StorageManager> {
    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.check_interval = Duration::from_millis(20);
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json"))
            .await
            .unwrap(),
    );
    Arc::new(
        StorageManager::new_with_config(dir.to_str().unwrap(), schema_manager, config)
            .await
            .unwrap(),
    )
}

/// The directory lock excludes a second holder, including one in this
/// process: two handles on one path are exactly the drop-and-reopen case.
#[tokio::test]
async fn compaction_dir_lock_is_exclusive_within_a_process() {
    use uni_store::storage::compaction::CompactionDirLock;
    let dir = tempdir().unwrap();
    let first = CompactionDirLock::try_acquire(dir.path()).unwrap();
    assert!(first.is_some(), "an unheld lock must be acquirable");
    assert!(
        CompactionDirLock::try_acquire(dir.path())
            .unwrap()
            .is_none(),
        "a second handle acquired a held compaction lock"
    );
    drop(first);
    assert!(
        CompactionDirLock::try_acquire(dir.path())
            .unwrap()
            .is_some(),
        "the lock must be released when its holder drops"
    );
}

/// A semantic pass (the `VACUUM` route) must not start while another handle
/// on the same directory holds the compaction lock, and must run once it is
/// released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_all_waits_for_a_foreign_compaction() {
    use uni_store::storage::compaction::{CompactionDirLock, Compactor};
    let dir = tempdir().unwrap();
    let storage = lock_test_storage(dir.path()).await;

    let foreign = CompactionDirLock::try_acquire(dir.path()).unwrap().unwrap();
    let pass = tokio::spawn({
        let storage = storage.clone();
        async move { Compactor::new(storage).compact_all().await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !pass.is_finished(),
        "compact_all ran while another handle held the directory's compaction lock"
    );
    drop(foreign);
    tokio::time::timeout(Duration::from_secs(30), pass)
        .await
        .expect("compact_all did not proceed after the lock was released")
        .unwrap()
        .unwrap();
}

/// Cancelling compaction stops the background loop by itself, without the
/// shutdown broadcast: `Drop for Uni` relies on it to stop a loop promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_compaction_stops_the_background_loop() {
    let dir = tempdir().unwrap();
    let storage = lock_test_storage(dir.path()).await;
    // The sender stays alive and silent for the whole test.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let handle = storage.clone().start_background_compaction(shutdown_rx);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !handle.is_finished(),
        "the loop exited before it was cancelled"
    );

    storage.cancel_compaction();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the compaction loop kept running after cancel_compaction")
        .unwrap();
}

// ── Write backpressure ─────────────────────────────────────────────────────
//
// These drive `Writer::check_write_pressure` through a real write by setting
// the flush-generation counter (`CompactionStatus::l1_runs`) directly — the
// same counter `flush_to_l1` increments and a compaction run resets. They run
// on a paused clock, so the waits are measured in virtual time: deterministic,
// and an 81.92 s sleep costs nothing to observe.

/// Hard-limit poll period, mirrored from the writer: the soft path must notice
/// relief at least this promptly.
const PRESSURE_POLL: Duration = Duration::from_millis(100);

/// Build a writer over a fresh store with `throttle` applied and the
/// flush-generation counter preset to `l1_runs`.
///
/// Compaction is enabled in the writer's config, so the throttle waits for
/// relief; no background loop runs in a bare writer, and the tests relieve
/// the pressure themselves through [`set_l1_runs`].
async fn writer_under_pressure(
    db_path_str: &str,
    throttle: uni_common::config::WriteThrottleConfig,
    l1_runs: usize,
) -> (Arc<StorageManager>, Writer) {
    writer_under_pressure_with(db_path_str, throttle, l1_runs, true).await
}

/// [`writer_under_pressure`] with the compaction switch given.
async fn writer_under_pressure_with(
    db_path_str: &str,
    throttle: uni_common::config::WriteThrottleConfig,
    l1_runs: usize,
    compaction_enabled: bool,
) -> (Arc<StorageManager>, Writer) {
    let mut config = UniConfig::default();
    config.compaction.enabled = compaction_enabled;
    config.throttle = throttle;
    let (storage, schema_manager, _) = setup_schema_and_storage(db_path_str, config.clone()).await;
    let writer =
        Writer::new_with_config(storage.clone(), schema_manager, 0, config, None, None, None)
            .await
            .unwrap();
    set_l1_runs(&storage, l1_runs);
    (storage, writer)
}

fn set_l1_runs(storage: &StorageManager, l1_runs: usize) {
    storage.compaction_status.lock().unwrap().l1_runs = l1_runs;
}

/// One vertex write, returning how long it took in (virtual) time.
async fn timed_write(writer: &Writer) -> Duration {
    let vid = writer.next_vid().await.unwrap();
    let started = tokio::time::Instant::now();
    writer
        .insert_vertex_with_labels(vid, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();
    started.elapsed()
}

/// A soft-throttled write must end as soon as compaction drops the run count
/// below `soft_limit`, not finish a delay computed from the count it saw first.
///
/// At 28 runs the default curve gives 10 ms × 2^12 = 40.96 s. That one
/// uninterruptible sleep outlived the 30 s statement timeout, which is how
/// `fork_point_atomic_under_concurrent_parent_writes_async_flush` failed with
/// `Timeout { 30000 }` while the background compaction had long since reset
/// the counter.
#[tokio::test(start_paused = true)]
async fn soft_throttle_ends_when_compaction_relieves_pressure() {
    let dir = tempdir().unwrap();
    let (storage, writer) = writer_under_pressure(
        dir.path().to_str().unwrap(),
        uni_common::config::WriteThrottleConfig::default(),
        28,
    )
    .await;

    let relief_after = Duration::from_millis(250);
    let relief_storage = storage.clone();
    let relief = tokio::spawn(async move {
        tokio::time::sleep(relief_after).await;
        set_l1_runs(&relief_storage, 0);
    });

    let waited = timed_write(&writer).await;
    relief.await.unwrap();

    assert!(
        waited >= relief_after,
        "the write must actually be throttled while pressure lasts; waited {waited:?}"
    );
    assert!(
        waited <= relief_after + PRESSURE_POLL,
        "the write must return within one poll period of relief ({relief_after:?}); \
         waited {waited:?}"
    );
}

/// Sustained soft pressure is a slowdown, not a stall: the wait is bounded
/// by one hard-limit poll period per run of the soft band, here
/// 100 ms × (32 − 16) = 1.6 s, rather than the 81.92 s the curve gives at 29
/// runs.
#[tokio::test(start_paused = true)]
async fn soft_throttle_wait_is_bounded_under_sustained_pressure() {
    let dir = tempdir().unwrap();
    let throttle = uni_common::config::WriteThrottleConfig::default();
    let (_storage, writer) =
        writer_under_pressure(dir.path().to_str().unwrap(), throttle, 29).await;

    let budget = PRESSURE_POLL * (throttle.hard_limit - throttle.soft_limit) as u32;
    let waited = timed_write(&writer).await;

    assert!(
        waited >= budget,
        "sustained pressure must still throttle for the full budget {budget:?}; \
         waited {waited:?}"
    );
    assert!(
        waited <= budget + PRESSURE_POLL,
        "the soft wait must stop at its budget {budget:?}; waited {waited:?}"
    );
}

/// Below the cap the original exponential curve is unchanged: one run over
/// `soft_limit` costs `base_delay × 2`.
#[tokio::test(start_paused = true)]
async fn soft_throttle_keeps_the_exponential_curve_below_the_budget() {
    let dir = tempdir().unwrap();
    let throttle = uni_common::config::WriteThrottleConfig::default();
    let (_storage, writer) =
        writer_under_pressure(dir.path().to_str().unwrap(), throttle, 17).await;

    let expected = throttle.base_delay * 2;
    let waited = timed_write(&writer).await;

    assert!(
        waited >= expected && waited <= expected + Duration::from_millis(5),
        "17 runs at soft_limit 16 must wait base_delay × 2 = {expected:?}; waited {waited:?}"
    );
}

/// The backoff exponent and the `base_delay` multiplication saturate rather
/// than overflow: a huge `base_delay` with a deep excess waits the budget.
///
/// Before the fix `base_delay * 2^31` panicked with "overflow when
/// multiplying duration" for any `base_delay` above about 8.6e9 s.
#[tokio::test(start_paused = true)]
async fn soft_throttle_backoff_saturates_instead_of_overflowing() {
    let dir = tempdir().unwrap();
    let throttle = uni_common::config::WriteThrottleConfig {
        soft_limit: 16,
        hard_limit: 16 + 1000,
        base_delay: Duration::from_secs(1 << 40),
    };
    let (_storage, writer) =
        writer_under_pressure(dir.path().to_str().unwrap(), throttle, 16 + 500).await;

    let budget = PRESSURE_POLL * 1000;
    let waited = timed_write(&writer).await;

    assert!(
        waited >= budget && waited <= budget + PRESSURE_POLL,
        "a saturated backoff must wait exactly the budget {budget:?}; waited {waited:?}"
    );
}

/// The hard limit still stalls writes until compaction relieves it, and the
/// soft band's budget does not let a write through at or above it.
#[tokio::test(start_paused = true)]
async fn hard_limit_stalls_until_pressure_drops() {
    let dir = tempdir().unwrap();
    let (storage, writer) = writer_under_pressure(
        dir.path().to_str().unwrap(),
        uni_common::config::WriteThrottleConfig::default(),
        32,
    )
    .await;

    // Well past the soft budget, so a write released by it would show.
    let relief_after = Duration::from_secs(10);
    let relief_storage = storage.clone();
    let relief = tokio::spawn(async move {
        tokio::time::sleep(relief_after).await;
        set_l1_runs(&relief_storage, 0);
    });

    let waited = timed_write(&writer).await;
    relief.await.unwrap();

    assert!(
        waited >= relief_after && waited <= relief_after + PRESSURE_POLL,
        "a hard-limited write must wait for relief at {relief_after:?}; waited {waited:?}"
    );
}

/// With compaction disabled nothing will lower the count: the soft band does
/// not wait, and the hard limit refuses the write at once with the typed
/// `WriteStalled` instead of polling forever.
#[tokio::test(start_paused = true)]
async fn compaction_disabled_soft_band_proceeds_and_hard_limit_refuses() {
    let dir = tempdir().unwrap();
    let (storage, writer) = writer_under_pressure_with(
        dir.path().to_str().unwrap(),
        uni_common::config::WriteThrottleConfig::default(),
        28,
        false,
    )
    .await;
    assert_eq!(
        timed_write(&writer).await,
        Duration::ZERO,
        "soft band, compaction off"
    );
    assert_eq!(writer.write_throttle_stats().waits, 0);

    set_l1_runs(&storage, 32);
    let vid = writer.next_vid().await.unwrap();
    let err = writer
        .insert_vertex_with_labels(vid, HashMap::new(), &["Person".to_string()], None)
        .await
        .expect_err("hard limit with compaction off must refuse");
    match err.downcast_ref::<uni_common::UniError>() {
        Some(uni_common::UniError::WriteStalled {
            l1_runs: 32,
            hard_limit: 32,
        }) => {}
        other => panic!("expected WriteStalled, got {other:?} ({err})"),
    }
    assert_eq!(writer.write_throttle_stats().stalled, 1);
}

/// `write_throttle_pressure` is 0 below the soft limit, rises through the
/// soft band, and is 1 at the hard limit; with compaction off the soft band
/// (which does not wait) reads 0.
#[test]
fn write_throttle_pressure_follows_the_bands() {
    use uni_store::runtime::writer::write_throttle_pressure as p;
    let t = uni_common::config::WriteThrottleConfig::default(); // 16 / 32
    assert_eq!(p(&t, true, 0), 0.0);
    assert_eq!(p(&t, true, 15), 0.0);
    let first = p(&t, true, 16);
    let last = p(&t, true, 31);
    assert!(first > 0.0 && first < last && last < 1.0, "{first} {last}");
    assert_eq!(p(&t, true, 32), 1.0);
    assert_eq!(p(&t, true, 99), 1.0);
    assert_eq!(p(&t, false, 20), 0.0);
    assert_eq!(p(&t, false, 32), 1.0);
}

/// A soft-band wait is counted once, with its duration.
#[tokio::test(start_paused = true)]
async fn throttle_waits_are_counted() {
    let dir = tempdir().unwrap();
    let (_storage, writer) = writer_under_pressure(
        dir.path().to_str().unwrap(),
        uni_common::config::WriteThrottleConfig::default(),
        20,
    )
    .await;
    let waited = timed_write(&writer).await;
    let stats = writer.write_throttle_stats();
    assert_eq!(stats.waits, 1);
    assert_eq!(stats.wait_time, waited);
    assert_eq!(stats.stalled, 0);
}
