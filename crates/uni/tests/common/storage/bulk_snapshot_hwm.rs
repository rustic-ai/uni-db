// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The snapshot a bulk commit records covers the load.
//!
//! A bulk commit saves a snapshot manifest and moves the latest pointer to it,
//! but `save_load_snapshot` copied the previous manifest's
//! `version_high_water_mark` while the load's rows carry a newer `_version`.
//! Candidate from triage: pinning to that snapshot (`_version <= hwm`) hides
//! the load, and a reopen restarts the version counter below the load's
//! version, so later writes reuse it.

// Rust guideline compliant

use std::collections::HashMap;

use uni_db::{DataType, Uni, UniConfig, Value};

fn config() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

async fn count(session: &uni_db::Session, q: &str) -> i64 {
    session.query(q).await.unwrap().rows()[0]
        .get::<i64>("c")
        .unwrap()
}

/// A bulk load into a fresh database, its snapshot pinned: the load is there,
/// before and after a reopen, and a write made after the reopen is not.
#[tokio::test]
async fn pinning_the_bulk_snapshot_shows_the_load() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let snapshot_id = {
        let db = Uni::open(&path).config(config()).build().await.unwrap();
        db.schema()
            .label("P")
            .property("k", DataType::Int64)
            .apply()
            .await
            .unwrap();
        let tx = db.session().tx().await.unwrap();
        let mut bulk = tx.bulk_writer().build().unwrap();
        bulk.insert_vertices(
            "P",
            (0..20)
                .map(|k| HashMap::from([("k".to_string(), Value::Int(k))]))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        let stats = bulk.commit().await.unwrap();
        assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);
        tx.commit().await.unwrap();

        let latest = db
            .storage()
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .unwrap()
            .expect("the bulk commit records a snapshot");
        let mut pinned = db.session();
        pinned.pin_to_version(&latest.snapshot_id).await.unwrap();
        assert_eq!(
            count(&pinned, "MATCH (n:P) RETURN count(n) AS c").await,
            20,
            "the snapshot the load recorded must contain the load (hwm {})",
            latest.version_high_water_mark
        );
        db.shutdown().await.unwrap();
        latest.snapshot_id
    };

    let db = Uni::open(&path).config(config()).build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:P {k: 100})").await.unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();
    let live = db.session();
    assert_eq!(count(&live, "MATCH (n:P) RETURN count(n) AS c").await, 21);
    let mut pinned = db.session();
    pinned.pin_to_version(&snapshot_id).await.unwrap();
    assert_eq!(
        count(&pinned, "MATCH (n:P) RETURN count(n) AS c").await,
        20,
        "after a reopen the bulk snapshot must still hold the load and only the load: a \
         write after the reopen must not reuse a version at or below its ceiling"
    );
}

/// A session pinned to a snapshot taken before a bulk load must not see the
/// load. The load's rows in the shared `vertices` table were stamped
/// `_version = 1` (a leftover from before #266) while its per-label rows carried
/// the reserved version, so every ceiling admitted them: a label-less scan of
/// the earlier snapshot returned the load's vertices.
#[tokio::test]
async fn a_snapshot_before_a_bulk_load_does_not_contain_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(config())
        .build()
        .await
        .unwrap();
    db.schema()
        .label("P")
        .property("k", DataType::Int64)
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:P {k: -1})").await.unwrap();
    tx.commit().await.unwrap();
    let before = db.create_snapshot("before").await.unwrap();

    let tx = db.session().tx().await.unwrap();
    let mut bulk = tx.bulk_writer().build().unwrap();
    bulk.insert_vertices(
        "P",
        (0..20)
            .map(|k| HashMap::from([("k".to_string(), Value::Int(k))]))
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    bulk.commit().await.unwrap();
    tx.commit().await.unwrap();

    let mut pinned = db.session();
    pinned.pin_to_version(&before).await.unwrap();
    assert_eq!(
        (
            count(&pinned, "MATCH (n:P) RETURN count(n) AS c").await,
            count(&pinned, "MATCH (n) WHERE n.k >= -1 RETURN count(n) AS c").await,
        ),
        (1, 1),
        "the earlier snapshot holds only the vertex created before the load"
    );
    let live = db.session();
    assert_eq!(
        count(&live, "MATCH (n) WHERE n.k >= -1 RETURN count(n) AS c").await,
        21
    );
}

/// The snapshot a bulk commit records must not change after it is taken.
///
/// Its ceiling is the load's version, reserved at the load's first write, and
/// a pinned session reads L1 only. A vertex committed before the load (a lower
/// version) but still in L0 was therefore missing from the snapshot when it
/// was taken -- 20 rows -- and joined it once a later flush stored it under
/// the ceiling -- 21 rows (measured on 7b5856437). The vertex committed before
/// the load, so the snapshot must show it from the start, and keep showing
/// exactly that.
#[tokio::test]
async fn the_bulk_snapshot_holds_older_l0_rows_from_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(config())
        .build()
        .await
        .unwrap();
    db.schema()
        .label("P")
        .property("k", DataType::Int64)
        .apply()
        .await
        .unwrap();
    db.flush().await.unwrap();
    // Committed before the load, left in L0.
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:P {k: -1})").await.unwrap();
    tx.commit().await.unwrap();

    let tx = db.session().tx().await.unwrap();
    let mut bulk = tx.bulk_writer().build().unwrap();
    bulk.insert_vertices(
        "P",
        (0..20)
            .map(|k| HashMap::from([("k".to_string(), Value::Int(k))]))
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let stats = bulk.commit().await.unwrap();
    assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);
    tx.commit().await.unwrap();
    let snapshot = db
        .storage()
        .snapshot_manager()
        .load_latest_snapshot()
        .await
        .unwrap()
        .expect("the bulk commit records a snapshot")
        .snapshot_id;

    let pinned_count = || async {
        let mut pinned = db.session();
        pinned.pin_to_version(&snapshot).await.unwrap();
        count(&pinned, "MATCH (n:P) RETURN count(n) AS c").await
    };
    let at_commit = pinned_count().await;
    db.flush().await.unwrap();
    let after_flush = pinned_count().await;
    assert_eq!(
        (at_commit, after_flush),
        (21, 21),
        "the snapshot holds the vertex committed before the load and the load, from the \
         moment it is taken"
    );
}

/// Every version ceiling captured while a load is staged excludes the load for
/// good.
///
/// A load reserves its version at its first staged write and becomes visible
/// only at commit. A ceiling captured in between covers that version: a
/// transaction begun then, a flush manifest cut then, and an explicit snapshot
/// taken then. Each must keep reading the state from before the load once it
/// is published, so the load is visible all at once, to exactly the readers
/// that start after its commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ceilings_captured_while_a_load_is_staged_exclude_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let (manifest_time, snapshot) = {
        let db = Uni::open(&path).config(config()).build().await.unwrap();
        db.schema()
            .label("P")
            .property("k", DataType::Int64)
            .apply()
            .await
            .unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:P {k: -1})").await.unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();

        let loader = db.session();
        let load_tx = loader.tx().await.unwrap();
        let mut bulk = load_tx.bulk_writer().batch_size(5).build().unwrap();
        bulk.insert_vertices(
            "P",
            (0..20)
                .map(|k| HashMap::from([("k".to_string(), Value::Int(k))]))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();

        // While the load is staged: another commit, a flush, a snapshot and a
        // transaction that reads before and after the load commits.
        db.session()
            .execute_with_retry("CREATE (:P {k: 100})")
            .await
            .unwrap();
        db.flush().await.unwrap();
        let manifest_time = chrono::Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let snapshot = db.create_snapshot("during").await.unwrap();
        let reader = db.session();
        let reader_tx = reader.tx().await.unwrap();
        assert_eq!(
            reader_tx
                .query("MATCH (n:P) RETURN count(n) AS c")
                .await
                .unwrap()
                .rows()[0]
                .get::<i64>("c")
                .unwrap(),
            2,
            "premise: the staged load is invisible"
        );

        let stats = bulk.commit().await.unwrap();
        assert!(stats.warnings.is_empty(), "{:?}", stats.warnings);
        load_tx.commit().await.unwrap();

        assert_eq!(
            count(&db.session(), "MATCH (n:P) RETURN count(n) AS c").await,
            22,
            "a session started after the commit sees the load"
        );
        for q in [
            "MATCH (n:P) RETURN count(n) AS c",
            "MATCH (n) WHERE n.k >= -1 RETURN count(n) AS c",
        ] {
            assert_eq!(
                reader_tx.query(q).await.unwrap().rows()[0]
                    .get::<i64>("c")
                    .unwrap(),
                2,
                "a transaction begun while the load was staged must not see it appear: {q}"
            );
        }
        reader_tx.rollback();
        let mut pinned = db.session();
        pinned.pin_to_timestamp(manifest_time).await.unwrap();
        assert_eq!(
            count(&pinned, "MATCH (n) WHERE n.k >= -1 RETURN count(n) AS c").await,
            2,
            "the flush manifest cut while the load was staged"
        );
        let mut pinned = db.session();
        pinned.pin_to_version(&snapshot).await.unwrap();
        assert_eq!(
            count(&pinned, "MATCH (n) WHERE n.k >= -1 RETURN count(n) AS c").await,
            2,
            "the snapshot taken while the load was staged"
        );
        db.shutdown().await.unwrap();
        (manifest_time, snapshot)
    };

    let db = Uni::open(&path).config(config()).build().await.unwrap();
    assert_eq!(
        count(&db.session(), "MATCH (n:P) RETURN count(n) AS c").await,
        22
    );
    let mut pinned = db.session();
    pinned.pin_to_timestamp(manifest_time).await.unwrap();
    assert_eq!(
        count(&pinned, "MATCH (n) WHERE n.k >= -1 RETURN count(n) AS c").await,
        2,
        "the flush manifest after a reopen"
    );
    let mut pinned = db.session();
    pinned.pin_to_version(&snapshot).await.unwrap();
    assert_eq!(
        count(&pinned, "MATCH (n) WHERE n.k >= -1 RETURN count(n) AS c").await,
        2,
        "the snapshot after a reopen"
    );
}
