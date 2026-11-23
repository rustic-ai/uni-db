// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for Issue #72: Version Counter Reset on Manifest Loss
//!
//! Verifies that the database detects lost manifest pointers and fails loudly
//! instead of silently resetting the version counter to 0, which would cause
//! data corruption.

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_common::core::snapshot::SnapshotManifest;
use uni_db::{Uni, UniConfig};
use uni_store::runtime::wal::WriteAheadLog;
use uni_store::storage::manager::StorageManager;
use uni_store::store_utils::{DEFAULT_TIMEOUT, delete_with_timeout, list_with_timeout};

/// A fixture database with the time-based auto-flush timer disabled.
///
/// `Drop for Uni` broadcasts shutdown, and the auto-flush task used to answer
/// with a full `flush_to_l1` that **nothing awaits**. With the WAL on it no
/// longer flushes on that signal, but the timer itself would still flush
/// mid-test, so it stays off. These tests delete manifest files
/// after the `Uni` goes out of scope, so that un-awaited flush can land *after*
/// the deletion and put a manifest back — turning "no manifest" into "manifest
/// present" and the assertion into a coin flip. It passed alone and failed in
/// the full failpoints suite, which is exactly the shape of a load-dependent
/// race rather than a logic error.
fn quiesced(path: &str) -> uni_db::UniBuilder {
    Uni::open(path).config(UniConfig {
        auto_flush_interval: None,
        ..Default::default()
    })
}

/// Test 1: Fresh database starts at version zero
#[tokio::test]
async fn test_fresh_database_starts_at_version_zero() -> Result<()> {
    let _db = Uni::temporary().build().await?;
    // If we get here, the database started successfully with version 0
    Ok(())
}

/// Test 2: Lost latest pointer recovers from manifest
#[tokio::test]
async fn test_lost_latest_pointer_recovers_from_manifest() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();

    // Create database, insert data, and flush
    {
        let db = quiesced(path).build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (n:Person {name: 'Alice'})").await?;
        tx.commit().await?;
        db.flush().await?;
    }

    // Manifests live under the STORAGE root, not the database root: the
    // `SnapshotManager`'s object store is built from `base_uri`, which is
    // `<db>/storage`. Listing `catalog/manifests` from `dir.path()` finds the
    // root `catalog/`, which holds only `schema.json` — so it came back empty,
    // the early return below fired, and this test silently skipped on every run
    // it has ever had. Asserting non-empty is what keeps it honest.
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?)
        as Arc<dyn object_store::ObjectStore>;
    let manifests_prefix = ObjectStorePath::from("storage/catalog/manifests");
    let metas = list_with_timeout(&store, Some(&manifests_prefix), DEFAULT_TIMEOUT).await?;

    assert!(
        !metas.is_empty(),
        "no manifest under storage/catalog/manifests after an explicit flush — \
         the rest of this test would be vacuous"
    );

    // Delete the latest pointer file, keeping the manifests it pointed at.
    let latest_path = ObjectStorePath::from("storage/catalog/latest");
    delete_with_timeout(&store, &latest_path, DEFAULT_TIMEOUT).await?;

    // Reopen - should recover from manifest (success means version recovery worked)
    let db_result = Uni::open(path).build().await;
    if let Err(e) = &db_result {
        panic!(
            "Database should recover from manifest when latest pointer is missing: {}",
            e
        );
    }

    // If we get here, database successfully recovered from manifest
    Ok(())
}

/// Test 3: L1 data with lost manifests fails loudly.
///
/// This is issue #72's actual shape and the one that must keep failing: L1 holds
/// rows whose versions came from a counter this open cannot reconstruct, because
/// the WAL is truncated at each flush and so does not bound what L1 already used.
/// Replaying from 0 over it would reuse those versions and corrupt data.
///
/// Two defects used to make this pass for the wrong reason, and they cancelled
/// out. It never flushed ("data only in WAL"), and it deleted `catalog/*` at the
/// database root while manifests live under `<db>/storage/catalog/`. So it
/// deleted nothing, no manifest ever existed to delete, and the guard fired on
/// the *absence* of a first flush — issue #275's case, not #72's. Whether a
/// table existed at all came down to a race with the un-awaited flush in
/// `Drop for Uni`.
///
/// The #275 case now has its own coverage in `first_flush_resilience.rs`, where
/// it must *succeed*. This one flushes explicitly so there is real L1 data, and
/// removes the manifests that describe it.
#[tokio::test]
async fn test_wal_without_manifest_fails_loudly() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();

    // Create database, insert, and FLUSH so L1 data and a manifest both exist.
    {
        let db = quiesced(path).build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (n:Person {name: 'Alice'})").await?;
        tx.commit().await?;
        db.flush().await?;

        // A second uncommitted-to-L1 write, so WAL segments outlive the flush
        // and the reopen genuinely sees "WAL + tables + no manifest".
        let tx = db.session().tx().await?;
        tx.execute("CREATE (n:Person {name: 'Bob'})").await?;
        tx.commit().await?;
    }

    // Delete all manifests AND the latest pointer, keeping the L1 data they
    // describe. Paths are relative to the STORAGE root — see test 2.
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?)
        as Arc<dyn object_store::ObjectStore>;
    let latest_path = ObjectStorePath::from("storage/catalog/latest");
    let manifests_prefix = ObjectStorePath::from("storage/catalog/manifests");

    // Delete latest pointer
    delete_with_timeout(&store, &latest_path, DEFAULT_TIMEOUT).await?;

    // Delete all manifests
    let metas = list_with_timeout(&store, Some(&manifests_prefix), DEFAULT_TIMEOUT).await?;
    assert!(
        !metas.is_empty(),
        "no manifests to delete — this test would then assert against issue \
         #275's fresh-store case, which is now expected to succeed"
    );
    for meta in metas {
        delete_with_timeout(&store, &meta.location, DEFAULT_TIMEOUT).await?;
    }

    // Try to reopen - should fail loudly
    let result = Uni::open(path).build().await;
    assert!(
        result.is_err(),
        "Database should fail when WAL exists but no manifests"
    );

    let error_msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Expected error but got success"),
    };
    assert!(
        error_msg.contains("WAL segments"),
        "Error message should mention WAL segments, got: {}",
        error_msg
    );
    assert!(
        error_msg.contains("no snapshot manifest"),
        "Error message should mention missing snapshot manifest, got: {}",
        error_msg
    );
    // The discriminator between #72 and #275 is whether L1 tables exist, so the
    // error has to say so. Without this, a regression that stopped checking
    // tables — and so rejected every unflushed store again — would still pass
    // both assertions above.
    assert!(
        error_msg.contains("table(s)"),
        "Error should report the L1 tables that make version reuse unsafe, got: {}",
        error_msg
    );

    Ok(())
}

/// Test 4: WAL has_segments() detection
#[tokio::test]
async fn test_wal_has_segments() -> Result<()> {
    let dir = tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let wal_path = ObjectStorePath::from("wal");

    let wal = WriteAheadLog::new(store.clone(), wal_path);

    // Initially empty
    let has_segments = wal.has_segments().await?;
    assert!(!has_segments, "Empty WAL should have no segments");

    // After append and flush
    use uni_common::core::id::Vid;
    use uni_store::runtime::wal::Mutation;

    wal.append(Mutation::InsertVertex {
        vid: Vid::new(1),
        properties: HashMap::new(),
        labels: vec!["Test".to_string()],
        created_at: None,
        updated_at: None,
    })?;
    wal.flush().await?;

    let has_segments_after = wal.has_segments().await?;
    assert!(has_segments_after, "WAL should have segments after flush");

    Ok(())
}

/// Test 5: SnapshotManager has_any_manifests() detection
#[tokio::test]
async fn test_snapshot_manager_has_any_manifests() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager =
        Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
    let _label_id = schema_manager.add_label("Test")?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let snapshot_mgr = storage.snapshot_manager();

    // Initially empty
    let has_manifests = snapshot_mgr.has_any_manifests().await?;
    assert!(!has_manifests, "Fresh database should have no manifests");

    // After saving a snapshot
    let manifest = SnapshotManifest {
        snapshot_id: "test_snapshot_1".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        parent_snapshot: None,
        schema_version: 1,
        version_high_water_mark: 10,
        wal_high_water_mark: 5,
        vertices: HashMap::new(),
        edges: HashMap::new(),
        writer_features: Vec::new(),
        unpublished_bulk_versions: Vec::new(),
        next_vid: None,
        next_eid: None,
        tables: Default::default(),
    };

    snapshot_mgr.save_snapshot(&manifest).await?;

    let has_manifests_after = snapshot_mgr.has_any_manifests().await?;
    assert!(has_manifests_after, "Should have manifests after save");

    Ok(())
}

/// A lost `latest` pointer is recovered from the NEWEST manifest.
///
/// Recovery used to take the last snapshot id in listing order, but snapshot
/// ids are random UUIDs, so that picked an arbitrary manifest and could roll
/// the version counter back below versions already in the tables. The ids are
/// rewritten here so the lexically last one is the oldest manifest, which makes
/// the old behaviour fail deterministically.
#[tokio::test]
async fn test_lost_latest_pointer_recovers_from_the_newest_manifest() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    {
        let db = quiesced(path).build().await?;
        for name in ["Alice", "Bob", "Carol"] {
            let tx = db.session().tx().await?;
            tx.execute(&format!("CREATE (n:Person {{name: '{name}'}})"))
                .await?;
            tx.commit().await?;
            db.flush().await?;
        }
        db.shutdown().await?;
    }

    let manifests_dir = dir.path().join("storage/catalog/manifests");
    let mut manifests: Vec<(u64, std::path::PathBuf, serde_json::Value)> =
        std::fs::read_dir(&manifests_dir)?
            .flatten()
            .map(|e| {
                let v: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(e.path()).unwrap()).unwrap();
                (v["version_high_water_mark"].as_u64().unwrap(), e.path(), v)
            })
            .collect();
    assert!(manifests.len() >= 3, "{} manifests", manifests.len());
    manifests.sort_by_key(|(hwm, _, _)| *hwm);
    let newest_hwm = manifests.last().unwrap().0;
    // Oldest gets the lexically largest id, newest the smallest.
    let count = manifests.len();
    // Timestamps reversed too, so neither the id nor the creation time alone
    // picks the newest manifest.
    let mut stamps: Vec<serde_json::Value> = manifests
        .iter()
        .map(|(_, _, v)| v["created_at"].clone())
        .collect();
    stamps.reverse();
    for (i, (_, file, mut v)) in manifests.into_iter().enumerate() {
        let id = format!("{:08}", count - i);
        v["snapshot_id"] = serde_json::json!(id);
        v["created_at"] = stamps[i].clone();
        v["parent_snapshot"] = serde_json::Value::Null;
        std::fs::remove_file(&file)?;
        std::fs::write(
            manifests_dir.join(format!("{id}.json")),
            serde_json::to_vec(&v)?,
        )?;
    }
    std::fs::remove_file(dir.path().join("storage/catalog/latest"))?;

    let db = quiesced(path).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (n:Person {name: 'Dave'})").await?;
    tx.commit().await?;
    db.flush().await?;
    let rows = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .await?;
    let names: Vec<String> = rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("name"))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(names, ["Alice", "Bob", "Carol", "Dave"]);
    db.shutdown().await?;

    // The manifest the reopened store published continues past the newest
    // version recovered, rather than restarting below it.
    let latest_id = std::fs::read_to_string(dir.path().join("storage/catalog/latest"))?;
    let latest: serde_json::Value = serde_json::from_slice(&std::fs::read(
        manifests_dir.join(format!("{}.json", latest_id.trim())),
    )?)?;
    let hwm = latest["version_high_water_mark"].as_u64().unwrap();
    assert!(
        hwm > newest_hwm,
        "recovered below the newest manifest: published {hwm}, newest was {newest_hwm}"
    );
    Ok(())
}
