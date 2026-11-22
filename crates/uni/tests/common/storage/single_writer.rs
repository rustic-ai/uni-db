// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! One writable handle per store (issue #348).
//!
//! A local store is held with an OS lock, an object store with a lease (the
//! S3 cases are in `hybrid_localstack_e2e`). Read-only handles take neither.

use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::api::multi_agent::{LeaseGuard, WriteLease, WriteLeaseProvider};
use uni_db::{DataType, Uni, UniConfig, UniError};

fn quick_grace() -> UniConfig {
    let mut config = UniConfig::default();
    config.writer_lease.acquire_grace = Duration::from_millis(300);
    config
}

async fn seeded(path: &str) -> Result<Uni> {
    let db = Uni::open(path).config(quick_grace()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

async fn count(db: &Uni) -> Result<i64> {
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    Ok(rows.rows()[0].get::<i64>("c")?)
}

fn assert_locked(result: Result<Uni, UniError>) -> String {
    match result {
        Ok(_) => panic!("a second writable handle opened"),
        Err(UniError::StoreLocked { holder, .. }) => holder,
        Err(other) => panic!("expected StoreLocked, got {other}"),
    }
}

/// The issue #348 reproduction: a second writable handle no longer opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_writable_handle_is_refused() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let a = seeded(&path).await?;
    let started = Instant::now();
    let holder = assert_locked(Uni::open(&path).config(quick_grace()).build().await);
    assert!(holder.contains(&std::process::id().to_string()), "{holder}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "waited {:?}",
        started.elapsed()
    );
    // The first handle is unaffected.
    let tx = a.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;
    assert_eq!(count(&a).await?, 2);
    Ok(())
}

/// A brand-new store used to fail its second open with a misleading "WAL
/// segments but no snapshot manifest"; now it is refused as held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_handle_on_a_fresh_store_is_refused_as_held() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let a = Uni::open(&path).config(quick_grace()).build().await?;
    let tx = a.session().tx().await?;
    tx.execute("CREATE (:Thing {n: 1})").await?;
    tx.commit().await?;
    assert_locked(Uni::open(&path).config(quick_grace()).build().await);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_handles_coexist_with_the_writer_and_write_nothing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let a = seeded(&path).await?;
    let r1 = Uni::open(&path).read_only().build().await?;
    let r2 = Uni::open(&path).read_only().build().await?;
    assert_eq!(count(&r1).await?, 1);
    assert_eq!(count(&r2).await?, 1);
    let tx = a.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;
    drop((r1, r2));
    // And a reader opened before a writer does not block it.
    a.shutdown().await?;
    let r = Uni::open(&path).read_only().build().await?;
    let w = Uni::open(&path).config(quick_grace()).build().await?;
    assert_eq!(count(&w).await?, 2);
    drop(r);
    Ok(())
}

#[tokio::test]
async fn dropping_a_handle_frees_the_store_at_once() -> Result<()> {
    // `current_thread`: the drop cannot wait for background tasks.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let a = seeded(&path).await?;
    drop(a);
    let b = Uni::open(&path).config(quick_grace()).build().await?;
    assert_eq!(count(&b).await?, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_handles_lingering_session_cannot_write() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let a = seeded(&path).await?;
    let lingering = a.session();
    a.shutdown().await?;
    // The store is free for a new writer...
    let b = Uni::open(&path).config(quick_grace()).build().await?;
    let schema_before = std::fs::read(dir.path().join("db/catalog/schema.json"))?;
    // ...and the old session cannot write behind its back: not data,
    let result = async {
        let tx = lingering.tx().await?;
        tx.execute("CREATE (:Person {name: 'Ghost'})").await?;
        tx.commit().await
    }
    .await;
    match result {
        Err(UniError::WriterFenced { .. }) => {}
        other => panic!("expected WriterFenced, got {other:?}"),
    }
    // ...not the schema,
    let ddl = lingering.run("CREATE LABEL GhostLabel (n INT)").await;
    assert!(ddl.is_err(), "a closed handle changed the schema");
    // ...and not the fork registry.
    assert!(
        lingering.fork("ghost-fork").await.is_err(),
        "a closed handle created a fork"
    );
    assert_eq!(
        std::fs::read(dir.path().join("db/catalog/schema.json"))?,
        schema_before,
        "the closed handle's DDL reached the catalog"
    );
    assert_eq!(count(&b).await?, 1);
    assert!(b.list_forks().await.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_prefixes_of_one_root_are_held_separately() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let a = Uni::open(format!("file://{}/root/a", dir.path().display()))
        .config(quick_grace())
        .build()
        .await?;
    let b = Uni::open(format!("file://{}/root/b", dir.path().display()))
        .config(quick_grace())
        .build()
        .await?;
    assert_locked(
        Uni::open(format!("file://{}/root/a", dir.path().display()))
            .config(quick_grace())
            .build()
            .await,
    );
    // A plain path and a file:// URI naming one directory are one store.
    let plain = dir.path().join("root/b").display().to_string();
    assert_locked(Uni::open(&plain).config(quick_grace()).build().await);
    drop((a, b));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unenforced_writes_take_no_lock() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let _a = Uni::open(&path)
        .write_lease(WriteLease::Unenforced)
        .build()
        .await?;
    let _b = Uni::open(&path)
        .write_lease(WriteLease::Unenforced)
        .build()
        .await?;
    Ok(())
}

#[tokio::test]
async fn a_dynamodb_lease_is_refused_as_not_implemented() {
    let dir = tempfile::tempdir().unwrap();
    let err = match Uni::open(dir.path().join("db").to_str().unwrap())
        .write_lease(WriteLease::DynamoDB {
            table: "locks".into(),
        })
        .build()
        .await
    {
        Ok(_) => panic!("opened with an unimplemented lease"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("not implemented"), "{err}");
}

/// A custom provider that records calls and can refuse.
#[derive(Default)]
struct Recording {
    calls: std::sync::Mutex<Vec<&'static str>>,
    refuse_acquire: bool,
    fail_heartbeat: std::sync::atomic::AtomicBool,
}

/// The provider handed to the builder; the test keeps the `Arc` to inspect.
struct Handle(std::sync::Arc<Recording>);

#[async_trait::async_trait]
impl WriteLeaseProvider for Handle {
    async fn acquire(&self) -> uni_db::Result<LeaseGuard> {
        self.0.calls.lock().unwrap().push("acquire");
        if self.0.refuse_acquire {
            return Err(UniError::Internal(anyhow::anyhow!("held by another agent")));
        }
        Ok(LeaseGuard {
            lease_id: "l1".into(),
            expires_at: chrono::Utc::now() + chrono::Duration::seconds(30),
        })
    }
    async fn heartbeat(&self, _guard: &LeaseGuard) -> uni_db::Result<()> {
        self.0.calls.lock().unwrap().push("heartbeat");
        if self
            .0
            .fail_heartbeat
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(UniError::Internal(anyhow::anyhow!("lease lost")));
        }
        Ok(())
    }
    async fn release(&self, _guard: LeaseGuard) -> uni_db::Result<()> {
        self.0.calls.lock().unwrap().push("release");
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_custom_provider_is_acquired_renewed_and_released() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let provider = std::sync::Arc::new(Recording::default());
    let mut config = UniConfig::default();
    config.writer_lease.renew_interval = Duration::from_millis(100);
    let db = Uni::open(&path)
        .config(config)
        .write_lease(WriteLease::Custom(Box::new(Handle(std::sync::Arc::clone(
            &provider,
        )))))
        .build()
        .await?;
    tokio::time::sleep(Duration::from_millis(350)).await;
    db.shutdown().await?;
    let calls = provider.calls.lock().unwrap().clone();
    assert_eq!(calls.first(), Some(&"acquire"));
    assert!(calls.contains(&"heartbeat"), "{calls:?}");
    assert_eq!(calls.last(), Some(&"release"));
    Ok(())
}

#[tokio::test]
async fn a_custom_provider_that_refuses_fails_the_open() {
    let dir = tempfile::tempdir().unwrap();
    let provider = std::sync::Arc::new(Recording {
        refuse_acquire: true,
        ..Default::default()
    });
    let result = Uni::open(dir.path().join("db").to_str().unwrap())
        .write_lease(WriteLease::Custom(Box::new(Handle(provider))))
        .build()
        .await;
    assert!(result.is_err(), "opened without the lease");
    assert!(
        !dir.path().join("db/catalog/schema.json").exists(),
        "wrote before the lease"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_custom_lease_stops_writes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let provider = std::sync::Arc::new(Recording::default());
    let mut config = UniConfig::default();
    config.writer_lease.renew_interval = Duration::from_millis(50);
    let db = Uni::open(&path)
        .config(config)
        .write_lease(WriteLease::Custom(Box::new(Handle(std::sync::Arc::clone(
            &provider,
        )))))
        .build()
        .await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    provider
        .fail_heartbeat
        .store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let result = async {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Person {name: 'Late'})").await?;
        tx.commit().await
    }
    .await;
    match result {
        Err(UniError::WriterFenced { .. }) => {}
        other => panic!("expected WriterFenced, got {other:?}"),
    }
    Ok(())
}

/// Paths, sizes and modification times of every file under `root`.
fn tree(
    root: &std::path::Path,
) -> std::collections::BTreeMap<String, (u64, std::time::SystemTime)> {
    fn walk(
        root: &std::path::Path,
        dir: &std::path::Path,
        out: &mut std::collections::BTreeMap<String, (u64, std::time::SystemTime)>,
    ) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else if let Ok(meta) = entry.metadata() {
                let rel = path.strip_prefix(root).unwrap().display().to_string();
                out.insert(rel, (meta.len(), meta.modified().unwrap()));
            }
        }
    }
    let mut out = Default::default();
    walk(root, root, &mut out);
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_open_changes_no_file() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let a = seeded(&path).await?;
    // Committed but not flushed: in the WAL, which the reader replays.
    let tx = a.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;
    a.shutdown().await?;
    std::thread::sleep(Duration::from_millis(20));
    let before = tree(&dir.path().join("db"));
    let r = Uni::open(&path).read_only().build().await?;
    assert_eq!(count(&r).await?, 2);
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(r);
    assert_eq!(
        tree(&dir.path().join("db")),
        before,
        "a read-only open wrote"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_snapshots_on_one_handle_do_not_fence_it() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db").display().to_string();
    let db = std::sync::Arc::new(seeded(&path).await?);
    let tasks: Vec<_> = (0..6)
        .map(|i| {
            let db = std::sync::Arc::clone(&db);
            tokio::spawn(async move { db.create_snapshot(&format!("snap{i}")).await })
        })
        .collect();
    for t in tasks {
        t.await??;
    }
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'After'})").await?;
    tx.commit().await?;
    assert_eq!(count(&db).await?, 2);
    Ok(())
}

#[tokio::test]
async fn a_failed_open_gives_the_store_back() {
    let dir = tempfile::tempdir().unwrap();
    // A catalog file that cannot be read fails the open after the store
    // was taken.
    std::fs::create_dir_all(dir.path().join("db/catalog")).unwrap();
    std::fs::write(dir.path().join("db/catalog/locy_rules.json"), b"not json").unwrap();
    let provider = std::sync::Arc::new(Recording::default());
    let result = Uni::open(dir.path().join("db").to_str().unwrap())
        .write_lease(WriteLease::Custom(Box::new(Handle(std::sync::Arc::clone(
            &provider,
        )))))
        .build()
        .await;
    assert!(result.is_err(), "opened with an unreadable Locy catalog");
    let calls = provider.calls.lock().unwrap().clone();
    assert_eq!(calls.first(), Some(&"acquire"), "{calls:?}");
    assert_eq!(
        calls.last(),
        Some(&"release"),
        "the failed open kept the lease: {calls:?}"
    );
    std::fs::remove_file(dir.path().join("db/catalog/locy_rules.json")).unwrap();
    // And the local lock is free again.
    let db = Uni::open(dir.path().join("db").to_str().unwrap())
        .config(quick_grace())
        .build()
        .await;
    assert!(db.is_ok(), "{:?}", db.err());
}

#[tokio::test]
async fn a_renew_interval_not_below_the_ttl_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = UniConfig::default();
    config.writer_lease.renew_interval = Duration::from_secs(30);
    config.writer_lease.ttl = Duration::from_secs(30);
    let result = Uni::open(dir.path().join("db").to_str().unwrap())
        .config(config)
        .build()
        .await;
    let err = match result {
        Ok(_) => panic!("opened with renew_interval == ttl"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("renew_interval"), "{err}");
}

/// A hybrid store's local directory is held too: the same directory opened
/// as a plain local store is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hybrid_stores_local_directory_is_held() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let local = dir.path().join("local");
    let _h = Uni::open(local.to_str().unwrap())
        .hybrid(&format!("file://{}/remote", dir.path().display()))
        .config(quick_grace())
        .build()
        .await?;
    assert_locked(
        Uni::open(local.to_str().unwrap())
            .config(quick_grace())
            .build()
            .await,
    );
    Ok(())
}
