// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A hybrid store's local WAL belongs to one lineage of the store.
//!
//! A hybrid host keeps its commits in a local WAL until they are flushed to
//! the object store. If it crashes and another host opens the store with its
//! own directory and writes, the first host's WAL no longer describes the
//! store: replaying it would lay commits made against an older state on top
//! of newer data, with ids the newer writer may have reused. Such a WAL is
//! refused (`UniError::StaleWal`), set aside on request, and ignored by a
//! read-only open. The remote side is a `file://` store, so these run without
//! an object-store server.
//!
//! The "crash" here is a drop without `shutdown()`: it leaves the same WAL a
//! crash does (nothing is flushed), but releases the lock at once rather than
//! after a lease lapses, so the second host need not wait.

use std::path::{Path, PathBuf};

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, UniError};

/// A bucket directory holding the remote side, and one local directory per
/// host.
struct Hosts {
    _dir: tempfile::TempDir,
    bucket: PathBuf,
    root: PathBuf,
}

impl Hosts {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let root = dir.path().to_path_buf();
        Ok(Self {
            bucket: root.join("bucket"),
            root,
            _dir: dir,
        })
    }

    fn local(&self, host: &str) -> PathBuf {
        self.root.join(host)
    }

    fn builder(&self, host: &str) -> uni_db::UniBuilder {
        Uni::open(self.local(host).to_str().unwrap())
            .hybrid(&format!("file://{}", self.bucket.join("g").display()))
            .storage_option(
                uni_db::store::location::FILE_BUCKET_ROOT_OPTION,
                self.bucket.display().to_string(),
            )
            .config(no_auto_flush())
    }

    async fn open(&self, host: &str) -> uni_db::Result<Uni> {
        self.builder(host).build().await
    }

    fn claim(&self) -> PathBuf {
        self.bucket.join("g/catalog/wal_claim.json")
    }

    fn uri(&self) -> String {
        format!("file://{}", self.bucket.join("g").display())
    }
}

/// Commits stay in the WAL until a flush is asked for.
fn no_auto_flush() -> UniConfig {
    let mut config = UniConfig::default();
    config.auto_flush_interval = None;
    config.auto_flush_threshold = usize::MAX;
    config
}

async fn create(db: &Uni, name: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(&format!("CREATE (:Person {{name: '{name}'}})"))
        .await?;
    tx.commit().await?;
    Ok(())
}

async fn people(db: &Uni) -> Result<Vec<String>> {
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS n ORDER BY n")
        .await?;
    rows.rows()
        .iter()
        .map(|r| Ok(r.get::<String>("n")?))
        .collect()
}

async fn declare(db: &Uni) -> Result<()> {
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    Ok(())
}

/// Files directly or indirectly under `dir`.
fn file_count(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() { file_count(&p) } else { 1 }
        })
        .sum()
}

/// Host `one` writes `Alice` (flushed) and `Lost` (in its WAL only), then
/// is dropped without `shutdown()`. With `flush_first` unset, before any
/// flush.
async fn crash_with_unflushed(hosts: &Hosts, flush_first: bool) -> Result<()> {
    let db = hosts.open("one").await?;
    declare(&db).await?;
    if flush_first {
        create(&db, "Alice").await?;
        db.flush().await?;
    }
    create(&db, "Lost").await?;
    // No shutdown, so nothing is flushed.
    drop(db);
    assert!(
        file_count(&hosts.local("one").join("wal")) > 0,
        "the crash left no WAL to recover"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_returning_after_another_wrote_is_refused() -> Result<()> {
    for flush_first in [true, false] {
        let hosts = Hosts::new()?;
        crash_with_unflushed(&hosts, flush_first).await?;

        let db = hosts.open("two").await?;
        declare(&db).await?;
        let before: Vec<String> = if flush_first {
            vec!["Alice".into()]
        } else {
            vec![]
        };
        assert_eq!(people(&db).await?, before, "flush_first={flush_first}");
        create(&db, "Bob").await?;
        db.shutdown().await?;

        match hosts.open("one").await {
            Err(UniError::StaleWal {
                segments,
                wal,
                store,
            }) => {
                // The one commit that never reached the object store.
                assert_eq!(segments, 1, "flush_first={flush_first}");
                assert_eq!(store, hosts.uri());
                assert!(
                    wal.contains(hosts.local("one").to_str().unwrap()),
                    "the error names {wal}, not host one's directory"
                );
            }
            Err(e) => anyhow::bail!("expected StaleWal, got {e}"),
            Ok(_) => anyhow::bail!("opened over another writer's newer data"),
        }
        // The refused open changed nothing: host two still sees its data.
        let db = hosts.open("two").await?;
        let mut after = before.clone();
        after.push("Bob".into());
        assert_eq!(people(&db).await?, after, "flush_first={flush_first}");
        db.shutdown().await?;
    }
    Ok(())
}

/// The other host need not have flushed, and the store need never have been
/// flushed at all: an open alone claims it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_returning_after_another_opened_is_refused() -> Result<()> {
    for flush_first in [true, false] {
        let hosts = Hosts::new()?;
        crash_with_unflushed(&hosts, flush_first).await?;

        let db = hosts.open("two").await?;
        declare(&db).await?;
        create(&db, "Bob").await?;
        drop(db);

        assert!(
            matches!(
                hosts.open("one").await,
                Err(UniError::StaleWal { segments: 1, .. })
            ),
            "flush_first={flush_first}"
        );
        // Host two's own unflushed commit is still its to replay.
        let db = hosts.open("two").await?;
        let expected: Vec<&str> = if flush_first {
            vec!["Alice", "Bob"]
        } else {
            vec!["Bob"]
        };
        assert_eq!(people(&db).await?, expected, "flush_first={flush_first}");
        db.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_wal_set_aside_opens_and_keeps_its_segments() -> Result<()> {
    let hosts = Hosts::new()?;
    crash_with_unflushed(&hosts, true).await?;
    let db = hosts.open("two").await?;
    create(&db, "Bob").await?;
    db.shutdown().await?;

    let wal_files = file_count(&hosts.local("one").join("wal"));
    let db = hosts.builder("one").set_aside_stale_wal().build().await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob"]);
    assert_eq!(
        file_count(&hosts.local("one").join("wal_set_aside")),
        wal_files,
        "the stale segments were not kept"
    );
    create(&db, "Carol").await?;
    db.shutdown().await?;

    // Host one now owns the store and reopens without the option. Host two
    // shut down cleanly, so its directory holds no WAL and opens too.
    let db = hosts.open("one").await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob", "Carol"]);
    db.shutdown().await?;
    let db = hosts.open("two").await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob", "Carol"]);
    db.shutdown().await?;
    Ok(())
}

/// The option changes nothing when the WAL is the store's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_aside_leaves_the_stores_own_wal_alone() -> Result<()> {
    let hosts = Hosts::new()?;
    crash_with_unflushed(&hosts, true).await?;
    let db = hosts.builder("one").set_aside_stale_wal().build().await?;
    assert_eq!(people(&db).await?, ["Alice", "Lost"]);
    assert_eq!(file_count(&hosts.local("one").join("wal_set_aside")), 0);
    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_returning_with_no_other_writer_replays_its_wal() -> Result<()> {
    for flush_first in [true, false] {
        let hosts = Hosts::new()?;
        crash_with_unflushed(&hosts, flush_first).await?;
        let db = hosts.open("one").await?;
        let expected: Vec<&str> = if flush_first {
            vec!["Alice", "Lost"]
        } else {
            vec!["Lost"]
        };
        assert_eq!(people(&db).await?, expected, "flush_first={flush_first}");
        db.shutdown().await?;
    }
    Ok(())
}

/// A clean shutdown flushes, so every commit reaches the object store and
/// the next host, on any directory, sees it; the first host's directory is
/// still usable afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clean_shutdown_hands_every_commit_over() -> Result<()> {
    let hosts = Hosts::new()?;
    let db = hosts.open("one").await?;
    declare(&db).await?;
    create(&db, "Alice").await?;
    db.shutdown().await?;

    let db = hosts.open("two").await?;
    assert_eq!(people(&db).await?, ["Alice"]);
    create(&db, "Bob").await?;
    db.shutdown().await?;

    let db = hosts.open("one").await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob"]);
    db.shutdown().await?;
    // The seal its clean close left was moved aside, holding no commits.
    assert!(
        file_count(&hosts.local("one").join("wal_set_aside")) > 0,
        "the earlier WAL's seal was left in place"
    );
    assert_eq!(file_count(&hosts.local("one").join("wal")), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_open_ignores_a_stale_wal() -> Result<()> {
    let hosts = Hosts::new()?;
    crash_with_unflushed(&hosts, true).await?;
    let db = hosts.open("two").await?;
    create(&db, "Bob").await?;
    db.shutdown().await?;

    let wal_files = file_count(&hosts.local("one").join("wal"));
    let db = hosts.builder("one").read_only().build().await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob"]);
    db.shutdown().await?;
    assert_eq!(
        file_count(&hosts.local("one").join("wal")),
        wal_files,
        "a read-only open touched the WAL"
    );
    Ok(())
}

/// A store opened before lineages were recorded has no claim: the local WAL
/// is replayed as before, and the store is then claimed for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_without_a_claim_adopts_the_local_wal() -> Result<()> {
    let hosts = Hosts::new()?;
    crash_with_unflushed(&hosts, true).await?;
    std::fs::remove_file(hosts.claim())?;
    let db = hosts.open("one").await?;
    assert_eq!(people(&db).await?, ["Alice", "Lost"]);
    db.shutdown().await?;

    let claim: serde_json::Value = serde_json::from_slice(&std::fs::read(hosts.claim())?)?;
    let local_id: serde_json::Value =
        serde_json::from_slice(&std::fs::read(hosts.local("one").join("wal_id.json"))?)?;
    assert_eq!(claim["wal_id"], local_id["wal_id"]);
    Ok(())
}

/// A remote-only store keeps its WAL in the object store, shared by every
/// writer: the next open replays it, whoever opens it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_only_store_replays_its_wal_for_the_next_writer() -> Result<()> {
    let hosts = Hosts::new()?;
    let open = || {
        Uni::open(hosts.uri())
            .storage_option(
                uni_db::store::location::FILE_BUCKET_ROOT_OPTION,
                hosts.bucket.display().to_string(),
            )
            .config(no_auto_flush())
            .build()
    };
    let db = open().await?;
    declare(&db).await?;
    create(&db, "Alice").await?;
    db.flush().await?;
    create(&db, "Lost").await?;
    drop(db);
    let db = open().await?;
    assert_eq!(people(&db).await?, ["Alice", "Lost"]);
    db.shutdown().await?;
    assert!(!hosts.claim().exists(), "a remote-only store was claimed");
    Ok(())
}

/// Plain local stores keep their WAL beside their data; nothing changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_store_replays_its_wal_as_before() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let path = path.to_str().unwrap();
    let db = Uni::open(path).config(no_auto_flush()).build().await?;
    declare(&db).await?;
    create(&db, "Alice").await?;
    db.flush().await?;
    create(&db, "Lost").await?;
    drop(db);
    let db = Uni::open(path).config(no_auto_flush()).build().await?;
    assert_eq!(people(&db).await?, ["Alice", "Lost"]);
    db.shutdown().await?;
    Ok(())
}

impl Hosts {
    /// The store opened remote-only (its own shared WAL), as a CLI or a
    /// maintenance job might.
    async fn open_remote_only(&self) -> uni_db::Result<Uni> {
        Uni::open(&self.uri())
            .storage_option(
                uni_db::store::location::FILE_BUCKET_ROOT_OPTION,
                self.bucket.display().to_string(),
            )
            .config(no_auto_flush())
            .build()
            .await
    }
}

/// A remote-only writer moves the store on too: a hybrid host that crashed
/// with unflushed commits is refused when it returns.
#[tokio::test]
async fn a_host_returning_after_a_remote_only_writer_is_refused() -> Result<()> {
    let hosts = Hosts::new()?;
    crash_with_unflushed(&hosts, true).await?;
    let db = hosts.open_remote_only().await?;
    create(&db, "Bob").await?;
    db.shutdown().await?;
    match hosts.open("one").await {
        Err(UniError::StaleWal { segments: 1, .. }) => {}
        Err(e) => anyhow::bail!("expected StaleWal, got {e}"),
        Ok(_) => anyhow::bail!("replayed over a remote-only writer's newer data"),
    }
    Ok(())
}

/// A copy of a host's directory (a VM image, an rsync to a standby) carries
/// its WAL's id but not its home: once the copy has claimed the store, the
/// original's return is refused.
#[tokio::test]
async fn a_copied_directory_supersedes_the_original() -> Result<()> {
    let hosts = Hosts::new()?;
    crash_with_unflushed(&hosts, true).await?;
    // The standby: a copy of host one's directory, opened as the store.
    copy_dir(&hosts.local("one"), &hosts.local("standby"))?;
    let db = hosts
        .builder("standby")
        .set_aside_stale_wal()
        .build()
        .await?;
    create(&db, "Bob").await?;
    db.shutdown().await?;
    match hosts.open("one").await {
        Err(UniError::StaleWal { .. }) => {}
        Err(e) => anyhow::bail!("expected StaleWal, got {e}"),
        Ok(_) => anyhow::bail!("the original replayed over its copy's newer data"),
    }
    Ok(())
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else if entry.file_name() != "writer.lock" {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// A backup of a hybrid store follows the WAL it copied: a hybrid host that
/// opens the copy with another WAL holding commits is refused.
#[tokio::test]
async fn a_backup_of_a_hybrid_store_carries_its_claim() -> Result<()> {
    let hosts = Hosts::new()?;
    let db = hosts.open("one").await?;
    declare(&db).await?;
    create(&db, "Alice").await?;
    db.flush().await?;
    let dest = hosts.root.join("backup");
    db.session()
        .run(&format!("BACKUP TO '{}'", dest.display()))
        .await?;
    db.shutdown().await?;
    let claim: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dest.join("catalog/wal_claim.json"))?)?;
    let wal_id: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dest.join("wal_id.json"))?)?;
    assert_eq!(
        claim["wal_id"], wal_id["wal_id"],
        "the copy follows its WAL"
    );
    Ok(())
}
