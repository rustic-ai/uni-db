// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! One writer per store.
//!
//! A writable handle holds either an OS lock on `R/writer.lock` (a store in a
//! local directory) or a lease object at `R/catalog/writer_lease.json` (an
//! object store). Read-only handles hold neither.
//!
//! The lease is created with `PutMode::Create`, renewed with
//! `PutMode::Update`, and taken over once it is released or expired. A holder
//! that finds its lease taken over, or cannot renew it before it would expire,
//! trips its [`WriteFence`]: from then on the handle refuses to write. The
//! fence also trips when the handle closes, so lingering sessions of a closed
//! handle cannot write either.
//!
//! What a lease cannot stop: a holder stalled past its expiry that resumes
//! and appends to a Lance table before its next fence check. Pinning table
//! versions in the snapshot manifest (Phase D of the object-store proposal)
//! closes that.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};

/// Lease object path, relative to the store root.
pub const LEASE_PATH: &str = "catalog/writer_lease.json";

/// Lock file name, in a local store's root directory.
pub const LOCK_FILE: &str = "writer.lock";

/// Lease timing.
#[derive(Debug, Clone, Copy)]
pub struct LeaseSettings {
    /// How long a lease lasts without renewal.
    pub ttl: Duration,
    /// How often the holder renews it.
    pub renew_interval: Duration,
    /// How long a writable open waits for a held store before failing.
    pub acquire_grace: Duration,
    /// Allowance for clocks disagreeing between holders.
    pub clock_skew: Duration,
}

impl Default for LeaseSettings {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
            acquire_grace: Duration::from_secs(5),
            clock_skew: Duration::from_secs(5),
        }
    }
}

/// The store is held by another writer.
#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "The store at {store} is open for writing elsewhere ({holder}). Close that handle, open \
     this one read-only, or wait for its lease to expire."
)]
pub struct StoreLocked {
    /// The store.
    pub store: String,
    /// Who holds it, as far as is known.
    pub holder: String,
}

/// The store cannot enforce a single writer.
#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "The object store holding {store} does not support the conditional writes a writer lease \
     needs ({detail}). Use WriteLease::Unenforced to open it anyway with one writer by convention."
)]
pub struct LeaseUnsupported {
    /// The store.
    pub store: String,
    /// What failed.
    pub detail: String,
}

/// Whether a handle may still write. Shared by everything that writes.
#[derive(Debug)]
pub struct WriteFence {
    tripped: AtomicBool,
    reason: parking_lot::Mutex<Option<String>>,
    /// Wall-clock millisecond after which the handle's lease may have lapsed:
    /// writes stop then even if no renewal has said so (a stalled process).
    deadline_ms: std::sync::atomic::AtomicI64,
}

impl Default for WriteFence {
    fn default() -> Self {
        Self {
            tripped: AtomicBool::new(false),
            reason: parking_lot::Mutex::new(None),
            deadline_ms: std::sync::atomic::AtomicI64::new(i64::MAX),
        }
    }
}

impl WriteFence {
    /// A fence that has not tripped.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Fails with [`UniError::WriterFenced`] once the fence has tripped.
    ///
    /// # Errors
    /// [`UniError::WriterFenced`], wrapped in `anyhow` so a caller can
    /// downcast it.
    ///
    /// [`UniError::WriterFenced`]: uni_common::UniError::WriterFenced
    pub fn check(&self) -> Result<()> {
        if now_ms() >= self.deadline_ms.load(Ordering::Acquire) {
            self.trip("its writer lease lapsed before it was renewed");
        }
        match self.reason_if_tripped() {
            Some(reason) => Err(uni_common::UniError::WriterFenced { reason }.into()),
            None => Ok(()),
        }
    }

    /// Why the fence tripped, if it has.
    #[must_use]
    pub fn reason_if_tripped(&self) -> Option<String> {
        self.tripped.load(Ordering::Acquire).then(|| {
            self.reason
                .lock()
                .clone()
                .unwrap_or_else(|| "its writer lease was lost".to_string())
        })
    }

    /// Trips the fence; the first reason is kept.
    pub fn trip(&self, reason: impl Into<String>) {
        let mut held = self.reason.lock();
        if held.is_none() {
            *held = Some(reason.into());
        }
        self.tripped.store(true, Ordering::Release);
    }

    /// Whether the fence has tripped.
    #[must_use]
    pub fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::Acquire)
    }

    /// Writes stop at `deadline_ms` (wall clock) unless it is moved on.
    pub fn set_deadline_ms(&self, deadline_ms: i64) {
        self.deadline_ms.store(deadline_ms, Ordering::Release);
    }

    /// The current deadline.
    #[must_use]
    pub fn deadline_ms(&self) -> i64 {
        self.deadline_ms.load(Ordering::Acquire)
    }
}

impl uni_common::core::write_gate::WriteGate for WriteFence {
    fn check(&self) -> anyhow::Result<()> {
        WriteFence::check(self)
    }
    fn trip(&self, reason: String) {
        WriteFence::trip(self, reason);
    }
}

/// An OS lock on a local store's directory, held until dropped.
#[derive(Debug)]
pub struct LocalDirLock {
    _file: std::fs::File,
    path: PathBuf,
}

impl LocalDirLock {
    /// The lock file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Takes the writer lock on the local store at `dir`, waiting up to
/// `grace` for a holder that is closing.
///
/// # Errors
/// [`StoreLocked`] when another handle keeps it; I/O errors.
pub async fn acquire_local(dir: &Path, grace: Duration) -> Result<LocalDirLock> {
    use std::io::{Read, Seek, Write};
    std::fs::create_dir_all(dir)?;
    let path = dir.join(LOCK_FILE);
    let deadline = std::time::Instant::now() + grace;
    loop {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| anyhow::anyhow!("open writer lock {}: {e}", path.display()))?;
        match file.try_lock() {
            Ok(()) => {
                // Who holds it, for the next opener's error message.
                file.set_len(0)?;
                file.rewind()?;
                write!(file, "{}", holder_description())?;
                file.sync_data()?;
                return Ok(LocalDirLock { _file: file, path });
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                if std::time::Instant::now() >= deadline {
                    let mut holder = String::new();
                    let _ = file.read_to_string(&mut holder);
                    let holder = if holder.trim().is_empty() {
                        "another handle".to_string()
                    } else {
                        holder.trim().to_string()
                    };
                    return Err(StoreLocked {
                        store: dir.display().to_string(),
                        holder,
                    }
                    .into());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(anyhow::anyhow!("lock {}: {e}", path.display()));
            }
        }
    }
}

/// This process, as a holder description.
fn holder_description() -> String {
    format!("pid {} on {}", std::process::id(), host_name())
}

fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|h| h.trim().to_string())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or_else(|| "unknown host".to_string())
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// The lease record.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeaseRecord {
    /// Random id of the holding handle.
    pub owner: String,
    /// Host name of the holder.
    pub host: String,
    /// Process id of the holder.
    pub pid: u32,
    /// Increases with every new holder.
    pub epoch: u64,
    /// When the lease lapses without renewal, in the holder's clock.
    pub expires_at_ms: i64,
    /// Set when the holder released it.
    #[serde(default)]
    pub released: bool,
}

/// A held lease on an object store.
#[derive(Debug)]
pub struct RemoteLease {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    settings: LeaseSettings,
    fence: Arc<WriteFence>,
    state: tokio::sync::Mutex<(LeaseRecord, object_store::UpdateVersion)>,
    renewal: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    released: AtomicBool,
}

impl RemoteLease {
    /// The epoch this holder took the lease at.
    pub async fn epoch(&self) -> u64 {
        self.state.lock().await.0.epoch
    }

    /// Starts renewing in the background until [`release`](Self::release).
    /// A renewal that finds the lease taken over, or none succeeding before
    /// the lease would lapse, trips the fence.
    pub fn start_renewal(self: &Arc<Self>) {
        let lease = Arc::downgrade(self);
        let interval = self.settings.renew_interval;
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(lease) = lease.upgrade() else { return };
                if lease.released.load(Ordering::Acquire) || lease.fence.is_tripped() {
                    return;
                }
                // A renewal stuck in retries must not outlive the lease.
                let left = lease.fence.deadline_ms().saturating_sub(now_ms()).max(1);
                let renewed = match tokio::time::timeout(
                    Duration::from_millis(u64::try_from(left).unwrap_or(1)),
                    lease.renew(),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("renewal timed out")),
                };
                if let Err(e) = renewed {
                    tracing::warn!(error = %e, "writer lease renewal failed");
                    let expires = lease.state.lock().await.0.expires_at_ms;
                    // Stop before the lease lapses rather than after: once
                    // it lapses another writer may take it.
                    let margin = i64::try_from(lease.settings.renew_interval.as_millis())
                        .unwrap_or(i64::MAX);
                    if now_ms().saturating_add(margin) >= expires {
                        lease.fence.trip(format!(
                            "its writer lease could not be renewed before it expired ({e})"
                        ));
                        return;
                    }
                }
                if lease.fence.is_tripped() {
                    return;
                }
            }
        });
        *self.renewal.lock() = Some(task);
    }

    /// Renews once. Trips the fence when the lease was taken over.
    ///
    /// # Errors
    /// The renewal failed.
    pub async fn renew(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        for _ in 0..2 {
            let mut next = state.0.clone();
            next.expires_at_ms = now_ms() + ttl_ms(&self.settings);
            let opts: PutOptions = PutMode::Update(state.1.clone()).into();
            match self
                .store
                .put_opts(&self.path, serde_json::to_vec(&next)?.into(), opts)
                .await
            {
                Ok(result) => {
                    self.fence
                        .set_deadline_ms(self.deadline_for(next.expires_at_ms));
                    *state = (
                        next,
                        UpdateVersion {
                            e_tag: result.e_tag,
                            version: result.version,
                        },
                    );
                    return Ok(());
                }
                Err(object_store::Error::Precondition { .. }) => {
                    let current = read_lease(&self.store, &self.path).await?;
                    match current {
                        // Our own earlier renewal landed though it was
                        // reported failed: adopt it and renew from there. A
                        // renewal only moves the expiry forward, so a record
                        // under our name that expires earlier was written by
                        // someone else.
                        Some((record, version))
                            if record.owner == state.0.owner
                                && record.epoch == state.0.epoch
                                && !record.released
                                && record.expires_at_ms >= state.0.expires_at_ms =>
                        {
                            self.fence
                                .set_deadline_ms(self.deadline_for(record.expires_at_ms));
                            *state = (record, version);
                            continue;
                        }
                        other => {
                            let holder = match other {
                                Some((record, _)) => format!(
                                    "{} (pid {} on {}, epoch {})",
                                    record.owner, record.pid, record.host, record.epoch
                                ),
                                None => "another writer".to_string(),
                            };
                            self.fence
                                .trip(format!("its writer lease was taken over by {holder}"));
                            anyhow::bail!("writer lease taken over by {holder}")
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("writer lease kept changing under its own holder")
    }

    /// When this holder must stop writing for a lease expiring at
    /// `expires_at_ms`: early enough that no other host, whose clock may run
    /// `clock_skew` ahead, can have taken it over yet.
    fn deadline_for(&self, expires_at_ms: i64) -> i64 {
        let skew = i64::try_from(self.settings.clock_skew.as_millis()).unwrap_or(0);
        expires_at_ms.saturating_sub(skew)
    }

    /// Gives the lease up, so the next writer takes it at once. The handle
    /// can no longer write afterwards.
    pub async fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(task) = self.renewal.lock().take() {
            task.abort();
        }
        // The fence may be tripped already (the hold trips it before
        // releasing), so it says nothing about whether the lease is still
        // ours. The release is conditional on the version this holder last
        // wrote: if another writer has taken the lease since, the put fails
        // and their lease is left alone.
        self.fence.trip("the handle was closed");
        // A renewal in flight holds the state; it is aborted above, but on a
        // runtime that cannot run it to its end the lock may never come free.
        // The lease then lapses on its own.
        let Ok(state) = tokio::time::timeout(Duration::from_secs(2), self.state.lock()).await
        else {
            tracing::warn!("could not release the writer lease in time; it lapses on its own");
            return;
        };
        let mut next = state.0.clone();
        next.released = true;
        next.expires_at_ms = 0;
        let opts: PutOptions = PutMode::Update(state.1.clone()).into();
        let bytes = match serde_json::to_vec(&next) {
            Ok(b) => b,
            Err(_) => return,
        };
        match self.store.put_opts(&self.path, bytes.into(), opts).await {
            Ok(_) => {}
            // Taken over already: the lease is someone else's now.
            Err(object_store::Error::Precondition { .. }) => {}
            Err(e) => {
                tracing::warn!(error = %e, "could not release the writer lease; it lapses on its own")
            }
        }
    }
}

fn ttl_ms(settings: &LeaseSettings) -> i64 {
    i64::try_from(settings.ttl.as_millis()).unwrap_or(i64::MAX)
}

async fn read_lease(
    store: &Arc<dyn ObjectStore>,
    path: &ObjPath,
) -> Result<Option<(LeaseRecord, UpdateVersion)>> {
    match store.get(path).await {
        Ok(got) => {
            let version = UpdateVersion {
                e_tag: got.meta.e_tag.clone(),
                version: got.meta.version.clone(),
            };
            let bytes = got.bytes().await?;
            let record: LeaseRecord = serde_json::from_slice(&bytes)
                .map_err(|e| anyhow::anyhow!("unreadable writer lease {path}: {e}"))?;
            Ok(Some((record, version)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Takes the writer lease of the store whose root is `store`, waiting up to
/// `settings.acquire_grace` for a live holder to release it.
///
/// # Errors
/// [`StoreLocked`], [`LeaseUnsupported`], or the store's errors.
pub async fn acquire_remote(
    store: Arc<dyn ObjectStore>,
    store_name: &str,
    settings: LeaseSettings,
    fence: Arc<WriteFence>,
) -> Result<Arc<RemoteLease>> {
    let path = ObjPath::from(LEASE_PATH);
    let deadline = std::time::Instant::now() + settings.acquire_grace;
    let unsupported = |e: object_store::Error| -> anyhow::Error {
        LeaseUnsupported {
            store: store_name.to_string(),
            detail: e.to_string(),
        }
        .into()
    };
    let owner = uuid::Uuid::new_v4().to_string();
    loop {
        let mut record = LeaseRecord {
            owner: owner.clone(),
            host: host_name(),
            pid: std::process::id(),
            epoch: 1,
            expires_at_ms: now_ms() + ttl_ms(&settings),
            released: false,
        };
        let current = read_lease(&store, &path).await?;
        let mode = match &current {
            None => PutMode::Create,
            // Our own write landed though it was reported failed.
            Some((held, version)) if held.owner == owner && !held.released => {
                fence.set_deadline_ms(
                    held.expires_at_ms.saturating_sub(
                        i64::try_from(settings.clock_skew.as_millis()).unwrap_or(0),
                    ),
                );
                return Ok(Arc::new(RemoteLease {
                    store,
                    path,
                    settings,
                    fence,
                    state: tokio::sync::Mutex::new((held.clone(), version.clone())),
                    renewal: parking_lot::Mutex::new(None),
                    released: AtomicBool::new(false),
                }));
            }
            Some((held, version)) => {
                let skew = i64::try_from(settings.clock_skew.as_millis()).unwrap_or(i64::MAX);
                let lapsed = held.released || held.expires_at_ms.saturating_add(skew) < now_ms();
                if !lapsed {
                    if std::time::Instant::now() >= deadline {
                        return Err(StoreLocked {
                            store: store_name.to_string(),
                            holder: format!(
                                "pid {} on {}, lease epoch {}, expires {}",
                                held.pid,
                                held.host,
                                held.epoch,
                                chrono::DateTime::from_timestamp_millis(held.expires_at_ms)
                                    .map(|t| t.to_rfc3339())
                                    .unwrap_or_default()
                            ),
                        }
                        .into());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                record.epoch = held.epoch + 1;
                PutMode::Update(version.clone())
            }
        };
        let opts: PutOptions = mode.into();
        match store
            .put_opts(&path, serde_json::to_vec(&record)?.into(), opts)
            .await
        {
            Ok(result) => {
                fence.set_deadline_ms(
                    record.expires_at_ms.saturating_sub(
                        i64::try_from(settings.clock_skew.as_millis()).unwrap_or(0),
                    ),
                );
                return Ok(Arc::new(RemoteLease {
                    store,
                    path,
                    settings,
                    fence,
                    state: tokio::sync::Mutex::new((
                        record,
                        UpdateVersion {
                            e_tag: result.e_tag,
                            version: result.version,
                        },
                    )),
                    renewal: parking_lot::Mutex::new(None),
                    released: AtomicBool::new(false),
                }));
            }
            // Another opener got there first: look again.
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => continue,
            Err(e @ object_store::Error::NotImplemented { .. })
            | Err(e @ object_store::Error::NotSupported { .. }) => return Err(unsupported(e)),
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn quick() -> LeaseSettings {
        LeaseSettings {
            ttl: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
            acquire_grace: Duration::from_millis(200),
            clock_skew: Duration::from_millis(0),
        }
    }

    async fn record(store: &Arc<dyn ObjectStore>) -> LeaseRecord {
        let bytes = store
            .get(&ObjPath::from(LEASE_PATH))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn is_locked(e: &anyhow::Error) -> bool {
        e.downcast_ref::<StoreLocked>().is_some()
    }

    // ---- fence ------------------------------------------------------------

    #[test]
    fn a_tripped_fence_refuses_with_its_first_reason() {
        let fence = WriteFence::new();
        assert!(fence.check().is_ok());
        fence.trip("lease taken over");
        fence.trip("closed");
        assert!(fence.check().is_err());
        assert!(fence.is_tripped());
        assert_eq!(
            fence.reason_if_tripped().as_deref(),
            Some("lease taken over")
        );
    }

    // ---- local lock -------------------------------------------------------

    #[tokio::test]
    async fn a_local_lock_excludes_a_second_holder_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let first = acquire_local(dir.path(), Duration::from_millis(100))
            .await
            .unwrap();
        assert!(first.path().ends_with(LOCK_FILE));
        let started = std::time::Instant::now();
        let err = acquire_local(dir.path(), Duration::from_millis(300))
            .await
            .expect_err("a second holder took the lock");
        assert!(is_locked(&err), "{err:#}");
        assert!(started.elapsed() >= Duration::from_millis(300));
        let holder = &err.downcast_ref::<StoreLocked>().unwrap().holder;
        assert!(holder.contains(&std::process::id().to_string()), "{holder}");
        drop(first);
        acquire_local(dir.path(), Duration::from_millis(100))
            .await
            .expect("free after release");
    }

    #[tokio::test]
    async fn a_local_lock_released_during_the_grace_is_taken() {
        let dir = tempfile::tempdir().unwrap();
        let first = acquire_local(dir.path(), Duration::from_millis(100))
            .await
            .unwrap();
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            drop(first);
        });
        acquire_local(dir.path(), Duration::from_secs(5))
            .await
            .expect("taken once the holder let go");
        releaser.await.unwrap();
    }

    // ---- remote lease -----------------------------------------------------

    #[tokio::test]
    async fn a_remote_lease_refuses_a_second_owner_then_passes_on_release() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = acquire_remote(store.clone(), "mem://s", quick(), WriteFence::new())
            .await
            .unwrap();
        assert_eq!(a.epoch().await, 1);
        let first = record(&store).await;
        assert!(!first.released);
        assert_eq!(first.pid, std::process::id());

        let err = acquire_remote(store.clone(), "mem://s", quick(), WriteFence::new())
            .await
            .expect_err("a second owner took a live lease");
        assert!(is_locked(&err), "{err:#}");

        a.release().await;
        assert!(record(&store).await.released);
        let b = acquire_remote(store.clone(), "mem://s", quick(), WriteFence::new())
            .await
            .unwrap();
        assert_eq!(b.epoch().await, 2);
        assert_ne!(record(&store).await.owner, first.owner);
    }

    #[tokio::test]
    async fn an_expired_lease_is_taken_over() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stale = LeaseRecord {
            owner: "crashed".into(),
            host: "elsewhere".into(),
            pid: 1,
            epoch: 7,
            expires_at_ms: chrono::Utc::now().timestamp_millis() - 1_000,
            released: false,
        };
        store
            .put(
                &ObjPath::from(LEASE_PATH),
                serde_json::to_vec(&stale).unwrap().into(),
            )
            .await
            .unwrap();
        let b = acquire_remote(store.clone(), "mem://s", quick(), WriteFence::new())
            .await
            .unwrap();
        assert_eq!(b.epoch().await, 8);
    }

    #[tokio::test]
    async fn a_holder_whose_lease_was_taken_over_is_fenced() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fence = WriteFence::new();
        let a = acquire_remote(store.clone(), "mem://s", quick(), Arc::clone(&fence))
            .await
            .unwrap();
        // Another process takes the lease over (as after a's lease expired
        // while it was paused).
        let mut taken = record(&store).await;
        taken.owner = "usurper".into();
        taken.epoch += 1;
        store
            .put(
                &ObjPath::from(LEASE_PATH),
                serde_json::to_vec(&taken).unwrap().into(),
            )
            .await
            .unwrap();
        assert!(a.renew().await.is_err());
        assert!(fence.is_tripped());
        let reason = &fence.reason_if_tripped().unwrap();
        assert!(
            reason.contains("usurper") || reason.contains("taken over"),
            "{reason}"
        );
        // Releasing a lease it no longer holds leaves the new holder's alone.
        a.release().await;
        assert_eq!(record(&store).await.owner, "usurper");
    }

    #[tokio::test]
    async fn renewal_extends_the_lease_and_release_fences() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fence = WriteFence::new();
        let a = acquire_remote(store.clone(), "mem://s", quick(), Arc::clone(&fence))
            .await
            .unwrap();
        let before = record(&store).await.expires_at_ms;
        tokio::time::sleep(Duration::from_millis(20)).await;
        a.renew().await.unwrap();
        assert!(record(&store).await.expires_at_ms > before);
        assert!(!fence.is_tripped());
        a.release().await;
        assert!(fence.is_tripped(), "a released handle could still write");
    }

    #[tokio::test]
    async fn background_renewal_keeps_a_short_lease_alive() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let settings = LeaseSettings {
            ttl: Duration::from_millis(600),
            renew_interval: Duration::from_millis(100),
            acquire_grace: Duration::from_millis(50),
            clock_skew: Duration::from_millis(0),
        };
        let fence = WriteFence::new();
        let a = acquire_remote(store.clone(), "mem://s", settings, Arc::clone(&fence))
            .await
            .unwrap();
        a.start_renewal();
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        // Long past the first expiry, the lease is still live and held.
        assert!(!fence.is_tripped());
        let err = acquire_remote(store.clone(), "mem://s", settings, WriteFence::new())
            .await
            .expect_err("a renewed lease was taken");
        assert!(is_locked(&err));
        a.release().await;
    }

    /// A store whose writes fail after `ok_puts` of them.
    #[derive(Debug)]
    struct FailingPuts {
        inner: InMemory,
        conditional: bool,
        ok_puts: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for FailingPuts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingPuts")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingPuts {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if !self.conditional && !matches!(opts.mode, PutMode::Overwrite) {
                return Err(object_store::Error::NotImplemented {
                    operation: "conditional put".into(),
                    implementer: "FailingPuts".into(),
                });
            }
            let left = self.ok_puts.load(std::sync::atomic::Ordering::SeqCst);
            if left == 0 {
                return Err(object_store::Error::Generic {
                    store: "FailingPuts",
                    source: Box::new(std::io::Error::other("unreachable")),
                });
            }
            self.ok_puts
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn a_store_without_conditional_writes_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(FailingPuts {
            inner: InMemory::new(),
            conditional: false,
            ok_puts: 100.into(),
        });
        let err = acquire_remote(store, "mem://s", quick(), WriteFence::new())
            .await
            .expect_err("a lease without conditional writes");
        assert!(err.downcast_ref::<LeaseUnsupported>().is_some(), "{err:#}");
        assert!(format!("{err:#}").contains("Unenforced"));
    }

    #[tokio::test]
    async fn renewal_failing_until_the_lease_would_lapse_fences_the_holder() {
        let inner = Arc::new(FailingPuts {
            inner: InMemory::new(),
            conditional: true,
            ok_puts: 1.into(), // the create
        });
        let store: Arc<dyn ObjectStore> = inner.clone();
        let settings = LeaseSettings {
            ttl: Duration::from_millis(400),
            renew_interval: Duration::from_millis(50),
            acquire_grace: Duration::from_millis(50),
            clock_skew: Duration::from_millis(0),
        };
        let fence = WriteFence::new();
        let a = acquire_remote(store, "mem://s", settings, Arc::clone(&fence))
            .await
            .unwrap();
        a.start_renewal();
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(
            fence.is_tripped(),
            "a holder that could not renew kept writing"
        );
        let reason = fence.reason_if_tripped().unwrap();
        assert!(reason.contains("renew"), "{reason}");
    }

    #[tokio::test]
    async fn a_holder_past_its_lease_deadline_is_fenced_without_a_renewal() {
        // A stalled process that resumes after its lease lapsed must not keep
        // writing until a renewal tells it so.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let settings = LeaseSettings {
            ttl: Duration::from_millis(300),
            renew_interval: Duration::from_millis(100),
            acquire_grace: Duration::from_millis(50),
            clock_skew: Duration::from_millis(0),
        };
        let fence = WriteFence::new();
        let _a = acquire_remote(store, "mem://s", settings, Arc::clone(&fence))
            .await
            .unwrap();
        assert!(fence.check().is_ok());
        // No renewal is running.
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(
            fence.check().is_err(),
            "a lapsed lease still allowed writes"
        );
    }

    #[tokio::test]
    async fn a_lease_lapsed_by_less_than_the_clock_skew_is_not_taken() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let recent = LeaseRecord {
            owner: "slow-clock".into(),
            host: "elsewhere".into(),
            pid: 1,
            epoch: 3,
            expires_at_ms: chrono::Utc::now().timestamp_millis() - 1_000,
            released: false,
        };
        store
            .put(
                &ObjPath::from(LEASE_PATH),
                serde_json::to_vec(&recent).unwrap().into(),
            )
            .await
            .unwrap();
        let settings = LeaseSettings {
            clock_skew: Duration::from_secs(5),
            ..quick()
        };
        let err = acquire_remote(store, "mem://s", settings, WriteFence::new())
            .await
            .expect_err("took a lease within the clock-skew allowance");
        assert!(is_locked(&err), "{err:#}");
    }

    #[tokio::test]
    async fn exactly_one_of_two_racing_openers_gets_the_lease() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (a, b) = tokio::join!(
            acquire_remote(store.clone(), "mem://s", quick(), WriteFence::new()),
            acquire_remote(store.clone(), "mem://s", quick(), WriteFence::new()),
        );
        assert_eq!(
            a.is_ok() as u8 + b.is_ok() as u8,
            1,
            "a={:?} b={:?}",
            a.as_ref().err(),
            b.as_ref().err()
        );
    }

    /// Applies an update, then reports a failure, as a request whose response
    /// was lost does (the client retries, or the caller timed out).
    #[derive(Debug)]
    struct LostAckOnce {
        inner: InMemory,
        lose_next_update: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for LostAckOnce {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LostAckOnce")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for LostAckOnce {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let update = matches!(opts.mode, PutMode::Update(_));
            let result = self.inner.put_opts(location, payload, opts).await?;
            if update
                && self
                    .lose_next_update
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(object_store::Error::Generic {
                    store: "LostAckOnce",
                    source: Box::new(std::io::Error::other("connection reset after send")),
                });
            }
            Ok(result)
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn a_renewal_that_landed_but_was_reported_failed_is_not_a_takeover() {
        let inner = Arc::new(LostAckOnce {
            inner: InMemory::new(),
            lose_next_update: false.into(),
        });
        let store: Arc<dyn ObjectStore> = inner.clone();
        let fence = WriteFence::new();
        let a = acquire_remote(store.clone(), "mem://s", quick(), Arc::clone(&fence))
            .await
            .unwrap();
        inner
            .lose_next_update
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(a.renew().await.is_err());
        // The next renewal finds its own lease, not someone else's.
        a.renew().await.unwrap();
        assert!(!fence.is_tripped(), "{:?}", fence.reason_if_tripped());
        a.release().await;
        assert!(record(&store).await.released, "the lease was not released");
    }

    /// Closing a handle trips its fence before the lease is given back; the
    /// release must still be written, or a reopen waits out the TTL.
    #[tokio::test]
    async fn a_release_after_the_fence_is_tripped_still_frees_the_lease() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fence = WriteFence::new();
        let a = acquire_remote(store.clone(), "mem://s", quick(), Arc::clone(&fence))
            .await
            .unwrap();
        fence.trip("the handle was closed");
        a.release().await;
        assert!(record(&store).await.released, "the lease was not released");
        let b = acquire_remote(store.clone(), "mem://s", quick(), WriteFence::new())
            .await
            .unwrap();
        b.release().await;
    }

    /// A holder whose lease was taken over leaves the new holder's lease
    /// alone when it closes.
    #[tokio::test]
    async fn a_release_after_a_takeover_leaves_the_new_lease_alone() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = acquire_remote(store.clone(), "mem://s", quick(), WriteFence::new())
            .await
            .unwrap();
        let path = ObjPath::from(LEASE_PATH);
        let mut taken = record(&store).await;
        taken.owner = "someone else".into();
        taken.epoch += 1;
        store
            .put(&path, serde_json::to_vec(&taken).unwrap().into())
            .await
            .unwrap();
        a.release().await;
        let now = record(&store).await;
        assert_eq!(now.owner, "someone else");
        assert!(!now.released, "the new holder's lease was released");
    }
}
