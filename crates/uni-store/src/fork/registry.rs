// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Persistent fork registry with create/drop 2PC.
//!
//! On disk the registry is `catalog/fork_registry.json`; per-fork
//! schema overlays live at `catalog/fork_schemas/{fork_id}.json` and
//! drop tombstones at `catalog/fork_tombstones/{fork_id}.json`. All
//! writes go through [`crate::store_utils::put_with_timeout`] (the
//! same primitive `SnapshotManager` uses) — atomicity is whatever
//! the underlying object store guarantees on PUT.
//!
//! The registry mutex is held only across in-memory mutation + the
//! single PUT for that mutation; it is *never* held across
//! `lance_branch::create_branch` or `delete_branch`. This preserves
//! the spec §10 guarantee that fork creation does not block primary.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tracing::{debug, instrument, warn};
use uni_common::api::error::UniError;
use uni_common::core::fork::{ForkId, ForkInfo, ForkRegistryFile, ForkStatus, SchemaDelta};

use crate::store_utils::{
    DEFAULT_TIMEOUT, delete_with_timeout, get_with_timeout, is_not_found, list_with_timeout,
    put_with_timeout,
};

/// Registry handle.
///
/// Holds the in-memory registry plus the locks needed for create/open
/// serialization and drop liveness checks. Cloning the handle clones the
/// internal `Arc`s; all clones see the same registry state.
#[derive(Clone)]
pub struct ForkRegistryHandle {
    inner: Arc<ForkRegistryInner>,
}

impl std::fmt::Debug for ForkRegistryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkRegistryHandle")
            .field("name_locks", &self.inner.name_locks.len())
            .field("holders", &self.inner.holders.len())
            .finish_non_exhaustive()
    }
}

struct ForkRegistryInner {
    store: Arc<dyn ObjectStore>,
    /// In-memory authoritative cache of `catalog/fork_registry.json`.
    cache: AsyncMutex<ForkRegistryFile>,
    /// Per-name mutex serializing concurrent open-or-create on the same
    /// fork name. Different names proceed in parallel.
    name_locks: DashMap<String, Arc<AsyncMutex<()>>>,
    /// Per-fork holder bookkeeping: a live-`ForkScope` count plus a
    /// `Notify` fired when the count returns to zero. Drop awaits the drain
    /// (deterministically, via the notify) rather than polling.
    holders: DashMap<ForkId, Arc<HolderState>>,
    /// Phase 4a: cap on total fork count enforced at `begin_create`.
    /// `None` ⇒ unbounded. Counts include Active + Pending + Tombstoned
    /// — tombstoned forks still hold branch state on disk until
    /// recovery completes, so counting them prevents churn-thrash.
    max_forks: AsyncMutex<Option<usize>>,
}

/// Per-fork holder bookkeeping (L1).
///
/// `count` is the number of live [`ForkScope`] handles; `drained` is fired
/// when `count` returns to zero so `drop_fork` can await the drain
/// deterministically (the last holder may be released by a background flush
/// finalizer that a fixed-duration poll could miss under CPU starvation).
#[derive(Debug, Default)]
pub(crate) struct HolderState {
    count: AtomicUsize,
    drained: Notify,
}

/// RAII guard that decrements a fork's holder count on drop.
///
/// Returned from [`ForkRegistryHandle::register_holder`]; the holding
/// session keeps it for its lifetime. Stored as `Arc` on `ForkScope`
/// so all clones of a forked session contribute to the same count.
pub struct ForkHolderGuard {
    state: Arc<HolderState>,
}

impl ForkHolderGuard {
    fn new(state: Arc<HolderState>) -> Self {
        state.count.fetch_add(1, Ordering::AcqRel);
        Self { state }
    }
}

impl Drop for ForkHolderGuard {
    fn drop(&mut self) {
        // When the last holder releases, wake anyone awaiting the drain.
        // `fetch_sub` returns the PREVIOUS value, so `== 1` means we just
        // went 1 → 0.
        if self.state.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.drained.notify_waiters();
        }
    }
}

fn registry_path() -> ObjectStorePath {
    ObjectStorePath::from("catalog/fork_registry.json")
}

fn schema_overlay_path(id: &ForkId) -> ObjectStorePath {
    ObjectStorePath::from(format!("catalog/fork_schemas/{id}.json"))
}

fn tombstone_path(id: &ForkId) -> ObjectStorePath {
    ObjectStorePath::from(format!("catalog/fork_tombstones/{id}.json"))
}

/// Wrap a typed error into `UniError::ForkLifecycle`.
fn lifecycle<E>(name: &str, stage: &'static str, source: E) -> UniError
where
    E: std::error::Error + Send + Sync + 'static,
{
    UniError::ForkLifecycle {
        name: name.to_string(),
        stage,
        source: Box::new(source),
    }
}

/// Wrap an `anyhow::Error` into `UniError::ForkLifecycle`.
///
/// `anyhow::Error` doesn't implement `std::error::Error` itself but
/// converts to `Box<dyn Error + Send + Sync>` via [`From`].
fn lifecycle_anyhow(name: &str, stage: &'static str, source: anyhow::Error) -> UniError {
    UniError::ForkLifecycle {
        name: name.to_string(),
        stage,
        source: source.into(),
    }
}

impl ForkRegistryHandle {
    /// Load the registry from disk, creating an empty one if absent.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::ForkCorruptRegistry`] if `catalog/fork_registry.json`
    /// exists but cannot be parsed, [`UniError::ForkLifecycle`] for IO failures.
    #[instrument(skip(store), level = "info")]
    pub async fn load(store: Arc<dyn ObjectStore>) -> Result<Self, UniError> {
        let path = registry_path();
        let cache = match get_with_timeout(&store, &path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| lifecycle("<registry>", "load", e))?;
                serde_json::from_slice::<ForkRegistryFile>(&bytes).map_err(|e| {
                    UniError::ForkCorruptRegistry {
                        message: format!("parse fork_registry.json: {e}"),
                    }
                })?
            }
            Err(e) if is_not_found(&e) => {
                // The registry has never been created (store reports NotFound).
                // Only then is an empty registry correct; the recovery driver in
                // `super::recovery` runs after load and reconciles any orphaned
                // partial states.
                ForkRegistryFile::default()
            }
            Err(e) => {
                // A transient/IO failure must NOT be read as "no registry":
                // starting empty here would let the next mutation persist an
                // empty registry over the real one, orphaning every existing
                // fork. Propagate as a lifecycle error per this method's
                // documented contract.
                return Err(lifecycle_anyhow("<registry>", "load", e));
            }
        };

        Ok(Self {
            inner: Arc::new(ForkRegistryInner {
                store,
                cache: AsyncMutex::new(cache),
                name_locks: DashMap::new(),
                holders: DashMap::new(),
                max_forks: AsyncMutex::new(None),
            }),
        })
    }

    /// Phase 4a: configure the budget cap on total fork count. Set
    /// from `Uni::open` after registry load. `None` means unbounded.
    pub async fn set_max_forks(&self, cap: Option<usize>) {
        *self.inner.max_forks.lock().await = cap;
    }

    /// Snapshot of the current registry (cheap clone of the file struct).
    pub async fn snapshot(&self) -> ForkRegistryFile {
        self.inner.cache.lock().await.clone()
    }

    /// List forks in `Active` status.
    pub async fn list_active(&self) -> Vec<ForkInfo> {
        let cache = self.inner.cache.lock().await;
        cache
            .forks
            .values()
            .filter(|f| f.status == ForkStatus::Active)
            .cloned()
            .collect()
    }

    /// Active forks whose `ttl_expires_at` is at or before `now`
    /// (Phase 4a, sweeper input). Pending and Tombstoned forks are
    /// skipped — they're handled by recovery, not the sweeper.
    pub async fn list_expired(&self, now: chrono::DateTime<chrono::Utc>) -> Vec<ForkInfo> {
        let cache = self.inner.cache.lock().await;
        cache
            .forks
            .values()
            .filter(|f| {
                f.status == ForkStatus::Active
                    && f.ttl_expires_at.map(|t| t <= now).unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Direct children of `parent_id` (Phase 3, nested forks).
    ///
    /// Returns every registry entry whose `parent_fork_id == Some(parent_id)`
    /// regardless of status — `Uni::drop_fork` and `drop_fork_cascade` need
    /// to see `Pending` and `Tombstoned` entries too so they don't let a
    /// half-created or half-dropped child slip through the guard.
    pub async fn list_children(&self, parent_id: ForkId) -> Vec<ForkInfo> {
        let cache = self.inner.cache.lock().await;
        cache
            .forks
            .values()
            .filter(|f| f.parent_fork_id == Some(parent_id))
            .cloned()
            .collect()
    }

    /// Look up a fork by id.
    ///
    /// # Errors
    ///
    /// [`UniError::ForkNotFound`] if no fork has this id.
    pub async fn get_by_id(&self, id: ForkId) -> Result<ForkInfo, UniError> {
        let cache = self.inner.cache.lock().await;
        cache
            .forks
            .values()
            .find(|f| f.id == id)
            .cloned()
            .ok_or_else(|| UniError::ForkNotFound {
                name: id.to_string(),
            })
    }

    /// Look up a fork by name.
    ///
    /// # Errors
    ///
    /// [`UniError::ForkNotFound`] if no fork has this name.
    pub async fn get(&self, name: &str) -> Result<ForkInfo, UniError> {
        let cache = self.inner.cache.lock().await;
        cache
            .forks
            .get(name)
            .cloned()
            .ok_or_else(|| UniError::ForkNotFound {
                name: name.to_string(),
            })
    }

    /// Acquire the per-name mutex for create-or-open serialization.
    ///
    /// Used by `Session::fork(name).build()` to ensure at most one
    /// open-or-create runs per name at a time. Different names proceed
    /// in parallel. The mutex map grows monotonically with fork churn;
    /// a sweeper is acceptable in Phase 4.
    pub async fn name_lock(&self, name: &str) -> Arc<AsyncMutex<()>> {
        self.inner
            .name_locks
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    /// Register a live session on `fork_id`, returning a guard.
    pub fn register_holder(&self, fork_id: ForkId) -> ForkHolderGuard {
        let state = self.inner.holders.entry(fork_id).or_default().clone();
        ForkHolderGuard::new(state)
    }

    /// Public holder count for `fork_id` (Phase 3 — cascade pre-validation).
    pub async fn holder_count_for(&self, fork_id: ForkId) -> usize {
        self.holder_count(fork_id)
    }

    fn holder_count(&self, fork_id: ForkId) -> usize {
        self.inner
            .holders
            .get(&fork_id)
            .map(|s| s.count.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Await the fork's holder count reaching zero, bounded by `timeout`. (L1)
    ///
    /// Returns the final holder count (0 on success). Deterministic: it wakes
    /// as soon as the last [`ForkHolderGuard`] drops — including when that
    /// drop happens inside a background flush finalizer that a fixed-duration
    /// poll could starve past — rather than sleeping a fixed budget.
    pub async fn wait_holders_drained(&self, fork_id: ForkId, timeout: Duration) -> usize {
        let Some(state) = self.inner.holders.get(&fork_id).map(|s| s.clone()) else {
            return 0; // no holder ever registered for this fork
        };
        if state.count.load(Ordering::Acquire) == 0 {
            return 0;
        }
        // Register interest BEFORE the final count re-check so a
        // `notify_waiters` between the two cannot be lost.
        let notified = state.drained.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if state.count.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let _ = tokio::time::timeout(timeout, notified).await;
        state.count.load(Ordering::Acquire)
    }

    // ============================================================
    // Internal persistence helpers — every PUT goes through these.
    // ============================================================

    /// Replace the registry file on disk with `cache`.
    ///
    /// Caller must hold the cache lock; this function only does IO.
    async fn put_registry(
        &self,
        cache: &ForkRegistryFile,
        name: &str,
        stage: &'static str,
    ) -> Result<(), UniError> {
        let json = serde_json::to_vec_pretty(cache).map_err(|e| lifecycle(name, stage, e))?;
        put_with_timeout(
            &self.inner.store,
            &registry_path(),
            Bytes::from(json),
            DEFAULT_TIMEOUT,
        )
        .await
        .map_err(|e| lifecycle_anyhow(name, stage, e))?;
        Ok(())
    }

    async fn put_schema_overlay(
        &self,
        id: &ForkId,
        delta: &SchemaDelta,
        name: &str,
    ) -> Result<(), UniError> {
        let json =
            serde_json::to_vec_pretty(delta).map_err(|e| lifecycle(name, "schema_overlay", e))?;
        put_with_timeout(
            &self.inner.store,
            &schema_overlay_path(id),
            Bytes::from(json),
            DEFAULT_TIMEOUT,
        )
        .await
        .map_err(|e| lifecycle_anyhow(name, "schema_overlay", e))?;
        Ok(())
    }

    async fn put_tombstone(&self, info: &ForkInfo) -> Result<(), UniError> {
        let json =
            serde_json::to_vec_pretty(info).map_err(|e| lifecycle(&info.name, "tombstone", e))?;
        put_with_timeout(
            &self.inner.store,
            &tombstone_path(&info.id),
            Bytes::from(json),
            DEFAULT_TIMEOUT,
        )
        .await
        .map_err(|e| lifecycle_anyhow(&info.name, "tombstone", e))?;
        Ok(())
    }

    async fn delete_tombstone(&self, id: &ForkId, _name: &str) -> Result<(), UniError> {
        // Treat missing tombstone as success — recovery may have already
        // cleaned it. Schema overlay deletion is paired with this in the
        // caller (`finish_drop`).
        if let Err(e) =
            delete_with_timeout(&self.inner.store, &tombstone_path(id), DEFAULT_TIMEOUT).await
        {
            warn!(fork_id = %id, "delete tombstone returned {e}");
        }
        Ok(())
    }

    async fn delete_schema_overlay(&self, id: &ForkId) {
        let _ =
            delete_with_timeout(&self.inner.store, &schema_overlay_path(id), DEFAULT_TIMEOUT).await;
    }

    /// Phase 2 Day 10: register a branch created after fork-point on
    /// the named dataset. Mutates the in-memory `ForkInfo.datasets`
    /// map and PUTs the updated registry file so a restart recovers
    /// the same dataset → branch mapping.
    ///
    /// Idempotent — re-registering an existing entry with the same
    /// branch name is a no-op; mismatched branch names error so a
    /// double-create bug doesn't silently lose data.
    pub async fn register_dataset_branch(
        &self,
        fork_id: ForkId,
        dataset: &str,
        branch: &str,
    ) -> Result<(), UniError> {
        let mut cache = self.inner.cache.lock().await;
        // Find by id (BTreeMap is keyed by name, so iterate).
        let entry = cache
            .forks
            .values_mut()
            .find(|f| f.id == fork_id)
            .ok_or_else(|| UniError::ForkNotFound {
                name: format!("<fork:{fork_id}>"),
            })?;
        match entry.datasets.get(dataset) {
            Some(existing) if existing == branch => return Ok(()),
            Some(existing) => {
                return Err(UniError::ForkCorruptRegistry {
                    message: format!(
                        "register_dataset_branch: dataset '{dataset}' already \
                         maps to '{existing}', refusing to overwrite with \
                         '{branch}'"
                    ),
                });
            }
            None => {}
        }
        entry
            .datasets
            .insert(dataset.to_string(), branch.to_string());
        let name = entry.name.clone();
        self.put_registry(&cache, &name, "registry_dynamic_branch")
            .await?;
        Ok(())
    }

    /// Replace the persisted schema overlay for `id` with `delta`.
    ///
    /// Used by [`crate::fork::ForkScope::add_label_to_overlay`] and
    /// [`crate::fork::ForkScope::add_edge_type_to_overlay`] to durably
    /// record fork-local schema additions. The caller is expected to
    /// hold the per-scope `overlay_lock` so two concurrent updates on
    /// the same fork don't clobber each other; cross-fork updates
    /// remain parallel because each scope has its own lock.
    ///
    /// The overlay is a single full-replace JSON file under
    /// `catalog/fork_schemas/{id}.json` — the same shape as the empty
    /// delta written at fork creation time. There is no in-memory
    /// registry cache to keep coherent (the registry doesn't cache
    /// overlays); the caller's `ArcSwap` is the in-memory source of
    /// truth.
    pub async fn update_schema_overlay(
        &self,
        id: &ForkId,
        delta: &SchemaDelta,
    ) -> Result<(), UniError> {
        // Resolve the fork name for diagnostic purposes only; the PUT
        // itself doesn't need the cache lock.
        let name = {
            let cache = self.inner.cache.lock().await;
            cache
                .forks
                .values()
                .find(|f| f.id == *id)
                .map(|f| f.name.clone())
                .unwrap_or_else(|| format!("<fork:{id}>"))
        };
        self.put_schema_overlay(id, delta, &name).await
    }

    /// Read the schema overlay for `id`. Returns empty if absent.
    pub async fn load_schema_overlay(&self, id: &ForkId) -> Result<SchemaDelta, UniError> {
        match get_with_timeout(&self.inner.store, &schema_overlay_path(id), DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| lifecycle("<schema-overlay>", "schema_overlay", e))?;
                serde_json::from_slice(&bytes).map_err(|e| UniError::ForkCorruptRegistry {
                    message: format!("parse fork_schemas/{id}.json: {e}"),
                })
            }
            // Only a genuine absence means "this fork has no overlay". A
            // timeout, an auth failure or an IO fault used to read the same way,
            // and the fork then ran on the *parent's* schema without anything
            // surfacing (#233).
            Err(e) if crate::store_utils::is_not_found(&e) => Ok(SchemaDelta::empty()),
            Err(e) => Err(lifecycle_anyhow("<schema-overlay>", "schema_overlay", e)),
        }
    }

    // ============================================================
    // Public mutators — used by Session::fork(...) and Uni::drop_fork
    // ============================================================

    /// Insert a `Pending` registry entry (create 2PC step 2).
    ///
    /// Returns `Err(ForkAlreadyExists)` if `name` is already registered.
    /// Caller must hold the per-name lock from [`Self::name_lock`].
    pub async fn begin_create(&self, info: ForkInfo) -> Result<(), UniError> {
        debug_assert_eq!(info.status, ForkStatus::Pending);
        let mut cache = self.inner.cache.lock().await;
        if cache.forks.contains_key(&info.name) {
            return Err(UniError::ForkAlreadyExists {
                name: info.name.clone(),
            });
        }
        // Phase 4a: enforce the budget cap. Counts include all
        // statuses so a churn loop of create/drop doesn't slip past
        // the cap while tombstones await recovery.
        if let Some(cap) = *self.inner.max_forks.lock().await {
            let current = cache.forks.len();
            if current >= cap {
                return Err(UniError::ForkBudgetExceeded { current, max: cap });
            }
        }
        let name = info.name.clone();
        cache.forks.insert(name.clone(), info);
        self.put_registry(&cache, &name, "registry_pending").await?;
        Ok(())
    }

    /// Promote `Pending` → `Active` with the resolved `datasets` map and
    /// write the empty schema overlay (create 2PC step 4).
    pub async fn finish_create(
        &self,
        name: &str,
        datasets: BTreeMap<String, String>,
    ) -> Result<ForkInfo, UniError> {
        let id = {
            let mut cache = self.inner.cache.lock().await;
            let entry = cache
                .forks
                .get_mut(name)
                .ok_or_else(|| UniError::ForkNotFound {
                    name: name.to_string(),
                })?;
            entry.datasets = datasets;
            entry.status = ForkStatus::Active;
            let id = entry.id;
            self.put_registry(&cache, name, "registry_active").await?;
            id
        };

        // Crash seam: the registry says Active but the overlay file does not
        // exist yet. `finish_create` is two PUTs, not one, and this window sits
        // between them — the fork is fully reachable while a file it nominally
        // owns is missing. Benign by design, since `load_schema_overlay`
        // returns an empty delta on any read failure, but a seam here turns
        // that from an assumption into something a test can assert.
        fail::fail_point!("fork::create-mid-finish");

        // Outside the cache lock — schema overlay PUT shouldn't block readers.
        self.put_schema_overlay(&id, &SchemaDelta::empty(), name)
            .await?;

        let info = self.get(name).await?;
        debug!(fork_id = %id, fork_name = %name, "fork active");
        Ok(info)
    }

    /// Roll back a `Pending` entry on partial failure during create.
    ///
    /// The cleanup also removes any schema-overlay file that may have
    /// been written for the rolled-back fork. In Phase 1 the overlay
    /// is only written in `finish_create`, so a Pending rollback
    /// usually finds nothing to delete — but Phase 2 may move overlay
    /// creation earlier, and recovery from a partial finish_create
    /// can leave overlay+pending registry on disk together. Capturing
    /// the id *before* removal keeps the cleanup correct under both.
    pub async fn rollback_create(&self, name: &str) -> Result<(), UniError> {
        let removed_id = {
            let mut cache = self.inner.cache.lock().await;
            let id = cache.forks.remove(name).map(|info| info.id);
            self.put_registry(&cache, name, "registry_pending").await?;
            id
        };
        // Outside the cache lock — IO must not block primary.
        if let Some(id) = removed_id {
            self.delete_schema_overlay(&id).await;
            // L4: reap the holder counter for the rolled-back fork.
            self.inner.holders.remove(&id);
        }
        self.inner
            .name_locks
            .remove_if(name, |_, lock| Arc::strong_count(lock) == 1);
        Ok(())
    }

    /// Drop 2PC step 1+2: write tombstone, flip registry to `Tombstoned`.
    ///
    /// # Errors
    ///
    /// - [`UniError::ForkNotFound`] if `name` is unknown.
    /// - [`UniError::ForkInUse`] if any session holds the fork.
    pub async fn begin_drop(&self, name: &str) -> Result<ForkInfo, UniError> {
        let info = {
            let cache = self.inner.cache.lock().await;
            cache
                .forks
                .get(name)
                .cloned()
                .ok_or_else(|| UniError::ForkNotFound {
                    name: name.to_string(),
                })?
        };

        let holders = self.holder_count(info.id);
        if holders > 0 {
            return Err(UniError::ForkInUse {
                name: info.name.clone(),
                holder_count: holders,
            });
        }

        // Step 1: durable intent.
        self.put_tombstone(&info).await?;

        // Step 2: flip status.
        let mut cache = self.inner.cache.lock().await;
        if let Some(entry) = cache.forks.get_mut(name) {
            entry.status = ForkStatus::Tombstoned;
            self.put_registry(&cache, name, "tombstone").await?;
        }

        Ok(info)
    }

    /// Drop 2PC step 4+5: remove the registry entry, delete tombstone +
    /// schema overlay files. Caller must have completed
    /// `lance_branch::delete_branch` for every entry in `info.datasets`
    /// before calling this.
    pub async fn finish_drop(&self, info: &ForkInfo) -> Result<(), UniError> {
        {
            let mut cache = self.inner.cache.lock().await;
            cache.forks.remove(&info.name);
            self.put_registry(&cache, &info.name, "registry_clear")
                .await?;
        }
        self.delete_tombstone(&info.id, &info.name).await?;
        self.delete_schema_overlay(&info.id).await;
        // L4: reap per-fork bookkeeping so the maps don't grow with fork
        // churn. `holders` is keyed by the fork's fresh ULID (so it grows
        // even under same-name churn); the fork is gone now, so no
        // concurrent `register_holder` can race it. `name_locks` is shared
        // by concurrent callers, so only reap it when no one else holds the
        // Arc (strong_count 1 = just the map entry).
        self.inner.holders.remove(&info.id);
        self.inner
            .name_locks
            .remove_if(info.name.as_str(), |_, lock| Arc::strong_count(lock) == 1);
        // Note: the fork's WAL (`wal_forks/{id}/`), id allocator, and fork-scoped
        // snapshot manifests live on the STORAGE object store, not the registry's
        // metadata store, so they are cleaned by `delete_fork_artifacts` from the
        // drop / recovery paths that hold the storage store (review H3).
        Ok(())
    }

    /// Discover any tombstones on disk (used by recovery).
    pub async fn list_tombstones(&self) -> Result<Vec<ForkInfo>, UniError> {
        let prefix = ObjectStorePath::from("catalog/fork_tombstones");
        let metas = list_with_timeout(&self.inner.store, Some(&prefix), DEFAULT_TIMEOUT)
            .await
            .map_err(|e| lifecycle_anyhow("<tombstones>", "recovery", e))?;

        let mut out = Vec::new();
        for meta in metas {
            let result = get_with_timeout(&self.inner.store, &meta.location, DEFAULT_TIMEOUT)
                .await
                .map_err(|e| lifecycle_anyhow("<tombstones>", "recovery", e))?;
            let bytes = result
                .bytes()
                .await
                .map_err(|e| lifecycle("<tombstones>", "recovery", e))?;
            let info: ForkInfo =
                serde_json::from_slice(&bytes).map_err(|e| UniError::ForkCorruptRegistry {
                    message: format!("parse tombstone {}: {e}", meta.location),
                })?;
            out.push(info);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;
    use uni_common::core::fork::ForkId;

    async fn fresh_handle() -> (TempDir, ForkRegistryHandle) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let handle = ForkRegistryHandle::load(store).await.unwrap();
        (dir, handle)
    }

    #[tokio::test]
    async fn empty_registry_loads_clean() {
        let (_dir, h) = fresh_handle().await;
        assert!(h.snapshot().await.forks.is_empty());
    }

    #[tokio::test]
    async fn begin_create_persists_and_rejects_duplicate() {
        let (_dir, h) = fresh_handle().await;
        let info = ForkInfo::new_pending(ForkId::new(), "scenario_a", "snap-1", 17);
        h.begin_create(info.clone()).await.unwrap();

        // Reload from disk and confirm Pending state survived the PUT.
        let store = h.inner.store.clone();
        let h2 = ForkRegistryHandle::load(store).await.unwrap();
        let snap = h2.snapshot().await;
        assert_eq!(snap.forks.len(), 1);
        assert_eq!(snap.forks["scenario_a"].status, ForkStatus::Pending);

        // Duplicate refused.
        let dup = ForkInfo::new_pending(ForkId::new(), "scenario_a", "snap-1", 17);
        let err = h.begin_create(dup).await.unwrap_err();
        assert!(matches!(err, UniError::ForkAlreadyExists { .. }));
    }

    #[tokio::test]
    async fn finish_create_promotes_and_writes_overlay() {
        let (_dir, h) = fresh_handle().await;
        let info = ForkInfo::new_pending(ForkId::new(), "scenario_b", "snap-1", 1);
        h.begin_create(info).await.unwrap();

        let mut datasets = BTreeMap::new();
        datasets.insert("vertices_Person".into(), "fork-b__v_Person".into());
        let promoted = h.finish_create("scenario_b", datasets).await.unwrap();
        assert_eq!(promoted.status, ForkStatus::Active);
        assert_eq!(promoted.datasets.len(), 1);

        // Overlay is empty in Phase 1 but the file must exist.
        let overlay = h.load_schema_overlay(&promoted.id).await.unwrap();
        assert!(overlay.is_empty());
    }

    #[tokio::test]
    async fn drop_2pc_clears_registry_and_files() {
        let (_dir, h) = fresh_handle().await;
        let info = ForkInfo::new_pending(ForkId::new(), "scenario_c", "snap-1", 1);
        h.begin_create(info).await.unwrap();
        let info = h
            .finish_create("scenario_c", BTreeMap::new())
            .await
            .unwrap();

        let tomb = h.begin_drop("scenario_c").await.unwrap();
        assert_eq!(tomb.id, info.id);

        // After begin_drop, status is Tombstoned, tombstone file exists.
        let snap = h.snapshot().await;
        assert_eq!(snap.forks["scenario_c"].status, ForkStatus::Tombstoned);
        let tombs = h.list_tombstones().await.unwrap();
        assert_eq!(tombs.len(), 1);

        h.finish_drop(&info).await.unwrap();
        let snap = h.snapshot().await;
        assert!(!snap.forks.contains_key("scenario_c"));
        let tombs = h.list_tombstones().await.unwrap();
        assert!(tombs.is_empty());
    }

    #[tokio::test]
    async fn drop_blocked_when_holders_alive() {
        let (_dir, h) = fresh_handle().await;
        let info = ForkInfo::new_pending(ForkId::new(), "scenario_d", "snap-1", 1);
        h.begin_create(info).await.unwrap();
        let info = h
            .finish_create("scenario_d", BTreeMap::new())
            .await
            .unwrap();

        let _holder = h.register_holder(info.id);
        let err = h.begin_drop("scenario_d").await.unwrap_err();
        match err {
            UniError::ForkInUse { holder_count, .. } => assert_eq!(holder_count, 1),
            other => panic!("expected ForkInUse, got {other:?}"),
        }

        // Drop the holder; now drop succeeds.
        drop(_holder);
        h.begin_drop("scenario_d").await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_distinct_creates_serialize_only_per_name() {
        // Different names proceed in parallel through their own name_locks.
        // Same name serializes; the second begin_create here would race
        // with the first, so we instead verify both names land.
        let (_dir, h) = fresh_handle().await;
        let h1 = h.clone();
        let h2 = h.clone();
        let t1 = tokio::spawn(async move {
            h1.begin_create(ForkInfo::new_pending(ForkId::new(), "fork-a", "snap-1", 1))
                .await
        });
        let t2 = tokio::spawn(async move {
            h2.begin_create(ForkInfo::new_pending(ForkId::new(), "fork-b", "snap-1", 1))
                .await
        });
        t1.await.unwrap().unwrap();
        t2.await.unwrap().unwrap();

        let snap = h.snapshot().await;
        assert_eq!(snap.forks.len(), 2);
    }

    #[tokio::test]
    async fn name_lock_grants_exclusive_per_name() {
        let (_dir, h) = fresh_handle().await;
        let lock = h.name_lock("scenario_e").await;
        let g1 = lock.try_lock();
        assert!(g1.is_ok());
        // While guard is held, a second try_lock against the same name's
        // lock fails. Re-fetching the same name returns the same Arc.
        let same = h.name_lock("scenario_e").await;
        assert!(Arc::ptr_eq(&lock, &same));
    }

    #[tokio::test]
    async fn corrupt_registry_surfaces_typed_error() {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        // Pre-populate with garbage.
        put_with_timeout(
            &store,
            &registry_path(),
            Bytes::from_static(b"{ not json"),
            DEFAULT_TIMEOUT,
        )
        .await
        .unwrap();

        let err = ForkRegistryHandle::load(store).await.unwrap_err();
        assert!(matches!(err, UniError::ForkCorruptRegistry { .. }));
    }

    #[tokio::test]
    async fn rollback_create_after_finish_cleans_overlay_file() {
        // Latent-bug regression: even though Phase 1's begin_create
        // doesn't write the overlay, finish_create does. Recovery may
        // rollback an entry that already has an overlay file on disk;
        // confirm the file is cleaned even when the registry entry is
        // already gone from the cache when delete is called.
        let (_dir, h) = fresh_handle().await;
        let info = ForkInfo::new_pending(ForkId::new(), "scenario_rb", "snap-1", 1);
        h.begin_create(info).await.unwrap();
        let info = h
            .finish_create("scenario_rb", BTreeMap::new())
            .await
            .unwrap();
        // Overlay file exists.
        let exists_before = h
            .inner
            .store
            .head(&schema_overlay_path(&info.id))
            .await
            .is_ok();
        assert!(exists_before);

        // Force a Pending state by rolling back from Active (simulates
        // recovery code path where the recovery driver reuses
        // rollback_create on an already-promoted entry).
        h.rollback_create("scenario_rb").await.unwrap();

        // Entry gone, overlay file cleaned up.
        assert!(!h.snapshot().await.forks.contains_key("scenario_rb"));
        let exists_after = h
            .inner
            .store
            .head(&schema_overlay_path(&info.id))
            .await
            .is_ok();
        assert!(
            !exists_after,
            "overlay file at {} should be deleted after rollback",
            schema_overlay_path(&info.id)
        );
    }

    #[tokio::test]
    async fn restart_preserves_active_forks() {
        // Phase 1 exit criterion: forks survive process restart.
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        {
            let h = ForkRegistryHandle::load(store.clone()).await.unwrap();
            let info = ForkInfo::new_pending(ForkId::new(), "persist_me", "snap-1", 1);
            h.begin_create(info).await.unwrap();
            h.finish_create("persist_me", BTreeMap::new())
                .await
                .unwrap();
            // Drop the handle; in-memory state goes away.
        }

        // Reload from the same store; entry must reappear with Active status.
        let h2 = ForkRegistryHandle::load(store).await.unwrap();
        let snap = h2.snapshot().await;
        assert_eq!(snap.forks.len(), 1);
        assert_eq!(snap.forks["persist_me"].status, ForkStatus::Active);
    }

    #[tokio::test]
    async fn holder_count_round_trips_under_concurrent_register_drop() {
        let (_dir, h) = fresh_handle().await;
        let info = ForkInfo::new_pending(ForkId::new(), "concurrent", "snap-1", 1);
        h.begin_create(info).await.unwrap();
        let info = h
            .finish_create("concurrent", BTreeMap::new())
            .await
            .unwrap();

        // Spawn 100 tasks that register a holder, do nothing, drop.
        let mut handles = Vec::new();
        for _ in 0..100 {
            let h_clone = h.clone();
            let id = info.id;
            handles.push(tokio::spawn(async move {
                let _g = h_clone.register_holder(id);
                tokio::task::yield_now().await;
                // _g drops at end of scope.
            }));
        }
        for jh in handles {
            jh.await.unwrap();
        }

        // After all holders drop, count is 0 — drop should now succeed.
        h.begin_drop("concurrent").await.unwrap();
    }

    /// L1: `wait_holders_drained` wakes the instant the last holder is
    /// released — even when that release races the wait — instead of
    /// burning the whole timeout. Uses `join!` so the release and the wait
    /// are driven concurrently on one task (the deterministic analogue of a
    /// background flush finalizer dropping the guard mid-wait).
    #[tokio::test]
    async fn wait_holders_drained_wakes_on_last_release() {
        let (_dir, h) = fresh_handle().await;
        let id = ForkId::new();
        let guard = h.register_holder(id);
        assert_eq!(h.holder_count_for(id).await, 1);

        let waiter = h.wait_holders_drained(id, Duration::from_secs(30));
        let releaser = async move {
            // Yield a few times so the waiter is parked on the notify first.
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
            drop(guard);
        };
        let (remaining, ()) = tokio::join!(waiter, releaser);
        assert_eq!(remaining, 0, "wait must observe the drain, not time out");
    }

    /// L1: while a holder is alive, `wait_holders_drained` returns the live
    /// count after the bounded timeout (it genuinely waits).
    #[tokio::test]
    async fn wait_holders_drained_times_out_while_held() {
        let (_dir, h) = fresh_handle().await;
        let id = ForkId::new();
        let _guard = h.register_holder(id);
        let remaining = h.wait_holders_drained(id, Duration::from_millis(50)).await;
        assert_eq!(
            remaining, 1,
            "a still-held fork must report its live holder"
        );

        // An unknown fork (no holder ever registered) drains immediately.
        assert_eq!(
            h.wait_holders_drained(ForkId::new(), Duration::from_secs(30))
                .await,
            0
        );
    }
}
