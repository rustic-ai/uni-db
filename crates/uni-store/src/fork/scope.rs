// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `ForkScope` — read-only state shared by every component of a forked
//! session.
//!
//! A `ForkScope` is owned by a forked `Session`'s `UniInner` and carries
//! everything `StorageManager` and `SchemaManager` need to resolve fork-
//! aware reads:
//!
//! - `fork_info` — registry record, including the dataset → branch map
//!   used to route Lance reads through the fork's branches.
//! - `overlay` — `SchemaDelta` merged on top of primary's schema by
//!   `UniInner::at_fork` at construction time.
//! - `registry` — back-reference for liveness queries; holders are
//!   tracked here so drop refuses while sessions are alive.
//! - `_holder` — RAII guard that decrements the holder count when the
//!   scope is dropped.
//!
//! `fork_info` is wrapped in plain `Arc` (no fork-side mutation today
//! — datasets only grow through `register_dynamic_branch` which goes
//! through the registry, not through `fork_info`). `overlay` is wrapped
//! in `ArcSwap` so fork-local strict-schema additions can be applied
//! atomically without rebuilding the scope.

// Rust guideline compliant

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use tokio::sync::Mutex as AsyncMutex;
use uni_common::core::fork::{ForkId, ForkInfo, SchemaDelta};
use uni_common::core::schema::{EdgeTypeMeta, LabelMeta};

use super::registry::{ForkHolderGuard, ForkRegistryHandle};

/// Phase 5a: tag for the fork-local index registry on `ForkScope`.
/// Phase 5b extends with `Vector` and `FullText` for lossy fusion.
///
/// `#[non_exhaustive]` so additional kinds (e.g. inverted-set,
/// JSON path) can land additively without breaking match sites.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ForkLocalIndexKind {
    /// Scalar BTree on a property — union fusion (Phase 5a-impl).
    ScalarBtree,
    /// Sorted on a property (range / ORDER BY) — k-way merge fusion (Phase 5a-impl).
    Sorted,
    /// VID/UID lookup index — fork-first fusion (Phase 5a-impl).
    VidUid,
    /// Vector (IVF/HNSW) index — top-k merge + rerank fusion (Phase 5b).
    Vector,
    /// Lance native FTS / inverted index — RRF fusion (Phase 5b).
    FullText,
    /// Learned-sparse (SPLADE) dot-product index — `SparseDot` rerank fusion
    /// (issue #95 Task #4).
    ///
    /// v1 retrieval on a fork is a brute-force branch scan re-scored by
    /// `sparse_dot` (see [`crate::storage::StorageManager::sparse_search`]); this
    /// variant is a planner/EXPLAIN marker that switches `uni.sparse.query` to the
    /// fused operator. A dedicated fork-local sparse postings dataset (Approach B)
    /// is deferred behind the M5 benchmark.
    Sparse,
}

/// Read-only scope identifying a forked session.
///
/// Constructed by `Session::fork(name).build()` (Day 7) via
/// [`ForkScope::new`]. Once built, both `fork_info` and `overlay` are
/// immutable for the scope's lifetime — Phase 1 forks are read-only.
pub struct ForkScope {
    fork_id: ForkId,
    fork_info: Arc<ForkInfo>,
    /// Schema additions on top of primary's schema. Mutable so that
    /// `Session::fork_schema()` can introduce fork-local labels and
    /// edge types without touching primary's `catalog/schema.json`.
    /// `ArcSwap` makes reads cheap and atomic; the `overlay_lock`
    /// below serializes the read-modify-write on the persistence side.
    ///
    /// # Invariant: fork-origin numeric ids are fork-local (L7)
    ///
    /// The overlay is frozen at fork time, so a label/edge-type id minted
    /// inside a fork (via `max(existing)+1`) does not observe primary's
    /// later additions and **can collide** with a primary id allocated
    /// after the fork point. This is benign because nothing trusts a
    /// fork-origin id across the fork↔primary boundary: promote
    /// (`uni_fork::diff`) re-creates by NAME, primary re-allocates its own
    /// id, and storage keys rows by label name. A fork-origin numeric id
    /// MUST NOT be trusted outside the fork's own view.
    overlay: Arc<ArcSwap<SchemaDelta>>,
    /// Serializes overlay updates *within a single fork* so two
    /// concurrent `add_label_to_overlay` calls don't clobber each
    /// other's persisted state. Held across the registry PUT and the
    /// `ArcSwap::store`. Cross-fork updates remain parallel.
    overlay_lock: Arc<AsyncMutex<()>>,
    registry: Arc<ForkRegistryHandle>,
    /// Branches created after fork construction, e.g. by
    /// [`crate::backend::BranchedBackend`] when the fork's writer
    /// flushes to a label whose dataset wasn't branched at fork-point.
    /// Consulted alongside `fork_info.datasets` by [`Self::branch_for`]
    /// so reads on the same session see writes through the same
    /// branch that produced them. Persisted out-of-band via
    /// [`ForkRegistryHandle::register_dataset_branch`] so a restart
    /// recovers the same mapping.
    dynamic_branches: Arc<DashMap<String, String>>,
    /// Phase 5a: per-table row count contributed by this fork's
    /// writes. Bumped by `BranchedBackend` after each successful
    /// flush. Read by `IndexRebuildManager` to decide whether to
    /// schedule a fork-local index build for the table. In-memory
    /// only — a process restart resets the counter, so the trigger
    /// re-fires on the next flush. The on-disk row count is the
    /// ground truth; this counter is only a flush-time accumulator.
    fragment_counts: Arc<DashMap<String, u64>>,
    /// Phase 5a: registry of completed fork-local index builds.
    /// Keyed on `(label, column)`; value is the SET of index kinds
    /// built for that pair. A single column can carry several kinds
    /// simultaneously (e.g. a `ScalarBtree` for equality plus a
    /// `FullText` for search), so this must be a set — a single-value
    /// map would let one build clobber another and the auto-builder
    /// would ping-pong between them forever. Read by the planner's
    /// `has_fork_index` check to decide whether to emit a specific
    /// `FusedIndexScan`. Written by the `IndexRebuildManager` after a
    /// fork-local build completes. In-memory only — a restart
    /// re-detects existing fork-local indexes by listing the fork's
    /// branch directory once at `Uni::open` time (Phase 5a uses lazy
    /// first-touch detection; see `repopulate_indexes_from_disk`).
    fork_local_indexes: Arc<DashMap<(String, String), HashSet<ForkLocalIndexKind>>>,
    /// RAII guard. Lifetime-tied to this `ForkScope`. Cloning the
    /// containing `Arc<ForkScope>` does *not* increment the holder
    /// count — only the constructor does, via `register_holder`.
    _holder: ForkHolderGuard,
}

impl std::fmt::Debug for ForkScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkScope")
            .field("fork_id", &self.fork_id)
            .field("fork_name", &self.fork_info.name)
            .finish_non_exhaustive()
    }
}

impl ForkScope {
    /// Build a new fork scope, registering a holder on `registry`.
    ///
    /// `fork_info` must already be in `Active` status; callers should
    /// have walked the registry's open-or-create flow before invoking.
    /// `overlay` is the schema delta loaded from
    /// `catalog/fork_schemas/{fork_id}.json`.
    #[must_use]
    pub fn new(
        fork_info: Arc<ForkInfo>,
        overlay: SchemaDelta,
        registry: Arc<ForkRegistryHandle>,
    ) -> Self {
        let holder = registry.register_holder(fork_info.id);
        Self {
            fork_id: fork_info.id,
            fork_info,
            overlay: Arc::new(ArcSwap::from_pointee(overlay)),
            overlay_lock: Arc::new(AsyncMutex::new(())),
            registry,
            dynamic_branches: Arc::new(DashMap::new()),
            fragment_counts: Arc::new(DashMap::new()),
            fork_local_indexes: Arc::new(DashMap::new()),
            _holder: holder,
        }
    }

    /// Phase 5a: record `rows_added` rows newly written through this
    /// fork to `table_name`. Idempotent under repeated calls — the
    /// counter is monotonically increasing within a process lifetime.
    pub fn record_fork_fragment(&self, table_name: &str, rows_added: u64) {
        if rows_added == 0 {
            return;
        }
        self.fragment_counts
            .entry(table_name.to_string())
            .and_modify(|c| *c += rows_added)
            .or_insert(rows_added);
    }

    /// Phase 5a: current accumulated row count for `table_name` on
    /// this fork. Returns 0 if the fork has never written to it.
    #[must_use]
    pub fn fragment_count(&self, table_name: &str) -> u64 {
        self.fragment_counts
            .get(table_name)
            .map(|r| *r.value())
            .unwrap_or(0)
    }

    /// Phase 5a: snapshot of every (table, count) pair recorded on
    /// this fork. Used by `IndexRebuildManager` to enumerate build
    /// candidates each polling tick.
    #[must_use]
    pub fn all_fragment_counts(&self) -> Vec<(String, u64)> {
        self.fragment_counts
            .iter()
            .map(|r| (r.key().clone(), *r.value()))
            .collect()
    }

    /// Phase 5a: register a completed fork-local index build.
    /// Called by `IndexRebuildManager` after the build lands on
    /// the fork's branch.
    pub fn register_fork_local_index(&self, label: &str, column: &str, kind: ForkLocalIndexKind) {
        self.fork_local_indexes
            .entry((label.to_string(), column.to_string()))
            .or_default()
            .insert(kind);
    }

    /// Phase 5a: whether a fork-local index of `kind` has been built
    /// for a `(label, column)` pair. Distinct kinds coexist on one
    /// column, so the planner asks for the specific kind it wants to
    /// fuse rather than a single stored value. Returns `false` when
    /// the planner should fall back to the inherited primary index
    /// (or to a plain scan).
    #[must_use]
    pub fn has_fork_local_index(
        &self,
        label: &str,
        column: &str,
        kind: ForkLocalIndexKind,
    ) -> bool {
        self.fork_local_indexes
            .get(&(label.to_string(), column.to_string()))
            .is_some_and(|r| r.value().contains(&kind))
    }

    /// Phase 5a: snapshot of every registered fork-local index, one
    /// `(label, column)` → `kind` tuple per built kind.
    #[must_use]
    pub fn all_fork_local_indexes(&self) -> Vec<((String, String), ForkLocalIndexKind)> {
        self.fork_local_indexes
            .iter()
            .flat_map(|r| {
                let key = r.key().clone();
                r.value()
                    .iter()
                    .map(move |kind| (key.clone(), *kind))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Stable fork identifier.
    #[must_use]
    pub fn fork_id(&self) -> ForkId {
        self.fork_id
    }

    /// Fork registry record (cheap `Arc::clone`).
    #[must_use]
    pub fn fork_info(&self) -> Arc<ForkInfo> {
        self.fork_info.clone()
    }

    /// Parent fork id (Phase 3). `None` ⇒ parent is primary.
    ///
    /// Used by `UniInner::at_fork` to walk the ancestor chain for
    /// overlay composition, and by `BranchedBackend` to route
    /// on-the-fly dataset creation through the parent's branch.
    #[must_use]
    pub fn parent_fork_id(&self) -> Option<ForkId> {
        self.fork_info.parent_fork_id
    }

    /// Schema delta to merge on top of primary's schema. Returns a
    /// snapshot of the current overlay; subsequent
    /// [`Self::add_label_to_overlay`] calls will not affect the
    /// returned `Arc`.
    #[must_use]
    pub fn overlay(&self) -> Arc<SchemaDelta> {
        self.overlay.load_full()
    }

    /// Branch name for a given Lance dataset, if this fork has one.
    ///
    /// Used by `StorageManager` dataset factories to route reads.
    /// Consults both the immutable fork-point datasets map (set by
    /// `finish_create`) and the dynamic-branches map (populated by
    /// [`Self::register_dynamic_branch`] when a flush hits a dataset
    /// that wasn't branched at fork-point). Returns `None` only if no
    /// branch exists on either side — the BranchedBackend then either
    /// creates one on the fly or surfaces an error.
    #[must_use]
    pub fn branch_for(&self, dataset_name: &str) -> Option<String> {
        if let Some(b) = self.fork_info.datasets.get(dataset_name) {
            return Some(b.clone());
        }
        self.dynamic_branches
            .get(dataset_name)
            .map(|r| r.value().clone())
    }

    /// Record a branch created after fork-point (e.g. for a dataset
    /// that didn't exist on primary at fork creation, or for
    /// compaction-only adjacency tables).
    ///
    /// In-memory only; the caller is responsible for persisting via
    /// [`ForkRegistryHandle::register_dataset_branch`] so a restart
    /// recovers the same mapping. Idempotent — re-registering an
    /// existing entry is a no-op.
    pub fn register_dynamic_branch(&self, dataset: String, branch: String) {
        self.dynamic_branches.insert(dataset, branch);
    }

    /// Append a label to the fork-local schema overlay and persist
    /// the new overlay to disk.
    ///
    /// Idempotent: if a label with the same name is already in the
    /// overlay (or in primary's schema, accessible to the caller via
    /// the merged `SchemaManager` not consulted here), the append
    /// still records this entry — callers should check for duplicates
    /// before invoking. The persistence-then-swap order means a
    /// failed PUT leaves the in-memory `ArcSwap` untouched and the
    /// returned error surfaces to the caller.
    ///
    /// Concurrency: serialized within a single fork by `overlay_lock`
    /// so two concurrent appends don't clobber each other's
    /// persisted state.
    pub async fn add_label_to_overlay(&self, name: String, meta: LabelMeta) -> anyhow::Result<()> {
        let _guard = self.overlay_lock.lock().await;
        let mut next = (**self.overlay.load()).clone();
        next.added_labels.push((name, meta));
        self.registry
            .update_schema_overlay(&self.fork_id, &next)
            .await
            .with_context(|| format!("persist schema overlay for fork {}", self.fork_id))?;
        self.overlay.store(Arc::new(next));
        Ok(())
    }

    /// Append an edge type to the fork-local schema overlay and
    /// persist. Same semantics as [`Self::add_label_to_overlay`].
    pub async fn add_edge_type_to_overlay(
        &self,
        name: String,
        meta: EdgeTypeMeta,
    ) -> anyhow::Result<()> {
        let _guard = self.overlay_lock.lock().await;
        let mut next = (**self.overlay.load()).clone();
        next.added_edge_types.push((name, meta));
        self.registry
            .update_schema_overlay(&self.fork_id, &next)
            .await
            .with_context(|| format!("persist schema overlay for fork {}", self.fork_id))?;
        self.overlay.store(Arc::new(next));
        Ok(())
    }

    /// Record schemaless edge-type names in the overlay and persist it.
    ///
    /// A fork interns a schemaless edge type in its merged schema only, which
    /// is never saved; this is how the name outlives the session, so a
    /// reopened fork can resolve its flushed `deltas_{name}_*` data. Names
    /// already recorded are skipped, and nothing is written when none are
    /// new. Returns whether the overlay changed.
    ///
    /// The caller may pass every name its merged schema knows, including the
    /// ones inherited from primary: recording those too is harmless, since
    /// [`uni_common::core::schema::SchemaManager::with_overlay`] interns a
    /// name primary already has as a no-op.
    ///
    /// # Errors
    /// Returns the registry PUT failure; the in-memory overlay is then left
    /// unchanged, so a later call retries.
    pub async fn record_schemaless_edge_types<I>(&self, names: I) -> anyhow::Result<bool>
    where
        I: IntoIterator<Item = String>,
    {
        let fresh: Vec<String> = {
            let current = self.overlay.load();
            names
                .into_iter()
                .filter(|n| !current.added_schemaless_edge_types.contains(n))
                .collect()
        };
        if fresh.is_empty() {
            return Ok(false);
        }
        let _guard = self.overlay_lock.lock().await;
        let mut next = (**self.overlay.load()).clone();
        let before = next.added_schemaless_edge_types.len();
        for name in fresh {
            if !next.added_schemaless_edge_types.contains(&name) {
                next.added_schemaless_edge_types.push(name);
            }
        }
        if next.added_schemaless_edge_types.len() == before {
            return Ok(false);
        }
        self.registry
            .update_schema_overlay(&self.fork_id, &next)
            .await
            .with_context(|| format!("persist schema overlay for fork {}", self.fork_id))?;
        self.overlay.store(Arc::new(next));
        Ok(true)
    }

    /// Registry handle (used by admin paths to e.g. compute holder counts).
    #[must_use]
    pub fn registry(&self) -> Arc<ForkRegistryHandle> {
        self.registry.clone()
    }
}
