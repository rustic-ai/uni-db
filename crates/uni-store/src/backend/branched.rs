// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork-aware backend wrapper.
//!
//! When `StorageManager::at_fork` is called, the resulting fork-scoped
//! manager swaps its `backend` to a [`BranchedBackend`] that wraps the
//! primary backend plus the fork's scope. Every read passes through
//! [`BranchedBackend`], which auto-fills `ScanRequest.branch` from the
//! scope's dataset → branch map. Writes are forbidden in Phase 1 and
//! return [`anyhow::Error`] surfaced as `UniError::ForkWritesNotYetSupported`
//! by the API gate above this layer.

// Rust guideline compliant

use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;

use super::branching::ForkBranching;
use super::traits::{RecordBatchStream, StorageBackend};
use super::types::*;
use crate::fork::ForkScope;

/// Backend decorator that routes reads through a fork's branches.
///
/// Owns an `Arc<dyn StorageBackend>` to the primary backend plus an
/// `Arc<ForkScope>` for branch lookups. Cloning is cheap (Arc-only).
pub struct BranchedBackend {
    inner: Arc<dyn StorageBackend>,
    scope: Arc<ForkScope>,
    /// The inner backend's branching capability, captured at construction.
    ///
    /// `None` when the backend has no copy-on-write branching, in which case
    /// every branch operation fails loudly via [`Self::branching`] rather than
    /// silently reading or writing primary — a fork whose isolation quietly
    /// evaporated is far worse than one that refuses to start.
    branching: Option<Arc<dyn ForkBranching>>,
}

impl BranchedBackend {
    /// Wrap `inner` so reads route through `scope`'s branches.
    #[must_use]
    pub fn new(inner: Arc<dyn StorageBackend>, scope: Arc<ForkScope>) -> Self {
        let branching = inner.branching();
        Self {
            inner,
            scope,
            branching,
        }
    }

    /// The branching capability, or a clear error if the backend lacks one.
    fn branching(&self) -> Result<&dyn ForkBranching> {
        self.branching.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "storage backend does not support fork branching; \
                 forks require a backend implementing ForkBranching"
            )
        })
    }

    /// Borrow the wrapped primary backend.
    ///
    /// Used by Day 4's fork-scoped `Writer` construction: the Writer
    /// needs an `Arc<dyn StorageBackend>` and on a fork that's *this*
    /// backend, but Writer-internal helpers that reach for the
    /// underlying lancedb path (e.g. `connection.create_table`) must
    /// route through the inner backend instead. This accessor makes
    /// the choice explicit.
    #[must_use]
    pub fn inner_backend(&self) -> Arc<dyn StorageBackend> {
        self.inner.clone()
    }

    /// Borrow the active fork scope.
    #[must_use]
    pub fn scope(&self) -> Arc<ForkScope> {
        self.scope.clone()
    }

    /// Apply the fork's branch to a `ScanRequest` if the table has one
    /// recorded in the scope and the request hasn't already set a branch.
    fn apply_branch(&self, mut request: ScanRequest) -> ScanRequest {
        if request.branch.is_none()
            && let Some(branch) = self.scope.branch_for(&request.table_name)
        {
            request.branch = Some(branch);
        }
        request
    }

    /// Phase 2 Day 10: ensure a branch exists for `table_name` on the
    /// fork, creating one on-the-fly when the dataset already lives on
    /// primary but wasn't branched at fork-point. Returns the branch
    /// name to write to.
    ///
    /// Errors when `table_name` doesn't exist on primary either —
    /// the caller (typically a write or a delete) needs a
    /// schema-bearing path (`create_table` / `create_empty_table`) to
    /// materialize the dataset.
    async fn ensure_branch_for_existing(&self, table_name: &str) -> Result<String> {
        if let Some(b) = self.scope.branch_for(table_name) {
            return Ok(b);
        }
        let branching = self.branching()?;
        if !branching.table_exists(table_name).await? {
            anyhow::bail!(
                "ensure_branch_for_existing('{table_name}'): dataset not on \
                 primary either; use create_table/create_empty_table"
            );
        }
        let parent_v = branching.current_version(table_name).await?;
        let branch_name = format!("fork_{}_{}", self.scope.fork_id(), table_name);
        branching
            .create_branch(table_name, &branch_name, parent_v)
            .await?;
        // Persist + record. Persistence first so a crash between the
        // Lance commit and the in-memory register leaves the on-disk
        // record consistent with what reads will resolve.
        self.scope
            .registry()
            .register_dataset_branch(self.scope.fork_id(), table_name, &branch_name)
            .await
            .map_err(|e| anyhow::anyhow!("persist dynamic branch: {e}"))?;
        self.scope
            .register_dynamic_branch(table_name.to_string(), branch_name.clone());
        Ok(branch_name)
    }

    /// Phase 2 Day 10: ensure a branch exists, creating both the
    /// dataset *and* the branch on the fork when neither exists on
    /// primary. Used by `create_table` / `create_empty_table` /
    /// `open_or_create_table`. The dataset is created with `schema`
    /// and (optionally) seeded with `initial_batches`.
    async fn ensure_branch_for_new(
        &self,
        table_name: &str,
        schema: Arc<ArrowSchema>,
        initial_batches: Vec<RecordBatch>,
    ) -> Result<String> {
        if let Some(b) = self.scope.branch_for(table_name) {
            return Ok(b);
        }
        let branching = self.branching()?;
        let branch_name = format!("fork_{}_{}", self.scope.fork_id(), table_name);
        if branching.table_exists(table_name).await? {
            // Dataset exists on primary but no branch yet — branch from
            // the current parent version. Treat the supplied batches
            // (if any) as the first writes on the new branch.
            let parent_v = branching.current_version(table_name).await?;
            branching
                .create_branch(table_name, &branch_name, parent_v)
                .await?;
            if !initial_batches.is_empty() {
                let arrow_schema = initial_batches[0].schema();
                branching
                    .write_to_branch(table_name, &branch_name, initial_batches, arrow_schema)
                    .await?;
            }
        } else {
            // Brand-new dataset — materialize an *empty* parent on
            // main first, branch from it, then write the real batches
            // to the branch. The two-step is critical for fork
            // isolation: writing the batches to main first (the
            // shape `create_dataset_then_branch` does) would leak the
            // fork's data into primary's view of the dataset, since
            // primary's reads always resolve through main.
            //
            // Phase 3 (nested forks): branching off main here is
            // correct even when this scope is a nested fork. By
            // construction `ensure_branch_for_new` only runs when
            // `scope.branch_for(table_name)` returned None, which for
            // a nested child means no ancestor in the chain had a
            // branch for this dataset at the child's creation time.
            // An ancestor's state for a never-touched dataset is empty,
            // so chaining through main vs. through an ancestor's
            // (nonexistent) branch produces the same reads. Primary
            // still cannot see the data because its schema doesn't
            // list the fork-only label — its reads never open this
            // dataset.
            branching
                .create_empty_table_then_branch(table_name, &branch_name, schema.clone())
                .await?;
            if !initial_batches.is_empty() {
                let arrow_schema = initial_batches[0].schema();
                branching
                    .write_to_branch(table_name, &branch_name, initial_batches, arrow_schema)
                    .await?;
            }
        }
        self.scope
            .registry()
            .register_dataset_branch(self.scope.fork_id(), table_name, &branch_name)
            .await
            .map_err(|e| anyhow::anyhow!("persist dynamic branch: {e}"))?;
        self.scope
            .register_dynamic_branch(table_name.to_string(), branch_name.clone());
        // The dataset for `table_name` now exists on disk (either we
        // just materialized it on main + branched off, or it already
        // existed). The inner backend's existence_cache (issue #55)
        // may be holding a stale `false` from a pre-creation read;
        // notify it so subsequent table_exists calls return true.
        self.inner.notify_table_created(table_name).await;
        Ok(branch_name)
    }
}

#[async_trait]
impl StorageBackend for BranchedBackend {
    // ── Reads — branch-aware ─────────────────────────────────────────

    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>> {
        self.inner.scan(self.apply_branch(request)).await
    }

    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream> {
        self.inner.scan_stream(self.apply_branch(request)).await
    }

    async fn count_rows(&self, table_name: &str, filter: Option<&FilterExpr>) -> Result<usize> {
        // Primary path counts via `Table::count_rows`. Branched count
        // delegates by scanning the branch and summing row counts; the
        // upstream lancedb 0.27.1 doesn't expose branch-aware count.
        if let Some(_branch) = self.scope.branch_for(table_name) {
            let mut request = ScanRequest::all(table_name).with_branch(_branch);
            if let Some(f) = filter {
                request = request.with_filter(f.clone());
            }
            let batches = self.inner.scan(request).await?;
            Ok(batches.iter().map(|b| b.num_rows()).sum())
        } else {
            self.inner.count_rows(table_name, filter).await
        }
    }

    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>> {
        // Schema is identical across branches — the schema is captured
        // at fork creation and overlays only add new columns. Delegate
        // to the primary backend.
        self.inner.get_table_schema(name).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn vector_search(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        metric: DistanceMetric,
        filter: FilterExpr,
        opts: VectorQueryOpts,
        counters: Option<std::sync::Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        // Phase 5b: when the fork has a branch for this dataset,
        // route through Lance's per-branch nearest-K — its
        // `base_paths` chain on the branch surfaces both fork-local
        // and parent-inherited rows in one scan, naturally fused.
        // When no branch exists (label never written through the
        // fork), delegate to primary's vector_search.
        //
        // The `metric` parameter is honored implicitly: Lance picks the
        // metric from the index built on the column. The `filter` (M6) is
        // threaded into the branch scan so the user predicate, the
        // `_deleted = false` guard, and the version HWM pin are all
        // honored — matching the non-branch path's semantics.
        if let Some(branch) = self.scope.branch_for(table) {
            return self
                .branching()?
                .vector_search_on_branch(table, &branch, column, query, k, &filter, opts)
                .await;
        }
        self.inner
            .vector_search(table, column, query, k, metric, filter, opts, counters)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn multivector_search(
        &self,
        table: &str,
        column: &str,
        query: &[Vec<f32>],
        k: usize,
        metric: DistanceMetric,
        filter: FilterExpr,
        opts: VectorQueryOpts,
        counters: Option<std::sync::Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        // Multi-vector retrieval over a forked/branched dataset has no Lance ANN
        // path — there is no per-branch multi-vector `nearest` (and lancedb
        // cannot open a `Table` on a non-main branch). It is instead handled one
        // layer up: `StorageManager::multivector_search` intercepts branched
        // tables, enumerates the branch's candidate vids via a branch-aware scan,
        // and the uni-query `multivector_rerank` helper re-scores by exact MaxSim
        // over disk + fork L0. So this method is never reached for a branched
        // table; the guard is defense-in-depth for any direct backend caller.
        if self.scope.branch_for(table).is_some() {
            anyhow::bail!(
                "multi-vector search on branches must go through \
                 StorageManager::multivector_search (branch scan + MaxSim re-rank)"
            );
        }
        self.inner
            .multivector_search(table, column, query, k, metric, filter, opts, counters)
            .await
    }

    async fn full_text_search(
        &self,
        table: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: FilterExpr,
        counters: Option<std::sync::Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        // Phase 5b: same per-branch routing as vector_search. Lance's
        // FTS query on a branch surfaces fork-local + parent-inherited
        // rows via `base_paths`. The `filter` (M6) is threaded through so
        // the predicate, `_deleted = false`, and the HWM pin are honored.
        if let Some(branch) = self.scope.branch_for(table) {
            return self
                .branching()?
                .full_text_search_on_branch(table, &branch, column, query, k, &filter)
                .await;
        }
        self.inner
            .full_text_search(table, column, query, k, filter, counters)
            .await
    }

    // ── Lifecycle / writes — Phase 2 routes to the fork's branches ──
    //
    // Phase 1 was bail-on-every-write. Phase 2 routes through
    // `crate::backend::lance_branch` helpers when the fork has a branch
    // for the named table; falls back to a `ForkLifecycle` error when
    // it doesn't (Phase 2 Day 10 lifts this with on-the-fly branch
    // creation for new labels).

    async fn table_names(&self) -> Result<Vec<String>> {
        self.inner.table_names().await
    }

    async fn table_exists(&self, name: &str) -> Result<bool> {
        self.inner.table_exists(name).await
    }

    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            anyhow::bail!(
                "create_table('{name}') on a forked backend requires at least \
                 one batch to derive the schema; use create_empty_table"
            );
        }
        let schema = batches[0].schema();
        // Phase 5a: tally rows for the fork's fragment counter.
        let rows_added: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        self.ensure_branch_for_new(name, schema, batches).await?;
        self.scope.record_fork_fragment(name, rows_added);
        Ok(())
    }

    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        self.ensure_branch_for_new(name, schema, Vec::new()).await?;
        Ok(())
    }

    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        // Idempotent: if the fork has a branch for this table the
        // dataset already exists on disk, so we're done. Otherwise
        // create dataset+branch (or branch from primary if the
        // dataset already exists on primary) so subsequent writes
        // through the fork's branched backend resolve correctly.
        if self.scope.branch_for(name).is_some() {
            return Ok(());
        }
        self.ensure_branch_for_new(name, schema, Vec::new()).await?;
        Ok(())
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        // Forks do not drop primary tables. The right Phase 6 verb for
        // fork-side drop is `db.drop_fork(...)`; per-table drop on a
        // fork has no spec story.
        anyhow::bail!(
            "drop_table('{name}') on a forked backend is not supported; \
             use db.drop_fork(...) to remove a fork in its entirety"
        )
    }

    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        // Phase 5a: tally rows up-front so we can bump the fork's
        // fragment counter after a successful write. Computed here
        // (not after the write) because batches are consumed by the
        // RecordBatchIterator below.
        let rows_added: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        // Try to ensure a branch from an existing primary dataset; if
        // primary doesn't have it either, materialize dataset+branch
        // on the fork using the supplied batches as the seed.
        let arrow_schema = batches[0].schema();
        let branch = match self.ensure_branch_for_existing(table_name).await {
            Ok(b) => b,
            Err(_) => {
                // Dataset doesn't exist on primary either — create it
                // on the fork via `ensure_branch_for_new`, seeded with
                // the batches. The branch returned then receives any
                // remaining append/overwrite semantics below.
                let _b = self
                    .ensure_branch_for_new(table_name, arrow_schema.clone(), batches.clone())
                    .await?;
                // ensure_branch_for_new already wrote the batches to the
                // branch; nothing more to do for Append. For Overwrite, the
                // batches *are* the only content, which matches.
                self.scope.record_fork_fragment(table_name, rows_added);
                return Ok(());
            }
        };
        let branching = self.branching()?;
        match mode {
            WriteMode::Append => {
                branching
                    .write_to_branch(table_name, &branch, batches, arrow_schema)
                    .await?;
            }
            WriteMode::Overwrite => {
                branching
                    .replace_branch_tip(table_name, &branch, batches, arrow_schema)
                    .await?;
            }
        }
        self.scope.record_fork_fragment(table_name, rows_added);
        Ok(())
    }

    async fn delete_rows(&self, table_name: &str, filter: &FilterExpr) -> Result<()> {
        let branch = self.ensure_branch_for_existing(table_name).await?;
        self.branching()?
            .delete_from_branch(table_name, &branch, filter)
            .await
    }

    async fn merge_insert(
        &self,
        table_name: &str,
        on: &[&str],
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        // Merge-insert is update-only, so it targets rows that already
        // exist in the dataset (on the fork branch or inherited via
        // base_paths). The dataset therefore exists on primary; route to
        // the fork's branch exactly like `delete_rows`. This is what lets a
        // fork flush a soft-delete or partial-column edit of an INHERITED
        // vertex — without it the flush bailed "merge_insert not supported".
        let branch = self.ensure_branch_for_existing(table_name).await?;
        let schema = batches[0].schema();
        self.branching()?
            .merge_insert_on_branch(table_name, &branch, on, batches, schema)
            .await
    }

    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()> {
        // On a fork, "replace the table atomically" means "replace the
        // branch's tip" — Lance commits a delete-all then an append.
        // Two manifest commits, not one; primary's main branch is
        // untouched. Spec contract differs from primary semantics, so
        // callers should be aware (commented at Phase 2 Decision D3).
        // If no branch exists, ensure one — branch from primary when
        // possible, otherwise create dataset+branch with the supplied
        // schema.
        let branch = match self.ensure_branch_for_existing(name).await {
            Ok(b) => b,
            Err(_) => {
                self.ensure_branch_for_new(name, schema.clone(), Vec::new())
                    .await?
            }
        };
        // An empty `batches` clears the table, but carries no schema of its
        // own — fall back to the caller's.
        let arrow_schema = if batches.is_empty() {
            schema
        } else {
            batches[0].schema()
        };
        self.branching()?
            .replace_branch_tip(name, &branch, batches, arrow_schema)
            .await
    }

    async fn lock_table_for_write(&self, name: &str) -> crate::backend::traits::TableWriteGuard {
        // Forward to the primary backend so a backfill and a flush that pass the same
        // table name serialize on the same per-table mutex.
        self.inner.lock_table_for_write(name).await
    }

    // ── MVCC ─────────────────────────────────────────────────────────

    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>> {
        self.inner.get_table_version(table_name).await
    }

    async fn rollback_table(&self, _table_name: &str, _target_version: u64) -> Result<()> {
        anyhow::bail!("rollback_table on a forked backend is not supported in Phase 1")
    }

    // ── Maintenance ──────────────────────────────────────────────────

    async fn optimize_table(
        &self,
        table_name: &str,
        version_retention: std::time::Duration,
    ) -> Result<OptimizeReport> {
        // Compaction on a fork is a Phase 5 concern. Phase 1 silently
        // delegates to the primary backend; for a fork-only table this
        // is a no-op because the fork has no L1 fragments yet.
        //
        // The report therefore describes the *primary's* table, not the
        // branch's, and a fork-only table yields an all-zero report.
        self.inner
            .optimize_table(table_name, version_retention)
            .await
    }

    async fn recover_staging(&self, table_name: &str) -> Result<()> {
        self.inner.recover_staging(table_name).await
    }

    // ── Cache passthrough ────────────────────────────────────────────

    fn invalidate_cache(&self, table_name: &str) {
        self.inner.invalidate_cache(table_name);
    }

    fn clear_cache(&self) {
        self.inner.clear_cache();
    }

    fn base_uri(&self) -> &str {
        self.inner.base_uri()
    }

    // ── Capability flags — same as inner ────────────────────────────

    fn supports_vector_search(&self) -> bool {
        self.inner.supports_vector_search()
    }

    fn supports_full_text_search(&self) -> bool {
        self.inner.supports_full_text_search()
    }

    fn supports_scalar_index(&self) -> bool {
        self.inner.supports_scalar_index()
    }

    /// Forward the inner backend's branching capability.
    ///
    /// Without this the decorator would fall through to the trait default
    /// (`None`), so a *forked* session — the only kind that wraps a
    /// `BranchedBackend` — would report no branching support and every
    /// fork-local index build would fail.
    fn branching(&self) -> Option<Arc<dyn ForkBranching>> {
        self.branching.clone()
    }

    // ── Index management — Phase 5 will revisit ─────────────────────

    // Index creation on a fork is built directly on the branch via
    // `fork/index_builder.rs` → `lance_branch::*_on_branch`, never through these
    // trait methods (the primary `IndexManager` only ever builds on the main
    // backend). They bail so an accidental fork-scoped call surfaces loudly.
    async fn create_vector_index(
        &self,
        _table: &str,
        _column: &str,
        _name: &str,
        _params: VectorIndexParams,
    ) -> Result<()> {
        anyhow::bail!("create_vector_index on a forked backend is not supported")
    }

    async fn create_fts_index(
        &self,
        _table: &str,
        _columns: &[&str],
        _name: Option<&str>,
        _tokenizer: &uni_common::core::schema::TokenizerConfig,
        _with_positions: bool,
    ) -> Result<()> {
        anyhow::bail!("create_fts_index on a forked backend is not supported")
    }

    async fn create_scalar_index(
        &self,
        _table: &str,
        _columns: &[&str],
        _index_type: ScalarIndexType,
        _name: Option<&str>,
    ) -> Result<()> {
        anyhow::bail!("create_scalar_index on a forked backend is not supported")
    }

    async fn drop_index(&self, _table: &str, _index_name: &str) -> Result<()> {
        anyhow::bail!("drop_index on a forked backend is not supported")
    }

    async fn list_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        self.inner.list_indexes(table).await
    }
}
