// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Core [`StorageBackend`] trait definition.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use futures::Stream;
use uni_common::core::schema::TokenizerConfig;

use super::branching::ForkBranching;
use super::types::*;

/// A record batch stream returned by [`StorageBackend::scan_stream`].
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// RAII guard serializing writes to a single table, from
/// [`StorageBackend::lock_table_for_write`].
///
/// Held across a multi-step read-modify-write (e.g. an index backfill's
/// scan → transform → [`StorageBackend::replace_table_atomic`]) so a concurrent
/// [`StorageBackend::write`] append cannot interleave between the read and the
/// overwrite and be silently discarded. A no-op for backends without per-table
/// write locking.
#[must_use = "the table write lock is released as soon as the guard is dropped"]
pub struct TableWriteGuard(
    // Held purely for its `Drop` side effect (releasing the per-table mutex).
    #[expect(dead_code, reason = "guard is held only to release the lock on drop")]
    Option<tokio::sync::OwnedMutexGuard<()>>,
);

impl std::fmt::Debug for TableWriteGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableWriteGuard").finish_non_exhaustive()
    }
}

impl TableWriteGuard {
    /// A no-op guard for backends that do not serialize writes per table.
    pub fn none() -> Self {
        Self(None)
    }

    /// Wrap an owned per-table mutex guard.
    pub fn held(guard: tokio::sync::OwnedMutexGuard<()>) -> Self {
        Self(Some(guard))
    }
}

/// Core storage backend trait.
///
/// All persistent storage operations go through this trait. Backends must be
/// thread-safe ([`Send`] + [`Sync`]) and have a static lifetime for use with
/// `Arc<dyn StorageBackend>`.
///
/// # Design Principles
///
/// - **Arrow-native**: All data interchange uses Arrow [`RecordBatch`].
/// - **SQL-string filters**: Filter expressions use SQL-like strings initially.
///   Backends that don't support SQL must parse/translate these strings.
/// - **Capabilities via default methods**: Optional features (vector search, FTS)
///   have default implementations that return "not supported" errors.
/// - **Table-level operations**: The backend manages individual tables (not the
///   higher-level graph schema). Table naming conventions are in [`super::table_names`].
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    // ========================
    // Table Lifecycle
    // ========================

    /// List all table names in the backend.
    async fn table_names(&self) -> Result<Vec<String>>;

    /// Check if a table exists.
    async fn table_exists(&self, name: &str) -> Result<bool>;

    /// Create a new table with initial data batches.
    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()>;

    /// Create a new empty table with the given schema.
    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()>;

    /// Open a table if it exists, or create it with the given schema.
    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()>;

    /// Drop a table by name.
    async fn drop_table(&self, name: &str) -> Result<()>;

    /// Notify the backend that a table now exists, even though no
    /// `create_table` / `create_empty_table` / `open_or_create_table`
    /// went through this trait. The default implementation is a
    /// no-op; backends that cache existence (e.g. `LanceDbBackend`'s
    /// `existence_cache` from issue #55) override to invalidate the
    /// stale negative entry. Used by `BranchedBackend` after it
    /// creates a fork-side dataset directly through the Lance branch
    /// primitives — that path does not call `create_table` on the
    /// inner backend, so without this hook the inner backend's
    /// existence cache silently keeps reporting `false` for the
    /// just-created table.
    async fn notify_table_created(&self, name: &str) {
        let _ = name;
    }

    // ========================
    // Read Operations
    // ========================

    /// Scan a table, collecting all matching rows into batches.
    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>>;

    /// Scan a table, returning a streaming iterator over record batches.
    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream>;

    /// Get the Arrow schema for a table. Returns `None` if the table doesn't exist.
    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>>;

    /// Count rows in a table, optionally with a filter.
    async fn count_rows(&self, table_name: &str, filter: Option<&FilterExpr>) -> Result<usize>;

    // ========================
    // Write Operations
    // ========================

    /// Write record batches to a table.
    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()>;

    /// Upsert via Lance MergeInsert. Source rows are joined to the
    /// target on the columns in `on`; matched rows have `UpdateAll`
    /// applied (i.e. every column present in the source overrides the
    /// target's value for that column; columns not in the source are
    /// preserved). Unmatched source rows are DROPPED — partial writes
    /// never INSERT (CREATE goes through `write` with `WriteMode::Append`).
    ///
    /// Used by `Writer::flush_stream_l1` when
    /// `UniConfig::partial_lance_writes` is on.
    async fn merge_insert(
        &self,
        _table_name: &str,
        _on: &[&str],
        _batches: Vec<RecordBatch>,
    ) -> Result<()> {
        anyhow::bail!("merge_insert not supported by this backend")
    }

    /// Delete rows matching a filter expression.
    async fn delete_rows(&self, table_name: &str, filter: &FilterExpr) -> Result<()>;

    /// Atomically replace a table's contents.
    ///
    /// Handles the case where batches may be empty (clears the table) and the
    /// table may not exist yet (creates it).
    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()>;

    /// Acquire the per-table write lock, returning a guard held until dropped.
    ///
    /// A caller performing a read-modify-write that spans multiple backend calls —
    /// a scan followed by [`Self::replace_table_atomic`], as the MUVERA FDE backfill
    /// does — must hold this across the whole sequence. Otherwise a concurrent
    /// [`Self::write`] append can land between the read and the full-table overwrite
    /// and be silently lost. [`Self::write`] / [`Self::merge_insert`] take the same
    /// lock internally, so holding it here makes them mutually exclusive.
    ///
    /// Backends without per-table write locking return a no-op guard.
    async fn lock_table_for_write(&self, name: &str) -> TableWriteGuard {
        let _ = name;
        TableWriteGuard::none()
    }

    // ========================
    // Versioning / MVCC
    // ========================

    /// Get the current version of a table. Returns `None` if the table doesn't exist.
    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>>;

    /// Roll back a table to a specific version.
    async fn rollback_table(&self, table_name: &str, target_version: u64) -> Result<()>;

    // ========================
    // Maintenance
    // ========================

    /// Optimize a table (compaction, cleanup, etc.).
    ///
    /// Returns what the call actually did. `Ok(OptimizeReport::default())` means
    /// the table was visited and there was nothing to merge — a result, not an
    /// error, and deliberately distinguishable from `Err`, which means the
    /// optimize did not run at all.
    /// `version_retention` is how long a superseded dataset version is kept
    /// before cleanup may delete it. It is a parameter rather than backend state
    /// because it belongs to compaction policy, and because a caller must be
    /// able to drive it to zero to observe reclaimed bytes at all.
    async fn optimize_table(
        &self,
        table_name: &str,
        version_retention: std::time::Duration,
    ) -> Result<OptimizeReport>;

    /// Recover a table from crash state (incomplete staging writes, etc.).
    async fn recover_staging(&self, table_name: &str) -> Result<()>;

    // ========================
    // Cache Management
    // ========================

    /// Invalidate any cached state for a table.
    fn invalidate_cache(&self, _table_name: &str) {}

    /// Clear all cached state.
    fn clear_cache(&self) {}

    // ========================
    // Metadata
    // ========================

    /// Get the base URI for this backend's storage location.
    fn base_uri(&self) -> &str;

    /// Copy-on-write branching support, if this backend provides it.
    ///
    /// `Some` unlocks the fork engine; `None` means forks are unavailable on
    /// this backend rather than silently unisolated. Returning `None` is the
    /// default so that a new backend cannot accidentally claim fork support it
    /// has not implemented.
    fn branching(&self) -> Option<Arc<dyn ForkBranching>> {
        None
    }

    // ========================
    // Capability Checks
    // ========================

    /// Whether this backend supports vector similarity search.
    fn supports_vector_search(&self) -> bool {
        false
    }

    /// Whether this backend supports full-text search.
    fn supports_full_text_search(&self) -> bool {
        false
    }

    /// Whether this backend supports scalar indexes.
    fn supports_scalar_index(&self) -> bool {
        false
    }

    // ========================
    // Optional Capabilities
    // ========================

    /// Perform a vector similarity search.
    ///
    /// `counters`, when present, records whether the search consulted an index.
    /// This is the only channel for it: unlike the scan path there is no
    /// `ScanRequest` here to carry one.
    #[expect(clippy::too_many_arguments)]
    async fn vector_search(
        &self,
        _table: &str,
        _column: &str,
        _query: &[f32],
        _k: usize,
        _metric: DistanceMetric,
        _filter: FilterExpr,
        _opts: VectorQueryOpts,
        _counters: Option<std::sync::Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        anyhow::bail!("Vector search not supported by this backend")
    }

    /// Late-interaction (ColBERT / MaxSim) search over a multi-vector column.
    ///
    /// `query` is a set of per-token vectors; each row's `List<FixedSizeList>`
    /// column is scored by MaxSim. Defaults to unsupported.
    #[expect(clippy::too_many_arguments)]
    async fn multivector_search(
        &self,
        _table: &str,
        _column: &str,
        _query: &[Vec<f32>],
        _k: usize,
        _metric: DistanceMetric,
        _filter: FilterExpr,
        _opts: VectorQueryOpts,
        _counters: Option<std::sync::Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        anyhow::bail!("Multi-vector search not supported by this backend")
    }

    /// Perform a full-text search.
    ///
    /// See [`StorageBackend::vector_search`] for what `counters` records.
    async fn full_text_search(
        &self,
        _table: &str,
        _column: &str,
        _query: &str,
        _k: usize,
        _filter: FilterExpr,
        _counters: Option<std::sync::Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<RecordBatch>> {
        anyhow::bail!("Full-text search not supported by this backend")
    }

    /// Create a named vector (ANN) index on a column with the given parameters.
    ///
    /// `name` is the index name to assign; an existing index of the same name is
    /// replaced. `params` selects the physical index shape and metric.
    ///
    /// # Errors
    /// Returns an error if the backend does not support vector indexing or the
    /// build fails.
    async fn create_vector_index(
        &self,
        _table: &str,
        _column: &str,
        _name: &str,
        _params: VectorIndexParams,
    ) -> Result<()> {
        anyhow::bail!("Vector indexing not supported by this backend")
    }

    /// Create a full-text search index over one or more columns.
    ///
    /// `name` is the index name (`None` lets the backend choose a default).
    /// `tokenizer` selects the analyzer pipeline (tokenizer, stemming,
    /// stop words, ...). `with_positions` enables phrase/position postings
    /// (it is ignored for tokenizers that cannot store positions, e.g. N-gram).
    ///
    /// # Errors
    /// Returns an error if the backend does not support FTS, the tokenizer
    /// configuration is invalid, or the build fails.
    async fn create_fts_index(
        &self,
        _table: &str,
        _columns: &[&str],
        _name: Option<&str>,
        _tokenizer: &TokenizerConfig,
        _with_positions: bool,
    ) -> Result<()> {
        anyhow::bail!("FTS indexing not supported by this backend")
    }

    /// Create a scalar index over one or more columns.
    ///
    /// `name` is the index name (`None` lets the backend choose a default).
    ///
    /// # Errors
    /// Returns an error if the backend does not support scalar indexing or the
    /// build fails.
    async fn create_scalar_index(
        &self,
        _table: &str,
        _columns: &[&str],
        _index_type: ScalarIndexType,
        _name: Option<&str>,
    ) -> Result<()> {
        anyhow::bail!("Scalar indexing not supported by this backend")
    }

    /// Drop an index by name.
    async fn drop_index(&self, _table: &str, _index_name: &str) -> Result<()> {
        anyhow::bail!("Index drop not supported by this backend")
    }

    /// List all indexes on a table.
    async fn list_indexes(&self, _table: &str) -> Result<Vec<IndexInfo>> {
        Ok(vec![])
    }
}
