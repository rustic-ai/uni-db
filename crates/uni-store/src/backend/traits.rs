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

/// Rows written to a table's storage but not yet part of any version of it.
///
/// Returned by [`StorageBackend::stage_append`]. A reader of the table cannot
/// see these rows until [`StorageBackend::publish_staged`] commits them, and
/// [`StorageBackend::discard_staged`] drops them without them ever having been
/// visible. This is what lets the bulk loader keep an in-progress load out of
/// every reader, every compaction pass and every in-memory cache: there is
/// nothing to read, so there is nothing to undo.
///
/// A backend that cannot stage returns a value whose rows are *already*
/// visible (see [`Self::is_pending`]); callers that need isolation check
/// [`StorageBackend::stages_invisibly`] before relying on it.
#[must_use = "staged rows stay invisible until `publish_staged` commits them"]
pub struct StagedAppend {
    rows: usize,
    // Backend-private description of the written files; `None` when the
    // backend wrote the rows visibly instead of staging them.
    payload: Option<Box<dyn std::any::Any + Send + Sync>>,
}

impl StagedAppend {
    /// Rows that a backend wrote visibly because it cannot stage.
    pub fn visible(rows: usize) -> Self {
        Self {
            rows,
            payload: None,
        }
    }

    /// Rows staged invisibly, described by a backend-private `payload`.
    pub fn pending(rows: usize, payload: Box<dyn std::any::Any + Send + Sync>) -> Self {
        Self {
            rows,
            payload: Some(payload),
        }
    }

    /// Number of rows in this append.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Whether the rows still await [`StorageBackend::publish_staged`].
    #[must_use]
    pub fn is_pending(&self) -> bool {
        self.payload.is_some()
    }

    /// The backend-private payload, `None` for a visible append.
    pub fn into_payload(self) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        self.payload
    }

    /// The backend-private payload by reference, `None` for a visible append.
    #[must_use]
    pub fn payload(&self) -> Option<&(dyn std::any::Any + Send + Sync)> {
        self.payload.as_deref()
    }
}

/// Keeps a table's staged files from being reclaimed while it is held.
///
/// Returned by [`StorageBackend::hold_staged_files`]. Staged files are
/// referenced by no table version until published, and a backend's version
/// cleanup may treat an unreferenced file past some age as abandoned and delete
/// it (Lance: 7 days). A bulk load holds the tables it stages into until every
/// staged file is published -- or, if the load is committed but its publish
/// could not be finished, until the process exits (see
/// [`Self::keep_until_exit`]), since the next open completes it from those
/// files. Dropping the hold releases it.
#[must_use = "dropping the hold releases it immediately"]
pub struct StagedFilesHold(Option<Box<dyn std::any::Any + Send + Sync>>);

impl StagedFilesHold {
    /// A hold that protects nothing, for a backend whose cleanup never deletes
    /// unreferenced files.
    pub fn none() -> Self {
        Self(None)
    }

    /// A hold released when `guard` is dropped.
    pub fn new(guard: Box<dyn std::any::Any + Send + Sync>) -> Self {
        Self(Some(guard))
    }

    /// Keep holding for the rest of the process.
    ///
    /// For staged files a committed load still needs at the next open: the
    /// files must survive any cleanup this process runs before then. The hold
    /// is leaked deliberately; it is a few bytes, once per incomplete load.
    pub fn keep_until_exit(self) {
        std::mem::forget(self);
    }
}

impl std::fmt::Debug for StagedFilesHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StagedFilesHold")
            .field(&self.0.is_some())
            .finish()
    }
}

/// What [`StorageBackend::publish_staged_record`] found and did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedPublish {
    /// The recorded rows were not yet in the table; they are now.
    Published,
    /// The recorded rows were already published by an earlier attempt, so
    /// nothing was committed.
    AlreadyPublished,
}

impl std::fmt::Debug for StagedAppend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedAppend")
            .field("rows", &self.rows)
            .field("pending", &self.is_pending())
            .finish()
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

    /// Which rows of an in-memory `batch` satisfy `filter`, judged as this
    /// backend's scans judge them: a row whose result is false or NULL fails.
    ///
    /// A filter handed to a scan sometimes also has to be applied to rows not
    /// yet in storage, such as the unflushed rows a search merges in. Judging
    /// them by the scan's own rules keeps the two halves of one answer
    /// consistent. The default uses [`FilterExpr::eval`], which refuses a
    /// [`FilterExpr::Raw`]; a backend whose scans accept raw SQL overrides it.
    ///
    /// # Errors
    /// Returns an error if the filter cannot be evaluated against `batch`.
    fn filter_batch(&self, batch: &RecordBatch, filter: &FilterExpr) -> Result<Vec<bool>> {
        (0..batch.num_rows())
            .map(|row| Ok(filter.eval(&super::eval::ArrowRow::new(batch, row))? == Some(true)))
            .collect()
    }

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

    /// Widen `table`'s stored schema so it accepts a batch it currently rejects.
    ///
    /// `add` names columns to create, all-null, on every existing row;
    /// `relax_nullable` names existing columns whose `NOT NULL` must become
    /// nullable. Both are **widening-only** operations: nothing is dropped,
    /// narrowed, renamed, or cast, so no row is ever rewritten and no value
    /// changes.
    ///
    /// This exists because uni builds a flush batch's Arrow schema from the
    /// *declared catalog properties* (`VertexDataset::get_arrow_schema`) and
    /// never consults the dataset. Declaring a property on a label that
    /// already has flushed data therefore produced a batch carrying a column
    /// the dataset had never had, which Lance rejects — wedging that label
    /// permanently (issue #249). Dropping a `NOT NULL` property is the mirror
    /// case, since Lance only tolerates a column missing from the batch when
    /// the *stored* field is nullable.
    ///
    /// Implementations must:
    /// - treat an absent table as a no-op (uni materializes tables lazily, so
    ///   a declared-but-never-flushed label has no dataset yet);
    /// - be idempotent — re-widening an already-wide table succeeds;
    /// - serialize against concurrent writes to the same table, and retry a
    ///   commit conflict rather than surfacing it.
    ///
    /// # Errors
    /// Returns an error if a name in `add` already exists with a different
    /// type, or if the backend cannot evolve schemas.
    async fn evolve_table_schema(
        &self,
        table_name: &str,
        add: &[arrow_schema::Field],
        relax_nullable: &[String],
    ) -> Result<()> {
        let _ = (table_name, add, relax_nullable);
        anyhow::bail!("schema evolution is not supported by this backend")
    }

    /// Whether [`Self::stage_append`] keeps rows invisible until published.
    ///
    /// `false` (the default) means `stage_append` writes visibly, so a caller
    /// that must not expose rows early has to fall back to a protocol that can
    /// undo a visible write.
    fn stages_invisibly(&self) -> bool {
        false
    }

    /// Write `batches` into `table_name`'s storage without publishing them.
    ///
    /// The table must already exist. Readers, compaction and every other
    /// writer go on seeing the table exactly as before until
    /// [`Self::publish_staged`] commits the result. The default writes the
    /// rows visibly and says so through [`StagedAppend::is_pending`].
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist or the write fails.
    async fn stage_append(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<StagedAppend> {
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        self.write(table_name, batches, WriteMode::Append).await?;
        Ok(StagedAppend::visible(rows))
    }

    /// Make every append in `staged` visible in `table_name`, as one commit.
    ///
    /// Publishing appends onto the table's *current* version, so writes that
    /// other writers committed after the rows were staged are kept.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails, in which case none of `staged`
    /// is visible, or if `staged` holds rows another backend staged.
    async fn publish_staged(&self, table_name: &str, staged: Vec<StagedAppend>) -> Result<()> {
        if staged.iter().any(StagedAppend::is_pending) {
            anyhow::bail!(
                "publish_staged('{table_name}'): this backend never stages rows, so it \
                 cannot publish rows staged by another backend"
            );
        }
        Ok(())
    }

    /// Drop the rows in `staged` without ever making them visible.
    ///
    /// Best effort: storage that cannot be reclaimed now is unreferenced, so
    /// no reader can reach it and the backend's own garbage collection may
    /// remove it later.
    ///
    /// # Errors
    ///
    /// Returns an error if reclaiming the storage failed.
    async fn discard_staged(&self, table_name: &str, staged: Vec<StagedAppend>) -> Result<()> {
        let _ = (table_name, staged);
        Ok(())
    }

    /// Hold `table_name`'s staged files against version cleanup until the
    /// returned [`StagedFilesHold`] is dropped.
    ///
    /// The default protects nothing, which is right for a backend whose
    /// cleanup never deletes files no version references.
    fn hold_staged_files(&self, table_name: &str) -> StagedFilesHold {
        let _ = table_name;
        StagedFilesHold::none()
    }

    /// A durable, backend-private description of the rows in `staged`.
    ///
    /// Written into a crash-recovery record before `staged` is published, so a
    /// process that dies part way through a multi-table publish can complete it
    /// at the next open with [`Self::publish_staged_record`]. Returns `None`
    /// when nothing in `staged` awaits publishing (a backend that cannot stage
    /// wrote it visibly).
    ///
    /// # Errors
    ///
    /// Returns an error if `staged` holds rows this backend cannot describe.
    fn staged_record(
        &self,
        table_name: &str,
        staged: &[StagedAppend],
    ) -> Result<Option<serde_json::Value>> {
        if staged.iter().any(StagedAppend::is_pending) {
            anyhow::bail!(
                "staged_record('{table_name}'): this backend never stages rows, so it \
                 cannot describe rows staged by another backend"
            );
        }
        Ok(None)
    }

    /// Publish the rows described by `record` (from [`Self::staged_record`])
    /// unless an earlier attempt already did.
    ///
    /// Exactly once: the backend recognizes its own earlier publish of the same
    /// rows at any version after `published_after` (the table's version before
    /// the first attempt), including one a later commit has since rewritten,
    /// and then commits nothing. Like [`Self::publish_staged`] it appends onto
    /// the current version, so other writers' commits are kept.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails, if `record` was not written by
    /// this backend, or if only part of the recorded storage is still present,
    /// so that neither publishing nor skipping it would be exact.
    async fn publish_staged_record(
        &self,
        table_name: &str,
        record: &serde_json::Value,
        published_after: Option<u64>,
    ) -> Result<RecordedPublish> {
        let _ = (record, published_after);
        anyhow::bail!(
            "publish_staged_record('{table_name}'): this backend never stages rows, so \
             it cannot publish a recorded staged append"
        )
    }

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

    /// Pin `version` of `table_name` under `tag`, so version cleanup keeps it.
    ///
    /// A snapshot restore rolls tables back to versions recorded when the
    /// snapshot was taken; an unpinned version older than the compaction
    /// retention window is deleted, after which the restore cannot run.
    ///
    /// # Errors
    ///
    /// The default refuses: a backend without versioned tables cannot keep a
    /// snapshot restorable.
    async fn tag_table_version(&self, table_name: &str, tag: &str, version: u64) -> Result<()> {
        let _ = (tag, version);
        anyhow::bail!("backend cannot pin versions of '{table_name}' for a snapshot restore")
    }

    /// Remove the pin `tag` from `table_name`, so version cleanup may delete
    /// the version it held.
    ///
    /// Idempotent: a tag or table that does not exist is not an error, so a
    /// snapshot deletion cut short converges when run again.
    ///
    /// # Errors
    ///
    /// When the tag exists and cannot be removed. The default succeeds: a
    /// backend that keeps the default [`Self::tag_table_version`] never pins
    /// anything.
    async fn delete_table_tag(&self, table_name: &str, tag: &str) -> Result<()> {
        let _ = (table_name, tag);
        Ok(())
    }

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

    /// Occupancy of the backend's own in-process caches, if it has any.
    ///
    /// A cheap, synchronous read. `None` (the default) means the backend
    /// keeps no such cache, not that it is empty.
    fn cache_usage(&self) -> Option<BackendCacheUsage> {
        None
    }

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

    /// A read-only view of this backend that reads every table in `versions`
    /// at the version given and treats every other table as absent.
    ///
    /// What an explicit snapshot's reads go through: its restore point
    /// records each table's version and tags it against cleanup, so reading
    /// those versions returns the snapshot's rows whatever compaction or later
    /// writes did to the live tables. `None` (the default) when the backend
    /// cannot read an older version; such a snapshot reads the live tables
    /// under its version ceiling instead.
    fn at_table_versions(
        &self,
        versions: &std::collections::BTreeMap<String, u64>,
    ) -> Option<Arc<dyn StorageBackend>> {
        let _ = versions;
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

    /// A full-text search of `table` that also scores `unstored` documents
    /// (id, text), all ranked as this backend's search would rank them once
    /// those documents were rows of `table`.
    ///
    /// A search merges unflushed rows into the hits for stored ones. Scoring
    /// them on another scale or other statistics than the backend will use
    /// once they are flushed made the ranking, and which rows made the top
    /// `k`, change at the flush. `filter` applies to stored rows; the caller
    /// filters the unstored documents. A document that does not match is
    /// absent. `table` need not exist yet.
    ///
    /// The default runs [`Self::full_text_search`] and scores an unstored
    /// document by the fraction of query words it contains (lowercased,
    /// whitespace-split), for a backend that cannot say how it ranks.
    ///
    /// # Errors
    /// Returns an error if the search fails or the documents cannot be
    /// scored.
    #[expect(clippy::too_many_arguments, reason = "mirrors full_text_search")]
    async fn full_text_search_with_unstored(
        &self,
        table: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: FilterExpr,
        unstored: Vec<(u64, String)>,
        counters: Option<std::sync::Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<TextSearch> {
        let stored = if self.table_exists(table).await? {
            self.full_text_search(table, column, query, k, filter, counters)
                .await?
        } else {
            Vec::new()
        };
        Ok(TextSearch {
            stored,
            unstored: unstored
                .into_iter()
                .filter_map(|(id, text)| {
                    let score = crate::storage::manager::compute_text_relevance(query, &text);
                    (score > 0.0).then_some((id, score))
                })
                .collect(),
        })
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
