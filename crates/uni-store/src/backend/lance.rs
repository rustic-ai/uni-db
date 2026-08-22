// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Lance implementation of the [`StorageBackend`] trait.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::{Stream, StreamExt, TryStreamExt};

use uni_common::core::schema::TokenizerConfig;

use super::lance_branch;
use super::lance_directory::LanceDirectory;
use super::traits::{RecordBatchStream, StorageBackend};
use super::types::*;

/// Lance implementation of [`StorageBackend`].
///
/// Built directly on `lance::Dataset` via [`LanceDirectory`]; the `lancedb`
/// layer it once wrapped has been removed. All Lance-specific code is confined
/// to this module and its siblings (`lance_branch`, `lance_directory`).
pub struct LanceDbBackend {
    /// The directory of Lance datasets this backend addresses.
    ///
    /// Owns table-name → dataset-path resolution and dataset opens; see its
    /// module docs for the layout contract it must uphold.
    directory: LanceDirectory,
    base_uri: String,
    /// Per-table write serialization mutex. Acquired by `write` and
    /// `create_table` around the check-then-create. Without this, two
    /// concurrent async-flush streams that both observe a table as
    /// not-yet-existing can both succeed at `create_table`, and Lance's
    /// CreateTableMode::Create (default) does NOT atomically reject
    /// the second — observed under in-memory backend, where the
    /// second Create writes a new dataset that REPLACES the first,
    /// silently losing the first's batch. Per-table mutex preserves
    /// parallelism across different tables (different labels).
    table_write_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Existence cache populated lazily by [`Self::table_exists`].
    ///
    /// Avoids paying for [`LanceDirectory::table_names`] (which lists every
    /// table in the database) on every `table_exists` call. uni-db's
    /// query planner calls `table_exists` per-table per-query, so without
    /// this cache, post-flush latency scales with total schema size.
    /// Updated synchronously by `create_table`, `create_empty_table`,
    /// `open_or_create_table`, and `drop_table` so the cache is the
    /// authoritative source after first population. See issue #55.
    existence_cache: DashMap<String, bool>,
    /// Schema cache populated lazily by [`Self::get_table_schema`].
    ///
    /// Lance schemas are stable for the table's lifetime under our usage
    /// (we never alter columns in place — schema-evolving migrations would
    /// drop/recreate the table). Caching avoids the per-query
    /// dataset open + schema conversion for every Cypher query that
    /// scans a label or edge type. See issue #55.
    schema_cache: DashMap<String, Arc<ArrowSchema>>,
}

/// Map uni's backend-neutral metric onto Lance's.
fn distance_metric_of(metric: DistanceMetric) -> lance_linalg::distance::MetricType {
    match metric {
        DistanceMetric::L2 => lance_linalg::distance::MetricType::L2,
        DistanceMetric::Cosine => lance_linalg::distance::MetricType::Cosine,
        DistanceMetric::Dot => lance_linalg::distance::MetricType::Dot,
    }
}

impl LanceDbBackend {
    /// Connect to a LanceDB database at the given URI.
    pub async fn connect(
        uri: &str,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        let directory = LanceDirectory::connect(uri, storage_options).await?;

        Ok(Self {
            directory,
            base_uri: uri.to_string(),
            table_write_locks: DashMap::new(),
            existence_cache: DashMap::new(),
            schema_cache: DashMap::new(),
        })
    }

    /// Get or insert the per-table write mutex used to serialize
    /// concurrent `write` / `create_table` against the same table.
    /// See `table_write_locks` field doc for context.
    fn write_lock_for(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.table_write_locks
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Write `batches` to `table` with `mode`, on raw Lance.
    ///
    /// `schema` travels separately so the empty case works: an empty `batches`
    /// carries no schema, and `WriteMode::Create` on an empty vector is how a
    /// schema-only table gets materialized. A single zero-row batch is what
    /// actually conveys the schema to Lance — the same normalization
    /// `LanceBranching::reader` performs.
    ///
    /// This is the one place primary writes reach storage, so storage options
    /// are threaded exactly once, via [`LanceDirectory::write_params`].
    async fn write_batches(
        &self,
        table: &str,
        mut batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
        mode: lance::dataset::WriteMode,
    ) -> Result<()> {
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(schema.clone()));
        }
        let uri = self.directory.dataset_uri(table);
        let params = self.directory.write_params(mode);
        let reader = arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        lance::Dataset::write(reader, &uri, Some(params))
            .await
            .map_err(|e| anyhow!("Write to '{}' ({:?}) failed: {}", table, mode, e))?;
        Ok(())
    }

    /// Execute a scan query on the primary branch.
    ///
    /// Mirrors [`Self::execute_branch_scan`] with one deliberate difference:
    /// scalar-index pushdown stays **enabled** here. The branch path disables
    /// it because a fork's `base_paths` chain can't resolve a BTree's
    /// `page_lookup.lance` past one level (#106); primary has no such chain,
    /// so it keeps the acceleration. The two paths therefore differ in plan,
    /// never in result set.
    async fn execute_primary_scan(&self, request: &ScanRequest) -> Result<RecordBatchStream> {
        let dataset = self.directory.open(&request.table_name).await?;
        let mut scanner = dataset.scan();

        if let ColumnProjection::Columns(cols) = &request.columns {
            scanner.project(cols).map_err(|e| {
                anyhow!(
                    "Project columns {:?} on '{}': {}",
                    cols,
                    request.table_name,
                    e
                )
            })?;
        }

        if !request.filter.is_trivially_true() {
            let sql = request.filter.to_sql()?;
            scanner
                .filter(&sql)
                .map_err(|e| anyhow!("Filter '{}' on '{}': {}", sql, request.table_name, e))?;
        }

        if let Some(limit) = request.limit {
            scanner
                .limit(Some(limit as i64), None)
                .map_err(|e| anyhow!("Limit on scan of '{}': {}", request.table_name, e))?;
        }

        attach_scan_stats(&mut scanner, request);

        let stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| anyhow!("Scan stream on '{}': {}", request.table_name, e))?;

        let mapped: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
            Box::pin(stream.map(|r| r.map_err(|e| anyhow!("{}", e))));
        Ok(mapped)
    }

    /// Execute a scan query on a Lance branch via the lower-level lance crate.
    async fn execute_branch_scan(
        &self,
        request: &ScanRequest,
        branch: &str,
    ) -> Result<RecordBatchStream> {
        let uri = self.directory.dataset_uri(&request.table_name);
        let dataset = lance_branch::open_branch(&uri, branch).await?;

        let mut scanner = dataset.scan();
        // Disable scalar-index pushdown on branch scans: a fork's `base_paths` chain
        // (child -> parent -> main) resolves data fragments but NOT a scalar (BTree) index's
        // `_indices/<id>/page_lookup.lance` across >1 level, so a filtered branch scan would
        // error on a nested fork (#106). This is result-set neutral — the filter still matches
        // the same rows via a sequential scan — and fork datasets are small, so the lost
        // acceleration is negligible. The primary (non-branch) scan path keeps the index.
        scanner.use_scalar_index(false);

        if let ColumnProjection::Columns(cols) = &request.columns {
            scanner.project(cols).map_err(|e| {
                anyhow!(
                    "Project columns {:?} on '{}@{}': {}",
                    cols,
                    request.table_name,
                    branch,
                    e
                )
            })?;
        }

        if !request.filter.is_trivially_true() {
            let sql = request.filter.to_sql()?;
            scanner.filter(&sql).map_err(|e| {
                anyhow!(
                    "Filter '{}' on '{}@{}': {}",
                    sql,
                    request.table_name,
                    branch,
                    e
                )
            })?;
        }

        if let Some(limit) = request.limit {
            scanner
                .limit(Some(limit as i64), None)
                .map_err(|e| anyhow!("Limit on branched scan failed: {}", e))?;
        }

        // Counted too, and expected to report zero index consultation: the
        // `use_scalar_index(false)` above is what makes this the negative half
        // of the index observable. Skipping the callback here would leave that
        // difference unobservable and the assertion unfalsifiable.
        attach_scan_stats(&mut scanner, request);

        let stream = scanner.try_into_stream().await.map_err(|e| {
            anyhow!(
                "Branched scan stream on '{}@{}': {}",
                request.table_name,
                branch,
                e
            )
        })?;

        let mapped: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
            Box::pin(stream.map(|r| r.map_err(|e| anyhow!("{}", e))));
        Ok(mapped)
    }

    /// Run a scan, dispatching to the primary or branch path based on `request.branch`.
    ///
    /// This dispatch — not `BranchedBackend::apply_branch`, which merely *sets*
    /// `request.branch` — is where a fork read actually happens, so it is where
    /// the branch counter is incremented. `BranchedBackend` resolves a branch per
    /// call and falls back to primary for tables the fork has never written, so a
    /// session with fork scope active can and does execute primary scans.
    /// Counting at selection time would report fork reads that never occurred,
    /// which is the difference between an execution witness and a config read.
    async fn execute_scan_stream(&self, request: &ScanRequest) -> Result<RecordBatchStream> {
        if let Some(branch) = request.branch.clone() {
            request.count_branch_scan();
            return self.execute_branch_scan(request, &branch).await;
        }
        self.execute_primary_scan(request).await
    }
}

#[async_trait]
impl StorageBackend for LanceDbBackend {
    // ========================
    // Table Lifecycle
    // ========================

    async fn table_names(&self) -> Result<Vec<String>> {
        self.directory
            .table_names()
            .await
            .map_err(|e| anyhow!("Failed to list tables: {}", e))
    }

    async fn table_exists(&self, name: &str) -> Result<bool> {
        if let Some(entry) = self.existence_cache.get(name) {
            return Ok(*entry);
        }
        let tables = self.table_names().await?;
        let exists = tables.iter().any(|t| t == name);
        // entry().or_insert preserves a value written by a concurrent
        // create_table/drop_table during our `table_names` await, which
        // is the authoritative state. Plain `insert` would race and
        // could overwrite a writer's `true` with our stale `false`.
        let final_value = *self
            .existence_cache
            .entry(name.to_string())
            .or_insert(exists);
        Ok(final_value)
    }

    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        // L6: reject names unsafe for the dataset path / Lance branch names
        // (a schemaless bad label/edge-type would otherwise panic Lance).
        crate::backend::table_names::validate_table_name(name)?;
        if batches.is_empty() {
            return Err(anyhow!(
                "Cannot create table '{}' with empty data. Use create_empty_table instead.",
                name
            ));
        }
        // Serialize concurrent create_table / write per-table. Without
        // this, two threads that both observed "table doesn't exist"
        // can both call create_table; CreateTableMode::Create's
        // exists-error is not perfectly atomic on some backends
        // (notably in-memory in lancedb 0.27.1), and the second Create
        // overwrites the first's data. See `table_write_locks` field doc.
        let lock = self.write_lock_for(name);
        let _guard = lock.lock().await;
        // Re-check existence under the lock. If a sibling stream
        // created the table while we were waiting, fall back to Append
        // (calling the inner machinery directly since we already hold
        // the per-table write lock).
        let schema = batches[0].schema();
        if self.table_exists(name).await? {
            self.write_batches(name, batches, schema, lance::dataset::WriteMode::Append)
                .await
                .map_err(|e| anyhow!("Failed to append (fallback from create) to '{name}': {e}"))?;
            return Ok(());
        }
        self.write_batches(name, batches, schema, lance::dataset::WriteMode::Create)
            .await
            .map_err(|e| anyhow!("Failed to create table '{name}': {e}"))?;
        self.existence_cache.insert(name.to_string(), true);
        Ok(())
    }

    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        // L6: reject unsafe names before they reach Lance.
        crate::backend::table_names::validate_table_name(name)?;
        self.write_batches(name, Vec::new(), schema, lance::dataset::WriteMode::Create)
            .await
            .map_err(|e| anyhow!("Failed to create empty table '{name}': {e}"))?;
        self.existence_cache.insert(name.to_string(), true);
        Ok(())
    }

    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        if self.table_exists(name).await? {
            // Just verify it can be opened
            self.directory.open(name).await?;
        } else {
            self.create_empty_table(name, schema).await?;
        }
        Ok(())
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        self.schema_cache.remove(name);
        self.directory
            .remove_table(name)
            .await
            .map_err(|e| anyhow!("Failed to drop table '{}': {}", name, e))?;
        self.existence_cache.insert(name.to_string(), false);
        Ok(())
    }

    async fn notify_table_created(&self, name: &str) {
        // BranchedBackend creates fork-side datasets via Lance's branch
        // primitives directly, bypassing this backend's create_table.
        // Without this hook the existence_cache (issue #55) would keep
        // a stale `false` and cause queries to silently see no rows.
        self.existence_cache.insert(name.to_string(), true);
    }

    // ========================
    // Read Operations
    // ========================

    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>> {
        // Fail closed (review C1): a scan error — transient I/O, an unparsable
        // filter, a corrupt fragment — MUST propagate, never collapse into an
        // empty result. Callers such as the MERGE existence-check treat "no
        // rows" as "row absent" and would create a duplicate node on a silently
        // swallowed error. The previous `Err(_) => Ok(vec![])` defeated that
        // fail-closed contract.
        //
        // The one benign not-an-error is a not-yet-created table, which
        // genuinely means "no rows". Detect that explicitly via `table_exists`
        // (the existence cache is kept correct for fork/branch datasets by
        // `notify_table_created`) so a missing table stays empty while every
        // real failure surfaces.
        if !self.table_exists(&request.table_name).await? {
            return Ok(vec![]);
        }

        let stream = self.execute_scan_stream(&request).await?;

        stream
            .try_collect()
            .await
            .map_err(|e| anyhow!("Failed to collect scan results: {}", e))
    }

    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream> {
        self.execute_scan_stream(&request).await
    }

    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>> {
        if let Some(entry) = self.schema_cache.get(name) {
            return Ok(Some(entry.clone()));
        }
        match self.directory.open(name).await {
            Ok(dataset) => {
                // `Dataset::schema()` is Lance's own schema type; the trait
                // hands out Arrow. The conversion is what lancedb's
                // `Table::schema()` did internally.
                let schema: Arc<ArrowSchema> = Arc::new(dataset.schema().into());
                self.schema_cache.insert(name.to_string(), schema.clone());
                Ok(Some(schema))
            }
            // Pre-existing behavior, preserved deliberately: any open failure
            // reads as "table absent", which also hides real I/O errors.
            Err(_) => Ok(None),
        }
    }

    async fn count_rows(&self, table_name: &str, filter: Option<&FilterExpr>) -> Result<usize> {
        let dataset = self.directory.open(table_name).await?;
        let predicate = filter.map(FilterExpr::to_sql).transpose()?;
        dataset
            .count_rows(predicate)
            .await
            .map_err(|e| anyhow!("Failed to count rows in '{}': {}", table_name, e))
    }

    // ========================
    // Write Operations
    // ========================

    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        // Serialize per-table writes. Lance's optimistic concurrency on
        // commit is sufficient for parallel Appends in theory, but
        // under async-flush we observed two concurrent Append/Create
        // mixes producing data loss on the in-memory backend. Holding
        // a per-table mutex eliminates that whole class of races at a
        // cost of serializing writes per-table (parallelism preserved
        // across different tables).
        let lock = self.write_lock_for(table_name);
        let _guard = lock.lock().await;

        let schema = batches[0].schema();
        // lancedb's `add(..).mode(Overwrite)` is `WriteMode::Overwrite`, which
        // commits the new contents as a fresh version rather than mutating in
        // place — that is where `replace_table_atomic`'s atomicity comes from.
        let lance_mode = match mode {
            WriteMode::Append => lance::dataset::WriteMode::Append,
            WriteMode::Overwrite => lance::dataset::WriteMode::Overwrite,
        };
        self.write_batches(table_name, batches, schema, lance_mode)
            .await?;

        Ok(())
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

        // Serialize per-table writes (same as `write`).
        let lock = self.write_lock_for(table_name);
        let _guard = lock.lock().await;

        // Build a reader for the partial-column source. The first batch's
        // schema describes the source subschema; Lance compares it against
        // the target via `allow_subschema=true` internally.
        let schema = batches[0].schema();
        let reader = arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        // `MergeInsertBuilder::try_new` takes owned join-key names.
        let on_owned: Vec<String> = on.iter().map(|s| (*s).to_string()).collect();

        // lancedb's merge_insert is exactly this builder — `try_new` + the
        // when-clauses + `try_build` — so behavior including partial-subschema
        // sources is unchanged. Deliberately NOT setting `WhenNotMatched`
        // beyond the default `DoNothing`: partial writes only update existing
        // rows; CREATE goes through the full-row Append path. Unmatched source
        // rows are dropped.
        let dataset = self.directory.open(table_name).await?;
        let mut builder = lance::dataset::MergeInsertBuilder::try_new(Arc::new(dataset), on_owned)
            .map_err(|e| anyhow!("merge_insert builder on '{}': {}", table_name, e))?;
        builder
            .when_matched(lance::dataset::WhenMatched::UpdateAll)
            .when_not_matched(lance::dataset::WhenNotMatched::DoNothing);
        let job = builder
            .try_build()
            .map_err(|e| anyhow!("merge_insert build on '{}': {}", table_name, e))?;
        job.execute_reader(Box::new(reader))
            .await
            .map_err(|e| anyhow!("merge_insert on '{}': {}", table_name, e))?;
        Ok(())
    }

    async fn delete_rows(&self, table_name: &str, filter: &FilterExpr) -> Result<()> {
        let mut dataset = self.directory.open(table_name).await?;
        dataset
            .delete(&filter.to_sql()?)
            .await
            .map_err(|e| anyhow!("Failed to delete from '{}': {}", table_name, e))?;
        Ok(())
    }

    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()> {
        // Clean up leftover staging table
        let staging_name = format!("{}_staging", name);
        if self.table_exists(&staging_name).await? {
            self.drop_table(&staging_name).await?;
        }

        if self.table_exists(name).await? {
            if batches.is_empty() {
                // Clear, not overwrite: an empty Overwrite would drop the
                // schema along with the rows. `delete("true")` keeps the table
                // and its schema, which is what callers expect from "replace
                // with nothing".
                let mut dataset = self.directory.open(name).await?;
                dataset
                    .delete("true")
                    .await
                    .map_err(|e| anyhow!("Failed to clear table '{}': {}", name, e))?;
            } else {
                let batch_schema = batches[0].schema();
                self.write_batches(
                    name,
                    batches,
                    batch_schema,
                    lance::dataset::WriteMode::Overwrite,
                )
                .await
                .map_err(|e| anyhow!("Failed to overwrite table '{}': {}", name, e))?;
            }
            // Invalidate cache since data changed
        } else if batches.is_empty() {
            self.create_empty_table(name, schema).await?;
        } else {
            self.create_table(name, batches).await?;
        }
        Ok(())
    }

    async fn lock_table_for_write(&self, name: &str) -> crate::backend::traits::TableWriteGuard {
        // Same per-table mutex `write` / `merge_insert` / `create_table` take, exposed as
        // an owned guard so a multi-step read-modify-write (the MUVERA FDE backfill's
        // scan → overwrite) can hold it across both calls and serialize against flush
        // appends. `replace_table_atomic`'s table-exists path takes no internal lock, so a
        // holder calling it does not deadlock.
        crate::backend::traits::TableWriteGuard::held(self.write_lock_for(name).lock_owned().await)
    }

    // ========================
    // Versioning / MVCC
    // ========================

    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>> {
        if !self.table_exists(table_name).await? {
            return Ok(None);
        }
        let dataset = self.directory.open(table_name).await?;
        Ok(Some(dataset.version().version))
    }

    async fn rollback_table(&self, table_name: &str, target_version: u64) -> Result<()> {
        // lancedb's protocol was `checkout(v)` then `restore()`: pin the handle
        // to the target version, then commit that as a new version. Opening
        // directly at the version is the same first step, and `restore` is
        // Lance's own — lancedb only forwarded it.
        let mut dataset = self
            .directory
            .open_at_version(table_name, target_version)
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to checkout version {} for '{}': {}",
                    target_version,
                    table_name,
                    e
                )
            })?;
        dataset.restore().await.map_err(|e| {
            anyhow!(
                "Failed to restore '{}' to version {}: {}",
                table_name,
                target_version,
                e
            )
        })?;
        Ok(())
    }

    // ========================
    // Maintenance
    // ========================

    async fn optimize_table(
        &self,
        table_name: &str,
        version_retention: std::time::Duration,
    ) -> Result<OptimizeReport> {
        let mut dataset = self.directory.open(table_name).await?;

        // The three steps lancedb's `OptimizeAction::All` performed, in order
        // (`lancedb/src/table/optimize.rs:172-186`). lancedb discarded its
        // `OptimizeStats`; we do not, because everything a caller needs to tell
        // a real compaction from a no-op is in these two return values (#172).
        //
        // An empty compaction plan yields `CompactionMetrics::default()` without
        // a commit, so all-zeros here is a genuine "nothing to merge" rather
        // than a counter that was never wired.
        let metrics = lance::dataset::optimize::compact_files(
            &mut dataset,
            lance::dataset::optimize::CompactionOptions::default(),
            None,
        )
        .await
        .map_err(|e| anyhow!("Failed to compact '{}': {}", table_name, e))?;

        // Prune versions older than the configured retention window (7 days by
        // default, matching lancedb's hardcoded value). This is safe for forks
        // despite the "retention must not drop
        // below the longest live fork chain" invariant: Lance's cleanup is
        // branch-aware — it calls `find_referenced_branches()` and then
        // `retain_branch_lineage_files()`, and `clean_referenced_branches`
        // defaults to false, so versions a live fork branch still needs are
        // retained regardless of age (`lance/src/dataset/cleanup.rs:146,181,930`).
        let policy = lance::dataset::cleanup::CleanupPolicy {
            before_timestamp: Some(
                chrono::Utc::now()
                    - chrono::Duration::from_std(version_retention)
                        .unwrap_or_else(|_| chrono::Duration::days(7)),
            ),
            ..Default::default()
        };
        let removal = lance::dataset::cleanup::cleanup_old_versions(&dataset, policy)
            .await
            .map_err(|e| anyhow!("Failed to prune old versions of '{}': {}", table_name, e))?;

        // `optimize_indices` reports nothing, so there is nothing to harvest here.
        lance::index::DatasetIndexExt::optimize_indices(
            &mut dataset,
            &lance_index::optimize::OptimizeOptions::default(),
        )
        .await
        .map_err(|e| anyhow!("Failed to optimize indices on '{}': {}", table_name, e))?;

        Ok(OptimizeReport {
            fragments_removed: metrics.fragments_removed,
            fragments_added: metrics.fragments_added,
            files_removed: metrics.files_removed,
            files_added: metrics.files_added,
            // Bytes freed by version pruning above, not by the compaction
            // rewrite — Lance reports no before/after size for the rewrite. With
            // the retention window in force this is 0 for any dataset younger
            // than it, which is every test fixture.
            bytes_reclaimed: removal.bytes_removed,
            old_versions_removed: removal.old_versions,
        })
    }

    async fn recover_staging(&self, name: &str) -> Result<()> {
        let staging_name = format!("{}_staging", name);

        if !self.table_exists(&staging_name).await? {
            return Ok(());
        }

        let main_exists = self.table_exists(name).await?;

        if main_exists {
            log::info!("Cleaning up leftover staging table: {}", staging_name);
            self.drop_table(&staging_name).await?;
        } else {
            log::warn!("Recovering table '{}' from staging after crash", name);

            let staging = self.directory.open(&staging_name).await?;
            let schema: Arc<ArrowSchema> = Arc::new(staging.schema().into());

            let stream = staging
                .scan()
                .try_into_stream()
                .await
                .map_err(|e| anyhow!("Failed to query staging: {}", e))?;
            let batches: Vec<RecordBatch> = stream
                .try_collect()
                .await
                .map_err(|e| anyhow!("Failed to collect staging data: {}", e))?;

            if batches.is_empty() {
                self.create_empty_table(name, schema).await?;
            } else {
                self.create_table(name, batches).await?;
            }

            self.drop_table(&staging_name).await?;
            log::info!("Successfully recovered table '{}' from staging", name);
        }

        Ok(())
    }

    // ========================
    // Cache Management
    // ========================

    /// No-op, as before the lancedb removal.
    ///
    /// These only ever cleared the `lancedb::Table` cache, which was never
    /// populated (a cached handle is version-pinned and would drop rows
    /// committed later), so both calls were already no-ops. That is preserved
    /// verbatim rather than quietly extended to `schema_cache`: changing what
    /// an explicit invalidation does is a behavior change, not a port. Worth
    /// revisiting — `schema_cache` is now the only cache, so a caller asking
    /// to invalidate currently gets nothing — but as its own piece of work.
    fn invalidate_cache(&self, _table_name: &str) {}

    /// No-op — see [`Self::invalidate_cache`].
    fn clear_cache(&self) {}

    // ========================
    // Metadata
    // ========================

    fn base_uri(&self) -> &str {
        &self.base_uri
    }

    fn branching(&self) -> Option<Arc<dyn crate::backend::branching::ForkBranching>> {
        Some(Arc::new(super::lance_branch::LanceBranching::new(
            self.base_uri.clone(),
        )))
    }

    // ========================
    // Capability Checks
    // ========================

    fn supports_vector_search(&self) -> bool {
        true
    }

    fn supports_full_text_search(&self) -> bool {
        true
    }

    fn supports_scalar_index(&self) -> bool {
        true
    }

    // ========================
    // Optional Capabilities
    // ========================

    // async_trait rewrites the signature, so clippy's arg count doesn't trip the
    // `too_many_arguments` lint here — use allow (expect would be unfulfilled).
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
    ) -> Result<Vec<RecordBatch>> {
        let dataset = self.directory.open(table).await?;
        let key = arrow_array::Float32Array::from(query.to_vec());
        let mut scanner = dataset.scan();
        scanner
            .nearest(column, &key, k)
            .map_err(|e| anyhow!("Failed to create vector search on '{}': {}", table, e))?;
        // The metric is passed explicitly rather than left to the index's own:
        // Lance uses it to decide whether an existing index is usable for this
        // query at all (`scanner.rs:3577`), which is what lancedb's
        // `.distance_type(..)` was doing.
        scanner.distance_metric(distance_metric_of(metric));

        if let Some(n) = opts.nprobes {
            scanner.nprobes(n);
        }
        if let Some(r) = opts.refine_factor {
            scanner.refine(r);
        }
        if let Some(ef) = opts.ef {
            scanner.ef(ef);
        }
        if !filter.is_trivially_true() {
            let sql = filter.to_sql()?;
            // lancedb's `only_if` defaulted to prefilter (`query.rs:782`), so
            // prefiltering here is exact parity — and it is also the correct
            // semantic: postfiltering would let excluded rows consume top-k
            // slots and shrink the result below k.
            scanner.prefilter(true);
            scanner
                .filter(&sql)
                .map_err(|e| anyhow!("Vector search filter '{}' on '{}': {}", sql, table, e))?;
        }

        scanner
            .try_into_stream()
            .await
            .map_err(|e| anyhow!("Vector search execution failed on '{}': {}", table, e))?
            .try_collect()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to collect vector search results from '{}': {}",
                    table,
                    e
                )
            })
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
    ) -> Result<Vec<RecordBatch>> {
        if query.is_empty() {
            return Err(anyhow!("multivector_search on '{}': empty query", table));
        }
        let dataset = self.directory.open(table).await?;

        // Late-interaction (MaxSim) query. lancedb expressed this as
        // `vector_search(first)` then `add_query_vector(..)` per remaining
        // token, which it accumulated into a list of query vectors. Lance's
        // `nearest` takes that shape directly: a `ListArray` whose every
        // element is one query vector of the column's dimension — it detects
        // multivector from the array type and validates each entry's length
        // against the column dim (`scanner.rs:1450-1472`).
        let mut builder =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::Float32Builder::new());
        for token in query {
            builder.values().append_slice(token);
            builder.append(true);
        }
        let key = builder.finish();

        let mut scanner = dataset.scan();
        scanner
            .nearest(column, &key, k)
            .map_err(|e| anyhow!("Failed to create multivector search on '{}': {}", table, e))?;
        scanner.distance_metric(distance_metric_of(metric));

        if let Some(n) = opts.nprobes {
            scanner.nprobes(n);
        }
        if let Some(r) = opts.refine_factor {
            scanner.refine(r);
        }
        if let Some(ef) = opts.ef {
            scanner.ef(ef);
        }
        if !filter.is_trivially_true() {
            let sql = filter.to_sql()?;
            // Prefilter, as in `vector_search` — see the note there.
            scanner.prefilter(true);
            scanner.filter(&sql).map_err(|e| {
                anyhow!("Multivector search filter '{}' on '{}': {}", sql, table, e)
            })?;
        }

        scanner
            .try_into_stream()
            .await
            .map_err(|e| anyhow!("Multivector search execution failed on '{}': {}", table, e))?
            .try_collect()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to collect multivector search results from '{}': {}",
                    table,
                    e
                )
            })
    }

    async fn full_text_search(
        &self,
        table: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: FilterExpr,
    ) -> Result<Vec<RecordBatch>> {
        use lance_index::scalar::FullTextSearchQuery;
        use lance_index::scalar::inverted::query::MatchQuery;

        let dataset = self.directory.open(table).await?;

        // These are `lance_index` types already — lancedb only forwarded them
        // to the same scanner, so the query object is unchanged.
        let match_query = MatchQuery::new(query.to_string()).with_column(Some(column.to_string()));
        let fts_query = FullTextSearchQuery {
            query: match_query.into(),
            limit: Some(k as i64),
            wand_factor: None,
        };

        let mut scanner = dataset.scan();
        scanner
            .full_text_search(fts_query)
            .map_err(|e| anyhow!("FTS query on '{}': {}", table, e))?;
        // `k` is applied both inside the FTS query and as a scan limit, as
        // before — the inner bound caps the BM25 candidate set, the outer one
        // the returned rows.
        scanner
            .limit(Some(k as i64), None)
            .map_err(|e| anyhow!("FTS limit on '{}': {}", table, e))?;

        if !filter.is_trivially_true() {
            let sql = filter.to_sql()?;
            scanner
                .filter(&sql)
                .map_err(|e| anyhow!("FTS filter '{}' on '{}': {}", sql, table, e))?;
        }

        scanner
            .try_into_stream()
            .await
            .map_err(|e| anyhow!("FTS search execution failed on '{}': {}", table, e))?
            .try_collect()
            .await
            .map_err(|e| anyhow!("Failed to collect FTS results from '{}': {}", table, e))
    }

    async fn create_vector_index(
        &self,
        table: &str,
        column: &str,
        name: &str,
        params: VectorIndexParams,
    ) -> Result<()> {
        use lance::index::vector::VectorIndexParams as LanceVectorParams;
        use lance_index::vector::hnsw::builder::HnswBuildParams;
        use lance_index::vector::ivf::IvfBuildParams;
        use lance_index::vector::pq::PQBuildParams;
        use lance_index::vector::sq::builder::SQBuildParams;

        let dt = match params.metric {
            DistanceMetric::L2 => lance_linalg::distance::MetricType::L2,
            DistanceMetric::Cosine => lance_linalg::distance::MetricType::Cosine,
            DistanceMetric::Dot => lance_linalg::distance::MetricType::Dot,
        };

        // The stage params are built explicitly and fed to the `with_*_params`
        // constructors rather than the positional shorthands (`ivf_pq(..)`),
        // because the shorthands demand values lancedb never asked us for
        // (e.g. `max_iterations`). Going through `..Default::default()` keeps
        // whatever lancedb's builders were defaulting to.
        let hnsw = |m: u32, ef_construction: u32| HnswBuildParams {
            m: m as usize,
            ef_construction: ef_construction as usize,
            ..Default::default()
        };

        let lance_params = match params.kind {
            // Flat is a single-partition IVF, matching the prior mapping.
            VectorIndexKind::Flat => {
                LanceVectorParams::with_ivf_flat_params(dt, IvfBuildParams::new(1))
            }
            VectorIndexKind::IvfFlat { num_partitions } => LanceVectorParams::with_ivf_flat_params(
                dt,
                IvfBuildParams::new(num_partitions as usize),
            ),
            VectorIndexKind::IvfPq {
                num_partitions,
                num_sub_vectors,
                num_bits,
            } => LanceVectorParams::with_ivf_pq_params(
                dt,
                IvfBuildParams::new(num_partitions as usize),
                PQBuildParams {
                    num_sub_vectors: num_sub_vectors as usize,
                    num_bits: usize::from(num_bits),
                    ..Default::default()
                },
            ),
            VectorIndexKind::IvfSq { num_partitions } => LanceVectorParams::with_ivf_sq_params(
                dt,
                IvfBuildParams::new(num_partitions as usize),
                SQBuildParams::default(),
            ),
            VectorIndexKind::IvfRq {
                num_partitions,
                num_bits,
            } => LanceVectorParams::ivf_rq(
                num_partitions as usize,
                // `None` means "whatever the backend defaults to"; RaBitQ's
                // canonical default is 8 bits, which is also what lancedb's
                // `IvfRqIndexBuilder::default()` left in place.
                num_bits.unwrap_or(8),
                dt,
            ),
            VectorIndexKind::HnswFlat {
                m,
                ef_construction,
                num_partitions,
            } => LanceVectorParams::ivf_hnsw(
                dt,
                IvfBuildParams::new(num_partitions as usize),
                hnsw(m, ef_construction),
            ),
            VectorIndexKind::HnswSq {
                m,
                ef_construction,
                num_partitions,
            } => LanceVectorParams::with_ivf_hnsw_sq_params(
                dt,
                IvfBuildParams::new(num_partitions as usize),
                hnsw(m, ef_construction),
                SQBuildParams::default(),
            ),
            VectorIndexKind::HnswPq {
                m,
                ef_construction,
                num_sub_vectors,
                num_partitions,
            } => LanceVectorParams::with_ivf_hnsw_pq_params(
                dt,
                IvfBuildParams::new(num_partitions as usize),
                hnsw(m, ef_construction),
                PQBuildParams {
                    num_sub_vectors: num_sub_vectors as usize,
                    // 8 bits matches the prior `PQBuildParams::new(_, 8)` default.
                    num_bits: 8,
                    ..Default::default()
                },
            ),
        };

        let mut dataset = self.directory.open(table).await?;
        lance::index::DatasetIndexExt::create_index(
            &mut dataset,
            &[column],
            lance_index::IndexType::Vector,
            Some(name.to_string()),
            &lance_params,
            true,
        )
        .await
        // `create_index` hands back the new IndexMetadata; the trait returns unit.
        .map(|_| ())
        .map_err(|e| {
            anyhow!(
                "Failed to create vector index '{}' on '{}.{}': {}",
                name,
                table,
                column,
                e
            )
        })
    }

    async fn create_scalar_index(
        &self,
        table: &str,
        columns: &[&str],
        index_type: ScalarIndexType,
        name: Option<&str>,
    ) -> Result<()> {
        // Lance discriminates the three scalar flavors by `BuiltinIndexType`
        // inside `ScalarIndexParams`, where lancedb used three distinct
        // `Index::*` variants.
        let builtin = match index_type {
            ScalarIndexType::BTree => lance_index::scalar::BuiltinIndexType::BTree,
            ScalarIndexType::Bitmap => lance_index::scalar::BuiltinIndexType::Bitmap,
            ScalarIndexType::LabelList => lance_index::scalar::BuiltinIndexType::LabelList,
        };
        let params = lance_index::scalar::ScalarIndexParams::for_builtin(builtin);

        let mut dataset = self.directory.open(table).await?;
        lance::index::DatasetIndexExt::create_index(
            &mut dataset,
            columns,
            lance_index::IndexType::Scalar,
            name.map(str::to_string),
            &params,
            true,
        )
        .await
        // `create_index` hands back the new IndexMetadata; the trait returns unit.
        .map(|_| ())
        .map_err(|e| {
            anyhow!(
                "Failed to create {:?} index on '{}.{:?}': {}",
                index_type,
                table,
                columns,
                e
            )
        })
    }

    async fn create_fts_index(
        &self,
        table: &str,
        columns: &[&str],
        name: Option<&str>,
        tokenizer: &TokenizerConfig,
        with_positions: bool,
    ) -> Result<()> {
        // Translate the requested analyzer pipeline into Lance params. A
        // config error (bad ngram bounds, unsupported stop-word language) is
        // surfaced here before we touch the table.
        //
        // `to_inverted_params` already returns `InvertedIndexParams`, which is
        // a `lance_index` type — lancedb only wrapped it in `Index::FTS`, so
        // this is the same params object reaching the same builder.
        let params =
            super::fts_analyzer::to_inverted_params(tokenizer, with_positions).map_err(|e| {
                anyhow!(
                    "invalid FTS tokenizer config for '{}.{:?}': {}",
                    table,
                    columns,
                    e
                )
            })?;

        let mut dataset = self.directory.open(table).await?;
        lance::index::DatasetIndexExt::create_index(
            &mut dataset,
            columns,
            lance_index::IndexType::Inverted,
            name.map(str::to_string),
            &params,
            true,
        )
        .await
        // `create_index` hands back the new IndexMetadata; the trait returns unit.
        .map(|_| ())
        .map_err(|e| {
            // Custom tokenizers (`lindera/*`, `jieba/*`) need dictionary files
            // under `LANCE_LANGUAGE_MODEL_HOME`; make that failure legible.
            if matches!(tokenizer, TokenizerConfig::Custom { .. })
                || matches!(
                    tokenizer,
                    TokenizerConfig::Analyzer(a)
                        if matches!(&a.base, uni_common::core::schema::BaseTokenizer::Custom(_))
                )
            {
                anyhow!(
                    "Failed to create FTS index on '{}.{:?}' with custom tokenizer {:?}: {}. \
                     CJK/custom tokenizers require dictionary files under the directory named by \
                     the LANCE_LANGUAGE_MODEL_HOME environment variable (uni does not ship them).",
                    table,
                    columns,
                    tokenizer,
                    e
                )
            } else {
                anyhow!(
                    "Failed to create FTS index on '{}.{:?}': {}",
                    table,
                    columns,
                    e
                )
            }
        })
    }

    async fn drop_index(&self, table: &str, index_name: &str) -> Result<()> {
        let mut dataset = self.directory.open(table).await?;
        lance::index::DatasetIndexExt::drop_index(&mut dataset, index_name)
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to drop index '{}' on '{}': {}",
                    index_name,
                    table,
                    e
                )
            })
    }

    async fn list_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        let dataset = self.directory.open(table).await?;
        let indices = lance::index::DatasetIndexExt::load_indices(&dataset)
            .await
            .map_err(|e| anyhow!("Failed to list indexes on '{}': {}", table, e))?;

        // `columns` is what callers actually use — all four production consumers
        // of this method filter on `idx.columns.contains(..)` and none reads
        // `index_type`. Lance's `IndexMetadata` carries field *ids* rather than
        // names, so resolve them through the dataset schema.
        let schema = dataset.schema();
        Ok(indices
            .iter()
            .map(|idx| IndexInfo {
                name: idx.name.clone(),
                columns: idx
                    .fields
                    .iter()
                    .filter_map(|fid| schema.field_by_id(*fid).map(|f| f.name.clone()))
                    .collect(),
                // Lance's `IndexMetadata` carries no index-type discriminant
                // (the type lives in the opaque `index_details` protobuf), so
                // this is reported as unknown rather than fabricated. Safe
                // because no consumer reads it — verified across all four
                // production callers of `list_indexes`, which filter on
                // `columns` alone. Populate it properly if that ever changes.
                index_type: String::from("unknown"),
            })
            .collect())
    }
}

/// Attaches Lance's execution-stats callback so a scan reports what its plan
/// actually did with indexes.
///
/// Lance harvests these counts by walking the DataFusion `MetricsSet` of the
/// plan it executed, so they are execution truth rather than a reading of what
/// we asked for. It must be set before `try_into_stream()`, which clones the
/// callback into the plan options; setting it afterwards is a silent no-op.
///
/// Deliberately used on the plain scan paths only. `indices_loaded` is summed
/// from one shared `IndexMetrics` across scalar, vector and FTS index nodes
/// alike, so it cannot say *which kind* was consulted. The distinction comes
/// from the call site: a scanner that never calls `nearest()` or
/// `full_text_search()` can only produce scalar-index nodes. Attaching this to
/// the vector or FTS search paths would destroy that, and nothing would fail.
fn attach_scan_stats(scanner: &mut lance::dataset::scanner::Scanner, request: &ScanRequest) {
    let Some(counters) = request.counters.clone() else {
        return;
    };
    scanner.scan_stats_callback(Arc::new(
        move |s: &lance::dataset::scanner::ExecutionSummaryCounts| {
            // Three terms, and the third is load-bearing rather than belt-and-braces.
            //
            // `indices_loaded` and `parts_loaded` are cache-MISS counters: Lance's
            // `MetricsCollector` documents that `record_index_loads` "should not be
            // called if the index is already in memory" and `record_parts_loaded`
            // likewise for a cached shard. They fire today only because a fresh
            // `Dataset` is opened per scan, so the cache is always cold. Add a
            // dataset or index cache and both go to zero on a warm hit while the
            // index is still very much being used.
            //
            // `index_comparisons` is recorded on the SEARCH path — "a B-tree index
            // may make comparisons while searching for a value" — so it is
            // indifferent to caching. It is what keeps this predicate true after
            // such a change. Do not simplify this to `indices_loaded > 0`.
            let consulted = s.indices_loaded > 0 || s.parts_loaded > 0 || s.index_comparisons > 0;
            // `iops` is the compaction witness: it falls with fragment count
            // (~5 per fragment on a full scan), measured by
            // `probe_compaction_moves_lance_io_counts` below.
            counters.add_lance_scan(consulted, s.index_comparisons, s.iops);
        },
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, UInt64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::TempDir;

    async fn create_test_backend() -> (TempDir, LanceDbBackend) {
        let temp_dir = TempDir::new().unwrap();
        let uri = temp_dir.path().to_str().unwrap();
        let backend = LanceDbBackend::connect(uri, None).await.unwrap();
        (temp_dir, backend)
    }

    fn test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn test_batch(ids: Vec<u64>, values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    /// **Phase 4B probe — does any per-query I/O count move across a compaction?**
    ///
    /// The deferred DQP compaction lever needs a witness, and `CompactionStats`
    /// cannot be one: `Lever::activated` takes two per-*query* `Witness`es while
    /// the stats are per-*run*. So the lever is blocked on finding a per-query
    /// counter that changes when fragments merge.
    ///
    /// `ExecutionSummaryCounts` is the candidate. [`attach_scan_stats`] already
    /// receives `iops`, `requests` and `bytes_read` on every scan and reads none
    /// of them — only the three index fields. Fewer fragments should mean fewer
    /// file opens, so `iops` is the natural place to look.
    ///
    /// This measures rather than assumes, and it lives here rather than in the
    /// DQP suite for a reason: those counts are visible only inside the callback
    /// closure. Reaching them from `crates/uni` means plumbing them through
    /// `QueryCounters` -> `QueryMetrics` -> `Witness`, which is exactly the
    /// production change this probe exists to justify or rule out.
    ///
    /// The conclusion is gated on a **confirmed merge**. A probe that observes
    /// nothing and concludes "unobservable" is how the index lever was wrongly
    /// deferred once already — that probe used a predicate shape that could
    /// never consult an index, so it measured nothing and blamed the counter.
    #[tokio::test]
    async fn probe_compaction_moves_lance_io_counts() {
        use std::sync::Mutex;

        /// One scan's raw counts.
        #[derive(Debug, Clone, Default)]
        struct Counts {
            iops: usize,
            requests: usize,
            bytes_read: usize,
        }

        async fn scan_counts(
            backend: &LanceDbBackend,
            table: &str,
            filter: Option<&str>,
        ) -> Counts {
            let dataset = backend.directory.open(table).await.unwrap();
            let mut scanner = dataset.scan();
            if let Some(f) = filter {
                scanner.filter(f).unwrap();
            }
            let seen: Arc<Mutex<Counts>> = Arc::new(Mutex::new(Counts::default()));
            let sink = Arc::clone(&seen);
            scanner.scan_stats_callback(Arc::new(
                move |s: &lance::dataset::scanner::ExecutionSummaryCounts| {
                    let mut g = sink.lock().unwrap();
                    g.iops += s.iops;
                    g.requests += s.requests;
                    g.bytes_read += s.bytes_read;
                },
            ));
            let mut stream = scanner.try_into_stream().await.unwrap();
            while let Some(b) = stream.next().await {
                b.unwrap();
            }
            let g = seen.lock().unwrap();
            g.clone()
        }

        let (_dir, backend) = create_test_backend().await;
        let table = "probe_io";

        // Five separate appends, so there are five fragments to merge. Append
        // only: a delete would leave a deletion file and change what compaction
        // does.
        backend
            .create_table(table, vec![test_batch(vec![0], vec![0])])
            .await
            .unwrap();
        for i in 1..5u64 {
            backend
                .write(
                    table,
                    vec![test_batch(
                        (i * 100..i * 100 + 100).collect(),
                        (0..100).collect(),
                    )],
                    WriteMode::Append,
                )
                .await
                .unwrap();
        }

        const SHAPES: [(&str, Option<&str>); 2] =
            [("full", None), ("filtered", Some("value > 50"))];
        const TRIALS: usize = 3;

        let mut before = Vec::new();
        for (name, filter) in SHAPES {
            for t in 0..TRIALS {
                before.push((name, t, scan_counts(&backend, table, filter).await));
            }
        }

        // The transition, and the proof it happened. Retention is irrelevant
        // here; what matters is `fragments_removed`.
        let report = backend
            .optimize_table(table, std::time::Duration::from_secs(7 * 24 * 3600))
            .await
            .unwrap();
        println!("\n[probe:compaction-io] optimize report: {report:?}");
        assert!(
            report.fragments_removed >= 2 && report.fragments_added < report.fragments_removed,
            "no merge happened, so nothing below is evidence about observability. \
             This is the trap that mis-deferred the index lever: a probe that \
             measures nothing must not conclude 'unobservable'. report={report:?}"
        );

        let mut after = Vec::new();
        for (name, filter) in SHAPES {
            for t in 0..TRIALS {
                after.push((name, t, scan_counts(&backend, table, filter).await));
            }
        }

        println!("| shape    | trial | iops b/a  | requests b/a | bytes_read b/a |");
        let mut iops_dropped = 0;
        let mut requests_dropped = 0;
        for ((name, t, b), (_, _, a)) in before.iter().zip(after.iter()) {
            println!(
                "| {name:<8} | {t}     | {:>3} / {:<3} | {:>3} / {:<8} | {:>7} / {:<7} |",
                b.iops, a.iops, b.requests, a.requests, b.bytes_read, a.bytes_read
            );
            if a.iops < b.iops {
                iops_dropped += 1;
            }
            if a.requests < b.requests {
                requests_dropped += 1;
            }
        }

        let n = before.len();
        // The denominator. Without I/O before, a zero delta says nothing —
        // the same argument `scans_reported` exists for.
        assert!(
            before.iter().all(|(_, _, c)| c.iops > 0),
            "no I/O was reported at all, so the comparison below is vacuous"
        );

        println!(
            "\nverdict: iops strictly lower on {iops_dropped}/{n} trials, \
             requests on {requests_dropped}/{n}"
        );

        // Clause 2 of the decision rule: **100%**, not a majority. An
        // intermittent witness fails the oracle's 80% activation floor for
        // reasons having nothing to do with the transition, which is why the
        // existing levers pin 1.0 in their own tests.
        assert_eq!(
            (iops_dropped, requests_dropped),
            (n, n),
            "the drop is not unanimous, so `iops` is not a dependable witness"
        );

        // Clause 4: the delta must **scale with how much was merged**. A
        // counter that moves the same amount whether five fragments merged or
        // two is a `> 0` constant wearing a number, and a lever built on it
        // would report activation it did not earn.
        let two_frag = "probe_io_small";
        backend
            .create_table(two_frag, vec![test_batch(vec![0], vec![0])])
            .await
            .unwrap();
        backend
            .write(
                two_frag,
                vec![test_batch((100..200).collect(), (0..100).collect())],
                WriteMode::Append,
            )
            .await
            .unwrap();

        let small_before = scan_counts(&backend, two_frag, None).await;
        let small_report = backend
            .optimize_table(two_frag, std::time::Duration::from_secs(7 * 24 * 3600))
            .await
            .unwrap();
        assert!(
            small_report.fragments_removed >= 2,
            "the two-fragment table did not merge: {small_report:?}"
        );
        let small_after = scan_counts(&backend, two_frag, None).await;

        let big_drop = before[0].2.iops - after[0].2.iops;
        let small_drop = small_before.iops - small_after.iops;
        println!(
            "scaling: {} fragments merged -> iops {} -> {} (drop {big_drop}); \
             {} fragments merged -> iops {} -> {} (drop {small_drop})",
            report.fragments_removed,
            before[0].2.iops,
            after[0].2.iops,
            small_report.fragments_removed,
            small_before.iops,
            small_after.iops
        );
        assert!(
            big_drop > small_drop,
            "merging {} fragments moved `iops` no more than merging {} did, so \
             the counter reports that compaction happened but not how much. \
             big={big_drop}, small={small_drop}",
            report.fragments_removed,
            small_report.fragments_removed
        );

        // Clause 3: not a cold-open artefact. Production opens a fresh
        // `Dataset` per scan (`LanceDirectory::open` always loads), so the
        // repeated trials above are already the production shape — but read
        // twice through **one** handle to record what a future dataset cache
        // would do to this witness. Reported, not asserted: it does not change
        // today's verdict, and pinning it would pin an implementation detail.
        let dataset = backend.directory.open(table).await.unwrap();
        let mut warm = Vec::new();
        for _ in 0..2 {
            let seen: Arc<Mutex<Counts>> = Arc::new(Mutex::new(Counts::default()));
            let sink = Arc::clone(&seen);
            let mut scanner = dataset.scan();
            scanner.scan_stats_callback(Arc::new(
                move |s: &lance::dataset::scanner::ExecutionSummaryCounts| {
                    sink.lock().unwrap().iops += s.iops;
                },
            ));
            let mut stream = scanner.try_into_stream().await.unwrap();
            while let Some(b) = stream.next().await {
                b.unwrap();
            }
            let g = seen.lock().unwrap();
            warm.push(g.iops);
        }
        println!("warm re-read through one handle: iops {warm:?}");
    }

    /// `LanceDirectory` reimplements lancedb's table listing and path layout
    /// from its source (`database/listing.rs:724,941`). Those are compatibility
    /// contracts, not APIs, so assert equivalence directly against lancedb
    /// rather than against a hand-written expectation: create tables through
    /// the lancedb path, then require both sides to agree.
    ///
    /// If lancedb ever changes its layout, this fails loudly instead of
    /// silently detaching primary reads from fork branch reads.
    #[tokio::test]
    async fn lance_directory_listing_matches_lancedb() {
        use crate::backend::lance_directory::LanceDirectory;

        let (dir, backend) = create_test_backend().await;
        let uri = dir.path().to_str().unwrap();
        let directory = LanceDirectory::connect(uri, None).await.unwrap();

        // A fresh directory: both must report no tables. `read_dir` on a
        // never-written base path must not be an error.
        assert!(directory.table_names().await.unwrap().is_empty());

        // Names chosen to exercise sort order and uni's real naming scheme
        // (`vertices_{label}`, `adjacency_{type}_{dir}`), including an
        // underscore-heavy name and one that sorts before the others.
        for name in [
            "vertices_Person",
            "adjacency_KNOWS_fwd",
            "deltas_KNOWS_bwd",
            "vertices_Zebra",
        ] {
            backend
                .create_table(name, vec![test_batch(vec![1], vec![10])])
                .await
                .unwrap();
        }

        let via_lancedb = backend.table_names().await.unwrap();
        let via_directory = directory.table_names().await.unwrap();

        let mut expected = via_lancedb.clone();
        expected.sort();
        assert_eq!(
            via_directory, expected,
            "LanceDirectory listing diverged from lancedb's: {via_directory:?} vs {expected:?}"
        );

        // Every listed name must resolve to an openable dataset — this is what
        // makes primary and the fork branch path agree on the layout.
        for name in &via_directory {
            directory.open(name).await.unwrap();
        }
    }

    #[tokio::test]
    async fn lock_table_for_write_provides_mutual_exclusion() {
        // The MUVERA FDE backfill holds this guard across its scan→overwrite so a
        // concurrent flush append cannot interleave and be lost (issue #96). Prove the
        // guard actually serializes two holders of the same table name: a second
        // acquisition must not proceed while the first is held, and a different table
        // name must not block.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let (_dir, backend) = create_test_backend().await;
        let backend = Arc::new(backend);

        let held = backend.lock_table_for_write("vertices_Doc").await;

        // A different table name is independent — acquiring it must not block.
        let other = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            backend.lock_table_for_write("vertices_Other"),
        )
        .await;
        assert!(
            other.is_ok(),
            "a different table's lock must be independent"
        );
        drop(other);

        // A second acquisition of the SAME name must block until the first is dropped.
        let entered = Arc::new(AtomicBool::new(false));
        let b2 = Arc::clone(&backend);
        let e2 = Arc::clone(&entered);
        let waiter = tokio::spawn(async move {
            let _g = b2.lock_table_for_write("vertices_Doc").await;
            e2.store(true, Ordering::SeqCst);
        });

        // While we hold the guard, the waiter must not have acquired it.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !entered.load(Ordering::SeqCst),
            "second holder acquired the same-name lock while it was still held"
        );

        drop(held);
        // Now the waiter can proceed.
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("waiter did not acquire the lock after release")
            .unwrap();
        assert!(entered.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_table_lifecycle() {
        let (_dir, backend) = create_test_backend().await;

        // Create empty table
        backend
            .create_empty_table("test", test_schema())
            .await
            .unwrap();
        assert!(backend.table_exists("test").await.unwrap());

        let names = backend.table_names().await.unwrap();
        assert!(names.contains(&"test".to_string()));

        // Drop table
        backend.drop_table("test").await.unwrap();
        assert!(!backend.table_exists("test").await.unwrap());
    }

    #[tokio::test]
    async fn test_scan_with_filter() {
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1, 2, 3], vec![100, 200, 300])])
            .await
            .unwrap();

        // Scan all
        let batches = backend.scan(ScanRequest::all("test")).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);

        // Scan with filter
        let batches = backend
            .scan(ScanRequest::all("test").with_filter(FilterExpr::compare(
                "id",
                CmpOp::Gt,
                Scalar::Int(1),
            )))
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    /// Fail-closed contract (review C1): a scan against an existing table that
    /// errors (here: an unparsable SQL filter) must surface as `Err`, never be
    /// silently masked into `Ok(vec![])` — otherwise the MERGE existence-check
    /// would read "no rows" and create a duplicate. A scan against a table that
    /// simply doesn't exist still legitimately returns an empty result.
    #[tokio::test]
    async fn test_scan_propagates_errors_but_tolerates_missing_table() {
        let (_dir, backend) = create_test_backend().await;

        // Missing table → empty, not an error.
        let batches = backend.scan(ScanRequest::all("never_created")).await;
        assert!(
            matches!(batches, Ok(ref b) if b.is_empty()),
            "scan of a non-existent table must be Ok(empty), got {batches:?}"
        );

        backend
            .create_table("test", vec![test_batch(vec![1, 2, 3], vec![100, 200, 300])])
            .await
            .unwrap();

        // A real scan failure on an existing table (unparsable filter referencing
        // a non-existent column) must propagate as Err, not collapse to empty.
        let result = backend
            .scan(
                ScanRequest::all("test")
                    .with_filter(FilterExpr::equals("no_such_column", Scalar::Int(1))),
            )
            .await;
        assert!(
            result.is_err(),
            "a scan failure on an existing table must propagate as Err, got Ok"
        );
    }

    #[tokio::test]
    async fn test_write_append_and_overwrite() {
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1, 2], vec![100, 200])])
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 2);

        // Append
        backend
            .write(
                "test",
                vec![test_batch(vec![3], vec![300])],
                WriteMode::Append,
            )
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 3);

        // Overwrite
        backend
            .write(
                "test",
                vec![test_batch(vec![10], vec![1000])],
                WriteMode::Overwrite,
            )
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_replace_table_atomic() {
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1, 2, 3], vec![100, 200, 300])])
            .await
            .unwrap();

        // Replace with new data
        backend
            .replace_table_atomic(
                "test",
                vec![test_batch(vec![4, 5], vec![400, 500])],
                test_schema(),
            )
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_version_and_rollback() {
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1], vec![100])])
            .await
            .unwrap();

        let v1 = backend.get_table_version("test").await.unwrap().unwrap();
        assert!(v1 > 0);

        // Append to create a new version
        backend
            .write(
                "test",
                vec![test_batch(vec![2], vec![200])],
                WriteMode::Append,
            )
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 2);

        // Rollback to v1
        backend.rollback_table("test", v1).await.unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_recover_staging() {
        let (_dir, backend) = create_test_backend().await;

        // No staging table — should be a no-op
        backend.recover_staging("test").await.unwrap();
        assert!(!backend.table_exists("test").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_table_schema() {
        let (_dir, backend) = create_test_backend().await;

        // Non-existent table
        assert!(backend.get_table_schema("missing").await.unwrap().is_none());

        // Create table and check schema
        backend
            .create_empty_table("test", test_schema())
            .await
            .unwrap();
        let schema = backend.get_table_schema("test").await.unwrap().unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        // The `table_cache` was removed for async-flush correctness
        // (see `get_or_open_table` doc comment). `invalidate_cache`
        // and `clear_cache` are still public on the backend trait but
        // are no-ops on `table_cache` now (they retain the legacy
        // signature for callers). This test now just exercises that
        // scan-then-invalidate doesn't error out.
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1], vec![100])])
            .await
            .unwrap();

        let _ = backend.scan(ScanRequest::all("test")).await.unwrap();
        backend.invalidate_cache("test"); // no-op now, just check it doesn't panic
        let _ = backend.scan(ScanRequest::all("test")).await.unwrap();
        backend.clear_cache();
        let _ = backend.scan(ScanRequest::all("test")).await.unwrap();
    }
}
