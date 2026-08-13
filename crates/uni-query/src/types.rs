// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Query result types and value re-exports.
//!
//! Core value types ([`Value`], [`Node`], [`Edge`], [`Path`]) are defined in
//! `uni_common::value` and re-exported here for backward compatibility.
//! Query-specific types ([`Row`], [`QueryResult`], [`QueryCursor`]) remain here.

use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use uni_common::{Result, UniError};

// Re-export core value types from uni-common.
#[doc(inline)]
pub use uni_common::value::{Edge, FromValue, Node, Path, Value};

/// Timing metrics collected during query execution.
///
/// All durations default to zero until the execution pipeline populates them.
#[derive(Debug, Clone, Default)]
pub struct QueryMetrics {
    /// Time spent parsing the query string into an AST.
    pub parse_time: Duration,
    /// Time spent planning (logical plan generation).
    pub plan_time: Duration,
    /// Time spent executing the plan.
    pub exec_time: Duration,
    /// Wall-clock time from query submission to result.
    pub total_time: Duration,
    /// Number of rows returned to the caller.
    pub rows_returned: usize,
    /// Number of rows examined by scans, before filtering and projection.
    pub rows_scanned: usize,
    /// Number of bytes read from storage.
    ///
    /// **Still unpopulated — always 0.** No planned consumer needs it; see the
    /// note on [`Self::cache_hits`].
    pub bytes_read: usize,
    /// Whether the plan was served from cache.
    ///
    /// **Write path only.** The sole assignment site is the transaction
    /// execution path; on the read path (`Session::query`) this is permanently
    /// `false`, because the read plan cache is session-local and reports its hits
    /// on `SessionMetrics::plan_cache_hits` instead. Do not use this to detect a
    /// warm cache on a read — use the session-metrics delta.
    pub plan_cache_hit: bool,
    /// Number of rows served from an L0 buffer (in-memory, unflushed).
    pub l0_reads: usize,
    /// Number of rows served from L1 / Lance storage.
    pub storage_reads: usize,
    /// Number of cache hits during execution.
    ///
    /// **Still unpopulated — always 0.** Kept for shape compatibility. A field
    /// that exists and always reads zero is a trap: anything asserting on it
    /// compiles and silently never fires, so treat this as absent until a
    /// consumer justifies wiring it.
    pub cache_hits: usize,
    /// Number of scans that executed against a fork's Lance branch.
    ///
    /// Counts what *executed*, not what was configured: `BranchedBackend`
    /// resolves a branch per call and falls back to primary for tables the fork
    /// has never written, so a forked session can run a query with this at 0.
    pub branch_scans: u64,
    /// Number of reads that executed against a pinned time-travel snapshot.
    ///
    /// Counts only manifest-pinned snapshot reads, never an ordinary read-write
    /// transaction's version pin.
    pub snapshot_reads: u64,
}

/// Single result row from a query.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column names shared across all rows in a result set.
    pub(crate) columns: Arc<Vec<String>>,
    /// Column values for this row.
    pub(crate) values: Vec<Value>,
}

impl Row {
    /// Create a new row from columns and values.
    pub fn new(columns: Arc<Vec<String>>, values: Vec<Value>) -> Self {
        Self { columns, values }
    }

    /// Returns the column names for this row.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the column values for this row.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consumes the row, returning the column values.
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    /// Gets a typed value by column name.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Query` if the column is missing,
    /// or `UniError::Type` if it cannot be converted.
    pub fn get<T: FromValue>(&self, column: &str) -> Result<T> {
        let idx = self
            .columns
            .iter()
            .position(|c| c == column)
            .ok_or_else(|| UniError::Query {
                message: format!("Column '{}' not found", column),
                query: None,
            })?;
        self.get_idx(idx)
    }

    /// Gets a typed value by column index.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Query` if the index is out of bounds,
    /// or `UniError::Type` if it cannot be converted.
    pub fn get_idx<T: FromValue>(&self, index: usize) -> Result<T> {
        if index >= self.values.len() {
            return Err(UniError::Query {
                message: format!("Column index {} out of bounds", index),
                query: None,
            });
        }
        T::from_value(&self.values[index])
    }

    /// Tries to get a typed value, returning `None` on failure.
    pub fn try_get<T: FromValue>(&self, column: &str) -> Option<T> {
        self.get(column).ok()
    }

    /// Gets the raw `Value` by column name.
    pub fn value(&self, column: &str) -> Option<&Value> {
        let idx = self.columns.iter().position(|c| c == column)?;
        self.values.get(idx)
    }

    /// Returns all column-value pairs as a map.
    pub fn as_map(&self) -> HashMap<&str, &Value> {
        self.columns
            .iter()
            .zip(&self.values)
            .map(|(col, val)| (col.as_str(), val))
            .collect()
    }

    /// Converts this row to a JSON object.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self.as_map()).unwrap_or(serde_json::Value::Null)
    }
}

impl std::ops::Index<usize> for Row {
    type Output = Value;
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

/// Warnings emitted during query execution.
///
/// Warnings indicate potential issues but do not prevent the query from
/// completing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum QueryWarning {
    /// An index is unavailable (e.g., still being rebuilt).
    IndexUnavailable {
        /// The label that the index is for.
        label: String,
        /// The name of the unavailable index.
        index_name: String,
        /// Reason the index is unavailable.
        reason: String,
    },
    /// A property filter could not use an index.
    NoIndexForFilter {
        /// The label being filtered.
        label: String,
        /// The property being filtered.
        property: String,
    },
    /// RRF fusion was requested in point-computation context where no global
    /// ranking is available, so it degenerated to equal-weight fusion.
    RrfPointContext,
    /// Generic warning message.
    Other(String),
}

impl std::fmt::Display for QueryWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryWarning::IndexUnavailable {
                label,
                index_name,
                reason,
            } => {
                write!(
                    f,
                    "Index '{}' on label '{}' is unavailable: {}",
                    index_name, label, reason
                )
            }
            QueryWarning::NoIndexForFilter { label, property } => {
                write!(
                    f,
                    "No index available for filter on {}.{}, using full scan",
                    label, property
                )
            }
            QueryWarning::RrfPointContext => {
                write!(
                    f,
                    "RRF fusion degenerated to equal-weight fusion in point-computation context \
                     (no global ranking available). Consider using method: 'weighted' with explicit weights."
                )
            }
            QueryWarning::Other(msg) => write!(f, "{}", msg),
        }
    }
}

/// Collection of query result rows.
#[derive(Debug)]
pub struct QueryResult {
    /// Column names shared across all rows.
    pub(crate) columns: Arc<Vec<String>>,
    /// Result rows.
    pub(crate) rows: Vec<Row>,
    /// Warnings emitted during query execution.
    pub(crate) warnings: Vec<QueryWarning>,
    /// Execution timing metrics.
    pub(crate) metrics: QueryMetrics,
}

impl QueryResult {
    /// Create a new query result.
    #[doc(hidden)]
    pub fn new(
        columns: Arc<Vec<String>>,
        rows: Vec<Row>,
        warnings: Vec<QueryWarning>,
        metrics: QueryMetrics,
    ) -> Self {
        Self {
            columns,
            rows,
            warnings,
            metrics,
        }
    }

    /// Returns the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if there are no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns all rows.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Consumes the result, returning the rows.
    pub fn into_rows(self) -> Vec<Row> {
        self.rows
    }

    /// Returns an iterator over the rows.
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    /// Returns warnings emitted during execution.
    pub fn warnings(&self) -> &[QueryWarning] {
        &self.warnings
    }

    /// Returns `true` if the query produced any warnings.
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Returns execution timing metrics.
    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }

    /// Update the parse timing and total time on the metrics.
    ///
    /// Used when the parse phase happens outside `execute_ast_internal` (e.g.,
    /// in `execute_internal_with_config` which parses first, then delegates).
    #[doc(hidden)]
    pub fn update_parse_timing(
        &mut self,
        parse_time: std::time::Duration,
        total_time: std::time::Duration,
    ) {
        self.metrics.parse_time = parse_time;
        self.metrics.total_time = total_time;
    }

    /// Fill in parse and plan timing measured upstream of result assembly.
    ///
    /// The cached read path parses and plans in the session layer and only then
    /// calls into execution, so the assembling function has no way to observe
    /// either duration. Without this the two fields report zero on every cached
    /// read — measurements taken and thrown away.
    #[doc(hidden)]
    pub fn set_frontend_timing(
        &mut self,
        parse_time: std::time::Duration,
        plan_time: std::time::Duration,
    ) {
        self.metrics.parse_time = parse_time;
        self.metrics.plan_time = plan_time;
    }

    /// Attach execution counters harvested from the executor.
    ///
    /// Takes the tuple shape `Executor::take_counters` returns.
    #[doc(hidden)]
    pub fn set_counters(&mut self, counters: (usize, usize, usize, u64, u64)) {
        let (l0_reads, storage_reads, rows_scanned, branch_scans, snapshot_reads) = counters;
        self.metrics.l0_reads = l0_reads;
        self.metrics.storage_reads = storage_reads;
        self.metrics.rows_scanned = rows_scanned;
        self.metrics.branch_scans = branch_scans;
        self.metrics.snapshot_reads = snapshot_reads;
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

/// Result of a write operation (CREATE, SET, DELETE, etc.).
#[derive(Debug)]
pub struct ExecuteResult {
    /// Number of entities affected.
    pub(crate) affected_rows: usize,
    /// Number of nodes created.
    pub(crate) nodes_created: usize,
    /// Number of nodes deleted.
    pub(crate) nodes_deleted: usize,
    /// Number of relationships created.
    pub(crate) relationships_created: usize,
    /// Number of relationships deleted.
    pub(crate) relationships_deleted: usize,
    /// Number of properties set.
    pub(crate) properties_set: usize,
    /// Number of labels added.
    pub(crate) labels_added: usize,
    /// Number of labels removed.
    pub(crate) labels_removed: usize,
    /// Execution timing metrics.
    pub(crate) metrics: QueryMetrics,
}

impl ExecuteResult {
    /// Create a new execute result with only an affected row count.
    ///
    /// All per-type counters default to zero. Use [`with_details`](Self::with_details)
    /// to populate detailed mutation statistics.
    #[doc(hidden)]
    pub fn new(affected_rows: usize) -> Self {
        Self {
            affected_rows,
            nodes_created: 0,
            nodes_deleted: 0,
            relationships_created: 0,
            relationships_deleted: 0,
            properties_set: 0,
            labels_added: 0,
            labels_removed: 0,
            metrics: QueryMetrics::default(),
        }
    }

    /// Create an execute result with detailed per-type mutation counters and metrics.
    #[doc(hidden)]
    pub fn with_details(
        affected_rows: usize,
        stats: &uni_store::runtime::l0::MutationStats,
        metrics: QueryMetrics,
    ) -> Self {
        Self {
            affected_rows,
            nodes_created: stats.nodes_created,
            nodes_deleted: stats.nodes_deleted,
            relationships_created: stats.relationships_created,
            relationships_deleted: stats.relationships_deleted,
            properties_set: stats.properties_set,
            labels_added: stats.labels_added,
            labels_removed: stats.labels_removed,
            metrics,
        }
    }

    /// Returns the number of affected entities.
    pub fn affected_rows(&self) -> usize {
        self.affected_rows
    }

    /// Returns the number of nodes created.
    pub fn nodes_created(&self) -> usize {
        self.nodes_created
    }

    /// Returns the number of nodes deleted.
    pub fn nodes_deleted(&self) -> usize {
        self.nodes_deleted
    }

    /// Returns the number of relationships created.
    pub fn relationships_created(&self) -> usize {
        self.relationships_created
    }

    /// Returns the number of relationships deleted.
    pub fn relationships_deleted(&self) -> usize {
        self.relationships_deleted
    }

    /// Returns the number of properties set.
    pub fn properties_set(&self) -> usize {
        self.properties_set
    }

    /// Returns the number of labels added.
    pub fn labels_added(&self) -> usize {
        self.labels_added
    }

    /// Returns the number of labels removed.
    pub fn labels_removed(&self) -> usize {
        self.labels_removed
    }

    /// Returns execution timing metrics.
    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }
}

/// Cursor-based result streaming for large result sets.
pub struct QueryCursor {
    /// Column names shared across all rows.
    pub(crate) columns: Arc<Vec<String>>,
    /// Async stream of row batches.
    pub(crate) stream: Pin<Box<dyn Stream<Item = Result<Vec<Row>>> + Send>>,
}

impl QueryCursor {
    /// Create a new query cursor.
    #[doc(hidden)]
    pub fn new(
        columns: Arc<Vec<String>>,
        stream: Pin<Box<dyn Stream<Item = Result<Vec<Row>>> + Send>>,
    ) -> Self {
        Self { columns, stream }
    }

    /// Returns the column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Fetches the next batch of rows.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<Row>>> {
        use futures::StreamExt;
        self.stream.next().await
    }

    /// Consumes all remaining rows into a single vector.
    ///
    /// # Errors
    ///
    /// Returns the first error encountered while streaming.
    pub async fn collect_remaining(mut self) -> Result<Vec<Row>> {
        use futures::StreamExt;
        let mut rows = Vec::new();
        while let Some(batch_res) = self.stream.next().await {
            rows.extend(batch_res?);
        }
        Ok(rows)
    }
}
