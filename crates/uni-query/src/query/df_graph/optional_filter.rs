// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! OPTIONAL MATCH filter with NULL row preservation.
//!
//! This module provides [`OptionalFilterExec`], a DataFusion [`ExecutionPlan`]
//! that applies a filter predicate while preserving OPTIONAL MATCH semantics:
//! when all matched rows for a source group are filtered out, the source row
//! is preserved with NULL values for all optional columns.
//!
//! # Problem
//!
//! Standard `FilterExec` removes rows that fail the predicate. For OPTIONAL
//! MATCH queries like:
//!
//! ```text
//! MATCH (n:Single) OPTIONAL MATCH (n)-[r]-(m) WHERE m:NonExistent RETURN r
//! ```
//!
//! The traverse finds matches (m=A, m=B), but WHERE filters all of them out.
//! A standard filter yields 0 rows. Cypher requires 1 row with r=NULL.
//!
//! # Algorithm
//!
//! 1. Consume each input batch
//! 2. Group rows by source VID columns (non-optional columns with `._vid`
//!    suffix OR struct columns containing a `_vid` field)
//! 3. Evaluate the filter predicate on the batch
//! 4. For each source group:
//!    - If at least one row passes the filter, emit those rows
//!    - If ALL rows fail the filter, emit one row with source columns preserved
//!      and optional columns set to NULL

use crate::query::df_graph::common::{arrow_err, batch_bytes, compute_plan_properties};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, new_null_array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Describes how to extract a grouping value from a column.
#[derive(Debug, Clone)]
enum SourceKeyColumn {
    /// Direct `._vid` column at the given index (e.g., column named `a._vid`).
    FlatVid(usize),
    /// Struct column at index, with `_vid` at the given field index within the struct.
    /// This handles `WITH *` projections that bundle variables into struct columns
    /// (e.g., column `a` containing fields `_vid`, `_labels`, etc.).
    StructVid(usize, usize),
    /// LargeBinary CypherValue blob column representing a node/edge variable.
    /// Used when variables are produced as CypherValue blobs (e.g., from MERGE)
    /// without flat `._vid` columns.
    CypherValueBlob(usize),
}

/// Filter with OPTIONAL MATCH NULL row preservation.
///
/// Applies a filter predicate but ensures that for each distinct source
/// row group, at least one output row is produced. When all matched rows
/// for a source group are filtered out, a single row with NULL optional
/// columns is emitted instead.
pub struct OptionalFilterExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Filter predicate to evaluate.
    predicate: Arc<dyn PhysicalExpr>,

    /// Variable names from the OPTIONAL MATCH pattern.
    ///
    /// Columns matching these variable prefixes (e.g., `m._vid`, `r._eid`)
    /// are set to NULL when the filter removes all matched rows for a
    /// source group.
    optional_variables: HashSet<String>,

    /// Columns the enclosing OPTIONAL MATCH clause created: every column of
    /// the input that did not enter the clause. The NULL row an entering row
    /// gets when nothing matched has all of them NULL.
    created_columns: Vec<usize>,

    /// The created columns that name an entity or a traversal step (`._vid`,
    /// `._eid`, a shortest path's `._path`, `__eid_to_*`, `_hop_count`). Each
    /// is non-null on a real match
    /// and NULL on a carrier row, the row a traversal inside the clause emits
    /// for an entering row it could not extend. A row passes only if all of
    /// them are non-null, so a carrier is never mistaken for a match — even
    /// one a later hop extended, or one this operator's predicate accepts.
    evidence_columns: Vec<usize>,

    /// Output schema (same as input).
    schema: SchemaRef,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for OptionalFilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OptionalFilterExec")
            .field("predicate", &self.predicate)
            .field("optional_variables", &self.optional_variables)
            .finish()
    }
}

impl OptionalFilterExec {
    /// Check whether a column belongs to an optional variable.
    fn is_optional_column_name(optional_variables: &HashSet<String>, col_name: &str) -> bool {
        optional_variables.iter().any(|var| {
            let var = var.as_str();
            // Match the bare variable column itself (struct column)
            col_name == var
            // Match "m._vid", "m.name", etc.
            || col_name.strip_prefix(var).is_some_and(|rest| rest.starts_with('.'))
            // Match the internal EID tracking column "__eid_to_m" exactly: a
            // suffix test also claimed "__eid_to_xm" for optional var `m`.
            || col_name.strip_prefix("__eid_to_") == Some(var)
        })
    }

    /// Create a new optional filter execution plan.
    ///
    /// The `optional_variables` set determines which columns are nulled out
    /// when all rows for a source group are filtered. Columns whose name
    /// starts with `{var}.` for any var in `optional_variables` are treated
    /// as optional.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<dyn PhysicalExpr>,
        optional_variables: HashSet<String>,
    ) -> Self {
        let input_schema = input.schema();
        let created_columns = clause_created_columns(&input, &optional_variables);
        // Only the first column of a name counts: it is the one every reader
        // resolves by name. A later namesake can be something else — a
        // traversal re-bound onto a relationship variable also emits
        // `__rebound_r._eid` as a property placeholder that stays NULL.
        let evidence_columns: Vec<usize> = created_columns
            .iter()
            .copied()
            .filter(|&idx| {
                let name = input_schema.field(idx).name();
                is_evidence_column(name) && input_schema.index_of(name).ok() == Some(idx)
            })
            .collect();
        // OptionalFilter can synthesize NULLs for optional columns even when upstream
        // declared them non-nullable (e.g., reused bound variables in OPTIONAL MATCH).
        // Ensure these columns are nullable in this operator's output schema.
        let fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                if (Self::is_optional_column_name(&optional_variables, f.name())
                    || created_columns.contains(&idx))
                    && !f.is_nullable()
                {
                    Field::new(f.name(), f.data_type().clone(), true)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let properties = compute_plan_properties(Arc::clone(&schema));

        Self {
            input,
            predicate,
            optional_variables,
            created_columns,
            evidence_columns,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Check whether a column belongs to an optional variable.
    fn is_optional_column(&self, col_name: &str) -> bool {
        Self::is_optional_column_name(&self.optional_variables, col_name)
    }

    /// Compute source key columns for grouping.
    ///
    /// Finds columns suitable for source-row grouping:
    /// 1. Flat `._vid` columns (e.g., `a._vid`) that are not optional
    /// 2. Struct columns (e.g., `a`) that contain a `_vid` field and are not optional
    /// 3. LargeBinary CypherValue blob columns for variables without a flat `._vid`
    ///    column (e.g., `b` from MERGE output)
    ///
    /// The struct case handles `WITH *` projections that bundle node variables
    /// into struct columns. The blob case handles MERGE output where variables
    /// are serialized as CypherValue blobs without separate `._vid` columns.
    fn compute_source_key_columns(&self) -> Vec<SourceKeyColumn> {
        // The row's identity as it entered the OPTIONAL MATCH, when the planner
        // tagged it (see `optional_source`). Grouping by node ids instead
        // merged entering rows that differ only in a scalar or that repeat, so
        // one passing row suppressed another row's NULL.
        if let Some(idx) = super::optional_source::newest_source_row_column(&self.schema, |name| {
            self.is_optional_column(name)
        }) {
            return vec![SourceKeyColumn::FlatVid(idx)];
        }
        let mut result = Vec::new();
        let mut covered_vars: HashSet<String> = HashSet::new();

        // First pass: find FlatVid and StructVid columns
        for (idx, field) in self.schema.fields().iter().enumerate() {
            if self.is_optional_column(field.name()) {
                continue;
            }
            if field.name().ends_with("._vid") {
                result.push(SourceKeyColumn::FlatVid(idx));
                if let Some(var_name) = field.name().strip_suffix("._vid") {
                    covered_vars.insert(var_name.to_string());
                }
            } else if let DataType::Struct(struct_fields) = field.data_type() {
                // Look for `_vid` field within struct (e.g., variable `a` has field `_vid`)
                for (fi, sf) in struct_fields.iter().enumerate() {
                    if sf.name() == "_vid" {
                        result.push(SourceKeyColumn::StructVid(idx, fi));
                        covered_vars.insert(field.name().to_string());
                        break;
                    }
                }
            }
        }

        // Second pass: find LargeBinary variable blob columns not yet covered.
        // These are bare variable names (no dots, no `__` prefix) of type LargeBinary
        // that don't have a corresponding `._vid` FlatVid column.
        for (idx, field) in self.schema.fields().iter().enumerate() {
            if self.is_optional_column(field.name()) {
                continue;
            }
            if *field.data_type() == DataType::LargeBinary
                && !field.name().contains('.')
                && !field.name().starts_with("__")
                && !covered_vars.contains(field.name())
            {
                result.push(SourceKeyColumn::CypherValueBlob(idx));
            }
        }

        result
    }
}

impl DisplayAs for OptionalFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let vars: Vec<&str> = self.optional_variables.iter().map(|s| s.as_str()).collect();
        write!(
            f,
            "OptionalFilterExec: predicate={}, optional_vars=[{}]",
            self.predicate,
            vars.join(", ")
        )
    }
}

impl ExecutionPlan for OptionalFilterExec {
    fn name(&self) -> &str {
        "OptionalFilterExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Plan(
                "OptionalFilterExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.predicate),
            self.optional_variables.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Taken before `context` is moved into the child's `execute`.
        let pool = Arc::clone(context.memory_pool());
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Pre-compute which column indices are optional vs source.
        let source_key_columns = self.compute_source_key_columns();
        let optional_col_indices: Vec<usize> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(idx, field)| {
                self.is_optional_column(field.name()) || self.created_columns.contains(idx)
            })
            .map(|(idx, _)| idx)
            .collect();

        // Debug: log schema and source key columns
        tracing::debug!(
            "OptionalFilterExec schema: {:?}",
            self.schema
                .fields()
                .iter()
                .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        );
        tracing::debug!(
            "OptionalFilterExec source_key_columns: {:?}, optional_cols: {:?}",
            source_key_columns,
            optional_col_indices
        );

        Ok(Box::pin(OptionalFilterStream {
            input: input_stream,
            predicate: Arc::clone(&self.predicate),
            schema: Arc::clone(&self.schema),
            source_key_columns,
            optional_col_indices,
            evidence_columns: self.evidence_columns.clone(),
            passed_keys: HashSet::new(),
            pending_null: HashMap::new(),
            pending_order: Vec::new(),
            flushed: false,
            metrics,
            // #261. This operator's buffers are unbounded in the number of
            // distinct source groups, and nothing counted them. It appears six
            // times across the LDBC SNB corpus -- four of them in IC1 alone.
            reservation: MemoryConsumer::new(format!("OptionalFilterExec[{partition}]"))
                .register(&pool),
            held: 0,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Stream implementing the optional filter logic.
struct OptionalFilterStream {
    /// Input stream.
    input: SendableRecordBatchStream,

    /// Filter predicate.
    predicate: Arc<dyn PhysicalExpr>,

    /// Output schema.
    schema: SchemaRef,

    /// Source key columns for grouping (flat VID or struct VID).
    source_key_columns: Vec<SourceKeyColumn>,

    /// Indices of optional columns (nulled for filtered-out groups).
    optional_col_indices: Vec<usize>,

    /// Columns that must all be non-null for a row to count as a match; see
    /// [`OptionalFilterExec::evidence_columns`].
    evidence_columns: Vec<usize>,

    /// Source groups that have already emitted at least one passing row, across
    /// every batch seen so far. A group here never needs a NULL recovery row.
    passed_keys: HashSet<Vec<u8>>,

    /// Source groups seen but not yet passed: a one-row representative batch
    /// (source columns preserved, optional columns NULL) to emit at end-of-stream
    /// if the group never passes. Keyed by source key; iteration order tracked in
    /// `pending_order` for deterministic output.
    pending_null: HashMap<Vec<u8>, RecordBatch>,
    pending_order: Vec<Vec<u8>>,

    /// Set once the input is exhausted and the pending NULL rows have been
    /// flushed, so `poll_next` returns `None` afterward.
    flushed: bool,

    /// Metrics.
    metrics: BaselineMetrics,

    /// Pool reservation for the cross-batch buffers (#261).
    ///
    /// Both `passed_keys` and `pending_null` live to end-of-stream by
    /// construction: a group that fails in this batch may pass in a later one,
    /// so neither decision can be made early. That makes this a semantically
    /// required barrier rather than an unchunked producer -- there is no
    /// bounding step available before the accounting, and what the reservation
    /// buys is a refusal naming this operator in place of a silent overrun.
    reservation: MemoryReservation,

    /// Bytes currently charged to `reservation`.
    held: usize,
}

/// What one entry costs, charged per structure it is stored in.
///
/// A key is stored up to three times -- in `passed_keys`, in `pending_null`, and
/// in `pending_order` -- so each store is charged separately rather than the key
/// being charged once.
fn key_bytes(key: &[u8]) -> usize {
    std::mem::size_of::<Vec<u8>>() + key.len()
}

impl OptionalFilterStream {
    /// Process a single input batch with optional filter semantics.
    ///
    /// Emits only the rows that pass the filter. NULL recovery rows are NOT
    /// emitted here — a source group whose rows span multiple batches must not
    /// produce a NULL row per batch, and must not produce one at all if it passes
    /// in a *later* batch. Instead, groups that fail in this batch are buffered in
    /// `pending_null` and flushed once, at end-of-stream, by [`Self::flush`].
    fn process_batch(&mut self, batch: RecordBatch) -> DFResult<RecordBatch> {
        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        // Evaluate the filter predicate.
        let filter_result = self.predicate.evaluate(&batch)?;
        let filter_array = filter_result.into_array(batch.num_rows())?;
        let predicate_bools = filter_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Filter predicate did not return BooleanArray".to_string(),
                )
            })?;
        // A carrier row is never a match, whatever the predicate says of it.
        let mut passing = predicate_bools.clone();
        for &idx in &self.evidence_columns {
            let bound =
                arrow::compute::is_not_null(batch.column(idx).as_ref()).map_err(arrow_err)?;
            passing = arrow::compute::and(&passing, &bound).map_err(arrow_err)?;
        }
        let filter_bools = &passing;

        // Group rows by source VID values.
        // Key = serialized source VID values, Value = list of row indices.
        let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        let mut group_order: Vec<Vec<u8>> = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let key = self.compute_source_key(&batch, row_idx);
            if !groups.contains_key(&key) {
                group_order.push(key.clone());
            }
            groups.entry(key).or_default().push(row_idx);
        }

        // For each group, collect passing rows and update cross-batch state.
        let mut passed_indices: Vec<usize> = Vec::new();

        for key in &group_order {
            let row_indices = &groups[key];
            let mut any_passed = false;

            for &row_idx in row_indices {
                if filter_bools.is_valid(row_idx) && filter_bools.value(row_idx) {
                    passed_indices.push(row_idx);
                    any_passed = true;
                }
            }

            if any_passed {
                // This group is satisfied; cancel any buffered NULL recovery row.
                if self.passed_keys.insert(key.clone()) {
                    self.reservation.try_grow(key_bytes(key))?;
                    self.held += key_bytes(key);
                }
                if let Some(dropped) = self.pending_null.remove(key) {
                    // The row is gone but its key stays in `pending_order` until
                    // the flush walks past it, so only the batch is given back.
                    let freed = batch_bytes(&dropped) + key_bytes(key);
                    self.reservation.shrink(freed);
                    self.held -= freed;
                }
            } else if !self.passed_keys.contains(key) && !self.pending_null.contains_key(key) {
                // Group failed and has not passed in any prior batch — remember a
                // single representative row (source cols kept, optional cols NULL)
                // to emit at end-of-stream unless a later batch passes it.
                let null_batch = self.build_null_row(&batch, row_indices[0])?;
                // A one-row Arrow batch is dominated by per-column buffer
                // overhead rather than by its single row, so a wide schema makes
                // each of these cost far more than the row it carries -- which
                // is why the charge is the batch's own reported size and not a
                // per-row estimate.
                let grew = batch_bytes(&null_batch) + 2 * key_bytes(key);
                self.reservation.try_grow(grew)?;
                self.held += grew;
                self.pending_null.insert(key.clone(), null_batch);
                self.pending_order.push(key.clone());
            }
        }

        // Build the output batch of passing rows only.
        if passed_indices.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for col in batch.columns() {
            columns.push(take_indices(col, &passed_indices)?);
        }

        self.metrics.record_output(passed_indices.len());

        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)
    }

    /// Build a one-row batch preserving `row_idx`'s source columns and NULLing
    /// every optional column — the deferred NULL recovery row for a source group.
    fn build_null_row(&self, batch: &RecordBatch, row_idx: usize) -> DFResult<RecordBatch> {
        let optional_set: HashSet<usize> = self.optional_col_indices.iter().copied().collect();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            if optional_set.contains(&col_idx) {
                columns.push(new_null_array(field.data_type(), 1));
            } else {
                columns.push(take_indices(batch.column(col_idx), &[row_idx])?);
            }
        }
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)
    }

    /// Emit all buffered NULL recovery rows as one batch, once the input is
    /// exhausted. Groups that never passed in any batch each contribute one row.
    fn flush(&mut self) -> DFResult<Option<RecordBatch>> {
        if self.pending_order.is_empty() {
            return Ok(None);
        }
        let keys = std::mem::take(&mut self.pending_order);
        let mut batches: Vec<RecordBatch> = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(b) = self.pending_null.remove(&key) {
                batches.push(b);
            }
        }
        if batches.is_empty() {
            return Ok(None);
        }
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        // Both the per-group batches and the combined one are live across this
        // call, so the combined batch is charged before the parts are released.
        let combined_bytes = refs.iter().map(|b| batch_bytes(b)).sum::<usize>();
        self.reservation.try_grow(combined_bytes)?;
        self.held += combined_bytes;
        let combined = arrow::compute::concat_batches(&self.schema, refs).map_err(arrow_err)?;
        drop(batches);
        // Everything this operator held is now either in `combined`, which
        // belongs to the consumer from here, or dropped.
        self.reservation.shrink(self.held);
        self.held = 0;
        self.metrics.record_output(combined.num_rows());
        Ok(Some(combined))
    }

    /// Compute a grouping key from source column values for a row.
    fn compute_source_key(&self, batch: &RecordBatch, row_idx: usize) -> Vec<u8> {
        use arrow_array::{LargeBinaryArray, StructArray};

        /// Append a u64 value (or NULL sentinel) to the key buffer.
        fn append_u64_key(key: &mut Vec<u8>, val: Option<u64>) {
            key.extend_from_slice(&val.unwrap_or(u64::MAX).to_le_bytes());
        }

        let mut key = Vec::with_capacity(self.source_key_columns.len() * 8);
        for skc in &self.source_key_columns {
            match skc {
                SourceKeyColumn::FlatVid(col_idx) => {
                    append_u64_key(&mut key, extract_u64_value(batch.column(*col_idx), row_idx));
                }
                SourceKeyColumn::StructVid(col_idx, field_idx) => {
                    let vid = batch
                        .column(*col_idx)
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .and_then(|sa| extract_u64_value(sa.column(*field_idx), row_idx));
                    append_u64_key(&mut key, vid);
                }
                SourceKeyColumn::CypherValueBlob(col_idx) => {
                    let col = batch.column(*col_idx);
                    if let Some(ba) = col.as_any().downcast_ref::<LargeBinaryArray>()
                        && ba.is_valid(row_idx)
                    {
                        let bytes = ba.value(row_idx);
                        // Length-prefix for unambiguous concatenation
                        key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                        key.extend_from_slice(bytes);
                    } else {
                        append_u64_key(&mut key, None);
                    }
                }
            }
        }
        key
    }
}

/// Whether a column identifies an entity or a traversal step, and so is
/// non-null on every row that really matched.
///
/// `{p}._path` is the vid list `GraphShortestPathExec` emits, the only
/// producer of that suffix. Inside an OPTIONAL MATCH the operator keeps a pair
/// with no path as a row whose path is NULL but whose target is a real node, so
/// the target's `._vid` alone would take that row for a match.
fn is_evidence_column(name: &str) -> bool {
    name.ends_with("._vid")
        || name.ends_with("._eid")
        || name.ends_with("._path")
        || name.starts_with("__eid_to_")
        || name == "_hop_count"
}

/// The input columns created inside the OPTIONAL MATCH clause this operator
/// belongs to.
///
/// When the clause's entering rows were tagged (see `optional_source`), the
/// columns that entered are exactly the tagging operator's output — its input
/// plus the row id — and every other column was created inside the clause. The difference is taken as a
/// multiset of names, so a column name repeated by an earlier clause (say a
/// second `_hop_count`) still counts once for each side. Without a tag — a
/// clause with no rows entering it, such as a leading `OPTIONAL MATCH` — the
/// clause's own variables are the created columns.
fn clause_created_columns(
    input: &Arc<dyn ExecutionPlan>,
    optional_variables: &HashSet<String>,
) -> Vec<usize> {
    let schema = input.schema();
    let tag = super::optional_source::newest_source_row_column(&schema, |name| {
        OptionalFilterExec::is_optional_column_name(optional_variables, name)
    })
    .and_then(|idx| super::optional_source::entering_schema(input, schema.field(idx).name()));
    match tag {
        Some(entering) => {
            let mut entered: HashMap<&str, usize> = HashMap::new();
            for field in entering.fields() {
                *entered.entry(field.name().as_str()).or_default() += 1;
            }
            schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(
                    |(idx, field)| match entered.get_mut(field.name().as_str()) {
                        Some(n) if *n > 0 => {
                            *n -= 1;
                            None
                        }
                        _ => Some(idx),
                    },
                )
                .collect()
        }
        None => schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                OptionalFilterExec::is_optional_column_name(optional_variables, f.name())
            })
            .map(|(idx, _)| idx)
            .collect(),
    }
}

/// Extract a u64 value from a column at a given row index.
fn extract_u64_value(col: &dyn Array, row_idx: usize) -> Option<u64> {
    use arrow_array::UInt64Array;
    let vid_array = col.as_any().downcast_ref::<UInt64Array>()?;
    vid_array
        .is_valid(row_idx)
        .then(|| vid_array.value(row_idx))
}

/// Take elements from an array at the given indices.
fn take_indices(array: &ArrayRef, indices: &[usize]) -> DFResult<ArrayRef> {
    let idx_array =
        arrow_array::UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
    arrow::compute::take(array.as_ref(), &idx_array, None).map_err(arrow_err)
}

impl Stream for OptionalFilterStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => match self.process_batch(batch) {
                    // A batch whose rows all fail produces no passing rows; keep
                    // pulling (its NULL recovery rows are flushed at end-of-stream)
                    // rather than emitting a spurious empty batch.
                    Ok(b) if b.num_rows() == 0 => continue,
                    Ok(b) => return Poll::Ready(Some(Ok(b))),
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // Input exhausted: emit the buffered NULL recovery rows once.
                    if self.flushed {
                        return Poll::Ready(None);
                    }
                    self.flushed = true;
                    return match self.flush() {
                        Ok(Some(b)) => Poll::Ready(Some(Ok(b))),
                        Ok(None) => Poll::Ready(None),
                        Err(e) => Poll::Ready(Some(Err(e))),
                    };
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for OptionalFilterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_optional_column() {
        let optional_vars: HashSet<String> =
            ["m".to_string(), "r".to_string()].into_iter().collect();
        let exec = OptionalFilterExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                arrow_schema::Schema::empty(),
            ))),
            Arc::new(datafusion::physical_expr::expressions::Literal::new(
                datafusion::common::ScalarValue::Boolean(Some(true)),
            )),
            optional_vars,
        );

        assert!(exec.is_optional_column("m._vid"));
        assert!(exec.is_optional_column("m.name"));
        assert!(exec.is_optional_column("r._eid"));
        assert!(exec.is_optional_column("r._type"));
        assert!(exec.is_optional_column("r"));
        assert!(exec.is_optional_column("m"));
        assert!(exec.is_optional_column("__eid_to_m"));

        assert!(!exec.is_optional_column("n._vid"));
        assert!(!exec.is_optional_column("n.name"));
        assert!(!exec.is_optional_column("x._vid"));
    }
}
