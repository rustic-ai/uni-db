// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Apply (correlated subquery) execution plan for DataFusion.
//!
//! Implements `CALL { ... }` subqueries by executing the subquery once per
//! input row, injecting the input row's columns as parameters, and cross-joining
//! the results.
//!
//! # Semantics
//!
//! For each row from the input plan:
//! 1. Optionally filter via `input_filter`
//! 2. Inject the input row's columns as parameters
//! 3. Re-plan and execute the subquery with those parameters
//! 4. Cross-join: merge each subquery result row with the input row
//!
//! If input produces zero rows (after filtering), execute the subquery once
//! with the base parameters (standalone CALL support).

use crate::query::df_graph::common::{
    arrow_err, collect_all_partitions, compute_plan_properties, execute_subplan, extract_row_params,
};
use crate::query::df_graph::{GraphExecutionContext, MutationContext};
use crate::query::planner::LogicalPlan;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::Stream;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::{Expr, UnaryOp};
use uni_store::storage::manager::StorageManager;

/// Apply (correlated subquery) execution plan.
///
/// The input is pre-planned as a physical plan (executed directly).
/// The subquery is stored as a **logical** plan and re-planned per row at runtime
/// with correlated parameters injected.
/// Handles both `SubqueryCall` (no input_filter) and `Apply` (with input_filter).
pub struct GraphApplyExec {
    /// Physical plan for the driving input (e.g., MATCH scan).
    /// Pre-planned at construction time to preserve property context.
    input_exec: Arc<dyn ExecutionPlan>,

    /// Logical plan for the correlated subquery (re-planned per row).
    subquery_plan: LogicalPlan,

    /// Optional pre-filter applied to input rows before subquery execution.
    input_filter: Option<Expr>,

    /// Graph execution context shared with sub-planners.
    graph_ctx: Arc<GraphExecutionContext>,

    /// DataFusion session context.
    session_ctx: Arc<RwLock<SessionContext>>,

    /// Storage manager for creating sub-planners.
    storage: Arc<StorageManager>,

    /// Schema for label/edge type lookups.
    schema_info: Arc<UniSchema>,

    /// Query parameters.
    params: HashMap<String, Value>,

    /// Output schema (merged: surviving input columns + subquery columns).
    /// Subquery fields override input fields of the same name.
    output_schema: SchemaRef,

    /// Indices into `input_exec.schema()` for input columns that survive the
    /// schema merge (i.e., their name is NOT also in the subquery's output).
    /// Pre-computed at construction so the per-row hot path avoids re-deriving
    /// the filter. The leading `kept_input_indices.len()` columns of
    /// `output_schema` correspond 1:1 to these input indices.
    kept_input_indices: Arc<[usize]>,

    /// Parallel to `kept_input_indices`: when `Some((var, prop))`, the kept
    /// input column `var.prop` must be refreshed from `sub_row[var]`'s Map
    /// instead of sliced from the input batch. This carries SET-mutated
    /// dotted columns from the subquery's post-SET Map across the Apply
    /// boundary so the outer plan's `RETURN v.prop` sees the updated value.
    kept_input_overrides: Arc<[Option<(String, String)>]>,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

    /// Outer mutation context, threaded into the per-row sub-planner so that
    /// `CALL { ... SET/CREATE/MERGE/DELETE ... }` writes route through the
    /// same transaction's L0 buffer. `None` for read-only outer plans.
    mutation_ctx: Option<Arc<MutationContext>>,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphApplyExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphApplyExec")
            .field("has_input_filter", &self.input_filter.is_some())
            .finish()
    }
}

impl GraphApplyExec {
    /// Create a new Apply execution plan.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input_exec: Arc<dyn ExecutionPlan>,
        subquery_plan: LogicalPlan,
        input_filter: Option<Expr>,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        output_schema: SchemaRef,
        kept_input_indices: Vec<usize>,
        kept_input_overrides: Vec<Option<(String, String)>>,
        mutation_ctx: Option<Arc<MutationContext>>,
    ) -> Self {
        let properties = compute_plan_properties(output_schema.clone());

        Self {
            input_exec,
            subquery_plan,
            input_filter,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            kept_input_indices: kept_input_indices.into(),
            kept_input_overrides: kept_input_overrides.into(),
            properties,
            mutation_ctx,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for GraphApplyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphApplyExec: filter={}",
            if self.input_filter.is_some() {
                "yes"
            } else {
                "none"
            }
        )
    }
}

impl ExecutionPlan for GraphApplyExec {
    fn name(&self) -> &str {
        "GraphApplyExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // No physical children — sub-plans are re-planned at execution time
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "GraphApplyExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let input_exec = self.input_exec.clone();
        let subquery_plan = self.subquery_plan.clone();
        let input_filter = self.input_filter.clone();
        let graph_ctx = self.graph_ctx.clone();
        let session_ctx = self.session_ctx.clone();
        let storage = self.storage.clone();
        let schema_info = self.schema_info.clone();
        let params = self.params.clone();
        let output_schema = self.output_schema.clone();
        let kept_input_indices = self.kept_input_indices.clone();
        let kept_input_overrides = self.kept_input_overrides.clone();
        let mutation_ctx = self.mutation_ctx.clone();

        let fut = async move {
            run_apply(
                input_exec,
                &subquery_plan,
                input_filter.as_ref(),
                &graph_ctx,
                &session_ctx,
                &storage,
                &schema_info,
                &params,
                &output_schema,
                &kept_input_indices,
                &kept_input_overrides,
                mutation_ctx.as_ref(),
            )
            .await
        };

        Ok(Box::pin(ApplyStream {
            state: ApplyStreamState::Running(Box::pin(fut)),
            schema: self.output_schema.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Core apply logic
// ---------------------------------------------------------------------------

/// Convert record batches into row-oriented `HashMap<String, Value>` representation.
fn batches_to_row_maps(batches: &[RecordBatch]) -> Vec<HashMap<String, Value>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row_idx| extract_row_params(batch, row_idx))
        })
        .collect()
}

/// Evaluate a Cypher filter expression against a row.
///
/// Supports simple binary comparisons and boolean operations needed for
/// input_filter pushdown (e.g., `p.age > 30`, `p.status = 'active'`).
fn evaluate_filter(filter: &Expr, row: &HashMap<String, Value>) -> bool {
    match filter {
        Expr::BinaryOp { left, op, right } => {
            use uni_cypher::ast::BinaryOp;
            match op {
                BinaryOp::And => evaluate_filter(left, row) && evaluate_filter(right, row),
                BinaryOp::Or => evaluate_filter(left, row) || evaluate_filter(right, row),
                _ => {
                    let left_val = resolve_expr_value(left, row);
                    let right_val = resolve_expr_value(right, row);
                    evaluate_comparison(op, left_val.as_ref(), right_val.as_ref())
                }
            }
        }
        Expr::UnaryOp {
            op: UnaryOp::Not,
            expr,
        } => !evaluate_filter(expr, row),
        _ => {
            // Truth-test any other expression on its resolved value. A shape the
            // fast-path cannot evaluate (`None`) is "unknown", which for a
            // pre-filter means KEEP the row — never silently drop it (mirrors the
            // `_ => true` operator backstop in `evaluate_comparison`). A resolved
            // value, including a genuine NULL, is truth-tested per Cypher 3VL.
            match resolve_expr_value(filter, row) {
                Some(val) => val.as_bool().unwrap_or(false),
                None => true,
            }
        }
    }
}

/// Resolve a simple expression to a Value using the row context.
///
/// Returns `Some(value)` for the shapes the fast-path input-filter evaluator
/// understands (literal, bare variable, or `var.key` property). Returns `None`
/// for any other shape (arithmetic, `IN`, `CASE`, function calls, a property on
/// a non-variable base, …) — i.e. "cannot evaluate", which callers must treat as
/// "keep the row", never as a silent drop. This distinguishes an unevaluable
/// shape from a genuine resolved `Value::Null`.
fn resolve_expr_value(expr: &Expr, row: &HashMap<String, Value>) -> Option<Value> {
    match expr {
        Expr::Literal(lit) => Some(lit.to_value()),
        Expr::Variable(name) => Some(row.get(name).cloned().unwrap_or(Value::Null)),
        Expr::Property(base_expr, key) => {
            if let Expr::Variable(var) = base_expr.as_ref() {
                // Look up "var.key" in the row map
                let col_name = format!("{}.{}", var, key);
                Some(row.get(&col_name).cloned().unwrap_or(Value::Null))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Compare two Values for ordering.
fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        // Exact i64-vs-f64 order (no lossy `as f64` cast above 2^53); NaN keeps
        // the prior `partial_cmp`-None (unordered) result.
        (Value::Int(a), Value::Float(b)) => {
            if b.is_nan() {
                None
            } else {
                Some(uni_common::cmp_i64_f64(*a, *b))
            }
        }
        (Value::Float(a), Value::Int(b)) => {
            if a.is_nan() {
                None
            } else {
                Some(uni_common::cmp_i64_f64(*b, *a).reverse())
            }
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Evaluate a binary comparison operator on two operands.
///
/// Handles equality (`Eq`, `NotEq`) directly and delegates ordering
/// comparisons (`Lt`, `LtEq`, `Gt`, `GtEq`) to [`compare_values`]. Either operand
/// being `None` means the fast-path could not evaluate that side (e.g. an
/// arithmetic or `IN`/`CASE` sub-expression); such a comparison is "unknown" and
/// KEEPS the row — a pre-filter must never silently drop a row it cannot evaluate.
fn evaluate_comparison(
    op: &uni_cypher::ast::BinaryOp,
    left: Option<&Value>,
    right: Option<&Value>,
) -> bool {
    use std::cmp::Ordering;
    use uni_cypher::ast::BinaryOp;

    let (Some(left), Some(right)) = (left, right) else {
        return true;
    };

    match op {
        BinaryOp::Eq => left == right,
        BinaryOp::NotEq => left != right,
        BinaryOp::Lt => compare_values(left, right) == Some(Ordering::Less),
        BinaryOp::LtEq => matches!(
            compare_values(left, right),
            Some(Ordering::Less | Ordering::Equal)
        ),
        BinaryOp::Gt => compare_values(left, right) == Some(Ordering::Greater),
        BinaryOp::GtEq => matches!(
            compare_values(left, right),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        // Any operator this fast-path evaluator does not implement (STARTS WITH,
        // CONTAINS, IN, `=~`, arithmetic, ...) must be treated as "unknown", which
        // for a pre-filter means KEEP the row — never silently drop it. Returning
        // `false` here previously discarded matching rows. In production such
        // shapes are not pushed into `input_filter` at all (the planner's
        // `apply_input_filter_supported` gate keeps them as a residual Filter that
        // evaluates the full grammar); this branch is the defensive backstop.
        _ => true,
    }
}

/// Build a typed column from row maps using a builder and value extractor.
///
/// For each row, looks up `col_name`, applies `extract` to get an `Option<T>`,
/// and appends the value or null to the builder.
/// The value for `col_name` in `row`, deriving `{var}.{prop}` from the entity
/// bound to `var` when the flat column is absent.
///
/// A row carries an entity either as a `Value::Map` — from which the planner's
/// dotted columns were split out alongside it — or natively, in which case those
/// dotted columns were never materialised and `{var}._vid` is simply missing.
/// Reading it flat then yields a null, and a non-nullable `{var}._vid` fails
/// batch construction outright (#234).
fn row_column(row: &HashMap<String, Value>, col_name: &str) -> Option<Value> {
    if let Some(v) = row.get(col_name) {
        return Some(v.clone());
    }
    let (var, prop) = col_name.split_once('.')?;
    let derived = row.get(var)?.entity_property(prop);
    (!derived.is_null()).then_some(derived)
}

fn build_column<B, T>(
    rows: &[HashMap<String, Value>],
    col_name: &str,
    mut builder: B,
    extract: impl Fn(&Value) -> Option<T>,
) -> ArrayRef
where
    B: arrow_array::builder::ArrayBuilder,
    B: PrimitiveAppend<T>,
{
    for row in rows {
        match row_column(row, col_name).as_ref().and_then(&extract) {
            Some(v) => builder.append_typed_value(v),
            None => builder.append_typed_null(),
        }
    }
    Arc::new(builder.finish_to_array())
}

/// Trait to abstract over typed append for primitive Arrow builders.
///
/// This avoids repeating the same get-value/convert/append-or-null pattern
/// for each numeric/boolean type in `rows_to_batch`.
trait PrimitiveAppend<T> {
    fn append_typed_value(&mut self, val: T);
    fn append_typed_null(&mut self);
    fn finish_to_array(self) -> ArrayRef;
}

macro_rules! impl_primitive_append {
    ($builder:ty, $native:ty, $array:ty) => {
        impl PrimitiveAppend<$native> for $builder {
            fn append_typed_value(&mut self, val: $native) {
                self.append_value(val);
            }
            fn append_typed_null(&mut self) {
                self.append_null();
            }
            fn finish_to_array(mut self) -> ArrayRef {
                Arc::new(self.finish()) as ArrayRef
            }
        }
    };
}

impl_primitive_append!(UInt64Builder, u64, arrow_array::UInt64Array);
impl_primitive_append!(Int64Builder, i64, arrow_array::Int64Array);
impl_primitive_append!(Int32Builder, i32, arrow_array::Int32Array);
impl_primitive_append!(Float64Builder, f64, arrow_array::Float64Array);
impl_primitive_append!(BooleanBuilder, bool, arrow_array::BooleanArray);

/// Build a RecordBatch from merged row maps using the output schema.
fn rows_to_batch(rows: &[HashMap<String, Value>], schema: &SchemaRef) -> DFResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let num_rows = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name();
        let col = match field.data_type() {
            DataType::UInt64 => build_column(
                rows,
                col_name,
                UInt64Builder::with_capacity(num_rows),
                |v| v.as_u64().or_else(|| v.as_i64().map(|i| i as u64)),
            ),
            DataType::Int64 => build_column(
                rows,
                col_name,
                Int64Builder::with_capacity(num_rows),
                Value::as_i64,
            ),
            DataType::Int32 => {
                build_column(rows, col_name, Int32Builder::with_capacity(num_rows), |v| {
                    v.as_i64().map(|i| i as i32)
                })
            }
            DataType::Float64 => build_column(
                rows,
                col_name,
                Float64Builder::with_capacity(num_rows),
                Value::as_f64,
            ),
            DataType::Boolean => build_column(
                rows,
                col_name,
                BooleanBuilder::with_capacity(num_rows),
                Value::as_bool,
            ),
            DataType::LargeBinary => {
                let mut builder = arrow_array::builder::LargeBinaryBuilder::with_capacity(
                    num_rows,
                    num_rows * 64,
                );
                for row in rows {
                    match row_column(row, col_name) {
                        Some(val) if !val.is_null() => {
                            let cv_bytes = uni_common::cypher_value_codec::encode(&val);
                            builder.append_value(&cv_bytes);
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::List(inner_field) if inner_field.data_type() == &DataType::Utf8 => {
                let mut builder = arrow_array::builder::ListBuilder::new(StringBuilder::new());
                for row in rows {
                    match row_column(row, col_name) {
                        Some(Value::List(items)) => {
                            for item in &items {
                                match item {
                                    Value::String(s) => builder.values().append_value(s),
                                    Value::Null => builder.values().append_null(),
                                    other => builder.values().append_value(format!("{other}")),
                                }
                            }
                            builder.append(true);
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Null => Arc::new(arrow_array::NullArray::new(num_rows)) as ArrayRef,
            // Default: Utf8 for everything else
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in rows {
                    match row.get(col_name) {
                        Some(Value::Null) | None => builder.append_null(),
                        Some(Value::String(s)) => builder.append_value(s),
                        Some(other) => builder.append_value(format!("{other}")),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
        };
        columns.push(col);
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(arrow_err)
}

/// Slice a single row, projecting only the input columns whose name survives
/// the Apply schema merge (i.e., not overridden by a subquery RETURN column).
fn slice_kept_row(batch: &RecordBatch, row_idx: usize, kept: &[usize]) -> Vec<ArrayRef> {
    kept.iter()
        .map(|&i| batch.column(i).slice(row_idx, 1))
        .collect()
}

/// Check if a logical plan is or contains a ProcedureCall node.
/// This helps distinguish procedure calls (CALL...YIELD) from regular subqueries (CALL { ... }).
fn is_procedure_call(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ProcedureCall { .. } => true,
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => is_procedure_call(input),
        _ => false,
    }
}

/// Recursively check whether a logical plan contains any write operation.
///
/// Subqueries that mutate state must execute once per correlated input row;
/// the IN-list batching optimization is safe only for read-only subqueries.
fn plan_contains_writes(plan: &LogicalPlan) -> bool {
    use crate::query::planner::LogicalPlan as LP;
    match plan {
        LP::Create { .. }
        | LP::CreateBatch { .. }
        | LP::Merge { .. }
        | LP::Delete { .. }
        | LP::Set { .. }
        | LP::Remove { .. }
        | LP::Foreach { .. } => true,
        LP::Project { input, .. }
        | LP::Filter { input, .. }
        | LP::Sort { input, .. }
        | LP::Limit { input, .. }
        | LP::Distinct { input }
        | LP::Unwind { input, .. }
        | LP::Aggregate { input, .. } => plan_contains_writes(input),
        LP::Apply {
            input, subquery, ..
        }
        | LP::SubqueryCall { input, subquery } => {
            plan_contains_writes(input) || plan_contains_writes(subquery)
        }
        _ => false,
    }
}

/// Build an owned, canonical cache key for a row's correlation parameters.
///
/// A `BTreeMap` makes the key order-independent and gives real `Hash` + `Eq`
/// over `Value`. The previous implementation keyed the dedup cache by a bare
/// `u64` `DefaultHasher` digest of `format!("{val:?}")` with NO equality
/// re-check, so any hash collision (or two values whose Debug renders
/// identically) returned another row's subquery results. (review H7)
fn canonical_params_key(params: &HashMap<String, Value>) -> BTreeMap<String, Value> {
    params.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Check if batching is eligible for this apply operation.
/// Returns true if:
/// - There are 2+ filtered entries (single row → existing path)
/// - At least one `._vid` correlation key exists
fn is_batch_eligible(filtered_entries: &[(&RecordBatch, usize, HashMap<String, Value>)]) -> bool {
    if filtered_entries.len() < 2 {
        return false;
    }

    // Check if at least one correlation key (._vid) exists
    filtered_entries
        .iter()
        .any(|(_, _, row_params)| row_params.keys().any(|k| k.ends_with("._vid")))
}

/// Run the apply operation: execute input, filter, correlate subquery, merge results.
///
/// Uses Arrow-native row slicing for input columns to preserve complex types
/// (Struct, List, etc.), and only converts to Value for parameter injection.
#[expect(clippy::too_many_arguments)]
async fn run_apply(
    input_exec: Arc<dyn ExecutionPlan>,
    subquery_plan: &LogicalPlan,
    input_filter: Option<&Expr>,
    graph_ctx: &Arc<GraphExecutionContext>,
    session_ctx: &Arc<RwLock<SessionContext>>,
    storage: &Arc<StorageManager>,
    schema_info: &Arc<UniSchema>,
    params: &HashMap<String, Value>,
    output_schema: &SchemaRef,
    kept_input_indices: &[usize],
    kept_input_overrides: &[Option<(String, String)>],
    mutation_ctx: Option<&Arc<MutationContext>>,
) -> DFResult<RecordBatch> {
    let apply_start = std::time::Instant::now();
    let is_proc_call = is_procedure_call(subquery_plan);
    tracing::debug!("run_apply: is_procedure_call={}", is_proc_call);

    // 1. Execute pre-planned input physical plan directly
    let task_ctx = session_ctx.read().task_ctx();
    let input_batches = collect_all_partitions(&input_exec, task_ctx).await?;

    // 2. Collect (batch_ref, row_idx) for rows that pass the input filter,
    //    along with their Value-based params for subquery injection.
    let mut filtered_entries: Vec<(&RecordBatch, usize, HashMap<String, Value>)> = Vec::new();
    for batch in &input_batches {
        for row_idx in 0..batch.num_rows() {
            let row_params = extract_row_params(batch, row_idx);
            if let Some(filter) = input_filter
                && !evaluate_filter(filter, &row_params)
            {
                continue;
            }
            filtered_entries.push((batch, row_idx, row_params));
        }
    }

    tracing::debug!(
        "run_apply: filtered_entries count = {}",
        filtered_entries.len()
    );

    let subquery_has_writes = plan_contains_writes(subquery_plan);

    // 3. Handle empty input: execute subquery once with base params.
    //
    // For unit subqueries (no RETURN, schema has no subquery fields) we skip
    // the call entirely: with zero outer rows there's nothing to drive
    // per-row side effects, and correlated parameter resolution would fail
    // (`Unresolved parameter: $n`). The same logic applies to any
    // write-bearing subquery — running it once with no outer correlation
    // would either fail to resolve params or write phantom rows.
    let is_unit_subquery = output_schema.fields().len() == kept_input_indices.len();
    if filtered_entries.is_empty() {
        if is_unit_subquery || subquery_has_writes {
            return Ok(RecordBatch::new_empty(output_schema.clone()));
        }
        let sub_batches = execute_subplan(
            subquery_plan,
            params,
            &HashMap::new(), // No outer values for empty input case
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            mutation_ctx,
        )
        .await?;
        let sub_rows = batches_to_row_maps(&sub_batches);
        return rows_to_batch(&sub_rows, output_schema);
    }

    // 4. Check if we can batch the subplan execution
    // IMPORTANT: Only batch when NOT a procedure call AND has input_filter.
    // - Procedure calls use outer_values (not params), incompatible with batching
    // - No input_filter indicates CALL subquery (e.g., MATCH (p) CALL { MATCH (p) })
    //   which requires per-row correlation, not batching
    // - Target pattern: procedure call → Apply with filter → MATCH traversal
    let has_filter = input_filter.is_some();

    if is_batch_eligible(&filtered_entries) && !is_proc_call && has_filter && !subquery_has_writes {
        tracing::debug!("run_apply: batching eligible, attempting batch execution");

        // Collect unique VID values and build batched params
        let mut vid_values: HashMap<String, Vec<Value>> = HashMap::new();
        for (_, _, row_params) in &filtered_entries {
            for (key, value) in row_params {
                if key.ends_with("._vid") {
                    vid_values
                        .entry(key.clone())
                        .or_default()
                        .push(value.clone());
                }
            }
        }

        // Build batched params: VID keys become Value::List
        let mut batched_params = params.clone();
        for (key, values) in &vid_values {
            batched_params.insert(key.clone(), Value::List(values.clone()));
        }

        // Add carry-through parameters from first row (for literals in projections)
        // These won't affect the WHERE filter but ensure planning succeeds
        if let Some((_, _, first_row_params)) = filtered_entries.first() {
            for (key, value) in first_row_params {
                if !key.ends_with("._vid") {
                    batched_params
                        .entry(key.clone())
                        .or_insert_with(|| value.clone());
                }
            }
        }

        // Execute subquery ONCE with batched VID params
        let subplan_start = std::time::Instant::now();
        let sub_batches = execute_subplan(
            subquery_plan,
            &batched_params,
            &HashMap::new(),
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            mutation_ctx,
        )
        .await?;
        let subplan_elapsed = subplan_start.elapsed();
        tracing::debug!(
            "run_apply: batch execute_subplan took {:?}",
            subplan_elapsed
        );

        // Build hash index: VID → Vec<subquery result rows>
        let sub_rows = batches_to_row_maps(&sub_batches);
        let mut sub_index: HashMap<i64, Vec<&HashMap<String, Value>>> = HashMap::new();

        // Find the VID key (should be the same for all rows)
        let vid_key = vid_values.keys().next().expect("at least one VID key");

        for sub_row in &sub_rows {
            if let Some(Value::Int(vid)) = sub_row.get(vid_key) {
                sub_index.entry(*vid).or_default().push(sub_row);
            }
        }

        // Hash-join: for each input row, look up by VID, emit input+subquery columns.
        // `kept_input_indices` filters out input columns whose names are
        // overridden by subquery RETURN columns.
        let num_input_cols = kept_input_indices.len();
        let num_output_cols = output_schema.fields().len();
        let mut column_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_output_cols];

        for (batch, row_idx, row_params) in &filtered_entries {
            // Extract VID from row params
            let input_vid = if let Some(Value::Int(vid)) = row_params.get(vid_key) {
                *vid
            } else {
                continue; // Skip if VID is not present
            };

            let input_row_arrays = slice_kept_row(batch, *row_idx, kept_input_indices);

            // Look up matching subquery rows by VID
            if let Some(matching_sub_rows) = sub_index.get(&input_vid) {
                for sub_row in matching_sub_rows {
                    append_cross_join_row(
                        &mut column_arrays,
                        &input_row_arrays,
                        sub_row,
                        output_schema,
                        num_input_cols,
                        kept_input_overrides,
                        is_unit_subquery,
                    )?;
                }
            } else if is_unit_subquery {
                // Unit subquery: side effects (writes) have run as part of the
                // bulk sub-plan execution above; pass the input row through.
                for (col_idx, arr) in input_row_arrays.iter().enumerate() {
                    column_arrays[col_idx].push(arr.clone());
                }
            }
            // else: inner join — skip input row (no subquery matches)
        }

        let result = concat_column_arrays(&column_arrays, output_schema);

        let apply_elapsed = apply_start.elapsed();
        tracing::debug!(
            "run_apply: completed (batched) in {:?}, 1 subplan execution",
            apply_elapsed
        );

        return result;
    }

    // 5. Fallback: For each input row, execute subquery and collect output column arrays.
    //    Used when batching is not eligible (single row, no VID keys, or procedure call).
    //    Each output row is: surviving input columns (sliced via
    //    `kept_input_indices`) + subquery columns. Input columns whose name is
    //    overridden by a subquery RETURN are dropped here so the merged
    //    `output_schema` matches the data layout.
    let num_input_cols = kept_input_indices.len();
    let num_output_cols = output_schema.fields().len();
    // Accumulate per-column arrays for all output rows
    let mut column_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_output_cols];

    let mut total_subplan_time = std::time::Duration::ZERO;
    let mut subplan_executions = 0;

    // Cache to deduplicate subplan executions for identical row parameters.
    // Keyed by the owned, sorted params (real Hash + Eq) — not a bare u64 hash —
    // so a hash collision can never serve a different row's results. (review H7)
    let mut subplan_cache: HashMap<BTreeMap<String, Value>, Vec<HashMap<String, Value>>> =
        HashMap::new();
    let mut cache_hits = 0;

    for (batch, row_idx, row_params) in &filtered_entries {
        // For procedure calls (CALL...YIELD), pass row_params as outer_values to avoid
        // shadowing user parameters. For regular subqueries (CALL { ... }), merge them
        // into parameters for backward compatibility with correlated variables.
        let (sub_params, sub_outer_values) = if is_procedure_call(subquery_plan) {
            // Procedure call: keep params separate from outer values
            (params.clone(), row_params.clone())
        } else {
            // Regular subquery: merge outer values into params (old behavior)
            let mut merged = params.clone();
            merged.extend(row_params.clone());
            (merged, HashMap::new())
        };

        // Check cache for identical row params. NEVER dedup a subquery that
        // WRITES: its side effects (e.g. `UNWIND [1,1,1] AS x CALL { CREATE (:N) }`)
        // must run once per outer row, so a params-keyed cache hit would execute
        // them only once. The cache is read-only-subquery-only.
        let params_key = canonical_params_key(row_params);
        let cached = if subquery_has_writes {
            None
        } else {
            subplan_cache.get(&params_key)
        };
        let sub_rows = if let Some(cached_rows) = cached {
            // Cache hit: reuse previous results
            cache_hits += 1;
            tracing::debug!("run_apply: cache hit for row params, skipping execute_subplan");
            cached_rows.clone()
        } else {
            // Cache miss: execute subplan
            let subplan_start = std::time::Instant::now();
            let sub_batches = execute_subplan(
                subquery_plan,
                &sub_params,
                &sub_outer_values,
                graph_ctx,
                session_ctx,
                storage,
                schema_info,
                mutation_ctx,
            )
            .await?;
            let subplan_elapsed = subplan_start.elapsed();
            total_subplan_time += subplan_elapsed;
            subplan_executions += 1;

            tracing::debug!(
                "run_apply: execute_subplan #{} took {:?}",
                subplan_executions,
                subplan_elapsed
            );

            let rows = batches_to_row_maps(&sub_batches);
            // Only memoize read-only subqueries (see above).
            if !subquery_has_writes {
                subplan_cache.insert(params_key, rows.clone());
            }
            rows
        };

        let input_row_arrays = slice_kept_row(batch, *row_idx, kept_input_indices);

        if sub_rows.is_empty() {
            if is_unit_subquery {
                // Unit subquery: side effects have executed; pass the input
                // row through (no subquery columns to append).
                for (col_idx, arr) in input_row_arrays.iter().enumerate() {
                    column_arrays[col_idx].push(arr.clone());
                }
            }
            // else: inner-join semantics — skip this input row.
            continue;
        }

        for sub_row in &sub_rows {
            append_cross_join_row(
                &mut column_arrays,
                &input_row_arrays,
                sub_row,
                output_schema,
                num_input_cols,
                kept_input_overrides,
                is_unit_subquery,
            )?;
        }
    }

    // 5. Concatenate all accumulated arrays per column
    let result = concat_column_arrays(&column_arrays, output_schema);

    let apply_elapsed = apply_start.elapsed();
    tracing::debug!(
        "run_apply: completed in {:?}, {} subplan executions, {} cache hits, {:?} total subplan time",
        apply_elapsed,
        subplan_executions,
        cache_hits,
        total_subplan_time
    );

    result
}

/// Build a single-row Arrow array from a builder and optional value.
fn single_row_array<B, T>(mut builder: B, val: Option<T>) -> ArrayRef
where
    B: PrimitiveAppend<T>,
{
    match val {
        Some(v) => builder.append_typed_value(v),
        None => builder.append_typed_null(),
    }
    builder.finish_to_array()
}

/// Convert a single Value to a single-row Arrow array of the given type.
fn value_to_single_row_array(val: &Value, data_type: &DataType) -> DFResult<ArrayRef> {
    Ok(match data_type {
        DataType::UInt64 => single_row_array(
            UInt64Builder::with_capacity(1),
            val.as_u64().or_else(|| val.as_i64().map(|v| v as u64)),
        ),
        DataType::Int64 => single_row_array(Int64Builder::with_capacity(1), val.as_i64()),
        DataType::Int32 => single_row_array(
            Int32Builder::with_capacity(1),
            val.as_i64().map(|v| v as i32),
        ),
        DataType::Float64 => single_row_array(Float64Builder::with_capacity(1), val.as_f64()),
        DataType::Boolean => single_row_array(BooleanBuilder::with_capacity(1), val.as_bool()),
        DataType::Null => Arc::new(arrow_array::NullArray::new(1)) as ArrayRef,
        DataType::LargeBinary => {
            let mut b = arrow_array::builder::LargeBinaryBuilder::with_capacity(1, 64);
            if val.is_null() {
                b.append_null();
            } else {
                let cv_bytes = uni_common::cypher_value_codec::encode(val);
                b.append_value(&cv_bytes);
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(1, 64);
            match val {
                Value::Null => b.append_null(),
                Value::String(s) => b.append_value(s),
                other => b.append_value(format!("{other}")),
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::List(inner_field) if inner_field.data_type() == &DataType::Utf8 => {
            let mut b = arrow_array::builder::ListBuilder::new(StringBuilder::new());
            match val {
                Value::List(items) => {
                    for item in items {
                        match item {
                            Value::String(s) => b.values().append_value(s),
                            Value::Null => b.values().append_null(),
                            other => b.values().append_value(format!("{other}")),
                        }
                    }
                    b.append(true);
                }
                Value::Null => b.append_null(),
                other => {
                    b.values().append_value(format!("{other}"));
                    b.append(true);
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Struct(fields) => {
            // Encode a graph entity (`Value::Map` / `Value::Node` / `Value::Edge`)
            // into a single-row StructArray matching the declared field
            // layout. Used by the unit-subquery refresh path so the bare
            // entity column reflects post-SET state — `compile_property_access`
            // (expr_compiler.rs) tries struct-field extraction before flat
            // columns, so a stale Struct would shadow our refreshed dotted
            // columns.
            let map_view: Option<&HashMap<String, Value>> = match val {
                Value::Map(m) => Some(m),
                Value::Node(n) => Some(&n.properties),
                Value::Edge(e) => Some(&e.properties),
                _ => None,
            };
            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
            for child_field in fields.iter() {
                let child_val = map_view
                    .and_then(|m| m.get(child_field.name()))
                    .cloned()
                    .unwrap_or(Value::Null);
                child_arrays.push(value_to_single_row_array(
                    &child_val,
                    child_field.data_type(),
                )?);
            }
            let pairs: Vec<(Arc<arrow_schema::Field>, ArrayRef)> =
                fields.iter().cloned().zip(child_arrays).collect();
            Arc::new(arrow_array::StructArray::from(pairs)) as ArrayRef
        }
        _ => {
            debug_assert!(
                false,
                "value_to_single_row_array: unhandled DataType {:?} — mirror the arm in rows_to_batch",
                data_type
            );
            let mut b = StringBuilder::with_capacity(1, 64);
            match val {
                Value::Null => b.append_null(),
                Value::String(s) => b.append_value(s),
                other => b.append_value(format!("{other}")),
            }
            Arc::new(b.finish()) as ArrayRef
        }
    })
}

/// Append one cross-joined row (input + subquery) to the per-column accumulator.
///
/// Input columns use Arrow-native sliced arrays to preserve complex types,
/// EXCEPT:
///   * `kept_input_overrides[i] = Some((var, prop))` — refresh `var.prop`
///     from the subquery's post-SET bare `var` Map in `sub_row` so dotted
///     columns surface fresh values across the Apply boundary.
///   * When `is_unit_subquery` is true, ALSO refresh any kept input column
///     whose name appears as a key in `sub_row`. Unit subqueries (no
///     RETURN, write-only side effects) re-emit the modified outer row
///     under the SAME column names; using the sub_row value gives outer
///     `RETURN v.prop` the post-SET binding even though the unit subquery
///     contributes no explicit RETURN fields.
///
/// Subquery columns convert `Value` to single-row Arrow arrays as before.
fn append_cross_join_row(
    column_arrays: &mut [Vec<ArrayRef>],
    input_row_arrays: &[ArrayRef],
    sub_row: &HashMap<String, Value>,
    output_schema: &SchemaRef,
    num_input_cols: usize,
    kept_input_overrides: &[Option<(String, String)>],
    is_unit_subquery: bool,
) -> DFResult<()> {
    // Add input columns (Arrow-native), with per-column refresh from sub_row
    // when applicable (see fn-doc).
    for (col_idx, arr) in input_row_arrays.iter().enumerate() {
        if let Some(Some((var, prop))) = kept_input_overrides.get(col_idx) {
            let extracted = match sub_row.get(var) {
                Some(Value::Map(m)) => m.get(prop).cloned().unwrap_or(Value::Null),
                Some(Value::Node(n)) => n.properties.get(prop).cloned().unwrap_or(Value::Null),
                Some(Value::Edge(e)) => e.properties.get(prop).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            let field = &output_schema.fields()[col_idx];
            let new_arr = value_to_single_row_array(&extracted, field.data_type())?;
            column_arrays[col_idx].push(new_arr);
            continue;
        }
        if is_unit_subquery {
            // Refresh the kept input column from the subquery's post-SET
            // sub_row. Two cases:
            //   * Dotted (`v.prop`): extract `prop` from `sub_row[v]` (the
            //     subquery emits the modified bare Map under key `v`, not
            //     as dotted columns).
            //   * Bare (`v`): replace with `sub_row[v]` itself, encoded
            //     into the field's declared type. This covers the Struct
            //     case (`value_to_single_row_array` now has a Struct arm)
            //     and is required because `compile_property_access` in
            //     `expr_compiler.rs` tries Struct-field extraction BEFORE
            //     the flat-column fallback — a stale Struct would shadow
            //     refreshed dotted columns.
            let field = &output_schema.fields()[col_idx];
            let refreshed: Option<Value> = if let Some(dot) = field.name().find('.') {
                let base = &field.name()[..dot];
                let prop = &field.name()[dot + 1..];
                // The native arms read only the *property* map, so a system
                // field — `_vid` above all — resolved to `None` and the column
                // kept its stale input value, which for a natively-encoded
                // entity was never materialised at all. A non-nullable
                // `{var}._vid` then failed batch construction (#234).
                match sub_row.get(base) {
                    Some(v @ (Value::Map(_) | Value::Node(_) | Value::Edge(_))) => {
                        let val = v.entity_property(prop);
                        (!val.is_null()).then_some(val)
                    }
                    _ => None,
                }
            } else {
                sub_row.get(field.name()).cloned()
            };
            if let Some(val) = refreshed {
                let new_arr = value_to_single_row_array(&val, field.data_type())?;
                column_arrays[col_idx].push(new_arr);
                continue;
            }
        }
        column_arrays[col_idx].push(arr.clone());
    }

    // Add subquery columns using Value -> Arrow conversion
    let num_output_cols = output_schema.fields().len();
    for (col_arr, field) in column_arrays[num_input_cols..num_output_cols]
        .iter_mut()
        .zip(output_schema.fields()[num_input_cols..num_output_cols].iter())
    {
        let col_name = field.name();
        let val = sub_row.get(col_name).cloned().unwrap_or(Value::Null);
        let arr = value_to_single_row_array(&val, field.data_type())?;
        col_arr.push(arr);
    }
    Ok(())
}

/// Concatenate per-column array accumulators into a single `RecordBatch`.
///
/// Returns an empty batch if no rows were accumulated.
fn concat_column_arrays(
    column_arrays: &[Vec<ArrayRef>],
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    if column_arrays[0].is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let mut final_columns: Vec<ArrayRef> = Vec::with_capacity(column_arrays.len());
    for arrays in column_arrays {
        let refs: Vec<&dyn arrow_array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
        let concatenated = arrow::compute::concat(&refs).map_err(arrow_err)?;
        final_columns.push(concatenated);
    }

    RecordBatch::try_new(output_schema.clone(), final_columns).map_err(arrow_err)
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

/// Stream state for the apply operation.
enum ApplyStreamState {
    /// The apply computation is running.
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    /// Computation completed.
    Done,
}

/// Stream that runs the apply operation and emits the result.
struct ApplyStream {
    state: ApplyStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for ApplyStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        match &mut self.state {
            ApplyStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = ApplyStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = ApplyStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            ApplyStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for ApplyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {

    mod dotted_column_resolution {
        use super::super::row_column;
        use std::collections::HashMap;
        use uni_common::core::id::Eid;
        use uni_common::value::{Edge, Node};
        use uni_common::{Value, Vid};

        /// A dotted column resolves from the entity when it was not materialised.
        ///
        /// The planner splits `{var}._vid` out alongside a map-encoded entity,
        /// so the flat column is there to read. A natively-encoded entity has no
        /// such column, and reading it flat yields a null that a non-nullable
        /// `{var}._vid` then rejects at batch construction.
        #[test]
        fn a_dotted_column_falls_back_to_the_entity() {
            let mut row: HashMap<String, Value> = HashMap::new();
            row.insert(
                "n".to_string(),
                Value::Node(Node {
                    vid: Vid::from(7),
                    labels: vec!["P".into()],
                    properties: HashMap::from([("name".to_string(), Value::String("b".into()))]),
                }),
            );
            assert_eq!(row_column(&row, "n._vid"), Some(Value::Int(7)));
            assert_eq!(row_column(&row, "n.name"), Some(Value::String("b".into())));
            assert_eq!(row_column(&row, "n.absent"), None);
        }

        /// The flat column still wins when it exists, so nothing is recomputed.
        #[test]
        fn an_existing_flat_column_is_preferred() {
            let mut row: HashMap<String, Value> = HashMap::new();
            row.insert("n._vid".to_string(), Value::Int(42));
            row.insert(
                "n".to_string(),
                Value::Node(Node {
                    vid: Vid::from(7),
                    labels: vec![],
                    properties: HashMap::new(),
                }),
            );
            assert_eq!(row_column(&row, "n._vid"), Some(Value::Int(42)));
        }

        /// An edge answers its own system fields too.
        #[test]
        fn an_edge_resolves_its_dotted_columns() {
            let mut row: HashMap<String, Value> = HashMap::new();
            row.insert(
                "r".to_string(),
                Value::Edge(Edge {
                    eid: Eid::from(3),
                    edge_type: "KNOWS".into(),
                    src: Vid::from(0),
                    dst: Vid::from(1),
                    properties: HashMap::new(),
                }),
            );
            assert_eq!(row_column(&row, "r._eid"), Some(Value::Int(3)));
            assert_eq!(
                row_column(&row, "r._type"),
                Some(Value::String("KNOWS".into()))
            );
        }

        /// A non-entity base resolves nothing rather than inventing a value.
        #[test]
        fn a_non_entity_base_resolves_nothing() {
            let mut row: HashMap<String, Value> = HashMap::new();
            row.insert("x".to_string(), Value::Int(1));
            assert_eq!(row_column(&row, "x._vid"), None);
            assert_eq!(row_column(&row, "missing._vid"), None);
        }
    }

    use super::*;

    fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    /// H7: the dedup cache key must distinguish rows by *value equality*, not by
    /// a lossy `u64` hash. Distinct param sets get distinct cache entries;
    /// identical params (regardless of insertion order) collapse to one.
    #[test]
    fn test_canonical_params_key_distinguishes_by_value() {
        let a = canonical_params_key(&params(&[("x", Value::Int(1))]));
        let b = canonical_params_key(&params(&[("x", Value::Int(2))]));
        let c = canonical_params_key(&params(&[("x", Value::Int(1))]));
        assert_ne!(a, b, "different values must yield different keys");
        assert_eq!(a, c, "equal values must yield equal keys");

        // Order-independence: same content, different insertion order → same key.
        let m1 = params(&[("a", Value::Int(1)), ("b", Value::String("z".into()))]);
        let m2 = params(&[("b", Value::String("z".into())), ("a", Value::Int(1))]);
        assert_eq!(canonical_params_key(&m1), canonical_params_key(&m2));

        // Used as a real cache key: two distinct rows never alias each other.
        let mut cache: HashMap<BTreeMap<String, Value>, &str> = HashMap::new();
        cache.insert(a.clone(), "row-1");
        cache.insert(b.clone(), "row-2");
        assert_eq!(cache.get(&a), Some(&"row-1"));
        assert_eq!(cache.get(&b), Some(&"row-2"));
        assert_eq!(cache.len(), 2);
    }

    /// Regression (D5 mirror, apply.rs cross-type Int/Float arm) — FIXED. The
    /// pushdown-filter comparator used to cast `i64 as f64`, collapsing an
    /// integer just above 2^53 onto the float. It now compares exactly via
    /// `cmp_i64_f64`, so `compare_values(Int(2^53+1), Float(2^53.0))` is
    /// `Some(Greater)` and the reverse is `Some(Less)`.
    #[test]
    fn repro_compare_values_int_float_precision_collapse() {
        use std::cmp::Ordering;
        let big_int = Value::Int(9_007_199_254_740_993);
        let float_2p53 = Value::Float(9_007_199_254_740_992.0);
        assert_eq!(
            compare_values(&big_int, &float_2p53),
            Some(Ordering::Greater)
        );
        assert_eq!(compare_values(&float_2p53, &big_int), Some(Ordering::Less));
    }

    // -----------------------------------------------------------------------
    // R6 / uni-query[22]: the input_filter fast-path must never silently DROP a
    // row it cannot confidently evaluate to false. Unknown operators, unknown
    // expression shapes (IN/CASE/function calls), and unresolvable operands
    // (arithmetic) all resolve to "unknown" → KEEP. A genuinely resolved value
    // (including NULL) is still truth-tested per Cypher 3VL. These guard the
    // symmetric keep-on-unknown backstop; in production the planner's
    // `apply_input_filter_supported` gate keeps such shapes out of input_filter.
    // -----------------------------------------------------------------------

    use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr as AstExpr};

    /// `a.name` property access as an AST expression.
    fn prop(var: &str, key: &str) -> AstExpr {
        AstExpr::Property(Box::new(AstExpr::Variable(var.into())), key.into())
    }

    /// A row binding `a.name = "Alice"` and `a.x = 10`.
    fn alice_row() -> HashMap<String, Value> {
        params(&[
            ("a.name", Value::String("Alice".into())),
            ("a.x", Value::Int(10)),
        ])
    }

    #[test]
    fn evaluate_filter_supported_eq_true_keeps_and_false_drops() {
        let row = alice_row();
        let eq_true = AstExpr::BinaryOp {
            left: Box::new(prop("a", "name")),
            op: BinaryOp::Eq,
            right: Box::new(AstExpr::Literal(CypherLiteral::String("Alice".into()))),
        };
        let eq_false = AstExpr::BinaryOp {
            left: Box::new(prop("a", "name")),
            op: BinaryOp::Eq,
            right: Box::new(AstExpr::Literal(CypherLiteral::String("Bob".into()))),
        };
        assert!(evaluate_filter(&eq_true, &row), "TRUE Eq keeps the row");
        assert!(!evaluate_filter(&eq_false, &row), "FALSE Eq drops the row");
    }

    #[test]
    fn evaluate_filter_null_property_3vl_drops() {
        // A resolved-but-NULL property truth-test is not TRUE → row dropped (3VL).
        let row = alice_row(); // no "a.missing"
        assert!(
            !evaluate_filter(&prop("a", "missing"), &row),
            "unresolved property resolves to NULL → truth-test false → drop"
        );
    }

    #[test]
    fn evaluate_filter_unknown_shape_in_list_keeps() {
        // `a.name IN [...]` is a dedicated Expr::In variant the fast-path cannot
        // evaluate → must KEEP, not drop.
        let row = alice_row();
        let in_expr = AstExpr::In {
            expr: Box::new(prop("a", "name")),
            list: Box::new(AstExpr::List(vec![AstExpr::Literal(
                CypherLiteral::String("Zed".into()),
            )])),
        };
        assert!(
            evaluate_filter(&in_expr, &row),
            "unevaluable IN shape must KEEP the row (never silent-drop)"
        );
    }

    #[test]
    fn evaluate_filter_unknown_shape_case_keeps() {
        let row = alice_row();
        let case_expr = AstExpr::Case {
            expr: None,
            when_then: vec![(
                AstExpr::Literal(CypherLiteral::Bool(true)),
                AstExpr::Literal(CypherLiteral::Bool(false)),
            )],
            else_expr: None,
        };
        assert!(
            evaluate_filter(&case_expr, &row),
            "unevaluable CASE shape must KEEP the row"
        );
    }

    #[test]
    fn evaluate_filter_arithmetic_operand_keeps() {
        // `a.x + 1 > 100`: the outer `>` is supported, but the left operand is an
        // arithmetic BinaryOp the fast-path cannot resolve → unknown → KEEP,
        // rather than dropping via a spurious NULL comparison.
        let row = alice_row();
        let arith_cmp = AstExpr::BinaryOp {
            left: Box::new(AstExpr::BinaryOp {
                left: Box::new(prop("a", "x")),
                op: BinaryOp::Add,
                right: Box::new(AstExpr::Literal(CypherLiteral::Integer(1))),
            }),
            op: BinaryOp::Gt,
            right: Box::new(AstExpr::Literal(CypherLiteral::Integer(100))),
        };
        assert!(
            evaluate_filter(&arith_cmp, &row),
            "comparison with an unresolvable arithmetic operand must KEEP the row"
        );
    }

    #[test]
    fn evaluate_filter_unknown_operator_keeps() {
        // A string operator (STARTS WITH) the comparison fast-path does not
        // implement → `_ => true` operator backstop → KEEP.
        let row = alice_row();
        let starts_with = AstExpr::BinaryOp {
            left: Box::new(prop("a", "name")),
            op: BinaryOp::StartsWith,
            right: Box::new(AstExpr::Literal(CypherLiteral::String("Al".into()))),
        };
        assert!(
            evaluate_filter(&starts_with, &row),
            "unimplemented operator must KEEP the row"
        );
    }

    #[test]
    fn evaluate_filter_and_or_not_compose() {
        let row = alice_row();
        let t = AstExpr::BinaryOp {
            left: Box::new(prop("a", "name")),
            op: BinaryOp::Eq,
            right: Box::new(AstExpr::Literal(CypherLiteral::String("Alice".into()))),
        };
        let f = AstExpr::BinaryOp {
            left: Box::new(prop("a", "name")),
            op: BinaryOp::Eq,
            right: Box::new(AstExpr::Literal(CypherLiteral::String("Bob".into()))),
        };
        let and = AstExpr::BinaryOp {
            left: Box::new(t.clone()),
            op: BinaryOp::And,
            right: Box::new(f.clone()),
        };
        let or = AstExpr::BinaryOp {
            left: Box::new(t.clone()),
            op: BinaryOp::Or,
            right: Box::new(f.clone()),
        };
        let not_f = AstExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(f),
        };
        assert!(!evaluate_filter(&and, &row), "TRUE AND FALSE = drop");
        assert!(evaluate_filter(&or, &row), "TRUE OR FALSE = keep");
        assert!(evaluate_filter(&not_f, &row), "NOT FALSE = keep");
    }
}
