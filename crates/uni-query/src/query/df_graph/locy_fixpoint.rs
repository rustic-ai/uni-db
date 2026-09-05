// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fixpoint iteration operator for recursive Locy strata.
//!
//! `FixpointExec` drives semi-naive evaluation: it repeatedly evaluates the rules
//! in a recursive stratum, feeding back deltas until no new facts are produced.

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    ScalarKey, arrow_err, collect_all_partitions, compute_plan_properties,
    execute_locy_clause_body, extract_scalar_key,
};
use crate::query::df_graph::locy_best_by::{BestByExec, SortCriterion};
use crate::query::df_graph::locy_errors::LocyRuntimeError;
use crate::query::df_graph::locy_explain::{
    ProofTerm, ProvenanceAnnotation, ProvenanceStore, compute_proof_probability,
};
use crate::query::df_graph::locy_fold::{FoldBinding, FoldExec};
use crate::query::df_graph::locy_priority::PriorityExec;
use crate::query::df_graph::locy_profile::LocyProfileCollector;
use crate::query::df_graph::locy_program::interruption;
use crate::query::executor::core::OperatorStats;
use crate::query::planner::LogicalPlan;
use arrow_array::RecordBatch;
use arrow_row::{RowConverter, SortField};
use arrow_schema::SchemaRef;
use datafusion::common::JoinType;
use datafusion::common::Result as DFResult;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, RwLock as StdRwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::Expr;
use uni_locy::{
    ClassifierRegistry, ModelInvocation, ModelInvocationCache, RuntimeWarning, RuntimeWarningCode,
    SemiringKind,
};
use uni_store::storage::manager::StorageManager;

/// IS-NOT (negation) semantics live in [`super::locy_complement`]; re-exported
/// here so the operator's historical paths keep resolving.
pub use super::locy_complement::{
    apply_anti_join_composite, apply_prob_complement_composite, multiply_prob_factors,
};

// ---------------------------------------------------------------------------
// DerivedScanRegistry — injection point for IS-ref data into subplans
// ---------------------------------------------------------------------------

/// Which view of a rule's derived relation a scan reads (issue #162).
///
/// A recursive rule's own facts exist in two shapes during iteration, and
/// different consumers need different ones:
///
/// * [`Contributions`](DerivedScanView::Contributions) — the pre-fold rows, one
///   per derivation. ALONG's `prev.x` needs these (it carries a value along one
///   *path*, not a KEY's aggregate), and so do self-negated IS NOT and
///   provenance, whose `base_fact_id` hashes must stay joinable with the hashes
///   `record_provenance` records.
/// * [`Folded`](DerivedScanView::Folded) — one row per KEY carrying that KEY's
///   folded value. A clause that folds a child's value needs this; reading
///   contributions instead makes it treat one child as N children, which is
///   issue #162.
///
/// Both views of the same rule can be live at once — a rule may have an ALONG
/// clause reading contributions and another clause folding its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedScanView {
    /// Pre-fold rows, one per derivation.
    Contributions,
    /// KEY-grouped snapshot, one row per KEY carrying its folded value.
    Folded,
}

/// A single entry in the derived scan registry.
///
/// Each entry corresponds to one `LocyDerivedScan` node in the logical plan tree.
/// The `data` handle is shared with the logical plan node so that writing data here
/// makes it visible when the subplan is re-planned and executed.
#[derive(Debug)]
pub struct DerivedScanEntry {
    /// Index matching the `scan_index` in `LocyDerivedScan`.
    pub scan_index: usize,
    /// Name of the rule this scan reads from.
    pub rule_name: String,
    /// Whether this is a self-referential scan (rule references itself).
    pub is_self_ref: bool,
    /// Which view of the rule's relation this scan reads.
    pub view: DerivedScanView,
    /// Shared data handle — write batches here to inject into subplans.
    pub data: Arc<RwLock<Vec<RecordBatch>>>,
    /// Schema of the derived relation.
    pub schema: SchemaRef,
}

/// Registry of derived scan handles for fixpoint iteration.
///
/// During fixpoint, each clause body may reference derived relations via
/// `LocyDerivedScan` nodes. The registry maps scan indices to shared data
/// handles so the fixpoint loop can inject delta/full facts before each
/// iteration.
#[derive(Debug, Default)]
pub struct DerivedScanRegistry {
    entries: Vec<DerivedScanEntry>,
}

impl DerivedScanRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an entry to the registry.
    pub fn add(&mut self, entry: DerivedScanEntry) {
        self.entries.push(entry);
    }

    /// Get an entry by scan index.
    pub fn get(&self, scan_index: usize) -> Option<&DerivedScanEntry> {
        self.entries.iter().find(|e| e.scan_index == scan_index)
    }

    /// Write data into a scan entry's shared handle.
    pub fn write_data(&self, scan_index: usize, batches: Vec<RecordBatch>) {
        if let Some(entry) = self.get(scan_index) {
            let mut guard = entry.data.write();
            *guard = batches;
        }
    }

    /// Get all entries for a given rule name.
    pub fn entries_for_rule(&self, rule_name: &str) -> Vec<&DerivedScanEntry> {
        self.entries
            .iter()
            .filter(|e| e.rule_name == rule_name)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// MonotonicAggState — tracking monotonic aggregates across iterations
// ---------------------------------------------------------------------------

/// Monotonic aggregate binding: maps a fold name to its aggregate
/// trait object and input column.
///
/// Dispatches purely through [`uni_plugin::traits::locy::LocyAggregate`]
/// (`update_step` / `initial_accum_f64` / `is_probability_aggregate` /
/// `is_noisy_or`).
#[derive(Debug, Clone)]
pub struct MonotonicFoldBinding {
    pub fold_name: String,
    pub aggregate: std::sync::Arc<dyn uni_plugin::traits::locy::LocyAggregate>,
    pub input_col_index: usize,
    /// Column name for name-based resolution (more robust than positional index).
    pub input_col_name: Option<String>,
}

/// Tracks monotonic aggregate accumulators across fixpoint iterations.
///
/// After each iteration, accumulators are updated and compared to their previous
/// snapshot. The fixpoint has converged (w.r.t. aggregates) when all accumulators
/// are stable (no change between iterations).
#[derive(Debug)]
pub struct MonotonicAggState {
    /// Current accumulator values keyed by (group_key, fold_name).
    accumulators: HashMap<(Vec<ScalarKey>, String), f64>,
    /// Snapshot from the previous iteration for stability check.
    prev_snapshot: HashMap<(Vec<ScalarKey>, String), f64>,
    /// Bindings describing which aggregates to track.
    bindings: Vec<MonotonicFoldBinding>,
}

impl MonotonicAggState {
    /// Create a new monotonic aggregate state.
    pub fn new(bindings: Vec<MonotonicFoldBinding>) -> Self {
        Self {
            accumulators: HashMap::new(),
            prev_snapshot: HashMap::new(),
            bindings,
        }
    }

    /// Update accumulators with new delta batches.
    ///
    /// Returns `true` if any accumulator value changed. When `strict` is
    /// `true`, MNOR/MPROD inputs outside `[0, 1]` produce an error
    /// instead of being clamped.
    ///
    /// `semiring_kind` selects the probability semiring for probability
    /// aggregates: `AddMultProb` (default, Phase 1/2 noisy-OR/product) or
    /// `MaxMinProb` (Viterbi/fuzzy — opt-in, callers emit
    /// `FuzzyNotProbabilistic`).
    ///
    /// Dispatch goes through each binding's `Arc<dyn LocyAggregate>` trait
    /// object via [`uni_plugin::traits::locy::LocyAggregate::update_step`].
    /// The trait object's `initial_accum_f64()` seeds the per-group
    /// accumulator. Under `MaxMinProb`, probability aggregates (MNOR /
    /// MPROD) bypass `update_step` and fold via the `MaxMinProb` semiring's
    /// `plus` (max) / `times` (min) instead — preserving the opt-in
    /// Viterbi/fuzzy semantics that `update_step`'s built-in noisy-OR /
    /// product path does not implement.
    ///
    /// Aggregates whose `update_step` returns `Err(CODE_UNKNOWN_FUNCTION)`
    /// (default impl — no row-level fast path; e.g., `AVG`, `COLLECT`)
    /// are skipped silently here — those run through the batch-shape
    /// [`uni_plugin::traits::locy::LocyAggState::ingest`] path in
    /// `apply_post_fixpoint_chain` instead.
    pub fn update(
        &mut self,
        key_indices: &[usize],
        delta_batches: &[RecordBatch],
        strict: bool,
        semiring_kind: SemiringKind,
    ) -> DFResult<bool> {
        let mut changed = false;
        for batch in delta_batches {
            for row_idx in 0..batch.num_rows() {
                let group_key = extract_scalar_key(batch, key_indices, row_idx);
                for binding in &self.bindings {
                    let idx = binding
                        .input_col_name
                        .as_ref()
                        .and_then(|name| batch.schema().index_of(name).ok())
                        .unwrap_or(binding.input_col_index);
                    if idx >= batch.num_columns() {
                        continue;
                    }
                    let col = batch.column(idx);
                    let val = extract_f64(col.as_ref(), row_idx);
                    if let Some(val) = val {
                        let map_key = (group_key.clone(), binding.fold_name.clone());
                        let initial = binding.aggregate.initial_accum_f64().unwrap_or(0.0);
                        let entry = self.accumulators.entry(map_key).or_insert(initial);
                        let old = *entry;
                        // Under `MaxMinProb`, probability aggregates (MNOR /
                        // MPROD) fold via the Viterbi/fuzzy semiring (max /
                        // min) rather than the trait object's built-in
                        // noisy-OR / product `update_step`. The inline domain
                        // checks below preserve the exact strict-mode error
                        // and clamp-warning literals. `is_noisy_or()`
                        // distinguishes MNOR (disjunction → max) from MPROD
                        // (conjunction → min). All other aggregates — and the
                        // default `AddMultProb` semiring — dispatch through
                        // the trait object's `update_step`.
                        if matches!(semiring_kind, SemiringKind::MaxMinProb)
                            && binding.aggregate.is_probability_aggregate()
                        {
                            use uni_locy::LocySemiring;
                            let sr = uni_locy::MaxMinProb;
                            let is_nor = binding.aggregate.is_noisy_or();
                            let label = if is_nor { "MNOR" } else { "MPROD" };
                            if strict && !(0.0..=1.0).contains(&val) {
                                return Err(datafusion::error::DataFusionError::Execution(
                                    format!(
                                        "strict_probability_domain: {label} input {val} is outside [0, 1]"
                                    ),
                                ));
                            }
                            if !strict && !(0.0..=1.0).contains(&val) {
                                tracing::warn!(
                                    "{label} input {val} outside [0,1], clamped to {}",
                                    val.clamp(0.0, 1.0)
                                );
                            }
                            let p = val.clamp(0.0, 1.0);
                            // MaxMinProb: MNOR -> max (plus), MPROD -> min (times).
                            *entry = if is_nor {
                                sr.plus(entry, &p)
                            } else {
                                sr.times(entry, &p)
                            };
                            if (*entry - old).abs() > f64::EPSILON {
                                changed = true;
                            }
                            continue;
                        }
                        match binding.aggregate.update_step(*entry, val, strict) {
                            Ok(new_val) => {
                                *entry = new_val;
                                if (*entry - old).abs() > f64::EPSILON {
                                    changed = true;
                                }
                            }
                            Err(e) if e.code == uni_plugin::FnError::CODE_UNKNOWN_FUNCTION => {
                                // Aggregate has no row-level fast path (AVG,
                                // COLLECT). Those run through the
                                // batch-shape `ingest` path elsewhere; skip.
                            }
                            Err(e) => {
                                // Strict-mode probability-domain violation,
                                // or another aggregate-specific failure.
                                return Err(datafusion::error::DataFusionError::Execution(
                                    e.message,
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(changed)
    }

    /// Take a snapshot of current accumulators for stability comparison.
    pub fn snapshot(&mut self) {
        self.prev_snapshot = self.accumulators.clone();
    }

    /// Check if accumulators are stable (no change since last snapshot).
    pub fn is_stable(&self) -> bool {
        if self.accumulators.len() != self.prev_snapshot.len() {
            return false;
        }
        for (key, val) in &self.accumulators {
            match self.prev_snapshot.get(key) {
                Some(prev) if (*val - *prev).abs() <= f64::EPSILON => {}
                _ => return false,
            }
        }
        true
    }

    /// Test-only accessor for accumulator values.
    #[cfg(test)]
    pub(crate) fn get_accumulator(&self, key: &(Vec<ScalarKey>, String)) -> Option<f64> {
        self.accumulators.get(key).copied()
    }
}

/// Extract f64 value from an Arrow column at a given row index.
fn extract_f64(col: &dyn arrow_array::Array, row_idx: usize) -> Option<f64> {
    if col.is_null(row_idx) {
        return None;
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Float64Array>() {
        Some(arr.value(row_idx))
    } else {
        col.as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .map(|arr| arr.value(row_idx) as f64)
    }
}

// ---------------------------------------------------------------------------
// RowDedupState — Arrow RowConverter-based persistent dedup set
// ---------------------------------------------------------------------------

/// Arrow-native row deduplication using [`RowConverter`].
///
/// Unlike the legacy `HashSet<Vec<ScalarKey>>` approach, this struct maintains a
/// persistent `seen` set across iterations so per-iteration cost is O(M) where M
/// is the number of candidate rows — the full facts table is never re-scanned.
struct RowDedupState {
    converter: RowConverter,
    seen: HashSet<Box<[u8]>>,
}

impl RowDedupState {
    /// Try to build a `RowDedupState` for the given schema.
    ///
    /// Returns `None` if any column type is not supported by `RowConverter`
    /// (triggers legacy fallback).
    fn try_new(schema: &SchemaRef) -> Option<Self> {
        let fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        match RowConverter::new(fields) {
            Ok(converter) => Some(Self {
                converter,
                seen: HashSet::new(),
            }),
            Err(e) => {
                tracing::warn!(
                    "RowDedupState: RowConverter unsupported for schema, falling back to legacy dedup: {}",
                    e
                );
                None
            }
        }
    }

    /// Populate the seen set from existing fact batches.
    ///
    /// Used after BEST BY in-loop pruning replaces the fact set, so that delta
    /// computation in subsequent iterations correctly recognizes surviving facts.
    fn ingest_existing(&mut self, facts: &[RecordBatch], _schema: &SchemaRef) {
        self.seen.clear();
        for batch in facts {
            if batch.num_rows() == 0 {
                continue;
            }
            let arrays: Vec<_> = batch.columns().to_vec();
            if let Ok(rows) = self.converter.convert_columns(&arrays) {
                for row_idx in 0..batch.num_rows() {
                    let row_bytes: Box<[u8]> = rows.row(row_idx).data().into();
                    self.seen.insert(row_bytes);
                }
            }
        }
    }

    /// Filter `candidates` to only rows not yet seen, updating the persistent set.
    ///
    /// Both cross-iteration dedup (rows already accepted in prior iterations) and
    /// within-batch dedup (duplicate rows in a single candidate batch) are handled
    /// in a single pass.
    fn compute_delta(
        &mut self,
        candidates: &[RecordBatch],
        schema: &SchemaRef,
    ) -> DFResult<Vec<RecordBatch>> {
        let mut delta_batches = Vec::new();
        for batch in candidates {
            if batch.num_rows() == 0 {
                continue;
            }

            // Vectorized encoding of all rows in this batch.
            let arrays: Vec<_> = batch.columns().to_vec();
            let rows = self.converter.convert_columns(&arrays).map_err(arrow_err)?;

            // One pass: check+insert into persistent seen set.
            let mut keep = Vec::with_capacity(batch.num_rows());
            for row_idx in 0..batch.num_rows() {
                let row_bytes: Box<[u8]> = rows.row(row_idx).data().into();
                keep.push(self.seen.insert(row_bytes));
            }

            let keep_mask = arrow_array::BooleanArray::from(keep);
            let new_cols = batch
                .columns()
                .iter()
                .map(|col| {
                    arrow::compute::filter(col.as_ref(), &keep_mask).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })
                })
                .collect::<DFResult<Vec<_>>>()?;

            if new_cols.first().is_some_and(|c| !c.is_empty()) {
                let filtered = RecordBatch::try_new(Arc::clone(schema), new_cols).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                delta_batches.push(filtered);
            }
        }
        Ok(delta_batches)
    }
}

// ---------------------------------------------------------------------------
// FixpointState — per-rule delta tracking during fixpoint iteration
// ---------------------------------------------------------------------------

/// Per-rule state for fixpoint iteration.
///
/// Tracks accumulated facts and the delta (new facts from the latest iteration).
/// Deduplication uses Arrow [`RowConverter`] with a persistent seen set (O(M) per
/// iteration) when supported, with a legacy `HashSet<Vec<ScalarKey>>` fallback.
pub struct FixpointState {
    rule_name: String,
    facts: Vec<RecordBatch>,
    delta: Vec<RecordBatch>,
    schema: SchemaRef,
    key_column_indices: Vec<usize>,
    /// KEY column names for recomputing indices after schema reconciliation.
    key_column_names: Vec<String>,
    /// All column indices for full-row dedup (legacy path only).
    all_column_indices: Vec<usize>,
    /// Running total of facts bytes for memory limit tracking.
    facts_bytes: usize,
    /// Maximum bytes allowed for this derived relation.
    max_derived_bytes: usize,
    /// Optional monotonic aggregate tracking.
    monotonic_agg: Option<MonotonicAggState>,
    /// Arrow RowConverter-based dedup state; `None` triggers legacy fallback.
    row_dedup: Option<RowDedupState>,
    /// Whether strict probability domain checks are enabled.
    strict_probability_domain: bool,
    /// Active probability semiring for this rule's MNOR/MPROD math.
    semiring_kind: SemiringKind,
    /// Issue #162: per-iteration folded view, present exactly when some scan
    /// in this stratum reads this rule's [`DerivedScanView::Folded`] view.
    fold_view: Option<FoldViewState>,
}

/// State backing the per-iteration folded view (issue #162).
///
/// A self-reference inside a recursive FOLD rule currently joins the rule's
/// pre-fold *contribution* rows, so a parent sees one row per contribution of
/// its child rather than one row per child. This state keeps a KEY-grouped
/// snapshot of the rule's own facts, refreshed at the end of every merge, and
/// [`update_derived_scan_handles`] feeds *that* to same-stratum references.
///
/// Two things follow from a value that can now move between iterations:
///
/// * contributions must be **replaced** on re-derivation rather than appended,
///   keyed on everything except the fold inputs — otherwise a parent that
///   already emitted a row against a child's partial value folds the stale row
///   in alongside the fresh one;
/// * convergence can no longer be "the delta is empty", because a clause
///   reading a full snapshot re-derives every iteration. `prev_rows` carries
///   the whole-row set forward so a value that moves in place still counts as
///   a change. Note [`MonotonicAggState`] cannot serve this role — it is only
///   ever fed the delta, and `merge_delta` returns early without updating it
///   when the delta is empty, so only *new rows* can keep the loop alive there.
///
/// # Which chain stages run per iteration
///
/// **PRIORITY and FOLD only.** Both produce the value a self-reference reads,
/// and PRIORITY has to come first or [`FoldExec`] drops `__priority` before it
/// can be honoured — the same ordering `apply_post_fixpoint_chain` uses.
///
/// **HAVING and BEST BY stay post-fixpoint.** They are filters over the folded
/// value, and applying a non-monotone filter per iteration would let a fact
/// appear, disappear and reappear, which the whole-row change test above reads
/// as perpetual progress. A rule that self-references *and* carries HAVING is
/// therefore evaluated against its unfiltered folded value and filtered once at
/// the end. `issue_162_having_is_not_applied_to_the_per_iteration_snapshot`
/// pins the distinction: a child that HAVING removes from the answer must still
/// have been visible to its parent while the fixpoint ran.
struct FoldViewState {
    /// The rule's FOLD bindings — the same ones `apply_post_fixpoint_chain`
    /// hands to [`FoldExec`], so the per-iteration snapshot and the final
    /// answer come from one aggregation implementation.
    bindings: Vec<FoldBinding>,
    /// Whether the rule carries PRIORITY. PRIORITY must run *before* FOLD or
    /// FOLD drops the `__priority` column, so the snapshot chain mirrors the
    /// post-fixpoint chain's ordering.
    has_priority: bool,
    /// Tolerance for the probability-domain check, threaded to `FoldExec`.
    probability_epsilon: f64,
    /// KEY-grouped snapshot of `facts`, in the rule's contribution schema.
    folded: Vec<RecordBatch>,
    /// Whole-row set from the previous merge, for change detection.
    prev_rows: HashSet<Vec<ScalarKey>>,
}

impl FixpointState {
    /// Create a new fixpoint state for a rule. Existing tests call this
    /// with the Phase 1/2 default; the fixpoint planner uses
    /// [`FixpointState::new_with_semiring`] to thread the configured
    /// semiring through.
    pub fn new(
        rule_name: String,
        schema: SchemaRef,
        key_column_indices: Vec<usize>,
        max_derived_bytes: usize,
        monotonic_agg: Option<MonotonicAggState>,
        strict_probability_domain: bool,
    ) -> Self {
        Self::new_with_semiring(
            rule_name,
            schema,
            key_column_indices,
            max_derived_bytes,
            monotonic_agg,
            strict_probability_domain,
            SemiringKind::AddMultProb,
        )
    }

    pub fn new_with_semiring(
        rule_name: String,
        schema: SchemaRef,
        key_column_indices: Vec<usize>,
        max_derived_bytes: usize,
        monotonic_agg: Option<MonotonicAggState>,
        strict_probability_domain: bool,
        semiring_kind: SemiringKind,
    ) -> Self {
        let num_cols = schema.fields().len();
        let row_dedup = RowDedupState::try_new(&schema);
        let key_column_names: Vec<String> = key_column_indices
            .iter()
            .filter_map(|&i| schema.fields().get(i).map(|f| f.name().clone()))
            .collect();
        Self {
            rule_name,
            facts: Vec::new(),
            delta: Vec::new(),
            schema,
            key_column_indices,
            key_column_names,
            all_column_indices: (0..num_cols).collect(),
            facts_bytes: 0,
            max_derived_bytes,
            monotonic_agg,
            row_dedup,
            strict_probability_domain,
            semiring_kind,
            fold_view: None,
        }
    }

    /// Turn on the per-iteration folded view for this rule.
    pub fn enable_fold_view(
        &mut self,
        bindings: Vec<FoldBinding>,
        has_priority: bool,
        probability_epsilon: f64,
    ) {
        self.fold_view = Some(FoldViewState {
            bindings,
            has_priority,
            probability_epsilon,
            folded: Vec::new(),
            prev_rows: HashSet::new(),
        });
    }

    /// Whether the per-iteration folded view is active.
    pub fn has_fold_view(&self) -> bool {
        self.fold_view.is_some()
    }

    /// The KEY-grouped snapshot a same-stratum reference should read.
    pub fn folded_facts(&self) -> &[RecordBatch] {
        self.fold_view.as_ref().map_or(&[], |fv| &fv.folded)
    }

    /// Resolve each fold binding's input column index against the live schema.
    fn fold_input_indices(&self) -> Vec<usize> {
        let Some(fv) = self.fold_view.as_ref() else {
            return Vec::new();
        };
        fv.bindings
            .iter()
            .filter_map(|b| {
                b.input_col_name
                    .as_ref()
                    .and_then(|n| self.schema.index_of(n).ok())
                    .or(Some(b.input_col_index))
                    .filter(|&i| i < self.schema.fields().len())
            })
            .collect()
    }

    /// Merge candidates by **replacing** any contribution
    /// whose derivation key already exists, instead of appending it.
    ///
    /// The derivation key is every column except the fold inputs: the KEY
    /// columns, the `__deriv_*` discriminators and any other yield column. Two
    /// rows sharing it are the same derivation re-evaluated against a child
    /// whose folded value has since moved, so the newer row wins.
    ///
    /// Returns `true` when the whole-row set changed — a new derivation, or an
    /// existing one whose value moved.
    pub async fn merge_fold_contributions(
        &mut self,
        candidates: Vec<RecordBatch>,
        task_ctx: &Arc<TaskContext>,
    ) -> DFResult<bool> {
        if let Some(first) = candidates.iter().find(|b| b.num_rows() > 0) {
            self.reconcile_schema(&first.schema());
        }
        let candidates = round_float_columns(&candidates);

        // Existing facts first, candidates after, so the newest row for a
        // derivation key is the one that survives.
        let mut all: Vec<RecordBatch> = self
            .facts
            .iter()
            .cloned()
            .chain(candidates)
            .filter(|b| b.num_rows() > 0)
            .collect();
        if all.is_empty() {
            self.delta.clear();
            return Ok(false);
        }
        let combined = if all.len() == 1 {
            all.pop().expect("len checked")
        } else {
            arrow::compute::concat_batches(&self.schema, &all).map_err(arrow_err)?
        };

        let fold_inputs = self.fold_input_indices();
        let deriv_key_indices: Vec<usize> = self
            .all_column_indices
            .iter()
            .copied()
            .filter(|i| !fold_inputs.contains(i))
            .collect();

        let mut latest: HashMap<Vec<ScalarKey>, u32> = HashMap::new();
        for row in 0..combined.num_rows() {
            let key = extract_scalar_key(&combined, &deriv_key_indices, row);
            latest.insert(key, row as u32);
        }
        let mut keep: Vec<u32> = latest.into_values().collect();
        keep.sort_unstable();

        let indices = arrow_array::UInt32Array::from(keep);
        let columns = combined
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(arrow_err)?;
        let retained =
            RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)?;

        // Change detection and delta over whole rows.
        let mut rows_now: HashSet<Vec<ScalarKey>> = HashSet::new();
        let mut new_row_indices: Vec<u32> = Vec::new();
        {
            let prev = &self.fold_view.as_ref().expect("fold view").prev_rows;
            for row in 0..retained.num_rows() {
                let full = extract_scalar_key(&retained, &self.all_column_indices, row);
                if !prev.contains(&full) {
                    new_row_indices.push(row as u32);
                }
                rows_now.insert(full);
            }
        }
        let changed = rows_now != self.fold_view.as_ref().expect("fold view").prev_rows;

        self.delta = if new_row_indices.is_empty() {
            Vec::new()
        } else {
            let idx = arrow_array::UInt32Array::from(new_row_indices);
            let cols = retained
                .columns()
                .iter()
                .map(|c| arrow::compute::take(c.as_ref(), &idx, None))
                .collect::<Result<Vec<_>, _>>()
                .map_err(arrow_err)?;
            vec![RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)?]
        };

        self.facts_bytes = batch_byte_size(&retained);
        if self.facts_bytes > self.max_derived_bytes {
            return Err(datafusion::error::DataFusionError::Execution(
                LocyRuntimeError::MemoryLimitExceeded {
                    rule: self.rule_name.clone(),
                    bytes: self.facts_bytes,
                    limit: self.max_derived_bytes,
                }
                .to_string(),
            ));
        }
        self.facts = vec![retained];
        if let Some(fv) = self.fold_view.as_mut() {
            fv.prev_rows = rows_now;
        }
        self.recompute_folded(task_ctx).await?;
        Ok(changed)
    }

    /// Recompute the KEY-grouped snapshot from the accumulated contributions.
    ///
    /// One output row per KEY: a representative contribution row with each
    /// fold column overwritten by that KEY's aggregate. Keeping the
    /// contribution schema means the reading clause resolves the same column
    /// names it does today, and the `__deriv_*` columns it does not project
    /// simply carry the representative's values.
    ///
    /// The aggregate itself comes from [`FoldExec`] — the same operator
    /// `apply_post_fixpoint_chain` runs — rather than a second implementation.
    /// That matters for aggregates with no row-level fast path (`AVG`,
    /// `COLLECT`): a hand-rolled `update_step` loop cannot compute them, and
    /// silently substituting the representative row's value would make a
    /// same-stratum consumer read a sample instead of an aggregate.
    ///
    /// PRIORITY is applied first, mirroring the post-fixpoint chain's order —
    /// `FoldExec` emits KEY plus fold columns only, so it would otherwise drop
    /// `__priority` before it could be honoured. HAVING and BEST BY are
    /// deliberately *not* applied here; see [`FoldViewState`].
    async fn recompute_folded(&mut self, task_ctx: &Arc<TaskContext>) -> DFResult<()> {
        if self.fold_view.is_none() {
            return Ok(());
        }
        if self.facts.is_empty() || self.facts.iter().all(|b| b.num_rows() == 0) {
            if let Some(fv) = self.fold_view.as_mut() {
                fv.folded.clear();
            }
            return Ok(());
        }

        let batches: Vec<RecordBatch> = self
            .facts
            .iter()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect();
        let combined = if batches.len() == 1 {
            batches[0].clone()
        } else {
            arrow::compute::concat_batches(&self.schema, &batches).map_err(arrow_err)?
        };
        let combined_schema = combined.schema();

        // Group rows by KEY, preserving first-seen order.
        let mut order: Vec<Vec<ScalarKey>> = Vec::new();
        let mut groups: HashMap<Vec<ScalarKey>, Vec<usize>> = HashMap::new();
        for row in 0..combined.num_rows() {
            let key = extract_scalar_key(&combined, &self.key_column_indices, row);
            match groups.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(mut e) => e.get_mut().push(row),
                std::collections::hash_map::Entry::Vacant(e) => {
                    order.push(key);
                    e.insert(vec![row]);
                }
            }
        }

        // Representative = the group's first row, restricted to the rows that
        // survive PRIORITY so the snapshot's non-fold columns agree with the
        // rows the fold actually aggregated.
        let priority_idx = combined_schema.index_of("__priority").ok();
        let representatives: Vec<u32> = order
            .iter()
            .map(|k| {
                let rows = &groups[k];
                let pick = priority_idx
                    .and_then(|pi| {
                        let col = combined.column(pi);
                        let best = rows
                            .iter()
                            .filter_map(|&r| extract_f64(col.as_ref(), r))
                            .fold(f64::NEG_INFINITY, f64::max);
                        rows.iter()
                            .copied()
                            .find(|&r| extract_f64(col.as_ref(), r).is_some_and(|v| v >= best))
                    })
                    .unwrap_or(rows[0]);
                pick as u32
            })
            .collect();
        let idx = arrow_array::UInt32Array::from(representatives);
        let mut columns = combined
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &idx, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(arrow_err)?;

        // PRIORITY -> FOLD over the accumulated contributions.
        let fv = self.fold_view.as_ref().expect("fold view checked above");
        let bindings = fv.bindings.clone();
        let has_priority = fv.has_priority;
        let epsilon = fv.probability_epsilon;
        let mut plan: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[vec![combined]],
            Arc::clone(&combined_schema),
            None,
        )?;
        if has_priority && let Some(pi) = priority_idx {
            let keys = self.key_indices_in(&plan.schema())?;
            plan = Arc::new(PriorityExec::new(plan, keys, pi));
        }
        let fold_keys = self.key_indices_in(&plan.schema())?;
        let n_keys = fold_keys.len();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(FoldExec::new_with_semiring(
            plan,
            fold_keys,
            bindings.clone(),
            self.strict_probability_domain,
            epsilon,
            self.semiring_kind,
        ));
        let folded_batches = collect_all_partitions(&plan, Arc::clone(task_ctx)).await?;
        let folded_out = match folded_batches.iter().find(|b| b.num_rows() > 0) {
            Some(b) => b.clone(),
            None => {
                // Nothing aggregated (all-empty input): leave the
                // representative rows as they stand.
                let folded =
                    RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)?;
                if let Some(fv) = self.fold_view.as_mut() {
                    fv.folded = vec![folded];
                }
                return Ok(());
            }
        };

        // `FoldExec` emits KEY columns (in `fold_keys` order) followed by one
        // column per binding. Map each KEY back to its row so the aggregate can
        // be grafted onto the representative.
        let out_key_indices: Vec<usize> = (0..n_keys).collect();
        let mut fold_row: HashMap<Vec<ScalarKey>, u32> = HashMap::new();
        for row in 0..folded_out.num_rows() {
            fold_row.insert(
                extract_scalar_key(&folded_out, &out_key_indices, row),
                row as u32,
            );
        }
        let mut picks: Vec<u32> = Vec::with_capacity(order.len());
        for key in &order {
            let Some(&r) = fold_row.get(key) else {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "rule '{}': FOLD produced no row for a KEY group present in \
                     its own facts — the folded view and the contribution set \
                     disagree",
                    self.rule_name
                )));
            };
            picks.push(r);
        }
        let picks = arrow_array::UInt32Array::from(picks);

        for (pos, binding) in bindings.iter().enumerate() {
            let Some(dest) = binding
                .input_col_name
                .as_ref()
                .and_then(|n| combined_schema.index_of(n).ok())
                .or(Some(binding.input_col_index))
                .filter(|&i| i < columns.len())
            else {
                continue;
            };
            let src = folded_out.column(n_keys + pos);
            let taken = arrow::compute::take(src.as_ref(), &picks, None).map_err(arrow_err)?;
            let want = combined_schema.field(dest).data_type();
            columns[dest] = if taken.data_type() == want {
                taken
            } else {
                // A fold whose output type is not the contribution column's
                // type (COLLECT -> List) cannot be carried in the contribution
                // schema. Fail loudly: substituting the representative row's
                // value here is exactly the silent-sample bug this rewrite
                // exists to remove.
                arrow::compute::cast(&taken, want).map_err(|_| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "rule '{}': FOLD '{}' produces {:?}, which cannot be \
                         represented in the '{}' column ({:?}) that a \
                         same-stratum reference reads. Move the fold into a \
                         separate non-recursive rule and consume that with IS.",
                        self.rule_name,
                        binding.output_name,
                        taken.data_type(),
                        combined_schema.field(dest).name(),
                        want,
                    ))
                })?
            };
        }

        let folded = RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)?;
        if let Some(fv) = self.fold_view.as_mut() {
            fv.folded = vec![folded];
        }
        Ok(())
    }

    /// Resolve this rule's KEY columns by name against `schema`.
    ///
    /// Positions drift as operators add and drop columns (`PriorityExec` drops
    /// `__priority`), so the cached `key_column_indices` are only valid against
    /// the contribution schema.
    fn key_indices_in(&self, schema: &SchemaRef) -> DFResult<Vec<usize>> {
        self.key_column_names
            .iter()
            .map(|n| {
                schema.index_of(n).map_err(|_| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "rule '{}': KEY column '{}' missing from the folded-view \
                         input schema",
                        self.rule_name, n
                    ))
                })
            })
            .collect()
    }

    /// Reconcile the pre-computed schema with the actual physical plan output.
    ///
    /// `infer_expr_type` may guess wrong (e.g. `Property → Float64` for a
    /// string column).  When the first real batch arrives with a different
    /// schema, update ours so that `RowDedupState` / `RecordBatch::try_new`
    /// use the correct types.
    fn reconcile_schema(&mut self, actual_schema: &SchemaRef) {
        if self.schema.fields() != actual_schema.fields() {
            tracing::debug!(
                rule = %self.rule_name,
                "Reconciling fixpoint schema from physical plan output",
            );
            self.schema = Arc::clone(actual_schema);
            self.row_dedup = RowDedupState::try_new(&self.schema);
            // Recompute key_column_indices from stored KEY column names.
            // Without this, FoldExec groups by wrong columns when the
            // physical plan reorders columns vs the pre-inferred schema.
            let new_indices: Vec<usize> = self
                .key_column_names
                .iter()
                .filter_map(|name| actual_schema.index_of(name).ok())
                .collect();
            if new_indices.len() == self.key_column_names.len() {
                self.key_column_indices = new_indices;
            }
            // else: not all KEY columns found in new schema — keep original indices
            let num_cols = actual_schema.fields().len();
            self.all_column_indices = (0..num_cols).collect();
        }
    }

    /// Merge candidate rows into facts, computing delta (truly new rows).
    ///
    /// Returns `true` if any new facts were added.
    pub async fn merge_delta(
        &mut self,
        candidates: Vec<RecordBatch>,
        task_ctx: Option<Arc<TaskContext>>,
    ) -> DFResult<bool> {
        if candidates.is_empty() || candidates.iter().all(|b| b.num_rows() == 0) {
            self.delta.clear();
            return Ok(false);
        }

        // Reconcile schema from the first non-empty candidate batch.
        // The physical plan's output types are authoritative over the
        // planner's inferred types.
        if let Some(first) = candidates.iter().find(|b| b.num_rows() > 0) {
            self.reconcile_schema(&first.schema());
        }

        // Round floats for stable dedup
        let candidates = round_float_columns(&candidates);

        // Compute delta: rows in candidates not already in facts
        let delta = self.compute_delta(&candidates, task_ctx.as_ref()).await?;

        if delta.is_empty() || delta.iter().all(|b| b.num_rows() == 0) {
            self.delta.clear();
            // Update monotonic aggs even with empty delta (for stability check)
            if let Some(ref mut agg) = self.monotonic_agg {
                agg.snapshot();
            }
            return Ok(false);
        }

        // Check memory limit
        let delta_bytes: usize = delta.iter().map(batch_byte_size).sum();
        if self.facts_bytes + delta_bytes > self.max_derived_bytes {
            return Err(datafusion::error::DataFusionError::Execution(
                LocyRuntimeError::MemoryLimitExceeded {
                    rule: self.rule_name.clone(),
                    bytes: self.facts_bytes + delta_bytes,
                    limit: self.max_derived_bytes,
                }
                .to_string(),
            ));
        }

        // Update monotonic aggs
        if let Some(ref mut agg) = self.monotonic_agg {
            agg.snapshot();
            agg.update(
                &self.key_column_indices,
                &delta,
                self.strict_probability_domain,
                self.semiring_kind,
            )?;
        }

        // Append delta to facts
        self.facts_bytes += delta_bytes;
        self.facts.extend(delta.iter().cloned());
        self.delta = delta;

        Ok(true)
    }

    /// Dispatch to vectorized LeftAntiJoin, Arrow RowConverter dedup, or legacy ScalarKey dedup.
    ///
    /// Priority order:
    /// 1. `arrow_left_anti_dedup` when `total_existing >= DEDUP_ANTI_JOIN_THRESHOLD` and task_ctx available.
    /// 2. `RowDedupState` (persistent HashSet, O(M) per iteration) when schema is supported.
    /// 3. `compute_delta_legacy` (rebuilds from facts, fallback for unsupported column types).
    async fn compute_delta(
        &mut self,
        candidates: &[RecordBatch],
        task_ctx: Option<&Arc<TaskContext>>,
    ) -> DFResult<Vec<RecordBatch>> {
        let total_existing: usize = self.facts.iter().map(|b| b.num_rows()).sum();
        if total_existing >= DEDUP_ANTI_JOIN_THRESHOLD
            && let Some(ctx) = task_ctx
        {
            return arrow_left_anti_dedup(candidates.to_vec(), &self.facts, &self.schema, ctx)
                .await;
        }
        if let Some(ref mut rd) = self.row_dedup {
            rd.compute_delta(candidates, &self.schema)
        } else {
            self.compute_delta_legacy(candidates)
        }
    }

    /// Legacy dedup: rebuild a `HashSet<Vec<ScalarKey>>` from all facts each call.
    ///
    /// Used as fallback when `RowConverter` does not support the schema's column types.
    fn compute_delta_legacy(&self, candidates: &[RecordBatch]) -> DFResult<Vec<RecordBatch>> {
        // Build set of existing fact row keys (ALL columns)
        let mut existing: HashSet<Vec<ScalarKey>> = HashSet::new();
        for batch in &self.facts {
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(batch, &self.all_column_indices, row_idx);
                existing.insert(key);
            }
        }

        let mut delta_batches = Vec::new();
        for batch in candidates {
            if batch.num_rows() == 0 {
                continue;
            }
            // Filter to only new rows
            let mut keep = Vec::with_capacity(batch.num_rows());
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(batch, &self.all_column_indices, row_idx);
                keep.push(!existing.contains(&key));
            }

            // Also dedup within the candidate batch itself
            for (row_idx, kept) in keep.iter_mut().enumerate() {
                if *kept {
                    let key = extract_scalar_key(batch, &self.all_column_indices, row_idx);
                    if !existing.insert(key) {
                        *kept = false;
                    }
                }
            }

            let keep_mask = arrow_array::BooleanArray::from(keep);
            let new_rows = batch
                .columns()
                .iter()
                .map(|col| {
                    arrow::compute::filter(col.as_ref(), &keep_mask).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })
                })
                .collect::<DFResult<Vec<_>>>()?;

            if new_rows.first().is_some_and(|c| !c.is_empty()) {
                let filtered =
                    RecordBatch::try_new(Arc::clone(&self.schema), new_rows).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                delta_batches.push(filtered);
            }
        }

        Ok(delta_batches)
    }

    /// Check if this rule has converged (no new facts and aggs stable).
    pub fn is_converged(&self) -> bool {
        let delta_empty = self.delta.is_empty() || self.delta.iter().all(|b| b.num_rows() == 0);
        let agg_stable = self.monotonic_agg.as_ref().is_none_or(|a| a.is_stable());
        delta_empty && agg_stable
    }

    /// Get all accumulated facts.
    pub fn all_facts(&self) -> &[RecordBatch] {
        &self.facts
    }

    /// Get the delta from the latest iteration.
    pub fn all_delta(&self) -> &[RecordBatch] {
        &self.delta
    }

    /// Consume self and return facts.
    pub fn into_facts(self) -> Vec<RecordBatch> {
        self.facts
    }

    /// Merge candidates using BEST BY semantics.
    ///
    /// Combines existing facts with new candidates, keeping only the best row
    /// per KEY group according to `sort_criteria`. Returns `true` if the
    /// best-per-KEY fact set actually changed (a genuinely better value was
    /// found or a new KEY appeared).
    ///
    /// This replaces `merge_delta` for rules with BEST BY, enabling convergence
    /// on cyclic graphs where dominated ALONG values would otherwise produce an
    /// unbounded stream of "new" full-row facts.
    pub fn merge_best_by(
        &mut self,
        candidates: Vec<RecordBatch>,
        sort_criteria: &[SortCriterion],
    ) -> DFResult<bool> {
        if candidates.is_empty() || candidates.iter().all(|b| b.num_rows() == 0) {
            self.delta.clear();
            return Ok(false);
        }

        // Reconcile schema from the first non-empty candidate batch.
        if let Some(first) = candidates.iter().find(|b| b.num_rows() > 0) {
            self.reconcile_schema(&first.schema());
        }

        // Round floats for stable dedup.
        let candidates = round_float_columns(&candidates);

        // Snapshot existing best-per-KEY facts for change detection.
        let old_best: HashMap<Vec<ScalarKey>, Vec<ScalarKey>> =
            self.build_key_criteria_map(sort_criteria);

        // Concat existing facts + new candidates.
        let mut all_batches = self.facts.clone();
        all_batches.extend(candidates);
        let all_batches: Vec<_> = all_batches
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect();
        if all_batches.is_empty() {
            self.delta.clear();
            return Ok(false);
        }

        let combined = arrow::compute::concat_batches(&self.schema, &all_batches)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        if combined.num_rows() == 0 {
            self.delta.clear();
            return Ok(false);
        }

        // Sort by KEY ASC then criteria, so the best row per KEY group comes
        // first.
        let mut sort_columns = Vec::new();
        for &ki in &self.key_column_indices {
            if ki >= combined.num_columns() {
                continue;
            }
            sort_columns.push(arrow::compute::SortColumn {
                values: Arc::clone(combined.column(ki)),
                options: Some(arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            });
        }
        for criterion in sort_criteria {
            if criterion.col_index >= combined.num_columns() {
                continue;
            }
            sort_columns.push(arrow::compute::SortColumn {
                values: Arc::clone(combined.column(criterion.col_index)),
                options: Some(arrow::compute::SortOptions {
                    descending: !criterion.ascending,
                    nulls_first: criterion.nulls_first,
                }),
            });
        }

        let sorted_indices =
            arrow::compute::lexsort_to_indices(&sort_columns, None).map_err(arrow_err)?;
        let sorted_columns: Vec<_> = combined
            .columns()
            .iter()
            .map(|col| arrow::compute::take(col.as_ref(), &sorted_indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        let sorted = RecordBatch::try_new(Arc::clone(&self.schema), sorted_columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        // Dedup: keep first (best) row per KEY group.
        let mut keep_indices: Vec<u32> = Vec::new();
        let mut prev_key: Option<Vec<ScalarKey>> = None;
        for row_idx in 0..sorted.num_rows() {
            let key = extract_scalar_key(&sorted, &self.key_column_indices, row_idx);
            let is_new_group = match &prev_key {
                None => true,
                Some(prev) => *prev != key,
            };
            if is_new_group {
                keep_indices.push(row_idx as u32);
                prev_key = Some(key);
            }
        }

        let keep_array = arrow_array::UInt32Array::from(keep_indices);
        let output_columns: Vec<_> = sorted
            .columns()
            .iter()
            .map(|col| arrow::compute::take(col.as_ref(), &keep_array, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        let pruned = RecordBatch::try_new(Arc::clone(&self.schema), output_columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        // Detect whether the best-per-KEY set actually changed.
        let new_best: HashMap<Vec<ScalarKey>, Vec<ScalarKey>> = {
            let mut map = HashMap::new();
            for row_idx in 0..pruned.num_rows() {
                let key = extract_scalar_key(&pruned, &self.key_column_indices, row_idx);
                let criteria: Vec<ScalarKey> = sort_criteria
                    .iter()
                    .flat_map(|c| extract_scalar_key(&pruned, &[c.col_index], row_idx))
                    .collect();
                map.insert(key, criteria);
            }
            map
        };
        let changed = old_best != new_best;

        tracing::debug!(
            rule = %self.rule_name,
            old_keys = old_best.len(),
            new_keys = new_best.len(),
            changed = changed,
            "BEST BY merge"
        );

        // Replace facts with the pruned set.
        self.facts_bytes = batch_byte_size(&pruned);
        self.facts = vec![pruned];
        if changed {
            // Delta is conceptually the new/improved facts, but since we
            // replaced the entire set, just mark delta non-empty.
            self.delta = self.facts.clone();
        } else {
            self.delta.clear();
        }

        // Rebuild row dedup from pruned facts for consistency.
        self.row_dedup = RowDedupState::try_new(&self.schema);
        if let Some(ref mut rd) = self.row_dedup {
            rd.ingest_existing(&self.facts, &self.schema);
        }

        Ok(changed)
    }

    /// Build a map from KEY column values to sort criteria values.
    fn build_key_criteria_map(
        &self,
        sort_criteria: &[SortCriterion],
    ) -> HashMap<Vec<ScalarKey>, Vec<ScalarKey>> {
        let mut map = HashMap::new();
        for batch in &self.facts {
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(batch, &self.key_column_indices, row_idx);
                let criteria: Vec<ScalarKey> = sort_criteria
                    .iter()
                    .flat_map(|c| extract_scalar_key(batch, &[c.col_index], row_idx))
                    .collect();
                map.insert(key, criteria);
            }
        }
        map
    }
}

/// Estimate byte size of a RecordBatch.
fn batch_byte_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum()
}

// ---------------------------------------------------------------------------
// Float rounding for stable dedup
// ---------------------------------------------------------------------------

/// Round all Float64 columns to 12 decimal places for stable dedup.
fn round_float_columns(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    batches
        .iter()
        .map(|batch| {
            let schema = batch.schema();
            let has_float = schema
                .fields()
                .iter()
                .any(|f| *f.data_type() == arrow_schema::DataType::Float64);
            if !has_float {
                return batch.clone();
            }

            let columns: Vec<arrow_array::ArrayRef> = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    if *schema.field(i).data_type() == arrow_schema::DataType::Float64 {
                        let arr = col
                            .as_any()
                            .downcast_ref::<arrow_array::Float64Array>()
                            .unwrap();
                        let rounded: arrow_array::Float64Array = arr
                            .iter()
                            .map(|v| v.map(|f| (f * 1e12).round() / 1e12))
                            .collect();
                        Arc::new(rounded) as arrow_array::ArrayRef
                    } else {
                        Arc::clone(col)
                    }
                })
                .collect();

            RecordBatch::try_new(schema, columns).unwrap_or_else(|_| batch.clone())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// LeftAntiJoin delta deduplication
// ---------------------------------------------------------------------------

/// Row threshold above which the vectorized Arrow LeftAntiJoin dedup path is used.
///
/// Below this threshold the persistent `RowDedupState` HashSet is O(M) and
/// avoids rebuilding the existing-row set; above it DataFusion's vectorized
/// HashJoinExec is more cache-efficient.
const DEDUP_ANTI_JOIN_THRESHOLD: usize = 300;

/// Deduplicate `candidates` against `existing` using DataFusion's HashJoinExec.
///
/// Returns rows in `candidates` that do not appear in `existing` (LeftAnti semantics).
/// `null_equals_null = true` so NULLs are treated as equal for dedup purposes.
/// Dedup `batches` by all columns (set semantics), keeping the first occurrence.
///
/// `arrow_left_anti_dedup` removes candidate rows that match the existing fact
/// set, but a single semi-naive iteration can emit the same row many times — e.g.
/// a transitive-closure rule derives the same `(a, b)` pair via every intermediate
/// `mid` on a path. A `LeftAnti` join does not remove these *within-candidate*
/// duplicates, so they would leak into the fact set. The `RowDedupState` and legacy
/// paths both dedup within the candidate batch ([`RowDedupState::compute_delta`],
/// [`FixpointState::compute_delta_legacy`]); this keeps the `arrow_left_anti_dedup`
/// path identical so dedup behavior does not change across `DEDUP_ANTI_JOIN_THRESHOLD`.
fn dedup_batches_all_columns(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> DFResult<Vec<RecordBatch>> {
    let fields: Vec<SortField> = schema
        .fields()
        .iter()
        .map(|f| SortField::new(f.data_type().clone()))
        .collect();
    // Unsupported column types: leave as-is. This path is only reached for facts
    // sets >= DEDUP_ANTI_JOIN_THRESHOLD; for those types the <threshold path uses
    // `compute_delta_legacy`, which dedups via `ScalarKey` instead.
    let Ok(converter) = RowConverter::new(fields) else {
        return Ok(batches);
    };
    let mut seen: HashSet<Box<[u8]>> = HashSet::new();
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let rows = converter
            .convert_columns(batch.columns())
            .map_err(arrow_err)?;
        let mut keep = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let row_bytes: Box<[u8]> = rows.row(row_idx).data().into();
            keep.push(seen.insert(row_bytes));
        }
        let keep_mask = arrow_array::BooleanArray::from(keep);
        let cols = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::filter(c.as_ref(), &keep_mask).map_err(arrow_err))
            .collect::<DFResult<Vec<_>>>()?;
        if cols.first().is_some_and(|c| !c.is_empty()) {
            out.push(RecordBatch::try_new(Arc::clone(schema), cols).map_err(arrow_err)?);
        }
    }
    Ok(out)
}

async fn arrow_left_anti_dedup(
    candidates: Vec<RecordBatch>,
    existing: &[RecordBatch],
    schema: &SchemaRef,
    task_ctx: &Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    if existing.is_empty() || existing.iter().all(|b| b.num_rows() == 0) {
        // No existing facts to anti-join against, but still dedup the candidates
        // among themselves (a single iteration may emit duplicate rows).
        return dedup_batches_all_columns(candidates, schema);
    }

    let left: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[candidates], Arc::clone(schema), None)?;
    let right: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[existing.to_vec()], Arc::clone(schema), None)?;

    let on: Vec<(
        Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    )> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let l: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(
                datafusion::physical_plan::expressions::Column::new(field.name(), i),
            );
            let r: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(
                datafusion::physical_plan::expressions::Column::new(field.name(), i),
            );
            (l, r)
        })
        .collect();

    if on.is_empty() {
        return Ok(vec![]);
    }

    let join = HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &JoinType::LeftAnti,
        None,
        PartitionMode::CollectLeft,
        datafusion::common::NullEquality::NullEqualsNull,
        // null_aware = false: this is a set-difference dedup (NOT EXISTS), not a
        // SQL NOT-IN. `NullEqualsNull` already makes NULL keys dedup against NULL
        // keys; enabling null-aware semantics would wrongly annihilate all rows
        // whenever the existing-fact side contains a NULL key.
        false,
    )?;

    let join_arc: Arc<dyn ExecutionPlan> = Arc::new(join);
    // LeftAnti removes candidates that match `existing`, but not duplicate rows
    // within the candidate set — dedup those to match the other delta strategies.
    let anti = collect_all_partitions(&join_arc, task_ctx.clone()).await?;
    dedup_batches_all_columns(anti, schema)
}

// ---------------------------------------------------------------------------
// Plan types for fixpoint rules
// ---------------------------------------------------------------------------

/// IS-ref binding: a reference from a clause body to a derived relation.
#[derive(Debug, Clone)]
pub struct IsRefBinding {
    /// Index into the DerivedScanRegistry.
    pub derived_scan_index: usize,
    /// Name of the rule being referenced.
    pub rule_name: String,
    /// Whether this is a self-reference (rule references itself).
    pub is_self_ref: bool,
    /// Whether this is a negated reference (NOT IS).
    pub negated: bool,
    /// For negated IS-refs: `(left_body_col, right_derived_col)` pairs for anti-join filtering.
    ///
    /// `left_body_col` is the VID column in the clause body (e.g., `"n._vid"`);
    /// `right_derived_col` is the corresponding KEY column in the negated rule's facts (e.g., `"n"`).
    /// Empty for non-negated IS-refs.
    pub anti_join_cols: Vec<(String, String)>,
    /// Whether the target rule has a PROB column.
    pub target_has_prob: bool,
    /// Name of the PROB column in the target rule, if any.
    pub target_prob_col: Option<String>,
    /// `(body_col, derived_col)` pairs for provenance tracking.
    ///
    /// Used by shared-proof detection to find which source facts a derived row
    /// consumed. Populated for all IS-refs (not just negated ones).
    pub provenance_join_cols: Vec<(String, String)>,
}

/// A single clause (body) within a fixpoint rule.
#[derive(Debug)]
pub struct FixpointClausePlan {
    /// The logical plan for the clause body.
    pub body_logical: LogicalPlan,
    /// IS-ref bindings used by this clause.
    pub is_ref_bindings: Vec<IsRefBinding>,
    /// Priority value for this clause (if PRIORITY semantics apply).
    pub priority: Option<i64>,
    /// ALONG binding variable names propagated from the planner.
    pub along_bindings: Vec<String>,
    /// Phase B Slice 3: neural-model invocations lifted out of YIELD
    /// items by the compiler. Each entry is evaluated per row after the
    /// clause body produces batches and before IS-ref handling.
    pub model_invocations: Vec<ModelInvocation>,
}

/// Physical plan for a single rule in a fixpoint stratum.
#[derive(Debug)]
pub struct FixpointRulePlan {
    /// Rule name.
    pub name: String,
    /// Clause bodies (each evaluates to candidate rows).
    pub clauses: Vec<FixpointClausePlan>,
    /// Output schema for this rule's derived relation.
    pub yield_schema: SchemaRef,
    /// Indices of KEY columns within yield_schema.
    pub key_column_indices: Vec<usize>,
    /// Priority value (if PRIORITY semantics apply).
    pub priority: Option<i64>,
    /// Whether this rule has FOLD semantics.
    pub has_fold: bool,
    /// FOLD bindings for post-fixpoint aggregation.
    pub fold_bindings: Vec<FoldBinding>,
    /// Post-FOLD filter expressions (HAVING semantics).
    pub having: Vec<Expr>,
    /// Whether this rule has BEST BY semantics.
    pub has_best_by: bool,
    /// BEST BY sort criteria for post-fixpoint selection.
    pub best_by_criteria: Vec<SortCriterion>,
    /// Whether this rule has PRIORITY semantics.
    pub has_priority: bool,
    /// Whether BEST BY should apply a deterministic secondary sort for
    /// tie-breaking. When false, tied rows are selected non-deterministically
    /// (faster but not repeatable across runs).
    pub deterministic: bool,
    /// Name of the PROB column in this rule's yield schema, if any.
    pub prob_column_name: Option<String>,
    /// True when any clause of this rule has ≥2 positive same-stratum
    /// IS-refs (non-linear recursion, e.g. `tc(a,b) :- tc(a,m), tc(m,b)`).
    /// Such rules get FULL facts (naive evaluation) instead of the latest
    /// delta on their self-ref scans: a delta-only join computes Δ×Δ and
    /// misses the Δ×F_old combinations, silently under-deriving. Covers
    /// both `p :- p, p` and `p :- p, q` (q in the same SCC) shapes.
    pub non_linear: bool,
    /// Post-fold YIELD projection specs `(output_name, expr, target_type)`.
    ///
    /// Non-empty only for rules with a YIELD column that is a computed
    /// expression over a FOLD output (`total * 2.0 AS score`). When present it
    /// lists every yield column in schema order and the post-fixpoint chain
    /// applies it after BEST BY to build the final output. Empty otherwise.
    pub yield_projection: Vec<(String, Expr)>,
}

// ---------------------------------------------------------------------------
// run_fixpoint_loop — the core semi-naive iteration algorithm
// ---------------------------------------------------------------------------

/// Run the semi-naive fixpoint iteration loop.
///
/// Evaluates all rules in a stratum repeatedly, feeding deltas back through
/// derived scan handles until convergence or limits are reached.
#[expect(clippy::too_many_arguments, reason = "Fixpoint loop needs all context")]
async fn run_fixpoint_loop(
    rules: Vec<FixpointRulePlan>,
    max_iterations: usize,
    timeout: Duration,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: Arc<StorageManager>,
    schema_info: Arc<UniSchema>,
    params: HashMap<String, Value>,
    registry: Arc<DerivedScanRegistry>,
    output_schema: SchemaRef,
    max_derived_bytes: usize,
    derivation_tracker: Option<Arc<ProvenanceStore>>,
    iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    exact_probability: bool,
    max_bdd_variables: usize,
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
    approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
    top_k_proofs: usize,
    timeout_flag: Arc<std::sync::atomic::AtomicU8>,
    semiring_kind: SemiringKind,
    classifier_registry: Arc<ClassifierRegistry>,
    classifier_cache: Option<Arc<ModelInvocationCache>>,
    classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    profile_collector: Option<Arc<LocyProfileCollector>>,
) -> DFResult<Vec<RecordBatch>> {
    let start = Instant::now();
    let task_ctx = session_ctx.read().task_ctx();

    // IMPORTANT: per rollout D-9 the FuzzyNotProbabilistic warning emitted
    // below is unsuppressible — do not gate on any suppression mechanism.
    // Fuzzy truth values are not probabilities; silent conflation is the
    // dominant pitfall in neuro-symbolic systems (LTN, NTP).
    if semiring_kind == SemiringKind::MaxMinProb {
        let mut warnings = warnings_slot.write().unwrap_or_else(|e| e.into_inner());
        let mut already_warned: HashSet<String> = warnings
            .iter()
            .filter(|w| w.code == RuntimeWarningCode::FuzzyNotProbabilistic)
            .map(|w| w.rule_name.clone())
            .collect();
        for rule in &rules {
            if rule.prob_column_name.is_some() && !already_warned.contains(&rule.name) {
                warnings.push(RuntimeWarning {
                    code: RuntimeWarningCode::FuzzyNotProbabilistic,
                    message: format!(
                        "rule '{}' carries a PROB column but is being evaluated under \
                         the MaxMinProb (fuzzy / Viterbi) semiring; outputs are fuzzy \
                         truth values, not probabilities",
                        rule.name
                    ),
                    rule_name: rule.name.clone(),
                    variable_count: None,
                    key_group: None,
                });
                already_warned.insert(rule.name.clone());
            }
        }
    }

    // Initialize per-rule state
    let mut states: Vec<FixpointState> = rules
        .iter()
        .map(|rule| {
            let monotonic_agg = if !rule.fold_bindings.is_empty() {
                let bindings: Vec<MonotonicFoldBinding> = rule
                    .fold_bindings
                    .iter()
                    .map(|fb| MonotonicFoldBinding {
                        fold_name: fb.output_name.clone(),
                        aggregate: std::sync::Arc::clone(&fb.aggregate),
                        input_col_index: fb.input_col_index,
                        input_col_name: fb.input_col_name.clone(),
                    })
                    .collect();
                Some(MonotonicAggState::new(bindings))
            } else {
                None
            };
            let mut state = FixpointState::new_with_semiring(
                rule.name.clone(),
                Arc::clone(&rule.yield_schema),
                rule.key_column_indices.clone(),
                max_derived_bytes,
                monotonic_agg,
                strict_probability_domain,
                semiring_kind,
            );
            // Issue #162: maintain the folded snapshot exactly when some scan
            // in this stratum asks for it. Deriving the decision from the
            // registry the planner built keeps one authority for it.
            let serves_folded_view = registry
                .entries
                .iter()
                .any(|e| e.rule_name == rule.name && e.view == DerivedScanView::Folded);
            if serves_folded_view && !rule.fold_bindings.is_empty() {
                state.enable_fold_view(
                    rule.fold_bindings.clone(),
                    rule.has_priority,
                    probability_epsilon,
                );
            }
            state
        })
        .collect();

    // Main iteration loop
    let mut converged = false;
    let mut total_iters = 0usize;
    for iteration in 0..max_iterations {
        total_iters = iteration + 1;
        tracing::debug!("fixpoint iteration {}", iteration);
        let mut any_changed = false;

        for rule_idx in 0..rules.len() {
            let rule = &rules[rule_idx];

            // Update derived scan handles for this rule's clauses
            update_derived_scan_handles(&registry, &states, rule_idx, &rules);

            // Evaluate clause bodies, tracking per-clause candidates for provenance.
            let mut all_candidates = Vec::new();
            let mut clause_candidates: Vec<Vec<RecordBatch>> = Vec::new();
            // Profiling (profile() path only): time this rule's evaluation in
            // this iteration and accumulate the clause-body operator trees.
            let rule_start = Instant::now();
            let mut iter_ops: Vec<OperatorStats> = Vec::new();
            for clause in &rule.clauses {
                // Phase B A4 follow-up: the planner inserts
                // `LogicalPlan::LocyModelInvoke` between the body and
                // `LocyProject` when this clause has neural-model
                // invocations, so `execute_subplan` runs the invocation
                // inline as part of the body plan tree.
                let mut batches = execute_locy_clause_body(
                    &clause.body_logical,
                    &params,
                    &graph_ctx,
                    &session_ctx,
                    &storage,
                    &schema_info,
                    profile_collector.is_some(),
                    &mut iter_ops,
                )
                .await?;
                // Apply negated IS-ref semantics: probabilistic complement or anti-join.
                for binding in &clause.is_ref_bindings {
                    if binding.negated
                        && !binding.anti_join_cols.is_empty()
                        && let Some(entry) = registry.get(binding.derived_scan_index)
                    {
                        let neg_facts = entry.data.read().clone();
                        if !neg_facts.is_empty() {
                            if binding.target_has_prob && rule.prob_column_name.is_some() {
                                // Probabilistic complement: add 1-p column instead of filtering.
                                let complement_col =
                                    format!("__prob_complement_{}", binding.rule_name);
                                if let Some(prob_col) = &binding.target_prob_col {
                                    batches = apply_prob_complement_composite(
                                        batches,
                                        &neg_facts,
                                        &binding.anti_join_cols,
                                        prob_col,
                                        &complement_col,
                                    )?;
                                } else {
                                    // target_has_prob but no prob_col: fall back to anti-join.
                                    batches = apply_anti_join_composite(
                                        batches,
                                        &neg_facts,
                                        &binding.anti_join_cols,
                                    )?;
                                }
                            } else {
                                // Boolean exclusion: anti-join (existing behavior)
                                batches = apply_anti_join_composite(
                                    batches,
                                    &neg_facts,
                                    &binding.anti_join_cols,
                                )?;
                            }
                        }
                    }
                }
                // Multiply complement columns into the PROB column (if any) and clean up
                let complement_cols: Vec<String> = if !batches.is_empty() {
                    batches[0]
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| f.name().starts_with("__prob_complement_"))
                        .map(|f| f.name().clone())
                        .collect()
                } else {
                    vec![]
                };
                if !complement_cols.is_empty() {
                    batches = multiply_prob_factors(
                        batches,
                        rule.prob_column_name.as_deref(),
                        &complement_cols,
                    )?;
                }

                // The anti-join has run; drop its hidden subject-`_vid` columns
                // before anything downstream can absorb them into fact identity.
                batches = super::locy_complement::strip_isnot_vid_columns(batches)?;

                clause_candidates.push(batches.clone());
                all_candidates.extend(batches);
            }

            // Merge candidates into facts.
            // For BEST BY rules, use a specialized merge that keeps only the
            // best row per KEY group, enabling convergence on cyclic graphs.
            let changed = if rule.has_best_by && !rule.best_by_criteria.is_empty() {
                states[rule_idx].merge_best_by(all_candidates, &rule.best_by_criteria)?
            } else if states[rule_idx].has_fold_view() {
                // Issue #162: replace contributions per derivation key and
                // refresh the folded snapshot the self-refs read.
                states[rule_idx]
                    .merge_fold_contributions(all_candidates, &task_ctx)
                    .await?
            } else {
                states[rule_idx]
                    .merge_delta(all_candidates, Some(Arc::clone(&task_ctx)))
                    .await?
            };
            if changed {
                any_changed = true;
                // Record provenance for newly derived facts when tracker is present.
                if let Some(ref tracker) = derivation_tracker {
                    record_provenance(
                        ProvenanceCtx {
                            tracker,
                            registry: &registry,
                            warnings_slot: &warnings_slot,
                        },
                        rule,
                        &states[rule_idx],
                        &clause_candidates,
                        iteration,
                        top_k_proofs,
                        ClassifierRefs {
                            registry: &classifier_registry,
                            cache: classifier_cache.as_ref(),
                            provenance_store: classifier_provenance_store.as_ref(),
                        },
                    )
                    .await;
                }
            }

            // Profiling: record this rule's per-iteration row (delta = net-new
            // facts merged this pass, plus the captured clause-body operators).
            if let Some(ref collector) = profile_collector {
                let delta_facts: usize = states[rule_idx]
                    .all_delta()
                    .iter()
                    .map(|b| b.num_rows())
                    .sum();
                collector.record(
                    &rule.name,
                    iteration,
                    delta_facts,
                    rule_start.elapsed().as_secs_f64() * 1000.0,
                    iter_ops,
                );
            }
        }

        // Check convergence
        if !any_changed && states.iter().all(|s| s.is_converged()) {
            tracing::debug!("fixpoint converged after {} iterations", iteration + 1);
            converged = true;
            break;
        }

        // Check timeout
        if start.elapsed() > timeout {
            tracing::warn!(
                "fixpoint timeout after {} iterations; returning partial results",
                iteration + 1,
            );
            interruption::set(&timeout_flag, interruption::TIMEOUT);
            break;
        }
    }

    // Write per-rule iteration counts to the shared slot.
    if let Ok(mut counts) = iteration_counts.write() {
        for rule in &rules {
            counts.insert(rule.name.clone(), total_iters);
        }
    }

    // Profiling: record each rule's final converged fact count.
    if let Some(ref collector) = profile_collector {
        for (idx, rule) in rules.iter().enumerate() {
            let facts: usize = states[idx].all_facts().iter().map(|b| b.num_rows()).sum();
            collector.set_final_facts(&rule.name, facts);
        }
    }

    // If we exhausted all iterations without converging, record the iteration
    // limit (distinct from a wall-clock timeout) and proceed with partial
    // results rather than discarding all work. `set` is first-wins, so a
    // wall-clock timeout recorded above is not overwritten here.
    if !converged && interruption::reason(&timeout_flag).is_none() {
        tracing::warn!(
            "fixpoint did not converge after {max_iterations} iterations; returning partial results",
        );
        interruption::set(&timeout_flag, interruption::ITERATION_LIMIT);
    }

    // Post-fixpoint processing per rule and collect output
    let task_ctx = session_ctx.read().task_ctx();
    let mut all_output = Vec::new();

    for (rule_idx, state) in states.into_iter().enumerate() {
        let rule = &rules[rule_idx];
        let mut facts = state.into_facts();
        if facts.is_empty() {
            continue;
        }

        // Detect shared proofs before FOLD collapses groups.
        //
        // TODO(C0-stage2): swap `detect_shared_lineage` for `TopKTag`
        // DNF inspection when `semiring_kind == TopKProofs { k }`.
        // The library-layer tag math has landed in
        // `crates/uni-locy/src/top_k_proofs.rs` (Phase C C0 Stage 1);
        // Stage 2 plumbs `TopKTag` through `MonotonicAggState` /
        // `FoldExec` so per-row dependency DNFs are available here.
        // Until Stage 2, this scalar `ProvenanceStore` path runs for
        // every semiring including `TopKProofs` (per rollout D-4
        // "graceful migration").
        //
        // Phase-3 shared-proof detection is meaningful only under
        // `AddMultProb` (and `BddExact`, which is the AddMultProb math
        // plus a WMC post-correction). Under `MaxMinProb`, `plus = max`
        // is idempotent — shared proofs don't double-count — so the
        // warning is moot and we skip the work.
        let shared_info = if semiring_kind == SemiringKind::MaxMinProb {
            None
        } else if let Some(ref tracker) = derivation_tracker {
            detect_shared_lineage(rule, &facts, tracker, &warnings_slot, semiring_kind)
        } else {
            None
        };

        // Apply BDD for shared groups if exact_probability is enabled.
        if exact_probability
            && let Some(ref info) = shared_info
            && let Some(ref tracker) = derivation_tracker
        {
            facts = apply_exact_wmc(
                facts,
                rule,
                info,
                tracker,
                max_bdd_variables,
                &warnings_slot,
                &approximate_slot,
            )?;
        }

        let processed = apply_post_fixpoint_chain(
            facts,
            rule,
            &task_ctx,
            strict_probability_domain,
            probability_epsilon,
            semiring_kind,
            derivation_tracker.as_ref().map(Arc::clone),
            top_k_proofs,
            Some(Arc::clone(&registry)),
        )
        .await?;
        all_output.extend(processed);
    }

    // If no output, return empty batch with output schema
    if all_output.is_empty() {
        all_output.push(RecordBatch::new_empty(output_schema));
    }

    Ok(all_output)
}

// ---------------------------------------------------------------------------
// Provenance recording helpers
// ---------------------------------------------------------------------------

/// Record provenance for all newly derived facts (rows in the current delta).
///
/// Called after `merge_delta` returns `true`. Attributes each new fact to the
/// clause most likely to have produced it, using first-derivation-wins semantics.
/// Borrowed bundle of classifier-side runtime state used by
/// provenance / EXPLAIN-reconstruction code paths. Keeps function
/// signatures under the too-many-arguments threshold.
pub(crate) struct ClassifierRefs<'a> {
    pub registry: &'a Arc<ClassifierRegistry>,
    pub cache: Option<&'a Arc<uni_locy::ModelInvocationCache>>,
    /// Phase C B1-B3 follow-up: when `Some`, EXPLAIN's neural_calls
    /// collection consults the side-channel provenance store first
    /// (populated by `apply_model_invocations`). This is the only way
    /// to surface NeuralProvenance for Python-registered classifiers,
    /// whose model_invocations may be rewritten away by the planner
    /// and so wouldn't trigger the re-invocation fallback.
    pub provenance_store: Option<&'a Arc<uni_locy::NeuralProvenanceStore>>,
}

/// Borrowed bundle of provenance-recording state: the in-flight
/// tracker, the derived-scan registry (used to resolve IS-ref inputs),
/// and the shared warnings slot. Bundled to keep
/// `record_provenance` / `record_and_detect_lineage_nonrecursive`
/// under the too-many-arguments threshold.
pub(crate) struct ProvenanceCtx<'a> {
    pub tracker: &'a Arc<ProvenanceStore>,
    pub registry: &'a Arc<DerivedScanRegistry>,
    pub warnings_slot: &'a Arc<StdRwLock<Vec<RuntimeWarning>>>,
}

async fn record_provenance(
    prov: ProvenanceCtx<'_>,
    rule: &FixpointRulePlan,
    state: &FixpointState,
    clause_candidates: &[Vec<RecordBatch>],
    iteration: usize,
    top_k_proofs: usize,
    classifiers: ClassifierRefs<'_>,
) {
    let tracker = prov.tracker;
    let registry = prov.registry;
    let warnings_slot = prov.warnings_slot;
    let classifier_registry = classifiers.registry;
    let classifier_cache = classifiers.cache;
    let all_indices: Vec<usize> = (0..rule.yield_schema.fields().len()).collect();

    // Pre-compute base fact probabilities for top-k mode.
    let base_probs = if top_k_proofs > 0 {
        tracker.base_fact_probs()
    } else {
        HashMap::new()
    };

    let mut topk_acc = TopKProofAccumulator::new();

    for delta_batch in state.all_delta() {
        for row_idx in 0..delta_batch.num_rows() {
            let row_hash = format!(
                "{:?}",
                extract_scalar_key(delta_batch, &all_indices, row_idx)
            )
            .into_bytes();
            let fact_row = batch_row_to_value_map(delta_batch, row_idx);
            let clause_index =
                find_clause_for_row(delta_batch, row_idx, &all_indices, clause_candidates);

            let support = collect_is_ref_inputs(rule, clause_index, delta_batch, row_idx, registry);

            let proof_probability = if top_k_proofs > 0 {
                compute_proof_probability(&support, &base_probs)
            } else {
                None
            };

            let entry = ProvenanceAnnotation {
                rule_name: rule.name.clone(),
                clause_index,
                support,
                along_values: {
                    let along_names: Vec<String> = rule
                        .clauses
                        .get(clause_index)
                        .map(|c| c.along_bindings.clone())
                        .unwrap_or_default();
                    along_names
                        .iter()
                        .filter_map(|name| fact_row.get(name).map(|v| (name.clone(), v.clone())))
                        .collect()
                },
                iteration,
                fact_row: fact_row.clone(),
                proof_probability,
                neural_calls: collect_neural_calls_for_row(
                    rule,
                    clause_index,
                    &fact_row,
                    classifier_registry,
                    classifier_cache,
                    classifiers.provenance_store,
                )
                .await,
            };
            if top_k_proofs > 0 {
                topk_acc.accumulate(&entry, &row_hash);
                tracker.record_top_k(row_hash, entry, top_k_proofs);
            } else {
                tracker.record(row_hash, entry);
            }
        }
    }

    topk_acc.emit_warning_if_any(rule, top_k_proofs, warnings_slot);
}

/// Phase C C0 Stage 2: collects per-row `Proof` tags during the
/// fixpoint row walk, then surfaces `TopKPruningCrossedDependency`
/// when post-walk top-K merging would drop a proof whose base RVs
/// overlap a retained one. The shared `BaseRv` interner is what
/// makes the overlap detectable — proofs grounded in the same
/// `base_fact_id` get the same `BaseRv`.
struct TopKProofAccumulator {
    per_fact: HashMap<Vec<u8>, Vec<uni_locy::Proof>>,
    base_rv_interner: HashMap<Vec<u8>, uni_locy::BaseRv>,
    next_rv: u32,
}

impl TopKProofAccumulator {
    fn new() -> Self {
        Self {
            per_fact: HashMap::new(),
            base_rv_interner: HashMap::new(),
            next_rv: 0,
        }
    }

    fn accumulate(&mut self, entry: &ProvenanceAnnotation, row_hash: &[u8]) {
        let mut base_rvs = uni_locy::BaseRvSet::empty();
        for term in &entry.support {
            let rv = *self
                .base_rv_interner
                .entry(term.base_fact_id.clone())
                .or_insert_with(|| {
                    let r = uni_locy::BaseRv(self.next_rv);
                    self.next_rv += 1;
                    r
                });
            base_rvs.insert(rv);
        }
        self.per_fact
            .entry(row_hash.to_vec())
            .or_default()
            .push(uni_locy::Proof {
                weight: entry.proof_probability.unwrap_or(0.0),
                base_rvs,
                neural_calls: Vec::new(),
            });
    }

    fn emit_warning_if_any(
        &self,
        rule: &FixpointRulePlan,
        top_k_proofs: usize,
        warnings_slot: &Arc<StdRwLock<Vec<RuntimeWarning>>>,
    ) {
        if top_k_proofs == 0 || self.per_fact.is_empty() {
            return;
        }
        let crossed_facts = self
            .per_fact
            .values()
            .filter(|proofs| {
                let (_kept, notice) =
                    uni_locy::merge_top_k_runtime(Vec::new(), (*proofs).clone(), top_k_proofs);
                notice == uni_locy::PruneNotice::CrossedDependency
            })
            .count();
        if crossed_facts == 0 {
            return;
        }
        let Ok(mut w) = warnings_slot.write() else {
            return;
        };
        let already = w.iter().any(|rw| {
            matches!(
                rw.code,
                uni_locy::types::RuntimeWarningCode::TopKPruningCrossedDependency
            ) && rw.rule_name == rule.name
        });
        if already {
            return;
        }
        w.push(RuntimeWarning {
            code: uni_locy::types::RuntimeWarningCode::TopKPruningCrossedDependency,
            rule_name: rule.name.clone(),
            message: format!(
                "rule '{}': top-K proof pruning (k={}) discarded {} fact(s) \
                 whose dependencies overlap retained proofs. The retained \
                 top-{} under-counts the true joint probability for those \
                 facts (Scallop, Huang et al. 2021). Increase k to recover.",
                rule.name, top_k_proofs, crossed_facts, top_k_proofs
            ),
            variable_count: None,
            key_group: None,
        });
    }
}

/// Collect IS-ref input facts for a derived row using provenance join columns.
///
/// For each non-negated IS-ref binding in the clause, extracts body-side key
/// values from the delta row and finds matching source rows in the registry.
/// Returns a `ProofTerm` for each match (with the source fact hash).
/// Phase C B1–B3: build [`uni_locy::NeuralProvenance`] entries for
/// the model invocations on this clause by reading each
/// invocation's output column from the post-LocyModelInvoke row.
/// `raw_probability` is the classifier's direct output;
/// `calibrated_probability` and `confidence_band` come from the
/// active Calibrator (when the classifier wraps one).
///
/// Phase C B1-B3 follow-up rewrite: re-evaluate the classifier
/// per fact using the ORIGINAL pre-rewrite feature expressions
/// (stored as `invocation.original_feature_exprs`). This works
/// for invocations in YIELD, ALONG, and FOLD positions uniformly
/// — the original args carry the input bindings regardless of
/// where the synthetic `__model_<n>` column ends up in the plan
/// tree. Memoization via `ModelInvocationCache` (already threaded)
/// absorbs repeat costs; EXPLAIN typically operates on small
/// derivation trees so the per-fact classifier call is bounded.
async fn collect_neural_calls_for_row(
    rule: &FixpointRulePlan,
    clause_index: usize,
    fact_row: &uni_locy::FactRow,
    classifier_registry: &Arc<ClassifierRegistry>,
    classifier_cache: Option<&Arc<uni_locy::ModelInvocationCache>>,
    provenance_store: Option<&Arc<uni_locy::NeuralProvenanceStore>>,
) -> Vec<uni_locy::NeuralProvenance> {
    let Some(clause) = rule.clauses.get(clause_index) else {
        return Vec::new();
    };
    if clause.model_invocations.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(clause.model_invocations.len());
    for invocation in &clause.model_invocations {
        // Build ClassifyInput from the REWRITTEN feature expressions
        // (referencing the synthetic `__feat_*` hidden columns that the
        // planner lifts model-call args into). This matches the writer's
        // path in `apply_model_invocations`, which iterates
        // `invocation.feature_exprs` to compute the same `input_hash`
        // that gets stored. Using the pre-rewrite `original_feature_exprs`
        // here would compute a different hash for YIELD-position
        // invocations (where the pre-rewrite expr references properties
        // not materialised into fact_row), causing the store lookup
        // below to miss and `neural_calls` to come back empty.
        let mut features = std::collections::HashMap::new();
        for (binding_name, feat_expr) in invocation
            .feature_names
            .iter()
            .zip(invocation.feature_exprs.iter())
        {
            features.insert(
                binding_name.clone(),
                eval_feature_expr_against_fact_row(feat_expr, fact_row),
            );
        }
        let input = uni_locy::ClassifyInput { features };
        let input_hash = input.stable_hash();

        // Store-first read path. `apply_model_invocations` writes a
        // NeuralProvenanceRecord per (model, input_hash) into the
        // side-channel store during fixpoint. If we find a record
        // there, surface it directly — this is the only path that
        // populates calibrated_probability + confidence_band for
        // Python-registered classifiers.
        if let Some(store) = provenance_store
            && let Some(record) = store.get(&invocation.model_name, input_hash)
        {
            out.push(uni_locy::NeuralProvenance {
                model_name: invocation.model_name.clone(),
                raw_probability: record.raw_probability,
                calibrated_probability: record.calibrated_probability,
                confidence_band: record.confidence_band,
            });
            continue;
        }

        // Fallback: re-invoke the classifier. Only reached when the
        // store wasn't populated for this (model, input) — e.g. older
        // sessions where the store wasn't threaded, or when EXPLAIN
        // runs against a row that fixpoint never touched.
        let Some(classifier) = classifier_registry.get(&invocation.model_name) else {
            continue;
        };
        let raw = if let Some(v) =
            classifier_cache.and_then(|c| c.get(&invocation.model_name, input_hash))
        {
            v
        } else {
            match classifier.classify(std::slice::from_ref(&input)).await {
                Ok(probs) => {
                    let v = probs.first().copied().unwrap_or(0.0);
                    if let Some(c) = classifier_cache {
                        c.insert(&invocation.model_name, input_hash, v);
                    }
                    v
                }
                Err(_) => continue,
            }
        };
        let calibrator = classifier.get_calibrator();
        let calibrated_probability = calibrator.as_ref().map(|_| raw);
        let confidence_band = calibrator.as_ref().and_then(|c| c.confidence_band(raw));
        out.push(uni_locy::NeuralProvenance {
            model_name: invocation.model_name.clone(),
            raw_probability: raw,
            calibrated_probability,
            confidence_band,
        });
    }
    out
}

/// Phase C B1-B3 follow-up: evaluate a model's pre-rewrite feature
/// expression against a fact_row, producing a `FeatureValue` for
/// classifier input reconstruction. Mirrors the compile-time
/// acceptance set in `validate_features`:
/// - `Variable(name)` → fact_row[name], coerced.
/// - `Property(Variable(v), prop)` → fact_row["v.prop"] (the
///   materialized property column).
fn eval_feature_expr_against_fact_row(
    expr: &uni_cypher::ast::Expr,
    fact_row: &uni_locy::FactRow,
) -> uni_locy::FeatureValue {
    use uni_cypher::ast::Expr;
    use uni_locy::FeatureValue;
    let value_to_feature = |v: Option<&uni_common::Value>| -> FeatureValue {
        match v {
            Some(uni_common::Value::Float(f)) => FeatureValue::Float(*f),
            Some(uni_common::Value::Int(i)) => FeatureValue::Int(*i),
            Some(uni_common::Value::Bool(b)) => FeatureValue::Bool(*b),
            Some(uni_common::Value::String(s)) => FeatureValue::String(s.clone()),
            Some(uni_common::Value::Node(n)) => {
                // Encode node by vid for `scorer(s)` style.
                FeatureValue::Int(n.vid.as_u64() as i64)
            }
            _ => FeatureValue::Null,
        }
    };
    // Phase D D1: resolve a sub-expression to its raw `uni_common::Value`
    // for the `similar_to` UDF input. Falls back to the node's property
    // when the materialized column key isn't directly present.
    let resolve_value = |sub: &Expr| -> uni_common::Value {
        match sub {
            Expr::Variable(name) => fact_row
                .get(name)
                .cloned()
                .unwrap_or(uni_common::Value::Null),
            Expr::Property(boxed, prop) if matches!(boxed.as_ref(), Expr::Variable(_)) => {
                let Expr::Variable(v) = boxed.as_ref() else {
                    unreachable!()
                };
                let key = format!("{}.{}", v, prop);
                if let Some(val) = fact_row.get(&key) {
                    return val.clone();
                }
                if let Some(uni_common::Value::Node(n)) = fact_row.get(v) {
                    return n
                        .properties
                        .get(prop)
                        .cloned()
                        .unwrap_or(uni_common::Value::Null);
                }
                uni_common::Value::Null
            }
            Expr::Literal(lit) => lit.to_value(),
            Expr::List(items) => {
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    out.push(match it {
                        Expr::Literal(lit) => lit.to_value(),
                        _ => uni_common::Value::Null,
                    });
                }
                uni_common::Value::List(out)
            }
            _ => uni_common::Value::Null,
        }
    };

    match expr {
        Expr::Variable(name) => value_to_feature(fact_row.get(name)),
        Expr::Property(boxed, prop) => {
            if let Expr::Variable(v) = boxed.as_ref() {
                // Try the materialized property column first.
                let key = format!("{}.{}", v, prop);
                if let Some(val) = fact_row.get(&key) {
                    return value_to_feature(Some(val));
                }
                // Fallback: try the synthetic hidden column that the
                // planner injects for property-access feature args
                // (`__feat_<var>_<prop>`). The writer side
                // (`apply_model_invocations`) already uses this
                // fallback (see `resolve_src` in the same file), so
                // mirroring it here keeps reader/writer input_hash
                // symmetric — without it, the YIELD-position case
                // (where `fact_row[v]` is a vid Int, not a Node)
                // returns Null and the store-lookup misses.
                let hidden_key = format!("__feat_{}_{}", v, prop);
                if let Some(val) = fact_row.get(&hidden_key) {
                    return value_to_feature(Some(val));
                }
                // Final fallback: read property directly from the
                // node value (works when fact_row carries the Node
                // rather than a vid Int).
                if let Some(uni_common::Value::Node(n)) = fact_row.get(v) {
                    return value_to_feature(n.properties.get(prop));
                }
            }
            FeatureValue::Null
        }
        Expr::FunctionCall { name, args, .. } if name == "similar_to" && args.len() == 2 => {
            let lv = resolve_value(&args[0]);
            let rv = resolve_value(&args[1]);
            match crate::query::similar_to::eval_similar_to_pure(&lv, &rv) {
                Ok(uni_common::Value::Float(f)) => FeatureValue::Float(f),
                _ => FeatureValue::Null,
            }
        }
        // `semantic_match` requires the Xervo embedder at this scope, which
        // is not threaded into the EXPLAIN re-evaluation path. Surface as
        // Null so neural-provenance still renders for the rest of the row.
        //
        // Phase D D1 graph-structural FunctionCalls (`degree_centrality`,
        // `pagerank_score`, `closeness_centrality`, `avg_neighbor`,
        // `max_neighbor`, `sum_neighbor`) require the `GraphAlgoHandle`
        // (algorithm registry + storage + PropertyManager) and an async
        // re-precompute pass — none of which are reachable from this
        // synchronous fact-row evaluator. Mode B re-evaluation surfaces
        // them as Null; the authoritative hot-path values are recorded
        // in `NeuralProvenanceStore` per fact (the EXPLAIN renderer
        // consults the store first when configured, falling back to
        // Mode B re-evaluation only as a backup).
        Expr::FunctionCall { name, .. }
            if matches!(
                name.as_str(),
                "degree_centrality"
                    | "pagerank_score"
                    | "closeness_centrality"
                    | "betweenness_centrality"
                    | "eigenvector_centrality"
                    | "harmonic_centrality"
                    | "katz_centrality"
                    | "avg_neighbor"
                    | "max_neighbor"
                    | "sum_neighbor"
            ) =>
        {
            FeatureValue::Null
        }
        _ => FeatureValue::Null,
    }
}

fn collect_is_ref_inputs(
    rule: &FixpointRulePlan,
    clause_index: usize,
    delta_batch: &RecordBatch,
    row_idx: usize,
    registry: &Arc<DerivedScanRegistry>,
) -> Vec<ProofTerm> {
    let clause = match rule.clauses.get(clause_index) {
        Some(c) => c,
        None => return vec![],
    };

    let mut inputs = Vec::new();
    let delta_schema = delta_batch.schema();

    for binding in &clause.is_ref_bindings {
        if binding.negated {
            continue;
        }
        if binding.provenance_join_cols.is_empty() {
            continue;
        }

        // Extract body-side values from the delta row for each provenance join col.
        let body_values: Vec<(String, ScalarKey)> = binding
            .provenance_join_cols
            .iter()
            .filter_map(|(body_col, _derived_col)| {
                let col_idx = delta_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == body_col)?;
                let key = extract_scalar_key(delta_batch, &[col_idx], row_idx);
                Some((body_col.clone(), key.into_iter().next()?))
            })
            .collect();

        if body_values.len() != binding.provenance_join_cols.len() {
            continue;
        }

        // Read current data from the registry entry for this IS-ref's rule.
        let entry = match registry.get(binding.derived_scan_index) {
            Some(e) => e,
            None => continue,
        };
        let source_batches = entry.data.read();
        let source_schema = &entry.schema;

        // Find matching source rows and hash them.
        for src_batch in source_batches.iter() {
            let all_src_indices: Vec<usize> = (0..src_batch.num_columns()).collect();
            for src_row in 0..src_batch.num_rows() {
                let matches = binding.provenance_join_cols.iter().enumerate().all(
                    |(i, (_body_col, derived_col))| {
                        let src_col_idx = source_schema
                            .fields()
                            .iter()
                            .position(|f| f.name() == derived_col);
                        match src_col_idx {
                            Some(idx) => {
                                let src_key = extract_scalar_key(src_batch, &[idx], src_row);
                                src_key.first() == Some(&body_values[i].1)
                            }
                            None => false,
                        }
                    },
                );
                if matches {
                    let fact_hash = format!(
                        "{:?}",
                        extract_scalar_key(src_batch, &all_src_indices, src_row)
                    )
                    .into_bytes();
                    inputs.push(ProofTerm {
                        source_rule: binding.rule_name.clone(),
                        base_fact_id: fact_hash,
                    });
                }
            }
        }
    }

    inputs
}

/// Phase D D-C0: per-body-row variant of [`collect_is_ref_inputs`] used
/// to pre-populate `FoldExec`'s `body_support_map` for TopKProofs MNOR.
///
/// At FOLD time, the rule's own facts haven't been recorded in the
/// `ProvenanceStore` yet (`record_provenance` runs after fact
/// materialization, and is keyed by post-YIELD hashes anyway), so the
/// support set for each pre-fold body row must be reconstructed
/// directly from the rule's IS-ref bindings + the source rules'
/// registry data.
///
/// We don't know which clause produced each body row at this point —
/// the iteration-local `clause_candidates` are gone — so we iterate
/// **every** clause's `is_ref_bindings`. The `provenance_join_cols`
/// schema check inside `collect_is_ref_inputs` already skips bindings
/// whose body columns aren't in the row's schema, so cross-clause
/// contamination is bounded (a binding only matches if its body cols
/// are present and the values join). For the single-clause TopKProofs
/// scenarios in TCK this is exact; for multi-clause TopKProofs rules
/// it is a conservative over-approximation that may inflate base-RV
/// counts (treated as the same RV under interning) — acceptable
/// because the DNF math collapses duplicates by inclusion-exclusion.
fn collect_is_ref_inputs_for_body_row(
    rule: &FixpointRulePlan,
    delta_batch: &RecordBatch,
    row_idx: usize,
    registry: &Arc<DerivedScanRegistry>,
) -> Vec<ProofTerm> {
    let mut combined: Vec<ProofTerm> = Vec::new();
    for clause_index in 0..rule.clauses.len() {
        let part = collect_is_ref_inputs(rule, clause_index, delta_batch, row_idx, registry);
        combined.extend(part);
    }
    combined
}

// ---------------------------------------------------------------------------
// Shared-lineage detection
// ---------------------------------------------------------------------------

/// Detect KEY groups in a rule's pre-fold facts where recursive derivation
/// may violate the independence assumption of MNOR/MPROD.
///
/// Uses a two-tier strategy:
/// 1. **Precise**: If the `ProvenanceStore` has populated `support` for facts
///    in the group, we recursively compute lineage (Cui & Widom 2000) and
///    check for pairwise overlap. A shared base fact proves a dependency.
/// 2. **Structural fallback**: When lineage tracking is unavailable (e.g., the
///    IS-ref subject variables were projected away), we check whether any fact
///    in a multi-row group was derived by a clause that has IS-ref bindings.
///    Recursive derivation through shared relations is a strong signal that
///    proof paths may share intermediate nodes.
///
/// Per-row data collected during shared-lineage detection.
#[expect(
    dead_code,
    reason = "Fields accessed via SharedLineageInfo in detect_shared_lineage"
)]
pub(crate) struct SharedGroupRow {
    pub fact_hash: Vec<u8>,
    pub lineage: HashSet<Vec<u8>>,
}

/// Information about groups with shared proofs, returned by `detect_shared_lineage`.
pub(crate) struct SharedLineageInfo {
    /// KEY group → rows with their base fact sets.
    pub shared_groups: HashMap<Vec<ScalarKey>, Vec<SharedGroupRow>>,
}

/// Build a byte key that uniquely identifies a row across all columns.
pub(crate) fn fact_hash_key(batch: &RecordBatch, all_indices: &[usize], row_idx: usize) -> Vec<u8> {
    format!("{:?}", extract_scalar_key(batch, all_indices, row_idx)).into_bytes()
}

/// Emits at most one `SharedProbabilisticDependency` warning per rule.
/// Returns `Some(SharedLineageInfo)` if any group has shared proofs.
fn detect_shared_lineage(
    rule: &FixpointRulePlan,
    pre_fold_facts: &[RecordBatch],
    tracker: &Arc<ProvenanceStore>,
    warnings_slot: &Arc<StdRwLock<Vec<RuntimeWarning>>>,
    semiring_kind: SemiringKind,
) -> Option<SharedLineageInfo> {
    use uni_locy::{RuntimeWarning, RuntimeWarningCode};

    // Only check rules with probability-domain fold bindings. M3:
    // dispatches via the `LocyAggregate` trait so user-authored
    // probability aggregates participate automatically — selected by the
    // trait's `is_probability_aggregate()` flag, not by hardcoded name.
    let has_prob_fold = rule
        .fold_bindings
        .iter()
        .any(|fb| fb.aggregate.is_probability_aggregate());
    if !has_prob_fold {
        return None;
    }

    // Group facts by KEY columns.
    let key_indices = &rule.key_column_indices;
    let all_indices: Vec<usize> = (0..rule.yield_schema.fields().len()).collect();

    let mut groups: HashMap<Vec<ScalarKey>, Vec<Vec<u8>>> = HashMap::new();
    for batch in pre_fold_facts {
        for row_idx in 0..batch.num_rows() {
            let key = extract_scalar_key(batch, key_indices, row_idx);
            let fact_hash = fact_hash_key(batch, &all_indices, row_idx);
            groups.entry(key).or_default().push(fact_hash);
        }
    }

    let mut shared_groups: HashMap<Vec<ScalarKey>, Vec<SharedGroupRow>> = HashMap::new();
    let mut any_shared = false;

    // Check each group with ≥2 rows.
    for (key, fact_hashes) in &groups {
        if fact_hashes.len() < 2 {
            continue;
        }

        // Tier 1: precise base-fact overlap detection via tracker inputs.
        let mut has_inputs = false;
        let mut per_row_bases: Vec<HashSet<Vec<u8>>> = Vec::new();
        for fh in fact_hashes {
            let bases = compute_lineage(fh, tracker, &mut HashSet::new());
            if let Some(entry) = tracker.lookup(fh)
                && !entry.support.is_empty()
            {
                has_inputs = true;
            }
            per_row_bases.push(bases);
        }

        let shared_found = if has_inputs {
            // At least some facts have tracked inputs — do precise comparison.
            let mut found = false;
            'outer: for i in 0..per_row_bases.len() {
                for j in (i + 1)..per_row_bases.len() {
                    if !per_row_bases[i].is_disjoint(&per_row_bases[j]) {
                        found = true;
                        break 'outer;
                    }
                }
            }
            found
        } else {
            // Tier 2: structural fallback — check if any fact in the group was
            // derived by a clause with IS-ref bindings (recursive derivation).
            fact_hashes.iter().any(|fh| {
                tracker.lookup(fh).is_some_and(|entry| {
                    rule.clauses
                        .get(entry.clause_index)
                        .is_some_and(|clause| clause.is_ref_bindings.iter().any(|b| !b.negated))
                })
            })
        };

        if shared_found {
            any_shared = true;
            // Collect the group rows with their base facts for BDD use.
            let rows: Vec<SharedGroupRow> = fact_hashes
                .iter()
                .zip(per_row_bases)
                .map(|(fh, bases)| SharedGroupRow {
                    fact_hash: fh.clone(),
                    lineage: bases,
                })
                .collect();
            shared_groups.insert(key.clone(), rows);
        }
    }

    // Phase 5: Cross-group correlation warning.
    // Check if any IS-ref input fact appears in multiple KEY groups.
    // This is independent of within-group sharing: even rules whose KEY groups
    // each have only one post-fold row can exhibit cross-group correlation when
    // different groups consume the same IS-ref base fact.
    {
        let mut input_to_groups: HashMap<Vec<u8>, HashSet<Vec<ScalarKey>>> = HashMap::new();
        for (key, fact_hashes) in &groups {
            for fh in fact_hashes {
                if let Some(entry) = tracker.lookup(fh) {
                    for input in &entry.support {
                        input_to_groups
                            .entry(input.base_fact_id.clone())
                            .or_default()
                            .insert(key.clone());
                    }
                }
            }
        }
        let has_cross_group = input_to_groups.values().any(|g| g.len() > 1);
        if has_cross_group && let Ok(mut warnings) = warnings_slot.write() {
            let already_warned = warnings.iter().any(|w| {
                w.code == RuntimeWarningCode::CrossGroupCorrelationNotExact
                    && w.rule_name == rule.name
            });
            if !already_warned {
                // Phase D F3: pick one canonical example of a shared
                // input fact and the KEY groups it bridges, so users
                // can correlate the warning with EXPLAIN output.
                let example =
                    input_to_groups
                        .iter()
                        .find(|(_, g)| g.len() > 1)
                        .map(|(input, groups)| {
                            let short = input
                                .iter()
                                .take(8)
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>();
                            let mut group_strs: Vec<String> =
                                groups.iter().map(|k| format!("{:?}", k)).collect();
                            group_strs.sort();
                            format!(
                                "input {} shared by groups [{}]",
                                short,
                                group_strs.join(", ")
                            )
                        });
                // Phase D F3 case 1 BDD-time deepening: count distinct
                // base facts (= BDD variables) that cross groups, so the
                // warning carries structured metadata mirroring
                // `BddLimitExceeded`. Users can correlate
                // `variable_count` with EXPLAIN's BDD output.
                let shared_variable_count =
                    input_to_groups.values().filter(|g| g.len() > 1).count();
                warnings.push(RuntimeWarning {
                    code: RuntimeWarningCode::CrossGroupCorrelationNotExact,
                    message: format!(
                        "Rule '{}': {} IS-ref base fact(s) are shared across different \
                         KEY groups. BDD corrects per-group probabilities but cannot \
                         account for cross-group correlations.",
                        rule.name, shared_variable_count
                    ),
                    rule_name: rule.name.clone(),
                    variable_count: Some(shared_variable_count),
                    key_group: example,
                });
            }
        }
    }

    if any_shared {
        // Phase D D-C0b: under `SemiringKind::TopKProofs`, the FOLD-time
        // DNF inclusion-exclusion math (shipped in D-C0) auto-corrects
        // for within-group base-fact sharing — the "Results may
        // overestimate" premise of `SharedProbabilisticDependency`
        // is no longer true. Suppress the warning under TopK; users
        // who chose TopKProofs explicitly opted into the
        // correctness-preserving path. Cross-group correlation
        // (`CrossGroupCorrelationNotExact`) still fires above because
        // D-C0 doesn't span KEY-group boundaries.
        let suppress_under_topk = matches!(semiring_kind, SemiringKind::TopKProofs { .. });
        if !suppress_under_topk && let Ok(mut warnings) = warnings_slot.write() {
            let already_warned = warnings.iter().any(|w| {
                w.code == RuntimeWarningCode::SharedProbabilisticDependency
                    && w.rule_name == rule.name
            });
            if !already_warned {
                warnings.push(RuntimeWarning {
                    code: RuntimeWarningCode::SharedProbabilisticDependency,
                    message: format!(
                        "Rule '{}' aggregates with MNOR/MPROD but some proof paths \
                         share intermediate facts, violating the independence assumption. \
                         Results may overestimate probability.",
                        rule.name
                    ),
                    rule_name: rule.name.clone(),
                    variable_count: None,
                    key_group: None,
                });
            }
        }
        Some(SharedLineageInfo { shared_groups })
    } else {
        None
    }
}

/// Record provenance and detect shared proofs for non-recursive strata.
///
/// Non-recursive rules are evaluated in a single pass (no fixpoint loop), so
/// the regular `record_provenance` + `detect_shared_lineage` path is never hit.
/// This function bridges that gap by recording a `ProvenanceAnnotation` for every
/// fact produced by each clause and then running the same two-tier detection
/// logic used by the recursive path.
#[expect(
    clippy::too_many_arguments,
    reason = "context bundle would be over-engineering for one call site"
)]
pub(crate) async fn record_and_detect_lineage_nonrecursive(
    rule: &FixpointRulePlan,
    tagged_clause_facts: &[(usize, Vec<RecordBatch>)],
    tracker: &Arc<ProvenanceStore>,
    warnings_slot: &Arc<StdRwLock<Vec<RuntimeWarning>>>,
    registry: &Arc<DerivedScanRegistry>,
    top_k_proofs: usize,
    classifiers: ClassifierRefs<'_>,
    semiring_kind: SemiringKind,
) -> Option<SharedLineageInfo> {
    let classifier_registry = classifiers.registry;
    let classifier_cache = classifiers.cache;
    let all_indices: Vec<usize> = (0..rule.yield_schema.fields().len()).collect();

    // Pre-compute base fact probabilities for top-k mode.
    let base_probs = if top_k_proofs > 0 {
        tracker.base_fact_probs()
    } else {
        HashMap::new()
    };

    let mut topk_acc = TopKProofAccumulator::new();

    // Record provenance for each clause's facts.
    for (clause_index, batches) in tagged_clause_facts {
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let row_hash = fact_hash_key(batch, &all_indices, row_idx);
                let fact_row = batch_row_to_value_map(batch, row_idx);

                let support = collect_is_ref_inputs(rule, *clause_index, batch, row_idx, registry);

                let proof_probability = if top_k_proofs > 0 {
                    compute_proof_probability(&support, &base_probs)
                } else {
                    None
                };

                let entry = ProvenanceAnnotation {
                    rule_name: rule.name.clone(),
                    clause_index: *clause_index,
                    support,
                    along_values: {
                        let along_names: Vec<String> = rule
                            .clauses
                            .get(*clause_index)
                            .map(|c| c.along_bindings.clone())
                            .unwrap_or_default();
                        along_names
                            .iter()
                            .filter_map(|name| {
                                fact_row.get(name).map(|v| (name.clone(), v.clone()))
                            })
                            .collect()
                    },
                    iteration: 0,
                    fact_row: fact_row.clone(),
                    proof_probability,
                    neural_calls: collect_neural_calls_for_row(
                        rule,
                        *clause_index,
                        &fact_row,
                        classifier_registry,
                        classifier_cache,
                        classifiers.provenance_store,
                    )
                    .await,
                };
                if top_k_proofs > 0 {
                    topk_acc.accumulate(&entry, &row_hash);
                    tracker.record_top_k(row_hash, entry, top_k_proofs);
                } else {
                    tracker.record(row_hash, entry);
                }
            }
        }
    }

    topk_acc.emit_warning_if_any(rule, top_k_proofs, warnings_slot);

    // Flatten all clause facts and run detection.
    let all_facts: Vec<RecordBatch> = tagged_clause_facts
        .iter()
        .flat_map(|(_, batches)| batches.iter().cloned())
        .collect();
    detect_shared_lineage(rule, &all_facts, tracker, warnings_slot, semiring_kind)
}

/// Apply exact weighted model counting (WMC) for shared-lineage groups.
///
/// Replaces multiple rows in groups with shared lineage with a single
/// representative row whose PROB column carries the BDD-computed exact
/// probability (Sang et al. 2005). For groups that exceed
/// `max_bdd_variables`, rows are left unchanged and a `BddLimitExceeded`
/// warning is emitted.
pub(crate) fn apply_exact_wmc(
    pre_fold_facts: Vec<RecordBatch>,
    rule: &FixpointRulePlan,
    shared_info: &SharedLineageInfo,
    tracker: &Arc<ProvenanceStore>,
    max_bdd_variables: usize,
    warnings_slot: &Arc<StdRwLock<Vec<RuntimeWarning>>>,
    approximate_slot: &Arc<StdRwLock<HashMap<String, Vec<String>>>>,
) -> DFResult<Vec<RecordBatch>> {
    use crate::query::df_graph::locy_bdd::{SemiringOp, weighted_model_count};
    use uni_locy::{RuntimeWarning, RuntimeWarningCode};

    // Find the probability-domain fold binding to know which column
    // to overwrite. M3: dispatch through the `LocyAggregate` trait so
    // user-authored probability aggregates participate.
    let prob_fold = rule
        .fold_bindings
        .iter()
        .find(|fb| fb.aggregate.is_probability_aggregate());
    let prob_fold = match prob_fold {
        Some(f) => f,
        None => return Ok(pre_fold_facts),
    };
    let semiring_op = if prob_fold.aggregate.is_noisy_or() {
        SemiringOp::Disjunction
    } else {
        SemiringOp::Conjunction
    };
    let prob_col_idx = prob_fold.input_col_index;
    // Fail loud rather than panic if the PROB column index is out of range
    // (the planner keeps it within the yield schema; a miss signals a bug).
    let prob_col_name = rule
        .yield_schema
        .fields()
        .get(prob_col_idx)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!(
                "PROB aggregate '{}' column index {} out of range ({} yield columns)",
                prob_fold.name,
                prob_col_idx,
                rule.yield_schema.fields().len(),
            ))
        })?
        .name()
        .clone();

    let key_indices = &rule.key_column_indices;
    let all_indices: Vec<usize> = (0..rule.yield_schema.fields().len()).collect();

    // Build a set of shared group keys for quick lookup.
    let shared_keys: HashSet<Vec<ScalarKey>> = shared_info.shared_groups.keys().cloned().collect();

    // Phase 1: Collect all rows for each shared KEY group across all batches.
    // Store (batch_index, row_index) pairs for each group.
    struct GroupAccum {
        base_facts: Vec<HashSet<Vec<u8>>>,
        base_probs: HashMap<Vec<u8>, f64>,
        /// First occurrence: (batch_index, row_index) — used as representative.
        representative: (usize, usize),
        row_locations: Vec<(usize, usize)>,
    }

    let mut group_accums: HashMap<Vec<ScalarKey>, GroupAccum> = HashMap::new();
    let mut non_shared_rows: Vec<(usize, usize)> = Vec::new(); // (batch_idx, row_idx)

    for (batch_idx, batch) in pre_fold_facts.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            let key = extract_scalar_key(batch, key_indices, row_idx);
            if shared_keys.contains(&key) {
                let fact_hash = fact_hash_key(batch, &all_indices, row_idx);
                let bases = compute_lineage(&fact_hash, tracker, &mut HashSet::new());

                let accum = group_accums.entry(key).or_insert_with(|| GroupAccum {
                    base_facts: Vec::new(),
                    base_probs: HashMap::new(),
                    representative: (batch_idx, row_idx),
                    row_locations: Vec::new(),
                });

                // Look up probabilities for base facts.
                for bf in &bases {
                    if !accum.base_probs.contains_key(bf)
                        && let Some(entry) = tracker.lookup(bf)
                        && let Some(val) = entry.fact_row.get(&prob_col_name)
                        && let Some(p) = value_to_f64(val)
                    {
                        accum.base_probs.insert(bf.clone(), p);
                    }
                }

                accum.base_facts.push(bases);
                accum.row_locations.push((batch_idx, row_idx));
            } else {
                non_shared_rows.push((batch_idx, row_idx));
            }
        }
    }

    // Phase 2: Compute BDD for each shared group (across all batches).
    // Track which (batch_idx, row_idx) pairs to keep vs drop.
    let mut keep_rows: HashSet<(usize, usize)> = HashSet::new();
    // Map of (batch_idx, row_idx) → overridden PROB value (for BDD-succeeded groups).
    let mut overrides: HashMap<(usize, usize), f64> = HashMap::new();

    // All non-shared rows are kept.
    for &loc in &non_shared_rows {
        keep_rows.insert(loc);
    }

    for (key, accum) in &group_accums {
        let bdd_result = weighted_model_count(
            &accum.base_facts,
            &accum.base_probs,
            semiring_op,
            max_bdd_variables,
        );

        if bdd_result.approximated {
            // Emit BddLimitExceeded warning (one per key group).
            if let Ok(mut warnings) = warnings_slot.write() {
                let key_desc = format!("{key:?}");
                let already_warned = warnings.iter().any(|w| {
                    w.code == RuntimeWarningCode::BddLimitExceeded
                        && w.rule_name == rule.name
                        && w.key_group.as_deref() == Some(&key_desc)
                });
                if !already_warned {
                    warnings.push(RuntimeWarning {
                        code: RuntimeWarningCode::BddLimitExceeded,
                        message: format!(
                            "Rule '{}': BDD variable limit exceeded ({} > {}). \
                             Falling back to independence-mode result.",
                            rule.name, bdd_result.variable_count, max_bdd_variables
                        ),
                        rule_name: rule.name.clone(),
                        variable_count: Some(bdd_result.variable_count),
                        key_group: Some(key_desc),
                    });
                }
            }
            if let Ok(mut approx) = approximate_slot.write() {
                let key_desc = format!("{key:?}");
                approx.entry(rule.name.clone()).or_default().push(key_desc);
            }
            // Keep all rows unchanged.
            for &loc in &accum.row_locations {
                keep_rows.insert(loc);
            }
        } else {
            // BDD succeeded: keep one representative row with overridden PROB.
            keep_rows.insert(accum.representative);
            overrides.insert(accum.representative, bdd_result.probability);
        }
    }

    // Phase 3: Build output batches by filtering kept rows per batch.
    let mut result_batches = Vec::new();
    for (batch_idx, batch) in pre_fold_facts.iter().enumerate() {
        let kept_indices: Vec<usize> = (0..batch.num_rows())
            .filter(|&row_idx| keep_rows.contains(&(batch_idx, row_idx)))
            .collect();

        if kept_indices.is_empty() {
            continue;
        }

        let indices = arrow::array::UInt32Array::from(
            kept_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        let mut columns: Vec<arrow::array::ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| arrow::compute::take(col, &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(arrow_err)?;

        // Check if any kept rows have PROB overrides.
        let override_map: Vec<Option<f64>> = kept_indices
            .iter()
            .map(|&row_idx| overrides.get(&(batch_idx, row_idx)).copied())
            .collect();

        // Resolve the PROB column position in THIS batch by name rather than by
        // the yield-schema position `prob_col_idx`: the post-fixpoint fact batch
        // may column-order differently from the yield schema, so a raw positional
        // write would overwrite the wrong column. `columns` mirrors `batch`'s
        // order (built from `batch.columns()`), and the result reuses
        // `batch.schema()`, so the batch-local index is the correct target.
        let batch_prob_idx = batch.schema().index_of(&prob_col_name).ok();
        if override_map.iter().any(|o| o.is_some())
            && let Some(bpi) = batch_prob_idx
            && bpi < columns.len()
        {
            // Rebuild the PROB column with overrides.
            let existing_prob = columns[bpi]
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>();
            let new_values: Vec<f64> = override_map
                .iter()
                .enumerate()
                .map(|(i, ov)| match ov {
                    Some(p) => *p,
                    None => existing_prob.map(|arr| arr.value(i)).unwrap_or(0.0),
                })
                .collect();
            columns[bpi] = Arc::new(arrow::array::Float64Array::from(new_values));
        }

        let result_batch = RecordBatch::try_new(batch.schema(), columns).map_err(arrow_err)?;
        result_batches.push(result_batch);
    }

    Ok(result_batches)
}

/// Extract an f64 from a `Value`, supporting Float and Int.
fn value_to_f64(val: &uni_common::Value) -> Option<f64> {
    match val {
        uni_common::Value::Float(f) => Some(*f),
        uni_common::Value::Int(i) => Some(*i as f64),
        _ => None,
    }
}

/// Compute the lineage of a derived fact (Cui & Widom 2000).
///
/// Recursively traverses the provenance store to collect the set of base-level
/// fact hashes that contribute to this derivation. A base fact is one with no
/// IS-ref support (a graph-level fact). Intermediate facts are expanded
/// transitively through the store.
fn compute_lineage(
    fact_hash: &[u8],
    tracker: &Arc<ProvenanceStore>,
    visited: &mut HashSet<Vec<u8>>,
) -> HashSet<Vec<u8>> {
    if !visited.insert(fact_hash.to_vec()) {
        return HashSet::new(); // Cycle guard.
    }

    match tracker.lookup(fact_hash) {
        Some(entry) if !entry.support.is_empty() => {
            let mut bases = HashSet::new();
            for input in &entry.support {
                let child_bases = compute_lineage(&input.base_fact_id, tracker, visited);
                bases.extend(child_bases);
            }
            bases
        }
        _ => {
            // Base fact (no tracker entry or no inputs).
            let mut set = HashSet::new();
            set.insert(fact_hash.to_vec());
            set
        }
    }
}

/// Determine which clause produced a given row by checking each clause's candidates.
///
/// Returns the index of the first clause whose candidates contain a matching row.
/// Falls back to 0 if no match is found.
fn find_clause_for_row(
    delta_batch: &RecordBatch,
    row_idx: usize,
    all_indices: &[usize],
    clause_candidates: &[Vec<RecordBatch>],
) -> usize {
    let target_key = extract_scalar_key(delta_batch, all_indices, row_idx);
    for (clause_idx, batches) in clause_candidates.iter().enumerate() {
        for batch in batches {
            if batch.num_columns() != all_indices.len() {
                continue;
            }
            for r in 0..batch.num_rows() {
                if extract_scalar_key(batch, all_indices, r) == target_key {
                    return clause_idx;
                }
            }
        }
    }
    0
}

/// Convert a single row from a `RecordBatch` at `row_idx` into a `HashMap<String, Value>`.
fn batch_row_to_value_map(
    batch: &RecordBatch,
    row_idx: usize,
) -> std::collections::HashMap<String, Value> {
    use uni_store::storage::arrow_convert::arrow_to_value;

    let schema = batch.schema();
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            let col = batch.column(col_idx);
            let val = arrow_to_value(col.as_ref(), row_idx, None).canonical_entity();
            (field.name().clone(), val)
        })
        .collect()
}

// ─── Phase B Slice 3: neural-model invocation pass ───────────────────────
//
// `apply_model_invocations` runs every `ModelInvocation` lifted from a
// clause's YIELD items against the body's output batches. For each
// invocation it:
//
//   1. Resolves each `feature_expr` to a column in the batch — Slice 3
//      supports plain `Expr::Variable("name")` references; richer
//      expressions (property access, nested calls) are deferred.
//   2. Builds one `ClassifyInput` per row keyed by the model's input
//      binding names.
//   3. Issues a single batched `NeuralClassifier::classify` call.
//   4. Appends the resulting `Float64Array` as a new column matching
//      `invocation.output_column`.
//
// Errors:
//   * `UnknownNeuralModel`: the model name isn't in the registry.
//   * Mismatched feature-expr / column: returned as a DataFusion
//     Execution error.

#[expect(
    clippy::too_many_arguments,
    reason = "model-invocation plumbing: registry, cache, provenance and three runtime handles"
)]
pub(crate) async fn apply_model_invocations(
    batches: Vec<RecordBatch>,
    invocations: &[uni_locy::ModelInvocation],
    registry: &Arc<ClassifierRegistry>,
    cache: Option<&Arc<uni_locy::ModelInvocationCache>>,
    provenance_store: Option<&Arc<uni_locy::NeuralProvenanceStore>>,
    path_context_handles: &HashMap<
        String,
        crate::query::df_graph::locy_model_invoke::PathContextHandle,
    >,
    xervo_runtime: &crate::query::df_graph::locy_model_invoke::XervoRuntimeHandle,
    graph_algo: &crate::query::df_graph::locy_model_invoke::GraphAlgoHandle,
) -> DFResult<Vec<RecordBatch>> {
    use uni_locy::ClassifyInput;
    if batches.is_empty() || invocations.is_empty() {
        return Ok(batches);
    }
    // Phase D D2: pre-embed all unique `semantic_match` query literals
    // once per call. Resolvers below lower each `semantic_match(prop,
    // 'text')` into a `SimilarTo { left: prop_col, right: Const(Vector) }`.
    let semantic_match_embeddings =
        pre_embed_semantic_match_queries(invocations, xervo_runtime).await?;
    // Phase D D1 graph-structural: pre-compute topology scores and
    // neighbor-property maps for every distinct (fn_name, args) tuple
    // appearing in any FEATURE FunctionCall. One pass per call; reused
    // across every row of every batch.
    let graph_feature_maps = precompute_graph_feature_maps(invocations, graph_algo).await?;
    let neighbor_feature_maps =
        precompute_neighbor_feature_maps(invocations, &batches, graph_algo).await?;
    let mut out_batches = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut current = batch;
        for invocation in invocations {
            let classifier = registry.get(&invocation.model_name).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "neural classifier '{}' not registered; \
                         add it to LocyConfig::classifier_registry",
                    invocation.model_name
                ))
            })?;

            // Resolve each feature_expr to a per-row evaluator.
            // Supported shapes (validated at compile time by
            // `extract_model_invocations` / `validate_features`):
            //   * `Expr::Variable("name")` — direct column reference.
            //   * `Expr::Property(Variable(v), prop)` — looked up by the
            //     conventional `"v.prop"` column name materialized by
            //     the planner's `translate_property_access` pipeline.
            //   * `Expr::FunctionCall { name: "similar_to"|"semantic_match", ... }`
            //     — Phase D D1/D2 retrieval-backed feature; both args
            //     resolved to columns; UDF evaluated per row against
            //     the row's `uni_common::Value` payloads.
            let resolvers = build_feature_resolvers(
                &current,
                invocation,
                path_context_handles,
                &semantic_match_embeddings,
                &graph_feature_maps,
                &neighbor_feature_maps,
            )?;

            // Build one ClassifyInput per row.
            let n_rows = current.num_rows();
            let mut inputs: Vec<ClassifyInput> = Vec::with_capacity(n_rows);
            let mut input_hashes: Vec<u64> = Vec::with_capacity(n_rows);
            for row_idx in 0..n_rows {
                let mut features = std::collections::HashMap::new();
                for resolver in &resolvers {
                    let value = resolver.eval_row(&current, row_idx)?;
                    features.insert(resolver.binding_name.clone(), value);
                }
                let input = ClassifyInput { features };
                input_hashes.push(input.stable_hash());
                inputs.push(input);
            }

            // Slice 2: memoization. Split inputs into cache hits and
            // misses; only call `classify` on the misses, then weave
            // the cached values back in by original row index.
            let mut probs: Vec<f64> = vec![0.0; n_rows];
            let mut miss_inputs: Vec<ClassifyInput> = Vec::new();
            let mut miss_row_indices: Vec<usize> = Vec::new();
            if let Some(c) = cache {
                for (row_idx, h) in input_hashes.iter().enumerate() {
                    match c.get(&invocation.model_name, *h) {
                        Some(v) => probs[row_idx] = v,
                        None => {
                            miss_row_indices.push(row_idx);
                            miss_inputs.push(inputs[row_idx].clone());
                        }
                    }
                }
            } else {
                miss_row_indices = (0..n_rows).collect();
                miss_inputs = inputs.clone();
            }

            // Phase C C-RawCalibratedSeparation: when a calibrator is
            // present, route through `raw_and_calibrated` so the
            // provenance store records both the base classifier's raw
            // output AND the post-calibrator value. Bare classifiers
            // (no calibrator) keep using `classify`. The downstream
            // `probs[row]` is always the *calibrated* value when
            // available — that's what the rule's PROB output column
            // and the memoization cache carry.
            let calibrator = classifier.get_calibrator();
            let (miss_raws, miss_calibrated) = if miss_inputs.is_empty() {
                (Vec::new(), Vec::new())
            } else if calibrator.is_some() {
                let pairs = classifier
                    .raw_and_calibrated(&miss_inputs)
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                if pairs.len() != miss_inputs.len() {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "classifier '{}' raw_and_calibrated returned {} outputs for {} inputs",
                        invocation.model_name,
                        pairs.len(),
                        miss_inputs.len()
                    )));
                }
                let raws: Vec<f64> = pairs.iter().map(|(r, _)| *r).collect();
                let cals: Vec<f64> = pairs.iter().map(|(r, c)| c.unwrap_or(*r)).collect();
                (raws, cals)
            } else {
                let r = classifier
                    .classify(&miss_inputs)
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                if r.len() != miss_inputs.len() {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "classifier '{}' returned {} outputs for {} inputs",
                        invocation.model_name,
                        r.len(),
                        miss_inputs.len()
                    )));
                }
                // No calibrator → raw == final.
                (r.clone(), r)
            };
            // The memoization cache stores the *final* (calibrated when
            // available) value, matching what downstream rules consume.
            // Track per-miss raws alongside so the provenance store
            // sees both. For cache hits, we don't have raws — the
            // provenance record for that row gets None for `raw` and
            // the cached value as `calibrated` (the only thing we
            // remembered). Future slice can extend the cache to carry
            // both; current behavior matches pre-fix correctness for
            // EXPLAIN of cache hits.
            let mut row_raw: Vec<Option<f64>> = vec![None; n_rows];
            for (i, &row_idx) in miss_row_indices.iter().enumerate() {
                probs[row_idx] = miss_calibrated[i];
                row_raw[row_idx] = Some(miss_raws[i]);
                if let Some(c) = cache {
                    c.insert(
                        &invocation.model_name,
                        input_hashes[row_idx],
                        miss_calibrated[i],
                    );
                }
            }

            // Phase C B1-B3 follow-up: when a provenance store is
            // configured, record (raw, calibrated, confidence_band)
            // per row. With C-RawCalibratedSeparation (above),
            // `row_raw[i]` carries the *pre-calibrator* value when we
            // computed it on this call; `probs[i]` is the
            // post-calibrator value. `confidence_band` comes from the
            // active Calibrator's `confidence_band(p)`.
            if let Some(store) = provenance_store {
                for row_idx in 0..n_rows {
                    let calibrated_value = probs[row_idx];
                    let (raw_value, calibrated) = match (row_raw[row_idx], &calibrator) {
                        (Some(raw), Some(_)) => (raw, Some(calibrated_value)),
                        (Some(raw), None) => (raw, None),
                        // Cache hit: we only have the calibrated value.
                        // Report it as raw with `calibrated == raw` to
                        // preserve telemetry shape; document this in
                        // the field doc.
                        (None, _) => (
                            calibrated_value,
                            calibrator.as_ref().map(|_| calibrated_value),
                        ),
                    };
                    let band = calibrator
                        .as_ref()
                        .and_then(|c| c.confidence_band(calibrated_value));
                    store.record(
                        &invocation.model_name,
                        input_hashes[row_idx],
                        uni_locy::NeuralProvenanceRecord {
                            raw_probability: raw_value,
                            calibrated_probability: calibrated,
                            confidence_band: band,
                            // Phase 12 EXPLAIN follow-up: stash the
                            // per-binding `FeatureValue` map that fed
                            // the classifier so Mode B re-evaluation
                            // can surface graph-structural feature
                            // values without re-precomputing topology
                            // or neighbor maps (the hot-path data is
                            // authoritative).
                            feature_inputs: inputs[row_idx].features.clone(),
                        },
                    );
                }
            }

            // Overwrite the placeholder column put there by the
            // compile-time YIELD rewrite. If the column doesn't yet
            // exist (defensive — shouldn't happen for well-formed
            // plans), fall back to appending.
            let out_col: Arc<dyn arrow_array::Array> =
                Arc::new(arrow_array::Float64Array::from(probs));
            let schema = current.schema();
            let target_idx = schema.index_of(&invocation.output_column).ok();
            let mut columns: Vec<Arc<dyn arrow_array::Array>> = current.columns().to_vec();
            let mut fields: Vec<Arc<arrow_schema::Field>> =
                schema.fields().iter().cloned().collect();
            match target_idx {
                Some(idx) => {
                    columns[idx] = out_col;
                    // Force the field's data type to Float64 in case
                    // the placeholder was inferred at a wider type.
                    fields[idx] = Arc::new(arrow_schema::Field::new(
                        &invocation.output_column,
                        arrow_schema::DataType::Float64,
                        true,
                    ));
                }
                None => {
                    columns.push(out_col);
                    fields.push(Arc::new(arrow_schema::Field::new(
                        &invocation.output_column,
                        arrow_schema::DataType::Float64,
                        true,
                    )));
                }
            }
            let new_schema = Arc::new(arrow_schema::Schema::new(fields));
            current = RecordBatch::try_new(new_schema, columns).map_err(arrow_err)?;
        }
        out_batches.push(current);
    }
    Ok(out_batches)
}

/// Extract a [`uni_locy::FeatureValue`] from a column at a given row.
/// Conservative cast set matching the property-graph value types Locy
/// currently exposes; unsupported types fall back to `Null`.
/// Per-feature evaluator built once from a clause's `feature_exprs`
/// and reused for every row in the batch. Supports plain column
/// reads (`Direct`) and the Phase D D1/D2 retrieval-backed UDFs
/// (`SimilarTo`, `SemanticMatch`).
struct FeatureResolver {
    binding_name: String,
    kind: FeatureResolverKind,
}

enum FeatureResolverKind {
    Direct(usize),
    SimilarTo {
        left: FeatureValueSrc,
        right: FeatureValueSrc,
    },
    /// Phase D D3 runtime: pull `column` from the source rule's
    /// derived facts via a pre-built `vid → FeatureValue` lookup. The
    /// `subject_col` is the index of `<subject_var>._vid` in the body
    /// batch; the lookup runs once per row.
    PathContext {
        subject_col: usize,
        vid_to_value: Arc<HashMap<u64, uni_locy::FeatureValue>>,
    },
    /// Phase D D1 graph-structural: look up the subject's pre-computed
    /// topology score (degree/pagerank/closeness). `subject_col` indexes
    /// the row's `<var>._vid`; `vid_to_score` is the whole-graph
    /// procedure output built once per `apply_model_invocations` call.
    GraphAlgoScore {
        subject_col: usize,
        vid_to_score: Arc<HashMap<u64, f64>>,
    },
    /// Phase D D1 graph-structural: aggregate a numeric property over
    /// each subject's one-hop outgoing neighborhood along a named edge
    /// type. `vid_to_values` maps subject vid → the list of numeric
    /// neighbor property values collected at precompute time; the
    /// per-row resolver applies the configured `op` (avg/max/sum).
    NeighborAggregate {
        subject_col: usize,
        op: NeighborAgg,
        vid_to_values: Arc<HashMap<u64, Vec<f64>>>,
    },
}

#[derive(Debug, Clone, Copy)]
enum NeighborAgg {
    Avg,
    Max,
    Sum,
}

/// Direction for one-hop neighborhood traversal. Mirrors
/// `uni_store::storage::direction::Direction` but is independent so
/// the typecheck / planner layer doesn't depend on uni-store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum NeighborDirection {
    Outgoing,
    Incoming,
    Both,
}

impl NeighborDirection {
    fn store_directions(self) -> &'static [uni_store::storage::direction::Direction] {
        use uni_store::storage::direction::Direction;
        match self {
            NeighborDirection::Outgoing => &[Direction::Outgoing],
            NeighborDirection::Incoming => &[Direction::Incoming],
            NeighborDirection::Both => &[Direction::Outgoing, Direction::Incoming],
        }
    }
}

impl NeighborAgg {
    fn from_fn_name(name: &str) -> Option<Self> {
        match name {
            "avg_neighbor" => Some(NeighborAgg::Avg),
            "max_neighbor" => Some(NeighborAgg::Max),
            "sum_neighbor" => Some(NeighborAgg::Sum),
            _ => None,
        }
    }

    fn apply(self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        match self {
            NeighborAgg::Avg => Some(values.iter().sum::<f64>() / values.len() as f64),
            NeighborAgg::Max => values.iter().copied().reduce(f64::max),
            NeighborAgg::Sum => Some(values.iter().sum()),
        }
    }
}

/// One side of a `similar_to` feature: either a column index in the
/// per-row batch or a constant value lifted from a literal expression.
enum FeatureValueSrc {
    Col(usize),
    Const(uni_common::Value),
}

impl FeatureValueSrc {
    fn resolve(&self, batch: &RecordBatch, row_idx: usize) -> uni_common::Value {
        match self {
            FeatureValueSrc::Col(idx) => extract_common_value(batch.column(*idx).as_ref(), row_idx),
            FeatureValueSrc::Const(v) => v.clone(),
        }
    }
}

impl FeatureResolver {
    fn eval_row(&self, batch: &RecordBatch, row_idx: usize) -> DFResult<uni_locy::FeatureValue> {
        match &self.kind {
            FeatureResolverKind::Direct(idx) => {
                Ok(extract_feature_value(batch.column(*idx).as_ref(), row_idx))
            }
            FeatureResolverKind::SimilarTo { left, right } => {
                let lv = left.resolve(batch, row_idx);
                let rv = right.resolve(batch, row_idx);
                match crate::query::similar_to::eval_similar_to_pure(&lv, &rv) {
                    Ok(uni_common::Value::Float(f)) => Ok(uni_locy::FeatureValue::Float(f)),
                    Ok(_) => Ok(uni_locy::FeatureValue::Null),
                    Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                        "similar_to UDF failed: {e}"
                    ))),
                }
            }
            FeatureResolverKind::PathContext {
                subject_col,
                vid_to_value,
            } => {
                let col = batch.column(*subject_col);
                if col.is_null(row_idx) {
                    return Ok(uni_locy::FeatureValue::Null);
                }
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
                    let vid = arr.value(row_idx);
                    Ok(vid_to_value
                        .get(&vid)
                        .cloned()
                        .unwrap_or(uni_locy::FeatureValue::Null))
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                    let vid = arr.value(row_idx) as u64;
                    Ok(vid_to_value
                        .get(&vid)
                        .cloned()
                        .unwrap_or(uni_locy::FeatureValue::Null))
                } else {
                    Ok(uni_locy::FeatureValue::Null)
                }
            }
            FeatureResolverKind::GraphAlgoScore {
                subject_col,
                vid_to_score,
            } => {
                let col = batch.column(*subject_col);
                if col.is_null(row_idx) {
                    return Ok(uni_locy::FeatureValue::Null);
                }
                let vid_opt: Option<u64> = if let Some(arr) =
                    col.as_any().downcast_ref::<arrow_array::UInt64Array>()
                {
                    Some(arr.value(row_idx))
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                    Some(arr.value(row_idx) as u64)
                } else {
                    // Fallback: subject column carries a Node-encoded
                    // `uni_common::Value` (LargeBinary via codec). Decode
                    // and pull the VID. This is the common case for
                    // bare-variable subjects where no `_vid` hidden
                    // column was materialized.
                    match extract_common_value(col.as_ref(), row_idx) {
                        uni_common::Value::Node(n) => Some(n.vid.as_u64()),
                        uni_common::Value::Int(i) => Some(i as u64),
                        _ => None,
                    }
                };
                Ok(vid_opt
                    .and_then(|v| vid_to_score.get(&v).copied())
                    .map(uni_locy::FeatureValue::Float)
                    .unwrap_or(uni_locy::FeatureValue::Null))
            }
            FeatureResolverKind::NeighborAggregate {
                subject_col,
                op,
                vid_to_values,
            } => {
                let vid_opt = extract_vid_from_column(batch.column(*subject_col).as_ref(), row_idx);
                Ok(vid_opt
                    .and_then(|v| vid_to_values.get(&v))
                    .and_then(|values| op.apply(values))
                    .map(uni_locy::FeatureValue::Float)
                    .unwrap_or(uni_locy::FeatureValue::Null))
            }
        }
    }
}

/// Extract a node VID from a per-row batch column. Handles the three
/// common shapes: `_vid` UInt64 columns, Int64 columns, and Node-encoded
/// LargeBinary columns (the standard `uni_common::Value::Node` codec
/// representation for a bare variable column).
fn extract_vid_from_column(col: &dyn arrow_array::Array, row_idx: usize) -> Option<u64> {
    if col.is_null(row_idx) {
        return None;
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        return Some(arr.value(row_idx));
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return Some(arr.value(row_idx) as u64);
    }
    match extract_common_value(col, row_idx) {
        uni_common::Value::Node(n) => Some(n.vid.as_u64()),
        uni_common::Value::Int(i) => Some(i as u64),
        _ => None,
    }
}

fn build_feature_resolvers(
    batch: &RecordBatch,
    invocation: &uni_locy::ModelInvocation,
    path_context_handles: &HashMap<
        String,
        crate::query::df_graph::locy_model_invoke::PathContextHandle,
    >,
    semantic_match_embeddings: &HashMap<String, Vec<f32>>,
    graph_feature_maps: &HashMap<String, Arc<HashMap<u64, f64>>>,
    neighbor_feature_maps: &NeighborFeatureMaps,
) -> DFResult<Vec<FeatureResolver>> {
    use uni_cypher::ast::Expr;
    let schema = batch.schema();
    let lookup_col = |name_or_property: String| -> DFResult<usize> {
        schema.index_of(&name_or_property).map_err(|_| {
            datafusion::error::DataFusionError::Execution(format!(
                "feature column '{name_or_property}' not found in clause body output schema"
            ))
        })
    };
    // Resolve a feature sub-expression to a per-row value source. Variables
    // and property accesses map to batch columns; list/scalar literals
    // become inline constants — required so `similar_to(s.embedding, [1,0,0])`
    // works without a hidden column for the literal vector.
    let resolve_src = |expr: &Expr| -> DFResult<FeatureValueSrc> {
        match expr {
            Expr::Variable(name) => {
                let col = if schema.index_of(name).is_ok() {
                    name.clone()
                } else {
                    let vid_name = format!("{}._vid", name);
                    if schema.index_of(&vid_name).is_ok() {
                        vid_name
                    } else {
                        name.clone()
                    }
                };
                Ok(FeatureValueSrc::Col(lookup_col(col)?))
            }
            Expr::Property(boxed, prop) if matches!(boxed.as_ref(), Expr::Variable(_)) => {
                let Expr::Variable(v) = boxed.as_ref() else {
                    unreachable!()
                };
                let direct = format!("{}.{}", v, prop);
                let col = if schema.index_of(&direct).is_ok() {
                    direct
                } else {
                    format!("__feat_{}_{}", v, prop)
                };
                Ok(FeatureValueSrc::Col(lookup_col(col)?))
            }
            Expr::Literal(lit) => Ok(FeatureValueSrc::Const(lit.to_value())),
            Expr::List(items) => {
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    out.push(match it {
                        Expr::Literal(lit) => lit.to_value(),
                        _ => uni_common::Value::Null,
                    });
                }
                Ok(FeatureValueSrc::Const(uni_common::Value::List(out)))
            }
            other => Err(datafusion::error::DataFusionError::Execution(format!(
                "unsupported feature sub-expression: {other:?}"
            ))),
        }
    };

    // Phase D D3 runtime: when the model declares a path-context
    // feature, build a `vid → FeatureValue` lookup once from the
    // source rule's converged facts and wrap it in an Arc so the
    // per-row resolver does a single hash lookup. The model's
    // `INPUT` bindings are unused under this form for MVP — the
    // resolver's binding name is the column name (matches how the
    // mock-classifier feature-driver pattern in TCK consumes it).
    if let Some(pc) = &invocation.path_context {
        let handle = path_context_handles.get(&pc.source_rule).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "model '{}' path_context references rule '{}' but no DerivedScanHandle \
                 was registered; this should never happen — the build_clause path \
                 mints a handle for every distinct source_rule in the invocation set",
                invocation.model_name, pc.source_rule
            ))
        })?;
        let subject_col = schema
            .index_of(&format!("{}._vid", pc.subject_var))
            .or_else(|_| schema.index_of(&pc.subject_var))
            .map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "model '{}' path_context: subject column '{}' (or '{0}._vid') not \
                     in body batch schema",
                    invocation.model_name, pc.subject_var
                ))
            })?;
        let vid_to_value =
            build_path_context_lookup(handle, &pc.subject_var, &pc.column, &invocation.model_name)?;
        return Ok(vec![FeatureResolver {
            binding_name: pc.column.clone(),
            kind: FeatureResolverKind::PathContext {
                subject_col,
                vid_to_value: Arc::new(vid_to_value),
            },
        }]);
    }

    let mut out = Vec::with_capacity(invocation.feature_exprs.len());
    for (i, fexpr) in invocation.feature_exprs.iter().enumerate() {
        let binding_name = invocation.feature_names[i].clone();
        let kind = match fexpr {
            Expr::FunctionCall { name, args, .. } if name == "similar_to" => {
                if args.len() != 2 {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "similar_to expects 2 args, got {}",
                        args.len()
                    )));
                }
                FeatureResolverKind::SimilarTo {
                    left: resolve_src(&args[0])?,
                    right: resolve_src(&args[1])?,
                }
            }
            Expr::FunctionCall { name, args, .. } if name == "semantic_match" => {
                // Phase D D2: lower `semantic_match(prop, 'text')` to a
                // `SimilarTo` resolver with the pre-embedded query
                // vector as the right side. The literal text was embedded
                // once via the Xervo runtime in `pre_embed_semantic_match_queries`.
                if args.len() != 2 {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "semantic_match expects 2 args, got {}",
                        args.len()
                    )));
                }
                let text = match &args[1] {
                    Expr::Literal(uni_cypher::ast::CypherLiteral::String(s)) => s.clone(),
                    other => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "semantic_match: 2nd arg must be a string literal, got {other:?}"
                        )));
                    }
                };
                let embedded = semantic_match_embeddings.get(&text).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "semantic_match: query text '{text}' was not pre-embedded. \
                         This is a bug — `apply_model_invocations` should have \
                         embedded all unique semantic_match texts up front. Most \
                         likely the Xervo runtime is not configured (configure \
                         via `LocyConfig::xervo_runtime` or its equivalent)."
                    ))
                })?;
                let right_vec: Vec<f32> = embedded.clone();
                FeatureResolverKind::SimilarTo {
                    left: resolve_src(&args[0])?,
                    right: FeatureValueSrc::Const(uni_common::Value::Vector(right_vec)),
                }
            }
            Expr::FunctionCall { name, args, .. }
                if matches!(
                    name.as_str(),
                    "degree_centrality"
                        | "pagerank_score"
                        | "closeness_centrality"
                        | "betweenness_centrality"
                        | "eigenvector_centrality"
                        | "harmonic_centrality"
                        | "katz_centrality"
                ) =>
            {
                if args.len() != 1 {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "{name} expects 1 arg, got {}",
                        args.len()
                    )));
                }
                let Expr::Variable(v) = &args[0] else {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "{name}(...) argument must be a node variable, got {:?}",
                        args[0]
                    )));
                };
                let subject_col = {
                    let direct = schema.index_of(v).ok();
                    let vid_name = format!("{}._vid", v);
                    let vid_col = schema.index_of(&vid_name).ok();
                    vid_col.or(direct).ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "{name}: subject column '{v}' (or '{v}._vid') not in body batch schema"
                        ))
                    })?
                };
                let vid_to_score = graph_feature_maps.get(name).cloned().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "{name}: pre-computed score map missing. This is a bug — \
                         `apply_model_invocations` should have called \
                         `precompute_graph_feature_maps` for every graph-structural \
                         feature before building resolvers. Most likely the graph \
                         algorithm registry is not configured."
                    ))
                })?;
                FeatureResolverKind::GraphAlgoScore {
                    subject_col,
                    vid_to_score,
                }
            }
            Expr::FunctionCall { name, args, .. }
                if matches!(
                    name.as_str(),
                    "avg_neighbor" | "max_neighbor" | "sum_neighbor"
                ) =>
            {
                if args.len() != 3 && args.len() != 4 {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "{name} expects 3 or 4 args, got {}",
                        args.len()
                    )));
                }
                let Expr::Variable(v) = &args[0] else {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "{name}(...) first argument must be a node variable, got {:?}",
                        args[0]
                    )));
                };
                let rel_type = match &args[1] {
                    Expr::Literal(uni_cypher::ast::CypherLiteral::String(s)) => s.clone(),
                    other => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "{name}: 2nd arg must be a string literal (rel-type), got {other:?}"
                        )));
                    }
                };
                let prop_name = match &args[2] {
                    Expr::Literal(uni_cypher::ast::CypherLiteral::String(s)) => s.clone(),
                    other => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "{name}: 3rd arg must be a string literal (property), got {other:?}"
                        )));
                    }
                };
                let direction_arg = match args.get(3) {
                    None => NeighborDirection::Outgoing,
                    Some(Expr::Literal(uni_cypher::ast::CypherLiteral::String(d))) => {
                        match d.to_uppercase().as_str() {
                            "OUTGOING" => NeighborDirection::Outgoing,
                            "INCOMING" => NeighborDirection::Incoming,
                            "BOTH" => NeighborDirection::Both,
                            other => {
                                return Err(datafusion::error::DataFusionError::Execution(
                                    format!(
                                        "{name}: direction must be OUTGOING|INCOMING|BOTH, got '{other}'"
                                    ),
                                ));
                            }
                        }
                    }
                    Some(other) => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "{name}: 4th arg must be a string literal (direction), got {other:?}"
                        )));
                    }
                };
                let subject_col = {
                    let direct = schema.index_of(v).ok();
                    let vid_name = format!("{}._vid", v);
                    let vid_col = schema.index_of(&vid_name).ok();
                    vid_col.or(direct).ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "{name}: subject column '{v}' (or '{v}._vid') not in body batch schema"
                        ))
                    })?
                };
                let vid_to_values = neighbor_feature_maps
                    .get(&(rel_type.clone(), prop_name.clone(), direction_arg))
                    .cloned()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "{name}: pre-computed neighbor map missing for ({rel_type}, {prop_name}, {direction_arg:?}). \
                             This is a bug — `apply_model_invocations` should have called \
                             `precompute_neighbor_feature_maps` for every neighbor-aggregator \
                             feature before building resolvers."
                        ))
                    })?;
                let op = NeighborAgg::from_fn_name(name).unwrap();
                FeatureResolverKind::NeighborAggregate {
                    subject_col,
                    op,
                    vid_to_values,
                }
            }
            other => match resolve_src(other)? {
                FeatureValueSrc::Col(idx) => FeatureResolverKind::Direct(idx),
                FeatureValueSrc::Const(_) => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "model '{}' feature must reference a variable or property — got a literal",
                        invocation.model_name
                    )));
                }
            },
        };
        out.push(FeatureResolver { binding_name, kind });
    }
    Ok(out)
}

/// Phase D D2: scan invocations' feature expressions for
/// `semantic_match(prop, 'text')` calls and embed each distinct
/// query string once via the Xervo runtime. Returns a
/// `text → Vec<f32>` map consumed at resolver-build time. Errors
/// cleanly when `semantic_match` is used without a configured
/// Xervo runtime.
async fn pre_embed_semantic_match_queries(
    invocations: &[uni_locy::ModelInvocation],
    xervo_runtime: &crate::query::df_graph::locy_model_invoke::XervoRuntimeHandle,
) -> DFResult<HashMap<String, Vec<f32>>> {
    use uni_cypher::ast::{CypherLiteral, Expr};
    // Collect (text, embedder_alias) pairs. The alias is per-model
    // (Phase D D2 follow-up): each invocation's `embedder_alias` overrides
    // the runtime-wide `"default"`. Two invocations sharing the same
    // (text, alias) reuse one embed call; same text under different
    // aliases is embedded twice. The cache key remains plain `text` —
    // a model's resolver fetches embeddings via the text it knows, and
    // mixed-alias re-embed of identical text under a different alias is
    // a rare-enough edge case that the "last writer wins" cache shape
    // is acceptable (documented).
    let mut needed: Vec<(String, String)> = Vec::new();
    for inv in invocations {
        let alias = inv
            .embedder_alias
            .clone()
            .unwrap_or_else(|| "default".to_string());
        for fexpr in &inv.feature_exprs {
            if let Expr::FunctionCall { name, args, .. } = fexpr
                && name == "semantic_match"
                && args.len() == 2
                && let Expr::Literal(CypherLiteral::String(s)) = &args[1]
            {
                let tuple = (s.clone(), alias.clone());
                if !needed.contains(&tuple) {
                    needed.push(tuple);
                }
            }
        }
    }
    if needed.is_empty() {
        return Ok(HashMap::new());
    }
    let runtime = xervo_runtime.as_ref().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "semantic_match: Uni-Xervo runtime not configured. Either provide \
             one via `LocyConfig::xervo_runtime` (or its equivalent setup \
             path) or pre-compute the query embedding and pass it via \
             `similar_to(prop, <literal_vector>)`."
                .to_string(),
        )
    })?;
    // Group needed (text, alias) by alias so each embedder is consulted
    // exactly once.
    let mut by_alias: HashMap<String, Vec<String>> = HashMap::new();
    for (text, alias) in &needed {
        by_alias
            .entry(alias.clone())
            .or_default()
            .push(text.clone());
    }
    let mut out: HashMap<String, Vec<f32>> = HashMap::new();
    for (alias, texts) in by_alias {
        let embedder = runtime.embedding(&alias).await.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "semantic_match: failed to obtain embedder for alias '{alias}': {e}. \
                 Register an embedder under that alias in your Uni-Xervo runtime, or \
                 pre-compute the query embedding and pass via similar_to."
            ))
        })?;
        let text_refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        let embeddings = embedder
            .embed(&text_refs)
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "semantic_match: embedder '{alias}' call failed: {e}"
                ))
            })?
            .vectors;
        if embeddings.len() != texts.len() {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "semantic_match: embedder '{alias}' returned {} vectors for {} queries",
                embeddings.len(),
                texts.len()
            )));
        }
        for (text, vec) in texts.into_iter().zip(embeddings) {
            out.insert(text, vec);
        }
    }
    Ok(out)
}

/// Phase D D1 graph-structural: scan invocations' feature expressions
/// for `degree_centrality(n)` / `pagerank_score(n)` / `closeness_centrality(n)`
/// calls and invoke the corresponding `uni.algo.*` procedure on the
/// configured `AlgorithmRegistry` once per distinct call. Returns a
/// `fn_name → Arc<HashMap<vid, score>>` map consumed at resolver-build
/// time. Errors cleanly when a graph-structural FEATURE is used
/// without a configured registry or storage handle.
///
/// Pre-computation is `O(graph)` per call. Across fixpoint iterations
/// the graph state can change, so the cache lives for the lifetime of
/// one `apply_model_invocations` call only — same lifetime as the
/// D2 query-embedding cache (`pre_embed_semantic_match_queries`).
async fn precompute_graph_feature_maps(
    invocations: &[uni_locy::ModelInvocation],
    graph_algo: &crate::query::df_graph::locy_model_invoke::GraphAlgoHandle,
) -> DFResult<HashMap<String, Arc<HashMap<u64, f64>>>> {
    use futures::StreamExt;
    use uni_algo::algo::procedures::AlgoContext;
    use uni_cypher::ast::Expr;

    // Map our user-facing FEATURE function names to the canonical
    // `uni.algo.*` procedure names registered in `AlgorithmRegistry`.
    fn procedure_for(fn_name: &str) -> Option<&'static str> {
        match fn_name {
            "degree_centrality" => Some("uni.algo.degreeCentrality"),
            "pagerank_score" => Some("uni.algo.pageRank"),
            "closeness_centrality" => Some("uni.algo.closeness"),
            "betweenness_centrality" => Some("uni.algo.betweenness"),
            "eigenvector_centrality" => Some("uni.algo.eigenvectorCentrality"),
            "harmonic_centrality" => Some("uni.algo.harmonicCentrality"),
            "katz_centrality" => Some("uni.algo.katzCentrality"),
            _ => None,
        }
    }

    // Collect the set of distinct topology-FEATURE names referenced
    // across all invocations. Args are always a single Variable, so
    // the precomputation key is just the function name.
    let mut needed: Vec<String> = Vec::new();
    for inv in invocations {
        for fexpr in &inv.feature_exprs {
            if let Expr::FunctionCall { name, .. } = fexpr
                && procedure_for(name).is_some()
                && !needed.contains(name)
            {
                needed.push(name.clone());
            }
        }
    }
    if needed.is_empty() {
        return Ok(HashMap::new());
    }

    let registry = graph_algo.registry.as_ref().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "graph-structural FEATURE invoked but no `AlgorithmRegistry` is \
             configured. Configure one on `GraphExecutionContext::with_algo_registry`."
                .to_string(),
        )
    })?;
    let storage = graph_algo.storage.as_ref().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "graph-structural FEATURE invoked but no storage handle was \
             threaded into the FEATURE runtime. This is a bug in df_planner."
                .to_string(),
        )
    })?;

    let mut out: HashMap<String, Arc<HashMap<u64, f64>>> = HashMap::new();
    for fn_name in needed {
        let proc_name = procedure_for(&fn_name).unwrap();
        let procedure = registry.get(proc_name).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "graph-structural FEATURE '{fn_name}' resolves to procedure \
                 '{proc_name}' which is not in the algorithm registry"
            ))
        })?;
        // Topology procedures take (nodeLabels[], relationshipTypes[],
        // [direction], [...]) — pass empty arrays for nodeLabels and
        // relationshipTypes to mean "all". The procedure fills the
        // remaining optional args from its signature defaults.
        let args: Vec<serde_json::Value> = vec![
            serde_json::Value::Array(Vec::new()),
            serde_json::Value::Array(Vec::new()),
        ];
        let algo_ctx = AlgoContext::new(
            storage.clone(),
            graph_algo.l0_manager.as_ref().map(Arc::clone),
        );
        // The AlgoProcedure trait routes direct (nodeLabels, edgeTypes)
        // args through the V2 projection entry point: build a projection
        // from the direct args, then execute against it.
        //
        // Fill optional algorithm-specific args (e.g. degree_centrality's
        // `direction`, eigenvector/katz `weightProperty`) with their schema
        // defaults for the projection build: `build_projection_from_direct_args`
        // feeds the specific args (`args[2..]`) to the adapter's
        // `customize_projection`, which indexes them positionally — the two
        // empty placeholder arrays alone would leave that slice empty and
        // panic. `validate_args` fills missing optionals WITHOUT type-checking
        // the defaults (some are `Null`-typed sentinels), so this never errors
        // for the placeholder shape.
        //
        // We pass the ORIGINAL `args` (not the filled ones) to
        // `execute_with_projection`, which re-runs `validate_args` internally:
        // re-feeding already-filled args would make those defaults look
        // "provided" and trip the type-check (`weightProperty: Null` vs
        // `String`). This mirrors the (now-removed) legacy
        // `AlgoProcedure::execute`, which validated once then built + ran.
        let filled_args = procedure
            .signature()
            .validate_args(args.clone())
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "graph-structural FEATURE '{fn_name}': argument validation failed: {e}"
                ))
            })?;
        let projection = uni_algo::algo::procedure_template::build_projection_from_direct_args(
            procedure.as_ref(),
            &algo_ctx,
            &filled_args,
        )
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "graph-structural FEATURE '{fn_name}': projection build failed: {e}"
            ))
        })?;
        let mut stream = procedure.execute_with_projection(algo_ctx, args, projection);
        let mut score_map: HashMap<u64, f64> = HashMap::new();
        let sig = procedure.signature();
        let node_idx = sig
            .yields
            .iter()
            .position(|(n, _)| *n == "nodeId")
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "procedure '{proc_name}' yield schema missing 'nodeId'"
                ))
            })?;
        // Most `uni.algo.*` centrality procedures yield `score`; the
        // `harmonicCentrality` family yields `centrality` instead. Accept
        // either to keep this dispatch independent of procedure-internal
        // naming choices.
        let score_idx = sig
            .yields
            .iter()
            .position(|(n, _)| *n == "score" || *n == "centrality")
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "procedure '{proc_name}' yield schema missing a numeric score column \
                     (expected 'score' or 'centrality')"
                ))
            })?;
        while let Some(row_res) = stream.next().await {
            let row = row_res.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "graph-structural FEATURE '{fn_name}': procedure '{proc_name}' failed: {e}"
                ))
            })?;
            let vid_v = row.values.get(node_idx);
            let score_v = row.values.get(score_idx);
            let (Some(vid_v), Some(score_v)) = (vid_v, score_v) else {
                continue;
            };
            let vid = vid_v.as_u64().or_else(|| vid_v.as_i64().map(|i| i as u64));
            let score = score_v
                .as_f64()
                .or_else(|| score_v.as_i64().map(|i| i as f64));
            if let (Some(vid), Some(score)) = (vid, score) {
                score_map.insert(vid, score);
            }
        }
        out.insert(fn_name, Arc::new(score_map));
    }
    Ok(out)
}

/// Phase D D1 graph-structural: one-hop neighborhood aggregator
/// precompute. Scans invocations' feature expressions for
/// `avg_neighbor` / `max_neighbor` / `sum_neighbor` FunctionCalls,
/// collects the distinct `(rel_type, prop_name)` pairs they need,
/// resolves each rel-type to a schema edge-type id, warms the
/// outgoing-adjacency CSR, and for every subject vid present in the
/// body batches walks the one-hop neighborhood and fetches the
/// requested property from each neighbor via `PropertyManager`.
/// Non-numeric neighbor property values are filtered out via
/// `Value::as_f64`.
///
/// Returns `Arc<HashMap<u64, Vec<f64>>>` keyed by `(rel_type, prop_name)`.
/// The resolver's runtime cost per row is then a single hash lookup
/// plus an `avg`/`max`/`sum` over the cached `Vec<f64>`.
///
/// Scope: **subject-set-only** — we only collect for vids that appear
/// in the body batches' subject columns (avoids pre-walking the entire
/// graph). Subjects with no outgoing edges of the named type land in
/// the map with an empty `Vec` so the resolver's `Null` semantics
/// remain crisp (empty → `Null` → classifier interprets per its
/// feature contract).
/// Per-`(rel_type, prop_name, direction)` cache of neighbor property
/// values keyed by subject vid, produced by
/// `precompute_neighbor_feature_maps` and consumed by
/// `FeatureResolverKind::NeighborAggregate` resolvers.
type NeighborFeatureMaps =
    HashMap<(String, String, NeighborDirection), Arc<HashMap<u64, Vec<f64>>>>;

async fn precompute_neighbor_feature_maps(
    invocations: &[uni_locy::ModelInvocation],
    batches: &[RecordBatch],
    graph_algo: &crate::query::df_graph::locy_model_invoke::GraphAlgoHandle,
) -> DFResult<NeighborFeatureMaps> {
    use uni_cypher::ast::{CypherLiteral, Expr};

    // Collect distinct (subject_var, rel_type, prop_name, direction)
    // tuples needed across all invocations. The subject_var tells us
    // which body batch column to scan for subject vids; the direction
    // is optional in the AST (defaults to OUTGOING).
    let parse_direction = |arg: Option<&Expr>| -> Option<NeighborDirection> {
        match arg {
            None => Some(NeighborDirection::Outgoing),
            Some(Expr::Literal(CypherLiteral::String(d))) => match d.to_uppercase().as_str() {
                "OUTGOING" => Some(NeighborDirection::Outgoing),
                "INCOMING" => Some(NeighborDirection::Incoming),
                "BOTH" => Some(NeighborDirection::Both),
                _ => None,
            },
            _ => None,
        }
    };
    let mut needed: Vec<(String, String, String, NeighborDirection)> = Vec::new();
    for inv in invocations {
        for fexpr in &inv.feature_exprs {
            if let Expr::FunctionCall { name, args, .. } = fexpr
                && NeighborAgg::from_fn_name(name).is_some()
                && (args.len() == 3 || args.len() == 4)
                && let Expr::Variable(v) = &args[0]
                && let Expr::Literal(CypherLiteral::String(rel)) = &args[1]
                && let Expr::Literal(CypherLiteral::String(prop)) = &args[2]
                && let Some(direction) = parse_direction(args.get(3))
            {
                let tuple = (v.clone(), rel.clone(), prop.clone(), direction);
                if !needed.contains(&tuple) {
                    needed.push(tuple);
                }
            }
        }
    }
    if needed.is_empty() {
        return Ok(HashMap::new());
    }

    let storage = graph_algo.storage.as_ref().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "neighbor-aggregator FEATURE invoked but no storage handle was \
             threaded into the FEATURE runtime. This is a bug in df_planner."
                .to_string(),
        )
    })?;
    let property_manager = graph_algo.property_manager.as_ref().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "neighbor-aggregator FEATURE invoked but no PropertyManager was \
             threaded into the FEATURE runtime. This is a bug in df_planner."
                .to_string(),
        )
    })?;
    // Build a QueryContext snapshot so L0-resident vertex properties
    // are visible to `get_vertex_prop_with_ctx`. Without a ctx, L0
    // property data is silently invisible (returns Null), which is
    // why the topology trio's `AlgoContext` consumes L0 via
    // `L0Manager` whereas property reads need this separate path.
    let query_ctx = graph_algo.l0_buffers.as_ref().map(|bufs| {
        uni_store::runtime::context::QueryContext::new_with_pending(
            bufs.current.clone(),
            bufs.transaction.clone(),
            bufs.pending_flush.clone(),
        )
    });

    // Group needed tuples by (rel_type, prop_name, direction) — one
    // precomputed map per key, regardless of which subject_var binding
    // points at it (the subject vids are unioned).
    let mut by_key: HashMap<(String, String, NeighborDirection), Vec<String>> = HashMap::new();
    for (subject_var, rel, prop, direction) in needed {
        by_key
            .entry((rel, prop, direction))
            .or_default()
            .push(subject_var);
    }

    let mut out: NeighborFeatureMaps = HashMap::new();
    for ((rel_type, prop_name, direction), subject_vars) in by_key {
        // Resolve edge_type_id from schema.
        let schema = storage.schema_manager().schema();
        let Some(edge_meta) = schema.edge_types.get(&rel_type) else {
            // Unregistered rel-type → empty map. The resolver surfaces
            // Null at row time, consistent with the no-neighbor case.
            out.insert((rel_type, prop_name, direction), Arc::new(HashMap::new()));
            continue;
        };
        let edge_type_id = edge_meta.id;

        // Warm adjacency for every direction we'll traverse. Mirrors
        // the pattern in projection.rs / procedure_template.rs.
        let edge_ver = storage.get_edge_version_by_id(edge_type_id);
        for dir in direction.store_directions() {
            storage
                .warm_adjacency(edge_type_id, *dir, edge_ver)
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "neighbor-aggregator warm_adjacency for '{rel_type}' / {dir:?} failed: {e}"
                    ))
                })?;
        }

        // Collect distinct subject vids from body batches across every
        // subject_var binding that this (rel, prop) pair uses.
        let mut subject_vids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for subject_var in &subject_vars {
            for batch in batches {
                let schema = batch.schema();
                let col_idx = schema
                    .index_of(&format!("{}._vid", subject_var))
                    .ok()
                    .or_else(|| schema.index_of(subject_var).ok());
                let Some(col_idx) = col_idx else { continue };
                let col = batch.column(col_idx);
                for row in 0..batch.num_rows() {
                    if let Some(v) = extract_vid_from_column(col.as_ref(), row) {
                        subject_vids.insert(v);
                    }
                }
            }
        }

        // For each subject, walk edges in the configured direction(s),
        // fetch neighbor property, coerce to f64, accumulate. Subjects
        // with no numeric neighbors retain an empty Vec (→ Null at
        // row time).
        let mut vid_to_values: HashMap<u64, Vec<f64>> = HashMap::new();
        let adj = storage.adjacency_manager();
        for subject_vid in subject_vids {
            let mut neighbors: Vec<(uni_common::core::id::Vid, uni_common::core::id::Eid)> =
                Vec::new();
            for dir in direction.store_directions() {
                neighbors.extend(adj.get_neighbors(
                    uni_common::core::id::Vid::from(subject_vid),
                    edge_type_id,
                    *dir,
                ));
            }
            let mut values: Vec<f64> = Vec::with_capacity(neighbors.len());
            for (neighbor_vid, _eid) in neighbors {
                let val = property_manager
                    .get_vertex_prop_with_ctx(neighbor_vid, &prop_name, query_ctx.as_ref())
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "neighbor-aggregator: failed to read property \
                             '{prop_name}' on neighbor vid {neighbor_vid:?}: {e}"
                        ))
                    })?;
                if let Some(f) = val.as_f64()
                    && !f.is_nan()
                {
                    values.push(f);
                }
            }
            vid_to_values.insert(subject_vid, values);
        }
        out.insert((rel_type, prop_name, direction), Arc::new(vid_to_values));
    }
    Ok(out)
}

/// Phase D D3: walk the source rule's converged batches and build
/// a `vid → FeatureValue` lookup for the named column. The subject
/// column in the derived rule's schema holds VIDs (UInt64) for node
/// variables; the value column type follows the rule's yield-schema
/// inference (typically Float64 / Int64 / Bool / String).
fn build_path_context_lookup(
    handle: &crate::query::df_graph::locy_model_invoke::PathContextHandle,
    _subject_var: &str,
    column: &str,
    model_name: &str,
) -> DFResult<HashMap<u64, uni_locy::FeatureValue>> {
    // The source rule's KEY column is its first yield column by
    // convention (`infer_yield_schema` orders KEYs first). The model's
    // local `subject_var` is just a binding alias — the actual join
    // matches the body row's VID against this canonical column.
    if handle.schema.fields().is_empty() {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "model '{model_name}' path_context: source rule has empty yield schema"
        )));
    }
    let subj_idx = 0_usize;
    let col_idx = handle.schema.index_of(column).map_err(|_| {
        datafusion::error::DataFusionError::Execution(format!(
            "model '{model_name}' path_context: column '{column}' not in \
             source rule's yield schema (have: {:?})",
            handle
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        ))
    })?;
    let batches = handle.data.read();
    let mut out: HashMap<u64, uni_locy::FeatureValue> = HashMap::new();
    for batch in batches.iter() {
        let subj_col = batch.column(subj_idx);
        let value_col = batch.column(col_idx);
        for row in 0..batch.num_rows() {
            if subj_col.is_null(row) {
                continue;
            }
            let vid = if let Some(a) = subj_col.as_any().downcast_ref::<arrow_array::UInt64Array>()
            {
                a.value(row)
            } else if let Some(a) = subj_col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                a.value(row) as u64
            } else {
                continue;
            };
            let v = extract_feature_value(value_col.as_ref(), row);
            // Last write wins on duplicates; derived rules typically have
            // unique KEY values, so this is a defensive guard.
            out.insert(vid, v);
        }
    }
    Ok(out)
}

/// Extract a `uni_common::Value` from one row of an Arrow column.
/// Used by the Phase D `similar_to` feature resolver, which needs
/// the raw `Value` (especially `Value::Vector(Vec<f32>)`) to feed
/// `eval_similar_to_pure`.
fn extract_common_value(col: &dyn arrow_array::Array, row_idx: usize) -> uni_common::Value {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, LargeStringArray, StringArray};
    if col.is_null(row_idx) {
        return uni_common::Value::Null;
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return uni_common::Value::Float(a.value(row_idx));
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return uni_common::Value::Int(a.value(row_idx));
    }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return uni_common::Value::Bool(a.value(row_idx));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return uni_common::Value::String(a.value(row_idx).to_string());
    }
    if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
        return uni_common::Value::String(a.value(row_idx).to_string());
    }
    if let Some(b) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
        let bytes = b.value(row_idx);
        if bytes.is_empty() {
            return uni_common::Value::Null;
        }
        return uni_common::cypher_value_codec::decode(bytes).unwrap_or(uni_common::Value::Null);
    }
    uni_common::Value::Null
}

fn extract_feature_value(col: &dyn arrow_array::Array, row_idx: usize) -> uni_locy::FeatureValue {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, LargeStringArray, StringArray};
    if col.is_null(row_idx) {
        return uni_locy::FeatureValue::Null;
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return uni_locy::FeatureValue::Float(a.value(row_idx));
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return uni_locy::FeatureValue::Int(a.value(row_idx));
    }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return uni_locy::FeatureValue::Bool(a.value(row_idx));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return uni_locy::FeatureValue::String(a.value(row_idx).to_string());
    }
    if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
        return uni_locy::FeatureValue::String(a.value(row_idx).to_string());
    }
    // Schema-less property storage: values arrive as LargeBinary
    // MessagePack-encoded `CypherValue`. Decode via the standard codec
    // and project the result to the matching `FeatureValue` variant.
    if let Some(b) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
        let bytes = b.value(row_idx);
        if bytes.is_empty() {
            return uni_locy::FeatureValue::Null;
        }
        let v = uni_common::cypher_value_codec::decode(bytes).unwrap_or(uni_common::Value::Null);
        return match v {
            uni_common::Value::Float(f) => uni_locy::FeatureValue::Float(f),
            uni_common::Value::Int(i) => uni_locy::FeatureValue::Int(i),
            uni_common::Value::Bool(b) => uni_locy::FeatureValue::Bool(b),
            uni_common::Value::String(s) => uni_locy::FeatureValue::String(s),
            uni_common::Value::Null => uni_locy::FeatureValue::Null,
            _ => uni_locy::FeatureValue::Null,
        };
    }
    uni_locy::FeatureValue::Null
}

/// Update derived scan handles before evaluating a rule's clause bodies.
///
/// For self-references: inject delta (semi-naive optimization).
/// For cross-references: inject full facts.
fn update_derived_scan_handles(
    registry: &DerivedScanRegistry,
    states: &[FixpointState],
    current_rule_idx: usize,
    rules: &[FixpointRulePlan],
) {
    let current_rule_name = &rules[current_rule_idx].name;

    for entry in &registry.entries {
        // Find the state for this entry's rule
        let source_state_idx = rules.iter().position(|r| r.name == entry.rule_name);
        let Some(source_idx) = source_state_idx else {
            continue;
        };

        let is_self = entry.rule_name == *current_rule_name;
        let data = if entry.view == DerivedScanView::Folded {
            // Issue #162: this scan reads one row per KEY carrying that KEY's
            // folded value, not one row per contribution. It is a full
            // snapshot, so semi-naive delta injection does not apply.
            states[source_idx].folded_facts().to_vec()
        } else if is_self && !rules[current_rule_idx].non_linear {
            // Self-ref in a linear rule: inject delta for semi-naive
            states[source_idx].all_delta().to_vec()
        } else {
            // Cross-ref, or self-ref of a non-linear rule (≥2 same-stratum
            // refs in one clause — Δ×Δ would miss Δ×F_old): inject full facts
            states[source_idx].all_facts().to_vec()
        };

        // If empty, write an empty batch so the scan returns zero rows
        let data = if data.is_empty() || data.iter().all(|b| b.num_rows() == 0) {
            vec![RecordBatch::new_empty(Arc::clone(&entry.schema))]
        } else {
            data
        };

        let mut guard = entry.data.write();
        *guard = data;
    }
}

// ---------------------------------------------------------------------------
// DerivedScanExec — physical plan that reads from shared data at execution time
// ---------------------------------------------------------------------------

/// Physical plan for `LocyDerivedScan` that reads from a shared `Arc<RwLock>` at
/// execution time (not at plan creation time).
///
/// This is critical for fixpoint iteration: the data handle is updated between
/// iterations, and each re-execution of the subplan must read the latest data.
pub struct DerivedScanExec {
    data: Arc<RwLock<Vec<RecordBatch>>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DerivedScanExec {
    pub fn new(data: Arc<RwLock<Vec<RecordBatch>>>, schema: SchemaRef) -> Self {
        let properties = compute_plan_properties(Arc::clone(&schema));
        Self {
            data,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for DerivedScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DerivedScanExec")
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for DerivedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DerivedScanExec")
    }
}

impl ExecutionPlan for DerivedScanExec {
    fn name(&self) -> &str {
        "DerivedScanExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let batches = {
            let guard = self.data.read();
            if guard.is_empty() {
                vec![RecordBatch::new_empty(Arc::clone(&self.schema))]
            } else {
                // Re-stamp every batch with this exec's schema. The shared
                // data Arc always holds batches with the rule's original
                // yield-schema names, but this scan may carry per-occurrence
                // aliased column names (multi-IS-ref clauses). Zero-copy:
                // only the schema pointer changes, never the columns.
                guard
                    .iter()
                    .map(|b| {
                        RecordBatch::try_new(Arc::clone(&self.schema), b.columns().to_vec())
                            .map_err(|e| {
                                datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                            })
                    })
                    .collect::<DFResult<Vec<_>>>()?
            }
        };
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            Arc::clone(&self.schema),
            None,
        )?))
    }
}

// ---------------------------------------------------------------------------
// Post-fixpoint chain — FOLD and BEST BY on converged facts
// ---------------------------------------------------------------------------

/// Apply post-FOLD WHERE (HAVING) filter to aggregated batches.
///
/// Converts each Cypher HAVING expression to a DataFusion physical expression
/// via `cypher_expr_to_df` → type coercion → `create_physical_expr`, evaluates
/// against the FOLD output, and keeps only rows where all conditions hold.
fn apply_having_filter(
    batches: Vec<RecordBatch>,
    having_exprs: &[Expr],
    schema: &SchemaRef,
    task_ctx: &Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    use arrow::compute::{and, filter_record_batch};
    use arrow_array::BooleanArray;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::execution_props::ExecutionProps;
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
    use datafusion::physical_expr::create_physical_expr;

    if batches.is_empty() {
        return Ok(batches);
    }

    // Build DFSchema from the FOLD output Arrow schema.
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).map_err(|e| {
        datafusion::common::DataFusionError::Internal(format!("HAVING schema conversion: {e}"))
    })?;

    // Use the active TaskContext's config rather than allocating a fresh
    // `SessionContext` per HAVING evaluation (~130 µs/call). HAVING uses only
    // built-in DataFusion arithmetic — no Cypher UDFs — so a default
    // `ExecutionProps` is sufficient (it's documented as cheap to construct).
    let config = (**task_ctx.session_config().options()).clone();
    let props = ExecutionProps::new();

    // Cypher Expr → DataFusion DfExpr → type-coerced DfExpr → PhysicalExpr.
    //
    // Type coercion is needed because FOLD aggregates produce Float64 (SUM,
    // AVG) or Int64 (COUNT), and literal comparisons like `total >= 100`
    // may mix Float64 columns with Int64 literals.
    let physical_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = having_exprs
        .iter()
        .map(|expr| {
            let df_expr = crate::query::df_expr::cypher_expr_to_df(expr, None).map_err(|e| {
                datafusion::common::DataFusionError::Internal(format!(
                    "HAVING expression conversion: {e}"
                ))
            })?;

            // Run DataFusion's type coercion by wrapping in a Filter plan,
            // applying the TypeCoercion analyzer rule, then extracting the
            // coerced predicate.
            let empty = datafusion::logical_expr::LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(df_schema.clone()),
                },
            );
            let filter_plan = LogicalPlanBuilder::from(empty)
                .filter(df_expr.clone())?
                .build()?;
            let coerced_expr = match TypeCoercion::new().analyze(filter_plan, &config) {
                Ok(datafusion::logical_expr::LogicalPlan::Filter(f)) => f.predicate,
                _ => df_expr,
            };

            create_physical_expr(&coerced_expr, &df_schema, &props)
        })
        .collect::<DFResult<Vec<_>>>()?;

    let mut result = Vec::new();
    for batch in batches {
        // Evaluate each condition and AND the boolean masks.
        let mut mask: Option<BooleanArray> = None;
        for phys_expr in &physical_exprs {
            let value = phys_expr.evaluate(&batch)?;
            let arr = value.into_array(batch.num_rows())?;
            let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "HAVING condition must evaluate to boolean".into(),
                )
            })?;
            mask = Some(match mask {
                None => bool_arr.clone(),
                Some(prev) => and(&prev, bool_arr).map_err(arrow_err)?,
            });
        }
        if let Some(ref m) = mask {
            let filtered = filter_record_batch(&batch, m).map_err(arrow_err)?;
            if filtered.num_rows() > 0 {
                result.push(filtered);
            }
        } else {
            result.push(batch);
        }
    }
    Ok(result)
}

/// Apply the post-fold YIELD projection to aggregated batches.
///
/// Evaluates each `(name, expr, target_type)` spec against the post-fold batch
/// (KEY columns + FOLD outputs) and assembles the final output batch in yield
/// order, coerced to the declared column types. Used for YIELD columns that are
/// computed expressions over FOLD outputs (e.g. `total * 2.0 AS score`), which
/// cannot be produced in the pre-fold body. Modeled on [`apply_having_filter`].
fn apply_post_fold_projection(
    batches: Vec<RecordBatch>,
    specs: &[(String, Expr)],
    schema: &SchemaRef,
    task_ctx: &Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::execution_props::ExecutionProps;
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
    use datafusion::physical_expr::create_physical_expr;

    if batches.is_empty() {
        return Ok(batches);
    }

    let df_schema = DFSchema::try_from(schema.as_ref().clone()).map_err(|e| {
        datafusion::common::DataFusionError::Internal(format!(
            "post-fold projection schema conversion: {e}"
        ))
    })?;
    let config = (**task_ctx.session_config().options()).clone();
    let props = ExecutionProps::new();

    // Compile each projection expression to a PhysicalExpr against the post-fold
    // schema. Type coercion (via a Projection plan) resolves mixed-type
    // arithmetic such as `total / count` (Float64 / Int64), mirroring HAVING.
    let physical_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = specs
        .iter()
        .map(|(name, expr)| {
            let df_expr = crate::query::df_expr::cypher_expr_to_df(expr, None).map_err(|e| {
                datafusion::common::DataFusionError::Internal(format!(
                    "post-fold projection '{name}' conversion: {e}"
                ))
            })?;
            let empty = datafusion::logical_expr::LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(df_schema.clone()),
                },
            );
            let proj_plan = LogicalPlanBuilder::from(empty)
                .project(vec![df_expr.clone()])?
                .build()?;
            let coerced = match TypeCoercion::new().analyze(proj_plan, &config) {
                Ok(datafusion::logical_expr::LogicalPlan::Projection(p)) => {
                    p.expr.into_iter().next().unwrap_or(df_expr)
                }
                _ => df_expr,
            };
            create_physical_expr(&coerced, &df_schema, &props)
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Build the output batches using each evaluated column's ACTUAL type — the
    // declared yield-schema type may be wrong for a fold-output alias (see the
    // schema-preference note in `apply_post_fixpoint_chain`), so forcing a cast
    // to it would corrupt the value.
    let mut result = Vec::new();
    let mut out_schema: Option<SchemaRef> = None;
    for batch in batches {
        let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(specs.len());
        for phys_expr in &physical_exprs {
            let value = phys_expr.evaluate(&batch)?;
            columns.push(value.into_array(batch.num_rows())?);
        }
        let batch_schema = match &out_schema {
            Some(s) => Arc::clone(s),
            None => {
                let fields: Vec<Arc<arrow_schema::Field>> = specs
                    .iter()
                    .zip(columns.iter())
                    .map(|((name, _), arr)| {
                        Arc::new(arrow_schema::Field::new(
                            name,
                            arr.data_type().clone(),
                            true,
                        ))
                    })
                    .collect();
                let s: SchemaRef = Arc::new(arrow_schema::Schema::new(fields));
                out_schema = Some(Arc::clone(&s));
                s
            }
        };
        result.push(RecordBatch::try_new(batch_schema, columns).map_err(arrow_err)?);
    }
    Ok(result)
}

/// Runs the post-fixpoint chain, then drops the hidden derivation
/// discriminators (issue #159).
///
/// The strip is a wrapper rather than a step inside the chain because the chain
/// has several exits — notably an early return for rules with no FOLD, BEST BY,
/// PRIORITY or HAVING, which is precisely the `ALONG`-only case that needs it.
/// A `FOLD` rule is already narrow by then (`FoldExec` emits KEY plus fold
/// columns), so for those this is a no-op.
///
/// Placement is load-bearing: the discriminators must survive *dedup*, which is
/// the whole point, but must not reach `write_facts_to_registry` or the derived
/// store. Stripping columns does not re-dedup, so multiplicity is preserved.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors the inner chain's signature"
)]
pub(crate) async fn apply_post_fixpoint_chain(
    facts: Vec<RecordBatch>,
    rule: &FixpointRulePlan,
    task_ctx: &Arc<TaskContext>,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    semiring_kind: SemiringKind,
    provenance_tracker: Option<Arc<ProvenanceStore>>,
    top_k_proofs_k: usize,
    registry: Option<Arc<DerivedScanRegistry>>,
) -> DFResult<Vec<RecordBatch>> {
    let out = apply_post_fixpoint_chain_inner(
        facts,
        rule,
        task_ctx,
        strict_probability_domain,
        probability_epsilon,
        semiring_kind,
        provenance_tracker,
        top_k_proofs_k,
        registry,
    )
    .await?;
    super::locy_complement::strip_derivation_discriminator_columns(out)
}

/// Apply post-fixpoint operators (FOLD, HAVING, BEST BY, PRIORITY) to converged facts.
#[expect(
    clippy::too_many_arguments,
    reason = "context bundle would be over-engineering for one call site"
)]
async fn apply_post_fixpoint_chain_inner(
    facts: Vec<RecordBatch>,
    rule: &FixpointRulePlan,
    task_ctx: &Arc<TaskContext>,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    semiring_kind: SemiringKind,
    provenance_tracker: Option<Arc<ProvenanceStore>>,
    top_k_proofs_k: usize,
    registry: Option<Arc<DerivedScanRegistry>>,
) -> DFResult<Vec<RecordBatch>> {
    if !rule.has_fold && !rule.has_best_by && !rule.has_priority && rule.having.is_empty() {
        return Ok(facts);
    }

    // Wrap facts in an in-memory data source.
    // Prefer the actual batch schema (from physical execution) over the
    // pre-computed yield_schema, which may have wrong inferred types
    // (e.g. Float64 for a string property).
    let schema = facts
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::clone(&rule.yield_schema));

    // Phase D D-C0: pre-compute body-row → IS-ref support map for
    // TopKProofs MNOR's DNF inclusion-exclusion math. Must be built
    // here because `facts` is moved into the memory source on the next
    // line. The map is keyed by a full-column row hash — only
    // meaningful when no downstream plan node strips/adds columns
    // between this batch view and the FoldExec input. PRIORITY drops
    // the `__priority` column, which would change row hashes; until
    // we plumb the map past PRIORITY, skip map construction for
    // PRIORITY rules (the failing TCK test doesn't use PRIORITY).
    // Read the active K from `semiring_kind` rather than the separate
    // `top_k_proofs_k` parameter — the latter is not always threaded
    // from the LocyProgram config (the semiring's `k` is the source of
    // truth).
    let topk_k: Option<usize> = match semiring_kind {
        SemiringKind::TopKProofs { k } if k > 0 => Some(k as usize),
        _ => None,
    };
    let body_support_map: Option<Arc<HashMap<Vec<u8>, Vec<ProofTerm>>>> = if topk_k.is_some()
        && !rule.has_priority
        && let Some(registry) = registry.as_ref()
    {
        let mut map: HashMap<Vec<u8>, Vec<ProofTerm>> = HashMap::new();
        for batch in &facts {
            let all_indices: Vec<usize> = (0..batch.num_columns()).collect();
            for row_idx in 0..batch.num_rows() {
                let support = collect_is_ref_inputs_for_body_row(rule, batch, row_idx, registry);
                if support.is_empty() {
                    continue;
                }
                let hash = fact_hash_key(batch, &all_indices, row_idx);
                map.insert(hash, support);
            }
        }
        if map.is_empty() {
            None
        } else {
            Some(Arc::new(map))
        }
    } else {
        None
    };

    let input: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[facts], schema.clone(), None)?;

    // Reconcile key indices: rule's indices are yield-schema positions but
    // the actual batch may have different column ordering after schema
    // reconciliation during fixpoint iteration (same pattern as
    // FixpointState::reconcile_schema).
    //
    // Fails closed on any name that does not resolve. A `filter_map` here would
    // silently *shorten* the KEY tuple, and these indices feed PriorityExec,
    // FoldExec and BestByExec below — grouping by a subset of the KEY merges
    // groups that must stay distinct, so FOLD over-aggregates, PRIORITY drops
    // rows that were max-priority in their true group, and BEST BY keeps one
    // row where it should keep several. With every name missing it degenerates
    // to a single global group. All silent wrong answers, and unlike
    // `FixpointState::reconcile_schema` (which guards by comparing arity and
    // keeping its original indices) there is nothing downstream to re-check.
    let key_column_indices: Vec<usize> = rule
        .key_column_indices
        .iter()
        .map(|&i| {
            // Positional index into the rule's own yield schema; out of range
            // would otherwise panic inside `field(i)`.
            let name = rule
                .yield_schema
                .fields()
                .get(i)
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(format!(
                        "KEY column index {i} out of range for yield schema with {} fields",
                        rule.yield_schema.fields().len()
                    ))
                })?
                .name();
            schema.index_of(name).map_err(|_| {
                datafusion::common::DataFusionError::Internal(format!(
                    "KEY column '{name}' missing from the post-fixpoint batch schema \
                     (columns: {:?}); grouping on a partial KEY would silently merge groups",
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                ))
            })
        })
        .collect::<datafusion::common::Result<Vec<usize>>>()?;

    // Apply PRIORITY first — keeps only rows with max __priority per KEY group,
    // then strips the __priority column from output.
    // Must run before FOLD so that the __priority column is still present.
    let current: Arc<dyn ExecutionPlan> = if rule.has_priority {
        let priority_schema = input.schema();
        let priority_idx = priority_schema.index_of("__priority").map_err(|_| {
            datafusion::common::DataFusionError::Internal(
                "PRIORITY rule missing __priority column".to_string(),
            )
        })?;
        Arc::new(PriorityExec::new(
            input,
            key_column_indices.clone(),
            priority_idx,
        ))
    } else {
        input
    };

    // Apply FOLD
    let current: Arc<dyn ExecutionPlan> = if rule.has_fold && !rule.fold_bindings.is_empty() {
        Arc::new(FoldExec::new_with_topk(
            current,
            key_column_indices.clone(),
            rule.fold_bindings.clone(),
            strict_probability_domain,
            probability_epsilon,
            semiring_kind,
            provenance_tracker.clone(),
            topk_k.unwrap_or(top_k_proofs_k),
            body_support_map.clone(),
        ))
    } else {
        current
    };

    // Apply HAVING (post-FOLD WHERE filter)
    let current: Arc<dyn ExecutionPlan> = if !rule.having.is_empty() {
        let batches = collect_all_partitions(&current, Arc::clone(task_ctx)).await?;
        let filtered = apply_having_filter(batches, &rule.having, &current.schema(), task_ctx)?;
        if filtered.is_empty() {
            return Ok(filtered);
        }
        MemorySourceConfig::try_new_exec(&[filtered], Arc::clone(&current.schema()), None)?
    } else {
        current
    };

    // Apply BEST BY
    let current: Arc<dyn ExecutionPlan> = if rule.has_best_by && !rule.best_by_criteria.is_empty() {
        Arc::new(BestByExec::new(
            current,
            key_column_indices.clone(),
            rule.best_by_criteria.clone(),
            rule.deterministic,
        ))
    } else {
        current
    };

    // Apply the post-fold YIELD projection LAST — so HAVING and BEST BY still
    // see the raw FOLD outputs, and its output is the final yield-schema shape.
    // Only present when a YIELD column is a computed expression over a FOLD
    // output (`total * 2.0 AS score`); the common path skips it entirely.
    if !rule.yield_projection.is_empty() {
        let batches = collect_all_partitions(&current, Arc::clone(task_ctx)).await?;
        return apply_post_fold_projection(
            batches,
            &rule.yield_projection,
            &current.schema(),
            task_ctx,
        );
    }

    collect_all_partitions(&current, Arc::clone(task_ctx)).await
}

// ---------------------------------------------------------------------------
// FixpointExec — DataFusion ExecutionPlan
// ---------------------------------------------------------------------------

/// DataFusion `ExecutionPlan` that drives semi-naive fixpoint iteration.
///
/// Has no physical children: clause bodies are re-planned from logical plans
/// on each iteration (same pattern as `RecursiveCTEExec` and `GraphApplyExec`).
pub struct FixpointExec {
    rules: Vec<FixpointRulePlan>,
    max_iterations: usize,
    timeout: Duration,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: Arc<StorageManager>,
    schema_info: Arc<UniSchema>,
    params: HashMap<String, Value>,
    derived_scan_registry: Arc<DerivedScanRegistry>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    max_derived_bytes: usize,
    /// Optional provenance tracker populated during fixpoint iteration.
    derivation_tracker: Option<Arc<ProvenanceStore>>,
    /// Shared slot written with per-rule iteration counts after convergence.
    iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    exact_probability: bool,
    max_bdd_variables: usize,
    /// Shared slot for runtime warnings collected during fixpoint iteration.
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
    /// Shared slot for groups where BDD fell back to independence mode.
    approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
    /// When > 0, retain at most this many proofs per fact (top-k provenance).
    top_k_proofs: usize,
    /// Shared flag: set to true on timeout to signal partial results.
    timeout_flag: Arc<std::sync::atomic::AtomicU8>,
    /// Active probability semiring (rollout D-7).
    semiring_kind: SemiringKind,
    /// Phase B Slice 3 registry of neural classifiers, keyed by the
    /// model name from `CREATE MODEL`. Held by `Arc` so executor clones
    /// share the same underlying map.
    classifier_registry: Arc<ClassifierRegistry>,
    /// Phase B follow-up: optional per-evaluation memoization cache
    /// for classifier outputs keyed by `(model_name, feature_hash)`.
    /// `None` → no caching; `Some` → cache shared across fixpoint
    /// iterations (and optionally across the entire query / multiple
    /// queries, when the caller threads it via `LocyConfig`).
    classifier_cache: Option<Arc<ModelInvocationCache>>,
    /// Phase C B1-B3 follow-up: per-query side-channel store
    /// for (raw, calibrated, confidence_band) records. Read by
    /// EXPLAIN; not used by the fixpoint inner loop directly
    /// (LocyModelInvokeExec writes; this struct just carries
    /// the Arc to keep the type wiring consistent across the
    /// LocyProgramExec/FixpointExec boundary).
    classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    /// Optional per-stratum profile collector. `Some` only on the `profile()`
    /// path; when set, each fixpoint iteration records per-rule timing, delta
    /// facts, and the clause-body operator tree into it. `None` → zero overhead.
    profile_collector: Option<Arc<LocyProfileCollector>>,
}

impl fmt::Debug for FixpointExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixpointExec")
            .field("rules_count", &self.rules.len())
            .field("max_iterations", &self.max_iterations)
            .field("timeout", &self.timeout)
            .field("output_schema", &self.output_schema)
            .field("max_derived_bytes", &self.max_derived_bytes)
            .finish_non_exhaustive()
    }
}

impl FixpointExec {
    /// Create a new `FixpointExec`.
    #[expect(
        clippy::too_many_arguments,
        reason = "FixpointExec configuration needs all context"
    )]
    #[deprecated(
        note = "use `new_with_semiring_classifiers_and_cache` (or the lighter \
                `new_with_semiring_and_classifiers` / `new_with_semiring`) — \
                this legacy ctor defaults the semiring to AddMultProb and \
                ships no classifier registry, which the Phase B+ runtime needs \
                explicitly. To be removed after C0 Stage 2."
    )]
    pub fn new(
        rules: Vec<FixpointRulePlan>,
        max_iterations: usize,
        timeout: Duration,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        output_schema: SchemaRef,
        max_derived_bytes: usize,
        derivation_tracker: Option<Arc<ProvenanceStore>>,
        iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
        approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
        top_k_proofs: usize,
        timeout_flag: Arc<std::sync::atomic::AtomicU8>,
    ) -> Self {
        Self::new_with_semiring_and_classifiers(
            rules,
            max_iterations,
            timeout,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            derived_scan_registry,
            output_schema,
            max_derived_bytes,
            derivation_tracker,
            iteration_counts,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            warnings_slot,
            approximate_slot,
            top_k_proofs,
            timeout_flag,
            SemiringKind::AddMultProb,
            Arc::new(ClassifierRegistry::new()),
        )
    }

    /// Variant accepting an explicit [`SemiringKind`]. Empty classifier
    /// registry; for the full variant call
    /// [`FixpointExec::new_with_semiring_and_classifiers`].
    #[expect(
        clippy::too_many_arguments,
        reason = "FixpointExec configuration needs all context"
    )]
    pub fn new_with_semiring(
        rules: Vec<FixpointRulePlan>,
        max_iterations: usize,
        timeout: Duration,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        output_schema: SchemaRef,
        max_derived_bytes: usize,
        derivation_tracker: Option<Arc<ProvenanceStore>>,
        iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
        approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
        top_k_proofs: usize,
        timeout_flag: Arc<std::sync::atomic::AtomicU8>,
        semiring_kind: SemiringKind,
    ) -> Self {
        Self::new_with_semiring_and_classifiers(
            rules,
            max_iterations,
            timeout,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            derived_scan_registry,
            output_schema,
            max_derived_bytes,
            derivation_tracker,
            iteration_counts,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            warnings_slot,
            approximate_slot,
            top_k_proofs,
            timeout_flag,
            semiring_kind,
            Arc::new(ClassifierRegistry::new()),
        )
    }

    /// Phase B Slice 3 entry: accepts both the semiring kind and the
    /// runtime classifier registry. The planner uses this when the
    /// program contains `CREATE MODEL` declarations.
    #[expect(
        clippy::too_many_arguments,
        reason = "FixpointExec configuration needs all context"
    )]
    pub fn new_with_semiring_and_classifiers(
        rules: Vec<FixpointRulePlan>,
        max_iterations: usize,
        timeout: Duration,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        output_schema: SchemaRef,
        max_derived_bytes: usize,
        derivation_tracker: Option<Arc<ProvenanceStore>>,
        iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
        approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
        top_k_proofs: usize,
        timeout_flag: Arc<std::sync::atomic::AtomicU8>,
        semiring_kind: SemiringKind,
        classifier_registry: Arc<ClassifierRegistry>,
    ) -> Self {
        Self::new_with_semiring_classifiers_and_cache(
            rules,
            max_iterations,
            timeout,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            derived_scan_registry,
            output_schema,
            max_derived_bytes,
            derivation_tracker,
            iteration_counts,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            warnings_slot,
            approximate_slot,
            top_k_proofs,
            timeout_flag,
            semiring_kind,
            classifier_registry,
            None,
            None,
        )
    }

    /// Phase B follow-up: full constructor accepting an optional
    /// memoization cache. Existing callers default to `None` (no cache);
    /// the impl_locy.rs entry passes the user's `config.classifier_cache`.
    #[expect(
        clippy::too_many_arguments,
        reason = "FixpointExec configuration needs all context"
    )]
    pub fn new_with_semiring_classifiers_and_cache(
        rules: Vec<FixpointRulePlan>,
        max_iterations: usize,
        timeout: Duration,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        output_schema: SchemaRef,
        max_derived_bytes: usize,
        derivation_tracker: Option<Arc<ProvenanceStore>>,
        iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
        approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
        top_k_proofs: usize,
        timeout_flag: Arc<std::sync::atomic::AtomicU8>,
        semiring_kind: SemiringKind,
        classifier_registry: Arc<ClassifierRegistry>,
        classifier_cache: Option<Arc<ModelInvocationCache>>,
        classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    ) -> Self {
        let properties = compute_plan_properties(Arc::clone(&output_schema));
        Self {
            rules,
            max_iterations,
            timeout,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            derived_scan_registry,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            max_derived_bytes,
            derivation_tracker,
            iteration_counts,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            warnings_slot,
            approximate_slot,
            top_k_proofs,
            timeout_flag,
            semiring_kind,
            classifier_registry,
            classifier_cache,
            classifier_provenance_store,
            profile_collector: None,
        }
    }

    /// Returns the shared iteration counts slot for post-execution inspection.
    pub fn iteration_counts(&self) -> Arc<StdRwLock<HashMap<String, usize>>> {
        Arc::clone(&self.iteration_counts)
    }

    /// Attach a profile collector so this stratum's fixpoint records per-rule,
    /// per-iteration timing, delta facts, and clause-body operator metrics.
    ///
    /// Mirrors `set_derivation_tracker`: call before wrapping the exec in an
    /// `Arc` and executing. Only the Locy `profile()` path sets this.
    pub fn set_profile_collector(&mut self, collector: Arc<LocyProfileCollector>) {
        self.profile_collector = Some(collector);
    }
}

impl DisplayAs for FixpointExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FixpointExec: rules=[{}], max_iter={}, timeout={:?}",
            self.rules
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            self.max_iterations,
            self.timeout,
        )
    }
}

impl ExecutionPlan for FixpointExec {
    fn name(&self) -> &str {
        "FixpointExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // No physical children — clause bodies are re-planned each iteration
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "FixpointExec has no children".to_string(),
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

        // Clone all fields for the async closure
        let rules = self
            .rules
            .iter()
            .map(|r| {
                // We need to clone the FixpointRulePlan, but it contains LogicalPlan
                // which doesn't implement Clone traditionally. However, our LogicalPlan
                // does implement Clone since it's an enum.
                FixpointRulePlan {
                    name: r.name.clone(),
                    clauses: r
                        .clauses
                        .iter()
                        .map(|c| FixpointClausePlan {
                            body_logical: c.body_logical.clone(),
                            is_ref_bindings: c.is_ref_bindings.clone(),
                            priority: c.priority,
                            along_bindings: c.along_bindings.clone(),
                            model_invocations: c.model_invocations.clone(),
                        })
                        .collect(),
                    yield_schema: Arc::clone(&r.yield_schema),
                    key_column_indices: r.key_column_indices.clone(),
                    priority: r.priority,
                    has_fold: r.has_fold,
                    fold_bindings: r.fold_bindings.clone(),
                    having: r.having.clone(),
                    has_best_by: r.has_best_by,
                    best_by_criteria: r.best_by_criteria.clone(),
                    has_priority: r.has_priority,
                    deterministic: r.deterministic,
                    prob_column_name: r.prob_column_name.clone(),
                    non_linear: r.non_linear,
                    yield_projection: r.yield_projection.clone(),
                }
            })
            .collect();

        let max_iterations = self.max_iterations;
        let timeout = self.timeout;
        let graph_ctx = Arc::clone(&self.graph_ctx);
        let session_ctx = Arc::clone(&self.session_ctx);
        let storage = Arc::clone(&self.storage);
        let schema_info = Arc::clone(&self.schema_info);
        let params = self.params.clone();
        let registry = Arc::clone(&self.derived_scan_registry);
        let output_schema = Arc::clone(&self.output_schema);
        let max_derived_bytes = self.max_derived_bytes;
        let derivation_tracker = self.derivation_tracker.clone();
        let iteration_counts = Arc::clone(&self.iteration_counts);
        let strict_probability_domain = self.strict_probability_domain;
        let probability_epsilon = self.probability_epsilon;
        let exact_probability = self.exact_probability;
        let max_bdd_variables = self.max_bdd_variables;
        let warnings_slot = Arc::clone(&self.warnings_slot);
        let approximate_slot = Arc::clone(&self.approximate_slot);
        let top_k_proofs = self.top_k_proofs;
        let timeout_flag = Arc::clone(&self.timeout_flag);
        let semiring_kind = self.semiring_kind;
        let classifier_registry = Arc::clone(&self.classifier_registry);
        let classifier_cache = self.classifier_cache.as_ref().map(Arc::clone);
        let classifier_provenance_store = self.classifier_provenance_store.as_ref().map(Arc::clone);
        let profile_collector = self.profile_collector.as_ref().map(Arc::clone);

        let fut = async move {
            run_fixpoint_loop(
                rules,
                max_iterations,
                timeout,
                graph_ctx,
                session_ctx,
                storage,
                schema_info,
                params,
                registry,
                output_schema,
                max_derived_bytes,
                derivation_tracker,
                iteration_counts,
                strict_probability_domain,
                probability_epsilon,
                exact_probability,
                max_bdd_variables,
                warnings_slot,
                approximate_slot,
                top_k_proofs,
                timeout_flag,
                semiring_kind,
                classifier_registry,
                classifier_cache,
                classifier_provenance_store,
                profile_collector,
            )
            .await
        };

        Ok(Box::pin(FixpointStream {
            state: FixpointStreamState::Running(Box::pin(fut)),
            schema: Arc::clone(&self.output_schema),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// FixpointStream — async state machine for streaming results
// ---------------------------------------------------------------------------

enum FixpointStreamState {
    /// Fixpoint loop is running.
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<Vec<RecordBatch>>> + Send>>),
    /// Emitting accumulated result batches one at a time.
    Emitting(Vec<RecordBatch>, usize),
    /// All batches emitted.
    Done,
}

struct FixpointStream {
    state: FixpointStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for FixpointStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let metrics = this.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            match &mut this.state {
                FixpointStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batches)) => {
                        if batches.is_empty() {
                            this.state = FixpointStreamState::Done;
                            return Poll::Ready(None);
                        }
                        this.state = FixpointStreamState::Emitting(batches, 0);
                        // Loop to emit first batch
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = FixpointStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                FixpointStreamState::Emitting(batches, idx) => {
                    if *idx >= batches.len() {
                        this.state = FixpointStreamState::Done;
                        return Poll::Ready(None);
                    }
                    let batch = batches[*idx].clone();
                    *idx += 1;
                    this.metrics.record_output(batch.num_rows());
                    return Poll::Ready(Some(Ok(batch)));
                }
                FixpointStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for FixpointStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn make_batch(names: &[&str], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(
                    names.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    // --- FixpointState dedup tests ---

    #[tokio::test]
    async fn test_fixpoint_state_empty_facts_adds_all() {
        let schema = test_schema();
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None, false);

        let batch = make_batch(&["a", "b", "c"], &[1, 2, 3]);
        let changed = state.merge_delta(vec![batch], None).await.unwrap();

        assert!(changed);
        assert_eq!(state.all_facts().len(), 1);
        assert_eq!(state.all_facts()[0].num_rows(), 3);
        assert_eq!(state.all_delta().len(), 1);
        assert_eq!(state.all_delta()[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fixpoint_state_exact_duplicates_excluded() {
        let schema = test_schema();
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None, false);

        let batch1 = make_batch(&["a", "b"], &[1, 2]);
        state.merge_delta(vec![batch1], None).await.unwrap();

        // Same rows again
        let batch2 = make_batch(&["a", "b"], &[1, 2]);
        let changed = state.merge_delta(vec![batch2], None).await.unwrap();
        assert!(!changed);
        assert!(
            state.all_delta().is_empty() || state.all_delta().iter().all(|b| b.num_rows() == 0)
        );
    }

    #[tokio::test]
    async fn test_fixpoint_state_partial_overlap() {
        let schema = test_schema();
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None, false);

        let batch1 = make_batch(&["a", "b"], &[1, 2]);
        state.merge_delta(vec![batch1], None).await.unwrap();

        // "a":1 is duplicate, "c":3 is new
        let batch2 = make_batch(&["a", "c"], &[1, 3]);
        let changed = state.merge_delta(vec![batch2], None).await.unwrap();
        assert!(changed);

        // Delta should have only "c":3
        let delta_rows: usize = state.all_delta().iter().map(|b| b.num_rows()).sum();
        assert_eq!(delta_rows, 1);

        // Total facts: a:1, b:2, c:3
        let total_rows: usize = state.all_facts().iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_fixpoint_state_convergence() {
        let schema = test_schema();
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None, false);

        let batch = make_batch(&["a"], &[1]);
        state.merge_delta(vec![batch], None).await.unwrap();

        // Empty candidates → converged
        let changed = state.merge_delta(vec![], None).await.unwrap();
        assert!(!changed);
        assert!(state.is_converged());
    }

    // --- RowDedupState tests ---

    #[test]
    fn test_row_dedup_persistent_across_calls() {
        // RowDedupState should remember rows from the first call so the second
        // call does not re-accept them (O(M) per iteration, no facts re-scan).
        let schema = test_schema();
        let mut rd = RowDedupState::try_new(&schema).expect("schema should be supported");

        let batch1 = make_batch(&["a", "b"], &[1, 2]);
        let delta1 = rd.compute_delta(&[batch1], &schema).unwrap();
        // First call: both rows are new.
        let rows1: usize = delta1.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows1, 2);

        // Second call with same rows: seen set already has them → empty delta.
        let batch2 = make_batch(&["a", "b"], &[1, 2]);
        let delta2 = rd.compute_delta(&[batch2], &schema).unwrap();
        let rows2: usize = delta2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, 0);

        // Third call with one old + one new: only the new row is returned.
        let batch3 = make_batch(&["a", "c"], &[1, 3]);
        let delta3 = rd.compute_delta(&[batch3], &schema).unwrap();
        let rows3: usize = delta3.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows3, 1);
    }

    #[test]
    fn test_row_dedup_null_handling() {
        use arrow_array::StringArray;
        use arrow_schema::{DataType, Field, Schema};

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let mut rd = RowDedupState::try_new(&schema).expect("schema should be supported");

        // Two rows: (NULL, 1) and (NULL, 1) — same NULLs → duplicate.
        let batch_nulls = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>, None::<&str>])),
                Arc::new(arrow_array::Int64Array::from(vec![1i64, 1i64])),
            ],
        )
        .unwrap();
        let delta = rd.compute_delta(&[batch_nulls], &schema).unwrap();
        let rows: usize = delta.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "two identical NULL rows should be deduped to one");

        // (NULL, 2) — NULL in same col but different non-null col → distinct.
        let batch_diff = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(arrow_array::Int64Array::from(vec![2i64])),
            ],
        )
        .unwrap();
        let delta2 = rd.compute_delta(&[batch_diff], &schema).unwrap();
        let rows2: usize = delta2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, 1, "(NULL, 2) is distinct from (NULL, 1)");
    }

    #[test]
    fn test_row_dedup_within_candidate_dedup() {
        // Duplicate rows within a single candidate batch should be collapsed to one.
        let schema = test_schema();
        let mut rd = RowDedupState::try_new(&schema).expect("schema should be supported");

        // Batch with three rows: a:1, a:1, b:2 — "a:1" appears twice.
        let batch = make_batch(&["a", "a", "b"], &[1, 1, 2]);
        let delta = rd.compute_delta(&[batch], &schema).unwrap();
        let rows: usize = delta.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "within-batch dup should be collapsed: a:1, b:2");
    }

    // --- Float rounding tests ---

    #[test]
    fn test_round_float_columns_near_duplicates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("dist", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a")])),
                Arc::new(Float64Array::from(vec![1.0000000000001, 1.0000000000002])),
            ],
        )
        .unwrap();

        let rounded = round_float_columns(&[batch]);
        assert_eq!(rounded.len(), 1);
        let col = rounded[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Both should round to same value
        assert_eq!(col.value(0), col.value(1));
    }

    // --- DerivedScanRegistry tests ---

    #[test]
    fn test_registry_write_read_round_trip() {
        let schema = test_schema();
        let data = Arc::new(RwLock::new(Vec::new()));
        let mut reg = DerivedScanRegistry::new();
        reg.add(DerivedScanEntry {
            scan_index: 0,
            rule_name: "reachable".into(),
            is_self_ref: true,
            view: DerivedScanView::Contributions,
            data: Arc::clone(&data),
            schema: Arc::clone(&schema),
        });

        let batch = make_batch(&["x"], &[42]);
        reg.write_data(0, vec![batch.clone()]);

        let entry = reg.get(0).unwrap();
        let guard = entry.data.read();
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].num_rows(), 1);
    }

    #[test]
    fn test_registry_entries_for_rule() {
        let schema = test_schema();
        let mut reg = DerivedScanRegistry::new();
        reg.add(DerivedScanEntry {
            scan_index: 0,
            rule_name: "r1".into(),
            is_self_ref: true,
            view: DerivedScanView::Contributions,
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::clone(&schema),
        });
        reg.add(DerivedScanEntry {
            scan_index: 1,
            rule_name: "r2".into(),
            is_self_ref: false,
            view: DerivedScanView::Contributions,
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::clone(&schema),
        });
        reg.add(DerivedScanEntry {
            scan_index: 2,
            rule_name: "r1".into(),
            is_self_ref: false,
            view: DerivedScanView::Contributions,
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::clone(&schema),
        });

        assert_eq!(reg.entries_for_rule("r1").len(), 2);
        assert_eq!(reg.entries_for_rule("r2").len(), 1);
        assert_eq!(reg.entries_for_rule("r3").len(), 0);
    }

    // --- MonotonicAggState tests ---

    #[test]
    fn test_monotonic_agg_update_and_stability() {
        let bindings = vec![MonotonicFoldBinding {
            fold_name: "total".into(),
            aggregate: std::sync::Arc::new(uni_plugin_builtin::locy_aggregates::SumAgg),
            input_col_index: 1,
            input_col_name: None,
        }];
        let mut agg = MonotonicAggState::new(bindings);

        // First update
        let batch = make_batch(&["a"], &[10]);
        agg.snapshot();
        let changed = agg
            .update(&[0], &[batch], false, SemiringKind::AddMultProb)
            .unwrap();
        assert!(changed);
        assert!(!agg.is_stable()); // changed since snapshot

        // Snapshot and check stability with no new data
        agg.snapshot();
        let changed = agg
            .update(&[0], &[], false, SemiringKind::AddMultProb)
            .unwrap();
        assert!(!changed);
        assert!(agg.is_stable());
    }

    // --- Memory limit test ---

    #[tokio::test]
    async fn test_memory_limit_exceeded() {
        let schema = test_schema();
        // Set a tiny limit
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1, None, false);

        let batch = make_batch(&["a", "b", "c"], &[1, 2, 3]);
        let result = state.merge_delta(vec![batch], None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("memory limit"), "Error was: {}", err);
    }

    // --- FixpointStream lifecycle test ---

    #[tokio::test]
    async fn test_fixpoint_stream_emitting() {
        use futures::StreamExt;

        let schema = test_schema();
        let batch1 = make_batch(&["a"], &[1]);
        let batch2 = make_batch(&["b"], &[2]);

        let metrics = ExecutionPlanMetricsSet::new();
        let baseline = BaselineMetrics::new(&metrics, 0);

        let mut stream = FixpointStream {
            state: FixpointStreamState::Emitting(vec![batch1, batch2], 0),
            schema,
            metrics: baseline,
        };

        let stream = Pin::new(&mut stream);
        let batches: Vec<RecordBatch> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[1].num_rows(), 1);
    }

    // ── MonotonicAggState MNOR/MPROD tests ──────────────────────────────

    fn make_f64_batch(names: &[&str], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_nor_binding() -> Vec<MonotonicFoldBinding> {
        vec![MonotonicFoldBinding {
            fold_name: "prob".into(),
            aggregate: std::sync::Arc::new(uni_plugin_builtin::locy_aggregates::MnorAgg),
            input_col_index: 1,
            input_col_name: None,
        }]
    }

    fn make_prod_binding() -> Vec<MonotonicFoldBinding> {
        vec![MonotonicFoldBinding {
            fold_name: "prob".into(),
            aggregate: std::sync::Arc::new(uni_plugin_builtin::locy_aggregates::MprodAgg),
            input_col_index: 1,
            input_col_name: None,
        }]
    }

    fn acc_key(name: &str) -> (Vec<ScalarKey>, String) {
        (vec![ScalarKey::Utf8(name.to_string())], "prob".to_string())
    }

    #[test]
    fn test_monotonic_nor_first_update() {
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[0.3]);
        let changed = agg
            .update(&[0], &[batch], false, SemiringKind::AddMultProb)
            .unwrap();
        assert!(changed);
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.3).abs() < 1e-10, "expected 0.3, got {}", val);
    }

    #[test]
    fn test_monotonic_nor_two_updates() {
        // Incremental NOR: acc = 1-(1-0.3)(1-0.5) = 0.65
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch1 = make_f64_batch(&["a"], &[0.3]);
        agg.update(&[0], &[batch1], false, SemiringKind::AddMultProb)
            .unwrap();
        let batch2 = make_f64_batch(&["a"], &[0.5]);
        agg.update(&[0], &[batch2], false, SemiringKind::AddMultProb)
            .unwrap();
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.65).abs() < 1e-10, "expected 0.65, got {}", val);
    }

    #[test]
    fn test_monotonic_prod_first_update() {
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch = make_f64_batch(&["a"], &[0.6]);
        let changed = agg
            .update(&[0], &[batch], false, SemiringKind::AddMultProb)
            .unwrap();
        assert!(changed);
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.6).abs() < 1e-10, "expected 0.6, got {}", val);
    }

    #[test]
    fn test_monotonic_prod_two_updates() {
        // Incremental PROD: acc = 0.6 * 0.8 = 0.48
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch1 = make_f64_batch(&["a"], &[0.6]);
        agg.update(&[0], &[batch1], false, SemiringKind::AddMultProb)
            .unwrap();
        let batch2 = make_f64_batch(&["a"], &[0.8]);
        agg.update(&[0], &[batch2], false, SemiringKind::AddMultProb)
            .unwrap();
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.48).abs() < 1e-10, "expected 0.48, got {}", val);
    }

    #[test]
    fn test_monotonic_nor_stability() {
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[0.3]);
        agg.update(&[0], &[batch], false, SemiringKind::AddMultProb)
            .unwrap();
        agg.snapshot();
        let changed = agg
            .update(&[0], &[], false, SemiringKind::AddMultProb)
            .unwrap();
        assert!(!changed);
        assert!(agg.is_stable());
    }

    #[test]
    fn test_monotonic_prod_stability() {
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch = make_f64_batch(&["a"], &[0.6]);
        agg.update(&[0], &[batch], false, SemiringKind::AddMultProb)
            .unwrap();
        agg.snapshot();
        let changed = agg
            .update(&[0], &[], false, SemiringKind::AddMultProb)
            .unwrap();
        assert!(!changed);
        assert!(agg.is_stable());
    }

    #[test]
    fn test_monotonic_nor_multi_group() {
        // (a,0.3),(b,0.5) then (a,0.5),(b,0.2) → a=0.65, b=0.6
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch1 = make_f64_batch(&["a", "b"], &[0.3, 0.5]);
        agg.update(&[0], &[batch1], false, SemiringKind::AddMultProb)
            .unwrap();
        let batch2 = make_f64_batch(&["a", "b"], &[0.5, 0.2]);
        agg.update(&[0], &[batch2], false, SemiringKind::AddMultProb)
            .unwrap();

        let val_a = agg.get_accumulator(&acc_key("a")).unwrap();
        let val_b = agg.get_accumulator(&acc_key("b")).unwrap();
        assert!(
            (val_a - 0.65).abs() < 1e-10,
            "expected a=0.65, got {}",
            val_a
        );
        assert!((val_b - 0.6).abs() < 1e-10, "expected b=0.6, got {}", val_b);
    }

    #[test]
    fn test_monotonic_prod_zero_absorbing() {
        // Zero absorbs: once 0.0, all further updates stay 0.0
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch1 = make_f64_batch(&["a"], &[0.5]);
        agg.update(&[0], &[batch1], false, SemiringKind::AddMultProb)
            .unwrap();
        let batch2 = make_f64_batch(&["a"], &[0.0]);
        agg.update(&[0], &[batch2], false, SemiringKind::AddMultProb)
            .unwrap();

        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.0).abs() < 1e-10, "expected 0.0, got {}", val);

        // Further updates don't change the absorbing zero
        agg.snapshot();
        let batch3 = make_f64_batch(&["a"], &[0.5]);
        let changed = agg
            .update(&[0], &[batch3], false, SemiringKind::AddMultProb)
            .unwrap();
        assert!(!changed);
        assert!(agg.is_stable());
    }

    #[test]
    fn test_monotonic_nor_clamping() {
        // 1.5 clamped to 1.0: acc = 1-(1-0)(1-1) = 1.0
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[1.5]);
        agg.update(&[0], &[batch], false, SemiringKind::AddMultProb)
            .unwrap();
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 1.0).abs() < 1e-10, "expected 1.0, got {}", val);
    }

    #[test]
    fn test_monotonic_nor_absorbing() {
        // p=1.0 absorbs: 0.3 then 1.0 → 1.0
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch1 = make_f64_batch(&["a"], &[0.3]);
        agg.update(&[0], &[batch1], false, SemiringKind::AddMultProb)
            .unwrap();
        let batch2 = make_f64_batch(&["a"], &[1.0]);
        agg.update(&[0], &[batch2], false, SemiringKind::AddMultProb)
            .unwrap();
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 1.0).abs() < 1e-10, "expected 1.0, got {}", val);
    }

    // ── MonotonicAggState strict mode tests (Phase 5) ─────────────────────

    #[test]
    fn test_monotonic_agg_strict_nor_rejects() {
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[1.5]);
        let result = agg.update(&[0], &[batch], true, SemiringKind::AddMultProb);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[test]
    fn test_monotonic_agg_strict_prod_rejects() {
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch = make_f64_batch(&["a"], &[2.0]);
        let result = agg.update(&[0], &[batch], true, SemiringKind::AddMultProb);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[test]
    fn test_monotonic_agg_strict_accepts_valid() {
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[0.5]);
        let result = agg.update(&[0], &[batch], true, SemiringKind::AddMultProb);
        assert!(result.is_ok());
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.5).abs() < 1e-10, "expected 0.5, got {}", val);
    }
}
