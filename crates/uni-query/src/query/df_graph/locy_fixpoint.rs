// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fixpoint iteration operator for recursive Locy strata.
//!
//! `FixpointExec` drives semi-naive evaluation: it repeatedly evaluates the rules
//! in a recursive stratum, feeding back deltas until no new facts are produced.

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    ScalarKey, arrow_err, collect_all_partitions, compute_plan_properties,
    execute_locy_clause_body, extract_scalar_key, operator_reservation,
};
use crate::query::df_graph::locy_best_by::{BestByExec, SortCriterion, order_column};
use crate::query::df_graph::locy_errors::LocyRuntimeError;
use crate::query::df_graph::locy_explain::{
    ProofTerm, ProvenanceAnnotation, ProvenanceStore, compute_proof_probability, top_k_fact_hash,
};
use crate::query::df_graph::locy_fold::{FoldBinding, FoldExec};
use crate::query::df_graph::locy_priority::PriorityExec;
use crate::query::df_graph::locy_profile::{
    LocyProfileCollector, PlanningMark, synthetic_operator,
};
use crate::query::df_graph::locy_program::interruption;
use crate::query::executor::core::OperatorStats;
use crate::query::planner::LogicalPlan;
use arrow_array::RecordBatch;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{Field, Schema as ArrowSchema, SchemaRef};
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
    /// One reference of a non-linear clause (two or more references into its
    /// own stratum), never shared with another scan (P5, R3). Between clause
    /// evaluations it holds what a shared self-reference scan would; while
    /// its clause runs, each semi-naive pass feeds it the delta, the old
    /// facts or the full facts (see `run_fixpoint_loop`). Negation and
    /// provenance never resolve to it.
    pub exclusive: bool,
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
        let keys = float_key_view(candidates);
        for (batch, key_batch) in candidates.iter().zip(&keys) {
            if batch.num_rows() == 0 {
                continue;
            }

            // Vectorized encoding of all rows in this batch, from the key view.
            let arrays: Vec<_> = key_batch.columns().to_vec();
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

/// `facts` with each row kept once, by the same rule the fixpoint uses to
/// suppress re-derivations: every column, compared through
/// [`float_key_view`].
///
/// A derived fact is a row, not a derivation (whole-row deduplication, Locy
/// reference §6.2). The fixpoint gets this from its delta computation; a
/// non-recursive rule, evaluated in one pass, has none, so three parallel edges
/// derived `(a, b)` three times.
pub(crate) fn dedup_fact_rows(facts: Vec<RecordBatch>) -> DFResult<Vec<RecordBatch>> {
    let Some(schema) = facts.first().map(RecordBatch::schema) else {
        return Ok(facts);
    };
    if let Some(mut seen) = RowDedupState::try_new(&schema) {
        return seen.compute_delta(&facts, &schema);
    }
    let all_columns: Vec<usize> = (0..schema.fields().len()).collect();
    let mut seen: HashSet<Vec<ScalarKey>> = HashSet::new();
    let keys = float_key_view(&facts);
    let mut out = Vec::with_capacity(facts.len());
    for (batch, key_batch) in facts.iter().zip(&keys) {
        let keep: Vec<bool> = (0..batch.num_rows())
            .map(|row| seen.insert(extract_scalar_key(key_batch, &all_columns, row)))
            .collect();
        let mask = arrow_array::BooleanArray::from(keep);
        out.push(arrow::compute::filter_record_batch(batch, &mask).map_err(arrow_err)?);
    }
    Ok(out)
}

/// Debug-build invariant: a rule's stored facts hold no row twice. Not checked
/// for an ALONG rule, whose facts are per path.
///
/// Both evaluation paths are meant to guarantee it — the fixpoint through its
/// delta computation, a non-recursive rule through [`dedup_fact_rows`] — and the
/// non-recursive path once silently did not. A regression of either now fails
/// every test that derives a duplicate, naming the rule, instead of returning
/// an extra row. Release builds skip the check.
pub(crate) fn debug_assert_facts_are_rows(rule: &str, facts: &[RecordBatch]) {
    if cfg!(debug_assertions) {
        let total: usize = facts.iter().map(RecordBatch::num_rows).sum();
        let distinct: usize = dedup_fact_rows(facts.to_vec())
            .unwrap_or_else(|e| panic!("Locy rule `{rule}`: cannot check its facts: {e}"))
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(
            total,
            distinct,
            "Locy rule `{rule}`: {} of its {total} facts are duplicate rows; \
             a derived fact is a row (whole-row deduplication)",
            total - distinct
        );
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
    /// How many leading batches of `facts` are exactly the facts the latest
    /// merge did not add (`facts \ delta`), when the merge kept them apart;
    /// `None` when the delta is interleaved with them (a FOLD contribution
    /// merge), and [`Self::old_facts`] filters instead. A non-linear clause's
    /// semi-naive passes read it (P5).
    old_len: Option<usize>,
    /// BEST BY rules only: the held row of every KEY, so a merge finds the
    /// row a candidate competes with without re-sorting the facts.
    best_index: HashMap<Vec<ScalarKey>, BestSlot>,
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
    /// Post-FOLD definitional threshold (`REQUIRE`, issue #265).
    ///
    /// Applied to the snapshot below on every iteration, which is what makes
    /// it constrain the recursion: the snapshot is exactly what a same-stratum
    /// self-reference reads. Empty for a rule that carries no `REQUIRE`, in
    /// which case the stage is skipped entirely.
    require: Vec<Expr>,
    /// The query's bound parameters, so a `REQUIRE` that references `$name`
    /// resolves on the per-iteration snapshot exactly as it does on the final
    /// answer (#273).
    params: HashMap<String, Value>,
    /// KEY-grouped snapshot of `facts`, in the rule's contribution schema.
    folded: Vec<RecordBatch>,
    /// Whole-row set from the previous merge, for change detection.
    prev_rows: HashSet<Vec<ScalarKey>>,
    /// The rows of [`Self::folded`] that the previous snapshot did not hold:
    /// a KEY that entered it, or one whose row changed in any column (issue
    /// #301). What a linear self-reference reads; see
    /// [`update_derived_scan_handles`] for why that is equivalent to reading
    /// the whole snapshot.
    folded_delta: Vec<RecordBatch>,
    /// The rows of [`Self::folded`] that the previous snapshot already held:
    /// `folded \ folded_delta`, what a non-linear clause's semi-naive passes
    /// read as the old snapshot (P5).
    folded_old: Vec<RecordBatch>,
    /// Exact (unrounded) whole-row set of [`Self::folded`], for
    /// [`Self::folded_delta`].
    folded_rows: HashSet<Vec<ScalarKey>>,
    /// Whether to keep the snapshot plan's operator metrics, which only the
    /// `profile()` path reads.
    collect_ops: bool,
    /// The operator metrics of the last snapshot recomputation (PRIORITY,
    /// FOLD), so a profile shows the per-iteration fold cost next to the
    /// clause bodies' (issue #301). Empty unless [`Self::collect_ops`].
    last_ops: Vec<OperatorStats>,
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
            old_len: Some(0),
            best_index: HashMap::new(),
        }
    }

    /// Turn on the per-iteration folded view for this rule.
    pub fn enable_fold_view(
        &mut self,
        bindings: Vec<FoldBinding>,
        has_priority: bool,
        probability_epsilon: f64,
        require: Vec<Expr>,
        params: HashMap<String, Value>,
    ) {
        self.fold_view = Some(FoldViewState {
            bindings,
            has_priority,
            probability_epsilon,
            require,
            params,
            folded: Vec::new(),
            prev_rows: HashSet::new(),
            folded_delta: Vec::new(),
            folded_old: Vec::new(),
            folded_rows: HashSet::new(),
            collect_ops: false,
            last_ops: Vec::new(),
        });
    }

    /// Keeps the folded snapshot's operator metrics for
    /// [`Self::take_fold_ops`]. Only the `profile()` path asks for them.
    pub fn collect_fold_ops(&mut self) {
        if let Some(fv) = self.fold_view.as_mut() {
            fv.collect_ops = true;
        }
    }

    /// The operator metrics of the last folded-snapshot recomputation, once.
    pub fn take_fold_ops(&mut self) -> Vec<OperatorStats> {
        self.fold_view
            .as_mut()
            .map(|fv| std::mem::take(&mut fv.last_ops))
            .unwrap_or_default()
    }

    /// Whether the per-iteration folded view is active.
    pub fn has_fold_view(&self) -> bool {
        self.fold_view.is_some()
    }

    /// The KEY-grouped snapshot a same-stratum reference should read.
    pub fn folded_facts(&self) -> &[RecordBatch] {
        self.fold_view.as_ref().map_or(&[], |fv| &fv.folded)
    }

    /// The rows of [`Self::folded_facts`] that are new or changed since the
    /// previous snapshot (issue #301).
    pub fn folded_delta(&self) -> &[RecordBatch] {
        self.fold_view.as_ref().map_or(&[], |fv| &fv.folded_delta)
    }

    /// The rows of [`Self::folded_facts`] the previous snapshot already held.
    pub fn folded_old(&self) -> &[RecordBatch] {
        self.fold_view.as_ref().map_or(&[], |fv| &fv.folded_old)
    }

    /// Replaces the folded snapshot, recording which of its rows the previous
    /// one did not hold.
    ///
    /// Rows are compared exactly, over every column — not through the rounded
    /// key view change detection uses — so a value that moves by less than the
    /// rounding is still re-read, as it would be from the whole snapshot.
    fn install_folded(&mut self, folded: Vec<RecordBatch>) -> DFResult<()> {
        let Some(fv) = self.fold_view.as_mut() else {
            return Ok(());
        };
        let mut rows_now: HashSet<Vec<ScalarKey>> = HashSet::new();
        let mut delta: Vec<RecordBatch> = Vec::new();
        let mut old: Vec<RecordBatch> = Vec::new();
        for batch in &folded {
            let all: Vec<usize> = (0..batch.num_columns()).collect();
            let mut changed: Vec<bool> = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let key = extract_scalar_key(batch, &all, row);
                changed.push(!fv.folded_rows.contains(&key));
                rows_now.insert(key);
            }
            let (new_rows, old_rows) = split_rows(batch, &changed)?;
            delta.extend(new_rows);
            old.extend(old_rows);
        }
        fv.folded_rows = rows_now;
        fv.folded_delta = delta;
        fv.folded_old = old;
        fv.folded = folded;
        Ok(())
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
            self.reconcile_schema(&first.schema())?;
        }
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
            self.old_len = Some(self.facts.len());
            return Ok(false);
        }
        let combined = if all.len() == 1 {
            all.pop().expect("len checked")
        } else {
            arrow::compute::concat_batches(&self.schema, &all).map_err(arrow_err)?
        };

        let fold_inputs = self.fold_input_indices();
        // A referenced probability moves with the child's folded value just
        // as the fold input does, so it is no part of the derivation's
        // identity either.
        let deriv_key_indices: Vec<usize> = self
            .all_column_indices
            .iter()
            .copied()
            .filter(|i| !fold_inputs.contains(i))
            .filter(|&i| {
                !self
                    .schema
                    .field(i)
                    .name()
                    .starts_with(crate::query::planner_locy_types::DERIV_ISPROB_COL_PREFIX)
            })
            .collect();

        // Rows before this index are existing facts; the rest are candidates.
        let facts_rows: usize = self.facts.iter().map(RecordBatch::num_rows).sum();
        // Keys come from the rounded view; the rows kept are exact.
        let combined_keys = float_key_view(std::slice::from_ref(&combined))
            .pop()
            .expect("one batch in, one out");
        let mut latest: HashMap<Vec<ScalarKey>, u32> = HashMap::new();
        for row in 0..combined.num_rows() {
            let key = extract_scalar_key(&combined_keys, &deriv_key_indices, row);
            let replaced = latest.insert(key, row as u32);
            // A derivation is evaluated once per iteration, so two candidates
            // sharing a derivation key must agree on their fold input. If they
            // differ, the key failed to tell two derivations apart and the
            // replace would silently drop one (issue #294). Checked in debug
            // builds, where every test and fuzz run exercises it.
            if cfg!(debug_assertions)
                && let Some(prev) = replaced
                && prev as usize >= facts_rows
                && extract_scalar_key(&combined_keys, &fold_inputs, prev as usize)
                    != extract_scalar_key(&combined_keys, &fold_inputs, row)
            {
                return Err(datafusion::error::DataFusionError::Internal(format!(
                    "rule '{}': two derivations of one iteration share a derivation key \
                     but differ in their fold input; the derivation discriminators do not \
                     identify them, and one would be dropped",
                    self.rule_name
                )));
            }
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
            let retained_keys = float_key_view(std::slice::from_ref(&retained))
                .pop()
                .expect("one batch in, one out");
            for row in 0..retained.num_rows() {
                let full = extract_scalar_key(&retained_keys, &self.all_column_indices, row);
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
        // The delta rows sit among the retained ones.
        self.old_len = None;
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
            return self.install_folded(Vec::new());
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
        // #261. The fixpoint's own `max_derived_bytes` budget covers facts and
        // deltas; these post-fixpoint materializations sit outside it.
        let mut reservation = operator_reservation("FixpointExec", 0, task_ctx);
        let folded_batches =
            collect_all_partitions(&plan, Arc::clone(task_ctx), &mut reservation).await?;
        if let Some(fv) = self.fold_view.as_mut()
            && fv.collect_ops
        {
            fv.last_ops = crate::query::executor::core::collect_plan_metrics(&plan);
        }
        let folded_out = match folded_batches.iter().find(|b| b.num_rows() > 0) {
            Some(b) => b.clone(),
            None => {
                // Nothing aggregated (all-empty input): leave the
                // representative rows as they stand.
                let folded =
                    RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)?;
                return self.install_folded(vec![folded]);
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
        // #265: REQUIRE is part of the rule's definition, so it applies to
        // every snapshot rather than once at the end. This is the whole
        // mechanism: the snapshot is what a same-stratum self-reference reads,
        // so a group excluded here cannot derive anything downstream of it.
        //
        // Safe to narrow the snapshot only because the compiler has already
        // proved the threshold one-way for a recursive rule
        // (`check_require_direction`); a predicate that could turn back off
        // would let a fact be withdrawn, which the change test below reads as
        // progress rather than oscillation.
        let (require, params) = self
            .fold_view
            .as_ref()
            .map(|fv| (fv.require.clone(), fv.params.clone()))
            .unwrap_or_default();
        let folded = if require.is_empty() {
            vec![folded]
        } else {
            apply_having_filter(vec![folded], &require, &self.schema, task_ctx, &params)?
        };
        self.install_folded(folded)
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
    ///
    /// # Errors
    ///
    /// Fails when the data types change after facts have been stored, since
    /// the stored facts can no longer be concatenated or deduplicated against
    /// rows of the new types.
    fn reconcile_schema(&mut self, actual_schema: &SchemaRef) -> DFResult<()> {
        let actual_schema = &nullable_discriminators(actual_schema);
        if self.schema.fields() == actual_schema.fields() {
            return Ok(());
        }
        // Two clauses of one rule can disagree on nothing but nullability —
        // a node bound by a scan is non-null, the same column bound by an IS
        // reference is nullable — and which clause's batch comes first
        // decided the schema. Widen to the union instead: the row encoding
        // depends only on the data types, so the dedup state stays valid.
        //
        // Resetting it here was a silent duplicate-fact bug (issue #301): the
        // fresh state no longer knew the facts already derived, so a
        // re-derivation in a later iteration was admitted as new. It stayed
        // hidden while every iteration re-ran every clause in the same
        // order, which always put the same clause's batch first.
        let same_types = self.schema.fields().len() == actual_schema.fields().len()
            && self
                .schema
                .fields()
                .iter()
                .zip(actual_schema.fields())
                .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type());
        if same_types {
            let widened: Vec<Arc<Field>> = self
                .schema
                .fields()
                .iter()
                .zip(actual_schema.fields())
                .map(|(a, b)| {
                    if a.is_nullable() || !b.is_nullable() {
                        Arc::clone(a)
                    } else {
                        Arc::clone(b)
                    }
                })
                .collect();
            self.schema = Arc::new(ArrowSchema::new_with_metadata(
                widened,
                self.schema.metadata().clone(),
            ));
            return Ok(());
        }
        if self.facts.iter().any(|b| b.num_rows() > 0) {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "Locy rule '{}': clause output types changed after facts were stored \
                 (stored {:?}, new {:?})",
                self.rule_name,
                self.schema.fields(),
                actual_schema.fields(),
            )));
        }
        tracing::debug!(
            rule = %self.rule_name,
            "Reconciling fixpoint schema from physical plan output",
        );
        self.schema = Arc::clone(actual_schema);
        // No facts are stored yet (checked above), so the fresh state misses
        // nothing.
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
        Ok(())
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
            self.old_len = Some(self.facts.len());
            // No candidates is an empty delta too: the accumulators did not
            // move, so they are stable from here. Without the snapshot, an
            // iteration whose clauses all derive nothing left `is_stable`
            // comparing against the snapshot taken *before* the last update,
            // and the fixpoint ran to its iteration limit (issue #301: hidden
            // while a non-recursive clause re-derived its rows every
            // iteration, which always took the empty-delta path below).
            if let Some(ref mut agg) = self.monotonic_agg {
                agg.snapshot();
            }
            return Ok(false);
        }

        // Reconcile schema from the first non-empty candidate batch.
        // The physical plan's output types are authoritative over the
        // planner's inferred types.
        if let Some(first) = candidates.iter().find(|b| b.num_rows() > 0) {
            self.reconcile_schema(&first.schema())?;
        }

        // Round floats for stable dedup
        // Compute delta: rows in candidates not already in facts. The dedup
        // compares a rounded key view; the rows stored stay exact.
        let delta = self.compute_delta(&candidates, task_ctx.as_ref()).await?;

        if delta.is_empty() || delta.iter().all(|b| b.num_rows() == 0) {
            self.delta.clear();
            self.old_len = Some(self.facts.len());
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
        self.old_len = Some(self.facts.len());
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
        for batch in &float_key_view(&self.facts) {
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(batch, &self.all_column_indices, row_idx);
                existing.insert(key);
            }
        }

        let mut delta_batches = Vec::new();
        let keys = float_key_view(candidates);
        for (batch, key_batch) in candidates.iter().zip(&keys) {
            if batch.num_rows() == 0 {
                continue;
            }
            // Filter to only new rows
            let mut keep = Vec::with_capacity(batch.num_rows());
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(key_batch, &self.all_column_indices, row_idx);
                keep.push(!existing.contains(&key));
            }

            // Also dedup within the candidate batch itself
            for (row_idx, kept) in keep.iter_mut().enumerate() {
                if *kept {
                    let key = extract_scalar_key(key_batch, &self.all_column_indices, row_idx);
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

    /// The facts the latest merge did not add: `facts \ delta` (P5).
    ///
    /// A non-linear clause's semi-naive pass reads these at the positions
    /// before its delta position. Every merge keeps whole rows unique, so
    /// removing the delta's rows by exact whole-row identity is the set
    /// difference.
    ///
    /// # Errors
    ///
    /// An Arrow failure filtering the facts.
    pub fn old_facts(&self) -> DFResult<Vec<RecordBatch>> {
        if let Some(n) = self.old_len {
            return Ok(self.facts[..n.min(self.facts.len())].to_vec());
        }
        let delta_rows: HashSet<Vec<ScalarKey>> = self
            .delta
            .iter()
            .flat_map(|b| {
                let all: Vec<usize> = (0..b.num_columns()).collect();
                (0..b.num_rows()).map(move |row| extract_scalar_key(b, &all, row))
            })
            .collect();
        let mut old = Vec::new();
        for batch in &self.facts {
            let all: Vec<usize> = (0..batch.num_columns()).collect();
            let in_delta: Vec<bool> = (0..batch.num_rows())
                .map(|row| delta_rows.contains(&extract_scalar_key(batch, &all, row)))
                .collect();
            old.extend(split_rows(batch, &in_delta)?.1);
        }
        Ok(old)
    }

    /// Consume self and return facts.
    pub fn into_facts(self) -> Vec<RecordBatch> {
        self.facts
    }

    /// Merge candidates using BEST BY semantics.
    ///
    /// Keeps only the best row per KEY group according to `sort_criteria`.
    /// Returns `true` if the best-per-KEY fact set actually changed (a
    /// genuinely better value was found or a new KEY appeared).
    ///
    /// This replaces `merge_delta` for rules with BEST BY, enabling convergence
    /// on cyclic graphs where dominated ALONG values would otherwise produce an
    /// unbounded stream of "new" full-row facts.
    ///
    /// The delta is the rows that are new for their KEY or strictly better
    /// (R2), not the whole pruned set: a self-reference fed an unchanged row
    /// would only re-derive candidates it derived when that row entered. On a
    /// tie the held row stays, so the delta never churns between equals; a
    /// candidate whose rounded criteria equal the held row's (a re-derivation,
    /// or a value that moved by less than the rounding) does not displace it
    /// either.
    ///
    /// # Cost
    ///
    /// O(candidates) plus the batches the merge rewrites, not O(facts): the
    /// held best row of every KEY is found through the `best_index` map
    /// rather than by re-sorting the facts with the candidates. A displaced
    /// row is filtered out of the one batch holding it, and the tail batches
    /// are merged binary-counter style (each row is re-copied O(log facts)
    /// times over the whole fixpoint), so the facts stay a short list of
    /// batches. Re-sorting everything on every merge made a BEST BY rule
    /// about 3x a plain one, and quadratic in the iteration count.
    ///
    /// # Errors
    ///
    /// An Arrow failure comparing, concatenating, filtering or taking rows,
    /// or an internal error when the held facts carry two rows for one KEY,
    /// which no BEST BY merge produces.
    pub fn merge_best_by(
        &mut self,
        candidates: Vec<RecordBatch>,
        sort_criteria: &[SortCriterion],
    ) -> DFResult<bool> {
        let candidates: Vec<RecordBatch> = candidates
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect();
        let Some(first) = candidates.first() else {
            self.delta.clear();
            self.old_len = Some(self.facts.len());
            return Ok(false);
        };

        // Reconcile schema from the first non-empty candidate batch.
        self.reconcile_schema(&first.schema())?;
        // A BEST BY rule never takes the whole-row dedup path, and the row
        // set it would describe is rewritten by every displacement below.
        // `None` sends a stray `merge_delta` to the legacy dedup, which reads
        // the facts themselves.
        self.row_dedup = None;
        let criteria = resolve_best_by_columns(&self.rule_name, &self.schema, sort_criteria)?;
        self.ensure_best_index(&criteria)?;

        let cand = arrow::compute::concat_batches(&self.schema, &candidates).map_err(arrow_err)?;
        let cand_keys = float_key_view(std::slice::from_ref(&cand))
            .pop()
            .expect("one batch in, one out");
        let rounded_criteria = |batch: &RecordBatch, row: usize| -> Vec<ScalarKey> {
            criteria
                .iter()
                .flat_map(|&(ci, _)| extract_scalar_key(batch, &[ci], row))
                .collect()
        };

        // The best candidate per KEY; the earliest on an exact tie.
        let cand_cmp = criteria_comparators(&cand, &cand, &criteria)?;
        let mut best: HashMap<Vec<ScalarKey>, usize> = HashMap::new();
        for row in 0..cand.num_rows() {
            let key = extract_scalar_key(&cand_keys, &self.key_column_indices, row);
            match best.entry(key) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(row);
                }
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    if lex_compare(&cand_cmp, row, *o.get()).is_lt() {
                        o.insert(row);
                    }
                }
            }
        }
        let mut winners: Vec<(usize, Vec<ScalarKey>)> =
            best.into_iter().map(|(key, row)| (row, key)).collect();
        winners.sort_unstable_by_key(|(row, _)| *row);

        // The held rows of the KEYs the candidates touch, gathered into one
        // batch of criteria columns so one comparator per criterion serves.
        let held_locs: Vec<(usize, usize)> = winners
            .iter()
            .filter_map(|(_, key)| self.best_index.get(key).map(|s| (s.batch, s.row)))
            .collect();
        let held_cols: Vec<arrow_array::ArrayRef> = criteria
            .iter()
            .map(|&(ci, _)| {
                let arrays: Vec<&dyn arrow_array::Array> =
                    self.facts.iter().map(|b| b.column(ci).as_ref()).collect();
                if held_locs.is_empty() {
                    Ok(arrow_array::new_empty_array(cand.column(ci).data_type()))
                } else {
                    arrow::compute::interleave(&arrays, &held_locs).map_err(arrow_err)
                }
            })
            .collect::<DFResult<_>>()?;
        let held_cmp: Vec<arrow::array::DynComparator> = criteria
            .iter()
            .zip(&held_cols)
            .map(|(&(ci, opts), held_col)| {
                let field = self.schema.field(ci);
                arrow::array::make_comparator(
                    order_column(cand.column(ci), field)?.as_ref(),
                    order_column(held_col, field)?.as_ref(),
                    opts,
                )
                .map_err(arrow_err)
            })
            .collect::<DFResult<_>>()?;

        let mut improved_rows: Vec<u32> = Vec::new();
        let mut displaced: HashMap<usize, Vec<usize>> = HashMap::new();
        // `held_locs` lists the held rows in `winners` order, so the n-th
        // winner with a held row compares against held row n.
        let mut held_pos = 0usize;
        for (row, key) in &winners {
            let row = *row;
            let Some(slot) = self.best_index.get(key) else {
                // A KEY seen for the first time.
                improved_rows.push(row as u32);
                continue;
            };
            let pos = held_pos;
            held_pos += 1;
            let strictly_better = lex_compare(&held_cmp, row, pos).is_lt();
            let moved = slot.criteria != rounded_criteria(&cand_keys, row);
            if strictly_better && moved {
                improved_rows.push(row as u32);
                displaced.entry(slot.batch).or_default().push(slot.row);
            }
        }

        tracing::debug!(
            rule = %self.rule_name,
            held_keys = self.best_index.len(),
            candidate_keys = winners.len(),
            improved = improved_rows.len(),
            displaced = displaced.values().map(Vec::len).sum::<usize>(),
            "BEST BY merge"
        );

        if improved_rows.is_empty() {
            self.delta.clear();
            self.old_len = Some(self.facts.len());
            return Ok(false);
        }

        // Drop the displaced rows from the batches that hold them.
        let mut rewritten: Vec<usize> = displaced.keys().copied().collect();
        rewritten.sort_unstable();
        for &b in &rewritten {
            let rows = &displaced[&b];
            let mut keep = vec![true; self.facts[b].num_rows()];
            for &r in rows {
                keep[r] = false;
            }
            self.facts[b] = arrow::compute::filter_record_batch(
                &self.facts[b],
                &arrow_array::BooleanArray::from(keep),
            )
            .map_err(arrow_err)?;
            self.index_best_batch(b, &criteria);
        }
        self.compact_best_tail(&criteria)?;

        // The held winners, then the delta, so `old_facts` is the leading
        // batches.
        let improved = take_rows(&cand, &self.schema, improved_rows)?;
        self.old_len = Some(self.facts.len());
        self.facts.push(improved.clone());
        self.index_best_batch(self.facts.len() - 1, &criteria);
        self.delta = vec![improved];
        self.facts_bytes = self.facts.iter().map(batch_byte_size).sum();
        Ok(true)
    }

    /// Makes [`Self::best_index`] describe the facts, rebuilding it when it
    /// does not hold one entry per fact row.
    ///
    /// Only [`Self::merge_best_by`] writes a BEST BY rule's facts, and it
    /// keeps the index in step, so the rebuild runs for the first merge of a
    /// state at most.
    fn ensure_best_index(&mut self, criteria: &[BestByColumn]) -> DFResult<()> {
        let rows: usize = self.facts.iter().map(RecordBatch::num_rows).sum();
        if self.best_index.len() == rows {
            return Ok(());
        }
        self.best_index.clear();
        for b in 0..self.facts.len() {
            self.index_best_batch(b, criteria);
        }
        if self.best_index.len() != rows {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "rule '{}': BEST BY facts hold {rows} rows for {} KEYs; a BEST BY \
                 merge keeps one row per KEY",
                self.rule_name,
                self.best_index.len()
            )));
        }
        Ok(())
    }

    /// Points [`Self::best_index`] at every row of fact batch `b`.
    fn index_best_batch(&mut self, b: usize, criteria: &[BestByColumn]) {
        let batch = &self.facts[b];
        let keys = float_key_view(std::slice::from_ref(batch))
            .pop()
            .expect("one batch in, one out");
        for row in 0..keys.num_rows() {
            let key = extract_scalar_key(&keys, &self.key_column_indices, row);
            let criteria = criteria
                .iter()
                .flat_map(|&(ci, _)| extract_scalar_key(&keys, &[ci], row))
                .collect();
            self.best_index.insert(
                key,
                BestSlot {
                    batch: b,
                    row,
                    criteria,
                },
            );
        }
    }

    /// Drops the fact batches a displacement emptied, then merges the
    /// trailing batches binary-counter style while the last holds at least
    /// half as many rows as the one before it, so the batch count stays
    /// logarithmic in the fact count and each row is re-copied O(log facts)
    /// times over the whole fixpoint.
    ///
    /// Only batches whose index changes are re-indexed.
    ///
    /// # Errors
    ///
    /// An Arrow failure concatenating batches.
    fn compact_best_tail(&mut self, criteria: &[BestByColumn]) -> DFResult<()> {
        if let Some(first_empty) = self.facts.iter().position(|b| b.num_rows() == 0) {
            self.facts.retain(|b| b.num_rows() > 0);
            for b in first_empty..self.facts.len() {
                self.index_best_batch(b, criteria);
            }
        }
        while let [.., prev, last] = self.facts.as_slice() {
            if last.num_rows() * 2 < prev.num_rows() {
                break;
            }
            let merged =
                arrow::compute::concat_batches(&self.schema, [prev, last]).map_err(arrow_err)?;
            self.facts.truncate(self.facts.len() - 2);
            self.facts.push(merged);
            self.index_best_batch(self.facts.len() - 1, criteria);
        }
        Ok(())
    }
}

/// Where a BEST BY rule's held row for one KEY sits in its facts, and that
/// row's criteria through the rounded key view.
#[derive(Debug, Clone)]
struct BestSlot {
    batch: usize,
    row: usize,
    criteria: Vec<ScalarKey>,
}

/// One comparator per BEST BY criterion between rows of `left` and `right`.
/// A BEST BY criterion resolved against a rule's facts: the column's
/// position and its sort options.
type BestByColumn = (usize, arrow::compute::SortOptions);

/// Resolves each BEST BY criterion by its column name in `schema`.
///
/// The criteria used to be applied by position, and a position past the
/// schema's end was silently dropped, so a criterion that did not line up
/// with the facts either ranked by the wrong column or by nothing at all.
///
/// # Errors
///
/// When `schema` holds no column of a criterion's name.
fn resolve_best_by_columns(
    rule_name: &str,
    schema: &arrow_schema::Schema,
    sort_criteria: &[SortCriterion],
) -> DFResult<Vec<BestByColumn>> {
    sort_criteria
        .iter()
        .map(|c| {
            let ci = schema.index_of(&c.column).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "rule '{rule_name}': BEST BY column '{}' is not among its facts' columns {:?}",
                    c.column,
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                ))
            })?;
            Ok((
                ci,
                arrow::compute::SortOptions {
                    descending: !c.ascending,
                    nulls_first: c.nulls_first,
                },
            ))
        })
        .collect()
}

fn criteria_comparators(
    left: &RecordBatch,
    right: &RecordBatch,
    criteria: &[(usize, arrow::compute::SortOptions)],
) -> DFResult<Vec<arrow::array::DynComparator>> {
    criteria
        .iter()
        .map(|&(ci, opts)| {
            // Cypher's order, not a CypherValue column's byte order.
            let field = left.schema_ref().field(ci).clone();
            arrow::array::make_comparator(
                order_column(left.column(ci), &field)?.as_ref(),
                order_column(right.column(ci), &field)?.as_ref(),
                opts,
            )
            .map_err(arrow_err)
        })
        .collect()
}

/// Lexicographic comparison of a left row and a right row under BEST BY
/// criterion comparators: `Less` when the left row ranks first.
fn lex_compare(
    comparators: &[arrow::array::DynComparator],
    left: usize,
    right: usize,
) -> std::cmp::Ordering {
    comparators
        .iter()
        .map(|cmp| cmp(left, right))
        .find(|o| o.is_ne())
        .unwrap_or(std::cmp::Ordering::Equal)
}

/// The rows `rows` of `batch`, under `schema`.
///
/// # Errors
///
/// An Arrow failure taking the rows or building the batch.
fn take_rows(batch: &RecordBatch, schema: &SchemaRef, rows: Vec<u32>) -> DFResult<RecordBatch> {
    let idx = arrow_array::UInt32Array::from(rows);
    let columns = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &idx, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(arrow_err)?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(arrow_err)
}

/// Splits `batch` by `mask`: the rows it marks, then the rest, each as zero
/// or one batch (the whole batch, uncopied, when one side takes every row).
///
/// # Errors
///
/// An Arrow failure filtering the batch.
fn split_rows(
    batch: &RecordBatch,
    mask: &[bool],
) -> DFResult<(Option<RecordBatch>, Option<RecordBatch>)> {
    let marked = mask.iter().filter(|m| **m).count();
    if marked == batch.num_rows() {
        return Ok(((marked > 0).then(|| batch.clone()), None));
    }
    if marked == 0 {
        return Ok((None, Some(batch.clone())));
    }
    let keep = arrow_array::BooleanArray::from(mask.to_vec());
    let rest = arrow::compute::not(&keep).map_err(arrow_err)?;
    let marked_rows = arrow::compute::filter_record_batch(batch, &keep).map_err(arrow_err)?;
    let rest_rows = arrow::compute::filter_record_batch(batch, &rest).map_err(arrow_err)?;
    Ok((Some(marked_rows), Some(rest_rows)))
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

/// A copy of `batches` for building dedup and change-detection *keys*: every
/// Float64 column rounded to an absolute 1e-12.
///
/// Values that differ only by floating-point noise from different evaluation
/// orders must compare equal, or the fixpoint never sees an empty delta; and a
/// value decaying toward 0 around a cycle (an `MPROD`, an `ALONG` product)
/// only converges because it eventually rounds to 0. That is a property of the
/// comparison, not of the data: the view is used for keys only, and the facts
/// keep their exact values. Rounding the stored values themselves turned every
/// magnitude below 1e-12 into 0 — an `ALONG` product of 1e-7 × 1e-7 was stored
/// as 0.0.
fn float_key_view(batches: &[RecordBatch]) -> Vec<RecordBatch> {
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

/// Rows of `candidates` not already in `existing`, and each only once, using
/// DataFusion's `HashJoinExec` (LeftAnti) with NULLs comparing equal.
///
/// Rows are compared through [`float_key_view`] and returned with their exact
/// values.
async fn arrow_left_anti_dedup(
    candidates: Vec<RecordBatch>,
    existing: &[RecordBatch],
    schema: &SchemaRef,
    task_ctx: &Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    // Rows are compared through the rounded key view (`float_key_view`) and
    // returned exact: keys pick the rows, the view is never stored.
    let candidates: Vec<RecordBatch> = candidates
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect();
    if candidates.is_empty() {
        return Ok(vec![]);
    }
    let exact = arrow::compute::concat_batches(schema, &candidates).map_err(arrow_err)?;
    let keys = float_key_view(std::slice::from_ref(&exact))
        .pop()
        .expect("one batch in, one out");

    // A single iteration may emit the same row twice: keep first occurrences.
    let converter = RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect(),
    )
    .map_err(arrow_err)?;
    let rows = converter
        .convert_columns(keys.columns())
        .map_err(arrow_err)?;
    let mut seen: HashSet<Box<[u8]>> = HashSet::new();
    let first: Vec<u32> = (0..keys.num_rows())
        .filter(|&i| seen.insert(rows.row(i).data().into()))
        .map(|i| i as u32)
        .collect();
    let take_exact = |indices: Vec<u32>| -> DFResult<Vec<RecordBatch>> {
        let idx = arrow_array::UInt32Array::from(indices);
        let cols = exact
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &idx, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(arrow_err)?;
        Ok(vec![
            RecordBatch::try_new(Arc::clone(schema), cols).map_err(arrow_err)?,
        ])
    };
    if existing.is_empty() || existing.iter().all(|b| b.num_rows() == 0) {
        return take_exact(first);
    }

    // Anti-join the first occurrences' keys against the existing facts' keys,
    // carrying each candidate's row index in a column the join does not use.
    const ROW: &str = "__locy_candidate_row";
    let first_idx = arrow_array::UInt32Array::from(first);
    let mut left_cols = keys
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &first_idx, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(arrow_err)?;
    left_cols.push(Arc::new(first_idx.clone()));
    let mut left_fields: Vec<Arc<arrow_schema::Field>> = schema.fields().iter().cloned().collect();
    left_fields.push(Arc::new(arrow_schema::Field::new(
        ROW,
        arrow_schema::DataType::UInt32,
        false,
    )));
    let left_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(left_fields));
    let left_batch =
        RecordBatch::try_new(Arc::clone(&left_schema), left_cols).map_err(arrow_err)?;

    let left: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[vec![left_batch]], Arc::clone(&left_schema), None)?;
    let right: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[float_key_view(existing)], Arc::clone(schema), None)?;

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
    let mut reservation = operator_reservation("FixpointExec", 0, task_ctx);
    let anti = collect_all_partitions(&join_arc, task_ctx.clone(), &mut reservation).await?;
    let mut survivors: Vec<u32> = anti
        .iter()
        .filter_map(|b| {
            b.column_by_name(ROW)?
                .as_any()
                .downcast_ref::<arrow_array::UInt32Array>()
                .map(|a| a.values().to_vec())
        })
        .flatten()
        .collect();
    survivors.sort_unstable();
    take_exact(survivors)
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
    /// For negated IS-refs: the anti-join key, from the clause's binding plan
    /// (see [`crate::query::planner_locy_types::LocyIsRef::negation_keys`]).
    /// Empty for non-negated IS-refs.
    pub anti_join_cols: Vec<crate::query::planner_locy_types::NegationKey>,
    /// Whether the target rule has a PROB column.
    pub target_has_prob: bool,
    /// Name of the PROB column in the target rule, if any.
    pub target_prob_col: Option<String>,
    /// For a positive reference to a PROB rule: the hidden columns carrying
    /// the referenced probability (and KEY hash) and how the clause reads it
    /// (see [`crate::query::planner_locy_types::PositiveProbRef`]).
    pub prob_ref: Option<crate::query::planner_locy_types::PositiveProbRef>,
    /// For a negated reference to a PROB rule from a rule that keeps
    /// lineage: the hidden column its complement fills with the facts it read
    /// (see [`crate::query::planner_locy_types::NegatedLineageRef`]).
    pub negated_lineage: Option<crate::query::planner_locy_types::NegatedLineageRef>,
    /// For a positive reference no lineage column names a fact for, when
    /// provenance is recorded: the transient columns naming it for EXPLAIN
    /// (see [`crate::query::planner_locy_types::SupportRef`]).
    pub support_ref: Option<crate::query::planner_locy_types::SupportRef>,
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
    /// Registry indices of the body's scans into its own stratum, one per
    /// distinct handle, in first-use order (P5). Empty for a clause that
    /// reads only the graph and lower strata; two or more only for a
    /// non-linear clause, whose handles are all exclusive.
    pub stratum_scans: Vec<usize>,
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
    /// Post-FOLD definitional threshold (`REQUIRE`, issue #265) — applied to
    /// every per-iteration snapshot, unlike `having`, which runs once after
    /// convergence.
    pub require: Vec<Expr>,
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
    per_rule_output: PerRuleOutput,
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
                    rule.require.clone(),
                    params.clone(),
                );
                if profile_collector.is_some() {
                    state.collect_fold_ops();
                }
            }
            state
        })
        .collect();

    // R5: negated references read lower strata, fixed for the whole loop, so
    // their banned sets / probability maps are built once and reused.
    let mut negation_cache = super::locy_complement::NegationIndexCache::default();

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
            let plan_mark = PlanningMark::take(graph_ctx.counters());
            let mut iter_ops: Vec<OperatorStats> = Vec::new();
            for clause in &rule.clauses {
                // Issue #301: a clause that reads nothing of this stratum —
                // no positive or negated reference to one of its rules — reads
                // only the graph and lower strata, which are fixed while the
                // fixpoint runs. Its candidates are the same every iteration,
                // and the first iteration already merged them: `merge_delta`
                // and `merge_best_by` keep existing facts and
                // `merge_fold_contributions` only ever replaces a contribution
                // by a re-derivation of it, so they stay in the facts. Running
                // it again re-scanned its whole input per iteration.
                //
                // An empty candidate set keeps `clause_candidates` aligned
                // with the clause indices provenance looks rows up by.
                let reads_stratum = clause.is_ref_bindings.iter().any(|b| b.is_self_ref);
                if iteration > 0 && !reads_stratum {
                    clause_candidates.push(Vec::new());
                    continue;
                }
                let clause_ctx = ClauseEvalCtx {
                    params: &params,
                    graph_ctx: &graph_ctx,
                    session_ctx: &session_ctx,
                    storage: &storage,
                    schema_info: &schema_info,
                    registry: &registry,
                    collect_ops: profile_collector.is_some(),
                };
                // P5: which view each of the clause's stratum scans reads.
                //
                // * None: the clause reads only fixed inputs (the graph, lower
                //   strata) and runs as is.
                // * One — a linear clause, self- or cross-reference alike: the
                //   referenced rule's delta. Every fact enters exactly one
                //   delta, and each rule merges once per iteration while this
                //   clause runs once per iteration in a fixed order, so the
                //   clause meets each delta exactly once (R3 mutual: a
                //   cross-reference used to read the whole relation).
                // * k >= 2 — a non-linear clause: k passes, pass i feeding
                //   scan i the delta, scans j < i the old facts and scans
                //   j > i the full facts (R3 non-linear). F_new^k \ F_old^k
                //   is exactly the disjoint union over i of
                //   F_old^{<i} x Δ_i x F_new^{>i}: a tuple of facts is joined
                //   in the pass of its first member from the latest delta, and
                //   in no other — so each derivation (and so each per-path or
                //   FOLD contribution row) is produced once, at the evaluation
                //   when its last member arrived, exactly as reading every
                //   position in full would first produce it.
                //
                // A non-linear pass whose delta scan is empty derives nothing
                // (every stratum reference is an inner join) and is skipped.
                let scans = &clause.stratum_scans;
                let mut batches: Vec<RecordBatch> = Vec::new();
                if scans.is_empty() {
                    batches = evaluate_clause_once(
                        clause,
                        rule.prob_column_name.as_deref(),
                        &clause_ctx,
                        &mut negation_cache,
                        &mut iter_ops,
                    )
                    .await?;
                } else {
                    for pass in 0..scans.len() {
                        let feeds: Vec<ScanFeed> = (0..scans.len())
                            .map(|j| match j.cmp(&pass) {
                                std::cmp::Ordering::Less => ScanFeed::Old,
                                std::cmp::Ordering::Equal => ScanFeed::Delta,
                                std::cmp::Ordering::Greater => ScanFeed::Full,
                            })
                            .collect();
                        // A linear clause runs even on an empty delta, as it
                        // always has (it costs one empty anchored lookup); a
                        // non-linear pass on one would still read its old and
                        // full scans, so it is skipped.
                        let skip_empty = scans.len() > 1;
                        if feed_stratum_scans(
                            &registry, &states, &rules, scans, &feeds, skip_empty,
                        )? {
                            batches.extend(
                                evaluate_clause_once(
                                    clause,
                                    rule.prob_column_name.as_deref(),
                                    &clause_ctx,
                                    &mut negation_cache,
                                    &mut iter_ops,
                                )
                                .await?,
                            );
                        }
                    }
                    // Back to the resting views negation and provenance read.
                    update_derived_scan_handles(&registry, &states, rule_idx, &rules);
                }

                // A PROB rule without a fold: its derivation rows, read
                // before the transient columns carrying them go. Provenance
                // reads them from `clause_candidates`; the facts never carry
                // them.
                if let Some(ref tracker) = derivation_tracker {
                    tracker.lineage().record_rows(rule, &batches)?;
                }
                clause_candidates.push(batches.clone());
                all_candidates.extend(super::locy_lineage::strip_transient_columns(batches)?);
            }

            // Merge candidates into facts.
            // For BEST BY rules, use a specialized merge that keeps only the
            // best row per KEY group, enabling convergence on cyclic graphs.
            let merge_started = Instant::now();
            let merge_op = if rule.has_best_by && !rule.best_by_criteria.is_empty() {
                "LocyBestByMerge"
            } else if states[rule_idx].has_fold_view() {
                "LocyFoldMerge"
            } else {
                "LocyFactDedup"
            };
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
            let merge_elapsed = merge_started.elapsed();
            if changed {
                any_changed = true;
                // Record provenance for newly derived facts when tracker is present.
                if let Some(ref tracker) = derivation_tracker {
                    record_provenance(
                        ProvenanceCtx {
                            tracker,
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
                    .await?;
                }
            }

            // Profiling: record this rule's per-iteration row (delta = net-new
            // facts merged this pass, plus the captured clause-body operators).
            if let Some(ref collector) = profile_collector {
                // The folded snapshot's own PRIORITY/FOLD pass is part of
                // this iteration's cost for this rule.
                iter_ops.extend(states[rule_idx].take_fold_ops());
                let delta_facts: usize = states[rule_idx]
                    .all_delta()
                    .iter()
                    .map(|b| b.num_rows())
                    .sum();
                // The merge is the row dedup (or BEST BY / FOLD merge): it runs
                // outside any physical plan, so it is reported as its own entry.
                iter_ops.push(synthetic_operator(merge_op, delta_facts, merge_elapsed));
                collector.record(
                    &rule.name,
                    iteration,
                    delta_facts,
                    rule_start.elapsed().as_secs_f64() * 1000.0,
                    iter_ops,
                    plan_mark.cost(graph_ctx.counters()),
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

    // Every rule's lineage is recorded before any is corrected: a rule's
    // formula reads the facts of the stratum's other rules too.
    let stratum_facts: Vec<Vec<RecordBatch>> =
        states.into_iter().map(FixpointState::into_facts).collect();
    if let Some(ref tracker) = derivation_tracker {
        for (rule, facts) in rules.iter().zip(&stratum_facts) {
            tracker.lineage().record_rule(rule, facts)?;
        }
    }
    let mut stratum_facts: Vec<Option<Vec<RecordBatch>>> =
        stratum_facts.into_iter().map(Some).collect();
    let mut processed_by_rule: Vec<Option<Vec<RecordBatch>>> = vec![None; rules.len()];
    // A rule without a fold first: nothing corrects its facts and its chain
    // reads no lineage, so it is finished before any rule of the stratum is
    // corrected, and a PROB rule's lineage — its published facts' rows — is
    // in place for the rules that read it.
    for (rule_idx, rule) in rules.iter().enumerate() {
        if !rule.fold_bindings.is_empty() {
            continue;
        }
        let Some(facts) = stratum_facts[rule_idx].take().filter(|f| !f.is_empty()) else {
            continue;
        };
        super::locy_lineage::warn_opaque_prob_reads(rule, &warnings_slot);
        let processed = apply_post_fixpoint_chain(
            facts,
            rule,
            &task_ctx,
            strict_probability_domain,
            probability_epsilon,
            semiring_kind,
            // See the call below.
            None,
            &params,
        )
        .await?;
        let processed = match derivation_tracker {
            Some(ref tracker) => tracker.lineage().publish_rows(
                rule,
                processed,
                &super::locy_lineage::ExactCtx {
                    exact: exact_probability,
                    max_bdd_variables,
                    semiring_kind,
                    warnings_slot: &warnings_slot,
                    approximate_slot: &approximate_slot,
                },
            )?,
            None => processed,
        };
        processed_by_rule[rule_idx] = Some(processed);
    }
    for (rule_idx, facts) in stratum_facts.into_iter().enumerate() {
        let rule = &rules[rule_idx];
        let processed = match (processed_by_rule[rule_idx].take(), facts) {
            (Some(processed), _) => processed,
            (None, Some(facts)) if !facts.is_empty() => {
                fold_rule_output(
                    facts,
                    rule,
                    FoldRuleCtx {
                        tracker: derivation_tracker.as_ref(),
                        semiring_kind,
                        exact_probability,
                        max_bdd_variables,
                        warnings_slot: &warnings_slot,
                        approximate_slot: &approximate_slot,
                        task_ctx: &task_ctx,
                        strict_probability_domain,
                        probability_epsilon,
                        params: &params,
                    },
                )
                .await?
            }
            _ => continue,
        };
        per_rule_output
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(rule.name.clone(), processed.clone());
        all_output.extend(processed);
    }

    // If no output, return empty batch with output schema
    if all_output.is_empty() {
        all_output.push(RecordBatch::new_empty(output_schema));
    }

    Ok(all_output)
}

/// What [`fold_rule_output`] needs from the fixpoint loop.
struct FoldRuleCtx<'a> {
    tracker: Option<&'a Arc<ProvenanceStore>>,
    semiring_kind: SemiringKind,
    exact_probability: bool,
    max_bdd_variables: usize,
    warnings_slot: &'a Arc<StdRwLock<Vec<RuntimeWarning>>>,
    approximate_slot: &'a Arc<StdRwLock<HashMap<String, Vec<String>>>>,
    task_ctx: &'a Arc<TaskContext>,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    params: &'a HashMap<String, Value>,
}

/// A converged rule's published facts: its pre-fold rows corrected by
/// shared-proof detection and exact inference, then its post-fixpoint chain.
///
/// # Errors
///
/// A lineage or chain failure.
async fn fold_rule_output(
    mut facts: Vec<RecordBatch>,
    rule: &FixpointRulePlan,
    ctx: FoldRuleCtx<'_>,
) -> DFResult<Vec<RecordBatch>> {
    super::locy_lineage::warn_opaque_prob_reads(rule, ctx.warnings_slot);

    // Shared-proof detection and, with `exact_probability`, exact
    // inference over the recorded lineage (`locy_lineage`). Under
    // `MaxMinProb`, `plus = max` is idempotent — shared proofs don't
    // double-count — so the warning is moot and the work is skipped.
    //
    // TODO(C0-stage2): `TopKProofs` could read `TopKTag` DNFs here once
    // `crates/uni-locy/src/top_k_proofs.rs` tags reach the runtime.
    if ctx.semiring_kind != SemiringKind::MaxMinProb
        && let Some(tracker) = ctx.tracker
    {
        let (corrected, _) = tracker.lineage().correct(
            rule,
            facts,
            &super::locy_lineage::ExactCtx {
                exact: ctx.exact_probability,
                max_bdd_variables: ctx.max_bdd_variables,
                semiring_kind: ctx.semiring_kind,
                warnings_slot: ctx.warnings_slot,
                approximate_slot: ctx.approximate_slot,
            },
        )?;
        facts = corrected;
    }

    apply_post_fixpoint_chain(
        facts,
        rule,
        ctx.task_ctx,
        ctx.strict_probability_domain,
        ctx.probability_epsilon,
        ctx.semiring_kind,
        // Recursive strata have no profile collector in scope here; the
        // non-recursive path in `locy_program.rs` is where these operators
        // become observable (#177).
        None,
        ctx.params,
    )
    .await
}

/// What [`evaluate_clause_once`] needs from the fixpoint loop.
struct ClauseEvalCtx<'a> {
    params: &'a HashMap<String, Value>,
    graph_ctx: &'a Arc<GraphExecutionContext>,
    session_ctx: &'a Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: &'a Arc<StorageManager>,
    schema_info: &'a Arc<UniSchema>,
    registry: &'a Arc<DerivedScanRegistry>,
    collect_ops: bool,
}

/// Runs one clause body against the scans as currently fed, then applies its
/// negations and folds the complement factors into the PROB column.
///
/// # Errors
///
/// The body's execution error, or a negation / complement failure.
async fn evaluate_clause_once(
    clause: &FixpointClausePlan,
    prob_column: Option<&str>,
    ctx: &ClauseEvalCtx<'_>,
    negation_cache: &mut super::locy_complement::NegationIndexCache,
    iter_ops: &mut Vec<OperatorStats>,
) -> DFResult<Vec<RecordBatch>> {
    // Phase B A4 follow-up: the planner inserts `LogicalPlan::LocyModelInvoke`
    // between the body and `LocyProject` when this clause has neural-model
    // invocations, so `execute_subplan` runs the invocation inline as part of
    // the body plan tree.
    let mut batches = execute_locy_clause_body(
        &clause.body_logical,
        ctx.params,
        ctx.graph_ctx,
        ctx.session_ctx,
        ctx.storage,
        ctx.schema_info,
        ctx.collect_ops,
        iter_ops,
    )
    .await?;
    // Apply negated IS-ref semantics: probabilistic complement or anti-join.
    batches = super::locy_complement::apply_negated_is_refs(
        batches,
        &clause.is_ref_bindings,
        ctx.registry,
        prob_column,
        Some(negation_cache),
        ctx.collect_ops.then_some(&mut *iter_ops),
    )?;
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
        batches = multiply_prob_factors(batches, prob_column, &complement_cols)?;
    }
    // Positive references to PROB rules: multiply in each referenced
    // probability the clause does not already read.
    batches = super::locy_lineage::apply_positive_prob_factors(batches, &clause.is_ref_bindings)?;

    // The anti-join has run; drop its hidden subject-`_vid` columns before
    // anything downstream can absorb them into fact identity.
    super::locy_complement::strip_isnot_vid_columns(batches)
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
/// tracker and the shared warnings slot. Bundled to keep
/// `record_provenance` / `record_provenance_nonrecursive`
/// under the too-many-arguments threshold.
pub(crate) struct ProvenanceCtx<'a> {
    pub tracker: &'a Arc<ProvenanceStore>,
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
) -> DFResult<()> {
    let tracker = prov.tracker;
    let warnings_slot = prov.warnings_slot;
    let classifier_registry = classifiers.registry;
    let classifier_cache = classifiers.cache;

    let weight_column = derivation_weight_column(rule);
    let mut topk_acc = TopKProofAccumulator::new();
    let attribution = clause_attribution(rule, clause_candidates)?;
    // The references only transient columns name: read from the candidates,
    // which the facts' rows were taken from.
    let transient = transient_support(rule, clause_candidates)?;

    for delta_batch in state.all_delta() {
        let (hash_indices, key_indices) = yield_positions_in(rule, &delta_batch.schema())?;
        let key_columns: Vec<arrow_array::ArrayRef> = key_indices
            .iter()
            .map(|&i| Arc::clone(delta_batch.column(i)))
            .collect();
        let resolver = super::locy_lineage::RefResolver::new(
            rule,
            delta_batch,
            super::locy_lineage::RefScope::Kept,
        )?;
        let derivation_indices =
            derivation_hash_indices(rule, &hash_indices, &delta_batch.schema());
        for row_idx in 0..delta_batch.num_rows() {
            let row_hash =
                provenance_fact_hash(&rule.name, delta_batch, &derivation_indices, row_idx);
            let fact_row = batch_row_to_value_map(delta_batch, row_idx);
            let clause_index =
                find_clause_for_row(rule, &attribution, delta_batch, &hash_indices, row_idx)?;

            let (mut support, fact_key) = row_provenance(&resolver, &key_columns, row_idx)?;
            if let Some(more) =
                transient.get(&extract_scalar_key(delta_batch, &hash_indices, row_idx))
            {
                support.extend(more.iter().cloned());
            }
            let proof_probability = compute_proof_probability(
                weight_column.is_some(),
                row_weight(&fact_row, weight_column),
                &support,
            );

            let entry = ProvenanceAnnotation {
                rule_name: rule.name.clone(),
                clause_index,
                fact_key,
                derivation: row_hash.clone(),
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
                let fact = top_k_fact_hash(&entry.rule_name, &entry.fact_key);
                topk_acc.accumulate(&entry, &fact);
                tracker.record_top_k(fact, entry, top_k_proofs);
            } else {
                tracker.record_derivation(row_hash, entry);
            }
        }
    }

    topk_acc.emit_warning_if_any(rule, top_k_proofs, warnings_slot);
    Ok(())
}

/// Phase C C0 Stage 2: collects per-row `Proof` tags during the
/// fixpoint row walk, then surfaces `TopKPruningCrossedDependency`
/// when post-walk top-K merging would drop a proof whose base RVs
/// overlap a retained one. The shared `BaseRv` interner is what
/// makes the overlap detectable — proofs grounded in the same
/// `base_fact_id` get the same `BaseRv`.
struct TopKProofAccumulator {
    /// Each fact's proofs, by derivation.
    per_fact: HashMap<Vec<u8>, Vec<(Vec<u8>, uni_locy::Proof)>>,
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

    /// Adds `entry` as a proof of the fact `fact` (its
    /// [`top_k_fact_hash`]), its referenced facts as its base variables. A
    /// derivation added again (refined by a later iteration) replaces its
    /// earlier proof.
    fn accumulate(&mut self, entry: &ProvenanceAnnotation, fact: &[u8]) {
        let mut base_rvs = uni_locy::BaseRvSet::empty();
        for term in &entry.support {
            let rv = *self
                .base_rv_interner
                .entry(top_k_fact_hash(&term.source_rule, &term.base_fact_id))
                .or_insert_with(|| {
                    let r = uni_locy::BaseRv(self.next_rv);
                    self.next_rv += 1;
                    r
                });
            base_rvs.insert(rv);
        }
        let proof = uni_locy::Proof {
            weight: entry.proof_probability.unwrap_or(0.0),
            base_rvs,
            neural_calls: Vec::new(),
        };
        let proofs = self.per_fact.entry(fact.to_vec()).or_default();
        match proofs.iter_mut().find(|(d, _)| *d == entry.derivation) {
            Some((_, existing)) => *existing = proof,
            None => proofs.push((entry.derivation.clone(), proof)),
        }
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
                let proofs: Vec<uni_locy::Proof> = proofs.iter().map(|(_, p)| p.clone()).collect();
                let (_kept, notice) =
                    uni_locy::merge_top_k_runtime(Vec::new(), proofs, top_k_proofs);
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

/// The pre-fold column holding a derivation's weight: the input of the
/// rule's `MNOR`/`MPROD` fold, or else its PROB column. `None` for a rule
/// without one.
fn derivation_weight_column(rule: &FixpointRulePlan) -> Option<&str> {
    match rule
        .fold_bindings
        .iter()
        .find(|fb| fb.aggregate.is_probability_aggregate())
    {
        Some(fold) => pre_fold_column_name(rule, fold.input_col_index),
        None => rule.prob_column_name.as_deref(),
    }
}

/// A derivation row's own weight, read from `column` of its pre-fold row.
/// `None` when the rule has no weight column or the row holds no number
/// there (a NULL input the fold skips).
fn row_weight(fact_row: &HashMap<String, Value>, column: Option<&str>) -> Option<f64> {
    match fact_row.get(column?)? {
        Value::Float(p) => Some(*p),
        Value::Int(p) => Some(*p as f64),
        _ => None,
    }
}

/// The facts each of an iteration's candidate rows references through
/// transient columns only (see
/// [`crate::query::planner_locy_types::LIN_COL_PREFIX`]), keyed by the row's
/// yield columns as [`clause_attribution`] keys it; the first candidate wins.
///
/// A recursive rule's facts are its candidates deduplicated, without those
/// columns, so a delta row's transient support is looked up here.
///
/// # Errors
///
/// Fails when a candidate batch lacks a yield or provenance column.
fn transient_support(
    rule: &FixpointRulePlan,
    clause_candidates: &[Vec<RecordBatch>],
) -> DFResult<HashMap<Vec<ScalarKey>, Vec<ProofTerm>>> {
    let mut support: HashMap<Vec<ScalarKey>, Vec<ProofTerm>> = HashMap::new();
    for batch in clause_candidates
        .iter()
        .flatten()
        .filter(|b| b.num_rows() > 0)
    {
        let resolver = super::locy_lineage::RefResolver::new(
            rule,
            batch,
            super::locy_lineage::RefScope::Transient,
        )?;
        if resolver.is_empty() {
            continue;
        }
        let (indices, _) = yield_positions_in(rule, &batch.schema())?;
        for row in 0..batch.num_rows() {
            let key = extract_scalar_key(batch, &indices, row);
            if support.contains_key(&key) {
                continue;
            }
            let refs = resolver.row(row)?;
            support.insert(
                key,
                refs.positive
                    .into_iter()
                    .chain(refs.support)
                    .map(proof_term)
                    .collect(),
            );
        }
    }
    Ok(support)
}

/// A referenced fact as a proof term.
fn proof_term(r: super::locy_lineage::RefTerm) -> ProofTerm {
    ProofTerm {
        source_rule: r.fact.0,
        base_fact_id: r.fact.1.to_vec(),
        prob: r.prob,
    }
}

/// The provenance of row `row` of a batch of `rule`'s pre-fold rows: the
/// facts it positively references (its support), and its own KEY hash, both
/// as lineage names facts.
///
/// The support is read through the one reference resolution,
/// [`super::locy_lineage::RefResolver`], so EXPLAIN shows, and `TopKProofs`
/// and exact inference evaluate, the same references. A reference no
/// lineage column names (to a rule without a PROB column, or from a rule
/// that keeps no lineage) is support too when provenance is recorded, read
/// from its transient column ([`crate::query::planner_locy_types::SupportRef`]);
/// a fact of a rule without a PROB column is certain, of probability 1.
fn row_provenance(
    resolver: &super::locy_lineage::RefResolver<'_>,
    key_columns: &[arrow_array::ArrayRef],
    row: usize,
) -> DFResult<(Vec<ProofTerm>, Vec<u8>)> {
    let support = if resolver.is_empty() {
        Vec::new()
    } else {
        let refs = resolver.row(row)?;
        refs.positive
            .into_iter()
            .chain(refs.support)
            .map(proof_term)
            .collect()
    };
    let key = super::locy_row_hash::row_hash(key_columns, row)?.to_vec();
    Ok((support, key))
}

// ---------------------------------------------------------------------------
// Provenance fact identity
// ---------------------------------------------------------------------------

/// Build a byte key that uniquely identifies a row across all columns.
pub(crate) fn fact_hash_key(batch: &RecordBatch, all_indices: &[usize], row_idx: usize) -> Vec<u8> {
    format!("{:?}", extract_scalar_key(batch, all_indices, row_idx)).into_bytes()
}

/// The key a derived fact's provenance is recorded and looked up under: its
/// rule's name and its row.
///
/// Two rules can derive the same tuple. Keyed by the row alone, the provenance
/// store kept whichever rule recorded it first, so EXPLAIN of the other rule
/// silently lost that fact, and a support term could point at the wrong rule's
/// derivation.
pub(crate) fn provenance_fact_hash(
    rule_name: &str,
    batch: &RecordBatch,
    all_indices: &[usize],
    row_idx: usize,
) -> Vec<u8> {
    let mut key = rule_name.as_bytes().to_vec();
    key.push(0);
    key.extend(fact_hash_key(batch, all_indices, row_idx));
    key
}

/// The columns a derivation's provenance is recorded under.
///
/// A rule with derivation discriminators (a recursive `FOLD` / `ALONG` rule,
/// issue #159) is recorded per derivation: its yield columns and hidden
/// derivation columns (`__deriv_*`, which the plan's yield schema may already
/// list), but neither a fold input nor a referenced probability. A fold input is a derivation's value, which changes
/// as the fixpoint refines the facts it references, not its identity. Any
/// other rule is recorded per yield row, its identity.
///
/// Keyed by the yield columns alone, two derivations of one fact through
/// different referenced facts that happened to carry the same value were one
/// provenance entry, and EXPLAIN silently showed one of them.
fn derivation_hash_indices(
    rule: &FixpointRulePlan,
    yield_indices: &[usize],
    schema: &SchemaRef,
) -> Vec<usize> {
    use crate::query::planner_locy_types::{
        DERIV_ISPROB_COL_PREFIX, FOLD_DISCRIMINATOR_COL_PREFIX,
    };
    let discriminated = schema
        .index_of(&format!("{FOLD_DISCRIMINATOR_COL_PREFIX}clause"))
        .is_ok();
    if !discriminated {
        return yield_indices.to_vec();
    }
    let fold_inputs: HashSet<usize> = rule
        .fold_bindings
        .iter()
        .filter_map(|fb| yield_indices.get(fb.input_col_index).copied())
        .collect();
    let extra = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, f)| {
            f.name().starts_with(FOLD_DISCRIMINATOR_COL_PREFIX) && !yield_indices.contains(i)
        })
        .map(|(i, _)| i);
    yield_indices
        .iter()
        .copied()
        .chain(extra)
        .filter(|i| {
            !fold_inputs.contains(i)
                && !schema.field(*i).name().starts_with(DERIV_ISPROB_COL_PREFIX)
        })
        .collect()
}

/// The name yield column `pos` of `rule` carries in a pre-fold fact batch.
///
/// A FOLD output is carried under its fold variable until the fold runs, and
/// a YIELD may rename it (`FOLD risk = MNOR(e.p) YIELD risk AS risk_out`), so
/// the pre-fold name is the binding's input column name, not the yield name.
pub(crate) fn pre_fold_column_name(rule: &FixpointRulePlan, pos: usize) -> Option<&str> {
    rule.fold_bindings
        .iter()
        .find(|fb| fb.input_col_index == pos)
        .and_then(|fb| fb.input_col_name.as_deref())
        .or_else(|| {
            rule.yield_schema
                .fields()
                .get(pos)
                .map(|f| f.name().as_str())
        })
}

/// Where `rule`'s yield columns sit in the fact batch `schema`, resolved by
/// name.
///
/// Returns every yield column in yield order, then the KEY columns. A fact's
/// provenance hash covers its yield columns in yield order, so recording and
/// lookup agree whatever column order a batch carries; positional indices
/// into a batch that orders its columns differently grouped by the wrong KEY
/// and hashed a row the store had recorded under another key. A column is
/// looked up under its pre-fold name ([`pre_fold_column_name`]), then its
/// yield name.
///
/// # Errors
///
/// Fails when a yield column is missing from `schema`. Hashing or grouping on
/// a subset would silently merge or split facts.
pub(crate) fn yield_positions_in(
    rule: &FixpointRulePlan,
    schema: &SchemaRef,
) -> DFResult<(Vec<usize>, Vec<usize>)> {
    let yield_fields = rule.yield_schema.fields();
    let all = yield_fields
        .iter()
        .enumerate()
        .map(|(pos, field)| {
            pre_fold_column_name(rule, pos)
                .and_then(|name| schema.index_of(name).ok())
                .or_else(|| schema.index_of(field.name()).ok())
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "rule '{}': yield column '{}' missing from the fact batch \
                         (columns: {:?})",
                        rule.name,
                        field.name(),
                        schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                    ))
                })
        })
        .collect::<DFResult<Vec<usize>>>()?;
    let key = rule
        .key_column_indices
        .iter()
        .map(|&i| {
            all.get(i).copied().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "rule '{}': KEY column index {i} out of range for {} yield columns",
                    rule.name,
                    yield_fields.len()
                ))
            })
        })
        .collect::<DFResult<Vec<usize>>>()?;
    Ok((all, key))
}

/// Record provenance for non-recursive strata.
///
/// Non-recursive rules are evaluated in a single pass (no fixpoint loop), so
/// the regular `record_provenance` path is never hit. This records a
/// `ProvenanceAnnotation` for every fact produced by each clause, for EXPLAIN
/// and top-k proofs. Shared-proof detection reads the lineage store instead
/// (`locy_lineage`).
pub(crate) async fn record_provenance_nonrecursive(
    rule: &FixpointRulePlan,
    tagged_clause_facts: &[(usize, Vec<RecordBatch>)],
    tracker: &Arc<ProvenanceStore>,
    warnings_slot: &Arc<StdRwLock<Vec<RuntimeWarning>>>,
    top_k_proofs: usize,
    classifiers: ClassifierRefs<'_>,
) -> DFResult<()> {
    let classifier_registry = classifiers.registry;
    let classifier_cache = classifiers.cache;

    let weight_column = derivation_weight_column(rule);
    let mut topk_acc = TopKProofAccumulator::new();

    // Record provenance for each clause's facts.
    for (clause_index, batches) in tagged_clause_facts {
        for batch in batches {
            let (hash_indices, key_indices) = yield_positions_in(rule, &batch.schema())?;
            let key_columns: Vec<arrow_array::ArrayRef> = key_indices
                .iter()
                .map(|&i| Arc::clone(batch.column(i)))
                .collect();
            let resolver = super::locy_lineage::RefResolver::new(
                rule,
                batch,
                super::locy_lineage::RefScope::All,
            )?;
            let derivation_indices = derivation_hash_indices(rule, &hash_indices, &batch.schema());
            for row_idx in 0..batch.num_rows() {
                let row_hash =
                    provenance_fact_hash(&rule.name, batch, &derivation_indices, row_idx);
                let fact_row = batch_row_to_value_map(batch, row_idx);

                let (support, fact_key) = row_provenance(&resolver, &key_columns, row_idx)?;
                let proof_probability = compute_proof_probability(
                    weight_column.is_some(),
                    row_weight(&fact_row, weight_column),
                    &support,
                );

                let entry = ProvenanceAnnotation {
                    rule_name: rule.name.clone(),
                    clause_index: *clause_index,
                    fact_key,
                    derivation: row_hash.clone(),
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
                    let fact = top_k_fact_hash(&entry.rule_name, &entry.fact_key);
                    topk_acc.accumulate(&entry, &fact);
                    tracker.record_top_k(fact, entry, top_k_proofs);
                } else {
                    tracker.record(row_hash, entry);
                }
            }
        }
    }

    topk_acc.emit_warning_if_any(rule, top_k_proofs, warnings_slot);
    Ok(())
}

/// The clause that produced each of this iteration's candidate rows, keyed by
/// the row's yield columns resolved by name; the first clause wins.
///
/// Built once per [`record_provenance`] call, so attributing a delta row is a
/// lookup rather than a scan of every candidate.
///
/// # Errors
///
/// Fails when a candidate batch lacks one of the rule's yield columns (see
/// [`yield_positions_in`]).
fn clause_attribution(
    rule: &FixpointRulePlan,
    clause_candidates: &[Vec<RecordBatch>],
) -> DFResult<HashMap<Vec<ScalarKey>, usize>> {
    let mut attribution: HashMap<Vec<ScalarKey>, usize> = HashMap::new();
    for (clause_idx, batches) in clause_candidates.iter().enumerate() {
        for batch in batches.iter().filter(|b| b.num_rows() > 0) {
            let (indices, _) = yield_positions_in(rule, &batch.schema())?;
            for row in 0..batch.num_rows() {
                attribution
                    .entry(extract_scalar_key(batch, &indices, row))
                    .or_insert(clause_idx);
            }
        }
    }
    Ok(attribution)
}

/// The clause that produced row `row_idx` of `delta_batch`, whose yield
/// columns sit at `hash_indices` (resolved by name).
///
/// Columns were compared by position, and a candidate batch whose column
/// count differed from the yield schema's was skipped; a row no clause
/// matched was attributed to clause 0. A delta row is always one of the
/// iteration's candidate rows (every merge takes its delta rows from the
/// candidates, exactly), so a miss means the columns did not line up, and
/// it fails rather than recording another clause's support and ALONG values.
///
/// # Errors
///
/// Fails when no clause's candidates hold the row.
fn find_clause_for_row(
    rule: &FixpointRulePlan,
    attribution: &HashMap<Vec<ScalarKey>, usize>,
    delta_batch: &RecordBatch,
    hash_indices: &[usize],
    row_idx: usize,
) -> DFResult<usize> {
    attribution
        .get(&extract_scalar_key(delta_batch, hash_indices, row_idx))
        .copied()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!(
                "rule '{}': a derived fact matches none of the iteration's clause \
                 candidates; its clause cannot be attributed",
                rule.name
            ))
        })
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
            // Documented exception (#233 class): this builds a plain
            // `HashMap` and has no error channel. Logged, then degraded.
            let hint = uni_store::storage::arrow_convert::type_hint_for_field(field);
            let val = match arrow_to_value(col.as_ref(), row_idx, hint) {
                Ok(v) => v.canonical_entity(),
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        column = %field.name(),
                        "fixpoint row: column failed to decode"
                    );
                    uni_common::Value::Null
                }
            };
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
            // The planner resolved the name to its exact body column (a node
            // to its `._vid`), so it is looked up as written.
            Expr::Variable(name) => Ok(FeatureValueSrc::Col(lookup_col(name.clone())?)),
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
                // The planner resolved the subject to its exact vid column.
                let subject_col = schema.index_of(v).map_err(|_| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "{name}: subject column '{v}' not in body batch schema"
                    ))
                })?;
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
                // The planner resolved the subject to its exact vid column.
                let subject_col = schema.index_of(v).map_err(|_| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "{name}: subject column '{v}' not in body batch schema"
                    ))
                })?;
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
        )
        .with_property_manager(graph_algo.property_manager.clone());
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
                // The planner resolved the subject to its exact vid column.
                let Ok(col_idx) = schema.index_of(subject_var) else {
                    continue;
                };
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

        // Walk the adjacency for every subject first, then read the property
        // for all neighbours in one batch rather than one round-trip per
        // neighbour (#220). The walk is in-memory; only the property read
        // reaches storage, so hoisting it is what removes the round-trips.
        let mut neighbors_by_subject: Vec<(u64, Vec<uni_common::core::id::Vid>)> = Vec::new();
        let mut all_neighbors: Vec<uni_common::core::id::Vid> = Vec::new();
        for subject_vid in subject_vids {
            let mut neighbors: Vec<uni_common::core::id::Vid> = Vec::new();
            for dir in direction.store_directions() {
                neighbors.extend(
                    adj.get_neighbors(
                        uni_common::core::id::Vid::from(subject_vid),
                        edge_type_id,
                        *dir,
                    )
                    .into_iter()
                    .map(|(neighbor_vid, _eid)| neighbor_vid),
                );
            }
            all_neighbors.extend(neighbors.iter().copied());
            neighbors_by_subject.push((subject_vid, neighbors));
        }
        all_neighbors.sort_unstable();
        all_neighbors.dedup();

        let props_by_vid = property_manager
            .get_batch_vertex_props(&all_neighbors, &[prop_name.as_str()], query_ctx.as_ref())
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "neighbor-aggregator: failed to read property '{prop_name}' \
                     across {} neighbours: {e}",
                    all_neighbors.len()
                ))
            })?;

        for (subject_vid, neighbors) in neighbors_by_subject {
            let mut values: Vec<f64> = Vec::with_capacity(neighbors.len());
            for neighbor_vid in neighbors {
                // A neighbour absent from the map, or carrying no value for
                // this property, contributes nothing — the same outcome the
                // per-neighbour read reached via a null that failed `as_f64`.
                let Some(val) = props_by_vid
                    .get(&neighbor_vid)
                    .and_then(|props| props.get(prop_name.as_str()))
                else {
                    continue;
                };
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
    // `Utf8View`. Falls through to `Null` without this arm, silently.
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::StringViewArray>() {
        return uni_common::Value::String(a.value(row_idx).to_string());
    }
    if let Some(b) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
        let bytes = b.value(row_idx);
        if bytes.is_empty() {
            return uni_common::Value::Null;
        }
        // Documented exception (#233 class): `extract_common_value` is
        // infallible with several callers inside the fixpoint loop, so a
        // corrupt cell has no error channel here. Logged, then degraded.
        return match uni_common::cypher_value_codec::decode(bytes) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, "Locy value: CypherValue failed to decode");
                uni_common::Value::Null
            }
        };
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
    // `Utf8View`. Falls through to `FeatureValue::Null` without this arm.
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::StringViewArray>() {
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
        // Documented exception (#233 class), as `extract_common_value` above:
        // `extract_feature_value` is infallible by signature.
        let v = match uni_common::cypher_value_codec::decode(bytes) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, "Locy feature: CypherValue failed to decode");
                uni_common::Value::Null
            }
        };
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

/// Writes every scan's resting view before a rule's clauses are evaluated.
///
/// A linear self-reference rests on the delta, everything else on the full
/// relation. This is what a scan holds whenever no clause is running, and so
/// what negation, provenance and the post-fixpoint chain read through the
/// registry. While a clause runs, [`feed_stratum_scans`] overrides the scans
/// that clause reads with their semi-naive feed and restores them after.
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
        let linear_self = is_self && !rules[current_rule_idx].non_linear;
        let data = if entry.view == DerivedScanView::Folded && linear_self {
            // Issue #162: this scan reads one row per KEY carrying that KEY's
            // folded value, not one row per contribution. Issue #301: a
            // linear self-reference reads only the rows of that snapshot that
            // are new or changed since the previous one, which derives
            // exactly what reading the whole snapshot would.
            //
            // Each iteration merges the clause's candidates into the
            // contributions by *replacement* on the derivation key — every
            // column but the fold inputs (`merge_fold_contributions`) — and a
            // linear clause's output is a union over the snapshot rows it
            // reads, one referenced row per output row. So the candidates the
            // whole snapshot S_t would yield split into those of the changed
            // rows Δ_t = S_t \ S_{t-1}, which this scan feeds, and those of the
            // unchanged rows, which were derived from that same row in an
            // earlier iteration (when it entered or last changed) and are
            // still in the contributions: re-deriving them would replace each
            // with an identical row. A changed row is re-fed, so every
            // contribution that depends on the referenced value is recomputed
            // and replaces its predecessor, exactly as before.
            //
            // A KEY that leaves the snapshot is fed by neither: today it is
            // simply no longer read, its earlier contributions stay in the
            // facts (merging only replaces, never removes), and that is what
            // happens here too. A KEY that comes back is new relative to the
            // snapshot it left, so it is re-fed. Row *order* within the facts
            // can differ from full feeding (an unchanged contribution keeps
            // its slot instead of moving to the end), which only decides the
            // representative of a KEY group's internal `__deriv_*` columns:
            // a FOLD rule yields nothing but its KEYs and folds (#293).
            states[source_idx].folded_delta().to_vec()
        } else if entry.view == DerivedScanView::Folded {
            // A cross-reference, or a non-linear self-reference (Δ×Δ would miss
            // Δ×F_old): the whole snapshot.
            states[source_idx].folded_facts().to_vec()
        } else if linear_self {
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

/// What a semi-naive pass feeds one stratum scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanFeed {
    /// The facts the latest merge added.
    Delta,
    /// The facts before the latest merge: full minus delta.
    Old,
    /// Every fact.
    Full,
}

/// One view of the relation a stratum scan reads, from its rule's state.
fn stratum_scan_view(
    state: &FixpointState,
    view: DerivedScanView,
    feed: ScanFeed,
) -> DFResult<Vec<RecordBatch>> {
    Ok(match (view, feed) {
        (DerivedScanView::Folded, ScanFeed::Delta) => state.folded_delta().to_vec(),
        (DerivedScanView::Folded, ScanFeed::Old) => state.folded_old().to_vec(),
        (DerivedScanView::Folded, ScanFeed::Full) => state.folded_facts().to_vec(),
        (DerivedScanView::Contributions, ScanFeed::Delta) => state.all_delta().to_vec(),
        (DerivedScanView::Contributions, ScanFeed::Old) => state.old_facts()?,
        (DerivedScanView::Contributions, ScanFeed::Full) => state.all_facts().to_vec(),
    })
}

/// Feeds a clause's stratum scans for one semi-naive pass (P5, R3).
///
/// `feeds[i]` is what scan `scans[i]` reads. With `skip_empty_delta`,
/// returns `false`, writing nothing, when the scan fed [`ScanFeed::Delta`]
/// would read no rows: every stratum reference is an inner join, so the pass
/// derives nothing.
fn feed_stratum_scans(
    registry: &DerivedScanRegistry,
    states: &[FixpointState],
    rules: &[FixpointRulePlan],
    scans: &[usize],
    feeds: &[ScanFeed],
    skip_empty_delta: bool,
) -> DFResult<bool> {
    let mut writes: Vec<(&DerivedScanEntry, Vec<RecordBatch>)> = Vec::with_capacity(scans.len());
    for (&scan, &feed) in scans.iter().zip(feeds) {
        let Some(entry) = registry.get(scan) else {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "clause reads stratum scan {scan}, which the registry does not hold"
            )));
        };
        let Some(source) = rules.iter().position(|r| r.name == entry.rule_name) else {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "stratum scan {scan} reads rule '{}', which is not in this stratum",
                entry.rule_name
            )));
        };
        let data = stratum_scan_view(&states[source], entry.view, feed)?;
        let rows: usize = data.iter().map(RecordBatch::num_rows).sum();
        if skip_empty_delta && feed == ScanFeed::Delta && rows == 0 {
            return Ok(false);
        }
        writes.push((entry, data));
    }
    for (entry, data) in writes {
        let data = if data.iter().all(|b| b.num_rows() == 0) {
            vec![RecordBatch::new_empty(Arc::clone(&entry.schema))]
        } else {
            data
        };
        *entry.data.write() = data;
    }
    Ok(true)
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
    /// Rows served per execution, so a profile reports how much of a derived
    /// relation each fixpoint iteration read (issue #301). Without it every
    /// derived scan reported 0 rows, hiding whether a recursive clause read
    /// the delta or the whole relation.
    metrics: ExecutionPlanMetricsSet,
}

impl DerivedScanExec {
    pub fn new(data: Arc<RwLock<Vec<RecordBatch>>>, schema: SchemaRef) -> Self {
        let properties = compute_plan_properties(Arc::clone(&schema));
        Self {
            data,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
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
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
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
        // The batches are already resident, so the rows served are known up
        // front; nothing is computed, so no elapsed time is recorded.
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        baseline.record_output(batches.iter().map(RecordBatch::num_rows).sum());
        baseline.done();
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

/// Wrap the query's parameters in a [`TranslationContext`] so `$name`
/// references in an expression compiled at execution time resolve to literals.
///
/// `None` when the program bound no parameters, which keeps the existing
/// zero-allocation path for the overwhelmingly common case.
fn translation_context_for_params(
    params: &HashMap<String, Value>,
) -> Option<crate::query::df_expr::TranslationContext> {
    if params.is_empty() {
        return None;
    }
    Some(crate::query::df_expr::TranslationContext {
        parameters: params.clone(),
        ..Default::default()
    })
}

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
    params: &HashMap<String, Value>,
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

    // `$param` references reach here unresolved — the post-FOLD WHERE is
    // compiled at execution time rather than by the DataFusion planner that
    // splices the query's parameters into the pre-FOLD body. Passing `None`
    // here was issue #273: the same `$thr` worked before FOLD and failed after
    // it with "Unresolved parameter". Built only when the rule has params to
    // bind, so a parameterless program allocates nothing.
    let translation_ctx = translation_context_for_params(params);

    // Cypher Expr → DataFusion DfExpr → type-coerced DfExpr → PhysicalExpr.
    //
    // Type coercion is needed because FOLD aggregates produce Float64 (SUM,
    // AVG), Int64 (COUNT) or a Cypher value, and literal comparisons like
    // `total >= 100` may mix those columns with Int64 literals.
    let physical_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = having_exprs
        .iter()
        .map(|expr| {
            let df_expr = crate::query::df_expr::cypher_expr_to_df(expr, translation_ctx.as_ref())
                .map_err(|e| {
                    datafusion::common::DataFusionError::Internal(format!(
                        "HAVING expression conversion: {e}"
                    ))
                })?;
            // A Cypher-value FOLD output (`MIN`, `COLLECT`, or `SUM` / `AVG`
            // over a schemaless or duration property) compares through the
            // Cypher comparison UDFs, as before a FOLD: DataFusion's own
            // coercion has no `LargeBinary > Int64`.
            let df_expr = crate::query::df_expr::apply_type_coercion(&df_expr, &df_schema)
                .map_err(|e| {
                    datafusion::common::DataFusionError::Internal(format!(
                        "HAVING type coercion: {e}"
                    ))
                })?;
            let df_expr = resolve_registered_udfs(df_expr, task_ctx.scalar_functions())?;

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
    params: &HashMap<String, Value>,
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
    // See `apply_having_filter` — a post-FOLD YIELD expression resolves its
    // `$param` references the same way, and for the same reason (issue #273).
    let translation_ctx = translation_context_for_params(params);

    // Compile each projection expression to a PhysicalExpr against the post-fold
    // schema. Type coercion (via a Projection plan) resolves mixed-type
    // arithmetic such as `total / count` (Float64 / Int64), mirroring HAVING.
    let physical_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = specs
        .iter()
        .map(|(name, expr)| {
            let df_expr = crate::query::df_expr::cypher_expr_to_df(expr, translation_ctx.as_ref())
                .map_err(|e| {
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
    post_ops: Option<&mut Vec<OperatorStats>>,
    // The query's bound parameters, for the `$name` references a post-FOLD
    // REQUIRE / WHERE / YIELD expression compiles at execution time (#273).
    params: &HashMap<String, Value>,
) -> DFResult<Vec<RecordBatch>> {
    let out = apply_post_fixpoint_chain_inner(
        facts,
        rule,
        task_ctx,
        strict_probability_domain,
        probability_epsilon,
        semiring_kind,
        post_ops,
        params,
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
    // Sink for the operators this chain builds and runs (#177). `PriorityExec`,
    // `FoldExec` and the post-fixpoint `BestByExec` are assembled here rather
    // than lowered into a clause body, so they sit in no plan the profile
    // collector otherwise walks — which is why the registry recorded them
    // unobservable however often they ran.
    mut post_ops: Option<&mut Vec<OperatorStats>>,
    params: &HashMap<String, Value>,
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
        Arc::new(FoldExec::new_with_semiring(
            current,
            key_column_indices.clone(),
            rule.fold_bindings.clone(),
            strict_probability_domain,
            probability_epsilon,
            semiring_kind,
        ))
    } else {
        current
    };

    // Apply REQUIRE (post-FOLD definitional threshold, issue #265).
    //
    // Runs here as well as on every per-iteration snapshot, and both are
    // needed for different reasons. The snapshot pass is what stops an
    // excluded group deriving anything downstream of it; this pass is what
    // keeps the group itself out of the answer. Without it a row that fails
    // the threshold still reaches the output, because the final answer is
    // built from the rule's contribution facts rather than from the snapshot.
    //
    // Before HAVING, mirroring the source order: REQUIRE says what the rule
    // derives, HAVING then filters what is shown of it.
    let current: Arc<dyn ExecutionPlan> = if !rule.require.is_empty() {
        let mut reservation = operator_reservation("FixpointExec", 0, task_ctx);
        let batches =
            collect_all_partitions(&current, Arc::clone(task_ctx), &mut reservation).await?;
        record_post_ops(&current, post_ops.as_deref_mut());
        let filtered =
            apply_having_filter(batches, &rule.require, &current.schema(), task_ctx, params)?;
        if filtered.is_empty() {
            return Ok(filtered);
        }
        MemorySourceConfig::try_new_exec(&[filtered], Arc::clone(&current.schema()), None)?
    } else {
        current
    };

    // Apply HAVING (post-FOLD WHERE filter)
    let current: Arc<dyn ExecutionPlan> = if !rule.having.is_empty() {
        let mut reservation = operator_reservation("FixpointExec", 0, task_ctx);
        let batches =
            collect_all_partitions(&current, Arc::clone(task_ctx), &mut reservation).await?;
        record_post_ops(&current, post_ops.as_deref_mut());
        let filtered =
            apply_having_filter(batches, &rule.having, &current.schema(), task_ctx, params)?;
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
        let mut reservation = operator_reservation("FixpointExec", 0, task_ctx);
        let batches =
            collect_all_partitions(&current, Arc::clone(task_ctx), &mut reservation).await?;
        record_post_ops(&current, post_ops.as_deref_mut());
        return apply_post_fold_projection(
            batches,
            &rule.yield_projection,
            &current.schema(),
            task_ctx,
            params,
        );
    }

    let mut reservation = operator_reservation("FixpointExec", 0, task_ctx);
    let out = collect_all_partitions(&current, Arc::clone(task_ctx), &mut reservation).await;
    record_post_ops(&current, post_ops);
    out
}

/// Append `plan`'s per-operator metrics to `sink`, if one was supplied.
///
/// Called after each execution point in the post-fixpoint chain rather than
/// once at the end, because each stage replaces `current` with an in-memory
/// source over its own output — so the tree holding `PriorityExec` and
/// `FoldExec` is gone by the time the next stage runs, and only the stage that
/// actually executed it can report it.
fn record_post_ops(plan: &Arc<dyn ExecutionPlan>, sink: Option<&mut Vec<OperatorStats>>) {
    if let Some(sink) = sink {
        sink.extend(crate::query::executor::core::collect_plan_metrics(plan));
    }
}

// ---------------------------------------------------------------------------
// FixpointExec — DataFusion ExecutionPlan
// ---------------------------------------------------------------------------

/// DataFusion `ExecutionPlan` that drives semi-naive fixpoint iteration.
///
/// Has no physical children: clause bodies are re-planned from logical plans
/// on each iteration (same pattern as `RecursiveCTEExec` and `GraphApplyExec`).
/// Each rule's converged output, keyed by rule name, filled when the fixpoint
/// finishes.
///
/// The operator's own stream concatenates every rule of the stratum, which is
/// what the `ExecutionPlan` contract can carry. Mutually recursive rules have
/// different facts, and a consumer that stored the concatenation under each
/// rule's name gave every rule the others' rows too.
pub type PerRuleOutput = Arc<StdRwLock<HashMap<String, Vec<RecordBatch>>>>;

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
    /// Each rule's converged output, keyed by rule name. See
    /// [`Self::per_rule_output`].
    per_rule_output: PerRuleOutput,
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
            per_rule_output: PerRuleOutput::default(),
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
    /// The slot this operator fills with each rule's own output once the
    /// fixpoint converges. Take it before executing; see [`PerRuleOutput`].
    pub fn per_rule_output(&self) -> PerRuleOutput {
        Arc::clone(&self.per_rule_output)
    }

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
                            stratum_scans: c.stratum_scans.clone(),
                        })
                        .collect(),
                    yield_schema: Arc::clone(&r.yield_schema),
                    key_column_indices: r.key_column_indices.clone(),
                    priority: r.priority,
                    has_fold: r.has_fold,
                    fold_bindings: r.fold_bindings.clone(),
                    require: r.require.clone(),
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
        let per_rule_output = Arc::clone(&self.per_rule_output);
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
                per_rule_output,
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

/// `schema` with every derivation-discriminator column marked nullable.
///
/// A clause that does not bind a discriminating variable projects NULL for it
/// (see `FOLD_DISCRIMINATOR_COL_PREFIX`), so these columns are nullable by
/// construction. The scan's own field is not: a relationship's `_eid` is
/// declared non-nullable, and adopting the first batch's schema verbatim would
/// then reject a sibling clause's NULL-padded batch (issue #294).
fn nullable_discriminators(schema: &SchemaRef) -> SchemaRef {
    use crate::query::planner_locy_types::FOLD_DISCRIMINATOR_COL_PREFIX;
    if !schema
        .fields()
        .iter()
        .any(|f| f.name().starts_with(FOLD_DISCRIMINATOR_COL_PREFIX) && !f.is_nullable())
    {
        return Arc::clone(schema);
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            if f.name().starts_with(FOLD_DISCRIMINATOR_COL_PREFIX) {
                Arc::new(f.as_ref().clone().with_nullable(true))
            } else {
                Arc::clone(f)
            }
        })
        .collect();
    Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

/// Replaces each placeholder Cypher UDF in `expr` (`dummy_udf_expr`) with the
/// one registered under its name.
///
/// # Errors
///
/// When the expression tree cannot be rewritten.
fn resolve_registered_udfs(
    expr: datafusion::logical_expr::Expr,
    registered: &HashMap<String, Arc<datafusion::logical_expr::ScalarUDF>>,
) -> DFResult<datafusion::logical_expr::Expr> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::Expr as DfExpr;

    expr.transform_up(|node| {
        if let DfExpr::ScalarFunction(func) = &node
            && let Some(udf) = registered.get(func.func.name())
        {
            return Ok(Transformed::yes(DfExpr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::clone(udf),
                    args: func.args.clone(),
                },
            )));
        }
        Ok(Transformed::no(node))
    })
    .map(|t| t.data)
}

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

    fn criterion(col_index: usize, column: &str) -> SortCriterion {
        SortCriterion {
            col_index,
            column: column.to_string(),
            ascending: true,
            nulls_first: false,
        }
    }

    /// The BEST BY merge ranks by the criterion's column name, not its
    /// position, and refuses a criterion its facts do not hold. It applied
    /// the position, and dropped a position past the schema's end, so the
    /// rule kept whichever row arrived first.
    #[test]
    fn best_by_merge_resolves_criteria_by_name_and_fails_closed() {
        let mut state = FixpointState::new(
            "test".into(),
            test_schema(),
            vec![0],
            1_000_000,
            None,
            false,
        );
        // The position names the KEY column; the name names `value`.
        let by_name = [criterion(0, "value")];
        state
            .merge_best_by(vec![make_batch(&["a", "a"], &[5, 2])], &by_name)
            .unwrap();
        state
            .merge_best_by(vec![make_batch(&["a"], &[1])], &by_name)
            .unwrap();
        let held: Vec<i64> = state
            .facts
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(held, vec![1]);

        let mut state = FixpointState::new(
            "test".into(),
            test_schema(),
            vec![0],
            1_000_000,
            None,
            false,
        );
        let err = state
            .merge_best_by(vec![make_batch(&["a"], &[1])], &[criterion(7, "missing")])
            .expect_err("a criterion the facts do not hold");
        assert!(
            err.to_string().contains("BEST BY column 'missing'"),
            "{err}"
        );
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
    fn test_float_key_view_near_duplicates() {
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

        let rounded = float_key_view(&[batch]);
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
            exclusive: false,
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
            exclusive: false,
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::clone(&schema),
        });
        reg.add(DerivedScanEntry {
            scan_index: 1,
            rule_name: "r2".into(),
            is_self_ref: false,
            view: DerivedScanView::Contributions,
            exclusive: false,
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::clone(&schema),
        });
        reg.add(DerivedScanEntry {
            scan_index: 2,
            rule_name: "r1".into(),
            is_self_ref: false,
            view: DerivedScanView::Contributions,
            exclusive: false,
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
        // The API layer classifies this refusal by the marker, so it must
        // survive into the rendered error (#289).
        assert!(
            err.contains(crate::query::df_graph::locy_errors::DERIVED_BYTES_LIMIT_MARKER),
            "Error was: {}",
            err
        );
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

    // --- Exact WMC over batches whose column order differs from the yield ---

    /// A rule `r` with yield `(a KEY, p)` and `FOLD p = MNOR(p)`.
    fn mnor_rule_plan() -> FixpointRulePlan {
        FixpointRulePlan {
            name: "r".into(),
            clauses: Vec::new(),
            yield_schema: Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("p", DataType::Float64, true),
            ])),
            key_column_indices: vec![0],
            priority: None,
            has_fold: true,
            fold_bindings: vec![FoldBinding {
                output_name: "p".into(),
                name: smol_str::SmolStr::new_static("MNOR"),
                aggregate: Arc::new(uni_plugin_builtin::locy_aggregates::MnorAgg),
                input_col_index: 1,
                input_col_name: Some("p".into()),
                distinct: false,
            }],
            require: Vec::new(),
            having: Vec::new(),
            has_best_by: false,
            best_by_criteria: Vec::new(),
            has_priority: false,
            deterministic: true,
            prob_column_name: Some("p".into()),
            non_linear: false,
            yield_projection: Vec::new(),
        }
    }

    /// A batch of rule `r`'s rows `(a, p)` with the columns in `names` order.
    fn r_batch(names: [&str; 2], a_first: bool, rows: &[(&str, f64)]) -> RecordBatch {
        let a: arrow_array::ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        ));
        let p: arrow_array::ArrayRef = Arc::new(Float64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        ));
        let (fields, cols) = if a_first {
            (
                vec![
                    Field::new(names[0], DataType::Utf8, true),
                    Field::new(names[1], DataType::Float64, true),
                ],
                vec![a, p],
            )
        } else {
            (
                vec![
                    Field::new(names[1], DataType::Float64, true),
                    Field::new(names[0], DataType::Utf8, true),
                ],
                vec![p, a],
            )
        };
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    /// `find_clause_for_row` matches a fact against each clause's candidates
    /// by column NAME.
    ///
    /// Clause 1's candidates carry `(p, a)`. Compared by position they never
    /// matched, and the fact was attributed to clause 0 — whose support and
    /// ALONG values provenance then recorded.
    #[test]
    fn find_clause_for_row_matches_candidates_by_name() {
        let rule = mnor_rule_plan();
        let delta = r_batch(["a", "p"], true, &[("x", 0.5)]);
        let clause_candidates = vec![
            vec![r_batch(["a", "p"], true, &[("y", 0.3)])],
            vec![r_batch(["a", "p"], false, &[("x", 0.5)])],
        ];
        let attribution = clause_attribution(&rule, &clause_candidates).unwrap();
        let (hash_indices, _) = yield_positions_in(&rule, &delta.schema()).unwrap();
        assert_eq!(
            find_clause_for_row(&rule, &attribution, &delta, &hash_indices, 0).unwrap(),
            1,
            "the fact is clause 1's candidate"
        );
    }

    /// A clause candidate batch missing a yield column is an error rather than
    /// a skipped batch, and a fact no clause produced is not given clause 0.
    #[test]
    fn find_clause_for_row_fails_closed() {
        let rule = mnor_rule_plan();
        let renamed = vec![vec![r_batch(["a", "p_alias"], true, &[("x", 0.5)])]];
        let err = clause_attribution(&rule, &renamed)
            .expect_err("a candidate batch lacking yield column 'p' is an error");
        assert!(err.to_string().contains("'p'"), "{err}");

        let delta = r_batch(["a", "p"], true, &[("x", 0.5)]);
        let other = vec![vec![r_batch(["a", "p"], true, &[("y", 0.3)])]];
        let attribution = clause_attribution(&rule, &other).unwrap();
        let (hash_indices, _) = yield_positions_in(&rule, &delta.schema()).unwrap();
        assert!(
            find_clause_for_row(&rule, &attribution, &delta, &hash_indices, 0).is_err(),
            "a fact absent from every clause's candidates has no clause"
        );
    }
}
