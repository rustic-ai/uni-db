// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! FOLD operator for Locy.
//!
//! `FoldExec` applies fold (lattice-join) semantics: for each group of rows sharing
//! the same KEY columns, it reduces non-key columns via their declared fold functions.

use crate::query::df_graph::common::{
    ScalarKey, arrow_err, collect_accounted, compute_plan_properties, concat_accounted,
    extract_scalar_key, operator_reservation,
};
use arrow_array::builder::Float64Builder;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::scalar::ScalarValue;
use futures::Stream;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use uni_locy::SemiringKind;
use uni_plugin::traits::locy::{FoldContext, FoldSemiring, LocyAggregate};

use super::locy_explain::ProofTerm;

/// Plugin-aware resolution of an aggregate name to a [`uni_plugin::traits::locy::LocyAggregate`].
///
/// Looks up `name` (case-folded) against the supplied [`uni_plugin::PluginRegistry`]
/// under the reserved built-in namespace. Returns `None` if no plugin claims
/// the aggregate.
///
/// Accepts legacy grammar aliases: the bare (`SUM`/`MAX`/`MIN`/`COUNT`) and
/// `M`-prefixed (`MSUM`/`MMAX`/`MMIN`/`MCOUNT`) forms, plus `NOR`→`MNOR`,
/// `PROD`→`MPROD`. `COUNTALL` (the zero-argument `COUNT()`/`MCOUNT()` form)
/// resolves to its own dedicated aggregate, distinct from null-skipping
/// `COUNT`.
///
/// # Examples
///
/// ```ignore
/// use uni_query::query::df_graph::locy_fold::{default_locy_plugin_registry, resolve_locy_aggregate};
/// let r = default_locy_plugin_registry();
/// let agg = resolve_locy_aggregate(&r, "SUM");
/// assert!(agg.is_some());
/// ```
/// Returns the monotonicity verdict for an aggregate name resolved through
/// the supplied [`uni_plugin::PluginRegistry`].
///
/// `Some(true)` — registered monotone aggregate (`Semilattice.monotone_join`
/// is `true`), sound in recursive Locy strata. `Some(false)` — registered
/// but non-monotone, must be rejected in recursion. `None` — unregistered.
///
/// Aliases (`MSUM`/`MMAX`/`MMIN`/`MCOUNT`/`NOR`/`PROD`/`COUNTALL`) are
/// canonicalized by [`resolve_locy_aggregate`] before lookup.
#[must_use]
pub fn is_monotonic_aggregate(registry: &uni_plugin::PluginRegistry, name: &str) -> Option<bool> {
    resolve_locy_aggregate(registry, name).map(|e| e.aggregate.semilattice().monotone_join)
}

/// The registry's verdict, falling back to the built-in `M*` contract.
///
/// This is the composition every consumer should use, and it exists because
/// registry-alone is wrong in both directions.
///
/// A bare `PluginRegistry::new()` — a plugin-less embedding, or a host that has
/// removed a plugin — answers `None` for `MSUM`, `MMAX` and the rest, so a
/// registry-only oracle would start rejecting recursive programs that use the
/// built-in lattice folds. Falling back to
/// [`uni_locy::default_monotonicity_oracle`] keeps that user-asserted contract
/// working regardless of registry state.
///
/// The compile-time oracle and the planner's guard must both go through this
/// function. When they disagree — as they did while the compiler consulted the
/// default and the planner consulted the registry — a program can compile
/// cleanly and then fail at plan time with an unstructured error, or the
/// reverse.
#[must_use]
pub fn locy_monotonicity_verdict(
    registry: &uni_plugin::PluginRegistry,
    name: &str,
) -> Option<bool> {
    is_monotonic_aggregate(registry, name).or_else(|| uni_locy::default_monotonicity_oracle(name))
}

#[must_use]
pub fn resolve_locy_aggregate(
    registry: &uni_plugin::PluginRegistry,
    name: &str,
) -> Option<std::sync::Arc<uni_plugin::registry::LocyAggregateEntry>> {
    let canonical = match name.to_uppercase().as_str() {
        "MMAX" => "MAX".to_owned(),
        "MMIN" => "MIN".to_owned(),
        "MCOUNT" => "COUNT".to_owned(),
        "NOR" => "MNOR".to_owned(),
        "PROD" => "MPROD".to_owned(),
        other => other.to_owned(),
    };
    let qname = uni_plugin::QName::builtin(canonical);
    // M8.6 dual-consult: session-local first (if a Session has set the
    // task-local via `scoped_with_session_plugin_registry`), then fall
    // back to the caller-supplied (instance) registry. This makes
    // session-scoped Locy aggregates visible without changing any
    // caller of `resolve_locy_aggregate`.
    if let Some(session_pr) = crate::current_session_plugin_registry()
        && let Some(entry) = session_pr.locy_aggregate(&qname)
    {
        return Some(entry);
    }
    if let Some(entry) = registry.locy_aggregate(&qname) {
        return Some(entry);
    }
    // P0.2: plugin-namespaced custom aggregate (e.g. `myplugin.MYAGG`).
    // The `builtin` lookup above only matches the reserved builtin
    // namespace, so a third-party aggregate registered under its own
    // plugin id was previously stored-but-unreachable. Resolve it the
    // same convention-agnostic way scalars/procedures do: try each
    // (namespace, local) split of the RAW name — not the builtin
    // alias-canonicalized form — against session-local then instance
    // registry, first hit wins.
    for cand in uni_plugin::QName::candidate_splits(name) {
        if let Some(session_pr) = crate::current_session_plugin_registry()
            && let Some(entry) = session_pr.locy_aggregate(&cand)
        {
            return Some(entry);
        }
        if let Some(entry) = registry.locy_aggregate(&cand) {
            return Some(entry);
        }
    }
    None
}

/// Returns a process-wide [`uni_plugin::PluginRegistry`] pre-populated with
/// the built-in Locy aggregates from `uni-plugin-builtin`.
///
/// Used by [`crate::query::df_planner::HybridPhysicalPlanner`] as a default
/// when the host has not supplied its own registry. Lazily initialized
/// at first call and shared thereafter.
///
/// # Panics
///
/// Panics only on framework-internal invariants: capability gating, qname
/// validation, or duplicate commit. The built-in registration set is fixed
/// and cannot trigger any of these at runtime.
#[must_use]
pub fn default_locy_plugin_registry() -> Arc<uni_plugin::PluginRegistry> {
    static REGISTRY: OnceLock<Arc<uni_plugin::PluginRegistry>> = OnceLock::new();
    Arc::clone(REGISTRY.get_or_init(|| {
        let registry = uni_plugin::PluginRegistry::new();
        let plugin_id = uni_plugin::PluginId::new(uni_plugin::QName::BUILTIN_NS);
        let caps = uni_plugin::CapabilitySet::from_iter_of([uni_plugin::Capability::LocyAggregate]);
        let mut r = uni_plugin::PluginRegistrar::new(plugin_id, &caps, &registry);
        uni_plugin_builtin::locy_aggregates::register_into(&mut r)
            .expect("built-in locy aggregates register");
        r.commit_to_registry().expect("commit built-in aggregates");
        Arc::new(registry)
    }))
}

/// A single FOLD binding: aggregate an input column into an output column.
///
/// Carries the canonical aggregate name (used as a sentinel for `COUNTALL`
/// and for batch-path dispatch in [`FoldExec`]) alongside the resolved
/// [`LocyAggregate`] trait object (used by the fixpoint runtime). The name
/// is one of: `SUM`, `MIN`, `MAX`, `COUNT`, `COUNTALL`, `AVG`, `COLLECT`,
/// `MNOR`, `MPROD`.
#[derive(Debug, Clone)]
pub struct FoldBinding {
    pub output_name: String,
    /// Canonical uppercase aggregate name.
    pub name: SmolStr,
    /// Resolved aggregate trait object (registry-backed).
    pub aggregate: Arc<dyn LocyAggregate>,
    pub input_col_index: usize,
    /// Column name for name-based resolution (more robust than positional index).
    /// `None` for `COUNTALL` which has no input column.
    pub input_col_name: Option<String>,
    /// `AGG(DISTINCT x)`: each group folds one row per distinct input value.
    ///
    /// The compiler admits it only in non-recursive rules, where the operator
    /// sees a group's whole bag at once.
    pub distinct: bool,
}

/// DataFusion `ExecutionPlan` that applies FOLD semantics.
///
/// Groups rows by KEY columns and computes aggregates (SUM, MAX, MIN, COUNT, AVG, COLLECT)
/// for each fold binding. Output schema is KEY columns + fold output columns.
#[derive(Debug)]
pub struct FoldExec {
    input: Arc<dyn ExecutionPlan>,
    key_indices: Vec<usize>,
    fold_bindings: Vec<FoldBinding>,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    /// Active probability semiring. `AddMultProb` (the default) preserves
    /// byte-identical Phase 1/2 noisy-OR / product behavior. `MaxMinProb`
    /// (Viterbi) is opt-in and produces fuzzy-truth values; callers up the
    /// stack also emit `FuzzyNotProbabilistic` on PROB-bearing rules.
    semiring_kind: SemiringKind,
    /// Phase D D-C0: under `SemiringKind::TopKProofs`, MNOR aggregates use
    /// DNF inclusion-exclusion over the row's support chain (lifted from
    /// the provenance tracker) rather than independence-mode noisy-OR.
    /// `None` for non-TopK semirings — keeps the byte-identical `f64`
    /// path for AddMultProb / MaxMinProb.
    provenance_tracker: Option<Arc<super::locy_explain::ProvenanceStore>>,
    /// Phase D D-C0: top-k retention used for proof pruning. Mirrors the
    /// fixpoint-loop config; passed through so per-group `TopKTag`
    /// merges respect the same K as the in-loop accumulator.
    top_k_proofs_k: usize,
    /// Pre-computed map of body-row content hash → IS-ref support
    /// (`Vec<ProofTerm>`) for use by `topk_dnf_disjunction`. Populated
    /// in `apply_post_fixpoint_chain` *before* this `FoldExec` is
    /// built, because at FOLD time the current rule's own facts are
    /// not yet recorded in the `ProvenanceStore` (which is keyed by
    /// post-YIELD hashes anyway). `None` for non-TopK semirings and
    /// for legacy callers.
    body_support_map: Option<Arc<BodySupportMap>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl FoldExec {
    /// Create a new `FoldExec`.
    ///
    /// # Arguments
    /// * `input` - Child execution plan
    /// * `key_indices` - Indices of KEY columns for grouping
    /// * `fold_bindings` - Aggregate bindings (output name, kind, input col index)
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        fold_bindings: Vec<FoldBinding>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
    ) -> Self {
        Self::new_with_semiring(
            input,
            key_indices,
            fold_bindings,
            strict_probability_domain,
            probability_epsilon,
            SemiringKind::AddMultProb,
        )
    }

    /// Variant taking an explicit [`SemiringKind`]. Existing callers can
    /// keep using [`FoldExec::new`] (which defaults to `AddMultProb`); the
    /// fixpoint planner uses this form to thread the configured semiring
    /// from [`uni_locy::LocyConfig::resolve`].
    pub fn new_with_semiring(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        fold_bindings: Vec<FoldBinding>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        semiring_kind: SemiringKind,
    ) -> Self {
        Self::new_with_topk(
            input,
            key_indices,
            fold_bindings,
            strict_probability_domain,
            probability_epsilon,
            semiring_kind,
            None,
            0,
            None,
        )
    }

    /// Phase D D-C0: variant that threads the provenance tracker and
    /// `top_k_proofs` config so MNOR under `SemiringKind::TopKProofs`
    /// can resolve each row's IS-ref support chain into a `Proof` and
    /// aggregate via DNF inclusion-exclusion.
    ///
    /// `body_support_map` (Phase D D-C0 follow-up) is a pre-computed
    /// `body_row_hash → Vec<ProofTerm>` map, populated by
    /// `apply_post_fixpoint_chain` for TopKProofs rules. The tracker
    /// alone is insufficient — its entries are keyed by post-YIELD
    /// row hashes and are only populated after FOLD runs; the pre-fold
    /// body rows seen here would never hit. The map closes that gap.
    #[expect(
        clippy::too_many_arguments,
        reason = "constructor mirrors the FoldExec fields; a builder would add no clarity"
    )]
    pub fn new_with_topk(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        fold_bindings: Vec<FoldBinding>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        semiring_kind: SemiringKind,
        provenance_tracker: Option<Arc<super::locy_explain::ProvenanceStore>>,
        top_k_proofs_k: usize,
        body_support_map: Option<Arc<BodySupportMap>>,
    ) -> Self {
        let input_schema = input.schema();
        let schema = Self::build_output_schema(&input_schema, &key_indices, &fold_bindings);
        let properties = compute_plan_properties(Arc::clone(&schema));

        Self {
            input,
            key_indices,
            fold_bindings,
            strict_probability_domain,
            probability_epsilon,
            semiring_kind,
            provenance_tracker,
            top_k_proofs_k,
            body_support_map,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn build_output_schema(
        input_schema: &SchemaRef,
        key_indices: &[usize],
        fold_bindings: &[FoldBinding],
    ) -> SchemaRef {
        let mut fields = Vec::new();

        // Key columns preserve original types
        for &ki in key_indices {
            fields.push(Arc::new(input_schema.field(ki).clone()));
        }

        // Fold output columns — type derived from the aggregate trait. The
        // input column type is resolved first (name-then-index) so
        // type-preserving aggregates (`MIN`/`MAX`) can return it.
        for binding in fold_bindings {
            let idx = binding
                .input_col_name
                .as_ref()
                .and_then(|name| input_schema.index_of(name).ok())
                .unwrap_or(binding.input_col_index);
            let input_type = if idx < input_schema.fields().len() {
                input_schema.field(idx).data_type().clone()
            } else {
                DataType::Float64
            };
            let output_type = binding.aggregate.output_type_for_input(&input_type);
            fields.push(Arc::new(Field::new(
                &binding.output_name,
                output_type,
                true,
            )));
        }

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for FoldExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FoldExec: key_indices={:?}, bindings={:?}",
            self.key_indices, self.fold_bindings
        )
    }
}

impl ExecutionPlan for FoldExec {
    fn name(&self) -> &str {
        "FoldExec"
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
                "FoldExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new_with_topk(
            Arc::clone(&children[0]),
            self.key_indices.clone(),
            self.fold_bindings.clone(),
            self.strict_probability_domain,
            self.probability_epsilon,
            self.semiring_kind,
            self.provenance_tracker.as_ref().map(Arc::clone),
            self.top_k_proofs_k,
            self.body_support_map.as_ref().map(Arc::clone),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let key_indices = self.key_indices.clone();
        let fold_bindings = self.fold_bindings.clone();
        let strict = self.strict_probability_domain;
        let epsilon = self.probability_epsilon;
        let semiring_kind = self.semiring_kind;
        let _provenance_tracker = self.provenance_tracker.as_ref().map(Arc::clone);
        let top_k_proofs_k = self.top_k_proofs_k;
        let body_support_map = self.body_support_map.as_ref().map(Arc::clone);
        let output_schema = Arc::clone(&self.schema);
        let input_schema = self.input.schema();

        // #261. This is an eager barrier: it holds the whole input, and
        // then the input and its concatenation at once. Nothing counted
        // either, so the pool could neither refuse nor report it.
        let mut reservation = operator_reservation("FoldExec", partition, &context);
        let fut = async move {
            // #261: charged as the barrier fills, not after it is full.
            let batches = collect_accounted(input_stream, &mut reservation).await?;

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(output_schema));
            }

            // Use the actual batch schema (may differ from pre-computed input_schema
            // after schema reconciliation in schemaless mode).
            let actual_schema = batches
                .first()
                .map(|b| b.schema())
                .unwrap_or(input_schema.clone());
            let batch = concat_accounted(&actual_schema, batches, &mut reservation)?;

            if batch.num_rows() == 0 {
                return Ok(RecordBatch::new_empty(output_schema));
            }

            // Group by key columns → row indices, preserving insertion order
            let mut groups: HashMap<Vec<ScalarKey>, Vec<usize>> = HashMap::new();
            let mut ordered_keys: Vec<Vec<ScalarKey>> = Vec::new();
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(&batch, &key_indices, row_idx);
                let entry = groups.entry(key.clone());
                if matches!(entry, std::collections::hash_map::Entry::Vacant(_)) {
                    ordered_keys.push(key);
                }
                entry.or_default().push(row_idx);
            }

            let num_groups = ordered_keys.len();

            // Build output columns
            let mut output_columns: Vec<arrow_array::ArrayRef> = Vec::new();

            // Key columns: take from first row of each group
            for &ki in &key_indices {
                if ki >= batch.num_columns() {
                    continue; // Skip invalid indices after schema reconciliation
                }
                let col = batch.column(ki);
                let first_indices: Vec<u32> =
                    ordered_keys.iter().map(|k| groups[k][0] as u32).collect();
                let idx_array = arrow_array::UInt32Array::from(first_indices);
                let taken = arrow::compute::take(col.as_ref(), &idx_array, None).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                output_columns.push(taken);
            }

            // Per-fold evaluation context. `TopKProofs` / `BddExact` are
            // provenance specializations handled above the aggregate (see
            // `compute_fold_aggregate`); the aggregate trait only sees the
            // two value-level combinators.
            let cx = FoldContext {
                strict,
                epsilon,
                semiring: match semiring_kind {
                    SemiringKind::MaxMinProb => FoldSemiring::MaxMin,
                    _ => FoldSemiring::AddMult,
                },
                // MIN / MAX over a Cypher value order it as `ORDER BY` does.
                order: Some(uni_plugin::traits::locy::ValueOrder(
                    crate::query::df_udfs::cypher_value_order,
                )),
            };

            // Fold binding columns: compute aggregates per group via the
            // resolved `LocyAggregate` trait object.
            for binding in &fold_bindings {
                let col: Arc<dyn Array> = if binding.name.as_str() == "COUNTALL" {
                    // COUNTALL has no input column — the aggregate ignores it.
                    Arc::new(arrow_array::Int64Array::from(vec![0i64; batch.num_rows()]))
                } else {
                    // Resolve the aggregate input column. When the binding carries
                    // a column name (the FOLD variable), the planner always projects
                    // it into the body batch (`build_clause`'s FOLD-input pass), so a
                    // name miss signals a real resolution bug — fail loud rather than
                    // fabricating a zeros column that would silently corrupt the
                    // aggregate (issue #145). `None` (test constructors / schema
                    // reconciliation) falls back to the positional index.
                    let resolved_idx = match &binding.input_col_name {
                        Some(name) => batch.schema().index_of(name).map_err(|_| {
                            datafusion::error::DataFusionError::Internal(format!(
                                "FOLD aggregate '{}' input column '{}' not found in \
                                 body batch (columns: {:?})",
                                binding.name,
                                name,
                                batch
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|f| f.name().clone())
                                    .collect::<Vec<_>>(),
                            ))
                        })?,
                        None => binding.input_col_index,
                    };
                    let column = batch.columns().get(resolved_idx).ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "FOLD aggregate '{}' input column index {} out of range \
                             ({} columns)",
                            binding.name,
                            resolved_idx,
                            batch.num_columns(),
                        ))
                    })?;
                    Arc::clone(column)
                };
                let topk_ctx = if matches!(semiring_kind, SemiringKind::TopKProofs { .. }) {
                    Some(TopKFoldCtx {
                        k: top_k_proofs_k,
                        batch: &batch,
                        body_support_map: body_support_map.as_deref(),
                    })
                } else {
                    None
                };
                let distinct_groups = binding
                    .distinct
                    .then(|| distinct_value_groups(col.as_ref(), &groups))
                    .transpose()?;
                let agg_col = compute_fold_aggregate(
                    col.as_ref(),
                    &binding.aggregate,
                    FoldGroups {
                        ordered_keys: &ordered_keys,
                        groups: distinct_groups.as_ref().unwrap_or(&groups),
                        num_groups,
                    },
                    &cx,
                    topk_ctx.as_ref(),
                )?;
                output_columns.push(agg_col);
            }

            RecordBatch::try_new(output_schema, output_columns).map_err(arrow_err)
        };

        Ok(Box::pin(FoldStream {
            state: FoldStreamState::Running(Box::pin(fut)),
            schema: Arc::clone(&self.schema),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Aggregate computation
// ---------------------------------------------------------------------------

/// Per-key-group data threaded into `compute_fold_aggregate` —
/// bundled to keep the aggregator's signature under the
/// too-many-arguments threshold.
struct FoldGroups<'a> {
    ordered_keys: &'a [Vec<ScalarKey>],
    groups: &'a HashMap<Vec<ScalarKey>, Vec<usize>>,
    num_groups: usize,
}

/// Body-row → IS-ref support for TopKProofs MNOR (Phase D D-C0), keyed by
/// the row's content over named columns.
///
/// Built over the pre-fold facts and read by [`FoldExec`] over its input
/// batch. Both sides hash the same columns, resolved by name in each batch:
/// the map was keyed by a positional hash over every column, so a batch whose
/// columns were reordered or renamed between the two missed every row and
/// silently folded under independence. A column the reading batch lacks is
/// an error rather than a hash over a subset, which would merge rows.
#[derive(Debug, Clone, Default)]
pub struct BodySupportMap {
    columns: Vec<String>,
    rows: HashMap<Vec<u8>, Vec<ProofTerm>>,
}

impl BodySupportMap {
    /// An empty map hashing rows over `columns`.
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: HashMap::new(),
        }
    }

    /// Where the map's columns sit in `schema`, in the map's order.
    ///
    /// # Errors
    ///
    /// Fails when `schema` lacks one of the columns.
    pub fn resolve(&self, schema: &SchemaRef) -> DFResult<Vec<usize>> {
        self.columns
            .iter()
            .map(|name| {
                schema.index_of(name).map_err(|_| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "TopKProofs support map column '{name}' missing from the FOLD \
                         input (columns: {:?})",
                        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                    ))
                })
            })
            .collect()
    }

    /// Records `support` for row `row` of `batch`, whose map columns sit at
    /// `columns` (from [`Self::resolve`]).
    pub fn insert(
        &mut self,
        batch: &RecordBatch,
        columns: &[usize],
        row: usize,
        support: Vec<ProofTerm>,
    ) {
        self.rows.insert(
            super::locy_fixpoint::fact_hash_key(batch, columns, row),
            support,
        );
    }

    /// The support recorded for row `row` of `batch`, whose map columns sit
    /// at `columns` (from [`Self::resolve`]).
    pub fn get(&self, batch: &RecordBatch, columns: &[usize], row: usize) -> Option<&[ProofTerm]> {
        self.rows
            .get(&super::locy_fixpoint::fact_hash_key(batch, columns, row))
            .map(Vec::as_slice)
    }

    /// Whether no row has support.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Phase D D-C0: per-call context for TopKProofs-aware aggregation.
/// Carries the K config and a pre-computed body-row → IS-ref support
/// map so MNOR / MPROD can build per-row `Proof`s and aggregate via
/// DNF inclusion-exclusion. The map is built in
/// `apply_post_fixpoint_chain` before `FoldExec` is constructed; the
/// provenance tracker is *not* sufficient at this stage because its
/// entries are keyed by post-YIELD row hashes and the current rule's
/// facts have not been recorded yet.
struct TopKFoldCtx<'a> {
    k: usize,
    batch: &'a RecordBatch,
    body_support_map: Option<&'a BodySupportMap>,
}

/// Compute one fold-output column by dispatching through the resolved
/// [`LocyAggregate`] trait object.
///
/// For each key group a fresh [`LocyAggState`](uni_plugin::traits::locy::LocyAggState)
/// is created, fed the group's rows via `ingest_indices`, and finalized; the
/// per-group [`ScalarValue`]s are assembled into the output array (whose type
/// follows the finalized scalars — `Int64`/`Float64` for `MIN`/`MAX` over those
/// inputs, `LargeBinary` for `COLLECT`, etc.).
///
/// `TopKProofs` noisy-OR is the one provenance specialization that sits
/// *above* the aggregate: when a TopK context is threaded and the aggregate is
/// noisy-OR, each group folds via DNF inclusion-exclusion over its support
/// chains instead (degrading to plain noisy-OR when no support map exists).
///
/// # Errors
///
/// Returns a [`DFResult`] error if a plugin aggregate rejects a value (e.g., a
/// strict probability-domain violation) or if the per-group scalars cannot be
/// assembled into an Arrow array.
fn compute_fold_aggregate(
    col: &dyn Array,
    aggregate: &Arc<dyn LocyAggregate>,
    groups_ctx: FoldGroups<'_>,
    cx: &FoldContext,
    topk_ctx: Option<&TopKFoldCtx<'_>>,
) -> DFResult<arrow_array::ArrayRef> {
    let ordered_keys = groups_ctx.ordered_keys;
    let groups = groups_ctx.groups;
    let num_groups = groups_ctx.num_groups;

    // Phase D D-C0: TopKProofs noisy-OR uses DNF inclusion-exclusion over each
    // row's support chain. This is a provenance specialization layered above
    // the MNOR aggregate; it falls back to independence-mode noisy-OR when no
    // support map is present (the common / non-recursive case).
    if let Some(ctx) = topk_ctx
        && aggregate.is_noisy_or()
    {
        let mut builder = Float64Builder::with_capacity(num_groups);
        for key in ordered_keys {
            builder.append_option(topk_dnf_disjunction(col, &groups[key], cx.strict, ctx)?);
        }
        return Ok(Arc::new(builder.finish()));
    }

    // Generic trait dispatch: one aggregate state per key group.
    let mut scalars: Vec<ScalarValue> = Vec::with_capacity(num_groups);
    for key in ordered_keys {
        let mut state = aggregate.create();
        state
            .ingest_indices(col, &groups[key], cx)
            .map_err(fn_error_to_df)?;
        scalars.push(state.finalize().map_err(fn_error_to_df)?);
    }
    ScalarValue::iter_to_array(scalars)
}

/// Keeps, per key group, the first row of each distinct input value.
///
/// This is `DISTINCT` in a FOLD aggregate: the aggregate then sees each value
/// once, as Cypher's `count(DISTINCT x)` does. NULL rows are kept, since every
/// aggregate skips them anyway.
///
/// # Errors
///
/// Returns an error if a value cannot be read from `col`.
fn distinct_value_groups(
    col: &dyn Array,
    groups: &HashMap<Vec<ScalarKey>, Vec<usize>>,
) -> DFResult<HashMap<Vec<ScalarKey>, Vec<usize>>> {
    groups
        .iter()
        .map(|(key, rows)| {
            let mut seen = std::collections::HashSet::new();
            let mut kept = Vec::with_capacity(rows.len());
            for &row in rows {
                if col.is_null(row) || seen.insert(ScalarValue::try_from_array(col, row)?) {
                    kept.push(row);
                }
            }
            Ok((key.clone(), kept))
        })
        .collect()
}

/// Map a plugin [`FnError`](uni_plugin::FnError) to a DataFusion error,
/// preserving the message so strict-domain text survives to the caller.
fn fn_error_to_df(e: uni_plugin::FnError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(e.message)
}

/// Phase D D-C0: TopKProofs MNOR via DNF inclusion-exclusion over the
/// rows' support chains. Each row contributes one `Proof` whose
/// weight is the row's MNOR-input value and whose `base_rvs` are
/// interned from the row's IS-ref support, resolved via
/// `ctx.body_support_map` — a precomputed map of body-row content
/// hash → `Vec<ProofTerm>`. The map is built in
/// `apply_post_fixpoint_chain` before FOLD; the `ProvenanceStore`
/// tracker cannot be used here because its entries are keyed by
/// post-YIELD hashes and the rule's own facts haven't been recorded
/// yet at FOLD time. Proofs are merged via `merge_top_k_runtime` (so
/// the K config is respected and `CrossedDependency` notices ride
/// the side-channel). The per-group output is
/// `TopKTag.to_dnf().weight(&base_weights)` — exact when no
/// dependency overlap exists, exact under inclusion-exclusion when
/// shared base facts appear across proofs.
///
/// Rows whose body-hash isn't in the support map (e.g. rules whose
/// MNOR runs over plain columns with no IS-ref bindings) contribute
/// empty-support Proofs — the math degrades to plain f64 noisy-OR
/// (independence-mode), preserving the pre-D-C0 byte-identical
/// AddMultProb behavior.
fn topk_dnf_disjunction(
    col: &dyn Array,
    indices: &[usize],
    strict: bool,
    ctx: &TopKFoldCtx<'_>,
) -> DFResult<Option<f64>> {
    use uni_locy::{BaseRv, BaseRvSet, Proof};

    let batch = ctx.batch;
    let support_columns = ctx
        .body_support_map
        .map(|m| m.resolve(&batch.schema()))
        .transpose()?;
    let mut interner: HashMap<Vec<u8>, BaseRv> = HashMap::new();
    let mut next_rv: u32 = 0;
    let mut base_weights: HashMap<BaseRv, f64> = HashMap::new();
    let mut proofs: Vec<Proof> = Vec::with_capacity(indices.len());

    for &i in indices {
        if col.is_null(i) {
            continue;
        }
        // Row's MNOR-input value (e.g. an IS-ref edge probability).
        let val = match col.as_any().downcast_ref::<arrow_array::Float64Array>() {
            Some(arr) => arr.value(i),
            None => match col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                Some(arr) => arr.value(i) as f64,
                None => continue,
            },
        };
        if strict && !(0.0..=1.0).contains(&val) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "strict_probability_domain: MNOR input {val} outside [0,1] under TopKProofs"
            )));
        }
        let weight = val.clamp(0.0, 1.0);

        // Resolve row's IS-ref support via the precomputed body-row map;
        // intern base facts into BaseRvs.
        let mut base_rvs = BaseRvSet::empty();
        let support = ctx
            .body_support_map
            .zip(support_columns.as_deref())
            .and_then(|(m, columns)| m.get(batch, columns, i));
        if let Some(support) = support {
            for term in support {
                let rv = *interner
                    .entry(term.base_fact_id.clone())
                    .or_insert_with(|| {
                        let r = BaseRv(next_rv);
                        next_rv += 1;
                        r
                    });
                base_rvs.insert(rv);
            }
        }
        // Single-row Proof. Base weights for the DNF: assign the row's
        // weight to each base RV under it (when no support exists,
        // base_rvs is empty and the proof's weight stands alone).
        // When multiple rows share the same base RV (shared-proof case),
        // take max — deterministic regardless of row visit order, and
        // a conservative upper bound for the noisy-OR DNF.
        if base_rvs.iter().count() > 0 {
            for rv in base_rvs.iter() {
                base_weights
                    .entry(rv)
                    .and_modify(|w| {
                        if weight > *w {
                            *w = weight;
                        }
                    })
                    .or_insert(weight);
            }
        }
        proofs.push(Proof {
            weight,
            base_rvs,
            neural_calls: Vec::new(),
        });
    }
    if proofs.is_empty() {
        return Ok(None);
    }
    // Partition proofs by whether they carry IS-ref support (`base_rvs`). Only
    // supported proofs are meaningful DNF clauses; an empty-base_rvs proof must
    // NOT enter the DNF, because `DependencyDnf::weight` treats an empty clause as
    // trivially true (prob 1.0) — so mixing a supported and an unsupported row in
    // one group would collapse the whole group's probability to 1.0. Unsupported
    // rows (the rule's MNOR running over plain columns, not derived facts) are
    // instead folded in as INDEPENDENT noisy-OR factors, preserving the pre-D-C0
    // AddMultProb behavior.
    let (supported, unsupported): (Vec<_>, Vec<_>) =
        proofs.into_iter().partition(|p| !p.base_rvs.is_empty());

    // Complement of the independent (unsupported) contributions: ∏ (1 - weight).
    let mut indep_complement = 1.0;
    for p in &unsupported {
        indep_complement *= 1.0 - p.weight;
    }

    if supported.is_empty() {
        // No DNF terms at all — pure independence-mode noisy-OR.
        return Ok(Some((1.0 - indep_complement).clamp(0.0, 1.0)));
    }

    // DNF inclusion-exclusion over the supported proofs. Merge top-K (which
    // dedupes by dependency_key intentionally — shared bases ARE the same
    // dependency).
    let k = if ctx.k == 0 { supported.len() } else { ctx.k };
    let (kept, _notice) = uni_locy::merge_top_k_runtime(Vec::new(), supported, k);
    let tag = uni_locy::TopKTag { proofs: kept };
    let dnf_prob = tag.to_dnf().weight(&base_weights);

    // Combine the DNF probability with the independent unsupported rows by
    // noisy-OR (they are independent events): 1 - (1 - dnf) · ∏ (1 - weight).
    Ok(Some(
        (1.0 - (1.0 - dnf_prob) * indep_complement).clamp(0.0, 1.0),
    ))
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

enum FoldStreamState {
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    Done,
}

struct FoldStream {
    state: FoldStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for FoldStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        match &mut self.state {
            FoldStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = FoldStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = FoldStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            FoldStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for FoldStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Only the test-local `LocyAggState` impl below still needs `Any`; the
    // production `ExecutionPlan` impl lost its `as_any` in DataFusion 54.
    use std::any::Any;

    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;

    /// Direct construction of a built-in `LocyAggregate` trait object for use
    /// in `FoldBinding` test fixtures. Avoids registry plumbing in tests that
    /// only need a working aggregate.
    fn builtin_agg(name: &str) -> Arc<dyn LocyAggregate> {
        use uni_plugin_builtin::locy_aggregates::*;
        match name {
            "SUM" | "MSUM" => Arc::new(SumAgg),
            "MAX" | "MMAX" => Arc::new(MaxAgg),
            "MIN" | "MMIN" => Arc::new(MinAgg),
            "COUNT" | "COUNTALL" | "MCOUNT" => Arc::new(CountAgg),
            "AVG" => Arc::new(AvgAgg),
            "COLLECT" => Arc::new(CollectAgg),
            "MNOR" | "NOR" => Arc::new(MnorAgg),
            "MPROD" | "PROD" => Arc::new(MprodAgg),
            other => panic!("unknown test aggregate `{other}`"),
        }
    }

    fn make_test_batch(names: Vec<&str>, values: Vec<f64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.into_iter().map(Some).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn make_memory_exec(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    async fn execute_fold(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        fold_bindings: Vec<FoldBinding>,
    ) -> RecordBatch {
        let exec = FoldExec::new(input, key_indices, fold_bindings, false, 1e-15);
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        if batches.is_empty() {
            RecordBatch::new_empty(exec.schema())
        } else {
            arrow::compute::concat_batches(&exec.schema(), &batches).unwrap()
        }
    }

    #[tokio::test]
    async fn test_sum_single_group() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![1.0, 2.0, 3.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "total".to_string(),
                name: SmolStr::new_static("SUM"),
                aggregate: builtin_agg("SUM"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let totals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((totals.value(0) - 6.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_count_non_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("a")])),
                Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
            ],
        )
        .unwrap();
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "cnt".to_string(),
                name: SmolStr::new_static("COUNT"),
                aggregate: builtin_agg("COUNT"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let counts = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 2); // null not counted
    }

    #[tokio::test]
    async fn test_max_min() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![3.0, 1.0, 5.0]);
        let input_max = make_memory_exec(batch.clone());
        let input_min = make_memory_exec(batch);

        let result_max = execute_fold(
            input_max,
            vec![0],
            vec![FoldBinding {
                output_name: "mx".to_string(),
                name: SmolStr::new_static("MAX"),
                aggregate: builtin_agg("MAX"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let result_min = execute_fold(
            input_min,
            vec![0],
            vec![FoldBinding {
                output_name: "mn".to_string(),
                name: SmolStr::new_static("MIN"),
                aggregate: builtin_agg("MIN"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        let max_vals = result_max
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(max_vals.value(0), 5.0);

        let min_vals = result_min
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(min_vals.value(0), 1.0);
    }

    #[tokio::test]
    async fn test_avg() {
        let batch = make_test_batch(vec!["a", "a", "a", "a"], vec![2.0, 4.0, 6.0, 8.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "average".to_string(),
                name: SmolStr::new_static("AVG"),
                aggregate: builtin_agg("AVG"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let avgs = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((avgs.value(0) - 5.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_multiple_groups() {
        let batch = make_test_batch(
            vec!["a", "a", "b", "b", "b"],
            vec![1.0, 2.0, 10.0, 20.0, 30.0],
        );
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "total".to_string(),
                name: SmolStr::new_static("SUM"),
                aggregate: builtin_agg("SUM"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 2);
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..2 {
            match names.value(i) {
                "a" => assert!((totals.value(i) - 3.0).abs() < f64::EPSILON),
                "b" => assert!((totals.value(i) - 60.0).abs() < f64::EPSILON),
                _ => panic!("unexpected name"),
            }
        }
    }

    #[tokio::test]
    async fn test_empty_input() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "total".to_string(),
                name: SmolStr::new_static("SUM"),
                aggregate: builtin_agg("SUM"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_multiple_bindings() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![1.0, 2.0, 3.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![
                FoldBinding {
                    output_name: "total".to_string(),
                    name: SmolStr::new_static("SUM"),
                    aggregate: builtin_agg("SUM"),
                    input_col_index: 1,
                    input_col_name: None,
                    distinct: false,
                },
                FoldBinding {
                    output_name: "cnt".to_string(),
                    name: SmolStr::new_static("COUNT"),
                    aggregate: builtin_agg("COUNT"),
                    input_col_index: 1,
                    input_col_name: None,
                    distinct: false,
                },
                FoldBinding {
                    output_name: "mx".to_string(),
                    name: SmolStr::new_static("MAX"),
                    aggregate: builtin_agg("MAX"),
                    input_col_index: 1,
                    input_col_name: None,
                    distinct: false,
                },
            ],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 4); // name + total + cnt + mx

        let totals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((totals.value(0) - 6.0).abs() < f64::EPSILON);

        let counts = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 3);

        let maxes = result
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(maxes.value(0), 3.0);
    }

    // ── MNOR tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_nor_single_group() {
        // MNOR({0.3, 0.5}) = 1 - (1-0.3)*(1-0.5) = 1 - 0.7*0.5 = 1 - 0.35 = 0.65
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.65).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_identity() {
        // MNOR({0.0, 0.0}) = 1 - (1-0)*(1-0) = 1 - 1 = 0.0
        let batch = make_test_batch(vec!["a", "a"], vec![0.0, 0.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_clamping() {
        // Out-of-range values should be clamped to [0, 1]
        let batch = make_test_batch(vec!["a", "a"], vec![-0.5, 1.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Clamped to (0.0, 1.0): MNOR = 1 - (1-0)*(1-1) = 1 - 1*0 = 1.0
        assert!((vals.value(0) - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_multiple_groups() {
        let batch = make_test_batch(vec!["a", "a", "b", "b"], vec![0.3, 0.5, 0.1, 0.2]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 2);
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..2 {
            match names.value(i) {
                // MNOR({0.3, 0.5}) = 0.65
                "a" => assert!((vals.value(i) - 0.65).abs() < 1e-10),
                // MNOR({0.1, 0.2}) = 1 - 0.9*0.8 = 1 - 0.72 = 0.28
                "b" => assert!((vals.value(i) - 0.28).abs() < 1e-10),
                _ => panic!("unexpected name"),
            }
        }
    }

    // ── MPROD tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_prod_single_group() {
        // MPROD({0.6, 0.8}) = 0.48
        let batch = make_test_batch(vec!["a", "a"], vec![0.6, 0.8]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.48).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_identity() {
        // MPROD({1.0, 1.0}) = 1.0
        let batch = make_test_batch(vec!["a", "a"], vec![1.0, 1.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_zero_absorbing() {
        // MPROD with 0.0 = 0.0 (zero is absorbing element)
        let batch = make_test_batch(vec!["a", "a", "a"], vec![0.5, 0.0, 0.8]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_underflow_protection() {
        // 50 × 0.5 ≈ 8.88e-16, should not be exactly 0 thanks to log-space
        let names: Vec<&str> = vec!["a"; 50];
        let values: Vec<f64> = vec![0.5; 50];
        let batch = make_test_batch(names, values);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 0.5_f64.powi(50); // ≈ 8.88e-16
        assert!(vals.value(0) > 0.0, "should not underflow to zero");
        assert!(
            (vals.value(0) - expected).abs() / expected < 1e-6,
            "result {} should be close to expected {}",
            vals.value(0),
            expected
        );
    }

    // ── MNOR/MPROD mathematical correctness tests ───────────────────────

    fn make_nullable_test_batch(names: Vec<&str>, values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.into_iter().map(Some).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_nor_single_element() {
        // MNOR({0.7}) = 0.7 (n=1 identity)
        let batch = make_test_batch(vec!["a"], vec![0.7]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.7).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_single_element() {
        // MPROD({0.7}) = 0.7 (n=1 identity)
        let batch = make_test_batch(vec!["a"], vec![0.7]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.7).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_three_elements() {
        // MNOR({0.3, 0.4, 0.5}) = 1 - (0.7)(0.6)(0.5) = 0.79
        let batch = make_test_batch(vec!["a", "a", "a"], vec![0.3, 0.4, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.79).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_four_elements_spec_example() {
        // Spec §4.5: MNOR({0.72, 0.54, 0.56, 0.42}) = 1 - (0.28)(0.46)(0.44)(0.58) = 0.96713024
        let batch = make_test_batch(vec!["a", "a", "a", "a"], vec![0.72, 0.54, 0.56, 0.42]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (vals.value(0) - 0.96713024).abs() < 1e-10,
            "expected 0.96713024, got {}",
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_prod_three_elements() {
        // MPROD({0.5, 0.5, 0.5}) = 0.125
        let batch = make_test_batch(vec!["a", "a", "a"], vec![0.5, 0.5, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.125).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_absorbing_element() {
        // p=1.0 absorbs: MNOR({0.3, 1.0}) = 1.0
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, 1.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_clamping() {
        // Out-of-range 2.0 clamped to 1.0: MPROD({2.0, 0.5}) = 1.0 * 0.5 = 0.5
        let batch = make_test_batch(vec!["a", "a"], vec![2.0, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.5).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_multiple_groups() {
        // a: MPROD({0.6, 0.8}) = 0.48, b: MPROD({0.5, 0.5}) = 0.25
        let batch = make_test_batch(vec!["a", "a", "b", "b"], vec![0.6, 0.8, 0.5, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 2);
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..2 {
            match names.value(i) {
                "a" => assert!((vals.value(i) - 0.48).abs() < 1e-10),
                "b" => assert!((vals.value(i) - 0.25).abs() < 1e-10),
                _ => panic!("unexpected group name"),
            }
        }
    }

    #[tokio::test]
    async fn test_nor_commutativity() {
        // Order independence: MNOR({0.2, 0.5, 0.8}) = MNOR({0.8, 0.5, 0.2}) = 0.92
        let fwd = make_test_batch(vec!["a", "a", "a"], vec![0.2, 0.5, 0.8]);
        let rev = make_test_batch(vec!["a", "a", "a"], vec![0.8, 0.5, 0.2]);
        let binding = vec![FoldBinding {
            output_name: "prob".to_string(),
            name: SmolStr::new_static("MNOR"),
            aggregate: builtin_agg("MNOR"),
            input_col_index: 1,
            input_col_name: None,
            distinct: false,
        }];
        let r1 = execute_fold(make_memory_exec(fwd), vec![0], binding.clone()).await;
        let r2 = execute_fold(make_memory_exec(rev), vec![0], binding).await;
        let v1 = r1
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let v2 = r2
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((v1 - 0.92).abs() < 1e-10);
        assert!((v2 - 0.92).abs() < 1e-10);
        assert!((v1 - v2).abs() < 1e-15, "commutativity violated");
    }

    #[tokio::test]
    async fn test_prod_commutativity() {
        // Order independence: MPROD({0.5, 0.25}) = MPROD({0.25, 0.5}) = 0.125
        let fwd = make_test_batch(vec!["a", "a"], vec![0.5, 0.25]);
        let rev = make_test_batch(vec!["a", "a"], vec![0.25, 0.5]);
        let binding = vec![FoldBinding {
            output_name: "prob".to_string(),
            name: SmolStr::new_static("MPROD"),
            aggregate: builtin_agg("MPROD"),
            input_col_index: 1,
            input_col_name: None,
            distinct: false,
        }];
        let r1 = execute_fold(make_memory_exec(fwd), vec![0], binding.clone()).await;
        let r2 = execute_fold(make_memory_exec(rev), vec![0], binding).await;
        let v1 = r1
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let v2 = r2
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((v1 - 0.125).abs() < 1e-10);
        assert!((v2 - 0.125).abs() < 1e-10);
        assert!((v1 - v2).abs() < 1e-15, "commutativity violated");
    }

    #[tokio::test]
    async fn test_nor_boundary_near_zero() {
        // Precision near 0: MNOR({0.001, 0.002}) = 1 - (0.999)(0.998) = 0.002998
        let batch = make_test_batch(vec!["a", "a"], vec![0.001, 0.002]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 1.0 - 0.999 * 0.998;
        assert!(
            (vals.value(0) - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_nor_boundary_near_one() {
        // Precision near 1: MNOR({0.999, 0.998}) = 1 - (0.001)(0.002) = 0.999998
        let batch = make_test_batch(vec!["a", "a"], vec![0.999, 0.998]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 1.0 - 0.001 * 0.002;
        assert!(
            (vals.value(0) - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_prod_boundary_near_zero() {
        // Precision near 0: MPROD({0.001, 0.002}) = 2e-6
        let batch = make_test_batch(vec!["a", "a"], vec![0.001, 0.002]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (vals.value(0) - 2e-6).abs() < 1e-15,
            "expected 2e-6, got {}",
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_nor_empty_input() {
        // Empty input → 0 rows output
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_nor_nan_handling() {
        // NaN propagates through noisy-OR
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, f64::NAN]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(vals.value(0).is_nan(), "NaN should propagate through MNOR");
    }

    #[tokio::test]
    async fn test_prod_nan_handling() {
        // NaN propagates through product
        let batch = make_test_batch(vec!["a", "a"], vec![0.5, f64::NAN]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(vals.value(0).is_nan(), "NaN should propagate through MPROD");
    }

    #[tokio::test]
    async fn test_prod_infinity_handling() {
        // +∞ clamped to 1.0: MPROD({0.5, ∞}) = 0.5 * 1.0 = 0.5
        let batch = make_test_batch(vec!["a", "a"], vec![0.5, f64::INFINITY]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.5).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_infinity_handling() {
        // +∞ clamped to 1.0, which absorbs: MNOR({0.3, ∞}) = 1.0
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, f64::INFINITY]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_all_null_values() {
        // All-null input → null output
        let batch = make_nullable_test_batch(vec!["a", "a"], vec![None, None]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(vals.is_null(0), "all-null MNOR should produce null");
    }

    #[tokio::test]
    async fn test_prod_all_null_values() {
        // All-null input → null output
        let batch = make_nullable_test_batch(vec!["a", "a"], vec![None, None]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(vals.is_null(0), "all-null MPROD should produce null");
    }

    #[tokio::test]
    async fn test_nor_mixed_null_values() {
        // Nulls skipped: MNOR({0.3, null, 0.5}) = 1 - (0.7)(0.5) = 0.65
        let batch = make_nullable_test_batch(vec!["a", "a", "a"], vec![Some(0.3), None, Some(0.5)]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.65).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_mixed_null_values() {
        // Nulls skipped: MPROD({0.6, null, 0.8}) = 0.6 * 0.8 = 0.48
        let batch = make_nullable_test_batch(vec!["a", "a", "a"], vec![Some(0.6), None, Some(0.8)]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.48).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_many_small_values() {
        // Large accumulation: 20 × 0.1 → 1 - 0.9^20 ≈ 0.8784
        let names: Vec<&str> = vec!["a"; 20];
        let values: Vec<f64> = vec![0.1; 20];
        let batch = make_test_batch(names, values);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 1.0 - 0.9_f64.powi(20);
        assert!(
            (vals.value(0) - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            vals.value(0)
        );
    }

    // ── Aggregate-trait classification tests ──────────────────────────────

    #[test]
    fn trait_dispatch_monotonicity() {
        for name in [
            "SUM", "MAX", "MIN", "COUNT", "AVG", "COLLECT", "MNOR", "MPROD",
        ] {
            let agg = builtin_agg(name);
            let sl = agg.semilattice();
            // MIN/MAX/MNOR/MPROD/COLLECT/COUNT are monotone; SUM/AVG are not.
            let expect_monotone =
                matches!(name, "MIN" | "MAX" | "MNOR" | "MPROD" | "COLLECT" | "COUNT");
            assert_eq!(
                sl.monotone_join, expect_monotone,
                "monotone_join mismatch for {name}"
            );
        }
    }

    #[test]
    fn trait_dispatch_initial_accumulator() {
        // The row-level fast path uses `initial_accum_f64()`.
        assert_eq!(builtin_agg("SUM").initial_accum_f64(), Some(0.0));
        assert_eq!(builtin_agg("COUNT").initial_accum_f64(), Some(0.0));
        assert_eq!(builtin_agg("MNOR").initial_accum_f64(), Some(0.0));
        assert_eq!(
            builtin_agg("MAX").initial_accum_f64(),
            Some(f64::NEG_INFINITY)
        );
        assert_eq!(builtin_agg("MIN").initial_accum_f64(), Some(f64::INFINITY));
        assert_eq!(builtin_agg("MPROD").initial_accum_f64(), Some(1.0));
        // AVG and COLLECT have no row-level fast path — return None.
        assert_eq!(builtin_agg("AVG").initial_accum_f64(), None);
        assert_eq!(builtin_agg("COLLECT").initial_accum_f64(), None);
    }

    #[test]
    fn trait_dispatch_probability_predicate() {
        // is_probability_aggregate is the trait predicate for probability-domain aggregates.
        for name in ["MNOR", "MPROD"] {
            assert!(
                builtin_agg(name).is_probability_aggregate(),
                "expected {name} to be probability-domain"
            );
        }
        for name in ["SUM", "MAX", "MIN", "COUNT", "AVG", "COLLECT"] {
            assert!(
                !builtin_agg(name).is_probability_aggregate(),
                "{name} should NOT be probability-domain"
            );
        }
    }

    #[test]
    fn trait_dispatch_noisy_or_predicate() {
        // is_noisy_or distinguishes MNOR from MPROD for semiring-op selection.
        assert!(builtin_agg("MNOR").is_noisy_or());
        assert!(!builtin_agg("MPROD").is_noisy_or());
    }

    // ── Strict mode tests (Phase 5) ──────────────────────────────────────

    async fn execute_fold_strict(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        fold_bindings: Vec<FoldBinding>,
        strict: bool,
    ) -> DFResult<RecordBatch> {
        let exec = FoldExec::new(input, key_indices, fold_bindings, strict, 1e-15);
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream).await?;
        if batches.is_empty() {
            Ok(RecordBatch::new_empty(exec.schema()))
        } else {
            arrow::compute::concat_batches(&exec.schema(), &batches).map_err(arrow_err)
        }
    }

    #[tokio::test]
    async fn test_nor_strict_rejects_above_one() {
        let batch = make_test_batch(vec!["a"], vec![1.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
            true,
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_nor_strict_rejects_negative() {
        let batch = make_test_batch(vec!["a"], vec![-0.1]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
            true,
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_prod_strict_rejects_above_one() {
        let batch = make_test_batch(vec!["a"], vec![2.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
            true,
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_prod_strict_rejects_negative() {
        let batch = make_test_batch(vec!["a"], vec![-0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                name: SmolStr::new_static("MPROD"),
                aggregate: builtin_agg("MPROD"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
            true,
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_nor_strict_accepts_valid() {
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                name: SmolStr::new_static("MNOR"),
                aggregate: builtin_agg("MNOR"),
                input_col_index: 1,
                input_col_name: None,
                distinct: false,
            }],
            true,
        )
        .await;
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
        let batch = result.unwrap();
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 0.65; // 1 - (1-0.3)(1-0.5)
        assert!(
            (vals.value(0) - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_count_all_groups_by_key() {
        // Two groups: "a" (2 rows), "b" (1 row)
        let batch = make_test_batch(vec!["a", "a", "b"], vec![10.0, 20.0, 30.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "cnt".to_string(),
                name: SmolStr::new_static("COUNTALL"),
                aggregate: builtin_agg("COUNTALL"),
                input_col_index: 0, // unused for CountAll
                input_col_name: None,
                distinct: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 2, "Should have 2 groups");
        let counts = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 2, "Group 'a' should have count 2");
        assert_eq!(counts.value(1), 1, "Group 'b' should have count 1");
    }

    // ── Registry-resolve sanity tests ────────────────────────────────────

    /// A test-only `LocyAggregate` that's not in `uni-plugin-builtin`.
    /// Used to prove `resolve_locy_aggregate` walks the registry rather
    /// than dispatching from a hardcoded built-in table.
    #[derive(Debug)]
    struct IdentityAgg;

    impl LocyAggregate for IdentityAgg {
        fn semilattice(&self) -> uni_plugin::traits::locy::Semilattice {
            uni_plugin::traits::locy::Semilattice::BOUNDED_MIN_MAX
        }
        fn output_type(&self) -> arrow_schema::DataType {
            arrow_schema::DataType::Float64
        }
        fn create(&self) -> Box<dyn uni_plugin::traits::locy::LocyAggState> {
            panic!("IdentityAgg::create not used in this sanity test")
        }
    }

    #[test]
    fn resolve_locy_aggregate_returns_registered_instance() {
        let registry = uni_plugin::PluginRegistry::new();
        let plugin_id = uni_plugin::PluginId::new(uni_plugin::QName::BUILTIN_NS);
        let caps = uni_plugin::CapabilitySet::from_iter_of([uni_plugin::Capability::LocyAggregate]);

        let registered: Arc<dyn LocyAggregate> = Arc::new(IdentityAgg);
        let mut r = uni_plugin::PluginRegistrar::new(plugin_id, &caps, &registry);
        r.locy_aggregate(
            uni_plugin::QName::builtin("TEST_IDENTITY"),
            Arc::clone(&registered),
        )
        .expect("register");
        r.commit_to_registry().expect("commit");

        let resolved = resolve_locy_aggregate(&registry, "TEST_IDENTITY")
            .expect("registered aggregate should resolve");
        assert!(
            Arc::ptr_eq(&registered, &resolved.aggregate),
            "registry must return the exact Arc that was registered"
        );

        // Unknown name still returns None — the resolver does not fall back.
        assert!(resolve_locy_aggregate(&registry, "NOT_REGISTERED").is_none());
    }

    #[test]
    fn resolve_locy_aggregate_finds_plugin_namespaced() {
        // P0.2 regression: a third-party aggregate registered under its own
        // plugin namespace (not `builtin`) must resolve via a dotted name.
        let registry = uni_plugin::PluginRegistry::new();
        let plugin_id = uni_plugin::PluginId::new("myplugin");
        let caps = uni_plugin::CapabilitySet::from_iter_of([uni_plugin::Capability::LocyAggregate]);

        let registered: Arc<dyn LocyAggregate> = Arc::new(IdentityAgg);
        let mut r = uni_plugin::PluginRegistrar::new(plugin_id, &caps, &registry);
        r.locy_aggregate(
            uni_plugin::QName::new("myplugin", "MYAGG"),
            Arc::clone(&registered),
        )
        .expect("register");
        r.commit_to_registry().expect("commit");

        // Before P0.2 this returned None (builtin-only lookup); now the
        // candidate-split fallback resolves `myplugin.MYAGG`.
        let resolved = resolve_locy_aggregate(&registry, "myplugin.MYAGG")
            .expect("plugin-namespaced aggregate should resolve via dotted name");
        assert!(Arc::ptr_eq(&registered, &resolved.aggregate));

        // A bare local name (no namespace) must NOT resolve — avoids
        // cross-namespace ambiguity.
        assert!(resolve_locy_aggregate(&registry, "MYAGG").is_none());
    }

    #[test]
    fn default_locy_plugin_registry_contains_all_builtins() {
        let r = default_locy_plugin_registry();
        for name in [
            "MIN", "MAX", "SUM", "MSUM", "COUNT", "AVG", "COLLECT", "MNOR", "MPROD",
        ] {
            assert!(
                resolve_locy_aggregate(&r, name).is_some(),
                "default registry should contain built-in `{name}`"
            );
        }
    }

    // ── User-defined aggregate runs through the trait (G1 regression) ─────

    /// A novel aggregate not in `uni-plugin-builtin`: per-group `max − min`.
    ///
    /// Requires real columnar state (two accumulators), so it is *not*
    /// expressible via the `update_step` scalar fast path. Before the fold
    /// executor dispatched through [`LocyAggState`], a binding like this hit
    /// the closed-enum `_ => Err("unsupported aggregate")` arm at runtime;
    /// now it executes through `create`/`ingest_indices`/`finalize`.
    #[derive(Debug)]
    struct RangeAgg;

    impl LocyAggregate for RangeAgg {
        fn semilattice(&self) -> uni_plugin::traits::locy::Semilattice {
            uni_plugin::traits::locy::Semilattice::NON_MONOTONE
        }
        fn output_type(&self) -> DataType {
            DataType::Float64
        }
        fn create(&self) -> Box<dyn uni_plugin::traits::locy::LocyAggState> {
            Box::new(RangeState {
                min: None,
                max: None,
            })
        }
    }

    #[derive(Debug)]
    struct RangeState {
        min: Option<f64>,
        max: Option<f64>,
    }

    impl uni_plugin::traits::locy::LocyAggState for RangeState {
        // `LocyAggState` is our own plugin trait and still declares `as_any`;
        // only DataFusion's traits dropped it in 54.
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn ingest_indices(
            &mut self,
            col: &dyn Array,
            indices: &[usize],
            _cx: &FoldContext,
        ) -> Result<(), uni_plugin::FnError> {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            for &i in indices {
                if arr.is_null(i) {
                    continue;
                }
                let v = arr.value(i);
                self.min = Some(self.min.map_or(v, |m| m.min(v)));
                self.max = Some(self.max.map_or(v, |m| m.max(v)));
            }
            Ok(())
        }
        fn merge(
            &mut self,
            _other: &dyn uni_plugin::traits::locy::LocyAggState,
        ) -> Result<(), uni_plugin::FnError> {
            Ok(())
        }
        fn finalize(&self) -> Result<ScalarValue, uni_plugin::FnError> {
            match (self.min, self.max) {
                (Some(lo), Some(hi)) => Ok(ScalarValue::Float64(Some(hi - lo))),
                _ => Ok(ScalarValue::Float64(None)),
            }
        }
    }

    #[tokio::test]
    async fn user_defined_aggregate_runs_in_non_recursive_fold() {
        // group "a": [1.0, 5.0] → range 4.0 ; group "b": [3.0] → range 0.0
        let batch = make_test_batch(vec!["a", "a", "b"], vec![1.0, 5.0, 3.0]);
        let input = make_memory_exec(batch);
        let binding = FoldBinding {
            output_name: "r".into(),
            name: SmolStr::new_static("RANGE"),
            aggregate: Arc::new(RangeAgg),
            input_col_index: 1,
            input_col_name: Some("value".to_string()),
            distinct: false,
        };
        let out = execute_fold(input, vec![0], vec![binding]).await;
        assert_eq!(out.num_rows(), 2);
        let col = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("range output is Float64");
        // ordered_keys preserves first-seen order: "a" then "b".
        assert_eq!(col.value(0), 4.0);
        assert_eq!(col.value(1), 0.0);
    }

    /// A `TopKProofs` noisy-OR group that mixes rows with IS-ref support and a
    /// row without any.
    ///
    /// An unsupported row has an empty `base_rvs` clause, and
    /// `DependencyDnf::weight` treats an empty clause as trivially true. Fed
    /// into the DNF with the supported rows, it drove the whole group to `1.0`.
    /// The unsupported row is instead an independent noisy-OR factor:
    /// `1 - (1 - dnf) * (1 - w)`.
    ///
    /// The public FOLD surface cannot build this group today (support resolves
    /// through a KEY column, so a group is uniformly supported or not), which
    /// is why the precondition is constructed here directly.
    #[test]
    fn topk_mixed_support_group_folds_unsupported_rows_independently() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("w", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("a")])),
                Arc::new(Float64Array::from(vec![0.5, 0.6, 0.4])),
            ],
        )
        .unwrap();
        let shared_base = ProofTerm {
            source_rule: "hub".to_string(),
            base_fact_id: b"hub-fact-x".to_vec(),
        };
        // Rows 0 and 1 share one base fact; row 2 has no support at all.
        let mut support = BodySupportMap::new(vec!["k".to_string(), "w".to_string()]);
        let columns = support.resolve(&batch.schema()).unwrap();
        for row in [0usize, 1] {
            support.insert(&batch, &columns, row, vec![shared_base.clone()]);
        }
        let ctx = TopKFoldCtx {
            k: 0,
            batch: &batch,
            body_support_map: Some(&support),
        };

        let p = topk_dnf_disjunction(batch.column(1).as_ref(), &[0, 1, 2], false, &ctx)
            .unwrap()
            .expect("a non-empty group folds to a probability");

        // The shared base carries the larger of its rows' weights (0.6), so the
        // DNF over the two supported proofs is 0.6; the unsupported 0.4 row
        // joins by independent noisy-OR.
        let expected = 1.0 - (1.0 - 0.6) * (1.0 - 0.4);
        assert!(
            (p - expected).abs() < 1e-12,
            "mixed group should fold to {expected}, got {p} (1.0 means the empty \
             clause of the unsupported row entered the DNF)"
        );
    }

    /// The rows of the mixed-support group above with two rows sharing a
    /// base fact, the support map built over columns `(k, w)`.
    fn shared_support_map() -> BodySupportMap {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("w", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a")])),
                Arc::new(Float64Array::from(vec![0.5, 0.6])),
            ],
        )
        .unwrap();
        let shared_base = ProofTerm {
            source_rule: "hub".to_string(),
            base_fact_id: b"hub-fact-x".to_vec(),
        };
        let mut support = BodySupportMap::new(vec!["k".to_string(), "w".to_string()]);
        let columns = support.resolve(&batch.schema()).unwrap();
        for row in 0..2 {
            support.insert(&batch, &columns, row, vec![shared_base.clone()]);
        }
        support
    }

    /// A FOLD input batch with columns `names` over the rows `("a", 0.5)`
    /// and `("a", 0.6)`, in that column order.
    fn fold_input(names: [&str; 2], k_first: bool) -> RecordBatch {
        let k: arrow_array::ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("a")]));
        let w: arrow_array::ArrayRef = Arc::new(Float64Array::from(vec![0.5, 0.6]));
        let (fields, cols) = if k_first {
            (
                vec![
                    Field::new(names[0], DataType::Utf8, true),
                    Field::new(names[1], DataType::Float64, true),
                ],
                vec![k, w],
            )
        } else {
            (
                vec![
                    Field::new(names[1], DataType::Float64, true),
                    Field::new(names[0], DataType::Utf8, true),
                ],
                vec![w, k],
            )
        };
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    /// The support map resolves its columns by name, so a FOLD input with
    /// its columns in another order still finds each row's support.
    ///
    /// Keyed by a positional hash over every column, the reordered batch
    /// missed both rows and folded them as independent: 1 - 0.5 * 0.4 = 0.8
    /// instead of the shared-proof 0.6.
    #[test]
    fn topk_support_map_resolves_columns_by_name_under_reorder() {
        let support = shared_support_map();
        let batch = fold_input(["k", "w"], false);
        let w_col = batch.schema().index_of("w").unwrap();
        let ctx = TopKFoldCtx {
            k: 0,
            batch: &batch,
            body_support_map: Some(&support),
        };
        let p = topk_dnf_disjunction(batch.column(w_col).as_ref(), &[0, 1], false, &ctx)
            .unwrap()
            .expect("a non-empty group folds to a probability");
        assert!(
            (p - 0.6).abs() < 1e-12,
            "the two rows share one base fact, so the group is 0.6; got {p} \
             (0.8 means the reordered rows missed their support)"
        );
    }

    /// A FOLD input that lacks a column the support map was built over is an
    /// error, not a silent fold under independence.
    #[test]
    fn topk_support_map_fails_closed_on_a_renamed_column() {
        let support = shared_support_map();
        let batch = fold_input(["k_alias", "w"], true);
        let ctx = TopKFoldCtx {
            k: 0,
            batch: &batch,
            body_support_map: Some(&support),
        };
        let err = topk_dnf_disjunction(batch.column(1).as_ref(), &[0, 1], false, &ctx)
            .expect_err("a support column missing from the FOLD input must fail");
        assert!(
            err.to_string().contains("'k'"),
            "the error names the missing column: {err}"
        );
    }
}
