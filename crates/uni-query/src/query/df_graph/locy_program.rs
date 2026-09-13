// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Top-level Locy program executor.
//!
//! `LocyProgramExec` orchestrates the full evaluation of a Locy program:
//! it evaluates strata in dependency order, runs fixpoint for recursive strata,
//! applies post-fixpoint operators (FOLD, PRIORITY, BEST BY), and then
//! executes the program's commands (goal queries, DERIVE, ASSUME, etc.).

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    collect_all_partitions, compute_plan_properties, execute_locy_clause_body, execute_subplan,
};
use crate::query::df_graph::locy_best_by::SortCriterion;
use crate::query::df_graph::locy_explain::ProvenanceStore;
use crate::query::df_graph::locy_fixpoint::{
    DerivedScanRegistry, DerivedScanView, FixpointClausePlan, FixpointExec, FixpointRulePlan,
    IsRefBinding,
};
use crate::query::df_graph::locy_fold::{FoldBinding, resolve_locy_aggregate};
use crate::query::df_graph::locy_profile::{
    LocyExecProfile, LocyProfileCollector, LocyStratumProfile,
};
use crate::query::executor::core::OperatorStats;
use crate::query::planner_locy_types::{
    LocyCommand, LocyIsRef, LocyRulePlan, LocyStratum, LocyYieldColumn,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::Expr;
use uni_locy::{
    ClassifierRegistry, CommandResult, FactRow, ModelInvocationCache, RuntimeWarning, SemiringKind,
};
use uni_plugin::PluginRegistry;
use uni_store::storage::manager::StorageManager;

// ---------------------------------------------------------------------------
// DerivedStore — cross-stratum fact sharing
// ---------------------------------------------------------------------------

/// Simple store for derived relation facts across strata.
///
/// Each rule's converged facts are stored here after its stratum completes,
/// making them available for later strata that depend on them.
pub struct DerivedStore {
    relations: HashMap<String, Vec<RecordBatch>>,
}

impl Default for DerivedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DerivedStore {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
        }
    }

    pub fn insert(&mut self, rule_name: String, facts: Vec<RecordBatch>) {
        self.relations.insert(rule_name, facts);
    }

    pub fn get(&self, rule_name: &str) -> Option<&Vec<RecordBatch>> {
        self.relations.get(rule_name)
    }

    /// Look up a rule's facts, tolerating the bare/qualified split that
    /// `MODULE` introduces.
    ///
    /// Inside `MODULE m`, writing `adult` *is* how you name `m.adult`. The
    /// compiler makes that work by offering every outer rule under both
    /// spellings in the catalog rather than by qualifying the referring rule —
    /// see the note in `uni-locy/src/compiler/mod.rs` explaining why handing
    /// the body the outer module context would break unqualified lookups
    /// elsewhere. The catalog is aliased, so the fact store has to resolve the
    /// same way, or an IS-ref compiles cleanly and then silently matches
    /// nothing at run time.
    ///
    /// An exact hit always wins. A bare name falls back to a unique qualified
    /// rule with that suffix; **ambiguity is not guessed at** — if two modules
    /// export the same bare name this returns `None` rather than picking one.
    /// The bare-vs-qualified policy itself lives in [`uni_locy::names`], shared
    /// with the registry-stratum pruner so the two cannot drift apart.
    pub fn get_module_aware(&self, rule_name: &str) -> Option<&Vec<RecordBatch>> {
        if let Some(facts) = self.relations.get(rule_name) {
            return Some(facts);
        }
        let key =
            uni_locy::names::resolve_unique(self.relations.keys().map(String::as_str), rule_name)?;
        self.relations.get(key)
    }

    pub fn fact_count(&self, rule_name: &str) -> usize {
        self.relations
            .get(rule_name)
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum())
            .unwrap_or(0)
    }

    pub fn rule_names(&self) -> impl Iterator<Item = &str> {
        self.relations.keys().map(|s| s.as_str())
    }
}

// ---------------------------------------------------------------------------
// LocyProgramExec — DataFusion ExecutionPlan
// ---------------------------------------------------------------------------

/// DataFusion `ExecutionPlan` that runs an entire Locy program.
///
/// Evaluates strata in dependency order, using `FixpointExec` for recursive
/// strata and direct subplan execution for non-recursive ones. After all
/// strata converge, dispatches commands.
pub struct LocyProgramExec {
    strata: Vec<LocyStratum>,
    commands: Vec<LocyCommand>,
    derived_scan_registry: Arc<DerivedScanRegistry>,
    plugin_registry: Arc<PluginRegistry>,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: Arc<StorageManager>,
    schema_info: Arc<UniSchema>,
    params: HashMap<String, Value>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    max_iterations: usize,
    timeout: Duration,
    max_derived_bytes: usize,
    deterministic_best_by: bool,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    exact_probability: bool,
    max_bdd_variables: usize,
    /// Active probability semiring (rollout D-7). Defaults to `AddMultProb`
    /// — the Phase 1/2 byte-identical behavior.
    semiring_kind: SemiringKind,
    /// Phase B Slice 3: runtime registry of `NeuralClassifier` impls
    /// keyed by model name. Held by `Arc` so executor clones share the
    /// same map.
    classifier_registry: Arc<ClassifierRegistry>,
    /// Phase B follow-up: optional memoization cache for classifier
    /// outputs. `None` → no caching.
    classifier_cache: Option<Arc<ModelInvocationCache>>,
    /// Phase C B1-B3 follow-up: per-query side-channel store for
    /// (raw, calibrated, confidence_band) records. Threaded to
    /// `FixpointExec` so EXPLAIN can read from it.
    classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    /// Shared slot for extracting the DerivedStore after execution completes.
    derived_store_slot: Arc<StdRwLock<Option<DerivedStore>>>,
    /// Shared slot for groups where BDD fell back to independence mode.
    approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
    /// Optional provenance tracker injected after construction (via `set_derivation_tracker`).
    derivation_tracker: Arc<StdRwLock<Option<Arc<ProvenanceStore>>>>,
    /// Shared slot written with per-rule iteration counts after fixpoint convergence.
    iteration_counts_slot: Arc<StdRwLock<HashMap<String, usize>>>,
    /// Shared slot written with peak memory bytes after fixpoint completes.
    peak_memory_slot: Arc<StdRwLock<usize>>,
    /// Shared slot for runtime warnings collected during evaluation.
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
    /// Shared slot for inline command results (QUERY, Cypher) executed inside `run_program()`.
    command_results_slot: Arc<StdRwLock<Vec<(usize, CommandResult)>>>,
    /// Top-k proof filtering: 0 = unlimited (default), >0 = retain at most k proofs per fact.
    top_k_proofs: usize,
    /// Shared interruption signal (see [`interruption`]): `interruption::NONE`
    /// while running, non-zero once the stratum loop or fixpoint is cut short.
    /// Decoded after execution to populate `incomplete_slot`.
    timeout_flag: Arc<std::sync::atomic::AtomicU8>,
    /// Shared slot populated when evaluation stops before completing. Holds the
    /// stop reason plus the skipped / unsound-complement rule lists; read after
    /// execution to populate `LocyResult.incomplete`. `None` for a complete run.
    incomplete_slot: Arc<StdRwLock<Option<uni_common::LocyIncomplete>>>,
    /// Whether to collect a structured execution profile. `false` for a plain
    /// `run()` (zero overhead); set to `true` by the `profile()` path. Atomic so
    /// it can be toggled through `&self` after construction (the exec is held
    /// behind an `Arc`), mirroring `set_derivation_tracker`.
    profile_enabled: std::sync::atomic::AtomicBool,
    /// Shared slot written with the structured execution profile when
    /// `profile_enabled` is set. Read after execution to build `LocyProfileOutput`.
    profile_slot: Arc<StdRwLock<Option<LocyExecProfile>>>,
}

/// Encoding for the shared interruption signal threaded through the stratum
/// loop and the recursive fixpoint as an `Arc<AtomicU8>`.
///
/// A single atomic byte records *why* evaluation stopped so the two layers can
/// agree on a reason without a second channel. `NONE` means "running or
/// completed normally".
pub(crate) mod interruption {
    use std::sync::atomic::{AtomicU8, Ordering};

    use uni_common::LocyIncompleteReason;

    /// No interruption: evaluation is running or completed normally.
    pub(crate) const NONE: u8 = 0;
    /// The wall-clock `timeout` budget was exhausted.
    pub(crate) const TIMEOUT: u8 = 1;
    /// A recursive stratum hit `max_iterations` without converging.
    pub(crate) const ITERATION_LIMIT: u8 = 2;

    /// Decodes the current interruption reason, if any.
    pub(crate) fn reason(flag: &AtomicU8) -> Option<LocyIncompleteReason> {
        match flag.load(Ordering::Relaxed) {
            TIMEOUT => Some(LocyIncompleteReason::Timeout),
            ITERATION_LIMIT => Some(LocyIncompleteReason::IterationLimit),
            _ => None,
        }
    }

    /// Records an interruption reason. First reason wins: a later, lower-priority
    /// signal (non-convergence) never overwrites an earlier wall-clock timeout,
    /// preserving the original precedence.
    pub(crate) fn set(flag: &AtomicU8, code: u8) {
        let _ = flag.compare_exchange(NONE, code, Ordering::Relaxed, Ordering::Relaxed);
    }
}

impl fmt::Debug for LocyProgramExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocyProgramExec")
            .field("strata_count", &self.strata.len())
            .field("commands_count", &self.commands.len())
            .field("max_iterations", &self.max_iterations)
            .field("timeout", &self.timeout)
            .field("output_schema", &self.output_schema)
            .field("max_derived_bytes", &self.max_derived_bytes)
            .finish_non_exhaustive()
    }
}

impl LocyProgramExec {
    #[expect(
        clippy::too_many_arguments,
        reason = "execution plan node requires full graph and session context"
    )]
    #[deprecated(
        note = "use `new_with_semiring_classifiers_and_cache` (or the lighter \
                `new_with_semiring_and_classifiers` / `new_with_semiring`) — \
                this legacy ctor defaults the semiring to AddMultProb and \
                ships no classifier registry. To be removed after C0 Stage 2."
    )]
    pub fn new(
        strata: Vec<LocyStratum>,
        commands: Vec<LocyCommand>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        plugin_registry: Arc<PluginRegistry>,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        output_schema: SchemaRef,
        max_iterations: usize,
        timeout: Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
    ) -> Self {
        Self::new_with_semiring_and_classifiers(
            strata,
            commands,
            derived_scan_registry,
            plugin_registry,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            SemiringKind::AddMultProb,
            Arc::new(ClassifierRegistry::new()),
        )
    }

    /// Constructor accepting an explicit semiring. Empty classifier
    /// registry; for the full Slice 3 variant call
    /// [`Self::new_with_semiring_and_classifiers`].
    #[expect(
        clippy::too_many_arguments,
        reason = "execution plan node requires full graph and session context"
    )]
    pub fn new_with_semiring(
        strata: Vec<LocyStratum>,
        commands: Vec<LocyCommand>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        plugin_registry: Arc<PluginRegistry>,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        output_schema: SchemaRef,
        max_iterations: usize,
        timeout: Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: SemiringKind,
    ) -> Self {
        Self::new_with_semiring_and_classifiers(
            strata,
            commands,
            derived_scan_registry,
            plugin_registry,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            semiring_kind,
            Arc::new(ClassifierRegistry::new()),
        )
    }

    /// Phase B Slice 3 entry: accepts both the semiring kind and the
    /// runtime classifier registry.
    #[expect(
        clippy::too_many_arguments,
        reason = "execution plan node requires full graph and session context"
    )]
    pub fn new_with_semiring_and_classifiers(
        strata: Vec<LocyStratum>,
        commands: Vec<LocyCommand>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        plugin_registry: Arc<PluginRegistry>,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        output_schema: SchemaRef,
        max_iterations: usize,
        timeout: Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: SemiringKind,
        classifier_registry: Arc<ClassifierRegistry>,
    ) -> Self {
        Self::new_with_semiring_classifiers_and_cache(
            strata,
            commands,
            derived_scan_registry,
            plugin_registry,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            semiring_kind,
            classifier_registry,
            None,
            None,
        )
    }

    /// Phase B follow-up: full constructor accepting the optional
    /// memoization cache. Existing callers default to `None` (no
    /// cache); `impl_locy.rs` threads `LocyConfig.classifier_cache`
    /// here.
    #[expect(
        clippy::too_many_arguments,
        reason = "execution plan node requires full graph and session context"
    )]
    pub fn new_with_semiring_classifiers_and_cache(
        strata: Vec<LocyStratum>,
        commands: Vec<LocyCommand>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        plugin_registry: Arc<PluginRegistry>,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        output_schema: SchemaRef,
        max_iterations: usize,
        timeout: Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: SemiringKind,
        classifier_registry: Arc<ClassifierRegistry>,
        classifier_cache: Option<Arc<ModelInvocationCache>>,
        classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    ) -> Self {
        let properties = compute_plan_properties(Arc::clone(&output_schema));
        Self {
            strata,
            commands,
            derived_scan_registry,
            plugin_registry,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            semiring_kind,
            classifier_registry,
            classifier_cache,
            classifier_provenance_store,
            derived_store_slot: Arc::new(StdRwLock::new(None)),
            approximate_slot: Arc::new(StdRwLock::new(HashMap::new())),
            derivation_tracker: Arc::new(StdRwLock::new(None)),
            iteration_counts_slot: Arc::new(StdRwLock::new(HashMap::new())),
            peak_memory_slot: Arc::new(StdRwLock::new(0)),
            warnings_slot: Arc::new(StdRwLock::new(Vec::new())),
            command_results_slot: Arc::new(StdRwLock::new(Vec::new())),
            top_k_proofs,
            timeout_flag: Arc::new(std::sync::atomic::AtomicU8::new(interruption::NONE)),
            incomplete_slot: Arc::new(StdRwLock::new(None)),
            profile_enabled: std::sync::atomic::AtomicBool::new(false),
            profile_slot: Arc::new(StdRwLock::new(None)),
        }
    }

    /// Enable structured execution profiling for this run.
    ///
    /// Must be called before `execute()`. When enabled, the stratum loop records
    /// per-stratum / per-rule / per-iteration timing, delta facts, and
    /// clause-body operator metrics into [`Self::profile_slot`]. Uses interior
    /// mutability so it works through `&self` (the exec is held behind an `Arc`).
    pub fn set_profile_enabled(&self, enabled: bool) {
        self.profile_enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns the shared execution-profile slot, populated after execution when
    /// profiling was enabled.
    pub fn profile_slot(&self) -> Arc<StdRwLock<Option<LocyExecProfile>>> {
        Arc::clone(&self.profile_slot)
    }

    /// Returns a shared handle to the derived store slot.
    ///
    /// After execution completes, the slot contains the `DerivedStore` with all
    /// converged facts. Read it with `slot.read().unwrap()`.
    pub fn derived_store_slot(&self) -> Arc<StdRwLock<Option<DerivedStore>>> {
        Arc::clone(&self.derived_store_slot)
    }

    /// Inject a `ProvenanceStore` to record provenance during fixpoint iteration.
    ///
    /// Must be called before `execute()` is invoked (i.e., before DataFusion runs
    /// the physical plan). Uses interior mutability so it works through `&self`.
    pub fn set_derivation_tracker(&self, tracker: Arc<ProvenanceStore>) {
        if let Ok(mut guard) = self.derivation_tracker.write() {
            *guard = Some(tracker);
        }
    }

    /// Returns the shared iteration counts slot.
    ///
    /// After execution, the slot contains per-rule iteration counts from the
    /// most recent fixpoint convergence. Sum the values for `total_iterations`.
    pub fn iteration_counts_slot(&self) -> Arc<StdRwLock<HashMap<String, usize>>> {
        Arc::clone(&self.iteration_counts_slot)
    }

    /// Returns the shared peak memory slot.
    ///
    /// After execution, the slot contains the peak byte count of derived facts
    /// across all strata. Read it with `slot.read().unwrap()`.
    pub fn peak_memory_slot(&self) -> Arc<StdRwLock<usize>> {
        Arc::clone(&self.peak_memory_slot)
    }

    /// Returns the shared runtime warnings slot.
    ///
    /// After execution, the slot contains warnings collected during fixpoint
    /// iteration (e.g. shared probabilistic dependencies).
    pub fn warnings_slot(&self) -> Arc<StdRwLock<Vec<RuntimeWarning>>> {
        Arc::clone(&self.warnings_slot)
    }

    /// Returns the shared approximate groups slot.
    ///
    /// After execution, the slot contains rule→key group descriptions for
    /// groups where BDD computation fell back to independence mode.
    pub fn approximate_slot(&self) -> Arc<StdRwLock<HashMap<String, Vec<String>>>> {
        Arc::clone(&self.approximate_slot)
    }

    /// Returns the shared command results slot.
    ///
    /// After execution, the slot contains `(command_index, CommandResult)` pairs
    /// for commands that were executed inline by `run_program()` (QUERY, Cypher).
    pub fn command_results_slot(&self) -> Arc<StdRwLock<Vec<(usize, CommandResult)>>> {
        Arc::clone(&self.command_results_slot)
    }

    /// Returns the shared interruption signal.
    ///
    /// After execution, a non-zero value means the evaluation was cut short
    /// (timeout or iteration limit) and the derived store holds partial results.
    /// Prefer [`LocyProgramExec::incomplete_slot`] for the decoded diagnostics.
    pub fn timeout_flag(&self) -> Arc<std::sync::atomic::AtomicU8> {
        Arc::clone(&self.timeout_flag)
    }

    /// Returns the shared incomplete-evaluation diagnostics slot.
    ///
    /// After execution, `Some(detail)` means evaluation stopped before
    /// completing; `detail` names the skipped / unsound-complement rules and the
    /// stop reason. `None` for a complete run.
    pub fn incomplete_slot(&self) -> Arc<StdRwLock<Option<uni_common::LocyIncomplete>>> {
        Arc::clone(&self.incomplete_slot)
    }
}

impl DisplayAs for LocyProgramExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LocyProgramExec: strata={}, commands={}, max_iter={}, timeout={:?}",
            self.strata.len(),
            self.commands.len(),
            self.max_iterations,
            self.timeout,
        )
    }
}

impl ExecutionPlan for LocyProgramExec {
    fn name(&self) -> &str {
        "LocyProgramExec"
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "LocyProgramExec has no children".to_string(),
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

        let strata = self.strata.clone();
        let registry = Arc::clone(&self.derived_scan_registry);
        let plugin_registry = Arc::clone(&self.plugin_registry);
        let graph_ctx = Arc::clone(&self.graph_ctx);
        let session_ctx = Arc::clone(&self.session_ctx);
        let storage = Arc::clone(&self.storage);
        let schema_info = Arc::clone(&self.schema_info);
        let params = self.params.clone();
        let output_schema = Arc::clone(&self.output_schema);
        let max_iterations = self.max_iterations;
        let timeout = self.timeout;
        let max_derived_bytes = self.max_derived_bytes;
        let deterministic_best_by = self.deterministic_best_by;
        let strict_probability_domain = self.strict_probability_domain;
        let probability_epsilon = self.probability_epsilon;
        let exact_probability = self.exact_probability;
        let max_bdd_variables = self.max_bdd_variables;
        let derived_store_slot = Arc::clone(&self.derived_store_slot);
        let approximate_slot = Arc::clone(&self.approximate_slot);
        let iteration_counts_slot = Arc::clone(&self.iteration_counts_slot);
        let peak_memory_slot = Arc::clone(&self.peak_memory_slot);
        let derivation_tracker = self.derivation_tracker.read().ok().and_then(|g| g.clone());
        let warnings_slot = Arc::clone(&self.warnings_slot);
        let commands = self.commands.clone();
        let command_results_slot = Arc::clone(&self.command_results_slot);
        let top_k_proofs = self.top_k_proofs;
        let timeout_flag = Arc::clone(&self.timeout_flag);
        let incomplete_slot = Arc::clone(&self.incomplete_slot);
        let semiring_kind = self.semiring_kind;
        let classifier_registry = Arc::clone(&self.classifier_registry);
        let classifier_cache = self.classifier_cache.as_ref().map(Arc::clone);
        let classifier_provenance_store = self.classifier_provenance_store.as_ref().map(Arc::clone);
        let profile_enabled = self
            .profile_enabled
            .load(std::sync::atomic::Ordering::Relaxed);
        let profile_slot = Arc::clone(&self.profile_slot);

        let fut = async move {
            run_program(
                strata,
                commands,
                registry,
                plugin_registry,
                graph_ctx,
                session_ctx,
                storage,
                schema_info,
                params,
                output_schema,
                max_iterations,
                timeout,
                max_derived_bytes,
                deterministic_best_by,
                strict_probability_domain,
                probability_epsilon,
                exact_probability,
                max_bdd_variables,
                derived_store_slot,
                approximate_slot,
                iteration_counts_slot,
                peak_memory_slot,
                derivation_tracker,
                warnings_slot,
                command_results_slot,
                top_k_proofs,
                timeout_flag,
                incomplete_slot,
                semiring_kind,
                classifier_registry,
                classifier_cache,
                classifier_provenance_store,
                profile_enabled,
                profile_slot,
            )
            .await
        };

        Ok(Box::pin(ProgramStream {
            state: ProgramStreamState::Running(Box::pin(fut)),
            schema: Arc::clone(&self.output_schema),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// ProgramStream — async state machine for streaming results
// ---------------------------------------------------------------------------

enum ProgramStreamState {
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<Vec<RecordBatch>>> + Send>>),
    Emitting(Vec<RecordBatch>, usize),
    Done,
}

struct ProgramStream {
    state: ProgramStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for ProgramStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let metrics = this.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            match &mut this.state {
                ProgramStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batches)) => {
                        if batches.is_empty() {
                            this.state = ProgramStreamState::Done;
                            return Poll::Ready(None);
                        }
                        this.state = ProgramStreamState::Emitting(batches, 0);
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = ProgramStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ProgramStreamState::Emitting(batches, idx) => {
                    if *idx >= batches.len() {
                        this.state = ProgramStreamState::Done;
                        return Poll::Ready(None);
                    }
                    let batch = batches[*idx].clone();
                    *idx += 1;
                    this.metrics.record_output(batch.num_rows());
                    return Poll::Ready(Some(Ok(batch)));
                }
                ProgramStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for ProgramStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// ---------------------------------------------------------------------------
// Inline command execution helpers
// ---------------------------------------------------------------------------

/// Execute Cypher passthrough via execute_subplan.
async fn execute_cypher_inline(
    query: &uni_cypher::ast::Query,
    schema_info: &Arc<UniSchema>,
    params: &HashMap<String, Value>,
    graph_ctx: &Arc<GraphExecutionContext>,
    session_ctx: &Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: &Arc<StorageManager>,
) -> DFResult<Vec<FactRow>> {
    let planner = crate::query::planner::QueryPlanner::new(Arc::clone(schema_info));
    let logical_plan = planner.plan(query.clone()).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Cypher plan error: {e}"))
    })?;
    let batches = execute_subplan(
        &logical_plan,
        params,
        &HashMap::new(),
        graph_ctx,
        session_ctx,
        storage,
        schema_info,
        None, // Locy paths are read-only (queries + fact extraction)
    )
    .await?;
    Ok(super::locy_eval::record_batches_to_locy_rows(&batches))
}

// ---------------------------------------------------------------------------
// run_program — core evaluation algorithm
// ---------------------------------------------------------------------------

#[expect(
    clippy::too_many_arguments,
    reason = "program evaluation requires full graph and session context"
)]
async fn run_program(
    strata: Vec<LocyStratum>,
    commands: Vec<LocyCommand>,
    registry: Arc<DerivedScanRegistry>,
    plugin_registry: Arc<PluginRegistry>,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: Arc<StorageManager>,
    schema_info: Arc<UniSchema>,
    params: HashMap<String, Value>,
    output_schema: SchemaRef,
    max_iterations: usize,
    timeout: Duration,
    max_derived_bytes: usize,
    deterministic_best_by: bool,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    exact_probability: bool,
    max_bdd_variables: usize,
    derived_store_slot: Arc<StdRwLock<Option<DerivedStore>>>,
    approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
    iteration_counts_slot: Arc<StdRwLock<HashMap<String, usize>>>,
    peak_memory_slot: Arc<StdRwLock<usize>>,
    derivation_tracker: Option<Arc<ProvenanceStore>>,
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
    command_results_slot: Arc<StdRwLock<Vec<(usize, CommandResult)>>>,
    top_k_proofs: usize,
    timeout_flag: Arc<std::sync::atomic::AtomicU8>,
    incomplete_slot: Arc<StdRwLock<Option<uni_common::LocyIncomplete>>>,
    semiring_kind: SemiringKind,
    classifier_registry: Arc<ClassifierRegistry>,
    classifier_cache: Option<Arc<ModelInvocationCache>>,
    classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    profile_enabled: bool,
    profile_slot: Arc<StdRwLock<Option<LocyExecProfile>>>,
) -> DFResult<Vec<RecordBatch>> {
    let start = Instant::now();
    let mut derived_store = DerivedStore::new();
    // Per-stratum profile rows, accumulated only when profiling is enabled.
    let mut stratum_profiles: Vec<LocyStratumProfile> = Vec::new();

    // IMPORTANT: per rollout D-9 the FuzzyNotProbabilistic warning is
    // unsuppressible. Emit one warning per PROB-bearing rule at program
    // start under MaxMinProb. The recursive path in `run_fixpoint_loop`
    // dedups against this set.
    if semiring_kind == SemiringKind::MaxMinProb {
        let mut warnings = warnings_slot.write().unwrap_or_else(|e| e.into_inner());
        let mut already: std::collections::HashSet<String> = warnings
            .iter()
            .filter(|w| w.code == uni_locy::RuntimeWarningCode::FuzzyNotProbabilistic)
            .map(|w| w.rule_name.clone())
            .collect();
        for stratum in &strata {
            for rule in &stratum.rules {
                let has_prob = rule.yield_schema.iter().any(|c| c.is_prob);
                if has_prob && !already.contains(&rule.name) {
                    warnings.push(RuntimeWarning {
                        code: uni_locy::RuntimeWarningCode::FuzzyNotProbabilistic,
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
                    already.insert(rule.name.clone());
                }
            }
        }
    }

    // Evaluate each stratum in topological order, tracking how far we get so an
    // interruption can distinguish rules left incomplete (partial fixpoint) from
    // rules never reached (skipped) — neither is "genuinely empty".
    let total_strata = strata.len();
    let mut completed_strata = 0usize;
    let mut partial_stratum: Option<usize> = None;
    for (stratum_idx, stratum) in strata.iter().enumerate() {
        // Write cross-stratum facts into registry handles for strata we depend on
        write_cross_stratum_facts(&registry, &derived_store, stratum);

        // Profiling (profile() path only): time this stratum and collect its
        // per-rule, per-iteration detail. `None` → zero overhead on `run()`.
        let stratum_start = Instant::now();
        let collector = profile_enabled.then(|| Arc::new(LocyProfileCollector::default()));

        let remaining_timeout = timeout.saturating_sub(start.elapsed());
        if remaining_timeout.is_zero() {
            tracing::warn!("Locy program timeout exceeded during stratum evaluation");
            interruption::set(&timeout_flag, interruption::TIMEOUT);
            break;
        }

        if stratum.is_recursive {
            // Convert LocyRulePlan → FixpointRulePlan and run fixpoint
            let fixpoint_rules = convert_to_fixpoint_plans(
                &stratum.rules,
                &registry,
                &plugin_registry,
                deterministic_best_by,
            )?;
            let fixpoint_schema = build_fixpoint_output_schema(&stratum.rules);

            let mut exec = FixpointExec::new_with_semiring_classifiers_and_cache(
                fixpoint_rules,
                max_iterations,
                remaining_timeout,
                Arc::clone(&graph_ctx),
                Arc::clone(&session_ctx),
                Arc::clone(&storage),
                Arc::clone(&schema_info),
                params.clone(),
                Arc::clone(&registry),
                fixpoint_schema,
                max_derived_bytes,
                derivation_tracker.clone(),
                Arc::clone(&iteration_counts_slot),
                strict_probability_domain,
                probability_epsilon,
                exact_probability,
                max_bdd_variables,
                Arc::clone(&warnings_slot),
                Arc::clone(&approximate_slot),
                top_k_proofs,
                Arc::clone(&timeout_flag),
                semiring_kind,
                Arc::clone(&classifier_registry),
                classifier_cache.as_ref().map(Arc::clone),
                classifier_provenance_store.as_ref().map(Arc::clone),
            );

            if let Some(ref c) = collector {
                exec.set_profile_collector(Arc::clone(c));
            }
            let task_ctx = session_ctx.read().task_ctx();
            let exec_arc: Arc<dyn ExecutionPlan> = Arc::new(exec);
            let batches = collect_all_partitions(&exec_arc, task_ctx).await?;

            // FixpointExec concatenates all rules' output; store per-rule.
            // For now, store all output under each rule name (since FixpointExec
            // handles per-rule state internally, the output is already correct).
            // NOTE(deferred): Per-rule fact demultiplexing is not yet implemented.
            // FixpointExec concatenates all rules' output into a single batch stream.
            // Proper demux requires FixpointExec to tag output batches with rule identity
            // (e.g. an extra column or side-channel), which is a non-trivial change to
            // run_fixpoint_loop. The current schema-field-count heuristic (filter below)
            // works because recursive stratum rules share compatible schemas.
            // Revisit when cross-stratum consumption of individual recursive rules is needed.
            for rule in &stratum.rules {
                // Skip DERIVE-only rules (empty yield_schema).
                if rule.yield_schema.is_empty() {
                    continue;
                }
                // Write converged facts into registry handles for cross-stratum consumers
                let rule_entries = registry.entries_for_rule(&rule.name);
                for entry in rule_entries {
                    if !entry.is_self_ref {
                        // Cross-stratum handles get the full fixpoint output
                        // In practice, FixpointExec already wrote self-ref handles;
                        // we need to write non-self-ref handles for later strata.
                        let all_facts: Vec<RecordBatch> = batches
                            .iter()
                            .filter(|b| {
                                // If schemas match, this batch belongs to this rule
                                let rule_schema = yield_columns_to_arrow_schema(&rule.yield_schema);
                                b.schema().fields().len() == rule_schema.fields().len()
                            })
                            .cloned()
                            .collect();
                        let mut guard = entry.data.write();
                        *guard = if all_facts.is_empty() {
                            vec![RecordBatch::new_empty(Arc::clone(&entry.schema))]
                        } else {
                            all_facts
                        };
                    }
                }
                derived_store.insert(rule.name.clone(), batches.clone());
            }
        } else {
            // Non-recursive: single-pass evaluation
            let fixpoint_rules = convert_to_fixpoint_plans(
                &stratum.rules,
                &registry,
                &plugin_registry,
                deterministic_best_by,
            )?;
            let task_ctx = session_ctx.read().task_ctx();

            for (rule, fp_rule) in stratum.rules.iter().zip(fixpoint_rules.iter()) {
                // DERIVE-only rules have empty yield_schema (the compiler's
                // infer_yield_schema only matches RuleOutput::Yield). Skip them
                // in the fixpoint loop — DERIVE materialization is handled by
                // the DERIVE command dispatch, not by the fixpoint.
                if rule.yield_schema.is_empty() {
                    continue;
                }

                // Record the single evaluation pass for this non-recursive rule.
                // The recursive branch writes per-rule fixpoint counts to this slot;
                // a non-recursive rule is evaluated exactly once, so without this a
                // purely non-recursive program would report `total_iterations == 0`.
                if let Ok(mut counts) = iteration_counts_slot.write() {
                    counts.insert(rule.name.clone(), 1);
                }

                // Process each clause independently (per-clause IS NOT).
                // Profiling: time this rule's single pass and collect its
                // clause-body operator trees.
                let rule_start = Instant::now();
                let mut iter_ops: Vec<OperatorStats> = Vec::new();
                let mut tagged_clause_facts: Vec<(usize, Vec<RecordBatch>)> = Vec::new();
                for (clause_idx, (clause, fp_clause)) in
                    rule.clauses.iter().zip(fp_rule.clauses.iter()).enumerate()
                {
                    // Phase B A4 follow-up: the planner inserts
                    // `LocyModelInvoke` between body and `LocyProject`
                    // when the clause has neural invocations.
                    let mut batches = execute_locy_clause_body(
                        &clause.body,
                        &params,
                        &graph_ctx,
                        &session_ctx,
                        &storage,
                        &schema_info,
                        collector.is_some(),
                        &mut iter_ops,
                    )
                    .await?;

                    // Apply negated IS-ref semantics per-clause.
                    for binding in &fp_clause.is_ref_bindings {
                        if binding.negated
                            && !binding.anti_join_cols.is_empty()
                            && let Some(entry) = registry.get(binding.derived_scan_index)
                        {
                            let neg_facts = entry.data.read().clone();
                            if !neg_facts.is_empty() {
                                if binding.target_has_prob && fp_rule.prob_column_name.is_some() {
                                    let complement_col =
                                        format!("__prob_complement_{}", binding.rule_name);
                                    if let Some(prob_col) = &binding.target_prob_col {
                                        batches =
                                            super::locy_fixpoint::apply_prob_complement_composite(
                                                batches,
                                                &neg_facts,
                                                &binding.anti_join_cols,
                                                prob_col,
                                                &complement_col,
                                            )?;
                                    } else {
                                        // target_has_prob but no prob_col: fall back to anti-join.
                                        batches = super::locy_fixpoint::apply_anti_join_composite(
                                            batches,
                                            &neg_facts,
                                            &binding.anti_join_cols,
                                        )?;
                                    }
                                } else {
                                    batches = super::locy_fixpoint::apply_anti_join_composite(
                                        batches,
                                        &neg_facts,
                                        &binding.anti_join_cols,
                                    )?;
                                }
                            }
                        }
                    }

                    // Multiply complement columns into PROB per-clause.
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
                        batches = super::locy_fixpoint::multiply_prob_factors(
                            batches,
                            fp_rule.prob_column_name.as_deref(),
                            &complement_cols,
                        )?;
                    }

                    // The anti-join has run; drop its hidden subject-`_vid`
                    // columns before `write_facts_to_registry`, which would
                    // otherwise silently fall back to the widened schema.
                    batches = super::locy_complement::strip_isnot_vid_columns(batches)?;

                    tagged_clause_facts.push((clause_idx, batches));
                }

                // Record provenance and detect shared proofs for non-recursive rules.
                //
                // TODO(C0-stage2): swap `record_and_detect_lineage_nonrecursive`
                // for `TopKTag` DNF inspection when
                // `semiring_kind == TopKProofs { k }`. Library-layer
                // tag math landed in
                // `crates/uni-locy/src/top_k_proofs.rs` (Phase C C0
                // Stage 1); Stage 2 wires per-row tags through the
                // runtime so dependencies are visible here.
                //
                // Under MaxMinProb, `plus = max` is idempotent so shared
                // proofs don't double-count — skip the (misleading) warning.
                let shared_info = if semiring_kind == SemiringKind::MaxMinProb {
                    None
                } else if let Some(ref tracker) = derivation_tracker {
                    super::locy_fixpoint::record_and_detect_lineage_nonrecursive(
                        fp_rule,
                        &tagged_clause_facts,
                        tracker,
                        &warnings_slot,
                        &registry,
                        top_k_proofs,
                        super::locy_fixpoint::ClassifierRefs {
                            registry: &classifier_registry,
                            cache: classifier_cache.as_ref(),
                            provenance_store: classifier_provenance_store.as_ref(),
                        },
                        semiring_kind,
                    )
                    .await
                } else {
                    None
                };

                // Flatten tagged facts for post-fixpoint chain.
                let mut all_clause_facts: Vec<RecordBatch> = tagged_clause_facts
                    .into_iter()
                    .flat_map(|(_, batches)| batches)
                    .collect();

                // Apply BDD for shared groups if exact_probability is enabled.
                if exact_probability
                    && let Some(ref info) = shared_info
                    && let Some(ref tracker) = derivation_tracker
                {
                    all_clause_facts = super::locy_fixpoint::apply_exact_wmc(
                        all_clause_facts,
                        fp_rule,
                        info,
                        tracker,
                        max_bdd_variables,
                        &warnings_slot,
                        &approximate_slot,
                    )?;
                }

                // Apply post-fixpoint operators (PRIORITY, FOLD, BEST BY) on union.
                let facts = super::locy_fixpoint::apply_post_fixpoint_chain(
                    all_clause_facts,
                    fp_rule,
                    &task_ctx,
                    strict_probability_domain,
                    probability_epsilon,
                    semiring_kind,
                    derivation_tracker.as_ref().map(Arc::clone),
                    top_k_proofs,
                    Some(Arc::clone(&registry)),
                )
                .await?;

                // Profiling: record this rule's single non-recursive pass.
                if let Some(ref c) = collector {
                    let fact_count: usize = facts.iter().map(|b| b.num_rows()).sum();
                    c.record(
                        &rule.name,
                        0,
                        fact_count,
                        rule_start.elapsed().as_secs_f64() * 1000.0,
                        std::mem::take(&mut iter_ops),
                    );
                    c.set_final_facts(&rule.name, fact_count);
                }

                // Write facts into registry handles for later strata
                write_facts_to_registry(&registry, &rule.name, &facts);
                derived_store.insert(rule.name.clone(), facts);
            }
        }

        // Profiling: assemble this stratum's profile row from the collector.
        if let Some(c) = collector {
            let rules = c.into_rules();
            let iterations = rules.iter().map(|r| r.iterations.len()).max().unwrap_or(0);
            let facts_derived: usize = rules.iter().map(|r| r.facts).sum();
            stratum_profiles.push(LocyStratumProfile {
                index: stratum_idx,
                recursive: stratum.is_recursive,
                elapsed_ms: stratum_start.elapsed().as_secs_f64() * 1000.0,
                iterations,
                facts_derived,
                rules,
            });
        }

        // The recursive fixpoint can set the interruption flag mid-stratum (the
        // non-recursive branch cannot). Stop here either way so later strata are
        // recorded as skipped rather than passed off as empty.
        if interruption::reason(&timeout_flag).is_some() {
            partial_stratum = Some(stratum_idx);
            break;
        }
        completed_strata += 1;
    }

    // If evaluation was cut short, record which rules were left incomplete vs.
    // never reached, flagging any complement (`IS NOT`) rules among them as
    // unsound. Read by impl_locy to choose Err(LocyIncomplete) vs. Ok(partial).
    if let Some(reason) = interruption::reason(&timeout_flag) {
        let skipped_start = match partial_stratum {
            Some(i) => i + 1,
            None => completed_strata,
        };
        let incomplete_rules: Vec<String> = partial_stratum
            .map(|i| strata[i].rules.iter().map(|r| r.name.clone()).collect())
            .unwrap_or_default();
        let skipped_rules: Vec<String> = strata[skipped_start..]
            .iter()
            .flat_map(|s| s.rules.iter().map(|r| r.name.clone()))
            .collect();
        let mut complement_rules_affected = Vec::new();
        for idx in partial_stratum
            .into_iter()
            .chain(skipped_start..total_strata)
        {
            for rule in &strata[idx].rules {
                if rule
                    .clauses
                    .iter()
                    .any(|c| c.is_refs.iter().any(|r| r.negated))
                {
                    complement_rules_affected.push(rule.name.clone());
                }
            }
        }
        if let Ok(mut slot) = incomplete_slot.write() {
            *slot = Some(uni_common::LocyIncomplete {
                reason,
                elapsed_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
                limit_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                max_iterations,
                completed_strata,
                total_strata,
                incomplete_rules,
                skipped_rules,
                complement_rules_affected,
            });
        }
    }

    // Compute peak memory from derived store byte sizes
    let peak_bytes: usize = derived_store
        .relations
        .values()
        .flat_map(|batches| batches.iter())
        .map(|b| {
            b.columns()
                .iter()
                .map(|col| col.get_buffer_memory_size())
                .sum::<usize>()
        })
        .sum();
    *peak_memory_slot.write().unwrap() = peak_bytes;

    // Assemble the full execution profile when profiling was enabled.
    if profile_enabled && let Ok(mut slot) = profile_slot.write() {
        *slot = Some(LocyExecProfile {
            total_elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
            peak_memory_bytes: peak_bytes,
            strata: std::mem::take(&mut stratum_profiles),
        });
    }

    // Execute inline Cypher commands via execute_subplan.
    //
    // QUERY/DERIVE/ASSUME/EXPLAIN/ABDUCE are deferred to the orchestrator
    // because they need orchestration the inline loop cannot provide: L0
    // fork/restore, tree output, and a store that has been through
    // `enrich_vids_with_nodes`.
    //
    // This comment used to give a different reason for QUERY specifically —
    // that the DerivedStore carried inferred types which "don't preserve the
    // actual property values", so the orchestrator's SLG path had to re-derive.
    // `FixpointState::reconcile_schema` fixed that by adopting the physical
    // plan's real output schema, and QUERY now answers from the derived store
    // rather than re-deriving. Deferral is about orchestration, not types.
    //
    // Cypher commands that appear AFTER a DERIVE command are also deferred:
    // they need the ephemeral L0 overlay populated by DERIVE to see derived
    // edges, which is only available in the orchestrator's dispatch loop.
    let first_derive_idx = commands
        .iter()
        .position(|c| matches!(c, LocyCommand::Derive { .. }));
    let mut inline_results: Vec<(usize, CommandResult)> = Vec::new();
    for (cmd_idx, cmd) in commands.iter().enumerate() {
        match cmd {
            LocyCommand::Cypher { query } => {
                // Defer Cypher commands that follow a DERIVE to the dispatch loop
                // so they can read from the ephemeral L0 overlay.
                if first_derive_idx.is_some_and(|di| cmd_idx > di) {
                    continue;
                }
                let rows = execute_cypher_inline(
                    query,
                    &schema_info,
                    &params,
                    &graph_ctx,
                    &session_ctx,
                    &storage,
                )
                .await?;
                inline_results.push((cmd_idx, CommandResult::Cypher(rows)));
            }
            LocyCommand::Validate { validate } => {
                // Phase C C3: collect ground-truth pairs via a
                // MATCH+TARGET query, join with the rule's derived
                // facts on KEY columns, compute metrics.
                let rule_key_cols: Vec<String> = strata
                    .iter()
                    .flat_map(|s| s.rules.iter())
                    .find(|r| r.name == validate.rule_name)
                    .map(|r| {
                        r.yield_schema
                            .iter()
                            .filter(|c| c.is_key)
                            .map(|c| c.name.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                let query =
                    super::locy_validate::validate_collection_query(validate, &rule_key_cols);
                let target_rows = execute_cypher_inline(
                    &query,
                    &schema_info,
                    &params,
                    &graph_ctx,
                    &session_ctx,
                    &storage,
                )
                .await?;
                let rule_facts: Vec<uni_locy::FactRow> = derived_store
                    .get(&validate.rule_name)
                    .map(|batches| super::locy_eval::record_batches_to_locy_rows(batches))
                    .unwrap_or_default();
                let result = super::locy_validate::run_validate(
                    validate,
                    &rule_key_cols,
                    &rule_facts,
                    target_rows,
                )
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!("VALIDATE error: {e}"))
                })?;
                inline_results.push((cmd_idx, CommandResult::Validate(result)));
            }
            LocyCommand::Calibrate {
                calibrate,
                model_inputs,
            } => {
                // Phase C C2: dispatch a CALIBRATE command. Build a
                // Cypher MATCH+RETURN query that projects the model's
                // input variables + the TARGET expression, execute
                // it, then drive `run_calibrate` over the collected
                // rows. The fitted calibrator + holdout metrics
                // surface as `CommandResult::Calibrate(...)`.
                //
                // Synthesize a CompiledModel snapshot from the carried
                // model_inputs so we can build the collection query
                // without lugging the full catalog through this call
                // site. Other fields the runtime doesn't read are
                // filled with defaults.
                let model_snapshot = uni_locy::CompiledModel {
                    name: calibrate.model_name.clone(),
                    inputs: model_inputs.clone(),
                    features: vec![],
                    path_context: None,
                    output_type: uni_cypher::locy_ast::OutputType::Prob,
                    output_name: String::new(),
                    xervo_alias: String::new(),
                    embedder_alias: None,
                    calibration: None,
                    version: None,
                    annotations: Default::default(),
                };
                let query =
                    super::locy_calibrate::calibrate_collection_query(calibrate, &model_snapshot);
                let rows = execute_cypher_inline(
                    &query,
                    &schema_info,
                    &params,
                    &graph_ctx,
                    &session_ctx,
                    &storage,
                )
                .await?;
                let mut catalog = std::collections::HashMap::new();
                catalog.insert(calibrate.model_name.clone(), model_snapshot);
                let result = super::locy_calibrate::run_calibrate(
                    calibrate,
                    &catalog,
                    &classifier_registry,
                    rows,
                )
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!("CALIBRATE error: {e}"))
                })?;
                inline_results.push((cmd_idx, CommandResult::Calibrate(result)));
            }
            _ => {}
        }
    }
    *command_results_slot.write().unwrap() = inline_results;

    let stats = vec![build_stats_batch(&derived_store, &strata, output_schema)];
    *derived_store_slot.write().unwrap() = Some(derived_store);
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Cross-stratum fact injection
// ---------------------------------------------------------------------------

/// Write already-evaluated facts into registry handles for cross-stratum IS-refs.
fn write_cross_stratum_facts(
    registry: &DerivedScanRegistry,
    derived_store: &DerivedStore,
    stratum: &LocyStratum,
) {
    // For each rule in this stratum, find IS-refs to rules in other strata
    for rule in &stratum.rules {
        for clause in &rule.clauses {
            for is_ref in &clause.is_refs {
                // If this IS-ref points to a rule already in the derived store
                // (i.e., from a previous stratum), write its facts into the
                // registry. Module-aware: inside `MODULE m` the referring rule
                // names `adult` while the store holds `m.adult`.
                if let Some(facts) = derived_store.get_module_aware(&is_ref.rule_name) {
                    write_facts_to_registry(registry, &is_ref.rule_name, facts);
                }
            }
        }
    }
}

/// Write facts into non-self-ref registry handles for a given rule.
fn write_facts_to_registry(registry: &DerivedScanRegistry, rule_name: &str, facts: &[RecordBatch]) {
    let entries = registry.entries_for_rule(rule_name);
    for entry in entries {
        if !entry.is_self_ref {
            let mut guard = entry.data.write();
            *guard = if facts.is_empty() || facts.iter().all(|b| b.num_rows() == 0) {
                vec![RecordBatch::new_empty(Arc::clone(&entry.schema))]
            } else {
                // Try to re-wrap batches with the entry's schema for column name
                // alignment. If the types don't match (e.g. inferred Float64 vs
                // actual Utf8 from schema mode), fall back to the batch's own
                // schema to avoid silent data loss.
                facts
                    .iter()
                    .filter(|b| b.num_rows() > 0)
                    .map(|b| {
                        RecordBatch::try_new(Arc::clone(&entry.schema), b.columns().to_vec())
                            .unwrap_or_else(|_| b.clone())
                    })
                    .collect()
            };
        }
    }
}

// ---------------------------------------------------------------------------
// LocyRulePlan → FixpointRulePlan conversion
// ---------------------------------------------------------------------------

/// Convert logical `LocyRulePlan` types to physical `FixpointRulePlan` types.
fn convert_to_fixpoint_plans(
    rules: &[LocyRulePlan],
    registry: &DerivedScanRegistry,
    plugin_registry: &PluginRegistry,
    deterministic_best_by: bool,
) -> DFResult<Vec<FixpointRulePlan>> {
    // `rules` is one stratum's rule set, so membership here means
    // "same stratum" — the recursion-detection set for `non_linear`.
    let stratum_rule_names: std::collections::HashSet<&str> =
        rules.iter().map(|r| r.name.as_str()).collect();
    rules
        .iter()
        .map(|rule| {
            let yield_schema = yield_columns_to_arrow_schema(&rule.yield_schema);
            let key_column_indices: Vec<usize> = rule
                .yield_schema
                .iter()
                .enumerate()
                .filter(|(_, yc)| yc.is_key)
                .map(|(i, _)| i)
                .collect();

            let clauses: Vec<FixpointClausePlan> = rule
                .clauses
                .iter()
                .map(|clause| {
                    let is_ref_bindings =
                        convert_is_refs(&clause.is_refs, registry, &stratum_rule_names)?;
                    Ok(FixpointClausePlan {
                        body_logical: clause.body.clone(),
                        is_ref_bindings,
                        priority: clause.priority,
                        along_bindings: clause.along_bindings.clone(),
                        model_invocations: clause.model_invocations.clone(),
                    })
                })
                .collect::<DFResult<Vec<_>>>()?;

            let fold_bindings =
                convert_fold_bindings(&rule.fold_bindings, &rule.yield_schema, plugin_registry)?;
            let best_by_criteria =
                convert_best_by_criteria(&rule.best_by_criteria, &rule.yield_schema)?;

            let has_priority = rule.priority.is_some();

            // Add __priority column to yield schema if PRIORITY is used
            let yield_schema = if has_priority {
                let mut fields: Vec<Arc<Field>> = yield_schema.fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new("__priority", DataType::Int64, true)));
                ArrowSchema::new(fields)
            } else {
                yield_schema
            };

            // Derivation-discriminator columns for a recursive FOLD / ALONG rule
            // (issue #159), appended in the same order the planner projects them.
            //
            // This schema is what `FixpointState::new` consumes to build both
            // `RowDedupState` and `all_column_indices`, so appending here is what
            // actually puts the discriminators into the dedup key.
            //
            // Widening it is also what keeps provenance correct rather than
            // merely safe: `record_provenance`, `detect_shared_lineage`,
            // `apply_exact_wmc` and `find_clause_for_row` all derive their column
            // indices from this schema, so the fact hash becomes
            // discriminator-aware — the semantics we want. Were the batches
            // widened but this schema left narrow, provenance would hash only the
            // leading columns (siblings would collide again, defeating the fix)
            // and `find_clause_for_row` would skip every candidate and silently
            // report clause 0.
            let deriv_cols = &rule.deriv_columns;
            let yield_schema = if deriv_cols.is_empty() {
                yield_schema
            } else {
                let mut fields: Vec<Arc<Field>> = yield_schema.fields().iter().cloned().collect();
                for (name, dt) in deriv_cols {
                    fields.push(Arc::new(Field::new(name, dt.clone(), true)));
                }
                ArrowSchema::new(fields)
            };

            let prob_column_name = rule
                .yield_schema
                .iter()
                .find(|yc| yc.is_prob)
                .map(|yc| yc.name.clone());

            // Non-linear recursion: any clause joining ≥2 positive
            // same-stratum IS-refs needs full facts on its self-ref scans
            // (see `FixpointRulePlan::non_linear`).
            let non_linear = rule.clauses.iter().any(|clause| {
                clause
                    .is_refs
                    .iter()
                    .filter(|ir| !ir.negated && stratum_rule_names.contains(ir.rule_name.as_str()))
                    .count()
                    >= 2
            });

            Ok(FixpointRulePlan {
                name: rule.name.clone(),
                clauses,
                yield_schema: Arc::new(yield_schema),
                key_column_indices,
                priority: rule.priority,
                has_fold: !rule.fold_bindings.is_empty(),
                fold_bindings,
                require: rule.require.clone(),
                having: rule.having.clone(),
                has_best_by: !rule.best_by_criteria.is_empty(),
                best_by_criteria,
                has_priority,
                deterministic: deterministic_best_by,
                prob_column_name,
                non_linear,
                yield_projection: rule.yield_projection.clone(),
            })
        })
        .collect()
}

/// Convert `LocyIsRef` to `IsRefBinding` by looking up scan indices in the registry.
///
/// `stratum_rule_names` is the set of rule names in the stratum being converted.
/// A reference is self-referential exactly when its target is in that set — the
/// same rule the planner used to mint the handle (see `get_or_create_derived_scan_handle`).
/// Selecting the entry whose `is_self_ref` matches that decision is essential for
/// negation: a recursive rule has BOTH a self-ref handle (carrying the final,
/// usually-empty semi-naive delta) and a non-self-ref handle (carrying the
/// converged facts). An `IS NOT <recursive rule>` reference is cross-stratum
/// (`is_self_ref == false`), so it must anti-join against the converged facts —
/// not the delta, which would silently under-filter.
fn convert_is_refs(
    is_refs: &[LocyIsRef],
    registry: &DerivedScanRegistry,
    stratum_rule_names: &std::collections::HashSet<&str>,
) -> DFResult<Vec<IsRefBinding>> {
    is_refs
        .iter()
        .map(|is_ref| {
            let entries = registry.entries_for_rule(&is_ref.rule_name);
            // Select the handle matching the planner's self-ref decision for this
            // reference: same-stratum targets use the delta (self-ref) handle for
            // semi-naive evaluation; cross-stratum targets (including every IS NOT
            // against a lower-stratum recursive rule) use the converged-facts handle.
            let want_self_ref = stratum_rule_names.contains(is_ref.rule_name.as_str());
            // Always the contributions view (issue #162). This index drives the
            // IS NOT anti-join / probabilistic complement and the provenance
            // `base_fact_id` hashes, both of which are defined over pre-fold
            // rows: the folded snapshot has one row per KEY, so anti-joining
            // against it would under-filter and its row hashes would never
            // match the ones `record_provenance` stores.
            let entry = entries
                .iter()
                .find(|e| {
                    e.is_self_ref == want_self_ref && e.view == DerivedScanView::Contributions
                })
                .or_else(|| entries.iter().find(|e| e.is_self_ref == want_self_ref))
                .or_else(|| entries.first())
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "No derived scan entry found for IS-ref to '{}'",
                        is_ref.rule_name
                    ))
                })?;

            // For negated IS-refs, compute (left_body_col, right_derived_col) pairs for
            // anti-join filtering. The derived column name is taken positionally from
            // the registry entry's schema (KEY columns come first).
            //
            // The left name prefers the hidden `{var}._vid` column the planner
            // projected (`LocyIsRef::subject_vid_cols`). The anti-join runs after
            // `LocyProject`, so the bare variable name only resolves when YIELD
            // happened to project that exact name — an aliased or unprojected
            // subject was previously unresolvable and the negation silently
            // became a no-op (issue #158). The bare-name fallback remains for
            // subjects that are not MATCH-bound node variables, which have no
            // `._vid` column; those still error in `apply_anti_join_composite`
            // rather than failing open.
            let left_col = |var: &String| {
                is_ref
                    .subject_vid_cols
                    .get(var)
                    .cloned()
                    .unwrap_or_else(|| var.clone())
            };
            let anti_join_cols = if is_ref.negated {
                let mut cols: Vec<(String, String)> = is_ref
                    .subjects
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| {
                        if let uni_cypher::ast::Expr::Variable(var) = s {
                            let right_col = entry
                                .schema
                                .fields()
                                .get(i)
                                .map(|f| f.name().clone())
                                .unwrap_or_else(|| var.clone());
                            Some((left_col(var), right_col))
                        } else {
                            None
                        }
                    })
                    .collect();
                // Include target variable in anti-join for composite-key IS NOT.
                // Without this, `d IS NOT known TO dis` only checks d, not (d, dis),
                // filtering ALL pairs where the drug has ANY indication regardless
                // of disease.
                if let Some(uni_cypher::ast::Expr::Variable(target_var)) = &is_ref.target {
                    let target_idx = is_ref.subjects.len();
                    if let Some(field) = entry.schema.fields().get(target_idx) {
                        cols.push((left_col(target_var), field.name().clone()));
                    }
                }
                cols
            } else {
                Vec::new()
            };

            // Provenance join cols: for ALL IS-refs (not just negated), compute
            // (body_col, derived_col) pairs so shared-proof detection can trace
            // which source facts contributed to each derived row.
            //
            // Uses the same `left_col` resolution as the anti-join above: for a
            // negated ref whose subject was renamed by YIELD, the bare name does
            // not exist post-projection and lineage would silently attribute to
            // nothing.
            let provenance_join_cols: Vec<(String, String)> = is_ref
                .subjects
                .iter()
                .enumerate()
                .filter_map(|(i, s)| {
                    if let uni_cypher::ast::Expr::Variable(var) = s {
                        let right_col = entry
                            .schema
                            .fields()
                            .get(i)
                            .map(|f| f.name().clone())
                            .unwrap_or_else(|| var.clone());
                        Some((left_col(var), right_col))
                    } else {
                        None
                    }
                })
                .collect();

            Ok(IsRefBinding {
                derived_scan_index: entry.scan_index,
                rule_name: is_ref.rule_name.clone(),
                is_self_ref: entry.is_self_ref,
                negated: is_ref.negated,
                anti_join_cols,
                target_has_prob: is_ref.target_has_prob,
                target_prob_col: is_ref.target_prob_col.clone(),
                provenance_join_cols,
            })
        })
        .collect()
}

/// Convert fold binding expressions to physical `FoldBinding`.
///
/// The input column is looked up by the fold binding's output name (e.g., "total")
/// in the yield schema, since the LocyProject aliases the aggregate input expression
/// to the fold output name. The aggregate name is resolved against
/// `plugin_registry` to obtain the [`uni_plugin::traits::locy::LocyAggregate`]
/// trait object at plan time.
fn convert_fold_bindings(
    fold_bindings: &[(String, String, Expr)],
    yield_schema: &[LocyYieldColumn],
    plugin_registry: &PluginRegistry,
) -> DFResult<Vec<FoldBinding>> {
    fold_bindings
        .iter()
        .map(|(name, yield_alias, expr)| {
            let (agg_name, _input_col_name) = parse_fold_aggregate(expr)?;
            let entry =
                resolve_locy_aggregate(plugin_registry, agg_name.as_str()).ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Unknown Locy aggregate '{agg_name}' — not registered in plugin registry"
                    ))
                })?;
            let aggregate = Arc::clone(&entry.aggregate);

            // CountAll has no input column — LocyProject skips the output column
            // entirely, so there is nothing to look up.
            if agg_name.as_str() == "COUNTALL" {
                return Ok(FoldBinding {
                    output_name: yield_alias.clone(),
                    name: agg_name,
                    aggregate,
                    input_col_index: 0, // unused for CountAll
                    input_col_name: None,
                });
            }

            // The LocyProject projects the aggregate input expression AS the fold
            // output name, so the input column index matches the yield schema position.
            // Also store the column name for name-based resolution at execution time
            // (more robust when schema reconciliation changes column ordering).
            let input_col_index = yield_schema
                .iter()
                .position(|yc| yc.name == *name || yc.name == *yield_alias)
                .unwrap_or(0);
            Ok(FoldBinding {
                output_name: yield_alias.clone(),
                name: agg_name,
                aggregate,
                input_col_index,
                input_col_name: Some(name.clone()),
            })
        })
        .collect()
}

/// Parse a fold aggregate expression into (canonical_name, input_column_name).
///
/// Normalizes grammar aliases to canonical names: `MSUM`→`SUM`, `MMAX`→`MAX`,
/// `MMIN`→`MIN`, `MCOUNT`→`COUNT`. The zero-arg `COUNT()`/`MCOUNT()` form
/// returns the `COUNTALL` sentinel. `MNOR`/`MPROD` are already canonical.
fn parse_fold_aggregate(expr: &Expr) -> DFResult<(smol_str::SmolStr, String)> {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            let upper = name.to_uppercase();
            let is_count = matches!(upper.as_str(), "COUNT" | "MCOUNT");

            // COUNT/MCOUNT with zero args → CountAll (like SQL COUNT(*))
            if is_count && args.is_empty() {
                return Ok((smol_str::SmolStr::new_static("COUNTALL"), String::new()));
            }

            let canonical = match upper.as_str() {
                "SUM" | "MSUM" => smol_str::SmolStr::new_static("SUM"),
                "MAX" | "MMAX" => smol_str::SmolStr::new_static("MAX"),
                "MIN" | "MMIN" => smol_str::SmolStr::new_static("MIN"),
                "COUNT" | "MCOUNT" => smol_str::SmolStr::new_static("COUNT"),
                "AVG" => smol_str::SmolStr::new_static("AVG"),
                "COLLECT" => smol_str::SmolStr::new_static("COLLECT"),
                "MNOR" => smol_str::SmolStr::new_static("MNOR"),
                "MPROD" => smol_str::SmolStr::new_static("MPROD"),
                // A dotted name is a plugin-namespaced custom aggregate
                // (e.g. `myplugin.MYAGG`); pass it through raw (preserving
                // case) so `resolve_locy_aggregate` resolves it by
                // namespace. Bare unknown names remain errors (typos).
                _ if name.contains('.') => smol_str::SmolStr::new(name.as_str()),
                _ => {
                    return Err(datafusion::error::DataFusionError::Plan(format!(
                        "Unknown FOLD aggregate function: {}",
                        name
                    )));
                }
            };
            let col_name = match args.first() {
                Some(Expr::Variable(v)) => v.clone(),
                Some(Expr::Property(_, prop)) => prop.clone(),
                Some(other) => other.to_string_repr(),
                None => {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "FOLD aggregate function requires at least one argument".to_string(),
                    ));
                }
            };
            Ok((canonical, col_name))
        }
        _ => Err(datafusion::error::DataFusionError::Plan(
            "FOLD binding must be a function call (e.g., SUM(x))".to_string(),
        )),
    }
}

/// Convert best-by criteria expressions to physical `SortCriterion`.
///
/// Resolves the criteria column by trying:
/// 1. Property name (e.g., `e.cost` → "cost")
/// 2. Variable name (e.g., `cost`)
/// 3. Full expression string (e.g., "e.cost" as a variable name)
fn convert_best_by_criteria(
    criteria: &[(Expr, bool)],
    yield_schema: &[LocyYieldColumn],
) -> DFResult<Vec<SortCriterion>> {
    criteria
        .iter()
        .map(|(expr, ascending)| {
            let col_name = match expr {
                Expr::Property(_, prop) => prop.clone(),
                Expr::Variable(v) => v.clone(),
                _ => {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "BEST BY criterion must be a variable or property reference".to_string(),
                    ));
                }
            };
            // Try exact match first, then try just the last component after '.'
            let col_index = yield_schema
                .iter()
                .position(|yc| yc.name == col_name)
                .or_else(|| {
                    let short_name = col_name.rsplit('.').next().unwrap_or(&col_name);
                    yield_schema.iter().position(|yc| yc.name == short_name)
                })
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "BEST BY column '{}' not found in yield schema",
                        col_name
                    ))
                })?;
            Ok(SortCriterion {
                col_index,
                ascending: *ascending,
                nulls_first: false,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

/// Convert `LocyYieldColumn` slice to Arrow schema using inferred types.
fn yield_columns_to_arrow_schema(columns: &[LocyYieldColumn]) -> ArrowSchema {
    let fields: Vec<Arc<Field>> = columns
        .iter()
        .map(|yc| Arc::new(Field::new(&yc.name, yc.data_type.clone(), true)))
        .collect();
    ArrowSchema::new(fields)
}

/// Build a combined output schema for fixpoint (union of all rules' schemas).
fn build_fixpoint_output_schema(rules: &[LocyRulePlan]) -> SchemaRef {
    // FixpointExec concatenates all rules' output, using the first rule's schema
    // as the output schema (all rules in a recursive stratum share compatible schemas).
    if let Some(rule) = rules.first() {
        Arc::new(yield_columns_to_arrow_schema(&rule.yield_schema))
    } else {
        Arc::new(ArrowSchema::empty())
    }
}

/// Build a stats RecordBatch summarizing derived relation counts.
fn build_stats_batch(
    derived_store: &DerivedStore,
    _strata: &[LocyStratum],
    output_schema: SchemaRef,
) -> RecordBatch {
    // Build a simple stats batch with rule_name and fact_count columns
    let mut rule_names: Vec<String> = derived_store.rule_names().map(String::from).collect();
    rule_names.sort();

    let name_col: arrow_array::StringArray = rule_names.iter().map(|s| Some(s.as_str())).collect();
    let count_col: arrow_array::Int64Array = rule_names
        .iter()
        .map(|name| Some(derived_store.fact_count(name) as i64))
        .collect();

    let stats_schema = stats_schema();
    RecordBatch::try_new(stats_schema, vec![Arc::new(name_col), Arc::new(count_col)])
        .unwrap_or_else(|_| RecordBatch::new_empty(output_schema))
}

/// Schema for the stats batch returned when no commands are present.
pub fn stats_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Arc::new(Field::new("rule_name", DataType::Utf8, false)),
        Arc::new(Field::new("fact_count", DataType::Int64, false)),
    ]))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, LargeBinaryArray, StringArray};

    #[test]
    fn test_derived_store_insert_and_get() {
        let mut store = DerivedStore::new();
        assert!(store.get("test").is_none());

        let schema = Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
            "x",
            DataType::LargeBinary,
            true,
        ))]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeBinaryArray::from(vec![
                Some(b"a" as &[u8]),
                Some(b"b"),
            ]))],
        )
        .unwrap();

        store.insert("test".to_string(), vec![batch.clone()]);

        let facts = store.get("test").unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].num_rows(), 2);
    }

    #[test]
    fn test_derived_store_fact_count() {
        let mut store = DerivedStore::new();
        assert_eq!(store.fact_count("empty"), 0);

        let schema = Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
            "x",
            DataType::LargeBinary,
            true,
        ))]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeBinaryArray::from(vec![Some(b"a" as &[u8])]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeBinaryArray::from(vec![
                Some(b"b" as &[u8]),
                Some(b"c"),
            ]))],
        )
        .unwrap();

        store.insert("test".to_string(), vec![batch1, batch2]);
        assert_eq!(store.fact_count("test"), 3);
    }

    #[test]
    fn test_stats_batch_schema() {
        let schema = stats_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "rule_name");
        assert_eq!(schema.field(1).name(), "fact_count");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_stats_batch_content() {
        let mut store = DerivedStore::new();
        let schema = Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
            "x",
            DataType::LargeBinary,
            true,
        ))]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeBinaryArray::from(vec![
                Some(b"a" as &[u8]),
                Some(b"b"),
            ]))],
        )
        .unwrap();
        store.insert("reach".to_string(), vec![batch]);

        let output_schema = stats_schema();
        let stats = build_stats_batch(&store, &[], Arc::clone(&output_schema));
        assert_eq!(stats.num_rows(), 1);

        let names = stats
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "reach");

        let counts = stats
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 2);
    }

    #[test]
    fn test_yield_columns_to_arrow_schema() {
        let columns = vec![
            LocyYieldColumn {
                name: "a".to_string(),
                is_key: true,
                is_prob: false,
                data_type: DataType::UInt64,
            },
            LocyYieldColumn {
                name: "b".to_string(),
                is_key: false,
                is_prob: false,
                data_type: DataType::LargeUtf8,
            },
            LocyYieldColumn {
                name: "c".to_string(),
                is_key: true,
                is_prob: false,
                data_type: DataType::Float64,
            },
        ];

        let schema = yield_columns_to_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "a");
        assert_eq!(schema.field(1).name(), "b");
        assert_eq!(schema.field(2).name(), "c");
        // Fields use inferred types
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(1).data_type(), &DataType::LargeUtf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        for field in schema.fields() {
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn test_key_column_indices() {
        let columns = [
            LocyYieldColumn {
                name: "a".to_string(),
                is_key: true,
                is_prob: false,
                data_type: DataType::LargeBinary,
            },
            LocyYieldColumn {
                name: "b".to_string(),
                is_key: false,
                is_prob: false,
                data_type: DataType::LargeBinary,
            },
            LocyYieldColumn {
                name: "c".to_string(),
                is_key: true,
                is_prob: false,
                data_type: DataType::LargeBinary,
            },
        ];

        let key_indices: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, yc)| yc.is_key)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(key_indices, vec![0, 2]);
    }

    #[test]
    fn test_parse_fold_aggregate_sum() {
        let expr = Expr::FunctionCall {
            name: "SUM".to_string(),
            args: vec![Expr::Variable("cost".to_string())],
            distinct: false,
            window_spec: None,
        };
        let (kind, col) = parse_fold_aggregate(&expr).unwrap();
        assert_eq!(kind.as_str(), "SUM");
        assert_eq!(col, "cost");
    }

    #[test]
    fn test_parse_fold_aggregate_monotonic() {
        let expr = Expr::FunctionCall {
            name: "MMAX".to_string(),
            args: vec![Expr::Variable("score".to_string())],
            distinct: false,
            window_spec: None,
        };
        let (kind, col) = parse_fold_aggregate(&expr).unwrap();
        assert_eq!(kind.as_str(), "MAX");
        assert_eq!(col, "score");
    }

    #[test]
    fn test_parse_fold_aggregate_unknown() {
        let expr = Expr::FunctionCall {
            name: "UNKNOWN_AGG".to_string(),
            args: vec![Expr::Variable("x".to_string())],
            distinct: false,
            window_spec: None,
        };
        assert!(parse_fold_aggregate(&expr).is_err());
    }

    #[test]
    fn test_no_commands_returns_stats() {
        let store = DerivedStore::new();
        let output_schema = stats_schema();
        let stats = build_stats_batch(&store, &[], Arc::clone(&output_schema));
        // Empty store → 0 rows
        assert_eq!(stats.num_rows(), 0);
    }
}
