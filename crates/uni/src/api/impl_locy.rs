// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Locy engine integration: wires the Locy compiler and native execution engine to the real database.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use uni_common::{Result, UniError, Value};
use uni_cypher::ast::{Expr, Pattern, Query};
use uni_cypher::locy_ast::RuleOutput;
use uni_locy::types::CompiledCommand;
use uni_locy::{
    CommandResult, CompiledProgram, DerivedFactSet, FactRow, LocyCompileError, LocyConfig,
    LocyError, LocyStats, RuntimeWarning, RuntimeWarningCode,
};
use uni_query::{QueryMetrics, QueryPlanner};

use crate::api::locy_result::LocyResult;
use uni_query::query::df_graph::locy_ast_builder::build_match_return_query;
use uni_query::query::df_graph::locy_delta::{RowRelation, RowStore};
use uni_query::query::df_graph::locy_derive::CollectedDeriveOutput;
use uni_query::query::df_graph::locy_eval::record_batches_to_locy_rows;
use uni_query::query::df_graph::locy_explain::ProvenanceStore;
use uni_query::query::df_graph::locy_sequence::{
    CommandFacts, CommandSequence, SequencePrograms, SequenceScope, reads_facts,
};
use uni_query::query::df_graph::{DerivedFactSource, LocyExecutionContext};

/// Copy `parent`'s deadline and cancellation token onto a derived context.
///
/// The Locy paths build a fresh `GraphExecutionContext` when a transaction
/// changes L0 visibility. That constructor takes no budget, so the derived
/// context would start unbounded even though it stands in for the parent for
/// the rest of the query. See issue #207.
fn carry_budget(
    mut ctx: uni_query::query::df_graph::GraphExecutionContext,
    parent: &uni_query::query::df_graph::GraphExecutionContext,
) -> uni_query::query::df_graph::GraphExecutionContext {
    if let Some(deadline) = parent.deadline_for_host() {
        ctx = ctx.with_deadline(deadline);
    }
    if let Some(token) = parent.cancellation_token_for_host() {
        ctx = ctx.with_cancellation_token(token);
    }
    // Counters travel with the budget. `GraphExecutionContext::with_parts`
    // defaults them to `None`, so a context rebuilt here silently stopped
    // counting — which made `tx.locy(...)` under-report against
    // `session.locy(...)`, since only the transaction path rebuilds one.
    ctx = ctx.with_counters(parent.counters().cloned());
    ctx
}

/// Session-level registry for pre-compiled Locy rules.
///
/// Rules registered here are automatically merged into subsequent `evaluate()`
/// calls, eliminating the need to redeclare rules across multiple evaluations
/// (e.g., baseline, EXPLAIN, ASSUME, ABDUCE in notebooks).
#[derive(Debug, Default, Clone)]
pub struct LocyRuleRegistry {
    /// Compiled rules indexed by rule name.
    pub rules: HashMap<String, uni_locy::types::CompiledRule>,
    /// Strata from registered programs, for execution ordering.
    pub strata: Vec<uni_locy::types::Stratum>,
    /// Registered source programs, the durable source of truth for the
    /// registry. Compiled state is a pure function of this list.
    pub sources: Vec<crate::api::locy_rule_catalog::RegisteredSource>,
}

/// Merge a session's registered rules into a freshly compiled program.
///
/// Registered rules fill any `rule_catalog` slot the program does not already
/// define. Only the registered *strata* the program can actually reach are
/// prepended, and the program's own strata are rebased above them so the
/// registered ones run first and stratum ids stay dense.
///
/// The catalog is merged in full while the strata are filtered, because the two
/// carry different risk. A catalog entry is inert metadata consulted during
/// planning; a stratum is *evaluated*. Merging every stratum made each
/// registered rule run in every program, so a single registered rule taking a
/// parameter made that parameter mandatory for programs referencing no rule at
/// all. Filtering the catalog instead would risk manufacturing spurious
/// "unknown rule" errors, so it stays complete and every existing failure path
/// behaves exactly as before.
///
/// Reachability is computed by [`uni_locy::prune`]; a reference to a rule that
/// does not exist is already rejected at compile time, so pruning cannot turn a
/// missing rule into a silently empty result.
///
/// Every clause's binding plan is recomputed against the merged catalog, since
/// a program clause referencing a registered rule had none until now.
///
/// # Errors
///
/// A binding error from [`uni_locy::attach_binding_plans`].
pub(crate) fn merge_registered_rules(
    compiled: &mut CompiledProgram,
    registry: &LocyRuleRegistry,
) -> std::result::Result<(), LocyCompileError> {
    if registry.rules.is_empty() {
        return Ok(());
    }
    for (name, rule) in &registry.rules {
        compiled
            .rule_catalog
            .entry(name.clone())
            .or_insert_with(|| rule.clone());
    }

    let seeds = uni_locy::prune::collect_referenced_rule_names(compiled);
    let keep = uni_locy::prune::reachable_pool_strata(&seeds, &registry.strata);

    // Renumber the retained strata densely. `remap` is keyed on the original
    // `id` rather than the pool index so it stays correct if a future registry
    // path ever assigns non-dense ids. Edges to pruned strata are dropped
    // rather than left dangling; nothing reads `depends_on` at run time, but a
    // coherent graph keeps plan dumps readable.
    let mut remap: HashMap<usize, usize> = HashMap::with_capacity(keep.len());
    let mut merged_strata: Vec<uni_locy::types::Stratum> =
        Vec::with_capacity(keep.len() + compiled.strata.len());
    for (new_id, &index) in keep.iter().enumerate() {
        let mut stratum = registry.strata[index].clone();
        remap.insert(stratum.id, new_id);
        stratum.id = new_id;
        stratum.depends_on = stratum
            .depends_on
            .iter()
            .filter_map(|d| remap.get(d).copied())
            .collect();
        merged_strata.push(stratum);
    }

    let base_id = merged_strata.len();
    for stratum in &mut compiled.strata {
        stratum.id += base_id;
        stratum.depends_on = stratum.depends_on.iter().map(|d| d + base_id).collect();
    }
    merged_strata.append(&mut compiled.strata);
    compiled.strata = merged_strata;
    uni_locy::attach_binding_plans(compiled)
}

/// Refuses `compiled` when `params` lacks a parameter it needs, naming every
/// missing one ([`crate::api::impl_query::check_parameters`]).
///
/// `program_parameters` are those the program's own text references, in
/// order of first appearance. `compiled` has the registered rules merged
/// ([`merge_registered_rules`]), and a registered rule's parameters are not in
/// the program's text, so each merged rule's are added after them, in
/// stratum order. Checked before evaluation: a registered rule's parameter
/// used to surface only when its clause first evaluated it, naming that one
/// parameter alone.
///
/// # Errors
///
/// `UniError::ParameterMissing` with every missing name.
pub(crate) fn check_program_parameters(
    program_parameters: &[String],
    compiled: &CompiledProgram,
    params: &HashMap<String, Value>,
) -> Result<()> {
    let mut names: Vec<String> = program_parameters.to_vec();
    for name in compiled
        .strata
        .iter()
        .flat_map(|s| &s.rules)
        .flat_map(|r| &r.parameters)
    {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }
    crate::api::impl_query::check_parameters(&names, params)
}

/// Rebuilds a fresh registry by recompiling each source in order.
///
/// The returned registry is a pure function of `sources`: rules, strata, and
/// per-source `rule_names` are recomputed from scratch, so strata ids stay
/// dense (no orphaned strata accumulate across re-registrations) and a stale
/// `rule_names` loaded from disk is corrected. `rule_names` on the input is
/// ignored.
///
/// # Errors
///
/// Returns a parse or compile error (as [`UniError`]) for the first source
/// that no longer parses or compiles.
pub(crate) fn rebuild_registry_from_sources(
    sources: &[crate::api::locy_rule_catalog::RegisteredSource],
    plugin_registry: &uni_plugin::PluginRegistry,
) -> Result<LocyRuleRegistry> {
    let oracle = plugin_monotonicity_oracle(plugin_registry);
    let mut registry = LocyRuleRegistry::default();
    for src in sources {
        let ast = uni_cypher::parse_locy(&src.source).map_err(map_parse_error)?;
        let external_names: Vec<String> = registry.rules.keys().cloned().collect();
        let compiled =
            uni_locy::compile_with_oracle(&ast, &HashMap::new(), &external_names, &oracle)
                .map_err(map_compile_error)?;
        let base_id = registry.strata.len();
        let mut this_names: Vec<String> = Vec::with_capacity(compiled.rule_catalog.len());
        for (name, rule) in compiled.rule_catalog {
            this_names.push(name.clone());
            registry.rules.insert(name, rule);
        }
        for mut stratum in compiled.strata {
            stratum.id += base_id;
            stratum.depends_on = stratum.depends_on.iter().map(|d| base_id + d).collect();
            registry.strata.push(stratum);
        }
        this_names.sort();
        registry
            .sources
            .push(crate::api::locy_rule_catalog::RegisteredSource {
                source: src.source.clone(),
                rule_names: this_names,
            });
    }
    Ok(registry)
}

/// Rebuilds a registry from persisted sources at database-open time.
///
/// With `skip_invalid` false, a single non-compiling source fails the open
/// with guidance naming the offending rules. With `skip_invalid` true, each
/// failing source is skipped with a warning and retained in the catalog file
/// (the file is not rewritten on load), so a fixed binary can recover it.
///
/// # Errors
///
/// Returns [`UniError::Internal`] when `skip_invalid` is false and any
/// persisted source no longer compiles.
pub(crate) fn build_locy_registry_from_persisted(
    sources: &[crate::api::locy_rule_catalog::RegisteredSource],
    skip_invalid: bool,
    plugin_registry: &uni_plugin::PluginRegistry,
) -> Result<LocyRuleRegistry> {
    if !skip_invalid {
        return rebuild_registry_from_sources(sources, plugin_registry).map_err(|e| {
            UniError::Internal(anyhow::anyhow!(
                "a persisted Locy rule in catalog/locy_rules.json no longer compiles: {e}. \
                 Re-register the rule, or open with skip_invalid_locy_rules(true) to skip it."
            ))
        });
    }

    // Recompile incrementally, dropping (and warning about) any source that
    // no longer compiles. Later sources may depend on earlier ones, so a
    // skipped source can cascade — that is the intended behavior.
    let mut good: Vec<crate::api::locy_rule_catalog::RegisteredSource> = Vec::new();
    for src in sources {
        let mut trial = good.clone();
        trial.push(src.clone());
        match rebuild_registry_from_sources(&trial, plugin_registry) {
            Ok(_) => good.push(src.clone()),
            Err(e) => {
                tracing::warn!(
                    rules = ?src.rule_names,
                    error = %e,
                    "skipping persisted Locy rule source that no longer compiles \
                     (skip_invalid_locy_rules); it is retained in catalog/locy_rules.json"
                );
            }
        }
    }
    rebuild_registry_from_sources(&good, plugin_registry)
}

/// Registers a Locy program into a registry, rebuilding from sources.
///
/// Registration is idempotent: an exact-duplicate source text is a no-op and
/// returns `Ok(false)`. Otherwise the program is appended and the whole
/// registry is rebuilt and swapped atomically, returning `Ok(true)`. The
/// boolean lets callers persist only when state actually changed.
///
/// Shared logic used by the [`RuleRegistry`](crate::RuleRegistry) facade and
/// [`SessionTemplate`](crate::SessionTemplate). Performs no I/O.
///
/// # Errors
///
/// Returns a parse or compile error if `program` is invalid.
pub(crate) fn register_rules_on_registry(
    registry_lock: &std::sync::RwLock<LocyRuleRegistry>,
    program: &str,
    plugin_registry: &uni_plugin::PluginRegistry,
) -> Result<bool> {
    // Hold the WRITE lock across the whole read → compile → rebuild → assign so
    // concurrent register/remove calls serialize and cannot clobber each other's
    // update. Previously the read lock was released before `*write() = rebuilt`,
    // so two concurrent registrations both rebuilt from the same snapshot and the
    // second overwrite silently dropped the first's rule (lost update).
    let mut registry = registry_lock.write().unwrap();
    // Idempotent: exact-duplicate registration changes nothing.
    if registry.sources.iter().any(|s| s.source == program) {
        return Ok(false);
    }
    let existing = registry.sources.clone();

    // Discover the rule names this program defines so that any prior source
    // defining them is superseded — the last registration of a name wins, and
    // each name is owned by exactly one source (keeping `remove` unambiguous).
    let new_names = compile_defined_names(program, &existing, plugin_registry)?;

    let mut kept: Vec<crate::api::locy_rule_catalog::RegisteredSource> =
        Vec::with_capacity(existing.len() + 1);
    for src in existing {
        let overlap = src
            .rule_names
            .iter()
            .filter(|n| new_names.contains(n))
            .count();
        if overlap == 0 {
            kept.push(src);
        } else if overlap != src.rule_names.len() {
            // Partial redefinition: the prior source defines names that are not
            // being redefined, so silently dropping it would lose them.
            let orphaned: Vec<&String> = src
                .rule_names
                .iter()
                .filter(|n| !new_names.contains(n))
                .collect();
            return Err(UniError::Query {
                message: format!(
                    "cannot redefine rule(s) {new_names:?}: their existing source program also \
                     defines {orphaned:?}. Clear the registry and re-register single-rule \
                     programs."
                ),
                query: None,
            });
        }
        // Fully superseded sources are dropped.
    }
    kept.push(crate::api::locy_rule_catalog::RegisteredSource {
        source: program.to_string(),
        rule_names: new_names,
    });

    let rebuilt = rebuild_registry_from_sources(&kept, plugin_registry)?;
    *registry = rebuilt;
    Ok(true)
}

/// Compiles `program` only to discover the rule names it defines.
///
/// Existing rule names are supplied as external context so references to
/// already-registered rules resolve; the returned names are exactly the rules
/// this program defines (its compiled rule catalog).
///
/// # Errors
///
/// Returns a parse or compile error if `program` is invalid.
fn compile_defined_names(
    program: &str,
    existing: &[crate::api::locy_rule_catalog::RegisteredSource],
    plugin_registry: &uni_plugin::PluginRegistry,
) -> Result<Vec<String>> {
    let ast = uni_cypher::parse_locy(program).map_err(map_parse_error)?;
    let external: Vec<String> = existing
        .iter()
        .flat_map(|s| s.rule_names.iter().cloned())
        .collect();
    let oracle = plugin_monotonicity_oracle(plugin_registry);
    let compiled = uni_locy::compile_with_oracle(&ast, &HashMap::new(), &external, &oracle)
        .map_err(map_compile_error)?;
    let mut names: Vec<String> = compiled.rule_catalog.keys().cloned().collect();
    names.sort();
    Ok(names)
}

/// Returns an invocation hint when `program` is a bare registered rule name.
///
/// A common mistake is to invoke a registered rule by passing its bare name
/// to `locy()`, which fails to parse. When the failed program is a lone
/// identifier matching a registered rule, this produces a hint pointing at
/// the `QUERY <name> … RETURN …` goal-query form. Returns `None` otherwise.
fn bare_rule_hint(program: &str, registry: &std::sync::RwLock<LocyRuleRegistry>) -> Option<String> {
    let trimmed = program.trim();
    let is_bare_ident = !trimmed.is_empty()
        && trimmed
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !is_bare_ident {
        return None;
    }
    let registry = registry.read().ok()?;
    if registry.rules.contains_key(trimmed) {
        return Some(format!(
            "Hint: '{trimmed}' is a registered Locy rule — invoke it with a goal query, \
             e.g. `QUERY {trimmed} [WHERE …] [RETURN …]`"
        ));
    }
    let lower = trimmed.to_ascii_lowercase();
    if let Some(actual) = registry
        .rules
        .keys()
        .find(|k| k.to_ascii_lowercase() == lower)
    {
        return Some(format!(
            "Hint: did you mean the registered Locy rule '{actual}'? Invoke it with a goal \
             query, e.g. `QUERY {actual} [WHERE …] [RETURN …]`"
        ));
    }
    None
}

/// Evaluate a Locy program against the database with a specific rule registry.
///
/// This is the core evaluation path used by Session and Transaction.
/// `principal` is who the program is authorized for (anonymous when `None`).
pub(crate) async fn evaluate_with_db_and_config(
    db: &crate::api::UniInner,
    program: &str,
    config: &LocyConfig,
    rule_registry: &std::sync::RwLock<LocyRuleRegistry>,
    cancel: crate::api::impl_query::CancelScope,
    max_memory: Option<usize>,
    principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
) -> Result<LocyResult> {
    evaluate_with_db_and_config_capturing(
        db,
        program,
        config,
        rule_registry,
        None,
        cancel,
        max_memory,
        principal,
    )
    .await
}

/// Like [`evaluate_with_db_and_config`], but optionally captures a structured
/// execution profile into `profile_capture` (the session `profile()` path).
#[expect(
    clippy::too_many_arguments,
    reason = "The session evaluation's program, registry, profile slot and per-call scope (cancellation, memory bound, principal)"
)]
pub(crate) async fn evaluate_with_db_and_config_capturing(
    db: &crate::api::UniInner,
    program: &str,
    config: &LocyConfig,
    rule_registry: &std::sync::RwLock<LocyRuleRegistry>,
    profile_capture: Option<&Arc<std::sync::Mutex<Option<uni_query::LocyExecProfile>>>>,
    cancel: crate::api::impl_query::CancelScope,
    max_memory: Option<usize>,
    principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
) -> Result<LocyResult> {
    // Compile with the given registry
    let (ast, parameters) = match uni_cypher::parse_locy_with_parameters(program) {
        Ok(parsed) => parsed,
        Err(e) => {
            let mut err = map_parse_error(e);
            if let (UniError::Parse { message, .. }, Some(hint)) =
                (&mut err, bare_rule_hint(program, rule_registry))
            {
                message.push('\n');
                message.push_str(&hint);
            }
            return Err(err);
        }
    };
    let external_names: Option<Vec<String>> = {
        let registry = rule_registry.read().unwrap();
        if registry.rules.is_empty() {
            None
        } else {
            Some(registry.rules.keys().cloned().collect())
        }
    };
    let oracle = plugin_monotonicity_oracle(&db.plugin_registry);
    let mut compiled = uni_locy::compile_with_oracle_and_config(
        &ast,
        &HashMap::new(),
        external_names.as_deref().unwrap_or(&[]),
        config,
        &oracle,
    )
    .map_err(map_compile_error)?;

    // Merge registered rules
    merge_registered_rules(&mut compiled, &rule_registry.read().unwrap())
        .map_err(map_compile_error)?;
    // After compiling and merging, so a program invalid for another reason
    // reports that; the merged rules' parameters count too.
    check_program_parameters(&parameters, &compiled, &config.params)?;

    // Create a LocyEngine directly from &UniInner.
    // Session-level: collect DERIVE output for deferred materialization.
    // Always create an ephemeral locy_l0 for the evaluation scope — this provides:
    // - DERIVE visibility: trailing Cypher sees DERIVE mutations
    // - ASSUME/ABDUCE isolation: fork/restore from this buffer
    // Read-only DB returns None and degrades gracefully.
    let locy_l0 = db
        .writer
        .as_ref()
        .map(|writer| writer.create_transaction_l0());
    let engine = LocyEngine {
        db,
        counters: std::sync::Arc::new(uni_store::QueryCounters::new()),
        tx_l0_override: locy_l0.clone(),
        locy_l0,
        collect_derive: true,
        read_snapshot: None,
        cancel,
        max_memory,
        principal,
    };
    engine
        .evaluate_compiled_capturing(compiled, program, config, profile_capture)
        .await
}

/// Build a monotonicity oracle backed by `registry`, falling back to the
/// built-in `M*` contract.
///
/// Returned as an `impl Fn` borrowed for the call: `MonotonicityOracle<'a>` is
/// `&'a dyn Fn(&str) -> Option<bool>`, so no `'static` bound, boxing or `Arc` is
/// needed — `&oracle` coerces at the call site.
pub(crate) fn plugin_monotonicity_oracle(
    registry: &uni_plugin::PluginRegistry,
) -> impl Fn(&str) -> Option<bool> + '_ {
    move |name: &str| {
        uni_query::query::df_graph::locy_fold::locy_monotonicity_verdict(registry, name)
    }
}

/// Engine for evaluating Locy programs against a real database.
pub struct LocyEngine<'a> {
    pub(crate) db: &'a crate::api::UniInner,
    /// When set, the engine routes reads/writes through this private L0 buffer
    /// (commit-time serialization for transactions).
    pub(crate) tx_l0_override: Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    /// Ephemeral L0 buffer for Locy evaluation scope.
    /// Session path: ephemeral per-locy() buffer (DERIVE writes here, discarded on return).
    /// Transaction path: same as tx_l0 (DERIVE auto-applies).
    /// ASSUME/ABDUCE fork from here via fork_l0/restore_l0.
    pub(crate) locy_l0: Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    /// When true, DERIVE commands collect ASTs + data instead of executing.
    /// Session-level evaluation sets this to true; transaction-level sets false.
    pub(crate) collect_derive: bool,
    /// Execution counters for this evaluation, shared by every `Executor` it
    /// builds.
    ///
    /// Owned here rather than per-executor because the ASSUME/ABDUCE path
    /// re-enters through `re_evaluate_strata` with a second executor: if each
    /// kept its own set, only one could be harvested and the other's scans would
    /// vanish. A vanished count is indistinguishable from work that never
    /// happened, which is the whole failure being fixed.
    pub(crate) counters: Arc<uni_store::QueryCounters>,
    /// The transaction's pinned read snapshot (Components C1 + C2), when
    /// evaluating inside a read-write transaction under SSI. Installed on
    /// the executor so Locy clause bodies read the frozen L0 generations and
    /// the version-pinned L1 view instead of live state. `None` at session
    /// level or with SSI disabled (live reads — a safe no-op downstream).
    pub(crate) read_snapshot: Option<uni_store::runtime::SnapshotView>,
    /// Cancellation scope for every Cypher statement this evaluation runs.
    ///
    /// Carries the enclosing session/transaction scope plus any per-call token
    /// set via `LocyBuilder::cancellation_token`. Before this existed the
    /// builder's setter wrote a field nothing ever read, so a caller that
    /// cancelled a Locy query — which the Python bindings expose and call —
    /// observed it run to completion.
    pub(crate) cancel: crate::api::impl_query::CancelScope,
    /// Per-evaluation override for the DataFusion memory pool bound.
    ///
    /// Locy already runs through `create_datafusion_planner`, so it has always
    /// been bounded by the database-level `max_query_memory`. What it lacked
    /// was the per-call knob `query_with(..).max_memory(..)` gives Cypher — the
    /// builder had no way to raise or lower the bound for one program
    /// (issue #284). `None` keeps the database setting.
    pub(crate) max_memory: Option<usize>,
    /// Who the program runs for, authorized against the `AuthzPolicy` chain
    /// before it runs ([`crate::api::locy_authz`]). `None` is the anonymous
    /// principal, as for Cypher.
    pub(crate) principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
}

impl LocyEngine<'_> {
    /// The database config with this evaluation's overrides applied, as handed
    /// to the executor that runs the program's Cypher clauses.
    ///
    /// Two things ride on it. `create_datafusion_planner` turns
    /// `max_query_memory` into the pool that bounds every operator, so
    /// overriding it is what gives the Locy builder the same per-call bound
    /// Cypher has had.
    ///
    /// And `query_timeout` is what `Executor::get_context` turns into the
    /// deadline every operator checks. This used to be the database setting
    /// alone, so `locy_with(..).timeout(2s)` had no representation in any
    /// operator check at all: the Locy budget was consulted only between
    /// strata and between fixpoint iterations, and a program
    /// that crossed none of those boundaries ran to completion and reported
    /// nothing (issue #283).
    ///
    /// An explicit Locy timeout *replaces* the database value, as
    /// `query_with(..).timeout(..)` does for Cypher. It used to be combined
    /// with `min`, because `LocyConfig::timeout` was a plain 300s default that
    /// could not be told apart from a value the caller asked for, and
    /// overriding with it unconditionally would have loosened the deadline for
    /// anyone who never set one. `min` bought that safety by silently
    /// discarding every explicit timeout above the database's 30s (#289). With
    /// the field an `Option`, an unset timeout keeps the `min` against
    /// [`LocyConfig::DEFAULT_TIMEOUT`] and a set one is honoured as given.
    fn executor_config(&self, locy: &LocyConfig) -> uni_common::UniConfig {
        let mut config = self.db.config.clone();
        if let Some(bytes) = self.max_memory {
            config.max_query_memory = bytes;
        }
        config.query_timeout = match locy.timeout {
            Some(explicit) => explicit,
            None => config.query_timeout.min(LocyConfig::DEFAULT_TIMEOUT),
        };
        config
    }
}

impl<'a> LocyEngine<'a> {
    /// Parse and compile a Locy program without executing it.
    ///
    /// If the session's rule registry contains pre-compiled rules, their names
    /// are passed to the compiler so that IS-ref and QUERY references to
    /// registered rules are accepted during validation.
    pub fn compile_only(&self, program: &str) -> Result<CompiledProgram> {
        self.compile_only_with_config(program, &LocyConfig::default())
    }

    /// Compile a Locy program (without registering) under an explicit
    /// [`LocyConfig`], so `neural_predicates_preview` gates fire on the
    /// transaction/evaluate path. A bare `compile_only` dropped the config, so
    /// preview-only programs failed to compile there.
    pub fn compile_only_with_config(
        &self,
        program: &str,
        config: &LocyConfig,
    ) -> Result<CompiledProgram> {
        self.compile_with_parameters(program, config)
            .map(|(compiled, _)| compiled)
    }

    /// [`Self::compile_only_with_config`], also returning every parameter
    /// the program's text references, in order of first appearance, for
    /// [`check_program_parameters`] once registered rules are merged.
    fn compile_with_parameters(
        &self,
        program: &str,
        config: &LocyConfig,
    ) -> Result<(CompiledProgram, Vec<String>)> {
        let (ast, parameters) =
            uni_cypher::parse_locy_with_parameters(program).map_err(map_parse_error)?;
        let registry = self.db.locy_rule_registry.read().unwrap();
        let external_names: Vec<String> = registry.rules.keys().cloned().collect();
        drop(registry);
        let oracle = plugin_monotonicity_oracle(&self.db.plugin_registry);
        let compiled = uni_locy::compile_with_oracle_and_config(
            &ast,
            &HashMap::new(),
            &external_names,
            config,
            &oracle,
        )
        .map_err(map_compile_error)?;
        Ok((compiled, parameters))
    }

    /// Compile and register a Locy program's rules for reuse.
    ///
    /// Rules registered here persist within the database session and are
    /// automatically merged into subsequent `evaluate()` calls, so notebooks
    /// can define rules once and run QUERY, EXPLAIN, ASSUME, ABDUCE without
    /// redeclaring the full rule set each time.
    pub fn register(&self, program: &str) -> Result<()> {
        let compiled = self.compile_only(program)?;
        let mut registry = self.db.locy_rule_registry.write().unwrap();
        for (name, rule) in compiled.rule_catalog {
            registry.rules.insert(name, rule);
        }
        // Merge strata, assigning new IDs to avoid collisions.
        let base_id = registry.strata.len();
        for mut stratum in compiled.strata {
            stratum.id += base_id;
            stratum.depends_on = stratum.depends_on.iter().map(|d| base_id + d).collect();
            registry.strata.push(stratum);
        }
        Ok(())
    }

    /// Clear all registered Locy rules from the session.
    pub fn clear_registry(&self) {
        let mut registry = self.db.locy_rule_registry.write().unwrap();
        registry.rules.clear();
        registry.strata.clear();
    }

    /// Parse, compile, and evaluate a Locy program with default config.
    pub async fn evaluate(&self, program: &str) -> Result<LocyResult> {
        self.evaluate_with_config(program, &LocyConfig::default())
            .await
    }

    /// Start building a Locy evaluation with fluent parameter binding.
    ///
    /// Mirrors `db.query_with(cypher).param(…).fetch_all()` for Cypher.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::Uni;
    /// # async fn example(db: &Uni) -> uni_db::Result<()> {
    /// let result = db.session()
    ///     .locy_with("CREATE RULE ep AS MATCH (e:Episode) WHERE e.agent_id = $aid YIELD KEY e")
    ///     .param("aid", "agent-123")
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn evaluate_with(&self, program: &str) -> crate::api::locy_builder::InnerLocyBuilder<'_> {
        crate::api::locy_builder::InnerLocyBuilder::new(self.db, program)
    }

    /// Convenience wrapper for EXPLAIN RULE commands.
    pub async fn explain(&self, program: &str) -> Result<LocyResult> {
        self.evaluate(program).await
    }

    /// Parse, compile, and evaluate a Locy program with custom config.
    ///
    /// If rules were previously registered via `register()`, they are
    /// automatically merged into the compiled program before execution.
    pub async fn evaluate_with_config(
        &self,
        program: &str,
        config: &LocyConfig,
    ) -> Result<LocyResult> {
        self.evaluate_with_config_capturing(program, config, None)
            .await
    }

    /// Like [`Self::evaluate_with_config`], but optionally captures a structured
    /// execution profile into `profile_capture` (the `profile()` builder path).
    pub(crate) async fn evaluate_with_config_capturing(
        &self,
        program: &str,
        config: &LocyConfig,
        profile_capture: Option<&Arc<std::sync::Mutex<Option<uni_query::LocyExecProfile>>>>,
    ) -> Result<LocyResult> {
        let (mut compiled, parameters) = self.compile_with_parameters(program, config)?;

        // Merge registered rules into the compiled program.
        merge_registered_rules(&mut compiled, &self.db.locy_rule_registry.read().unwrap())
            .map_err(map_compile_error)?;
        // After compiling and merging, so a program invalid for another
        // reason reports that; the merged rules' parameters count too.
        check_program_parameters(&parameters, &compiled, &config.params)?;

        self.evaluate_compiled_capturing(compiled, program, config, profile_capture)
            .await
    }

    /// Evaluate an already-compiled Locy program with custom config.
    ///
    /// This is the core execution path: it takes a `CompiledProgram` (with any
    /// registry merges already applied) and runs it through planning, execution,
    /// and command dispatch. `program` is the source text it was compiled
    /// from, the resource path its authorization names.
    pub async fn evaluate_compiled_with_config(
        &self,
        compiled: CompiledProgram,
        program: &str,
        config: &LocyConfig,
    ) -> Result<LocyResult> {
        self.evaluate_compiled_capturing(compiled, program, config, None)
            .await
    }

    /// Like [`Self::evaluate_compiled_with_config`], but optionally captures a
    /// structured execution profile.
    ///
    /// When `profile_capture` is `Some`, the `LocyProgramExec` is run with
    /// profiling enabled and the resulting [`uni_query::LocyExecProfile`] is
    /// stored into the slot for the caller (the `profile()` builder path) to
    /// read. `None` → zero profiling overhead.
    /// Evaluate a compiled program, racing the whole evaluation against the
    /// cancellation scope.
    ///
    /// The guard has to be here rather than deeper: the fixpoint runtime has no
    /// cooperative cancellation check of its own, and a rule body evaluates
    /// through the native/DataFusion stratum path without ever reaching
    /// `execute_ast_internal_with_tx_l0`. Guarding only the Cypher statements
    /// Locy dispatches therefore left the common case — a long-running
    /// fixpoint — completely unguarded. This mirrors how the Cypher terminals
    /// enforce cancellation: race the execution, do not rely on the inner
    /// layers to poll a token.
    ///
    /// Every entry point that runs a program reaches here, so this is where
    /// the program is authorized: before anything of it runs, for the
    /// engine's principal, with `program` (its source text) as the resource
    /// path. Locy consulted no policy at all before.
    pub(crate) async fn evaluate_compiled_capturing(
        &self,
        compiled: CompiledProgram,
        program: &str,
        config: &LocyConfig,
        profile_capture: Option<&Arc<std::sync::Mutex<Option<uni_query::LocyExecProfile>>>>,
    ) -> Result<LocyResult> {
        crate::api::locy_authz::authorize_locy_program(
            self.db,
            self.principal.as_deref(),
            program,
            &compiled,
        )?;
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(UniError::Cancelled),
            res = self.evaluate_compiled_capturing_inner(compiled, config, profile_capture) => res,
        }
    }

    async fn evaluate_compiled_capturing_inner(
        &self,
        compiled: CompiledProgram,
        config: &LocyConfig,
        profile_capture: Option<&Arc<std::sync::Mutex<Option<uni_query::LocyExecProfile>>>>,
    ) -> Result<LocyResult> {
        // A session-level evaluation (it collects DERIVE output rather than
        // writing) runs its commands against an ephemeral overlay, discarded
        // on return: a write command lands there, so later commands see it
        // and the shared graph does not. Every collecting entry point gets
        // one here, not only those that build it themselves: without it a
        // prepared program's DERIVE was invisible to its later commands, its
        // ASSUME failed to fork, and its Cypher write went straight to the
        // shared graph.
        refuse_undeferrable_procedures(&compiled.commands, self.collect_derive, false, |name| {
            uni_plugin::QName::parse(name)
                .ok()
                .and_then(|q| self.db.plugin_registry.procedure(&q))
                .map(|e| e.signature.mode)
        })?;
        if self.collect_derive
            && self.locy_l0.is_none()
            && let Some(writer) = self.db.writer.as_ref()
        {
            let overlay = writer.create_transaction_l0();
            let scoped = LocyEngine {
                db: self.db,
                tx_l0_override: Some(Arc::clone(&overlay)),
                locy_l0: Some(overlay),
                collect_derive: true,
                counters: Arc::clone(&self.counters),
                read_snapshot: self.read_snapshot.clone(),
                cancel: self.cancel.clone(),
                max_memory: self.max_memory,
                principal: self.principal.clone(),
            };
            return Box::pin(scoped.evaluate_compiled_capturing_inner(
                compiled,
                config,
                profile_capture,
            ))
            .await;
        }
        let start = Instant::now();

        // Goal-directed (magic-sets) rewrite of qualifying QUERY commands; the
        // identity unless `config.goal_directed` asks for it (uni_locy::magic).
        let (compiled, _goal_report) =
            uni_locy::magic::rewrite_goal_directed(compiled, config.goal_directed, &config.params);

        // Capture current version for staleness detection in DerivedFactSet
        let evaluated_at_version = if self.collect_derive {
            if let Some(writer) = self.db.writer.as_ref() {
                writer.l0_manager.get_current().read().current_version
            } else {
                0
            }
        } else {
            0
        };

        // 1. Build logical plan. Timed for `QueryMetrics::plan_time`: this
        // plans every stratum's clause bodies up front, and was the one
        // planning step a Locy result did not report.
        let plan_start = Instant::now();
        let schema = self.db.schema.schema();
        let query_planner = uni_query::QueryPlanner::new(schema);
        // A run with a provenance tracker (step 4) plans the transient
        // provenance and lineage columns too.
        let plan_builder = uni_query::query::locy_planner::LocyPlanBuilder::new(&query_planner)
            .with_provenance(compiled.needs_provenance_tracker());
        let logical = plan_builder
            .build_program_plan_with_full_neural(
                &compiled,
                config.max_iterations,
                config.effective_timeout(),
                config.max_derived_bytes,
                config.deterministic_best_by,
                config.strict_probability_domain,
                config.probability_epsilon,
                config.exact_probability,
                config.max_bdd_variables,
                config.top_k_proofs,
                config
                    .resolve()
                    .map_err(|e| UniError::Query {
                        message: format!("LocyConfigError: {e}"),
                        query: None,
                    })?
                    .kind,
                std::sync::Arc::new(config.classifier_registry.clone()),
                config.classifier_cache.clone().or_else(|| {
                    Some(std::sync::Arc::new(uni_locy::ModelInvocationCache::new(
                        config.classifier_cache_max,
                    )))
                }),
                config
                    .classifier_provenance_store
                    .clone()
                    .or_else(|| Some(std::sync::Arc::new(uni_locy::NeuralProvenanceStore::new()))),
            )
            .map_err(|e| UniError::Query {
                message: format!("LocyPlanBuildError: {e:#}"),
                query: None,
            })?;
        let plan_time = plan_start.elapsed();

        // 2. Create executor + physical planner
        let mut df_executor = uni_query::Executor::new(self.db.storage.clone());
        let executor_config = self.executor_config(config);
        let limits = LocyLimits::new(&executor_config, config);
        df_executor.set_config(executor_config);
        df_executor.set_counters(self.counters.clone());
        if let Some(ref w) = self.db.writer {
            df_executor.set_writer(w.clone());
        }
        // Install the transaction's private L0 on the executor — exactly like
        // the Cypher path (`execute_internal_with_tx_l0`). This gives Locy
        // clause bodies read-your-writes over the transaction's uncommitted
        // state AND puts the tx's `occ_read_set` into the planner's
        // L0Context, so graph scans inside the fixpoint are wrapped with
        // `ReadSetRecordingExec` and participate in SSI validation. Without
        // this, a `tx.locy(...)` read-modify-write commits on reads the OCC
        // validator never saw (architecture review §2.4).
        if let Some(tx_l0) = self.tx_l0_override.clone() {
            df_executor.set_transaction_l0(tx_l0);
        }
        // And the pinned read snapshot (C1 frozen L0 generations + C2
        // version-pinned L1 storage), matching the Cypher path's
        // `set_read_snapshot` — Locy reads see the same snapshot the rest of
        // the transaction does.
        if self.read_snapshot.is_some() {
            df_executor.set_read_snapshot(self.read_snapshot.clone());
        }
        df_executor.set_xervo_runtime(self.db.xervo_runtime.clone());
        df_executor.set_procedure_registry(self.db.procedure_registry.clone());
        if let Ok(reg) = self.db.custom_functions.read()
            && !reg.is_empty()
        {
            df_executor.set_custom_functions(std::sync::Arc::new(reg.clone()));
        }

        let (session_ctx, planner, _prop_mgr) = df_executor
            .create_datafusion_planner(&self.db.properties, &config.params)
            .await
            .map_err(|e| map_native_df_error(e, limits))?;

        // 3. Physical plan
        let exec_plan = planner
            .plan(&logical)
            .map_err(|e| map_native_df_error(e, limits))?;

        // 4. Create tracker for EXPLAIN commands or shared-proof detection
        let needs_tracker = compiled.needs_provenance_tracker();
        let tracker: Option<Arc<uni_query::query::df_graph::ProvenanceStore>> = if needs_tracker {
            Some(Arc::new(uni_query::query::df_graph::ProvenanceStore::new()))
        } else {
            None
        };

        let (
            derived_store_slot,
            iteration_counts_slot,
            peak_memory_slot,
            warnings_slot,
            approximate_slot,
            command_results_slot,
            timeout_flag,
            incomplete_slot,
            profile_slot,
        ) = if let Some(program_exec) =
            exec_plan.downcast_ref::<uni_query::query::df_graph::LocyProgramExec>()
        {
            if let Some(ref t) = tracker {
                program_exec.set_derivation_tracker(Arc::clone(t));
            }
            if profile_capture.is_some() {
                program_exec.set_profile_enabled(true);
            }
            program_exec.set_allow_partial(config.allow_partial);
            (
                program_exec.derived_store_slot(),
                program_exec.iteration_counts_slot(),
                program_exec.peak_memory_slot(),
                program_exec.warnings_slot(),
                program_exec.approximate_slot(),
                program_exec.command_results_slot(),
                program_exec.timeout_flag(),
                program_exec.incomplete_slot(),
                Some(program_exec.profile_slot()),
            )
        } else {
            (
                Arc::new(std::sync::RwLock::new(None)),
                Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
                Arc::new(std::sync::RwLock::new(0usize)),
                Arc::new(std::sync::RwLock::new(Vec::new())),
                Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
                Arc::new(std::sync::RwLock::new(Vec::new())),
                Arc::new(std::sync::atomic::AtomicU8::new(0)),
                Arc::new(std::sync::RwLock::new(None)),
                None,
            )
        };

        // The base state the facts below are derived from, for incremental
        // ASSUME / ABDUCE (see `NativeExecutionAdapter::enable_top_level_baseline`).
        // Taken before the run, so a commit during it is detected afterwards.
        let base_before_run = base_fingerprint(self.db, &self.read_snapshot);

        // 5. Execute strata
        let _stats_batches = uni_query::Executor::collect_batches(&session_ctx, exec_plan)
            .await
            .map_err(|e| map_native_df_error(e, limits))?;

        // 5b. Hand the captured execution profile back to the profile() caller.
        if let (Some(capture), Some(slot)) = (profile_capture, profile_slot.as_ref())
            && let Ok(mut p) = slot.write()
            && let Some(prof) = p.take()
        {
            *capture.lock().unwrap_or_else(|e| e.into_inner()) = Some(prof);
        }

        // 5c. The program's budget covers what follows the run too: the
        // conversion and hydration of its facts and every command. Before,
        // only the operators and the checks between strata and iterations
        // consulted it, so a run that finished just inside its deadline went
        // on converting, hydrating and answering commands unbounded (a
        // 1200-item cross join: 2.6 s against a 614 ms timeout).
        let deadline = start + limits.timeout;
        if let Some(detail) = incomplete_slot.read().ok().and_then(|g| g.clone())
            && !config.allow_partial
        {
            // The same error the end of this function would raise, without
            // first converting and hydrating facts nobody will see.
            return Err(UniError::LocyIncomplete {
                detail: Box::new(detail),
            });
        }
        let mut commands_cut = false;
        if Instant::now() >= deadline
            && incomplete_slot.read().map(|g| g.is_none()).unwrap_or(false)
        {
            if !config.allow_partial {
                return Err(budget_spent(limits));
            }
            // The facts are complete; the commands are not run.
            if let Ok(mut slot) = incomplete_slot.write() {
                *slot = Some(commands_cut_short(&compiled, config, start, limits));
            }
            commands_cut = true;
        }

        // 6. Extract native DerivedStore
        let native_store = derived_store_slot
            .write()
            .unwrap()
            .take()
            .unwrap_or_default();

        // 7. Convert native DerivedStore → row-based RowStore for the commands
        //
        // A program that may not return partial facts stops converting and
        // hydrating them once its budget is spent, failing with the `Timeout`
        // it would raise afterwards anyway. Under `allow_partial` the facts
        // are returned complete (and hydrated), as documented.
        let strict_deadline = (!config.allow_partial).then_some(deadline);
        let mut orch_store = native_store_to_row_store(&native_store, &compiled, strict_deadline)
            .map_err(|DeadlinePassed| budget_spent(limits))?;

        // 7b. Enrich VID / EID integers → Node / Edge objects so commands can access
        //     node properties (d.name etc.) and IS-ref joins work correctly
        //     across FOLD-rule boundaries.
        {
            let orch_rows: HashMap<String, Vec<FactRow>> = orch_store
                .iter()
                .map(|(k, v)| (k.clone(), v.rows.clone()))
                .collect();
            let (enriched_rows, hydration_warnings) = enrich_vids_with_nodes(
                &native_store,
                &compiled.rule_catalog,
                orch_rows,
                planner.graph_ctx(),
                planner.session_ctx(),
                strict_deadline,
            )
            .await
            .map_err(|DeadlinePassed| budget_spent(limits))?;
            if !hydration_warnings.is_empty()
                && let Ok(mut w) = warnings_slot.write()
            {
                w.extend(hydration_warnings);
            }
            for (name, rows) in enriched_rows {
                if let Some(rel) = orch_store.get_mut(&name) {
                    rel.rows = rows;
                }
            }
        }

        // Commands run in sequence: a command that writes (DERIVE, a Cypher
        // write) changes the state, and a later command that reads facts reads
        // them derived again in the new state — the strata the write can
        // affect, the rest reused (`locy_sequence`, shared with ASSUME bodies).
        // Each state's store is shared by reference, so a command that only
        // reads cannot change what a later one reads
        // (`locy_command_noninterference`).
        let commands_may_write = compiled.commands.iter().any(command_may_write);
        // ASSUME / ABDUCE may reuse these facts for the strata their mutations
        // cannot affect. Not when a provenance tracker ran: it can change PROB
        // values (shared-proof BDD) in ways a hypothetical re-evaluation,
        // which runs without one, does not reproduce.
        let hypothetical = compiled
            .commands
            .iter()
            .any(|c| matches!(c, CompiledCommand::Assume(_) | CompiledCommand::Abduce(_)));
        let offer_baseline = tracker.is_none();
        let orch_store = Arc::new(orch_store);
        // The raw facts, kept only when something can reuse them: a later
        // state after a write, or a hypothetical command.
        let raw_facts = (commands_may_write || (hypothetical && offer_baseline))
            .then(|| Arc::new(native_store.clone()));

        // 8. Dispatch commands via native trait interfaces
        let native_ctx = NativeExecutionAdapter::new(
            self.db,
            &native_store,
            &compiled,
            planner.graph_ctx().clone(),
            planner.session_ctx().clone(),
            config.params.clone(),
            self.tx_l0_override.clone(),
            self.read_snapshot.clone(),
            self.cancel.clone(),
        );
        // Propagate locy_l0 to the adapter for DERIVE/ASSUME/ABDUCE scoping.
        *native_ctx.locy_l0.lock().unwrap() = self.locy_l0.clone();
        *native_ctx.sequenced_tracker.lock().unwrap() = tracker.clone();
        // Every later evaluation of the program records provenance when its
        // own run did, and shares what remains of its budget.
        let native_ctx = native_ctx.with_budget(tracker.is_some(), start, limits.timeout);
        let mut top_level_baseline = false;
        if hypothetical
            && offer_baseline
            && let Some(raw) = &raw_facts
        {
            native_ctx.enable_top_level_baseline(
                base_before_run,
                Arc::clone(raw),
                Arc::clone(&orch_store),
            );
            top_level_baseline = true;
        }
        let mut sequence = CommandSequence::new(
            SequencePrograms {
                parent: &compiled,
                body: None,
            },
            SequenceScope::TopLevel,
            CommandFacts::top_level(raw_facts, orch_store, base_before_run.map(|b| b.token())),
            offer_baseline,
            top_level_baseline,
        );
        // A session program's writes are replayed later by `tx.apply`, where
        // a node created by an earlier command of the program gets a new id.
        // The first id such a node can have, to refuse a DERIVE that names one.
        let program_start_vid = match self.db.writer.as_ref() {
            Some(writer) if self.collect_derive && commands_may_write => {
                Some(writer.allocator.current_vid().await)
            }
            _ => None,
        };
        let mut locy_stats = LocyStats {
            total_iterations: iteration_counts_slot
                .read()
                .map(|c| c.values().sum::<usize>())
                .unwrap_or(0),
            peak_memory_bytes: peak_memory_slot.read().map(|v| *v).unwrap_or(0),
            ..LocyStats::default()
        };
        let approx_for_explain = approximate_slot
            .read()
            .map(|a| a.clone())
            .unwrap_or_default();
        // Collect inline results (QUERY, Cypher) already executed by run_program()
        let inline_map: HashMap<usize, CommandResult> =
            command_results_slot.write().unwrap().drain(..).collect();

        let mut command_results = Vec::new();
        let mut collected_derives: Vec<CollectedDeriveOutput> = Vec::new();
        let timed_out_early =
            commands_cut || timeout_flag.load(std::sync::atomic::Ordering::Relaxed) != 0;
        // Skip command dispatch when evaluation was cut short — the partial
        // derived store may be incomplete and QUERY would hit the expired
        // budget.
        if !timed_out_early {
            for (cmd_idx, cmd) in compiled.commands.iter().enumerate() {
                if let Some(result) = inline_map.get(&cmd_idx) {
                    // Already executed inline by run_program
                    command_results.push(result.clone());
                    continue;
                }
                // Each command runs only inside the program's budget.
                if Instant::now() >= deadline {
                    if !config.allow_partial {
                        return Err(budget_spent(limits));
                    }
                    merge_incomplete(
                        &mut native_ctx.rederivation_incomplete.lock().unwrap(),
                        commands_cut_short(&compiled, config, start, limits),
                    );
                    break;
                }
                let facts = if reads_facts(cmd) {
                    sequence
                        .facts(&native_ctx, config, &mut locy_stats)
                        .await
                        .map_err(|e| map_runtime_error(e, limits))?
                } else {
                    sequence.last_derived()
                };
                // A re-derivation cut short under `allow_partial` ends the
                // commands, as a cut-short run of the program does: the
                // facts are partial. The cut is reported in `incomplete`.
                if native_ctx.is_cut_short() {
                    break;
                }
                // The provenance and approximations of the state the command
                // reads: the program's own until the facts are derived again.
                let tracker = native_ctx.sequenced_tracker.lock().unwrap().clone();
                let approximate = native_ctx
                    .sequenced_approximate
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| approx_for_explain.clone());
                let outcome = match dispatch_native_command(
                    cmd,
                    &compiled,
                    &native_ctx,
                    config,
                    facts,
                    &mut locy_stats,
                    tracker,
                    &approximate,
                    self.collect_derive,
                    &mut collected_derives,
                    program_start_vid,
                )
                .await
                {
                    Ok(outcome) => outcome,
                    // The budget ran out inside the command (an in-memory
                    // QUERY checks it between rows, here or in an ASSUME
                    // body): under `allow_partial` a cut like any other, the
                    // command not answered.
                    Err(LocyError::Timeout { .. }) if config.allow_partial => {
                        merge_incomplete(
                            &mut native_ctx.rederivation_incomplete.lock().unwrap(),
                            commands_cut_short(&compiled, config, start, limits),
                        );
                        break;
                    }
                    Err(e) => return Err(map_runtime_error(e, limits)),
                };
                // A command whose own re-evaluations (ASSUME, ABDUCE) were
                // cut short answered from partial facts: not reported.
                if native_ctx.is_cut_short() {
                    break;
                }
                command_results.push(outcome.result);
                if let Some(written) = outcome.written {
                    sequence.record_write(written);
                }
                // Test seam: time passing between a command's completion and
                // the check below (`sleep(ms)` makes the deadline pass there).
                fail::fail_point!("locy::after-command");
                // A command that finished past the deadline answered late:
                // the program overran its budget. Without `allow_partial`
                // that is the `Timeout` below; with it, the answer stands
                // (its work, a write included, is done) but the overrun is
                // reported and no later command runs. It used to go
                // unreported, a late result returned as on time.
                if config.allow_partial && Instant::now() > deadline {
                    merge_incomplete(
                        &mut native_ctx.rederivation_incomplete.lock().unwrap(),
                        commands_cut_short(&compiled, config, start, limits),
                    );
                    break;
                }
            }
        }
        let orch_store = sequence.finish(&native_ctx);
        let sequenced_warnings =
            std::mem::take(&mut *native_ctx.sequenced_warnings.lock().unwrap());
        if !sequenced_warnings.is_empty()
            && let Ok(mut w) = warnings_slot.write()
        {
            w.extend(sequenced_warnings);
        }

        // A command that ran past the deadline answered late: the program
        // overran its budget, which is reported, not returned as on time —
        // as the `Timeout` without `allow_partial`, in `incomplete` with it.
        // Judged at the instant `evaluation_time` ends, so a result reported
        // complete always took at most its budget.
        let finished = Instant::now();
        if finished > deadline {
            if !config.allow_partial {
                return Err(budget_spent(limits));
            }
            merge_incomplete(
                &mut native_ctx.rederivation_incomplete.lock().unwrap(),
                commands_cut_short(&compiled, config, start, limits),
            );
        }
        let evaluation_time = finished.duration_since(start);
        // ASSUME and ABDUCE plan strata again for each hypothetical state;
        // that planning ran inside `evaluation_time` and is planning too.
        let plan_time = plan_time + native_ctx.hypothetical_plan_time();

        // 9. Build the returned derived map, every rule hydrated.
        //
        // `LocyResult::derived` is public and holds every rule's facts, returned
        // by a command or not, so every rule is hydrated (the Python binding
        // converts the whole map too). It holds the facts the program derived
        // before its first command, the state its first command reads. Without
        // a write command, the command store is exactly these rows, already
        // hydrated, and is reused. A write command (DERIVE, a Cypher write)
        // changes the graph, and the entities the facts name are then read in
        // their state after the commands, so hydration runs again, as it
        // always did. (ASSUME and ABDUCE write only to a hypothetical overlay
        // that is discarded before this point.)
        let mut base_derived: HashMap<String, Vec<FactRow>> = if commands_may_write {
            native_store
                .rule_names()
                .filter_map(|name| {
                    native_store
                        .get(name)
                        .map(|batches| (name.to_string(), record_batches_to_locy_rows(batches)))
                })
                .collect()
        } else {
            Arc::try_unwrap(orch_store.into_store())
                .unwrap_or_else(|shared| (*shared).clone())
                .into_iter()
                .map(|(name, relation)| (name, relation.rows))
                .collect()
        };

        // Stamp _approximate on facts in rules that had BDD fallback groups.
        let approximate_groups = approximate_slot
            .read()
            .map(|a| a.clone())
            .unwrap_or_default();
        for (rule_name, groups) in &approximate_groups {
            if !groups.is_empty()
                && let Some(rows) = base_derived.get_mut(rule_name)
            {
                for row in rows.iter_mut() {
                    row.insert("_approximate".to_string(), Value::Bool(true));
                }
            }
        }

        let enriched_derived = if commands_may_write {
            let (enriched, hydration_warnings) = enrich_vids_with_nodes(
                &native_store,
                &compiled.rule_catalog,
                base_derived,
                planner.graph_ctx(),
                planner.session_ctx(),
                strict_deadline,
            )
            .await
            .map_err(|DeadlinePassed| budget_spent(limits))?;
            if !hydration_warnings.is_empty()
                && let Ok(mut w) = warnings_slot.write()
            {
                w.extend(hydration_warnings);
            }
            enriched
        } else {
            base_derived
        };

        // 10. Build DerivedFactSet from collected derives (session path only)
        let derived_fact_set = if !collected_derives.is_empty() {
            let mut all_vertices = HashMap::new();
            let mut all_edges = Vec::new();
            let mut all_queries = Vec::new();
            for output in collected_derives {
                for (label, verts) in output.vertices {
                    all_vertices
                        .entry(label)
                        .or_insert_with(Vec::new)
                        .extend(verts);
                }
                all_edges.extend(output.edges);
                all_queries.extend(output.queries);
            }
            Some(DerivedFactSet {
                vertices: all_vertices,
                edges: all_edges,
                stats: locy_stats.clone(),
                evaluated_at_version,
                mutation_queries: all_queries,
                // A Cypher write command's statement may read `$params`;
                // the replay runs it with the values it ran with here.
                mutation_params: config.params.clone(),
            })
        } else {
            None
        };

        // 11. Build final LocyResult
        let warnings = warnings_slot.read().map(|w| w.clone()).unwrap_or_default();
        let mut incomplete = incomplete_slot.read().ok().and_then(|g| g.clone());
        // A re-derivation cut short under `allow_partial` (without it, it
        // already failed the program) is reported with the program's own.
        if let Some(cut) = native_ctx.rederivation_incomplete.lock().unwrap().take() {
            merge_incomplete(&mut incomplete, cut);
        }
        // An over-budget evaluation is a hard error by default: partial,
        // possibly-unsound facts must not be returned silently. Callers that
        // want anytime / best-effort semantics opt in via `allow_partial`, which
        // returns the partial result with its `incomplete` diagnostics populated.
        if let Some(detail) = &incomplete
            && !config.allow_partial
        {
            return Err(UniError::LocyIncomplete {
                detail: Box::new(detail.clone()),
            });
        }
        Ok(build_locy_result(
            enriched_derived,
            command_results,
            &compiled,
            evaluation_time,
            plan_time,
            locy_stats,
            warnings,
            approximate_groups,
            derived_fact_set,
            incomplete,
            self.counters.snapshot(),
        ))
    }

    /// Run only the fixpoint strata (no commands) via the native DataFusion path.
    ///
    /// Used by `re_evaluate_strata()` so that savepoint-scoped mutations from
    /// ASSUME/ABDUCE hypothetical states are visible — the `Executor` is configured
    /// with `self.db.writer`, which holds the active transaction handle.
    ///
    /// `reused` strata are copied from an earlier evaluation instead of
    /// derived (incremental ASSUME / ABDUCE).
    ///
    /// Returns the derived facts and the time spent building the logical
    /// plan, the same step the program's own `plan_time` measures.
    ///
    /// # Errors
    ///
    /// A planning or execution failure, and `UniError::LocyIncomplete` when
    /// the run stopped early (timeout, iteration limit) and
    /// `config.allow_partial` is unset: a hypothetical state's facts are then
    /// partial, and a command answering from them would answer short.
    async fn run_strata_native(
        &self,
        compiled: &CompiledProgram,
        config: &LocyConfig,
        reused: Option<uni_query::query::df_graph::ReusedStrata>,
        tracker: Option<Arc<ProvenanceStore>>,
    ) -> Result<StrataRun> {
        let plan_start = Instant::now();
        let schema = self.db.schema.schema();
        let query_planner = uni_query::QueryPlanner::new(schema);
        // Reused strata are copied, not evaluated, so they are not planned.
        let plan_builder = uni_query::query::locy_planner::LocyPlanBuilder::new(&query_planner)
            .with_provenance(tracker.is_some())
            .with_reused_strata(
                reused
                    .as_ref()
                    .map(|r| r.positions.clone())
                    .unwrap_or_default(),
            );
        // Planned as the program's own run is, model invocations included:
        // a rule calling a classifier derives the same facts here.
        let logical = plan_builder
            .build_program_plan_with_full_neural(
                compiled,
                config.max_iterations,
                config.effective_timeout(),
                config.max_derived_bytes,
                config.deterministic_best_by,
                config.strict_probability_domain,
                config.probability_epsilon,
                config.exact_probability,
                config.max_bdd_variables,
                config.top_k_proofs,
                config
                    .resolve()
                    .map_err(|e| UniError::Query {
                        message: format!("LocyConfigError: {e}"),
                        query: None,
                    })?
                    .kind,
                std::sync::Arc::new(config.classifier_registry.clone()),
                config.classifier_cache.clone().or_else(|| {
                    Some(std::sync::Arc::new(uni_locy::ModelInvocationCache::new(
                        config.classifier_cache_max,
                    )))
                }),
                config
                    .classifier_provenance_store
                    .clone()
                    .or_else(|| Some(std::sync::Arc::new(uni_locy::NeuralProvenanceStore::new()))),
            )
            .map_err(|e| UniError::Query {
                message: format!("LocyPlanBuildError: {e:#}"),
                query: None,
            })?;
        let plan_time = plan_start.elapsed();

        let mut df_executor = uni_query::Executor::new(self.db.storage.clone());
        let executor_config = self.executor_config(config);
        let limits = LocyLimits::new(&executor_config, config);
        df_executor.set_config(executor_config);
        df_executor.set_counters(self.counters.clone());
        if let Some(ref w) = self.db.writer {
            df_executor.set_writer(w.clone());
        }
        // Pass the tx_l0_override so the fixpoint planner sees uncommitted mutations
        // (ASSUME/ABDUCE hypothetical state, session DERIVE mutations, etc.)
        if let Some(ref l0) = self.tx_l0_override {
            df_executor.set_transaction_l0(l0.clone());
        }
        // The same pinned snapshot and functions the program's own run used:
        // the hypothetical state is that state plus the mutations, and the
        // strata it reuses were derived under them.
        if self.read_snapshot.is_some() {
            df_executor.set_read_snapshot(self.read_snapshot.clone());
        }
        df_executor.set_xervo_runtime(self.db.xervo_runtime.clone());
        df_executor.set_procedure_registry(self.db.procedure_registry.clone());
        if let Ok(reg) = self.db.custom_functions.read()
            && !reg.is_empty()
        {
            df_executor.set_custom_functions(std::sync::Arc::new(reg.clone()));
        }

        let (session_ctx, planner, _) = df_executor
            .create_datafusion_planner(&self.db.properties, &config.params)
            .await
            .map_err(|e| map_native_df_error(e, limits))?;
        let exec_plan = planner
            .plan(&logical)
            .map_err(|e| map_native_df_error(e, limits))?;

        let (derived_store_slot, incomplete_slot, warnings_slot, approximate_slot) =
            if let Some(program_exec) =
                exec_plan.downcast_ref::<uni_query::query::df_graph::LocyProgramExec>()
            {
                if let Some(reused) = reused {
                    program_exec.set_reused_strata(reused);
                }
                if let Some(tracker) = tracker {
                    program_exec.set_derivation_tracker(tracker);
                }
                program_exec.set_allow_partial(config.allow_partial);
                (
                    program_exec.derived_store_slot(),
                    program_exec.incomplete_slot(),
                    program_exec.warnings_slot(),
                    program_exec.approximate_slot(),
                )
            } else {
                (
                    Arc::new(std::sync::RwLock::new(None)),
                    Arc::new(std::sync::RwLock::new(None)),
                    Arc::new(std::sync::RwLock::new(Vec::new())),
                    Arc::new(std::sync::RwLock::new(HashMap::new())),
                )
            };

        let _ = uni_query::Executor::collect_batches(&session_ctx, exec_plan)
            .await
            .map_err(|e| map_native_df_error(e, limits))?;
        let incomplete = incomplete_slot.read().ok().and_then(|g| g.clone());
        if let Some(detail) = &incomplete
            && !config.allow_partial
        {
            return Err(UniError::LocyIncomplete {
                detail: Box::new(detail.clone()),
            });
        }
        let store = derived_store_slot
            .write()
            .unwrap()
            .take()
            .unwrap_or_default();
        Ok(StrataRun {
            store,
            plan_time,
            warnings: warnings_slot.read().map(|w| w.clone()).unwrap_or_default(),
            approximate: approximate_slot
                .read()
                .map(|a| a.clone())
                .unwrap_or_default(),
            incomplete,
        })
    }
}

/// What one run of a program's strata produced.
struct StrataRun {
    store: uni_query::query::df_graph::DerivedStore,
    /// Time spent building the logical plan.
    plan_time: Duration,
    warnings: Vec<RuntimeWarning>,
    /// Rules whose groups fell back to approximate probabilities.
    approximate: HashMap<String, Vec<String>>,
    /// How the run was cut short, when it was: only under `allow_partial`,
    /// since otherwise the cut is an error.
    incomplete: Option<uni_common::LocyIncomplete>,
}

/// The error for a program that spent its budget outside any operator: the
/// timeout an operator's deadline reports.
fn budget_spent(limits: LocyLimits) -> UniError {
    UniError::Timeout {
        timeout_ms: u64::try_from(limits.timeout.as_millis()).unwrap_or(u64::MAX),
    }
}

/// The `incomplete` record of a program whose facts are complete but whose
/// budget ran out before its commands did: a timeout with no rule
/// incomplete or skipped, every stratum evaluated.
fn commands_cut_short(
    compiled: &CompiledProgram,
    config: &LocyConfig,
    start: Instant,
    limits: LocyLimits,
) -> uni_common::LocyIncomplete {
    uni_common::LocyIncomplete {
        reason: uni_common::LocyIncompleteReason::Timeout,
        elapsed_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
        limit_ms: u64::try_from(limits.timeout.as_millis()).unwrap_or(u64::MAX),
        max_iterations: config.max_iterations,
        completed_strata: compiled.strata.len(),
        total_strata: compiled.strata.len(),
        incomplete_rules: Vec::new(),
        skipped_rules: Vec::new(),
        complement_rules_affected: Vec::new(),
    }
}

/// Folds `cut` into `into`: the first cut's reason, limits and strata
/// counts, every cut's incomplete, skipped and unsound rules, each once.
fn merge_incomplete(
    into: &mut Option<uni_common::LocyIncomplete>,
    cut: uni_common::LocyIncomplete,
) {
    let Some(existing) = into else {
        *into = Some(cut);
        return;
    };
    existing.elapsed_ms = existing.elapsed_ms.max(cut.elapsed_ms);
    for (all, more) in [
        (&mut existing.incomplete_rules, cut.incomplete_rules),
        (&mut existing.skipped_rules, cut.skipped_rules),
        (
            &mut existing.complement_rules_affected,
            cut.complement_rules_affected,
        ),
    ] {
        for rule in more {
            if !all.contains(&rule) {
                all.push(rule);
            }
        }
    }
}

/// A failed re-evaluation as the error its command fails with, keeping the
/// typed cases typed: a cut-short run stays `LocyIncomplete` and a deadline
/// stays `Timeout`, as the program's own run reports them, instead of
/// becoming a message inside a generic runtime error.
fn uni_error_to_locy(e: UniError) -> LocyError {
    match e {
        UniError::LocyIncomplete { detail } => LocyError::Incomplete { detail },
        UniError::Timeout { timeout_ms } => {
            let limit = Duration::from_millis(timeout_ms);
            LocyError::Timeout {
                elapsed: limit,
                limit,
            }
        }
        other => LocyError::ExecutorError {
            message: other.to_string(),
        },
    }
}

// ── Hypothetical baselines (incremental ASSUME / ABDUCE, A4) ─────────────────

/// Identifies the shared base state a Locy evaluation reads under the session
/// path: the writer's current L0 buffer (which a flush replaces) and its write
/// counters (which a commit advances).
///
/// `None` when nothing needs checking: under a pinned read snapshot the base
/// state cannot move, and without a writer nothing can write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct BaseFingerprint {
    l0: usize,
    version: u64,
    mutations: usize,
    pending_flush: usize,
}

impl BaseFingerprint {
    /// The fingerprint as one opaque value, for
    /// [`LocyExecutionContext::base_state_token`].
    fn token(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

fn base_fingerprint(
    db: &crate::api::UniInner,
    read_snapshot: &Option<uni_store::runtime::SnapshotView>,
) -> Option<BaseFingerprint> {
    if read_snapshot.is_some() {
        return None;
    }
    let writer = db.writer.as_ref()?;
    let current = writer.l0_manager.get_current();
    let (version, mutations) = {
        let l0 = current.read();
        (l0.current_version, l0.mutation_count)
    };
    Some(BaseFingerprint {
        l0: Arc::as_ptr(&current) as usize,
        version,
        mutations,
        pending_flush: writer.l0_manager.get_pending_flush().len(),
    })
}

/// The facts of one earlier state and how to tell that it is still current.
struct HypotheticalBaseline {
    facts: Arc<uni_query::query::df_graph::DerivedStore>,
    /// The same facts hydrated in that state, when held.
    rows: Option<Arc<RowStore>>,
    /// The adapter's epoch when the facts were derived.
    epoch: u64,
    /// The base state when the facts were derived.
    base: Option<BaseFingerprint>,
}

// ── NativeExecutionAdapter — implements DerivedFactSource + LocyExecutionContext ─

struct NativeExecutionAdapter<'a> {
    db: &'a crate::api::UniInner,
    native_store: &'a uni_query::query::df_graph::DerivedStore,
    compiled: &'a CompiledProgram,
    /// Execution contexts from the fixpoint planner for columnar query execution.
    graph_ctx: Arc<uni_query::query::df_graph::GraphExecutionContext>,
    session_ctx: Arc<parking_lot::RwLock<datafusion::prelude::SessionContext>>,
    /// Query parameters threaded from LocyConfig; passed to execute_subplan so
    /// that $param references in rule MATCH WHERE clauses are resolved.
    params: HashMap<String, Value>,
    /// Private transaction L0 override for commit-time serialization.
    tx_l0_override: Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    /// Pinned MVCC read snapshot (C1 frozen L0 generations + C2 version-pinned
    /// L1 storage), cloned from the `LocyEngine`. When present, command-dispatch
    /// pattern matching (`execute_pattern`) must read base facts from the frozen
    /// snapshot rather than live storage, so a Locy program's result does not
    /// depend on whether it runs via `session.locy()` (no snapshot) or
    /// `tx.locy()` (snapshot) — see REQ-1b. `None` for the session path.
    read_snapshot: Option<uni_store::runtime::SnapshotView>,
    /// Locy-scoped L0 buffer. DERIVE mutations go here. ASSUME/ABDUCE fork from here.
    /// Protected by std::sync::Mutex for interior mutability (fork/restore swap the Arc).
    locy_l0: std::sync::Mutex<Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>>,
    /// Stack of saved L0 states for nested fork/restore (ASSUME inside ASSUME).
    l0_save_stack:
        std::sync::Mutex<Vec<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>>,
    /// Which mutation history the current state is: replaced by a fresh value
    /// on every write this adapter issues, restored by `restore_l0`. Two
    /// moments with equal epochs see the same Locy-issued writes, which is
    /// how a [`HypotheticalBaseline`] knows it still describes the state.
    epoch: std::sync::atomic::AtomicU64,
    /// Source of fresh epochs; never reused within the adapter.
    next_epoch: std::sync::atomic::AtomicU64,
    /// The epoch at each `fork_l0`, restored by the paired `restore_l0`.
    epoch_save_stack: std::sync::Mutex<Vec<u64>>,
    /// Facts of earlier states, innermost last: the program's own (when
    /// enabled), then one per enclosing ASSUME body.
    baselines: std::sync::Mutex<Vec<HypotheticalBaseline>>,
    /// Cancellation scope, cloned from the `LocyEngine`, for the Cypher
    /// statements command dispatch runs.
    cancel: crate::api::impl_query::CancelScope,
    /// Time spent planning the strata of hypothetical re-evaluations
    /// (`ASSUME` bodies, `ABDUCE` candidates) and of the re-derivations
    /// after a top-level write, in nanoseconds. Reported in the result's
    /// `plan_time` next to the program's own planning.
    hypothetical_plan_nanos: std::sync::atomic::AtomicU64,
    /// The provenance of the current top-level state, when the program
    /// records provenance (EXPLAIN, shared-proof detection): the program's
    /// own tracker, replaced by a fresh one each time the facts are derived
    /// again after a write.
    sequenced_tracker: std::sync::Mutex<Option<Arc<ProvenanceStore>>>,
    /// The approximate-probability groups of the current top-level state,
    /// once the facts were derived again after a write; `None` before.
    sequenced_approximate: std::sync::Mutex<Option<HashMap<String, Vec<String>>>>,
    /// Warnings raised while deriving the facts again after a write.
    sequenced_warnings: std::sync::Mutex<Vec<RuntimeWarning>>,
    /// Whether the program records provenance (see
    /// [`CompiledProgram::needs_provenance_tracker`]): every re-evaluation
    /// then records it too, a hypothetical one included, since exact
    /// probabilities and EXPLAIN both depend on it, and reuses no stratum.
    records_provenance: bool,
    /// When the program's budget runs out: its start plus its timeout.
    /// Every re-evaluation runs with what remains, not a fresh timeout.
    deadline: Option<Instant>,
    /// The program's start and timeout, for the `Timeout` a check of
    /// `deadline` reports.
    budget: Option<(Instant, Duration)>,
    /// The re-evaluations cut short under `allow_partial`, merged; reported
    /// with the program's result.
    rederivation_incomplete: std::sync::Mutex<Option<uni_common::LocyIncomplete>>,
}

impl<'a> NativeExecutionAdapter<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "Threads the fixpoint planner's contexts, params, tx L0, and pinned read snapshot into the command-dispatch adapter; grouping into a struct would just move the argument list."
    )]
    fn new(
        db: &'a crate::api::UniInner,
        native_store: &'a uni_query::query::df_graph::DerivedStore,
        compiled: &'a CompiledProgram,
        graph_ctx: Arc<uni_query::query::df_graph::GraphExecutionContext>,
        session_ctx: Arc<parking_lot::RwLock<datafusion::prelude::SessionContext>>,
        params: HashMap<String, Value>,
        tx_l0_override: Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
        read_snapshot: Option<uni_store::runtime::SnapshotView>,
        cancel: crate::api::impl_query::CancelScope,
    ) -> Self {
        Self {
            db,
            native_store,
            compiled,
            graph_ctx,
            session_ctx,
            params,
            tx_l0_override,
            read_snapshot,
            locy_l0: std::sync::Mutex::new(None),
            l0_save_stack: std::sync::Mutex::new(Vec::new()),
            epoch: std::sync::atomic::AtomicU64::new(0),
            next_epoch: std::sync::atomic::AtomicU64::new(1),
            epoch_save_stack: std::sync::Mutex::new(Vec::new()),
            baselines: std::sync::Mutex::new(Vec::new()),
            cancel,
            hypothetical_plan_nanos: std::sync::atomic::AtomicU64::new(0),
            sequenced_tracker: std::sync::Mutex::new(None),
            sequenced_approximate: std::sync::Mutex::new(None),
            sequenced_warnings: std::sync::Mutex::new(Vec::new()),
            records_provenance: false,
            deadline: None,
            budget: None,
            rederivation_incomplete: std::sync::Mutex::new(None),
        }
    }

    /// The adapter for a program that records provenance when
    /// `records_provenance`, started at `start` with the budget `timeout`.
    fn with_budget(mut self, records_provenance: bool, start: Instant, timeout: Duration) -> Self {
        self.records_provenance = records_provenance;
        self.deadline = Some(start + timeout);
        self.budget = Some((start, timeout));
        self
    }

    /// The `Timeout` of a program whose budget is spent, `None` while it
    /// is not (or without a budget).
    fn budget_spent(&self) -> Option<LocyError> {
        let (start, limit) = self.budget?;
        (Instant::now() >= start + limit).then(|| self.timeout_error())
    }

    /// The `Timeout` the program's budget reports: its elapsed time and
    /// limit (zero without a budget, which has no deadline to pass).
    fn timeout_error(&self) -> LocyError {
        let (start, limit) = self.budget.unwrap_or((Instant::now(), Duration::ZERO));
        LocyError::Timeout {
            elapsed: start.elapsed(),
            limit,
        }
    }

    /// Total planning time of the hypothetical re-evaluations run so far.
    fn hypothetical_plan_time(&self) -> Duration {
        Duration::from_nanos(
            self.hypothetical_plan_nanos
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Offer the program's own facts to ASSUME / ABDUCE for reuse.
    ///
    /// `base` is the base state's fingerprint taken before the program ran.
    /// Valid until a Locy-issued write or a change of the base state.
    fn enable_top_level_baseline(
        &self,
        base: Option<BaseFingerprint>,
        facts: Arc<uni_query::query::df_graph::DerivedStore>,
        rows: Arc<RowStore>,
    ) {
        let baseline = HypotheticalBaseline {
            facts,
            rows: Some(rows),
            epoch: self.epoch.load(std::sync::atomic::Ordering::SeqCst),
            base,
        };
        self.baselines.lock().unwrap().insert(0, baseline);
    }

    /// Record that the state changed: a write is about to run.
    fn advance_epoch(&self) {
        let fresh = self
            .next_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.epoch.store(fresh, std::sync::atomic::Ordering::SeqCst);
    }

    /// Storage pinned to the transaction's snapshot version (C2) when a read
    /// snapshot is installed, else live storage. Mirrors
    /// `Executor::effective_storage` so command-dispatch reads honor the same L1
    /// version boundary the fixpoint planner used.
    fn effective_storage(&self) -> Arc<uni_store::storage::manager::StorageManager> {
        self.read_snapshot
            .as_ref()
            .and_then(|s| s.pinned_storage.clone())
            .unwrap_or_else(|| self.db.storage.clone())
    }

    /// The context an entity lookup scans through: the fixpoint's, on
    /// [`Self::effective_storage`] (pinned to the transaction's snapshot when
    /// one is installed), so a lookup and a Cypher read of the same entity see
    /// the same version.
    ///
    /// It reads the current state's L0 too ([`Self::state_ctx`]): inside an
    /// ASSUME body or an ABDUCE candidate that is the hypothetical overlay,
    /// so an entity the hypothetical mutation created resolves. Read through
    /// the program's own overlay instead, such an entity stayed a raw id, and
    /// a body EXPLAIN whose WHERE named it fell back to re-execution.
    fn lookup_ctx(&self) -> Arc<uni_query::query::df_graph::GraphExecutionContext> {
        self.state_ctx()
    }

    /// The context a read of the current state goes through: the active
    /// Locy L0 (the hypothetical overlay inside ASSUME / ABDUCE, else the
    /// program's own overlay or transaction L0) over the pinned snapshot's
    /// frozen generations and version-pinned storage when one is installed,
    /// else live L0 and storage; the fixpoint's context when none of these
    /// apply. It inherits the fixpoint context's budget and counters.
    ///
    /// REQ-1b: when a read snapshot is pinned (the `tx.locy()` path), base
    /// facts come from the frozen snapshot generations and the
    /// version-pinned storage, exactly as the fixpoint planner read them,
    /// not live state; otherwise a concurrent commit would leak into command
    /// dispatch. The active L0 stays live for read-your-writes, and the
    /// property manager stays on live storage, matching
    /// `Executor::create_datafusion_planner`.
    fn state_ctx(&self) -> Arc<uni_query::query::df_graph::GraphExecutionContext> {
        let active_l0 = self
            .locy_l0
            .lock()
            .unwrap()
            .clone()
            .or_else(|| self.tx_l0_override.clone());
        if (active_l0.is_some() || self.read_snapshot.is_some())
            && let Some(writer) = self.db.writer.as_ref()
        {
            let (current_l0, pending_flush_l0s) = match &self.read_snapshot {
                Some(snap) => (snap.main.clone(), snap.extra.clone()),
                None => (
                    writer.l0_manager.get_current(),
                    writer.l0_manager.get_pending_flush(),
                ),
            };
            let l0_ctx = uni_query::query::df_graph::L0Context {
                current_l0: Some(current_l0),
                transaction_l0: active_l0,
                pending_flush_l0s,
            };
            // Derived from `self.graph_ctx`, so it inherits that context's
            // budget too: otherwise it silently drops the deadline (#207).
            return Arc::new(carry_budget(
                uni_query::query::df_graph::GraphExecutionContext::with_l0_context(
                    self.effective_storage(),
                    l0_ctx,
                    self.graph_ctx.property_manager().clone(),
                ),
                &self.graph_ctx,
            ));
        }
        self.graph_ctx.clone()
    }
}

#[async_trait]
impl DerivedFactSource for NativeExecutionAdapter<'_> {
    fn lookup_derived(&self, rule_name: &str) -> std::result::Result<Vec<FactRow>, LocyError> {
        let batches = self
            .native_store
            .get(rule_name)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        Ok(record_batches_to_locy_rows(batches))
    }

    fn lookup_derived_batches(
        &self,
        rule_name: &str,
    ) -> std::result::Result<Vec<RecordBatch>, LocyError> {
        Ok(self
            .native_store
            .get(rule_name)
            .map(|v| v.to_vec())
            .unwrap_or_default())
    }

    async fn execute_pattern(
        &self,
        pattern: &Pattern,
        where_conditions: &[Expr],
    ) -> std::result::Result<Vec<RecordBatch>, LocyError> {
        let query = build_match_return_query(pattern, where_conditions);
        let schema = self.db.schema.schema();
        let logical_plan =
            QueryPlanner::new(schema)
                .plan(query)
                .map_err(|e| LocyError::ExecutorError {
                    message: e.to_string(),
                })?;

        // Storage pinned to the transaction's snapshot version (C2) when a read
        // snapshot is installed, else live storage.
        let effective_storage = self.effective_storage();
        // The current state: the active Locy L0 (the hypothetical overlay
        // inside ASSUME / ABDUCE) over the pinned snapshot (REQ-1b).
        let state_ctx = self.state_ctx();
        let effective_ctx = &state_ctx;

        // Use the fixpoint planner's execution contexts directly via execute_subplan.
        uni_query::query::df_graph::common::execute_subplan(
            &logical_plan,
            &self.params,
            &HashMap::new(),
            effective_ctx,
            &self.session_ctx,
            &effective_storage,
            &self.db.schema.schema(),
            None, // Locy fixpoint path is read-only
        )
        .await
        .map_err(|e| LocyError::ExecutorError {
            message: e.to_string(),
        })
    }

    async fn lookup_nodes_by_vids(
        &self,
        vids: &[u64],
    ) -> std::result::Result<HashMap<u64, Value>, LocyError> {
        self.lookup_entities_by_id(vids, EntityKind::Node).await
    }

    async fn lookup_edges_by_eids(
        &self,
        eids: &[u64],
    ) -> std::result::Result<HashMap<u64, Value>, LocyError> {
        self.lookup_entities_by_id(eids, EntityKind::Edge).await
    }
}

#[async_trait]
impl LocyExecutionContext for NativeExecutionAdapter<'_> {
    /// A DERIVE rule's facts are its body re-run now — the fixpoint does not
    /// evaluate DERIVE rules — through [`resolve_clause_with_is_refs`]: the
    /// MATCH through [`DerivedFactSource::execute_pattern`] (which reads the
    /// adapter's current `locy_l0`, the hypothetical overlay inside ASSUME),
    /// then each IS reference as a semi-join, filter or anti-join over
    /// `derived_store`, in the clause's binding-plan order.
    ///
    /// Re-running only the MATCH and its WHERE expressions, as this did,
    /// dropped every IS reference: `WHERE a IS flag` derived for every `a`, and
    /// `WHERE a IS reach TO b` derived nothing, since only the reference binds
    /// `b`. And the referenced facts must be `derived_store`'s, not the
    /// program's, so that inside ASSUME they are the hypothetical ones.
    ///
    /// Any other rule's facts are `derived_store`'s rows, already hydrated by
    /// whichever evaluation produced that store, in the state it ran in.
    ///
    /// [`resolve_clause_with_is_refs`]: uni_query::query::df_graph::locy_delta::resolve_clause_with_is_refs
    async fn lookup_derived_enriched(
        &self,
        rule_name: &str,
        derived_store: &RowStore,
    ) -> std::result::Result<Vec<FactRow>, LocyError> {
        if let Some(rule) = self.compiled.rule_catalog.get(rule_name) {
            let is_derive_rule = rule
                .clauses
                .iter()
                .all(|c| matches!(c.output, RuleOutput::Derive(_)));
            if is_derive_rule {
                let mut all_rows = Vec::new();
                for clause in &rule.clauses {
                    all_rows.extend(
                        uni_query::query::df_graph::locy_delta::resolve_clause_with_is_refs(
                            clause,
                            self,
                            derived_store,
                            &self.compiled.rule_catalog,
                            None,
                        )
                        .await?,
                    );
                }
                return Ok(all_rows);
            }
        }
        Ok(derived_store
            .get(rule_name)
            .map(|relation| relation.rows.clone())
            .unwrap_or_default())
    }

    async fn execute_cypher_read(
        &self,
        ast: Query,
    ) -> std::result::Result<Vec<FactRow>, LocyError> {
        // A Cypher command that writes changes the state a hypothetical
        // baseline describes.
        if uni_locy::incremental::query_may_write(&ast) {
            self.advance_epoch();
        }
        // Route through locy_l0 so trailing Cypher sees DERIVE/ASSUME mutations.
        // locy_l0 is the "active" L0 for this evaluation scope.
        //
        // Every branch runs with the program's parameters and under its
        // cancellation scope, as the inline Cypher commands ahead of the
        // first write do: a command after a write is dispatched here, and
        // without them a `$param` read failed as unresolved and a `$param`
        // write stored NULL.
        let active_l0 = self.locy_l0.lock().unwrap().clone();
        let result = if let Some(ref l0) = active_l0 {
            self.db
                .execute_ast_internal_with_tx_l0(
                    ast,
                    "<locy>",
                    self.params.clone(),
                    self.db.config.clone(),
                    l0.clone(),
                    self.cancel.clone(),
                )
                .await
        } else if let Some(ref tx_l0) = self.tx_l0_override {
            self.db
                .execute_ast_internal_with_tx_l0(
                    ast,
                    "<locy>",
                    self.params.clone(),
                    self.db.config.clone(),
                    tx_l0.clone(),
                    self.cancel.clone(),
                )
                .await
        } else {
            self.db
                .execute_ast_internal(
                    ast,
                    "<locy>",
                    self.params.clone(),
                    self.db.config.clone(),
                    self.cancel.clone(),
                )
                .await
        }
        .map_err(|e| LocyError::ExecutorError {
            message: e.to_string(),
        })?;
        Ok(result
            .into_rows()
            .into_iter()
            .map(|row| {
                let cols: Vec<String> = row.columns().to_vec();
                cols.into_iter().zip(row.into_values()).collect()
            })
            .collect())
    }

    async fn execute_mutation(
        &self,
        ast: Query,
        params: HashMap<String, Value>,
    ) -> std::result::Result<usize, LocyError> {
        // Advanced before running: a mutation that fails part way may still
        // have written.
        self.advance_epoch();
        // Route through locy_l0 for all mutations within this evaluation scope.
        let active_l0 = self.locy_l0.lock().unwrap().clone();
        if let Some(ref l0) = active_l0 {
            let before = l0.read().mutation_count;
            self.db
                .execute_ast_internal_with_tx_l0(
                    ast,
                    "<locy>",
                    params,
                    self.db.config.clone(),
                    l0.clone(),
                    self.cancel.clone(),
                )
                .await
                .map_err(|e| LocyError::ExecutorError {
                    message: e.to_string(),
                })?;
            let after = l0.read().mutation_count;
            return Ok(after.saturating_sub(before));
        }
        if let Some(ref tx_l0) = self.tx_l0_override {
            let before = tx_l0.read().mutation_count;
            self.db
                .execute_ast_internal_with_tx_l0(
                    ast,
                    "<locy>",
                    params,
                    self.db.config.clone(),
                    tx_l0.clone(),
                    self.cancel.clone(),
                )
                .await
                .map_err(|e| LocyError::ExecutorError {
                    message: e.to_string(),
                })?;
            let after = tx_l0.read().mutation_count;
            return Ok(after.saturating_sub(before));
        }
        // Standard path: mutations go through writer's global L0
        let before = self.db.get_mutation_count().await;
        self.db
            .execute_ast_internal(
                ast,
                "<locy>",
                params,
                self.db.config.clone(),
                self.cancel.clone(),
            )
            .await
            .map_err(|e| LocyError::ExecutorError {
                message: e.to_string(),
            })?;
        let after = self.db.get_mutation_count().await;
        Ok(after.saturating_sub(before))
    }

    async fn fork_l0(&self) -> std::result::Result<(), LocyError> {
        let mut guard = self.locy_l0.lock().unwrap();
        let current = guard.as_ref().ok_or_else(|| LocyError::SavepointFailed {
            message: "no active Locy L0 to fork".into(),
        })?;
        // Clone the current L0 buffer (deep copy — forked WAL is None)
        let cloned = Arc::new(parking_lot::RwLock::new(current.read().clone()));
        // Save the original, replace with the clone for hypothetical mutations
        let previous = guard.replace(cloned).unwrap();
        self.l0_save_stack.lock().unwrap().push(previous);
        self.epoch_save_stack
            .lock()
            .unwrap()
            .push(self.epoch.load(std::sync::atomic::Ordering::SeqCst));
        Ok(())
    }

    async fn restore_l0(&self) -> std::result::Result<(), LocyError> {
        let saved =
            self.l0_save_stack
                .lock()
                .unwrap()
                .pop()
                .ok_or_else(|| LocyError::SavepointFailed {
                    message: "no saved L0 to restore".into(),
                })?;
        let mut guard = self.locy_l0.lock().unwrap();
        *guard = Some(saved);
        // The restored buffer is the state at the fork, so is its epoch.
        if let Some(epoch) = self.epoch_save_stack.lock().unwrap().pop() {
            self.epoch.store(epoch, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    async fn re_evaluate_strata(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
    ) -> std::result::Result<RowStore, LocyError> {
        Ok(self
            .re_evaluate_strata_reusing(program, config, None)
            .await?
            .rows)
    }

    fn reuse_baseline(&self) -> Option<uni_query::query::df_graph::StrataBaseline> {
        let baselines = self.baselines.lock().unwrap();
        let top = baselines.last()?;
        let current = self.epoch.load(std::sync::atomic::Ordering::SeqCst);
        // The innermost baseline is the newest; if it is stale, so is every
        // older one (a Locy write since, or the base state moved).
        (top.epoch == current && top.base == base_fingerprint(self.db, &self.read_snapshot)).then(
            || uni_query::query::df_graph::StrataBaseline {
                facts: top.facts.clone(),
                rows: top.rows.clone(),
            },
        )
    }

    fn push_reuse_baseline(&self, baseline: uni_query::query::df_graph::StrataBaseline) {
        let entry = HypotheticalBaseline {
            facts: baseline.facts,
            rows: baseline.rows,
            epoch: self.epoch.load(std::sync::atomic::Ordering::SeqCst),
            base: base_fingerprint(self.db, &self.read_snapshot),
        };
        self.baselines.lock().unwrap().push(entry);
    }

    fn pop_reuse_baseline(&self) {
        self.baselines.lock().unwrap().pop();
    }

    async fn re_evaluate_strata_reusing(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
        reused: Option<uni_query::query::df_graph::ReusedStrata>,
    ) -> std::result::Result<uni_query::query::df_graph::ReEvaluation, LocyError> {
        // A hypothetical state records provenance when the program does:
        // exact probabilities are corrected only with a tracker, so without
        // one an ASSUME body read the independence approximation of a value
        // the same QUERY at the top level read exactly, and a body EXPLAIN
        // had no derivations to show. A reused stratum carries no
        // provenance, so nothing is reused then.
        let tracker = self
            .records_provenance
            .then(|| Arc::new(ProvenanceStore::new()));
        let reused = if tracker.is_some() { None } else { reused };
        let (evaluation, run_warnings) = self
            .re_evaluate_with(program, config, reused, tracker)
            .await?;
        for w in &run_warnings {
            tracing::warn!(rule = %w.rule_name, "{}", w.message);
        }
        Ok(evaluation)
    }

    async fn re_evaluate_sequenced(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
        reused: Option<uni_query::query::df_graph::ReusedStrata>,
    ) -> std::result::Result<uni_query::query::df_graph::ReEvaluation, LocyError> {
        // A program that records provenance records it for the new state
        // too, so EXPLAIN after a write reads the derivations of the facts it
        // explains. A reused stratum would carry no provenance, so nothing is
        // reused then.
        let had_tracker = self.sequenced_tracker.lock().unwrap().is_some();
        let tracker = had_tracker.then(|| Arc::new(ProvenanceStore::new()));
        let reused = if had_tracker { None } else { reused };
        let (evaluation, run_warnings) = self
            .re_evaluate_with(program, config, reused, tracker.clone())
            .await?;
        if had_tracker {
            *self.sequenced_tracker.lock().unwrap() = tracker;
        }
        *self.sequenced_approximate.lock().unwrap() = Some(evaluation.approximate.clone());
        self.sequenced_warnings.lock().unwrap().extend(run_warnings);
        Ok(evaluation)
    }

    fn base_state_token(&self) -> Option<u64> {
        base_fingerprint(self.db, &self.read_snapshot).map(|b| b.token())
    }

    fn is_cut_short(&self) -> bool {
        self.rederivation_incomplete.lock().unwrap().is_some()
    }

    fn check_interrupt(&self) -> std::result::Result<(), LocyError> {
        if self.cancel.is_cancelled() {
            // The message `map_runtime_error` maps to `UniError::Cancelled`,
            // as an operator's cancellation reads.
            return Err(LocyError::ExecutorError {
                message: "Query cancelled".to_string(),
            });
        }
        match self.budget_spent() {
            Some(spent) => Err(spent),
            None => Ok(()),
        }
    }
}

impl NativeExecutionAdapter<'_> {
    /// Re-evaluates `program`'s strata in the current state, copying the
    /// `reused` strata, recording provenance into `tracker` when given.
    ///
    /// The run gets what remains of the program's budget, not a fresh
    /// timeout. When it is cut short (budget, `max_iterations`), it fails
    /// with [`LocyError::Incomplete`] unless `config.allow_partial`, in
    /// which case its partial facts are returned and the cut is recorded
    /// for the program's result.
    ///
    /// Returns the facts, with their provenance and approximate-probability
    /// groups, and the warnings the run and the hydration raised.
    async fn re_evaluate_with(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
        reused: Option<uni_query::query::df_graph::ReusedStrata>,
        tracker: Option<Arc<ProvenanceStore>>,
    ) -> std::result::Result<
        (
            uni_query::query::df_graph::ReEvaluation,
            Vec<RuntimeWarning>,
        ),
        LocyError,
    > {
        let reused_positions = reused
            .as_ref()
            .map(|r| r.positions.clone())
            .unwrap_or_default();
        // Rows already hydrated in this state, for rules only reused strata
        // derive (a name an evaluated stratum also derives — a body rule
        // shadowing a parent rule — takes the evaluated facts, as the run's
        // store does).
        let rehydrate_free: HashMap<String, RowRelation> =
            match reused.as_ref().and_then(|r| r.hydrated.clone()) {
                Some(hydrated) => {
                    let evaluated: HashSet<&str> = program
                        .strata
                        .iter()
                        .enumerate()
                        .filter(|(pos, _)| !reused_positions.contains(pos))
                        .flat_map(|(_, s)| s.rules.iter().map(|r| r.name.as_str()))
                        .collect();
                    program
                        .strata
                        .iter()
                        .enumerate()
                        .filter(|(pos, _)| reused_positions.contains(pos))
                        .flat_map(|(_, s)| &s.rules)
                        .filter(|r| !evaluated.contains(r.name.as_str()))
                        .filter_map(|r| {
                            hydrated
                                .get(&r.name)
                                .map(|rel| (r.name.clone(), rel.clone()))
                        })
                        .collect()
                }
                None => HashMap::new(),
            };
        let strata_only = CompiledProgram {
            strata: program.strata.clone(),
            rule_catalog: program.rule_catalog.clone(),
            model_catalog: program.model_catalog.clone(),
            warnings: vec![],
            commands: vec![],
        };
        // Pass the current locy_l0 so re-evaluation sees hypothetical state.
        let locy_l0 = self.locy_l0.lock().unwrap().clone();
        let engine = LocyEngine {
            db: self.db,
            // Shared, not fresh: this re-entry's scans belong to the same
            // evaluation, and a second set would be harvested by nobody. The
            // adapter's graph context is where the evaluation's counters
            // already live, so they are taken from there rather than duplicated
            // onto the adapter.
            counters: self
                .graph_ctx
                .counters()
                .cloned()
                .unwrap_or_else(|| std::sync::Arc::new(uni_store::QueryCounters::new())),
            tx_l0_override: locy_l0.clone(),
            locy_l0,
            collect_derive: false,
            // The program's own pinned snapshot: the hypothetical state is
            // that state plus the fork's mutations, and reused strata were
            // derived from it.
            read_snapshot: self.read_snapshot.clone(),
            cancel: self.cancel.clone(),
            // The adapter carries no override of its own; the bound for this
            // re-entry comes from the database config, as it did before.
            max_memory: None,
            // Runs strata of a program already authorized as a whole; it is
            // not an entry point and does not reach the authorization.
            principal: None,
        };
        // What remains of the program's budget; an exhausted one stops the
        // run before its first stratum, as an interruption.
        let remaining_config;
        let config = match self.deadline {
            Some(deadline) => {
                remaining_config = LocyConfig {
                    timeout: Some(deadline.saturating_duration_since(Instant::now())),
                    ..config.clone()
                };
                &remaining_config
            }
            None => config,
        };
        let StrataRun {
            store: native_store,
            plan_time,
            warnings: mut run_warnings,
            approximate,
            incomplete,
        } = engine
            .run_strata_native(&strata_only, config, reused, tracker.clone())
            .await
            .map_err(uni_error_to_locy)?;
        if let Some(cut) = incomplete {
            // `run_strata_native` returns a cut-short run only under
            // `allow_partial`.
            merge_incomplete(&mut self.rederivation_incomplete.lock().unwrap(), cut);
        }
        self.hypothetical_plan_nanos.fetch_add(
            u64::try_from(plan_time.as_nanos()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        // Bounded as the program's own conversion and hydration are: only
        // when partial facts may not be returned.
        let strict_deadline = self.deadline.filter(|_| !config.allow_partial);
        let spent = |DeadlinePassed| self.timeout_error();
        let mut store =
            native_store_to_row_store(&native_store, program, strict_deadline).map_err(spent)?;

        // Enrich VID / EID integers → Node / Edge objects so QUERY inside
        // ASSUME/ABDUCE can access node properties and IS-ref joins work.
        // Rows reused already hydrated are not looked up again.
        let store_rows: HashMap<String, Vec<FactRow>> = store
            .iter()
            .filter(|(k, _)| !rehydrate_free.contains_key(*k))
            .map(|(k, v)| (k.clone(), v.rows.clone()))
            .collect();

        // Enrichment resolves each vid by scanning exactly those vertices (`lookup_vertices`).
        // It MUST read through the same hypothetical `locy_l0` overlay the fixpoint
        // just used — otherwise a node created only inside this ASSUME/ABDUCE block
        // (which lives in the forked overlay, not base storage) is not found, the
        // vid stays a bare `Value::Int`, and any predicate reading its properties
        // (e.g. `QUERY ... WHERE b.name = 'B'`) evaluates `Int.name` → NULL and
        // silently drops the row. Rebuild an overlay-aware context mirroring the
        // command-dispatch path (`dispatch`/`execute_pattern`).
        let enrich_ctx = self.state_ctx();
        let (enriched, hydration_warnings) = enrich_vids_with_nodes(
            &native_store,
            &program.rule_catalog,
            store_rows,
            &enrich_ctx,
            &self.session_ctx,
            strict_deadline,
        )
        .await
        .map_err(spent)?;
        run_warnings.extend(hydration_warnings);
        for (name, rows) in enriched {
            if let Some(rel) = store.get_mut(&name) {
                rel.rows = rows;
            }
        }
        store.extend(rehydrate_free);

        Ok((
            uni_query::query::df_graph::ReEvaluation {
                rows: store,
                facts: Some(Arc::new(native_store)),
                reused: reused_positions,
                tracker,
                approximate,
            },
            run_warnings,
        ))
    }
}

/// Refuses a Cypher command calling a `Schema`- or `Dbms`-mode procedure
/// (`uni.schema.createLabel`, `uni.admin.snapshot.restore`, …) where its
/// effect could not be contained: anywhere in a session-level program, whose
/// writes are only collected for `tx.apply` and whose overlay is discarded,
/// and anywhere in an `ASSUME` body, which is hypothetical. These procedures
/// act on the catalog or the storage directly, not on the overlay, so they
/// would take effect at once — a session program ran
/// `uni.admin.snapshot.create()` and `uni.schema.dropLabel()` for real.
fn refuse_undeferrable_procedures(
    commands: &[CompiledCommand],
    session_level: bool,
    in_assume: bool,
    registered_mode: impl Fn(&str) -> Option<uni_plugin::traits::procedure::ProcedureMode> + Copy,
) -> Result<()> {
    use uni_plugin::traits::procedure::ProcedureMode;
    for command in commands {
        match command {
            CompiledCommand::Cypher(q) if session_level || in_assume => {
                let mode = uni_query::strongest_procedure_mode(q, &registered_mode);
                if matches!(mode, Some(m) if !matches!(m, ProcedureMode::Read | ProcedureMode::Write))
                {
                    let place = if in_assume {
                        "an ASSUME body"
                    } else {
                        "a session-level Locy program"
                    };
                    return Err(UniError::Query {
                        message: format!(
                            "a schema or administrative procedure cannot run in {place}: its \
                             effect is immediate and cannot be deferred or discarded. Run it \
                             with session.run() (administrative) or in a transaction (schema)."
                        ),
                        query: None,
                    });
                }
            }
            CompiledCommand::Assume(a) => {
                refuse_undeferrable_procedures(
                    &a.body_commands,
                    session_level,
                    true,
                    registered_mode,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

// ── Native command dispatch ───────────────────────────────────────────────────

/// Whether `cmd` can write to the graph its program runs against: a DERIVE,
/// or a Cypher command that is not read-only. ASSUME and ABDUCE write only
/// to a hypothetical state they discard.
fn command_may_write(cmd: &CompiledCommand) -> bool {
    match cmd {
        CompiledCommand::DeriveCommand(_) => true,
        CompiledCommand::Cypher(q) => uni_locy::incremental::query_may_write(q),
        _ => false,
    }
}

/// What a top-level command produced.
struct DispatchOutcome {
    result: CommandResult,
    /// The footprint of what it wrote, when it wrote: the facts later
    /// commands read must be derived again.
    written: Option<uni_locy::incremental::MutationFootprint>,
}

impl DispatchOutcome {
    fn read(result: CommandResult) -> Self {
        Self {
            result,
            written: None,
        }
    }
}

/// Refuses a session-level DERIVE that names a node an earlier command of
/// the same program created.
///
/// A session program's writes come back as a `DerivedFactSet` that
/// `tx.apply` replays, and a DERIVE's statements find the nodes they connect
/// by id. A node an earlier command created exists only in the program's
/// ephemeral overlay; the replay creates it again under a new id, so the
/// DERIVE's statement would match nothing and its edge would be silently
/// lost. Nodes allocated before the program started, or not in the overlay,
/// exist outside it.
fn refuse_program_created_endpoints(
    ctx: &NativeExecutionAdapter<'_>,
    rule: &str,
    bound_vids: &std::collections::BTreeSet<u64>,
    program_start_vid: Option<u64>,
) -> std::result::Result<(), LocyError> {
    let Some(start) = program_start_vid else {
        return Ok(());
    };
    let Some(overlay) = ctx.locy_l0.lock().unwrap().clone() else {
        return Ok(());
    };
    let overlay = overlay.read();
    let created = bound_vids.iter().find(|vid| {
        **vid >= start
            && overlay
                .vertex_labels
                .contains_key(&uni_common::core::id::Vid::new(**vid))
    });
    match created {
        Some(vid) => Err(LocyError::EvaluationError {
            message: format!(
                "DERIVE {rule} connects node {vid}, which an earlier command of this \
                 session-level program created. The program's writes are returned as a \
                 DerivedFactSet that tx.apply() replays, and the replay creates that node \
                 under a new id, so this DERIVE could not find it. Run the program with \
                 tx.locy(), or apply the creating command's facts before deriving from them."
            ),
        }),
        None => Ok(()),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Threads the program, adapter, current facts, provenance and the session path's collection state into one command"
)]
fn dispatch_native_command<'a>(
    cmd: &'a CompiledCommand,
    program: &'a CompiledProgram,
    ctx: &'a NativeExecutionAdapter<'a>,
    config: &'a LocyConfig,
    facts: &'a CommandFacts,
    stats: &'a mut LocyStats,
    tracker: Option<Arc<ProvenanceStore>>,
    approximate_groups: &'a HashMap<String, Vec<String>>,
    collect_derive: bool,
    collected_derives: &'a mut Vec<CollectedDeriveOutput>,
    program_start_vid: Option<u64>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = std::result::Result<DispatchOutcome, LocyError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let orch_store = facts.store();
        match cmd {
            CompiledCommand::GoalQuery(gq) => {
                let rows = uni_query::query::df_graph::locy_query::evaluate_query(
                    gq,
                    program,
                    config,
                    orch_store,
                    &|| ctx.check_interrupt(),
                )?;
                Ok(DispatchOutcome::read(CommandResult::Query(rows)))
            }
            CompiledCommand::ExplainRule(eq) => {
                let node = uni_query::query::df_graph::locy_explain::explain_rule(
                    eq,
                    program,
                    ctx,
                    config,
                    orch_store,
                    stats,
                    tracker.as_deref(),
                    Some(approximate_groups),
                )
                .await?;
                Ok(DispatchOutcome::read(CommandResult::Explain(node)))
            }
            CompiledCommand::Assume(ca) => {
                // An ASSUME restores the state it started from, so it writes
                // nothing the following commands can see.
                let result = uni_query::query::df_graph::locy_assume::evaluate_assume(
                    ca, program, ctx, config, stats,
                )
                .await?;
                Ok(DispatchOutcome::read(result))
            }
            CompiledCommand::Abduce(aq) => {
                let result = uni_query::query::df_graph::locy_abduce::evaluate_abduce(
                    aq, program, ctx, config, orch_store, stats,
                )
                .await?;
                Ok(DispatchOutcome::read(CommandResult::Abduce(result)))
            }
            CompiledCommand::DeriveCommand(dc) => {
                if collect_derive {
                    // Session path: collect ASTs + data for deferred materialization.
                    let output = uni_query::query::df_graph::locy_derive::collect_derive_facts(
                        dc, program, ctx, orch_store,
                    )
                    .await?;
                    refuse_program_created_endpoints(
                        ctx,
                        &dc.rule_name.to_string(),
                        &output.bound_vids,
                        program_start_vid,
                    )?;
                    let affected = output.affected;

                    // Replay the mutations into the ephemeral overlay so that
                    // later commands see the derived facts. Skipped when there
                    // is no overlay (a read-only database).
                    let mut footprint = uni_locy::incremental::MutationFootprint::default();
                    if ctx.tx_l0_override.is_some() {
                        for query in &output.queries {
                            footprint.extend(&uni_locy::incremental::query_footprint(query));
                            ctx.execute_mutation(query.clone(), HashMap::new()).await?;
                        }
                    }

                    collected_derives.push(output);
                    Ok(DispatchOutcome {
                        result: CommandResult::Derive { affected },
                        written: (affected > 0 && ctx.tx_l0_override.is_some())
                            .then_some(footprint),
                    })
                } else {
                    // Transaction path: auto-apply mutations
                    let (affected, footprint) =
                        uni_query::query::df_graph::locy_derive::derive_command_footprint(
                            dc, program, ctx, orch_store, stats,
                        )
                        .await?;
                    Ok(DispatchOutcome {
                        result: CommandResult::Derive { affected },
                        written: (affected > 0).then_some(footprint),
                    })
                }
            }
            CompiledCommand::Cypher(q) => {
                let rows = ctx.execute_cypher_read(q.clone()).await?;
                stats.queries_executed += 1;
                let written = uni_locy::incremental::command_write_footprint(q);
                if collect_derive && written.is_some() {
                    // A session program's write is returned with its DERIVE
                    // output, in program order, for `tx.apply` to replay.
                    collected_derives.push(CollectedDeriveOutput {
                        queries: vec![q.clone()],
                        vertices: HashMap::new(),
                        edges: Vec::new(),
                        affected: 0,
                        bound_vids: std::collections::BTreeSet::new(),
                    });
                }
                Ok(DispatchOutcome {
                    result: CommandResult::Cypher(rows),
                    written,
                })
            }
            CompiledCommand::Calibrate(cc) => {
                // Before the program's first write, CALIBRATE runs inline in
                // `run_program`. After it, here, so its collection query sees
                // the state the earlier commands left.
                let model = program.model_catalog.get(&cc.model_name).ok_or_else(|| {
                    LocyError::EvaluationError {
                        message: format!("CALIBRATE: unknown model '{}'", cc.model_name),
                    }
                })?;
                let query = uni_query::query::df_graph::locy_calibrate::calibrate_collection_query(
                    cc, model,
                );
                let rows = ctx.execute_cypher_read(query).await?;
                let result = uni_query::query::df_graph::locy_calibrate::run_calibrate(
                    cc,
                    &program.model_catalog,
                    &Arc::new(config.classifier_registry.clone()),
                    rows,
                )
                .await
                .map_err(|e| LocyError::EvaluationError {
                    message: format!("CALIBRATE error: {e}"),
                })?;
                Ok(DispatchOutcome::read(CommandResult::Calibrate(result)))
            }
            CompiledCommand::Validate(cv) => {
                // Before the program's first write, VALIDATE runs inline in
                // `run_program`. After it, here, against the facts and the
                // graph of the state the earlier commands left.
                let rule_key_cols: Vec<String> = program
                    .rule_catalog
                    .get(&cv.rule_name)
                    .map(|r| {
                        r.yield_schema
                            .iter()
                            .filter(|c| c.is_key)
                            .map(|c| c.name.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                let query = uni_query::query::df_graph::locy_validate::validate_collection_query(
                    cv,
                    &rule_key_cols,
                );
                let target_rows = ctx.execute_cypher_read(query).await?;
                let rule_facts: Vec<FactRow> = facts
                    .raw()
                    .and_then(|raw| raw.get(&cv.rule_name))
                    .map(|batches| record_batches_to_locy_rows(batches))
                    .unwrap_or_default();
                let result = uni_query::query::df_graph::locy_validate::run_validate(
                    cv,
                    &rule_key_cols,
                    &rule_facts,
                    target_rows,
                )
                .map_err(|e| LocyError::EvaluationError {
                    message: format!("VALIDATE error: {e}"),
                })?;
                Ok(DispatchOutcome::read(CommandResult::Validate(result)))
            }
        }
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Collect the distinct VIDs appearing under `vid_columns` across `rows`.
fn unique_vids_in(rows: &[FactRow], vid_columns: &HashSet<String>) -> HashSet<i64> {
    rows.iter()
        .flat_map(|row| {
            vid_columns.iter().filter_map(|col| {
                if let Some(Value::Int(vid)) = row.get(col) {
                    Some(*vid)
                } else {
                    None
                }
            })
        })
        .collect()
}

/// A rule's entity-ID columns (`UInt64`) and which ID space each holds.
fn entity_id_columns(
    batches: &[arrow_array::RecordBatch],
    edges: Option<&HashSet<String>>,
) -> HashMap<String, EntityKind> {
    batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .filter(|f| *f.data_type() == arrow_schema::DataType::UInt64)
                .map(|f| {
                    let kind = if edges.is_some_and(|e| e.contains(f.name())) {
                        EntityKind::Edge
                    } else {
                        EntityKind::Node
                    };
                    (f.name().clone(), kind)
                })
                .collect()
        })
        .unwrap_or_default()
}

impl NativeExecutionAdapter<'_> {
    /// Resolve entity IDs of one kind: VIDs by [`lookup_vertices`], EIDs by
    /// [`lookup_edges`].
    ///
    /// IDs that do not resolve are absent from the map; the caller decides
    /// whether that is an error.
    async fn lookup_entities_by_id(
        &self,
        ids: &[u64],
        kind: EntityKind,
    ) -> std::result::Result<HashMap<u64, Value>, LocyError> {
        let ids: HashSet<i64> = ids
            .iter()
            .filter_map(|&id| i64::try_from(id).ok())
            .collect();
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ctx = self.lookup_ctx();
        let found = match kind {
            EntityKind::Node => lookup_vertices(&ids, &ctx, &self.session_ctx).await,
            EntityKind::Edge => lookup_edges(&ids, &ctx).await,
        };
        found
            .map(|map| {
                map.into_iter()
                    .filter_map(|(id, entity)| u64::try_from(id).ok().map(|id| (id, entity)))
                    .collect()
            })
            .map_err(|message| LocyError::EvaluationError {
                message: format!("entity lookup failed: {message}"),
            })
    }
}

/// Which ID space a fact column's integers belong to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EntityKind {
    Node,
    Edge,
}

/// Replace every ID-valued cell under `columns` with its resolved entity,
/// leaving cells whose ID did not resolve untouched.
fn substitute_entities(
    rows: Vec<FactRow>,
    columns: &HashMap<String, EntityKind>,
    resolved: &HashMap<EntityKind, &HashMap<i64, Value>>,
) -> Vec<FactRow> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(k, v)| {
                    if let Some(kind) = columns.get(&k)
                        && let Value::Int(id) = &v
                    {
                        let new_v = resolved
                            .get(kind)
                            .and_then(|m| m.get(id))
                            .cloned()
                            .unwrap_or(v);
                        return (k, new_v);
                    }
                    (k, v)
                })
                .collect()
        })
        .collect()
}

/// Minimum wall-clock budget granted to a VID hydration lookup.
///
/// Hydration is bounded work — one lookup per entity kind over a known ID
/// list — but it runs *after* the fixpoint, so it inherits whatever is left of
/// the evaluation deadline. A fixpoint that consumed the budget used to leave
/// hydration racing a few microseconds, and the resulting timeout was swallowed:
/// the rule's KEY columns silently stayed raw integers while every other rule in
/// the same run came back as `Value::Node`. The encoding of a result must not
/// depend on timing, so an inherited remainder below this floor is raised to it.
/// A caller whose deadline is already more generous keeps their own.
const VID_HYDRATION_MIN_BUDGET: Duration = Duration::from_secs(30);

/// Resolve EIDs to `Value::Edge`s by looking up exactly those relationships.
///
/// The relationship counterpart of [`lookup_vertices`]. This used to run
/// `MATCH ()-[entity]->() WHERE id(entity) IN [...]`, which has no EID
/// pushdown: it walked every vertex and every relationship of the graph and
/// tested each against the whole literal list, O(E × |eids|) — 1.15 s for 8k
/// EIDs. Now:
///
/// - endpoints and type come from `StorageManager::lookup_edges_by_eids` — the
///   L0 chain (transaction, hypothetical and pending-flush buffers, with their
///   tombstones), then the main edges table read by `_eid` through the
///   context's storage, which is the fork's branch on a fork and is bounded by
///   the snapshot's version when pinned. Under a read-write transaction every
///   requested EID joins the SSI read set there;
/// - properties come from `PropertyManager::get_batch_edge_props` with
///   `_all_props`, hinted with the types just resolved — the call the Cypher
///   traversal makes to materialise `RETURN r`, so both read the same tiers
///   with the same precedence.
///
/// The entity is the `_eid` / `_type` / `_src` / `_dst` / `_all_props` map
/// canonicalised by [`Value::canonical_entity`], with endpoints in stored
/// orientation, as `MATCH ()-[r]->()` binds them.
///
/// # Errors
///
/// Returns a description of the failure if either read fails. EIDs that do not
/// resolve are absent from the map; the caller decides whether that is a
/// failure.
async fn lookup_edges(
    ids: &HashSet<i64>,
    graph_ctx: &Arc<uni_query::query::df_graph::GraphExecutionContext>,
) -> std::result::Result<HashMap<i64, Value>, String> {
    use uni_common::core::id::{Eid, Vid};

    let eids: Vec<Eid> = ids
        .iter()
        .filter_map(|&id| u64::try_from(id).ok())
        .map(Eid::from)
        .collect();
    if eids.is_empty() {
        return Ok(HashMap::new());
    }
    let query_ctx = graph_ctx.query_context();
    let records = graph_ctx
        .storage()
        .lookup_edges_by_eids(&eids, Some(&query_ctx))
        .await
        .map_err(|e| format!("EID lookup failed: {e}"))?;
    if records.is_empty() {
        return Ok(HashMap::new());
    }
    let found: Vec<Eid> = records.keys().copied().collect();
    let types: Vec<String> = records
        .values()
        .map(|r| r.edge_type.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let mut props = graph_ctx
        .property_manager()
        .get_batch_edge_props(&found, &["_all_props"], Some(&types), Some(&query_ctx))
        .await
        .map_err(|e| format!("EID lookup failed to read properties: {e}"))?;

    let mut resolved = HashMap::with_capacity(records.len());
    for (eid, record) in records {
        let id = i64::try_from(eid.as_u64())
            .map_err(|_| format!("EID lookup: {eid:?} does not fit an Int"))?;
        let endpoint = |vid: Vid| {
            i64::try_from(vid.as_u64())
                .map(Value::Int)
                .map_err(|_| format!("EID lookup: endpoint {vid:?} does not fit an Int"))
        };
        let properties = props.remove(&Vid::from(eid.as_u64())).unwrap_or_default();
        let entity = HashMap::from([
            ("_eid".to_string(), Value::Int(id)),
            ("_type".to_string(), Value::String(record.edge_type)),
            ("_src".to_string(), endpoint(record.src_vid)?),
            ("_dst".to_string(), endpoint(record.dst_vid)?),
            ("_all_props".to_string(), Value::Map(properties)),
        ]);
        resolved.insert(id, Value::Map(entity).canonical_entity());
    }
    Ok(resolved)
}

/// Resolve VIDs to `Value::Node`s by scanning exactly those vertices.
///
/// This is the same scan `MATCH (entity) WHERE id(entity) IN [...] RETURN
/// entity` plans — a label-less vertex scan with the VID list pushed down to
/// Lance and to L0 — without the two costs that statement adds on top. The
/// statement had to be parsed with every VID as a literal (130 ms for 8k VIDs),
/// and its residual `FilterExec` re-evaluated the `IN` per row against the
/// whole list, O(N²): 38.6 ms → 585.8 ms for 2k → 8k VIDs while the scan
/// beneath it went 5.7 ms → 19.6 ms. The scan already returns only the listed
/// VIDs, and a filter can only remove rows, so dropping it changes no answer:
/// anything not asked for is ignored when the result is substituted.
///
/// The node is built the way the Cypher read path builds it: the scan's
/// `_vid`, `_labels` and `_all_props` columns, decoded by `arrow_to_value`, as
/// an entity map canonicalised by [`Value::canonical_entity`]. Visibility is
/// the scan's, so `graph_ctx` decides it: its L0 chain (transaction and
/// hypothetical overlays included), its storage (pinned snapshot, fork branch)
/// and its tombstones.
///
/// # Errors
///
/// Returns a description of the failure if the scan cannot run or its output
/// cannot be decoded. VIDs that do not resolve are absent from the map.
async fn lookup_vertices(
    ids: &HashSet<i64>,
    graph_ctx: &Arc<uni_query::query::df_graph::GraphExecutionContext>,
    session_ctx: &Arc<parking_lot::RwLock<datafusion::prelude::SessionContext>>,
) -> std::result::Result<HashMap<i64, Value>, String> {
    use datafusion::physical_plan::ExecutionPlan;
    use uni_query::query::df_graph::{GraphScanExec, ReadSetRecordingExec};

    const VARIABLE: &str = "entity";
    let vids: Vec<u64> = ids
        .iter()
        .filter_map(|&id| u64::try_from(id).ok())
        .collect();
    if vids.is_empty() {
        return Ok(HashMap::new());
    }
    let scan: Arc<dyn ExecutionPlan> = Arc::new(
        GraphScanExec::new_schemaless_all_scan(
            graph_ctx.clone(),
            VARIABLE,
            vec!["_all_props".to_string()],
            None,
        )
        .with_vid_list_filter(vids),
    );
    // Inside a transaction the planner wraps every scan so the rows it returns
    // join the SSI read set; this read is no exception.
    let records_reads = graph_ctx
        .l0_context()
        .transaction_l0
        .as_ref()
        .is_some_and(|l0| l0.read().occ_read_set.is_some());
    let plan: Arc<dyn ExecutionPlan> = if records_reads {
        Arc::new(ReadSetRecordingExec::new(scan, graph_ctx.clone(), VARIABLE))
    } else {
        scan
    };
    let task_ctx = session_ctx.read().task_ctx();
    let batches = datafusion::physical_plan::collect(plan, task_ctx)
        .await
        .map_err(|e| format!("VID lookup failed to execute: {e}"))?;

    let mut resolved = HashMap::with_capacity(ids.len());
    for batch in &batches {
        let column = |field: &str| {
            batch
                .column_by_name(&format!("{VARIABLE}.{field}"))
                .ok_or_else(|| format!("VID lookup: scan output lacks `{VARIABLE}.{field}`"))
        };
        let (vid_col, labels_col, props_col) =
            (column("_vid")?, column("_labels")?, column("_all_props")?);
        for row in 0..batch.num_rows() {
            let decode = |col: &arrow_array::ArrayRef| {
                uni_store::storage::arrow_convert::arrow_to_value(col.as_ref(), row, None)
                    .map_err(|e| format!("VID lookup failed to decode: {e}"))
            };
            let vid = decode(vid_col)?;
            let Value::Int(id) = vid else {
                return Err(format!("VID lookup: `_vid` decoded as {vid:?}"));
            };
            let entity = HashMap::from([
                ("_vid".to_string(), vid),
                ("_labels".to_string(), decode(labels_col)?),
                ("_all_props".to_string(), decode(props_col)?),
            ]);
            resolved.insert(id, Value::Map(entity).canonical_entity());
        }
    }
    Ok(resolved)
}

/// Hydrate raw VID and EID columns in `derived` into `Value::Node` and
/// `Value::Edge` objects.
///
/// A rule's facts carry a node as its VID and a single-hop relationship as its
/// EID, both `UInt64`; `rule_catalog` says which columns hold relationships
/// (`uni_locy::entity_columns`), since the two ID spaces overlap and resolving
/// an EID as a VID would substitute an unrelated node.
///
/// Returns the enriched rows and any warnings describing rules whose hydration
/// could not complete; those rules keep integer IDs, and the warning is how a
/// caller learns the shape it got is not the shape it expected.
///
/// With a `deadline` (a program that may not return partial facts), the
/// lookups run under it, and once it passed the hydration stops with
/// [`DeadlinePassed`]: such a program fails with `Timeout` anyway. Without
/// one, the lookups get at least [`VID_HYDRATION_MIN_BUDGET`], so the facts
/// an `allow_partial` program returns are hydrated whatever the timing.
async fn enrich_vids_with_nodes(
    native_store: &uni_query::query::df_graph::DerivedStore,
    rule_catalog: &HashMap<String, uni_locy::types::CompiledRule>,
    derived: HashMap<String, Vec<FactRow>>,
    graph_ctx: &Arc<uni_query::query::df_graph::GraphExecutionContext>,
    session_ctx: &Arc<parking_lot::RwLock<datafusion::prelude::SessionContext>>,
    deadline: Option<Instant>,
) -> std::result::Result<(HashMap<String, Vec<FactRow>>, Vec<RuntimeWarning>), DeadlinePassed> {
    let mut enriched = HashMap::new();
    let mut warnings: Vec<RuntimeWarning> = Vec::new();
    let edge_columns = uni_locy::entity_columns::edge_columns(rule_catalog);
    let passed = || deadline.is_some_and(|d| Instant::now() >= d);
    if passed() {
        return Err(DeadlinePassed);
    }

    // Give the bounded lookup a usable budget when the inherited one is spent.
    let now = std::time::Instant::now();
    let floor = now + VID_HYDRATION_MIN_BUDGET;
    // `None` means "no deadline at all" and must stay unbounded — only an
    // inherited deadline too small for the bounded lookup is raised to the floor.
    let enrich_ctx: Arc<uni_query::query::df_graph::GraphExecutionContext> =
        match (deadline, graph_ctx.deadline_for_host()) {
            (Some(d), _) => Arc::new((**graph_ctx).clone().with_deadline(d)),
            (None, Some(d)) if d < floor => Arc::new((**graph_ctx).clone().with_deadline(floor)),
            _ => graph_ctx.clone(),
        };

    // Every rule's ID columns, then the union of every rule's IDs per kind: one
    // lookup per kind for the whole store rather than one per rule, so a vertex
    // that several rules derive is read once.
    let id_columns: HashMap<String, HashMap<String, EntityKind>> = derived
        .keys()
        .map(|name| {
            let columns = entity_id_columns(
                native_store.get(name).map_or(&[][..], Vec::as_slice),
                edge_columns.get(name),
            );
            (name.clone(), columns)
        })
        .collect();
    let ids_of = |rows: &[FactRow], columns: &HashMap<String, EntityKind>, kind: EntityKind| {
        let of_kind: HashSet<String> = columns
            .iter()
            .filter(|(_, k)| **k == kind)
            .map(|(c, _)| c.clone())
            .collect();
        unique_vids_in(rows, &of_kind)
    };
    let mut wanted: HashMap<EntityKind, HashSet<i64>> = HashMap::new();
    for (name, rows) in &derived {
        for kind in [EntityKind::Node, EntityKind::Edge] {
            let ids = ids_of(rows, &id_columns[name], kind);
            if !ids.is_empty() {
                wanted.entry(kind).or_default().extend(ids);
            }
        }
    }
    let mut lookups: HashMap<EntityKind, std::result::Result<HashMap<i64, Value>, String>> =
        HashMap::new();
    for (kind, ids) in &wanted {
        let lookup = match kind {
            EntityKind::Node => lookup_vertices(ids, &enrich_ctx, session_ctx).await,
            EntityKind::Edge => lookup_edges(ids, &enrich_ctx).await,
        };
        // A lookup the deadline stopped is the program's cut, not a rule
        // left unhydrated.
        if lookup.is_err() && passed() {
            return Err(DeadlinePassed);
        }
        lookups.insert(*kind, lookup);
    }

    for (name, rows) in derived {
        let columns = &id_columns[&name];
        let mut resolved: HashMap<EntityKind, &HashMap<i64, Value>> = HashMap::new();
        // Why every failure is recorded: an unhydrated rule is indistinguishable
        // from an empty one to a caller doing `hasattr(r, "properties")`, so a
        // swallowed error here reads downstream as "no rows matched".
        let mut failures: Vec<String> = Vec::new();
        for kind in [EntityKind::Node, EntityKind::Edge] {
            let ids = ids_of(&rows, columns, kind);
            if ids.is_empty() {
                continue;
            }
            let what = match kind {
                EntityKind::Node => "VID",
                EntityKind::Edge => "EID",
            };
            match lookups.get(&kind) {
                // All or nothing per rule and kind, as when each rule ran its
                // own lookup: a rule is never returned half hydrated.
                Some(Ok(map)) => {
                    let found = ids.iter().filter(|id| map.contains_key(id)).count();
                    if found == ids.len() {
                        resolved.insert(kind, map);
                    } else {
                        failures.push(format!(
                            "{what} lookup resolved {found}/{} entities",
                            ids.len()
                        ));
                    }
                }
                Some(Err(message)) => failures.push(message.clone()),
                None => failures.push(format!("{what} lookup did not run")),
            }
        }

        for message in failures {
            tracing::warn!(rule = %name, "{message}; KEY columns keep raw IDs");
            warnings.push(RuntimeWarning {
                code: RuntimeWarningCode::EntityHydrationIncomplete,
                message: format!("{message}; KEY columns keep raw IDs"),
                rule_name: name.clone(),
                variable_count: None,
                key_group: None,
            });
        }

        let rows = substitute_entities(rows, columns, &resolved);
        enriched.insert(name, rows);
    }

    Ok((enriched, warnings))
}

#[allow(clippy::too_many_arguments)]
fn build_locy_result(
    derived: HashMap<String, Vec<FactRow>>,
    command_results: Vec<CommandResult>,
    compiled: &CompiledProgram,
    evaluation_time: Duration,
    plan_time: Duration,
    mut orchestrator_stats: LocyStats,
    warnings: Vec<RuntimeWarning>,
    approximate_groups: HashMap<String, Vec<String>>,
    derived_fact_set: Option<DerivedFactSet>,
    incomplete: Option<uni_common::LocyIncomplete>,
    counters: uni_store::CounterSnapshot,
) -> LocyResult {
    // The goal-directed rewrite's copies and magic rules are evaluation
    // detail, not rules of the program.
    let mut derived = derived;
    derived.retain(|name, _| !uni_locy::magic::is_goal_directed_rule(name));
    let total_facts: usize = derived.values().map(|v| v.len()).sum();
    // Reflect how far evaluation actually got: the full count for a complete
    // run, or the recorded completed-strata count when it was cut short.
    orchestrator_stats.strata_evaluated = incomplete
        .as_ref()
        .map_or(compiled.strata.len(), |d| d.completed_strata);
    orchestrator_stats.derived_nodes = total_facts;
    orchestrator_stats.evaluation_time = evaluation_time;

    let inner = uni_locy::LocyResult {
        derived,
        stats: orchestrator_stats,
        command_results,
        warnings,
        compile_warnings: compiled.warnings.clone(),
        approximate_groups,
        derived_fact_set,
        incomplete,
    };
    // The counters are one `Arc` of atomics shared by the whole evaluation, so
    // this total already spans every fixpoint iteration — there is nothing to
    // sum here. They were ticking all along; only the harvest was missing, so
    // `metrics().rows_scanned` read 0 however much the rules scanned.
    // `evaluation_time` spans planning too; `exec_time` is what follows it,
    // so `plan_time + exec_time = total_time` as for a Cypher query.
    let mut metrics = QueryMetrics {
        total_time: evaluation_time,
        plan_time,
        exec_time: evaluation_time.saturating_sub(plan_time),
        rows_returned: total_facts,
        ..Default::default()
    };
    metrics.apply_counters(&counters);
    LocyResult::new(inner, metrics)
}

/// The budget a conversion or hydration ran out of: its deadline passed
/// before it finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeadlinePassed;

/// `native`'s facts as rows, for the commands and the result.
///
/// With a `deadline` (a program that may not return partial facts), the
/// conversion checks it before each batch and stops once it passed: such a
/// program then fails with `Timeout` anyway, so finishing the conversion
/// first only made it fail late. Under `allow_partial` the facts are
/// returned complete, as documented, so no deadline is given.
fn native_store_to_row_store(
    native: &uni_query::query::df_graph::DerivedStore,
    compiled: &CompiledProgram,
    deadline: Option<Instant>,
) -> std::result::Result<RowStore, DeadlinePassed> {
    let mut result = RowStore::new();
    for name in native.rule_names() {
        if let Some(batches) = native.get(name) {
            let mut rows = Vec::new();
            for batch in batches {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    return Err(DeadlinePassed);
                }
                rows.extend(record_batches_to_locy_rows(std::slice::from_ref(batch)));
            }
            let rule = compiled.rule_catalog.get(name);
            let columns: Vec<String> = rule
                .map(|r| r.yield_schema.iter().map(|yc| yc.name.clone()).collect())
                .unwrap_or_else(|| {
                    rows.first()
                        .map(|r| r.keys().cloned().collect())
                        .unwrap_or_default()
                });
            result.insert(name.to_string(), RowRelation::new(columns, rows));
        }
    }
    Ok(result)
}

// ── Error mapping ──────────────────────────────────────────────────────────

fn map_parse_error(e: uni_cypher::ParseError) -> UniError {
    UniError::Parse {
        message: format!("LocyParseError: {e}"),
        position: None,
        line: None,
        column: None,
        context: None,
    }
}

pub(crate) fn map_compile_error(e: LocyCompileError) -> UniError {
    UniError::Query {
        message: format!("LocyCompileError: {e}"),
        query: None,
    }
}

/// The budgets a Locy evaluation's executor runs under.
///
/// Carried to the error mappers so a refusal reports the limit it hit: the
/// operator that trips a deadline or a pool reservation cannot know it.
#[derive(Debug, Clone, Copy)]
struct LocyLimits {
    /// The deadline every operator checks (`UniConfig::query_timeout` as
    /// resolved by [`LocyEngine::executor_config`]).
    timeout: std::time::Duration,
    /// The executor's memory pool (`max_query_memory`).
    max_query_memory: usize,
    /// A single derived relation's budget (`LocyConfig::max_derived_bytes`).
    max_derived_bytes: usize,
}

impl LocyLimits {
    fn new(executor_config: &uni_common::UniConfig, locy: &LocyConfig) -> Self {
        Self {
            timeout: executor_config.query_timeout,
            max_query_memory: executor_config.max_query_memory,
            max_derived_bytes: locy.max_derived_bytes,
        }
    }
}

fn map_runtime_error(e: LocyError, limits: LocyLimits) -> UniError {
    match e {
        LocyError::SavepointFailed { ref message } => UniError::Transaction {
            message: format!("LocyRuntimeError: {message}"),
        },
        // A command's own budget check. The same condition the operator
        // deadline reports as `Timeout`, so it must not surface as a failed
        // program merely because a different layer noticed it (#289).
        LocyError::Timeout { limit, .. } => UniError::Timeout {
            timeout_ms: u64::try_from(limit.as_millis()).unwrap_or(u64::MAX),
        },
        // A re-evaluation cut short: the same report the program's own
        // cut-short run gives.
        LocyError::Incomplete { detail } => UniError::LocyIncomplete { detail },
        other => map_native_df_error(other, limits),
    }
}

/// Map an error from the Locy executor to a `UniError`.
///
/// A program refused for cost — deadline, cancellation, the operator memory
/// pool, a derived relation's `max_derived_bytes` — gets the typed variant the
/// Cypher path raises for the same condition. Everything else is a failed
/// program and stays `UniError::Query`. This used to wrap every error as
/// `Query`, so a caller could tell "too expensive" from "wrong" only by
/// matching the message (#289).
fn map_native_df_error(e: impl std::fmt::Display, limits: LocyLimits) -> UniError {
    let msg = e.to_string();
    if msg.contains(uni_query::query::df_graph::locy_errors::DERIVED_BYTES_LIMIT_MARKER) {
        return UniError::MemoryLimitExceeded {
            limit_bytes: limits.max_derived_bytes,
            message: msg,
        };
    }
    if let Some(refusal) = crate::api::impl_query::classify_limit_refusal(
        &msg,
        limits.timeout,
        limits.max_query_memory,
    ) {
        return refusal;
    }
    if let Some(missing) = UniError::parameter_missing_from_message(&msg) {
        return missing;
    }
    UniError::Query {
        message: format!("LocyRuntimeError: {msg}"),
        query: None,
    }
}
