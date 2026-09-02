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
    LocyError, LocyStats, RuntimeWarning,
};
use uni_query::{QueryMetrics, QueryPlanner};

use crate::api::locy_result::LocyResult;
use uni_query::query::df_graph::locy_ast_builder::build_match_return_query;
use uni_query::query::df_graph::locy_delta::{RowRelation, RowStore, extract_cypher_conditions};
use uni_query::query::df_graph::locy_derive::CollectedDeriveOutput;
use uni_query::query::df_graph::locy_eval::record_batches_to_locy_rows;
use uni_query::query::df_graph::locy_explain::ProvenanceStore;
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
pub(crate) fn merge_registered_rules(compiled: &mut CompiledProgram, registry: &LocyRuleRegistry) {
    if registry.rules.is_empty() {
        return;
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
pub(crate) async fn evaluate_with_db_and_config(
    db: &crate::api::UniInner,
    program: &str,
    config: &LocyConfig,
    rule_registry: &std::sync::RwLock<LocyRuleRegistry>,
    cancel: crate::api::impl_query::CancelScope,
) -> Result<LocyResult> {
    evaluate_with_db_and_config_capturing(db, program, config, rule_registry, None, cancel).await
}

/// Like [`evaluate_with_db_and_config`], but optionally captures a structured
/// execution profile into `profile_capture` (the session `profile()` path).
pub(crate) async fn evaluate_with_db_and_config_capturing(
    db: &crate::api::UniInner,
    program: &str,
    config: &LocyConfig,
    rule_registry: &std::sync::RwLock<LocyRuleRegistry>,
    profile_capture: Option<&Arc<std::sync::Mutex<Option<uni_query::LocyExecProfile>>>>,
    cancel: crate::api::impl_query::CancelScope,
) -> Result<LocyResult> {
    // Compile with the given registry
    let ast = match uni_cypher::parse_locy(program) {
        Ok(ast) => ast,
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
    merge_registered_rules(&mut compiled, &rule_registry.read().unwrap());

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
        tx_l0_override: locy_l0.clone(),
        locy_l0,
        collect_derive: true,
        read_snapshot: None,
        cancel,
    };
    engine
        .evaluate_compiled_capturing(compiled, config, profile_capture)
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
        let ast = uni_cypher::parse_locy(program).map_err(map_parse_error)?;
        let registry = self.db.locy_rule_registry.read().unwrap();
        let external_names: Vec<String> = registry.rules.keys().cloned().collect();
        drop(registry);
        let oracle = plugin_monotonicity_oracle(&self.db.plugin_registry);
        uni_locy::compile_with_oracle_and_config(
            &ast,
            &HashMap::new(),
            &external_names,
            config,
            &oracle,
        )
        .map_err(map_compile_error)
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
        let mut compiled = self.compile_only_with_config(program, config)?;

        // Merge registered rules into the compiled program.
        merge_registered_rules(&mut compiled, &self.db.locy_rule_registry.read().unwrap());

        self.evaluate_compiled_capturing(compiled, config, profile_capture)
            .await
    }

    /// Evaluate an already-compiled Locy program with custom config.
    ///
    /// This is the core execution path: it takes a `CompiledProgram` (with any
    /// registry merges already applied) and runs it through planning, execution,
    /// and command dispatch.
    pub async fn evaluate_compiled_with_config(
        &self,
        compiled: CompiledProgram,
        config: &LocyConfig,
    ) -> Result<LocyResult> {
        self.evaluate_compiled_capturing(compiled, config, None)
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
    pub(crate) async fn evaluate_compiled_capturing(
        &self,
        compiled: CompiledProgram,
        config: &LocyConfig,
        profile_capture: Option<&Arc<std::sync::Mutex<Option<uni_query::LocyExecProfile>>>>,
    ) -> Result<LocyResult> {
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
        let start = Instant::now();

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

        // 1. Build logical plan
        let schema = self.db.schema.schema();
        let query_planner = uni_query::QueryPlanner::new(schema);
        let plan_builder = uni_query::query::locy_planner::LocyPlanBuilder::new(&query_planner);
        let logical = plan_builder
            .build_program_plan_with_full_neural(
                &compiled,
                config.max_iterations,
                config.timeout,
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
                message: format!("LocyPlanBuildError: {e}"),
                query: None,
            })?;

        // 2. Create executor + physical planner
        let mut df_executor = uni_query::Executor::new(self.db.storage.clone());
        df_executor.set_config(self.db.config.clone());
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
            .map_err(map_native_df_error)?;

        // 3. Physical plan
        let exec_plan = planner.plan(&logical).map_err(map_native_df_error)?;

        // 4. Create tracker for EXPLAIN commands or shared-proof detection
        let has_explain = compiled
            .commands
            .iter()
            .any(|c| matches!(c, CompiledCommand::ExplainRule(_)));
        let has_prob_fold = compiled.strata.iter().any(|s| {
            s.rules.iter().any(|r| {
                r.clauses.iter().any(|c| {
                    c.fold.iter().any(|f| {
                        if let uni_cypher::ast::Expr::FunctionCall { name, .. } = &f.aggregate {
                            matches!(name.to_uppercase().as_str(), "MNOR" | "MPROD")
                        } else {
                            false
                        }
                    })
                })
            })
        });
        let needs_tracker = has_explain || has_prob_fold;
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
        ) = if let Some(program_exec) = exec_plan
            .as_any()
            .downcast_ref::<uni_query::query::df_graph::LocyProgramExec>(
        ) {
            if let Some(ref t) = tracker {
                program_exec.set_derivation_tracker(Arc::clone(t));
            }
            if profile_capture.is_some() {
                program_exec.set_profile_enabled(true);
            }
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

        // 5. Execute strata
        let _stats_batches = uni_query::Executor::collect_batches(&session_ctx, exec_plan)
            .await
            .map_err(map_native_df_error)?;

        // 5b. Hand the captured execution profile back to the profile() caller.
        if let (Some(capture), Some(slot)) = (profile_capture, profile_slot.as_ref())
            && let Ok(mut p) = slot.write()
            && let Some(prof) = p.take()
        {
            *capture.lock().unwrap_or_else(|e| e.into_inner()) = Some(prof);
        }

        // 6. Extract native DerivedStore
        let native_store = derived_store_slot
            .write()
            .unwrap()
            .take()
            .unwrap_or_default();

        // 7. Convert native DerivedStore → row-based RowStore for SLG/EXPLAIN
        let mut orch_store = native_store_to_row_store(&native_store, &compiled);

        // 7b. Enrich VID integers → full Node objects so SLG/QUERY can access
        //     node properties (d.name etc.) and IS-ref joins work correctly
        //     across FOLD-rule boundaries.
        {
            let orch_rows: HashMap<String, Vec<FactRow>> = orch_store
                .iter()
                .map(|(k, v)| (k.clone(), v.rows.clone()))
                .collect();
            let enriched_rows = enrich_vids_with_nodes(
                self.db,
                &native_store,
                orch_rows,
                planner.graph_ctx(),
                planner.session_ctx(),
            )
            .await;
            for (name, rows) in enriched_rows {
                if let Some(rel) = orch_store.get_mut(&name) {
                    rel.rows = rows;
                }
            }
        }

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
        let timed_out_early = timeout_flag.load(std::sync::atomic::Ordering::Relaxed) != 0;
        // Skip command dispatch when evaluation was cut short — the partial
        // derived store may be incomplete and SLG/QUERY would hit the expired
        // budget.
        if !timed_out_early {
            for (cmd_idx, cmd) in compiled.commands.iter().enumerate() {
                if let Some(result) = inline_map.get(&cmd_idx) {
                    // Already executed inline by run_program
                    command_results.push(result.clone());
                    continue;
                }
                let result = dispatch_native_command(
                    cmd,
                    &compiled,
                    &native_ctx,
                    config,
                    &mut orch_store,
                    &mut locy_stats,
                    tracker.clone(),
                    start,
                    &approx_for_explain,
                    self.collect_derive,
                    &mut collected_derives,
                )
                .await
                .map_err(map_runtime_error)?;
                command_results.push(result);
            }
        }

        let evaluation_time = start.elapsed();

        // 9. Build derived map, enrich VID columns with full nodes
        let mut base_derived: HashMap<String, Vec<FactRow>> = native_store
            .rule_names()
            .filter_map(|name| {
                native_store
                    .get(name)
                    .map(|batches| (name.to_string(), record_batches_to_locy_rows(batches)))
            })
            .collect();

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

        let enriched_derived = enrich_vids_with_nodes(
            self.db,
            &native_store,
            base_derived,
            planner.graph_ctx(),
            planner.session_ctx(),
        )
        .await;

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
            })
        } else {
            None
        };

        // 11. Build final LocyResult
        let warnings = warnings_slot.read().map(|w| w.clone()).unwrap_or_default();
        let incomplete = incomplete_slot.read().ok().and_then(|g| g.clone());
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
            locy_stats,
            warnings,
            approximate_groups,
            derived_fact_set,
            incomplete,
        ))
    }

    /// Run only the fixpoint strata (no commands) via the native DataFusion path.
    ///
    /// Used by `re_evaluate_strata()` so that savepoint-scoped mutations from
    /// ASSUME/ABDUCE hypothetical states are visible — the `Executor` is configured
    /// with `self.db.writer`, which holds the active transaction handle.
    async fn run_strata_native(
        &self,
        compiled: &CompiledProgram,
        config: &LocyConfig,
    ) -> Result<uni_query::query::df_graph::DerivedStore> {
        let schema = self.db.schema.schema();
        let query_planner = uni_query::QueryPlanner::new(schema);
        let plan_builder = uni_query::query::locy_planner::LocyPlanBuilder::new(&query_planner);
        let logical = plan_builder
            .build_program_plan_with_semiring(
                compiled,
                config.max_iterations,
                config.timeout,
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
            )
            .map_err(|e| UniError::Query {
                message: format!("LocyPlanBuildError: {e}"),
                query: None,
            })?;

        let mut df_executor = uni_query::Executor::new(self.db.storage.clone());
        df_executor.set_config(self.db.config.clone());
        if let Some(ref w) = self.db.writer {
            df_executor.set_writer(w.clone());
        }
        // Pass the tx_l0_override so the fixpoint planner sees uncommitted mutations
        // (ASSUME/ABDUCE hypothetical state, session DERIVE mutations, etc.)
        if let Some(ref l0) = self.tx_l0_override {
            df_executor.set_transaction_l0(l0.clone());
        }
        df_executor.set_xervo_runtime(self.db.xervo_runtime.clone());
        df_executor.set_procedure_registry(self.db.procedure_registry.clone());

        let (session_ctx, planner, _) = df_executor
            .create_datafusion_planner(&self.db.properties, &config.params)
            .await
            .map_err(map_native_df_error)?;
        let exec_plan = planner.plan(&logical).map_err(map_native_df_error)?;

        let derived_store_slot = if let Some(program_exec) =
            exec_plan
                .as_any()
                .downcast_ref::<uni_query::query::df_graph::LocyProgramExec>()
        {
            program_exec.derived_store_slot()
        } else {
            Arc::new(std::sync::RwLock::new(None))
        };

        let _ = uni_query::Executor::collect_batches(&session_ctx, exec_plan)
            .await
            .map_err(map_native_df_error)?;
        Ok(derived_store_slot
            .write()
            .unwrap()
            .take()
            .unwrap_or_default())
    }
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
    /// Cancellation scope, cloned from the `LocyEngine`, for the Cypher
    /// statements command dispatch runs.
    cancel: crate::api::impl_query::CancelScope,
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
            cancel,
        }
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

    /// Execute a Query AST via execute_subplan, reusing the fixpoint contexts.
    async fn execute_query_ast(
        &self,
        ast: Query,
    ) -> std::result::Result<Vec<RecordBatch>, LocyError> {
        let schema = self.db.schema.schema();
        let logical_plan =
            QueryPlanner::new(schema)
                .plan(ast)
                .map_err(|e| LocyError::ExecutorError {
                    message: e.to_string(),
                })?;
        uni_query::query::df_graph::common::execute_subplan(
            &logical_plan,
            &self.params,
            &HashMap::new(),
            &self.graph_ctx,
            &self.session_ctx,
            // Honor the pinned snapshot storage (C2) so fact-extraction reads the
            // same L1 version boundary the fixpoint used (REQ-1b).
            &self.effective_storage(),
            &self.db.schema.schema(),
            None, // Locy fact-extraction path is read-only
        )
        .await
        .map_err(|e| LocyError::ExecutorError {
            message: e.to_string(),
        })
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

        // When a locy_l0 or transaction L0 is active, OR a read snapshot is
        // pinned, the stored graph_ctx may not reflect the right L0/storage view
        // for this dispatch. Rebuild a temporary context.
        //
        // REQ-1b: when a read snapshot is pinned (the `tx.locy()` path), the
        // rebuilt context MUST read base facts from the FROZEN snapshot
        // generations (`snap.main` + `snap.extra`) and the version-pinned
        // storage, exactly like the fixpoint planner did — not live storage and
        // live L0. Otherwise a concurrent commit (or a flush completing
        // mid-transaction) would leak into command-dispatch pattern matching,
        // making a program's result depend on whether it ran via `session.locy()`
        // (no snapshot ⇒ live) vs `tx.locy()` (snapshot ⇒ frozen). The
        // transaction L0 stays live for read-your-writes. PropertyManager
        // intentionally stays on live storage (read-your-writes on properties),
        // matching `Executor::create_datafusion_planner`.
        let tx_l0_for_ctx = self
            .locy_l0
            .lock()
            .unwrap()
            .clone()
            .or_else(|| self.tx_l0_override.clone());
        let transaction_ctx: Option<Arc<uni_query::query::df_graph::GraphExecutionContext>> =
            if tx_l0_for_ctx.is_some() || self.read_snapshot.is_some() {
                if let Some(writer) = self.db.writer.as_ref() {
                    let (current_l0, pending_flush_l0s) = match &self.read_snapshot {
                        Some(snap) => (snap.main.clone(), snap.extra.clone()),
                        None => (
                            writer.l0_manager.get_current(),
                            writer.l0_manager.get_pending_flush(),
                        ),
                    };
                    let l0_ctx = uni_query::query::df_graph::L0Context {
                        current_l0: Some(current_l0),
                        transaction_l0: tx_l0_for_ctx,
                        pending_flush_l0s,
                    };
                    // Derived from `self.graph_ctx`, so it inherits that
                    // context's budget too — otherwise the transaction-scoped
                    // context silently drops the deadline. See #207.
                    Some(Arc::new(carry_budget(
                        uni_query::query::df_graph::GraphExecutionContext::with_l0_context(
                            effective_storage.clone(),
                            l0_ctx,
                            self.graph_ctx.property_manager().clone(),
                        ),
                        &self.graph_ctx,
                    )))
                } else {
                    None
                }
            } else {
                None
            };

        let effective_ctx = transaction_ctx.as_ref().unwrap_or(&self.graph_ctx);

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
        if vids.is_empty() {
            return Ok(HashMap::new());
        }
        // Mirror the existing `lookup_derived_enriched` pattern: build a
        // `MATCH (n) WHERE id(n) IN [...]` Cypher AST and run it through
        // execute_query_ast, then index the returned rows by their vid.
        let vids_literal = vids
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let query_str =
            format!("MATCH (n) WHERE id(n) IN [{vids_literal}] RETURN id(n) AS _vid, n");
        let mut out: HashMap<u64, Value> = HashMap::new();
        if let Ok(ast) = uni_cypher::parse(&query_str)
            && let Ok(batches) = self.execute_query_ast(ast).await
        {
            for row in record_batches_to_locy_rows(&batches) {
                if let (Some(Value::Int(vid)), Some(node)) = (row.get("_vid"), row.get("n"))
                    && *vid >= 0
                {
                    out.insert(*vid as u64, node.clone());
                }
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl LocyExecutionContext for NativeExecutionAdapter<'_> {
    async fn lookup_derived_enriched(
        &self,
        rule_name: &str,
    ) -> std::result::Result<Vec<FactRow>, LocyError> {
        use arrow_schema::DataType;

        if let Some(rule) = self.compiled.rule_catalog.get(rule_name) {
            let is_derive_rule = rule
                .clauses
                .iter()
                .all(|c| matches!(c.output, RuleOutput::Derive(_)));
            if is_derive_rule {
                let mut all_rows = Vec::new();
                for clause in &rule.clauses {
                    let cypher_conds = extract_cypher_conditions(&clause.where_conditions);
                    let raw_batches = self
                        .execute_pattern(&clause.match_pattern, &cypher_conds)
                        .await?;
                    all_rows.extend(record_batches_to_locy_rows(&raw_batches));
                }
                return Ok(all_rows);
            }
        }

        let batches = self
            .native_store
            .get(rule_name)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let rows = record_batches_to_locy_rows(batches);

        let vid_columns: HashSet<String> = batches
            .first()
            .map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .filter(|f| *f.data_type() == DataType::UInt64)
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default();

        if vid_columns.is_empty() {
            return Ok(rows);
        }

        let unique_vids = unique_vids_in(&rows, &vid_columns);

        if unique_vids.is_empty() {
            return Ok(rows);
        }

        let query_str = vid_lookup_query(&unique_vids);
        let mut vid_to_node: HashMap<i64, Value> = HashMap::new();
        if let Ok(ast) = uni_cypher::parse(&query_str)
            && let Ok(batches) = self.execute_query_ast(ast).await
        {
            for row in record_batches_to_locy_rows(&batches) {
                if let (Some(Value::Int(vid)), Some(node)) = (row.get("_vid"), row.get("n")) {
                    vid_to_node.insert(*vid, node.clone());
                }
            }
        }

        Ok(substitute_nodes(rows, &vid_columns, &vid_to_node))
    }

    async fn execute_cypher_read(
        &self,
        ast: Query,
    ) -> std::result::Result<Vec<FactRow>, LocyError> {
        // Route through locy_l0 so trailing Cypher sees DERIVE/ASSUME mutations.
        // locy_l0 is the "active" L0 for this evaluation scope.
        let active_l0 = self.locy_l0.lock().unwrap().clone();
        let result = if let Some(ref l0) = active_l0 {
            self.db
                .execute_ast_internal_with_tx_l0(
                    ast,
                    "<locy>",
                    HashMap::new(),
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
                    HashMap::new(),
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
                    HashMap::new(),
                    self.db.config.clone(),
                    crate::api::impl_query::CancelScope::default(),
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
                crate::api::impl_query::CancelScope::default(),
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
        Ok(())
    }

    async fn re_evaluate_strata(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
    ) -> std::result::Result<RowStore, LocyError> {
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
            tx_l0_override: locy_l0.clone(),
            locy_l0,
            collect_derive: false,
            read_snapshot: None,
            cancel: self.cancel.clone(),
        };
        let native_store = engine
            .run_strata_native(&strata_only, config)
            .await
            .map_err(|e| LocyError::ExecutorError {
                message: e.to_string(),
            })?;
        let mut store = native_store_to_row_store(&native_store, program);

        // Enrich VID integers → full Node objects so SLG/QUERY inside
        // ASSUME/ABDUCE can access node properties and IS-ref joins work.
        let store_rows: HashMap<String, Vec<FactRow>> = store
            .iter()
            .map(|(k, v)| (k.clone(), v.rows.clone()))
            .collect();

        // Enrichment resolves each vid by running `MATCH (n) WHERE id(n) IN [...]`.
        // It MUST read through the same hypothetical `locy_l0` overlay the fixpoint
        // just used — otherwise a node created only inside this ASSUME/ABDUCE block
        // (which lives in the forked overlay, not base storage) is not found, the
        // vid stays a bare `Value::Int`, and any predicate reading its properties
        // (e.g. `QUERY ... WHERE b.name = 'B'`) evaluates `Int.name` → NULL and
        // silently drops the row. Rebuild an overlay-aware context mirroring the
        // command-dispatch path (`dispatch`/`execute_pattern`).
        let enrich_storage = self.effective_storage();
        let enrich_ctx: Arc<uni_query::query::df_graph::GraphExecutionContext> = {
            let tx_l0_for_ctx = self
                .locy_l0
                .lock()
                .unwrap()
                .clone()
                .or_else(|| self.tx_l0_override.clone());
            if (tx_l0_for_ctx.is_some() || self.read_snapshot.is_some())
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
                    transaction_l0: tx_l0_for_ctx,
                    pending_flush_l0s,
                };
                Arc::new(carry_budget(
                    uni_query::query::df_graph::GraphExecutionContext::with_l0_context(
                        enrich_storage.clone(),
                        l0_ctx,
                        self.graph_ctx.property_manager().clone(),
                    ),
                    &self.graph_ctx,
                ))
            } else {
                self.graph_ctx.clone()
            }
        };
        let enriched = enrich_vids_with_nodes(
            self.db,
            &native_store,
            store_rows,
            &enrich_ctx,
            &self.session_ctx,
        )
        .await;
        for (name, rows) in enriched {
            if let Some(rel) = store.get_mut(&name) {
                rel.rows = rows;
            }
        }

        Ok(store)
    }
}

// ── Native command dispatch ───────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn dispatch_native_command<'a>(
    cmd: &'a CompiledCommand,
    program: &'a CompiledProgram,
    ctx: &'a NativeExecutionAdapter<'a>,
    config: &'a LocyConfig,
    orch_store: &'a mut RowStore,
    stats: &'a mut LocyStats,
    tracker: Option<Arc<ProvenanceStore>>,
    start: Instant,
    approximate_groups: &'a HashMap<String, Vec<String>>,
    collect_derive: bool,
    collected_derives: &'a mut Vec<CollectedDeriveOutput>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = std::result::Result<CommandResult, LocyError>> + Send + 'a,
    >,
> {
    Box::pin(async move {
        match cmd {
            CompiledCommand::GoalQuery(gq) => {
                let rows = uni_query::query::df_graph::locy_query::evaluate_query(
                    gq, program, ctx, config, orch_store, stats, start,
                )
                .await?;
                Ok(CommandResult::Query(rows))
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
                Ok(CommandResult::Explain(node))
            }
            CompiledCommand::Assume(ca) => {
                let rows = uni_query::query::df_graph::locy_assume::evaluate_assume(
                    ca, program, ctx, config, stats,
                )
                .await?;
                Ok(CommandResult::Assume(rows))
            }
            CompiledCommand::Abduce(aq) => {
                let result = uni_query::query::df_graph::locy_abduce::evaluate_abduce(
                    aq,
                    program,
                    ctx,
                    config,
                    orch_store,
                    stats,
                    tracker.as_deref(),
                )
                .await?;
                Ok(CommandResult::Abduce(result))
            }
            CompiledCommand::DeriveCommand(dc) => {
                if collect_derive {
                    // Session path: collect ASTs + data for deferred materialization.
                    let output = uni_query::query::df_graph::locy_derive::collect_derive_facts(
                        dc, program, ctx,
                    )
                    .await?;
                    let affected = output.affected;

                    // Replay mutations to the ephemeral L0 so that subsequent
                    // trailing Cypher commands can read the derived edges.
                    // Guard: skip when no L0 exists (read-only DB).
                    // Replay mutations to the ephemeral L0 so that subsequent
                    // trailing Cypher commands can read the derived edges.
                    // Guard: skip when no L0 exists (read-only DB).
                    if ctx.tx_l0_override.is_some() {
                        for query in &output.queries {
                            ctx.execute_mutation(query.clone(), HashMap::new()).await?;
                        }
                    }

                    collected_derives.push(output);
                    Ok(CommandResult::Derive { affected })
                } else {
                    // Transaction path: auto-apply mutations
                    let affected = uni_query::query::df_graph::locy_derive::derive_command(
                        dc, program, ctx, stats,
                    )
                    .await?;
                    Ok(CommandResult::Derive { affected })
                }
            }
            CompiledCommand::Cypher(q) => {
                let rows = ctx.execute_cypher_read(q.clone()).await?;
                stats.queries_executed += 1;
                Ok(CommandResult::Cypher(rows))
            }
            CompiledCommand::Calibrate(cc) => {
                // CALIBRATE runs inline inside LocyProgramExec::run_program
                // and surfaces its result via the command_results_slot;
                // by the time we get here that lookup has already
                // succeeded. Reaching this arm is a programming
                // error (the inline result was missing).
                Err(LocyError::EvaluationError {
                    message: format!(
                        "internal: CALIBRATE '{}' missing inline result; \
                         dispatch_native_command should not have been invoked \
                         for a Calibrate command",
                        cc.model_name
                    ),
                })
            }
            CompiledCommand::Validate(cv) => {
                // VALIDATE, like CALIBRATE, runs inline inside
                // LocyProgramExec::run_program and surfaces via
                // command_results_slot. Reaching this arm is a
                // programming error.
                Err(LocyError::EvaluationError {
                    message: format!(
                        "internal: VALIDATE '{}' missing inline result; \
                         dispatch_native_command should not have been invoked \
                         for a Validate command",
                        cv.rule_name
                    ),
                })
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

/// Build the Cypher statement that resolves each VID back to its node.
fn vid_lookup_query(vids: &HashSet<i64>) -> String {
    let vids_literal = vids
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("MATCH (n) WHERE id(n) IN [{vids_literal}] RETURN id(n) AS _vid, n")
}

/// Replace every VID-valued cell under `vid_columns` with its resolved node,
/// leaving cells whose VID did not resolve untouched.
fn substitute_nodes(
    rows: Vec<FactRow>,
    vid_columns: &HashSet<String>,
    vid_to_node: &HashMap<i64, Value>,
) -> Vec<FactRow> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(k, v)| {
                    if vid_columns.contains(&k)
                        && let Value::Int(vid) = &v
                    {
                        let new_v = vid_to_node.get(vid).cloned().unwrap_or(v);
                        return (k, new_v);
                    }
                    (k, v)
                })
                .collect()
        })
        .collect()
}

async fn enrich_vids_with_nodes(
    db: &crate::api::UniInner,
    native_store: &uni_query::query::df_graph::DerivedStore,
    derived: HashMap<String, Vec<FactRow>>,
    graph_ctx: &Arc<uni_query::query::df_graph::GraphExecutionContext>,
    session_ctx: &Arc<parking_lot::RwLock<datafusion::prelude::SessionContext>>,
) -> HashMap<String, Vec<FactRow>> {
    use arrow_schema::DataType;
    let mut enriched = HashMap::new();

    for (name, rows) in derived {
        let vid_columns: HashSet<String> = native_store
            .get(&name)
            .and_then(|batches| batches.first())
            .map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .filter(|f| *f.data_type() == DataType::UInt64)
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default();

        if vid_columns.is_empty() {
            enriched.insert(name, rows);
            continue;
        }

        let unique_vids = unique_vids_in(&rows, &vid_columns);

        if unique_vids.is_empty() {
            enriched.insert(name, rows);
            continue;
        }

        let query_str = vid_lookup_query(&unique_vids);
        let mut vid_to_node: HashMap<i64, Value> = HashMap::new();
        if let Ok(ast) = uni_cypher::parse(&query_str) {
            let schema = db.schema.schema();
            if let Ok(logical_plan) = uni_query::QueryPlanner::new(schema).plan(ast)
                && let Ok(batches) = uni_query::query::df_graph::common::execute_subplan(
                    &logical_plan,
                    &HashMap::new(),
                    &HashMap::new(),
                    graph_ctx,
                    session_ctx,
                    // Use the (snapshot-pinned, when in a tx) storage carried by
                    // graph_ctx rather than live `db.storage`, so node enrichment
                    // honors the same L1 version boundary as the fixpoint and
                    // does not leak post-snapshot rows from a mid-tx flush
                    // (REQ-1b, C2 storage pin).
                    graph_ctx.storage(),
                    &db.schema.schema(),
                    None, // Locy inline Cypher path is read-only
                )
                .await
            {
                for row in record_batches_to_locy_rows(&batches) {
                    if let (Some(Value::Int(vid)), Some(node)) = (row.get("_vid"), row.get("n")) {
                        vid_to_node.insert(*vid, node.clone());
                    }
                }
            }
        }

        enriched.insert(name, substitute_nodes(rows, &vid_columns, &vid_to_node));
    }

    enriched
}

#[allow(clippy::too_many_arguments)]
fn build_locy_result(
    derived: HashMap<String, Vec<FactRow>>,
    command_results: Vec<CommandResult>,
    compiled: &CompiledProgram,
    evaluation_time: Duration,
    mut orchestrator_stats: LocyStats,
    warnings: Vec<RuntimeWarning>,
    approximate_groups: HashMap<String, Vec<String>>,
    derived_fact_set: Option<DerivedFactSet>,
    incomplete: Option<uni_common::LocyIncomplete>,
) -> LocyResult {
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
    let metrics = QueryMetrics {
        total_time: evaluation_time,
        exec_time: evaluation_time,
        rows_returned: total_facts,
        ..Default::default()
    };
    LocyResult::new(inner, metrics)
}

fn native_store_to_row_store(
    native: &uni_query::query::df_graph::DerivedStore,
    compiled: &CompiledProgram,
) -> RowStore {
    let mut result = RowStore::new();
    for name in native.rule_names() {
        if let Some(batches) = native.get(name) {
            let rows = record_batches_to_locy_rows(batches);
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
    result
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

fn map_compile_error(e: LocyCompileError) -> UniError {
    UniError::Query {
        message: format!("LocyCompileError: {e}"),
        query: None,
    }
}

fn map_runtime_error(e: LocyError) -> UniError {
    match e {
        LocyError::SavepointFailed { ref message } => UniError::Transaction {
            message: format!("LocyRuntimeError: {message}"),
        },
        other => UniError::Query {
            message: format!("LocyRuntimeError: {other}"),
            query: None,
        },
    }
}

fn map_native_df_error(e: impl std::fmt::Display) -> UniError {
    UniError::Query {
        message: format!("LocyRuntimeError: {e}"),
        query: None,
    }
}
