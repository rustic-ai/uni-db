// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Traits for native Locy command dispatch.
//!
//! `DerivedFactSource` provides read-only access to derived facts and the graph,
//! replacing `CypherExecutor` for read operations in the native path.
//!
//! `LocyExecutionContext` extends it with write operations (mutations, L0 fork/restore,
//! and strata re-evaluation) needed by ASSUME, DERIVE, and ABDUCE.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use uni_common::Value;
use uni_cypher::ast::{Expr, Pattern, Query};
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError};

use super::locy_delta::RowStore;
use super::locy_explain::ProvenanceStore;
use super::locy_program::{DerivedStore, ReusedStrata};

/// The facts of one evaluated state, offered for reuse by a hypothetical
/// re-evaluation of the same program (incremental `ASSUME` / `ABDUCE`, A4).
///
/// An adapter hands one out through
/// [`LocyExecutionContext::reuse_baseline`] only while the state it was
/// derived from is the current state, so the holder may reuse any stratum a
/// mutation applied *after* obtaining it cannot affect.
#[derive(Clone)]
pub struct StrataBaseline {
    /// Every evaluated rule's facts in that state, by rule name.
    pub facts: Arc<DerivedStore>,
    /// The same facts as rows, entities hydrated in that state, when held.
    pub rows: Option<Arc<RowStore>>,
}

/// The result of re-evaluating a program's strata.
pub struct ReEvaluation {
    /// Every rule's facts as rows, entities hydrated in the evaluated state.
    pub rows: RowStore,
    /// The raw facts, for a nested re-evaluation to reuse; `None` when the
    /// adapter does not keep them.
    pub facts: Option<Arc<DerivedStore>>,
    /// Positions of the strata whose facts were copied rather than derived.
    pub reused: BTreeSet<usize>,
    /// The provenance recorded while deriving these facts, when the program
    /// records provenance (an `EXPLAIN RULE` or a probabilistic fold
    /// anywhere in it): what an `EXPLAIN RULE` reading them explains.
    pub tracker: Option<Arc<ProvenanceStore>>,
    /// The rules whose groups fell back to approximate probabilities in
    /// this evaluation, by rule name.
    pub approximate: HashMap<String, Vec<String>>,
}

/// Read-only access to derived facts and graph data.
///
/// Replaces `CypherExecutor` for read operations in the native command dispatch path.
/// `lookup_derived` converts RecordBatch-based native facts to `FactRow` format internally.
#[async_trait]
pub trait DerivedFactSource: Send + Sync {
    /// Look up all facts for a rule. Returns `FactRow`-based results.
    fn lookup_derived(&self, rule_name: &str) -> Result<Vec<FactRow>, LocyError>;

    /// Look up all facts for a rule as raw RecordBatches (zero-copy from native store).
    ///
    /// Default implementation returns an error. Override in native adapters to read
    /// directly from `DerivedStore` without converting to rows.
    fn lookup_derived_batches(&self, _rule_name: &str) -> Result<Vec<RecordBatch>, LocyError> {
        Err(LocyError::EvaluationError {
            message: "lookup_derived_batches not implemented for this adapter".into(),
        })
    }

    /// Execute a graph MATCH query with optional WHERE conditions.
    ///
    /// Returns raw `RecordBatch`es so the native path stays columnar.
    /// Callers that need `Vec<FactRow>` must convert via `record_batches_to_locy_rows`.
    ///
    /// Used by EXPLAIN derivation trees and delta resolution.
    /// AST construction happens inside the adapter via `build_match_return_query`.
    async fn execute_pattern(
        &self,
        pattern: &Pattern,
        where_conditions: &[Expr],
    ) -> Result<Vec<RecordBatch>, LocyError>;

    /// Resolve a set of node vids to full `Value::Node` values.
    ///
    /// Used by Mode A EXPLAIN to enrich tracker fact_rows (which carry
    /// node bindings as `Value::Int(vid)`) before WHERE-predicate
    /// evaluation, so that `WHERE n.prop = X` style filters resolve
    /// against materialised Node properties instead of returning Null.
    ///
    /// Default impl returns an empty map. Callers must handle missing
    /// vids gracefully — typically by leaving the original
    /// `Value::Int(vid)` in place. Native adapters override this.
    async fn lookup_nodes_by_vids(&self, _vids: &[u64]) -> Result<HashMap<u64, Value>, LocyError> {
        Ok(HashMap::new())
    }

    /// Resolve relationship EIDs to their `Value::Edge`, as
    /// [`Self::lookup_nodes_by_vids`] resolves VIDs.
    ///
    /// A rule's facts carry a single-hop relationship as its EID, and EIDs
    /// overlap VIDs numerically, so an EID must never be resolved as a VID.
    /// Default impl returns an empty map; native adapters override this.
    async fn lookup_edges_by_eids(&self, _eids: &[u64]) -> Result<HashMap<u64, Value>, LocyError> {
        Ok(HashMap::new())
    }
}

/// DB operations needed by ASSUME, DERIVE, and ABDUCE.
///
/// Extends `DerivedFactSource` with write operations.
#[async_trait]
pub trait LocyExecutionContext: DerivedFactSource {
    /// The facts DERIVE writes from for `rule_name`, entities hydrated.
    ///
    /// `derived_store` is the store the issuing command reads — the program's
    /// at top level, an ASSUME body's re-evaluated one inside ASSUME — so IS
    /// references resolve against the facts of the state the command runs in.
    ///
    /// Default implementation returns that store's rows for the rule, falling
    /// back to `lookup_derived()`. Native adapters override it to re-run a
    /// DERIVE rule's body, which the fixpoint does not evaluate.
    async fn lookup_derived_enriched(
        &self,
        rule_name: &str,
        derived_store: &RowStore,
    ) -> Result<Vec<FactRow>, LocyError> {
        match derived_store.get(rule_name) {
            Some(relation) => Ok(relation.rows.clone()),
            None => self.lookup_derived(rule_name),
        }
    }

    /// Execute a pre-compiled Cypher read query (e.g. from a `CompiledCommand::Cypher`).
    ///
    /// Used by ASSUME/ABDUCE body dispatch where a Query AST is already available.
    async fn execute_cypher_read(&self, ast: Query) -> Result<Vec<FactRow>, LocyError>;

    /// Execute a mutation (CREATE/MERGE/DELETE), returning affected row count.
    async fn execute_mutation(
        &self,
        ast: Query,
        params: HashMap<String, Value>,
    ) -> Result<usize, LocyError>;

    /// Fork the current Locy L0 buffer for hypothetical reasoning.
    ///
    /// Saves the current L0 state and replaces it with a clone.
    /// Mutations after fork are isolated to the clone.
    /// Call `restore_l0()` to undo all hypothetical changes.
    async fn fork_l0(&self) -> Result<(), LocyError>;

    /// Restore the Locy L0 buffer to its state before the last `fork_l0()`.
    ///
    /// Discards all mutations made since the fork.
    async fn restore_l0(&self) -> Result<(), LocyError>;

    /// Re-evaluate all strata in the current (possibly mutated) state.
    ///
    /// Used by ASSUME and ABDUCE `validate_modification` to check hypothetical states.
    /// Returns the row-based `RowStore` after convergence.
    async fn re_evaluate_strata(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
    ) -> Result<RowStore, LocyError>;

    /// Re-evaluate the strata, copying the facts of `reused` strata instead of
    /// deriving them.
    ///
    /// The caller vouches for the reuse (see [`ReusedStrata`]). The default
    /// implementation ignores it and re-derives everything, reporting no
    /// stratum as reused.
    async fn re_evaluate_strata_reusing(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
        reused: Option<ReusedStrata>,
    ) -> Result<ReEvaluation, LocyError> {
        let _ = reused;
        Ok(ReEvaluation {
            rows: self.re_evaluate_strata(program, config).await?,
            facts: None,
            reused: BTreeSet::new(),
            tracker: None,
            approximate: HashMap::new(),
        })
    }

    /// Re-evaluate the strata for a program's top-level command sequence,
    /// after a command wrote (see [`super::locy_sequence`]).
    ///
    /// Unlike a hypothetical re-evaluation, the facts replace the program's
    /// own for every later command, so an adapter that records provenance for
    /// the program records it here too. The default implementation is
    /// [`Self::re_evaluate_strata_reusing`].
    async fn re_evaluate_sequenced(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
        reused: Option<ReusedStrata>,
    ) -> Result<ReEvaluation, LocyError> {
        self.re_evaluate_strata_reusing(program, config, reused)
            .await
    }

    /// Identifies the state outside this evaluation's own writes (what other
    /// transactions commit to), for checking that facts derived earlier in
    /// the evaluation still describe it apart from those writes.
    ///
    /// Equal tokens mean the outside state did not change in between.
    /// `None` when it cannot change (a pinned snapshot) or nothing can
    /// write; the default.
    fn base_state_token(&self) -> Option<u64> {
        None
    }

    /// The facts of the current state, when the adapter holds them.
    ///
    /// Call it **before** applying a hypothetical mutation: the baseline is
    /// returned only while the state it was derived from is still current, so
    /// a later mutation is exactly what separates it from the hypothetical
    /// state. `None` when no such facts are held, which only costs reuse.
    fn reuse_baseline(&self) -> Option<StrataBaseline> {
        None
    }

    /// Record `baseline` as the facts of the current state, for nested
    /// hypothetical commands. Paired with [`Self::pop_reuse_baseline`].
    fn push_reuse_baseline(&self, baseline: StrataBaseline) {
        let _ = baseline;
    }

    /// Drop the baseline the last [`Self::push_reuse_baseline`] recorded.
    fn pop_reuse_baseline(&self) {}

    /// Whether a re-evaluation of this program was cut short under
    /// `allow_partial` (its budget ran out, or a recursive stratum hit
    /// `max_iterations`).
    ///
    /// Commands stop there, as they do when the program's own run is cut
    /// short: the facts later commands would read are partial, and so is the
    /// result of the command that hit the cut, which is not reported. The
    /// cut itself is, in the program's `incomplete`. The default never cuts.
    fn is_cut_short(&self) -> bool {
        false
    }

    /// Fails when the program must stop now: its budget is spent
    /// ([`LocyError::Timeout`]) or it was cancelled.
    ///
    /// Work that runs inside no operator, an in-memory QUERY over the
    /// derived facts above all, calls it between rows, since no operator's
    /// deadline or cancellation check can interrupt it. The default never
    /// stops.
    ///
    /// # Errors
    ///
    /// `LocyError::Timeout` once the budget is spent; an executor error
    /// reporting the cancellation once cancelled.
    fn check_interrupt(&self) -> Result<(), LocyError> {
        Ok(())
    }
}
