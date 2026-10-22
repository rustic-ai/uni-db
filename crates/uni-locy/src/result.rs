use std::collections::HashMap;
use std::time::Duration;

use uni_common::{Eid, Properties, Value, Vid};

use crate::types::{RuntimeWarning, RuntimeWarningCode};

/// A single row of bindings from a Locy evaluation result.
pub type FactRow = HashMap<String, Value>;

/// Every rule's facts, keyed by rule name.
pub type RuleFacts = HashMap<String, Vec<FactRow>>;

/// The result of evaluating a compiled Locy program.
#[derive(Debug, Clone)]
pub struct LocyResult {
    /// Derived facts per rule name.
    pub derived: HashMap<String, Vec<FactRow>>,
    /// Execution statistics.
    pub stats: LocyStats,
    /// Results from Phase 4 commands.
    pub command_results: Vec<CommandResult>,
    /// Runtime warnings collected during evaluation.
    pub warnings: Vec<RuntimeWarning>,
    /// Compile-time warnings carried over from `CompiledProgram.warnings`.
    /// Phase C C4: surfaces `UncalibratedNeuralPredicate` /
    /// `FoldInRecursivePath` / `UncalibratedLLMLogprobs` /
    /// `MsumNonNegativity` / `ProbabilityDomainViolation` so test
    /// harnesses and downstream tooling can inspect them on the
    /// returned `LocyResult` rather than re-running the compiler.
    pub compile_warnings: Vec<crate::types::CompilerWarning>,
    /// Groups where BDD computation fell back to independence mode.
    /// Maps rule name → list of human-readable key group descriptions.
    pub approximate_groups: HashMap<String, Vec<String>>,
    /// When present, contains the derived facts from a session-level DERIVE
    /// that have not yet been applied. Use `tx.apply(derived)` to materialize.
    pub derived_fact_set: Option<DerivedFactSet>,
    /// Diagnostics for an evaluation that stopped before completing, present
    /// only on the `allow_partial` path. Names which rules were left
    /// incomplete or skipped (so a zero-row count can be distinguished from a
    /// genuinely empty rule) and which complement rules are consequently
    /// unsound. `None` for a normal, complete evaluation.
    pub incomplete: Option<uni_common::LocyIncomplete>,
}

/// Result of executing a single Phase 4 command.
#[derive(Debug, Clone)]
pub enum CommandResult {
    Query(Vec<FactRow>),
    /// Result of an `ASSUME { ... } THEN { ... }` block.
    Assume {
        /// One result per body command, in program order: a `QUERY`'s rows,
        /// a `DERIVE`'s affected count, a Cypher command's rows, a nested
        /// `ASSUME`'s own result. Empty when the body defines rules only.
        results: Vec<CommandResult>,
        /// Every rule's facts in the hypothetical state the body's first
        /// command reads: after the block's mutations, before any body
        /// command, the parent program's rules and the body's own. The same
        /// point the program's [`LocyResult::derived`] is taken at, before
        /// its first command.
        derived: RuleFacts,
    },
    Explain(DerivationNode),
    Abduce(AbductionResult),
    Derive {
        affected: usize,
    },
    Cypher(Vec<FactRow>),
    /// Phase C C2: result of a `CALIBRATE` statement — the fitted
    /// calibrator plus pre- and post-calibration holdout metrics.
    Calibrate(CalibrationResult),
    /// Phase C C3: result of a `VALIDATE` statement — the metric
    /// values computed over `(rule_output, ground_truth)` pairs.
    Validate(ValidationResult),
}

/// Outcome of a single `VALIDATE` invocation. Phase C C3.
///
/// `metrics` maps each requested metric to its scalar value. The
/// `n_samples` field reports how many `(prediction, label)` pairs
/// were retained after joining the rule's PROB column with the
/// TARGET expression. Bare `ECE` produces a `EceBinningBias`
/// compile-time warning (surfaced via `LocyResult.compile_warnings`).
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub rule_name: String,
    pub prob_column: String,
    pub n_samples: usize,
    pub metrics: Vec<(uni_cypher::locy_ast::ValidationMetric, f64)>,
}

impl ValidationResult {
    pub fn metric(&self, m: uni_cypher::locy_ast::ValidationMetric) -> Option<f64> {
        self.metrics
            .iter()
            .find(|(name, _)| *name == m)
            .map(|(_, v)| *v)
    }
}

/// Phase C C1a: per-prediction confidence interval surfaced by
/// uncertainty-aware calibrators. For split-conformal, the band is
/// `[p - q, p + q]` clipped to `[0, 1]` where `q` is the
/// `(1 - alpha)`-quantile of holdout nonconformity scores.
#[derive(Debug, Clone, Copy)]
pub struct ConfidenceBand {
    pub lower: f64,
    pub upper: f64,
    pub source: ConfidenceSource,
}

/// Phase C C1a: provenance tag for a [`ConfidenceBand`] — identifies
/// which uncertainty-quantification machinery produced the bounds.
/// `Conformal` shipped in C1a; ensemble and credal variants follow in
/// D-C1e as extensibility hooks for future calibrators.
#[derive(Debug, Clone, Copy)]
pub enum ConfidenceSource {
    /// Split-conformal predictor: `alpha` is the miscoverage rate
    /// (e.g. `0.1` → 90% coverage). Band is centered on the point
    /// estimate, ± the `(1 - alpha)`-quantile of holdout
    /// nonconformity scores. Shipped in C1a.
    Conformal { alpha: f64 },
    /// Phase D D-C1e: bootstrap or N-of-K ensemble calibrator. The
    /// band is derived from cross-estimator variance: `[p - σ, p + σ]`
    /// (clipped to `[0, 1]`) where `σ` is the standard deviation of
    /// per-estimator predictions on the holdout. `n_estimators` is the
    /// number of base learners that voted, surfaced so consumers can
    /// reason about the noise floor of the band.
    EnsembleVariance { n_estimators: usize },
    /// Phase D D-C1e: credal (imprecise-probability) calibrator. The
    /// band is an explicit interval `[lower, upper]` derived from a
    /// credal prior rather than a point estimate ± halo. The two
    /// `_prior` fields surface the calibrator's lower / upper prior
    /// hyperparameters so consumers can map the band back to its
    /// belief-revision shape.
    Credal { lower_prior: f64, upper_prior: f64 },
}

#[cfg(test)]
mod confidence_source_tests {
    use super::ConfidenceSource;

    #[test]
    fn conformal_debug_format() {
        let s = ConfidenceSource::Conformal { alpha: 0.1 };
        let dbg = format!("{:?}", s);
        assert!(dbg.contains("Conformal"));
        assert!(dbg.contains("0.1"));
    }

    #[test]
    fn ensemble_variance_debug_format() {
        let s = ConfidenceSource::EnsembleVariance { n_estimators: 50 };
        let dbg = format!("{:?}", s);
        assert!(dbg.contains("EnsembleVariance"));
        assert!(dbg.contains("50"));
    }

    #[test]
    fn credal_debug_format() {
        let s = ConfidenceSource::Credal {
            lower_prior: 0.1,
            upper_prior: 0.9,
        };
        let dbg = format!("{:?}", s);
        assert!(dbg.contains("Credal"));
        assert!(dbg.contains("0.1"));
        assert!(dbg.contains("0.9"));
    }
}

/// Outcome of a single `CALIBRATE` invocation. Phase C C2.
///
/// `calibrator` is the fitted transform; user code typically wraps it
/// over the base classifier via `CalibratedClassifier` and re-registers
/// the wrapped classifier in `LocyConfig::classifier_registry` for
/// subsequent evaluations.
#[derive(Debug, Clone)]
pub struct CalibrationResult {
    pub model_name: String,
    pub method: crate::calibration::CalibrationMethodKind,
    pub n_samples: usize,
    pub holdout_size: usize,
    pub calibrator: std::sync::Arc<dyn crate::calibration::Calibrator>,
    pub raw_brier: f64,
    pub raw_ece: f64,
    pub calibrated_brier: f64,
    pub calibrated_ece: f64,
    /// Phase C C1a: for conformal calibrators, the
    /// `(1 - alpha)`-quantile of holdout nonconformity scores —
    /// the half-width of every confidence band the calibrator will
    /// emit at inference. `None` for non-conformal methods.
    pub confidence_band_quantile: Option<f64>,
}

/// Phase C B1–B3: per neural-model invocation provenance, attached
/// to a [`DerivationNode`] when the derivation's body invoked one
/// or more classifiers. `raw_probability` is the classifier's
/// direct output; `calibrated_probability` is the post-Calibrator
/// value (when any calibrator other than `Identity` is registered).
/// `confidence_band` is populated when the active calibrator is
/// conformal (or any future band-emitting calibrator).
#[derive(Debug, Clone)]
pub struct NeuralProvenance {
    pub model_name: String,
    pub raw_probability: f64,
    pub calibrated_probability: Option<f64>,
    pub confidence_band: Option<ConfidenceBand>,
}

/// A node in a derivation tree, produced by EXPLAIN RULE.
#[derive(Debug, Clone)]
pub struct DerivationNode {
    pub rule: String,
    pub clause_index: usize,
    pub priority: Option<i64>,
    pub bindings: HashMap<String, Value>,
    pub along_values: HashMap<String, Value>,
    pub children: Vec<DerivationNode>,
    pub graph_fact: Option<String>,
    /// True when this node's probability was computed via BDD fallback
    /// (independence mode) because the group exceeded `max_bdd_variables`.
    pub approximate: bool,
    /// Probability of this specific proof path, populated when top-k proof
    /// filtering is active (Scallop, Huang et al. 2021).
    pub proof_probability: Option<f64>,
    /// Phase C B1–B3: neural-model invocations that contributed to
    /// this fact's derivation. Empty for purely-symbolic
    /// derivations.
    pub neural_calls: Vec<NeuralProvenance>,
}

/// Result of an ABDUCE query.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AbductionResult {
    pub modifications: Vec<ValidatedModification>,
}

/// A modification with validation status and cost.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidatedModification {
    pub modification: Modification,
    /// Whether this modification satisfies the ABDUCE goal when applied via savepoint.
    pub validated: bool,
    /// Cost metric for ranking modifications: RemoveEdge=1.0, ChangeProperty=0.5, AddEdge=1.5.
    pub cost: f64,
}

/// A proposed graph modification from ABDUCE.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum Modification {
    /// Remove one relationship the derivation read.
    ///
    /// The `*_var` fields name the rule body's variables, for display. The
    /// edge itself is identified by `eid` when the body binds it, otherwise by
    /// its endpoints `source_vid` / `target_vid` (`source_vid` is the edge's
    /// actual source, whatever the direction the body wrote it in).
    RemoveEdge {
        source_var: String,
        target_var: String,
        edge_var: String,
        edge_type: String,
        /// Property constraints on the edge, applied in addition to its
        /// identity.
        match_properties: HashMap<String, Value>,
        /// Identity of the edge to remove, when the rule body binds it.
        eid: Option<Eid>,
        /// Identity of the edge's source node.
        source_vid: Option<Vid>,
        /// Identity of the edge's target node.
        target_vid: Option<Vid>,
    },
    /// Change one property of one node or edge the derivation read.
    ///
    /// `element_var` is the rule body's variable; the entity changed is the
    /// one `vid` (a node) or `eid` (an edge) identifies.
    ChangeProperty {
        element_var: String,
        property: String,
        old_value: Box<Value>,
        new_value: Box<Value>,
        /// Identity of the node to change, when the element is a node.
        vid: Option<Vid>,
        /// Identity of the edge to change, when the element is an edge.
        eid: Option<Eid>,
    },
    /// Add one relationship between two existing nodes.
    ///
    /// The `*_var` fields name the rule body's variables, for display, in the
    /// edge's own direction. The nodes the edge joins are `source_vid` and
    /// `target_vid`.
    AddEdge {
        source_var: String,
        target_var: String,
        edge_type: String,
        properties: HashMap<String, Value>,
        /// Identity of the new edge's source node.
        source_vid: Option<Vid>,
        /// Identity of the new edge's target node.
        target_vid: Option<Vid>,
    },
}

/// A derived edge to be materialized.
#[derive(Debug, Clone)]
pub struct DerivedEdge {
    pub edge_type: String,
    pub source_label: String,
    pub source_properties: Properties,
    pub target_label: String,
    pub target_properties: Properties,
    pub edge_properties: Properties,
}

/// Pure-data representation of facts derived by a session-level DERIVE.
///
/// Apply to a transaction via `tx.apply(derived)` or `tx.apply_with(derived)`.
#[derive(Debug, Clone)]
pub struct DerivedFactSet {
    /// New vertices grouped by label.
    pub vertices: HashMap<String, Vec<Properties>>,
    /// Derived edges connecting source/target vertices.
    pub edges: Vec<DerivedEdge>,
    /// Evaluation statistics from the DERIVE run.
    pub stats: LocyStats,
    /// Database version at evaluation time (for staleness detection).
    pub evaluated_at_version: u64,
    /// Internal: Cypher ASTs for faithful replay during `tx.apply()`.
    #[doc(hidden)]
    pub mutation_queries: Vec<uni_cypher::ast::Query>,
    /// Internal: the program's parameters, which `tx.apply()` replays
    /// `mutation_queries` with, so a write command reading `$param` stores
    /// the value it was evaluated with.
    #[doc(hidden)]
    pub mutation_params: HashMap<String, Value>,
}

impl DerivedFactSet {
    /// Total number of derived facts (vertices + edges).
    pub fn fact_count(&self) -> usize {
        self.vertices.values().map(|v| v.len()).sum::<usize>() + self.edges.len()
    }

    /// True when there is nothing to apply: no facts were derived and the
    /// program issued no other write (a Cypher write command's statement is
    /// replayed too).
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty() && self.edges.is_empty() && self.mutation_queries.is_empty()
    }
}

/// Statistics collected during Locy program evaluation.
#[derive(Debug, Clone, Default)]
pub struct LocyStats {
    pub strata_evaluated: usize,
    pub total_iterations: usize,
    pub derived_nodes: usize,
    pub derived_edges: usize,
    pub evaluation_time: Duration,
    pub queries_executed: usize,
    pub mutations_executed: usize,
    /// Peak memory used by derived relations (in bytes).
    pub peak_memory_bytes: usize,
    /// Strata derived by hypothetical re-evaluations (`ASSUME` bodies and
    /// `ABDUCE` candidate checks), summed over every re-evaluation.
    pub hypothetical_strata_evaluated: usize,
    /// Strata a hypothetical re-evaluation copied from the state before its
    /// mutation instead of deriving, because the mutation could not affect
    /// them (see [`crate::incremental`]). Summed over every re-evaluation.
    pub hypothetical_strata_reused: usize,
    /// Facts the derived (not reused) strata of hypothetical re-evaluations
    /// produced, summed over every re-evaluation.
    pub hypothetical_facts_derived: usize,
    /// Strata derived again because an earlier top-level command wrote (a
    /// `DERIVE`, a Cypher write) and a later command read the facts, summed
    /// over every such re-derivation.
    pub sequenced_strata_evaluated: usize,
    /// Strata such a re-derivation copied from the state before the write
    /// instead of deriving, because the write could not affect them. Summed
    /// over every re-derivation.
    pub sequenced_strata_reused: usize,
}

impl LocyResult {
    /// Get derived facts for a specific rule.
    pub fn derived_facts(&self, rule: &str) -> Option<&Vec<FactRow>> {
        self.derived.get(rule)
    }

    /// Get rows from the first row-producing command result.
    ///
    /// Row-producing results are `QUERY` and Cypher statements, taken in
    /// program order; an `ASSUME … THEN { … }` is searched depth first, so its
    /// body's first row-producing result counts in the ASSUME's place (see
    /// [`CommandResult::rows`]). The Python binding's `rows` follows the same
    /// rule. Use [`Self::command_results`] to reach a specific command.
    pub fn rows(&self) -> Option<&Vec<FactRow>> {
        self.command_results.iter().find_map(CommandResult::rows)
    }

    /// Get column names from the first row-producing command result's first row.
    ///
    /// Column names are returned in deterministic (sorted) order. The
    /// underlying [`FactRow`] is a `HashMap`, whose iteration order is
    /// randomized per-run; callers (snapshot tests, golden outputs,
    /// downstream display) rely on a stable ordering.
    pub fn columns(&self) -> Option<Vec<String>> {
        self.rows().and_then(|rows| {
            rows.first().map(|row| {
                let mut cols: Vec<String> = row.keys().cloned().collect();
                cols.sort();
                cols
            })
        })
    }

    /// Get execution statistics.
    pub fn stats(&self) -> &LocyStats {
        &self.stats
    }

    /// Get the total number of fixpoint iterations.
    pub fn iterations(&self) -> usize {
        self.stats.total_iterations
    }

    /// Get runtime warnings collected during evaluation.
    pub fn compile_warnings(&self) -> &[crate::types::CompilerWarning] {
        &self.compile_warnings
    }

    pub fn command_results(&self) -> &[CommandResult] {
        &self.command_results
    }

    pub fn warnings(&self) -> &[RuntimeWarning] {
        &self.warnings
    }

    /// Check whether a specific warning code was emitted.
    pub fn has_warning(&self, code: &RuntimeWarningCode) -> bool {
        self.warnings.iter().any(|w| w.code == *code)
    }

    /// True when the evaluation was cut short by a timeout or iteration
    /// limit. The `derived` map then contains whatever facts were
    /// accumulated before the cutoff; partial results may not satisfy
    /// the fixpoint invariant.
    ///
    /// This is exactly `self.incomplete.is_some()`. Inspect
    /// [`incomplete`](LocyResult::incomplete) for the reason and the
    /// skipped/unsound rule lists. Note it is only ever `true` on the
    /// opt-in `allow_partial` path — by default an incomplete evaluation
    /// returns [`UniError::LocyIncomplete`] instead of a result.
    ///
    /// [`UniError::LocyIncomplete`]: uni_common::UniError::LocyIncomplete
    pub fn timed_out(&self) -> bool {
        self.incomplete.is_some()
    }
}

impl CommandResult {
    /// If this is an Explain result, return the derivation node.
    pub fn as_explain(&self) -> Option<&DerivationNode> {
        match self {
            CommandResult::Explain(node) => Some(node),
            _ => None,
        }
    }

    /// If this is a Query result, return the rows.
    pub fn as_query(&self) -> Option<&Vec<FactRow>> {
        match self {
            CommandResult::Query(rows) => Some(rows),
            _ => None,
        }
    }

    /// If this is an Abduce result, return it.
    pub fn as_abduce(&self) -> Option<&AbductionResult> {
        match self {
            CommandResult::Abduce(result) => Some(result),
            _ => None,
        }
    }

    /// If this is an Assume result, return its per-command results and its
    /// derived facts.
    pub fn as_assume(&self) -> Option<(&[CommandResult], &RuleFacts)> {
        match self {
            CommandResult::Assume { results, derived } => Some((results, derived)),
            _ => None,
        }
    }

    /// The rows this result produced, searching an Assume depth first.
    ///
    /// A `Query` or `Cypher` result is its own rows; an `Assume` result is the
    /// rows of the first row-producing result among its body's results,
    /// descending into nested ASSUMEs in order; any other result (and an
    /// Assume whose body produced no rows) has none.
    pub fn rows(&self) -> Option<&Vec<FactRow>> {
        match self {
            CommandResult::Query(rows) | CommandResult::Cypher(rows) => Some(rows),
            CommandResult::Assume { results, .. } => results.iter().find_map(CommandResult::rows),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: previously, `columns()` returned `HashMap::keys()` order,
    /// which is randomized per-run. Snapshot tests and downstream consumers
    /// rely on deterministic column ordering.
    #[test]
    fn columns_returned_in_sorted_order() {
        let mut row = FactRow::new();
        // Insert in deliberately non-alphabetic order. With a HashMap this
        // is enough to surface nondeterminism on many runs; sorting makes
        // the test deterministic regardless of hasher state.
        row.insert("zeta".into(), Value::Int(1));
        row.insert("alpha".into(), Value::Int(2));
        row.insert("mu".into(), Value::Int(3));

        let result = LocyResult {
            derived: HashMap::new(),
            stats: LocyStats::default(),
            command_results: vec![CommandResult::Query(vec![row])],
            warnings: Vec::new(),
            compile_warnings: Vec::new(),
            approximate_groups: HashMap::new(),
            derived_fact_set: None,
            incomplete: None,
        };

        let cols = result
            .columns()
            .expect("expected columns for non-empty result");
        assert_eq!(
            cols,
            vec!["alpha".to_owned(), "mu".to_owned(), "zeta".to_owned()]
        );
    }

    fn row(col: &str, v: i64) -> FactRow {
        [(col.to_string(), Value::Int(v))].into()
    }

    fn result_of(command_results: Vec<CommandResult>) -> LocyResult {
        LocyResult {
            derived: HashMap::new(),
            stats: LocyStats::default(),
            command_results,
            warnings: Vec::new(),
            compile_warnings: Vec::new(),
            approximate_groups: HashMap::new(),
            derived_fact_set: None,
            incomplete: None,
        }
    }

    /// `rows()` descends depth first into ASSUME results: a DERIVE is not
    /// row-producing, a nested ASSUME's first row-producing result comes
    /// before a later sibling's, and an ASSUME with none is skipped.
    #[test]
    fn rows_descends_depth_first_into_assume() {
        let empty_assume = CommandResult::Assume {
            results: vec![CommandResult::Derive { affected: 2 }],
            derived: HashMap::new(),
        };
        let nested = CommandResult::Assume {
            results: vec![
                CommandResult::Derive { affected: 1 },
                CommandResult::Assume {
                    results: vec![CommandResult::Query(vec![row("inner", 1)])],
                    derived: HashMap::new(),
                },
                CommandResult::Query(vec![row("outer", 2)]),
            ],
            derived: HashMap::from([("r".to_string(), vec![row("d", 3)])]),
        };
        let result = result_of(vec![
            empty_assume,
            nested,
            CommandResult::Query(vec![row("top", 4)]),
        ]);
        assert_eq!(result.rows(), Some(&vec![row("inner", 1)]));
        assert_eq!(result.columns(), Some(vec!["inner".to_string()]));

        let (results, derived) = result.command_results[1]
            .as_assume()
            .expect("an Assume result");
        assert_eq!(results.len(), 3);
        assert_eq!(derived.get("r"), Some(&vec![row("d", 3)]));
        assert!(result.command_results[2].as_assume().is_none());
        assert_eq!(result.command_results[0].rows(), None);
    }

    #[test]
    fn abduce_result_serializes_to_json() {
        let result = AbductionResult {
            modifications: vec![
                ValidatedModification {
                    modification: Modification::ChangeProperty {
                        element_var: "a".into(),
                        property: "flagged".into(),
                        old_value: Box::new(Value::String("false".into())),
                        new_value: Box::new(Value::String("true".into())),
                        vid: Some(Vid::new(1)),
                        eid: None,
                    },
                    validated: true,
                    cost: 0.5,
                },
                ValidatedModification {
                    modification: Modification::RemoveEdge {
                        source_var: "a".into(),
                        target_var: "b".into(),
                        edge_var: "e".into(),
                        edge_type: "TRANSFERS_TO".into(),
                        match_properties: HashMap::from([("amount".into(), Value::Float(1000.0))]),
                        eid: Some(Eid::new(7)),
                        source_vid: Some(Vid::new(1)),
                        target_vid: Some(Vid::new(2)),
                    },
                    validated: false,
                    cost: 1.0,
                },
                ValidatedModification {
                    modification: Modification::AddEdge {
                        source_var: "a".into(),
                        target_var: "b".into(),
                        edge_type: "FLAGGED_BY".into(),
                        properties: HashMap::new(),
                        source_vid: Some(Vid::new(1)),
                        target_vid: Some(Vid::new(3)),
                    },
                    validated: true,
                    cost: 1.5,
                },
            ],
        };

        let json = serde_json::to_value(&result).expect("serialization failed");
        let mods = json["modifications"].as_array().unwrap();
        assert_eq!(mods.len(), 3);
        assert_eq!(mods[0]["validated"], true);
        assert_eq!(mods[0]["cost"], 0.5);
        assert!(mods[0]["modification"]["ChangeProperty"].is_object());
        assert!(mods[1]["modification"]["RemoveEdge"].is_object());
        assert!(mods[2]["modification"]["AddEdge"].is_object());
    }
}
