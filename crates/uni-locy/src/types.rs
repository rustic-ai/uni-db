use std::collections::HashMap;

use uni_cypher::ast::{Clause, Expr, Pattern, Query};
use uni_cypher::locy_ast::{
    AbduceQuery, AlongBinding, BestByClause, DeriveCommand, ExplainRule, FoldBinding, GoalQuery,
    RuleCondition, RuleOutput,
};

/// A fully validated and stratified Locy program, ready for the orchestrator.
#[derive(Debug, Clone)]
pub struct CompiledProgram {
    pub strata: Vec<Stratum>,
    pub rule_catalog: HashMap<String, CompiledRule>,
    /// Compiled neural-predicate declarations from `CREATE MODEL`
    /// statements (Phase B preview). Empty unless
    /// `LocyConfig::neural_predicates_preview` is set.
    pub model_catalog: HashMap<String, CompiledModel>,
    pub warnings: Vec<CompilerWarning>,
    pub commands: Vec<CompiledCommand>,
}

/// A compiled `CREATE MODEL` declaration (Phase B preview).
///
/// Lowered from `uni_cypher::locy_ast::ModelDefinition`. The feature
/// expressions are kept as Cypher AST; the runtime evaluates them per
/// row in a future slice (`LocyModelInvoke`).
#[derive(Debug, Clone)]
pub struct CompiledModel {
    pub name: String,
    pub inputs: Vec<CompiledInputBinding>,
    pub features: Vec<uni_cypher::ast::Expr>,
    /// Phase D D3: optional path-context feature `FEATURES (subject, col) FROM rule_name`.
    pub path_context: Option<uni_cypher::locy_ast::PathContextFeature>,
    pub output_type: uni_cypher::locy_ast::OutputType,
    pub output_name: String,
    pub xervo_alias: String,
    /// Phase D D2 follow-up: optional embedder alias surfaced via
    /// `USING xervo('classify/X', embedder='alias')`. When `None`,
    /// `semantic_match` query-text embedding falls back to `"default"`.
    pub embedder_alias: Option<String>,
    pub calibration: Option<uni_cypher::locy_ast::CalibrationMethod>,
    pub version: Option<String>,
    pub annotations: uni_cypher::locy_ast::ModelAnnotations,
}

#[derive(Debug, Clone)]
pub struct CompiledInputBinding {
    pub variable: String,
    pub label: Option<String>,
}

/// A compiled command (non-rule statement) ready for execution.
#[derive(Debug, Clone)]
pub enum CompiledCommand {
    GoalQuery(GoalQuery),
    Assume(CompiledAssume),
    Abduce(AbduceQuery),
    ExplainRule(ExplainRule),
    DeriveCommand(DeriveCommand),
    Cypher(Query),
    /// Phase C C2: `CALIBRATE` statement — collects `(pred, label)`
    /// pairs from the MATCH pattern, invokes the registered classifier,
    /// fits the chosen calibrator on a holdout split.
    Calibrate(CompiledCalibrate),
    /// Phase C C3: `VALIDATE` statement — joins a rule's PROB output
    /// against ground truth and reports requested metrics.
    Validate(CompiledValidate),
}

/// Compiled `CALIBRATE` command — Phase C C2.
#[derive(Debug, Clone)]
pub struct CompiledCalibrate {
    pub model_name: String,
    pub pattern: uni_cypher::ast::Pattern,
    pub where_expr: Option<uni_cypher::ast::Expr>,
    pub target_expr: uni_cypher::ast::Expr,
    pub method: uni_cypher::locy_ast::CalibrationMethod,
    /// Resolved holdout fraction (default 0.2 when the source omitted it).
    pub holdout: f64,
}

/// Compiled `VALIDATE` command — Phase C C3.
#[derive(Debug, Clone)]
pub struct CompiledValidate {
    pub rule_name: String,
    pub pattern: uni_cypher::ast::Pattern,
    pub where_expr: Option<uni_cypher::ast::Expr>,
    pub target_expr: uni_cypher::ast::Expr,
    pub metrics: Vec<uni_cypher::locy_ast::ValidationMetric>,
    /// Name of the rule's PROB column (resolved from `rule_catalog`).
    /// Used by the runtime to find the prediction value in derived facts.
    pub prob_column: String,
}

/// A compiled ASSUME block with mutations and body program.
#[derive(Debug, Clone)]
pub struct CompiledAssume {
    pub mutations: Vec<Clause>,
    pub body_program: CompiledProgram,
    pub body_commands: Vec<CompiledCommand>,
}

/// A group of rules that must be evaluated together (one SCC).
#[derive(Debug, Clone)]
pub struct Stratum {
    pub id: usize,
    pub rules: Vec<CompiledRule>,
    pub is_recursive: bool,
    pub depends_on: Vec<usize>,
}

/// A named rule with all its clauses merged and validated.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub name: String,
    pub clauses: Vec<CompiledClause>,
    pub yield_schema: Vec<YieldColumn>,
    pub priority: Option<i64>,
}

/// A single clause (one CREATE RULE ... AS ... definition).
#[derive(Debug, Clone)]
pub struct CompiledClause {
    pub match_pattern: Pattern,
    pub where_conditions: Vec<RuleCondition>,
    pub along: Vec<AlongBinding>,
    pub fold: Vec<FoldBinding>,
    /// Post-FOLD filter conditions (HAVING semantics).
    pub having: Vec<Expr>,
    pub best_by: Option<BestByClause>,
    pub output: RuleOutput,
    pub priority: Option<i64>,
    /// Phase B Slice 3: neural-model invocations extracted from this
    /// clause's YIELD items (or, in future slices, other body sites).
    /// Each invocation produces a synthetic output column whose values
    /// are filled at runtime by the registered [`crate::NeuralClassifier`];
    /// the original `model(args)` expression in YIELD has been rewritten
    /// to a [`uni_cypher::ast::Expr::Variable`] reference to that column.
    pub model_invocations: Vec<ModelInvocation>,
    /// Column names that the compiler appended to this clause's YIELD
    /// as hidden materialization items (e.g. `"s.tier"` for
    /// `scorer(s.tier)`). They flow through projection/fixpoint to feed
    /// `apply_model_invocations`, then get stripped from the final
    /// `LocyResult` rows by the runtime.
    pub hidden_yield_cols: Vec<String>,
}

/// A single neural-model invocation site extracted from a clause body.
///
/// At runtime, after the clause body produces a batch of rows, each
/// invocation evaluates its `feature_exprs` per row, packs them into
/// [`crate::ClassifyInput`]s, calls the classifier in one batched
/// `classify` call, then appends the result vector as a new column
/// `output_column` to the batch.
#[derive(Debug, Clone)]
pub struct ModelInvocation {
    /// Name of the model from `CREATE MODEL <name>`.
    pub model_name: String,
    /// Synthetic column name where the per-row probabilities are
    /// written. Generated as `__model_<name>_<idx>` where `idx` is a
    /// dedup index for repeated invocations of the same model.
    pub output_column: String,
    /// Argument expressions from the invocation — one per declared
    /// `INPUT` binding. Evaluated in clause-body scope to produce the
    /// per-row feature value passed under the binding's `variable` name.
    pub feature_exprs: Vec<Expr>,
    /// Names of the model's `INPUT` bindings in declaration order, used
    /// as feature keys when building [`crate::ClassifyInput`].
    pub feature_names: Vec<String>,
    /// Property-access expressions referenced by `feature_exprs`,
    /// recorded as `(variable, property)` pairs (e.g. for
    /// `scorer(s.tier)` → `[("s", "tier")]`). The compiler appends a
    /// matching hidden YIELD item for each so the planner's standard
    /// property-materialization pipeline produces a column named
    /// `"<variable>.<property>"` in the body batch; runtime then
    /// reads from that column.
    pub feature_property_refs: Vec<(String, String)>,
    /// Phase C B1–B3: when the invocation appears in a YIELD item
    /// (e.g. `scorer(s) AS risk`), this carries the user-visible
    /// alias (`risk`) — distinct from the synthetic
    /// `output_column` (`__model_scorer_0`). Allows EXPLAIN to look
    /// up the model output by the column name that survives
    /// `LocyProject`'s projection. `None` when the invocation lives
    /// only inside an ALONG / FOLD expression and never surfaces
    /// as a user-visible YIELD column.
    pub yield_alias: Option<String>,
    /// Phase C B1-B3 follow-up: the user-authored feature
    /// expressions BEFORE the `InvocationLifter` rewrote them to
    /// `Variable("__model_<n>_<idx>")` references. Preserved so
    /// EXPLAIN can reconstruct `ClassifyInput` per fact at lookup
    /// time (the rewritten `feature_exprs` carry synthetic-column
    /// references that can't be evaluated against a post-projection
    /// fact_row). Same length and ordering as `feature_exprs` and
    /// `feature_names`.
    pub original_feature_exprs: Vec<Expr>,
    /// Phase D D3: snapshot of the model's `path_context` declaration
    /// (if any) carried onto the invocation so the runtime can pull
    /// the named column from the source rule's derived facts at
    /// classify time without re-consulting the model catalog.
    pub path_context: Option<uni_cypher::locy_ast::PathContextFeature>,
    /// Phase D D2 follow-up: optional embedder alias from the model's
    /// `USING xervo('classify/X', embedder='alias')` clause. When
    /// `None`, the runtime falls back to alias `"default"` for
    /// `semantic_match` query-text embedding.
    pub embedder_alias: Option<String>,
}

/// A column in a rule's YIELD schema.
#[derive(Debug, Clone, PartialEq)]
pub struct YieldColumn {
    pub name: String,
    pub is_key: bool,
    pub is_prob: bool,
}

/// A non-fatal compiler diagnostic.
#[derive(Debug, Clone)]
pub struct CompilerWarning {
    pub code: WarningCode,
    pub message: String,
    pub rule_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WarningCode {
    MsumNonNegativity,
    ProbabilityDomainViolation,
    /// Phase B F1: a clause has a recursive IS-ref and a FOLD aggregate
    /// but no ALONG clause. Not an error — a recursive FOLD rolls up per KEY,
    /// composing one level at a time (a self-reference reads the folded value,
    /// issue #162). The warning flags the choice because authors who wanted a
    /// value accumulated along each *path* want ALONG instead.
    /// (Stress Corpus B3.)
    FoldInRecursivePath,
    /// Phase C C4: `VALIDATE METRICS ece` was requested; the equal-width
    /// binning ECE is biased in the small-sample regime (Kumar et al.
    /// NeurIPS 2019). Use `DEBIASED_ECE` instead for an unbiased
    /// estimator. The bare ECE value is still reported.
    EceBinningBias,
    /// Phase B G1-lite: a `CREATE MODEL` declares no CALIBRATION (or
    /// `CALIBRATION None`) AND the `xervo_alias` heuristically looks like
    /// an LLM provider (`generate/...`, `chat/...`, `llm/...`). Raw LLM
    /// logprobs are not calibrated probabilities (rollout D-10). Treat
    /// as a documentation hint until Xervo exposes `calibration_source`.
    UncalibratedLLMLogprobs,
    /// Phase C C4: a rule body invokes a `CREATE MODEL` whose output
    /// is PROB AND which declares no CALIBRATION (or `CALIBRATION None`).
    /// The fitted probability flows into the probabilistic stack
    /// (MNOR / MPROD / complement) — without calibration, the
    /// downstream aggregates compound the miscalibration. Run a
    /// `CALIBRATE` statement to fit a transform, or explicitly mark
    /// the choice with `CALIBRATION none` to acknowledge the risk
    /// (the warning still fires for the explicit-`none` case to keep
    /// the acknowledgement visible — same pattern as Phase A's
    /// `FuzzyNotProbabilistic`, rollout D-9).
    UncalibratedNeuralPredicate,
    /// Phase C F2a: two or more neural-model invocations in the
    /// same rule share an INPUT VARIABLE argument
    /// (e.g. `model_a(s)` and `model_b(s)`). Under
    /// independence-by-default composing the probabilities via
    /// MNOR/MPROD is likely wrong since both share the random
    /// variable `s`. Suppressed when ALL invocations involved
    /// carry the `@independent` annotation on their `CREATE MODEL`
    /// declaration. Rollout D-8.
    SharedNeuralInputArgument,
    /// Phase C F2b: two or more neural-model invocations in the
    /// same rule share an equivalent FEATURE VALUE expression
    /// (e.g. `model_a(s.tier)` and `model_b(s.tier)`). Different
    /// from F2a — even when binding variables differ, the feature
    /// input is structurally identical so the same correlation
    /// concern applies. Suppression by `@independent` annotation.
    SharedNeuralFeatureValue,
    /// Phase D F3 case 3: a rule body has both a positive IS-ref
    /// and an IS NOT (complement) to *different* rules on the
    /// *same* subject variable. When the positive and negated
    /// rules share base facts, the independence assumption that
    /// underlies the probabilistic complement / aggregation is
    /// violated. This is a structural over-detection (the MVP
    /// fires whenever the pattern matches, even if no actual base
    /// overlap exists at runtime); a future refinement will gate
    /// on runtime support-set intersection.
    PositiveComplementCorrelation,
    /// Phase D F3 case 2: a rule body has two or more positive
    /// IS-refs to *different* PROB-bearing rules on the *same*
    /// subject variable. The implicit `p AND q` conjunction
    /// assumes independence between `p` and `q`, which is wrong
    /// when the two rules share base facts. Structural
    /// over-detection (the MVP fires whenever the pattern
    /// matches, even if no actual support overlap exists at
    /// runtime); a future refinement will gate on runtime
    /// support-set intersection.
    CrossPredicateCorrelation,
    /// Phase D F3 case 4 (F2c): two or more neural-model
    /// invocations in the same rule receive retrieval-backed
    /// features (`similar_to(prop, _)` / `semantic_match(prop,
    /// _)`) over the *same* node property. The two models
    /// condition on the same retrieval evidence, so the implicit
    /// independence assumption that underlies composition via
    /// MNOR/MPROD/etc. is suspect. Suppressed when all involved
    /// models carry `@independent`. Structural over-detection;
    /// a future refinement could gate on cosine similarity of
    /// the pre-embedded query vectors (queries are constants per
    /// `apply_model_invocations` call).
    SharedRetrievalContext,
}

/// Probability semiring used to evaluate MNOR/MPROD aggregates, PROB
/// complement, and cross-predicate combination.
///
/// `AddMultProb` is the Phase 1/2 default (noisy-OR and product under the
/// independence assumption). `MaxMinProb` is the Viterbi/fuzzy semiring
/// and triggers a non-suppressible `RuntimeWarningCode::FuzzyNotProbabilistic`
/// whenever it evaluates a PROB-bearing rule (rollout decision D-9).
/// `BddExact` is whole-group weighted model counting (Phase 7) and is
/// dispatched outside the row-at-a-time `LocySemiring` trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SemiringKind {
    #[default]
    AddMultProb,
    MaxMinProb,
    BddExact,
    /// Phase C C0: top-K proof tracking with per-row dependency DNFs
    /// (impl plan §1.6, decision D-7). Each row carries up to `k`
    /// proofs whose `base_rvs` flag shared dependencies; the per-tag
    /// probability is computed via inclusion-exclusion over the DNF.
    ///
    /// Stage 1 (this slice): library-layer math complete; runtime
    /// `SemiringDispatch` falls back to `AddMultProb` row math with a
    /// loud tracing warn. Stage 2 wires tag flow through
    /// `MonotonicAggState` and `FoldExec`.
    TopKProofs {
        k: u32,
    },
}

/// Classification of runtime warnings emitted during evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeWarningCode {
    /// Two or more proof paths aggregated by MNOR/MPROD share an
    /// intermediate fact, violating the independence assumption.
    SharedProbabilisticDependency,
    /// A shared-proof group exceeded `max_bdd_variables`, so the BDD
    /// computation fell back to the independence-mode result.
    BddLimitExceeded,
    /// Base facts are shared across different KEY groups within the same
    /// rule. The BDD corrects per-group probabilities but cannot account
    /// for cross-group correlations.
    CrossGroupCorrelationNotExact,
    /// The `MaxMinProb` (fuzzy / Viterbi) semiring evaluated a PROB-bearing
    /// rule. Per rollout decision D-9 this warning is **unsuppressible**:
    /// fuzzy truth values are not probabilities, and silent conflation is
    /// the dominant pitfall in neuro-symbolic systems (LTN, NTP).
    FuzzyNotProbabilistic,
    /// Entity hydration did not complete, so KEY columns for the named rule
    /// carry raw integer VIDs instead of full `Value::Node` objects.
    ///
    /// The encoding of a result must not depend on timing. When the VID lookup
    /// cannot finish, callers that reach for node properties (`r.properties`)
    /// silently see nothing rather than an error, so this degradation is
    /// reported rather than swallowed.
    EntityHydrationIncomplete,
    /// Phase C C0: a `TopKProofs::plus` operation discarded a proof
    /// whose `base_rvs` overlapped a retained proof — top-K is too
    /// small for the program's dependency structure (impl plan §3.0,
    /// rollout doc §6). Increase `k` or accept the
    /// approximation. Emitted from library code; Stage 2 wires it
    /// into the runtime tag flow.
    TopKPruningCrossedDependency,
}

/// A non-fatal runtime diagnostic collected during evaluation.
#[derive(Debug, Clone)]
pub struct RuntimeWarning {
    /// Warning classification.
    pub code: RuntimeWarningCode,
    /// Human-readable explanation.
    pub message: String,
    /// Rule that triggered the warning, when applicable.
    pub rule_name: String,
    /// BDD variable count for the affected group (BddLimitExceeded only).
    pub variable_count: Option<usize>,
    /// Human-readable KEY group description (BddLimitExceeded only).
    pub key_group: Option<String>,
}
