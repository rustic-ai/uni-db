use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum LocyCompileError {
    #[error("cyclic negation among rules: {}", rules.join(", "))]
    CyclicNegation { rules: Vec<String> },

    #[error("undefined rule: {name}")]
    UndefinedRule { name: String },

    #[error("prev reference in non-recursive rule '{rule}', field '{field}'")]
    PrevInBaseCase { rule: String, field: String },

    #[error("non-monotonic aggregate '{aggregate}' in recursive rule '{rule}'")]
    NonMonotonicInRecursion { rule: String, aggregate: String },

    #[error("BEST BY with monotonic fold '{fold}' in rule '{rule}'")]
    BestByWithMonotonicFold { rule: String, fold: String },

    #[error("post-FOLD WHERE in rule '{rule}' requires a FOLD clause")]
    HavingWithoutFold { rule: String },

    #[error(
        "REQUIRE in recursive rule '{rule}' is not monotone: {detail}. \
         A REQUIRE constrains the recursion itself, so it must only ever be \
         able to turn from false to true as the fixpoint grows — a lower bound \
         (>=, >) over a non-decreasing fold, or an upper bound (<=, <) over a \
         non-increasing one. Otherwise a fact could be derived and then \
         withdrawn, which the fixpoint reads as progress and would run to the \
         iteration limit. Use the post-FOLD WHERE instead to filter the \
         converged answer (issue #265)"
    )]
    NonMonotonicFilterInRecursion { rule: String, detail: String },

    #[error("REQUIRE in rule '{rule}' requires a FOLD clause")]
    RequireWithoutFold { rule: String },

    #[error("wardedness violation: variable '{variable}' in rule '{rule}' not bound by MATCH")]
    WardednessViolation { rule: String, variable: String },

    #[error("YIELD schema mismatch in rule '{rule}': {detail}")]
    YieldSchemaMismatch { rule: String, detail: String },

    #[error("mixed priority in rule '{rule}': some clauses have PRIORITY, others don't")]
    MixedPriority { rule: String },

    #[error("module not found: {name}")]
    ModuleNotFound { name: String },

    #[error("import not found: rule '{rule}' in module '{module}'")]
    ImportNotFound { module: String, rule: String },

    #[error(
        "IS arity mismatch in rule '{rule}': reference to '{target}' provides {actual} bindings, but '{target}' yields {expected} columns"
    )]
    IsArityMismatch {
        rule: String,
        target: String,
        expected: usize,
        actual: usize,
    },

    /// The subject of an `IS NOT` reference is not a node.
    ///
    /// Negation joins on node identity, so the subject has to be something that
    /// has a vid: a node variable bound by MATCH, or one bound by an earlier
    /// positive `IS ... TO` target in the same WHERE. A scalar (a YIELD alias,
    /// an ALONG or FOLD name, a generator output) or a relationship variable
    /// cannot be one, and the runtime already rejects all of them — this just
    /// says so at compile time instead of after evaluation starts.
    ///
    /// The message deliberately contains the literal `IS NOT`; callers and
    /// tests match on it to identify negation failures regardless of phase.
    #[error(
        "IS NOT subject '{variable}' in rule '{rule}' is not a node bound by MATCH. \
         Negation joins on node identity, so the subject must be a node variable \
         bound by the rule's MATCH pattern (or by an earlier positive `IS ... TO` \
         target); a scalar value or a relationship variable cannot be one."
    )]
    IsNotSubjectNotANode { rule: String, variable: String },

    #[error(
        "prev.{field} in rule '{rule}' references unknown column; available columns from IS references: {available}"
    )]
    PrevFieldNotInSchema {
        rule: String,
        field: String,
        available: String,
    },

    #[error("rule '{rule}' has {count} PROB columns; at most 1 is allowed")]
    MultipleProbColumns { rule: String, count: usize },

    // ─── Phase B (neural predicates preview) ─────────────────────────────
    #[error(
        "CREATE MODEL '{model_name}' parsed but neural_predicates_preview is disabled; \
         set LocyConfig::neural_predicates_preview = true to enable"
    )]
    NeuralPreviewDisabled { model_name: String },

    #[error("model name collision: '{name}' is already declared")]
    ModelNameCollision { name: String },

    #[error(
        "model '{name}' arity mismatch in rule '{rule}': expected {expected} input(s), got {actual}"
    )]
    ModelArityMismatch {
        name: String,
        rule: String,
        expected: usize,
        actual: usize,
    },

    // ─── Phase C C2: CALIBRATE statement ────────────────────────────────
    #[error(
        "CALIBRATE references unknown model '{name}'; declare it with \
         CREATE MODEL first"
    )]
    CalibrateUnknownModel { name: String },

    #[error(
        "CALIBRATE on model '{name}': calibration only applies to PROB \
         outputs, but '{name}' is declared as {declared}"
    )]
    CalibrateOnNonProbModel { name: String, declared: String },

    #[error(
        "CALIBRATE on model '{model_name}': HOLDOUT must be in the open \
         interval (0, 1); got {holdout}"
    )]
    CalibrateInvalidHoldout { model_name: String, holdout: f64 },

    #[error(
        "CALIBRATE '{model_name}' parsed but neural_predicates_preview is \
         disabled; set LocyConfig::neural_predicates_preview = true to enable"
    )]
    CalibratePreviewDisabled { model_name: String },

    // ─── Phase C C3: VALIDATE statement ─────────────────────────────────
    #[error(
        "VALIDATE references unknown rule '{name}'; declare it with \
         CREATE RULE first"
    )]
    ValidateUnknownRule { name: String },

    #[error(
        "VALIDATE rule '{name}' has no PROB column; calibration metrics \
         only apply to probability outputs"
    )]
    ValidateRuleHasNoProbColumn { name: String },

    #[error("VALIDATE rule '{name}' must request at least one metric")]
    ValidateNoMetrics { name: String },

    /// Phase B follow-up: a WHERE clause invokes a neural model.
    /// The lift machinery would require splitting the rule's
    /// `body_logical` into pre-filter and post-filter halves so the
    /// classifier can run between them — a planner refactor we've
    /// scoped out of the current slice. Surface a clear error at
    /// compile time directing the user to move the invocation into
    /// a YIELD item, e.g. as a witness column they can filter on
    /// downstream.
    #[error(
        "rule '{rule}' invokes neural model '{model}' in a WHERE clause, \
         which is not yet supported. Lift the call into YIELD (e.g. \
         `YIELD KEY x, {model}(x) AS p`) and apply the filter on the \
         materialized rule output instead."
    )]
    WhereModelInvocationNotYetSupported { rule: String, model: String },

    /// A neural-model invocation's feature expression is not a plain
    /// variable or a single `node.property` access. Today's runtime
    /// reads features either from match-bound variables (e.g.
    /// `scorer(s)`) or from materialized property columns (e.g.
    /// `scorer(s.tier)`); arithmetic (`scorer(s.tier + 1)`) and nested
    /// calls (`scorer(normalize(s.revenue))`) are deferred to a
    /// follow-up slice.
    #[error(
        "rule '{rule}': neural model '{model}' feature expression \
         {expr} is unsupported — only plain variables and direct \
         property access (`var.prop`) are accepted today"
    )]
    UnsupportedFeatureExpression {
        rule: String,
        model: String,
        expr: String,
    },
}
