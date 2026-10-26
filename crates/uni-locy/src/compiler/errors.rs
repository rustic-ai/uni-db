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

    /// A FOLD clause yields a column that is neither a KEY nor a FOLD output.
    ///
    /// A FOLD groups rows by the KEY columns and emits one row per group, so
    /// such a column has no single value to carry and the runtime drops it.
    /// Rejecting it here replaces a silently absent column, which a later
    /// `QUERY ... RETURN` would read as NULL (issue #293).
    #[error(
        "YIELD column '{column}' in FOLD rule '{rule}' is neither a KEY nor a FOLD output. \
         A FOLD emits one row per distinct KEY, so any other column would be dropped. \
         `KEY` marks a single YIELD item: to group by several columns write \
         `YIELD KEY a, KEY b, total`, not `YIELD KEY a, b, total`"
    )]
    UngroupedFoldYield { rule: String, column: String },

    /// An aggregate function call in a rule's YIELD item or ALONG binding.
    ///
    /// A rule aggregates through FOLD alone: a YIELD or ALONG expression is
    /// evaluated per row, where an aggregate has no group. It used to reach
    /// the physical planner and fail there with "Physical plan does not
    /// support logical expression AggregateFunction".
    #[error(
        "InvalidAggregation - aggregation function in the {position} of rule '{rule}'. \
         A rule aggregates through FOLD: write `FOLD total = SUM(x)` and yield `total`"
    )]
    AggregateOutsideFold { rule: String, position: String },

    /// A QUERY references a variable its rule does not yield.
    ///
    /// The QUERY is evaluated over the rule's derived facts, so a variable
    /// that is not one of its YIELD columns has no value in any row. It used
    /// to evaluate to NULL, which made a dropped or misspelled column look
    /// like missing data (issue #293).
    #[error(
        "QUERY {rule} references '{variable}', which rule '{rule}' does not yield; \
         its columns are: {}",
        available.join(", ")
    )]
    UnknownQueryVariable {
        rule: String,
        variable: String,
        available: Vec<String>,
    },

    #[error("wardedness violation: variable '{variable}' in rule '{rule}' not bound by MATCH")]
    WardednessViolation { rule: String, variable: String },

    #[error("YIELD schema mismatch in rule '{rule}': {detail}")]
    YieldSchemaMismatch { rule: String, detail: String },

    #[error("mixed priority in rule '{rule}': some clauses have PRIORITY, others don't")]
    MixedPriority { rule: String },

    /// Clauses of one rule state different FOLD, post-FOLD WHERE, REQUIRE or
    /// BEST BY.
    ///
    /// These apply to the whole rule, after its clauses are unioned, so a rule
    /// has one of each. The planner used to take them from the first clause
    /// that folds and silently ignore what any other clause stated.
    #[error(
        "clauses of rule '{rule}' disagree on {part}: {detail}. A rule's FOLD, \
         post-FOLD WHERE, REQUIRE and BEST BY apply to all of its clauses at \
         once, so every clause that folds must state the same ones; a clause \
         without FOLD seeds the fold and states none of them"
    )]
    ClauseMismatch {
        rule: String,
        part: String,
        detail: String,
    },

    /// A FOLD aggregate in a recursive rule uses `DISTINCT`.
    ///
    /// A recursive FOLD is computed incrementally, one iteration's new rows at
    /// a time, so it never sees the whole bag a distinct count would need.
    #[error(
        "FOLD '{fold}' in recursive rule '{rule}' uses DISTINCT, which only a \
         non-recursive rule supports: a recursive FOLD folds each iteration's \
         new rows and never sees the whole set of values. Fold the distinct \
         values in a non-recursive rule that reads this one"
    )]
    DistinctFoldInRecursion { rule: String, fold: String },

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

    /// `prev` written anywhere but as a direct operand of an ALONG expression.
    ///
    /// `prev.<column>` reads the referenced fact in a recursive clause, and
    /// only ALONG gives it that meaning. Every other slot (and an ALONG under
    /// a function call or parentheses) is plain Cypher, where `prev` is just an
    /// unbound variable that failed at run time with "No field named prev".
    #[error(
        "`prev` in the {slot} of rule '{rule}': `prev.<column>` reads the referenced fact only \
         as a direct operand of an ALONG expression in a recursive clause (for example \
         `ALONG c = prev.c + e.w`). To read a referenced rule's value elsewhere, use its \
         column name."
    )]
    PrevOutsideAlong { rule: String, slot: String },

    /// An expression names a variable that is not bound where it is written.
    #[error(
        "variable `{variable}` in the {slot} of rule '{rule}' is not bound there. Visible: \
         {available:?}"
    )]
    UnboundVariable {
        rule: String,
        slot: String,
        variable: String,
        available: Vec<String>,
    },

    /// A FOLD binding whose value is not a single aggregate call.
    ///
    /// The runtime folds the call's argument, so `FOLD t = SUM(e.w) * 1.0`
    /// has nothing to fold, and the recursion checks only see top-level calls.
    #[error(
        "FOLD `{name}` in rule '{rule}' must be a single aggregate call such as \
         `SUM(e.w)`; compute with its result in YIELD instead (`... YIELD {name} * 1.0 AS x`)"
    )]
    FoldNotAnAggregateCall { rule: String, name: String },

    /// An `IS NOT` reference compares an already-bound variable with one of
    /// the referenced rule's value columns.
    ///
    /// A fact is identified by its KEY columns, and negation asks whether a
    /// fact exists, so it cannot compare a value. An *unbound* variable on a
    /// value column is legal: it is existentially quantified and ignored, so
    /// `b IS NOT stake TO agg` means `b IS NOT stake`. Bindings follow
    /// [`crate::is_binding::binding_sequence`].
    #[error(
        "IS NOT reference to '{target}' in rule '{rule}' compares the bound variable \
         `{variable}` with `{column}`, which is not a KEY column of '{target}'. Negation asks \
         whether a fact exists, and a fact is identified by its KEY, so IS NOT cannot compare a \
         value; test it in WHERE after a positive IS reference instead."
    )]
    IsNotBindsValueColumn {
        rule: String,
        target: String,
        variable: String,
        column: String,
    },

    /// The subject of an `IS NOT` reference is not a node.
    ///
    /// Negation joins on node identity, so a subject on a node KEY column has
    /// to be something that has a vid: a node variable bound by MATCH, a node
    /// a positive IS reference binds anywhere in the body (before or after
    /// the negation — binding does not depend on conjunct order), or a YIELD
    /// alias of either. A scalar (an ALONG or FOLD name, a generator output, a
    /// YIELD alias of a non-node) or a relationship variable cannot be one,
    /// and neither can a name bound nowhere. Decided by the clause's binding
    /// plan ([`crate::binding_plan`]).
    ///
    /// The message deliberately contains the literal `IS NOT`; callers and
    /// tests match on it to identify negation failures regardless of phase.
    #[error(
        "IS NOT subject '{variable}' in rule '{rule}' is not a bound node. \
         Negation joins on node identity, so the subject must be a node variable \
         bound by the rule's MATCH pattern or by a positive IS reference anywhere \
         in the body; a scalar value or a relationship variable cannot be one."
    )]
    IsNotSubjectNotANode { rule: String, variable: String },

    /// A clause reads a bare name that two or more IS references expose.
    ///
    /// A referenced rule's value column that no subject or target binds is
    /// readable by its bare name. When two references expose the same name,
    /// nothing says which one a read means, and the planner used to pick
    /// whichever reference came first in the WHERE, so swapping two conjuncts
    /// changed the answer (issue #300). Exposing a duplicate without reading
    /// it stays legal.
    #[error(
        "column `{column}` read in the {slot} of rule '{rule}' is ambiguous: it is a value \
         column of {}. Bind it positionally to a name of its own instead, for example \
         `(o, e, p1) IS stake`",
        refs.join(" and ")
    )]
    AmbiguousRuleColumn {
        rule: String,
        column: String,
        slot: String,
        refs: Vec<String>,
    },

    /// An IS-reference position joins a node, a relationship and a scalar.
    ///
    /// Each column of a rule holds one kind of value: a node, a relationship,
    /// or an ordinary scalar, decided by the rule's YIELD expression. A
    /// variable of one kind on a column of another can never match correctly:
    /// a node's id compared with an integer column matches whichever integers
    /// happen to equal a node id (issue #300).
    #[error(
        "IS reference to '{target}' in rule '{rule}' puts `{variable}`, a {found}, on \
         column `{column}`, which holds a {expected}"
    )]
    IsRefKindMismatch {
        rule: String,
        target: String,
        variable: String,
        column: String,
        expected: crate::binding_plan::ColumnKind,
        found: crate::binding_plan::ColumnKind,
    },

    /// Generators whose arguments can only be bound by each other's outputs.
    #[error(
        "generators in rule '{rule}' depend on each other's outputs and can never run: {}",
        generators.join(", ")
    )]
    GeneratorCycle {
        rule: String,
        generators: Vec<String>,
    },

    /// A KEY variable of an `IS NOT` reference is bound nowhere in the body.
    ///
    /// Negation asks whether a fact with these KEY values exists, so every
    /// KEY value must come from somewhere: MATCH, a positive IS reference or
    /// a generator.
    #[error(
        "IS NOT reference to '{target}' in rule '{rule}': KEY variable `{variable}` is not \
         bound by MATCH, a positive IS reference or a generator, so negation has no value \
         to test"
    )]
    IsNotKeyUnbound {
        rule: String,
        target: String,
        variable: String,
    },

    /// `prev.<field>` in a clause with several recursive IS references.
    ///
    /// `prev` names the fact of the one IS reference that recurses; with two
    /// of them it is unclear which fact it reads.
    #[error(
        "prev.{field} in rule '{rule}' is ambiguous: the clause has more than one IS \
         reference to a rule of its own recursive group"
    )]
    AmbiguousPrevRef { rule: String, field: String },

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
