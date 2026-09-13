use std::collections::{HashMap, HashSet};

use uni_cypher::ast::{BinaryOp, Expr};
use uni_cypher::locy_ast::{
    AlongBinding, FoldBinding, LocyExpr, LocyYieldItem, RuleCondition, RuleDefinition, RuleOutput,
    resolve_yield_column_names,
};

use uni_common::locy::FoldDirection;

use super::errors::LocyCompileError;
use super::stratify::StratificationResult;
use crate::types::{
    CompiledClause, CompiledModel, CompiledRule, CompilerWarning, ModelInvocation, WarningCode,
    YieldColumn,
};
use uni_cypher::locy_ast::OutputType;

/// Predicate returning aggregate monotonicity by name.
///
/// `Some(true)` — registered as a monotone Locy aggregate, sound to use in
/// recursive strata. `Some(false)` — registered but non-monotone, must be
/// rejected in recursion. `None` — unknown to the oracle, treated as
/// rejection in recursive strata.
pub type MonotonicityOracle<'a> = &'a (dyn Fn(&str) -> Option<bool> + 'a);

/// Default oracle for callers without a `PluginRegistry`.
///
/// Recognises exactly the six built-in `M`-prefixed lattice folds —
/// `MMAX`, `MMIN`, `MCOUNT`, `MNOR`, `MPROD`, `MSUM` — as a user-asserted
/// monotonicity contract, and answers `None` for everything else, which the
/// recursive-stratum check rejects.
///
/// This is an **exact list, not an `M`-prefix rule**: `MFOO` answers `None`.
/// (An earlier version of this comment claimed a prefix convention the body has
/// never implemented.)
///
/// Hosts that load `uni-plugin-builtin` should supply a registry-backed
/// oracle instead so user-registered aggregates participate in the check.
#[must_use]
pub fn default_monotonicity_oracle(name: &str) -> Option<bool> {
    match name.to_uppercase().as_str() {
        "MMAX" | "MMIN" | "MCOUNT" | "MNOR" | "MPROD" | "MSUM" => Some(true),
        _ => None,
    }
}

/// Resolves an aggregate name to the direction its value moves in (#265).
///
/// The sibling of [`MonotonicityOracle`], kept separate rather than folded into
/// it so that adding direction does not change that type and every one of its
/// call sites. Answers [`FoldDirection::Unknown`] for anything it does not
/// recognise, which rejects `REQUIRE`.
pub type DirectionOracle<'a> = &'a (dyn Fn(&str) -> FoldDirection + 'a);

/// Default direction oracle for callers without a `PluginRegistry`.
///
/// Covers exactly the names [`default_monotonicity_oracle`] accepts, since a
/// `REQUIRE` is only ever checked on a fold that already passed the
/// monotonicity gate. Mirrors the table in
/// `skills/uni-db/references/locy.md`, which is the user-facing statement of
/// the same facts.
///
/// `MSUM` is non-decreasing **only over non-negative inputs** — the
/// precondition the `MsumNonNegativity` warning already covers. It is reported
/// as non-decreasing here and the `REQUIRE` check surfaces the caveat, rather
/// than refusing the overwhelmingly common non-negative case.
pub fn default_direction_oracle(name: &str) -> FoldDirection {
    match name.to_uppercase().as_str() {
        "MMAX" | "MCOUNT" | "MNOR" | "MSUM" => FoldDirection::NonDecreasing,
        "MMIN" | "MPROD" => FoldDirection::NonIncreasing,
        _ => FoldDirection::Unknown,
    }
}

/// Names Locy treats as a *declared* lattice fold for the `BEST BY` guard.
///
/// Deliberately **not** the injected [`MonotonicityOracle`], and deliberately
/// not implemented in terms of it. The two ask different questions that only
/// happen to share a signature:
///
/// * the oracle asks *"is this aggregate sound under a fixpoint?"* — a lattice
///   property, which a plugin registry answers authoritatively;
/// * this asks *"did the user write a declared lattice fold?"* — a syntactic
///   marker over the six built-in `M*` spellings.
///
/// Conflating them inverts the oracle's meaning: `check_best_by_monotonic_fold`
/// treats `Some(true)` as an *error*, so pointing it at a registry would newly
/// reject `BEST BY … FOLD MAX(x)` / `MIN` / `COUNT` / `COLLECT` — all of which
/// are `monotone_join: true` in the builtin registry, and all of which are
/// ordinary, runtime-correct programs. The guard also runs for *every* rule,
/// not just recursive ones, so that rejection would not even be confined to
/// recursion.
fn is_declared_lattice_fold(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "MMAX" | "MMIN" | "MCOUNT" | "MNOR" | "MPROD" | "MSUM"
    )
}

/// Validate all rules and produce `CompiledRule` entries plus warnings.
///
/// `model_catalog` carries the Phase B `CREATE MODEL` declarations; rule
/// bodies that reference a model name via function-call syntax are
/// validated for arity here. An empty catalog (the legacy path) is
/// equivalent to "no models registered".
///
/// `is_monotonic` resolves aggregate function names to a tri-state
/// monotonicity verdict used for the recursive-stratum check (see
/// [`MonotonicityOracle`]).
pub fn check(
    rule_groups: &HashMap<String, Vec<&RuleDefinition>>,
    strat: &StratificationResult,
    model_catalog: &HashMap<String, CompiledModel>,
    module_ctx: &super::modules::ModuleContext,
    is_monotonic: MonotonicityOracle<'_>,
) -> Result<(HashMap<String, CompiledRule>, Vec<CompilerWarning>), LocyCompileError> {
    let mut compiled_rules = HashMap::new();
    let mut warnings = Vec::new();

    // Process rules in deterministic order
    let mut rule_names: Vec<&String> = rule_groups.keys().collect();
    rule_names.sort();

    for rule_name in rule_names {
        let definitions = &rule_groups[rule_name];
        let scc_idx = strat.scc_map[rule_name.as_str()];
        let is_recursive = strat.is_recursive[scc_idx];

        check_mixed_priority(rule_name, definitions)?;

        let mut yield_schema = infer_yield_schema(rule_name, definitions)?;

        // Implicit PROB: if a fold uses MNOR/MPROD, mark the matching yield column as PROB
        for def in definitions.iter() {
            for_each_fold_call(&def.fold, |fold, name, _args| {
                if matches!(name.to_uppercase().as_str(), "MNOR" | "MPROD")
                    && let Some(col) = yield_schema.iter_mut().find(|c| c.name == fold.name)
                {
                    col.is_prob = true;
                }
            });
        }

        // Phase B A5: auto-flag PROB for YIELD items whose expression is
        // a neural-model invocation declaring `OUTPUT PROB`. The yield
        // column's name is the explicit alias or the model's output
        // identifier when used bare. We resolve to the column name in
        // `yield_schema` produced by `infer_yield_schema`.
        for def in definitions.iter() {
            if let RuleOutput::Yield(yc) = &def.output {
                for item in &yc.items {
                    let Expr::FunctionCall { name, .. } = &item.expr else {
                        continue;
                    };
                    let Some(model) = model_catalog.get(name) else {
                        continue;
                    };
                    if model.output_type != OutputType::Prob {
                        continue;
                    }
                    // Column name: alias if present, else fall back to the
                    // function-call name (mirroring infer_yield_schema's
                    // alias-then-default policy).
                    let col_name = item.alias.clone().unwrap_or_else(|| name.clone());
                    if let Some(col) = yield_schema.iter_mut().find(|c| c.name == col_name) {
                        col.is_prob = true;
                    }
                }
            }
        }

        // Validate: at most 1 PROB column per rule
        let prob_count = yield_schema.iter().filter(|c| c.is_prob).count();
        if prob_count > 1 {
            return Err(LocyCompileError::MultipleProbColumns {
                rule: rule_name.clone(),
                count: prob_count,
            });
        }

        let scc_rules = &strat.sccs[scc_idx];

        let mut clauses = Vec::new();
        for def in definitions {
            // Check prev in any clause that lacks a self-IS-reference within the same SCC
            let has_self_is = def.where_conditions.iter().any(|cond| {
                if let RuleCondition::IsReference(is_ref) = cond {
                    // The SCC set holds module-qualified names; the IS-ref name is
                    // raw, so qualify it first or self-recursion detection breaks
                    // inside a MODULE (e.g. "r" vs "foo.r").
                    let qualified = super::modules::resolve_rule_name(
                        module_ctx,
                        &is_ref.rule_name.to_string(),
                    );
                    scc_rules.contains(&qualified)
                } else {
                    false
                }
            });
            if !has_self_is {
                check_prev_in_base_case(rule_name, def)?;
            }

            if is_recursive {
                check_non_monotonic_in_recursion(rule_name, def, is_monotonic)?;
                check_msum_warning(rule_name, def, &mut warnings);
                check_probability_domain_warning(rule_name, def, &mut warnings);
                // F1: clause has FOLD + recursive IS-ref (same SCC) + no ALONG
                // → almost certainly a semantic mistake (Stress Corpus B3).
                check_fold_in_recursive_path(rule_name, def, scc_rules, &mut warnings);
                // #265: the same shape *plus* a post-FOLD WHERE, which does not
                // constrain the recursion it looks like it constrains.
                check_having_in_recursive_path(rule_name, def, scc_rules, &mut warnings);
                // #265: REQUIRE constrains the recursion, so its threshold
                // must be provably one-way or the fixpoint cannot terminate.
                check_require_direction(rule_name, def, &default_direction_oracle)?;
            }

            // NB: deliberately outside the `is_recursive` block above — a
            // BEST BY / lattice-fold contradiction is wrong in any rule.
            check_best_by_monotonic_fold(rule_name, def)?;

            // Validate model invocations in this clause's body. Each call
            // `model_name(arg1, ..., argN)` must (1) refer to a declared
            // model, and (2) supply N arguments matching `INPUT` arity.
            // Phase C C4: emits `UncalibratedNeuralPredicate` when an
            // invoked PROB model has no CALIBRATION declared.
            check_model_invocations(rule_name, def, model_catalog, &mut warnings)?;
            // Phase C F2: detect cross-model input sharing that
            // composes under independence-by-default — fires F2a /
            // F2b unless all involved models carry `@independent`.
            check_shared_neural_inputs(rule_name, def, model_catalog, &mut warnings);
            // Phase D F3 case 3: rule body has both `IS p` and `IS NOT q`
            // on the same subject — emits PositiveComplementCorrelation.
            check_positive_complement_pair(rule_name, def, &mut warnings);

            // HAVING (post-FOLD WHERE) requires a FOLD clause.
            if !def.having.is_empty() && def.fold.is_empty() {
                return Err(LocyCompileError::HavingWithoutFold {
                    rule: rule_name.clone(),
                });
            }
            // REQUIRE names a FOLD output, so it needs one to name (#265).
            if !def.require.is_empty() && def.fold.is_empty() {
                return Err(LocyCompileError::RequireWithoutFold {
                    rule: rule_name.clone(),
                });
            }

            // Phase B Slice 3 + A4 follow-up: extract model invocations
            // from YIELD items, ALONG bindings, and FOLD aggregate
            // expressions. All three positions are lifted into hidden
            // `__model_<n>_<idx>` columns produced by the runtime's
            // `LocyModelInvokeExec` (inserted by the planner between
            // the clause body and `LocyProject`). Property-access
            // feature exprs (e.g. `scorer(s.tier)`) accumulate hidden
            // YIELD items so the standard property-materialization
            // pipeline feeds the invocation pass.
            let extracted = extract_model_invocations(rule_name, def, model_catalog)?;

            clauses.push(CompiledClause {
                match_pattern: def.match_pattern.clone(),
                where_conditions: def.where_conditions.clone(),
                along: extracted.along,
                fold: extracted.fold,
                require: def.require.clone(),
                having: def.having.clone(),
                best_by: def.best_by.clone(),
                output: extracted.output,
                priority: def.priority,
                model_invocations: extracted.invocations,
                hidden_yield_cols: extracted.hidden_yield_cols,
            });
        }

        let priority = definitions.first().and_then(|d| d.priority);

        compiled_rules.insert(
            rule_name.clone(),
            CompiledRule {
                name: rule_name.clone(),
                clauses,
                yield_schema,
                priority,
            },
        );
    }

    // Second pass: validate IS reference arity and prev field names (all yield schemas are inferred by now)
    // Also runs Phase D F3 case 2 detection — needs all rules'
    // yield_schemas in place to identify PROB-bearing targets.
    for (rule_name, rule) in &compiled_rules {
        for clause in &rule.clauses {
            check_cross_predicate_correlation(rule_name, clause, &compiled_rules, &mut warnings);
            check_is_not_subjects_are_nodes(rule_name, clause)?;
            // Collect IS references that are within the same SCC (self-IS-refs)
            let scc_idx = strat.scc_map[rule_name.as_str()];
            let scc_rules = &strat.sccs[scc_idx];

            let mut has_self_is = false;
            let mut is_ref_targets = Vec::new();

            for cond in &clause.where_conditions {
                if let RuleCondition::IsReference(is_ref) = cond {
                    let target_name = is_ref.rule_name.to_string();
                    if let Some(target_rule) = compiled_rules.get(&target_name) {
                        let binding_count =
                            is_ref.subjects.len() + is_ref.target.is_some() as usize;
                        if binding_count > target_rule.yield_schema.len() {
                            return Err(LocyCompileError::IsArityMismatch {
                                rule: rule_name.clone(),
                                target: target_name,
                                expected: target_rule.yield_schema.len(),
                                actual: binding_count,
                            });
                        }
                    }

                    if scc_rules.contains(&target_name) {
                        has_self_is = true;
                        is_ref_targets.push(target_name);
                    }
                }
            }

            // For clauses with self-IS-refs, validate that prev fields exist in referenced schemas
            if has_self_is {
                // Collect available columns: yield columns + along names from all IS-referenced rules
                let mut available_cols: HashSet<String> = HashSet::new();
                for target_name in &is_ref_targets {
                    if let Some(target_rule) = compiled_rules.get(target_name) {
                        for col in &target_rule.yield_schema {
                            available_cols.insert(col.name.clone());
                        }
                        for target_clause in &target_rule.clauses {
                            for along in &target_clause.along {
                                available_cols.insert(along.name.clone());
                            }
                        }
                    }
                }

                for along in &clause.along {
                    for prev_field in collect_prev_refs(&along.expr) {
                        if !available_cols.contains(&prev_field) {
                            let mut sorted: Vec<&str> =
                                available_cols.iter().map(|s| s.as_str()).collect();
                            sorted.sort();
                            return Err(LocyCompileError::PrevFieldNotInSchema {
                                rule: rule_name.clone(),
                                field: prev_field,
                                available: sorted.join(", "),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok((compiled_rules, warnings))
}

// ─── Mixed priority ──────────────────────────────────────────────────────────

fn check_mixed_priority(
    rule_name: &str,
    definitions: &[&RuleDefinition],
) -> Result<(), LocyCompileError> {
    if definitions.len() < 2 {
        return Ok(());
    }
    let some_have = definitions.iter().any(|d| d.priority.is_some());
    let some_lack = definitions.iter().any(|d| d.priority.is_none());
    if some_have && some_lack {
        return Err(LocyCompileError::MixedPriority {
            rule: rule_name.to_string(),
        });
    }
    Ok(())
}

// ─── YIELD schema ────────────────────────────────────────────────────────────

fn infer_yield_schema(
    rule_name: &str,
    definitions: &[&RuleDefinition],
) -> Result<Vec<YieldColumn>, LocyCompileError> {
    let mut schema: Option<Vec<YieldColumn>> = None;

    for def in definitions {
        if let RuleOutput::Yield(yc) = &def.output {
            let columns = yield_columns_from_items(&yc.items);
            if let Some(ref existing) = schema {
                if existing.len() != columns.len() {
                    return Err(LocyCompileError::YieldSchemaMismatch {
                        rule: rule_name.to_string(),
                        detail: format!(
                            "clause has {} columns, expected {}",
                            columns.len(),
                            existing.len()
                        ),
                    });
                }
                // Check is_prob consistency across clauses
                for (i, (e, c)) in existing.iter().zip(columns.iter()).enumerate() {
                    if e.is_prob != c.is_prob {
                        return Err(LocyCompileError::YieldSchemaMismatch {
                            rule: rule_name.to_string(),
                            detail: format!(
                                "column {} '{}' has inconsistent PROB annotation across clauses",
                                i, e.name
                            ),
                        });
                    }
                }
            } else {
                schema = Some(columns);
            }
        }
    }

    Ok(schema.unwrap_or_default())
}

fn yield_columns_from_items(items: &[LocyYieldItem]) -> Vec<YieldColumn> {
    resolve_yield_column_names(items)
        .into_iter()
        .zip(items.iter())
        .map(|(name, item)| YieldColumn {
            name,
            is_key: item.is_key,
            is_prob: item.is_prob,
        })
        .collect()
}

// ─── prev in base case ──────────────────────────────────────────────────────

fn check_prev_in_base_case(rule_name: &str, def: &RuleDefinition) -> Result<(), LocyCompileError> {
    for along in &def.along {
        if let Some(field) = find_prev_ref(&along.expr) {
            return Err(LocyCompileError::PrevInBaseCase {
                rule: rule_name.to_string(),
                field,
            });
        }
    }
    Ok(())
}

fn find_prev_ref(expr: &LocyExpr) -> Option<String> {
    // `collect_prev_refs` walks left-first, matching this fn's former
    // `.or_else` order, so the first collected ref is the same one.
    collect_prev_refs(expr).into_iter().next()
}

fn collect_prev_refs(expr: &LocyExpr) -> Vec<String> {
    match expr {
        LocyExpr::PrevRef(field) => vec![field.clone()],
        LocyExpr::BinaryOp { left, right, .. } => {
            let mut refs = collect_prev_refs(left);
            refs.extend(collect_prev_refs(right));
            refs
        }
        LocyExpr::UnaryOp(_, inner) => collect_prev_refs(inner),
        LocyExpr::Cypher(_) => vec![],
    }
}

// ─── Non-monotonic in recursion ──────────────────────────────────────────────

fn check_non_monotonic_in_recursion(
    rule_name: &str,
    def: &RuleDefinition,
    is_monotonic: MonotonicityOracle<'_>,
) -> Result<(), LocyCompileError> {
    try_for_each_fold_call(&def.fold, |_fold, name, _args| {
        if matches!(is_monotonic(name), Some(true)) {
            Ok(())
        } else {
            Err(LocyCompileError::NonMonotonicInRecursion {
                rule: rule_name.to_string(),
                aggregate: name.to_string(),
            })
        }
    })
}

// ─── MSUM warning ────────────────────────────────────────────────────────────

fn check_msum_warning(rule_name: &str, def: &RuleDefinition, warnings: &mut Vec<CompilerWarning>) {
    for_each_fold_call(&def.fold, |fold, name, args| {
        if name.to_uppercase() != "MSUM" {
            return;
        }
        let is_literal = args
            .first()
            .is_some_and(|arg| matches!(arg, Expr::Literal(_)));
        if !is_literal {
            warnings.push(CompilerWarning {
                code: WarningCode::MsumNonNegativity,
                message: format!(
                    "MSUM argument in fold '{}' may be negative; \
                     ensure non-negativity for convergence",
                    fold.name
                ),
                rule_name: rule_name.to_string(),
            });
        }
    });
}

// ─── MNOR/MPROD probability domain warning ───────────────────────────────────

fn check_probability_domain_warning(
    rule_name: &str,
    def: &RuleDefinition,
    warnings: &mut Vec<CompilerWarning>,
) {
    for_each_fold_call(&def.fold, |fold, name, args| {
        let upper = name.to_uppercase();
        if !matches!(upper.as_str(), "MNOR" | "MPROD") {
            return;
        }
        let is_literal = args
            .first()
            .is_some_and(|arg| matches!(arg, Expr::Literal(_)));
        if !is_literal {
            warnings.push(CompilerWarning {
                code: WarningCode::ProbabilityDomainViolation,
                message: format!(
                    "{upper} argument in fold '{}' may be outside [0,1]; \
                     ensure values are valid probabilities for convergence",
                    fold.name
                ),
                rule_name: rule_name.to_string(),
            });
        }
    });
}

// ─── IS NOT subject must be a node ─────────────────────────────────────────

/// Reject an `IS NOT` whose subject cannot carry node identity.
///
/// The anti-join keys on vids — `verify_key_columns_are_vids` in
/// `uni-query`'s `locy_complement.rs` enforces exactly this at runtime, with
/// "a scalar property cannot be used as an `IS NOT` subject". So every program
/// this rejects already fails; the only change is that it now fails at compile
/// time, before any evaluation, with a message that names the variable.
///
/// Two things this must NOT do, both of which would reject valid programs:
///
/// 1. **Never validate `is_ref.target`.** The shape
///    `WHERE d IS signal TO dis, d IS NOT known TO dis` binds `dis` through the
///    *positive* reference's TO-target, not through MATCH. It appears in 11
///    programs across the TCK, the Python binding tests and a published
///    notebook. Only `is_ref.subjects` is checked.
/// 2. **Accumulate bindings left to right.** A positive `x IS rule TO y` makes
///    `y` a node for *later* conditions (the planner emits a `ScanAll` for it),
///    so a pre-scan that ignored ordering would be wrong in the other
///    direction — accepting a subject bound only by a later condition.
///
/// Tuple subjects (`(a, b) IS NOT risk`) are checked element-wise.
///
/// A **YIELD alias of a node** counts as a node: `YIELD KEY a AS subj` makes
/// `subj` another name for the node `a`, and `WHERE subj IS NOT flagged`
/// resolves fine at runtime because the projected column still holds a vid.
/// Only aliases of non-node expressions (`YIELD a.n AS x`) are rejected. An
/// earlier version of this check ignored aliases entirely and false-rejected
/// two working programs.
fn check_is_not_subjects_are_nodes(
    rule_name: &str,
    clause: &CompiledClause,
) -> Result<(), LocyCompileError> {
    let mut bound_nodes = super::warded::extract_pattern_node_variables(&clause.match_pattern);

    // YIELD aliases that rename a node keep denoting that node.
    if let RuleOutput::Yield(yield_clause) = &clause.output {
        for item in &yield_clause.items {
            if let (Some(alias), Expr::Variable(var)) = (&item.alias, &item.expr)
                && bound_nodes.contains(var)
            {
                bound_nodes.insert(alias.clone());
            }
        }
    }

    for cond in &clause.where_conditions {
        let RuleCondition::IsReference(is_ref) = cond else {
            continue;
        };
        if is_ref.negated {
            for subject in &is_ref.subjects {
                if !bound_nodes.contains(subject) {
                    return Err(LocyCompileError::IsNotSubjectNotANode {
                        rule: rule_name.to_string(),
                        variable: subject.clone(),
                    });
                }
            }
        } else if let Some(target) = &is_ref.target {
            // A positive IS-ref's TO-target is materialized as a node, so it is
            // a valid subject for any *subsequent* condition.
            bound_nodes.insert(target.clone());
        }
    }
    Ok(())
}

// ─── Phase D F3 case 3: positive + complement on same subject ──────────────

fn check_positive_complement_pair(
    rule_name: &str,
    def: &RuleDefinition,
    warnings: &mut Vec<CompilerWarning>,
) {
    // Index IS-refs by their first subject variable. The first subject
    // is the canonical "head" — e.g. `s IS p` or `(s, t) IS p`.
    let mut by_subject: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
    for cond in &def.where_conditions {
        if let RuleCondition::IsReference(is_ref) = cond
            && let Some(subj) = is_ref.subjects.first()
        {
            let entry = by_subject.entry(subj.clone()).or_default();
            let target = is_ref.rule_name.to_string();
            if is_ref.negated {
                entry.1.push(target);
            } else {
                entry.0.push(target);
            }
        }
    }
    for (subj, (positives, negateds)) in by_subject {
        for p in &positives {
            for q in &negateds {
                if p == q {
                    continue;
                }
                warnings.push(CompilerWarning {
                    code: WarningCode::PositiveComplementCorrelation,
                    message: format!(
                        "rule '{rule_name}': WHERE {subj} IS {p}, {subj} IS NOT {q} — \
                         positive and complement on the same subject correlate when \
                         their support sets overlap (CrossGroupCorrelationNotExact). \
                         Use BDD/TopKProofs for exact composition, or accept the \
                         independence approximation.",
                    ),
                    rule_name: rule_name.to_string(),
                });
            }
        }
    }
}

// ─── Phase D F3 case 2: cross-predicate correlation ────────────────────────

fn check_cross_predicate_correlation(
    rule_name: &str,
    clause: &CompiledClause,
    compiled_rules: &HashMap<String, CompiledRule>,
    warnings: &mut Vec<CompilerWarning>,
) {
    // Index positive IS-refs by their first subject variable, recording
    // target rule names. Negated refs are case 3's concern, not this one.
    let mut by_subject: HashMap<String, Vec<String>> = HashMap::new();
    for cond in &clause.where_conditions {
        if let RuleCondition::IsReference(is_ref) = cond
            && !is_ref.negated
            && let Some(subj) = is_ref.subjects.first()
        {
            by_subject
                .entry(subj.clone())
                .or_default()
                .push(is_ref.rule_name.to_string());
        }
    }
    for (subj, mut targets) in by_subject {
        // Dedupe and keep only distinct targets.
        targets.sort();
        targets.dedup();
        if targets.len() < 2 {
            continue;
        }
        // Count PROB-bearing targets.
        let prob_targets: Vec<&str> = targets
            .iter()
            .filter(|t| {
                compiled_rules
                    .get(t.as_str())
                    .is_some_and(|r| r.yield_schema.iter().any(|c| c.is_prob))
            })
            .map(String::as_str)
            .collect();
        if prob_targets.len() < 2 {
            continue;
        }
        warnings.push(CompilerWarning {
            code: WarningCode::CrossPredicateCorrelation,
            message: format!(
                "rule '{rule_name}': WHERE {subj} IS {} (multiple PROB-bearing IS-refs \
                 on the same subject) — the implicit conjunction assumes \
                 independence between predicates, which is wrong when their \
                 support sets overlap. Use BDD/TopKProofs for exact composition, \
                 or accept the independence approximation.",
                prob_targets.join(", ")
            ),
            rule_name: rule_name.to_string(),
        });
    }
}

// ─── BEST BY + monotonic fold ────────────────────────────────────────────────

/// Reject `BEST BY` combined with a declared lattice fold.
///
/// `BEST BY` selects one witness row; a lattice fold aggregates across all of
/// them. Writing both in one rule is a semantic contradiction the user almost
/// certainly did not intend.
///
/// Membership comes from [`is_declared_lattice_fold`], **not** from the
/// injected [`MonotonicityOracle`] — see that function for why the two must not
/// be unified.
fn check_best_by_monotonic_fold(
    rule_name: &str,
    def: &RuleDefinition,
) -> Result<(), LocyCompileError> {
    if def.best_by.is_none() {
        return Ok(());
    }
    try_for_each_fold_call(&def.fold, |_fold, name, _args| {
        if is_declared_lattice_fold(name) {
            Err(LocyCompileError::BestByWithMonotonicFold {
                rule: rule_name.to_string(),
                fold: name.to_string(),
            })
        } else {
            Ok(())
        }
    })
}

// ─── F1: FOLD in recursive path without ALONG ───────────────────────────────

/// Phase B F1 (Stress Corpus B3): a clause has a FOLD aggregate AND
/// references a rule in its own SCC (recursive IS-ref). Aggregating inside a
/// recursive stratum currently **under-counts**: the semi-naive fixpoint
/// deduplicates intermediate rows by all columns, so two derivations that
/// reach the same KEY with numerically equal values collapse into one before
/// the fold observes them (<https://github.com/rustic-ai/uni-db/issues/159>).
///
/// This fires whether or not an `ALONG` clause is present. ALONG was
/// previously treated as a fix and suppressed the warning, but ALONG
/// accumulators are ordinary non-KEY columns and participate in the same
/// dedup — two distinct paths carrying an equal accumulated value collapse
/// exactly as FOLD inputs do. Suppressing on ALONG therefore silenced the
/// case the user had just been advised to write.
///
/// Conservative scope: only fires for self-SCC IS-refs (the common
/// recursive case). Cross-SCC recursion via non-recursive stratification
/// won't trigger it.
fn check_fold_in_recursive_path(
    rule_name: &str,
    def: &RuleDefinition,
    scc_rules: &std::collections::HashSet<String>,
    warnings: &mut Vec<CompilerWarning>,
) {
    if def.fold.is_empty() {
        return;
    }
    let has_recursive_is_ref = def.where_conditions.iter().any(|cond| {
        if let RuleCondition::IsReference(is_ref) = cond {
            scc_rules.contains(&is_ref.rule_name.to_string())
        } else {
            false
        }
    });
    if has_recursive_is_ref {
        warnings.push(CompilerWarning {
            code: WarningCode::FoldInRecursivePath,
            message: format!(
                "rule '{}' aggregates inside a recursive stratum. A \
                 self-reference reads the target's FOLDED value per KEY, so \
                 the rollup composes one level at a time: a parent folds its \
                 children's values, not the derivations behind them (issue \
                 #162). Reach for ALONG instead when you want a value \
                 accumulated along each PATH rather than a per-KEY rollup. \
                 Two cautions remain: an unbounded aggregate (MSUM, MCOUNT, \
                 COUNT, COLLECT) over CYCLIC data has no fixpoint and will run \
                 to the iteration limit rather than converge; and MNOR/MPROD \
                 assume derivations are independent, which is false when they \
                 share base facts — enable exact_probability for the BDD \
                 computation there. (Stress Corpus B3)",
                rule_name
            ),
            rule_name: rule_name.to_string(),
        });
    }
}

/// Warn when a post-FOLD `WHERE` sits on a self-referencing rule (#265).
///
/// The filter runs once, over the converged answer. A self-reference therefore
/// reads the rule's *unfiltered* folded value, and the rule can derive facts
/// from groups the threshold excluded — while those groups are correctly absent
/// from the output, which is what makes it hard to spot. The OFAC 50 %
/// ownership rule is the canonical shape: an owner under the threshold is
/// filtered out of the answer yet still qualifies everything downstream of it.
///
/// Deliberately a warning. The post-fixpoint reading is correct for the PROB
/// case of #162 — a child that HAVING removes from the answer must still have
/// been visible to its parent while the fixpoint ran — and "filter the answer"
/// and "the threshold is part of the definition" are written identically, so
/// nothing in the syntax says which was meant. Rejecting would refuse the
/// intended use along with the mistaken one.
///
/// Fires alongside [`WarningCode::FoldInRecursivePath`] rather than replacing
/// it: that one is about how a recursive rollup composes, this one about a
/// filter that does not constrain what it appears to.
fn check_having_in_recursive_path(
    rule_name: &str,
    def: &RuleDefinition,
    scc_rules: &std::collections::HashSet<String>,
    warnings: &mut Vec<CompilerWarning>,
) {
    if def.having.is_empty() || def.fold.is_empty() {
        return;
    }
    let has_recursive_is_ref = def.where_conditions.iter().any(|cond| {
        if let RuleCondition::IsReference(is_ref) = cond {
            scc_rules.contains(&is_ref.rule_name.to_string())
        } else {
            false
        }
    });
    if !has_recursive_is_ref {
        return;
    }
    warnings.push(CompilerWarning {
        code: WarningCode::HavingInRecursivePath,
        message: format!(
            "rule '{}' filters its FOLD result inside a recursive stratum. The \
             post-FOLD WHERE is applied ONCE to the converged answer, not per \
             iteration, so it does not constrain the recursion: the \
             self-reference reads this rule's UNFILTERED folded value, and \
             rows the threshold excludes can still derive further rows. They \
             are absent from the output while having contributed to it, which \
             is why the result looks plausible. This is the intended reading \
             when the filter is meant to select what you see (issue #162). If \
             the threshold is meant to be part of the rule's DEFINITION — an \
             ownership or voting-control cutoff, a quorum, a cost ceiling — \
             write REQUIRE instead of the post-FOLD WHERE: it is applied to \
             every iteration, so it constrains what the recursion derives. \
             (issue #265)",
            rule_name
        ),
        rule_name: rule_name.to_string(),
    });
}

/// Reject a `REQUIRE` whose threshold could turn back off (#265).
///
/// `REQUIRE` is applied to each iteration's folded snapshot, so it constrains
/// what the recursion derives. That is sound only while the predicate can move
/// in one direction: a **lower** bound over a non-decreasing fold, or an
/// **upper** bound over a non-increasing one. The constrained operator is then
/// still monotone and its least fixpoint exists.
///
/// The reverse pairings must be rejected here rather than warned about,
/// because the runtime cannot recover from them. The fixpoint's change test
/// compares whole *contribution* rows, not the folded view, so a group leaving
/// the snapshot is invisible to the producing rule and reaches consumers as a
/// row set that flips back and forth — which reads as progress, not as
/// oscillation. Such a program would spin to `max_iterations` and return
/// partial results.
///
/// Conservative by construction: anything whose monotonicity this cannot
/// *prove* is refused, including an aggregate that declines to declare a
/// direction. A predicate mentioning no FOLD output at all is constant across
/// iterations and is admitted.
fn check_require_direction(
    rule_name: &str,
    def: &RuleDefinition,
    direction_of: DirectionOracle<'_>,
) -> Result<(), LocyCompileError> {
    if def.require.is_empty() {
        return Ok(());
    }
    let reject = |detail: String| {
        Err(LocyCompileError::NonMonotonicFilterInRecursion {
            rule: rule_name.to_string(),
            detail,
        })
    };

    for predicate in &def.require {
        let Expr::BinaryOp { left, op, right } = predicate else {
            return reject(format!(
                "`{predicate:?}` is not a comparison against a fold output"
            ));
        };
        // Which side names a FOLD output decides which way the bound points:
        // `agg >= 50` is a lower bound, `50 <= agg` is the same bound written
        // backwards.
        let left_fold = fold_output_named(def, left);
        let right_fold = fold_output_named(def, right);
        let (fold, lower_bound) = match (left_fold, right_fold) {
            (Some(_), Some(_)) => {
                return reject(
                    "both sides name a fold output, so neither bound is fixed".to_string(),
                );
            }
            // No fold output mentioned: constant across iterations, so it can
            // never flip. Admitted.
            (None, None) => continue,
            (Some(fold), None) => match op {
                BinaryOp::Gt | BinaryOp::GtEq => (fold, true),
                BinaryOp::Lt | BinaryOp::LtEq => (fold, false),
                _ => {
                    return reject(format!(
                        "`{}` is compared with an operator that is not an \
                         inequality; equality and inequality can both turn back off",
                        fold.name
                    ));
                }
            },
            (None, Some(fold)) => match op {
                BinaryOp::Lt | BinaryOp::LtEq => (fold, true),
                BinaryOp::Gt | BinaryOp::GtEq => (fold, false),
                _ => {
                    return reject(format!(
                        "`{}` is compared with an operator that is not an \
                         inequality; equality and inequality can both turn back off",
                        fold.name
                    ));
                }
            },
        };

        let Expr::FunctionCall { name, .. } = &fold.aggregate else {
            return reject(format!("`{}` is not bound by an aggregate call", fold.name));
        };
        let direction = direction_of(name);
        if !direction.admits_bound(lower_bound) {
            let bound = if lower_bound { "lower" } else { "upper" };
            return reject(match direction {
                FoldDirection::Unknown => format!(
                    "`{}` declares no direction, so a {bound} bound on it \
                     cannot be shown to be one-way",
                    name.to_uppercase()
                ),
                FoldDirection::NonDecreasing => format!(
                    "`{}` is non-decreasing, so an upper bound on `{}` can turn \
                     from true back to false",
                    name.to_uppercase(),
                    fold.name
                ),
                FoldDirection::NonIncreasing => format!(
                    "`{}` is non-increasing, so a lower bound on `{}` can turn \
                     from true back to false",
                    name.to_uppercase(),
                    fold.name
                ),
                _ => format!("`{}` cannot carry a {bound} bound", name.to_uppercase()),
            });
        }
    }
    Ok(())
}

/// The FOLD binding an expression names, if the expression is exactly that
/// output column.
fn fold_output_named<'a>(def: &'a RuleDefinition, expr: &Expr) -> Option<&'a FoldBinding> {
    let Expr::Variable(name) = expr else {
        return None;
    };
    def.fold.iter().find(|f| &f.name == name)
}

// âââ Model invocation validation â
// ─── Model invocation validation ─────────────────────────────────────────────

/// Walk a clause body's expressions and validate any `model_name(args...)`
/// function-call that resolves to a Phase-B model in `model_catalog`. We
/// only flag KNOWN model names — bare function calls that don't match a
/// declared model are treated as built-ins (handled elsewhere) so that an
/// undeclared model name doesn't poison every function-call site.
///
/// Validations performed here:
///   * Arity: number of args must equal `model.inputs.len()`.
///
/// Output-type mismatch is intentionally deferred until the rule-output
/// surface is known (Phase B Slice 3 wires PROB column inference to model
/// output type). Unknown-model errors fire only when the caller writes
/// the call as `model_name(...)` AND the name shadows a declared model
/// that doesn't exist yet — currently surfaced via the rule's normal
/// `UndefinedRule` path.
fn check_model_invocations(
    rule_name: &str,
    def: &RuleDefinition,
    model_catalog: &HashMap<String, crate::types::CompiledModel>,
    warnings: &mut Vec<CompilerWarning>,
) -> Result<(), LocyCompileError> {
    if model_catalog.is_empty() {
        return Ok(());
    }
    // Track (rule, model) pairs we've already warned about so a model
    // invoked from multiple sites in the same rule warns just once.
    let mut warned: HashSet<String> = HashSet::new();
    // Helper closure: validates arity AND emits C4 warning.
    let mut visit = |expr: &Expr| -> Result<(), LocyCompileError> {
        walk_function_calls(expr, &mut |name, arg_count| {
            let Some(model) = model_catalog.get(name) else {
                return Ok(());
            };
            // Arity check.
            let expected = model.inputs.len();
            if arg_count != expected {
                return Err(LocyCompileError::ModelArityMismatch {
                    name: name.to_string(),
                    rule: rule_name.to_string(),
                    expected,
                    actual: arg_count,
                });
            }
            // C4: emit `UncalibratedNeuralPredicate` for PROB models
            // without an active calibration declaration.
            if model.output_type == uni_cypher::locy_ast::OutputType::Prob
                && matches!(
                    model.calibration,
                    None | Some(uni_cypher::locy_ast::CalibrationMethod::None)
                )
                && !warned.contains(&model.name)
            {
                warnings.push(CompilerWarning {
                    code: WarningCode::UncalibratedNeuralPredicate,
                    message: format!(
                        "rule '{}' invokes neural model '{}' (PROB output) with no \
                         CALIBRATION; downstream MNOR/MPROD/complement compound any \
                         miscalibration. Run `CALIBRATE {} ON MATCH ... TARGET ... \
                         METHOD platt_scaling` to fit a transform, or acknowledge the \
                         risk with `CALIBRATION none` in the model declaration",
                        rule_name, model.name, model.name
                    ),
                    rule_name: rule_name.to_string(),
                });
                warned.insert(model.name.clone());
            }
            Ok(())
        })
    };
    for cond in &def.where_conditions {
        if let RuleCondition::Expression(e) = cond {
            visit(e)?;
        }
    }
    for fold in &def.fold {
        visit(&fold.aggregate)?;
    }
    // ALONG and HAVING can also carry model invocations, and InvocationLifter
    // lifts from them — so their arity/type must be checked here too.
    for binding in &def.along {
        if let LocyExpr::Cypher(e) = &binding.expr {
            visit(e)?;
        }
    }
    for e in &def.having {
        visit(e)?;
    }
    if let RuleOutput::Yield(yc) = &def.output {
        for item in &yc.items {
            visit(&item.expr)?;
        }
    }
    // Phase B follow-up (Slice 7): with arity validation done above,
    // reject any *valid-arity* WHERE-position model invocations with
    // a clear error. Runtime support requires splitting `body_logical`
    // at the planner level into pre-filter (MATCH+projection+invocation)
    // and post-filter (WHERE+YIELD) halves so the classifier can run
    // between them. Until that lands, direct users to lift the call
    // into YIELD where the current invocation machinery handles it.
    for cond in &def.where_conditions {
        if let RuleCondition::Expression(e) = cond {
            let mut found_model: Option<String> = None;
            walk_function_calls(e, &mut |name, _arg_count| {
                if found_model.is_none() && model_catalog.contains_key(name) {
                    found_model = Some(name.to_string());
                }
                Ok(())
            })?;
            if let Some(model) = found_model {
                return Err(LocyCompileError::WhereModelInvocationNotYetSupported {
                    rule: rule_name.to_string(),
                    model,
                });
            }
        }
    }
    Ok(())
}

/// Visit every `Expr::FunctionCall` sub-node, calling `f(name, arg_count)`.
/// Phase C F2: emit `SharedNeuralInputArgument` (F2a) and
/// `SharedNeuralFeatureValue` (F2b) warnings when multiple
/// neural-model invocations in the same rule share an input
/// variable or an equivalent feature expression. Suppressed when
/// every invocation involved carries the `@independent` annotation
/// on its `CREATE MODEL` declaration.
///
/// Pattern modelled on `check_fold_in_recursive_path` — pushes
/// directly to the per-rule warnings vec.
fn check_shared_neural_inputs(
    rule_name: &str,
    def: &RuleDefinition,
    model_catalog: &HashMap<String, CompiledModel>,
    warnings: &mut Vec<CompilerWarning>,
) {
    if model_catalog.is_empty() {
        return;
    }
    // Collect (model_name, feature_expr) pairs from every model
    // invocation in this rule. F3-AlongFoldExtract: walk YIELD, ALONG,
    // and FOLD positions. ALONG carries a `LocyExpr` (Cypher + prev
    // refs); FOLD carries an `Expr` (the aggregate, e.g. `MNOR(scorer(s))`).
    let mut invocations: Vec<(&str, &Vec<Expr>)> = Vec::new();
    fn collect_from_cypher_expr<'a>(
        expr: &'a Expr,
        model_catalog: &HashMap<String, CompiledModel>,
        out: &mut Vec<(&'a str, &'a Vec<Expr>)>,
    ) {
        if let Expr::FunctionCall { name, args, .. } = expr {
            if let Some(model) = model_catalog.get(name)
                && args.len() == model.inputs.len()
            {
                out.push((name.as_str(), args));
            }
            // Recurse into args (composed model calls inside
            // aggregates like `MNOR(scorer(s))`).
            for a in args {
                collect_from_cypher_expr(a, model_catalog, out);
            }
        }
    }
    fn collect_from_locy_expr<'a>(
        lexpr: &'a uni_cypher::locy_ast::LocyExpr,
        model_catalog: &HashMap<String, CompiledModel>,
        out: &mut Vec<(&'a str, &'a Vec<Expr>)>,
    ) {
        use uni_cypher::locy_ast::LocyExpr;
        match lexpr {
            LocyExpr::Cypher(e) => collect_from_cypher_expr(e, model_catalog, out),
            LocyExpr::BinaryOp { left, right, .. } => {
                collect_from_locy_expr(left, model_catalog, out);
                collect_from_locy_expr(right, model_catalog, out);
            }
            LocyExpr::UnaryOp(_, inner) => collect_from_locy_expr(inner, model_catalog, out),
            LocyExpr::PrevRef(_) => {}
        }
    }
    if let RuleOutput::Yield(yc) = &def.output {
        for item in &yc.items {
            collect_from_cypher_expr(&item.expr, model_catalog, &mut invocations);
        }
    }
    for along in &def.along {
        collect_from_locy_expr(&along.expr, model_catalog, &mut invocations);
    }
    for fold in &def.fold {
        collect_from_cypher_expr(&fold.aggregate, model_catalog, &mut invocations);
    }
    if invocations.len() < 2 {
        return;
    }
    let all_independent = |models: &[&str]| -> bool {
        models.iter().all(|m| {
            model_catalog
                .get(*m)
                .is_some_and(|cm| cm.annotations.independent)
        })
    };
    // Sort + dedup a group's models. Returns the unique set only when
    // ≥ 2 distinct non-independent models share the group (i.e. a
    // warning is warranted); `None` means no warning to emit.
    let group_to_warn = |models: &[&str]| -> Option<Vec<String>> {
        let mut unique: Vec<&str> = models.to_vec();
        unique.sort();
        unique.dedup();
        if unique.len() >= 2 && !all_independent(&unique) {
            Some(unique.into_iter().map(str::to_string).collect())
        } else {
            None
        }
    };
    // ── F2a: group by shared input-variable name ────────────────
    let mut by_var: HashMap<String, Vec<&str>> = HashMap::new();
    for (model, args) in &invocations {
        for a in args.iter() {
            if let Expr::Variable(v) = a {
                by_var.entry(v.clone()).or_default().push(model);
            }
        }
    }
    let mut warned_a: HashSet<String> = HashSet::new();
    for (var, models) in &by_var {
        if let Some(unique) = group_to_warn(models)
            && warned_a.insert(var.clone())
        {
            warnings.push(CompilerWarning {
                code: WarningCode::SharedNeuralInputArgument,
                message: format!(
                    "rule '{}' invokes multiple neural models \
                     ({}) on the same input variable '{}'; under \
                     independence-by-default the composed \
                     probability assumes independence which is \
                     likely wrong (rollout D-8). Either annotate \
                     the models with `@independent` (if you have \
                     evidence they're conditionally independent \
                     given upstream context), or use \
                     `CALIBRATE` / TopKProofs for honest \
                     composition.",
                    rule_name,
                    unique.join(", "),
                    var
                ),
                rule_name: rule_name.to_string(),
            });
        }
    }
    // ── F2b: group by structural equality of NON-Variable feature exprs ──
    let mut by_expr: HashMap<String, Vec<&str>> = HashMap::new();
    for (model, args) in &invocations {
        for a in args.iter() {
            // Skip plain variables — F2a covers those.
            if matches!(a, Expr::Variable(_)) {
                continue;
            }
            let key = format!("{:?}", a);
            by_expr.entry(key).or_default().push(model);
        }
    }
    for models in by_expr.values() {
        if let Some(unique) = group_to_warn(models) {
            warnings.push(CompilerWarning {
                code: WarningCode::SharedNeuralFeatureValue,
                message: format!(
                    "rule '{}' invokes multiple neural models \
                     ({}) on an equivalent feature expression; \
                     even with distinct binding variables the \
                     probabilities share a common input value and \
                     cannot be composed under independence \
                     (rollout D-8). Annotate `@independent` or \
                     use TopKProofs for honest composition.",
                    rule_name,
                    unique.join(", ")
                ),
                rule_name: rule_name.to_string(),
            });
        }
    }
    // ── F2c (F3 case 4): group by retrieval-feature property path ──
    // Catches `similar_to(prop, _)` / `semantic_match(prop, _)` calls
    // that share the same `Property(Variable(v), prop)` operand across
    // ≥ 2 distinct model invocations. Suppressed by `@independent`.
    let mut by_retrieval_prop: HashMap<(String, String), Vec<&str>> = HashMap::new();
    for (model, args) in &invocations {
        for a in args.iter() {
            if let Expr::FunctionCall {
                name: fname,
                args: inner,
                ..
            } = a
                && matches!(fname.as_str(), "similar_to" | "semantic_match")
                && let Some(first) = inner.first()
                && let Expr::Property(boxed, prop) = first
                && let Expr::Variable(v) = boxed.as_ref()
            {
                by_retrieval_prop
                    .entry((v.clone(), prop.clone()))
                    .or_default()
                    .push(model);
            }
        }
    }
    for ((v, prop), models) in &by_retrieval_prop {
        if let Some(unique) = group_to_warn(models) {
            warnings.push(CompilerWarning {
                code: WarningCode::SharedRetrievalContext,
                message: format!(
                    "rule '{rule_name}' invokes multiple neural models ({}) \
                     whose features retrieve from the same property '{v}.{prop}'; \
                     independence between models is unlikely when both condition \
                     on the same retrieval evidence. Annotate `@independent` or \
                     use TopKProofs for honest composition.",
                    unique.join(", ")
                ),
                rule_name: rule_name.to_string(),
            });
        }
    }
}

fn walk_function_calls<F>(expr: &Expr, f: &mut F) -> Result<(), LocyCompileError>
where
    F: FnMut(&str, usize) -> Result<(), LocyCompileError>,
{
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            f(name, args.len())?;
            for a in args {
                walk_function_calls(a, f)?;
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_function_calls(left, f)?;
            walk_function_calls(right, f)
        }
        Expr::UnaryOp { expr: inner, .. } => walk_function_calls(inner, f),
        Expr::List(items) => {
            for i in items {
                walk_function_calls(i, f)?;
            }
            Ok(())
        }
        Expr::Map(entries) => {
            for (_, v) in entries {
                walk_function_calls(v, f)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

// ─── Slice 3: model invocation extraction + YIELD rewriting ─────────────────

/// Phase B Slice 3: lift neural-model calls out of YIELD items.
///
/// For each top-level `YIELD ... model_name(args) [AS alias]` where
/// `model_name` is declared in `model_catalog`, this emits a
/// [`ModelInvocation`] entry whose `output_column` matches the YIELD
/// item's resolved column name, and rewrites the YIELD item's expression
/// to a placeholder literal `0.0`. At runtime, the body projection still
/// materializes a column with that name (initially zero); the
/// invocation pass then **overwrites** that column with the classifier's
/// per-row output before any downstream operator (FOLD, IS-ref) reads
/// from it.
///
/// Why a literal placeholder rather than a synthetic column? The body's
/// projection is built by the planner from the (rewritten) YIELD items
/// alone — there's no plan-node insertion point between projection and
/// downstream operators where a brand-new column could be threaded in
/// without restructuring the planner. Overwriting an existing column
/// keeps the diff to the runtime alone.
///
/// Slice 3 limits the extraction to direct top-level model calls in
/// YIELD items (the common case from DEEP_LOCY.md §9.4). Nested calls
/// (`f(model_x(s))`, `model_x(s) + 1`), invocations in WHERE, FOLD, or
/// ALONG are not lifted in this slice — they parse and validate (arity)
/// but won't execute at runtime. Follow-up slices extend the lift.
pub(crate) struct ExtractedInvocations {
    pub output: RuleOutput,
    pub along: Vec<AlongBinding>,
    pub fold: Vec<FoldBinding>,
    pub invocations: Vec<ModelInvocation>,
    pub hidden_yield_cols: Vec<String>,
}

/// Accumulator state for `extract_model_invocations`. Walks YIELD,
/// ALONG, and FOLD positions of a clause body; whenever a known
/// model FunctionCall is encountered, lifts it into a fresh
/// `ModelInvocation` and replaces the call site with a
/// `Variable("__model_<name>_<idx>")` reference. The runtime
/// `LocyModelInvokeExec` (planner-inserted between the body and
/// `LocyProject`) materializes the column.
struct InvocationLifter<'a> {
    rule_name: &'a str,
    model_catalog: &'a HashMap<String, CompiledModel>,
    invocations: Vec<ModelInvocation>,
    /// Pairs of `(__feat_<var>_<prop>, Property(Variable(var), prop))`
    /// expressions to emit as hidden YIELD items. Deduplicated by
    /// column name via `seen_hidden`.
    hidden_items: Vec<(String, Expr)>,
    seen_hidden: std::collections::HashSet<String>,
    counter: usize,
}

/// Return type of `InvocationLifter::validate_features`: rewritten feature
/// argument list plus the `(var, property)` pairs accumulated for hidden-YIELD
/// materialization.
type ValidatedFeatures = Result<(Vec<Expr>, Vec<(String, String)>), LocyCompileError>;

impl<'a> InvocationLifter<'a> {
    fn new(rule_name: &'a str, model_catalog: &'a HashMap<String, CompiledModel>) -> Self {
        Self {
            rule_name,
            model_catalog,
            invocations: Vec::new(),
            hidden_items: Vec::new(),
            seen_hidden: std::collections::HashSet::new(),
            counter: 0,
        }
    }

    /// Record a `var.prop` feature reference: push it onto
    /// `refs` for the invocation record and emit the matching hidden
    /// YIELD item (`__feat_<var>_<prop>`) so the property is
    /// materialized into the per-row fact_row, deduping by column name.
    fn register_property_feature(&mut self, v: &str, prop: &str, refs: &mut Vec<(String, String)>) {
        refs.push((v.to_string(), prop.to_string()));
        let col_name = format!("__feat_{}_{}", v, prop);
        if self.seen_hidden.insert(col_name.clone()) {
            let hidden_expr =
                Expr::Property(Box::new(Expr::Variable(v.to_string())), prop.to_string());
            self.hidden_items.push((col_name, hidden_expr));
        }
    }

    /// Validate feature expressions and emit hidden YIELD items for
    /// shapes that require pre-materialization (`Property(Variable,
    /// prop)` for graph properties; `similar_to(...)` /
    /// `semantic_match(...)` for retrieval-backed features). Returns
    /// the possibly-rewritten feature_exprs and the per-invocation
    /// `feature_property_refs` for record-keeping.
    fn validate_features(&mut self, model_name: &str, args: &[Expr]) -> ValidatedFeatures {
        let mut feature_property_refs = Vec::new();
        let mut rewritten = Vec::with_capacity(args.len());
        for fexpr in args {
            match fexpr {
                Expr::Variable(_) => {
                    rewritten.push(fexpr.clone());
                }
                Expr::Property(boxed_inner, prop)
                    if matches!(boxed_inner.as_ref(), Expr::Variable(_)) =>
                {
                    if let Expr::Variable(v) = boxed_inner.as_ref() {
                        self.register_property_feature(v, prop, &mut feature_property_refs);
                    }
                    rewritten.push(fexpr.clone());
                }
                Expr::FunctionCall {
                    name, args: fargs, ..
                } if matches!(
                    name.as_str(),
                    "avg_neighbor" | "max_neighbor" | "sum_neighbor"
                ) =>
                {
                    // Phase D D1 graph-structural: one-hop neighborhood
                    // aggregators. Args: subject Variable, rel-type
                    // string literal, property string literal, and an
                    // optional 4th direction string literal
                    // ('OUTGOING' | 'INCOMING' | 'BOTH'; default
                    // 'OUTGOING').
                    if fargs.len() != 3 && fargs.len() != 4 {
                        return Err(LocyCompileError::UnsupportedFeatureExpression {
                            rule: self.rule_name.to_string(),
                            model: model_name.to_string(),
                            expr: format!(
                                "{}(...) requires 3 or 4 arguments (subject, 'REL_TYPE', 'property' [, 'OUTGOING' | 'INCOMING' | 'BOTH']), got {}",
                                name,
                                fargs.len()
                            ),
                        });
                    }
                    match &fargs[0] {
                        Expr::Variable(_) => {}
                        other => {
                            return Err(LocyCompileError::UnsupportedFeatureExpression {
                                rule: self.rule_name.to_string(),
                                model: model_name.to_string(),
                                expr: format!(
                                    "{}(...) first argument must be a node variable, got {other:?}",
                                    name
                                ),
                            });
                        }
                    }
                    for (i, inner) in fargs.iter().enumerate().skip(1) {
                        let is_string_literal = matches!(
                            inner,
                            Expr::Literal(uni_cypher::ast::CypherLiteral::String(_))
                        );
                        if !is_string_literal {
                            return Err(LocyCompileError::UnsupportedFeatureExpression {
                                rule: self.rule_name.to_string(),
                                model: model_name.to_string(),
                                expr: format!(
                                    "{}(...) argument {} must be a string literal, got {inner:?}",
                                    name,
                                    i + 1
                                ),
                            });
                        }
                    }
                    // If a direction was supplied, validate its value.
                    if let Some(Expr::Literal(uni_cypher::ast::CypherLiteral::String(dir))) =
                        fargs.get(3)
                    {
                        let upper = dir.to_uppercase();
                        if !matches!(upper.as_str(), "OUTGOING" | "INCOMING" | "BOTH") {
                            return Err(LocyCompileError::UnsupportedFeatureExpression {
                                rule: self.rule_name.to_string(),
                                model: model_name.to_string(),
                                expr: format!(
                                    "{}(...) 4th argument (direction) must be 'OUTGOING', 'INCOMING', or 'BOTH'; got '{dir}'",
                                    name
                                ),
                            });
                        }
                    }
                    rewritten.push(fexpr.clone());
                }
                Expr::FunctionCall {
                    name, args: fargs, ..
                } if matches!(
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
                    // Phase D D1 graph-structural: single-arg topology
                    // features. Arg must be `Expr::Variable(_)` — the
                    // subject binding whose `_vid` is materialized into
                    // the per-row fact_row via the hidden YIELD pipeline.
                    if fargs.len() != 1 {
                        return Err(LocyCompileError::UnsupportedFeatureExpression {
                            rule: self.rule_name.to_string(),
                            model: model_name.to_string(),
                            expr: format!(
                                "{}(...) requires exactly 1 argument, got {}",
                                name,
                                fargs.len()
                            ),
                        });
                    }
                    match &fargs[0] {
                        Expr::Variable(_) => {}
                        other => {
                            return Err(LocyCompileError::UnsupportedFeatureExpression {
                                rule: self.rule_name.to_string(),
                                model: model_name.to_string(),
                                expr: format!(
                                    "{}(...) argument must be a node variable, got {other:?}",
                                    name
                                ),
                            });
                        }
                    }
                    rewritten.push(fexpr.clone());
                }
                Expr::FunctionCall {
                    name, args: fargs, ..
                } if matches!(name.as_str(), "similar_to" | "semantic_match") => {
                    // Phase D D1/D2: retrieval-backed feature expressions.
                    // Both functions take exactly 2 args. Property-access
                    // args register the (var, prop) pair so the standard
                    // hidden-YIELD pipeline materializes them into the
                    // per-row fact_row; the FunctionCall itself flows
                    // through `original_feature_exprs` and is evaluated
                    // per-row in `apply_model_invocations` /
                    // `eval_feature_expr_against_fact_row`.
                    if fargs.len() != 2 {
                        return Err(LocyCompileError::UnsupportedFeatureExpression {
                            rule: self.rule_name.to_string(),
                            model: model_name.to_string(),
                            expr: format!(
                                "{}(...) requires exactly 2 arguments, got {}",
                                name,
                                fargs.len()
                            ),
                        });
                    }
                    for inner in fargs {
                        match inner {
                            Expr::Variable(_) | Expr::Literal(_) | Expr::List(_) => {}
                            Expr::Property(boxed_inner, prop)
                                if matches!(boxed_inner.as_ref(), Expr::Variable(_)) =>
                            {
                                if let Expr::Variable(v) = boxed_inner.as_ref() {
                                    self.register_property_feature(
                                        v,
                                        prop,
                                        &mut feature_property_refs,
                                    );
                                }
                            }
                            other => {
                                return Err(LocyCompileError::UnsupportedFeatureExpression {
                                    rule: self.rule_name.to_string(),
                                    model: model_name.to_string(),
                                    expr: format!(
                                        "{}(...) argument must be Variable, Property, or Literal — got {other:?}",
                                        name
                                    ),
                                });
                            }
                        }
                    }
                    rewritten.push(fexpr.clone());
                }
                other => {
                    return Err(LocyCompileError::UnsupportedFeatureExpression {
                        rule: self.rule_name.to_string(),
                        model: model_name.to_string(),
                        expr: format!("{other:?}"),
                    });
                }
            }
        }
        Ok((rewritten, feature_property_refs))
    }

    /// Lift any model FunctionCall in `expr` (recursively) into the
    /// invocations accumulator, returning an Expr with each call site
    /// replaced by `Variable("__model_<name>_<idx>")`.
    fn lift_expr(&mut self, expr: &Expr) -> Result<Expr, LocyCompileError> {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } if self.model_catalog.contains_key(name) => {
                let model = &self.model_catalog[name];
                if args.len() != model.inputs.len() {
                    // Arity already validated by check_model_invocations;
                    // pass through unchanged so the existing error path
                    // surfaces it.
                    return Ok(expr.clone());
                }
                let synthetic = format!("__model_{}_{}", name, self.counter);
                self.counter += 1;
                // Phase C B1-B3 follow-up: capture the pre-rewrite
                // feature args BEFORE validate_features mutates
                // them. EXPLAIN uses these to rebuild ClassifyInput
                // per fact (the rewritten copy carries synthetic
                // column references that don't evaluate against a
                // post-projection fact_row).
                let original_feature_exprs = args.clone();
                let (rewritten_feature_exprs, feature_property_refs) =
                    self.validate_features(name, args)?;
                let feature_names: Vec<String> =
                    model.inputs.iter().map(|b| b.variable.clone()).collect();
                self.invocations.push(ModelInvocation {
                    model_name: name.clone(),
                    output_column: synthetic.clone(),
                    feature_exprs: rewritten_feature_exprs,
                    feature_names,
                    feature_property_refs,
                    // Filled in by the YIELD-item walk in
                    // extract_model_invocations after lift_expr
                    // returns — the caller knows whether the
                    // invocation came from a YIELD position
                    // (alias-bearing) or ALONG/FOLD (no alias).
                    yield_alias: None,
                    original_feature_exprs,
                    path_context: model.path_context.clone(),
                    embedder_alias: model.embedder_alias.clone(),
                });
                let _ = (distinct, window_spec);
                Ok(Expr::Variable(synthetic))
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => {
                let new_args = args
                    .iter()
                    .map(|a| self.lift_expr(a))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Expr::FunctionCall {
                    name: name.clone(),
                    args: new_args,
                    distinct: *distinct,
                    window_spec: window_spec.clone(),
                })
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.lift_expr(left)?),
                op: *op,
                right: Box::new(self.lift_expr(right)?),
            }),
            Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.lift_expr(inner)?),
            }),
            Expr::List(items) => Ok(Expr::List(
                items
                    .iter()
                    .map(|e| self.lift_expr(e))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Expr::Map(entries) => Ok(Expr::Map(
                entries
                    .iter()
                    .map(|(k, v)| self.lift_expr(v).map(|nv| (k.clone(), nv)))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            // Leaf or non-recursive shapes — no model call can hide.
            _ => Ok(expr.clone()),
        }
    }

    fn lift_locy_expr(&mut self, expr: &LocyExpr) -> Result<LocyExpr, LocyCompileError> {
        match expr {
            LocyExpr::Cypher(e) => Ok(LocyExpr::Cypher(self.lift_expr(e)?)),
            LocyExpr::BinaryOp { left, op, right } => Ok(LocyExpr::BinaryOp {
                left: Box::new(self.lift_locy_expr(left)?),
                op: *op,
                right: Box::new(self.lift_locy_expr(right)?),
            }),
            LocyExpr::UnaryOp(op, inner) => Ok(LocyExpr::UnaryOp(
                *op,
                Box::new(self.lift_locy_expr(inner)?),
            )),
            LocyExpr::PrevRef(_) => Ok(expr.clone()),
        }
    }
}

fn extract_model_invocations(
    rule_name: &str,
    def: &RuleDefinition,
    model_catalog: &HashMap<String, CompiledModel>,
) -> Result<ExtractedInvocations, LocyCompileError> {
    let mut lifter = InvocationLifter::new(rule_name, model_catalog);

    // ── YIELD position ──────────────────────────────────────────
    let new_output = match &def.output {
        RuleOutput::Yield(yc) => {
            let mut new_items = Vec::with_capacity(yc.items.len());
            for item in &yc.items {
                let before = lifter.invocations.len();
                let new_expr = lifter.lift_expr(&item.expr)?;
                // Tag every invocation lifted from THIS YIELD item
                // with the item's user-visible alias so EXPLAIN can
                // look up the model output by the column name that
                // survives `LocyProject`'s projection.
                let yield_alias = item.alias.clone().or_else(|| match &new_expr {
                    Expr::Variable(n) => Some(n.clone()),
                    _ => None,
                });
                for inv in lifter.invocations[before..].iter_mut() {
                    inv.yield_alias = yield_alias.clone();
                }
                new_items.push(LocyYieldItem {
                    is_key: item.is_key,
                    is_prob: item.is_prob,
                    expr: new_expr,
                    alias: item.alias.clone(),
                });
            }
            RuleOutput::Yield(uni_cypher::locy_ast::YieldClause { items: new_items })
        }
        other => other.clone(),
    };

    // ── ALONG position ──────────────────────────────────────────
    let mut new_along = Vec::with_capacity(def.along.len());
    for binding in &def.along {
        new_along.push(AlongBinding {
            name: binding.name.clone(),
            expr: lifter.lift_locy_expr(&binding.expr)?,
        });
    }

    // ── FOLD position ───────────────────────────────────────────
    let mut new_fold = Vec::with_capacity(def.fold.len());
    for binding in &def.fold {
        new_fold.push(FoldBinding {
            name: binding.name.clone(),
            aggregate: lifter.lift_expr(&binding.aggregate)?,
        });
    }

    // ── Hidden YIELD items for property-feature refs ─────────────
    let mut hidden_yield_cols: Vec<String> = Vec::with_capacity(lifter.hidden_items.len());
    let new_output = match new_output {
        RuleOutput::Yield(mut yc) => {
            for (col_name, hidden_expr) in &lifter.hidden_items {
                yc.items.push(LocyYieldItem {
                    is_key: false,
                    is_prob: false,
                    expr: hidden_expr.clone(),
                    alias: Some(col_name.clone()),
                });
                hidden_yield_cols.push(col_name.clone());
            }
            RuleOutput::Yield(yc)
        }
        other => other,
    };

    Ok(ExtractedInvocations {
        output: new_output,
        along: new_along,
        fold: new_fold,
        invocations: lifter.invocations,
        hidden_yield_cols,
    })
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Iterate folds whose aggregate is a `FunctionCall`, yielding the function
/// name and argument slice. Folds whose aggregate is not a function call are
/// skipped — every caller of this module's checks treats non-call aggregates
/// as inert.
fn for_each_fold_call<'a>(
    folds: &'a [FoldBinding],
    mut visit: impl FnMut(&'a FoldBinding, &str, &'a [Expr]),
) {
    for fold in folds {
        if let Expr::FunctionCall { name, args, .. } = &fold.aggregate {
            visit(fold, name, args);
        }
    }
}

/// Error-returning variant of [`for_each_fold_call`] for checks that
/// short-circuit with a `LocyCompileError`.
fn try_for_each_fold_call<'a, E>(
    folds: &'a [FoldBinding],
    mut visit: impl FnMut(&'a FoldBinding, &str, &'a [Expr]) -> Result<(), E>,
) -> Result<(), E> {
    for fold in folds {
        if let Expr::FunctionCall { name, args, .. } = &fold.aggregate {
            visit(fold, name, args)?;
        }
    }
    Ok(())
}
