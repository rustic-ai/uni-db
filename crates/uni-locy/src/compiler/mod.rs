pub mod dependency;
pub mod errors;
pub mod models;
pub mod stratify;
pub mod typecheck;
pub mod warded;

pub mod modules;

use std::collections::HashMap;

use uni_cypher::locy_ast::{LocyProgram, LocyStatement, RuleDefinition};

use crate::config::LocyConfig;
use crate::types::{CompiledAssume, CompiledCommand, CompiledProgram, Stratum};
use errors::LocyCompileError;
pub use typecheck::{MonotonicityOracle, default_monotonicity_oracle};

/// Validate and stratify a parsed Locy program into a `CompiledProgram`.
///
/// Pipeline: group_rules → dependency graph → stratify → wardedness → typecheck → assemble.
/// Defaults to a config with `neural_predicates_preview = false` (use
/// [`compile_with_config`] to opt into the Phase B preview surface) and
/// [`default_monotonicity_oracle`] for the recursive-stratum FOLD check.
pub fn compile(program: &LocyProgram) -> Result<CompiledProgram, LocyCompileError> {
    compile_with_modules(program, &HashMap::new())
}

/// Compile with pre-registered external rule names.
///
/// Rules in `external_rules` are treated as valid targets for IS-ref and
/// QUERY references, even though they are not defined in this program.
/// Used by `LocyEngine::evaluate()` when a session-level rule registry
/// contains previously compiled rules.
pub fn compile_with_external_rules(
    program: &LocyProgram,
    external_rules: &[String],
) -> Result<CompiledProgram, LocyCompileError> {
    compile_with_context(
        program,
        &HashMap::new(),
        external_rules,
        false,
        &default_monotonicity_oracle,
    )
}

/// Compile with pre-registered external rule names AND a [`LocyConfig`].
///
/// The Phase B variant: surfaces the `neural_predicates_preview` flag
/// through registries that wouldn't otherwise see it. Other config
/// fields are ignored at compile time (they affect runtime only).
pub fn compile_with_external_rules_and_config(
    program: &LocyProgram,
    external_rules: &[String],
    config: &LocyConfig,
) -> Result<CompiledProgram, LocyCompileError> {
    compile_with_context(
        program,
        &HashMap::new(),
        external_rules,
        config.neural_predicates_preview,
        &default_monotonicity_oracle,
    )
}

/// Compile a Locy program with module resolution context.
///
/// `available_modules` maps module names to their exported rule names,
/// enabling MODULE/USE declarations to resolve rule references across modules.
/// When empty (e.g., from `compile()`), module resolution is a no-op.
pub fn compile_with_modules(
    program: &LocyProgram,
    available_modules: &HashMap<String, Vec<String>>,
) -> Result<CompiledProgram, LocyCompileError> {
    compile_with_context(
        program,
        available_modules,
        &[],
        false,
        &default_monotonicity_oracle,
    )
}

/// Phase B entry: compile with a [`LocyConfig`] so neural-predicate preview
/// gates can fire. Equivalent to [`compile_with_modules`] when the config
/// has `neural_predicates_preview = false`.
pub fn compile_with_config(
    program: &LocyProgram,
    config: &LocyConfig,
) -> Result<CompiledProgram, LocyCompileError> {
    compile_with_context(
        program,
        &HashMap::new(),
        &[],
        config.neural_predicates_preview,
        &default_monotonicity_oracle,
    )
}

/// Compile a Locy program with a host-supplied monotonicity oracle.
///
/// Hosts that hold a `uni_plugin::PluginRegistry` should pass a closure
/// that resolves aggregate names through the registry and reads
/// `Semilattice.monotone_join`, so user-registered aggregates participate
/// in the recursive-stratum FOLD check.
/// Compile with both a host-supplied monotonicity oracle and a [`LocyConfig`].
///
/// The combination every `uni` host entry point needs: hosts hold a
/// `PluginRegistry` *and* a config carrying the neural-predicate preview gate,
/// and neither [`compile_with_oracle`] nor [`compile_with_config`] offers both.
///
/// Deliberately a seventh shim rather than a `CompileOptions` struct.
/// [`MonotonicityOracle`] carries a lifetime parameter, so such a struct cannot
/// derive `Default` and would need a hand-written impl plus a borrowed empty
/// module map — real work for no call-site benefit while exactly one new
/// combination is required.
pub fn compile_with_oracle_and_config(
    program: &LocyProgram,
    available_modules: &HashMap<String, Vec<String>>,
    external_rules: &[String],
    config: &LocyConfig,
    is_monotonic: MonotonicityOracle<'_>,
) -> Result<CompiledProgram, LocyCompileError> {
    compile_with_context(
        program,
        available_modules,
        external_rules,
        config.neural_predicates_preview,
        is_monotonic,
    )
}

pub fn compile_with_oracle(
    program: &LocyProgram,
    available_modules: &HashMap<String, Vec<String>>,
    external_rules: &[String],
    is_monotonic: MonotonicityOracle<'_>,
) -> Result<CompiledProgram, LocyCompileError> {
    compile_with_context(
        program,
        available_modules,
        external_rules,
        false,
        is_monotonic,
    )
}

/// Compile a Locy program with module resolution and external rule context.
fn compile_with_context(
    program: &LocyProgram,
    available_modules: &HashMap<String, Vec<String>>,
    external_rules: &[String],
    neural_predicates_preview: bool,
    is_monotonic: MonotonicityOracle<'_>,
) -> Result<CompiledProgram, LocyCompileError> {
    let (model_catalog, mut model_warnings) =
        models::compile_models(program, neural_predicates_preview)?;
    let module_ctx = modules::resolve_modules(program, available_modules)?;
    let rule_groups = group_rules_with_context(program, &module_ctx);
    let mut rule_names: Vec<String> = rule_groups.keys().cloned().collect();
    // Include external (registered) rules in the valid-name set.
    rule_names.extend(external_rules.iter().cloned());

    if rule_groups.is_empty() {
        let mut extra_warnings: Vec<crate::types::CompilerWarning> = Vec::new();
        let empty_rule_catalog = HashMap::new();
        let commands = extract_commands(
            program,
            &rule_names,
            &module_ctx,
            &model_catalog,
            &empty_rule_catalog,
            neural_predicates_preview,
            &mut extra_warnings,
            is_monotonic,
        )?;
        model_warnings.extend(extra_warnings);
        return Ok(CompiledProgram {
            strata: Vec::new(),
            rule_catalog: HashMap::new(),
            model_catalog,
            warnings: model_warnings,
            commands,
        });
    }

    let dep_graph = dependency::build_dependency_graph_with_models(
        &rule_groups,
        &module_ctx,
        external_rules,
        &model_catalog,
    )?;
    let strat = stratify::stratify(&dep_graph)?;
    warded::check_wardedness(&rule_groups)?;
    let (compiled_rules, mut warnings) = typecheck::check(
        &rule_groups,
        &strat,
        &model_catalog,
        &module_ctx,
        is_monotonic,
    )?;
    // Carry model-compilation warnings (e.g. G1-lite UncalibratedLLMLogprobs)
    // into the final program. Append after typecheck so source order is
    // preserved for `models -> rules` warning streams.
    warnings.append(&mut model_warnings);

    // Assemble strata in topological order
    let mut strata = Vec::new();
    for &scc_idx in &strat.scc_order {
        let scc_rules: Vec<_> = strat.sccs[scc_idx]
            .iter()
            .filter_map(|name| compiled_rules.get(name).cloned())
            .collect();

        if scc_rules.is_empty() {
            continue;
        }

        let depends_on: Vec<usize> = strat.scc_depends_on[scc_idx].iter().copied().collect();

        strata.push(Stratum {
            id: scc_idx,
            rules: scc_rules,
            is_recursive: strat.is_recursive[scc_idx],
            depends_on,
        });
    }

    let mut extra_command_warnings: Vec<crate::types::CompilerWarning> = Vec::new();
    let commands = extract_commands(
        program,
        &rule_names,
        &module_ctx,
        &model_catalog,
        &compiled_rules,
        neural_predicates_preview,
        &mut extra_command_warnings,
        is_monotonic,
    )?;
    warnings.extend(extra_command_warnings);

    Ok(CompiledProgram {
        strata,
        rule_catalog: compiled_rules,
        model_catalog,
        warnings,
        commands,
    })
}

/// Extract non-rule statements as compiled commands, validating rule references.
/// Returns the commands and any extra warnings emitted by command
/// compilation (e.g., Phase C C4 `EceBinningBias`).
#[expect(
    clippy::too_many_arguments,
    reason = "Threads the compile context (rules, modules, catalogs, preview gate, warning sink) plus the caller's monotonicity oracle into command extraction; grouping into a struct would just move the argument list."
)]
fn extract_commands(
    program: &LocyProgram,
    defined_rules: &[String],
    module_ctx: &modules::ModuleContext,
    model_catalog: &HashMap<String, crate::types::CompiledModel>,
    rule_catalog: &HashMap<String, crate::types::CompiledRule>,
    neural_predicates_preview_flag: bool,
    extra_warnings: &mut Vec<crate::types::CompilerWarning>,
    // Threaded so an ASSUME body inherits the caller's oracle rather than
    // silently re-defaulting at the boundary.
    is_monotonic: MonotonicityOracle<'_>,
) -> Result<Vec<CompiledCommand>, LocyCompileError> {
    let validate_rule = |raw_name: &str| -> Result<(), LocyCompileError> {
        let resolved = modules::resolve_rule_name(module_ctx, raw_name);
        if !defined_rules.contains(&resolved) {
            return Err(LocyCompileError::UndefinedRule { name: resolved });
        }
        Ok(())
    };

    let mut commands = Vec::new();
    for stmt in &program.statements {
        match stmt {
            LocyStatement::Rule(_) => {} // handled by group_rules
            LocyStatement::GoalQuery(gq) => {
                validate_rule(&gq.rule_name.to_string())?;
                commands.push(CompiledCommand::GoalQuery(gq.clone()));
            }
            LocyStatement::ExplainRule(eq) => {
                validate_rule(&eq.rule_name.to_string())?;
                commands.push(CompiledCommand::ExplainRule(eq.clone()));
            }
            LocyStatement::AbduceQuery(aq) => {
                validate_rule(&aq.rule_name.to_string())?;
                commands.push(CompiledCommand::Abduce(aq.clone()));
            }
            LocyStatement::DeriveCommand(dc) => {
                validate_rule(&dc.rule_name.to_string())?;
                commands.push(CompiledCommand::DeriveCommand(dc.clone()));
            }
            LocyStatement::AssumeBlock(ab) => {
                // Compile the ASSUME body as a sub-program
                let body_program_ast = uni_cypher::locy_ast::LocyProgram {
                    module: None,
                    uses: vec![],
                    statements: ab.body.clone(),
                };
                let body_module_ctx = modules::ModuleContext::default();
                let body_rule_groups = group_rules(&body_program_ast);

                // The body compiles with no module of its own, so a reference
                // to an outer rule resolves bare (`adult`) while the enclosing
                // `MODULE m` catalogued that rule qualified (`m.adult`) -- and
                // the reference failed with `UndefinedRule`. Offer every outer
                // rule under both spellings: inside `MODULE m`, writing `adult`
                // *is* how you name `m.adult`.
                //
                // Deliberately NOT fixed by handing the body the outer module
                // context. That would also qualify the body's *own* rules
                // (`m.eligible`), while the runtime looks those up unqualified
                // straight from the AST -- `locy_assume.rs` and `locy_query.rs`
                // both do `program.rule_catalog.get(&rule_name)` -- so
                // `QUERY eligible` would compile and then fail at run time with
                // "rule not found". Aliasing leaves every catalog key and every
                // lookup exactly as it was.
                let outer_visible: Vec<String> = defined_rules
                    .iter()
                    .flat_map(|qualified| {
                        let bare = qualified.rsplit_once('.').map(|(_, bare)| bare.to_string());
                        std::iter::once(qualified.clone()).chain(bare)
                    })
                    .collect();
                let all_rule_names: Vec<String> = outer_visible
                    .iter()
                    .chain(body_rule_groups.keys())
                    .cloned()
                    .collect();
                let mut body_extra_warnings: Vec<crate::types::CompilerWarning> = Vec::new();
                let body_commands = extract_commands(
                    &body_program_ast,
                    &all_rule_names,
                    &body_module_ctx,
                    model_catalog,
                    rule_catalog,
                    neural_predicates_preview_flag,
                    &mut body_extra_warnings,
                    is_monotonic,
                )?;
                extra_warnings.extend(body_extra_warnings);

                // Compile body rules if any exist. Thread the outer context —
                // the enclosing program's rule names (so the body can IS-ref
                // them) and the neural-predicates-preview flag — instead of a
                // bare `compile()` that drops both.
                let body_compiled = if !body_rule_groups.is_empty() {
                    compile_with_context(
                        &body_program_ast,
                        &HashMap::new(),
                        &outer_visible,
                        neural_predicates_preview_flag,
                        // Inherit the caller's oracle. Re-defaulting here would
                        // silently stop a host-supplied oracle at the ASSUME
                        // boundary, so a plugin aggregate legal in the outer
                        // program would be rejected inside an ASSUME body.
                        is_monotonic,
                    )?
                } else {
                    CompiledProgram {
                        strata: Vec::new(),
                        rule_catalog: HashMap::new(),
                        model_catalog: HashMap::new(),
                        warnings: Vec::new(),
                        commands: Vec::new(),
                    }
                };

                commands.push(CompiledCommand::Assume(CompiledAssume {
                    mutations: ab.mutations.clone(),
                    body_program: body_compiled,
                    body_commands,
                }));
            }
            LocyStatement::Cypher(query) => {
                commands.push(CompiledCommand::Cypher(query.clone()));
            }
            LocyStatement::Model(_) => {
                // Models are catalog entries, not commands — handled by
                // the model-compilation pipeline (see compiler/models.rs).
            }
            LocyStatement::Calibrate(cc) => {
                // Phase C C2: validate + lower to CompiledCalibrate.
                // Caller threads `neural_predicates_preview` so we can
                // reject when the gate is off.
                commands.push(CompiledCommand::Calibrate(compile_calibrate(
                    cc,
                    model_catalog,
                    neural_predicates_preview_flag,
                )?));
            }
            LocyStatement::Validate(vc) => {
                // Phase C C3: validate rule existence + PROB column;
                // Phase C C4 emits `EceBinningBias` when bare ECE is
                // requested. Validation operates on the rule_catalog
                // directly — the neural-predicates preview gate isn't
                // re-required here (a rule that invokes models has
                // already been compiled under it transitively).
                let (cv, validate_warnings) = compile_validate(vc, rule_catalog)?;
                extra_warnings.extend(validate_warnings);
                commands.push(CompiledCommand::Validate(cv));
            }
        }
    }
    Ok(commands)
}

// ─── Phase C C2: CALIBRATE compiler ──────────────────────────────────────

/// Lower a `CalibrateCommand` AST to a `CompiledCalibrate`, validating
/// against the model catalog and resolving the holdout default. Gated
/// by `neural_predicates_preview` — `CALIBRATE` is a Phase B / C
/// preview surface and rejected when the flag is off.
fn compile_calibrate(
    cc: &uni_cypher::locy_ast::CalibrateCommand,
    model_catalog: &HashMap<String, crate::types::CompiledModel>,
    neural_predicates_preview: bool,
) -> Result<crate::types::CompiledCalibrate, LocyCompileError> {
    let name = cc.model_name.to_string();
    if !neural_predicates_preview {
        return Err(LocyCompileError::CalibratePreviewDisabled { model_name: name });
    }
    let model = model_catalog
        .get(&name)
        .ok_or_else(|| LocyCompileError::CalibrateUnknownModel { name: name.clone() })?;
    if model.output_type != uni_cypher::locy_ast::OutputType::Prob {
        return Err(LocyCompileError::CalibrateOnNonProbModel {
            name,
            declared: format!("{:?}", model.output_type),
        });
    }
    let holdout = cc.holdout.unwrap_or(0.2);
    if !(0.0 < holdout && holdout < 1.0) {
        return Err(LocyCompileError::CalibrateInvalidHoldout {
            model_name: name,
            holdout,
        });
    }
    Ok(crate::types::CompiledCalibrate {
        model_name: name,
        pattern: cc.pattern.clone(),
        where_expr: cc.where_expr.clone(),
        target_expr: cc.target_expr.clone(),
        method: cc.method,
        holdout,
    })
}

// ─── Phase C C3: VALIDATE compiler ───────────────────────────────────────

/// Lower a `ValidateCommand` AST to a `CompiledValidate`. Validates
/// rule existence, that the rule yields a PROB column, and emits a
/// Phase C C4 `EceBinningBias` warning when bare `ECE` is requested.
/// Returns `(compiled, warnings)`.
fn compile_validate(
    vc: &uni_cypher::locy_ast::ValidateCommand,
    rule_catalog: &HashMap<String, crate::types::CompiledRule>,
) -> Result<
    (
        crate::types::CompiledValidate,
        Vec<crate::types::CompilerWarning>,
    ),
    LocyCompileError,
> {
    let name = vc.rule_name.to_string();
    let rule = rule_catalog
        .get(&name)
        .ok_or_else(|| LocyCompileError::ValidateUnknownRule { name: name.clone() })?;
    let prob_col = rule
        .yield_schema
        .iter()
        .find(|c| c.is_prob)
        .ok_or_else(|| LocyCompileError::ValidateRuleHasNoProbColumn { name: name.clone() })?
        .name
        .clone();
    if vc.metrics.is_empty() {
        return Err(LocyCompileError::ValidateNoMetrics { name });
    }
    let mut warnings = Vec::new();
    if vc
        .metrics
        .contains(&uni_cypher::locy_ast::ValidationMetric::Ece)
    {
        warnings.push(crate::types::CompilerWarning {
            code: crate::types::WarningCode::EceBinningBias,
            message: format!(
                "VALIDATE '{name}' requested bare `ECE` — the equal-width-binning \
                 estimator is biased in the small-sample regime \
                 (Kumar et al. NeurIPS 2019). Use `DEBIASED_ECE` instead."
            ),
            rule_name: name.clone(),
        });
    }
    Ok((
        crate::types::CompiledValidate {
            rule_name: name,
            pattern: vc.pattern.clone(),
            where_expr: vc.where_expr.clone(),
            target_expr: vc.target_expr.clone(),
            metrics: vc.metrics.clone(),
            prob_column: prob_col,
        },
        warnings,
    ))
}

fn group_rules(program: &LocyProgram) -> HashMap<String, Vec<&RuleDefinition>> {
    let mut groups: HashMap<String, Vec<&RuleDefinition>> = HashMap::new();
    for stmt in &program.statements {
        if let LocyStatement::Rule(rule_def) = stmt {
            let name = rule_def.name.to_string();
            groups.entry(name).or_default().push(rule_def);
        }
    }
    groups
}

/// Group `CREATE RULE` statements with module-qualified names.
fn group_rules_with_context<'a>(
    program: &'a LocyProgram,
    module_ctx: &modules::ModuleContext,
) -> HashMap<String, Vec<&'a RuleDefinition>> {
    let mut groups: HashMap<String, Vec<&RuleDefinition>> = HashMap::new();
    for stmt in &program.statements {
        if let LocyStatement::Rule(rule_def) = stmt {
            let raw_name = rule_def.name.to_string();
            let name = modules::resolve_rule_name(module_ctx, &raw_name);
            groups.entry(name).or_default().push(rule_def);
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{WarningCode, YieldColumn};
    use uni_cypher::parse_locy;

    // ── Step 1: Single non-recursive rule → 1 stratum ───────────────────

    #[test]
    fn step1_single_non_recursive_rule() {
        let prog =
            parse_locy("CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b").unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.strata.len(), 1);
        assert!(!compiled.strata[0].is_recursive);
        assert_eq!(compiled.strata[0].rules.len(), 1);
        assert_eq!(compiled.strata[0].rules[0].name, "reachable");
        assert_eq!(compiled.strata[0].rules[0].clauses.len(), 1);
        assert_eq!(
            compiled.strata[0].rules[0].yield_schema,
            vec![
                YieldColumn {
                    name: "a".into(),
                    is_key: false,
                    is_prob: false,
                },
                YieldColumn {
                    name: "b".into(),
                    is_key: false,
                    is_prob: false,
                },
            ]
        );
    }

    // ── Step 2: A depends on B via IS → 2 strata, correct order ─────────

    #[test]
    fn step2_two_strata_dependency_order() {
        let prog = parse_locy(
            "CREATE RULE base AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             CREATE RULE derived AS MATCH (x)-[:FOLLOWS]->(y) WHERE x IS base TO y YIELD x, y",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.strata.len(), 2);

        let base_pos = compiled
            .strata
            .iter()
            .position(|s| s.rules.iter().any(|r| r.name == "base"))
            .expect("base stratum not found");
        let derived_pos = compiled
            .strata
            .iter()
            .position(|s| s.rules.iter().any(|r| r.name == "derived"))
            .expect("derived stratum not found");

        assert!(
            base_pos < derived_pos,
            "base stratum must precede derived in evaluation order"
        );

        let base_id = compiled.strata[base_pos].id;
        let derived_stratum = &compiled.strata[derived_pos];
        assert!(derived_stratum.depends_on.contains(&base_id));
    }

    // ── Step 3: Recursive rule (A IS A) → 1 recursive stratum ───────────

    #[test]
    fn step3_recursive_rule_single_stratum() {
        let prog = parse_locy(
            "CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(mid) \
             WHERE mid IS reachable TO b YIELD a, b",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.strata.len(), 1);
        assert!(compiled.strata[0].is_recursive);
        assert_eq!(compiled.strata[0].rules.len(), 1);
        assert_eq!(compiled.strata[0].rules[0].name, "reachable");
        assert_eq!(compiled.strata[0].rules[0].clauses.len(), 2);
    }

    // ── Step 4: Cyclic negation → CYCLIC_NEGATION error ─────────────────

    #[test]
    fn step4_cyclic_negation() {
        let prog = parse_locy(
            "CREATE RULE a AS MATCH (x)-[:R]->(y) WHERE x IS NOT b YIELD x, y \
             CREATE RULE b AS MATCH (x)-[:R]->(y) WHERE x IS NOT a YIELD x, y",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::CyclicNegation { rules } => {
                assert!(rules.contains(&"a".to_string()));
                assert!(rules.contains(&"b".to_string()));
            }
            e => panic!("expected CyclicNegation, got {e:?}"),
        }
    }

    // ── Step 5: prev in non-recursive → PREV_IN_BASE_CASE error ─────────

    #[test]
    fn step5_prev_in_non_recursive() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) ALONG cost = prev.cost + 1 YIELD a, b",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::PrevInBaseCase { rule, field } => {
                assert_eq!(rule, "r");
                assert_eq!(field, "cost");
            }
            e => panic!("expected PrevInBaseCase, got {e:?}"),
        }
    }

    // ── Step 6: SUM in recursive stratum → NON_MONOTONIC error ──────────

    #[test]
    fn step6_non_monotonic_in_recursion() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS total \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD total = SUM(a.cost) YIELD a, b, total",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::NonMonotonicInRecursion { rule, aggregate } => {
                assert_eq!(rule, "r");
                assert_eq!(aggregate.to_uppercase(), "SUM");
            }
            e => panic!("expected NonMonotonicInRecursion, got {e:?}"),
        }
    }

    // ── Step 7: MSUM warning → MSUM_NON_NEGATIVITY warning ──────────────

    #[test]
    fn step7_msum_warning() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS total \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD total = MSUM(a.weight) YIELD a, b, total",
        )
        .unwrap();

        let compiled = compile(&prog).unwrap();
        // MSUM warning is the primary assertion. As of Phase B this rule
        // ALSO trips F1 (FOLD + recursive IS-ref + no ALONG) — Stress
        // Corpus B3 — which is a legitimate co-diagnosis; the recursive
        // clause should use ALONG for per-path aggregation.
        assert!(
            compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::MsumNonNegativity),
            "expected MsumNonNegativity warning, got: {:?}",
            compiled.warnings
        );
    }

    // ── Step 8: BEST BY + MSUM → BEST_BY_WITH_MONOTONIC_FOLD error ──────

    #[test]
    fn step8_best_by_with_monotonic_fold() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS total \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD total = MSUM(a.cost) BEST BY total ASC YIELD a, b, total",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::BestByWithMonotonicFold { rule, fold } => {
                assert_eq!(rule, "r");
                assert_eq!(fold.to_uppercase(), "MSUM");
            }
            e => panic!("expected BestByWithMonotonicFold, got {e:?}"),
        }
    }

    // ── MNOR probability domain warning ────────────────────────────────

    #[test]
    fn mnor_probability_domain_warning() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS score \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD score = MNOR(a.weight) YIELD a, b, score",
        )
        .unwrap();

        let compiled = compile(&prog).unwrap();
        assert!(
            compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::ProbabilityDomainViolation)
        );
    }

    // ── MNOR + BEST BY → BestByWithMonotonicFold error ───────────────

    #[test]
    fn mnor_best_by_rejected() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS score \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD score = MNOR(a.weight) BEST BY score ASC YIELD a, b, score",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::BestByWithMonotonicFold { rule, fold } => {
                assert_eq!(rule, "r");
                assert_eq!(fold.to_uppercase(), "MNOR");
            }
            e => panic!("expected BestByWithMonotonicFold, got {e:?}"),
        }
    }

    // ── BEST BY is decided syntactically, not by the oracle ─────────

    /// Tripwire for the registry-backed oracle.
    ///
    /// `check_best_by_monotonic_fold` consumes its predicate **inverted** —
    /// "monotone" means *reject*. It also runs for every rule, not only
    /// recursive ones. So the moment the injected `MonotonicityOracle` is
    /// pointed at a plugin registry, any rule combining `BEST BY` with a fold
    /// the registry calls monotone starts failing to compile — and the builtin
    /// registry marks `MAX`, `MIN`, `COUNT`, `COUNTALL` and `COLLECT` as
    /// `monotone_join: true`.
    ///
    /// `BEST BY score DESC … FOLD peak = MAX(a.cost)` is an ordinary,
    /// runtime-correct program. This test compiles one under an oracle that
    /// answers `Some(true)` for `MAX` — exactly what a registry-backed oracle
    /// does — and asserts it is accepted. If someone re-unifies the BEST BY
    /// guard with the oracle, this goes red.
    #[test]
    fn best_by_with_registry_monotone_fold_still_compiles() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) \
             FOLD peak = MAX(a.cost) BEST BY peak ASC YIELD KEY a, KEY b, peak",
        )
        .unwrap();

        // Stands in for the registry: MAX/MIN/COUNT/COLLECT are monotone joins.
        let registry_like = |name: &str| match name.to_uppercase().as_str() {
            "MAX" | "MIN" | "COUNT" | "COUNTALL" | "COLLECT" => Some(true),
            "SUM" | "AVG" => Some(false),
            _ => None,
        };

        let result = compile_with_oracle(&prog, &HashMap::new(), &[], &registry_like);
        assert!(
            result.is_ok(),
            "BEST BY with a registry-monotone fold must still compile; got {:?}",
            result.err()
        );
    }

    /// Inverse guard: the six declared `M*` lattice folds are still refused,
    /// even when the oracle claims to know nothing about them.
    ///
    /// The rule is deliberately **non-recursive**. A recursive one would trip
    /// `check_non_monotonic_in_recursion` first (that check *does* consult the
    /// oracle, and correctly so), and the BEST BY guard would never be reached —
    /// the test would pass for the wrong reason.
    #[test]
    fn best_by_with_declared_lattice_fold_still_rejected_under_registry_oracle() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) \
             FOLD total = MSUM(a.cost) BEST BY total ASC YIELD KEY a, KEY b, total",
        )
        .unwrap();

        // Answers `None` for MSUM — if the guard consulted the oracle, this
        // program would now be accepted.
        let registry_like = |name: &str| match name.to_uppercase().as_str() {
            "MAX" | "MIN" => Some(true),
            _ => None,
        };

        match compile_with_oracle(&prog, &HashMap::new(), &[], &registry_like).unwrap_err() {
            LocyCompileError::BestByWithMonotonicFold { rule, fold } => {
                assert_eq!(rule, "r");
                assert_eq!(fold.to_uppercase(), "MSUM");
            }
            e => panic!("expected BestByWithMonotonicFold, got {e:?}"),
        }
    }

    // ── HAVING without FOLD → HavingWithoutFold error ───────────────

    #[test]
    fn having_without_fold_rejected() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) \
             FOLD n = COUNT(*) WHERE n >= 3 YIELD KEY a, n",
        )
        .unwrap();
        // With FOLD present, HAVING should compile fine.
        assert!(compile(&prog).is_ok());

        // Now test without FOLD — parser won't allow positional WHERE after
        // a missing FOLD, so the grammar itself prevents this combination.
        // Verify the compiler guard catches it if AST is constructed directly.
        // (The grammar test is covered by the integration test in locy_fold_having.rs.)
    }

    // ── MPROD probability domain warning ─────────────────────────────

    #[test]
    fn mprod_probability_domain_warning() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 1 AS score \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD score = MPROD(a.weight) YIELD a, b, score",
        )
        .unwrap();

        let compiled = compile(&prog).unwrap();
        assert!(
            compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::ProbabilityDomainViolation)
        );
    }

    // ── Step 9: Undefined rule reference → UNDEFINED_RULE error ─────────

    #[test]
    fn step9_undefined_rule() {
        let prog =
            parse_locy("CREATE RULE r AS MATCH (x)-[:R]->(y) WHERE x IS nonexistent YIELD x, y")
                .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::UndefinedRule { name } => {
                assert_eq!(name, "nonexistent");
            }
            e => panic!("expected UndefinedRule, got {e:?}"),
        }
    }

    // ── Step 10: NEW without warded position → WARDEDNESS_VIOLATION ──────

    #[test]
    fn step10_wardedness_violation() {
        let prog = parse_locy(
            "CREATE RULE base AS MATCH (a)-[:R]->(b) YIELD a, b \
             CREATE RULE r AS MATCH (x)-[:R]->(y) WHERE y IS base TO z \
             DERIVE (NEW n:T)-[:LINK]->(z)",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::WardednessViolation { rule, variable } => {
                assert_eq!(rule, "r");
                assert_eq!(variable, "z");
            }
            e => panic!("expected WardednessViolation, got {e:?}"),
        }
    }

    // ── Step 11: YIELD schema inference → correct columns ────────────────

    #[test]
    fn step11_yield_schema_inference() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, a.cost AS total_cost",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        let rule = &compiled.rule_catalog["r"];
        assert_eq!(
            rule.yield_schema,
            vec![
                YieldColumn {
                    name: "a".into(),
                    is_key: true,
                    is_prob: false,
                },
                YieldColumn {
                    name: "b".into(),
                    is_key: true,
                    is_prob: false,
                },
                YieldColumn {
                    name: "total_cost".into(),
                    is_key: false,
                    is_prob: false,
                },
            ]
        );
    }

    #[test]
    fn step11_yield_schema_mismatch() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, a.cost \
             CREATE RULE r AS MATCH (a)-[:E]->(c) YIELD KEY a, c.cost",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::YieldSchemaMismatch { rule, .. } => {
                assert_eq!(rule, "r");
            }
            e => panic!("expected YieldSchemaMismatch, got {e:?}"),
        }
    }

    // ── Step 12: Mixed priority → MIXED_PRIORITY error ───────────────────

    #[test]
    fn step12_mixed_priority() {
        let prog = parse_locy(
            "CREATE RULE r PRIORITY 1 AS MATCH (a)-[:E]->(b) YIELD a, b \
             CREATE RULE r AS MATCH (a)-[:E]->(c) YIELD a, c",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::MixedPriority { rule } => {
                assert_eq!(rule, "r");
            }
            e => panic!("expected MixedPriority, got {e:?}"),
        }
    }

    // ── Phase 4 Step 1: Commands extraction ───────────────────────────

    #[test]
    fn phase4_step1_query_command_extracted() {
        let prog = parse_locy(
            "CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             QUERY reachable WHERE a = 'Alice' RETURN a, b",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.commands.len(), 1);
        assert!(matches!(
            &compiled.commands[0],
            CompiledCommand::GoalQuery(_)
        ));
    }

    #[test]
    fn phase4_step1_undefined_rule_in_command() {
        let prog = parse_locy(
            "CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             QUERY nonexistent WHERE a = 'Alice'",
        )
        .unwrap();
        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::UndefinedRule { name } => {
                assert_eq!(name, "nonexistent");
            }
            e => panic!("expected UndefinedRule, got {e:?}"),
        }
    }

    #[test]
    fn phase4_step1_multiple_commands() {
        let prog = parse_locy(
            "CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             QUERY reachable WHERE a = 'Alice' \
             EXPLAIN RULE reachable WHERE a = 'Bob'",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.commands.len(), 2);
        assert!(matches!(
            &compiled.commands[0],
            CompiledCommand::GoalQuery(_)
        ));
        assert!(matches!(
            &compiled.commands[1],
            CompiledCommand::ExplainRule(_)
        ));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Phase B: CREATE MODEL preview-flag gating + reference validation + F1
    // ══════════════════════════════════════════════════════════════════════

    fn cfg_preview_on() -> crate::LocyConfig {
        crate::LocyConfig {
            neural_predicates_preview: true,
            ..Default::default()
        }
    }

    #[test]
    fn phase_b_preview_off_rejects_create_model() {
        let prog =
            parse_locy("CREATE MODEL m AS INPUT (s) OUTPUT PROB risk USING xervo('classify/m')")
                .unwrap();
        let result = compile(&prog);
        match result {
            Err(LocyCompileError::NeuralPreviewDisabled { model_name }) => {
                assert_eq!(model_name, "m");
            }
            other => panic!("expected NeuralPreviewDisabled, got {other:?}"),
        }
    }

    #[test]
    fn phase_b_preview_on_compiles_model() {
        let prog = parse_locy(
            "CREATE MODEL supplier_risk AS \
             INPUT (s:Supplier) \
             OUTPUT PROB risk \
             USING xervo('classify/supplier-risk-v3') \
             CALIBRATION platt_scaling \
             VERSION '3.1.0'",
        )
        .unwrap();
        let compiled = compile_with_config(&prog, &cfg_preview_on()).unwrap();
        assert_eq!(compiled.model_catalog.len(), 1);
        let m = &compiled.model_catalog["supplier_risk"];
        assert_eq!(m.xervo_alias, "classify/supplier-risk-v3");
        assert_eq!(m.version.as_deref(), Some("3.1.0"));
        // No UncalibratedLLMLogprobs because (a) calibration is set, and
        // (b) alias isn't LLM-shaped.
        assert!(
            !compiled
                .warnings
                .iter()
                .any(|w| w.code == crate::types::WarningCode::UncalibratedLLMLogprobs)
        );
    }

    #[test]
    fn phase_b_g1_uncalibrated_llm_warning() {
        let prog = parse_locy(
            "CREATE MODEL chatty AS \
             INPUT (s) OUTPUT PROB out USING xervo('generate/gpt-4o')",
        )
        .unwrap();
        let compiled = compile_with_config(&prog, &cfg_preview_on()).unwrap();
        assert!(
            compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::UncalibratedLLMLogprobs),
            "expected UncalibratedLLMLogprobs warning, got: {:?}",
            compiled.warnings
        );
    }

    #[test]
    fn phase_b_g1_uncalibrated_llm_suppressed_when_calibrated() {
        let prog = parse_locy(
            "CREATE MODEL chatty AS \
             INPUT (s) OUTPUT PROB out USING xervo('generate/gpt-4o') \
             CALIBRATION platt_scaling",
        )
        .unwrap();
        let compiled = compile_with_config(&prog, &cfg_preview_on()).unwrap();
        assert!(
            !compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::UncalibratedLLMLogprobs)
        );
    }

    #[test]
    fn phase_b_model_name_collision() {
        let prog = parse_locy(
            "CREATE MODEL dup AS INPUT (s) OUTPUT PROB risk USING xervo('classify/a') \
             CREATE MODEL dup AS INPUT (s) OUTPUT PROB risk USING xervo('classify/b')",
        )
        .unwrap();
        let result = compile_with_config(&prog, &cfg_preview_on());
        assert!(matches!(
            result,
            Err(LocyCompileError::ModelNameCollision { name }) if name == "dup"
        ));
    }

    #[test]
    fn phase_b_model_arity_mismatch_in_rule() {
        // Rule body invokes the model with 2 args; declaration has 1
        // input. Compile must reject with ModelArityMismatch.
        let prog = parse_locy(
            "CREATE MODEL scorer AS \
             INPUT (s) OUTPUT PROB out USING xervo('classify/s') \
             CREATE RULE r AS MATCH (s) WHERE scorer(s, s) > 0.5 YIELD s",
        )
        .unwrap();
        let result = compile_with_config(&prog, &cfg_preview_on());
        match result {
            Err(LocyCompileError::ModelArityMismatch {
                name,
                rule,
                expected,
                actual,
            }) => {
                assert_eq!(name, "scorer");
                assert_eq!(rule, "r");
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            other => panic!("expected ModelArityMismatch, got {other:?}"),
        }
    }

    #[test]
    fn phase_b_model_arity_correct_accepted() {
        // YIELD-position invocation: the supported path; should compile.
        let prog = parse_locy(
            "CREATE MODEL scorer AS \
             INPUT (s) OUTPUT PROB out USING xervo('classify/s') \
             CREATE RULE r AS MATCH (s) YIELD KEY s, scorer(s) AS risk",
        )
        .unwrap();
        let compiled = compile_with_config(&prog, &cfg_preview_on()).unwrap();
        assert!(compiled.model_catalog.contains_key("scorer"));
        assert!(compiled.rule_catalog.contains_key("r"));
    }

    #[test]
    fn phase_b_where_model_invocation_rejected() {
        // Phase B Slice 7: WHERE-position invocations error at compile
        // time with a clear "use YIELD instead" message until the
        // planner refactor that supports pre-filter invocation lands.
        let prog = parse_locy(
            "CREATE MODEL scorer AS \
             INPUT (s) OUTPUT PROB out USING xervo('classify/s') \
             CREATE RULE r AS MATCH (s) WHERE scorer(s) > 0.5 YIELD KEY s",
        )
        .unwrap();
        match compile_with_config(&prog, &cfg_preview_on()) {
            Err(LocyCompileError::WhereModelInvocationNotYetSupported { rule, model }) => {
                assert_eq!(rule, "r");
                assert_eq!(model, "scorer");
            }
            other => {
                panic!("expected WhereModelInvocationNotYetSupported, got {other:?}")
            }
        }
    }

    #[test]
    fn phase_b_f1_fold_in_recursive_path_without_along() {
        // The very same shape as step7_msum_warning — F1 must fire.
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS total \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD total = MSUM(a.weight) YIELD a, b, total",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();
        assert!(
            compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::FoldInRecursivePath),
            "expected FoldInRecursivePath, got: {:?}",
            compiled.warnings
        );
    }

    // ── #265: REQUIRE, the definitional threshold ────────────────────────

    /// Build the OFAC-shaped recursive rule with a given fold and REQUIRE.
    fn require_program(fold: &str, cmp: &str) -> String {
        format!(
            "CREATE RULE blocked AS MATCH (e:Entity) WHERE e.designated = true \
             YIELD KEY e, 100.0 AS agg \
             CREATE RULE blocked AS MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o IS blocked FOLD agg = {fold} REQUIRE agg {cmp} \
             YIELD KEY e, agg"
        )
    }

    /// A lower bound over a non-decreasing fold is one-way, so it is accepted.
    #[test]
    fn issue_265_lower_bound_over_non_decreasing_fold_is_accepted() {
        let prog = parse_locy(&require_program("MSUM(s.pct)", ">= 50.0")).unwrap();
        compile(&prog).expect("MSUM only rises, so `agg >= 50` can only turn on");
    }

    /// An upper bound over the same fold can turn back off, so it is rejected.
    ///
    /// The negative twin of the test above: identical but for the operator.
    /// Without it, a check that accepted everything would still pass.
    #[test]
    fn issue_265_upper_bound_over_non_decreasing_fold_is_rejected() {
        let prog = parse_locy(&require_program("MSUM(s.pct)", "<= 50.0")).unwrap();
        match compile(&prog) {
            Err(LocyCompileError::NonMonotonicFilterInRecursion { rule, .. }) => {
                assert_eq!(rule, "blocked");
            }
            other => panic!("expected NonMonotonicFilterInRecursion, got {other:?}"),
        }
    }

    /// The mirror pairing: an upper bound over a non-increasing fold is fine.
    #[test]
    fn issue_265_upper_bound_over_non_increasing_fold_is_accepted() {
        let prog = parse_locy(&require_program("MPROD(s.pct)", "<= 0.5")).unwrap();
        compile(&prog).expect("MPROD only falls, so `agg <= 0.5` can only turn on");
    }

    /// ... and a lower bound over it is rejected.
    #[test]
    fn issue_265_lower_bound_over_non_increasing_fold_is_rejected() {
        let prog = parse_locy(&require_program("MPROD(s.pct)", ">= 0.5")).unwrap();
        assert!(matches!(
            compile(&prog),
            Err(LocyCompileError::NonMonotonicFilterInRecursion { .. })
        ));
    }

    /// Equality can turn back off in both directions, so it is refused.
    #[test]
    fn issue_265_an_equality_require_is_rejected() {
        let prog = parse_locy(&require_program("MSUM(s.pct)", "= 50.0")).unwrap();
        assert!(matches!(
            compile(&prog),
            Err(LocyCompileError::NonMonotonicFilterInRecursion { .. })
        ));
    }

    /// Outside a recursive stratum there is one pass and nothing can oscillate,
    /// so any direction is admissible.
    ///
    /// This is what makes a rule refactor-safe: the author writes REQUIRE for
    /// the meaning they want, and it keeps that meaning if recursion arrives
    /// later — at which point the direction gate starts applying.
    #[test]
    fn issue_265_direction_is_not_policed_outside_recursion() {
        let prog = parse_locy(
            "CREATE RULE stake AS MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o.designated = true FOLD agg = MSUM(s.pct) REQUIRE agg <= 50.0 \
             YIELD KEY e, agg",
        )
        .unwrap();
        compile(&prog).expect("a non-recursive rule evaluates once; nothing can flip");
    }

    /// REQUIRE names a FOLD output, so it needs a FOLD to name.
    #[test]
    fn issue_265_require_without_fold_is_rejected() {
        let prog =
            parse_locy("CREATE RULE r AS MATCH (e:Entity) REQUIRE e.pct >= 50.0 YIELD KEY e")
                .unwrap();
        assert!(matches!(
            compile(&prog),
            Err(LocyCompileError::RequireWithoutFold { .. })
        ));
    }

    /// #265: a post-FOLD WHERE on a self-referencing rule is warned about.
    ///
    /// The OFAC 50 % ownership shape, reduced. `o IS blocked` makes the stratum
    /// recursive, so `WHERE agg >= 50.0` filters the converged answer rather
    /// than the recursion: an owner below the threshold is absent from the
    /// output yet still qualifies rows downstream of it.
    #[test]
    fn issue_265_having_on_a_self_referencing_rule_warns() {
        let prog = parse_locy(
            "CREATE RULE blocked AS MATCH (e:Entity) WHERE e.designated = true \
             YIELD KEY e, 100.0 AS agg \
             CREATE RULE blocked AS MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o IS blocked FOLD agg = MSUM(s.pct) WHERE agg >= 50.0 \
             YIELD KEY e, agg",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();
        assert!(
            compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::HavingInRecursivePath),
            "expected HavingInRecursivePath, got: {:?}",
            compiled.warnings
        );
    }

    /// The negative twin: the same filter outside a recursive stratum is fine.
    ///
    /// Without this, a check that warned on every post-FOLD WHERE would pass
    /// the positive case above while making the warning useless. The only
    /// difference here is that the self-reference is replaced by a base-fact
    /// test, which is exactly the control #265's reproduction uses.
    #[test]
    fn issue_265_having_without_recursion_does_not_warn() {
        let prog = parse_locy(
            "CREATE RULE blocked AS MATCH (e:Entity) WHERE e.designated = true \
             YIELD KEY e, 100.0 AS agg \
             CREATE RULE blocked AS MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o.designated = true FOLD agg = MSUM(s.pct) WHERE agg >= 50.0 \
             YIELD KEY e, agg",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();
        assert!(
            !compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::HavingInRecursivePath),
            "a post-FOLD WHERE outside a recursive stratum applies as written \
             and must not warn, got: {:?}",
            compiled.warnings
        );
    }

    /// A recursive FOLD with no post-FOLD WHERE warns only about the rollup.
    ///
    /// Separates this warning from its neighbour: `FoldInRecursivePath` is
    /// about how a recursive rollup composes, `HavingInRecursivePath` about a
    /// filter that does not constrain what it appears to. Firing the second
    /// whenever the first fires would make it noise.
    #[test]
    fn issue_265_a_recursive_fold_without_having_warns_only_about_the_fold() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS total \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD total = MSUM(a.weight) YIELD a, b, total",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();
        assert!(
            compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::FoldInRecursivePath),
            "the neighbouring warning must still fire, got: {:?}",
            compiled.warnings
        );
        assert!(
            !compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::HavingInRecursivePath),
            "no post-FOLD WHERE is present, so #265's warning must stay silent, \
             got: {:?}",
            compiled.warnings
        );
    }

    #[test]
    fn phase_b_f1_fires_even_when_along_present() {
        // Same recursive structure but with ALONG. F1 used to be suppressed
        // here, on the theory that ALONG gives per-path aggregation and so
        // avoids the problem. It does not: the ALONG accumulator is an
        // ordinary non-KEY column and is deduplicated by the fixpoint exactly
        // like a FOLD input, so two paths carrying an equal accumulated value
        // still collapse (issue #159, pinned by
        // `locy_issue_159_along_must_not_collapse_equal_valued_paths`).
        // Suppressing here silenced the formulation the warning itself used to
        // recommend, so the ALONG case is now warned about too.
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[e:E]->(b) ALONG total = e.weight \
             YIELD a, b, total \
             CREATE RULE r AS MATCH (a)-[e:E]->(mid) WHERE mid IS r TO b \
             ALONG total = prev.total + e.weight \
             FOLD total = MSUM(total) YIELD a, b, total",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();
        assert!(
            compiled
                .warnings
                .iter()
                .any(|w| w.code == WarningCode::FoldInRecursivePath),
            "FoldInRecursivePath must fire for a recursive fold even with ALONG, got: {:?}",
            compiled.warnings
        );
    }

    // ── IS NOT subject must be a node ────────────────────────────────────

    /// The shape that must keep compiling: `dis` is bound by the *positive*
    /// `IS ... TO`, not by MATCH.
    ///
    /// This is the highest-value assertion in the group. Eleven programs use it
    /// — the `novel`/`known` drug-repurposing family across seven TCK feature
    /// files, a Python binding test, and a published website notebook — and all
    /// eleven break if the check ever validates `is_ref.target` instead of only
    /// `is_ref.subjects`.
    #[test]
    fn is_not_to_target_bound_by_earlier_positive_is_ref_compiles() {
        let prog = parse_locy(
            "CREATE RULE signal AS MATCH (d:Drug)-[:HINTS]->(x:Disease) YIELD KEY d, KEY x \
             CREATE RULE known AS MATCH (d:Drug)-[:TREATS]->(x:Disease) YIELD KEY d, KEY x \
             CREATE RULE novel AS MATCH (d:Drug) \
             WHERE d IS signal TO dis, d IS NOT known TO dis \
             YIELD KEY d, KEY dis",
        )
        .unwrap();
        assert!(
            compile(&prog).is_ok(),
            "an IS NOT TO-target bound by an earlier positive IS ... TO must compile"
        );
    }

    /// A subject bound by an earlier positive `IS ... TO` target is itself a
    /// node, so it is a valid negation subject. Binding is order-sensitive:
    /// `mid` exists only because the positive reference precedes the negation.
    #[test]
    fn is_not_subject_bound_by_earlier_positive_is_ref_compiles() {
        let prog = parse_locy(
            "CREATE RULE link AS MATCH (a:N)-[:R]->(b:N) YIELD KEY a, KEY b \
             CREATE RULE blocked AS MATCH (n:N) WHERE n.bad = true YIELD KEY n \
             CREATE RULE ok AS MATCH (a:N) \
             WHERE a IS link TO mid, mid IS NOT blocked \
             YIELD KEY a, KEY mid",
        )
        .unwrap();
        assert!(
            compile(&prog).is_ok(),
            "a subject bound by an earlier positive IS ... TO target is a node"
        );
    }

    /// A YIELD alias that renames a MATCH-bound **node** is still a node.
    ///
    /// `YIELD KEY a AS subj` makes `subj` another name for the node `a`, and
    /// the projected column still holds a vid, so `subj IS NOT flagged`
    /// resolves at runtime. A first version of this check looked only at the
    /// MATCH pattern and false-rejected this shape — two working tests in
    /// `locy_issue_158_is_not_subject_scope.rs` caught it. Aliases of
    /// *non*-node expressions stay rejected; see
    /// `is_not_yield_alias_subject_is_rejected`, whose alias is `a.n`.
    #[test]
    fn is_not_yield_alias_of_a_node_compiles() {
        let prog = parse_locy(
            "CREATE RULE flagged AS MATCH (a:A)-[:E]->(b:B) YIELD KEY a, KEY b \
             CREATE RULE probe AS MATCH (a:A), (b:B) WHERE subj IS NOT flagged \
             YIELD KEY b, KEY a AS subj",
        )
        .unwrap();
        assert!(
            compile(&prog).is_ok(),
            "an alias of a MATCH-bound node is itself a valid negation subject"
        );
    }

    /// A plain MATCH-bound node subject, and the tuple form, both compile.
    #[test]
    fn is_not_match_bound_node_subjects_compile() {
        let prog = parse_locy(
            "CREATE RULE risk AS MATCH (a:N)-[:R]->(b:N) YIELD KEY a, KEY b \
             CREATE RULE safe AS MATCH (a:N), (b:N) WHERE (a, b) IS NOT risk \
             YIELD KEY a, KEY b",
        )
        .unwrap();
        assert!(
            compile(&prog).is_ok(),
            "a tuple of MATCH-bound node subjects must compile"
        );
    }

    /// A subject bound inside a parenthesized sub-pattern must compile.
    ///
    /// `collect_path_vars` carries a comment recording that an empty
    /// `Parenthesized` arm previously caused false `WardednessViolation`s on
    /// valid rules; a quantified sub-pattern has to be parenthesised, so the
    /// node-only collector this check uses must recurse the same way.
    #[test]
    fn is_not_subject_inside_parenthesized_subpattern_compiles() {
        let prog = parse_locy(
            "CREATE RULE flagged AS MATCH (n:N) WHERE n.bad = true YIELD KEY n \
             CREATE RULE probe AS MATCH (a:N)-[:R]->(b:N) WHERE b IS NOT flagged \
             YIELD KEY a, KEY b",
        )
        .unwrap();
        assert!(compile(&prog).is_ok());
    }

    /// The target case: the subject is only a YIELD alias, never MATCH-bound.
    /// Before this check it compiled and failed at evaluation time.
    #[test]
    fn is_not_yield_alias_subject_is_rejected() {
        let prog = parse_locy(
            "CREATE RULE flagged AS MATCH (a:A)-[:E]->(b:B) YIELD KEY a, KEY b \
             CREATE RULE probe AS MATCH (a:A), (b:B) WHERE x IS NOT flagged \
             YIELD KEY b, a.n AS x",
        )
        .unwrap();
        match compile(&prog).unwrap_err() {
            LocyCompileError::IsNotSubjectNotANode { rule, variable } => {
                assert_eq!(rule, "probe");
                assert_eq!(variable, "x");
            }
            e => panic!("expected IsNotSubjectNotANode, got {e:?}"),
        }
    }

    /// A relationship variable exposes `_eid`, not `_vid`, so it cannot key the
    /// anti-join. The runtime rejects it; this rejects it earlier.
    #[test]
    fn is_not_relationship_variable_subject_is_rejected() {
        let prog = parse_locy(
            "CREATE RULE flagged AS MATCH (n:N) WHERE n.bad = true YIELD KEY n \
             CREATE RULE probe AS MATCH (a:N)-[e:R]->(b:N) WHERE e IS NOT flagged \
             YIELD KEY a, KEY b",
        )
        .unwrap();
        match compile(&prog).unwrap_err() {
            LocyCompileError::IsNotSubjectNotANode { rule, variable } => {
                assert_eq!(rule, "probe");
                assert_eq!(variable, "e");
            }
            e => panic!("expected IsNotSubjectNotANode, got {e:?}"),
        }
    }

    /// An ALONG name used as a negation subject is rejected — it is a scalar.
    ///
    /// Note the clause order: the grammar fixes `MATCH → WHERE → ALONG → FOLD`
    /// (`locy.pest` `rule_definition`), so an ALONG or FOLD name is *declared
    /// after* the WHERE that would reference it and can never legally precede
    /// it. Writing them in the other order is a parse error, not a compile
    /// error. So in a well-formed program the name is simply unbound at WHERE
    /// time, which is what this asserts — and it is why the negation-subject
    /// check never needs a special case for ALONG or FOLD names.
    #[test]
    fn is_not_along_name_as_subject_is_rejected() {
        let prog = parse_locy(
            "CREATE RULE flagged AS MATCH (n:N) WHERE n.bad = true YIELD KEY n \
             CREATE RULE probe AS MATCH (a:N)-[e:R]->(b:N) \
             WHERE total IS NOT flagged ALONG total = e.weight \
             YIELD KEY a, KEY b, total",
        )
        .unwrap();
        match compile(&prog).unwrap_err() {
            LocyCompileError::IsNotSubjectNotANode { rule, variable } => {
                assert_eq!(rule, "probe");
                assert_eq!(variable, "total");
            }
            e => panic!("expected IsNotSubjectNotANode, got {e:?}"),
        }
    }

    /// A subject that is bound nowhere at all.
    #[test]
    fn is_not_entirely_unbound_subject_is_rejected() {
        let prog = parse_locy(
            "CREATE RULE flagged AS MATCH (n:N) WHERE n.bad = true YIELD KEY n \
             CREATE RULE probe AS MATCH (a:N) WHERE ghost IS NOT flagged YIELD KEY a",
        )
        .unwrap();
        match compile(&prog).unwrap_err() {
            LocyCompileError::IsNotSubjectNotANode { rule, variable } => {
                assert_eq!(rule, "probe");
                assert_eq!(variable, "ghost");
            }
            e => panic!("expected IsNotSubjectNotANode, got {e:?}"),
        }
    }

    /// Only the *negated* form is constrained — a positive `IS` resolves in the
    /// body plan before projection and does not join on node identity.
    #[test]
    fn positive_is_ref_with_non_node_subject_still_compiles() {
        let prog = parse_locy(
            "CREATE RULE flagged AS MATCH (n:N) WHERE n.bad = true YIELD KEY n \
             CREATE RULE probe AS MATCH (a:N)-[e:R]->(b:N) WHERE e IS flagged \
             YIELD KEY a, KEY b",
        )
        .unwrap();
        assert!(
            compile(&prog).is_ok(),
            "positive IS is unaffected by the negation-subject rule"
        );
    }
}
