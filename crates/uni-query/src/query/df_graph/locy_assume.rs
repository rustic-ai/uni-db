// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! ASSUME block evaluation via `LocyExecutionContext`.
//!
//! Ported from `uni-locy/src/orchestrator/assume.rs`. Uses `LocyExecutionContext`
//! for L0 fork/restore, mutations, and strata re-evaluation.
//!
//! # Incremental re-evaluation (A4)
//!
//! The hypothetical state differs from the state before the block only by the
//! block's mutations. A stratum those mutations cannot affect
//! ([`uni_locy::incremental::affected_strata`]) has the same facts in both
//! states, so `re_evaluate_hypothetical` copies them from the facts of the
//! state before the mutation (the adapter's [`StrataBaseline`]) and derives
//! only the affected strata, from scratch. `ABDUCE` validates each candidate
//! modification the same way.
//!
//! Affected strata are recomputed in full rather than seeded from the
//! change: a delta-seeded fixpoint would need the delta of every base clause
//! (the matches the mutation added or removed), which no operator computes,
//! and is unsound for deletions, negation and aggregates anyway.

// Rust guideline compliant

use std::collections::BTreeSet;

use uni_cypher::ast::Query;
use uni_locy::incremental::{MutationFootprint, affected_strata, mutation_footprint};
use uni_locy::result::CommandResult;
use uni_locy::types::{CompiledAssume, CompiledCommand};
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_delta::RowStore;

use super::locy_explain::ProvenanceStore;
use super::locy_program::ReusedStrata;
use super::locy_traits::{LocyExecutionContext, ReEvaluation, StrataBaseline};

/// The strata of `program` whose facts `baseline` can supply in the state a
/// mutation with footprint `footprint` produces from the baseline's state.
///
/// A stratum qualifies when the mutation cannot affect it and the baseline
/// holds every relation it derives (a rule the baseline's evaluation never
/// ran, e.g. one the goal-directed rewrite pruned, is derived instead).
fn reuse_plan(
    program: &CompiledProgram,
    baseline: &StrataBaseline,
    footprint: &MutationFootprint,
) -> Option<ReusedStrata> {
    let affected = affected_strata(program, footprint);
    let positions: BTreeSet<usize> = program
        .strata
        .iter()
        .enumerate()
        .filter(|(pos, stratum)| {
            !affected[*pos]
                && stratum
                    .rules
                    .iter()
                    .filter(|r| !r.yield_schema.is_empty())
                    .all(|r| baseline.facts.get(&r.name).is_some())
        })
        .map(|(pos, _)| pos)
        .collect();
    // Hydrated rows stay correct when every entity they name reads back
    // unchanged; otherwise the reused facts are hydrated again.
    let hydrated = baseline
        .rows
        .clone()
        .filter(|_| footprint.preserves_existing_entities());
    (!positions.is_empty()).then(|| ReusedStrata {
        facts: baseline.facts.clone(),
        positions,
        hydrated,
    })
}

/// Re-evaluates `program` and records what was derived and what reused.
async fn re_evaluate_counted(
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    reused: Option<ReusedStrata>,
    stats: &mut LocyStats,
) -> Result<ReEvaluation, LocyError> {
    let result = ctx
        .re_evaluate_strata_reusing(program, config, reused)
        .await?;
    stats.hypothetical_strata_reused += result.reused.len();
    stats.hypothetical_strata_evaluated += program.strata.len() - result.reused.len();
    if let Some(facts) = &result.facts {
        stats.hypothetical_facts_derived += program
            .strata
            .iter()
            .enumerate()
            .filter(|(pos, _)| !result.reused.contains(pos))
            .flat_map(|(_, s)| &s.rules)
            .map(|r| facts.fact_count(&r.name))
            .sum::<usize>();
    }
    tracing::debug!(
        strata = program.strata.len(),
        reused = result.reused.len(),
        "locy.hypothetical.re_evaluated"
    );
    Ok(result)
}

/// Re-evaluates `program` in a hypothetical state: the state `baseline` was
/// derived in, changed by a mutation with footprint `footprint`.
///
/// Strata the mutation cannot affect take their facts from `baseline`; the
/// rest are derived from scratch. With no baseline everything is derived.
///
/// # Errors
///
/// Whatever the re-evaluation raises.
pub(crate) async fn re_evaluate_hypothetical(
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    baseline: Option<&StrataBaseline>,
    footprint: &MutationFootprint,
    stats: &mut LocyStats,
) -> Result<ReEvaluation, LocyError> {
    let reused = baseline.and_then(|b| reuse_plan(program, b, footprint));
    re_evaluate_counted(program, ctx, config, reused, stats).await
}

/// A binding error from planning an ASSUME body against its parent's rules.
fn binding_error(e: uni_locy::LocyCompileError) -> LocyError {
    LocyError::EvaluationError {
        message: format!("LocyCompileError: {e}"),
    }
}

/// Evaluate an ASSUME block: fork L0, apply mutations, re-evaluate rules,
/// execute body commands, collect results, then restore.
pub async fn evaluate_assume(
    assume: &CompiledAssume,
    parent_program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    stats: &mut LocyStats,
) -> Result<Vec<FactRow>, LocyError> {
    // 1. Fork L0 for hypothetical reasoning
    //
    // The fork/restore pair below is deliberately NOT exception-safe: every `?`
    // between here and step 6 skips `restore_l0()`, leaving the hypothetical
    // clone installed. That is sound, but for a non-local reason worth stating
    // because it does not read as sound locally.
    //
    // `fork_l0` swaps only the `NativeExecutionAdapter`'s own `locy_l0` pointer
    // to a deep clone and parks the real `Arc` on `l0_save_stack`; hypothetical
    // mutations land in the clone. A `Transaction` holds its *own* `Arc` to the
    // original buffer, so it never observes the fork -- and the adapter is built
    // fresh per evaluation and dropped when this call returns, error or not, so
    // an un-restored clone dies with it. The command dispatch loop propagates
    // errors immediately, so no later command runs against a forked adapter.
    //
    // Verified 2026-09-05 against the transaction path (where `locy_l0` *is*
    // `tx_l0`): with a failing `THEN` body, the hypothetical row is materialized
    // in the fork and still absent from the transaction and the commit.
    //
    // Two changes would make the leak real, so pair either with a drop guard:
    // reusing one adapter across evaluations, or catching an ASSUME/ABDUCE error
    // and continuing the dispatch loop instead of propagating it.
    ctx.fork_l0()
        .await
        .map_err(|e| LocyError::SavepointFailed {
            message: format!("failed to fork L0: {}", e),
        })?;

    // The facts of the state the mutations apply to. Taken before they run:
    // the adapter offers a baseline only while its state is current.
    let baseline = ctx.reuse_baseline();
    let footprint = mutation_footprint(&assume.mutations);

    // 2. Execute mutations
    if !assume.mutations.is_empty() {
        let query = Query::Single(uni_cypher::ast::Statement {
            clauses: assume.mutations.clone(),
        });
        ctx.execute_mutation(query, config.params.clone()).await?;
        stats.mutations_executed += 1;
    }

    // The body is compiled as its own program, so its plan build saw only the
    // body's `rule_catalog`. A body rule that IS-references an outer rule --
    // the ordinary way to reason hypothetically about existing rules -- failed
    // with `IS-reference to unknown rule`, and the body commands' lookups
    // consulted the parent's catalog, which lacks the body's own rules. Neither
    // half worked, with or without a `MODULE`.
    //
    // Build one catalog visible to both: the parent's rules, plus the body's
    // shadowing on collision. Module-qualified parent rules are also exposed
    // under their bare last segment, mirroring what the compiler does when it
    // validates the body -- inside `MODULE m`, writing `adult` *is* how you name
    // `m.adult`, and the body's AST carries the bare spelling.
    let merged_rule_catalog = {
        let mut merged = std::collections::HashMap::new();
        for (name, rule) in &parent_program.rule_catalog {
            if let Some((_, bare)) = name.rsplit_once('.') {
                merged.insert(bare.to_string(), rule.clone());
            }
            merged.insert(name.clone(), rule.clone());
        }
        // Body rules last so a body-local name shadows an outer one.
        for (name, rule) in &assume.body_program.rule_catalog {
            merged.insert(name.clone(), rule.clone());
        }
        merged
    };

    // 3. Re-evaluate the parent program's strata in the mutated state,
    // reusing the strata the mutations cannot affect.
    let parent = re_evaluate_hypothetical(
        parent_program,
        ctx,
        config,
        baseline.as_ref(),
        &footprint,
        stats,
    )
    .await?;
    let parent_facts = parent.facts;
    // Kept for nested hypothetical commands only when there is a state to offer.
    let parent_rows = parent_facts
        .as_ref()
        .map(|_| std::sync::Arc::new(parent.rows.clone()));
    let mut assume_derived_store: RowStore = parent.rows;
    stats.queries_executed += 1; // rough accounting for the re-evaluation

    // 4. Also evaluate the body program's strata, if any.
    //
    // The body's strata are evaluated **after the parent's, in one run**, not
    // standalone. A body rule may IS-reference an outer rule
    // (`CREATE RULE eligible AS MATCH (p:Person) WHERE p IS adult ...`), and a
    // standalone evaluation of just the body's strata gives that reference no
    // facts to resolve against, so the rule derives nothing.
    //
    // That gap used to be invisible: the body's QUERY went through the SLG
    // resolver, which re-derives a goal's dependencies on demand and so
    // recomputed `adult` for itself. Now that QUERY answers from the derived
    // store, the store has to actually contain the answer.
    if !assume.body_program.strata.is_empty() {
        // The body's strata, planned against the merged catalog, appended to
        // the parent's so cross-references resolve. Parent strata come first —
        // stratification order is a dependency order, and a body rule may
        // depend on a parent rule but not the reverse.
        let combined_strata: Vec<_> = parent_program
            .strata
            .iter()
            .cloned()
            .chain(assume.body_program.strata.iter().cloned())
            .collect();
        let mut body_program = CompiledProgram {
            rule_catalog: merged_rule_catalog.clone(),
            strata: combined_strata,
            ..assume.body_program.clone()
        };
        // The body was compiled without the parent's schemas, so a body clause
        // referencing a parent rule had no binding plan until this catalog.
        uni_locy::attach_binding_plans(&mut body_program).map_err(binding_error)?;
        // The parent's strata were derived in this very state a moment ago;
        // only the body's own strata are new. Positional, not by name: a body
        // rule may shadow a parent rule's name.
        let reused = parent_facts.clone().map(|facts| ReusedStrata {
            facts,
            positions: (0..parent_program.strata.len()).collect(),
            // Hydrated in this very state.
            hydrated: parent_rows.clone(),
        });
        let body = re_evaluate_counted(&body_program, ctx, config, reused, stats).await?;
        for (name, rel) in body.rows {
            assume_derived_store.insert(name, rel);
        }
    }

    // The parent's strata with the merged catalog: body commands must be able
    // to name the body's own rules, and a nested ASSUME must still see the
    // parent's strata as its own parent, so the two are combined rather than
    // one replacing the other.
    let mut command_program = CompiledProgram {
        rule_catalog: merged_rule_catalog,
        ..parent_program.clone()
    };
    uni_locy::attach_binding_plans(&mut command_program).map_err(binding_error)?;

    // The body's facts are fixed from here on: body commands read them by
    // shared reference, as top-level commands do.
    let assume_derived_store = assume_derived_store;

    // Nested hypothetical commands (ASSUME, ABDUCE) start from this state, so
    // they reuse the parent strata derived here. The parent's facts, not the
    // body's: a nested command re-evaluates `command_program`, whose strata
    // are the parent's, and a body rule may shadow a parent rule's name.
    let pushed_baseline = parent_facts.is_some();
    if let Some(facts) = parent_facts {
        ctx.push_reuse_baseline(StrataBaseline {
            facts,
            rows: parent_rows,
        });
    }

    // 5. Execute body commands and collect results
    let mut result_rows = Vec::new();
    for cmd in &assume.body_commands {
        let cmd_result = dispatch_body_command(
            cmd,
            &command_program,
            ctx,
            config,
            &assume_derived_store,
            stats,
        )
        .await?;
        match cmd_result {
            CommandResult::Query(rows) => result_rows.extend(rows),
            CommandResult::Cypher(rows) => result_rows.extend(rows),
            _ => {}
        }
    }

    if pushed_baseline {
        ctx.pop_reuse_baseline();
    }

    // If no commands produced rows, collect all derived facts
    if result_rows.is_empty() && assume.body_commands.is_empty() {
        for relation in assume_derived_store.values() {
            result_rows.extend(relation.rows.iter().cloned());
        }
    }

    // 6. Restore L0 (discard hypothetical mutations)
    ctx.restore_l0()
        .await
        .map_err(|e| LocyError::SavepointFailed {
            message: format!("failed to restore L0: {}", e),
        })?;

    Ok(result_rows)
}

/// Dispatch a body command inside an ASSUME block.
///
/// Uses Box::pin for recursive async (nested ASSUME may dispatch sub-commands).
fn dispatch_body_command<'a>(
    cmd: &'a CompiledCommand,
    program: &'a CompiledProgram,
    ctx: &'a dyn LocyExecutionContext,
    config: &'a LocyConfig,
    derived_store: &'a RowStore,
    stats: &'a mut LocyStats,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<CommandResult, LocyError>> + Send + 'a>,
> {
    Box::pin(async move {
        match cmd {
            CompiledCommand::GoalQuery(gq) => {
                // The same path as a top-level QUERY: the facts this body's
                // re-evaluation published, filtered and projected.
                let rows = super::locy_query::evaluate_query(gq, program, config, derived_store)?;
                Ok(CommandResult::Query(rows))
            }
            CompiledCommand::DeriveCommand(dc) => {
                let affected =
                    super::locy_derive::derive_command(dc, program, ctx, derived_store, stats)
                        .await?;
                Ok(CommandResult::Derive { affected })
            }
            CompiledCommand::ExplainRule(eq) => {
                let node = super::locy_explain::explain_rule(
                    eq,
                    program,
                    ctx,
                    config,
                    derived_store,
                    stats,
                    None::<&ProvenanceStore>,
                    None,
                )
                .await?;
                Ok(CommandResult::Explain(node))
            }
            CompiledCommand::Abduce(aq) => {
                let result = super::locy_abduce::evaluate_abduce(
                    aq,
                    program,
                    ctx,
                    config,
                    derived_store,
                    stats,
                )
                .await?;
                Ok(CommandResult::Abduce(result))
            }
            CompiledCommand::Assume(ca) => {
                let rows = evaluate_assume(ca, program, ctx, config, stats).await?;
                Ok(CommandResult::Assume(rows))
            }
            CompiledCommand::Cypher(q) => {
                let rows = ctx.execute_cypher_read(q.clone()).await?;
                stats.queries_executed += 1;
                Ok(CommandResult::Cypher(rows))
            }
            CompiledCommand::Calibrate(_) => {
                // CALIBRATE inside an ASSUME body is rejected for now:
                // hypothetical calibration runs would need to roll back
                // alongside the rest of the savepoint, which is more
                // bookkeeping than this slice is signing up for. The
                // top-level CALIBRATE path runs in `run_program`.
                Err(LocyError::EvaluationError {
                    message: "CALIBRATE statements are not yet supported inside ASSUME \
                              blocks; declare CALIBRATE at the top level instead"
                        .to_string(),
                })
            }
            CompiledCommand::Validate(_) => {
                // Same reasoning as CALIBRATE: VALIDATE inside an
                // ASSUME body would measure the hypothetical state,
                // which we may want eventually but defer for now.
                Err(LocyError::EvaluationError {
                    message: "VALIDATE statements are not yet supported inside ASSUME \
                              blocks; declare VALIDATE at the top level instead"
                        .to_string(),
                })
            }
        }
    })
}
