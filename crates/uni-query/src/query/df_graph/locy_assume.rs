// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! ASSUME block evaluation via `LocyExecutionContext`.
//!
//! Ported from `uni-locy/src/orchestrator/assume.rs`. Uses `LocyExecutionContext`
//! for L0 fork/restore, mutations, and strata re-evaluation.

use std::time::Instant;

use uni_cypher::ast::Query;
use uni_locy::result::CommandResult;
use uni_locy::types::{CompiledAssume, CompiledCommand};
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_delta::RowStore;

use super::locy_explain::ProvenanceStore;
use super::locy_traits::LocyExecutionContext;

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

    // 3. Re-evaluate the parent program's strata in the mutated state
    let mut assume_derived_store: RowStore = ctx.re_evaluate_strata(parent_program, config).await?;
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
        let body_program = CompiledProgram {
            rule_catalog: merged_rule_catalog.clone(),
            strata: combined_strata,
            ..assume.body_program.clone()
        };
        let body_store = ctx.re_evaluate_strata(&body_program, config).await?;
        for (name, rel) in body_store {
            assume_derived_store.insert(name, rel);
        }
    }

    // The parent's strata with the merged catalog: body commands must be able
    // to name the body's own rules, and a nested ASSUME must still see the
    // parent's strata as its own parent, so the two are combined rather than
    // one replacing the other.
    let command_program = CompiledProgram {
        rule_catalog: merged_rule_catalog,
        ..parent_program.clone()
    };

    // 5. Execute body commands and collect results
    let mut result_rows = Vec::new();
    let assume_start = Instant::now();
    for cmd in &assume.body_commands {
        let cmd_result = dispatch_body_command(
            cmd,
            &command_program,
            ctx,
            config,
            &mut assume_derived_store,
            stats,
            assume_start,
        )
        .await?;
        match cmd_result {
            CommandResult::Query(rows) => result_rows.extend(rows),
            CommandResult::Cypher(rows) => result_rows.extend(rows),
            _ => {}
        }
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
    derived_store: &'a mut RowStore,
    stats: &'a mut LocyStats,
    start: Instant,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<CommandResult, LocyError>> + Send + 'a>,
> {
    Box::pin(async move {
        match cmd {
            CompiledCommand::GoalQuery(gq) => {
                // For FOLD rules (MNOR/MPROD), the SLG resolver does not apply
                // post-fixpoint aggregation and would return raw pre-FOLD match rows.
                // Use the pre-computed `derived_store` (which ran the full native fixpoint
                // including FOLD aggregation and VID→Node enrichment).
                let rule_name_str = gq.rule_name.to_string();
                let is_fold_rule = program
                    .rule_catalog
                    .get(&rule_name_str)
                    .map(|r| r.clauses.iter().any(|c| !c.fold.is_empty()))
                    .unwrap_or(false);

                let fold_relation = if is_fold_rule {
                    derived_store.get(&rule_name_str)
                } else {
                    None
                };
                if let Some(relation) = fold_relation {
                    let rows = relation.rows.clone();
                    // Apply the QUERY WHERE filter before RETURN — this FOLD path
                    // (like the one in `locy_query::evaluate_query`) does not reach
                    // the shared filter, so omitting it silently ignored the
                    // predicate for FOLD rules queried inside an ASSUME block.
                    let filtered = super::locy_query::filter_where(
                        rows,
                        gq.where_expr.as_ref(),
                        &config.params,
                    );
                    let projected = super::locy_query::apply_return_clause(
                        filtered,
                        &gq.return_clause,
                        &config.params,
                    )
                    .map_err(|e| LocyError::QueryResolutionError {
                        message: format!("ASSUME FOLD query projection: {e}"),
                    })?;
                    return Ok(CommandResult::Query(projected));
                }

                let rows = super::locy_query::evaluate_query(
                    gq,
                    program,
                    ctx,
                    config,
                    derived_store,
                    stats,
                    start,
                )
                .await?;
                Ok(CommandResult::Query(rows))
            }
            CompiledCommand::DeriveCommand(dc) => {
                let affected = super::locy_derive::derive_command(dc, program, ctx, stats).await?;
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
                    None,
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
