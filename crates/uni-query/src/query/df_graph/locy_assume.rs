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
//! block's mutations. A stratum those mutations cannot affect has the same
//! facts in both states, so it is copied from the facts of the state before
//! the mutation (the adapter's baseline) and only the affected strata are
//! derived. `ABDUCE` validates each candidate modification the same way.
//!
//! The body's commands run in sequence with the same machinery as a
//! program's top-level commands ([`super::locy_sequence`]): a body command
//! that writes makes the facts later body commands read stale, and they are
//! derived again, incrementally, when one reads them.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use uni_cypher::ast::Query;
use uni_locy::incremental::{MutationFootprint, command_write_footprint, mutation_footprint};
use uni_locy::types::{CompiledAssume, CompiledCommand};
use uni_locy::{CommandResult, CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_sequence::{
    CommandFacts, CommandSequence, SequencePrograms, SequenceScope, derive_command_facts,
    reads_facts,
};
use super::locy_traits::LocyExecutionContext;

pub(crate) use super::locy_sequence::re_evaluate_hypothetical;

/// A binding error from planning an ASSUME body against its parent's rules.
fn binding_error(e: uni_locy::LocyCompileError) -> LocyError {
    LocyError::EvaluationError {
        message: format!("LocyCompileError: {e}"),
    }
}

/// Evaluate an ASSUME block: fork L0, apply mutations, re-evaluate rules,
/// execute body commands, collect results, then restore.
///
/// Returns a [`CommandResult::Assume`] holding one result per body command,
/// in order, and every rule's facts in the hypothetical state the first body
/// command reads (after the mutations, before any body command).
///
/// # Errors
///
/// A failed fork or restore of the hypothetical state, the mutations, the
/// re-evaluation, or any body command.
pub async fn evaluate_assume(
    assume: &CompiledAssume,
    parent_program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    stats: &mut LocyStats,
) -> Result<CommandResult, LocyError> {
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

    // The body's program: its strata, planned against the merged catalog,
    // appended to the parent's so cross-references resolve. Parent strata
    // come first — stratification order is a dependency order, and a body
    // rule may depend on a parent rule but not the reverse.
    //
    // The body's strata are evaluated **after the parent's, in one run**, not
    // standalone. A body rule may IS-reference an outer rule
    // (`CREATE RULE eligible AS MATCH (p:Person) WHERE p IS adult ...`), and a
    // standalone evaluation of just the body's strata gives that reference no
    // facts to resolve against, so the rule derives nothing.
    let body_program = if assume.body_program.strata.is_empty() {
        None
    } else {
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
        Some(body_program)
    };
    let programs = SequencePrograms {
        parent: parent_program,
        body: body_program.as_ref(),
    };
    // What a body `ABDUCE` validates its candidates against: the same
    // programs the body's facts come from, its own rules included.
    let validation = SequencePrograms {
        parent: parent_program,
        body: body_program.as_ref(),
    };

    // 3. Derive the facts the body reads in the mutated state, reusing the
    // strata the mutations cannot affect.
    let facts = derive_command_facts(
        &programs,
        SequenceScope::Hypothetical,
        ctx,
        config,
        baseline.as_ref(),
        &footprint,
        stats,
    )
    .await?;

    // The parent's strata with the merged catalog: body commands must be able
    // to name the body's own rules, and a nested ASSUME must still see the
    // parent's strata as its own parent, so the two are combined rather than
    // one replacing the other.
    let mut command_program = CompiledProgram {
        rule_catalog: merged_rule_catalog,
        ..parent_program.clone()
    };
    uni_locy::attach_binding_plans(&mut command_program).map_err(binding_error)?;

    // The facts the result reports as `derived`: those of the state the
    // first body command reads, after the mutations and before any body
    // command — the point a program's own `LocyResult::derived` is taken at,
    // before its first command. Kept as a shared handle: the state after the
    // last body command is not derived at all when the body ends in a write
    // (re-derivation is lazy), and deriving it only to report it would cost
    // a re-evaluation no command asked for.
    let derived_store = facts.shared_store();

    // A nested ASSUME starts from the current state, so it reuses the parent
    // strata derived in it. The parent's facts, not the body's: a nested
    // ASSUME re-evaluates `command_program`, whose strata are the parent's,
    // and a body rule may shadow a parent rule's name.
    let mut sequence = CommandSequence::offering(programs, SequenceScope::Hypothetical, facts, ctx);

    // 4. Execute body commands in sequence, one result per command.
    //
    // The commands run as they would one after another in a transaction: a
    // command that writes (DERIVE, a Cypher write) changes the hypothetical
    // graph, so the facts every later command reads are derived again in
    // the new state — only the strata that write can affect, the rest
    // reused. Answering a later QUERY from the facts derived before the write
    // was a silent stale read, while a later Cypher read saw the write.
    let mut results = Vec::with_capacity(assume.body_commands.len());
    for cmd in &assume.body_commands {
        // A cut-short re-evaluation (`allow_partial`) ends the body: the
        // facts left are partial. The program reports the cut and drops the
        // ASSUME's result.
        if ctx.is_cut_short() {
            break;
        }
        let facts = if reads_facts(cmd) {
            sequence.facts(ctx, config, stats).await?
        } else {
            sequence.last_derived()
        };
        if ctx.is_cut_short() {
            break;
        }
        let outcome = dispatch_body_command(
            cmd,
            &command_program,
            &validation,
            ctx,
            config,
            facts,
            stats,
        )
        .await?;
        results.push(outcome.result);
        if let Some(written) = outcome.written {
            sequence.record_write(written);
        }
    }

    // 5. Release the sequence's hold on the facts, then take the derived
    // facts without copying them when nothing else shares them.
    drop(sequence.finish(ctx));
    // Every other holder (the sequence's facts, the baseline it offered)
    // is gone by now, so the facts move out without a copy. A second holder
    // is not wrong, only a silent full copy of every rule's rows.
    debug_assert_eq!(
        Arc::strong_count(&derived_store),
        1,
        "the ASSUME's derived facts are still shared after the body finished"
    );
    let derived: HashMap<String, Vec<FactRow>> = Arc::try_unwrap(derived_store)
        .unwrap_or_else(|shared| (*shared).clone())
        .into_iter()
        .map(|(name, relation)| (name, relation.rows))
        .collect();

    // 6. Restore L0 (discard hypothetical mutations)
    ctx.restore_l0()
        .await
        .map_err(|e| LocyError::SavepointFailed {
            message: format!("failed to restore L0: {}", e),
        })?;

    Ok(CommandResult::Assume { results, derived })
}

/// What a body command produced.
struct BodyOutcome {
    /// Its result, the same a top-level command of its kind reports.
    result: CommandResult,
    /// The footprint of what it wrote to the hypothetical graph, when it
    /// wrote: the facts later commands read must be derived again.
    written: Option<MutationFootprint>,
}

impl BodyOutcome {
    fn read(result: CommandResult) -> Self {
        Self {
            result,
            written: None,
        }
    }
}

/// Dispatch a body command inside an ASSUME block.
///
/// `program` is the parent's strata with the merged catalog, which commands
/// name rules through; `validation` is the programs the body's facts come
/// from, which an `ABDUCE` re-derives in each candidate state; `facts` is
/// the current hypothetical state's facts, with their provenance.
///
/// Uses Box::pin for recursive async (nested ASSUME may dispatch sub-commands).
fn dispatch_body_command<'a>(
    cmd: &'a CompiledCommand,
    program: &'a CompiledProgram,
    validation: &'a SequencePrograms<'a>,
    ctx: &'a dyn LocyExecutionContext,
    config: &'a LocyConfig,
    facts: &'a CommandFacts,
    stats: &'a mut LocyStats,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<BodyOutcome, LocyError>> + Send + 'a>>
{
    Box::pin(async move {
        let derived_store = facts.store();
        match cmd {
            CompiledCommand::GoalQuery(gq) => {
                // The same path as a top-level QUERY: the facts of the
                // current hypothetical state, filtered and projected.
                let rows =
                    super::locy_query::evaluate_query(gq, program, config, derived_store, &|| {
                        ctx.check_interrupt()
                    })?;
                Ok(BodyOutcome::read(CommandResult::Query(rows)))
            }
            CompiledCommand::DeriveCommand(dc) => {
                let (affected, footprint) = super::locy_derive::derive_command_footprint(
                    dc,
                    program,
                    ctx,
                    derived_store,
                    stats,
                )
                .await?;
                Ok(BodyOutcome {
                    result: CommandResult::Derive { affected },
                    written: (affected > 0).then_some(footprint),
                })
            }
            CompiledCommand::ExplainRule(eq) => {
                // The provenance recorded while deriving the hypothetical
                // facts, when the program records provenance (an EXPLAIN
                // anywhere in it does), so the tree is the one a top-level
                // EXPLAIN gives in the same state.
                let node = super::locy_explain::explain_rule(
                    eq,
                    program,
                    ctx,
                    config,
                    derived_store,
                    stats,
                    facts.tracker().map(|t| t.as_ref()),
                    facts.approximate(),
                )
                .await?;
                Ok(BodyOutcome::read(CommandResult::Explain(node)))
            }
            CompiledCommand::Abduce(aq) => {
                // Each candidate is applied on top of the hypothetical state
                // and checked against the body's programs, so a body rule is
                // validated as it is derived here.
                let result = super::locy_abduce::evaluate_abduce_with(
                    aq,
                    program,
                    validation,
                    ctx,
                    config,
                    derived_store,
                    stats,
                )
                .await?;
                Ok(BodyOutcome::read(CommandResult::Abduce(result)))
            }
            CompiledCommand::Assume(ca) => {
                // A nested ASSUME restores the state it started from, so it
                // writes nothing the following commands can see.
                let result = evaluate_assume(ca, program, ctx, config, stats).await?;
                Ok(BodyOutcome::read(result))
            }
            CompiledCommand::Cypher(q) => {
                let rows = ctx.execute_cypher_read(q.clone()).await?;
                stats.queries_executed += 1;
                Ok(BodyOutcome {
                    result: CommandResult::Cypher(rows),
                    written: command_write_footprint(q),
                })
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
