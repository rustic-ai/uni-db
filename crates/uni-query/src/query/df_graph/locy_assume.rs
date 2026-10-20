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
use uni_locy::incremental::{
    MutationFootprint, affected_strata, mutation_footprint, query_footprint,
};
use uni_locy::types::{CompiledAssume, CompiledCommand};
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_delta::RowStore;
use super::locy_program::{DerivedStore, ReusedStrata};
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

/// The programs an ASSUME body's facts come from.
struct BodyPrograms<'a> {
    /// The enclosing program, whose strata the body's commands read.
    parent: &'a CompiledProgram,
    /// The parent's strata followed by the body's own rules, when the body
    /// defines any.
    body: Option<&'a CompiledProgram>,
}

/// The facts an ASSUME body's commands read, in one hypothetical state.
struct BodyFacts {
    /// The parent program's raw facts, for a later state or a nested ASSUME
    /// to reuse; `None` when the adapter does not keep them.
    parent_facts: Option<std::sync::Arc<DerivedStore>>,
    /// The parent program's facts as hydrated rows, kept with `parent_facts`.
    parent_rows: Option<std::sync::Arc<RowStore>>,
    /// Every rule's rows: the parent's, with the body's own on top.
    store: RowStore,
}

impl BodyFacts {
    /// The parent's facts as the baseline of the next state.
    fn baseline(&self) -> Option<StrataBaseline> {
        self.parent_facts.clone().map(|facts| StrataBaseline {
            facts,
            rows: self.parent_rows.clone(),
        })
    }
}

/// Offers `facts` as the baseline a nested ASSUME reuses; returns whether
/// one was pushed.
fn push_baseline(ctx: &dyn LocyExecutionContext, facts: &BodyFacts) -> bool {
    match facts.baseline() {
        Some(baseline) => {
            ctx.push_reuse_baseline(baseline);
            true
        }
        None => false,
    }
}

/// Derives the facts an ASSUME body reads in the current hypothetical state:
/// the state `baseline` was derived in, changed by a mutation with footprint
/// `footprint`.
///
/// The parent's strata the mutation cannot affect are reused from `baseline`;
/// the body's own strata are always derived, on top of the parent's.
async fn derive_body_facts(
    programs: &BodyPrograms<'_>,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    baseline: Option<&StrataBaseline>,
    footprint: &MutationFootprint,
    stats: &mut LocyStats,
) -> Result<BodyFacts, LocyError> {
    let parent =
        re_evaluate_hypothetical(programs.parent, ctx, config, baseline, footprint, stats).await?;
    stats.queries_executed += 1; // rough accounting for the re-evaluation
    let parent_facts = parent.facts;
    // Kept for nested hypothetical commands only when there is a state to offer.
    let parent_rows = parent_facts
        .as_ref()
        .map(|_| std::sync::Arc::new(parent.rows.clone()));
    let mut store: RowStore = parent.rows;
    if let Some(body_program) = programs.body {
        // The parent's strata were derived in this very state a moment ago;
        // only the body's own strata are new. Positional, not by name: a body
        // rule may shadow a parent rule's name.
        let reused = parent_facts.clone().map(|facts| ReusedStrata {
            facts,
            positions: (0..programs.parent.strata.len()).collect(),
            // Hydrated in this very state.
            hydrated: parent_rows.clone(),
        });
        let body = re_evaluate_counted(body_program, ctx, config, reused, stats).await?;
        for (name, rel) in body.rows {
            store.insert(name, rel);
        }
    }
    Ok(BodyFacts {
        parent_facts,
        parent_rows,
        store,
    })
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
    let programs = BodyPrograms {
        parent: parent_program,
        body: body_program.as_ref(),
    };

    // 3. Derive the facts the body reads in the mutated state, reusing the
    // strata the mutations cannot affect.
    let mut facts =
        derive_body_facts(&programs, ctx, config, baseline.as_ref(), &footprint, stats).await?;

    // The parent's strata with the merged catalog: body commands must be able
    // to name the body's own rules, and a nested ASSUME must still see the
    // parent's strata as its own parent, so the two are combined rather than
    // one replacing the other.
    let mut command_program = CompiledProgram {
        rule_catalog: merged_rule_catalog,
        ..parent_program.clone()
    };
    uni_locy::attach_binding_plans(&mut command_program).map_err(binding_error)?;

    // A nested ASSUME starts from the current state, so it reuses the parent
    // strata derived in it. The parent's facts, not the body's: a nested
    // ASSUME re-evaluates `command_program`, whose strata are the parent's,
    // and a body rule may shadow a parent rule's name.
    let mut pushed_baseline = push_baseline(ctx, &facts);

    // 4. Execute body commands in sequence and collect results.
    //
    // The commands run as they would one after another in a transaction: a
    // command that writes (DERIVE, a Cypher write) changes the hypothetical
    // graph, so the facts every later command reads are derived again in
    // the new state — only the strata that write can affect, the rest
    // reused. Answering a later QUERY from the facts derived before the write
    // was a silent stale read, while a later Cypher read saw the write.
    let mut result_rows = Vec::new();
    for cmd in &assume.body_commands {
        let outcome =
            dispatch_body_command(cmd, &command_program, ctx, config, &facts.store, stats).await?;
        result_rows.extend(outcome.rows);
        let Some(written) = outcome.written else {
            continue;
        };
        let before = facts.baseline();
        facts = derive_body_facts(&programs, ctx, config, before.as_ref(), &written, stats).await?;
        if pushed_baseline {
            ctx.pop_reuse_baseline();
        }
        pushed_baseline = push_baseline(ctx, &facts);
    }

    if pushed_baseline {
        ctx.pop_reuse_baseline();
    }

    // 5. A body with no commands returns every derived fact.
    if assume.body_commands.is_empty() {
        for relation in facts.store.values() {
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

/// What a body command produced.
struct BodyOutcome {
    /// The rows it contributes to the ASSUME's result.
    rows: Vec<FactRow>,
    /// The footprint of what it wrote to the hypothetical graph, when it
    /// wrote: the facts later commands read must be derived again.
    written: Option<MutationFootprint>,
}

impl BodyOutcome {
    fn read(rows: Vec<FactRow>) -> Self {
        Self {
            rows,
            written: None,
        }
    }
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
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<BodyOutcome, LocyError>> + Send + 'a>>
{
    Box::pin(async move {
        match cmd {
            CompiledCommand::GoalQuery(gq) => {
                // The same path as a top-level QUERY: the facts of the
                // current hypothetical state, filtered and projected.
                let rows = super::locy_query::evaluate_query(gq, program, config, derived_store)?;
                Ok(BodyOutcome::read(rows))
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
                // A top-level DERIVE reports `CommandResult::Derive { affected }`;
                // in a body, whose result is rows, that is one `affected` row.
                // Dropping it left a DERIVE-only body silently empty.
                let row: FactRow = [(
                    "affected".to_string(),
                    uni_common::Value::Int(affected as i64),
                )]
                .into();
                Ok(BodyOutcome {
                    rows: vec![row],
                    written: (affected > 0).then_some(footprint),
                })
            }
            CompiledCommand::ExplainRule(_) | CompiledCommand::Abduce(_) => {
                // The compiler rejects both in an ASSUME body
                // (`LocyCompileError::AssumeBodyCommandNotAllowed`): their
                // results are not rows, so an ASSUME would compute them and
                // drop them. A hand-built program reaching here fails the same
                // way rather than running a discarded command.
                Err(LocyError::EvaluationError {
                    message: "ABDUCE and EXPLAIN RULE are not allowed inside an ASSUME body; \
                              run them at the top level after the ASSUME"
                        .to_string(),
                })
            }
            CompiledCommand::Assume(ca) => {
                // A nested ASSUME restores the state it started from, so it
                // writes nothing the following commands can see.
                let rows = evaluate_assume(ca, program, ctx, config, stats).await?;
                Ok(BodyOutcome::read(rows))
            }
            CompiledCommand::Cypher(q) => {
                let rows = ctx.execute_cypher_read(q.clone()).await?;
                stats.queries_executed += 1;
                let footprint = query_footprint(q);
                let wrote = !footprint.is_empty() || !footprint.preserves_existing_entities();
                Ok(BodyOutcome {
                    rows,
                    written: wrote.then_some(footprint),
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
