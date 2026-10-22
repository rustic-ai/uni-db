// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runs Locy commands in sequence against the state earlier commands leave.
//!
//! The commands of a program, and the commands of an `ASSUME ... THEN` body,
//! run one after another: a command that writes (`DERIVE`, a Cypher write)
//! changes the graph, and every later command that reads facts reads them
//! derived again in the new state. [`CommandSequence`] holds the facts of the
//! current state and the writes not yet folded into them; both dispatch loops
//! (`impl_locy.rs` at the top level, [`super::locy_assume`] in a body) use it.
//!
//! # Incremental re-derivation
//!
//! The state after a write differs from the state before it only by the
//! write. A stratum the write cannot affect
//! ([`uni_locy::incremental::affected_strata`]) has the same facts in both
//! states, so it is copied from the facts before the write and only the
//! affected strata are derived, from scratch. The same criterion serves the
//! hypothetical states of `ASSUME` and `ABDUCE`
//! (`re_evaluate_hypothetical`).
//!
//! Affected strata are recomputed in full rather than seeded from the
//! change: a delta-seeded fixpoint would need the delta of every base clause
//! (the matches the mutation added or removed), which no operator computes,
//! and is unsound for deletions, negation and aggregates anyway.
//!
//! # Laziness
//!
//! A write only marks the facts stale; they are derived again when a later
//! command reads them. A Cypher command reads the graph, not the facts, so a
//! program ending in `DERIVE` or a Cypher command derives nothing after its
//! last write, and consecutive writes are folded into one re-derivation.

// Rust guideline compliant

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use uni_locy::incremental::{MutationFootprint, affected_strata};
use uni_locy::types::CompiledCommand;
use uni_locy::{CompiledProgram, LocyConfig, LocyError, LocyStats};

use super::locy_delta::RowStore;
use super::locy_explain::ProvenanceStore;
use super::locy_program::{DerivedStore, ReusedStrata};
use super::locy_traits::{LocyExecutionContext, ReEvaluation, StrataBaseline};

/// Where a command sequence runs, which decides how its re-derivations are
/// run and counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceScope {
    /// A program's top-level commands, whose writes are real (a
    /// transaction's, or a session program's ephemeral overlay).
    TopLevel,
    /// An `ASSUME` body or an `ABDUCE` candidate check, whose writes are
    /// discarded with the hypothetical state.
    Hypothetical,
}

/// The programs a command sequence's facts come from.
#[derive(Debug)]
pub struct SequencePrograms<'a> {
    /// The program whose strata the commands read.
    pub parent: &'a CompiledProgram,
    /// The parent's strata followed by an `ASSUME` body's own rules, when
    /// the body defines any; `None` at the top level.
    pub body: Option<&'a CompiledProgram>,
}

/// The facts a command reads, in one state.
pub struct CommandFacts {
    /// The parent program's raw facts, for a later state or a nested
    /// hypothetical command to reuse; `None` when the adapter does not keep
    /// them.
    parent_facts: Option<Arc<DerivedStore>>,
    /// The parent program's facts as hydrated rows, kept with
    /// `parent_facts`. At the top level the same rows as [`Self::store`].
    parent_rows: Option<Arc<RowStore>>,
    /// Every rule's rows: the parent's, with a body's own on top.
    store: Arc<RowStore>,
    /// The adapter's [`LocyExecutionContext::base_state_token`] when these
    /// facts were derived.
    base: Option<u64>,
    /// The provenance recorded while deriving these facts, when the program
    /// records provenance; `None` for a program's own facts, whose
    /// provenance the adapter holds.
    tracker: Option<Arc<ProvenanceStore>>,
    /// The rules whose groups fell back to approximate probabilities while
    /// deriving these facts; `None` for a program's own facts.
    approximate: Option<HashMap<String, Vec<String>>>,
}

impl std::fmt::Debug for CommandFacts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut rules: Vec<&String> = self.store.keys().collect();
        rules.sort();
        f.debug_struct("CommandFacts")
            .field("rules", &rules)
            .field("raw_facts", &self.parent_facts.is_some())
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl CommandFacts {
    /// The facts a program's own evaluation derived, before its first
    /// command: `facts` raw (when kept), `store` hydrated, both in the state
    /// `base` identifies.
    #[must_use]
    pub fn top_level(
        facts: Option<Arc<DerivedStore>>,
        store: Arc<RowStore>,
        base: Option<u64>,
    ) -> Self {
        Self {
            parent_facts: facts,
            parent_rows: Some(Arc::clone(&store)),
            store,
            base,
            tracker: None,
            approximate: None,
        }
    }

    /// The provenance recorded while deriving these facts, when they were
    /// derived again (after a write, in a hypothetical state) by a program
    /// that records provenance.
    #[must_use]
    pub fn tracker(&self) -> Option<&Arc<ProvenanceStore>> {
        self.tracker.as_ref()
    }

    /// The rules whose groups fell back to approximate probabilities while
    /// deriving these facts again; `None` for a program's own facts.
    #[must_use]
    pub fn approximate(&self) -> Option<&HashMap<String, Vec<String>>> {
        self.approximate.as_ref()
    }

    /// Every rule's facts as hydrated rows.
    #[must_use]
    pub fn store(&self) -> &RowStore {
        &self.store
    }

    /// The parent program's raw facts, when held.
    #[must_use]
    pub fn raw(&self) -> Option<&DerivedStore> {
        self.parent_facts.as_deref()
    }

    /// Every rule's facts as hydrated rows, shared.
    #[must_use]
    pub fn into_store(self) -> Arc<RowStore> {
        self.store
    }

    /// Every rule's facts as hydrated rows, shared, keeping `self`.
    #[must_use]
    pub fn shared_store(&self) -> Arc<RowStore> {
        Arc::clone(&self.store)
    }

    /// The parent's facts as the baseline of the next state.
    fn baseline(&self) -> Option<StrataBaseline> {
        Some(StrataBaseline {
            facts: self.parent_facts.clone()?,
            rows: self.parent_rows.clone(),
        })
    }
}

/// Whether `cmd` reads the derived facts, so it must see them in the
/// current state. A Cypher command and `CALIBRATE` read the graph instead.
#[must_use]
pub fn reads_facts(cmd: &CompiledCommand) -> bool {
    !matches!(
        cmd,
        CompiledCommand::Cypher(_) | CompiledCommand::Calibrate(_)
    )
}

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

/// Re-evaluates `program` in `scope` and records what was derived and what
/// reused.
async fn re_evaluate_counted(
    program: &CompiledProgram,
    scope: SequenceScope,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    reused: Option<ReusedStrata>,
    stats: &mut LocyStats,
) -> Result<ReEvaluation, LocyError> {
    let result = match scope {
        SequenceScope::TopLevel => ctx.re_evaluate_sequenced(program, config, reused).await?,
        SequenceScope::Hypothetical => {
            ctx.re_evaluate_strata_reusing(program, config, reused)
                .await?
        }
    };
    let evaluated = program.strata.len() - result.reused.len();
    match scope {
        SequenceScope::TopLevel => {
            stats.sequenced_strata_reused += result.reused.len();
            stats.sequenced_strata_evaluated += evaluated;
        }
        SequenceScope::Hypothetical => {
            stats.hypothetical_strata_reused += result.reused.len();
            stats.hypothetical_strata_evaluated += evaluated;
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
        }
    }
    tracing::debug!(
        strata = program.strata.len(),
        reused = result.reused.len(),
        ?scope,
        "locy.sequence.re_evaluated"
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
    re_evaluate_counted(
        program,
        SequenceScope::Hypothetical,
        ctx,
        config,
        reused,
        stats,
    )
    .await
}

/// Derives the facts commands read in the current state: the state
/// `baseline` was derived in, changed by a mutation with footprint
/// `footprint`.
///
/// The parent's strata the mutation cannot affect are reused from
/// `baseline`; a body's own strata are always derived, on top of the
/// parent's.
///
/// # Errors
///
/// Whatever the re-evaluation raises.
pub(crate) async fn derive_command_facts(
    programs: &SequencePrograms<'_>,
    scope: SequenceScope,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    baseline: Option<&StrataBaseline>,
    footprint: &MutationFootprint,
    stats: &mut LocyStats,
) -> Result<CommandFacts, LocyError> {
    // Taken before deriving: a change of the base state during the
    // derivation then shows as a mismatch the next time.
    let base = ctx.base_state_token();
    let reused = baseline.and_then(|b| reuse_plan(programs.parent, b, footprint));
    let parent = re_evaluate_counted(programs.parent, scope, ctx, config, reused, stats).await?;
    stats.queries_executed += 1; // rough accounting for the re-evaluation
    let parent_facts = parent.facts;
    let Some(body_program) = programs.body else {
        let store = Arc::new(parent.rows);
        return Ok(CommandFacts {
            parent_facts,
            parent_rows: Some(Arc::clone(&store)),
            store,
            base,
            tracker: parent.tracker,
            approximate: Some(parent.approximate),
        });
    };
    // Kept for nested hypothetical commands only when there is a state to offer.
    let parent_rows = parent_facts.as_ref().map(|_| Arc::new(parent.rows.clone()));
    let mut store: RowStore = parent.rows;
    // The parent's strata were derived in this very state a moment ago;
    // only the body's own strata are new. Positional, not by name: a body
    // rule may shadow a parent rule's name.
    let reused = parent_facts.clone().map(|facts| ReusedStrata {
        facts,
        positions: (0..programs.parent.strata.len()).collect(),
        // Hydrated in this very state.
        hydrated: parent_rows.clone(),
    });
    let body = re_evaluate_counted(body_program, scope, ctx, config, reused, stats).await?;
    for (name, rel) in body.rows {
        store.insert(name, rel);
    }
    // The body's run covers the parent's strata too (copied, or derived
    // again when provenance is recorded), so its provenance is the state's.
    let mut approximate = parent.approximate;
    approximate.extend(body.approximate);
    Ok(CommandFacts {
        parent_facts,
        parent_rows,
        store: Arc::new(store),
        base,
        tracker: body.tracker.or(parent.tracker),
        approximate: Some(approximate),
    })
}

/// Offers `facts` as the baseline a nested hypothetical command reuses;
/// returns whether one was pushed.
fn push_baseline(ctx: &dyn LocyExecutionContext, facts: &CommandFacts) -> bool {
    match facts.baseline() {
        Some(baseline) => {
            ctx.push_reuse_baseline(baseline);
            true
        }
        None => false,
    }
}

/// The facts of a command sequence's current state, derived again on demand
/// after a write.
#[derive(Debug)]
pub struct CommandSequence<'a> {
    programs: SequencePrograms<'a>,
    scope: SequenceScope,
    facts: CommandFacts,
    /// The writes since `facts` were derived, merged; `None` while they are
    /// current.
    pending: Option<MutationFootprint>,
    /// Whether each state's facts are offered to nested hypothetical
    /// commands as their baseline.
    offer_baseline: bool,
    /// Whether a baseline of this sequence is on the adapter's stack.
    pushed_baseline: bool,
}

impl<'a> CommandSequence<'a> {
    /// A sequence starting from `facts`, the facts of the current state.
    ///
    /// `offer_baseline` makes every later state's facts the baseline nested
    /// hypothetical commands reuse; `pushed_baseline` says the current
    /// state's already is (the sequence pops it when it moves on).
    #[must_use]
    pub fn new(
        programs: SequencePrograms<'a>,
        scope: SequenceScope,
        facts: CommandFacts,
        offer_baseline: bool,
        pushed_baseline: bool,
    ) -> Self {
        Self {
            programs,
            scope,
            facts,
            pending: None,
            offer_baseline,
            pushed_baseline,
        }
    }

    /// A sequence starting from `facts` that offers each state's facts to
    /// nested hypothetical commands, starting with these.
    #[must_use]
    pub(crate) fn offering(
        programs: SequencePrograms<'a>,
        scope: SequenceScope,
        facts: CommandFacts,
        ctx: &dyn LocyExecutionContext,
    ) -> Self {
        let pushed = push_baseline(ctx, &facts);
        Self::new(programs, scope, facts, true, pushed)
    }

    /// Records that a command wrote, with footprint `footprint`: the facts
    /// are stale until derived again.
    pub fn record_write(&mut self, footprint: MutationFootprint) {
        match &mut self.pending {
            Some(pending) => pending.extend(&footprint),
            None => self.pending = Some(footprint),
        }
    }

    /// Whether a write happened since the facts were last derived.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.pending.is_some()
    }

    /// The facts as they stood when last derived, without deriving them
    /// again: for a command that does not read them.
    #[must_use]
    pub fn last_derived(&self) -> &CommandFacts {
        &self.facts
    }

    /// The facts of the current state, derived again first if a command
    /// wrote since they were last derived.
    ///
    /// The strata the writes cannot affect are copied from the previous
    /// state, unless the base state outside this evaluation's own writes
    /// moved in between (another transaction committed), in which case
    /// everything is derived.
    ///
    /// # Errors
    ///
    /// Whatever the re-derivation raises.
    pub async fn facts(
        &mut self,
        ctx: &dyn LocyExecutionContext,
        config: &LocyConfig,
        stats: &mut LocyStats,
    ) -> Result<&CommandFacts, LocyError> {
        if let Some(written) = self.pending.take() {
            let before = if self.facts.base == ctx.base_state_token() {
                self.facts.baseline()
            } else {
                None
            };
            self.facts = derive_command_facts(
                &self.programs,
                self.scope,
                ctx,
                config,
                before.as_ref(),
                &written,
                stats,
            )
            .await?;
            if self.pushed_baseline {
                ctx.pop_reuse_baseline();
                self.pushed_baseline = false;
            }
            if self.offer_baseline {
                self.pushed_baseline = push_baseline(ctx, &self.facts);
            }
        }
        Ok(&self.facts)
    }

    /// Ends the sequence, removing its baseline from the adapter, and
    /// returns the facts as they stood when last derived.
    pub fn finish(self, ctx: &dyn LocyExecutionContext) -> CommandFacts {
        if self.pushed_baseline {
            ctx.pop_reuse_baseline();
        }
        self.facts
    }
}
