# Proposal: what the resolution program still pays for

Status: **implemented**, with five changes from the design below; see §10.
Date: 2026-11-14

Follows `docs/proposals/locy_similarity_join_330.md` (#330), §10.5 and §10.6.
Scope: `uni-locy`, `uni-query` (Locy runtime, planner, subquery execution),
`uni` (`impl_locy.rs`), `uni-store` (L0 scan). No rule, grammar or result
changes except where §8 says a choice is open.

## 1. Summary

After the two fixes of #330 the entity-resolution program
(`website/docs/features/nlp-extraction.md`) takes 20.4 s at 1,000 mentions ×
1,000 entities in a debug build and 5.9 s in release. A sampling profile says
where the rest goes. Scoring is under 5% of it. Four things the program does
not ask for account for most of it:

1. **Provenance.** One `MNOR` in the program switches proof recording on for
   every rule. It is 70% of rule evaluation, and with the default
   configuration it changes no result.
2. **Hydration.** Every rule's facts are turned into rows and filled with
   full nodes, embeddings included, three times per program. The commands
   read 500 of the 210,757 facts.
3. **`NOT EXISTS` once per row.** The new-entity `MERGE` runs its subquery,
   with a scan of every mention, for each of 1,000 outer rows.
4. **One statement per derived fact.** `DERIVE` plans and runs a Cypher
   statement for each of its 500 edges.

The plan is five pieces of work, each useful on its own, in this order:

| | Work | Section |
|---|---|---|
| W0 | Phase timers in the Locy profile | §4 |
| W1 | Record provenance only where something reads it | §5 |
| W2 | Convert and hydrate only what is read | §6 |
| W3 | `[NOT] EXISTS` as a semi- or anti-join | §7 |
| W4 | `DERIVE` writes its facts in one statement | §7.4 |

None of this changes how the cost grows: `match_evidence` is still mentions ×
entities facts. That is step 3 of the #330 proposal and stays deferred.

## 2. What was measured

Debug build, `nlp_resolve::resolution_program_stage_times`, 1,000 × 1,000,
one type, 64-dimensional auto-embedded vectors, everything in one
transaction (so every vertex is in L0). One run of each figure.

### 2.1 The program

| | Time |
|---|---|
| Whole program, one `locy` call | 20.39 s |
| of which its first evaluation of the strata | 7.39 s |
| First `DERIVE refers_to`, run alone | 12.24 s (strata 7.42 s) |
| New-entity `MERGE`, run alone | 10.69 s |
| Second `DERIVE refers_to`, run alone | 4.27 s (strata 2.10 s) |

### 2.2 The profile

`sample`, 5 ms interval, over one run of the probe: the 300 × 300 and the
1,000 × 1,000 sizes, each as separate steps and then as one program. Figures
are inclusive seconds summed over that whole run, so they give proportions,
not the cost of one program. `ExistsExecExpr::evaluate` is on two threads'
stacks at once (the caller waits on the worker), so its raw 27.0 s is halved
here; its children add up to about 13.

| Function | Seconds | What it is |
|---|---|---|
| `run_program` | 16.6 | all rule evaluation |
| `record_provenance_nonrecursive` | 5.7 | per-row proof annotations |
| `LineageStore::correct` | 4.7 | shared-proof analysis |
| `LineageStore::record_rule` | 0.7 | lineage rows |
| `similar_to` scoring | 1.5 | |
| `enrich_vids_with_nodes` | 5.1 | hydration |
| of which `substitute_entities` | 2.4 | node clones into rows |
| of which `unique_vids_in` | 1.3 | id collection, done twice |
| of which `lookup_vertices` | 1.3 | the scan by id |
| `record_batches_to_locy_rows` | 1.5 | batches to rows |
| `derive_command_footprint` | 4.1 | the DERIVE command |
| of which `execute_mutation` | 3.1 | one statement per fact |
| `ExistsExecExpr::evaluate` | 13.5 | the `NOT EXISTS` |
| of which `columnar_scan_vertex_batch` | 6.7 | scanning `Mention` per row |
| of which `build_l0_vertex_batch` | 5.0 | rebuilding the L0 batch per row |
| of which `PatternExistsExecExpr` | 4.8 | `NOT (o)-[:REFERS_TO]->()` per row |
| of which physical planning | 0.8 | a plan per row |

### 2.3 One experiment

With `needs_provenance_tracker` forced to false for this program (a
throwaway edit, reverted), same probe:

| | Tracker on | Tracker off |
|---|---|---|
| First pass, strata | 7.42 s | 2.16 s |
| First pass, `same_as` | 5.1 s | 0.74 s |
| First pass, `match_evidence` | 2.3 s | 1.24 s |
| First `DERIVE refers_to`, run alone | 12.24 s | 6.33 s |
| Whole program | 20.39 s | 20.10 s |

The program's links were the same (`assert_scale_resolved` passed).

The last row is not explained. The first evaluation got 5.2 s faster and the
whole program 0.3 s. Either the run varies by seconds (the `MERGE` alone
measured between 10.4 s and 13.2 s across runs), or something else got slower
without a tracker; a tracker also disables the baseline that stratum reuse
keeps (`offer_baseline = tracker.is_none()`, `impl_locy.rs`). The profile
cannot split a program by phase, which is why W0 comes first.

Not measured: a release build of any of this beyond the totals in #330
§10.6, flushed data, more than one entity type, and anything in §5–§7 as
built. Every saving quoted below is an estimate from §2.2 unless it says
"measured".

## 3. Findings

### 3.1 Provenance is program-wide and mostly unread

`CompiledProgram::needs_provenance_tracker` (`uni-locy/src/types.rs`) is true
when any rule folds with `MNOR` or `MPROD`, or the program has an
`EXPLAIN RULE`. When it is true:

- the planner projects the hidden support columns for every rule
  (`LocyPlanBuilder::with_provenance`);
- `record_provenance_nonrecursive` (`locy_fixpoint.rs`) runs for every
  non-recursive rule, `match_evidence`'s 209,757 rows included. Per row it
  decodes every column into a `HashMap<String, Value>`, copies that map twice
  more, formats the key with `{:?}`, and takes the store's write lock;
- `LineageStore::correct` (`locy_lineage.rs`) runs once per probability-fold
  rule: Tarjan over the lineage graph and a support set per fact;
- no stratum is reused after a write.

Who reads the result:

- **The annotations** (`ProvenanceStore`): only `EXPLAIN RULE` in the same
  program, top-level or in an `ASSUME` body. `ABDUCE`, `VALIDATE`, `DERIVE`
  and `QUERY` do not. `LocyResult` carries none of it.
- **The lineage**: `correct` rewrites probabilities only under
  `exact_probability` or the `TopKProofs` semiring. With the defaults it
  returns its input unchanged; its product is two warnings
  (`SharedProbabilisticDependency`, `CrossGroupCorrelationNotExact`).

For `same_as` in particular the lineage is trivial. Its one reference is to
`match_evidence`, which has no `PROB` column, so every row's reference list
is empty and each fact's rows are independent by construction. The analysis
cannot find sharing there.

### 3.2 Every rule is converted and hydrated, three times

In `evaluate_compiled_capturing_inner` (`uni/src/api/impl_locy.rs`):

- step 7 converts every rule's batches to rows
  (`native_store_to_row_store`), and step 7b clones those rows and hydrates
  them (`enrich_vids_with_nodes`);
- a command that reads facts after a write re-derives, and `re_evaluate_with`
  converts and hydrates every rule again;
- when any command may write, step 9 converts the initial store again and
  hydrates it in the post-command state, for `LocyResult::derived`.

Hydration replaces each id cell with a clone of the whole node. `Node` holds
its properties in a `HashMap` with no sharing, so each `KEY m` / `KEY e` cell
of each fact gets its own copy, the 64-dimensional embedding included: about
420,000 node clones per pass for `match_evidence`. `unique_vids_in` walks
each rule's rows twice per kind.

What reads it: `DERIVE refers_to` reads `resolved`, 500 facts. `LocyResult`
exposes every rule's facts through `derived_rules()`, read or not. There is
no option to leave any out, and the comment in the code says so: every rule
is hydrated because `derived` is public.

### 3.3 `NOT EXISTS` runs its body once per outer row

`ExistsExecExpr::evaluate` (`expr_compiler.rs`) plans the body logically once
per batch, then for each outer row builds a physical planner, plans, and
executes. Each execution scans every `Mention`. Three things make that
expensive here:

- **1,000 executions, not 500.** The outer `WHERE` is
  `NOT (m)-[:REFERS_TO]->() AND NOT EXISTS {...}`. DataFusion evaluates the
  right side of an `AND` on a selection only when at most 20% of the rows
  pass the left. Half pass, so the subquery runs for all 1,000. Read from the
  code, not counted.
- **The L0 batch is rebuilt each time, with every property.**
  `build_l0_vertex_batch` (`uni-store/src/runtime/columnar_scan.rs`) clones
  all properties of every unflushed vertex of the label into a map, the
  embedding included, and only then builds the projected columns. The Lance
  side of the scan is projected to `type` and `text`.
- **Nothing decorrelates it.** The planner has no semi- or anti-join for
  `EXISTS`. The one existing shortcut (`exists_decorrelate.rs`, one execution
  per source) needs the outer node to appear bare in the body's pattern; here
  it appears only in the `WHERE`.

`COUNT { }` and `COLLECT { }` share this code.

### 3.4 `DERIVE` writes fact by fact

`derive_command_footprint` (`locy_derive.rs`) builds one
`MATCH (m), (e) WHERE id(m) = X AND id(e) = Y CREATE (m)-[:REFERS_TO]->(e)`
per fact and plans and runs each. Before that it copies both endpoints'
properties into a `DerivedEdge` per fact, which the transaction path then
discards. Re-running the rule's body (`resolve_clause_with_is_refs`) narrows
the `MATCH` by an `IN` list per variable, so it reads up to
distinct-m × distinct-e rows before cutting them to the facts' pairs.

### 3.5 The profile does not see most of this

`LocyExecProfile` times strata, rules and operators. Provenance is inside a
rule's time but is no operator, which is why `same_as` showed 5 s with 1 s of
operators. Conversion, hydration and command dispatch are outside it
altogether.

## 4. W0: phase timers

Add to `LocyExecProfile`, per program:

- conversion, hydration and result-building time, each with the rule count
  and fact count it covered, per pass (initial, re-evaluation, result);
- per command: kind, time, statements executed, whether it triggered a
  re-evaluation, strata evaluated and reused;
- per rule: provenance and lineage time, beside the operators.

Cost: `Instant` pairs at phase boundaries, only when profiling is on. This
makes every later claim checkable from one run and settles §2.3.

## 5. W1: provenance where something reads it

Split the one flag into the two needs it bundles, and decide the second per
rule.

**Annotations.** Record them only when the program has an `EXPLAIN RULE`,
top-level or in an `ASSUME` body: `commands_need_provenance_tracker`, which
exists. `record_provenance_nonrecursive` and `record_provenance` then run
only for such programs. `EXPLAIN RULE` in a later program re-evaluates and is
unaffected; Mode B (re-execution) already covers a rule with no entries.

**Lineage.** Keep it for a probability-fold rule, and for the rules it
depends on, only when a row of that rule can carry a probabilistic
reference: the rule has a positive `IS` to a rule with a `PROB` column, a
probabilistic `IS NOT`, or a `PROB` factor. Decide this at compile time from
the rule's clauses. A fold rule with none of these is read-once by
construction; skip `record_rule` and `correct` for it in every mode,
`exact_probability` included, since there is nothing to correct.

**The planner flag** follows the lineage need, per rule, not the program.

**Stratum reuse.** `offer_baseline` and the sequenced re-evaluation refuse to
reuse a stratum when a tracker exists. Narrow that to strata holding a rule
that records lineage.

Results: unchanged in every mode. Warnings: unchanged, because a rule that
cannot share a proof never produced them.

Estimated effect: the measured 7.42 s → 2.16 s of §2.3 for this program's
first pass. A program with `EXPLAIN RULE`, or with real probabilistic
references, keeps today's cost; making the recording itself cheaper (one map
per row, no lock per row) is a separate, smaller change, worth doing only if
W0 shows such programs matter.

Tests: `needs_provenance_tracker`'s two unit tests split into the two needs;
every test in `locy_explain_children`, `locy_proof_probability`,
`locy_issue_315_dependency_diagnostics`, `locy_prob_is_ref_semantics`,
`repro_locy_runtime_wmc_shared_lineage` and the `TopKProofs` TCK features
must pass unchanged; a new test that a program with an `MNOR` over a
`PROB`-less rule records no annotations and reuses strata; the oracle's
differential tests.

## 6. W2: convert and hydrate what is read

Two parts. The first changes nothing visible.

### 6.1 The command store

Commands read named rules: a `DERIVE` reads the rules its body references,
`QUERY`, `EXPLAIN RULE`, `ABDUCE` and `VALIDATE` name theirs. Compute that
set once per program. For the store the dispatch loop hands to commands
(step 7b and each re-evaluation), convert and hydrate only those rules.

Also, within hydration: collect each rule's ids once, not twice.

### 6.2 `LocyResult::derived`

This is where the choice is (§8, question 1). `derived` is public and holds
every rule, hydrated. Three options:

- **(a) Lazy per rule.** Keep the batches; convert and hydrate a rule on
  first `derived_rule(name)`. The `LazyDerived` mechanism exists for rules
  pruned under goal-directed evaluation. The difficulty is the state: today's
  facts are hydrated "in the state after the commands", and a lazy read would
  hydrate against whatever the transaction holds when it is called. It needs
  a pinned read, or the documented meaning changes.
- **(b) Opt out.** A `LocyConfig` field naming which rules' facts to return:
  all (the default, today's behaviour), none, or a list. The resolution
  program's caller sets none. No semantic change for anyone else, and no
  saving for anyone who does not set it.
- **(c) Share nodes.** Hydrate once per distinct node and share it across
  cells (`Arc<Node>` in `Value::Node`, or an id-plus-table row form). The
  largest saving and the largest change: `Value` is the engine's public value
  type.

Recommendation: 6.1 and (b) now; (a) only with a pinned read; (c) not for
this.

Estimated effect: hydration and conversion are about 6.6 s of the profiled
run against 16.6 s of rule evaluation. With 6.1 and (b) the command store for
this program holds 500 facts, not 210,757.

Tests: the C-NONINTERFERENCE and "hydration covers every rule" invariants in
the Black Book's Appendix B2 are restated for the command store and
`derived` separately; existing `LocyResult` tests pass with the default; a
new test that a `DERIVE` over a rule outside the returned set still writes
its facts.

## 7. W3 and W4: the commands

### 7.1 `[NOT] EXISTS` as a join

When a `WHERE` has a top-level `AND` conjunct `EXISTS { MATCH pattern WHERE
p }` or its negation, and:

- the body is one `MATCH` of variables the outer scope does not bind, with
  no `OPTIONAL`, no further clause, and no mutation;
- the outer variables appear only in `p`;

plan it as a join of the outer input with the body's pattern:

- `p`'s conjuncts are classified as today for a cross join
  (`classify_join_predicate`): inner-only ones go beneath the inner side,
  equalities across the two become hash keys, the rest become the join
  filter;
- with at least one equality: `HashJoinExec`, `LeftSemi` for `EXISTS`,
  `LeftAnti` for `NOT EXISTS`, with the residual as its `JoinFilter`;
- with none: `NestedLoopJoinExec` of the same two kinds. Still one scan of
  the inner side.

Semantics to hold:

- `EXISTS` is never NULL. A NULL key or a NULL filter result is "no match":
  the default null-unequal hash join and a filter that keeps only `true` give
  exactly that, for both kinds. `LeftAnti` must not be the null-aware
  variant.
- The inner scan is recorded in the read set once, with the same rows it
  records today per execution.
- Everything else keeps today's path: an `EXISTS` under `OR` or in a
  projection, a body with several clauses, a body whose pattern names an
  outer variable (the per-source path already serves that shape).

For the new-entity `MERGE` this is a `LeftAnti` hash join on `type` with the
`ENDS WITH` / `=` and `<` disjunction as its filter, and
`NOT (o)-[:REFERS_TO]->()` beneath the inner side: one scan of `Mention`
where there are 1,000.

Both join kinds already run in this codebase without a filter
(`semi_join_on_vid`, `arrow_left_anti_dedup`). A semi- or anti-join with a
filter does not; the residual here calls `toLower` and compares strings,
which the standard expression path compiles, but a residual holding a custom
expression (a pattern predicate over both sides, `similar_to`) needs the
join's intermediate schema to carry what that expression reads. Decline
those in the first version.

### 7.2 Cheaper per-row execution, for what 7.1 declines

- **Subquery conjuncts last.** Split a `WHERE` into a `FilterExec` of its
  ordinary conjuncts beneath a `FilterExec` of its subquery conjuncts, so a
  subquery runs only for rows the rest keeps. Here that is 500 executions,
  not 1,000. Conjunct order is not observable in Cypher.
- **Project before cloning in `build_l0_vertex_batch`.** Copy only the
  properties the scan's schema asks for. This helps every scan of unflushed
  rows, not only subqueries.

These two are small and independent of 7.1; the second is the single
cheapest change in this document.

### 7.3 Estimated effect

The `MERGE` is 10.7 s alone. Its inner join is 1,000 × 1,000 on one type with
a string filter, comparable to one `match_evidence` clause (under a second in
debug in the first pass). 7.2 alone: half the executions, and the L0 rebuild
(5.0 s of the 13.5 s profiled) falls with the projection.

### 7.4 W4: `DERIVE` in one statement

Write a `DERIVE`'s facts with one statement per clause shape:

```cypher
UNWIND $facts AS f
MATCH (m), (e) WHERE id(m) = f.m AND id(e) = f.e
CREATE (m)-[:REFERS_TO]->(e)
```

One plan, one footprint walk. Per-fact property values become fields of the
unwound map. Skip building `DerivedEdge`s when the caller does not collect
them (`collect_derive == false`).

Check first that `UNWIND` of a parameter followed by `MATCH ... WHERE id(v) =
f.x` plans as the id lookup join (`VidLookupJoinExec`), not a scan per row;
issue #55's work says it should. If labels are needed in the `MATCH` to keep
today's plan, group the facts by label pair.

Estimated effect: `execute_mutation` is 3.1 s of the profiled run for about
2,600 statements across its sizes.

Tests for §7: the existing `cypher_exists`, `exists_per_source_test`,
`scalar_subquery_test`, `outer_entity_in_subquery` and the
`ExistentialSubquery` TCK features unchanged; a new file in the manner of
`bugs/pattern_predicate_join_side.rs` with the join's answers against the
per-row path for each shape of 7.1, each decline, NULL keys and NULL filter
results, uncommitted rows, and a plan-shape assertion with its negative twin;
`DERIVE` tests unchanged, plus a statement-count assertion.

## 8. Order, risk and open questions

Order: W0, then W1, then 7.2, then W2 (6.1 and the chosen option), then 7.1,
then W4. W1 and 7.2 are the two with a measured or near-certain saving and
no visible change. 7.1 is the largest piece and the one a planner mistake
would make wrong rather than slow.

Open questions:

1. **`LocyResult::derived`.** Option (a), (b) or (c) of §6.2? (b) adds a
   config field; (a) changes when hydration reads state; (c) changes `Value`.
2. **Warnings without lineage.** W1 assumes a fold over `PROB`-less
   references can never warrant `SharedProbabilisticDependency` or
   `CrossGroupCorrelationNotExact`. That was read from `lineage_rows` and
   `RefScope::Kept`, not tested against a case built to break it.
3. **How wide to make 7.1.** `COUNT { } > 0` and `COUNT { } = 0` are the same
   joins; a general `COUNT { }` is a grouped join. In or out of the first
   version?
4. **The whole-program figure of §2.3.** Unexplained until W0.

## 9. What this does not do

- (Tracked as #335.) `match_evidence` still holds mentions × entities facts per clause, and the
  0.5 threshold keeps 209,257 of them at 1,000 × 1,000. Only the top-k
  generator of the #330 proposal (§7 there) changes that.
- `DERIVE`'s re-run of its rule body still reads up to distinct-m ×
  distinct-e rows (§3.4).
- A cross join without an equality still pushes no one-sided conjunct down
  (#330 proposal, §6.1).

## 10. As built

Debug build, `nlp_resolve::resolution_program_stage_times`, 1,000 × 1,000, the
whole program in one `locy` call, one run of each figure.
`UNI_PROBE_PROGRAM_ONLY=1` runs only that.

### 10.1 Result

| Step | Whole program | What moved |
|---|---|---|
| Before | 20.6 s | |
| W1, provenance by need | 18.5 s | first pass's strata 7.4 s to 2.0 s; see 10.3 for why the total moved less |
| §7.2 and W2, `returned_rules = Nothing` | 9.6 s | hydration and conversion 3.4 s to 0.2 s; the `MERGE` 10.0 s to 4.5 s |
| §7.1, `NOT EXISTS` as an anti-join | 4.9 s | the `MERGE` 4.5 s to 0.14 s |
| W4, `DERIVE` in one statement | 4.4 s | each `DERIVE` command about 0.4 s less |

With the default `returned_rules` (`All`) the result's facts are still
converted and hydrated once, about 1.5 s more.

What is left of the 4.4 s: the first pass's strata, 2.0 s; the re-evaluation
after the first `DERIVE`, 1.0 s; the two `DERIVE` commands' fact collection,
about 0.35 s each. `match_evidence` is still mentions × entities (§9).

Release build, same probe, two runs:

| | Before (#330 §10.6) | As built |
|---|---|---|
| Whole program, 1,000 × 1,000 | 5.86 s | 1.17 s, 1.21 s |
| Whole program, 300 × 300 | 727 ms | 148 ms, 151 ms |
| New-entity `MERGE`, run alone, 1,000 | 1.78 s | 45 ms, 50 ms |
| First `DERIVE refers_to`, run alone, 1,000 | 2.96 s | 1.03 s, 1.06 s |
| First pass's strata, 1,000 | 1.68 s | 0.59 s, 0.60 s |

At 1,000 × 1,000 in release the default `returned_rules` (`All`) costs 0.26 s
more (1.56 s), and with the join rewrite off (`UNI_EXISTS_JOIN=0`) the
in-program `MERGE` is 0.70 s where it is 0.05 s (1.89 s in all). Of the 1.2 s,
the two passes of the strata are 0.9 s.

Flushed data and several entity types, release build, whole program at
1,000 × 1,000, two runs (`UNI_PROBE_FLUSH=1`, `UNI_PROBE_TYPES=5`):

| | Unflushed | Flushed |
|---|---|---|
| One type | 1.20 s, 1.23 s | 1.25 s, 1.25 s |
| Five types | 0.54 s, 0.55 s | 0.59 s, 0.59 s |

Flushing changes little. With five types the type equality cuts the pairing
to a fifth and the first pass's strata fall from 0.58 s to 0.16 s; the links
are the same. There is no "before" for these two settings.

### 10.2 What changed from the design

1. **W1's rule for skipping lineage was too loose.** A fold with no
   probabilistic reference can still share a proof: two rows of one fact
   resting on the same node or relationship property are one *row event*,
   and that raises `SharedProbabilisticDependency` with no `IS` at all. The
   need is read off the planned program instead
   (`program_can_share_proofs`): no row event and no lineage column naming a
   probabilistic fact. The tracker is dropped only then, and only under
   default inference; exact inference and the `TopKProofs` semiring keep
   today's behaviour. The per-rule lineage decision and the per-stratum reuse
   rule of §5 were not built: the decision is per program.
2. **Annotations and lineage are separate.** A program that keeps a tracker
   for lineage alone records no annotations (`ProvenanceStore::lineage_only`).
   `top_k_proofs > 0` keeps them, since its pruning warning comes from the
   annotation path.
3. **W2's command store is narrowed only for `QUERY`, `DERIVE`, `VALIDATE`,
   Cypher and `CALIBRATE` programs.** `ASSUME`, `ABDUCE` and `EXPLAIN RULE`
   read more than the rules they name. Open question 1 was settled as option
   (b): `LocyConfig::returned_rules`.
4. **§7.1 is built in the physical planner, for node-only bodies.** A logical
   plan node would have touched 65 places. A body with a relationship keeps
   the per-row or per-source path. `COUNT { }` is not rewritten (open
   question 3), which makes it the test oracle.
5. **W4 batches the transaction path only**, and only facts between existing
   nodes. The session path publishes one statement per fact for `tx.apply`.

### 10.3 The unexplained figure of §2.3

With provenance off the whole program moved 0.3 s where its first pass moved
5.2 s. The phase timers show why: the in-program `MERGE` took 4.5 s with a
tracker and 10 s without. The tracker did not make it faster. A sampling
profile of each shows the per-row scans rebuilding the L0 batch without a
tracker (`build_l0_vertex_batch`, 856 of 1,039 samples) and not at all with
one: there the mentions were no longer in L0 when the `MERGE` ran. The likely
cause, not confirmed, is a background flush that the longer strata gave time
to finish; the mentions are committed but unflushed when the program starts. §7.2's projection and then
§7.1 removed that cost on either path.

### 10.4 Found on the way, fixed afterwards

The per-row subquery path failed on `toLower` of a NULL outer value:
`MATCH (m:M) WHERE COUNT { MATCH (o:M) WHERE toLower(o.s) ENDS WITH
toLower(m.s) } = 0` raised "Unsupported data type NULL for function lower"
when some `m.s` was NULL. An untyped NULL literal reached DataFusion's
`lower`, which has no signature for it; the same held for `toUpper` and the
trims. `first_string_arg` (`uni-query-functions`, `df_expr.rs`) now hands
them a NULL string. Guard: `bugs::exists_join::string_functions_of_null_are_null`,
and the resolution shape is back in that file's comparison against the
per-row path.
