# `REQUIRE`: a definitional threshold for recursive Locy rules (#265)

**Status:** implemented. Closes the expressiveness half of issue #265; the
warning and documentation halves shipped in the preceding commit
(`feat(locy): warn when a post-FOLD WHERE cannot constrain its own recursion`).

Four claims in the design below were wrong and are corrected in
"What implementation changed" at the end — read that before trusting a
file:line in the Implementation section.

## Context

A rule whose *definition* depends on an aggregate crossing a threshold cannot be
written today. The post-FOLD `WHERE` is applied once to the converged answer, so
a self-reference reads the rule's **unfiltered** folded value and derives facts
from groups the threshold excluded — while those groups are correctly absent
from the output, which is what makes the wrong answer look plausible.

```locy
CREATE RULE blocked AS
    MATCH (o:Entity)-[s:OWNS]->(e:Entity)
    WHERE o IS blocked
    FOLD agg = MSUM(s.pct)
    WHERE agg >= 50.0        // filters the ANSWER, not the recursion
    YIELD KEY e, agg
```

`A` owns 11 % of `B`; `B` owns 60 % of `C`. `B` is below the threshold and is
absent from the result, yet still qualifies `C`. Answer `{A, C}`; the OFAC 50 %
Rule says `{A}`.

### Why this needs syntax rather than compiler inference

Locy's `M`-aggregates are Shkapsky/Zaniolo monotonic aggregates, and the
property at stake is **pre-mappability** (PreM): a constraint γ is PreM to the
immediate-consequence operator `T` when `γ(T(I)) = γ(T(γ(I)))` for every
interpretation `I` (Zaniolo et al., arXiv:1707.05681). When PreM holds, pushing
the constraint into the recursion is a pure optimisation and a compiler may do
it silently.

**PreM fails here.** Checked by hand on the graph above: `γ(T(I))` = `{A, C}`,
`γ(T(γ(I)))` = `{A}`. The pushed form is therefore a *second semantics* —
`lfp(Tγ)` rather than `γ(lfp(T))` — not an optimisation. Both are well defined
and users want each, so the author has to say which.

DeALS has no keyword for this: the constraint is an ordinary body goal
(`is_min((Y),(Dy))`) and PreM is the criterion. Locy cannot copy that, because
the constraint references a `FOLD` output that does not exist until after
`FOLD`. Hence a new post-`FOLD` clause.

## Design

```text
FOLD agg = MSUM(s.pct)
REQUIRE agg >= 50.0      -- part of the definition; constrains the recursion
WHERE   agg >= 50.0      -- unchanged: filters the converged answer
```

- `WHERE` keeps today's meaning exactly. No existing program changes.
- `REQUIRE` is applied to the per-iteration folded snapshot — the thing a
  self-reference reads — so it constrains what the rule can derive.
- Both may appear; `REQUIRE` precedes `WHERE`, which reads as "constrain the
  derivation, then filter what is shown".
- **Legal in non-recursive rules**, where the two coincide. This makes rules
  refactor-safe: adding a self-reference later keeps the author's meaning
  instead of silently flipping it.

### Why not the other keywords

`HAVING` is the internal name of the construct that has the bug
(`fold_having_clause`, `def.having`) and SQL's `HAVING` is a presentation
filter — the exact meaning being inverted. `QUALIFY` reads well but
Snowflake/Teradata/BigQuery use it as a post-window presentation filter, the
same trap. `CONSTRAIN`/`ASSERT` collide with `CREATE CONSTRAINT … ASSERT …`
(`cypher.pest:723`). Accepted risk on `REQUIRE`: Neo4j 5 uses it in constraint
DDL while uni-db still uses Neo4j 4's `ASSERT`; both readings mean "this must
hold", so neither inverts timing.

### Termination — the load-bearing argument

`REQUIRE` is sound only when the predicate can never go true → false as the
fixpoint grows. A lower bound over a non-decreasing fold, or an upper bound over
a non-increasing one, can only flip false → true, so `Tγ` stays monotone and
`lfp(Tγ)` exists.

The reverse pairings must be a **compile error**, not a warning, because the
runtime cannot recover. `prev_rows` (`locy_fixpoint.rs:734`) is computed over
*contribution* rows, not the folded view, so a group leaving the snapshot is
invisible to the producing rule's own change test. It propagates only through
consumers, whose contributions are replaced rather than deleted (the `latest`
map, `:703-709`), so a value admitted on iteration N and excluded on N+1 makes
the consumer's row set flip back and forth — and `rows_now != prev_rows` reads
every flip as *progress*. A non-monotone `REQUIRE` would therefore spin to
`max_iterations` and return partial results.

This is where monotonicity finally does its proper job. It is the wrong tool for
deciding *intent* — #162's case is monotone too and wants the opposite answer —
but with intent stated explicitly by the keyword, direction becomes exactly the
soundness gate.

## Implementation

### 1. Grammar — `crates/uni-cypher/src/grammar/locy.pest`

Add `REQUIRE = @{ ^"require" ~ !ident_char }` to the **contextual** keyword block
(~`:33-43`), *not* to `locy_keyword_reserved` (`:86`). Contextual keeps `require`
usable as an ordinary identifier, so no existing program breaks — the same
treatment `PRIORITY`, `NEW`, `EXPORT` and `PROB` get.

```
fold_require_clause = { REQUIRE ~ expression ~ ((AND | ",") ~ expression)* }
```

and place it in `rule_definition` (`:150-159`) between `fold_clause?` and
`fold_having_clause?`.

### 2. AST — `crates/uni-cypher/src/locy_ast.rs`

`RuleDefinition` (`:75`) gains `require: Vec<Expr>` beside `having` (`:84`).

**`#[serde(default)]` is mandatory.** `RuleDefinition` derives `Deserialize` and
rule sources are persisted to `catalog/locy_rules.json`
(`crates/uni/src/api/locy_rule_catalog.rs`); without it, every previously stored
rule fails to load. Only two construction sites (`locy_ast.rs`,
`grammar/locy_walker.rs`), plus the walker arm that populates it.

### 3. Per-aggregate direction

A new `FoldDirection { NonDecreasing, NonIncreasing, Unknown }`.

**Do not put it on `Semilattice`.** That struct is not `#[non_exhaustive]`
(`crates/uni-plugin/src/traits/locy.rs:225`), so a new field breaks every plugin
constructing one — and it could not live there anyway, because `MinAgg` and
`MaxAgg` both return the same `Semilattice::BOUNDED_MIN_MAX` constant and are
indistinguishable in it.

Instead add a default-bodied trait method, which is backward compatible and
follows `output_type_for_input` / `initial_accum_f64`:

```rust
fn direction(&self) -> FoldDirection { FoldDirection::Unknown }
```

Built-ins override in `crates/uni-plugin-builtin/src/locy_aggregates.rs`, taking
the values from the table already in `skills/uni-db/references/locy.md:313-319`:
`MSUM`, `MMAX`, `MCOUNT`, `MNOR` non-decreasing; `MMIN`, `MPROD` non-increasing.
A plugin aggregate that does not override answers `Unknown`, and `REQUIRE` on it
is rejected — conservative by construction.

Compiler side, mirror the existing oracle rather than changing its type
(`typecheck.rs:23` is threaded through `compiler/mod.rs:123, 138, 155, 269`):

```rust
pub type DirectionOracle<'a> = &'a (dyn Fn(&str) -> FoldDirection + 'a);
pub fn default_direction_oracle(name: &str) -> FoldDirection { … }
```

with a registry-backed variant alongside `plugin_monotonicity_oracle`
(`crates/uni/src/api/impl_locy.rs:448`), which needs a matching field on
`uni_plugin::registry::LocyAggregateEntry` next to `monotone_join`.

### 4. Typecheck — `crates/uni-locy/src/compiler/typecheck.rs`

`check_require_direction`, called beside `check_non_monotonic_in_recursion`
(`:176`) inside the `is_recursive` block. Pair each `REQUIRE` comparison against
the fold's direction; on a mismatch or `Unknown`, raise a new
`LocyCompileError::NonMonotonicFilterInRecursion { rule, aggregate, comparison }`
(`compiler/errors.rs`). This is the error #265 originally proposed, now attached
to the construct where refusing is correct rather than to `WHERE`, where it
would refuse #162's legitimate case.

`MSUM` is non-decreasing **only on non-negative inputs**, which the existing
`MsumNonNegativity` warning already covers; `REQUIRE` over `MSUM` inherits that
caveat and should say so in its message. The OFAC case (percentages) satisfies
it.

### 5. Compiled form — `crates/uni-locy/src/types.rs:129`

`CompiledClause` gains `require`. Note the hazard: there are ~26 struct literals
of it, almost all `#[cfg(test)]` in `locy_planner.rs` (module at `:2334`,
representative `:2457`), plus `prune.rs:213`. Derive `Default` for the struct and
use `..Default::default()` in the test literals while adding the field, so the
next field addition costs one line rather than twenty-six.

### 6. Planner — `crates/uni-query/src/query/locy_planner.rs`

Mirror the six `having` sites: build with `substitute_fold_aliases` (`:964-971`),
store into `LocyRulePlan` (`:1077`), declare on
`crates/uni-query/src/query/planner_locy_types.rs:48`.

### 7. Runtime — `crates/uni-query/src/query/df_graph/locy_fixpoint.rs`

`FoldViewState` (`:539-556`) gains `require: Vec<Expr>`, populated where
`fold_view` is constructed (`enable_fold_view`, ~`:1750`).

In `recompute_folded` (`:787`) — today PRIORITY then FOLD — apply the filter
after the aggregates are grafted onto the representative rows and immediately
before `fv.folded = …`. Reuse the existing free function unchanged:

```rust
fn apply_having_filter(
    batches: Vec<RecordBatch>, having_exprs: &[Expr],
    schema: &SchemaRef, task_ctx: &Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>>
```

`task_ctx`, the batch and its schema are all in scope, so no refactor is needed.
**One caveat to get right:** the snapshot carries the *contribution* schema with
grafted aggregate columns, so fold outputs appear under contribution column
names rather than `output_name` aliases — the alias substitution from step 6
must be applied consistently, and a test should cover a `REQUIRE` that
references an aliased fold output.

Filtering `fv.folded` is precisely what constrains the recursion, because the
folded view is what a same-stratum self-reference reads. `prev_rows` is
unaffected, which is correct: the producing rule's own convergence test still
runs over contributions.

### 8. Close the loop on the shipped warning

`HavingInRecursivePath` (`typecheck.rs`, added in the preceding commit) should name
`REQUIRE` as the answer once it exists, and should not fire when the rule uses
`REQUIRE` instead.

### 9. Documentation

The three places corrected by the preceding commit all need the new construct:
`skills/uni-db/references/locy.md` §7, `website/docs/locy/advanced/along-fold-bestby.md`,
`docs/complete_locy.md` §15.2. Also `website/docs/locy/reference/syntax-cheatsheet.md`.

## Verification

**The decisive test** is one graph and two programs differing only in the
keyword: `REQUIRE` returns `{A}`, `WHERE` returns `{A, C}`. If both return the
same thing, nothing was implemented. Belongs in the TCK beside the existing
`crates/uni-locy-tck/tck/features/correlation/HavingInRecursivePath.feature`,
whose "The excluded group still drives the recursion" scenario already pins the
`WHERE` half.

Also:

- **Compile error, both directions.** `REQUIRE agg >= x` over `MMIN` (upper
  bound over non-increasing is fine; lower bound over non-increasing is not) —
  test each of the four pairings, and a plugin aggregate answering `Unknown`.
- **Termination.** A rejected pairing must fail at compile time, not run to
  `max_iterations`. Assert the error, and assert no partial-results warning.
- **#162 is untouched.** `issue_162_having_is_not_applied_to_the_per_iteration_snapshot`
  must still pass unchanged — `WHERE` semantics are not being altered.
- **Non-recursive equivalence.** `REQUIRE` and `WHERE` agree in a non-recursive
  stratum.
- **Persistence.** A rule stored before this change still loads
  (`catalog/locy_rules.json` round-trip) — the `serde(default)` guard.
- `cargo nextest run -p uni-locy -p uni-locy-tck -p uni-query -p uni-db`;
  `scripts/run_tck_with_report.sh` for the openCypher side (grammar change).

## Out of scope

Inferring PreM automatically, so the compiler could push a constraint on its own
where it is safe — a genuine follow-up, and the literature's main subject, but it
optimises the `WHERE` path rather than adding expressiveness. `BEST BY` per
iteration, which has the same post-fixpoint restriction and no reported demand.


## What implementation changed

Four things in the plan above did not survive contact with the code. Recorded
here rather than silently edited, because each was a confident claim.

**`REQUIRE` must be applied twice, not once.** The design said the per-iteration
snapshot filter was the whole mechanism. It is not. The snapshot pass stops an
excluded group *deriving* anything downstream — necessary, and it works — but
the final answer is assembled from the rule's contribution facts, not from the
snapshot, so the excluded group itself still reached the output. Measured
directly: the OFAC program returned 3 rows with neither pass, 2 with only the
snapshot pass (`C` correctly gone, `B` still present), and 1 once `REQUIRE` was
also applied in `apply_post_fixpoint_chain` ahead of `HAVING`.

**`serde(default)` is hygiene, not a migration guard.** The plan called it
mandatory because rule sources are persisted. They are — but as *source text*:
`crates/uni/src/api/locy_rule_catalog.rs` stores the verbatim program and
recompiles on open, explicitly "never compiled artifacts". No stored AST carries
the old shape, so nothing would have failed to load. The attribute is kept as
ordinary hygiene for a `Deserialize` type and its rustdoc now says so.

**`LocyAggregateEntry` needed no new field.** It already holds
`aggregate: Arc<dyn LocyAggregate>`, and the existing monotonicity path is
`e.aggregate.semilattice().monotone_join` (`locy_fold.rs:63-64`), so the
direction oracle is a one-line mirror. The plugin registry surface is untouched.

**`FoldDirection` lives in `uni-common`, not `uni-plugin`.** `uni-locy` does not
depend on `uni-plugin` — deliberately — so the compiler could not see a type
defined there. It sits in `uni-common`, which both already depend on, and is
re-exported from `uni_plugin::traits::locy` so plugin authors see it where they
would expect.

Two smaller deviations. `CompiledClause` did not get a `Default` derive: it
holds `RuleOutput` and `Pattern`, neither of which implements `Default`, and
adding that to shared AST types for test ergonomics was more than the change
warranted — the ~26 literals were updated mechanically instead. And the alias
test is non-recursive, because a *recursive* rule that renames a fold output in
`YIELD` fails with "FOLD aggregate 'SUM' input column 'agg' not found in body
batch" — reproduced identically with the post-FOLD `WHERE` in place of
`REQUIRE`, so it is a pre-existing limitation of recursive FOLD plus aliasing
and not something this work introduced.

### The bug this work nearly shipped with

`FixpointRulePlan` is cloned **field by field** inside `execute`
(`locy_fixpoint.rs`, "We need to clone the FixpointRulePlan, but it contains
LogicalPlan"), so a newly added field silently arrives empty at runtime no
matter how correctly it was populated upstream. `require` was threaded correctly
through the AST, the compiler, and the planner, and was still empty at the
fixpoint. Anything added to that struct in future must be added there too; the
compiler will not say so.
