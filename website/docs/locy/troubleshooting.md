# Locy Troubleshooting

## Compile Errors

### Undefined Rule

Cause: `IS`/`IS NOT` references a non-existent rule.

Fix: Define referenced rule first or import the module containing it.

### Cyclic Negation

Cause: stratification violation through negation cycles.

Fix: refactor rule dependencies to remove negative cycles.

### Schema Mismatch

Cause: overloaded rule clauses yield incompatible column sets/types.

Fix: align `YIELD` contracts across clauses.

### NonMonotonicInRecursion

Cause: a recursive rule folds with an aggregate whose registered semilattice declares `monotone_join: false` — `SUM` and `AVG` among the built-ins — or with an aggregate the compiler cannot resolve at all.

Fix: switch to a monotone aggregate (`MSUM` for additive accumulation, `MMAX`/`MMIN`, or the plain `MIN`/`MAX`/`COUNT`/`COLLECT`), or move the non-monotone fold into a separate non-recursive rule that consumes the recursive relation with `IS`.

### FoldInRecursivePath

Cause: a clause has a recursive `IS`-reference and a `FOLD` aggregate but no `ALONG`. This is a warning, not an error, and the shape is legitimate — it is how a bill-of-materials or risk rollup is written.

Fix: nothing, if you wanted a per-KEY rollup. The warning exists because authors who wanted a value accumulated along each *path* — a cost, a hop count — want `ALONG` instead. See [Rule semantics](rule-semantics.md#what-a-self-reference-reads) for what the self-reference binds.

### A recursive rollup returns a number that disagrees with its own children

Cause: on releases before the issue #162 fix, a self-reference inside a recursive `FOLD` rule read the target's *pre-fold* rows rather than its folded value, and whole-row deduplication then collapsed equal-valued siblings. Any node with two or more children of equal value lost all but one of them, and the error propagated to every ancestor — optimistically, for `MNOR`/`MPROD`.

Fix: upgrade. The invariant to check needs no external oracle: an assembly's rolled-up value must equal the fold of its children's rolled-up values.

### BestByWithMonotonicFold

Cause: `BEST BY` used in the same rule as a declared lattice fold — `MSUM`, `MMAX`, `MMIN`, `MCOUNT`, `MNOR` or `MPROD`. The check is syntactic over those six names and applies to every rule, recursive or not.

Fix: Declared lattice folds are incompatible with witness selection. Use a separate rule to compute the aggregate, then reference it with `IS` in the `BEST BY` rule. Plain `MAX`, `MIN`, `COUNT` and `COLLECT` are fine alongside `BEST BY`.

### ProbabilityDomainViolation (warning)

Cause: `MNOR` or `MPROD` used with non-literal arguments that may produce values outside `[0, 1]`.

Fix: The compiler warning is informational — values are clamped at runtime unless `strict_probability_domain = true`. Sanitize upstream scores if clamping is masking a data quality issue.

## Runtime Issues

### Incomplete Evaluation (timeout or iteration limit)

An evaluation that exceeds its wall-clock `timeout` or its `max_iterations` cap is
a **hard error by default** — it does *not* return partial facts silently. The
error (`UniError::LocyIncomplete` in Rust; `UniLocyIncompleteError` in Python)
distinguishes the two causes via `reason` (`timeout` vs `iteration_limit`) and
names which rules were left incomplete or skipped, so a zero-row count is never
mistaken for a genuinely empty result.

- **`reason = iteration_limit`**: recursion did not converge. Increase
  `max_iterations` only after verifying the rule actually converges (a
  non-monotone rule may never reach a fixed point).
- **`reason = timeout`**: narrow query goals or reduce branching; raise `timeout`
  with caution.

**Negation caveat:** any `IS NOT` / complement rule touched by the cutoff is
listed in `complement_rules_affected`. Stratified negation over an unfinished
relation is **unsound**, so those results must not be trusted at all — not even
the "looks empty" ones.

**Best-effort / anytime semantics:** to inspect the partial result instead of
erroring, set `allow_partial` (Rust: `.allow_partial(true)` on the builder;
Python: `session.locy_with(prog).with_config({"allow_partial": True}).run()`, or
`LocyConfig(allow_partial=True)`). The returned result then has `timed_out =
True` and an `incomplete` diagnostics object carrying the same `reason`,
strata counts, and `incomplete_rules` / `skipped_rules` /
`complement_rules_affected` lists. You are responsible for checking them.

The `timeout` is the budget of the whole program: its own evaluation, every
evaluation it runs again after a write, every ASSUME body and ABDUCE candidate,
and its commands all share it. A program whose budget runs out after its facts
are complete stops before its next command; with `allow_partial` its result has
the complete facts and an `incomplete` with reason `timeout` and no rule listed.
When an evaluation the program runs again (after a write, or in an ASSUME body)
is cut short, `allow_partial` reports it in the same `incomplete`, naming its
rules, and the commands stop there: the command that needed those facts is not
answered from them. A QUERY over the derived facts checks the budget every 1024 rows. With
`allow_partial`, a QUERY the budget stops is not answered and the cut is
reported. A command that finishes past the deadline keeps its answer, but the
overrun is reported in `incomplete` and no later command runs: a result
reported complete took at most its `timeout`. Without `allow_partial`,
converting and hydrating the facts stop at the deadline too, and the program
fails with `Timeout`. With `allow_partial`, converting the derived facts to
rows and hydrating their node and relationship IDs can run past the deadline.
The result promises the facts the run derived, complete and hydrated, whatever
the timing. A run cut by its deadline starts this work after the deadline, so
bounding it would return no facts, or raw IDs, exactly when partial results are
wanted. The overrun is reported in `incomplete`.

### Memory Pressure

Lower result breadth and tune `max_derived_bytes`.

### Probability Domain Failures

If `strict_probability_domain = true`, values outside `[0, 1]` cause evaluation errors instead of being clamped.

Fix: sanitize upstream scores or disable strict mode while investigating the source.

### Shared-Probability Warnings (`SharedProbabilisticDependency`)

Cause: multiple MNOR/MPROD proof paths reuse the same evidence; the independence assumption is violated.

Fix: enable `exact_probability = true` for exact per-group results via BDD evaluation, or accept the independence approximation and inspect `warnings` / `_approximate` markers to understand which groups are affected. Exact mode then reports the informational `SharedDependencyResolved` instead; if the warning persists in exact mode, its `probability.resolution` is `fallback` and `reason` says why (see BDD Limit Exceeded below).

### BDD Limit Exceeded (`BddLimitExceeded`)

Cause: `exact_probability` is enabled but a proof group exceeded `max_bdd_variables`. That group fell back to independence mode.

Fix: increase `max_bdd_variables` (at the cost of memory/CPU), or restructure the rule to reduce the number of independent variables in a single aggregate group.

### Cross-Group Correlation (`CrossGroupCorrelationNotExact`)

Cause: shared evidence spans multiple aggregate key groups. Each group is exact internally (via BDD), but cross-group correlation is still approximate.

Fix: This is a fundamental limitation when shared evidence crosses group boundaries. If exact cross-group probability is required, restructure the query so all correlated facts fall in the same aggregate group.

## Debug Workflow

1. `session.compile_locy(program)` to validate program structure.
2. Run with smaller datasets.
3. Use `EXPLAIN RULE` on specific bindings.
4. Add constraints to `QUERY`/`ABDUCE` scopes.
