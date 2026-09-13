# Rule Semantics

## Evaluation Pipeline

1. Parse Locy program.
2. Build dependency graph.
3. Validate types/schema compatibility.
4. Stratify rules.
5. Evaluate each stratum to fixpoint.
6. Execute command phase (`QUERY`, `DERIVE`, `EXPLAIN`, `ABDUCE`, `ASSUME` body).

## Two-Phase Execution

Locy execution is split into two distinct phases with different execution engines:

**Phase 1 — Strata Evaluation (DataFusion)**

Rules compile to DataFusion `LogicalPlan` nodes. The query engine runs them through a fixpoint loop per stratum. Expression functions (`similar_to()`, etc.) have full access to storage, schema, and the Xervo embedding runtime.

**Phase 2 — Command Dispatch (Row-Level)**

After strata converge, commands (`QUERY`, `DERIVE`, `ABDUCE`, `ASSUME`) execute on materialized `Vec<Row>` facts. WHERE filters use a lightweight row-level evaluator. This path supports vector cosine similarity but not auto-embedding, FTS, or multi-source fusion.

| Context | Execution | Vector | Auto-Embed | FTS |
|---------|-----------|--------|------------|-----|
| Rule `MATCH ... WHERE/YIELD` | DataFusion | ✓ | ✓ | ✓ |
| Rule `ALONG / FOLD / REQUIRE / post-FOLD WHERE` | DataFusion | ✓ | ✓ | ✓ |
| `DERIVE ... WHERE` | In-memory | ✓ | ✗ | ✗ |
| `ABDUCE ... WHERE` | In-memory | ✓ | ✗ | ✗ |
| `ASSUME ... WHERE` | In-memory | ✓ | ✗ | ✗ |

## Semi-Naive Evaluation

Within a recursive stratum, Locy only re-evaluates rules using *newly derived* facts (the delta) rather than all known facts each iteration. This provides exponential speedup for transitive closures:

```
Iteration 0: delta₀ = base facts from MATCH
Iteration 1: delta₁ = evaluate(rules, delta₀) − known_facts
Iteration 2: delta₂ = evaluate(rules, delta₁) − known_facts
...
Iteration n: deltaₙ = ∅ → fixpoint reached
```

One exception: a rule whose folded value is read by a self-reference in the same
stratum publishes a full per-KEY snapshot rather than a delta, because an
aggregate is a whole-relation quantity — see *What a self-reference reads* below.


## Overloaded Rules

Multiple `CREATE RULE` clauses sharing one name define one logical relation. Clauses can be prioritized where supported.

## Negation Rules

`IS NOT` requires stratification-safe dependencies. Cyclic negation is rejected at compile time.

If the referenced rule exposes a `PROB` column, `IS NOT` becomes probabilistic complement (`1 - p`) rather than Boolean anti-join. Rules without a `PROB` column keep standard Boolean negation.

## Monotonic Recursion

Recursive aggregation requires monotonic operators. Non-monotonic recursive shapes are rejected at compile time with `NonMonotonicInRecursion`.

Monotonicity is decided by the aggregate registry: the compiler reads the aggregate's `monotone_join` semilattice flag, falling back to the six built-in `M*` names (`MSUM`, `MMAX`, `MMIN`, `MCOUNT`, `MNOR`, `MPROD`) only when the registry has no entry for the name. So `MIN`, `MAX`, `COUNT`, `COUNT(*)` and `COLLECT` are legal inside recursive strata too; `SUM` and `AVG` are non-monotone and rejected.

Monotone does not imply convergent. `COUNT`, `COLLECT`, `MSUM` and `MCOUNT` are monotone but unbounded — they have no top element. For the ones the fixpoint loop tracks row by row (`COUNT`, `MSUM`, `MCOUNT`) that means a recursive fold can run to `max_iterations` instead of reaching a fixed point; the iteration cap is the backstop. `COLLECT` is the exception: it has no row-level accumulator and is assembled after the fixpoint, so it does not itself keep the loop iterating.

### What a self-reference reads

A rule inside a recursive stratum can reference itself. What that reference
binds depends on whether the rule aggregates:

- **The rule carries `FOLD`** — the reference sees **one row per KEY, carrying
  that KEY's folded value** as of the previous iteration. A rollup therefore
  composes one level at a time: a parent folds its children's values, and each
  child has already folded its own.
- **The referencing clause carries `ALONG`** — it sees the pre-fold rows, one
  per derivation. `prev.x` accumulates a value along a single *path*, and a
  per-KEY aggregate is not defined per path. This is decided per clause, so a
  sibling clause of the same rule that folds an inherited value still reads the
  folded view.
- **The rule carries `REQUIRE`** — the folded value the reference sees is
  filtered by it, every iteration. That is what makes `REQUIRE` constrain the
  recursion: a group below the threshold is not visible to the self-reference,
  so nothing downstream of it is derived. A post-FOLD `WHERE` does *not* do
  this — it is applied once, after convergence, so the reference still reads
  the unfiltered value. See
  [ALONG, FOLD, BEST BY](advanced/along-fold-bestby.md#require-when-the-threshold-is-part-of-the-definition).
- **A reference to a lower stratum** always reads that rule's published, folded
  facts. It always has.

```
CREATE RULE build AS
  MATCH (p:Part) WHERE p IS NOT assembly
  YIELD KEY p, 0.5 AS b

CREATE RULE build AS
  MATCH (p:Part)-[:CONTAINS]->(c:Part)
  WHERE c IS build
  FOLD b = MPROD(b)
  YIELD KEY p, b
```

For `TOP → MID → {L1, L2}` with both leaves at 0.5: `MID = 0.5 × 0.5 = 0.25`,
and `TOP` folds `MID`'s single value, so `TOP = 0.25` too.

The distinction is invisible for an associative aggregate over a bare inherited
column — folding a child's rows and folding its folded value agree for `MPROD`
and `MSUM`. It is visible for `MCOUNT`, which counts a node's **children**, not
its leaves, and for any fold whose argument is a computed expression.

Convergence follows the value, not just the row count: for these rules the
fixpoint has settled when no KEY has been added *and* no value has moved.

`MNOR` and `MPROD` are monotonic and bounded, therefore legal inside recursive strata. They assume independent derivations unless `exact_probability` is enabled.

**Plugin-registered aggregates.** Because the verdict comes from the registry, an aggregate registered by a plugin participates in the check on the same terms as a built-in: declaring `monotone_join: true` makes it usable inside a recursive stratum. Load the plugin first and this holds for rules compiled through `session.locy(...)` and for rules registered with `db.rules().register(...)`.

It does *not* extend to reopening a database. Persisted rules are recompiled during open, before any plugin can be added, so a stored rule folding over a plugin aggregate no longer compiles and the open fails naming that rule — unless you open with `skip_invalid_locy_rules(true)`, which drops it with a warning. Rules folding over built-in aggregates (`MIN`, `MAX`, `COUNT`, `COLLECT`, the `M*` names) reload normally.

## Determinism

`BEST BY` can use deterministic tie-breaking through config (`deterministic_best_by = true`).

## Limits and Guardrails

Key guardrails come from `LocyConfig`:

- `max_iterations`
- `timeout`
- `max_derived_bytes`
- `max_explain_depth`
- `max_slg_depth`
- `strict_probability_domain`
- `probability_epsilon`
- `exact_probability`
- `max_bdd_variables`
- `top_k_proofs`
- `deterministic_best_by`

See [Errors & Limits](reference/errors-limits.md) for operational guidance.
