# Advanced: ALONG, FOLD, BEST BY

## ALONG (Path-Carried Values)

`ALONG` carries state through recursive expansion.

```locy
CREATE RULE shortest AS
MATCH (a)-[e:EDGE]->(b)
ALONG dist = prev.dist + e.weight
YIELD KEY a, KEY b, dist
```

Use `prev.<field>` to reference prior recursive step values.

## FOLD (Aggregation)

`FOLD` aggregates rule outputs after derivation.

```locy
CREATE RULE totals AS
MATCH (a)-[:EDGE]->(b)
FOLD total = SUM(b.value)
YIELD KEY a, total
```

### FOLD in a recursive rule

A recursive `FOLD` rolls up **per KEY, one level at a time**: a self-reference
binds one row per KEY of the target carrying that KEY's folded value, so a
parent folds its children's values and each child has already folded its own.

```locy
CREATE RULE build AS
MATCH (p:Part) WHERE p IS NOT assembly
YIELD KEY p, 0.5 AS b

CREATE RULE build AS
MATCH (p:Part)-[:CONTAINS]->(c:Part)
WHERE c IS build
FOLD b = MPROD(b)
YIELD KEY p, b
```

For `TOP → MID → {L1, L2}` with both leaves at 0.5, `MID` is `0.25` and `TOP`
folds that single value, so `TOP` is `0.25` too.

`ALONG` is the per-path alternative, and it wins on its own clause: a clause
carrying `ALONG` reads the pre-fold rows, because `prev.<field>` accumulates
along one path and a per-KEY aggregate is not defined per path. The choice is
made per clause, so a sibling clause of the same rule that folds an inherited
value still gets the folded value. See
[Rule semantics](../rule-semantics.md#what-a-self-reference-reads).

### Standard Aggregators

| Operator | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Row count | `FOLD n = COUNT(*)` |
| `COUNT(expr)` | Non-null count | `FOLD n = COUNT(b.value)` |
| `SUM(expr)` | Sum | `FOLD total = SUM(b.amount)` |
| `AVG(expr)` | Average | `FOLD avg = AVG(b.score)` |
| `MIN(expr)` | Minimum | `FOLD low = MIN(b.price)` |
| `MAX(expr)` | Maximum | `FOLD high = MAX(b.price)` |
| `COLLECT(expr)` | Collect into list | `FOLD paths = COLLECT(b.name)` |

`SUM` and `AVG` are **non-monotone** — they can decrease between iterations, which violates fixpoint requirements, so the compiler rejects them inside a recursive stratum. The rest (`COUNT(*)`, `COUNT(expr)`, `MIN`, `MAX`, `COLLECT`) declare a monotone join and are accepted in recursion.

`COUNT` and `COLLECT` are monotone but **unbounded** — the accumulator has no top element. That only costs iterations for `COUNT`, whose accumulator the fixpoint loop tracks: a recursive fold over it can iterate until `max_iterations` rather than converging. `COLLECT` is assembled after the fixpoint, so it does not itself keep the loop iterating.

### Monotonic Aggregators (Safe in Recursion)

Monotonic variants guarantee the aggregate only ever moves forward in its own lattice order between iterations — upward for `MSUM`/`MMAX`/`MCOUNT`/`MNOR`, downward for `MMIN`/`MPROD` — enabling safe use inside recursive strata:

| Operator | Formula | Identity | Use When |
|----------|---------|----------|----------|
| `MSUM(expr)` | Running sum | `0` | Non-negative additive accumulation |
| `MMAX(expr)` | Running maximum | `−∞` | Worst-case / dominant value |
| `MMIN(expr)` | Running minimum | `+∞` | Best-case / bottleneck |
| `MCOUNT(expr)` | Running count | `0` | Monotonically growing count |
| `MNOR(expr)` | `1 − ∏(1 − pᵢ)` | `0.0` | Independent OR-causes (probabilities) |
| `MPROD(expr)` | `∏ pᵢ` | `1.0` | Independent AND-conditions (probabilities) |

These six spellings are Locy's *declared lattice folds*. Writing one is an explicit assertion that the fold is a lattice join, and it is what the [`BEST BY` guard](#best-by-witness-selection) keys on. `MSUM` and `MCOUNT` are unbounded, so the same `max_iterations` caveat applies to them.

### Monotonic Probabilistic Folds

For probability domains, use `MNOR` (noisy-OR) and `MPROD` (product):

```locy
CREATE RULE failure_risk AS
MATCH (c:Component)-[:HAS_SIGNAL]->(s:QualitySignal)
FOLD risk = MNOR(1.0 - s.pass_rate)
YIELD KEY c, risk
```

See [Probabilistic Logic](probabilistic-logic.md) for full documentation.

### Multiple FOLD Clauses

A rule can have multiple `FOLD` clauses to compute several aggregates simultaneously:

```locy
CREATE RULE exposure AS
MATCH (a:Account)-[t:TRANSFER*]->(b:Account)
WHERE b IS suspicious
FOLD total = MSUM(t.amount),
     path_count = MCOUNT()
YIELD KEY a, total, path_count
```

## Post-FOLD WHERE (HAVING)

A `WHERE` clause after `FOLD` filters aggregated groups — equivalent to SQL's `HAVING`. Only rows where the condition holds are kept.

```locy
CREATE RULE frequent_payer AS
MATCH (p:Person)-[r:PAID]->(i:Invoice)
FOLD n = COUNT(*)
WHERE n >= 3
YIELD KEY p, n
```

This yields only people who made 3 or more payments. The `WHERE n >= 3` runs *after* FOLD computes the count.

### Multiple Conditions

Combine conditions with `AND`:

```locy
CREATE RULE big_spenders AS
MATCH (p:Person)-[r:PAID]->(i:Invoice)
FOLD n = COUNT(*), total = SUM(r.amount)
WHERE n >= 2 AND total >= 1000
YIELD KEY p, n, total
```

### Available Columns

Post-FOLD `WHERE` can reference:
- **FOLD output columns** (`n`, `total` in the examples above)
- **KEY columns** (the grouped-by entities)

It **cannot** reference pre-aggregation columns that were consumed by FOLD.

### Relationship to QUERY WHERE

Post-FOLD `WHERE` filters during rule evaluation. `QUERY ... WHERE` filters after:

```locy
// Filter inside the rule (during FOLD):
CREATE RULE counts AS
MATCH (e:Ev) FOLD n = COUNT(*)
WHERE n >= 3
YIELD KEY e.action, n

// Filter outside (at query time):
QUERY counts WHERE n >= 5 RETURN *
```

Both are valid. Use post-FOLD `WHERE` when the filter belongs to the rule rather than to one call site. Use `QUERY WHERE` for ad-hoc filtering at call sites.

In a **recursive** rule, see the caveat below before treating it as intrinsic — there, it filters the answer rather than the definition.

### Caveat: in a recursive rule, it does not constrain the recursion

Every example above is non-recursive, where post-FOLD `WHERE` behaves exactly like SQL `HAVING`. In a rule that references **itself**, it does not.

There the filter is applied **once, to the converged answer** — not per iteration. The self-reference reads the rule's *unfiltered* folded value, so rows the threshold excludes can still derive further rows. They are correctly absent from the output *while having contributed to it*, which is what makes this hard to notice: the answer looks reasonable, and small acyclic examples often agree with the intended reading.

```locy
// WRONG if you meant "an owner only counts once it reaches 50%".
CREATE RULE blocked AS
MATCH (o:Entity)-[s:OWNS]->(e:Entity)
WHERE o IS blocked          // self-reference: the stratum is recursive
FOLD agg = MSUM(s.pct)
WHERE agg >= 50.0           // filters the ANSWER, not the recursion
YIELD KEY e, agg
```

An entity holding 11% is filtered out of the result, yet still satisfies `o IS blocked` on the next hop, so everything *it* owns is derived regardless. Moving the aggregate into its own rule and filtering where it is consumed does not help — the filter still sits outside the recursion.

The compiler warns with `HavingInRecursivePath`; check `result.warnings()`.

**This is the intended behaviour when the filter selects what you see.** A child that the threshold removes from the answer must still have been visible to its parent while the fixpoint ran — that is what probabilistic rules need, and why this is a warning rather than an error. The two intents are spelled identically, so the compiler cannot tell them apart.

### REQUIRE: when the threshold is part of the definition

Write `REQUIRE` instead when the threshold *defines* the rule — an ownership or voting-control cutoff, a quorum, a cumulative-risk ceiling, reachability under a cost budget. It is applied to every iteration's folded snapshot, which is what a self-reference reads, so it constrains what the recursion derives:

```locy
CREATE RULE blocked AS
MATCH (o:Entity)-[s:OWNS]->(e:Entity)
WHERE o IS blocked
FOLD agg = MSUM(s.pct)
REQUIRE agg >= 50.0         // constrains the RECURSION
YIELD KEY e, agg
```

On the graph above this answers `{A}`: the owner at 11% is excluded, so nothing downstream of it is derived. The same rule with `WHERE` answers `{A, C}`.

Both clauses may appear on one rule, `REQUIRE` first — constrain the derivation, then filter what is shown.

**`REQUIRE` must be one-way.** In a recursive rule it is rejected at compile time unless the comparison can only ever turn from false to true as the fixpoint grows: a **lower** bound (`>=`, `>`) over a non-decreasing fold (`MSUM` over non-negative values, `MMAX`, `MCOUNT`, `MNOR`), or an **upper** bound (`<=`, `<`) over a non-increasing one (`MMIN`, `MPROD`). Equality is never admissible. The reverse pairings would let a fact be derived and then withdrawn, which the fixpoint reads as progress rather than oscillation, so the rule would run to the iteration limit instead of converging.

Outside recursion the direction is not policed and `REQUIRE` means exactly what the post-FOLD `WHERE` means — there is one pass, so nothing can flip. That makes it safe to reach for from the start: a rule keeps the meaning its author wrote if a self-reference is added later.

## BEST BY (Witness Selection)

`BEST BY` picks the best candidate row by ordering expression.

```locy
CREATE RULE cheapest AS
MATCH (a)-[e:EDGE]->(b)
ALONG cost = prev.cost + e.weight
BEST BY cost ASC
YIELD KEY a, KEY b, cost
```

### BEST BY and FOLD

`BEST BY` picks one witness row; a declared lattice fold aggregates across all of them. Combining the two in one rule is rejected at compile time with `BestByWithMonotonicFold`.

The guard is **syntactic** over the six declared lattice folds (`MSUM`, `MMAX`, `MMIN`, `MCOUNT`, `MNOR`, `MPROD`) and applies to **every** rule, recursive or not. It is deliberately decoupled from the recursion monotonicity check, so `BEST BY` combined with `MAX`, `MIN`, `COUNT` or `COLLECT` is legal even though those aggregates are monotone.

To get both, compute the aggregate in its own rule and reference it with `IS` from the `BEST BY` rule.

## Using `similar_to` in ALONG and BEST BY

The `similar_to()` expression function works in ALONG accumulators and BEST BY selectors, enabling semantic similarity scoring along recursive paths.

### Semantic Relevance Along Paths

```locy
CREATE RULE semantic_path AS
MATCH (a:Document)-[:LINKS_TO]->(b:Document)
ALONG relevance = prev.relevance * similar_to(b.embedding, $query)
YIELD KEY a, KEY b, relevance
```

### Best Semantically Similar Path

```locy
CREATE RULE best_match AS
MATCH (a:Topic)-[:RELATED]->(b:Topic)
ALONG score = prev.score + similar_to(b.embedding, $query)
BEST BY score DESC
YIELD KEY a, KEY b, score
```

### Hybrid Scoring in Rules

```locy
CREATE RULE hybrid_relevant AS
MATCH (q:Query)-[:SEARCHES]->(d:Document)
WHERE similar_to([d.embedding, d.content], q.text,
  {method: 'weighted', weights: [0.7, 0.3]}) > 0.5
YIELD KEY q, KEY d
```

See the [Vector Search guide](../../guides/vector-search.md#similar_to-expression-function) for full `similar_to` documentation.

## Practical Guidance

- Use `ALONG` for accumulators (distance, risk, confidence, similarity).
- Use `FOLD` when you need grouped summaries.
- Use post-FOLD `WHERE` to discard groups that don't meet a threshold (e.g., `WHERE count >= 3`) — but in a recursive rule that filters the answer, not the recursion; use `REQUIRE` when the threshold is part of the definition.
- Use `BEST BY` when you need one witness path, not all candidates.

## Related

- [Rule Semantics](../rule-semantics.md)
- [Internals: Architecture](../../internals/locy/architecture.md)
