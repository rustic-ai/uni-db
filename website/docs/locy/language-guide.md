# Locy Language Guide

## Rule Definition

```cypher
CREATE RULE rule_name [PRIORITY n] AS
MATCH ...
[WHERE ...]              // pre-aggregation filter
[ALONG ...]
[FOLD ...]
[REQUIRE ...]            // definitional threshold (constrains recursion)
[WHERE ...]              // post-FOLD filter (HAVING semantics)
[BEST BY ...]
YIELD ...
```

The first `WHERE` filters rows before aggregation. The second `WHERE` (after `FOLD`) filters aggregated groups — e.g., `WHERE count >= 3`.

## Rule References

### Unary

<!-- doctest: skip -->
```cypher
WHERE n IS suspicious
WHERE n IS NOT suspicious
```

### Binary/Tuple

<!-- doctest: skip -->
```cypher
WHERE a IS reachable TO b
WHERE (x, y, c) IS control
```

Variables bind the referenced rule's columns in one order: its `KEY` columns, then its other
columns, each in `YIELD` order. Subjects take columns from the front and a `TO` target takes the
next one, so against `YIELD KEY o, KEY e, pct` both `(o, e, v) IS stake` and `(o, e) IS stake TO v`
bind `v` to each matching fact's `pct`.

**Binding does not depend on the order of the `WHERE` conditions.** Each variable has exactly one
binder in a clause body — the `MATCH` pattern, a position of a positive `IS` reference, or a
generator output — and every other occurrence of it is a test (an equality against the binder).
`WHERE o IS seed AND (o, e, pct) IS stake` and `WHERE (o, e, pct) IS stake AND o IS seed` mean the
same thing and return the same rows.

A position also has a *kind* — a node, a relationship, or a plain value — taken from the referenced
rule's `YIELD` expression, not from whether the column is a `KEY`. A `KEY` can hold a scalar
(`YIELD KEY n, KEY t.code AS code`), and then `n IS tag TO c` binds `c` to that value, not to a
node. Putting a variable of one kind on a column of another (a relationship variable on a node
column, a node on a scalar column) is a compile error, `IsRefKindMismatch`: a node id compared with
an integer column would match whichever integers happen to equal a node id.

**`IS NOT`** runs after every positive condition, wherever it is written, so `NOT b IS blocked` may
precede the reference that binds `b`:

<!-- doctest: skip -->
```cypher
WHERE NOT b IS blocked, a IS link TO b    -- same as: a IS link TO b, NOT b IS blocked
```

Every variable on a `KEY` position of a negated reference must be bound somewhere in the body — by
`MATCH`, a positive `IS` reference or a generator; a node `KEY` needs a bound node. `IS NOT` cannot
test a value column: an unbound variable there is existential and ignored (`b IS NOT stake TO agg`
means `b IS NOT stake`), a bound one is a compile error.

### Reading Another Rule's Values

A positive reference also exposes the referenced rule's *value* columns — the ones none of its
subjects or target binds — by name, so a second rule can aggregate what a first one computed. `KEY`
columns are never exposed by name: read a `KEY` by binding it to a variable of your own.

<!-- doctest: skip -->
```cypher
CREATE RULE stake AS
MATCH (o:E)-[r:OWNS]->(e:E)
FOLD pct = MAX(r.pct)
YIELD KEY o, KEY e, pct

CREATE RULE exposure AS
MATCH (o:E), (e:E)
WHERE o IS stake TO e
FOLD total = MSUM(pct)
YIELD KEY e, total
```

If two references in one clause expose the same name, exposing it is fine, but *reading* it by its
bare name is a compile error, `AmbiguousRuleColumn` — nothing says which reference it means. Bind
each value positionally to a name of its own instead:

<!-- doctest: skip -->
```cypher
-- AmbiguousRuleColumn: both r1 and r2 yield `score`
WHERE a IS r1, a IS r2, score > 5

-- explicit
WHERE (a, s1) IS r1, (a, s2) IS r2, s1 > 5
```

(Before this rule the first reference in the `WHERE` silently won, so swapping two conditions
changed the answer.)

## Expression Functions in Rules

Cypher expression functions work inside `WHERE`, `ALONG`, `FOLD`, `BEST BY`, and `YIELD`. The `similar_to()` function is particularly useful for semantic scoring in rules:

```locy
// Filter by semantic similarity in WHERE
CREATE RULE relevant_docs AS
MATCH (q:Query)-[:ABOUT]->(topic:Topic)<-[:TAGGED]-(d:Document)
WHERE similar_to(d.embedding, q.text) > 0.7
YIELD KEY q, KEY d, similar_to(d.embedding, q.text) AS score

// Use as PROB value for probabilistic derivation
CREATE RULE related AS
MATCH (a:Paper)-[:CITES]->(b:Paper)
YIELD KEY a, KEY b, similar_to(b.embedding, a.embedding) AS PROB
```

`similar_to()` supports metric-aware vector scoring (Cosine, L2, Dot Product), FTS scoring, and multi-source hybrid fusion. See the [Vector Search guide](../guides/vector-search.md#similar_to-expression-function) for full documentation.

`PROB` can be written as `expr AS PROB`, `expr AS alias PROB`, or `expr PROB`. At most one output column per rule can be marked this way.

### Probabilistic Aggregation with MNOR and MPROD

```locy
// Noisy-OR: probability that at least one cause fires
CREATE RULE failure_risk AS
MATCH (c:Component)-[:HAS_SIGNAL]->(s:QualitySignal)
FOLD risk = MNOR(1.0 - s.pass_rate)
YIELD KEY c, risk

// Product: joint probability that all conditions hold
CREATE RULE vendor_reliability AS
MATCH (v:Vendor)-[:SUPPLIES]->(c:Component)
WHERE c IS failure_risk
FOLD reliability = MPROD(1.0 - failure_risk.risk)
YIELD KEY v, reliability
```

See [Probabilistic Logic](advanced/probabilistic-logic.md) for full documentation of MNOR and MPROD.

### Neural Predicates

Declare a learned scoring function and call it inside a rule:

```locy
CREATE MODEL failure_likelihood AS
  INPUT (a)
  FEATURES a.score
  OUTPUT PROB risk
  USING xervo('classify/failure-likelihood-v1')

CREATE RULE at_risk AS
  MATCH (a:Asset)
  YIELD KEY a, failure_likelihood(a.score) AS risk
```

Register a Python callable (or Rust `NeuralClassifier` impl) under the model name in `LocyConfig.classifier_registry` before running. Calibrate against held-out labels with `CALIBRATE failure_likelihood ON MATCH (a:Asset) TARGET a.actually_failed METHOD platt_scaling`; validate with `VALIDATE failure_likelihood ON MATCH (a:Asset) TARGET a.actually_failed METRICS brier_score, auc`. `EXPLAIN` traces every classifier invocation with raw and calibrated probabilities, a confidence band, and the feature dict that produced the score.

See [Neural Predicates](advanced/neural-predicates.md) for the full reference (every FEATURES source, every calibration method, every warning).

!!! note "Rule vs command expressions"
    In rule bodies (`WHERE`, `YIELD`, `ALONG`, `FOLD`), `similar_to()` runs inside DataFusion with full capability — metric-aware vector scoring, auto-embedding, FTS, and multi-source fusion. In command WHERE clauses (`DERIVE ... WHERE`, `ABDUCE ... WHERE`), only basic vector similarity (cosine) is available because commands execute on materialized rows after strata converge without schema context. Commands run in program order, and a command reads what earlier commands wrote (see [Commands run in sequence](rule-semantics.md#commands-run-in-sequence)).

## Goal Query

```locy
QUERY reachable WHERE a.name = 'Alice' RETURN b
```

`RETURN` takes Cypher's projection forms. An aggregate groups the rows by the
returned items that are not aggregates, as Cypher's `RETURN` does, and with no
such item an empty result is still one row:

```locy
QUERY reachable RETURN a.name AS from, count(*) AS n, collect(b.name) AS to
  ORDER BY n DESC LIMIT $k
```

A `WHERE` that cannot be evaluated, or is not a boolean, is an error rather
than a filter that drops every row.

## DERIVE in Rules (Graph Mutation)

Rules can use `DERIVE` instead of `YIELD` to directly write graph mutations:

```locy
// Infer a new edge from rule output
CREATE RULE infer_risk AS
MATCH (a:Account)-[:TRANSFER]->(b:Account)
WHERE a IS flagged
DERIVE (b)-[:RISK_FROM]->(a)

// Derive an edge to a marker node (DERIVE has no bare-node form)
CREATE RULE flag_accounts AS
MATCH (a:Account), (m:Marker)
WHERE a.fraud_score > 0.8
DERIVE (a)-[:FLAGGED_AS]->(m)
```

`DERIVE` rules run in Phase 2 (command dispatch) on converged derived facts. Use `YIELD` when you want to produce queryable derived facts; use `DERIVE` when you want to write mutations back to the graph.

## Derivation Commands

```locy
DERIVE reachable WHERE a.name = 'Alice'
```

## Hypothetical Reasoning

```locy
ASSUME {
  CREATE (:Node {name: 'Temp'})
} THEN {
  QUERY reachable RETURN b
}
```

The body may hold rules, `QUERY`, Cypher, `DERIVE` and nested `ASSUME`.
`ABDUCE` and `EXPLAIN RULE` in an ASSUME body are compile errors: run them at
the top level, or inside a transaction you roll back (`tx()` or
`session.scratch()`). See
[DERIVE, ASSUME, ABDUCE](advanced/derive-assume-abduce.md#assume).

## Abductive Reasoning

```locy
ABDUCE NOT reachable WHERE a.name = 'Alice' RETURN b
```

## Explainability

```locy
EXPLAIN RULE reachable WHERE a.name = 'Alice'
```

## Modules

<!-- doctest: skip -->
```cypher
MODULE acme.security
USE acme.common
```

For advanced semantics of `ALONG`, `FOLD`, `BEST BY`, and mutation reasoning, continue to the advanced pages.
