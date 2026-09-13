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
    In rule bodies (`WHERE`, `YIELD`, `ALONG`, `FOLD`), `similar_to()` runs inside DataFusion with full capability — metric-aware vector scoring, auto-embedding, FTS, and multi-source fusion. In command WHERE clauses (`DERIVE ... WHERE`, `ABDUCE ... WHERE`), only basic vector similarity (cosine) is available because commands execute on materialized rows after strata converge without schema context.

## Goal Query

```locy
QUERY reachable WHERE a.name = 'Alice' RETURN b
```

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
