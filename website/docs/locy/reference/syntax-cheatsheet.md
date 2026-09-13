# Locy Syntax Cheatsheet

## Rule

```cypher
CREATE RULE name [PRIORITY n] AS
MATCH ...
[WHERE ...]                         // pre-aggregation filter
[ALONG x = expr]
[FOLD agg = aggregate(expr)]
[REQUIRE agg_condition]             // definitional threshold (constrains recursion)
[WHERE agg_condition]               // post-FOLD filter (HAVING)
[BEST BY expr ASC|DESC]
YIELD KEY a, value AS alias, prob_expr AS PROB
// OR, for graph mutation rules (edge/node props are inline maps, no SET):
DERIVE (src)-[:TYPE {prop: expr}]->(dst)
```

The second `WHERE` (after `FOLD`) filters on aggregated values — equivalent to SQL's `HAVING`. It can reference FOLD output columns and KEY columns.

`REQUIRE` takes the same kind of condition but belongs to the rule's *definition*: in a recursive rule it is applied to every iteration, so it constrains what the rule derives, where the post-FOLD `WHERE` only filters the converged answer. It must be one-way — a lower bound over a non-decreasing fold (`MSUM`, `MMAX`, `MCOUNT`, `MNOR`) or an upper bound over a non-increasing one (`MMIN`, `MPROD`) — or the compiler rejects it.

### FOLD Aggregators

| Operator | Semantics | Use In Recursion? |
|----------|-----------|-------------------|
| `COUNT(*)` / `COUNT(expr)` | Row count | Safe (unbounded) |
| `SUM(expr)` | Arithmetic sum | Non-recursive only |
| `AVG(expr)` | Arithmetic mean | Non-recursive only |
| `MIN(expr)` | Minimum value | Safe in recursion |
| `MAX(expr)` | Maximum value | Safe in recursion |
| `COLLECT(expr)` | Collect into list | Safe (unbounded) |
| `MSUM(expr)` | Monotonic sum (non-decreasing) | Safe (unbounded) |
| `MMAX(expr)` | Monotonic maximum | Safe in recursion |
| `MMIN(expr)` | Monotonic minimum | Safe in recursion |
| `MCOUNT(expr)` | Monotonic count | Safe (unbounded) |
| `MNOR(expr)` | Noisy-OR probability: `1 − ∏(1 − pᵢ)` | Safe in recursion |
| `MPROD(expr)` | Product probability: `∏ pᵢ` | Safe in recursion |

Recursion safety comes from the aggregate's registered semilattice (`monotone_join`), not from an `M` prefix; the compiler falls back to the six built-in `M*` names only when the aggregate registry has no entry. `SUM` and `AVG` are non-monotone and are rejected inside a recursive stratum.

The aggregates marked *unbounded* are monotone but have no top element, so a recursive fold over one can iterate until `max_iterations` rather than converging — the iteration cap is the backstop. The exception is `COLLECT`, which is assembled after the fixpoint rather than tracked inside it, so it does not itself keep the loop iterating.

## Goal Query

```cypher
QUERY name [WHERE ...] [RETURN ...]
```

## Derive Command

```cypher
DERIVE name [WHERE ...]
```

## Explain

```cypher
EXPLAIN RULE name [WHERE ...]
```

## Assume

<!-- doctest: skip -->
```cypher
ASSUME { <cypher mutations> } THEN { <locy/cypher body> }
```

## Abduce

```cypher
ABDUCE [NOT] name [WHERE ...] [RETURN ...]
```

## Neural Predicates

<!-- doctest: skip -->
```cypher
CREATE MODEL name AS
  INPUT (binding [: Label])
  [FEATURES feature_expr (, feature_expr)*]
  [FEATURES (subject, column) FROM source_rule]
  OUTPUT (PROB | SCORE | LABEL | VECTOR) result_name
  USING xervo('provider_alias' [, embedder = 'embed_alias'])
  [CALIBRATION (platt_scaling | isotonic_regression | temperature_scaling | beta_calibration | dirichlet | conformal | conformal(alpha) | none)]
  [VERSION 'string']

CALIBRATE model_name ON MATCH pattern [WHERE expr] TARGET expr METHOD calibration_method [HOLDOUT n]
VALIDATE  model_name ON MATCH pattern [WHERE expr] TARGET expr METRICS metric (, metric)*
```

`metric` ∈ `brier_score | ece | debiased_ece | accuracy | log_loss | auc`. The classifier-registry key is the `CREATE MODEL <name>`, not the `USING xervo('alias')` provider hint. The feature dict the callable receives is keyed by the `INPUT` binding name; values are the evaluated argument expressions at the call site. See [Neural Predicates](../advanced/neural-predicates.md).

## Modules

<!-- doctest: skip -->
```cypher
MODULE my.module
USE shared.rules
```
