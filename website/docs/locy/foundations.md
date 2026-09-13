# Locy Foundations

## Mental Model

Locy evaluates rules over a property graph and produces **derived facts**. Those derived facts can then be queried, explained, or materialized back into the graph.

## Core Concepts

### Base Facts vs Derived Facts

- Base facts: persisted nodes/edges/properties.
- Derived facts: inferred rows produced by rule evaluation.

### Strata and Fixpoint

Locy groups rules into strata using dependency analysis:

- Positive recursion evaluates until no new facts appear.
- Negation is restricted via stratification to preserve correctness.

### Rule Anatomy

```locy
CREATE RULE reachable AS
MATCH (a:Node)-[:EDGE]->(b:Node)
YIELD KEY a, KEY b
```

A rule typically has:

- `MATCH` pattern
- Optional conditions (`WHERE`, `IS`, `IS NOT`)
- Optional path or aggregate clauses (`ALONG`, `FOLD`, `REQUIRE`, post-FOLD `WHERE`, `BEST BY`)
- Output (`YIELD` or `DERIVE`)

## Safety and Predictability

- Rule schemas are validated at compile time.
- Cyclic negation is rejected.
- `LocyConfig` lets you cap iterations, memory, and timeout.
- Probabilistic guardrails: `top_k_proofs` limits proof enumeration per aggregate group, `exact_probability` enables BDD-based exact evaluation for shared-proof groups (capped by `max_bdd_variables`).

## Next

Continue with [Quickstart](quickstart.md) for an end-to-end example.
