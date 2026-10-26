# Locy Python API Integration

## Setup

All Locy operations go through a **Session** (sync) or **AsyncSession** (async), created from a `Uni` instance:

```python
import uni_db

db = uni_db.Uni.open("./my-graph")   # or Uni.temporary() for in-memory
session = db.session()
```

## Sync API

```python
out = session.locy(program)
```

With parameters:

```python
out = session.locy(program, {"threshold": 0.5})
```

## Async API

```python
out = await session.locy(program)
```

With parameters:

```python
out = await session.locy(program, {"threshold": 0.5})
```

## Fluent Builder (`SessionLocyBuilder`)

For advanced configuration, use the builder API:

```python
out = (
    session.locy_with(program)
    .param("threshold", 0.5)
    .timeout(60.0)
    .max_iterations(500)
    .with_config({
        "max_explain_depth": 50,
        "max_abduce_candidates": 30,
        "max_abduce_results": 10,
        "max_derived_bytes": 64 * 1024 * 1024,
        "deterministic_best_by": True,
        # Probabilistic reasoning
        "strict_probability_domain": True,
        "probability_epsilon": 1e-15,
        "exact_probability": True,
        "max_bdd_variables": 1000,
        "top_k_proofs": 100,
        # Goal-directed QUERY evaluation: "auto" (default), "off", "on" or "shadow"
        "goal_directed": "on",
    })
    .run()
)
```

`goal_directed` takes `"auto"`, `"off"`, `"on"` or `"shadow"`
(case-insensitive; any other string raises `ValueError`). The same option is
the `goal_directed=` keyword of `uni_db.LocyConfig(...)`, which reports it
back as a lowercase string. Under the default `"auto"`, a `QUERY` whose
`WHERE` pins a node KEY column derives only what that goal reaches; its
answers are unchanged, and so is `result.derived`: a rule only such goals
needed is derived the first time `result.derived` is read, against the
snapshot the evaluation read, and the warnings that derivation raised are
appended to `result.warnings`. That first read blocks while it runs and raises
what the derivation raised; in async code use `await result.derived_async()`,
which derives on the runtime without blocking the event loop and returns the
same dict. `"off"` evaluates every rule in full; with `"on"` such
a rule is left out of `result.derived` rather than returned partial;
`"shadow"` answers from the rewrite while evaluating every rule. See
[Goal-directed QUERY](../advanced/modules-priority-query.md#goal-directed-query).

Builder methods:

| Method | Description |
|--------|-------------|
| `.param(name, value)` | Add a named parameter |
| `.params(dict)` | Add multiple parameters |
| `.timeout(seconds)` | Set evaluation timeout |
| `.max_iterations(n)` | Set recursion iteration cap |
| `.with_config(dict)` | Set full `LocyConfig` options |
| `.cancellation_token(token)` | Attach a cancellation token; cancelling it aborts the evaluation with `UniCancelledError` |
| `.run()` | Execute and return `LocyResult` |

## Compile-Only Check

Validate a program without evaluating it:

```python
compiled = session.compile_locy(program)
print(compiled.num_strata, compiled.num_rules, compiled.rule_names)
```

## Return Contract (`LocyResult`)

`LocyResult` is a class with the following attributes:

| Attribute | Type | Description |
|-----------|------|-------------|
| `derived` | `dict[str, list[dict]]` | Facts derived by each rule (keyed by rule name) |
| `stats` | `LocyStats` | Iteration counts, timing, stratum info |
| `command_results` | `list` | One result object per command: `QueryCommandResult`, `AssumeCommandResult`, `DeriveCommandResult`, `CypherCommandResult`, `ExplainCommandResult`, `AbduceCommandResult` |
| `warnings` | `list[dict]` | Runtime warnings (e.g., `SharedProbabilisticDependency`) |
| `approximate_groups` | `dict[str, list[str]]` | Rule/key groups that fell back to approximate probability mode |
| `derived_fact_set` | `DerivedFactSet` | Opaque fact set for `tx.apply()` materialization: every write of a session-level program (DERIVE output and Cypher writes), in program order |

Helper methods:

| Method | Description |
|--------|-------------|
| `.has_warning(code)` | Check if a specific warning code is present |
| `.warnings_list()` | Get all warnings |
| `.derived_facts(rule)` | Get derived facts for a specific rule |
| `.rows()` | Get result rows from the first `QUERY` or Cypher command, searching an `ASSUME`'s body results depth first |
| `.columns()` | Get query result column names |
| `.iterations` | Number of fixpoint iterations performed |

### ASSUME results

An `ASSUME ... THEN { ... }` returns an `AssumeCommandResult` with one result per
body command:

```python
result = session.locy("""
CREATE RULE link AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (a)-[:LINK]->(b)
CREATE RULE linked AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b
ASSUME { CREATE (:Marker) } THEN {
  QUERY linked RETURN a.name AS a, b.name AS b
  DERIVE link
  QUERY linked RETURN a.name AS a, b.name AS b
}
""")
assume = result.command_results[0]
before, derive, after = assume.results   # QueryCommandResult, DeriveCommandResult, QueryCommandResult
derive.affected                          # edges the body's DERIVE wrote
assume.derived["linked"]                 # facts after the mutations, before the body's first command
assume.rows                              # the first row-producing result's rows: `before.rows`
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `results` | `list` | One result per body command, in order; a nested ASSUME is an `AssumeCommandResult` |
| `derived` | `dict[str, list[dict]]` | Every rule's facts in the hypothetical state the body's first command reads |
| `rows` | `list[dict]` | The first row-producing body result's rows (depth first), or `[]` |

### Warnings

Each warning dict has:

```python
{
    "code": "shared_probabilistic_dependency",  # the snake_case code, see below
    "severity": "warning",    # "info" for a status report (shared_dependency_resolved)
    "message": "...",
    "rule_name": "supplier_risk",
    "variable_count": None,   # set for bdd_limit_exceeded, and after a fallback
    "key_group": None,        # set when a specific key group is affected
    "probability": {          # shared-dependency codes only, else None
        "inference_mode": "approximate",   # "exact", "top_k_proofs"
        "resolution": "not_attempted",     # "completed", "fallback"
        "reason": "exact_probability_disabled",  # or "bdd_variable_limit_exceeded"; None when completed
        "groups_detected": 1,              # KEY groups resting on shared evidence
        "groups_fallen_back": 1,           # of those, groups that kept the independence value
        "configured_bdd_variable_limit": None,  # max_bdd_variables, when resolution was attempted
        "effective_bdd_variable_limit": None,   # the limit applied (capped by the BDD library)
    },
}
```

Warning codes:

| Code | Meaning |
|------|---------|
| `shared_probabilistic_dependency` | MNOR/MPROD proof paths reuse the same evidence and the value was computed as if they did not: exact mode was off, or a group fell back. The result may overestimate; `probability` says which |
| `shared_dependency_resolved` | Informational (`severity == "info"`): proof paths share evidence, and exact (or top-k proof) inference evaluated every shared group without the independence assumption |
| `bdd_limit_exceeded` | `exact_probability` was enabled but group exceeded `max_bdd_variables`; fell back to independence mode |
| `cross_group_correlation_not_exact` | Shared evidence spans multiple aggregate groups; cross-group correlation is approximate |

With two suppliers depending on one facility event of probability 0.2, an
MNOR over the suppliers is 0.36 in ordinary mode, with
`shared_probabilistic_dependency` (`inference_mode: "approximate"`,
`resolution: "not_attempted"`), and 0.2 with `exact_probability=True`, with
`shared_dependency_resolved` (`inference_mode: "exact"`,
`resolution: "completed"`) and no overestimation warning.

Rows from approximate groups are marked with `_approximate = True` in the derived facts.

### Checking Warnings

```python
out = session.locy(program)

if out.warnings:
    for w in out.warnings:
        print(f"[{w['code']}] {w['message']}")

# Check for a specific warning code (the snake_case code string)
has_shared = out.has_warning("shared_probabilistic_dependency")

# Inspect which groups are approximate
for rule, groups in out.approximate_groups.items():
    print(f"Rule '{rule}' has approximate groups: {groups}")
```

## Notes

- Python API currently materializes query result lists.
- Use focused `QUERY` clauses to keep payloads manageable.
- A registered `AuthzPolicy` authorizes every Locy evaluation before it runs,
  as it does Cypher; see [Authorization](rust.md#authorization) for the
  actions a program asks for.

## Full API References

- [Main Python API Reference](../../reference/python-api.md)
- [Generated Python API Docs](../../api/python/index.md)
