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
        # Goal-directed QUERY evaluation: "off" (default), "on" or "shadow"
        "goal_directed": "on",
    })
    .run()
)
```

`goal_directed` takes `"off"`, `"on"` or `"shadow"` (case-insensitive; any
other string raises `ValueError`). The same option is the `goal_directed=`
keyword of `uni_db.LocyConfig(...)`, which reports it back as a lowercase
string. With `"on"`, a `QUERY` whose `WHERE` pins a node KEY column derives
only what that goal reaches; its answers are unchanged, but a rule only such
goals needed is left out of `result.derived` rather than returned partial.
`"shadow"` answers from the rewrite while keeping `derived` complete. See
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
    "code": "SharedProbabilisticDependency",  # or "BddLimitExceeded", "CrossGroupCorrelationNotExact"
    "message": "...",
    "rule_name": "supplier_risk",
    "variable_count": None,   # set for BddLimitExceeded
    "key_group": None,        # set when a specific key group is affected
}
```

Warning codes:

| Code | Meaning |
|------|---------|
| `SharedProbabilisticDependency` | Multiple MNOR/MPROD proof paths reuse the same evidence; result is approximate |
| `BddLimitExceeded` | `exact_probability` was enabled but group exceeded `max_bdd_variables`; fell back to independence mode |
| `CrossGroupCorrelationNotExact` | Shared evidence spans multiple aggregate groups; cross-group correlation is approximate |

Rows from approximate groups are marked with `_approximate = True` in the derived facts.

### Checking Warnings

```python
out = session.locy(program)

if out.warnings:
    for w in out.warnings:
        print(f"[{w['code']}] {w['message']}")

# Check for a specific warning code
has_shared = out.has_warning("SharedProbabilisticDependency")

# Inspect which groups are approximate
for rule, groups in out.approximate_groups.items():
    print(f"Rule '{rule}' has approximate groups: {groups}")
```

## Notes

- Python API currently materializes query result lists.
- Use focused `QUERY` clauses to keep payloads manageable.

## Full API References

- [Main Python API Reference](../../reference/python-api.md)
- [Generated Python API Docs](../../api/python/index.md)
