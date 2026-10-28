# Locy Rust API Integration

## Primary Entry Point

All Locy operations go through a **Session**, created from a `Uni` instance:

```rust
let session = db.session();
```

## Core Methods

```rust
// Simple evaluation (takes only the program string)
let result = session.locy(program).await?;

// With inline parameters — use the builder, not a params argument
let result = session.locy_with(program)
    .param("threshold", 0.5)
    .run()
    .await?;

// Compile-only check (no evaluation)
let compiled = session.compile_locy(program)?;

// Explain a Locy program — terminal on the builder, returns LocyExplainOutput
let explain = session.locy_with(program).explain()?;
```

## Fluent Builder (`LocyBuilder`)

For advanced configuration, use the builder API:

```rust
use std::time::Duration;

let result = session.locy_with(program)
    .param("threshold", 0.5)
    .timeout(Duration::from_secs(60))
    .max_iterations(500)
    .with_config(cfg)
    .run()
    .await?;
```

Builder methods:

| Method | Description |
|--------|-------------|
| `.param(name, value)` | Add a named parameter |
| `.params(map)` | Add multiple parameters |
| `.timeout(duration)` | Set evaluation timeout (`std::time::Duration`). Replaces the database `query_timeout` for this evaluation, above or below it |
| `.max_iterations(n)` | Set recursion iteration cap |
| `.with_config(cfg)` | Set full `LocyConfig` options (taken by value) |
| `.cancellation_token(token)` | Attach a cancellation token; cancelling it aborts the evaluation with `UniError::Cancelled` |
| `.run()` | Execute and return `LocyResult` |
| `.explain()` | Compile and return `LocyExplainOutput` without evaluating |

## Config (`LocyConfig`)

Common fields:

- `max_iterations`
- `timeout` (`Option<Duration>`; `None` = 300s between strata, database `query_timeout` inside operators)
- `max_explain_depth`
- `max_slg_depth` (deprecated, no effect; using it is a deprecation warning)
- `goal_directed` (`GoalDirected::Auto` default, `Off`, `On`, `Shadow`): magic-sets evaluation of `QUERY ... WHERE`; see [Modules, PRIORITY, QUERY](../advanced/modules-priority-query.md#goal-directed-query)
- `max_abduce_candidates`
- `max_abduce_results`
- `max_derived_bytes`
- `deterministic_best_by`
- `strict_probability_domain`
- `probability_epsilon`
- `exact_probability`
- `max_bdd_variables`
- `top_k_proofs`

## Result Shape (`LocyResult`)

- every rule's facts, through `result.derived_rules().await?` (a `HashMap<String, Vec<FactRow>>`, `FactRow = HashMap<String, Value>`) or `result.derived_rule(name).await?`. Async and fallible: a rule the default goal-directed mode left out is derived on that first read, against the evaluation's snapshot. `result.evaluated_rules()` is the synchronous view of what the evaluation itself derived; `result.lazily_derived_rules()` names the rest
- `stats: LocyStats`
- `command_results: Vec<CommandResult>`
- `warnings: Vec<RuntimeWarning>`
- `approximate_groups: HashMap<String, Vec<String>>`
- `derived_fact_set: Option<DerivedFactSet>` — opaque fact set for `tx.apply()` materialization: every write of a session-level program (DERIVE output and Cypher writes), in program order

Helpers:

- `result.rows()`
- `result.columns()`
- `result.stats()`
- `result.warnings()`
- `result.has_warning(...)`
- `result.derived_rule(rule_name).await?`
- `result.iterations()`

## Profile Output (`LocyProfileOutput`)

```rust
let (result, profile) = session.locy_with(program).profile().await?;
println!("{profile}");
```

`profile.profile.strata[i].rules[j].iterations[k]` is one rule's evaluation in one
fixpoint pass: `delta_facts`, `elapsed_ms`, `planning_ms` / `plans` (clause bodies are
re-planned every pass), and `operators`. Besides the physical-plan operators of the clause
bodies, `operators` lists the steps that run outside a plan, with `actual_rows` = rows kept:

- `LocyIsNotAntiJoin` (or `LocyIsNotComplement` under PROB) — one per `IS NOT` reference applied
- `LocyFactDedup`, `LocyBestByMerge` or `LocyFoldMerge` — the fact merge, once per pass

`result.metrics().physical_plans` counts every physical plan the program built.

Note: `result.rows()` returns the rows of the first row-producing command in program order:
a `QUERY` or a Cypher statement, with an `ASSUME { ... } THEN { ... }` block searched depth
first through its body's results. The Python `rows()` method follows the same rule. Use
`result.command_results()` to reach a specific command's result.

An ASSUME returns `CommandResult::Assume { results, derived }`: `results` holds one
`CommandResult` per body command, in order (a nested ASSUME's own `Assume` among them), and
`derived` every rule's facts in the hypothetical state the body's first command reads.
`CommandResult::as_assume()` returns both; `CommandResult::rows()` is the first
row-producing result, depth first.

```rust
let result = session
    .locy("... ASSUME { CREATE (:Marker) } THEN { QUERY linked RETURN a  DERIVE link  QUERY linked RETURN a }")
    .await?;
let (results, derived) = result.command_results()[0].as_assume().expect("an ASSUME");
assert!(matches!(results[1], CommandResult::Derive { .. }));
let after_derive = results[2].rows().expect("the second QUERY's rows");
```

## Explain Output (`LocyExplainOutput`)

```rust
let explain = session.locy_with(program).explain()?;
```

`explain()` is the terminal method on the builder — it compiles the program and returns the logical plan and compilation details without executing it.

## Compile-Only (`CompiledProgram`)

```rust
let compiled = session.compile_locy(program)?;
println!("strata: {}, rules: {}", compiled.num_strata(), compiled.num_rules());
println!("rule names: {:?}", compiled.rule_names());
```

Validates the program structure and returns metadata without evaluation.

## Authorization

When an `AuthzPolicy` is registered, every Locy evaluation is authorized
before any of it runs: `session.locy`, `tx.locy`, their `locy_with` builders
(`run` and `profile`), and prepared programs. The principal is the session's
or transaction's, and the resource path is the program text. A program asks
for:

- `read`, for what it evaluates: rules, `QUERY`, `EXPLAIN RULE`, `ABDUCE`,
  and read-only Cypher commands;
- one more action per command that writes, the action Cypher uses for that
  statement: `write` for a Cypher write and for `DERIVE`, `schema` for a DDL
  procedure, `dbms` for an administrative one (`uni.admin.*`). `ASSUME`
  mutations ask for `write` too, although they are discarded.

A `DERIVE` request names what the equivalent Cypher write would: the labels,
relationship types and inline property keys of the patterns its rule matches
— and of every rule that rule reaches through `IS`, since those bind what it
writes — together with those it creates (`MERGED_WITH` for `DERIVE MERGE`).
A policy keyed on a label therefore sees a derive over that label.

`tx.apply(derived)` asks for `write` for each statement it replays, with the
path `<locy-apply>`. A refused program returns
`UniError::AuthorizationDenied` and has no effect.

## Full API References

- [Main Rust API Reference](../../reference/rust-api.md)
- [Internals: Compiler](../../internals/locy/compiler.md)
