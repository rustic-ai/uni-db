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
- `max_slg_depth`
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

- `derived: HashMap<String, Vec<FactRow>>` (where `FactRow = HashMap<String, Value>`)
- `stats: LocyStats`
- `command_results: Vec<CommandResult>`
- `warnings: Vec<RuntimeWarning>`
- `approximate_groups: HashMap<String, Vec<String>>`
- `derived_fact_set: DerivedFactSet` — opaque fact set for `tx.apply()` materialization

Helpers:

- `result.rows()`
- `result.columns()`
- `result.stats()`
- `result.warnings()`
- `result.has_warning(...)`
- `result.derived_facts(rule_name)`
- `result.iterations`

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

## Full API References

- [Main Rust API Reference](../../reference/rust-api.md)
- [Internals: Compiler](../../internals/locy/compiler.md)
