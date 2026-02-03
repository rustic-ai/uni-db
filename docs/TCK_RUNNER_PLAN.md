# OpenCypher TCK Runner Implementation Plan

**Date**: 2026-01-24
**Status**: Draft
**Target Crate**: `crates/uni-tck`

This document outlines the step-by-step plan to build a compliance runner for the OpenCypher Technology Compatibility Kit (TCK) against the `uni` database engine.

## 1. Architecture Overview

The runner bridges the official Gherkin (`.feature`) test files with the `uni` Rust API.

*   **Test Runner**: `cucumber` (Rust crate)
*   **Database**: `uni::Uni` (In-memory mode)
*   **Test Source**: `cypher-tck/tck-M23/tck/features`
*   **Verification**: Compares `uni::QueryResult` against TCK expectation tables.

## 2. Implementation Phases

### Phase 1: Scaffolding & Scalars (Literals)
**Goal**: Run minimal tests validating `RETURN` statements with scalar values (Int, Float, Bool, String, Null).

#### Task 1.1: Project Setup
- [ ] Create new binary crate `crates/uni-tck`.
- [ ] Add to workspace `members` in root `Cargo.toml`.
- [ ] Configure dependencies in `crates/uni-tck/Cargo.toml`:
    - `cucumber` (latest)
    - `tokio` (macros, rt-multi-thread)
    - `futures`
    - `anyhow`
    - `uni-db` (path: `../uni`)
    - `uni-query` (path: `../uni-query`)
    - `uni-common` (path: `../uni-common`)

#### Task 1.2: World Implementation (`src/world.rs`)
- [ ] Define `UniWorld` struct:
    ```rust
    pub struct UniWorld {
        pub db: Option<Uni>,
        pub last_result: Option<QueryResult>,
        pub last_error: Option<UniError>,
    }
    ```
- [ ] Implement `cucumber::World` trait with `async` support.
- [ ] Implement `Default` to initialize empty state.

#### Task 1.3: Basic Steps (`src/steps/general.rs`)
- [ ] `Given any graph`: Initialize `Uni::in_memory().build()`.
- [ ] `Given an empty graph`: Same as above.
- [ ] `When executing query: <doc_string>`:
    - Execute `db.query()`.
    - Store success in `last_result`.
    - Store failure in `last_error`.

#### Task 1.4: Scalar Type Conversion (`src/conversions.rs`)
- [ ] Implement parser `parse_tck_value(s: &str) -> Value`:
    - `'str'` -> `Value::String`
    - `123` -> `Value::Int`
    - `12.3` -> `Value::Float`
    - `true`/`false` -> `Value::Bool`
    - `null` -> `Value::Null`

#### Task 1.5: Assertion Steps (`src/steps/results.rs`)
- [ ] `Then the result should be, in any order:`
    - Parse Gherkin table.
    - Assert `last_result` exists.
    - Check column names match.
    - Compare rows (sorting required for "in any order").
- [ ] `And no side effects`: Implement as no-op or read-only check.

#### Task 1.6: Verification
- [ ] Run `cargo run -p uni-tck -- --filter Literals`
- [ ] Pass `Literals1.feature` through `Literals8.feature`.

---

### Phase 2: Graph Structure (Nodes & Relationships)
**Goal**: Verify pattern matching logic (`MATCH`, `CREATE`, `MERGE`) by comparing graph elements.

#### Task 2.1: Graph Element Parsing
- [ ] Extend `parse_tck_value` to handle TCK node/edge syntax:
    - Node: `(:Label {prop: val})`
    - Edge: `[:TYPE {prop: val}]`
    - Path: `<(...)--(...)>`
- [ ] Implement a `Matcher` trait to compare TCK patterns against `uni::Value::Node`/`Edge`.
    - *Note*: TCK nodes in tables don't usually verify ID, just Labels + Properties.

#### Task 2.2: Setup Steps
- [ ] `Given having executed: <doc_string>`:
    - Execute setup queries (e.g., `CREATE (n)`).
    - Ensure DB state persists to the "When" step.

#### Task 2.3: Verification
- [ ] Run `Match*.feature`.
- [ ] Run `Create*.feature`.

---

### Phase 3: Complex Types & Errors
**Goal**: Support Lists, Maps, and expected Error states.

#### Task 3.1: Complex Conversions
- [ ] List: `[1, 2, 3]`
- [ ] Map: `{a: 1, b: 2}`

#### Task 3.2: Error Assertions
- [ ] `Then a <ErrorType> should be raised at <...>`:
    - Check `last_error`.
    - Map `UniError` variants to TCK error types (e.g., `SyntaxError`, `TypeError`).

---

### Phase 4: CI Integration
**Goal**: Run TCK as part of the build process.

- [ ] Create `run_tck.sh` script.
- [ ] Filter out known unsupported features (e.g., `ListComprehensions`).
- [ ] Add to GitHub Actions workflow.
