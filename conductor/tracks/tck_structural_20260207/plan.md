# Implementation Plan - TCK Structural Data & Dynamic Access

## Phase 1: Dynamic Access (Maps & Lists) [checkpoint: d51cb46]
- [x] Task: Implement dynamic Map access `m[prop_expr]` in DataFusion planner.
    - [x] Write reproduction tests.
    - [x] Update `df_expr.rs` to handle `Expr::MapIndex`.
- [x] Task: Standardize List indexing `list[idx_expr]` behavior (negative indices, Null handling).
    - [x] Write tests for edge cases.
    - [x] Implement fix in `df_expr.rs` and `expr_eval.rs`.
- [x] Task: Conductor - User Manual Verification 'Phase 1: Dynamic Access' (Protocol in workflow.md) d51cb46

## Phase 2: Collection Functions [checkpoint: d51cb46]
- [x] Task: Standardize `keys()` for all structural types (Node, Relationship, Map).
    - [x] Write tests for structural `keys()`.
    - [x] Update `KeysUdf` in `df_udfs.rs`.
- [x] Task: Implement/Standardize `labels()`, `nodes()`, and `relationships()` functions.
    - [x] Write tests.
    - [x] Implement UDFs or planner translations.
- [x] Task: Conductor - User Manual Verification 'Phase 2: Collection Functions' (Protocol in workflow.md) d51cb46

## Phase 3: List Comprehensions & Advanced Collections (Deferred)

- [ ] Task: Fix List Comprehension `[x IN list WHERE pred | expr]` scoping and mixed types.

- [ ] Task: Standardize `REDUCE`, `ALL`, `ANY`, `SINGLE`, `NONE` semantics.

- [ ] Task: Conductor - User Manual Verification 'Phase 3: List Comprehensions' (Protocol in workflow.md)
