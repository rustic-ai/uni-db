# Implementation Plan - TCK Structural Data & Dynamic Access

## Phase 1: Dynamic Access (Maps & Lists)
- [ ] Task: Implement dynamic Map access `m[prop_expr]` in DataFusion planner.
    - [ ] Write reproduction tests.
    - [ ] Update `df_expr.rs` to handle `Expr::MapIndex`.
- [ ] Task: Standardize List indexing `list[idx_expr]` behavior (negative indices, Null handling).
    - [ ] Write tests for edge cases.
    - [ ] Implement fix in `df_expr.rs` and `expr_eval.rs`.
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Dynamic Access' (Protocol in workflow.md)

## Phase 2: Collection Functions
- [ ] Task: Standardize `keys()` for all structural types (Node, Relationship, Map).
    - [ ] Write tests for structural `keys()`.
    - [ ] Update `KeysUdf` in `df_udfs.rs`.
- [ ] Task: Implement/Standardize `labels()`, `nodes()`, and `relationships()` functions.
    - [ ] Write tests.
    - [ ] Implement UDFs or planner translations.
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Collection Functions' (Protocol in workflow.md)

## Phase 3: List Comprehensions & Advanced Collections
- [ ] Task: Fix List Comprehension `[x IN list WHERE pred | expr]` scoping and mixed types.
    - [ ] Write tests for nested and mixed-type comprehensions.
    - [ ] Update vectorized operator or fallback evaluator.
- [ ] Task: Standardize `REDUCE`, `ALL`, `ANY`, `SINGLE`, `NONE` semantics.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: List Comprehensions' (Protocol in workflow.md)
