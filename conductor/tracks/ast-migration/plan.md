# Implementation Plan - True AST Migration

- [x] Phase 1: Update Type Definitions
    - [x] Update `planner.rs` imports to use `uni_cypher::ast::Expr`
    - [x] Update `LogicalPlan` enum in `planner.rs` (14 variants)
    - [x] Update helper structs in `planner.rs` (AnyInPredicate, etc.)
    - [x] Remove legacy `Expr` and `Operator` imports

- [x] Phase 2: Update Planner Expression Handling
    - [x] Rename `Expr::Identifier` to `Expr::Variable`
    - [x] Update Operator enum references
    - [x] Update `plan_single` (partial)
    - [x] Update `plan_schema_command` (partial)

- [ ] Phase 3: Update Executor Expression Evaluation
    - [ ] Update `evaluate_expr` (FAILED: Large block replacement failed)
    - [x] Update `eval_binary_op` in `expr_eval.rs`

- [x] Phase 4: Update DataFusion Integration
    - [x] Update `cypher_expr_to_df` signature in `df_expr.rs`
    - [x] Update pattern matching in `df_expr.rs`

- [ ] Phase 5: Update Write Executor
    - [ ] Update `executor/write.rs` pattern construction (FAILED: Large block replacement failed)

- [x] Phase 6: Delete Legacy Code
    - [x] Delete `crates/uni-query/src/query/ast.rs`
    - [x] Delete `crates/uni-query/src/query/ast_adapter.rs`
    - [x] Delete `crates/uni-query/src/query/ast_convert.rs`
    - [x] Delete `crates/uni-query/src/query/expr.rs`
    - [x] Remove module declarations in `crates/uni-query/src/query/mod.rs`

- [ ] Phase 7: Integration Testing
    - [ ] Run tests (Blocked by compilation errors)