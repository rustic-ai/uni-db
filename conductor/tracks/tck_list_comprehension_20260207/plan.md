# Implementation Plan - TCK List Comprehension

## Phase 1: Expression Compiler Infrastructure
- [x] Task: Implement `CypherPhysicalExprCompiler` to bridge Cypher AST and DataFusion PhysicalExpr directly.
    - [x] Create `crates/uni-query/src/query/df_graph/expr_compiler.rs`.
    - [x] Implement recursive compilation logic handling schema extension.
    - [x] Support mixing standard DF expressions with custom `ListComprehensionPhysicalExpr`.
- [x] Task: Integrate Compiler into `HybridPhysicalPlanner`.
    - [x] Replace `cypher_expr_to_df` + `DefaultPhysicalPlanner` pipeline in Projection/Filter.

## Phase 2: ListComprehension Implementation
- [x] Task: Implement `ListComprehensionPhysicalExpr` evaluation logic.
    - [x] Local Unnest / Flattening.
    - [x] Inner expression evaluation on flattened batch.
    - [x] Reconstruction of list offsets.
- [x] Task: Verify with basic TCK scenarios (`[x IN list | x]`).

## Phase 3: Advanced Features
- [ ] Task: Implement `WHERE` clause support in comprehension.
- [ ] Task: Implement Outer Scope Capture (referencing variables outside comprehension).
- [ ] Task: Support `REDUCE` using the same compiler infrastructure.

## Phase 4: Verification
- [ ] Task: Conductor - User Manual Verification 'Phase 1-3: List Comprehensions' (Protocol in workflow.md)
