# Implementation Plan - TCK List Comprehension

## Phase 1: Expression Compiler Infrastructure
- [ ] Task: Implement `CypherPhysicalExprCompiler` to bridge Cypher AST and DataFusion PhysicalExpr directly.
    - [ ] Create `crates/uni-query/src/query/df_graph/expr_compiler.rs`.
    - [ ] Implement recursive compilation logic handling schema extension.
    - [ ] Support mixing standard DF expressions with custom `ListComprehensionPhysicalExpr`.
- [ ] Task: Integrate Compiler into `HybridPhysicalPlanner`.
    - [ ] Replace `cypher_expr_to_df` + `DefaultPhysicalPlanner` pipeline in Projection/Filter.

## Phase 2: ListComprehension Implementation
- [ ] Task: Implement `ListComprehensionPhysicalExpr` evaluation logic.
    - [ ] Local Unnest / Flattening.
    - [ ] Inner expression evaluation on flattened batch.
    - [ ] Reconstruction of list offsets.
- [ ] Task: Verify with basic TCK scenarios (`[x IN list | x]`).

## Phase 3: Advanced Features
- [ ] Task: Implement `WHERE` clause support in comprehension.
- [ ] Task: Implement Outer Scope Capture (referencing variables outside comprehension).
- [ ] Task: Support `REDUCE` using the same compiler infrastructure.

## Phase 4: Verification
- [ ] Task: Conductor - User Manual Verification 'Phase 1-3: List Comprehensions' (Protocol in workflow.md)
