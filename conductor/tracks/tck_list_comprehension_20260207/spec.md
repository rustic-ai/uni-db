# Track Specification - TCK List Comprehension

## Objective
Achieve compatibility with openCypher TCK for List Comprehensions `[x IN list WHERE pred | expr]` and `REDUCE` functions by implementing them in the DataFusion execution engine.

## Current State
- `List12 - List Comprehension`: 14% pass rate.
- Currently falls back to the legacy row-based executor (`read.rs`), which supports them but is slow and lacks integration with DataFusion operators (like vector search).
- DataFusion does not yet natively support Lambda functions (e.g., `list_transform(list, x -> x + 1)`) in a way that is exposed to SQL/LogicalPlan easily for us.

## Strategy: Custom Physical Expression
To avoid the overhead of `UNNEST` + `GROUP BY` (shuffle) and the lack of native Lambdas, we will implement a custom **DataFusion Physical Expression** (`ListComprehensionPhysicalExpr`).

### Architecture
1.  **Logical Plan**: Keep `Expr::ListComprehension`.
2.  **Planner**: `HybridPhysicalPlanner` translates `Expr::ListComprehension` into a custom `PhysicalExpr` instead of a UDF.
3.  **Physical Expression**: `ListComprehensionPhysicalExpr` struct implementing `PhysicalExpr`.
    *   **Inputs**:
        *   `list_expr`: PhysicalExpr for the source list.
        *   `predicate_expr`: Optional PhysicalExpr for `WHERE`.
        *   `map_expr`: PhysicalExpr for the projection `| expr`.
        *   `variable_name`: Name of the loop variable.
    *   **Execution (`evaluate`)**:
        *   Evaluates `list_expr` to get `ListArray`.
        *   **Flattening**: Constructs a new `RecordBatch` where the loop variable column contains ALL elements from ALL lists in the batch (essentially local unnest).
        *   **Context**: Repeats/Takes other columns if they are referenced in the inner expressions (outer scope capture).
        *   **Evaluation**:
            *   Evaluates `predicate_expr` on the flattened batch.
            *   Filters the flattened batch.
            *   Evaluates `map_expr` on the filtered batch.
        *   **Reconstruction**: Re-groups the results into `ListArray` based on original offsets.

### Advantages
- **Vectorized**: Inner expressions run on batches (flattened arrays), leveraging DataFusion's SIMD capabilities.
- **Local**: No shuffle or global coordination required.
- **Composable**: Inner expressions can be any valid DataFusion PhysicalExpr.

## Target Features
- [ ] Implement `ListComprehensionPhysicalExpr` (struct, evaluate, etc.).
- [ ] Update `HybridPhysicalPlanner` to compile `ListComprehension` to this expr.
- [ ] Handle variable scoping (inner variable `x` + outer variables).
- [ ] Implement `REDUCE` using similar pattern (`ReducePhysicalExpr`).

## Success Criteria
- `List12` pass rate > 90%.
- Performance improvement over fallback executor.
