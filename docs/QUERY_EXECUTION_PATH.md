# Query Execution Path

**Date**: 2026-02-20
**Purpose**: Document the dual execution strategy (DataFusion + Legacy Executor)

---

## Overview

Uni uses a **hybrid execution model** with DataFusion as the primary engine and a legacy row-based executor as fallback:

```
Query → Parser → Planner → Executor.execute()
                              ├─→ DataFusion (vectorized, optimized)
                              └─→ Legacy Executor (row-by-row, full feature support)
```

## Execution Strategy

Located in: `crates/uni-query/src/query/executor/read.rs:305-340`

```rust
pub fn execute(&self, plan: LogicalPlan, ...) -> Result<Vec<HashMap<String, Value>>> {
    if Self::is_ddl_or_admin(&plan) {
        // DDL/Admin → Always use legacy executor
        self.execute_subplan(plan, prop_manager, params, ctx).await
    } else {
        // Read queries → Try DataFusion first
        match self.execute_datafusion(plan.clone(), prop_manager).await {
            Ok(batches) => self.record_batches_to_rows(batches),
            Err(e) => {
                // Fall back to legacy executor
                self.execute_subplan(plan, prop_manager, params, ctx).await
            }
        }
    }
}
```

---

## DataFusion Path (Vectorized)

### When Used
- Read queries (MATCH, RETURN, WITH, aggregations)
- **Mutation queries** (CREATE, SET, REMOVE, DELETE, MERGE, FOREACH) — default since M3/M4
- Queries with expressions that can be translated to DataFusion

### Advantages
- Vectorized execution (SIMD)
- Columnar processing with Apache Arrow
- Query optimization (predicate pushdown, projection pushdown)
- Parallel execution
- Memory-efficient streaming

### Translation Layer
Located in: `crates/uni-query/src/query/df_expr.rs`

Converts Cypher AST to DataFusion expressions:
- Arithmetic operators
- Comparison operators
- Boolean logic (AND, OR, NOT)
- String operations (CONTAINS, STARTS WITH, ENDS WITH)
- Array indexing and slicing
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Math functions (ABS, CEIL, FLOOR, LOG, SIN, COS, etc.)
- String functions (UPPER, LOWER, TRIM, SUBSTRING, etc.)

### What Falls Back to Legacy
Located in: `df_expr.rs` - expressions that return `Err(anyhow!(...)`

**Blocked by DataFusion Limitations:**
1. **Quantifier expressions** (ALL/ANY/SINGLE/NONE)
   - Requires lambda functions
   - Tracked: https://github.com/apache/datafusion/issues/14205
   - Example: `ALL(x IN list WHERE x > 0)`

2. **List comprehensions** (not implemented in parser yet)
   - Also requires lambda functions
   - Example: `[x IN list WHERE x > 0 | x * 2]`

3. **Subqueries** (EXISTS, scalar subqueries)
   - Correlated subquery support limited
   - Example: `WHERE EXISTS { MATCH (n)--() }`

4. **Pattern comprehensions**
   - Not yet implemented in parser
   - Example: `[p = (a)-[:KNOWS]->(b) | b.name]`

---

## Mutation Execution

All mutation types route through DataFusion by default via `MutationExec` operators. The only
remaining fallback trigger for mutations is **LOAD CSV** (not yet supported in the DF engine).

### Mutation Routing Table

| Mutation Type | DataFusion Operator | Default Path | Fallback Trigger |
|---------------|-------------------|--------------|-----------------|
| CREATE | `MutationCreateExec` | DF | LOAD CSV, config toggle |
| SET | `MutationSetExec` | DF | LOAD CSV, config toggle |
| REMOVE | `MutationRemoveExec` | DF | LOAD CSV, config toggle |
| DELETE | `MutationDeleteExec` | DF | LOAD CSV, config toggle |
| MERGE | `MutationMergeExec` | DF | LOAD CSV, config toggle |
| FOREACH | `ForeachExec` | DF | LOAD CSV, config toggle |

### Rollback Toggle

Mutations can be rolled back to the legacy fallback path per-clause or globally using
`MutationPathConfig`. This is a runtime configuration — no recompilation needed.

**Disable all mutations (global rollback):**
```rust
use uni_db::{Uni, UniConfig, MutationPathConfig};

let config = UniConfig {
    mutation_path: MutationPathConfig::all_disabled(),
    ..Default::default()
};
let db = Uni::in_memory().config(config).build().await?;
```

**Disable a single clause:**
```rust
use uni_db::{Uni, UniConfig, MutationPathConfig};

let config = UniConfig {
    mutation_path: MutationPathConfig {
        merge_enabled: false,  // Only MERGE uses fallback
        ..MutationPathConfig::all_enabled()
    },
    ..Default::default()
};
let db = Uni::in_memory().config(config).build().await?;
```

### Implementation Details

Located in: `crates/uni-query/src/query/df_graph/mutation_common.rs`

- `MutationExec` implements DataFusion's `ExecutionPlan` trait with `MutationKind` dispatch.
- `MutationContext` holds shared resources (executor, writer, property manager, params, query context).
- Eager barrier: input batches are fully consumed before mutation dispatch (clause-scoped writer lock).
- MERGE manages its own writer lock internally (acquires/releases per-row for read subplans).
- Routing logic in `read.rs`: DDL/Admin → LOAD CSV fallback → config gate → DataFusion path.

---

## Legacy Executor Path (Row-by-Row)

### When Used
1. **Always** for DDL/Admin operations (CREATE INDEX, DROP, ALTER, SHOW, etc.)
2. **Fallback** when DataFusion translation fails (quantifiers, subqueries)
3. **Mutations with LOAD CSV** (not yet supported in DF engine)
4. **Mutations with config toggle** (`MutationPathConfig` per-clause disable)

### Advantages
- Full Cypher feature support
- Graph-specific operations (traversals, path finding)
- Variable binding and scoping
- Flexible expression evaluation

### Disadvantages
- Row-by-row processing (not vectorized)
- No SIMD optimizations
- Higher memory usage per row

### Implementation
Located in: `crates/uni-query/src/query/executor/read.rs`

Key functions:
- `execute_subplan()` - Main entry point
- `evaluate_expr()` - Recursive expression evaluator
- `execute_match()` - Pattern matching
- `execute_create()` - Node/edge creation
- `execute_aggregate()` - Aggregation logic

---

## Feature Matrix

| Feature | DataFusion | Legacy | Notes |
|---------|-----------|--------|-------|
| **Core Operators** |
| Arithmetic (+, -, *, /, %, ^) | Yes | Yes | Fully vectorized |
| Comparison (=, <>, <, >, <=, >=) | Yes | Yes | Pushdown to storage |
| Boolean (AND, OR, XOR, NOT) | Yes | Yes | |
| Bitwise (&, \|, ^^, ~, <<, >>) | Yes | Yes | |
| **String Operations** |
| CONTAINS, STARTS WITH, ENDS WITH | Yes | Yes | |
| Regex (=~) | Yes | Yes | Uses DataFusion regexp_match |
| String functions (UPPER, LOWER, etc.) | Yes | Yes | |
| **Array Operations** |
| Array indexing `list[0]` | Yes | Yes | DataFusion array_element |
| Array slicing `list[1..3]` | Yes | Yes | DataFusion array_slice |
| Array functions (size, head, tail) | Yes | Yes | |
| **List Operations** |
| Quantifiers (ALL/ANY/SINGLE/NONE) | No | Yes | Blocked by lambda support |
| List comprehensions | No | No | Not implemented yet |
| UNWIND | Yes | Yes | DataFusion unnest |
| **Aggregations** |
| COUNT, SUM, AVG, MIN, MAX | Yes | Yes | Vectorized in DataFusion |
| collect() | Yes | Yes | DataFusion array_agg |
| **Subqueries** |
| EXISTS { ... } | No | Yes | Correlated subquery |
| COUNT { ... } | No | Yes | |
| Scalar subqueries | No | Yes | |
| **Graph Operations** |
| Pattern matching | Partial | Yes | Partial DF support |
| Variable-length paths | No | Yes | Graph-specific |
| Path expressions | No | Yes | |
| **Mutations** |
| CREATE (nodes/edges) | Yes | Yes | DF default, MutationCreateExec |
| SET (properties/labels) | Yes | Yes | DF default, MutationSetExec |
| REMOVE (properties/labels) | Yes | Yes | DF default, MutationRemoveExec |
| DELETE / DETACH DELETE | Yes | Yes | DF default, MutationDeleteExec |
| MERGE | Yes | Yes | DF default, MutationMergeExec |
| FOREACH | Yes | Yes | DF default, ForeachExec |
| **DDL/Admin** |
| CREATE INDEX, DROP, ALTER | N/A | Yes | Schema operations |
| SHOW, VACUUM | N/A | Yes | |

---

## Performance Considerations

### When DataFusion Helps Most
1. **Large scans with filters**
   - Predicate pushdown to Lance storage
   - Vectorized filtering
   - Example: `MATCH (n:Person) WHERE n.age > 30 RETURN n`

2. **Aggregations over many rows**
   - Vectorized aggregates
   - Parallel processing
   - Example: `MATCH (n:Person) RETURN COUNT(n), AVG(n.age)`

3. **Complex arithmetic/string operations**
   - SIMD optimizations
   - Example: `RETURN n.price * 1.1 WHERE UPPER(n.name) CONTAINS 'ACME'`

### When Legacy is Acceptable
1. **Queries with quantifiers**
   - Typically small lists (< 1000 elements)
   - Row-by-row overhead acceptable
   - Example: `WHERE ALL(x IN n.tags WHERE x > 0)`

2. **Subqueries with small result sets**
   - EXISTS checks on small graphs
   - Example: `WHERE EXISTS { MATCH (n)-[:KNOWS]->() }`

3. **DDL operations**
   - Not performance-critical
   - Executed infrequently

### Optimization Tips
1. **Avoid quantifiers on large lists in hot paths**
   - Use UNWIND + aggregation instead
   - Example: `UNWIND list AS x WHERE x > 0 WITH count(x) AS c`

2. **Push filters early**
   - DataFusion can push to storage
   - Example: `WHERE n.age > 30` (good) vs. `WITH n WHERE n.age > 30` (worse)

3. **Use DataFusion-friendly expressions when possible**
   - Prefer built-in functions over UDFs
   - Avoid mixing quantifiers with aggregations

---

## Future Improvements

### Short-Term (DataFusion Updates)
1. **Lambda function support** ([Issue #14205](https://github.com/apache/datafusion/issues/14205))
   - Would enable vectorized quantifiers

2. **Better correlated subquery support**
   - Would enable EXISTS translation
   - Already in progress in DataFusion

### Long-Term (Uni-Specific)
1. **Custom DataFusion UDFs for graph operations**
   - Path traversal UDFs
   - Graph-specific aggregates

2. **Hybrid execution within single query**
   - Use DataFusion for scans/filters
   - Use legacy for graph-specific parts
   - Currently all-or-nothing per query

3. **Cost-based execution path selection**
   - Estimate rows/complexity
   - Choose DataFusion vs legacy based on query characteristics

4. **LOAD CSV support in DataFusion engine**
   - Last remaining mutation fallback trigger
   - Would eliminate all mutation-specific fallback paths

---

## Debugging

### Determining Execution Path

**Enable debug logging:**
```bash
RUST_LOG=uni_query::query::executor=debug cargo run
```

**Look for log messages:**
```
DEBUG DataFusion execution failed (falling back to legacy): Quantifier expressions not supported
```

**In code:**
```rust
// See: crates/uni-query/src/query/executor/read.rs:326
log::debug!("DataFusion execution failed (falling back to legacy): {}", e);
```

### Common Fallback Scenarios
1. **Quantifier in WHERE clause**
   - Log: `"Quantifier expressions not supported"`
   - Path: Legacy executor

2. **EXISTS subquery**
   - Log: `"EXISTS subqueries not yet supported"`
   - Path: Legacy executor

3. **DDL query**
   - Path: Legacy executor (no DataFusion attempt)

4. **Mutation with LOAD CSV**
   - Path: Legacy executor (LOAD CSV not supported in DF engine)

---

## References

- [DataFusion Lambda Functions Issue](https://github.com/apache/datafusion/issues/14205)
- [DataFusion Aggregate Functions](https://datafusion.apache.org/user-guide/sql/aggregate_functions.html)
- [DataFusion Array Functions](https://datafusion.apache.org/user-guide/sql/special_functions.html)
- `crates/uni-query/src/query/executor/read.rs` - Main executor
- `crates/uni-query/src/query/df_expr.rs` - DataFusion translation
- `crates/uni-query/src/query/df_graph/mutation_common.rs` - Mutation operators
- `docs/DATAFUSION_MUTATION_IMPLEMENTATION_PLAN.md` - Full mutation migration plan
- `CYPHER_IMPLEMENTATION_STATUS.md` - Feature coverage

---

**Last Updated**: 2026-02-20
**Maintained By**: Development Team
