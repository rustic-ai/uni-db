# Query Execution Path

**Date**: 2026-02-21
**Purpose**: Document the DataFusion-based query execution architecture

---

## Overview

Uni uses **DataFusion as its sole query execution engine** for all read and mutation queries. DDL/Admin operations (CREATE INDEX, DROP, ALTER, SHOW) are handled by dedicated handlers outside the DataFusion path.

```
Query → Parser → Planner → Executor.execute()
                              ├─→ DataFusion (reads, mutations, procedures)
                              └─→ DDL/Admin handlers (schema operations)
```

## DataFusion Execution

### Capabilities
- Vectorized execution (SIMD) via Apache Arrow columnar processing
- Query optimization (predicate pushdown, projection pushdown)
- Parallel execution and memory-efficient streaming
- All mutation types (CREATE, SET, REMOVE, DELETE, MERGE, FOREACH) via `MutationExec` operators

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

### Expressions Not Yet Translatable to DataFusion

Located in: `df_expr.rs` — expressions that return `Err(anyhow!(...)`

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

All mutation types route through DataFusion via `MutationExec` operators.

### Mutation Operators

| Mutation Type | DataFusion Operator |
|---------------|-------------------|
| CREATE | `MutationCreateExec` |
| SET | `MutationSetExec` |
| REMOVE | `MutationRemoveExec` |
| DELETE | `MutationDeleteExec` |
| MERGE | `MutationMergeExec` |
| FOREACH | `ForeachExec` |

### Implementation Details

Located in: `crates/uni-query/src/query/df_graph/mutation_common.rs`

- `MutationExec` implements DataFusion's `ExecutionPlan` trait with `MutationKind` dispatch.
- `MutationContext` holds shared resources (executor, writer, property manager, params, query context).
- Eager barrier: input batches are fully consumed before mutation dispatch (clause-scoped writer lock).
- MERGE manages its own writer lock internally (acquires/releases per-row for read subplans).

---

## Feature Matrix

| Feature | DataFusion | Notes |
|---------|-----------|-------|
| **Core Operators** |
| Arithmetic (+, -, *, /, %, ^) | Yes | Fully vectorized |
| Comparison (=, <>, <, >, <=, >=) | Yes | Pushdown to storage |
| Boolean (AND, OR, XOR, NOT) | Yes | |
| Bitwise (&, \|, ^^, ~, <<, >>) | Yes | |
| **String Operations** |
| CONTAINS, STARTS WITH, ENDS WITH | Yes | |
| Regex (=~) | Yes | Uses DataFusion regexp_match |
| String functions (UPPER, LOWER, etc.) | Yes | |
| **Array Operations** |
| Array indexing `list[0]` | Yes | DataFusion array_element |
| Array slicing `list[1..3]` | Yes | DataFusion array_slice |
| Array functions (size, head, tail) | Yes | |
| **List Operations** |
| Quantifiers (ALL/ANY/SINGLE/NONE) | No | Blocked by lambda support |
| List comprehensions | No | Not implemented yet |
| UNWIND | Yes | DataFusion unnest |
| **Aggregations** |
| COUNT, SUM, AVG, MIN, MAX | Yes | Vectorized |
| collect() | Yes | DataFusion array_agg |
| **Subqueries** |
| EXISTS { ... } | No | Correlated subquery |
| COUNT { ... } | No | |
| Scalar subqueries | No | |
| **Graph Operations** |
| Pattern matching | Partial | |
| Variable-length paths | No | Graph-specific |
| Path expressions | No | |
| **Mutations** |
| CREATE (nodes/edges) | Yes | MutationCreateExec |
| SET (properties/labels) | Yes | MutationSetExec |
| REMOVE (properties/labels) | Yes | MutationRemoveExec |
| DELETE / DETACH DELETE | Yes | MutationDeleteExec |
| MERGE | Yes | MutationMergeExec |
| FOREACH | Yes | ForeachExec |
| **DDL/Admin** |
| CREATE INDEX, DROP, ALTER | N/A | Schema operations |
| SHOW, VACUUM | N/A | |

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

### Optimization Tips
1. **Push filters early**
   - DataFusion can push to storage
   - Example: `WHERE n.age > 30` (good) vs. `WITH n WHERE n.age > 30` (worse)

2. **Use DataFusion-friendly expressions when possible**
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
   - Use specialized operators for graph-specific parts

3. **Cost-based execution path selection**
   - Estimate rows/complexity
   - Choose optimal execution strategy based on query characteristics

---

## Debugging

### Enable debug logging:
```bash
RUST_LOG=uni_query::query::executor=debug cargo run
```

---

## References

- [DataFusion Lambda Functions Issue](https://github.com/apache/datafusion/issues/14205)
- [DataFusion Aggregate Functions](https://datafusion.apache.org/user-guide/sql/aggregate_functions.html)
- [DataFusion Array Functions](https://datafusion.apache.org/user-guide/sql/special_functions.html)
- `crates/uni-query/src/query/executor/read.rs` - Main executor
- `crates/uni-query/src/query/df_expr.rs` - DataFusion translation
- `crates/uni-query/src/query/df_graph/mutation_common.rs` - Mutation operators
- `CYPHER_IMPLEMENTATION_STATUS.md` - Feature coverage

---

**Last Updated**: 2026-02-21
