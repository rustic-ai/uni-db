# Mutation Support Specification (DELETE, SET, MERGE)

This document outlines the design and implementation plan for adding `DELETE`, `SET`, and `MERGE` support to Uni's Cypher query engine.

## 1. Overview

Currently, Uni supports read-only queries via the vectorized execution engine. Write operations are limited to the `Writer` API and are not exposed via Cypher. This specification defines how to bridge this gap, enabling full CRUD capabilities.

## 2. Grammar & Parsing

We need to extend the `CypherParser` (using `open_cypher_grammar` or custom logic) to support the following clauses.

### 2.1 DELETE / DETACH DELETE
```cypher
MATCH (n:Person) WHERE n.name = 'DeleteMe'
DELETE n
```
- `DELETE`: Marks nodes/relationships for deletion. Fails if nodes still have relationships.
- `DETACH DELETE`: Removes relationships attached to the node before deleting the node.

### 2.2 SET / REMOVE
```cypher
MATCH (n:Person {id: 1})
SET n.email = 'new@example.com', n.updated_at = timestamp()
REMOVE n.old_prop
```
- `SET`: Updates or adds properties.
- `REMOVE`: Removes properties (equivalent to `SET n.prop = null` in some systems, but distinct in Cypher).

### 2.3 MERGE
```cypher
MERGE (n:Person {email: 'user@example.com'})
ON CREATE SET n.created = timestamp()
ON MATCH SET n.last_login = timestamp()
RETURN n
```
- `MERGE`: "Match or Create". Requires existence check followed by conditional insert.

## 3. Logical Plan

New logical operators will be added to `uni_db::query::logical_plan`.

```rust
pub enum LogicalOperator {
    // ... existing ...
    SetProperty {
        input: Box<LogicalPlan>,
        items: Vec<SetItem>, // (Var, Key, Expr)
    },
    Delete {
        input: Box<LogicalPlan>,
        node_vars: Vec<Var>,
        edge_vars: Vec<Var>,
        detach: bool,
    },
    // MERGE is a composite operation usually expanded during planning, 
    // but might need a dedicated operator for atomicity.
}
```

## 4. Physical Plan (Vectorized Execution)

The `VectorizedExecution` engine will be extended with "Side-Effect Operators". Unlike `Project` or `Filter` which transform data, these operators consume batches and apply changes to the `Writer`.

### 4.1 VectorizedSet
**Input:** `VectorizedBatch`
**Action:**
1. Evaluate expressions for the property values.
2. Group updates by VID/EID.
3. Call `Writer::insert_vertex(vid, props)` or `Writer::insert_edge(...)`.
   - *Note:* `Writer::insert_vertex` in `L0Buffer` already implements **merge** semantics (patching properties), which matches `SET` behavior.
   - For `REMOVE`, we might need a specific `Writer::remove_property` or pass a `Null` value if we decide `Null` means remove (standard Cypher treats Null property as non-existent).

### 4.2 VectorizedDelete
**Input:** `VectorizedBatch`
**Action:**
1. Iterate over VIDs (for nodes) or EIDs (for relationships).
2. Call `Writer::delete_vertex(vid)` or `Writer::delete_edge(eid)`.
3. If `detach` is true, we must first look up all incident edges (using `AdjacencyCache` or `Storage`) and delete them. *Optimization:* This can be expensive; ideally, we push this to the storage layer or expand it in the logical plan.

## 5. Execution Model & Consistency

### 5.1 Single-Writer
Uni is single-writer. The `Executor` currently holds a read view (`StorageManager`). For mutations:
1. The `Executor` must have access to the `Writer` (currently wrapped in `Arc<Mutex<Writer>>` or similar in the main app).
2. Mutations write to `L0Buffer`.

### 5.2 Visibility (Read-Your-Writes)
- **Standard Cypher:** Changes made in a query are visible to subsequent clauses in the same query.
- **Uni Implementation:** 
  - `L0Buffer` writes are immediate in memory.
  - However, the `VectorizedEngine` processes data in pipeline streams.
  - If a `MATCH` downstream needs to see a `SET` upstream, it must re-read from `L0`.
  - **MVP constraint:** We will aim for "Statement-level Atomicity". Mutations are applied as the batch flows through. Downstream operators (like `RETURN`) usually just return the passed variables. If `RETURN n` fetches properties *again*, it might see the update. If it uses the *already scanned* batch, it won't.
  - **Proposed Semantics for Phase 1:** Variables bound *before* the modification retain their old state in the batch. If re-matched, they see new state. This is consistent with many vectorized engines.

## 6. Implementation Roadmap

### Phase 1.1: Parser & Logical Plan
- Update `CypherParser` to handle `SET`, `DELETE`.
- Add `LogicalOperator::Set`, `LogicalOperator::Delete`.
- Update `QueryPlanner` to map AST to Logical Plan.

### Phase 1.2: Physical Operators
- Implement `VectorizedSet` operator.
- Implement `VectorizedDelete` operator.
- Integrate `Writer` into `ExecutionContext`.

### Phase 1.3: End-to-End Test
- Test: `CREATE (n {p:1}) SET n.p=2 RETURN n.p` -> Expect 2.
- Test: `CREATE (n) DELETE n RETURN count(n)` -> Expect 0 (or empty).
