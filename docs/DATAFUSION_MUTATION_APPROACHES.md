# DataFusion Mutation Path: Requirements and Design Options

**Date:** 2026-02-18  
**Status:** Design analysis

## 1. Goal

Unify mutation execution so writes can flow through the DataFusion-oriented pipeline, while preserving openCypher mutation semantics and mutation TCK behavior.

This document analyzes two approaches:

1. **Approach A:** DataFusion for row production + a `Writer`-backed mutation sink adapter.
2. **Approach B:** Native DataFusion mutation operators/sinks that own mutation semantics.

---

## 2. Current Baseline (Code Reality)

### 2.1 Routing and planning

- DataFusion planner rejects write plans today:
  - `crates/uni-query/src/query/df_planner.rs:690-698`
  - `crates/uni-query/src/query/df_planner.rs:807-809`
- Executor routes any write-containing logical plan to fallback executor:
  - `crates/uni-query/src/query/executor/read.rs:798-805`
  - `crates/uni-query/src/query/executor/read.rs:993-1020`

### 2.2 Where mutations are implemented now

- Mutation logic is in fallback execution + writer helpers:
  - `crates/uni-query/src/query/executor/read.rs:3599-3990`
  - `crates/uni-query/src/query/executor/write.rs:906-1633`
- Durability and storage write path is `Writer` (L0/WAL/flush):
  - `crates/uni-store/src/runtime/writer.rs`

### 2.3 Important current gaps (relevant to both approaches)

- `SET n = expr` and `SET n += expr` map forms are still unsupported in mutation execution:
  - `crates/uni-query/src/query/executor/write.rs:1358-1368`
- MERGE exists but is not full parity; MERGE-in-FOREACH is explicitly simplified:
  - `crates/uni-query/src/query/executor/read.rs:4184-4187`
- Recent mutation TCK report still shows major MERGE/SET failures:
  - `compliance_reports/schema/last_run_report.md`

---

## 3. Requirements (Spec + TCK)

## 3.1 openCypher mutation requirements that must hold

From `crates/uni-query/docs/openCypher9.pdf` (sections: `CREATE`, `DELETE`, `SET`, `REMOVE`, `MERGE`, and "State visibility and behaviour between clauses"):

1. **Clause state visibility / eagerness**
   - Each clause operates on the full input result set before handing off.
   - Each clause sees changes from prior clauses, not later ones.
   - This is the key semantic for mixed read/write queries.
2. **CREATE**
   - Create nodes/relationships and bind new entities into row stream for later clauses.
3. **DELETE / DETACH DELETE**
   - Plain `DELETE` must fail on node-with-relationships.
   - `DETACH DELETE` removes node plus incident relationships.
4. **SET**
   - Property update, label update, full-map replace (`=`), map append/merge (`+=`).
   - Null map entries and map semantics must match spec/TCK behavior.
5. **REMOVE**
   - Remove properties and labels.
   - Property removal semantics align with "properties are absent, not stored as null".
6. **MERGE**
   - Match-or-create semantics.
   - Whole pattern all-or-nothing behavior.
   - `ON CREATE` / `ON MATCH`.
   - Undirected relationship MERGE may create arbitrary direction.
   - MERGE map-parameter caveat (must spell properties explicitly).

## 3.2 Mutation TCK requirements that matter most

Mutation feature files:

- `crates/uni-tck/tck/features/clauses/create/Create1-6.feature`
- `crates/uni-tck/tck/features/clauses/delete/Delete1-6.feature`
- `crates/uni-tck/tck/features/clauses/set/Set1-6.feature`
- `crates/uni-tck/tck/features/clauses/remove/Remove1-3.feature`
- `crates/uni-tck/tck/features/clauses/merge/Merge1-9.feature`

Critical TCK themes:

1. **Interop and persistence across clauses**
   - `Create6`, `Delete6`, `Set6`, `Remove3`: side effects must persist independent of `LIMIT/SKIP/WHERE/RETURN/WITH`.
2. **Side-effect accounting parity**
   - `+nodes`, `-nodes`, `+relationships`, `-relationships`, `+/-properties`, `+/-labels`.
   - Checked in: `crates/uni-tck/src/steps/and.rs`.
3. **SET map forms**
   - `Set4` (`SET n = map`) and `Set5` (`SET n += map`) semantics are mandatory.
4. **MERGE semantics depth**
   - `Merge1-9` cover binding, path merge, ON CREATE/ON MATCH, undirected relationship merge, UNWIND + MERGE pipelines, errors.
5. **DELETE over complex values**
   - `Delete5` includes deleting from list/map/nested structures and proper failures.

## 3.3 Clause-by-clause requirement matrix (spec + TCK + current gap)

| Area | Spec expectation | TCK coverage | Current status/gap |
|---|---|---|---|
| CREATE nodes/labels/properties | Create entities and bind for downstream clauses | `Create1`, `Create6` | Implemented in fallback; not on DF path |
| CREATE relationships/directions/types | Relationship creation rules and validation | `Create2`, `Create5` | Implemented in fallback; not on DF path |
| CREATE interop with MATCH/WITH/MERGE | Correct state visibility across clause boundaries | `Create3`, `Create6` | Partial; persistence scenarios still failing in report |
| DELETE node/relationship | Delete semantics and errors for invalid targets | `Delete1`, `Delete2`, `Delete5` | Implemented in fallback; complex nested delete scenarios still weak |
| DETACH DELETE | Remove node + incident relationships | `Delete1`, `Delete3`, `Delete6` | Implemented in fallback; parity gaps remain |
| SET property and label | Property/label updates, null behavior, idempotent label set | `Set1`, `Set2`, `Set3`, `Set6` | Mostly implemented in fallback; persistence gaps remain |
| SET map replace | `SET n = map` replace semantics | `Set4` | Not implemented in executor (`write.rs` unsupported path) |
| SET map append | `SET n += map` merge semantics | `Set5` | Not implemented in executor (`write.rs` unsupported path) |
| REMOVE property | Remove property instead of storing null | `Remove1`, `Remove3` | Implemented in fallback; some persistence gaps |
| REMOVE label | Idempotent label removal | `Remove2`, `Remove3` | Implemented in fallback; some persistence gaps |
| MERGE node | Match-or-create with correct binding behavior | `Merge1`, `Merge9` | Partial implementation; significant failures remain |
| MERGE relationship | Full-pattern all-or-nothing semantics | `Merge5` | Partial; behavior gaps remain |
| MERGE `ON CREATE`/`ON MATCH` | Conditional mutation on match/create outcome | `Merge2`, `Merge3`, `Merge4`, `Merge6`, `Merge7`, `Merge8` | Partial and incomplete parity |
| MERGE in pipelines | Works with `WITH`, `UNWIND`, prior updates | `Merge9`, `Create3` scenario links | High risk area |
| Mutation side-effect accounting | Exact counts of nodes/rels/props/labels | `Create6`, `Delete6`, `Set6`, `Remove3` | Not fully stable |

---

## 3.4 Non-negotiable semantic invariants

1. **Write clause boundaries are eager boundaries.**
2. **Later clauses must see prior writes in the same statement.**
3. **Earlier clauses must never see future writes.**
4. **Mutation side effects are independent of `LIMIT/SKIP/WHERE/RETURN` row trimming after write clauses.**
5. **MERGE must preserve whole-pattern semantics (no partial pattern merge).**
6. **`SET n = map` and `SET n += map` must match openCypher/TCK map semantics exactly.**

---

## 4. Approach A: DataFusion + Writer Sink Adapter

## 4.1 Core idea

Keep mutation semantics in the existing Writer-backed logic, but let DataFusion produce candidate rows for mutation clauses. A mutation sink consumes those rows and applies writes via `Writer`.

In short: **DataFusion for row production, Writer for mutation semantics + durability**.

## 4.2 How hybrid read/write queries execute

For a query like:

```cypher
MATCH (a:A) WHERE a.x > 10
WITH a
SET a.y = a.x + 1
WITH a
MATCH (a)-[:R]->(b)
RETURN a, b
```

Execution shape:

1. DataFusion executes `MATCH/WHERE/WITH` read part and yields rows.
2. At `SET`, mutation sink eagerly consumes all incoming rows and applies updates via `Writer`.
3. Updated rows continue; next clauses read with same statement state visibility.
4. Return/projection executes after updates.

This matches openCypher clause-eagerness requirements if write boundaries are explicit eager barriers.

## 4.3 What must be built

1. Planner support to allow write logical nodes in DF-oriented execution path.
2. Mutation sink adapter APIs per clause (`CREATE`, `DELETE`, `SET`, `REMOVE`, later `MERGE`).
3. Row<->entity conversion consistency (Node/Edge/value shapes) at sink boundary.
4. Statement-level side effect accounting integrated with sink execution.
5. Eager barrier at every write clause boundary.
6. Complete map-based SET semantics (`Set4/Set5`) in shared mutation implementation.

## 4.4 Pros

1. **Lowest semantic risk**: reuses current `Writer` behavior and locking/durability path.
2. **Fastest path to TCK progress** for CREATE/DELETE/REMOVE/basic SET.
3. **Incremental adoption**: clause-by-clause enablement possible.
4. **Single source of truth for writes** remains in one mutation engine.

## 4.5 Cons

1. Boundary conversion overhead (batch rows to rowwise mutation application).
2. Write segments remain mostly row-oriented and less vectorized.
3. Need careful plumbing to keep row mutations visible for subsequent projections.

## 4.6 Caveats / failure modes

1. **Do not stream lazily across write boundaries**; enforce eager consumption per clause.
2. **Lock scope**: hold writer lock at clause scope, not row scope, to avoid contention and semantic drift.
3. **Read-your-write in same statement** must include L0 context for all subsequent reads.
4. **Map SET parity** must be implemented before claiming write unification.
5. **MERGE remains high-risk**; keep fallback MERGE path until full parity.
6. **FOREACH semantics** need strict ordered side effects; no parallel mutation application.

## 4.7 Detailed implementation blueprint

1. **Planner change**
   - Stop rejecting write logical nodes for DF-oriented path.
   - Insert explicit mutation boundaries in physical planning.
2. **Mutation sink contract**
   - Input: row stream (or small batches) with bound entities/aliases.
   - Output: updated row stream and side-effect counters.
   - Backend: single acquired `Writer` per clause execution.
3. **Code reuse strategy**
   - Reuse/factor current mutation entrypoints in:
     - `crates/uni-query/src/query/executor/write.rs`
     - `crates/uni-query/src/query/executor/read.rs` write arms
   - Keep one semantic implementation for fallback and sink path.
4. **Correctness boundaries**
   - Before sink: fully evaluate clause input rows.
   - In sink: apply writes and patch bound row values (for same-query RETURN/WITH usage).
   - After sink: downstream clauses read updated state via same query context.
5. **Rollout gates**
   - Gate by clause family and run targeted mutation TCK slices first.

---

## 5. Approach B: Native DataFusion Mutation Operators/Sinks

## 5.1 Core idea

Implement mutation semantics as first-class DataFusion physical operators/sinks. `Writer` is used as a low-level persistence primitive only.

In short: **DataFusion owns mutation semantics**.

## 5.2 How hybrid read/write queries execute

The full logical pipeline (read + write + post-write read) is planned into DF execution nodes. Mutation nodes consume batches, perform writes, and output updated rows for downstream operators.

## 5.3 What must be built

1. Full planning support in `df_planner` for write nodes.
2. Physical operators/sinks for each mutation clause.
3. Full semantic implementation for:
   - `SET` map replace/append semantics.
   - `DELETE/DETACH DELETE` correctness.
   - `MERGE` all-or-nothing pattern semantics + `ON CREATE`/`ON MATCH`.
4. Deterministic ordered mutation execution model over DataFusion runtime.
5. Statement-local state and side-effect counters integrated with DF execution context.

## 5.4 Pros

1. Cleaner long-term single-engine architecture.
2. Better optimization opportunities for mixed query pipelines over time.
3. Potentially fewer framework boundaries once fully complete.

## 5.5 Cons

1. **Highest implementation effort** and highest regression risk.
2. Requires re-encoding complex openCypher mutation semantics in new operators.
3. MERGE correctness burden is significant.
4. Debugging complexity rises (semantic bugs across planner + physical engine).

## 5.6 Caveats / failure modes

1. DataFusion execution is pull-based/parallel-capable; mutation clauses must still behave logically eager and ordered.
2. Parallel mutation without strict ordering may violate side effects or MERGE behavior.
3. Row object shape parity (node/edge/path map forms) must match downstream expectations.
4. Error and rollback behavior must preserve current writer semantics.
5. Side-effect accounting has to remain exact for TCK checks.

## 5.7 Detailed implementation blueprint

1. **Logical planning**
   - Extend `df_planner` to plan mutation logical nodes instead of erroring.
2. **Physical operator set**
   - Create mutation physical operators for `CREATE`, `DELETE`, `SET`, `REMOVE`, `MERGE`.
   - Each operator must support eager execution boundary.
3. **Execution model controls**
   - Enforce single-writer critical section for mutation operators.
   - Prevent out-of-order/parallel write effects for a single statement.
4. **Semantic migration**
   - Port all edge-case semantics from fallback mutation code.
   - Keep parity tests at each clause migration step.
5. **Risk containment**
   - Feature flag per clause.
   - Automatic fallback to proven mutation path for not-yet-parity clauses.

---

## 6. Can `Writer` Be the Sink?

Yes. In practice:

1. In **Approach A**, `Writer` is explicitly the sink engine.
2. In **Approach B**, `Writer` can still be the persistence backend, but mutation semantics live in DF operators.

So the real difference is not whether `Writer` is used; it is **where mutation semantics live**.

---

## 7. Side-by-Side Comparison

| Dimension | Approach A (DF + Writer sink) | Approach B (Native DF mutation) |
|---|---|---|
| Time to first usable rollout | Faster | Slower |
| Semantic regression risk | Lower | Higher |
| Reuse existing mutation code | High | Low/Medium |
| Long-term architectural purity | Medium | High |
| MERGE feasibility near-term | Medium (defer/keep fallback) | Medium/Low (hard upfront) |
| Best first target | CREATE/DELETE/REMOVE/basic SET | None until core framework is ready |

---

## 8. Recommended Delivery Sequence

1. Implement **Approach A first** with strict eager write boundaries.
2. Complete missing SET map semantics (`SET n =`, `SET n +=`) in shared mutation engine.
3. Enable mutation clauses incrementally:
   - `CREATE` -> `DELETE/DETACH DELETE` -> `REMOVE` -> `SET`.
4. Keep MERGE on fallback path until parity milestones are met.
5. Reassess Approach B only after mutation TCK parity is stable.

---

## 9. Acceptance Checklist (Both Approaches)

1. All mutation clauses obey openCypher state visibility and eagerness semantics.
2. `Create6/Delete6/Set6/Remove3` persistence scenarios pass.
3. `Set4/Set5` map semantics pass.
4. `Delete5` complex list/map delete scenarios pass.
5. MERGE parity target is explicit (either fully supported or clearly gated/fallback).
6. Side-effect counts match TCK (`+/-nodes`, `+/-relationships`, `+/-properties`, `+/-labels`).
7. Mixed read/write pipelines (`WITH`, `UNWIND`, aggregates, `LIMIT/SKIP`) preserve side effects and row outputs.
