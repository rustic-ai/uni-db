# DataFusion Mutation Implementation Plan

**Date:** 2026-02-18
**Last updated:** 2026-02-21
**Status:** **COMPLETE** — All milestones (M0–M5) delivered. Legacy executor fallback has been removed;
all queries now route exclusively through DataFusion. The `MutationPathConfig` rollback toggle and
`LOAD CSV` clause have been removed. Historical references to these below are kept for context.
**Depends on:** `docs/DATAFUSION_MUTATION_APPROACHES.md`
**Primary strategy:** Implement Approach A first, keep Approach B as staged follow-up.

## 0. Current Status

| Milestone | Status | Summary |
|-----------|--------|---------|
| **M0** | **COMPLETE** | Baseline TCK archived in `compliance_reports/`; routing infrastructure established (since removed — all queries now use DF exclusively) |
| **M1** | **COMPLETE** | All SET semantic forms implemented; Gate A satisfied |
| **M2** | **COMPLETE** | `MutationExec` framework built; `MutationContext` wired; eager barrier; all 4 operators dispatched via planner |
| **M3** | **COMPLETE** | All 4 operators wired in DF planner; simple terminal mutations route to DF path; complex mutations (RETURN/WITH, nested) fall back. M3 parity fixes round 1: label SET schemaless, multi-property REMOVE batching, DELETE/CREATE error validation (+23 TCK scenarios). Round 2: schemaless edge visibility in batch detach-delete, two-pass non-detach DELETE, BindPath null-safety, property type validation, edge uniqueness in GraphTraverseMainExec (+12 TCK scenarios) |
| **M4** | **COMPLETE** | All 76/76 MERGE scenarios passing. Merge1-9 at 100%. 8-phase implementation + DF routing: terminal MERGE now flows through DataFusion `MutationMergeExec` operator (same framework as CREATE/SET/REMOVE/DELETE). Complex MERGE (with RETURN/WITH, nested mutations) falls back. |
| **M5** | **COMPLETE** | Hardening, performance, default rollout |

### Overall TCK (schemaless mode, 2026-02-19)

| Metric | Value |
|--------|-------|
| Total scenarios | 3897 |
| Passed | 3814 |
| Failed | 82 |
| Pass rate | **97.9%** |

### Mutation TCK Breakdown

| Feature | Passed | Total | Rate | Status |
|---------|--------|-------|------|--------|
| **Create1** | **20** | **20** | **100%** | **Complete** (was 19/20) |
| Create2 | 24 | 24 | **100%** | Complete |
| **Create3** | **13** | **13** | **100%** | **Complete** (was 11/13) |
| Create4 | 2 | 2 | **100%** | Complete |
| **Create5** | **5** | **5** | **100%** | **Complete** (was 4/5) |
| **Create6** | **14** | **14** | **100%** | **Complete** (was 12/14) |
| **Delete1** | **8** | **8** | **100%** | **Complete** (was 6/8) |
| **Delete2** | **5** | **5** | **100%** | **Complete** (was 4/5) |
| **Delete3** | **2** | **2** | **100%** | **Complete** (was 1/2) |
| Delete4 | 3 | 3 | **100%** | Complete |
| **Delete5** | **9** | **9** | **100%** | **Complete** (was 2/9) |
| **Delete6** | **14** | **14** | **100%** | **Complete** (was 12/14) |
| **Set1** | **11** | **11** | **100%** | **Complete** (was 10/11) |
| Set2 | 3 | 3 | **100%** | Complete |
| **Set3** | **8** | **8** | **100%** | **Complete** (was 1/8) |
| **Set4** | **5** | **5** | **100%** | **Gate A** |
| **Set5** | **5** | **5** | **100%** | **Gate A** |
| **Set6** | **21** | **21** | **100%** | **Complete** (was 14/21) |
| **Remove1** | **7** | **7** | **100%** | **Complete** (was 4/7) |
| Remove2 | 5 | 5 | **100%** | Complete |
| **Remove3** | **21** | **21** | **100%** | **Complete** (was 18/21) |
| **Merge1** | **17** | **17** | **100%** | **Complete** (was 8/17) |
| **Merge2** | **6** | **6** | **100%** | **Complete** (was 4/6) |
| **Merge3** | **5** | **5** | **100%** | **Complete** (was 3/5) |
| **Merge4** | **2** | **2** | **100%** | **Complete** (was 0/2) |
| **Merge5** | **29** | **29** | **100%** | **Complete** (was 19/29) |
| **Merge6** | **6** | **6** | **100%** | **Complete (ON CREATE SET)** |
| **Merge7** | **5** | **5** | **100%** | **Complete (ON MATCH SET)** |
| **Merge8** | **1** | **1** | **100%** | **Complete (ON CREATE + ON MATCH)** |
| **Merge9** | **4** | **4** | **100%** | **Complete** (was 2/4) |

### Gate Status

| Gate | Status | Details |
|------|--------|---------|
| **Gate A** | **SATISFIED** | Set4: 5/5, Set5: 5/5. Entity-copy and map forms working. |
| Gate B | **SATISFIED** | Create6: **14/14** ✅, Delete6: **14/14** ✅, Remove3: **21/21** ✅, Set6: **21/21** ✅, Delete5: **9/9** ✅ |
| Gate C | Not applicable yet | Requires new path implementation |
| Gate D | **SATISFIED** | No regressions in read-only suites. Full TCK at 97.9% (3814/3897), +20 vs pre-MERGE baseline, +55 vs pre-M3 baseline. |
| Gate E | **SATISFIED** | Gate A done; M3 complete; M4 MERGE complete (76/76 scenarios). |

## 1. Scope and Objectives

This plan turns the mutation design into an execution program with milestones and file-level tasks.

Objectives:

1. Route mutation queries through a DataFusion-compatible pipeline without losing openCypher semantics.
2. Preserve writer durability and single-writer correctness.
3. Reach mutation TCK parity in staged gates.
4. ~~Keep rollback/fallback path available during rollout.~~ (Completed and removed — DF is now the sole path.)

## 2. Definition of Done

Done means all of the following:

1. Mutation clauses (`CREATE`, `DELETE`, `SET`, `REMOVE`) execute via DF + Writer sink path by default.
2. All SET forms pass TCK:
   - `SET n = <map>` (replace all properties with map) — `Set4`
   - `SET n += <map>` (merge map into existing properties) — `Set5`
   - `SET n = <node/relationship>` (copy all properties from entity) — spec p.109, Merge6[6]
   - Null entity target is silently ignored — `Set4[5]`, `Set5[1]`
3. Persistence/interop scenarios pass for `Create6`, `Delete6`, `Set6`, `Remove3`.
4. Side-effect accounting remains exact (`+/-nodes`, `+/-relationships`, `+/-properties`, `+/-labels`).
5. `Delete5` (expression-based and nested delete targets, path deletion) passes.
6. MERGE is parity-complete and enabled on new path:
   - 76/76 TCK scenarios passing. ✅
   - Terminal MERGE routes through `MutationMergeExec` on DF path. ✅
   - Complex MERGE (RETURN/WITH, nested) falls back via `needs_mutation_fallback()`. ✅
7. No regression in non-mutation query suites.

## 3. Milestone Plan (Recommended)

## M0 - Baseline and Safety Rails ✅ COMPLETE

Goal: establish controlled rollout and parity measurement before behavior changes.

**Status: COMPLETE** (2026-02-18)

### M0 Implementation Details

> **Note:** The `MutationPathConfig` routing toggle was used during the M0–M5 rollout to allow
> per-clause fallback to the legacy executor. It has since been removed — all mutations now route
> through DataFusion exclusively.

- Baseline TCK reports archived in `compliance_reports/schema/` and `compliance_reports/schemaless/`.

Tasks:

1. ~~Define `MutationPathConfig` struct and attach it to `QueryContext`.~~ ✅ (since removed)
2. ~~Add per-clause routing check at dispatch points in the executor.~~ ✅ (since removed)
3. Capture baseline TCK slices and store results in `compliance_reports/`. ✅

---

## M1 - Semantic Hardening in Existing Mutation Engine ✅ COMPLETE

Goal: remove ALL known semantic gaps in shared mutation logic before plugging into the sink path.
M1 is a prerequisite for every subsequent milestone. No sink work begins until all M1 exit criteria pass.

**Status: COMPLETE** (2026-02-18)

- Gate A satisfied: Set4 5/5, Set5 5/5
- Merge6 ON CREATE SET: 6/6 (including entity-copy and map-append forms)
- Merge7 ON MATCH SET: 5/5
- Merge8 ON CREATE + ON MATCH: 1/1
- No regressions in Set1-3, Set6
- Full TCK: 3750/3897 (96.2%), +83 scenarios vs pre-M1 baseline

### M1 Implementation Details

**Step 1-5: SET map-replace (Set4) and SET map-append (Set5)**
- Implemented in `crates/uni-query/src/query/executor/write.rs`
- `SET n = <map>` replace semantics: full property replacement with null-removal
- `SET n += <map>` append semantics: merge with null-removal for explicit nulls
- Null entity target silently ignored (Set4[5], Set5[1])
- `SET n.prop = NULL` correctly removes property

**Step 6-7: Entity-copy (Merge6[6]) and multiple SET items**
- `SET r = <entity>` copies all properties from source to target
- Multiple SET items applied in left-to-right order within same write boundary
- Row values updated after mutations for downstream clause visibility

**Step 8: Edge property visibility fix (Merge6[2][3][6][7])**
- Root cause: L0 edge properties invisible through DataFusion query path for schemaless edges
- Fix 1: `crates/uni-query/src/query/df_planner.rs` — added `_all_props` to `edge_properties`
  when wildcard edge access detected (schemaless edges store properties by name in L0, not as
  overflow_json blobs)
- Fix 2: `crates/uni-store/src/runtime/property_manager.rs` — `overlay_l0_edge_batch` now
  includes all L0 properties when `_all_props` is in the requested properties list

### M1 Tasks

1. **`SET n = <map>` replace semantics** (spec p.110, `Set4`) ✅
   - Replace all existing properties with the map contents.
   - Keys in the new map with null values are treated as absent (property is removed).
   - An empty map `{}` removes all properties.
   - Missing keys in the new map cause the corresponding existing property to be removed.

2. **`SET n += <map>` append/merge semantics** (spec p.110, `Set5`) ✅
   - Merge new map into existing properties: new keys are added, overlapping keys are updated.
   - Keys explicitly set to null in the new map remove the corresponding existing property.
   - An empty map `{}` is a no-op (no side effects).
   - Keys not present in the new map are retained unchanged.

3. **`SET n = <node/relationship>` entity-copy semantics** (spec p.109, Merge6[6]) ✅
   - Copy all properties from a source graph element to the target element.
   - This is a full replace: all previous properties on the target are removed, then source properties
     are copied in. Side effects: `-properties` for removed props, `+properties` for added props.
   - Source can be any graph element bound in the current row (node or relationship).
   - This is a distinct code path from `SET n = <map>`; both must share the same replace logic.

4. **Null entity target must be silently ignored** (spec implied, `Set4[5]`, `Set5[1]`) ✅
   - When the mutation target evaluates to `null` (e.g., from a non-matching `OPTIONAL MATCH`),
     the SET operation is skipped with zero side effects.
   - This applies to all mutation clauses: `SET`, `REMOVE`, `DELETE`.
   - This is distinct from null values *inside* a map; it is a null *target entity*.

5. **`SET n.prop = NULL` removes the property** (spec p.109) ✅
   - Setting a property to null is equivalent to `REMOVE n.prop`.
   - Side effect: `-properties: 1` (not `+properties: 1`).
   - Validate this is handled correctly in the shared property-update path.

6. **Multiple SET items in one clause are applied in order** (spec p.112) ✅
   - `SET n.position = 'Developer', n.surname = 'Taylor'` must apply both items atomically
     in left-to-right order within the same write boundary.
   - Add an explicit test to confirm ordering is preserved when the second item depends on
     the first (e.g., `SET n.x = 1, n.y = n.x + 1`).

7. **Normalize row-updating behavior after mutations** so downstream clauses see updated values. ✅
   - After any SET/REMOVE/CREATE, the bound row values for affected entities must reflect the
     new property/label state before the next clause executes.

8. **Edge property visibility through DataFusion query path** (Merge6[2][3][6][7]) ✅
   - L0 edge properties were invisible through the DF scan path for schemaless edges.
   - Fixed by adding `_all_props` to edge_properties in planner and updating L0 overlay filter
     to include all properties when `_all_props` is requested.

Primary files:

- `crates/uni-query/src/query/executor/write.rs`
- `crates/uni-query/src/query/executor/read.rs`
- `crates/uni-query/src/query/planner.rs` (SetItem handling adjustments)
- `crates/uni-query/src/query/df_planner.rs` (edge property visibility)
- `crates/uni-store/src/runtime/property_manager.rs` (L0 edge property overlay)

Test gates:

1. `Set4` passes (all 5 scenarios, including null-entity Scenario [5]). ✅
2. `Set5` passes (all 5 scenarios, including null-entity Scenario [1]). ✅
3. No regressions in `Set1-3`, `Set6`. ✅
4. Entity-copy `SET n = <node>` produces correct property delta and side-effect count (Merge6[6]). ✅
5. Multiple SET items are applied in declared order. ✅
6. `Merge6` all 6 scenarios pass (ON CREATE SET with all forms). ✅
7. `Merge7` all 5 scenarios pass (ON MATCH SET with all forms). ✅
8. `Merge8` passes (combined ON CREATE + ON MATCH). ✅

Exit criteria:

1. All semantic forms above are implemented and tested in the shared mutation engine. ✅
2. Gate A is satisfied: `Set4` + `Set5` fully pass before any sink work begins. ✅

---

## M2 - Mutation Sink Infrastructure (Approach A Core) ✅ COMPLETE

Goal: introduce a Writer-backed mutation sink abstraction and wire it into the execution pipeline.

**Status: COMPLETE** (2026-02-18)

### M2 Implementation Details

**Unified MutationExec Framework:**
- `MutationExec` struct in `crates/uni-query/src/query/df_graph/mutation_common.rs` implements
  `ExecutionPlan` trait with generic dispatch via `MutationKind` enum.
- Thin typed wrappers: `MutationCreateExec`, `MutationSetExec`, `MutationRemoveExec`,
  `MutationDeleteExec` (type aliases + constructor functions in separate modules).
- `MutationContext` holds shared resources: `executor`, `writer`, `prop_manager`, `params`,
  `query_ctx` (for L0 buffer visibility).

**Eager Barrier Pattern:**
- `execute_mutation_stream()` collects all input RecordBatches to completion before dispatching.
- Writer lock acquired once per clause (clause-scoped, not row-scoped).
- `apply_mutations()` dispatches by `MutationKind` to existing hardened write helpers from M1.

**Planner Wiring:**
- `HybridPhysicalPlanner.mutation_ctx: Option<Arc<MutationContext>>` field.
- `with_mutation_context()` sets context; `require_mutation_ctx()` validates presence.
- All 4 operators (CREATE, CreateBatch, SET, REMOVE, DELETE) fully wired in `plan_internal()`.

**Routing Logic:**
- `execute_datafusion()` in `read.rs` builds `MutationContext` when `contains_write_operations()`
  detects write clauses in the plan.
- Dispatch: DDL/Admin → DataFusion path for all reads and mutations.

### M2 Tasks

1. **Define mutation sink interface** ✅
   - Input: row stream (batch or row-wise) with bound entities/aliases.
   - Output: updated row stream with mutated entity values reflected, and side-effect counters.
   - Backend: single acquired `Writer` per clause execution (clause-scoped lock, not row-scoped).

2. **Implement sink operations for `CREATE`, `DELETE`, `SET`, `REMOVE`** ✅
   Delegates to hardened write helpers from M1 via `apply_mutations()`.

3. **Enforce eager barrier semantics at each write clause** ✅
   - `execute_mutation_stream()` fully consumes input before yielding output.
   - Integration test in `crates/uni/tests/df_mutation_test.rs`.

4. **Expression-based DELETE targets** (spec DELETE, `Delete5`)
   - The sink's delete operation must accept an *evaluated* expression value as its target, not only
     a bound variable name. Valid targets include:
     - A node or relationship variable
     - A list element: `DELETE list[i]`
     - A map property: `DELETE map.key`
     - A nested map/list path: `DELETE map.key.list[i]`
     - A path object (decomposed into constituent nodes and relationships)
   - Expression evaluation must happen before the delete call, inside the sink dispatch.

5. **Path deletion semantics** (spec DELETE, `Delete5[7]`)
   - When the delete target is a path object, the executor must decompose it into its nodes and
     relationships and delete all of them. Relationships are deleted before their endpoint nodes.
   - Path deletion must produce correct `-nodes`, `-relationships`, `-labels` side-effect counts.

6. **Read-your-write visibility for post-mutation reads** ✅
   - `QueryContext` with L0 buffer + `transaction_l0` + `pending_flush_l0s` provides visibility.
   - `QueryContext::new_with_pending()` added in `crates/uni-store/src/runtime/context.rs`.

Primary files:

- `crates/uni-query/src/query/df_graph/mutation_common.rs` (`MutationExec`, `MutationContext`, `MutationKind`)
- `crates/uni-query/src/query/df_graph/mutation_create.rs` (thin wrapper + constructor)
- `crates/uni-query/src/query/df_graph/mutation_set.rs` (thin wrapper + constructor)
- `crates/uni-query/src/query/df_graph/mutation_remove.rs` (thin wrapper + constructor)
- `crates/uni-query/src/query/df_graph/mutation_delete.rs` (thin wrapper + constructor)
- `crates/uni-query/src/query/df_graph/mod.rs` (exports)
- `crates/uni-query/src/query/df_planner.rs` (planner wiring)
- `crates/uni-query/src/query/executor/read.rs` (routing, MutationContext construction)
- `crates/uni-query/src/query/executor/write.rs` (mutation helpers)
- `crates/uni-store/src/runtime/context.rs` (`QueryContext::new_with_pending`)

Test gates:

1. Unit tests for sink row→mutation mapping and side-effect counters. ✅
2. Correctness test: `CREATE (n) RETURN n` followed by `MATCH (n) RETURN count(n)` in same session
   sees the created node (read-your-write). ✅
3. Integration tests for mixed query pipelines with `WITH`, `UNWIND`, `LIMIT`, `SKIP`. ✅ (partial)
4. Unit test: expression-based delete target (list index, map property) dispatches correctly.

Exit criteria:

1. New path works for at least one write clause end-to-end. ✅
2. Eager barrier is proven correct by the read-your-write correctness test. ✅

---

## M3 - Clause-by-Clause Enablement on New Path ✅ COMPLETE

Goal: move stable mutation clauses to DF + Writer sink incrementally, with full TCK parity per clause.

**Status: COMPLETE** (2026-02-19)

- All 5 mutation operators (CREATE, SET, REMOVE, DELETE, MERGE) fully wired in `HybridPhysicalPlanner`.
- Simple terminal mutations (no RETURN/WITH shaping, no nested mutations) route to DF path.
- Complex mutations with RETURN/WITH, SKIP/LIMIT, or nested mutations fall back via
  `needs_mutation_fallback()` in `read.rs`.
- MERGE routed through `MutationMergeExec` for terminal queries (M4.1 update).

### M3 Parity Fixes Round 1 (2026-02-18, +23 TCK scenarios)

Targeted 3 root causes accounting for 19 of 30 non-MERGE mutation failures. Achieved +23
(exceeded target due to cascading fixes to MERGE and bonus DELETE syntax validation).

| Fix | Files Modified | Scenarios Fixed |
|-----|----------------|-----------------|
| Label SET schemaless: removed schema validation in `SetItem::Labels` | `write.rs` | Set3[1-7], Set6[8-14] (+14) |
| Multi-property REMOVE batching: read-once/write-once per variable | `write.rs`, `expr_eval.rs` | Remove1[2,4,7] (+3) |
| DELETE connected node: `all_edge_type_ids()` + `ConstraintVerificationFailed` error | `write.rs`, `read.rs`, `impl_query.rs` | Delete1[7] (+1) |
| DELETE syntax validation: reject `Expr::LabelCheck` in DELETE | `planner.rs` | Delete1[8], Delete2[5] (+2) |
| CREATE already-bound: standalone bare node check | `planner.rs` | Create1[13] (+1) |
| Cascade: label SET fix applied to MERGE ON CREATE/ON MATCH SET | (via `write.rs`) | Merge2[1], Merge3[2] (+2) |

### M3 Parity Fixes Round 2 (2026-02-19, +12 TCK scenarios)

Targeted remaining Delete, Set, and Create failures. Achieved +12 with zero regressions.

| Fix | Files Modified | Scenarios Fixed |
|-----|----------------|-----------------|
| Schemaless edge visibility in `batch_detach_delete_vertices`: changed `schema.edge_types` to `schema.all_edge_type_ids()` | `read.rs` | Delete5[1], Delete5[5] (+2) |
| Two-pass non-detach DELETE: collect all edges/nodes across paths, delete edges first, then nodes. Dedup via `seen_vids`/`seen_eids` | `read.rs`, `mutation_common.rs` | Delete5[7] (+1) |
| BindPath null-safety for OPTIONAL MATCH: check for null path/edge/node variables before constructing path | `read.rs` | Delete3[2] (+1) |
| Property type validation: reject Map, Node, Edge, Path as property values; reject nested lists | `write.rs`, `error.rs` | Set1[10] (+1) |
| Edge uniqueness in `GraphTraverseMainExec`: added `used_edge_columns` with scope filtering via `scope_match_variables` | `traverse.rs`, `df_planner.rs`, `planner.rs` | Create5[3] (+1) |
| Cascade: Delete5 expression/path fixes covered remaining scenarios | (via `read.rs`) | Delete5[2,3,4,6,8,9] (+6) |

### Routing Helpers in `read.rs`

| Helper | Purpose |
|--------|---------|
| `contains_write_operations()` | Detects any write op in plan tree (triggers MutationContext build) |

### Prerequisite

Gate A must be satisfied (M1 exit criteria met) before enabling any clause on the new path. ✅

### Enablement Order

1. `CREATE`
2. `DELETE` / `DETACH DELETE`
3. `REMOVE`
4. `SET`

### Tasks Per Clause

1. Enable routing for clause. ✅ (all 4 wired)
2. Run clause-specific TCK family and targeted integration tests.
3. Fix parity gaps.
4. Promote clause flag to default-on only after TCK gate + perf sanity pass.

### CREATE-Specific Tasks

- Validate that newly created nodes/relationships are immediately visible in subsequent clauses
  in the same statement (read-your-write via L0 buffer).
- Run the `UNWIND + CREATE + SET n = map` integration test (spec p.104):
  ```cypher
  UNWIND $props AS map
  CREATE (n)
  SET n = map
  ```
  This requires CREATE (new path) and SET map-replace (M1) to work in a single pipeline.
  Verify correct side-effect counts for both created nodes and set properties.
- Validate path variable binding: `CREATE p = (a)-[:R]->(b) RETURN p` returns the full path.

### DELETE-Specific Tasks

- Validate expression-based delete targets from M2 work end-to-end (Delete5 scenarios).
- Validate path deletion: delete a path object and confirm all constituent nodes and relationships
  are removed with correct side-effect counts.
- Validate DELETE error semantics: deleting a node with relationships without DETACH must raise
  a runtime error (`ConstraintVerificationFailed`). ✅ Fixed: `check_vertex_has_no_edges` uses
  `schema.all_edge_type_ids()`, error mapped to `UniError::Constraint`.
- Validate relationship-only delete leaves endpoint nodes intact.
- Validate DELETE syntax: reject label-check expressions (`DELETE n:Label`). ✅ Fixed in planner.

### SET-Specific Tasks

- Validate all six SET semantic forms from M1 work through the sink path, not just the fallback.
- Confirm side-effect counts for entity-copy form (`SET n = <node>`) are correct through sink.

Primary files:

- `crates/uni-query/src/query/executor/read.rs` (routing)
- `crates/uni-query/src/query/df_planner.rs` (planning rules)
- `crates/uni-query/src/query/df_graph/` (physical implementation)
- `crates/uni-query/src/query/executor/result_normalizer.rs` (if row shape adjustments needed)

TCK gates:

1. `Create1-6`: Create1 **20/20** ✅, Create2 **24/24** ✅, Create3 **13/13** ✅, Create4 **2/2** ✅, Create5 **5/5** ✅, Create6 **14/14** ✅
2. `Delete1-6`: Delete1 **8/8** ✅, Delete2 **5/5** ✅, Delete3 **2/2** ✅, Delete4 **3/3** ✅, Delete5 **9/9** ✅, Delete6 **14/14** ✅
3. `Remove1-3`: Remove1 **7/7** ✅, Remove2 **5/5** ✅, Remove3 **21/21** ✅
4. `Set1-6`: Set1 **11/11** ✅, Set2 **3/3** ✅, Set3 **8/8** ✅, Set4 **5/5** ✅, Set5 **5/5** ✅, Set6 **21/21** ✅

Integration test gate:

1. `UNWIND + CREATE + SET n = map` bulk-create pipeline produces correct node count, property count,
   and side-effect accounting.

Exit criteria:

1. All four clause families pass at full TCK parity.
2. No regression in read-only TCK families.

---

## M4 - MERGE Strategy Decision and Delivery ✅ COMPLETE

Goal: choose and implement a clear MERGE plan. Gate A (M1 exit) and M3 exit are prerequisites.

**Status: COMPLETE** (2026-02-19)

- **All 76/76 MERGE TCK scenarios passing (100%)**
- Merge1: **17/17** (was 8/17) — core node MERGE, property resolution, path binding, labels, VariableAlreadyBound
- Merge2: **6/6** (was 4/6) — read-own-writes, ON CREATE SET with property expressions
- Merge3: **5/5** (was 3/5) — read-own-writes, ON MATCH SET with property expressions
- Merge4: **2/2** (was 0/2) — read-own-writes with mixed ON CREATE/ON MATCH SET
- Merge5: **29/29** (was 19/29) — relationship MERGE, undirected, sequential chains, path binding, deleted entities
- Merge6: **6/6 (100%)** — ON CREATE SET (all forms including entity-copy and map-append)
- Merge7: **5/5 (100%)** — ON MATCH SET (all forms)
- Merge8: **1/1 (100%)** — combined ON CREATE + ON MATCH
- Merge9: **4/4** (was 2/4) — MERGE in pipelines with property expressions

### Prerequisite

M1 exit criteria (including all SET map forms and entity-copy) must be satisfied before MERGE
ON CREATE/ON MATCH subclause work begins, because those subclauses use the same SET semantics.
**Prerequisite satisfied** — Gate A complete.

### Implementation Summary (8 phases, completed 2026-02-19)

**Phase 1: VariableAlreadyBound validation** — `planner.rs`
- MERGE node variable already in scope raises `SyntaxError: VariableAlreadyBound` when standalone or introducing new labels/properties.
- Bare variable endpoints in relationship patterns remain valid.
- TCK: Merge1[15], Merge5[22]

**Phase 2: Multiple labels in MERGE match** — `write.rs`
- Scan now filters all labels (not just first) via `hasLabel` conjunction filter for multi-label nodes.
- TCK: Merge1[10]

**Phase 3: Property expression resolution** — `write.rs`
- Added `resolve_merge_properties` method: evaluates property map expressions against current row context to produce concrete literals before building scan filters.
- Added `value_to_literal_expr` helper for Value→Expr conversion.
- Applied at 4 call sites: bound node properties, unbound node properties, relationship properties, target node properties.
- TCK: Merge1[11], Merge5[14], Merge9[4] (+cascading fixes)

**Phase 4: Read-own-writes verification** — no additional code change needed
- Phase 3 property resolution unblocked these scenarios. Per-row MERGE correctly sees prior rows' creates via shared L0 buffer.
- TCK: Merge2[5], Merge3[4], Merge4[1], Merge4[2]

**Phase 5: Deleted entities invisible to MERGE** — `write.rs`
- Fixed TCK side effects to use gross (not net) counts via ID set comparison.
- Fixed `__used_edges` key conflict in `final_matches` filter.
- TCK: Merge1[14], Merge5[20], Merge5[21]

**Phase 6: Path variable binding** — `write.rs`
- Implemented path variable binding in `execute_merge`: constructs `Value::Path` from matched/created nodes and edges after MERGE completes.
- TCK: Merge1[13], Merge5[10]

**Phase 7: Undirected MERGE + startNode/endNode** — `write.rs`, expression evaluator
- Fixed `startNode()`/`endNode()` to support `_src`/`_dst` fallback and `find_node_by_vid` for MERGE-created edges.
- TCK: Merge5[11]

**Phase 8: Sequential chains / double aliasing** — `read.rs`
- Fixed BFS self-loop detection (removed `visited.insert` for source vertex).
- Fixed empty-string key (`""`) polluting `final_matches` filter between sequential MERGE clauses: unnamed MERGE nodes use `""` as placeholder variable, now skipped in consistency filter alongside `__`-prefixed keys.
- TCK: Merge5[9], Merge5[18], Merge5[19], Merge1[9]

### Phase 9: DataFusion MutationExec routing (M4.1, 2026-02-19)

Wired MERGE into the DataFusion `MutationExec` framework so terminal MERGE queries (no RETURN/WITH
shaping, no nested mutations) flow through the DF path. Complex MERGE continues to use fallback via
`needs_mutation_fallback()`.

**Key design decision: Writer lock handling.** Unlike CREATE/SET/REMOVE/DELETE which use simple
per-row helpers called under a pre-acquired writer lock, `execute_merge()` manages its own writer
lock internally (acquires/releases per-row because `execute_merge_match()` needs to run a read
subplan between lock acquisitions). Therefore, `execute_mutation_inner()` handles MERGE *before*
the writer lock acquisition, delegating entirely to `execute_merge()`.

Files modified:
- `crates/uni-query/src/query/df_graph/mutation_common.rs` — Added `MutationKind::Merge` variant
  with `pattern`, `on_match`, `on_create` fields. MERGE branch in `execute_mutation_inner()` skips
  the shared writer lock and delegates to `executor.execute_merge()` directly.
- `crates/uni-query/src/query/df_graph/mutation_merge.rs` **(NEW)** — Thin wrapper with
  `MutationMergeExec` type alias and `new_merge_exec()` constructor.
- `crates/uni-query/src/query/df_graph/mod.rs` — Added module export.
- `crates/uni-query/src/query/df_planner.rs` — Replaced MERGE error stub with `MutationMergeExec`
  planning via `new_merge_exec()`.
- `crates/uni-query/src/query/executor/read.rs` — Renamed `contains_merge_or_foreach()` to
  `contains_foreach()` (removed MERGE from always-fallback gate). Added `LogicalPlan::Merge` to
  `is_mutation_plan()` and `has_nested_mutations()`.

Verification: 76/76 MERGE TCK, 92/92 merge unit tests, 3814/3897 full TCK (zero regressions).

### Legacy Options (superseded)

~~**Option 1** (recommended near-term): Keep MERGE on fallback path~~ — **Superseded: full parity achieved and DF routing wired.**

**ON CREATE SET** (Merge6) ✅ COMPLETE — 6/6
**ON MATCH SET** (Merge7/8) ✅ COMPLETE — 5/5 + 1/1
**MERGE in pipelines** (Merge9) ✅ COMPLETE — 4/4

### MERGE Map Parameter Error Case

The spec (p.121) states MERGE does not support map parameters the way CREATE does. Add a test that
validates MERGE with a map parameter either raises an error or is explicitly documented as unsupported.
This applies under both Option 1 (fallback must handle it correctly) and Option 2.

Primary files:

- `crates/uni-query/src/query/executor/write.rs` (MERGE execution: match-or-create, property resolution, path binding, final_matches filter)
- `crates/uni-query/src/query/executor/read.rs` (routing: `contains_foreach()`, `is_mutation_plan()`, `has_nested_mutations()`)
- `crates/uni-query/src/query/planner.rs` (VariableAlreadyBound validation in validate_merge_clause)
- `crates/uni-query/src/query/df_graph/mutation_merge.rs` (MutationMergeExec constructor)
- `crates/uni-query/src/query/df_graph/mutation_common.rs` (MutationKind::Merge, MERGE branch in execute_mutation_inner)
- `crates/uni-query/src/query/df_planner.rs` (MERGE planning via new_merge_exec)

TCK gates:

1. `Merge1-9` full family: **76/76 (100%)** ✅

Exit criteria:

1. MERGE behavior is parity-complete: **76/76 scenarios passing.** ✅
2. Map parameter behavior is tested and matches spec. ✅
3. Terminal MERGE queries route through DataFusion MutationExec path. ✅

---

## M5 - Hardening, Performance, and Default Rollout ✅ COMPLETE

Goal: production-readiness and safe default enablement.

**Status: COMPLETE** (2026-02-20)

### M5 Implementation Details

**Code cleanup:**
- Removed legacy executor fallback paths, `MutationPathConfig`, and `LOAD CSV` clause.
- All queries now route exclusively through DataFusion.
- Deleted `DF_NATIVE_MUTATION.md` (superseded by this document).

**Benchmark suite** (`crates/uni/benches/mutation_benchmarks.rs`):
- Criterion benchmarks for mutation operations:
  `create_100_nodes`, `set_100_properties`, `delete_100_nodes`, `create_then_match`, `merge_50_nodes`.
- Uses schemaless in-memory `Uni` for minimal setup overhead.

**Stress tests** (`crates/uni/tests/mutation_stress_test.rs`):
- 6 `#[ignore]`d stress tests at 10k scale: `create_10k_nodes`, `set_10k_nodes`, `delete_10k_nodes`,
  `mixed_mutations_10k`, `merge_10k_ops`, `create_edges_5k`.

**Documentation:**
- Updated `docs/QUERY_EXECUTION_PATH.md` with DataFusion-only execution architecture.

Tasks:

1. Remove legacy executor fallback and consolidate on DataFusion engine. ✅
2. Benchmark mixed read/write workloads. ✅
3. Validate memory and lock behavior under high row counts. ✅ (stress tests)

Primary files:

- `crates/uni-query/src/query/executor/read.rs`
- `crates/uni/benches/mutation_benchmarks.rs` (benchmarks)
- `crates/uni/tests/mutation_stress_test.rs` (stress tests)
- `docs/QUERY_EXECUTION_PATH.md` (execution architecture docs)

Exit criteria:

1. All queries route through DataFusion exclusively. ✅

---

## 4. File-Level Work Breakdown

## 4.1 `crates/uni-query`

1. `src/query/executor/read.rs` ✅
   - DDL/Admin routing to dedicated handlers; all other queries through DataFusion.
   - `MutationContext` built in `execute_datafusion()` when write operations detected.
2. `src/query/executor/write.rs` ✅
   - All SET semantic forms implemented: map-replace, map-append, entity-copy, null-property.
   - Null-entity silent-skip guard at mutation helper entry.
   - Reusable APIs: `execute_create_pattern()`, `execute_set_items_locked()`,
     `execute_remove_items_locked()`, `execute_delete_item_locked()`.
   - `EdgeIdentity` struct + `extract_edge_identity()` for edge mutation helpers.
3. `src/query/executor/core.rs`
4. `src/query/df_planner.rs` ✅
   - All 5 mutation operators wired: `new_create_exec`, `new_set_exec`, `new_remove_exec`, `new_delete_exec`, `new_merge_exec`.
   - `with_mutation_context()` / `require_mutation_ctx()` for context management.
   - `CreateBatch` handled by chaining individual `new_create_exec` calls.
5. `src/query/df_graph/mod.rs` ✅
   - Exports: `MutationContext`, `MutationExec`, and all typed wrappers including `MutationMergeExec`.
6. `src/query/df_graph/mutation_common.rs` ✅ (NEW)
   - `MutationExec` struct implementing `ExecutionPlan` with `MutationKind` dispatch.
   - `MutationContext` struct with executor, writer, prop_manager, params, query_ctx.
   - `MutationKind` enum: Create, Set, Remove, Delete, Merge variants.
   - `execute_mutation_stream()` with eager barrier pattern.
   - `apply_mutations()` dispatching to write helpers by kind.
   - MERGE handled specially: delegates to `executor.execute_merge()` before writer lock
     acquisition (MERGE manages its own lock internally).
7. `src/query/df_graph/mutation_{create,set,remove,delete,merge}.rs` ✅ (NEW)
   - Thin wrappers: type alias + typed constructor function per clause.
8. `src/query/df_graph/traverse.rs` ✅
   - `GraphTraverseMainExec`: added `used_edge_columns` for relationship uniqueness enforcement.
   - `GraphTraverseMainStream`: per-row edge uniqueness filtering in `expand_batch`.
9. `src/query/planner.rs` ✅
   - `TraverseMainByType`: added `scope_match_variables` for MATCH-scoped edge uniqueness.
10. `src/query/executor/result_normalizer.rs`
    - Preserve output shape parity for mutated rows if needed. (not yet needed)

## 4.2 `crates/uni-store`

1. `src/runtime/writer.rs` — unchanged, serves as durability backend.
2. `src/runtime/context.rs` ✅
   - `QueryContext::new_with_pending()` added for constructing context with pending L0 buffers.
   - Read-your-write visibility confirmed via L0 buffer + transaction_l0 + pending_flush_l0s.
3. `src/runtime/property_manager.rs` ✅
   - `overlay_l0_edge_batch` updated to include all L0 properties when `_all_props` requested.

## 4.3 `crates/uni-tck`

1. Use mutation feature subsets as mandatory release gates.
2. Keep side-effect assertions unchanged; they are contract checks.

Files:

- `crates/uni-tck/src/steps/and.rs` (reference for side-effect contract)
- `crates/uni-tck/src/matcher/error.rs` (error classification, `is_runtime_detail_code`)
- `crates/uni-tck/tck/features/clauses/{create,delete,set,remove,merge}/*.feature`

## 4.4 `docs`

1. Keep `docs/DATAFUSION_MUTATION_APPROACHES.md` as design reference.
2. Update `docs/QUERY_EXECUTION_PATH.md` after rollout changes.
3. Add migration notes once defaults switch.

---

## 5. Test and Validation Plan

## 5.1 Core commands

1. `cargo nextest run`
2. `scripts/run_tck_with_report.sh "~Create|~Delete|~Set|~Remove|~Merge"` for focused mutation runs.
3. Full TCK run before default enablement.

## 5.2 Stage gates

**Gate A** (prerequisite for M2 sink work and M4 ON CREATE/ON MATCH): ✅ **SATISFIED**
- `Set4` passes — all 5 scenarios including null-entity Scenario [5]. ✅
- `Set5` passes — all 5 scenarios including null-entity Scenario [1]. ✅
- Entity-copy `SET n = <node/rel>` passes with correct side-effect counts (Merge6[6]). ✅
- Multiple SET items ordered correctly. ✅

**Gate B** (prerequisite for promoting each clause to default-on): ✅ **SATISFIED**
- `Create6`: **14/14 (100%)** ✅ (was 12/14)
- `Delete6`: **14/14 (100%)** ✅ (was 12/14)
- `Remove3`: **21/21 (100%)** ✅ (was 18/21)
- `Set6`: **21/21 (100%)** ✅ (was 14/21) — label SET in schemaless mode now works.
- `Delete5`: **9/9 (100%)** ✅ (was 2/9) — schemaless edge visibility, two-pass non-detach DELETE, path deletion all fixed.

**Gate C** (prerequisite for M5 default rollout): **NOT APPLICABLE YET**
- Side-effect counts are identical between fallback and new path for a sampled query corpus
  covering all clause families.

**Gate D** (prerequisite for any milestone promotion): ✅ **SATISFIED**
- No regression in read-only TCK families (`Match1-9`, `Return1-8`, `With1-7`, etc.).
- Full TCK at 97.9% (3814/3897), +138 vs pre-M1 baseline.

**Gate E** (prerequisite for M4 MERGE phases): ✅ **SATISFIED**
- Gate A is satisfied. ✅
- M3 is complete (all 4 operators wired, simple mutations on DF path). ✅
- M4 MERGE complete: 76/76 scenarios passing (100%). ✅

## 5.3 Additional Integration Tests Required

The following integration scenarios are not covered by individual TCK files but are required for
correctness and must be written as separate integration tests:

1. **Read-your-write barrier test**: `CREATE (n:X) WITH n MATCH (m:X) RETURN count(m)` must return
   1, not 0. Validates that the eager write boundary makes created nodes visible to subsequent reads.

2. **UNWIND + CREATE + SET n = map bulk pipeline** (spec p.104):
   ```cypher
   UNWIND [{name:'A'}, {name:'B'}] AS props
   CREATE (n:Person)
   SET n = props
   RETURN n.name
   ```
   Must produce 2 nodes with correct names and side effects: `+nodes:2, +labels:1, +properties:2`.

3. **Expression-based DELETE from nested structure**:
   ```cypher
   MATCH (u:User)
   WITH {key: collect(u)} AS nodeMap
   DETACH DELETE nodeMap.key[0]
   ```
   Must delete exactly one node with correct side-effect counts.

4. **MERGE ON CREATE SET with entity-copy** (Merge6[6]): ✅ **PASSING**
   ```cypher
   MERGE (a)-[r:TYPE]->(b)
   ON CREATE SET r = a
   ```
   Must copy node properties to relationship on create; uses entity-copy form from M1.

## 5.4 Observability

1. Log selected path per clause (`fallback` vs `df_writer_sink`) using the tracing tag from M0.
2. Emit counters by clause and path.
3. Track error rates and rollback frequency by path.

---

## 6. Risk Register

1. **Semantic drift at write boundaries**
   - Mitigation: explicit eager barriers and the read-your-write correctness test in M2.

2. **Incorrect side-effect counts**
   - Mitigation: retain TCK side-effect gates; add unit tests around counters for each SET form
     (including entity-copy and null-property removal).

3. **Expression-based delete targets failing in sink path**
   - Mitigation: expression evaluation for delete targets is an explicit M2 task; Delete5 is a
     mandatory Gate B requirement before DELETE is promoted to default-on.

4. **MERGE complexity stalling rollout** ✅ MITIGATED
   - Status: MERGE fully implemented with 76/76 TCK scenarios passing. 8-phase implementation completed.
     Terminal MERGE now routes through DataFusion `MutationMergeExec` (M4.1). MERGE manages its own
     writer lock internally, so `execute_mutation_inner()` delegates to `execute_merge()` without
     pre-acquiring the lock.

5. **ON CREATE/ON MATCH SET map forms broken without M1 foundation** ✅ MITIGATED
   - Mitigation: Gate E enforces that M1 (all SET forms) and M3 SET promotion are complete before
     any MERGE ON CREATE/ON MATCH phase begins.
   - Status: M1 complete. All ON CREATE SET (Merge6: 6/6) and ON MATCH SET (Merge7: 5/5, Merge8: 1/1)
     forms working correctly including entity-copy and map-append.

6. **Performance regressions in high-cardinality writes** ✅ MITIGATED
   - Mitigation: benchmarked before default-on; no regressions observed.

7. **Lock contention / writer scope regressions** ✅ MITIGATED
   - Mitigation: clause-scoped lock policy (lock acquired per clause, not per row) and load tests.

---

## 7. Rollout Strategy

**COMPLETE** — All mutation clauses now route through DataFusion exclusively.
The `MutationPathConfig` toggle used during rollout has been removed.

Historical rollout order: `CREATE` → `DELETE` → `REMOVE` → `SET` → `MERGE`.

---

## 8. Known Issues and Remaining Gaps

### Relationship property visibility (schemaless, post-flush)

Named property access on schemaless edge types (e.g., `e.valid_from`) returns NULL after
`flush()` because the storage layer does not have schema metadata for these edge types. This
affects queries that reference specific edge properties by name on schemaless edge types that
have been flushed to storage. Wildcard access (e.g., `RETURN e`) works correctly because it
uses `_all_props` which surfaces properties from L0 and from the overflow_json storage path.

**Impact**: `test_valid_at_edge_temporal` fails (1 cargo test). Not a mutation-specific issue.
**Scope**: Affects any schemaless edge type after flush. Does not affect L0-resident edges.

### Relationship mutation side-effects with SKIP/LIMIT/aggregation ✅ RESOLVED

Set6 is now **21/21 (100%)**. The remaining failures were caused by `SET n:Label` being
rejected in schemaless mode (same root cause as Set3). Fixed by removing schema validation
in the `SetItem::Labels` branch of `execute_set_items_locked` in `write.rs`.

### Expression-based DELETE (Delete5) ✅ RESOLVED

Delete5 is now **9/9 (100%)**. Fixes:
- `batch_detach_delete_vertices` now uses `schema.all_edge_type_ids()` instead of
  `schema.edge_types` to include schemaless edge types (Delete5[1], Delete5[5]).
- Non-detach DELETE restructured to two-pass: collect all edges and nodes across all paths/items
  first, delete all edges, then delete all nodes. Deduplication via `seen_vids`/`seen_eids`
  HashSets prevents double-deletion when paths share nodes/edges (Delete5[7]).
- Same two-pass pattern applied to DataFusion mutation path in `mutation_common.rs`.
- Path deletion and expression-based targets (list index, map property, nested paths) all working.

### OPTIONAL MATCH path binding (Delete3) ✅ RESOLVED

Delete3 is now **2/2 (100%)**. Fixed in `BindPath` handler: when OPTIONAL MATCH finds no match,
edge/node variables are `Value::Null`, but `coerce_row_edge()` was creating placeholder
`Edge{eid:0, type:""}` instead of preserving null. Added null-safety check — if any path,
edge, or node variable is null, the path variable is set to `Value::Null` instead of
constructing a placeholder path.

### Property type validation (Set1) ✅ RESOLVED

Set1 is now **11/11 (100%)**. Added `validate_property_value` in `write.rs` that rejects
`Map`, `Node`, `Edge`, `Path` as direct property values, and `List` containing any of the
above or nested lists. Error: `TypeError: InvalidPropertyType`. Added `InvalidPropertyType`
to `is_runtime_detail_code` in `error.rs`. Validation called in all three property SET branches.

### Mixed-direction multi-hop MATCH (Create5) ✅ RESOLVED

Create5 is now **5/5 (100%)**. Root cause: `GraphTraverseMainExec` (DataFusion schemaless
traverse operator) was missing relationship uniqueness enforcement. Added `used_edge_columns`
to `GraphTraverseMainExec` and `GraphTraverseMainStream`, with per-row filtering to exclude
already-used edges. Scoped via `scope_match_variables` in the logical plan to prevent
cross-clause filtering (e.g., edges reused across MATCH clauses via WITH remain valid).

### Label-based SET (Set3) ✅ RESOLVED

Set3 is now **8/8 (100%)**. Fixed by removing schema label validation in `execute_set_items_locked`
(`write.rs`). In schemaless mode, labels are now created dynamically via the Writer, matching
CREATE behavior.

### Multi-property REMOVE (Remove1) ✅ RESOLVED

Remove1 is now **7/7 (100%)**. Fixed by batching property removals per variable in
`execute_remove_items_locked` (`write.rs`) — reads properties once, nulls all specified
properties, writes back once. Also fixed `eval_keys` in `expr_eval.rs` to exclude null-valued
properties for entities (nodes/edges).

### DELETE error validation (Delete1, Delete2) ✅ RESOLVED

Delete1 is now **8/8 (100%)** and Delete2 is now **5/5 (100%)**. Fixes:
- Delete1[7]: `check_vertex_has_no_edges` now uses `schema.all_edge_type_ids()` to include
  schemaless edge types. Error message includes `ConstraintVerificationFailed: DeleteConnectedNode`
  prefix, mapped to `UniError::Constraint` in `impl_query.rs`.
- Delete1[8], Delete2[5]: DELETE clause now validates that targets are simple variable references,
  rejecting `Expr::LabelCheck` expressions with `SyntaxError: InvalidDelete`.

### CREATE already-bound variable (Create1) ✅ RESOLVED

Create1 is now **20/20 (100%)**. Fixed in planner: standalone bare nodes in CREATE that
reference variables from previous clauses (MATCH/WITH) are now rejected with
`SyntaxError: VariableAlreadyBound`. Bare nodes used as relationship endpoints in the same
CREATE (self-loop patterns) remain valid.

### MERGE core match-or-create (Merge1-5, Merge9) ✅ RESOLVED

All MERGE scenarios now passing: **76/76 (100%)**. Implemented in 8 phases + DF routing (M4.1).
Terminal MERGE queries now flow through `MutationMergeExec` on the DataFusion path. Phases:

1. **VariableAlreadyBound validation** (`planner.rs`): MERGE node variables already in scope
   raise `SyntaxError` when standalone or introducing new labels/properties. Merge1[15], Merge5[22].

2. **Multi-label MERGE match** (`write.rs`): Scan now checks all labels via `hasLabel` conjunction,
   not just the first label. Merge1[10].

3. **Property expression resolution** (`write.rs`): `resolve_merge_properties` evaluates property
   map expressions against the current row context to produce concrete literals before scan filter
   construction. Applied at 4 call sites (bound/unbound nodes, relationships, target nodes).
   Merge1[11], Merge5[14], Merge9[4].

4. **Read-own-writes**: No code change needed — Phase 3 unblocked these. Per-row MERGE sees prior
   creates via shared L0 buffer. Merge2[5], Merge3[4], Merge4[1,2].

5. **Deleted entities invisible** (`write.rs`): TCK side effects use gross counts via ID set
   comparison. Fixed `__used_edges` key conflict in `final_matches` filter. Merge1[14], Merge5[20,21].

6. **Path variable binding** (`write.rs`): Constructs `Value::Path` from matched/created pattern
   elements. Merge1[13], Merge5[10].

7. **Undirected MERGE + startNode/endNode** (`write.rs`, expression evaluator): `_src`/`_dst`
   fallback in startNode/endNode + `find_node_by_vid` for MERGE-created edges. Merge5[11].

8. **Sequential chains** (`read.rs`, `write.rs`): Fixed BFS self-loop detection. Fixed empty-string
   key (`""`) polluting `final_matches` filter — unnamed MERGE nodes use `""` as placeholder,
   now skipped alongside `__`-prefixed keys. Merge5[9,18,19], Merge1[9].

---

## 9. Approach B Program (After Approach A Stabilizes)

Only start when Approach A is stable for all mutation clauses (CREATE, SET, REMOVE, DELETE, MERGE).

Phases:

1. Design native DF mutation operator API and execution constraints.
2. Implement one clause prototype (`CREATE`) and compare against Approach A baseline.
3. Expand clause support only if complexity/performance tradeoff is favorable.
4. Keep rollback to Approach A path at all times during migration.
