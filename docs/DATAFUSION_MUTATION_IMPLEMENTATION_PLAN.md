# DataFusion Mutation Implementation Plan

**Date:** 2026-02-18
**Last updated:** 2026-02-18
**Depends on:** `docs/DATAFUSION_MUTATION_APPROACHES.md`
**Primary strategy:** Implement Approach A first, keep Approach B as staged follow-up.

## 0. Current Status

| Milestone | Status | Summary |
|-----------|--------|---------|
| **M0** | Not started | Baseline capture done via `compliance_reports/`; `MutationPathConfig` not yet implemented |
| **M1** | **COMPLETE** | All SET semantic forms implemented; Gate A satisfied |
| **M2** | Not started | |
| **M3** | Not started | |
| **M4** | Partially complete (via M1 edge property fix) | Merge6: 6/6, Merge7: 5/5, Merge8: 1/1 |
| **M5** | Not started | |

### Overall TCK (schemaless mode, 2026-02-18)

| Metric | Value |
|--------|-------|
| Total scenarios | 3897 |
| Passed | 3741 |
| Failed | 155 |
| Pass rate | **96.0%** |

### Mutation TCK Breakdown

| Feature | Passed | Total | Rate | Status |
|---------|--------|-------|------|--------|
| Create1 | 19 | 20 | 95% | Near-parity |
| Create2 | 24 | 24 | **100%** | Complete |
| Create3 | 11 | 13 | 85% | |
| Create4 | 2 | 2 | **100%** | Complete |
| Create5 | 4 | 5 | 80% | |
| Create6 | 12 | 14 | 86% | |
| Delete1 | 6 | 8 | 75% | |
| Delete2 | 4 | 5 | 80% | |
| Delete3 | 1 | 2 | 50% | |
| Delete4 | 3 | 3 | **100%** | Complete |
| Delete5 | 2 | 9 | 22% | Blocked (expression-based targets) |
| Delete6 | 12 | 14 | 86% | |
| Set1 | 10 | 11 | 91% | |
| Set2 | 3 | 3 | **100%** | Complete |
| Set3 | 1 | 8 | 12% | Blocked (label-set semantics) |
| **Set4** | **5** | **5** | **100%** | **Gate A** |
| **Set5** | **5** | **5** | **100%** | **Gate A** |
| Set6 | 12 | 21 | 57% | |
| Remove1 | 4 | 7 | 57% | |
| Remove2 | 5 | 5 | **100%** | Complete |
| Remove3 | 18 | 21 | 86% | |
| Merge1 | 8 | 17 | 47% | |
| Merge2 | 3 | 6 | 50% | |
| Merge3 | 2 | 5 | 40% | |
| Merge4 | 0 | 2 | 0% | |
| Merge5 | 19 | 29 | 66% | |
| **Merge6** | **6** | **6** | **100%** | **Complete (ON CREATE SET)** |
| **Merge7** | **5** | **5** | **100%** | **Complete (ON MATCH SET)** |
| **Merge8** | **1** | **1** | **100%** | **Complete (ON CREATE + ON MATCH)** |
| Merge9 | 2 | 4 | 50% | |

### Gate Status

| Gate | Status | Details |
|------|--------|---------|
| **Gate A** | **SATISFIED** | Set4: 5/5, Set5: 5/5. Entity-copy and map forms working. |
| Gate B | Not satisfied | Create6: 12/14, Delete5: 2/9, Set6: 12/21, Remove3: 18/21 |
| Gate C | Not applicable yet | Requires new path implementation |
| Gate D | Satisfied | No regressions in read-only suites |
| Gate E | Partially satisfied | Gate A done; M3 SET promotion not yet started |

## 1. Scope and Objectives

This plan turns the mutation design into an execution program with milestones and file-level tasks.

Objectives:

1. Route mutation queries through a DataFusion-compatible pipeline without losing openCypher semantics.
2. Preserve writer durability and single-writer correctness.
3. Reach mutation TCK parity in staged gates.
4. Keep rollback/fallback path available during rollout.

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
6. MERGE has an explicit status:
   - either parity-complete and enabled on new path, or
   - still pinned to fallback with clear feature flag and tests.
7. No regression in non-mutation query suites.

## 3. Milestone Plan (Recommended)

## M0 - Baseline and Safety Rails

Goal: establish controlled rollout and parity measurement before behavior changes.

### M0 Routing Toggle Design Decision (resolve before coding)

The per-clause toggle is a `MutationPathConfig` struct field on `QueryContext`, one boolean per clause
family (`create`, `delete`, `set`, `remove`, `merge`). Default for all fields is `false` (fallback path).
This is a runtime configuration struct, not a Cargo feature flag, so it can be toggled per-query in
tests and per-instance in production without recompilation. The toggle is checked at the point where
the executor dispatches mutation clauses. When toggled to `true`, the clause routes to the new
DF + Writer sink path; otherwise it falls through to the existing fallback executor.

Tasks:

1. Define `MutationPathConfig` struct and attach it to `QueryContext`.
2. Add per-clause routing check at dispatch points in the executor.
3. Add "new path vs fallback" marker in query metrics/logging using the existing tracing infrastructure.
4. Capture baseline TCK slices and store results in `compliance_reports/`.

Primary files:

- `crates/uni-query/src/query/executor/read.rs`
- `crates/uni-query/src/query/executor/mod.rs`
- `crates/uni-query/src/query/mod.rs`
- `compliance_reports/` (report snapshots, no behavior change)

Exit criteria:

1. `MutationPathConfig` is defined, all fields default to fallback, and routing checks compile.
2. Can toggle each mutation clause to fallback/new path independently via `QueryContext`.
3. Baseline TCK reports for mutation feature families are archived.

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
- Full TCK: 3741/3897 (96.0%), +74 scenarios vs pre-M1 baseline

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

## M2 - Mutation Sink Infrastructure (Approach A Core)

Goal: introduce a Writer-backed mutation sink abstraction and wire it into the execution pipeline.

### M2 Tasks

1. **Define mutation sink interface**
   - Input: row stream (batch or row-wise) with bound entities/aliases.
   - Output: updated row stream with mutated entity values reflected, and side-effect counters.
   - Backend: single acquired `Writer` per clause execution (clause-scoped lock, not row-scoped).

2. **Implement sink operations for `CREATE`, `DELETE`, `SET`, `REMOVE`** by delegating to the
   hardened write helpers from M1. The sink must not re-implement mutation semantics.

3. **Enforce eager barrier semantics at each write clause**
   - The sink must fully consume all input rows and commit all writes before yielding any output row.
   - DataFusion is pull-based; the barrier must be an explicit materialization step, not implicit.
   - Write a correctness test: after a `CREATE`, a subsequent `MATCH` in the same statement must
     see all created nodes (read-your-write visibility proof).

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

6. **Read-your-write visibility for post-mutation reads**
   - After the sink commits a write clause, subsequent reads in the same statement must go through
     the L0 buffer / statement context so that created/modified entities are visible.
   - Confirm `src/runtime/context.rs` and `src/runtime/l0_visibility.rs` provide this guarantee
     without changes, or document what must change.

Primary files:

- `crates/uni-query/src/query/df_planner.rs`
- `crates/uni-query/src/query/df_graph/mod.rs`
- `crates/uni-query/src/query/df_graph/` (new sink module(s))
- `crates/uni-query/src/query/executor/read.rs`
- `crates/uni-query/src/query/executor/write.rs`
- `crates/uni-store/src/runtime/writer.rs` (only if API extension needed)
- `crates/uni-store/src/runtime/context.rs`
- `crates/uni-store/src/runtime/l0_visibility.rs`

Test gates:

1. Unit tests for sink row→mutation mapping and side-effect counters.
2. Correctness test: `CREATE (n) RETURN n` followed by `MATCH (n) RETURN count(n)` in same session
   sees the created node (read-your-write).
3. Integration tests for mixed query pipelines with `WITH`, `UNWIND`, `LIMIT`, `SKIP`.
4. Unit test: expression-based delete target (list index, map property) dispatches correctly.

Exit criteria:

1. New path works for at least one write clause end-to-end under the `MutationPathConfig` flag.
2. Eager barrier is proven correct by the read-your-write correctness test.

---

## M3 - Clause-by-Clause Enablement on New Path

Goal: move stable mutation clauses to DF + Writer sink incrementally, with full TCK parity per clause.

### Prerequisite

Gate A must be satisfied (M1 exit criteria met) before enabling any clause on the new path.

### Enablement Order

1. `CREATE`
2. `DELETE` / `DETACH DELETE`
3. `REMOVE`
4. `SET`

### Tasks Per Clause

1. Enable routing for clause under `MutationPathConfig` flag.
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
  a runtime error (`ConstraintValidationFailed` or equivalent).
- Validate relationship-only delete leaves endpoint nodes intact.

### SET-Specific Tasks

- Validate all six SET semantic forms from M1 work through the sink path, not just the fallback.
- Confirm side-effect counts for entity-copy form (`SET n = <node>`) are correct through sink.

Primary files:

- `crates/uni-query/src/query/executor/read.rs` (routing)
- `crates/uni-query/src/query/df_planner.rs` (planning rules)
- `crates/uni-query/src/query/df_graph/` (physical implementation)
- `crates/uni-query/src/query/executor/result_normalizer.rs` (if row shape adjustments needed)

TCK gates:

1. `Create1-6` (including Create6 persistence scenarios)
2. `Delete1-6` (including Delete5 expression-based targets)
3. `Remove1-3`
4. `Set1-6` (including Set4 and Set5)

Integration test gate:

1. `UNWIND + CREATE + SET n = map` bulk-create pipeline produces correct node count, property count,
   and side-effect accounting.

Exit criteria:

1. All four clause families pass at full TCK parity.
2. No regression in read-only TCK families.

---

## M4 - MERGE Strategy Decision and Delivery (Partially Complete)

Goal: choose and implement a clear MERGE plan. Gate A (M1 exit) and M3 exit are prerequisites.

**Status: Phases 3-4 COMPLETE** (via M1 semantic hardening + edge property visibility fix)

- Merge6 (ON CREATE SET): **6/6 (100%)** — all forms working including entity-copy and map-append
- Merge7 (ON MATCH SET): **5/5 (100%)** — all forms working
- Merge8 (ON CREATE + ON MATCH): **1/1 (100%)**
- Merge9 (MERGE in pipelines): 2/4 (50%) — partial
- Merge1-5 (core match-or-create): still have gaps (see breakdown below)

### Prerequisite

M1 exit criteria (including all SET map forms and entity-copy) must be satisfied before MERGE
ON CREATE/ON MATCH subclause work begins, because those subclauses use the same SET semantics.
**Prerequisite satisfied** — Gate A complete.

### Option 1 (recommended near-term)

1. Keep MERGE on fallback path until full parity can be proven.
2. Document the explicit rationale for the deferral (complexity of all-or-nothing pattern semantics).
3. Continue new-path support for other mutation clauses.

### Option 2

1. Implement MERGE sink semantics on new path after all other mutation clauses are stable.
2. Deliver in sub-phases:

   **Phase 1: Node merge** — remaining gaps
   - Match-or-create semantics for single node patterns.
   - TCK gate: `Merge1` (8/17), `Merge2` (3/6), `Merge3` (2/5), `Merge4` (0/2).
   - Remaining failures involve label matching, property comparison, and side-effect counting.

   **Phase 2: Relationship merge** — remaining gaps
   - All-or-nothing pattern semantics for relationship patterns.
   - Undirected relationship MERGE creates with an arbitrary but deterministic direction.
   - TCK gate: `Merge5` (19/29).

   **Phase 3: ON CREATE SET** (Merge6) ✅ COMPLETE
   - `ON CREATE SET r.prop = value` — property assignment on create. ✅
   - `ON CREATE SET r.prop = null` — null-setting is a no-op (property not stored). ✅
   - `ON CREATE SET r = <entity>` — entity-copy form; reuses M1 entity-copy logic. ✅
   - `ON CREATE SET r += <map>` — map-append form; reuses M1 `+=` logic. ✅
   - Gate A (Set4+Set5) is a hard prerequisite for this phase. ✅
   - TCK gate: `Merge6` **6/6 (100%)**. ✅

   **Phase 4: ON MATCH SET** (Merge7) ✅ COMPLETE
   - `ON MATCH SET` applies only when the pattern was matched, not created. ✅
   - `ON MATCH SET` uses the same SET semantic forms as ON CREATE SET; no new logic required. ✅
   - TCK gate: `Merge7` **5/5 (100%)**, `Merge8` **1/1 (100%)**. ✅

   **Phase 5: MERGE in pipelines** (Merge9) — partial
   - MERGE combined with `WITH`, `UNWIND`, prior updates.
   - TCK gate: `Merge9` (2/4).

### MERGE Map Parameter Error Case

The spec (p.121) states MERGE does not support map parameters the way CREATE does. Add a test that
validates MERGE with a map parameter either raises an error or is explicitly documented as unsupported.
This applies under both Option 1 (fallback must handle it correctly) and Option 2.

Primary files:

- `crates/uni-query/src/query/executor/write.rs` (shared MERGE semantics)
- `crates/uni-query/src/query/executor/read.rs` (MERGE routing)
- `crates/uni-query/src/query/df_planner.rs`
- `crates/uni-query/src/query/df_graph/` (if MERGE sink added)

TCK gates:

1. `Merge1-9` full family.

Exit criteria:

1. MERGE behavior is either parity-complete or intentionally pinned to fallback with explicit rationale.
2. Map parameter behavior is tested and matches spec.

---

## M5 - Hardening, Performance, and Default Rollout

Goal: production-readiness and safe default enablement.

Tasks:

1. Remove temporary compatibility shims not needed post-parity.
2. Benchmark mixed read/write workloads and compare with fallback.
3. Validate memory and lock behavior under high row counts.
4. Set defaults to new path for stable clauses (`MutationPathConfig` field defaults flipped to `true`).

Primary files:

- `crates/uni-query/src/query/executor/read.rs`
- `crates/uni-query/src/query/df_graph/`
- `benches/` additions as needed

Exit criteria:

1. Stable default behavior and documented rollback toggle (set `MutationPathConfig` fields to `false`).

---

## 4. File-Level Work Breakdown

## 4.1 `crates/uni-query`

1. `src/query/executor/read.rs`
   - Add `MutationPathConfig` routing check per clause dispatch point.
   - Add transition routing from fallback to sink path.
   - Keep explicit fallback for unsupported clauses.
2. `src/query/executor/write.rs`
   - Implement all SET semantic forms: map-replace, map-append, entity-copy, null-property.
   - Add null-entity silent-skip guard at the entry of each mutation helper.
   - Expose reusable mutation helper APIs for the sink path.
   - Keep side-effect and row-update behavior canonical (sink delegates here, not re-implements).
3. `src/query/executor/mod.rs`
   - Define `MutationPathConfig` struct.
4. `src/query/df_planner.rs`
   - Replace "write unsupported" path with planned mutation boundaries.
   - Emit physical nodes for sink execution.
5. `src/query/df_graph/mod.rs`
   - Register mutation sink physical nodes.
6. `src/query/df_graph/*` (new modules expected)
   - `mutation_sink.rs` (or clause-specific sink modules).
   - Eager barrier materialization and row mapping.
   - Expression evaluation for delete targets (list index, map property, nested path, path object).
7. `src/query/executor/result_normalizer.rs`
   - Preserve output shape parity for mutated rows if needed.

## 4.2 `crates/uni-store`

1. `src/runtime/writer.rs`
   - Keep as durability backend.
   - Add small API adjustments only if sink contract requires it.
2. `src/runtime/context.rs` and `src/runtime/l0_visibility.rs`
   - Confirm read-your-write visibility for post-mutation reads in same statement.
   - Document the guarantee explicitly; if it is not currently guaranteed, implement it before M2.

## 4.3 `crates/uni-tck`

1. Use mutation feature subsets as mandatory release gates.
2. Keep side-effect assertions unchanged; they are contract checks.

Files:

- `crates/uni-tck/src/steps/and.rs` (reference for side-effect contract)
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

**Gate B** (prerequisite for promoting each clause to default-on): **NOT YET SATISFIED**
- `Create6`: 12/14 (86%) — 2 relationship-create-with-aggregation scenarios remaining.
- `Delete6`: 12/14 (86%) — 2 relationship-delete-with-aggregation scenarios remaining.
- `Set6`: 12/21 (57%) — relationship property SET with SKIP/LIMIT/aggregation gaps.
- `Remove3`: 18/21 (86%) — relationship property REMOVE with SKIP/LIMIT/aggregation gaps.
- `Delete5`: 2/9 (22%) — expression-based and nested delete targets blocked.

**Gate C** (prerequisite for M5 default rollout): **NOT APPLICABLE YET**
- Side-effect counts are identical between fallback and new path for a sampled query corpus
  covering all clause families.

**Gate D** (prerequisite for any milestone promotion): ✅ **SATISFIED**
- No regression in read-only TCK families (`Match1-9`, `Return1-8`, `With1-7`, etc.).
- Full TCK at 96.0% (3741/3897), +74 vs previous run.

**Gate E** (prerequisite for M4 MERGE ON CREATE/ON MATCH phases): **PARTIALLY SATISFIED**
- Gate A is satisfied. ✅
- M3 is not yet started (SET clause family not yet on new path).
- However, ON CREATE SET (Merge6) and ON MATCH SET (Merge7/8) already work via fallback path.

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

4. **MERGE complexity stalling rollout**
   - Mitigation: decouple MERGE from core clause rollout; keep fallback under Option 1.

5. **ON CREATE/ON MATCH SET map forms broken without M1 foundation** ✅ MITIGATED
   - Mitigation: Gate E enforces that M1 (all SET forms) and M3 SET promotion are complete before
     any MERGE ON CREATE/ON MATCH phase begins.
   - Status: M1 complete. All ON CREATE SET (Merge6: 6/6) and ON MATCH SET (Merge7: 5/5, Merge8: 1/1)
     forms working correctly including entity-copy and map-append.

6. **Performance regressions in high-cardinality writes**
   - Mitigation: benchmark before default-on and keep `MutationPathConfig` rollback available.

7. **Lock contention / writer scope regressions**
   - Mitigation: clause-scoped lock policy (lock acquired per clause, not per row) and load tests.

8. **`MutationPathConfig` toggle overhead**
   - Mitigation: toggle is a struct field check, not a lock; overhead is negligible. Confirm with
     a micro-benchmark if the hot path is affected.

---

## 7. Rollout Strategy

1. Ship feature-flagged by clause via `MutationPathConfig` (default all-fallback).
2. Enable in this order: `CREATE` → `DELETE` → `REMOVE` → `SET`.
3. Keep MERGE fallback until explicit promotion (after Gate E + `Merge1-9` pass).
4. Promote per clause after TCK gate + perf sanity + Gate D (no read regressions).

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

### Relationship mutation side-effects with SKIP/LIMIT/aggregation

Set6, Remove3, Create6, Delete6 all have failures (57%-86%) related to relationship
mutations combined with SKIP, LIMIT, and aggregation in RETURN/WITH. The side-effect
counts are correct but result set shaping after relationship mutations has gaps. This is
a Gate B blocker.

### Expression-based DELETE (Delete5)

Delete5 has 7/9 failing scenarios. Expression-based delete targets (list index, map property,
nested paths, path objects) are not yet implemented. This is an M2 task.

### Label-based SET (Set3)

Set3 has 7/8 failing scenarios. `SET n:Label` (adding labels) is not yet fully implemented.

---

## 9. Approach B Program (After Approach A Stabilizes)

Only start when Approach A is stable for non-MERGE mutation clauses.

Phases:

1. Design native DF mutation operator API and execution constraints.
2. Implement one clause prototype (`CREATE`) and compare against Approach A baseline.
3. Expand clause support only if complexity/performance tradeoff is favorable.
4. Keep rollback to Approach A path at all times during migration.
