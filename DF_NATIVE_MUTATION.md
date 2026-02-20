# Full DataFusion Mutation Coverage

Remove the fallback executor for complex mutations and route everything through DataFusion.

## Phases

| Phase | Status | Summary |
|-------|--------|---------|
| **1** Output Batch Reconstruction | Done | Core fix: `execute_mutation_inner` reconstructs output batches |
| **2** CREATE/MERGE Schema Extension | Done | Extended output schema for newly created entities |
| **3** Remove Fallback Conditions | Done | Only LOAD CSV still falls back |
| **4** FOREACH DataFusion Operator | Done | `ForeachExec` operator, no more fallback for FOREACH |
| **5** LOAD CSV (Deferred) | Done | Minimal TCK coverage, deferred |
| **6** Parity Testing & Verification | Done | All 18 regressions fixed, 3812/3895 (97.9%) |
| **7** Default Enablement | Done | All DF mutations enabled by default |

## Phase 6 — Parity Testing & Verification (Complete)

**Baseline**: 3809/3895 passed (before fixes)
**Final**: 3812/3895 passed (97.9%) — +3 over baseline

### Fixes Applied

| Fix | Regressions Fixed | Root Cause |
|-----|-------------------|------------|
| **Fix 5**: Edge property types in `TraverseMainByTypeExec` | Set6[17-21], Remove3[17-19], Delete6[12] (9) | Edge props hardcoded as `Utf8` — `Int(42)` became `"42"` |
| **Fix 6**: `_all_props` update in `apply_properties_to_entity` | Set4[2-4], Set5[4] (4) | SET `n = {map}` didn't remove old properties from `_all_props` |
| **Fix 7**: `_all_props` update in REMOVE | Remove1[7] (1) | REMOVE of missing property injected `Null` into `_all_props` |
| **Fix 8**: Bare entity Map assembly from dotted columns | Delete2[3] (1) | Edge variables were only dotted columns, no bare Map for `evaluate_expr` |
| **Fix 9**: DELETE Path reconstruction | Delete3[1], Delete5[7] (2) | Arrow struct→Value conversion loses Path type; added `Path::try_from` in DELETE wildcard arms + field name fallbacks in `Node::try_from`/`Edge::try_from` |
| **Fix 10**: startNode/endNode UDFs | Merge5[11] (1) | `startNode(r)`/`endNode(r)` had no DataFusion UDF; added UDFs with node variable hints from CREATE/MERGE patterns |

**Total fixed**: 18 of 18 regressions from baseline.

### Key Implementation Details

**Fix 9 — Path Reconstruction**:
- `mutation_common.rs` and `write.rs`: In DELETE handler wildcard `_` arms, attempt `Path::try_from(&val)` before treating as vertex/edge
- `value.rs`: Added Arrow field names (`_vid`, `_eid`, `_type_name`) to `Node::try_from`/`Edge::try_from` fallback lists

**Fix 10 — startNode/endNode**:
- `df_udfs.rs`: `StartNodeUdf`/`EndNodeUdf` implementations decode cv_encoded edge, extract `_src_vid`/`_dst_vid`, match against node columns
- `df_expr.rs`: STARTNODE/ENDNODE translation passes node columns from both `variable_kinds` (MATCH nodes) and `node_variable_hints` (CREATE/MERGE nodes)
- `df_planner.rs`: `collect_mutation_node_hints()` extracts node variable names from CREATE/MERGE patterns, stored separately from `variable_kinds` to avoid affecting ID/TYPE/HASLABEL dotted-column resolution
