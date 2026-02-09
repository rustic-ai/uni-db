# VLP TCK Fix Plan — Progress Tracker

## Architecture Note

The executor uses `serde_json::Value` internally for row data (`HashMap<String, serde_json::Value>`). At the result boundary, these get converted to `uni_common::value::Value` (re-exported as `crate::types::Value` in uni-query). The `ResultNormalizer` then converts maps with `_vid`/`_eid`/`_label` keys into proper `Value::Node`/`Value::Edge`/`Value::Path` types.

This `serde_json::Value` usage throughout the executor is a known design debt — ideally it should use `uni_common::Value` everywhere except at the storage boundary.

---

## Fix Status

### Fix 1: Variable-Length Paths in Schemaless Executor — IN PROGRESS

**Files changed:** `crates/uni-query/src/query/executor/read.rs`, `crates/uni-query/src/query/planner.rs`

**What's done:**
- Removed the hard-block for VLP at the old line 3697
- Implemented `execute_traverse_main_by_type_vlp()` with BFS traversal
- Implemented `execute_traverse_main_by_type_vlp_entry()` (edge loading + delegation)
- Added `lookup_vertex_labels()` helper method
- Added `is_variable_length` flag to `LogicalPlan::TraverseMainByType`
- Changed planner to keep `step_var = rel.variable` for VLP (not map to path_var)
- Executor now calls VLP entry directly when `is_variable_length=true`
- Zero-length path support (`min_hops == 0`)

**What's still broken (Match4 results):**
- **[1] `*1..1` fixed-length VLP:** `r` shows as `Path(Path{...})` instead of `[[:T]]`.
  - Root cause: The VLP BFS sets `r` as `serde_json::Value::Array(edge_objs)`, but the result normalizer sees the array of edge-like maps and the overall path-like structure, converting it to `Value::Path`. Need to investigate the `serde_json::Value` → `uni_common::Value` conversion boundary to understand why an array of edge objects becomes a Path.
  - **TODO:** Trace the full conversion path from executor `HashMap<String, serde_json::Value>` to `Row { values: Vec<Value> }` to understand where the Path conversion happens.
- **[2] Simple VLP:** Node format issue — `x` returned as node but TCK can't match it. Probably property/label format mismatch.
- **[4] Longer paths:** `size()` expects a List but gets something else — the setup query `size(nodeList) - 2` fails. This is a pre-existing issue with `size()` on node lists.
- **[5] Property predicate VLP:** 6 results instead of 1 — VLP doesn't filter edges by properties like `{year: 1988}`. Need to add edge property filtering in VLP BFS.
- **[7] Bound relationship in VLP:** No result found — complex multi-MATCH with bound relationship. Needs relationship binding support across MATCH clauses.
- **[8] List-based VLP:** No result found — uses `UNWIND` and relationship lists, complex scenario.
- **[9] Missing asterisk:** PEG parse error doesn't contain `InvalidRelationshipPattern`. The `[:LIKES..]` pattern fails at PEG level before reaching `build_range`. Pre-parse detection in `parse()` catches `..` in brackets but may not trigger for this specific pattern.

**What passes now:** [3] zero-length VLP, [6] bound node VLP, [10] negative bound

### Fix 2: Relationship Uniqueness in Schemaless Executor — DONE (in Fix 1)

**File:** `crates/uni-query/src/query/executor/read.rs`

**What's done:**
- Added `__used_edges` extraction from input row in single-hop path
- Added edge skip for used edges in single-hop loop
- Added `__used_edges` tracking to each result row
- VLP BFS also tracks `used_edges_from_prev` and skips them

### Fix 3: Multi-Label hasLabel Fix — DONE

**File:** `crates/uni-query/src/query/executor/read.rs`

**What's done:**
- Changed hasLabel to split colon-joined `_label` strings: `label_str.split(':').any(|l| l == label_to_check)`
- Applied to both `_vid`-containing maps and legacy object format
- Added `_labels` array to node objects in single-hop path (and VLP)
- Kept `_label` colon-joined for backward compat

### Fix 4: OPTIONAL MATCH Named Path = Null — PENDING

**File:** `crates/uni-query/src/query/executor/read.rs`

Depends on Fix 1 being complete (path building). May work automatically once VLP path building is correct.

### Fix 5: Validation Errors — DONE

**File:** `crates/uni-query/src/query/planner.rs`

**What's done:**
- **Match1[6] InvalidParameterUse for node predicates:** Added `Expr::Parameter` check in `plan_unbound_node` properties handling
- **Match2[8] InvalidParameterUse for relationship predicates:** Added `Expr::Parameter` check at start of `plan_traverse_with_source`
- **Match3[29] RelationshipUniquenessViolation:** Changed the existing relationship variable scope check to detect `Edge` reuse as `RelationshipUniquenessViolation` (separate from `VariableTypeConflict`)

### Fix 6: sum() Returns Float Instead of Int — DONE

**File:** `crates/uni-query/src/query/executor/core.rs`

Changed `Accumulator::Sum(s) => json!(*s)` to `Accumulator::Sum(s) => numeric_to_json(*s)` which converts f64→i64 when fractional part is 0.

### Fix 7: VariableTypeConflict False Positive — DONE

**File:** `crates/uni-query/src/query/planner.rs`

Added `Scalar → Node/Edge` upgrade in `add_var_to_scope`: when existing var is `Scalar` and new type is `Node` or `Edge`, allow the rebinding (for CREATE context where a scalar from WITH holds a node reference).

### Fix 8: Error Message Format — PARTIALLY DONE

**Files:** `crates/uni-cypher/src/grammar/walker.rs`, `crates/uni-cypher/src/grammar/mod.rs`

**What's done:**
- `build_range()` error messages now include "SyntaxError: InvalidRelationshipPattern" prefix
- Added negative range bound detection
- Added pre-parse detection in `parse()` for `..` and `*-` patterns inside brackets

**What works:** [10] negative bound now passes
**What's broken:** [9] missing asterisk — `[:LIKES..]` doesn't trigger the bracket-content detection (needs investigation of exact PEG error position)

### Fix 9: Cartesian Product Property Type — NOT STARTED

**Root cause (from investigation):** For nodes stored only in L0 (as in TCK tests), integer properties should be preserved as `Value::Int`. The issue may be in the `serde_json::Value` representation in the executor — `json!(1)` creates `Number(1)` which should convert correctly. Needs more investigation.

---

## Key Architectural Issues Discovered

1. **Executor uses `serde_json::Value`:** The entire executor pipeline works with `HashMap<String, serde_json::Value>`. This gets converted to `uni_common::value::Value` at the result boundary via `ResultNormalizer`. The normalizer uses heuristics (checking for `_vid`, `_eid`, `nodes`+`relationships` keys) to detect nodes/edges/paths. This means any map that happens to have these keys gets auto-converted.

2. **VLP step variable semantics:** In Cypher, `[r*]` means `r` is a **list of relationships**. The planner was incorrectly mapping `r` to `path_variable` for VLP. Fixed to keep `r` as `step_variable` and set it to `Value::Array(edge_objs)` in the VLP BFS.

3. **`is_variable_length` flag needed:** For `*1..1`, min=max=1 looks identical to single-hop. Added `is_variable_length` field to `LogicalPlan::TraverseMainByType` so the executor can distinguish and always use VLP behavior for range patterns.

---

## Next Steps (Priority Order)

1. **Trace the serde_json → uni_common Value conversion** to understand why VLP step variable array becomes Path. Look for `From` impls or explicit conversion code.
2. **Fix VLP edge property filtering** (Match4[5]) — filter edges in BFS by property predicates
3. **Fix Match4[9]** — improve PEG error detection for missing asterisk
4. **Run full Match TCK** to measure overall improvement
5. **Run full TCK report** (`scripts/run_tck_with_report.sh`) for final numbers

---

## Files Modified

| File | Fixes |
|------|-------|
| `crates/uni-query/src/query/executor/read.rs` | Fix 1, 2, 3 |
| `crates/uni-query/src/query/planner.rs` | Fix 1, 5, 7 |
| `crates/uni-query/src/query/executor/core.rs` | Fix 6 |
| `crates/uni-cypher/src/grammar/walker.rs` | Fix 8 |
| `crates/uni-cypher/src/grammar/mod.rs` | Fix 8 |

## Test Commands

```bash
# Specific Match feature
TCK_FEATURE=clauses/match/Match4.feature cargo test -p uni-tck --test cucumber

# All Match features
TCK_FEATURE=match cargo test -p uni-tck --test cucumber

# Full TCK report
scripts/run_tck_with_report.sh
```
