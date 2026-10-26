# Triage of issues #302–#317 (2026-10-23)

All 15 issues were filed on 2026-10-21 or 2026-10-22 against **uni-db 4.1.1**.
At triage time it was the latest release and `main` was at 4.2.0, unreleased.
The reports were investigated through the public API, in isolated worktrees
off `main` (`c56f98f73`). The
4.1.1 behaviour was checked against the published wheel or crate wherever the
issue was not reproducible on `main`.

## Status (updated 2026-10-25)

- **All P1s are fixed on `main`:** #304 (`d78cb3748`), #312 (`0f9043325`),
  #316 plus the typed-Int64 `sum` wrap (`d6e455e5e`), #306 plus the fork
  flush failure and the other fork defects below (`279755263`). Triage also
  led to `610666b75`, which fixes a class not in any issue: UNWIND had its own
  evaluator that turned unknown functions into empty lists and other
  expressions into NULL.
- **Regression tests landed** for the issues `main` had already fixed: #305,
  #308, #309, #310 (declared case), #313 (`crates/uni/tests/common/bugs/` and
  `common/locy/`). The #310 schemaless specification is now a regular test
  alongside the local BM25 fix (verification recorded in the follow-up triage).
- **P2 implementation completed:** #307, #314, #315, the query options and
  PROFILE gaps, CREATE path binding and the empty-flush write-throttle defect.
  The October 24 handoff reports full merged-tree CI passing.
- **Local follow-up fixes:** virtual-ID comparisons, schemaless FTS scoring,
  and procedure inner-query counters. See `followup_triage_2026_10_25.md` for
  verification and the remaining backlog.
- **Still open:** #302, #311 (needs a reproduction), and #317. Release status
  needs a live publication check; a local version number does not prove shipping.
  The grammar's stale SLG comment has been corrected.

## Summary

| # | Title (short) | On `main` | Severity | Next step |
|---|---|---|---|---|
| 304 | Declaring a populated schemaless label hides its nodes | **Fixed** `d78cb3748` | **P1** silent wrong answer | Release |
| 306 | Fork diff empty for schemaless labels | **Fixed** `279755263`, with 2 more fork defects | **P1** | Release |
| 312 | Matched node's properties copied into every joined row | **Fixed** `0f9043325` | **P1** (writes fail at 1 GiB) | Release |
| 314 | Classifier relation consumed through IS: Arrow error | **Fixed** | P2 | Yield typing and transient-column handling landed |
| 316 | Exact integer aggregation | **Fixed** `d6e455e5e` (bug part) | P1 for the bug part | Release |
| 302 | uni-locy always depends on candle | Valid | P3 | Open: reimplement the classifier without candle (S) |
| 307 | QUERY WHERE costs the full fixpoint | **Fixed** | P2 | Auto default and lazy derived facts landed |
| 305 | Dropped BulkWriter loses committed rows | Fixed on `main` | was P0 in 4.1.1 | Regression test landed; release |
| 308 | EXPLAIN RULE doesn't expand recursion | Fixed on `main` (#303) | was P2 | Regression test landed; release |
| 309 | Tuple IS / prefix-TO, third KEY | Fixed on `main` (#303) | was P2 | Regression test landed; release |
| 310 | FTS scores constant 0.5 before index build | Declared fixed; schemaless fixed locally | P2 | Regular BM25 regression coverage; see follow-up verification |
| 313 | Exact probability loses event identity | Fixed on `main` (lineage store) | was P1 | 8-case regression test landed; release |
| 311 | In-tx BTree read after SET misses/repeats rows | **Not reproduced** on `main` or 4.1.1 | P3, needs info | Open: ask the reporter for the exact transaction |
| 315 | Probability diagnostics, exact/fallback status | **Fixed** | P2 | Exact/fallback diagnostics and warning timing landed |
| 317 | Business-action schemas for ABDUCE | Feature | P3 (L) | Open; workaround: ASSUME per action |

At triage time six of the fifteen were already fixed on `main` but
unreleased: #305, #308, #309, #310 (declared case), #313, and #317's noisy
candidates.

## New defects found during triage (not in any issue)

Status: 1-7 fixed in the completed P1/P2 batch; the stale documentation
in item 8 has also been corrected. The details below retain the original
measurements and proposed mechanisms as historical triage, not current blockers.

1. **A fork becomes unflushable** (found during #306, P1, arguably P0).
   - A fork-only schemaless edge type keeps its id→name mapping only in that
     fork's in-memory schema, which is never saved.
   - After the fork's last session drops, the next session's `fork.flush()`
     fails with `Internal(Edge type ID 2147483649 not found)`. Every
     `promote_from_fork` flushes first, so it fails the same way.
   - A 2×2 repro isolates the trigger: fork-only type × reopened session.
   - Deserves its own issue.
2. **Typed Int64 `sum()` wraps silently** (found during #316, P1).
   - `sum` over a declared `Int64` property holding `9223372036854775807` and
     `1` returns `-9223372036854775808`, both unflushed and flushed. That was
     measured through the Python wheel.
   - It goes through DataFusion's `sum`, which uses `add_wrapping`. The
     schemaless path (`CypherSum`) instead falls back to Float.
3. **`promote_from_fork` refuses schemaless labels** with `LabelNotFound`
   (found during #306).
4. **`TxQueryBuilder::max_memory(..).execute()` ignores the limit.** Only
   `fetch_all` and `profile` honour it. Found by code reading during #312.
5. **`TxQueryBuilder::timeout()` cannot raise the limit above
   `UniConfig.query_timeout`.** Measured during #311: a 1800 s builder timeout
   still failed at 30 s.
6. **PROFILE reports no memory:** `peak_memory_bytes` is hard-coded to 0. Found
   by code reading during #312.
7. **In-tx `SET` is about 30× slower under back-to-back flushes** (measured
   during #311). Results stay correct; the cause is not analysed.
8. **Stale docs:**
   - `locy.pest:438` says QUERY uses SLG resolution. It does not.
   - The ABDUCE docs promise "the minimal set of changes"; only single edits
     are validated.
   - `typecheck.rs:1707` describes the model placeholder as a literal; the
     code uses a variable.

## Per-issue notes

### #304: declaring a populated label hides it

**Reproduces only for nodes flushed before the declaration.**

- A flush writes a vertex into `vertices_{label}` only if the label is
  declared (`writer.rs:6261-6280`). Earlier schemaless rows therefore live only
  in the main `vertices` table.
- Declaring the label migrates nothing (`schema.rs:1832`, `api/schema.rs:161`).
- After declaration the planner switches from `ScanMainByLabels` to `Scan`,
  which reads only the empty per-label table (`planner.rs:8441` vs `:8483`).
- Unlabelled scans still find the data. The problem persists across reopen.

**Fix:** backfill from the main table in `apply()`. Storage must be written
before the catalog changes, the #249 rule.

**Same class (inferred):** `existing_row_count` (`manager.rs:1315`) also
ignores the main table, which defeats #249's NOT NULL relaxation.

### #306: fork diff and schemaless

- `crates/uni-fork/src/diff.rs:50-62` builds the label and edge-type sets from
  `schema().labels` and `schema().edge_types` only. Schemaless types are never
  scanned.
- The promote gate (`fork_admin.rs:542-558`) has the same blind spot.
- `edges_skipped` (`diff.rs:1132`) under-counts (inferred).
- `fork_candidate_dataset_names` (`fork.rs:549`) may not branch schemaless
  edge datasets. Candidate, unverified.

### #312: property fan-out in write joins

- `mark_pattern_variables` (`planner.rs:13029`) marks every CREATE/MERGE
  pattern variable `"*"`, so every property, plus `_all_props`, rides every
  join. Three eager barriers then each charge it to the memory pool.
- **Measured:** about 2.4 MB per row with a 200 KB property. Fails at 1 GiB
  around 430 rows. Read-only queries are unaffected.
- **Corrections to the issue:** cost is linear in N×S, not quadratic. Read
  traversals are not affected.
- **Fix:** `STRUCT_ONLY_SENTINEL` for bound endpoints.
- **General form:** late materialization, related to #184.

### #314: classifier relation through IS

**A, hidden column leak.** `__feat_*` hidden yield columns are stored in the
fixpoint's facts. They are stripped only on the user-row path
(`locy_eval.rs:710`).

**B, wrong consumer type.** The model output yield is typed LargeUtf8 by the
`infer_expr_type` catch-all (`locy_planner.rs:845`), while the executor
produces Float64.

**Error site:** `DerivedScanExec` re-stamps the schema positionally
(`locy_fixpoint.rs:5434`).

### #316: integer SUM

- `FOLD SUM` and `MSUM` are always f64 (`locy_aggregates.rs:392-398`,
  `:1146-1164`), and the schemaless path casts its exact integer to float
  (`:546-554`).
- Cypher `sum()` and Locy `QUERY … RETURN sum()` keep integers, so the
  languages disagree, despite the code's own "same meaning in both languages"
  comment.
- **Fix, step 1 (S):** type-preserving FOLD SUM. This is a behaviour change:
  Python gets `int`.
- **Fix, step 2 (M):** a checked or exact aggregate.

### #311: not reproduced

Agent built an 18-arm deterministic matrix: base {flushed, unflushed, half} ×
mid-tx flush × index {none, BTree, Hash}. It also ran concurrent-flush stress
up to N=20 000 and 36 re-key cycles. All exact, on both `main` and 4.1.1.

**Untested shapes:**
- concurrent writers from another session;
- multi-label nodes;
- MERGE or `SET +=`;
- streaming reads inside the transaction.

### #307 (perf), #302 (deps), #315/#317 (features)

**#307.** Magic sets (`goal_directed: On`) gives 53 ms vs 1552 ms at n = 16k,
with identical answers. It is off by default because it breaks the `.derived`
full-relation contract.

**#302.** Candle is used only by `CandleLinearClassifier`, which is a dot
product plus a sigmoid. Removing it saves 45 crates in minimal builds, and also
in the slim ONNX wheels.

**#315.** Blocked on #313 shipping. The `shared_probabilistic_dependency`
warning fires even when exact mode resolved the dependency.

**#317.** Additive language feature (L).
