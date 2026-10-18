# Open issues plan (2026-10-17)

Plan for the items left open after #300 (`3c1ce405a`..`0a12624a8`), #301
(`2c87ba7d1`) and the CDC checkpoint fix (`011d55e49`). All of those are on
`md/main` (`3f6a7e414`).

Every item states what was **measured** and what is **inferred**. Nothing
below is a finding until a test confirms it. Each fix ships with a test that
fails with the fix reverted.

## Summary

| ID | Item | Kind | Evidence | Size | Decision needed |
|---|---|---|---|---|---|
| W1 | Background compaction outlives `Drop for Uni` | Lifecycle / possible corruption | Measured (panic 5/5); two-compactor overlap inferred | M | — |
| W2 | Default `memory` CDC provider retains every commit | Unbounded memory | Code read; growth not measured | S | **Yes** (D1) |
| W3 | In-memory load `UNWIND … MATCH {uid} … CREATE` super-linear | Perf | Measured (8.7 s at 4k, 220 s at 16k, >900 s at 40k); cause unknown | ? | — |
| P1 | A9: constant on the left (`$p = n.uid`) skips the index | Perf | Measured 2k→8k rows vs 1 | S | — |
| P2 | A8: shortestPath with unbound / filtered target is ≈N² | Perf | Measured 6.1 s → 83 s | L | — |
| P3 | A3: every Locy rule is hydrated, returned or not | Perf | Measured +1.94 s on an 18 ms fixpoint | M | — |
| P4 | A1b + A2: IS-ref anchoring on the target side and cross-stratum | Perf | Measured 2N rows/iteration; 2k→8k rows | M | — |
| P5 | R2 + R3 + R5: BEST BY delta, non-linear / mutual Δ⋈F, IS NOT rebuild | Perf | Measured O(L²) / ∝facts per iteration | L | — |
| P6 | F1: range / STARTS WITH / IS NULL bypass scalar indexes | Perf | Measured 2k→8k rows vs 1 | M | — |
| P7 | A5: VLP property maps pre-scan the whole edge type | Perf | Wall + code (counter blind) | M | — |
| P8 | A1: Locy QUERY WHERE derives the whole fixpoint (no goal pushdown) | Perf | Measured 76k facts for 19 rows | XL | **Yes** (D2) |
| P9 | A7, A6, A4, R1, A10 | Perf (lower rank) | Measured (A10 not probed) | — | R1: **Yes** (D3) |
| O1 | Profiler blind spots | Observability | Measured (pre-scans, anti-join, dedup invisible) | S–M | — |

## Decisions needed

Decided 2026-10-17: **D1 → (a)**, the memory provider is not started by
default. **D2 → deferred**: P8 waits until the other perf items land. **D3 →
investigate first**: check the spec, the TCK and the #293/#294 identity work
for what relies on per-path identity, then recommend.

D3 finding: per-path identity is intended (#159). It is pinned by
`locy_issue_159_recursive_fold_dedup.rs:242` (bill-of-materials totals 9),
`locy_facts_and_filters.rs:304` (downstream SUM reads 4.0) and the oracle's bag
model (`uni-locy-oracle/src/ir.rs:284`). Merging equal facts would bring back
those silent under-counts. The defect was the docs: `complete_locy.md` said
whole-row dedup applies to every rule. Fixed by documenting the ALONG exception
and its cost (`complete_locy.md`, `website/docs/locy/advanced/along-fold-bestby.md`).
A per-fact multiplicity count would remove the R1 blowup without changing
semantics, but needs its own proposal and only pays off when equal-valued
paths are common.

- **D1, the memory CDC provider (W2).** Option (a), recommended: stop starting
  it by default and keep it as an opt-in test/authoring tool, which is what its
  rustdoc says it is. Option (b): keep it on by default and bound the buffer
  (ring of N batches or bytes). Option (a) also removes per-commit CDC work for
  every database that has no real consumer.
- **D2, goal-directed Locy evaluation (P8).** This is the largest change. It needs
  its own proposal: magic-sets rewrite vs. seeding the KEY binding into the
  base clauses only for linear rules, and interaction with FOLD, BEST BY,
  negation and PROB. The goal resolver was removed earlier (uni-locy
  `config.rs:75`); the proposal must state why it failed.
- **D3, ALONG facts per derivation path (R1).** Is one fact per path intended
  semantics (paths are distinguishable through ALONG values) or should equal
  `(KEY, value)` facts merge? If intended, document the exponential cost on
  DAGs and point users at BEST BY.

## Correctness and lifecycle first

### W1. Background compaction outlives its database

**Measured.** In a debug build, dropping a database and its directory while
compaction is scheduled panics 5/5 at `compaction.rs:764`. Keeping the
directory 5 s after drop: 2/5. Keeping it 30 s: 0/5. The panic is on a detached
task, so tests pass anyway. With the directory kept alive, no EID is missing
after compaction (data loss ruled out for the bulk-writer path).

**Mechanism.**
- `Drop for Uni` (`crates/uni/src/api/mod.rs:1199`) calls `shutdown_blocking`,
  which broadcasts and joins nothing (`uni-plugin-host/src/shutdown.rs:88`).
- The compaction loop (`uni-store/src/storage/manager.rs:1391-1422`) polls the
  shutdown signal only between ticks; a running `execute_compaction` holds
  `Arc<StorageManager>` and finishes.
- `exists_by_eid(..).unwrap_or(false)` (`compaction.rs:757`) turns an I/O error
  into "EID missing".

**Inferred, test first.** Drop and immediately reopen the same path (the perf
probe does this) can run two compactors on one directory, because
`CompactionGuard` is per instance. This is the one part that could corrupt data.

**Plan.**
1. A test for the two-compactor overlap: reopen during an in-flight compaction
   and assert either mutual exclusion or an intact store (edge count, property
   sums, adjacency consistency). Run it in a loop to bound the false-negative
   rate. If it reproduces, it becomes the priority.
2. Propagate the error in the check instead of `unwrap_or(false)`.
3. A cancellation token checked inside `execute_compaction` at safe points:
   before `adj_ds.replace` and before the delta clear, so a cancelled run leaves
   the delta authoritative.
4. A bounded join of tracked background tasks on drop, plus a documented
   `Uni::shutdown().await` for callers that can await. Make the perf probe
   and `Uni::temporary` use it.
5. If (1) reproduces: a cross-instance lock on the directory (lock file), not
   only the in-process guard.

**Tests.** Drop-while-compacting must produce no panic and no task alive
after shutdown returns (counted via the task tracker). The reopen test from
step 1. Revert check for each.

**Same family.** The CDC fix (`011d55e49`) was another background task on the
caller's runtime that blocked or outlived its owner. As part of W1, audit
every `tokio::spawn` / `track_task` in `uni`, `uni-store` and `uni-plugin-host`
for: blocking I/O inside, locks held across I/O, and survival past drop.

**W1 status: fixed, uncommitted.**
- **The overlap reproduced, with wrong answers.** The loop dropped A mid-pass,
  reopened as B, loaded data and flushed.

  | Run | A mid-pass at drop | Two compactors overlapped | Wrong answers in live B |
  |---|---|---|---|
  | Before the fix | 19/19 | 19/19 | 4/19 |
  | Control (wait for A before reopening) | — | 0/15 | 0/15 |
  | After the fix | 17/17 | 0/17 | 0/17 |

  - The wrong answer: the reverse-traversal sum came back 0 while the forward
    sum was correct.
  - A clean reopen was intact, so nothing was damaged on disk.
  - Cause (inferred): the other instance's whole-table overwrite.
- **Fix.**
  - Cancellation token checked at five safe points, each where a redo is a
    no-op.
  - The `:757` error is now propagated instead of read as "missing".
  - Drop cancels, signals, and joins tracked tasks for up to 5 s. On
    current_thread it only signals. `shutdown_in_place` cancels too.
  - Cross-instance file lock `<storage>/compaction.lock`, local stores only. A
    writable open waits up to 30 s for it, then fails.
  - The background loop is `biased` and skips missed ticks instead of
    bursting them.
  - CSR compaction moved to `spawn_blocking`.
  - The shutdown handle keeps unfinished tasks after a timeout.
- **Revert checks.** Six fixes, each red on its own test when reverted.
  - Not tested: the `:757` propagation (no I/O-error injection) and the cancel
    in `shutdown_in_place`.
- **Teeth patch.** `compaction_property_loss.patch` was regenerated with the
  same lines; it still fails its target test.
- **Suites.** uni-db 3267/3267, uni-store 764/764, plugin-host 75/75,
  failpoints 174/174.
- **Audit findings, not fixed (W1-followups):**
  - `triggers.rs:749`: plugin `fire` runs synchronously on an executor thread,
    untracked.
  - `scheduler.rs:345,543`: sidecar fsync under a parking_lot lock, plus a
    `block_on`, with an untracked completion task.
  - `cdc_runtime.rs:636`: the `streams` lock is held across the plugin's
    synchronous `deliver`.
  - `index_rebuild.rs:366`: each rebuild adds a worker and a handle that is
    never removed.
  - `index_rebuild.rs:482` and `writer.rs:6430`: detached spawns.
  - `trigger_async_compaction`: detached and dead.
  - Flush stream tasks: not tracked by shutdown.

### W2. Default memory CDC provider retains every commit

**Code read (not measured).** `MemoryCdcOutputProvider` is registered by
default (`uni-plugin-builtin/src/extras.rs:22`). Its stream pushes a clone of
every delivered `CdcBatch` into `MemoryCdcStream.delivered`, and nothing can
read it: the runtime holds the stream as `Box<dyn CdcStream>` and nothing
downcasts it. It is cleared only on shutdown.

**Plan.**
1. Measure first: a long-running write loop (e.g. 100k commits) with an RSS or
   allocator counter, with and without the provider, to size the problem.
2. Apply D1. For (a): confirm whether any test or binding relies on the
   default stream (grep for `"memory"` provider use, CDC e2e tests, Python
   bindings), and move those to explicit registration.
3. A test that a default database starts no CDC stream (a), or that the buffer
   stays within its bound after N commits (b).

**W2 status (2026-10-17): done, uncommitted.** Every registered CDC provider
gets a stream (`cdc_runtime.rs:493/:601`), and registration alone switches on
per-commit CDC extraction (`transaction.rs:1127`), so the fix is to stop
registering it by default: opt in with `MemoryCdcPlugin`
(`uni-plugin-builtin/src/extras.rs`). The captured batches are now readable
(`delivered()`). **Breaking:** `MemoryCdcOutputProvider` is built with
`::new()`, and `MemoryCdcStream.delivered` is now `Arc<Mutex<Vec<CdcBatch>>>`.
RSS over 20k commits: +236.6 MB before, +55.8 MB after. **Open:** the remaining
~2.9 KB per commit on the default path is unexplained. It may be L0 growth
(20k unflushed nodes) or another retention; measure it with a flush in the
loop.

### W3. In-memory load via UNWIND + MATCH-by-uid + CREATE is super-linear

**Measured (root-caused 2026-10-17; probe `crates/uni/tests/common/perf/l0_load_probe.rs`).**
Two independent defects:

- **W3a, a string-keyed correlated MATCH becomes a cross join.**
  `UNWIND $rows AS r MATCH (a:E {uid: r.a})` plans as a `CrossJoin` + residual
  filter (`planner.rs:7209/7226`). `try_plan_cross_join_as_hash_join` does
  classify the equality (`equi_pairs=1`), but bails at `df_planner.rs:4711`
  because `unify_join_key_types` (`df_planner.rs:9753`) returns `None` for Utf8
  against LargeBinary (the CypherValue `r.a`). CrossJoin rows: 502,000 at
  N=1k and 2,004,000 at N=2k (N×V). The read-only `RETURN count(*)` form is
  just as slow, so writes, SSI and L0 are not involved. Integer keys go through
  `tointeger` to a HashJoin and stay linear (0.17 → 0.22 ms per edge, vs
  1.15 → 8.70 for strings). `UNWIND $strings AS u MATCH (n {uid: u})` has the
  same defect.
- **W3b, L0 scans ignore property predicates.** On committed but unflushed
  vertices, a single `MATCH (a:E {uid:$s})` reports `rows_scanned = V` per
  side and `index_scans = 0`; after a flush it is 1 and 1. The L0 side builds
  every L0 row of the label (`L0VertexTargets::All`, `columnar_scan.rs:2492`,
  built at `:1784`) before filtering. A loop of single-row writes is O(N·V)
  until the next flush. This one affects ordinary users.

**Plan.**
1. W3a: add Utf8/LargeUtf8 ↔ LargeBinary to `unify_join_key_types`, decoding
   the CypherValue side to a string key and NULL for non-strings. That matches
   `cypher_eq`, since a string never equals a non-string. Check the numeric
   cross-type cases the existing `tointeger` path covers against `cypher_eq`
   (`1 = 1.0` is true) while there. Differential test: hash-join answer vs
   the residual-filter answer over mixed-type keys, NULLs and duplicates.
2. W3b: push equality predicates into the L0 vertex batch build (filter before
   materializing). For indexed properties, consider an L0 equality index.
   Guard: `rows_scanned` of 1 per side on unflushed vertices.
3. Guard for W3a: at 1k and 4k, no `CrossJoinExec` in the loader's profile,
   and the largest operator `actual_rows` is at most 2N.

**W3a status: fixed, uncommitted.**
- **Design.** `unify_join_key_types` now picks the join key by dtype:
  - pairs where Arrow equality already matches Cypher equality: keys used as
    they are;
  - two integer types: cast to Int64;
  - Utf8 against LargeUtf8: cast to LargeUtf8;
  - CypherValue, and float against int: the new `_cypher_join_key` UDF
    (`df_udfs/join_key.rs`). Values `cypher_eq` calls equal always get the same
    key. Above 2^53 int/float equality isn't transitive, so different values
    can also share a key, and every matched pair is re-checked with the original
    predicate as a `JoinFilter`. `VidLookupJoinExec` is skipped when a re-check
    is needed.
- **Silent wrong answers in the old mixed-type path (all fixed, each a repro in
  `bugs/mixed_type_hash_join.rs`):**
  1. An int column against a CypherValue matched `1.5` and `'1'` to `1`; a bool
     or list key aborted the query.
  2. A float column against a CypherValue matched `1.5` to `1.0`, and `'1'`.
  3. CypherValue against CypherValue compared raw bytes, so it missed `1` vs
     `1.0`.
  4. Native Int64 against Float64 matched `1` to `1.5`.
  5. `VidLookupJoinExec` returned 0 rows instead of 2 for mixed-type extra pairs.
- **Counters at N=1k.**
  - Correlated MATCH: CrossJoin of 502,000 rows → HashJoin of 1,000; 415 → 88 ms.
  - Loader: 999 → 202 ms.
  - At 4k the largest operator holds 4,000 rows and there is no CrossJoin.
- **Revert checks.**
  - Old logic restored: 5/5 tests fail.
  - JoinFilter dropped: 3 fail, from the 2^53 collision.
  - VidLookupJoin guard removed: 1 fails.
- **Suites.** uni-query 1170/1170, uni-db 3267/3267, Cypher TCK both modes and
  Locy TCK 530/530 all pass.

**W3b status: fixed, uncommitted.**
- **Design.** A one-way L0 prefilter. `l0_equality_prefilter` (`scan.rs`) reads
  `=`, `IN`, `AND` and single-column `OR` back out of the #57 runtime filter.
  `l0_equality_candidates` (`columnar_scan.rs`) skips an L0 row only when its
  merged value is missing, NULL, or converts to the column type unchanged and
  differs. Every vid the flushed side returned is kept, so the newer L0 version
  still shadows the stale L1 row.
- **Rejected alternative.** An L0 value index was not built, because L0 property
  maps are written from several sites outside `L0Buffer`.
- **Counters.**
  - Unflushed lookup: V → ≤ 1 at 2k and 8k.
  - Split L1/L0: 2001 → ≤ 2.
  - Write loop worst statement: 2199 → ≤ 2.
- **Tests.** `index/l0_equality_prefilter.rs` is a shadowing differential
  against an unpushed reference. It covers updates away from, onto and to an
  existing key, delete, recreate, REMOVE label, relabel, NULL, `IN`, `AND`, and
  an int column with a float literal. Each runs flushed, unflushed, mixed,
  in-transaction, after rollback and in a fork.
- **Revert checks.** With the prefilter off, the counter guards fail. With the
  shadow set emptied, the differential fails and returns a stale row.
- **Suites.** uni-store 764/764, uni-db 3267/3267, Cypher TCK both modes and
  Locy TCK 530/530 all pass.
- **Open.**
  - The schemaless (label-less) scan path is not covered.
  - The candidate pass still walks the label's L0 members with one hash probe
    each. Rows are no longer built, but the walk is O(V) (inferred, not timed).

## Performance

Order by impact × frequency × cost. Each item gets a counter guard at two
scales (the `perf_class_probe` fixtures) that fails with the fix reverted.
Promote the probe assertions for the fixed items out of `#[ignore]`.

### P1. A9: commuted equality skips the index (small, do first)

`$p = n.uid` and `'u42' = n.uid` scan every row; `n.uid = $p` reads 1.
Normalize `literal/param = property` to `property = literal/param` before
index matching, in the same place that recognizes `prop = value` for
pushdown. Also cover `IN` with the list on the left if the matcher has the same
asymmetry, and every other comparison operator (`<` flips to `>`) so P6 inherits it.
Guard: both forms read 1 row, `index_scans = 1`.

### P2. A8: shortestPath with an unbound or filtered target

**Measured.** `shortestPath((a {uid})-[:R*]->(b:N)) WHERE b.x < 20`: source ×
every target is cross-joined, then one BFS per row
(`shortest_path.rs:819-826`), with the target filter applied after the search.
6.1 s → 83 s for 2k → 8k nodes; a bound target stays at 11 ms.

**Plan.**
1. Push target predicates into the target scan before the cross join, so `B`
   is the filtered set (`b.x < 20` gives 20 targets, not N).
2. Group rows by source and run one single-source BFS per distinct source
   that stops when every target in the group is settled. Cost becomes O(sources ×
   reachable subgraph), not O(|B| × BFS).
3. Bidirectional BFS for the single bound-pair case (the doc claims it today;
   make the code match or correct the doc).
4. Preserve semantics: one shortest path per (a, b) pair, tie-breaking as
   today, `allShortestPaths` unchanged unless it shares the defect (probe it).

### P3. A3: hydration of rules that are not returned

`enrich_vids_with_nodes` (`crates/uni/src/api/impl_locy.rs:2108`) hydrates every
derived rule, returned or not, via `MATCH (entity) WHERE id(entity) IN [<all
ids>]` (`impl_locy.rs:1915`). +1.94 s for 8k unreturned facts against an 18 ms
fixpoint, growing 7.2x for 4x N.

**Plan.**
1. Hydrate only what a command's result exposes: QUERY / RETURN projections,
   DERIVE outputs, EXPLAIN and ABDUCE targets. Keep the public contract of
   `derived` rule results; if an API returns every rule's facts, hydrate those
   lazily or on access.
2. Replace the `IN [<N literals>]` query with a vid lookup (a batch
   `get_batch_vertex_props` style call or the `VidLookupJoinExec` path).
   Verify the candidate O(N²) explanation (IN list evaluated per scanned row)
   with a counter before and after.
3. Watch the #298 / `locy_vid_hydration_timing_dependent_encoding` contract:
   KEY columns must keep a stable encoding (`Value::Node`).

### P4. A1b + A2: extend #301 anchoring to target-side and lower-stratum refs

- **A1b.** `(a)-[:T]->(m) WHERE m IS r TO b`: the IS-ref binds the *target*
  of the walk, so the #301 anchor (source side only) does not apply, and every
  iteration scans 2N rows. Extend `choose_anchor` to accept a KEY position on
  any plain MATCH node, and plan the walk from the anchor in the reverse
  direction (the planner already has bound-target / reversal support for
  Cypher, #219/#224).
- **A2.** A non-recursive clause referencing a lower-stratum rule does a full
  label scan and traverse, then a hash join with the (already final) facts.
  The lower stratum's facts are complete before this stratum runs, so their
  size is known: anchor when `|facts|` is below a threshold relative to the
  label count (a cheap cardinality rule, not a cost model).
- Keep the #300 binding-plan invariants: anchoring is a planning decision
  recorded in `ClauseBindingPlan`, never a change in which variable binds.
- Close the #301 leftover too: a `BindNode` position on a ref still joins a
  full `ScanAll` per iteration (non-linear TC does two); join via
  `Property(var, _vid)` so it can use the vid lookup.

### P5. Recursion: R2 BEST BY, R3 non-linear and mutual, R5 IS NOT

- **R2.** `merge_best_by` sets `delta = facts.clone()` on any change
  (`locy_fixpoint.rs:1607`). Set delta to the rows that were added or improved
  (the code already computes `old_best` for change detection). Measured:
  DerivedScan Σ1..L (O(L²)) vs L.
- **R3 non-linear.** Both refs read full facts every iteration. Semi-naive
  rewrite: evaluate `Δ⋈F_old ∪ F⋈Δ` (one clause copy per ref position, each
  with that ref reading delta and the others full). Measured: about 1M join rows
  in the final delta-0 iteration at 8k.
- **R3 mutual.** A cross-reference inside a mutually recursive stratum reads
  the other rule's full relation. Give each rule's cross-ref the other rule's
  delta, keeping full facts where a clause is non-linear.
- **R5.** `apply_anti_join_composite` (`locy_complement.rs:395-402`) rebuilds
  the banned `HashSet` every iteration per clause. Negated rules are in a
  lower stratum (stratification), so the set is fixed for the whole stratum:
  build it once per stratum and reuse it.
- The oracle (`uni-locy-oracle`) differential and the topo relations are the
  correctness net for all four. Add random-program families that force
  non-linear and mutual recursion with BEST BY, and run the 500-case soak.

### P6. F1: range, prefix and null predicates bypass scalar indexes

Only `=` and `IN` on an Online Hash/BTree index reach Lance. Ranges,
BETWEEN, STARTS WITH and IS NULL become a full scan plus FilterExec.

**Plan.** Translate these into `FilterExpr` (the structured backend filter,
see the backend-portability work) so a BTree index serves ranges, prefixes as
ranges, and IS NULL. Keep the residual `FilterExec` above it for exact
semantics: Cypher three-valued logic, type coercion (int vs float bounds),
string collation. A data-independent pushed predicate was a live wrong answer
once (Lance `"col"` is a string literal), so every translated form gets a
differential test against the unpushed plan over mixed-type, NULL and
boundary rows, flushed and in L0.

### P7. A5: VLP property maps pre-scan the whole edge type

`build_edge_property_filter` (`traverse.rs:3645`) and
`build_accepting_vid_filter` (`traverse.rs:3561`) enumerate every edge or
target of the type before an anchored walk. Evaluate the map per edge while
walking (fetch the edge's properties for visited edges only), or bound the
pre-scan to the reachable frontier. Count the pre-scan in `rows_scanned` first
(O1), so the guard can see it.

### P8. A1: goal-directed Locy evaluation

Blocked on D2: a separate proposal first. Measured: `QUERY reach WHERE
a.uid='c0_0'` derives 76k facts for a 19-row answer, 75x the anchored
form at N=8k.

### P9. Lower-ranked confirmed items

- **A7.** Anchored disjoint patterns: a bound value from `a` is not passed
  sideways into `b`'s indexed lookup; the planner hash-joins two scans. Fix
  candidate: rewrite `b.uid = a.name` with `a` single-row-bound into a
  dependent index lookup (sideways information passing).
- **A6.** Named path anchored on the far end disables anchor reversal. Allow
  reversal and reverse the path value afterwards.
- **A4.** ASSUME re-derives every stratum (about 2x the program). Incremental
  re-evaluation from the changed facts. Large; rank stays low.
- **R1.** Pending D3.
- **A10.** `{ext_id: …}` pushdown on labelled nodes: write the probe first.
- **R4, R6.** Not scaling costs today (R4 flat at 8k facts; R6 a ~1.3 ms
  per-iteration floor). Revisit only if a larger fixture shows growth.

## O1. Profiler blind spots

The probes could not see several costs. Fixing these makes the guards above
possible and stops hiding the next regression.
- VLP property-map pre-scans (`traverse.rs:3561/3645`) do not count toward
  `rows_scanned`.
- The IS NOT anti-join and the row dedup are not operators in the Locy profile
  tree.
- No planning-time counter (R6).
- An ASSUME run's `rows()` read 0 (first command result is not a Query);
  check whether that is an API trap for users.

**O1 status: implemented, uncommitted.**
- **VLP pre-scans are counted.** `main_edge.rs` `count_scanned_rows`, and the
  L0 overlay in `traverse.rs` `build_edge_adjacency_map`.
  - VLP `{w:1}` map, `rows_scanned` at N=200/800: 1/1 → 401/1,601.
  - The no-map control stays 1/1.
- **The Locy profile tree shows the anti-join and dedup.**
  - `LocyIsNotAntiJoin` / `LocyIsNotComplement` come from the new shared
    `locy_complement::apply_negated_is_refs`, which de-duplicates two copies.
  - `LocyFactDedup` / `LocyBestByMerge` / `LocyFoldMerge` are recorded per rule
    per iteration.
- **Planning time is counted.**
  - `QueryCounters.{physical_plans, planning_nanos}` feed
    `QueryMetrics`, `ProfileOutput.{logical,physical}_planning_ms` and
    `LocyIterationProfile.{planning_ms, plans}`.
  - Measured: about 0.29 ms of re-planning per Locy iteration, out of R6's
    about 1.3 ms floor.
- **Tests.** `perf/profiler_blind_spots.rs` has 6 tests at two scales. All
  were red before the fix, and each of 7 reverts goes red.
- **Docs.** Black Book "What the profiler counts". Fixed the Black Book
  PROFILE example and the website Rust profile example, which were both wrong.
- **ASSUME `rows()` trap (decision needed).**
  - Rust `LocyResult::rows()` returns only the first `Query`. For an ASSUME
    program it returns `None`, even though `command_results()` holds
    `Assume(3 rows)`.
  - Python `rows` returns the first Query, Assume or Cypher result (from
    reading the code, not run).
  - So the two languages disagree, and Python also contradicts the docs. A
    repro test pins the Rust behavior.
- **Open.**
  - The Python bindings lack the new metrics.
  - Locy program-level logical planning is not timed (`impl_locy.rs` step 1).
  - There is no per-operator rows-scanned field.
  - Property point-reads are not counted.

**ASSUME `rows()` decision (2026-10-17): first row-producing result.**
- Rust `LocyResult::rows()` now returns the first `Query`, `Assume` or `Cypher`
  result, which is what Python `rows` already did.
- A program with a Cypher statement before its QUERY now returns the Cypher rows
  in Rust, as it already did in Python.
- Updated: the `profiler_blind_spots.rs` guard `assume_rows_are_returned_by_rows`,
  plus the website Rust, Python and Python-reference docs.
- uni-locy 197/197 and the uni-db locy/assume/profiler filter 378/378 pass.

**P3 status: implemented, uncommitted.**
- **Contract.** `LocyResult.derived` is public and documented, and Python
  converts it eagerly, so every rule stays hydrated. Making it lazy would be a
  breaking change and was not done.
- **The O(N²) cause, verified.**
  - The residual `_cypher_in` filter scans the whole literal list per row:
    `FilterExec` took 38.6 → 585.8 ms (15.2x for 4x N).
  - Every rule was also hydrated twice, giving `rows_scanned` of 4N+2.
- **Fix (in `impl_locy.rs`).**
  - `lookup_vertices`: a VID-pushed scan that also records reads into the SSI
    read set.
  - One union lookup per kind.
  - The returned map reuses the command-store hydration unless the program has
    a DERIVE.
- **Measured (debug, flushed), one unreturned rule.**

  | Facts | Before | After |
  |---|---|---|
  | 2k | 268.7 ms | 52.4 ms |
  | 8k | 1919.7 ms | 174.8 ms |

  Growth for 4x N went from 7.1x to 3.3x. L0 data gives the same numbers.
- **Tests.** `locy/locy_entity_hydration.rs`:
  - a count guard and a timing guard, each red on the revert of its fix;
  - value equality against Cypher across L0, transactions, deletes, forks,
    QUERY, EXPLAIN, ASSUME, ABDUCE and DERIVE.
- **Open.**
  - Edge hydration still uses `MATCH ()-[e]->() WHERE id(e) IN [...]`, which
    has no EID pushdown: 1.15 s at 8k EIDs. It needs a main-edges lookup by EID.
  - DERIVE's `lookup_derived_enriched` uses the base context, not the ASSUME
    overlay. This was not investigated.

**P4 status: implemented, uncommitted.**
- **A1b.** Anchoring on the target side already worked after #301. The
  audit's "2N" was the BindNode `ScanAll` of the bound node.
- **BindNode.** The join key changed to `Property(var, "_vid")` so it can use
  `VidLookupJoinExec`.
- **A2.** `Anchor.source = LowerStratum`: a KEY `TestNode` on a walkable MATCH
  node is seeded by `Distinct(Project(LocyDerivedScan))`, a pure semi-join. The
  reference is still joined in full with every test. The candidate is
  suppressed when another node carries an equality.
- **No plan-time threshold (accepted).**
  - The seed is a subset of the label, so the walk never starts from more nodes
    than the unanchored plan.
  - The extra cost is O(|facts|).
  - At run time, `VidLookupJoinExec` switches to one full pass when distinct
    vids × 4 ≥ label rows.
  - Worst case, every vertex a fact: 49.7 vs 51.1 ms.
- **Counters per iteration (flushed, 2k / 8k noise).**

  | Case | Before | After |
  |---|---|---|
  | A1b bound-node / BindNode, GraphScan | 1,012 / 4,012 | 2 / 2 |
  | A2 one fact, scan / traverse | 1,011 / 2,008 and 4,011 / 8,008 | 1 / 1 |
  | Non-linear TC, GraphScan | 2,030 / 8,030 | 23 / 23 |
  | All-pairs TC total (full-pass arm) | ~76k / ~304k | 50,100 / 200,400 |

- **Tests.** `locy/locy_anchor_target_cross_stratum.rs`:
  - exact-count guards, each red on revert;
  - a brute-force differential over directions, multi-hop, non-linear, two
    lower-stratum references, FOLD with a shared anchor, and ALONG + BEST BY;
  - 5 SSI conflict programs.
- **Changed test.** `issue_301_no_anchor_without_a_walkable_recursive_reference`
  asserted the old "lower stratum never anchors" rule. It is now a positive
  test, and its path-variable half is kept.
- **Suites.** uni-query 865/865, uni-locy + oracle 241/241, `^locy::`
  333/333.
- **Open.**
  - No test isolates the `ReadSetRecordingExec` look-through: traversal's
    adjacency recording masks it.
  - A pure edge insert from the anchor does not conflict, per the item-level
    `OccReadSet` contract (phantoms out of scope).
  - Lower-stratum anchoring considers KEY positions only.

**P2 status: implemented, uncommitted. The search counter is now
per query (`QueryCounters`/`QueryMetrics::shortest_path_searches`); the static is removed and the revert is red again.**
- **Fix.**
  - `shortest_path.rs`: one on-demand search per source (`SourceBfs` /
    `SourceLayers`), reused across rows and batches with the same source, and
    grown only until the target is settled.
  - `allShortestPaths` had the same defect and is fixed the same way.
  - `planner.rs` pushes an endpoint-only WHERE conjunct into that endpoint's
    scan, except inside OPTIONAL MATCH.
  - The "bidirectional" claim in the module doc was false and is corrected.
- **Measured.**

  | Query | Before | After |
  |---|---|---|
  | `b.x < 20`, 2k nodes | 6.1 s | 13.5 ms |
  | `b.x < 20`, 8k nodes | 83 s | 15.8 ms |
  | Unbound `shortestPath`, 4k | 23 s | ~0.8 s |
  | Unbound `allShortestPaths`, 4k | 36–58 s | 3.0–3.5 s |

  Rows into the path operator for `b.x < 20`: 2,000 → 20.
- **Tests.** `cypher_path/shortest_path_many_targets.rs`:
  - a BFS oracle over 12 random graphs (flushed and L0, self-loops, parallel
    edges, directions, type alternation, hop bounds, maps), plus OPTIONAL and
    fork variants;
  - a pushdown row guard and a search-count guard.
  - Three reverts each go red.
- **Open.**
  - Output building now dominates: `find_edge` runs twice per hop, and
    `build_output_batch` takes 0.5–2.2 s at 2k.
  - Tie-breaking differs from run to run, because
    `AdjacencyManager::get_neighbors` builds a fresh `HashMap` per call. This
    was true before the change too; tests accept any shortest path.
  - The target range filter is still a `FilterExec` over the full scan (P6).

### W4. OPTIONAL MATCH WHERE pushed into the mandatory clause (new, from P2; agent running)

Reported repro: `MATCH (a:N) WHERE a.x < 4 OPTIONAL MATCH (a)-[:R]->(b:N) WHERE
a.x < 3 RETURN a.id, b.id` drops the x=3 row instead of returning `b = NULL`.
`plan_where_clause` step 3 pushes conjuncts on earlier-bound variables into the
earlier clause's scan. This is a silent wrong answer and takes priority over the
remaining perf items. The plan: verify, map every pushdown site, apply one rule
(an OPTIONAL WHERE never filters the mandatory input), add a differential
oracle, and run both Cypher TCK modes.

**W4 status: fixed, uncommitted.**
- **Reproduced.** Red in all 5 variants (schemaless and indexed, flushed and
  unflushed, batch size 1).
- **Class map (measured).**
  - Step 3 pushdown of a conjunct on an earlier-bound variable (indexed mode).
  - Chained OPTIONALs pushed into the previous OPTIONAL's traverse target.
  - The KNN rewrite drops the scan's `optional` flag.
  - An inline label or property map on a bound node became a plain `Filter`
    (`planner.rs:5813`; the shortestPath endpoint at ~4925).
  - An OPTIONAL clause that creates no variables was planned as mandatory.
  - **Not exposed (measured):** step 2, step 4 `Apply`, UNWIND, EXISTS, a
    bound relationship, the IN-list merge, OPTIONAL inside `CALL{}`.
- **Fix.**
  - `WhereScope::{Mandatory, Optional{created}}` (`planner.rs:2661`). Under
    `Optional`, no pushdown step runs, and the whole WHERE becomes one
    `OptionalFilterExec` over the created variables.
  - `hoist_bound_node_constraints`: inline constraints on bound nodes move into
    the WHERE.
  - `name_relationships_between_bound_nodes`: hidden names, so a clause whose
    ends are all bound can still null its relationship.
  - P2's per-site gate is folded into this rule.
- **Tests.** `bugs/optional_where_bound_var_pushdown.rs`, about 40 cases
  against a `MATCH ⊎ NOT EXISTS` oracle.
  - Full revert: 25 indexed and 8 schemaless failures.
  - Reverting the relationship naming alone: 3 failures.
- **Suites.** uni-query 865/865; targeted uni-db 932/932 and 1199/1199;
  Cypher TCK both modes 3925/3926 with 0 failed.
- **Follow-up (narrowed rule).** The combined run found that the first rule
  blocked every pushdown: `OPTIONAL MATCH (b:N) WHERE b.uid='u42'` read 4003
  rows with 0 index scans.
  - Now a conjunct whose only variable is created by the clause may be pushed
    into that variable's own `optional` scan (`optional_scan_reachable`).
  - The KNN rewrite, label disjunction, traverse `target_filter` and `Apply`
    input stay off under Optional.
  - Measured: 3 rows and 1 index scan at 1k and 4k.
  - Counter guard `optional_where_bound_var_pushdown_counters_{1000,4000}` fails
    on the over-broad gate.
  - Suites: oracle green, uni-db filtered 2035/2035, TCK both modes 0 failed.
- **Open.** `VectorKnn` has no optional flag, so an OPTIONAL KNN stays
  unrewritten.
- **Not examined:** the QPP outer-source filter sites (`planner.rs:~6037/6056`).

### W5. Defects found by the W4 oracle (repros exist, unfixed)
1. **Silent wrong answer: the KNN threshold is ignored.** With a Flat cosine
   index, `WHERE vector_similarity(d.emb,[0.0,1.0]) > 0.5` returns all 6 rows,
   including 3 at similarity 0.0. The plan is `VectorKnn{threshold:0.5,k:100}`.
2. **Silent wrong answer:** `vector_similarity` on an outer variable inside
   EXISTS returns false for a matching `a`.
3. **Silent wrong answer (schemaless only):** `CALL { WITH a WITH * WHERE NOT
   EXISTS {…} RETURN 1 AS b }` returns no rows; it should return a3, a4 and a5.
4. **Errors on outer variables inside EXISTS:** `a:L` (`No field named
   "a._labels"`), `labels(a)`, and `shortestPath((a)-[:R*]->(c))`.
5. **Planning errors:**
   - Schemaless shortestPath fails with `Edge type R not found` (no
     `TraverseMainByType` fallback, `planner.rs:~4974`).
   - The KNN rewrite rejects a negative literal (`UnaryOp Neg`).

**W5 #1, #2, 5b status: fixed, uncommitted (vector agent).**
- **#1 root cause.** `extract_simple_vector_similarity` / `replace_scan_with_knn`
  rewrote `vector_similarity(...) OP t` into `VectorKnn{k:100}`.
  - The executor compared `t` against the index's native score: (1+cos)/2 for
    cosine, 1/(1+d) for L2, raw dot for dot.
  - It kept ties under a strict `>`.
  - It silently truncated at 100 rows.
  - Reproduced on all 8 indexed configs. `> 0.5` returned 100 rows where 4
    were correct.
- **#1 fix.** The rewrite is removed: a threshold is an exact per-row filter.
  Only `n.prop ~= <constant>` becomes `VectorKnn`, and the unused `threshold`
  field is gone. IVF/HNSW can't answer a threshold exactly, so it is never
  served from the index.
- **#2.** `arrow_to_json_value` (`unwind.rs`) had no `FixedSizeList` case, so an
  outer vector arrived NULL in EXISTS bodies. The fallback is now the canonical
  `arrow_to_value`.
- **5b.** `evaluate_simple_expr` handles unary minus, NOT, binary operators and
  scalar functions.
- **Found by the same oracle and fixed.**
  - `vector_distance` on NULL now returns NULL.
  - Stored-vs-stored `vector_similarity` was rounded to f32 (0.8 →
    0.800000011920929).
  - CALL `uni.vector.query` rejected a negative literal.
  - Multivector inline `vector_similarity` (it relied on the rewrite) is now
    computed per row as MaxSim.
- **Tests.** `vector_search/vector_predicate_exactness.rs`, 18 tests against a
  brute-force oracle. Six revert checks, each red.
- **Changed tests.**
  - `plugin_index_handle_dispatch.rs` asserted cosine-0.0 rows passing `> 0.0`.
  - `plan_shape/proofs.rs` asserted the threshold rewrite.
  - Both now use `~=`.
- **Perf consequence (user-visible).** A `vector_similarity` threshold is now a
  full label scan even with an HNSW index. The fast path is `~=` or
  `uni.vector.query`. Benchmarks doc annotated.
- **Open.**
  - `~=` still has a hardcoded `k: 100` and descends through `Limit`; its
    semantics there are undefined.
  - `arrow_to_json_value` still returns NULL, with a warning, on a decode error.

**W5 #3, #4, 5a status: fixed, uncommitted (subquery agent).**
- **Wider class than reported.** 185–188 of 396 matrix cases failed per mode.
  - Silent wrong answers: `properties(a).x` and `keys(a)` read NULL in EXISTS;
    nested EXISTS inside CALL; an outer `r` was ignored, so EXISTS was true on
    every row.
  - Every operation on an outer relationship errored.
  - **Live silent wrong answer:** `UNWIND ns AS a RETURN EXISTS{MATCH (a)-->(b)}`
    was always true, and COUNT returned 6 on every row.
  - Top-level `startNode(rel)` after `WITH r AS rel` errored or was silently
    NULL.
- **Root cause.** Subquery imports were single parameters (a VID or a map), not
  entity columns. #3: the CALL body's `a` was one CypherValue, so the inner
  EXISTS re-bound `a` as a fresh scan.
- **Fix.**
  - New `LogicalPlan::OuterRow` leaf. Node and relationship imports are planned
    from the outer row's own columns, wired into EXISTS, pattern comprehension
    and per-row Apply.
  - `._eid` detection and typed body scope.
  - UNWIND-bound entities are recognised by value.
  - startNode/endNode endpoint hydration.
  - shortestPath falls back to the schemaless registry.
- **Tests.** `bugs/outer_entity_in_subquery.rs`: 34 operations × 15 contexts ×
  2 modes × 2 storages against a per-row top-level oracle. All 12 hunk reverts
  go red.
- **Suites.** uni-query + functions 1176/1176, uni-db filtered 2013/2013, TCK
  both modes 0 failed.
- **Decided 2026-10-17: drop, as Neo4j does (breaking). BUILT, uncommitted.** Mandatory MATCH drops no-path / NULL-endpoint / `a=b` under `*1..` pairs; OPTIONAL null-extends per entering row (fixed: an entering row with no candidate target was silently dropped, and duplicate entering rows merged); unnamed `MATCH shortestPath(...)` now plans. Tests `cypher_path/shortest_path_no_path.rs` (26 queries × 4), 5 reverts red; release note `docs/release_notes/RELEASE_NOTES_UNRELEASED.md` (version TBD). Was: A mandatory `MATCH p = shortestPath(...)` keeps no-path
  pairs as NULL rows. This is documented in the Black Book and pinned by tests;
  Neo4j drops them.
- **Open.**
  - The batched Apply path still uses parameter imports.
  - The `rewrite_query_correlated` property-to-parameter rewrite is now
    redundant.

### P10. Constant `IN` lists are O(rows × |list|) (new, from P3)

`_cypher_in` (`df_expr.rs:1028`) scans the list per row and has no hash-set path
for constant lists. This affects user queries with large literal `IN` lists, not
only hydration. Fix: precompute a set keyed by `_cypher_join_key` (W3a), and
re-check with `cypher_eq` on a key hit to keep 2^53 and NULL semantics.

## Sequencing

1. **W1** test for the two-compactor overlap (could be corruption), then the W1
   lifecycle fix and the background-task audit.
2. **W2** once D1 is decided (small).
3. **P1** (small, immediate win), then **O1**, so later guards can see what
   they measure.
4. **P2**, **P3**, **P4**: largest measured costs on common shapes.
5. **W3** investigation, in parallel with P2–P4 (independent code).
6. **P5**, behind the oracle soak.
7. **P6**, **P7**, then **P9**.
8. **P8** proposal (D2).

Each step: a test written first and run red, the fix, revert check, a
`profile()` counter guard at two scales, the relevant Locy / Cypher TCK lane,
and `docs/UNI_BLACK_BOOK.md` updated where internals change. Commits only on
approval.
