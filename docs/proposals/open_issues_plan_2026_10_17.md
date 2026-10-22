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
- **W1-followups status: all seven fixed, uncommitted.** Each was confirmed
  real by a test that fails with the fix reverted (current_thread starvation
  tests with a slow plugin or persistence backend; task-count tests; a
  failpoint-held flush stream). Per-site table and remaining open items:
  `docs/UNI_BLACK_BOOK.md`, "Background-task rules". Not revert-tested: the
  detached-spawn removals in `index_rebuild.rs` / `writer.rs` (no observable
  without a drop-time probe) and the deferral-tick move to the blocking pool.

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
RSS over 20k commits: +236.6 MB before, +55.8 MB after.

**W2 residual (2026-10-17): no per-commit leak.** Probe:
`connectors::cdc_runtime_e2e::w2_residual_memory_growth_observation`
(ignored), which reads live heap from glibc `mallinfo2` because RSS only
ratchets. Debug build, 20k commits:

| Regime | Measured | Reading |
|---|---|---|
| Default config | RSS +230.5 MB, live heap +23.5 MB, 216.8 MB free chunks held by glibc (`malloc_trim` returns 63 MB) | RSS growth is mostly allocator retention, not objects |
| New node per commit, no flush | 0.95 KB/commit; the final flush frees 23.4 → 7.6 MB | L0, as expected |
| One vertex rewritten per commit, no flush | ~2.7 KB/commit to ~1.2k, ~250 B to ~5k, then 4 B/commit through 20k (plateau ~4.2 MB) | Bounded per-commit state. The knee matches `CommitRegistry` capacity 4096 (inferred, not counted) |
| Rewrite, flush every 250 / 1000 / 5000 | 420 / 129 / 33 B/commit at 10k–20k, i.e. ~100–170 KB per flush, never freed | Per-flush retention |
| Same, flush every 250, Lance session caches capped at 1 MB (temporary edit, reverted) | 402 → 57 B/commit | ~86% of the per-flush growth is Lance's `Session` metadata/index cache |
| New node per commit, flush every 1000 | 298 B/commit at 10k–20k vs 129 for rewrite | ~170 B per stored row survives flush (per-row state, not attributed) |

**Open.** The Lance session uses `Session::default()`
(`lance_directory.rs`): an LRU bounded at 1 GiB metadata + 6 GiB index per
database, with no `UniConfig` knob. ~14 KB per flush (the capped run's
57 B/commit) and the ~170 B per stored row are unattributed. Under four
concurrent probes the default-config run failed once with "commit timed out"
(auto-flush on, heavy load); not investigated.

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
  - ~~The schemaless (label-less) scan path is not covered.~~ Done (wave B):
    `scan::schemaless_l0_prefilter` + `schemaless_l0_prefilter_candidates`;
    unflushed label-less lookup V → ≤ 1 at 2k/8k, write loop 2099 → ≤ 2. A
    `WHERE` is still not pushed into a scan of an undeclared label, so there
    only an inline property map engages it. See the Black Book.
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

**P6 status: implemented, uncommitted.** `<`, `<=`, `>`, `>=`, `AND`-ed ranges,
`STARTS WITH` (as a half-open string range, literal or parameter) and
`IS [NOT] NULL` on an Online BTree (null tests also Hash) go to Lance through
`pushdown::indexed_range_filter`, with the conjunct kept above the scan.
Measured, flushed: `n.x < 10`, `n.x >= 100 AND n.x < 110`,
`n.s STARTS WITH 'u42'`, `n.t IS NULL` read the answer (59 / 10 / 101 / 25
rows) at 2k, 8k and 32k instead of the label (2,042 / 8,042 / 32,042), wall
7-8 ms against 37-40 ms at 32k. A non-selective range is ~1.5x slower than
unpushed (60 vs 38 ms at 32k) after making the superseded-version re-read one
`_vid` range for dense sets (it was 3x via chunked `IN` lists). Indexed ranges
over L0 now prune L0 rows too (range bounds in the W3b pre-filter). Found and
fixed on the way, each a silent wrong answer or a spurious error on an indexed
equality: datetime normalization applied to STRING columns, `= -0.0` lost the
row, NaN / ±inf operands errored, `i64::MIN` errored (respelled in
`FilterExpr::to_sql`). Also: `-2.5` parsed as `Neg(2.5)` so negative float
bounds were never pushed (parser now folds it), `STARTS WITH $p` was not
pushable, and `-null` errored in `SET`. Not pushed by design: NaN / inf / zero
float bounds, type-mismatched / NULL / out-of-range operands, INT32 / FLOAT32
ranges, `NOT` / `OR`, the empty prefix, Hash ranges, unindexed columns
(measured ~2x for a selective unindexed range at 32k, flat at 8k; not adopted).
Open: the residual orders floats by IEEE total order (`-0.0 < 0.0`,
`NaN = NaN`), unlike openCypher. Tests: `index/range_prefix_null_pushdown.rs`
(6 tests + an ignored probe), unit tests in `pushdown.rs`, `eval.rs`,
`columnar_scan.rs`, `grammar/mod.rs`; every change reverted red.

### P7. A5: VLP property maps pre-scan the whole edge type

`build_edge_property_filter` (`traverse.rs:3645`) and
`build_accepting_vid_filter` (`traverse.rs:3561`) enumerate every edge or
target of the type before an anchored walk. Evaluate the map per edge while
walking (fetch the edge's properties for visited edges only), or bound the
pre-scan to the reachable frontier. Count the pre-scan in `rows_scanned` first
(O1), so the guard can see it.

**P7 status: implemented, uncommitted.**
- **Choice: bound the read to the reachable region, per input batch.**
  Evaluating per edge inside the walk would need an async read inside the
  synchronous enumeration; instead `ReachableMapFilter` (`traverse.rs`) runs a
  breadth-first pass from each batch's sources before the batch is walked,
  bounded by the hop limit, expanding only through passing edges, with one
  batched `get_batch_edge_props` read per layer and one target-property read
  per batch. Used by simple VLP and by `shortestPath`/`allShortestPaths`; QPP
  keeps the whole-type form.
- **Fall-back, by measurement.** One read round ≈ 0.8 ms; the whole-type scan
  ≈ 5 µs per edge (deep chain, every edge passing: 100 ms at 20k edges, 390 ms
  at 80k). A fixed 16-round cap made `*1..40` 115.6 ms against 38.4 ms
  uncapped, so the cap is `max(16, type edges / 160)` rounds, plus "more than
  half the type" for types of ≥ 4096 edges.
- **Measured (debug, flushed, anchored `*1..3`, best of 3).**

  | Query | 20k nodes before | after | 80k before | after |
  |---|---|---|---|---|
  | `{w: 0}` | 171.9 ms | 9.7 ms | 736.2 ms | 14.2 ms |
  | `(b:N {x: 2})` | 262.1 ms | 10.0 ms | 1149.2 ms | 13.8 ms |
  | no map (control) | 8.4 ms | 7.7 ms | 9.8 ms | 10.9 ms |

  `rows_scanned`: 40,001 / 160,001 → 7 / 7 (edge map), 40,002 / 160,002 → 15
  / 15 (target map).
- **Silent wrong answers found and fixed on the way.** `{w: 1.0}` against an
  integer `w` returned no rows (structural equality; now `cypher_eq`), and
  `{w: $p}` matched every edge (the parameter was not extracted, leaving no
  conditions; `df_planner.rs` `extract_edge_property_conditions` now resolves
  it). Both were wrong before P7 as well.
- **Tests.** `cypher_path::vlp_property_map_oracle`: map vs `WHERE all(r IN rs
  ...)` / `WHERE b.x = ...` reference over random graphs with parallel edges,
  self-loops, missing `w`, float/string/null/param values, three directions,
  four hop ranges, path and `DISTINCT` (reachability) forms, flushed + L0, an
  uncommitted transaction after a `SET` on flushed edges, a fork, and a deep
  chain that triggers the fall-back. `perf::profiler_blind_spots`: the O1 VLP
  guards are rewritten to pin the region read exactly at N=200/800, and a QPP
  guard keeps O1's whole-type counting under test. Reverting `traverse.rs`
  turns the oracle (`{w: 1.0}`) and all three guards red (401 rows instead of
  7); removing just the region read's `count_rows_scanned` turns the exact
  guard red (1 instead of 7).
- **Open.**
  - A non-constant map value (`{w: a.x}`) is dropped and the map ignored —
    silent wrong answer, pinned by an ignored test; needs a residual
    `all(r IN rs WHERE ...)` filter from the logical planner.
  - A list expression over a VLP relationship list fails with "all columns in
    a record batch must have the same length" once one source's trails exceed a
    batch (8,198 rows, seed 5 of the oracle); independent of P7 and of the
    neighbor order (both reverted, still fails). Pinned by an ignored test.
  - QPP per-hop maps still pre-scan the whole type.

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
  **Status: implemented, uncommitted.** Unaffected strata are reused, affected
  ones recomputed from scratch (no delta seeding); criterion in
  `uni_locy::incremental`, mechanism in the Black Book (ASSUME, *Incremental
  re-evaluation*). ASSUME of a Company node over an n-person KNOWS chain:
  `rows_scanned` plain/ASSUME 6,006/6,016 (n=2k), 24,006/24,016 (n=8k); 12,015
  at n=2k with reuse disabled.
- **R1.** Pending D3.
- **A10.** `{ext_id: …}` pushdown on labelled nodes: write the probe first.
- **P9 status (2026-10-18): A7 and A6 implemented, uncommitted; A10 probed.**
  - A7: `PropertyLookupJoinExec`; anchored disjoint pattern `rows_scanned`
    401 / 1,601 → 2 / 2 (N = 400 / 1,600); an all-label build stays 800 / 3,200.
  - A6: named far-end-anchored path 800 / 3,200 → 1 / 1; `p` identical to the
    unreversed plan (differential over flushed, L0, tx, fork, OPTIONAL).
  - A10: a labelled `{ext_id: …}` returns **no rows** once the data is
    flushed (`rows_scanned` = N, `index_scans` 0); the per-label `ext_id`
    column is never read. Fix is in the storage read path; ignored repro
    `perf::anchored_lookup_test::a10_ext_id_equality_on_a_labelled_node`.
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
  - ~~The Python bindings lack the new metrics.~~ **Done (2026-10-17):**
    `QueryMetrics.{physical_plans, physical_plan_time_ms,
    shortest_path_searches, subquery_executions, lance_iops}` (class and the
    `ExecuteResult.metrics` dict), `ProfileOutput.{logical_planning_ms,
    physical_planning_ms, physical_plans}`; Locy iteration `planning_ms` /
    `plans` were already reachable through the serde-built `strata` tree and
    are now tested and documented. `tests/test_planning_and_search_metrics.py`
    (14 tests, sync + async): 11 red with the bindings zeroed, the other 3 are
    a zero control, the vector-search counters (bound before this) and the
    async Locy tree (serde path). The parity guard gained a struct-field axis
    (`QueryMetrics`, `ProfileOutput`, `LocyStats`) plus a dict-vs-class key
    check; each of 4 synthetic drops is red.
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
  - ~~No test isolates the `ReadSetRecordingExec` look-through~~ (closed
    2026-10-17): `bugs::vid_lookup_join_reachability::vid_lookup_probe_reads_are_in_the_ssi_read_set`
    runs `MATCH (a:Source) MATCH (b:Target) WHERE id(b) = a.linked_vid` in a
    transaction, so `b` is read only by the lookup (no traversal). It asserts
    both operators ran, that a concurrent SET on the looked-up target
    conflicts, and that a SET on a skipped target does not. Red on both
    reverts: `wire_probe` dropping the wrapper (no conflict), and `probe_scan`
    not looking through (operator not emitted).
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
  - ~~Output building now dominates~~ Done (see "P2 leftovers" below).
  - ~~Tie-breaking differs from run to run~~ Done (see below).
  - The target range filter is still a `FilterExec` over the full scan (P6).

**P2 leftovers status: implemented, uncommitted.**
- **Edges recorded during the search.** `parent` / `predecessors` carry a
  `Hop` (eid, type, stored orientation); `find_edge` is gone, and so is the
  per-hop orientation probe (undirected searches read the two directions
  separately). New counter `QueryMetrics::shortest_path_adjacency_reads`
  (Rust and Python). Ladder graph, unbound target: 41,889 → 1,977 adjacency
  reads at 2k nodes, 202,258 → 7,954 at 8k; `allShortestPaths` 109,454 →
  1,996 and 757,231 → 7,973. Operator time at 8k: `shortestPath` 1777 →
  1042 ms, `allShortestPaths` 8381 → 6241 ms; at 2k the operator times were
  within noise (420 vs 565 ms) on a machine at load average 60–100, so the
  counts are the evidence, not the timings.
- **allShortestPaths and parallel edges (openCypher: each distinct edge
  sequence is a distinct path).** The rows for two parallel edges used to
  carry the same relationship (`find_edge` returned the first match); they now
  carry each edge. Which parallel edge `shortestPath` reports is unchanged in
  rule — the first admitted edge in expansion order — and now always the one
  the search took.
- **Deterministic tie-breaking.** `AdjacencyManager::get_neighbors` /
  `get_neighbors_at_version` sort by `(neighbor vid, eid)` (sorted vector, not
  a per-call `HashMap`); the transaction-L0 overlay re-sorts. Release
  micro-benchmark per call: degree 4 1.16 → 1.02 µs, 16 1.61 → 1.01, 64 4.90
  → 1.06–1.15, 512 20.5 → 1.65–1.71 (Main CSR only); layered (CSR + frozen +
  active) 1.21 → 1.06–1.10, 5.54 → 1.56–2.03, 19.6 → 5.8–6.0.
- **Tests.** `shortest_path_many_targets`: the BFS oracle now also checks
  relationship identity (edge `k`) — `shortestPath`'s relationships must be a
  shortest admitted edge sequence, `allShortestPaths` rows must equal the
  oracle's edge sequences; `shortest_path_tie_break_is_deterministic` (30 runs,
  directed and undirected, flushed / L0 / uncommitted transaction / fork, same
  path across tiers); `path_output_reads_no_extra_adjacency` (n = 60 / 240);
  AM unit tests for order, dedup, tombstones, self-loops. Reverts: AM order →
  30 distinct paths in 30 runs; overlay sort → 29 in the transaction tier;
  `find_edge` output → 3,599 reads for n = 60 and duplicate `allShortestPaths`
  rows. Each restored byte-identical (`cmp`).

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
  - ~~The batched Apply path still uses parameter imports.~~ Done (wave B):
    the batched path was unreachable and is removed; empty input now yields
    no rows (DF and fallback). The reachable defect was the in-query procedure
    call: any non-DF-listed procedure sent the whole query to the fallback
    executor (`RETURN a` = VID, labels / edge properties NULL); entity
    arguments failed; the post-YIELD `input_filter` dropped `a.x = 2.0`
    against 2; predicates crossed a `LIMIT`. All fixed; see the Black Book.
  - ~~The `rewrite_query_correlated` property-to-parameter rewrite is now
    redundant.~~ Removed (wave B), with label lifting, after measuring no
    change in correctness suites, TCK both modes, or `index_scans` /
    `rows_scanned` at two scales.

### P10. Constant `IN` lists are O(rows × |list|) (new, from P3)

`_cypher_in` (`df_expr.rs:1028`) scans the list per row and has no hash-set path
for constant lists. This affects user queries with large literal `IN` lists, not
only hydration. Fix: precompute a set keyed by `_cypher_join_key` (W3a), and
re-check with `cypher_eq` on a key hit to keep 2^53 and NULL semantics.

**P10 status: implemented, uncommitted.** `InListIndex` in
`uni-query-functions/src/df_udfs/in_list.rs`, used when the list argument is a
scalar and the element a column of ≥ 2 rows; see the Black Book. Debug build,
`UNWIND range(1,n) … WHERE x IN $ids` with n = |ids|, best of 3: 90.6 →
7.5 ms at 2k, 2556.8 → 23.0 ms at 8k (linear control 8.0 / 14.3 ms). Found on
the way and fixed: list parameters with no common native item type were
stringified item by item (`$l = [[]]` arrived as `[Bytes("[]")]`);
`typed_large_list_to_cv_array` rejected `[]` and date list literals compared
with a CypherValue; `arrow_to_json_value` now errors instead of answering
NULL.

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

## Deferred-items wave (started 2026-10-17, after commits 5ebf6f0c6..2650428b2)

**Decisions.**
- P8 magic sets: **implement**, as a compiler-level program rewrite in
  `uni-locy`, falling back to full evaluation wherever the rewrite is unsafe.
- `~=`: k = LIMIT (+SKIP) when the KNN feeds that LIMIT directly; otherwise a
  `UniConfig` default with a truncation warning. The KNN never goes below a
  filter that sits above a LIMIT. OPTIONAL KNN gets an `optional` flag.
- Release notes: stay `UNRELEASED` until release time.

**Wave A (running in parallel, file-disjoint).**

| Agent | Items |
|---|---|
| 1 | W1 follow-ups (background-task audit, 7 items) |
| 2 | P5 (R2 BEST BY delta, R3 non-linear and mutual semi-naive, R5 IS NOT set) |
| 3 | P6 (range, prefix and null index pushdown) |
| 4 | P7 (VLP pre-scan), P2 path-output cost, deterministic neighbor order |
| 5 | P10 (constant IN hash set) and `arrow_to_json_value` decode errors |
| 6 | Edge EID lookup (P3 edge hydration), Locy plan time, DERIVE overlay |
| 7 | Python binding metrics, W2 residual RSS, ReadSet look-through test |
| 8 | P8 magic sets |
| 9 | `~=` k semantics and OPTIONAL KNN |

**Wave B (planner-heavy, after wave A).**
- P9: A7 sideways information passing, A6 named-path reversal, A10 probe,
  A4 incremental ASSUME.
- Batched Apply path imports, and removing the redundant
  `rewrite_query_correlated`.
- W3b schemaless L0 prefilter.

**Agent 7 outcome (bindings, RSS, SSI test): done, uncommitted.**
- **Python bindings.** New metrics on `QueryMetrics`, `ProfileOutput` and the
  `ExecuteResult.metrics` dict.
  - The parity guard now also checks struct fields and the dict keys; before,
    it compared methods only.
  - `test_planning_and_search_metrics.py`: 14 tests, 11 of them red on revert.
  - Full Python suite 1005 passed.
- **W2 residual RSS: no per-commit leak.**
  - Of +230 MB RSS, only +23.5 MB is live heap; glibc keeps the rest as free
    chunks.
  - Growth while unflushed is L0, and an L0 rewrite levels off.
  - Each flush leaves ~100–170 KB, mostly Lance's default session cache, which
    can grow to 1 GiB of metadata plus 6 GiB of index per database. With the
    caches capped it is 57 B/commit.
  - Probe: `cdc_runtime_e2e::w2_residual_memory_growth_observation`
    (`#[ignore]`).
- **Decision pending.** No `UniConfig` sizing exists for the Lance session
  cache.
- **SSI look-through test.**
  `bugs::vid_lookup_join_reachability::vid_lookup_probe_reads_are_in_the_ssi_read_set`
  fails on both reverts.
- **Coordinator TODO.** Run `check_documented_counts.py` at the end; it
  reported Locy TCK 528 against 530 in `docs/complete_locy.md`.

**Agent 6 outcome (edge lookup, plan time, DERIVE): done, uncommitted.**
- **Edge hydration by EID.**
  - `MainEdgeDataset::find_edges_by_eids_counted` reads the main table, and
    `l0_visibility::lookup_edges` covers L0, recording SSI reads.
  - `StorageManager::lookup_edges_by_eids` asks L0 first, then the main table,
    and handles forks and pinned snapshots.
  - Used by `impl_locy.rs` `lookup_edges`. The Cypher edge-lookup queries are
    deleted.

  | Measure | Before | After |
  |---|---|---|
  | Rows to hydrate 1 EID | the whole graph | 1 |
  | Time, 4k / 16k nodes | 75 / 286 ms | ~4.5 ms flat |

- **Locy `plan_time`.** It was 0; it is now timed (~0.5 ms out of 8 ms).
  ASSUME/ABDUCE re-planning is not included.
- **Silent wrong answer, fixed.** `extract_cypher_conditions` dropped every
  IS reference from a DERIVE rule body, at top level and inside ASSUME.
  - `WHERE a IS flag` with one `flag` fact wrote 4 edges instead of 1.
  - `a IS reach TO b` wrote none of the intended edges.
  - DERIVE now resolves IS references through the binding-plan evaluator,
    against the issuing command's store (inside ASSUME, the overlay).
- **Tests.** In `locy_entity_hydration.rs` and `locy_program_plan_time.rs`.
  Guards are red on revert; value equality against Cypher holds across tx,
  pinned, fork and ASSUME.
- **Open.** Planning Cypher `id(r) = $e` as a lookup needs a new operator.

**Agent 1 outcome (W1 follow-ups): all 7 real, all fixed, uncommitted.**
- **Async trigger fire.** Runs on `spawn_blocking` and is tracked.
- **Deferral tick.** Moved to the blocking pool, untested.
- **Scheduler.** Each run is one tracked task, and the lifecycle writes run on
  the blocking pool.
- **CDC.** The driver owns the streams and delivers on the blocking pool; a
  plugin panic halts only its own stream.
- **Index rebuild leak.** One manager per `UniInner`; before, there was one
  per `rebuild()` call, and they overwrote each other's state file.
- **Detached spawns.** Replaced by a worker requeue and a synchronous
  `enqueue`. Not revert-tested.
- **`trigger_async_compaction`.** Deleted.
- **Flush streams.** Now counted, and Drop waits for them.
- **`ShutdownHandle`.** Now `Clone`, and prunes finished handles.
- **Behavior change.** A failed rebuild task with attempts left also retries
  after a restart.
- **Open.**
  - `DeferralQueue::push` does an fsync under a lock on the commit path.
  - Fork and snapshot `UniInner`s get a `ShutdownHandle` that is never
    signalled.
- **Regression seen (investigation agent running).** In 2 of 4 runs of
  `compaction_lifecycle_test`, a live compaction panicked at
  `compaction.rs:~808` ("EID … not found in main_edges"). That module was
  green in the round-3 combined run.

**Compaction EID panic: root-caused and fixed, uncommitted.**
- **Cause.** `uni-bulk` `flush_edges_buffer` wrote the FWD and BWD deltas
  before `main_edges`. Unguarded background compaction could read delta EIDs
  that had no `main_edges` row yet. `Writer::flush_to_l1` already writes
  `main_edges` first, for exactly this reason.
- **Proof.** With a 500 ms gap between the writes, the original order failed
  5/5 and the fixed order 0/5.
- **Not the cause.** `exists_by_eid` and P6's FilterExpr work.
- **Measured.** The failure rate under load was 1/18; with the fix,
  `compaction_lifecycle` passed 48/48.
- **Regression test.** `bulk_dual_write_resilience.rs` (failpoints, at the new
  `delta::before-write-run` seam) fails when reverted (0 of 200 edges in
  `main_edges`).
- **Follow-up (agent running).** Two candidates from code reading: compaction
  can make an in-flight bulk load's edges readable before commit, and bulk
  `abort()` does not roll back L2 adjacency.

**Agent 4 outcome (P7, P2 output, tie-break): done, uncommitted.**
- **P7.** `ReachableMapFilter` reads only the region reachable within the hop
  bound, through edges that pass the map. It falls back to the whole-type scan
  by measured cost.
  - `{w:0}` at 80k nodes: 736 → 14 ms.
  - `rows_scanned`: 160,001 → 7.
- **Silent wrong answers fixed.** `{w: 1.0}` against an integer `w` matched
  nothing (structural comparison), and `{w: $p}` matched every edge (the
  parameter was never extracted).
- **O1 guard reworked.** It now asserts an exact `rows_scanned` equal to the
  region size.
- **P2 output.** `Hop` records the edge it took, and `find_edge` is gone.
  - Adjacency reads at 8k: 202k → 8k.
  - Parallel edges used to give duplicate identical rows; each distinct edge
    sequence is now its own row (openCypher).
  - New counter `shortest_path_adjacency_reads`.
- **Tie-break.** `get_neighbors` returns rows sorted by (vid, eid), and is
  faster too (degree 512: 20.5 → 1.7 µs). 30/30 runs give the same path.
- **New defects, pinned by `#[ignore]` repros, for wave B.**
  - Silent wrong answer: `{w: a.x}` (a non-literal map value) is dropped, so
    the map matches everything.
  - A list expression over a VLP relationship list errors once one source's
    trails exceed a batch (8,198 rows): "all columns … same length".
  - QPP per-hop maps still pre-scan the whole type.

**Agent 8 outcome (P8 magic sets): implemented, default Off, uncommitted.**
- **Design.** `uni-locy/src/magic.rs` `rewrite_goal_directed` is a
  compile-level rewrite into an ordinary program.
  - Goal bindings: `c.p = literal/$param` or `IN` on a node KEY column.
  - Sideways information passing runs through MATCH patterns only. Magic rules
    read only the graph, so stratification is preserved.
  - The safety matrix falls back for ALONG, recursive FOLD/BEST BY, PROB,
    PRIORITY, generators, neural models, DERIVE, EXPLAIN, ABDUCE and ASSUME.
- **Modes.** `LocyConfig::goal_directed`: Off (default), On, Shadow. `On`
  drops rules that only a rewritten goal needed from `derived`, which breaks
  the documented "`derived` = every rule" contract. That is why the default is
  Off.
- **Measured** (disjoint chains, TC from one node). Off: 4,750 / 19,000 facts.
  On: 210 / 210.
- **Soak.** 500 cases and 9,935 goals, 6,517 of them rewritten: 0 On≠Off,
  0 divergences from the oracle.
- **TCK.** Off and Shadow 530/530. On 518/530; all 12 are `derived` counts, as
  designed.
- **Open.**
  - No Python `goal_directed` yet.
  - Not yet built: SIP through IS-refs, scalar KEY adornment, EXPLAIN.
  - Relaxing the fallbacks needs proofs.
  - New bug: `QUERY … RETURN 'a' + 'b'` fails (`locy_eval.rs`
    `eval_binary_op` routes Add only to `numeric_op`).

**Agent 3 outcome (P6): done, uncommitted.**
- **Served by the index.** Ranges and `STARTS WITH` on a BTree; `IS [NOT]
  NULL` on a BTree or Hash. The residual filter is kept above.
  - At 32k: `n.x < 10` reads 59 rows against 32,042 (8.5 vs 37 ms).
  - Unflushed rows are pruned for indexed ranges.
- **Cost.** Non-selective indexed ranges run about 1.5x slower than a scan
  (~60 vs 38 ms at 32k), from re-checking newer versions. There are no column
  statistics to pick between the two up front.
- **Fixed on the way.**
  - Indexed equality: a datetime-looking string literal was rewritten (silent
    miss); `-0.0` lost its row; NaN/inf errored; `i64::MIN` errored.
  - Negative float literals now fold to a literal.
  - `STARTS WITH $p` now reaches the scan.
  - `-null` in SET returns NULL; `i64::MIN` negation reports overflow.
- **Tests.** `index/range_prefix_null_pushdown.rs`: 943 differential cases
  across flushed, mixed, mutated, transaction and fork data, plus guards. Every
  revert goes red.
- **Changed test.** In `transition_probe`, the tripwire `index_scans == 0`
  became `>= 1`. Its own message anticipated a BTree range route, and the
  logical-plan invariant still holds.
- **New for wave B.**
  - **Spec deviation:** floats compare by IEEE total order (`-0.0 < 0.0`,
    `NaN = NaN` true); openCypher says `-0.0 = 0.0` and every NaN comparison is
    false. The fix belongs in `df_expr.rs`.
  - Property names that are SQL keywords in pushed filters are untested.

**Agent 5 outcome (P10, decode errors): done, uncommitted.**
- **P10.** `in_list.rs` `InListIndex` hashes a constant list per batch by
  `_cypher_join_key` and re-checks hits with `entity_aware_eq`. Null, list,
  map and vector items go to a residual scan.
  - At 8k: 2,557 → 23 ms.
  - A 41 × 52 value differential agrees with the per-row scan.
- **Fixed on the way.**
  - Silent wrong answer: a heterogeneous list parameter had its items turned
    into text bytes, so `[] IN $l` was false and dates never matched.
  - `c.v = []` errored.
- **Decode errors.** `arrow_to_json_value` returns `DFResult`; every caller
  propagates the error. Untyped bytes come back as `Value::Bytes`.
- **New for wave B.**
  - **Silent wrong answer:** a parameter mixing ints and floats is widened to
    a float list, so `$l = [2^53+1, 1.5]` makes `2^53 IN $l` true. Vector
    parameters must not regress.
  - A list arriving as a column with repeated rows (the IC5 shape) is still
    scanned per row.

**Agent 9 outcome (`~=` k, OPTIONAL KNN): done, uncommitted.**
- **Sizing.** k = LIMIT (+SKIP) when the KNN feeds that LIMIT directly
  (including `ORDER BY n._score DESC`, WITH…LIMIT, and per UNION branch).
  Otherwise it is `UniConfig::vector_knn_default_k` (default 100), with a
  warning when the cap is hit. That covers a residual filter, inline maps,
  ORDER BY on another key, DISTINCT and no LIMIT.
- **Never below a LIMIT.**
  - Silent wrong answer, fixed: `MATCH (n) WITH n LIMIT 5 MATCH (n) WHERE
    n.emb ~= q` returned the top 5 of the whole label.
  - It is now refused at planning (`reject_unplanned_approx_eq`), as is any
    `~=` that cannot replace its scan.
- **OPTIONAL KNN.** `VectorKnn{k: Option, optional}` emits a NULL row when
  nothing qualifies.
- **Fixed on the way.** Sub-plan warnings were collected into a store nobody
  read, so `~=` inside CALL{} never warned.
- **Tests.** `knn_limit_semantics.rs` (9) and the uni-query
  `planner/knn_sizing.rs`. All 7 reverts go red. TCK both modes 0 failed.
  Python config parity OK.
- **New for wave B (unverified).** The planner may apply `WITH … ORDER BY /
  SKIP / LIMIT … WHERE` before the ORDER BY/SKIP/LIMIT; check against the
  openCypher spec and TCK.

**Agent 2 outcome (P5): done, uncommitted.**
- **Counters.**

  | Item | Counter | Before | After |
  |---|---|---|---|
  | R2 | DerivedScan, L = 20 / 80 | 190 / 3,160 | 19 / 79 |
  | R3 mutual | DerivedScan | 120 / 1,680 | 20 / 80 |
  | R3 non-linear | HashJoin rows over the run, N = 2k / 8k | 722,800 / 2,891,200 | 114,000 / 456,000 (one per derivation) |
  | R5 | IS NOT set | built every iteration | built once, no measurable overhead |

- **Form.** Non-linear clauses run in k passes: scan i reads Δ, earlier scans
  read old facts, later scans read full facts. F_newᵏ \ F_oldᵏ is the disjoint
  union of these passes, so each tuple joins exactly once, which keeps the
  ALONG/FOLD per-derivation identities. The derivation is in the Black Book.
- **Silent wrong answers found by the new oracle families (both fixed).**
  - BEST BY pruned the body before IS NOT, so the runner-up that negation keeps
    was already lost. This was also wrong under full feeding.
  - Int→Float drift: `YIELD e.w + e.w` over a schemaless integer came back as
    a Float, because `infer_expr_type` types properties as Float64. The new
    `arithmetic_yield_type` fixes it.
- **Soak.** 500 cases, 0 divergences. A deliberate Δ×Δ bug gives 14/96
  diverging. Locy TCK 530/530.
- **Open.**
  - Non-linear old and full scans are still O(relation) per pass; that needs an
    index.
  - `merge_best_by` sorts all facts per merge, so BEST BY is about 3x plain.

**Bulk-load isolation: confirmed and fixed (staging), uncommitted.**
- **Measured before the fix.**
  - Uncommitted bulk vertices and edges were visible to other sessions, even
    with no compaction: the writer appended into the live tables.
  - Abort after a compaction left 31 traversable edges, 30 of them dangling
    with NULL properties, and they persisted after reopen.
  - **Data loss:** abort restored each table's pre-load version, so it
    destroyed vertices and edges that other sessions committed during the
    load.
  - A CSR warmed during the load kept aborted edges traversable until reopen.
- **Fix.**
  - Flushes stage Lance data files (`execute_uncommitted`).
  - `commit()` publishes one Append per table against the current version,
    under the table write lock, in the order per-label vertices → vertices →
    `main_edges` → deltas.
  - Abort discards the staged files. Indexes are updated after publish.
  - The crash marker covers only the publish step, and includes the touched L2
    adjacency tables.
  - Forks (no staging support) keep the old path.
- **Tests.** `bulk_isolation_resilience.rs` (failpoints), 4 tests. Forcing the
  old protocol turns them red.
- **Open.**
  - A crash mid-publish still rolls back by version, so the window is the
    publish step, not the whole load.
  - One crash-marker file per database.
  - Bulk abort on a fork fails, as it did before.

**Wave A verification (2026-10-17/18).**
- **Results.** Workspace 7765/7767, failpoints 179/179, both TCKs green,
  Python 1007/1008.
- **Fixed after the run.**
  - A stale `arch_entity_identity` ratchet entry.
  - 11 rustdoc links from public docs to private items.
  - The Python `plan_time` contract (Locy now measures planning).
  - The generated Python API page.
  - The doc count (528 → 530).
- **Teeth patch.** `lance_col_fusion.patch` is regenerated at the BTree range
  site, in `build_indexed_property_pushdown`. Hash columns no longer push
  ranges, so the old control and oracle stopped being witnesses.
  - New control: `range_prefix_null_pushdown::translated_forms_match_the_unpushed_oracle`.
    All 3 variants fail under the patch; with it applied, 778 of 781 tests
    pass, the 3 failures being exactly the control.
  - Under Lance 11 the defect is a silent `[]` on string columns and a cast
    error on numeric columns.
- **Wave B (running).** B1 value semantics, B2 non-literal pattern maps, B3
  Locy operators and Python `goal_directed`, B4 P9 planner items, B5 Apply
  imports and schemaless L0, B6 keyword identifiers and a BTree DQP fixture,
  B7 incremental ASSUME.

**Wave B interrupted (2026-10-18).**
- **What happened.** All seven wave B agents stopped mid-edit when the account
  hit its weekly limit (it resets 2026-10-21).
- **Tree state.** The tree compiles, but it mixes the verified wave A work with
  partial wave B edits.
  - Lib suites: 2233/2235. The 2 failures are B6's new identifier-quoting
    unit tests in `backend/eval.rs`.
  - uni-db: 3331/3349. Of the 18 failures, some are new, unfinished wave B
    tests (float semantics, Locy operator parity, A6 reversal, QPP maps). Others
    are wave A tests the partial edits broke: `outer_entity_in_subquery` ×4,
    `range_prefix_null_pushdown` unflushed, `locy_facts_and_filters` ×4,
    `entity_comparison` ×2, and the `plan_shape::gate` operator registry ×2.
- **Not committable** until wave B is finished or backed out. Temporary probe
  files are present: `bugs/apply_probe_tmp.rs` and
  `cypher_read/zz_probe_float.rs`.
- **Disk.** It filled again; `target/debug/examples` and `incremental` were
  deleted (515 GB).

**B3 outcome (Locy operators, Python goal_directed): done, uncommitted.**
- **Locy operators.** `locy_eval.rs` now calls the shared Cypher
  `eval_binary_op`.
  - The 6,103-expression parity matrix had 1,018 disagreements before and 0
    after.
  - Old classes: numeric-only `+`, no `=~`, `[1,null]=[1,null]` true, NaN equal
    to NaN, debug-build overflow panics.
- **Python.** `goal_directed` is exposed, and the parity script now checks
  `LocyConfig`.
- **Cypher defects the matrix exposed (unfixed, for a follow-up).**
  - A DataFusion panic on `n.missing AND null`.
  - About 150 planning or run-time errors on NULL operands.
  - Literal vs property path differences: temporal arithmetic, `date - date`
    returns Int vs a duration, row-path `n.s + 1` errors.
  - `n.s =~ 'ab'` fails to plan.
  - Batch short-circuit: `true OR n.i`.
  - The shared `eval_binary_op` disagrees with DataFusion on `2 ^ 3` and
    `NULL AND 1`.
- **Under check (B6).** A Python BTree two-sided range returned 0 rows with
  double-quoted names. This is likely B6's teeth patch having been applied
  during the run; the current tree has none.

**B2 outcome (non-literal pattern maps): done, uncommitted.**
- **Fixed silent drops.** VLP edge maps with non-literal values, in MATCH,
  OPTIONAL, EXISTS, comprehensions and single-hop QPP.
- **Fixed silent wrong answers.**
  - `OPTIONAL MATCH (b {x: a.x+10})` lost unmatched `a` rows (a W4 violation).
  - A QPP inner-source map was checked on the first iteration only; it is now
    refused.
- **Fixed loud errors.** A map naming a variable bound later in the same
  pattern, and a shortestPath target map.
- **Refused with a clear error.** Non-constant shortestPath edge maps
  (Neo4j accepts these) and non-constant multi-step QPP inner maps.
- **Design.** One definition of a walk-constant value; other entries become an
  `all(e IN step WHERE …)` residual, which nulls rather than drops under
  OPTIONAL.
- **Tests.** 43 pairs in `vlp_property_map_oracle.rs` across flushed, L0,
  mixed and fork data. Reverting the core fix turns 20 of 43 red. TCK both
  modes unchanged.
- **Open.**
  - QPP inner-source *labels* may have the same first-iteration-only defect
    (not probed).
  - A non-constant map on an anonymous VLP now enumerates paths instead of
    using reachability (perf not measured).

**B6 outcome (identifier quoting, BTree fixture, teeth): done, uncommitted.**
- **Python range failure.** It came from B6's teeth window: the revert patch
  was applied to the shared tree for about 42 s. On the final tree the test
  passes and the repro returns 2 rows.
- **Bare identifiers in pushed filters.**
  - At the Lance level, 90 of 222 renderings failed.
  - End to end, 82 of 486 cases failed: Lance parse errors surfaced to the
    user, and properties named `null`/`true` silently returned `[]`.
  - Lance's identifier quote is the backtick only (from lance-datafusion 11
    source, confirmed empirically).
- **Fix.** `quote_ident` backtick-quotes every identifier; a name containing a
  backtick is refused and falls to the residual filter. The range-path
  `is_bare_identifier` gate is removed.
- **Silent data loss fixed.** `SET n.\`order\``, map-literal keys and
  map-projection selectors kept the backticks, so writes went to a property
  literally named with backticks (`walker.rs` → `normalize_identifier`).
- **Fixture.** A `TypedBTreeIndexed` DQP mode and
  `flush_pushdown_btree_smoke`. The teeth entry is
  `lance_col_fusion|translated_forms_match_the_unpushed_oracle|flush_pushdown_btree_smoke`;
  both fail under the patch.
- **Open.**
  - The grammar rejects the doubled-backtick escape.
  - The DDL walker sites (not audited) still keep backticks.
  - VLP `edge_lance_filter` is dead code.
  - `topo_rewrites_unflushed_small_batches` failed once under load
    (unexplained, message not captured).

**B1 outcome (value semantics): done, uncommitted.**
- **Floats now follow IEEE.**
  - Old behavior: `-0.0 = 0.0` was false, NaN equalled itself, `n.f = 0.0`
    missed `-0.0`, `-0.0` and `0.0` were separate DISTINCT groups, and
    `CASE … WHEN 0.0` missed `-0.0`.
  - New `float_cmp.rs` UDFs, plus native forms guarded with `isnan` against
    literals, which keeps the L0 prefilter working.
  - Group and DISTINCT keys are folded (`float_equivalence_key`).
- **Mixed int/float lists** (parameters and literals) are encoded whole as a
  CypherValue, so `2^53 IN [2^53+1, 1.5]` is no longer true. Vector
  parameters are guarded across all their readers and writers.
- **VLP list over more than one batch.** A sliced `LargeListArray`'s
  `values()` paired with offset-based indices (`quantifier.rs` and
  `comprehension.rs`); fixed by `compact_large_list`.
- **WITH … ORDER BY … SKIP/LIMIT … WHERE.** uni filtered before the page,
  against the openCypher BNF and spec. It now filters after the page
  (`where_after_page`).
- **Tests.** `float_ieee_semantics_test` (7), `mixed_numeric_list_param_test`,
  the un-ignored VLP test, and `with_where_filters_after_order_by_skip_limit`.
  Reverts go red. TCK both modes unchanged.
- **Open.**
  - Int vs float comparisons still widen to f64 (openCypher compares them
    exactly).
  - `1` and `1.0` are not treated as equal for DISTINCT/grouping.
  - `[1, n.f]` is widened by `make_array`.
  - VLP edge L0 float checks were not audited.

**B4 outcome (P9): A7 and A6 done, uncommitted; A10 is a correctness bug (agent running).**
- **A7.** New `PropertyLookupJoinExec` publishes `prop IN (build keys)` into a
  `DynamicPropertyFilter` on an indexed probe scan.
  - It requires exact bare same-dtype keys; mixed types keep the hash join.
  - It falls back to one scan above 4,096 keys or at 25% of the label.
  - rows_scanned 401 / 1,601 → 2 / 2. A large bound side shows no regression.
- **A6.** A named fixed-length path anchored on the far end now reverses; `p`
  is rebuilt in written order. rows_scanned 800 / 3,200 → 1 / 1.
- **A10 (silent wrong answer).** `MATCH (n:N {ext_id:'x'})` finds the node
  before a flush and nothing after it: the labelled scan reads `ext_id` from
  `overflow_json`, which the flush strips. Repro:
  `a10_ext_id_equality_on_a_labelled_node` (ignored).
- **New planner error.** An inline map on a later node that names an outer
  variable fails to plan: `MATCH (s) MATCH (a)-[:R]->(b {uid: s.name})` gives
  "No field named s". An agent is fixing both.

**B7 outcome (A4 incremental ASSUME/ABDUCE): done, uncommitted.**
- **How it decides.** `uni-locy/src/incremental.rs` decides from syntax which
  strata a mutation can affect: the labels, relationship types and property
  keys it writes, against what each rule reads. Opaque constructs always
  recompute.
- **Reuse.** Unaffected strata are seeded from the pre-mutation facts
  (`set_reused_strata`). An epoch plus an L0 fingerprint guards against a
  base-state change.
- **Fixed on the way.**
  - The hypothetical run read live storage, not the pinned snapshot.
  - It dropped custom functions.
  - A partial hypothetical run was silently accepted.
- **Measured.** An ASSUME adds 10 rows over the plain program at both 2k and
  8k; before, it cost about 2x the program.
- **Differential.** Against a committed-mutation second database: 48 cases
  with 0 divergences, and a 500-case soak with 0 divergences once MNOR/MPROD
  floats are compared at 12 significant digits (fold row order).
- **Open.**
  - Affected strata are recomputed from scratch.
  - Reused strata are still planned.
  - ABDUCE candidate defects (existing, unfixed): a `RemoveEdge` with empty
    endpoint properties appears to delete every edge of its type; duplicate
    `ChangeProperty` candidates with `element_var "a._all_props"`;
    `evaluate_abduce` reads the top-level store inside ASSUME.
- **PR-lane flake (agent fixing).** `random_programs_match_the_oracle` misses
  its `mpb` coverage floor in 4 of 5 runs: a generator problem in a P5
  family.

**B5 outcome (procedure Apply, correlated rewrite, schemaless L0): done, uncommitted.**
- **Batched Apply path.** It could never run, so it was deleted as dead code.
- **In-query procedure calls (`MATCH … CALL p(args) YIELD`) had 5 defects,
  all fixed.**
  - Plugin and `uni.system` procedures were routed to the row-based fallback.
    There, `RETURN a` gave a VID, `labels(a)` gave NULL and `r.w` gave NULL:
    silent wrong answers.
  - Entity-reading arguments failed or became NULL; they are now hoisted into
    a Project.
  - Empty input ran the body once.
  - `input_filter` dropped a stored `2` under `= 2.0`.
  - A predicate was pushed below a LIMIT.
  - Also: `coalesce` over a subquery failed.
- **`rewrite_query_correlated` and label lifting removed.** Measured A/B
  first: counters identical (`index_scans` 200, `rows_scanned` 400), and wall
  time within noise.
- **Schemaless L0 prefilter.** Unflushed label-less lookups: 2000 → ≤ 1 rows.
  The shadow set is not load-bearing there, because output re-reads L0 by
  vid; it is kept as a defence.
- **Open.**
  - UNWIND'd entity with a label in a pattern fails (`No field named
    "a._labels"`), at top level and in EXISTS.
  - `coalesce` with `r['w']` gives an ambiguous-field error when two edge
    types share `w`.
  - `CALL p(x)` without YIELD inside a query never runs (from reading the
    code, not reproduced).
  - `plan_apply` may stringify non-string yields of plugin procedures (not
    verified).
  - B5's note that WITH…LIMIT…WHERE filters before the page predates B1's fix
    (re-check in the combined run).

**A10 outcome: fixed (silent wrong answer and data loss), uncommitted.**
- **Cause.** A flush moves a string `ext_id` into the per-label column and
  strips it from `overflow_json`. Every per-label reader (columnar scan, five
  `PropertyManager` paths) read only the blob.
- **Wrong for flushed declared labels.** Filters, `n.ext_id`, `properties`,
  `keys`, `RETURN n`, traversals, OPTIONAL, forks, after reopen, and MERGE
  (missed the existing vertex).
- **Data loss.**
  - Any SET on a flushed labelled vertex rewrote its map without `ext_id`,
    and the next flush NULLed it in both tables.
  - A non-string `ext_id` was dropped at flush.
- **Fix.** `property_builder.rs` `stored_in_ext_id_column` /
  `restore_ext_id_column`, used by flush, the scan, `PropertyManager` and
  compaction.
- **Knock-on.** Fork promote's insert-only twin strips an `ext_id` that
  primary already holds; a fork-only `ext_id` is now carried over (it was
  silently lost before).
- **Pushdown.** `{ext_id:…}` on a labelled node uses the per-label BTree:
  rows_scanned 400 / 1600 → 1 / 1.
- **Outer-variable map on a traverse target** ("No field named s"):
  non-self entries become a residual filter above the traversal; under
  OPTIONAL it null-extends.
- **Tests.** `bugs/ext_id_on_labelled_nodes.rs` and
  `bugs/outer_variable_in_pattern_map.rs`. All 5 reverts go red. TCK both
  modes unchanged.
- **Open.**
  - A user property named `overflow_json` collides with the internal blob
    (ignored repro); fix by rejecting reserved names on write.
  - `ext_id` on L0 labelled rows is not prefiltered.
  - The trigger pre-image may expose `overflow_json` (unverified).

**Final combined verification (2026-10-18), wave A + wave B.**

| Check | Result |
|---|---|
| fmt | Passes; mod ordering fixed by `cargo fmt` |
| clippy `-D warnings` | Clean, including `--features failpoints` |
| Workspace nextest | 7845/7846; the 1 failure is the stale teeth patch below, since fixed |
| Failpoints | 179/179 |
| Locy TCK | 530/530 in both modes |
| Cypher TCK | 3925/3926 in both modes (0 failed) |
| rustdoc `-D warnings` | Clean |
| Python | uni-db 1027 passed; uni-pydantic 273; pyo3 loader 35/35 |
| Parity, doc and static scripts | Pass |
| `random_programs_match_the_oracle` | 3/3 |

- **Teeth patch.** `compaction_property_loss.patch` is regenerated against the
  `restore_ext_id_column` helper, plus the overflow/timestamp restores. With it
  applied, `vacuum_preserves_schemaless_properties` fails; reverted, the file
  is byte-identical. All 7 patches apply, and the teeth test passes.
- **No temporary probes remain.** The only open-defect `#[ignore]` is the
  `overflow_json` property-name collision.

## Wave C (2026-10-18)

**Locy ABDUCE / ASSUME runtime (items C-L1..C-L6): done, uncommitted.**
- **`RemoveEdge` deleted every edge of its type, so ABDUCE NOT accepted the wrong
  edits. Fixed.**
  - Validation ran `MATCH (a)-[e:R]->(b) DELETE e` with no identity, because
    `match_properties` was filled only for String bindings.
  - With `A1` holding two `R` edges, a single removal came back `validated`.
  - `RemoveEdge` now carries `eid`, `source_vid` and `target_vid`. The mutation
    is pinned with `id()`, and the source/target follow the edge's direction.
  - A hop that can be identified neither way is not proposed.
  - Tests: `locy_abduce_identity::abduce_remove_{named,anonymous,incoming}_edge_removes_only_that_edge`.
    Revert: all 3 fail.
- **`ChangeProperty` was applied to every node, and to nodes instead of an edge.
  Duplicates under `a._all_props`. Fixed.**
  - It now carries `vid` or `eid`. An edge is set through `()-[e]-()`.
  - Only the user's (undotted) entity variables are read, in sorted order.
  - Tests: `abduce_change_property_changes_only_that_entity` (zeroing `b.k` no
    longer validates; no dotted or duplicate candidates) and
    `abduce_change_edge_property_changes_the_edge`. Revert: both fail.
  - `locy_is_ref_binding::is_ref_binding_abduce_follows_the_binding` passed only
    because of the delete-all bug: its goal matched two facts, which no single
    removal ends. Its goal now names one fact (`AND t.name = 'b'`). It still
    fails if the tree stops at `via`.
  - API: new fields on `Modification`, and new keys in the Python dicts.
- **"`evaluate_abduce` reads the top-level store inside ASSUME": no observable
  defect.**
  - The `ctx.lookup_derived` read fed only the unused `_matching` parameter. The
    tree and validation already used the passed store, so the read was removed
    as dead code.
  - Two things keep any ABDUCE-in-ASSUME issue from being observed:
    - An ASSUME body discards ABDUCE results; only QUERY and Cypher rows are
      surfaced.
    - ABDUCE validation inside ASSUME re-derives only the parent's strata, so an
      ABDUCE of a body-local rule would validate against no facts.
  - Whether to surface ABDUCE results from ASSUME bodies is a design decision.
- **`plan_time` now counts ASSUME/ABDUCE re-planning.**
  - `run_strata_native` returns its plan time and the adapter accumulates it.
  - 60 strata + 8 affecting ASSUMEs: 3.14 ms before (equal to the program
    alone), 28.8 ms after. `plan_time <= total_time` still holds.
  - Test: `locy_program_plan_time_counts_assume_replanning`. Revert: fails.
- **Reused strata are no longer planned.**
  - `LocyPlanBuilder::with_reused_strata` emits a stub: names and YIELD columns,
    no clause bodies.
  - 8 ASSUMEs reusing all 60 strata: 26.7 ms → 3.4 ms (program alone 3.2 ms).
  - Test: `locy_program_plan_time_skips_reused_strata`. Revert: fails.
  - Delta seeding: not done. It needs per-clause match deltas that no operator
    computes, and is unsound for deletions, negation and aggregates.
- **Exact WMC, silent wrong answer, fixed.**
  - PROB was looked up under the YIELD alias, but pre-fold facts carry the fold
    variable.
  - `FOLD prob = MNOR(..) YIELD prob AS pr` gave 0.7 instead of 0.79.
  - Test: `wmc_shared_lineage_exact_path_with_renamed_fold_output`. Revert: 0.7.
- **Exact WMC positional reads, fixed.**
  - `detect_shared_lineage`, `apply_exact_wmc` and both provenance recorders
    grouped and hashed facts by position. They now resolve columns by name and
    fail closed on a missing column.
  - Tests: `exact_wmc_resolves_key_and_hash_columns_by_name` and
    `exact_wmc_fails_closed_on_missing_yield_column`.
- **TopK mixed-support group: already fixed by 430ea620a, now tested.**
  - Test: `topk_mixed_support_group_folds_unsupported_rows_independently`
    (0.76). Revert: 1.0.
- **Two ignored e2e tests removed, with pointers left in their place.** Each
  asserted a value no fix could produce:
  - 1.0, on a group that is not mixed;
  - 0.745, against the TCK's 0.79.
- **Open.**
  - Exact mode and the IS-reference probability (0.745 vs 0.79): a semantics
    decision.
  - Positive ABDUCE `AddEdge` validates through `MATCH (a), (b) CREATE ...`, a
    cross product with no labels. Candidate (by inspection, unverified): this
    over-approximates. A fix needs concrete endpoint enumeration.
  - `find_clause_for_row` and the TopK support map still index by position.
- **Tests.**
  - Locy TCK: 530/530 in both modes.
  - uni-locy-oracle: 46/46.
  - uni-query: 881/881.
  - Locy integration: green except `locy_facts_and_filters_query_where_matches_cypher`
    (QUERY WHERE `^` error text) and `locy_operator_parity_matches_cypher`
    (22 disagreements). Both are in the expression evaluator, which other
    agents are editing.

**Wave C outcome: Cypher expression-evaluation defects from the B3 matrix
(plan L1273-1281). Done, uncommitted.**
- **The matrix now asserts.** `locy::locy_operator_parity` used to list Cypher
  defects and pass anyway. It now fails when any literal or property form of a
  pairing disagrees, when a form panics, or when a form differs from the
  openCypher rule. Unary `NOT`/`-` forms are checked the same way.
- **Coverage.** The matrix grew from 10 to 13 operand types (`localdatetime`,
  `localtime`, `time`): 10,274 expressions. Edge cases now carry openCypher
  answers.
- **Before and after.** The matrix had 236 Cypher oracle defects before; it has
  0 now. `cypher_read::expression_value_semantics_test` has 10 tests, one per
  class.
- **Root causes, by item:**
  1. **`n.missing AND null` panic.** `compile_binary_op` built DataFusion's
     `BinaryExpr::And` over a `Null`-typed right operand, and DataFusion
     panics on that. A NULL left operand failed to plan instead. Fix: a
     CypherValue operand of AND/OR goes to the new `_cypher_and` /
     `_cypher_or` UDFs; an untyped NULL operand is cast to Boolean.
  2. **About 150 errors on NULL operands, in three classes:**
     - `coerce_binary_expr` cast an untyped NULL to the other operand's type,
       so `'ab' + null` reached DataFusion's arithmetic coercion. That NULL is
       now folded to NULL.
     - `^` was `power(cypher_number(l), cypher_number(r))`, which decoded
       `'ab'` before looking at the NULL. It is now `_cypher_pow`.
     - `=~` went to DataFusion's regex operator with a LargeBinary or Null
       operand. It is now `_cypher_regex`, with `regexp_match` kept for two
       strings.
  3. **Temporal and string arithmetic, literal vs row path.** Arrow kernels
     answered `date - date` as an Int and rendered a duration as `1 days`. They
     had no kernel for a datetime/time struct or for `duration * n`.
     `needs_cypher_temporal_arithmetic` and `build_cypher_plus` now route those
     to `_cypher_*`. `cypher_arith_return_type` declared `Float64` for
     `2.5 * duration`; that is fixed. On the row path, `eval_add` had no
     `string + scalar`.
  4. **`n.s =~ 'ab'`.** Fixed under item 2.
  5. **`true OR n.i`.** DataFusion's batch short-circuit decided this, so the
     answer depended on batch composition. The UDF evaluates per row.
  6. **`eval_binary_op` vs DataFusion.** `eval_binary_op` is now the
     openCypher definition:
     - `^` is a float.
     - AND/OR: a dominating operand wins in either position. Otherwise a
       non-boolean is a type error, so `NULL AND 1` is an error. The TCK
       rejects a non-boolean literal at compile time (Boolean1-3 [8]); Locy
       now applies the same literal check in `locy_eval::eval_expr`.
     - XOR rejects a non-boolean beside NULL.
     - String predicates over a non-string are NULL.
     - `string + scalar` concatenates.

     Locy's four local overrides are deleted.
- **Found along the way, and fixed:**
  - **A silent wrong answer.** On the CypherValue × Int64/Float64 fast path,
    `n.dur * n.k` was NULL. A non-numeric value answered NULL; it now falls
    back to the evaluator (`CvArithOutcome::Fallback`).
  - **Numbers as durations.** The row path read a number as a duration in
    microseconds, so `date - 1` returned the previous day. That is now a type
    error, as in openCypher.
  - **Local datetime comparisons.** `localdatetime = datetime` compared
    instants on literals. `'ab' = localdatetime(..)` failed parsing a
    timestamp.
  - **Negation.** `-localdatetime(..)` negated the epoch offset.
- **Revert checks.**
  - With every source hunk reverted (scoped patch, restored byte-identically),
    all 10 new tests and the matrix fail on assertions. The AND/OR test
    panics inside DataFusion, which is the defect itself.
  - Targeted: reverting the physical AND/OR routing fails the batch, panic
    and Kleene tests.
  - Targeted: reverting `Fallback` fails
    `non_numeric_property_against_an_integer_column_is_not_null` (NULL
    durations).
- **Tests run:**
  - uni-query + uni-query-functions lib tests: 794/794.
  - uni-db integration, from a frozen nextest archive: 3428/3432. The 4
    failures are in other agents' in-flight areas (bulk/fork store, a
    parenthesized Locy MATCH). They passed in the previous archive run, and
    my only change since was an error string.
  - Cypher TCK, every scenario, both modes: 3925/3925.
  - Locy TCK, both modes: 530/530.
  - clippy `-D warnings` is clean on my code. uni-query is blocked only by
    another agent's doc-comment lint at `expr_compiler.rs:1593`.
- **Open, not taken:**
  - `=~` is a partial match (`'ab' =~ 'b'` is true). Neo4j matches the whole
    string. Both Cypher paths and Locy agree on partial, so this is a
    decision for the user.
  - `'2020-01-02' + duration('P1D')` still does date arithmetic on the row
    path for legacy string-encoded temporals; the literal path concatenates.
  - Docs updated: `website/docs/guides/cypher-querying.md`, black book,
    release notes.

### Wave C — storage and write path (2026-10-18)

**C-S1 `overflow_json` / storage-column names: fixed (rejection).**
- **Measured.** Every name in the storage schema builders was written as an
  undeclared property and read back on a declared label, a schemaless label and
  an edge, unflushed and flushed. `ext_id`, `eid`, `src_vid`, `dst_vid`, `op`,
  `type`, `labels`, `props_json` round-trip, so they stay writable.
  `overflow_json` reads NULL while `properties()` holds it. `_vid`, `_uid`,
  `_eid`, `_deleted`, `_version`, `_labels`, `_created_at`, `_updated_at` read
  NULL on a declared label, are hidden from `keys()` on a schemaless one, or
  return the system value (`n._vid` = vertex id, `n._created_at` = timestamp).
  `SET n._deleted = 7` failed with an Arrow type error.
- **Fix.** `WRITE_RESERVED_PROPERTY_NAMES` + `check_written_property_names`
  (`uni-common/src/core/schema.rs`). The check runs in every `Writer` vertex and
  edge insert path, in `BulkWriter::insert_vertices`/`insert_edges` (regardless
  of `validate_constraints`), and at plan time for a literal `SET x.k` target
  (`planner.rs::reject_reserved_set_target`). The error is
  `UniError::InvalidIdentifier`. It is tagged across the executor's string
  boundary (`RESERVED_PROPERTY_TAG`) and recovered in `impl_query.rs`.
  Transaction bulk inserts and the appender no longer flatten it to `Internal`.
- **Found on the way and fixed: silent wrong answer.** On an edge type declared
  with no properties, every flushed edge with an undeclared property reported
  `overflow_json` (the blob as byte values) in `keys(r)`, `properties(r)` and
  `RETURN r`, and a `SET` staged it back. Cause:
  `PropertyManager::merge_overflow_into_props` stored the blob in the map when
  the planner requested `overflow_json`.
- **Tests.** The ignored repro became
  `bugs::ext_id_on_labelled_nodes::..._reserved_storage_names_are_refused_at_write`.
  It covers 9 names × 12 Cypher forms, plus both bulk inserts, `BulkWriter` and
  the appender, with a no-trace check before and after flush.
  `..._flushed_edge_does_not_expose_overflow_blob` covers the leak.
  `..._other_reserved_names_round_trip` now also covers `type`/`labels`/`props_json`.
  A refused write is now a failure there, where it used to be skipped.
  Python: `test_e2e_errors.py::test_storage_column_property_name_refused`.
- **Revert checks.** Check stubbed: the refusal test fails. Blob insertion
  restored: the leak test fails.
- **Open (decision).** Other `_`-prefixed names (`_id`, `_foo`) are accepted at
  write, while DDL refuses them. Measured: such a name is dropped on a declared
  label and hidden from `keys()` on a schemaless one. Refusing the whole prefix
  matches DDL, but it is breaking. `bugs::user_maps_are_not_entities` writes
  `_id` as a stored property today. Options: refuse all `_` names at write, or
  make the read path honour them.

**C-S2 trigger pre-image: real, fixed.**
- **Cause.** `PreExistingProbe::extend_with_l1` (`uni-plugin-host/src/triggers.rs`)
  copied every column. A flushed vertex's `old_value` carried `overflow_json`
  (undeclared properties nested under it), `_deleted`, `_uid`, `_created_at`
  and `_updated_at`.
- **Fix.** Skip `_` columns, unpack the blob, and let declared columns win.
- **Test.** `triggers::trigger_dispatch::update_pre_image_matches_between_l0_and_flushed_vertex`.
- **Revert check.** Fails with the full storage map.
- **Open.** Flushed edges still have no L1 pre-image (L0 only, as documented).

**C-S3 `ext_id` on unflushed labelled rows: fixed.**
- **Measured.** `rows_scanned` 2000/8000 → ≤ 1 for both spellings.
- **Cause.** The `ext_id` pushdown is Lance SQL only, so there was no runtime
  filter to recover a prefilter from.
- **Fix.** The planner hands the scan an `L0PropertyPrefilter`
  (`GraphScanExec::with_planner_l0_prefilter`, which was the schemaless-only
  field). `columnar_scan` treats `ext_id` as a `STRING` column.
- **Tests.** `index::l0_equality_prefilter::unflushed_ext_id_lookup_reads_only_matching_rows`,
  and `ext_id_shadowing_cases_match_an_unpushed_reference` (L1/L0, tx, rollback,
  fork, non-string ext_id).
- **Revert checks.** Fallback removed: the counter guard fails (2000). The
  differential fails on an over-eager prefilter. The shadow set is not
  load-bearing here: with it emptied the differential stays green, because
  `build_ext_id_property_column` re-reads L0.
- **Found on the way and fixed: a freed `ext_id` stayed taken forever once its
  holder was flushed.** This applied to SET away, REMOVE and DELETE, even after
  re-flush, on CREATE, SET, UNWIND CREATE and bulk insert. Causes:
  `find_by_ext_id(s)` took the newest row *carrying* the id, and the writer
  never asked L0 whether a newer version superseded it. The batch path also
  read stale cross-buffer L0 versions. Fix: `current_ext_id_holders`,
  `Writer::l0_ext_id_verdict`, and newest-version-only key collection in
  `validate_vertex_batch_constraints`. Tests:
  `index::batch_extid_uniqueness::freed_ext_id_can_be_taken_again_in_every_storage_state`
  (12 states × 4 paths) and `held_ext_id_is_refused_in_every_storage_state`
  (anti-vacuity). Revert check: the freed test fails.

**C-S4 W3b `WHERE` on an undeclared label: fixed.**
- **Measured.** `MATCH (n:M) WHERE n.p = v` read 2000/8000 rows, and now reads
  ≤ 1. This holds for `=`, `$p = n.p`, `IN`, `AND`, and both cross-product
  orders. OPTIONAL MATCH still null-extends.
- **Fix.** `ScanMainByLabels` is now a target in `rewrite_target_reachable`,
  `optional_scan_reachable` and `push_predicate_to_scan`. The cross-product arm
  of `push_predicate_to_scan` now asks `rewrite_target_reachable`. Without that,
  the predicate went to the side that does not bind the variable and was lost
  (measured with the check removed: 2000 rows instead of 1).
- **Test.** `index::l0_equality_prefilter::unflushed_where_on_undeclared_label_reads_only_matching_rows`.
  The existing schemaless shadowing differentials now run through the pushed
  form.
- **Revert checks.** Arm removed: the counter fails. Side check removed: the
  row count fails.
- **O(V).** Confirmed by timer (debug): the candidate pass was 1.6 ms at 2k and
  7.5 ms at 8k. It now records membership instead of a per-vid verdict map and
  walks the smallest label set: 1.24 ms and 5.3 ms. The walk itself remains,
  for the same no-L0-index reason as before.

**C-S5 bulk residuals.**
- **Abort on a fork: reproduced and fixed.** Abort errored, and the 20 aborted
  rows stayed on the fork. They were also visible mid-load.
- **Found and fixed: data loss on primary.** An interrupted fork load
  (writer dropped) made reopen roll **primary** back. A primary vertex
  committed and flushed meanwhile was lost (3 → 2). A primary table the fork
  load saw absent was dropped (1 → 0). Cause: each fork flush wrote the
  database crash marker with `BranchedBackend::get_table_version`, which is
  primary's.
- **Fix.** `ForkBranching::{stages_on_branch, stage_on_branch, publish_on_branch,
  discard_on_branch}` (Lance impl in `lance_branch.rs`). `BranchedBackend`
  implements staging on the fork's branch. `BulkWriter::writes_crash_marker`
  is false on a fork (no `Active`/`Committed` marker, and no `clear`).
- **Tests.** `fork::fork_bulk_load::*` (4): abort ×3 states, commit
  (fork-only, traversal), interrupted reopen ×2 labels, and a publish failure
  via `FAIL_AFTER_PERLABEL_WRITE`.
- **Revert checks.** Staging off: the abort and publish tests fail.
  Marker kept on forks: the publish-crash test fails (primary 3 → 2).
- **Crash mid-publish (primary): reported, not changed.** The marker is written
  `Active` just before the first publish commit. Recovery restores each
  published table, and the touched L2 adjacency tables, to its pre-publish
  version. So it also discards any other writer's commit to those tables
  inside the publish window. A fork has no reconciliation at all: a failed
  publish leaves per-label published and main not (pinned in the test).
- **One marker per database: reported, not changed.** The write guard is per
  session, so two sessions can bulk-load at once. B's `Active` overwrites A's,
  and either load's commit clears the other's marker. Per-load markers would
  not suffice, because version rollback of a shared table also discards the
  other load's rows. That needs fragment-targeted rollback (a design change).

**Verification.**
- uni-db integration: 3431/3438. The 7 failures are another agent's in-progress
  untracked tests: 6 `cypher_read::numeric_exact_comparison_test` and
  `locy::locy_abduce_identity::abduce_add_edge_validates_one_edge_not_every_pair`.
  The latter still fails with my writer changes stubbed out.
- uni-store + uni-common + uni-bulk + uni-plugin-host: 1054/1054.
- Failpoints lane: 180/180, plus 25/25 non-feature.

**Wave C outcome: lifecycle and background tasks (DeferralQueue fsync, fork
`ShutdownHandle`, revert tests, "commit timed out").** Uncommitted.

- **1. `DeferralQueue::push` fsync under the queue lock (L1028).** Real.
  - Measured with `deferral_push_contention::concurrent_deferrals_are_all_persisted`
    (uni-plugin-host; 8 threads x 25 `dispatch_before` deferrals, sidecar on):
    before, 6.6-10.2 s wall, per-commit p50 243-385 ms, a reader blocked on the
    queue lock for up to 473-656 ms; after, 2.2-2.7 s, p50 51-83 ms, reader
    waits 0.2-2.8 ms. p99 is still 275-470 ms: a push behind a write waits for
    that write plus its own.
  - Contract (struct docs): no transactional tie between a deferral and its
    commit, but `push` persisted before returning. Kept: `push` still returns
    once the sidecar holds the item.
  - Fix (`triggers.rs`): snapshot under the queue lock, encode and write
    outside it, under a separate write lock holding the last written
    generation; a push already covered by a later write skips its own.
  - Tests: `the_sidecar_write_does_not_hold_the_queue_lock` (test seam inside
    the write; `pending()` must return), `concurrent_pushes_are_all_persisted`.
    Revert check: re-taking the queue lock around the write makes the first
    red (`Err(Timeout)`).
  - Open: the write still runs on the committing task's executor thread.
- **2. Fork/snapshot `ShutdownHandle` never signalled (L1029).** Real.
  - Repro: `trigger_dispatch::async_fire_on_a_fork_is_counted_and_awaited_by_shutdown`,
    `..._awaited_by_drop`, `background_task_lifecycle_test::fork_worker_is_counted_and_stopped_by_primary_shutdown`,
    `fork_worker_stops_when_the_fork_is_dropped`. All four red before the fix
    (fire not counted; `drop` returned with the fire running; fork rebuild
    worker not counted).
  - Fix: `ShutdownHandle::derive` (own signal, parent's task list; the
    parent's shutdown fires derived signals recursively) used by
    `UniInner::derived_clone`; `Drop for UniInner` calls `release` (signals
    only that view's tasks, so a live nested fork keeps its own). The
    same-fork shared `Arc<UniInner>` invariant is untouched: release runs
    when the last sibling session goes.
  - `release` is not independently revert-tested: with it removed the fork
    drop test still passes, because dropping the handle's last clone closes
    the channel and the rebuild worker treats `Closed` as shutdown. It is
    kept so stopping does not depend on each task handling `Closed`. Unit
    tests `parent_shutdown_reaches_derived_handles`, `release_does_not_cascade`.
  - Found on the way: each `async_indexes(true)` bulk load built its own
    `IndexRebuildManager` and worker (3 loads left 3 live tasks:
    `bulk_loads_with_async_indexes_share_one_worker`). Fixed with
    `BulkBackend::rebuild_manager` (the view's one manager). Revert check:
    red (3 tasks).
- **3. Revert tests for the earlier un-revert-tested fixes.** All four now
  have a test that is red with the fix reverted:
  - compaction `:757` propagation: new fail point `main_edge::exists-by-eid`;
    `compaction_resilience::main_edges_read_failure_in_the_invariant_check_is_an_error`
    (uni-store, failpoints, debug builds only since the check is
    `cfg(debug_assertions)`). Reverted to `unwrap_or(false)`: the pass panics
    "EID 0 … not found in main_edges".
  - cancel in `shutdown_in_place`:
    `compaction_lifecycle_test::shutdown_cancels_the_pass_in_flight`
    (fail point holds the pass at a safe point until cancelled). Reverted:
    shutdown waited 25.6 s and the pass was not cancelled.
  - detached-spawn removals: new fail points `index_rebuild::save-state` and
    `index_rebuild::execute`; `a_failed_rebuild_writes_nothing_after_shutdown`
    (re-adding the detached retry task: a state write lands after shutdown,
    5 vs 4) and `post_flush_rebuild_scheduling_writes_nothing_after_shutdown`
    (re-adding the detached `schedule` spawn: 3 vs 2).
  - deferral tick on the blocking pool:
    `trigger_defer::a_slow_deferred_fire_does_not_stall_a_current_thread_runtime`.
    Inline tick: the runtime stalled 2.0 s.
- **4. "commit timed out" under four probes (L206-207).** Not reproduced: 12
  runs of 4 concurrent `default_disk` W2 probes x 8,000 commits, then 4 x
  6,000 and 8 x 6,000 instrumented, load average up to 30; no timeout.
  - Instrumented (temporary prints, removed): the background auto-flush
    (`check_flush` → `flush_to_l1`) held `flush_lock` for up to 0.94 s across
    its Lance write; commits waited for the lock up to 1.47 s (budget 5 s).
    Waits longer than any measured flush hold also occur, so the async
    finalize (which takes `flush_lock`) and scheduling delay contribute.
  - Mechanism shown deterministically with an injected slow Lance write:
    `a_slow_background_flush_does_not_time_out_commits` (commit_timeout 1 s,
    write held 2.5 s → `CommitTimeout`). Fix: `Writer::check_flush_in_background`
    rotates under the lock and writes through the flush coordinator, as the
    commit path does. Revert check: red. That this was the cause of the one
    original failure is inferred, not confirmed.
  - Test note: a tokio-timer wait in that test does not wake while the
    injected `std::thread::sleep` blocks a worker, so the test waits on its
    own thread instead.
  - Open: the async finalize still holds `flush_lock` while publishing.
- **Suites.** uni-plugin-host 87/87; uni-store + uni-bulk 775/775; failpoints
  lane (`-p uni-db -p uni-store --features failpoints --run-ignored all`,
  resilience/recovery/durability/crash_harness/lifecycle filter) 219/219;
  uni-db runtime/triggers/connectors/fork/bulk 314/314. Full uni-db
  integration: 3439 run; 179 failed to exec because a concurrent rebuild
  replaced the binary, and passed on rerun; 2 failed for value reasons outside
  this area (`constant_in_list_known_answers`, `9007199254740993 IN
  [9007199254740992.0]`; `abduce_add_edge_validates_one_edge_not_every_pair`,
  a test file another agent added today). Clippy: no warnings in uni-bulk,
  uni-store, uni-db or uni-plugin-host. `-D warnings` itself was blocked by
  other agents' in-progress uni-cypher / uni-query warnings.

### Wave C — Cypher planner items (L1427–L1435, L1304, L795, L1295)

- **UNWIND'd entity with a label (L1427): reproduced, fixed.** Wider than
  reported: any `LabelCheck`/`hasLabel` on a whole-entity column failed
  (`MATCH (a:L)`, `EXISTS { MATCH (a:L)… }`, `WHERE a:L`, `RETURN a:L`).
  - Cause: both compile against a flat `{var}._labels` column
    (`df_expr.rs` `LabelCheck`/`HASLABEL`).
  - Fix: `expr_compiler.rs::resolve_flat_column_properties` rewrites the
    check to a new `_cypher_has_labels` UDF (`df_udfs.rs`) when the schema has
    `var` but no `var._labels/_type/_vid/_eid`. The UDF takes either encoding,
    nodes and edges.
  - Test: `bugs/unwound_entity_label_check.rs` (3 tests, both modes, flushed
    and L0).
- **`coalesce(r['w'], 0)` ambiguous (L1429): reproduced (schema mode), fixed.**
  The wildcard edge-property expansion (`df_planner.rs`, schema'd traverse)
  listed `w` once per edge type, which made two `r.w` columns. It is now
  deduplicated. Test: `bugs/edge_wildcard_shared_property.rs`.
- **In-query `CALL p(x)` without YIELD (L1431): reproduced, fixed.** The
  planner dropped the clause (a declared WRITE procedure made 0 markers).
  - A void procedure now plans as `Apply` and runs once per row.
  - A procedure with results needs `YIELD` inside a query (openCypher and
    Neo4j). This is a new `SyntaxError`; before, the call was silently
    skipped.
  - Found on the way:
    - Sub-plans were built without the transaction's writer, so a per-row
      WRITE procedure failed loudly (`common.rs`).
    - `Apply` memoised procedure calls, so `UNWIND [1,1,1] … uni.create.uuid()`
      gave 1 distinct uuid.
    - A unit `CALL {}` repeated its input row once per body row: 6 rows
      instead of 2 (`apply.rs`).
- **`plan_apply` stringifies yields (L1433): reproduced, fixed.** `result + 1`
  gave `"11"`. `infer_logical_plan_schema` now takes the procedure registry.
  The new `procedure_call::declared_yield_field` (plugin signature, then
  algorithm provider, then legacy outputs) types the yields; the standalone
  `build_schema` uses it too.
  - Test: `bugs/in_query_procedure_call.rs` (7 tests).
- **WITH…LIMIT…WHERE (L1435): not reproduced.** Probed label scans with and
  without a BTree index, flushed and L0, projections, a following MATCH, a
  `~=` KNN and an ORDER BY over similarity. All filter the page.
  - Pinned by `cypher_read/with_where_after_page_scans.rs`. The test fails
    when `where_after_page` is forced off.
- **QPP inner-source label (L1304): reproduced, fixed.** Worse than "first
  iteration only". With an outer node before the group, the label was never
  checked. Unquantified `(a) ((x:M)-->(y)) (b)` ignored `:M` as well.
  - Fix: a departure constraint, `NfaTransition::source_constraint` via
    `PathNfa::with_iteration_source_constraint`, checked in
    `expand_neighbors`. It is carried as `QppStepInfo::source_label`.
    `use_simple_vlp` no longer swallows a labelled inner source or target.
    Two labels on the source are refused.
  - Found on the way: an inner target label the schema does not declare was
    dropped, so `(y:Nope)` matched every node.
- **QPP outer-source filter sites (L795): reproduced, fixed (W4 class).**
  `MATCH (a:L) OPTIONAL MATCH (a) ((x {id:1})-[:R]->(y)) (b)` dropped 3 of
  4 rows. Under OPTIONAL, the inner-source filter on a bound source now runs
  after the walk as an `OptionalFilterExec` over the clause's variables.
  - Test: `cypher_path/qpp_inner_source_constraints.rs` (4 tests;
    variable-length oracle).
- **New, found while probing item 1: schemaless rebound relationship (silent
  wrong answer), fixed.** `MATCH ()-[r:R]->() WITH r MATCH (x)-[r:R]->(y)`
  with `R` undeclared gave 16 rows for 4 edges, or ignored the type. The
  schemaless branch now walks `__rebound_r` and adds the `_eid` filter, like
  the schema'd path. Both share `rebound_relationship_check`.
  - Test: `bugs/rebound_relationship_schemaless.rs`, with the declared-schema
    run as the oracle.
- **Non-constant shortestPath edge maps (L1295): not implemented; design
  only.** The allow-set is built once per operator at warming. A row-dependent
  map needs per-row conditions inside `GraphShortestPathExec`:
  1. Carry the non-walk-constant entries on `LogicalPlan::ShortestPath`.
  2. Compile them against the input schema.
  3. Per batch, evaluate them per row and group the rows by the condition
     key.
  4. Build or cache an `EidFilter` (or `ReachableMapFilter`) per key,
     charged to the reservation.
  5. Key the retained source BFS by (source, key).
  6. A NULL value admits no edge.

  The oracle must be the minimum length over the filtered variable-length
  paths, not `shortestPath(...) WHERE all(...)`, which filters after the
  search. Estimate: about 300 lines in `shortest_path.rs` plus plumbing.
- **Revert checks.** Every fix was toggled off in place, scoped to my hunks,
  and restored. Every new test went red on an assertion; the writer revert
  gives a loud error.
- **Tests run.**
  - uni-query + uni-query-functions: 1201/1202. The one failure,
    `repro_finding_06_regex_null_collapses_to_false`, is in the concurrent
    `df_expr` regex work, not in these changes.
  - uni-db integration full run: the first pass had transient mass failures
    under concurrent load. Every failed test was rerun: all pass, except
    `locy_warded_parenthesized_path`. That one was a regression from the
    first `use_simple_vlp` change, fixed by allowing the plain path when
    there is no quantifier and no outer target.
  - Cypher TCK (Match|With|Unwind|Call|Quantif|Path|Pattern|Return|Optional):
    1744/1744 in each mode.
- **Open, outside this area.**
  - `size()` of an algorithm's list yield counts characters, standalone and
    per row. `cv_encoded` Utf8 is not decoded by `size()`.
  - A declared WRITE procedure's writes bypass the transaction: they survive
    `rollback()` and are invisible inside the tx before commit.
  - `range(0, null)` raises instead of returning null. Unverified against
    the spec.
- Cypher TCK, both modes: 3925/3925.
- Python: 1031 passed, 15 skipped (new `test_storage_column_property_name_refused` ×4 included).
- Clippy `-D warnings`: clean on every file touched here. The workspace run is
  red only in other agents' in-flight hunks: `uni-cypher` walker,
  `uni-query-functions` float_cmp, `locy_fixpoint`, and `df_planner` around
  line 10535.

**Wave C outcome: Locy runtime performance and positional reads (BEST BY merge,
non-linear scans, provenance by name, positive ABDUCE): uncommitted.**
- **`merge_best_by` re-sorted all facts every merge: fixed.**
  - Each merge concatenated the facts with the candidates, lexsorted
    everything, and rebuilt the key map and the row-dedup set: O(F log F) per
    merge.
  - `FixpointState::best_index` now maps each KEY to its held row and rounded
    criteria. A merge compares the best candidate per KEY with the held row
    (`make_comparator` on exact criteria) and filters a displaced row out of
    the one batch holding it. Tail batches merge binary-counter style. Tie and
    rounding rules are unchanged: the held row wins an exact tie, and a
    candidate whose rounded criteria match does not displace it.
  - Measured with `perf_class_probe_best_by_merge` (40 layers x 500, skip
    edges so held rows are improved; 19,500 facts, 40 iterations):
    `LocyBestByMerge` 4.4 s → 0.73 s over the run, with identical facts (19,500)
    and delta rows (209,500). Wall time on this contended box: 34–43 s →
    14–21 s.
  - What remains is not the merge. BEST BY is still about 9x plain reach
    (1.6–2.0 s) on this graph because it derives 209,500 delta rows to plain
    reach's 19,500: each node improves about 10 times (Bellman-Ford order).
  - Test: `recursion_semi_naive_best_by_matches_dijkstra_with_improvements`
    (a cyclic graph with late improvements, checked against Dijkstra). All
    existing BEST BY and semi-naive guards pass.
- **Non-linear old/full scans: measured, not implemented. Decision needed.**
  - `perf_class_probe_nonlinear_full_scan`: TC (`a IS tc TO m, m IS tc TO b`)
    on a 128-chain plus an inert bipartite block that never composes.
    - Recursive iterations: 343–438 ms with no inert facts; 856–879 ms with
      90,000 inert facts.
    - `DerivedScanExec` rows: 51,308 → 1,581,308. `HashJoinExec` output is the
      same 341,376 in both cases.
    - 9 iterations either way: non-linear TC converges in log(diameter).
  - An index would cut this by up to about 2x, and only when a large part of
    the relation never joins. The change is a new physical operator: a
    persistent join index per derived scan, grown with each delta, with old
    and full views. It would replace `HashJoinExec` for Old/Full feeds. That
    is an architecture decision, not a local fix.
- **`find_clause_for_row` and the TopK support map indexed by position: fixed.**
  - `find_clause_for_row` compared columns by position, skipped any candidate
    batch whose width differed, and gave a fact no clause matched to clause 0.
    It was also O(delta x candidates).
  - Now `clause_attribution` builds a by-name map once per
    `record_provenance` call. A missing column or an unmatched fact is an
    error.
  - The TopKProofs `body_support_map` is now a `BodySupportMap` that hashes
    named columns. FoldExec resolves them in its own batch, and a missing
    column is an error.
  - Tests (unit): `find_clause_for_row_matches_candidates_by_name`,
    `find_clause_for_row_fails_closed`,
    `topk_support_map_resolves_columns_by_name_under_reorder` (0.8 instead of
    0.6 under the positional hash), `topk_support_map_fails_closed_on_a_renamed_column`.
    Revert: all 4 fail.
  - No end-to-end column reorder was found within the fixpoint. Clause batches
    reach the merge in one column order, so these were latent.
- **Positive ABDUCE `AddEdge` over-approximated: reproduced, fixed.**
  - Validation ran `MATCH (a)(b) CREATE (a)-[:T]->(b)`: an edge between every
    pair of nodes.
  - On `(a)-[:R]->(b)-[:R]->(c)` with no edges, each single `AddEdge` came
    back `validated: true`, though no single edge derives the goal.
  - Candidates are now concrete. `AddEdge` gains `source_vid` and
    `target_vid` (Rust and the Python dicts), and validation pins both
    endpoints with `id()`.
  - Endpoints come from the hop's labelled nodes in id order. The goal `WHERE`
    filters them when it reads only that hop's endpoint variables. The total
    is capped by `max_abduce_candidates`.
  - Hop direction is honoured: `(b)<-[:R]-(a)` adds `a -> b`.
  - Tests: `abduce_add_edge_validates_one_edge_not_every_pair` and
    `abduce_add_edge_proposes_concrete_endpoints` (only `A -> C` validates).
    Revert (unpinned validation): both fail.
  - The enumeration policy is a choice the user may revisit: label-scoped
    pairs, filtered by the goal, in id order, within the budget. A goal that
    needs an edge to a node late in id order can fall outside the budget when
    the `WHERE` does not filter the hop.
- **Tests.**
  - Locy integration: 410/410 on the second run. On the first run,
    `assume_cost_does_not_scale_with_unaffected_strata` and
    `locy_entity_hydration_edge_cost_does_not_grow_with_the_graph` (wall-clock)
    failed. Both passed in isolation, in 5 repeats and in the rerun. The
    first test's failure message was not captured.
  - Locy TCK: 530/530 in both modes.
  - uni-locy-oracle: 46/46 on the first run.
    - Open: `random_programs_match_the_oracle` failed once in 15 runs. It
      draws a clock seed per run, and the failing run's seed and divergence
      were not captured (only the summary line was kept).
    - The other 14 runs passed, 12 of them in isolation.
    - It is not attributed to this change or excluded from it. Re-run with
      `--no-capture` until it fails, then replay it with
      `ORACLE_RANDOM_SEED`.
  - uni-query lib: 498/498.
  - uni-locy: 224/224.

**Wave C outcome: numeric (int/float) value semantics (plan L1357-1361). Done, uncommitted.**
- **Oracle.** `cypher_read::numeric_exact_comparison_test`, 9 tests. Expectations
  come from an exact `i64`/`f64` comparison, not a transcription. Values: around
  2^53, `i64::MIN`/`MAX`, ±2^63, ±0.0, NaN, ±inf. Every check runs on literals,
  on parameters, and on typed, indexed and schemaless properties, in L0 and
  after a flush. It covers 1,008 predicate forms per flush state, projected
  values, 3,456 literal pairs, column pairs, hash joins, `IN`, `ORDER BY`,
  `min`/`max`, DISTINCT, grouping and UNION.
- **Item 1: int vs float widened to f64. Reproduced on every path; fixed.**
  - The row evaluator (`cypher_eq` / `cypher_partial_cmp`, expr_eval.rs) went
    through `as_f64`. The new `cypher_numeric_cmp` wraps `uni_common::cmp_i64_f64`.
  - DataFusion path: `float_comparison` cast the integer operand to `Float64`.
    It now casts to `Int64` (`float_comparison_operand_type`), and
    `_cypher_float_*` accepts `Int64` on either side (float_cmp.rs). The physical
    compiler had the same cast (expr_compiler.rs).
  - Literal native form: `native_float_comparison` compared a widened integer
    column. It now rewrites to the operand's own type, which keeps L0 prefilter
    and index pushdown exact:
    - `integer_bound`: `x < 2.5` becomes `x <= 2`.
    - `float_bound`: `y < 2^53+1` becomes `y <= 2^53`.
    - `=` / `<>` against an unrepresentable value keeps the UDF.
  - CypherValue fast paths widened: `cv_bytes_as_f64` in `compare_cv_numeric`,
    `compare_cv_vs_i64`, and CV×CV in `try_fast_compare`. They now go through
    `compare_numbers`. List ordering (`cypher_value_cmp`) is fixed too.
  - Join keys needed no change. The bucket is coarse, and the re-check is now
    exact.
  - Storage pushdown was already type-exact (`index_operand_fits_column`,
    `indexed_range_filter`).
- **Item 2: `1` and `1.0` as one value. Reproduced; fixed.**
  - Neo4j keeps the first value seen (`Distinct` emits the original row, and
    `IntegralValue`/`FloatingPointValue` hash and compare numerically). The TCK
    does not pin it.
  - `_cypher_value_key` now folds an integral float that fits `i64` to that
    integer (`canonical_number`).
  - A CypherValue group key (Distinct, Aggregate, UNION dedup) is emitted through
    a new `_cypher_first_seen` UDAF over the original column, via
    `equivalence_group_key` / `plan_equivalence_distinct` in df_planner.rs. So
    `UNWIND [1.0, 1] AS x RETURN DISTINCT x` gives `1.0`, and `[1, 1.0]` gives `1`.
  - The `collect(DISTINCT)` key canonicalizes numbers.
  - `RETURN 1 AS x UNION RETURN 1.0 AS x` failed to plan ("mismatched schemas").
    Int-vs-float union columns are now encoded as CypherValue
    (`coerce_union_entity_columns`).
  - Typed `Float64` keys still show the canonical float, as in wave B.
- **Item 3: `[1, n.f]` widened by `make_array`. Did not reproduce.**
  - `list_of_int_and_float_expression_keeps_element_types` covers
    `[2^53+1, n.f]`, `[n.f, 1]` and `[n.i, 0.5]`, in L0 and flushed. It passed
    before any change in this wave. No fix was made.
- **Item 4: VLP edge L0 float checks. Real, but not L0-specific.**
  - The schemaless VLP walk (`GraphVariableLengthTraverseMainStream`,
    traverse.rs) matched inline edge maps with structural `Value ==`, and
    nothing re-checks it. `[:R*1..2 {w: 0}]` missed `w = 0.0`, and NaN matched NaN.
  - It now uses `map_matches` (Cypher equality), as the declared-type walks
    already did.
  - Same class in pattern predicates (`pattern_exists.rs`): `(a)-->(:B {k: 1})`
    missed `k = 1.0`, and `{k: null}` matched a node without `k`. Fixed with
    `cypher_eq`.
  - Declared-type VLP, shortest path and fixed-hop edges were already correct.
- **Found along the way: a planning error, fixed.** `n.i IN [m.i]` (any types)
  failed with "No field named m". `PredicateAnalyzer::is_pushable` (pushdown.rs)
  accepted any `Expr::List` as constant. It now requires literal/parameter items
  (`is_constant_list`).
  - Test: `in_list_of_another_variables_property_stays_above_the_scan`.
- **Revert checks.** Each fix was reverted on its own, by a scoped string swap
  restored byte-identically, and its tests failed on assertions:
  - row/CV evaluator;
  - UDF operand cast;
  - literal integer bound;
  - CV fast paths;
  - CV key folding;
  - first-seen;
  - UNION coercion;
  - schemaless VLP map;
  - pattern-predicate map;
  - pushdown list check.

  An `IN`-list change in `build_cypher_in_list` was dropped. Its `InList` is only
  built for `._vid` correlation keys, so a revert changed nothing.
- **Updated tests.** Three asserted the defect, `2^53+1 = 2^53.0` true, and now
  assert the exact answer:
  - `in_list::constant_list_known_answers`;
  - `join_key::integers_that_round_to_one_float_share_a_bucket`;
  - `constant_in_list_test`'s known-answer table.
- **Open.**
  - Not converted, because no reproduction was built: Locy's `values_equal` /
    `value_less_than` (locy_eval.rs) and the legacy executor's
    `cypher_cross_type_cmp` still widen.
  - `canonical_numeric_key` (executor/core.rs) and `canonical_value_key`
    (executor/read.rs) admit `f = 2^63` through `<= i64::MAX as f64`, which
    saturates.
  - `sum(DISTINCT x)` over a CypherValue column sums the canonical keys:
    `[1.0, 1]` gives `1`, not `1.0`.

### Wave C — test-harness hygiene and release notes (2026-10-18)

- **`topo_rewrites_unflushed_small_batches` under load: reproduced, fixed.**
  - Repro: 36 concurrent copies of the test binary plus a build. 34 of 36
    failed, every one with the same message:
    `[locy_negation] Operation timed out after 30000ms`. The query was
    `MATCH (a:Person), (b:Person) WHERE … AND NOT EXISTS { MATCH
    (a)-[:KNOWS*1..]->(b) }`. With 12 concurrent copies, 48 of 48 passed.
  - Root cause: the fixture inherited the default 30 s `query_timeout`. That
    reference query evaluates its unbounded VLP once per pair (52 × 52), at
    5.9 s alone with the two-row batch and 3.8 s with the default batch. It
    crosses 30 s once the box is oversubscribed. The seed is fixed and the
    answers never diverged; the "divergence" was the deadline.
  - Fix: `build_topo` sets `query_timeout` to 300 s
    (`metamorphic/dqp/topo.rs`). The six-minute nextest limit still catches a
    hang or a runaway rewrite.
  - A/B under identical load (24 old + 24 fixed copies per round, two rounds):
    the old binary failed 25 of 48 runs, all on the same timeout; the fixed
    binary passed 48 of 48, with a slowest run of 302 s.
  - Open: costing 5.9 s for 2,704 pairs is a perf finding. The correlated
    `NOT EXISTS` over a VLP is not decorrelated into one reachability pass per
    distinct `a`. It is not fixed here.
- **`collect_ids` swallowed errors: the propagation was already in place.**
  - `collect_ids` and `collect_property_snapshot` already used `?`, and
    `run_query_step` panics on `Err`.
  - Three fail-open paths remained:
    - `collect_ids` skipped a non-integer id.
    - `add_entity_to_snapshot` skipped a non-entity value, or a map with no
      identity.
    - `get_labels` skipped a row that did not decode.
  - All three now return an error.
  - The seam is an in-module test: the helpers are private, but they take the
    query string. The blocked placeholder
    `tests/repro_collect_ids_swallows_error.rs` is replaced by five unit tests
    in `src/world.rs`: three refusals and two healthy-path controls.
  - Revert check: restoring the old forms fails all three refusal tests.
  - Full Cypher TCK, with the stricter helpers: schemaless 3925/3926 and
    sidecar 3925/3926 (1 skipped), 0 failed in both. The counts are the same as
    the previous run, so no scenario was passing on a swallowed error.
  - The script rewrote `compliance_reports/{schema,schemaless}/`. Those files
    are excluded from commits.
- **Stale ignore comments:**
  - `l3e_vlp_path_edges_set`: passes, and asserts `count(e.flag) = 2` on edges
    created without `flag`, so a no-op SET fails it. The doc said the SET
    no-ops and the test was ignored; both corrected.
  - `repro_rule_registry_lost_update`: not ignored; it passes. The doc now says
    it is a race detector whose detection of a reverted fix is likely, not
    certain. This was reasoned, not measured: `impl_locy.rs` is being edited
    by another agent. A dead `reproduced_at` variable was removed.
  - `correctness_repros.rs` header (L8) and Locy section (L1606): no test is
    ignored. The comments now say so, and name the uni-db / uni-query
    behavioral guards for [25], [24], [13] and [10].
    - Gap: no test covers multi-hop `ABDUCE NOT`.
  - Tightening the vacuous arms found one dead test. `repro_19` asserted
    nothing (it only printed); it now asserts the filter. The `Err` arms of
    [16], [35], [19-COPY], [24], [13] and [10] printed and passed; they now
    panic. [13]'s program (`… OUTPUT PROB e.w`) had never parsed, so that test
    had passed without planning anything; its syntax is corrected. 41/41 pass.
  - `repro_vertex_props_unresolved_vid_fan_out`: the stale line references
    and "the defect" docs are corrected. Revert check: disabling the L0 label
    lookup in `get_batch_vertex_props` fails 2 of 3 tests (the control still
    passes); the file was restored byte-identically.
- **`repro_finding_06_regex_null_collapses_to_false`: the test was out of
  date; the engine was correct.**
  - The test evaluated the bare translation in a plain `SessionContext`, and
    `=~` now translates to a `_cypher_regex` placeholder: "UDF not
    registered".
  - The test now runs the planner's pipeline: translate, resolve, coerce,
    resolve. It asserts NULL on both bindings: native string match (Utf8) and
    the CypherValue UDF.
  - Revert check: making `native_regex_match` drop its NULL guard fails it.
- **Release notes:** added entries for:
  - the memory CDC provider no longer being registered by default, with the
    `MemoryCdcOutputProvider` and `delivered` type changes;
  - `LocyResult::rows()`;
  - `vector_similarity` thresholds no longer using the index (from
    `2650428b2`);
  - the compaction file lock / 30 s writable-open wait (from `5ebf6f0c6`, not
    marked breaking in its commit).
  - The mandatory-`shortestPath` entry was already present.

### Wave C — procedure-call defects found by C3 (2026-10-18)

- **WRITE procedure writes bypassed the transaction: reproduced, fixed.**
  - In a `tx`, `CALL t.mark()` (declared `WRITE`, body `CREATE (:Marker)`)
    survived `rollback()`. Other sessions saw it before `commit()`, and the
    transaction's own reads did not. This held standalone and per row.
  - Two transactions that bumped one counter through a procedure both
    committed, with n = 2. The same plain `SET` refuses the second commit.
  - Cause: `QueryProcedureHost::execute_inner_query`
    (`procedure_host.rs`) built the inner `Executor` with the writer but no
    `transaction_l0_override`. Mutations went to the writer's main L0, and the
    tx buffer was only appended to the read list.
  - Fix: the inner executor takes `l0_context.transaction_l0` as its
    transaction L0. It is no longer duplicated into the pending list.
  - Outside a transaction nothing changes: `Session::run` auto-commits, and a
    declared trigger's after-commit action (no tx buffer) writes main L0.
  - Test: `bugs/write_procedure_in_transaction.rs` (4 tests: rollback, read
    your writes plus isolation, autocommit, conflict parity with plain
    writes). Revert: 3 fail on assertions. The autocommit test passes either
    way, as expected.
  - Other write procedures checked:
    - `uni.periodic.submit` and `iterate` run their own session and
      transaction through `SchedulerControl`, so they commit independently,
      as APOC's do. Left as designed.
    - WASM, Rhai and PyO3 procedures have no graph-write channel through the
      procedure host. Their `write` mode is a declaration only.
    - The algorithm adapters run their inner queries in `Read` mode.
- **Algorithm list yields were text: reproduced, fixed. Node yields had the
  same defect.**
  - `size(clique)` gave 7 for 3 ids. The cause was
    `procedure_call::json_to_value`: arrays and objects became
    `Value::String(json)`, written into a `Utf8` column (`value_type_to_arrow`).
    Only `record_batches_to_rows` parsed the text back, through `cv_encoded`.
  - Fix: a new `procedure_call::algo_yield_field` is now the algorithm
    adapter's yield typing.
    - List, map and path yields are `LargeBinary` CypherValue columns tagged
      `cv_encoded`.
    - Node and relationship yields are `Int64` ids.
  - The other parts of the fix:
    - `build_typed_column` gained a `LargeBinary` arm (codec encode).
    - `build_algo_record_batch` converts structured JSON with `Value::from`.
    - `declared_yield_field` keeps the provider field's metadata.
  - Consumers covered by tests, standalone and per row: `size`, `UNWIND`,
    `[i]`, `IN`, list comprehension, `keys()` on the map yield, and
    `WHERE id(t) IN clique`.
  - Node yield: `articulationPoints … YIELD node MATCH (p) WHERE id(p) = node`
    returned `[]`, because `node` was the string `"1"`.
  - Test: `bugs/algorithm_list_yield_values.rs` (3 tests). Revert: the list
    tests give 7 instead of 3, and the node test gives `[]`.
- **`range(0, null)`: not changed.**
  - The openCypher TCK (`List11`) has no null case. It requires
    `InvalidArgumentType` only for non-integer types and `NumberOutOfRange`
    for step 0.
  - The Neo4j Cypher Manual's `range()` section says nothing about null.
    Its signature types every argument `INTEGER`.
  - The current error (`InvalidArgumentType … must be an integer`) deviates
    from neither. Returning null instead would follow the general
    null-in/null-out convention. That is a design choice for the user.
- **Found, not fixed.**
  - `uni.plugin.declareProcedure` has no argument-names parameter, but
    `SyntheticProcedurePlugin` reads `arg_names` from the signature JSON. So a
    declared procedure cannot take arguments: a body using `$k` fails with
    `Unresolved parameter: $k`. Fixing it is an API change (a new argument).
  - `isBipartite` yields `partition`, a reserved word (window `PARTITION
    BY`). `YIELD partition RETURN partition` fails to parse; it needs
    `AS p` or backticks.
  - `uni.algo.diameter` always yields `path = null` (its `map_result` emits
    `Value::Null`).
  - `AlgoProviderBridge` (`uni-plugin-builtin/src/algorithms/bridge.rs`)
    writes list yields as `LargeBinary` serde_json bytes, not the CypherValue
    codec. No `CALL` reaches it for a static algorithm, because the plugin
    procedure resolves first. Its consumers were not checked.
  - Inner procedure reads in a tx use the live main L0, not the
    transaction's pinned snapshot (`L0Context` has no `SnapshotView`).
    Candidate: a SNAPSHOT-isolation gap for procedure bodies. Unverified.
- **Tests run.**
  - uni-db integration, `bugs|plugin|algo|graph_algo|triggers|session_tx|ssi_*|scratch_tx|hooks|auth|cypher_read|cypher_write|e2e`
    plus `procedure`/`trigger` names: 1740/1745. The 5 failures are
    `bugs::mixed_type_hash_join` (3),
    `cypher_read::numeric_exact_comparison_test` (untracked, in progress) and
    `bugs::issue_269…an_unflushed_vid…`. None of these issues a `CALL`, and
    every changed line is on a procedure path. These belong to the
    join-key/comparison and bulk work in flight.
  - Unit tests for uni-query, uni-plugin-host, uni-plugin-custom and
    uni-plugin-builtin: 914/914.
  - Cypher TCK `~Call`: 52/52 in each mode, plus 1 skipped.
  - Python (`maturin develop`, then transaction, procedure, algo, plugin and
    error files): 288 passed, 10 skipped. A probe confirmed the built wheel
    returns 3 for `size(clique)`.
  - `cargo clippy -D warnings`: clean for uni-query and for uni-db
    (`--lib --test integration`).

### Wave C — identifier quoting (L1330–L1332)

**Done, uncommitted.**
- **Doubled-backtick escape (measured, fixed).** `` `a``b` `` was a parse error,
  so no name containing a backtick could be written. New shared pest rule
  `escaped_name` (openCypher `EscapedSymbolicName`) used by `identifier`,
  `identifier_or_keyword`, `alias_identifier`, `locy_identifier`, and by
  `parameter` (`` $`p q` ``, which the openCypher grammar allows).
  `normalize_identifier` undoubles the escape.
- **Walker sites kept backticks (candidate → confirmed, fixed).** Variables
  (pattern, UNWIND, FOREACH, comprehension, quantifier, REDUCE, SET/REMOVE
  targets, map-projection selector), `WITH RECURSIVE` name, procedure names,
  parameters, index/constraint names, DDL property names (`CREATE LABEL`,
  `CREATE EDGE TYPE`, `ALTER … PROPERTY`, composite constraint lists), and Locy
  generator names/outputs and `prev` fields all used raw `as_str()`.
  `` MATCH (`n`) RETURN n `` failed to plan; `` CREATE LABEL L (`my prop` STRING) ``
  declared `` `my prop` ``. `extract_property_name` split the raw text on `.`
  (`` p.`a.b` `` → `` b` ``). All now go through `walker::ident`.
  Test: `uni-cypher/tests/repro_escaped_identifier.rs` (6; quoted AST == bare
  AST over 40 query pairs, escape cases, Locy). Revert: grammar 4 fail, `ident`
  5 fail, unescape step 3 fail, `extract_property_name` 1 fail, Locy generator
  1 fail.
- **Storage round trip: two declared names Lance cannot hold (found, fixed by
  refusal).** A declared `a.b` was accepted and took writes, then every flush of
  the label failed (`Top level field a.b cannot contain .`). A declared
  `` has`tick `` flushed, then every read of the label failed (`No field named`):
  `Scanner::project` escapes the name, but lance-datafusion's
  `ColumnCapturingVisitor` passes the unescaped root name to the field-path
  parser, so the column is never loaded. Pinned by
  `uni-store` `storage::lance_backtick_column_probe` (fails, by design, when
  Lance fixes it). `validate_property_name` now refuses `.` and backtick in a
  declared name; undeclared they work via `overflow_json`.
  Revert: `unstorable_property_names_are_schemaless_only` fails.
- **Refused `CREATE LABEL` / `CREATE EDGE TYPE` / `SchemaBuilder::apply` left the
  label behind (found while testing, fixed).** Properties are now validated
  before the schema is touched. Revert of the DDL pre-check: "label D leaked".
  The builder pre-check covers property *names* only; a builder failing for
  another reason part-way (e.g. a type conflict) still leaves earlier changes.
- **`quote_cypher_identifier`** (`get_label_info` / `get_edge_type_info`) now
  escapes instead of refusing a backtick. Revert: `quoted_ddl_names_round_trip`
  fails.
- **Lance filter pushdown:** unchanged. B6's `quote_ident` already refuses a
  backtick name and leaves it to the residual filter; the end-to-end test
  filters on an undeclared `` has`tick `` flushed and in L0.
- **EXPLAIN / Python:** EXPLAIN carries the unescaped names (asserted).
  Python: `TestQuotedIdentifiers` in `test_cypher_features.py` (2), result keys
  are unescaped (`"x`y"`).
- **VLP `edge_lance_filter` (confirmed dead, removed).** Rendered in
  `df_planner.rs` (two sites), threaded through both VLP execs and streams,
  never read (`#[expect(dead_code)]` since the first release). The VLP property
  map is enforced per edge by `edge_property_conditions`. Wiring it as a
  preselection would need an L0 union and a design; not done.
- **Tests run.** uni-cypher integration 53/53; uni-cypher + uni-locy +
  uni-locy-tck + uni-common + uni-query 1972/1973 (the one failure was my
  new docs snippet using `--` as a comment; fixed to `//`, uni-cypher re-run
  53/53); uni-store
  storage/schema 113/113; uni-db integration 3458/3458 (first pass: 321 exec
  failures because another build replaced the binary mid-run, plus one
  timeout under load; all 322 re-run green); TCK schemaless 3925/3925,
  sidecar 3925/3925; pytest cypher features 23, schema suites 63. Clippy
  `-D warnings` clean on touched crates except two lints in files outside
  this area (`float_cmp.rs:617`, `df_planner.rs:10558`).
- **Open.**
  - A declared property with `.` or a backtick could be supported by encoding
    column names for Lance; refused for now (design choice).
  - `SchemaBuilder::apply` is not atomic for non-name failures.
  - The pydantic OGM interpolates names into Cypher unquoted (not audited).

### Wave C — planner/executor: shortest-path row maps, QPP region maps, IN column runs (2026-10-18)

- **Non-constant shortestPath / allShortestPaths maps (L1295, C3's design):
  implemented.** `shortestPath((a)-[:R*1..4 {w: a.x}]->(b))` was refused.
  - Planning: `planner.rs` splits the map; walk-constant entries stay in
    `edge_filter_expr`, the rest go on the new
    `ShortestPath`/`AllShortestPaths.edge_row_conditions`. A value may name the
    endpoints and earlier variables. One naming the path or the relationship
    variable is refused. The property collector reads `a.x` from these values.
  - Execution: `df_planner.rs` compiles each value against the operator's
    input. `GraphShortestPathExec` evaluates them per batch and groups rows by
    (source, value set). Each value set gets its own allow-set (the constant
    entries plus that set's values), a `ReachableMapFilter` covered over that
    set's sources. The sets are kept across batches in `keyed_filters` and
    charged to the reservation. The retained source search is keyed by
    (source, value set). A `NULL` value admits no edge, without a read.
  - Oracle: `cypher_path/shortest_path_row_dependent_map.rs`. It takes the
    minimum length, and for `allShortestPaths` the number of minimum-length
    trails, of the variable-length match with `all(r IN rs WHERE …)`. Cases:
    7 maps × 3 directions × 3 hop ranges × 2 binding forms, random graphs
    (parallel edges, self-loops, missing `w`, `NULL` `x`), schema and
    schemaless, flushed and unflushed. A per-relationship check confirms every
    returned relationship satisfies the map. A hand-worked ring checks
    grouping, OPTIONAL and the refusals. Before the fix, 270 of 315 cases per
    run failed with the refusal; the 45 that passed are the constant-map
    controls.
  - Revert checks: grouping every row under one value set gives 159 failures
    plus the ring. Reusing the retained search across value sets gives 78
    failures. Both are assertions.
  - `vlp_property_map_oracle::non_constant_map_values_that_must_gate_a_walk_are_refused`
    no longer lists the two shortestPath queries; they are answered now. The
    QPP refusals stay.
- **QPP per-hop maps pre-scanned the whole type (L496, L1076): fixed.**
  - `ReachableMapFilter` is generalized to hop slots
    (`traverse.rs`, `ReachableMapFilter::for_steps`). Each hop of the
    iteration has its own types, direction, edge map and fail-closed vertex
    map. The pass tracks `(vertex, slot)` at the shallowest depth, and the
    simple pattern is the one-slot case. The fall-back builds every slot's
    whole-type filters.
  - Counters: `perf::profiler_blind_spots::qpp_property_map_reads_only_the_reachable_region`
    replaces the old ">= 2n" guard. It pins `rows_scanned = 1 + region edges`,
    computed from the graph: 11 at N=200 and at N=800. With the QPP branch
    reverted it reads 401, and the test fails.
  - Answers: `cypher_path/qpp_region_map_oracle.rs` compares 4 shapes
    against spelled-out one- and two-iteration patterns: edge and vertex maps,
    mixed and undirected hops, the same type on both hops. It runs over 8
    random graphs, mixed L0/L1, then all flushed. Schemaless is not covered,
    because a multi-hop QPP needs declared types (an existing refusal).
- **IN over a list column repeated in runs (L1145-1146, IC5): fixed.**
  `in_list::invoke_column_list` splits the list column into runs of identical
  rows (equal bytes, or equal Arrow values). It decodes each run once and
  answers a run of 2+ rows from an `InListIndex`, a single row by the scan.
  - Counter (unit test): 512 rows in runs of 4,096 items made 2,097,152
    comparisons on the scan, and now no more than the 256-item case.
    `list_column_runs_cost_does_not_grow_with_list` fails with the path
    disabled.
  - Time (debug, 8 runs × 1,024 rows, IN cost above a control carrying the
    same lists): 40 / 149 / 613 ms → 6 / 16 / 91 ms at 256 / 1,024 / 4,096
    items. The remainder is the per-row byte comparison of the replicated
    list. Not measured on LDBC IC5 itself.
  - Answers: three-valued, with `NULL` lists and lists holding `NULL`, against
    the scan (`list_column_runs_match_the_scan`) and against
    `any(y IN fs WHERE y = x)` (`perf/in_list_column_runs.rs`).
- **Tests run.** cypher_path + perf + cypher_read: 907/907. uni-db
  integration full: 3460/3460. uni-query + uni-query-functions: 1208/1208.
  TCK `/Match|Path|Quantif|List/`: 1268/1268 in each mode. Clippy
  `-D warnings` is clean on uni-query, uni-query-functions and uni-db
  (lib + integration).

### C12: two intermittent failures (`random_programs_match_the_oracle`, `assume_cost_does_not_scale_with_unaffected_strata`)

- **`random_programs_match_the_oracle`: reproduced, 2 failures in 104
  clock-seeded runs.** Both were coverage-floor misses, and neither was an
  engine/oracle divergence. Across about 10,000 cases there were 0
  divergences. Seed `1792828304592699810`: `AmbiguousExpected in 1 of 96
  (bounds 3%..20%)`. Seed `1792839557312118029`: `cost`/`tot` non-empty in
  18 of 96 (floor 20%). Replaying them oracle-only reproduces both, so the
  cause is the generator alone.
  - Root cause: the floors were set too close to the generator's own rates.
    `AmbiguousExpected` comes from family 4 (50%) × variant < 15, which gives
    7.5%. A binomial at 96 cases falls below 3 about once in 45 runs, and
    because the band is two-sided, no per-case rate does better than once
    in about 250 (best at 10.5%). `cost` was non-empty in 36% of cases (miss
    about once in 3,000). `ScalarKeyStr` reached 29% against a 15% floor
    (about once in 1,100).
  - Fix, with no floor lowered:
    - `random.rs` `draw_case` and `AMBIGUOUS_EVERY = 10` give an exact quota:
      1 case in 10 is redrawn until it has the ambiguity, the rest until they
      lack it. The redraw is capped at 2,000 draws, so the floor still fires
      if the generator loses the shape.
    - `generator.rs` weights `along` to 0.65 (`cost` now 48%) and draws the
      scalar-KEY kinds at 75/70 (`ScalarKeyStr` now 38%).
  - Measured with an oracle-only probe (scratch, removed) over 96-case
    batches:
    - Before: floor misses in 9 of 600 and 5 of 200 batches.
    - After: 0 of 500. The summed binomial miss probability of the remaining
      lower floors is about 5e-5 per run.
    - Post-fix end to end: 36/36 runs green, and the `uni-locy-oracle` suite
      passed 46/46.
- **`assume_cost_does_not_scale_with_unaffected_strata`: mechanism
  reproduced.** Running it concurrently (and in the full locy filter) doubled
  `rows_scanned` for one arm at n=8000 in 5–12 of 40 runs: 48012 vs 24006,
  with l0/storage = 32005/24006. Seeding crosses `auto_flush_threshold`
  (10k), and the 5 s `auto_flush_interval` timer can fire at either scale
  under load. While a flush is in flight, a scan reads the rows from both
  the pending L0 and the Lance version already written. MVCC dedup keeps the
  results correct, but the counters count both tiers
  (`merge_lance_and_l0`, `crates/uni-store/src/runtime/columnar_scan.rs`).
  The window runs from the Lance writes (`flush_stream_l1`) to
  `complete_flush` (`writer.rs` phase J). The test fails when the doubled
  read lands in the ASSUME arm and not in the plain one.
  - Fix: `db.flush().await?` (a drain barrier) after seeding, so both arms
    read quiescent tiers. With it, 0 of 40 concurrent runs doubled, and all 3
    full locy runs gave a stable `extra = 10` at both scales.
  - A class survey of every counter-asserting test under
    `crates/uni/tests/common` found 3 more exposed to the timer:
    `dqp::counters::l0_reads_zero_when_flushed_and_nonzero_when_dirty`
    (exact), `profiler_blind_spots::vlp_property_map_region_read_counts_l0_edges`
    (exact difference) and
    `issue_175_index_consulted::an_unflushed_query_reports_no_lance_scan`.
    Each needs its data in L0, so each now builds with
    `auto_flush_interval: None`. Targeted suites pass 50/50.
  - Still open, a design question: reads during a flush do the work twice.
    Removing that would mean readers skip a pending L0 whose table is
    already committed at or below the Lance version they read, and that
    needs per-buffer, per-table committed versions. This was not built.
- **`locy_entity_hydration_edge_cost_does_not_grow_with_the_graph`: did not
  fail** in 3 full locy runs under the oracle loops' load. Ratios were
  0.014/0.012, 0.010/0.003 and 0.008/0.002 against a 0.05 guard, and the
  control ran 418–3060 ms. The design is already ratio-based (min of 3), and
  its fixture flushes, so the flush window does not reach it. There is no
  evidence for changing it.
- **Seen in the full locy runs, outside C12's area and left alone (files under
  active edit):** `locy_operator_parity_matches_cypher` failed in all 3 runs
  (`[9007199254740993] = [9007199254740992.0]`: Cypher returns true, the
  oracle wants false; the third run timed out at 180 s).
  `locy_numeric_exact_recursive_best_by_orders_by_value` failed in run 1 only
  (`BEST BY w ASC` gave `[-1.0, 3.0, 2.0]`, wanted `[-1.0, -1.0, 2.0]`).

**Wave C outcome: numeric follow-ups (C2's open list: Locy comparisons, the
fallback executor, DISTINCT aggregates). Done, uncommitted.**
- **Item 1: Locy `values_equal` / `value_less_than` widened. Reproduced; fixed.**
  - Rule `WHERE`, `QUERY WHERE`, FOLD `MIN`/`MAX` and the scalar IS-reference
    join were already exact (shared evaluator and DataFusion path). They are
    pinned as controls in `locy::locy_numeric_exact`.
  - `QUERY ... ORDER BY` and `QUERY RETURN min/max` went through `value_cmp`,
    which widened, and tied any two values of different types. `2^53 + 1`
    tied with `2^53.0`, and `i64::MAX` with `2^63`. `value_cmp` is now
    `cypher_value_order` (Cypher's `ORDER BY` order), and `value_less_than`
    uses `cypher_numeric_cmp` (locy_eval.rs).
  - `QUERY RETURN DISTINCT`, `count(DISTINCT)` and `collect(DISTINCT)` kept `1`
    and `1.0` apart: 4 rows where Cypher gives 3. They now key on
    `canonical_cypher_value` (newly public in float_cmp.rs) and keep the first
    value seen (locy_query.rs).
  - `values_equal` (EXPLAIN's in-memory IS-ref matching) is now exact. Only a
    unit test shows the defect: the DataFusion join builds the facts exactly,
    so no fact can carry a colliding key.
  - **Found: BEST BY sorted a CypherValue criterion by its encoded bytes.**
    Every integer ranked below every float, so `BEST BY v` over `5` and `1.5`
    chose `5`, and negative numbers ranked above positive ones.
    - Fixed in `BestByExec` with `locy_best_by::order_column` (Cypher sort keys).
    - Fixed in the recursive merge `merge_best_by` (`criteria_comparators`,
      `held_cmp`): `(a, c)` held `3` from iteration 1 against `-1` from
      iteration 2. Before the fix this failed 10 of 10 runs; after, 30 of 30
      passed. It is deterministic.
    - The one "intermittent" failure in C12's logs (`locyfull_1.log`) was
      that graph, briefly in the tree before the fixpoint fix was applied.
  - **BEST BY criteria in the fixpoint merge are resolved by name.** They
    were resolved by position, and a position past the schema's end was
    silently dropped. `SortCriterion::column` is resolved in the facts'
    schema, and an absent column is a plan error
    (`resolve_best_by_columns`).
  - The parity matrix gained near-2^53 edge cases.
    - **Found: typed Cypher list comparison widened.**
      `[9007199254740993] = [9007199254740992.0]` was true. Cause:
      `coerce_mismatched_types` cast a `List<Int64>` and a `List<Float64>` to
      `List<Float64>` (df_expr.rs).
    - Fix: an integer list against a float list is no longer cast. It goes to
      the list UDFs, which compare elements exactly.
    - The matrix measured 90 s alone and 180 s in the full suite, where it
      hit the default three-minute kill. The nextest ceiling is now six
      minutes (`.config/nextest.toml`, default and ci).
- **Item 2: fallback executor. Reproduced; fixed.**
  - Public path: a standalone `CALL` of a registered (non-DataFusion)
    procedure runs the whole statement on the fallback executor
    (`is_ddl_or_admin` recurses into children).
  - Reproduced there:
    - `min`/`max` widened (`cypher_cross_type_cmp`, now `cypher_numeric_cmp`);
    - `DISTINCT`, grouping and `UNION` folded `2^63` into `i64::MAX`
      (`canonical_value_key`);
    - only `count` honored `DISTINCT`; `sum`, `avg`, `min`, `max`, `collect`
      and the percentiles folded every row. Now there is a generic
      `Accumulator::Distinct` keyed on `canonical_cypher_value`.
  - `canonical_numeric_key` had the same saturation, and it also keys
    **MERGE** on the main path. `MERGE (:K {k: 9223372036854775808.0})`
    matched the `i64::MAX` node, and `UNWIND` MERGE deduplicated the two.
  - New shared helper `uni_common::f64_to_exact_i64`, bounded to
    `[-2^63, 2^63)`. `numeric_to_value` and `canonical_number` use it too.
- **Item 3: `sum(DISTINCT x)` over `[1.0, 1]`. Reproduced; fixed.** The answer
  was `2.0`, not the `1` C2 noted: nothing was deduplicated.
  - The `DistinctUdaf` wrapper (`_cypher_sum` / `_cypher_avg` / `_cypher_min`
    / `_cypher_max` / plugins) compared argument tuples as raw scalars. It now
    dedups by equivalence class and keeps the first tuple
    (`DistinctAccumulator::equivalence_key`).
  - `collect(DISTINCT x)` gave `[1]`, not `[1.0]`.
    `coerce_aggregate_function` replaced the argument with its class key.
    `_cypher_collect` is now exempt, because its accumulator dedups by class
    itself. `count(DISTINCT)` keeps the key, which is harmless for a count.
- **Tests (new).**
  - `locy::locy_numeric_exact` (9).
  - `cypher_read::numeric_exact_comparison_test`:
    `distinct_aggregates_see_the_first_value_seen`,
    `merge_keys_are_exact_at_the_i64_boundary`,
    `fallback_executor_compares_numbers_exactly`,
    `integer_and_float_lists_compare_exactly`.
  - Unit tests: `locy_eval::numeric_tests` (2); `executor::core::tests` (3);
    `locy_fixpoint::tests::best_by_merge_resolves_criteria_by_name_and_fails_closed`.
  - Doctests for `f64_to_exact_i64` and `canonical_cypher_value`.
- **Revert checks.** Each fix was reverted by a scoped string swap and
  restored byte-identically (md5 checked). Its tests failed on assertions:
  - `value_cmp`;
  - Locy DISTINCT;
  - `BestByExec` order;
  - merge comparators;
  - by-name criteria;
  - `DistinctAccumulator`;
  - collect exemption;
  - list cast;
  - `canonical_numeric_key`;
  - `cypher_cross_type_cmp`;
  - `canonical_value_key`;
  - legacy `Distinct`;
  - `values_equal` / `value_less_than` (unit).
- **Verification.**
  - uni-common, uni-query-functions and uni-query lib: 994 and 507 passed.
  - uni-db integration, full: 3472/3475. The 3 failures:
    - `locy_operator_parity` (the list defect above), since fixed and passing;
    - `unanchored_pattern_comprehension_test::a_subquery_in_the_body_still_takes_the_per_row_path`
      and `exists_per_source_test::exists_per_source_matches_per_row_and_brute_force`.
      Both are in the EXISTS/comprehension decorrelation area, where another
      agent's `exists_decorrelate.rs` is in progress. They are not
      investigated here.
  - Locy subset after the fixpoint change: 395/395.
  - Cypher TCK, `/Aggregation|Call|Union|Return|With/`: 700/700 in both modes.
    `/List|Comparison|Equality|Null/`: 381/381 in both modes.
  - Locy TCK: 530/530 in both modes.
  - clippy `-D warnings` is clean on my crates. On uni-query it needed
    `-A clippy::type_complexity`, because of the foreign
    `exists_decorrelate.rs:343`.
- **Open (probed, not fixed).**
  - `percentileDisc` on the fallback executor ignores its percentile
    argument (always 0): `percentileDisc(v, 1.0)` over `[1, 2, 3, 2^53+1]`
    gives `1`.
  - On the DataFusion path, `percentileDisc` folds through `f64` and returns
    a float. The same query gives `9007199254740992.0` for `2^53 + 1`, and
    `3.0` for an integer input.

### Wave C — correlated `EXISTS` over a VLP, once per source (C11 follow-up, 2026-10-18)

- **Reproduced.** `MATCH (a:Person), (b:Person) WHERE NOT EXISTS { MATCH
  (a)-[:KNOWS*1..]->(b) }` over the DQP topology fixture (52 x 52, half in L0,
  two-row batch): 6.25 s in a debug build, 1,383 ms in release; 2,705
  physical plans (the query's own plus one per pair). `rows_scanned` stayed at
  104 throughout, so it cannot see this cost: the per-pair walks read
  adjacency, not scans.
- **Root cause.** `ExistsExecExpr::evaluate` (`df_graph/expr_compiler.rs`)
  physically plans and executes the body once per outer row, with every
  import bound. Reachability mode was already engaged (the body is wrapped in
  `MultiplicityInsensitive`); the cost was 2,704 sub-plan builds and runs,
  each walking out of the same `a` again to test a different `b`.
- **Fix: per-source evaluation** (`df_graph/exists_decorrelate.rs`, wired into
  `ExistsExecExpr`).
  - `BodyShape::analyze` accepts a body of non-optional `MATCH` clauses whose
    references `free_variables` can determine. Its candidates are node
    variables that occur only as bare `(v)`: no label, map, inline or clause
    `WHERE` naming them, and no parameter of that name.
  - At run time, a candidate the outer row binds as a node is left free when
    another outer entity named by the pattern stays bound (otherwise the free
    body would be an unanchored whole-graph enumeration). The body is planned
    a second time against the batch minus `v`'s columns. Rows are grouped by
    a row-encoded key over every column of every other referenced name, and
    one execution per key returns each matching `v`. A row answers from the
    count at its own `v._vid`.
  - Exact for `EXISTS` (both plans keep `MultiplicityInsensitive`) and for
    `COUNT`. Nothing is reinterpreted as reachability, so bounded ranges
    (`*2..3`), undirected trails at `a = b` and `*0..` keep their semantics.
    That covers the walk-vs-trail question in the brief without answering it.
  - A key runs free only on its second sighting (twice in a batch, or seen in
    an earlier batch); a single-row source stays on the cheaper bound path.
    Answers are cached in the expression, which is built per execution, up to
    4M identities. NULL import identities, `COLLECT`, `OPTIONAL MATCH` /
    `WITH` / `RETURN` bodies, quantified groups, `shortestPath` and nested
    subqueries keep the per-row path.
- **Measured.** Fixture query, physical plans 2,705 → 53 (half in L0) and
  2,305 → 49 (flushed). Wall time: debug 6.25 s → 0.29 s; release 1,383 →
  105 ms (half in L0, batch 2) and 504 → 26 ms (flushed, default batch).
  Answers are identical (2,584 / 2,238 / 120 / 66 / 188 / 180).
  `topo_rewrites_unflushed_small_batches` took 56 s and `topo_rewrites_flushed`
  84 s in a loaded run; most of a case is the other relations. The 300 s
  timeout is left as it is, and its comment now records the new figure.
- **Tests** (`cypher_read/exists_per_source_test.rs`):
  - `exists_per_source_matches_per_row_and_brute_force`: 4 random graphs
    (cycles, a self-loop and a parallel edge in each), schema and schemaless,
    flushed and half-L0, batch default / 2 / 3. It covers three directions and
    9 ranges (`""`, `*`, `*1..`, `*1..2`, `*2..3`, `*2..`, `*0..`, `*0..1`,
    `*3`). The checks are: EXISTS against the per-row path and against a
    brute-force trail search; NOT EXISTS as the complement; the
    pattern-predicate spelling; COUNT against the per-row path and the
    top-level enumeration. Six more shapes: a fresh intermediate, `WHERE`
    on `a`, a third import, NULL endpoints from `OPTIONAL MATCH` (EXISTS and
    COUNT), and an outer relationship variable.
  - `exists_per_source_undirected_unbounded`: undirected unbounded ranges on a
    fixed graph. On the random graphs they are exponential, so the random
    graphs skip them.
  - `exists_per_source_cost_does_not_scale_with_targets`: 30 x 30, batch
    default and 2. The per-row form builds exactly 1 + 900 plans; the
    per-source form builds at most 1 + 60.
  - Three `BodyShape` unit tests in uni-query.
  - Revert check: disabling `BatchGrouping::build` fails the cost test (900
    sub-plans for 30 sources). A mutation that drops the grouping key fails
    both oracle tests (`EXISTS per-source vs per-row`). Restored
    byte-identically both times.
- **Not changed.** At first I counted EXISTS executions into
  `subquery_executions`. That broke
  `unanchored_pattern_comprehension_test::a_subquery_in_the_body_still_takes_the_per_row_path`
  (6 against 3), because the metric's meaning had changed. I reverted it, and
  the cost test uses `physical_plans` instead. The earlier full-run failures
  of that test and of `exists_per_source_matches_per_row_and_brute_force`
  (reported above by another item) came from this in-progress state. Both
  pass now.
- **Verification.**
  - uni-query lib: 507/507.
  - uni-db integration, `cypher_read` + `cypher_path` + `bugs` + `perf` +
    `topo`: 1480/1481. The one failure was the counter test above, now fixed
    and passing.
  - Exists tests in debug: 26 s, 14 s and 4 s.
  - Full Cypher TCK: schemaless 3925/3926 and sidecar 3925/3926 (1 skipped),
    0 failed.
  - clippy `-D warnings` is clean on uni-query (`--lib --tests`) and uni-db
    (`--lib --test integration`).
- **Open (observed, not fixed).**
  - An undirected unbounded VLP (`-[:R*]-`) under EXISTS is not run in
    reachability mode. It enumerates trails: about 0.65 s for a
    `count(*)`-sized enumeration on graphs of at most 10 edges, in release.
  - On the same graphs, the per-row form with a `WHERE` inside the body
    (`… WHERE a.id = a.id`) took 2.6–6 s against 0.65 s. One candidate cause,
    unverified, is that a body `WHERE` also keeps the walk out of
    reachability mode.
  - Neither is decided here.

### Wave C — percentiles exact, reachability from distances (C13/C14 follow-ups, 2026-10-18)

- **Item 1: fallback `percentileDisc` ignored its percentile. Reproduced;
  fixed.** `Executor::update_accumulators` (`executor/read.rs`) never
  evaluated the second argument, and `Accumulator::new` fixed the percentile at
  `0.0`. It now reads the first non-NULL percentile per group
  (`Accumulator::set_percentile`, `wants_percentile`), with the same
  `NumberOutOfRange` check as the DataFusion path. The fallback also turned a
  float result into an integer (`numeric_to_value`: `10.0` became `10`); that
  helper had no other caller and is gone.
- **Item 2: DataFusion `percentileDisc` went through `f64`. Reproduced;
  fixed.** Both planners (`df_planner.rs`, `df_expr.rs`) cast the input to
  Float64 before the aggregate. The input now goes in uncast, and one UDAF
  (`CypherPercentileUdaf`, disc and cont) keeps exact `Int`/`Float` values,
  state as Cypher values. Its return type is the input's (`Int64`, `Float64`,
  else a Cypher value). Both executors call one definition,
  `uni_query_functions::percentile` (exact sort with NaN last, index
  `round(p * (n - 1))`, interpolation of two integers in integer arithmetic,
  so it is rounded once). A non-number percentile is now an error on both
  paths; it was read as `0`.
  - TCK `Aggregation6` expects only float inputs, so it did not see this. The
    index rule is unchanged and both paths share it. Neo4j's implementation
    is believed to use nearest rank, which would give `2` for
    `percentileDisc([1,2,3,4], 0.5)` where this gives `3`. That was not checked
    against Neo4j; it is a design question, left open.
  - `bugs::numeric_functions_take_numbers` expected `Float(4.0)` for
    `percentileDisc` over `[-2.5, 4]`. It now expects the input, `Int(4)`.
- **Found while testing item 2: cross-label property type leak. Reproduced;
  fixed.** `GraphScanExec::build_schemaless_vertex_schema` (`scan.rs`) typed
  each property of a schemaless scan from the first label anywhere in the
  schema that declared it. With `F.v FLOAT`, `MATCH (s:S) RETURN s.v` on the
  undeclared `S` read `2^53 + 1` as `2^53`, a string as NULL, and
  `WHERE s.v = 2^53 + 1` matched nothing. Read side only: the writer coerces
  under a vertex's own labels. A property now takes a declared type only from
  the scanned labels (agreeing). `MATCH (n)` is always the stored value.
  - Locy's planner mirrored the old rule (`undeclared_property_arrow_type`). It
    now returns `LargeBinary`, as its own comment said it should.
  - Locy's `LargeBinary → Int64` coercion (`coerce_physical_expr`) read
    through Float64. It now uses a new exact `_cypher_integer` UDF; a float
    still truncates.
  - Without these two Locy changes, `locy_schemaless_property_type_inference::partially_declared_schema_undeclared_label_round_trips`
    and `locy_value_types_recursive_max` failed.
  - The same first-wins lookup remains in `df_graph/common.rs`
    `infer_expr_type` (correlated-subquery output schemas). It is open and not
    probed.
- **Item 3: undirected unbounded VLP under EXISTS. The premise was wrong; the
  cost was real and is fixed.** Instrumenting the dispatch showed that
  `Reachability` mode *was* engaged for `-[:R*]-` under EXISTS, directed or
  not, with or without a body WHERE.
  - The cost was inside that mode. On a cyclic graph (every undirected one with
    an edge) `bfs_endpoints_only` kept a frontier to the hop ceiling. Each depth
    then ran an exponential `has_trail_valid_path` search. Debug build: 295 ms
    for 7 sources on a 10-edge graph, 3 s for a 5-clique, more than 150 s for a
    6-clique.
  - The schemaless `GraphVariableLengthTraverseMainExec::bfs` ignored the
    output mode and listed every trail: a 6-clique took 7 s, or 20 s for
    `DISTINCT`.
  - Fix (`df_graph/trail_reach.rs`, `PathNfa::simple_chain_range`): for a plain
    `[:T*min..max]` in Trail mode, decide from BFS distances. A shortest walk is
    a trail, so for `v != s` the answer is `min <= dist <= max`. The shortest
    closed trail is `1 + min over e of dist_{G-e}(u, s)`.
  - Undecidable cases fall back per source: an accepted endpoint whose shortest
    trail is shorter than `min`, i.e. a longest-trail question.
  - The fallback search is now capped at the number of relationships reachable
    from the source. It used to go to the ceiling and then emit a false
    "default hop bound … results are incomplete" warning on any cyclic graph.
  - The schemaless exec uses the same function (`reachable_endpoints`).
  - Measured: 10-edge graph 295 ms to 1.4 ms (debug, VLP time); 4×K5 + K6 runs
    in under 1 s, against more than 150 s / 7 s before.
- **Item 4: body WHERE. Did not reproduce as stated.** With and without
  `WHERE a.id = a.id`, the VLP ran in `Reachability` mode with equal VLP time.
  The guarded form is slower (11 ms to 28 ms here) because it takes C14's
  per-row path: `b` is named in the WHERE, so the body runs per pair. The walk
  mode is not the cause. C14's 2.6–6 s against 0.65 s is consistent with the
  old per-call search being multiplied by per-row execution, but that was not
  re-measured on the old code.
- **Tests.**
  - `cypher_read::percentile_exact_test` (3): disc over ints, floats, mixed
    and 2^53 sets on UNWIND, typed and schemaless columns, the fallback, L0 and
    flushed, and grouped; cont exactness; bad arguments.
  - `cypher_read::schemaless_property_type_scope_test` (1).
  - `cypher_read::trail_reachability_test`:
    - `matches_brute_force`: 4 random graphs of up to 15 edges plus a small
      graph, 3 directions, 9 ranges, against a `(vertex, used-edge-set)`
      exhaustive oracle. Covers EXISTS per-source and per-row, the pattern
      predicate, `DISTINCT` and NOT EXISTS, and asserts no warnings.
    - `does_not_list_trails`: 4×K5 + K6 under a 5 s deadline.
  - Unit tests: `trail_reach::tests` (4) and `percentile::tests` (3), plus
    doctests.
  - `exists_per_source_test` helpers are now `pub(crate)` for reuse.
- **Revert checks.** All failed as stated, then were restored byte-identically
  (md5):
  - fallback `set_percentile`: assertion;
  - uncast DF input: assertion;
  - scan label scoping: assertion (Float, NULL, empty filter);
  - schema'd distance path: deadline error;
  - schemaless distance path: deadline error;
  - trail cap: the warning assertion.
- **Verification.**
  - uni-query + uni-query-functions lib: 816/816.
  - uni-db integration full: 3483/3483.
  - Cypher TCK, full: 3925/3925 in both schemaless and sidecar modes.
  - Locy TCK: 530/530 in both modes.
  - clippy `-D warnings` is clean on uni-query-functions and uni-query
    (`--lib --tests`) and uni-db (`--lib --test integration`).
- **Open.** An undirected unbounded range with `min >= 2` (`-[:R*2..]-`) keeps
  the exponential per-depth search wherever an endpoint is nearer than `min`.
  Other open items: `infer_expr_type` above; the percentile index rule
  (unverified against Neo4j);
  a named but unused relationship variable (`-[r:R*]-`) still forces
  `StepVariable` enumeration.

**Wave C outcome: exact-mode probability through an IS reference (0.745 vs
0.79). Stopped at the TCK; no code changed.** (Superseded: the decisions were
approved and implemented; see the next block.)
- **Derivation.** The diamond has three independent edges: X = s→t (0.7),
  Y = s→m (0.3), Z = m→t (0.5). Under possible-worlds semantics, `reach(s,t)`
  holds iff X ∨ (Y ∧ Z). P = 1 − (1 − 0.7)(1 − 0.3·0.5) = 1 − 0.3·0.85 =
  **0.745**. The terms do not share a variable, so the independence evaluation
  of the same formula also gives 0.745.
- **Where 0.79 comes from.** 0.79 = 1 − (1 − 0.7)(1 − 0.3) = P(X ∨ Y). That
  treats `mid IS reach TO b` as certain, so worlds where Y holds and Z fails
  still derive `reach(s,t)`. It is not double counting. The IS reference is a
  dropped conjunct. The TCK scenario "BDD mode produces numeric probability
  for MNOR diamond" (`monotonic/ExactProbability.feature`) is wrong under
  possible-worlds semantics. So is "MPROD diamond with exact_probability"
  (0.21 = 0.7·0.3; possible worlds give 0.7·0.3·0.5 = 0.105).
- **Mechanism (measured).** The pre-fold `reach` rows carry `a, b, prob,
  __deriv_clause, __deriv_eid_e, __deriv_vid_mid`. The provenance join for
  `mid IS reach TO b` is `[("mid", "a")]`: it is keyed on a body column `mid`
  that the rows do not carry (only `__deriv_vid_mid`), and it omits the `TO b`
  target. `collect_is_ref_inputs` (`locy_fixpoint.rs`) therefore records no
  support. `detect_shared_lineage` falls to its Tier-2 structural flag, and
  `apply_exact_wmc` treats the recursive row as a base fact weighted by its own
  `prob` (0.3). The BDD is X ∨ Y = 0.79, the independence value. The
  `SharedProbabilisticDependency` warning on this diamond is a false positive.
  Probe: exact and default mode both return 0.79. With two parallel s→t edges
  at 0.5, both return 0.825; possible worlds give 0.7875.
- **Why this needs a decision.** Positive IS is a Boolean semi-join in the
  runtime (`target_has_prob` is read only for `IS NOT`). The TCK also relies on
  a "read the referenced PROB explicitly" style: `FOLD p = MNOR(p)` in
  `ProbabilisticStress` B2, and `FOLD pen = MNOR(hz)` in the complement chain.
  If exact mode simply conjoined each referenced fact's event with the row's
  weight, those programs would count the referenced probability twice (p²).
  A correct exact mode has to tell a weight that reads the referenced PROB from
  one that does not. `docs/complete_locy.md` §12.3 ("Multiple IS / IS NOT
  references with PROB ... multiply") describes neither runtime behavior.
- **Open.** The TCK change (0.79 → 0.745, 0.21 → 0.105) needs approval, as
  does how a weight that reads the referenced PROB is counted. The provenance
  join, the recursive lineage formula and per-row variable identity then need
  reworking.

### Wave C — ABDUCE / EXPLAIN RULE in an ASSUME body become compile errors (2026-10-18)

Decision (user): reject rather than surface. Follows the B-wave note above
("An ASSUME body discards ABDUCE results").

- **Defect reproduced:** `evaluate_assume` (`crates/uni-query/src/query/df_graph/locy_assume.rs`,
  body-command loop) kept only `CommandResult::Query` / `Cypher` rows; an ABDUCE
  or EXPLAIN RULE in the body ran and its result was dropped. The old TCK
  feature `AssumeNestedAbduce.feature` asserted only "evaluation should succeed".
- **Fix:** `LocyCompileError::AssumeBodyCommandNotAllowed { command }`, raised in
  `compiler/mod.rs` for each `AssumeBlock` whose body holds `AbduceQuery` or
  `ExplainRule` (a nested body is checked when the recursive `extract_commands`
  compiles it). The message names the alternatives: top-level command after
  the ASSUME, `tx()` + mutate + command + `rollback()`, `Session::scratch()`.
  `dispatch_body_command` refuses both arms too (for a hand-built program)
  instead of evaluating and discarding.
- **DERIVE in a body: kept.** It is not discarded in effect: its mutations land
  in the hypothetical L0 and a later body Cypher command reads them
  (`locy_entity_hydration_derive_inside_assume_reads_the_overlay` checks this
  against a real transaction). Only its `affected` count is not part of the
  ASSUME result, and a body whose only command is DERIVE returns no rows.
- **Python:** `Session.scratch()` / `AsyncSession.scratch()` + stubs;
  `bindings/uni-db/tests/test_session_scratch.py`.
- **Tests:** `crates/uni-locy/tests/assume_body_command_not_allowed.rs` (6);
  TCK feature flipped to 7 scenarios (parse ok, 4 compile errors incl. nested
  and EXPLAIN, evaluation error, top-level ABDUCE alternative). Revert check:
  removing the compiler check fails 5/6 unit tests by assertion.

### Wave C — Lance session cache sizing, magic-sets default (2026-10-19)

**(a) `UniConfig` sizing for Lance's session caches: done, uncommitted.**
- **Shape found.** One `lance::session::Session` per database already
  existed, in `LanceDirectory` (`backend/lance_directory.rs`), built with
  `Session::default()` (6 GiB index + 1 GiB metadata). Every primary open
  goes through it; a fork's `BranchedBackend` wraps the same
  `LanceDbBackend`, so forks share it.
- **Change.** `UniConfig::lance_metadata_cache_bytes` /
  `lance_index_cache_bytes` (`Option<usize>`, `None` = Lance default) →
  `StorageManager::new_with_store_and_storage_options` →
  `LanceDbBackend::connect_with_cache_sizes` →
  `LanceDirectory::connect_with_cache_sizes`, which builds the two
  `QuickCacheBackend`s itself (`Session::with_cache_backends`) and keeps the
  handles. New defaulted `StorageBackend::cache_usage()` (Lance: `Some`,
  `BranchedBackend`: delegates) feeds new
  `DatabaseMetrics::storage_{metadata,index}_cache_bytes` (Rust and Python).
  Python: both keys in `UNI_CONFIG_KEYS`, `apply_uni_config`,
  `uni_config_to_py`. Parity guard now also covers `DatabaseMetrics` fields.
- **Not routed through the shared session.** `lance_branch` free functions
  (fork branch reads/writes), `UidIndex`, `JsonPathIndex`, inverted/sparse
  posting datasets and `Dataset::write` each build a fresh default session
  per call that is dropped with the dataset. They retain nothing between
  calls, so the W2 growth is not theirs, but the caps do not govern their
  in-call caching either, and fork branch reads get no cross-query cache.
  Routing them through the shared session is a separate decision: it changes
  fork read caching (and needs a check that a deleted-and-recreated branch
  cannot hit a stale cached manifest; Lance keys manifests by version plus
  e-tag).
- **Tests.** `storage::lance_cache_sizing` (3): metadata and index cap each
  bound live occupancy (uncapped 622,389 B metadata / 51,010 B index; capped
  at 16 KiB: 16,362 B / 14,954 B), and zero caps disable caching while the
  indexed lookup still answers. Revert (caps not passed to the backend): all
  3 fail on their assertions. Python
  `test_config_merge_and_validation.py`: 2 new tests plus the new keys in
  the accepted-keys test.
- **W2 probe** (`W2_LANCE_CACHE`, new knob; debug build, 20k rewrite
  commits, flush every 250): uncapped heap +13.4 MB, 454 B/commit over the
  last 10k, metadata cache 6.02 MB at the end; 1 MiB caps: heap +8.0 MB,
  94.7 B/commit, metadata cache 1.04 MB. The 5.4 MB heap gap matches the
  5.0 MB metadata-cache gap.

**(b) Locy magic sets default: verified Off, nothing changed.**
`GoalDirected` is `#[default] Off` (`uni-locy/src/magic.rs`),
`LocyConfig::default()` sets `Off` (`config.rs`), Python's `LocyConfig(...)`
and dict form start from that default. Docs say Off: `locy/api/rust.md`,
`locy/api/python.md`, `locy/reference/errors-limits.md`, Black Book.

**Wave C outcome: `range()` with a null argument returns null (user decision,
follow-up to the open item above): uncommitted.**
- **Reproduced.** `RETURN range(0, null)`, a null parameter, a null `WITH`
  binding, a null stored `Int64` value (L0 and flushed), the fallback executor
  (`CALL test.range_null() YIELD v RETURN range(0, v)`) and Locy
  `QUERY … RETURN range(0, x)` all raised
  `InvalidArgumentType - range() end must be an integer`.
- **Four evaluators, made to agree.**
  - DataFusion UDF (`uni-query-functions/src/df_udfs.rs`, `RangeUdf` and
    `extract_i64_range_arg`): the extractor returns `Option<i64>`; any null
    (typed or untyped null scalar, null array slot, encoded Cypher null) nulls
    that row. A null wins over a type error or zero step in another position,
    so the row is null-checked before any argument error is raised.
    `UInt64` above `i64::MAX` now raises `NumberOutOfRange` instead of
    wrapping.
  - Fallback/Locy evaluator (`expr_eval.rs::eval_range_function`): null check
    first; its errors now carry `ArgumentError: InvalidArgumentType` /
    `NumberOutOfRange` (they were untagged).
  - `UNWIND` row evaluator (`df_graph/unwind.rs`): already null-propagated;
    errors tagged with the same two codes.
  - No constant folding or pushdown handles `range` (`is_constant_list` does
    not); `expr_compiler::is_list_producing` only classifies it.
- **Tests.** `cypher_read::range_null_test` (6 tests: DataFusion literal /
  param / WITH / UNWIND-bound, stored column L0 + flushed, `UNWIND range(…)`
  zero rows, fallback executor, Locy `QUERY … RETURN`, unchanged type and step
  errors). Unit: 3 in `df_udfs`, 2 in `expr_eval`, 1 in `unwind`.
- **Revert check.** Disabling the null handling in the UDF and `expr_eval`:
  4 of 6 integration tests and 3 unit tests fail on the assertion; the
  `UNWIND` test still passes because that evaluator null-propagated before.
- **Runs.** uni-query-functions + uni-query lib 822/822; integration
  `cypher_read` + `locy` + unwind/range filters 1071/1071; Cypher TCK
  3925/3925 in each mode (a first pass showed 9 failures in undirected
  self-loop and `keys()` scenarios while another agent's edits were mid-build;
  they passed on the rerun); clippy `-D warnings` clean on uni-query-functions,
  uni-query, uni-db.

**`_`-prefixed user properties are readable (user decision on the C-S1 open item).**
- **Reproduced** (`bugs::underscore_property_reads::underscore_property_reads_every_form_and_state`,
  declared + schemaless label, vertex + edge, unflushed + flushed): 46 probe
  failures before the fix. `keys()`, `properties()` (vertex and edge) and
  `n {.*}` / `e {.*}` dropped `_foo` / `__dbl` / `_bar` in every state and
  mode; `n {._foo}` failed to plan on a declared label (`No field named
  "n._foo"`). Already correct before the fix: `n._foo`, `WHERE n._foo = …`,
  `{_foo: 6}` inline match, ORDER BY, traversal reads, the returned node /
  relationship, Locy rule bodies.
- **Root cause.** 48 sites (17 files) separated an entity's user properties
  from its system fields with `starts_with('_')`. Replaced by one predicate,
  `uni_common::core::schema::is_reserved_property_name`
  (`RESERVED_PROPERTY_NAMES` + `RESERVED_PROPERTY_PREFIXES`, the renamed and
  widened `WRITE_RESERVED_PROPERTY_NAMES`), which the write refusal uses too.
  Column- and variable-name checks (`__` planner columns, batch-field names in
  `expr_compiler`, `optional_filter`, `traverse`) were left alone: a different
  namespace. Declaring a `_` property is still refused.
- **Reserved set, verified in code:** storage columns; entity-map keys
  `entity_ref_from_map` reads as an edge tell (`_type`, `_type_name`, `_src`,
  `_dst`, `_src_vid`, `_dst_vid`) and `_all_props`; planner per-variable
  columns `_label`, `_score`, `_fwd`; Lance row columns (`_rowid`, ...); the
  property-set sentinels; prefixes `_gen_`, `__fde_`, `__alias_of__`. `_fwd`
  was missed first and caught by the TCK (Graph8 [6]/[7], Match3, Match7 [24],
  Merge5 [12]: an undirected relationship listed `_fwd` in `keys()`). `_id` is
  deliberately NOT reserved (`bugs::user_maps_are_not_entities` stores it as
  imported-JSON data); `underscore_property_reads_id_is_a_user_property` pins
  it reads back, and Python `Node["_id"]` / `Edge["_id"]` now return a stored
  `_id` property before the deprecated id alias.
- **Write refusal widened accordingly**, with one exemption: a prefixed name
  the schema declares (a generated `_gen_*`, a MUVERA `__fde_*`) is the
  engine's own column and passes `check_written_property_names(keys,
  schema)`. Without it 5 tests failed (c1-c3 partial SET with generators,
  `test_expression_index`, `muvera_update_and_tombstone`). MERGE literal keys
  are now refused at plan time (`reject_reserved_merge_keys`): with the MERGE
  fix below a reserved key reached a read plan and failed untyped.
- **Two non-`_` defects the probes exposed, fixed:** (1) `MERGE (n:N {k: v})`
  with `k` undeclared on a declared, flushed label failed `No field named k`
  (batched key scan filtered a column the key does not have); such keys now
  take the general path (`merge_keys_are_stored_columns`, write.rs) — the
  batched fast path is lost for that shape (perf, unmeasured).
  `underscore_property_reads_merge_on_undeclared_key_after_flush`. (2)
  `keys()` / `properties()` of a path element read the `properties` container
  as a property (`["properties"]`, `{properties: {...}}`); shared
  `uni_common::value::entity_property_container`.
- **Revert check** (keys-UDF filter back to `starts_with('_')`, properties-UDF
  container back to `_all_props`-only, MERGE gate disabled): both tests failed
  on assertions — `keys(n)`/`keys(e)` lost `_foo`/`_bar`, path
  `properties(...)` returned `{properties:{...}}`, MERGE failed `No field named
  _foo` / `nick`. Restored byte-identically (cmp).
- **Runs:** uni-db `--test integration` 3495/3495; TCK schemaless 3925/3925
  and sidecar 3925/3925 (direct nextest, compliance_reports untouched);
  uni-locy-tck 535/535; lib tests of uni-common, uni-store,
  uni-query-functions, uni-query, uni-plugin-host, uni-bulk 1488/1488; clippy
  `-D warnings` clean on those plus uni-tck, uni-db and uni-python. Python:
  full suite 1047 passed / 15 skipped; new
  `test_e2e_crud.py::test_underscore_property_reads_back` (4 params). Its
  revert check (alias restored ahead of the property in `types.rs`) failed on
  `node["_id"] == "u1"`; restored byte-identically.
- **Left open:** CSV `COPY TO` export emits storage columns and not undeclared
  properties (any name, not `_`-specific); `catalog_scan` still refuses any
  `_`-prefixed plugin catalog column (explicit error, not a silent drop);
  `result_normalizer::is_user_property` also drops user properties named
  `label` / `type` / `properties` (not probed).

**Wave C outcome: positive IS references to PROB rules: implemented,
uncommitted.**
- **Semantics.**
  - A positive `IS` reference to a PROB rule, from a rule with a PROB column,
    multiplies the referenced probability into the row's PROB value. This
    mirrors `IS NOT`'s `1 - p`.
  - Count once: a PROB expression that already reads `p` as exactly one
    product factor (`MNOR(p)`, `MNOR(e.w * p)`) is not multiplied again.
  - Any other read of `p` is not multiplied either, and raises the new
    runtime warning `ProbabilityReadNotFactor`. The planner classifies each
    reference (`locy_planner::classify_prob_read`).
  - Exact mode evaluates each MNOR/MPROD fact as OP over its rows of
    (row event AND each referenced fact's formula), by BDD, as a least
    fixpoint. The new module `locy_lineage.rs` replaces the old
    `detect_shared_lineage` / `apply_exact_wmc` / `compute_lineage`.
  - Two rows share an event when their weight is the same expression over
    the same nodes and relationships (`__deriv_event`).
  - Referenced facts are joined through a hash of their KEY
    (`__deriv_isref_*`), not the old `(mid, a)` join, which never matched.
  - `SharedProbabilisticDependency` fires only when a formula reads an event
    twice. Without it, default and exact mode agree.
- **Values.**
  - MNOR diamond: 0.745 in both modes (was 0.79).
  - MPROD diamond: 0.105 (was 0.21).
  - Parallel 0.5 edges: 0.7875 in both modes (was 0.825).
  - Shared `C->D` graph: exact 0.264, default 0.2784.
  - Self-loop and cycle graphs are exact against a brute-force enumeration of
    possible worlds.
- **Found on the way and fixed.**
  - The diamond's warning was a false positive: nodes in common, no edge in
    common.
  - Exact mode treated two rows on one edge as two events: 0.145 instead of
    0.1 with a self-loop, found by the property test.
  - A renamed MNOR/MPROD output (`YIELD prob AS pr`) was not marked PROB
    (`typecheck.rs`).
  - The row-hash column was declared non-nullable, so clauses with and
    without a lineage column failed to union.
- **TCK.** 12 scenarios were rewritten with derivations in the feature
  comments (10 in `ExactProbability`, 2 in `SharedProofDetection`), and 2
  were added to `SharedProofDetection`. All other positive-PROB scenarios
  read the referenced probability as a factor and are unchanged.
- **Tests.**
  - `locy::locy_prob_is_ref_semantics`: 11 tests, including a 24-graph
    property test against brute-force worlds.
  - `locy_lineage` unit tests: 5. `locy_bdd` unit tests: 3.
  - Oracle: a `ProbFactor` IR, a `hop` rule in random programs, and a
    possible-worlds unit test.
  - Revert checks: the join, formula construction, count-once, default
    multiply and oracle factor each fail their tests when undone.
  - Locy TCK: 535/535 in both modes. Locy integration: 391/391.
  - uni-locy: 230. uni-locy-oracle: 47. uni-query lib: 507.
  - Clippy `-D warnings`: clean on uni-query, uni-locy, uni-locy-oracle and
    uni-db.
- **Open.**
  - EXPLAIN Mode B and the TopKProofs support map still use the old
    `collect_is_ref_inputs` join, which records no support when the subject
    is projected away.
  - `IS NOT` stays an independence factor inside exact formulas: the
    complement is not a negated BDD literal.
  - Cross-group correlation is reported only for shared referenced facts,
    not for shared row events.

## Wave P0 (2026-10-19)

### proctx — procedure reads inside a transaction

- **Probe matrix.** Declared `READ` and `WRITE` Cypher procedures,
  `uni.algo.wcc`, `uni.algo.reachability` (`GraphView`), `uni.algo.mst`
  (edge weights), `uni.vector.query`, `uni.fts.query`; each against the
  transaction's own Cypher read as oracle, unflushed and flushed:
  read-your-writes, read-your-deletes (own and earlier committed), own
  edge-property updates, isolation from a later commit (flushed and not),
  rollback, and SSI read-set membership. Forks not probed.
- **Violations found (before).**
  - Algorithm projections: tx-created edges not followed (reachability 2
    vs 3), deleted vertices still projected (wcc/reachability 2 vs 1, also a
    flushed vertex deleted by an earlier committed tx), tx edge-weight
    updates unread (mst 1.0 vs 5.0), projected rows not in the read set
    (a concurrent change to `n0` committed cleanly, Cypher conflicted).
  - Declared `WRITE` procedure bodies read the live L0: saw a later
    commit (3 vs 2). Declared `READ` and `WRITE` saw it once flushed (simple
    executor built its host over live storage).
  - Vector and FTS: no violation.
- **Root causes.** (1) Six sites built an `L0Manager` for algorithm readers by
  appending the tx buffer to the pending-flush list (the oldest layer), and
  `ProjectionBuilder` neither overlaid tx edges, applied tombstones nor
  recorded reads. (2) Its property manager was built over the tx-pinned
  storage, which skips the tx buffer. (3) A writer-enabled inner executor
  reads its writer's live L0 unless given a read snapshot. (4)
  `procedure.rs` used `self.storage` instead of `effective_storage()`.
- **Fix.** One shared path: `L0Context::read_view` → `L0Manager::from_view`
  (tx buffer kept newest, surfaced as `SnapshotView::transaction`);
  `overlay_l0_neighbors` moved to `uni-store::l0_visibility` and used by both
  Cypher expansion and `ProjectionBuilder`; projection applies vertex
  tombstones from every layer and records the OCC read set;
  `AlgoContext`/`ProjectionBuilder`/`AlgorithmHostBridge` carry the calling
  query's property manager; inner queries get a `read_snapshot` over the
  outer view; `astar`'s heuristic reads through the L0 view.
- **Tests.** `bugs::procedure_reads_in_transaction`: 6 tests. Revert checks:
  each of the five fix parts (read-set recording, inner read snapshot,
  `effective_storage`, property manager, edge overlay + tombstones) undone
  alone fails its test; files restored byte-identically (md5).
- **Open.** A named projection (`uni.graph.project`) built inside a
  transaction likely captures uncommitted rows that survive rollback
  (unverified, from reading `graph.rs`; it is a materialized,
  process-global object, so this needs a decision, not a patch).
  `uni.graph.*` runs on the fallback executor over live storage. Forks
  were not probed.

### P0 infer — property types resolved only from the variable's own labels

- **Repro (before → after), both L0 and flushed, `F.v FLOAT` declared, `S`/`Q`
  schemaless.** `CALL { MATCH (s:S) RETURN s.v AS v }`:
  `9007199254740992.0`/`NULL` → `9007199254740993`/`'x'`; a `WHERE v = 2^53+1`
  after the CALL matched 0 rows → 1. `MATCH ()-[e]->() RETURN e.w` (R declares
  `w FLOAT`, Q does not): rounded/`NULL` → exact/`'x'`. Locy `s.v + 0`,
  `ALONG q = s.v`, unlabelled `(s)` `YIELD s.v`, `[e:Q]` `e.w`: run error
  "cannot read the string as a number" → correct values. `UNION ALL` of `s.v`
  and `f.v`, and of `1` and `'a'`: "mismatched schemas" error → mixed column.
  Not reproduced (already correct): EXISTS/COUNT/COLLECT subqueries, pattern
  comprehension, OPTIONAL MATCH, WITH chains, single-hop and VLP traversal
  targets, plain-MATCH aggregates.
- **Root cause.** Four sites typed a property by name across every label:
  `infer_expr_type` (correlated-subquery output schema, first label wins),
  `merged_edge_schema_props` (any one declaring edge type typed all of them),
  Locy's `uniform_declared_type` (used for arithmetic, ALONG, unlabelled nodes
  and every relationship), and the UNION reconciler rejected the resulting
  `LargeBinary`-vs-typed pair.
- **Fix.** `Schema::scoped_property_meta(EntityScope::{AllOf, OneOf}, prop)` in
  `uni-common/src/core/schema.rs`, used by the schemaless scan (`scan.rs`),
  `merged_edge_schema_props` and `infer_logical_plan_schema` (`common.rs`; a
  variable's scope is collected from the subquery plan's own scans/traversals,
  imports and rebindings are unknown → `LargeBinary`), and
  `locy_planner::property_arrow_type` (scope from the clause's MATCH pattern,
  edges included; `uniform_declared_type` and `property_declared_anywhere`
  removed). Subquery arithmetic is numeric only over two numbers; `avg` of
  integers is typed `Float64` (no visible symptom measured for the old `Int64`).
  `coerce_union_entity_columns` (`df_planner.rs`) encodes any differing column
  pair to CypherValue.
- **Tests.** `cypher_read::schemaless_property_type_scope_test`: 3 new tests
  (`_subqueries`, `_edges`, `_locy`, both flush modes); `uni-common`
  unit test `scoped_property_meta_reads_only_the_labels_in_scope`.
  `union_entity_test::a_non_entity_type_conflict_is_still_rejected` asserted
  the non-openCypher rejection; replaced by
  `a_non_entity_type_conflict_keeps_each_rows_value` (needs sign-off).
  Revert check: all 3 new tests fail with the fix undone, pass restored
  (md5-identical).
- **Open.** `[e:R|Q]` mixing a declared and an undeclared edge type is
  rejected at plan time ("Mixed known and unknown edge types not yet
  supported"), in Cypher and Locy — a separate limitation, not touched.

**Bulk publish crash recovery (data loss): reproduced and fixed, uncommitted.**
- **Measured before the fix** (failpoints, `delta::before-write-run` at the bwd
  delta publish):
  - Another session committed and flushed a vertex and an edge while a bulk
    publish was in flight. The process then crashed in that publish. After
    reopen the vertex and the edge were both gone (`(0, 0)`, expected `(1, 1)`).
  - The same loss happened when a publish failed and the process kept running:
    writes committed afterwards were rolled back at the next open.
  - One marker per database: load A failed mid-publish, then load B
    committed. B's commit deleted A's marker, so A stayed half-published for
    good (`vertices 20, fwd 30, bwd 0`). The same happened when two
    publishes were interrupted before the process died.
  - A `Committed` marker moved the latest snapshot pointer back past a flush
    made after the load's manifest was saved. No rows were lost in that test.
- **Root cause.** Recovery undid a publish by restoring each table to its
  pre-publish Lance version, which drops every later commit to that table, not
  only the load's. The marker was also one file shared by all loads.
- **Fix.** A crash that happens after publishing starts is completed, not
  undone.
  - Before the first publish commit, the marker records each table's staged
    fragments: `StorageBackend::staged_record`, which is Lance's fragment
    serde form.
  - Recovery calls `publish_staged_record`. It appends onto the current
    version, exactly once: it skips a table when the current manifest, or the
    Append transaction of a retained version after the pre-publish one, names
    one of its files.
    - If every file is gone, the table counts as published, because it was
      rewritten and then pruned.
    - If only some files are gone, recovery refuses and keeps the marker.
  - Markers are per load: `catalog/bulk_flush_intents/{uuid}.json`. Commit and
    abort touch only their own marker.
  - Each marker is deleted only after its own reconciliation succeeds. The
    legacy single file is still read, as a rollback.
  - Recovery marks user indexes on the load's labels `Stale`.
  - A `Committed` marker flips the latest pointer only if it still points at
    the load snapshot's parent.
  - Files: `uni-store/src/backend/{traits,lance}.rs`,
    `uni-bulk/src/{flush_intent,bulk}.rs`, `uni-bulk/Cargo.toml` (`fail`,
    `futures`).
- **Contract change.** An interrupted publish used to be rolled back on
  reopen. It is now completed. Two existing tests were changed to the new
  outcome, with exact counts:
  - `bulk_partial_flush_rolled_back_on_reopen` is now
    `bulk_partial_publish_completed_on_reopen`. It expects 5 rows, checks
    both vertex tables, then 8 rows after a clean load.
  - `..._rolls_back_adjacency_resilience` is now
    `..._completes_consistently_resilience`. It expects the full load in both
    directions, exactly once, after two reopens and a compaction.
- **Tests.**
  - `bulk_isolation_resilience.rs`, 6 new tests: commits made inside the
    publish; commits made after a failed publish; another load commits; two
    interrupted publishes; recovery interrupted
    (`bulk::recover::after-table`); `Committed` pointer
    (`bulk::commit::after-committed-marker`).
  - uni-store unit test `staged_record_publishes_exactly_once`: covers the
    repeat, compaction followed by pruning, and a partly missing record.
- **Revert checks.**
  - Original sources restored: all 8 bulk crash tests failed. That is the 6
    above plus the 2 changed ones. The `Committed` test needed its failpoint
    added to the original.
  - All-files-gone branch disabled: the unit test fails.
  - Sources restored byte-identically (`cmp`).
- **Open.**
  - A publish that fails without a crash stays partly visible in the running
    process until the next open completes it.
  - A backend that cannot stage still recovers by version rollback. Only test
    doubles are in that position.

### props — COPY TO property set; entity-field name collisions

- **Repro (before, measured on `d2b4875e0`).** `COPY D TO` with rows only in
  L0: `No data for label 'D'`. Flushed CSV export: declared columns plus the
  raw `overflow_json` blob, no undeclared properties. Parquet: the storage table
  verbatim (`_uid`, `_version`, `overflow_json`; edges `src_vid`, `type`,
  `props_json`). CSV edge export: unsupported. Schemaless label: `Unknown
  label`. COPY FROM CSV into a declared `INT64`: `expected Int64 but found
  Utf8`. With `file_sandbox` enabled, `COPY TO` outside `allowed_paths`
  succeeded. `MATCH (n:F) CALL { WITH n SET ... } RETURN n` dropped user
  properties `label`, `type`, `properties` (L0 and flushed).
- **Root causes.** (1) Two COPY implementations: the live one
  (`write.rs`, `CopyTo`/`CopyFrom`) dumped Lance tables; the other
  (`read.rs`, `LogicalPlan::Copy`, with sandbox and cloud support) was never
  planned. (2) The executor's `file_sandbox` field was never set; the
  database's `UniConfig` reached `config.file_sandbox`, which nothing read.
  (3) `ResultNormalizer::is_user_property` excluded `label`/`type`/`properties`
  by name; Locy's `extract_properties_from_map` and
  `Value::canonical_entity` excluded `properties` by name.
- **Fix.** New `executor/copy.rs`: export reads `id(), properties()` through the
  query engine; layout `_vid` | `_eid,_src,_dst` + sorted union of property
  keys; Parquet via the storage `PropertyExtractor`; import of that layout
  (declared-type parsing, endpoint columns, schemaless targets, cloud URLs,
  sandbox). Dead `LogicalPlan::Copy` and its executor code removed (plus helpers
  only it used). `validate_path` reads `config.file_sandbox`. Normalizer, Locy
  decoder and `canonical_entity` use only `is_reserved_property_name` +
  `entity_property_container`.
- **Tests.** `cypher_read::copy_user_properties` (3: export matrix
  declared/schemaless × vertex/edge × L0/flushed × CSV/Parquet; CSV and
  Parquet round trip; sandbox for COPY TO/FROM and BACKUP),
  `bugs::entity_field_name_collision` (3), normalizer unit test, `copy.rs`
  unit tests (2), Python `test_property_name_collision.py` (3);
  `bugs::repro_edge_export` updated (it pinned the storage-column layout).
  Revert check: the new tests run against a `git archive` of `d2b4875e0` —
  all 3 COPY tests, the flattened-map test and the normalizer unit test fail;
  working tree untouched.
- **Open.** (a) `CALL uni.fts.query(...) YIELD node RETURN node` returns the
  node with **no** properties and `properties(node)` is NULL
  (`plan_procedure_call` drops the `*` whole-entity request) — not a name
  issue, not fixed. (b) A declared `MAP` property through
  `CALL { WITH n SET ... }` hits `debug_assert` in
  `apply.rs::value_to_single_row_array` (unhandled `List<Struct<key,value>>`;
  release builds fall back to text). (c) `COPY FROM` writes with no
  transaction buffer (bypasses an enclosing `tx`) and flushes. (d) A flattened
  entity map carrying a map-valued user property named `properties` and no
  `_all_props` is still read as a serialised entity — no producer found.

### assume — ASSUME body commands run in sequence

- **Defect (measured).** Inside `ASSUME { m } THEN { body }` the body's facts
  were derived once before its first command, so a `QUERY` or a second
  `DERIVE` after a `DERIVE` / Cypher write read the pre-write facts (every case
  of `locy_assume_body_sequencing` returned `[]` against a tx + rollback oracle
  that returns the derived rows). A `DERIVE`'s `affected` count and a nested
  ASSUME's rows were dropped, so a DERIVE-only body returned no rows.
- **Fix.** `locy_assume.rs`: `BodyFacts` / `derive_body_facts`; after each
  writing body command the parent strata are re-derived incrementally against
  the write's footprint (`reuse_plan`) and the body's strata on top; the
  nested-ASSUME baseline is re-pushed. `locy_derive::derive_command_footprint`
  returns the union footprint of the DERIVE's mutations
  (`MutationFootprint::extend`, new in `uni_locy::incremental`). Rows: one
  `{affected: n}` per DERIVE, nested ASSUME rows included.
- **Open (measured, not fixed).** The same stale read exists at the **top
  level**: `DERIVE link` then `QUERY linked` in one program (tx or session)
  returns no QUERY rows; the docs describe commands as reading converged
  facts, so this needs a semantics decision.

### regex — `=~` is a full match (Java `Pattern.matches`)

- Decision: `=~` matches the whole string, as in Neo4j. openCypher 9 dropped
  `=~` and the TCK has no scenario for it, so Neo4j is the reference.
- Before: every evaluator was a find-anywhere match (`'abc' =~ 'b'` was true):
  the per-row evaluator used `Regex::is_match`, and two string operands were
  lowered to DataFusion's `regexp_match` (logical) or `~` (physical).
- Fix: one compiler, `expr_eval::compile_cypher_regex`, parses the pattern as
  written with `regex-syntax` and wraps the syntax tree in `\A … \z`; all
  paths (`_cypher_regex`, now vectorized for strings; fallback executor;
  Locy) use it. The DataFusion `regexp_match` / `~` lowerings are removed.
- Tests: `cypher_read::regex_full_match_test` (literals, params, typed /
  schemaless / indexed columns in L0 and flushed, fallback executor, Locy
  rule WHERE and QUERY RETURN, NULL and invalid patterns) and unit tests in
  `expr_eval`. Revert check: 4 of 5 integration tests fail on the old code
  (the NULL / invalid-pattern pin passes on both, as intended).
- Follow-up (same day): patterns now follow Java's dialect and defaults.
  `java_regex` translates the `regex-syntax` AST into an HIR with Java's
  meaning (ASCII `\w\d\s\b`/POSIX, Java case folding, Java line terminators,
  Java `\p{..}` resolution, `$` before a final terminator) and builds the
  matcher from the HIR. Constructs with no exact translation (possessive,
  lookaround, backreferences, …) are invalid-pattern errors; four
  input-dependent cases are refused per input. The first round's HIR
  `Display` round-trip was wrong for a repetition of a repetition
  (`'' =~ '(?:a{2})?'` was false); the HIR is no longer printed.
- Evidence: `testdata/java_regex_oracle.tsv` (381 cases, answers from JDK 25
  `Pattern.matches`) passes; randomized differential runs (282k
  pattern/subject pairs over two generators) gave no answer that differs from
  Java, only deliberate refusals.
- Open: Unicode-version skew (Rust std's case tables are Unicode 17, JDK 21 is 15.0,
  JDK 25 is 16.0) changes answers for characters new since then; the rejected
  constructs above are not translated.

### P0 trigger — edge pre-images for flushed edges

- **Repro (before → after), each unflushed vs flushed.** Declared `R {w, opt}`
  edge carrying undeclared `note`, schemaless `S`. Flushed edge, `SET` /
  `REMOVE` / `MERGE ... ON MATCH SET`: `EDGE_CREATE`, `old_value` NULL →
  `EDGE_UPDATE` with `{w, opt, note}` (same as unflushed). `DELETE r` and
  `DETACH DELETE` of an endpoint: `EDGE_DELETE` with NULL `old_value` →
  full pre-image. `old.w = 1 AND n.w = 2` matched 0 → 1. Edge deletes,
  flushed or not, were labelled `type:1` / `type:2147483649` → `R` / `S`, so
  an `-[:R]`-filtered delete trigger never fired. An edge updated after a
  flush reported only that write's keys (`{w:2}`) → `{w:2, note:'n'}`.
- **Data defects found on the way (before → after).** Flushed edge, `SET r.w`,
  flush: undeclared `note` → NULL (lost) → kept. `REMOVE r.opt`, flush,
  `SET r.w`, flush: `opt` came back → stays NULL.
- **Root causes.** (1) `PreExistingProbe` had an L1 lookup for vertices only;
  edges came from the L0 chain's newest buffer. (2) Tombstone rows used a
  `type:<id>` placeholder instead of resolving the id. (3) The edge delta
  replay behind the mutation prefetch (`get_batch_edge_props_for_type`, and
  the single-EID read) read only declared columns and merged versions key by
  key, so an older row refilled keys the newest row had dropped.
- **Fix.** `PreExistingProbe::pending_edge_candidates` +
  `extend_edges_with_committed_state` (one `get_batch_edge_props` call over
  the committed L0 chain and L1, `_all_props`); tombstone type resolved via
  the schema; `PropertyManager::edge_delta_row_props` /
  `replay_edge_delta_rows` shared by both replay sites. Files:
  `crates/uni-plugin-host/src/triggers.rs`, `crates/uni/src/api/transaction.rs`,
  `crates/uni-store/src/runtime/property_manager.rs`.
- **Tests.** `triggers::trigger_edge_pre_image`: 14 tests. Revert checks: probe
  + label fix undone → 10 fail; replay fix undone → 2 fail (the data tests);
  files restored byte-identically (md5).
- **Cost.** 1000 flushed edges updated in one commit with a trigger: 124–383 ms
  with the probe (3 runs), 109–282 ms without (6 runs); the difference is
  inside run-to-run noise at this size. The lookup is chunked `IN` scans, not
  per edge.
- **Open.** The probe now runs an L1 lookup for every edge a commit writes,
  including new ones, whenever triggers or CDC are active (the vertex probe
  already did this). No user-facing trigger page exists under `website/docs/`
  to update.
- **Suites.** uni-db integration `cypher_read|cypher_path|bugs|locy`: 1829 run,
  1826 passed, 3 timed out under load (`vlp_property_maps_match_the_reference_on_a_fork`,
  `trail_reachability_matches_brute_force`, `repro_get_edges_scales_with_graph_size`),
  which pass alone (19–71 s). Cypher TCK 3925/3925 schemaless and sidecar;
  Locy TCK 539/539 both modes. Clippy `-D warnings` clean on uni-common,
  uni-query, uni-db (`--lib --test integration`).

### lineage — one reference resolution; `IS NOT` in exact formulas

- **Repro (before).** Diamond `s -0.7-> t`, `s -0.3-> m -0.5-> t`:
  `reach(s,t) AND NOT link(m,t)` was 0.3725 in exact mode (possible worlds
  0.35) with no warning in default mode. `TopKProofs(8)` on the shared
  `C -> D` graph gave `reach(A,D)` 0.2784 (possible worlds 0.264); the hub
  TCK scenario gave 0.7 (possible worlds 0.88). EXPLAIN (recorded
  provenance) showed no child under any derivation, and a recursive FOLD
  rule's derivation was one entry per iteration it changed value (28
  entries for one derivation through a self-loop) while two derivations
  with equal values were one. Two `IS NOT` of one PROB rule in a clause gave
  `(1 - p_a)^2` (0.0441 instead of 0.105).
- **Root causes.** (1) `collect_is_ref_inputs` resolved references by the
  subject's body column, which YIELD drops (`mid`), and ignored `TO`: no
  support for provenance or the TopK fold map. (2) The TopK fold weighted a
  referenced fact by the largest row and treated it as the rows' only event.
  (3) The complement folded into the row event as an independent factor.
  (4) Provenance was keyed by the yield row (value included, and the plan's
  yield schema already lists the `__deriv_*` columns, `__deriv_isprob_*`
  among them). (5) The complement column was named by rule only.
- **Fix.** `locy_lineage::RefResolver` is the one resolution (hidden
  `__deriv_isref_*` / new `__deriv_isnot_*`); lineage, provenance support
  (`row_provenance`) and so TopK read it; the old join, `BodySupportMap`,
  `topk_dnf_disjunction` and `provenance_join_cols` are deleted. `IS NOT`
  to a PROB rule in an MNOR/MPROD rule projects `__deriv_isnot_*`, filled by
  the complement with each matched fact's KEY hash and probability; exact
  mode conjoins `NOT (OR of their formulas)`, evaluated per strongly
  connected component, dependencies first; the analysis counts negated
  facts' variables, so the warning fires. `TopKProofs { k }` runs in
  `LineageStore::correct`: non-read-once facts and MNOR facts with more
  than `k` rows are evaluated exactly over their `k` best rows. Mode A
  EXPLAIN builds children from support (expanded once per tree; leaves
  `(cycle)` / `(derived above)` / `(depth limit)`), keys recursive-FOLD
  provenance per derivation (`derivation_hash_indices`) with the latest
  value (`record_derivation`), and hides `__deriv_*` bindings. Complement
  columns are named per reference.
- **Tests.** `locy_prob_is_ref_semantics`: +6 (negated literal on the
  diamond, edge AND NOT its own reachability = 0, 24-graph negation
  property test over 4 program shapes vs brute force incl. completeness,
  EXPLAIN children vs graph derivations over 12 random graphs in both
  modes, TopK(8)/TopK(1) values, two negations of one rule). `locy_lineage`
  unit: +5. TCK: +4 scenarios (2 ExactProbability, 2 TopKProofs); the hub
  scenario 0.7 -> 0.88 and `repro_locy_runtime_topk_mnor_mixed_support`
  likewise, derivation in comments. Three `locy_fold` unit tests of the
  deleted DNF code removed with it. Oracle: +1 (complement equals possible
  worlds without sharing; the oracle models default mode, unchanged).
- **Revert checks.** Support forced empty, TopK correction off, negated
  columns ignored, complement named by rule: 6 integration + 3 unit tests
  fail with the values above, the rest pass. Provenance keyed by yield row:
  the EXPLAIN test fails. Restored byte-identically (md5).
- **Suites.** uni-query lib 512/512; Locy integration 407/407; Locy TCK
  539/539 both lanes; uni-locy 231/231; uni-locy-oracle 48/48. Clippy
  `-D warnings` clean on uni-query, uni-locy-oracle, uni-db.
- **Open.**
  - Support lists only probabilistic references (to a PROB rule, from an
    MNOR/MPROD rule); a reference to a non-PROB rule, or from a non-fold
    PROB rule, has no hidden identity column, so Mode A shows no child for
    it. Carrying one for every reference would change set-semantics rules'
    fact identity; needs a decision.
  - EXPLAIN Mode B (ABDUCE's tree) still re-runs each clause and shows one
    derivation per fact; it binds `mid` from the re-run MATCH, so the
    projected-away subject does not affect it (not probed further).
  - `proof_probability` (`top_k_proofs` retention) is computed from
    `base_fact_probs`, which keys by a literal `PROB` column; unchanged and
    almost always `None`.
  - A negated fact of a non-fold PROB rule is one variable, like a positive
    one; its own dependence on other facts is not seen.

### P0 trigger follow-up — `partial_lance_writes` and edges

- **Cypher (flag off/on, primary and fork), scenarios A–D.** All four read
  back correctly after flush, compaction (primary) and reopen, before this
  change too. With the flag on, a Cypher edge `SET` still appended a second
  full delta row: the commit merge (`L0Buffer::merge_validated` →
  `insert_edge_impl`) re-inserts each edge as a full row and never carried
  `edge_partial_keys`, so the partial edge route was unreachable from Cypher.
- **Writer API (`Uni::writer()`, `tx_l0 = None`), flag on, before.** A:
  `REMOVE` row then partial `SET` in one window → flush fails, "Ambiguous
  merge inserts are prohibited: multiple source rows match the same target
  row (eid = 0)". C: create + partial `SET` in one window → readable after
  flush and compaction, **gone after reopen**; with no prior `R` flush the
  flush fails (merge target table missing). B: Lance's MergeInsert updated
  **both** delta rows of the edge to `_version` 5 (`p=1` on one, `p=NULL` on
  the other); the read happened to return the right row, so the replay tie
  is shown but a wrong read was not reproduced.
- **Root cause.** Edge delta tables are an append log (one row per flushed
  version, `op = 1` rows for deletes); a MergeInsert keyed on `eid` is
  undefined on it.
- **Fix.** Edges always store full rows: `Writer::insert_edge_partial_full`
  delegates to `insert_edge`; the flush's partial edge split,
  `L0Buffer::edge_partial_keys` / `insert_edge_partial_full`, and
  `DeltaDataset::build_partial_record_batch` / `merge_insert_partial_run`
  are removed. Vertices keep the partial path. Files: `writer.rs`, `l0.rs`,
  `delta.rs`, `uni-common/src/config.rs` (doc), `property_manager.rs` (doc).
- **Cost (measured, 1000 flushed edges, 3 flush rounds × 3 runs).** Flush
  37–43 ms as MergeInsert vs 28–33 ms as full Append; delta rows after 3
  rounds 1000 vs 4000 (the flag-off growth, folded by compaction). Read
  latency on the larger delta table not measured.
- **Tests.** `cypher_write::partial_lance_edge_state`: 8 tests (A–D over
  {flag off, on} × {primary, fork}; A/B/C through the writer; a shape guard
  that a flag-on edge `SET` appends a full row). Revert check: the three
  files restored to their prior state → 3 fail (A-writer, C-writer, shape
  guard); fix restored byte-identically (md5).
- **Open.** The same commit merge drops `vertex_partial_keys`, so the vertex
  partial path is likely unreachable from Cypher transactions too
  (unverified; vertex tables were not inspected).

### ASSUME differential reuse floor (test calibration)

- **Repro.** `assume_matches_a_fresh_evaluation` required reused strata ≥ 10%
  of all strata over every case. Programs with an MNOR / MPROD fold run with a
  provenance tracker, and the runtime then skips the ASSUME baseline, so they
  never reuse. Seed 1792452210422130158: 29 of 46 cases tracked (0 reused /
  17,743 evaluated), 17 eligible (28.1%); total 7.0% → fail, 0 divergences.
- **Fix.** The predicate is now `CompiledProgram::needs_provenance_tracker`
  (`uni-locy/src/types.rs`), used by `impl_locy.rs` and by the test (on the
  same ASSUME text). The 10% floor applies to tracker-free cases only, with at
  least `max(compared / 6, 3)` such cases; tracked cases must reuse 0 (pins
  current behaviour). Messages carry both subsets' counts.
- **Data.** That seed now passes (eligible 28.1%). 10 random seeds: eligible
  reuse 30.6–64.7%, eligible cases 16–26 of 42–47 compared (minimum is 7).
  One of them (15273698863137603002) was at 7.8% total, so would also have
  failed before.
- **Teeth.** Baseline disabled in `impl_locy.rs` → the floor assertion fires
  (eligible 0 / 5,901); file restored byte-identically (md5).
- **Open.** The tracked-reuse-is-0 assertion was not exercised in a failing
  state.

## Decisions follow-up (2026-10-19)

### Top-level Locy commands run in sequence

- **Decision.** A program's top-level commands run in sequence, as ASSUME
  bodies already did (`ef0e8f68c`): a command reads the state every earlier
  command of the same program left.
- **Repro (before).** Matrix of 12 cases × 7 entry points (`session.locy`,
  `locy_with`, session profile, `session.prepare_locy`, `tx.locy`,
  `tx.locy_with`, tx profile) × {L0, flushed}, against the oracle (each
  command its own `tx.locy` in one rolled-back transaction):
  - DERIVE→QUERY, QUERY→DERIVE→QUERY, DERIVE→DERIVE (reads LINK),
    DERIVE→ABDUCE, DERIVE→EXPLAIN, DERIVE→recursive rule: every entry point
    returned the pre-DERIVE facts.
  - Cypher write→QUERY: "Mutation context not set" on every entry point.
  - Prepared program: DERIVE invisible to a later Cypher read (`n=0`), the
    second DERIVE affected 0, ASSUME "no active Locy L0 to fork", and a
    Cypher write went straight to the shared graph.
  - DERIVE with 0 rows, DERIVE→ASSUME and the independent-rule case already
    matched.
- **After.** 168/168 match the oracle; every oracle matches a hard-coded
  answer.
- **Root causes.**
  1. The dispatch loop gave every command the store derived before the
     first command.
  2. `run_program` ran Cypher commands ahead of the first DERIVE inline,
     through `execute_subplan` with no `MutationContext`; a write there fails.
  3. `PreparedLocy` / `InnerLocyBuilder` evaluated with no ephemeral L0.
- **Fix.**
  - New `uni-query/src/query/df_graph/locy_sequence.rs` (`CommandSequence`,
    `CommandFacts`, `derive_command_facts`, `reuse_plan`), used by both the
    top-level dispatch (`impl_locy.rs`) and `locy_assume.rs`.
  - Re-derivation is lazy (on the next fact-reading command), incremental
    (`affected_strata`), and skips reuse when the base-state token moved.
  - The inline loop stops at the first write command; VALIDATE and CALIBRATE
    after it are dispatched.
  - Every collecting evaluation gets an overlay L0.
  - Tracker programs re-derive with a fresh `ProvenanceStore`.
  - `run_strata_native` plans with the classifier registry.
- **Commit scope.** `tx.locy`: writes land in the transaction; the caller
  commits. `session.locy`: nothing commits (unchanged contract). Every write,
  Cypher writes included, is collected in program order into one
  `DerivedFactSet`; `tx.apply` replays it in one transaction, so the whole
  program is atomic. A session DERIVE over a node the program created is
  refused, since the replay re-ids it.
- **`derived`.** Stays the pre-command fixpoint, so that trailing writes
  cost nothing. The TCK parity check compares only QUERYs ahead of the first
  write.
- **Tests.**
  - `locy::locy_toplevel_sequencing` (18).
  - TCK `derive/CommandSequencing.feature` (5, plus a sidecar schema).
  - Python sync (4 params) and async (2).
  - `locy_assume_incremental::a_write_before_the_assume_disables_reuse` now
    asserts the post-DERIVE reuse (`sequenced` 2/0, `hypothetical` 1/1). Its
    8-row assertion is what excludes stale reuse.
- **Revert check.** 6 core files restored to HEAD → 15/18 fail (the 3
  no-write / trailing-write counter guards pass by construction). Restored
  byte-identically (md5).
- **Perf** (debug build, tx, 200 nodes, median of 3):

  | Program | Before | After |
  |---|---|---|
  | No-write | 59.9 ms | 58.2 ms |
  | Three DERIVEs | 3689 ms | 3207 ms |
  | Three DERIVEs + QUERY | 3526 ms | 3633 ms |

  Writes dominate both multi-DERIVE programs; the differences are within
  run-to-run noise.
- **Open.**
  - `tx.prepare_locy` ignores the transaction's L0, so it does not see the
    transaction's writes (separate gap).
  - Locy Cypher commands and `tx.apply` replays get no `$params`.
  - CALIBRATE after a write is untested.
  - Re-derivations each get the full program timeout, and partial
    (`allow_partial`) re-derivation does not surface `incomplete`.

### Bulk commit point and honest `commit()` outcome

- **Repro (before → after).** H1, a publish failing after `vertices_P`
  followed by the retry a plain `Err` invites: 40 vertices and 60 edges after
  reopen for a 20/30 load → `Ok` with a warning, 20/30 live and after reopen.
  H2, a snapshot-manifest failure after the full publish: doubled live and
  after reopen → `Ok` with a warning, one copy. H3, live readers after a
  mid-publish `Err`: `MATCH (n:P)`=20, `MATCH (n)`=0 until reopen → 20/20 when
  `commit()` returns. All three fail on HEAD plus the two new seams, pass after.
- **Design as built** (`crates/uni-bulk/src/bulk.rs`). The `Active` publish
  marker is the commit point. Before it, `prepare_commit` flushes, saves the
  schema and records the publish plan; any failure runs `discard_load`
  (staged files deleted) and returns a plain error = not applied. A failed
  marker PUT is read back: present → committed; absent after a terminal store
  error → not applied; otherwise `UniError::BulkCommitOutcomeUnknown`, staged
  files kept. After it, the publish is essential: the first attempt uses the
  staging handles, then up to 3 retries go through
  `flush_intent::publish_records`, the exactly-once path recovery uses. Still
  failing → `UniError::BulkCommitIncomplete` (do not retry). Best-effort, so
  reported in `BulkStats::warnings`: default indexes; user index rebuild (a
  failed sync rebuild marks the indexes `Stale` in memory); snapshot manifest
  (its only reader is the growth trigger's count); the `Committed` marker
  rewrite; the pointer flip; the index-status schema save; receipt and marker
  delete. The marker is kept for the next open when the flip or schema save
  failed. The schema save before the commit point is essential: `load_schema`
  is in-memory only, so a lost save left rows under labels the stored schema
  lacks.
- **API.** `BulkWriter::load_id()`, `BulkStats::{load_id, warnings}`,
  `Uni::bulk_load_applied(load_id)` (marker, then a per-load receipt under
  `catalog/bulk_loads_applied/`, written before the marker is deleted), two
  `UniError` variants; Python `writer.load_id`, `stats.load_id/warnings`,
  `Uni`/`AsyncUni.bulk_load_applied`, `UniBulkCommitIncompleteError`,
  `UniBulkCommitOutcomeUnknownError` (`load_id`, `reason`). OGM `bulk_add`
  commits every label as one load and re-raises the two exceptions. Before, it
  committed one load per label, so a failure on label 2 left label 1 applied
  behind a `BulkLoadError`.
- **7-day grace window: closed.** Lance's `cleanup_old_versions` deletes
  unreferenced files older than 7 days. In-process completion removes the
  window for the common case, but not for a load left `Incomplete` or
  `Unknown`, or a writer open for a week. `StorageBackend::hold_staged_files`
  now makes `LanceDbBackend::optimize_table` skip cleanup on held tables.
  Revert check, holds disabled, files aged 8 days: 0 of 30 edges after reopen,
  a silent skip. Across a restart there is no window, because `Uni::open`
  fails unless recovery succeeds.
- **Found on the way, fixed.** A fork's bulk commit called
  `SchemaManager::save`, writing the fork's merged schema over primary's.
  Measured: a fork-local label existed on primary after reopen. Now skipped on
  a fork (`fork_bulk_load_does_not_leak_fork_schema_to_primary`).
- **Open.** Success-path visibility. Publishing is one Lance commit per table,
  so a reader during a successful commit sees a partial load. Measured with a
  pause before each table (20 vertices / 30 edges): after `vertices_P`,
  `(n:P)`=20 and `(n)`=0; after the fwd delta, fwd=30 and bwd=0; never
  dangling. There is no small sound fix: readers would have to resolve every
  table at one cut. `BulkCommitIncomplete` keeps the partial live view until
  reopen; no in-process re-drive exists. A fork load that fails mid-publish
  returns `Internal`, because there is no record to complete from.

### `=~` hybrid routing (decision implemented)

- **Built.** `java_regex` is now a port of JDK 25 `Pattern`'s parser, which
  accepts exactly the patterns Java accepts. A pattern without look-around,
  backreferences, atomic groups or possessive quantifiers runs on the
  `regex-automata` automaton. Inputs that trip an anchor guard (an inner
  terminator under `$`, U+0085/U+2028/U+2029, a combining mark next to
  `\b`, `(?d)` inner `\n`) are answered by a lazily built `fancy-regex`
  translation. Every other pattern runs on `fancy-regex` with a step limit.
  The "Unsupported regex input" refusal is gone.
  - **Corrected (Wave Q, 2026-10-21):** "a port of JDK 25 `Pattern`'s parser" no longer describes the code: the module was replaced by a clean-room implementation (see the note at the end of Q6).
- **Still rejected at compile time:**
  - case-insensitive backreferences;
  - backreferences into a repetition that can match empty;
  - look-behinds whose Java `int` window overflows, or that read
    supplementary characters under Java's UTF-16 stepping. Admitted
    exceptions: a BMP-only body, a single uniform class in a one-unit
    window, or an unbounded window with uniform sets and no `\b` or
    look-around;
  - variable-length look-behinds containing fancy constructs;
  - blocks, `\p{java…}`, `\N{}`, `\X`, `\b{g}`, and `(?c)` on a class or
    property.
- **Agreement with JDK 25:** 812,148 cases, 0 wrong answers.
  - **Retracted (Wave P1, 2026-10-20):** the 0 was wrong. A rerun found 2 wrong answers vs JDK 25 (`"aa𐐨" =~ "(?s)(\1{0,2}\p{L}){0,2}"`: Java true, uni false; a zero-length loop iteration case) and a `fancy-regex` panic; see the Wave P1 block.

  | Source | Cases |
  |---|---|
  | `gen_cases.py`, `all` profile, 2 seeds | 480,000 |
  | `gen_cases.py`, `anchors` profile | 100,000 |
  | Prototype case files | 232,148 |

  Every disagreement is a documented rejection. Neo4j runs Java 21
  (Unicode 15.0), the oracle runs JDK 25 (Unicode 16.0), and case mappings
  use Unicode 17.0.
- **Throughput.** 1M strings, release build, before and after:

  | Pattern | Before | After |
  |---|---|---|
  | `abc.*` | 13.6 ms | 14.5 ms |
  | `.*foo.*` | 29.6 ms | 30.5 ms |
  | `(?i)(foo\|bar\|baz).*` | 17.1 ms | 17.8 ms |
  | `.*graph$` | 24.0 ms | 24.7 ms |
  | `(?m)^alpha.*` | 14.0 ms | 14.2 ms |
  | `.*\bfoo\b.*` | 50.3 ms | 51.6 ms |

  - `.*\bfoo\b.*` stays on the automaton for inputs that do not trip its
    guard.
  - When every input trips a guard it costs 3.3 s per 1M; the old code
    refused those inputs.
  - A look-ahead pattern costs about 0.7–0.9 s per 1M.
- **Limits.**
  - `UniError::RegexBacktrackLimit { pattern, limit }`, with
    `UniConfig::regex_backtrack_limit` defaulting to 1M.
  - Python: `UniRegexBacktrackLimitError` and the `regex_backtrack_limit`
    config key.
  - The deadline is checked between rows. The vectorized UDF is registered
    per query because DataFusion's spawned partitions do not see a
    task-local; the row evaluator uses the `REGEX_RUNTIME` task-local.

### D3: ASSUME returns one result per body command

- **Decision (D3).** `CommandResult::Assume(Vec<FactRow>)` becomes
  `CommandResult::Assume { results: Vec<CommandResult>, derived: RuleFacts }`
  (`RuleFacts = HashMap<String, Vec<FactRow>>`, new alias). Breaking; 4.2.0 is
  unreleased.
- **Repro (before).** `ASSUME { CREATE (:Marker) } THEN { QUERY linked; DERIVE
  link; QUERY linked }` over `A->B->C->D`: one result holding 4 flat rows
  (`affected=3`, `A-B`, `B-C`, `C-D`). The empty first QUERY and the DERIVE
  could not be told apart from the second QUERY.
- **After.** Three results: `Query([])`, `Derive { affected: 3 }`,
  `Query([A-B, B-C, C-D])`.
- **`derived`.** Taken at the state the first body command reads: after the
  mutations, before any body command. This is where the top-level
  `LocyResult::derived` is taken (before the first command). The post-body
  state is not reported because re-derivation is lazy and a body ending in a
  write never derives it. `CommandFacts::shared_store` holds the handle;
  `Arc::try_unwrap` after `CommandSequence::finish` avoids a copy (from reading
  the code, not measured). A rules-only body has no results; its facts are in
  `derived`. It used to return every fact flattened as rows.
- **Rows helpers.** `CommandResult::rows()` and `as_assume()` are new.
  `LocyResult::rows()` searches depth first and skips an ASSUME with no
  row-producing body result. Python `AssumeCommandResult` has `.results`,
  `.derived`, and `.rows`: the first row-producing result's rows, or `[]`.
  `LocyResult.rows()` skips via `has_rows`.
- **ABDUCE / EXPLAIN ban.** The ban stays. The old reason ("results would be
  discarded") stopped being true, so the text now says "not yet supported".
  The real gaps, from reading the code:
  - `re_evaluate_strata_reusing` runs without a `ProvenanceStore`, so EXPLAIN
    would always use Mode B.
  - `evaluate_abduce` validates candidates by re-evaluating the parent's
    strata, so the body's rules are left out.
  - The message changed, so `AssumeNestedAbduce.feature`,
    `locy_command_noninterference`, and `test_session_scratch.py` changed with
    it.
- **Tests.**
  - `locy_assume_body_sequencing`: now compares each body result, kind and
    rows, with the same command run alone in a tx + rollback oracle, plus
    hand-computed rows per command. New:
    `derived_is_the_state_the_first_body_command_reads`.
  - `locy_assume_incremental::nested_assume_reuses_the_outer_hypothetical_state`:
    the comment claimed nested rows were excluded, but they were concatenated.
    The test now asserts the exact shape `[Assume[Query], Query]` and both
    answers `{acme, mid, outer}` (3 rows each). The old check was
    `contains("outer")`.
  - TCK `assume/AssumeBodyResults.feature` (3 scenarios, sidecar schema), with
    path-addressed steps `the result at path i.j... should be ...`. The legacy
    "Assume with N rows" steps now require exactly one row-producing body
    command.
  - `result.rs` unit test; 2 Python tests in `test_e2e_locy.py`.
  - The other ~12 sites in 8 files (including `uni-locy-oracle` `assume.rs`)
    now address the body's single QUERY exactly, or panic.
- **Revert check.** Flattening the results back into one `Query` failed 12/13
  targeted tests. `derive_then_assume_body_reads_the_derived_edges` passes by
  construction: it has one QUERY and both arms render the same. Restored
  byte-identically (cmp, md5 `8352353478b539d1`).
- **Suites.**
  - uni-db `--test integration` `/locy/`: 468/468.
  - uni-locy: 232/232.
  - uni-locy-oracle: 48 passed, 5 skipped (ignored).
  - uni-query lib: 512/512.
  - Locy TCK: 547/547 schemaless and 547/547 sidecar.
  - pytest: 1063 passed, 15 skipped.
  - stubtest: clean.
  - Checks: ruff, `gen_python_api_reference --check` (regenerated; the regex
    agent's stub was included too), `check_doc_symbols`,
    `check_rust_python_parity`, and `check_documented_counts` all pass.
    Counts are now 74 files / 547 scenarios; I also added the missing
    `CommandSequencing` row.
- **Clippy.** `-D warnings` is clean on uni-locy, uni-query, uni-locy-tck,
  uni-db (`--lib --test integration`), and uni-python.

### Soft write throttle: bounded, re-checking wait

- **Decision.** The soft band (`soft_limit ≤ l1_runs < hard_limit`) waits in
  steps of at most 100 ms (the hard limit's poll period), re-reads `l1_runs`
  before each step, and stops once the count is below `soft_limit` or the
  budget `min(base_delay × 2^(runs − soft_limit), 100 ms × (hard_limit −
  soft_limit))` is spent. That is 1.6 s at the defaults. No config knob was
  added: the cap comes from the existing limits. The exponent and the
  multiplication saturate. The hard band is unchanged (100 ms poll, no time
  bound).
- **Repro (before).** Five uni-store tests on a paused clock, with
  `l1_runs` set directly (`storage::background_compaction_test::soft_throttle_*`,
  `hard_limit_stalls_until_pressure_drops`):
  - relief at 250 ms while the count is 28: the write waited 40.96 s;
  - the count held at 29: the write waited 81.92 s (budget 1.6 s);
  - `base_delay` = 2^40 s: panic "overflow when multiplying duration by
    scalar".
  Two uni-db tests (`storage::write_backpressure_test`): with soft_limit 1,
  hard_limit 11 and base_delay 10 s, an untimed `CREATE` waited 10.0 s
  (budget 1 s).
- **Root cause.** `Writer::check_write_pressure` slept the whole curve in
  one `tokio::time::sleep`. It never re-read the count and had no cap.
- **Fix.** `crates/uni-store/src/runtime/writer.rs`: a `write_pressure_step`
  pure function plus a loop in `check_write_pressure`.
- **Deadline.** The writer has no deadline. The executor races the statement
  future against `query_timeout` and drops it, which already ended the old
  sleep at the deadline: `statement_deadline_ends_a_soft_throttle_wait` passes
  on both arms. It stays as a guard. Each step is now ≤ 100 ms, so either
  band ends at the next poll after a drop.
- **Revert check.** With the fix reverted, uni-store failed 3/5
  (`ends_when_compaction_relieves_pressure`, `wait_is_bounded_under_sustained_pressure`,
  `backoff_saturates_instead_of_overflowing`) and uni-db failed 1/2
  (`soft_band_write_waits_the_budget_not_the_curve`). Restored
  byte-identically (md5).
- **Target test.** `fork_point_atomic_under_concurrent_parent_writes_async_flush`,
  16 parallel copies × 4 rounds:
  - before: 8, 4, 9, 7 / 16 passed; every failure was a `Timeout`;
  - after: 16, 16, 16, 16 / 16.
  The test-site comment, which blamed worker scheduling and suggested `-j1`,
  now gives the throttle cause.
- **Suites.**
  - uni-store `--lib --tests`: 783/783 (6 skipped), and 797/797 with
    failpoints.
  - uni-db `--test integration` `/fork|flush|compaction|throttle|pressure|write/`:
    728/728.
  - uni-db failpoints `/resilience|recovery|durability|crash|flush/`: 366/366.
  - Clippy `-D warnings`: clean on uni-store (± failpoints) and uni-db.
- **Open (from reading the code, not run).**
  - The throttle is charged per row, so a multi-row statement in the soft
    band pays the budget once per row.
  - Only a background `execute_compaction` resets `l1_runs`. The manual
    `compact` / `compact_label` / `compact_edge_type` do not, so compacting by
    hand does not relieve a throttled writer.
  - `execute_compaction` sets the count to 0 when it finishes, which also
    erases flushes that landed while it was running.
  - With `compaction.enabled = false`, every write stalls after `hard_limit`
    flushes.
  - `DatabaseMetrics::write_throttle_pressure` is hard-coded to 0. There are
    no throttle metrics or counters to update.

## Wave P1 (2026-10-20)

### WP1 — Cypher planning and procedures (a, b, d, G2 read side, h, e)

- **a. Search-procedure `YIELD node`.** Before: `RETURN node`/`node {.*}`
  empty, `properties()` NULL, `keys()` `[]`, `labels()` an error, for
  `uni.fts.query`/`uni.vector.query`/`uni.search`, L0 and flushed. Root:
  `plan_procedure_call` dropped `"*"`, and the yield stayed a VID-string
  placeholder inside the query. Fix: `"*"` adds `_all_props` (full fetch) and
  the placeholder is replaced by the scan's node struct (`df_planner.rs`,
  `search_procedures.rs`).
- **b. `[r:R|Q]`, declared + undeclared.** Before: "Mixed known and unknown
  edge types not yet supported" in Cypher, VLP and Locy. Fix: one
  `TraverseMainByType` over all names (`planner.rs`); Locy plan errors print
  the cause chain (`impl_locy.rs` `{e:#}`).
- **d. `SET` in `CALL {}` for declared types.** Before: debug panic in
  `value_to_single_row_array` for MAP. Root: the fallback wrote display text
  into a `Utf8` column; temporal structs were also taken for entities. Fix:
  every unhandled type goes through `build_property_column_static`, checked
  for the exact type (`apply.rs`). Covered: MAP, LIST<INT/FLOAT/STRING>,
  VECTOR, DATETIME, DATE, TIME, DURATION, FLOAT32, INT32, BOOL, STRING.
- **G2 read side.** Before: with `R2` declared `A -> F`, `count(r)` 1 of 3,
  violating targets/sources dropped. Root: planner narrowed the anonymous
  source (label scan) and the unlabelled target (inferred label check). The
  CSR load in `adjacency_manager.rs` did **not** need a change: the
  compacted + reopened store passes without it, because `compact_adjacency`
  files every delta under each declared label. Fix: both narrowings removed
  (`planner.rs`); the planner shape tests in `pattern_anchor_test.rs` now pin
  the fail-open plan. Cost: `()-[r:R]->()` is `ScanAll` again (LDBC SF1
  WORK_AT was measured 5.2 s / 2278 MB unnarrowed vs 0.13 s / 30 MB narrowed;
  not re-measured).
- **h. Missing `$param`.** Before: writes stored NULL (`SET` erased 42); NOT
  NULL key said "cannot be null". Root: `unwrap_or(Null)` in
  `executor/read.rs` and `locy_eval.rs`. Fix: an error at each evaluation
  site, typed as `UniError::ParameterMissing { names }` at the API layer
  (`impl_query.rs`, Locy `map_native_df_error`); reads use the same message.
  The check runs at first evaluation, before any write; a single
  pre-execution check listing every missing name was not built (eight parse
  call sites plus the plan cache, which skips parsing).
- **e. `uni.graph.project` in a tx.** Before: dirty-tx projection visible to
  others, survived rollback. Fix: refused with
  `UniError::UncommittedWritesInTransaction` when the tx L0 has mutations
  (`procedures_plugin/graph.rs`). A clean tx already built from its snapshot
  (test passed before the fix).
- **Tests.** `bugs::{search_yield_node_is_full_entity,
  mixed_declared_undeclared_edge_types, call_subquery_set_declared_types,
  declared_endpoint_violation_reads, unbound_parameter_in_write,
  graph_project_in_transaction}` (16 tests). Revert check: all fixes reverted
  together, 14/16 failed (the 2 controls pass by design); restored, 8 files
  md5-identical, `impl_locy.rs` re-applied hunk by hunk (WP3 edited it in
  between).


### WP4: Java regex (R1–R4)

- **R1, Unicode version.** Neo4j 2026.09 (current) requires "Java SE 21 and
  Java SE 25 Platform Specification"
  (https://neo4j.com/docs/operations-manual/current/installation/requirements/),
  so the minimum, Java 21 (Unicode 15.0), is the pin.
  - `java_regex/ucd_tables.rs` is generated from Temurin 21.0.9 by
    `scripts/java_regex_oracle/regenerate_ucd_tables.sh`
    (`GenUcdTables.java`): general category, script, script names and
    aliases, the binary properties `CharPredicates` reads, and
    `toUpperCase`/`toLowerCase`, for every code point.
  - Taking the data from JDK 21 is the exact form of filtering by `Age=15.0`.
    It also keeps the case mappings and properties that 16.0 changed for
    existing characters.
  - `regex-syntax` and Rust `char` no longer supply any class or case data.
    Unicode `\b` uses the pinned word set. The automaton's own Unicode word
    boundary is guarded by the characters its tables disagree on.
  - The seven 16.0 scripts are unknown names again.
  - A test checks 363 property spellings against fingerprints of what JDK
    21's `Pattern` matches, over all 0x110000 code points.
- **R2, look-behinds over supplementary characters.** Bounded windows over
  bodies made of sets, groups, alternations and bounded repetitions are
  rewritten into the code-point sequences Java's UTF-16 window admits. This
  covers stages 1–2, fixed and bounded windows. `\b` and look-around bodies
  stay rejected.
  - On the `all` profile, the cases rejected for this reason fell from 12,096
    to 8,448.
  - On the new `lookbehind` profile they fell from 199,584 to 76,944.
- **R3, fancy-regex panic.** The fault is in the crate, shown with a
  standalone repro against fancy-regex 0.17.0:
  `\A(?:x(\1?a))+\z` on `"xaxaa"` panics at `vm.rs:837`. With contiguous
  iterations, `\A(\1?a){2}\z` on `"aaa"` returns false where Java returns
  true.
  - fancy-regex writes a group's new start into its capture slot on entry.
    A backreference inside that group then reads `(new start, old end)`.
    Java reads the previous iteration's capture.
  - Fix: a backreference inside the group it names is rejected at compile
    time.
  - `is_match_backtracking` also runs under `catch_unwind` (defence in
    depth). A panic there becomes `RegexMatchError::Other`.
- **R4, wrong answers vs JDK 25.** One was the R3 class. The other,
  `(?i:[a~~b]|(?<!\G…)){2}`, is Java's `Loop` ending at its first
  zero-length iteration even below `cmin`.
  - The new `backref` profile then found a third class: Java keeps captures
    made inside atomic groups, possessive quantifiers and look-arounds after
    backtracking past them.
  - Both new classes are rejected. The exception is the `(?=(a+))a*b\1`
    idiom, a top-level positive look-around or atomic group whose
    always-set group is named only after it.
  - Two seeds of `backref` and of `all` now show 0 differences.
- **Cost on `all` (seed 11, 271,080 Java-valid cases).** 30,160 cases were
  rejected before and 30,248 after. The newly rejected cases are:
  - 3,128 self-referencing backreferences;
  - 464 stale-capture backreferences;
  - 632 position-dependent empty loops.
  The R2 rewrite recovers 3,648 cases. On `df777cb3e`, the self-referencing
  cases were answered by luck: an empty or reversed capture.
- **Gate.** 0 wrong answers against Java 21 on every profile:

  | Profile | Cases |
  |---|---|
  | `all` | 2 × 300,000 |
  | `anchors` | 300,000 |
  | `unicode` | 468,468 |
  | `lookbehind` | 300,000 |
  | `backref` | 2 × 300,000 |
  | targeted Unicode | 14,710 |
  | `testdata` | 605 |

  - Against JDK 25 there are also 0 differences, except on the cases where
    JDK 21 and JDK 25 themselves differ (Unicode 16.0 data). Those are
    36,939 `unicode` cases and 6,711 targeted cases, and uni-db answers
    them as Java 21 does. Running only the cases where the two JDKs agree
    gives 0 wrong answers.
  - The oracle now records `timeout` for a case Java itself does not finish
    in 2 s. 9 `backref` cases timed out.
- **Throughput** (1M strings, release; the machine was shared, so noise is
  about ±30%). `abc.*`, `.*foo.*`, `(?i)(foo|bar|baz).*`, `.*graph$` and
  `(?m)^alpha.*` stay within noise.
  - `.*\bfoo\b.*` went from 61–90 ms to 43–63 ms, because ASCII inputs skip
    the combining-mark guard.
  - `(?U).*\bfoo\b.*` went from 32–69 ms to 38–83 ms.
- **Suites.**
  - uni-query-functions lib: 328/328.
  - uni-query-functions integration: 21/21.
  - uni-db `test(/regex/)`: 9/9.
  - openCypher TCK: schemaless 3925/3925 and sidecar 3925/3925.
  - clippy `-D warnings` on uni-query-functions (`--no-deps`, because
    uni-store from another package failed clippy at the time): clean.
  - Revert check: case tables back on Rust `char`, `\p{IsLetter}` back on
    `regex-syntax`, and the self-reference, stale-capture, empty-loop and R2
    rewrites disabled. Five new tests and the oracle test failed. The
    restore is byte-identical (md5).

### WP3: Locy (c, L1, L2, L3 + L3b, L4, L5, L6)

- **c. Proof weights and top-k retention.**
  - Before: every `proof_probability` was `None`, and `top_k_proofs = 1` kept both derivations of a fact.
  - Root cause: weights came from `base_fact_probs`, keyed by row provenance hash under a literal `"PROB"` column, and were looked up by the support terms' KEY hash, so the keys never matched. Retention was keyed per derivation, not per fact.
  - Fix:
    - a derivation weighs its own pre-fold PROB value (the fold input, else the PROB column);
    - `ProofTerm::prob` carries `RefTerm.prob`;
    - `record_top_k` keys by fact (`top_k_fact_hash`), replaces a re-recorded derivation, and keeps the `k` heaviest;
    - the accumulator for `TopKPruningCrossedDependency` groups by fact.
  - After: hand-computed 0.9, and 0.4 kept over 0.3 at `k = 1`.
- **L1. A transaction-prepared program runs as `tx.locy` does.**
  - Before: `tx.prepare_locy` returned `[]` where `tx.locy` returned `[7]`. It also ignored the snapshot and cancellation, and collected its writes.
  - Fix: `PreparedLocy::new_tx_bound` with `PreparedTxBinding` and the transaction's cancel scope.
- **L2. `$params` reach every command and the replay.**
  - Fix: `execute_cypher_read` passes the params, and every branch uses the program's cancel scope.
  - The params are stored on `DerivedFactSet::mutation_params`, and `replay_facts` passes them.
  - Before: NULL writes, "Unresolved parameter" on a read after a write, and NULL after `tx.apply`.
- **L3 + L3b. ABDUCE and EXPLAIN RULE in ASSUME bodies.** The ban and `AssumeBodyCommandNotAllowed` are gone.
  - `needs_provenance_tracker` recurses into ASSUME bodies.
  - A hypothetical re-evaluation gets a fresh tracker, with no reuse, when the program records provenance. `ReEvaluation` and `CommandFacts` carry the tracker and approximations to a body EXPLAIN.
  - `evaluate_abduce_with` validates against the body's programs.
  - Found on the way: `lookup_ctx` read the fixpoint context's L0, not the hypothetical overlay, so a body EXPLAIN naming a created node fell back to Mode B. Every read of the current state now goes through `state_ctx`.
  - L3b before → after, exact five-edge diamond (hand-computed): the body returned 0.484375 → 0.46875, the same as the top level.
  - Without the ban fixes, ABDUCE over a body rule found it empty in every candidate state, so every removal "validated".
- **L4. One budget per program, and every cut reported.**
  - Re-evaluations run with `deadline - now`.
  - Where the time went (debug, 1200 items, 287k facts, 300 ms timeout): clause body 289 ms, dedup 525 ms, run 781 ms, row-store conversion 1.08 s, hydration 2.08 s, QUERY 2.85 s, then `Ok`.
  - Now checked:
    - after each clause body and before the post-fixpoint chain (`run_program`);
    - after the run (fails before conversion and hydration);
    - before each command and after the last.
  - `allow_partial`:
    - a cut re-evaluation records `LocyIncomplete` (merged into the result's), and `is_cut_short` stops the commands;
    - an operator deadline is an interruption (`cut_by_deadline`);
    - without `allow_partial`, the cut is the typed `LocyIncomplete`, not a string.
  - Before → after: 8 write+QUERY rounds against a 3-derivation timeout went from `Ok` to a timeout error. A `max_iterations` cut went from 100 of 239 rows with `incomplete=None` to `incomplete` naming `reach`.
  - Left open: under `allow_partial`, converting and hydrating the facts returned is not bounded by the deadline. An in-memory QUERY over the facts is not interruptible, so a command can overrun by its own duration; the program then fails with `Timeout`.
- **L5.** CALIBRATE after a write: regression test (20 seeded + 20 written = 40 collected, session and tx; CALIBRATE alone collects 20) and TCK `neural/Calibrate.feature` (40, then 80 after the write). It passed before the change too; it was not a bug.
- **L6.** `debug_assert_eq!(Arc::strong_count(&derived_store), 1)` before the unwrap in `evaluate_assume`. It held across the whole Locy suite in debug.
- **Tests.**
  - uni-db `locy::{locy_proof_probability (4), locy_tx_prepared_and_params (8), locy_assume_explain_abduce (7), locy_rederivation_budget (5)}`.
  - `locy_command_noninterference::command_noninterference_inside_assume`, now that the body commands run.
  - uni-locy `assume_body_explain_abduce` (5), which replaces `assume_body_command_not_allowed`.
  - uni-query unit tests for `cut_by_deadline`, `record_top_k` replacement, and the weight rule.
  - TCK, +6 scenarios:
    - `AssumeNestedAbduce.feature` rewritten to check results;
    - `TopKProofs` +2, plus the existing EXPLAIN scenario now asserts 0.9;
    - `ErrorConditions` +2;
    - `Calibrate` +1.
  - Python `test_session_scratch.py`: body ABDUCE matches scratch.
- **Revert check.** The 15 source files were restored to HEAD (keeping WP1's two format strings and WP2's transaction hunks) and the test binary was built and copied.
  - 23 of 24 new or changed uni-db tests failed. The CALIBRATE test passed by construction.
  - The restore is byte-identical (md5 of all 15 files).
- **Suites.**
  - uni-db `/locy/`: 493/493.
  - uni-locy: 231/231.
  - uni-locy-oracle: 48 passed, 5 skipped.
  - uni-query lib: 514/514.
  - Locy TCK: 553/553 schemaless and 553/553 sidecar.
  - pytest Locy/scratch: 133 + 31 passed.
  - stubtest: clean.
  - `check_documented_counts`: OK (553).
  - `gen_python_api_reference`: regenerated.
  - `check_rust_python_parity`: fails on `ParameterMissing`, `UncommittedWritesInTransaction` and `DatabaseMetrics.write_throttle_wait_time`. None of them is this package's addition.
- **Clippy** `-D warnings`: clean on uni-locy, uni-query, uni-locy-tck, and uni-db (`--lib --test integration`).

### WP5: bulk visibility and compaction accounting (B1, B2, B3, B-side, T1, T2)

- **B1, one load becomes visible at once.** Measured on df777cb3e with a pause before each table publish: with a warm CSR, `(n:P)` = 20 and `(n)` = 0, then vertices with no edges; with a cold CSR, fwd 30 and bwd 0.
  - Fix: a per-load `_version` gate, `uni-store/src/storage/bulk_gate.rs`. `commit` hides the load's version before the first publish and releases it after the last one. The CSRs of the load's edge types are invalidated under the gate's lock.
  - The gate is applied through `RowVisibility` everywhere in the `StorageManager` read tree: the vertex, delta and main-table scans, the property manager, the columnar scan, and vector, FTS and sparse search.
  - `AdjacencyManager::warm` installs a CSR only if the gate's epoch is unchanged.
  - Semantic compaction excludes holds. A background pass skips the tick; a manual pass polls.
  - Limit (documented): other processes, other handles on the same path, and a fork created mid-publish are not gated.
- **Found along the way: `vertices` rows of a bulk load carried `_version = 1`.** This was left over from before #266. Consequences:
  - the gate could not hide them; the label-less view showed 20 while hidden;
  - a session pinned to a snapshot taken before a load saw the load through `MATCH (n)`: 21 rows where the snapshot held 1.
  Fixed: they now carry the load's version.
- **B2, re-drive.** After `BulkCommitIncomplete` the load is handed to a tracked background task, `Redrive` in `bulk.rs`.
  - It retries `publish_records` with 200 ms → 30 s backoff and keeps the load hidden meanwhile.
  - Then it finishes as recovery would: shows the load, registers `VidLabelsIndex`, marks indexes Stale, raises the hwm, and writes the receipt before deleting the marker.
  - It stops on the shutdown signal or the cancel token. The test shows no task left after `shutdown_in_place`, the marker intact after shutdown, and the next open completing the load.
- **B3, fork markers.** A staging fork keeps its marker and receipt under `catalog/forks/{id}/` (`flush_intent::Markers`).
  - `BranchedBackend` gains `staged_record` and `publish_staged_record`. They are exactly-once on the branch (`lance_branch::publish_recorded_on_branch`), ignoring the primary-version bound.
  - `UniInner::at_fork` runs recovery on the fork's storage.
  - New `Session::bulk_load_applied`, with Python sync and async versions and stubs.
  - A fork's `mark_indexes_stale` does not save the schema.
  - `fork_bulk_load_failed_publish_*` pinned the old residual (22 by label, 0 label-less, `Internal`). It now asserts that the load completes in-process: `Ok` with a warning, 22 by label, 20 label-less, applied.
- **B-side: real.** On a fresh DB, pinning the bulk snapshot showed 0 of 20 (hwm 0).
  - Fix: the manifest takes `max(hwm, write_version)`. Markers carry the version, so recovery and the re-drive raise the hwm (`raise_snapshot_hwm`) when the commit saved no snapshot.
  - Residual (documented): L0 rows with a lower version join that snapshot once flushed.
- **T1 / T2.** `StorageManager::compact` (and so `uni.admin.compact()` and the plugin scheduler) is now the background pass (`run_full_pass`): Tier 2 and Tier 3, under `CompactionGuard` and the directory lock, waiting rather than overlapping.
  - Every full pass subtracts the `l1_runs` it saw at its start.
  - Scoped compactions leave the count alone.
- **Tests added.**
  - `storage::manual_compaction_l1_runs` (2 tests): a throttled writer is unblocked; concurrent passes serialize.
  - `storage::bulk_snapshot_hwm` (2 tests).
  - `bulk_isolation_resilience` (failpoints, 5 tests): B1 warm and cold with background compaction ticking, B2 completion, B2 stop, T2, B3 incomplete with fork reopen.
  - `fork::fork_bulk_load`: 2 tests extended.
  - Python: 2 fork `bulk_load_applied` tests.
  - Unit tests in `bulk_gate`.
- **Revert checks** (two batches, files restored and `cmp`-identical): every new or changed test failed with its fix reverted, except one. `bulk_redrive_stops` passes trivially when no re-drive exists. It was made discriminating (`background_task_count == baseline + 1`) and checked against a re-drive that ignores shutdown: shutdown then waited 30 s.
- **Suites** (separate target dir).
  - uni-store `--lib --tests`: 790/790. An earlier run failed WP2's in-flight `compaction_disabled_soft_band_proceeds_and_hard_limit_refuses` mid-edit; it passed on re-run.
  - uni-store with failpoints: 804/804.
  - uni-db failpoints filter: 405/405.
  - uni-db non-failpoint filter: 335/335.
  - uni-bulk has no tests.
  - pytest bulk and fork files: 106 passed.
  - stubtest: 1 error, `DatabaseMetrics.write_throttle_wait_secs` (WP2).
  - parity: `ParameterMissing` and `UncommittedWritesInTransaction` (WP1).
  - Clippy `-D warnings`: clean on uni-bulk, uni-store (± failpoints), uni-db (± failpoints).

### WP2 — write path: counters, VEC, COPY in a transaction, vertex partial path, throttle, endpoints

- **CNT — `ExecuteResult` counters.** Repro (df777cb3e, `cypher_write::execute_counters_matrix`, 25 statements × {schema, schemaless} × {`execute`, `execute_with`} and over stored rows): `SET n.v = 7` reported `+nodes 1 +labels 1 +properties 3`, `MERGE … ON CREATE SET` `+nodes 2`, `REMOVE n.k` `+nodes 1 +labels 2`, `SET n:L` / `REMOVE n:L` nothing. Root cause: every row rewrite went through the create path (`L0Buffer::insert_vertex_with_labels` / `insert_edge` count a create), and the label writes counted nothing. Fix: create vs update split (`Writer` / `L0Buffer::update_vertex_with_labels`, `update_edge`); the executor records property effects from the old row it already holds (`Writer::record_property_effects`) with the TCK triple rule (add `+1`, remove `-1`, different value both, same value nothing), label effects where it writes the label set, and an entity created in the same statement reports its net state (`StatementScope` remembers what the statement created). New `ExecuteResult::properties_removed` (Python too). Deliberate differences from the TCK's graph-diff metric: `labels` counts per node (Neo4j's `labelsAdded`), not distinct labels in the graph; properties of deleted entities are not counted. Files: `l0.rs`, `writer.rs`, `write.rs`, `types.rs`, `transaction.rs`, `impl_query.rs` (scope), bindings.
- **VEC — declared values in L0.** Repro (`cypher_write::declared_value_coercion_l0`): before a flush `RETURN n` / `properties(n)` gave `e: List[Int]`, `f: Int(1)`; after, `Vector` / `Float`. `n.e[0]` failed "cannot index into Vector". Root cause: `coerce_and_validate_property_value` returned the value unchanged once `accepts()` held. Fix: `to_declared_form` (Int→Float for float columns, numeric List→Vector for `VECTOR` and per token for multi-vector columns); `index` UDF indexes `Value::Vector`. Files: `write.rs`, `df_udfs.rs`. Open: the row evaluator's `Expr::ArrayIndex` in `executor/read.rs` (WP1's file) has no `Vector` arm; `SET x.y = n.e[0]` there still fails. Slicing (`n.e[0..1]`) not covered.
- **f — COPY FROM in a transaction.** Repro (`cypher_write::copy_from_transaction`): the tx saw 0 of 2 rows, another session saw 2 before commit, 2 rows on disk before commit, 2 after rollback. Fix: `execute_copy_from` writes through `transaction_l0_override`; `flush_to_l1` only without a transaction. Over-limit import fails with the transaction memory error (test with 4 KiB limit). File: `copy.rs`.
- **VPK — vertex partial path.** Evidence first, on unmodified df777cb3e store code (`cypher_write::partial_lance_vertex_state`, the edge A–D scenarios against vertices): Cypher arms (flag off/on × primary/fork) 4/4 pass — the path is unreachable from a transaction. Writer arms with the flag on: A (full-row REMOVE then partial SET in one window) → `p`, `x` resurrected after the flush; B (partial SET over a flushed row that dropped `p`) → `p` resurrected after compaction; C (create + partial SET in one window) → vertex absent after the flush; shape guard → 1 row (MergeInsert). Removed: `vertex_partial_keys`, the L0 partial inserts, the flush's per-label partial batch, `VertexDataset::build_partial_record_batch` / `merge_insert_batch`, the partial validator, `touched_needs_full_read`, `has_partial_vertex_keys` and its storage-underneath read. `insert_vertex_partial[_full]` stay as full-row updates (`insert_vertex_partial` now merges over the row read with the transaction's context, not storage only). `uni_partial_writes_total` is no longer emitted; `partial_lance_writes` is inert. After: 8/8.
- **T3 — throttle per statement.** Repro: 1 row 1.0 s, 4-row UNWIND 4.0 s, 2 nodes + 1 edge 3.0 s, `bulk_insert_edges` ×3 3.0 s. Fix: `l0::StatementScope` opened per statement (`execute_internal_with_tx_l0`, profile, AST variants) and per bulk batch (`begin_batch`, no create tracking); `check_write_pressure(tx_l0)` charges the first write and marks the buffer admitted. After: each ≈1.0 s (`storage::write_backpressure_test::soft_band_charges_once_per_statement_or_batch`).
- **T4.** `write_throttle_pressure` computed (`writer::write_throttle_pressure`); new `write_throttle_waits`, `write_throttle_wait_time`, `writes_stalled` (drop-guard recorded, so a wait cut by a deadline counts); Prometheus `uni_write_throttle_waits_total`, `uni_write_throttle_wait_seconds`, `uni_write_stalled_total`; `l1_run_count` doc corrected.
- **T5.** `Transaction::bounded_write` bounds `bulk_insert_*`, `update_vertex_properties`, `delete_vertex_by_vid` by `query_timeout` (type-erased: the nested future overflowed the auto-trait recursion limit under `async_trait`). Hard band: bulk insert now `Timeout` at ≈1 s (was still blocked at 4 s).
- **T6.** `compaction.enabled = false`: soft band does not wait; hard limit → `UniError::WriteStalled { l1_runs, hard_limit }` immediately (message names `CALL uni.admin.compact()`), recovered across the executor string boundary; Python `UniWriteStalledError`. Was: wait, then `Timeout`.
- **G2 write side.** `Writer::insert_edge` checks declared endpoints (tx buffer → committed buffers → `VidLabelsIndex` → storage); covers CREATE, MERGE (both fast paths), bulk insert, COPY FROM, fork promote. `update_edge` does not re-check. `REMOVE n:L` that would orphan an existing edge is refused (`check_label_removal_keeps_edges_declared`, CSR + L0 overlay). Typed `UniError::EdgeEndpointViolation` (Python `UniEdgeEndpointError`, a `UniConstraintError`). Tests changed to the new rule: `cypher_remove::test_cypher_remove` (now asserts the refusal, deletes the edge, then removes the label), and five writer-level tests that wrote edges between unlabelled or nonexistent vertices under a declared type (`compaction_test::test_compaction_l1_to_l2`, `wal_coordination::test_wal_preservation_after_flush`, uni-store `test_issue_75_batch_frontier`, `writer::tests::vertex_delete_cascade_retracts_edges_from_the_adjacency_overlay`) now label the endpoints.
- **M1.** RC3 test asserts the path: `physical_plans` of the batched MERGE (1) against a general-path control forced by `ON MATCH SET` (151 for M = 150), and puts the full timing + plan-count line in every failure message. The 8× bar is unchanged. One run: merge/create 1.1×, plans 1 vs 151.
- **Revert checks.** Group 1 (CNT, VEC, COPY, T3, T5, G2 reverted together) → 13/14 new tests failed; the one pass (`set_stores_the_declared_form`, which only read `n.e`/`n.f`, which the projection casts) was strengthened to compare `properties(n)` and failed in group 2 (VEC write path + T6 reverted: 2/3 failed, T4's test passed as expected); group 3 (T4) → 1/1 failed. Restored; all 7 files md5-identical. VPK's revert check is the evidence run above (unmodified store code).
- **Suites.** uni-store `--lib --tests` 790/790, with failpoints 804/804. uni-db `/cypher_write|storage|session_tx|ssi|trigger|fork|bugs|flush|compaction|throttle|pressure|copy|bulk/` 1647/1647. uni-db failpoints `/resilience|recovery|durability|crash|flush/` 382/382. openCypher TCK schemaless 3925/3925 (1 skipped), sidecar 3925/3925 (1 skipped). Clippy `-D warnings`: clean on uni-store (± failpoints), uni-query, uni-db (`--lib --test integration`), uni-common, uni-query-functions. pytest (`maturin develop`, `-n auto`) 1068 passed, 15 skipped; stubtest clean. `check_rust_python_parity`: my items bound; it fails on `ParameterMissing` and `UncommittedWritesInTransaction` (WP1's variants, not bound in Python yet).

### P1 follow-up — G2 sound endpoint narrowing, Python mappings, vector index/slice, nested Python config

- **Measured first (LDBC SF1 `ldbcdb8`, reflink copies, release `uni` CLI, process peak RSS).** `MATCH ()-[w:WORK_AT]->() RETURN max(w.workFrom)`: df777cb3e (built from `git archive` in a separate target dir) 0.23–0.28 s, 0.83 GB; WP tree 6.0 s, 1.47 GB; `count(r)` 0.07–0.09 s vs 5.3 s, 1.39 GB. The query was **not** refused by the 1 GiB pool, contrary to the old comment's prediction. The labelled form `(:Person)-[w:WORK_AT]->()` took 0.23 s on the WP tree.
- **Design implemented.** `EdgeTypeMeta::endpoint_conformance` (`Unverified` = serde default for legacy catalogs; `Enforced` set at creation unless the name is already in the schemaless registry; `Verified { at }`). The planner restores source narrowing (single label only, as measured before) and target inference, both gated per side by `QueryPlanner::narrowing_side`: every type in the pattern must conform and declare that side. A declaration cannot change in place (re-declaration with other endpoints refused, drop tombstones the name), so nothing resets it on change; the test pins the refusal.
- **`CALL uni.admin.verifyEdgeEndpoints([type])`.** Flush, read version `V`, read each unverified type's live `(src, dst)` at `V` (projected, newest row wins, bulk-hidden rows included), check labels in bulk against `VidLabelsIndex`, wait (10 s) for transaction pins older than `V`, then record `Verified { at: now }` and save per type. Refused on a fork and inside a transaction. Older views distrust later verifications: fork merge (`created_at`, nested forks trust none), `at_snapshot` (private schema copy), `restore_snapshot` (persisted, catalog before pointer). A first version that verified every row incl. tombstones was dropped: compaction keeps deleted rows in the main edges table, so a store that ever held a violating edge could never verify.
- **Bypass paths.** `BulkWriter::insert_edges` wrote straight to Lance unchecked: now enforces (load's own vertices via a VID-run map, then the database). Fork promote, COPY, CREATE/MERGE, tx bulk insert go through `Writer::insert_edge` (WP2). WAL replay can only replay pre-enforcement records for legacy (unverified) types. Not covered: an older binary writing edges without saving the catalog; a pre-upgrade staged bulk load published later by re-drive.
- **After.** WP-tree-plus-fix binary on a fresh copy: unverified 5.7 s / 1.25 GB (fails open, correct); whole-store verification 12.3 s, 1.7 GB peak, all 15 types clean; verified 0.26–0.42 s / 0.81–0.85 GB, `count(r)` 0.07–0.21 s.
- **Task 3 (edge-table-driven `()-[r:R]->()`) not done.** It needs a new logical/physical operator that emits `(src, r, dst)` from the type's adjacency or main edge rows with no source scan, carrying the source/target VID and label columns, edge property hydration, MVCC/L0/fork visibility and the bulk gate the traverse already handles — a new scan operator, not a rewrite. With verified or enforced types the narrowed plan already brings WORK_AT back to the pre-WP1 0.26 s, so the remaining gain is for multi-source-label and unverified types.
- **Python.** `UniParameterMissingError(UniQueryError)` with `names`, `UniUncommittedWritesError(UniTransactionError)` with `operation`, `reason`; `.pyi`, stub-drift allowlist, `python-api-symbols.md` regenerated. Nested `config({"compaction": {...}, "throttle": {...}})` binds every field (durations as seconds or `timedelta`, unknown nested key → `ValueError`, `Uni.config()` reports both); `check_rust_python_parity` gained a nested-struct axis with no allowlist (it caught `version_retention` on the first run). `UniWriteStalledError` now raised end to end in pytest. Finding: a query missing several parameters reports only the first in `names` (`RETURN $a, $b` → `['a']`, read and write paths); left as found.
- **Vector index/slice.** Row evaluator `Expr::ArrayIndex` gains a `Vector` arm; `Expr::ArraySlice` and `_cypher_list_slice` slice a `Vector` into a list of floats. Repro `vector_search::vector_index_slice` failed on both paths ("first argument must be a list", "cannot index into Vector", "Cannot slice Vector").
- **Docs.** `website/docs/reference/configuration.md` (manual/background compaction both lower `l1_runs` by the count at pass start, scoped compaction does not; `WriteStalled` way out; Python nested config), release notes, Black Book, `features/` and `guides/cypher-querying.md`.
- **Tests.** `bugs::declared_endpoint_narrowing` (8: enforced narrows across flush/reopen; clean legacy verifies then narrows; violations reported and edges visible; re-declaration keeps the record; declaring over schemaless edges stays unverified; forks; bulk loader; snapshots/restore/open transaction). `declared_endpoint_violation_reads`' helper now removes the record (a real legacy catalog lacks it). uni-query `pattern_anchor_test` endpoint section rewritten for enforced/unverified/verified/disjunction. Two WP2 casualties fixed (`index::adjacency_cache_test::test_adjacency_manager_lifecycle`, `adjacency_cache_edge_cases::test_overlay_neighbors_via_writer` wrote edges between unlabelled vids). 4 pytests.
- **Revert checks** (each restored `cmp`-identical): planner gate → 6/11 fail; fork distrust → fail once the test reopens the old fork (first version passed for free: the open session's schema was frozen; test strengthened); schemaless-registry check, bulk enforcement, at_snapshot distrust, restore distrust, pin wait, in-tx refusal, three vector arms → each fails its test; Python exception arms + nested config → 4/7 fail.
- **Suites.** uni-query + uni-common + uni-bulk + uni-store + uni-query-functions 2239/2239; uni-db `/cypher_read|cypher_write|cypher_path|plan_shape|index|bugs|fork|schema|vector/` 2312/2312; TCK schemaless 3925/3925, sidecar 3925/3925 (1 skipped each); pytest 1072 passed, 15 skipped; stubtest clean; ruff clean; `check_rust_python_parity` OK; clippy `-D warnings` clean on uni-common, uni-store, uni-query-functions, uni-query, uni-bulk, uni-db (`--lib --test integration`), uni-python.
- **Open.** `restore_snapshot` does not rewind table data after a reopen (the restored store kept the post-snapshot edge count, 1 not 3); its only test never reads after restoring. Not fixed here.

### P1 follow-up — `restore_snapshot` rewinds data; `ParameterMissing` names every parameter

- **Restore repro** (`storage::snapshot_restore_rewinds`, on-disk store; snapshot `Alice(30)->Bob(40)->Carol:VIP(50)`, then flushed `DETACH DELETE Bob`, `SET Alice.age=31`, `+Dave`, 2 edges, and unflushed `SET Carol.age=99`, `+Eve`, 1 edge, `REMOVE Carol:VIP`). Before, same handle and after reopen alike: `[Alice(31), Carol(99), Dave(60), Eve(70)]` — the store kept every post-snapshot write, unflushed ones included (the shutdown flush moved `latest` off the restored snapshot). After: snapshot state in the same handle, after reopen, and with later writes on top, for `Uni::restore_snapshot` and `uni.admin.snapshot.restore`.
- **Root cause.** Restore only moved `catalog/latest`. Readers resolve every table at its newest Lance version; MVCC `_version` filtering (what time travel uses) cannot rewind either, because a vertex delete is a `MergeInsert` that rewrites the snapshot's row in place. Reopen also restarted the version counter below versions already in storage.
- **Fix.** Explicit snapshots record a `RestorePoint` (every table and `indexes/` dataset version, Lance-tagged `uni_snapshot_{id}`, plus catalog index definitions). `Writer::restore_snapshot`: paused compaction + `flush_lock`, flush, withdraw later verifications (kept), fsynced new manifest + `restore_intent.json`, rewind (rollback / empty tables created since / restore or delete index datasets, re-widen declared properties), publish, reset adjacency (overlay included), reload VID labels, clear caches, OCC fence, rebuild indexes whose definitions changed since; open-time `recover_interrupted_restore`. Side fixes: `LanceDbBackend::rollback_table` now drops its cached schema (stale schema → `No field named email` after restore); `optimize_table` sets `error_if_tagged_old_versions: false` (Lance's default fails cleanup on an old tagged version). Refused: non-explicit/legacy snapshot, open transaction, inside a transaction, bulk publish, fork. Files: `uni-store` `snapshot/{manager,restore}.rs`, `runtime/{writer,occ}.rs`, `storage/{manager,adjacency_manager,shadow_csr}.rs`, `backend/{traits,lance}.rs`; `uni` `api/mod.rs`; `uni-query` `executor/procedure.rs`. `StorageManager::restore_snapshot` removed.
- **ParameterMissing.** Before: `RETURN $a, $b` → `names == ['a']` (first evaluated). Fix: `uni_cypher::parse_with_parameters` / `parse_locy_with_parameters` collect every `parameter` pest pair; `UniInner::check_statement_parameters` refuses with all missing names (first-appearance order) at every execute/cursor/profile parse site, on session and `UniInner` plan-cache hits (`PlanCacheEntry::parameters`), prepared queries and both Locy entries; not `EXPLAIN`. A semantic planning error wins over a missing parameter (first version broke TCK `Match1[6]`, `Match2[8]`, `Merge1[16]`, `Merge5[27]`: `InvalidParameterUse` expected; fixed). Locy checks after compile. Files: `uni-cypher` `grammar/{mod,walker}.rs`, `lib.rs`; `uni` `api/{impl_query,session,prepared,impl_locy}.rs`.
- **Tests.** `storage::snapshot_restore_rewinds` (6: API/procedure rewind, compaction past zero retention, later schema/FTS/UNIQUE, refusals, abort-mid-rewind child under failpoints); `snapshot_test` now asserts the in-transaction refusal and a real rewind; `bugs::unbound_parameter_in_write::…names_every_missing_parameter`; `occ` fence unit test; restore-module unit tests; pytest `test_restore_snapshot_rewinds_data`, `test_parameter_missing_names_every_parameter`.
- **Revert checks** (each `cmp`-identical after): no rewind → 5/5 fail; no tag → compaction test fails; tagged-cleanup error → fails (only after the test also called scoped `compact`, the full pass swallows optimize errors); no schema-cache drop → schema test fails; no index rebuild → schema test fails; no adjacency reset → 5/5; no VID-labels reload → 5/5 (first version passed for free; added label-predicate and VLP `:VIP` assertions); no open-time recovery → crash test fails; no-op parameter check → `['a']`; no session cache-hit check → `['p']`; no tx cache-hit check → `['k']`.
- **Suites.** uni-cypher 140/140 + doctests; uni-store 793/793; uni-query 905/905; uni-db `/snapshot|restore|bugs|cypher_read|cypher_write|admin|fork|locy|prepared|session/` 2223/2223; uni-db failpoints `/restore|snapshot|resilience|recovery|crash/` 132/132; TCK schemaless 3925/3925, sidecar 3925/3925 (1 skipped each); Locy TCK 553/553 both modes; pytest 1074 passed, 15 skipped; stubtest, ruff clean; clippy `-D warnings` clean on uni-cypher, uni-store, uni-query, uni-db (± failpoints).
- **Open.** No snapshot deletion, so pinned versions are never reclaimed. Catalog not rewound (renames/type changes after a snapshot not reconciled). Forks not rewound; the shared UID index is rewound, dropping UIDs forks registered since. A parameter used only by a previously registered Locy rule is still reported at evaluation, first name only. Background index builds are not paused during a restore (data-consistent but unmeasured).

### P1 follow-up — restore refused while forks exist

- **Defect.** `restore_snapshot` rewinds primary tables and the shared `UidIndex` but not fork branches, so after a restore a fork kept rows cut from the pre-restore primary and lost the UIDs it registered after the snapshot. Decision: refuse.
- **Fix.** New `UniError::RestoreBlockedByForks { forks }` (tagged display, JSON-encoded names so a name with `,` or `: ` round-trips; `restore_blocked_by_forks_from_message` recovers it on the `uni.admin.snapshot.restore` path). `Writer::restore_snapshot` refuses while the registry holds any entry, any status. Race: fork creation captures its fork point under `flush_lock` and registers its Pending entry afterwards, so a registry check alone had a window. `Writer::restore_epoch`: the restore bumps it under `flush_lock` and re-checks the registry; `create_fork_2pc` reads it before the capture and after `begin_create`, rolling back with `ForkLifecycle { stage: "restore_race" }` on a change. New seam `fork::create-before-begin`. Python `UniRestoreBlockedByForksError` (`forks`), `.pyi`, stub-drift entry. `Uni::restore_snapshot` now keeps typed writer errors instead of wrapping them in `Internal`. Files: `uni-common` `api/error.rs`; `uni-store` `runtime/writer.rs`; `uni` `api/{mod,fork,impl_query}.rs`; `bindings/uni-db` `src/exceptions.rs`, `uni_db/__init__.pyi`, `tests/test_stub_drift.py`.
- **Tests.** `storage::snapshot_restore_rewinds::forks::restore_refused_while_forks_exist` (outer + nested inner fork; API and procedure both typed, data untouched, restore runs after `drop_fork_cascade`); `…::resilience::fork_created_across_a_restore_is_rolled_back` (failpoints: fork held at the seam across a full restore, then rolled back, not registered, retry sees the restored 3 people); pytest `test_restore_snapshot_refused_while_forks_exist`. The two existing failpoint restore tests moved into `mod resilience`: their old path matched none of the failpoint lane's `resilience|recovery|durability|crash_harness` filter, so CI never ran the crash-completion test.
- **Revert check.** Refusal calls removed and epoch comparison disabled: both new Rust tests fail (restore succeeded with forks; the racing fork was created); restored, `md5sum -c` OK.

### P1 follow-up — full local CI run (findings fixed)

- **ABDUCE NOT proposed nothing through an anonymous node** (found by the flagship-notebook lane: `locy_semiconductor_yield_excursion` asserted ABDUCE candidates, got 0). Repro: `(l:L)-[:OBSERVED]->(:F)-[:ON]->(t:T)`, `ABDUCE NOT q` → 0 candidates; same body with `(f:F)` → 2. Root cause: Wave C's identity rule (`6ee8dc3a5`, "a hop identified neither way is not proposed") is right, but the derivation's evidence row binds only named elements, so every hop touching an anonymous node was unidentifiable. Fix: `locy_abduce::name_anonymous_elements` builds the derivation tree over a program copy whose body paths name anonymous nodes and relationships (`abduce_anon_{n}`, checked against the clause text); generated names are reported as empty variables and are not property candidates. Tests `locy_abduce_identity::abduce_through_anonymous_middle_node_proposes_both_edges`, `…::abduce_remove_edge_into_anonymous_node_removes_only_that_edge`; revert (naming disabled): both fail; notebook passes after.
- **`cargo deny`**: RUSTSEC-2026-0378/0379 (wasmtime 49.0.3) → wasmtime family 49.0.4 (lockfile + `uni-plugin-wasm` floor).
- **rustdoc**: `uni-bulk` `flush_intent` module doc linked private `publish_records` → plain code span.
- **fmt**: `tests/common/{bugs,vector_search}/mod.rs` unsorted; in `vector_search` the insertion of `vector_index_slice` had taken the `#[allow(clippy::module_inception)]` meant for `vector_search`.
- **uni-db**: `perf::stress_test::test_stress_concurrent_read_write` wrote `LINK` edges between vertices that were never created, which the declared-endpoint enforcement (G2) now refuses; the test creates its `:Node` vertices first. `teeth::every_revert_patch_still_applies`: the restore-race edit moved `issue_097.patch`'s context; regenerated, and the control test `fork_inherits_unflushed_single_node` fails under it.
- **Docs build**: 3 broken links fixed (`features/forks.md` anchor, `locy/troubleshooting.md` path, `plugins/graph-algorithms.md` anchor); the remaining 20 warnings are notebook pages produced only by `convert_notebooks.py` in the deploy job.

### P1 follow-up — mutating built-in procedures ran in read-only contexts and authorized as `read`

- **Claim** (automated security review, unverified): `uni.admin.snapshot.restore` / `verifyEdgeEndpoints` (and possibly other `uni.admin.*`) pass the read-only oracle and are authorized as `read`. **Confirmed, and wider.** The executor built-ins (`uni.admin.*`, `uni.schema.create*`/`drop*`) are dispatched by name in `Executor::execute_procedure`, not registered; `validate_read_only_with`'s predicate asked only the plugin registry, so every built-in was a read.
- **Measured before** (`bugs::admin_procedure_read_only`, 7 tests, all red; log `sec_admin_ro/repro_before_final.log`): `session.query`, `query_with().fetch_all()` and session `prepare()` ran `compact`, `snapshot.create`, `snapshot.restore` (rewound the store), `verifyEdgeEndpoints`, `createLabel`, `dropLabel`; a pinned session and a `.read_only()` handle ran `compact` and the DDL procedures and wrote the on-disk catalog (`Person` dropped, `Probe` created; `snapshot.*`/`verify` failed there only on the missing writer); `… VERSION AS OF` ran `compact`, `createLabel`, `dropLabel`; `session.locy` ran `compact`, `snapshot.create` and the DDL procedures for real; an `AuthzPolicy` saw `read` from `session.query`, `session.run`, `tx.query` (`write` from `tx.execute`). Separately, session `prepare()` used the registry-blind `validate_read_only`, so a declared `WRITE` procedure prepared and wrote a node.
- **Fix.** One table, `uni_query::BUILTIN_PROCEDURE_MODES` (`Dbms`: compact, snapshot.create/restore, verifyEdgeEndpoints; `Schema`: the 8 DDL procedures; `Read`: compactionStatus, snapshot.list). `validate_read_only_with` consults it before the predicate (covers `validate_read_only`, time travel, prepared, pinned, read-only handle). Dispatch reaches built-in arms only for a classified name (unclassified = not callable); a unit test scans the dispatch `match` both ways. `strongest_procedure_mode` / `is_admin_statement` (uni-query) feed `transaction::statement_verb` (`dbms`/`schema`, else `classify_verb`) used by `tx.query`/`execute`/`prepare`/builders, and `Session::run`, which runs an admin statement outside any transaction (`run_admin_statement`, `dbms` verb, `ReadOnly` when pinned or writer-less) and DDL in an auto-commit tx (`schema`). Session prepared queries get `Session::procedure_is_write`. Session-level Locy programs and ASSUME bodies refuse `Schema`/`Dbms` procedures (`refuse_undeferrable_procedures`). Python `Session.run` / `AsyncSession.run` + stubs (no Cypher route to `restore`/`verify` remained otherwise). Neo4j check: its modes are `READ/WRITE/SCHEMA/DBMS` and `DBMS` procedures (`dbms.info`, `tx.getMetaData`) *do* run in read transactions there; admin rights are the separate `@Admin` privilege, and restore is not a procedure at all. uni's own contract already refused registered `Dbms` procedures in `Session::query` (`procedure_is_write` = mode ≠ `Read`), so the built-ins now follow uni's rule, with `dbms` as the privilege verb. Files: `uni-query` `lib.rs`, `query/executor/procedure.rs`; `uni` `api/{session,transaction,prepared,impl_locy}.rs`; `bindings/uni-db` `src/{builders,async_api}.rs`, `uni_db/__init__.pyi`. 29 existing test call sites moved from `session.query` to `session.run` (+2 via a variable); Python throttle test likewise.
- **Revert check.** Five components reverted at once (oracle ignores table, prepared registry-blind, Locy refusal off, verbs back to `classify_verb`, no admin routing): 7/7 tests fail, each on its own surface; restored, `cmp` identical.
- **Suites.** uni-query 909/909; uni-db `--test integration` full 3682/3682 (109 skipped), `--lib` 18/18; TCK schemaless 3925/3925, sidecar 3925/3925 (1 skipped each); Locy TCK 553/553 both modes; pytest 1077 passed, 15 skipped (2 new); stubtest, ruff, `check_rust_python_parity` clean; clippy `-D warnings` clean on uni-query, uni-db (`--lib --tests`), uni-python.
- **Open.** `uni.create.vNode`/`vEdge` and `graph.rs` procedures are registered `Read` with in-memory store mutation (unchanged, by design per their comments). `Session::run`'s admin path does not use the plan cache. A `tx.locy` program (not session-level) may still call a `Dbms` procedure at top level, as `tx.query` may; `restore`/`verify` refuse inside a transaction themselves, `compact`/`snapshot.create` run.


## Wave Q (2026-10-21)

### Q6 — Java regex: no valid pattern rejected

- **Before.** On `7b5856437`, valid Java patterns were refused as "unsupported Java construct": case-insensitive backreferences, backreferences inside their own group or into an empty-matching loop, captures kept past atomic/possessive/look-around bodies, position-dependent empty loops (`(?:a|\G){2}`), look-behinds with `\b`, look-arounds or overflowing windows, blocks, `\p{java…}`, `\N{…}`, `\X`, `\b{g}`, `(?c)`. On the new profiles the old code rejects 236,440 of 236,440 Java-valid `javaprops` cases, 193,417/193,417 `names`, 224,816/300,000 `grapheme`, 241,264/300,000 `canon`, 151,432/300,000 `cibackref`, 106,432/278,264 `emptyloop`; on the Wave P1 gate files it rejected 29,328 (`all2`), 30,248 (`all`), 58,592 (`backref`), 58,871 (`backref2`), 102,264 (`lookbehind`).
- **Root cause.** Java's answers in those cases follow from its matcher's state handling (captures never restored past a node that matched on its own, `Loop`'s zero-length stop, UTF-16 look-behind stepping, `CIBackRef`'s unit counting, `GraphemeBound` reading `matcher.last`), which a translation to `fancy-regex` cannot express, and the missing constructs had no data (names, blocks, `Character` predicates, normalization, grapheme rules).
- **Fix.** `java_regex::engine` is a port of the JDK 21 matcher (node graph + `match` of every node + Java's `Matcher` state, `localsPos` memo included) over UTF-16; it replaces `fancy-regex` (dependency removed; `stacker` added). New: `grapheme.rs` (JDK 21 `Grapheme`), `nfc.rs` (`NFC(s)` single code point via NFD lookup), `names.rs` (`Character.codePointOf`). `GenUcdTables.java` now also emits the `Character.isXxx` tables, blocks (`UnicodeBlock.of` + every `forName` key by reflection), combining classes, decompositions, case-to-ASCII tables and `ucd_names.bin` (34,887 names, 0.5 MB). Parser: `\N{}`, `\X`, `\b{g}`, `(?c)` → `NfcChar`, `CIBackRef` modes, `hasGroupRef`; Rust-version `to_uppercase` replaced by the JDK's mappings for name lookups. Routing: position-dependent empty loops now go to the port (they were rejected, the automaton would answer them wrongly). The backtrack limit counts backtracks at choice points (as fancy-regex counted pops); recursion is capped at 2^17 levels (~45 MB, `RecursionLimit`; Java 21 overflows a 1 MB stack after <2,000 `(?:a|b)*` iterations, the port allows ~30,000); a minimum count whose empty iteration left the state unchanged skips the identical rest (`\b{1073741824}`). Inputs where Java throws (`(?=ab)a\b{g}b` on `"ab"`) fail with `RegexMatchError::JavaException` naming the exception. Item 5: `(?c)` is Java's inline `CANON_EQ` flag (affects classes and properties: `NFCCharProperty`), now implemented. Files: `crates/uni-query-functions/src/java_regex/{engine,grapheme,nfc,names}.rs` (new), `{mod,parse,sets,study,emit,tests}.rs`, `ucd_tables.rs`, `ucd_names.bin`, `expr_eval.rs` (test), `Cargo.toml`; `scripts/java_regex_oracle/{GenUcdTables.java,JavaRegexOracle.java,gen_cases.py,regenerate_ucd_tables.sh}`; testdata (+87 cases, `reject` marks dropped).
  - **Corrected (Wave Q, 2026-10-21):** `engine`, `nfc`, `sets`, `study` and `emit` no longer exist and nothing in `java_regex` is a port of JDK code: the module was replaced by a clean-room implementation (see the note at the end of Q6).
- **Oracle.** `JavaRegexOracle` records `exception:<class>` and `stackoverflow`, and takes `-Doracle.timeout.ms`. New `gen_cases.py` profiles: `javaprops`, `names`, `grapheme`, `canon`, `cibackref`, `emptyloop`, `lookaround`. The 9 `backref2` cases that timed out at 2 s were re-run with a 30-minute limit on JDK 21 and 25: all `false` on both (one comparable `backref` case takes JDK 21 76 s); the port agrees on all 9: 7 with the limit raised to 10^9, the other 2 unlimited in release (`false`, about 13 min each). They stay out of the curated testdata (they need minutes).
- **Gate (final binary), wrong answers vs JDK 21 / JDK 25.** `all` 300,000: 0/0; `all2` 300,000: 0/0; `anchors` 300,000: 0/0; `unicode` 468,468: 0/36,939; `lookbehind` 300,000: 0/0; `backref` 300,000: 0/0 (16 stopped by the default limit); `backref2` 300,000: 0/0 (17 limit, 9 Java timeouts); targeted 14,710: 0/6,711; `javaprops` 300,000: 0/37; `names` 300,300: 0/0; `grapheme` 300,000: 0/1; `canon` 300,000: 0/0; `cibackref` 300,000: 0/0; `emptyloop` 300,000: 0/0; `lookaround` 300,000: 0/0; testdata 692: 0. Total 4,383,170 + 692. Every JDK 25 difference is a case where JDK 21 and 25 themselves differ (counts equal, uni-db = JDK 21 there). Java exceptions reproduced with the same class: 1,102 `grapheme`, 2,391 `lookaround`, 5 `emptyloop`, 1 testdata. Rejections of Java-valid patterns: 0 on every file. The limit cases agree with Java when the limit is raised: measured on an earlier build that counted every failed node (55 cases then; same matcher), 54 at 10^9 and one at 10^11 (119 s release; JDK 21 takes 76 s).
- **Throughput** (1M strings, release, shared machine). Automaton patterns unchanged (`abc.*` 16.7→17.2 ms, `.*foo.*` 34.3→34.3, `.*\bfoo\b.*` 38.5→38.6). Backtracking route faster than fancy-regex: `.*(?=foo).*` 744→486 ms, `(\w+) \1.*` 229→140, `(?:(?!foo).)*` 603→510, `(?s).*(?<=\p{L})foo.*` 1021→750; every input tripping a guard: `.*\bfoo\b.*` 3434→692 ms. Exponential backref patterns run ~2–4× slower per backtrack than fancy-regex.
- **Tests.** Unit: `java_only_constructs_answer_as_java_21`, `case_insensitive_backreferences_answer_as_java_21`, `java_backtracking_state_is_reproduced`, `java_matcher_exceptions_are_reproduced`, `deep_recursion_fails_with_the_typed_error`, look-behind cases with `\b`/look-around/`\X`, `names`/`grapheme`/`nfc` module tests, 727 property fingerprints (all `\p{java…}` ±`(?i)`, every block); the four rejection tests replaced by Java-verified answers. Integration (`cypher_read::regex_full_match_test`): 14 new `CASES` on every evaluator path, `regex_java_exceptions_and_recursion_limit_are_errors`; the timeout test's pattern now has a backreference (Java's loop memo makes `(?:a|aa)*c` fast, so it no longer timed out).
- **Revert check.** CIBackRef folding off, `GraphemeBound` from the position instead of `matcher.last`, `Loop` without the zero-length stop, `\N` table ignored: 8/23 `java_regex` tests fail; restored, md5 identical.
- **Suites.** uni-query-functions lib + integration 356/356; uni-db `--test integration` `test(/regex/)` 10/10; TCK schemaless 3925/3925 and sidecar 3925/3925 (1 skipped each); clippy `-D warnings` clean on uni-query-functions (`--lib --tests`) and uni-db (`--lib --test integration --no-deps`: uni-query from another package failed `type_complexity` at the time).
- **Open.** Inputs past the recursion cap error where Java with a large stack answers. The default backtrack limit stops 33 gate cases Java answers (`false`) in up to ~76 s. JDK 25's grapheme rules (Unicode 15.1+ GB9c) are not followed (Java 21 is the pin).
- **Replaced by a clean-room implementation (2026-10-21).** The module described above was deleted and rewritten from `docs/specs/java_regex_behaviour.md`, public JDK documentation, Unicode 15.0 data and black-box probes of JDK 21/25 (sources and probe findings: `crates/uni-query-functions/src/java_regex/PROVENANCE.md`). New layout: `parse` → AST with code-point sets → `analyze` (loop strategy, look-behind arithmetic) → `linear` (`regex-automata`, chosen per pattern) or `backtrack` (UTF-16 VM with an explicit stack: backtrack limit 1,000,000 steps, 100,000 pending choice points, 4,000,000 stack records; nesting over 2,000 refused). The gate counts above were measured on the replaced module; architecture: `docs/UNI_BLACK_BOOK.md`, "`=~` is Java's `Pattern.matches`".

### Q5 — bulk loads (fork mid-publish, publish-time endpoints, bulk snapshot, non-staging refusal, uni-bulk tests)

- **Fork created mid-publish.** Before (7b5856437, publish parked before `main_edges`): the fork saw the load's 20 vertices by label and 0 of its 30 edges, for good. Fix: `create_fork_2pc` waits for the parent's gate (`BulkVisibilityGate::wait_open`, new, Notify-based; 30 s bound) and re-reads `BulkVisibilityGate::epoch` after `flush_and_capture_fork_point`, retrying the capture when it moved. Past the bound: `ForkLifecycle { stage: "bulk_publish" }`, nothing registered. Seams `fork::create-before-capture`, `fork::create-bulk-publish-wait`. After: 20/30/30/30.
- **Publish-time endpoint re-check.** Broader than the brief's pre-upgrade case. A pre-upgrade catalog reads `Unverified` anyway, but an endpoint older than the load can lose its label (or be deleted) while the load's edge is still hidden. The label-removal guard sees no edge. Before: `E` stayed `enforced` and `MATCH ()-[r:E]->()` returned 0 of 1, live and after reopen. Fix (`uni-bulk/src/endpoint_check.rs`): `commit` and the re-drive re-check the endpoints `insert_edges` found outside the load (L0 first, then storage, not `VidLabelsIndex`), and on a violation record the type `Unverified`. The record is saved on the primary; a fork keeps it in `catalog/forks/{id}/bulk_endpoints_unverified.json`, applied at every fork open. The violation is reported in `BulkStats::warnings` or the log. Recovery at open runs before WAL replay, so a completed marker becomes `Committed` without `endpoints_checked`, and the new `check_recovered_bulk_endpoints` runs after replay (primary and fork opens): it reads the load's edges by `_version` (all of the type's edges if the marker predates `version`), checks them, then writes the receipt and deletes the marker.
- **Bulk snapshot gained older L0 rows.** Before: pinned to the load's snapshot, 20 rows; after a flush, 21. Root cause: the ceiling is the load's version, reserved at first write, while rows committed before it can still sit in L0, which pinned reads skip. Fix: `finalize` and the re-drive flush L0 first when it holds rows (`flush_l0_if_buffered`). A failed flush or manifest now keeps the marker, so the next open raises the ceiling. Recovery at open needs no flush: L0 is empty, and WAL replay assigns versions above the ceiling. After: 21/21.
- **Non-staging backend.** `BulkWriterBuilder::build` returns the new `UniError::BulkLoadUnsupported`. Removed: visible writes, `write_active`, `persist_active_intent`, the version-rollback abort, and the non-staging `Committed` commit point. Recovery rolls back only the legacy single marker. A per-load marker without publish records is refused and kept. Python routes the error to base `UniError`, with a parity exemption.
- **Found on the way.** `BulkWriter` was `!Sync` (the progress callback was `Box<dyn Fn + Send>`), so every future it returned was `!Send` and a load could not run on a spawned task. The callback is now behind a `Mutex`; the public bound is unchanged.
- **Tests.** uni-bulk unit tests (new, 14). `bulk_gate` 4 new. `bulk_isolation_resilience` 5 new: fork waits, capture retried, fork refused while incomplete, re-drive re-check, recovery re-check after WAL replay. `storage::bulk_endpoint_recheck` 2 new. `storage::bulk_snapshot_hwm` 1 new.
- **Revert checks.** Batch 1 (fork.rs original, no flush, no downgrade, no staging refusal): 7/8 new integration tests and 3 unit tests fail. The 8th, `a_conforming_load_keeps_the_types_record`, guards against false positives. Batch 2 (epoch re-check off, recovered check without the writer's L0, commit re-check off): the capture-race, recovery and commit tests fail (0 of 1 edges found). Restored, md5 identical.
- **Suites.** uni-bulk + uni-store 812/812; uni-store with failpoints 812/812; uni-db `/bulk|fork|storage|snapshot/` 425/429 then 13/13 on rerun. The 4 that failed (`snapshot_restore_rewinds` ×3, `procedure_reads_in_transaction_on_fork`) touch no bulk path. They passed on a rebuild with my sources unchanged, while other packages' files were changing; cause not established. uni-db failpoints `/bulk|resilience|recovery|crash/` 179/179. pytest bulk + fork files 106 passed; `check_rust_python_parity` OK. Clippy: no warnings in uni-bulk, uni-store or uni-db. `-D warnings` was blocked by other packages' in-flight uni-query / uni-query-functions lints.
- **Open.**
  - A label removal can still slip past the re-check when its own guard runs before the publish and it commits after the re-check has read the labels (unmeasured).
  - A non-`DETACH` delete of an endpoint while the edge is hidden leaves a dangling edge. The re-check downgrades the type but does not remove the edge.
  - The load's version is reserved at its first write, not at commit. So a snapshot or flush manifest taken while a load is staged has a ceiling covering the load, and a transaction begun in that window can see the load appear mid-transaction once it publishes (from the code; unmeasured). Fixing that needs per-pin hidden-version capture and manifest exclusions (snapshot/restore territory).

### Q8 — edge-driven source for `()-[r:R]->()` (G2 Task 3)

- **Measured (LDBC SF1 `ldbcdb8`, reflink copies, release `uni` CLI, query time / process peak RSS, 3 runs each, host load average 9–18).** Unverified `WORK_AT`: `RETURN max(w.workFrom)` 5.6–6.9 s / 1.24–1.61 GB → 0.33–0.62 s / 0.82–0.89 GB; `count(w)` 5.6–7.6 s / 1.59–1.63 GB → 0.11–0.13 s / 0.90–0.91 GB. Verified (label-narrowed `Scan`, a path this change does not touch): 0.30–0.52 s → 0.32–0.43 s and 0.10–0.34 s → 0.10–0.22 s, within noise. "Before" is the same tree with the planner hook disabled.
- **Design.** It is not a separate `(src, r, dst)` edge-table scan. `EdgeSourceScanExec` sits directly over the unchanged all-vertex `GraphScanExec`. At execute time it warms the CSRs and collects every vid that the layers `get_neighbors` reads hold an entry for (Main CSR, frozen and active overlays, the shadow CSR on a versioned read, and the transaction L0 on a live read; `AdjacencyManager::collect_source_vids`, `GraphExecutionContext::edge_source_candidates`). It then publishes those vids as `_vid IN` chunks into the scan's `DynamicVidFilter`. The traversal above is unchanged. Rows match the old plan by construction: same scan and visibility, filtered only by vid; the candidates are a superset of the vertices that have a neighbor; and the traversal drops sources with no neighbor. A full scan is kept in an SSI read-write transaction, so the read set is unchanged (Locy always runs with one), and when `vertex_scan_beats_lookup` fires. The planner (`plan_edge_driven_source`) emits it only over an unfiltered, non-optional `ScanAll` source feeding a fixed-length, non-optional hop over native types. A separate edge-row operator would have had to reimplement the visibility the traversal already has, and gave no further benefit here.
- **Tests.** `cypher_read::edge_source_scan_test` (9): 16 query pairs (as written vs `WHERE id(x) >= 0`, which keeps the all-vertex scan), covering directed, incoming, undirected, `[:R|S]`, all types, self-loops, bound target, and entity structs. Both plans are asserted per pair. 200 edge-less vertices are added so that the restriction actually happens: the source operator's row count is asserted below 200, or above it in an SSI transaction. Scenarios: L0, flushed, mixed, deletes (edge and DETACH), reopen, fork (unflushed and flushed) and primary beside it, in-flight bulk load then commit, transaction with SSI on and off, pinned snapshot, unverified and verified catalogs (incl. a label-narrowed type). Also `edge_source_scan_plan_shape` (uses/avoids) and an SSI commit-outcome equivalence test. Registry row `EdgeSourceScanExec` is `Proven`.
- **Revert checks** (each restored `md5sum -c` OK). Planner hook off → 8/9 fail. Overlay layers off → 4 fail (L0, mixed, fork, pinned live). Transaction-L0 candidates off → `in_transaction` ssi=false fails (missing rows). SSI fallback off → `in_transaction` ssi=true fails, and the commit-outcome test fails (`SET` on an edge-less vertex conflicts on the old plan and committed on the new one). Shadow-CSR candidates off → no test failed. It is kept because it mirrors `get_neighbors_at_version`. No fixture where it changes the answer was found. A pinned view's fresh CSR is warmed at the pinned version, so a deleted edge is already in its Main CSR.
- **Finding (not fixed, outside Q8).** `pin_to_version` to a snapshot taken before a `DETACH DELETE` does not see the deleted vertex or its edges, on either plan. Repro: seed, flush, `create_snapshot`, `MATCH (a:A {v: 2}) DETACH DELETE a`, flush, pin. `MATCH (x) RETURN x.v` lacks 2, and R count is 4 instead of 5. Same mechanism as the restore notes: a vertex delete rewrites the snapshot's row in place. The pinned test deletes an edge instead.
- **Suites.** uni-query 911/911; uni-store lib 420/420; uni-db `/cypher_read|cypher_path|plan_shape|fork|bulk|bugs/` 1759/1759; TCK schemaless 3925/3925 and sidecar 3925/3925 (1 skipped each).
- **Clippy** `-D warnings` clean: uni-store, uni-query (`--lib --tests`), uni-db (`--lib --test integration`).
- **Open.** Variable-length (min ≥ 1) and `TraverseMainByType` (schemaless types) sources are not restricted. Locy bodies always have a read set, so they never restrict.

### Q2 — declared endpoints and the writer (`update_edge`, older binaries, trigger probe)

- **`update_edge` can move an edge (repro, before → after).** `WITH {_eid: e, _src: s, _dst: f, _type: 'R'} AS m SET m.w = 7` on an existing `A -[R]-> F` edge: stored the new source `S` (violating `R`), a new type `Q`, or the reversed direction, buffered, flushed and after a reopen → refused with new `UniError::EdgeIdentityChange { eid, stored, requested }` (Python `UniEdgeIdentityError(UniConstraintError)`); the edge's own identity still updates. A forged map naming a nonexistent eid with an `S` source wrote an unchecked edge → `EdgeEndpointViolation`. Cause: `Writer::update_edge` trusted the row binding's `(src, dst, type)`, and a map with `_eid/_src/_dst/_type` is such a binding. Fix: `Writer::check_edge_identity` (tx/current/pending L0 `edge_endpoints`, then `L0Buffer::edge_identity_memo` filled from the source's outgoing adjacency once per `(src, type)`, then `lookup_edges_by_eids`; unknown eid → `check_edge_endpoints`). A first version read the adjacency per edge: `SET` over 100k edges of one hub did not finish in ~10 min (debug, killed); with the memo the 100k SET executes in 8.6–15.2 s, against 9.4–19.4 s for a build without the check (same harness, shared machine).
- **Older binary writes edges.** Every catalog save (`Schema::writer_features`), manifest (`SnapshotManager::save_snapshot` stamps `SnapshotManifest::writer_features`) and WAL segment (`WalSegment::writer_features`) records `endpoint_enforcement`. On open, after WAL replay, `Writer::distrust_conformance_after_unmarked_writes` downgrades every `enforced`/`verified` type to `unverified` (warn + save; in memory only for read-only opens and forks) when a replayed segment is unmarked or the latest manifest is unmarked and is not `Schema::trusted_unmarked_manifest`. An unmarked latest manifest found at open becomes that floor, so a store whose last flush predates the upgrade does not downgrade types declared after it. Older binary simulated by `WriteAheadLog::without_writer_features` and by publishing a stripped copy of the latest manifest. Not covered: a bulk load staged by an older binary and published by a later re-drive (Q5), Lance writes by an older binary with WAL disabled that crash before their manifest.
- **Found on the way: commits after a reopen were lost in a crash.** Three flushed commits, clean close, reopen, commit, crash image → `[1,2,3]` (row 4 lost) → `[1,2,3,4]`. Measured: the reopened WAL wrote segment LSN 1 under a manifest `wal_high_water_mark` of 3, and `replay_since(3)` skipped it. Cause: `WriteAheadLog::initialize` resumes after the highest LSN on disk, and a flush truncates every segment ≤ its mark. Fix: `WriteAheadLog::ensure_lsn_above(hwm)` in `Writer::replay_wal` (also raises `flushed_lsn`).
- **Trigger/CDC edge probe cost.** Measured that the probe ran on every commit of every database: the built-in node-only `:_AuditMe` trigger is registered by default, so `need_events` is always true. Probe's own time (`uni::trigger_probe` debug event; debug build, loaded 22-core box), before → after:

  | n | arm | no user trigger | edge trigger |
  |---|---|---|---|
  | 100k | create (empty L1) | 23–39 ms → not run | 21–24 ms → skipped |
  | 100k | create beside 100k flushed | 345–412 ms → not run | 457–936 ms → skipped |
  | 100k | SET 100k flushed | 1.03–1.30 s → not run | 1.11–1.25 s → 1.33–1.45 s (needed) |
  | 1M | create (empty L1) | 0.32 s → not run | 0.20 s → skipped |
  | 1M | create beside 1M flushed | 3.9 s → not run | 7.1 s → skipped |
  | 1M | SET 1M flushed | 15.0 s → not run | 12.0 s → 10.3 s (needed) |

  Commit wall time, 1M SET without a user trigger: 46.1 s → 15.7 s; 1M create beside flushed with an edge trigger: 41.5 s → 15.7 s (wall times are noisy under the shared load; the probe column is the isolating measurement). Fix: `TriggerRouter::wants_edge_events` gates the edge read (CDC or a trigger with an edge or `PROPERTY_CHANGE` bit); `L0Buffer::created_edges` (allocator-fresh ids from `L0Buffer::insert_edge`) are left out of `pending_edge_candidates`. A required pre-image read (SET on flushed edges with an edge trigger) is one batched read already; not changed.
- **Open.** The vertex half of the probe (`extend_with_l1`) still runs for every vertex on every commit because of the default node trigger (not measured here). The identity memo holds one entry per neighbour of each hub touched, until the buffer rotates or the transaction ends (not charged to `estimated_size`).

### Q4 — snapshots: deletion, catalog rewind, index builds paused during restore

- **Repro (before).** No way to delete a snapshot: its `uni_snapshot_{id}` tags pinned its versions forever. After a restore the catalog kept every post-snapshot change. Measured by the new tests with the fixes reverted: a property renamed after the snapshot read `age = None` for every restored row, and a table created after it was only emptied, so its columns kept their later types. A background build held between its physical commit and its catalog record let the restore finish first (`waited = false`). The restored catalog then held `person_age`, declared after the snapshot and `Online`, and `person_name`'s `last_built_at` had moved to the post-snapshot build.
- **Fix.** (1) `Writer::delete_snapshot` / `Uni::delete_snapshot` / `CALL uni.admin.snapshot.delete(id)` (`Dbms` in `BUILTIN_PROCEDURE_MODES`, refused inside a transaction) / Python `delete_snapshot` sync and async. Under `flush_lock` it runs unpin (`StorageBackend::delete_table_tag`, index datasets via `restore::release_restore_point`), then deletes the restore point, the names and the manifest. When `latest` names the manifest, it republishes a copy under a fresh id first. Refused while a restore runs (flag checked before the lock, so it refuses instead of queueing), while a restore intent is on disk, and while a fork's `parent_snapshot_id` is the snapshot. An unknown id is a typed `SnapshotNotFound`: it now has a tagged display plus `snapshot_not_found_from_message` on the procedure path, and Python's `UniSnapshotNotFoundError` carries `snapshot_id`. `restore_snapshot` also returns this error for an unknown id. (2) `RestorePoint::catalog` is the whole `Schema`, captured before the table versions. `rewind_storage` now drops tables created after the snapshot instead of emptying them; forks are refused, so no branch lives in one. After every table is rewound it runs `install_catalog`, which sets `schema_version = max(live, snap) + 1`, keeps the live `writer_features` / `trusted_unmarked_manifest`, withdraws verifications after the snapshot, re-widens storage, saves, then fsyncs via the new `SchemaManager::set_local_file` / `local_file`. Decision on old points: a restore point without a catalog is refused with a clear error. It was written only by unreleased P1 builds and stores index definitions only; restoring its data under today's catalog is the half-restore this item removes. An interrupted legacy restore found at open is still completed the old way. Recovery at open now pauses compaction. (3) `StorageManager::pause_index_builds` / `index_build_permit` (a shared `tokio::sync::RwLock`). The rebuild worker holds the permit from claiming a task until it has saved the catalog, and `Indexes::rebuild(_, false)` holds it too. `create_snapshot` and `restore_snapshot` take the exclusive side first (builds, then compaction, then `flush_lock`). New seam `index_manager::scalar-built`.
- **Files.** `uni-store`: `snapshot/{manager,restore}.rs`, `runtime/writer.rs`, `storage/{manager,index_rebuild,index_manager}.rs`, `backend/{traits,lance}.rs`. `uni-common`: `api/error.rs`, `core/schema.rs` (`local_file`). `uni-query`: `executor/procedure.rs`, `lib.rs` (oracle test line). `uni`: `api/{mod,indexes,impl_query}.rs`. Bindings: `src/{core,sync_api,async_api,exceptions}.rs`, `.pyi`, `tests/test_stub_drift.py`, `tests/test_e2e_persistence.py`. Docs: Black Book, release notes, the website snapshot/Cypher/Python/Rust pages, and the regenerated `python-api-symbols.md` (which also picked up other agents' stub additions).
- **Tests.** `storage::snapshot_restore_rewinds`: `restore_rewinds_the_catalog` and `restore_procedure_rewinds_the_catalog` (they replace `restore_keeps_later_schema_usable`, which asserted the old semantics), `restore_refuses_a_restore_point_without_a_catalog`, `deletion::{deleted_snapshot_versions_are_reclaimed_and_others_still_restore, delete_procedure_and_refusals}`, and under failpoints `resilience::{index_build_racing_a_restore_cannot_outlive_the_rewind, delete_refused_while_a_restore_runs}`. The reclaim test shows that the `_versions` manifests only the deleted snapshot pinned survive compaction (retention 0) before the delete and are removed after it, while the other snapshot keeps its tags and restores. Three pytests and a doctest were added.
- **Revert check.** Catalog install, drop-not-empty, tag release, both gate sides and the refusal flag were reverted together. All 5 discriminating Rust tests failed, each on its own assertion (see Repro). The files were restored and `md5sum -c` passed.
- **Suites.** uni-store 798/798 and with failpoints 812/812. uni-db integration `/snapshot|restore|index|schema|admin|fork/` 700/700. Failpoints `/restore|snapshot|resilience|recovery|crash/` 148/148. `/declared_endpoint|snapshot_restore_rewinds/` 23/23 (an earlier run failed `declared_endpoint_narrowing_enforced_type_narrows` at its L0 phase, before any snapshot code ran; it passed on rerun after the other agents' planner edits settled). uni-query mode/oracle unit tests 4/4. pytest 1080 passed, 15 skipped. ruff and stubtest clean. Clippy `-D warnings` is clean on uni-store (± failpoints), uni-common, uni-db (± failpoints) and uni-python. uni-query's only finding is `type_complexity` in `locy_lineage.rs`, outside this package.
- **Open.** DDL is not serialized with a restore: an `ALTER`/`CREATE INDEX` running concurrently can interleave with the catalog swap. Builds that the flush path and DDL trigger are not gated (the flush path is serialized by `flush_lock`). Restore and delete take ids, not snapshot names. No `uni snapshot delete` CLI subcommand.

### Q7 — cleanup (props (d), proctx on forks, triggers page, `partial_lance_writes`, counters, docs build)

- **props (d) — reachable, fixed.** Before: a writing unit subquery over `LABEL M (name STRING, properties MAP)` re-emitted `n` with the nested map taken as the container — `RETURN n` gained an `inner` property, `n.properties` read `NULL`; a parameter `{_vid, _labels, name, properties: {inner: 1}}` came back as a node with only `{inner: 1}`, `keys($m) = [inner]`. Root cause: `entity_property_container` took any map-valued `properties` on an entity map as the serialised container. Fix: `value::is_serialised_entity_map` — the container only when every other key is a system field or id spelling; `canonical_entity`, the `index()` UDF and the legacy property read go through it. Ambiguous shape left: a flattened entity whose *only* user property is a map named `properties` reads as serialised (documented on the function).
- **Found alongside, fixed:** after `CALL { WITH n SET ... }`, `properties(n)` and `collect(n)` returned `NULL` for any entity (plain `{name, w}` too) and `n.name` read `NULL` when the entity column was a struct: `apply.rs::value_to_single_row_array` filled a native `Node`/`Edge` struct from its property map only, so `_vid`/`_labels` were null and the struct read as a null entity. Now system fields come from `entity_property` and `_all_props` from the property map.
- **proctx on forks.** New `bugs::procedure_reads_in_transaction_on_fork` (3 tests; {primary, fork} × 4 flush combinations; fork with committed fork data, later primary commits, own uncommitted write). Declared READ/WRITE procedures, wcc, reachability, vector and FTS all matched the transaction's `MATCH` oracle before any change; `uni.graph.project` projected the fork's committed graph and refused after a write; `uni.schema.labels` listed the fork's merged labels. One violation: `uni.schema.labels` `nodeCount` was the storage table's row count (fork: 2 or 3 vs 4; primary: 2 vs 5 unflushed) — no L0, no transaction writes, every stored version. Now an inner `MATCH (n:L) RETURN count(n)` per label through the caller's view (cost: one count query per label per call, was metadata).
- **Triggers/CDC page** `website/docs/features/triggers-cdc.md`, in the nav under Features. `reference/rust-api.md`'s CDC paragraph no longer claims host redelivery (the host delivers only commits after `start`; replay is the provider's).
- **`partial_lance_writes`.** `#[deprecated]` on `UniConfig` and `WriterConfig`; `UniConfig::warn_deprecated_options` (one warning per process, called from `UniBuilder::build`); config reference and parity allowlist say no-op; never Python-bound (`config()` rejects the key), so no stub change. `uni_partial_writes_total` appears in no doc or dashboard.
- **Counters vs TCK — rule: Neo4j's `QueryStatistics`.** The TCK feature files do expect `-properties` for deleted entities (`Delete2` [3], `Delete6`, `Merge1` [14]) and graph-level `+labels` (`Create1` [4], `Merge1` [5]), but `uni-tck` computes those side effects by diffing graph state (`crates/uni-tck/src/world.rs`, `capture_before` / `capture_after`), never from `ExecuteResult` — which is why the TCK passed before. openCypher specifies no execution counters, so they follow Neo4j: `properties_removed` counts explicit `REMOVE` / `SET ... = null` (and overwrites) only, a deleted entity's properties are not counted, `labels_added` is per node. A deleted-property count was implemented and measured (debug, 20k flushed nodes with 3 properties and an edge: `DETACH DELETE` 1.7–2.5 s vs 1.0–1.1 s), then reverted by decision. Kept: `L0Buffer` counts a tombstone once (`DETACH DELETE n, m` with `m = n` reported `nodes_deleted 2`). Documented in the Cypher guide (*Write Counters*), Rust/Python API pages, rustdoc, Black Book.
- **Docs build:** 20 → 0 warnings (`zensical build --strict` after `convert_notebooks.py --no-execute`; CI `docs` job and runbook run the same).
- **Tests.** `bugs::entity_field_name_collision::{flattened_entity_with_a_map_valued_properties_property, unit_subquery_reemits_the_entity_with_its_identity}`, `value::tests::property_container` (3), `bugs::procedure_reads_in_transaction_on_fork` (3), `cypher_write::execute_counters_matrix::deleting_counts_entities_not_their_properties` (incl. the counted-once case); `union_entity_test::a_non_entity_type_conflict_keeps_each_rows_value` now checks the ints are the P ids and the nodes the P nodes. Revert check (all fixes undone together, before the counter decision): every new test failed, including the unit test for flattened maps; restored, md5-identical.
- **Suites.** uni-db integration `/bugs|fork|cypher_read|cypher_write|procedure|vector|search/` 1941/1942; the failure, `constant_in_list_cost_is_linear`, asserts a wall-clock ratio and its own linear control grew 0.7x for 4x input in that run (machine load); it passed 3/3 alone. Item tests after the union change 13/13. uni-common `property_container` 3/3. openCypher TCK schemaless 3925/3925 and sidecar 3925/3925 (1 skipped each). pytest 1080 passed, 15 skipped. Docs `zensical build --strict`: no issues. Clippy `-D warnings` clean on uni-common, uni-store, uni-query-functions; uni-query's only finding is `type_complexity` in `locy_lineage.rs` (Q1's); uni-db `--lib --test integration` clean with that one lint allowed.
- **Open.** `properties(m)` on a variable bound to a map (`WITH {a: 1} AS m RETURN properties(m)`) is refused at plan time ("requires a node, relationship, or map"; `planner.rs::validate_function_call` rejects `VariableType::Scalar`, which may hold a map) — Graph9 [4] only uses a literal; not fixed (planner.rs outside this package's allowed hunk). `Node`/`Edge::try_from(&Value::Map)` still takes any `properties` key as the property map. CDC module docs (`cdc_runtime.rs`) still say "redelivered after a restart" (unverified by test). `uni.plugin.dropDeclared` may leave a declared trigger firing until restart (from reading `registry.remove_named_unique`, unverified).

### Q1 — Locy: non-fold PROB lineage, EXPLAIN children and Mode B, reuse pin, QUERY budget, registered-rule parameters

- **1. Negated (and positive) fact of a PROB rule without a fold, exact mode.** Repro on the diamond: `reach(s,t) AND NOT edge(s,t)`, where `edge` is a PROB rule without a fold. Exact mode gave 0.2235 against a possible-worlds value of 0.045. Root cause: such a rule kept no lineage, so a referenced fact became one fresh `Var::Base`. Fix: when provenance is recorded (`LocyPlanBuilder::with_provenance`), a PROB rule without a fold projects transient lineage columns (`__lin_*`). `LineageStore::record_rows` accumulates its distinct derivations before deduplication, and `publish_rows` keeps those of its published facts, ORed per KEY. A recursive stratum finishes its rules without a fold before correcting the others. Positive references are covered by the same mechanism. Files: `locy_planner.rs`, `planner_locy_types.rs`, `locy_lineage.rs`, `locy_fixpoint.rs`, `locy_program.rs`, `impl_locy.rs`. Default and independence modes are unchanged; the property test checks that default equals possible worlds wherever no warning is raised.
- **2. EXPLAIN Mode A children** for references to non-PROB rules and from PROB rules without a fold. These now carry a `SupportRef` (`__lin_sup_*`, plus `__lin_supp_*` when the target is a PROB rule), read through `RefScope`. The recursive path reads them from the candidates (`transient_support`). A non-PROB fact is certain: `compute_proof_probability(has_prob, …)` gives 1.0, and a non-PROB rule referencing PROB facts gets the product of their probabilities. This matches `probabilistic-logic.md`, which says a referenced fact of a rule without PROB is certain. `locy_assume_explain_abduce::explain_in_an_assume_body…` pinned the old flat tree; its hand-checked shape is now the chain `A→B→D→X`, and body equals top level as before.
- **3. EXPLAIN Mode B (ABDUCE's tree), probed** over recursion, Boolean negation, FOLD COUNT, probabilistic negation, PRIORITY and BEST BY. The first four were correct. Two cases were wrong. PRIORITY: Mode B took the first clause in text order that derives the KEY, so ABDUCE NOT proposed removing `E` (not validated) and never `F`. BEST BY: Mode B matched on KEY alone and explained the 5-weight row instead of the kept 1-weight row. Fix: try clauses highest PRIORITY first, and require the evidence row to match the fact's non-KEY yielded values (`yields_match`). A third defect showed up along the way: a removal under `[e:E|G]` named type `E` for a `G` edge and never validated. Removals now use the bound edge's type (`locy_abduce.rs`). Mode B still re-runs a probabilistic `IS NOT` as a Boolean anti-join (`calling_rule_prob_col: None`). The probe passed anyway (ABDUCE validated the edge), so the defect is not reproduced and is left open.
- **4. ASSUME reuse floor teeth.** Reverting only `offer_baseline` (impl_locy) leaves tracked reuse at 0, as does reverting only the provenance guard in `re_evaluate_strata_reusing`: each guard alone keeps reuse at 0. Reverting both gives tracked reuse 8149 / 17743 with seed 1792452210422130158, and the pin fires; 0 divergences were found. The test now evaluates every check and reports all failures together, so a divergence cannot hide the pin. It also refuses a vacuous pin (no tracked case). impl_locy was restored, md5 OK.
- **5. `allow_partial` deadline.** An in-memory QUERY now checks the deadline and cancellation every 1024 evaluated rows (`check_interrupt`). A `Timeout` from a command is a cut under `allow_partial`. A command that finishes late keeps its answer, but the overrun is reported. The final check uses the instant that `evaluation_time` ends on. Without `allow_partial`, conversion and hydration stop at the deadline (`DeadlinePassed` → `Timeout`). **Stopped (decision conflicts with evidence):** bounding conversion and hydration under `allow_partial`. The documented contract (`troubleshooting.md`: "its result has the complete facts", `LocyConfig::allow_partial`) and the `VID_HYDRATION_MIN_BUDGET` invariant (encoding independent of timing) both oppose it. A deadline-cut run would then always return no facts, or unhydrated ones, because conversion starts after the deadline. Tests: two deterministic `locy_query` unit tests count the checks. A timing-sweep integration test (engine-clock invariant) did not fail under the revert in two runs and took about 100 s, so it was not kept. The late-overrun reporting has no discriminating test.
- **6. Registered-rule parameters.** `RuleDefinition::parameters` (from the pest pairs) → `CompiledRule::parameters`; `check_program_parameters` runs after `merge_registered_rules` (session, `LocyEngine`, and now `PreparedLocy`, which checked nothing up front). Before: `["c"]` then an evaluation error; after: `["c","a","b","d"]`.
- **Revert check** (5 files, one build, restored md5 OK): the item-1 diamond and property tests, the 4 Mode A child tests, the PRIORITY and BEST BY probes, the parameter test and the 2 QUERY unit tests all failed. The probes for recursion, negation, FOLD and probabilistic negation passed (they are controls).
- **Suites.** uni-db `/locy/` 507/507; Locy TCK 553/553 schemaless and 553/553 sidecar; uni-locy + uni-query (lib+tests) 1142/1142; uni-locy-oracle 48 passed / 5 skipped; uni-cypher 138/140, where the 2 failures are `docs_snippets` over gitignored generated `website/docs/examples/python/*.md` and the untracked `features/triggers-cdc.md` (not Q1 files). Clippy `-D warnings` is clean on uni-cypher, uni-locy, uni-query, uni-locy-oracle and uni-db (`--lib --test integration`).
- **Follow-up (coordinator decisions).**
  - (a) Under `allow_partial`, conversion and hydration may run past the deadline, as decided. The docs (troubleshooting, release notes, Black Book) now say so and explain why.
  - (b) A non-fold PROB fact's own value is now exact. Repro: `twice(a,b) :- (a)-[e]->(b), a IS reach TO b, PROB e.p` on the diamond, queried directly in exact mode, gave 0.5215; possible worlds give 0.7. After the fix it is 0.7. In the random graphs, `hop(n1,n1,n1)` gave 0.81 against possible worlds 0.9. Fix: `LineageStore::publish_rows` now returns the facts after `correct_rows`. A row derived more than one way, or whose row formula is not read-once, raises `SharedProbabilisticDependency`; in exact mode and `TopKProofs` it is recomputed as the OR of its derivations. The property test now runs 32 graphs over 4 programs (adding `twice`) and also asserts the non-fold rule's directly queried values, exact against possible worlds and default against possible worlds where no warning is raised.
  - (c) The late overrun now has a deterministic test. A `locy::after-command` failpoint seam sleeps past the deadline after a command completes. `locy::locy_budget_resilience` (failpoint lane) asserts that `incomplete` is set, with the answer kept, under `allow_partial`, and that the program fails with `Timeout` without it; a control run without the seam is complete.
  - Revert check: removing both late-overrun checks and the exact recomputation in `correct_rows` makes all 3 new tests fail (`incomplete` unset; 0.5215; 0.81). Restored, md5 OK.
- **Open.** Mode B still treats a probabilistic `IS NOT` as Boolean (not reproduced).

### Q9 — storage isolation (versioned reads after deletes, bulk version reservation, Q2 follow-ups)

- **Versioned reads after a delete (Q8 finding).** Repro (`storage::versioned_read_after_delete`, declared and schemaless): seed `v = 1..5`, flush, snapshot, `DETACH DELETE v = 2` + relabel + create, flush. Before: pinned and `VERSION AS OF` read `[1, 3, 4, 5]`; a transaction begun before the delete read the same; a label removed after the snapshot made `(a:A)-[:R]->(b:B)` return nothing; schemaless `R` lost deleted edges and showed new ones on a flush-manifest pin. Forks were already correct. Root causes, each confirmed by its own revert: (1) the flush wrote vertex tombstones as a `MergeInsert` on `_vid`, restamping every stored row of the vertex with the delete's version; tombstones are now appended full rows (main and label tables) and the partial-tombstone helpers are deleted; `load_vid_labels_index` now ranks rows by version instead of `_deleted = false` + last row. (2) A pinned view copied the live VID-to-labels index; `rewind_vid_labels_index_to_pin` re-reads the vertices changed above the pin. (3) `find_edges_by_type_names` applied no snapshot ceiling; it uses `edge_row_visibility`. Found on the way, measured: (4) semantic compaction collapsed history under every pinned reader (declared label read `[1, 3, 5]` after `uni.admin.compact()`; a transaction too), contrary to the Black Book; explicit snapshots now read their restore point's tagged table versions (`StorageBackend::at_table_versions`), compaction merges no vertex row above the oldest pin and folds no delta above the oldest snapshot-view pin (`PinnedVersions::begin_merge`), and records a horizon (`catalog/compaction_horizon.json`) below which a flush-manifest pin made later is refused (`InvalidArgument`). (5) The CSR warm filtered L2 adjacency by `_version`, a column it does not have: any pinned read after a compaction failed with a schema error. (6) Appended tombstones hit `NOT NULL` list/sparse/multivector columns whose builders write null for a deleted row (`fork_index_sparse`, `fork_index_multivector` failed); `fill_deleted_nulls` gives deleted rows a zero value, live nulls still refused. Also updated: `compaction_property_fidelity::a_truncated_delete_still_tombstones_every_label` now checks each vid's newest row (it read the first row, i.e. assumed in-place tombstones).
- **Bulk version reserved at first write (Q5 finding).** Repro without failpoints (`storage::bulk_snapshot_hwm::ceilings_captured_while_a_load_is_staged_exclude_it`), measured with the exclusion disabled: a transaction begun while the load was staged read 2 then 22 rows; a flush manifest cut then read 22 (in-process and after reopen); the explicit snapshot read 2 (restore-point read). With failpoints (`bulk_isolation_resilience::ceilings_captured_mid_publish_exclude_the_load_resilience`, publish parked after `main_edges`): the mid-publish snapshot read `(30, 1, 20, 0)` (load vertices, no load edges) and restoring it kept them. Fix: `L0Manager::reserve_bulk_version` reserves and records the version under the buffer lock every ceiling capture reads; `SnapshotView::unpublished_bulk` and `SnapshotManifest::unpublished_bulk_versions` carry the versions to exclude for good (`pinned_at_version_excluding`, `visibility_at`, CSR warm); the load's own manifest and `raise_snapshot_hwm` recompute/clear it; restore deletes rows above the ceiling or of unpublished versions (`drop_rows_outside_the_snapshot`). Rotation no longer reuses a version reserved across it (by construction; not reproduced). Q5's gate, re-drive and recovery suites pass unchanged.
- **(a) Identity memo charged.** `EdgeIdentityMemo::charge` adds 40 B/identity + 24 B/scanned pair to `estimated_size` and `size_bytes`; merge and fork leave the charge out. Tests: `identity_memo_is_charged_to_its_buffer` (the memo-filling update grew 133 B before, 8157 B after), `merge_and_clone_leave_the_identity_memo_charge_out`.
- **(b) Vertex probe measured and gated.** `triggers::trigger_vertex_probe_cost` (ignored harness; probe time from the `uni::trigger_probe` debug event, debug build). Before, no user trigger: 100k `SET` flushed 0.73–1.04 s, create beside flushed 1.0–1.07 s; 1M 18.0 s and 53.9 s (commit 67.5 s). Wasted: the only trigger was `:_AuditMe`. Fix: candidates filtered by `TriggerRouter::wants_node_events_for` (unless CDC) and `L0Buffer::created_vertices` skipped. After: not run in those arms (1M create-beside commit 13.3 s); with a node trigger on the label, `SET` still reads (18.2 → 18.1 s at 1M) and create-beside (34.4 s) is skipped. The transaction-level gate is covered by this measurement only; the router predicate and created-vertex skip have unit tests.
- **Revert checks.** One mechanism at a time, each restored with `md5sum -c` OK: append tombstones → 4 tests fail; restore-point reads → 2; label rewind → 5; schemaless ceiling → 2; compaction floor → 2; L2 filter → 2; null fill → 2 (the fork tests); label-index load → 1; bulk reservation → staged-window test (tx 22 ≠ 2) and mid-publish test; restore deletion → mid-publish test (`restored`); memo charge, memo merge, created-vertex skip → their tests.
- **Suites.** uni-store + uni-bulk + uni-plugin-host + uni-common 1102/1102; uni-store + uni-bulk with failpoints 829/829; uni-db `/snapshot|restore|fork|bulk|storage|cypher_read|edge_source|trigger|compaction|vacuum|time_travel|update_edge_identity/` 1158/1158; uni-db failpoints `/bulk|resilience|recovery|crash|snapshot|compaction/` 255/255; TCK schemaless 3925/3925, sidecar 3925/3925 (1 skipped each). Clippy `-D warnings` clean: uni-common, uni-store (± failpoints), uni-bulk, uni-plugin-host, uni-db (± failpoints).
- **Open.**
  - A transaction's edge tier is still unpinned (live CSR): its traversals see edges committed after it began, a bulk load's included.
  - A flush-manifest pin is refused after compaction merged its history; only explicit snapshots are durable time-travel points. A session pinned to a flush manifest holds compaction back for as long as it lives.
  - `get_vertex_ext_ids` still filters `_deleted = false` with no version ranking; with appended tombstones a deleted vid keeps its entry (only consulted for vids taken from query rows; unmeasured).
  - Each transaction begin deep-copies the VID-to-labels index (`pinned_at_version`), O(vertices) per transaction (from the code; unmeasured).
  - The built-in audit trigger's label filter is `":_AuditMe"` while event rows carry `_AuditMe`, so it likely never fires (from the code; unverified).
