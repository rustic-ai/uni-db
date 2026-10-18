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
