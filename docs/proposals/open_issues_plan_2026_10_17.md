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
