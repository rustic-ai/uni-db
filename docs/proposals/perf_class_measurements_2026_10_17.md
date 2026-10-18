# Perf-class audit — measurements (2026-10-17)

Measures the candidates predicted from code reading in the 2026-10-17 audit
(anchoring / filters / recursion). Tree: local `main` at `0a12624a8` plus the
uncommitted #301 recursive-fixpoint anchoring change.

## Method

- Probe: `crates/uni/tests/common/perf/perf_class_probe.rs` (registered in
  `tests/common/perf/mod.rs`), four `#[ignore]` tests:
  `perf_class_probe_cypher`, `perf_class_probe_locy_anchor`,
  `perf_class_probe_recursion`, `perf_class_probe_timing`.
  Run: `cargo nextest run -p uni-db --test integration --run-ignored all -E 'test(perf_class_probe)' --no-capture`
  (`PERF_PROBE_SCALES=2000,8000` sets the Locy anchoring scale; it defaults to 100,400).
- Every fixture is bulk-loaded (`bulk_writer`), flushed, then reopened, so all
  rows are read from L1. Debug build.
- Counters come from `profile()`: `QueryMetrics.rows_scanned` / `index_scans`,
  per-operator `actual_rows` (Cypher `runtime_stats`; Locy per-stratum,
  per-rule and per-iteration operator trees), plus `delta_facts`, facts and
  iterations. Wall time is recorded next to them.
- Verdict rule: **CONFIRMED** means a counter grows with the whole label or
  relation while a control (indexed equality, anchored formulation, or
  single-rule form) stays O(answer) at both scales. **REFUTED** means the
  counter tracks the control. **UNMEASURABLE** means the counters cannot see
  the work; the reason is given.
- Cypher graph fixture (`graph_fixture`): `:N {id, uid, name, x}` with indexes
  on `uid` and `x` (`name` = uid of the next node, unindexed), `N/8` `:S`
  nodes, and `2N` `:R {w}` edges (`i→i+1`, `i→7i+3`). Scales: N = 2000 and 8000.
- Locy anchoring fixture (`chains_fixture`): k disjoint 20-node `:T` chains
  over `:E {uid (indexed), seed}`. Scales: N = 2000 (k=100) and 8000 (k=400).
- Recursion fixtures: the #301 shape (chain of length L plus 4000 noise
  edges, L = 20 / 80), and a diamond lattice of k = 6 / 12 diamonds.

## Results

Notation: `a → b` = value at the small scale → value at the 4x scale (unless
another axis is stated). "ctl" = control query.

| ID | Verdict | Probe counters (small → large) | Control counters (small → large) | Growth probe / ctl | Mechanism (located) |
|---|---|---|---|---|---|
| A1 Locy `QUERY … WHERE a.uid=` over recursive rule | **CONFIRMED** | facts 19,000 → 76,000; rows_scanned 73,000 → 292,000; wall 790 → 3,797 ms; identical to `QUERY reach` with no WHERE | anchored rule (`WHERE a.uid='c0_0'` in the base clause): facts 19 → 19; rows_scanned 58 → 58; wall 55 → 50 ms | 4.0x / 1.0x | The QUERY WHERE is applied after the full fixpoint; nothing pushes the binding into the rule (no magic sets). |
| A1b (new) Recursive clause whose IS-ref is on the pattern's **target** (`(a)-[:T]->(m) WHERE m IS reach TO b`) | **CONFIRMED** | per-iteration `GraphScanExec` = 4,000 → 16,000 (2N) in every iteration 1–15, while delta falls 1,800 → 400 | source-side IS-ref (`(o)-[:T]->(b) WHERE o IS d`): `GraphScanExec` 1 per iteration (#301 fix working) | 4.0x / 1.0x | The #301 anchoring covers a derived ref on the traversal source only; a ref on the target re-scans the label every iteration. |
| A2 Cross-stratum IS-ref in a non-recursive clause | **CONFIRMED** | rows_scanned 2,005 → 8,005; at N=100/400 the `nb` ops are GraphScan = N, GraphTraverse ≈ 0.95N, then a HashJoin against the 1 `sd` fact | single-rule (`e.uid='c0_0'` inline): rows_scanned 3 → 3; Cypher form: 2 → 2 | 4.0x / 1.0x | The lower-stratum fact set is hash-joined *after* a full label scan + traverse; it is not used as the scan anchor. |
| A3 Hydration of an unreturned rule | **CONFIRMED** | adding an unreturned `allv` (N facts) to a 1-fact program: rows_scanned 3 → 6,003 / 24,003; wall 13 → 271 ms (N=2k) and 13 → 1,958 ms (N=8k); fixpoint time only 7.4 / 18.2 ms; `allv` KEY came back as `Value::Node` | program without `allv`: rows_scanned 3 → 3, wall 13.4 → 12.9 ms | wall 7.2x for 4x N (super-linear) / 1.0x | `enrich_vids_with_nodes` (`crates/uni/src/api/impl_locy.rs:2108`) hydrates every derived rule, returned or not, via `MATCH (entity) WHERE id(entity) IN [<all ids>]` (`impl_locy.rs:1915`). Cause of the super-linear growth, verified by per-operator profile: the residual `_cypher_in` FilterExec scans the N-literal list per row (38.6 → 585.8 ms, 15.2x for 4x N), and every rule was hydrated twice (4N+2 rows). Fixed in P3, see the plan. |
| A4 ASSUME re-evaluation | **CONFIRMED** | ASSUME (adds 1 isolated node) + same QUERY: rows_scanned 144,034 → 576,034; wall 1,467 → 6,660 ms | plain program: rows_scanned 73,000 → 292,000; wall 724 → 3,753 ms | rows_scanned = 1.97x the plain program at both scales | The hypothetical re-derives every stratum from scratch: O(program), not O(change). (The `rows()` of the ASSUME run read 0 because the first command result is not a Query; this is a reading artifact.) |
| A5 VLP / edge-pattern property map (`-[:R*1..3 {w:1}]->`, `(b:N {x:2})`) | **CONFIRMED (wall + code; counters blind)** | edge map: 26.6 → 65.1 → 246.1 ms (N = 2k/8k/32k); target map: 34.7 → 99.0 → 414.0 ms; rows_scanned 1 at every scale | VLP without the map: 8.0 → 5.8 → 7.6 ms; 1-hop with the same map: 9.1 → 7.3 → 12.4 ms | ~linear in edge count / flat | `build_edge_property_filter` (`crates/uni-query/src/query/df_graph/traverse.rs:3645`) enumerates every edge of the type ("`None` source-vids ⇒ whole-type scan"), and `build_accepting_vid_filter` (`traverse.rs:3561`) does the same for target-property maps, before the anchored walk. Neither pre-scan reaches `rows_scanned`, which is a counter blind spot in its own right. |
| A6 Named path with an anchor on the far end (`p = (a)-[:R]->(b {uid:…})`) | **CONFIRMED** | `GraphScanExec` 2,000 → 8,000; `GraphTraverseExec` 4,000 → 16,000; wall 33 → 85 ms | same pattern without `p =`: GraphScan 1 → 1, traverse 2 → 2. A named path anchored on the *near* end is also 1 → 1 | 4.0x / 1.0x | Binding a path variable disables anchor reversal, so the scan starts at the unanchored `a`. Only the far-end-anchored form is affected. |
| A7 Disjoint patterns joined by WHERE | **CONFIRMED (anchored form) / REFUTED (cross product)** | `(a {uid:'u42'}), (b:N) WHERE b.uid = a.name`: rows_scanned 2,001 → 8,001 (full `b` scan + HashJoin); the 2-MATCH form is the same | direct `b.uid='u43'`: 1 → 1. Unanchored `(a:S),(b:N) WHERE a.name=b.uid`: rows_scanned 2,250 → 9,000 = \|S\|+\|N\|, through a HashJoin, with no cross product | 4.0x / 1.0x | A bound value from `a` is not passed sideways into `b`'s indexed lookup; the planner hash-joins two scans. No cross product was produced. |
| A8 shortestPath with an unbound / filtered target | **CONFIRMED (severe)** | `shortestPath((a {uid:'u0'})-[:R*]->(b:N)) WHERE b.x < 20`: GraphScan(b) 2,000 → 8,000; CrossJoin 2,000 → 8,000; ShortestPath rows 2,000 → 8,000; Filter after it 20 → 20; wall 6,148 → 83,116 ms. Fully unbound target at N=2k: 5,508 ms | bound target `b {uid:'u19'}`: rows_scanned 2 → 2, wall 11.5 → 11.0 ms | wall 13.5x for 4x N (≈N²) / 1.0x | Source × every target is cross-joined, then `compute_shortest_path` runs one BFS per row (`crates/uni-query/src/query/df_graph/shortest_path.rs:819-826`), which is \|B\| BFS. The `b.x < 20` predicate is applied **after** the path search instead of being pushed into `b`'s scan. Not bidirectional. |
| A9 Parameter or literal on the left (`$p = n.uid`, `'u42' = n.uid`) | **CONFIRMED** | rows_scanned 2,000 → 8,000; index_scans 0 (both the `$p` and the literal forms) | `n.uid = $p` and `{uid: $p}`: 1 → 1, index_scans 1 | 4.0x / 1.0x | Index pushdown matches only `prop = value`, not the commuted form. |
| A10 `{ext_id: …}` pushdown | not run | — | — | — | Planner comment (`planner.rs:7351`) says an ext_id equality is pushed down for unlabelled nodes. No probe was written. |
| F1 Range `<` / BETWEEN on an indexed int | **CONFIRMED** | rows_scanned 2,000 → 8,000 for a 10-row answer | indexed `x = 42`: 1 → 1 | 4.0x / 1.0x | Only `=` and `IN` on a scalar index are pushed to Lance; ranges become a full scan plus FilterExec. |
| F1 `STARTS WITH` on an indexed string | **CONFIRMED** | 2,000 → 8,000 (answer 11 → 111) | `uid = 'u42'`: 1 → 1 | 4.0x | Same: no prefix pushdown. |
| F1 `IS NULL` on an indexed property | **CONFIRMED** | 2,000 → 8,000 (answer 0) | 1 → 1 | 4.0x | Same. |
| F1 `IN [..]` on an indexed property | **REFUTED** | rows_scanned 3 → 3, index_scans 1 | — | 1.0x | IN is pushed to the index. |
| F1 unindexed `=` / `<>` | expected (no defect) | 2,000 → 8,000 | — | — | No index exists, or the predicate is non-selective; a full scan is required. |
| F2 Edge-property predicate `r.w > 5` / `r.w = 7` | **REFUTED** as a whole-relation cost | anchored: traverse 2 → 2, then Filter; unanchored `r.w = 7`: traverse 4,000 → 16,000, the same as its unfiltered control | anchored ctl traverse 2 → 2; unanchored ctl 4,000 → 16,000 | equal to ctl | The predicate is applied after the traversal, but the traversal is O(degree) when anchored. Side effect: the edge predicate adds an index_scan and ~2.3x wall on the unanchored form (15 → 34 ms, 50 → 118 ms), consistent with a separate edge-property fetch. |
| F3 Target-node filter after hydration | **REFUTED** | rows_scanned 3 → 3 (anchor + 2 targets) | 1 → 1 | 1.0x | O(degree). |
| F4 `index_hits` semantics | measured | `index_hits` = 1 on an index-served `GraphScanExec` whether it returns 1 or 3 rows, at both scales; 0 on full scans | — | — | It counts index-served scan calls, not rows or `_vid` chunks. The "counts chunks" prediction is not supported. |
| R1 Recursive ALONG on a diamond lattice | **CONFIRMED (exponential)** | ALONG with no BEST BY: facts 252 → 16,380 for k = 6 → 12 (nodes 21 → 39); `QUERY … b.uid='s1'` returns 2 identical rows (`c = 2`, one per path) | plain reach: facts 18 → 36; ALONG + BEST BY: facts 18 → 36, but `BestByExec` rows 114 → 444 (see R2) | 65x for 2x nodes / 2x | Without BEST BY, each derivation path is its own fact even when `(KEY, value)` coincide (per-path identity), so facts = paths = O(2^k). Recursive `FOLD SUM` is rejected at compile time (non-monotonic); MSUM was covered by the #301 probe. |
| R2 BEST BY delta = all facts | **CONFIRMED** | chain L = 20 → 80: `DerivedScanExec` 190 → 3,160 (= Σ1..19 → Σ1..79); `BestByExec` and `GraphTraverseExec` the same; wall 102 → 390 ms | plain reach: DerivedScan 19 → 79, traverse 19 → 79 | 16.6x for 4x L (O(L²)) / 4.2x | `merge_best_by` sets `self.delta = self.facts.clone()` whenever anything changed (`crates/uni-query/src/query/df_graph/locy_fixpoint.rs:1607`), so the next iteration's delta is the whole fact set, not the improved rows. |
| R3 Non-linear recursion (`a IS tc TO m, m IS tc TO b`) | **CONFIRMED** | per-iteration `DerivedScanExec` ≈ 2×\|facts so far\| (N=2k, iteration 5: 36,800 at delta 600; the final delta-0 iteration: 38,000); HashJoin rows summed over iterations 722,800 → 2,891,200 | linear TC on the same data: HashJoin total 19,000 → 76,000 (= facts) | 4.0x in data, but per iteration ∝ facts, not delta (iteration 6 does 247k / 988k join rows for delta 0) | Both IS-refs read the full relation each iteration (F⋈F); no Δ⋈F ∪ F⋈Δ rewrite. |
| R3 Mutual recursion (even/odd) | **CONFIRMED** | chain L = 20 → 80: `ev` DerivedScan 55 → 820 (= Σ1..10 → Σ1..40), `od` 65 → 860; `ev` traverse 54 → 819 | single-rule reach: DerivedScan 19 → 79 | O(L²) / O(L) | In a mutually recursive stratum each rule reads the other rule's full relation every iteration; the #301 delta anchoring applies only to self-refs. |
| R4 Dedup anti-join over all facts once facts ≥ 300 | **REFUTED at this scale** (wall only; counters blind) | chain L=80 with an extra base clause seeding ~2,002 → ~8,002 facts (noise 4k → 16k): 1.45 / 1.42 / 1.48 → 1.44 / 1.46 / 1.41 ms per iteration (3 interleaved rounds, load < 4) | plain (same chain, no extra facts): 1.29 / 1.26 / 1.44 → 1.26 / 1.27 / 1.32 ms per iteration | extra ≈ +0.15 ms per iteration, flat across 4x accumulated facts | Code: `compute_delta` → `arrow_left_anti_dedup(candidates, &self.facts, …)` once total facts ≥ `DEDUP_ANTI_JOIN_THRESHOLD = 300` (`locy_fixpoint.rs:1365-1369`, `1713`). That is O(all facts) per iteration by construction, but the cost does not show at 8k facts. Dedup is not an operator in the profile tree. |
| R5 IS NOT banned set rebuilt per iteration | **CONFIRMED** (wall, stable over 3 rounds; counters blind) | chain L=80, delta = 1 per iteration, banned 2,002 → 8,002 facts: 2.28 / 2.34 / 2.35 → 5.56 / 5.42 / 5.43 ms per iteration; locy total 192–199 → 456–468 ms | plain (no IS NOT): 1.26–1.44 ms per iteration at both sizes | overhead over plain 1.0 → 4.2 ms per iteration (4.2x for 4x \|banned\|) on every iteration, independent of delta | `apply_anti_join_composite` (`crates/uni-query/src/query/df_graph/locy_complement.rs:395-402`) rebuilds a `HashSet` from every negated fact, called per clause per iteration after cloning `entry.data` (`locy_fixpoint.rs:2133-2159`). The anti-join is not an operator in the profile: the operator rows match the plain run (DerivedScan 19/79). |
| R6 Physical plan rebuilt per iteration | **UNMEASURABLE** (fixed overhead, not a scaling cost) | per-iteration floor at delta = 1: 1.26–1.44 ms with 4k noise edges and 1.26–1.32 ms with 16k | — | flat in graph size | `execute_subplan_with_outer_vars` builds a new `HybridPhysicalPlanner` for every clause body on every iteration (`crates/uni-query/src/query/df_graph/common.rs:1852`). The profile exposes no planning time, so the floor cannot be attributed to planning. Either way it is ~1.3 ms × iterations and does not grow with data. |

### Side finding (not a candidate)

During the probes a background compaction task hit a debug assertion twice:
`EID … from Delta L1 not found in main_edges after compaction`
(`crates/uni-store/src/storage/compaction.rs:764`). Both times it fired right
after a probe's database and its tempdir were dropped.

Resolved by a follow-up investigation: there is no data loss. With the
directory kept alive and compaction forced, all 2,000 bulk-written EIDs are
present. The assertion fires because background compaction outlives
`Drop for Uni`, and `exists_by_eid(..).unwrap_or(false)` turns Lance's "Not
found" on the deleted directory into "missing". The fix is tracked as W1 in
`open_issues_plan_2026_10_17.md`.

## Ranking of confirmed items (impact × likely frequency)

1. **A8 shortestPath with an unbound or filtered target.** Cost is ≈N² (6 s → 83 s
   for 2k → 8k nodes) on a common query shape, and the target predicate is
   applied after the search.
2. **A1 Locy QUERY/DERIVE with a bound key evaluates the whole fixpoint.** This is
   the usual way a Locy rule gets queried. Facts ∝ relation (76k for a 19-row
   answer), 75x slower than the anchored form at N=8k.
3. **A3 hydration of every rule, returned or not.** It runs on every Locy call that
   has a large helper rule, grows super-linearly (2 s for 8k unreturned facts
   against a fixpoint of 18 ms), and outweighs the fixpoint itself.
4. **F1 range / STARTS WITH / IS NULL ignore scalar indexes.** Very common;
   linear full scan for a selective answer.
5. **A9 commuted equality (`$p = n.uid`) skips the index.** Common in generated
   or ORM queries; full scan instead of 1 row.
6. **R2 BEST BY and R3 mutual / non-linear recursion re-read all facts each
   iteration.** O(L²) or worse (R3 non-linear: ~1M join rows on the last,
   delta-0 iteration at 8k). BEST BY is the standard shortest/cheapest-path idiom.
7. **A2 cross-stratum IS-ref and A1b target-side IS-ref scan the whole label.**
   Linear in the label for a 1-fact anchor. Rule layering is common, so this is
   frequent.
8. **A5 VLP property maps pre-scan the whole edge type or label.** Linear,
   invisible to `rows_scanned`, ~250–400 ms at 32k.
9. **A7 anchored disjoint patterns.** Linear in `b`'s label; HashJoin, not a cross
   product.
10. **R5 IS NOT rebuilds the banned set every iteration.** Linear in \|banned\| per iteration (+4.2 ms per iteration at 8k banned, 4x the plain iteration). Common in exclusion rules over long recursions.
11. **A4 ASSUME full re-evaluation.** It doubles the program's cost, but ASSUME is
    rarer.
12. **A6 named path anchored on the far end.** Linear, but the shape is less common.
13. **R1 per-path ALONG facts.** Exponential, but only without BEST BY on
    DAGs with many equal-cost paths. It may be intended semantics; the
    cost should be documented.

Inferred rather than measured: that R5's cost is the per-iteration `HashSet` rebuild (the code shows the rebuild; the timing confirms the cost is linear in \|banned\| per iteration), the attribution of R6's floor to planning, the O(N²) explanation for A3's super-linearity, and the compaction-assert explanation. A first R4/R5/R6 timing run taken under load average 70–84 from a concurrent build was discarded. The figures above come from a rerun at load < 4.
