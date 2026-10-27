# bbtree applications in Uni: investigation and evaluation

Date: 2026-10-26. Status: research and isolated probes; no production adoption.

Follow-up: [real-Value tests, timing, profiles, and public-query controls](bbtree_followup_2026_10_26.md) refine the recommendations below.

## Decision

Do not replace collections across Uni. The most promising application is a new,
generation-scoped L0 property index, where avoiding repeated label-member scans
could matter much more than the choice between two B-trees. Existing bulk-load
interval lookup is a smaller, direct candidate. Immutable startup accelerators
are worth a separate experiment. Most existing ordered maps are small or carry
contracts that a collection swap does not improve.

The shared `bbtree::BBTreeMap` has an unresolved Rust data-race concern. The
`unsync` collection and immutable file maps should be evaluated separately from
that concurrency implementation. This report does not certify either as safe.

## Scope and evidence

The investigation searched Rust sources across `crates/` and the Rust Python
binding, then followed storage, query, Locy, bulk, fork, plugin and CRDT paths.
The initial ordered-collection search found 483 matching lines, including tests,
imports and comments: this is not a count of production maps or bottlenecks.
The matrix below groups the application families identified by that search and
the adjacent hash-map, indexing and snapshot code. It is a source review, not an
exhaustive dynamic profile of every workload or transitive dependency.

Evidence levels used here:

- **Local probe:** isolated operation on this machine, without the Uni runtime.
- **Source:** current implementation and its documented contracts.
- **Upstream:** author measurements, not independently reproduced here.
- **Hypothesis:** plausible application whose end-to-end benefit is unmeasured.

Upstream design review is pinned to bbtree commit
`f0a5d361ecb4df176b3e07d4e33bb399dc3d22bf`. Local probes use the published,
unmodified `bbtree 0.1.5` crate. These are separate source snapshots.

## What is available to use

| Component or idea | Useful workload | Integration implications |
|---|---|---|
| `unsync::BBTreeMap` / `BBTreeSet` | Mutable ordered data owned by one task, or protected by an existing external lock | Retains the lock; does not make compound state transitions concurrent. Check API, trait and serialization compatibility. |
| Shared `BBTreeMap` | Many readers of a mutable ordered index | Optimistic traversal, leaf callbacks and copied values; no snapshot scans or multi-key atomicity. Rust racing-read concern applies. |
| Inline keys and prefix skipping | Large ordered indexes of integers, bytes or strings | Compound keys need a correct order-preserving encoding; an arbitrary Rust tuple does not automatically gain inline-key optimization. |
| Batched lookups / inserts | Many independent probes of an index larger than cache | Caller must expose batches; transactions and batch atomicity remain the caller's responsibility. |
| `bbtree-mmap` / file maps | Immutable, derived lookup tables reused across restarts | Local files, immutable mapping lifetime, layout/version checks, publication and invalidation need design. Not a durable transactional database replacement. |
| `bbtree-store` | Variable-length byte records with allocator-based memory accounting | A cache building block; eviction policy, visibility, encoding and concurrency safety remain concerns. |
| SIMD and memory-access interleaving as techniques | Existing index search kernels and batched hydration | Can be adopted without changing the storage format or importing the shared map. Benchmark the actual caller. |

## Application matrix

“Potential” means an opportunity to evaluate, not a demonstrated Uni speedup.

| Application | Current source and semantics | Benefit assessment | Recommendation |
|---|---|---|---|
| L0 secondary property index | `runtime/columnar_scan.rs::l0_prefilter_candidates` probes every L0 label member, then merges verdicts across buffers | **High conditional potential:** seek matching values instead of repeatedly inspecting nonmatching members. This is an indexing change; std trees/hash indexes may deliver most of the gain. | Highest-priority design/profiling candidate. Start with declared INT64 properties and a restricted query shape. |
| Bulk-load VID-to-label intervals | `uni-bulk/src/bulk.rs::load_vid_labels`, `record_load_vid_labels`, `endpoint_labels`; predecessor seek over compressed VID runs | **Moderate, size-dependent:** local bbtree gains appear only at larger interval counts. A sorted vector is faster in the read-only probe. Number of runs, not vertices, controls size. | Measure real interval counts and interleaved insert/lookup patterns before changing. |
| Sparse embedding normalization | `runtime/writer.rs::sparse_pairs_to_value`; `u32` term aggregation, duplicate sums, sorted output | **Poor bbtree candidate in local probes.** Stable sorting plus reduction is a stronger alternative for larger inputs. Inference may dominate total latency. | Consider an independent sort/reduce experiment, preserving float summation order. |
| Pinned version counts | `storage/adjacency_manager.rs::PinnedVersions`; `entry`, minima, refcounts and merge horizon under one mutex | **Poor at small sizes:** local tree-operation probe regresses. Collection concurrency cannot replace atomic pin/horizon registration. | Keep std unless a real profile and large distinct-version counts justify work. |
| Commit announcement sequencing | `runtime/publish_sequencer.rs::State::{ready,lsns}` | **Low:** sizes depend on outstanding commits/gaps; issue/release order and announced LSN advance are compound invariants. | Retain mutex and semantics; no claim that a concurrent map removes serialization. |
| Bulk visibility and reserved versions | `storage/bulk_gate.rs::GateState`, `runtime/l0_manager.rs::bulk_reserved` | **Low:** gate already publishes a shared immutable array; map replacement does not remove epoch/snapshot work. | Keep current representation pending profiling. |
| Deferred trigger queue | `uni-plugin-host/src/triggers.rs::DeferralQueue`; ordered deadlines, split/drain, sidecar generations | **Conditional:** large queues may benefit, but `Instant` is not a built-in inline key, callbacks and persistence can dominate. | Benchmark actual queue-size distribution; compare std, heap and timing-wheel designs when appropriate. |
| CDC pending checkpoints | `uni-plugin-host/src/cdc_runtime.rs`; plugin-name-to-LSN map | **Low:** likely governed by plugin count and checkpoint I/O; no cardinality profile collected. | Do not prioritize a map swap. |
| Correlated query cache keys | `uni-query/.../apply.rs::canonical_params_key`; canonical `BTreeMap<String, Value>` inside a HashMap key | **Poor bbtree result in small-key surrogate.** Sorted vector improves construction/hash probe. `Value` cloning, equality and subplan work are not measured. | Evaluate a collision-safe canonical vector/row encoding before bbtree. Keep full equality checks. |
| Locy DISTINCT and canonical properties | `locy_query.rs`, `locy_ast_builder.rs`, `locy_abduce.rs`; sorted row/property representations | **Same conditional alternative:** per-row canonicalization can accumulate, but small maps do not resemble million-key lookup benchmarks. | Profile, preserve Cypher numeric equivalence and first-row retention; try sorted representations. |
| Locy compilation and name sets | `uni-locy/src/{binding_plan,magic,incremental}.rs`, reused-stratum and bound-VID sets | **Low/unknown:** mostly deterministic bookkeeping; no evidence it dominates execution. | Preserve deterministic output; optimize only after profiling compile-heavy workloads. |
| Locy fixpoint dedup, BEST BY and joins | `locy_fixpoint.rs::RowDedupState` already uses Arrow row encoding and a persistent HashSet; other indexes use hash keys | **Not a direct fit:** equality and grouped lookup do not require sorted order. New range-bound rule evaluation could use an ordered index. | Keep hashing for equality; avoid replacing delta evaluation or DataFusion joins with a generic tree. |
| L0 primary ID/property maps | `runtime/l0.rs`, `SimpleGraph`; HashMaps, FxHashMaps, adjacency vectors | **Weak direct fit:** local numeric point probes favor hashing. Changing these maps does not eliminate multi-map consistency and generation freezing. | Keep point maps; add targeted secondary indexes rather than replace the whole L0. |
| VID remapping and VID-label snapshots | `runtime/vid_remapper.rs`, `storage/vid_labels.rs`; dense vectors, hashes, sharded Arc copy-on-write | **Weak:** reverse array lookup and cheap shard sharing are distinct advantages. bbtree clone is not a persistent-tree snapshot. | Preserve these designs. Benchmark any derived static lookup separately. |
| Graph adjacency and algorithms | `storage/{csr,adjacency_overlay,shadow_csr,adjacency_manager}.rs`, `SimpleGraph`, `uni-algo` | **Poor direct fit:** CSR already offers contiguous neighbor iteration; a B-tree does not improve BFS/SSSP/ANN mathematics. Ordered composite edges are a redesign possibility for other query shapes. | Retain CSR/vector paths. Compare bulk prefetching techniques where profiling finds dependent misses. |
| UID resolution and fork promotion | `storage/index.rs::UidIndex`, `uni-fork/src/diff.rs::batch_resolve_primary_vids`; persistent Lance index plus liveness verification | **Conditional derived-cache opportunity:** fewer repeated storage lookups could matter. Cache hit behavior and invalidation are unmeasured. | Use versioned/branch-aware keys and preserve primary liveness checks and schemaless fallback. A faster lookup must not treat fork UIDs as live primary rows. |
| FTS/sparse posting dictionaries and JSON paths | `storage/{inverted_index,sparse_index,json_index}.rs`; hashes, posting vectors and persistent datasets | **Conditional for new prefix/range dictionaries**, weak for existing equality-only terms. Changing the dictionary does not accelerate scoring arithmetic or posting intersection by itself. | Evaluate dictionary size, prefix workload, update cost and memory against hashes/sorted arrays. |
| Persistent Lance scalar/vector indexes | `backend/lance.rs`, `storage/index_manager.rs`; native Lance APIs and scalar B-tree pages | **No automatic benefit:** Lance's persistent scalar B-tree is not the MemWAL skiplist in the bbtree experiment. Vector/FTS kernels are different again. | Separate upstream kernel experiments from collection replacement. Preserve branch-local index behavior. |
| Lance MemWAL | No MemWAL/ShardWriter/LsmPointLookupPlanner use found in Uni crates | **No direct current benefit.** Adopting that path could gain a different ingestion/index architecture, but must coexist with or replace existing WAL/L0/visibility machinery. | Treat as an architectural comparison, not an upgrade toggle. |
| Immutable startup accelerators | VID-label/UID maps or other reproducible local lookup tables rebuilt at open | **Moderate hypothesis:** file maps could avoid rebuilding mutable trees. Need an actual startup profile and sorted-array/Arrow comparison. | Build only derived, disposable artifacts first; stamp source versions, checksums/layout, and publish atomically. |
| Property caches / bounded byte store | `runtime/property_manager.rs` uses LRU caches of `(VID, property) -> Value`; bbtree-store budgets byte pages | **Conditional:** budgeting may help a new cache, but ordered lookup does not supply LRU eviction; encoding Values adds cost. | Compare complete cache behavior, hit rate, eviction and retained memory; shared-tree safety gate applies. |
| Catalogs, snapshots, plugins and trust data | `uni-common/core/fork.rs`, snapshot managers, plugin manifest/verify/loader maps; deterministic serialization and public types | **Low:** small metadata and I/O-oriented operations; bbtree has no matching serde integration in the inspected crate. | Keep std wire/public representations; adapters would add work and compatibility obligations. |
| CRDT state | `uni-crdt` uses FxHashMaps/FxHashSets; builtin plugin has some ordered counters/sets | **Weak direct fit:** merge/causality is the work, not ordered range lookup. Adding `Ord` changes generic bounds. | Preserve merge algebra, tombstones and serialization; no blanket replacement. |
| Python facade / bindings | Calls through Rust implementations | **Indirect only:** benefits require a faster Rust path and may be hidden by conversion overhead. | Measure through the public API after a Rust candidate wins; preserve sync/async symmetry. |

## Why Lance's result does not automatically transfer

The upstream experiment replaces a specific MemWAL arena skiplist with two maps:
newest row per value, and older versions separately. It adds order-preserving
encodings and batches both probes and puts. Its reported 10M-row Int64 results
include 6.8x index seek, 2.3x API hit lookup, 4.0x batched lookup and 3.9x puts.
The same report shows approximately unchanged DataFusion-plan-per-lookup and
write/close workloads. Data and WALs were on tmpfs; cells ran once, on one CPU.

Uni currently writes Arrow batches through `Dataset::write` and stages fragment
appends through `InsertBuilder::execute_uncommitted`; its own WAL and L0 handle
recent mutations. `LanceDirectory::write_params` uses normal default write
parameters plus storage credentials. No use of the optimized MemWAL path was
found. The reported ratios therefore do not predict faster Uni writes or reads.

Upstream evidence: [integration results and change description](https://github.com/XiangpengHao/bbtree-eval/blob/main/integrations/lance/RESULTS.md).

## Local probe method and limits

The supporting [probe source and reproduction instructions](bbtree_evaluation_2026_10_26/probe.md)
and [raw samples](bbtree_evaluation_2026_10_26/results-native.csv) accompany this
report. A generated summary contains medians, min/max samples and relative speeds.

- Intel Core Ultra 9 185H, x86_64, CPU 1 affinity, native code generation;
  AVX2 is present, AVX-512 is absent. This is not the author's Zen 5 machine.
- Rust 1.99.1; release, thin LTO, one codegen unit, system allocator; no allocator
  instrumentation in timed code. The published crate has an unused-method warning.
- Five rounds per case with implementation order rotated; each sample runs for
  at least 100 ms after warmup. Report medians, not confidence intervals or p99.
  An initial 3 x 30 ms screening run showed noise; the final protocol uses
  longer samples and discards none of its measurements.
- Sparse probes include map creation, aggregation, two output vectors and drop.
  They compare complete output vectors against std for the generated inputs.
  Stable sorting preserves duplicate input order. u64 widening is a separate
  representation experiment, not a recommendation.
- Pin probes include tree construction, 2,048 increment/minimum/decrement cycles
  over existing keys and drop; they do not include mutex contention, version-key
  deletion, merge horizons or full transaction work.
- Canonical-key probes use String/u64 fields, not actual Uni `Value`s; all paths
  hash ordered key/value pairs identically. They do not measure cache equality
  collisions or subquery execution. Checks compare digests.
- Bulk predecessor and point-hit probes build maps outside timing and repeat a
  fixed 4,096-query set, giving a warm working subset. They are not cold/random
  probes over the whole map. Bulk values retain a String payload.
- Property-range probes build indexes outside timing and run 16 queries returning
  a small property interval from synthetic integer rows. Tuple keys and packed
  u128 keys are separate bbtree variants. Checks compare result sums. No index
  maintenance, snapshot merging, tombstones, coercion or planner work is measured.
- No concurrent-map speed, RSS, allocator fragmentation, mmap cold start, full
  Uni query/ingestion throughput or tail-latency measurement was performed.

These are exploratory selection probes, not replacements for the repository's
Criterion benchmarks or conformance suites. The large shared-map safety question
does not arise from running these single-threaded `unsync` probes.

## Local results

Median times below are nanoseconds per operation as labelled. Smaller is better.
Map construction is excluded for prebuilt lookup/range cases and included for
normalization, canonical keys and the pin proxy. Ratios compare only that case.

| Workload | std BTreeMap | bbtree unsync | Other comparator |
|---|---:|---:|---|
| Large u64 point hit (1M keys) | 105.2 | 71.2 | hash: 11.2; bb_batch32: 48.9 |
| Bulk predecessor (100K intervals) | 72.5 | 48.8 | sorted_vec: 29.2 |
| Bulk predecessor (16 intervals) | 11.8 | 17.8 | sorted_vec: 3.3 |
| Sparse normalize (1,024 mostly unique pairs) | 37,111.0 | 48,104.5 | stable_sort_vec: 9,884.2 |
| Sparse normalize (4,096 mostly unique pairs) | 243,264.4 | 234,140.7 | stable_sort_vec: 41,416.9 |
| Pin operation proxy (8 versions) | 7.4 | 42.8 | not measured |
| Canonical key build/hash (4 fields) | 143.3 | 250.3 | sorted_vec: 89.4 |
| Property range (100K rows, per query) | 49.0 | 102.4 | sorted_vec: 35.6; scan: 22,483.3; bb_tuple: 157.2 |

Interpretation:

- Large u64 point lookups favor bbtree over std in this probe, especially in
  batches, but the standard hash map is substantially faster still. No reason
  to replace equality-only hash indexes follows from the tree comparison.
- Bulk predecessor lookup benefits only at larger run counts; a pre-sorted vector
  wins this read-only comparison. Real insertion interleavings are unmeasured.
- Small coordination and canonical maps regress with bbtree. Sparse normalization
  favors stable sort/reduce for larger inputs; std remains better in some tiny,
  duplicate-heavy cases. Widening u32 terms to u64 did not help.
- The property-range result supports testing an auxiliary index, not adopting
  bbtree specifically. Std wins here even against the packed-u128 variant.
  The scan comparator is a contiguous synthetic scan; production probes involve
  hash maps, generations and residual checks, so these ratios are not query gains.

Final protocol: 78 implementation/size cells, 390 retained timing
samples. Largest within-cell max/min ratio: 1.54.
This is a short single-machine selection exercise, not a statistical performance
guarantee. See [all medians and sample ranges](bbtree_evaluation_2026_10_26/summary.csv).

## Correctness and integration gates

1. **Rust memory model.** The pinned concurrency design acknowledges racing plain
   reads; the Miri filter excludes threaded tests because of races. Version
   validation and keeping pages allocated do not legalize non-atomic conflicting
   accesses. Resolve this before adopting the shared tree. Selected Verus
   encoding/comparison proofs do not prove the concurrent tree's soundness.
2. **Snapshot and transaction semantics.** Preserve generation freezing, per-tx
   private buffers, atomic publication, version pin horizons and cancellation.
   Shared-map scans are not snapshots; batched writes are not transactions.
3. **Encoding correctness.** Preserve signed order, numeric coercion, nulls, NaNs,
   composite separators, strings and Cypher equality. The packed-u128 probe only
   covers two nonnegative u64 fields and is not a general Value encoding.
4. **L0 negative updates.** A property index must represent removal as well as
   insertion. A newer row that ceases matching must suppress the older indexed
   hit; relabeling, deletion, hidden bulk versions and frozen generations count.
   Retain conservative residual filtering and fallback where encoding is unsafe.
5. **Fork identity.** Never bypass BranchedBackend, per-fork schema/allocators or
   primary liveness verification. A global UID cache is not branch isolation.
6. **Persistence and memory.** Shared bbtree retains retired node pages for reuse
   until destruction. Account for historical peak size and owned heap values.
   Mmap artifacts need generation stamps, corruption detection, layout checks,
   immutable-file publication and fallback rebuild; they cannot become a second,
   independently authoritative catalog writer.
7. **API compatibility.** Keep public serialized metadata types stable, preserve
   deterministic order, and avoid callback re-entry into a held leaf. Existing
   compound locks cannot be removed because individual map operations are safe.

Sources: [concurrency design](https://github.com/XiangpengHao/bbtree/blob/f0a5d361ecb4df176b3e07d4e33bb399dc3d22bf/design/09-concurrency.md),
[proof scope](https://github.com/XiangpengHao/bbtree/blob/f0a5d361ecb4df176b3e07d4e33bb399dc3d22bf/design/08-fuzzing-verification.md),
[Rust's undefined-behavior rules](https://doc.rust-lang.org/reference/behavior-considered-undefined.html#undefinedrace),
[file format](https://github.com/XiangpengHao/bbtree/blob/f0a5d361ecb4df176b3e07d4e33bb399dc3d22bf/bbtree/src/file.rs),
[bounded byte-store design](https://github.com/XiangpengHao/bbtree/blob/f0a5d361ecb4df176b3e07d4e33bb399dc3d22bf/design/16-bbtree-store.md).

## Evaluation order

1. Instrument representative workloads: L0 members probed/retained, repeated
   property predicates per generation, distinct pin versions, bulk interval-run
   count, canonical-key field counts, startup rebuild time and peak memory.
2. Prototype generation-local declared-property indexing with std and bbtree
   unsync side by side. Compare equality hashes, ordered trees and sorted frozen
   vectors; include maintenance, freeze/flush and delete/relabel costs. Reuse the
   existing scalar prefilter as the differential oracle, then run public queries.
3. Independently benchmark stable sparse normalization and canonical vectors.
   These opportunities do not require bbtree. Preserve float order and full
   content equality, respectively.
4. Benchmark bulk intervals under actual allocation interleavings. A vector can
   win on reads but may require ordered insertion or sorting; run compression
   can make the existing tree too small for a replacement to matter.
5. Profile restart before prototyping an immutable accelerator. Compare validated
   mmap opening with existing reconstruction and sorted/Arrow formats, including
   cold pages, artifact invalidation and source-version checking.
6. Only after the Rust safety concern is resolved, evaluate concurrent map/store
   use with realistic non-Copy values, skew, writes, scans, churn and memory limits.

As an illustration, speeding up a component 2x yields only 1.11x end-to-end if it
accounts for 20% of runtime, and 1.43x if it accounts for 60%. Those fractions
have not been measured for Uni. No production speedup is asserted in this report.
