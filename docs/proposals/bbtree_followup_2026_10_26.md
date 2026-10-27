# bbtree follow-up: measured collection and L0 experiments

Date: 2026-10-26. Status: evaluation complete; experimental code only.

Subsequent [controlled L0 backend comparison](bbtree_l0_comparison_2026_10_26.md) measures standard and bbtree variants with matching key encodings, including sustained churn.

## Decision

The strongest Uni opportunity is a generation-aware L0 property index integrated
with native VID scans. In a quiet 32K-node public-query fixture, an experimental
index plus a literal VID hint reduced a selective range query from 13.24 ms to
2.79 ms (4.74×). At 2K nodes the same approach was slower. The prototype uses
`std::collections::BTreeMap`; this result supports avoiding repeated scans and
does not establish a benefit from bbtree itself.

Stable sort/reduce also merits a production change for larger sparse vectors.
Canonical correlated-query keys offer smaller savings with a sorted vector.
Retain the existing tree for general bulk interval insertion: a vector regressed
3.58× under large random insertion. Warm immutable file views remain interesting,
but the experiment does not establish startup or cold-I/O gains.

The [original investigation](bbtree_evaluation_2026_10_26.md) covers the algorithm,
upstream Lance claim, shared-map concurrency concern, and 24 application families
across storage, queries, Locy, bulk, forks, plugins and CRDTs. Its application
matrix remains the scope inventory. These measurements refine its priorities;
they do not measure every workload or transitive dependency. Uni's own WAL/L0 and
Dataset writes differ from the upstream Lance MemWAL benchmark, so those upstream
speedups cannot be applied directly to Uni.

## Protocol and evidence

The [reproduction instructions](bbtree_evaluation_2026_10_26/followup/README.md)
and [environment](bbtree_evaluation_2026_10_26/followup/environment.txt) accompany
all logs and raw Criterion samples. Hardware: Intel Core Ultra 9 185H, x86_64,
CPU 1 affinity, system allocator; no AVX-512 or `target-cpu=native` flag.
Rust 1.99.1. Published bbtree 0.1.5 is pinned as a dev dependency with its file
feature. No production collection or query implementation was replaced.

Measurements reuse the dev dependency cache with `uni-db`, `uni-store`,
`uni-query`, `uni-common`, and `bbtree` at opt-level 3, debug assertions and
overflow checks disabled. Other dependencies retain the repository's dev
package overrides, generally opt-level 2. This is **not a release build**.
Absolute timings and ratios require confirmation on production configurations.

Criterion covered 61 cells with 20 samples each (1,220 samples), 200 ms warmup,
and a 1 second measurement target per cell. The [summary CSV](bbtree_evaluation_2026_10_26/followup/criterion_summary.csv)
contains median confidence bounds; raw samples live alongside it. Tables below
use medians from that run and compare alternatives within the run. Criterion's
printed historical “change” comparisons are not used: the retained
`assertions_enabled_pilot/` used different flags. CPU profiles exposed debug-only
precondition overhead in that pilot, leading to the corrected configuration.

The later public-query control used the same configuration and kernels, with a
literal-hint variant added to the harness. It has seven warmed calls per cell,
sequential variant order, and no randomized repetitions, tail-latency estimate,
or confidence claim. Its wall times include `.profile()` instrumentation.

## L0: index candidates, then enter the native scan path

The microbenchmark compares the production `l0_prefilter_candidates` against an
experimental immutable INT64 property view. Rows contain real `Value` properties
and a string payload. Keys are `(property_value, VID)` in a standard BTreeMap;
replacement tracking and unsupported-value fallback preserve the oracle's
candidate semantics. The range has width 16; values in this fixture are hashed,
so width is not an exact result-count guarantee.

| L0 members | Existing prefilter | Prebuilt index query | Build + query + drop |
|---:|---:|---:|---:|
| 2,000 | 104.6 µs | 0.535 µs | 303.3 µs |
| 8,000 | 630.4 µs | 0.341 µs | 2.160 ms |
| 32,000 | 6.663 ms | 0.298 µs | 18.993 ms |

Construction needs roughly 3–4 avoided scans to pay back in this component
experiment. This omits integration, invalidation, contention and memory costs.
The sampled scan profile attributes about 70% self time to the prefilter itself;
string comparison and hashing are substantial additional costs. It probes label
membership and property maps even when few candidates emerge. The prebuilt-index
result is not a whole-query speedup.

The public-query fixture creates sequential `N.x` values, declares an index,
and keeps all data in unflushed L0. Automatic flushing is disabled and there are
no concurrent writers or L1 rows. It constructs the experimental index once,
then checks every query's answer. All range variants return count 16.

| Members | Baseline range | Parameter VID hint | Literal VID hint | One-time index build |
|---:|---:|---:|---:|---:|
| 2,000 | 1.666 ms | 1.953 ms | 2.590 ms | 0.346 ms |
| 8,000 | 3.041 ms | 3.033 ms | 2.586 ms | 2.136 ms |
| 32,000 | 13.243 ms | 14.040 ms | 2.792 ms | 17.174 ms |

The literal variant includes candidate lookup, ID sorting, query-text rebuilding,
and normal planning/execution in each timed call. The one-time index build is
outside those calls. At 32K members, the 4.74× gain repays that build after about
two repeated reads in this fixture, before maintenance and integration costs.
At 2K it does not repay. The 8K difference is modest and needs stronger sampling.

The parameter hint (`id(n) IN $vids`) failed to improve the large case. Source
inspection explains the difference: `extract_vid_from_cypher_filter` in
`crates/uni-query/src/query/df_planner.rs` accepts an `Expr::List` in this branch,
not a parameter expression. The literal list reaches the existing native VID
scan; supplying candidates merely as a filter parameter leaves the expensive
scan path. This is a measured planner limitation, not a correctness failure.
The experiment's literal construction is a control, not a proposed public API.

A correlated query with 128 outer rows and 8 distinct parameter keys returned
128 and scanned eight matching rows, consistent with subquery caching. Its
medians were 6.18, 10.52 and 113.09 ms at the three sizes. There is no alternative
canonical-key implementation installed in the executor, so these are baseline
observations, not an end-to-end canonical-key comparison.

Production integration must bind the index to the exact visible L0 generation
and transaction view. It must handle newer nonmatching writes, deletion and label
changes, label-only replacement rows, unsupported values, hidden/bulk state,
flush/freeze, snapshot and branch isolation. It must also carry candidates into
the physical scan and preserve existing fallback behavior. The flattened fixture
index solves neither live maintenance nor concurrent visibility. It retains a
row map and tree, so its memory overhead must be measured before adoption.

## Sparse vectors: remove per-coordinate tree work

The comparison returns actual `Value::SparseVector` values. Stable sorting keeps
duplicate summation in input order; the accumulator begins at positive zero,
matching the existing implementation. Nonfinite input weights are filtered.

| Input pairs | Mostly unique: tree → sort | Duplicate-heavy: tree → sort |
|---:|---:|---:|
| 16 | 0.394 → 0.156 µs | 0.140 → 0.146 µs |
| 128 | 3.531 → 1.114 µs | 1.456 → 1.052 µs |
| 1,024 | 46.069 → 9.395 µs | 20.562 → 8.904 µs |
| 4,096 | 265.625 → 39.979 µs | 158.323 → 39.860 µs |

At 1K/4K pairs, mostly unique inputs improve 4.90×/6.64× and duplicate-heavy
inputs 2.31×/3.97×. Tiny duplicate-heavy inputs slightly regress. In the 1K
profile loops, retired instructions fell from about 499K to 175K per iteration,
and branch misses from 2,855 to 35. Cache misses stayed near eight per iteration.
This supports reduced tree/branch work; it does not demonstrate reduced DRAM or
cache-coherence traffic. The sorted output reserves input-sized capacity, which
can retain extra memory for duplicate-heavy data; no memory benefit is claimed.

## Canonical keys: smaller benefit with real Values

This benchmark clones and hashes actual `Properties` with integer, string, list
and nested-map Values, instead of a scalar surrogate. Equality remains full
structural equality; hashes do not replace identity.

| Fields | BTreeMap | Sorted Vec | bbtree unsync |
|---:|---:|---:|---:|
| 4 | 0.572 µs | 0.426 µs | 0.674 µs |
| 16 | 2.395 µs | 1.873 µs | 2.806 µs |
| 64 | 12.397 µs | 9.699 µs | 12.874 µs |

Sorted vectors improve the kernel 1.28–1.34×. bbtree loses all three cases.
The 16-field perf loop retired about 12% fewer instructions with the vector;
cloning/hashing remain. The correlated query already caches repeated keys, so
these kernel savings cannot be multiplied by its entire runtime.

## Bulk intervals: preserve mixed insertion behavior

Each insertion includes a string label and three predecessor lookups. The
vector uses binary insertion/replacement, not an assumed monotonic append.
Counts here are interval records, not graph vertices.

| Order and interval count | BTreeMap | Sorted Vec |
|---|---:|---:|
| Ascending, 1,024 | 0.182 ms | 0.088 ms |
| Ascending, 16,384 | 3.483 ms | 1.908 ms |
| Random, 1,024 | 0.178 ms | 0.120 ms |
| Random, 16,384 | 4.235 ms | 15.143 ms |

The large random vector is 3.58× slower despite fewer branch misses. Its perf
loop retires about 122M instructions versus 32M for the tree: movement outweighs
lookup savings. Static predecessor probes in the original investigation were
insufficient to justify replacing a structure that is still being populated.
An immutable post-build view could be evaluated separately if lookup dominates.

## Immutable files: warm-open potential, no restart claim

The 32K-entry `u64 → u64` proxy takes 362.4 µs to rebuild a HashMap, 221.1 µs
to rebuild bbtree, 1.97 µs to validate/open a warm owned FileMap image, and
0.193 µs on its trusted open path. Validation visits the pages; trusted opening
requires the immutable image provenance documented by the crate.

These are owned bytes already in memory, not cold mmap, page-fault, filesystem,
actual VID-label payload, or Uni restart measurements. The data shape is a proxy
for label/identity metadata. A sorted or Arrow-backed immutable representation
could also avoid reconstruction. Persisted views need format compatibility,
checksums/provenance, invalidation and atomic publication. Do not select bbtree
or advertise startup gains from the warm-open ratio alone.

## Correctness, profiles, and remaining limits

`cargo nextest` passed five selected tests: four new differential tests and one
existing collection-storage integration test. The new tests cover bitwise sparse
results and summation order (special floats and 64 seeded random-bit inputs),
nested Value keys with forced hash collisions, INT64 candidate equivalence across
multiple generations and 16 seeds, unsupported/null values, bounds, IN, VID
ranges, shadows, replacements and interval interleavings. They do not constitute
concurrency, persistence, or full transaction-index verification.

All 61 benchmark smoke cases passed, including a final smoke after the literal
control was added. All 84 unprofiled public-query measurements asserted the
expected result. Formatting passed. Evidence includes nine hardware-counter
loops and six CPU recordings: four kernels and two whole-fixture query runs.
The final query recording had 979 samples and no lost samples; it still shows
L0 prefilter work prominently (37% self). That recording includes baseline and
hint variants, fixture creation, index construction and shutdown; this percentage
is not an individual query's removable fraction. PMU summaries use the CPU-core
counters with approximately 99% coverage, excluding CPU-atom startup artifacts.
Process counters include setup and teardown; loop wall times are separate.

The shared concurrent bbtree implementation was not installed or certified.
The source-level racing-read concern recorded in the original report remains an
adoption blocker. These single-threaded kernels cannot establish coherence
traffic improvements. There is no NUMA, writer-contention, cold-I/O, RSS, p99,
branch-isolated index, or production-release result in this evaluation.

Engineering priorities from the evidence:

1. Integrate a generation-aware INT64 L0 index with native candidate scans,
   guarded by selectivity/reuse and measured maintenance/memory costs.
2. Apply stable sort/reduce to larger sparse inputs with preserved float
   semantics and an explicit output-capacity policy.
3. Evaluate sorted canonical key vectors in the executor with full-query timing.
4. Retain general mutable bulk interval trees; revisit immutable lookup only
   against actual allocation/run distributions.
5. Compare immutable bbtree, sorted and existing columnar views with cold-open
   and real restart workloads before choosing a persistent format.

The reproducible harness and evidence are complete. No production optimization
is enabled, and no git or GitHub action was performed.
