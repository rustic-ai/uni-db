# L0 property index: BTreeMap versus bbtree

Date: 2026-10-26. Experimental comparison complete; no production change.

## Decision

Retain standard BTreeMap as the default reference backend for a selective-read
L0 property index. This comparison does not justify a general bbtree replacement.
Across two complete processes, bbtree improved broad candidate enumeration by
roughly 4–7%, but lost point hits and short replacement bursts. A separate
sustained-churn experiment found one useful exception: packed bbtree improved
replacement time about 10% against packed standard BTreeMap. Memory usage of
the full prototype stayed similar.

This directly compares the same auxiliary index, unlike the upstream Lance
MemWAL experiment. It does not measure WAL append/recovery or establish faster
Uni transactions. The earlier 4.7× public-query improvement came from avoiding
repeated L0 scans; these results do not attribute that improvement to bbtree.

## Controlled implementations

The same generic `PropertyIndex<T>` code maintains property identity, the row
map, unsupported-value fallback, bounds/IN handling, VID ranges, shadowed rows,
and replacement/removal semantics. Only the ordered-key backend changes:

| Backend | Ordered storage | Key representation |
|---|---|---|
| std tuple | standard BTreeMap | `(i64 value, u64 VID)` |
| bbtree tuple | `bbtree::unsync::BBTreeMap` 0.1.5 | same tuple |
| std packed | standard BTreeMap | `u128` |
| bbtree packed | `bbtree::unsync::BBTreeMap` 0.1.5 | same `u128` |

The encoding flips the signed integer's sign bit, puts it in the high 64 bits,
and puts the VID in the low 64 bits. It preserves ordering across negative
values and both i64 extremes. The crate's Auto strategy recognizes u128 for
inline keys. The tuple variants give the direct collection swap; the packed
variants control for encoding and let bbtree exercise its integer-key path.
This is a single-owner tree comparison; it installs no concurrent shared tree
and makes no concurrency-soundness claim.

The existing production prefilter remains the correctness oracle. Seven selected
nextest tests passed, including expanded multi-generation equivalence checks for
all four backends and a new mutation test: 4,096 interleaved writes/removes per
alternative, null and unsupported values, signed extremes and repeated range
checks. All final benchmark preflights also compared candidate sets exactly.
An additional sustained-churn test passed after one million packed-key
replacements on 32K rows, checking full, negative, old-value and current-point
candidate sets every 100K operations against the standard backend.
The tests cover prototype semantics, not a live transaction-index integration.

## Timing protocol

Four sizes: 2K, 8K, 32K and 128K label members. Each fixture stores real Value
properties and a string payload; x values come from deterministic mixed integers
modulo the fixture size. All backends in a process see the same L0 context.
There are two complete processes, each with eight rounds and backend order
rotated across rounds. The tables show medians of 16 round averages. Each round
runs 2,000 queries per point/narrow case, 20 broad queries, 2,048 fresh inserts,
4,096 replacements and 2,048 removals. Build, retirement and rebuild/drop each
run once per round. That gives 2,560 timing observations in total, not 2,560
independent machines or single-operation latency samples. HashMaps retain their
normal randomized seeds. No p99 or confidence interval is claimed.

Hardware and flags match the preceding follow-up: Intel Core Ultra 9 185H,
CPU 1 affinity, system allocator, Rust 1.99.1, no target-cpu=native. The benchmark,
bbtree and relevant Uni crates use opt-level 3 with assertions/overflow checks
off; other dependencies retain repository dev overrides. This is **not release**.
No builds ran during final timing. A global allocator wrapper performs a disabled
flag check during timing; allocation counters are enabled only in separate
memory passes. These numbers should not be directly pooled with earlier runs
whose harness and allocator instrumentation differed.

## Results at 32K rows

| Operation | std tuple | bbtree tuple | std packed | bbtree packed |
|---|---:|---:|---:|---:|
| Build index (ms) | 20.10 | 21.36 | 20.46 | 19.27 |
| Fixed-value range, width 1 (ns) | 42.08 | 117.59 | 36.67 | 84.60 |
| Known point hit, IN (ns) | 83.54 | 181.15 | 84.16 | 144.71 |
| Range width 16 (ns) | 312.87 | 343.91 | 312.81 | 310.91 |
| Range covering half the values (µs) | 521.72 | 484.92 | 516.77 | 494.11 |
| Insert fresh VID (ns) | 106.65 | 119.01 | 101.52 | 96.69 |
| Replace existing VID (ns) | 312.38 | 349.97 | 295.10 | 335.20 |
| Remove VID (ns) | 126.42 | 151.39 | 134.24 | 125.85 |
| Retire/drop mutated index (µs) | 520.49 | 307.14 | 477.17 | 358.22 |
| Rebuild and drop (ms) | 21.49 | 21.18 | 20.96 | 20.91 |

The fixed-value width-1 predicate matches zero rows here; the guaranteed point
hit matches two VIDs. Width 16 matches ten VIDs and the broad range 15,916.
Known point hits are measured independently so empty ranges cannot stand in for
successful lookup behavior. The IN path constructs the same small output and
predicate-value vector in both implementations.

At this size, tuple bbtree is 2.17× slower for point hits and 12% slower for
replacements, while its broad scan is 7% faster. With packed keys, bbtree is
1.72× slower for point hits, 14% slower for replacements and 4% faster for broad
scans compared with packed standard BTreeMap. Narrow packed-key ranges are tied.

## Results at 128K rows

| Operation | std tuple | bbtree tuple | std packed | bbtree packed |
|---|---:|---:|---:|---:|
| Build index (ms) | 95.91 | 100.95 | 96.92 | 94.79 |
| Fixed-value range, width 1 (ns) | 71.62 | 155.88 | 64.06 | 142.46 |
| Known point hit, IN (ns) | 74.67 | 192.20 | 76.60 | 162.91 |
| Range width 16 (ns) | 520.89 | 552.32 | 533.07 | 542.97 |
| Range covering half the values (µs) | 2202.90 | 2047.31 | 2214.88 | 2083.59 |
| Insert fresh VID (ns) | 163.39 | 180.33 | 161.32 | 163.32 |
| Replace existing VID (ns) | 458.76 | 569.11 | 450.71 | 559.93 |
| Remove VID (ns) | 144.24 | 194.91 | 167.79 | 170.64 |
| Retire/drop mutated index (µs) | 2387.91 | 1645.50 | 2419.14 | 1698.76 |
| Rebuild and drop (ms) | 99.72 | 104.95 | 100.72 | 96.59 |

The guaranteed point hit matches one VID, width 16 matches 15, and the broad
range 63,897. Compared with matching representations, tuple/packed bbtree takes
2.57×/2.13× as long for point hits and 1.24×/1.24× as long for replacements.
Broad scans take about 7%/6% less time. The direction of these effects holds in
both processes: tuple point-hit ratios were 2.62× and 2.52×; packed ratios were
2.11× and 2.15×. Packed narrow ranges differ by only a few percent between runs.

Packed bbtree's build savings are modest: approximately 6% at 32K and 2% at 128K
against packed standard BTreeMap. Its faster destruction does not turn into a
similarly large rebuild/drop improvement. The 2K/8K results and every range of
round averages are available in the CSVs; they likewise show no broad dominance.

The mutation phases run in order: fresh VIDs are added first, then existing VIDs
are replaced, then selected VIDs removed. Duplicate replacement/removal targets
follow the same deterministic sequence for every backend. Retirement measures
dropping that mutated auxiliary index. Rebuild/drop reconstructs from the original
context. These are lifecycle costs relevant to generation turnover, **not an
actual Lance flush, WAL replay, freeze publication, or storage-I/O benchmark**.

## Sustained churn: a packed-key exception

The focused profile workload runs one million replacements on 32K VIDs, versus
4,096 replacements in the mixed benchmark. A packed-key result differed, so it
was repeated unprofiled in eight rotated-order rounds, with a separate process
for each backend and round (32 processes, 32 million replacements total).
Fixture and initial index construction are outside each replacement timer.

| Backend | Median ns/replacement | Process-average range (ns) |
|---|---:|---:|
| std tuple | 232.03 | 227.41–245.54 |
| bbtree tuple | 292.56 | 283.07–349.29 |
| std packed | 239.41 | 234.09–270.99 |
| bbtree packed | 216.18 | 211.51–241.23 |

Here packed bbtree takes about 10% less time than packed standard BTreeMap,
and 7% less than standard tuple keys. Tuple bbtree still loses by about 26%.
The process-average ranges overlap for the packed comparison; these are eight
runs on one host, not a confidence guarantee.

This is a different workload from a short update burst. New values span signed
64-bit integers and targets cover the entire index repeatedly, changing the
initial dense value distribution. The result supports testing packed bbtree in
a sustained-update workload, while the point-read penalties argue against a
blanket choice. It does not imply a 10% WAL or transaction throughput gain.
The [separate raw runs](bbtree_l0_comparison_2026_10_26/steady-churn.log) and
[summary](bbtree_l0_comparison_2026_10_26/steady-churn-summary.csv) keep this
workload distinct from the main table.

## Memory: full prototype, not just tree pages

Allocation accounting measures requested live heap bytes, peak bytes and
allocation requests while building the index, excluding the already-built L0
fixture. It is not RSS, allocator-resident memory, or cold-page behavior. The
same row map and fallback set remain in every implementation. Index payloads
here are Int Values; the fixture's string payload is not cloned into the index.
For the memory churn pass, every original VID is replaced and then removed.
All four implementations return the counted live allocation total to zero when
the index is dropped.

| 32K rows | Retained after build (MiB) | Build allocation calls | After deleting every row (MiB) |
|---|---:|---:|---:|
| std_tuple | 7.392 | 4299 | 6.563 |
| bb_tuple | 7.286 | 756 | 6.564 |
| std_packed | 7.392 | 4299 | 6.563 |
| bb_packed | 7.382 | 866 | 6.564 |

Tuple bbtree reduces tree allocation calls substantially, but total retained
bytes fall by only about 1%. Packed bbtree's total is essentially the same as
the standard tree. The empty index retains roughly 6.56 MiB at 32K because the
shared row map retains its capacity; replacing the ordered structure does not
address that footprint. The 128K full-index retained footprint is about 29–30 MiB
for every backend. No full-engine memory reduction is established.

## Profiling and limitations

Twelve hardware-counter runs cover the four backends and three operations:
point hits, broad scans, and replacements. Eight sampled CPU recordings cover
point hits and replacements. All commands completed successfully. Core-PMU
coverage was approximately 99%; raw commands, text reports and counter files
are retained. Process counters include fixture/index setup and destruction;
they are not exact counters for an individual lookup. Recordings are short,
so their sample percentages are exploratory rather than precise attribution.

In matching tuple-key process runs, bbtree's broad-scan retired instruction total
was about 4% lower; its point-hit total was about 30% higher and replacement total
about 20% higher. Branch misses decreased in the bbtree process runs without
producing uniformly faster operations. Packed controls also show higher point
and replacement instruction totals. Cache-miss totals include setup and do not
establish a cache-coherence benefit. CPU reports show tree search, candidate
collection and shared row-map work; no assumption that the faster tree dominates
the entire query is warranted.

The focused replacement profiles use the sustained workload above; they should
not be treated as attribution for the shorter mixed-workload replacement cells.

The experiments do not measure writer contention, batching, thread scalability,
NUMA, release builds, a live index's maintenance/invalidation, snapshot/branch
isolation, actual flush I/O or WAL throughput. A generation-aware production
index still needs those integration contracts and selectivity/reuse policies.
For a selective-read implementation, standard BTreeMap is the better default
reference choice from this evidence. Packed bbtree remains a measured candidate
for sustained-update workloads, subject to the cost of their actual read mix.
A separate batched or different data-layout design would need its own comparison.

## Evidence and code

- [Reproduction instructions, raw results and profiles](bbtree_l0_comparison_2026_10_26/README.md)
- [Timing summary](bbtree_l0_comparison_2026_10_26/summary.csv) and [all round averages](bbtree_l0_comparison_2026_10_26/samples.csv)
- [Allocation measurements](bbtree_l0_comparison_2026_10_26/memory.csv)
- [Test log](bbtree_l0_comparison_2026_10_26/tests-final.log)
- Shared kernels: `crates/uni/benches/support/collection_candidates.rs`
- Timing/allocation/profile driver: `crates/uni/benches/support/l0_comparison.rs`
- Existing integration binary: `crates/uni/tests/common/perf/collection_candidates.rs`

All 61 original benchmark smoke cases and formatting checks passed after the
harness changes. The initial run without a guaranteed point-hit control is
retained separately and excluded from the final summaries. No production code,
WAL implementation or dependency version was changed. No git or GitHub
operations were performed.
