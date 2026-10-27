# Uni performance review

Review date: October 26, 2026. The three findings and measurements below are the
original baseline. Local corrections and after-fix measurements are recorded in
the [implementation report](review_fixes_2026_10_26.md).

See the [combined correctness and performance triage](review_triage_2026_10_26.md)
for workload qualifications, relative priority, and verification gates.

Three execution paths perform avoidable work that grows quadratically: recursive
Cypher traversal, retirement of constraint keys during deletion, and removal of
edges sharing a high-degree endpoint. The deletion costs also occupy the writer's
commit serialization lock. All three findings have measurements against current
source, controls, and probes that assert the expected results.

This complements the [correctness review](codebase_review_2026_10_26.md). It is a
targeted review of query execution and buffered mutations, not an exhaustive
audit of every crate. The measurements below use the development/test build;
absolute timings and speed ratios are not production release estimates.

## Confirmed findings

| Finding | Trigger | Measured impact |
| --- | --- | --- |
| Recursive CTE rescans the label at every step | Reachability along a chain | 900 nodes cause 811,800 row visits; 12.61 seconds versus 10.57 ms for an equivalent bounded traversal |
| Vertex deletion scans the entire constraint map | Deleting many uniquely constrained vertices resident in L0 | At 16,000 vertices, commit takes 1,459 ms versus 115 ms without a constraint |
| Edge deletion repeatedly filters adjacency lists | Deleting many L0 edges sharing an endpoint | At 16,000 edges, commit takes 1,203 ms for a star versus 110 ms for disjoint edges |

All table values are medians of three runs. These are performance defects within
the stated workloads; no correctness failure occurred in the final five probes.

## Recursive CTE execution rescans the full label

The reproduction creates a directed chain with one integer property per node
and asks for the number of reachable nodes:

```cypher
WITH RECURSIVE reach AS (
  MATCH (root:Item {i: 0}) RETURN root
  UNION
  MATCH (a:Item)-[:NEXT]->(b:Item) WHERE a IN reach RETURN b
)
MATCH (n:Item) WHERE n IN reach RETURN count(n) AS c
```

For a chain of `N` nodes, the control is:

```cypher
MATCH (root:Item {i: 0})-[:NEXT*0..899]->(n:Item)
RETURN count(DISTINCT n) AS c
```

The upper bound is generated as `N - 1`; `899` is the 900-node case. Both queries
are asserted to return exactly `N`. An unbounded variable-length traversal is
not a valid control here because its default depth limit truncates longer chains.
The final measurements use the explicit bound throughout.

| Nodes | CTE row visits | Control row visits | CTE wall time | Control wall time |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 10,200 | 100 | 241.32 ms | 3.79 ms |
| 300 | 90,600 | 300 | 1,552.11 ms | 5.63 ms |
| 900 | 811,800 | 900 | 12,609.90 ms | 10.57 ms |

`rows_scanned` and `l0_reads` both report `N × (N + 2)` for the CTE. Although each
round's frontier contains only one node, the query revisits the whole label.
Tripling chain length from 300 to 900 multiplies row visits by about nine and
wall time by about eight. Output cardinality grows only threefold.

In [recursive_cte.rs](../../crates/uni-query/src/query/df_graph/recursive_cte.rs),
lines 321–344 bind the frontier as a list parameter and call `execute_subplan`
again. The membership predicate does not turn this workload into a scan of only
the frontier IDs. In
[common.rs](../../crates/uni-query/src/query/df_graph/common.rs), lines 2006–2014
and 2084–2097 also create, plan, and collect a fresh physical subplan each round.
The row counters establish the repeated full scans; these timings do not
separately quantify planning versus execution overhead.

**Suggested correction:** represent the working set as a physical input or a
VID lookup driving expansion, so recursive membership need not rescan the
label. Reuse prepared execution structure where parameter binding permits it.
Preserve deduplication, cycles, cancellation, and snapshot semantics. This
finding concerns Cypher recursive CTEs, not a claim about every Locy rule.

**Acceptance:** preserve exact reachable counts for chains and cyclic graphs;
make row visits grow with the reachable graph rather than chain depth times
label size. Benchmark unrelated nodes in the same label as well as chain length.

## Vertex deletion repeatedly scans the constraint index

In [l0.rs](../../crates/uni-store/src/runtime/l0.rs), lines 1198–1203 run
`constraint_index.retain` and `merge_guard_index.retain` for every deleted vertex.
The former scans keys belonging to every vertex even though
`constraint_keys_by_vid` already records the deleting vertex's keys. The deletion
then removes that reverse-map entry without using it to retire keys directly.
`insert_constraint_key`, lines 1590–1609, already demonstrates ownership-checked
direct key removal.

Deleting `D` vertices from a buffer with `K` constraint keys therefore repeatedly
scans that map. Deleting all vertices with one unique key each produces quadratic
work. The public reproduction commits the fixture, then executes
`MATCH (n:Item) DELETE n` in another transaction, timing execution and commit
separately. The control differs only in whether `Item.i` is unique.

| Vertices deleted | Commit without constraint | Commit with unique constraint |
| ---: | ---: | ---: |
| 1,000 | 6.14 ms | 11.06 ms |
| 4,000 | 24.75 ms | 105.67 ms |
| 16,000 | 114.91 ms | 1,459.30 ms |

At 16,000 vertices, query execution itself takes 651.99 ms without the constraint
and 643.65 ms with it. The large difference is in commit, which is about 12.7
times slower. That ratio is for commit only, not the entire delete transaction.

A direct `L0Buffer` control isolates key retirement: deleting 4,000 uniquely
keyed vertices takes 85.11 ms and deleting 16,000 takes 1,357.00 ms, approximately
16 times the work for four times the input. At 16,000, deletion with no keys takes
20.16 ms. The direct probe has no query planner or database WAL I/O in its timer.

The same global-retain pattern exists for MERGE guards at line 1203 and edge
constraint keys at line 1451. Those are source-confirmed related paths; the
timings here cover vertex uniqueness only.

**Suggested correction:** consume `constraint_keys_by_vid` and remove only keys
still owned by the deleted VID. Maintain equivalent reverse ownership for MERGE
guards and edge keys, or retire their keys once per deletion batch. Avoid deleting
a key that has since been reassigned to another entity.

**Acceptance:** deletion work should track deleted entities and their keys,
including deletion of a small subset from a large map. Preserve key reuse,
property changes, multiple constraints, rollback, and replay behavior.

## Edge deletion repeatedly scans a hub adjacency list

In [simple_graph.rs](../../crates/uni-common/src/graph/simple_graph.rs), lines
156–167 find an edge in constant time but remove it by retaining the entire
outgoing and incoming adjacency vectors. Deleting all `D` edges of a star scans
the central vector at lengths `D`, `D - 1`, and so on: quadratic work.

Both `apply_edge_deletion` at
[l0.rs](../../crates/uni-store/src/runtime/l0.rs) line 1452 and the incident-edge
loop in `apply_vertex_deletion` at lines 1156–1171 call this method. Gathering
incident edges in linear time does not eliminate the repeated scans during
removal. The public probe deletes edges explicitly; the cascade path is supported
by source inspection rather than a separately timed public `DETACH DELETE` case.

The reproduction bulk-inserts and commits either a star or disjoint directed
edges, then runs `MATCH ()-[r:R]->() DELETE r` and commits. Both shapes delete the
same number of edges and assert a final edge count of zero.

| Edges deleted | Disjoint commit | Star commit |
| ---: | ---: | ---: |
| 1,000 | 6.51 ms | 10.62 ms |
| 4,000 | 23.84 ms | 94.08 ms |
| 16,000 | 110.25 ms | 1,202.68 ms |

At 16,000 edges, query execution takes 1,040.39 ms for disjoint edges and 849.19 ms
for the star. Commit is about 10.9 times slower for the star despite its smaller
vertex population. This ratio applies to commit only.

A direct `SimpleGraph` probe excludes query execution, WAL, and constraints. Star
deletion rises from 103.40 ms at 4,000 edges to 1,639.70 ms at 16,000, approximately
16 times the work for four times the input. The 16,000 disjoint-edge control takes
4.46 ms. Edge-removal order differs between this probe and transaction merge, so
their absolute times should not be equated.

**Suggested correction:** batch edge retirement and filter each affected
adjacency vector once, or maintain edge positions supporting constant-time
removal while updating moved entries. Preserve self-loops, parallel edges,
edge types, traversal consistency, and tombstones.

**Acceptance:** deleting all edges of a star should scale approximately linearly;
disjoint graphs should retain comparable performance. Exercise both explicit
edge deletion and vertex cascade deletion.

## Why the deletion costs affect other writers

[writer.rs](../../crates/uni-store/src/runtime/writer.rs), lines 1880–1898, acquires
`flush_lock` across WAL work and merging the transaction into the committed L0.
The `merge_take` call at line 2273 reaches the deletion loops in
[l0.rs](../../crates/uni-store/src/runtime/l0.rs), including vertex tombstones at
line 1750 and edge tombstones at lines 1808–1820.

The repeated scans therefore lengthen a shared commit critical section. Other
commits using the same writer must wait for it. This follows from the lock scope;
the probes measured single-writer latency, not concurrent throughput or lock-wait
distributions. Retain this lock's correctness contract while reducing the work
performed inside it.

## Measurement scope and reproduction

Five probes passed, with three repetitions per case and increasing input sizes.
Fixtures are outside the timed regions. Public deletion probes time query
execution and commit separately; direct probes isolate the suspected structures.
Recursive query order alternates between repetitions. All final counts are
asserted. Timings record wall time and process-wide CPU time using the existing
`perf::cpu_clock` helper; CPU time includes worker threads. CPU and wall results
are close in this run.

The build uses the ordinary nextest development/test profile: workspace code is
unoptimized and dependencies use the repository's development optimization
override. Optimized benchmarks are needed before assigning production latency
targets. The source-level repeated scans and row-visit counts establish the
algorithmic defects independently of those absolute times.

The database is in-memory with automatic flush disabled, keeping fixtures in L0.
Fork background workers are disabled; other configuration remains at its default.
The current default flush threshold is 10,000 mutations with a five-second timer,
so the 16,000-item cases deliberately exercise a larger resident buffer. They
represent workloads configured to retain larger L0 buffers, not a claim that
every default-configured 16,000-item deletion has these timings. Already-flushed
L1 data and storage I/O are not benchmarked here.

Run the preserved probes from the repository root:

```bash
python3 docs/proposals/performance_review_2026_10_26/run_probes.py
```

The runner temporarily registers the probes in the existing integration binary,
runs nextest serially, and removes its registration in `finally`. It clears
`RUSTC_WRAPPER` to avoid the local sccache sandbox failure. No new integration-test
binary is introduced.

Evidence:

- [Probe source](performance_review_2026_10_26/probes.rs)
- [Portable runner](performance_review_2026_10_26/run_probes.py)
- [Passing run and raw measurements](performance_review_2026_10_26/results.txt)
- [Computed medians](performance_review_2026_10_26/medians.json)
- [Reviewed source hashes](performance_review_2026_10_26/reviewed_source_sha256.json)

The temporary registration used for this review has been removed. Production
source was not changed. Start with constraint-key retirement because the vertex
reverse index already exists, then address adjacency retirement and CTE frontier
execution. Validate optimized scaling and concurrent commit latency after fixes.
