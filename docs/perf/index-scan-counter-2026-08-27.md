# What `idx_scans` actually counts — 2026-08-27

**Date:** 2026-08-27
**Machine:** n/a — a wiring audit, not a timing measurement
**Gates:** nothing. This document exists to say what a number *means* before
anything is optimized against it.

## The question

Every LDBC SNB query reports `idx_scans=0`, including after adding BTree indexes
cut IC4 from **62.2 s to 3.1 s**. Something is plainly consulting an index. So
either the counter is under-wired, or it counts something narrower than its name
suggests — and until that is settled, no Wave-5 optimization can be judged by
it.

## The answer

**The counter is real, correct, and wired to exactly one scan path.**

`QueryCounters::add_lance_scan` is bumped from a single call site —
`attach_scan_stats` in `crates/uni-store/src/backend/lance.rs:1262` — which is
reached only when a `ScanRequest` carries `counters: Some(_)`. Exactly one
producer sets that:

```
StorageManager::scan_vertex_table_counted   (storage/manager.rs:1645)
  └─ called from GraphScanExec              (df_graph/scan.rs:1864)
```

Everything else passes `None`. `scan_vertex_table` — the uncounted wrapper —
hard-codes it (`manager.rs:1634`), and `scan_delta_table`, which serves **every
edge scan**, has no `counters` parameter at all.

The blast radius is visible in one line. Counting mentions of `counters` across
the ~40 files in `crates/uni-query/src/query/df_graph/`:

```
mod.rs           11   (the context plumbing itself)
scan.rs           8   (GraphScanExec — the one wired path)
catalog_scan.rs   1
```

Every other operator: **zero**. Not `traverse.rs`, not `ext_id_lookup.rs`, not
`vector_knn.rs`, not `shortest_path.rs`, `main_edge.rs`, or `bind_fixed_path.rs`.

That table is where the audit could have stopped and been wrong. A zero there
does not by itself mean an operator's index work went unreported — it means the
operator never hands counters to a `ScanRequest`, which for most of them is
because it never builds one. The next section follows each to what it actually
calls.

## What the uncounted work actually is

The gap is not one thing. Following each operator to what it really calls
splits it three ways, and only the first is a wiring omission:

**(a) Lance scans that go through `ScanRequest` and pass `None`.** The uncounted
`scan_vertex_table` wrapper, `scan_main_vertex_table`, and `scan_delta_table`
(one caller, `Executor::scan_edge_type_l1`). These are the ones a `counters`
parameter fixes, and they are cheap.

**(b) Lance operations that never build a `ScanRequest`.** `vector_knn` calls
`StorageManager::vector_search`, which is Lance's `nearest()` — a different API
that `attach_scan_stats` is not attached to and cannot be attached to as-is.
Full-text search is the same shape. These need their own stats callback, not a
threaded parameter.

**Closed 2026-08-28.** They now have one, and it is deliberately *not*
`attach_scan_stats`'s predicate — see the note on `partitions_searched` at the
end of this document. Counted separately as `vector_index_scans` and
`fts_index_scans`, with `searches_reported` as their denominator.

**(c) Work that is not a Lance scan at all.** This is the correction that
matters, and it is the opposite of what the operator-count table suggests on
first reading. `traverse.rs` reads neighbours through
`GraphExecutionContext::get_neighbors` → `AdjacencyManager`, which serves from
the in-memory CSR plus the L0 overlays. There is no Lance scan there for
`add_lance_scan` to observe. Wiring counters into the traversal would not raise
`idx_scans`, because the traversal is not doing the kind of work this counter
counts. `ext_id_lookup` likewise resolves through
`StorageManager::find_vertex_by_ext_id`, one lookup rather than a scan.

So `idx_scans=0` on a query whose runtime fell 20× is **not a contradiction and
not a bug in the counter**. It is the counter answering a narrower question than
the one being asked of it: *did a `ScanRequest`-based Lance scan consult an
index?* — and most of an LDBC query's time is spent somewhere that question does
not reach.

Note that `scans_reported` did its job here. It is the denominator that exists
so a zero in `idx_scans` can be distinguished from silence, and it is what makes
this diagnosable at all: `idx_scans=0, scans_reported=4` says "four scans were
observed and none used an index", which is a claim narrow enough to falsify.
`idx_scans=0` alone would have been unfalsifiable.

## What to fix, and in what order

1. **Rename what is reported, or scope it.** `idx_scans` reads as "how often
   this query used an index". It means "how often a `ScanRequest`-based Lance
   scan reported index activity". That is a defensible thing to count; it is not
   what the name promises, and the name is doing more damage than the missing
   wiring.
2. ~~**Category (a)**~~ — **done.** `scan_delta_table_counted` and
   `scan_main_vertex_table_counted` now carry the query's counters, and the
   schemaless vertex scan and the L1 edge scan route through them. Pinned by
   `a_schemaless_scan_reaches_the_denominator`, which was observed failing with
   `scans_reported=0` before the wiring landed.
3. **Category (b) — done.** `vector_search`, `multivector_search` and
   `full_text_search` have their own stats callback. This was the most
   misleading gap, because an *index lookup* that cannot report consulting an
   index is precisely the operator a reader expects `idx_scans` to be about.
4. **Category (c) needs a different counter, not this one.** Traversal cost is
   adjacency-cache work. Reporting it under a Lance-scan counter would make the
   number less honest, not more.

With (2) and (3) landed, the honest reading of `idx_scans` is
"`ScanRequest` Lance scans that consulted an index". It must not be used to
compare query plans that differ in how much work they do outside such a scan —
which is most LDBC plans, because their time is in category (c).

**Not established here:** *which* scans the BTree indexes accelerated in IC4.
That needs the SF1 fixture and a run with the counters from (2) and (3) in
place. The claim this document makes is about what the counter can observe, not
about where IC4's 59 seconds went.

## Method

```bash
# The one bump site, and the one producer that enables it.
grep -rn "add_lance_scan" --include=*.rs crates/            # 1 non-test site
grep -rn "with_counters(" --include=*.rs crates/            # 4 sites, 1 storage

# Per-operator coverage.
for f in crates/uni-query/src/query/df_graph/*.rs; do
  n=$(grep -c 'counters' "$f"); [ "$n" != "0" ] && echo "$(basename $f): $n"
done
```

Every operator in the table above was then followed to what it actually calls,
rather than inferred from the count. That is what turned "`traverse.rs` is the
big uncounted one" into category (c) — it issues no Lance scan at all, so the
count of zero is correct rather than missing.

## Related

- `docs/testing/single-shape-coverage-2026-08-27.md` — the same failure mode one
  level up: a measurement that is sound but answers a narrower question than the
  one being asked, and looks like coverage until you ask what it ranges over.
- `crates/uni/tests/common/bugs/issue_175_index_consulted.rs` — pins that the
  counter fires when a vertex scan *does* consult an index, which is why this
  audit concluded "under-wired" rather than "broken".


## Closing category (b) — 2026-08-28

### Why the scan predicate could not be reused

`attach_scan_stats` ORs `indices_loaded`, `parts_loaded` and
`index_comparisons`. That is sound on a plain scan only because of the call
site: a scanner that never calls `nearest()` can produce nothing but
scalar-index nodes. On the search paths that guarantee is gone, and
`vector_search` sets `prefilter(true)` with a SQL filter — so a *scalar* index
serving the prefilter lights all three terms while the KNN runs brute force.

Measured, 500 rows, no vector index, filter on a Hash-indexed column:

```
indices_loaded=1  parts_loaded=1  index_comparisons=4096
all_counts = batches_processed, fragments_scanned, ranges_scanned,
             row_replacements, rows_scanned        <- no partitions_searched
```

The OR would have reported an ANN search there. That is the same class of
result as `recall@10 = 1.000 while the index under test never ran`.

### The discriminator

`all_counts["partitions_searched"]`. Only `AnnIndexMetrics` and
`FtsIndexMetrics` register it, and Lance's brute-force `KNNVectorDistanceExec`
builds no index metric at all, so a scalar prefilter cannot produce it.

Two caveats live in the code beside the constant:

- Lance documents `all_counts` as "subject to change ... debugging purposes".
  This is an unpinned dependency on a metric *name*, so the positive tests say
  "Lance may have renamed `partitions_searched`" rather than "the index was not
  used" — a rename must produce a diagnosis, not a false negative.
- It counts IVF partitions probed. Lance wraps HNSW as an IVF sub-index and
  builds `Flat` as a single partition, so a consulted `Flat` index reads 1. It
  answers *was an index searched*, never *how approximate was the search*.

### What actually blocked the wiring

Not the predicate. Two places dropped the counters before they could reach a
search: `GraphExecutionContext::query_context()` propagated the deadline but not
the counters, and `QueryProcedureHost` had no counters field at all — so every
`CALL uni.vector.query(...)` ran with none. Both are fixed; a `ScanRequest` had
hidden the problem on the scan path because it carries counters itself.

### The measurement that surprised us

Corpus size was expected to be the constraint — Lance falls back to brute force
on small datasets. It is not: 500 rows produce a real IVF plan, and the whole
test file runs in about a second.

The real trap is **index build order**. An index applied to an empty table has
nothing to train on, and Lance plans a flat scan from then on — `indices_loaded
= 0`, every row scanned, and a positive test that silently proves nothing.
`an_index_built_before_ingest_is_not_consulted` pins that as an observable
outcome rather than a comment.
