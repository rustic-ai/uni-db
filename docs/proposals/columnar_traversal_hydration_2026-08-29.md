# Columnar traversal hydration

Status: proposed, 2026-08-29. Issue: #209.

## The defect, as measured

Reaching a property through a traversal costs far more than reading the same
column through a scan, and the gap is not in the storage layer — both sit on the
same `_vid IN (…)` filtered read.

Locally reproducible in seconds via `crates/uni/examples/hydration_path_probe.rs`
(allocator-instrumented). Holding the traversal's output at 60,000 rows and
growing the target label table from 60k to 300k rows with **unreachable decoy
rows** — rows no edge reaches, which cannot change the answer or the number of
rows hydrated:

| arm | rows produced | peak MiB | bytes/row |
|---|---|---|---|
| scan, 1 property | 300,000 | 94.3 | **329** |
| traverse untyped, no property | 60,000 | 75.0 | **1,311** |
| traverse untyped, 1 property | 60,000 | 1620.9 | **28,327** |
| traverse typed `(t:Tgt)`, no property | 60,000 | 1622.5 | 28,356 |

Three things follow, and each is a control for the others:

1. **Cost scales with the target *table*, not with rows produced.** The decoys
   are the only thing that changed; output rows were identical.
2. **Reading one column costs 21.6× the same traversal without it**, and 86× the
   scan path over the same data.
3. **Label verification takes the same path.** The typed arm blows up with no
   property read at all, because `hasLabel` resolution hydrates too. Any
   target-side attribute access pays this, not just properties.

At LDBC SF1 the same defect OOM-kills IC3 at a 16 GiB cap; `count(*)` over its
2.84M-row join costs 4.1 GB while `count(message.creationDate)` over the identical
join does not complete.

## Root cause

`PropertyManager::get_batch_vertex_props{,_for_label_projected}`
(`crates/uni-store/src/runtime/property_manager.rs:483`, `:1197`) issues
`backend.scan(ScanRequest)` with `_vid IN (…)` and a column projection — then
immediately destroys the result. `extract_row_properties` (`:1641`) plus
`merge_overflow_into_props` (`:1708`) shred the `RecordBatch` row by row into
`HashMap<Vid, HashMap<String, Value>>` (`Properties` is
`HashMap<String, Value>`, `crates/uni-common/src/lib.rs:45`). The caller —
`build_target_property_columns` (`traverse.rs:962`) via
`build_property_column_static` (`scan.rs:2565`) — then walks that map back into
an Arrow array.

So the columnar data already exists inside the property manager and is
round-tripped through a per-row hash map to reach an Arrow column. Two costs
follow: one `HashMap` allocation and `String` key per row, and a per-batch
re-scan of the target table whose cost is `O(batches × table_size)`.

The label-free variant is worse: it loops over every candidate label table
(`:508`-`:528`), falling back to **every label in the schema** if `VidLabelsIndex`
misses a single vid, issuing one filtered scan per label.

## What the scan path already does

`crates/uni-query/src/query/df_graph/scan.rs::columnar_scan_vertex_batch_static`
(`:1716`) calls
`StorageManager::scan_vertex_table_counted(label, columns, filter, counters)
-> Result<Option<RecordBatch>>` (`manager.rs:1645`) and then stays in Arrow
throughout: MVCC dedup, L0 merge, tombstone and label-overwrite filtering,
output-schema mapping. It never builds a `HashMap<String, Value>` except for the
`_all_props` and overflow blob paths.

**The API this proposal needs therefore already exists.** The work is to route
traversal hydration through it and keep the result columnar.

## Design

Add, beside the existing map API:

```rust
/// Rows in `vids` order, one row per requested vid, `_vid` retained.
/// A vid not visible under this snapshot yields a null row, which is
/// distinct from a property that is null.
pub async fn get_batch_vertex_props_columnar(
    &self, vids: &[Vid], label: Option<&str>, requested: &[&str],
    ctx: Option<&QueryContext>,
) -> Result<RecordBatch>
```

Two decisions carry the design:

- **Return rows in the caller's `vids` order, doing the gather internally.** The
  storage read returns scan order and drops invisible vids; a `Vid → row_index`
  map built once per batch plus `arrow::compute::take` aligns it. That is one
  `u64` hash per row against today's `HashMap<String, Value>` allocation per row,
  and it converts most random-access callers into positional ones for free.
- **Preserve presence separately from nullity.** Today "absent from the map"
  means "not visible", and several callers key correctness off it
  (`search_procedures.rs:480`/`:564` drop such candidates,
  `pattern_exists.rs:369`, `build_vertex_property_filter`). A null `_vid` in the
  output row carries that signal; a null property column does not.

`_all_props` keeps its current behaviour — it is a whole-entity request and has
no narrower columnar form. It is unaffected by this change.

## Migration, by what each caller actually needs

From the caller survey. The groups matter because they determine cost.

**Positional — migrate by deleting a loop.** `build_target_property_columns`
(`traverse.rs:962`), `hydrate_vlp_target_properties` (`:4490`), `build_edge_columns`
(`:1083`), `vector_knn.rs:871`, `search_procedures.rs:945`,
`pattern_comprehension.rs:313`/`:357`, `uni-algo/src/algo/projection.rs:810`
(which hand-rolls columnarisation today). These build Arrow arrays over the same
slice they fetched.

**Dedupe-then-lookup — free once the API returns aligned rows.**
`build_edge_adjacency_and_target_props` (`:2718`, whose map is shared across
`poll_next` via `Arc` and indexed per expansion tuple), `pattern_exists.rs:369`,
`build_vertex_property_filter` (`:2835`), `projection.rs:710`. These need a
`Vid → row` map, which the aligned-return decision supplies.

**Stay on the map API.** `EntityPropertyCache` (`common.rs:534`), `Prefetch`
(`mutation_common.rs:118`), the MERGE prefetch (`write.rs:2469`). These are
random-access caches and write paths that want owned, mutable `Properties`.
Migrating them buys nothing and risks the SET/REMOVE paths.

**Order of work**

1. The columnar API plus its unit tests, with the map API untouched.
2. `build_target_property_columns` — the path the probe measures. Stop here and
   re-measure; if the numbers do not move, the design is wrong and nothing else
   has been touched.
3. The remaining positional callers.
4. The dedupe-then-lookup callers.
5. Only then consider deleting map-API paths that no longer have callers.

**Do not fold in** the `LargeBinary` blob encoding of scalar properties on the
unlabelled path (`traverse.rs:1026`, `pattern_comprehension.rs:322`,
`expand_batch` at `:2427`, which encodes *every* target property as a CypherValue
blob). It is a real second inefficiency and a separate change; conflating them
would make this one unmeasurable.

## Acceptance

Measured by `hydration_path_probe`, which runs in seconds:

- **Decoy insensitivity.** Growing the target table 5× with unreachable rows must
  not materially change the traversal's per-row cost. Today it is 10.6×. This is
  the primary criterion — it tests the `O(table_size)` behaviour directly.
- **Reading one column approaches the scan's per-row cost**, against 86× today.
- **Cost scales with the number of properties read.** Flat is the signature of a
  per-row allocation that ignores the request.
- The typed arm (`hasLabel` verification) improves alongside, since it shares the
  path.

Then, at SF1 under the protocol used for #198/#209 — `systemd-run --user --scope
-p MemoryMax=16G -p MemorySwapMax=0 -p OOMPolicy=continue`, `OOMPolicy` being
required or systemd kills the bench's own supervisor — `ic3_stage_probe` stage 7
(`count(message.creationDate)` over the 2.84M-row join) should fall well below
its current 10.5 GB.

**IC3 completing is not promised by this change.** Stage 7 alone consumes 10.5 GB
of the 16 GB budget today, and the full query adds a join and an aggregation on
top. IC3 has already been attributed twice to mechanisms it does not use; the
criterion here is the per-row cost, and whether IC3 passes is a measurement to
take afterwards, not a prediction to make now.

## Risks

- **MVCC and L0 correctness.** The map path applies version ranking, tombstones
  and an L0 overlay per vid. The scan path does the equivalent with Arrow
  kernels (`mvcc_dedup_batch_by`, `merge_lance_and_l0`, `filter_l0_tombstones`),
  so the logic exists — but it is the part where a mistake is a wrong answer, not
  a slow query. The existing repros are the guard:
  `uni-store/tests/common/bugs/repro_03_batch_edge_props_version_ignored.rs`,
  `repro_07_batch_vertex_props_version_ignored.rs`,
  `repro_17_overlay_tombstone_ungated.rs`, `repro_edge_props_lost_after_compaction.rs`,
  and `crates/uni/tests/common/storage/property_batch_test.rs`.
- **Schemaless properties.** Values in `overflow_json` have no column to read.
  The columnar path must keep the existing decode for those, and only for those.
- **The label-free path** must not regress into scanning every label in the
  schema. `VidLabelsIndex` resolution should be a precondition for the columnar
  route, with the map path as fallback.
- **Two paths during migration.** Both APIs exist until step 5, so behaviour must
  match. The step-2 stop-and-measure exists so that divergence is caught while
  only one caller has moved.
