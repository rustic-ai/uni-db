# Index keys: scalar indexes for nested and opaque property types (2026-11-23)

Follows #356. A BTree, Hash or Bitmap index on a property stored as a nested
Arrow column (DATETIME, TIME, POINT, MAP, LIST, VECTOR) cannot be built, and one
on an opaque column (BTIC, DURATION, JSON) builds but no predicate can use it.
#356's interim fix refuses the nested ones at declaration. This design lets the
types that have a meaningful order or a useful prefilter be indexed, through
**index keys**: flat columns derived from the property, indexed in its place,
with predicates on the property rewritten into predicates on the key.

The design comes from three read-only investigations (planner, storage and
build, per-type semantics with probes). File references are to the code at the
time of writing.

## Principles

1. **A key filter is a superset, never the answer.** The original predicate on
   the property is always kept: in the scan's Arrow runtime filter and in the
   `Filter` above the scan. A key may admit extra rows; it must never drop a
   match. This is the AGENTS.md candidate-prefilter rule applied to Lance
   pushdown.
2. **Keys are derived at batch build, never stored in L0**, like
   `__jfts_<column>` (`VertexDataset::build_record_batch_with_timestamps`,
   `json_fts_derived_rows`). The content UID is computed from the row's
   properties with every derived column (`__jfts_`, `__key_`) stripped.
3. **Keys follow the engine's actual comparison semantics**, verified by probe,
   not openCypher's. Where `WHERE =` (by instant) and `IN` / DISTINCT /
   grouping (structural) differ, the key serves the coarser one and the
   residual does the rest.
4. **Only same-type, constant operands are served.** `<>`, cross-type
   comparisons (which the engine answers as constant false/true/NULL), and
   non-constant operands are never rewritten.
5. **Differential tests are the proof.** Every served predicate shape is run
   with and without the index, on flushed rows, unflushed rows, updated and
   deleted rows, and parameters, and the answers must match.

## Catalog

`ScalarIndexConfig` gains
`#[serde(default, skip_serializing_if = "Option::is_none")] key: Option<IndexKey>`.
`properties` keeps the logical property (`["ts"]`), so `SHOW INDEXES`,
`get_label_info`, `label_property_is_indexed` and Python keep reporting the
property; they gain an optional "key" detail.

```rust
pub struct IndexKey { pub kind: KeyKind, pub columns: Vec<String> }
pub enum KeyKind {
    DateTimeInstant,        // [__key_dt_<p>]           Int64 nanos_since_epoch
    TimeUtc,                // [__key_time_<p>]         Int64 nanos − offset·1e9
    BticBounds,             // [__key_lo_<p>, __key_hi_<p>] Int64 ms, raw (sentinels kept)
    MapPath { path: Vec<String>, value: DataType }, // [__key_map_<p>_<path>] typed T
    PointCoords,            // [__key_px_<p>, __key_py_<p>] Float64 (x,y | lon,lat)
}
```

`key` is chosen from the property's declared type when the index is declared;
a user does not spell it. `"__key_"` joins `RESERVED_PROPERTY_PREFIXES`.
`same_config_as` compares `key`. `IndexKey::physical_columns()` is what is built,
listed and matched. An older binary ignores `key` and sees a BTree on `ts`,
which it fails to build (as today).

DATETIME uses a derived Int64 too, not Lance's nested path
`ts.nanos_since_epoch`: one lifecycle for every key, and the filter renderer
(`quote_ident`) and `Scalar` (no timestamp variant) would both need changes for
a nested path.

## Storage and build (generalizing `__jfts_`)

- **Derivation:** one `derived_rows(schema, label, vertices)` replaces
  `json_fts_derived_rows`, covering JSON full-text text and every key kind.
  Every label-table batch builder already goes through
  `build_record_batch_with_timestamps` (flush, compaction, declared-label
  backfill, bulk).
- **UID:** `compute_vertex_uid` hashes properties with `__jfts_` / `__key_`
  names removed. This also fixes an existing drift: compaction
  (`compaction.rs` ~328) and SET on a flushed vertex (`get_all_vertex_props`
  → `merge_props`) carry `__jfts_` text into the row's properties, so the
  stored UID of a JSON-full-text-indexed row changes at compaction. Pinned by a
  regression test first.
- **Create:** storage first, as for `__jfts_`: backfill the key columns on the
  head backend under the table write lock (`replace_table_atomic`), then
  `add_internal_property` for each, then build one Lance index per physical
  column, then `add_index` + `save` (which publishes the cut).
- **List / match:** `list_indexes` keeps reporting columns;
  `ensure_declared_indexes`'s `covers` compares against
  `physical_columns()` (every key column covered). A config with a key is not
  refused by `scalar_index_refusal`.
- **Drop:** drops every physical index of the config, and unregisters key
  columns no other index uses (as `__jfts_` does).
- **Rebuild:** re-derives (backfill) before building, as JSON full-text does.
- **Hiding:** key columns are reserved names, hidden where `__jfts_` is.
  `get_label_info`'s `property_infos_for` currently lists internal properties
  (`__jfts_`, `__fde_`, `_gen_`); it must skip reserved names.
- **Forks:** a key DDL on primary widens live fork branches with `AllNulls`
  (the `widen_for_checked_properties` loop); never backfill a branch. Key
  pushdown is not used on a fork, pinned snapshot or transaction view (the
  index cannot vouch for those views), matching the existing prefilter rule.
  Fork-local auto-builds skip key indexes.

## Planner

- **Hook:** in `IndexAwareAnalyzer` (`uni-query-functions/src/pushdown.rs`)
  and `build_indexed_property_pushdown` (`df_planner.rs` ~2253), after
  `substitute_params`. Not in the logical planner, which does not see
  parameter values and whose expression-index rewrite replaces the original.
- **Constant folding:** `fold_constant_operand(expr, params) -> Option<Value>`
  evaluates `datetime(…)`, `time(…)`, `btic(…)`, `point(…)` with all-constant
  arguments (and `$param` values of those types), reusing the statement-clock
  rule and refusing `.realtime()`. `is_row_independent` accepts such calls so
  `datetime(…) < m.ts` is oriented.
- **Rendering:** a key bound is a `FilterExpr::Compare` on the key column with
  a `Scalar::Int` / `Scalar::Float` / `Scalar::Str`. Bounds for one predicate
  are AND-ed (OR for the two-range BTIC and antimeridian cases). This is added
  to the Lance SQL only; the Arrow runtime filter stays the original predicate,
  because unflushed rows have no key columns.
- **Type check:** `index_operand_fits_column` (`df_planner.rs` ~187-224) loses
  its `_ => true` arm for key and nested columns, so a mistyped literal is never
  pushed against a struct or key column. `l0_property_prefilter`
  (`scan.rs` ~2052) returns `None` for nested and opaque types.
- **Observability:** `collect_index_usage` reports the logical property with
  `used: true` and the key kind; PROFILE's `index_scans` counts the lookups.

## Per-type keys and served predicates

| Type | Key | Served (all with the original kept) | Not served |
|---|---|---|---|
| DATETIME | `nanos_since_epoch` (Int64) | `= < <= > >=`, `IN` (superset), pattern map, `$param` | `<>`, cross-type, ORDER BY |
| TIME | `nanos_since_midnight − offset·1e9` (Int64) | `= < <= > >=`, `IN` (superset) | `<>`, cross-type |
| BTIC | `lo`, `hi` (Int64 ms, raw) | `btic_overlaps / contains / during / before / after / meets / starts / finishes / equals / contains_point` with one constant side; `=` (lo and hi equal, superset); `disjoint` / `adjacent` as an OR | ORDER BY |
| MAP (typed `MAP<string,T>`) | the value at a declared key path (type T) | `= < <= > >=`, `IN` on `n.m.k` | whole-map `=` |
| POINT | x, y (or lon, lat), Float64, −0.0 normalized | `point.withinBBox(p, …)`, `distance(p, c) < r` / `<= r` as a bbox superset (Cartesian ±r; WGS-84 by lat ± r/R and lon widened by 1/cos(lat), clamped at the poles, union across the antimeridian) | `=`, ordering |
| LIST | none | refused, pointing to `CREATE INVERTED INDEX` (LIST<STRING>) or LabelList | |
| VECTOR | none | refused, pointing to a vector index | |
| DURATION, JSON (whole column) | none | a new BTree is refused (byte order is meaningless; DURATION is not orderable) | |

Semantics the keys rest on (probed):

- DATETIME `WHERE =`/ranges compare instants; `IN`/DISTINCT/grouping are
  structural (offset and zone). A `localdatetime` written into a DATETIME
  column is stored offset-NULL and WHERE treats its local nanos as the instant:
  its key is those nanos. A `localdatetime` *literal* against a DATETIME column
  is incomparable: not served.
- TIME compares `nanos − offset·1e9` with no modulo (range fits Int64). A
  `localtime` stored in a TIME column (offset NULL): the key must reproduce
  whatever WHERE evaluates for such rows; the implementation probes it and the
  differential test pins it.
- BTIC predicates compare raw `lo`/`hi` (sentinels `i64::MIN`/`MAX` behave);
  `btic_lo`/`btic_hi` return NULL for sentinels, so key columns store the raw
  values, not those functions. `contains_point`'s point converts as the
  predicate does (datetime `nanos / 1e6` truncating toward zero, date
  `days · 86 400 000`, Int as ms).
- Floats: −0.0 and +0.0 are equal in Cypher; float keys normalize −0.0.

## Phases

1. **Framework + DATETIME** — catalog `key`, derived-column lifecycle (create,
   backfill, list/match, drop, rebuild, fork widening), UID stripping with the
   `__jfts_` drift regression test, constant folding, key-aware analyzer and
   pushdown, type check, `get_label_info` hiding, EXPLAIN. DATETIME end to end.
2. **BTIC** — two-key derivation and the predicate-to-bounds rule.
3. **TIME and typed MAP paths** — the Int64 time key; a path-key index form
   (`CREATE INDEX … ON (n.m.k)` resolves to `MapPath`).
4. **POINT, and refusals with pointers** — coordinate keys and the bbox /
   distance rules; LIST, VECTOR, DURATION and whole-JSON refusals naming the
   right index kind.
5. **Review and verification** — an independent review; workspace, failpoint,
   TCK and Python suites; docs (Black Book index chapter, `concepts/indexing.md`,
   `features/schema-indexing.md`), release notes.

Each phase lands with differential tests (index vs no index) over: flushed,
unflushed, updated and deleted rows; literals and parameters; boundary values
(1 ns / 1 ms apart, sentinels, −0.0, NULL); and EXPLAIN / PROFILE showing the
key used.

## Out of scope (found while researching; separate follow-ups)

- Python's `"timestamp"` type name maps to DATETIME, so Python cannot declare
  the flat TIMESTAMP type.
- Point equality is NULL/false (a `point()` literal carries `z: null`);
  a cartesian point written to a geographic column is stored NULL and its
  pre-flush read fails; `distance` / `withinBBox` raise on NULL arguments.
- `n.m['k']`, `n.j['k']` and `ORDER BY n.j.a` fail at planning; typed-MAP
  whole-map `=` returns nothing.
- A `localdatetime` / `localtime` stored in DATETIME / TIME columns gets
  inconsistent WHERE vs ORDER BY semantics.
- `toString` on a TIME column fails.
- `executor/core.rs::compare_temporal` returns Equal for any two BTIC values.
- Serving ORDER BY from a key index (none of the scalar indexes serve ORDER BY
  today).
- The JSON full-text backfill does not widen live fork branches.
