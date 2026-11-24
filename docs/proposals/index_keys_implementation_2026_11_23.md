# Index keys: implementation proposal (2026-11-23)

Implements the design in [index_keys_2026_11_23.md](index_keys_2026_11_23.md):
scalar indexes on DATETIME, TIME, BTIC, typed MAP paths and POINT through
derived **key columns**, and a clear refusal, naming the right index kind, for
LIST, VECTOR, DURATION and whole JSON columns. Follows #356, whose interim fix
refuses every scalar index on a nested type.

This document is the work plan: the data model, the algorithms, every code
location that changes, the tests that prove each step, and the order of work.
Code references were checked against the tree on 2026-11-23.

## 1. Goals and non-goals

**Goals**

- `CREATE INDEX … ON (n.p)` (and the builder and procedure equivalents) on a
  DATETIME, TIME, BTIC or POINT property, or on a typed MAP key path
  (`n.m.k`), builds an index that the planner uses for the predicates listed in
  §5, with answers identical to an unindexed scan.
- The same declaration on LIST, VECTOR, DURATION or a whole JSON column is
  refused with an error that names the index kind that does fit.
- Existing catalogs keep opening. A scalar index already recorded on one of
  these types is upgraded to a keyed index on its next build.

**Non-goals** (listed in the design's out-of-scope section): serving ORDER BY
from a key; JSON path keys; the engine bugs found during research (point
equality, `n.m['k']`, local temporals in zoned columns, `compare_temporal` on
BTIC, TIME `toString`); Python's `"timestamp"` → DATETIME mapping.

## 2. Invariants this work must keep

From AGENTS.md and the research:

1. **A key filter is a superset.** It is added to the Lance SQL only. The
   scan's Arrow runtime filter and the `Filter` above the scan stay the original
   predicate. (`build_indexed_property_pushdown`, `df_planner.rs:2253`, already
   renders both; the key changes only the first.)
2. **Unflushed rows have no key columns.** They are never filtered by a key.
   The existing path merges L0 rows unfiltered after the Lance read
   (`columnar_scan.rs` ~1384-1393) and drops superseded pushdown rows
   (`drop_superseded_pushdown_rows`, ~1408); keys rely on both.
3. **Keys are computed at batch build and never stored in L0**, like
   `__jfts_` (`VertexDataset::build_record_batch_with_timestamps`,
   `vertex.rs:96`). The content UID never includes a derived column.
4. **Storage before catalog**: backfill a key column on the head backend
   before the catalog registers it; every Lance commit goes through
   `LanceDbBackend`; a catalog save publishes the cut (`CutPublisher`).
5. **No key pushdown where the index cannot vouch for the view**: a fork, a
   pinned snapshot, or a transaction's private L0 (the existing prefilter rule,
   `candidate_prefilter`, `df_planner.rs:2163`).

## 3. Data model

### 3.1 Catalog (`crates/uni-common/src/core/schema/index_types.rs`)

```rust
pub struct ScalarIndexConfig {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,          // logical: ["ts"] or ["meta.k"]
    pub index_type: ScalarIndexType,
    pub where_clause: Option<String>,
    #[serde(default)]
    pub metadata: IndexMetadata,
    /// Set when the property's type is indexed through derived key columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<IndexKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexKey {
    pub kind: KeyKind,
    /// Physical key columns, in `KeyKind::column_roles()` order.
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
#[non_exhaustive]
pub enum KeyKind {
    DateTimeInstant,                       // [__key_dt_<p>]           Int64
    TimeUtc,                               // [__key_time_<p>]         Int64
    BticBounds,                            // [__key_lo_<p>, __key_hi_<p>] Int64
    MapPath { path: Vec<String>, value: DataType }, // [__key_map_<p>_<path…>] T
    PointCoords { geographic: bool },      // [__key_px_<p>, __key_py_<p>] Float64
}
```

Helpers, all in `index_types.rs`:

- `KeyKind::for_property(dt: &DataType, path: &[String]) -> KeyChoice` returns
  `Key(KeyKind)`, `Plain` (a flat type: no key), or `Refuse(&'static str)`
  with the index kind to use instead (§5.6). Replaces
  `ScalarIndexType::cannot_index` as the decision point.
- `KeyKind::column_names(property: &str) -> Vec<String>` builds the reserved
  names. A MAP path segment is sanitized to `[A-Za-z0-9_]` with a short hash
  suffix, so two paths never share a column.
- `KeyKind::arrow_types() -> Vec<ArrowDataType>`.
- `IndexKey::physical_columns(&self) -> &[String]`.
- `ScalarIndexConfig::physical_columns(&self) -> Vec<&str>`: the key's columns,
  or `properties` when there is no key.
- `same_config_as` (`index_types.rs` ~187) compares `key` too.

`Schema::scalar_index_refusal` (added by #356, `index_types.rs` ~765) becomes
`Schema::resolve_scalar_index(&mut ScalarIndexConfig) -> Result<()>`: it sets
`key` from the declared type (or leaves it `None` for flat types) and returns
the refusal for the `Refuse` cases. `SchemaManager::add_index`
(`schema.rs` ~2661) and `IndexManager::create_scalar_index`
(`index_manager.rs:809`) call it for new definitions; an existing definition
without `key` on a keyable type gets one at its next build (upgrade, §7).

### 3.2 Reserved names (`crates/uni-common/src/core/schema.rs:2977`)

`RESERVED_PROPERTY_PREFIXES` gains `"__key_"`. Key columns are registered with
`add_internal_property` (`schema.rs:2508`) as nullable, typed per
`KeyKind::arrow_types` (Int64 → `DataType::Int64`, Float64 → `Float64`, a MAP
value type as declared).

### 3.3 Key derivation (`crates/uni-store/src/storage/index_keys.rs`, new)

```rust
/// The key values of `value` for `kind`, one per key column; `None` for a
/// NULL or absent property, or a value the key cannot represent.
pub fn derive(kind: &KeyKind, value: &Value) -> Vec<Option<KeyValue>>;
pub enum KeyValue { Int(i64), Float(f64), Str(String) /* MAP of strings */ }
```

| Kind | From | Key value(s) |
|---|---|---|
| `DateTimeInstant` | `Temporal::DateTime { nanos_since_epoch, .. }` | `nanos_since_epoch` |
| | offset-NULL row (a `localdatetime` stored in the column) | its `nanos_since_epoch` (WHERE treats it as the instant; probed) |
| `TimeUtc` | `Temporal::Time { nanos_since_midnight, offset_seconds }` | `nanos_since_midnight − offset_seconds·1e9` (no modulo; range fits Int64) |
| | offset-NULL row | the value WHERE compares; Phase 3 probes it and pins it with a differential test |
| `BticBounds` | `Temporal::Btic { lo, hi, .. }` or a BTIC string literal (as `build_btic_column`, `arrow_convert.rs:1857`) | raw `lo`, raw `hi` (sentinels `i64::MIN` / `i64::MAX` kept; not `btic_lo`/`btic_hi`, which return NULL for them) |
| `MapPath` | the value at `path` | the value, coerced to `value`'s type; missing key → `None` |
| `PointCoords` | `{x, y[, z], crs}` / `{latitude, longitude, crs}` | `(x, y)` or `(longitude, latitude)`, −0.0 → +0.0 |

The table is the specification; each row gets a unit test in `index_keys.rs`.

## 4. Storage and build

### 4.1 One derived-column step (`crates/uni-store/src/storage/vertex.rs`)

Replace `json_fts_derived_rows` (`vertex.rs:327`) with
`derived_rows(schema, label, vertices) -> Option<Vec<Properties>>`, which fills
both `__jfts_<col>` text (unchanged) and every key column of every keyed
scalar index on the label whose columns are registered. Called from
`build_record_batch_with_timestamps`, which every label-table batch goes
through: flush (`writer.rs` ~7765), compaction (`compaction.rs` ~407),
declared-label backfill (`manager.rs` ~2318), bulk (`uni-bulk/src/bulk.rs`
~1186).

### 4.2 Content UID without derived columns (fixes an existing drift)

`VertexDataset::compute_vertex_uid` (`vertex.rs:47`) and its callers hash a
property map. Add `strip_derived(props) -> Cow<Properties>` removing every
`__jfts_` and `__key_` entry, applied:

- in `build_record_batch_with_timestamps` before hashing (`vertex.rs` ~111);
- where compaction rebuilds `row_props` from `schema.properties[label]`
  (`compaction.rs` ~328-341), which today includes `__jfts_` and so re-hashes
  a JSON-full-text-indexed row to a different UID;
- where a SET on a flushed vertex reads every declared column
  (`PropertyManager::get_all_vertex_props`, `property_manager.rs` ~1422, fed to
  `merge_props`, `executor/write.rs` ~363-380 / ~590-615), so derived values
  never enter L0.

Step 1 of Phase 1 is a regression test that fails today (§9, `T1`).

### 4.3 Create (`IndexManager::create_scalar_index`, `index_manager.rs:809`)

For a config with a key, in this order:

1. `resolve_scalar_index` (refusals, key choice).
2. If the label table exists: `backfill_key_columns(&config)` — generalized
   from `backfill_json_fts_column` (`index_manager.rs:1199`): head backend,
   table write lock, scan, `derive`, `replace_table_atomic` with the live
   schema plus the key columns.
3. `add_internal_property` for each key column not yet registered.
4. Widen live fork branches with the key columns (`AllNulls`), reusing the
   branch loop of `StorageManager::widen_for_checked_properties`
   (`manager.rs:2879`). Never backfill a branch.
5. Build one Lance BTree per physical column, named `<index>__<role>`
   (`idx_ts__dt`, `idx_valid__lo`, `idx_valid__hi`).
6. `add_index` + `save` (publishes the cut).

A build failure of any physical index records the config `Stale` (retried) as
today; a refusal records nothing.

### 4.4 List, match, drop, rebuild

- `ensure_declared_indexes` (`index_manager.rs:925`): `covers` compares
  `config.physical_columns()`; a keyed index is covered when every key column
  has an index. A keyed index whose key columns are not registered yet (a
  catalog from before this change) goes through §4.3 from step 2.
- `drop_index` (`index_manager.rs:1286`): drop every physical index of the
  config; unregister key columns no remaining index uses (as `__jfts_` does).
- `rebuild_indexes_for_label` (`index_manager.rs:1334`): re-derives (step 2)
  before building, as JSON full-text does.
- `LanceDbBackend::list_indexes` (`lance.rs:2064`) is unchanged: key columns
  are top-level.

### 4.5 Visibility

- Hidden wherever `is_reserved_property_name` (`schema.rs:2985`) already hides
  `__jfts_`: scan projection, `RETURN n`, `properties(n)`, schema procedures.
- `get_label_info` → `property_infos_for` (`crates/uni/src/api/schema.rs`
  ~1168) lists every catalog property, internal ones included (`__jfts_`,
  `__fde_`, `_gen_` today). Skip reserved names.
- `SHOW INDEXES`, `uni.schema.indexes()`, `IndexInfo` and Python gain an
  optional `key` detail (`"datetime instant"`, `"btic lo/hi"`, …).

### 4.6 Forks

- Fork-local auto-builds (`crates/uni/src/api/fork_maintenance.rs` ~105,
  `uni-fork/src/maintenance.rs`) skip keyed configs.
- `fork_index_exists` and the fusion rewrite are unaffected (no keyed
  fork-local indexes).
- Key pushdown is off for forked views (§2.5), so a branch that predates a key
  column is never asked to filter on it.

## 5. Planner

### 5.1 Constant folding (`crates/uni-query-functions/src/pushdown.rs`)

```rust
/// The value of `expr` when it is constant for the statement: a literal, a
/// resolved parameter, or `datetime / localdatetime / date / time /
/// localtime / duration / btic / point` called on constant arguments
/// (evaluated with `crate::datetime::eval_datetime_function` and the BTIC and
/// spatial evaluators). `None` for anything row-dependent, `realtime`
/// variants, and a call that fails to evaluate.
pub fn fold_constant_operand(expr: &Expr, params: &HashMap<String, Value>) -> Option<Value>;
```

- Reuses the statement-clock rule of `try_constant_fold_temporal`
  (`df_expr.rs` ~2335-2389); no-argument clock calls fold to the statement
  time, `.realtime()` never folds.
- `is_row_independent` (`pushdown.rs:1264`) accepts the same calls, so
  `datetime('…') < m.ts` is oriented by `orient_property_comparisons`
  (`pushdown.rs` ~1210).
- Runs in the physical planner, after `substitute_params`
  (`pushdown.rs:1511`), so a parameter's per-execution value becomes a key
  bound; the logical plan cache is unaffected.

### 5.2 Key-aware analysis

`IndexAwareAnalyzer::analyze` (`pushdown.rs:122`) records, for each conjunct
on a keyed property, a `KeyedPredicate { property, index_name, key, form }`
in a new `PushdownStrategy::keyed_predicates`. `form` is one of the shapes in
§5.4. The conjunct still goes to `residual` (it is never Lance-pushable as is:
its column is nested or opaque). `indexed_equality_column` /
`indexed_range_column` (`pushdown.rs:200`, `:253`) skip keyed indexes, so a
keyed index is never mistaken for a plain one.

### 5.3 Rendering (`build_indexed_property_pushdown`, `df_planner.rs:2253`)

For each `KeyedPredicate`: fold the constant operand (§5.1); if it folds to a
value of the column's own type, compute the bounds (§5.4) and render them as
`FilterExpr::Compare { column: key_col, op, value: Scalar::Int|Float|Str }`,
combined with `And` / `Or`. Append to the Lance SQL (`extra_lance_filter`,
`df_planner.rs` ~3150). The Arrow runtime filter (`extra_runtime_filter`) and
the outer `FilterExec` are unchanged: they evaluate the original predicate.

If the operand does not fold, folds to NULL, or folds to another type, the
predicate is not rewritten: the scan reads everything and the residual
answers, exactly as today.

Guards:

- `index_operand_fits_column` (`df_planner.rs:187`): its `_ => true` arm
  becomes `false` for nested and binary columns, so no literal is ever pushed
  against a struct or a key-less opaque column.
- `l0_property_prefilter` (`scan.rs:2052`) returns `None` for nested and
  opaque property types.
- No key pushdown when the view is a fork, pinned, or a transaction's.

### 5.4 Bounds per type

Notation: `k` the key column(s), `c` the folded constant.

**DATETIME** (`k` = instant nanos) and **TIME** (`k` = UTC nanos):

| Predicate | Key bound | Exact? |
|---|---|---|
| `p = c`, pattern map `{p: c}` | `k = key(c)` | exact (instant equality) |
| `p < / <= / > / >= c` | same op on `k` | exact |
| `p IN [c1…]` (all constants) | `k IN (key(c1)…)` | superset (IN is structural) |
| `p <> c`, cross-type `c` | not rewritten | — |

**BTIC** (`lo`, `hi`; constant `c` with `c.lo`, `c.hi`; `p` the property, in
either argument position — mirrored predicates swap):

| Predicate | Bound |
|---|---|
| `btic_overlaps(p, c)` | `lo < c.hi AND hi > c.lo` |
| `btic_contains(p, c)` | `lo <= c.lo AND hi >= c.hi` |
| `btic_contains(c, p)` / `btic_during(p, c)` | `lo >= c.lo AND hi <= c.hi` (`during`: strict) |
| `btic_before(p, c)` | `hi <= c.lo` |
| `btic_after(p, c)` | `lo >= c.hi` |
| `btic_meets(p, c)` | `hi = c.lo` |
| `btic_starts(p, c)` | `lo = c.lo AND hi < c.hi` |
| `btic_finishes(p, c)` | `hi = c.hi AND lo > c.lo` |
| `btic_equals(p, c)`, `p = c` | `lo = c.lo AND hi = c.hi` (`=` also compares meta: superset) |
| `btic_disjoint(p, c)` | `hi <= c.lo OR lo >= c.hi` |
| `btic_adjacent(p, c)` | `hi = c.lo OR lo = c.hi` |
| `btic_contains_point(p, t)` | `lo <= ms(t) AND hi > ms(t)`, `ms` as the predicate converts (datetime `nanos / 1e6` toward zero, date `days·86 400 000`, Int as is) |

The existing `BticContainsPointRule` (`rewrite/rules/btic.rs`) rewrites
`btic_contains_point` into `btic_lo(e) <= p AND btic_hi(e) > p` before
planning. Key analysis matches that rewritten form too (`btic_lo(n.p) op c`
and `btic_hi(n.p) op c`), with the sentinel caveat: `btic_lo`/`btic_hi` return
NULL for an unbounded side, so the bound uses the raw key with the sentinel
included, which keeps it a superset.

**MAP path** (`k` = value at the path, type T): `n.m.k = / < / <= / > / >= c`
and `IN` with `c` of type T → the same op on `k` (exact for `=`/ranges; `IN`
superset). Whole-map `=` is not rewritten.

**POINT** (`kx`, `ky`):

| Predicate | Bound |
|---|---|
| `point.withinBBox(p, ll, ur)` | `kx >= ll.x AND kx <= ur.x AND ky >= ll.y AND ky <= ur.y`; geographic with `ll.lon > ur.lon`: `(kx >= ll.lon OR kx <= ur.lon) AND ky …` |
| `distance(p, c) < r` / `<= r` (and `r > distance(…)`) | Cartesian: `kx ∈ [c.x−r, c.x+r] AND ky ∈ [c.y−r, c.y+r]`. Geographic (R = 6 371 000 m, the engine's haversine): `ky ∈ [lat−Δ, lat+Δ]`, `Δ = r/R` in degrees; longitude `± Δ/cos(max |lat±Δ|)`, the whole range when that reaches a pole, split in two across the antimeridian |
| a CRS of the constant different from the column | not rewritten (`distance` errors there; the residual raises the same error) |

Every POINT bound is a superset; the residual computes the exact predicate.

### 5.5 Observability (`planner.rs:11700`)

`collect_index_usage` reports a keyed predicate as `used: true` on the logical
property, with the key kind; PROFILE's `index_scans` comes from
`index_consulted` as for plain indexes. Tests assert both.

### 5.6 Refusals with pointers

`KeyKind::for_property` refuses, with these messages:

| Type | Message points to |
|---|---|
| `LIST<STRING>` | `CREATE INVERTED INDEX … ON (n.p)` |
| other `LIST<T>` | an inverted index once it supports T; today: none, queries scan |
| `VECTOR`, `LIST<VECTOR>` | `CREATE VECTOR INDEX` |
| `DURATION` | durations are not ordered (`<` is NULL); no scalar index |
| JSON (whole column) | `CREATE JSON FULLTEXT INDEX`, or an index on a typed MAP path |
| `MAP` (whole map) | an index on `n.m.<key>` |

A new BTree on DURATION or a whole JSON column is refused. One already in a
catalog stays as it is: it builds, no predicate is pushed to it (as today),
and it reports `Online`.

## 6. Declaration surfaces

All four paths reach `SchemaManager::add_index` and `create_scalar_index`, so
`resolve_scalar_index` covers them:

- Cypher `CREATE INDEX` (`planner.rs` ~12240, `normalize_expression_for_storage`
  turns `n.meta.k` into `"meta.k"`; `executor/read.rs` ~3020). A dotted
  property whose head is a declared MAP property resolves to `MapPath`; any
  other expression keeps going to the expression-index path.
- `uni.schema.createIndex` (`executor/ddl_procedures.rs` ~435-560).
- Rust `SchemaBuilder::…index(…)` (`crates/uni/src/api/schema.rs` ~668,
  `apply` ~440-470).
- Python `create_index_definition` (`bindings/uni-db/src/core.rs` ~810).

## 7. Compatibility and upgrade

- **Older catalogs:** a scalar index on a keyable type without `key` (declared
  before this change; recorded `Stale` or `Failed` by #356's interim fix) is
  resolved at its next `ensure_declared_indexes` / rebuild: `key` is set,
  columns are backfilled and indexed, status becomes `Online`.
- **Downgrade:** an older binary ignores `key`, sees a BTree on the nested
  property, fails to build it (as before) and ignores the `__key_` columns,
  which it does not have in its catalog view. Data is unaffected.
- **#356's interim refusal** narrows to the `Refuse` cases of §5.6.
- **Content UIDs** of rows on labels with a JSON full-text index change once,
  at their next rewrite, to the value a fresh write computes (§4.2). Promote and
  diff already fall back to a content scan for UID misses; the change makes them
  match where they did not.

## 8. Work packages

Each package lands with its tests passing, clippy clean, and the docs it
touches updated. Packages run in sequence in one checkout.

### P1 — Framework and DATETIME (largest)

1. `T1`: regression test for the `__jfts_` UID drift on compaction and on SET
   of a flushed row (fails first). Then §4.2 (strip derived columns).
2. Catalog types and helpers (§3.1, §3.2), with serde round-trip and
   `same_config_as` unit tests; old JSON catalogs deserialize unchanged.
3. `index_keys.rs` with `DateTimeInstant` (§3.3) and unit tests.
4. `derived_rows` (§4.1) replacing `json_fts_derived_rows`; JSON full-text
   tests unchanged.
5. Create / backfill / list / match / drop / rebuild / fork widening / hiding
   (§4.3-§4.6).
6. Constant folding, key-aware analysis, rendering, guards, EXPLAIN
   (§5.1-§5.5) for DATETIME.
7. `resolve_scalar_index` on all four declaration paths (§6); #356's tests
   updated: DATETIME now builds `Online`.
8. Tests `T2`-`T8` (§9).

### P2 — BTIC

`BticBounds` derivation, the bounds table (§5.4) with both argument
positions, the `btic_lo`/`btic_hi` rewritten form, `contains_point`
conversions. Tests `T9`-`T10`.

### P3 — TIME and MAP paths

`TimeUtc` (probe offset-NULL rows first), `MapPath` resolution in Cypher DDL,
the procedure and the builders, typed coercion. Tests `T11`-`T12`.

### P4 — POINT and refusals

`PointCoords`, the bbox and distance bounds (antimeridian, poles), §5.6
refusals and messages. Tests `T13`-`T14`.

### P5 — Review and verification

An independent review of the full diff; workspace nextest, the failpoint lane,
both TCKs, Python (pytest, stubtest), the rustdoc gate, static guards; docs
(Black Book index chapter, `website/docs/concepts/indexing.md`,
`features/schema-indexing.md`, `guides/schema-design.md`), release notes, and
an AGENTS.md invariant for key columns.

## 9. Tests

All integration tests go in `crates/uni/tests/common/index/index_keys/`
(a `mod` under `index/mod.rs`), never a new top-level test binary.

The core harness, `differential(db_indexed, db_plain, queries)`, opens two
databases with the same writes, one with the index and one without, runs each
query against both, and asserts equal answers. It runs every query at four
stages: unflushed; flushed; after updates and deletes that are not yet
flushed; after those are flushed. Each query also runs with its constant as a
literal and as a parameter. EXPLAIN on the indexed side must show the key
used; PROFILE's `index_scans` must be non-zero after a flush.

| Id | What it proves |
|---|---|
| T1 | A row's stored `_uid` on a JSON-full-text-indexed label is unchanged by compaction and by SET of another property on a flushed row, and equals a fresh write's UID |
| T2 | DATETIME differential: same instant at different offsets and zones, 1 ns apart, offset-NULL rows, NULL, `IN` vs `=`, `<>` (not rewritten), cross-type literals (not rewritten, answers equal) |
| T3 | DATETIME index lifecycle: declared before data and after data (backfill), drop (columns unregistered), rebuild, reopen, upgrade of a #356-era `Failed` entry to `Online` |
| T4 | Key columns hidden: `RETURN n`, `properties(n)`, `get_label_info`, SHOW, Python |
| T5 | Forks: a fork created before the index reads and writes the label; no key pushdown on the fork; promote and diff unaffected |
| T6 | Bulk load and compaction fill key columns; answers equal after each |
| T7 | Statement clock: `datetime()` folds once per statement; `.realtime()` does not fold |
| T8 | `index_operand_fits_column`: a string literal against a DATETIME property is never pushed |
| T9 | BTIC differential for every predicate in §5.4, both argument positions, unbounded and half-unbounded intervals, granularity and certainty variants, `=` vs `btic_equals` |
| T10 | `btic_contains_point` with Int, datetime (pre-epoch sub-millisecond), date points |
| T11 | TIME differential: offsets crossing midnight (`23:00-05:00` vs `04:00Z`), `+14:00`, offset-NULL rows |
| T12 | MAP path: typed values, missing keys, two paths on one map, `n.m.k` index declared through all four surfaces |
| T13 | POINT: cartesian and WGS-84 bbox and distance, antimeridian wrap, near the poles, −0.0, wrong-CRS constants (same error with and without the index) |
| T14 | Refusals: LIST, VECTOR, DURATION, JSON, whole MAP, each naming the right index kind; existing DURATION/JSON BTrees still open |

Unit tests: `index_keys.rs` derivation per kind; bounds functions per
predicate (property-based where cheap: random intervals and points checked
against the exact predicate, asserting the bound admits every match).

## 10. Risks

| Risk | Mitigation |
|---|---|
| A bound drops a match (wrong operator at an edge, sentinel, −0.0, antimeridian) | the differential harness over boundary data; property tests on bounds; the residual never removed |
| Backfill on a large label rewrites the table | the same cost as a JSON full-text index today; documented; runs under the table lock like `backfill_json_fts_column` |
| Key columns appear in a fork branch created before them | branches are widened with `AllNulls`; key pushdown off on forks |
| UID change for existing `__jfts_` rows | one-time; makes stored UIDs match fresh writes; promote/diff already tolerate misses |
| Constant folding evaluates a side-effecting or clock call wrongly | only the listed constructors; statement-clock rule reused; `.realtime()` excluded; T7 |
| A plain index path mistakes a keyed index for a flat one | analyzer skips keyed configs for plain equality/range; T2 asserts the rendered SQL names the key column |
| Older binary opens a store with keyed indexes | ignores `key` and `__key_` columns; documented in release notes |

## 11. Effort

| Package | Estimate |
|---|---|
| P1 framework + DATETIME | 2–3 days |
| P2 BTIC | 1 day |
| P3 TIME + MAP paths | 1 day |
| P4 POINT + refusals | 1 day |
| P5 review + verification | 0.5–1 day |
