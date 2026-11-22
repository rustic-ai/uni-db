# Documentation review against the code (2026-11-22, uni-db 6.0.0)

Eight reviews checked `website/docs/`, `skills/uni-db/` and `docs/UNI_BLACK_BOOK.md`
against the code at `f7b99b71`. Each finding names the doc location and the code that
contradicts it. Probes ran against the Python bindings built from this tree. About 900
doc examples and probe statements were run.

The storage, object-store, single-writer, hybrid-WAL and read-cut text written with the
6.0.0 work is largely accurate. Most findings are in older material.

Severity: **wrong** (states false behaviour or an API that does not exist), **stale**
(was true), **missing** (current behaviour a reader would look for), **inconsistent**
(two docs disagree), **minor**.

## Fix decisions

Every defect in section 1 was traced to its root cause before fixing. C21 was not a code defect: algorithm
id outputs are integers on purpose (a test pins it), so only the declared yield type changes.
One more defect was found while tracing, recorded as C35 below. Decisions on the defects that
change a contract:

- **C1 tagged fork drop:** refused while the fork has tags. The error names them; untag first.
  Recovery leaves a fork Tombstoned when a branch delete fails rather than orphaning branches.
- **C3 DDL `DEFAULT`:** implemented for new writes; existing rows are not backfilled.
- **C11 inverted indexes:** used by the planner as a candidate prefilter for
  `ANY(x IN n.p WHERE x IN …)`, with `CREATE INVERTED INDEX` DDL.
- **C12 full-text query syntax:** phrases, `NOT`/`-term`, `AND`/`OR`, in flushed and unflushed data alike.
- **C16 `DROP LABEL` / `DROP EDGE TYPE`:** refused while data exists. Neo4j has no `DROP LABEL`
  (labels disappear with their last node); the user removes the label or deletes the data first.
- **C23 CALIBRATE:** a model's FEATURES expressions supply its INPUT values by position, as at a
  call site.
- **C24 JSON full-text:** a derived text column per JSON full-text index, filled at flush and searched by
  `uni.fts.query` and `CONTAINS` prefiltering.
- **Fork diff pairing:** vertices sharing `(label, ext_id)` pair into `changed`; when one side is the
  other's ancestor, vertices and edges also pair by id.

Defaults taken where the choice was minor:
- C9 keeps one NEW node per row and stores its properties.
- C26 gives Locy `$session.<key>` as Cypher has it.
- C20 keeps Python `fork_info` returning `None` and removes the always-zero
  `vertices_skipped_no_uid`.
- C28 flushes the fork before tagging.
- C29 resolves UID-index misses by a primary scan.
- C30 rejects unknown import names.
- C13 reports an out-of-range BTIC bound as an error.

### Status

| Defects | Status |
|---|---|
| C6, C7, C8, C30 | Fixed (OGM nullable and sparse vectors, edges on pending nodes, `uni import` name) |
| C21 | Not a defect: ids are intended; declared yield type corrected |
| C20 `fork_info` | Kept as documented (Python returns `None`) |
| All others | Open |

## 1. Code defects found (the docs describe the intended behaviour)

These are not doc fixes. The docs were left describing the intended behaviour; each needs
a code fix or a deliberate decision to change the documented contract. Items marked ✓
were reproduced a second time independently.

| # | Defect | Evidence |
|---|---|---|
| C1 ✓ | `drop_fork` on a fork tagged with `tag_fork` fails at stage `delete_branch` and leaves it Tombstoned. Docs promise tag-then-drop is safe for audit retention. After any successful drop, `list_fork_tags` / `untag_fork` raise `ForkNotFound` (they read the registry). No test covers tag-then-drop. | `crates/uni/src/api/fork_admin.rs:229-256, 829-830`; `crates/uni/tests/common/fork/fork_tag.rs` |
| C2 ✓ | Date and datetime subtraction return a negated duration: `date('2020-01-02') - date('2020-01-01')` is `P-1D`, datetime gives `PT-24H`. `duration.between` is correct. | probe |
| C3 ✓ | `DEFAULT` in `CREATE LABEL` DDL is parsed and dropped; new rows read NULL. | `cypher.pest:958`; `PropertyMeta` has no default (`crates/uni-common/src/core/schema.rs:832`) |
| C4 ✓ | A sparse vector written as a map literal (`{indices, values}`) reads back as `SparseVector` but is never found by `uni.sparse.query` (or the sparse arm of `uni.search`). The same value passed as a `SparseVector` parameter is found. | probe; `guides/sparse-vectors.md:90-95` example returns 0 rows |
| C5 ✓ | Locy `USE module` can never resolve: every host passes an empty module map, so `resolve_use` always returns `ModuleNotFound`, even for modules registered with `rules().register`. The TCK tests parsing only. | `crates/uni/src/api/session.rs:851-853`; `impl_locy.rs:204,369,483,681`; `crates/uni-locy/src/compiler/modules.rs:40-65`; `ModuleComposition.feature:4-5` |
| C6 ✓ | OGM `Vector[N] \| None` fields are declared NOT NULL (`lb.vector` regardless of nullability); commit fails with "cannot be null". | `bindings/uni-pydantic/src/uni_pydantic/schema.py:304-306` |
| C7 | OGM `SparseVector[N]` fields cannot be saved (`python_to_db_value` has no case). | `uni_pydantic/types.py:474-498` |
| C8 | OGM `tx.create_edge` on nodes `tx.add`-ed in the same transaction raises `NotPersisted`. | `uni_pydantic/session.py:145-158` |
| C9 | Locy `DERIVE (NEW x:L {props})` drops the properties (only `_skolem_id` is kept); edge properties work. One NEW node is made per matched row. | `crates/uni-query/src/query/df_graph/locy_ast_builder.rs:283-292` |
| C10 | Locy `REQUIRE` direction uses only `default_direction_oracle`; the registry directions on `MaxAgg`/`MinAgg`/`CountAgg` are never consulted, so `REQUIRE` over `MAX` is rejected. | `crates/uni-locy/src/compiler/typecheck.rs:238` |
| C11 | The planner never constructs `LogicalPlan::InvertedIndexLookup`; `StorageManager::inverted_index()` has no non-test caller; the DataFusion planner errors on the node. `CREATE FULLTEXT INDEX ... OPTIONS {normalize, max_terms_per_doc}` builds a FULLTEXT index and ignores the options. | `planner.rs:15058+`; `manager.rs:5332`; `df_planner.rs:1673`; `ddl_procedures.rs:459` |
| C12 | Full-text `uni.fts.query`: the default operator is OR (✓); `NOT` and phrase queries have no effect. | probe |
| C13 | `btic_lo` / `btic_hi` panic (multiply overflow) for any bound before ~1677. | `crates/uni-query-functions/src/expr_eval.rs:2799` |
| C14 | `pow` / `power` fail on integer arguments (DataFusion type error); `uni.bitwise.shiftLeft` / `shiftRight` cannot be called (lookup lower-cases the name). | `crates/uni-query-functions/src/df_udfs.rs:1779-1786` |
| C15 | `created_at()` / `updated_at()` return NULL for schemaless labels and for edges; on insert `updated_at` is earlier than `created_at`. | probe |
| C16 | `DROP LABEL` succeeds while the label has vertices, and they stay matchable. | probe |
| C17 | The 100-hop default bound warning is not raised for schemaless edge types. | probe |
| C18 | A window function in `WITH ... WHERE` fails with "No field named ROW_NUMBER() OVER (...)"; the same window in `RETURN` works. | probe |
| C19 | `SHOW INDEXES` lists JSON full-text and sparse indexes as name and type `UNKNOWN`; `uni.schema.constraints()` always yields `target = NULL`; `DROP INDEX IF EXISTS missing` still errors. | probe |
| C20 | Promote on a read-only handle raises `UniInternalError` instead of `ReadOnly`; Python `fork_info("missing")` returns `None` where Rust returns `ForkNotFound`; Python `PromoteReport.vertices_skipped_no_uid` is always 0. | `bindings/uni-db/src/types.rs:4907,4950` |
| C21 | `uni.algo.bridges` / `articulationPoints` / `mst` / `dijkstra` declare Node outputs but return integer ids. | probe |
| C22 | `similar_to` returns NULL on a `SparseVector` column and errors on `LIST<VECTOR>`. | probe |

Found while fixing the docs (reported once, not reproduced a second time unless marked):

| # | Defect | Evidence |
|---|---|---|
| C23 | CALIBRATE turns any non-scalar INPUT value (a node) into `FeatureValue::Null`, so the classifier receives `{'a': None}` rows; with such inputs it reported `raw_brier = 0.0` and `raw_ece = 0.0` on mixed targets. | `locy_calibrate.rs` `row_to_feature` |
| C24 | JSON full-text indexes are built but never read: `PushdownStrategy::json_fts_predicates` is used only in tests, and `uni.fts.query` on a JSON column fails ("must resolve to Utf8 … got LargeBinary"). | `crates/uni-query-functions/src/pushdown.rs:146` |
| C25 | A window function anywhere in `WITH` fails ("No field named ROW_NUMBER() OVER (...)"), not only with a following `WHERE` (extends C18). | probe |
| C26 | Locy does not see session params (`LocyBuilder::run` never merges them). | `locy_builder.rs:219-247` |
| C27 | `uni.schema.createIndex(..., {type:'VECTOR'})` succeeds on a `VECTOR` property declared without dimensions (0). | probe |
| C28 | `list_fork_tags` returned `[]` right after `tag_fork` on a fork with no flushed data; after a flush it listed the tag. | probe |
| C29 | `promote_from_fork_with_options(upsert=True)` right after a plain promote of two vertices without `ext_id` reported `vertices_inserted=2` rather than skipping them, which suggests duplicate inserts. Not reproduced. | probe |
| C30 | `uni import <name>` ignores the dataset name and always runs the Semantic Scholar importer. | `crates/uni-cli/src/main.rs` |
| C31 | `MCOUNT(*)` does not parse; `MCOUNT()` does. | probe |
| C32 | `examples/example-pyo3-geo` pins `pyo3 = "0.27"` while `uni-plugin-pyo3` uses 0.29, so the example does not resolve (two crates link `python`) and the `cargo run` in `plugins/loaders/pyo3.md` cannot work. | `examples/example-pyo3-geo/Cargo.toml` |
| C33 | Because of C23, the CALIBRATE-fitted calibrators in the drug-drug-interaction and predictive-maintenance notebooks come out inverted, and calibrated Brier scores are worse than raw. Those notebooks now rank on the raw classifier score; restore the calibrated ranking once C23 is fixed. | notebook runs |
| C34 | A WASM or Extism load with an empty grant list reports the missing `ScalarFn` as `UniInternalError`, not a typed capability error. `rules().remove(name)` while another rule depends on it fails with `undefined rule: name` without naming the dependent. | probe |
| C35 | Fork tags are named `fork_{tag}_{dataset}` with no fork id, but Lance tags are dataset-wide: two forks tagging `audit` collide, `list_fork_tags` lists another fork's tags, and `untag_fork` can delete them. | `crates/uni/src/api/fork_admin.rs:734,755,809,859-864` |

Review findings the code showed to be wrong (the docs were left as they were):
`uni.schema.createIndex(..., {type:'BITMAP'})` builds a real Lance Bitmap index (only Cypher
`CREATE INDEX` always builds a BTree); `id()` returns a signed Int64 (the skill and Black Book
`UInt64` were the wrong side); a rebuilding index is `Building`/`Stale`/`Failed` and queries fall
back to a scan (the Black Book "old Online keeps serving" was the wrong side); `ProvidedSurfaces`
has `storage_backends` and no `connectors`; `uni.search` does yield `vector_score` and `fts_score`;
`rerank_score` is null without a reranker on vector and FTS queries.

Undecided whether code or doc: fork diff does not pair changed rows (`changed` is always
empty; a property edit is one delete plus one add). For edges the code says this is
intentional (`crates/uni-fork/src/diff.rs:342-349`). The docs were changed to describe
the current behaviour.

## 2. Storage, durability, configuration

- **wrong** Compaction defaults documented as `max_l1_runs` 4 and `check_interval` 30 s; code is 8 and 10 s (`crates/uni-common/src/config.rs:58,61`). `guides/performance-tuning.md:364,368,385,388,394,397`; `internals/storage-engine.md:1080`; Black Book 345, 1537, 2499, 8035, 8038, 9093, 9096.
- **wrong** `flush_drain_timeout` documented as 120 s; code 900 s (`config.rs:906`). `reference/configuration.md:224`; Black Book 2033, 9085.
- **wrong** Env var `UNI_FLUSH_STREAM_TIMEOUT_SECS`; code reads `UNI_FLUSH_STREAM_TIMEOUT` (`config.rs:901`). `reference/configuration.md:20,155,511`.
- **wrong** Black Book on-disk tree (1251-1308): `schema.json` and `wal/` are under `R/`, not `R/storage/`; tables have the `.lance` suffix; `LanceDbStore::vertex_table_name` is now free functions in `crates/uni-store/src/backend/table_names.rs`; inverted index path is `indexes/{label}/{property}_inverted` (`storage/index_manager.rs:431`). The same chapter at 1337-1343 is right.
- **wrong** `concepts/architecture.md:352-375` dataset layout (`storage/schema.json`, `snapshots/manifest_v42.json`, `edges_CITES/`, `adj_out_CITES_Paper/`, `delta_CITES_out/`, `indexes/vector_paper_embedding/`). Real: `R/catalog/schema.json`, `storage/catalog/manifests/<uuid>.json`, main `edges` plus `deltas_CITES_{fwd,bwd}`, `adjacency_CITES_fwd`.
- **wrong** `internals/storage-engine.md:390-510` dataset trees: `storage/vertices_Paper.lance`, main `edges` plus per-type deltas, `adjacency_CITES_fwd`; edge columns `src_vid`/`dst_vid` (`storage/delta.rs:123`, `storage/main_edge.rs:187`).
- **wrong** `concepts/concurrency.md`:
  - `SnapshotManifest` fields at 181-188 and the JSON at 264-290. Real fields: `created_at`, `vertices`, `edges`, `version_high_water_mark`, `wal_high_water_mark`, `tables`, `next_vid`/`next_eid`, … (`crates/uni-common/src/core/snapshot.rs:9-50`).
  - Flush diagram names at 253-256.
  - The `ExecutionContext { l0_buffer }` passage at 220-226, and "Read Uncommitted = With L0 access" at 318 (L0 holds committed data).
  - Non-existent APIs: `tx.query_scalar`, `tx.execute(cypher, params)` (one argument, `transaction.rs:659`), `|tx| async move {}` (needs `Pin<Box<…>>`, `session.rs:943`), `snapshot_manager.current()`, `executor.execute(q,&snapshot)`, `writer.insert_batch`.
  - The primitives table at 365-369: L0 is `Arc<RwLock<L0Buffer>>`, `IdAllocator` is a Mutex plus atomics, `SnapshotManager` is `Arc`.
- **wrong** Tier-1 CSR compaction trigger `frozen_segments >= 4`; default is 2 and it runs as a background task (`config.rs:63`, `writer.rs:8044`). `internals/storage-engine.md:285`; Black Book 8000.
- **wrong** `auto_flush_min_mutations` default 100; code 1 (`config.rs:853`). Black Book 2006 (its 9082 says 1). Black Book 2020 `UniConfig.async_flush` is `async_flush_enabled`.
- **wrong** `Uni::compact` does not exist; `configuration.md:325,341`. The API is `db.compaction().compact(name)` (`crates/uni/src/api/compaction.rs:22`). `performance-tuning.md:418-419` uses `db.compact_label` / `db.compact_edge_type`, which are only on `StorageManager`.
- **wrong** `BACKUP '/path'` and `COPY (Label) TO/FROM '/path' FORMAT csv`; grammar is `BACKUP TO <string> (WITH map)?` and `COPY ident TO|FROM <string> (WITH map)?` (`cypher.pest:931-934`). `skills/uni-db/references/cypher.md:518`.
- **stale** "BulkWriter bypasses isolation; already-flushed batches cannot be rolled back". Bulk writes are staged and `abort()` discards everything (`crates/uni-bulk/src/bulk.rs:2763-2778`). `skills/uni-db/references/rust-api.md:367`; `skills/uni-db/SKILL.md:450`.
- **wrong** `reference/troubleshooting.md:610-613` storage inspection paths: tables are `<db>/storage/vertices.lance` etc.; there is no `adjacency/` directory.
- **wrong** `internals/storage-engine.md:939` lists a "Memory" URI backend; `memory://` is rejected; `in_memory()` is a temporary directory.
- **inconsistent** `guides/data-ingestion.md:395-400` says a concurrent reader can see a load in some tables; lines 326-336 (and the read-cut design) say readers never see part of a load.
- **stale** Property cache "default 10,000 entries" (`concepts/architecture.md:297`); capacity is `cache_size / 1024`.
- **stale** L0 "Capacity: default 128MB" (`internals/storage-engine.md:108`); no such setting.
- **stale** ByRunCount "L1 delta tables with data ≥ threshold" (`performance-tuning.md:368`): it counts flush generations (`manager.rs:3590-3604`). Black Book 9095 marks `max_l1_age` "(planned)"; ByAge is implemented (`manager.rs:3609`).
- **stale** Black Book 8058: remote stores have no compaction lock; Black Book 1361 says the writer lease admits one compacting handle.
- **minor** `internals/benchmarks.md:33` Criterion 0.5; workspace uses 0.8.
- **inconsistent** Crashed writer blocks ~35 s (`expires_at + clock_skew < now`, `lease.rs:588`); `features/storage-cloud.md:159` and `reference/configuration.md:662` say 30 s.
- **inconsistent** `configuration.md:605` "the first ID reservation fails" on a store without conditional writes; with the default lease the open is refused first (`LeaseUnsupported`, `lease.rs:643`).
- **minor** Black Book 7656-7667 manifest fields lack `tables`, `next_vid`/`next_eid`, `writer_features`, `unpublished_bulk_versions`.
- **missing** Black Book UniConfig reference (8805-9200) lacks `writer_lease`, `read_cut_refresh`, `commit_timeout`, `ssi_enabled`, `async_flush_enabled`, `max_pending_flushes`.
- **missing** `configuration.md:673-686` BACKUP: no `WITH {…}` destination options; inherited credentials are dropped whenever the destination resolves a different endpoint (`storage/manager.rs:1280-1310`).
- **minor** `concepts/concurrency.md:107-109` does not say one writable handle per store is enforced (`StoreLocked`/`WriterFenced`).

## 3. Python API and OGM

- **wrong** `async with uni_db.AsyncUni.open(path) as db:` fails (open is a coroutine). `reference/python-api.md:65,1892`; `plugins/reference.md:74`. Use `db = await AsyncUni.open(p)` then `async with db:`.
- **wrong** Session params are read as `$session.<key>`, not `$<key>` (`crates/uni/src/api/session.rs:1601-1617`). `getting-started/programming-guide.md:1050,1056,1063,1080-1086`; `skills/uni-db/SKILL.md:217,278`; Black Book 8233-8237; `guides/cypher-querying.md` ~1120-1150 (also uses a non-existent `session().set(...).build()`; `set` is on `params()`); `skills/uni-db/references/cypher.md:281`. Locy does not see session params at all (`locy_builder.rs:219-247` never merges them); `skills/uni-db/references/rust-api.md:742-748` reads `$threshold` in Locy.
- **wrong** Tags "survive the drop" and `list_fork_tags` after `drop_fork` (`reference/python-api.md:941-955`); see C1.
- **wrong** `programming-guide.md:1124-1130` subscripts `ExplainOutput`/`ProfileOutput`; they use attributes.
- **wrong** `programming-guide.md:1327-1336` catches `RuntimeError`/`ValueError`/`OSError`; errors are `UniError` subclasses of `Exception`.
- **wrong** `getting-started/quickstart.md:299` `json.dumps(explain_cmd.tree)` fails (Node objects).
- **wrong** "Python 3.9+" (`quickstart.md:10`); `requires-python = ">=3.10"`.
- **wrong** `skills/uni-db/references/python-api.md`: `add_hook(name, hook)` (`:177`), `SessionTemplateBuilder.hook(name, hook)` (`:549`), `vector_similarity` not `vector.cosine_similarity` (`:689`), Datalog-syntax Locy (`:731-735`), AsyncSession has no `register_function`/`explain`/`profile` (`:188`), `Vid` supports equality and hashing, not ordering (`:576`).
- **wrong** `skills/uni-db/SKILL.md:339` `result["reachable"]`; use `result.derived["reachable"]` or `result.rows()`.
- **wrong** Black Book 8783 `await session.add(alice)`; async OGM `add` is not awaitable.
- **wrong** Stub docstrings: `__init__.pyi:1892` `session.set(...)`, `__init__.pyi:2312` `session.execute(...)`; `uni_pydantic/__init__.py` `Uni("./my_graph")`.
- **minor** `session.id` is a method (`python-api.md:449`).
- **stale** `python-api.md:107,127` context-manager exit and `shutdown()` described as flush only.
- **minor** `db.write_lease()` returns `None` unless set (`python-api.md:140,185`); `WriteLease.DYNAMODB` unmentioned in python-api.md.
- **inconsistent** `derived_fact_set` can be `None`; `approximate_groups` is a dict (`python-api.md:1071-1072,1041-1047` vs `locy/api/python.md:121`).
- **minor** Promote gate (`python-api.md:981`): only a type the fork declared is refused; schemaless types are accepted.
- **missing** Builder tables omit `max_memory`, `cancellation_token`, `profile`, `params`, `allow_stale`, `skip_before_triggers`, StreamingAppender `write_batch`/`abort`/`buffered_count`, UniBuilder fork options and `skip_invalid_locy_rules`, Uni `bulk_load_applied`/`uri`/`background_task_count`.
- **missing** About 20 exception classes are undocumented in `python-api.md:1777-1876` and `skills/uni-db/references/python-api.md:619-661` (`UniUncommittedWritesError`, `UniConstraintConflictError`, `UniLockTimeoutError`, `UniWriteStalledError`, `UniParameterMissingError`, `UniEdgeEndpointError`, `UniEdgeIdentityError`, `UniLocyIncompleteError`, `UniRestoreBlockedByForksError`, the `UniFork*Error`s, `UniBulkCommit*Error`).
- **minor** Skill result types lack BulkStats `load_id`/`warnings`, ExecuteResult `properties_removed`, DataType `BTIC`/`BYTES`/`sparse_vector`, LocyConfig `allow_partial`/`goal_directed`/`semiring`/`returned_rules`, CommitNotification `rules_promoted`/`causal_version`/`dropped_before`.
- **inconsistent** Black Book maps Python `datetime` to `Timestamp`; the OGM maps it to `datetime`.
- **minor** Black Book schema (8193-8205) declares NOT NULL `KNOWS.weight` and `embedding` that the bulk example (8266) omits; `python-api.md:660` has the same `.vector()` NOT NULL trap.
- **minor** `python-api.md:1187` lists `rerank()` while `:1118` says Xervo has none; `temporary()` is an on-disk scratch dir, not in-memory (`:96`); rows are `Row` objects (`:1488`); `reference/pydantic-ogm.md:15` says `poetry install` (hatchling and uv); `reference/pydantic-ogm.md:497` and `guides/pydantic-ogm.md:243-249` call `update(status=...)` on a model without `status`.

## 4. Rust API, CLI, workspace

- **wrong** The CLI has no `--version`/`-V` (`crates/uni-cli/src/main.rs:22-24`); `getting-started/cli-reference.md:16`, `installation.md:163-191` (also usage `uni [OPTIONS] [COMMAND]`, `query` is "Run a query", global `--storage-option`).
- **wrong** `for row in &results` does not compile (only `impl IntoIterator for QueryResult`, `crates/uni-query/src/types.rs:451`); `programming-guide.md:498,513,1415,1436,1454`.
- **wrong** `EdgeData` fields are `src_vid`/`dst_vid` (`programming-guide.md:987`).
- **wrong** `ExecuteResult` counters are methods (`reference/rust-api.md:1453-1456`).
- **wrong** `QueryMetrics` has no `duration` (`reference/rust-api.md:913,1441`); fields are `parse_time`, `plan_time`, `exec_time`, `total_time`.
- **wrong** `Row::get_by_index` → `get_idx`/`values`/`value` (`reference/rust-api.md:1483`).
- **wrong** `QueryCursor::next` → `next_batch`/`collect_remaining` (`reference/rust-api.md:1495`).
- **wrong** `SnapshotManifest.id` → `snapshot_id` (`reference/rust-api.md:1599`).
- **wrong** `CompiledProgram` has no `num_strata`/`num_rules`/`rule_names` (`locy/api/rust.md:155-156`).
- **wrong** `ExecuteBuilderSync` has no `profile` (`reference/rust-api.md:1300`).
- **wrong** `RuleRegistry::register`/`remove`/`clear` are async; `clear` returns `Result<()>` (skill `rust-api.md:410,414`; `reference/rust-api.md:1661`).
- **wrong** `StreamingAppender::abort` is `async fn abort(self) -> Result<()>` (`reference/rust-api.md:843`; skill `rust-api.md:382`).
- **inconsistent** `BulkWriter::insert_edges` returns `Result<Vec<Eid>>` (skill `rust-api.md:360`).
- **wrong** `DataType` sketch `Boolean`/`Json` (`reference/rust-api.md:632-644`, `reference/index.md:72-81`); variants are `Bool`, `CypherValue`, plus `Int32`, `Float32`, `Timestamp`, `Time`, `Point`, `SparseVector`, `BinaryVector`, `Btic`, `Crdt`, `List`, `Map`.
- **wrong** `CrdtKind` → `CrdtType` (`skills/uni-db/SKILL.md:173`).
- **wrong** `Value::Node/Edge/Path` are tuple variants (`reference/rust-api.md:1517-1519`).
- **wrong** git URL `rustic-ai/uni` → `rustic-ai/uni-db` (`programming-guide.md:15`).
- **wrong** Black Book 10125 `uni import semantic-scholar` omits the required `--citations`; Black Book Appendix A "multi-line query input" is not supported.
- **stale** Versions: `installation.md:27` `uni-db = "3"`, `installation.md:165` "uni 2.0.0", `reference/rust-api.md:51` "Uni 2.0.0".
- **stale** Black Book 293 Lance "Pinned at 13.0.0"; it is 14.0.2.
- **stale** Black Book workspace tree (226-253) lists 22 crates; there are 29 (missing `uni-btic`, `uni-bulk`, `uni-fork`, `uni-locy-oracle`, `uni-query-functions`, `uni-sidecar`, `uni-sparse-vector`) plus five wheel-variant crates; `uni-cli` also has `plugin`. AGENTS.md "~27 crates".
- **stale** Black Book crate dependency graph (257-287) and layer header (204).
- **stale** Session hook methods are deprecated (`session.rs:1281-1312`) but shown as current: `reference/rust-api.md:299-306,1184-1185,1280`; skill `rust-api.md:168-170`.
- **stale** Enum sketches: `IndexType` lacks `FullTextWithAnalyzer`, `Sparse`; `VectorMetric` lacks `L1`; skill `VectorAlgo` lacks `Muvera`; skill `SchemaChange` lacks `description` fields.
- **missing** MSRV `rust-version = "1.96"` (`Cargo.toml:94`); feature flags `apoc-core`, `rhai-plugins`, `extism-plugins`, `wasm-plugins`, `pyo3-plugins`, `failpoints` (`installation.md:275-315`).
- **missing** `reference/rust-api.md` lacks `Session::run`, `scratch`, `transact_with_retry`, `execute_with_retry`, `fork`, `fork_schema`, `flush`; `Uni` fork admin, `shutdown_in_place`, `xervo`, `uri`, plugin loaders; `UniBuilder::plugin_trust`, `skip_invalid_locy_rules`, `build_sync`; `ApplyBuilder::allow_stale` (fresh is the default since 2.0.7).
- **minor** CLI `--storage-option` is global; `plugin install` refuses `http(s)://`.
- **minor** `LocyBuilder` tables omit `allow_partial`, `max_memory`, `profile`; `SessionSync::rules()` returns `RuleRegistrySync`; `LocyResult::rows()` is `Option<&Vec<FactRow>>` (`reference/rust-api.md:868-869`); orphaned `build()` row (`:178`); Black Book overview counts (L1 metric; "40+" vs 42).

## 5. Locy

- **wrong** `USE` imports (see C5) are documented as working: `locy/language-guide.md:239-245`, `locy/advanced/modules-priority-query.md:3-17`, `locy/reference/syntax-cheatsheet.md:107-113`, `skills/uni-db/references/locy.md:21-23,§13`, Black Book 6258-6270 and 6889-6903. Docs now state that `MODULE` inside one program and fully qualified `QUERY a.b.rule` work, and `USE` is not wired yet.
- **wrong** Examples using `prev.` without a base clause fail with `PrevInBaseCase`: `along-fold-bestby.md:8-11,283-287,305-308,314-318`; Black Book `shortest_risk` (~6325), `best_route` (~6640), `converging_score` (~6680), `risk_chain` (~6955).
- **wrong** `DERIVE MERGE a, b` emits a `MERGED_WITH` edge, it does not combine nodes (`locy_derive.rs:429-440`): `derive-assume-abduce.md:22-26`, Black Book ~6670, skill.
- **wrong** NEW node properties (see C9): skill DERIVE section and supply-chain example; `syntax-cheatsheet.md:17`.
- **inconsistent** `language-guide.md:221-225` says ABDUCE / EXPLAIN RULE inside ASSUME are compile errors; they run (as `derive-assume-abduce.md:101-118` says).
- **wrong** Dotted access `failure_risk.risk` does not compile; use the bare column (`language-guide.md:138`, `probabilistic-logic.md:77`).
- **wrong** `quickstart.md:15-16,62-64` never creates node C.
- **wrong** Black Book `total_exposure` (unbound `t`), `risk_summary` (aggregate in YIELD), RBAC `inherited_permission` (unbound `u`), supply-chain `provenance` (aggregate in ALONG); `along-fold-bestby.md:174-180` `t.amount` over `[t:TRANSFER*]`.
- **wrong** Black Book PRIORITY (~6656-6664): PRIORITY picks, per KEY group, the clauses with the highest number within one rule; mixing is `MixedPriority` (`locy_priority.rs:6-32`). `modules-priority-query.md:21-26` does not say which number wins and its example appears to expect the opposite.
- **inconsistent** Black Book ABDUCE "minimal set of changes" (~6833); candidates are single edits.
- **wrong** Black Book `DERIVE ... RETURN` (~6815); DERIVE has no RETURN.
- **stale** Black Book issue #158 note (~6905): a negated KEY only needs to be bound.
- **wrong** `HavingInRecursivePath` is a compile warning (`compile_warnings`), not in `result.warnings` (`along-fold-bestby.md:253`, skill).
- **wrong** The recursive `blocked` example has no seed clause and "the graph above" is never shown (`along-fold-bestby.md:241-270`, skill, Black Book REQUIRE).
- **wrong** `locy/advanced/neural-predicates.md`: semiring is `{'top_k_proofs': k}`, not `"TopKProofs(8)"`; `neural_provenance_store` is `classifier_provenance_store` (`crates/uni-locy/src/config.rs:172`); `top_k_proofs` caps EXPLAIN provenance (`locy_fixpoint.rs:2948-2951`).
- **wrong** `ProbabilityDomainViolation` is a static hint for non-literal MNOR/MPROD arguments in recursive rules (`typecheck.rs:225-229,872-895`): `errors-limits.md:59`, `neural-predicates.md`, `probabilistic-logic.md:126,268`, `troubleshooting.md:47-51`.
- **wrong** Black Book REQUIRE says the registry-backed oracle mirrors `direction()` (see C10).
- **stale/missing** Black Book Locy config block (~6912): `timeout: Option<Duration>`; missing `allow_partial`, `semiring`, `returned_rules`, `neural_predicates_preview`, classifier fields. `errors-limits.md:21-36` missing `allow_partial`, `semiring` and warnings `MsumNonNegativity`, `HavingInRecursivePath`, `CountFoldSeedCounted`, `PositiveComplementCorrelation`, `CrossPredicateCorrelation`, runtime `EntityHydrationIncomplete`.
- **minor** Skill reserved keywords lack `MODEL`; there is no EXPORT concept; UNION works only between plain Cypher queries; `rule-semantics.md:30` lists `ASSUME ... WHERE`.

## 6. Cypher, data model, types, schema

- **wrong** `WITH RECURSIVE name(cols) AS` (skill `cypher.md:75`, Black Book ~3255); grammar has no column list (`cypher.pest:726`). `guides/cypher-querying.md` ~1700 ends with SQL `SELECT`.
- **wrong** `PROFILE` is not a Cypher keyword: skill `cypher.md:618-626`, `reference/glossary.md:202-203`, `guides/cypher-querying.md` feature table; `db.profile(...)` (guide ~1785) → `query_with(q).profile()` returning `(QueryResult, ProfileOutput)`.
- **wrong** Time travel `MATCH ... VERSION AS OF ... RETURN`; the clause follows the whole query (`cypher.pest:644`). Skill `cypher.md:589-599`, Black Book ~5945-5955.
- **wrong** Bitwise names `bitwise_and`, `shift_left` → `uni.bitwise.and/or/xor/not` (skill `cypher.md:430`, Black Book ~3735); shifts are C14.
- **wrong** `NOT IN`, `EXISTS(prop)`, `RETURN p.*` are unsupported (skill `cypher.md:254`; guide ~490, ~500, ~520). `NOT x IN [...]` works.
- **wrong** Window in `WITH` + `WHERE` (skill `cypher.md:683`, guide ~640); see C18.
- **stale** "Mixing manual and aggregate window functions is not supported" (`features/window-functions.md`, skill `cypher.md:464`); it works.
- **wrong** `pow(2, 3)`; see C14. `2^3` works.
- **wrong** `uni.schema.dropIndex`/`dropConstraint` take one argument (guide ~2290, `executor/procedure.rs:946-975`); `createEdgeType` types are STRING/INT/INT32/FLOAT/FLOAT32/BOOL/DATETIME/DATE/BTIC/VECTOR/LIST/MAP (`ddl_procedures.rs:652-668`).
- **wrong** `REQUIRE` constraint syntax (guide ~2075): only `ON ... ASSERT`; `ASSERT p.a IS NOT NULL` is a non-enforced CHECK; use `EXISTS(...)`.
- **wrong** DROP LABEL "must have no vertices" (guide ~2065); see C16.
- **wrong** EXPLAIN output samples (guide ~1735, `internals/query-planning.md:456-485`); real `EXPLAIN` returns a `plan` column with a debug dump; `query_with().explain()` returns `ExplainOutput`.
- **wrong** `concepts/crdt-types.md:485-514` VCRegister concurrent merge: deterministic tie-break on the larger serialized value (`vc_register.rs:76-100`); "Supported Variants" (`:544-551`) lacks `VectorClock`, `VCRegister`; "2P-Set" does not exist; DDL accepts only `GCOUNTER`, `LWWREGISTER` (`executor/write.rs:806-807`).
- **wrong** `internals/query-rewriting.md:101-102` `validAt`/`overlaps` use strict `>`.
- **wrong** `guides/temporal-intervals.md:59` BTIC datetime literal has second granularity.
- **wrong** Black Book ~3640-3660 positional temporal constructors fail (string and map forms only); `duration.inMonths/inDays/inSeconds` return Durations and take two arguments; `index(list, i)` is element-at-index.
- **wrong** `uni.admin.compactionStatus()` has no `l1_size_bytes` (skill `cypher.md:580`, guide ~2155); columns `l1_runs, l1_rows, l1_estimated_bytes, in_progress, pending, total_compactions, total_bytes_reclaimed, status_refreshes`. `SHOW INDEXES` has no status column.
- **wrong** `concepts/data-model.md`: no `Deprecated` state (`schema.rs:26`); Timestamp is nanosecond; no `Json` type (`JSON` maps to `CypherValue`); `SparseVector`, `BinaryVector`, `Btic` missing; VID/EID examples use the removed packed layout (`:38,140,395-397`); "strict schema" contradicts schemaless default; schemaless map properties are rejected.
- **stale** `concepts/identity.md:213-219` UID index path is `indexes/uni_id_to_vid/{label}/index.lance` with `_uid_hex`; `:202` VIDs are one global counter (`runtime/id_allocator.rs:24-27`); UniId example length.
- **stale** `reference/glossary.md:74,154-155,300-301` packed VID/EID layout; gryf (`:99-101,145,319`) replaced by `SimpleGraph`; `session.add_hook()` deprecated.
- **inconsistent** `id()` signed (`features/cypher-querying.md`) vs `UInt64` (skill, Black Book).
- **wrong** Skill `schema-indexing.md:60-90`: DDL `INTEGER`/`FLOAT` are Int64/Float64; `LIST<T>`/`MAP<K, V>`; Point `crs` is Utf8 (also Black Book ~640).
- **inconsistent** `guides/cypher-querying.md` ~960-1010 duplicated write-counter section; DETACH DELETE gives `properties_removed = 0`.
- **wrong** `features/data-model-multimodal.md` `.vector("embedding", 384)` is NOT NULL, so its next CREATE fails.
- **stale** `guides/schema-design.md:466-482`, Black Book 1618-1627 `json_get_*` overflow rewrite does not exist.
- **minor** Black Book ~1187 `CREATE EDGE TYPE` needs `FROM ... TO`; `internals/vectorized-execution.md:688-694` `batch_size` is the cursor page size, the engine batch is `execution_batch_size`.

## 7. Indexes, search, Xervo, NLP, neural predicates

- **wrong** Sparse map-literal example (`guides/sparse-vectors.md:90-95`, Black Book ~5040, skill `vector-hybrid-search.md:166`); see C4.
- **wrong** Inverted-index DDL and planner use (`concepts/indexing.md:546-551,612-634`, Black Book ~2700); see C11. Rust example (`concepts/indexing.md:559-562`) misses required fields.
- **wrong** Index options `WITH {...}` → `OPTIONS {...}` (skill `schema-indexing.md:260-280`, skill `vector-hybrid-search.md:433`, Black Book ~2648-2654); key is `partitions`, not `num_partitions`.
- **wrong** Default vector index is IVF-PQ, not HNSW-SQ (skill `schema-indexing.md:254,259`).
- **wrong** Sparse index via procedure is `{type:'SPARSE'}` (skill `vector-hybrid-search.md:132-134`).
- **wrong** YIELD `vector_score`/`fts_score` do not exist (skill `vector-hybrid-search.md:12,295`, Black Book 4709, 5117); columns `vid, distance, score, rerank_score` / `vid, score, rerank_score`.
- **wrong** `uni.fts.query` score is `bm25/(bm25+fts_k)`, not relative to the top match (skill `:308`, Black Book 5120).
- **wrong** Vector `threshold` is a minimum score (`search_procedures.rs:1620-1628`; `concepts/indexing.md:240-247`); the plugin argument doc (`procedures_plugin/vector.rs:62`) also says distance.
- **wrong** `CALIBRATE ... ON MATCH ... TARGET ... METHOD ... [HOLDOUT n]`, `VALIDATE rule ... METRICS ...` (`locy.pest:606-645`): `features/neural-predicates.md:13-14,119-120`, skill `neural-predicates.md:130,150`, `locy/advanced/neural-predicates.md:220`; metrics `brier_score`, `auc`.
- **wrong** CALIBRATE passes `{'a': None}` rows to the classifier, breaking `max(0.0, r["a"])` (`features/neural-predicates.md:37-39`); `holdout_size` is an integer count.
- **wrong** Xervo catalog values are snake_case (`embed_hybrid`, `rerank`, `eager`), Black Book 5370, 5470, 5509.
- **wrong** Env vars `VERTEX_AI_TOKEN`/`VERTEX_AI_PROJECT`, `VOYAGE_API_KEY` (skill `xervo.md:53,55`).
- **wrong** `xervo_catalog_from_file/str` are free functions (`crates/uni/src/lib.rs:110`), not builder methods (skill `xervo.md:197-207`).
- **wrong** Python `Xervo` has no `rerank` (skill `vector-hybrid-search.md:914-920`).
- **wrong** `embedding` config needs `alias` (`features/hybrid-search.md:368-372`).
- **wrong** FTS operators (`concepts/indexing.md:397-415`); see C12. `:386` "tokenizer configuration is not exposed via DDL" is stale (FULLTEXT OPTIONS accept `tokenizer`, `language`, `stemmer`, `stopwords`, …). Ngram default minimum is 3.
- **wrong** Black Book ~5126-5132 analyzer options: `stopwords` takes a bool or list; `stemmer` a bool.
- **wrong** JSON FTS examples (`concepts/indexing.md:436-477`): `_doc` is reserved; `CONTAINS` on JSON is case-sensitive substring; `uni.fts.query` needs Utf8.
- **stale** PQ `sub_vectors` default is dimension-aware (skill `vector-hybrid-search.md:433,467,540`).
- **stale** `db.rebuild_indexes` → `db.indexes().rebuild(label, background)` (`concepts/indexing.md:662,825`).
- **minor** `HASH` is not creatable via the procedure; `bitmap` builds a BTree (`concepts/indexing.md:307,326`, `features/schema-indexing.md:7`, skill `schema-indexing.md:224`).
- **inconsistent** `similar_to` returns raw cosine, not 0-1 (Black Book ~2667, 4744, skill `:264`).
- **missing** Fusion methods `dbsf`, `relative_score`/`rsf` (`features/hybrid-search.md:103`, skill `:350`); unknown methods fall back to RRF.
- **wrong** `rerank_score` equals `score` without a reranker; `uni.sparse.query` ignores `options.reranker` (`guides/sparse-vectors.md:140`, `concepts/indexing.md:294`, Black Book 5079, 5085).
- **wrong** Black Book `sparse_vector(30522)` (~5020) is not a function; MaxSim example (~5348) needs `reranker_property`; `EmbeddingCfg` snippet (~5384) uses derives it lacks.
- **inconsistent** Black Book index choice (~3040-3060 vs ~2620); `concepts/indexing.md:680` vs Black Book ~2750 on a non-Online index.
- **stale** Black Book rebuild triggers (~2546); `IndexRebuildConfig` has growth (0.5) and age only.
- **minor** L2 is squared (`concepts/indexing.md:50`); index storage layout (`:729-748`); test paths under `crates/uni/tests/common/` (`guides/bge-m3-hybrid-retrieval.md:11`, Black Book ~5395); skill `xervo.md:5,25` task list; `db.xervo()?` (skill `:658`); `SharedNeuralInputArgument`; `SHOW INDEXES` columns.

## 8. Forks and graph algorithms

- **wrong** Tag-then-drop (C1): `features/forks.md:389-398`, skill `forks.md:31,139,165`, Black Book 9480, AGENTS.md 295, `fork_admin.rs:680-686` doc comment.
- **wrong** Black Book 9427 says branch-delete errors are swallowed; drop returns `ForkLifecycle { stage: "delete_branch" }`.
- **wrong** Diff `changed` pairs (see section 1): `features/forks.md:156,160-173`, skill `forks.md:216-220`, Black Book 9536-9541, AGENTS.md 157-158, 262. Vertex UID includes `ext_id`; the edge key includes properties.
- **missing** `drop_fork` waits `drop_fork_drain_timeout` (10 s) before `ForkInUse` (`fork_admin.rs:83-88`); `config.rs:710-713` says it applies only to async flush.
- **inconsistent** Fork compaction "Phase 5" vs "planned": `features/forks.md:21,418`, skill `forks.md:83`, AGENTS.md 257, `writer.rs:8096-8145` warning text.
- **missing** `ForkNameInvalid`, `RestoreBlockedByForks`, `ReadOnly` in the error tables; `ForkLifecycle` is also raised by drop, open and create.
- **missing** `promote_from_fork_with_options`, `PromoteOptions`, `ConflictPolicy`, and the full report fields.
- **minor** "Python mirrors Rust one-to-one" (`features/forks.md:19`); see C20.
- **missing** `ForkLocalIndexKind::Sparse`, `FusionKind::SparseRrf/SparseDot` (`features/forks.md:350-356,366`, Black Book 9517).
- **stale** Paths: `docs/complete_python_api.md` is retired; `fork_index_recall_bench.rs` is under `crates/uni/tests/common/fork/`; `fork_index_builder.rs` is now `crates/uni-fork/src/maintenance.rs` / `crates/uni/src/api/fork_maintenance.rs`; Black Book test paths now under `tests/common/`.
- **stale** Black Book 9359 "in a future phase — promote"; Black Book 9509, AGENTS.md 283 "Phase 5b will replace the decay".
- **minor** Parent auto-flush now applies to primary-rooted forks too (`features/forks.md:80`, skill `forks.md:29`).
- **wrong** Black Book algorithm catalogue (6030-6205): names (`algo.harmonic` → `uni.algo.harmonicCentrality`, `eigenvector` → `eigenvectorCentrality`, `katz` → `katzCentrality`, `topologicalSort` → `topoSort`, `cycleDetection` → `hasCycle`, `bipartiteCheck` → `isBipartite`, `kcore` → `kCore`, `dinic` → `maxFlow`, `graphMetrics` → `metrics`); map-style arguments are ignored (positional only); Dijkstra yields `nodeId, distance, path`; missing `dijkstra`, `shortestPath`, `diameter`, `sssp`, `reachability`, `gcpagerank`, `gcwalks`, `gcoverlap`; counts.
- **wrong** `max_projection_memory`, `max_vertices` do not exist (Black Book 6132, 6150; skill `graph-algorithms.md:109,440-441`).
- **wrong** `avg_neighbor`/`max_neighbor`/`sum_neighbor` take `(subject, 'REL', 'prop' [, direction])` (`typecheck.rs:1848-1870`; `features/graph-algorithms.md:76-78,83`).
- **wrong** `CALL uni.algo.pageRank()` needs label and type lists (`features/graph-algorithms.md:26,41`); no BFS/DFS procedure (`:12`).
- **wrong** Skill YIELD types Node for bridges/articulationPoints/maxMatching (see C21).
- **wrong/missing** Skill: no `uni.algo.dijkstra` section; allSimplePaths builds a projection; labelPropagation `seed`; 12 arena kernels.
- **missing** Built-in algorithms require declared labels and edge types.
- **stale** `plugins/graph-algorithms.md:213-215,349-351`, skill `:619-620` engine-message recipes (`sub` ewise op does not exist; `compare(a, b, "gt")`; `mod` exists).
- **minor** Skill DAG note on reverse edges (`:445`).

## 9. Plugins, triggers, bulk ingest, overview pages

- **wrong** The default Python wheel does not include the WASM loaders (`bindings/uni-db/Cargo.toml` `default = []`; `.github/workflows/ci.yml:392-447` builds a separate wheel): `plugins/quickstart.md:19-23`, `plugins/reference.md:15-18`, `loaders/wasm-components.md:277-278`, `authoring.md:395`, `loaders/index.md:25-26`, Black Book ~9835.
- **wrong** Rust loaders take `host_grants: &uni_plugin::CapabilitySet` (`crates/uni/src/api/plugins.rs:622-628,690-696`), and `Uni::open` needs `.build().await?`: `reference.md:124-139`, `quickstart.md:102-115`, `authoring.md:433-442`, `wasm-components.md:292-294`, `extism.md:229-231`.
- **wrong** Python `Uni`/`AsyncUni` have `load_python_plugin` (`sync_api.rs:1333-1349`, `async_api.rs:828`): `loaders/pyo3.md:109-117,225-226`, `reference.md:103`, `loaders/index.md:16,28,30`.
- **inconsistent** Non-Rust loaders can author graph algorithms (`adapter_algorithm.rs` in each loader; `algorithm-plugin` world): `plugins/index.md:29-39,85`, `concepts.md:49`, `authoring.md:33-34`, `loaders/index.md:13,24-30`, `rhai.md:175-177`, `pyo3.md:227`, `wasm-components.md:52`.
- **wrong** Extism host functions are `uni_kms_sign`, `uni_kms_verify`, `uni_secret_acquire`, `uni_http_get`, `uni_http_post`, `uni_graph_call` (`crates/uni-plugin-extism/src/host_svc/mod.rs:43-54,172-207`); no fs or host-query functions: `concepts.md:107`, `trust-and-capabilities.md:195-196`, `wasm-components.md:45-46`, `extism.md:271`.
- **inconsistent** Artifact pins are enforced on all four byte loaders (`plugins.rs:633,701,763,830`): `trust-and-capabilities.md:245-246`, `plugins/index.md:79`.
- **wrong** `add_plugin` does not intersect with a host grant set (`plugins.rs:318-320`; `authoring-rust.md:138-140`).
- **wrong** Lance is not a registered storage plugin: `plugins/index.md:9,46`, `concepts.md:16,30`, `authoring-rust.md:132-133`, Black Book surface table.
- **wrong** The Python source-string plugin loader is lenient about unknown, quota and internal grants (`sync_api.rs:1342`, `builders.rs:1310,1332`, `async_api.rs:838,2608,2632`): `trust-and-capabilities.md:176-178`.
- **wrong** Extism aggregate exports are `agg_<q>_new/update/merge/evaluate`, procedures `proc_<q>_invoke` (`extism.md:154-158`).
- **wrong** `features/bulk-ingest.md:14-56`: Python needs the label declared; Rust `insert_edges` takes `Vec<EdgeData>` and properties are `HashMap<String, uni_common::Value>`. **missing** commit point, `BulkCommitIncomplete`, `bulk_load_applied`, `abort()`, `skip_before_triggers`.
- **wrong** Black Book `declareTrigger` argument shape (~10030); signature `(qname, event_filter, body[, deps_json])`.
- **wrong** Black Book "signature verification is default-on"; default `SignaturePolicy::Disabled` (`crates/uni/src/api/plugin_trust.rs:63-70`).
- **inconsistent** Surface counts: `SurfaceKind` has 22 variants (`surfaces/mod.rs:1320`); 26 extension-surface capability variants. `concepts.md:9,23,45`, `loaders/index.md:13,24`, Black Book loader matrix, ~9915, glossary D.
- **stale** Black Book plugin details: `QName { namespace, local }`; no `storage_backend` or connectors list; opentelemetry 0.32 / tracing-opentelemetry 0.33; test paths under `crates/uni/tests/common/{loaders,reload}/`; 72 kernels; `ProvidedSurfaces` has `connectors`; `ExternalIo`; footer "Version 2.2.1".
- **inconsistent** Algorithm counts: `website/docs/index.md:29` (42) vs `:96` (36); Black Book "35 native".
- **wrong** `reference/glossary.md:74,155,301` packed IDs; gryf; deprecated `add_hook`.
- **missing** `algorithms_registered` in returned dicts and `LoadOutcome` (`reference.md:83-93,109-111,176-185`, `quickstart.md:133-152`, `rhai.md:108`, `pyo3.md:122-124`).
- **wrong** `use-cases/rag-knowledge-graph.md:119,197,213,226`: `Uni` has no `query_with`; `xervo()` is not a `Result`; "one Docker container".
- **minor** `reference/index.md:72-81` type names; built-in `LabelAuditTrigger` unlisted; `authoring-rust.md:12` operators and connectors were removed in 3.0.

## Not reviewed

`.ipynb` notebooks under `website/docs/examples/`; benchmark numbers against fresh runs;
Black Book 1730-1845 (CSR) and 7240-7536 in detail; `docs/complete_locy.md`.
