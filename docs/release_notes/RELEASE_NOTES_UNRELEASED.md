# uni-db — unreleased

Changes since [5.3.0](RELEASE_NOTES_5.3.0.md).

Add new changes here.

## Features

- **Indexes:** a BTree, Hash or Bitmap index on a `DATETIME` property builds and is used. It is built on a hidden key column holding each value's instant; `=`, `<`, `<=`, `>`, `>=`, `IN` and pattern-map equality against a constant DATETIME (a literal, a `datetime(...)` call, `datetime()`, or a parameter) read only matching rows from storage, with the predicate still applied, so answers are those of an unindexed scan. `<>`, comparisons with other types and `datetime.realtime()` are answered without it. An index on a `DATETIME` property recorded `Failed` by an earlier release (or declared before its property was typed `DATETIME`) is built at the next writable open, never inside a flush or on a fork. A store with such an index opened by an older release sees it as a BTree it cannot build, as before.
- **Indexes:** a BTree, Hash or Bitmap index on a `BTIC` property is now used. It is built on two hidden key columns holding each interval's bounds; `btic_overlaps`, `btic_contains`, `btic_during`, `btic_before`, `btic_after`, `btic_meets`, `btic_starts`, `btic_finishes`, `btic_equals`, `btic_disjoint` and `btic_adjacent` with the property as either argument and a constant interval (`btic('…')` or a parameter) as the other, `btic_contains_point` with a constant point, `=` / `IN` against constant intervals, and `btic_lo` / `btic_hi` compared with a constant datetime read only candidate rows from storage, with the predicate still applied. Before, such an index was built on the packed column and no predicate used it; one already in a catalog is rebuilt as a keyed index at the next writable open.

## Fixes

- **Indexes:** a row of a label with a JSON full-text index keeps its content UID through compaction and through a `SET` of another property after a flush; the index's text was hashed into it. Existing rows get the UID a fresh write computes at their next rewrite.
- **Compaction:** compacting a label with a JSON full-text or MUVERA index no longer hangs.
- **Indexes:** creating a JSON full-text, MUVERA or `DATETIME` index over flushed rows, rebuilding a label's indexes, and compacting a label no longer drop the label's other indexes (its `_vid` / `_uid` / `ext_id` indexes and the declared ones) until the next flush; filling the index's column rewrites the table, which dropped them.
- **Indexes:** creating a `DATETIME` (or `BTIC`) index right after a flush that wrote only a delete no longer makes the next flush of the label fail with "has no system column '__key_…'": the table rewrite that fills the key columns left a stale cached schema.
- **Schema:** `get_label_info` no longer lists the engine's internal columns (`__jfts_`, `__fde_`, `_gen_`, `__key_`).
- **Cypher:** every `datetime()` / `date()` / `time()` in a statement reads one clock; filters translated separately could read slightly different instants.

- **Cypher:** `a - b` on two dates or two datetimes gives the duration from `b` to `a`; it was negated.
- **Cypher:** window functions work in `WITH`, including its `WHERE` and `ORDER BY`. In an aggregating `WITH` or `RETURN`, a window runs over the grouped rows (`rank() OVER (ORDER BY count(*))`, `sum(count(*)) OVER ()`).
- **Functions:** `pow(x, y)` / `power(x, y)` accept integers and return a float, like `^`.
- **Functions:** `uni.bitwise.shiftLeft` / `shiftRight` can be called; a shift amount outside 0..63 is an error. `uni.bitwise.*` accepts a column mixed with a literal and returns NULL for a NULL operand.
- **Functions:** `btic_lo` / `btic_hi` raise an error for a bound outside the datetime range (about 1677 to 2262) instead of returning a wrapped value.
- **Functions:** `similar_to` scores sparse-vector properties (dot product) and `LIST<VECTOR>` properties (MaxSim); a sparse vector passed as a parameter is no longer NULL.
- **Traversal:** the 100-hop default-bound warning is raised for schemaless edge types too.
- **Schema:** `SHOW INDEXES` and `uni.schema.indexes()` name inverted, JSON full-text and sparse indexes; `uni.schema.constraints()` fills `target`; `DROP INDEX IF EXISTS` on a missing index succeeds.
- **Schema:** `uni.schema.createLabel` requires `VECTOR(N)` with N > 0, and a vector index over a property without a dimension is refused. An existing vector index stored with dimension 0 fails to rebuild until its property is declared with a dimension.
- **Algorithms:** the id outputs of `bridges`, `articulationPoints`, `mst`, `maxMatching` and `dijkstra` are declared INTEGER, which is what they return.
- **Schema:** DDL `DEFAULT` applies to new rows (CREATE, MERGE, bulk, COPY, edges, `ALTER ... ADD PROPERTY`). It must be a constant and is stored in the declared type. An explicit `NULL` is kept, and existing rows are not backfilled.
- **Schema:** `DROP LABEL` / `DROP EDGE TYPE` and `uni.schema.dropLabel` / `dropEdgeType` are refused while data still carries the label or type; the error gives the count.
- **Sparse vectors:** a `{indices, values}` map literal is stored canonically (ids sorted, repeated ids summed) and found by `uni.sparse.query` and `uni.search`; a malformed map is a TypeError.
- **Timestamps:** `updated_at()` is no longer earlier than `created_at()` on insert, and both work on schemaless labels and on edges after a flush.
- **Locy:** `USE module` and `USE module { r }` resolve modules registered with `rules().register`, in session and database registries, and across a restart.
- **Locy:** `DERIVE (NEW x:L {props})` stores the properties, evaluated per matched row.
- **Locy:** a recursive `REQUIRE` uses the aggregate's declared direction from the registry, so `MAX`, `MIN`, `COUNT` and plugin aggregates can carry a bound.
- **Locy:** CALIBRATE passes the model's FEATURES values under the INPUT names, the same inputs as a call site; a node input is its vid. A FEATURES/INPUT count mismatch is the compile error `CalibrateFeatureArity`, and a feature null on every row is a runtime error. A `TARGET` string `'false'` or `'0'` is a negative label.
- **Locy:** programs see session params as `$session.<key>`, as Cypher does.
- **Locy:** `MCOUNT(*)` parses, and `COUNT(*)` in a FOLD is the zero-argument row count. `EXPLAIN RULE` over a rule with a row-count fold beside another fold no longer fails.
- **Locy:** `rules().remove(name)` is refused while other rules depend on it, naming them.
- **Forks:** dropping a tagged fork is refused with `ForkTagged` (Python `UniForkTaggedError`, with `name` and `tags`) instead of leaving it Tombstoned; untag first. `drop_fork_cascade` checks the whole subtree.
- **Forks:** recovery no longer finishes a tombstoned drop whose branch could not be deleted; the fork stays Tombstoned and the log names the blocking tags.
- **Forks:** tags are per fork, so two forks can use the same tag name, and `list_fork_tags` / `untag_fork` see only the fork's own. New tags are named `fork_{fork_id}_{tag}_{dataset}`; existing tags are still read.
- **Forks:** `tag_fork` flushes the fork first, and errors (`ForkLifecycle`, stage `tag`) on a fork with nothing branched.
- **Forks:** diff fills `changed`, pairing an edited row by `(label, ext_id)` always, and by inherited VID / EID when one side is the other's ancestor. `VertexPropertyChange` and `EdgePropertyChange` gain fields for the paired row.
- **Forks:** promote no longer duplicates a vertex primary committed but has not flushed.
- **Forks:** promote on a read-only handle raises `ReadOnly`, and `Session.flush` errors are typed.
- **Forks:** `tag_fork`, `promote_from_fork` and the diff functions return `ForkNotFound` for an unknown name instead of creating the fork.
- **Python:** `PromoteReport.vertices_skipped_no_uid` is removed; it was always 0.
- **Plugins:** a WASM or Extism load the registrar refuses raises `InvalidArgument` naming the capability, as other loaders do.
- **Examples:** the PyO3 and Rhai plugin examples build against the current pyo3, arrow and datafusion.
- **Full-text search:** before a label's first flush, unflushed rows are tokenized by the declared FULLTEXT or JSON full-text index's analyzer, so `quick` finds `Quick` as it will after the flush.
- **Indexes:** a flush replaces an updated vertex's inverted-index postings, so a list changed, emptied or set to null no longer leaves the vertex under its old terms; a sparse vector set to null leaves no postings.
- **Indexes:** dropping the last JSON full-text index on a column unregisters its hidden `__jfts_` text column.
- **Forks:** promote upsert reads the `created_at` of the vertices it updates in one batch instead of once per vertex.
- **Examples:** the WASM and Extism plugin examples use arrow 58.
- **OGM:** a rollback after a successful `commit()` (an error raised later in the `with` block) no longer detaches the committed nodes or replaces the error with `UniTransactionAlreadyCompletedError`.
- **Forks:** promote looks up the UIDs the UID index misses among primary's unflushed rows only, instead of scanning the whole label.
- **Forks:** recovery finishes a tombstoned drop whose dataset no longer exists instead of keeping the fork Tombstoned.
- **Schema:** a dropped label's or edge type's DDL `DEFAULT`s no longer apply to writes that reuse the name.
- **Locy:** a vector or list-of-numbers feature reaches the classifier as a vector (at call sites, in CALIBRATE and in EXPLAIN), instead of as null.
- **Durability:** `shutdown()` returns a failed final flush's error instead of logging it, after still stopping the background tasks and releasing the writer lease; on a hybrid store the error says the commits are only in this host's local WAL (#354). Python's `shutdown()` raises it; a `with` block raises it when the block succeeded and issues a `RuntimeWarning` when the block is already raising.
- **Storage:** a flush's first write to a table on an object store checks that table alone, where it listed and checked every table in the store, a round trip each (#355).
- **Storage:** `flush_stream_timeout` bounds an async flush's time without a table commit, not its whole run, so a flush to a slow object store that keeps committing is not abandoned; a stream still ends after 15 times the timeout in all (#355).
- **Indexes:** a BTree, Hash or Bitmap index on a `TIME`, point, list, map or vector property is refused when declared (a `DATETIME` one is now indexed through a key, see Features): Lance cannot build one over a nested column, so it stayed `Stale` and was retried at every flush. One already in a catalog is recorded `Failed` and no longer retried, and a background rebuild no longer reports it `Online` (#356).
- **Search:** the planner serves `ANY(x IN n.p WHERE x IN <list|$param>)` from an Online inverted index as a candidate prefilter, unflushed rows included, with the predicate re-applied. New `CREATE INVERTED INDEX ... OPTIONS {normalize, max_terms_per_doc}` DDL.
- **Search:** unknown OPTIONS keys on FULLTEXT, INVERTED and JSON FULLTEXT indexes are refused, and `IF NOT EXISTS` no longer replaces an inverted, JSON full-text or sparse index.
- **Search:** `uni.fts.query` interprets `AND`, `OR`, `NOT`/`-term`, quoted phrases and parentheses, on flushed, unflushed and schemaless rows. A blank query matches nothing; an all-negated or malformed query is an error, as is a phrase on an index without positions. FULLTEXT accepts `with_positions`.
- **Search:** JSON full-text indexes keep a derived text column of the document's string values (optionally limited by `paths`), filled at flush and backfilled on creation. `uni.fts.query` searches JSON columns, and `CONTAINS` on a JSON column or path is prefiltered from it.
- **Storage:** a `SET`, `REMOVE` or `MERGE ... ON MATCH SET` on a flushed vertex keeps its `created_at`, before and after the next flush.
- **OGM:** a `Vector[N] | None` field is a nullable vector column, so saving a node without a vector no longer fails with "cannot be null".
- **OGM:** `SparseVector[N]` fields are saved as `uni_db.SparseVector`; a `TypeMappingError` is raised if the bindings lack that class.
- **OGM:** `tx.create_edge` accepts nodes added to the same transaction (sync and async). A failed commit clears the vid, session and identity-map entry of the nodes it had created.
- **CLI:** `uni import <name>` runs the importer the name selects; an unknown name fails and lists the available importers (`semantic-scholar`).
