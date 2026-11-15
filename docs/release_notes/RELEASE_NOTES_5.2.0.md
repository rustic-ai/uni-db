# uni-db 5.2.0

Changes since [5.1.0](RELEASE_NOTES_5.1.0.md) (released 2026-11-11).

5.2.0 makes Locy programs pay only for what they use. The entity-resolution
program of 5.1.0, at 1,000 mentions × 1,000 entities, runs in 1.2 s where it
took 7.2 s (release build): similarity is scored from the stored vectors
directly, proof recording and fact hydration happen only where something
reads them, a `NOT EXISTS` tied to the outer row by its `WHERE` is planned
as an anti-join, and a `DERIVE` writes its facts in one statement. A new
`LocyConfig::returned_rules` chooses which rules' facts a result carries,
and the Locy profile now shows the work done outside rule evaluation.
Behaviour and API changes are listed first.

## ⚠️ Behaviour and API changes

**`LocyConfig` and `LocyExecProfile` have new public fields (Rust source
change).** `LocyConfig::returned_rules`, and `LocyExecProfile::phases` /
`commands`. A struct literal that names every field no longer compiles;
add `..Default::default()`, as the documented examples do. Python is
unaffected.

**Full-text indexes built by an earlier release should be rebuilt.** Lance
14 revised its built-in stop-word lists, which every full-text index but an
n-gram one uses by default. English no longer drops its 49 negations and
contractions (`no`, `not`, `nor`, `don't`, …): they are indexed and
searchable. An index built before holds no postings for them, so rows
indexed earlier are still not found by those words while rows added since
are. Nothing errors. With `index_rebuild.auto_rebuild_enabled` such indexes
are rebuilt when the database opens; otherwise (the default) a warning names
them at open, and `db.indexes().rebuild(label)` rebuilds them. Indexes with
`custom_stop_words`, with stop-word removal off, or on the n-gram tokenizer
are unaffected. Fork-local full-text indexes are not tracked; rebuild them
with `build_fork_local_index`.

**`IndexMetadata` has a new public field (Rust source change).**
`analysis_revision` records which revision of full-text analysis an index
was built under. A struct literal naming every field needs
`..Default::default()`.

**A `WHERE` evaluates its subqueries last.** `EXISTS { }`, `COUNT { }` and
`COLLECT { }` conjuncts now run only for the rows the other conjuncts keep.
Answers are unchanged. A subquery that raised an error for a row the rest of
the `WHERE` rejects no longer runs for that row, so no longer raises.

**A Locy program with an `MNOR` or `MPROD` fold no longer always records
provenance.** It is recorded when something can use it: an `EXPLAIN RULE` in
the program, `top_k_proofs`, exact inference or the `TopKProofs` semiring,
or a fold whose proofs can share an event or read a probabilistic fact.
Facts, probabilities and warnings are unchanged. Such a program now reuses
strata across `ASSUME` / `ABDUCE` and after a write, as a program without a
fold does, so `hypothetical_strata_reused` and `sequenced_strata_reused` can
be non-zero where they were always zero.

## New features

- **`LocyConfig::returned_rules`: choose which rules' facts a result
  carries.** `All` (the default) returns every rule's facts, `Nothing` none,
  `Only(names)` the named rules. Each returned fact is turned into a row and
  its node and edge ids replaced by the full entities, so a program that
  derives many facts and only writes a few is faster with fewer returned.
  Commands are answered the same, and `stats.derived_nodes` still counts
  every fact. Python: `returned_rules="all" | "none" | [names]`, as a
  `LocyConfig` keyword or a config-dict key.
- **The Locy profile lists the work outside rule evaluation.**
  `profile.phases` gives the conversion and hydration of facts and each
  re-evaluation after a write, with the rules and facts covered;
  `profile.commands` gives each command's time, the facts a `DERIVE` wrote
  and the strata evaluated and reused before it. Per rule, the operator list
  gains `LocyProvenance` and `LocyLineage` when the program records them.
  Python: `LocyProfileOutput.phases` / `.commands`.

## Performance

Figures are the entity-resolution program of
`website/docs/features/nlp-extraction.md`, 1,000 mentions × 1,000 entities,
release build.

- **Whole program: 7.2 s to 1.2 s** (0.55 s with five entity types; flushed
  data within 5% of unflushed). With the default `returned_rules` add 0.3 s.
- **Similarity of two stored vectors is scored from the Arrow buffers.**
  `similar_to` and `vector_similarity` over two vector properties no longer
  convert each pair to values first. A 1,000 × 1,000 similarity rule: 285 ms
  to 50 ms at 8 dimensions, 1.96 s to 0.53 s at 384. Results are bit-identical.
- **A pattern predicate that reads one side of a join is decided on that
  side.** `MATCH (a), (b) WHERE NOT (a)-[:R]->() AND a.t = b.t` evaluated the
  pattern once per joined pair; it now filters `a` before the join.
- **`[NOT] EXISTS { MATCH (o:L) WHERE … }` tied to the outer row by its
  `WHERE` alone is a semi- or anti-join.** The subquery ran once per outer
  row, scanning the label each time. Applies to a body of plain nodes binding
  new variables; other shapes keep the per-row path. The resolution program's
  new-entity `MERGE`: 1.78 s to 0.05 s.
- **Provenance and hydration by need** (above): the first pass's rule
  evaluation 1.68 s to 0.59 s; the commands read 500 of 210,757 facts and
  only those are hydrated for them.
- **A `DERIVE`'s facts between existing nodes are written by one statement**
  in a transaction (`tx.locy`), not one each.
- **A scan of unflushed rows copies only the properties it projects.**

The number of pairs a similarity rule reads is unchanged; reaching the
vector index from a rule is tracked as #335.

## Correctness fixes

- **Forks work on cloud storage.** Creating a fork on a database whose data
  is on S3, GCS or Azure failed with "open lance dataset at s3://…" as soon
  as a table that had not been created yet was a candidate, typically an
  edge type's adjacency table before its first compaction: on an object
  store every candidate was taken to exist.
- **The cloud configuration's endpoint and credentials reach every dataset.**
  Index datasets, fork branches and snapshots were opened without them and
  worked only when the same values were also in the environment
  (`AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, …). With the credentials in
  `cloud_config` alone, a flush failed with "Bucket not found".
- **A string function of an untyped NULL is NULL.** `toLower`, `toUpper`,
  `trim`, `lTrim` and `rTrim` of a NULL literal failed with "Unsupported
  data type NULL for function lower". The literal could come from the query
  text, a NULL parameter, or an outer row's NULL value inside a subquery.

## Dependencies and build requirements

- **Lance 13.0.0 → 14.0.2** (`lance`, `lance-index`, `lance-linalg`). Arrow,
  Parquet and DataFusion are unchanged (58.4.1, 54.1.0): Lance 14 still
  requires them. New datasets are still written in file format 2.1 and
  existing ones keep their format. A database written by 5.1.0 opens and
  writes under 5.2.0, and one written by 5.2.0 was read back by 5.1.0 in
  testing (primary, forks, indexes, a pinned snapshot). Upstream fixes
  gained include rows dropped when a negated predicate met an inexact
  scalar index result, and ordered scans over 8,192 rows returned out of
  order.
- **uni-xervo 0.19.1 → 0.19.2.** A title or an initial no longer ends a
  kniv v7 sentence ("Sen. Robert F. Kennedy spoke today." was three
  sentences and two entities), and the kniv generation is detected from the
  model's metadata contents.
- Compatible updates of 29 other direct Rust dependencies (tokio 1.54,
  serde, regex 1.13, pest 2.9, uuid 1.29, clap, rhai 1.27, and others), and
  of the Python and documentation tooling.
