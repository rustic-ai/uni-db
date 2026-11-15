# Plan: upgrade Lance 13.0.0 to 14.0.2

Status: **done**; see §11 for what was built and how it differs from the plan.
Date: 2026-11-14

Follows `docs/proposals/lance_13_upgrade_2026_10_26.md` (11 → 13), whose method
this plan reuses. Scope: `lance`, `lance-index`, `lance-linalg` in
`uni-store`; no Arrow, Parquet or DataFusion change.

## 1. Summary

- **The code should compile unchanged.** Every Lance API this project calls
  has the same public signature in 14.0.2. That was checked by diffing the
  13.0.0 sources against the 14.0.2 crates.io tarballs, not by building.
- **Three things need work**: an FTS index built with a built-in stop-word
  list should be rebuilt; `lance::Error` has a new `Shared` variant that can
  hide `NotFound` from our variant matches; a persisted bulk-load marker holds
  a serialized Lance `Fragment` whose form must still parse.
- **It does not unblock Arrow or DataFusion.** Lance 14 stays on arrow 58 and
  datafusion 54. The move to arrow 59 / datafusion 55 is Lance 15, which is at
  beta.21 and not on crates.io.
- **It does not fix the branch bugs.** The branch code is byte-identical in 13
  and 14. Three branch data-loss fixes exist only in 15.0.0-beta.8 and later.
- **What it buys**: four fixes upstream marks critical, and several that touch
  paths we use (§3). No new feature we need.

Recommendation: do it, as a contained change, with the cross-version fixture
of the last upgrade re-run. Estimated two to three days, most of it
verification. §8 gives the case for waiting for 15 instead.

## 2. Where we are

All real Lance code is in `crates/uni-store`, behind the `lance-backend`
feature. No other crate imports `lance`.

| File | Role |
|---|---|
| `backend/lance.rs` | `LanceDbBackend`, the `StorageBackend` implementation |
| `backend/lance_branch.rs` | branches, tags, branch writes (`ForkBranching`) |
| `backend/lance_directory.rs` | URI layout, session and caches, write params |
| `backend/lance_fts.rs` | BM25 over unindexed rows, using Lance FTS internals |
| `backend/fts_analyzer.rs` | tokenizer config to `InvertedIndexParams` |
| `storage/manager.rs` | conflict retry, dataset writes |
| `storage/index.rs`, `json_index.rs`, `inverted_index.rs`, `sparse_index.rs` | index datasets under `indexes/` |
| `snapshot/restore.rs` | tag, restore and rewind of index datasets |

Manifests:

- Root `Cargo.toml`: `lance`, `lance-index`, `lance-linalg` at `13.0.0`. The
  three move together: Lance pins its sub-crates exactly, and a drift puts two
  Lance and DataFusion trees in the lockfile.
- `uni-store/Cargo.toml`: a private `lance-object-store` alias for
  object_store 0.14, since DataFusion 54 and Uni stay on 0.13.
- `fuzz/Cargo.lock` is a separate lockfile and a static guard checks it.
- `deny.toml` ignores three advisories reached through `lance-io`, with the
  note "Re-test on every lance upgrade."

On-disk policy (unchanged by this plan): new datasets are written in file
format 2.1; appends and overwrites keep an existing dataset's format
(`lance_directory.rs`, `write_params`). Lance's own default is 2.2 in both 13
and 14.

## 3. What changed upstream

Releases: 13.0.0 (2026-10-07), 14.0.0 (10-26, never published to crates.io),
14.0.1 (10-28), 14.0.2 (10-29). 290 commits. No 13.0.x patch releases exist.
The repository moved to `lance-format/lance`.

### Dependencies

| Requirement | 13.0.0 | 14.0.2 |
|---|---|---|
| Rust version | 1.91 | 1.94 (the workspace floor is already 1.96) |
| arrow, datafusion, roaring, prost, tokio | 58, 54, … | unchanged |
| object_store | ^0.14.1 | ^0.14.2 (already locked at 0.14.2) |
| zstd, in `lance-encoding` | ^0.13 | ^0.14 |
| jsonb | ^0.5.3 | replaced by a vendored `lance-jsonb` |
| New | | `blake3`, `nalgebra`, `hex`, `num-traits` |

zstd: the workspace patches `zstd` 0.13 to a facade over the patched 0.14.3.
Lance 14 asks for 0.14 directly, so it reaches the real 0.14.3 already in the
lockfile. The facade stays, because Arrow, Parquet and Wasmtime still ask for
0.13. `compat/README.md` should stop naming Lance.

### Breaking changes, and whether they reach us

| Change | Reaches us? |
|---|---|
| File format 2.3 mini-block offsets | No. We pin 2.1 |
| BTree scalar index in the row-address domain | No. It applies to datasets with stable row ids, which we never enable; our BTree indexes stay version 0 |
| `ScalarIndexPlugin::load_index` takes an index version | No. We implement no plugin |
| jsonb vendored into `lance-jsonb` | No. We use no Lance JSON helper |
| Batch distance functions removed from `lance-linalg` | No. We use only `MetricType` |
| `Operation` gains a non-exhaustive `Unknown` variant | Check: `lance.rs` reads `Transaction.operation` |
| `lance::Error` gains `Shared { source }` | **Yes**, see §4.2 |
| MemWAL and blob changes | No |

### Behaviour changes that touch our paths

- **FTS stop-word lists changed** for 14 Latin-script languages and for
  Chinese, Japanese, Korean, Arabic, Greek, Russian and Tamil. Upstream:
  "Existing FTS indices must be rebuilt". See §4.1.
- **Automatic probing for vector search** now has calibrated profiles up to
  k = 100,000 (was 100). We pass `nprobes` explicitly, which is unchanged, but
  any search that leaves it unset can shift in recall and latency for k > 100.
- **Cleanup** bounds its delete concurrency by the store's I/O parallelism and
  treats `.tmp*` files under `data/` as unverified (the 7-day rule). We hold
  staged bulk-load files across cleanup, so `held_staged_files_survive_cleanup`
  is the test to watch.
- **Commit retry**: the retry budget starts after catching up. Our own retry
  loop sits above Lance's.
- **Storage options**: alias conflicts resolve deterministically and explicit
  options beat environment variables. On native S3, `copy_if_not_exists`
  defaults to `multipart` from 14.0.1.
- **2.1 writer**: a page's variable-width dictionary is capped at 8 MiB by
  default. Output is still valid 2.1.

### Fixes we gain

Marked critical upstream:

- rows dropped when a negated exact predicate was combined with an inexact
  scalar index result;
- an indexed merge-insert that missed overlay-stale rows;
- nested `FixedSizeList` flattening;
- RLE-compressed mini-block dictionary reads.

Others on paths we use: ordered scans over 8,192 rows returned shuffled when
the optimizer added parallelism; cleanup reclaiming abandoned `.tmp*` data
files; a `MergeStream` deadlock; Merge commits assigning fragment ids.

### On-disk format

Unchanged between 13 and 14: default data storage version, table format
version, index metadata version, and the vector, bitmap, label-list and FTS
index versions.

Changed:

- The inline transaction moves from manifest field 21 to field 23. Lance 14
  still reads 21; Lance 13 ignores 23 and falls back to the `_transactions/`
  file.
- New manifest feature flags (managed blobs, non-reusable field ids, and two
  marked unsupported in release builds). Each is set only by an explicit
  opt-in we do not use.

So, from reading the code and not from a test: a database written by 13 opens
and writes under 14, and a database written by 14 without those opt-ins opens
under 13. §6 tests both.

## 4. Work items

### 4.1 FTS indexes with stop words

`fts_analyzer.rs` maps a tokenizer config with `remove_stop_words` and a
built-in language list onto Lance's stop words. After the upgrade such an
index holds terms removed under the old list, while queries and our flat BM25
path over unindexed rows (`lance_fts.rs`) tokenize with the new one. Nothing
errors; scores and matches drift.

- Find which analyzer configs can reach a built-in list.
- Decide how an existing index is rebuilt: on first open after upgrade, by an
  admin procedure, or documented as a manual step. A stored marker of the
  Lance version an index was built with would make the first automatic.
- Indexes on the `simple`, `whitespace` or `ngram` tokenizers without stop
  words are unaffected.
- Test: an index built before the change against a query whose terms differ
  between the two lists. This needs the old binding, as in §6.

### 4.2 `Error::Shared`

Errors from shared reads (`lance-io` object reader, `lance-encoding` decoder)
can now arrive as `Error::Shared { source }` wrapping the real error. A direct
`matches!(e, lance::Error::NotFound { .. })` misses it. Our direct matches:

- `lance_directory.rs`, the not-found classifier (which also matches message
  text);
- `store_utils.rs`, `DatasetNotFound` / `NotFound`;
- `lance.rs` and `snapshot/restore.rs`, `RefNotFound` on tag delete;
- `storage/manager.rs`, the conflict classifier (`CommitConflict`,
  `RetryableCommitConflict`, `IncompatibleTransaction`,
  `TooMuchWriteContention`, `DatasetAlreadyExists`).

Change each to classify `e.find_root()`. Whether a shared error can actually
reach these sites was not traced; the change is cheap either way. A
misclassified conflict would stop a retry, and a misclassified not-found would
turn "absent" into a hard error.

### 4.3 Persisted `Fragment` in bulk-load markers

A staged bulk load serializes `Vec<lance::table::format::Fragment>` with serde
into the marker under `catalog/.../bulk_flush_intents/`, and publishing
deserializes it. A marker written by 13 and published by 14 must still parse.

- Diff `Fragment` and its nested types between the two versions.
- Test: a marker JSON captured from 13 deserializes under 14 and publishes.
- If the form changed, the upgrade must refuse or migrate an in-flight marker.

### 4.4 Small checks

- `Operation::Unknown`: confirm the read of `Transaction.operation` in
  `lance.rs` has a wildcard arm.
- `partitions_searched`: our "index consulted" counters read a Lance debug
  metric by name. If it was renamed they read zero silently. The
  `issue_175_*` tests cover it.
- The cancellation probe `lance_scan_polled_after_its_runtime_is_dropped_fails_in_lance`
  changed its answer in the last upgrade; re-check it.
- `deny.toml`: re-test the three advisory ignores. `lance-io` now names
  `quick-xml ^0.41`, which may retire one.

## 5. Order

1. Bump the three version strings; refresh `Cargo.lock` and `fuzz/Cargo.lock`
   (workspace-only updates; re-pin `time` at 0.3.47 if a broad update moved
   it). Confirm one Lance tree and one DataFusion tree in the lockfile.
2. Build. Fix what does not compile; none is expected.
3. §4.2 and §4.4, with their tests.
4. `uni-store` tests, then the workspace suite.
5. §4.3, then §4.1.
6. Cross-version fixture (§6) and the performance smoke.
7. The remaining lanes (§7), documentation, release note.

Before step 1, save a Python extension built with Lance 13: the fixture and
the FTS test both need the old binary.

## 6. Cross-version verification

Reuse `fixture.py` and `verify.py` from
`docs/proposals/lance_13_upgrade_2026_10_26/`, each step in its own process:

1. With the Lance 13 extension, build the fixture database: declared and
   schemaless vertices, edges, BTree, vector and FTS indexes, a fork and a
   nested fork, a tag and a snapshot.
2. With Lance 14: open it, write on primary and both forks, compact, rebuild
   indexes.
3. With Lance 13 again: open the result and read it, the pinned snapshot
   included. This is the rollback check.
4. Lance 13 reads a database freshly created by Lance 14.

Additions for this upgrade:

- an FTS index with English stop words in the fixture (§4.1);
- a staged, unpublished bulk load left in the fixture (§4.3);
- the known gap of the last run stays excluded and stated: full-text queries
  on an inherited fork fail with a missing branch-local tokens dataset. The
  fixture is not evidence about it.

## 7. Verification lanes

Every lane of `docs/local_ci_runbook.md`, plus what is specific to Lance:

- `uni-store`: `lance_version_policy`, `lance_branch_retention`, the
  `lance_branch.rs` unit tests (`create_branch_not_idempotent_at_same_version`
  and the `phase5a`, `phase5b` and `phase249` spikes), the staged-write tests
  in `lance.rs`, `eval_agrees_with_lance_sql`.
- `uni-db`: `dense_index` recall (calibrated against Lance's HNSW; the last
  two upgrades moved it), `issue_175_*`, `issue_290_runtime_bound_state`,
  `lance_cache_sizing`, the fork suites.
- Failpoints, both TCKs in both schema modes, the Python suites.
- LocalStack cloud tests (`ci.yml`). They are the only check of the S3 default
  change, and they are an emulator, not live S3.
- Performance: `perf.py` old against new, and the IAI gate on a CI runner.

Not reproducible locally, as last time: CUDA and Metal wheels, live Azure and
GCP, the strict IAI comparison.

## 8. The case for waiting for Lance 15

- 15 is what unblocks arrow 59 and datafusion 55, and it carries the three
  branch data-loss fixes (deleting a nested branch deleting its parent's data;
  a losing concurrent `create_branch` committing into the winner; a deleted
  parent leaving files that block re-creation). Our fork system is built on
  branches, and `create_branch` recovery already works around one of these.
- Going to 14 now means a second upgrade, with its own fixture run, when 15
  ships.
- Against waiting: 15 has no release date, needs Rust 1.95 and drags Arrow,
  Parquet and DataFusion with it, so it is a much larger change. 14 is small
  and carries fixes for silent wrong answers (dropped rows, shuffled ordered
  scans) on paths we use today.

Whether any of the three branch bugs is reachable through our fork paths is
worth checking regardless of the upgrade: a concurrent `create_fork`, and a
`drop_fork` of a nested fork's parent.

## 9. Not verified

- That 14.0.2 compiles against this workspace.
- Read and write compatibility in either direction; §3 is from version
  constants and feature flags.
- Whether `Error::Shared` reaches our match sites.
- Whether the S3 `multipart` default affects manifest commits.
- Whether 2.1 output differs beyond the dictionary cap.
- Whether the open upstream issues against 14 (a legacy-layout FTS index after
  deferred-remap compaction; filtered scans after adding a nullable struct
  child in 2.2; slow opening of many scalar index segments since 13)
  reproduce here.

## 10. Stale references to fix on the way

- `compat/README.md` names Lance 11.
- `AGENTS.md` gives the old path of `lance_branch_retention.rs`; it is
  `crates/uni-store/tests/common/fork_recovery/lance_branch_retention.rs`.
- `lance_branch.rs` still speaks of lancedb 0.27.1; `manager.rs` cites
  lance-3.0.1.
- The Black Book's "Lance dependency and file-format policy" names 13.0.0.

## 11. As built

Lance is at 14.0.2. No call site needed a signature change; `uni-store`
compiled unchanged.

### What was done

- **Bump.** The three version strings, `Cargo.lock` and `fuzz/Cargo.lock`.
  One Lance tree and one DataFusion tree in the lockfile; `time` still at
  0.3.47; `jsonb` replaced by `lance-jsonb`. `cargo deny check` passes with
  the existing ignores.
- **§4.2, `Error::Shared`.** All five classifiers match `find_root()`. The
  wrapper is not reachable as a `lance::Error` through `anyhow`'s chain (its
  source is an `Arc`), so the change is needed, not only prudent. Test:
  `a_shared_conflict_is_still_a_conflict`.
- **§4.3, bulk-load markers.** No work: `lance-table`'s `fragment.rs` differs
  between the versions by two lines in an unrelated constructor. Test:
  `staged_record_keeps_its_stored_form` pins the stored field names and that
  a record round-tripped through JSON publishes.
- **§4.4.** Both reads of `Transaction.operation` already had a fallback
  arm. The cancellation probe, the `partitions_searched` counters and the
  format-policy tests pass unchanged.
- **§4.1, full-text indexes.** Larger than planned, see below.

### §4.1 as built

The plan spoke of indexes "built with a built-in stop-word list". That is
every full-text index but an n-gram one: `TokenizerConfig::Standard`,
`Whitespace` and `Custom` all start from `InvertedIndexParams::default()`,
which removes English stop words.

What changed for English: Lance 13 used the 198-word NLTK list; Lance 14
uses a 149-word list that is a subset of it. The 49 words dropped from the
list are the negations and contractions (`no`, `not`, `nor`, `don`,
`don't`, `isn't`, …). So no word became newly unsearchable. Words that were
dropped at index time are now searched for: found in rows indexed since,
not in rows indexed before. For rows indexed before, that is the behaviour
they had before the upgrade.

Built:

- `IndexMetadata::analysis_revision` and `FTS_ANALYSIS_REVISION = 1`
  (`uni-common`); absent in a stored schema reads as 0.
- `TokenizerConfig::uses_builtin_stop_words` and
  `IndexDefinition::built_under_outdated_analysis`.
- A rebuild trigger in `RebuildTriggerChecker::labels_needing_rebuild`.
- At open: outdated indexes are queued when
  `index_rebuild.auto_rebuild_enabled` is set, and named in a warning when
  it is not, which is the default. Automatic rebuilds are opt-in, so the
  upgrade does not start rebuilding every full-text index unasked.
- Every build outcome stamps the current revision.

Not covered: fork-local full-text indexes, which are not in the schema.

### Cross-version verification (§6)

With the scripts of the 13 upgrade, adapted, and a Lance 13 wheel built from
the tree just before the bump:

| Step | Result |
|---|---|
| Lance 13 builds the fixture | done; a search for `not` finds nothing |
| Lance 14 opens it, checks every recorded answer, writes on primary and both forks, compacts, rebuilds | pass; `not` now finds its document |
| Lance 13 reads the result, the pinned snapshot included | pass |
| Lance 14 opens a fresh Lance 13 database with automatic rebuilds on | the full-text index is rebuilt within 0.3 s and stamped revision 1; a later open does not rebuild it |
| The same with automatic rebuilds off | `not` finds nothing, as before the upgrade |

The fixture has declared and schemaless vertices, an edge, BTree, vector and
full-text indexes, a fork and a nested fork, a tag and a snapshot. It has no
unpublished bulk load (§4.3 was settled by the source diff and the unit
test) and, as last time, no full-text query on an inherited fork.

Performance smoke (`perf.py`, 5,000 rows, medians of 3): ingest 0.92, point
lookup 0.73, range 0.75, scan 0.84 of the Lance 13 times. The two
extensions were built by different maturin commands, so read this as "no
regression seen", not as a speed-up.

### S3, against LocalStack 4.13.1

An emulator, not live S3.

- The two CI lanes pass: `uni-store` `cloud_integration_test` (17) and
  `uni-db` `hybrid_localstack_e2e` (1). The second stores its Lance datasets
  on S3, flushes twice and reopens between.
- A broader run through the Python binding, with remote storage on S3,
  passes on Lance 14 and gives the same answers on Lance 13: 25 flushed
  commits, an edge, BTree, full-text and vector indexes built and queried, a
  snapshot, an update and a delete, compaction, then a reopen from S3 with
  every answer and the pinned snapshot checked again. So Lance 14's S3
  commits, the `copy_if_not_exists` default included, work on this path.
- **Forks did not work on S3, on Lance 13 and 14 alike**; fixed afterwards.
  `session.fork(..)` failed with "open lance dataset at
  s3://…/adjacency_KNOWS_fwd.lance". Two causes:
  - `LanceBranching::table_exists` answered `true` for every dataset on an
    object store, so a candidate not created yet (an edge type's adjacency
    table before its first compaction) was branched and its open failed. It
    now asks the directory, which lists `_versions/` through the object
    store.
  - Datasets opened by URI alone (the branch helpers, the index datasets
    under `indexes/`, the snapshot code) got no storage options, so the
    endpoint and credentials of the cloud configuration never reached them:
    they worked only where the same values were also in the environment.
    With them in the configuration alone, even a primary flush failed
    ("Bucket not found"). `lance_directory` now records each cloud
    directory's options and `open_dataset_uri` / `with_store_params` apply
    them by the URI's directory.

  Test: `hybrid_localstack_e2e::test_hybrid_localstack_forks_e2e`, run with
  the credentials in the environment and, with
  `UNI_LOCALSTACK_CONFIG_ONLY=1`, in the cloud configuration alone.
- **Still open: dropping a fork session on a remote store stalls 30 s on a
  single-threaded runtime.** The drop seals the fork's WAL, which is on the
  remote store, from the process-wide IO runtime, while the S3 client's
  pooled connection is driven by the caller's runtime; a `current_thread`
  runtime is blocked in that drop, so the request waits out its timeout and
  then retries on a fresh connection. It succeeds, and a multi-threaded
  runtime is unaffected (1.9 s against 32 s for the test above).

### Not done

- CUDA and Metal wheels, live Azure and GCP, the IAI gate.
- Whether any of the three upstream branch bugs (§8) is reachable through
  the fork system.
- The stale comments in `lance_branch.rs` (lancedb 0.27.1) are still there.
