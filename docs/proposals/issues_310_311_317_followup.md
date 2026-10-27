# Follow-up: #310, #311 and #317

Local investigation following PR #319. This records checkout evidence, not a
claim that any issue is closed, merged or released.

## #310: existing scoring fixes

The three current `issue_310` tests pass. They cover declared text ranking
before flush, after flush and reopen; schemaless parity with the declared
unindexed scorer; and the visible corpus after buffered replacements/deletes.
The current implementation uses BM25 for the schemaless path too. No additional
FTS production change was made in this investigation.

## #311: expanded evidence and a distinct MERGE defect

The original report describes ordinary `MATCH … SET … RETURN`, followed by
indexed read-back, missing or duplicating rows intermittently. Its existing
18-arm matrix, index-use control, concurrent-flush stress and repeated re-key
cycles all pass. This investigation does not establish that historical defect
has been fixed.

The application source linked from the issue already uses a stable view slot
and collects reads before its writes. It therefore does not supply the original
pre-workaround transaction or failing dataset:
<https://github.com/milliondreams/ontiq/blob/ccr-a51f7ece-kvz22m/server/crates/ontiq-uni/src/views.rs>.

History check: [162934c](https://github.com/milliondreams/ontiq/commit/162934c2220934a23101c2d3a917f19e2968e1c5) introduces that file (`@@ -0,0 +1,1009 @@`) and already documents stable
slots, reads before writes and avoiding indexed-property rewrites. The
pre-workaround experiment is therefore not available from an earlier committed
version of that file. This does not establish that no separate reproduction
exists elsewhere.

### Added cases

Tests stay in the existing integration module tree; no test binary was added.

- A 12-arm matrix: no view index/BTree × one/two labels × ordinary SET,
  `SET +=` and keyed `MERGE … ON MATCH SET`. Each arm uses 3,000 nodes with
  string `sid`, 257-row execution batches and 37-row cursor pages. Compare
  exact `(VID, sid)` identities, not only counts, against an unfiltered scan.
  Verify write RETURN, streamed old/new keys, both label anchors, and committed,
  flushed reads. BTree arms assert actual baseline index use.
- Four stores in one process, running those re-key paths together. Nextest
  otherwise isolates test cases in separate processes, whereas the report
  mentions parallel tests/stores in one test binary.
- Another session commits a disjoint indexed-property update and flushes while
  the re-key reader is paused after its first cursor page. Assert exact rows,
  own-write visibility and the pinned snapshot. A retriable serialization abort
  is permitted, but must publish no partial re-key; explicitly retry and verify
  the final result. Do not treat correct SSI aborts as lost/duplicate rows.
- Strengthen the original matrix to assert distinct write-return identities,
  and the existing stress test to check distinct unfiltered-oracle identities.

### Reproduced production failure

The new matrix deterministically failed on `multi_label=true, write=Merge`,
even with **no view index**: the Member-label lookup returned zero rows instead
of 3,000. The same-process test failed too. Ordinary SET and SET-map arms before
that case passed. This is not evidence that BTree lookup caused the failure.

The minimized shape is one existing node:

```cypher
CREATE (:A:B {x: 0})
// commit and flush, then in a new transaction:
MERGE (n:A {x: 0}) ON MATCH SET n.x = 7 RETURN labels(n)
// previously returned ['A']; after commit, MATCH (n:B) found nothing
```

The keyed MERGE fast path constructed its node binding with just the pattern's
required label `A`. The property-write path correctly uses the binding's label
set, so it wrote the node without `B`. Plain keyed MERGE also returned incomplete
`labels(n)`; the keyed-endpoint relationship fast path used the same helper.

The local repair binds the complete visible label set: query L0 first, then
labels fetched in bounded main-table batches under `QueryContext::l1_visibility`
(or the storage view's row visibility). All three fast-path binding sites use
`Executor::bind_matched_node`. Label prefetch is independent of the property
prefetch gate, so CRDT-bearing labels retain the same guarantee. This adds no
per-node storage scan. Normal stale-binding label-write handling is retained.

An intermediate version used the storage label index. The additional snapshot
regression rejected that version: after another session relabelled A:B to A:C
and flushed, an older transaction's MERGE incorrectly returned A:C. Executor
storage can be live while its query context carries a pin; the final label
batch must honor that context explicitly. The new test also verifies the
transaction's own earlier relabel wins over persisted labels. Neither assertion
was weakened to accommodate the intermediate implementation.

A one-node regression covers plain keyed MERGE, ON MATCH property updates and
keyed relationship endpoint binding, each from flushed and unflushed data.
It asserts returned labels, persisted labels and readability through B.

### Validation

Final validation of the snapshot-aware implementation:

- 504 selected integration tests passed, covering MERGE, label writes, SSI,
  inherited fork mutations and all #311 cases, including ignored stress tests.
- Five stress iterations of the five new cases passed: 25 test executions.
- Strict Clippy passed for all targets of `uni-query` and `uni-db` (`--no-deps
  -- -D warnings`); workspace formatting and all seven static guards passed.
- Negative control: temporarily binding only the required MERGE label made
  the one-node test fail: returned labels `['A']`, expected `['A', 'B']`.
  The fixed source was restored in a `finally` block. An additional snapshot
  assertion failed the intermediate live-index implementation; it passes with
  query-visible batched labels. No assertion or test scheduling was weakened.

Reproduction/validation commands:

```sh
cargo nextest run -p uni-db --test integration --run-ignored all \
  -E 'test(issue_311) or test(label_write_) or test(fork_partial_mutation_inherited) or test(cypher_merge_actions)'
cargo nextest run -p uni-db --test integration --run-ignored all --no-fail-fast \
  -E 'test(/merge|label_write|issue_311|ssi|fork_partial_mutation/) and not test(/soak|perf|scaling|stress/) or test(issue_311)'
cargo nextest run -p uni-db --test integration --stress-count 5 \
  -E 'test(issue_311_multilabel_merge_map_and_stream_matrix) or test(issue_311_parallel_stores_rekey_in_one_process) or test(issue_311_other_writer_commits_during_streamed_read) or test(label_write_keyed_merge)'
cargo clippy -p uni-query -p uni-db --all-targets --no-deps -- -D warnings
cargo fmt --all -- --check
python3 scripts/ci/static_guards.py
```

This is targeted validation, not a new full workspace/CI matrix run. The new
MERGE defect is related evidence, not grounds to close the original intermittent
SET/index issue. No commit, push, issue filing or GitHub comment was made for
this investigation.

Remaining evidence to seek for the original report: the pre-workaround
transaction, exact schema and dataset, and the application test that fails
with same-process parallel execution. Do not patch index dedup speculatively.

## #317: revised design, no new Action abstraction

The earlier action-catalog design was withdrawn after review. See
[Constrained hypothetical queries](locy_business_actions_317.md). Domain
eligibility stays in ordinary Locy rules, consequences in explicit Cypher
ASSUME blocks, and business measures in ordinary query columns. A new executable
Python example verifies that composition using current APIs in one pinned
scratch transaction: the cheapest eligible quote fails delivery, Q1 at 590 cents
passes the complete feasibility goal, invalid quotes are not simulated, and
all candidate evaluations restore the baseline regardless of order.

Current APIs require a host loop. The proposed native extension is a generic
correlated hypothetical-query operator, not a business-action type/registry,
new domain configuration language or `CREATE ACTION`. No native correlation or
new action API has been implemented. Existing generic ABDUCE stays unchanged.
