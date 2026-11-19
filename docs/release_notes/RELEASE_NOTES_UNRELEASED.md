# uni-db — unreleased

Changes since [5.2.1](RELEASE_NOTES_5.2.1.md).

## Correctness fixes

- **A filtered multi-vector search returned rows the filter excludes** (#343).
  `uni.vector.query` on a `List<Vector>` column applied its `filter` to flushed
  rows only, so an unflushed row outside the filter was returned and could take
  a top-`k` slot from a row inside it. After such an update was flushed, the
  superseded row version still matched. The filter now applies to unflushed
  rows, and only a vertex's latest flushed row can match, with or without a
  MUVERA index and on a fork.
- **Sparse search now honours `filter`** (#344). `uni.sparse.query` rejected any
  filter (the behaviour 2.4.1 introduced), and the sparse arm of `uni.search`
  ignored it, so a filtered hybrid search returned rows the filter excluded.
  Both now rank only the rows the filter allows. The filter is resolved against
  the label's table before the sparse index is consulted, so its cost grows
  with the number of rows that pass it.
- **A multi-vector or sparse search returned fewer than `k` rows, or none, when
  its best matches had unflushed updates or deletes.** Candidates dropped after
  the flushed fetch were not replaced. The fetch now widens by the number of
  unflushed rows that can displace a flushed hit, as dense search already did.
  This did not need a filter to happen.
- **A search did not follow an unflushed label change.** After `REMOVE n:L` on
  a flushed vertex, the multi-vector, sparse and full-text searches of `L`
  still returned it; after `SET n:L`, every search of `L`, dense included,
  missed it. Both lasted until the next flush. `MATCH` was already correct, and
  the searches now agree with it.
- **A multi-vector search lost unflushed rows once a label had about a thousand
  of them.** The 1,000-candidate cap was applied to the flushed and unflushed
  candidates together and dropped an arbitrary share of the unflushed ones,
  best matches included. It now bounds the flushed candidates only.

## Python

- **`pre_filter` on the OGM's `vector_search` and `sparse_search` is now
  applied.** It was accepted and silently dropped. It is passed to the search
  procedure as its filter: a predicate over the label's columns by bare name
  (`"age >= 18"`, not `"n.age >= 18"`), applied before the top-`k` cut. A value
  written in Cypher syntax did nothing before and is an error now.

## Dependencies

- `cargo deny` ignores RUSTSEC-2026-0543 (`chrono`) and RUSTSEC-2026-0555
  (`chrono-tz`), both "unmaintained" with no patched version; moving off them
  changes the public API and is tracked in #341.
- `libsais-rs` 0.2.2 → 0.2.4 and `clipboard-win` 5.4.1 → 5.4.3 in the lockfile;
  the older versions were yanked.
