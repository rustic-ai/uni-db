# uni-db 5.3.0

Changes since [5.2.1](RELEASE_NOTES_5.2.1.md).

5.3.0 makes a search give the same answer before and after a flush. A
`filter` now holds on unflushed and updated rows in multi-vector search, and
sparse search accepts one for the first time. Searches also follow unflushed
label changes and no longer come up short when their best matches have
unflushed updates or deletes. Behaviour and API changes are listed first.

## ⚠️ Behaviour and API changes

- **`uni.sparse.query` accepts a `filter`, and the sparse arm of `uni.search`
  applies it** (#344). `uni.sparse.query` rejected any filter with a "not yet
  supported" error (the behaviour 2.4.1 introduced), and `uni.search` passed
  its filter to the dense and full-text arms only, so a filtered hybrid search
  returned rows the filter excluded. Both now rank only the rows the filter
  allows. A caller that relied on the error, or on the sparse arm being
  unfiltered, sees different results. The filter is resolved against the
  label's table before the sparse index is consulted, so its cost grows with
  the number of rows that pass it: a `_vid IN (…)` allow-list is cheap, a
  filter most of a large table passes is not.
- **`pre_filter` on the Python OGM's `vector_search` and `sparse_search` is now
  applied.** It was accepted and silently dropped. It is passed to the search
  procedure as its filter: a predicate over the label's columns by bare name
  (`"age >= 18"`, not `"n.age >= 18"`), applied before the top-`k` cut. A value
  written in Cypher syntax did nothing before and is an error now.
  `uni-pydantic` 5.3.0 requires `uni-db>=5.3.0`.
- **Rust: two `uni-store` signatures changed.** `StorageManager::sparse_search`
  takes a `filter` and a query context, and `SparseVectorIndex::query_topk`
  takes an optional set of allowed vids. Code calling either directly needs
  the extra arguments (`None` keeps the old behaviour). The `uni-db` facade is
  unchanged.

## Correctness fixes

- **A filtered multi-vector search returned rows the filter excludes** (#343).
  `uni.vector.query` on a `List<Vector>` column applied its `filter` to flushed
  rows only, so an unflushed row outside the filter was returned and could take
  a top-`k` slot from a row inside it. After such an update was flushed, the
  superseded row version still matched. The filter now applies to unflushed
  rows, and only a vertex's latest flushed row can match, with or without a
  MUVERA index and on a fork.
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

## Dependencies and build requirements

- **uni-xervo 0.20.0** (was 0.19.2) in the lockfile and the published wheels.
  The requirement stays `>=0.19.2, <1.0.0`. What a uni-db user can notice:
  - Model catalog validation is stricter, so a catalog that built under 0.19
    can fail when the runtime is built: a task the provider does not support,
    unchecked `local/onnx` and OCR option values, and a model `revision` that
    is a path are now rejected up front instead of failing, or misbehaving,
    on first use.
  - On the `uni-db-cuda` and `uni-db-metal` wheels, each accelerated
    `local/onnx` model is checked against CPU when it loads. A model whose
    accelerator output disagrees runs on CPU instead of returning wrong
    results. Loading costs one extra CPU session and forward pass; set
    `"accelerator_check": "probe"` on an alias for the 0.19 behaviour.
  - `graph_optimization_level` and the thread-count options now take effect
    for every `local/onnx` task; an alias that set them was silently ignored.
- **wasmtime 49.0.7** (was 49.0.5), for RUSTSEC-2026-0566 through -0573,
  published 2026-11-19. `uni-plugin-wasm` now requires `>=49.0.6`. The fixes
  matter to builds that load WebAssembly plugins.
- `cargo deny` ignores RUSTSEC-2026-0543 (`chrono`) and RUSTSEC-2026-0555
  (`chrono-tz`), both "unmaintained" with no patched version. Moving off them
  changes the public API and is tracked in #341. A crate that depends on
  uni-db and runs `cargo deny` with the default policy needs the same ignores.
- `libsais-rs` 0.2.2 → 0.2.4 and `clipboard-win` 5.4.1 → 5.4.3 in the lockfile;
  the older versions were yanked.
