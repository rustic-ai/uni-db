# Choosing a `refine_factor` default — measurement

**Date:** 2026-08-26
**Bench:** `crates/uni/benches/ann_pareto.rs`
**Question:** should IVF-PQ default `refine_factor` above 1, and to what?

`docs/perf/ann-2026-08-25.md` found that IVF-PQ — the **default** index
algorithm — returns ~0.56 recall@10 without a `refine_factor`, and ~0.99 with
one, for ~3% throughput. This measures the axes a default would have to respect
before a number is chosen, rather than shipping the one value that happened to
be measured first.

## The knob that actually sets the difficulty

PQ compresses each vector to `sub_vectors` bytes (at the default 8
bits/sub-vector). The compression ratio is therefore

```
ratio = dim * 4 / sub_vectors
```

and `sub_vectors` defaults to **16 regardless of `dim`**
(`crates/uni-common/src/vector_index_opts.rs`). So the default index compresses
*harder as embeddings get wider*:

| dim | example | default compression |
|---:|---|---:|
| 128 | SIFT | 32x |
| 384 | MiniLM | 96x |
| 768 | BERT-base | 192x |
| 960 | GIST | 240x |
| 1536 | OpenAI ada-002 | 384x |

That is the deeper issue: a dimension-blind `sub_vectors` means recall loss
grows with the very dimensionality modern embeddings have.

## Axis 1 — compression (SIFT-1M, 128-d, nprobes=64, k=10)

| `sub_vectors` | compression | no refine | refine=5 | refine=20 |
|---:|---:|---:|---:|---:|
| 64 | 8x | 0.878 | 0.990 | 0.990 |
| 32 | 16x | 0.722 | 0.986 | 0.990 |
| **16 (default)** | **32x** | **0.558** | 0.904 | 0.994 |
| 8 | 64x | 0.358 | 0.710 | 0.910 |

Recall without refine degrades monotonically with compression, and **the refine
needed to recover it grows with compression too**: 5 suffices at 8-16x, 20 is
needed at 32x, and at 64x even 20 only reaches 0.910.

## Axis 2 — dimensionality (GIST, 960-d, `sub_vectors`=16 -> 240x, n=50k)

| nprobes | no refine | refine=5 | refine=20 |
|---:|---:|---:|---:|
| 16 | 0.310 | 0.600 | 0.760 |
| 64 | 0.300 | 0.620 | **0.880** |

A different corpus, a different distribution, and 7.5x the dimensionality —
and the compression story holds. At 240x, **refine alone cannot rescue recall**:
20 reaches only 0.88. Fixing `refine_factor` without fixing `sub_vectors` is
insufficient for wide embeddings.

## Axis 3 — cost vs `k` (SIFT-1M, `sub_vectors`=16, nprobes=64)

Refine re-scores roughly `k * refine_factor` candidates, so its cost scales with
`k`:

| k | no refine | refine=5 | refine=20 | QPS: none -> refine=20 |
|---:|---:|---:|---:|---|
| 1 | 0.500 | 0.760 | 0.940 | 56.5 -> 60.1 (no cost) |
| 10 | 0.568 | 0.910 | 0.984 | 54.9 -> 52.2 (**-5%**) |
| 100 | 0.628 | 0.960 | 0.984 | 41.9 -> 34.0 (**-19%**) |

`refine=5` stays close to free at every `k` measured (-5% at k=100 for 0.960
recall). `refine=20` is free at k<=10 and costs 19% at k=100. A bare multiplier
therefore gets expensive exactly where result sets are large.

## Implemented 2026-08-26

Both changes shipped; the recommendation below is what they implement.

**Measured after, on the same corpus, with no options passed** (SIFT-1M, 128-d,
K=10):

| cell | before | after |
|---|---:|---:|
| `nprobes=16` | 0.5360 | **0.9080** |
| `nprobes=64` | 0.5620 | **0.9780** |
| `nprobes=128` | 0.5620 | **0.9820** |

Two deviations from the recommendation, both forced by evidence:

- **`sub_vectors` must be a divisor of `dim`.** `IndexManager` rejects
  `dim % sub != 0`, so the `max(16, dim/8)` rule proposed below would have made
  index creation *fail* for many dimensions (100, for one). The shipped rule is
  the smallest **divisor** of `dim` at or above `dim/8`, which hits the same
  ~32x target and is always encodable. Dimensions under 128, and those with no
  divisor in range (primes), keep 16.
- **The refine floor is 12, not 10.** At 32x, refine=5 measured 0.902 and
  refine=8 measured 0.944 — both short of the 0.95 bar. 12 clears it.

**Scalar and RaBitQ quantization were deliberately left alone.** They are far
milder than PQ, and an HNSW-SQ index already reaches 0.98 through `ef_search`.
Adding a refine default there flattened that curve — a refine pass re-scores
exactly, so recall stops responding to `ef_search` — and it broke two existing
recall tests. Every number in this document is PQ; defaulting SQ by analogy
would have been the same unmeasured guess this work exists to remove.

**A note on the QPS column.** Absolute throughput is not comparable across the
runs here: the earlier ones ran on `/tmp` (tmpfs) and the later ones on real disk
after the quota problem below. Recall is the stable quantity, as recorded in
`docs/perf/ann-2026-08-25.md`.

## Recommended policy

Two changes, because fixing either alone leaves a hole:

1. **Make `sub_vectors` dimension-aware** so compression stops growing with
   `dim`. Targeting <= ~32x — e.g. `sub_vectors = max(16, dim / 8)` — keeps a
   960-d index at 32x instead of 240x, which is the regime where a modest refine
   works. This is the change that matters for modern embedding widths.
2. **Default `refine_factor` from the resulting compression**, not a constant.
   At <= 32x the data supports something in the 5-20 band; ~10 is the
   conservative pick, reaching >= 0.95 across every compression <= 32x measured.
3. **Bound the work, not just the multiplier.** Cap total refine candidates
   (`k * refine_factor`) so large-`k` queries do not pay the 19% seen at k=100.

Both remain overridable per query, and `refine_factor: 1` stays the explicit
opt-out for anyone who wants raw PQ speed.

## Limits, stated

- **One machine.** QPS figures are not portable; the recall figures are the
  transferable part.
- **GIST is measured at n=50k, not 1M**, because the 960-d index build silently
  does nothing at larger n on this build — see the open defect below. The
  compression conclusion is directional there rather than a full-corpus curve.
- **`bits_per_subvector` was not swept** (left at the default 8). It is the
  other half of the compression ratio.
- **Only `nprobes` 16 and 64 were paired with refine.** The optimum is
  bracketed, not located.

## The "960-d index build" defect — diagnosed

Filed above as an open, undiagnosed defect ("the vector index build silently does
nothing above some corpus size at 960-d"). It was **not** dimension-related, and
it was two things stacked:

1. **The environment.** `Uni::temporary()` places the database under
   `std::env::temp_dir()`, and on this machine `/tmp` is a **32 GB tmpfs with
   ~7 GB free**. GIST at 200k x 960-d is 768 MB per index family; three families
   plus Lance's own copies exhausted it. SIFT at 1M x 128-d (512 MB) fit, which
   is why it looked dimension-specific. Tracing showed the real error:
   `Failed to create table 'vertices': LanceError(IO): Disk quota exceeded`.
   With `TMPDIR` on real disk the same 200k GIST build succeeds in 3.5s.

2. **A real product defect underneath it.** The flush *failed*, and
   `flush_to_l1` still returned `Ok`. The chain:
   - the async flush stream fails; its rotated L0 stays on `pending_flush` and
     the WAL retains the data (data-safe by design, recovered on replay);
   - the drain completes anyway, because the failure decrements `pending_count`;
   - `flush_inline_under_lock` cannot repair it — it only ever writes
     `get_current()`, never a buffer stranded by an earlier failure;
   - so the barrier reports success having flushed nothing, the index build finds
     no table, declines at `debug!`, and reports `Online`.

   This is the *same* silent-success family as the drain-timeout defect fixed in
   `docs/perf/ann-2026-08-25.md`, and it survived that fix. `flush_to_l1` now
   fails when the coordinator has recorded a failed flush.

**A false start worth recording.** The first attempt asserted `pending_flush` was
empty after the barrier. That is a proxy, not the condition, and it produced a
false positive: under full-suite load a healthy in-flight flush can sit there
transiently, and the check failed `async_flush_visibility_after_drain` — a test
that passes in isolation. Replaced with an explicit failed-flush counter on the
coordinator, which has no timing race.

**A finding about the test environment, too.** Before this check, flushes were
silently failing under tmpfs pressure during the full suite and nothing reported
it. Running the suite with `TMPDIR` on real disk is what makes it green
(2571/2571); on tmpfs the same run reports a genuine, previously invisible flush
failure.
