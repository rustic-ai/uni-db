# madsim spike — verdict: **REJECT**, with a working alternative

**Date:** 2026-08-25
**Item:** C2, the last open acceptance criterion of
`docs/proposals/test_harness_and_benchmarks_2026-08-11.md` §4.5 / Phase 10.3
**Scope as agreed:** the uni-owned path only — background compaction loop,
WAL, L0, flush coordinator. Workspace-wide adoption was never on the table.

## Verdict

**Reject.** Not on effort, and not on the "Lance sits underneath" hand-wave the
proposal used — that argument was directionally right but never checked. On
measurement, three independent facts each sink it on their own, and a fourth
removes the motivation.

The thing the spike was chartered to buy — deterministic scheduling for a
timer-driven background loop — is **already available in tokio**, costs one
dev-dependency feature, and is now demonstrated working at a **36× speedup** on
the very test that motivated the item.

---

## 1. madsim does not simulate what this path is made of

The decisive evidence is in madsim-tokio's own source, not in reasoning about
it. `madsim-tokio-0.2.30/src/lib.rs` is a two-branch shim:

```rust
#[cfg(not(madsim))] pub use tokio::*;
#[cfg(madsim)]      pub use self::sim::*;
```

and inside `mod sim`, under a comment reading literally **`// not simulated
API`**:

| tokio surface | under madsim |
|---|---|
| `net`, `time`, `task::spawn`, `runtime`, `signal` | **simulated** |
| `fs` | **passthrough** — carries `// TODO: simulate fs` |
| `sync` (`broadcast`, `Mutex`, `Semaphore`, `Notify`, `mpsc`, `oneshot`) | **passthrough** |
| `io`, `select!`, `join!`, `process` | **passthrough** |

Now the background compaction loop
(`crates/uni-store/src/storage/manager.rs:1016-1060`) and the flush path:

- `tokio::time::interval_at`, `tokio::spawn` → simulated ✓
- `tokio::select!`, `tokio::sync::broadcast` → **not simulated**
- every byte of WAL and Lance IO → **not simulated**

So madsim would deterministically control *when the timer fires* and nothing
else. The interleavings that actually produce flakiness here — IO completion
order, semaphore acquisition, channel wakeups — stay exactly as
nondeterministic as they are today.

## 2. The path is dense with primitives madsim cannot intercept

madsim substitutes `tokio`. It does not touch anything else, and this path is
mostly not tokio:

- `parking_lot::Mutex` / `RwLock` — `runtime/flush_coordinator.rs:66,67,78,141,150`,
  `runtime/writer.rs:447`
- `std::sync::atomic` — `flush_coordinator.rs:70,71,512`
- `dashmap` — `writer.rs:518`
- `std::sync::Mutex` via `uni_common::sync::acquire_mutex` — `runtime/wal.rs:336,364,389`
- `std::panic::catch_unwind` around the flush stream — `flush_coordinator.rs:493`
- **`tokio::task::spawn_blocking` for the WAL fsync** — `wal.rs:411,415`, wrapping
  raw `std::fs::File::sync_all()` on the file *and its parent directory*
  (`wal.rs:100-104`). Durability here deliberately bypasses `object_store`
  entirely, so it is unreachable by any async shim.
- `object_store`'s `LocalFileSystem` routes every put/get/list through
  `spawn_blocking` (`object_store-0.13.2/src/local.rs:351,424,450,471,499,562`).

## 3. There is no seam to apply it to

The proposal's "uni-owned path only" framing assumes the path can be run without
Lance. It cannot.

`StorageBackend` (`crates/uni-store/src/backend/traits.rs:70-377`) has exactly
one real implementor, `LanceDbBackend` (`backend/lance.rs:267`). `BranchedBackend`
is a wrapper; both `FaultBackend`s are test-only wrappers around a real Lance
backend. **There is no in-memory backend.** The trait even embeds
`tokio::sync::OwnedMutexGuard` in its public `TableWriteGuard` type
(`traits.rs:50`).

`L0Buffer` alone is pure memory (`runtime/l0.rs`), but it is not independently
schedulable — the flush coordinator reaches a `StorageBackend` through
`StorageManager` as soon as a flush finalizes (`writer.rs:609`, `:5744`).

And below that, Lance and lancedb spawn their own threads and runtimes, which is
fatal to simulation regardless of what uni does:

- `std::thread::spawn` — `lancedb-0.30.0/src/table/dataset.rs:629`,
  `io/object_store/io_tracking.rs:198`
- `rayon` — `lance-index-7.0.0/src/vector/{hnsw/builder,bq/builder,kmeans}.rs`
- `spawn_blocking` — `lance-7.0.0/src/dataset/hash_joiner.rs:47,84,173,260`, and more
- `block_in_place` — `lancedb-0.30.0/src/embeddings/{openai,bedrock}.rs`

## 4. The adoption mechanism is the one this repo already rejected once

madsim requires replacing `tokio` itself — a `[patch]`/cfg-aliased dependency —
plus a global `--cfg madsim`. That is workspace-wide by construction: `lance`,
`lancedb`, `object_store` and DataFusion would all compile against it unless
separately patched.

The loom lane already faced this choice and went the other way, for a reason
recorded in `crates/uni-store/Cargo.toml:24-32`:

> Gated as features (not a global `--cfg loom`) so the instrumentation never
> leaks into dependencies — loom-aware crates like `concurrent-queue` must NOT
> see `cfg(loom)`.

madsim cannot be gated that way. Its whole mechanism *is* the global cfg.

Structural cost, for completeness: `uni-store` has `autotests = false` and two
`[[test]]` entries against a cap of 3 (`docs/test_layout.md:15`), so a madsim
lane would consume the last slot, plus a fourth CI model-checking job.

---

## 5. What to do instead — measured, not proposed

The motivation was real. `background_compaction_test.rs` starts the loop and
then **sleeps for real wall-clock time** hoping enough ticks land:

```rust
tokio::time::sleep(run_duration).await;   // 1-2 seconds, per test
```

That is both slow and timing-dependent. tokio's own paused clock fixes exactly
that, with no new dependency beyond a feature, no global cfg, and no fourth CI
lane.

`tokio/test-util` is **not** part of tokio's `full` feature and was enabled
nowhere in this workspace; no test used `time::pause`, `time::advance`, or
`start_paused`. It is now a `uni-store` dev-dependency, and
`background_compaction_ticks_deterministically_on_a_paused_clock`
(`crates/uni-store/tests/common/storage/background_compaction_test.rs`)
demonstrates it against the real loop, real Lance IO included:

| | wall-clock |
|---|---|
| `test_background_compaction_runs_semantic` (2 s real sleep) | ~2.08 s |
| the same loop on a paused clock | **0.048-0.058 s**, 5/5 stable |

**One caveat, stated because it is the whole reason to record this rather than
just assert it.** A single bulk `advance()` does *not* work: it fires the timers
and returns before the loop has polled them, and the shutdown then wins the
`select!` — `total_compactions == 0`. The working shape is to advance one
interval at a time and yield between steps, so each tick's IO can complete. The
clock only auto-advances when every task is idle, which is what makes the result
deterministic rather than merely fast.

**Follow-up: done 2026-08-25.** All six cases in
`background_compaction_test.rs` now drive the loop on a paused clock through
the shared `run_compaction_cycle` helper. The standalone demo test was removed
once it became redundant; its reasoning lives on the helper.

| test | before | after |
|---|---:|---:|
| `test_compaction_by_size_trigger` | 2.076 s | 0.058 s |
| `test_background_compaction_runs_semantic` | 2.084 s | 0.057 s |
| `test_l1_runs_counts_non_empty_only` | 2.071 s | 0.061 s |
| `test_compaction_by_age_trigger` | 2.285 s | 0.275 s |
| `test_background_compaction_handles_empty_db` | 1.017 s | 0.012 s |
| `test_compaction_status_tracks_data_size` | 0.569 s | 0.046 s |

8/8 repeat runs stable at 0.263-0.280 s. The whole `uni-store` suite dropped
from ~2.9 s to 0.994 s.

**One case could not be fully virtualized, and that is a finding worth
keeping.** `oldest_l1_age` is computed from `SystemTime::now()`
(`storage/manager.rs`), not from tokio's clock, so under a paused clock the
aging sleep in `test_compaction_by_age_trigger` would auto-advance instantly
and the data would never age past `max_l1_age` — the trigger would never fire.
It keeps a real `std::thread::sleep` for the aging and virtualizes only the
loop's ticking. Converting it blindly would have produced a test that fails
mysteriously, or passes for the wrong reason.

## 6. What would change the verdict

Recorded so this is re-openable on evidence rather than re-litigated:

1. An in-memory `StorageBackend` implementor, making the uni-owned path runnable
   without Lance.
2. madsim gaining simulated `fs` and `sync` (its source carries the `fs` TODO
   today).
3. A need for deterministic *multi-node* or network-partition testing — madsim's
   actual design centre, and something tokio's paused clock cannot do. uni-db is
   embedded, so this is not on the roadmap.

Until then the marginal determinism madsim would add over a paused clock is
timer ordering that is already deterministic, at the cost of a global cfg the
repo has explicitly refused before.
