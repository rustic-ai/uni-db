---
title: Forks
status: ga
---

# Forks

Forks are **named, durable, isolated branches** of the graph. A fork lets a session reason about an alternate version of the database — for "what if" analyses, regulatory scenarios, write-audit-publish workflows, or simply for an inspectable counterfactual that survives across restarts.

Forks are a sibling of [snapshots](snapshots-time-travel.md). Where a snapshot is a read-only point-in-time view, a fork is a *named*, *durable*, *writable* parallel timeline.

## Status

Forks are **shipped and GA** in both Rust and Python. The full surface is available: writable and nestable forks; lifecycle controls (TTL, budget, Lance tags, parent→child cancellation, pin/refresh on forked sessions); and diff + promotion for write-audit-publish workflows. The Python bindings (`Session.fork`/`fork_schema`, `Uni.list_forks`/`drop_fork`/`drop_fork_cascade`/`tag_fork`/`diff_fork_primary`/`promote_from_fork`) mirror the Rust API one-to-one.

Fork-local indexes and index fusion are **shipped** as a Rust API (see [Fork-local indexes](#fork-local-indexes)); they are not yet exposed in the Python bindings. Fork **compaction** is the remaining planned enhancement, so until it lands, long-lived heavy-write forks should be `drop_fork`-and-recreate to bound fragment accumulation (see [Operational signals](#operational-signals)).

## Quick start

```rust
use uni_db::Uni;

let db = Uni::in_memory().build().await?;
db.schema()
    .label("Person")
    .property("name", uni_db::DataType::String)
    .apply()
    .await?;

let session = db.session();
let tx = session.tx().await?;
tx.execute("CREATE (:Person {name: 'Alice'})").await?;
tx.commit().await?;

// Take a fork at the current state.
let scenario = session.fork("scenario_1").await?;

// Write through the fork — lands on the fork's branch only.
let tx = scenario.tx().await?;
tx.execute("CREATE (:Person {name: 'Bob-on-fork'})").await?;
tx.commit().await?;

// Primary sees only Alice; fork sees Alice + Bob-on-fork.
assert_eq!(
    session.query("MATCH (p:Person) RETURN p.name").await?.rows().len(),
    1
);
assert_eq!(
    scenario.query("MATCH (p:Person) RETURN p.name").await?.rows().len(),
    2
);
```

## API

### Session-level

| Method | Description |
|---|---|
| `Session::fork(name)` | Open or create a fork. Returns a `ForkBuilder`; `.await` for open-or-create. |
| `ForkBuilder::new_()` | Require a fresh fork. Errors with `ForkAlreadyExists` if the name is taken. |
| `Session::is_forked()` | `true` when this session was returned by `fork`. |

### Database-level admin

| Method | Description |
|---|---|
| `Uni::list_forks()` | All Active forks. |
| `Uni::fork_info(name)` | Metadata for a single fork. |
| `Uni::drop_fork(name)` | Full 2PC drop. Errors with `ForkHasChildren` while nested children exist. |
| `Uni::drop_fork_cascade(name)` | Drop the fork and every descendant; pre-validates the subtree for live sessions / open transactions and surfaces `ForkSubtreeInUse` on any blocker before tombstoning anything. |
| `Uni::diff_fork_primary(name)` | Structural diff `diff(primary, fork)` returning a `ForkDiff` of added / deleted / changed vertices and edges, paired by content UID. |
| `Uni::diff_forks(a, b)` | Structural diff between two named forks. `diff(a, b).invert() == diff(b, a)`. |
| `Uni::promote_from_fork(name, &[PromotePattern])` | Scan the fork per pattern, dedup by content UID, insert matches on primary in one transaction. Mix vertex and edge patterns in one call. |
| `Session::flush()` | Flush the session's writer to L1. On a forked session this flushes the fork's L0 to its Lance branches. Phase 3 auto-flushes a parent fork during nested-fork creation, so most users never call this directly. |

### Fork-local schema additions

`Session::fork_schema()` mirrors `db.schema()` but lands new labels and edge types in the fork's persisted overlay (`catalog/fork_schemas/{fork_id}.json`) and the fork's in-memory `SchemaManager`. Primary's `catalog/schema.json` is **never** touched.

```rust
let forked = session.fork("scenario_1").await?;
forked
    .fork_schema()
    .label("OnlyOnFork")
    .edge_type("ONLY_ON_FORK", &["Item"], &["Item"])
    .apply()
    .await?;

// Strict-schema mode now lets the fork CREATE new fork-only entities:
let tx = forked.tx().await?;
tx.execute("CREATE (:OnlyOnFork)").await?;
tx.commit().await?;
```

Required only under `UniConfig { strict_schema: true, .. }`. In schemaless mode (the default) `BranchedBackend` materializes the dataset+branch on the fly without a schema entry; calling `fork_schema()` is harmless but unnecessary.

`apply()` errors with `UniError::InvalidArgument` on a non-forked session.

### Errors

All fork-related errors are `UniError::Fork*` variants. Each maps to a dedicated Python exception (a subclass of `UniError`):

| Rust variant | Fields | Python exception | Raised when |
|---|---|---|---|
| `ForkNotFound` | `name` | `UniForkNotFoundError` | The named fork does not exist. |
| `ForkAlreadyExists` | `name` | `UniForkAlreadyExistsError` | `ForkBuilder::new_()` on a name that is taken. |
| `ForkInUse` | `name`, `holder_count` | `UniForkInUseError` | `drop_fork` while sessions still hold the fork. |
| `ForkInflightTx` | `name` | `UniForkInflightTxError` | `drop_fork` while a `Transaction` is still open on the fork. |
| `ForkHasChildren` | `name`, `children` | `UniForkHasChildrenError` | `drop_fork` while nested children exist — drop them first or use `drop_fork_cascade`. |
| `ForkSubtreeInUse` | `blockers` | `UniForkSubtreeInUseError` | `drop_fork_cascade` blocked by live sessions / open transactions in the subtree. No branch is deleted. |
| `ForkBudgetExceeded` | `current`, `max` | `UniForkBudgetExceededError` | Fork creation would exceed `UniConfig::max_forks`. |
| `PendingFlushTimeout` | `name` | (timeout `UniError`) | `drop_fork` could not drain pending async flushes within `UniConfig::drop_fork_drain_timeout` (default `10s`). |
| `ForkCorruptRegistry` | `message` | `UniForkCorruptRegistryError` | The on-disk fork registry failed to parse or validate. |
| `ForkLifecycle` | `name`, `stage`, `source` | `UniForkLifecycleError` | A tag / untag / list-tags operation failed at the named `stage`. |

`ForkInflightTx` fires when `drop_fork` is called while at least one `Transaction` is alive on the fork — commit or roll back first, then retry. `forked.tx()` is fully writable; there is no read-only-fork restriction.

## Nested forks

`session.fork(name)` always parents the new fork on the *receiver* session — primary if the receiver is a primary session, the receiver's fork otherwise.

```rust
let primary = db.session();
let a = primary.fork("scenario_a").await?;
let tx = a.tx().await?;
tx.execute("CREATE (:Person {name: 'A-only'})").await?;
tx.commit().await?;

// Fork the fork. b's parent is a.
let b = a.fork("scenario_b").await?;
let tx = b.tx().await?;
tx.execute("CREATE (:Person {name: 'B-only'})").await?;
tx.commit().await?;

// b sees primary's rows + a's writes (snapshot at b's creation) + its own.
// a sees primary's rows + its own — not b's writes.
// primary sees only its own rows.
```

**Read resolution.** A leaf-fork read chains through Lance `base_paths` from the leaf's branch up through every ancestor branch to main. Lance handles this transparently — the depth cost is one extra commit lookup per level. The Phase 3 perf-sanity test asserts depth-5 latency within 5× depth-1 latency on the same query. All query types resolve through a nested fork — plain `MATCH`, filtered scans, vector ANN, and full-text search alike. (Branch scans use sequential predicate filtering rather than scalar-index pushdown, which is negligible for fork-sized datasets; an earlier limitation that errored on vector/FTS and filtered reads over a fork-of-a-fork was fixed in 2.3.0.)

**Snapshot isolation at every level.** Writes on an ancestor *after* a descendant was created stay invisible to the descendant. Writes on a descendant never leak up. Sibling forks under the same parent are mutually isolated.

**Drop semantics.** `drop_fork(name)` errors with `ForkHasChildren` while any descendant exists, listing the immediate children. `drop_fork_cascade(name)` walks the subtree, pre-validates every node for live sessions and open transactions, and only then drops deepest-first via the single-fork path. A crash mid-cascade resumes through the existing tombstone recovery — partial cascade state is safe.

**Non-goals.** Hypothesis persistence (ASSUME-style snapshots) is *not* part of forks. Re-parenting a fork is not supported and not planned.

## Promotion and diff

Promotion and diff close the write-audit-publish loop. Identity is **content-addressed UID** for vertices (`SHA3-256(label, ext_id, properties)`) and `(src_uid, dst_uid)` scoped to the edge type for edges — so siblings off a shared parent, or two unrelated forks that happened to roll the same VIDs, compare correctly.

### Diff

```rust
let diff = db.diff_fork_primary("audit").await?;
println!(
    "{} added, {} deleted, {} changed",
    diff.vertices.added.len(),
    diff.vertices.deleted.len(),
    diff.vertices.changed.len(),
);
for v in &diff.vertices.added {
    println!("+ ({} uid={}) {:?}", v.label, v.uid, v.properties);
}
```

`ForkDiff::invert()` swaps `added` ↔ `deleted` and `before` ↔ `after` so `diff(a, b).invert() == diff(b, a)` by construction.

### Promote

```rust
use uni_db::PromotePattern;

let report = db.promote_from_fork(
    "publish_q2",
    &[
        PromotePattern::label("Person"),
        PromotePattern::label("Document").where_clause("n.status = 'final'"),
        PromotePattern::edge_type("AUTHORED_BY"),
    ],
).await?;

println!(
    "inserted: {} vertices, {} edges; skipped {} UID conflicts, {} dup edges, {} orphan edges",
    report.vertices_inserted,
    report.edges_inserted,
    report.vertices_skipped_uid_conflict,
    report.edges_skipped_duplicate,
    report.edges_skipped_no_endpoint,
);
```

All inserts run inside a single primary transaction that commits at the end — partial-failure semantics are atomic across patterns. Edge endpoints must already exist on primary (or be promoted earlier in the same call by a vertex pattern); otherwise the edge is counted in `edges_skipped_no_endpoint` and skipped.

Three counters report work that completed but could **not be verified against
primary**, because a lookup against primary failed rather than returning an
answer. They are not errors — the promote still committed — but a nonzero value
means the neighbouring counters understate the truth:

| Counter | What could not be confirmed |
|---|---|
| `vertices_inserted_unverified` | Whether these vertices already existed on primary. They may be duplicates, and `vertices_skipped_uid_conflict` undercounts. |
| `edges_inserted_unverified` | Whether these edges already existed on primary. They may be duplicates, and `edges_skipped_duplicate` undercounts. |
| `vertices_deletes_unverified` | Whether rows marked for delete-promotion are still on primary. The delete was **not** issued and the rows remain. |

Re-running the promote after the transient failure clears is safe: the dedup
pass runs again and skips whatever did land.

### Python

The same surface is available via `uni_db` in Python, sync and async.

!!! note "Async parity"
    Every `Uni` / `Session` fork method has an `AsyncUni` / `AsyncSession` twin with the same
    name and signature — `await db.diff_fork_primary(...)`, `await db.promote_from_fork(...)`,
    `await db.list_forks()`, `await session.fork(name).ttl(td).build()`, and so on.

=== "Python (sync)"

    ```python
    import uni_db

    diff = db.diff_fork_primary("audit")
    print(diff)  # ForkDiff(vertices=added=2/deleted=0/changed=0, edges=...)
    for v in diff.vertices.added:
        print(v.label, v.uid, v.properties)

    report = db.promote_from_fork(
        "publish_q2",
        [
            uni_db.PromotePattern.label("Person"),
            uni_db.PromotePattern.edge_type("KNOWS", where_clause="r.since > 2020"),
        ],
    )
    print(report)
    ```

=== "Python (async)"

    ```python
    import uni_db

    diff = await db.diff_fork_primary("audit")
    for v in diff.vertices.added:
        print(v.label, v.uid, v.properties)

    report = await db.promote_from_fork(
        "publish_q2",
        [
            uni_db.PromotePattern.label("Person"),
            uni_db.PromotePattern.edge_type("KNOWS", where_clause="r.since > 2020"),
        ],
    )
    print(report)
    ```

### Non-goals

- Multi-edge promotion (parallel edges of the same type between the same endpoints with different property bags) — requires an edge-content UID; deferred.
- Schema migration during promote — fork-only labels / edge types must be registered on primary before promote, or the call errors with `LabelNotFound` / `EdgeTypeNotFound`.

## Cheaper alternatives for throwaway rollouts

A named, durable fork is the right tool when you want to **keep** or **promote** the result. For per-iteration speculation you intend to *discard* — a search or planning loop that opens a scenario, writes into it, reads it back, and throws it away — a full fork (~10 ms of Lance branch + registry + WAL work) is overkill. Two lighter primitives cost ~1 ms:

- **`Session::scratch()`** — a write-isolated, transient "fork" for speculation that **mutates**. It returns a transaction with a private L0 over a pinned base and an in-memory id allocator, giving read-your-writes (including edge traversal). Its `commit()` is **refused** — all writes are discarded on drop — and it never advances the global id allocator, so thousands of open-write-discard rollouts stay affordable. See [Scratch transactions](transactions-consistency.md#scratch-transactions--throwaway-write-isolation).
- **`Session::pin_to_version(snapshot_id)`** — the fork-free **read-only** snapshot: an in-memory pin, no Lance branch / registry / WAL. Prefer it over creating-and-dropping a fork per read-only iteration.

Reach for a real fork only when the result must survive — to diff, promote, tag, or keep across restarts.

| Rollout need | Use |
|---|---|
| Speculate with writes, then discard | `Session::scratch()` (~1 ms) |
| Read-only snapshot iteration | `Session::pin_to_version` (~1 ms) |
| Keep / diff / promote / tag the result | A real fork (`session.fork(name)`) |

## Snapshot vs Fork

| | Snapshot | Fork |
|---|---|---|
| Identity | Snapshot id (content) | Name (user-chosen) |
| Mutable | No | Yes |
| Survives restart | Yes | Yes |
| Used for | Time-travel reads | What-if scenarios, audit, sandbox |
| API | `session.pin_to_version` | `session.fork(name)` |

## Storage layout

A fork is one Lance branch per dataset (vertex, edge-delta, adjacency). Reads chain to the parent via Lance's `base_paths` resolution. Primary writes after the fork-point are invisible to the fork; fork writes never appear on primary.

At fork creation, every dataset that exists on disk gets branched: the main label-agnostic `vertices` and `edges` tables, every `vertices_{label}`, and every `deltas_{type}_{fwd,bwd}` and `adjacency_{type}_{fwd,bwd}`. Datasets that don't exist yet (e.g. a label with no rows at fork-point, or a brand-new fork-only label) get materialized on-the-fly the first time the fork's writer touches them, with the parent commit on `main` left empty so primary's view stays untouched. The dynamic dataset → branch mapping is persisted into the fork's registry entry, so a restart recovers the same view.

On disk:

- `catalog/fork_registry.json` — the registry of all forks.
- `catalog/fork_schemas/{fork_id}.json` — per-fork schema overlay written by `fork_schema()` (empty under the default `strict_schema: false` mode, where datasets are materialized on-the-fly without a schema entry).
- `catalog/fork_tombstones/{fork_id}.json` — durable drop intent, removed on completion.
- `catalog/forks/{fork_id}/id_allocator.json` — per-fork VID/EID allocator, bootstrapped from primary's HWM at fork creation.
- `wal_forks/{fork_id}/` — per-fork WAL stream. Replayed in `at_fork`; primary's recovery never reads it.

## Concurrency and isolation

- **Fork creation does not block primary** (spec §10). Reads and writes on primary continue at full throughput while a fork is being created.
- **Different forks proceed in parallel.** Same-name open-or-create serializes via a per-name async mutex.
- **Same-name fork sessions share a writer.** Two `session.fork("x")` calls on the same name resolve to the same `UniInner` (cached as `Weak<UniInner>` so the cache never extends a session's lifetime). A commit on session A is immediately visible to session B's reads — no flush required.
- **Multiple sessions can hold the same fork.** A holder count is tracked and `drop_fork` refuses with `ForkInUse` while sessions are alive, or with `ForkInflightTx` when an open transaction has yet to commit or roll back.
- **Lance compaction honors branch references.** Primary GC will not reclaim fragments that a live fork still references.

:::caution Forks isolate **data**, not **code**
A fork's isolation is over graph data and schema. Custom functions and
plugins are registered at the `Uni` (database) level and are **shared** by
primary and every fork — registering one in any session affects all of
them. A fork is **not** a code or privilege sandbox. (Registering a UDF
already requires in-process Rust/Python access, so this is a clarification
of scope, not a vulnerability.)
:::

## Fork-local indexes

A fork accumulates its writes on its own Lance branches, so a query on a forked session has to consult both the parent's indexes and the fork-local rows. A **fork-local index** builds a secondary index over just the fork's branch and **fuses** it with the parent index at query time — keeping point lookups, range scans, vector ANN, and full-text search fast on a fork without rebuilding any of primary's indexes.

!!! note "Rust-only today"
    `build_fork_local_index` is a Rust API; it is not yet exposed in the Python bindings.

```rust
use uni_db::store::fork::ForkLocalIndexKind;

let fork = session.fork("scenario").await?;
// ... writes on the fork ...
fork.build_fork_local_index("Person", "email", ForkLocalIndexKind::ScalarBtree).await?;
```

`build_fork_local_index(label, column, kind)` flushes the fork's pending L0 writes, then builds the index over the fork's branch. It errors with `UniError::InvalidArgument` on a non-forked session.

`ForkLocalIndexKind` selects what is built and how it fuses with the parent:

| Kind | Fuses via | Use for |
|---|---|---|
| `ScalarBtree` | `BtreeUnion` | Equality / `IN` point lookups on a scalar column. |
| `Sorted` | `SortedKWayMerge` | Range scans and `ORDER BY`. |
| `VidUid` | `VidUidForkFirst` | VID / UID identity lookups (fork-first, falls back to primary). |
| `Vector` | `AnnRerank` | Vector ANN search (top-k merge + rerank). |
| `FullText` | `Bm25Rrf` | BM25 full-text search (reciprocal-rank fusion). |

**Automatic building.** A background builder also creates fork-local indexes once a fork's per-label row count crosses a threshold, so most workloads never call the API directly:

| `UniConfig` field | Default | Purpose |
|---|---|---|
| `fork_index_build_threshold` | `10_000` | Per-fork, per-label row count that triggers an auto-build. |
| `fork_index_builder_interval` | `30s` | Background builder poll interval. |
| `disable_fork_index_builder` | `false` | Skip spawning the builder (e.g. in tests). |

**Seeing fusion in a plan.** When a fork-local index is used, `explain()` shows a `FusedIndexScan` node whose `FusionKind` is one of `BtreeUnion`, `SortedKWayMerge`, `VidUidForkFirst`, `AnnRerank`, or `Bm25Rrf`. The lossy kinds (`Vector`, `FullText`) are additionally wrapped as `FusedIndexScanWrapped`, purely for observability — runtime behaviour is identical.

## Lifecycle admin

**TTL.** Stamp a wall-clock expiry on the fork; a background sweeper reaps expired forks via `drop_fork_cascade`.

```rust
let fork = session
    .fork("ephemeral")
    .ttl(std::time::Duration::from_secs(3600))
    .await?;
```

`UniConfig::fork_default_ttl` is the fallback when the builder doesn't specify a TTL. The sweeper polls every `UniConfig::fork_sweeper_interval` (default 60s); set `UniConfig::disable_fork_sweeper = true` in tests that race against TTL.

**Budget.** Cap total fork count to bound operational footprint:

```rust
let cfg = UniConfig { max_forks: Some(100), ..UniConfig::default() };
```

Hitting the cap surfaces `ForkBudgetExceeded { current, max }`. Counts include `Active + Pending + Tombstoned` so create/drop churn cannot slip past while tombstones await recovery.

**Tags.** Pin a Lance tag to the fork's current branch tip per dataset. Tagged versions are GC-exempt — they survive Lance compaction *and* fork drops, which makes a tag-then-drop sequence safe for audit retention:

```rust
db.tag_fork("audit-fork", "2026-q1-close").await?;
let tags = db.list_fork_tags("audit-fork").await?;
db.drop_fork("audit-fork").await?;   // branches go; tagged versions remain
db.untag_fork("audit-fork", "2026-q1-close").await?;  // idempotent
```

The on-disk tag is `fork_{tag}_{dataset}`. `list_fork_tags` returns the deduplicated user-visible tag names.

**Cancellation.** Forked sessions inherit a child cancellation token from their parent. Cancelling a parent cascades to every descendant; cancelling a child does not affect the parent. Each level is independent of its siblings:

```rust
let primary = db.session();
let a = primary.fork("a").await?;
let b = a.fork("b").await?;
let b_token = b.cancellation_token();  // capture BEFORE cancelling
primary.cancel();
assert!(b_token.is_cancelled());        // cascade reached the grandchild
```

`Session::cancel()` cancels the currently-held token and replaces it with a fresh one — capture token clones before calling cancel if you want to observe propagation in tests.

**Pin on forked sessions.** Pin a forked session to a snapshot the same way as a primary session. Reads route through the fork's branches at the pinned version; writes return `UniError::ReadOnly` while pinned. `refresh()` unpins.

## Operational signals

- `uni_fork_l1_flushes{fork=...}` — gauge incremented on every successful fork flush. A proxy for fragment growth on the fork's branches.
- `tracing::warn!` fires once per writer when the per-fork flush count crosses `UniConfig::fork_fragment_warn_threshold` (default 256). Fork compaction is deferred to Phase 5; until then, long-lived heavy-write forks should be `drop_fork`-and-recreate to bound fragment accumulation.

## Crash recovery

Recovery runs in `Uni::open` before any session is exposed.

- **Pending fork** (create crashed before completion) → rolled back. Branches force-deleted via the missing-branch-tolerant `lance_branch::delete_branch` wrapper.
- **Tombstoned fork** (drop crashed mid-2PC) → completed. Branches deleted, registry entry removed, tombstone + overlay files cleaned.

Recovery is idempotent — running it twice is a no-op the second time.

## See also

- [Snapshots & time travel](snapshots-time-travel.md) — the read-only, point-in-time sibling of forks.
- Example notebooks — `bindings/uni-db/examples/forks.ipynb` (Python, sync), `bindings/uni-db/examples/forks_async.ipynb` (Python, async), `examples/rust/forks.ipynb` (Rust).
- Example programs — `crates/uni/examples/fork_promote.rs` (write-audit-publish), `crates/uni/examples/fork_nested.rs` (nested forks), `crates/uni/examples/fork_rule_developer.rs` (rule development on a fork).
