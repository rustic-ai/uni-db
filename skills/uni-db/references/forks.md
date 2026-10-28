# Forks

Named, durable, isolated branches of the graph. Phase 4a: writable + nested + lifecycle (TTL, tags, budget, cancellation, pin).

## Quick start

```rust
let session = db.session();
let fork = session.fork("scenario_1").await?;
// or .new_().await for must-create

let tx = fork.tx().await?;
tx.execute("CREATE (:Person {name: 'Bob-on-fork'})").await?;
tx.commit().await?;
// Lands on the fork's branches; primary is unchanged.
```

## API

| Method | What it does |
|---|---|
| `Session::fork(name)` | Open or create a fork. `.await` opens-or-creates; `.new_().await` errors with `ForkAlreadyExists` if the name is taken. |
| `Session::is_forked()` | `true` when this session was returned by `fork`. |
| `Session::fork_schema()` | Fork-local schema mutation builder. Mirrors `db.schema()`. Adds labels and edge types to the fork's overlay only — primary is unaffected. Required under `strict_schema: true` to introduce fork-only labels. Errors with `InvalidArgument` on a non-forked session. |
| `Uni::list_forks()` | All Active forks. |
| `Uni::fork_info(name)` | Metadata for a single fork. |
| `Uni::drop_fork(name)` | Full 2PC drop. Errors with `ForkInUse` while sessions are alive, `ForkInflightTx` while a transaction is open on the fork, `ForkHasChildren` while nested children exist. |
| `Uni::drop_fork_cascade(name)` | Drop the fork and every descendant. Pre-validates the whole subtree (every node must satisfy the `ForkInUse`/`ForkInflightTx` checks); errors with `ForkSubtreeInUse` before tombstoning anything. Drops deepest-first; crash mid-cascade resumes via tombstone recovery. |
| `Session::flush()` | Flush the session's writer to L1. On a forked session this flushes the fork's L0 to its Lance branches. Phase 3 auto-flushes a parent fork during nested-fork creation, but `Session::flush()` is the explicit knob when you need it. |
| `Session::fork(name).ttl(Duration)` | Phase 4a. Set a wall-clock TTL on the fork; the background sweeper drops it via cascade once expired. Has no effect when the fork already exists. |
| `Uni::tag_fork(name, tag)` | Phase 4a. Tag a fork's branches with a Lance tag pinned to the current version. Tagged refs are GC-exempt — they survive Lance compaction *and* fork drops, which makes a tag-then-drop sequence safe for audit retention. |
| `Uni::untag_fork(name, tag)` | Phase 4a. Idempotent — missing tags are no-ops. |
| `Uni::list_fork_tags(name)` | Phase 4a. Returns the unique tag names applied to this fork. |
| `Session::pin_to_version(snapshot_id)` / `pin_to_timestamp(ts)` / `refresh()` | Phase 4a now works on forked sessions. Pinned forked sessions read through the fork's branches at the pinned version; writes return `ReadOnly`. Also the cheap **read-only** rollout primitive — see "Cheaper alternatives" below. |
| `Session::scratch()` | Open an ephemeral, write-isolated transaction — the cheap **write** rollout primitive. Behaves like `tx()` (`execute`, `query` with read-your-writes incl. edge traversal, `rollback`, drop) but `commit()` is refused, so writes are always discarded. See "Cheaper alternatives" below. |

## Errors

`UniError::Fork*`: `ForkNotFound`, `ForkAlreadyExists`, `ForkInUse { name, holder_count }`, `ForkInflightTx { name }`, `ForkHasChildren { name, children }`, `ForkSubtreeInUse { blockers }`, `ForkBudgetExceeded { current, max }`, `ForkCorruptRegistry`, `ForkLifecycle { name, stage, source }`.

## Cheaper alternatives: pinned reads and scratch writes

A full fork creates a Lance branch, a registry 2PC entry, and a per-fork WAL (~10ms) — the right cost when you want to **keep** or **promote** results, but overkill for throwaway per-iteration speculation (an MCTS rollout, a what-if scan). Two lighter primitives cover those cases at roughly **~1ms** each, with no branch, no registry, no WAL, and without advancing the global `id_allocator.json`:

| Need | Use | What it gives you |
|---|---|---|
| Read-only rollout / what-if scan | `session.pin_to_version(snapshot_id)` | An in-memory pin to a read snapshot. Reads route as normal; writes return `ReadOnly`. `refresh()` releases the pin. |
| Mutating rollout (write, read-your-writes, discard) | `session.scratch()` | An ephemeral write-isolated transaction over a pinned read base with an in-memory id allocator. |
| Keep / promote the result | a real `fork(name)` | Durable branch you can diff, tag, promote, or drop. |

```rust
// Read-only speculation: pin, scan, refresh — no fork.
let mut s = db.session();
s.pin_to_version(&snapshot_id).await?;
let hits = s.query("MATCH (n:Node) WHERE n.score > 0.9 RETURN n").await?;
s.refresh().await?; // release the pin

// Mutating speculation: scratch tx, read-your-writes, discard on drop.
let scratch = session.scratch().await?;
scratch.execute("CREATE (:Move {ply: 1})-[:LEADS_TO]->(:Move {ply: 2})").await?;
let seen = scratch.query("MATCH (a:Move)-[:LEADS_TO]->(b:Move) RETURN b.ply").await?;
// scratch.commit() would return an error — the writes are always thrown away.
drop(scratch); // rolls back; the global id allocator is untouched
```

`scratch()` sees its own writes — including edges it just created via traversal — but the writes never leave the process, so thousands of open-write-discard rollouts stay affordable. Reach for `scratch()` for **mutating** rollouts, `pin_to_version` for **read-only** ones, and a real fork only when you need to promote or retain the results.

## What sibling sessions on the same fork see

Two `session.fork("x")` calls on the same name resolve to the same `UniInner`. A commit on session A is immediately visible to session B's reads — no flush required. The cache is `Weak<UniInner>` so it never extends a session's lifetime.

## What writes through a fork actually do

- The fork's writer flushes through `BranchedBackend` to the fork's Lance branches.
- Datasets that exist on primary at fork-point (main `vertices`/`edges` plus per-label / per-edge-type tables) are branched eagerly.
- Datasets that don't exist yet are materialized on-the-fly: an empty parent commit on `main` (so primary stays untouched) plus a branch with the actual data. The dataset → branch mapping is persisted into the fork's registry entry, so a restart recovers the same view.
- Per-fork `IdAllocator` (`catalog/forks/{fork_id}/id_allocator.json`) is bootstrapped from primary's HWM — so VID/EID streams don't collide with primary.
- Per-fork WAL lives at `wal_forks/{fork_id}/` (NOT `wal/forks/...`).

## Operational signals

- `uni_fork_l1_flushes{fork=...}` gauge — per-fork flush count.
- `tracing::warn!` once per writer when the count crosses `UniConfig::fork_fragment_warn_threshold` (default 256). Fork compaction is Phase 5; until then, drop-and-recreate to bound fragment growth.

## Strict-schema mode

When the database is built with `UniConfig { strict_schema: true, .. }`,
unknown labels and edge types are rejected upfront. To introduce a
fork-only label or edge type, declare it through `Session::fork_schema()`:

```rust
forked
    .fork_schema()
    .label("OnlyOnFork")
    .edge_type("ONLY_ON_FORK", &["Item"], &["Item"])
    .apply()
    .await?;
```

Subsequent writes from any session sharing the fork (Day 8 cache)
see the addition immediately; primary's strict-schema view is
unchanged. The overlay is persisted to
`catalog/fork_schemas/{fork_id}.json` so a restart preserves it.

## Nested forks (Phase 3)

```rust
let a = session.fork("scenario_a").await?;
let tx = a.tx().await?;
tx.execute("CREATE (:Person {name: 'A-only'})").await?;
tx.commit().await?;

// Fork the fork. Parent is inferred from the receiver session.
let b = a.fork("scenario_b").await?;
let names: Vec<String> = b
    .query("MATCH (p:Person) RETURN p.name")
    .await?
    .rows()
    .iter()
    .filter_map(|r| r.get::<String>("p.name").ok())
    .collect();
// `names` contains primary's rows + A's writes (snapshot at b's creation).
```

Parent-inference rule: `session.fork(name)` always parents the new fork on the *receiver* session. If the receiver is a primary session, the new fork is a child of primary; if the receiver is a forked session, the new fork is a child of that fork. There is no API to override the parent.

Read resolution: a leaf-fork read chains through `base_paths` from the leaf's branch up through every ancestor branch to main. Lance handles this transparently; the depth cost is one extra commit lookup per level (Phase 3 perf sanity asserts depth-5 within 5× depth-1 latency).

Drop semantics:
- `drop_fork(parent)` errors with `ForkHasChildren` while any descendant exists.
- `drop_fork_cascade(root)` removes the whole subtree. Pre-validates every node for `ForkInUse`/`ForkInflightTx` before tombstoning; on any blocker errors with `ForkSubtreeInUse` *and no branch is deleted*.

## Lifecycle admin (Phase 4a)

**TTL.** `session.fork("ephemeral").ttl(Duration::from_secs(3600)).await?` stamps a wall-clock expiry on the fork. The background sweeper polls every `UniConfig::fork_sweeper_interval` (default 60s) and drops expired forks via `drop_fork_cascade`. `UniConfig::fork_default_ttl` is the fallback when the builder doesn't specify one. Set `UniConfig::disable_fork_sweeper = true` in tests that race against TTL.

**Budget.** `UniConfig::max_forks: Option<usize>` caps total fork count (Active + Pending + Tombstoned). Hitting the cap surfaces `ForkBudgetExceeded { current, max }`. Tombstoned forks count because they still hold branch state on disk until recovery completes.

**Tags.** `db.tag_fork(name, "audit-2026-q1").await?` creates one Lance tag per dataset, namespaced `fork_{tag}_{dataset}`, pinned to the branch's current version. Tags are GC-exempt — a tagged-then-dropped fork's state remains on disk for audit retention. `db.list_fork_tags(name)` returns the deduplicated user-visible tag names; `db.untag_fork(name, tag)` is idempotent.

**Cancellation.** Forked sessions hold `parent_token.child_token()`. Cancelling a parent session cascades to every descendant; cancelling a child does not affect the parent. `Session::cancel()` cancels the currently-held token and replaces it with a fresh one — tests asserting propagation must capture token clones BEFORE calling cancel.

**Pin on forked sessions.** `Session::pin_to_version`, `pin_to_timestamp`, `refresh`, `is_pinned` all work on forked sessions in Phase 4a. The pinned manager preserves `fork_scope` so reads still route through the fork's branches at the pinned version; writes return `UniError::ReadOnly` while pinned.

## Python (Phase 4b)

The full fork surface is bound to Python through both sync (`Uni` /
`Session`) and async (`AsyncUni` / `AsyncSession`) facades. Builders
chain synchronously; only the terminal `.build()` / `.apply()` is
awaitable on the async side.

```python
import uni_db
from datetime import timedelta

db = uni_db.Uni.builder().max_forks(100).build()
db.schema().label("Person").property("name", "string").apply()

primary = db.session()
fork = primary.fork("scenario").ttl(timedelta(hours=1)).build()
tx = fork.tx()
tx.execute("CREATE (:Person {name: 'fork-only'})")
tx.commit()

db.tag_fork("scenario", "audit-2026-q1")    # GC-exempt; survives drop
del fork
db.drop_fork("scenario")

# Async equivalent:
# fork = await primary.fork("scenario").ttl(timedelta(hours=1)).build()
```

Typed exceptions carry payload attributes:

```python
try:
    db.drop_fork("a")
except uni_db.UniForkHasChildrenError as e:
    print(e.children)             # ['b', 'c']
except uni_db.UniForkInUseError as e:
    print(e.holder_count)         # 1
except uni_db.UniForkBudgetExceededError as e:
    print(e.current, e.max)       # 100 100
```

See `bindings/uni-db/examples/fork_quickstart.py` and
`bindings/uni-db/examples/fork_audit.py` for runnable demos. Full
reference at `docs/complete_python_api.md` § "24. Forks (Phase 4b)".

## Fork-local index fusion (Phase 5a-impl)

Once a fork accumulates ~10k rows on a label whose column primary has
indexed, the background builder schedules a fork-local `ScalarBtree`
build automatically. After the build registers, the planner rewrites
`MATCH (n:L {col: $x})` queries on the fork from `Scan` to
`FusedIndexScan { kind: BtreeUnion }` — visible in
`session.query_with(...).explain().await?.plan_text`.

For deterministic builds (tests, power users) call
`Session::build_fork_local_index(label, column, kind)` with
`ForkLocalIndexKind::ScalarBtree`, `Sorted`, `VidUid` (Phase 5a-impl)
or `Vector`, `FullText` (Phase 5b).

## Phase 5b — vector + FTS lossy fork-local fusion

`Vector` and `FullText` kinds build per-fork Lance native indexes
that `BranchedBackend::vector_search` and `full_text_search` route
through automatically. `CALL uni.vector.query(...)` and
`CALL uni.fts.query(...)` on a forked session return fused results
across primary-inherited and fork-local rows via Lance's
`base_paths` chain. Recall scaffold lives at
`crates/uni/tests/fork_index_recall_bench.rs` (`#[ignore]`'d).

## Phase 6 / 6b — Diff & promote

`db.diff_fork_primary(name)` and `db.diff_forks(a, b)` return a
`ForkDiff { vertices, edges }` describing the structural delta
between two views. Identity is **content UID**, not VID — two
unrelated forks that happened to roll the same VIDs still pair
correctly. `diff(a, b).invert() == diff(b, a)` by construction.

`db.promote_from_fork(name, &[PromotePattern])` scans the fork per
pattern, dedups by `(label, uid)` for vertices and
`(src_uid, dst_uid, edge_type)` for edges, and bulk-inserts on
primary inside one transaction. Mix vertex and edge patterns in
one call — endpoints inserted by an earlier vertex pattern become
visible to a subsequent edge pattern via an in-memory cache.

```rust
use uni_db::PromotePattern;

let report = db.promote_from_fork(
    "staging",
    &[
        PromotePattern::label("Person"),
        PromotePattern::edge_type("KNOWS").where_clause("r.since > 2020"),
    ],
).await?;
```

Python:

```python
report = db.promote_from_fork(
    "staging",
    [
        uni_db.PromotePattern.label("Person"),
        uni_db.PromotePattern.edge_type("KNOWS", where_clause="r.since > 2020"),
    ],
)
```

Edge endpoints must already exist on primary by UID (or be
promoted in the same call); otherwise the edge surfaces in
`PromoteReport.edges_skipped_no_endpoint`.

What's still out of scope after Phase 7:
- Multi-edge promotion (parallel edges of the same type between
  the same endpoints with different property bags). Needs an
  edge-content UID column.
- Property additions through `fork_schema()` (label/edge-type
  only for now; `SchemaDelta::added_properties` is reserved for
  a follow-up).
- Locy `FORK { … }` syntax — explicitly excluded by the spec.
- Hypothesis persistence (ASSUME snapshots).
