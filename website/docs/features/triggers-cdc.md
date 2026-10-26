---
title: Triggers & CDC
---

# Triggers & Change Data Capture

Uni can react to committed writes in three ways. They differ in what they see
and what they guarantee:

| Mechanism | What it receives | Guarantee | Surfaces |
|---|---|---|---|
| **Triggers** | One Arrow batch of mutation events per matching commit, with pre-images | `Before*` triggers can veto the commit; `After*` triggers run once the commit has landed | Rust `TriggerPlugin`; Cypher `uni.plugin.declareTrigger` (usable from Python) |
| **CDC outputs** | One `CdcBatch` per commit, with the same event rows | Contiguous: a stream halts on a gap instead of skipping one. Checkpointed | Rust `CdcOutputProvider` + `CdcStream` |
| **Commit notifications** | Commit metadata only (version, labels, counts), no rows | Best effort: a slow consumer loses the oldest notifications | Rust `Session::watch()`, Python `Session.watch()` |

Use a trigger to act on a change, or to refuse it. Use CDC to ship every
change to another system. Use `watch()` to learn that something changed. For
`watch()`, see [Commit notifications](../reference/rust-api.md#delivery-best-effort-not-a-feed).

Triggers and CDC outputs are plugin surfaces. Only native Rust plugins can
author them; the WASM, Extism, Rhai and PyO3 loaders cannot. The plugin
mechanics (manifest, capabilities, `Uni::add_plugin`) are covered in
[Authoring (Rust)](../plugins/authoring-rust.md). From Python, triggers are
available through the Cypher `declareTrigger` procedure. Python has no CDC
API.

## Mutation events

Triggers and CDC both receive mutation events as an Arrow `RecordBatch` with
one fixed schema:

| Column | Type | Contents |
|---|---|---|
| `event_kind` | `UInt8` | `1` node create, `2` node update, `3` node delete, `4` edge create, `5` edge update, `6` edge delete |
| `vid_or_eid` | `Int64` | Vertex id for node events, edge id for edge events |
| `label` | `Utf8` | The label for node events, the edge type for edge events |
| `property` | `Utf8` | Always the empty string |
| `old_value` | `LargeBinary`, nullable | The pre-image: the entity's committed user properties, serialized as JSON. Null for a create |
| `new_value` | `LargeBinary`, nullable | Always null |
| `properties_new` | `LargeBinary`, nullable | Post-mutation values of the properties that some trigger predicate references (`n.x`), as a CypherValue-encoded map. Otherwise null |
| `properties_old` | `LargeBinary`, nullable | Pre-image values of the properties that some trigger predicate references (`old.x`). Otherwise null |

Each written or deleted entity produces one row; there is no row per
property. A vertex with several labels produces one row per label.

The event-kind masks are `NODE_CREATE`, `NODE_UPDATE`, `NODE_DELETE`,
`EDGE_CREATE`, `EDGE_UPDATE` and `EDGE_DELETE`. The `TriggerEventMask` type
also defines `PROPERTY_CHANGE`, `LABEL_ADDED` and `LABEL_REMOVED`, but
event extraction never produces those kinds, so a trigger subscribed only to
them never fires.

### Create, update or delete

A write to an entity that already existed before the transaction is an
**update** and carries a pre-image. A write to a new entity is a **create**
and carries none. A delete carries the pre-image of what it deleted. This
holds whether the entity's committed state is still in memory (L0) or has
already been flushed to storage (L1). See [Pre-images](#pre-images).

### Pre-images

`old_value` is the entity's committed state before the transaction, read
from the newest place it lives:

- **Vertices.** The engine first checks the in-memory L0 chain (current and
  pending-flush buffers, newest first). A tombstone in a newer buffer counts
  as a delete, so stale properties in an older buffer cannot come back. Any
  vertex the transaction touched that L0 does not hold is looked up in
  storage, in batched `_vid IN (...)` scans per label. Either way the
  pre-image is the user property map: declared properties, `ext_id`, and
  undeclared properties. It never contains an internal `_`-prefixed column.
- **Edges.** Every edge the transaction writes or deletes is read in one
  batched lookup over the committed L0 chain layered on storage. Newer L0
  state wins over storage. The pre-image holds the edge's whole committed
  property set (declared, undeclared and schemaless). A property that was
  removed is absent from it. Edge-delete rows carry the edge type name, so a
  delete trigger filtered on `-[:Type]-` fires.

A pre-image read is never served from the transaction's own uncommitted
buffer.

Known gaps:

- A vertex that has no label in the transaction's buffer skips the storage
  lookup. If its committed state was already flushed, it can be reported as a
  create with no pre-image.
- If the storage lookup for vertices fails, the affected vertices are
  reported as creates and the commit still succeeds.
- An edge whose type id cannot be resolved to a name is skipped for writes.
  On delete it is reported with the placeholder label `type:<id>`, which no
  edge-type filter matches.

## Triggers

### Phases and fire modes

A trigger subscribes to one `TriggerPhase`. All four phases run at commit
time; none runs per statement.

| Phase | Runs | Can veto |
|---|---|---|
| `BeforeMutation`, then `BeforeCommit` | Before the commit is written | Yes, in `Synchronous` mode |
| `AfterMutation`, then `AfterCommit` | After the commit is published | No |

`FireMode` sets how the trigger runs:

- **`Synchronous`.** Runs on the commit path, before `commit()` returns.
  - In a `Before*` phase, returning `TriggerOutcome::Reject { reason }` or an
    `Err` aborts the transaction. `commit()` then fails with
    `UniError::TriggerRejected { trigger, reason }` and none of the
    transaction's writes land.
  - In an `After*` phase, an error or panic is logged and the commit still
    succeeds.
- **`Async`.** Runs after the commit on a blocking worker, once per matching
  commit, and cannot reject. Errors and panics are logged. `shutdown` and
  `drop` wait for in-flight async fires.
- **`EventualConsistency`.** Matching batches are coalesced per trigger and
  fired as one combined batch. A bucket is flushed when it is older than
  `UniConfig::ec_flush_interval` (default 1 s) or reaches
  `UniConfig::ec_flush_threshold` rows (default 10,000). A bucket that grows
  past four times the threshold is drained immediately. The combined fire
  reports the session and transaction id of the first commit in the window.

`Async` and `EventualConsistency` triggers subscribed to a `Before*` phase
are skipped.

A trigger can also return `TriggerOutcome::Defer { until }`, built with
`TriggerDeferral::from_payload` (re-fire on the next tick) or
`TriggerDeferral::after` (re-fire after a delay). The re-fire calls
`on_deferred`, which defaults to `fire`. Deferring again is capped at ten
attempts, after which the item is dropped with a warning.

- A deferral returned in a `Before*` phase is queued only if every `Before*`
  trigger passes, so an aborted transaction leaves none behind.
- With a local data path, the deferral queue is mirrored to
  `<data_path>/_system/deferred_triggers.json` and reloaded on open.
  Otherwise it is in memory only.
- The queue file is not written in the same transaction as the commit. A
  crash between a commit and the next queue write can lose a deferred item.

### Filters

A `TriggerSubscription` selects events with:

- `events`: a `TriggerEventMask`; combine masks with `.union()`.
- `labels`: node events only. Exact match; `None` means every label. Write
  bare names (`"Account"`); the Cypher form `":Account"` names the same label.
- `edge_types`: edge events only. Exact match; `None` means every type. A
  leading `:` is accepted here too.
- `predicate_source`: a Cypher boolean expression evaluated per event row.
  - `n.<prop>` reads the post-mutation value and `old.<prop>` the pre-image.
  - The event columns (`event_kind`, `vid_or_eid`, `label`, ...) are also in
    scope.
  - The predicate is compiled at commit time. One that does not compile
    makes the commit fail with `TriggerRejected`.
  - If evaluation fails, a `Before*` phase aborts the commit; an `After*`
    phase logs the error and skips the trigger.

The `properties` field of `TriggerSubscription` is not enforced: every
matching event is delivered regardless of which properties changed. To react
to a specific property, reference it in the predicate, for example
`old.balance <> n.balance`.

### Rust: a `TriggerPlugin`

Implement `TriggerPlugin` (`uni_plugin::traits::trigger`) and register it
from a plugin whose manifest declares `Capability::Trigger`:

```rust
use std::sync::Arc;
use uni_plugin::FnError;
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome,
    TriggerPhase, TriggerPlugin, TriggerSubscription,
};

struct NoNegativeBalance {
    sub: TriggerSubscription,
}

impl NoNegativeBalance {
    fn new() -> Self {
        Self {
            sub: TriggerSubscription {
                phase: TriggerPhase::BeforeCommit,
                events: TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
                labels: Some(vec!["Account".into()]),
                edge_types: None,
                properties: None,
                predicate_source: Some("n.balance < 0".into()),
                fire_mode: FireMode::Synchronous,
                // The first line of `docs` names the trigger in errors.
                docs: "no-negative-balance".into(),
            },
        }
    }
}

impl TriggerPlugin for NoNegativeBalance {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(&self, _ctx: TriggerContext<'_>, events: &MutationBatch) -> Result<TriggerOutcome, FnError> {
        // `events.events` holds only the rows that matched the predicate.
        Ok(TriggerOutcome::Reject {
            reason: format!("{} account(s) would go negative", events.events.num_rows()),
        })
    }
}

// In your `Plugin::register`:
//     r.trigger(Arc::new(NoNegativeBalance::new()))?;
// then install the plugin with `db.add_plugin(plugin)?`.
```

`fire` is synchronous and must not call `block_on` on the host runtime. A
panic inside it is caught at the dispatcher. `TriggerContext` has the
public fields `session_id` and `tx_id`, and a `host()` accessor.

Triggers are read from the database's instance plugin registry
(`Uni::plugin_registry()`), which forked sessions share (see
[Forks](#forks)).

### Cypher: `uni.plugin.declareTrigger`

A declared trigger runs a Cypher action after a matching commit. It needs no
compiled code, so it works from any binding:

```cypher
CALL uni.plugin.declareTrigger(
  'mycorp.audit',                      // qualified name
  'CREATE ON :Person WHEN n.age > 18', // event filter
  'CREATE (:AuditLog {person: $vid, kind: $event_kind})'  // action
)
```

```python
from uni_db import Uni

db = Uni.in_memory()
tx = db.session().tx()
tx.execute("""
  CALL uni.plugin.declareTrigger(
    'mycorp.audit', 'CREATE ON :Person', 'CREATE (:AuditLog {person: $vid})')
""")
tx.commit()
```

The arguments are `(qname, event_filter, body)`, all strings, plus an
optional fourth `deps_json` (default `'[]'`). The procedure yields
`registered`. The event filter grammar is:

```text
VERB ('|' VERB)*  [ON :Label | ON -[:Type]-]  [WHEN <boolean expr>]  [[ASYNC]]
```

- Verbs are `CREATE` (or `INSERT`), `UPDATE` (or `SET`) and `DELETE` (or
  `REMOVE`). Keywords are case-insensitive.
- `ON :Label` restricts the trigger to node events and `ON -[:Type]-` to
  edge events. With no `ON`, both match.
- `WHEN` takes the same predicate language as `predicate_source` above.
- An empty filter, or one with no recognised verb, matches every node and
  edge create, update and delete, and logs a warning.

How a declared trigger behaves:

- It always runs as `AfterCommit` + `Async`. Declaring `[SYNC]` is an error
  and stores nothing. A declared trigger therefore cannot veto a commit; use
  a Rust `TriggerPlugin` for that.
- The action runs once per matching event row, with the parameters `$vid`
  (`vid_or_eid`), `$label` and `$event_kind` (the integer kind above).
- The action's writes go straight to the database's in-memory buffer and
  WAL, not through a new commit, so they do not fire triggers again. A depth
  guard (four levels) stops any further nesting.
- A failing action is logged; the commit that fired it has already
  succeeded.
- Declarations are persisted and reinstalled when the database is reopened.
  List them with `CALL uni.plugin.listDeclared()`. `CALL
  uni.plugin.dropDeclared(qname)` removes the persisted declaration and
  yields `removed`.

## CDC outputs

A CDC output is a push-style sink. Implement `CdcOutputProvider`
(`uni_plugin::traits::cdc`) and register it from a plugin whose manifest
declares `Capability::Cdc`, via `PluginRegistrar::cdc_output`. `Capability::Cdc`
cannot be granted to a guest plugin.

```rust
pub trait CdcOutputProvider: Send + Sync {
    fn name(&self) -> &str;
    fn start(&self, ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError>;
    fn resumes_from_checkpoint(&self) -> bool { true }
}

pub trait CdcStream: Send {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError>;
    fn checkpoint(&mut self) -> Result<CdcLsn, FnError>;
    fn shutdown(&mut self) -> Result<(), FnError>;
}
```

Each commit becomes one `CdcBatch`:

- `mutations`: the event rows described in [Mutation events](#mutation-events).
  With CDC alone, no trigger predicate references properties, so
  `properties_new` and `properties_old` are null. The pre-image in
  `old_value` is still populated.
- `lsn_start`: the database version the transaction started from.
- `lsn_end`: the database version after the commit.
- `commit_timestamp`.

A commit with no event rows is delivered as an empty batch with the same
schema.

Batches arrive in commit order: `lsn_end` never decreases from one batch to
the next. A commit's notification is ordered at the moment it becomes visible,
under the writer's commit lock, so a commit that finishes its post-commit work
first still cannot be delivered ahead of an earlier one. (Before this was
fixed, a commit landing in that window could make an earlier commit report
the later commit's version and be delivered after it.)

A bulk load (`bulk_writer`, `appender`) is delivered as one or more batches,
all carrying the load's LSN range (`lsn_start` the load's version, `lsn_end`
the database version when it became visible, which can equal the previous
commit's `lsn_end`). Its rows are split across them in chunks of at most 4096,
and each stream is checkpointed once, after the last. See
[Bulk loads](#bulk-loads).

### Delivery

- **After commit, off the commit path.** A commit does not wait for sinks.
  One background driver delivers each commit to every stream in turn, then
  asks the stream for its checkpoint. Both calls run on the blocking pool.
- **Halt on a gap, never skip.** A stream is halted when:
  - its `deliver` errors or panics;
  - its `checkpoint` panics;
  - a commit's mutation batch could not be built;
  - the driver falls more than `UniConfig::commit_channel_capacity` commits
    behind (default 256). This one halts every stream.

  A halted stream receives nothing more until the database is reopened. Each
  halt logs an error and increments `uni_cdc_stream_halted_total`. A panic
  in one stream does not affect the others.
- **Checkpoints.** The latest LSN each stream reports is persisted to
  `<data_path>/_system/cdc_checkpoints.json` by a background writer, never
  past a gap. The persisted position can lag the last delivered commit. On
  reopen, `start` receives that position as `CdcStartContext::from_lsn`.
  The host delivers only commits made after the stream starts. Any replay
  of commits between the checkpoint and the restart must come from the
  provider itself.
  - A provider whose `start` ignores `from_lsn` should return `false` from
    `resumes_from_checkpoint()`, and its checkpoint is not persisted.
  - A database opened from a remote object-store URI has no local data path
    and persists no CDC checkpoints.
- **Late registration.** A provider added after the database is open is
  started at the next commit it can be given rows for. Earlier commits are
  not backfilled. A commit made while no CDC output was registered captures
  no rows. A provider that registers after such a commit, but before the
  driver reaches it, starts after that commit and does not checkpoint past
  it: its first checkpoint is the first commit it received.

No CDC provider is registered by default. For tests, the built-in in-memory
provider is opt-in:

```rust
use uni_plugin_builtin::extras::{MemoryCdcOutputProvider, MemoryCdcPlugin};

let captured = MemoryCdcOutputProvider::new();
db.add_plugin(MemoryCdcPlugin::with_provider(captured.clone()))?;
// ... commit ...
let batches = captured.delivered();
```

It keeps every batch until shutdown, ignores `from_lsn` and does not resume
from a checkpoint. Do not leave it on in a long-running process. When it was
registered by default it cost 236.6 MB of RSS over 20,000 small commits,
against 55.8 MB without it.

## Bulk loads

A bulk load (`tx.bulk_writer()`, `tx.appender()`) writes straight to storage,
not through a transaction, and commits on its own. Once it is visible it emits:

- one commit notification, with `tx_id` the load id and `session_id`
  `"bulk-load"`;
- a `NODE_CREATE` row per vertex and label and an `EDGE_CREATE` row per edge,
  read back from storage in chunks of at most 4096 rows. A load's events are
  never held in memory at once. `old_value` and `properties_old` are null;
  `properties_new` follows the same predicate-referenced rule as a commit.
- After-phase triggers fire once per chunk, so a large load reaches a trigger
  as several batches.
- CDC gets the chunks as batches of one announcement, as described under
  [CDC outputs](#cdc-outputs).

A synchronous `BeforeMutation` / `BeforeCommit` trigger cannot veto a load: its
events exist only after the load is visible. A load that would write rows such
a trigger subscribes to (by event kind and label or edge type; a predicate is
not evaluated, so a predicated trigger counts as matching) is refused with
`UniError::BulkLoadBlockedByTrigger` before those rows are staged, or at
`commit()` for a trigger registered mid-load, and nothing of it becomes
visible. `BulkWriterBuilder::skip_before_triggers(true)` (and the same option on
the appender builder) loads anyway, without running those triggers.

Once a load is visible, its events are delivered by a background task of
their own, which `commit()` waits for. Dropping the `commit()` future does not
stop the delivery: every event still reaches triggers and CDC. (The dropped
commit skips the load's index and snapshot finalization; the next open
completes it and delivers the load's events again, so consumers see them
twice.) A shutdown during the delivery stops it; CDC then halts with its
checkpoint before the load, and the next open delivers the whole load again.

A load completed in the background after `BulkCommitIncomplete` emits its events
when it becomes visible. A load the database completes while opening, after a
crash that came before its events were delivered, emits them from the open:
announced before `build()` returns, so ahead of any commit made through the
handle, with its rows streamed in the background. Only consumers that exist
during the open see them: built-in triggers and triggers declared with
`uni.plugin.declareTrigger` (the open reactivates those), and CDC outputs
already registered -- which the public API offers no way to do before
`build()` returns. A trigger or CDC output registered after the open does not
get the load. Delivery is at least once: a crash during it delivers the load
again at the next open. A fork's recovered load fires triggers when the fork is
next opened. See [Data ingestion](../guides/data-ingestion.md#triggers-cdc-and-the-enclosing-transaction).

A transaction commit that is abandoned after its commit point (its `commit()`
future dropped, or failing, before the notification goes out) is announced with
`mutations_failed: true` instead of not at all, so CDC halts rather than skip
it.

## Crash recovery

A commit is durable once it is written to the write-ahead log, and announced
(its commit notification sent, its after-phase triggers fired) just after. If
the process dies in between, the next open announces it:

- before `build()` returns, ahead of any commit made through the handle, with
  `tx_id` `"wal-recovery-<lsn>"` and `session_id` `"wal-recovery"`;
- with the same events the commit would have produced: each write is
  classified (create, update, delete) and its pre-image read against the state
  the commit was made on;
- only to consumers that exist during the open: built-in triggers and triggers
  declared with `uni.plugin.declareTrigger`. Before-phase triggers do not run;
  the commit is already durable.

Delivery is at least once: a crash just after a commit was announced can
announce it again at the next open. A database closed normally, by
`shutdown()` or by dropping it, records which commits were announced, so a
clean reopen announces nothing twice.

## Forks

Forked sessions share the primary's plugin registry:

- **Triggers fire on fork commits.** A trigger registered on the database
  fires for commits on every fork as well as on the primary; there is no
  per-fork scoping. The pre-image probe and a declared trigger's action use
  the fork's own buffers, so a declared action triggered by a fork commit
  writes to that fork. `shutdown` and `drop` of the database wait for async
  fires started on a fork.
- **CDC sees primary commits only.** The CDC driver subscribes to the
  primary's commit channel, so fork commits are never delivered to a CDC
  output. They still pay for building the event batch (see
  [Costs](#costs)).
- **`watch()` is per fork.** On a forked session it reports only that fork's
  commits.
- **Scratch transactions** refuse `commit()`, so no trigger fires for them.

## Costs

With no triggers and no CDC outputs registered, a commit does no event
extraction at all. Registering any trigger or any CDC output turns the
following on for every commit, on the primary and on every fork:

| Work | Cost |
|---|---|
| Event extraction | One event row per written or deleted entity, built under a short read of the transaction buffer. |
| Pre-image probe | An L0 scan, plus a storage (L1) lookup for every vertex and every edge the commit writes, including entities that turn out to be new. Lookups are chunked `IN` scans, not one per entity. |
| CDC batch | Built only when a CDC output is registered. |
| CDC checkpoint | Each persisted checkpoint is an atomic file write with two fsyncs, on the blocking pool. Writes coalesce while one is in flight. Skipped for providers that do not resume from a checkpoint. |
| Synchronous triggers | Run on the commit path and add their own time to `commit()`. |

Measured: 1000 flushed edges updated in one commit with a trigger registered
took 124–383 ms with the edge pre-image probe (3 runs) and 109–282 ms
without it (6 runs). At this size the difference is inside run-to-run noise.

## See also

- [Plugin concepts](../plugins/concepts.md) and [Authoring (Rust)](../plugins/authoring-rust.md): the plugin surfaces and registration.
- [Transactions & Consistency](transactions-consistency.md): commit semantics.
- [Forks](forks.md).
- [Rust API: commit notifications](../reference/rust-api.md#delivery-best-effort-not-a-feed).
