# uni-db 5.0.0

This release combines all changes since 4.1.1, including the work originally
planned for 4.2.0 and the subsequent unreleased changes. **4.2.0 was never
released.** The major version reflects changes to query results, transaction
and storage guarantees, and public Rust and Python APIs. Review the behavior
and API migration notes below when upgrading from the 4.1 series.

Highlights include consistent Cypher and Locy results across query plans and
flush boundaries, storage and transaction recovery fixes, graph-query
optimizations, goal-directed Locy evaluation, and the Lance 13 upgrade.

Upgraded the Rust storage backend to Lance 13.0.0 and Arrow/Parquet 58.4.1,
with DataFusion remaining at the latest compatible 54.1.0. New datasets
continue to use file format 2.1; appends and overwrites preserve an existing
dataset's format. The dependency upgrade does not opt databases into format 2.2.

Repository and wheel builds now route older transitive zstd requirements through
private compatibility facades to patched zstd 0.14.3, zstd-safe 8.2.0 and
zstd-sys 2.1.2. This fixes reused-context data leakage, seekable output lengths
past EOF, and malformed legacy-frame output handling without adding advisory
exceptions. Cargo root patches do not propagate to applications using published
Rust crates; see [compatibility scope](../../compat/README.md).

## ⚠️ Behaviour and API changes

**Python configuration and API parity.** Sync and async builders now accept all
**44 active Rust `UniConfig` fields**, including file-sandbox, object-store
request policy and index-rebuild options, alongside compaction and throttle.
`db.config()` returns the same keys and can configure another builder. Nested
config calls merge; rejected updates retain previous settings, and cache-size
and parallelism setters now obey call order. Durations accept seconds or
`timedelta`, with `None` for optional durations; invalid durations raise Python
errors. The deprecated `partial_lance_writes` no-op remains rejected.

**Configuration migration:** unknown Locy and provider-specific cloud config
keys now raise `ValueError` instead of being ignored. Correct spelling mistakes
or remove unused keys. Optional cloud credentials accept explicit `None`, as
Rust does. These changes do not alter Rust configuration defaults.

`LocyConfig(...)` and dictionary configuration both expose `semiring`,
`neural_predicates_preview` and `classifier_cache_max`. Session APIs now include
`execute_with_retry`, `compile_locy_with_config`, `build_fork_local_index`,
`replacement_scans_enabled` and `set_replacement_scans`. Transaction execute
and Locy builders accept `.params({...})`. Both database flavors also expose
`background_task_count` and database-scoped `load_python_plugin`; the existing
session plugin loader retains session scope. Async counterparts are awaitable
where the operation requires it. See the
[Python configuration reference](../../website/docs/reference/python-api.md#database-configuration)
for keys, units and examples.

**Locy no longer requires Candle.** The built-in `CandleLinearClassifier`
retains its public API, safetensors model format, and f32 inference while using
CPU scalar arithmetic directly. Minimal Lance-backed Rust builds no longer
pull in Candle through Locy; optional embedding providers retain their existing
feature controls.

**Storage and query boundary corrections.** Concurrent UNIQUE, NodeKey,
external-ID and MERGE key claims remain protected after a competing commit
flushes. Forks retain the empty view of datasets absent at their creation,
including across first writes, nested forks and reopen. Anonymous MERGE performs
intra-statement deduplication and registers commit guards like named MERGE.
Fork cardinality refreshes preserve primary's shared count cache, and concurrent
key-conflict diagnostics identify the key kind, including `ext_id`.
Dense vector search replenishes candidates invalidated by buffered replacements,
deletions or label changes, and fork scans receive the requested metric.
`WITH RECURSIVE` reports an iteration-limit error when its 1,000-round budget
ends without convergence; it previously returned successful partial results.

**Buffered deletion and recursive traversal scaling.** Key retirement uses
reverse ownership maps, and graph edge removal updates adjacency positions in
constant time. These eliminate quadratic work in large buffered delete commits.
Recursive frontier membership in leading MATCH clauses drives VID-restricted
scans instead of a full label scan every round.

**Keyed MERGE preserves every label of a matched node.** Matching an existing
`A:B` node with `MERGE (n:A {key: ...})` previously bound only `A`, so
`labels(n)` omitted `B` and an `ON MATCH SET` property update could remove
`B` from subsequent reads. Keyed node and relationship-endpoint fast paths
now retain the complete visible label set, including earlier changes in the
transaction while excluding another session's later relabels.

**Identity expressions use signed IDs consistently.** A virtual node's
`id(node)` returned a negative integer, but `id(node) < 0` returned false
because the comparison used its unsigned storage bits. Node and relationship
IDs now use the same signed representation inside expressions, through WITH,
and after collecting and unwinding entities. `elementId` uses that signed
integer's string representation too. Ordinary unsigned property values keep
their existing numeric conversion behavior.

**Schemaless full-text searches use BM25.** `uni.fts.query` previously ranked
an undeclared text property by word overlap, giving every single-word match
the same `0.5` score. It now computes BM25 over the visible documents with the
same tokenizer as a declared property without an index. Buffered updates,
deletes and replacements are applied before computing statistics, so flushing
does not change the scores. Existing schemaless filter restrictions remain.

**Procedure metrics include inner queries.** A declared procedure could
return rows while reporting zero scanned rows. Its inner queries, including
nested and per-row calls, now share the statement's counters. Hosts created
by the simple executor and graph projection resolver also inherit the
statement's memory pool, so their inner queries use its configured limit.

### Results that change

**`sum` of nothing is 0, as in Cypher.** A `sum` over no rows, or over only NULLs, returned NULL
(SQL's rule) on the DataFusion and Cypher-value paths but 0 on the row executor, so the answer
depended on the plan. It is now `0` (`0.0` for a float column) everywhere: grouped `sum`, windowed
`sum(...) OVER`, and Locy `FOLD SUM` / `MSUM`. `min`, `max` and `avg` of nothing stay NULL. A query
that tested `sum(x) IS NULL` to detect an empty group needs `count(x) = 0` instead.

**An anonymous variable-length relationship yields one row per path.** `MATCH (x)-[:R*2..2]->(e)
RETURN count(*)` over a diamond returned 1, while the same query with `[r:R*2..2]` returned 2.
openCypher binds a row per matched path whether or not the relationship is named, and both forms
now agree. Under `DISTINCT`, `EXISTS`, `min`/`max`, or a set-semantic Locy rule, where multiplicity
cannot be observed, the cheaper reachability search is still used.

**Locy keeps value types.** `FOLD MIN`/`MAX`/`MMIN`/`MMAX`, `COLLECT`, `ALONG` columns and
properties yielded from an unlabelled node or a relationship were cast to `Float64`, so an integer
came back as `1.0`, and above 2^53 the value itself was wrong. They now keep their argument's type.

**A Locy condition that cannot be evaluated is an error.** `QUERY`, `DERIVE`, `EXPLAIN RULE` and
`ABDUCE` `WHERE` clauses treated an evaluation error or a non-boolean as "drop the row". They now
raise; `true` keeps a row, `false` and NULL drop it. `QUERY ... WHERE` also uses three-valued logic
now: `WHERE a.age <> 0` no longer keeps rows whose `age` is NULL.

**A dynamically typed non-boolean in a boolean context is a type error.** `NOT n.b` kept a row whose
`b` was `1`, and `CASE WHEN n.b` took its `ELSE`. Both now raise, as Cypher does. A NULL is still
NULL.

**A user map is not a node.** Any map with an `_id`, `_vid` or `vid` key was treated as a vertex, so
`{_id: 0, x: 1} = {_id: 0, x: 2}` was true and `collect(DISTINCT ...)` merged them. A map is now an
entity only if it has an entity's full shape (`_labels` for a vertex; a type and endpoints for an
edge). `_id` is a common key in imported JSON, so this is likely to affect real data.

**A non-`DETACH` `DELETE` refuses a node with relationships created in the same transaction.**
`CREATE (a)-[:R]->(b)` followed by `DELETE a` in one transaction passed the check and cascaded the
edge away, behaving as `DETACH DELETE`.

**Locy compiler rejects two programs it used to accept.** A `FOLD` clause's `YIELD` item that is
not a `KEY`, not a fold output and not an expression over one is `UngroupedFoldYield` (`KEY` marks a
single item, so `YIELD KEY o, e, pct` declares `e` as a plain column, which was silently dropped;
write `KEY o, KEY e`). A `QUERY` that reads a variable its rule does not yield is
`UnknownQueryVariable`. A literal seed into a `COUNT` fold now counts as one row and raises the
`count_fold_seed_counted` warning; seed with `NULL AS n` to declare a key without counting it.

**Smaller Locy evaluator changes.** `/` and `%` error only for integer zero (a float zero gives IEEE
infinity or NaN). `toInteger` / `toFloat` of an unparsable string is NULL. An ordering comparison
between types with no order is NULL, not false. `^` of a non-number is a type error, not `0.0`.
These match Cypher.

**`UniConfig::parallelism` takes effect.** It had no reader; every query ran with DataFusion's
default partition count. It now sets `target_partitions`. Its default is the CPU count, which is
also DataFusion's, so only callers who set it see a change.

**A `QUERY` that pins a node KEY derives only what its goal reaches, by
default (#307).** `QUERY supertype WHERE c.sid = 'C15999'` derived every
concept's closure before filtering it (1,552 ms against 53 ms goal-directed at
16k concepts, with identical rows). `LocyConfig::goal_directed` has a new
mode, `GoalDirected::Auto`, and it is the default (it was `Off`): qualifying
top-level `QUERY` commands are answered by the magic-sets rewrite, and a rule
only such goals needed is not evaluated with the program but derived on the
first read of the result's derived facts, against the snapshot the
evaluation read, so those facts are what `Off` gives. Programs with a write
command or `EXPLAIN RULE`, and every evaluation inside a transaction, are
evaluated in full. Visible differences: `stats.derived_nodes` and `profile()`
count the work actually done; the first read of the derived facts can take
time and fail (a timeout, a closed database), caching nothing on failure;
`warnings()` (Python: `warnings`) includes the warnings deriving such rules
raised once they were derived, while the `warnings` field keeps the
evaluation's own; `stats.derived_nodes` keeps counting what the evaluation
derived; a session evaluation that left rules out reads a pinned snapshot
throughout, and holds it until the facts are read or the result dropped; a
rule left out of an evaluation cut short is listed in
`incomplete.skipped_rules`. `GoalDirected::Off` restores the old behaviour.
In `On` and `Auto` alike, an interrupted evaluation's diagnostics now name the
program's rules: a generated copy or magic rule (such as
`__locy_gdm1_reachable_bf`) is reported as the rule it was generated from.

**Commits are announced in commit order, with their own version.** A commit's
version was re-read, and its notification sent, after the writer's commit lock
was released. A commit landing in between lent its version to the earlier one
and could be announced first: with one commit paused after its commit point
and a second committed meanwhile, the first returned the second's version
(`CommitResult::version` 3 for both), and `session.watch()` and CDC both saw
`[3, 3]`. CDC checkpoints the newest `lsn_end` it delivered, so the sequence
could also go backwards. The version is now taken, and the announcement order
fixed, under the lock; `CommitResult::version`, `CommitNotification::version`
and `CdcBatch::lsn_end` are each the commit's own, and `lsn_end` never
decreases across deliveries.

**Bulk loads emit trigger and CDC events.** `tx.bulk_writer()` and
`tx.appender()` loads produced no `AfterCommit` trigger rows, no CDC batch and
no commit notification. Once visible, a load is now announced as one commit
notification and emits `NODE_CREATE` (per vertex and label) and `EDGE_CREATE`
(per edge) rows to after-phase triggers and CDC, read back from storage in
chunks of at most 4096 rows. A trigger therefore fires once per chunk, and CDC
receives one or more `CdcBatch`es sharing the load's LSN range, checkpointed
once after the last. `bulk.commit()` returns after the events are delivered.
A load completed in the background after `BulkCommitIncomplete` emits them when
it becomes visible. A load completed by recovery at open emits them too, if the
crash came before they were delivered: the open announces it before returning
and streams its rows in the background, to the triggers that exist at open --
built-in ones and triggers declared with `uni.plugin.declareTrigger`, which the
open reactivates -- and to CDC outputs registered by then (none can be, through
the public API). Plugins registered with `add_plugin` or a `PluginRegistrar`
after the open returns do not see it. The load's marker is kept until the
delivery finishes, so a crash during it delivers the load again at the next
open: at least once, possibly twice, never not at all. A fork's recovered load
fires triggers when the fork is next opened; a read-only open delivers nothing
and leaves the load to the next writable open.

**Cancelling a bulk commit no longer loses or halts its events.** Once a load
is visible, its events are delivered by a task of their own: dropping the
`bulk.commit()` future -- while it waited for its place in the commit order, or
part way through streaming the rows -- used to announce nothing at all (no
trigger fired; CDC never heard of the load) or to cut the load short (the
remaining rows never reached triggers, and every CDC stream halted). The
delivery now finishes whether or not the caller waits for it. A dropped
`commit()` skips the load's index and snapshot finalization, which the next
open completes; that open also delivers the load's events again, so consumers
see them twice. A shutdown during the delivery stops it: CDC halts with its
checkpoint before the load, and the next open delivers the whole load again.

**A commit abandoned after its commit point halts CDC instead of leaving a
hole.** A transaction commit cancelled or failing after it became visible --
for example a `commit()` future dropped at the schema save that follows the
commit -- gave up its place in the announcement order silently: watchers never
heard of the commit, and CDC delivered the next one and checkpointed past it.
It is now announced with `mutations_failed: true`, so CDC halts (as it does
for a commit whose mutations could not be materialized) and watchers see it.

**A bulk load that a synchronous before-trigger would see is refused.** Such a
trigger cannot veto a load that is already visible, so a load writing vertices
under a label (or edges of a type) that a synchronous `BeforeMutation` /
`BeforeCommit` trigger subscribes to now fails with the new
`UniError::BulkLoadBlockedByTrigger`, before those rows are staged (or at
`commit()` for a trigger registered mid-load), and nothing of it becomes
visible. Opt in with `skip_before_triggers(true)` on the bulk-writer or
appender builder to load without running those triggers.

**Versioned reads return the state as of their version.** A snapshot taken
before a `DETACH DELETE` lost the deleted vertex once the delete was flushed
(`MATCH (x) RETURN x.v` gave `[1, 3, 4, 5]` for `[1..5]`), and so did a
transaction begun before it: a flushed vertex delete rewrote the vertex's
stored rows in place. It now appends a tombstone row, as edge deletes always
did. Also fixed, for sessions pinned with `pin_to_version` / `VERSION AS OF`:
a later compaction pass no longer changes what an explicit snapshot reads (it
now reads the table versions its restore point tagged); a traversal's
target-label filter uses the labels the vertices had at the snapshot; and a
schemaless relationship type is read under the snapshot's ceiling (edges
deleted after the snapshot were missing, edges created after it were shown).
A transaction's vertex reads, and a session pinned to a flush manifest, also
survive a compaction pass: compaction keeps the rows any open pin reads. A
session pinned *after* a compaction pass to a flush manifest older than the
history that pass merged is refused with `InvalidArgument`; explicit
snapshots (`create_snapshot`) are never refused.

**A `SET` on stored relationships counts its identity checks toward the
transaction's memory limit.** The per-hub identities a `SET` reads to check
that it does not move an edge are now charged to the transaction buffer, so a
`SET` over a hub with many stored relationships can reach
`max_transaction_memory` sooner (about 40 bytes per relationship of each hub
touched).

**Commits no longer read flushed vertices for triggers that cannot fire.** The
commit-time pre-image read for vertices ran for every written vertex whenever
any trigger was registered, the default label-scoped audit trigger included.
It now runs only for vertices a registered node trigger or CDC can receive,
and never for vertices the transaction created: a 1M-vertex commit beside 1M
stored ones went from 67.5 s to 13.3 s (debug build) with no user trigger.

**A transaction reads edges and properties as of its snapshot.** A
transaction's traversals shared the live adjacency, so it saw an edge committed
after it began (and through it a vertex outside its snapshot) and kept seeing
an edge deleted after it began, through Cypher, Locy, algorithms and
procedures alike; and its edge and vertex property reads used live storage and
a shared cache, so a value another transaction updated and flushed changed
under it. Edge reads are now filtered by the transaction's snapshot, and its
property reads are bounded by it and bypass the shared cache. A transaction
still sees its own uncommitted edges and properties.

Compaction now respects the same snapshot: while a transaction is open, no
pass folds an edge delta committed after it began, since folded edges carry no
version and the transaction would see them. A long-lived transaction therefore
holds newer edge deltas in L1 until it ends. A bulk load does not need its
host transaction to stay open: the load commits on its own, so ending the host
transaction early lets compaction proceed during a long load.

**Promote upsert updates a vertex whose `ext_id` was reused.** After a vertex
was deleted and another created with the same `ext_id`, the `ext_id` lookup
could resolve to the deleted row, so `promote_from_fork_with_options` with
upsert inserted a duplicate instead of updating the live vertex. The lookup
now uses each vertex's newest row.

**Beginning a transaction no longer scales with the database.** Each begin
copied the whole VID-to-labels index: 67 ms at 200k vertices, 636× its cost at
1k. The index is now copy-on-write, and a begin costs about 10 µs at either
size.

**Locy programs are authorized.** No Locy entry point consulted the
registered `AuthzPolicy` chain: with a deny-all policy, `session.locy`,
`tx.locy`, their builders and prepared programs all ran, a `tx.locy` Cypher
write wrote, `tx.locy("CALL uni.admin.snapshot.create(..)")` created a
snapshot, and `tx.apply` replayed a `DerivedFactSet`. Each now asks the policy
before anything runs, for the session's or transaction's principal, with the
program text as the resource path: `read` for what the program evaluates, and
for each command that writes the action Cypher uses for that statement
(`write` for a Cypher write, `DERIVE` or `ASSUME` mutation; `schema` / `dbms`
for a DDL / administrative procedure). `tx.apply` asks `write` for each
statement it replays (path `<locy-apply>`). A refusal is
`AuthorizationDenied`. A policy that denies an action now refuses the Locy
programs that need it.
A `DERIVE` request names what the equivalent Cypher write would — the
patterns its rule (and every rule it reaches by `IS`) matches, as well as what
it creates — so a label-keyed policy cannot be bypassed by deriving instead of
writing.

**`DERIVE MERGE a, b` runs.** It emitted `MERGE (a)-[:MERGED_WITH]->(b)` with
neither variable bound, so every evaluation failed with "MERGE pattern node
must have a label or be a bound variable". Both nodes are now rebound by id
first, as the endpoints of a derived edge are.

**A label write through a second binding of the same node keeps the first.**
`SET` and `REMOVE` of a label rebuilt the node's whole label set from the
labels its row binding carried when matched, so when two bindings of one node
wrote — two variables in a row, or one variable in two rows — the second
undid the first: `MATCH (a:A), (b:A) WHERE id(a) = id(b) SET a:X SET b:Y` left
`[A, Y]`, `REMOVE a:B REMOVE b:C` left `[A, B]`, and a property `SET` through
the second binding put back a label `REMOVE` had just taken off. Label writes
now apply to the node's current labels, both writes take effect, and every
binding of the node returns its final labels.

**The built-in audit trigger fires.** `LabelAuditTrigger`, registered by
default, subscribed to `":_AuditMe"` while event rows carry the bare label
`_AuditMe`, so it never matched. It now subscribes to `_AuditMe`, and a
trigger subscription's `labels` / `edge_types` accept the Cypher form
(`":Label"`) as the same name as the bare one. Commits that write `_AuditMe`
vertices now pay the vertex pre-image read for that trigger.

**A bulk load is invisible to every reader that started before it
committed.** A load reserves its version when it starts staging. A
transaction begun while it was staged saw it appear part way through once it
committed (2 rows, then 22), a session pinned to a flush made meanwhile saw it,
and a snapshot taken while its tables were being published held half of it
(its vertices without its edges), including after a restore. Each now keeps
excluding the load; restoring such a snapshot deletes the load's rows.

**Locy exact probabilities read a PROB rule without a fold as its own
formula.** In `exact_probability` mode, a fact of a PROB rule without a FOLD
(`YIELD KEY a, KEY b, e.p AS p PROB`) that another rule references (`IS` or
`IS NOT`) was one independent variable. Its dependence on the edges and facts
it rests on was lost. On the diamond `s -0.7-> t`, `s -0.3-> m -0.5-> t`,
`reach(s, t) AND NOT edge(s, t)` gave 0.2235, where the possible-worlds value
is 0.045. Exact mode now composes the fact's own lineage: its row's event, the
facts it references, and the negation of the facts it negates. The fact's own
value is exact too. `MATCH (a)-[e]->(b) WHERE a IS reach TO b YIELD KEY a,
KEY b, e.p AS p PROB` gave `0.7 × 0.745` for `(s, t)` and now gives 0.7. A row
derived several ways, or one reading shared evidence, raises
`SharedProbabilisticDependency` in default mode. Default mode is
unchanged. It still reports `SharedProbabilisticDependency` where the
independence approximation is not exact.

**EXPLAIN RULE lists every referenced fact.** A derivation's children used to
be only its probabilistic references (to a PROB rule, from an `MNOR`/`MPROD`
rule). A reference to a rule without a PROB column, or from a PROB rule without
a FOLD, showed no child, so a transitive closure's tree was flat. Every
positive reference is now a child. A fact of a rule without a PROB column is
certain: its derivations have `proof_probability` 1.0 (it was `None`), and a
derivation of such a rule that references PROB facts weighs the product of
their probabilities.

**ABDUCE explains the clause and row a fact came from.** The tree ABDUCE
builds (EXPLAIN's Mode B) took the first clause, in text order, with a body row
sharing the fact's KEY. For a PRIORITY rule or a BEST BY rule it therefore
explained another clause's or another row's derivation, and `ABDUCE NOT`
proposed edits that left the fact in place. It now tries clauses highest
PRIORITY first and matches the fact's non-KEY yielded values too. A removal
under `[e:E|G]` names the type of the edge that was read (it named `E`, and
never validated).

**`ParameterMissing` names a registered rule's parameters up front.** A
parameter read only by a rule registered earlier (`db.rules().register(..)`)
was reported when its clause first evaluated it, naming that one parameter. A
prepared Locy program checked no parameter up front. Both now refuse before
evaluation, naming every missing parameter: the program's own, then those of
each registered rule it runs.

**Locy budget: an in-memory QUERY stops at the deadline, and a late answer is
reported.** A QUERY over the derived facts checks the program's deadline and
cancellation every 1024 rows. With `allow_partial`, a command that finished
past the deadline used to be returned with `incomplete` unset. It now keeps its
answer but reports a `timeout` `incomplete`, and no later command runs.
Without `allow_partial`, converting and hydrating the facts stop at the
deadline, and the `Timeout` comes sooner. With `allow_partial` they still run
to completion, possibly past the deadline. The result promises complete,
hydrated facts, and a deadline-cut run starts this work after its deadline.
The overrun is reported.

**Mutating built-in procedures are refused in read-only contexts and authorized
as `dbms` / `schema`.** The built-in `uni.admin.compact`,
`uni.admin.snapshot.create`, `uni.admin.snapshot.restore`,
`uni.admin.verifyEdgeEndpoints` and the DDL procedures
`uni.schema.create{Label,EdgeType,Index,Constraint}` /
`uni.schema.drop{Label,EdgeType,Index,Constraint}` are dispatched by name, not
through the plugin registry, and the read-only check asked only the registry
for a procedure's mode, so it took every one of them for a read. Measured
before the fix: `session.query()`, `query_with().fetch_all()` and a session
`prepare()` ran all of them (a restore rewound the store); a session pinned with
`pin_to_version` and a handle opened with `.read_only()` ran `compact()` and the
DDL procedures and wrote the catalog on disk (`Person` dropped, `Probe`
created); `VERSION AS OF` statements ran `compact()`, `createLabel` and
`dropLabel`; a `session.locy()` program ran `compact()`, `snapshot.create()` and
the DDL procedures at once; and an `AuthzPolicy` saw the action `read` for all
of them in `session.query`, `session.run` and `tx.query` (`write` in
`tx.execute`). Now:

- Each built-in's mode lives in one table, `uni_query::BUILTIN_PROCEDURE_MODES`
  (`Dbms`: compact, snapshot.create/restore, verifyEdgeEndpoints; `Schema`: the
  DDL procedures; `Read`: `compactionStatus`, `snapshot.list`). The read-only
  check, the authorization verb and dispatch all read it; a built-in without an
  entry is not callable.
- `session.query()`, its builder, session `prepare()`, pinned sessions,
  read-only handles and time-travel statements refuse them with the read-only
  `UniError::Query`. `compactionStatus()` and `snapshot.list()` still run there.
- **Run them with `session.run()`** (new in Python: `Session.run` /
  `AsyncSession.run`). An administrative (`Dbms`) statement runs outside any
  transaction — `snapshot.restore()` and `verifyEdgeEndpoints()` refuse to run
  inside one — authorized as `dbms`; on a pinned session or a read-only handle it
  fails with `UniError::ReadOnly`. A DDL procedure runs in an auto-commit
  transaction, authorized as `schema`. `uni query` (the CLI) already used
  `session.run()`.
- In a transaction, `tx.query` / `tx.execute` / `tx.prepare` authorize a
  statement calling a `Dbms` procedure as `dbms` and one calling a `Schema`
  procedure as `schema`, for registered plugin procedures as for built-ins.
- A session-level Locy program, and any `ASSUME` body, refuses a Cypher command
  calling a `Schema` or `Dbms` procedure: its effect is immediate and cannot be
  collected for `tx.apply` or discarded.
- A session `prepare()` now refuses a registered `WRITE`/`SCHEMA`/`DBMS` plugin
  procedure, as `session.query()` does. It used the registry-blind check, so a
  prepared `CALL t.mark()` (a declared `WRITE` procedure) executed and wrote.

**An `AuthzPolicy` sees each Cypher statement once, under one verb.**
`tx.execute` authorized the statement and then handed it to the `tx.query`
body, which authorized it again, so a policy saw two identical `write` checks
for one `tx.execute(..)` — an audit policy logged every write twice and a
counting policy charged it twice. Every Cypher entry point — `session.query` /
`run`, the session and transaction builders (`fetch_all`, `fetch_one`,
`execute`, `run`, `profile`, `explain`, `cursor`), `tx.query` / `tx.execute`
and prepared statements — now makes exactly one check per statement. Inside a
transaction a pure read is checked as `read` on every entry point; before,
`tx.query` said `read` while `tx.execute`, the transaction builders and
`tx.prepare` said `write` for the same read.

**Procedures called from Locy, prepared statements, transaction builders and
cursors see the caller's principal.** Procedure capability gates read the
principal from a task-local that each entry point installs around execution.
`session.locy` / `locy_with().run()`, `tx.query` and `tx.execute` installed
it; `tx.locy`, `tx.locy_with()` (`run`, `profile`), `session.locy_with().profile()`,
`prepare_locy` (session and transaction), `prepare` (session and transaction)
and the transaction builders (`fetch_all`, `execute`, `run`, `profile`,
`cursor`) did not, so a procedure they called saw no principal and a
capability-gated procedure was refused for a principal that held the
capability. A cursor installed it only while being built, not while it streamed,
so a procedure reached on a later poll saw no principal and no session-scoped
plugins. All of them now run under the caller's principal, and a cursor
re-installs the principal and session plugin registry around every poll.

**A Locy program's commands run in sequence.** A command now reads the state
every earlier command of the same program left. A `DERIVE` or a Cypher write
changes the graph, and every later command — `QUERY`, another `DERIVE`,
`EXPLAIN RULE`, `ABDUCE`, `ASSUME`, `VALIDATE`, a Cypher read — reads the
changed graph *and the rule facts derived from it*. Before, every command read
the facts derived before the first command ran: `DERIVE link` followed by
`QUERY linked` returned the `affected` count and no `QUERY` rows, through both
`session.locy` and `tx.locy`, while a Cypher read after the `DERIVE` saw the
edges. A program now behaves exactly like its commands run one by one, each as
its own `tx.locy`, in one transaction — the same contract ASSUME bodies got
above. After a write, only the strata it can affect are derived again, and only
when a later command reads facts; a program without a write command derives
nothing again, and nothing is derived after a program's last write.
`LocyStats::sequenced_strata_evaluated` / `sequenced_strata_reused` (Python:
`stats.sequenced_strata_evaluated` / `sequenced_strata_reused`) count it.

- A Cypher write as a top-level Locy command works. Ahead of the program's
  first `DERIVE` it failed with "Mutation context not set". In `tx.locy` it
  writes into the transaction. In `session.locy` it writes nothing to the
  database: it lands in the program's overlay, which later commands read, and is
  returned in `derived_fact_set` next to the `DERIVE` output, in program order,
  so `tx.apply` replays the whole program in one transaction.
  `DerivedFactSet::is_empty()` (and Python's `is_empty()`) is now false for a
  set holding only such a write.
- A session-level `DERIVE` that connects a node an earlier command of the same
  program created (`DERIVE (NEW ...)`, a Cypher `CREATE`) fails with an
  evaluation error. `tx.apply` would re-create that node under a new id and the
  `DERIVE` statement, which finds its endpoints by id, would silently match
  nothing. Use `tx.locy` for such programs.
- `LocyResult::derived` still holds the facts the program derived before its
  first command.
- A prepared Locy program (`session.prepare_locy` / `tx.prepare_locy`) now runs
  against an overlay like `session.locy`. Its `DERIVE` was invisible to its
  later commands, its `ASSUME` failed with "no active Locy L0 to fork", and a
  Cypher write in it went straight to the shared graph, outside any
  transaction.
- Re-derivation after a write, inside ASSUME bodies too, now plans
  model-invoking rules with the classifier registry, as the program's own run
  does.

**Edge triggers see the edge's old properties after a flush, and delete
triggers match their edge type.** An edge already flushed to storage was
reported to triggers and CDC as `EDGE_CREATE` with no `old_value` when it was
next updated (`SET`, `REMOVE`, `MERGE ... ON MATCH SET`), and as an
`EDGE_DELETE` with no `old_value` when deleted directly or by `DETACH DELETE`
of an endpoint; `old.<prop>` predicates on such edges never matched. An edge
updated after a flush reported only the keys of that later write. The
pre-image is now the edge's whole committed property set, declared,
undeclared and schemaless, read in one batched lookup per commit. Edge deletes
were also labelled `type:<id>` instead of the type name, so a trigger
subscribed to `-[:R]` never fired on deleting an `R` edge; they now carry the
name.

**`partial_lance_writes` no longer applies to edges.** With the flag on, an
edge update written through the writer's non-transactional API was flushed as
a Lance `MergeInsert` of the touched columns into the edge's delta rows. An
edge created and updated before one flush was dropped from storage (absent
after reopen); a `REMOVE` followed by a `SET` of the same edge before one flush
failed the whole flush ("ambiguous merge insert"); and an edge with several
stored versions had all of them rewritten to the same version, leaving its
current state to scan order. Edge writes are now always stored as full rows.
Cypher writes were not affected: the transaction commit already stored edges
as full rows. Measured on 1,000 edge updates, a flush took 28-33 ms as full
rows against 37-43 ms as `MergeInsert`.

**`partial_lance_writes` no longer applies to vertices either; the flag is
inert.** With the flag on, a vertex update written through the writer's
non-transactional API (`Writer::insert_vertex_partial_full` /
`insert_vertex_partial`) flushed as a Lance `MergeInsert` of the touched
columns, keyed on the vertex id, into a table that holds one row per flushed
version of the vertex. Properties removed earlier in the same flush window
came back after the flush; a property removed in an earlier flushed row came
back after compaction; and a vertex created and updated in one flush window
was gone after the flush. Every vertex write is now stored as a full row.
Cypher writes were not affected (the transaction commit never carried the
partial classification). `uni_partial_writes_total`, which counted calls that
then wrote full rows anyway, is no longer emitted.
`UniConfig::partial_lance_writes` and `WriterConfig::partial_lance_writes` are
now `#[deprecated]` no-ops: they still compile and are accepted, and setting
`partial_lance_writes = true` logs one warning per process when a database
opens. The flag was never reachable from Python (`config()` rejects the key).

**An entity re-emitted by a writing subquery keeps its identity.** After
`MATCH (n) CALL { WITH n SET ... } RETURN ...`, `properties(n)` and
`collect(n)` returned `null` and `n.name` could read `null`, for any entity.
A flattened entity map (from that subquery, or a parameter carrying `_vid` and
`_labels`) whose map-valued property is named `properties` no longer has that
map read as its whole property set: the other properties were dropped, or the
nested keys appeared as properties.

**`uni.schema.labels` `nodeCount` counts what the caller sees.** It was the
row count of the label's storage table: unflushed nodes and a transaction's
own writes were missing, deleted and superseded versions were counted, and on
a fork it read whichever table the branch resolved to. It now counts the
label's live nodes in the calling query's view (one count per label per call).

**`ExecuteResult` counters report what the statement changed.** Every update
reported itself as a create: `MATCH (n) SET n.v = 7` returned
`nodes_created = 1`, `labels_added = 1`, `properties_set = 3`;
`MERGE … ON CREATE SET` returned `nodes_created = 2`; `REMOVE n.k` returned
`nodes_created = 1`, `labels_added = 2`; and `SET n:L` / `REMOVE n:L` counted no
labels. The counters now follow the openCypher TCK's side effects: a property
added counts in `properties_set`, a property removed (or set to null) in the
new `properties_removed`, an overwrite with a different value in both, and
assigning the value a property already has in neither. Labels count per node
whose label set changes. A node or edge created and changed in the same
statement reports only its final state. `properties_removed` counts explicit
`REMOVE` / `SET ... = null` (and overwrites) only; a deleted entity's
properties are not counted, and labels count per node, as in Neo4j's query
statistics. (The openCypher TCK's side effects are a graph diff its harness
takes, not execution counters.) A node or relationship named twice in one
`DELETE` is now counted once: `DETACH DELETE n, m` with `m = n` reported
`nodes_deleted = 2`. Python: `ExecuteResult.properties_removed`.

**Declared edge endpoints are enforced on write.** An edge type declared with
source or target labels (`edge_type("R", &["A"], &["B"])`) now refuses an edge
whose source or target carries none of them, on `CREATE`, `MERGE`,
`bulk_insert_edges`, `COPY … FROM` and fork promote, with the new
`UniError::EdgeEndpointViolation { edge_type, endpoint, labels, allowed }`
(Python: `UniEdgeEndpointError`, a `UniConstraintError`). A `REMOVE n:L` that
would leave one of the node's existing edges outside its declaration is
refused with the same error. Until now nothing checked, while the planner
narrows patterns by the declaration; stores written before this release may
hold violating edges, and they stay readable. An edge type declared with no
endpoint labels, or not declared, accepts any endpoint.

**`COPY … FROM` inside a transaction is part of the transaction.** It wrote
straight to the shared buffer and flushed to disk: the transaction did not see
its own imported rows, other sessions saw them before the commit, and a
rollback left them in place. The rows now go to the transaction: visible only
to it, discarded on rollback, durable on commit, and an import larger than
`max_transaction_memory` fails with the transaction memory error.

**Declared `VECTOR` and float properties read the same before and after a
flush.** A list literal written to a `VECTOR` column and an integer written to
a `FLOAT` column were held as written until the next flush, so `RETURN n` and
`properties(n)` showed `List[Int]` / `Int` before it and `Vector` / `Float`
after. They are now converted when written. `n.e[0]` on a vector property
returns the element instead of failing with "cannot index into Vector", and
`n.e[1..3]` returns the slice as a list of floats instead of failing with
"first argument must be a list" / "Cannot slice Vector" -- in a read and in a
write expression such as `SET n.x = n.e[0]`, which failed on both.

**The write throttle is charged once per statement or batch.** In the soft
band every written row paid the throttle budget: with a 1 s budget a four-row
`UNWIND … CREATE` waited 4 s, a two-node one-edge `CREATE` 3 s, and
`bulk_insert_edges` of three edges 3 s. Each now waits once. A direct
`Writer` call outside a transaction still pays per call.

**Bulk inserts honour the statement timeout.** `Transaction::bulk_insert_vertices`,
`bulk_insert_vertices_labeled`, `bulk_insert_edges`, and the promote helpers
`update_vertex_properties` / `delete_vertex_by_vid` are bounded by
`query_timeout` like a Cypher write; at the throttle's hard limit they used to
block until compaction ran. A batch cut short leaves the transaction
rollback-only.

**With compaction disabled, writes at the hard limit fail at once.** With
`compaction.enabled = false`, a write at `throttle.hard_limit` used to wait
until its deadline and fail with `Timeout`, and soft-band writes waited for a
compaction that never came. A write at the hard limit now fails immediately
with the new `UniError::WriteStalled { l1_runs, hard_limit }` (Python:
`UniWriteStalledError`), whose message names `CALL uni.admin.compact()`, and
the soft band does not wait.

**Updating a flushed edge no longer drops or restores properties.** A `SET` on
an edge that had been flushed lost the edge's undeclared properties once the
result was flushed again, and a property removed with `REMOVE` (then flushed)
came back on the edge's next `SET`. The read that seeds an edge update now
includes the undeclared properties and takes the newest stored row as the
edge's state instead of merging older rows into it key by key.

**A bulk load's `commit()` now says whether the load was applied, and a crash
during it no longer loses other sessions' writes.** A load is decided by one
durable write inside `commit()`, its commit point (the recovery marker that
records every staged row):

- Before it, a failure returns a plain error, and the load is not applied:
  nothing is visible, now or after a reopen, and its staged files are deleted.
- After it, `commit()` finishes the publish in-process, retrying a failed table
  through the same exactly-once path recovery uses, and returns `Ok`. Later
  steps that do not decide the load (index builds, the snapshot manifest and
  latest pointer, the index-status schema save, the marker delete) no longer
  fail it; they are listed in the new `BulkStats::warnings` and completed by
  the next open or the index rebuild worker.
- If the publish still cannot be finished, `commit()` returns the new
  `UniError::BulkCommitIncomplete { load_id, .. }` (Python
  `UniBulkCommitIncompleteError`): committed, completed exactly once on the
  next open, **do not retry**.
- If writing the marker fails in a way that may still have written it and
  reading it back does not settle it, `commit()` returns the new
  `UniError::BulkCommitOutcomeUnknown { load_id, .. }` (Python
  `UniBulkCommitOutcomeUnknownError`).

Before, every one of about fifteen steps after the commit point returned a
plain error although the next open always completed the load. Measured: a
failure part way through the publish, followed by the retry such an error
invites, left 40 vertices and 60 edges after reopen for a 20-vertex, 30-edge
load; a failure after the whole load was published left it doubled at once;
and after a mid-publish failure live readers saw the vertices under
`MATCH (n:Label)` (20) but not under `MATCH (n)` (0) until the next open. The
`uni-pydantic` OGM wrapped every such error in `BulkLoadError`, and
`bulk_add` committed one load per label, so a failure on the second label left
the first applied behind that retry-inviting error. `bulk_add` now commits all
labels as one load and lets the two new exceptions propagate unchanged.

The load's id is exposed as `BulkWriter::load_id()`, `BulkStats::load_id`, and
`writer.load_id` / `stats.load_id` in Python, and the new
`Uni::bulk_load_applied(load_id)` (Python `Uni.bulk_load_applied`,
`AsyncUni.bulk_load_applied`) says whether a load reached its commit point,
before or after a reopen. It reads the load's marker and, once that is gone, a
small receipt written per load under `catalog/bulk_loads_applied/`.

Recovery completes an interrupted load instead of restoring each table's
pre-load version, which also discarded what other sessions had committed and
flushed to those tables meanwhile (measured: a vertex and an edge committed
during the commit, or after a failed one, were gone after reopen). Each load
keeps its own recovery marker: with one marker per database, a second load
overwrote or deleted the first's, which then stayed half-visible (vertices and
one edge direction) for good. A crash during recovery is completed by the next
open.

Staged files of a load that is still open, or committed but left to the next
open, are now held against version cleanup in the process. Lance deletes a
data file no version references once it is 7 days old; a compaction pass in a
long-running process could delete them, and recovery then read "every staged
file gone" as "already published" and skipped the tables silently (measured
with aged files: all 30 edges of a committed load missing after reopen).

**A storage backend that cannot stage is refused a bulk load.**
`BulkWriterBuilder::build` returns the new `UniError::BulkLoadUnsupported`
(Python: the base `UniError`) before anything is written, instead of writing
visibly and undoing an abort or a crash by rolling each table back to an older
version, which also discarded other writers' commits. Every Lance-backed
database and fork stages, so only custom `StorageBackend` implementations are
affected. `BulkWriter`'s futures are now `Send` (the progress callback is kept
behind a mutex), so a load can run on a spawned task. New:
`uni_bulk::check_recovered_bulk_endpoints`, called at open after WAL replay.

`BulkStats` gained two public fields (`load_id`, `warnings`); code that builds
it with a struct literal needs `..Default::default()`. A schema installed with
`load_schema` (in memory only) is now saved before the commit point rather than
after the publish, so a load's labels cannot be lost while its rows are kept.

**`=~` matches the whole string, as in Neo4j.** The regex operator was a
partial ("find anywhere") match, so `'abc' =~ 'b'` was true. It now follows
Java's `Pattern.matches`: the pattern must match the entire string, so
`'abc' =~ 'b'` is false and `'abc' =~ '.*b.*'` is true. Anchors `^`/`$` are now
redundant; the end is absolute, so `'abc\n' =~ 'abc'` and `'abc\n' =~ 'abc$'`
are false (also under `(?m)`). Alternation is decided over the whole string
(`'ab' =~ 'a|ab'` is true) and inline flags such as `(?i)` and `(?s)` still
apply. This holds on every path: literals, parameters, typed and schemaless
properties (in memory and flushed), the fallback executor and Locy rule
bodies and `QUERY … RETURN`. NULL handling is unchanged. To keep a substring
search, put `.*` on both sides of the pattern (`'.*b.*'`). The `uni-pydantic`
`PropertyProxy.like()` filter is `=~` and changes the same way.

**`=~` patterns are Java regular expressions, as in Neo4j.** Patterns were
read with the Rust `regex` crate's syntax and defaults. They now follow
`java.util.regex` with no flags, so these answers change:

- `\w`, `\d`, `\s`, `\b` and POSIX classes such as `\p{Lower}` are ASCII:
  `'é' =~ '\\w'` is now `false`. `(?U)` makes them Unicode.
- `(?i)` folds ASCII letters only (`'É' =~ '(?i)é'` is now `false`); add
  `(?u)` for Unicode case folding, which follows Java's rules.
- `.` no longer matches `\r`, U+0085, U+2028 or U+2029 (without `(?s)`).
- `$` also matches before a final line terminator: `'a\n' =~ 'a$\\n'` is now
  `true`.
- `\v` is vertical whitespace (it was a vertical tab), `\<` and `\>` are the
  characters `<` and `>` (they were word-boundary assertions), and
  `[[:alpha:]]` is a class of the characters `:alpha` (it was the POSIX class).
- `\p{..}` names resolve as in Java: `\p{Greek}` and `\p{Alphabetic}` are now
  errors (write `\p{IsGreek}`, `\p{IsAlphabetic}`).

Java constructs are now evaluated with Java's meaning rather than rejected:
look-ahead and look-behind, backreferences, atomic groups and possessive
quantifiers (`a*+` was read as `(a*)+`) run on a backtracking matcher, and
`\Q…\E`, `\h`, `\R`, `\Z`, `\G`, `\e`, `\cX`, octal escapes, `(?d)`, `(?i-i)`
and stacked quantifiers (`x{1}{2}`) parse as Java parses them. Patterns
without those constructs, `\b` or anchors inside the pattern or under `(?m)`
run on a linear-time automaton; the choice is made per pattern, so no input
is refused. Rust-only syntax is
rejected (`(?P<name>…)`, `\u{..}`, `[a--b]`, `a{,3}`, `(?R)`).

A match that needs more than `UniConfig::regex_backtrack_limit` backtracking
steps (default 10,000,000) fails with the new `UniError::RegexBacktrackLimit`
(`UniRegexBacktrackLimitError` in Python, carrying `pattern` and `limit`), and
a long scan of `=~` rows now honours the query timeout. Every pattern Java
accepts is accepted and answered as Java 21 answers it: patterns with
look-around, backreferences, atomic groups, possessive quantifiers, `\X`,
`\b{g}` or `(?c)` run on a backtracking matcher over the string's UTF-16
units that follows Java's semantics, so case-insensitive
backreferences (`'kK' =~ '(?i)(k)\1'` is `true`), backreferences inside the
group they name or into a loop that can match empty, captures kept after
backtracking past a look-ahead, Java's end-at-the-first-empty-iteration loop
rule (`'a' =~ '(?:a|\G){2}'` is `false`), and look-behinds with word
boundaries, look-arounds or supplementary characters all give Java's answer.
Unicode blocks (`\p{InGreek}`), `\p{java…}`, `\N{…}`, `\X` (grapheme
clusters), `\b{g}` and `(?c)` (canonical equivalence) are supported. Where
Java's matcher itself throws on an input (`'ab' =~ '(?=ab)a\b{g}b'` reads past
the end of the string), the query fails with an error naming that exception,
as it does in Neo4j. Java throws `StackOverflowError` on a backtracking loop
over a long string (`(?:a|b)*` past about 1,500 characters on a default
thread stack); uni-db answers such inputs and fails with "Regex recursion
limit exceeded" only when its backtrack stack outgrows a 64 MB budget (a
2,000,000-character `(?:a|b)*` match is answered). A pattern nested
more than 2,000 groups deep is rejected with Java's "Stack overflow during
pattern compilation". See the Cypher querying guide.

The regex semantics are pinned to Java 21 (Unicode 15.0), the minimum JVM the
current Neo4j release supports: character classes, properties, scripts and
`(?iu)` case folding use tables generated from JDK 21, no longer the Rust
toolchain's (Unicode 17.0) or `regex-syntax`'s (Unicode 16.0). Characters
assigned in Unicode 16.0 or later are unassigned (`'\u{1C8A}' =~ '\p{L}'` is
`false`), their case pairs no longer fold (`'\u{A7CF}' =~ '(?iu)\u{A7CE}'` is
`false`), and the scripts added in 16.0 (`\p{IsGaray}`, `\p{IsTodhri}`, …)
are invalid names, as in Java 21. A pattern with a backreference inside its
own group could panic the backtracking matcher (`(?:x(\1?a))+`); it is now
answered as Java answers it.

**Integers and floats compare exactly; `1` and `1.0` are one value.** An
integer compared with a float was widened to `f64`, which rounds above 2^53:
`9007199254740993 = 9007199254740992.0` was true, and orderings across the
boundary were wrong. This held on every path: literals, parameters, typed and
schemaless properties, the row evaluator, joins, `IN`, `ORDER BY` and
`min`/`max`. Comparisons are now exact, and the integer form of
a float bound keeps index pushdown (`n.i < 2.5` is `n.i <= 2`).

- `DISTINCT`, grouping, `UNION`, `count(DISTINCT)` and `collect(DISTINCT)` over a
  column mixing integers and floats now treat `1` and `1.0` (and `0`, `0.0`,
  `-0.0`) as one value, and keep the first one seen. They were separate rows.
- `RETURN 1 AS x UNION RETURN 1.0 AS x` failed to plan ("mismatched schemas").
  An integer column against a float column now unions as Cypher values, each
  row keeping its type.
- An inline edge map on a variable-length path over a schemaless edge type
  (`[:R*1..2 {w: 0}]`) and a node map in a pattern predicate
  (`WHERE (a)-->(:B {w: 1})`) compared structurally: `{w: 0}` missed `w = 0.0`,
  and a `NaN` matched a `NaN`. In a pattern predicate, `{k: null}` matched a
  node with no `k`; it now matches nothing. Both use Cypher equality now.
- An aggregate over `DISTINCT` sees the first value of each class, as
  `RETURN DISTINCT` keeps it. `sum(DISTINCT x)` over `[1.0, 1]` was `2.0` (the
  two were not deduplicated) and is `1.0`; `collect(DISTINCT x)` over
  `[1.0, 1]` was `[1]` and is `[1.0]`; `min`/`max(DISTINCT)` likewise.
- `MERGE (:K {k: 9223372036854775808.0})` matched an existing node with
  `k = 9223372036854775807`: the float `2^63` was folded into `i64::MAX`.
  It now creates the node. Batched `UNWIND ... MERGE` deduplicated the two the
  same way.
- A statement run whole by the fallback executor (a standalone `CALL` of a
  registered procedure, followed by `RETURN`) compared numbers by widening
  too, and folded `2^63` into `i64::MAX`: `min`/`max`, `DISTINCT`, grouping
  and `UNION` were wrong at the boundary. On that path only `count` honored
  `DISTINCT`; `sum`, `avg`, `min`, `max`, `collect` and the percentiles
  folded every row. Both are fixed.
- Locy: a `QUERY`'s `ORDER BY`, `min`/`max` and `DISTINCT` (including
  `count(DISTINCT)` and `collect(DISTINCT)`) compare numbers exactly and
  treat `1` and `1.0` as one value, as Cypher does. `ORDER BY` and
  `min`/`max` also order values of different types as Cypher does; they tied
  and kept arrival order.
- Locy: `BEST BY` over a column of mixed or schemaless numbers chose by the
  values' encoded bytes, so every integer ranked below every float
  (`BEST BY v` over `5` and `1.5` chose `5`) and negative numbers ranked
  above positive ones. It now chooses by value, in recursive rules too,
  where a new candidate is weighed against the fact already held for its KEY.

**Cypher operators answer the same on literals and on properties, per
openCypher.** Literals are typed statically and evaluated by Arrow kernels;
schemaless properties are evaluated per row. The two paths disagreed, and one of
them was often wrong:

- `n.missing AND null` panicked. `null AND n.b`, `'ab' + null`, `true - null`,
  `duration('P1D') * null`, `1 =~ n.missing` and similar failed to plan, and
  `'ab' ^ n.missing` failed at run time. All are now NULL.
- `n.s =~ 'ab'` over a schemaless property failed to plan. It now matches.
- `true OR n.i` over a non-boolean `n.i` was true when every row of the batch
  had a true left operand, and a type error otherwise. `AND`/`OR` now follow
  Kleene logic per row, in either operand order. A dominating operand decides
  the answer whatever the other one is: `false AND x` is false and `true OR x`
  is true. Otherwise a non-boolean operand is a type error, beside NULL too:
  `NULL AND n.i` and `n.missing XOR n.i` are now errors where the row path
  answered NULL.
- `2 ^ 3` is `8.0` everywhere. The interpreted evaluator, which Locy also uses,
  answered `8`.
- `date - date` is a duration on the literal path too. It was an integer day
  count.
- `datetime + duration`, `datetime - datetime` and `duration * number` now plan
  over literals.
- `n.s + 1` concatenates (`'ab1'`) on the row path, as the literal path already
  did. A float or a duration concatenates its `toString`: `'a' + 1.0` is
  `'a1.0'` and `'a' + duration('P1D')` is `'aP1D'`, not `'a1 days'`.
- A number is no longer read as a duration in microseconds. `datetime(..) + 1`
  and `date(..) - 1` are type errors on every path. Before, the row path moved
  the value by 1 µs, so `date - 1` returned the previous day.
- `STARTS WITH` / `ENDS WITH` / `CONTAINS` with a non-string operand is NULL in
  the interpreted evaluator too. It was an error there.
- `localdatetime(..) = datetime(..)` was true for equal instants on literals.
  `'ab' = localdatetime(..)` failed parsing `'ab'` as a timestamp. Both are now
  incomparable types (`=` false, `<` NULL), as the row path answered. A zoned
  `TIMESTAMP` column still compares with a datetime and a string as an instant.
- `-localdatetime(..)` negated the epoch offset. It is now a type error.
- **A silent wrong answer.** `n.dur * n.k`, with a schemaless duration and a
  declared `INT` column, was NULL. A vectorized fast path treated every
  non-numeric value as NULL. It now hands such a batch to the evaluator, which
  answers the duration (or the type error, for `n.s - n.k`).

**A `shortestPath` pair with no path is dropped by a mandatory `MATCH`, as in
openCypher.** `MATCH p = shortestPath((a)-[:R*]->(b))` (and `allShortestPaths`)
used to return a pair whose endpoints are not connected as a row with `p` NULL.
It now returns no row for that pair, as openCypher and Neo4j do; `OPTIONAL MATCH`
keeps the row with `p` NULL. The same applies to a NULL endpoint and to a source
equal to its target under `*1..` (a node reaches itself only through the
zero-length path of `*0..`). A query that counted rows to count candidate pairs,
or tested `p IS NULL` after a mandatory `MATCH`, should use `OPTIONAL MATCH`.

**`OPTIONAL MATCH` with an unbound `shortestPath` target is null-extended per
entering row.** `MATCH (a) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:N))`
returned one NULL-path row for every `N` node `a` does not reach, beside the ones
it does. It now returns only the reachable targets, or a single row with `b` and
`p` NULL when `a` reaches none of them. An entering row with no candidate target
at all (an empty label, or a property map nothing satisfies) used to be dropped;
it now gets its NULL row.

**`n.prop ~= q` returns as many neighbours as the `LIMIT` it feeds.** The
search was capped at a hardcoded 100, so `RETURN n LIMIT 500` silently returned
100 rows and `SKIP 100 LIMIT 20` returned none. `K` is now `SKIP + LIMIT` when the
search feeds that `LIMIT` directly (projections, or `ORDER BY n._score DESC`);
elsewhere — no `LIMIT`, a residual filter such as `~= q AND n.x > 3`, a traversal,
another `ORDER BY` — it is the new `UniConfig::vector_knn_default_k` (default 100,
also a Python `config()` key), and a search that fills it adds a result warning.
`~=` on a variable it cannot replace the label scan of (bound behind a `LIMIT`, a
relationship endpoint, a row-dependent query vector, under `OR`) is now a planning
error naming `vector_similarity`; `MATCH (n) WITH n LIMIT 5 WHERE n.emb ~= q` used
to return the 5 nearest neighbours of the whole label. Under `OPTIONAL MATCH`, `~=`
now uses the index and yields a NULL row when nothing qualifies (it was
previously left unrewritten, reaching an expression compiler that does not
support `~=`).

**Equality on an indexed property no longer loses or rejects edge-case
operands.** On a property with a scalar index, `n.s = '2021-06-01T00:00Z'` on a
`STRING` property returned no rows (the literal was rewritten as a timestamp
before reaching storage), `n.f = -0.0` on a `FLOAT` property missed the `-0.0`
row, `n.f = $p` with `$p` NaN or infinite failed, and `n.x = $p` with `$p` the
smallest 64-bit integer failed with `could not convert to literal of type
'Int64'`. All four now return what the unindexed query returns.

**`-null` is null in `SET` expressions.** `SET n.f = -n.f` failed with
`Cannot negate non-numeric value: Null` on a row where `f` is NULL; it now sets
NULL. Negating the smallest 64-bit integer is an overflow error instead of a
wrapped value.

**Float comparisons follow openCypher, not IEEE total order.** `-0.0 = 0.0`
was false and `-0.0 < 0.0`, `0 > -0.0` true; `n.f < 0.0` returned a row holding
`0.0 / 0.0` (a NaN with its sign bit set sorted below every number), `n.f =
n.f` held for a NaN row, the join `n.f = m.f` paired a NaN row with itself and
missed `-0.0` with `0.0`, and `-0.0` and `0.0` were two groups under
`DISTINCT`, grouping, `count(DISTINCT …)` and `UNION`. Now `-0.0` equals `0.0`,
every comparison involving `NaN` is false except `<>`, and `-0.0`/`0.0` (and all
NaNs) are one group; a group of zeros shows `0.0`.

**A list mixing integers and floats keeps each element's type.** `RETURN $l`
with `$l = [1, 1.5]` returned `[1.0, 1.5]` (so did the literal `[1, 1.5]`), and
with `$l = [9007199254740993, 1.5]` both `9007199254740992 IN $l` and
`n.x IN $l` for `x = 9007199254740992` matched, because the integer was rounded
to a float. Such lists now keep their integers; numeric lists passed as query
vectors (`~=`, `vector_similarity`, `uni.vector.query`, VECTOR writes) answer
as before.

**`WITH … ORDER BY … SKIP … LIMIT … WHERE` filters after the page.**
`UNWIND range(1, 10) AS x WITH x ORDER BY x LIMIT 3 WHERE x > 2` returned
`[3, 4, 5]` (the filter ran before `LIMIT`); it now returns `[3]`, as the
openCypher grammar and spec define. A `WITH DISTINCT` whose `WHERE` follows a
`SKIP`/`LIMIT` and reads a variable the projection dropped is now a
`VariableNotDefined` error.

**A list expression over a long variable-length path result no longer fails.**
`[r IN rs | r.k]`, `all(r IN rs WHERE …)` and other comprehensions over a
relationship list failed with "all columns in a record batch must have the same
length" once the traversal produced more than one batch of rows for its input.

**`ABDUCE NOT` validates the edit it reports.** A `RemoveEdge` candidate was
validated by deleting every edge of its type, and a `ChangeProperty` by setting
the property on every node (for an edge variable, on every node instead of the
edge), so a removal that leaves the goal derived could come back `validated`.
Each candidate now carries the identity of the one entity it edits —
`RemoveEdge.eid` / `source_vid` / `target_vid` and `ChangeProperty.vid` / `eid`
(new fields; also in the Python dicts) — and validation applies only that edit.
A relationship with neither a bound variable nor bound endpoints is no longer
proposed. Anonymous nodes and relationships in the rule body still identify
their edge: ABDUCE binds them internally while building its derivation tree
(the first version of this change proposed no edge at all for a body such as
`(l:Lot)-[:OBSERVED]->(:Feature)-[:ON]->(t:Tool)`, which emptied the
semiconductor flagship notebook's ABDUCE). Such a removal reports the anonymous
element's variable as empty and carries its `eid` / vids. Property candidates are no longer repeated under helper names such as
`a._all_props`. `source_var` / `target_var` of a removal now follow the edge's
direction, so `(b)<-[:R]-(a)` reports `a` as the source.

**Positive `ABDUCE` proposes and validates one concrete edge.** An `AddEdge`
candidate named only the rule's variables and was validated by adding an edge
of its type between every pair of nodes, so an edit no single edge achieves
(a two-hop rule) came back `validated`, at a cost quadratic in the node count.
Each candidate is now one edge between two existing nodes, carried in the new
`AddEdge.source_vid` / `target_vid` fields (also in the Python dicts), and
validation adds exactly that edge. Endpoints are enumerated from the nodes
carrying the hop's endpoint labels in id order, filtered by the goal's `WHERE`
when it reads only the hop's endpoint variables, up to
`max_abduce_candidates`. `source_var` / `target_var` follow the edge's
direction, so `(b)<-[:R]-(a)` adds `a -> b`.

**Locy provenance attributes a fact to its clause by column name, and fails
instead of guessing.** EXPLAIN's clause attribution compared a fact with each
clause's candidates by column position and gave clause 0 to a fact it could
not match; the TopKProofs MNOR support map was keyed the same way and, on a
miss, folded the group as independent. Both now resolve columns by name and
fail with an error on a missing column.

**Locy exact probability honours a renamed FOLD output.** With
`exact_probability`, a probabilistic FOLD whose output was renamed in YIELD
(`FOLD prob = MNOR(e.p) YIELD ..., prob AS pr`) never received its exact (BDD)
probability for shared-proof groups: the group silently kept one derivation's
weight (0.7 on the reachability diamond, whose exact value is 0.745 — see
below). The exact path now resolves columns by name and fails with an error
instead of skipping.

**Locy: a positive `IS` reference to a PROB rule carries its probability
(breaking: probability values change).** The row holds only when the
referenced fact does, so the fact's probability is now multiplied into the
calling row's PROB value, as `IS NOT` multiplies `1 - p`; it was a Boolean
filter. On the reachability diamond `s -0.7-> t`, `s -0.3-> m -0.5-> t` with
`FOLD prob = MNOR(e.p)` in both clauses, `reach(s,t)` was
`1 - (1 - 0.7)(1 - 0.3) = 0.79`, which treated `reach(m,t)` as certain; it is
`1 - (1 - 0.7)(1 - 0.3 · 0.5) = 0.745`, the probability that `t` is reachable
when each edge is present independently. With MPROD it was 0.21 and is 0.105.
- **Count once.** A clause whose PROB expression already reads the referenced
  probability as one product factor (`FOLD p = MNOR(p)`,
  `MNOR(e.w * p)`) is unchanged. Any other read (`1 - p`, `p * p`) is not
  multiplied either and raises the new runtime warning
  `ProbabilityReadNotFactor` (`probability_read_not_factor`).
- **Exact mode** (`exact_probability`) evaluates each fact under
  possible-worlds semantics: the OR (MNOR) or AND (MPROD) of its rows, each row
  its own event AND the formula of every fact it references. Two rows over the
  same expression and the same nodes and relationships are one event. It used
  to treat a recursive row as an independent fact of its own weight, so it
  never corrected anything a positive reference shared.
- **`SharedProbabilisticDependency` fires only on shared evidence** — two
  proofs over one edge or one referenced fact, or a cycle — not on any
  recursive clause. The diamond above no longer warns (its proofs share no
  edge); `A -0.5-> B -0.4-> C -0.6-> D` plus `A -0.3-> C` does, and exact mode
  gives `reach(A,D) = 0.264` against the independence 0.2784. Without a
  warning the two modes agree.
- **A renamed MNOR/MPROD output is a PROB column.** `FOLD prob = MNOR(..) YIELD
  prob AS pr` left `pr` unmarked, so neither `IS` nor `IS NOT` read it as a
  probability.

**Locy: `IS NOT` inside exact formulas, `TopKProofs` over lineage, EXPLAIN
shows referenced facts (breaking: probability values change).**
- **Exact mode negates the negated facts' formulas.** In an MNOR/MPROD rule an
  `IS NOT` against a PROB rule was an independent `1 - p` factor even in exact
  mode, so a row needing a path and the absence of an edge on it was wrong.
  On `s -0.7-> t`, `s -0.3-> m -0.5-> t`, `reach(s,t) AND NOT link(m,t)` was
  0.3725 and is `0.7 · 0.5 = 0.35`. Default mode keeps `1 - p`, and now raises
  `SharedProbabilisticDependency` when the negated fact shares an event with
  the rest of the row (it did not, so the wrong value came without a warning).
- **`TopKProofs { k }` is evaluated over lineage.** Each MNOR fact is the exact
  probability of its `k` most probable rows, each row its own event AND the
  facts it references. It used to treat a row's referenced facts as its only
  events, weighted by the largest row, so two edges (0.7, 0.6) into one hub
  both referencing a certain `hub_score(h)` folded to 0.7 instead of 0.88;
  and through the old reference join it saw no reference at all for
  `mid IS reach TO b`, where `mid` is not yielded, so `reach(A,D)` on the
  shared-edge graph was 0.2784 instead of 0.264. With `k` at least a fact's
  row count the value is now the possible-worlds probability.
- **EXPLAIN RULE (recorded provenance) shows the facts each derivation
  references,** expanded into their own derivations where first shown; it
  showed no children at all. The references are the ones exact mode and
  `TopKProofs` evaluate, so a subject the rule does not yield is no longer
  lost. A recursive FOLD rule's derivations are one entry each: two
  derivations with equal values through different facts were shown as one.
  Hidden `__deriv_*` columns no longer appear in EXPLAIN bindings.
- **Two `IS NOT` references to one PROB rule in a clause** each contribute
  their own complement.

**A property named after a storage column is refused at write.** `CREATE`,
`SET`, `MERGE`, `Transaction::bulk_insert_*`, `BulkWriter`, the streaming
appender and the Python bindings now fail with `UniError::InvalidIdentifier`
(Python `UniInvalidIdentifierError`) for a property named `overflow_json`,
`_vid`, `_uid`, `_eid`, `_deleted`, `_version`, `_labels`, `_created_at` or
`_updated_at`. Such a write used to be accepted and then misread: `overflow_json`
read NULL while `properties()` held it, and the `_` names were dropped on a
declared label, hidden from `keys()` on a schemaless one, or shadowed by the
system value (`n._vid` returned the vertex id). The other storage column names
(`ext_id`, `eid`, `src_vid`, `dst_vid`, `op`, `type`, `labels`, `props_json`)
round-trip and stay writable. Schema DDL already refused all of these.

**A property whose name starts with `_` reads back.** Only the engine's own
names are reserved now (`uni_common::core::schema::is_reserved_property_name`,
one rule for writes and reads). Any other `_`-prefixed property — `_source`,
`__tag` — was accepted on write and then hidden by every read path that
treated a leading `_` as internal: it was missing from `keys()`,
`properties()` and `n {.*}` on vertices and relationships, flushed or not,
and `n {._foo}` failed to plan on a declared label. It now appears in every
read form, as `n._foo` and `WHERE n._foo = …` already did. In exchange the
reserved set grew to the map-encoding keys and planner columns the engine gives
a meaning (`_type`, `_type_name`, `_src`, `_dst`, `_src_vid`,
`_dst_vid`, `_all_props`, `_label`, `_score`, `_fwd`, Lance's `_rowid`-family
row columns, `__set_struct__`,
`__with_passthrough__`, and names starting with `_gen_`, `__fde_` or
`__alias_of__`): a write of one is refused with `UniError::InvalidIdentifier`.
Such a property was already hidden on read, and each of these names is one
the engine reads with its own meaning (`_type`, or `_src` with `_dst`, is how
an entity map is recognized as an edge).

**`MERGE` keyed on an undeclared property of a declared label works after a
flush.** `MERGE (n:N {nick: 'q'})`, where `N` is declared and `nick` is not,
failed with `No field named nick` once `N` had been flushed: the batched key
lookup filtered the label's table by a column the property does not have. Such
a pattern now takes the general MERGE path, which reads undeclared properties.

**`keys()` and `properties()` of a path element list its properties.**
`keys(relationships(p)[0])` returned `["properties"]` and
`properties(relationships(p)[0])` a map nesting them under `properties`,
while `relationships(p)[0].w` read the value.

**A flushed relationship no longer reports its storage blob.** On an edge type
declared with no properties, every flushed edge carrying an undeclared property
listed `overflow_json` in `keys(r)`, `properties(r)` and the returned
relationship, as a list of byte values, and a `SET` on it staged that entry
back as a property.

**A trigger sees a flushed vertex's properties as it sees an unflushed one's.**
The `old_value` pre-image of a vertex read back from storage carried
`overflow_json`, `_deleted`, `_uid`, `_created_at` and `_updated_at`, and its
undeclared properties sat nested under `overflow_json`.

**An `ext_id` given up can be taken again.** Once its holder was flushed, an
`ext_id` the holder later changed, removed or lost by deletion was still
reported as taken, permanently (`ext_id 'x' already exists`), on CREATE, SET,
batched CREATE and bulk insert.

**A bulk load on a fork is staged, and an interrupted one no longer damages
primary.** On a fork, `BulkWriter::abort()` failed (`rollback_table on a forked
backend is not supported`) and left the aborted rows on the fork, and the rows
were visible to the fork before `commit()`. Worse, an interrupted fork load
(crash, or a writer dropped without commit or abort) made the next open roll
**primary** tables back to the versions they had when the load began, losing
primary writes made meanwhile, and drop primary tables the load had seen
absent. A fork now stages its load on its own branches, exactly as primary
does, and writes no database crash marker. A fork load that fails part-way
through `commit()` can leave its per-label tables published and its main
table not; nothing reconciles a fork at reopen.

**A slow background flush no longer fails a commit with `CommitTimeout`.** The
time-based auto-flush (`auto_flush_interval`) held the commit lock for the
whole Lance write, and a commit waits at most `commit_timeout` (5 s) for that
lock. With async flush on (the default) it now writes Lance outside the lock,
as a commit's own flush trigger already did.

**Background work started on a fork stops with the database.** Async trigger
fires and index-rebuild workers started through a fork session were tracked
by a shutdown handle nothing ever signalled: `shutdown()` and `drop` neither
stopped nor waited for them, and `background_task_count()` did not count
them. They are now counted, signalled and waited for like the primary's, and
the fork's own workers stop when its last session is dropped.

**A bulk load with `async_indexes(true)` no longer leaves a worker behind.**
Each such load started its own index-rebuild worker, which ran until shutdown,
so N loads left N live tasks; the load now queues on the database's one
rebuild worker, and `indexes().rebuild_status()` sees its tasks.

**Concurrent commits that defer a trigger no longer queue on each other's
disk writes.** With a local data path, each `Defer` outcome rewrote and fsynced
the deferral sidecar while holding the queue lock. Eight threads committing 25
deferrals each took 6.6-10.2 s at a median of 243-385 ms per commit, and the
queue was unreadable for up to 0.66 s at a time; they now take 2.2-2.7 s at
51-83 ms, with the queue readable within 3 ms. A commit still returns only
once its deferral is on disk.

**A procedure called inside a query runs, and its yields keep their types.**
- `MATCH … CALL p(x) RETURN …` without `YIELD` was dropped from the plan: the
  procedure never ran. A procedure without results now runs once per row and
  keeps each row once. A procedure with results must name them with `YIELD`
  inside a query, as openCypher and Neo4j require; this is now a
  `SyntaxError` (it used to be silently skipped). A standalone `CALL p()` is
  unchanged.
- A per-row `YIELD` of a plugin procedure was always a string, so
  `UNWIND ['1'] AS s CALL uni.number.parseInt(s) YIELD result RETURN result + 1`
  answered `'11'`. Yields now take the procedure's declared type, as the
  standalone call's do.
- A `WRITE` procedure called per row failed with `inner write requires a
  writer-enabled procedure host`. Two equal input rows shared one call, so a
  writing or non-deterministic procedure ran once for both. A unit
  `CALL { … }` repeated its input row once per row its body made.

**A label check works on a node that arrives as one value.** `(a:L)`, `a:L`
and `EXISTS { MATCH (a:L)-->() }` on an `UNWIND` element (or any list item)
failed with `No field named "a._labels"`.

**`r['w']` over relationship types that share `w`.** When the relationship
could be either of two types that both declare `w`, `coalesce(r['w'], 0)`
failed with an ambiguous-field error.

**Quantified path patterns check the first inner node's label on every
iteration.** In `(a) ((x:L)-[:R]->(y)){1,3} (b)` the label on `x` was checked
only on the first iteration, and only when no outer node preceded the group,
so paths through nodes without `L` were returned. Without a quantifier,
`(a) ((x:M)-->(y)) (b)` ignored `:M`. An inner node label the schema does not
declare was ignored too, so `(y:Undeclared)` matched every node. A first inner
node with two labels is now refused, as for the other inner nodes. Under
`OPTIONAL MATCH`, a property map on the first inner node dropped the rows it
did not match instead of returning them with NULLs.

**A relationship matched again in a later clause is the same relationship,
schemaless.** `MATCH ()-[r:R]->() WITH r MATCH (x)-[r:R]->(y)` with `R`
undeclared paired every `r` with every `R` edge, or ignored the type.

**A quoted name is the same name everywhere.** `` MATCH (`n`) RETURN n ``
failed to plan, because the pattern bound `` `n` `` with its backticks while
`n` read the bare name. The same held for `UNWIND … AS`, `FOREACH`, list
comprehensions, `all/any/none/single`, `reduce`, `SET`/`REMOVE` targets, map
projections, `CALL` procedure names, index and constraint names, Locy
generator names and outputs, and the property names of `CREATE LABEL`,
`CREATE EDGE TYPE` and `ALTER … PROPERTY`: `` CREATE LABEL L (`my prop` STRING) ``
declared a property literally named `` `my prop` `` that `` n.`my prop` `` never
read. Index DDL on `` p.`a.b` `` took the name `` b` ``.

**Declaring a property whose name contains `.` or a backtick is refused.** The
storage layer cannot hold such a column. A dotted declaration used to succeed
and accept writes, and then every flush of the label failed; a backtick
declaration flushed, and then every read of the label failed with
`No field named`. Both names still work as undeclared properties.

**A refused `CREATE LABEL` / `CREATE EDGE TYPE` or schema `apply()` leaves
nothing behind.** A property refused part-way left the label (and the
properties before it) in the schema, so a corrected retry failed with
"already exists".

**A `WRITE` procedure's writes belong to the calling transaction.** The body of
a procedure declared with `uni.plugin.declareProcedure(..., 'WRITE', ...)`
wrote straight into the shared store: its writes survived `rollback()`, other
sessions saw them before `commit()`, and the transaction's own later reads did
not. This held for a standalone `CALL` and for a per-row `CALL` inside a query.
The writes now go to the transaction, so rollback discards them, commit
publishes them, reads in the transaction see them, and two transactions that
update the same node through a procedure conflict as plain writes do.
`Session::run` still auto-commits.

**A procedure called inside a transaction reads the transaction's view.**
Checked for declared Cypher procedures (`READ` and `WRITE`), `uni.algo.*`
algorithms, `GraphView` providers, `uni.vector.query` and `uni.fts.query`,
against the transaction's own Cypher reads:

- An algorithm projection (`uni.algo.reachability`, `wcc`, `mst`, ...) did not
  follow edges the transaction had created, still counted vertices and edges
  it had deleted, and read committed edge weights instead of the
  transaction's own updates. A flushed vertex deleted by an earlier,
  committed transaction was also still projected until the delete was
  flushed. The
  projection now reads the transaction's buffer as its newest layer, applies
  vertex and edge deletions from every layer, and reads edge properties as
  Cypher does.
- The body of a declared `WRITE` procedure saw nodes another session
  committed after the transaction began. A declared `READ` procedure saw
  them too once they had been flushed. Both now read the transaction's
  snapshot.
- Inside a read-write transaction, the vertices and edges an algorithm
  projection reads join the transaction's read set, as a Cypher scan's do,
  so a concurrent change to them fails the commit.
- `uni.algo.astar` read its heuristic property from flushed storage only.

Vector and full-text search already behaved correctly.

**An algorithm's list, map and node yields have their real type inside the
query.** They arrived as text that was parsed back only when rows were
returned, so `RETURN nodeIds` looked right while `size(nodeIds)` counted
characters, `UNWIND nodeIds` gave one string, `nodeIds[0]` and `x IN nodeIds`
failed, and `id(n) = node` (`articulationPoints`, `bridges`, `mst`, ...) never
matched. List, map and path yields are now Cypher values, and node and
relationship yields are integer ids.

**A `vector_similarity` threshold is an exact per-row filter.**
`WHERE vector_similarity(n.emb, $q) > t` was rewritten to a top-100 KNN over the
vector index that compared `t` against the index's native score rather than
the cosine similarity the function computes, so it returned at most 100 rows
(plus ties) and could keep the wrong ones. It now returns every node above the
threshold, and only those, with or without an index — and therefore scans the
label. Use `~=` or `uni.vector.query` for indexed top-K search.

**`percentileDisc` returns one of its inputs, exactly; both executors read
the percentile.** On the DataFusion path every input went through `f64`, so
`percentileDisc` over integers returned a float (`3.0` for `3`) and
`2^53 + 1` came back `9007199254740992.0`. It now returns the input itself,
with its type: an integer column gives an integer, a column of mixed type a
Cypher value. `percentileCont` still returns a float, now interpolated
between the exact inputs (halfway between `2^53 + 1` and `2^53 + 5` is
`2^53 + 3`, rounded once, where it was `2^53 + 2`). The fallback executor (a
standalone `CALL` of a registered procedure followed by `RETURN`) never read
the percentile argument, so both percentiles answered the 0th; it now reads
it, rejects one outside `[0, 1]` with `NumberOutOfRange`, and no longer turns a
float result such as `10.0` into the integer `10`. A percentile that is not a
number (`percentileDisc(x, 'a')`) is now an error on both paths; it was read
as `0`.

**A property declared on one label no longer types the same-named property
of another label.** A scan of an undeclared label, a multi-label pattern, or
`MATCH (n)` typed each property from the first label anywhere in the schema
that declared it. With `F.v` declared `FLOAT`, `MATCH (s:S) RETURN s.v` on the
schemaless `S` read an integer `9007199254740993` as `9007199254740992.0`, a
string as `NULL`, and `WHERE s.v = 9007199254740993` matched nothing. The
stored values were right; only the read was wrong. A property now takes a
declared type only from the scanned labels' own declarations, and is
otherwise read as the stored value. Locy's rule planner mirrored the old rule
and now mirrors the new one, and a Locy column of stored values brought to an
integer column is read exactly rather than through `f64`.

The same rule now holds everywhere a property column is typed before it is
read:

- **`CALL { ... }` subqueries.** Their output columns took the type from the
  first label declaring the name, so `CALL { MATCH (s:S) RETURN s.v AS v }`
  returned `9007199254740992.0` and `NULL`, and a later `WHERE v = ...` on the
  integer matched nothing. A subquery's property column is now typed from the
  labels or relationship types its variable is matched with, and is otherwise
  the stored value. Arithmetic in a subquery's `RETURN` is numeric only when
  both sides are: `s.v + 0` on a string concatenates (`'x0'`) instead of
  returning `NULL`.
- **Relationships matched over several types.** `MATCH ()-[e]->() RETURN e.w`
  typed `w` from the one type declaring it, so a schemaless type's integer
  rounded and its string read `NULL`. A relationship property is now typed
  only when every type in the pattern declares it at the same type.
- **Locy rules.** A property of a schemaless label, an unlabelled node or a
  relationship type that does not declare it, used in `YIELD` arithmetic, an
  `ALONG` binding or projected from an unlabelled node, failed the run with
  `cannot read the string "x" as a number` (or rounded an integer) because it
  took the type another label declared.
- **`UNION` of differently typed columns.** `MATCH (s:S) RETURN s.v AS v UNION
  ALL MATCH (f:F) RETURN f.v AS v`, and `RETURN 1 AS v UNION ALL RETURN 'a' AS
  v`, were rejected as "cannot UNION branches with mismatched schemas". A
  column whose branches differ in type is now unified as stored values, which
  is valid openCypher.

**Reachability over a variable-length pattern no longer lists trails.** An
`EXISTS { MATCH (a)-[:R*]-(b) }`, a pattern predicate, or `RETURN DISTINCT` /
`count(DISTINCT ...)` over `[:R*min..max]` is now answered from breadth-first
distances wherever those decide it: always for a lower bound of 0 or 1, and for
a higher one when every endpoint's shortest trail reaches it. On a cyclic graph
(every undirected one with an edge) the schema'd traversal searched for a trail
of each length up to the hop ceiling, and the schemaless one listed every
trail: an `EXISTS` over a 6-clique took 7 s to over 150 s in a debug build, and
now takes milliseconds. Answers are unchanged. The remaining search (a lower
bound above the shortest trail) stops at the number of relationships it can
reach, and no longer warns that an unbounded pattern was truncated at the hop
ceiling when no trail could have been that long.

**`range()` with a null argument returns null.** `range(0, null)`,
`range(null, 3)` and `range(0, 3, null)` raised `InvalidArgumentType … must be
an integer`; they now return null, as other scalar functions do for a null
argument, and `UNWIND range(0, null) AS i` produces no rows. A null wins over
every other check, so `range(null, 3, 0)` is null too. A non-null argument that
is not an integer still raises `InvalidArgumentType`, and a zero step still
raises `NumberOutOfRange`. All four evaluators agree: the DataFusion function,
the `UNWIND` evaluator, the fallback executor (whose errors now carry those
two codes; they were untagged) and Locy's `QUERY … RETURN`.

**An `ASSUME` body's commands run in sequence.** Inside
`ASSUME { ... } THEN { body }`, a `DERIVE` or Cypher write changes the
hypothetical graph, and every later command in the body now reads the changed
graph *and the rule facts derived from it*. Before, the body's facts were
derived once, before its first command: a `QUERY` (or a second `DERIVE`) after
a `DERIVE` answered from the graph without the derived edges — silently, while
a Cypher read in the same body saw them. The body now behaves exactly like its
commands run one by one in a transaction that is rolled back. After a write,
only the strata it can affect are derived again.

- An ASSUME now returns one result per body command (see *API changes*), so a
  `DERIVE`'s `affected` count and a nested ASSUME's results are reported in
  their own entries. Both used to be dropped, so a body whose only command was
  `DERIVE` returned no rows.
- A body that only defines rules (no commands) returned every fact its rules
  derived, flattened into the ASSUME's rows. It now has no results; the facts
  are in the result's `derived` map, keyed by rule. `LocyResult::rows()` and
  Python `LocyResult.rows()` skip such an ASSUME.

**`COPY ... TO` exports the user-visible properties.** A CSV export wrote the
label's storage table — its declared columns and the raw `overflow_json` blob —
so every undeclared property was missing, and rows still in memory (not yet
flushed) were missing too. A Parquet export wrote the storage table verbatim
(`_uid`, `_version`, `overflow_json`, ...; `src_vid`, `type`, `props_json` for
edges), CSV edge export failed, and a schemaless label was "unknown". Every
export now writes exactly what `properties()` returns, for declared and
schemaless labels and edge types, flushed or not. Layout, both formats: `_vid`
(vertices) or `_eid`, `_src`, `_dst` (edges), then one column per property — the
union of the keys over all exported entities, sorted by name; an entity without
one has an empty cell (CSV) or null (Parquet). Parquet keeps declared types and
gives an undeclared column its values' shared type, or the tagged value encoding
when they are mixed or structured.

**`COPY ... FROM` reads that layout back.** `COPY E FROM` takes endpoints from
`_src`/`_dst` when the file has them (still `src`/`dst` otherwise, and
`src_col`/`dst_col` override). An empty CSV cell is now absent rather than an
empty string; declared `INT64`, `FLOAT64` and `BOOLEAN` columns are parsed
(only `INT32` was — the others failed the import); an unparsable endpoint id is
an error instead of vertex 0; Parquet cells decode with the declared type; and
schemaless labels and edge types are accepted unless `strict_schema` is on.
CSV options `delimiter` and `header` (or `headers`) apply to both directions.
Text is all CSV carries: an undeclared property imports from CSV as a string —
use Parquet to round-trip its type.

**`UniConfig::file_sandbox` is enforced.** The executor kept its own sandbox
field that nothing in the database set, so an enabled sandbox never restricted
`BACKUP` or `COPY`. Both now refuse a path outside `allowed_paths` when the
sandbox is enabled ("Path validation failed"); the embedded default (disabled)
is unchanged. `COPY` also reads and writes `s3://`, `gs://` and `az://` URLs,
as documented — the live code path only took local paths.

**A user property named `label`, `type` or `properties` is kept on every
entity form.** `MATCH (n) CALL { WITH n SET ... } RETURN n` returned the node
without those three properties (flushed or not), while `n.label` read them:
the result normalizer dropped inline keys with those names when building a node
or edge from a flattened entity map. Locy's entity decoding likewise dropped an
inline `properties`, and `Value::canonical_entity` a non-map `properties`.
Which keys are properties is now decided only by the reserved-name rule and the
entity's property container. Plain `RETURN n`, `properties()`, `keys()`, map
projection and paths were not affected.

**Soft write throttling no longer outlasts the statement timeout.** Between
`throttle.soft_limit` (16) and `throttle.hard_limit` (32) uncompacted flush
generations, each write slept `base_delay × 2^(runs − soft_limit)` in one
uninterruptible sleep: 40.96 s at 28 runs, 81.92 s at 29. It did not end when
compaction reset the count, so a one-row `CREATE` failed with
`Timeout { 30000 }` behind a compaction that had already finished. The wait is
now re-checked every 100 ms, ends within one check of the count falling below
`soft_limit`, and is capped at `100 ms × (hard_limit − soft_limit)` (1.6 s at
the defaults). Below the cap the curve is unchanged (up to 1.28 s at 23 runs).
The backoff also saturates instead of panicking with "overflow when multiplying
duration" for a `base_delay` above about 8.6e9 s. The hard limit is unchanged:
writes stop until compaction catches up.

**A search procedure's `YIELD node` is a full node.** `uni.fts.query`,
`uni.vector.query` and `uni.search` yielded a node with no properties when it
was used whole: `RETURN node` and `node {.*}` were empty, `properties(node)`
was `NULL`, `keys(node)` was `[]` and `labels(node)` failed. Only a property
the query named (`node.id`) was fetched. The node now carries every property,
declared or not, before and after a flush.

**`[r:R|Q]` mixing a declared and an undeclared relationship type matches
either type**, in a plain pattern, a variable-length pattern and a Locy rule
body. It failed with "Mixed known and unknown edge types not yet supported".
A Locy rule-planning error now also carries its cause instead of only
"LocyPlanBuildError: planning MATCH pattern".

**`SET` inside `CALL { }` works for every declared property type.** A declared
`MAP`, `LIST<INT>`, `VECTOR`, temporal or `FLOAT32` property set inside a unit
subquery panicked in debug builds and failed in release with a column-type
mismatch.

**Edges that violate their type's declared endpoints are matched.** With `R`
declared `A -> F`, a store holding `(:A)-[:R]->(:S)` dropped that edge from
`MATCH (a:A)-[:R]->(b)` and from `MATCH ()-[r:R]->() RETURN count(r)`: the
planner narrowed an endpoint the pattern did not label to the declared label.
Writes now enforce declared endpoints, but a store written before can hold such
edges; a pattern that names no label no longer filters by one.

**Declared endpoints narrow reads again, where that is sound.** Each edge type
in the catalog now records whether its stored edges are known to conform to
its declaration (`EdgeTypeMeta::endpoint_conformance`): `enforced` for a type
created under write-time enforcement, `verified` after a check, `unverified`
otherwise -- including every type in a catalog written by an earlier release,
and a type declared over a name that already labels schemaless edges. The
planner narrows an unlabelled endpoint of a typed relationship (an anonymous
source to a label scan, an unlabelled target to the declared label) only when
every type in the pattern conforms; otherwise it reads every vertex and matches
violating edges, as above. LDBC SF1, `MATCH ()-[w:WORK_AT]->() RETURN
max(w.workFrom)`, process peak RSS: 5.7-6.7 s and 1.25-1.47 GB unverified,
0.26-0.42 s and 0.81-0.85 GB once verified (the narrowed plan of the previous
release measured 0.23-0.28 s and 0.83 GB).

**`CALL uni.admin.verifyEdgeEndpoints([edgeType])`** checks the live edges of
each unverified declared type (or of the one named) against their declared
labels -- after flushing, as of one version -- and records the clean ones
`verified`. It yields `edge_type`, `edges_checked`, `source_violations`,
`target_violations` and `conformance` per type; a type with violations stays
unverified and its edges stay readable. Each type's result is saved as it
completes. Measured on the LDBC SF1 store: all 15 types in 12.3 s (1.7 GB peak
RSS, outside the query pool), `HAS_CREATOR`'s 3.06M edges in 2.8 s. It refuses
to run inside a transaction, and waits up to 10 s for transactions whose
snapshot predates its read (they could still see an edge deleted before it),
then fails naming them. A verification never covers a view fixed before it: a
fork created earlier keeps the type un-narrowed (a nested fork trusts no
verification), as does a read pinned to an earlier snapshot, and
`restore_snapshot` / `uni.admin.snapshot.restore` withdraw it from the catalog.
Types enforced since creation narrow everywhere. The procedure is refused on a
fork.

**The bulk loader enforces declared endpoints.** `BulkWriter::insert_edges`
writes straight to storage and checked nothing; it now refuses an edge whose
endpoint, among the load's own vertices or the database's, carries none of the
declared labels, with `UniError::EdgeEndpointViolation`.

**A property write cannot move a relationship.** `SET` / `REMOVE` on a map
carrying `_eid`, `_src`, `_dst` and `_type` (which the write path treats as a
relationship) stored whatever endpoints and type the map named for that edge
id: `WITH {_eid: e, _src: s, _dst: f, _type: 'R'} AS m SET m.w = 1` moved an
existing `R` edge to new endpoints, or to another type, with no
declared-endpoint check. It is now refused with the new
`UniError::EdgeIdentityChange { eid, stored, requested }` (Python:
`UniEdgeIdentityError`, a `UniConstraintError`); a map naming an edge id that
does not exist is held to the declared endpoints like a create. The check
reads each edge's stored identity from the write buffers, then the source's
outgoing adjacency (once per source and type, so a `SET` over a hub's edges
stays linear), then storage.

**Writes by an older binary void endpoint conformance.** An older binary does
not check declared endpoints, so edges it writes after a type was marked
`enforced` or `verified` can violate the declaration the planner narrows on.
Every catalog save, snapshot manifest and WAL segment now records the writer's
features (`endpoint_enforcement`). On open, a replayed WAL segment without
them, or a latest manifest without them that is not the pre-existing one an
enforcing binary already accepted, sets every `enforced` / `verified` type to
`unverified` (saved, with a warning naming them); run
`CALL uni.admin.verifyEdgeEndpoints()` to restore narrowing. A read-only open
downgrades its in-memory view only. A store first opened by this release whose
last flush was by an older one keeps types declared afterwards `enforced`.

**A commit made after a reopen survives a crash.** A flush deletes the WAL
segments its manifest covers, so a cleanly closed store had none, and the
reopened log numbered its segments from 1 again while the manifest's WAL mark
was higher; recovery replays only segments above the mark, so every commit
between the reopen and the next flush was lost in a crash (three flushed
commits, close, reopen, commit, crash: the fourth row was gone). The log now
numbers above the manifest's mark.

**Commits no longer read every written edge back from storage.** The
built-in `:_AuditMe` node trigger is registered by default, so every commit
ran the trigger pre-image probe, which reads the committed state of every edge
the commit writes in one batched storage lookup. It now runs only when a CDC
output or a trigger subscribed to edge events is registered, and never for
edges the commit created (their ids are fresh). Probe time, debug build: 1M
edges `SET` after a flush 15.0 s per commit, 1M new edges beside 1M flushed
ones 3.9-7.1 s, now 0 s; a required edge pre-image read is unchanged.

**A missing parameter is an error, in writes as in reads.** `CREATE {v: $x}`,
`SET n.v = $x`, `MERGE`, `ON CREATE`/`ON MATCH SET`, `UNWIND .. CREATE` and
`FOREACH` stored `NULL` when `x` was not supplied -- `SET` erased an existing
value. They now fail with the new `UniError::ParameterMissing { names }`
(openCypher `ParameterMissing`, Neo4j `Statement.ParameterMissing`) before
writing anything. Reads, which failed with an untyped "Unresolved parameter",
and Locy rules raise the same error. A NOT NULL key fed from a missing
parameter names the parameter instead of reporting "cannot be null". A
parameter supplied as `null` is still a value.

**`ParameterMissing` names every missing parameter.** `RETURN $a, $b` with no
parameters reported `names == ["a"]`: the check ran when an expression was
first evaluated, so it named whichever parameter it reached first, and none at
all for one in a branch no row reached (`MATCH (n:T) WHERE n.k = -1 AND n.v =
$x` returned no rows). A statement is now checked against its parameters before
it runs, as openCypher specifies (`ParameterMissing` at compile time) and Neo4j
reports (`Expected parameter(s): a, b`): `names` lists every parameter the
statement references and the caller did not supply, once each, in order of first
appearance (Python: `UniParameterMissingError.names`). This covers session and
transaction statements, plan-cache hits, cursors, `PROFILE`, prepared queries
and Locy programs (`session.locy`, `tx.locy`); `EXPLAIN` still plans without
parameters, as in Neo4j. Text that only looks like a parameter, inside a string
literal or a comment, is not one. A parameter referenced only by a rule
registered earlier is still reported when the rule is evaluated.

**`restore_snapshot` restores the database.** `Uni::restore_snapshot` and
`CALL uni.admin.snapshot.restore` only moved the `latest` manifest pointer:
readers read every table at its newest version, so after a restore -- in the
same handle and after a reopen -- the store kept everything written after the
snapshot (measured: vertices `Alice(31), Carol(99), Dave, Eve` where the
snapshot held `Alice(30), Bob, Carol(50)`). A shutdown flush then overwrote the
pointer as well. Now a restore rewinds every table and index dataset to its
version at the snapshot and takes effect at once: in the same handle and after
a reopen, reads see the snapshot's vertices, edges, labels and property values
and nothing written after it, flushed or still in memory. Writes after the
restore build on the restored state. To make this possible an explicit snapshot
(`create_snapshot`, `uni.admin.snapshot.create`) now records the version of
every table and pins it with a storage tag, so compaction's version cleanup
keeps it; those versions are retained for as long as the snapshot exists.
Limits:

- Only an explicit snapshot can be restored. A manifest written by an ordinary
  flush (listed by `uni.admin.snapshot.list` too), or a snapshot taken by an
  earlier version, is refused with an error saying it has no restore point.
- A restore is refused while a transaction is open, inside a transaction
  (`uni.admin.snapshot.restore` in `tx.execute`; a rollback could not undo it),
  while a bulk load is publishing, and on a fork. A transaction begun during a
  restore cannot commit after it (it is aborted as a conflict).
- The schema catalog is restored with the data: labels, edge types,
  properties, indexes and constraints declared after the snapshot are gone,
  ones dropped since are back, and a property renamed or redeclared with
  another type since has its old name and type. Tables created after the
  snapshot are dropped rather than emptied, so a later column type cannot
  outlive the restore. A snapshot whose restore point predates catalog
  recording is refused with an error saying so.
- Background index builds are paused for the restore: a build under way
  finishes first, and queued builds run afterwards against the restored data.
  Before, a build that read the store before the rewind could record its
  index `Online` after it, over a table the rewind had rolled back.
- A restore is refused while the database has forks, with the new
  `UniError::RestoreBlockedByForks { forks }` (Python
  `UniRestoreBlockedByForksError`, attribute `forks`), naming every fork the
  registry holds, whether active, being created or being dropped. A restore
  rewinds primary storage and the UID index that primary and the forks share,
  but not fork branches: a fork would keep data cut from the pre-restore
  primary and lose the UID entries it registered after the snapshot. Drop the
  forks first (`drop_fork`, or `drop_fork_cascade` for a fork with children).
  A fork whose creation races a restore is rolled back with
  `UniError::ForkLifecycle { stage: "restore_race" }` and can be retried.
  Restore applies to the primary database only.
- A crash during a restore is completed when the database is next opened.

**Snapshots can be deleted.** A restorable snapshot pins every storage version
it records with a tag, so compaction never reclaimed them. `Uni::delete_snapshot`,
`CALL uni.admin.snapshot.delete(id)` (mode `Dbms`: through `session.run`, not
inside a transaction) and Python `delete_snapshot` (sync and async) remove the
snapshot's tags, restore point, names and manifest; the next compaction then
reclaims the versions no other snapshot pins, and other snapshots still restore.
An unknown id is `UniError::SnapshotNotFound` (Python `UniSnapshotNotFoundError`,
now with a `snapshot_id` attribute; also recovered on the procedure path, and
now returned by `restore_snapshot` for an unknown id). Refused while a restore
runs or is incomplete and while a fork was created from the snapshot. Deleting
the snapshot `latest` names republishes it under a fresh id first, so the store
still opens.

**`uni.graph.project` inside a transaction with uncommitted writes is
refused** with the new `UniError::UncommittedWritesInTransaction`. The
projection catalog is shared by every session and not undone by a rollback (as
in Neo4j GDS), so such a projection published the transaction's writes before
commit and kept them after a rollback. A transaction without writes builds the
projection from its own snapshot.

**A bulk load becomes visible all at once.** A load is published table by
table, and a reader between two of those commits saw part of it: measured with
a pause before each table (20 vertices, 30 edges), `MATCH (n:P)` returned the
20 vertices while `MATCH (n)` returned none, then every vertex appeared with
none of its edges (a warm adjacency cache), or the edges appeared in one
direction only (30 forward, 0 backward). Readers of the database handle the
load runs on -- its sessions, transactions and pinned snapshots -- now see none
of a load until its last table is published, and then all of it. Another
process, or a second handle opened on the same path, is not covered and can
still see a load part way. Semantic compaction no longer runs while a load is
being published (a background pass skips that tick; `uni.admin.compact()`
waits).

**A load left incomplete is completed in the background.** After
`BulkCommitIncomplete`, nothing completed the load until the next open, and
readers saw the part already published (20 vertices by label, nothing else).
A background task now retries the publish with backoff (200 ms doubling to
30 s), keeps the load hidden until it completes, and then finishes it as the
next open would; it stops when the database shuts down, and the next open
completes the load then. The error message says so.

**Fork bulk loads have the primary's outcomes.** A fork load kept no recovery
marker: `bulk_load_applied` answered `false` after a successful fork load, and
a failure part way through publishing returned `Internal` with the per-label
table published and the main table not, through reopen. A fork's marker and
receipt now live in the fork's namespace (`catalog/forks/{fork_id}/`); its load
is completed in-process, in the background, or when the fork is next opened,
and returns `Ok`, `BulkCommitIncomplete` or `BulkCommitOutcomeUnknown` like a
primary load. Ask about it with the new `Session::bulk_load_applied` on a
session of that fork; `Uni::bulk_load_applied` answers only for primary loads.

**Snapshots and bulk loads.** The snapshot a bulk commit records now covers
the load: pinned to it, a session saw none of the load (its version ceiling
was the previous manifest's, 0 on a fresh database), and the next open
restarted the version counter below the load's version. And a session pinned
to a snapshot taken *before* a bulk load no longer sees the load through a
label-less `MATCH (n)`: the load's rows in the shared vertex table carried
`_version = 1` (measured: 21 vertices where the snapshot held 1).

**A fork is never cut part way through a bulk publish.** A fork created while
a load was being published branched off the tables published so far and kept
that part for good (measured: the load's 20 vertices, none of its 30 edges).
Fork creation now waits for the publish to finish (up to 30 s) and branches
off all of it. While a load stays incomplete past that wait (its background
re-drive keeps failing) fork creation fails with
`UniError::ForkLifecycle { stage: "bulk_publish", .. }`, creates nothing, and
can be retried.

**A bulk load's snapshot no longer changes after it is taken.** A vertex
committed before a load but not yet flushed was missing from the snapshot the
load's commit records, then appeared in it once flushed (measured: 20 rows,
then 21). The commit now flushes such rows first, so they are in the snapshot
from the start.

**Published bulk edges are checked against their type's declared endpoints
again.** An endpoint that existed before the load could lose its label (or be
deleted) while the load's edge was still invisible, so the label-removal check
let it through; the type stayed enforced and `MATCH ()-[r:E]->()` silently
missed the edge (0 rows instead of 1), live and after a reopen. The same held
for a load completed by the background re-drive or by recovery at the next
open. The load is applied either way; a type with such an endpoint is now
recorded unverified (reported in `BulkStats::warnings` or the log), so the
edge is found. Run `CALL uni.admin.verifyEdgeEndpoints()` once the endpoints
are fixed. Recovery checks after the write-ahead log is replayed, so a label
removal that was not yet flushed counts.

**A manual full compaction relieves write throttling.** `CALL
uni.admin.compact()` (Rust `StorageManager::compact`) optimised tables but
left the flush-generation count that drives the write throttle unchanged, so a
writer at `throttle.hard_limit` stayed stalled (measured: the count stayed at
3 and the next `CREATE` timed out). It now runs the same full pass as
background compaction, including folding the L1 deltas, under the same guard
and directory lock -- it waits for a running pass instead of overlapping it
(two overlapping manual passes failed with a Lance commit conflict) -- and
lowers the count. `db.compaction().compact(label_or_edge_type)` still only
optimises that table's files and leaves the count alone. A compaction pass,
manual or background, now subtracts the runs it saw when it started instead of
resetting the count to 0, which erased flushes that landed during the pass.

**Locy `EXPLAIN RULE` reports each derivation's weight, and `top_k_proofs`
keeps the heaviest.** A recorded derivation's `proof_probability` was always
`None`, so `top_k_proofs` kept an arbitrary set, and it kept every derivation of
a fact anyway because it counted per derivation rather than per fact. A
derivation now weighs its own PROB value (its local weight times every
probability it referenced; a leaf weighs exactly its own value), and
`top_k_proofs = k` keeps the `k` heaviest derivations of each fact:
`reach(A, C)` over `A -0.5-> B -0.8-> C` and `A -0.3-> C` keeps the 0.4
derivation at `k = 1`, not the 0.3 one recorded first. The
`TopKPruningCrossedDependency` analysis runs on these weights. `proof_probability`
is set whatever `top_k_proofs` is.

**A Locy program prepared in a transaction runs as `tx.locy` does.**
`tx.prepare_locy(program)` built a session-style program: it did not read the
transaction's uncommitted writes (`tx.locy` returned `[7]` where the prepared
program returned `[]`), ignored its snapshot and cancellation, and collected its
writes instead of writing them into the transaction. Its reads and writes now go
through the transaction, under its snapshot and cancellation scope. A
session-prepared program is unchanged.

**A Locy program's `$params` reach every command and the `tx.apply` replay.**
A Cypher command after the program's first write ran with no parameters: a
`$param` read failed with "Unresolved parameter" and a `$param` write stored
NULL. And `tx.apply` replayed a session program's Cypher writes without
parameters, storing NULL. Both now use the program's parameters.

**A Locy program's `timeout` covers the whole program.** Each evaluation a
program runs again (after a write, for an ASSUME body or an ABDUCE candidate)
got the full timeout afresh, and converting, hydrating and answering commands
after the program's own evaluation was not bounded at all: eight writes each
followed by a `QUERY` ran three timeouts long and returned `Ok`, and a
1200-item cross join ran 2.85 s against a 300 ms timeout (debug build). Every evaluation now
gets what remains of the program's budget, and the budget is checked after the
evaluation, before each command and after the last. A program past it fails
with `Timeout` (or `LocyIncomplete` when an evaluation was cut short).

**`allow_partial` reports every cut.** Under `allow_partial`:
- an evaluation run again after a write, or in an ASSUME body, that stopped at
  `max_iterations` or the budget returned its partial facts as complete
  (`max_iterations = 100`: 100 of 239 rows, `incomplete` unset). It is now
  reported in `incomplete`, naming its rules, and the commands stop there. The
  command that needed the cut-short facts is not answered from them. Without
  `allow_partial` the cut is a `LocyIncomplete` error; it was a generic runtime
  error with the message inside;
- an operator's deadline raised `Timeout` instead of being reported like the
  checks between strata and iterations; it is now an `incomplete` with reason
  `timeout`;
- a budget that runs out after the facts are complete skips the commands and
  reports an `incomplete` with reason `timeout` and no rule listed.

**A schema change racing a snapshot restore no longer brings the old catalog
back at the next open.** Catalog saves were not ordered: a schema change that
had read the catalog before `restore_snapshot` (or `uni.admin.snapshot.restore`)
installed the snapshot's, and wrote it after, left the pre-restore catalog on
disk. The handle kept answering from the restored catalog, so nothing looked
wrong until the database was reopened; then labels, properties and indexes
declared after the snapshot were back over storage rewound to before them, and
a property renamed after the snapshot read NULL on every row under its
restored name. Saves are now written in the order they read the catalog, which
also covers two schema changes, or a schema change and a commit, saving at
once. A schema change still in progress when a restore installs the snapshot's
catalog is undone along with everything else declared after the snapshot.

**A crash while adding a property no longer leaves the label unreadable.**
Adding a property to a populated label (`db.schema()...apply()` or
`ALTER ... ADD PROPERTY`) declared it in the live catalog before adding its
column to storage. Every commit saves the catalog if it changed, so a commit
from another session in that window wrote the new property to disk ahead of
its column, and a crash before the column was added made every read of the
label fail after reopen (`column types must match schema types`). Storage is
now widened before the property is declared. A widening that fails also leaves
the catalog unchanged; before, the half-applied declaration stayed in the live
catalog and the next commit saved it.

**`tx.bulk_writer()`'s documentation described the load as part of the
transaction.** It is not, and was not: `BulkWriter::commit` commits the load on
its own, other sessions see it at once, rolling the transaction back keeps it,
and the transaction's own reads do not see it. The rustdoc now says so.

## Correctness fixes

### Silent wrong answers — Cypher

#### OPTIONAL MATCH

- **A multi-step `OPTIONAL MATCH` emitted extra NULL rows** beside its real matches, one per dead
  end (`OPTIONAL MATCH (a)-[:R]->(b)-[:S*]->(c)`), or extended rows an earlier path had already
  failed, returning `[6, 7]` where `[6, NULL]` was due. Each traversal inside the clause decided
  "no match" for the rows in front of it, which is exact only for the first step. The clause now
  decides once, at its end, per entering row.
- **Rows entering an `OPTIONAL MATCH` that bound the same node were merged**, so after an `UNWIND`
  one of two NULL rows disappeared, and identical entering rows collapsed to one. Each entering row
  now carries its own id.
- **A chunked optional traversal** (one input batch expanding past the execution batch size,
  e.g. an `OPTIONAL MATCH` over a hub) emitted a NULL row per chunk for every input row whose
  matches were in another chunk: 90 rows for 70.
- **An entity an `OPTIONAL MATCH` did not bind was a struct of NULLs**, not NULL. `labels(b)`
  failed, `keys(b)` returned `[]`, and `[b, 1]` or `{k: b}` became NULL as a whole.

#### Patterns

- **Inline element predicates were ignored.** `(m WHERE m.id = 3)` and `[r WHERE r.w = 1]` parsed
  but never became a filter, in Cypher and in Locy rule bodies. Inside `EXISTS { MATCH ... }` they
  failed with `UndefinedVariable`. They are now applied, and refused on a variable-length element,
  where they would apply per edge.
- **A relationship after a variable-length one could reuse its edges.** `(a)<-[:K*1..2]-(x)-[:K]->(b)`
  walked back out along the edge it came in by: 221 rows where brute force gives 104. Fixed for
  Cypher and for Locy rule bodies.
- **Reachability under `DISTINCT` / `EXISTS` was order-dependent.** An endpoint was dropped if the
  first predecessor found lay on a walk that reused an edge, even when a later one had a valid
  trail. The same query returned 95 to 103 of its 103 rows over 40 runs.
- **Pattern predicates and pattern comprehensions honoured only part of a pattern.** `WHERE
  (n:Person)-[:R]->()` also matched a `Robot`; relationship property maps were ignored; an edge
  could be traversed twice. A pattern now takes the vectorized path only when it uses features that
  path implements; the rest run as correlated subqueries.
- **A label disjunction on a traversal target** (`(n)-[:R]->(m:Robot|C)`) returned nothing: the
  labels were ANDed, then pinned to the first.

#### Expressions and projection

- **`WITH b AS a` did not rebind `a`.** In a swap (`WITH b AS a, a AS b WHERE a.id > b.id`) the
  `a.*` columns still held the old `a`, returning 14 rows of 36. A property read through a chain of
  renames (`WITH n AS a WITH a AS x RETURN x.id`) returned NULL.
- **`reduce` truncated floats** to the accumulator's type: `reduce(s = 0, v IN [1.5, 2.5] | s + v)`
  returned 3. With a `null` start it could panic in Arrow.
- **Window `sum` truncated floats**: `sum(n.f) OVER (...)` over 0.5 and 1.25 returned 1.
- **`collect(DISTINCT ...)` keyed values by display string**, merging `1` with `'1'` and `[1]` with
  `['1']`. It now keys structurally, and in a hash set instead of a linear scan.

### Silent wrong answers — Locy

- **Recursive `FOLD` merged parallel edges and distinct paths** (#294). `MSUM` over edges of 30 and
  40 into the same node, plus 20 from another, returned 60 or 50 depending on batch order, never 90.
  The same collapse one level down — two parallel equal-weight edges extending the same fact — gave
  one `ALONG` fact instead of two and a downstream `SUM` of 4 for 7. A derivation is now identified
  by every relationship it used and by the row it extended.
- **An ungrouped `FOLD` `YIELD` column was dropped without a diagnostic** (#293), and `QUERY ...
  RETURN e.uid` then read NULL. Now a compile error (see above).
- **Mutually recursive rules returned each other's facts.** `QUERY even` over `even`/`odd`
  returned odd's rows too, and which symptom appeared depended on hash-set order.
- **A non-recursive rule's facts were not deduplicated.** Three parallel edges gave `(a, b)` three
  times in `QUERY` results and `derived_facts`. A `FOLD` still counts every derivation, since it
  aggregates the bag before this point; `ALONG` facts stay one per path.
- **Fixpoint float rounding was applied to stored values**, so any magnitude below 1e-12 became 0:
  an `ALONG` product of 1e-7 × 1e-7 was stored as `0.0`. Rounding now applies only to comparisons.
- **A seeded `FOLD` column** failed with `concatenate arrays of different data types` when the seed
  and the folded input had different types (`MCOUNT(r)` beside an integer seed, or `MSUM(r.pct)`
  with a seed of `0`).
- **`QUERY ... ORDER BY` a column not returned** did nothing: the key was evaluated after
  projection and was NULL for every row.
- **An SLG goal binding** failed to match `3.0` against an integer key, and an aliased key
  (`YIELD KEY n.k AS k`) failed with "Variable 'k' not defined".

### Storage, search and schema

- **A chunked label scan dropped unflushed rows past a vid gap.** With 9,000 flushed rows, 60,000 of
  another label and 7 new ones, `MATCH (p:P) RETURN count(p)` returned 9,000. The walk asked flushed
  storage alone where the label's next row was.
- **Declaring a `NOT NULL` property on a label whose rows were all unflushed** recorded it as
  `NOT NULL`, after which every flush failed and those rows could not be written at all. Unflushed
  rows now count as existing, so the property is recorded nullable with a warning, as for flushed
  rows.
- **A vertex `UNIQUE` key stayed taken after it was freed** by an unflushed `DELETE`, a flushed
  `SET`, or an unflushed `SET` (which also tripped the commit-time SSI guard with "unique key
  already committed by a concurrent transaction").
- **Full-text and vector search returned a node through a value it no longer has**, both after an
  unflushed update and through the older row versions an append-only table keeps after a flush.
  A search is repeated with a wider fetch only when stale hits were dropped and left it short, so
  `ef_search` keeps its meaning.
- **`(n {ext_id: 'x'})` without a label** missed unflushed vertices, could not project properties
  (`No field named "n.name"`), and returned every property as a string. It is now planned as the
  ordinary vertex scan, with the `ext_id` equality pushed down to the index.
- **Pinned sessions reported `snapshot_reads: 0`** for queries that touched only edges or the main
  vertex table.

### Errors that are now answers

- `elementId(n)` on a scan- or traversal-bound variable failed with "No field named n".
- `EXISTS { MATCH (n:Person)-[:R]->() }` with `n` bound outside failed with
  `No field named "n._labels"`.
- A comprehension, quantifier or `reduce` inside a `CASE` branch failed with "Column references
  column 'v' at index 6 but input schema only has 2 columns", or, for a pattern comprehension,
  silently returned empty. `CASE WHEN ... THEN null ELSE reduce(...) END` failed at run time.
- `size([x IN xs | x]) * 1.5` failed with "Invalid arithmetic operation: Int64 * Float64".
- A repeated property predicate on a variable-length target failed in Lance with "Duplicate column
  name".
- A bare `WHERE n.b` on a schemaless boolean property failed to plan.
- A map compared with a scalar (`1 < {k: 1}`) failed to plan or failed at run time; it now follows
  openCypher (ordering NULL, `=` false, `<>` true).


## API changes

- **`UniConfig` has a new public field, `execution_batch_size: Option<usize>`.** Code that
  constructs `UniConfig` with a struct literal and no `..Default::default()` must add it.
- **`LocyCompileError` has two new variants**, `UngroupedFoldYield` and `UnknownQueryVariable`. An
  exhaustive `match` must handle them.
- **`uni-store`: `StorageManager::materialized_row_count` is now `existing_row_count`** and takes an
  `&L0Context`, so it counts unflushed rows as well (see the `NOT NULL` fix below).
- **`uni-query`: `VlpOutputMode::EndpointsOnly` is split** into `Reachability` (the old search) and
  `EndpointsPerPath` (the new default).

**`LocyResult` derived facts are read through async accessors (#307,
breaking).** The public field `uni_locy::LocyResult::derived` is gone (also
through `uni_db::locy::LocyResult`'s `Deref`), and so is the synchronous
`derived_facts(rule)`. Use `result.derived_rules().await?` (every rule, a
`&RuleFacts`) or `result.derived_rule(name).await?` (`Option<&Vec<FactRow>>`):
they are async and fallible because a rule the default `GoalDirected::Auto`
left out is derived on that first read. `evaluated_rules()` is the synchronous
view of what the evaluation itself derived, `lazily_derived_rules()` names the
rest, `lazy_derived()` exposes the deferred derivation
(`uni_locy::LazyDerived`, `DerivedMaterializer`). A `LocyResult` is built with
`LocyResult::with_derived(..)` (and `with_lazy_derived`) instead of a struct
literal. `GoalDirected` has the new variant `Auto` (the `Default`); an
exhaustive `match` must handle it. New: `uni_locy::magic::{AutoFallback,
auto_fallback, materialization_program}`,
`GoalDirectedReport::generated_rule_origins`, `uni_locy::Materialized`; `uni_db::locy` re-exports
`FactRow`, `RuleFacts` and `LazyDerived`. Python: `LocyResult.derived` keeps
its shape (a dict) and becomes a getter that derives left-out rules on first
access, blocking; `await result.derived_async()` is the non-blocking form for
async code; `goal_directed` accepts `"auto"`, which `LocyConfig.goal_directed`
reports by default.

**Commit events** (`uni-store`, `uni-plugin-host`, `uni-bulk`, `uni-common`).
`CommitNotification` has a new field, `mutation_stream: Option<u64>` (`None`
for every transaction commit); a struct literal must set it.
`Writer::commit_transaction_l0_sequenced` (returns `CommittedTx { wal_lsn,
flush_pending, version, ticket }`) and `Writer::sequence_announcement` are new,
with `runtime::publish_sequencer::{PublishSequencer, PublishTicket}`.
`CdcRuntime::open_mutation_stream` / `MutationStreamSender` are new.
`BulkBackend` has a new field `events: Option<Arc<dyn BulkEventSink>>` (the
`uni_bulk::events` module: `BulkEventSink`, `VisibleLoad`, `LoadEventReader`,
`BulkWriteKind`, `BULK_EVENT_CHUNK_ROWS`); `BulkConfig` has
`skip_before_triggers`. `MutationEvents::creates` with `CreatedVertex` /
`CreatedEdge`, `TriggerRouter::synchronous_before_trigger_for` and
`TriggerRouter::has_after_triggers` are new. `UniError::BulkLoadBlockedByTrigger
{ trigger, target }` is new (Python: base `UniError`). `BulkEventSink::publish`
returns `uni_bulk::events::EventDelivery { Finished, Interrupted }`.
`Writer::commit_transaction_l0_sequenced` takes an `on_abandon` announcement,
installed with the new `PublishTicket::on_abandon`. **Breaking:**
`uni_bulk::recover_interrupted_bulk_load` returns `Vec<RecoveredLoad>` (the
loads whose events are still to be delivered) instead of `()`;
`uni_bulk::finish_recovered_load_events`, `TriggerRouter::wants_after_event`
and `ShutdownHandle::is_signalled` are new. Python: `skip_before_triggers` on
`TxBulkWriterBuilder`, `TxAppenderBuilder` and their async twins.

**Snapshot restore** (`uni-store`). `StorageManager::restore_snapshot` is
removed: it only moved the manifest pointer. `Writer::restore_snapshot` and
`Writer::create_snapshot` replace it and `Writer::flush_to_l1` for snapshots;
`Uni::restore_snapshot` returns `UniError::ReadOnly` on a read-only handle and
the new `UniError::RestoreBlockedByForks` while forks exist (also recovered on
the procedure path); `Writer::restore_epoch` is new.
`StorageBackend::tag_table_version` is new (the default refuses), and so is
`StorageBackend::delete_table_tag` (idempotent; the default succeeds).
`Writer::delete_snapshot`, `Uni::delete_snapshot`,
`StorageManager::pause_index_builds` / `index_build_permit`,
`restore::release_restore_point`, `RestorePoint::catalog`, and
`SnapshotManager::{delete_restore_point, delete_snapshot_manifest,
snapshot_exists, latest_snapshot_id, remove_snapshot_names}` are new;
`restore::rewind_storage` takes the snapshot's creation time.
`UniError::SnapshotNotFound`'s display now starts with the
`SnapshotNotFound: "<id>": ` tag (`UniError::snapshot_not_found_from_message`).
`SnapshotManager` gains `save_restore_point` / `load_restore_point` and the
restore-intent methods; `RestorePoint`, `RestoreIntent` and the
`uni_store::snapshot::restore` module are new. `AdjacencyManager::reset`,
`StorageManager::reload_vid_labels_index` and
`StorageManager::pause_compaction` are new. Lance version cleanup no longer
fails on a tagged old version (`error_if_tagged_old_versions: false`).

**Parameter discovery** (`uni-cypher`). `parse_with_parameters` and
`parse_locy_with_parameters` return the parsed statement and the names of the
parameters it references.

**Locy internals for Wave P1** (`uni-locy`, `uni-query`).
`DerivedFactSet::mutation_params` (hidden, like `mutation_queries`) holds the
program's parameters for the replay. `LocyError::Incomplete { detail }` maps to
`UniError::LocyIncomplete`. `LocyCompileError::AssumeBodyCommandNotAllowed` is
removed. `ProofTerm` gains `prob`; `ProvenanceAnnotation` gains `derivation`;
`compute_proof_probability(own, support)` replaces the map-based version and
`ProvenanceStore::base_fact_probs` is removed; `top_k_fact_hash` is new;
`ProvenanceStore::all_entries` returns every retained derivation.
`ReEvaluation` gains `tracker` and `approximate`; `CommandFacts` gains
`tracker()` and `approximate()`; `LocyExecutionContext::is_cut_short` is new
(default `false`); `locy_abduce::evaluate_abduce_with` validates against
`SequencePrograms`; `LocyProgramExec::set_allow_partial` is new.

**Bulk visibility and fork bulk markers** (`uni-store`, `uni-bulk`, `uni-db`).
`StorageManager::compact` takes `self: &Arc<Self>`. The `MainVertexDataset` /
`MainEdgeDataset` readers take `version: impl IntoRowVisibility` (an
`Option<u64>` still works). New: `uni_store::storage::bulk_gate`
(`BulkVisibilityGate`, `GateHold`, `RowVisibility`), `StorageManager::bulk_gate`,
`row_visibility`, `edge_row_visibility`, `visibility_at`, and
`MainEdgeDataset::find_visible_edges_by_type_names_counted`. `BulkBackend` gains
`track_task` (a struct literal must set it). `ForkBranching` gains
`staged_record_on_branch` and `publish_staged_record_on_branch` (with failing
defaults). New `Session::bulk_load_applied` (Python `Session.bulk_load_applied`,
`AsyncSession.bulk_load_applied`). Failpoint seam `bulk::redrive::attempt`.


**Locy command sequencing** (`uni-locy`, `uni-query`). `LocyStats` gains
`sequenced_strata_evaluated` and `sequenced_strata_reused` (a struct literal
without `..Default::default()` stops compiling). `LocyExecutionContext` gains
`re_evaluate_sequenced` and `base_state_token`, both with defaults.
`CollectedDeriveOutput` gains `bound_vids`. `uni_locy::incremental` gains
`query_may_write` and `command_write_footprint`. The ASSUME body helpers moved
to the new `uni_query::query::df_graph::locy_sequence` module, which both the
top-level dispatch and ASSUME bodies use.

**`LogicalPlan::Copy` is removed** (`uni-query`). The planner never built
it; `COPY` plans as `CopyTo` / `CopyFrom`. The executor code behind it, a
second COPY implementation, is gone with it. `Executor::set_file_sandbox` now
overrides `config.file_sandbox`.

**Minimum supported Rust version is now 1.96 (was 1.91).** The WASM plugin
runtime moves to wasmtime/wasmtime-wasi 49 (and extism 1.41), the first
releases carrying the fixes for RUSTSEC-2026-0352, -0359, -0362 and -0363;
wasmtime 49 declares `rust-version = "1.96.0"`. rustls moves to 0.23.46
(RUSTSEC-2026-0346 to -0349). Run `rustup update stable` before building.

**A Locy `ASSUME … THEN` returns one result per body command.**
`CommandResult::Assume(Vec<FactRow>)`, the body commands' rows concatenated,
is now `CommandResult::Assume { results: Vec<CommandResult>, derived:
HashMap<String, Vec<FactRow>> }`. `results` holds, in order, the result each
body command gives at the top level: `Query(rows)`, `Derive { affected }`,
`Cypher(rows)`, a nested ASSUME's own `Assume { .. }`. A `QUERY; DERIVE;
QUERY` body used to return one list in which the first QUERY's rows, the
`{affected: n}` row and the second QUERY's rows could not be told apart.
`derived` is every rule's facts (the program's and the body's) in the
hypothetical state the body's first command reads, after the ASSUME's
mutations: the point the top-level `LocyResult::derived` is taken at.
`CommandResult::as_assume()` returns both; `CommandResult::rows()` returns the
first row-producing result, searching an ASSUME depth first. Migration: replace
`CommandResult::Assume(rows)` with `CommandResult::Assume { results, .. }` and
pick the command (`results[i]`), or call `.rows()` for a single-command body.
Python: `AssumeCommandResult` gains `.results` and `.derived`; `.rows` is the
first row-producing body result's rows (depth first), or `[]`, so a
single-command body reads as before.

**`ABDUCE` and `EXPLAIN RULE` inside a Locy `ASSUME … THEN` body run in the
hypothetical state.** An `ABDUCE` or `EXPLAIN RULE` in the body (or in a nested
ASSUME's body) used to be evaluated and its result silently dropped. Each now
returns its result among the ASSUME's `results`, the same result the command
gives at the top level of a program run in a transaction that applied the
ASSUME's mutations and is rolled back. `EXPLAIN RULE` explains the hypothetical
facts from the derivations recorded while deriving them, and `ABDUCE` validates
each candidate on top of the hypothetical state against every rule the body
reads, the body's own rules included. (An intermediate development state
rejected both with `LocyCompileError::AssumeBodyCommandNotAllowed`; that variant
is gone.)

**A Locy `QUERY` inside an `ASSUME` body reads exact probabilities under
`exact_probability`.** The shared-proof correction runs only when the program
records provenance, and an ASSUME body's evaluation recorded none, so a body
`QUERY` read the independence approximation of a value the same `QUERY` read
exactly at the top level: 0.484375 against 0.46875 for reachability over a
five-edge diamond with every edge 0.5, with no error. Every evaluation of a
program that records provenance now records it.

**The in-memory CDC provider is no longer registered by default.** Every
database registered `MemoryCdcOutputProvider`, which kept every committed batch
until shutdown and made every commit extract CDC events (236.6 MB of RSS over
20,000 small commits, against 55.8 MB without it). A database now has no CDC
stream unless a provider is added. To keep the old behavior, add it explicitly
and keep a handle to read the batches back:

```rust
use uni_plugin_builtin::extras::{MemoryCdcOutputProvider, MemoryCdcPlugin};
let captured = MemoryCdcOutputProvider::new();
db.add_plugin(MemoryCdcPlugin::with_provider(captured.clone()))?;
// ... commits ...
let batches = captured.delivered();
```

`MemoryCdcOutputProvider` is no longer a unit struct: construct it with `new()`
or `Default`; it is a cloneable handle with `delivered()` and `delivered_len()`.
`MemoryCdcStream.delivered` is now `Arc<Mutex<Vec<CdcBatch>>>` (shared with the
provider that started the stream) instead of `Vec<CdcBatch>`.

**Rust `LocyResult::rows()` returns the first row-producing result.** It
returned the first `QUERY` result only, so a program whose first row-producing
command was an `ASSUME … THEN { … }` or a Cypher statement returned that later
`QUERY`'s rows, or `None`. It now takes the first `QUERY` or Cypher result in
program order, searching an `ASSUME`'s body results depth first, as the Python
binding's `rows` does. Use `command_results()` to reach a specific command.

**A writable open waits for a compaction still running on the same local
path.** Dropping a `Uni` handle did not stop an in-flight background compaction,
so reopening the path at once ran two compactors on one store and could return
wrong answers. Compaction now holds a file lock, `<storage>/compaction.lock`,
for each pass, and a dropped handle cancels its pass and joins its background
tasks for up to 5 s. A writable open of a local store waits up to 30 s for that
lock and then fails with "another handle is still compacting"; remote stores are
unaffected. A cancelled pass reports `CompactionCancelled`.

## New

- **`execution_batch_size`** (`UniConfig` field and Python `config` key) sets the number of rows
  per batch inside the query engine (DataFusion's `batch_size`). `None`, the default, keeps 8,192.
  It never changes results; a smaller value bounds per-batch memory. `batch_size` is unchanged and
  sizes the pages a query cursor returns; it was previously documented as an execution setting,
  which it never was.
- **Label disjunction in expressions.** `WHERE n:A|B` and `n IS :A|B` were parse errors, though a
  pattern accepted `:A|B`. They now mean `n:A OR n:B`. Mixing disjunction with conjunction
  (`n:A:B|C`) is refused.
- **Locy `QUERY ... RETURN` aggregates** group as Cypher's `RETURN` does. `count(*)` failed with
  "unsupported expression: Wildcard" and `sum(x)` returned `x`. `SKIP` and `LIMIT` accept any
  expression; `LIMIT $n` was silently ignored.
- **A Locy rule body may join an expression and an `IS` reference with `AND`.** `WHERE x AND NOT a
  IS r TO b` failed to parse. An `OR` before the reference is still refused.
- **TCK harnesses read `UNI_TCK_PARALLELISM` and `UNI_TCK_EXECUTION_BATCH_SIZE`**, so the existing
  Cypher and Locy expectations double as a determinism check across engine settings.

**`MATCH ()-[r:R]->()` reads its sources from the edge type.** An
unconstrained source that the planner cannot narrow to one declared label (an
unverified legacy type, a type with several source labels, `[:R|S]`, or any
type) was a scan of every vertex followed by a traversal from each. The source
scan is now restricted at run time to the vertices that hold an edge of the
pattern's types in its direction, read from the adjacency the traversal itself
reads (new `EdgeSourceScanExec`), so the rows are unchanged: direction,
undirected patterns (each edge twice), self-loops, deletes, unflushed and
transaction-local edges, forks, pinned snapshots and in-flight bulk loads were
compared against the old plan. LDBC SF1, `ldbcdb8`, unverified `WORK_AT`,
release CLI: `RETURN max(w.workFrom)` 5.6–6.9 s → 0.33–0.62 s, `count(w)`
5.6–7.6 s → 0.11–0.13 s, process peak RSS 1.24–1.63 GB → 0.82–0.91 GB. Inside
a read-write transaction with SSI, and when the sources are a quarter or more
of all vertices, the whole table is still read. Variable-length and `OPTIONAL`
hops are unchanged.

**Python: `UniParameterMissingError` and `UniUncommittedWritesError`.** The
typed `UniError::ParameterMissing` and `UniError::UncommittedWritesInTransaction`
reached Python as the base `UniError`. They now raise
`UniParameterMissingError` (a `UniQueryError`, with `names`) and
`UniUncommittedWritesError` (a `UniTransactionError`, with `operation` and
`reason`).

**Python: compaction and write-throttle settings.** `config({...})` accepts
`"compaction"` and `"throttle"` as nested dicts with every field of
`CompactionConfig` and `WriteThrottleConfig` (durations in seconds or as a
`timedelta`); only the keys given change, an unknown key is a `ValueError`, and
`Uni.config()` reports both. `compaction.enabled = False` with a low
`throttle.hard_limit` is how `UniWriteStalledError` is reached from Python.

**Write-throttle metrics.** `DatabaseMetrics::write_throttle_pressure` was
always 0; it now reads 0 below `throttle.soft_limit`, rises through the soft
band, and is 1 at `hard_limit`. New `write_throttle_waits`,
`write_throttle_wait_time` and `writes_stalled` count the writes that waited,
the time they waited, and the writes refused with `WriteStalled` (Python:
`write_throttle_waits`, `write_throttle_wait_time_secs`, `writes_stalled`).
Prometheus: `uni_write_throttle_waits_total`, `uni_write_throttle_wait_seconds`,
`uni_write_stalled_total`. `l1_run_count` is documented as what it is: the
uncompacted flush generations, not completed compaction runs.

**`CompiledProgram::needs_provenance_tracker()`.** Reports whether running a
compiled Locy program attaches a provenance tracker (an `EXPLAIN RULE` command,
or an `MNOR` / `MPROD` fold, anywhere in the program, ASSUME bodies included).
The runtime now uses this method to decide, and when it holds every evaluation
of the program records provenance and `ASSUME` / `ABDUCE` reuse no strata, so
`LocyStats::hypothetical_strata_reused` stays zero for such programs.

**Python: `Session.scratch()` and `AsyncSession.scratch()`.** They expose
Rust's `Session::scratch()`: a transaction with read-your-writes over a pinned
read base whose `commit()` is refused, so its writes are always discarded on
`rollback()` or drop. It is the cheap way to run a command such as `ABDUCE`
against a hypothetical change.

**The storage engine's caches can be sized.** Every table a database opens
reads through one Lance session whose metadata and index caches default to
1 GiB and 6 GiB. They fill as tables are flushed and read and are trimmed only
when full, so a long-running process that flushes often kept growing toward
that size (about 100–170 KB per flush in the W2 commit-loop probe). Two new
`UniConfig` fields set the capacities: `lance_metadata_cache_bytes` and
`lance_index_cache_bytes` (`None`, the default, keeps Lance's sizes; `Some(0)`
disables a cache). A fork shares its primary's caches. Both are accepted by
the Python `config({...})` dict, and `DatabaseMetrics` (Rust and Python) now
reports occupancy as `storage_metadata_cache_bytes` and
`storage_index_cache_bytes`. `StorageBackend` gains a defaulted
`cache_usage()` method.

**A correlated `EXISTS` or `COUNT` between two bound nodes runs once per
source.** `MATCH (a), (b) WHERE NOT EXISTS { MATCH (a)-[:R*1..]->(b) }`
executed its body once per `(a, b)` pair. When the body names one outer node
only as a bare `(b)`, rows that share every other import are now answered by
one execution with `b` unbound. Over 52 x 52 pairs the body ran 52 times
instead of 2,704: 1,383 ms → 105 ms with half the edges in L0 and a two-row
batch, 504 ms → 26 ms flushed with the default batch (release build). Answers are unchanged for every direction and
range, including bounded ones such as `*2..3`. A body that reads
every endpoint it imports (a label, a property map or a `WHERE` on each) keeps
the per-row evaluation, as do `COLLECT` and `OPTIONAL MATCH` bodies.

**`shortestPath` / `allShortestPaths` accept a relationship map computed per
row.** `MATCH (a), (b) MATCH p = shortestPath((a)-[:R* {w: a.x}]-(b))` was
refused; it now returns, for each row, the shortest path whose every
relationship satisfies the map with that row's values (Neo4j accepts these
too). Rows are searched per distinct value set; a `NULL` value matches no
relationship. A value naming the path or its relationship variable is refused.

**Quantified path pattern maps read only what the walk can reach.** A per-hop
map, `((x)-[:R {w: 0}]->(y)-[:R]->(z)){1,2}`, read every edge of the type
before an anchored walk; it is now read over the region the walk reaches, as
variable-length maps already were. From one anchor: 401 → 11 rows read at 400
`:R` edges, 1,601 → 11 at 1,600. Answers are unchanged.

**`x IN list` with the list repeated across rows is hashed once per run.** A
list from `WITH` or `collect()` carried over the rows of an `UNWIND` (the LDBC
IC5 shape) was scanned once per row. Each run of identical rows now builds one
hash index and probes it: with lists of 4,096 items over 8,192 rows the test
went from 613 ms to 91 ms (debug build, over the cost of carrying the lists).

**The doubled-backtick escape in quoted names.** `` `a``b` `` is the name
``a`b``, as in openCypher, for every kind of name, and `` $`p q` `` is a
parameter. A name containing a backtick could not be written before.
`get_label_info` / `get_edge_type_info` now count elements whose name contains
a backtick instead of refusing them.

**Locy `plan_time` includes ASSUME and ABDUCE planning, and reused strata are
not planned again.** `LocyResult::metrics().plan_time` (Python `plan_time_ms`)
used to cover only the program's first plan; the plans each ASSUME body and
ABDUCE candidate build for their hypothetical state are now added. A stratum
such a state reuses is no longer planned: 8 ASSUMEs over a 60-stratum program
that reuse every stratum went from 26.7 ms to 3.4 ms of planning.

**A recursive `BEST BY` merge costs its delta, not the whole fact set.** Each
merge re-sorted every fact with the new candidates; it now keeps the held best
row per KEY in an index and touches only the KEYs the candidates name. On a
40-layer graph of 19,500 facts the merge took 0.73 s instead of 4.4 s over the
run, with the same facts and deltas.

**Range, prefix and null predicates use scalar indexes.** On a property with an
Online BTree index, `<`, `<=`, `>`, `>=` (and `AND`-ed pairs), `STARTS WITH`
(literal or parameter) and `IS [NOT] NULL` narrow the scan through the index
instead of reading the whole label; a Hash index serves `IS [NOT] NULL`. A
selective range over 32,000 rows went from 37 ms to 7-8 ms. A range that matches
most of the label is about 1.5x slower than before (measured 60 ms against 38 ms
at 32,000 rows), the cost of re-checking each returned row against newer
versions. See *Performance Tuning → Use Predicate Pushdown*.

**The unnamed form `MATCH shortestPath(...)` plans.** It failed with
`shortestPath must be assigned to a variable`; it now matches like the named form.

**Equality lookups on unflushed rows of an undeclared label and by `ext_id`
read only matching rows.** `MATCH (n:M) WHERE n.p = $v` on a label the schema
does not declare read every unflushed `M` vertex (2,000 / 8,000 rows) where the
inline `(n:M {p: $v})` read 1; the `WHERE` form now reads 1 too, as do `IN` and
`AND` with other conjuncts. `MATCH (n:L {ext_id: $x})` and
`WHERE n.ext_id = $x` on a declared label read every unflushed `L` row; they now
read 1.

## Testing and CI

**Python API validation:** 1,189 tests passed with 15 existing skips on the
rebuilt extension. Sync/async and runtime/stub parity checks, stubtest, strict
binding Clippy, formatting, static guards and slim ONNX binding compilation
passed. The Rust/Python source guard now covers 19 facade/builder/result types,
12 returned data types, database and Locy configuration, and all cloud config
variants. Native Rust handles and language-specific API differences remain
explicit exclusions; see the [API parity review](../proposals/python_api_parity_2026_10_27.md).

Regression coverage includes query equivalence and randomized oracle harnesses
that run on PRs and nightly:

- **Query-rewrite relations** (`metamorphic::dqp::topo`) hold data and engine fixed and compare
  equivalent formulations: named ↔ anonymous relationship, `OPTIONAL MATCH` ↔ `MATCH` ⊎ `NOT
  EXISTS`, `EXISTS` ↔ `COUNT > 0`, `*i..j` ↔ unrolled hops, `DISTINCT` ↔ grouping, aggregates ↔
  `reduce` over `collect`, `UNWIND` relations, and five Locy-vs-Cypher relations. The fixture has
  self-loops, parallel edges, cycles and a dense cluster, and runs flushed and half in L0 at a
  two-row batch.
- **A wide DQP tier** puts a label across more than one scan slice with vid gaps, which is what the
  chunked-scan data loss needed and no earlier tier produced.
- **The Locy naive oracle** now evaluates `FOLD`, recursive `FOLD`, `ALONG`, `BEST BY` and `PROB`
  over bags, against random multigraph programs.
- **The DQP fork, pinned and flush levers** now run Locy programs, one per rule family.
- Debug builds assert that every Locy rule's facts are distinct rows and that two fold candidates
  with the same derivation key have the same input.

Black Book Appendix B2 records the invariants these fixes established, each with the test that
pins it.

Parser regression fixtures and fuzz seeds cover nested-list and pattern
backtracking. Lexical lookahead and shared-prefix grammar factoring avoid
exponential parsing on the captured malformed inputs; probability projection
forms such as `p AS PROB` retain their annotation and alias behavior.

TCK report scripts generate reports for both schema modes and preserve failing
test exit codes. The nightly binding soak runs five 10,000-case shards, retaining
the full 50,000-case workload and existing time limits. Expanded indexed-property
re-key tests cover streaming and concurrent stores; passing coverage does not
establish a fix for the original intermittent #311 report.

**Performance qualification remains pending.** The local strict IAI comparison
flags a 53.84% instruction reduction for vertex lookup, beyond its 50%
excessive-improvement guard. Comparable CI-runner measurements are still needed;
the baseline and thresholds are unchanged. See the
[CI completion report](../proposals/parser_backtracking_2026_10_27.md).

## Dependencies and build requirements

- Lance 13.0.0, Arrow/Parquet 58.4.1, and DataFusion 54.1.0 stay aligned.
  New databases use storage format 2.1; existing formats are preserved.
- Locy's built-in linear classifier loads safetensors directly; minimal
  Lance-backed builds no longer pull in Candle through Locy. Optional embedding
  providers retain their existing feature controls.
- Minimum supported Rust is 1.96. Wasmtime/wasmtime-wasi 49 and Extism 1.41
  replace the older runtime; the advisory exceptions listed in the abandoned
  4.2 draft are no longer the current policy. The current lock resolves
  wasmtime 49.0.4 and rustls 0.23.46.
- async-trait 0.1.92 and PyO3 0.29.3 retain compatibility with the current
  compiler and bindings. The local CI runbook includes the workflow checks.
