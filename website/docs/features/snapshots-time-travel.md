# Snapshots & Time Travel

Uni supports point-in-time snapshots and time-travel queries. Snapshots are durable checkpoints; time travel lets you query historical versions without mutating state.

## What It Provides

- Create, list, restore and delete snapshots.
- `VERSION AS OF` and `TIMESTAMP AS OF` queries for historical reads.
- Read-only time-travel safety checks.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let snap_id = db.create_snapshot("daily").await?;
    let rows = session.query(&format!(
        "MATCH (n) RETURN count(n) AS c VERSION AS OF '{}'",
        snap_id
    )).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()

    # Snapshot via procedure: an administrative call, so `run`, not `query`
    rows = session.run("CALL uni.admin.snapshot.create('daily') YIELD snapshot_id RETURN snapshot_id")
    snap_id = rows[0]["snapshot_id"]

    rows = session.query(
        f"MATCH (n) RETURN count(n) AS c VERSION AS OF '{snap_id}'"
    )
    print(rows)
    ```

## What a Historical Read Sees

A read at a snapshot returns the graph as it was when the snapshot was taken:
vertices deleted, relabelled or changed later are returned as they were, and
vertices, relationships and bulk loads committed later are not. This holds for
`VERSION AS OF`, for a session pinned with `pin_to_version` or
`pin_to_timestamp`, and for the vertices a transaction reads.

- A snapshot taken with `create_snapshot` (or `uni.admin.snapshot.create`)
  reads the table versions it recorded, so it keeps answering the same after
  compaction and later writes, until it is deleted.
- `pin_to_timestamp` and `TIMESTAMP AS OF` can resolve to the checkpoint an
  ordinary flush wrote. Such a checkpoint reads the current tables under its
  version, and compaction merges the older row versions it would need. While a
  session is pinned to it, compaction keeps those rows. A pin made after a
  compaction pass merged history the checkpoint needs is refused with an
  `InvalidArgument` error. Take an explicit snapshot for a point you need to
  read later.
- A bulk load becomes visible all at once, to readers that start after it
  commits. A snapshot, flush or transaction started while it was loading never
  sees it, and restoring such a snapshot does not bring any of it back.

## Restoring a Snapshot

`restore_snapshot(id)` (Python `db.restore_snapshot(id)`, Cypher
`CALL uni.admin.snapshot.restore(id)` through `session.run()` — `session.query()`
is read-only and refuses it) puts the database back to the snapshot:
from the moment it returns, in the same handle and after a reopen, reads see
the snapshot's nodes, relationships, labels and property values, and nothing
written after it -- whether those writes had been flushed or not. New writes
build on the restored state.

- Only snapshots taken with `create_snapshot` / `uni.admin.snapshot.create` can
  be restored. They keep the storage versions they need for as long as they
  exist. Other entries in `uni.admin.snapshot.list` (written by ordinary
  flushes), and snapshots taken by earlier releases, are refused.
- A restore is refused while a transaction is open, inside a transaction, while
  a bulk load is publishing, and on a fork. It cannot be undone except by
  restoring another snapshot, so take one first if you may want to go back.
- The schema is restored with the data. Labels, relationship types,
  properties, indexes and constraints declared after the snapshot are gone;
  ones dropped since are back; a property renamed or redeclared with another
  type since has its old name and type again.
- Background index builds wait while a restore runs, and a build already under
  way finishes before the restore starts rewinding. Builds queued meanwhile run
  afterwards, against the restored data.
- Snapshots taken before restore points recorded the schema (an earlier
  development build) are refused with an error saying so; delete them with
  `delete_snapshot`.
- A restore is refused while the database has any fork, with
  `UniRestoreBlockedByForksError` in Python (`UniError::RestoreBlockedByForks`
  in Rust), whose `forks` attribute names them. Forks are not rewound, so a fork
  kept across a restore would no longer match the restored database. Drop the
  forks first (`drop_fork`, or `drop_fork_cascade` for a fork with children),
  then restore. A fork being created while a restore runs fails with a
  `UniForkLifecycleError` whose `stage` is `"restore_race"`; retry it.
- If the process stops during a restore, the next open completes it.

## Deleting a Snapshot

A restorable snapshot keeps every storage version it needs for as long as it
exists, whatever `compaction.version_retention` says, so old snapshots hold
disk space. `delete_snapshot(id)` (Python `db.delete_snapshot(id)`, Cypher
`CALL uni.admin.snapshot.delete(id)` through `session.run()`) removes the
snapshot: it no longer appears in `list_snapshots`, cannot be restored or read
with `VERSION AS OF`, and the next compaction reclaims the versions only it
kept. Other snapshots keep their own versions and still restore.

- An unknown id raises `UniSnapshotNotFoundError` in Python (with a
  `snapshot_id` attribute) and `UniError::SnapshotNotFound` in Rust.
- A deletion is refused while a restore runs or is incomplete, while a fork
  was created from the snapshot, inside a transaction, and on a read-only
  handle or session.
- A deletion that fails part way can be run again.

## Use Cases

- Auditing and reproducible analytics.
- Debugging or regression analysis.
- Checkpointing before bulk operations.

## When To Use

Use snapshots and time travel when you need historical reads or safe rollbacks without exporting or duplicating data.
