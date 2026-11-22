# Bulk Ingest

Uni includes a bulk writer optimized for high-throughput ingestion. It batches writes and rebuilds indexes at commit time for faster loading.

## What It Provides

- High-throughput vertex and edge insertion.
- Deferred index rebuilding for faster loads.
- Progress tracking and stats.

## Example

The bulk writer is strict: the labels and edge types it writes must be declared first.

=== "Rust"
    ```rust
    use std::collections::HashMap;
    use uni_db::{EdgeData, Uni, Value};

    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE KNOWS () FROM Person TO Person").await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    let mut writer = tx.bulk_writer().build()?;

    let mut v1 = HashMap::new();
    v1.insert("name".to_string(), Value::from("Alice"));
    let mut v2 = HashMap::new();
    v2.insert("name".to_string(), Value::from("Bob"));

    let vids = writer.insert_vertices("Person", vec![v1, v2]).await?;
    writer
        .insert_edges("KNOWS", vec![EdgeData::new(vids[0], vids[1], HashMap::new())])
        .await?;
    let stats = writer.commit().await?;

    println!("{:?}", stats);
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()

    tx = session.tx()
    tx.execute("CREATE LABEL Person (name STRING)")
    tx.execute("CREATE EDGE TYPE KNOWS () FROM Person TO Person")
    tx.commit()

    tx = session.tx()
    writer = tx.bulk_writer().build()

    vids = writer.insert_vertices("Person", [
        {"name": "Alice"},
        {"name": "Bob"},
    ])
    writer.insert_edges("KNOWS", [(vids[0], vids[1], {})])
    stats = writer.commit()

    print(stats)
    ```

## Commit, Abort and Recovery

- **Nothing is visible before `commit()`.** Rows are staged; `commit()` makes the whole load visible at once, and readers never see part of it. `abort()` discards everything staged.
- **The load commits on its own.** Committing or rolling back the transaction the writer was built from neither adds to nor undoes the load.
- **`commit()` says whether to retry.** `Ok(stats)` means the load is applied. `BulkCommitIncomplete` means it is committed and will be finished in the background or at the next open: do not retry. `BulkCommitOutcomeUnknown` means the commit point may or may not have been written: reopen and check `db.bulk_load_applied(load_id)` (the id is also `stats.load_id`) before retrying. Any other error leaves nothing behind, so retrying is safe.
- **Before-triggers block a load.** A synchronous before-trigger that watches what the load writes refuses it with `BulkLoadBlockedByTrigger`; `skip_before_triggers(true)` on the builder loads anyway without running them. After-phase triggers and CDC still receive the load's events.

See [Data Ingestion](../guides/data-ingestion.md#what-commit-returning-means) for the full contract.

## Use Cases

- Initial dataset loads.
- Importing large CSV/JSONL exports.
- Batch updates after offline processing.

## When To Use

Use bulk ingest when load speed matters more than per-write latency and you can rebuild indexes at the end.
