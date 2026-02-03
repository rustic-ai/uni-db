# Bulk Ingest

Uni includes a bulk writer optimized for high-throughput ingestion. It batches writes and rebuilds indexes at commit time for faster loading.

## What It Provides

- High-throughput vertex and edge insertion.
- Deferred index rebuilding for faster loads.
- Progress tracking and stats.

## Example

=== "Rust"
    ```rust
    use std::collections::HashMap;
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let mut writer = db.bulk_writer().build()?;

    let mut v1 = HashMap::new();
    v1.insert("name".to_string(), serde_json::json!("Alice"));
    let mut v2 = HashMap::new();
    v2.insert("name".to_string(), serde_json::json!("Bob"));

    let vids = writer.insert_vertices("Person", vec![v1, v2]).await?;
    writer.insert_edges("KNOWS", vec![(vids[0], vids[1], HashMap::new())]).await?;
    let stats = writer.commit().await?;

    println!("{:?}", stats);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    writer = db.bulk_writer().build()

    vids = writer.insert_vertices("Person", [
        {"name": "Alice"},
        {"name": "Bob"},
    ])
    writer.insert_edges("KNOWS", [(vids[0], vids[1], {})])
    stats = writer.commit()

    print(stats)
    ```

## Use Cases

- Initial dataset loads.
- Importing large CSV/JSONL exports.
- Batch updates after offline processing.

## When To Use

Use bulk ingest when load speed matters more than per-write latency and you can rebuild indexes at the end.
