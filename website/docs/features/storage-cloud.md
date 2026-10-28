# Storage & Cloud Durability

Uni stores data in Lance tables and can persist to local disk or object storage (S3, GCS, Azure). You can use hybrid mode to keep metadata local while storing bulk data remotely.

## What It Provides

- Local filesystem storage for low-latency access.
- Object-store durability via `s3://`, `gs://`, `az://`.
- Hybrid mode for fast local WAL/metadata + remote data.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./local_meta")
        .remote_storage("s3://my-bucket/graph-data", CloudStorageConfig::s3_from_env("my-bucket"))
        .build()
        .await?;
    let session = db.session();

    session.query("MATCH (n) RETURN count(n)").await?;
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./local_meta") \
        .hybrid("./local_meta", "s3://my-bucket/graph-data") \
        .build()
    session = db.session()

    session.query("MATCH (n) RETURN count(n)")
    ```

## Use Cases

- Durable storage without running a separate DB server.
- Large datasets that fit better in object storage.
- Hybrid deployments with fast local writes.

## When To Use

Use hybrid or object-store mode when local disk is limited or when you need cloud-backed durability and easy backups.
