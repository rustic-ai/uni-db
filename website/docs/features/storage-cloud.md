# Storage & Cloud Durability

Uni stores data in Lance tables on local disk or in an object store: Amazon S3 and S3-compatible services (MinIO, Cloudflare R2, LocalStack), Google Cloud Storage, and Azure Blob Storage. A store can live entirely in the object store, or in **hybrid mode** keep its write-ahead log on local disk and everything else in the object store.

## What It Provides

- Local filesystem storage for low-latency access.
- Object-store durability via `s3://`, `gs://`, `az://` (and `abfss://`).
- Many stores in one bucket, each under its own prefix.
- Hybrid mode: a local WAL for fast durable commits, data in the object store.

## Opening a store

=== "Rust"
    ```rust
    use uni_db::{CloudStorageConfig, Uni};

    # async fn demo() -> Result<(), uni_db::UniError> {
    // Configured from the environment (AWS_* variables, IRSA, ECS task roles, ...).
    let db = Uni::open("s3://my-bucket/graph").build().await?;

    // Or explicitly — e.g. MinIO with static keys.
    let db = Uni::open("s3://my-bucket/graph")
        .cloud_storage(CloudStorageConfig::S3 {
            bucket: "my-bucket".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("http://localhost:9000".into()),
            access_key_id: Some("minio".into()),
            secret_access_key: Some("minio-secret".into()),
            session_token: None,
            virtual_hosted_style: false,
        })
        .build()
        .await?;

    // Any object_store option, passed to both storage clients.
    let db = Uni::open("az://my-container/graph")
        .storage_option("azure_storage_use_emulator", "true")
        .build()
        .await?;
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.UniBuilder.open("s3://my-bucket/graph").build()

    db = (
        uni_db.UniBuilder.open("s3://my-bucket/graph")
        .cloud_config({
            "provider": "s3",
            "bucket": "my-bucket",
            "region": "us-east-1",
            "endpoint": "http://localhost:9000",
            "access_key_id": "minio",
            "secret_access_key": "minio-secret",
        })
        .build()
    )

    db = (
        uni_db.UniBuilder.open("az://my-container/graph")
        .storage_options({"azure_storage_use_emulator": "true"})
        .build()
    )
    ```

=== "CLI"
    ```bash
    uni repl --path s3://my-bucket/graph \
        --storage-option aws_endpoint=http://localhost:9000
    ```

### Where configuration comes from

One set of options configures both storage clients — Uni's own (catalog, WAL, ID allocator) and Lance's (tables and indexes) — so they always talk to the same endpoint with the same credentials. From lowest to highest precedence:

1. **The environment**: `AWS_*` (including `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, web-identity (IRSA) and container credentials; named profiles in `~/.aws/config` are not read — export them first, e.g. `eval "$(aws configure export-credentials --format env)"`), `GOOGLE_*` and `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_*` (`AZURE_STORAGE_ACCOUNT` is accepted for the account name).
2. **The URI**: bucket or container, and the prefix.
3. **The typed configuration** (`cloud_storage` / `cloud_config`). A field left unset falls back to the environment.
4. **Raw storage options** (`storage_option` / `storage_options` / `--storage-option`), using [`object_store` configuration keys](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html) such as `aws_endpoint`, `aws_conditional_put`, `google_service_account`, `azure_storage_use_emulator`, `timeout`. Keys only Lance understands (`client_max_retries`, …) are passed to Lance alone.

Also:

- If the URI and the configuration name different buckets, the open fails.
- An `http://` endpoint enables plain HTTP unless `allow_http` is set.
- With a custom endpoint and no region, the region defaults to `us-east-1`. Without either, the bucket's region is looked up.
- An `AWS_SESSION_TOKEN` in the environment is used together with static keys from the configuration or options, by both storage clients. Unset it when you give long-lived keys explicitly.

### URI forms

| Provider | Form |
|---|---|
| S3 | `s3://bucket/prefix` (also `s3a://`) |
| GCS | `gs://bucket/prefix` (also `gcs://`) |
| Azure | `az://container/prefix`, account from the configuration or `AZURE_STORAGE_ACCOUNT_NAME`; or `abfss://container@account.dfs.core.windows.net/prefix` |
| Local | a path, or `file:///path` |

The form `az://account/container`, documented before 5.4, is still accepted, with a deprecation warning, when the configured account (typed configuration, storage option or environment) is the URI's host.

## Layout

Every store, local or remote, has the same layout under its root `R` (the directory, or the bucket plus prefix):

```text
R/catalog/            schema, Locy rules, fork registry, layout.json
R/wal/                write-ahead log          (hybrid: local directory)
R/id_allocator.json   ID allocator             (hybrid: local directory)
R/storage/            tables, indexes, snapshot manifests, fork WALs
```

Nothing a store writes leaves its prefix, so several stores can share a bucket. `catalog/layout.json` records the layout.

## Hybrid mode

=== "Rust"
    ```rust
    let db = Uni::open("./local-wal")
        .hybrid("s3://my-bucket/graph")
        .build()
        .await?;
    ```

=== "Python"
    ```python
    db = (
        uni_db.UniBuilder.open("./local-wal")
        .hybrid("./local-wal", "s3://my-bucket/graph")
        .build()
    )
    ```

The WAL and ID allocator live in the local directory, and everything else lives in the object store. Configure the remote side as for a remote-only store. In Python, `hybrid`'s first argument must be the path the builder was opened at.

- Commits are durable once written to the local WAL. Until the next flush (by default within 5 seconds) they exist only on that host.
- If the local directory is lost, the store reopens from the object store with a fresh directory. ID allocation resumes above every ID in use. Commits that were not yet flushed are lost.
- A hybrid store has one host. Do not open it from two hosts at once.

## Concurrency

A store, local or remote, supports **one writable handle at a time**. Uni does not yet take a lock or lease on the store, so run a single writer per store. On an object store, the ID allocator needs conditional writes and detects a concurrent writer through them; a service without them fails the first ID reservation. For an S3-compatible service, set `aws_conditional_put` to the mechanism it supports.

## Stores written before 5.4

- **Local stores and bucket-root remote stores** open unchanged.
  - A local store already has the standard layout.
  - A bucket-root remote store, which keeps its tables directly under the root, is recognized as the `flat` layout and stays that way. Its tables are never moved, so its forks keep working.
- **Remote stores under a prefix** (`s3://bucket/prefix`) are refused with an error naming the files involved. Before 5.4, such a store wrote its catalog, snapshot pointer and WAL to the **bucket root**, not under its prefix, so stores in one bucket could overwrite each other's metadata. To open one:
  1. Make sure no other store in the bucket still uses the bucket-root files.
  2. Copy the bucket root's `catalog/` to `prefix/catalog/`.
  3. For a remote-only store, also copy `wal/` and `id_allocator.json` under the prefix.
  4. Open the store. It is recognized as `flat`.

## Use Cases

- Durable storage without running a separate DB server.
- Large datasets that fit better in object storage.
- Many tenants or environments in one bucket, one prefix each.
- Hybrid deployments with fast local writes.

## When To Use

Use hybrid or object-store mode when local disk is limited, or when you need cloud-backed durability and easy backups.
