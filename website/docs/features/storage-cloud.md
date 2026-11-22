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

1. **The environment**: `AWS_*` (including `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, web-identity (IRSA) and container credentials, and a named profile through `AWS_PROFILE` — its credentials, including SSO, assumed roles and `credential_process`, and its `region` and `endpoint_url` where nothing else sets them. Uni reads `~/.aws` only for the profile `AWS_PROFILE` names: set `AWS_PROFILE=default` to use the default profile), `GOOGLE_*` and `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_*` (`AZURE_STORAGE_ACCOUNT` is accepted for the account name).
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

The WAL and ID allocator live in the local directory, and everything else lives in the object store, forks included (a fork's WAL is remote). Configure the remote side as for a remote-only store. In Python, `hybrid`'s first argument must be the path the builder was opened at.

A hybrid store has **one host at a time**: the one holding its lease. Hybrid mode trades the object store's latency on each commit for a window in which recent commits exist only on that host.

- **Commits** are durable once written to the local WAL. They reach the object store at the next flush: by default within 5 seconds, or after 10,000 mutations (`auto_flush_interval`, `auto_flush_threshold`). Until then they exist only on that host.
- **A clean close** (`shutdown()`) flushes, so every commit is in the object store and the next host, on any directory, sees all of it. If that flush fails, the error is logged and the commits stay in the local WAL. A drop without `shutdown()` does not flush either: the commits stay in the local WAL and are replayed when the same directory is opened again.
- **A second host** cannot write while the first holds the store. Its writable open fails with `StoreLocked`; a read-only open sees what has been flushed.
- **Losing the local directory** loses the unflushed commits and nothing else. The store reopens from the object store with a fresh directory, and ID allocation resumes above every ID in use.
- **A host that comes back after another took over** holds a WAL that no longer describes the store. That happens when it crashed (its lease then lapsed) or was dropped without `shutdown()`, with unflushed commits, and another host opened the store meanwhile. Its commits were made against a state the store has moved on from, so they are not replayed: the open fails with `StaleWal`, which names the WAL and how many segments hold commits. Open with `set_aside_stale_wal()` to move those segments to `wal_set_aside/` in the local directory, never deleted, and open the store as the object store has it. A read-only open ignores such a WAL.

The store knows whose WAL it follows: each local directory's WAL carries an id (`wal_id.json`), and each writable open records its WAL's id in the object store (`catalog/wal_claim.json`) before its first commit, whether or not the store has ever been flushed. A store opened before this release has no record; its first open replays the local WAL as before and records it.

## Concurrency

A store, local or remote, has **one writable handle at a time**, and any number of read-only handles alongside it.

- **A local store** (a path or `file://`) is held by an OS lock on `writer.lock`. The OS releases it when the handle closes or the process exits.
- **An object store** is held by a lease object, `catalog/writer_lease.json`. The holder renews it every 10 seconds, and it lapses 30 seconds after the last renewal. A writer that crashed therefore blocks new writers for at most about 30 seconds. Both times are set in `UniConfig::writer_lease`. Closing the handle (`shutdown()`, or dropping it) gives the lease back at once. Prefer `shutdown()`: a drop on a single-threaded async runtime can only send the release once that runtime runs again, so a reopen right after it may wait out `acquire_grace`, and a process that exits straight after the drop leaves the lease to lapse.
- **A second writable open** waits a few seconds (`acquire_grace`) for a handle that is closing, then fails with `StoreLocked`, which names the holder's process and host. Open it read-only instead, or close the other handle.
- **A writer that loses its lease** stops writing and fails with `WriterFenced`. That happens when its lease was taken over after it stalled past expiry, or when it couldn't renew in time. So do sessions of a handle that was closed.
- **Catalog writes** (`latest`, the schema, named snapshots, the fork registry) are compare-and-swap on object stores, so a stale writer cannot overwrite them.

This needs conditional writes (`If-Match` / `If-None-Match`, which S3, GCS, Azure, MinIO and R2 provide). An object store without them is refused. A service that silently ignores those headers cannot be detected, so check yours before relying on the lease. `WriteLease::Unenforced` opens a store with no lock or lease, which means one writer by convention, and logs a warning. It also lets the ID allocator write without conditional writes. `WriteLease::Custom` uses your own provider: it is acquired before the first write, heartbeated, and released on close.

A writer stops on its own once its lease could have lapsed (expiry minus `clock_skew`), even if a stalled renewal never told it so. Two limits remain. A writer stalled in the middle of a table append can complete that one append after resuming; pinning table versions in the snapshot manifest is planned to close this. Lease expiry assumes hosts' clocks agree to within `clock_skew` (5 seconds). Forks created from a read-only handle still write their own fork state.

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
