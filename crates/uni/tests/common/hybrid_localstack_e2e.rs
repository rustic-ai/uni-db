// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use tempfile::tempdir;
use uni_common::CloudStorageConfig;
use uni_db::{DataType, Uni};

#[tokio::test]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_hybrid_localstack_from_zero_e2e() -> Result<()> {
    configure_localstack_env();

    let temp_dir = tempdir()?;
    let local_meta = temp_dir.path().join("meta");
    std::fs::create_dir_all(&local_meta)?;

    let bucket = format!("hybrid-e2e-{}", std::process::id());
    create_localstack_bucket(&bucket).await?;

    let cloud_cfg = localstack_cloud_config(&bucket);
    let remote_url = format!("s3://{}", bucket);

    // Start from empty remote + empty local metadata directory.
    let db = Uni::open(local_meta.to_string_lossy().to_string())
        .hybrid(&remote_url)
        .cloud_storage(cloud_cfg.clone())
        .build()
        .await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.execute(
        "
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS {since: 2024}]->(b)
    ",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;
    drop(db);

    // Reopen and verify graph state survives in hybrid mode.
    let db = Uni::open(local_meta.to_string_lossy().to_string())
        .hybrid(&remote_url)
        .cloud_storage(cloud_cfg.clone())
        .build()
        .await?;

    let res = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(res.rows()[0].get::<i64>("c")?, 2);

    let rel = db
        .session()
        .query("MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.name, b.name, k.since")
        .await?;
    assert_eq!(rel.len(), 1);
    assert_eq!(rel.rows()[0].get::<String>("a.name")?, "Alice");
    assert_eq!(rel.rows()[0].get::<String>("b.name")?, "Bob");
    assert_eq!(rel.rows()[0].get::<i64>("k.since")?, 2024);

    // Mutate after reopen, flush, and verify again after another reopen.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    db.flush().await?;
    drop(db);

    let db = Uni::open(local_meta.to_string_lossy().to_string())
        .hybrid(&remote_url)
        .cloud_storage(cloud_cfg.clone())
        .build()
        .await?;

    let res = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    assert_eq!(res.rows()[0].get::<i64>("c")?, 3);
    drop(db);

    // Local metadata artifacts should exist in hybrid mode.
    assert!(local_meta.join("id_allocator.json").exists());
    assert!(local_meta.join("wal").exists());

    // Remote data artifacts should exist in S3.
    let store = build_localstack_store(&bucket)?;
    let schema_bytes = store
        .get(&ObjPath::from("catalog/schema.json"))
        .await?
        .bytes()
        .await?;
    assert!(!schema_bytes.is_empty());

    let object_count = store
        .list(None)
        .try_fold(0usize, |acc, _| async move { Ok(acc + 1) })
        .await?;
    assert!(object_count > 0);

    Ok(())
}

/// A fork, and a fork of it, on a database whose data is on S3.
///
/// A fork is a Lance branch on each of the primary's datasets. The branch
/// code opened those datasets by URI alone, without the storage options the
/// cloud configuration supplies, so creating a fork failed with "open lance
/// dataset at s3://…" wherever the store needed its configured endpoint or
/// credentials. Two things were wrong: the existence check answered "yes" for
/// every dataset on an object store, so a table not created yet (an edge
/// type's adjacency table before its first compaction) was branched and the
/// open failed; and datasets opened by URI alone never received the cloud
/// configuration's endpoint and credentials.
///
/// Run it with `UNI_LOCALSTACK_CONFIG_ONLY=1` as well, which leaves the
/// endpoint and credentials to the cloud configuration (see
/// [`configure_localstack_env`]).
///
/// Multi-threaded on purpose. Dropping a fork session seals the fork's WAL,
/// which is on the remote store, from the process-wide IO runtime; the S3
/// client's pooled connection is driven by this test's runtime, and a
/// single-threaded one is blocked in that very drop, so the request waits out
/// its 30 s timeout before retrying on a fresh connection.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_hybrid_localstack_forks_e2e() -> Result<()> {
    configure_localstack_env();

    let temp_dir = tempdir()?;
    let local_meta = temp_dir.path().join("meta");
    std::fs::create_dir_all(&local_meta)?;
    let bucket = format!("hybrid-fork-{}", std::process::id());
    create_localstack_bucket(&bucket).await?;
    let cloud_cfg = localstack_cloud_config(&bucket);
    let remote_url = format!("s3://{}", bucket);
    let open = || async {
        Uni::open(local_meta.to_string_lossy().to_string())
            .hybrid(&remote_url)
            .cloud_storage(cloud_cfg.clone())
            .build()
            .await
    };
    let people = |session: uni_db::Session| async move {
        let rows = session
            .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
            .await?;
        rows.rows()
            .iter()
            .map(|r| r.get::<String>("name"))
            .collect::<std::result::Result<Vec<_>, _>>()
    };

    let db = open().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2024}]->(b:Person {name: 'Bob'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let primary = db.session();
    let fork = primary
        .fork("f1")
        .await
        .map_err(|e| anyhow::anyhow!("create fork on S3: {e:#}"))?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'Fork'})").await?;
    tx.commit().await?;
    fork.flush().await?;
    let child = fork
        .fork("f2")
        .await
        .map_err(|e| anyhow::anyhow!("create nested fork on S3: {e:#}"))?;
    let tx = child.tx().await?;
    tx.execute("CREATE (:Person {name: 'Child'})").await?;
    tx.commit().await?;
    child.flush().await?;

    // The primary keeps moving after the forks were taken.
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'Later'})").await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(people(db.session()).await?, ["Alice", "Bob", "Later"]);
    assert_eq!(people(fork).await?, ["Alice", "Bob", "Fork"]);
    assert_eq!(people(child).await?, ["Alice", "Bob", "Child", "Fork"]);
    drop(db);

    // Reopened from S3, each fork still reads its own branch.
    let db = open().await?;
    let fork = db.session().fork("f1").await?;
    let child = fork.fork("f2").await?;
    assert_eq!(people(fork).await?, ["Alice", "Bob", "Fork"]);
    assert_eq!(people(child).await?, ["Alice", "Bob", "Child", "Fork"]);
    let edge = db
        .session()
        .query("MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b")
        .await?;
    assert_eq!(edge.len(), 1);

    db.drop_fork_cascade("f1").await?;
    assert!(db.list_forks().await.is_empty());
    assert_eq!(people(db.session()).await?, ["Alice", "Bob", "Later"]);
    Ok(())
}

/// Dropping a fork session on a single-threaded runtime does not stall.
///
/// The drop records the fork WAL's announced mark, and a fork's WAL is on
/// the remote store. Blocking this runtime's only thread on that write
/// starved it: the store's pooled connections are driven by tasks of this
/// runtime, so the request waited out its 30 s timeout before retrying on a
/// fresh connection. The seal is now left to the runtime, and reopening the
/// fork waits for it.
#[tokio::test]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_hybrid_localstack_fork_drop_does_not_stall() -> Result<()> {
    configure_localstack_env();

    let temp_dir = tempdir()?;
    let local_meta = temp_dir.path().join("meta");
    std::fs::create_dir_all(&local_meta)?;
    let bucket = format!("hybrid-drop-{}", std::process::id());
    create_localstack_bucket(&bucket).await?;
    let db = Uni::open(local_meta.to_string_lossy().to_string())
        .hybrid(&format!("s3://{bucket}"))
        .cloud_storage(localstack_cloud_config(&bucket))
        .build()
        .await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let names = |session: uni_db::Session| async move {
        let rows = session
            .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
            .await?;
        rows.rows()
            .iter()
            .map(|r| r.get::<String>("name"))
            .collect::<std::result::Result<Vec<_>, _>>()
    };

    // Committed, not flushed: the fork's WAL has an announced mark to seal.
    let fork = db.session().fork("f1").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'Fork'})").await?;
    tx.commit().await?;
    let started = std::time::Instant::now();
    drop(fork);
    let dropped = started.elapsed();
    assert!(
        dropped < std::time::Duration::from_secs(5),
        "dropping the fork session took {dropped:?}"
    );

    // Reopened at once, the fork has its commit, replayed from its WAL.
    let started = std::time::Instant::now();
    let fork = db.session().fork("f1").await?;
    assert_eq!(names(fork).await?, ["Alice", "Fork"]);
    let reopened = started.elapsed();
    assert!(
        reopened < std::time::Duration::from_secs(10),
        "reopening and reading the fork took {reopened:?}"
    );
    assert_eq!(names(db.session()).await?, ["Alice"]);
    Ok(())
}

// ---- Remote-only stores and prefixes (issues #349, #350) ------------------

fn config_only() -> bool {
    std::env::var_os("UNI_LOCALSTACK_CONFIG_ONLY").is_some()
}

/// A bucket name no earlier run of this test can have used, even with a
/// recycled process id against a long-lived LocalStack.
fn unique_bucket(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

async fn bucket_keys(bucket: &str) -> Result<Vec<String>> {
    let store = build_localstack_store(bucket)?;
    let mut keys: Vec<String> = store
        .list(None)
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await?;
    keys.retain(|k| k != ".marker");
    keys.sort();
    Ok(keys)
}

async fn names(db: &Uni) -> Result<Vec<String>> {
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;
    Ok(rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("name"))
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

async fn seed(db: &Uni, people: &[&str]) -> Result<()> {
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for p in people {
        tx.execute(&format!("CREATE (:Person {{name: '{p}'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

/// #350: `Uni::open("s3://…")` takes its endpoint and credentials from the
/// environment, as Lance already did.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_remote_only_open_from_the_environment() -> Result<()> {
    configure_localstack_env();
    if config_only() {
        // This one is about the environment; the config-only run covers
        // `test_remote_only_open_from_a_typed_config`.
        return Ok(());
    }
    let bucket = unique_bucket("remote-env");
    create_localstack_bucket(&bucket).await?;
    let uri = format!("s3://{bucket}/graph");
    let db = Uni::open(&uri).build().await?;
    seed(&db, &["Alice"]).await?;
    drop(db);
    let db = Uni::open(&uri).build().await?;
    assert_eq!(names(&db).await?, ["Alice"]);
    let keys = bucket_keys(&bucket).await?;
    assert!(keys.iter().all(|k| k.starts_with("graph/")), "{keys:?}");
    Ok(())
}

/// #350: a remote-only store opened with a typed config, without relying on
/// the environment for its endpoint or credentials.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_remote_only_open_from_a_typed_config() -> Result<()> {
    configure_localstack_env();
    let bucket = unique_bucket("remote-cfg");
    create_localstack_bucket(&bucket).await?;
    let uri = format!("s3://{bucket}/graph");
    let open = || {
        Uni::open(&uri)
            .cloud_storage(localstack_cloud_config(&bucket))
            .build()
    };
    let db = open().await?;
    seed(&db, &["Alice", "Bob"]).await?;
    let fork = db.session().fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'Forked'})").await?;
    tx.commit().await?;
    fork.flush().await?;
    drop(fork);
    drop(db);

    let db = open().await?;
    assert_eq!(names(&db).await?, ["Alice", "Bob"]);
    let fork = db.session().fork("f").await?;
    let rows = fork.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(rows.rows()[0].get::<i64>("c")?, 3);
    Ok(())
}

/// #349: two stores under different prefixes of one bucket, remote-only and
/// hybrid, keep every object under their own prefix and their own schema.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_prefixes_in_one_bucket_are_independent() -> Result<()> {
    configure_localstack_env();
    let bucket = unique_bucket("prefixes");
    create_localstack_bucket(&bucket).await?;
    let cfg = localstack_cloud_config(&bucket);

    let a = Uni::open(format!("s3://{bucket}/a"))
        .cloud_storage(cfg.clone())
        .build()
        .await?;
    a.schema()
        .label("OnlyInA")
        .property("n", DataType::Int64)
        .apply()
        .await?;
    let tx = a.session().tx().await?;
    tx.execute("CREATE (:OnlyInA {n: 1})").await?;
    tx.commit().await?;
    a.flush().await?;

    let local = tempdir()?;
    let b = Uni::open(local.path().to_string_lossy().to_string())
        .hybrid(&format!("s3://{bucket}/b"))
        .cloud_storage(cfg.clone())
        .build()
        .await?;
    let labels: Vec<String> = b.schema().current().labels.keys().cloned().collect();
    assert!(labels.is_empty(), "b inherited {labels:?}");
    seed(&b, &["Bob"]).await?;
    drop((a, b));

    let keys = bucket_keys(&bucket).await?;
    let stray: Vec<&String> = keys
        .iter()
        .filter(|k| !k.starts_with("a/") && !k.starts_with("b/"))
        .collect();
    assert!(stray.is_empty(), "objects outside the prefixes: {stray:?}");
    // The standard layout, under each prefix.
    for k in [
        "a/catalog/schema.json",
        "a/catalog/layout.json",
        "a/id_allocator.json",
        "a/storage/catalog/latest",
        "b/catalog/schema.json",
        "b/storage/catalog/latest",
    ] {
        assert!(keys.iter().any(|x| x == k), "missing {k} in {keys:?}");
    }
    // Hybrid keeps its WAL and allocator local.
    assert!(!keys.iter().any(|k| k.starts_with("b/wal/")), "{keys:?}");
    assert!(local.path().join("id_allocator.json").exists());

    let a = Uni::open(format!("s3://{bucket}/a"))
        .cloud_storage(cfg)
        .build()
        .await?;
    let labels: Vec<String> = a.schema().current().labels.keys().cloned().collect();
    assert_eq!(labels, ["OnlyInA"]);
    Ok(())
}

/// Losing a hybrid store's local directory does not reuse VIDs: the
/// LocalStack reproduction overwrote an existing vertex.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_hybrid_reopen_with_a_fresh_local_dir_reuses_no_vid() -> Result<()> {
    configure_localstack_env();
    let bucket = unique_bucket("hostloss");
    create_localstack_bucket(&bucket).await?;
    let remote = format!("s3://{bucket}/g");
    let open = |dir: &std::path::Path| {
        Uni::open(dir.to_string_lossy().to_string())
            .hybrid(&remote)
            .cloud_storage(localstack_cloud_config(&bucket))
            .build()
    };
    let first = tempdir()?;
    let db = open(first.path()).await?;
    seed(&db, &["Alice", "Bob"]).await?;
    db.shutdown().await?;

    let second = tempdir()?;
    let db = open(second.path()).await?;
    assert_eq!(names(&db).await?, ["Alice", "Bob"]);
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(names(&db).await?, ["Alice", "Bob", "Carol"]);
    Ok(())
}

/// A bucket-root store in the flat shape ≤ 5.3.0 wrote keeps working in place,
/// and a prefixed store whose metadata ≤ 5.3.0 put at the bucket root is
/// refused rather than opened empty.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_flat_and_legacy_prefixed_stores_on_s3() -> Result<()> {
    configure_localstack_env();
    let bucket = unique_bucket("legacy");
    create_localstack_bucket(&bucket).await?;
    let cfg = localstack_cloud_config(&bucket);
    let store = build_localstack_store(&bucket)?;

    // Build a standard store at the bucket root, then move it into the flat
    // shape: tables and snapshot catalog directly under the root.
    let root = format!("s3://{bucket}");
    let db = Uni::open(&root).cloud_storage(cfg.clone()).build().await?;
    seed(&db, &["Alice"]).await?;
    db.shutdown().await?;
    for key in bucket_keys(&bucket).await? {
        let target = if let Some(rest) = key.strip_prefix("storage/") {
            rest.to_string()
        } else if key == "catalog/layout.json" {
            store.delete(&ObjPath::from(key.as_str())).await?;
            continue;
        } else {
            continue;
        };
        store
            .copy(
                &ObjPath::from(key.as_str()),
                &ObjPath::from(target.as_str()),
            )
            .await?;
        store.delete(&ObjPath::from(key.as_str())).await?;
    }
    let db = Uni::open(&root).cloud_storage(cfg.clone()).build().await?;
    assert_eq!(names(&db).await?, ["Alice"]);
    let fork = db.session().fork("flatfork").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'InFork'})").await?;
    tx.commit().await?;
    fork.flush().await?;
    drop(fork);
    db.shutdown().await?;
    let layout = store
        .get(&ObjPath::from("catalog/layout.json"))
        .await?
        .bytes()
        .await?;
    assert!(String::from_utf8_lossy(&layout).contains("flat"));
    assert!(
        !bucket_keys(&bucket)
            .await?
            .iter()
            .any(|k| k.starts_with("storage/")),
        "flat store gained a storage/ directory"
    );

    // ≤ 5.3.0 under a prefix: tables at the prefix, metadata at the root.
    store
        .put(
            &ObjPath::from("p/vertices.lance/_versions/1.manifest"),
            b"x".to_vec().into(),
        )
        .await?;
    let err = match Uni::open(format!("s3://{bucket}/p"))
        .cloud_storage(cfg)
        .build()
        .await
    {
        Ok(_) => anyhow::bail!("legacy prefixed store opened"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("catalog/latest"), "{err}");
    Ok(())
}

/// BACKUP of an S3 store into the same bucket produces a store that opens.
/// In the default run the environment also supplies the endpoint and keys;
/// under `UNI_LOCALSTACK_CONFIG_ONLY=1` the backup and COPY targets can only
/// be reached with the open database's own endpoint and credentials, so that
/// run is what proves they are inherited.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_backup_of_an_s3_store_opens_as_a_store() -> Result<()> {
    configure_localstack_env();
    let bucket = unique_bucket("backup");
    create_localstack_bucket(&bucket).await?;
    let cfg = localstack_cloud_config(&bucket);
    let db = Uni::open(format!("s3://{bucket}/live"))
        .cloud_storage(cfg.clone())
        .build()
        .await?;
    seed(&db, &["Alice", "Bob"]).await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    let rows = db
        .session()
        .run(&format!("BACKUP TO 's3://{bucket}/backups/b1'"))
        .await?;
    assert_eq!(
        rows.rows()[0].value("status"),
        Some(&uni_db::Value::String("Backup completed".into()))
    );
    drop(db);

    let keys = bucket_keys(&bucket).await?;
    assert!(
        keys.iter()
            .any(|k| k.starts_with("backups/b1/storage/") && k.contains(".lance/")),
        "no tables in the backup: {keys:?}"
    );
    let stray: Vec<&String> = keys
        .iter()
        .filter(|k| !k.starts_with("live/") && !k.starts_with("backups/b1/"))
        .collect();
    assert!(stray.is_empty(), "{stray:?}");
    let restored = Uni::open(format!("s3://{bucket}/backups/b1"))
        .cloud_storage(cfg)
        .build()
        .await?;
    assert_eq!(names(&restored).await?, ["Alice", "Bob", "Carol"]);
    let tx = restored.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Dave'})").await?;
    tx.commit().await?;
    restored.flush().await?;
    assert_eq!(names(&restored).await?, ["Alice", "Bob", "Carol", "Dave"]);

    // COPY to the store's own bucket inherits its settings too.
    restored
        .session()
        .run(&format!(
            "COPY Person TO 's3://{bucket}/exports/people.parquet' WITH {{format: 'parquet'}}"
        ))
        .await?;
    assert!(
        bucket_keys(&bucket)
            .await?
            .iter()
            .any(|k| k == "exports/people.parquet"),
        "COPY TO did not write the export"
    );
    Ok(())
}

/// A store configured only by a named AWS profile: endpoint, region and
/// credentials all come from `AWS_PROFILE`, for uni's client and Lance's.
///
/// Mutates the process environment; nextest runs each test in its own
/// process, which keeps that from leaking into other tests.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_remote_only_open_from_a_named_aws_profile() -> Result<()> {
    let dir = tempdir()?;
    let config = dir.path().join("aws-config");
    std::fs::write(
        &config,
        "[profile uni-localstack]\n\
         region = us-east-1\n\
         endpoint_url = http://localhost:4566\n\
         aws_access_key_id = test\n\
         aws_secret_access_key = test\n",
    )?;
    // SAFETY: this test's own process; set before any client is built.
    unsafe {
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
        ] {
            std::env::remove_var(name);
        }
        std::env::set_var("AWS_PROFILE", "uni-localstack");
        std::env::set_var("AWS_CONFIG_FILE", &config);
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", dir.path().join("none"));
        std::env::set_var("AWS_ALLOW_HTTP", "true");
    }
    let bucket = unique_bucket("profile");
    create_localstack_bucket(&bucket).await?;
    let uri = format!("s3://{bucket}/graph");
    let db = Uni::open(&uri).build().await?;
    seed(&db, &["Alice", "Bob"]).await?;
    drop(db);
    let db = Uni::open(&uri).build().await?;
    assert_eq!(names(&db).await?, ["Alice", "Bob"]);
    let keys = bucket_keys(&bucket).await?;
    assert!(
        keys.iter()
            .any(|k| k.starts_with("graph/storage/") && k.contains(".lance/")),
        "{keys:?}"
    );
    Ok(())
}

fn localstack_cloud_config(bucket: &str) -> CloudStorageConfig {
    CloudStorageConfig::S3 {
        bucket: bucket.to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:4566".to_string()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
        session_token: None,
        virtual_hosted_style: false,
    }
}

fn configure_localstack_env() {
    // `UNI_LOCALSTACK_CONFIG_ONLY=1`: leave the endpoint and credentials to
    // the cloud configuration alone, as a deployment that passes them
    // explicitly does. Only the plain-HTTP allowance, which the
    // configuration cannot express, comes from the environment.
    if std::env::var_os("UNI_LOCALSTACK_CONFIG_ONLY").is_some() {
        // SAFETY: as below.
        unsafe {
            for name in [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
                "AWS_ENDPOINT_URL",
                "AWS_ENDPOINT_URL_S3",
                "AWS_PROFILE",
            ] {
                std::env::remove_var(name);
            }
            std::env::set_var("AWS_ALLOW_HTTP", "true");
        }
        return;
    }
    // SAFETY: Integration test mutates process env before creating stores.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
        std::env::set_var("AWS_ALLOW_HTTP", "true"); // Required for Lance to allow HTTP to LocalStack
    }
}

fn build_localstack_store(bucket: &str) -> Result<AmazonS3> {
    Ok(AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_endpoint("http://localhost:4566")
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?)
}

type AmazonS3 = object_store::aws::AmazonS3;

async fn create_localstack_bucket(bucket: &str) -> Result<()> {
    // LocalStack accepts unsigned PUT /{bucket} for bucket creation in test env.
    let status = std::process::Command::new("curl")
        .args([
            "-sSf",
            "-X",
            "PUT",
            &format!("http://localhost:4566/{bucket}"),
        ])
        .status()?;
    if !status.success() {
        anyhow::bail!("failed to create localstack bucket: {bucket}");
    }

    let store = build_localstack_store(bucket)?;
    store
        .put(&ObjPath::from(".marker"), Vec::<u8>::new().into())
        .await?;
    Ok(())
}
