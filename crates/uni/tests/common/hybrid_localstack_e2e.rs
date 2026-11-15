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
        .remote_storage(&remote_url, cloud_cfg.clone())
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
        .remote_storage(&remote_url, cloud_cfg.clone())
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
        .remote_storage(&remote_url, cloud_cfg.clone())
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
            .remote_storage(&remote_url, cloud_cfg.clone())
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
        .remote_storage(&format!("s3://{bucket}"), localstack_cloud_config(&bucket))
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
                "AWS_REGION",
                "AWS_ENDPOINT_URL",
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
