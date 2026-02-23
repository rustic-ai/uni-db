// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use futures::TryStreamExt;
use object_store::ObjectStore;
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
        .hybrid(&local_meta, &remote_url)
        .cloud_config(cloud_cfg.clone())
        .build()
        .await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.execute("CREATE (:Person {name: 'Bob'})").await?;
    db.execute(
        "
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS {since: 2024}]->(b)
    ",
    )
    .await?;
    db.flush().await?;
    drop(db);

    // Reopen and verify graph state survives in hybrid mode.
    let db = Uni::open(local_meta.to_string_lossy().to_string())
        .hybrid(&local_meta, &remote_url)
        .cloud_config(cloud_cfg.clone())
        .build()
        .await?;

    let res = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(res.rows()[0].get::<i64>("c")?, 2);

    let rel = db
        .query("MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.name, b.name, k.since")
        .await?;
    assert_eq!(rel.len(), 1);
    assert_eq!(rel.rows()[0].get::<String>("a.name")?, "Alice");
    assert_eq!(rel.rows()[0].get::<String>("b.name")?, "Bob");
    assert_eq!(rel.rows()[0].get::<i64>("k.since")?, 2024);

    // Mutate after reopen, flush, and verify again after another reopen.
    db.execute("CREATE (:Person {name: 'Carol'})").await?;
    db.flush().await?;
    drop(db);

    let db = Uni::open(local_meta.to_string_lossy().to_string())
        .hybrid(&local_meta, &remote_url)
        .cloud_config(cloud_cfg.clone())
        .build()
        .await?;

    let res = db.query("MATCH (p:Person) RETURN count(p) AS c").await?;
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
    // SAFETY: Integration test mutates process env before creating stores.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
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
