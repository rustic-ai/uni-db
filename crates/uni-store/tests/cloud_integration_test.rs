// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cloud Storage Integration Tests
//!
//! Tests for the cloud storage module supporting S3, GCS, and Azure.
//! Uses InMemory store for fast local testing and LocalStack for S3 integration.
//!
//! To run LocalStack tests:
//! ```bash
//! docker run -d --name localstack -p 4566:4566 localstack/localstack
//! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//!     cargo test --test cloud_integration_test -- --ignored
//! ```

use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use tempfile::tempdir;

use uni_common::CloudStorageConfig;
use uni_store::cloud::{build_cloud_store, build_store_from_url, copy_store_prefix, is_cloud_url};

#[test]
fn test_is_cloud_url_detection() {
    // Cloud URLs
    assert!(is_cloud_url("s3://bucket/path"));
    assert!(is_cloud_url("s3://my-bucket/prefix/data"));
    assert!(is_cloud_url("gs://bucket/path"));
    assert!(is_cloud_url("az://account/container"));
    assert!(is_cloud_url("azure://account/container/path"));

    // Local paths
    assert!(!is_cloud_url("/local/path"));
    assert!(!is_cloud_url("./relative/path"));
    assert!(!is_cloud_url("file:///local/path"));
    assert!(!is_cloud_url("C:\\Windows\\Path"));
}

#[test]
fn test_cloud_storage_config_s3() {
    let config = CloudStorageConfig::S3 {
        bucket: "test-bucket".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:4566".to_string()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
        session_token: None,
        virtual_hosted_style: false,
    };

    assert_eq!(config.bucket_name(), "test-bucket");
    assert_eq!(config.to_url(), "s3://test-bucket");
}

#[test]
fn test_cloud_storage_config_gcs() {
    let config = CloudStorageConfig::Gcs {
        bucket: "my-gcs-bucket".to_string(),
        service_account_path: Some("/path/to/key.json".to_string()),
        service_account_key: None,
    };

    assert_eq!(config.bucket_name(), "my-gcs-bucket");
    assert_eq!(config.to_url(), "gs://my-gcs-bucket");
}

#[test]
fn test_cloud_storage_config_azure() {
    let config = CloudStorageConfig::Azure {
        container: "mycontainer".to_string(),
        account: "myaccount".to_string(),
        access_key: Some("access-key".to_string()),
        sas_token: None,
    };

    assert_eq!(config.bucket_name(), "mycontainer");
    assert_eq!(config.to_url(), "az://myaccount/mycontainer");
}

#[tokio::test]
async fn test_build_store_from_local_path() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_str().unwrap();

    let (store, prefix) = build_store_from_url(path)?;
    assert!(prefix.as_ref().is_empty());

    // Test basic put/get operations
    let test_path = object_store::path::Path::from("test.txt");
    store
        .put(&test_path, Bytes::from("hello world").into())
        .await?;

    let result = store.get(&test_path).await?.bytes().await?;
    assert_eq!(result.as_ref(), b"hello world");

    Ok(())
}

#[tokio::test]
async fn test_build_store_from_file_url() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_str().unwrap();
    let file_url = format!("file://{}", path);

    let (store, prefix) = build_store_from_url(&file_url)?;
    assert!(prefix.as_ref().is_empty());

    // Verify the store works
    let test_path = object_store::path::Path::from("test.txt");
    store
        .put(&test_path, Bytes::from("file url test").into())
        .await?;

    let result = store.get(&test_path).await?.bytes().await?;
    assert_eq!(result.as_ref(), b"file url test");

    Ok(())
}

#[tokio::test]
async fn test_copy_store_prefix() -> Result<()> {
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    let src_dir = tempdir()?;
    let dst_dir = tempdir()?;

    // Create source store and add some files
    let src_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(src_dir.path())?);

    src_store
        .put(
            &Path::from("data/file1.txt"),
            Bytes::from("content1").into(),
        )
        .await?;
    src_store
        .put(
            &Path::from("data/file2.txt"),
            Bytes::from("content2").into(),
        )
        .await?;
    src_store
        .put(
            &Path::from("data/subdir/file3.txt"),
            Bytes::from("content3").into(),
        )
        .await?;

    // Create destination store
    let dst_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dst_dir.path())?);

    // Copy data/ prefix
    let copied = copy_store_prefix(
        &src_store,
        &dst_store,
        &Path::from("data"),
        &Path::from("backup/data"),
    )
    .await?;

    assert_eq!(copied, 3);

    // Verify files were copied
    let result = dst_store
        .get(&Path::from("backup/data/file1.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"content1");

    let result = dst_store
        .get(&Path::from("backup/data/subdir/file3.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"content3");

    Ok(())
}

// =============================================================================
// InMemory Object Store Tests (No External Dependencies)
// =============================================================================
// These tests use the InMemory object store to verify cloud functionality
// without requiring LocalStack, MinIO, or any external services.

#[tokio::test]
async fn test_inmemory_store_basic_operations() -> Result<()> {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Test put
    let path = Path::from("test/hello.txt");
    store
        .put(&path, Bytes::from("Hello InMemory!").into())
        .await?;

    // Test get
    let result = store.get(&path).await?.bytes().await?;
    assert_eq!(result.as_ref(), b"Hello InMemory!");

    // Test list
    let list: Vec<_> = store
        .list(Some(&Path::from("test/")))
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].location, path);

    // Test delete
    store.delete(&path).await?;
    assert!(store.get(&path).await.is_err());

    Ok(())
}

use futures::StreamExt;

#[tokio::test]
async fn test_copy_store_prefix_inmemory() -> Result<()> {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    // Create source and destination InMemory stores
    let src_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dst_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Populate source with test data
    src_store
        .put(
            &Path::from("data/file1.txt"),
            Bytes::from("content1").into(),
        )
        .await?;
    src_store
        .put(
            &Path::from("data/file2.txt"),
            Bytes::from("content2").into(),
        )
        .await?;
    src_store
        .put(
            &Path::from("data/nested/file3.txt"),
            Bytes::from("content3").into(),
        )
        .await?;
    src_store
        .put(
            &Path::from("other/ignored.txt"),
            Bytes::from("should not be copied").into(),
        )
        .await?;

    // Copy only the "data" prefix
    let copied = copy_store_prefix(
        &src_store,
        &dst_store,
        &Path::from("data"),
        &Path::from("backup"),
    )
    .await?;

    assert_eq!(copied, 3, "Should copy exactly 3 files from data/ prefix");

    // Verify files were copied with correct content
    let result = dst_store
        .get(&Path::from("backup/file1.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"content1");

    let result = dst_store
        .get(&Path::from("backup/file2.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"content2");

    let result = dst_store
        .get(&Path::from("backup/nested/file3.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"content3");

    // Verify "other" prefix was NOT copied
    assert!(
        dst_store
            .get(&Path::from("backup/ignored.txt"))
            .await
            .is_err(),
        "Files outside the source prefix should not be copied"
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_store_prefix_empty_source() -> Result<()> {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    let src_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dst_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Copy from empty prefix
    let copied = copy_store_prefix(
        &src_store,
        &dst_store,
        &Path::from("nonexistent"),
        &Path::from("backup"),
    )
    .await?;

    assert_eq!(copied, 0, "Copying empty prefix should return 0");

    Ok(())
}

#[tokio::test]
async fn test_copy_store_prefix_to_root() -> Result<()> {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    let src_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dst_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    src_store
        .put(&Path::from("data/test.txt"), Bytes::from("test").into())
        .await?;

    // Copy to root (empty prefix)
    let copied =
        copy_store_prefix(&src_store, &dst_store, &Path::from("data"), &Path::from("")).await?;

    assert_eq!(copied, 1);

    // File should be at root
    let result = dst_store
        .get(&Path::from("test.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"test");

    Ok(())
}

#[tokio::test]
async fn test_copy_store_prefix_large_files() -> Result<()> {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    let src_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dst_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Create a larger file (1MB)
    let large_content = vec![0u8; 1024 * 1024];
    src_store
        .put(
            &Path::from("data/large.bin"),
            Bytes::from(large_content.clone()).into(),
        )
        .await?;

    let copied = copy_store_prefix(
        &src_store,
        &dst_store,
        &Path::from("data"),
        &Path::from("backup"),
    )
    .await?;

    assert_eq!(copied, 1);

    let result = dst_store
        .get(&Path::from("backup/large.bin"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.len(), 1024 * 1024);
    assert_eq!(result.as_ref(), large_content.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_inmemory_simulated_backup_flow() -> Result<()> {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    // Simulate a database with catalog and storage directories
    let db_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let backup_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Create database structure
    db_store
        .put(
            &Path::from("catalog/schema.json"),
            Bytes::from(r#"{"version": 1}"#).into(),
        )
        .await?;
    db_store
        .put(
            &Path::from("catalog/snapshots/snap1.json"),
            Bytes::from(r#"{"id": "snap1"}"#).into(),
        )
        .await?;
    db_store
        .put(
            &Path::from("storage/vertices/Person.lance/data.lance"),
            Bytes::from("vertex data").into(),
        )
        .await?;
    db_store
        .put(
            &Path::from("storage/edges/KNOWS.lance/data.lance"),
            Bytes::from("edge data").into(),
        )
        .await?;

    // Backup catalog
    let catalog_copied = copy_store_prefix(
        &db_store,
        &backup_store,
        &Path::from("catalog"),
        &Path::from("backup-2024/catalog"),
    )
    .await?;
    assert_eq!(catalog_copied, 2);

    // Backup storage
    let storage_copied = copy_store_prefix(
        &db_store,
        &backup_store,
        &Path::from("storage"),
        &Path::from("backup-2024/storage"),
    )
    .await?;
    assert_eq!(storage_copied, 2);

    // Verify backup integrity
    let schema = backup_store
        .get(&Path::from("backup-2024/catalog/schema.json"))
        .await?
        .bytes()
        .await?;
    assert_eq!(schema.as_ref(), br#"{"version": 1}"#);

    let vertex_data = backup_store
        .get(&Path::from(
            "backup-2024/storage/vertices/Person.lance/data.lance",
        ))
        .await?
        .bytes()
        .await?;
    assert_eq!(vertex_data.as_ref(), b"vertex data");

    Ok(())
}

#[tokio::test]
async fn test_inmemory_cross_store_copy() -> Result<()> {
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    // Test copying from local filesystem to InMemory (simulates local->cloud backup)
    let local_dir = tempdir()?;
    let local_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(local_dir.path())?);
    let cloud_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Create local files
    local_store
        .put(
            &Path::from("data/local.txt"),
            Bytes::from("local file").into(),
        )
        .await?;

    // Copy local -> "cloud" (InMemory)
    let copied = copy_store_prefix(
        &local_store,
        &cloud_store,
        &Path::from("data"),
        &Path::from("cloud-backup"),
    )
    .await?;

    assert_eq!(copied, 1);

    let result = cloud_store
        .get(&Path::from("cloud-backup/local.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"local file");

    // Now test copying from InMemory back to local (simulates cloud->local restore)
    let restore_dir = tempdir()?;
    let restore_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(restore_dir.path())?);

    let restored = copy_store_prefix(
        &cloud_store,
        &restore_store,
        &Path::from("cloud-backup"),
        &Path::from("restored"),
    )
    .await?;

    assert_eq!(restored, 1);

    let result = restore_store
        .get(&Path::from("restored/local.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"local file");

    Ok(())
}

// =============================================================================
// S3/LocalStack Integration Tests - Run with --ignored
// =============================================================================
// Requires: LocalStack running on localhost:4566

#[tokio::test]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_s3_basic_operations() -> Result<()> {
    let config = CloudStorageConfig::S3 {
        bucket: "test-bucket".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:4566".to_string()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
        session_token: None,
        virtual_hosted_style: false,
    };

    // Create bucket first (LocalStack specific)
    create_localstack_bucket("test-bucket").await?;

    let store = build_cloud_store(&config)?;

    // Test put
    let path = object_store::path::Path::from("test/hello.txt");
    store
        .put(&path, Bytes::from("Hello from S3!").into())
        .await?;

    // Test get
    let result = store.get(&path).await?.bytes().await?;
    assert_eq!(result.as_ref(), b"Hello from S3!");

    // Test delete
    store.delete(&path).await?;

    // Verify deletion
    assert!(store.get(&path).await.is_err());

    Ok(())
}

#[tokio::test]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_s3_url_parsing() -> Result<()> {
    // Ensure bucket exists
    create_localstack_bucket("url-test-bucket").await?;

    // Set environment variables for URL-based store creation
    // SAFETY: This test runs in isolation; env var modification is safe here.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
    }

    let (store, prefix) = build_store_from_url("s3://url-test-bucket/data")?;

    assert_eq!(prefix.as_ref(), "data");

    // Test operations
    let path = object_store::path::Path::from("file.txt");
    store
        .put(&path, Bytes::from("URL test content").into())
        .await?;

    let result = store.get(&path).await?.bytes().await?;
    assert_eq!(result.as_ref(), b"URL test content");

    Ok(())
}

#[tokio::test]
#[ignore = "Requires LocalStack running on localhost:4566"]
async fn test_s3_copy_prefix() -> Result<()> {
    use object_store::path::Path;

    // Ensure buckets exist
    create_localstack_bucket("src-bucket").await?;
    create_localstack_bucket("dst-bucket").await?;

    let src_config = CloudStorageConfig::S3 {
        bucket: "src-bucket".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:4566".to_string()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
        session_token: None,
        virtual_hosted_style: false,
    };

    let dst_config = CloudStorageConfig::S3 {
        bucket: "dst-bucket".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:4566".to_string()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
        session_token: None,
        virtual_hosted_style: false,
    };

    let src_store = build_cloud_store(&src_config)?;
    let dst_store = build_cloud_store(&dst_config)?;

    // Create test files in source
    src_store
        .put(&Path::from("data/a.txt"), Bytes::from("file a").into())
        .await?;
    src_store
        .put(&Path::from("data/b.txt"), Bytes::from("file b").into())
        .await?;

    // Copy prefix
    let copied = copy_store_prefix(
        &src_store,
        &dst_store,
        &Path::from("data"),
        &Path::from("backup"),
    )
    .await?;

    assert_eq!(copied, 2);

    // Verify
    let result = dst_store
        .get(&Path::from("backup/a.txt"))
        .await?
        .bytes()
        .await?;
    assert_eq!(result.as_ref(), b"file a");

    Ok(())
}

/// Helper to create a bucket in LocalStack
async fn create_localstack_bucket(bucket: &str) -> Result<()> {
    use object_store::ObjectStore;
    use object_store::aws::AmazonS3Builder;

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

    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_endpoint("http://localhost:4566")
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;

    // Put marker to verify the bucket is usable.
    store
        .put(
            &object_store::path::Path::from(".marker"),
            Bytes::from("").into(),
        )
        .await?;

    Ok(())
}
