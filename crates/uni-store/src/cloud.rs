// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cloud storage builders for S3, GCS, and Azure.
//!
//! This module provides utilities to construct `ObjectStore` instances
//! for various cloud providers. It supports both explicit configuration
//! and URL-based store creation.
//!
//! # Examples
//!
//! ```ignore
//! use uni_common::CloudStorageConfig;
//! use uni_store::cloud::build_cloud_store;
//!
//! let config = CloudStorageConfig::s3_from_env("my-bucket");
//! let store = build_cloud_store(&config)?;
//! ```

use std::sync::Arc;

use anyhow::{Result, anyhow};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use uni_common::CloudStorageConfig;

/// Builds an `ObjectStore` from a `CloudStorageConfig`.
///
/// Creates the appropriate cloud storage client based on the configuration
/// variant, applying all specified credentials and settings.
///
/// # Errors
///
/// Returns an error if the cloud store cannot be built due to invalid
/// credentials, missing required parameters, or network issues.
///
/// # Examples
///
/// ```ignore
/// let config = CloudStorageConfig::S3 {
///     bucket: "my-bucket".to_string(),
///     region: Some("us-east-1".to_string()),
///     endpoint: None,
///     access_key_id: None,
///     secret_access_key: None,
///     session_token: None,
///     virtual_hosted_style: false,
/// };
/// let store = build_cloud_store(&config)?;
/// ```
pub fn build_cloud_store(config: &CloudStorageConfig) -> Result<Arc<dyn ObjectStore>> {
    match config {
        CloudStorageConfig::S3 {
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            virtual_hosted_style,
        } => {
            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

            if let Some(r) = region {
                builder = builder.with_region(r);
            }

            if let Some(e) = endpoint {
                builder = builder.with_endpoint(e);
            }

            if let Some(key) = access_key_id {
                builder = builder.with_access_key_id(key);
            }

            if let Some(secret) = secret_access_key {
                builder = builder.with_secret_access_key(secret);
            }

            if let Some(token) = session_token {
                builder = builder.with_token(token);
            }

            if *virtual_hosted_style {
                builder = builder.with_virtual_hosted_style_request(true);
            } else {
                // Path-style is required for LocalStack and MinIO
                builder = builder.with_virtual_hosted_style_request(false);
            }

            // Allow HTTP for local development (LocalStack, MinIO)
            if endpoint.as_ref().is_some_and(|e| e.starts_with("http://")) {
                builder = builder.with_allow_http(true);
            }

            let store = builder.build()?;
            Ok(Arc::new(store))
        }
        CloudStorageConfig::Gcs {
            bucket,
            service_account_path,
            service_account_key,
        } => {
            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

            if let Some(path) = service_account_path {
                builder = builder.with_service_account_path(path);
            }

            if let Some(key) = service_account_key {
                builder = builder.with_service_account_key(key);
            }

            let store = builder.build()?;
            Ok(Arc::new(store))
        }
        CloudStorageConfig::Azure {
            container,
            account,
            access_key,
            sas_token,
        } => {
            let mut builder = MicrosoftAzureBuilder::new()
                .with_container_name(container)
                .with_account(account);

            if let Some(key) = access_key {
                builder = builder.with_access_key(key);
            }

            if let Some(token) = sas_token {
                // Parse SAS token query string into key-value pairs
                // SAS tokens look like: sv=2019-02-02&ss=b&srt=sco&...
                let query_pairs: Vec<(String, String)> = token
                    .trim_start_matches('?')
                    .split('&')
                    .filter_map(|pair| {
                        let mut parts = pair.splitn(2, '=');
                        let key = parts.next()?;
                        let value = parts.next().unwrap_or("");
                        Some((key.to_string(), value.to_string()))
                    })
                    .collect();
                builder = builder.with_sas_authorization(query_pairs);
            }

            let store = builder.build()?;
            Ok(Arc::new(store))
        }
    }
}

/// Parses a URL and returns the appropriate `ObjectStore` and path prefix.
///
/// Supports the following URL schemes:
/// - `s3://bucket/path` - Amazon S3
/// - `gs://bucket/path` - Google Cloud Storage
/// - `az://account/container/path` - Azure Blob Storage
/// - `file:///path` or `/path` - Local filesystem
///
/// # Errors
///
/// Returns an error if the URL scheme is not recognized or if the store
/// cannot be created.
///
/// # Examples
///
/// ```ignore
/// let (store, prefix) = build_store_from_url("s3://my-bucket/data")?;
/// ```
pub fn build_store_from_url(url: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    // Handle local paths directly
    if !url.contains("://") {
        let local_path = std::path::Path::new(url);
        let store = Arc::new(LocalFileSystem::new_with_prefix(local_path)?);
        return Ok((store, Path::from("")));
    }

    if let Some(path) = url.strip_prefix("file://") {
        let local_path = std::path::Path::new(path);
        let store = Arc::new(LocalFileSystem::new_with_prefix(local_path)?);
        return Ok((store, Path::from("")));
    }

    let parsed = url::Url::parse(url).map_err(|e| anyhow!("Invalid URL '{}': {}", url, e))?;
    let scheme = parsed.scheme();

    match scheme {
        "s3" => {
            let bucket = parsed
                .host_str()
                .ok_or_else(|| anyhow!("S3 URL missing bucket: {}", url))?;
            let prefix = parsed.path().trim_start_matches('/');

            let config = CloudStorageConfig::s3_from_env(bucket);
            let store = build_cloud_store(&config)?;

            Ok((store, Path::from(prefix)))
        }
        "gs" => {
            let bucket = parsed
                .host_str()
                .ok_or_else(|| anyhow!("GCS URL missing bucket: {}", url))?;
            let prefix = parsed.path().trim_start_matches('/');

            let config = CloudStorageConfig::gcs_from_env(bucket);
            let store = build_cloud_store(&config)?;

            Ok((store, Path::from(prefix)))
        }
        "az" | "azure" | "abfs" | "abfss" => {
            // Format: az://account/container/path
            let account = parsed
                .host_str()
                .ok_or_else(|| anyhow!("Azure URL missing account: {}", url))?;
            let path_parts: Vec<&str> = parsed
                .path()
                .trim_start_matches('/')
                .splitn(2, '/')
                .collect();

            if path_parts.is_empty() || path_parts[0].is_empty() {
                return Err(anyhow!("Azure URL missing container: {}", url));
            }

            let container = path_parts[0];
            let prefix = path_parts.get(1).unwrap_or(&"");

            let config = CloudStorageConfig::Azure {
                container: container.to_string(),
                account: account.to_string(),
                access_key: std::env::var("AZURE_STORAGE_ACCESS_KEY").ok(),
                sas_token: std::env::var("AZURE_STORAGE_SAS_TOKEN").ok(),
            };

            let store = build_cloud_store(&config)?;

            Ok((store, Path::from(*prefix)))
        }
        _ => Err(anyhow!(
            "Unsupported URL scheme '{}'. Supported: s3://, gs://, az://",
            scheme
        )),
    }
}

/// Copies all objects from a source prefix to a destination prefix.
///
/// This function streams objects from the source store and uploads them
/// to the destination store, preserving the relative path structure.
///
/// # Errors
///
/// Returns an error if any object cannot be read or written.
pub async fn copy_store_prefix(
    src_store: &Arc<dyn ObjectStore>,
    dst_store: &Arc<dyn ObjectStore>,
    src_prefix: &Path,
    dst_prefix: &Path,
) -> Result<usize> {
    use futures::TryStreamExt;

    let mut copied_count = 0usize;
    let mut stream = src_store.list(Some(src_prefix));

    while let Some(meta) = stream.try_next().await? {
        let bytes = src_store.get(&meta.location).await?.bytes().await?;

        // Calculate relative path from source prefix
        let relative = meta
            .location
            .as_ref()
            .strip_prefix(src_prefix.as_ref())
            .unwrap_or(meta.location.as_ref())
            .trim_start_matches('/');

        let dst_path = if dst_prefix.as_ref().is_empty() {
            Path::from(relative)
        } else {
            Path::from(format!("{}/{}", dst_prefix.as_ref(), relative))
        };

        dst_store.put(&dst_path, bytes.into()).await?;
        copied_count += 1;
    }

    Ok(copied_count)
}

/// Checks if a URL points to a cloud storage location.
#[must_use]
pub fn is_cloud_url(url: &str) -> bool {
    url.starts_with("s3://")
        || url.starts_with("gs://")
        || url.starts_with("az://")
        || url.starts_with("azure://")
        || url.starts_with("abfs://")
        || url.starts_with("abfss://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_cloud_url() {
        assert!(is_cloud_url("s3://bucket/path"));
        assert!(is_cloud_url("gs://bucket/path"));
        assert!(is_cloud_url("az://account/container"));
        assert!(is_cloud_url("azure://account/container"));
        assert!(!is_cloud_url("/local/path"));
        assert!(!is_cloud_url("file:///local/path"));
        assert!(!is_cloud_url("./relative/path"));
    }

    #[test]
    fn test_cloud_storage_config_bucket_name() {
        let s3 = CloudStorageConfig::S3 {
            bucket: "my-s3-bucket".to_string(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_hosted_style: false,
        };
        assert_eq!(s3.bucket_name(), "my-s3-bucket");

        let gcs = CloudStorageConfig::Gcs {
            bucket: "my-gcs-bucket".to_string(),
            service_account_path: None,
            service_account_key: None,
        };
        assert_eq!(gcs.bucket_name(), "my-gcs-bucket");

        let azure = CloudStorageConfig::Azure {
            container: "my-container".to_string(),
            account: "myaccount".to_string(),
            access_key: None,
            sas_token: None,
        };
        assert_eq!(azure.bucket_name(), "my-container");
    }

    #[test]
    fn test_cloud_storage_config_to_url() {
        let s3 = CloudStorageConfig::S3 {
            bucket: "bucket".to_string(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_hosted_style: false,
        };
        assert_eq!(s3.to_url(), "s3://bucket");

        let gcs = CloudStorageConfig::Gcs {
            bucket: "bucket".to_string(),
            service_account_path: None,
            service_account_key: None,
        };
        assert_eq!(gcs.to_url(), "gs://bucket");

        let azure = CloudStorageConfig::Azure {
            container: "container".to_string(),
            account: "account".to_string(),
            access_key: None,
            sas_token: None,
        };
        assert_eq!(azure.to_url(), "az://account/container");
    }

    #[tokio::test]
    async fn test_build_store_from_local_url() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let result = build_store_from_url(path);
        assert!(result.is_ok());

        let (store, prefix) = result.unwrap();
        assert!(prefix.as_ref().is_empty());

        // Verify the store works
        let test_path = Path::from("test.txt");
        store
            .put(&test_path, bytes::Bytes::from("hello").into())
            .await
            .unwrap();

        let data = store.get(&test_path).await.unwrap().bytes().await.unwrap();
        assert_eq!(data.as_ref(), b"hello");
    }
}
