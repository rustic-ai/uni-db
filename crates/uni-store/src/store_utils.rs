// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Utilities for object store operations with timeout support.
//!
//! These wrappers prevent operations from hanging indefinitely when the
//! underlying storage becomes unresponsive.

use anyhow::{Result, anyhow};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{GetResult, ObjectMeta, ObjectStore, PutOptions, PutResult};
use std::sync::Arc;
use std::time::Duration;

/// Default timeout for object store operations (30 seconds).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default number of retries for transient object store failures.
pub const DEFAULT_RETRIES: usize = 3;

/// Gets an object from the store with a timeout and retries.
pub async fn get_with_timeout(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    timeout: Duration,
) -> Result<GetResult> {
    let mut last_err = anyhow!("Unknown error");
    for i in 0..=DEFAULT_RETRIES {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(100 * i as u64)).await;
        }
        match tokio::time::timeout(timeout, store.get(path)).await {
            Ok(Ok(res)) => return Ok(res),
            Ok(Err(e)) => last_err = anyhow!(e),
            Err(_) => {
                last_err = anyhow!(
                    "Object store get timed out after {:?} for path: {}",
                    timeout,
                    path
                )
            }
        }
    }
    Err(last_err)
}

/// Puts an object to the store with a timeout and retries.
pub async fn put_with_timeout(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    bytes: Bytes,
    timeout: Duration,
) -> Result<PutResult> {
    let mut last_err = anyhow!("Unknown error");
    for i in 0..=DEFAULT_RETRIES {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(100 * i as u64)).await;
        }
        match tokio::time::timeout(timeout, store.put(path, bytes.clone().into())).await {
            Ok(Ok(res)) => return Ok(res),
            Ok(Err(e)) => last_err = anyhow!(e),
            Err(_) => {
                last_err = anyhow!(
                    "Object store put timed out after {:?} for path: {}",
                    timeout,
                    path
                )
            }
        }
    }
    Err(last_err)
}

/// Puts an object to the store with options and a timeout.
///
/// # Errors
///
/// Returns an error if the operation times out or the underlying put fails.
pub async fn put_opts_with_timeout(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    bytes: Bytes,
    opts: PutOptions,
    timeout: Duration,
) -> Result<PutResult> {
    tokio::time::timeout(timeout, store.put_opts(path, bytes.into(), opts))
        .await
        .map_err(|_| {
            anyhow!(
                "Object store put_opts timed out after {:?} for path: {}",
                timeout,
                path
            )
        })?
        .map_err(Into::into)
}

/// Deletes an object from the store with a timeout.
///
/// # Errors
///
/// Returns an error if the operation times out or the underlying delete fails.
pub async fn delete_with_timeout(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, store.delete(path))
        .await
        .map_err(|_| {
            anyhow!(
                "Object store delete timed out after {:?} for path: {}",
                timeout,
                path
            )
        })?
        .map_err(Into::into)
}

/// Lists objects in the store, collecting results with a per-item timeout.
///
/// This function collects the stream into a Vec. For large listings, consider
/// using the streaming approach directly with appropriate timeouts.
///
/// # Errors
///
/// Returns an error if any list operation times out or fails.
pub async fn list_with_timeout(
    store: &Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
    timeout: Duration,
) -> Result<Vec<ObjectMeta>> {
    let stream: BoxStream<'_, object_store::Result<ObjectMeta>> = store.list(prefix);
    let mut stream = Box::pin(stream);
    let mut results = Vec::new();

    // Set a deadline for the entire listing operation
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(anyhow!(
                "Object store list timed out after {:?} for prefix: {:?}",
                timeout,
                prefix
            ));
        }

        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(meta))) => results.push(meta),
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(None) => break, // Stream exhausted
            Err(_) => {
                return Err(anyhow!(
                    "Object store list timed out after {:?} for prefix: {:?}",
                    timeout,
                    prefix
                ));
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_put_get_with_timeout() -> Result<()> {
        let dir = tempdir()?;
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = Path::from("test.txt");
        let content = Bytes::from("hello world");

        put_with_timeout(&store, &path, content.clone(), DEFAULT_TIMEOUT).await?;

        let result = get_with_timeout(&store, &path, DEFAULT_TIMEOUT).await?;
        let bytes = result.bytes().await?;
        assert_eq!(bytes, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_with_timeout() -> Result<()> {
        let dir = tempdir()?;
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = Path::from("to_delete.txt");

        put_with_timeout(&store, &path, Bytes::from("data"), DEFAULT_TIMEOUT).await?;
        delete_with_timeout(&store, &path, DEFAULT_TIMEOUT).await?;

        // Verify deleted
        let result = get_with_timeout(&store, &path, DEFAULT_TIMEOUT).await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_list_with_timeout() -> Result<()> {
        let dir = tempdir()?;
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);

        // Create some files
        put_with_timeout(
            &store,
            &Path::from("a.txt"),
            Bytes::from("a"),
            DEFAULT_TIMEOUT,
        )
        .await?;
        put_with_timeout(
            &store,
            &Path::from("b.txt"),
            Bytes::from("b"),
            DEFAULT_TIMEOUT,
        )
        .await?;

        let results = list_with_timeout(&store, None, DEFAULT_TIMEOUT).await?;
        assert_eq!(results.len(), 2);

        Ok(())
    }
}
