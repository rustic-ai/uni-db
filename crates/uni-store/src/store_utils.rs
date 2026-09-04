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
use object_store::{GetResult, ObjectMeta, ObjectStore, ObjectStoreExt, PutOptions, PutResult};
use std::sync::Arc;
use std::time::Duration;

/// Default timeout for object store operations (30 seconds).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default number of retries for transient object store failures.
pub const DEFAULT_RETRIES: usize = 3;

/// Retries an async operation with exponential backoff and timeout.
///
/// Executes `op` up to `DEFAULT_RETRIES + 1` times, sleeping with linear
/// backoff (100ms * attempt) between retries. Each attempt is wrapped in
/// a timeout. On timeout, the provided `timeout_msg` is used as the error.
///
/// # Errors
///
/// Returns the last error if all attempts fail or time out.
async fn retry_with_timeout<T, F, Fut>(timeout: Duration, timeout_msg: &str, op: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, object_store::Error>>,
{
    let mut last_err = anyhow!("Unknown error");
    for i in 0..=DEFAULT_RETRIES {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(100 * i as u64)).await;
        }
        match tokio::time::timeout(timeout, op()).await {
            Ok(Ok(res)) => return Ok(res),
            Ok(Err(e)) => {
                // Terminal errors describe a state backoff cannot change.
                if is_terminal(&e) {
                    return Err(anyhow::Error::from(e));
                }
                last_err = anyhow!(e);
            }
            Err(_) => last_err = anyhow!("{}", timeout_msg),
        }
    }
    Err(last_err)
}

/// Gets an object from the store with a timeout and retries.
///
/// # Errors
///
/// Returns an error if all retry attempts fail or time out.
pub async fn get_with_timeout(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    timeout: Duration,
) -> Result<GetResult> {
    let msg = format!(
        "Object store get timed out after {:?} for path: {}",
        timeout, path
    );
    retry_with_timeout(timeout, &msg, || store.get(path)).await
}

/// Returns `true` when retrying an object-store operation cannot help.
///
/// A terminal error describes a state the store will keep reporting: the object
/// is absent, already present, fails a precondition, or the caller is not
/// permitted. Retrying spends the whole backoff budget to arrive at the same
/// answer — and in `ResilientObjectStore` it also charges a *system* failure
/// against the circuit breaker for what is an application-level outcome.
///
/// Matching is on the **variant**, never on the rendered message. A store can
/// wrap a transient failure in `Generic` with text that reads "not found" — a
/// proxy returning a 404 HTML body, or an S3-compatible endpoint reporting a
/// missing bucket. Substring matching calls those terminal and abandons a blip
/// that would have healed; `tests/common/bugs/repro_id_allocator_substring_notfound.rs`
/// pins the same defect one layer up.
///
/// `object_store::Error` is `#[non_exhaustive]`, so the wildcard is mandatory —
/// this can never become a compile-time exhaustiveness check. It answers
/// **retryable** deliberately: a bounded backoff before failing is the safe
/// response to an unrecognised error, where declaring it terminal would abandon
/// a recoverable operation immediately.
#[must_use]
pub fn is_terminal(err: &object_store::Error) -> bool {
    use object_store::Error as E;
    matches!(
        err,
        E::NotFound { .. }
            | E::AlreadyExists { .. }
            | E::Precondition { .. }
            | E::NotModified { .. }
            | E::PermissionDenied { .. }
            | E::Unauthenticated { .. }
            | E::NotSupported { .. }
            | E::NotImplemented { .. }
            | E::InvalidPath { .. }
            | E::UnknownConfigurationKey { .. }
    )
}

/// Returns `true` when an object-store read error is a permanent `NotFound`.
///
/// Distinguishes "the object was never created" (empty result is correct) from
/// a transient/IO failure (which must propagate). [`get_with_timeout`] preserves
/// `object_store::Error::NotFound` through `anyhow` (see `retry_with_timeout`),
/// so a downcast that also matches the variant is reliable — transient errors
/// are wrapped generically and never match.
///
/// Deliberately **narrower** than [`is_terminal`], and not implemented in terms
/// of it. `is_terminal` answers "will a retry help?"; this answers "is the
/// object absent?". Widening it to the whole terminal set would make a
/// `PermissionDenied` or `Precondition` read as "absent", so every caller that
/// substitutes a default on `is_not_found` would silently swallow a real
/// failure — the fail-open shape this module exists to avoid.
///
/// # Examples
///
/// ```ignore
/// match get_with_timeout(&store, &path, DEFAULT_TIMEOUT).await {
///     Ok(r) => parse(r),
///     Err(e) if is_not_found(&e) => Default::default(),
///     Err(e) => return Err(e),
/// }
/// ```
#[must_use]
pub fn is_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<object_store::Error>()
        .is_some_and(|e| matches!(e, object_store::Error::NotFound { .. }))
}

/// Returns `true` when an error means a Lance dataset does not exist.
///
/// Distinguishes "this index was never created" — where an empty result is the
/// correct answer — from a permission, IO or manifest failure, which must
/// propagate. `Dataset::open` on an absent path is the normal state of an index
/// before its first write, so a caller cannot simply propagate every error; it
/// also cannot swallow every error, which is how a broken index came to read as
/// "nothing registered" (#233).
///
/// Deliberately narrow, for the same reason [`is_not_found`] is: widening this
/// to "any Lance error" would restore the fail-open behaviour it exists to
/// replace. `object_store::Error::NotFound` is accepted too, because Lance
/// surfaces a missing path either way depending on the store.
#[must_use]
pub fn is_dataset_not_found(err: &anyhow::Error) -> bool {
    if is_not_found(err) {
        return true;
    }
    #[cfg(feature = "lance-backend")]
    {
        err.chain().any(|cause| {
            cause.downcast_ref::<lance::Error>().is_some_and(|e| {
                matches!(
                    e,
                    lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. }
                )
            })
        })
    }
    // Without the Lance backend there is no Lance error to classify, and the
    // `object_store` check above has already run.
    #[cfg(not(feature = "lance-backend"))]
    false
}

/// Puts an object to the store with a timeout and retries.
///
/// # Errors
///
/// Returns an error if all retry attempts fail or time out.
pub async fn put_with_timeout(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    bytes: Bytes,
    timeout: Duration,
) -> Result<PutResult> {
    let msg = format!(
        "Object store put timed out after {:?} for path: {}",
        timeout, path
    );
    retry_with_timeout(timeout, &msg, || store.put(path, bytes.clone().into())).await
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

    #[tokio::test]
    async fn test_get_not_found_returns_immediately() -> Result<()> {
        let dir = tempdir()?;
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = Path::from("does_not_exist.txt");

        let start = std::time::Instant::now();
        let result = get_with_timeout(&store, &path, DEFAULT_TIMEOUT).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "should return error for missing file");
        assert!(
            result.unwrap_err().to_string().contains("not found"),
            "error should contain 'not found'"
        );
        // Without the fix, this would take ~600ms (3 retries with backoff).
        assert!(
            elapsed.as_millis() < 200,
            "NotFound should not retry — took {}ms",
            elapsed.as_millis()
        );

        Ok(())
    }
}
