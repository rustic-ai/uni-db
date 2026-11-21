// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use std::fmt::{Debug, Display};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;
use uni_common::config::ObjectStoreConfig;

#[derive(Debug)]
struct CircuitBreaker {
    failures: AtomicU32,
    last_failure: AtomicI64, // timestamp as millis
    threshold: u32,
    reset_timeout: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, reset_timeout: Duration) -> Self {
        Self {
            failures: AtomicU32::new(0),
            last_failure: AtomicI64::new(0),
            threshold,
            reset_timeout,
        }
    }

    fn allow_request(&self) -> bool {
        let failures = self.failures.load(Ordering::Relaxed);
        if failures < self.threshold {
            return true;
        }

        let last = self.last_failure.load(Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        if (now - last) > self.reset_timeout.as_millis() as i64 {
            // Half-open: allow one request (or probabilistic)
            // Ideally we transition state. For simple Atomic impl, we allow retry after timeout.
            return true;
        }
        false
    }

    fn report_success(&self) {
        // Only reset if we were in a failure state to avoid contention?
        // Relaxed store is cheap.
        self.failures.store(0, Ordering::Relaxed);
    }

    fn report_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.last_failure.store(now, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct ResilientObjectStore {
    inner: Arc<dyn ObjectStore>,
    config: ObjectStoreConfig,
    cb: CircuitBreaker,
    /// Retries this layer adds; 0 over a client that already retries.
    max_retries: u32,
}

impl ResilientObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, config: ObjectStoreConfig) -> Self {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30)); // Hardcoded defaults for CB for now or use config?
        // We can use config.max_retries * 2 as threshold?
        // Let's use 5 failures and 30s reset.
        let max_retries = config.max_retries;
        Self {
            inner,
            config,
            cb,
            max_retries,
        }
    }

    /// Timeouts and the circuit breaker, without retries of its own: for a
    /// client that already retries (an object-store client built with the
    /// same [`ObjectStoreConfig`]), where retrying here too would multiply
    /// the attempts and the time before an error surfaces.
    pub fn without_retries(inner: Arc<dyn ObjectStore>, config: ObjectStoreConfig) -> Self {
        Self {
            max_retries: 0,
            ..Self::new(inner, config)
        }
    }

    async fn retry<F, Fut, T>(&self, mut f: F, op_name: &str) -> StoreResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = StoreResult<T>>,
    {
        if !self.cb.allow_request() {
            return Err(object_store::Error::Generic {
                store: "ResilientObjectStore",
                source: Box::new(std::io::Error::other("Circuit breaker open")),
            });
        }

        let mut attempt = 0;
        let mut backoff = self.config.retry_backoff_base;

        loop {
            match f().await {
                Ok(val) => {
                    self.cb.report_success();
                    return Ok(val);
                }
                Err(e) => {
                    // Classify BEFORE spending an attempt. A terminal error is an
                    // application-level outcome (absent, already present, denied),
                    // not a system failure, so it must neither retry nor count
                    // against the breaker.
                    //
                    // This check has to precede the attempt-budget check below:
                    // with the order reversed, a terminal error arriving on the
                    // final attempt is misreported as a breaker failure and never
                    // reaches the classifier at all.
                    //
                    // Matching is on the variant. The previous
                    // `to_string().contains("not found")` also caught a *transient*
                    // `Generic` whose message happened to read that way — a proxy
                    // 404 body, or a wrapped S3 "bucket not found" — and gave up on
                    // a blip that would have healed.
                    if crate::store_utils::is_terminal(&e) {
                        return Err(e);
                    }

                    attempt += 1;
                    if attempt > self.max_retries {
                        self.cb.report_failure();
                        return Err(e);
                    }

                    warn!(
                        error = %e,
                        attempt,
                        operation = op_name,
                        "ObjectStore operation failed, retrying"
                    );

                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, self.config.retry_backoff_max);
                }
            }
        }
    }

    async fn timeout<F, Fut, T>(&self, f: F, duration: std::time::Duration) -> StoreResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = StoreResult<T>>,
    {
        tokio::time::timeout(duration, f())
            .await
            .map_err(|_| object_store::Error::Generic {
                store: "ResilientObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "operation timed out",
                )),
            })?
    }
}

impl Display for ResilientObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResilientObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ResilientObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let timeout = self.config.write_timeout;
        if !self.cb.allow_request() {
            return Err(object_store::Error::Generic {
                store: "ResilientObjectStore",
                source: Box::new(std::io::Error::other("Circuit breaker open")),
            });
        }
        let res = self
            .timeout(|| self.inner.put_opts(location, payload, opts), timeout)
            .await;
        match &res {
            Ok(_) => self.cb.report_success(),
            // A terminal error is an application-level outcome, not a store
            // failure. `put_opts` is the conditional-write entry point, so a
            // writer losing an OCC race reports `AlreadyExists` / `Precondition`
            // here; counting those meant five ordinary write conflicts opened
            // the breaker and took every path through the store offline for 30s.
            //
            // `put_opts` does not route through `retry`, so this is the only
            // place that classification can happen for it.
            Err(e) if crate::store_utils::is_terminal(e) => {}
            Err(_) => self.cb.report_failure(),
        }
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        let timeout = self.config.write_timeout;
        self.retry(
            || async {
                self.timeout(
                    || self.inner.put_multipart_opts(location, opts.clone()),
                    timeout,
                )
                .await
            },
            "put_multipart_opts",
        )
        .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let timeout = self.config.read_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.get_opts(location, options.clone()), timeout)
                    .await
            },
            "get_opts",
        )
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        let timeout = self.config.read_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.get_ranges(location, ranges), timeout)
                    .await
            },
            "get_ranges",
        )
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        if !self.cb.allow_request() {
            return futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "ResilientObjectStore",
                    source: Box::new(std::io::Error::other("Circuit breaker open")),
                })
            })
            .boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        // Check CB? List returns stream.
        // We can check CB on initial call.
        if !self.cb.allow_request() {
            // How to return error stream?
            // We can return a stream that yields an error immediately.
            // But BoxStream signature expects Stream of Results.
            return futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "ResilientObjectStore",
                    source: Box::new(std::io::Error::other("Circuit breaker open")),
                })
            })
            .boxed();
        }

        // We wrap stream to report failure/success?
        // Too complex for P2. Just pass through.
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        if !self.cb.allow_request() {
            return futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "ResilientObjectStore",
                    source: Box::new(std::io::Error::other("Circuit breaker open")),
                })
            })
            .boxed();
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        let timeout = self.config.read_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.list_with_delimiter(prefix), timeout)
                    .await
            },
            "list_with_delimiter",
        )
        .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        let timeout = self.config.write_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.copy_opts(from, to, options.clone()), timeout)
                    .await
            },
            "copy_opts",
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    // ── CircuitBreaker unit tests ────────────────────────────────────

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        assert!(cb.allow_request());
        assert_eq!(cb.failures.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cb_closed_to_open_after_threshold() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        for _ in 0..5 {
            cb.report_failure();
        }
        assert!(!cb.allow_request(), "CB should be open after 5 failures");
    }

    #[test]
    fn test_cb_stays_closed_below_threshold() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        for _ in 0..4 {
            cb.report_failure();
        }
        assert!(cb.allow_request(), "CB should stay closed with 4 failures");
    }

    /// Reset timeout for the tests that assert the breaker is *open*.
    ///
    /// It has to outlast scheduling jitter between `report_failure` and
    /// `allow_request`: with 1 ms, a thread preempted for a millisecond under a
    /// loaded test run found the breaker already half-open and failed "CB should
    /// be open". `HALF_OPEN_WAIT` sleeps past it.
    const RESET: Duration = Duration::from_millis(200);
    const HALF_OPEN_WAIT: Duration = Duration::from_millis(300);

    #[test]
    fn test_cb_open_to_half_open_after_timeout() {
        let cb = CircuitBreaker::new(2, RESET);
        cb.report_failure();
        cb.report_failure();
        assert!(!cb.allow_request(), "CB should be open");

        std::thread::sleep(HALF_OPEN_WAIT);
        assert!(
            cb.allow_request(),
            "CB should allow probe after reset timeout"
        );
    }

    #[test]
    fn test_cb_half_open_to_closed_on_success() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(1));
        cb.report_failure();
        cb.report_failure();
        std::thread::sleep(Duration::from_millis(5));

        // In half-open state, report success
        cb.report_success();
        assert_eq!(cb.failures.load(Ordering::Relaxed), 0);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_cb_half_open_to_open_on_failure() {
        let cb = CircuitBreaker::new(2, RESET);
        cb.report_failure();
        cb.report_failure();
        std::thread::sleep(HALF_OPEN_WAIT);
        assert!(cb.allow_request(), "CB should be half-open after the reset");

        // In half-open state, report another failure
        cb.report_failure();
        assert!(
            !cb.allow_request(),
            "CB should be open again after failure in half-open"
        );
    }

    #[test]
    fn test_cb_success_resets_failures() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        cb.report_failure();
        cb.report_failure();
        cb.report_failure();
        assert_eq!(cb.failures.load(Ordering::Relaxed), 3);

        cb.report_success();
        assert_eq!(cb.failures.load(Ordering::Relaxed), 0);
    }

    // ── ResilientObjectStore integration tests ───────────────────────

    #[tokio::test]
    async fn test_resilient_store_retry_succeeds() {
        // InMemory store succeeds on first try — verify success path
        let inner = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let config = ObjectStoreConfig {
            connect_timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(10),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        };
        let store = ResilientObjectStore::new(inner, config);

        // Write and read back
        let path = Path::from("test/key");
        store
            .put(&path, PutPayload::from_static(b"hello"))
            .await
            .unwrap();
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello");

        // CB should be healthy
        assert_eq!(store.cb.failures.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_resilient_store_circuit_open_rejects() {
        let inner = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let config = ObjectStoreConfig {
            connect_timeout: Duration::from_secs(5),
            max_retries: 1,
            retry_backoff_base: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(10),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        };
        let store = ResilientObjectStore::new(inner, config);

        // Manually trip the circuit breaker
        for _ in 0..5 {
            store.cb.report_failure();
        }

        // All operations should fail with circuit breaker error
        let err = store.get(&Path::from("any")).await.unwrap_err();
        assert!(
            err.to_string().contains("Circuit breaker open"),
            "Expected circuit breaker error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_resilient_store_not_found_skips_retry() {
        let inner = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let config = ObjectStoreConfig {
            connect_timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(10),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        };
        let store = ResilientObjectStore::new(inner, config);

        // Get a non-existent key — should return error but NOT increment CB
        let err = store.get(&Path::from("nonexistent")).await.unwrap_err();
        assert!(err.to_string().to_lowercase().contains("not found"));
        assert_eq!(
            store.cb.failures.load(Ordering::Relaxed),
            0,
            "Not-found errors should not count as CB failures"
        );
    }

    /// Fails every read with a transient error, counting attempts.
    #[derive(Debug, Default)]
    struct FlakyReads {
        inner: InMemory,
        reads: std::sync::atomic::AtomicU32,
    }

    impl Display for FlakyReads {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyReads")
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyReads {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> StoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, _location: &Path, _options: GetOptions) -> StoreResult<GetResult> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Err(object_store::Error::Generic {
                store: "FlakyReads",
                source: Box::new(std::io::Error::other("connection reset")),
            })
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<Path>>,
        ) -> BoxStream<'static, StoreResult<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Over a client that retries on its own (an object-store client built
    /// with the same retry settings), the wrapper keeps its timeouts and
    /// circuit breaker but makes one attempt, so retries do not multiply.
    #[tokio::test]
    async fn without_retries_makes_one_attempt() {
        let config = ObjectStoreConfig {
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(2),
            ..Default::default()
        };
        let inner = Arc::new(FlakyReads::default());
        let store = ResilientObjectStore::without_retries(inner.clone(), config.clone());
        assert!(store.get(&Path::from("k")).await.is_err());
        assert_eq!(inner.reads.load(Ordering::SeqCst), 1);
        assert_eq!(store.cb.failures.load(Ordering::Relaxed), 1);

        let inner = Arc::new(FlakyReads::default());
        let store = ResilientObjectStore::new(inner.clone(), config);
        assert!(store.get(&Path::from("k")).await.is_err());
        assert_eq!(inner.reads.load(Ordering::SeqCst), 4);
    }
}
