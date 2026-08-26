// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct CompactionConfig {
    /// Enable background compaction (default: true)
    pub enabled: bool,

    /// Max uncompacted flush generations before triggering compaction (default: 8)
    pub max_l1_runs: usize,

    /// Max L1 size in bytes before compaction (default: 256MB)
    pub max_l1_size_bytes: u64,

    /// Max age of oldest L1 run before compaction (default: 1 hour)
    pub max_l1_age: Duration,

    /// Background check interval (default: 10s)
    pub check_interval: Duration,

    /// Number of compaction worker threads (default: 1)
    pub worker_threads: usize,

    /// Number of frozen L0-csr overlay segments that must accumulate before
    /// `AdjacencyManager::compact` is spawned post-flush (default: 2).
    ///
    /// Each frozen segment adds per-read overhead until merged back into the
    /// Main CSR. Lowering this triggers compaction sooner; higher values
    /// batch more segments per compaction at the cost of slower reads while
    /// they accumulate. The default of 2 keeps the read-side overhead
    /// bounded across a wide range of write rates. See issue #55.
    pub frozen_segments_compact_threshold: usize,

    /// How long a superseded dataset version is retained before compaction may
    /// delete it (default: 7 days).
    ///
    /// This is what `CompactionStats::bytes_reclaimed` measures against: nothing
    /// is reclaimed until a version is older than this, so a database younger
    /// than the window always reports zero. It is configurable so that a test
    /// can set it to zero and observe a non-zero reclaim — without that, the
    /// field would be real in production and indistinguishable from a constant
    /// everywhere it could be checked.
    ///
    /// Lowering it below the lifetime of the longest live fork chain is safe:
    /// version cleanup is branch-aware and retains versions a live branch still
    /// needs regardless of age.
    pub version_retention: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_l1_runs: 8,
            max_l1_size_bytes: 256 * 1024 * 1024,
            max_l1_age: Duration::from_secs(3600),
            check_interval: Duration::from_secs(10),
            worker_threads: 1,
            frozen_segments_compact_threshold: 2,
            version_retention: Duration::from_secs(7 * 24 * 3600),
        }
    }
}

/// Configuration for background index rebuilding.
#[derive(Clone, Debug)]
pub struct IndexRebuildConfig {
    /// Maximum number of retry attempts for failed index builds (default: 3).
    pub max_retries: u32,

    /// Delay between retry attempts (default: 60s).
    pub retry_delay: Duration,

    /// How often to check for pending index rebuild tasks (default: 5s).
    pub worker_check_interval: Duration,

    /// Row growth ratio to trigger rebuild (default: 0.5 = 50%). Set 0.0 to disable.
    pub growth_trigger_ratio: f64,

    /// Max index age before rebuild. `None` disables the time-based trigger.
    pub max_index_age: Option<Duration>,

    /// Enable post-flush automatic rebuild scheduling (default: false).
    pub auto_rebuild_enabled: bool,
}

impl Default for IndexRebuildConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay: Duration::from_secs(60),
            worker_check_interval: Duration::from_secs(5),
            growth_trigger_ratio: 0.5,
            max_index_age: None,
            auto_rebuild_enabled: false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WriteThrottleConfig {
    /// Uncompacted flush generations to start throttling (default: 16)
    pub soft_limit: usize,

    /// Uncompacted flush generations to stop writes entirely (default: 32)
    pub hard_limit: usize,

    /// Base delay when throttling (default: 10ms)
    pub base_delay: Duration,
}

impl Default for WriteThrottleConfig {
    fn default() -> Self {
        Self {
            soft_limit: 16,
            hard_limit: 32,
            base_delay: Duration::from_millis(10),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObjectStoreConfig {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub max_retries: u32,
    pub retry_backoff_base: Duration,
    pub retry_backoff_max: Duration,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(60),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(10),
        }
    }
}

/// Security configuration for file system operations.
/// Controls which paths can be accessed by BACKUP, COPY, and EXPORT commands.
///
/// Disabled by default for backward compatibility in embedded mode.
/// MUST be enabled for server mode with untrusted clients.
#[derive(Clone, Debug, Default)]
pub struct FileSandboxConfig {
    /// If true, file operations are restricted to allowed_paths.
    /// If false, all paths are allowed (NOT RECOMMENDED for server mode).
    pub enabled: bool,

    /// List of allowed base directories for file operations.
    /// Paths must be absolute and canonical.
    /// File operations are only allowed within these directories.
    pub allowed_paths: Vec<PathBuf>,
}

/// Deployment mode for the database.
///
/// Used to determine appropriate security defaults.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeploymentMode {
    /// Embedded/library mode where the host application controls access.
    /// File sandbox is disabled by default for backward compatibility.
    #[default]
    Embedded,
    /// Server mode with untrusted clients.
    /// File sandbox is enabled by default with restricted paths.
    Server,
}

/// HTTP server configuration.
///
/// Controls CORS, authentication, and other HTTP-related security settings.
///
/// # Security
///
/// **CWE-942 (Overly Permissive CORS)**, **CWE-306 (Missing Authentication)**:
/// Production deployments should configure explicit `allowed_origins` and
/// enable API key authentication.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Allowed CORS origins.
    ///
    /// - Empty vector: No CORS headers (most restrictive)
    /// - `["*"]`: Allow all origins (NOT RECOMMENDED for production)
    /// - Explicit list: Only allow specified origins (RECOMMENDED)
    ///
    /// # Security
    ///
    /// **CWE-942**: Using `["*"]` allows any website to make requests to
    /// your server, potentially exposing sensitive data.
    pub allowed_origins: Vec<String>,

    /// Optional API key for request authentication.
    ///
    /// When set, all API requests must include the header:
    /// `X-API-Key: <key>`
    ///
    /// # Security
    ///
    /// **CWE-306**: Without authentication, any client can execute queries.
    /// Enable this for any deployment accessible beyond localhost.
    pub api_key: Option<String>,

    /// Whether to require API key for metrics endpoint.
    ///
    /// Default: false (metrics are public for observability tooling)
    pub require_auth_for_metrics: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            // Default to localhost-only origin for development safety
            allowed_origins: vec!["http://localhost:3000".to_string()],
            api_key: None,
            require_auth_for_metrics: false,
        }
    }
}

impl ServerConfig {
    /// Create a permissive config for local development only.
    ///
    /// # Security
    ///
    /// **WARNING**: Do not use in production. This config allows all CORS origins
    /// and has no authentication.
    #[must_use]
    pub fn development() -> Self {
        Self {
            allowed_origins: vec!["*".to_string()],
            api_key: None,
            require_auth_for_metrics: false,
        }
    }

    /// Create a production config with explicit origins and required API key.
    ///
    /// # Panics
    ///
    /// Panics if `api_key` is empty.
    #[must_use]
    pub fn production(allowed_origins: Vec<String>, api_key: String) -> Self {
        assert!(
            !api_key.is_empty(),
            "API key must not be empty for production"
        );
        Self {
            allowed_origins,
            api_key: Some(api_key),
            require_auth_for_metrics: true,
        }
    }

    /// Returns a security warning if the config is insecure.
    pub fn security_warning(&self) -> Option<&'static str> {
        let allows_all_origins = self.allowed_origins.iter().any(|o| o == "*");
        if allows_all_origins && self.api_key.is_none() {
            Some(
                "Server config has permissive CORS (allow all origins) and no API key. \
                 This is insecure for production deployments.",
            )
        } else if allows_all_origins {
            Some(
                "Server config has permissive CORS (allow all origins). \
                 Consider restricting to specific origins for production.",
            )
        } else if self.api_key.is_none() {
            Some(
                "Server config has no API key authentication. \
                 Enable api_key for production deployments.",
            )
        } else {
            None
        }
    }
}

impl FileSandboxConfig {
    /// Creates a sandboxed config that only allows operations in the specified directories.
    pub fn sandboxed(paths: Vec<PathBuf>) -> Self {
        Self {
            enabled: true,
            allowed_paths: paths,
        }
    }

    /// Creates a config with appropriate defaults for the deployment mode.
    ///
    /// # Security
    ///
    /// - **Embedded mode**: Sandbox disabled (host application controls access)
    /// - **Server mode**: Sandbox enabled with default paths `/var/lib/uni/data` and
    ///   `/var/lib/uni/backups`
    ///
    /// **CWE-22 (Path Traversal)**: Server deployments MUST enable the sandbox to
    /// prevent arbitrary file read/write via BACKUP, COPY, and EXPORT commands.
    pub fn default_for_mode(mode: DeploymentMode) -> Self {
        match mode {
            DeploymentMode::Embedded => Self {
                enabled: false,
                allowed_paths: vec![],
            },
            DeploymentMode::Server => Self {
                enabled: true,
                allowed_paths: vec![
                    PathBuf::from("/var/lib/uni/data"),
                    PathBuf::from("/var/lib/uni/backups"),
                ],
            },
        }
    }

    /// Returns a security warning message if the sandbox is disabled.
    ///
    /// Call this at startup to alert administrators about potential security risks.
    /// Returns `Some(message)` if a warning should be displayed, `None` otherwise.
    ///
    /// # Security
    ///
    /// **CWE-22 (Path Traversal)**, **CWE-73 (External Control of File Name)**:
    /// Disabled sandbox allows unrestricted filesystem access for BACKUP, COPY,
    /// and EXPORT commands, which can lead to:
    /// - Arbitrary file read/write in server deployments
    /// - Data exfiltration to attacker-controlled paths
    /// - Potential privilege escalation via file overwrites
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(warning) = config.file_sandbox.security_warning() {
    ///     tracing::warn!(target: "uni_db::security", "{}", warning);
    /// }
    /// ```
    pub fn security_warning(&self) -> Option<&'static str> {
        if !self.enabled {
            Some(
                "File sandbox is DISABLED. This allows unrestricted filesystem access \
                 for BACKUP, COPY, and EXPORT commands. Enable sandbox for server \
                 deployments: file_sandbox.enabled = true",
            )
        } else {
            None
        }
    }

    /// Returns whether the sandbox is in a potentially insecure state.
    ///
    /// Returns `true` if the sandbox is disabled or enabled with no allowed paths.
    pub fn is_potentially_insecure(&self) -> bool {
        !self.enabled || self.allowed_paths.is_empty()
    }

    /// Validate that a path is within the allowed sandbox.
    /// Returns Ok(canonical_path) if allowed, Err if not.
    pub fn validate_path(&self, path: &str) -> Result<PathBuf, String> {
        if !self.enabled {
            // Sandbox disabled - allow all paths
            return Ok(PathBuf::from(path));
        }

        if self.allowed_paths.is_empty() {
            return Err("File sandbox is enabled but no allowed paths configured".to_string());
        }

        // Resolve the path to canonical form to prevent traversal attacks
        let input_path = Path::new(path);

        // For paths that don't exist yet (e.g., export destinations), we need to
        // check their parent directory exists and is within allowed paths
        let canonical = if input_path.exists() {
            input_path
                .canonicalize()
                .map_err(|e| format!("Failed to canonicalize path: {}", e))?
        } else {
            // Path doesn't exist - check parent
            let parent = input_path
                .parent()
                .ok_or_else(|| "Invalid path: no parent directory".to_string())?;
            if !parent.exists() {
                return Err(format!(
                    "Parent directory does not exist: {}",
                    parent.display()
                ));
            }
            let canonical_parent = parent
                .canonicalize()
                .map_err(|e| format!("Failed to canonicalize parent: {}", e))?;
            // Reconstruct with canonical parent + original filename
            let filename = input_path
                .file_name()
                .ok_or_else(|| "Invalid path: no filename".to_string())?;
            canonical_parent.join(filename)
        };

        // Check if the canonical path is within any allowed directory
        for allowed in &self.allowed_paths {
            // Ensure allowed path is canonical too
            let canonical_allowed = if allowed.exists() {
                allowed.canonicalize().unwrap_or_else(|_| allowed.clone())
            } else {
                allowed.clone()
            };

            if canonical.starts_with(&canonical_allowed) {
                return Ok(canonical);
            }
        }

        Err(format!(
            "Path '{}' is outside allowed sandbox directories. Allowed: {:?}",
            path, self.allowed_paths
        ))
    }
}

#[derive(Clone, Debug)]
pub struct UniConfig {
    /// Maximum adjacency cache size in bytes (default: 1GB)
    pub cache_size: usize,

    /// Number of worker threads for parallel execution
    pub parallelism: usize,

    /// Size of each data morsel/batch (number of rows)
    pub batch_size: usize,

    /// Maximum size of traversal frontier before pruning
    pub max_frontier_size: usize,

    /// Auto-flush threshold for L0 buffer (default: 10_000 mutations)
    pub auto_flush_threshold: usize,

    /// Auto-flush interval for L0 buffer (default: 5 seconds).
    /// Flush triggers if time elapsed AND mutation count >= auto_flush_min_mutations.
    /// Set to None to disable time-based flush.
    pub auto_flush_interval: Option<Duration>,

    /// Minimum mutations required before the time-based flush triggers
    /// (default: 1).
    ///
    /// Prevents unnecessary flushes when activity is minimal. Raising this
    /// (e.g., to 1000) lets small bursts coalesce into one flush — useful
    /// for benchmark workloads — but for active databases with high write
    /// rates, raising it reduces flush frequency and lets the active overlay
    /// grow larger between flushes, which can hurt read latency. Tune with
    /// `compaction.frozen_segments_compact_threshold` together. See issue
    /// #55 for the trade-off discussion.
    pub auto_flush_min_mutations: usize,

    /// Enable write-ahead logging (default: true)
    pub wal_enabled: bool,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// Write throttling configuration
    pub throttle: WriteThrottleConfig,

    /// File sandbox configuration for BACKUP/COPY/EXPORT commands.
    /// MUST be enabled with allowed paths in server mode to prevent arbitrary file access.
    pub file_sandbox: FileSandboxConfig,

    /// Default query execution timeout (default: 30s)
    pub query_timeout: Duration,

    /// Maximum wall time a transaction commit may take before it is aborted with
    /// `CommitTimeout` (default: 5s). This guards against a commit blocking on the
    /// writer/flush lock, but it also bounds the commit's own compute time — so
    /// workloads that commit very large transactions in a single shot (bulk-history
    /// backfills, or unoptimized debug builds) may need to raise it.
    pub commit_timeout: Duration,

    /// Default maximum memory per query (default: 1GB)
    pub max_query_memory: usize,

    /// Maximum transaction buffer memory in bytes (default: 1GB).
    /// Limits memory usage during transactions to prevent OOM.
    pub max_transaction_memory: usize,

    /// Maximum rows for in-memory compaction (default: 5M, ~725MB at 145 bytes/row).
    /// Configurable OOM guard to prevent memory exhaustion during compaction.
    pub max_compaction_rows: usize,

    /// Maximum iterations for recursive CTE evaluation (default: 1000).
    pub max_recursive_cte_iterations: usize,

    /// Object store resilience configuration
    pub object_store: ObjectStoreConfig,

    /// Background index rebuild configuration
    pub index_rebuild: IndexRebuildConfig,

    /// When true, reject writes that reference labels or edge types not declared
    /// in the schema. Default: false (schemaless mode — any label or edge type
    /// is accepted and dynamically registered).
    pub strict_schema: bool,

    /// Enable Lance `MergeInsert` for SET-only flushes (default: false).
    ///
    /// When true, `Writer::insert_vertex_partial` records the touched
    /// property keys into L0 and the flush emits a partial-column source
    /// to Lance via `MergeInsertBuilder` — skipping the read of (and write
    /// of) the unchanged columns. Wide-row schemas with vector indexes
    /// benefit most (~17 ms/row → ~3 ms/row on the issue #72 ingest
    /// workload). See the Round-11 plan section in
    /// `plan-and-implement-a-valiant-flame.md`.
    pub partial_lance_writes: bool,

    /// When true, auto-embedding for vertex writes is deferred from the
    /// per-row `insert_vertex_*` path to the next L1 flush, where the
    /// existing `process_embeddings_for_batch` issues one model call for
    /// the whole flush batch instead of N per-row calls.
    ///
    /// Trade-off: in-tx reads of the embedding column on a freshly
    /// SET/inserted vertex see the OLD storage value (or no value, for
    /// new vertices) until flush. Existing behavior is identical to
    /// today's `process_embeddings_impl(target_prop present)` short-circuit
    /// (writer.rs:2727) — updating only the source text never refreshes
    /// the embedding mid-tx, deferred or not. Opt-in for workloads that
    /// don't read embeddings between write and commit.
    ///
    /// Default: `false` (preserves bit-for-bit compatibility with
    /// pre-Phase-B releases).
    pub defer_embeddings: bool,

    /// Per-fork L1 fragment-count threshold above which a `tracing::warn!`
    /// fires once per crossing during fork flush. Long-lived heavy-write
    /// forks accumulate fragments because fork compaction is deferred to
    /// Phase 5; this surfaces the risk operationally. Default: 256.
    pub fork_fragment_warn_threshold: usize,

    /// Per-transaction VID/EID reservoir refill size. Each `Transaction`
    /// pre-reserves this many IDs at a time from the global `IdAllocator`,
    /// amortizing its `tokio::Mutex` over `N` allocations. Tradeoff:
    /// larger = fewer global-mutex acquisitions but more wasted IDs on
    /// short transactions (capped at `batch_size - 1` per tx). u64 ID space
    /// makes the waste negligible. Default: 16.
    pub tx_id_reservoir_batch: usize,

    /// When `true`, `check_flush` on the commit path dispatches via the
    /// async path (`flush_to_l1_async`): rotate L0 under `flush_lock`,
    /// then spawn the streaming + finalize work on a background task.
    /// Concurrent committers no longer queue on the flush's long I/O.
    ///
    /// When `false` (default for now), `check_flush` calls the original
    /// synchronous `flush_to_l1` and holds `flush_lock` across the full
    /// L1-streaming write. This is the kill-switch.
    pub async_flush_enabled: bool,

    /// Maximum number of L0→L1 flushes that may be in-flight simultaneously
    /// when `async_flush_enabled` is true. The (N+1)th rotate blocks until
    /// one of the in-flight flushes finalizes. Bounds WAL retention and
    /// memory growth. Default: 2.
    pub max_pending_flushes: usize,

    /// Maximum wall-clock time an async L0→L1 stream phase may run before the
    /// flush coordinator converts it into a data-safe flush *failure* (issue
    /// #132). A stalled sparse/multi-vector Lance read-modify-write would
    /// otherwise never submit its rotate-seq, wedging the finalizer's
    /// consecutive-seq pipeline and — via back-pressure permit saturation —
    /// parking every later commit forever on `flush_lock`. On timeout the old
    /// L0 is retained in `pending_flush`, WAL data is NOT truncated, the
    /// permit is released, and `expected` advances; recovery is via WAL replay
    /// / a later retry. Only meaningful when `async_flush_enabled` is true.
    /// Default: 60s. Override with `UNI_FLUSH_STREAM_TIMEOUT` (seconds).
    pub flush_stream_timeout: Duration,

    /// Maximum time `drop_fork` will wait for pending async flushes on
    /// that fork before failing with `PendingFlushTimeout`. Only meaningful
    /// when `async_flush_enabled` is true. Default: 10s.
    pub drop_fork_drain_timeout: Duration,

    /// Maximum time `flush_to_l1` will wait **without progress** while draining
    /// in-flight async flushes, before failing. Default: 900s.
    ///
    /// This bounds a *stall*, not total drain time. Ingesting a million rows
    /// queues roughly one flush per `auto_flush_threshold` mutations, so the
    /// backlog scales with the data and no fixed total is right for every load —
    /// a 3.8 GB corpus legitimately drains for well past any constant worth
    /// setting. What marks a wedged coordinator is that the pending count stops
    /// falling, which is what this measures.
    ///
    /// The default is deliberately generous because this is a backstop, not a
    /// control: its job is to stop a wedged coordinator hanging forever, and the
    /// per-stream `flush_stream_timeout` is what is meant to catch a stuck
    /// stream first. **Known gap:** a single large flush (measured on a 3.8 GB,
    /// 960-d corpus) can run past `flush_stream_timeout` without the pending
    /// count moving, so at this layer a legitimately slow flush and a stalled
    /// one are indistinguishable. Until that is resolved the bound has to exceed
    /// the longest single flush a deployment expects. Erring high is the right
    /// side to err on: the failure this replaced returned success while not
    /// having flushed.
    ///
    /// `flush_to_l1` is a **synchronization barrier**: fork setup, shutdown and
    /// test fixtures rely on it meaning "all writes are now durably in Lance".
    /// It previously bounded this wait with `drop_fork_drain_timeout` (10s) and
    /// **discarded the result**, so above roughly 600k rows the drain silently
    /// did not finish and the barrier returned success anyway. Downstream, the
    /// index build found no flushed table, declined at `debug!` level, and
    /// reported `Online` — measured consequence was an ANN index that was never
    /// built while every query quietly fell back to an exact scan.
    ///
    /// This is a backstop against a wedged coordinator, not a normal control:
    /// each individual stream is already bounded by `flush_stream_timeout`, so a
    /// healthy drain finishes far inside it (a 1M-row flush measures ~27s).
    /// Exceeding it is an error, never a silent partial flush.
    pub flush_drain_timeout: Duration,

    /// Phase 4a: cap on total fork count (Active + Pending + Tombstoned).
    /// `None` = unbounded. When set, `Session::fork(name).await` errors
    /// with `UniError::ForkBudgetExceeded` once the cap is reached.
    /// Tombstoned forks count because they still hold branch state on
    /// disk until recovery completes; counting them prevents churn-thrash.
    ///
    /// **Production guidance (L11):** the default is `None` (unbounded) to
    /// avoid surprising existing embedders, but each fork's branches scale
    /// with schema size and persist until dropped, so unbounded fork churn
    /// is an on-disk growth risk. Production deployments that create forks
    /// from untrusted/automated callers SHOULD set an explicit `max_forks`
    /// (and ideally `fork_default_ttl`).
    pub max_forks: Option<usize>,

    /// Phase 4a: default TTL applied to forks when the user does not
    /// supply one via `session.fork(name).ttl(...)`. `None` = no TTL.
    /// The background sweeper drops forks whose `ttl_expires_at` is in
    /// the past via `drop_fork_cascade`.
    pub fork_default_ttl: Option<Duration>,

    /// Phase 4a: how often the background TTL sweeper polls the
    /// registry for expired forks. Default: 60 seconds.
    pub fork_sweeper_interval: Duration,

    /// Phase 4a: skip spawning the TTL sweeper. Tests should set this
    /// to `true` when they want deterministic control over fork
    /// lifetimes; production should leave it `false`.
    pub disable_fork_sweeper: bool,

    /// Phase 5a: minimum per-fork row count (per label) before the
    /// background `IndexRebuildManager` schedules a fork-local index
    /// build. Below this threshold, fork reads inherit primary's
    /// indexes through Lance `base_paths`; above it, the planner
    /// switches to `FusedIndexScan` once the build completes. Default
    /// 10,000 rows per spec §8.
    pub fork_index_build_threshold: u64,

    /// Phase 5a-impl Step 7: how often the background fork index
    /// builder polls active forks for build candidates. Default
    /// 30 seconds.
    pub fork_index_builder_interval: Duration,

    /// Phase 5a-impl Step 7: skip spawning the background fork index
    /// builder. Tests that exercise the manual `Session::build_fork_local_index`
    /// trigger should set this to `true` so timing isn't dependent on
    /// the polling cadence.
    pub disable_fork_index_builder: bool,

    /// Enable Serializable Snapshot Isolation and optimistic concurrency
    /// control (default: `true`).
    ///
    /// When `true`, read-write transactions read from a pinned L0 snapshot,
    /// track an item-level read/write-set, and validate at commit under
    /// `flush_lock`: a write-write or read-write conflict against a commit
    /// landed since the transaction's snapshot aborts with
    /// `UniError::SerializationConflict`, a duplicate concurrent `MERGE` on a
    /// unique key aborts with `UniError::ConstraintConflict`, and `FOR UPDATE`
    /// acquires per-key row locks. Callers should wrap contended writes in
    /// `Session::transact_with_retry`, which re-runs retriable conflicts.
    ///
    /// When `false`, the engine reverts to last-writer-wins: concurrent
    /// read-modify-write transactions can silently lose updates, concurrent
    /// `MERGE` can create duplicate unique keys, and `FOR UPDATE` is a no-op
    /// (a `tracing::warn!` is emitted when a query requests it). Reads run
    /// against the live L0 with no snapshot pinning. This reproduces the
    /// pre-SSI behavior bit-for-bit and skips the (near-zero, but non-nil)
    /// read-set/validation overhead — appropriate only for single-writer
    /// workloads or callers that guard read-modify-write externally.
    ///
    /// Defaults to `true` because silent lost updates are a correctness hazard
    /// for any concurrent-writer workload.
    pub ssi_enabled: bool,

    /// Max wall-clock time an `EventualConsistency` trigger's coalescing
    /// bucket may accumulate before it is flushed as a single batched fire
    /// (default: 1s). The per-`Uni` deferral tick drains buckets whose
    /// oldest pending batch is older than this. Lower = fresher fires,
    /// higher = more coalescing per fire. See `FireMode::EventualConsistency`.
    pub ec_flush_interval: Duration,

    /// Row count at which an `EventualConsistency` trigger's coalescing
    /// bucket flushes early, before `ec_flush_interval` elapses (default:
    /// 10_000). A bucket whose accumulated rows reach this threshold is
    /// drained on the next tick regardless of age; a bucket that overshoots
    /// `4 ×` this cap is force-drained inline at enqueue time (back-pressure)
    /// so an in-flight commit's coalescing buffer can never grow unbounded.
    pub ec_flush_threshold: usize,
}

impl Default for UniConfig {
    fn default() -> Self {
        let parallelism = thread::available_parallelism().map_or(4, |n| n.get());

        Self {
            cache_size: 1024 * 1024 * 1024, // 1GB
            parallelism,
            batch_size: 1024, // Default morsel size
            max_frontier_size: 1_000_000,
            auto_flush_threshold: 10_000,
            auto_flush_interval: Some(Duration::from_secs(5)),
            auto_flush_min_mutations: 1,
            wal_enabled: true,
            compaction: CompactionConfig::default(),
            throttle: WriteThrottleConfig::default(),
            file_sandbox: FileSandboxConfig::default(),
            query_timeout: Duration::from_secs(30),
            commit_timeout: Duration::from_secs(5),
            max_query_memory: 1024 * 1024 * 1024,       // 1GB
            max_transaction_memory: 1024 * 1024 * 1024, // 1GB
            max_compaction_rows: 5_000_000,             // 5M rows
            max_recursive_cte_iterations: 1000,
            object_store: ObjectStoreConfig::default(),
            index_rebuild: IndexRebuildConfig::default(),
            strict_schema: false,
            partial_lance_writes: false,
            defer_embeddings: false,
            fork_fragment_warn_threshold: 256,
            tx_id_reservoir_batch: 16,
            // Default ON as of the Item-B-deep-fix landing (per-table
            // write serialization + Lance Table cache removed + drain
            // in flush_to_l1). Validated by full UNI_ASYNC_FLUSH=1
            // cross-crate nextest: 1754/1754 pass.
            //
            // `UNI_ASYNC_FLUSH=0` / `=false` / `=no` (case-insensitive)
            // explicitly DISABLES async flush — useful for bisecting
            // suspected async-flush regressions and for the sync-only
            // benchmarks in `flush_pressure.rs`. Unset = default
            // behavior (true).
            async_flush_enabled: std::env::var("UNI_ASYNC_FLUSH").map_or(true, |v| {
                let v = v.to_ascii_lowercase();
                !(v == "0" || v == "false" || v == "no")
            }),
            max_pending_flushes: 2,
            // Bound a stalled async flush stream so a lost-wakeup in the
            // sparse/multivec Lance RMW can't permanently wedge the pipeline
            // (issue #132). 60s ≈ 200× a healthy flush, so it never kills a
            // legitimately-slow flush, yet recovers a true stall in a minute.
            flush_stream_timeout: std::env::var("UNI_FLUSH_STREAM_TIMEOUT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(60)),
            drop_fork_drain_timeout: Duration::from_secs(10),
            flush_drain_timeout: Duration::from_secs(900),
            max_forks: None,
            fork_default_ttl: None,
            fork_sweeper_interval: Duration::from_secs(60),
            disable_fork_sweeper: false,
            fork_index_build_threshold: 10_000,
            fork_index_builder_interval: Duration::from_secs(30),
            disable_fork_index_builder: false,
            // Correctness-first default: SSI/OCC on. See the field docs for
            // the behavioral contract and the migration note (concurrent
            // writers now observe aborts instead of silent lost updates;
            // wrap them in `Session::transact_with_retry`).
            ssi_enabled: true,
            // EventualConsistency coalescing (WS-E): default to a 1s window
            // and a 10k-row early-flush threshold, mirroring the L0
            // auto-flush knobs so operators reason about them together.
            ec_flush_interval: Duration::from_secs(1),
            ec_flush_threshold: 10_000,
        }
    }
}

/// Cloud storage backend configuration.
///
/// Supports Amazon S3, Google Cloud Storage, and Azure Blob Storage.
/// Each variant contains the credentials and connection parameters for
/// its respective cloud provider.
///
/// # Examples
///
/// ```ignore
/// // Create S3 configuration from environment variables
/// let config = CloudStorageConfig::s3_from_env("my-bucket");
///
/// // Create explicit S3 configuration for LocalStack testing
/// let config = CloudStorageConfig::S3 {
///     bucket: "test-bucket".to_string(),
///     region: Some("us-east-1".to_string()),
///     endpoint: Some("http://localhost:4566".to_string()),
///     access_key_id: Some("test".to_string()),
///     secret_access_key: Some("test".to_string()),
///     session_token: None,
///     virtual_hosted_style: false,
/// };
/// ```
#[derive(Clone, Debug)]
pub enum CloudStorageConfig {
    /// Amazon S3 storage configuration.
    S3 {
        /// S3 bucket name.
        bucket: String,
        /// AWS region (e.g., "us-east-1"). Uses AWS_REGION env var if None.
        region: Option<String>,
        /// Custom endpoint URL for S3-compatible services (MinIO, LocalStack).
        endpoint: Option<String>,
        /// AWS access key ID. Uses AWS_ACCESS_KEY_ID env var if None.
        access_key_id: Option<String>,
        /// AWS secret access key. Uses AWS_SECRET_ACCESS_KEY env var if None.
        secret_access_key: Option<String>,
        /// AWS session token for temporary credentials.
        session_token: Option<String>,
        /// Use virtual-hosted-style requests (bucket.s3.region.amazonaws.com).
        virtual_hosted_style: bool,
    },
    /// Google Cloud Storage configuration.
    Gcs {
        /// GCS bucket name.
        bucket: String,
        /// Path to service account JSON key file.
        service_account_path: Option<String>,
        /// Service account JSON key content (alternative to path).
        service_account_key: Option<String>,
    },
    /// Azure Blob Storage configuration.
    Azure {
        /// Azure container name.
        container: String,
        /// Azure storage account name.
        account: String,
        /// Azure storage account access key.
        access_key: Option<String>,
        /// Azure SAS token for limited access.
        sas_token: Option<String>,
    },
}

impl CloudStorageConfig {
    /// Creates an S3 configuration using environment variables.
    ///
    /// Reads credentials from standard AWS environment variables:
    /// - `AWS_ACCESS_KEY_ID`
    /// - `AWS_SECRET_ACCESS_KEY`
    /// - `AWS_SESSION_TOKEN` (optional)
    /// - `AWS_REGION` or `AWS_DEFAULT_REGION`
    /// - `AWS_ENDPOINT_URL` (optional, for S3-compatible services)
    #[must_use]
    pub fn s3_from_env(bucket: &str) -> Self {
        Self::S3 {
            bucket: bucket.to_string(),
            region: std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .ok(),
            endpoint: std::env::var("AWS_ENDPOINT_URL").ok(),
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
            virtual_hosted_style: false,
        }
    }

    /// Creates a GCS configuration using environment variables.
    ///
    /// Reads service account path from `GOOGLE_APPLICATION_CREDENTIALS`.
    #[must_use]
    pub fn gcs_from_env(bucket: &str) -> Self {
        Self::Gcs {
            bucket: bucket.to_string(),
            service_account_path: std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
            service_account_key: None,
        }
    }

    /// Creates an Azure configuration using environment variables.
    ///
    /// Reads credentials from Azure environment variables:
    /// - `AZURE_STORAGE_ACCOUNT`
    /// - `AZURE_STORAGE_ACCESS_KEY` (optional)
    /// - `AZURE_STORAGE_SAS_TOKEN` (optional)
    ///
    /// # Panics
    ///
    /// Panics if `AZURE_STORAGE_ACCOUNT` is not set.
    #[must_use]
    pub fn azure_from_env(container: &str) -> Self {
        Self::Azure {
            container: container.to_string(),
            account: std::env::var("AZURE_STORAGE_ACCOUNT")
                .expect("AZURE_STORAGE_ACCOUNT environment variable required"),
            access_key: std::env::var("AZURE_STORAGE_ACCESS_KEY").ok(),
            sas_token: std::env::var("AZURE_STORAGE_SAS_TOKEN").ok(),
        }
    }

    /// Returns the bucket/container name for this configuration.
    #[must_use]
    pub fn bucket_name(&self) -> &str {
        match self {
            Self::S3 { bucket, .. } => bucket,
            Self::Gcs { bucket, .. } => bucket,
            Self::Azure { container, .. } => container,
        }
    }

    /// Returns a URL-style identifier for this storage location.
    #[must_use]
    pub fn to_url(&self) -> String {
        match self {
            Self::S3 { bucket, .. } => format!("s3://{bucket}"),
            Self::Gcs { bucket, .. } => format!("gs://{bucket}"),
            Self::Azure {
                container, account, ..
            } => format!("az://{account}/{container}"),
        }
    }
}

#[cfg(test)]
mod security_tests {
    use super::*;

    /// Tests for CWE-22 (Path Traversal) prevention in file sandbox.
    mod file_sandbox {
        use super::*;

        #[test]
        fn test_sandbox_disabled_allows_all_paths() {
            let config = FileSandboxConfig::default();
            assert!(!config.enabled);
            // When disabled, all paths are allowed
            assert!(config.validate_path("/tmp/test").is_ok());
        }

        #[test]
        fn test_sandbox_enabled_with_no_paths_rejects() {
            let config = FileSandboxConfig {
                enabled: true,
                allowed_paths: vec![],
            };
            let result = config.validate_path("/tmp/test");
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("no allowed paths configured"));
        }

        #[test]
        fn test_sandbox_rejects_outside_path() {
            let config = FileSandboxConfig {
                enabled: true,
                allowed_paths: vec![PathBuf::from("/var/lib/uni")],
            };
            let result = config.validate_path("/etc/passwd");
            assert!(result.is_err());
            assert!(result.unwrap_err().contains("outside allowed sandbox"));
        }

        #[test]
        fn test_is_potentially_insecure() {
            // Disabled is insecure
            let disabled = FileSandboxConfig::default();
            assert!(disabled.is_potentially_insecure());

            // Enabled with no paths is insecure
            let no_paths = FileSandboxConfig {
                enabled: true,
                allowed_paths: vec![],
            };
            assert!(no_paths.is_potentially_insecure());

            // Enabled with paths is secure
            let secure = FileSandboxConfig::sandboxed(vec![PathBuf::from("/data")]);
            assert!(!secure.is_potentially_insecure());
        }

        #[test]
        fn test_security_warning_when_disabled() {
            let disabled = FileSandboxConfig::default();
            assert!(disabled.security_warning().is_some());

            let enabled = FileSandboxConfig::sandboxed(vec![PathBuf::from("/data")]);
            assert!(enabled.security_warning().is_none());
        }

        #[test]
        fn test_deployment_mode_defaults() {
            let embedded = FileSandboxConfig::default_for_mode(DeploymentMode::Embedded);
            assert!(!embedded.enabled);

            let server = FileSandboxConfig::default_for_mode(DeploymentMode::Server);
            assert!(server.enabled);
            assert!(!server.allowed_paths.is_empty());
        }
    }
}
