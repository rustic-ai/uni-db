// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct CompactionConfig {
    /// Enable background compaction (default: true)
    pub enabled: bool,

    /// Max L1 runs before triggering compaction (default: 4)
    pub max_l1_runs: usize,

    /// Max L1 size in bytes before compaction (default: 256MB)
    pub max_l1_size_bytes: u64,

    /// Max age of oldest L1 run before compaction (default: 1 hour)
    pub max_l1_age: Duration,

    /// Background check interval (default: 30s)
    pub check_interval: Duration,

    /// Number of compaction worker threads (default: 1)
    pub worker_threads: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_l1_runs: 4,
            max_l1_size_bytes: 256 * 1024 * 1024,
            max_l1_age: Duration::from_secs(3600),
            check_interval: Duration::from_secs(30),
            worker_threads: 1,
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
}

impl Default for IndexRebuildConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay: Duration::from_secs(60),
            worker_check_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WriteThrottleConfig {
    /// L1 run count to start throttling (default: 8)
    pub soft_limit: usize,

    /// L1 run count to stop writes entirely (default: 16)
    pub hard_limit: usize,

    /// Base delay when throttling (default: 10ms)
    pub base_delay: Duration,
}

impl Default for WriteThrottleConfig {
    fn default() -> Self {
        Self {
            soft_limit: 8,
            hard_limit: 16,
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
        if self.allowed_origins.contains(&"*".to_string()) && self.api_key.is_none() {
            Some(
                "Server config has permissive CORS (allow all origins) and no API key. \
                 This is insecure for production deployments.",
            )
        } else if self.allowed_origins.contains(&"*".to_string()) {
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

    /// Minimum mutations required before time-based flush triggers (default: 1).
    /// Prevents unnecessary flushes when there's minimal activity.
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

    /// Default maximum memory per query (default: 1GB)
    pub max_query_memory: usize,

    /// Object store resilience configuration
    pub object_store: ObjectStoreConfig,

    /// Background index rebuild configuration
    pub index_rebuild: IndexRebuildConfig,
}

impl Default for UniConfig {
    fn default() -> Self {
        let parallelism = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

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
            max_query_memory: 1024 * 1024 * 1024, // 1GB
            object_store: ObjectStoreConfig::default(),
            index_rebuild: IndexRebuildConfig::default(),
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
