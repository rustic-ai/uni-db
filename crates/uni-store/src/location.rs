// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Where a store lives, resolved once.
//!
//! A [`StoreLocation`] is built from the URI a database is opened with, an
//! optional typed [`CloudStorageConfig`], raw storage options, and the
//! environment. It owns the one option map that configures **both** object
//! store clients — uni's own (`object_store` 0.13, for the catalog, WAL and
//! ID allocator) and Lance's (for tables and indexes) — and a store handle
//! rooted at the store's root `R`, never at the bucket.
//!
//! Every store kind uses the same layout under `R`:
//!
//! ```text
//! R/catalog/            schema, Locy rules, fork registry, layout marker
//! R/wal/                primary WAL           (hybrid: local directory)
//! R/id_allocator.json                         (hybrid: local directory)
//! R/storage/            tables, indexes, snapshot manifests, fork WALs
//! ```
//!
//! Remote stores written before this layout keep their tables directly under
//! `R` ([`StoreLayout::Flat`]); they are recognized, not moved.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::prefix::PrefixStore;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use uni_common::CloudStorageConfig;
use uni_common::config::ObjectStoreConfig;

/// Path of the layout marker, relative to the store root.
pub const LAYOUT_MARKER_PATH: &str = "catalog/layout.json";

/// Storage option naming the directory a `file://` location treats as its
/// bucket root. Only consulted for legacy-layout detection; meant for tests
/// that reproduce an object-store bucket on the local filesystem.
#[doc(hidden)]
pub const FILE_BUCKET_ROOT_OPTION: &str = "uni_file_bucket_root";

/// Directory, under the store root, holding tables in [`StoreLayout::Standard`].
pub const TABLES_DIR: &str = "storage";

/// Source of environment variables. Lets resolution be tested without
/// mutating the process environment.
pub trait EnvSource: Send + Sync {
    /// Every variable, as `(name, value)`.
    fn vars(&self) -> Vec<(String, String)>;

    /// One variable.
    fn get(&self, name: &str) -> Option<String> {
        self.vars()
            .into_iter()
            .find_map(|(k, v)| (k == name).then_some(v))
    }
}

/// The process environment.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn vars(&self) -> Vec<(String, String)> {
        std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect()
    }

    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

impl EnvSource for BTreeMap<String, String> {
    fn vars(&self) -> Vec<(String, String)> {
        self.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    fn get(&self, name: &str) -> Option<String> {
        BTreeMap::get(self, name).cloned()
    }
}

/// Object-store provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// Amazon S3 and S3-compatible services.
    S3,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
    /// A local directory addressed as `file://`, laid out like an object store.
    File,
}

impl Scheme {
    /// The scheme Lance and `object_store` both understand.
    #[must_use]
    pub fn canonical(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gs",
            Self::Azure => "az",
            Self::File => "file",
        }
    }
}

/// What kind of location a store is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocationKind {
    /// A plain local directory path.
    Local {
        /// The database directory.
        dir: PathBuf,
    },
    /// An object store (or `file://`) location.
    Remote {
        /// Provider.
        scheme: Scheme,
        /// Bucket or container (empty for `file://`).
        bucket: String,
        /// Decoded path under the bucket, without leading or trailing `/`.
        prefix: String,
    },
}

/// How a store's tables are placed under its root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoreLayout {
    /// Tables, indexes and snapshot manifests under `R/storage/`.
    Standard,
    /// Tables directly under `R` — remote stores written before the
    /// standard layout.
    Flat,
}

/// Result of inspecting a store before opening it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayoutProbe {
    /// The store records its layout in [`LAYOUT_MARKER_PATH`].
    Recorded(StoreLayout),
    /// The store has no marker; its layout was inferred from what it holds.
    Inferred(StoreLayout),
    /// A prefixed store written by uni-db ≤ 5.3.0: tables under the prefix,
    /// metadata at the bucket root.
    LegacyPrefixed {
        /// Bucket-root metadata objects found.
        root_files: Vec<String>,
    },
}

/// Inputs to [`StoreLocation::resolve`].
#[derive(Clone, Default)]
pub struct LocationSpec {
    /// The URI or path the store is opened at.
    pub uri: String,
    /// Typed provider configuration.
    pub cloud: Option<CloudStorageConfig>,
    /// Raw `object_store` / Lance storage options; highest precedence.
    pub options: BTreeMap<String, String>,
}

impl std::fmt::Debug for LocationSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocationSpec")
            .field("uri", &self.uri)
            .field("cloud", &self.cloud)
            .field("options", &StorageOptions(self.options.clone()))
            .finish()
    }
}

/// Resolved storage options. `Debug` prints keys and only non-secret values.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct StorageOptions(pub(crate) BTreeMap<String, String>);

impl StorageOptions {
    /// Look up one option by its canonical name.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// All options.
    #[must_use]
    pub fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

impl std::fmt::Debug for StorageOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (k, v) in &self.0 {
            if is_public_option(k) {
                m.entry(k, v);
            } else {
                m.entry(k, &"<redacted>");
            }
        }
        m.finish()
    }
}

/// Options whose values are safe to print.
fn is_public_option(key: &str) -> bool {
    const PUBLIC: &[&str] = &[
        "aws_region",
        "aws_default_region",
        "aws_bucket",
        "aws_endpoint",
        "aws_endpoint_url_s3",
        "aws_virtual_hosted_style_request",
        "aws_s3_express",
        "aws_imdsv1_fallback",
        "aws_metadata_endpoint",
        "aws_role_arn",
        "aws_role_session_name",
        "aws_endpoint_url_sts",
        "aws_skip_signature",
        "aws_conditional_put",
        "aws_copy_if_not_exists",
        "aws_request_payer",
        "aws_web_identity_token_file",
        "aws_server_side_encryption",
        "google_bucket",
        "google_base_url",
        "google_service_account",
        "google_application_credentials",
        "google_skip_signature",
        "azure_storage_account_name",
        "azure_container_name",
        "azure_storage_endpoint",
        "azure_storage_use_emulator",
        "azure_msi_endpoint",
        "azure_storage_client_id",
        "azure_storage_tenant_id",
        "azure_use_azure_cli",
        "azure_skip_signature",
        "allow_http",
        "allow_invalid_certificates",
        "connect_timeout",
        "timeout",
        "user_agent",
        "client_max_retries",
        "client_retry_timeout",
        "download_retry_count",
        "use_opendal",
        FILE_BUCKET_ROOT_OPTION,
    ];
    PUBLIC.contains(&key)
}

/// Joins a relative path onto a store URI or path, with exactly one `/`.
#[must_use]
pub fn join_uri(base: &str, rel: &str) -> String {
    let base = base.trim_end_matches('/');
    let rel = rel.trim_matches('/');
    if rel.is_empty() {
        base.to_string()
    } else {
        format!("{base}/{rel}")
    }
}

const SUPPORTED: &str = "s3://, s3a://, gs://, gcs://, az://, abfss://, file://, or a local path";

/// Canonical `object_store` key for `key` under `scheme`, or `None` when the
/// provider does not know it (it may still be a Lance-only option).
fn canonical_key(scheme: Scheme, key: &str) -> Option<String> {
    use object_store::aws::AmazonS3ConfigKey;
    use object_store::azure::AzureConfigKey;
    use object_store::gcp::GoogleConfigKey;
    let key = key.to_ascii_lowercase();
    match scheme {
        Scheme::S3 => key
            .parse::<AmazonS3ConfigKey>()
            .ok()
            .map(|k| k.as_ref().to_string()),
        Scheme::Gcs => key
            .parse::<GoogleConfigKey>()
            .ok()
            .map(|k| k.as_ref().to_string()),
        Scheme::Azure => key
            .parse::<AzureConfigKey>()
            .ok()
            .map(|k| k.as_ref().to_string()),
        Scheme::File => None,
    }
}

fn env_prefix(scheme: Scheme) -> &'static str {
    match scheme {
        Scheme::S3 => "AWS_",
        Scheme::Gcs => "GOOGLE_",
        Scheme::Azure => "AZURE_",
        Scheme::File => "\0",
    }
}

/// What a URI says, before any option is applied.
struct ParsedUri {
    kind: LocationKind,
    /// Account named by the URI (Azure `abfss://c@acct…`).
    account: Option<String>,
}

fn parse_uri(
    uri: &str,
    cloud: Option<&CloudStorageConfig>,
    known_account: Option<&str>,
) -> Result<ParsedUri> {
    if !uri.contains("://") {
        return Ok(ParsedUri {
            kind: LocationKind::Local {
                dir: PathBuf::from(uri),
            },
            account: None,
        });
    }
    let url = url::Url::parse(uri).map_err(|e| anyhow!("Invalid store URI '{uri}': {e}"))?;
    let decoded_path = || -> Result<String> {
        let path = object_store::path::Path::from_url_path(url.path())
            .map_err(|e| anyhow!("Invalid path in store URI '{uri}': {e}"))?;
        Ok(path.as_ref().trim_matches('/').to_string())
    };
    let host = || -> Result<String> {
        url.host_str()
            .filter(|h| !h.is_empty())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("Store URI '{uri}' names no bucket or container"))
    };
    let (scheme, bucket, prefix, account) = match url.scheme() {
        "s3" | "s3a" => (Scheme::S3, host()?, decoded_path()?, None),
        "gs" | "gcs" => (Scheme::Gcs, host()?, decoded_path()?, None),
        "az" | "azure" => {
            let host = host()?;
            let path = decoded_path()?;
            // `az://account/container/…`, the form uni-db ≤ 5.3.0 documented:
            // recognized when the configured account is the host.
            let account = match cloud {
                Some(CloudStorageConfig::Azure { account, .. }) => Some(account.as_str()),
                _ => known_account,
            };
            match account {
                Some(account) if account == host => {
                    let (container, rest) = path.split_once('/').unwrap_or((path.as_str(), ""));
                    if container.is_empty() {
                        bail!("Store URI '{uri}' names no container");
                    }
                    tracing::warn!(
                        uri,
                        "az://<account>/<container> is deprecated; use az://<container>/<path> \
                         with the account in the configuration"
                    );
                    (
                        Scheme::Azure,
                        container.to_string(),
                        rest.to_string(),
                        Some(host),
                    )
                }
                _ => (Scheme::Azure, host, path, None),
            }
        }
        "abfss" | "abfs" => {
            let user = url.username();
            let host = host()?;
            if user.is_empty() {
                (Scheme::Azure, host, decoded_path()?, None)
            } else {
                let account = host
                    .split('.')
                    .next()
                    .filter(|a| !a.is_empty())
                    .ok_or_else(|| anyhow!("Store URI '{uri}' names no account"))?
                    .to_string();
                (
                    Scheme::Azure,
                    user.to_string(),
                    decoded_path()?,
                    Some(account),
                )
            }
        }
        "file" => {
            let path = url
                .to_file_path()
                .map_err(|()| anyhow!("Invalid file URI '{uri}'"))?;
            let prefix = path
                .to_string_lossy()
                .trim_start_matches('/')
                .trim_end_matches('/')
                .to_string();
            (Scheme::File, String::new(), prefix, None)
        }
        other => bail!("Unsupported store URI scheme '{other}' in '{uri}'. Supported: {SUPPORTED}"),
    };
    Ok(ParsedUri {
        kind: LocationKind::Remote {
            scheme,
            bucket,
            prefix,
        },
        account,
    })
}

/// The option map, built layer by layer.
struct OptionLayers {
    scheme: Scheme,
    map: BTreeMap<String, String>,
    /// Keys set above the environment.
    explicit: std::collections::BTreeSet<String>,
}

impl OptionLayers {
    fn new(scheme: Scheme) -> Self {
        Self {
            scheme,
            map: BTreeMap::new(),
            explicit: Default::default(),
        }
    }

    fn key(&self, key: &str) -> String {
        canonical_key(self.scheme, key).unwrap_or_else(|| key.to_ascii_lowercase())
    }

    fn env(&mut self, env: &dyn EnvSource) {
        let prefix = env_prefix(self.scheme);
        for (name, value) in env.vars() {
            if name.starts_with(prefix)
                && let Some(key) = canonical_key(self.scheme, &name)
            {
                self.map.insert(key, value);
            }
        }
        // Variables `from_env` reads by a name outside the provider prefix.
        match self.scheme {
            Scheme::Gcs => {
                if let Some(v) = env.get("SERVICE_ACCOUNT") {
                    self.map.entry("google_service_account".into()).or_insert(v);
                }
            }
            Scheme::Azure => {
                if let Some(v) = env.get("IDENTITY_ENDPOINT") {
                    self.map.entry("azure_msi_endpoint".into()).or_insert(v);
                }
                // The name the Azure CLI and SDKs use, which object_store's
                // key set lacks.
                if let Some(v) = env.get("AZURE_STORAGE_ACCOUNT") {
                    self.map
                        .entry("azure_storage_account_name".into())
                        .or_insert(v);
                }
            }
            Scheme::S3 | Scheme::File => {}
        }
    }

    fn set(&mut self, key: &str, value: impl Into<String>) {
        let key = self.key(key);
        self.map.insert(key.clone(), value.into());
        self.explicit.insert(key);
    }

    fn set_opt(&mut self, key: &str, value: Option<&String>) {
        if let Some(v) = value {
            self.set(key, v.clone());
        }
    }

    fn is_explicit(&self, key: &str) -> bool {
        self.explicit.contains(&self.key(key))
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.map.get(&self.key(key))
    }
}

fn duration_option(d: std::time::Duration) -> String {
    if d.subsec_millis() == 0 {
        format!("{}s", d.as_secs())
    } else {
        format!("{}ms", d.as_millis())
    }
}

/// Builds the option map for an object-store location.
async fn resolve_options(
    scheme: Scheme,
    bucket: &str,
    url_account: Option<&str>,
    spec: &LocationSpec,
    env: &dyn EnvSource,
    net: &ObjectStoreConfig,
) -> Result<BTreeMap<String, String>> {
    let mut o = OptionLayers::new(scheme);
    o.env(env);

    // URL layer.
    match scheme {
        Scheme::S3 => o.set("aws_bucket", bucket),
        Scheme::Gcs => o.set("google_bucket", bucket),
        Scheme::Azure => {
            o.set("azure_container_name", bucket);
            if let Some(a) = url_account {
                o.set("azure_storage_account_name", a);
            }
        }
        Scheme::File => {}
    }

    // Typed config layer.
    if let Some(cfg) = &spec.cloud {
        let cfg_bucket = cfg.bucket_name();
        let matches = matches!(
            (cfg, scheme),
            (CloudStorageConfig::S3 { .. }, Scheme::S3)
                | (CloudStorageConfig::Gcs { .. }, Scheme::Gcs)
                | (CloudStorageConfig::Azure { .. }, Scheme::Azure)
        );
        if !matches {
            bail!(
                "The cloud storage configuration is for {} but the store URI '{}' is not",
                cfg.to_url(),
                spec.uri
            );
        }
        if cfg_bucket != bucket {
            bail!(
                "The store URI '{}' names bucket '{bucket}' but the cloud storage \
                 configuration names bucket '{cfg_bucket}'",
                spec.uri
            );
        }
        match cfg {
            CloudStorageConfig::S3 {
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                session_token,
                virtual_hosted_style,
                ..
            } => {
                o.set_opt("aws_region", region.as_ref());
                o.set_opt("aws_endpoint", endpoint.as_ref());
                o.set_opt("aws_access_key_id", access_key_id.as_ref());
                o.set_opt("aws_secret_access_key", secret_access_key.as_ref());
                o.set_opt("aws_session_token", session_token.as_ref());
                if *virtual_hosted_style {
                    o.set("aws_virtual_hosted_style_request", "true");
                }
            }
            CloudStorageConfig::Gcs {
                service_account_path,
                service_account_key,
                ..
            } => {
                o.set_opt("google_service_account", service_account_path.as_ref());
                o.set_opt("google_service_account_key", service_account_key.as_ref());
            }
            CloudStorageConfig::Azure {
                account,
                access_key,
                sas_token,
                ..
            } => {
                if let Some(url_account) = url_account
                    && url_account != account
                {
                    bail!(
                        "The store URI '{}' names account '{url_account}' but the cloud \
                         storage configuration names account '{account}'",
                        spec.uri
                    );
                }
                o.set("azure_storage_account_name", account.clone());
                o.set_opt("azure_storage_account_key", access_key.as_ref());
                o.set_opt("azure_storage_sas_key", sas_token.as_ref());
            }
        }
    }

    // Raw options layer.
    for (k, v) in &spec.options {
        if k == FILE_BUCKET_ROOT_OPTION {
            continue;
        }
        o.set(k, v.clone());
    }

    if scheme == Scheme::S3 {
        // An endpoint chosen above the environment beats an inherited
        // `AWS_ENDPOINT_URL_S3`, which both clients would otherwise prefer.
        if o.is_explicit("aws_endpoint") && !o.is_explicit("aws_endpoint_url_s3") {
            let ep = o.get("aws_endpoint").cloned().unwrap_or_default();
            o.map.insert("aws_endpoint_url_s3".into(), ep);
        }
        let endpoint = o
            .get("aws_endpoint_url_s3")
            .or_else(|| o.get("aws_endpoint"))
            .cloned();
        if o.get("aws_region").is_none() {
            let region = match (o.get("aws_default_region").cloned(), &endpoint) {
                (Some(r), _) => r,
                // A custom endpoint (MinIO, R2, LocalStack): Lance would
                // otherwise probe the instance metadata service for a region.
                (None, Some(_)) => "us-east-1".to_string(),
                (None, None) => {
                    let mut opts = object_store::ClientOptions::new();
                    for (k, v) in &o.map {
                        if let Ok(key) = k.parse::<object_store::ClientConfigKey>() {
                            opts = opts.with_config(key, v);
                        }
                    }
                    object_store::aws::resolve_bucket_region(bucket, &opts)
                        .await
                        .map_err(|e| {
                            anyhow!(
                                "Could not determine the region of bucket '{bucket}' ({e}). \
                                 Set AWS_REGION, the `aws_region` storage option, or the \
                                 region in the cloud storage configuration."
                            )
                        })?
                }
            };
            o.map.insert("aws_region".into(), region);
        }
    }

    // A plain-HTTP endpoint needs the HTTP allowance, unless it was decided
    // explicitly.
    if o.get("allow_http").is_none() {
        let endpoint_key = match scheme {
            Scheme::S3 => o
                .get("aws_endpoint_url_s3")
                .or_else(|| o.get("aws_endpoint")),
            Scheme::Gcs => o.get("google_base_url"),
            Scheme::Azure => o.get("azure_storage_endpoint"),
            Scheme::File => None,
        };
        if endpoint_key.is_some_and(|e| e.starts_with("http://")) {
            o.map.insert("allow_http".into(), "true".into());
        }
    }

    // Network settings, unless chosen explicitly.
    o.map
        .entry("connect_timeout".into())
        .or_insert_with(|| duration_option(net.connect_timeout));
    o.map
        .entry("timeout".into())
        .or_insert_with(|| duration_option(net.read_timeout.max(net.write_timeout)));

    Ok(o.map)
}

fn retry_config(net: &ObjectStoreConfig) -> object_store::RetryConfig {
    object_store::RetryConfig {
        backoff: object_store::BackoffConfig {
            init_backoff: net.retry_backoff_base,
            max_backoff: net.retry_backoff_max,
            base: 2.0,
        },
        max_retries: net.max_retries as usize,
        ..Default::default()
    }
}

/// uni's own client for the bucket, from the resolved options. Options the
/// provider does not know are left for Lance.
fn build_bucket_store(
    scheme: Scheme,
    options: &BTreeMap<String, String>,
    net: &ObjectStoreConfig,
) -> Result<Arc<dyn ObjectStore>> {
    use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
    use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
    use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
    let lance_only = |k: &str| tracing::debug!(key = k, "storage option passed to Lance only");
    Ok(match scheme {
        Scheme::S3 => {
            let mut b = AmazonS3Builder::new();
            for (k, v) in options {
                match k.parse::<AmazonS3ConfigKey>() {
                    Ok(key) => b = b.with_config(key, v),
                    Err(_) => lance_only(k),
                }
            }
            Arc::new(b.with_retry(retry_config(net)).build()?)
        }
        Scheme::Gcs => {
            let mut b = GoogleCloudStorageBuilder::new();
            for (k, v) in options {
                match k.parse::<GoogleConfigKey>() {
                    Ok(key) => b = b.with_config(key, v),
                    Err(_) => lance_only(k),
                }
            }
            Arc::new(b.with_retry(retry_config(net)).build()?)
        }
        Scheme::Azure => {
            let mut b = MicrosoftAzureBuilder::new();
            for (k, v) in options {
                match k.parse::<AzureConfigKey>() {
                    Ok(key) => b = b.with_config(key, v),
                    Err(_) => lance_only(k),
                }
            }
            Arc::new(b.with_retry(retry_config(net)).build()?)
        }
        Scheme::File => bail!("a file:// location has no bucket store"),
    })
}

fn prefixed(store: Arc<dyn ObjectStore>, prefix: &str) -> Arc<dyn ObjectStore> {
    if prefix.is_empty() {
        store
    } else {
        Arc::new(PrefixStore::new(store, prefix))
    }
}

/// A resolved store location. See the module docs.
#[derive(Clone)]
pub struct StoreLocation {
    kind: LocationKind,
    root_uri: String,
    options: StorageOptions,
    meta: Arc<dyn ObjectStore>,
    bucket_root: Option<Arc<dyn ObjectStore>>,
    fs_root: Option<PathBuf>,
}

impl std::fmt::Debug for StoreLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreLocation")
            .field("kind", &self.kind)
            .field("root_uri", &self.root_uri)
            .field("options", &self.options)
            .field("fs_root", &self.fs_root)
            .finish_non_exhaustive()
    }
}

impl StoreLocation {
    /// Resolves a location. A plain local directory must already exist.
    ///
    /// # Errors
    /// An unsupported scheme, a bucket that disagrees with the typed config,
    /// or a store that cannot be built.
    pub async fn resolve(
        spec: &LocationSpec,
        env: &dyn EnvSource,
        net: &ObjectStoreConfig,
    ) -> Result<Self> {
        let uri = spec.uri.trim();
        // The account the configuration names, to recognize the legacy
        // `az://account/container` form.
        let known_account = ["azure_storage_account_name", "account_name"]
            .iter()
            .find_map(|k| spec.options.get(*k).cloned())
            .or_else(|| env.get("AZURE_STORAGE_ACCOUNT_NAME"))
            .or_else(|| env.get("AZURE_STORAGE_ACCOUNT"));
        let parsed = parse_uri(uri, spec.cloud.as_ref(), known_account.as_deref())?;
        match parsed.kind {
            LocationKind::Local { dir } => {
                if spec.cloud.is_some() {
                    bail!(
                        "A cloud storage configuration was given for the local path '{uri}'. \
                         Open an object-store URI, or use hybrid mode for data in an object \
                         store."
                    );
                }
                let meta: Arc<dyn ObjectStore> = Arc::new(
                    LocalFileSystem::new_with_prefix(&dir)
                        .map_err(|e| anyhow!("Open local store at {}: {e}", dir.display()))?,
                );
                Ok(Self {
                    root_uri: dir.display().to_string(),
                    kind: LocationKind::Local { dir: dir.clone() },
                    options: StorageOptions::default(),
                    meta,
                    bucket_root: None,
                    fs_root: Some(dir),
                })
            }
            LocationKind::Remote {
                scheme: Scheme::File,
                prefix,
                ..
            } => {
                if spec.cloud.is_some() {
                    bail!("A cloud storage configuration was given for the file URI '{uri}'");
                }
                let path = PathBuf::from(format!("/{prefix}"));
                let (bucket_store, rel, bucket_root): (Arc<dyn ObjectStore>, String, bool) =
                    match spec.options.get(FILE_BUCKET_ROOT_OPTION) {
                        Some(root) => {
                            let root = PathBuf::from(root);
                            let rel = path
                                .strip_prefix(&root)
                                .map_err(|_| {
                                    anyhow!(
                                        "{FILE_BUCKET_ROOT_OPTION} {} does not contain {}",
                                        root.display(),
                                        path.display()
                                    )
                                })?
                                .to_string_lossy()
                                .trim_matches('/')
                                .to_string();
                            std::fs::create_dir_all(&root)?;
                            (
                                Arc::new(LocalFileSystem::new_with_prefix(&root)?),
                                rel,
                                true,
                            )
                        }
                        None => (Arc::new(LocalFileSystem::new()), prefix, false),
                    };
                let root_uri = url::Url::from_file_path(&path)
                    .map_err(|()| anyhow!("Invalid file URI '{uri}'"))?
                    .to_string()
                    .trim_end_matches('/')
                    .to_string();
                Ok(Self {
                    kind: LocationKind::Remote {
                        scheme: Scheme::File,
                        bucket: String::new(),
                        prefix: rel.clone(),
                    },
                    root_uri,
                    options: StorageOptions::default(),
                    meta: prefixed(Arc::clone(&bucket_store), &rel),
                    bucket_root: bucket_root.then_some(bucket_store),
                    fs_root: Some(path),
                })
            }
            LocationKind::Remote {
                scheme,
                bucket,
                prefix,
            } => {
                let mut options =
                    resolve_options(scheme, &bucket, parsed.account.as_deref(), spec, env, net)
                        .await?;
                options
                    .entry("client_max_retries".into())
                    .or_insert_with(|| net.max_retries.to_string());
                let bucket_store = build_bucket_store(scheme, &options, net)
                    .map_err(|e| anyhow!("Build the object store client for '{uri}': {e:#}"))?;
                let mut url = url::Url::parse(&format!("{}://{bucket}/", scheme.canonical()))
                    .map_err(|e| anyhow!("Invalid store URI '{uri}': {e}"))?;
                url.set_path(&prefix);
                let root_uri = url.to_string().trim_end_matches('/').to_string();
                Ok(Self {
                    kind: LocationKind::Remote {
                        scheme,
                        bucket,
                        prefix: prefix.clone(),
                    },
                    root_uri,
                    options: StorageOptions(options),
                    meta: prefixed(Arc::clone(&bucket_store), &prefix),
                    bucket_root: Some(bucket_store),
                    fs_root: None,
                })
            }
        }
    }

    /// [`resolve`](Self::resolve) against the process environment.
    ///
    /// # Errors
    /// See [`resolve`](Self::resolve).
    pub async fn resolve_from_env(spec: &LocationSpec, net: &ObjectStoreConfig) -> Result<Self> {
        Self::resolve(spec, &ProcessEnv, net).await
    }

    /// What kind of location this is.
    #[must_use]
    pub fn kind(&self) -> &LocationKind {
        &self.kind
    }

    /// Whether the location is an object store reached over the network.
    #[must_use]
    pub fn is_remote(&self) -> bool {
        matches!(&self.kind, LocationKind::Remote { scheme, .. } if *scheme != Scheme::File)
    }

    /// The decoded prefix under the bucket; empty for a bucket root or a
    /// local directory.
    #[must_use]
    pub fn prefix(&self) -> &str {
        match &self.kind {
            LocationKind::Remote { prefix, .. } => prefix,
            LocationKind::Local { .. } => "",
        }
    }

    /// The store root `R` as a URI (or path for a local directory), without
    /// a trailing `/`.
    #[must_use]
    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }

    /// The resolved option map.
    #[must_use]
    pub fn options(&self) -> &StorageOptions {
        &self.options
    }

    /// uni's store handle rooted at `R`.
    #[must_use]
    pub fn meta_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.meta)
    }

    /// A handle rooted at the bucket (remote and `file://` only).
    #[must_use]
    pub fn bucket_root(&self) -> Option<Arc<dyn ObjectStore>> {
        self.bucket_root.clone()
    }

    /// The local directory behind `R`, when there is one (plain paths and
    /// `file://`). Enables fsync and the compaction lock.
    #[must_use]
    pub fn fs_root(&self) -> Option<&std::path::Path> {
        self.fs_root.as_deref()
    }

    /// URI (or path) Lance opens tables under.
    #[must_use]
    pub fn table_uri(&self, layout: StoreLayout) -> String {
        match (&self.kind, layout) {
            (LocationKind::Local { dir }, _) => dir.join(TABLES_DIR).display().to_string(),
            (_, StoreLayout::Standard) => join_uri(&self.root_uri, TABLES_DIR),
            (_, StoreLayout::Flat) => self.root_uri.clone(),
        }
    }

    /// uni's store handle rooted at the table root.
    ///
    /// # Errors
    /// A local table directory that cannot be created.
    pub fn table_store(&self, layout: StoreLayout) -> Result<Arc<dyn ObjectStore>> {
        match (&self.kind, layout) {
            (LocationKind::Local { dir }, _) => {
                let tables = dir.join(TABLES_DIR);
                std::fs::create_dir_all(&tables)?;
                Ok(Arc::new(LocalFileSystem::new_with_prefix(&tables)?))
            }
            (_, layout) => {
                // A `file://` table root is a directory the lock and fsync
                // paths open files in directly.
                if let Some(dir) = self.table_fs_root(layout) {
                    std::fs::create_dir_all(dir)?;
                }
                Ok(match layout {
                    StoreLayout::Standard => prefixed(self.meta_store(), TABLES_DIR),
                    StoreLayout::Flat => self.meta_store(),
                })
            }
        }
    }

    /// The local directory behind the table root, when there is one.
    #[must_use]
    pub fn table_fs_root(&self, layout: StoreLayout) -> Option<PathBuf> {
        let root = self.fs_root.as_ref()?;
        Some(match (&self.kind, layout) {
            (LocationKind::Local { .. }, _) | (_, StoreLayout::Standard) => root.join(TABLES_DIR),
            (_, StoreLayout::Flat) => root.clone(),
        })
    }

    /// Storage options for Lance: the same map uni's client was built from,
    /// plus Lance's own retry knob. `None` for local directories.
    #[must_use]
    pub fn lance_storage_options(&self) -> Option<HashMap<String, String>> {
        self.is_remote().then(|| {
            self.options
                .0
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
    }

    /// Inspects the store to decide its layout. Writes nothing.
    ///
    /// # Errors
    /// The store cannot be listed or read.
    pub async fn probe_layout(&self) -> Result<LayoutProbe> {
        if let Some(bytes) = get_optional(&self.meta, LAYOUT_MARKER_PATH).await? {
            let marker: LayoutMarker = serde_json::from_slice(&bytes)
                .map_err(|e| anyhow!("Invalid {LAYOUT_MARKER_PATH} in {}: {e}", self.root_uri))?;
            return Ok(LayoutProbe::Recorded(marker.layout));
        }
        let top = self
            .meta
            .list_with_delimiter(None)
            .await
            .map_err(|e| anyhow!("List the store at {}: {e}", self.root_uri))?;
        let dirs: Vec<String> = top
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().map(str::to_string))
            .collect();
        let tables_at_root = dirs.iter().any(|d| d.ends_with(".lance"));
        let has_storage = dirs.iter().any(|d| d == TABLES_DIR);
        let catalog = self
            .meta
            .list_with_delimiter(Some(&ObjPath::from("catalog")))
            .await
            .map(|r| !r.objects.is_empty() || !r.common_prefixes.is_empty())
            .or_else(|e| match e {
                object_store::Error::NotFound { .. } => Ok(false),
                e => Err(anyhow!("List the catalog at {}: {e}", self.root_uri)),
            })?;

        if !self.prefix().is_empty()
            && !catalog
            && tables_at_root
            && let Some(bucket) = &self.bucket_root
        {
            let mut root_files = Vec::new();
            for f in ["catalog/latest", "catalog/schema.json"] {
                if get_optional(bucket, f).await?.is_some() {
                    root_files.push(f.to_string());
                }
            }
            if !root_files.is_empty() {
                return Ok(LayoutProbe::LegacyPrefixed { root_files });
            }
        }
        if has_storage {
            return Ok(LayoutProbe::Inferred(StoreLayout::Standard));
        }
        let root_latest = get_optional(&self.meta, "catalog/latest").await?.is_some();
        if tables_at_root || root_latest {
            return Ok(LayoutProbe::Inferred(StoreLayout::Flat));
        }
        Ok(LayoutProbe::Inferred(StoreLayout::Standard))
    }

    /// Records `layout` in [`LAYOUT_MARKER_PATH`] if no marker exists.
    ///
    /// # Errors
    /// The marker cannot be written.
    pub async fn write_layout_marker(&self, layout: StoreLayout) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&LayoutMarker { version: 2, layout })?;
        let path = ObjPath::from(LAYOUT_MARKER_PATH);
        let opts: PutOptions = PutMode::Create.into();
        match self.meta.put_opts(&path, bytes.clone().into(), opts).await {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Err(object_store::Error::NotImplemented { .. })
            | Err(object_store::Error::NotSupported { .. }) => {
                if get_optional(&self.meta, LAYOUT_MARKER_PATH)
                    .await?
                    .is_none()
                {
                    self.meta.put(&path, bytes.into()).await?;
                }
                Ok(())
            }
            Err(e) => Err(anyhow!(
                "Write {LAYOUT_MARKER_PATH} in {}: {e}",
                self.root_uri
            )),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LayoutMarker {
    version: u32,
    layout: StoreLayout,
}

/// The object at `path`, or `None` when there is none.
async fn get_optional(store: &Arc<dyn ObjectStore>, path: &str) -> Result<Option<bytes::Bytes>> {
    match store.get(&ObjPath::from(path)).await {
        Ok(r) => Ok(Some(r.bytes().await?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow!("Read {path}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    fn net() -> ObjectStoreConfig {
        ObjectStoreConfig::default()
    }

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn spec(uri: &str) -> LocationSpec {
        LocationSpec {
            uri: uri.to_string(),
            ..Default::default()
        }
    }

    /// Static keys and a region keep resolution off the network.
    fn quiet_env() -> BTreeMap<String, String> {
        env(&[
            ("AWS_ACCESS_KEY_ID", "AKIDENV"),
            ("AWS_SECRET_ACCESS_KEY", "envsecret"),
            ("AWS_REGION", "eu-west-1"),
        ])
    }

    async fn resolve(spec: &LocationSpec, env: &BTreeMap<String, String>) -> StoreLocation {
        StoreLocation::resolve(spec, env, &net())
            .await
            .unwrap_or_else(|e| panic!("resolve {:?}: {e:#}", spec.uri))
    }

    fn remote(loc: &StoreLocation) -> (Scheme, String, String) {
        match loc.kind() {
            LocationKind::Remote {
                scheme,
                bucket,
                prefix,
            } => (*scheme, bucket.clone(), prefix.clone()),
            other => panic!("expected remote, got {other:?}"),
        }
    }

    // ---- L1: parsing -----------------------------------------------------

    #[tokio::test]
    async fn s3_uri_forms_parse_to_bucket_and_prefix() {
        let e = quiet_env();
        for (uri, bucket, prefix, root) in [
            ("s3://b", "b", "", "s3://b"),
            ("s3://b/", "b", "", "s3://b"),
            ("s3://b/p", "b", "p", "s3://b/p"),
            ("s3://b/p/q/", "b", "p/q", "s3://b/p/q"),
            ("s3a://b/p", "b", "p", "s3://b/p"),
        ] {
            let loc = resolve(&spec(uri), &e).await;
            assert_eq!(
                remote(&loc),
                (Scheme::S3, bucket.to_string(), prefix.to_string()),
                "{uri}"
            );
            assert_eq!(loc.root_uri(), root, "{uri}");
            assert!(loc.is_remote());
        }
    }

    #[tokio::test]
    async fn gcs_and_azure_uris_parse() {
        let e = env(&[("AZURE_STORAGE_ACCOUNT_NAME", "acct")]);
        let loc = resolve(&spec("gs://g/p"), &e).await;
        assert_eq!(remote(&loc), (Scheme::Gcs, "g".into(), "p".into()));
        let loc = resolve(&spec("gcs://g/p"), &e).await;
        assert_eq!(remote(&loc), (Scheme::Gcs, "g".into(), "p".into()));
        assert_eq!(loc.root_uri(), "gs://g/p");

        let loc = resolve(&spec("az://cont/p"), &e).await;
        assert_eq!(remote(&loc), (Scheme::Azure, "cont".into(), "p".into()));
        assert_eq!(
            loc.options().get("azure_storage_account_name"),
            Some("acct")
        );

        let loc = resolve(&spec("abfss://cont@acct2.dfs.core.windows.net/p/q"), &e).await;
        assert_eq!(remote(&loc), (Scheme::Azure, "cont".into(), "p/q".into()));
        assert_eq!(
            loc.options().get("azure_storage_account_name"),
            Some("acct2")
        );
        assert_eq!(loc.root_uri(), "az://cont/p/q");
    }

    #[tokio::test]
    async fn legacy_azure_account_container_form_is_accepted() {
        let cfg = CloudStorageConfig::Azure {
            container: "cont".into(),
            account: "acct".into(),
            access_key: None,
            sas_token: None,
        };
        let s = LocationSpec {
            uri: "az://acct/cont/p".into(),
            cloud: Some(cfg),
            ..Default::default()
        };
        let loc = resolve(&s, &env(&[])).await;
        assert_eq!(remote(&loc), (Scheme::Azure, "cont".into(), "p".into()));
    }

    #[tokio::test]
    async fn percent_encoded_prefix_is_decoded_once() {
        let loc = resolve(&spec("s3://b/a%20b/c"), &quiet_env()).await;
        assert_eq!(remote(&loc).2, "a b/c");
    }

    #[tokio::test]
    async fn file_uri_is_a_remote_style_local_location() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/store", dir.path().display());
        let loc = resolve(&spec(&uri), &env(&[])).await;
        let (scheme, _, prefix) = remote(&loc);
        assert_eq!(scheme, Scheme::File);
        assert!(prefix.ends_with("store"), "{prefix}");
        assert!(!loc.is_remote());
        assert_eq!(loc.fs_root(), Some(dir.path().join("store").as_path()));
        assert!(loc.lance_storage_options().is_none());
    }

    #[tokio::test]
    async fn plain_path_is_local() {
        let dir = tempfile::tempdir().unwrap();
        let loc = resolve(&spec(&dir.path().display().to_string()), &env(&[])).await;
        assert_eq!(
            loc.kind(),
            &LocationKind::Local {
                dir: dir.path().to_path_buf()
            }
        );
        assert_eq!(loc.fs_root(), Some(dir.path()));
        assert_eq!(
            loc.table_uri(StoreLayout::Standard),
            dir.path().join("storage").display().to_string()
        );
    }

    #[tokio::test]
    async fn unsupported_schemes_are_rejected() {
        for uri in ["memory://x", "http://host/x", "ftp://h/x", "adl://c/p"] {
            let err = StoreLocation::resolve(&spec(uri), &env(&[]), &net())
                .await
                .expect_err(uri);
            let msg = format!("{err:#}");
            assert!(
                msg.contains("Unsupported") && msg.contains("s3://"),
                "{uri}: {msg}"
            );
        }
    }

    // ---- L2: precedence --------------------------------------------------

    #[tokio::test]
    async fn precedence_env_url_config_options() {
        let e = env(&[
            ("AWS_REGION", "env-region"),
            ("AWS_ENDPOINT_URL", "http://env:1"),
            ("AWS_ACCESS_KEY_ID", "AKIDENV"),
            ("AWS_SECRET_ACCESS_KEY", "envsecret"),
            ("AWS_BUCKET", "envbucket"),
        ]);
        // Env alone.
        let loc = resolve(&spec("s3://urlbucket/p"), &e).await;
        let o = loc.options();
        assert_eq!(o.get("aws_region"), Some("env-region"));
        assert_eq!(o.get("aws_endpoint"), Some("http://env:1"));
        assert_eq!(o.get("aws_bucket"), Some("urlbucket"), "URL beats env");

        // Typed config beats env; a None field leaves env in place.
        let cfg = CloudStorageConfig::S3 {
            bucket: "urlbucket".into(),
            region: Some("cfg-region".into()),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_hosted_style: false,
        };
        let mut s = spec("s3://urlbucket/p");
        s.cloud = Some(cfg);
        let loc = resolve(&s, &e).await;
        assert_eq!(loc.options().get("aws_region"), Some("cfg-region"));
        assert_eq!(loc.options().get("aws_endpoint"), Some("http://env:1"));
        assert_eq!(loc.options().get("aws_access_key_id"), Some("AKIDENV"));

        // Raw options beat everything, and accept aliases.
        s.options = BTreeMap::from([("region".to_string(), "opt-region".to_string())]);
        let loc = resolve(&s, &e).await;
        assert_eq!(loc.options().get("aws_region"), Some("opt-region"));
    }

    #[tokio::test]
    async fn bucket_mismatch_between_url_and_config_errors() {
        let cfg = CloudStorageConfig::S3 {
            bucket: "other".into(),
            region: Some("r".into()),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_hosted_style: false,
        };
        let s = LocationSpec {
            uri: "s3://b/p".into(),
            cloud: Some(cfg),
            ..Default::default()
        };
        let err = StoreLocation::resolve(&s, &quiet_env(), &net())
            .await
            .expect_err("mismatch");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("names bucket 'b'") && msg.contains("names bucket 'other'"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn allow_http_is_derived_from_an_http_endpoint_unless_set() {
        let mut s = spec("s3://b");
        s.options = BTreeMap::from([("aws_endpoint".into(), "http://localhost:4566".into())]);
        let loc = resolve(&s, &quiet_env()).await;
        assert_eq!(loc.options().get("allow_http"), Some("true"));

        s.options.insert("allow_http".into(), "false".into());
        let loc = resolve(&s, &quiet_env()).await;
        assert_eq!(loc.options().get("allow_http"), Some("false"));

        s.options = BTreeMap::from([("aws_endpoint".into(), "https://s3.example".into())]);
        let loc = resolve(&s, &quiet_env()).await;
        assert_eq!(loc.options().get("allow_http"), None);
    }

    #[tokio::test]
    async fn explicit_endpoint_also_overrides_the_s3_specific_env_endpoint() {
        let mut e = quiet_env();
        e.insert("AWS_ENDPOINT_URL_S3".into(), "http://env-s3:1".into());
        // Env alone: the S3-specific variable wins, as the AWS SDKs do.
        let loc = resolve(&spec("s3://b"), &e).await;
        assert_eq!(
            loc.options().get("aws_endpoint_url_s3"),
            Some("http://env-s3:1")
        );
        // An endpoint set above env replaces both keys.
        let mut s = spec("s3://b");
        s.options = BTreeMap::from([("endpoint".into(), "http://opt:2".into())]);
        let loc = resolve(&s, &e).await;
        assert_eq!(loc.options().get("aws_endpoint"), Some("http://opt:2"));
        assert_eq!(
            loc.options().get("aws_endpoint_url_s3"),
            Some("http://opt:2")
        );
    }

    #[tokio::test]
    async fn region_defaults_when_an_endpoint_is_set() {
        let mut s = spec("s3://b");
        s.options = BTreeMap::from([("aws_endpoint".into(), "http://minio:9000".into())]);
        let loc = resolve(&s, &env(&[])).await;
        assert_eq!(loc.options().get("aws_region"), Some("us-east-1"));
    }

    #[tokio::test]
    async fn explicit_credentials_keep_the_environment_session_token_for_both_clients() {
        // Lance merges any environment key the map lacks, so dropping an
        // inherited token on uni's side alone would split the two clients:
        // the catalog would sign without it and the tables with it.
        let mut e = quiet_env();
        e.insert("AWS_SESSION_TOKEN".into(), "env-token".into());
        let mut s = spec("s3://b");
        s.options = BTreeMap::from([
            ("aws_access_key_id".into(), "AKIDOPT".into()),
            ("aws_secret_access_key".into(), "optsecret".into()),
        ]);
        let loc = resolve(&s, &e).await;
        assert_eq!(loc.options().get("aws_access_key_id"), Some("AKIDOPT"));
        assert_eq!(loc.options().get("aws_session_token"), Some("env-token"));
        let lance = loc.lance_storage_options().unwrap();
        assert_eq!(
            lance.get("aws_session_token").map(String::as_str),
            Some("env-token")
        );
    }

    #[tokio::test]
    async fn azure_storage_account_names_the_account() {
        let loc = resolve(
            &spec("az://cont/p"),
            &env(&[("AZURE_STORAGE_ACCOUNT", "acct")]),
        )
        .await;
        assert_eq!(
            loc.options().get("azure_storage_account_name"),
            Some("acct")
        );
    }

    #[tokio::test]
    async fn legacy_azure_form_is_recognized_with_the_account_from_the_environment() {
        let loc = resolve(
            &spec("az://acct/cont/p"),
            &env(&[("AZURE_STORAGE_ACCOUNT_NAME", "acct")]),
        )
        .await;
        assert_eq!(remote(&loc), (Scheme::Azure, "cont".into(), "p".into()));
        assert_eq!(loc.root_uri(), "az://cont/p");
        let mut s = spec("az://acct/cont/p");
        s.options = BTreeMap::from([("account_name".into(), "acct".into())]);
        let loc = resolve(&s, &env(&[])).await;
        assert_eq!(remote(&loc), (Scheme::Azure, "cont".into(), "p".into()));
    }

    #[test]
    fn proxy_urls_are_redacted() {
        let o = StorageOptions(BTreeMap::from([(
            "proxy_url".into(),
            "http://user:proxysecret@proxy:3128".into(),
        )]));
        assert!(!format!("{o:?}").contains("proxysecret"));
    }

    #[tokio::test]
    async fn timeouts_and_retries_are_lowered_unless_set() {
        let n = ObjectStoreConfig {
            connect_timeout: std::time::Duration::from_secs(7),
            read_timeout: std::time::Duration::from_secs(20),
            write_timeout: std::time::Duration::from_secs(40),
            max_retries: 2,
            ..Default::default()
        };
        let loc = StoreLocation::resolve(&spec("s3://b"), &quiet_env(), &n)
            .await
            .unwrap();
        assert_eq!(loc.options().get("connect_timeout"), Some("7s"));
        assert_eq!(loc.options().get("timeout"), Some("40s"));
        let lance = loc.lance_storage_options().unwrap();
        assert_eq!(
            lance.get("client_max_retries").map(String::as_str),
            Some("2")
        );

        let mut s = spec("s3://b");
        s.options = BTreeMap::from([("timeout".into(), "5s".into())]);
        let loc = StoreLocation::resolve(&s, &quiet_env(), &n).await.unwrap();
        assert_eq!(loc.options().get("timeout"), Some("5s"));
    }

    #[tokio::test]
    async fn gcs_and_azure_env_keys_are_picked_up() {
        let e = env(&[
            ("SERVICE_ACCOUNT", "/sa.json"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "YXprZXk="),
            ("IDENTITY_ENDPOINT", "http://msi"),
        ]);
        // The GCS client reads the credential file at build time, so a
        // missing file proves the variable reached it.
        for (vars, path) in [
            (e.clone(), "/sa.json"),
            (
                env(&[("GOOGLE_APPLICATION_CREDENTIALS", "/adc.json")]),
                "/adc.json",
            ),
        ] {
            let err = StoreLocation::resolve(&spec("gs://g"), &vars, &net())
                .await
                .expect_err("no credential file");
            assert!(format!("{err:#}").contains(path), "{err:#}");
        }
        let loc = resolve(&spec("az://c"), &e).await;
        assert_eq!(
            loc.options().get("azure_storage_account_key"),
            Some("YXprZXk=")
        );
        assert_eq!(loc.options().get("azure_msi_endpoint"), Some("http://msi"));
    }

    #[tokio::test]
    async fn unknown_options_are_forwarded_to_lance_only() {
        let mut s = spec("s3://b");
        s.options = BTreeMap::from([("use_opendal".into(), "false".into())]);
        let loc = resolve(&s, &quiet_env()).await;
        let lance = loc.lance_storage_options().unwrap();
        assert_eq!(lance.get("use_opendal").map(String::as_str), Some("false"));
    }

    // ---- L3: one map for both clients -----------------------------------

    #[tokio::test]
    async fn lance_gets_the_same_options_as_uni() {
        for uri in ["s3://b/p", "gs://g/p", "az://c/p"] {
            let e = {
                let mut e = quiet_env();
                e.insert("AZURE_STORAGE_ACCOUNT_NAME".into(), "acct".into());
                e
            };
            let loc = resolve(&spec(uri), &e).await;
            let lance = loc.lance_storage_options().unwrap();
            for (k, v) in loc.options().as_map() {
                assert_eq!(lance.get(k), Some(v), "{uri}: {k}");
            }
        }
    }

    // ---- L4: secrets never print ----------------------------------------

    #[tokio::test]
    async fn debug_never_prints_secrets() {
        let cfg = CloudStorageConfig::S3 {
            bucket: "b".into(),
            region: Some("r".into()),
            endpoint: None,
            access_key_id: Some("AKIDSECRETID".into()),
            secret_access_key: Some("verysecretvalue".into()),
            session_token: Some("sessiontokenvalue".into()),
            virtual_hosted_style: false,
        };
        let s = LocationSpec {
            uri: "s3://b".into(),
            cloud: Some(cfg.clone()),
            options: BTreeMap::from([("aws_secret_access_key".into(), "optsecretvalue".into())]),
        };
        let loc = resolve(&s, &env(&[])).await;
        for text in [format!("{cfg:?}"), format!("{s:?}"), format!("{loc:?}")] {
            for secret in ["verysecretvalue", "sessiontokenvalue", "optsecretvalue"] {
                assert!(!text.contains(secret), "{text}");
            }
        }
        assert!(format!("{loc:?}").contains("aws_region"));
    }

    // ---- L6: the root store is prefixed ----------------------------------

    #[tokio::test]
    async fn meta_and_table_stores_stay_under_the_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/bucket/p", dir.path().display());
        let loc = resolve(&spec(&uri), &env(&[])).await;
        let meta = loc.meta_store();
        meta.put(
            &Path::from("catalog/schema.json"),
            PutPayload::from_static(b"{}"),
        )
        .await
        .unwrap();
        let tables = loc.table_store(StoreLayout::Standard).unwrap();
        tables
            .put(&Path::from("catalog/latest"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        assert!(dir.path().join("bucket/p/catalog/schema.json").is_file());
        assert!(dir.path().join("bucket/p/storage/catalog/latest").is_file());
        // A listing through the prefixed handle is relative to it.
        use futures::TryStreamExt;
        let listed: Vec<_> = meta
            .list(Some(&Path::from("catalog")))
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed, vec!["catalog/schema.json".to_string()]);
        assert_eq!(
            loc.table_uri(StoreLayout::Standard),
            format!("file://{}/bucket/p/storage", dir.path().display())
        );
        assert_eq!(
            loc.table_uri(StoreLayout::Flat),
            format!("file://{}/bucket/p", dir.path().display())
        );
    }

    // ---- L7: URI joins ---------------------------------------------------

    #[test]
    fn join_uri_normalizes_slashes() {
        assert_eq!(join_uri("s3://b/p", "indexes/x"), "s3://b/p/indexes/x");
        assert_eq!(join_uri("s3://b/p/", "indexes/x"), "s3://b/p/indexes/x");
        assert_eq!(join_uri("s3://b/p/", "/indexes/x/"), "s3://b/p/indexes/x");
        assert_eq!(join_uri("/tmp/db", ""), "/tmp/db");
        assert_eq!(join_uri("s3://b", "t.lance"), "s3://b/t.lance");
    }

    // ---- layout probing --------------------------------------------------

    async fn file_loc(dir: &std::path::Path, rel: &str) -> StoreLocation {
        resolve(&spec(&format!("file://{}/{rel}", dir.display())), &env(&[])).await
    }

    fn touch(path: std::path::PathBuf) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"x").unwrap();
    }

    #[tokio::test]
    async fn probe_new_store_is_standard() {
        let dir = tempfile::tempdir().unwrap();
        let loc = file_loc(dir.path(), "a").await;
        assert_eq!(
            loc.probe_layout().await.unwrap(),
            LayoutProbe::Inferred(StoreLayout::Standard)
        );
    }

    #[tokio::test]
    async fn probe_recognizes_a_flat_store() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path().join("a/catalog/latest"));
        touch(dir.path().join("a/vertices.lance/_versions/1.manifest"));
        let loc = file_loc(dir.path(), "a").await;
        assert_eq!(
            loc.probe_layout().await.unwrap(),
            LayoutProbe::Inferred(StoreLayout::Flat)
        );
    }

    #[tokio::test]
    async fn probe_prefers_the_recorded_marker() {
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path().join("a/vertices.lance/_versions/1.manifest"));
        let loc = file_loc(dir.path(), "a").await;
        loc.write_layout_marker(StoreLayout::Standard)
            .await
            .unwrap();
        // A second write keeps the first marker.
        loc.write_layout_marker(StoreLayout::Flat).await.unwrap();
        assert_eq!(
            loc.probe_layout().await.unwrap(),
            LayoutProbe::Recorded(StoreLayout::Standard)
        );
    }

    #[tokio::test]
    async fn probe_detects_a_legacy_prefixed_store() {
        let dir = tempfile::tempdir().unwrap();
        // ≤ 5.3.0: tables under the prefix, metadata at the bucket root.
        touch(
            dir.path()
                .join("bucket/a/vertices.lance/_versions/1.manifest"),
        );
        touch(dir.path().join("bucket/catalog/schema.json"));
        touch(dir.path().join("bucket/catalog/latest"));
        let bucketed = |rel: &str| LocationSpec {
            uri: format!("file://{}/bucket/{rel}", dir.path().display()),
            options: BTreeMap::from([(
                FILE_BUCKET_ROOT_OPTION.to_string(),
                dir.path().join("bucket").display().to_string(),
            )]),
            ..Default::default()
        };
        let loc = resolve(&bucketed("a"), &env(&[])).await;
        assert_eq!(remote(&loc).2, "a");
        match loc.probe_layout().await.unwrap() {
            LayoutProbe::LegacyPrefixed { root_files } => {
                assert!(
                    root_files.iter().any(|f| f == "catalog/latest"),
                    "{root_files:?}"
                );
            }
            other => panic!("expected legacy, got {other:?}"),
        }
        // A sibling that is itself fresh is not affected by the root files.
        let fresh = resolve(&bucketed("b"), &env(&[])).await;
        assert_eq!(
            fresh.probe_layout().await.unwrap(),
            LayoutProbe::Inferred(StoreLayout::Standard)
        );
    }

    #[tokio::test]
    async fn bucket_root_store_is_never_legacy_prefixed() {
        // Everything at the root, as a ≤ 5.3.0 bucket-root store has it.
        let dir = tempfile::tempdir().unwrap();
        touch(dir.path().join("catalog/latest"));
        touch(dir.path().join("catalog/schema.json"));
        touch(dir.path().join("vertices.lance/_versions/1.manifest"));
        let s = LocationSpec {
            uri: format!("file://{}", dir.path().display()),
            options: BTreeMap::from([(
                FILE_BUCKET_ROOT_OPTION.to_string(),
                dir.path().display().to_string(),
            )]),
            ..Default::default()
        };
        let loc = resolve(&s, &env(&[])).await;
        assert_eq!(remote(&loc).2, "");
        assert_eq!(
            loc.probe_layout().await.unwrap(),
            LayoutProbe::Inferred(StoreLayout::Flat)
        );
    }
}
