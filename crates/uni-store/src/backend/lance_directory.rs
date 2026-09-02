// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A directory of Lance datasets — the `lancedb::Connection` replacement.
//!
//! `lancedb`'s only structural contribution over core Lance is modest: it maps
//! a base URI plus a logical table name onto a dataset directory, and it can
//! enumerate the tables it finds there. Everything else `uni-store` used it for
//! (scan, write, merge-insert, index build, vector/FTS search) has a direct
//! core-Lance equivalent, and the fork path in
//! [`crate::backend::lance_branch`] has been calling those equivalents all
//! along.
//!
//! This module supplies that missing piece so the rest of the backend can drop
//! to raw [`lance::Dataset`], collapsing the two parallel implementations —
//! `backend::lance` on lancedb and `backend::lance_branch` on core Lance —
//! into one.
//!
//! # The layout contract
//!
//! A table named `vertices_Person` lives at `{base_uri}/vertices_Person.lance`.
//! This is not an API we are given; it is a convention we must match exactly,
//! because it is what makes primary reads and fork branch reads resolve to the
//! same dataset. Verified against `lancedb-0.30.0`:
//!
//! - `database/listing.rs:724` — `format!("{}.{}", name, LANCE_EXTENSION)`
//! - `database/listing.rs:941` — `table_names` = `read_dir(base)`, keep entries
//!   whose extension is `lance`, return the file stem, sorted
//!
//! [`LanceDirectory::table_names`] reproduces that listing, and
//! [`LanceDirectory::dataset_uri`] that path shape. Changing either silently
//! detaches primary from the fork branches.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{WriteMode, WriteParams};
use lance::io::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor};
use lance::session::Session;
use object_store::path::Path as ObjectPath;

/// Directory suffix marking a Lance dataset. Matches lancedb's
/// `LANCE_EXTENSION`.
const LANCE_EXTENSION: &str = "lance";

/// A base URI containing Lance datasets addressed by logical table name.
///
/// Cheap to clone (all fields are shared or small).
#[derive(Clone)]
pub struct LanceDirectory {
    /// The URI as configured, e.g. `/var/lib/uni` or `s3://bucket/prefix`.
    base_uri: String,
    /// Object store over `base_uri`, built once at connect. Used only for
    /// [`Self::table_names`]; dataset opens go through [`DatasetBuilder`],
    /// which builds its own store from the dataset URI.
    store: Arc<ObjectStore>,
    /// `base_uri`'s path within `store`, as returned by `ObjectStore::from_uri`.
    base_path: ObjectPath,
    /// Cloud credentials / tuning knobs, re-applied on every dataset open.
    storage_options: Option<HashMap<String, String>>,
    /// Shared Lance session, reused by every dataset open.
    ///
    /// The session owns Lance's index and metadata caches. Without it each
    /// `DatasetBuilder` gets `session: None`, i.e. a *fresh* cache, so an ANN
    /// index is re-read and re-decoded on every single query. Measured on
    /// SIFT-1M: HNSW query cost was linear in corpus size and unaffected by
    /// `ef_search`, `m`, partition count or payload encoding, with the graph
    /// search itself only ~6% of samples and the rest in page decode, memmove
    /// and allocation.
    ///
    /// Sharing the session does **not** reintroduce the staleness the
    /// `open` docs warn about: that concern is caching the `Dataset` *handle*,
    /// which pins a manifest version. Every open still loads the current
    /// manifest; only content-addressed index and metadata blocks are reused.
    session: Arc<Session>,
}

impl std::fmt::Debug for LanceDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits `storage_options` — it carries credentials.
        f.debug_struct("LanceDirectory")
            .field("base_uri", &self.base_uri)
            .field("base_path", &self.base_path)
            .finish_non_exhaustive()
    }
}

impl LanceDirectory {
    /// Open the directory at `base_uri`.
    ///
    /// `storage_options` are the same cloud options `lancedb::connect` took,
    /// and are threaded into both the listing store and every dataset open.
    ///
    /// # Errors
    ///
    /// Returns an error if `base_uri` cannot be parsed or its object store
    /// cannot be constructed (bad scheme, missing credentials).
    pub async fn connect(
        base_uri: &str,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        // Storage options reach `ObjectStoreParams` through an accessor rather
        // than a plain field, because Lance supports refreshing credentials
        // mid-session. Ours are fixed for the process, hence the static form.
        let params = ObjectStoreParams {
            storage_options_accessor: storage_options
                .clone()
                .map(|opts| Arc::new(StorageOptionsAccessor::with_static_options(opts))),
            ..Default::default()
        };
        let registry = Arc::new(ObjectStoreRegistry::default());
        let (store, base_path) = ObjectStore::from_uri_and_params(registry, base_uri, &params)
            .await
            .with_context(|| format!("open object store for '{base_uri}'"))?;
        Ok(Self {
            base_uri: base_uri.to_string(),
            store,
            base_path,
            storage_options,
            session: Arc::new(Session::default()),
        })
    }

    /// The configured base URI.
    #[must_use]
    pub fn base_uri(&self) -> &str {
        &self.base_uri
    }

    /// Resolve a logical table name to its dataset URI.
    ///
    /// See the module docs: this shape is a compatibility contract with
    /// lancedb-written data and with `lance_branch`, not a free choice.
    #[must_use]
    pub fn dataset_uri(&self, table: &str) -> String {
        if self.base_uri.ends_with('/') {
            format!("{}{table}.{LANCE_EXTENSION}", self.base_uri)
        } else {
            format!("{}/{table}.{LANCE_EXTENSION}", self.base_uri)
        }
    }

    /// Every table in the directory, sorted.
    ///
    /// Lists the base path and keeps entries ending in `.lance`, returning the
    /// stem. Entries that are not Lance datasets are ignored rather than
    /// erroring, matching lancedb.
    ///
    /// This is O(number of tables) and hits the object store, which is why
    /// callers cache existence rather than probing per query (issue #55).
    ///
    /// # Errors
    ///
    /// Returns an error if the base path cannot be listed. A *missing* base
    /// path is not an error — it yields an empty list, which is the correct
    /// answer for a database that has never been written.
    pub async fn table_names(&self) -> Result<Vec<String>> {
        let entries = match self.store.read_dir(self.base_path.clone()).await {
            Ok(entries) => entries,
            // A base directory that does not exist yet has no tables. Treating
            // this as an error would make `table_exists` fail on a fresh
            // database instead of answering "no".
            Err(e) if is_not_found(&e) => return Ok(Vec::new()),
            Err(e) => {
                return Err(anyhow::anyhow!(e))
                    .with_context(|| format!("list tables under '{}'", self.base_uri));
            }
        };
        let mut names: Vec<String> = entries
            .iter()
            .filter_map(|entry| entry.strip_suffix(&format!(".{LANCE_EXTENSION}")))
            .map(String::from)
            .collect();
        names.sort();
        Ok(names)
    }

    /// Open `table` at its latest version.
    ///
    /// Every read and write path opens fresh rather than caching the handle: a
    /// [`Dataset`] is pinned to the version it was opened at and does not
    /// refresh, so a cached handle would silently miss rows committed by a
    /// later flush. This mirrors why `LanceDbBackend::get_or_open_table`
    /// never populated its cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset does not exist or cannot be opened.
    pub async fn open(&self, table: &str) -> Result<Dataset> {
        let uri = self.dataset_uri(table);
        self.builder(&uri)
            .load()
            .await
            .with_context(|| format!("open table '{table}' at '{uri}'"))
    }

    /// Open `table` pinned at `version`.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset or that version does not exist.
    pub async fn open_at_version(&self, table: &str, version: u64) -> Result<Dataset> {
        let uri = self.dataset_uri(table);
        self.builder(&uri)
            .with_version(version)
            .load()
            .await
            .with_context(|| format!("open table '{table}' at version {version} ('{uri}')"))
    }

    /// A [`DatasetBuilder`] for `uri` carrying this directory's storage
    /// options.
    fn builder(&self, uri: &str) -> DatasetBuilder {
        let builder = DatasetBuilder::from_uri(uri).with_session(self.session.clone());
        match &self.storage_options {
            Some(opts) => builder.with_storage_options(opts.clone()),
            None => builder,
        }
    }

    /// The storage options, for callers that must build their own writer.
    #[must_use]
    pub fn storage_options(&self) -> Option<&HashMap<String, String>> {
        self.storage_options.as_ref()
    }

    /// [`WriteParams`] for `mode`, carrying this directory's storage options.
    ///
    /// Writes need the object-store credentials threaded through
    /// `store_params`; without it a cloud-backed write would fall back to
    /// unauthenticated access and fail only in deployment, never in tests.
    #[must_use]
    pub fn write_params(&self, mode: WriteMode) -> WriteParams {
        WriteParams {
            mode,
            store_params: self.storage_options.clone().map(|opts| ObjectStoreParams {
                storage_options_accessor: Some(Arc::new(
                    StorageOptionsAccessor::with_static_options(opts),
                )),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Delete `table` and everything under it.
    ///
    /// Mirrors lancedb's `drop_tables` (`database/listing.rs:707`), minus its
    /// `commit_handler.delete()` step. That step only matters for an *external*
    /// manifest store (e.g. DynamoDB), where commit state lives outside the
    /// dataset directory; uni configures no such handler, so commit state is
    /// inside the directory and `remove_dir_all` reclaims it. Revisit if an
    /// external commit handler is ever configured.
    ///
    /// Removing a table that does not exist is an error, matching lancedb's
    /// `TableNotFound`.
    ///
    /// # Errors
    ///
    /// Returns an error if the table is absent or the delete fails.
    pub async fn remove_table(&self, table: &str) -> Result<()> {
        let path = self
            .base_path
            .clone()
            .join(format!("{table}.{LANCE_EXTENSION}"));
        self.store
            .remove_dir_all(path)
            .await
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("drop table '{table}'"))
    }
}

/// Whether a Lance error is a "not found", so a missing base directory can be
/// distinguished from a real listing failure.
fn is_not_found(e: &lance::Error) -> bool {
    // Lance wraps the object_store error; match on both its own NotFound and
    // the rendered message, since the wrapping layer varies by scheme.
    matches!(e, lance::Error::NotFound { .. })
        || matches!(e, lance::Error::DatasetNotFound { .. })
        || e.to_string().contains("No such file or directory")
        || e.to_string().contains("NotFound")
}
