// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Index datasets under the read cut.
//!
//! The UID map, JSON, inverted and sparse indexes keep their content in Lance
//! datasets under `{table root}/indexes/`, opened by URI rather than through
//! the table backend. Their versions are part of the read cut too, keyed by
//! their path under the table root (`indexes/...`): reads open the version
//! the published manifest names, writes record (and check) the version they
//! produce, and a writable open rolls them back like the tables.
//!
//! Each index type carries a [`DatasetCut`] naming its dataset and the cut.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;

use crate::storage::manager::StorageManager;
use crate::storage::read_cut::{CommitTicket, ReadCut};

/// One index dataset under a store's read cut.
#[derive(Clone, Debug, Default)]
pub struct DatasetCut {
    cut: Option<Arc<ReadCut>>,
    /// The dataset's path under the table root: its key in the cut.
    key: String,
    /// Whether reads open the published version (a reader), or the newest
    /// (a writer updating the dataset in place).
    at_cut: bool,
}

impl DatasetCut {
    /// Outside any cut: reads and writes see the newest version.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// The dataset at `uri` under `base_uri`'s `cut`.
    #[must_use]
    pub fn new(cut: Option<Arc<ReadCut>>, base_uri: &str, uri: &str, at_cut: bool) -> Self {
        let key = uri
            .strip_prefix(base_uri.trim_end_matches('/'))
            .unwrap_or(uri)
            .trim_matches('/')
            .to_string();
        Self { cut, key, at_cut }
    }

    /// The same dataset, read at its newest version (for a writer).
    #[must_use]
    pub fn for_writes(&self) -> Self {
        Self {
            at_cut: false,
            ..self.clone()
        }
    }

    /// Opens the dataset at `uri`: for a reader at the version the published
    /// cut names (absent when it names none), otherwise at its newest.
    ///
    /// # Errors
    /// The dataset could not be opened, or is absent at the cut (a not-found
    /// error, as for a dataset never written).
    #[cfg(feature = "lance-backend")]
    pub async fn open(&self, uri: &str) -> lance::Result<lance::Dataset> {
        // Boxed as `dyn`: callers nest this in futures whose Send proof
        // otherwise overflows (E0275); only erasing the type stops it.
        let open: std::pin::Pin<
            Box<dyn std::future::Future<Output = lance::Result<lance::Dataset>> + Send + '_>,
        > = Box::pin(self.open_inner(uri));
        open.await
    }

    #[cfg(feature = "lance-backend")]
    async fn open_inner(&self, uri: &str) -> lance::Result<lance::Dataset> {
        let published = self
            .cut
            .as_ref()
            .filter(|_| self.at_cut)
            .and_then(|cut| cut.view());
        match published {
            Some(published) => match published.tables.get(&self.key) {
                Some(&version) => {
                    crate::backend::lance_directory::dataset_builder(uri)
                        .with_version(version)
                        .load()
                        .await
                }
                None => Err(lance::Error::not_found(uri)),
            },
            None => crate::backend::lance_directory::open_dataset_uri(uri).await,
        }
    }

    /// Starts a commit on the dataset; see [`ReadCut::begin`]. Fails first
    /// once the handle's write fence has tripped, so no index-dataset commit
    /// skips it.
    ///
    /// # Errors
    /// The fence has tripped.
    pub fn begin(&self) -> Result<Option<CommitTicket>> {
        self.check_fence()?;
        Ok(self.cut.as_ref().map(|cut| cut.begin(&self.key)))
    }

    /// Fails once the handle's write fence has tripped; see
    /// [`ReadCut::check_fence`].
    ///
    /// # Errors
    /// The fence has tripped.
    pub fn check_fence(&self) -> Result<()> {
        match &self.cut {
            Some(cut) => cut.check_fence(),
            None => Ok(()),
        }
    }

    /// Records a version of the dataset this handle produced without a
    /// ticket (an index built on it).
    pub fn record(&self, version: u64) {
        if let Some(cut) = self.cut.as_ref().filter(|cut| cut.is_recording()) {
            cut.record(&self.key, version);
        }
    }
}

/// Records the version a commit on an index dataset produced; see
/// [`CommitTicket::finish`].
///
/// # Errors
/// A foreign commit was folded in.
pub fn finish_commit(ticket: Option<CommitTicket>, read: Option<u64>, version: u64) -> Result<()> {
    match ticket {
        Some(ticket) => ticket.finish(read, version),
        None => Ok(()),
    }
}

/// Every index dataset under `storage`'s table root, as a key.
#[cfg(feature = "lance-backend")]
async fn list(storage: &StorageManager) -> Result<Vec<String>> {
    use crate::store_utils::{DEFAULT_TIMEOUT, list_with_timeout};
    use object_store::path::Path as ObjectStorePath;

    let store = storage.store();
    let prefix = ObjectStorePath::from("indexes");
    let metas = list_with_timeout(&store, Some(&prefix), DEFAULT_TIMEOUT).await?;
    let mut out: Vec<String> = metas
        .iter()
        .filter_map(|m| {
            let path = m.location.as_ref();
            let (root, rest) = path.split_once("/_versions/")?;
            // A branch of a dataset lives in its `tree/` directory and is not
            // a dataset of its own.
            (rest.ends_with(".manifest") && !root.contains("/tree/")).then(|| root.to_string())
        })
        .collect();
    out.sort();
    out.dedup();
    Ok(out)
}

#[cfg(feature = "lance-backend")]
fn uri(storage: &StorageManager, key: &str) -> String {
    format!("{}/{key}", storage.base_path().trim_end_matches('/'))
}

/// The newest version of every index dataset.
///
/// # Errors
/// A listing or open failed.
#[cfg(feature = "lance-backend")]
pub(crate) async fn head_versions(storage: &StorageManager) -> Result<BTreeMap<String, u64>> {
    let mut out = BTreeMap::new();
    for key in list(storage).await? {
        let dataset =
            crate::backend::lance_directory::open_dataset_uri(&uri(storage, &key)).await?;
        out.insert(key, dataset.version().version);
    }
    Ok(out)
}

/// Rolls every index dataset back to the version `target` names, and
/// deletes those it does not name; returns what it undid.
///
/// # Errors
/// A dataset could not be read, restored or deleted.
#[cfg(feature = "lance-backend")]
pub(crate) async fn reconcile(
    storage: &StorageManager,
    target: &BTreeMap<String, u64>,
) -> Result<Vec<String>> {
    use anyhow::Context;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;
    use object_store::path::Path as ObjectStorePath;

    let mut undone = Vec::new();
    for key in list(storage).await? {
        let at = uri(storage, &key);
        match target.get(&key) {
            Some(&named) => {
                let current = crate::backend::lance_directory::open_dataset_uri(&at).await?;
                let version = current.version().version;
                if version <= named {
                    continue;
                }
                let mut dataset = crate::backend::lance_directory::dataset_builder(&at)
                    .with_version(named)
                    .load()
                    .await
                    .with_context(|| format!("open index dataset {at} at v{named}"))?;
                dataset
                    .restore()
                    .await
                    .with_context(|| format!("restore index dataset {at} to v{named}"))?;
                undone.push(format!("{key} v{version} to v{named}"));
            }
            None => {
                // Written by a commit no manifest names: it indexes only rows
                // the open discards, and is rebuilt from the tables on the
                // next write.
                let store = storage.store();
                let prefix = ObjectStorePath::from(format!("{key}/"));
                let paths: Vec<ObjectStorePath> = store
                    .list(Some(&prefix))
                    .map_ok(|m| m.location)
                    .try_collect()
                    .await
                    .with_context(|| format!("list index dataset {key}"))?;
                for path in paths {
                    store
                        .delete(&path)
                        .await
                        .with_context(|| format!("delete {path}"))?;
                }
                undone.push(format!("{key} (named by no manifest)"));
            }
        }
    }
    Ok(undone)
}

/// Without the Lance backend there are no index datasets.
#[cfg(not(feature = "lance-backend"))]
pub(crate) async fn head_versions(_storage: &StorageManager) -> Result<BTreeMap<String, u64>> {
    Ok(BTreeMap::new())
}

/// Without the Lance backend there are no index datasets.
#[cfg(not(feature = "lance-backend"))]
pub(crate) async fn reconcile(
    _storage: &StorageManager,
    _target: &BTreeMap<String, u64>,
) -> Result<Vec<String>> {
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dataset_is_keyed_by_its_path_under_the_table_root() {
        let cut = DatasetCut::new(
            None,
            "file:///s/a/storage/",
            "file:///s/a/storage/indexes/uni_id_to_vid/P/index.lance",
            true,
        );
        assert_eq!(cut.key, "indexes/uni_id_to_vid/P/index.lance");
        assert!(!cut.for_writes().at_cut);
    }
}
