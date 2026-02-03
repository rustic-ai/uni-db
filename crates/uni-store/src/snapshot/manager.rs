// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::store_utils::{DEFAULT_TIMEOUT, get_with_timeout, list_with_timeout, put_with_timeout};
use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;
use uni_common::core::snapshot::SnapshotManifest;

pub struct SnapshotManager {
    store: Arc<dyn ObjectStore>,
}

impl SnapshotManager {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn manifest_path(&self, snapshot_id: &str) -> ObjectStorePath {
        ObjectStorePath::from(format!("catalog/manifests/{}.json", snapshot_id))
    }

    fn latest_ptr_path(&self) -> ObjectStorePath {
        ObjectStorePath::from("catalog/latest")
    }

    fn named_snapshots_path(&self) -> ObjectStorePath {
        ObjectStorePath::from("catalog/named_snapshots.json")
    }

    #[instrument(skip(self, manifest), fields(snapshot_id = %manifest.snapshot_id, size_bytes), level = "info")]
    pub async fn save_snapshot(&self, manifest: &SnapshotManifest) -> Result<()> {
        let path = self.manifest_path(&manifest.snapshot_id);
        let json = serde_json::to_string_pretty(manifest)?;
        tracing::Span::current().record("size_bytes", json.len());
        put_with_timeout(&self.store, &path, Bytes::from(json), DEFAULT_TIMEOUT).await?;
        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn load_snapshot(&self, snapshot_id: &str) -> Result<SnapshotManifest> {
        let path = self.manifest_path(snapshot_id);
        let result = get_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await?;
        let bytes = result.bytes().await?;
        let content = String::from_utf8(bytes.to_vec())?;
        let manifest: SnapshotManifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }

    pub async fn list_snapshots(&self) -> Result<Vec<String>> {
        let prefix = ObjectStorePath::from("catalog/manifests");
        let metas = list_with_timeout(&self.store, Some(&prefix), DEFAULT_TIMEOUT).await?;
        let mut ids = Vec::new();

        for meta in metas {
            if let Some(filename) = meta.location.filename()
                && filename.ends_with(".json")
            {
                ids.push(filename.trim_end_matches(".json").to_string());
            }
        }
        Ok(ids)
    }

    pub async fn load_latest_snapshot(&self) -> Result<Option<SnapshotManifest>> {
        let latest_path = self.latest_ptr_path();
        match get_with_timeout(&self.store, &latest_path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(anyhow::Error::from)?;
                let snapshot_id = String::from_utf8(bytes.to_vec())?;
                let snapshot_id = snapshot_id.trim();
                if snapshot_id.is_empty() {
                    return Ok(None);
                }
                Ok(Some(self.load_snapshot(snapshot_id).await?))
            }
            Err(e) if e.to_string().contains("not found") => Ok(None),
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self), level = "info")]
    pub async fn set_latest_snapshot(&self, snapshot_id: &str) -> Result<()> {
        let path = self.latest_ptr_path();
        put_with_timeout(
            &self.store,
            &path,
            Bytes::from(snapshot_id.to_string()),
            DEFAULT_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn load_named_snapshots(&self) -> Result<HashMap<String, String>> {
        let path = self.named_snapshots_path();
        match get_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8(bytes.to_vec())?;
                Ok(serde_json::from_str(&content)?)
            }
            Err(_) => Ok(HashMap::new()),
        }
    }

    pub async fn save_named_snapshot(&self, name: &str, snapshot_id: &str) -> Result<()> {
        let mut map = self.load_named_snapshots().await?;
        map.insert(name.to_string(), snapshot_id.to_string());

        let json = serde_json::to_string_pretty(&map)?;
        put_with_timeout(
            &self.store,
            &self.named_snapshots_path(),
            Bytes::from(json),
            DEFAULT_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    pub async fn get_named_snapshot(&self, name: &str) -> Result<Option<String>> {
        let map = self.load_named_snapshots().await?;
        Ok(map.get(name).cloned())
    }

    /// Find the most recent snapshot created at or before the given timestamp.
    pub async fn find_snapshot_at_time(
        &self,
        target: DateTime<Utc>,
    ) -> Result<Option<SnapshotManifest>> {
        let ids = self.list_snapshots().await?;
        let mut best: Option<SnapshotManifest> = None;

        for id in ids {
            if let Ok(m) = self.load_snapshot(&id).await
                && m.created_at <= target
                && best.as_ref().is_none_or(|b| m.created_at > b.created_at)
            {
                best = Some(m);
            }
        }
        Ok(best)
    }
}
