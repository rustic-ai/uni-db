// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub snapshot_id: String,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub parent_snapshot: Option<String>,
    pub schema_version: u32,
    pub version_high_water_mark: u64,
    pub wal_high_water_mark: u64,

    pub vertices: HashMap<String, LabelSnapshot>,
    pub edges: HashMap<String, EdgeSnapshot>,
    /// Writer features of the binary that saved this manifest
    /// (`uni_common::core::schema::WRITER_FEATURES`); stamped by
    /// `SnapshotManager::save_snapshot`. Empty in a manifest an older binary
    /// wrote.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writer_features: Vec<String>,
    /// Bulk-load versions at or below `version_high_water_mark` that were
    /// not visible when this manifest was cut. A bulk load reserves its
    /// version when it starts staging and becomes visible only when it
    /// commits, so a reader pinned to this manifest excludes these versions
    /// for good. Empty in a manifest an older binary wrote.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unpublished_bulk_versions: Vec<u64>,
    /// End of the VID range the store's ID allocator had reserved when this
    /// manifest was saved: no vertex in the store has a VID at or above it.
    /// Lets an open whose `id_allocator.json` is gone (a hybrid store's
    /// lost local directory) restart above every VID in use. `None` in a
    /// manifest an older binary wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_vid: Option<u64>,
    /// EID counterpart of [`next_vid`](Self::next_vid).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_eid: Option<u64>,
}

/// Snapshot counters for one entity kind (a vertex label or an edge type).
///
/// Labels and edge types capture the identical shape, so one struct serves
/// both via the [`LabelSnapshot`] / [`EdgeSnapshot`] aliases — the map key
/// (in [`SnapshotManifest::vertices`] vs `edges`) already conveys which kind.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntitySnapshot {
    pub version: u32,
    pub count: u64,
    pub lance_version: u64,
}

/// Snapshot counters for a vertex label. Alias of [`EntitySnapshot`].
pub type LabelSnapshot = EntitySnapshot;

/// Snapshot counters for an edge type. Alias of [`EntitySnapshot`].
pub type EdgeSnapshot = EntitySnapshot;

impl SnapshotManifest {
    pub fn new(snapshot_id: String, schema_version: u32) -> Self {
        Self {
            snapshot_id,
            name: None,
            created_at: Utc::now(),
            parent_snapshot: None,
            schema_version,
            version_high_water_mark: 0,
            wal_high_water_mark: 0,
            vertices: HashMap::new(),
            edges: HashMap::new(),
            writer_features: crate::core::schema::current_writer_features(),
            unpublished_bulk_versions: Vec::new(),
            next_vid: None,
            next_eid: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manifest_without_id_marks_parses_and_marks_round_trip() {
        let old = r#"{"snapshot_id":"s","name":null,"created_at":"2026-01-01T00:00:00Z",
            "parent_snapshot":null,"schema_version":1,"version_high_water_mark":3,
            "wal_high_water_mark":2,"vertices":{},"edges":{}}"#;
        let m: SnapshotManifest = serde_json::from_str(old).unwrap();
        assert_eq!((m.next_vid, m.next_eid), (None, None));
        assert!(!serde_json::to_string(&m).unwrap().contains("next_vid"));

        let mut m = m;
        m.next_vid = Some(1000);
        m.next_eid = Some(2000);
        let back: SnapshotManifest =
            serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!((back.next_vid, back.next_eid), (Some(1000), Some(2000)));
    }
}
