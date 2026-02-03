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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelSnapshot {
    pub version: u32,
    pub count: u64,
    pub lance_version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeSnapshot {
    pub version: u32,
    pub count: u64,
    pub lance_version: u64,
}

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
        }
    }
}
