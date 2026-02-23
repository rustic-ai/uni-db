// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::wal::{Mutation, WriteAheadLog};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{instrument, trace};
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};
use uni_common::graph::simple_graph::{Direction, SimpleGraph};
use uni_crdt::Crdt;

/// Returns the current timestamp in microseconds since Unix epoch.
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[derive(Clone, Debug)]
pub struct TombstoneEntry {
    pub eid: Eid,
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub edge_type: u32,
}

pub struct L0Buffer {
    /// Graph topology using simple adjacency lists
    pub graph: SimpleGraph,
    /// Soft-deleted edges (tombstones for LSM-style merging)
    pub tombstones: HashMap<Eid, TombstoneEntry>,
    /// Soft-deleted vertices
    pub vertex_tombstones: HashSet<Vid>,
    /// Edge version tracking for MVCC
    pub edge_versions: HashMap<Eid, u64>,
    /// Vertex version tracking for MVCC
    pub vertex_versions: HashMap<Vid, u64>,
    /// Edge properties (stored separately from topology)
    pub edge_properties: HashMap<Eid, Properties>,
    /// Vertex properties (stored separately from topology)
    pub vertex_properties: HashMap<Vid, Properties>,
    /// Edge endpoint lookup: eid -> (src, dst, type)
    pub edge_endpoints: HashMap<Eid, (Vid, Vid, u32)>,
    /// Vertex labels (VID -> list of label names)
    /// New in storage design: vertices can have multiple labels
    pub vertex_labels: HashMap<Vid, Vec<String>>,
    /// Edge types (EID -> type name)
    pub edge_types: HashMap<Eid, String>,
    /// Current version counter
    pub current_version: u64,
    /// Mutation count for flush decisions
    pub mutation_count: usize,
    /// Write-ahead log for durability
    pub wal: Option<Arc<WriteAheadLog>>,
    /// WAL LSN at the time this L0 was rotated for flush.
    /// Used to ensure WAL truncation doesn't remove entries needed by pending flushes.
    pub wal_lsn_at_flush: u64,
    /// Vertex creation timestamps (microseconds since epoch)
    pub vertex_created_at: HashMap<Vid, i64>,
    /// Vertex update timestamps (microseconds since epoch)
    pub vertex_updated_at: HashMap<Vid, i64>,
    /// Edge creation timestamps (microseconds since epoch)
    pub edge_created_at: HashMap<Eid, i64>,
    /// Edge update timestamps (microseconds since epoch)
    pub edge_updated_at: HashMap<Eid, i64>,
}

impl std::fmt::Debug for L0Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L0Buffer")
            .field("vertex_count", &self.graph.vertex_count())
            .field("edge_count", &self.graph.edge_count())
            .field("tombstones", &self.tombstones.len())
            .field("vertex_tombstones", &self.vertex_tombstones.len())
            .field("current_version", &self.current_version)
            .field("mutation_count", &self.mutation_count)
            .finish()
    }
}

impl L0Buffer {
    /// Merge CRDT properties into an existing property map.
    /// Attempts CRDT merge if both values are valid CRDTs, falls back to overwrite.
    fn merge_crdt_properties(entry: &mut Properties, properties: Properties) {
        for (k, v) in properties {
            // Attempt merge if CRDT — convert to serde_json::Value for CRDT deserialization
            let json_v: serde_json::Value = v.clone().into();
            if let Ok(mut new_crdt) = serde_json::from_value::<Crdt>(json_v)
                && let Some(existing_v) = entry.get(&k)
                && let Ok(existing_crdt) = serde_json::from_value::<Crdt>(existing_v.clone().into())
            {
                // Use try_merge to avoid panic on type mismatch
                if new_crdt.try_merge(&existing_crdt).is_ok()
                    && let Ok(merged_json) = serde_json::to_value(new_crdt)
                {
                    entry.insert(k, uni_common::Value::from(merged_json));
                    continue;
                }
                // try_merge failed (type mismatch) - fall through to overwrite
            }
            // Fallback: Overwrite
            entry.insert(k, v);
        }
    }

    /// Returns an estimate of the buffer size in bytes.
    pub fn size_bytes(&self) -> usize {
        let mut total = 0;
        // Topology
        total += self.graph.vertex_count() * 8;
        total += self.graph.edge_count() * 24;

        // Properties (rough estimate)
        for props in self.vertex_properties.values() {
            for k in props.keys() {
                total += k.len() + 16; // Value size is variable, 16 is a floor
            }
        }
        for props in self.edge_properties.values() {
            for k in props.keys() {
                total += k.len() + 16;
            }
        }

        // Metadata
        total += self.tombstones.len() * 64;
        total += self.edge_versions.len() * 16;
        total += self.vertex_versions.len() * 16;

        total
    }

    pub fn new(start_version: u64, wal: Option<Arc<WriteAheadLog>>) -> Self {
        Self {
            graph: SimpleGraph::new(),
            tombstones: HashMap::new(),
            vertex_tombstones: HashSet::new(),
            edge_versions: HashMap::new(),
            vertex_versions: HashMap::new(),
            edge_properties: HashMap::new(),
            vertex_properties: HashMap::new(),
            edge_endpoints: HashMap::new(),
            vertex_labels: HashMap::new(),
            edge_types: HashMap::new(),
            current_version: start_version,
            mutation_count: 0,
            wal,
            wal_lsn_at_flush: 0,
            vertex_created_at: HashMap::new(),
            vertex_updated_at: HashMap::new(),
            edge_created_at: HashMap::new(),
            edge_updated_at: HashMap::new(),
        }
    }

    pub fn insert_vertex(&mut self, vid: Vid, properties: Properties) {
        self.insert_vertex_with_labels(vid, properties, vec![]);
    }

    /// Insert a vertex with associated labels.
    pub fn insert_vertex_with_labels(
        &mut self,
        vid: Vid,
        properties: Properties,
        labels: Vec<String>,
    ) {
        self.current_version += 1;
        let version = self.current_version;
        let now = now_micros();

        if let Some(wal) = &self.wal {
            let _ = wal.append(&Mutation::InsertVertex {
                vid,
                properties: properties.clone(),
            });
        }

        self.vertex_tombstones.remove(&vid);

        let entry = self.vertex_properties.entry(vid).or_default();
        Self::merge_crdt_properties(entry, properties);
        self.vertex_versions.insert(vid, version);

        // Set timestamps - created_at only set if this is a new vertex
        self.vertex_created_at.entry(vid).or_insert(now);
        self.vertex_updated_at.insert(vid, now);

        // Track labels — always create an entry so unlabeled vertices are
        // distinguishable from "not in L0" when queried via get_vertex_labels.
        let existing = self.vertex_labels.entry(vid).or_default();
        for label in labels {
            if !existing.contains(&label) {
                existing.push(label);
            }
        }

        self.graph.add_vertex(vid);
        self.mutation_count += 1;
    }

    /// Add labels to an existing vertex.
    pub fn add_vertex_labels(&mut self, vid: Vid, labels: Vec<String>) {
        let existing = self.vertex_labels.entry(vid).or_default();
        for label in labels {
            if !existing.contains(&label) {
                existing.push(label);
            }
        }
    }

    /// Remove a label from an existing vertex.
    /// Returns true if the label was found and removed, false otherwise.
    pub fn remove_vertex_label(&mut self, vid: Vid, label: &str) -> bool {
        if let Some(labels) = self.vertex_labels.get_mut(&vid)
            && let Some(pos) = labels.iter().position(|l| l == label)
        {
            labels.remove(pos);
            self.current_version += 1;
            self.mutation_count += 1;
            // Note: WAL logging for label mutations not yet implemented
            // Currently consistent with add_vertex_labels behavior
            return true;
        }
        false
    }

    /// Set the type for an edge.
    pub fn set_edge_type(&mut self, eid: Eid, edge_type: String) {
        self.edge_types.insert(eid, edge_type);
    }

    pub fn delete_vertex(&mut self, vid: Vid) -> Result<()> {
        self.current_version += 1;
        let version = self.current_version;

        if let Some(wal) = &mut self.wal {
            wal.append(&Mutation::DeleteVertex { vid })?;
        }

        // Cascade delete: collect all edges connected to this vertex and create tombstones
        // This ensures edges in L0 get proper tombstones that will be flushed to L1
        let edges_to_delete: Vec<(Eid, Vid, Vid, u32)> = self
            .edge_endpoints
            .iter()
            .filter(|(_, (src, dst, _))| *src == vid || *dst == vid)
            .map(|(eid, (src, dst, etype))| (*eid, *src, *dst, *etype))
            .collect();

        for (eid, src, dst, etype) in edges_to_delete {
            // Create tombstone directly without WAL (vertex delete already logged)
            self.tombstones.insert(
                eid,
                TombstoneEntry {
                    eid,
                    src_vid: src,
                    dst_vid: dst,
                    edge_type: etype,
                },
            );
            self.edge_versions.insert(eid, version);
            self.graph.remove_edge(eid);
            self.mutation_count += 1;
        }

        self.vertex_tombstones.insert(vid);
        self.vertex_properties.remove(&vid);
        self.vertex_versions.insert(vid, version);
        self.graph.remove_vertex(vid);
        self.mutation_count += 1;
        Ok(())
    }

    pub fn insert_edge(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
    ) -> Result<()> {
        self.current_version += 1;
        let version = self.current_version;
        let now = now_micros();

        if let Some(wal) = &mut self.wal {
            wal.append(&Mutation::InsertEdge {
                src_vid,
                dst_vid,
                edge_type,
                eid,
                version,
                properties: properties.clone(),
            })?;
        }

        // Add vertices to graph topology if they don't exist.
        // IMPORTANT: Only add to graph structure, do NOT call insert_vertex.
        // insert_vertex creates a new version with empty properties, which would
        // cause MVCC to pick the empty version as "latest", losing original properties.
        if !self.graph.contains_vertex(src_vid) {
            self.graph.add_vertex(src_vid);
        }
        if !self.graph.contains_vertex(dst_vid) {
            self.graph.add_vertex(dst_vid);
        }

        // Add edge to graph
        self.graph.add_edge(src_vid, dst_vid, eid, edge_type);

        // Store metadata with CRDT merge logic
        if !properties.is_empty() {
            let entry = self.edge_properties.entry(eid).or_default();
            Self::merge_crdt_properties(entry, properties);
        }

        self.edge_versions.insert(eid, version);
        self.edge_endpoints
            .insert(eid, (src_vid, dst_vid, edge_type));
        self.tombstones.remove(&eid);

        // Set timestamps - created_at only set if this is a new edge
        self.edge_created_at.entry(eid).or_insert(now);
        self.edge_updated_at.insert(eid, now);

        self.mutation_count += 1;

        Ok(())
    }

    pub fn delete_edge(
        &mut self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
    ) -> Result<()> {
        self.current_version += 1;
        let version = self.current_version;
        let now = now_micros();

        if let Some(wal) = &mut self.wal {
            wal.append(&Mutation::DeleteEdge {
                eid,
                src_vid,
                dst_vid,
                edge_type,
                version,
            })?;
        }

        // Add tombstone for LSM merging
        self.tombstones.insert(
            eid,
            TombstoneEntry {
                eid,
                src_vid,
                dst_vid,
                edge_type,
            },
        );
        self.edge_versions.insert(eid, version);

        // Update timestamp - deletion is an update
        self.edge_updated_at.insert(eid, now);

        // Remove from graph (safe, no panics)
        self.graph.remove_edge(eid);
        self.mutation_count += 1;

        Ok(())
    }

    /// Returns neighbors in the specified direction.
    /// O(degree) complexity - iterates only edges connected to the vertex.
    pub fn get_neighbors(
        &self,
        vid: Vid,
        edge_type: u32,
        direction: Direction,
    ) -> Vec<(Vid, Eid, u64)> {
        let edges = self.graph.neighbors(vid, direction);

        edges
            .iter()
            .filter(|e| e.edge_type == edge_type && !self.is_tombstoned(e.eid))
            .map(|e| {
                let neighbor = match direction {
                    Direction::Outgoing => e.dst_vid,
                    Direction::Incoming => e.src_vid,
                };
                let version = self.edge_versions.get(&e.eid).copied().unwrap_or(0);
                (neighbor, e.eid, version)
            })
            .collect()
    }

    pub fn is_tombstoned(&self, eid: Eid) -> bool {
        self.tombstones.contains_key(&eid)
    }

    /// Returns all VIDs in vertex_labels that match the given label name.
    /// Used for L0 overlay during vertex scanning.
    pub fn vids_for_label(&self, label_name: &str) -> Vec<Vid> {
        self.vertex_labels
            .iter()
            .filter(|(_, labels)| labels.iter().any(|l| l == label_name))
            .map(|(vid, _)| *vid)
            .collect()
    }

    /// Returns all vertex VIDs in the L0 buffer.
    ///
    /// Used for schemaless scanning (MATCH (n) without label).
    pub fn all_vertex_vids(&self) -> Vec<Vid> {
        self.vertex_properties.keys().copied().collect()
    }

    /// Returns all VIDs in vertex_labels that match any of the given label names.
    /// Used for L0 overlay during multi-label vertex scanning.
    pub fn vids_for_labels(&self, label_names: &[&str]) -> Vec<Vid> {
        self.vertex_labels
            .iter()
            .filter(|(_, labels)| label_names.iter().any(|ln| labels.iter().any(|l| l == *ln)))
            .map(|(vid, _)| *vid)
            .collect()
    }

    /// Returns all VIDs that have ALL specified labels.
    pub fn vids_with_all_labels(&self, label_names: &[&str]) -> Vec<Vid> {
        self.vertex_labels
            .iter()
            .filter(|(_, labels)| label_names.iter().all(|ln| labels.iter().any(|l| l == *ln)))
            .map(|(vid, _)| *vid)
            .collect()
    }

    /// Gets the labels for a VID.
    pub fn get_vertex_labels(&self, vid: Vid) -> Option<&[String]> {
        self.vertex_labels.get(&vid).map(|v| v.as_slice())
    }

    /// Gets the edge type for an EID.
    pub fn get_edge_type(&self, eid: Eid) -> Option<&str> {
        self.edge_types.get(&eid).map(|s| s.as_str())
    }

    /// Returns all EIDs in edge_types that match the given type name.
    /// Used for L0 overlay during schemaless edge scanning.
    pub fn eids_for_type(&self, type_name: &str) -> Vec<Eid> {
        self.edge_types
            .iter()
            .filter(|(eid, etype)| *etype == type_name && !self.tombstones.contains_key(eid))
            .map(|(eid, _)| *eid)
            .collect()
    }

    /// Returns all edge EIDs in the L0 buffer (non-tombstoned).
    ///
    /// Used for schemaless scanning (`MATCH ()-[r]->()`) without type.
    pub fn all_edge_eids(&self) -> Vec<Eid> {
        self.edge_endpoints
            .keys()
            .filter(|eid| !self.tombstones.contains_key(eid))
            .copied()
            .collect()
    }

    /// Returns edge endpoint data (src_vid, dst_vid) for an EID.
    pub fn get_edge_endpoints(&self, eid: Eid) -> Option<(Vid, Vid)> {
        self.edge_endpoints
            .get(&eid)
            .map(|(src, dst, _)| (*src, *dst))
    }

    /// Returns full edge endpoint data (src_vid, dst_vid, edge_type_id) for an EID.
    pub fn get_edge_endpoint_full(&self, eid: Eid) -> Option<(Vid, Vid, u32)> {
        self.edge_endpoints.get(&eid).copied()
    }

    #[instrument(skip(self, other), level = "trace")]
    pub fn merge(&mut self, other: &L0Buffer) -> Result<()> {
        trace!(
            other_mutation_count = other.mutation_count,
            "Merging L0 buffer"
        );
        // Merge Vertices
        for &vid in &other.vertex_tombstones {
            self.delete_vertex(vid)?;
        }

        for (vid, props) in &other.vertex_properties {
            let labels = other.vertex_labels.get(vid).cloned().unwrap_or_default();
            self.insert_vertex_with_labels(*vid, props.clone(), labels);
        }

        // Merge vertex labels that might not have properties
        for (vid, labels) in &other.vertex_labels {
            if !self.vertex_labels.contains_key(vid) {
                self.vertex_labels.insert(*vid, labels.clone());
            }
        }

        // Merge Edges - insert all edges from edge_endpoints, using empty props if none exist
        for (eid, (src, dst, etype)) in &other.edge_endpoints {
            if other.tombstones.contains_key(eid) {
                self.delete_edge(*eid, *src, *dst, *etype)?;
            } else {
                let props = other.edge_properties.get(eid).cloned().unwrap_or_default();
                self.insert_edge(*src, *dst, *etype, *eid, props)?;
            }
        }

        // Merge edge types
        for (eid, etype) in &other.edge_types {
            self.edge_types.insert(*eid, etype.clone());
        }

        Ok(())
    }

    /// Replay mutations from WAL without re-logging them.
    /// Used during startup recovery to restore L0 state from persisted WAL.
    /// Uses CRDT merge semantics to ensure recovered state matches pre-crash state.
    #[instrument(skip(self, mutations), level = "debug")]
    pub fn replay_mutations(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        trace!(count = mutations.len(), "Replaying mutations");
        for mutation in mutations {
            match mutation {
                Mutation::InsertVertex { vid, properties } => {
                    // Apply without WAL logging, with CRDT merge semantics
                    self.current_version += 1;
                    let version = self.current_version;

                    self.vertex_tombstones.remove(&vid);
                    let entry = self.vertex_properties.entry(vid).or_default();
                    Self::merge_crdt_properties(entry, properties);
                    self.vertex_versions.insert(vid, version);
                    self.graph.add_vertex(vid);
                    self.mutation_count += 1;
                }
                Mutation::DeleteVertex { vid } => {
                    self.current_version += 1;
                    let version = self.current_version;

                    // Cascade delete: collect all edges connected to this vertex and create tombstones
                    // This mirrors the logic in delete_vertex() to ensure WAL replay produces the same state
                    let edges_to_delete: Vec<(Eid, Vid, Vid, u32)> = self
                        .edge_endpoints
                        .iter()
                        .filter(|(_, (src, dst, _))| *src == vid || *dst == vid)
                        .map(|(eid, (src, dst, etype))| (*eid, *src, *dst, *etype))
                        .collect();

                    for (eid, src, dst, etype) in edges_to_delete {
                        self.tombstones.insert(
                            eid,
                            TombstoneEntry {
                                eid,
                                src_vid: src,
                                dst_vid: dst,
                                edge_type: etype,
                            },
                        );
                        self.edge_versions.insert(eid, version);
                        self.graph.remove_edge(eid);
                        self.mutation_count += 1;
                    }

                    self.vertex_tombstones.insert(vid);
                    self.vertex_properties.remove(&vid);
                    self.vertex_versions.insert(vid, version);
                    self.graph.remove_vertex(vid);
                    self.mutation_count += 1;
                }
                Mutation::InsertEdge {
                    src_vid,
                    dst_vid,
                    edge_type,
                    eid,
                    version: _,
                    properties,
                } => {
                    self.current_version += 1;
                    let version = self.current_version;

                    // Add vertices if they don't exist
                    if !self.graph.contains_vertex(src_vid) {
                        self.graph.add_vertex(src_vid);
                    }
                    if !self.graph.contains_vertex(dst_vid) {
                        self.graph.add_vertex(dst_vid);
                    }

                    self.graph.add_edge(src_vid, dst_vid, eid, edge_type);

                    // CRDT merge for edge properties
                    if !properties.is_empty() {
                        let entry = self.edge_properties.entry(eid).or_default();
                        Self::merge_crdt_properties(entry, properties);
                    }

                    self.edge_versions.insert(eid, version);
                    self.edge_endpoints
                        .insert(eid, (src_vid, dst_vid, edge_type));
                    self.tombstones.remove(&eid);
                    self.mutation_count += 1;
                }
                Mutation::DeleteEdge {
                    eid,
                    src_vid,
                    dst_vid,
                    edge_type,
                    version: _,
                } => {
                    self.current_version += 1;
                    let version = self.current_version;

                    self.tombstones.insert(
                        eid,
                        TombstoneEntry {
                            eid,
                            src_vid,
                            dst_vid,
                            edge_type,
                        },
                    );
                    self.edge_versions.insert(eid, version);
                    self.graph.remove_edge(eid);
                    self.mutation_count += 1;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l0_buffer_ops() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid_ab = Eid::new(101);

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new())?;

        let neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, vid_b);
        assert_eq!(neighbors[0].1, eid_ab);

        l0.delete_edge(eid_ab, vid_a, vid_b, 1)?;
        assert!(l0.is_tombstoned(eid_ab));

        // Verify neighbors are empty after deletion
        let neighbors_after = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors_after.len(), 0);

        Ok(())
    }

    #[test]
    fn test_l0_buffer_multiple_edges() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_ac = Eid::new(102);

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new())?;
        l0.insert_edge(vid_a, vid_c, 1, eid_ac, HashMap::new())?;

        let neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 2);

        // Delete one edge
        l0.delete_edge(eid_ab, vid_a, vid_b, 1)?;

        // Should still have one neighbor
        let neighbors_after = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors_after.len(), 1);
        assert_eq!(neighbors_after[0].0, vid_c);

        Ok(())
    }

    #[test]
    fn test_l0_buffer_edge_type_filter() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_ac = Eid::new(201); // Different edge type

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new())?;
        l0.insert_edge(vid_a, vid_c, 2, eid_ac, HashMap::new())?;

        // Filter by edge type 1
        let type1_neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(type1_neighbors.len(), 1);
        assert_eq!(type1_neighbors[0].0, vid_b);

        // Filter by edge type 2
        let type2_neighbors = l0.get_neighbors(vid_a, 2, Direction::Outgoing);
        assert_eq!(type2_neighbors.len(), 1);
        assert_eq!(type2_neighbors[0].0, vid_c);

        Ok(())
    }

    #[test]
    fn test_l0_buffer_incoming_edges() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_cb = Eid::new(102);

        // a -> b and c -> b
        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new())?;
        l0.insert_edge(vid_c, vid_b, 1, eid_cb, HashMap::new())?;

        // Check incoming edges to b
        let incoming = l0.get_neighbors(vid_b, 1, Direction::Incoming);
        assert_eq!(incoming.len(), 2);

        Ok(())
    }

    /// Regression test: merge should preserve edges without properties
    #[test]
    fn test_merge_empty_props_edge() -> Result<()> {
        let mut main_l0 = L0Buffer::new(0, None);
        let mut tx_l0 = L0Buffer::new(0, None);

        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid_ab = Eid::new(101);

        // Insert edge with empty properties in transaction L0
        tx_l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new())?;

        // Verify edge exists in tx_l0
        assert!(tx_l0.edge_endpoints.contains_key(&eid_ab));
        assert!(!tx_l0.edge_properties.contains_key(&eid_ab)); // No properties entry

        // Merge into main L0
        main_l0.merge(&tx_l0)?;

        // Edge should exist in main L0 after merge
        assert!(main_l0.edge_endpoints.contains_key(&eid_ab));
        let neighbors = main_l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, vid_b);

        Ok(())
    }

    /// Regression test: WAL replay should use CRDT merge semantics
    #[test]
    fn test_replay_crdt_merge() -> Result<()> {
        use crate::runtime::wal::Mutation;
        use serde_json::json;
        use uni_common::Value;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Create GCounter CRDT values using correct serde format:
        // {"t": "gc", "d": {"counts": {...}}}
        let counter1: Value = json!({
            "t": "gc",
            "d": {"counts": {"node1": 5}}
        })
        .into();
        let counter2: Value = json!({
            "t": "gc",
            "d": {"counts": {"node2": 3}}
        })
        .into();

        // First mutation: insert vertex with counter1
        let mut props1 = HashMap::new();
        props1.insert("counter".to_string(), counter1.clone());
        l0.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: props1,
        }])?;

        // Second mutation: insert same vertex with counter2 (should merge)
        let mut props2 = HashMap::new();
        props2.insert("counter".to_string(), counter2.clone());
        l0.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: props2,
        }])?;

        // Verify CRDT was merged (both node1 and node2 counts present)
        let stored_props = l0.vertex_properties.get(&vid).unwrap();
        let stored_counter = stored_props.get("counter").unwrap();

        // Convert back to serde_json::Value for nested access
        let stored_json: serde_json::Value = stored_counter.clone().into();
        // The merged counter should have both node1: 5 and node2: 3
        let data = stored_json.get("d").unwrap();
        let counts = data.get("counts").unwrap();
        assert_eq!(counts.get("node1"), Some(&json!(5)));
        assert_eq!(counts.get("node2"), Some(&json!(3)));

        Ok(())
    }
}
