// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Adjacency cache for fast neighbor lookups.
//!
//! In the new storage model, the cache keys by (edge_type, direction) without
//! label partitioning. VIDs are used directly as offsets into the CSR.

use crate::runtime::l0::L0Buffer;
use crate::storage::csr::CompressedSparseRow;
use crate::storage::manager::StorageManager;
use dashmap::DashMap;
use metrics;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uni_common::core::id::{Eid, Vid};

pub use super::direction::Direction;

pub struct AdjacencyCache {
    /// CSR for each (edge_type, direction) pair.
    ///
    /// In the current design, adjacency is not partitioned by label.
    /// VIDs are used directly as offsets into the CSR.
    /// Edge type is u32 with bit 31 = 0 for schema'd, 1 for schemaless.
    csr_maps: DashMap<(u32, Direction), Arc<CompressedSparseRow>>,

    /// Current memory usage
    current_bytes: AtomicUsize,
}

impl std::fmt::Debug for AdjacencyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdjacencyCache")
            .field("csr_maps_count", &self.csr_maps.len())
            .field("current_bytes", &self.current_bytes.load(Ordering::Relaxed))
            .finish()
    }
}

impl AdjacencyCache {
    pub fn new(_max_bytes: usize) -> Self {
        Self {
            csr_maps: DashMap::new(),
            current_bytes: AtomicUsize::new(0),
        }
    }

    /// Warm cache from Storage (L2 + L1) for a specific edge type and direction.
    /// This rebuilds the CSR from scratch.
    pub async fn warm(
        &self,
        storage: &StorageManager,
        edge_type_id: u32,
        direction: Direction,
        version: Option<u64>,
    ) -> anyhow::Result<()> {
        let schema = storage.schema_manager().schema();

        let edge_type_name = schema
            .edge_type_name_by_id(edge_type_id)
            .ok_or_else(|| anyhow::anyhow!("Edge type {} not found", edge_type_id))?;

        // Determine which labels to load adjacency for based on edge type metadata.
        let labels_to_load: Vec<String> = {
            let edge_meta = schema.edge_types.get(edge_type_name);
            match (direction, edge_meta) {
                (Direction::Outgoing, Some(meta)) => meta.src_labels.clone(),
                (Direction::Incoming, Some(meta)) => meta.dst_labels.clone(),
                (Direction::Both, Some(meta)) => {
                    let mut labels = meta.src_labels.clone();
                    labels.extend(meta.dst_labels.iter().cloned());
                    labels.sort();
                    labels.dedup();
                    labels
                }
                _ => Vec::new(),
            }
        };

        let mut entries = Vec::new();
        let mut deleted_eids = std::collections::HashSet::new();

        // Determine which directions to read
        let directions_to_read = match direction {
            Direction::Outgoing => vec![(Direction::Outgoing, "fwd")],
            Direction::Incoming => vec![(Direction::Incoming, "bwd")],
            Direction::Both => vec![(Direction::Outgoing, "fwd"), (Direction::Incoming, "bwd")],
        };

        for (read_dir, dir_str) in directions_to_read {
            // Try to read adjacency data for all relevant labels
            for label_name in &labels_to_load {
                // 1. Read L2 (Adjacency Dataset) from LanceDB
                let adj_ds = storage.adjacency_dataset(edge_type_name, label_name, dir_str);
                let lancedb_store = storage.lancedb_store();

                if let Ok(adj_ds) = adj_ds
                    && let Ok(table) = adj_ds.open_lancedb(lancedb_store).await
                {
                    use arrow_array::{ListArray, UInt64Array};
                    use futures::TryStreamExt;
                    use lancedb::query::{ExecutableQuery, QueryBase};

                    // Apply version filtering if querying a snapshot
                    let mut query = table.query();
                    if let Some(hwm) = version {
                        query = query.only_if(format!("_version <= {}", hwm));
                    }

                    let stream = query.execute().await;
                    if let Ok(stream) = stream {
                        let batches: Vec<arrow_array::RecordBatch> =
                            stream.try_collect().await.unwrap_or_default();

                        for batch in batches {
                            let src_col = batch
                                .column_by_name("src_vid")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap();
                            let neighbors_list = batch
                                .column_by_name("neighbors")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<ListArray>()
                                .unwrap();
                            let eids_list = batch
                                .column_by_name("edge_ids")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<ListArray>()
                                .unwrap();

                            for i in 0..batch.num_rows() {
                                let src_u64 = src_col.value(i);
                                // Use VID's raw value directly as offset
                                let src_offset = src_u64;

                                let neighbors_array_ref = neighbors_list.value(i);
                                let neighbors = neighbors_array_ref
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .unwrap();

                                let eids_array_ref = eids_list.value(i);
                                let eids = eids_array_ref
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .unwrap();

                                for j in 0..neighbors.len() {
                                    entries.push((
                                        src_offset,
                                        Vid::from(neighbors.value(j)),
                                        Eid::from(eids.value(j)),
                                    ));
                                }
                            }
                        }
                    }
                }
            }

            // 2. Read L1 (Delta) from LanceDB
            let delta_ds = storage.delta_dataset(edge_type_name, dir_str)?;
            let lancedb_store = storage.lancedb_store();

            if let Ok(table) = delta_ds.open_lancedb(lancedb_store).await {
                use arrow_array::{UInt8Array, UInt64Array};
                use futures::TryStreamExt;
                use lancedb::query::{ExecutableQuery, QueryBase};

                // Apply version filtering if querying a snapshot
                let mut query = table.query();
                if let Some(hwm) = version {
                    query = query.only_if(format!("_version <= {}", hwm));
                }

                let stream = query.execute().await;
                if let Ok(stream) = stream {
                    let batches: Vec<arrow_array::RecordBatch> =
                        stream.try_collect().await.unwrap_or_default();

                    for batch in batches {
                        let src_col = batch
                            .column_by_name("src_vid")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let dst_col = batch
                            .column_by_name("dst_vid")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let eid_col = batch
                            .column_by_name("eid")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let op_col = batch
                            .column_by_name("op")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap();

                        for i in 0..batch.num_rows() {
                            let src_vid = Vid::from(src_col.value(i));
                            let dst_vid = Vid::from(dst_col.value(i));

                            let eid = Eid::from(eid_col.value(i));
                            let op = op_col.value(i); // 0=Insert, 1=Delete

                            if op == 0 {
                                // For BWD/Incoming: key by dst_vid, neighbor is src_vid
                                // For FWD/Outgoing: key by src_vid, neighbor is dst_vid
                                let (key, neighbor) = if read_dir == Direction::Incoming {
                                    (dst_vid.as_u64(), src_vid)
                                } else {
                                    (src_vid.as_u64(), dst_vid)
                                };
                                entries.push((key, neighbor, eid));
                            } else {
                                deleted_eids.insert(eid);
                            }
                        }
                    }
                }
            }
        }

        // Filter out deleted edges from entries
        if !deleted_eids.is_empty() {
            entries.retain(|(_, _, eid)| !deleted_eids.contains(eid));
        }

        // Determine max offset
        let max_offset = entries.iter().map(|(o, _, _)| *o).max().unwrap_or(0);

        // Build CSR
        let csr = CompressedSparseRow::new(max_offset as usize, entries);

        // Update stats
        let size = csr.memory_usage();
        self.current_bytes.fetch_add(size, Ordering::Relaxed);

        // Store in cache (keyed by edge_type and direction only, not label)
        self.csr_maps
            .insert((edge_type_id, direction), Arc::new(csr));

        Ok(())
    }

    /// Get CSR for an edge type and direction (new API without label).
    pub fn get_csr_unified(
        &self,
        edge_type: u16,
        direction: Direction,
    ) -> Option<Arc<CompressedSparseRow>> {
        self.csr_maps
            .get(&(edge_type, direction))
            .map(|r| r.value().clone())
    }

    /// Get CSR for an edge type and direction.
    pub fn get_csr(
        &self,
        edge_type: u16,
        direction: Direction,
    ) -> Option<Arc<CompressedSparseRow>> {
        self.csr_maps
            .get(&(edge_type, direction))
            .map(|r| r.value().clone())
    }

    /// Get neighbors with O(1) lookup
    pub fn get_neighbors(
        &self,
        _vid: Vid,
        edge_type: u16,
        direction: Direction,
    ) -> Option<Arc<CompressedSparseRow>> {
        let res = self
            .csr_maps
            .get(&(edge_type, direction))
            .map(|r| r.value().clone());

        if res.is_some() {
            metrics::counter!("uni_adjacency_cache_hits_total").increment(1);
        } else {
            metrics::counter!("uni_adjacency_cache_misses_total").increment(1);
        }
        res
    }

    /// Merge with L0 buffer for uncommitted edges
    pub fn get_neighbors_with_l0<'a>(
        &'a self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
        l0: Option<&'a L0Buffer>,
    ) -> Vec<(Vid, Eid)> {
        let mut neighbors_map: HashMap<Eid, Vid> = HashMap::new();

        // 1. Get from CSR
        if let Some(csr) = self.get_neighbors(vid, edge_type, direction) {
            let (n, e) = csr.get_neighbors(vid);
            for (&neighbor, &eid) in n.iter().zip(e.iter()) {
                neighbors_map.insert(eid, neighbor);
            }
        }

        // 2. Overlay L0
        if let Some(l0) = l0 {
            self.overlay_l0_neighbors(vid, edge_type, direction, l0, &mut neighbors_map);
        }

        neighbors_map.into_iter().map(|(e, n)| (n, e)).collect()
    }

    /// Merge with multiple L0 buffers for uncommitted edges.
    /// L0s should be provided oldest-first so newer writes win.
    pub fn get_neighbors_with_l0s<'a>(
        &'a self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
        l0: Option<&'a L0Buffer>,
        pending_l0s: &[&'a L0Buffer],
    ) -> Vec<(Vid, Eid)> {
        let mut neighbors_map: HashMap<Eid, Vid> = HashMap::new();

        // 1. Get from CSR
        if let Some(csr) = self.get_neighbors(vid, edge_type, direction) {
            let (n, e) = csr.get_neighbors(vid);
            for (&neighbor, &eid) in n.iter().zip(e.iter()) {
                neighbors_map.insert(eid, neighbor);
            }
        }

        // 2. Overlay pending L0s (oldest first)
        for pending_l0 in pending_l0s {
            self.overlay_l0_neighbors(vid, edge_type, direction, pending_l0, &mut neighbors_map);
        }

        // 3. Overlay current L0 (newest, wins)
        if let Some(l0) = l0 {
            self.overlay_l0_neighbors(vid, edge_type, direction, l0, &mut neighbors_map);
        }

        neighbors_map.into_iter().map(|(e, n)| (n, e)).collect()
    }

    pub fn overlay_l0_neighbors<'a>(
        &'a self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
        l0: &'a L0Buffer,
        neighbors_map: &mut HashMap<Eid, Vid>,
    ) {
        use uni_common::graph::simple_graph::Direction as SimpleDirection;

        // Determine which directions to query from L0
        let directions: &[SimpleDirection] = match direction {
            Direction::Outgoing => &[SimpleDirection::Outgoing],
            Direction::Incoming => &[SimpleDirection::Incoming],
            Direction::Both => &[SimpleDirection::Outgoing, SimpleDirection::Incoming],
        };

        // Apply L0 inserts for each direction
        for &dir in directions {
            for (neighbor, eid, _ver) in l0.get_neighbors(vid, edge_type, dir) {
                neighbors_map.insert(eid, neighbor);
            }
        }

        // Apply L0 Tombstones
        for eid in l0.tombstones.keys() {
            neighbors_map.remove(eid);
        }
    }

    pub fn overlay_l0_neighbors_with_type<'a>(
        &'a self,
        vid: Vid,
        edge_type: u16,
        direction: Direction,
        l0: &'a L0Buffer,
        neighbors_map: &mut HashMap<Eid, (Vid, u16)>,
    ) {
        use uni_common::graph::simple_graph::Direction as SimpleDirection;

        // Determine which directions to query from L0
        let directions: &[SimpleDirection] = match direction {
            Direction::Outgoing => &[SimpleDirection::Outgoing],
            Direction::Incoming => &[SimpleDirection::Incoming],
            Direction::Both => &[SimpleDirection::Outgoing, SimpleDirection::Incoming],
        };

        // Apply L0 inserts for each direction
        for &dir in directions {
            for (neighbor, eid, _ver) in l0.get_neighbors(vid, edge_type, dir) {
                neighbors_map.insert(eid, (neighbor, edge_type));
            }
        }

        // Apply L0 Tombstones
        for eid in l0.tombstones.keys() {
            neighbors_map.remove(eid);
        }
    }

    /// Invalidate cache entries for a specific edge type.
    pub fn invalidate(&self, edge_type: u16) {
        self.csr_maps.retain(|(k_edge, _), _| *k_edge != edge_type);
    }
}
