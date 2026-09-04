// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::backend::types::{FilterExpr, Scalar};
use crate::runtime::context::QueryContext;
use crate::runtime::l0::L0Buffer;
use crate::runtime::l0_visibility;
use crate::storage::main_vertex::MainVertexDataset;
use crate::storage::manager::StorageManager;
use crate::storage::value_codec::CrdtDecodeMode;
use anyhow::{Result, anyhow};
use arrow_array::{Array, BooleanArray, RecordBatch, UInt64Array};
use lru::LruCache;
use metrics;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, instrument, warn};
use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{DataType, SchemaManager};
use uni_crdt::Crdt;

pub struct PropertyManager {
    storage: Arc<StorageManager>,
    schema_manager: Arc<SchemaManager>,
    /// Plugin registry consulted by CRDT merges via
    /// [`uni_crdt::Crdt::merge_via_registry`]. The legacy 3-arg
    /// [`Self::new`] passes an empty registry so callers that don't
    /// wire plugin dispatch keep getting bit-identical native
    /// behavior (empty registry → `merge_via_registry`'s native
    /// fallback). Production paths in `UniInner` use
    /// [`Self::with_plugin_registry`] to share the host's registry.
    plugin_registry: Arc<uni_plugin::PluginRegistry>,
    /// Cache is None when capacity=0 (caching disabled)
    vertex_cache: Option<Mutex<LruCache<(Vid, String), Value>>>,
    edge_cache: Option<Mutex<LruCache<(uni_common::core::id::Eid, String), Value>>>,
    cache_capacity: usize,
}

impl PropertyManager {
    /// Construct a `PropertyManager` with an empty plugin registry.
    ///
    /// Back-compat shim for the ~17 algorithm and test call sites that
    /// don't need registry-dispatched CRDT merges. Equivalent to
    /// [`Self::with_plugin_registry`] with `Arc::new(PluginRegistry::new())`.
    pub fn new(
        storage: Arc<StorageManager>,
        schema_manager: Arc<SchemaManager>,
        capacity: usize,
    ) -> Self {
        Self::with_plugin_registry(
            storage,
            schema_manager,
            capacity,
            Arc::new(uni_plugin::PluginRegistry::new()),
        )
    }

    /// Construct a `PropertyManager` wired to a shared `PluginRegistry`.
    ///
    /// CRDT merges in this `PropertyManager` consult `plugin_registry`
    /// for `CrdtKindProvider`s matching each `Crdt::kind()`; matched
    /// kinds dispatch through the provider (so hot-reloaded plugins
    /// take effect immediately), unmatched kinds fall back to the
    /// native `Crdt::try_merge`.
    pub fn with_plugin_registry(
        storage: Arc<StorageManager>,
        schema_manager: Arc<SchemaManager>,
        capacity: usize,
        plugin_registry: Arc<uni_plugin::PluginRegistry>,
    ) -> Self {
        // Capacity of 0 disables caching
        let (vertex_cache, edge_cache) = if capacity == 0 {
            (None, None)
        } else {
            let cap = NonZeroUsize::new(capacity).unwrap();
            (
                Some(Mutex::new(LruCache::new(cap))),
                Some(Mutex::new(LruCache::new(cap))),
            )
        };

        Self {
            storage,
            schema_manager,
            plugin_registry,
            vertex_cache,
            edge_cache,
            cache_capacity: capacity,
        }
    }

    pub fn cache_size(&self) -> usize {
        self.cache_capacity
    }

    /// Check if caching is enabled
    pub fn caching_enabled(&self) -> bool {
        self.cache_capacity > 0
    }

    /// Clear all caches.
    /// Call this when L0 is rotated, flushed, or compaction occurs to prevent stale reads.
    pub async fn clear_cache(&self) {
        if let Some(ref cache) = self.vertex_cache {
            cache.lock().await.clear();
        }
        if let Some(ref cache) = self.edge_cache {
            cache.lock().await.clear();
        }
    }

    /// Invalidate a specific vertex's cached properties.
    pub async fn invalidate_vertex(&self, _vid: Vid) {
        if let Some(ref cache) = self.vertex_cache {
            let mut cache = cache.lock().await;
            // LruCache doesn't have a way to iterate and remove, so we pop entries
            // that match the vid. This is O(n) but necessary for targeted invalidation.
            // For simplicity, clear the entire cache - LRU will repopulate as needed.
            cache.clear();
        }
    }

    /// Invalidate a specific edge's cached properties.
    pub async fn invalidate_edge(&self, _eid: uni_common::core::id::Eid) {
        if let Some(ref cache) = self.edge_cache {
            let mut cache = cache.lock().await;
            // Same approach as invalidate_vertex
            cache.clear();
        }
    }

    #[instrument(skip(self, ctx), level = "trace")]
    pub async fn get_edge_prop(
        &self,
        eid: uni_common::core::id::Eid,
        prop: &str,
        ctx: Option<&QueryContext>,
    ) -> Result<Value> {
        // 1. Check if deleted in any L0 layer
        if l0_visibility::is_edge_deleted(eid, ctx) {
            return Ok(Value::Null);
        }

        // 2. Check L0 chain for property (transaction -> main -> pending)
        if let Some(val) = l0_visibility::lookup_edge_prop(eid, prop, ctx) {
            return Ok(val);
        }

        // 3. Check Cache (if enabled)
        if let Some(ref cache) = self.edge_cache {
            let mut cache = cache.lock().await;
            if let Some(val) = cache.get(&(eid, prop.to_string())) {
                debug!(eid = ?eid, prop, "Cache HIT");
                metrics::counter!("uni_property_cache_hits_total", "type" => "edge").increment(1);
                return Ok(val.clone());
            } else {
                debug!(eid = ?eid, prop, "Cache MISS");
                metrics::counter!("uni_property_cache_misses_total", "type" => "edge").increment(1);
            }
        }

        // 4. Fetch from Storage
        let all = self.get_all_edge_props_with_ctx(eid, ctx).await?;
        let val = all
            .as_ref()
            .and_then(|props| props.get(prop).cloned())
            .unwrap_or(Value::Null);

        // 5. Update Cache (if enabled) - Cache ALL fetched properties, not just requested one
        if let Some(ref cache) = self.edge_cache {
            let mut cache = cache.lock().await;
            if let Some(ref props) = all {
                for (prop_name, prop_val) in props {
                    cache.put((eid, prop_name.clone()), prop_val.clone());
                }
            } else {
                // No properties found, cache the null result for this property
                cache.put((eid, prop.to_string()), Value::Null);
            }
        }

        Ok(val)
    }

    pub async fn get_all_edge_props_with_ctx(
        &self,
        eid: uni_common::core::id::Eid,
        ctx: Option<&QueryContext>,
    ) -> Result<Option<Properties>> {
        // 1. Check if deleted in any L0 layer
        if l0_visibility::is_edge_deleted(eid, ctx) {
            return Ok(None);
        }

        // 2. Accumulate properties from L0 layers (oldest to newest)
        let mut final_props = l0_visibility::accumulate_edge_props(eid, ctx).unwrap_or_default();

        // 3. Fetch from storage runs
        let storage_props = self.fetch_all_edge_props_from_storage(eid).await?;

        // 4. Handle case where edge exists but has no properties
        if final_props.is_empty() && storage_props.is_none() {
            if l0_visibility::edge_exists_in_l0(eid, ctx) {
                return Ok(Some(Properties::new()));
            }
            return Ok(None);
        }

        // 5. Merge storage properties (L0 takes precedence)
        if let Some(sp) = storage_props {
            for (k, v) in sp {
                final_props.entry(k).or_insert(v);
            }
        }

        Ok(Some(final_props))
    }

    async fn fetch_all_edge_props_from_storage(&self, eid: Eid) -> Result<Option<Properties>> {
        // In the new design, we scan all edge types since EID doesn't embed type info
        self.fetch_all_edge_props_from_storage_with_hint(eid, None)
            .await
    }

    async fn fetch_all_edge_props_from_storage_with_hint(
        &self,
        eid: Eid,
        type_name_hint: Option<&str>,
    ) -> Result<Option<Properties>> {
        let schema = self.schema_manager.schema();
        let backend = self.storage.backend();

        // If hint provided, use it directly
        let type_names: Vec<&str> = if let Some(hint) = type_name_hint {
            vec![hint]
        } else {
            // Scan all edge types
            schema.edge_types.keys().map(|s| s.as_str()).collect()
        };

        for type_name in type_names {
            let type_props = schema.properties.get(type_name);

            // For now, edges are primarily in Delta runs before compaction to L2 CSR.
            // We check FWD delta runs.
            if self.storage.delta_dataset(type_name, "fwd").is_err() {
                continue; // Edge type doesn't exist, try next
            }

            // Use backend for edge property lookup
            use crate::backend::table_names;
            use crate::backend::types::ScanRequest;

            let table_name = table_names::delta_table_name(type_name, "fwd");
            if !backend.table_exists(&table_name).await.unwrap_or(false) {
                continue; // No data for this type, try next
            }

            let base_filter = FilterExpr::equals("eid", Scalar::UInt(eid.as_u64()));
            let filter_expr = self.storage.apply_version_filter(base_filter);

            let batches = match backend
                .scan(ScanRequest::all(&table_name).with_filter(filter_expr))
                .await
            {
                Ok(b) => b,
                Err(_) => continue,
            };

            // Collect all rows for this edge, sorted by version
            let mut rows: Vec<(u64, u8, Properties)> = Vec::new();

            for batch in batches {
                let op_col = match batch.column_by_name("op") {
                    Some(c) => c
                        .as_any()
                        .downcast_ref::<arrow_array::UInt8Array>()
                        .unwrap(),
                    None => continue,
                };
                let ver_col = match batch.column_by_name("_version") {
                    Some(c) => c.as_any().downcast_ref::<UInt64Array>().unwrap(),
                    None => continue,
                };

                for row in 0..batch.num_rows() {
                    let ver = ver_col.value(row);
                    let op = op_col.value(row);
                    let mut props = Properties::new();

                    if op != 1 {
                        // Not a delete - extract properties
                        if let Some(tp) = type_props {
                            for (p_name, p_meta) in tp {
                                if let Some(col) = batch.column_by_name(p_name)
                                    && !col.is_null(row)
                                {
                                    let val =
                                        Self::value_from_column(col.as_ref(), &p_meta.r#type, row)?;
                                    props.insert(p_name.clone(), val);
                                }
                            }
                        }
                    }
                    rows.push((ver, op, props));
                }
            }

            if rows.is_empty() {
                continue;
            }

            // Sort by version (ascending) so we merge in order
            rows.sort_by_key(|(ver, _, _)| *ver);

            // Merge properties across all versions
            // For CRDT properties: merge values
            // For non-CRDT properties: later versions overwrite earlier ones
            let mut merged_props: Properties = Properties::new();
            let mut is_deleted = false;

            for (_, op, props) in rows {
                if op == 1 {
                    // Delete operation - mark as deleted
                    is_deleted = true;
                    merged_props.clear();
                } else {
                    is_deleted = false;
                    for (p_name, p_val) in props {
                        // Check if this is a CRDT property
                        let is_crdt = type_props
                            .and_then(|tp| tp.get(&p_name))
                            .map(|pm| matches!(pm.r#type, DataType::Crdt(_)))
                            .unwrap_or(false);

                        if is_crdt {
                            // Merge CRDT values
                            if let Some(existing) = merged_props.get(&p_name) {
                                // A failed merge used to drop `p_val` and keep the
                                // older value, silently. Two other sites resolved
                                // the same failure the opposite way; propagating
                                // removes the choice rather than picking a winner
                                // nobody has the information to pick (#233).
                                let merged = self.merge_crdt_values(existing, &p_val)?;
                                merged_props.insert(p_name, merged);
                            } else {
                                merged_props.insert(p_name, p_val);
                            }
                        } else {
                            // Non-CRDT: later version overwrites
                            merged_props.insert(p_name, p_val);
                        }
                    }
                }
            }

            if is_deleted {
                return Ok(None);
            }

            if !merged_props.is_empty() {
                return Ok(Some(merged_props));
            }
        }

        // Fallback to main edges table props_json for unknown/schemaless types.
        // Bounded by the same high water mark the delta tier is filtered by
        // above — otherwise this tier alone reads at HEAD while the others
        // honour the snapshot. The hwm is `None` unless this `PropertyManager`
        // was built over pinned storage (today: `UniInner::at_snapshot`), so
        // for a read-write transaction both tiers read at HEAD by design.
        use crate::storage::main_edge::MainEdgeDataset;
        if let Some(props) = MainEdgeDataset::find_props_by_eid(
            self.storage.backend(),
            eid,
            self.storage.version_high_water_mark(),
        )
        .await?
        {
            return Ok(Some(props));
        }

        Ok(None)
    }

    /// Reports whether a *live* flushed edge of `edge_type` already carries the
    /// given unique-key values, excluding `exclude_eid`.
    ///
    /// The committed-storage half of the edge-uniqueness full-horizon probe (the
    /// in-memory L0 layers are checked separately via `has_edge_constraint_key`).
    /// The per-type delta table is an LSM log — multiple versions per eid, later
    /// `op = 0` writes overwrite properties, `op = 1` deletes — so a naive
    /// `prop = val AND op = 0` count would wrongly flag an edge that was later
    /// deleted or updated away from `val`. Instead this narrows to candidate eids
    /// on ONE key property (guaranteed present in some `op = 0` row iff the edge
    /// currently holds that value), then resolves each candidate's *current
    /// merged* properties via `fetch_all_edge_props_from_storage_with_hint`
    /// and confirms the full key still matches on a live edge. Correct — never
    /// leaks a duplicate — without an O(edges) full scan.
    ///
    /// # Errors
    /// Propagates backend scan errors — fails closed rather than treating a
    /// conflict as absent.
    pub async fn flushed_edge_key_conflict(
        &self,
        edge_type: &str,
        key_values: &[(String, Value)],
        exclude_eid: Option<Eid>,
    ) -> Result<bool> {
        if key_values.is_empty() {
            return Ok(false);
        }
        use crate::backend::table_names;
        use crate::backend::types::ScanRequest;

        let table_name = table_names::delta_table_name(edge_type, "fwd");
        let backend = self.storage.backend();
        if !backend.table_exists(&table_name).await.unwrap_or(false) {
            return Ok(false);
        }

        // Narrow to candidate eids via the first key property. Any live edge whose
        // current value of this property equals `probe_val` must have set it in an
        // `op = 0` row, so this filter catches every genuine conflict; a candidate
        // that was since deleted or updated is discarded by the per-eid resolution
        // below.
        let (probe_prop, probe_val) = &key_values[0];
        let probe_scalar = match probe_val {
            Value::String(s) => Scalar::Str(s.clone()),
            Value::Int(n) => Scalar::Int(*n),
            Value::Float(f) => Scalar::Float(*f),
            Value::Bool(b) => Scalar::Bool(*b),
            // A NULL/unsupported key value can't satisfy a UNIQUE key — nothing to
            // probe (NodeKey's NOT-NULL half is enforced separately at the call site).
            _ => return Ok(false),
        };
        let base_filter = FilterExpr::all([
            FilterExpr::equals(probe_prop.as_str(), probe_scalar),
            FilterExpr::equals("op", Scalar::Int(0)),
        ]);
        let filter_expr = self.storage.apply_version_filter(base_filter);

        let batches = backend
            .scan(ScanRequest::all(&table_name).with_filter(filter_expr))
            .await?;

        // Distinct candidate eids from the narrowed scan.
        let mut candidates: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for batch in &batches {
            let Some(eid_col) = batch
                .column_by_name("eid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if !eid_col.is_null(row) {
                    candidates.insert(eid_col.value(row));
                }
            }
        }

        let exclude = exclude_eid.map(|e| e.as_u64());
        for raw in candidates {
            if Some(raw) == exclude {
                continue;
            }
            let Some(props) = self
                .fetch_all_edge_props_from_storage_with_hint(Eid::new(raw), Some(edge_type))
                .await?
            else {
                continue; // deleted / not live
            };
            // The candidate's *current* value of every key property must match.
            if key_values.iter().all(|(p, v)| props.get(p) == Some(v)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Batch load properties for multiple vertices
    pub async fn get_batch_vertex_props(
        &self,
        vids: &[Vid],
        properties: &[&str],
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Vid, Properties>> {
        let schema = self.schema_manager.schema();
        let mut result = HashMap::new();
        // Tracks vids seen as a per-label deletion tombstone, so the schemaless
        // main-table fallback below never resurrects a deleted vertex.
        let mut tombstoned: std::collections::HashSet<Vid> = std::collections::HashSet::new();
        // MVCC: highest `_version` seen per vid across the storage scan. Rows are
        // NOT guaranteed in version order, so we must rank — a stale older row
        // arriving after a newer one must not overwrite it (finding [7], the
        // batch analogue of the single-vid version-max fix).
        let mut best_version: HashMap<Vid, u64> = HashMap::new();
        // `_all_props` is a wildcard sentinel meaning "every property" — used by
        // schemaless projections that cannot enumerate names. It widens the
        // per-label column set, overflow merge, and L0 overlay below.
        let wants_all = properties.contains(&"_all_props");
        if vids.is_empty() {
            return Ok(result);
        }

        // In the new storage model, VIDs are pure auto-increment and don't embed label info.
        // We need to scan all label datasets to find the vertices.

        // Try VidLabelsIndex for O(1) label resolution
        let labels_to_scan: Vec<String> = {
            let mut needed: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut all_resolved = true;
            for &vid in vids {
                if let Some(labels) = self.storage.get_labels_from_index(vid) {
                    needed.extend(labels);
                } else {
                    all_resolved = false;
                    break;
                }
            }
            if all_resolved {
                needed.into_iter().collect()
            } else {
                schema.labels.keys().cloned().collect() // Fallback to full scan
            }
        };

        // 2. Fetch from storage - scan relevant label datasets
        for label_name in &labels_to_scan {
            // Filter to properties that exist in this label's schema. Under the
            // `_all_props` wildcard, request every declared column for the label.
            let label_schema_props = schema.properties.get(label_name);
            let valid_props: Vec<&str> = if wants_all {
                label_schema_props
                    .map(|props| props.keys().map(String::as_str).collect())
                    .unwrap_or_default()
            } else {
                properties
                    .iter()
                    .cloned()
                    .filter(|p| label_schema_props.is_some_and(|props| props.contains_key(*p)))
                    .collect()
            };
            // Note: don't skip when valid_props is empty; overflow_json may have the properties

            // A label resolved from the VidLabelsIndex (or the schema fallback)
            // may not have a per-label typed dataset — a schemaless label, or a
            // label whose typed table isn't visible in this (e.g. fork-scoped)
            // storage schema. Skip it gracefully rather than failing the whole
            // batch fetch, mirroring the `table_exists` skip just below. Its
            // properties, if any, come from the L0 overlay / main table instead.
            let ds = match self.storage.vertex_dataset(label_name) {
                Ok(ds) => ds,
                Err(_) => continue,
            };
            let backend = self.storage.backend();
            let vtable_name = ds.table_name();

            if !backend.table_exists(&vtable_name).await.unwrap_or(false) {
                continue; // Table doesn't exist yet — skip this label
            }

            let base_filter =
                FilterExpr::one_of("_vid", vids.iter().map(|v| Scalar::UInt(v.as_u64())));

            let final_filter = self.storage.apply_version_filter(base_filter);

            // Build column list for projection
            let mut columns: Vec<String> = Vec::with_capacity(valid_props.len() + 4);
            columns.push("_vid".to_string());
            columns.push("_version".to_string());
            columns.push("_deleted".to_string());
            columns.extend(valid_props.iter().map(|s| s.to_string()));
            // Add overflow_json to fetch non-schema properties
            columns.push("overflow_json".to_string());

            use crate::backend::types::ScanRequest;
            let request = ScanRequest::all(&vtable_name)
                .with_filter(final_filter)
                .with_columns(columns);

            let batches: Vec<RecordBatch> = match backend.scan(request).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        label = %label_name,
                        error = %e,
                        "failed to scan label table, skipping"
                    );
                    continue;
                }
            };
            for batch in batches {
                let vid_col = match batch
                    .column_by_name("_vid")
                    .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                {
                    Some(c) => c,
                    None => continue,
                };
                let del_col = match batch
                    .column_by_name("_deleted")
                    .and_then(|col| col.as_any().downcast_ref::<BooleanArray>())
                {
                    Some(c) => c,
                    None => continue,
                };
                let ver_col = batch
                    .column_by_name("_version")
                    .and_then(|col| col.as_any().downcast_ref::<UInt64Array>());

                for row in 0..batch.num_rows() {
                    let vid = Vid::from(vid_col.value(row));
                    let version = ver_col
                        .map(|c| if c.is_null(row) { 0 } else { c.value(row) })
                        .unwrap_or(0);

                    // Skip rows older than the newest already applied for this vid.
                    if best_version.get(&vid).is_some_and(|bv| version < *bv) {
                        continue;
                    }
                    best_version.insert(vid, version);

                    if del_col.value(row) {
                        result.remove(&vid);
                        tombstoned.insert(vid);
                        continue;
                    }

                    // A newer live row un-tombstones the vid.
                    tombstoned.remove(&vid);
                    let label_props = schema.properties.get(label_name);
                    let mut props =
                        Self::extract_row_properties(&batch, row, &valid_props, label_props)?;
                    Self::merge_overflow_into_props(&batch, row, properties, &mut props)?;
                    result.insert(vid, props);
                }
            }
        }

        // 2b. Schemaless main-table fallback: any requested vid with no per-label
        // row and no tombstone may store its properties in the main table's
        // `props_json`. Insert before the L0 overlay below so uncommitted edits
        // still take precedence.
        let missing: Vec<Vid> = vids
            .iter()
            .copied()
            .filter(|vid| !result.contains_key(vid) && !tombstoned.contains(vid))
            .collect();
        self.main_table_fallback(&missing, &mut result).await?;

        // 3. Overlay L0 buffers in age order: pending (oldest to newest) -> current -> transaction
        if let Some(ctx) = ctx {
            let mut l0_served = 0usize;
            // First, overlay pending flush L0s in order (oldest first, so iterate forward)
            for pending_l0_arc in &ctx.pending_flush_l0s {
                let pending_l0 = pending_l0_arc.read();
                l0_served += self.overlay_l0_batch(vids, &pending_l0, properties, &mut result)?;
            }

            // Then overlay current L0 (newer than pending)
            let l0 = ctx.l0.read();
            l0_served += self.overlay_l0_batch(vids, &l0, properties, &mut result)?;

            // Finally overlay transaction L0 (newest)
            // Skip transaction L0 if querying a snapshot
            // (Transaction changes are at current version, not in snapshot)
            if self.storage.version_high_water_mark().is_none()
                && let Some(tx_l0_arc) = &ctx.transaction_l0
            {
                let tx_l0 = tx_l0_arc.read();
                l0_served += self.overlay_l0_batch(vids, &tx_l0, properties, &mut result)?;
            }
            ctx.count_l0_rows(l0_served);
        }

        Ok(result)
    }

    /// Overlays one L0 buffer onto `result`, returning the number of vids it
    /// actually served.
    ///
    /// The return value is the property path's "rows served from L0" count. It
    /// counts vids whose properties were *taken* from this buffer — not vids
    /// merely asked about, and not tombstones or version-gated entries that were
    /// skipped — so a caller can distinguish an L0 that contributed from one that
    /// was consulted and had nothing to give.
    fn overlay_l0_batch(
        &self,
        vids: &[Vid],
        l0: &L0Buffer,
        properties: &[&str],
        result: &mut HashMap<Vid, Properties>,
    ) -> Result<usize> {
        let mut served = 0usize;
        let schema = self.schema_manager.schema();
        // `_all_props` is a wildcard sentinel: overlay every L0 property, not just
        // the named ones. Schemaless projections (`RETURN n`) request it because
        // they cannot enumerate property names up front.
        let wants_all = properties.contains(&"_all_props");
        for &vid in vids {
            // If deleted in L0, remove from result.
            if l0.vertex_tombstones.contains(&vid) {
                // Version-gate the tombstone exactly like the property branch
                // below: a deletion committed *after* the pinned snapshot (its
                // version is beyond the high-water mark) must not remove a
                // vertex that is still visible at the pinned version. Without
                // this gate a beyond-pin tombstone wrongly deletes a live row
                // under a version-pinned / time-travel read.
                let tombstone_version = l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                if self
                    .storage
                    .version_high_water_mark()
                    .is_some_and(|hwm| tombstone_version > hwm)
                {
                    continue;
                }
                result.remove(&vid);
                continue;
            }
            // If in L0, check version before merging
            if let Some(l0_props) = l0.vertex_properties.get(&vid) {
                // Skip entries beyond snapshot boundary
                let entry_version = l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                if self
                    .storage
                    .version_high_water_mark()
                    .is_some_and(|hwm| entry_version > hwm)
                {
                    continue;
                }

                let entry = result.entry(vid).or_default();
                // In new storage model, get labels from L0Buffer
                let labels = l0.get_vertex_labels(vid);

                for (k, v) in l0_props {
                    if wants_all || properties.contains(&k.as_str()) {
                        // Check if property is CRDT by looking up in any of the vertex's labels
                        let is_crdt = labels
                            .and_then(|label_list| {
                                label_list.iter().find_map(|ln| {
                                    schema
                                        .properties
                                        .get(ln)
                                        .and_then(|lp| lp.get(k))
                                        .filter(|pm| matches!(pm.r#type, DataType::Crdt(_)))
                                })
                            })
                            .is_some();

                        if is_crdt {
                            let existing = entry.entry(k.clone()).or_insert(Value::Null);
                            // `unwrap_or(v.clone())` discarded the merged CRDT
                            // state on failure and let the newer value win --
                            // the opposite default to the two sites above, for
                            // the same failure (#233).
                            *existing = self.merge_crdt_values(existing, v)?;
                        } else {
                            entry.insert(k.clone(), v.clone());
                        }
                    }
                }
                served += 1;
            }
        }
        Ok(served)
    }

    /// Load properties as Arrow columns for vectorized processing
    /// Batch load properties for multiple edges
    pub async fn get_batch_edge_props(
        &self,
        eids: &[uni_common::core::id::Eid],
        properties: &[&str],
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Vid, Properties>> {
        let schema = self.schema_manager.schema();
        let mut result = HashMap::new();
        if eids.is_empty() {
            return Ok(result);
        }
        // MVCC: highest `_version` seen per eid across the delta scan. Rows are
        // not version-ordered, so a stale older row must not overwrite a newer
        // one (finding [3]); without this, an out-of-order DELETE/live pair
        // resurrected a deleted edge's props depending on scan order.
        let mut best_version: HashMap<uni_common::core::id::Eid, u64> = HashMap::new();
        // Eids whose winning delta row is a delete. Gates the main-edges
        // fallback below so a tombstoned edge is not resurrected (C2).
        let mut tombstoned: std::collections::HashSet<uni_common::core::id::Eid> =
            std::collections::HashSet::new();

        // In the new storage model, EIDs are pure auto-increment and don't embed type info.
        // We need to scan all edge type datasets to find the edges.

        // Try to resolve edge types from L0 context for O(1) lookup
        let types_to_scan: Vec<String> = {
            if let Some(ctx) = ctx {
                let mut needed: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                let mut all_resolved = true;
                for &eid in eids {
                    if let Some(etype) = ctx.l0.read().get_edge_type(eid) {
                        needed.insert(etype.to_string());
                    } else {
                        all_resolved = false;
                        break;
                    }
                }
                if all_resolved {
                    needed.into_iter().collect()
                } else {
                    schema.edge_types.keys().cloned().collect() // Fallback to full scan
                }
            } else {
                schema.edge_types.keys().cloned().collect() // No context, full scan
            }
        };

        // 2. Fetch from storage (Delta runs) - scan relevant edge types
        for type_name in &types_to_scan {
            let type_props = schema.properties.get(type_name);
            let valid_props: Vec<&str> = properties
                .iter()
                .cloned()
                .filter(|p| type_props.is_some_and(|props| props.contains_key(*p)))
                .collect();
            // Note: don't skip when valid_props is empty; overflow_json may have the properties

            let delta_ds = match self.storage.delta_dataset(type_name, "fwd") {
                Ok(ds) => ds,
                Err(_) => continue,
            };
            let backend = self.storage.backend();
            let dtable_name = delta_ds.table_name();

            if !backend.table_exists(&dtable_name).await.unwrap_or(false) {
                continue; // Table doesn't exist yet — skip this edge type
            }

            let base_filter =
                FilterExpr::one_of("eid", eids.iter().map(|e| Scalar::UInt(e.as_u64())));

            let final_filter = self.storage.apply_version_filter(base_filter);

            // Build column list for projection
            let mut columns: Vec<String> = Vec::with_capacity(valid_props.len() + 4);
            columns.push("eid".to_string());
            columns.push("_version".to_string());
            columns.push("op".to_string());
            columns.extend(valid_props.iter().map(|s| s.to_string()));
            // Add overflow_json to fetch non-schema properties
            columns.push("overflow_json".to_string());

            use crate::backend::types::ScanRequest;
            let request = ScanRequest::all(&dtable_name)
                .with_filter(final_filter)
                .with_columns(columns);

            let batches: Vec<RecordBatch> = match backend.scan(request).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        edge_type = %type_name,
                        error = %e,
                        "failed to scan edge delta table, skipping"
                    );
                    continue;
                }
            };
            for batch in batches {
                let eid_col = match batch
                    .column_by_name("eid")
                    .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                {
                    Some(c) => c,
                    None => continue,
                };
                let op_col = match batch
                    .column_by_name("op")
                    .and_then(|col| col.as_any().downcast_ref::<arrow_array::UInt8Array>())
                {
                    Some(c) => c,
                    None => continue,
                };
                let ver_col = batch
                    .column_by_name("_version")
                    .and_then(|col| col.as_any().downcast_ref::<UInt64Array>());

                for row in 0..batch.num_rows() {
                    let eid = uni_common::core::id::Eid::from(eid_col.value(row));
                    let version = ver_col
                        .map(|c| if c.is_null(row) { 0 } else { c.value(row) })
                        .unwrap_or(0);

                    // Skip rows older than the newest already applied for this eid.
                    if best_version.get(&eid).is_some_and(|bv| version < *bv) {
                        continue;
                    }
                    best_version.insert(eid, version);

                    // op=1 is Delete
                    if op_col.value(row) == 1 {
                        result.remove(&Vid::from(eid.as_u64()));
                        // Record it, or the main-edges fallback below will
                        // re-hydrate the edge precisely *because* it is
                        // missing from `result` -- review finding C2, which
                        // the vertex path guards with its own `tombstoned`
                        // set and this one did not.
                        tombstoned.insert(eid);
                        continue;
                    }
                    // A newer live row un-deletes the edge.
                    tombstoned.remove(&eid);

                    let mut props =
                        Self::extract_row_properties(&batch, row, &valid_props, type_props)?;
                    Self::merge_overflow_into_props(&batch, row, properties, &mut props)?;
                    // Reuse Vid as key for compatibility with materialized_property
                    result.insert(Vid::from(eid.as_u64()), props);
                }
            }
        }

        // 3. Overlay L0 buffers in age order: pending (oldest to newest) -> current -> transaction
        if let Some(ctx) = ctx {
            // First, overlay pending flush L0s in order (oldest first, so iterate forward)
            for pending_l0_arc in &ctx.pending_flush_l0s {
                let pending_l0 = pending_l0_arc.read();
                self.overlay_l0_edge_batch(eids, &pending_l0, properties, &mut result)?;
            }

            // Then overlay current L0 (newer than pending)
            let l0 = ctx.l0.read();
            self.overlay_l0_edge_batch(eids, &l0, properties, &mut result)?;

            // Finally overlay transaction L0 (newest)
            // Skip transaction L0 if querying a snapshot
            // (Transaction changes are at current version, not in snapshot)
            if self.storage.version_high_water_mark().is_none()
                && let Some(tx_l0_arc) = &ctx.transaction_l0
            {
                let tx_l0 = tx_l0_arc.read();
                self.overlay_l0_edge_batch(eids, &tx_l0, properties, &mut result)?;
            }
        }

        // 4. Main-edges fallback — the delta tables are NOT the durable home of
        // edge properties.
        //
        // Adjacency compaction folds topology into the L2 table (which carries
        // only `src_vid`/`neighbors`/`edge_ids`) and then physically deletes the
        // delta rows it incorporated, on the stated invariant that "edge
        // properties survive in main_edges (dual-written during flush)". Both
        // sibling readers honour that invariant — the single-EID path and the
        // per-type batch path each fall back here — but this one did not, so
        // from the first compaction onward it returned nothing for every EID and
        // never recovered.
        //
        // The projection is the caller that made it visible: a missing property
        // becomes `NaN`, `edge_mask_window` compares `v >= lo && v <= hi` which
        // NaN fails under *any* window, so every masked traversal silently read
        // zero after a few hundred write transactions. Weighted algorithms
        // degraded at the same instant, defaulting to unit weights.
        //
        // Only unresolved EIDs reach the main-edges scan, so the fast path is
        // unchanged for edges whose properties are still in the delta runs.
        //
        // The misses are gathered first and resolved in **one** batched scan.
        // Resolving them one at a time is what made LDBC IC5 unanswerable: each
        // EID cost its own `ScanRequest` at ~1.5 ms, and because adjacency
        // compaction deletes the delta rows it folds into L2, every edge on a
        // compacted or reloaded store misses and pays it. A 4809-edge traversal
        // spent 7.3 s of 7.4 s in this block; IC5's ~1.6M edges extrapolated to
        // ~41 min against a 300 s budget.
        {
            use crate::storage::main_edge::MainEdgeDataset;

            let mut unresolved: Vec<uni_common::core::id::Eid> = Vec::new();
            for &eid in eids {
                // Skip both L0 deletes and delta-table deletes: either is a
                // tombstone the fallback must not undo (C2).
                if l0_visibility::is_edge_deleted(eid, ctx) || tombstoned.contains(&eid) {
                    continue;
                }
                // This map is keyed by Vid-from-Eid, matching the delta scan above.
                let key = uni_common::core::id::Vid::from(eid.as_u64());
                let missing_any = match result.get(&key) {
                    None => true,
                    Some(found) => properties.iter().any(|p| !found.contains_key(*p)),
                };
                if missing_any {
                    unresolved.push(eid);
                }
            }

            if !unresolved.is_empty() {
                let fetched = MainEdgeDataset::find_props_by_eids(
                    self.storage.backend(),
                    &unresolved,
                    self.storage.version_high_water_mark(),
                )
                .await?;
                for (eid, props) in fetched {
                    let key = uni_common::core::id::Vid::from(eid.as_u64());
                    let entry = result.entry(key).or_default();
                    for (k, v) in props {
                        entry.entry(k).or_insert(v);
                    }
                }
            }
        }

        Ok(result)
    }

    fn overlay_l0_edge_batch(
        &self,
        eids: &[uni_common::core::id::Eid],
        l0: &L0Buffer,
        properties: &[&str],
        result: &mut HashMap<Vid, Properties>,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();
        for &eid in eids {
            let vid_key = Vid::from(eid.as_u64());
            if l0.tombstones.contains_key(&eid) {
                result.remove(&vid_key);
                continue;
            }
            if let Some(l0_props) = l0.edge_properties.get(&eid) {
                // Skip entries beyond snapshot boundary
                let entry_version = l0.edge_versions.get(&eid).copied().unwrap_or(0);
                if self
                    .storage
                    .version_high_water_mark()
                    .is_some_and(|hwm| entry_version > hwm)
                {
                    continue;
                }

                let entry = result.entry(vid_key).or_default();
                // In new storage model, get edge type from L0Buffer
                let type_name = l0.get_edge_type(eid);

                let include_all = properties.contains(&"_all_props");
                for (k, v) in l0_props {
                    if include_all || properties.contains(&k.as_str()) {
                        // Check if property is CRDT
                        let is_crdt = type_name
                            .and_then(|tn| schema.properties.get(tn))
                            .and_then(|tp| tp.get(k))
                            .map(|pm| matches!(pm.r#type, DataType::Crdt(_)))
                            .unwrap_or(false);

                        if is_crdt {
                            let existing = entry.entry(k.clone()).or_insert(Value::Null);
                            // `unwrap_or(v.clone())` discarded the merged CRDT
                            // state on failure and let the newer value win --
                            // the opposite default to the two sites above, for
                            // the same failure (#233).
                            *existing = self.merge_crdt_values(existing, v)?;
                        } else {
                            entry.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Batch load labels for multiple vertices.
    pub async fn get_batch_labels(
        &self,
        vids: &[Vid],
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Vid, Vec<String>>> {
        let mut result = HashMap::new();
        if vids.is_empty() {
            return Ok(result);
        }

        // Phase 1: Get from L0 layers (oldest to newest).
        //
        // A plain write adds labels, so it unions; a `SET`/`REMOVE` label
        // *replaces* the set and records `vertex_label_overwrites`. Unioning
        // both alike ignored removals entirely -- `REMOVE n:Label` was a no-op
        // and the label came back on the next read. This mirrors
        // `columnar_scan::build_labels_column_for_known_label`, which already
        // gets the rule right: union for plain writes, newest overwrite wins.
        let mut overwritten: HashMap<Vid, Vec<String>> = HashMap::new();
        if let Some(ctx) = ctx {
            let mut collect_labels = |l0: &L0Buffer| {
                for &vid in vids {
                    let Some(labels) = l0.get_vertex_labels(vid) else {
                        continue;
                    };
                    if l0.vertex_label_overwrites.contains(&vid) {
                        // Newest wins: buffers are visited oldest to newest.
                        overwritten.insert(vid, labels.to_vec());
                    } else {
                        result
                            .entry(vid)
                            .or_default()
                            .extend(labels.iter().cloned());
                    }
                }
            };

            for l0_arc in &ctx.pending_flush_l0s {
                collect_labels(&l0_arc.read());
            }
            collect_labels(&ctx.l0.read());
            if let Some(tx_l0_arc) = &ctx.transaction_l0 {
                collect_labels(&tx_l0_arc.read());
            }
        }

        // Phase 2: Get from storage (try VidLabelsIndex first, then LanceDB fallback)
        let mut vids_needing_lancedb = Vec::new();

        /// Merge new labels into an existing label list, skipping duplicates.
        fn merge_labels(existing: &mut Vec<String>, new_labels: Vec<String>) {
            for l in new_labels {
                if !existing.contains(&l) {
                    existing.push(l);
                }
            }
        }

        for &vid in vids {
            // An overwrite is authoritative: the stored set is exactly what it
            // replaced, so merging it back would undo the removal.
            if overwritten.contains_key(&vid) {
                continue;
            }
            // Otherwise the stored set is still merged in even when L0 has
            // entries for this vid. Skipping it truncated a flushed
            // multi-label vertex to whatever labels one `SET n.prop` happened
            // to carry.
            if let Some(labels) = self.storage.get_labels_from_index(vid) {
                merge_labels(result.entry(vid).or_default(), labels);
            } else {
                vids_needing_lancedb.push(vid);
            }
        }

        // Fallback to storage backend for VIDs not in the index
        if !vids_needing_lancedb.is_empty() {
            let backend = self.storage.backend();
            let version = self.storage.version_high_water_mark();
            let storage_labels = MainVertexDataset::find_batch_labels_by_vids(
                backend,
                &vids_needing_lancedb,
                version,
            )
            .await?;

            for (vid, labels) in storage_labels {
                merge_labels(result.entry(vid).or_default(), labels);
            }
        }

        // Apply overwrites last: they replace whatever was accumulated.
        for (vid, labels) in overwritten {
            result.insert(vid, labels);
        }

        // Deduplicate and sort labels
        for labels in result.values_mut() {
            labels.sort();
            labels.dedup();
        }

        Ok(result)
    }

    pub async fn get_all_vertex_props(&self, vid: Vid) -> Result<Properties> {
        Ok(self
            .get_all_vertex_props_with_ctx(vid, None)
            .await?
            .unwrap_or_default())
    }

    pub async fn get_all_vertex_props_with_ctx(
        &self,
        vid: Vid,
        ctx: Option<&QueryContext>,
    ) -> Result<Option<Properties>> {
        // 1. Check if deleted in any L0 layer
        if l0_visibility::is_vertex_deleted(vid, ctx) {
            return Ok(None);
        }

        // 2. Accumulate properties from L0 layers (oldest to newest)
        let l0_props = l0_visibility::accumulate_vertex_props(vid, ctx);

        // 3. Fetch from storage
        let storage_props_opt = self.fetch_all_props_from_storage(vid).await?;

        // 4. Handle case where vertex doesn't exist in either layer
        if l0_props.is_none() && storage_props_opt.is_none() {
            return Ok(None);
        }

        let mut final_props = l0_props.unwrap_or_default();

        // 5. Merge storage properties (L0 takes precedence)
        if let Some(storage_props) = storage_props_opt {
            for (k, v) in storage_props {
                final_props.entry(k).or_insert(v);
            }
        }

        // 6. Normalize CRDT properties - convert JSON strings to JSON objects
        // In the new storage model, we need to get labels from context/L0
        if let Some(ctx) = ctx {
            // Try to get labels from L0 layers
            let labels = l0_visibility::get_vertex_labels(vid, ctx);
            for label in &labels {
                self.normalize_crdt_properties(&mut final_props, label)?;
            }
        }

        Ok(Some(final_props))
    }

    /// Batch-fetch properties for multiple vertices of a known label.
    ///
    /// Queries L0 layers in-memory, then fetches remaining VIDs from LanceDB in
    /// a single `_vid IN (...)` query on the label table. Much faster than
    /// per-vertex `get_all_vertex_props_with_ctx` when many vertices need loading.
    ///
    /// Fetches every columnar property. Callers that only need a subset (e.g. a
    /// vector/search procedure materializing `RETURN node.<prop>`) should prefer
    /// [`Self::get_batch_vertex_props_for_label_projected`] to avoid decoding
    /// unread heavy columns such as `List(Vector)` (issue #134).
    pub async fn get_batch_vertex_props_for_label(
        &self,
        vids: &[Vid],
        label: &str,
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Vid, Properties>> {
        self.get_batch_vertex_props_for_label_projected(vids, label, ctx, None)
            .await
    }

    /// Like [`Self::get_batch_vertex_props_for_label`], but restricts the LanceDB
    /// fetch to `requested_props` (plus the id/version/overflow bookkeeping
    /// columns) when `Some`. Unread columnar properties — notably heavy
    /// `List(Vector)` columns — are then never decoded (issue #134). `None`
    /// fetches all columnar properties, identical to the unprojected method.
    ///
    /// Requested names that are not declared columnar properties are ignored
    /// here; they are served from `overflow_json`, which is always fetched.
    pub async fn get_batch_vertex_props_for_label_projected(
        &self,
        vids: &[Vid],
        label: &str,
        ctx: Option<&QueryContext>,
        requested_props: Option<&[String]>,
    ) -> Result<HashMap<Vid, Properties>> {
        let mut result: HashMap<Vid, Properties> = HashMap::new();
        let mut need_storage: Vec<Vid> = Vec::new();

        // Phase 1: Check L0 layers for each VID (fast, in-memory).
        for &vid in vids {
            if l0_visibility::is_vertex_deleted(vid, ctx) {
                continue;
            }
            let l0_props = l0_visibility::accumulate_vertex_props(vid, ctx);
            // Skipping storage when L0 has the vid is only sound while L0 rows
            // are *complete* -- the invariant `insert_vertex_partial_full`
            // documents, and the default write path upholds by merging a full
            // map before staging.
            //
            // `partial_lance_writes` breaks it: `insert_vertex_partial` stages
            // only the touched keys, so the L0 row is a delta and the stored
            // properties are still the rest of the truth. Reading L0 alone
            // dropped them -- and since the SET prefetch merges over this map
            // and writes the result back, a dropped property was deleted, not
            // just missing from one read.
            let partial = l0_visibility::has_partial_vertex_keys(vid, ctx);
            match l0_props {
                Some(props) if !partial => {
                    result.insert(vid, props);
                }
                Some(props) => {
                    // Delta row: keep it, but read storage underneath. The L0
                    // values win on shared keys, applied after the storage
                    // merge below.
                    result.insert(vid, props);
                    need_storage.push(vid);
                }
                None => need_storage.push(vid),
            }
        }

        // If everything was resolved from L0, skip storage entirely.
        if need_storage.is_empty() {
            // Normalize CRDT properties for L0-resolved vertices.
            if ctx.is_some() {
                for props in result.values_mut() {
                    self.normalize_crdt_properties(props, label)?;
                }
            }
            return Ok(result);
        }

        // Phase 2: Batch-fetch from LanceDB for remaining VIDs.
        let schema = self.schema_manager.schema();
        let label_props = schema.properties.get(label);

        let mut prop_names: Vec<String> = Vec::new();
        if let Some(props) = label_props {
            prop_names = match requested_props {
                // Prune to the requested columnar props; any requested name that
                // is not a declared column is served from overflow_json below, so
                // dropping it here is safe and avoids decoding unread columns.
                Some(reqs) => reqs
                    .iter()
                    .filter(|r| props.contains_key(r.as_str()))
                    .cloned()
                    .collect(),
                None => props.keys().cloned().collect(),
            };
        }

        let mut columns: Vec<String> = vec![
            "_vid".to_string(),
            "_deleted".to_string(),
            "_version".to_string(),
        ];
        columns.extend(prop_names.iter().cloned());
        columns.push("overflow_json".to_string());

        // Build IN filter for all VIDs at once.
        let base_filter = FilterExpr::one_of(
            "_vid",
            need_storage.iter().map(|v| Scalar::UInt(v.as_u64())),
        );

        let filter_expr = self.storage.apply_version_filter(base_filter);

        let table_name = crate::backend::table_names::vertex_table_name(label);
        let batches: Vec<RecordBatch> = self
            .storage
            .backend()
            .scan(
                crate::backend::types::ScanRequest::all(&table_name)
                    .with_filter(filter_expr.clone())
                    .with_columns(columns.clone()),
            )
            .await?;

        let prop_name_refs: Vec<&str> = prop_names.iter().map(|s| s.as_str()).collect();

        // Track best version per VID for proper version-based merging.
        let mut per_vid_best_version: HashMap<Vid, u64> = HashMap::new();
        let mut per_vid_props: HashMap<Vid, Properties> = HashMap::new();

        for batch in batches {
            let vid_col = match batch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            {
                Some(c) => c,
                None => continue,
            };
            let deleted_col = match batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
            {
                Some(c) => c,
                None => continue,
            };
            let version_col = match batch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            {
                Some(c) => c,
                None => continue,
            };

            for row in 0..batch.num_rows() {
                let vid = Vid::from(vid_col.value(row));
                let version = version_col.value(row);

                if deleted_col.value(row) {
                    if per_vid_best_version
                        .get(&vid)
                        .is_none_or(|&best| version >= best)
                    {
                        per_vid_best_version.insert(vid, version);
                        per_vid_props.remove(&vid);
                    }
                    continue;
                }

                let mut current_props =
                    Self::extract_row_properties(&batch, row, &prop_name_refs, label_props)?;

                if let Some(overflow_props) = Self::extract_overflow_properties(&batch, row)? {
                    for (k, v) in overflow_props {
                        current_props.entry(k).or_insert(v);
                    }
                }

                let best = per_vid_best_version.get(&vid).copied();
                let mut best_opt = best;
                let mut merged = per_vid_props.remove(&vid);
                self.merge_versioned_props(
                    current_props,
                    version,
                    &mut best_opt,
                    &mut merged,
                    label_props,
                )?;
                if let Some(v) = best_opt {
                    per_vid_best_version.insert(vid, v);
                }
                if let Some(p) = merged {
                    per_vid_props.insert(vid, p);
                }
            }
        }

        // Merge storage results with any L0 partial props already in result.
        for (vid, storage_props) in per_vid_props {
            let entry = result.entry(vid).or_default();
            for (k, v) in storage_props {
                entry.entry(k).or_insert(v);
            }
        }

        // Phase 2b: schemaless main-table fallback. A `need_storage` vid that
        // produced no per-label verdict — neither a live row (present in `result`)
        // nor a tombstone (present in `per_vid_best_version`) — may be a schemaless
        // vertex whose properties live in the main table's `props_json` rather than
        // this label's Lance table. Hydrate those here, mirroring the single-VID
        // `MainVertexDataset::find_props_by_vid` fallback.
        let missing: Vec<Vid> = need_storage
            .iter()
            .copied()
            .filter(|vid| !result.contains_key(vid) && !per_vid_best_version.contains_key(vid))
            .collect();
        self.main_table_fallback(&missing, &mut result).await?;

        // Phase 3: Normalize CRDT properties.
        if ctx.is_some() {
            for props in result.values_mut() {
                self.normalize_crdt_properties(props, label)?;
            }
        }

        Ok(result)
    }

    /// Hydrate `missing` vids from the main (schemaless) vertex table into `out`.
    ///
    /// Batch counterpart of the single-VID `MainVertexDataset::find_props_by_vid`
    /// fallback used by `get_vertex_props`: vertices whose properties live in the
    /// main table's `props_json` (a schemaless label, or a label with no visible
    /// per-label typed dataset) have no row in any `vertices_<label>` table, so a
    /// per-label scan returns nothing. Callers pass only vids with no per-label
    /// verdict — neither a live row nor a tombstone — so a per-label deletion is
    /// never resurrected by an older main-table row. Existing entries in `out` are
    /// preserved (`or_insert`); the caller applies any L0 overlay afterwards.
    ///
    /// # Errors
    ///
    /// Returns an error if the main-table scan or its `props_json` decode fails.
    async fn main_table_fallback(
        &self,
        missing: &[Vid],
        out: &mut HashMap<Vid, Properties>,
    ) -> Result<()> {
        if missing.is_empty() {
            return Ok(());
        }
        let main_props = MainVertexDataset::find_batch_props_by_vids(
            self.storage.backend(),
            missing,
            self.storage.version_high_water_mark(),
        )
        .await?;
        for (vid, props) in main_props {
            out.entry(vid).or_insert(props);
        }
        Ok(())
    }

    /// Batch-fetch properties for multiple edges of a known type.
    ///
    /// Mirrors `get_batch_vertex_props_for_label` (above) for the edge path.
    /// Issues one `eid IN (...)` scan against the delta table for the edge
    /// type, replaying per-EID version history (op-replay + CRDT merge) the
    /// same way `fetch_all_edge_props_from_storage_with_hint` does. Far
    /// faster than per-edge `get_all_edge_props_with_ctx` when many edges
    /// of the same type need loading (e.g., batched SET/REMOVE on edges
    /// matched by a MATCH).
    ///
    /// EIDs of deleted edges or those with no rows in delta storage are
    /// omitted from the returned map; callers can fall back to the per-EID
    /// path for misses.
    pub async fn get_batch_edge_props_for_type(
        &self,
        eids: &[Eid],
        type_name: &str,
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Eid, Properties>> {
        use crate::backend::table_names;
        use crate::backend::types::ScanRequest;

        let mut result: HashMap<Eid, Properties> = HashMap::new();
        if eids.is_empty() {
            return Ok(result);
        }

        // Phase 1: L0 check per EID. Skip deleted; serve from L0 if it has
        // an accumulated property set; otherwise note for storage scan.
        let mut need_storage: Vec<Eid> = Vec::new();
        for &eid in eids {
            if l0_visibility::is_edge_deleted(eid, ctx) {
                continue;
            }
            let l0_props = l0_visibility::accumulate_edge_props(eid, ctx);
            // Edge L0 semantics: even an empty accumulator means "edge exists
            // in L0 with no user props yet" — we still go to storage to pick
            // up the persisted full row (mirrors get_all_edge_props_with_ctx).
            if let Some(props) = l0_props {
                result.insert(eid, props);
            }
            need_storage.push(eid);
        }

        if need_storage.is_empty() {
            return Ok(result);
        }

        // Phase 2: One scan with `eid IN (...)` on the delta table.
        let schema = self.schema_manager.schema();
        let type_props = schema.properties.get(type_name);

        if self.storage.delta_dataset(type_name, "fwd").is_err() {
            return Ok(result);
        }

        let table_name = table_names::delta_table_name(type_name, "fwd");
        let backend = self.storage.backend();
        if !backend.table_exists(&table_name).await.unwrap_or(false) {
            return Ok(result);
        }

        let base_filter =
            FilterExpr::one_of("eid", need_storage.iter().map(|e| Scalar::UInt(e.as_u64())));
        let filter_expr = self.storage.apply_version_filter(base_filter);

        let batches = match backend
            .scan(ScanRequest::all(&table_name).with_filter(filter_expr))
            .await
        {
            Ok(b) => b,
            Err(_) => return Ok(result), // Storage error: per-row fallback handles correctness
        };

        // Collect (eid, version, op, props) tuples, then group + replay per EID.
        let mut per_eid_rows: HashMap<Eid, Vec<(u64, u8, Properties)>> = HashMap::new();
        for batch in batches {
            let eid_col = match batch
                .column_by_name("eid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            {
                Some(c) => c,
                None => continue,
            };
            let op_col = match batch
                .column_by_name("op")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt8Array>())
            {
                Some(c) => c,
                None => continue,
            };
            let ver_col = match batch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            {
                Some(c) => c,
                None => continue,
            };

            for row in 0..batch.num_rows() {
                let eid = Eid::from(eid_col.value(row));
                let ver = ver_col.value(row);
                let op = op_col.value(row);
                let mut props = Properties::new();

                if op != 1
                    && let Some(tp) = type_props
                {
                    for (p_name, p_meta) in tp {
                        if let Some(col) = batch.column_by_name(p_name)
                            && !col.is_null(row)
                        {
                            let val = Self::value_from_column(col.as_ref(), &p_meta.r#type, row)?;
                            props.insert(p_name.clone(), val);
                        }
                    }
                }
                per_eid_rows.entry(eid).or_default().push((ver, op, props));
            }
        }

        // Eids whose replay ends in a delete; gates the fallback below (C2).
        let mut replay_tombstoned: std::collections::HashSet<uni_common::core::id::Eid> =
            std::collections::HashSet::new();
        for (eid, mut rows) in per_eid_rows {
            rows.sort_by_key(|(ver, _, _)| *ver);

            let mut merged_props: Properties = Properties::new();
            let mut is_deleted = false;

            for (_, op, props) in rows {
                if op == 1 {
                    is_deleted = true;
                    merged_props.clear();
                } else {
                    is_deleted = false;
                    for (p_name, p_val) in props {
                        let is_crdt = type_props
                            .and_then(|tp| tp.get(&p_name))
                            .map(|pm| matches!(pm.r#type, DataType::Crdt(_)))
                            .unwrap_or(false);
                        if is_crdt {
                            if let Some(existing) = merged_props.get(&p_name) {
                                // A failed merge used to drop `p_val` and keep the
                                // older value, silently. Two other sites resolved
                                // the same failure the opposite way; propagating
                                // removes the choice rather than picking a winner
                                // nobody has the information to pick (#233).
                                let merged = self.merge_crdt_values(existing, &p_val)?;
                                merged_props.insert(p_name, merged);
                            } else {
                                merged_props.insert(p_name, p_val);
                            }
                        } else {
                            merged_props.insert(p_name, p_val);
                        }
                    }
                }
            }

            if is_deleted {
                // Deleted in storage; remove any L0 accumulation that may
                // have been recorded under this EID by Phase 1 (matches
                // is_edge_deleted single-EID semantics).
                result.remove(&eid);
                // And record it, or the main-edges fallback below re-hydrates
                // the edge *because* its entry is now missing -- review
                // finding C2, which the vertex path guards and this one did
                // not.
                replay_tombstoned.insert(eid);
                continue;
            }

            // L0 takes precedence over storage for shared keys; insert
            // storage values only where L0 did not already provide them.
            let entry = result.entry(eid).or_default();
            for (k, v) in merged_props {
                entry.entry(k).or_insert(v);
            }
        }

        // Schemaless / overflow edge props live in the main edges table's
        // `props_json`, NOT in the per-type delta columns — so the delta replay
        // above recovers only typed columns and leaves a schemaless edge (or any
        // prop absent from the type schema) empty here. Mirror the single-EID
        // `fetch_all_edge_props_from_storage` fallback: for any requested EID
        // still unresolved (no entry, or an empty one) fall back to the main
        // edges table. Without this, a fork SET/REMOVE on an inherited schemaless
        // relationship read an empty prefetch and wiped the edge's untouched
        // properties (#102). Only misses pay the per-EID lookup, so the batch
        // fast-path is preserved for fully-typed edges.
        //
        // The misses are gathered first and resolved in one batched scan, for
        // the reason given on the sibling fallback in `get_batch_edge_props`:
        // an entirely schemaless relationship type misses on *every* EID, so a
        // per-EID lookup here is a full `ScanRequest` per edge.
        use crate::storage::main_edge::MainEdgeDataset;

        let mut unresolved: Vec<uni_common::core::id::Eid> = Vec::new();
        for &eid in eids {
            // Skip both L0 deletes and edges whose replay ended in a delete:
            // either is a tombstone the fallback must not undo (C2).
            if l0_visibility::is_edge_deleted(eid, ctx) || replay_tombstoned.contains(&eid) {
                continue;
            }
            if result.get(&eid).is_none_or(|p| p.is_empty()) {
                unresolved.push(eid);
            }
        }

        if !unresolved.is_empty() {
            let fetched = MainEdgeDataset::find_props_by_eids(
                self.storage.backend(),
                &unresolved,
                self.storage.version_high_water_mark(),
            )
            .await?;
            for (eid, props) in fetched {
                let entry = result.entry(eid).or_default();
                for (k, v) in props {
                    entry.entry(k).or_insert(v);
                }
            }
        }

        Ok(result)
    }

    /// Normalize CRDT properties by converting JSON strings to JSON objects.
    /// This handles the case where CRDT values come from Cypher CREATE statements
    /// as `Value::String("{\"t\": \"gc\", ...}")` and need to be parsed into objects.
    fn normalize_crdt_properties(&self, props: &mut Properties, label: &str) -> Result<()> {
        let schema = self.schema_manager.schema();
        let label_props = match schema.properties.get(label) {
            Some(p) => p,
            None => return Ok(()),
        };

        for (prop_name, prop_meta) in label_props {
            if let DataType::Crdt(_) = prop_meta.r#type
                && let Some(val) = props.get_mut(prop_name)
            {
                *val = Value::from(Self::parse_crdt_value(val)?);
            }
        }

        Ok(())
    }

    /// Extract properties from a single batch row.
    fn extract_row_properties(
        batch: &RecordBatch,
        row: usize,
        prop_names: &[&str],
        label_props: Option<&HashMap<String, uni_common::core::schema::PropertyMeta>>,
    ) -> Result<Properties> {
        let mut props = Properties::new();
        for name in prop_names {
            let col = match batch.column_by_name(name) {
                Some(col) => col,
                None => continue,
            };
            if col.is_null(row) {
                continue;
            }
            if let Some(prop_meta) = label_props.and_then(|p| p.get(*name)) {
                let val = Self::value_from_column(col.as_ref(), &prop_meta.r#type, row)?;
                props.insert((*name).to_string(), val);
            }
        }
        Ok(props)
    }

    /// Extract overflow properties from the overflow_json column.
    ///
    /// Returns None if the column doesn't exist or the value is null,
    /// otherwise parses the JSON blob and returns the properties.
    fn extract_overflow_properties(batch: &RecordBatch, row: usize) -> Result<Option<Properties>> {
        use arrow_array::LargeBinaryArray;

        let overflow_col = match batch.column_by_name("overflow_json") {
            Some(col) => col,
            None => return Ok(None), // Column doesn't exist (old schema)
        };

        if overflow_col.is_null(row) {
            return Ok(None);
        }

        let binary_array = overflow_col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| anyhow!("overflow_json is not LargeBinaryArray"))?;

        let jsonb_bytes = binary_array.value(row);

        // Decode the CypherValue blob directly to `Value`. Routing through
        // `serde_json` would stringify temporal values (and is unnecessary —
        // the blob already decodes to a `Value::Map`).
        match uni_common::cypher_value_codec::decode(jsonb_bytes)
            .map_err(|e| anyhow!("Failed to decode CypherValue: {}", e))?
        {
            Value::Map(map) => Ok(Some(map)),
            Value::Null => Ok(None),
            other => Err(anyhow!(
                "overflow_json decoded to a non-map value: {other:?}"
            )),
        }
    }

    /// Merge overflow properties from the overflow_json column into an existing props map.
    ///
    /// Handles two concerns:
    /// 1. If `overflow_json` is explicitly requested in `properties`, stores the raw JSONB
    ///    bytes as a JSON array of u8 values.
    /// 2. Extracts individual overflow properties and merges those that are in `properties`.
    fn merge_overflow_into_props(
        batch: &RecordBatch,
        row: usize,
        properties: &[&str],
        props: &mut Properties,
    ) -> Result<()> {
        use arrow_array::LargeBinaryArray;

        let overflow_col = match batch.column_by_name("overflow_json") {
            Some(col) if !col.is_null(row) => col,
            _ => return Ok(()),
        };

        // Store raw JSONB bytes if explicitly requested
        if properties.contains(&"overflow_json")
            && let Some(binary_array) = overflow_col.as_any().downcast_ref::<LargeBinaryArray>()
        {
            let jsonb_bytes = binary_array.value(row);
            let bytes_list: Vec<Value> =
                jsonb_bytes.iter().map(|&b| Value::Int(b as i64)).collect();
            props.insert("overflow_json".to_string(), Value::List(bytes_list));
        }

        // Extract and merge individual overflow properties. `_all_props` is a
        // wildcard sentinel: merge every overflow property, not just named ones.
        let wants_all = properties.contains(&"_all_props");
        if let Some(overflow_props) = Self::extract_overflow_properties(batch, row)? {
            for (k, v) in overflow_props {
                if wants_all || properties.contains(&k.as_str()) {
                    props.entry(k).or_insert(v);
                }
            }
        }

        Ok(())
    }

    /// Merge CRDT properties from source into target.
    fn merge_crdt_into(
        &self,
        target: &mut Properties,
        source: Properties,
        label_props: Option<&HashMap<String, uni_common::core::schema::PropertyMeta>>,
        crdt_only: bool,
    ) -> Result<()> {
        for (k, v) in source {
            if let Some(prop_meta) = label_props.and_then(|p| p.get(&k)) {
                if let DataType::Crdt(_) = prop_meta.r#type {
                    let existing_v = target.entry(k).or_insert(Value::Null);
                    *existing_v = self.merge_crdt_values(existing_v, &v)?;
                } else if !crdt_only {
                    target.insert(k, v);
                }
            }
        }
        Ok(())
    }

    /// Handle version-based property merging for storage fetch.
    fn merge_versioned_props(
        &self,
        current_props: Properties,
        version: u64,
        best_version: &mut Option<u64>,
        best_props: &mut Option<Properties>,
        label_props: Option<&HashMap<String, uni_common::core::schema::PropertyMeta>>,
    ) -> Result<()> {
        if best_version.is_none_or(|best| version > best) {
            // Newest version: strictly newer
            if let Some(mut existing_props) = best_props.take() {
                // Merge CRDTs from existing into current
                let mut merged = current_props;
                for (k, v) in merged.iter_mut() {
                    if let Some(prop_meta) = label_props.and_then(|p| p.get(k))
                        && let DataType::Crdt(_) = prop_meta.r#type
                        && let Some(existing_val) = existing_props.remove(k)
                    {
                        *v = self.merge_crdt_values(v, &existing_val)?;
                    }
                }
                *best_props = Some(merged);
            } else {
                *best_props = Some(current_props);
            }
            *best_version = Some(version);
        } else if Some(version) == *best_version {
            // Same version: merge all properties
            if let Some(existing_props) = best_props.as_mut() {
                self.merge_crdt_into(existing_props, current_props, label_props, false)?;
            } else {
                *best_props = Some(current_props);
            }
        } else {
            // Older version: only merge CRDTs
            if let Some(existing_props) = best_props.as_mut() {
                self.merge_crdt_into(existing_props, current_props, label_props, true)?;
            }
        }
        Ok(())
    }

    async fn fetch_all_props_from_storage(&self, vid: Vid) -> Result<Option<Properties>> {
        // In the new storage model, VID doesn't embed label info.
        // We need to scan all label datasets to find the vertex's properties.
        let schema = self.schema_manager.schema();
        let mut merged_props: Option<Properties> = None;
        let mut global_best_version: Option<u64> = None;

        // Try VidLabelsIndex for O(1) label resolution
        let label_names: Vec<String> = if let Some(labels) = self.storage.get_labels_from_index(vid)
        {
            labels
        } else {
            schema.labels.keys().cloned().collect() // Fallback to full scan
        };

        for label_name in &label_names {
            let label_props = schema.properties.get(label_name);

            // Get property names from schema
            let mut prop_names: Vec<String> = Vec::new();
            if let Some(props) = label_props {
                prop_names = props.keys().cloned().collect();
            }

            // Build column selection
            let mut columns: Vec<String> = vec!["_deleted".to_string(), "_version".to_string()];
            columns.extend(prop_names.iter().cloned());
            // Add overflow_json column to fetch non-schema properties
            columns.push("overflow_json".to_string());

            // Query using backend scan API
            let base_filter = FilterExpr::equals("_vid", Scalar::UInt(vid.as_u64()));

            let filter_expr = self.storage.apply_version_filter(base_filter);

            let table_name = crate::backend::table_names::vertex_table_name(label_name);
            let batches: Vec<RecordBatch> = match self
                .storage
                .backend()
                .scan(
                    crate::backend::types::ScanRequest::all(&table_name)
                        .with_filter(filter_expr.clone())
                        .with_columns(columns.clone()),
                )
                .await
            {
                Ok(b) => b,
                Err(_) => continue,
            };

            // Convert Vec<String> to Vec<&str> for downstream use
            let prop_name_refs: Vec<&str> = prop_names.iter().map(|s| s.as_str()).collect();

            for batch in batches {
                let deleted_col = match batch
                    .column_by_name("_deleted")
                    .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                {
                    Some(c) => c,
                    None => continue,
                };
                let version_col = match batch
                    .column_by_name("_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                {
                    Some(c) => c,
                    None => continue,
                };

                for row in 0..batch.num_rows() {
                    let version = version_col.value(row);

                    if deleted_col.value(row) {
                        if global_best_version.is_none_or(|best| version >= best) {
                            global_best_version = Some(version);
                            merged_props = None;
                        }
                        continue;
                    }

                    let mut current_props =
                        Self::extract_row_properties(&batch, row, &prop_name_refs, label_props)?;

                    // Also extract overflow properties from overflow_json column
                    if let Some(overflow_props) = Self::extract_overflow_properties(&batch, row)? {
                        // Merge overflow properties into current_props
                        for (k, v) in overflow_props {
                            current_props.entry(k).or_insert(v);
                        }
                    }

                    self.merge_versioned_props(
                        current_props,
                        version,
                        &mut global_best_version,
                        &mut merged_props,
                        label_props,
                    )?;
                }
            }
        }

        // Fallback to main table props_json for unknown/schemaless labels.
        // Gated on "no per-label verdict" (neither a live row nor a tombstone
        // was seen), so a per-label deletion tombstone is never overridden by
        // an older main-table row.
        if merged_props.is_none()
            && global_best_version.is_none()
            && let Some(main_props) = MainVertexDataset::find_props_by_vid(
                self.storage.backend(),
                vid,
                self.storage.version_high_water_mark(),
            )
            .await?
        {
            return Ok(Some(main_props));
        }

        Ok(merged_props)
    }

    pub async fn get_vertex_prop(&self, vid: Vid, prop: &str) -> Result<Value> {
        self.get_vertex_prop_with_ctx(vid, prop, None).await
    }

    #[instrument(skip(self, ctx), level = "trace")]
    pub async fn get_vertex_prop_with_ctx(
        &self,
        vid: Vid,
        prop: &str,
        ctx: Option<&QueryContext>,
    ) -> Result<Value> {
        // 1. Check if deleted in any L0 layer
        if l0_visibility::is_vertex_deleted(vid, ctx) {
            return Ok(Value::Null);
        }

        // 2. Determine if property is CRDT type
        // First check labels from context/L0, then fall back to scanning all labels in schema
        let schema = self.schema_manager.schema();
        let labels = ctx
            .map(|c| l0_visibility::get_vertex_labels(vid, c))
            .unwrap_or_default();

        let is_crdt = if !labels.is_empty() {
            // Check labels from context
            labels.iter().any(|ln| {
                schema
                    .properties
                    .get(ln)
                    .and_then(|lp| lp.get(prop))
                    .map(|pm| matches!(pm.r#type, DataType::Crdt(_)))
                    .unwrap_or(false)
            })
        } else {
            // No labels from context - check if property is CRDT in ANY label
            schema.properties.values().any(|label_props| {
                label_props
                    .get(prop)
                    .map(|pm| matches!(pm.r#type, DataType::Crdt(_)))
                    .unwrap_or(false)
            })
        };

        // 3. Check L0 chain for property
        if is_crdt {
            // For CRDT, accumulate and merge values from all L0 layers
            let l0_val = self.accumulate_crdt_from_l0(vid, prop, ctx)?;
            return self.finalize_crdt_lookup(vid, prop, l0_val).await;
        }

        // 4. Non-CRDT: Check L0 chain for property (returns first found)
        if let Some(val) = l0_visibility::lookup_vertex_prop(vid, prop, ctx) {
            return Ok(val);
        }

        // 5. Check Cache (if enabled)
        if let Some(ref cache) = self.vertex_cache {
            let mut cache = cache.lock().await;
            if let Some(val) = cache.get(&(vid, prop.to_string())) {
                debug!(vid = ?vid, prop, "Cache HIT");
                metrics::counter!("uni_property_cache_hits_total", "type" => "vertex").increment(1);
                return Ok(val.clone());
            } else {
                debug!(vid = ?vid, prop, "Cache MISS");
                metrics::counter!("uni_property_cache_misses_total", "type" => "vertex")
                    .increment(1);
            }
        }

        // 6. Fetch from Storage
        let storage_val = self.fetch_prop_from_storage(vid, prop).await?;

        // 7. Update Cache (if enabled)
        if let Some(ref cache) = self.vertex_cache {
            let mut cache = cache.lock().await;
            cache.put((vid, prop.to_string()), storage_val.clone());
        }

        Ok(storage_val)
    }

    /// Accumulate CRDT values from all L0 layers by merging them together.
    fn accumulate_crdt_from_l0(
        &self,
        vid: Vid,
        prop: &str,
        ctx: Option<&QueryContext>,
    ) -> Result<Value> {
        let mut merged = Value::Null;
        // The visitor returns `bool` rather than a `Result`, so a failed merge
        // used to be skipped and the accumulator returned as if that layer had
        // not existed — a silently short CRDT value. The error is carried out of
        // the closure instead, and visiting stops at the first one: continuing
        // would keep folding layers onto an accumulator already known to be
        // wrong (#233).
        let mut failure: Option<anyhow::Error> = None;
        l0_visibility::visit_l0_buffers(ctx, |l0| {
            if let Some(props) = l0.vertex_properties.get(&vid)
                && let Some(val) = props.get(prop)
            {
                match self.merge_crdt_values(&merged, val) {
                    Ok(new_merged) => merged = new_merged,
                    Err(e) => {
                        failure = Some(e);
                        return true; // Stop visiting
                    }
                }
            }
            false // Continue visiting all layers
        });
        if let Some(e) = failure {
            return Err(e);
        }
        Ok(merged)
    }

    /// Finalize CRDT lookup by merging with cache/storage.
    async fn finalize_crdt_lookup(&self, vid: Vid, prop: &str, l0_val: Value) -> Result<Value> {
        // Check Cache (if enabled)
        let cached_val = if let Some(ref cache) = self.vertex_cache {
            let mut cache = cache.lock().await;
            cache.get(&(vid, prop.to_string())).cloned()
        } else {
            None
        };

        if let Some(val) = cached_val {
            let merged = self.merge_crdt_values(&val, &l0_val)?;
            return Ok(merged);
        }

        // Fetch from Storage
        let storage_val = self.fetch_prop_from_storage(vid, prop).await?;

        // Update Cache (if enabled)
        if let Some(ref cache) = self.vertex_cache {
            let mut cache = cache.lock().await;
            cache.put((vid, prop.to_string()), storage_val.clone());
        }

        // Merge L0 + Storage
        self.merge_crdt_values(&storage_val, &l0_val)
    }

    async fn fetch_prop_from_storage(&self, vid: Vid, prop: &str) -> Result<Value> {
        // In the new storage model, VID doesn't embed label info.
        // We need to scan all label datasets to find the property.
        let schema = self.schema_manager.schema();
        let mut best_version: Option<u64> = None;
        let mut best_value: Option<Value> = None;

        // Try VidLabelsIndex for O(1) label resolution
        let label_names: Vec<String> = if let Some(labels) = self.storage.get_labels_from_index(vid)
        {
            labels
        } else {
            schema.labels.keys().cloned().collect() // Fallback to full scan
        };

        for label_name in &label_names {
            // Check if property is defined in schema for this label
            let prop_meta = schema
                .properties
                .get(label_name)
                .and_then(|props| props.get(prop));

            // Even if property is not in schema, we still check overflow_json

            // Query using backend scan API
            let base_filter = FilterExpr::equals("_vid", Scalar::UInt(vid.as_u64()));

            let filter_expr = self.storage.apply_version_filter(base_filter);

            // Always request metadata columns and overflow_json
            let mut columns = vec![
                "_deleted".to_string(),
                "_version".to_string(),
                "overflow_json".to_string(),
            ];

            // Only request the property column if it's defined in schema
            if prop_meta.is_some() {
                columns.push(prop.to_string());
            }

            let table_name = crate::backend::table_names::vertex_table_name(label_name);
            let batches: Vec<RecordBatch> = match self
                .storage
                .backend()
                .scan(
                    crate::backend::types::ScanRequest::all(&table_name)
                        .with_filter(filter_expr.clone())
                        .with_columns(columns),
                )
                .await
            {
                Ok(b) => b,
                Err(_) => continue,
            };

            for batch in batches {
                let deleted_col = match batch
                    .column_by_name("_deleted")
                    .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                {
                    Some(c) => c,
                    None => continue,
                };
                let version_col = match batch
                    .column_by_name("_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                {
                    Some(c) => c,
                    None => continue,
                };
                for row in 0..batch.num_rows() {
                    let version = version_col.value(row);

                    if deleted_col.value(row) {
                        if best_version.is_none_or(|best| version >= best) {
                            best_version = Some(version);
                            best_value = None;
                        }
                        continue;
                    }

                    // First try schema column if property is in schema
                    let mut val = None;
                    if let Some(meta) = prop_meta
                        && let Some(col) = batch.column_by_name(prop)
                    {
                        val = Some(if col.is_null(row) {
                            Value::Null
                        } else {
                            Self::value_from_column(col, &meta.r#type, row)?
                        });
                    }

                    // If not in schema column, check overflow_json
                    if val.is_none()
                        && let Some(overflow_props) =
                            Self::extract_overflow_properties(&batch, row)?
                        && let Some(overflow_val) = overflow_props.get(prop)
                    {
                        val = Some(overflow_val.clone());
                    }

                    // If we found a value (from schema or overflow), merge it
                    if let Some(v) = val {
                        if let Some(meta) = prop_meta {
                            // Use schema type for merging (handles CRDT)
                            self.merge_prop_value(
                                v,
                                version,
                                &meta.r#type,
                                &mut best_version,
                                &mut best_value,
                            )?;
                        } else {
                            // Overflow property: use simple LWW merging
                            if best_version.is_none_or(|best| version >= best) {
                                best_version = Some(version);
                                best_value = Some(v);
                            }
                        }
                    }
                }
            }
        }

        // Fallback to main-table props_json for unknown/schemaless labels —
        // their rows have no per-label table at all (mirrors
        // `fetch_all_props_from_storage`). Gated on "no per-label verdict"
        // (neither a live value nor a tombstone was seen), so a per-label
        // tombstone is never overridden by an older main-table row.
        if best_value.is_none()
            && best_version.is_none()
            && let Some(main_props) = MainVertexDataset::find_props_by_vid(
                self.storage.backend(),
                vid,
                self.storage.version_high_water_mark(),
            )
            .await?
        {
            return Ok(main_props.get(prop).cloned().unwrap_or(Value::Null));
        }

        Ok(best_value.unwrap_or(Value::Null))
    }

    /// Decode an Arrow column value with strict CRDT error handling.
    pub fn value_from_column(col: &dyn Array, data_type: &DataType, row: usize) -> Result<Value> {
        crate::storage::value_codec::decode_column_value(
            col,
            data_type,
            row,
            CrdtDecodeMode::Strict,
        )
    }

    /// Merge two `Value`-wrapped CRDT operands.
    ///
    /// Routes through [`uni_crdt::Crdt::merge_via_registry`] using the
    /// `PropertyManager`'s `plugin_registry`. With an empty registry
    /// (the legacy 3-arg [`Self::new`] default) `merge_via_registry`
    /// falls back to `Crdt::try_merge`, preserving native semantics.
    ///
    /// # Errors
    ///
    /// Returns an `anyhow::Error` when either operand is malformed
    /// CRDT JSON, the variants disagree, or the registry-dispatched
    /// merge surfaces a `CrdtError`.
    pub fn merge_crdt_values(&self, a: &Value, b: &Value) -> Result<Value> {
        // Handle the case where values are JSON strings containing CRDT JSON
        // (this happens when values come from Cypher CREATE statements)
        // Parse before checking for null to ensure proper format conversion
        if a.is_null() {
            return Self::parse_crdt_value(b).map(Value::from);
        }
        if b.is_null() {
            return Self::parse_crdt_value(a).map(Value::from);
        }

        let a_parsed = Self::parse_crdt_value(a)?;
        let b_parsed = Self::parse_crdt_value(b)?;

        let mut crdt_a: Crdt = serde_json::from_value(a_parsed)?;
        let crdt_b: Crdt = serde_json::from_value(b_parsed)?;
        // M10 follow-up: route through `merge_via_registry` so a
        // hot-reloaded `CrdtKindProvider` plugin can intercept the
        // merge. With an empty registry (the 3-arg `new()` default)
        // this falls back to `Crdt::try_merge`, preserving prior
        // behavior bit-for-bit.
        crdt_a
            .merge_via_registry(&crdt_b, &self.plugin_registry)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(Value::from(serde_json::to_value(crdt_a)?))
    }

    /// Parse a CRDT value that may be either a JSON object or a JSON string containing JSON.
    /// Returns `serde_json::Value` for internal CRDT processing.
    fn parse_crdt_value(val: &Value) -> Result<serde_json::Value> {
        if let Value::String(s) = val {
            // Value is a JSON string - parse the string content as JSON
            serde_json::from_str(s).map_err(|e| anyhow!("Failed to parse CRDT JSON string: {}", e))
        } else {
            // Convert uni_common::Value to serde_json::Value for CRDT processing
            Ok(serde_json::Value::from(val.clone()))
        }
    }

    /// Merge a property value based on version, handling CRDT vs LWW semantics.
    fn merge_prop_value(
        &self,
        val: Value,
        version: u64,
        data_type: &DataType,
        best_version: &mut Option<u64>,
        best_value: &mut Option<Value>,
    ) -> Result<()> {
        if let DataType::Crdt(_) = data_type {
            self.merge_crdt_prop_value(val, version, best_version, best_value)
        } else {
            // Standard LWW
            if best_version.is_none_or(|best| version >= best) {
                *best_version = Some(version);
                *best_value = Some(val);
            }
            Ok(())
        }
    }

    /// Merge CRDT property values across versions (CRDTs merge regardless of version).
    fn merge_crdt_prop_value(
        &self,
        val: Value,
        version: u64,
        best_version: &mut Option<u64>,
        best_value: &mut Option<Value>,
    ) -> Result<()> {
        if best_version.is_none_or(|best| version > best) {
            // Newer version: merge with existing if present
            if let Some(existing) = best_value.take() {
                *best_value = Some(self.merge_crdt_values(&val, &existing)?);
            } else {
                *best_value = Some(val);
            }
            *best_version = Some(version);
        } else if Some(version) == *best_version {
            // Same version: merge
            let existing = best_value.get_or_insert(Value::Null);
            *existing = self.merge_crdt_values(existing, &val)?;
        } else {
            // Older version: still merge for CRDTs
            if let Some(existing) = best_value.as_mut() {
                *existing = self.merge_crdt_values(existing, &val)?;
            }
        }
        Ok(())
    }
}
