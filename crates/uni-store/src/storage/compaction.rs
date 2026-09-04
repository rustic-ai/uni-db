// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::storage::delta::{ENTRY_SIZE_ESTIMATE, L1Entry, Op};
use crate::storage::manager::StorageManager;
use anyhow::{Result, anyhow};
use arrow_array::Array;
use arrow_array::builder::{ListBuilder, UInt64Builder};
use arrow_array::{
    LargeBinaryArray, ListArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use metrics;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{error, info, instrument};
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::DataType;
use uni_common::{Properties, Value};
use uni_crdt::Crdt;

pub struct Compactor {
    storage: Arc<StorageManager>,
}

impl Compactor {
    pub fn new(storage: Arc<StorageManager>) -> Self {
        Self { storage }
    }

    #[instrument(skip(self), level = "info")]
    pub async fn compact_all(&self) -> Result<SemanticCompactionReport> {
        let start = std::time::Instant::now();
        let schema = self.storage.schema_manager().schema();
        let mut report = SemanticCompactionReport::default();

        // Compact Vertices
        for label in schema.labels.keys() {
            info!("Compacting vertices for label {}", label);
            match self.compact_vertices(label).await {
                Ok(v) if v.ran => {
                    report.vertex_passes += 1;
                    report.crdt_merges += v.crdt_merges;
                }
                Ok(_) => {}
                Err(e) => error!("Failed to compact vertices for {}: {}", label, e),
            }

            // Crash window: some labels' vertex tables have been replaced and
            // the rest have not, and no adjacency has been compacted at all
            // (that phase runs after this loop). A vertex carrying two labels
            // therefore lives in one compacted table and one uncompacted table
            // simultaneously.
            //
            // Recovery duty: both label anchors must agree about it. The
            // tombstone fan-out writes a tombstone into EVERY label's table, and
            // this pass physically drops tombstones — so one anchor can lose the
            // evidence of a delete while the other keeps it, and
            // `find_props_by_vid` takes a global best version across all of the
            // vid's labels.
            //
            // `schema.labels` is a map, so which label this fires after is not
            // defined. Assertions must hold whichever one ran.
            fail::fail_point!("compaction::between-labels");
        }

        // Compact Edges
        for (edge_type, meta) in &schema.edge_types {
            // Outgoing: src_labels
            for label in &meta.src_labels {
                info!("Compacting adjacency {} -> {} (fwd)", label, edge_type);
                match self.compact_adjacency(edge_type, label, "fwd").await {
                    Ok(info) => report.adjacency.push(info),
                    Err(e) => {
                        error!(
                            "Failed to compact adjacency {} -> {}: {}",
                            label, edge_type, e
                        );
                    }
                }
            }

            // Crash window: for this edge type, `adj_..._fwd` is merged AND its
            // deltas are cleared, while `adj_..._bwd` and its deltas are
            // untouched. The two directions therefore disagree about every edge
            // deleted since the last compaction.
            //
            // Recovery duty: reads must still agree in both directions — the
            // bwd side resolves through its intact L1 overlay — and the next
            // compaction must converge them. Nothing in this loop ties the two
            // directions together, so the agreement is a claim about the read
            // path's delta overlay, not about compaction.
            fail::fail_point!("compaction::between-fwd-and-bwd");

            // Incoming: dst_labels
            for label in &meta.dst_labels {
                info!("Compacting adjacency {} <- {} (bwd)", label, edge_type);
                match self.compact_adjacency(edge_type, label, "bwd").await {
                    Ok(info) => report.adjacency.push(info),
                    Err(e) => {
                        error!(
                            "Failed to compact adjacency {} <- {}: {}",
                            label, edge_type, e
                        );
                    }
                }
            }
        }

        metrics::counter!("uni_compaction_runs_total").increment(1);
        metrics::histogram!("uni_compaction_duration_seconds")
            .record(start.elapsed().as_secs_f64());

        Ok(report)
    }

    #[instrument(skip(self), fields(rows_processed, duration_ms), level = "info")]
    pub async fn compact_vertices(&self, label: &str) -> Result<VertexCompactionReport> {
        let start = std::time::Instant::now();
        let mut crdt_merges = 0usize;
        let schema_manager = self.storage.schema_manager();
        let schema = schema_manager.schema();

        let label_props = schema
            .properties
            .get(label)
            .ok_or_else(|| anyhow!("Label not found"))?;

        // Identify CRDT properties
        let crdt_props: HashSet<String> = label_props
            .iter()
            .filter(|(_, meta)| matches!(meta.r#type, DataType::Crdt(_)))
            .map(|(name, _)| name.clone())
            .collect();

        let dataset = self.storage.vertex_dataset(label)?;
        let backend = self.storage.backend();
        let table_name = dataset.table_name();

        // Check if table exists
        if !backend.table_exists(&table_name).await.unwrap_or(false) {
            info!("No vertex data to compact for label '{}'", label);
            // `ran: false` — nothing was measured, as distinct from a pass that
            // ran and merged no CRDT properties.
            return Ok(VertexCompactionReport::default());
        }

        // In-memory compaction for now (MVP).
        // For large datasets, this needs to be streaming/chunked with external sort.
        // Current approach: Read ALL, merge in map, write NEW.
        // TODO(perf): This accumulates ALL vertices in memory, causing OOM for large
        // labels (millions of vertices). Refactor to use streaming merge-sort with
        // constant memory usage (e.g., external sort or Lance fragment-by-fragment merge).

        let row_count = backend.count_rows(&table_name, None).await?;
        crate::storage::delta::check_oom_guard(
            row_count,
            self.storage.config.max_compaction_rows,
            label,
            "vertices",
        )?;

        info!(
            label = %label,
            row_count,
            estimated_bytes = row_count * 200,
            "Starting vertex compaction"
        );

        // Serialize the whole read → merge → OVERWRITE against concurrent flushes.
        // The flush path takes the same per-table lock in
        // `merge_insert_batch_with_lance_conflict_retry`; without it, an unguarded
        // `AddDataMode::Overwrite` below would silently discard rows a flush
        // appended in the window between this scan and the overwrite commit.
        let _write_guard = backend.lock_table_for_write(&table_name).await;

        use crate::backend::types::ScanRequest;
        let batches: Vec<RecordBatch> = backend.scan(ScanRequest::all(&table_name)).await?;

        // Vid -> (Properties, Deleted)
        let mut vertex_state: HashMap<Vid, (Properties, bool)> = HashMap::new();
        let mut vertex_versions: HashMap<Vid, u64> = HashMap::new();
        let mut vertex_labels: HashMap<Vid, Vec<String>> = HashMap::new();
        let mut created_at: HashMap<Vid, i64> = HashMap::new();
        let mut updated_at: HashMap<Vid, i64> = HashMap::new();

        let mut rows_processed = 0;

        for batch in batches {
            rows_processed += batch.num_rows();
            let vid_col = batch
                .column_by_name("_vid")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let ver_col = batch
                .column_by_name("_version")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let del_col = batch
                .column_by_name("_deleted")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap();

            // Read _labels column (List<Utf8>) if present
            let labels_col = batch
                .column_by_name("_labels")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>());

            // `ext_id` and `overflow_json` are on the reserved-property list, so
            // they can NEVER appear in `label_props` — the schema-driven rebuild
            // below is structurally incapable of seeing them. Read them from
            // their physical columns and put them back into the property map, so
            // `build_record_batch_with_timestamps` re-derives `ext_id`, `_uid`
            // and `overflow_json` from the same inputs the flush path used.
            let ext_id_col = batch
                .column_by_name("ext_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let overflow_col = batch
                .column_by_name("overflow_json")
                .and_then(|c| c.as_any().downcast_ref::<LargeBinaryArray>());
            // Timestamps are metadata rather than properties, so they are
            // carried in side maps and handed to the batch builder directly.
            let created_col = batch
                .column_by_name("_created_at")
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>());
            let updated_col = batch
                .column_by_name("_updated_at")
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>());

            for i in 0..batch.num_rows() {
                let vid = Vid::from(vid_col.value(i));
                let version = ver_col.value(i);
                let deleted = del_col.value(i);

                // Extract labels from the _labels column (keep latest version's labels)
                if let Some(list_arr) = labels_col
                    && version >= *vertex_versions.entry(vid).or_insert(0)
                {
                    let labels = crate::storage::arrow_convert::labels_from_list_array(list_arr, i);
                    if !labels.is_empty() {
                        vertex_labels.insert(vid, labels);
                    }
                }

                let current_entry = vertex_state
                    .entry(vid)
                    .or_insert((Properties::new(), false));
                let current_version = vertex_versions.entry(vid).or_insert(0);

                // If this row is newer than what we've seen (or same), we apply logic.
                // Wait, if we process unordered, we need to be careful.
                // For CRDTs, we MERGE regardless of version (commutative).
                // For LWW, we take MAX version.

                // If it's a deletion, and it's newer, it wins.
                if deleted {
                    if version >= *current_version {
                        current_entry.1 = true;
                        current_entry.0.clear(); // Clear properties on delete
                        *current_version = version;
                    }
                    continue;
                }

                // It's an update/insert
                // Extract props and track NULLs (property removals)
                let mut row_props = Properties::new();
                let mut null_props = Vec::new(); // Track explicitly NULL properties
                for (name, meta) in label_props {
                    if let Some(col) = batch.column_by_name(name) {
                        if col.is_null(i) {
                            // Property was explicitly removed (set to NULL)
                            null_props.push(name.clone());
                        } else {
                            let val = crate::storage::value_codec::decode_column_value(
                                col.as_ref(),
                                &meta.r#type,
                                i,
                                crate::storage::value_codec::CrdtDecodeMode::Strict,
                            )?;
                            row_props.insert(name.clone(), val);
                        }
                    }
                }

                // Restore the reserved columns the loop above cannot reach.
                if let Some(col) = ext_id_col
                    && !col.is_null(i)
                {
                    row_props.insert(
                        "ext_id".to_string(),
                        Value::String(col.value(i).to_string()),
                    );
                }
                if let Some(col) = overflow_col
                    && !col.is_null(i)
                    && let Value::Map(overflow) =
                        uni_common::cypher_value_codec::decode(col.value(i))?
                {
                    // Schemaless properties merge exactly like declared ones;
                    // `build_overflow_json_column` re-splits them on the way out.
                    crate::storage::property_builder::merge_overflow_into(&mut row_props, overflow);
                }

                // `_created_at` is the earliest we have seen for this vid and
                // `_updated_at` the latest: rows arrive unordered, so taking the
                // row's own value would depend on scan order.
                if let Some(col) = created_col
                    && !col.is_null(i)
                {
                    created_at
                        .entry(vid)
                        .and_modify(|existing| *existing = (*existing).min(col.value(i)))
                        .or_insert(col.value(i));
                }
                if let Some(col) = updated_col
                    && !col.is_null(i)
                {
                    updated_at
                        .entry(vid)
                        .and_modify(|existing| *existing = (*existing).max(col.value(i)))
                        .or_insert(col.value(i));
                }

                crdt_merges += Self::merge_row_into_state(
                    row_props,
                    null_props,
                    version,
                    current_entry,
                    current_version,
                    &crdt_props,
                    self.storage.plugin_registry().map(|a| a.as_ref()),
                )?;
            }
        }

        // Convert state to RecordBatch and write OVERWRITE
        let mut valid_vertices = Vec::new();
        let mut valid_versions = Vec::new();
        let mut valid_deleted = Vec::new(); // Should be all false if we filter out tombstones?
        // Or we keep tombstones if they are recent?
        // Compaction usually removes tombstones.

        for (vid, (props, deleted)) in vertex_state {
            if !deleted {
                let labels = vertex_labels.remove(&vid).unwrap_or_default();
                valid_vertices.push((vid, labels, props));
                valid_versions.push(vertex_versions[&vid]);
                valid_deleted.push(false);
            }
        }

        if !valid_vertices.is_empty() {
            let batch = dataset.build_record_batch_with_timestamps(
                &valid_vertices,
                &valid_deleted,
                &valid_versions,
                &schema,
                Some(&created_at),
                Some(&updated_at),
            )?;
            dataset
                .replace(self.storage.backend(), batch, &schema)
                .await?;

            // `replace` goes through `replace_table_atomic`, which is
            // `WriteMode::Overwrite`, and Lance drops every index on the dataset
            // when it overwrites. Nothing else puts them back: `optimize_indices`
            // runs only on the *other* compaction path (`optimize_table`, via
            // Lance's `compact_files`), and by the time it runs there is no index
            // left to optimize.
            //
            // The effect is silent and permanent. uni's own schema still reports
            // `status: Online` with the original `last_built_at`, because the
            // status is truthful about the build and nothing notices a later
            // write removed the artifact — so every query on this label falls
            // back to a full scan for the life of the store. On the LDBC SF1
            // fixture that is all six Person indexes gone after the first
            // background compaction tick, and all fourteen queries reporting
            // `index_scans=0` against indexes that are present on disk but
            // absent from Lance's manifest (#247).
            // Both kinds, because the overwrite took both. `ensure_default_indexes`
            // restores the ones uni creates for its own read paths (`_vid` and
            // friends); `rebuild_indexes_for_label` restores the ones the user
            // declared. The first is invisible to `schema.indexes`, so rebuilding
            // only the declared set leaves the table partially indexed — measured:
            // 4 indexes before the overwrite, 1 after a declared-only rebuild.
            dataset
                .ensure_default_indexes(self.storage.backend())
                .await?;

            #[cfg(feature = "lance-backend")]
            if let Err(e) = self
                .storage
                .index_manager()
                .rebuild_indexes_for_label(label)
                .await
            {
                // Fail the compaction rather than leave the table silently
                // unindexed: the rows are already replaced and correct, so the
                // data is fine, but reporting success here is what made this
                // invisible for as long as it was.
                return Err(anyhow::anyhow!(
                    "vertex compaction replaced '{label}' but could not rebuild its indexes,                      which the overwrite dropped: {e}"
                ));
            }
        }

        // Crash window: the per-label table has been replaced with the merged,
        // tombstone-free row set, while `main_vertices` still holds the original
        // rows INCLUDING the `_deleted = true` tombstones. The two tables
        // disagree by construction until the next compaction.
        //
        // Recovery duty: a vertex deleted before the crash must stay deleted —
        // the surviving main-table row must not resurrect it — and a survivor
        // must keep every property, including the reserved columns this pass
        // reconstructs.
        fail::fail_point!("compaction::after-vertex-replace");

        let duration = start.elapsed();
        let rows_reclaimed = rows_processed as u64 - valid_vertices.len() as u64;
        metrics::counter!("uni_compaction_rows_reclaimed_total", "type" => "vertex")
            .increment(rows_reclaimed);

        tracing::Span::current().record("rows_processed", rows_processed);
        tracing::Span::current().record("duration_ms", duration.as_millis());
        info!(
            rows = rows_processed,
            duration_ms = duration.as_millis(),
            "Vertex compaction completed"
        );

        metrics::histogram!("uni_compaction_duration_seconds", "type" => "vertex")
            .record(duration.as_secs_f64());

        Ok(VertexCompactionReport {
            ran: true,
            rows_processed,
            crdt_merges,
        })
    }

    fn merge_crdt_values(
        a: &Value,
        b: &Value,
        registry: Option<&uni_plugin::PluginRegistry>,
    ) -> Result<Value> {
        if a.is_null() {
            return Ok(b.clone());
        }
        if b.is_null() {
            return Ok(a.clone());
        }
        let mut crdt_a: Crdt = serde_json::from_value(a.clone().into())?;
        let crdt_b: Crdt = serde_json::from_value(b.clone().into())?;
        // Operand order: self=existing (a), other=new (b). Preserve — a custom
        // provider's merge may be non-commutative.
        match registry {
            Some(reg) => crdt_a
                .merge_via_registry(&crdt_b, reg)
                .map_err(|e| anyhow::anyhow!("{e}"))?,
            None => crdt_a
                .try_merge(&crdt_b)
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        }
        Ok(Value::from(serde_json::to_value(crdt_a)?))
    }

    /// Merge row properties into state based on version comparison.
    fn merge_row_into_state(
        row_props: Properties,
        null_props: Vec<String>,
        version: u64,
        current_entry: &mut (Properties, bool),
        current_version: &mut u64,
        crdt_props: &HashSet<String>,
        registry: Option<&uni_plugin::PluginRegistry>,
    ) -> Result<usize> {
        // Returned rather than accumulated through an out-param: one more
        // `&mut` argument would push this past clippy's arity limit, and the
        // count is genuinely a result of the call.
        let mut crdt_merges = 0usize;
        if version > *current_version {
            // New version wins for LWW, merge for CRDTs
            *current_version = version;
            current_entry.1 = false;

            for (k, v) in row_props {
                if crdt_props.contains(&k) {
                    let existing = current_entry.0.entry(k.clone()).or_insert(Value::Null);
                    *existing = Self::merge_crdt_values(existing, &v, registry)?;
                    crdt_merges += 1;
                } else {
                    current_entry.0.insert(k, v);
                }
            }

            // Remove properties explicitly set to NULL in the newer version
            for null_prop in &null_props {
                if !crdt_props.contains(null_prop) {
                    current_entry.0.remove(null_prop);
                }
            }
        } else if version == *current_version {
            // Same version: merge all
            current_entry.1 = false;
            for (k, v) in row_props {
                if crdt_props.contains(&k) {
                    let existing = current_entry.0.entry(k.clone()).or_insert(Value::Null);
                    *existing = Self::merge_crdt_values(existing, &v, registry)?;
                    crdt_merges += 1;
                } else {
                    current_entry.0.insert(k, v);
                }
            }
        } else {
            // Older version: only merge CRDTs
            if !current_entry.1 {
                for (k, v) in row_props {
                    if crdt_props.contains(&k) {
                        let existing = current_entry.0.entry(k.clone()).or_insert(Value::Null);
                        *existing = Self::merge_crdt_values(existing, &v, registry)?;
                        crdt_merges += 1;
                    }
                }
            }
        }
        Ok(crdt_merges)
    }

    #[instrument(skip(self), fields(delta_count, duration_ms), level = "info")]
    pub async fn compact_adjacency(
        &self,
        edge_type: &str,
        label: &str,
        direction: &str,
    ) -> Result<CompactionInfo> {
        let start = std::time::Instant::now();
        let schema = self.storage.schema_manager().schema();

        // 1. Load all L1 Deltas sorted by key
        let delta_ds = self.storage.delta_dataset(edge_type, direction)?;
        let deltas = delta_ds
            .scan_all_backend(self.storage.backend(), &schema)
            .await?;

        let delta_count = deltas.len();
        tracing::Span::current().record("delta_count", delta_count);

        if deltas.is_empty() {
            // Nothing to compact, return info anyway
            return Ok(CompactionInfo {
                edge_type: edge_type.to_string(),
                direction: direction.to_string(),
            });
        }

        // Group deltas by src_vid (if fwd) or dst_vid (if bwd)
        // We'll use a HashMap for now since we loaded all into memory.
        // Value is list of ops for that vertex.
        let mut delta_map: HashMap<Vid, Vec<L1Entry>> = HashMap::new();
        for entry in &deltas {
            let key = if direction == "fwd" {
                entry.src_vid
            } else {
                entry.dst_vid
            };
            delta_map.entry(key).or_default().push(entry.clone());
        }

        // Sort each VID's ops by version to ensure correct ordering
        // This guarantees Delete(v=2) beats Insert(v=1) regardless of scan order
        for ops in delta_map.values_mut() {
            ops.sort_by_key(|e| e.version);
        }

        // 2. Open L2 Adjacency stream
        let adj_ds = self
            .storage
            .adjacency_dataset(edge_type, label, direction)?;

        // We need to write a NEW version.
        // Strategy:
        // - Read L2 batch by batch.
        // - For each row (vertex), check if we have deltas.
        // - Apply deltas.
        // - Write to new batch.
        // - Track which vertices from deltas we've processed.
        // - After L2 stream ends, process remaining "new" vertices from deltas.

        // Output Builders
        let mut src_vid_builder = UInt64Builder::new();
        let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
        let mut edge_ids_builder = ListBuilder::new(UInt64Builder::new());

        let mut processed_vids = HashSet::new();

        // Try to read from backend (canonical storage)
        let backend = self.storage.backend();
        let adj_table_name = adj_ds.table_name();
        if backend.table_exists(&adj_table_name).await.unwrap_or(false) {
            let adj_row_count = backend.count_rows(&adj_table_name, None).await?;
            crate::storage::delta::check_oom_guard(
                adj_row_count,
                self.storage.config.max_compaction_rows,
                &format!("{}_{}", edge_type, label),
                direction,
            )?;

            info!(
                edge_type = %edge_type,
                label = %label,
                direction = %direction,
                adj_row_count,
                delta_count,
                estimated_bytes = adj_row_count * 100 + delta_count * ENTRY_SIZE_ESTIMATE,
                "Starting adjacency compaction"
            );

            use crate::backend::types::ScanRequest;
            let batches: Vec<RecordBatch> = backend.scan(ScanRequest::all(&adj_table_name)).await?;

            for batch in batches {
                let src_col = batch
                    .column_by_name("src_vid")
                    .ok_or(anyhow!("Missing src_vid"))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or(anyhow!("Invalid src_vid"))?;
                let neighbors_col = batch
                    .column_by_name("neighbors")
                    .ok_or(anyhow!("Missing neighbors"))?
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or(anyhow!("Invalid neighbors"))?;
                let edge_ids_col = batch
                    .column_by_name("edge_ids")
                    .ok_or(anyhow!("Missing edge_ids"))?
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or(anyhow!("Invalid edge_ids"))?;

                for i in 0..batch.num_rows() {
                    let vid = Vid::from(src_col.value(i));
                    processed_vids.insert(vid);

                    // Reconstruct current adjacency list
                    let n_list = neighbors_col.value(i);
                    let n_array = n_list.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let e_list = edge_ids_col.value(i);
                    let e_array = e_list.as_any().downcast_ref::<UInt64Array>().unwrap();

                    let mut current_edges: HashMap<Eid, Vid> = HashMap::new();
                    for j in 0..n_array.len() {
                        current_edges
                            .insert(Eid::from(e_array.value(j)), Vid::from(n_array.value(j)));
                    }

                    if let Some(ops) = delta_map.get(&vid) {
                        apply_deltas_to_edges(&mut current_edges, ops, direction);
                    }

                    append_edges_to_builders(
                        vid,
                        &current_edges,
                        &mut src_vid_builder,
                        &mut neighbors_builder,
                        &mut edge_ids_builder,
                    );
                }
            }
        }

        // Process new vertices (in deltas but not in L2)
        for (vid, ops) in delta_map {
            if processed_vids.contains(&vid) {
                continue;
            }

            let mut current_edges: HashMap<Eid, Vid> = HashMap::new();
            apply_deltas_to_edges(&mut current_edges, &ops, direction);

            append_edges_to_builders(
                vid,
                &current_edges,
                &mut src_vid_builder,
                &mut neighbors_builder,
                &mut edge_ids_builder,
            );
        }

        // Final Flush — always replace L2, even when the compacted output is
        // empty. If every edge for this (edge_type, direction) was deleted the
        // builders are empty; skipping the replace here would leave the stale
        // pre-delete L2 rows intact while the tombstone-clear below erases the
        // Delta L1 deletes, resurrecting the deleted edges on the next read.
        // Writing the (possibly empty) batch overwrites L2 to match the deltas.
        {
            let src_arr = Arc::new(src_vid_builder.finish());
            let neighbors_arr = Arc::new(neighbors_builder.finish());
            let edge_ids_arr = Arc::new(edge_ids_builder.finish());

            let schema = adj_ds.get_arrow_schema();
            let batch = RecordBatch::try_new(schema, vec![src_arr, neighbors_arr, edge_ids_arr])?;

            // Replace the table with compacted data
            adj_ds.replace(self.storage.backend(), batch).await?;
        }

        // Crash window: L2 `adj_{et}_{dir}` has been fully overwritten with
        // merge(L2, deltas), but `delta_{et}_{dir}` still holds every merged row
        // at `_version <= clear_hwm`. This is the only genuine write-then-delete
        // window in the compaction path.
        //
        // Recovery duty: none — the redo must be a no-op. Re-applying the same
        // deltas onto an already-merged L2 is safe only because
        // `apply_deltas_to_edges` is a per-op HashMap insert/remove. That is a
        // property of the merge, not a protocol guarantee, so it is asserted
        // rather than assumed.
        fail::fail_point!("compaction::after-adj-replace-before-delta-clear");

        // CRITICAL: Clear Delta L1 after compaction
        // Topology ops from Delta L1 are now incorporated into L2 adjacency.
        // Edge properties survive in main_edges (dual-written during flush).
        // Clearing Delta L1 prevents stale topology data from being read.
        if !deltas.is_empty() {
            info!(
                "Clearing Delta L1 for edge_type={} direction={} after compaction (incorporated {} ops)",
                edge_type,
                direction,
                deltas.len()
            );

            // Invariant: Every EID in Delta L1 must have a corresponding entry in
            // main_edges, because Writer::flush_to_l1 performs a dual-write.
            // Tests that create delta entries directly (for schema/overflow testing)
            // must not call compact_adjacency without also populating main_edges.
            #[cfg(debug_assertions)]
            {
                use crate::storage::main_edge::MainEdgeDataset;

                let delta_eids: std::collections::HashSet<Eid> =
                    deltas.iter().map(|e| e.eid).collect();

                for eid in delta_eids {
                    let main_edge_exists =
                        MainEdgeDataset::exists_by_eid(self.storage.backend(), eid)
                            .await
                            .unwrap_or(false);

                    debug_assert!(
                        main_edge_exists,
                        "EID {} from Delta L1 not found in main_edges after compaction. \
                        This indicates edge properties were not dual-written during flush.",
                        eid.as_u64()
                    );
                }
            }

            // Clear ONLY the deltas we actually merged into L2 — those at or
            // below the high-water-mark captured from the rows read at the top
            // of this function. A concurrent flush that appended new deltas
            // inside the read→clear window stamped them with a strictly higher
            // `_version`, so the predicate-delete leaves them intact to be
            // reprocessed next compaction. This replaces the old unconditional
            // empty-batch wipe, whose only guard was an instantaneous
            // `flush_in_progress` check that a flush starting AND finishing in
            // the window slipped past — silently wiping its rows. (review H11)
            let clear_hwm = deltas.iter().map(|e| e.version).max().unwrap_or(0);
            let delta_ds = self.storage.delta_dataset(edge_type, direction)?;
            delta_ds
                .delete_up_to_version(self.storage.backend(), clear_hwm)
                .await?;
        }

        let duration = start.elapsed();
        tracing::Span::current().record("duration_ms", duration.as_millis());
        info!(
            delta_count,
            duration_ms = duration.as_millis(),
            "Adjacency compaction completed"
        );

        metrics::histogram!("uni_compaction_duration_seconds", "type" => "adjacency")
            .record(duration.as_secs_f64());

        Ok(CompactionInfo {
            edge_type: edge_type.to_string(),
            direction: direction.to_string(),
        })
    }
}

/// Apply delta operations to an edge map, returning the resolved neighbor for the direction.
fn apply_deltas_to_edges(current_edges: &mut HashMap<Eid, Vid>, ops: &[L1Entry], direction: &str) {
    for op in ops {
        match op.op {
            Op::Insert => {
                let neighbor = if direction == "fwd" {
                    op.dst_vid
                } else {
                    op.src_vid
                };
                current_edges.insert(op.eid, neighbor);
            }
            Op::Delete => {
                current_edges.remove(&op.eid);
            }
        }
    }
}

/// Write sorted edges from a HashMap into adjacency list builders.
fn append_edges_to_builders(
    vid: Vid,
    current_edges: &HashMap<Eid, Vid>,
    src_vid_builder: &mut UInt64Builder,
    neighbors_builder: &mut ListBuilder<UInt64Builder>,
    edge_ids_builder: &mut ListBuilder<UInt64Builder>,
) {
    if current_edges.is_empty() {
        return;
    }
    src_vid_builder.append_value(vid.as_u64());

    let mut sorted_eids: Vec<_> = current_edges.keys().cloned().collect();
    sorted_eids.sort();

    for eid in sorted_eids {
        let neighbor = current_edges[&eid];
        neighbors_builder.values().append_value(neighbor.as_u64());
        edge_ids_builder.values().append_value(eid.as_u64());
    }
    neighbors_builder.append(true);
    edge_ids_builder.append(true);
}

/// Information returned by adjacency compaction about what was compacted.
/// Used to coordinate in-memory CSR re-warm after storage compaction.
#[derive(Debug, Clone)]
pub struct CompactionInfo {
    pub edge_type: String,
    pub direction: String,
}

/// What one vertex-label semantic compaction pass did.
///
/// Deliberately a plain struct rather than `Option<usize>`: `Option` is
/// `#[must_use]`, and a dozen callers invoke `compact_vertices(..).await?;` in
/// statement position, which would all break under `-D warnings` for no gain.
#[derive(Debug, Clone, Copy, Default)]
pub struct VertexCompactionReport {
    /// Whether the pass ran at all. `false` means the label had no table, so
    /// nothing was measured — a `crdt_merges` of `0` only means "no merges
    /// happened" when this is `true`.
    pub ran: bool,
    /// Rows read during the pass.
    pub rows_processed: usize,
    /// CRDT value merges performed.
    pub crdt_merges: usize,
}

/// What a full semantic (tier-2) compaction pass did.
#[derive(Debug, Clone, Default)]
pub struct SemanticCompactionReport {
    /// Adjacency compactions performed — the unchanged payload its consumers
    /// use to re-warm the in-memory CSR.
    pub adjacency: Vec<CompactionInfo>,
    /// Vertex passes that actually ran. The denominator for `crdt_merges`.
    pub vertex_passes: usize,
    /// CRDT value merges across every vertex pass.
    pub crdt_merges: usize,
}

#[cfg(test)]
mod ws_d_crdt_tests {
    //! WS-D (P0.4): the compaction merge path must route CRDT merges through
    //! the plugin registry (previously it called native `try_merge` directly).
    //! These exercise `Compactor::merge_crdt_values` in isolation: with a
    //! registered provider the registry path runs; with `None` it falls back
    //! to native, byte-for-byte. (The full flush→compact integration test that
    //! proves the stamped `StorageManager` registry reaches `compact_vertices`
    //! at runtime is a recommended follow-up.)

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use uni_common::Value;
    use uni_crdt::{Crdt, GCounter};
    use uni_plugin::traits::crdt::{CrdtKind, CrdtKindProvider, CrdtOp, CrdtState, ScalarValue};
    use uni_plugin::{
        Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry,
    };

    #[derive(Default)]
    struct CountingProvider {
        calls: AtomicUsize,
    }
    impl CrdtKindProvider for CountingProvider {
        fn kind(&self) -> CrdtKind {
            CrdtKind::new("uni-crdt:g-counter")
        }
        fn empty(&self) -> Box<dyn CrdtState> {
            Box::new(St {
                inner: Crdt::GCounter(GCounter::new()),
            })
        }
        fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let inner =
                Crdt::from_msgpack(bytes).map_err(|e| FnError::new(0xA01, format!("{e}")))?;
            Ok(Box::new(St { inner }))
        }
    }
    struct St {
        inner: Crdt,
    }
    impl CrdtState for St {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn apply(&mut self, _op: &CrdtOp) -> Result<(), FnError> {
            Ok(())
        }
        fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
            let o = other
                .as_any()
                .downcast_ref::<St>()
                .ok_or_else(|| FnError::new(0xA10, "type mismatch"))?;
            self.inner
                .try_merge(&o.inner)
                .map_err(|e| FnError::new(0xA11, format!("{e}")))
        }
        fn value(&self) -> Result<ScalarValue, FnError> {
            Ok(ScalarValue::Utf8(Some(self.inner.type_name().to_owned())))
        }
        fn persist(&self) -> Result<Vec<u8>, FnError> {
            self.inner
                .to_msgpack()
                .map_err(|e| FnError::new(0xA12, format!("{e}")))
        }
    }

    fn gcounter(replica: &str, by: u64) -> Value {
        let mut g = GCounter::new();
        g.increment(replica, by);
        Value::from(serde_json::to_value(Crdt::GCounter(g)).unwrap())
    }

    #[test]
    fn compaction_merge_routes_through_registry() {
        let registry = PluginRegistry::new();
        let provider = Arc::new(CountingProvider::default());
        let caps = CapabilitySet::from_iter_of([Capability::Crdt]);
        let mut r = PluginRegistrar::new(PluginId::new("test.counting"), &caps, &registry);
        r.crdt_kind(
            CrdtKind::new("uni-crdt:g-counter"),
            Arc::clone(&provider) as Arc<dyn CrdtKindProvider>,
        )
        .unwrap();
        r.commit_to_registry().unwrap();

        let a = gcounter("r1", 5); // existing
        let b = gcounter("r2", 7); // new
        let merged = super::Compactor::merge_crdt_values(&a, &b, Some(&registry)).unwrap();

        assert!(
            provider.calls.load(Ordering::SeqCst) > 0,
            "compaction merge must route through the registered provider"
        );
        let crdt: Crdt = serde_json::from_value(merged.into()).unwrap();
        match crdt {
            Crdt::GCounter(g) => assert_eq!(g.value(), 12, "5 + 7 = 12"),
            other => panic!("expected GCounter, got {other:?}"),
        }
    }

    #[test]
    fn compaction_merge_falls_back_to_native_without_registry() {
        let a = gcounter("r1", 3);
        let b = gcounter("r2", 4);
        let merged = super::Compactor::merge_crdt_values(&a, &b, None).unwrap();
        let crdt: Crdt = serde_json::from_value(merged.into()).unwrap();
        match crdt {
            Crdt::GCounter(g) => assert_eq!(g.value(), 7, "3 + 4 = 7 native fallback"),
            other => panic!("expected GCounter, got {other:?}"),
        }
    }
}
