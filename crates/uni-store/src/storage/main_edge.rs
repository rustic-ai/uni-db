// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Main edge table for unified edge storage.
//!
//! This module implements the main `edges` table as described in STORAGE_DESIGN.md.
//! The main table contains all edges in the graph with:
//! - `_eid`: Internal edge ID (primary key)
//! - `src_vid`: Source vertex ID
//! - `dst_vid`: Destination vertex ID
//! - `type`: Edge type name
//! - `props_json`: All properties as JSONB blob
//! - `_deleted`: Soft-delete flag
//! - `_version`: MVCC version
//! - `_created_at`: Creation timestamp
//! - `_updated_at`: Update timestamp

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::{FilterExpr, Scalar, ScalarIndexType, ScanRequest};
use crate::storage::arrow_convert::build_timestamp_column_from_eid_map;
use anyhow::{Result, anyhow};
use arrow_array::builder::{LargeBinaryBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use sha3::{Digest, Sha3_256};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::{Eid, UniId, Vid};

/// Maximum EIDs per `_eid IN (...)` chunk in
/// [`MainEdgeDataset::find_props_by_eids`].
///
/// Mirrors `MAX_VIDS_PER_CHUNK` on the vertex side: an unbounded `IN` list
/// inflates the scan request and stops the scalar index earning its keep, while
/// a bounded one preserves the indexed lookup at any set size.
const MAX_EIDS_PER_CHUNK: usize = 10_000;

/// Requested EIDs per table row at which one full scan beats chunked lookups.
///
/// A request covering more than `rows / EID_SCAN_CROSSOVER_RATIO` EIDs is a
/// candidate for a single pass. Measured, not guessed: see
/// [`MainEdgeDataset::prefers_full_scan`].
const EID_SCAN_CROSSOVER_RATIO: usize = 4_096;

/// How much of the table the requested EIDs must range over before scanning.
///
/// A request is only worth one pass if its EIDs are spread thinly. A dense
/// request of the same size reads far fewer pages per lookup and stays cheaper
/// chunked — measured at ~24 us per EID dense against ~78 us sparse.
const EID_SPAN_RATIO: usize = 32;

/// Below this many EIDs, never scan — a point lookup on a small table would
/// otherwise pay the scan's fixed cost for a handful of rows.
///
/// Deliberately low. It guards the degenerate case only; the two ratios above
/// are what actually choose between the arms, and raising this to do their job
/// would also stop small fixtures from ever reaching the scan path, leaving it
/// untested.
const MIN_EIDS_FOR_FULL_SCAN: usize = 64;

/// One candidate row for an edge, while ranking a type scan by `_version`.
///
/// Carries `deleted` because a tombstone is a *winning* row, not an absent one:
/// it has to beat the older live row before being filtered out.
struct VersionedEdge {
    src_vid: Vid,
    dst_vid: Vid,
    edge_type: String,
    properties: Properties,
    version: u64,
    deleted: bool,
}

/// Which edge endpoint a pushed-down vid set constrains in
/// [`MainEdgeDataset::find_edges_by_type_names`].
///
/// `Src` for outgoing traversals, `Dst` for incoming, `Either` for
/// undirected (`Both`) traversals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointSide {
    Src,
    Dst,
    Either,
}

/// Main edge dataset for the unified `edges` table.
///
/// This table contains all edges regardless of type, providing:
/// - Fast ID-based lookups without knowing the edge type
/// - Unified traversal queries
#[derive(Debug)]
pub struct MainEdgeDataset {
    _base_uri: String,
}

impl MainEdgeDataset {
    /// Create a new MainEdgeDataset.
    pub fn new(base_uri: &str) -> Self {
        Self {
            _base_uri: base_uri.to_string(),
        }
    }

    /// Compute the content-addressed UID for an edge.
    ///
    /// Edge identity is the SHA3-256 of
    /// `(src_uid, dst_uid, edge_type, sorted_properties)` — the same
    /// content-addressed pattern as
    /// `MainVertexDataset::compute_vertex_uid` but extended with
    /// edge endpoint UIDs and the edge type. This lets fork diff and
    /// promote distinguish parallel edges between the same endpoints
    /// when their property bags differ (multi-edge support).
    ///
    /// Property iteration is sorted by key for deterministic hashing
    /// across machines and runs.
    pub fn compute_edge_uid(
        src_uid: &UniId,
        dst_uid: &UniId,
        edge_type: &str,
        props: &Properties,
    ) -> UniId {
        let mut hasher = Sha3_256::new();

        // Endpoint UIDs first — direction is significant
        // (src→dst ≠ dst→src for the same property bag).
        hasher.update(b"src:");
        hasher.update(src_uid.as_bytes());
        hasher.update(b"\0");
        hasher.update(b"dst:");
        hasher.update(dst_uid.as_bytes());
        hasher.update(b"\0");

        // Edge type.
        hasher.update(b"type:");
        hasher.update(edge_type.as_bytes());
        hasher.update(b"\0");

        // Properties sorted by key (matches compute_vertex_uid).
        let mut sorted_keys: Vec<_> = props.keys().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            if let Some(val) = props.get(key) {
                hasher.update(key.as_bytes());
                hasher.update(b":");
                hasher.update(val.to_string().as_bytes());
                hasher.update(b"\0");
            }
        }

        let result = hasher.finalize();
        UniId::from_bytes(result.into())
    }

    /// Get the Arrow schema for the main edges table.
    pub fn get_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_eid", DataType::UInt64, false),
            Field::new("src_vid", DataType::UInt64, false),
            Field::new("dst_vid", DataType::UInt64, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("props_json", DataType::LargeBinary, true),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new(
                "_created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "_updated_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]))
    }

    /// Get the table name for the main edges table.
    pub fn table_name() -> &'static str {
        "edges"
    }

    /// Build a record batch for the main edges table.
    ///
    /// # Arguments
    /// * `edges` - List of (eid, src_vid, dst_vid, edge_type, properties, deleted, version) tuples
    /// * `created_at` - Optional map of Eid -> nanoseconds since epoch
    /// * `updated_at` - Optional map of Eid -> nanoseconds since epoch
    pub fn build_record_batch(
        edges: &[(Eid, Vid, Vid, String, Properties, bool, u64)],
        created_at: Option<&HashMap<Eid, i64>>,
        updated_at: Option<&HashMap<Eid, i64>>,
    ) -> Result<RecordBatch> {
        let arrow_schema = Self::get_arrow_schema();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        // _eid column
        let eids: Vec<u64> = edges
            .iter()
            .map(|(e, _, _, _, _, _, _)| e.as_u64())
            .collect();
        columns.push(Arc::new(UInt64Array::from(eids)));

        // src_vid column
        let src_vids: Vec<u64> = edges
            .iter()
            .map(|(_, s, _, _, _, _, _)| s.as_u64())
            .collect();
        columns.push(Arc::new(UInt64Array::from(src_vids)));

        // dst_vid column
        let dst_vids: Vec<u64> = edges
            .iter()
            .map(|(_, _, d, _, _, _, _)| d.as_u64())
            .collect();
        columns.push(Arc::new(UInt64Array::from(dst_vids)));

        // type column
        let mut type_builder = StringBuilder::new();
        for (_, _, _, edge_type, _, _, _) in edges.iter() {
            type_builder.append_value(edge_type);
        }
        columns.push(Arc::new(type_builder.finish()));

        // props_json column (JSONB binary encoding)
        let mut props_json_builder = LargeBinaryBuilder::new();
        for (_, _, _, _, props, _, _) in edges.iter() {
            let jsonb_bytes = {
                let json_val = serde_json::to_value(props).unwrap_or(serde_json::json!({}));
                let uni_val: uni_common::Value = json_val.into();
                uni_common::cypher_value_codec::encode(&uni_val)
            };
            props_json_builder.append_value(&jsonb_bytes);
        }
        columns.push(Arc::new(props_json_builder.finish()));

        // _deleted column
        let deleted: Vec<bool> = edges.iter().map(|(_, _, _, _, _, d, _)| *d).collect();
        columns.push(Arc::new(BooleanArray::from(deleted)));

        // _version column
        let versions: Vec<u64> = edges.iter().map(|(_, _, _, _, _, _, v)| *v).collect();
        columns.push(Arc::new(UInt64Array::from(versions)));

        // _created_at and _updated_at columns using shared builder
        let eids = edges.iter().map(|(e, _, _, _, _, _, _)| *e);
        columns.push(build_timestamp_column_from_eid_map(
            eids.clone(),
            created_at,
        ));
        columns.push(build_timestamp_column_from_eid_map(eids, updated_at));

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Write a batch to the main edges table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    /// Race-safe under async-flush — see
    /// `crate::storage::manager::write_batch_with_lance_conflict_retry`.
    pub async fn write_batch(backend: &dyn StorageBackend, batch: RecordBatch) -> Result<()> {
        let table_name = table_names::main_edge_table_name();
        crate::storage::manager::write_batch_with_lance_conflict_retry(backend, table_name, batch)
            .await
    }

    /// Ensure default indexes exist on the main edges table.
    ///
    /// Checks for existing indexes before creating to avoid expensive
    /// full-table rebuilds on every flush (LanceDB replaces indexes on create).
    pub async fn ensure_default_indexes(backend: &dyn StorageBackend) -> Result<()> {
        let table_name = table_names::main_edge_table_name();
        let indices = backend.list_indexes(table_name).await?;

        let has_index = |col: &str| {
            indices
                .iter()
                .any(|idx| idx.columns.contains(&col.to_string()))
        };

        for (column, idx_type) in [
            ("_eid", ScalarIndexType::BTree),
            ("src_vid", ScalarIndexType::BTree),
            ("dst_vid", ScalarIndexType::BTree),
            ("type", ScalarIndexType::BTree),
        ] {
            if has_index(column) {
                continue;
            }
            log::info!("Creating {} index on main_edges", column);
            if let Err(e) = backend
                .create_scalar_index(table_name, &[column], idx_type, None)
                .await
            {
                crate::storage::record_default_index_failure(table_name, column, &e);
            }
        }

        Ok(())
    }

    /// Check whether an edge exists by EID, regardless of deletion status.
    ///
    /// Unlike `find_props_by_eid`, this does NOT filter by `_deleted = false`,
    /// so it returns true for both active and soft-deleted edges. Used by the
    /// compaction invariant check to verify dual-writes occurred.
    pub async fn exists_by_eid(backend: &dyn StorageBackend, eid: Eid) -> Result<bool> {
        let filter = FilterExpr::equals("_eid", Scalar::UInt(eid.as_u64()));
        let batches = Self::execute_query(backend, filter, Some(vec!["_eid"])).await?;
        Ok(!batches.is_empty() && batches.iter().any(|b| b.num_rows() > 0))
    }

    /// Execute a query on the main edges table.
    ///
    /// Returns empty vec if table doesn't exist.
    async fn execute_query(
        backend: &dyn StorageBackend,
        filter: FilterExpr,
        columns: Option<Vec<&str>>,
    ) -> Result<Vec<RecordBatch>> {
        let table_name = table_names::main_edge_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(Vec::new());
        }

        let mut request = ScanRequest::all(table_name).with_filter(filter);
        if let Some(cols) = columns {
            request = request.with_columns(cols.into_iter().map(String::from).collect());
        }

        backend.scan(request).await
    }

    /// Find properties for an edge by EID in the main edges table.
    ///
    /// Returns the props_json parsed into a Properties HashMap if found.
    /// This is used as a fallback for unknown/schemaless edge types.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///   Mirrors [`MainVertexDataset::find_props_by_vid`]; without it a
    ///   snapshot-pinned reader reads L0 and the delta tier at its snapshot but
    ///   this L1 fallback at HEAD, so a post-snapshot write becomes visible.
    ///
    ///   The bound only bites when the calling `PropertyManager` was built over
    ///   pinned storage, which today means `UniInner::at_snapshot`'s
    ///   time-travel view. A read-write transaction routes its *scans* through
    ///   `pinned_at_version` but deliberately keeps the live, unbounded
    ///   `PropertyManager` so property point-reads honour read-your-writes —
    ///   see the design note at `uni-query`'s `executor/read.rs`.
    ///   Schemaless and overflow edge properties live only in `props_json`
    ///   (never in delta columns), so this path is reached on *every* such
    ///   read — not only after compaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails or JSON parsing fails.
    ///
    /// [`MainVertexDataset::find_props_by_vid`]: crate::storage::main_vertex::MainVertexDataset::find_props_by_vid
    pub async fn find_props_by_eid(
        backend: &dyn StorageBackend,
        eid: Eid,
        version: Option<u64>,
    ) -> Result<Option<Properties>> {
        // MVCC (review C2): the scan must see deletion tombstones — the
        // highest-version row wins, and a deleted winner yields `None`.
        // Filtering `_deleted = false` here would let an OLDER live version
        // resurrect an edge whose tombstone is the true (highest-version)
        // winner.
        //
        // The version bound is a *conjunct*, not a substitute for that rule: it
        // narrows the candidate set to rows visible at the snapshot, and the
        // tombstone-winner selection below then runs unchanged over whatever
        // survives. This is the same composition `find_props_by_vid` uses.
        let filter = super::with_version_bound(
            FilterExpr::equals("_eid", Scalar::UInt(eid.as_u64())),
            version,
        );
        let batches = Self::execute_query(
            backend,
            filter,
            Some(vec!["props_json", "_version", "_deleted"]),
        )
        .await?;

        if batches.is_empty() {
            return Ok(None);
        }

        // Find the row with highest version (latest), tombstones included.
        let mut best_props: Option<Properties> = None;
        let mut best_version: u64 = 0;
        let mut best_deleted = false;

        for batch in &batches {
            let props_col = batch.column_by_name("props_json");
            let version_col = batch.column_by_name("_version");
            let deleted_col = batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

            if let (Some(props_arr), Some(ver_arr)) = (
                props_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>()),
                version_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            ) {
                for i in 0..batch.num_rows() {
                    let version = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };

                    if version >= best_version {
                        best_version = version;
                        best_deleted = deleted_col.is_some_and(|d| d.value(i));
                        best_props = if best_deleted {
                            Some(Properties::new())
                        } else {
                            Some(Self::parse_props_json(props_arr, i)?)
                        };
                    }
                }
            }
        }

        if best_deleted {
            return Ok(None);
        }
        Ok(best_props)
    }

    /// Parse props_json from a LargeBinaryArray (JSONB) at the given index.
    /// Properties for many edges by EID, resolved in one scan per chunk.
    ///
    /// The batched counterpart of [`MainEdgeDataset::find_props_by_eid`]. Any
    /// caller resolving a whole traversal's edges must use this form: the
    /// singular one issues a separate `ScanRequest` per edge, measured at
    /// ~1.5 ms each against a persisted SF1 store, so a 4809-edge traversal
    /// spent 7.3 s of its 7.4 s in that loop alone. The batched scan keeps the
    /// `_eid` BTree index in play while collapsing N round trips into one.
    ///
    /// EIDs with no row, and those whose winning row is a tombstone, are absent
    /// from the returned map — the batched spelling of the singular form's
    /// `Ok(None)`.
    ///
    /// Version selection matches [`MainEdgeDataset::find_props_by_eid`] exactly,
    /// per EID: the highest `_version` wins with tombstones included in the
    /// contest, so a delete cannot be undone by an older live row. Ties resolve
    /// to the later row, as they do there.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails or a `props_json` blob cannot
    /// be decoded.
    pub async fn find_props_by_eids(
        backend: &dyn StorageBackend,
        eids: &[Eid],
        version: Option<u64>,
    ) -> Result<HashMap<Eid, Properties>> {
        if eids.is_empty() {
            return Ok(HashMap::new());
        }

        let table_name = table_names::main_edge_table_name();
        if !backend.table_exists(table_name).await? {
            return Ok(HashMap::new());
        }

        // Winner per EID: (version, deleted, props). Tombstones stay in the map
        // until the end so a lower-version live row cannot displace them.
        let mut best: HashMap<Eid, (u64, bool, Properties)> = HashMap::new();
        let columns = vec!["_eid", "props_json", "_version", "_deleted"];

        if Self::prefers_full_scan(backend, table_name, eids).await? {
            // One pass, filtered in memory. Streamed rather than collected: the
            // whole table's `props_json` would otherwise be resident at once.
            let wanted: std::collections::HashSet<u64> = eids.iter().map(Eid::as_u64).collect();
            let filter = super::with_version_bound(FilterExpr::Literal(true), version);
            let request = ScanRequest::all(table_name)
                .with_filter(filter)
                .with_columns(columns.into_iter().map(String::from).collect());

            let mut stream = backend.scan_stream(request).await?;
            while let Some(batch) = futures::TryStreamExt::try_next(&mut stream).await? {
                Self::merge_winning_props(&batch, Some(&wanted), &mut best)?;
            }
        } else {
            for chunk in eids.chunks(MAX_EIDS_PER_CHUNK) {
                let filter = super::with_version_bound(
                    FilterExpr::one_of("_eid", chunk.iter().map(|e| Scalar::UInt(e.as_u64()))),
                    version,
                );
                let batches = Self::execute_query(backend, filter, Some(columns.clone())).await?;
                for batch in &batches {
                    Self::merge_winning_props(batch, None, &mut best)?;
                }
            }
        }

        Ok(best
            .into_iter()
            .filter_map(|(eid, (_, deleted, props))| (!deleted).then_some((eid, props)))
            .collect())
    }

    /// Whether one unfiltered pass beats chunked `_eid IN (...)` lookups.
    ///
    /// The two strategies scale differently: a full scan costs the table, an
    /// indexed lookup costs the request. Measured at LDBC SF1 against a
    /// 17 256 038-row edge table: ~330 ms flat for the scan, against ~78 us per
    /// EID when the request is spread thinly and ~24 us when it is dense. Hence
    /// two conditions rather than one — size alone picks the wrong arm for a
    /// large *dense* request, which is a measured regression, not a hypothetical.
    ///
    /// Both arms return identical results; only the read strategy differs.
    /// Thresholds derive from the row count rather than being fixed, because the
    /// scan arm scales with the table and the lookup arm does not.
    ///
    /// The constants are fitted to one dataset on one machine. They are honest
    /// about direction and rough about magnitude; a selectivity estimate from
    /// real statistics would beat them.
    ///
    /// # Errors
    ///
    /// Returns an error if the row count cannot be read.
    async fn prefers_full_scan(
        backend: &dyn StorageBackend,
        table_name: &str,
        eids: &[Eid],
    ) -> Result<bool> {
        // Cheap tests first: a small request can never repay a scan, whatever
        // the table looks like, and this avoids the row count entirely.
        let requested = eids.len();
        if requested < MIN_EIDS_FOR_FULL_SCAN {
            return Ok(false);
        }

        // Unfiltered, so this reads fragment metadata rather than rows.
        let rows = backend.count_rows(table_name, None).await?;

        // Enough of the table to repay reading all of it.
        if requested.saturating_mul(EID_SCAN_CROSSOVER_RATIO) < rows {
            return Ok(false);
        }

        // ...and spread thinly enough that the lookups would range over it
        // anyway. Without this a *dense* request of the same size is pushed onto
        // a scan that costs it roughly double: measured at SF1, 28 909 clustered
        // EIDs took 702 ms chunked against 1 572 ms scanned, while 11 653 thinly
        // spread ones went the other way, 1 152 ms chunked against 772 ms scanned.
        let (Some(lo), Some(hi)) = (
            eids.iter().map(Eid::as_u64).min(),
            eids.iter().map(Eid::as_u64).max(),
        ) else {
            return Ok(false);
        };
        let span = hi.saturating_sub(lo) as usize;
        Ok(span.saturating_mul(EID_SPAN_RATIO) >= rows)
    }

    /// Fold one batch into the per-EID winner map, optionally filtering to `wanted`.
    ///
    /// Shared by both read strategies so the MVCC rule cannot drift between
    /// them: highest `_version` wins with tombstones in the contest, ties to the
    /// later row.
    ///
    /// # Errors
    ///
    /// Returns an error if a `props_json` blob cannot be decoded.
    fn merge_winning_props(
        batch: &RecordBatch,
        wanted: Option<&std::collections::HashSet<u64>>,
        best: &mut HashMap<Eid, (u64, bool, Properties)>,
    ) -> Result<()> {
        let eid_arr = batch
            .column_by_name("_eid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let props_arr = batch
            .column_by_name("props_json")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>());
        let ver_arr = batch
            .column_by_name("_version")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let deleted_col = batch
            .column_by_name("_deleted")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>());

        let (Some(eid_arr), Some(props_arr), Some(ver_arr)) = (eid_arr, props_arr, ver_arr) else {
            return Ok(());
        };

        for i in 0..batch.num_rows() {
            if eid_arr.is_null(i) {
                continue;
            }
            let raw_eid = eid_arr.value(i);
            if wanted.is_some_and(|w| !w.contains(&raw_eid)) {
                continue;
            }
            let eid = Eid::from(raw_eid);
            let row_version = if ver_arr.is_null(i) {
                0
            } else {
                ver_arr.value(i)
            };

            // Strictly-older rows lose; `>=` keeps the singular form's
            // tie-breaking, where the later row wins.
            if best
                .get(&eid)
                .is_some_and(|(winning, _, _)| row_version < *winning)
            {
                continue;
            }

            let deleted = deleted_col.is_some_and(|d| d.value(i));
            let props = if deleted {
                Properties::new()
            } else {
                Self::parse_props_json(props_arr, i)?
            };
            best.insert(eid, (row_version, deleted, props));
        }

        Ok(())
    }

    fn parse_props_json(arr: &arrow_array::LargeBinaryArray, idx: usize) -> Result<Properties> {
        if arr.is_null(idx) || arr.value(idx).is_empty() {
            return Ok(Properties::new());
        }
        let bytes = arr.value(idx);
        let uni_val = uni_common::cypher_value_codec::decode(bytes)
            .map_err(|e| anyhow!("Failed to decode CypherValue: {}", e))?;
        let json_val: serde_json::Value = uni_val.into();
        serde_json::from_value(json_val).map_err(|e| anyhow!("Failed to parse props_json: {}", e))
    }

    /// Find edge data (eid, src_vid, dst_vid, edge_type, props) by multiple type names in the main edges table.
    ///
    /// Returns all non-deleted edges with any of the given type names.
    /// This is used for OR relationship type queries like `[:KNOWS|HATES]`.
    ///
    /// `endpoint_filter` pushes a bounded endpoint set into the scan (review
    /// perf #5: a 1-source schemaless traversal used to materialize the whole
    /// edge type). `None` keeps the full-type scan.
    pub async fn find_edges_by_type_names(
        backend: &dyn StorageBackend,
        type_names: &[&str],
        endpoint_filter: Option<(EndpointSide, &[Vid])>,
    ) -> Result<Vec<(Eid, Vid, Vid, String, Properties)>> {
        if type_names.is_empty() {
            return Ok(Vec::new());
        }

        // MVCC (#181): NO `_deleted = false` pushdown here. The main edges
        // table is append-only (`write_batch`, an Append), so a deleted edge has
        // both a live row and a tombstone at a higher `_version`. Filtering on
        // `_deleted` would select the stale live row and discard its own
        // tombstone, resurrecting the edge — with a null endpoint if the delete
        // came from a `DETACH DELETE` whose vertex is now gone.
        //
        // Instead every row for the type is ranked by `_eid`, highest version
        // wins, and an eid whose winner is a tombstone is dropped. Same rule as
        // the sibling `find_props_by_eid`, whose comment records the identical
        // hazard for key reads.
        //
        // Cost: tombstone rows now leave storage and are discarded here, in
        // proportion to delete volume until compaction rewrites the table.
        // `not_deleted()` was a post-index row filter, never an index probe, so
        // the `type` BTree and the endpoint pushdown below still bound the scan.
        let base_filter = FilterExpr::one_of(
            "type",
            type_names.iter().map(|t| Scalar::Str((*t).to_string())),
        );

        // Winners keyed by eid, carried across every endpoint chunk below so an
        // edge whose live row and tombstone land in different chunks still ranks
        // correctly.
        let mut winners: HashMap<Eid, VersionedEdge> = HashMap::new();
        match endpoint_filter {
            None => {
                // Fetch all columns for edge data
                let batches = Self::execute_query(backend, base_filter.clone(), None).await?;
                for batch in &batches {
                    Self::rank_edges_from_batch(batch, &mut winners)?;
                }
            }
            Some((_, [])) => {}
            Some((side, vids)) => {
                // Chunked so the rendered predicate stays parseable for large sets.
                const VID_CHUNK: usize = 8192;
                for chunk in vids.chunks(VID_CHUNK) {
                    let ids = || chunk.iter().map(|v| Scalar::UInt(v.as_u64()));
                    let endpoint_clause = match side {
                        EndpointSide::Src => FilterExpr::one_of("src_vid", ids()),
                        EndpointSide::Dst => FilterExpr::one_of("dst_vid", ids()),
                        EndpointSide::Either => FilterExpr::any_of([
                            FilterExpr::one_of("src_vid", ids()),
                            FilterExpr::one_of("dst_vid", ids()),
                        ]),
                    };
                    let filter = FilterExpr::all([base_filter.clone(), endpoint_clause]);
                    let batches = Self::execute_query(backend, filter, None).await?;
                    for batch in &batches {
                        Self::rank_edges_from_batch(batch, &mut winners)?;
                    }
                }
            }
        }

        // Drop eids whose winning row is a tombstone, then sort by eid.
        //
        // Sorted because the ranking turns scan order into map order, and a
        // deterministic result is worth more than the saved sort: no current
        // consumer depends on ordering — `StorageManager` passes it straight
        // through and the traversal builds a HashMap from it — and pinning that
        // here stops an ordering assumption creeping back in.
        let mut edges: Vec<(Eid, Vid, Vid, String, Properties)> = winners
            .into_iter()
            .filter(|(_, w)| !w.deleted)
            .map(|(eid, w)| (eid, w.src_vid, w.dst_vid, w.edge_type, w.properties))
            .collect();
        edges.sort_unstable_by_key(|(eid, ..)| eid.as_u64());

        Ok(edges)
    }

    /// Fold one batch into the per-eid winners map, keeping the highest
    /// `_version` row for each edge.
    ///
    /// Ties go to the tombstone: `version > best || (version == best && deleted)`.
    /// Deliberately NOT the sibling's `version >= best` — that resolves a tie as
    /// "last row scanned wins", and Lance guarantees no scan order, so the same
    /// data could rank either way across fragments. Preferring the tombstone is
    /// order-independent and errs in the safe direction: it can under-report a
    /// live edge, never resurrect a deleted one. (`find_props_by_eid` still uses
    /// the `>=` form and should be aligned in a follow-up, with its own test.)
    ///
    /// A same-version live/dead pair is what a create-and-delete in one flush
    /// window produces; any other source of one would be a writer bug. So the
    /// drop is correct here and is not over-filtering.
    fn rank_edges_from_batch(
        batch: &RecordBatch,
        winners: &mut HashMap<Eid, VersionedEdge>,
    ) -> Result<()> {
        let Some(eid_arr) = batch
            .column_by_name("_eid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        else {
            return Ok(());
        };
        let Some(src_arr) = batch
            .column_by_name("src_vid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        else {
            return Ok(());
        };
        let Some(dst_arr) = batch
            .column_by_name("dst_vid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        else {
            return Ok(());
        };
        let type_arr = batch
            .column_by_name("type")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>());
        let props_arr = batch
            .column_by_name("props_json")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>());
        let version_arr = batch
            .column_by_name("_version")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let deleted_arr = batch
            .column_by_name("_deleted")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

        for i in 0..batch.num_rows() {
            if eid_arr.is_null(i) || src_arr.is_null(i) || dst_arr.is_null(i) {
                continue;
            }

            let eid = Eid::new(eid_arr.value(i));
            let version = version_arr
                .filter(|arr| !arr.is_null(i))
                .map(|arr| arr.value(i))
                .unwrap_or(0);
            let deleted = deleted_arr.is_some_and(|arr| !arr.is_null(i) && arr.value(i));

            let wins = match winners.get(&eid) {
                None => true,
                Some(best) => version > best.version || (version == best.version && deleted),
            };
            if !wins {
                continue;
            }

            // Decode props only for a row that actually wins, matching the
            // sibling's laziness — a churned edge can have many superseded rows.
            let properties = props_arr
                .map(|arr| Self::parse_props_json(arr, i))
                .transpose()?
                .unwrap_or_default();

            winners.insert(
                eid,
                VersionedEdge {
                    src_vid: Vid::new(src_arr.value(i)),
                    dst_vid: Vid::new(dst_arr.value(i)),
                    edge_type: type_arr
                        .filter(|arr| !arr.is_null(i))
                        .map(|arr| arr.value(i).to_string())
                        .unwrap_or_default(),
                    properties,
                    version,
                    deleted,
                },
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main_edge_schema() {
        let schema = MainEdgeDataset::get_arrow_schema();
        assert_eq!(schema.fields().len(), 9);
        assert!(schema.field_with_name("_eid").is_ok());
        assert!(schema.field_with_name("src_vid").is_ok());
        assert!(schema.field_with_name("dst_vid").is_ok());
        assert!(schema.field_with_name("type").is_ok());
        assert!(schema.field_with_name("props_json").is_ok());
        assert!(schema.field_with_name("_deleted").is_ok());
        assert!(schema.field_with_name("_version").is_ok());
        assert!(schema.field_with_name("_created_at").is_ok());
        assert!(schema.field_with_name("_updated_at").is_ok());
    }

    #[test]
    fn test_build_record_batch() {
        use uni_common::Value;
        let mut props = HashMap::new();
        props.insert("weight".to_string(), Value::Float(0.5));

        let edges = vec![(
            Eid::new(1),
            Vid::new(1),
            Vid::new(2),
            "KNOWS".to_string(),
            props,
            false,
            1u64,
        )];

        let batch = MainEdgeDataset::build_record_batch(&edges, None, None).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);
    }

    #[test]
    fn test_build_record_batch_multiple_edges() {
        use uni_common::Value;

        let edges = vec![
            (
                Eid::new(1),
                Vid::new(1),
                Vid::new(2),
                "KNOWS".to_string(),
                HashMap::from([("since".to_string(), Value::Int(2020))]),
                false,
                1u64,
            ),
            (
                Eid::new(2),
                Vid::new(2),
                Vid::new(3),
                "WORKS_AT".to_string(),
                HashMap::new(),
                false,
                2u64,
            ),
            (
                Eid::new(3),
                Vid::new(1),
                Vid::new(3),
                "KNOWS".to_string(),
                HashMap::new(),
                true, // deleted
                3u64,
            ),
        ];

        let batch = MainEdgeDataset::build_record_batch(&edges, None, None).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 9);

        // Verify type column has correct values
        let type_col = batch
            .column_by_name("type")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(type_col.value(0), "KNOWS");
        assert_eq!(type_col.value(1), "WORKS_AT");
        assert_eq!(type_col.value(2), "KNOWS");
    }

    #[test]
    fn test_build_record_batch_with_timestamps() {
        let edges = vec![(
            Eid::new(1),
            Vid::new(1),
            Vid::new(2),
            "KNOWS".to_string(),
            HashMap::new(),
            false,
            1u64,
        )];

        let mut created_at: HashMap<Eid, i64> = HashMap::new();
        created_at.insert(Eid::new(1), 1_000_000_000);

        let mut updated_at: HashMap<Eid, i64> = HashMap::new();
        updated_at.insert(Eid::new(1), 2_000_000_000);

        let batch =
            MainEdgeDataset::build_record_batch(&edges, Some(&created_at), Some(&updated_at))
                .unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Timestamp columns should exist and not be all null
        let created_col = batch.column_by_name("_created_at").unwrap();
        assert!(!created_col.is_null(0), "created_at should be populated");
    }

    /// Build a one-row main-edge batch. Mirrors the shape
    /// `test_edge_key_reads_respect_tombstone_winner` uses.
    #[cfg(test)]
    fn edge_row(
        eid: u64,
        src: u64,
        dst: u64,
        ty: &str,
        deleted: bool,
        version: u64,
    ) -> RecordBatch {
        MainEdgeDataset::build_record_batch(
            &[(
                Eid::new(eid),
                Vid::new(src),
                Vid::new(dst),
                ty.to_string(),
                HashMap::new(),
                deleted,
                version,
            )],
            None,
            None,
        )
        .unwrap()
    }

    async fn scan_backend() -> (tempfile::TempDir, crate::backend::lance::LanceDbBackend) {
        let dir = tempfile::TempDir::new().unwrap();
        let be = crate::backend::lance::LanceDbBackend::connect(dir.path().to_str().unwrap(), None)
            .await
            .unwrap();
        (dir, be)
    }

    /// #181: a type scan must honour deletion tombstones.
    ///
    /// The sibling `find_props_by_eid` was fixed for this in review C2;
    /// `find_edges_by_type_names` never was. The main edges table is
    /// append-only, so a deleted edge has both a live row and a tombstone, and
    /// filtering `_deleted = false` picks the stale live one.
    #[tokio::test]
    async fn test_type_scan_respects_tombstone_winner() {
        let (_dir, be) = scan_backend().await;
        let backend: &dyn StorageBackend = &be;

        MainEdgeDataset::write_batch(backend, edge_row(1, 1, 2, "KNOWS", false, 1))
            .await
            .unwrap();
        MainEdgeDataset::write_batch(backend, edge_row(2, 3, 4, "KNOWS", false, 1))
            .await
            .unwrap();

        let live = MainEdgeDataset::find_edges_by_type_names(backend, &["KNOWS"], None)
            .await
            .unwrap();
        assert_eq!(live.len(), 2, "both edges visible while live");

        // Tombstone eid 1 at a higher version — the winning row.
        MainEdgeDataset::write_batch(backend, edge_row(1, 1, 2, "KNOWS", true, 2))
            .await
            .unwrap();

        let after = MainEdgeDataset::find_edges_by_type_names(backend, &["KNOWS"], None)
            .await
            .unwrap();
        let eids: Vec<u64> = after.iter().map(|(e, ..)| e.as_u64()).collect();
        assert_eq!(
            eids,
            vec![2],
            "the deleted edge must not be resurrected by its own older live row"
        );
    }

    /// #181: at equal `_version`, the tombstone wins — in either write order.
    ///
    /// The sibling uses `version >= best`, i.e. last-row-scanned-wins, and Lance
    /// guarantees no scan order. Ranking here is order-independent: a tie can
    /// only under-report, never resurrect.
    #[tokio::test]
    async fn test_type_scan_tie_prefers_the_tombstone() {
        for (first_deleted, label) in [(false, "live-then-dead"), (true, "dead-then-live")] {
            let (_dir, be) = scan_backend().await;
            let backend: &dyn StorageBackend = &be;

            MainEdgeDataset::write_batch(backend, edge_row(7, 1, 2, "R", first_deleted, 5))
                .await
                .unwrap();
            MainEdgeDataset::write_batch(backend, edge_row(7, 1, 2, "R", !first_deleted, 5))
                .await
                .unwrap();

            let rows = MainEdgeDataset::find_edges_by_type_names(backend, &["R"], None)
                .await
                .unwrap();
            assert!(
                rows.is_empty(),
                "{label}: a same-version tie must resolve to the tombstone"
            );
        }
    }

    /// #181 on the endpoint-filtered branch, which takes a different scan path
    /// (chunked, with a src/dst predicate pushed down).
    #[tokio::test]
    async fn test_type_scan_endpoint_filter_respects_tombstone() {
        let (_dir, be) = scan_backend().await;
        let backend: &dyn StorageBackend = &be;

        MainEdgeDataset::write_batch(backend, edge_row(1, 1, 2, "KNOWS", false, 1))
            .await
            .unwrap();
        MainEdgeDataset::write_batch(backend, edge_row(1, 1, 2, "KNOWS", true, 2))
            .await
            .unwrap();

        let vids = [Vid::new(1), Vid::new(2)];
        for side in [EndpointSide::Src, EndpointSide::Dst, EndpointSide::Either] {
            let rows =
                MainEdgeDataset::find_edges_by_type_names(backend, &["KNOWS"], Some((side, &vids)))
                    .await
                    .unwrap();
            assert!(
                rows.is_empty(),
                "{side:?}: tombstone must win on the endpoint-filtered path too"
            );
        }
    }

    /// MVCC regression (review C2): a deletion tombstone written at a higher
    /// version must win over the older live row. `find_props_by_eid` filtered
    /// `_deleted = false` before version-ranking, so an older live version
    /// resurrected a deleted edge.
    #[tokio::test]
    async fn test_edge_key_reads_respect_tombstone_winner() {
        use crate::backend::lance::LanceDbBackend;
        use uni_common::Value;

        let dir = tempfile::TempDir::new().unwrap();
        let be = LanceDbBackend::connect(dir.path().to_str().unwrap(), None)
            .await
            .unwrap();
        let backend: &dyn StorageBackend = &be;

        let mut props = HashMap::new();
        props.insert("weight".to_string(), Value::Float(0.5));

        // v1: live edge.
        let live = MainEdgeDataset::build_record_batch(
            &[(
                Eid::new(1),
                Vid::new(1),
                Vid::new(2),
                "KNOWS".to_string(),
                props.clone(),
                false,
                1u64,
            )],
            None,
            None,
        )
        .unwrap();
        MainEdgeDataset::write_batch(backend, live).await.unwrap();

        // Sanity: visible while live.
        assert!(
            MainEdgeDataset::find_props_by_eid(backend, Eid::new(1), None)
                .await
                .unwrap()
                .is_some()
        );

        // v2: deletion tombstone at a higher version — the winning row.
        let dead = MainEdgeDataset::build_record_batch(
            &[(
                Eid::new(1),
                Vid::new(1),
                Vid::new(2),
                "KNOWS".to_string(),
                props,
                true,
                2u64,
            )],
            None,
            None,
        )
        .unwrap();
        MainEdgeDataset::write_batch(backend, dead).await.unwrap();

        assert_eq!(
            MainEdgeDataset::find_props_by_eid(backend, Eid::new(1), None)
                .await
                .unwrap(),
            None,
            "deleted (highest-version) winner must not resurrect edge props"
        );
    }
}
