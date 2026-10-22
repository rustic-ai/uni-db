// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Declared edge endpoints, checked again once a bulk load is published.
//!
//! `BulkWriter::insert_edges` checks each edge's endpoints against its type's
//! declaration when the edge is buffered. The load's edges then stay invisible
//! until the load is published, which may be much later: at the end of
//! `commit`, in the background re-drive of a load `commit` could not finish,
//! or at the next open. Meanwhile an endpoint that existed before the load can
//! lose its label or be deleted -- the label-removal guard and `DELETE` see no
//! edge on it, because the load's edges are not visible yet -- and a marker
//! written by a release that did not check endpoints at all can be completed
//! by this one.
//!
//! The load is past its commit point by then and cannot be refused. Instead,
//! every edge type it wrote whose catalog record says no stored edge violates
//! the declaration (`EndpointConformance::Enforced` or `Verified`) has its
//! endpoints checked again; a type with a violation is recorded `Unverified`,
//! so the planner stops narrowing unlabelled endpoints on it (narrowing would
//! silently drop the violating edges), and the caller reports it.

// Rust guideline compliant

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use arrow_array::Array;
use uni_common::core::id::Vid;
use uni_common::core::schema::EndpointConformance;
use uni_store::backend::table_names;
use uni_store::backend::types::{FilterExpr, Scalar, ScanRequest, cols};
use uni_store::runtime::writer::Writer;
use uni_store::storage::main_vertex::MainVertexDataset;
use uni_store::storage::manager::StorageManager;

/// VIDs per storage read when resolving endpoint labels.
const LABEL_BATCH: usize = 8_192;

/// The endpoints of one edge type's edges that have to be checked.
#[derive(Debug, Default, Clone)]
pub(crate) struct TypeEndpoints {
    /// Source VIDs.
    pub sources: HashSet<Vid>,
    /// Target VIDs.
    pub targets: HashSet<Vid>,
}

/// One edge type found violating its declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Downgrade {
    /// The edge type, now `Unverified`.
    pub edge_type: String,
    /// Source endpoints without a declared source label.
    pub source_violations: usize,
    /// Target endpoints without a declared target label.
    pub target_violations: usize,
}

impl Downgrade {
    /// The line reported for it (`BulkStats::warnings`, the log).
    pub fn describe(&self) -> String {
        format!(
            "edge type '{}': {} source and {} target endpoint(s) of the published load no \
             longer carry a declared label, so its endpoint conformance is now unverified \
             (run CALL uni.admin.verifyEdgeEndpoints() once they are fixed)",
            self.edge_type, self.source_violations, self.target_violations
        )
    }
}

/// The edge types among `edge_types` that the catalog records as conforming
/// and that declare at least one endpoint label.
pub(crate) fn conforming_types<'a>(
    storage: &StorageManager,
    edge_types: impl IntoIterator<Item = &'a String>,
) -> Vec<String> {
    let schema = storage.schema_manager().schema();
    let mut out: Vec<String> = edge_types
        .into_iter()
        .filter(|name| {
            schema.edge_types.get(name.as_str()).is_some_and(|meta| {
                meta.endpoint_conformance.is_conforming()
                    && !(meta.src_labels.is_empty() && meta.dst_labels.is_empty())
            })
        })
        .cloned()
        .collect();
    out.sort();
    out.dedup();
    out
}

/// The edge types a recorded publish touched, read from its table names.
pub(crate) fn edge_types_of_tables<'a>(
    storage: &StorageManager,
    tables: impl IntoIterator<Item = &'a String>,
) -> Vec<String> {
    let tables: HashSet<&String> = tables.into_iter().collect();
    let schema = storage.schema_manager().schema();
    let mut out: Vec<String> = schema
        .edge_types
        .keys()
        .filter(|name| tables.contains(&table_names::delta_table_name(name, "fwd")))
        .cloned()
        .collect();
    out.sort();
    out
}

/// The endpoints of the published edges of `edge_types` that carry
/// `version` (every row of one load carries its version), or of every stored
/// edge of those types when the load's version is unknown (a marker of an
/// earlier release). Read from the main edges table, deleted rows included.
pub(crate) async fn load_edge_endpoints(
    storage: &StorageManager,
    edge_types: &[String],
    version: Option<u64>,
) -> Result<BTreeMap<String, TypeEndpoints>> {
    let mut out = BTreeMap::new();
    let backend = storage.backend();
    let table = table_names::main_edge_table_name();
    if edge_types.is_empty() || !backend.table_exists(table).await? {
        return Ok(out);
    }
    for edge_type in edge_types {
        let mut filter = vec![FilterExpr::one_of("type", [Scalar::Str(edge_type.clone())])];
        if let Some(v) = version {
            filter.push(FilterExpr::one_of(cols::VERSION, [Scalar::UInt(v)]));
        }
        let request = ScanRequest::all(table)
            .with_filter(FilterExpr::all(filter))
            .with_columns(vec!["src_vid".to_string(), "dst_vid".to_string()]);
        let mut endpoints = TypeEndpoints::default();
        for batch in backend.scan(request).await? {
            let col = |name: &str| {
                batch
                    .column_by_name(name)
                    .and_then(|c| {
                        c.as_any()
                            .downcast_ref::<arrow_array::UInt64Array>()
                            .cloned()
                    })
                    .ok_or_else(|| anyhow::anyhow!("main edges table: column '{name}' missing"))
            };
            let (srcs, dsts) = (col("src_vid")?, col("dst_vid")?);
            for i in 0..batch.num_rows() {
                if !srcs.is_null(i) {
                    endpoints.sources.insert(Vid::new(srcs.value(i)));
                }
                if !dsts.is_null(i) {
                    endpoints.targets.insert(Vid::new(dsts.value(i)));
                }
            }
        }
        out.insert(edge_type.clone(), endpoints);
    }
    Ok(out)
}

/// The current labels of `vids`: a committed but unflushed change from L0
/// (current buffer, then buffers being flushed), the rest from storage in
/// batches. A vertex deleted, or absent everywhere, has no labels.
///
/// Not the in-memory `VidLabelsIndex`: it learns of a label change only when
/// the change is flushed, so it would still credit an endpoint with a label
/// whose removal is in L0.
async fn current_labels(
    storage: &StorageManager,
    writer: Option<&Writer>,
    vids: &HashSet<Vid>,
) -> Result<HashMap<Vid, Vec<String>>> {
    let mut labels: HashMap<Vid, Vec<String>> = HashMap::with_capacity(vids.len());
    let mut from_storage: Vec<Vid> = Vec::new();
    let buffers = writer.map(|w| {
        let mut b = vec![w.l0_manager.get_current()];
        b.extend(w.l0_manager.get_pending_flush());
        b
    });
    'vids: for &vid in vids {
        if let Some(buffers) = &buffers {
            for buffer in buffers {
                let guard = buffer.read();
                if guard.vertex_tombstones.contains(&vid) {
                    labels.insert(vid, Vec::new());
                    continue 'vids;
                }
                if let Some(found) = guard.get_vertex_labels(vid) {
                    labels.insert(vid, found.to_vec());
                    continue 'vids;
                }
            }
        }
        from_storage.push(vid);
    }
    for chunk in from_storage.chunks(LABEL_BATCH) {
        let found =
            MainVertexDataset::find_batch_labels_by_vids(storage.backend(), chunk, None::<u64>)
                .await?;
        for &vid in chunk {
            labels.insert(vid, found.get(&vid).cloned().unwrap_or_default());
        }
    }
    Ok(labels)
}

/// Check `endpoints` against their types' declarations and record every
/// conforming type with a violation `Unverified` in the catalog (in memory;
/// the caller persists). Types the catalog already records `Unverified`, and
/// types without a declaration, are skipped. Returns the types downgraded.
///
/// # Errors
///
/// Returns an error if labels cannot be read or the catalog cannot be
/// updated; nothing is downgraded then, and the caller must not record the
/// check as done.
pub(crate) async fn downgrade_violating_types(
    storage: &StorageManager,
    writer: Option<&Writer>,
    endpoints: &BTreeMap<String, TypeEndpoints>,
) -> Result<Vec<Downgrade>> {
    let sm = storage.schema_manager();
    let schema = sm.schema();
    let mut found = Vec::new();
    for (edge_type, ends) in endpoints {
        let Some(meta) = schema.edge_types.get(edge_type.as_str()) else {
            continue;
        };
        if !meta.endpoint_conformance.is_conforming() {
            continue;
        }
        let mut counts = [0usize; 2];
        for (side, allowed, vids) in [
            (0, &meta.src_labels, &ends.sources),
            (1, &meta.dst_labels, &ends.targets),
        ] {
            if allowed.is_empty() || vids.is_empty() {
                continue;
            }
            let labels = current_labels(storage, writer, vids).await?;
            counts[side] = vids
                .iter()
                .filter(|vid| {
                    !labels
                        .get(vid)
                        .is_some_and(|have| have.iter().any(|l| allowed.contains(l)))
                })
                .count();
        }
        if counts != [0, 0] {
            found.push(Downgrade {
                edge_type: edge_type.clone(),
                source_violations: counts[0],
                target_violations: counts[1],
            });
        }
    }
    for d in &found {
        sm.set_endpoint_conformance(&d.edge_type, EndpointConformance::Unverified)?;
        tracing::warn!(edge_type = %d.edge_type, "bulk load published: {}", d.describe());
    }
    Ok(found)
}

/// Make downgrades durable: the catalog is saved on the primary. A fork's
/// `SchemaManager` keeps the primary's path, so a save would write the fork's
/// merged view over the primary's catalog; a fork records its downgraded
/// types in its own catalog namespace instead
/// (`catalog/forks/{id}/bulk_endpoints_unverified.json`), which
/// [`apply_fork_downgrades`] applies at every open of the fork.
pub(crate) async fn persist_downgrades(
    storage: &StorageManager,
    found: &[Downgrade],
) -> Result<()> {
    if found.is_empty() {
        return Ok(());
    }
    let Some(path) = fork_record_path(storage) else {
        return storage.schema_manager().save().await;
    };
    let mut types = read_fork_record(storage, &path).await?;
    types.extend(found.iter().map(|d| d.edge_type.clone()));
    types.sort();
    types.dedup();
    uni_store::store_utils::put_with_timeout(
        &storage.store(),
        &path,
        bytes::Bytes::from(serde_json::to_vec(&types)?),
        RECORD_TIMEOUT,
    )
    .await?;
    Ok(())
}

/// Store timeout for a fork's downgrade record.
const RECORD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Where a fork records its downgraded edge types; `None` on the primary.
fn fork_record_path(storage: &StorageManager) -> Option<object_store::path::Path> {
    storage.fork_scope().map(|scope| {
        object_store::path::Path::from(format!(
            "catalog/forks/{}/bulk_endpoints_unverified.json",
            scope.fork_id()
        ))
    })
}

async fn read_fork_record(
    storage: &StorageManager,
    path: &object_store::path::Path,
) -> Result<Vec<String>> {
    match uni_store::store_utils::get_with_timeout(&storage.store(), path, RECORD_TIMEOUT).await {
        Ok(found) => Ok(serde_json::from_slice(&found.bytes().await?)?),
        Err(e) if uni_store::store_utils::is_not_found(&e) => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// On a fork, record `Unverified` in memory for every edge type a published
/// bulk load was found violating on the fork (see [`persist_downgrades`]);
/// a no-op on the primary, whose catalog carries the downgrade itself.
///
/// # Errors
///
/// Returns an error if the record cannot be read.
pub(crate) async fn apply_fork_downgrades(storage: &StorageManager) -> Result<()> {
    let Some(path) = fork_record_path(storage) else {
        return Ok(());
    };
    let sm = storage.schema_manager();
    for name in read_fork_record(storage, &path).await? {
        if sm.schema().edge_types.contains_key(name.as_str()) {
            sm.set_endpoint_conformance(&name, EndpointConformance::Unverified)?;
        }
    }
    Ok(())
}
