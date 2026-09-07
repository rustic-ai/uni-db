// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork diff & promote engine (Phase 6+).
//!
//! `compute_diff` computes the structural delta between two views; `run_promote`
//! scans a fork for matched rows and bulk-inserts them onto primary. Both are
//! generic over the [`ForkQueryHost`] / [`ForkPromoteSink`] host traits that
//! uni-db implements for its `Session`/`Transaction` types.

// ============================================================================
// Diff engine
// ============================================================================

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;
use uni_common::Properties;
use uni_common::Result;
use uni_common::Value;
use uni_common::core::id::{UniId, Vid};

use crate::host::{ForkPromoteSink, ForkQueryHost};
use crate::types::{
    ConflictPolicy, DiffEdge, DiffVertex, EdgeDiff, ForkDiff, PromoteBaseline, PromoteOptions,
    PromotePattern, PromoteReport, PropertyChange, VertexDiff, VertexPropertyChange,
};

/// Compute the structural delta between two views.
///
/// Both `a` and `b` may be primary or forked sessions. The convention is
/// *forward*: returned `ForkDiff.vertices.added` is rows present in `b`
/// but not `a`; `deleted` is rows in `a` but not `b`.
///
/// Identity is content-addressed UID for vertices and `(src_uid,
/// dst_uid)` for edges, scoped by edge type — so two unrelated forks
/// with overlapping VIDs but distinct content pair correctly.
pub async fn compute_diff<Q: ForkQueryHost + ?Sized>(a: &Q, b: &Q) -> Result<ForkDiff> {
    let mut diff = ForkDiff::default();

    // vid → ext_id per side: ext_id is folded into the content UID but stripped
    // from query results, so look it up from storage (review H4). Propagate any
    // fetch error: `get_vertex_ext_ids` already returns an empty map when the
    // vertices table is absent, so any `Err` here is a genuine scan/IO failure.
    // Swallowing it (empty map) would make one side hash every ext_id-bearing
    // vertex to a different identity, reporting unchanged rows as add+delete.
    let ext_a = a.storage().get_vertex_ext_ids().await?;
    let ext_b = b.storage().get_vertex_ext_ids().await?;

    let labels_a: HashSet<String> = a.schema().schema().labels.keys().cloned().collect();
    let labels_b: HashSet<String> = b.schema().schema().labels.keys().cloned().collect();
    let labels_union: Vec<&String> = labels_a.union(&labels_b).collect();

    for label in labels_union {
        let rows_a = scan_label_nodes(a, label, &ext_a).await?;
        let rows_b = scan_label_nodes(b, label, &ext_b).await?;
        diff_label(label, rows_a, rows_b, &mut diff.vertices);
    }

    let edges_a: HashSet<String> = a.schema().schema().edge_types.keys().cloned().collect();
    let edges_b: HashSet<String> = b.schema().schema().edge_types.keys().cloned().collect();
    let edges_union: Vec<&String> = edges_a.union(&edges_b).collect();

    for edge_type in edges_union {
        let rows_a = scan_edge_type(a, edge_type, &ext_a).await?;
        let rows_b = scan_edge_type(b, edge_type, &ext_b).await?;
        diff_edge_type(edge_type, rows_a, rows_b, &mut diff.edges);
    }

    Ok(diff)
}

/// A vertex's `ext_id` for content-UID computation, from a `vid → ext_id` map
/// sourced from storage (`StorageManager::get_vertex_ext_ids`).
///
/// `ext_id` is folded into the storage `_uid` but is stripped from query
/// results, so the diff can't recover it by re-hashing query rows — two
/// vertices differing only by `ext_id` would collapse to one identity (review
/// H4). We fold it back into the *recomputed* UID (not the storage `_uid`,
/// which diverges from a recompute and breaks L0/flushed consistency). Vertices
/// without an `ext_id` are absent from the map → `None`, i.e. unchanged
/// behavior. Limitation: covers flushed rows; a vertex created fork-local and
/// not yet flushed is absent from the map (its `ext_id` collapse only matters
/// for promote, and flushing the fork closes it).
fn ext_id_for(map: &HashMap<Vid, String>, vid: Vid) -> Option<&str> {
    map.get(&vid).map(String::as_str)
}

/// Content UID for a query-stripped vertex, matching the registered UID.
///
/// The write path (`writer.rs` flush finalize) hashes the *stored* property
/// map, which still carries the `ext_id` key, so `ext_id` folds into the digest
/// twice: once via the dedicated argument and once via the `"ext_id"` property.
/// Cypher results strip that key, so re-inject it here before hashing to
/// reproduce the registered digest exactly — otherwise an `ext_id`-bearing
/// row's content UID never matches and the promote dedup silently never fires.
///
/// When `ext_id` is `None` the props are hashed unchanged (a no-op re-injection).
fn content_uid_with_ext_id(
    label: &str,
    ext_id: Option<&str>,
    stripped_props: &Properties,
) -> UniId {
    use uni_store::storage::vertex::VertexDataset;
    match ext_id {
        Some(eid) => {
            let mut props = stripped_props.clone();
            props.insert("ext_id".to_string(), Value::String(eid.to_string()));
            VertexDataset::compute_vertex_uid(label, Some(eid), &props)
        }
        None => VertexDataset::compute_vertex_uid(label, None, stripped_props),
    }
}

/// One bucketed vertex row keyed by content UID.
type VertexBucket = HashMap<UniId, VertexRow>;
/// One bucketed edge row keyed by content-addressed edge UID
/// (`compute_edge_uid(src_uid, dst_uid, type, properties)`). Two
/// parallel edges between the same endpoints with different property
/// bags hash to different keys and therefore appear as distinct
/// entries — that's the Phase 7d multi-edge semantics.
type EdgeBucket = HashMap<UniId, EdgeRow>;

#[derive(Debug, Clone)]
struct VertexRow {
    label: String,
    vid: Vid,
    properties: Properties,
}

#[derive(Debug, Clone)]
struct EdgeRow {
    src_uid: UniId,
    dst_uid: UniId,
    properties: Properties,
}

async fn scan_label_nodes<Q: ForkQueryHost + ?Sized>(
    s: &Q,
    label: &str,
    ext_ids: &HashMap<Vid, String>,
) -> Result<VertexBucket> {
    use uni_store::storage::vertex::VertexDataset;
    let cypher = format!("MATCH (n:`{}`) RETURN n", escape_backticks(label));
    let result = s.query(&cypher).await?;
    let mut bucket = VertexBucket::new();
    for row in result.rows() {
        let Some(Value::Node(node)) = row.value("n") else {
            continue;
        };
        // The MATCH already filters to nodes carrying `label`, so the bucketed
        // row's label is always `label`. Fold the stored `ext_id` into the UID
        // so ext_id-distinct vertices don't collapse (review H4).
        let uid = VertexDataset::compute_vertex_uid(
            label,
            ext_id_for(ext_ids, node.vid),
            &node.properties,
        );
        if bucket
            .insert(
                uid,
                VertexRow {
                    label: label.to_string(),
                    vid: node.vid,
                    properties: node.properties.clone(),
                },
            )
            .is_some()
        {
            // Two distinct vertices hashed to the same content UID — one will be
            // dropped from the diff. Observable signal for residual identity
            // collisions (review H4).
            warn!(
                label,
                vid = node.vid.as_u64(),
                "fork diff: vertex content-UID collision; a row is being shadowed"
            );
        }
    }
    Ok(bucket)
}

async fn scan_edge_type<Q: ForkQueryHost + ?Sized>(
    s: &Q,
    edge_type: &str,
    ext_ids: &HashMap<Vid, String>,
) -> Result<EdgeBucket> {
    use uni_store::storage::main_edge::MainEdgeDataset;
    use uni_store::storage::vertex::VertexDataset;
    let cypher = format!(
        "MATCH (a)-[r:`{}`]->(b) RETURN a, r, b",
        escape_backticks(edge_type)
    );
    let result = s.query(&cypher).await?;
    let mut bucket = EdgeBucket::new();
    for row in result.rows() {
        let (Some(Value::Edge(edge)), Some(Value::Node(a)), Some(Value::Node(b))) =
            (row.value("r"), row.value("a"), row.value("b"))
        else {
            continue;
        };
        let a_label = a.labels.first().cloned().unwrap_or_default();
        let b_label = b.labels.first().cloned().unwrap_or_default();
        let src_uid =
            VertexDataset::compute_vertex_uid(&a_label, ext_id_for(ext_ids, a.vid), &a.properties);
        let dst_uid =
            VertexDataset::compute_vertex_uid(&b_label, ext_id_for(ext_ids, b.vid), &b.properties);
        let edge_uid =
            MainEdgeDataset::compute_edge_uid(&src_uid, &dst_uid, edge_type, &edge.properties);
        if bucket
            .insert(
                edge_uid,
                EdgeRow {
                    src_uid,
                    dst_uid,
                    properties: edge.properties.clone(),
                },
            )
            .is_some()
        {
            warn!(
                edge_type,
                "fork diff: edge content-UID collision; a row is being shadowed"
            );
        }
    }
    Ok(bucket)
}

/// Split two content-keyed buckets into *added* (present in `b`, not `a`)
/// and *deleted* (present in `a`, not `b`) rows, moving each row out of its
/// owning map via the supplied builders. Returns the rows shared by both
/// buckets (`(uid, row_a, row_b)`) so the caller can diff their properties.
fn partition_added_deleted<R, A, D>(
    mut a: HashMap<UniId, R>,
    mut b: HashMap<UniId, R>,
    mut mk_added: A,
    mut mk_deleted: D,
) -> Vec<(UniId, R, R)>
where
    A: FnMut(UniId, R),
    D: FnMut(UniId, R),
{
    let keys_a: HashSet<UniId> = a.keys().copied().collect();
    let keys_b: HashSet<UniId> = b.keys().copied().collect();

    let mut common = Vec::new();
    for uid in &keys_b {
        if !keys_a.contains(uid) {
            mk_added(*uid, b.remove(uid).expect("key from keys_b"));
        }
    }
    for uid in &keys_a {
        if keys_b.contains(uid) {
            let row_a = a.remove(uid).expect("key from keys_a");
            let row_b = b.remove(uid).expect("shared key in b");
            common.push((*uid, row_a, row_b));
        } else {
            mk_deleted(*uid, a.remove(uid).expect("key from keys_a"));
        }
    }
    common
}

fn diff_label(label: &str, a: VertexBucket, b: VertexBucket, out: &mut VertexDiff) {
    let common = partition_added_deleted(
        a,
        b,
        |uid, row| {
            out.added.push(DiffVertex {
                label: row.label,
                uid,
                vid: Some(row.vid),
                properties: row.properties,
            });
        },
        |uid, row| {
            out.deleted.push(DiffVertex {
                label: row.label,
                uid,
                vid: Some(row.vid),
                properties: row.properties,
            });
        },
    );
    for (uid, row_a, row_b) in common {
        let changes = property_changes(&row_a.properties, &row_b.properties);
        if !changes.is_empty() {
            out.changed.push(VertexPropertyChange {
                label: label.to_string(),
                uid,
                changes,
            });
        }
    }
}

fn diff_edge_type(edge_type: &str, a: EdgeBucket, b: EdgeBucket, out: &mut EdgeDiff) {
    // Note: under content-addressed identity, two edges with the same
    // edge_uid have, by construction, identical (src, dst, type,
    // properties) — so the shared (intersection) rows cannot contain a
    // property difference. The `changed` branch is intentionally
    // unreachable under multi-edge semantics; property mutations surface
    // as added+deleted of distinct edge UIDs. `EdgePropertyChange` remains
    // in the public API for forward compatibility with a future identity
    // model that anchors on a stable edge id. We therefore discard the
    // common rows.
    partition_added_deleted(
        a,
        b,
        |edge_uid, row| {
            out.added.push(DiffEdge {
                edge_type: edge_type.to_string(),
                edge_uid,
                src_uid: row.src_uid,
                dst_uid: row.dst_uid,
                properties: row.properties,
            });
        },
        |edge_uid, row| {
            out.deleted.push(DiffEdge {
                edge_type: edge_type.to_string(),
                edge_uid,
                src_uid: row.src_uid,
                dst_uid: row.dst_uid,
                properties: row.properties,
            });
        },
    );
}

fn property_changes(a: &Properties, b: &Properties) -> Vec<PropertyChange> {
    let mut changes = Vec::new();
    let keys: HashSet<&String> = a.keys().chain(b.keys()).collect();
    let mut sorted: Vec<&String> = keys.into_iter().collect();
    sorted.sort();
    for k in sorted {
        let va = a.get(k);
        let vb = b.get(k);
        if va != vb {
            changes.push(PropertyChange {
                key: k.clone(),
                before: va.cloned(),
                after: vb.cloned(),
            });
        }
    }
    changes
}

fn escape_backticks(s: &str) -> String {
    s.replace('`', "``")
}

/// Render an iterator of VID-bearing values as a comma-separated list of
/// their `u64` ids for a Cypher `id(n) IN [...]` clause.
fn vid_in_list(vids: impl IntoIterator<Item = u64>) -> String {
    vids.into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve a set of UIDs to their primary VIDs in two queries
/// regardless of the input size.
///
/// Returns a `HashMap<UniId, Vid>` containing only those UIDs that
/// successfully resolve to a *primary* VID (i.e., a candidate VID
/// from the shared `UidIndex` is actually present in primary's view
/// of the label's vertex table). UIDs absent from the result map
/// either had no candidate registered or all candidates pointed at
/// fork-only rows.
///
/// Two queries per call regardless of `uids.len()`: one IN-filter
/// scan of `UidIndex`'s dataset (collecting **all** registered VIDs
/// per UID — `UidIndex::resolve_uids` collapses to one VID per UID
/// which loses fork/primary disambiguation), and one primary Cypher
/// MATCH with an `id(n) IN [...]` predicate returning each live node.
///
/// A candidate UID counts as present only when a resolved VID is both
/// live on primary **and** whose current content still hashes to that
/// exact UID. The shared `UidIndex` is append-only (`writer.rs` only
/// appends), so it never drops a vertex's pre-edit content-UID; a stale
/// mapping can therefore resolve to a still-live vid whose properties
/// have since changed. The live-content check rejects those stale
/// matches, so a fork row diverging from a primary edit is correctly
/// treated as absent and inserted as a twin. `primary_ext_ids` supplies
/// each primary vid's `ext_id` to recompute the same double-folded UID.
async fn batch_resolve_primary_vids<Q: ForkQueryHost + ?Sized>(
    primary: &Q,
    primary_storage: &Arc<uni_store::storage::manager::StorageManager>,
    label: &str,
    uids: &[UniId],
    primary_ext_ids: &HashMap<Vid, String>,
) -> (HashMap<UniId, Vid>, bool) {
    // NOTE: every error path below degrades to whatever has been
    // resolved so far (an empty or partial map) rather than
    // propagating. This is deliberate: `run_promote` treats an
    // unresolved UID as "not present on primary" and inserts it, so a
    // transient resolve failure must not abort the promote. The returned
    // `degraded` flag (M5) tells the caller that "absent" was inferred
    // from a failed resolve, so the resulting inserts are unverified and
    // may be duplicates — surfaced as `vertices_inserted_unverified`.
    let mut out: HashMap<UniId, Vid> = HashMap::new();
    if uids.is_empty() {
        return (out, false);
    }
    // Collect *all* candidate VIDs per UID by scanning the shared
    // UidIndex with an IN filter. The shared index is not
    // branch-isolated, so a single UID may have a fork-only VID and
    // a primary VID both registered — we keep both and let the
    // primary Cypher MATCH below decide which is real.
    // A missing index or a failed scan both degrade to "not present" (the
    // `degraded` flag tells the caller the resulting inserts are unverified).
    let Ok(uix) = primary_storage.uid_index(label) else {
        return (out, true);
    };
    let Ok(candidates_per_uid) = uix.resolve_all_vids(uids).await else {
        return (out, true);
    };
    if candidates_per_uid.is_empty() {
        return (out, false);
    }
    // Single Cypher with IN clause over every candidate VID across
    // every UID. Primary's branched backend filters out fork-only
    // VIDs naturally — they have no row in the primary view.
    let vid_set: HashSet<u64> = candidates_per_uid
        .values()
        .flat_map(|vs| vs.iter().map(|v| v.as_u64()))
        .collect();
    // Return the live node (not just its id) so we can re-verify the resolved
    // vertex's CURRENT content, not merely its liveness — the shared UidIndex
    // never drops a pre-edit content-UID, so presence alone would let a fork row
    // dedup against a stale UID pointing at a since-edited primary vertex.
    let cypher = format!(
        "MATCH (n:`{}`) WHERE id(n) IN [{}] RETURN n",
        escape_backticks(label),
        vid_in_list(vid_set)
    );
    let Ok(rs) = primary.query(&cypher).await else {
        return (out, true);
    };
    // vid → live content-UID for every present candidate vertex, recomputed from
    // its current props with `ext_id` re-injected to mirror the registered UID.
    let mut live_uid_by_vid: HashMap<u64, UniId> = HashMap::new();
    for row in rs.rows() {
        let Some(Value::Node(node)) = row.value("n") else {
            continue;
        };
        let live_uid = content_uid_with_ext_id(
            label,
            ext_id_for(primary_ext_ids, node.vid),
            &node.properties,
        );
        live_uid_by_vid.insert(node.vid.as_u64(), live_uid);
    }
    for (uid, vids) in candidates_per_uid {
        // The UID is present on primary only if some candidate VID is both live
        // AND still hashes to this exact UID — rejecting stale post-edit mappings.
        if let Some(vid) = vids
            .into_iter()
            .find(|v| live_uid_by_vid.get(&v.as_u64()) == Some(&uid))
        {
            out.insert(uid, vid);
        }
    }
    (out, false)
}

/// Resolve fork candidate vertices to existing primary VIDs by their
/// stable `(label, ext_id)` identity, returning each match's current
/// primary properties for the upsert equality check.
///
/// Unlike [`batch_resolve_primary_vids`] (which keys by mutable
/// content-UID and so cannot recognize an *edited* vertex), this keys by
/// the immutable `ext_id`, so a fork edit resolves to the same primary
/// vertex instead of looking like a brand-new row. Fork rows whose
/// `ext_id` is absent are not returned here and fall back to the
/// content-UID path.
///
/// A failed primary round-trip degrades to an empty map (treated as "not
/// present" → insert), matching the deliberate non-aborting contract — but the
/// second element of the returned pair says so, exactly as
/// [`batch_resolve_primary_vids`] does. Without it an edited fork vertex whose
/// lookup failed was inserted as a DUPLICATE instead of updated in place, and
/// a delete-promotion was silently skipped, both reported as a clean promote.
async fn batch_resolve_primary_by_ext_id<Q: ForkQueryHost + ?Sized>(
    primary: &Q,
    primary_ext_ids: &HashMap<Vid, String>,
    label: &str,
    ext_ids: &HashSet<String>,
) -> (HashMap<String, (Vid, Properties)>, bool) {
    let mut out: HashMap<String, (Vid, Properties)> = HashMap::new();
    if ext_ids.is_empty() {
        return (out, false);
    }
    // Invert primary's vid→ext_id map for just the candidate ext_ids.
    // `get_vertex_ext_ids` is not label-scoped, so the Cypher below
    // confirms the label (and fetches current props).
    let mut ext_to_vid: HashMap<String, Vid> = HashMap::new();
    for (vid, eid) in primary_ext_ids {
        if ext_ids.contains(eid) {
            ext_to_vid.insert(eid.clone(), *vid);
        }
    }
    if ext_to_vid.is_empty() {
        return (out, false);
    }
    let cypher = format!(
        "MATCH (n:`{}`) WHERE id(n) IN [{}] RETURN id(n) AS vid, n AS node",
        escape_backticks(label),
        vid_in_list(ext_to_vid.values().map(|v| v.as_u64()))
    );
    let Ok(rs) = primary.query(&cypher).await else {
        return (out, true);
    };
    let mut vid_to_props: HashMap<u64, Properties> = HashMap::new();
    for row in rs.rows() {
        if let Ok(vid) = row.get::<i64>("vid")
            && let Some(Value::Node(node)) = row.value("node")
        {
            vid_to_props.insert(vid as u64, node.properties.clone());
        }
    }
    for (eid, vid) in ext_to_vid {
        if let Some(props) = vid_to_props.get(&vid.as_u64()) {
            out.insert(eid, (vid, props.clone()));
        }
    }
    (out, false)
}

// ============================================================================
// Promote engine
// ============================================================================

/// Scan a fork session for matches per pattern, then bulk-insert the
/// matched vertices on primary (deduplicated by content-derived UID)
/// and edges (deduplicated by `(src_uid, dst_uid, edge_type)`).
///
/// Edges whose endpoints don't exist on primary by UID are skipped and
/// counted in `edges_skipped_no_endpoint` — promote the missing
/// vertices first via a vertex pattern, then re-run.
///
/// If the call contains no edge patterns, incidental edges on the fork
/// are counted in `edges_skipped` and a tracing warning is emitted.
pub async fn run_promote<Q, S>(
    fork: &Q,
    primary: &Q,
    primary_tx: &S,
    patterns: &[PromotePattern],
    options: &PromoteOptions,
    baseline: Option<&PromoteBaseline>,
) -> Result<PromoteReport>
where
    Q: ForkQueryHost + ?Sized,
    S: ForkPromoteSink + ?Sized,
{
    use uni_store::storage::vertex::VertexDataset;

    let mut report = PromoteReport {
        per_pattern_inserted: vec![0usize; patterns.len()],
        ..Default::default()
    };

    let primary_storage = primary.storage();
    // vid → ext_id maps so promote keys candidates by the same ext_id-aware
    // content UID, distinguishing ext_id-distinct rows (review H4). Propagate any
    // fetch error: an empty map is only correct when the table is genuinely
    // absent (the callee already handles that), so a swallowed transient failure
    // would make the delete-promotion pass read every baseline ext_id row as
    // "deleted on the fork" and mass-delete live primary vertices.
    let fork_ext_ids = fork.storage().get_vertex_ext_ids().await?;
    let primary_ext_ids = primary_storage.get_vertex_ext_ids().await?;
    let mut any_edge_pattern = false;
    // Cache of vertices just promoted inside this call. Edge patterns
    // check this before falling back to primary's UidIndex + Cypher
    // verify — pending tx_l0 writes aren't visible to a primary
    // Cypher round-trip until commit, so without this cache an edge
    // pattern in the same call wouldn't see endpoints we just added.
    let mut just_inserted: HashMap<(String, UniId), Vid> = HashMap::new();

    for (idx, pattern) in patterns.iter().enumerate() {
        match pattern {
            PromotePattern::Vertex {
                label,
                where_clause,
            } => {
                let cypher = match where_clause {
                    Some(w) => format!(
                        "MATCH (n:`{}`) WHERE {} RETURN n",
                        escape_backticks(label),
                        w
                    ),
                    None => format!("MATCH (n:`{}`) RETURN n", escape_backticks(label)),
                };

                let result = fork.query(&cypher).await?;
                if result.rows().is_empty() {
                    continue;
                }

                // First pass: extract (uid, props, ext_id) for every fork
                // row, skipping rows already in the within-call cache.
                let mut candidates: Vec<(UniId, Properties, Option<String>)> =
                    Vec::with_capacity(result.rows().len());
                for row in result.rows() {
                    let Some(Value::Node(node)) = row.value("n") else {
                        continue;
                    };
                    let ext_id = ext_id_for(&fork_ext_ids, node.vid).map(str::to_string);
                    // Re-inject `ext_id` so this recomputed content UID matches the
                    // one registered by writer.rs (which hashes stored props that
                    // still carry the `ext_id` key). See `content_uid_with_ext_id`.
                    // A stale pre-edit UID that still resolves to a live-but-edited
                    // primary vertex is rejected by the live-content check in
                    // `batch_resolve_primary_vids`, so this no longer risks the
                    // insert-only-twin regression that previously forced a deferral.
                    let uid = content_uid_with_ext_id(label, ext_id.as_deref(), &node.properties);
                    if just_inserted.contains_key(&(label.clone(), uid)) {
                        report.vertices_skipped_uid_conflict += 1;
                        continue;
                    }
                    candidates.push((uid, node.properties.clone(), ext_id));
                }

                // M4 upsert: resolve ext_id-bearing candidates against
                // primary by their stable `(label, ext_id)` identity so a
                // fork EDIT updates the existing vertex instead of inserting
                // a twin. Only consulted when `options.upsert`.
                let mut ext_resolve_degraded = false;
                let ext_resolved: HashMap<String, (Vid, Properties)> = if options.upsert {
                    let ext_ids: HashSet<String> = candidates
                        .iter()
                        .filter_map(|(_, _, e)| e.clone())
                        .collect();
                    let (m, degraded) =
                        batch_resolve_primary_by_ext_id(primary, &primary_ext_ids, label, &ext_ids)
                            .await;
                    // A degraded ext_id resolve makes an EDITED fork vertex look
                    // absent, so it is inserted as a duplicate instead of
                    // updated in place. Same meaning as the content-UID path's
                    // flag, so it feeds the same counter.
                    ext_resolve_degraded = degraded;
                    m
                } else {
                    HashMap::new()
                };
                if ext_resolve_degraded {
                    warn!(
                        label = %label,
                        "promote could not resolve primary twins by ext_id; edited rows may be \
                         inserted as duplicates instead of updated in place"
                    );
                }

                // Per-label fork-point baseline (merge mode only).
                let label_baseline = baseline.and_then(|b| b.ext.get(label));

                // Partition: ext_id matches become in-place upserts; every
                // other candidate flows through the content-UID
                // insert-or-skip path (unchanged legacy behavior).
                let mut uid_candidates: Vec<(UniId, Properties)> =
                    Vec::with_capacity(candidates.len());
                for (uid, props, ext_id) in candidates {
                    let resolved = ext_id
                        .as_ref()
                        .and_then(|e| ext_resolved.get(e).map(|r| (e.clone(), r)));
                    let Some((eid, (pvid, pprops))) = resolved else {
                        uid_candidates.push((uid, props));
                        continue;
                    };
                    match label_baseline.and_then(|m| m.get(&eid)) {
                        // Baseline-aware merge (with_merge): reconcile the
                        // fork value `props` against primary-now `pprops` and
                        // the fork-point baseline `b`.
                        Some(b) => {
                            if props == *pprops {
                                // Already converged — keeps re-promote
                                // idempotent. Must be checked first.
                                report.vertices_skipped_no_op += 1;
                            } else if props == *b {
                                // Fork left this vertex untouched since the
                                // fork point — never revert primary's edit.
                                report.vertices_skipped_no_op += 1;
                            } else if *pprops != *b {
                                // Both sides moved off baseline → conflict.
                                report.vertices_conflicting += 1;
                                if options.on_conflict == ConflictPolicy::Overwrite {
                                    primary_tx
                                        .update_vertex_properties(label, *pvid, props)
                                        .await?;
                                    report.vertices_updated += 1;
                                }
                            } else {
                                // Only the fork changed → clean fast-forward.
                                primary_tx
                                    .update_vertex_properties(label, *pvid, props)
                                    .await?;
                                report.vertices_updated += 1;
                            }
                        }
                        // No baseline for this ext_id: fork-wins upsert.
                        None => {
                            if props == *pprops {
                                report.vertices_skipped_no_op += 1;
                            } else {
                                primary_tx
                                    .update_vertex_properties(label, *pvid, props)
                                    .await?;
                                report.vertices_updated += 1;
                            }
                        }
                    }
                }

                // Batch-resolve the remaining candidates by content-UID.
                // Two queries total per pattern (UidIndex.resolve_uids +
                // Cypher IN-clause verify) instead of 2N. `degraded` (M5)
                // signals the resolve could not confirm presence.
                let uids_to_check: Vec<UniId> = uid_candidates.iter().map(|(u, _)| *u).collect();
                let (on_primary, degraded) = batch_resolve_primary_vids(
                    primary,
                    &primary_storage,
                    label,
                    &uids_to_check,
                    &primary_ext_ids,
                )
                .await;

                let mut to_insert: Vec<Properties> = Vec::with_capacity(uid_candidates.len());
                let mut insert_uids: Vec<UniId> = Vec::with_capacity(uid_candidates.len());
                for (uid, props) in uid_candidates {
                    if on_primary.contains_key(&uid) {
                        report.vertices_skipped_uid_conflict += 1;
                    } else {
                        to_insert.push(props);
                        insert_uids.push(uid);
                    }
                }

                if !to_insert.is_empty() {
                    let n = to_insert.len();
                    let vids = primary_tx.bulk_insert_vertices(label, to_insert).await?;
                    for (uid, vid) in insert_uids.into_iter().zip(vids) {
                        just_inserted.insert((label.clone(), uid), vid);
                    }
                    report.vertices_inserted += n;
                    report.per_pattern_inserted[idx] = n;
                    // M5: presence could not be confirmed for this batch, so
                    // some of these inserts may be duplicates of existing
                    // primary rows. Surface it instead of silently dup'ing.
                    if degraded {
                        report.vertices_inserted_unverified += n;
                        warn!(
                            label = %label,
                            count = n,
                            "promote inserted vertices whose primary presence could not be \
                             confirmed (resolve degraded); they may be duplicates"
                        );
                    }
                }
            }
            PromotePattern::Edge {
                edge_type,
                where_clause,
            } => {
                any_edge_pattern = true;
                let cypher = match where_clause {
                    Some(w) => format!(
                        "MATCH (a)-[r:`{}`]->(b) WHERE {} RETURN a, r, b",
                        escape_backticks(edge_type),
                        w
                    ),
                    None => format!(
                        "MATCH (a)-[r:`{}`]->(b) RETURN a, r, b",
                        escape_backticks(edge_type)
                    ),
                };

                let result = fork.query(&cypher).await?;
                if result.rows().is_empty() {
                    continue;
                }

                use uni_store::storage::main_edge::MainEdgeDataset;

                // First pass: extract every fork edge into a typed
                // record so we can batch-resolve endpoints and
                // pre-fetch primary parallel edges in one shot each.
                struct ForkEdgeRow {
                    a_label: String,
                    b_label: String,
                    src_uid: UniId,
                    dst_uid: UniId,
                    edge_uid: UniId,
                    edge_props: Properties,
                }
                let mut fork_edges: Vec<ForkEdgeRow> = Vec::with_capacity(result.rows().len());
                for row in result.rows() {
                    let (Some(Value::Edge(edge)), Some(Value::Node(a)), Some(Value::Node(b))) =
                        (row.value("r"), row.value("a"), row.value("b"))
                    else {
                        continue;
                    };
                    let (Some(a_label), Some(b_label)) = (a.labels.first(), b.labels.first())
                    else {
                        continue;
                    };
                    let (a_label, b_label) = (a_label.clone(), b_label.clone());
                    let src_uid = VertexDataset::compute_vertex_uid(
                        &a_label,
                        ext_id_for(&fork_ext_ids, a.vid),
                        &a.properties,
                    );
                    let dst_uid = VertexDataset::compute_vertex_uid(
                        &b_label,
                        ext_id_for(&fork_ext_ids, b.vid),
                        &b.properties,
                    );
                    let edge_uid = MainEdgeDataset::compute_edge_uid(
                        &src_uid,
                        &dst_uid,
                        edge_type,
                        &edge.properties,
                    );
                    fork_edges.push(ForkEdgeRow {
                        a_label,
                        b_label,
                        src_uid,
                        dst_uid,
                        edge_uid,
                        edge_props: edge.properties.clone(),
                    });
                }

                // Group endpoints by label so we can batch-resolve
                // each label's UIDs in a single round-trip.
                let mut to_resolve: HashMap<String, HashSet<UniId>> = HashMap::new();
                for fe in &fork_edges {
                    if !just_inserted.contains_key(&(fe.a_label.clone(), fe.src_uid)) {
                        to_resolve
                            .entry(fe.a_label.clone())
                            .or_default()
                            .insert(fe.src_uid);
                    }
                    if !just_inserted.contains_key(&(fe.b_label.clone(), fe.dst_uid)) {
                        to_resolve
                            .entry(fe.b_label.clone())
                            .or_default()
                            .insert(fe.dst_uid);
                    }
                }
                let mut endpoint_resolved: HashMap<(String, UniId), Vid> = HashMap::new();
                let mut endpoints_degraded = false;
                for (lbl, uid_set) in to_resolve {
                    let uid_vec: Vec<UniId> = uid_set.into_iter().collect();
                    let (resolved, degraded) = batch_resolve_primary_vids(
                        primary,
                        &primary_storage,
                        &lbl,
                        &uid_vec,
                        &primary_ext_ids,
                    )
                    .await;
                    // The identical call on the vertex path consumes this; here
                    // it was bound to `_degraded` and dropped, so an endpoint
                    // that merely failed to resolve was reported as absent.
                    endpoints_degraded |= degraded;
                    for (uid, vid) in resolved {
                        endpoint_resolved.insert((lbl.clone(), uid), vid);
                    }
                }
                // Seed with just_inserted cache hits.
                for ((lbl, uid), vid) in just_inserted.iter() {
                    endpoint_resolved.insert((lbl.clone(), *uid), *vid);
                }

                // Pre-fetch primary's parallel edges for dedup: one
                // query covering every (src_vid, dst_vid) pair across
                // all resolved fork edges. Hash by computed edge UID.
                let mut resolved_pairs: HashSet<(Vid, Vid)> = HashSet::new();
                for fe in &fork_edges {
                    let s = endpoint_resolved.get(&(fe.a_label.clone(), fe.src_uid));
                    let d = endpoint_resolved.get(&(fe.b_label.clone(), fe.dst_uid));
                    if let (Some(s), Some(d)) = (s, d) {
                        resolved_pairs.insert((*s, *d));
                    }
                }
                let mut primary_edge_uids: HashSet<UniId> = HashSet::new();
                let mut dedup_degraded = false;
                if !resolved_pairs.is_empty() {
                    let src_vids: HashSet<u64> =
                        resolved_pairs.iter().map(|(s, _)| s.as_u64()).collect();
                    let dst_vids: HashSet<u64> =
                        resolved_pairs.iter().map(|(_, d)| d.as_u64()).collect();
                    let dedup_cypher = format!(
                        "MATCH (a)-[r:`{}`]->(b) \
                         WHERE id(a) IN [{}] AND id(b) IN [{}] \
                         RETURN a, r, b",
                        escape_backticks(edge_type),
                        vid_in_list(src_vids),
                        vid_in_list(dst_vids),
                    );
                    // An `if let Ok` here left `primary_edge_uids` empty on a
                    // query failure, so EVERY fork edge looked new and promote
                    // inserted duplicates on primary — reported as a clean
                    // `edges_inserted` with `edges_skipped_duplicate = 0`, and
                    // durable. Record the failure instead of assuming absence.
                    match primary.query(&dedup_cypher).await {
                        Err(e) => {
                            dedup_degraded = true;
                            warn!(
                                edge_type = %edge_type,
                                error = %e,
                                "promote could not pre-fetch primary's existing edges for \
                                 dedup; inserted edges may be duplicates"
                            );
                        }
                        Ok(rs) => {
                            for row in rs.rows() {
                                let (
                                    Some(Value::Edge(existing)),
                                    Some(Value::Node(ea)),
                                    Some(Value::Node(eb)),
                                ) = (row.value("r"), row.value("a"), row.value("b"))
                                else {
                                    continue;
                                };
                                let ea_label = ea.labels.first().cloned().unwrap_or_default();
                                let eb_label = eb.labels.first().cloned().unwrap_or_default();
                                let esrc = VertexDataset::compute_vertex_uid(
                                    &ea_label,
                                    ext_id_for(&primary_ext_ids, ea.vid),
                                    &ea.properties,
                                );
                                let edst = VertexDataset::compute_vertex_uid(
                                    &eb_label,
                                    ext_id_for(&primary_ext_ids, eb.vid),
                                    &eb.properties,
                                );
                                let euid = MainEdgeDataset::compute_edge_uid(
                                    &esrc,
                                    &edst,
                                    edge_type,
                                    &existing.properties,
                                );
                                primary_edge_uids.insert(euid);
                            }
                        }
                    }
                }

                // Second pass: classify each fork edge against the
                // resolved endpoints and primary edge-UID set. Edges
                // are accumulated and bulk-inserted in one call.
                let mut edges_to_insert: Vec<(Vid, Vid, Properties)> =
                    Vec::with_capacity(fork_edges.len());
                let mut pattern_inserted = 0usize;
                for fe in fork_edges {
                    let src_vid = endpoint_resolved
                        .get(&(fe.a_label.clone(), fe.src_uid))
                        .copied();
                    let dst_vid = endpoint_resolved
                        .get(&(fe.b_label.clone(), fe.dst_uid))
                        .copied();
                    let (src_vid, dst_vid) = match (src_vid, dst_vid) {
                        (Some(s), Some(d)) => (s, d),
                        _ => {
                            report.edges_skipped_no_endpoint += 1;
                            continue;
                        }
                    };
                    if primary_edge_uids.contains(&fe.edge_uid) {
                        report.edges_skipped_duplicate += 1;
                        continue;
                    }
                    edges_to_insert.push((src_vid, dst_vid, fe.edge_props));
                    pattern_inserted += 1;
                }
                if !edges_to_insert.is_empty() {
                    let n = edges_to_insert.len();
                    primary_tx
                        .bulk_insert_edges(edge_type, edges_to_insert)
                        .await?;
                    report.edges_inserted += n;
                    // Mirrors `vertices_inserted_unverified` on the vertex
                    // path: either the dedup pre-fetch or the endpoint resolve
                    // degraded, so these inserts may duplicate rows that are
                    // already on primary.
                    if dedup_degraded || endpoints_degraded {
                        report.edges_inserted_unverified += n;
                        warn!(
                            edge_type = %edge_type,
                            count = n,
                            "promote inserted edges whose primary presence could not be \
                             confirmed (resolve degraded); they may be duplicates"
                        );
                    }
                }
                report.per_pattern_inserted[idx] = pattern_inserted;
            }
        }
    }

    // Delete-promotion (M4): a vertex present at the fork point but removed
    // on the fork is deleted on primary. Opt-in and ext_id-keyed. We scan
    // the FULL fork label (ignoring per-pattern where-clauses, which select
    // which present rows to *promote*, not which to keep), so a filtered-out
    // but still-present fork row is never read as a deletion. A row primary
    // added after the fork point is absent from the baseline and so is never
    // a delete candidate — the anti-spurious-delete guarantee. Runs after
    // the pattern loop so vertex deletes are issued last in tx order.
    if options.delete_promotion
        && let Some(baseline) = baseline
    {
        let mut del_labels: Vec<&str> = patterns
            .iter()
            .filter(|p| !p.is_edge())
            .map(|p| p.label_name())
            .collect();
        del_labels.sort_unstable();
        del_labels.dedup();

        for label in del_labels {
            let cypher = format!("MATCH (n:`{}`) RETURN n", escape_backticks(label));
            let result = fork.query(&cypher).await?;
            let mut fork_now_ext: HashSet<String> = HashSet::new();
            let mut fork_now_noext: HashSet<UniId> = HashSet::new();
            for row in result.rows() {
                if let Some(Value::Node(node)) = row.value("n") {
                    match ext_id_for(&fork_ext_ids, node.vid) {
                        Some(eid) if !eid.is_empty() => {
                            fork_now_ext.insert(eid.to_string());
                        }
                        _ => {
                            fork_now_noext.insert(VertexDataset::compute_vertex_uid(
                                label,
                                None,
                                &node.properties,
                            ));
                        }
                    }
                }
            }

            // ext_id rows present at the fork point, absent on the fork now.
            if let Some(base_ext) = baseline.ext.get(label) {
                let deleted_ext: HashSet<String> = base_ext
                    .keys()
                    .filter(|eid| !fork_now_ext.contains(*eid))
                    .cloned()
                    .collect();
                if !deleted_ext.is_empty() {
                    // Resolve against primary NOW; delete only those still
                    // present (idempotent if primary already removed them).
                    let (resolved, degraded) = batch_resolve_primary_by_ext_id(
                        primary,
                        &primary_ext_ids,
                        label,
                        &deleted_ext,
                    )
                    .await;
                    // A degraded resolve here means the delete is silently
                    // SKIPPED — the row stays on primary and the report says
                    // nothing. Count it rather than letting it disappear.
                    if degraded {
                        report.vertices_deletes_unverified += deleted_ext.len();
                        warn!(
                            label = %label,
                            count = deleted_ext.len(),
                            "promote could not resolve rows marked for delete-promotion; \
                             they were left in place on primary"
                        );
                    }
                    for (eid, (pvid, pprops)) in resolved {
                        // A fork-delete racing a primary-edit: if primary's
                        // current props diverged from the fork-point baseline,
                        // honor ConflictPolicy. Under Skip (the with_merge
                        // default) leave primary's concurrently-edited row
                        // untouched and record the conflict; only Overwrite
                        // proceeds with the delete. Mirrors the divergence check
                        // in the upsert path above.
                        let primary_diverged = base_ext.get(&eid).is_some_and(|b| *b != pprops);
                        if primary_diverged && options.on_conflict != ConflictPolicy::Overwrite {
                            report.vertices_conflicting += 1;
                            continue;
                        }
                        primary_tx.delete_vertex(label, pvid).await?;
                        report.vertices_deleted += 1;
                    }
                }
            }

            // Non-ext_id fork-point rows that vanished can't be safely
            // delete-promoted (no stable identity); surface the count.
            if let Some(base_noext) = baseline.no_ext.get(label) {
                let gone = base_noext
                    .iter()
                    .filter(|u| !fork_now_noext.contains(*u))
                    .count();
                report.vertices_skipped_no_ext_id_for_delete += gone;
            }
        }
    }

    // When the call contains no edge patterns, surface incidental edges
    // on the fork so callers see they exist (and weren't promoted).
    if !any_edge_pattern {
        let mut edge_seen = 0usize;
        for et in fork.schema().schema().edge_types.keys() {
            let cypher = format!(
                "MATCH ()-[r:`{}`]->() RETURN count(r) AS c",
                escape_backticks(et)
            );
            if let Ok(rs) = fork.query(&cypher).await
                && let Some(row) = rs.rows().first()
                && let Ok(c) = row.get::<i64>("c")
            {
                edge_seen += c as usize;
            }
        }
        if edge_seen > 0 {
            report.edges_skipped = edge_seen;
            warn!(
                target: "uni::promote",
                edges_skipped = edge_seen,
                "promote_from_fork: fork contains {} edges; pass \
                 PromotePattern::edge_type(...) to promote them",
                edge_seen
            );
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_store::storage::vertex::VertexDataset;

    /// `content_uid_with_ext_id` reproduces the writer's double-folded UID.
    ///
    /// The write path hashes stored props that still carry the `ext_id` key, so
    /// re-injecting it on the query-stripped promote side must yield the exact
    /// same digest — otherwise the promote UID dedup can never fire.
    #[test]
    fn content_uid_with_ext_id_matches_registered_double_fold() {
        // Registered side: ext_id folded twice (arg + "ext_id" property key).
        let mut registered_props = Properties::new();
        registered_props.insert("ext_id".to_string(), Value::String("p1".to_string()));
        registered_props.insert("name".to_string(), Value::String("Alice".to_string()));
        let registered = VertexDataset::compute_vertex_uid("Person", Some("p1"), &registered_props);

        // Promote side: query-stripped props (no "ext_id" key) fed to the helper.
        let mut stripped = Properties::new();
        stripped.insert("name".to_string(), Value::String("Alice".to_string()));
        let recomputed = content_uid_with_ext_id("Person", Some("p1"), &stripped);

        assert_eq!(
            registered, recomputed,
            "re-injection must reproduce the registered double-folded UID"
        );
    }

    /// Without an `ext_id` the helper is a pure pass-through (no re-injection).
    #[test]
    fn content_uid_with_ext_id_is_noop_without_ext_id() {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        let direct = VertexDataset::compute_vertex_uid("Person", None, &props);
        let via_helper = content_uid_with_ext_id("Person", None, &props);
        assert_eq!(direct, via_helper, "None ext_id must not alter the digest");
    }

    /// Differing content yields differing UIDs — the basis for the live-content
    /// check that rejects a stale pre-edit UID against a since-edited vertex.
    #[test]
    fn content_uid_with_ext_id_distinguishes_diverged_content() {
        let mut age_30 = Properties::new();
        age_30.insert("age".to_string(), Value::Int(30));
        let mut age_99 = Properties::new();
        age_99.insert("age".to_string(), Value::Int(99));
        let uid_30 = content_uid_with_ext_id("Person", Some("p1"), &age_30);
        let uid_99 = content_uid_with_ext_id("Person", Some("p1"), &age_99);
        assert_ne!(
            uid_30, uid_99,
            "the same ext_id with diverged content must hash differently"
        );
    }
}
