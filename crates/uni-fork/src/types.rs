// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 6 — Fork diff & promote types.
//!
//! `ForkDiff` describes the structural delta between two fork views
//! (or a fork and primary). The convention is *forward*: `diff(a, b)`
//! is the delta that, if applied to `a`, would produce `b`. So
//! `added` rows exist in `b` only, `deleted` exist in `a` only, and
//! `changed` is a per-row before/after on rows with matching identity.
//!
//! **Identity** is `UniId` for vertices and `(src_uid, dst_uid, type)`
//! for edges. Both are content-addressed (vertex UID = SHA3-256 of
//! `(label, ext_id, properties)`; edge UID is the tuple of endpoint
//! UIDs plus the edge type), so the diff is correct across two
//! unrelated forks that happen to have rolled the same VIDs. The
//! per-side VID is preserved on `DiffVertex` as informational; pairing
//! never depends on it.
//!
//! Phase 6a (the initial MVP) keyed diffs by VID. Phase 6b lifted
//! identity to UID so siblings-off-a-shared-parent and totally
//! unrelated forks compare correctly.
//!
//! `PromotePattern` is the spec for what to scan on a fork during
//! `Uni::promote_from_fork`. Phase 6 supports the most common shape
//! (label + optional Cypher WHERE clause); future phases may grow
//! relationship-aware patterns.

use std::collections::{HashMap, HashSet};
use std::fmt;

use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::{UniId, Vid};

/// The full delta from one fork view to another.
#[derive(Debug, Clone, Default)]
pub struct ForkDiff {
    /// Per-label vertex deltas.
    pub vertices: VertexDiff,
    /// Per-edge-type edge deltas.
    pub edges: EdgeDiff,
}

impl ForkDiff {
    /// Returns `true` when there are no vertex or edge differences.
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty() && self.edges.is_empty()
    }

    /// Total rows in this diff across vertices and edges.
    pub fn total_rows(&self) -> usize {
        self.vertices.total_rows() + self.edges.total_rows()
    }

    /// Return the inverse: swap added/deleted and swap before/after in
    /// every property change. By construction
    /// `diff(a,b).invert() == diff(b,a)`.
    pub fn invert(mut self) -> Self {
        self.vertices = self.vertices.invert();
        self.edges = self.edges.invert();
        self
    }
}

/// Vertex-side of [`ForkDiff`].
#[derive(Debug, Clone, Default)]
pub struct VertexDiff {
    /// Rows present in `b` but not `a`.
    pub added: Vec<DiffVertex>,
    /// Rows present in `a` but not `b`.
    pub deleted: Vec<DiffVertex>,
    /// **Reserved; always empty.** Identity is content-addressed (the UID
    /// folds in the properties), so two rows with matching identity have
    /// identical properties by construction — a property change surfaces as
    /// a paired `deleted` + `added` instead. Kept for forward compatibility
    /// with a future stable-id diff. (L12)
    pub changed: Vec<VertexPropertyChange>,
}

impl VertexDiff {
    /// Returns `true` when added, deleted, and changed are all empty.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.deleted.is_empty() && self.changed.is_empty()
    }

    /// Sum of added + deleted + changed counts.
    pub fn total_rows(&self) -> usize {
        self.added.len() + self.deleted.len() + self.changed.len()
    }

    fn invert(self) -> Self {
        Self {
            added: self.deleted,
            deleted: self.added,
            changed: self
                .changed
                .into_iter()
                .map(VertexPropertyChange::invert)
                .collect(),
        }
    }
}

/// Edge-side of [`ForkDiff`].
#[derive(Debug, Clone, Default)]
pub struct EdgeDiff {
    /// Edges present in `b` but not `a`.
    pub added: Vec<DiffEdge>,
    /// Edges present in `a` but not `b`.
    pub deleted: Vec<DiffEdge>,
    /// **Reserved; always empty.** The edge UID folds in the properties, so
    /// a property change surfaces as a paired `deleted` + `added`, not here.
    /// Kept for forward compatibility. (L12)
    pub changed: Vec<EdgePropertyChange>,
}

impl EdgeDiff {
    /// Returns `true` when added, deleted, and changed are all empty.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.deleted.is_empty() && self.changed.is_empty()
    }

    /// Sum of added + deleted + changed counts.
    pub fn total_rows(&self) -> usize {
        self.added.len() + self.deleted.len() + self.changed.len()
    }

    fn invert(self) -> Self {
        Self {
            added: self.deleted,
            deleted: self.added,
            changed: self
                .changed
                .into_iter()
                .map(EdgePropertyChange::invert)
                .collect(),
        }
    }
}

/// A vertex row from one side of a diff.
#[derive(Debug, Clone)]
pub struct DiffVertex {
    /// The vertex's label.
    pub label: String,
    /// Content-addressed identity (`compute_vertex_uid(label, None,
    /// properties)`). This is the bucketing key during diff.
    pub uid: UniId,
    /// Informational: which VID this row carried on the side it was
    /// scanned from. `None` if the per-side scan returned a node
    /// without a VID, which should not happen in practice.
    pub vid: Option<Vid>,
    /// Property bag for the vertex (user properties only).
    pub properties: Properties,
}

/// A change to one vertex's properties.
#[derive(Debug, Clone)]
pub struct VertexPropertyChange {
    /// The vertex's label.
    pub label: String,
    /// UID of the vertex — the pairing key across sides.
    pub uid: UniId,
    /// One entry per property whose value differs between sides.
    pub changes: Vec<PropertyChange>,
}

impl VertexPropertyChange {
    fn invert(self) -> Self {
        Self {
            label: self.label,
            uid: self.uid,
            changes: self
                .changes
                .into_iter()
                .map(PropertyChange::invert)
                .collect(),
        }
    }
}

/// An edge row from one side of a diff.
#[derive(Debug, Clone)]
pub struct DiffEdge {
    /// The edge type.
    pub edge_type: String,
    /// Content-addressed edge UID (computed via
    /// `MainEdgeDataset::compute_edge_uid` over
    /// `(src_uid, dst_uid, edge_type, sorted_properties)`). Two
    /// parallel edges between the same endpoints with different
    /// property bags have different `edge_uid`s — that's how the
    /// diff distinguishes them.
    pub edge_uid: UniId,
    /// Source vertex UID (content-addressed).
    pub src_uid: UniId,
    /// Destination vertex UID (content-addressed).
    pub dst_uid: UniId,
    /// Property bag for the edge.
    pub properties: Properties,
}

/// A change to one edge's properties.
#[derive(Debug, Clone)]
pub struct EdgePropertyChange {
    /// The edge type.
    pub edge_type: String,
    /// Source vertex UID.
    pub src_uid: UniId,
    /// Destination vertex UID.
    pub dst_uid: UniId,
    /// One entry per property whose value differs between sides.
    pub changes: Vec<PropertyChange>,
}

impl EdgePropertyChange {
    fn invert(self) -> Self {
        Self {
            edge_type: self.edge_type,
            src_uid: self.src_uid,
            dst_uid: self.dst_uid,
            changes: self
                .changes
                .into_iter()
                .map(PropertyChange::invert)
                .collect(),
        }
    }
}

/// A single property's before/after pair.
#[derive(Debug, Clone)]
pub struct PropertyChange {
    /// Property key.
    pub key: String,
    /// Value on the `a` side, or `None` if absent.
    pub before: Option<Value>,
    /// Value on the `b` side, or `None` if absent.
    pub after: Option<Value>,
}

impl PropertyChange {
    fn invert(self) -> Self {
        Self {
            key: self.key,
            before: self.after,
            after: self.before,
        }
    }
}

/// Selector for `Uni::promote_from_fork`.
///
/// Two shapes:
/// - [`PromotePattern::label`] — match every vertex with this label;
///   bulk-inserted on primary, deduplicated by content-derived UID.
/// - [`PromotePattern::edge_type`] — match every edge of this type
///   whose endpoints already exist on primary; the edge is inserted
///   between the resolved primary endpoints, deduplicated by
///   `(src_uid, dst_uid, edge_type)`.
///
/// Both variants accept an optional Cypher `WHERE` clause, interpolated
/// verbatim into the fork-side scan. Callers are responsible for
/// quoting and parameter safety.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PromotePattern {
    /// Promote vertices.
    Vertex {
        /// Vertex label.
        label: String,
        /// Optional `WHERE` predicate on the fork-side scan.
        where_clause: Option<String>,
    },
    /// Promote edges. Endpoints must already exist on primary (by UID);
    /// fork-only endpoints are skipped and counted in
    /// [`PromoteReport::edges_skipped_no_endpoint`].
    Edge {
        /// Edge type.
        edge_type: String,
        /// Optional `WHERE` predicate on the fork-side scan. The bound
        /// names are `a` (source), `r` (edge), `b` (destination).
        where_clause: Option<String>,
    },
}

impl PromotePattern {
    /// Match every vertex with this label.
    pub fn label(label: impl Into<String>) -> Self {
        Self::Vertex {
            label: label.into(),
            where_clause: None,
        }
    }

    /// Match every edge with this type. Endpoints must already exist
    /// on primary (resolved by UID); fork-only endpoints are counted
    /// and skipped — they need to be promoted first via a vertex
    /// pattern.
    pub fn edge_type(edge_type: impl Into<String>) -> Self {
        Self::Edge {
            edge_type: edge_type.into(),
            where_clause: None,
        }
    }

    /// Restrict the scan to rows matching this Cypher predicate.
    /// Verbatim interpolation — caller owns quoting.
    pub fn where_clause(mut self, expr: impl Into<String>) -> Self {
        let expr = expr.into();
        match &mut self {
            Self::Vertex { where_clause, .. } | Self::Edge { where_clause, .. } => {
                *where_clause = Some(expr)
            }
        }
        self
    }

    /// Vertex label for vertex patterns. Empty string for edge patterns.
    pub fn label_name(&self) -> &str {
        match self {
            Self::Vertex { label, .. } => label,
            Self::Edge { .. } => "",
        }
    }

    /// Edge type for edge patterns. Empty string for vertex patterns.
    pub fn edge_type_name(&self) -> &str {
        match self {
            Self::Edge { edge_type, .. } => edge_type,
            Self::Vertex { .. } => "",
        }
    }

    /// The optional `WHERE` predicate.
    pub fn where_expr(&self) -> Option<&str> {
        match self {
            Self::Vertex { where_clause, .. } | Self::Edge { where_clause, .. } => {
                where_clause.as_deref()
            }
        }
    }

    /// `true` if this pattern targets edges.
    pub fn is_edge(&self) -> bool {
        matches!(self, Self::Edge { .. })
    }
}

impl fmt::Display for PromotePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Vertex {
                label,
                where_clause: Some(w),
            } => write!(f, "(:{label} WHERE {w})"),
            Self::Vertex {
                label,
                where_clause: None,
            } => write!(f, "(:{label})"),
            Self::Edge {
                edge_type,
                where_clause: Some(w),
            } => write!(f, "[:{edge_type} WHERE {w}]"),
            Self::Edge {
                edge_type,
                where_clause: None,
            } => write!(f, "[:{edge_type}]"),
        }
    }
}

/// Options controlling `Uni::promote_from_fork` merge behavior.
///
/// Additive and `#[non_exhaustive]`: the legacy `promote_from_fork`
/// entry point uses `PromoteOptions::default()` (insert-only), so existing
/// callers are unaffected.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PromoteOptions {
    /// When `true`, a fork edit to a vertex that already exists on primary
    /// (matched by `(label, ext_id)`) is applied as an in-place property
    /// update instead of inserting a twin. Vertices without an `ext_id`
    /// keep the legacy content-UID insert-or-skip behavior. Default
    /// `false` preserves the historical insert-only contract.
    pub upsert: bool,

    /// When `true`, after the pattern loop, primary vertices present at the
    /// fork point but removed on the fork are deleted from primary
    /// (ext_id-keyed; rows without an `ext_id` are reported, never deleted).
    /// Requires a fork-point baseline; the host builds it and passes it to
    /// [`crate::run_promote`]. Off by default because it removes primary rows.
    pub delete_promotion: bool,

    /// How to resolve a vertex that diverged on BOTH primary and the fork
    /// since the fork point (a concurrent edit). Only consulted in the
    /// baseline-aware merge (i.e. when a baseline is supplied).
    pub on_conflict: ConflictPolicy,
}

/// Policy for a concurrent divergent edit during a baseline-aware merge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConflictPolicy {
    /// Leave primary's value untouched; count it in `vertices_conflicting`.
    /// The safe default.
    #[default]
    Skip,
    /// Apply the fork's value over primary's; count in both
    /// `vertices_conflicting` and `vertices_updated`.
    Overwrite,
}

impl PromoteOptions {
    /// Insert-only (legacy) options — the default.
    #[must_use]
    pub fn insert_only() -> Self {
        Self::default()
    }

    /// Enable ext_id-keyed upsert of existing primary vertices (fork-wins,
    /// no baseline).
    #[must_use]
    pub fn with_upsert() -> Self {
        Self {
            upsert: true,
            ..Self::default()
        }
    }

    /// Enable baseline-aware merge: ext_id upsert + conflict detection +
    /// delete-promotion. The host supplies a [`PromoteBaseline`].
    #[must_use]
    pub fn with_merge() -> Self {
        Self {
            upsert: true,
            delete_promotion: true,
            on_conflict: ConflictPolicy::Skip,
        }
    }

    /// Set the conflict policy (builder).
    #[must_use]
    pub fn on_conflict(mut self, policy: ConflictPolicy) -> Self {
        self.on_conflict = policy;
        self
    }
}

/// Fork-point snapshot of primary, keyed for the merge/delete passes.
///
/// Built by reading primary pinned at the fork's `parent_snapshot_id`.
/// `ext` keys rows by their stable `(label, ext_id)`; `no_ext` tracks
/// `ext_id`-less rows by content-UID so they can be reported as
/// un-promotable on delete, never deleted.
#[derive(Debug, Clone, Default)]
pub struct PromoteBaseline {
    /// label → (ext_id → properties at the fork point).
    pub ext: HashMap<String, HashMap<String, Properties>>,
    /// label → content-UIDs of fork-point rows that had no `ext_id`.
    pub no_ext: HashMap<String, HashSet<UniId>>,
}

impl PromoteBaseline {
    /// `true` when the baseline holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ext.is_empty() && self.no_ext.is_empty()
    }
}

/// Outcome of `Uni::promote_from_fork`.
#[derive(Debug, Clone, Default)]
pub struct PromoteReport {
    /// Number of vertices inserted into primary.
    pub vertices_inserted: usize,
    /// Number of existing primary vertices updated in place (upsert by
    /// `(label, ext_id)`; only populated when `PromoteOptions::upsert`).
    pub vertices_updated: usize,
    /// Number of fork rows that matched an existing primary vertex by
    /// `(label, ext_id)` with identical properties — nothing written.
    pub vertices_skipped_no_op: usize,
    /// Number of vertices inserted while their primary presence could
    /// NOT be confirmed (a transient resolve failure degraded to
    /// "absent → insert"). A non-zero value means some of
    /// `vertices_inserted` may be duplicates; see the warning logged at
    /// promote time. (M5)
    pub vertices_inserted_unverified: usize,
    /// Number of fork rows skipped because primary already has the same UID.
    pub vertices_skipped_uid_conflict: usize,
    /// Number of primary vertices deleted because they existed at the fork
    /// point but the fork has since removed them (delete-promotion; only
    /// populated when `PromoteOptions::delete_promotion`).
    pub vertices_deleted: usize,
    /// Number of fork-point rows without an `ext_id` that vanished from the
    /// fork — they cannot be safely resolved on primary for deletion, so
    /// they are reported here rather than deleted.
    pub vertices_skipped_no_ext_id_for_delete: usize,
    /// Number of ext_id targets where both primary and the fork diverged
    /// from the fork-point baseline (a concurrent divergent edit), resolved
    /// per `PromoteOptions::on_conflict`.
    pub vertices_conflicting: usize,
    /// Number of edges inserted into primary.
    pub edges_inserted: usize,
    /// Number of fork edges skipped because primary already has an
    /// edge of the same type between the resolved endpoints.
    pub edges_skipped_duplicate: usize,
    /// Number of fork edges skipped because at least one endpoint had
    /// no UID match on primary. To insert these edges, promote the
    /// missing vertices first via a vertex pattern, then re-run.
    pub edges_skipped_no_endpoint: usize,
    /// Edges inserted while primary's existing-edge pre-fetch or the endpoint
    /// resolve was degraded, so they may duplicate rows already on primary.
    ///
    /// The edge-side twin of [`Self::vertices_inserted_unverified`]. A nonzero
    /// value means `edges_skipped_duplicate` under-counts: the dedup set could
    /// not be built, so every fork edge looked new.
    pub edges_inserted_unverified: usize,
    /// Rows marked for delete-promotion whose primary twin could not be
    /// resolved, so the delete was skipped and the row remains on primary.
    pub vertices_deletes_unverified: usize,
    /// Number of edges that touched a promoted vertex but were not
    /// themselves promoted (no edge pattern in the call). Phase 6
    /// MVP's behaviour: silently skip + warn. Phase 6b adds explicit
    /// edge patterns; when no edge pattern is given, this counter
    /// still surfaces incidental edges for visibility.
    pub edges_skipped: usize,
    /// Per-pattern row counts so callers can see which pattern matched
    /// what. Indexed by pattern position in the input slice.
    pub per_pattern_inserted: Vec<usize>,
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_change_inverts_before_after() {
        let pc = PropertyChange {
            key: "age".into(),
            before: Some(Value::Int(30)),
            after: Some(Value::Int(31)),
        };
        let inv = pc.clone().invert();
        assert_eq!(inv.before, pc.after);
        assert_eq!(inv.after, pc.before);
    }

    #[test]
    fn vertex_diff_invert_swaps_added_deleted() {
        let v_a = DiffVertex {
            label: "Person".into(),
            uid: UniId::from_bytes([1; 32]),
            vid: Some(Vid::new(1)),
            properties: Default::default(),
        };
        let v_b = DiffVertex {
            label: "Person".into(),
            uid: UniId::from_bytes([2; 32]),
            vid: Some(Vid::new(2)),
            properties: Default::default(),
        };
        let d = VertexDiff {
            added: vec![v_a.clone()],
            deleted: vec![v_b.clone()],
            changed: vec![],
        };
        let inv = d.invert();
        assert_eq!(inv.added.len(), 1);
        assert_eq!(inv.deleted.len(), 1);
    }

    #[test]
    fn fork_diff_default_is_empty() {
        let d = ForkDiff::default();
        assert!(d.is_empty());
        assert_eq!(d.total_rows(), 0);
    }

    #[test]
    fn promote_pattern_display() {
        let p = PromotePattern::label("Person");
        assert_eq!(format!("{}", p), "(:Person)");
        let p2 = PromotePattern::label("Person").where_clause("n.age > 30");
        assert_eq!(format!("{}", p2), "(:Person WHERE n.age > 30)");
    }
}
