// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod adjacency;
pub mod adjacency_manager;
pub mod adjacency_overlay;
pub mod arrow_convert;
pub mod compaction;
pub mod csr;
pub mod delta;
pub mod direction;
#[cfg(feature = "lance-backend")]
pub mod edge;
#[cfg(feature = "lance-backend")]
pub mod index;
pub mod index_manager;
pub mod index_rebuild;
#[cfg(feature = "lance-backend")]
pub mod inverted_index;
#[cfg(feature = "lance-backend")]
pub mod json_index;
pub mod main_edge;
pub mod main_vertex;
pub mod manager;
pub mod muvera_index;
pub mod property_builder;
pub mod resilient_store;
pub mod shadow_csr;
#[cfg(feature = "lance-backend")]
pub mod sparse_index;
pub mod value_codec;
pub mod vertex;
pub mod vid_labels;

use crate::backend::types::FilterExpr;

/// Record a default index that could not be built.
///
/// These builds are deliberately fail-open, and should stay that way: a missing
/// index makes queries slower, not wrong, and refusing the write that triggered
/// it would turn a degraded index into a failed insert. What was wrong is that
/// the consequence was unrecorded — four sites logged a `warn!` and one
/// (`UidIndex::ensure_uid_hex_index`) discarded the error with `.ok()` and no
/// log at all, so a store could end up with **no index** and nothing anywhere
/// said so. The symptom is a full scan where a lookup was planned, which reads
/// as "the database is slow" rather than as a failure (#233, Tier 2).
///
/// A `warn!` is greppable in a log nobody kept. The counter makes it countable,
/// which is the same trade the two deliberate Tier 1 fail-open sites took.
///
/// One place rather than five, so a caller cannot log without counting.
pub fn record_default_index_failure(table: &str, column: &str, err: &dyn std::fmt::Display) {
    metrics::counter!(
        "uni_default_index_build_failures_total",
        "table" => table.to_string(),
        "column" => column.to_string(),
    )
    .increment(1);
    log::warn!(
        "failed to create the default `{column}` index on `{table}`: {err}.          Queries filtering on it will fall back to a full scan."
    );
}

/// Conjoin the snapshot-isolation version bound onto a scan `filter`.
///
/// When `version` is `Some(hwm)`, restricts the scan to rows at or below the
/// high water mark; `None` leaves the filter unchanged (global visibility).
/// This bound is SSI/OCC-critical and must be applied identically across all
/// snapshot reads — conjoining structured nodes is what guarantees that, where
/// the previous `push_str(" AND …")` would have mis-bound against any body
/// carrying a top-level `OR`.
///
/// Shared by the vertex- and edge-side main-table readers. It lived in
/// `main_vertex` alone while the edge side had no bound at all, which is how
/// the two drifted: L0 and the delta tier gated edge reads by version, the L1
/// main-table fallback did not.
pub(crate) fn with_version_bound(filter: FilterExpr, version: Option<u64>) -> FilterExpr {
    match version {
        Some(hwm) => FilterExpr::all([filter, FilterExpr::version_at_most(hwm)]),
        None => filter,
    }
}

pub use adjacency::AdjacencyDataset;
pub use adjacency_manager::AdjacencyManager;
pub use csr::CompressedSparseRow;
pub use delta::DeltaDataset;
pub use direction::Direction;
#[cfg(feature = "lance-backend")]
pub use edge::EdgeDataset;
#[cfg(feature = "lance-backend")]
pub use index::UidIndex;
pub use index_manager::{IndexManager, IndexRebuildStatus, IndexRebuildTask};
pub use index_rebuild::IndexRebuildManager;
#[cfg(feature = "lance-backend")]
pub use inverted_index::InvertedIndex;
pub use main_edge::{EndpointSide, MainEdgeDataset};
pub use main_vertex::MainVertexDataset;
pub use manager::StorageManager;
pub use resilient_store::ResilientObjectStore;
pub use vertex::VertexDataset;
pub use vid_labels::{EidTypeIndex, VidLabelsIndex};
