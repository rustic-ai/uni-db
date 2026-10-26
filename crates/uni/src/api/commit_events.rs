// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The per-row mutation events of a commit, as triggers and CDC receive them.
//!
//! Shared by `Transaction::commit`, which builds them from the transaction's
//! private L0 before the commit, and by WAL recovery
//! (`wal_recovery::replay_wal`), which rebuilds them for a commit the crashed
//! process made durable but never announced, from that commit's WAL span
//! replayed into a scratch buffer. Both classify each write (create, update,
//! delete) and take pre-images against the committed state the commit landed
//! on, so recovery must call [`extract`] before applying the span.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::l0_manager::L0Manager;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

use crate::api::triggers::{MutationEvents, PreExistingProbe, TriggerRouter};

/// The vertex labels and edge types `l0` writes, each once.
pub(crate) fn affected(l0: &L0Buffer) -> (Vec<String>, Vec<String>) {
    let labels: Vec<String> = l0
        .vertex_labels
        .values()
        .flatten()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let edge_types: Vec<String> = l0
        .edge_types
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    (labels, edge_types)
}

/// Build the mutation events of the writes in `tx_l0`, classified against the
/// committed state `l0_manager` (main L0 and its pending flushes) and L1 hold.
///
/// Reads only the pre-images a consumer can receive: every one when
/// `cdc_active`, otherwise those of the vertices and edges a trigger in
/// `router` is subscribed to. Without `l0_manager` (no writer) nothing is
/// probed and every write classifies as a create.
pub(crate) async fn extract(
    storage: &Arc<StorageManager>,
    properties: &PropertyManager,
    l0_manager: Option<&Arc<L0Manager>>,
    tx_l0: &RwLock<L0Buffer>,
    router: &TriggerRouter,
    cdc_active: bool,
) -> MutationEvents {
    let mut probe_opt = l0_manager.map(|l0m| PreExistingProbe::from_l0_chain(l0m, &tx_l0.read()));

    // Extend the probe with an L1 storage existence scan for VIDs
    // not found in the L0 chain. The candidate snapshot is taken
    // under a short sync lock; the async probe runs outside the
    // lock to keep the L0 buffer available to other readers and
    // to satisfy the Send bound on the surrounding async fn.
    //
    // Edges take a different route: every edge the tx writes or deletes
    // gets its committed state (L0 chain over L1) read in one batched
    // lookup, which both detects pre-existence for edges already flushed
    // and supplies their whole pre-image.
    //
    // The edge read runs only when an edge event can be delivered: a CDC
    // output, or a trigger subscribed to edge events. Node-only triggers
    // (the default `:_AuditMe` audit trigger among them) never read an
    // edge's pre-image.
    let wants_edge_pre_images = cdc_active || router.wants_edge_events();
    if let Some(ref mut probe) = probe_opt {
        let schema_manager = storage.schema_manager();
        let (candidates, edge_candidates) = {
            let l0 = tx_l0.read();
            let resolve = |type_id: u32| schema_manager.edge_type_name_by_id_unified(type_id);
            let mut candidates = probe.pending_l1_candidates(&l0);
            // A vertex's pre-image feeds only its node event rows, which
            // are emitted under its labels in this transaction; read it
            // only when CDC or a trigger can receive one of them.
            if !cdc_active {
                candidates.retain(|(vid, _)| {
                    let labels = l0.vertex_labels.get(vid).map_or(&[][..], Vec::as_slice);
                    router.wants_node_events_for(labels)
                });
            }
            (
                candidates,
                if wants_edge_pre_images {
                    probe.pending_edge_candidates(&l0, &resolve)
                } else {
                    Vec::new()
                },
            )
        };
        if !candidates.is_empty() {
            let started = std::time::Instant::now();
            let vertices = candidates.len();
            probe.extend_with_l1(candidates, storage).await;
            tracing::debug!(
                target: "uni::trigger_probe",
                vertices,
                elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                "vertex pre-image probe"
            );
        }
        if !edge_candidates.is_empty()
            && let Some(l0m) = l0_manager
        {
            let schema = schema_manager.schema();
            let started = std::time::Instant::now();
            let edges = edge_candidates.len();
            probe
                .extend_edges_with_committed_state(edge_candidates, l0m, properties, &schema)
                .await;
            tracing::debug!(
                target: "uni::trigger_probe",
                edges,
                elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                "edge pre-image probe"
            );
        }
    }

    // Includes the CDC path: when triggers aren't registered but CDC
    // subscribers are, we still extract events (with no property bag — the
    // trigger predicate gate is the only consumer of `properties_referenced`).
    let l0 = tx_l0.read();
    let props_referenced = router.properties_referenced();
    // #233: the edge type NAME may be missing from `edge_types`, but
    // the type id is always carried in `edge_endpoints`. Give the
    // event builder a schema-backed resolver so it never falls back to
    // an empty type name, which the trigger router silently fails to
    // match.
    let schema_for_types = storage.schema_manager();
    let resolve_edge_type =
        move |type_id: u32| schema_for_types.edge_type_name_by_id_unified(type_id);
    MutationEvents::from_l0_with_probe(
        &l0,
        probe_opt.as_ref(),
        &props_referenced,
        &resolve_edge_type,
    )
}

// Rust guideline compliant
