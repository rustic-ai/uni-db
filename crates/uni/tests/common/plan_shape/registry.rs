// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The physical-operator registry the gate enforces.
//!
//! One row per **observable runtime name**, not per impl. `MutationExec`
//! returns one of five per-instance `display_name`s (`mutation_common.rs`), and
//! proving that SET fires says nothing about DETACH DELETE — so those get
//! separate rows keyed on the same `ty`.
//!
//! Adding an operator without a row fails [`super::gate`]. That is the point:
//! the failure mode this exists to stop is an operator quietly ceasing to be
//! emitted while every result-comparing test stays green.

/// How we know whether this operator can run.
///
/// Three states, and the third one needs justifying. The first draft of this
/// registry had only `Proven` and `Unreachable`, on the reasoning that a
/// "pending" variant becomes the permanent home of every hard case. That was
/// right about the risk and wrong about the facts: 27 of these operators are
/// plainly *reachable* — a MATCH emits `GraphScanExec`, UNWIND emits
/// `GraphUnwindExec` — they simply have no test asserting it. Filing them under
/// `Unreachable` would have written a falsehood into the data, and would have
/// broken [`super::gate::unreachable_operators_stay_unreachable`] the moment
/// anyone proved one.
///
/// So `Unproven` exists, and the dumping-ground risk is handled by a **ratchet**
/// instead: `MAX_UNPROVEN` may never increase. New operators must arrive
/// `Proven` or `Unreachable`, and every retrofit lowers the bound permanently.
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// A named, non-ignored test asserts this operator appears in an executed
    /// plan. The gate verifies the test exists *and* that some test really
    /// calls an assertion helper with this operator's `runtime_name`.
    Proven {
        by: &'static str,
        in_file: &'static str,
    },
    /// No planner path can emit it. The reason is the artifact: it is what the
    /// next reader needs to decide wire-it-up versus delete-it.
    Unreachable { reason: &'static str },
    /// Reachable, but nothing asserts it. This is a *gap*, counted and
    /// ratcheted — never a resting place.
    Unproven,
}

/// One physically-observable execution operator.
#[derive(Debug, Clone, Copy)]
pub struct Operator {
    /// Type named in `impl ExecutionPlan for <ty>`; joins this table to the
    /// source scan. Not unique — see the module docs on `MutationExec`.
    pub ty: &'static str,
    /// What `ExecutionPlan::name()` returns, i.e. what shows up in
    /// `ProfileOutput::runtime_stats[].operator`. This is what assertions match.
    pub runtime_name: &'static str,
    pub status: Status,
}

/// The ratchet. **This number may only ever go down.**
///
/// Every operator here is one that could stop being emitted with nothing
/// turning red — the condition that let `vid_lookup_join.rs` reach 0/441
/// executed lines under a dedicated 15-test suite. Retrofitting a proof lowers
/// the bound; the gate fails if the count exceeds it, so a new operator cannot
/// arrive unproven.
pub const MAX_UNPROVEN: usize = 32;

pub const OPERATORS: &[Operator] = &[
    // ── Measured 2026-08-14 ────────────────────────────────────────────────
    Operator {
        ty: "VidLookupJoinExec",
        runtime_name: "VidLookupJoinExec",
        status: Status::Proven {
            by: "documented_query_uses_the_vid_lookup_join",
            in_file: "crates/uni/tests/common/bugs/vid_lookup_join_reachability.rs",
        },
    },
    // 1,044 lines across these two, constructed ONLY from `iteration_driver.rs`'s
    // own `#[cfg(test)] mod tests`. Their self-tests give them nonzero coverage,
    // which is why a coverage map does not flag them — the opposite blind spot
    // from vid_lookup_join, and the reason this registry exists alongside it.
    Operator {
        ty: "PowerStepExec",
        runtime_name: "PowerStepExec",
        status: Status::Unreachable {
            reason: "No planner path constructs it. df_planner.rs never mentions \
                     iteration_driver; the only call sites are inside that file's own \
                     `#[cfg(test)] mod tests`. Resolve by wiring the iteration driver \
                     into the Locy fixpoint planner (then flip to Proven) or deleting \
                     it — but do not leave it unclassified.",
        },
    },
    Operator {
        ty: "GraphGatherStepExec",
        runtime_name: "GraphGatherStepExec",
        status: Status::Unreachable {
            reason: "Same as PowerStepExec — constructed only from iteration_driver.rs's \
                     own `#[cfg(test)] mod tests`, with no df_planner.rs construction \
                     site. Wire it up or delete it; its unit tests make coverage look \
                     healthy either way.",
        },
    },
    // ── Everything else: recorded, not yet proven ──────────────────────────
    Operator {
        ty: "GraphScanExec",
        runtime_name: "GraphScanExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphTraverseExec",
        runtime_name: "GraphTraverseExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphTraverseMainExec",
        runtime_name: "GraphTraverseMainExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphVariableLengthTraverseExec",
        runtime_name: "GraphVariableLengthTraverseExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphVariableLengthTraverseMainExec",
        runtime_name: "GraphVariableLengthTraverseMainExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationCreateExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationSetExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationDeleteExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationRemoveExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationMergeExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "ForeachExec",
        runtime_name: "ForeachExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphUnwindExec",
        runtime_name: "GraphUnwindExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "OptionalFilterExec",
        runtime_name: "OptionalFilterExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphApplyExec",
        runtime_name: "GraphApplyExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphProcedureCallExec",
        runtime_name: "GraphProcedureCallExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "CatalogVertexScanExec",
        runtime_name: "CatalogVertexScanExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "CatalogEdgeScanExec",
        runtime_name: "CatalogEdgeScanExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphVectorKnnExec",
        runtime_name: "GraphVectorKnnExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphShortestPathExec",
        runtime_name: "GraphShortestPathExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphExtIdLookupExec",
        runtime_name: "GraphExtIdLookupExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "RecursiveCTEExec",
        runtime_name: "RecursiveCTEExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "BindFixedPathExec",
        runtime_name: "BindFixedPathExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "BindZeroLengthPathExec",
        runtime_name: "BindZeroLengthPathExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "EndpointHydrateExec",
        runtime_name: "EndpointHydrateExec",
        status: Status::Proven {
            by: "the_post_with_endpoint_query_runs_the_hydration_operator",
            in_file: "crates/uni/tests/common/cypher_read/start_end_node_test.rs",
        },
    },
    Operator {
        ty: "ReadSetRecordingExec",
        runtime_name: "ReadSetRecordingExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "LocyProgramExec",
        runtime_name: "LocyProgramExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "FixpointExec",
        runtime_name: "FixpointExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "DerivedScanExec",
        runtime_name: "DerivedScanExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "FoldExec",
        runtime_name: "FoldExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "BestByExec",
        runtime_name: "BestByExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "PriorityExec",
        runtime_name: "PriorityExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "LocyModelInvokeExec",
        runtime_name: "LocyModelInvokeExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "StorageScanExec",
        runtime_name: "StorageScanExec",
        status: Status::Unproven,
    },
];
