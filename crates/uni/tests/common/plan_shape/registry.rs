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
/// As of #177 the bound is zero and no row uses this status.
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
///
/// It is now **zero** (#177), which changes what this constant does. It no
/// longer records a debt to pay down; it is a floor, and every operator added
/// from here must arrive `Proven` or `Unreachable` or the gate fails on the
/// spot. `Unproven` keeps its meaning — an honest home for "reachable, not yet
/// asserted" — but reaching for it now requires lowering a bound that cannot be
/// lowered, which is the intended friction.
pub const MAX_UNPROVEN: usize = 0;

pub const OPERATORS: &[Operator] = &[
    // ── Measured 2026-09-20 ────────────────────────────────────────────────
    Operator {
        ty: "DeadlineGuardExec",
        runtime_name: "DeadlineGuardExec",
        status: Status::Proven {
            by: "a_deadline_is_enforced_under_a_pipeline_breaking_operator",
            in_file: "crates/uni/tests/common/perf/query_limits_test.rs",
        },
    },
    // ── Measured 2026-08-14 ────────────────────────────────────────────────
    // ── Added 2026-10-18 (P9 / A7) ────────────────────────────────────────
    Operator {
        ty: "PropertyLookupJoinExec",
        runtime_name: "PropertyLookupJoinExec",
        status: Status::Proven {
            by: "a7_anchored_disjoint_pattern_reads_only_the_matching_rows",
            in_file: "crates/uni/tests/common/perf/anchored_lookup_test.rs",
        },
    },
    Operator {
        ty: "EdgeSourceScanExec",
        runtime_name: "EdgeSourceScanExec",
        status: Status::Proven {
            by: "edge_source_scan_plan_shape",
            in_file: "crates/uni/tests/common/cypher_read/edge_source_scan_test.rs",
        },
    },
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
        status: Status::Proven {
            by: "a_labelled_match_runs_the_graph_scan",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphTraverseExec",
        runtime_name: "GraphTraverseExec",
        status: Status::Proven {
            by: "a_declared_single_hop_runs_the_graph_traverse",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphTraverseMainExec",
        runtime_name: "GraphTraverseMainExec",
        status: Status::Proven {
            by: "an_undeclared_single_hop_runs_the_main_traverse_instead",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphVariableLengthTraverseExec",
        runtime_name: "GraphVariableLengthTraverseExec",
        status: Status::Proven {
            by: "a_declared_variable_length_hop_runs_the_vlp_traverse",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphVariableLengthTraverseMainExec",
        runtime_name: "GraphVariableLengthTraverseMainExec",
        status: Status::Proven {
            by: "an_undeclared_variable_length_hop_runs_the_main_vlp_traverse",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationCreateExec",
        status: Status::Proven {
            by: "a_create_runs_the_create_mutation",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationSetExec",
        status: Status::Proven {
            by: "a_set_runs_the_set_mutation_and_not_its_siblings",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationDeleteExec",
        status: Status::Proven {
            by: "a_delete_runs_the_delete_mutation",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationRemoveExec",
        status: Status::Proven {
            by: "a_remove_runs_the_remove_mutation",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationMergeExec",
        status: Status::Proven {
            by: "a_merge_runs_the_merge_mutation",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "ForeachExec",
        runtime_name: "ForeachExec",
        // Was unprovable rather than unproven: #176 found the clause had no
        // grammar rule and no AST variant, so no query could construct the plan
        // node and the 154 executable lines beneath it were unreachable. The
        // front end now exists, and running the operator for the first time
        // immediately turned up two defects in it — a lossy row/batch round
        // trip on the pass-through, and a per-item scope rebuilt from the
        // original row. "Never ran" was not "not worth running", and it was not
        // "works" either.
        status: Status::Proven {
            by: "the_foreach_clause_plans_the_foreach_operator",
            in_file: "crates/uni/tests/common/cypher_write/foreach_test.rs",
        },
    },
    Operator {
        ty: "GraphUnwindExec",
        runtime_name: "GraphUnwindExec",
        status: Status::Proven {
            by: "an_unwind_runs_the_graph_unwind",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "OptionalSourceRowIdExec",
        runtime_name: "OptionalSourceRowIdExec",
        status: Status::Proven {
            by: "an_optional_match_tags_its_entering_rows",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "OptionalFilterExec",
        runtime_name: "OptionalFilterExec",
        status: Status::Proven {
            by: "an_optional_match_predicate_runs_the_optional_filter",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphApplyExec",
        runtime_name: "GraphApplyExec",
        status: Status::Proven {
            by: "a_call_subquery_runs_the_graph_apply",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphProcedureCallExec",
        runtime_name: "GraphProcedureCallExec",
        status: Status::Proven {
            by: "a_procedure_call_runs_the_procedure_call_operator",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // A plugin `CatalogProvider` registers a virtual label and
    // `MATCH (n:External)` dispatches to it. The fixture already existed and
    // asserted only the rows; the proof adds the shape, with a native-label
    // twin because the two are chosen by schema resolution, not query text.
    Operator {
        ty: "CatalogVertexScanExec",
        runtime_name: "CatalogVertexScanExec",
        status: Status::Proven {
            by: "vertex_match_runs_the_catalog_vertex_scan",
            in_file: "crates/uni/tests/common/plugin/plugin_virtual_label_dispatch.rs",
        },
    },
    // This row said "reachable only in principle", pending a native
    // source/target label resolution "the MVP doesn't cover", and said to prove
    // it by finishing that resolution rather than testing the planner of the
    // day. The resolution landed in the meantime — `plugin_mid_pattern_virtual.rs`
    // is exactly that work — and nobody revisited the row. Measured: both the
    // native-source and virtual-source shapes emit it.
    Operator {
        ty: "CatalogEdgeScanExec",
        runtime_name: "CatalogEdgeScanExec",
        status: Status::Proven {
            by: "a_virtual_edge_traversal_runs_the_catalog_edge_scan",
            in_file: "crates/uni/tests/common/plugin/plugin_mid_pattern_virtual.rs",
        },
    },
    Operator {
        ty: "GraphVectorKnnExec",
        runtime_name: "GraphVectorKnnExec",
        status: Status::Proven {
            by: "an_approx_eq_predicate_runs_the_vector_knn",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphShortestPathExec",
        runtime_name: "GraphShortestPathExec",
        status: Status::Proven {
            by: "a_shortest_path_runs_the_shortest_path_operator",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphExtIdLookupExec",
        runtime_name: "GraphExtIdLookupExec",
        status: Status::Unreachable {
            reason: "Retired 2026-09-28: an unlabelled `(n {ext_id: ...})` now plans as a \
                     schemaless ScanAll with the ext_id equality pushed to the main table's \
                     index. The dedicated lookup read flushed rows only, projected no \
                     properties and stringified every value; delete it with \
                     `LogicalPlan::ExtIdLookup` once nothing external depends on the variant.",
        },
    },
    Operator {
        ty: "RecursiveCTEExec",
        runtime_name: "RecursiveCTEExec",
        status: Status::Proven {
            by: "a_with_recursive_runs_the_recursive_cte",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "BindFixedPathExec",
        runtime_name: "BindFixedPathExec",
        status: Status::Proven {
            by: "a_named_fixed_path_runs_the_bind_fixed_path",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "BindZeroLengthPathExec",
        runtime_name: "BindZeroLengthPathExec",
        status: Status::Proven {
            by: "a_single_node_path_runs_the_bind_zero_length_path",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
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
        status: Status::Proven {
            by: "a_transactional_read_runs_the_read_set_recording",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // UNOBSERVABLE, not merely unproven. It is the root of a plan that
    // Still in neither profile surface: `impl_locy.rs` executes it directly so
    // no `ProfileOutput` is produced, and `LocyProfileOutput` carries what is
    // *inside* it rather than itself. Proven the way this row prescribed —
    // planning the logical node in `uni-query` and asserting `plan.name()`.
    // That proves emission rather than execution, which is one step weaker than
    // every other row here and is said plainly rather than papered over.
    Operator {
        ty: "LocyProgramExec",
        runtime_name: "LocyProgramExec",
        status: Status::Proven {
            by: "a_locy_program_node_plans_the_locy_program_operator",
            in_file: "crates/uni-query/tests/common/planner/locy_program_plan_shape.rs",
        },
    },
    // Was unobservable for a structural reason: constructed imperatively and
    // driven with `collect_all_partitions`, it is a child of no plan, so it
    // could not reach `runtime_stats` however often it ran, and the collector it
    // is handed records the clause bodies inside it rather than itself.
    // Resolved by giving the *stratum* an operator list. Attributing a fixpoint
    // driver to one of its rules would have been a misstatement — it evaluates
    // all of them — so it is reported where it belongs.
    Operator {
        ty: "FixpointExec",
        runtime_name: "FixpointExec",
        status: Status::Proven {
            by: "a_recursive_rule_runs_the_fixpoint_operator",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // Observable via `session.locy_with(..).profile()` op names (a positive
    // IS-reference cross-joins one in per occurrence). The gap was the accessor,
    // now `plan_shape::locy_plan_ops`.
    Operator {
        ty: "DerivedScanExec",
        runtime_name: "DerivedScanExec",
        status: Status::Proven {
            by: "an_is_reference_runs_the_derived_scan",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // `LocyFold` is still never lowered into a clause body -- wrapping the body
    // would double-apply the aggregate -- so the `FoldExec` that runs is built
    // in the post-fixpoint chain. Fixed the way this row prescribed: the chain
    // now reports its own operators into the rule's profile. Recorded at each of
    // the chain's execution points, because every stage replaces the plan with
    // an in-memory source over its output and only the stage that ran a tree can
    // report it.
    Operator {
        ty: "FoldExec",
        runtime_name: "FoldExec",
        status: Status::Proven {
            by: "a_fold_runs_the_fold_operator",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // The guard is a trap worth keeping stated: the body wrapper is emitted only
    // when `best_by.is_some() && fold.is_empty()`. With FOLD it is deferred to
    // the post-fixpoint chain, which is why every pre-existing BEST BY test in
    // the tree — all of which pair it with FOLD — could not serve as this proof.
    // The proof uses BEST BY without FOLD; the chain path is now observable too.
    Operator {
        ty: "BestByExec",
        runtime_name: "BestByExec",
        status: Status::Proven {
            by: "a_best_by_without_fold_runs_the_best_by_operator",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // Same shape as `FoldExec`: `clause.priority` stays a scalar field and is
    // never lowered into the body plan (the planner's own
    // `test_clause_with_priority` asserts no wrapper), so it runs only in the
    // post-fixpoint chain — which now reports its operators.
    Operator {
        ty: "PriorityExec",
        runtime_name: "PriorityExec",
        status: Status::Proven {
            by: "a_priority_rule_runs_the_priority_operator",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // A generator condition (`name(args) -> (outs)`) in a rule body.
    Operator {
        ty: "LocyGenerateExec",
        runtime_name: "LocyGenerateExec",
        status: Status::Proven {
            by: "a_generator_rule_runs_the_generate_operator",
            in_file: "crates/uni/tests/common/locy/locy_generator_plugin.rs",
        },
    },
    // A clause calling a `CREATE MODEL` name emits it, with a `MockClassifier`
    // standing in for the service. Proving it turned up a real defect first:
    // `LocyBuilder::profile` compiled its explain half through the no-config
    // entry point, so it rejected any `CREATE MODEL` program that `run()`
    // executed happily. Fixed by threading the builder's config into the
    // compile step.
    Operator {
        ty: "LocyModelInvokeExec",
        runtime_name: "LocyModelInvokeExec",
        status: Status::Proven {
            by: "a_model_call_runs_the_model_invoke",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // Not reachable from Cypher at all: it comes from
    // `StorageTableProvider::scan`, and nothing in `uni`/`uni-query` registers
    // that provider into a session context. The virtual-label MATCH path goes
    // to `CatalogVertexScanExec` instead — a different leg to the same trait.
    // Proven from `crates/uni-query/tests` against a raw DataFusion
    // `SessionContext`, as this row prescribed. Note the pre-existing EXPLAIN
    // assertion there is not the proof: it matches by substring and disjoins
    // with `"TableScan"`, the *logical* name, so it holds either way.
    Operator {
        ty: "StorageScanExec",
        runtime_name: "StorageScanExec",
        status: Status::Proven {
            by: "a_storage_table_scan_runs_the_storage_scan_exec",
            in_file: "crates/uni-query/tests/common/dispatch/storage_table_provider.rs",
        },
    },
];
