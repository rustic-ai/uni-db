pub mod bug_bulk_edge_create_repro;
pub mod bug_call_yield_where_dropped;
pub mod bug_coalesce_utf8;
pub mod bug_edge_id_in_where;
pub mod bug_empty_typed_list_inference;
pub mod bug_merge_on_create_not_null;
pub mod bug_rc13_int16_missing;
pub mod bug_rc2_merge_phantom_and_atomic_set;
pub mod bug_rc3_merge_general_path_perf;
pub mod bug_rc6_get_edges_post_flush_step;
pub mod bug_rc_pattern_where_index_pushdown;
pub mod bug_rebuild_indexes_path;
pub mod bug_traversal_filtering;
pub mod bug_vector_query_l0_scores;
pub mod bytes_aggregates;
pub mod bytes_computed_projection;
pub mod bytes_containers;
pub mod bytes_list_functions;
pub mod bytes_maps_projection;
pub mod bytes_pipeline;
pub mod collect_plan_vars_join_recovery;
pub mod deleted_vertex_label_resurrection;
pub mod graph_projection_tx_visibility;
pub mod issue115_storage_path_contract;
pub mod issue116_composite_key_flush;
pub mod issue43_insert_latency_diagnostic;
pub mod issue43_repro;
pub mod issue46_compaction_race;
pub mod issue47_edge_latency;
pub mod issue47_instrumented;
pub mod issue47_profile;
pub mod issue47_repro_standalone;
pub mod issue49_datetime_autoembed_latency;
pub mod issue53_unwind_match_perf;
pub mod issue69_unwind_merge_perf;
pub mod issue_100_collect_bytes;
pub mod issue_131_locy_iter_cross_join;
pub mod issue_137_vector_dim_enforcement;
pub mod issue_41_pattern_exists_perf;
pub mod issue_55_batch_edge_patterns;
pub mod issue_55_cross_match_pushdown;
pub mod issue_55_get_edges_scaling;
pub mod issue_55_get_edges_scaling_autoembed;
pub mod issue_55_instrumented;
pub mod issue_55_observed_in_growth;
pub mod issue_55_observed_in_growth_no_embed;
pub mod issue_55_probe_layer;
pub mod issue_57_match_label_hash_index;
pub mod issue_68_type_mismatch;
pub mod issue_93_bytes_round_trip;
pub mod locy_is_not_complement_recursion;
pub mod pattern_exists_unbound_param;
pub mod relationship_uniqueness_invariant;
pub mod repro_edge_export;
// Correctness-scan Wave 0 repros.
pub mod repro_fork_sweeper_shutdown;
pub mod repro_hybrid_dense_arm_swallow;
pub mod repro_schema_edge_type_swallow;
// Correctness-scan Wave 1 repros (R5 constraint visibility).
pub mod bug_bulk_index_skip_both_defer_false_repro;
pub mod bug_bulk_unique_preexisting_repro;
// Correctness-scan Wave 0 findings that fell through — neither fixed nor
// deferred; tracked as D9/D10 in docs/correctness-deferred.md (bug-pinning
// tests are #[ignore]d until fixed).
pub mod bug_bulk_check_int_float_repro; // uni-bulk[5] / D10
pub mod bug_bulk_check_large_int_repro; // uni-bulk compare_values i64->f64 (D5 mirror)
pub mod bug_bulk_flush_intent_abandon_repro; // uni-bulk[2] / D9
// Correctness-scan Wave 1 repros (R10 integer precision / lossy key).
pub mod bug_bulk_unique_key_lossy_repro;
// Correctness-scan Wave 1 repros (R11 Locy compile-context / registry).
pub mod repro_locy_tx_neural_preview;
pub mod repro_rule_promotion_strata;
pub mod repro_rule_registry_lost_update;
// Correctness-scan Wave 2 repros (L6 security/authz).
pub mod repro_authz_query_bypass;
pub mod repro_config_path_plugin_registry;
// Correctness-scan Wave 2 repros (L7 commit-timeout after durable point).
pub mod repro_commit_timeout_after_durable;
// Correctness-scan Wave 4 repro (fork-local index-kind collision).
pub mod hash_index_range_quoting;
pub mod map_projection_aggregate;
pub mod repro_fork_index_kind_collision;
pub mod shutdown_reaps_scratch_dir;
pub mod test_issue_72_version_recovery;
pub mod test_overflow_fix;
pub mod test_python_repro;
pub mod traverse_labels_after_flush;
pub mod unwind_correlated_hash_index_pushdown;
pub mod vlp_label_filter_after_flush;
// `VERSION AS OF` reaching the planner un-unwrapped (explain/profile/cursor).
pub mod repro_time_travel_explain_profile_cursor;
// Tier 0 item 0.15: properties() empty for an unlabelled multi-label endpoint.
pub mod repro_multi_label_endpoint_properties;
// Tier 1.6: get_edge_type_info interpolated the type name into Cypher unquoted
// and swallowed the resulting parse error as a count of 0.
pub mod repro_edge_type_info_count;
// Tier 1.4: the CHECK evaluator existed as two copies whose equality operators
// had drifted, so bulk and tx disagreed on the same row.
pub mod repro_tx_check_int_float;
// Tier 1.5: the compile-time monotonicity oracle never consulted the registry.
pub mod repro_issue_157_command_forms;
pub mod repro_issue_157_registry_rule_leak;
// #166: an inline property map on an *anonymous* relationship pattern is
// parsed and then discarded — the filter is gated on a bound edge variable.
pub mod repro_issue_166_rel_property_map_ignored;
// #166 family: the same dropped-edge-predicate shape in MERGE, QPP and
// shortestPath, found by asking where else the #166 gate occurs.
pub mod repro_issue_166_family_dropped_edge_predicates;
// #167: dropping a temporary database without `shutdown()` usually strands
// its `uni_mem_*` directory; `Drop` signals and awaits nothing.
pub mod repro_issue_167_temporary_leaks_on_drop;
// #168: a read-only handle warmed on an edge query never re-warms its CSR, so
// it serves stale edges (while nodes, which reopen the dataset, stay fresh).
pub mod repro_issue_168_readonly_stale_edges;
// #168 family: other caches a second handle warms once and never revalidates.
pub mod repro_issue_168_family_stale_second_handle;
// #169: a fork of a read-only parent captures a zeroed `ForkPoint`, so its
// version floor and VID allocator start at 0 and the two languages disagree.
pub mod repro_issue_169_readonly_fork_incoherent;
pub mod repro_registry_monotonicity_oracle;
pub mod vid_lookup_join_reachability;
