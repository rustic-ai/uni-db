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
pub mod issue_175_index_consulted;
pub mod issue_175_vector_fts_consulted;
pub mod issue_223_main_edge_scan_shape;
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
pub mod repro_233_periodic_cancel_durability;
pub mod repro_edge_export;
pub mod underscore_property_reads;
// Correctness-scan Wave 0 repros.
pub mod repro_fork_sweeper_shutdown;
pub mod repro_hybrid_dense_arm_swallow;
pub mod repro_schema_edge_type_swallow;
// Correctness-scan Wave 1 repros (R5 constraint visibility).
pub mod bug_bulk_index_skip_both_defer_false_repro;
pub mod bug_bulk_unique_preexisting_repro;
// Correctness-scan Wave 0 findings D9/D10 (docs/correctness-deferred.md).
// Both are FIXED — these tests are active and assert the fixed behavior. The
// note here used to say they were "neither fixed nor deferred" with the
// bug-pinning tests "#[ignore]d until fixed"; neither was true any more.
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
// #181: flush() resurrected an edge removed by DETACH DELETE, leaving it with a
// null endpoint — correct before the flush, wrong after.
pub mod repro_issue_181_flush_resurrects_detached_edge;
// #182: flush() raised an internal error after deleting a vertex that had never
// been flushed — the tombstone had no table to merge into.
pub mod repro_issue_182_delete_before_first_flush;
// #249: declaring a property on a label that already has flushed data leaves
// the Lance dataset unchanged, so the next write to that label is rejected.
pub mod boolean_context_is_strict;
pub mod bug_cypher_value_arg_decode;
pub mod delete_sees_tx_local_edges;
pub mod duplicate_property_projection;
pub mod element_id_of_bound_variables;
pub mod ext_id_lookup_sees_everything;
pub mod ext_id_on_labelled_nodes;
pub mod fts_rank_survives_flush;
pub mod fts_sees_current_text;
pub mod inline_element_where;
pub mod issue_225_merge_bound_anchor;
pub mod issue_266_bulk_update_lost_on_inserting_handle;
pub mod issue_268_reverse_traversal_anchor;
pub mod issue_269_bulk_rows_absent_from_vid_labels_index;
pub mod issue_290_runtime_bound_state;
pub mod issue_311_tx_index_rekey;
// A CREATE / MERGE endpoint carries only its identity, not its property bag.
pub mod issue_312_write_endpoint_fanout;
pub mod label_disjunction_in_expressions;
pub mod label_disjunction_on_traversal_target;
pub mod merge_path_binding;
pub mod merge_per_row_counters;
pub mod nested_scope_in_case_branch;
pub mod not_null_on_unflushed_rows;
pub mod optional_entity_is_null;
pub mod optional_match_clause_close;
pub mod optional_match_row_identity;
pub mod optional_traverse_chunked;
pub mod optional_where_bound_var_pushdown;
pub mod outer_variable_in_pattern_map;
// An outer node / relationship variable supports every entity operation inside a subquery.
pub mod outer_entity_in_subquery;
pub mod pattern_fast_path_constraints;
pub mod reachability_trail_any_predecessor;
pub mod reduce_accumulator_types;
pub mod relabel_reads;
pub mod repro_distinct_entity_over_counts;
pub mod repro_issue_249_add_property_to_existing_label;
pub mod repro_issue_304_declare_populated_label;
pub mod repro_issue_304_declared_type_conflict;
pub mod repro_issue_304_edge_type_declaration;
pub mod repro_optional_match_over_unwound_entity;
pub mod repro_path_element_hydration_after_flush;
pub mod repro_registry_monotonicity_oracle;
pub mod repro_schemaless_pattern_comprehension_empty;
pub mod repro_stream_error_loses_abort_type;
pub mod scan_range_walk_l0_rows;
pub mod schemaless_search_after_flush;
pub mod search_filter_on_unflushed;
pub mod sum_of_nothing_is_zero;
pub mod typed_values_survive_rewrites;
// sum/avg take numbers or durations; a string is a type error.
pub mod sum_avg_take_numbers_or_durations;
// An unsigned value above i64::MAX reads as a float, not a negative integer.
pub mod uint64_values_do_not_wrap;
// Numeric functions and statistics reject a non-number instead of answering NULL.
pub mod numeric_functions_take_numbers;
// Durations sort by length; min/max follow ORDER BY; `<` between durations is null.
pub mod duration_order;
// min/max over a declared DateTime / Time: the earliest or latest instant, offset kept.
pub mod min_max_keep_temporal_values;
pub mod mixed_type_hash_join;
pub mod unique_key_reuse;
pub mod user_maps_are_not_entities;
pub mod vid_lookup_join_payoff;
pub mod vid_lookup_join_reachability;
pub mod vlp_default_hop_bound_warns;
pub mod vlp_limit_stops_enumeration;
pub mod vlp_pinned_endpoint_prunes;
pub mod vlp_relationship_uniqueness;
pub mod with_rebinds_entity_names;
// A label check on a node that arrives as one value (`UNWIND`, a list).
pub mod unwound_entity_label_check;
// Two relationship types sharing a property: one `r.w` column, not two.
pub mod edge_wildcard_shared_property;
// A procedure called per row: void calls run, yields keep their types.
pub mod in_query_procedure_call;
// A WRITE procedure's writes belong to the calling transaction.
pub mod write_procedure_in_transaction;
// A procedure called in a transaction reads the transaction's view.
pub mod procedure_reads_in_transaction;
pub mod procedure_reads_in_transaction_on_fork;
// An algorithm's list and map yields are lists and maps to every consumer.
pub mod algorithm_list_yield_values;
// A relationship bound earlier and matched again, with its type undeclared.
pub mod rebound_relationship_schemaless;
// A quoted identifier (incl. the doubled-backtick escape) denotes one name
// across DDL, writes, reads, indexes, constraints and parameters.
pub mod escaped_identifier_round_trip;
// A user property named `label`, `labels`, `type` or `properties` is an
// ordinary property on every read path, never taken for an entity field.
pub mod call_subquery_set_declared_types;
pub mod declared_endpoint_narrowing;
pub mod declared_endpoint_violation_reads;
// `SET` / `REMOVE` on a relationship cannot move it or change its type.
pub mod update_edge_identity;
// An older binary writing after an enforcing one voids endpoint conformance.
pub mod endpoint_conformance_writer_features;
pub mod entity_field_name_collision;
pub mod graph_project_in_transaction;
pub mod mixed_declared_undeclared_edge_types;
pub mod search_yield_node_is_full_entity;
pub mod unbound_parameter_in_write;
// WAL LSNs stay above the manifest mark after a reopen.
pub mod wal_lsn_after_reopen;
// Mutating built-in procedures are refused in read-only contexts and are
// authorized as `dbms` / `schema`, never `read`.
pub mod admin_procedure_read_only;
// UNWIND evaluates its list expression with the expression compiler, and
// types `nodes(p)` / `relationships(p)` elements as entities.
pub mod unwind_entity_attribute_functions;
// #305: dropping an unfinished BulkWriter must not lose committed rows.
pub mod issue_305_dropped_bulk_writer;
// #310: FTS scores must not collapse to 0.5 before the index is built.
pub mod issue_310_fts_score_constant_before_index;
