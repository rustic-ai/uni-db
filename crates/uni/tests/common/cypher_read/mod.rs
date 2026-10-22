pub mod aggregate_window_test;
pub mod case_test;
pub mod collected_list_interning_test;
pub mod comparison_test;
pub mod constant_in_list_test;
pub mod cypher_aggregation;
pub mod cypher_call;
pub mod cypher_clauses_ext;
pub mod cypher_copy;
pub mod cypher_exists;
pub mod cypher_filtering;
pub mod cypher_gaps;
pub mod cypher_label_disjunction_support;
pub mod cypher_limit_order;
pub mod cypher_list_comprehension;
pub mod cypher_optional;
pub mod cypher_pushdown;
pub mod cypher_reduce;
pub mod cypher_subquery_parser;
pub mod cypher_union;
pub mod datetime_epoch_map_test;
pub mod ddl_numeric_width_test;
pub mod dynamic_access_test;
pub mod entity_after_unwind_test;
mod entity_comparison_test;
pub mod entity_function_binding_shapes_test;
pub mod entity_identity_in_list_test;
pub mod explain_test;
pub mod expression_value_semantics_test;
pub mod float_ieee_semantics_test;
pub mod int_arithmetic_overflow_test;
pub mod map_literal_test;
pub mod map_projection_test;
pub mod mixed_numeric_list_param_test;
pub mod mixed_type_list_literal_test;
pub mod normalization_test;
pub mod not_operator_test;
pub mod null_handling_test;
pub mod numeric_exact_comparison_test;
pub mod order_by_flow_test;
pub mod order_by_sort_key_test;
pub mod path_property_access;
pub mod pattern_comprehension_column_order_test;
pub mod pattern_comprehension_entity_test;
pub mod pattern_predicate_in_projection_test;
pub mod prepared_query_test;
pub mod procedure_call_df_test;
pub mod procedure_variable_args_test;
pub mod pushdown_hydration_e2e;
pub mod recursive_cte_execution_test;
pub mod reduce_execution_test;
pub mod return_expressions_test;
pub mod scalar_subquery_test;
pub mod start_end_node_test;
pub mod stddev_variance_test;
pub mod temporal_property_type_test;
pub mod traversal_target_dedup_test;
pub mod type_conversion_test;
pub mod unanchored_pattern_comprehension_test;
pub mod union_entity_test;
pub mod unwind_source_pruning_test;
pub mod where_clause_test;
pub mod where_debug_test;
pub mod window_execution_test;
pub mod with_clause_test;
// WITH … LIMIT … WHERE over scans a predicate could be pushed into.
pub mod with_orderby_regression_test;
pub mod with_where_after_page_scans;
// Correlated EXISTS / COUNT bodies answered once per source, not per pair.
pub mod exists_per_source_test;
// percentileDisc returns an exact input; both executors read the percentile.
pub mod percentile_exact_test;
// A declared property type applies only to the labels that declare it.
pub mod schemaless_property_type_scope_test;
// Reachability consumers of a variable-length pattern decide from distances.
pub mod trail_reachability_test;
// range() with a null argument returns null on every evaluator.
pub mod range_null_test;
// `=~` matches the whole string (Java `matches()`) on every evaluator.
pub mod regex_full_match_test;
// COPY TO writes exactly the user-visible properties; COPY FROM reads them back.
pub mod copy_user_properties;
// Unconstrained traversal sources read from the edge type's adjacency.
pub mod edge_source_scan_test;
