pub mod btic_cypher_test;
pub mod locy_assume_module_context;
pub mod locy_assume_params;
pub mod locy_derive_visibility;
pub mod locy_fold_having;
pub mod locy_fold_property_key;
pub mod locy_fold_related_repro;
pub mod locy_generator_plugin;
pub mod locy_integration;
pub mod locy_is_ref_fold_alias;
pub mod locy_is_ref_value_col_type;
pub mod locy_issue_111_duration_arithmetic_repro;
pub mod locy_issue_112_key_no_fold_value_repro;
pub mod locy_issue_113_btic_contains_where_repro;
pub mod locy_issue_145_fold_rename_repro;
pub mod locy_issue_94_key_property_repro;
pub mod locy_native_integration;
pub mod locy_non_send_repro;
pub mod locy_nonlinear_recursion;
pub mod locy_predicate_plugin;
pub mod locy_prefix_to_grouped_recursion;
pub mod locy_profile;
pub mod locy_program_persistence;
pub mod locy_warded_parenthesized_path;
pub mod repro_locy_runtime_distinct_debug_dedup;
// Correctness-scan Wave 2 repros (R12 Locy probabilistic).
pub mod locy_rule_durability;
pub mod locy_snapshot_context_independence;
pub mod locy_ssi_read_set;
pub mod locy_timeout_partial;
pub mod locy_type_projection_matrix;
pub mod repro_locy_runtime_abduce_target_var;
pub mod repro_locy_runtime_topk_mnor_mixed_support;
pub mod repro_locy_runtime_wmc_shared_lineage;
pub mod value_assert;
// Issue #158: IS NOT fails open when the negated subject is not a projected
// column name (the reported "must be a KEY column" trigger is incorrect).
pub mod locy_issue_158_is_not_subject_scope;
// Issue #159: aggregation inside a recursive rule dedups equal values.
pub mod locy_issue_159_recursive_fold_dedup;
// Issue #160: QUERY (SLG) and derived (fixpoint) diverge when an IS-ref
// introduces a variable binding the MATCH pattern does not provide.
pub mod locy_issue_160_query_derived_parity;
// Schemaless YIELD property columns are inferred as Float64, so `derived`
// reports NULL for strings and floats for ints while QUERY reports the real
// value. Found by the generic QUERY/derived parity guard.
pub mod locy_schemaless_property_type_inference;
// A string literal in YIELD is NULL in `derived` while QUERY returns it.
// Same class as the schemaless property defect, different cause.
pub mod locy_string_literal_yield;
// Issue #162: a PROB fold that is correct in its own rule arrives one factor
// short at the rule that consumes it, when the folded values are equal.
pub mod locy_issue_162_prob_fold_consumer;
// Issue #162 diagnostic: which BOM shapes the recursive MPROD rollup gets wrong.
pub mod locy_issue_162_shape_matrix;
// Issue #162: scaling guard for the per-iteration folded view.
pub mod locy_issue_162_fold_scaling;
// Repros for the two Debug-fallback nondeterminism defects (#236 + sibling).
pub mod locy_debug_fallback_nondeterminism;
