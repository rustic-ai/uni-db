pub mod btic_cypher_test;
pub mod locy_abduce_identity;
pub mod locy_assume_incremental;
pub mod locy_assume_module_context;
pub mod locy_assume_params;
pub mod locy_derive_visibility;
pub mod locy_entity_hydration;
pub mod locy_fold_having;
pub mod locy_fold_property_key;
pub mod locy_fold_related_repro;
pub mod locy_generator_plugin;
pub mod locy_goal_directed;
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
pub mod locy_program_plan_time;
pub mod locy_warded_parenthesized_path;
pub mod repro_locy_runtime_distinct_debug_dedup;
// Correctness-scan Wave 2 repros (R12 Locy probabilistic).
pub mod locy_rule_durability;
pub mod locy_snapshot_context_independence;
pub mod locy_ssi_read_set;
pub mod locy_timeout_partial;
pub mod locy_toplevel_sequencing;
pub mod locy_type_projection_matrix;
pub mod repro_locy_runtime_abduce_target_var;
pub mod repro_locy_runtime_topk_mnor_mixed_support;
pub mod repro_locy_runtime_wmc_shared_lineage;
// A positive IS reference to a PROB rule is a probabilistic condition.
pub mod locy_prob_is_ref_semantics;
pub mod value_assert;
// Issue #158: IS NOT fails open when the negated subject is not a projected
// column name (the reported "must be a KEY column" trigger is incorrect).
pub mod locy_issue_158_is_not_subject_scope;
// Issue #159: aggregation inside a recursive rule dedups equal values.
pub mod locy_issue_159_recursive_fold_dedup;
// A seed clause alongside a COUNT fold: the seed is one counted row.
pub mod locy_command_noninterference;
pub mod locy_count_fold_seed;
// Mutually recursive rules in one stratum keep their own facts.
pub mod locy_mutual_recursion_per_rule_facts;
// Issue #293: a FOLD silently dropped YIELD columns that were not KEY.
pub mod locy_issue_293_fold_ungrouped_yield;
// Issue #294: recursive FOLD collapsed parallel edges between one node pair.
pub mod locy_issue_294_parallel_edge_fold;
// Issue #301: recursive rule cost tracks the frontier, not the relation
// (cost guard, correctness cases, and an #[ignore] measurement probe).
pub mod locy_issue_301_fixpoint_delta;
// Issue #307: a QUERY WHERE on one subject derives what its goal reaches
// under the default configuration (and an #[ignore] measurement probe).
pub mod locy_issue_307_query_goal_pruning;
// P5 (2026-10-17 perf-class audit): semi-naive feeding of BEST BY (R2),
// non-linear and mutual recursion (R3), and the IS NOT banned set (R5).
pub mod locy_recursion_semi_naive;
// P4 (2026-10-17 perf-class audit): anchoring on the walk's target (A1b), on a
// lower-stratum reference (A2), and the node a reference binds.
pub mod locy_anchor_target_cross_stratum;
// Issue #160: QUERY (SLG) and derived (fixpoint) diverge when an IS-ref
// introduces a variable binding the MATCH pattern does not provide.
pub mod locy_issue_160_query_derived_parity;
// Schemaless YIELD property columns are inferred as Float64, so `derived`
// reports NULL for strings and floats for ints while QUERY reports the real
// value. Found by the generic QUERY/derived parity guard.
pub mod locy_schemaless_property_type_inference;
pub mod locy_scope_check;
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
pub mod locy_issue_216_in_three_valued_logic;
// Every operator over every operand-type pairing answers as Cypher does.
pub mod locy_issue_267_node_key_scaling;
pub mod locy_metrics_counters;
pub mod locy_operator_parity;
// Issue #272: `IS <rule> TO <value column>` returns zero rows with no error.
pub mod locy_issue_272_is_ref_to_value_column;
// Issue #273: a query parameter is not resolved in the post-FOLD (HAVING) position.
pub mod locy_issue_273_post_fold_param;
// A negated IS-ref whose TO target lands on a scalar column excluded nothing.
pub mod locy_is_not_scalar_to_target;
pub mod locy_is_ref_binding;
// Issue #300: IS-reference binding must not depend on conjunct order or names.
pub mod locy_is_ref_name_resolution;
// Issue #300: anchored IS references join by hash in every order.
pub mod locy_is_ref_hash_join;
// W4: a fact is a row; QUERY WHERE is three-valued; a sum of nothing is 0.
pub mod locy_facts_and_filters;
// W4: MIN/MAX/COLLECT and yielded properties keep their value types.
pub mod locy_clause_contract;
pub mod locy_single_engine;
// Entity functions (type, labels, id, keys, startNode, ...) in a Locy QUERY RETURN.
pub mod locy_entity_functions;
// A numeric FOLD refuses a string or a duration instead of summing it to 0.
pub mod locy_fold_reads_numbers;
// FOLD COLLECT/MIN/MAX/COUNT over every declared column type, Cypher as the oracle.
pub mod locy_fold_any_column_type;
pub mod locy_value_types;
pub mod locy_yield_list_values;
// Locy compares an integer with a float by exact value, as Cypher does.
pub mod locy_numeric_exact;
// An ASSUME body's commands run in sequence: a QUERY after a DERIVE or a
// Cypher write in the same body reads the state that write left.
pub mod locy_assume_body_sequencing;
// Wave P1 (WP3): proof weights and top-k retention (c), tx-prepared programs
// and `$params` in commands and replays (L1, L2), EXPLAIN / ABDUCE and exact
// probabilities in ASSUME bodies and CALIBRATE after a write (L3, L5), and the
// re-derivation budget (L4).
pub mod locy_assume_explain_abduce;
pub mod locy_proof_probability;
pub mod locy_rederivation_budget;
pub mod locy_tx_prepared_and_params;
// Wave Q (Q1): a registered rule's parameters are checked before evaluation.
pub mod locy_registered_rule_parameters;
// Wave Q (Q1): EXPLAIN children for every positive reference (Mode A), and
// Mode B probed over the same programs.
pub mod locy_explain_children;
// Wave Q (Q1): a command finishing past the deadline is reported (failpoints).
pub mod locy_budget_resilience;
// #308: EXPLAIN RULE expands a recursive derivation into its supporting facts.
pub mod locy_issue_308_explain_recursive_children;
// #309: tuple IS / prefix-TO bind a third (scalar) KEY column positionally.
pub mod locy_issue_309_third_key_binding;
// #313: exact-mode MNOR keeps distinct event identities apart.
pub mod locy_issue_313_exact_event_identity;
// Issue #314: classifier-backed relation consumed through IS.
pub mod locy_issue_314_classifier_relation_is;
// Issue #314 (class): a YIELD column carries its expression's type on every surface.
pub mod locy_yield_expression_types;
// Issue #315: shared-dependency diagnostics say whether exact inference resolved it.
pub mod locy_issue_315_dependency_diagnostics;
// #307: a failed deferred derivation of left-out rules caches nothing and
// is retried (failpoints).
pub mod locy_lazy_derived_resilience;
