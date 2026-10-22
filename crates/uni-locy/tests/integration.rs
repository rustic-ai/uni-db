// Consolidated integration-test binary: every test file links into one
// binary to minimize compile/link time. See docs/test_layout.md.
// Add new integration tests as a `mod` here, NOT as a new tests/*.rs file.

mod assume_body_explain_abduce;
mod repro_along_having_arity_skipped;
mod repro_along_prev_in_comparison;
mod repro_assume_body_outer_rule_ref;
mod repro_isotonic_tied_preds;
mod repro_module_self_recursion_typecheck;
mod repro_pathctx_list_no_dep_edge;
mod repro_warded_parenthesized_path;
