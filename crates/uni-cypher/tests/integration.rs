// Consolidated integration-test binary: every test file links into one
// binary to minimize compile/link time. See docs/test_layout.md.
// Add new integration tests as a `mod` here, NOT as a new tests/*.rs file.

mod docs_snippets;
mod parse_depth_limit;
mod parser_backtracking_regression;
mod path_quantifier_range;
mod repro_backtick_property_name;
mod repro_escaped_identifier;
mod repro_locy_derive_backtick_label;
mod repro_map_key_not_unescaped;
mod repro_nesting_guard_end_evasion;
mod repro_parse_expression_trailing_garbage;
mod repro_remove_label_backtick;
mod tck_test_suite;
