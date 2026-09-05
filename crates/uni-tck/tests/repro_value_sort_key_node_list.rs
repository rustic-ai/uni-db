//! Repro for crates/uni-tck/src/matcher/result.rs:261 (value_sort_key).
//!
//! `value_sort_key` collapses every `Value::Node` to the constant key
//! `"8:node"` (and Edges/Paths/Maps/Lists/Vectors to content-free keys).
//! The `ignore_list_order` branch of the matcher sorts both lists by this key
//! and then zips element-wise. Because the key carries no discriminating
//! content for nodes and the sort is stable, two lists that are permutations of
//! each other keep their original relative order, so the zip pairs mismatched
//! nodes and the order-independent comparison degenerates into an
//! order-sensitive one.
//!
//! This drives the REAL public matcher API
//! (`match_result_ignoring_list_order`) with a real `QueryResult`.

use std::collections::HashMap;
use std::sync::Arc;

use uni_common::core::id::Vid;
use uni_query::{Node, QueryMetrics, QueryResult, Row, Value};
use uni_tck::matcher::match_result_ignoring_list_order;

/// Build a `Value::Node` with a single label `N` and an integer `id` property.
fn node(id: i64) -> Value {
    let mut properties = HashMap::new();
    properties.insert("id".to_string(), Value::Int(id));
    Value::Node(Node {
        vid: Vid::from(id as u64),
        labels: vec!["N".to_string()],
        properties,
    })
}

/// Wrap a single `l`-column list cell into a one-row `QueryResult`.
fn one_row_list_result(list: Vec<Value>) -> QueryResult {
    let columns = Arc::new(vec!["l".to_string()]);
    let row = Row::new(Arc::clone(&columns), vec![Value::List(list)]);
    QueryResult::new(columns, vec![row], Vec::new(), QueryMetrics::default())
}

/// Same list of nodes, permuted, must compare equal when list order is ignored.
#[test]
fn permuted_node_list_should_be_equal_ignoring_order() {
    // actual = [Node(id=1), Node(id=2)]
    let actual = one_row_list_result(vec![node(1), node(2)]);
    // expected = [Node(id=2), Node(id=1)] -- same multiset, different order.
    let mut expected_row = HashMap::new();
    expected_row.insert("l".to_string(), Value::List(vec![node(2), node(1)]));
    let expected_rows = vec![expected_row];

    let result = match_result_ignoring_list_order(&actual, &expected_rows);

    // CORRECT behavior (post-fix): Ok(()) -- the two lists are equal as multisets
    // and the comparison ignores element order. `value_sort_key` now encodes each
    // node's content, so the two lists sort into the same order and the zip pairs
    // Node(id=1) with Node(id=1) and Node(id=2) with Node(id=2).
    assert!(
        result.is_ok(),
        "permuted node lists must compare equal when ignoring element order, got {:?}",
        result
    );

    // Sanity: identical order DOES compare equal, proving the values themselves
    // are equal and only the permutation trips the matcher.
    let same_order_actual = one_row_list_result(vec![node(1), node(2)]);
    let mut same_order_expected = HashMap::new();
    same_order_expected.insert("l".to_string(), Value::List(vec![node(1), node(2)]));
    assert!(
        match_result_ignoring_list_order(
            &same_order_actual,
            std::slice::from_ref(&same_order_expected)
        )
        .is_ok(),
        "identical-order node lists must match"
    );
}

/// The correct-behavior assertion: permuted node lists compare equal.
#[test]
fn permuted_node_list_correct_behavior() {
    let actual = one_row_list_result(vec![node(1), node(2)]);
    let mut expected_row = HashMap::new();
    expected_row.insert("l".to_string(), Value::List(vec![node(2), node(1)]));

    match_result_ignoring_list_order(&actual, std::slice::from_ref(&expected_row))
        .expect("permuted node lists should compare equal when ignoring element order");
}
