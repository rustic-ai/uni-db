// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_cypher::ast::{BinaryOp, CallKind, Clause, Expr, Query};

fn parse_query(input: &str) -> Query {
    uni_query::parse_cypher(input).unwrap()
}

fn get_match_where(query: Query) -> Expr {
    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };
    let Clause::Match(m) = stmt.clauses.into_iter().next().unwrap() else {
        panic!("Expected MATCH clause");
    };
    m.where_clause.expect("Expected WHERE clause")
}

#[test]
fn test_parse_complex_where_with_or_and() {
    let query = parse_query("MATCH (n) WHERE n.age > 20 AND (n.name = 'Alice' OR n.name = 'Bob')");
    let where_expr = get_match_where(query);

    // Top level should be AND: n.age > 20 AND (...)
    let Expr::BinaryOp { op, right, .. } = where_expr else {
        panic!("Expected BinaryOp in WHERE clause");
    };
    assert_eq!(op, BinaryOp::And);

    // Right side should be OR: (n.name = 'Alice' OR n.name = 'Bob')
    let Expr::BinaryOp { op: inner_op, .. } = right.as_ref() else {
        panic!("Right side should be BinaryOp (OR)");
    };
    assert_eq!(*inner_op, BinaryOp::Or);
}

#[test]
fn test_parse_nested_function_calls() {
    let query = parse_query("RETURN count(distinct toInteger(head(keys(n))))");

    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };
    assert!(stmt.clauses.iter().any(|c| matches!(c, Clause::Return(_))));
}

#[test]
fn test_parse_list_comprehension_edge_cases() {
    // Empty list
    let _ = parse_query("RETURN [x IN [] | x]");

    // Nested
    let _ = parse_query("RETURN [x IN [1,2] | [y IN [3,4] | x+y]]");

    // Filter with mapping (filter-only syntax requires | expr)
    let _ = parse_query("RETURN [x IN [1] WHERE x > 0 | x]");
}

#[test]
fn test_parse_map_literal_edge_cases() {
    // Empty map
    let _ = parse_query("RETURN {}");

    // Nested map
    let _ = parse_query("RETURN {a: {b: 1}}");

    // Keywords as keys - this is the failing case
    let _ = parse_query("RETURN {match: 1, return: 2}");
}

#[test]
fn test_parse_dotted_procedure_calls() {
    // Single identifier
    assert_call_procedure("CALL proc() YIELD x", "proc");

    // Two parts
    assert_call_procedure("CALL uni.algo.pageRank() YIELD score", "uni.algo.pageRank");

    // Three parts
    assert_call_procedure(
        "CALL uni.vector.query('Label', 'prop', [1.0], 10) YIELD node",
        "uni.vector.query",
    );

    // Four parts (main use case for uni.vector.query)
    assert_call_procedure(
        "CALL uni.vector.query('Label', 'prop', [1.0], 10) YIELD node, distance",
        "uni.vector.query",
    );

    // Five parts (edge case)
    assert_call_procedure("CALL a.b.c.d.e() YIELD result", "a.b.c.d.e");
}

fn assert_call_procedure(cypher: &str, expected_procedure: &str) {
    let query = parse_query(cypher);
    let Query::Single(stmt) = query else {
        panic!("Expected single query for: {}", cypher);
    };
    let Some(Clause::Call(call)) = stmt.clauses.first() else {
        panic!("Expected CALL clause for: {}", cypher);
    };
    let CallKind::Procedure { procedure, .. } = &call.kind else {
        panic!("Expected Procedure kind for: {}", cypher);
    };
    assert_eq!(
        procedure, expected_procedure,
        "Procedure name mismatch for: {}",
        cypher
    );
}
