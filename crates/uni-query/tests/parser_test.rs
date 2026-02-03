// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_cypher::ast::{BinaryOp, Clause, Expr, Query};

fn parse_query(input: &str) -> Query {
    uni_query::parse_cypher(input).unwrap()
}

fn get_match_where(query: Query) -> Option<Expr> {
    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };
    let Clause::Match(m) = stmt.clauses.first()? else {
        panic!("Expected MATCH clause");
    };
    m.where_clause.clone()
}

#[test]
fn test_parse_string_operators() {
    // CONTAINS
    let query = parse_query("MATCH (n) WHERE n.name CONTAINS 'foo' RETURN n");
    let Some(Expr::BinaryOp { op, .. }) = get_match_where(query) else {
        panic!("Expected binary op in WHERE clause");
    };
    assert_eq!(op, BinaryOp::Contains);

    // STARTS WITH - verify parsing succeeds
    let _ = parse_query("MATCH (n) WHERE n.name STARTS WITH 'foo' RETURN n");

    // ENDS WITH - verify parsing succeeds
    let _ = parse_query("MATCH (n) WHERE n.name ENDS WITH 'foo' RETURN n");
}

#[test]
fn test_parse_regex_operator() {
    // Basic regex operator
    let query = parse_query("MATCH (n) WHERE n.email =~ '.*@gmail\\.com$' RETURN n");
    let Some(Expr::BinaryOp { op, right, .. }) = get_match_where(query) else {
        panic!("Expected binary op in WHERE clause");
    };
    assert_eq!(op, BinaryOp::Regex, "Expected Regex operator");

    let Expr::Literal(uni_cypher::ast::CypherLiteral::String(pattern)) = right.as_ref() else {
        panic!("Expected string literal pattern");
    };
    assert_eq!(pattern, ".*@gmail\\.com$");

    // Case insensitive regex
    let query = parse_query("MATCH (n:Person) WHERE n.name =~ '(?i)john' RETURN n");
    let Some(Expr::BinaryOp { op, .. }) = get_match_where(query) else {
        panic!("Expected binary op in WHERE clause");
    };
    assert_eq!(op, BinaryOp::Regex);
}
