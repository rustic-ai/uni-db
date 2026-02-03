// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for OpenCypher compatibility features (v2.0)
//!
//! This file tests:
//! - Bitwise functions (uni.bitwise.*)
//! - RETURN * syntax
//!
//! Note: List and pattern comprehensions are deferred to v2.1+ due to LALRPOP LR(1) limitations.
//! See PATTERN_COMP_DEFER.md for details.

use uni_cypher::ast::{Clause, Expr, Query, ReturnItem};

fn parse_query(input: &str) -> Query {
    uni_query::parse_cypher(input).expect("Parse failed")
}

fn parse_return_expr(input: &str) -> Expr {
    let query = parse_query(input);

    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };

    let return_clause = stmt
        .clauses
        .iter()
        .find_map(|c| {
            if let Clause::Return(r) = c {
                Some(r)
            } else {
                None
            }
        })
        .expect("Expected return clause");

    match &return_clause.items[0] {
        ReturnItem::Expr { expr, .. } => expr.clone(),
        ReturnItem::All => panic!("Expected expression, got RETURN *"),
    }
}

// ============================================================================
// Bitwise Function Tests (uni.bitwise.*)
// ============================================================================

#[test]
fn test_parse_bitwise_or_function() {
    let expr = parse_return_expr("RETURN uni_bitwise_or(5, 3)");
    let Expr::FunctionCall { name, args, .. } = expr else {
        panic!("Expected function call, got {:?}", expr);
    };

    assert_eq!(name, "uni_bitwise_or");
    assert_eq!(args.len(), 2);
    assert_eq!(
        args[0],
        Expr::Literal(uni_cypher::ast::CypherLiteral::Integer(5))
    );
    assert_eq!(
        args[1],
        Expr::Literal(uni_cypher::ast::CypherLiteral::Integer(3))
    );
}

#[test]
fn test_parse_bitwise_and_function() {
    let expr = parse_return_expr("RETURN uni_bitwise_and(12, 10)");
    let Expr::FunctionCall { name, args, .. } = expr else {
        panic!("Expected function call, got {:?}", expr);
    };

    assert_eq!(name, "uni_bitwise_and");
    assert_eq!(args.len(), 2);
}

#[test]
fn test_parse_bitwise_xor_function() {
    let expr = parse_return_expr("RETURN uni_bitwise_xor(12, 10)");
    let Expr::FunctionCall { name, args, .. } = expr else {
        panic!("Expected function call, got {:?}", expr);
    };

    assert_eq!(name, "uni_bitwise_xor");
    assert_eq!(args.len(), 2);
}

#[test]
fn test_parse_bitwise_not_function() {
    let expr = parse_return_expr("RETURN uni_bitwise_not(5)");
    let Expr::FunctionCall { name, args, .. } = expr else {
        panic!("Expected function call, got {:?}", expr);
    };

    assert_eq!(name, "uni_bitwise_not");
    assert_eq!(args.len(), 1);
    assert_eq!(
        args[0],
        Expr::Literal(uni_cypher::ast::CypherLiteral::Integer(5))
    );
}

#[test]
fn test_parse_shift_left_function() {
    let expr = parse_return_expr("RETURN uni_bitwise_shiftLeft(3, 2)");
    let Expr::FunctionCall { name, args, .. } = expr else {
        panic!("Expected function call, got {:?}", expr);
    };

    assert_eq!(name, "uni_bitwise_shiftLeft");
    assert_eq!(args.len(), 2);
}

#[test]
fn test_parse_shift_right_function() {
    let expr = parse_return_expr("RETURN uni_bitwise_shiftRight(12, 2)");
    let Expr::FunctionCall { name, args, .. } = expr else {
        panic!("Expected function call, got {:?}", expr);
    };

    assert_eq!(name, "uni_bitwise_shiftRight");
    assert_eq!(args.len(), 2);
}

#[test]
fn test_parse_nested_bitwise_functions() {
    // uni_bitwise_or(uni_bitwise_and(12, 10), 5)
    let expr = parse_return_expr("RETURN uni_bitwise_or(uni_bitwise_and(12, 10), 5)");
    let Expr::FunctionCall { name, args, .. } = expr else {
        panic!("Expected function call, got {:?}", expr);
    };

    assert_eq!(name, "uni_bitwise_or");
    assert_eq!(args.len(), 2);

    // First argument should be a nested function call
    let Expr::FunctionCall {
        name: inner_name, ..
    } = &args[0]
    else {
        panic!("Expected nested function call");
    };
    assert_eq!(inner_name, "uni_bitwise_and");
}

// ============================================================================
// RETURN * Tests
// ============================================================================

#[test]
fn test_parse_return_star() {
    let query = parse_query("RETURN *");
    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };

    let Clause::Return(ret) = &stmt.clauses[0] else {
        panic!("Expected RETURN clause");
    };

    assert_eq!(ret.items.len(), 1);
    assert!(matches!(ret.items[0], ReturnItem::All));
}

#[test]
fn test_parse_return_star_with_items() {
    let query = parse_query("RETURN *, n.name AS name");
    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };

    let Clause::Return(ret) = &stmt.clauses[0] else {
        panic!("Expected RETURN clause");
    };

    assert_eq!(ret.items.len(), 2);
    assert!(matches!(ret.items[0], ReturnItem::All));

    match &ret.items[1] {
        ReturnItem::Expr { alias, .. } => {
            assert_eq!(alias.as_deref(), Some("name"));
        }
        ReturnItem::All => panic!("Expected expression item"),
    }
}

#[test]
fn test_parse_return_star_in_match() {
    let query = parse_query("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN *");
    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };

    // Should have MATCH and RETURN clauses
    assert_eq!(stmt.clauses.len(), 2);

    let Clause::Return(ret) = &stmt.clauses[1] else {
        panic!("Expected RETURN clause as second clause");
    };

    assert_eq!(ret.items.len(), 1);
    assert!(matches!(ret.items[0], ReturnItem::All));
}

// ============================================================================
// Tests for features that are NOT supported (should fail to parse)
// ============================================================================

#[test]
#[should_panic(expected = "Parse failed")]
fn test_bitwise_or_operator_not_supported() {
    // Bitwise operators are no longer supported in grammar
    parse_return_expr("RETURN 5 | 3");
}

#[test]
#[should_panic(expected = "Parse failed")]
fn test_bitwise_and_operator_not_supported() {
    parse_return_expr("RETURN 5 & 3");
}

#[test]
#[should_panic(expected = "Parse failed")]
fn test_bitwise_xor_operator_not_supported() {
    parse_return_expr("RETURN 5 ^^ 3");
}

#[test]
#[should_panic(expected = "Parse failed")]
fn test_bitwise_not_operator_not_supported() {
    parse_return_expr("RETURN ~5");
}

#[test]
#[should_panic(expected = "Parse failed")]
fn test_shift_left_operator_not_supported() {
    parse_return_expr("RETURN 3 << 2");
}

#[test]
#[should_panic(expected = "Parse failed")]
fn test_shift_right_operator_not_supported() {
    parse_return_expr("RETURN 12 >> 2");
}

// ============================================================================
// List Comprehensions - Deferred to v2.1+
// ============================================================================

#[test]
fn test_list_comprehension_basic() {
    let _expr = parse_return_expr("RETURN [x IN [1,2,3] | x * 2]");
}

#[test]
fn test_list_comprehension_with_where() {
    let _expr = parse_return_expr("RETURN [x IN [1,2,3] WHERE x > 1 | x * 2]");
}
