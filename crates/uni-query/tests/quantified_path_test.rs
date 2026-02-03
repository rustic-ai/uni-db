// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_cypher::ast::{Clause, MatchClause, PatternElement, Query, Range};

fn parse_match(input: &str) -> MatchClause {
    let query = uni_query::parse_cypher(input).unwrap();

    let Query::Single(stmt) = query else {
        panic!("Expected single query");
    };
    let Clause::Match(m) = stmt.clauses.into_iter().next().unwrap() else {
        panic!("Expected MATCH clause");
    };
    m
}

fn get_quantified_range(m: &MatchClause) -> &Range {
    let PatternElement::Parenthesized { range, .. } = &m.pattern.paths[0].elements[0] else {
        panic!("Expected parenthesized pattern element");
    };
    range.as_ref().expect("Expected range on quantified path")
}

#[test]
fn test_parse_quantified_path_fixed() {
    let m = parse_match("MATCH ((a)-[:REL]->(b)){3} RETURN a");

    assert_eq!(
        m.pattern.paths[0].elements.len(),
        1,
        "Expected one parenthesized pattern element"
    );

    let range = get_quantified_range(&m);
    assert_eq!(range.min, Some(3));
    assert_eq!(range.max, Some(3));
}

#[test]
fn test_parse_quantified_path_range() {
    let m = parse_match("MATCH ((a)-[:REL]->(b)){1,5} RETURN a");

    let range = get_quantified_range(&m);
    assert_eq!(range.min, Some(1));
    assert_eq!(range.max, Some(5));
}

#[test]
fn test_parse_quantified_path_unbounded() {
    let m = parse_match("MATCH ((a)-[:REL]->(b)){1,} RETURN a");

    let range = get_quantified_range(&m);
    assert_eq!(range.min, Some(1));
    assert_eq!(range.max, None); // Unbounded
}

#[test]
fn test_parse_normal_pattern() {
    let m = parse_match("MATCH (a)-[:REL]->(b) RETURN a");

    // Normal patterns have Node and Relationship elements, not Parenthesized
    let path = &m.pattern.paths[0];
    assert!(
        path.elements.len() > 1,
        "Expected multiple elements in normal pattern"
    );

    // Verify none are parenthesized
    for elem in &path.elements {
        assert!(!matches!(elem, PatternElement::Parenthesized { .. }));
    }
}
