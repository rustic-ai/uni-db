// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for JSON Full-Text Search functionality.

use chrono::Utc;
use uni_common::core::schema::{JsonFtsIndexConfig, LabelMeta, Schema, SchemaElementState};
use uni_query::query::pushdown::{IndexAwareAnalyzer, PushdownStrategy};

/// Helper to create a test schema with a document label and JSON FTS index.
fn create_fts_test_schema() -> Schema {
    let mut schema = Schema::default();

    // Add Article label (document type)
    let article_meta = LabelMeta {
        id: 1,
        created_at: Utc::now(),
        state: SchemaElementState::Active,
    };
    schema.labels.insert("Article".to_string(), article_meta);

    // Add JSON FTS index on _doc column
    schema
        .indexes
        .push(uni_common::core::schema::IndexDefinition::JsonFullText(
            JsonFtsIndexConfig {
                name: "article_fts".to_string(),
                label: "Article".to_string(),
                column: "_doc".to_string(),
                paths: vec![],
                with_positions: true,
            },
        ));

    schema
}

#[test]
fn test_parse_create_json_fts_index() {
    let query = "CREATE JSON FULLTEXT INDEX article_fts FOR (a:Article) ON _doc";
    let result = uni_query::parse_cypher(query);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_parse_create_json_fts_index_with_options() {
    let query = "CREATE JSON FULLTEXT INDEX article_fts FOR (a:Article) ON _doc OPTIONS {with_positions: true}";
    let result = uni_query::parse_cypher(query);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_parse_create_json_fts_index_if_not_exists() {
    let query = "CREATE JSON FULLTEXT INDEX article_fts IF NOT EXISTS FOR (a:Article) ON _doc";
    let result = uni_query::parse_cypher(query);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
}

#[test]
fn test_pushdown_contains_predicate_on_fts_column() {
    let schema = create_fts_test_schema();
    let analyzer = IndexAwareAnalyzer::new(&schema);

    // Create a CONTAINS predicate: n._doc CONTAINS 'graph'
    let expr = uni_cypher::ast::Expr::BinaryOp {
        left: Box::new(uni_cypher::ast::Expr::Property(
            Box::new(uni_cypher::ast::Expr::Variable("n".to_string())),
            "_doc".to_string(),
        )),
        op: uni_cypher::ast::BinaryOp::Contains,
        right: Box::new(uni_cypher::ast::Expr::Literal(
            uni_cypher::ast::CypherLiteral::String("graph".into()),
        )),
    };

    let strategy = analyzer.analyze(&expr, "n", 1);

    // Should be routed to JSON FTS predicates
    assert_eq!(
        strategy.json_fts_predicates.len(),
        1,
        "Expected 1 FTS predicate"
    );
    assert_eq!(strategy.json_fts_predicates[0].0, "_doc");
    assert_eq!(strategy.json_fts_predicates[0].1, "graph");
    assert!(strategy.json_fts_predicates[0].2.is_none()); // No path filter
}

#[test]
fn test_pushdown_contains_predicate_on_non_fts_column() {
    let schema = create_fts_test_schema();
    let analyzer = IndexAwareAnalyzer::new(&schema);

    // Create a CONTAINS predicate on non-indexed column: n.title CONTAINS 'graph'
    let expr = uni_cypher::ast::Expr::BinaryOp {
        left: Box::new(uni_cypher::ast::Expr::Property(
            Box::new(uni_cypher::ast::Expr::Variable("n".to_string())),
            "title".to_string(), // Not FTS-indexed
        )),
        op: uni_cypher::ast::BinaryOp::Contains,
        right: Box::new(uni_cypher::ast::Expr::Literal(
            uni_cypher::ast::CypherLiteral::String("graph".into()),
        )),
    };

    let strategy = analyzer.analyze(&expr, "n", 1);

    // Should NOT be routed to JSON FTS predicates (column not indexed)
    assert!(
        strategy.json_fts_predicates.is_empty(),
        "Non-indexed column should not use FTS"
    );
    // Should be in lance predicates (pushable)
    assert!(
        !strategy.lance_predicates.is_empty(),
        "Should be pushed to Lance"
    );
}

#[test]
fn test_pushdown_strategy_default() {
    let strategy = PushdownStrategy::default();
    assert!(strategy.uid_lookup.is_none());
    assert!(strategy.json_fts_predicates.is_empty());
    assert!(strategy.lance_predicates.is_empty());
    assert!(strategy.residual.is_empty());
}
