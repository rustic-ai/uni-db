// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_common::core::schema::{LabelMeta, Schema, SchemaElementState};
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr};
use uni_query::query::pushdown::{IndexAwareAnalyzer, LanceFilterGenerator, PredicateAnalyzer};

#[test]
fn test_lance_filter_generation() {
    // n.name CONTAINS 'foo'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::Contains,
        right: Box::new(Expr::Literal(CypherLiteral::String("foo".to_string()))),
    };

    let filter = LanceFilterGenerator::generate(&[expr], "n", None).unwrap();
    assert_eq!(filter, "name LIKE '%foo%'");

    // n.title STARTS WITH 'Intro'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "title".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("Intro".to_string()))),
    };

    let filter = LanceFilterGenerator::generate(&[expr], "n", None).unwrap();
    assert_eq!(filter, "title LIKE 'Intro%'");
}

#[test]
fn test_or_to_in_conversion() {
    // Manually construct: n.status = 'a' OR n.status = 'b'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "status".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::String("a".to_string()))),
        }),
        op: BinaryOp::Or,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "status".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::String("b".to_string()))),
        }),
    };

    let analyzer = PredicateAnalyzer::new();
    let analysis = analyzer.analyze(&expr, "n");

    // Should be pushed as a single IN expression
    assert_eq!(analysis.pushable.len(), 1);
    assert!(analysis.residual.is_empty());

    let pushed = &analysis.pushable[0];
    if let Expr::In { list, .. } = pushed {
        if let Expr::List(items) = list.as_ref() {
            assert_eq!(items.len(), 2);
        } else {
            panic!("Expected list on RHS of IN");
        }
    } else {
        panic!("Expected IN expression, got {:?}", pushed);
    }

    // Verify SQL generation
    let sql = LanceFilterGenerator::generate(&analysis.pushable, "n", None).unwrap();
    // order might vary? No, vec insertion order.
    assert!(sql == "status IN ('a', 'b')" || sql == "status IN ('b', 'a')");
}

#[test]
fn test_is_null_pushdown() {
    let expr = Expr::IsNull(Box::new(Expr::Property(
        Box::new(Expr::Variable("n".to_string())),
        "email".to_string(),
    )));

    let analyzer = PredicateAnalyzer::new();
    let analysis = analyzer.analyze(&expr, "n");

    assert_eq!(analysis.pushable.len(), 1);
    assert!(analysis.residual.is_empty());

    let sql = LanceFilterGenerator::generate(&analysis.pushable, "n", None).unwrap();
    assert_eq!(sql, "email IS NULL");
}

#[test]
fn test_is_not_null_pushdown() {
    let expr = Expr::IsNotNull(Box::new(Expr::Property(
        Box::new(Expr::Variable("n".to_string())),
        "email".to_string(),
    )));

    let analyzer = PredicateAnalyzer::new();
    let analysis = analyzer.analyze(&expr, "n");

    assert_eq!(analysis.pushable.len(), 1);

    let sql = LanceFilterGenerator::generate(&analysis.pushable, "n", None).unwrap();
    assert_eq!(sql, "email IS NOT NULL");
}

#[test]
fn test_predicate_flattening() {
    // (a=1 AND b=2)
    // Analyzer splits conjuncts into vector
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "a".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::Integer(1))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "b".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::Integer(2))),
        }),
    };

    let analyzer = PredicateAnalyzer::new();
    let analysis = analyzer.analyze(&expr, "n");

    assert_eq!(analysis.pushable.len(), 2);

    let sql = LanceFilterGenerator::generate(&analysis.pushable, "n", None).unwrap();
    assert_eq!(sql, "a = 1 AND b = 2");
}

// =====================================================================
// IndexAwareAnalyzer Tests
// =====================================================================

fn create_test_schema_with_label(label: &str, label_id: u16) -> Schema {
    let mut schema = Schema::default();
    schema.labels.insert(
        label.to_string(),
        LabelMeta {
            id: label_id,
            created_at: chrono::Utc::now(),
            state: SchemaElementState::Active,
            description: None,
        },
    );
    schema
}

#[test]
fn test_index_aware_uid_extraction() {
    // Test that _uid = 'valid_base32' is recognized
    // Note: We can't test actual UID lookup without a valid Base32Lower multibase string
    // but we can verify the pattern detection

    let schema = create_test_schema_with_label("Person", 1);

    // Create a predicate: n._uid = 'invalid_format'
    // This should NOT be extracted (invalid UID format)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "_uid".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::String(
            "not-a-valid-uid".to_string(),
        ))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Invalid UID should not be extracted
    assert!(strategy.uid_lookup.is_none());
    // Should become residual since _uid column doesn't exist in Lance
    assert!(!strategy.residual.is_empty() || !strategy.lance_predicates.is_empty());
}

#[test]
fn test_index_aware_jsonpath_extraction() {
    let schema = create_test_schema_with_label("Doc", 2);

    // Create predicate: n.title = 'Hello'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "title".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::String("Hello".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 2);

    // Without json_indexes, title should go to lance_predicates
    assert!(strategy.json_fts_predicates.is_empty());
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_index_aware_non_indexed_property() {
    let schema = create_test_schema_with_label("Doc", 2);

    // Create predicate: n.author = 'John' (not indexed)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "author".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::String("John".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 2);

    // Should NOT be extracted as jsonpath lookup (no index)
    assert!(strategy.json_fts_predicates.is_empty());

    // Should go to lance_predicates
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_index_aware_combined_predicates() {
    let schema = create_test_schema_with_label("Doc", 2);

    // Create predicate: n.title = 'Hello' AND n.author = 'John'
    // Without json_indexes, both go to lance_predicates
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "title".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::String("Hello".to_string()))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "author".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::String("John".to_string()))),
        }),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 2);

    // Without json_indexes, both should go to lance_predicates
    assert!(strategy.json_fts_predicates.is_empty());
    assert_eq!(strategy.lance_predicates.len(), 2);
}

// =====================================================================
// BTree STARTS WITH Pushdown Tests
// =====================================================================

use uni_common::core::schema::{IndexDefinition, IndexStatus, ScalarIndexConfig, ScalarIndexType};

fn create_test_schema_with_btree_index(label: &str, label_id: u16, index_property: &str) -> Schema {
    let mut schema = Schema::default();
    schema.labels.insert(
        label.to_string(),
        LabelMeta {
            id: label_id,
            created_at: chrono::Utc::now(),
            state: SchemaElementState::Active,
            description: None,
        },
    );
    schema
        .indexes
        .push(IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, index_property),
            label: label.to_string(),
            properties: vec![index_property.to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: Default::default(),
        }));
    schema
}

#[test]
fn test_btree_starts_with_extraction() {
    let schema = create_test_schema_with_btree_index("Person", 1, "name");

    // Create predicate: n.name STARTS WITH 'John'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("John".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Should be extracted as BTree prefix scan
    assert_eq!(strategy.btree_prefix_scans.len(), 1);
    assert_eq!(strategy.btree_prefix_scans[0].0, "name");
    assert_eq!(strategy.btree_prefix_scans[0].1, "John");
    assert_eq!(strategy.btree_prefix_scans[0].2, "Joho"); // 'n' + 1 = 'o'

    // Should NOT be in lance_predicates (routed to BTree scan instead)
    assert!(strategy.lance_predicates.is_empty());
}

#[test]
fn test_btree_starts_with_non_indexed_property() {
    let schema = create_test_schema_with_btree_index("Person", 1, "name");

    // Create predicate: n.email STARTS WITH 'john@' (not indexed)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "email".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("john@".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Should NOT be extracted as BTree prefix scan (no index on email)
    assert!(strategy.btree_prefix_scans.is_empty());

    // Should go to lance_predicates as LIKE predicate
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_btree_starts_with_empty_prefix() {
    let schema = create_test_schema_with_btree_index("Person", 1, "name");

    // Create predicate: n.name STARTS WITH '' (empty prefix - matches all)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Empty prefix should NOT be optimized via BTree (matches all, no benefit)
    assert!(strategy.btree_prefix_scans.is_empty());

    // Should go to lance_predicates
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_btree_starts_with_hash_index_not_used() {
    let mut schema = create_test_schema_with_label("Person", 1);
    // Add a Hash index instead of BTree
    schema
        .indexes
        .push(IndexDefinition::Scalar(ScalarIndexConfig {
            name: "idx_person_name".to_string(),
            label: "Person".to_string(),
            properties: vec!["name".to_string()],
            index_type: ScalarIndexType::Hash, // Not BTree
            where_clause: None,
            metadata: Default::default(),
        }));

    // Create predicate: n.name STARTS WITH 'John'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("John".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Hash index should NOT be used for STARTS WITH
    assert!(strategy.btree_prefix_scans.is_empty());

    // Should go to lance_predicates as LIKE predicate
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_btree_prefix_increment_logic() {
    // Test the increment_last_char helper via BTree extraction
    let schema = create_test_schema_with_btree_index("Person", 1, "name");

    // Test various prefixes and verify upper bounds
    let test_cases = vec![
        ("A", "B"),       // Simple single char
        ("Z", "["),       // Z + 1 = [
        ("abc", "abd"),   // c + 1 = d
        ("test", "tesu"), // t + 1 = u
        ("123", "124"),   // Numeric strings
    ];

    for (prefix, expected_upper) in test_cases {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "name".to_string(),
            )),
            op: BinaryOp::StartsWith,
            right: Box::new(Expr::Literal(CypherLiteral::String(prefix.to_string()))),
        };

        let analyzer = IndexAwareAnalyzer::new(&schema);
        let strategy = analyzer.analyze(&expr, "n", 1);

        assert_eq!(
            strategy.btree_prefix_scans.len(),
            1,
            "Failed for prefix: {}",
            prefix
        );
        assert_eq!(strategy.btree_prefix_scans[0].1, prefix);
        assert_eq!(
            strategy.btree_prefix_scans[0].2, expected_upper,
            "Upper bound mismatch for prefix: {}",
            prefix
        );
    }
}

#[test]
fn test_btree_starts_with_special_characters() {
    let schema = create_test_schema_with_btree_index("Person", 1, "name");

    // Prefix with single quote (should be escaped in SQL)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("O'Brien".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Should still be extracted
    assert_eq!(strategy.btree_prefix_scans.len(), 1);
    assert_eq!(strategy.btree_prefix_scans[0].1, "O'Brien");
}

#[test]
fn test_btree_starts_with_unicode() {
    let schema = create_test_schema_with_btree_index("Person", 1, "name");

    // Unicode prefix
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("日本".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Should be extracted (Unicode char increment works)
    assert_eq!(strategy.btree_prefix_scans.len(), 1);
    assert_eq!(strategy.btree_prefix_scans[0].1, "日本");
}

#[test]
fn test_btree_starts_with_multiple_indexed_properties() {
    let mut schema = create_test_schema_with_label("Person", 1);
    // Index covers multiple properties
    schema
        .indexes
        .push(IndexDefinition::Scalar(ScalarIndexConfig {
            name: "idx_person_name_email".to_string(),
            label: "Person".to_string(),
            properties: vec!["name".to_string(), "email".to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: Default::default(),
        }));

    // Test name (indexed)
    let expr1 = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("John".to_string()))),
    };

    // Test email (also indexed)
    let expr2 = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "email".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("john@".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);

    let strategy1 = analyzer.analyze(&expr1, "n", 1);
    assert_eq!(strategy1.btree_prefix_scans.len(), 1);

    let strategy2 = analyzer.analyze(&expr2, "n", 1);
    assert_eq!(strategy2.btree_prefix_scans.len(), 1);
}

#[test]
fn test_btree_prefix_scan_skips_non_online_index() {
    // Start with a normal BTree-indexed schema, then set the index to Building
    let mut schema = create_test_schema_with_btree_index("Person", 1, "name");
    if let IndexDefinition::Scalar(cfg) = &mut schema.indexes[0] {
        cfg.metadata.status = IndexStatus::Building;
    }

    // Create predicate: n.name STARTS WITH 'John'
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("John".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Building index should NOT be used for BTree prefix scan
    assert!(strategy.btree_prefix_scans.is_empty());
    // Should fall through to lance_predicates
    assert_eq!(strategy.lance_predicates.len(), 1);

    // Now set to Online — should be used
    if let IndexDefinition::Scalar(cfg) = &mut schema.indexes[0] {
        cfg.metadata.status = IndexStatus::Online;
    }
    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);
    assert_eq!(strategy.btree_prefix_scans.len(), 1);
    assert_eq!(strategy.btree_prefix_scans[0].0, "name");
    assert!(strategy.lance_predicates.is_empty());
}

// =====================================================================
// Scalar Equality Lookup Tests (Issue #57 — Hash index support)
// =====================================================================

fn create_test_schema_with_hash_index(label: &str, label_id: u16, index_property: &str) -> Schema {
    let mut schema = create_test_schema_with_label(label, label_id);
    schema
        .indexes
        .push(IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, index_property),
            label: label.to_string(),
            properties: vec![index_property.to_string()],
            index_type: ScalarIndexType::Hash,
            where_clause: None,
            metadata: Default::default(),
        }));
    schema
}

#[test]
fn test_hash_index_equality_lookup_string_literal() {
    let schema = create_test_schema_with_hash_index("Entity", 1, "entity_id");

    // MATCH (n:Entity {entity_id: "abc"})  →  n.entity_id = "abc"
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "entity_id".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::String("abc".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Hash index SHOULD be used for equality lookup
    assert_eq!(strategy.scalar_equality_lookups.len(), 1);
    assert_eq!(strategy.scalar_equality_lookups[0].0, "entity_id");

    // Should NOT fall through to lance
    assert!(strategy.lance_predicates.is_empty());
}

#[test]
fn test_hash_index_equality_lookup_parameter() {
    let schema = create_test_schema_with_hash_index("Entity", 1, "entity_id");

    // MATCH (n:Entity) WHERE n.entity_id = $eid
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "entity_id".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Parameter("eid".to_string())),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    assert_eq!(strategy.scalar_equality_lookups.len(), 1);
    assert_eq!(strategy.scalar_equality_lookups[0].0, "entity_id");
    assert!(strategy.lance_predicates.is_empty());
}

#[test]
fn test_btree_index_also_used_for_equality_lookup() {
    // BTree indexes also support equality lookups (O(log N))
    let schema = create_test_schema_with_btree_index("Person", 1, "name");

    // n.name = "John"
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::String("John".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    assert_eq!(strategy.scalar_equality_lookups.len(), 1);
    assert_eq!(strategy.scalar_equality_lookups[0].0, "name");
    assert!(strategy.lance_predicates.is_empty());
}

#[test]
fn test_scalar_equality_reversed_operand_order() {
    let schema = create_test_schema_with_hash_index("Entity", 1, "entity_id");

    // "abc" = n.entity_id  (reversed order — should still match)
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(CypherLiteral::String("abc".to_string()))),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "entity_id".to_string(),
        )),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    assert_eq!(strategy.scalar_equality_lookups.len(), 1);
    assert_eq!(strategy.scalar_equality_lookups[0].0, "entity_id");
}

#[test]
fn test_scalar_equality_non_indexed_property_not_extracted() {
    let schema = create_test_schema_with_hash_index("Entity", 1, "entity_id");

    // n.name = "John" — name has NO index, should fall through to lance
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::String("John".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    assert!(strategy.scalar_equality_lookups.is_empty());
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_hash_index_not_used_for_starts_with() {
    // Hash index should NOT be used for STARTS WITH (only BTree supports range scans)
    let schema = create_test_schema_with_hash_index("Entity", 1, "entity_id");

    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "entity_id".to_string(),
        )),
        op: BinaryOp::StartsWith,
        right: Box::new(Expr::Literal(CypherLiteral::String("abc".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // STARTS WITH should NOT use hash index for prefix scan
    assert!(strategy.btree_prefix_scans.is_empty());
    // Should NOT use scalar equality either (different op)
    assert!(strategy.scalar_equality_lookups.is_empty());
    // Falls through to lance
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_scalar_equality_skips_building_index() {
    let mut schema = create_test_schema_with_hash_index("Entity", 1, "entity_id");
    // Set index status to Building
    if let IndexDefinition::Scalar(cfg) = &mut schema.indexes[0] {
        cfg.metadata.status = IndexStatus::Building;
    }

    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "entity_id".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::String("abc".to_string()))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // Building index should NOT be used
    assert!(strategy.scalar_equality_lookups.is_empty());
    // Falls through to lance
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_scalar_equality_combined_with_other_predicates() {
    let schema = create_test_schema_with_hash_index("Entity", 1, "entity_id");

    // n.entity_id = "abc" AND n.status = "active"
    // entity_id is hash-indexed, status is not
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "entity_id".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::String("abc".to_string()))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "status".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::String("active".to_string()))),
        }),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // entity_id goes to scalar_equality_lookups
    assert_eq!(strategy.scalar_equality_lookups.len(), 1);
    assert_eq!(strategy.scalar_equality_lookups[0].0, "entity_id");

    // status goes to lance_predicates (no index)
    assert_eq!(strategy.lance_predicates.len(), 1);
}

#[test]
fn test_scalar_equality_integer_literal() {
    let schema = create_test_schema_with_hash_index("Person", 1, "age");

    // n.age = 30
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "age".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::Integer(30))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    assert_eq!(strategy.scalar_equality_lookups.len(), 1);
    assert_eq!(strategy.scalar_equality_lookups[0].0, "age");
}

#[test]
fn test_scalar_equality_system_column_not_extracted() {
    // _vid, _uid etc. should NOT go through scalar_equality_lookups
    // They have their own dedicated paths (UID lookup, VID short-circuit)
    let schema = create_test_schema_with_hash_index("Entity", 1, "_vid");

    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "_vid".to_string(),
        )),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(CypherLiteral::Integer(42))),
    };

    let analyzer = IndexAwareAnalyzer::new(&schema);
    let strategy = analyzer.analyze(&expr, "n", 1);

    // System columns should NOT be extracted via scalar equality
    assert!(strategy.scalar_equality_lookups.is_empty());
}
