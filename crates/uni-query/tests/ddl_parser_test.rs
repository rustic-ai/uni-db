// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_cypher::ast::*;

fn parse_schema(input: &str) -> SchemaCommand {
    let query = uni_query::parse_cypher(input).unwrap();

    let Query::Schema(cmd) = query else {
        panic!("Expected schema command");
    };
    *cmd
}

#[test]
fn test_create_label() {
    let input = "CREATE LABEL Person (name STRING NOT NULL, age INT)";
    let cmd = parse_schema(input);

    let SchemaCommand::CreateLabel(c) = cmd else {
        panic!("Expected CreateLabel");
    };

    assert_eq!(c.name, "Person");
    assert_eq!(c.properties.len(), 2);

    assert_eq!(c.properties[0].name, "name");
    assert_eq!(c.properties[0].data_type, "STRING");
    assert!(!c.properties[0].nullable);

    assert_eq!(c.properties[1].name, "age");
    assert_eq!(c.properties[1].data_type, "INT");
    assert!(c.properties[1].nullable); // Default is nullable
}

#[test]
fn test_create_edge_type() {
    let input = "CREATE EDGE TYPE FRIENDS (since INT) FROM Person TO Person";
    let cmd = parse_schema(input);

    let SchemaCommand::CreateEdgeType(c) = cmd else {
        panic!("Expected CreateEdgeType");
    };

    assert_eq!(c.name, "FRIENDS");
    assert_eq!(c.src_labels, vec!["Person"]);
    assert_eq!(c.dst_labels, vec!["Person"]);
    assert_eq!(c.properties.len(), 1);
    assert_eq!(c.properties[0].name, "since");
    assert_eq!(c.properties[0].data_type, "INT");
}

#[test]
fn test_create_constraint_unique() {
    let input = "CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE";
    let cmd = parse_schema(input);

    let SchemaCommand::CreateConstraint(c) = cmd else {
        panic!("Expected CreateConstraint");
    };

    assert_eq!(c.label, "Person");
    assert!(matches!(c.constraint_type, ConstraintType::Unique));
    assert_eq!(c.properties, vec!["email"]);
}

#[test]
fn test_create_constraint_exists() {
    let input = "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS(n.name)";
    let cmd = parse_schema(input);

    let SchemaCommand::CreateConstraint(c) = cmd else {
        panic!("Expected CreateConstraint");
    };

    assert_eq!(c.label, "Person");
    assert!(matches!(c.constraint_type, ConstraintType::Exists));
    assert_eq!(c.properties, vec!["name"]);
}

#[test]
fn test_alter_label() {
    let input = "ALTER LABEL Person ADD PROPERTY email STRING";
    let cmd = parse_schema(input);

    let SchemaCommand::AlterLabel(c) = cmd else {
        panic!("Expected AlterLabel");
    };

    assert_eq!(c.name, "Person");
    let AlterAction::AddProperty(prop) = &c.action else {
        panic!("Expected AddProperty action");
    };
    assert_eq!(prop.name, "email");
    assert_eq!(prop.data_type, "STRING");
}
