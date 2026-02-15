// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for overflow_json column functionality in vertex and edge tables.

use anyhow::Result;
use arrow_array::{Array, LargeBinaryArray};
use chrono::Utc;
use std::collections::HashMap;
use uni_common::core::id::Vid;
use uni_common::core::schema::{DataType, LabelMeta, PropertyMeta, Schema, SchemaElementState};
use uni_store::storage::vertex::VertexDataset;

/// Helper to decode CypherValue binary to JSON string.
fn decode_jsonb(bytes: &[u8]) -> Result<String> {
    let uni_val = uni_common::cypher_value_codec::decode(bytes)?;
    let json_val: serde_json::Value = uni_val.into();
    Ok(json_val.to_string())
}

/// Helper to create a test schema with partial properties.
fn create_test_schema_with_partial_props() -> Schema {
    let mut schema = Schema::default();

    // Add Person label
    schema.labels.insert(
        "Person".to_string(),
        LabelMeta {
            id: 1,
            created_at: Utc::now(),
            state: SchemaElementState::Active,
        },
    );

    // Add properties for Person label
    let mut person_props = HashMap::new();
    person_props.insert(
        "name".to_string(),
        PropertyMeta {
            r#type: DataType::String,
            nullable: false,
            added_in: 1,
            state: SchemaElementState::Active,
            generation_expression: None,
        },
    );
    person_props.insert(
        "age".to_string(),
        PropertyMeta {
            r#type: DataType::Int,
            nullable: true,
            added_in: 1,
            state: SchemaElementState::Active,
            generation_expression: None,
        },
    );
    schema.properties.insert("Person".to_string(), person_props);

    schema
}

#[test]
fn test_vertex_schema_includes_overflow_json() -> Result<()> {
    let schema = create_test_schema_with_partial_props();
    let vertex_dataset = VertexDataset::new("test_uri", "Person", 1);
    let arrow_schema = vertex_dataset.get_arrow_schema(&schema)?;

    // Verify overflow_json column exists
    assert!(
        arrow_schema.field_with_name("overflow_json").is_ok(),
        "overflow_json column should exist in vertex schema"
    );

    let field = arrow_schema.field_with_name("overflow_json")?;
    assert_eq!(
        field.data_type(),
        &arrow_schema::DataType::LargeBinary,
        "overflow_json should be LargeBinary type (JSONB binary format)"
    );
    assert!(field.is_nullable(), "overflow_json should be nullable");

    Ok(())
}

#[test]
fn test_build_overflow_json_column_filters_schema_props() -> Result<()> {
    let schema = create_test_schema_with_partial_props();
    let dataset = VertexDataset::new("test_uri", "Person", 1);

    let mut props1 = HashMap::new();
    props1.insert(
        "name".to_string(),
        uni_common::Value::String("Alice".to_string()),
    );
    props1.insert("age".to_string(), uni_common::Value::Int(30));
    props1.insert(
        "city".to_string(),
        uni_common::Value::String("NYC".to_string()),
    ); // overflow!
    props1.insert(
        "phone".to_string(),
        uni_common::Value::String("555-1234".to_string()),
    ); // overflow!

    let mut props2 = HashMap::new();
    props2.insert(
        "name".to_string(),
        uni_common::Value::String("Bob".to_string()),
    );
    props2.insert("age".to_string(), uni_common::Value::Int(25));
    // No overflow for Bob

    let vertices = vec![
        (Vid::new(1), vec!["Person".to_string()], props1),
        (Vid::new(2), vec!["Person".to_string()], props2),
    ];

    let deleted = vec![false, false];
    let versions = vec![1, 1];

    let batch = dataset.build_record_batch(&vertices, &deleted, &versions, &schema)?;

    // Check overflow_json column
    let overflow_col = batch
        .column_by_name("overflow_json")
        .expect("overflow_json column should exist");
    let binary_array = overflow_col
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("overflow_json should be LargeBinaryArray (JSONB binary)");

    // Alice should have overflow properties
    assert!(!binary_array.is_null(0), "Alice should have overflow data");
    let alice_jsonb_bytes = binary_array.value(0);
    let alice_json = decode_jsonb(alice_jsonb_bytes).expect("Should decode JSONB");
    let alice_overflow: HashMap<String, serde_json::Value> =
        serde_json::from_str(&alice_json).expect("Should parse JSON");
    assert_eq!(
        alice_overflow.len(),
        2,
        "Alice should have 2 overflow props"
    );
    assert_eq!(
        alice_overflow.get("city").and_then(|v| v.as_str()),
        Some("NYC")
    );
    assert_eq!(
        alice_overflow.get("phone").and_then(|v| v.as_str()),
        Some("555-1234")
    );
    // Schema properties should NOT be in overflow
    assert!(!alice_overflow.contains_key("name"));
    assert!(!alice_overflow.contains_key("age"));

    // Bob should have null overflow (no overflow properties)
    assert!(
        binary_array.is_null(1),
        "Bob should have null overflow_json"
    );

    Ok(())
}

#[test]
fn test_build_overflow_json_excludes_ext_id() -> Result<()> {
    let schema = create_test_schema_with_partial_props();
    let dataset = VertexDataset::new("test_uri", "Person", 1);

    let mut props = HashMap::new();
    props.insert(
        "name".to_string(),
        uni_common::Value::String("Charlie".to_string()),
    );
    props.insert(
        "ext_id".to_string(),
        uni_common::Value::String("charlie-123".to_string()),
    ); // system column
    props.insert(
        "custom_field".to_string(),
        uni_common::Value::String("value".to_string()),
    ); // overflow

    let vertices = vec![(Vid::new(1), vec!["Person".to_string()], props)];
    let deleted = vec![false];
    let versions = vec![1];

    let batch = dataset.build_record_batch(&vertices, &deleted, &versions, &schema)?;

    let overflow_col = batch.column_by_name("overflow_json").unwrap();
    let binary_array = overflow_col
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();

    let jsonb_bytes = binary_array.value(0);
    let overflow_json = decode_jsonb(jsonb_bytes)?;
    let overflow: HashMap<String, serde_json::Value> = serde_json::from_str(&overflow_json)?;

    // ext_id should NOT be in overflow (it's a system column)
    assert!(!overflow.contains_key("ext_id"));
    // custom_field should be in overflow
    assert_eq!(
        overflow.get("custom_field").and_then(|v| v.as_str()),
        Some("value")
    );

    Ok(())
}

#[test]
fn test_empty_overflow_properties() -> Result<()> {
    let schema = create_test_schema_with_partial_props();
    let dataset = VertexDataset::new("test_uri", "Person", 1);

    // Create vertex with only schema-defined properties
    let mut props = HashMap::new();
    props.insert(
        "name".to_string(),
        uni_common::Value::String("Dave".to_string()),
    );
    props.insert("age".to_string(), uni_common::Value::Int(40));

    let vertices = vec![(Vid::new(1), vec!["Person".to_string()], props)];
    let deleted = vec![false];
    let versions = vec![1];

    let batch = dataset.build_record_batch(&vertices, &deleted, &versions, &schema)?;

    let overflow_col = batch.column_by_name("overflow_json").unwrap();
    let binary_array = overflow_col
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();

    // Should be null when no overflow properties
    assert!(binary_array.is_null(0));

    Ok(())
}

#[cfg(test)]
mod delta_tests {
    use super::*;
    use uni_common::core::id::Eid;
    use uni_common::core::schema::EdgeTypeMeta;
    use uni_store::storage::delta::{DeltaDataset, L1Entry, Op};

    fn create_test_edge_schema() -> Schema {
        let mut schema = Schema::default();

        // Add Person label (required for edge type)
        schema.labels.insert(
            "Person".to_string(),
            LabelMeta {
                id: 1,
                created_at: Utc::now(),
                state: SchemaElementState::Active,
            },
        );

        // Add KNOWS edge type
        schema.edge_types.insert(
            "KNOWS".to_string(),
            EdgeTypeMeta {
                id: 1,
                src_labels: vec!["Person".to_string()],
                dst_labels: vec!["Person".to_string()],
                state: SchemaElementState::Active,
            },
        );

        // Add property for KNOWS edge type
        let mut knows_props = HashMap::new();
        knows_props.insert(
            "since".to_string(),
            PropertyMeta {
                r#type: DataType::Int,
                nullable: true,
                added_in: 1,
                state: SchemaElementState::Active,
                generation_expression: None,
            },
        );
        schema.properties.insert("KNOWS".to_string(), knows_props);

        schema
    }

    #[test]
    fn test_delta_schema_includes_overflow_json() -> Result<()> {
        let schema = create_test_edge_schema();
        let delta_dataset = DeltaDataset::new("test_uri", "KNOWS", "fwd");
        let arrow_schema = delta_dataset.get_arrow_schema(&schema)?;

        // Verify overflow_json column exists
        assert!(arrow_schema.field_with_name("overflow_json").is_ok());

        let field = arrow_schema.field_with_name("overflow_json")?;
        assert_eq!(
            field.data_type(),
            &arrow_schema::DataType::LargeBinary,
            "overflow_json should be LargeBinary type (JSONB binary format)"
        );
        assert!(field.is_nullable());

        Ok(())
    }

    #[test]
    fn test_delta_build_overflow_json() -> Result<()> {
        let schema = create_test_edge_schema();
        let delta_dataset = DeltaDataset::new("test_uri", "KNOWS", "fwd");

        let mut props1 = HashMap::new();
        props1.insert("since".to_string(), uni_common::Value::Int(2020)); // in schema
        props1.insert("strength".to_string(), uni_common::Value::Float(0.8)); // overflow!

        let mut props2 = HashMap::new();
        props2.insert("since".to_string(), uni_common::Value::Int(2021)); // in schema only

        let entries = vec![
            L1Entry {
                src_vid: Vid::new(1),
                dst_vid: Vid::new(2),
                eid: Eid::new(1),
                op: Op::Insert,
                version: 1,
                properties: props1,
                created_at: Some(0),
                updated_at: Some(0),
            },
            L1Entry {
                src_vid: Vid::new(2),
                dst_vid: Vid::new(3),
                eid: Eid::new(2),
                op: Op::Insert,
                version: 1,
                properties: props2,
                created_at: Some(0),
                updated_at: Some(0),
            },
        ];

        let batch = delta_dataset.build_record_batch(&entries, &schema)?;

        let overflow_col = batch.column_by_name("overflow_json").unwrap();
        let binary_array = overflow_col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();

        // First entry should have overflow
        assert!(!binary_array.is_null(0));
        let jsonb_bytes = binary_array.value(0);
        let overflow1_json = decode_jsonb(jsonb_bytes)?;
        let overflow1: HashMap<String, serde_json::Value> = serde_json::from_str(&overflow1_json)?;
        assert_eq!(
            overflow1.get("strength").and_then(|v| v.as_f64()),
            Some(0.8)
        );
        assert!(!overflow1.contains_key("since")); // Schema prop not in overflow

        // Second entry should have null overflow
        assert!(binary_array.is_null(1));

        Ok(())
    }
}
