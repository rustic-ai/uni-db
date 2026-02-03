// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_common::core::schema::{Schema, SchemaElementState};

#[test]
fn test_minimal_schema_deserialization() -> Result<()> {
    let json = r#"{
        "schema_version": 1,
        "labels": {
            "Person": { "id": 1 }
        },
        "edge_types": {
            "KNOWS": { "id": 1, "src_labels": ["Person"], "dst_labels": ["Person"] }
        },
        "properties": {
            "Person": {
                "name": { "type": "String", "nullable": false }
            }
        }
    }"#;

    let schema: Schema = serde_json::from_str(json)?;

    // Check defaults
    let label = schema.labels.get("Person").unwrap();
    assert_eq!(label.id, 1);
    assert_eq!(label.state, SchemaElementState::Active);

    let edge = schema.edge_types.get("KNOWS").unwrap();
    assert_eq!(edge.id, 1);
    assert_eq!(edge.state, SchemaElementState::Active);

    let prop = schema
        .properties
        .get("Person")
        .unwrap()
        .get("name")
        .unwrap();
    assert_eq!(prop.added_in, 1);
    assert_eq!(prop.state, SchemaElementState::Active);

    Ok(())
}
