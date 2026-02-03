// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_ddl_procedures() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create Label
    db.query(
        r#"
        CALL uni.schema.createLabel('Product', {
            "properties": {
                "name": { "type": "STRING" },
                "price": { "type": "FLOAT" },
                "tags": { "type": "STRING", "nullable": true }
            },
            "indexes": [
                { "property": "name", "type": "BTREE" }
            ],
            "constraints": [
                { "type": "UNIQUE", "properties": ["name"] }
            ]
        })
    "#,
    )
    .await?;

    assert!(db.label_exists("Product").await?);

    let info = db
        .get_label_info("Product")
        .await?
        .expect("Label should exist");
    assert!(info.properties.iter().any(|p| p.name == "name"));
    assert!(info.properties.iter().any(|p| p.name == "price"));
    assert!(
        info.indexes
            .iter()
            .any(|i| i.properties.contains(&"name".to_string()))
    );
    assert!(
        info.constraints
            .iter()
            .any(|c| c.constraint_type == "UNIQUE" && c.properties.contains(&"name".to_string()))
    );

    // 2. Create Edge Type
    db.query(
        r#"
        CALL uni.schema.createEdgeType('RELATED_TO', ['Product'], ['Product'], {
            "properties": {
                "weight": { "type": "FLOAT" }
            }
        })
    "#,
    )
    .await?;
    assert!(db.edge_type_exists("RELATED_TO").await?);

    // 3. Create Index separately
    db.query(
        r#"
        CALL uni.schema.createIndex('Product', 'price', {
            "type": "BTREE",
            "name": "idx_price"
        })
    "#,
    )
    .await?;

    let info_updated = db.get_label_info("Product").await?.unwrap();
    assert!(info_updated.indexes.iter().any(|i| i.name == "idx_price"));

    // 4. Create Constraint separately
    db.query(
        r#"
        CALL uni.schema.createConstraint('Product', 'EXISTS', ['price'])
    "#,
    )
    .await?;
    let info_updated_2 = db.get_label_info("Product").await?.unwrap();
    assert!(
        info_updated_2
            .constraints
            .iter()
            .any(|c| c.constraint_type == "EXISTS" && c.properties.contains(&"price".to_string()))
    );

    // 5. Drop Index
    db.query("CALL uni.schema.dropIndex('idx_price')").await?;
    let info_dropped_idx = db.get_label_info("Product").await?.unwrap();
    assert!(
        !info_dropped_idx
            .indexes
            .iter()
            .any(|i| i.name == "idx_price")
    );

    // 6. Drop Label
    db.query("CALL uni.schema.dropLabel('Product')").await?;
    assert!(!db.label_exists("Product").await?);

    Ok(())
}

#[tokio::test]
async fn test_ddl_validation() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Invalid identifier
    let res = db
        .query("CALL uni.schema.createLabel('Invalid-Name!', {})")
        .await;
    assert!(res.is_err());
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("must contain only alphanumeric and underscore")
            || err_str.contains("Invalid identifier")
    );

    // Reserved word
    let res = db.query("CALL uni.schema.createLabel('MATCH', {})").await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("reserved word"));

    // Invalid Data Type
    let res = db
        .query(
            r#"
        CALL uni.schema.createLabel('ValidName', {
            "properties": { "bad_prop": { "type": "UNKNOWN_TYPE" } }
        })
    "#,
        )
        .await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("Unknown data type"));

    Ok(())
}
