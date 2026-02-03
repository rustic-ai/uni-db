// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_expression_index() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. Create label
    db.execute("CREATE LABEL User (email STRING)").await?;

    // 2. Create Expression Index
    db.execute("CREATE INDEX lower_email FOR (u:User) ON (lower(u.email))")
        .await?;

    // 3. Verify schema metadata
    let schema = db.get_schema();
    let _idx = schema
        .indexes
        .iter()
        .find(|i| {
            if let uni_db::core::schema::IndexDefinition::Scalar(c) = i {
                c.name == "lower_email"
            } else {
                false
            }
        })
        .expect("Index not found");

    // Check if generated property was added
    let props = schema.properties.get("User").unwrap();
    // Parser stores the original expression (lowercase, no variable prefix)
    let original_expr = "lower(email)";
    // Column name generation normalizes function names to uppercase
    let gen_col = uni_db::core::schema::SchemaManager::generated_column_name(original_expr);
    if !props.contains_key(&gen_col) {
        println!("Available properties: {:?}", props.keys());
    }
    assert!(
        props.contains_key(&gen_col),
        "Generated column {} not found",
        gen_col
    );
    let meta = props.get(&gen_col).unwrap();
    assert_eq!(meta.generation_expression, Some(original_expr.to_string()));

    // 4. Insert Data (should compute generated column)
    db.execute("CREATE (:User {email: 'Alice@Example.com'})")
        .await?;

    // 5. Verify data (querying generated column directly)
    // Use ALIAS because default column name includes variable prefix (e.g. "u._gen...")
    let result = db
        .query(&format!("MATCH (u:User) RETURN u.{} AS val", gen_col))
        .await?;
    assert_eq!(result.len(), 1);
    // Should be lowercased
    assert_eq!(result.rows()[0].get::<String>("val")?, "alice@example.com");

    // 6. Verify query with expression predicate (Planner rewriting)
    // The planner should rewrite `lower(u.email)` to `u._gen_LOWER_u_email`
    // And since we have an index on it, it should be fast (though here we just check correctness)
    let result = db
        .query("MATCH (u:User) WHERE lower(u.email) = 'alice@example.com' RETURN u.email AS email")
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].get::<String>("email")?,
        "Alice@Example.com"
    );

    Ok(())
}
