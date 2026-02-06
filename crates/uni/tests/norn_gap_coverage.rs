// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_common::core::schema::{DataType, InvertedIndexConfig};
use uni_db::Uni;
use uni_db::api::schema::IndexType;

#[tokio::test]
async fn test_vector_match_operator_coverage() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Schema with Vector Index
    db.schema()
        .label("Item")
        .property("id", DataType::Int64)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .index(
            "embedding",
            IndexType::Vector(uni_db::api::schema::VectorIndexCfg {
                algorithm: uni_db::api::schema::VectorAlgo::Flat,
                metric: uni_db::api::schema::VectorMetric::L2,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    // 2. Insert Data
    db.execute("CREATE (i:Item {id: 1, embedding: [0.0, 0.0]})")
        .await?;
    db.execute("CREATE (i:Item {id: 2, embedding: [1.0, 1.0]})")
        .await?;

    // Flush to ensure data is visible to vector index (which uses Lance)
    db.flush().await?;

    // 3. Query using ~= operator
    // Target [0.1, 0.1] should match id 1
    let query_vec = vec![0.1, 0.1];
    let results = db
        .query_with(
            "
        MATCH (i:Item)
        WHERE i.embedding ~= $q
        RETURN i.id
        LIMIT 1
    ",
        )
        .param("q", query_vec)
        .fetch_all()
        .await?;

    assert_eq!(results.rows.len(), 1);
    assert_eq!(results.rows[0].get::<i64>("i.id")?, 1);

    Ok(())
}

#[tokio::test]
async fn test_merge_composite_key_coverage() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Schema with Composite Unique Constraint
    // We must use procedures/DDL or manually add constraint as SchemaBuilder doesn't support composite constraints directly yet in high-level API?
    // Let's use the DDL procedure which we know works.
    db.query(
        r#"
        CALL uni.schema.createLabel('User', {
            "properties": {
                "org": { "type": "STRING" },
                "username": { "type": "STRING" }
            },
            "constraints": [
                { "type": "UNIQUE", "properties": ["org", "username"] }
            ]
        })
    "#,
    )
    .await?;

    // Also create the index to back it up (DDL might do it? `createLabel` does not implied create index for constraint yet in all paths, let's ensure it)
    // The Executor::execute_merge optimization checks for constraint AND existence of index via `composite_lookup`.
    // `composite_lookup` requires an index? No, it uses `scan().filter(...)` on the dataset.
    // Wait, `IndexManager::composite_lookup` uses `scan().filter(...)`. It doesn't strictly require a *composite index structure*, just the dataset.
    // So explicit index creation might not be strictly required for correctness, but good for perf.
    // Let's create it to be sure.
    db.execute("CREATE INDEX idx_user_comp FOR (u:User) ON (u.org, u.username)")
        .await?;

    // 2. Merge First Time (Create)
    let res1 = db
        .execute("MERGE (u:User {org: 'Acme', username: 'alice'}) RETURN u")
        .await?;
    assert_eq!(res1.affected_rows, 1);

    db.flush().await?;

    // 3. Merge Second Time (Match)
    let _res2 = db
        .execute("MERGE (u:User {org: 'Acme', username: 'alice'}) RETURN u")
        .await?;

    // Verify count is still 1 (deduplicated)
    let count = db.query("MATCH (u:User) RETURN count(u) AS c").await?;
    assert_eq!(count.rows[0].get::<i64>("c")?, 1);

    Ok(())
}

#[tokio::test]
async fn test_inverted_index_edge_cases() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Schema
    let inv_config = InvertedIndexConfig {
        name: "idx_tags".to_string(),
        label: "Post".to_string(),
        property: "tags".to_string(),
        normalize: true,
        max_terms_per_doc: 100,
    };

    db.schema()
        .label("Post")
        .property("id", DataType::Int64)
        .property("tags", DataType::List(Box::new(DataType::String)))
        .index("tags", IndexType::Inverted(inv_config))
        .apply()
        .await?;

    // 2. Insert Edge Cases
    db.execute("CREATE (p:Post {id: 1, tags: []})").await?; // Empty list
    db.execute("CREATE (p:Post {id: 2, tags: ['a']})").await?; // Normal
    // Null list handled by nullable property? Default is not nullable in builder unless property_nullable called.
    // Let's test empty list behavior.

    db.flush().await?;

    // 3. Query
    // ANY IN empty list -> should match nothing
    let res = db
        .query("MATCH (p:Post) WHERE ANY(t IN p.tags WHERE t IN ['a']) RETURN p.id")
        .await?;
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0].get::<i64>("p.id")?, 2);

    // Query for something that shouldn't match empty
    let res_empty = db
        .query("MATCH (p:Post) WHERE ANY(t IN p.tags WHERE t IN ['b']) RETURN p.id")
        .await?;
    assert_eq!(res_empty.rows.len(), 0);

    Ok(())
}
