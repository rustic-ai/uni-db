// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
#[ignore = "Query streaming/cursor functionality not yet implemented"]
async fn test_query_cursor_streaming() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Insert 100 persons
    for i in 0..100 {
        db.execute(&format!("CREATE (:Person {{name: 'Person {}'}})", i))
            .await?;
    }

    // Query with cursor
    let mut cursor = db
        .query_cursor("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .await?;

    assert_eq!(cursor.columns(), &["p.name"]);

    let mut total_rows = 0;
    while let Some(batch_res) = cursor.next_batch().await {
        let batch = batch_res?;
        total_rows += batch.len();
        // Each batch should be <= default batch size (usually 1024 or similar)
        // In our in-memory test, it might be all in one batch if default is 1024.
    }

    assert_eq!(total_rows, 100);

    // Test with smaller batch size in config if possible
    let db2 = Uni::in_memory()
        .config(uni_db::UniConfig {
            batch_size: 10,
            ..Default::default()
        })
        .build()
        .await?;

    db2.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    for i in 0..100 {
        db2.execute(&format!("CREATE (:Person {{name: 'Person {}'}})", i))
            .await?;
    }

    let mut cursor2 = db2.query_cursor("MATCH (p:Person) RETURN p.name").await?;
    let first_batch = cursor2.next_batch().await.unwrap()?;
    assert_eq!(first_batch.len(), 10);

    let remaining = cursor2.collect_remaining().await?;
    assert_eq!(remaining.len(), 90);

    Ok(())
}
