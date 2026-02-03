// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::time::Duration;
use uni_db::Uni;

#[tokio::test]
#[ignore = "Query timeout enforcement not yet implemented"]
async fn test_query_timeout() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("Node").apply().await?;

    // Create some data
    for _ in 0..100 {
        db.execute("CREATE (:Node)").await?;
    }

    // This query should be very fast, but let's set an extremely short timeout
    let res = db
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .fetch_all()
        .await;

    assert!(res.is_err());
    let err_msg = res.err().unwrap().to_string();
    assert!(err_msg.contains("Query timed out"));

    Ok(())
}

#[tokio::test]
#[ignore = "Query memory limit enforcement not yet implemented"]
async fn test_query_memory_limit() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("Node").apply().await?;

    // Create some data
    for _ in 0..100 {
        db.execute("CREATE (:Node)").await?;
    }

    // Set an extremely small memory limit
    let res = db
        .query_with("MATCH (n:Node) RETURN n")
        .max_memory(100) // 100 bytes
        .fetch_all()
        .await;

    assert!(res.is_err());
    let err_msg = res.err().unwrap().to_string();
    assert!(err_msg.contains("Query exceeded memory limit"));

    Ok(())
}
