// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_reduce_execution() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Test with literal list
    let result = db
        .query("RETURN reduce(total = 0, x IN [1, 2, 3] | total + x) AS sum")
        .await?;
    assert_eq!(result.len(), 1);
    let sum: i64 = result.rows()[0].get("sum")?;
    assert_eq!(sum, 6);

    // Test with list comprehension (filtering)
    let result = db.query("RETURN reduce(acc = 0, x IN [1, 2, 3, 4] | acc + CASE WHEN x % 2 = 0 THEN x ELSE 0 END) AS even_sum").await?;
    assert_eq!(result.len(), 1);
    let even_sum: i64 = result.rows()[0].get("even_sum")?;
    assert_eq!(even_sum, 6); // 2 + 4

    // Test with string concatenation
    let result = db
        .query("RETURN reduce(s = '', x IN ['a', 'b', 'c'] | s + x) AS concat")
        .await?;
    assert_eq!(result.len(), 1);
    let concat: String = result.rows()[0].get("concat")?;
    assert_eq!(concat, "abc");

    Ok(())
}
