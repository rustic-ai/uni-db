// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_db::Uni;

#[tokio::test]
async fn test_reduce_simple() {
    let _ = std::fs::remove_dir_all("tmp/test_reduce_simple");
    let uni = Uni::open("tmp/test_reduce_simple").build().await.unwrap();
    uni.execute("CREATE LABEL Item (values JSON)")
        .await
        .unwrap();

    // Create some data
    uni.execute("CREATE (:Item {values: [1, 2, 3, 4, 5]})")
        .await
        .unwrap();
    uni.flush().await.unwrap();

    // Test REDUCE
    let result = uni
        .query("MATCH (n:Item) RETURN reduce(acc = 0, x IN n.values | acc + x) AS sum")
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get::<i64>("sum").unwrap(), 15);
}

#[tokio::test]
async fn test_reduce_with_variable_scope() {
    let _ = std::fs::remove_dir_all("tmp/test_reduce_scope");
    let uni = Uni::open("tmp/test_reduce_scope").build().await.unwrap();
    uni.execute("CREATE LABEL Item (vals JSON)").await.unwrap();

    uni.execute("CREATE (:Item {vals: [10, 20]})")
        .await
        .unwrap();
    uni.flush().await.unwrap();

    // REDUCE with init value from another expression
    let result = uni
        .query("MATCH (n:Item) RETURN reduce(acc = 100, x IN n.vals | acc + x) AS total")
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get::<i64>("total").unwrap(), 130); // 100 + 10 + 20
}

#[tokio::test]
async fn test_reduce_list_literal() {
    let _ = std::fs::remove_dir_all("tmp/test_reduce_literal");
    let uni = Uni::open("tmp/test_reduce_literal").build().await.unwrap();

    let result = uni
        .query("RETURN reduce(s = '', x IN ['a', 'b', 'c'] | s + x) AS str")
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get::<String>("str").unwrap(), "abc");
}
