use tempfile::TempDir;
use uni_db::UniBuilder;

#[tokio::test]
async fn test_list_labels_lifecycle() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let temp_dir = TempDir::new()?;
    let db = UniBuilder::new(temp_dir.path().to_str().unwrap().to_string())
        .build()
        .await?;

    // Initially no labels
    let labels = db.list_labels().await?;
    println!("Initial labels: {:?}", labels);
    assert_eq!(labels.len(), 0);

    // Create a vertex with label X
    db.execute("CREATE (:X)").await?;

    // Debug: Check what the count query returns
    let count_result = db.query("MATCH (n:X) RETURN count(n) as count").await?;
    println!("Count query result: {:?}", count_result);
    if !count_result.rows.is_empty() {
        match count_result.rows[0].get::<i64>("count") {
            Ok(c) => println!("Got count as i64: {}", c),
            Err(e) => println!("Failed to get as i64: {}", e),
        }
    }

    // Check if X is in schema
    let has_label = db.label_exists("X").await?;
    println!("Has label X in schema: {}", has_label);

    let labels = db.list_labels().await?;
    println!("After CREATE (:X): {:?}", labels);
    assert!(
        labels.contains(&"X".to_string()),
        "Label X should appear after creating vertex"
    );

    // Delete all X vertices
    db.execute("MATCH (n:X) DELETE n").await?;
    let labels = db.list_labels().await?;
    println!("After DELETE all X: {:?}", labels);
    assert!(
        !labels.contains(&"X".to_string()),
        "Label X should disappear after deleting all vertices"
    );

    Ok(())
}
