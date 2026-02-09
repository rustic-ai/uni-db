use uni_db::Uni;

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_create_drop() {
    println!("Creating DB...");
    let db = Uni::in_memory().build().await.unwrap();
    println!("DB created");
    drop(db);
    println!("DB dropped");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    println!("Test complete");
}
