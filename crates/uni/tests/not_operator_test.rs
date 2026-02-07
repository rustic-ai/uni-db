use uni_db::Uni;
use uni_query::Value;

#[tokio::test]
async fn test_not_operator() {
    let db = Uni::in_memory().build().await.unwrap();

    // RETURN NOT true -> false
    let result = db.query("RETURN NOT true AS val").await.unwrap();
    assert_eq!(result.rows[0].value("val").unwrap(), &Value::Bool(false));

    // RETURN NOT null -> null
    let result = db.query("RETURN NOT null AS val").await.unwrap();
    assert_eq!(result.rows[0].value("val").unwrap(), &Value::Null);

    // RETURN NOT 1 -> Error
    let result = db.query("RETURN NOT 1 AS val").await;
    assert!(result.is_err());
    let err = result.err().unwrap().to_string();
    println!("Error: {}", err);
    assert!(err.contains("InvalidArgumentType"));
}
