use uni_db::Uni;
use uni_query::Value;

#[tokio::test]
async fn test_map_literal_return() {
    let db = Uni::in_memory().build().await.unwrap();
    let result = db.query("RETURN {a: 1} AS map").await.unwrap();

    let row = result.rows.first().unwrap();
    let val = row.value("map").unwrap();

    println!("Value: {:?}", val);

    if let Value::Map(m) = val {
        assert_eq!(m.get("a"), Some(&Value::Int(1)));
    } else {
        panic!("Expected Map, got {:?}", val);
    }
}

#[tokio::test]

async fn test_mixed_list_literal_return() {
    let db = Uni::in_memory().build().await.unwrap();

    let result = db.query("RETURN [1, 2.9] AS list").await.unwrap();

    let row = result.rows.first().unwrap();

    let val = row.value("list").unwrap();

    println!("Value: {:?}", val);
}
