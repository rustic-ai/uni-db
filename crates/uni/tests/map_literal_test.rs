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
async fn test_nested_list_literal_return() {
    let db = Uni::in_memory().build().await.unwrap();
    let result = db.query("RETURN [[1, 2]] AS list").await.unwrap();

    let row = result.rows.first().unwrap();
    let val = row.value("list").unwrap();

    println!("Value: {:?}", val);

    // Expected: List([List([Int(1), Int(2)])])
    if let Value::List(l) = val {
        assert_eq!(l.len(), 1);
        if let Value::List(inner) = &l[0] {
            assert_eq!(inner.len(), 2);
            assert_eq!(inner[0], Value::Int(1));
        } else {
            panic!("Expected nested List, got {:?}", l[0]);
        }
    } else {
        panic!("Expected List, got {:?}", val);
    }
}
