use uni_db::Uni;
use uni_query::Value;

#[tokio::test]
async fn test_labels_function() {
    let db = Uni::in_memory().build().await.unwrap();

    db.query("CREATE (:Person:Student {name: 'Alice'})")
        .await
        .unwrap();
    let result = db
        .query("MATCH (n:Person) RETURN labels(n) AS l")
        .await
        .unwrap();
    let val = result.rows[0].value("l").unwrap();
    println!("Labels: {:?}", val);

    if let Value::List(l) = val {
        assert_eq!(l.len(), 2);
        assert!(l.contains(&Value::String("Person".to_string())));
        assert!(l.contains(&Value::String("Student".to_string())));
    } else {
        panic!("Expected List, got {:?}", val);
    }
}

#[tokio::test]
async fn test_path_functions() {
    let db = Uni::in_memory().build().await.unwrap();

    db.query("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await
        .unwrap();
    let result = db
        .query(
            "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN nodes(p) AS n, relationships(p) AS r",
        )
        .await
        .unwrap();

    let nodes = result.rows[0].value("n").unwrap();
    let rels = result.rows[0].value("r").unwrap();

    // nodes(p) should return a list of Node objects
    if let Value::List(l) = nodes {
        assert_eq!(l.len(), 2);
        // ... verify Node content ...
    } else {
        panic!("Expected List for nodes(p), got {:?}", nodes);
    }

    // relationships(p) should return a list of Relationship objects
    if let Value::List(l) = rels {
        assert_eq!(l.len(), 1);
        // ... verify Relationship content ...
    } else {
        panic!("Expected List for relationships(p), got {:?}", rels);
    }
}
