use uni_db::Uni;
use uni_query::Value;

#[tokio::test]
async fn test_labels_function() {
    let db = Uni::in_memory().build().await.unwrap();

    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:Person:Student {name: 'Alice'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let result = db
        .session()
        .query("MATCH (n:Person) RETURN labels(n) AS l")
        .await
        .unwrap();
    let val = result.rows()[0].value("l").unwrap();
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

    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let result = db
        .session()
        .query(
            "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN nodes(p) AS n, relationships(p) AS r",
        )
        .await
        .unwrap();

    let nodes = result.rows()[0].value("n").unwrap();
    let rels = result.rows()[0].value("r").unwrap();

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

// ---------------------------------------------------------------------------
// Path element properties must survive a flush.
//
// Path node/edge structs carry user properties in a `properties` blob filled in
// while the batch is built. That lookup must consult storage as well as L0 —
// reading L0 alone silently drops every property once the vertex has been
// flushed out of the write buffer (the #135 / #141 failure shape).
// ---------------------------------------------------------------------------

/// Chain `n1 -[t1]-> n2 -[t2]-> n3`, every element carrying a distinct value so
/// a uniform or null answer is distinguishable from the correct one.
async fn setup_named_chain() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();

    let tx = db.session().tx().await.unwrap();
    tx.execute(
        "CREATE (a:N {name: 'n1'})-[:R {tag: 't1'}]->(b:N {name: 'n2'})-[:R {tag: 't2'}]->(c:N {name: 'n3'})",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    db
}

async fn chain_path_names(db: &Uni) -> Vec<Value> {
    let result = db
        .session()
        .query(
            "MATCH p = (a:N {name: 'n1'})-[:R*2..2]->(c:N) \
             RETURN [x IN nodes(p) | x.name] AS names, \
                    [e IN relationships(p) | e.tag] AS tags",
        )
        .await
        .unwrap();
    assert_eq!(result.rows().len(), 1);

    let names = result.rows()[0].value("names").unwrap().clone();
    let tags = result.rows()[0].value("tags").unwrap().clone();
    vec![names, tags]
}

fn expect_strings(val: &Value, want: &[&str], what: &str) {
    let Value::List(items) = val else {
        panic!("expected a list for {what}, got {val:?}");
    };
    let got: Vec<String> = items
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => panic!("expected a string in {what}, got {other:?}"),
        })
        .collect();
    assert_eq!(got, want, "{what}");
}

#[tokio::test]
async fn test_path_element_properties_before_flush() {
    // Guards L0 precedence: unflushed writes must still be visible.
    let db = setup_named_chain().await;
    let vals = chain_path_names(&db).await;
    expect_strings(&vals[0], &["n1", "n2", "n3"], "nodes(p) names");
    expect_strings(&vals[1], &["t1", "t2"], "relationships(p) tags");
}

#[tokio::test]
async fn test_path_element_properties_after_flush() {
    let db = setup_named_chain().await;
    db.flush().await.unwrap();
    let vals = chain_path_names(&db).await;
    expect_strings(&vals[0], &["n1", "n2", "n3"], "nodes(p) names");
    expect_strings(&vals[1], &["t1", "t2"], "relationships(p) tags");
}

/// The same chain with a declared schema, which routes the traversal through
/// the schema'd executor rather than the schemaless main-table one. Both build
/// path structs and both need the pre-fetch.
async fn setup_named_chain_with_schema() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();

    db.schema()
        .label("N")
        .property("name", uni_db::DataType::String)
        .apply()
        .await
        .unwrap();
    db.schema()
        .edge_type("R", &["N"], &["N"])
        .property("tag", uni_db::DataType::String)
        .apply()
        .await
        .unwrap();

    let tx = db.session().tx().await.unwrap();
    tx.execute(
        "CREATE (a:N {name: 'n1'})-[:R {tag: 't1'}]->(b:N {name: 'n2'})-[:R {tag: 't2'}]->(c:N {name: 'n3'})",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    db
}

#[tokio::test]
async fn test_schemad_path_element_properties_before_flush() {
    let db = setup_named_chain_with_schema().await;
    let vals = chain_path_names(&db).await;
    expect_strings(&vals[0], &["n1", "n2", "n3"], "nodes(p) names");
    expect_strings(&vals[1], &["t1", "t2"], "relationships(p) tags");
}

#[tokio::test]
async fn test_schemad_path_element_properties_after_flush() {
    let db = setup_named_chain_with_schema().await;
    db.flush().await.unwrap();
    let vals = chain_path_names(&db).await;
    expect_strings(&vals[0], &["n1", "n2", "n3"], "nodes(p) names");
    expect_strings(&vals[1], &["t1", "t2"], "relationships(p) tags");
}

/// Run `query` against the flushed chain and assert the `names` column.
async fn assert_flushed_path_names(db: Uni, query: &str, want: &[&str]) {
    db.flush().await.unwrap();
    let result = db.session().query(query).await.unwrap();
    assert_eq!(result.rows().len(), 1, "query: {query}");
    expect_strings(
        result.rows()[0].value("names").unwrap(),
        want,
        "nodes(p) names",
    );
}

#[tokio::test]
async fn test_shortest_path_element_properties_after_flush() {
    // shortestPath resolves a target label id, so this needs the declared schema.
    assert_flushed_path_names(
        setup_named_chain_with_schema().await,
        "MATCH p = shortestPath((a:N {name: 'n1'})-[:R*]->(c:N {name: 'n3'})) \
         RETURN [x IN nodes(p) | x.name] AS names",
        &["n1", "n2", "n3"],
    )
    .await;
}

#[tokio::test]
async fn test_fixed_path_element_properties_after_flush() {
    assert_flushed_path_names(
        setup_named_chain().await,
        "MATCH p = (a:N {name: 'n1'})-[:R]->(b:N) RETURN [x IN nodes(p) | x.name] AS names",
        &["n1", "n2"],
    )
    .await;
}

#[tokio::test]
async fn test_zero_length_path_element_properties_after_flush() {
    assert_flushed_path_names(
        setup_named_chain().await,
        "MATCH p = (a:N {name: 'n1'}) RETURN [x IN nodes(p) | x.name] AS names",
        &["n1"],
    )
    .await;
}

#[tokio::test]
async fn test_pattern_comprehension_path_element_properties_after_flush() {
    let db = setup_named_chain().await;
    db.flush().await.unwrap();
    let result = db
        .session()
        .query(
            "MATCH (a:N {name: 'n1'}) \
             RETURN [p = (a)-[:R]->(b) | [x IN nodes(p) | x.name]] AS paths",
        )
        .await
        .unwrap();
    assert_eq!(result.rows().len(), 1);
    let paths = result.rows()[0].value("paths").unwrap().clone();
    let Value::List(items) = &paths else {
        panic!("expected a list, got {paths:?}");
    };
    assert_eq!(items.len(), 1);
    expect_strings(&items[0], &["n1", "n2"], "pattern comprehension nodes(p)");
}

// ---------------------------------------------------------------------------
// shortestPath binds its relationship variable as a list of relationships.
//
// Plain openCypher variable-length semantics — `r` in
// `shortestPath((a)-[r:E*]->(b))` holds every relationship on the returned
// path, in path order. Purely additive: `r` was previously dropped by the
// planner and surfaced as `UndefinedVariable` at the RETURN.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_shortest_path_binds_relationship_list() {
    let db = setup_named_chain_with_schema().await;
    let result = db
        .session()
        .query(
            "MATCH p = shortestPath((a:N {name: 'n1'})-[r:R*]->(c:N {name: 'n3'})) \
             RETURN size(r) AS len, [e IN r | e.tag] AS tags",
        )
        .await
        .unwrap();

    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("len").unwrap(), 2);
    expect_strings(
        result.rows()[0].value("tags").unwrap(),
        &["t1", "t2"],
        "shortestPath edge list tags",
    );
}

#[tokio::test]
async fn test_shortest_path_relationship_list_after_flush() {
    let db = setup_named_chain_with_schema().await;
    db.flush().await.unwrap();
    let result = db
        .session()
        .query(
            // The engine requires shortestPath to be assigned to a path
            // variable, so `p` is bound here even though only `r` is read.
            "MATCH p = shortestPath((a:N {name: 'n1'})-[r:R*]->(c:N {name: 'n3'})) \
             RETURN [e IN r | e.tag] AS tags",
        )
        .await
        .unwrap();

    assert_eq!(result.rows().len(), 1);
    expect_strings(
        result.rows()[0].value("tags").unwrap(),
        &["t1", "t2"],
        "shortestPath edge list tags",
    );
}

#[tokio::test]
async fn test_shortest_path_edge_list_matches_path_relationships() {
    let db = setup_named_chain_with_schema().await;
    let result = db
        .session()
        .query(
            "MATCH p = shortestPath((a:N {name: 'n1'})-[r:R*]->(c:N {name: 'n3'})) \
             RETURN [e IN r | e.tag] AS from_r, \
                    [e IN relationships(p) | e.tag] AS from_p",
        )
        .await
        .unwrap();

    assert_eq!(result.rows().len(), 1);
    let from_r = result.rows()[0].value("from_r").unwrap();
    let from_p = result.rows()[0].value("from_p").unwrap();
    assert_eq!(from_r, from_p);
}

#[tokio::test]
async fn test_all_shortest_paths_binds_relationship_list() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("N")
        .property("name", uni_db::DataType::String)
        .apply()
        .await
        .unwrap();
    db.schema()
        .edge_type("R", &["N"], &["N"])
        .property("tag", uni_db::DataType::String)
        .apply()
        .await
        .unwrap();

    // Two distinct two-hop routes from n1 to n3, so allShortestPaths returns
    // two rows and each row's `r` must match that row's path, not the other's.
    let tx = db.session().tx().await.unwrap();
    tx.execute(
        "CREATE (a:N {name: 'n1'}), (m1:N {name: 'm1'}), (m2:N {name: 'm2'}), (z:N {name: 'n3'}) \
         CREATE (a)-[:R {tag: 'a1'}]->(m1), (m1)-[:R {tag: 'b1'}]->(z) \
         CREATE (a)-[:R {tag: 'a2'}]->(m2), (m2)-[:R {tag: 'b2'}]->(z)",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let result = db
        .session()
        .query(
            "MATCH p = allShortestPaths((a:N {name: 'n1'})-[r:R*]->(c:N {name: 'n3'})) \
             RETURN [e IN r | e.tag] AS tags ORDER BY tags",
        )
        .await
        .unwrap();

    assert_eq!(result.rows().len(), 2);
    expect_strings(
        result.rows()[0].value("tags").unwrap(),
        &["a1", "b1"],
        "first path tags",
    );
    expect_strings(
        result.rows()[1].value("tags").unwrap(),
        &["a2", "b2"],
        "second path tags",
    );
}

// ---------------------------------------------------------------------------
// List-typed bindings survive projection.
//
// A relationship list, a node list and a group variable are distinct variable
// types rather than an entity type plus a side-channel flag, so their listness
// propagates through WITH — including through an alias, which previously
// dropped it.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_vlp_edge_list_survives_with_alias() {
    let db = setup_named_chain_with_schema().await;

    // The un-aliased form always worked; the aliased one dropped the listness
    // and failed with "size() requires a string, list, or path argument".
    for query in [
        "MATCH (a:N {name: 'n1'})-[r:R*1..2]->(b:N) WITH r RETURN size(r) AS len ORDER BY len",
        "MATCH (a:N {name: 'n1'})-[r:R*1..2]->(b:N) WITH r AS rs RETURN size(rs) AS len ORDER BY len",
    ] {
        let result = db.session().query(query).await.unwrap_or_else(|e| {
            panic!("query failed: {query}\n{e}");
        });
        let lens: Vec<i64> = result
            .rows()
            .iter()
            .map(|r| r.get::<i64>("len").unwrap())
            .collect();
        assert_eq!(lens, vec![1, 2], "query: {query}");
    }
}

#[tokio::test]
async fn test_relationships_and_nodes_are_list_typed() {
    let db = setup_named_chain_with_schema().await;

    let result = db
        .session()
        .query(
            "MATCH p = (a:N {name: 'n1'})-[:R*2..2]->(c:N) \
             WITH nodes(p) AS ns, relationships(p) AS rs \
             RETURN size(ns) AS n_count, size(rs) AS r_count",
        )
        .await
        .unwrap();

    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("n_count").unwrap(), 3);
    assert_eq!(result.rows()[0].get::<i64>("r_count").unwrap(), 2);
}

#[tokio::test]
async fn test_unwind_unwraps_a_node_list() {
    let db = setup_named_chain_with_schema().await;

    // UNWIND is the inverse of the list binding: the items are single nodes,
    // so ordinary property access on them is legal.
    let result = db
        .session()
        .query(
            "MATCH p = (a:N {name: 'n1'})-[:R*2..2]->(c:N) \
             WITH nodes(p) AS ns UNWIND ns AS n RETURN n.name AS name ORDER BY name",
        )
        .await
        .unwrap();

    let names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap())
        .collect();
    assert_eq!(names, vec!["n1", "n2", "n3"]);
}
