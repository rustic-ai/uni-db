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

// ---------------------------------------------------------------------------
// A relationship's `_type_name` must survive a flush.
//
// `l0_visibility::get_edge_type` reads the write buffers alone and has no
// storage fallback, unlike its sibling `get_edge_endpoints` whose rustdoc says
// callers must fall back to storage. So a flushed edge reported a made-up type
// name — the #135 / #141 shape again, this time on the type rather than the
// properties.
// ---------------------------------------------------------------------------

/// Read the `_type_name` of every element of a step variable.
async fn step_variable_type_names(db: &Uni, query: &str) -> Vec<String> {
    let result = db.session().query(query).await.unwrap();
    assert_eq!(result.rows().len(), 1, "query: {query}");
    let Value::List(items) = result.rows()[0].value("types").unwrap() else {
        panic!("expected a list of type names");
    };
    items
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => panic!("expected a string type name, got {other:?}"),
        })
        .collect()
}

const CHAIN_TYPES: &str = "MATCH (a:N {name: 'n1'})-[r:R*2..2]->(c:N) \
                           RETURN [e IN r | e._type_name] AS types";

#[tokio::test]
async fn test_edge_type_name_before_flush() {
    // Guards L0 precedence: the resident edge's type must still resolve.
    let db = setup_named_chain_with_schema().await;
    assert_eq!(step_variable_type_names(&db, CHAIN_TYPES).await, ["R", "R"]);
}

#[tokio::test]
async fn test_edge_type_name_after_flush() {
    let db = setup_named_chain_with_schema().await;
    db.flush().await.unwrap();
    assert_eq!(step_variable_type_names(&db, CHAIN_TYPES).await, ["R", "R"]);
}

/// Two different edge types traversed by one pattern. The schemaless path names
/// every edge with the pattern's whole type list joined by `|`, so this is the
/// case that distinguishes a per-edge resolution from a per-pattern one.
async fn setup_mixed_type_chain() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (a:N {name: 'n1'})-[:A]->(b:N {name: 'n2'})-[:B]->(c:N {name: 'n3'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

const MIXED_TYPES: &str = "MATCH (a:N {name: 'n1'})-[r:A|B*2..2]->(c:N) \
                           RETURN [e IN r | e._type_name] AS types";

#[tokio::test]
async fn test_mixed_edge_type_names_before_flush() {
    let db = setup_mixed_type_chain().await;
    assert_eq!(
        step_variable_type_names(&db, MIXED_TYPES).await,
        ["A", "B"],
        "each edge must report its own type, not the pattern's type list"
    );
}

#[tokio::test]
async fn test_mixed_edge_type_names_after_flush() {
    let db = setup_mixed_type_chain().await;
    db.flush().await.unwrap();
    assert_eq!(step_variable_type_names(&db, MIXED_TYPES).await, ["A", "B"]);
}

#[tokio::test]
async fn test_shortest_path_edge_type_name_after_flush() {
    let db = setup_named_chain_with_schema().await;
    db.flush().await.unwrap();
    let types = step_variable_type_names(
        &db,
        "MATCH p = shortestPath((a:N {name: 'n1'})-[:R*]->(c:N {name: 'n3'})) \
         RETURN [e IN relationships(p) | e._type_name] AS types",
    )
    .await;
    assert_eq!(types, ["R", "R"]);
}

/// A relationship must report its STORED orientation even when the query walked
/// it backwards. This is the property a bundled-context refactor could silently
/// break — `_src` and `_dst` are both UInt64, so a transposition is a wrong
/// answer rather than a type error. Asserted after a flush, where the
/// orientation comes from the adjacency probe rather than the write buffer.
#[tokio::test]
async fn test_reverse_traversal_reports_stored_orientation() {
    let db = setup_named_chain_with_schema().await;
    db.flush().await.unwrap();

    let forward = db
        .session()
        .query(
            "MATCH (a:N {name: 'n1'})-[r:R*1..1]->(b:N) \
             RETURN r[0]._src AS src, r[0]._dst AS dst",
        )
        .await
        .unwrap();
    let backward = db
        .session()
        .query(
            "MATCH (b:N {name: 'n2'})<-[r:R*1..1]-(a:N) \
             RETURN r[0]._src AS src, r[0]._dst AS dst",
        )
        .await
        .unwrap();

    assert_eq!(forward.rows().len(), 1);
    assert_eq!(backward.rows().len(), 1);
    let pair = |r: &uni_db::QueryResult| {
        (
            r.rows()[0].get::<i64>("src").unwrap(),
            r.rows()[0].get::<i64>("dst").unwrap(),
        )
    };
    assert_eq!(
        pair(&forward),
        pair(&backward),
        "walking the edge backwards must not flip the reported endpoints"
    );
}

/// The same edge, reached five different ways, must report the same type name.
/// Each way is a separate call site of the shared appender, so this catches one
/// site wired wrong — the characteristic failure of this kind of refactor.
#[tokio::test]
async fn test_edge_type_name_agrees_across_call_sites() {
    let db = setup_named_chain_with_schema().await;
    db.flush().await.unwrap();

    let single = |val: &Value, what: &str| -> String {
        let Value::List(items) = val else {
            panic!("expected a list for {what}, got {val:?}");
        };
        match items.first() {
            Some(Value::String(s)) => s.clone(),
            other => panic!("expected a string in {what}, got {other:?}"),
        }
    };

    let result = db
        .session()
        .query(
            "MATCH p = (a:N {name: 'n1'})-[r:R*1..1]->(b:N) \
             RETURN [e IN relationships(p) | e._type_name] AS via_path, \
                    [e IN r | e._type_name] AS via_step",
        )
        .await
        .unwrap();
    let via_path = single(result.rows()[0].value("via_path").unwrap(), "via_path");
    let via_step = single(result.rows()[0].value("via_step").unwrap(), "via_step");

    let sp = db
        .session()
        .query(
            "MATCH p = shortestPath((a:N {name: 'n1'})-[:R*]->(b:N {name: 'n2'})) \
             RETURN [e IN relationships(p) | e._type_name] AS types",
        )
        .await
        .unwrap();
    let via_shortest = single(sp.rows()[0].value("types").unwrap(), "shortestPath");

    let group = db
        .session()
        .query(
            "MATCH (s:N {name: 'n1'})((x)-[q:R]->(y)){1}(t:N) \
             RETURN [e IN q | e._type_name] AS types",
        )
        .await
        .unwrap();
    let via_group = single(group.rows()[0].value("types").unwrap(), "group variable");

    let pc = db
        .session()
        .query(
            "MATCH (a:N {name: 'n1'}) \
             RETURN [p = (a)-[:R]->(b) | [e IN relationships(p) | e._type_name]] AS paths",
        )
        .await
        .unwrap();
    let Value::List(pc_items) = pc.rows()[0].value("paths").unwrap() else {
        panic!("expected a list of paths");
    };
    let via_comprehension = single(&pc_items[0], "pattern comprehension");

    for (what, got) in [
        ("path variable", &via_path),
        ("step variable", &via_step),
        ("shortestPath", &via_shortest),
        ("group variable", &via_group),
        ("pattern comprehension", &via_comprehension),
    ] {
        assert_eq!(got, "R", "{what} reported the wrong type name");
    }
}
