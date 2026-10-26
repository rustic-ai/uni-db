// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `CREATE p = (a)-[:R]->(b)` binds `p` to the path it created.
//!
//! openCypher allows a path variable on a CREATE pattern part, and Neo4j
//! returns the created path from it. Parsing and the mutation output schema
//! already carried `p`, but the planner's CREATE arm never put it in scope —
//! so `RETURN p` failed with `UndefinedVariable` — and no CREATE executor
//! assembled the path. MERGE had the path binding, but it named only the
//! anonymous relationships of the pattern, so an anonymous node was silently
//! left out of the path (`MERGE p = (a)-[:R]->(:X)` bound a one-node path).
//!
//! Every assertion reads the parts of `p` back (`nodes`, `relationships`,
//! `length`, endpoint ids) rather than only checking that the query plans: a
//! path missing an element still plans, still returns, and still has a length.

use anyhow::Result;
use uni_db::Uni;

async fn db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE R (w INT) FROM E TO E").await?;
    tx.commit().await?;
    Ok(db)
}

/// Run a write query in its own committed transaction and return its rows.
async fn write(db: &Uni, cypher: &str) -> Result<uni_db::QueryResult> {
    let tx = db.session().tx().await?;
    let r = tx.query(cypher).await?;
    tx.commit().await?;
    Ok(r)
}

#[tokio::test]
async fn create_binds_a_named_path() -> Result<()> {
    let db = db().await?;
    let r = write(
        &db,
        "CREATE p = (a:E {uid:'a'})-[r:R {w: 7}]->(b:E {uid:'b'}) \
         RETURN length(p) AS len, [n IN nodes(p) | n.uid] AS uids, \
                [e IN relationships(p) | e.w] AS ws, \
                [e IN relationships(p) | type(e)] AS types, \
                id(nodes(p)[0]) = id(a) AND id(nodes(p)[1]) = id(b) \
                    AND id(relationships(p)[0]) = id(r) AS same_elements, \
                p IS NULL AS p_null",
    )
    .await?;
    assert_eq!(r.rows().len(), 1);
    let row = &r.rows()[0];
    assert!(!row.get::<bool>("p_null")?, "p came back null");
    assert_eq!(row.get::<i64>("len")?, 1);
    assert_eq!(row.get::<Vec<String>>("uids")?, vec!["a", "b"]);
    assert_eq!(row.get::<Vec<i64>>("ws")?, vec![7]);
    assert_eq!(row.get::<Vec<String>>("types")?, vec!["R"]);
    assert!(
        row.get::<bool>("same_elements")?,
        "p does not hold the nodes and relationship the CREATE bound"
    );

    // `p` itself is returnable and is a path value.
    let r = write(
        &db,
        "CREATE p = (:E {uid:'c'})-[:R]->(:E {uid:'d'}) RETURN p",
    )
    .await?;
    match r.rows()[0].value("p") {
        Some(uni_db::Value::Path(path)) => {
            assert_eq!(path.nodes.len(), 2);
            assert_eq!(path.edges.len(), 1);
            assert_eq!(path.edges[0].src, path.nodes[0].vid);
            assert_eq!(path.edges[0].dst, path.nodes[1].vid);
        }
        other => panic!("RETURN p should be a path, got {other:?}"),
    }

    // And it is the path a later MATCH finds.
    let m = db
        .session()
        .query(
            "MATCH p = (:E {uid:'a'})-[:R]->(:E {uid:'b'}) \
             RETURN [n IN nodes(p) | n.uid] AS uids, length(p) AS len",
        )
        .await?;
    assert_eq!(m.rows()[0].get::<Vec<String>>("uids")?, vec!["a", "b"]);
    assert_eq!(m.rows()[0].get::<i64>("len")?, 1);
    Ok(())
}

/// Anonymous nodes and relationships are part of the path too. These are the
/// elements nothing else in the row carries, so binding them needs internal
/// names — and a missing one would silently shorten `p`.
#[tokio::test]
async fn create_path_includes_anonymous_elements() -> Result<()> {
    let db = db().await?;
    let r = write(
        &db,
        "CREATE p = (:E {uid:'x'})-[:R {w: 1}]->(:E {uid:'y'})<-[:R {w: 2}]-(:E {uid:'z'}) \
         RETURN length(p) AS len, [n IN nodes(p) | n.uid] AS uids, \
                [e IN relationships(p) | e.w] AS ws, \
                p",
    )
    .await?;
    let row = &r.rows()[0];
    assert_eq!(row.get::<i64>("len")?, 2);
    assert_eq!(row.get::<Vec<String>>("uids")?, vec!["x", "y", "z"]);
    assert_eq!(row.get::<Vec<i64>>("ws")?, vec![1, 2]);
    // The second relationship was written right-to-left: z -> y.
    let Some(uni_db::Value::Path(path)) = row.value("p") else {
        panic!("RETURN p should be a path, got {:?}", row.value("p"));
    };
    let vid = |i: usize| path.nodes[i].vid;
    let ends: Vec<_> = path.edges.iter().map(|e| (e.src, e.dst)).collect();
    assert_eq!(ends, vec![(vid(0), vid(1)), (vid(2), vid(1))]);
    // No internal binding name leaks into the result.
    for col in r.columns() {
        assert!(!col.starts_with("__"), "internal column {col} leaked");
    }
    Ok(())
}

/// `startNode` / `endNode` of a path's relationships resolve to the path's
/// nodes, whether the path was created or matched.
///
/// The endpoints a relationship names are hydrated beside it when the
/// relationship is a variable, or an element of a list variable. Reached
/// through a function over the path — `relationships(p)` — they were not, and
/// `startNode(e)` failed with "endpoint not available in this scope".
#[tokio::test]
async fn endpoints_of_a_created_paths_relationships() -> Result<()> {
    let db = db().await?;
    let r = write(
        &db,
        &format!(
            "CREATE p = (:E {{uid:'x'}})-[:R {{w: 1}}]->(:E {{uid:'y'}})<-[:R {{w: 2}}]-(:E {{uid:'z'}}) \
             RETURN {PATH_ENDS}"
        ),
    )
    .await?;
    assert_path_ends(&r)
}

/// The MATCH twin of the test above.
#[tokio::test]
async fn endpoints_of_a_matched_paths_relationships() -> Result<()> {
    let db = db().await?;
    write(
        &db,
        "CREATE (:E {uid:'x'})-[:R {w: 1}]->(:E {uid:'y'})<-[:R {w: 2}]-(:E {uid:'z'})",
    )
    .await?;
    let r = db
        .session()
        .query(&format!(
            "MATCH p = (:E {{uid:'x'}})-[:R]->(:E {{uid:'y'}})<-[:R]-(:E {{uid:'z'}}) \
             RETURN {PATH_ENDS}"
        ))
        .await?;
    assert_path_ends(&r)
}

const PATH_ENDS: &str = "[e IN relationships(p) | startNode(e).uid + '>' + endNode(e).uid] AS dirs, \
                         [e IN relationships(p) | id(startNode(e))] AS starts, \
                         [n IN nodes(p) | id(n)] AS ids";

/// The x -> y <- z path's relationships end where the path says.
fn assert_path_ends(r: &uni_db::QueryResult) -> Result<()> {
    assert_eq!(r.rows().len(), 1);
    let row = &r.rows()[0];
    assert_eq!(
        row.get::<Vec<String>>("dirs")?,
        vec!["x>y", "z>y"],
        "the second relationship runs z -> y"
    );
    let ids = row.get::<Vec<i64>>("ids")?;
    assert_eq!(row.get::<Vec<i64>>("starts")?, vec![ids[0], ids[2]]);
    Ok(())
}

/// Two named paths in one CREATE each get their own elements. The internal
/// names given to anonymous elements are per path; a name shared between the
/// two paths would bind the first path's relationship into the second.
#[tokio::test]
async fn two_paths_in_one_create_do_not_share_elements() -> Result<()> {
    let db = db().await?;
    let r = write(
        &db,
        "CREATE p = (:E {uid:'a'})-[:R {w: 1}]->(:E {uid:'b'}), \
                q = (:E {uid:'c'})-[:R {w: 2}]->(:E {uid:'d'}) \
         RETURN [n IN nodes(p) | n.uid] AS pu, [e IN relationships(p) | e.w] AS pw, \
                [n IN nodes(q) | n.uid] AS qu, [e IN relationships(q) | e.w] AS qw",
    )
    .await?;
    let row = &r.rows()[0];
    assert_eq!(row.get::<Vec<String>>("pu")?, vec!["a", "b"]);
    assert_eq!(row.get::<Vec<i64>>("pw")?, vec![1]);
    assert_eq!(row.get::<Vec<String>>("qu")?, vec!["c", "d"]);
    assert_eq!(row.get::<Vec<i64>>("qw")?, vec![2]);
    Ok(())
}

/// Consecutive CREATE clauses are planned as one batch; each keeps its path.
#[tokio::test]
async fn consecutive_creates_each_bind_their_path() -> Result<()> {
    let db = db().await?;
    let r = write(
        &db,
        "CREATE p = (:E {uid:'a'})-[:R {w: 1}]->(:E {uid:'b'}) \
         CREATE q = (:E {uid:'c'})-[:R {w: 2}]->(:E {uid:'d'}) \
         RETURN [n IN nodes(p) | n.uid] AS pu, [n IN nodes(q) | n.uid] AS qu",
    )
    .await?;
    let row = &r.rows()[0];
    assert_eq!(row.get::<Vec<String>>("pu")?, vec!["a", "b"]);
    assert_eq!(row.get::<Vec<String>>("qu")?, vec!["c", "d"]);
    Ok(())
}

/// A path whose endpoints were bound by MATCH holds those nodes.
#[tokio::test]
async fn create_path_over_matched_endpoints() -> Result<()> {
    let db = db().await?;
    write(&db, "CREATE (:E {uid:'a'}), (:E {uid:'b'})").await?;
    let r = write(
        &db,
        "MATCH (a:E {uid:'a'}), (b:E {uid:'b'}) \
         CREATE p = (a)-[:R {w: 3}]->(b) \
         RETURN [n IN nodes(p) | n.uid] AS uids, [e IN relationships(p) | e.w] AS ws, \
                id(nodes(p)[0]) = id(a) AND id(nodes(p)[1]) = id(b) AS same",
    )
    .await?;
    let row = &r.rows()[0];
    assert_eq!(row.get::<Vec<String>>("uids")?, vec!["a", "b"]);
    assert_eq!(row.get::<Vec<i64>>("ws")?, vec![3]);
    assert!(row.get::<bool>("same")?);
    Ok(())
}

/// One path per input row, each holding that row's elements.
#[tokio::test]
async fn create_path_per_row_and_in_call_subquery() -> Result<()> {
    let db = db().await?;
    let r = write(
        &db,
        "UNWIND ['a', 'b', 'c'] AS u \
         CREATE p = (:E {uid: u})-[:R]->(:E {uid: u + '2'}) \
         RETURN u, [n IN nodes(p) | n.uid] AS uids ORDER BY u",
    )
    .await?;
    assert_eq!(r.rows().len(), 3);
    for row in r.rows() {
        let u: String = row.get("u")?;
        assert_eq!(row.get::<Vec<String>>("uids")?, vec![u.clone(), u + "2"]);
    }

    let r = write(
        &db,
        "UNWIND ['s', 't'] AS u \
         CALL { WITH u CREATE p = (:E {uid: u})-[:R]->(:E {uid: u + '2'}) RETURN p } \
         RETURN u, [n IN nodes(p) | n.uid] AS uids, length(p) AS len ORDER BY u",
    )
    .await?;
    assert_eq!(r.rows().len(), 2);
    for row in r.rows() {
        let u: String = row.get("u")?;
        assert_eq!(row.get::<i64>("len")?, 1);
        assert_eq!(row.get::<Vec<String>>("uids")?, vec![u.clone(), u + "2"]);
    }
    Ok(())
}

/// A path variable is legal in a CREATE inside FOREACH; it is simply not
/// visible outside the FOREACH body.
#[tokio::test]
async fn create_path_inside_foreach() -> Result<()> {
    let db = db().await?;
    write(
        &db,
        "FOREACH (u IN ['f', 'g'] | CREATE p = (:E {uid: u})-[:R]->(:E {uid: u + '2'}))",
    )
    .await?;
    let n: i64 = db
        .session()
        .query("MATCH (:E)-[e:R]->(:E) RETURN count(e) AS n")
        .await?
        .rows()[0]
        .get("n")?;
    assert_eq!(n, 2);
    Ok(())
}

/// A path variable may not rebind a name already in scope, nor share a name
/// with an element of its own pattern (Neo4j: `VariableAlreadyBound`).
#[tokio::test]
async fn create_path_variable_must_be_new() -> Result<()> {
    let db = db().await?;
    write(&db, "CREATE (:E {uid:'a'})").await?;
    for cypher in [
        "MATCH (p:E) CREATE p = (:E)-[:R]->(:E) RETURN p",
        "CREATE p = (p:E)-[:R]->(:E)",
        "CREATE p = (:E)-[p:R]->(:E)",
        "CREATE p = (:E)-[:R]->(:E), p = (:E)-[:R]->(:E)",
        "CREATE p = (:E)-[:R]->(:E) CREATE p = (:E)-[:R]->(:E)",
    ] {
        let tx = db.session().tx().await?;
        let err = tx.query(cypher).await.err();
        let msg = err.map(|e| e.to_string()).unwrap_or_default();
        assert!(
            msg.contains("VariableAlreadyBound"),
            "{cypher} should fail with VariableAlreadyBound, got: {msg:?}"
        );
    }
    // A path bound by an earlier CREATE cannot be used as a node.
    let tx = db.session().tx().await?;
    let msg = tx
        .query("CREATE p = (:E)-[:R]->(:E) CREATE (p)-[:R]->(:E)")
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(
        msg.contains("VariableTypeConflict"),
        "a path used as a node should be a type conflict, got: {msg:?}"
    );
    Ok(())
}
