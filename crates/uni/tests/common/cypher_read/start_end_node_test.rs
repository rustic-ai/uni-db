//! `startNode(r)` / `endNode(r)` on a relationship bound by a MATCH traversal.
//!
//! These are standard openCypher functions and the queries below are valid.
//! They used to fail whenever the relationship came from a `MATCH` traversal
//! rather than from `MERGE`/`CREATE`:
//!
//! ```text
//! MATCH ()-[e:KNOWS]->() RETURN startNode(e).name
//! Schema error: No field named e.
//! Valid fields are "_anon_0._vid", …, "e._eid", "e._type".
//! ```
//!
//! That error message is also the answer. #187 proposed that the endpoint VIDs
//! were already on the edge columns and only needed resolving; the schema it
//! prints says otherwise — there is no `e._src_vid`. What *is* there is
//! `_anon_0` and `_anon_1`: the traversal's own endpoint variables. For a
//! single-hop traversal in a known direction, `startNode(e)` is not a value to
//! compute at all, it is a variable already in scope, so the planner rewrites
//! it to that variable (`resolve_traversal_endpoints`). Doing it in the logical
//! plan rather than at DataFusion translation time is what keeps
//! `startNode(e).name` narrowing to one column instead of materialising the
//! whole endpoint.
//!
//! The openCypher TCK exercises these functions in exactly one scenario,
//! `Merge5` [11] (`clauses/merge/Merge5.feature:219`), where the relationship is
//! bound by `MERGE`. That scenario passed throughout. So the suite covered the
//! feature in the one context where it worked and could not see the context
//! where it did not — the same single-context blind spot that hid unanchored
//! pattern comprehensions. The MERGE case is kept below as a control, so a
//! failure there is a regression rather than the old gap.
//!
//! The undirected case (#188) is closed too, and not by making the start node
//! statically knowable — it is not. Which end of `-[e]-` is the relationship's
//! tail is a per-row fact, but a fact the traversal already knew and threw
//! away: the adjacency indexes an undirected edge under *both* endpoints, and
//! nothing recorded which side a row matched. The traversal now reports it on
//! `{r}._fwd` and the planner rewrites the call to a `CASE` over the hop's two
//! variables, so both branches stay ordinary variable references and the
//! property narrowing above still applies.
//!
//! `_fwd` is computed only when a query asks for it, so an undirected traversal
//! that never calls `startNode`/`endNode` costs exactly what it cost before.
//!
//! One shape is still open, `#[ignore]`d against the issue that tracks it: once
//! a `WITH` drops the endpoint variables from scope the relationship value is
//! all that is left, and turning its `_src_vid` back into a node with
//! properties needs a lookup against the vertex table that a scalar UDF cannot
//! do — the remaining work under #187.

use uni_db::{Uni, Value};

/// `a` -[:KNOWS]-> `b`.
async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// The context the TCK covers, kept as the control: this passes today, so a
/// failure here would mean a regression rather than the known gap.
#[tokio::test]
async fn start_and_end_node_work_on_a_merge_bound_relationship() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    let rows = tx
        .query_with(
            "CREATE (x {id: 2}), (y {id: 1}) MERGE (x)-[r:R]-(y) \
             RETURN startNode(r).id AS s, endNode(r).id AS e",
        )
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(rows.rows()[0].values()[0], Value::Int(2));
    assert_eq!(rows.rows()[0].values()[1], Value::Int(1));
}

#[tokio::test]
async fn start_node_property_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() RETURN startNode(e).name AS s, endNode(e).name AS t")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::String("b".to_string()));
}

/// Not a property-access problem: returning the node itself fails the same way.
#[tokio::test]
async fn start_node_whole_entity_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() RETURN startNode(e) AS s")
        .await
        .unwrap();
    assert!(matches!(r.rows()[0].values()[0], Value::Node(_)));
}

/// Nor is it about materializing properties: `id()` of the endpoint fails too.
#[tokio::test]
async fn id_of_start_node_on_a_match_bound_relationship() {
    let db = fixture().await;
    let direct = db
        .session()
        .query("MATCH (n:P {name:'a'}) RETURN id(n)")
        .await
        .unwrap();
    let via_edge = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() RETURN id(startNode(e))")
        .await
        .unwrap();
    assert_eq!(via_edge.rows()[0].values()[0], direct.rows()[0].values()[0]);
}

/// The one shape still open. A `WITH` narrows scope to `rel`, so the endpoint
/// variables the rewrite would resolve to are gone by the time `startNode` is
/// called, and only a vertex lookup could recover them.
#[tokio::test]
#[ignore = "#187 remainder: a WITH drops the endpoint variables, and resolving \
            the relationship's _src_vid back to a node needs a vertex lookup"]
async fn start_node_after_a_with_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() WITH e AS rel RETURN startNode(rel).name AS s")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
}

/// Direction is what makes the rewrite sound, so it is worth a test of its own:
/// `<-[e]-` traverses against the arrow, so the relationship's start node is the
/// traversal's *target*, not its source. Getting this backwards would swap the
/// two endpoints silently — a wrong answer, not an error.
#[tokio::test]
async fn start_and_end_node_follow_the_arrow_not_the_traversal() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (y:P)<-[e:KNOWS]-(x:P) RETURN startNode(e).name AS s, endNode(e).name AS t")
        .await
        .unwrap();
    // The edge is a->b. Read backwards from b, the start is still a.
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::String("b".to_string()));
}

/// The same edge reached through an undirected pattern.
///
/// Which end of an undirected relationship is the start is a per-row fact, so
/// no static rewrite applies — but it is a fact the traversal *knows* and used
/// to discard. The adjacency lists an undirected edge under both endpoints, and
/// the orientation is now carried on `{r}._fwd`, so the planner rewrites the
/// call to a `CASE` over the two candidate variables, both already in scope
/// with their properties materialised.
///
/// The rejected alternative is worth recording: handing the UDF a bare
/// `_src_vid` and letting it fall back to a minimal `{_vid}` node would make
/// this query *plan* in a few lines. `id(startNode(e))` would start working
/// while `startNode(e).name` returned NULL — a loud error traded for a silent
/// wrong answer, which is the one direction this codebase's ordering principle
/// says never to move in. That is why every assertion below reads a property
/// as well as an id.
#[tokio::test]
async fn start_and_end_node_on_an_undirected_match() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (x:P {name:'b'})-[e:KNOWS]-(y:P) RETURN id(startNode(e)) AS s, id(endNode(e)) AS t")
        .await
        .unwrap();
    let a = db
        .session()
        .query("MATCH (n:P {name:'a'}) RETURN id(n) AS v")
        .await
        .unwrap();
    let b = db
        .session()
        .query("MATCH (n:P {name:'b'}) RETURN id(n) AS v")
        .await
        .unwrap();
    // Walked from `b`, but the edge is still a->b.
    assert_eq!(r.rows()[0].values()[0], a.rows()[0].values()[0]);
    assert_eq!(r.rows()[0].values()[1], b.rows()[0].values()[0]);
}

/// A variable-length step variable holds a *list* of relationships, so there is
/// no single pair of endpoints to rewrite to. The pass must leave it alone
/// rather than resolve it to the pattern's outer endpoints, which would be the
/// wrong nodes for every hop but the last.
#[tokio::test]
async fn variable_length_step_variable_is_not_rewritten() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (x:P {name:'a'})-[e:KNOWS*1..2]->(y:P) RETURN size(e) AS hops")
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(1));
}

/// The discriminating test for the undirected case, and the reason the one
/// above is not enough on its own.
///
/// The edge is `a -> b`, so `startNode` is `a` and `endNode` is `b` no matter
/// which end the pattern is anchored at. Anchoring at `b` walks the edge
/// backwards and anchoring at `a` walks it forwards, so the two runs disagree
/// about which of the traversal's own variables holds the tail — and a rewrite
/// that resolves the endpoint to a fixed side gets exactly one of them right.
///
/// Asserting the two runs against *each other* is what makes that visible: a
/// single-anchor test passes with the orientation inverted, and passes again
/// with the orientation missing entirely.
#[tokio::test]
async fn undirected_endpoints_do_not_depend_on_the_anchor() {
    let db = fixture().await;
    async fn ids(db: &Uni, name: &str) -> Vec<Value> {
        let r = db
            .session()
            .query(&format!(
                "MATCH (x:P {{name:'{name}'}})-[e:KNOWS]-(y:P) \
                 RETURN id(startNode(e)) AS s, id(endNode(e)) AS t, \
                        startNode(e).name AS sn, endNode(e).name AS tn"
            ))
            .await
            .unwrap();
        r.rows()[0].values().to_vec()
    }
    let from_a = ids(&db, "a").await;
    let from_b = ids(&db, "b").await;
    assert_eq!(
        from_a, from_b,
        "the relationship's endpoints changed with the anchor the pattern was \
         walked from; the edge is a->b in both runs"
    );
    // And they are the right way round, not merely consistent.
    assert_eq!(
        from_a[2],
        Value::String("a".to_string()),
        "startNode is `a`"
    );
    assert_eq!(from_a[3], Value::String("b".to_string()), "endNode is `b`");
}

/// Two edges between the same pair, one each way, matched undirectedly.
///
/// Each row must report its *own* edge's endpoints, so the two rows disagree.
/// A single orientation flag reused across the pair — or one derived from the
/// vertex pair rather than the edge — collapses them into two identical rows,
/// which this catches and the single-edge fixture cannot.
#[tokio::test]
async fn reciprocal_edges_keep_their_own_endpoints() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'b'}), (y:P {name:'a'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let r = db
        .session()
        .query(
            "MATCH (x:P {name:'a'})-[e:KNOWS]-(y:P) \
             RETURN startNode(e).name AS s, endNode(e).name AS t ORDER BY s",
        )
        .await
        .unwrap();
    let pairs: Vec<(String, String)> = r
        .rows()
        .iter()
        .map(|row| match (&row.values()[0], &row.values()[1]) {
            (Value::String(s), Value::String(t)) => (s.clone(), t.clone()),
            other => panic!("expected two strings, got {other:?}"),
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("a".to_string(), "b".to_string()),
            ("b".to_string(), "a".to_string())
        ],
        "each of the two reciprocal edges must report its own direction"
    );
}

/// A self-loop matched undirectedly.
///
/// The adjacency lists it once, not twice, so this pins that the orientation
/// work did not reintroduce the duplicate the dedup guard exists to prevent.
/// Both endpoints are the same vertex, so either orientation names it.
#[tokio::test]
async fn a_self_loop_yields_one_row_with_equal_endpoints() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'})").await.unwrap();
    tx.execute("MATCH (x:P {name:'a'}) CREATE (x)-[:KNOWS]->(x)")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let r = db
        .session()
        .query(
            "MATCH (x:P)-[e:KNOWS]-(y:P) \
             RETURN startNode(e).name AS s, endNode(e).name AS t",
        )
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 1, "a self-loop is one relationship");
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::String("a".to_string()));
}

/// An undirected `startNode` under an aggregate.
///
/// `_fwd` varies per row while an aggregate spans rows, so the `CASE` must land
/// *inside* `collect(...)`, not around it. Lifted around it the aggregate would
/// see one orientation for the whole group and return `["b","b"]` or
/// `["a","c"]` — a plausible-looking list that no single-row test can catch.
/// The mixture is the evidence.
#[tokio::test]
async fn an_aggregate_over_undirected_endpoints_does_not_split_its_group() {
    let db = aggregate_fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (x:P {name:'a'})-[e:KNOWS]-(y:P) \
             RETURN collect(startNode(e).name) AS names",
        )
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 1, "one group, not one per orientation");
    let Value::List(items) = &r.rows()[0].values()[0] else {
        panic!("expected a list, got {:?}", r.rows()[0].values()[0]);
    };
    let mut names: Vec<String> = items
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect();
    names.sort();
    // `b -> a` contributes `b`; `a -> c` contributes `a`. One from each
    // orientation, which is only possible if the CASE is inside the aggregate.
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}

/// `b -> a` and `a -> c`: walking undirected from `a` reaches one edge in each
/// orientation, so the two rows take opposite `CASE` branches.
async fn aggregate_fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'}), (:P {name:'c'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'b'}), (y:P {name:'a'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'c'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// Returning the *whole* endpoint of an undirected relationship.
///
/// This is the shape that exposed a gap wider than `startNode`: a `CASE` whose
/// branches are two node variables did not work at all, with or without any
/// endpoint call. `find_common_result_type` had no rule for entity structs, and
/// two nodes are the same *Cypher* type without being the same *Arrow* type —
/// a scanned anchor carries `_all_props` and a traversal target does not — so
/// the pair fell through to the Utf8 fallback and died on `Unsupported CAST
/// from Struct(..) to Utf8`.
///
/// Both anchors are asserted, and the properties are read rather than just the
/// id, for the same reason as everywhere else in this file: an endpoint that
/// resolves to the right vertex with an empty property bag is the failure this
/// feature exists to avoid.
#[tokio::test]
async fn the_whole_endpoint_of_an_undirected_relationship_resolves() {
    let db = fixture().await;
    for anchor in ["a", "b"] {
        let r = db
            .session()
            .query(&format!(
                "MATCH (x:P {{name:'{anchor}'}})-[e:KNOWS]-(y:P) \
                 RETURN startNode(e) AS s, endNode(e) AS t"
            ))
            .await
            .unwrap_or_else(|e| panic!("anchored at {anchor}: {e}"));
        let row = r.rows()[0].values().to_vec();
        for (value, expected) in [(&row[0], "a"), (&row[1], "b")] {
            let Value::Node(node) = value else {
                panic!("anchored at {anchor}: expected a node, got {value:?}");
            };
            assert_eq!(
                node.properties.get("name"),
                Some(&Value::String(expected.to_string())),
                "anchored at {anchor}: wrong endpoint, or an endpoint with no properties"
            );
            assert_eq!(node.labels, vec!["P".to_string()]);
        }
    }
}

/// The endpoints of an undirected relationship between two *different* labels.
///
/// Their structs differ by real property columns, not just by `_all_props`, so
/// this is the case a same-label fixture cannot reach: any fix that works by
/// making the two shapes coincide would pass the test above and fail here.
#[tokio::test]
async fn undirected_endpoints_across_two_labels_resolve() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL Person (name STRING)")
        .await
        .unwrap();
    tx.execute("CREATE LABEL City (title STRING, pop INT)")
        .await
        .unwrap();
    tx.execute("CREATE EDGE TYPE LIVES_IN FROM Person TO City")
        .await
        .unwrap();
    tx.execute("CREATE (:Person {name:'ann'}), (:City {title:'oslo', pop: 7})")
        .await
        .unwrap();
    tx.execute(
        "MATCH (p:Person {name:'ann'}), (c:City {title:'oslo'}) CREATE (p)-[:LIVES_IN]->(c)",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Anchored at the City end, so the traversal walks against the arrow and the
    // relationship's tail is the *other* variable.
    let r = db
        .session()
        .query("MATCH (c:City)-[e:LIVES_IN]-(p:Person) RETURN startNode(e) AS s, endNode(e) AS t")
        .await
        .unwrap();
    let row = r.rows()[0].values().to_vec();
    let Value::Node(start) = &row[0] else {
        panic!("expected a node, got {:?}", row[0]);
    };
    let Value::Node(end) = &row[1] else {
        panic!("expected a node, got {:?}", row[1]);
    };
    assert_eq!(start.labels, vec!["Person".to_string()]);
    assert_eq!(
        start.properties.get("name"),
        Some(&Value::String("ann".to_string()))
    );
    assert_eq!(end.labels, vec!["City".to_string()]);
    assert_eq!(
        end.properties.get("title"),
        Some(&Value::String("oslo".to_string()))
    );
}

/// A `CASE` over two node variables, with no endpoint call involved.
///
/// The regression test for the underlying gap: a user can write this directly,
/// and it failed the same way before the entity-struct coercion rule existed.
#[tokio::test]
async fn a_case_over_two_node_variables_resolves() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (x:P {name:'a'})-[:KNOWS]->(y:P) \
             RETURN CASE WHEN x.name = 'a' THEN y ELSE x END AS n",
        )
        .await
        .unwrap();
    let Value::Node(node) = &r.rows()[0].values()[0] else {
        panic!("expected a node, got {:?}", r.rows()[0].values()[0]);
    };
    assert_eq!(
        node.properties.get("name"),
        Some(&Value::String("b".to_string()))
    );
}

/// An aggregate over a *directed* endpoint — a regression test for a bug the
/// undirected work uncovered rather than caused.
///
/// This pass runs after planning, so an `Aggregate`'s output columns have
/// already been named by their rendered expression and the projection above
/// refers to them by that string. Rewriting `collect(startNode(e).name)` into
/// `collect(x.name)` renamed the column out from under its own consumer, and the
/// query died with `No field named "collect(startNode(e).name)"`. It failed for
/// the directed case too, which had shipped — the directed fix's tests never put
/// an endpoint call under an aggregate, so nothing looked.
#[tokio::test]
async fn an_aggregate_over_a_directed_endpoint_keeps_its_output_name() {
    let db = aggregate_fixture().await;
    let r = db
        .session()
        .query("MATCH (x:P)-[e:KNOWS]->(y:P) RETURN collect(startNode(e).name) AS names")
        .await
        .unwrap();
    let Value::List(items) = &r.rows()[0].values()[0] else {
        panic!("expected a list, got {:?}", r.rows()[0].values()[0]);
    };
    let mut names: Vec<String> = items
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect();
    names.sort();
    // The two edges are `b -> a` and `a -> c`, so their start nodes are `a` and `b`.
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}

/// The same undirected guarantee on a **schemaless** graph.
///
/// A declared schema plans to `GraphTraverseExec`; an undeclared one plans to
/// `GraphTraverseMainByType`. They are two separate single-hop operators, so a
/// fix applied to one is not a fix for the other — and the failure mode of a
/// missed operator is a null orientation, which reads as "backwards" rather
/// than as an error. Every fixture above declares its schema, so none of them
/// would notice.
#[tokio::test]
async fn schemaless_undirected_endpoints_do_not_depend_on_the_anchor() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    async fn ends(db: &Uni, name: &str) -> Vec<Value> {
        let r = db
            .session()
            .query(&format!(
                "MATCH (x:P {{name:'{name}'}})-[e:KNOWS]-(y:P) \
                 RETURN startNode(e).name AS s, endNode(e).name AS t"
            ))
            .await
            .unwrap();
        r.rows()[0].values().to_vec()
    }
    let from_a = ends(&db, "a").await;
    let from_b = ends(&db, "b").await;
    assert_eq!(from_a, from_b, "endpoints changed with the anchor");
    assert_eq!(from_a[0], Value::String("a".to_string()));
    assert_eq!(from_a[1], Value::String("b".to_string()));
}
