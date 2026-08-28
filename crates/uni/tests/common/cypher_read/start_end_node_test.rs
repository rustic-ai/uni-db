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

/// Returning the *whole* endpoint of an undirected relationship is not
/// supported, and fails loudly.
///
/// The rewrite produces a `CASE` whose two branches are node structs, which the
/// expression compiler cannot yet unify. Pinned as a test rather than left
/// undocumented because the failure mode that matters is the other one: if this
/// ever starts returning a row, it must return the *right* endpoint and not a
/// null-filled stand-in. A test that asserts the error is what makes that
/// transition visible instead of silent.
#[tokio::test]
async fn whole_endpoint_of_an_undirected_relationship_errors_rather_than_guessing() {
    let db = fixture().await;
    let err = db
        .session()
        .query("MATCH (x:P)-[e:KNOWS]-(y:P) RETURN startNode(e) AS s")
        .await
        .expect_err("a CASE over two node structs is not supported yet");
    let msg = err.to_string();
    assert!(
        msg.contains("Struct") || msg.contains("not implemented"),
        "expected an unsupported-shape error, got: {msg}"
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
