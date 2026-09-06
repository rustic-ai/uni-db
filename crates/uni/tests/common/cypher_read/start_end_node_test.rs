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
//! The shape this file once listed as open is closed: when a `WITH` drops the
//! endpoint variables from scope, `EndpointHydrateExec` pre-fetches the
//! endpoints' labels and properties and hands them to the UDF, so there is no
//! `#[ignore]` left here and no `{_vid}`-only stand-in behind it. A call that
//! reaches the UDF with neither a rewrite nor a hydration now errors rather
//! than answering with a properties-less node.

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

/// A `WITH` narrows scope to `rel`, so the endpoint variables the rewrite would
/// resolve to are gone by the time `startNode` is called.
///
/// The relationship value still carries `_src` / `_dst`; what it lacks is the
/// endpoint's *properties*. `EndpointHydrateExec` fetches them before the
/// projection and hands them to the existing UDF as extra arguments.
///
/// Both `id()` and the property are asserted, deliberately. Before this,
/// `startnode_endnode_impl` answered an unresolvable endpoint with a stand-in
/// map holding only `_vid` — so an `id()`-only assertion passes while every
/// property reads NULL, which is exactly the trade #188 refused.
#[tokio::test]
async fn start_node_after_a_with_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH ()-[e:KNOWS]->() WITH e AS rel \
             RETURN id(startNode(rel)) AS i, startNode(rel).name AS s",
        )
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(0));
    assert_eq!(r.rows()[0].values()[1], Value::String("a".to_string()));
}

/// `endNode` has to travel the same path — the stand-in failed both identically,
/// so a `startNode`-only suite bounds nothing here.
#[tokio::test]
async fn end_node_after_a_with_on_a_match_bound_relationship() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH ()-[e:KNOWS]->() WITH e AS rel \
             RETURN id(endNode(rel)) AS i, endNode(rel).name AS t",
        )
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int(1));
    assert_eq!(r.rows()[0].values()[1], Value::String("b".to_string()));
}

/// The whole node, not just one property — a hydration that returned an
/// identity would still satisfy the property assertions above if the property
/// happened to be the only one materialised.
#[tokio::test]
async fn the_whole_start_node_after_a_with_carries_its_properties() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]->() WITH e AS rel RETURN startNode(rel) AS s")
        .await
        .unwrap();
    match &r.rows()[0].values()[0] {
        Value::Node(n) => {
            assert_eq!(n.labels, vec!["P".to_string()]);
            assert_eq!(
                n.properties.get("name"),
                Some(&Value::String("a".to_string()))
            );
        }
        other => panic!("expected a Node, got {other:?}"),
    }
}

/// No traversal binding exists here at all: `r` is an element of a path's
/// relationship list, so there is no endpoint *variable* anywhere to resolve to.
/// This is the shape carrying an endpoint through `relationships(path)` that
/// projection widening could never have reached.
#[tokio::test]
async fn start_node_of_a_relationship_taken_from_a_path() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH p = ()-[:KNOWS]->() WITH relationships(p)[0] AS r \
             RETURN startNode(r).name AS s, endNode(r).name AS t",
        )
        .await
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::String("a".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::String("b".to_string()));
}

/// LDBC IC14's shape, reduced: the relationship is a *list comprehension*
/// variable over `relationships(path)`, so the endpoint has to be materialised
/// per element and stay aligned with the element it belongs to.
#[tokio::test]
async fn start_node_inside_a_list_comprehension_over_a_paths_relationships() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH p = ()-[:KNOWS]->() WITH relationships(p) AS rels \
             RETURN [x IN rels | startNode(x).name] AS s, \
                    [x IN rels | endNode(x).name] AS t",
        )
        .await
        .unwrap();
    assert_eq!(
        r.rows()[0].values()[0],
        Value::List(vec![Value::String("a".to_string())])
    );
    assert_eq!(
        r.rows()[0].values()[1],
        Value::List(vec![Value::String("b".to_string())])
    );
}

/// An undirected hop files the edge under both endpoints, so the traversal walks
/// it once from each end. `startNode` is a fact about the relationship, not about
/// how it was matched, so both rows must agree — and they must agree *after* a
/// `WITH`, where the `_fwd` rewrite no longer applies and the hydration reads
/// `_src` off the relationship instead.
#[tokio::test]
async fn undirected_start_node_after_a_with_is_the_same_on_both_rows() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]-() WITH e AS rel RETURN startNode(rel).name AS s")
        .await
        .unwrap();
    let names: Vec<Value> = r.rows().iter().map(|x| x.values()[0].clone()).collect();
    assert_eq!(names.len(), 2, "the edge is walked from each end");
    assert_eq!(
        names,
        vec![
            Value::String("a".to_string()),
            Value::String("a".to_string())
        ],
        "startNode is the edge's stored tail on both rows"
    );
}

/// The relationship *value* an undirected match yields must also describe the
/// edge the way it is stored.
///
/// This failed independently of `startNode`: the edge struct took `_src`/`_dst`
/// from the traversal's own source and target, which for an undirected hop are
/// whichever end the row matched from. The same edge came back as `a->b` on one
/// row and `b->a` on the other, with no error — a fabricated relationship.
#[tokio::test]
async fn an_undirected_match_does_not_reverse_the_relationship_it_returns() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:KNOWS]-() RETURN e AS r")
        .await
        .unwrap();
    let edges: Vec<(u64, u64)> = r
        .rows()
        .iter()
        .map(|row| match &row.values()[0] {
            Value::Edge(e) => (e.src.as_u64(), e.dst.as_u64()),
            other => panic!("expected an Edge, got {other:?}"),
        })
        .collect();
    assert_eq!(edges.len(), 2, "the edge is walked from each end");
    assert_eq!(
        edges,
        vec![(0, 1), (0, 1)],
        "the edge is a->b; matching it backwards must not report b->a"
    );
}

/// `a` -[:LIKES]-> `b`, with no `CREATE EDGE TYPE` — the schemaless path.
///
/// A type with no DDL is planned by `plan_traverse_main_by_type` against the
/// main edges table, which is a different planner path from the schema'd
/// `Traverse` the fixture above exercises.
async fn schemaless_fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:LIKES]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// The same guarantee as the schema'd test above, on the schemaless path.
///
/// The fix for the schema'd path pushed `_fwd` into the traversal's requested
/// edge properties so the structural projection could put `_src`/`_dst` back the
/// way the edge is stored. `plan_traverse_main_by_type` still called that same
/// projection but never requested the column, so the projection silently took
/// its no-`_fwd` fallback — raw traversal order — and the defect survived
/// untouched on a path no test covered (#193).
///
/// Both tests are needed: neither path can stand in for the other, and the
/// schema'd one passes while this shape is broken.
#[tokio::test]
async fn a_schemaless_undirected_match_does_not_reverse_the_relationship_it_returns() {
    let db = schemaless_fixture().await;
    let r = db
        .session()
        .query("MATCH ()-[e:LIKES]-() RETURN e AS r")
        .await
        .unwrap();
    let edges: Vec<(u64, u64)> = r
        .rows()
        .iter()
        .map(|row| match &row.values()[0] {
            Value::Edge(e) => (e.src.as_u64(), e.dst.as_u64()),
            other => panic!("expected an Edge, got {other:?}"),
        })
        .collect();
    assert_eq!(edges.len(), 2, "the edge is walked from each end");
    let (a, b) = (edges[0], edges[1]);
    assert_eq!(
        a, b,
        "the same edge came back with its endpoints reversed: {a:?} then {b:?}"
    );
}

/// A relationship yielded by a pattern comprehension keeps its stored direction.
///
/// Third path to the same guarantee. Comprehensions build their edge column in
/// `build_edge_entity_column` rather than through the traversal's structural
/// projection, so neither of the two tests above constrains it. The anchor is
/// `b`, the edge's *head*, so a comprehension that pairs src/dst with the walk
/// order rather than the stored orientation reports `b->a` (#193).
#[tokio::test]
async fn a_comprehension_relationship_keeps_its_stored_direction() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (a:P {name:'b'}) RETURN [ (a)-[e:KNOWS]-(x) | e ] AS es")
        .await
        .unwrap();
    let edges: Vec<(u64, u64)> = match &r.rows()[0].values()[0] {
        Value::List(items) => items
            .iter()
            .map(|v| match v {
                Value::Edge(e) => (e.src.as_u64(), e.dst.as_u64()),
                other => panic!("expected an Edge, got {other:?}"),
            })
            .collect(),
        other => panic!("expected a List, got {other:?}"),
    };
    assert_eq!(edges.len(), 1, "b has exactly one KNOWS edge");
    assert_eq!(
        edges[0],
        (0, 1),
        "the edge is stored a->b; anchoring the comprehension on b must not report b->a"
    );
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

/// The hydration is an operator, so pin that it actually runs.
///
/// Bag-comparing the rows cannot tell "the endpoint was hydrated" from "the
/// endpoint happened to be in scope"; only the plan shape can. Without this the
/// operator could stop being emitted and every assertion above would still pass
/// through the older rewrite path — which is exactly what the plan-shape gate
/// exists to prevent.
#[tokio::test]
async fn the_post_with_endpoint_query_runs_the_hydration_operator() {
    let db = fixture().await;
    let session = db.session();
    crate::plan_shape::assert_plan_uses(
        &session,
        "MATCH ()-[e:KNOWS]->() WITH e AS rel RETURN startNode(rel).name AS s",
        "EndpointHydrateExec",
    )
    .await;
}

/// And that it is *not* emitted when the endpoints are still in scope — the
/// rewrite handles those without any lookup, and paying for a batched property
/// fetch there would be a silent regression in the common case.
#[tokio::test]
async fn an_in_scope_endpoint_does_not_pay_for_hydration() {
    let db = fixture().await;
    let session = db.session();
    crate::plan_shape::assert_plan_avoids(
        &session,
        "MATCH (a)-[e:KNOWS]->(b) RETURN startNode(e).name AS s",
        "EndpointHydrateExec",
    )
    .await;
}

/// LDBC SNB Interactive **IC14**, the query this issue was filed for, run in
/// full against a miniature graph.
///
/// IC14 is the reason #187 mattered: it correlates a pattern comprehension with
/// `a.id = startNode(r).id AND b.id = endNode(r).id`, where `r` is a list
/// comprehension variable over `relationships(path)`. No endpoint variable
/// exists anywhere in that scope, which is why carrying variables through the
/// projection could never have reached it.
///
/// The fixture is deliberately tiny; the assertion is that the query *plans and
/// executes*, and that the weight it computes reflects the one qualifying
/// comment path rather than coming back zero because every endpoint was NULL.
#[tokio::test]
async fn ldbc_ic14_plans_and_executes() {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    for ddl in [
        "CREATE LABEL Person (id INT)",
        "CREATE LABEL Comment (id INT)",
        "CREATE LABEL Post (id INT)",
        "CREATE EDGE TYPE KNOWS FROM Person TO Person",
        "CREATE EDGE TYPE REPLY_OF FROM Comment TO Post",
    ] {
        tx.execute(ddl).await.unwrap();
    }
    tx.commit().await.unwrap();
    // LDBC's HAS_CREATOR runs from *either* message type to a Person, which the
    // single-source DDL form cannot express — and without the Post source the
    // weight pattern below cannot match at all.
    db.schema()
        .edge_type("HAS_CREATOR", &["Comment", "Post"], &["Person"])
        .apply()
        .await
        .unwrap();

    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:Person {id:1}), (:Person {id:2})")
        .await
        .unwrap();
    tx.execute("CREATE (:Comment {id:10}), (:Post {id:20})")
        .await
        .unwrap();
    tx.execute("MATCH (a:Person {id:1}), (b:Person {id:2}) CREATE (a)-[:KNOWS]->(b)")
        .await
        .unwrap();
    // One qualifying weight path: comment 10 by person 1, replying to post 20,
    // which person 2 created. That makes the pattern comprehension match with
    // a.id = 1 and b.id = 2 — the endpoints of the KNOWS relationship `r`.
    tx.execute("MATCH (c:Comment {id:10}), (p:Person {id:1}) CREATE (c)-[:HAS_CREATOR]->(p)")
        .await
        .unwrap();
    tx.execute("MATCH (c:Comment {id:10}), (p:Post {id:20}) CREATE (c)-[:REPLY_OF]->(p)")
        .await
        .unwrap();
    tx.execute("MATCH (p:Post {id:20}), (q:Person {id:2}) CREATE (p)-[:HAS_CREATOR]->(q)")
        .await
        .unwrap();
    // A second, NON-qualifying comment path between two other people. The
    // pattern matches it too, so the `WHERE` has to evaluate
    // `a.id = startNode(r).id` for a row whose `a` and `b` are *not* the
    // relationship's endpoints — which is the ordinary case at any real scale,
    // and the one a single qualifying pair hides completely.
    tx.execute("CREATE (:Person {id:3}), (:Person {id:4})")
        .await
        .unwrap();
    tx.execute("CREATE (:Comment {id:11}), (:Post {id:21})")
        .await
        .unwrap();
    tx.execute("MATCH (c:Comment {id:11}), (p:Person {id:3}) CREATE (c)-[:HAS_CREATOR]->(p)")
        .await
        .unwrap();
    tx.execute("MATCH (c:Comment {id:11}), (p:Post {id:21}) CREATE (c)-[:REPLY_OF]->(p)")
        .await
        .unwrap();
    tx.execute("MATCH (p:Post {id:21}), (q:Person {id:4}) CREATE (p)-[:HAS_CREATOR]->(q)")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let query = "
MATCH path = allShortestPaths((person1:Person { id: 1 })-[:KNOWS*0..]-(person2:Person { id: 2 }))
WITH collect(path) as paths
UNWIND paths as path
WITH path, relationships(path) as rels_in_path
WITH
    [n in nodes(path) | n.id ] as personIdsInPath,
    [r in rels_in_path |
        reduce(w=0.0, v in [
            (a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b:Person)
            WHERE
                (a.id = startNode(r).id and b.id=endNode(r).id) OR (a.id=endNode(r).id and b.id=startNode(r).id)
            | 1.0] | w+v)
    ] as weight1
WITH
    personIdsInPath,
    reduce(w=0.0,v in weight1| w+v) as w1
RETURN personIdsInPath, w1 AS pathWeight
ORDER BY pathWeight desc";

    let r = db
        .session()
        .query(query)
        .await
        .expect("IC14 must plan and execute");
    assert!(!r.rows().is_empty(), "IC14 returned no rows");
    assert_eq!(
        r.columns(),
        &["personIdsInPath".to_string(), "pathWeight".to_string()]
    );
    // The shortest path is person1 -> person2, so the id list is [1, 2].
    assert_eq!(
        r.rows()[0].values()[0],
        Value::List(vec![Value::Int(1), Value::Int(2)])
    );
    // The weight, which is the half that actually exercises `startNode(r)`.
    // Without the qualifying comment path above, the pattern comprehension
    // matches nothing, the `reduce` never evaluates its body, and this test
    // passes without ever calling the endpoint code it exists to cover.
    assert_eq!(
        r.rows()[0].values()[1],
        Value::Float(1.0),
        "one qualifying comment path contributes weight 1.0"
    );
}

/// A returned relationship value carries the stored orientation, not the
/// traversal's.
///
/// `Traverse` encodes the arrow as `(source_variable, direction)`, where the
/// source is the end the row *walked from* — for an `Incoming` hop that is the
/// arrow's **head**. `endpoints_for_direction` knows this and is what makes
/// `startNode`/`endNode` correct above. The edge struct built for `RETURN r` is
/// a separate, fourth derivation of the same fact, and it read traversal order
/// directly: `MATCH (b)<-[r]-(a) RETURN r` reported `r.src = b`.
///
/// # Why this was not caught
///
/// Every test in this file asks `startNode(r)` / `endNode(r)`, which take the
/// planner-rewrite path and were always right. Nothing asserted on the
/// relationship **value**. Two derivations of one fact, one tested — the same
/// shape that hid #193.
///
/// The stakes rose with `reversed_for_bound_anchor`, which rewrites a pattern
/// written from its unbound end into the opposite direction for performance. An
/// `Outgoing` pattern with its far end bound now reaches this code as
/// `Incoming`, so a query that was correct-but-slow became fast-and-wrong.
/// Both spellings are asserted below for that reason.
#[tokio::test]
async fn a_returned_relationship_keeps_its_stored_direction() {
    let db = fixture().await;
    let session = db.session();

    // Written Incoming. `b` is the arrow's head, so it must be `dst`.
    let incoming = session
        .query(
            "MATCH (y:P {name:'b'})<-[r:KNOWS]-(x:P) \
             RETURN startNode(r).name AS s, endNode(r).name AS e",
        )
        .await
        .unwrap();
    assert_eq!(incoming.rows()[0].values()[0], Value::String("a".into()));
    assert_eq!(incoming.rows()[0].values()[1], Value::String("b".into()));

    // The same edge as a *value*. This is the derivation that was wrong.
    let as_value = session
        .query("MATCH (y:P {name:'b'})<-[r:KNOWS]-(x:P) RETURN r")
        .await
        .unwrap();
    let rel = format!("{:?}", as_value.rows()[0].values()[0]);

    // Resolve the two vids so the assertion names nodes rather than integers.
    let ids = session
        .query("MATCH (n:P) RETURN n.name AS name, id(n) AS vid ORDER BY name")
        .await
        .unwrap();
    let vid_of = |name: &str| -> i64 {
        ids.rows()
            .iter()
            .find(|r| r.values()[0] == Value::String(name.into()))
            .and_then(|r| match r.values()[1] {
                Value::Int(v) => Some(v),
                _ => None,
            })
            .expect("vid")
    };
    let (a, b) = (vid_of("a"), vid_of("b"));

    assert!(
        rel.contains(&format!("src: Vid({a})")),
        "the arrow runs a->b, so src must be `a`; got {rel}"
    );
    assert!(
        rel.contains(&format!("dst: Vid({b})")),
        "the arrow runs a->b, so dst must be `b`; got {rel}"
    );

    // The Outgoing spelling with the far end bound: `reversed_for_bound_anchor`
    // turns this into an Incoming plan, so it exercises the same code path by a
    // different route and must agree.
    let reversed = session
        .query(
            "MATCH (y:P {name:'b'}) WITH DISTINCT y \
             MATCH (x:P)-[r:KNOWS]->(y) RETURN r",
        )
        .await
        .unwrap();
    let rel2 = format!("{:?}", reversed.rows()[0].values()[0]);
    assert!(
        rel2.contains(&format!("src: Vid({a})")) && rel2.contains(&format!("dst: Vid({b})")),
        "an anchor-reversed plan must report the same orientation as the \
         spelling it was rewritten from; got {rel2}"
    );
}

// ---------------------------------------------------------------------------
// OPTIONAL MATCH (#243, face 3)
// ---------------------------------------------------------------------------
//
// The rewrite above resolves `startNode(r)` to an endpoint *variable*, so it
// never consults `r`. On an OPTIONAL hop the endpoint the pattern hangs off is
// the anchor from the enclosing scope, and it stays bound on a row that did not
// match — so the call returned a node where Cypher requires null. Which side
// leaks follows the direction: `startNode` on an outgoing hop, `endNode` on an
// incoming one, so a test written only for the outgoing case would miss half of
// it.
//
// Nothing in this file previously used OPTIONAL MATCH at all, which is why the
// suite could not see this. The fix marks the binding optional and guards the
// rewrite on `r._eid`; these assert both halves — null on the miss row, and the
// real endpoint still returned on the row that matched.

/// `a`-[:KNOWS]->`b`, plus `lonely` with no relationships at all.
///
/// One query over this fixture produces both a matching and a missing row, so a
/// guard that over-nulls fails just as loudly as one that leaks.
async fn optional_fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'}), (:P {name:'lonely'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// `[(anchor, start, end)]`, ordered by anchor, for one OPTIONAL hop spelling.
async fn optional_endpoints(db: &Uni, pattern: &str) -> Vec<(String, Value, Value)> {
    let query = format!(
        "MATCH (n:P) OPTIONAL MATCH {pattern} \
         RETURN n.name AS anchor, startNode(r).name AS s, endNode(r).name AS e \
         ORDER BY anchor"
    );
    db.session()
        .query(&query)
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|row| {
            let vals = row.values();
            let Value::String(anchor) = &vals[0] else {
                panic!("anchor must be a string, got {:?}", vals[0]);
            };
            (anchor.clone(), vals[1].clone(), vals[2].clone())
        })
        .collect()
}

#[tokio::test]
async fn optional_match_outgoing_nulls_both_endpoints_on_a_miss() {
    let db = optional_fixture().await;
    let rows = optional_endpoints(&db, "(n)-[r:KNOWS]->(m:P)").await;

    // 'a' matches: both endpoints are real.
    assert_eq!(
        rows[0],
        (
            "a".to_string(),
            Value::String("a".to_string()),
            Value::String("b".to_string())
        ),
        "the matching row must keep its endpoints"
    );
    // 'b' has no OUTGOING edge, 'lonely' has none at all. `startNode` is the
    // side that leaked here: it resolves to the still-bound anchor `n`.
    for row in &rows[1..] {
        assert_eq!(
            (&row.1, &row.2),
            (&Value::Null, &Value::Null),
            "an unmatched OPTIONAL hop must null both endpoints, got {row:?}"
        );
    }
}

#[tokio::test]
async fn optional_match_incoming_nulls_both_endpoints_on_a_miss() {
    let db = optional_fixture().await;
    let rows = optional_endpoints(&db, "(n)<-[r:KNOWS]-(m:P)").await;

    // Only 'b' has an incoming edge, and the endpoints stay in stored
    // orientation regardless of which way the pattern was walked.
    let b = rows.iter().find(|r| r.0 == "b").expect("row for 'b'");
    assert_eq!(
        (&b.1, &b.2),
        (
            &Value::String("a".to_string()),
            &Value::String("b".to_string())
        ),
        "the matching row must report stored orientation"
    );
    // `endNode` is the leaking side for an incoming hop -- the mirror of the
    // outgoing case, and the half an outgoing-only test cannot reach.
    for row in rows.iter().filter(|r| r.0 != "b") {
        assert_eq!(
            (&row.1, &row.2),
            (&Value::Null, &Value::Null),
            "an unmatched OPTIONAL hop must null both endpoints, got {row:?}"
        );
    }
}

#[tokio::test]
async fn optional_match_undirected_nulls_both_endpoints_on_a_miss() {
    let db = optional_fixture().await;
    let rows = optional_endpoints(&db, "(n)-[r:KNOWS]-(m:P)").await;

    // An undirected hop takes the `_fwd` CASE. Both 'a' and 'b' match it, from
    // opposite ends, and both must report the edge as stored.
    for anchor in ["a", "b"] {
        let row = rows.iter().find(|r| r.0 == anchor).expect("matching row");
        assert_eq!(
            (&row.1, &row.2),
            (
                &Value::String("a".to_string()),
                &Value::String("b".to_string())
            ),
            "an undirected match must not depend on the anchor, got {row:?}"
        );
    }
    // On a miss `_fwd` is null, the CASE takes its ELSE, and the still-bound
    // endpoint came back -- so the guard has to wrap the CASE, not one branch.
    let lonely = rows.iter().find(|r| r.0 == "lonely").expect("row 'lonely'");
    assert_eq!(
        (&lonely.1, &lonely.2),
        (&Value::Null, &Value::Null),
        "an unmatched undirected OPTIONAL hop must null both endpoints"
    );
}

#[tokio::test]
async fn id_of_an_unmatched_optional_endpoint_is_null() {
    let db = optional_fixture().await;
    let rows = db
        .session()
        .query(
            "MATCH (n:P) OPTIONAL MATCH (n)-[r:KNOWS]->(m:P) \
             RETURN n.name AS anchor, id(startNode(r)) AS sid ORDER BY anchor",
        )
        .await
        .unwrap();

    // `id()` reads identity out of the guarded rewrite's struct. Asserting it
    // separately matters: the guard changes the expression's shape, and an
    // accessor that did not understand that shape would fail here while every
    // property test above still passed.
    let ids: Vec<Value> = rows.rows().iter().map(|r| r.values()[1].clone()).collect();
    assert!(
        matches!(ids[0], Value::Int(_)),
        "the matching row's endpoint must still have an id, got {:?}",
        ids[0]
    );
    for id in &ids[1..] {
        assert_eq!(
            id,
            &Value::Null,
            "id() of an unmatched OPTIONAL endpoint must be null"
        );
    }
}
