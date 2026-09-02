//! A pattern must be planned from its bound end, whichever end it is written at.
//!
//! `plan_path` walks elements left to right and scans the first node when it is
//! unbound. If the bound node is written *last*, that scan is an unlabelled
//! `ScanAll` cross-joined against the incoming rows, and the binding is only
//! reapplied as a filter above the traversal — too high for
//! `try_plan_cross_join_as_hash_join` to recover.
//!
//! Measured at LDBC SF1 before the fix, on identical rows:
//! `(forum)-[:CONTAINER_OF]->(post)` 349 ms against
//! `(post)<-[:CONTAINER_OF]-(forum)` not finishing at all. A label on the
//! unbound end does not rescue it — the cost is the cross product, not the scan
//! width — so there is no spelling a user can reach for.
//!
//! # Why this test asserts on plan shape
//!
//! The two spellings return identical answers, so no correctness test can tell
//! them apart; only the plan can. Asserting "no `CrossJoin` appears" also states
//! the property directly, rather than pinning the current node ordering, so a
//! future join-order change does not have to update it.

use tempfile::tempdir;
use uni_query::query::planner::{LogicalPlan, QueryPlanner};

/// Forum/Post/Person with the two edge types the LDBC repro uses.
async fn planner() -> QueryPlanner {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let sm = uni_common::core::schema::SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();

    for label in ["Forum", "Post", "Person"] {
        sm.add_label(label).unwrap();
        sm.add_property(label, "id", uni_common::core::schema::DataType::Int64, true)
            .unwrap();
    }
    sm.add_edge_type("CONTAINER_OF", vec!["Forum".into()], vec!["Post".into()])
        .unwrap();
    sm.add_edge_type("HAS_MEMBER", vec!["Forum".into()], vec!["Person".into()])
        .unwrap();

    QueryPlanner::new(sm.schema())
}

/// Does any node in the plan tree cross-join?
///
/// Via the `Debug` rendering because `LogicalPlan::children` is private. The
/// variant name is the stable part of that output; if `CrossJoin` is ever
/// renamed, this must follow.
fn has_cross_join(plan: &LogicalPlan) -> bool {
    format!("{plan:?}").contains("CrossJoin")
}

fn plan_of(p: &QueryPlanner, cypher: &str) -> LogicalPlan {
    let ast = uni_cypher::parse(cypher).unwrap_or_else(|e| panic!("parse {cypher}: {e}"));
    p.plan(ast).unwrap_or_else(|e| panic!("plan {cypher}: {e}"))
}

/// The same hop written from either end plans without a cross product.
///
/// This is #219 reduced to its smallest shape. `forum` is bound by the preceding
/// `WITH`; `post` is not. Written `(post)<-[:CONTAINER_OF]-(forum)` the walk used
/// to start at the unbound `post`.
#[tokio::test]
async fn a_hop_written_from_its_unbound_end_does_not_cross_join() {
    let p = planner().await;

    let bound_first = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (forum)-[:CONTAINER_OF]->(post) RETURN count(*) AS c",
    );
    assert!(
        !has_cross_join(&bound_first),
        "the already-fast spelling must stay free of a cross join"
    );

    let bound_last = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (post)<-[:CONTAINER_OF]-(forum) RETURN count(*) AS c",
    );
    assert!(
        !has_cross_join(&bound_last),
        "a pattern written from its unbound end must still be planned from the \
         bound one, not cross-joined against a scan of every vertex"
    );
}

/// A label on the unbound end must not be what rescues the plan.
///
/// Before the fix this spelling was equally unusable: the label narrows the scan
/// but the cross product remains. Asserting it here stops a future change from
/// "fixing" only the unlabelled case and leaving the labelled one behind.
#[tokio::test]
async fn a_labelled_unbound_end_does_not_cross_join_either() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (post:Post)<-[:CONTAINER_OF]-(forum) RETURN count(*) AS c",
    );
    assert!(!has_cross_join(&plan));
}

/// Reversal is declined where it cannot help, rather than applied blindly.
///
/// With the bound node in the *middle*, neither end anchors the walk — that is a
/// join-ordering problem, not an ordering one, and is out of scope. The test
/// pins the decision so the guard is not quietly widened into a rewrite that
/// reverses a path it cannot improve.
#[tokio::test]
async fn a_middle_bound_pattern_is_left_alone() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (post)<-[:CONTAINER_OF]-(forum)-[:HAS_MEMBER]->(person) \
         RETURN count(*) AS c",
    );
    assert!(
        has_cross_join(&plan),
        "documenting the v1 limit: a middle-bound pattern still cross-joins. \
         If this starts passing, join ordering landed and this test should \
         become an assertion that it does NOT cross-join."
    );
}
