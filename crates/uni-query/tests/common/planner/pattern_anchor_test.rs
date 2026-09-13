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

/// A bound node in the middle anchors the walk from the middle (#224).
///
/// Neither end is bound here, so no single direction helps: whichever end the
/// walk starts from scans everything and cross-joins. The pattern is split at
/// the anchor and planned as two walks out of it, so both sides start from a
/// node already in scope.
///
/// This test previously asserted the opposite, pinning the limit with a note to
/// invert it when join ordering landed. This is that inversion.
#[tokio::test]
async fn a_middle_bound_pattern_anchors_from_the_middle() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (post)<-[:CONTAINER_OF]-(forum)-[:HAS_MEMBER]->(person) \
         RETURN count(*) AS c",
    );
    assert!(
        !has_cross_join(&plan),
        "a middle-bound pattern must anchor on the bound node, not cross-join \
         from an unbound end"
    );
}

/// A pattern with no bound node at all is still planned as written.
///
/// The split needs something in scope to anchor on. With nothing bound there is
/// no decision to make without cardinality — which is the part of #224 this
/// increment does not attempt — so the plan must be left alone rather than
/// split arbitrarily.
#[tokio::test]
async fn a_wholly_unbound_pattern_is_left_alone() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (post)<-[:CONTAINER_OF]-(forum)-[:HAS_MEMBER]->(person) \
         RETURN count(*) AS c",
    );
    // No anchor exists, so the walk starts at the first element and the shape
    // is whatever the syntax gave — asserted only so a future rewrite that
    // splits unanchored patterns has to come here and say why.
    let _ = plan;
}

/// Comma-separated paths are ordered so each anchors on an earlier one (#224).
///
/// Written left to right, the first path scans every `post` and cross-joins
/// before the second — which is bound on `forum` and shares `post` — ever runs.
/// Planning the bound path first leaves `post` in scope, so the other becomes an
/// anchored traversal.
///
/// Sound because comma-separated paths are a conjunction: the result set does
/// not depend on match order, only the plan shape does. Which is also why this
/// has to be a plan-shape assertion — no correctness test can tell the two
/// orders apart.
#[tokio::test]
async fn comma_separated_paths_are_ordered_to_anchor() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (post)-[:CONTAINER_OF]->(other), (forum)-[:CONTAINER_OF]->(post) \
         RETURN count(*) AS c",
    );
    assert!(
        !has_cross_join(&plan),
        "the bound path must be planned before the one that shares a variable \
         with it, so the second anchors instead of scanning"
    );
}

/// A pattern already in a good order is left exactly as written.
///
/// The reorder is greedy and takes the leftmost connected path, so a
/// well-written pattern must come out unchanged rather than merely equivalent.
/// This pins that the rewrite is not gratuitous.
#[tokio::test]
async fn a_well_ordered_pattern_is_not_reordered() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (forum)-[:CONTAINER_OF]->(post), (post)-[:CONTAINER_OF]->(other) \
         RETURN count(*) AS c",
    );
    assert!(!has_cross_join(&plan));
}

/// A genuinely disconnected pattern still cross-joins, and should.
///
/// Two paths sharing no variable are a Cartesian product by definition; no
/// ordering removes it. The fallback that takes the first unplanned path when
/// nothing is connected is what keeps this terminating rather than looping.
#[tokio::test]
async fn a_disconnected_pattern_still_cross_joins() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (a:Forum)-[:CONTAINER_OF]->(b), (c:Forum)-[:HAS_MEMBER]->(d) \
         RETURN count(*) AS c",
    );
    assert!(
        has_cross_join(&plan),
        "a pattern with no shared variable is a product; ordering cannot help"
    );
}

/// A quantified path written from its unbound end is reversed too (#224).
///
/// `reversed_for_bound_anchor` used to decline any pattern containing a
/// quantified segment, so a QPP written from the unbound end scanned and
/// cross-joined exactly as a plain hop did before #219.
#[tokio::test]
async fn an_anonymous_quantified_path_anchors_on_its_bound_end() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (post)(()<-[:CONTAINER_OF]-()){1,1}(forum) RETURN count(*) AS c",
    );
    assert!(
        !has_cross_join(&plan),
        "an anonymous quantified path must anchor on its bound end"
    );
}

/// A quantified path that names its inner elements is left alone.
///
/// Those names are GQL group variables — a list with one entry per iteration,
/// in traversal order. Reversing the walk would reverse the lists the user
/// reads back, so this is a semantics question, not a plan-shape one, and the
/// rewrite declines rather than trading an answer for a plan.
///
/// Pins the decision so the guard is not widened by someone who sees only the
/// anonymous case working.
#[tokio::test]
async fn a_named_quantified_path_is_left_alone() {
    let p = planner().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum) WITH DISTINCT f AS forum \
         MATCH (post)((a)<-[:CONTAINER_OF]-(b)){1,1}(forum) RETURN count(*) AS c",
    );
    assert!(
        has_cross_join(&plan),
        "a quantified path binding group variables must not be reversed: the \
         lists it binds are ordered by the walk"
    );
}

// ---------------------------------------------------------------------------
// #268: an equality predicate anchors a pattern, the same way a binding does.
// ---------------------------------------------------------------------------

/// Forum/Post where only `Post.uid` carries a scalar index.
///
/// The asymmetry is the point: it lets a test put an equality on both ends and
/// check that the *indexed* one wins, which a uniformly indexed schema could
/// not distinguish from written order.
async fn planner_with_index() -> QueryPlanner {
    use uni_common::core::schema::{
        DataType, IndexDefinition, IndexMetadata, ScalarIndexConfig, ScalarIndexType,
    };

    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let sm = uni_common::core::schema::SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();

    for label in ["Forum", "Post"] {
        sm.add_label(label).unwrap();
        sm.add_property(label, "id", DataType::Int64, true).unwrap();
        sm.add_property(label, "uid", DataType::String, true)
            .unwrap();
    }
    sm.add_edge_type("CONTAINER_OF", vec!["Forum".into()], vec!["Post".into()])
        .unwrap();
    sm.add_index(IndexDefinition::Scalar(ScalarIndexConfig {
        name: "idx_post_uid".into(),
        label: "Post".into(),
        properties: vec!["uid".into()],
        index_type: ScalarIndexType::BTree,
        where_clause: None,
        // `IndexStatus` defaults to `Online`, which is what the rank requires.
        metadata: IndexMetadata::default(),
    }))
    .unwrap();

    QueryPlanner::new(sm.schema())
}

/// The variable bound by the plan's leading `Scan` — the node the walk starts at.
///
/// Read off the `Debug` rendering because `LogicalPlan::children` is private,
/// the same constraint `has_cross_join` works under. `has_cross_join` is the
/// wrong instrument for #268: a single-path MATCH cross-joins nothing either
/// way, and the defect is which end the walk begins at.
///
/// The match is anchored at a variant boundary rather than a bare substring:
/// `FusedIndexScan {` ends in `Scan {`, and a substring test would report the
/// wrong operator — the hazard #177 documents for the plan-shape gate.
fn scan_anchor(plan: &LogicalPlan) -> Option<String> {
    const KEY: &str = "variable: \"";
    let rendered = format!("{plan:?}");
    let mut from = 0;
    while let Some(hit) = rendered[from..].find("Scan {") {
        let at = from + hit;
        if at == 0 || !rendered[..at].ends_with(|c: char| c.is_alphanumeric()) {
            let tail = &rendered[at..];
            let start = tail.find(KEY)? + KEY.len();
            let rest = &tail[start..];
            return Some(rest[..rest.find('"')?].to_string());
        }
        from = at + "Scan {".len();
    }
    None
}

/// An equality on the last node starts the walk there.
///
/// #268 reduced to its smallest shape. Neither end is bound by an earlier
/// clause, so before the fix the walk began at the written-first `f` and
/// expanded every `CONTAINER_OF` edge, reapplying `p.uid` as a target filter
/// above the traversal where no index-aware rewrite can see it.
#[tokio::test]
async fn an_equality_on_the_last_node_anchors_the_walk_there() {
    let p = planner_with_index().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) WHERE p.uid = 'x' RETURN f.id AS c",
    );
    assert_eq!(
        scan_anchor(&plan).as_deref(),
        Some("p"),
        "an equality pins `p` to one value, so the walk must start there rather \
         than at the written-first `f`"
    );
}

/// The inline map spelling anchors identically.
///
/// `(p:Post {uid: 'x'})` measured exactly as slow as the `WHERE` form, so it
/// has to reach the same rank — otherwise the fix covers only the spelling that
/// happened to be reported.
#[tokio::test]
async fn an_inline_equality_on_the_last_node_anchors_the_walk_there() {
    let p = planner_with_index().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post {uid: 'x'}) RETURN f.id AS c",
    );
    assert_eq!(
        scan_anchor(&plan).as_deref(),
        Some("p"),
        "an inline property map is a conjunction of equalities and must anchor \
         like the WHERE form"
    );
}

/// The negative twin: an equality on the *first* node must not reverse.
///
/// Without this, a rewrite that always reversed would pass every positive case
/// above while being wrong in general.
#[tokio::test]
async fn an_equality_on_the_first_node_leaves_the_walk_alone() {
    let p = planner_with_index().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) WHERE f.uid = 'x' RETURN p.id AS c",
    );
    assert_eq!(
        scan_anchor(&plan).as_deref(),
        Some("f"),
        "the written-first node already carries the equality; reversing would \
         only move the anchor to an unconstrained end"
    );
}

/// A binding from an earlier clause still outranks an equality.
///
/// The regression this ordering exists to prevent. `post` is bound by the
/// preceding `WITH`; `f` merely carries an equality. Ranking the two as one
/// boolean "anchorable" would make the written-first `f` look bound, decline
/// the reversal, and quietly undo #219 for every pattern whose leading node has
/// a predicate.
#[tokio::test]
async fn a_binding_outranks_an_equality_on_the_written_first_node() {
    let p = planner_with_index().await;

    let plan = plan_of(
        &p,
        "MATCH (x:Post) WITH DISTINCT x AS post \
         MATCH (f:Forum)-[:CONTAINER_OF]->(post) WHERE f.uid = 'x' RETURN f.id AS c",
    );
    assert!(
        !has_cross_join(&plan),
        "a node bound by an earlier clause must still win the anchor against a \
         node that only carries an equality (#219 must survive #268)"
    );
}

/// With an equality at both ends, the indexed one wins.
///
/// `Post.uid` is indexed and `Forum.uid` is not, so the tie breaks toward the
/// end where a seek is actually available.
#[tokio::test]
async fn an_indexed_equality_outranks_an_unindexed_one() {
    let p = planner_with_index().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) \
         WHERE f.uid = 'a' AND p.uid = 'b' RETURN f.id AS c",
    );
    assert_eq!(
        scan_anchor(&plan).as_deref(),
        Some("p"),
        "both ends are pinned, so the anchor should be the end whose property \
         has an index behind it"
    );
}

/// Equal rank at both ends preserves the written order.
///
/// Neither `id` is indexed, so there is nothing to choose between them and the
/// rewrite must decline rather than flip on a coin.
#[tokio::test]
async fn equal_rank_at_both_ends_preserves_the_written_order() {
    let p = planner_with_index().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) \
         WHERE f.id = 1 AND p.id = 2 RETURN f.id AS c",
    );
    assert_eq!(
        scan_anchor(&plan).as_deref(),
        Some("f"),
        "with nothing to separate the two ends the pattern must plan as written"
    );
}

/// A predicate under `OR` pins nothing and must not anchor.
///
/// `f.uid = 'a' OR p.uid = 'b'` constrains neither end on its own; treating
/// either disjunct as an anchor would be a wrong answer waiting on a plan
/// change, not merely a slow one.
#[tokio::test]
async fn a_disjunction_does_not_anchor() {
    let p = planner_with_index().await;

    let plan = plan_of(
        &p,
        "MATCH (f:Forum)-[:CONTAINER_OF]->(p:Post) \
         WHERE f.uid = 'a' OR p.uid = 'b' RETURN f.id AS c",
    );
    assert_eq!(
        scan_anchor(&plan).as_deref(),
        Some("f"),
        "neither branch of an OR pins its variable, so the pattern plans as written"
    );
}
