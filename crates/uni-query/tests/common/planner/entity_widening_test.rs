//! No query that reads narrow properties may mark an entity `"*"`.
//!
//! `"*"` means "materialise every declared column plus `_all_props`". It is the
//! safe default — under-materialising would be a wrong answer, over-materialising
//! is only expensive — which is exactly why it goes unnoticed: nothing fails, the
//! query just carries the whole entity.
//!
//! # Why this test exists at the plan level
//!
//! Three separate issues have been the same defect, each found by a memory abort
//! rather than by a test:
//!
//! * #62 — `id`/`elementId`/`type` unregistered, so a label-disjunction `Union`
//!   resolved different property sets per branch;
//! * #134 — `startNode`/`endNode` unregistered, pulling every column on the row;
//! * #203 — `hasLabel` unregistered, so every *labelled traversal target* pulled
//!   its whole schema plus `_all_props`. At LDBC SF1 that made IC4 request
//!   1.5 GB against a 1 GiB pool.
//!
//! Each was fixed by registering one name in `FUNCTION_SPECS`. The class was not:
//! `analyze_function_property_requirements` **fails open** — an unregistered
//! function marks every bare-variable argument `"*"` — and the planner
//! synthesises predicates of its own (`hasLabel` is one), so a new synthesis site
//! can reintroduce this at any time.
//!
//! Asserting on the *plan's* collected properties catches that regardless of
//! which function is responsible, which a list of known function names would not.
//! `collect_properties_from_plan` is the right seam: #203's wildcard was already
//! present in its output, before `reconcile_passthrough_properties` ran.

use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_query::query::planner::{QueryPlanner, collect_properties_from_plan};

/// Person/Company with a `WORKS_AT` edge — the shape #203 turned up on.
async fn planner() -> QueryPlanner {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut sm = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();

    sm.add_label("Person").unwrap();
    sm.add_property("Person", "name", DataType::String, true)
        .unwrap();
    sm.add_property("Person", "age", DataType::Int32, true)
        .unwrap();
    sm.add_label("Company").unwrap();
    sm.add_property("Company", "name", DataType::String, true)
        .unwrap();
    sm.add_property("Company", "revenue", DataType::Int64, true)
        .unwrap();
    sm.add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])
        .unwrap();
    sm.add_edge_type("WORKS_AT", vec!["Person".into()], vec!["Company".into()])
        .unwrap();

    QueryPlanner::new(sm.schema())
}

/// Mirrors `WITH_PASSTHROUGH_SENTINEL`, which is `pub(crate)` and so cannot be
/// imported here. A bare entity forwarded through a projection is tagged with it
/// and resolved to `"*"` or a narrowed struct later, by
/// `reconcile_passthrough_properties` — a step this seam does not include. If the
/// constant's value changes, this must follow.
const PASSTHROUGH: &str = "__with_passthrough__";

/// Every variable's collected property set, straight from the plan.
fn collected(
    p: &QueryPlanner,
    cypher: &str,
) -> std::collections::HashMap<String, std::collections::HashSet<String>> {
    let ast = uni_cypher::parse(cypher).unwrap_or_else(|e| panic!("parse {cypher}: {e}"));
    let plan = p.plan(ast).unwrap_or_else(|e| panic!("plan {cypher}: {e}"));
    collect_properties_from_plan(&plan)
}

/// Variables the plan marks `"*"` outright — materialise everything, no deferral.
fn widened(p: &QueryPlanner, cypher: &str) -> Vec<String> {
    let mut v: Vec<String> = collected(p, cypher)
        .into_iter()
        .filter(|(_, props)| props.contains("*"))
        .map(|(var, _)| var)
        .collect();
    v.sort();
    v
}

/// A labelled traversal target reads two properties; neither entity is returned
/// whole, so neither may be widened.
///
/// This is #203 reduced to its smallest shape. Before the fix, `hasLabel` — which
/// the planner synthesises for the `:Company` and `:Person` labels — took the
/// unknown-function fallback and marked both `a` and `b` `"*"`, even though `b`'s
/// `revenue` is never read.
#[tokio::test]
async fn a_labelled_traversal_target_is_not_widened() {
    let p = planner().await;
    let w = widened(
        &p,
        "MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a.name, b.name",
    );
    assert!(
        w.is_empty(),
        "no property of `a` or `b` is read beyond `name`, and neither is returned \
         whole, so nothing should be materialised wide — got {w:?}. A synthesised \
         predicate is taking the unknown-function fallback in \
         `analyze_function_property_requirements`; register it in FUNCTION_SPECS \
         with the treatment `hasLabel`/`startNode`/`id` have."
    );
}

/// The same, one hop deeper: the middle variable is both a traversal target and
/// the next hop's source, which is the position IC4's `post` occupies.
#[tokio::test]
async fn an_intermediate_traversal_variable_is_not_widened() {
    let p = planner().await;
    let w = widened(
        &p,
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name",
    );
    assert!(
        w.is_empty(),
        "`b` is a join vertex whose properties are never read; widening it \
         materialises a whole entity per row for nothing — got {w:?}"
    );
}

/// The control: an entity genuinely returned whole must still be a candidate to
/// stay wide, and a narrow sibling must not be.
///
/// Without this, narrowing everything unconditionally would satisfy the two tests
/// above while breaking `RETURN n`.
///
/// At this seam a returned-whole entity carries the passthrough marker rather
/// than `"*"` — the choice between wide and a narrowed struct is
/// `reconcile_passthrough_properties`'s, one step later. What matters here is
/// that the two variables are treated *differently*: `a` is deferred, `b` is
/// resolved to the one property it uses.
#[tokio::test]
async fn an_entity_returned_whole_is_not_narrowed_at_this_stage() {
    let p = planner().await;
    let props = collected(
        &p,
        "MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a, b.name",
    );

    let a = props.get("a").expect("`a` must be collected");
    assert!(
        a.contains(PASSTHROUGH) || a.contains("*"),
        "`a` is returned whole, so it must stay a candidate for wide treatment — \
         got {a:?}"
    );

    let b = props.get("b").expect("`b` must be collected");
    assert!(
        !b.contains("*") && !b.contains(PASSTHROUGH),
        "`b` is only ever read as `b.name`, so it must be resolved narrow here — \
         got {b:?}"
    );
}
