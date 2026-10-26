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
    let sm = SchemaManager::load(&path.join("schema.json"))
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

/// Mirrors `STRUCT_ONLY_SENTINEL` (`pub(crate)`): build the entity struct with
/// its identity and the properties named elsewhere, not the whole schema.
const STRUCT_ONLY: &str = "__set_struct__";

/// A CREATE over a matched endpoint needs only the endpoint's identity (#312).
///
/// The writer reuses a bound node by its vid, so the endpoint must reach it as
/// a struct column — but marking it `"*"` carried every property, `_all_props`
/// included, through every join beneath the write. One 200 KB property became
/// ~2.4 MB per row and a CREATE over a few hundred rows exhausted a 1 GiB pool.
#[tokio::test]
async fn a_create_endpoint_is_struct_only() {
    let p = planner().await;
    for q in [
        "MATCH (a:Person) CREATE (a)-[:WORKS_AT]->(:Company)",
        "MATCH (a:Person), (b:Company) CREATE (a)-[:WORKS_AT]->(b)",
        "MATCH (a:Person) CREATE (a)-[:WORKS_AT]->(c:Company) RETURN c.name",
    ] {
        let props = collected(&p, q);
        let a = props.get("a").expect("`a` must be collected");
        assert!(
            a.contains(STRUCT_ONLY) && !a.contains("*"),
            "{q}: `a` is only written through, so it must be struct-only — got {a:?}"
        );
    }
}

/// The MERGE twin of the above. MERGE's match side also reads `_labels`, which
/// the struct-only struct carries.
#[tokio::test]
async fn a_merge_endpoint_is_struct_only() {
    let p = planner().await;
    for q in [
        "MATCH (a:Person), (b:Company) MERGE (a)-[:WORKS_AT]->(b)",
        "MATCH (a:Person) MERGE (a)-[:WORKS_AT]->(:Company {name: 'x'})",
    ] {
        let props = collected(&p, q);
        let a = props.get("a").expect("`a` must be collected");
        assert!(
            a.contains(STRUCT_ONLY) && !a.contains("*"),
            "{q}: `a` is only written through, so it must be struct-only — got {a:?}"
        );
    }
}

/// `ON MATCH SET a.name = … a.age …` reads `age`: it must be collected beside
/// the struct-only marker, or the SET would compute from a missing property.
#[tokio::test]
async fn a_merge_set_keeps_the_properties_it_reads() {
    let p = planner().await;
    let props = collected(
        &p,
        "MATCH (a:Person), (b:Company) MERGE (a)-[:WORKS_AT]->(b) \
         ON MATCH SET a.name = toString(a.age + 1)",
    );
    let a = props.get("a").expect("`a` must be collected");
    assert!(
        a.contains("age") && a.contains(STRUCT_ONLY),
        "`a.age` is read by ON MATCH SET — got {a:?}"
    );
}

/// A named path is built from the row's element values, so every element of
/// it stays whole — for MERGE and CREATE alike. The CREATE queries also pin
/// that the planner brings a CREATE path variable into scope: before it did,
/// `CREATE p = … RETURN p` failed to plan with `UndefinedVariable`.
#[tokio::test]
async fn a_named_write_path_keeps_its_elements_wide() {
    let p = planner().await;
    for q in [
        "MATCH (a:Person), (b:Company) MERGE p = (a)-[:WORKS_AT]->(b) RETURN length(p)",
        "MATCH (a:Person) MERGE p = (a)-[:WORKS_AT]->(:Company) RETURN length(p)",
        "MATCH (a:Person), (b:Company) CREATE p = (a)-[:WORKS_AT]->(b) RETURN length(p)",
        "MATCH (a:Person) CREATE p = (a)-[:WORKS_AT]->(:Company) RETURN p",
    ] {
        let props = collected(&p, q);
        let a = props.get("a").expect("`a` must be collected");
        assert!(
            a.contains("*"),
            "{q}: `a` is an element of the named path `p`, so it must stay wide — got {a:?}"
        );
    }
}

/// A subquery body's `RETURN *` can export an outer entity whole without naming
/// it, so the struct-only marker must not narrow it.
#[tokio::test]
async fn a_subquery_wildcard_widens_a_create_endpoint() {
    let p = planner().await;
    let props = collected(
        &p,
        "MATCH (a:Person) CREATE (a)-[:WORKS_AT]->(:Company) \
         RETURN COLLECT { MATCH (a)-[:KNOWS]->(x:Person) RETURN * } AS xs",
    );
    let a = props.get("a").expect("`a` must be collected");
    assert!(
        a.contains("*"),
        "a subquery `RETURN *` may export `a` whole — got {a:?}"
    );
}
