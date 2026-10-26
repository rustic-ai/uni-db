// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for <https://github.com/rustic-ai/uni-db/issues/160>
//!
//! In a single Locy evaluation, `result.derived[r]` holds facts while the
//! corresponding `QUERY r` yields zero rows. `QUERY` is the documented
//! interactive entry point, so the failure mode is an ad-hoc query silently
//! reporting "nothing found" about data that does contain matches.
//!
//! ## The issue's stated trigger is wrong
//!
//! The report requires a "multi-level derivation chain whose base is recursive"
//! and states that querying the recursive rule directly does not reproduce.
//! Measured behaviour contradicts both claims:
//!
//! | shape                                      | derived | QUERY |
//! |--------------------------------------------|---------|-------|
//! | recursive rule queried directly            | 3       | **2** |
//! | one level above it                         | 1       | **0** |
//! | two levels, two IS-refs                    | 1       | **0** |
//! | two levels, one IS-ref                     | 1       | **0** |
//! | two IS-refs over a **non**-recursive base  | 1       | **0** |
//!
//! Isolating the variable shows the real trigger is neither depth, recursion,
//! nor repeated refs, but whether an `IS`-ref **introduces a variable binding
//! that the MATCH pattern does not already provide**:
//!
//! | `(r, p) IS rp` with `p` bound in MATCH     | derived 1 | QUERY 1 |
//! | `(r, p) IS rp` with `p` introduced by ref  | derived 1 | QUERY 0 |
//!
//! The recursive case only looks special because its recursive clause
//! (`WHERE (parent, p) IS role_perm`) happens to introduce `p`.
//!
//! ## Root cause (FIXED)
//!
//! `derived` and `QUERY` are served by two different engines.
//!
//! `derived` comes from the DataFusion semi-naive fixpoint's `DerivedStore`
//! (`uni/src/api/impl_locy.rs:878-911`). `QUERY` goes through a top-down SLG
//! resolver that discards the converged facts and re-derives from scratch:
//! `uni-query/src/query/df_graph/locy_query.rs:72-87` seeds a fresh `RowStore`
//! with **FOLD rules only**. The SLG path then runs the Cypher MATCH first and
//! applies `IS`-refs as a *filter* over the resulting rows
//! (`locy_delta.rs:74-108`, `:249`) — it can check an already-bound subject but
//! has no mechanism to *bind* a fresh one from a derived fact, so fresh-binding
//! refs match nothing and the row is dropped.
//!
//! The bottom-up engine implements the full language; the top-down engine
//! implemented a strictly weaker subset, and the weaker one backs `QUERY`.
//!
//! The fix is narrower than "SLG cannot bind": `is_ref_matches` conflated *not
//! bound* with *bound to NULL*, so an absent subject compared as NULL and
//! matched nothing. The subject branch now distinguishes the two, and
//! `semi_join_is_ref` binds absent subjects from the derived fact — the outer
//! loop was already a cross product, so an IS-ref becomes a generator for free.
//! The negation paths compare only KEY positions the binding plan binds: relaxing
//! them would flip `IS NOT` with an unbindable subject from keep-all to
//! keep-nothing.
//!
//! ## Test-coverage gap that let this ship
//!
//! `uni-locy-tck/tck/features/combinations/SemanticParity.feature` exists and is
//! titled "QUERY Results Must Match Derived Relations ... to catch SLG/fixpoint
//! divergences". Its scenarios cover FOLD, `IS NOT`, PROB and a three-stratum
//! chain — but none had a fresh-binding `IS`-ref, and the recursion feature
//! files contained no `QUERY` at all. The guard was built with a hole in it.
//! That is now closed by a generic parity check in the TCK's `store_result`,
//! which compares derived key tuples against QUERY rows on every scenario.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(issue_160)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// alice -> senior; senior INHERITS junior; junior GRANTS READ; senior GRANTS
/// WRITE. So alice transitively holds both permissions.
async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .label("Role")
        .property("name", DataType::String)
        .done()
        .label("Perm")
        .property("action", DataType::String)
        .done()
        .edge_type("HAS_ROLE", &["Person"], &["Role"])
        .done()
        .edge_type("INHERITS", &["Role"], &["Role"])
        .done()
        .edge_type("GRANTS", &["Role"], &["Perm"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'alice'})").await?;
    tx.execute("CREATE (:Role {name: 'junior'})").await?;
    tx.execute("CREATE (:Role {name: 'senior'})").await?;
    tx.execute("CREATE (:Perm {action: 'READ'})").await?;
    tx.execute("CREATE (:Perm {action: 'WRITE'})").await?;
    for stmt in [
        "MATCH (p:Person {name:'alice'}), (r:Role {name:'senior'}) CREATE (p)-[:HAS_ROLE]->(r)",
        "MATCH (s:Role {name:'senior'}), (j:Role {name:'junior'}) CREATE (s)-[:INHERITS]->(j)",
        "MATCH (r:Role {name:'junior'}), (p:Perm {action:'READ'}) CREATE (r)-[:GRANTS]->(p)",
        "MATCH (r:Role {name:'senior'}), (p:Perm {action:'WRITE'}) CREATE (r)-[:GRANTS]->(p)",
    ] {
        tx.execute(stmt).await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// Runs `program` and returns `(derived_row_count, query_row_count)` for `rule`.
///
/// Row *counts* rather than contents, because cardinality is what diverged.
/// (The SLG path also used to leak the internal `overflow_json` column into
/// user-visible properties, which would have masked this; that is fixed and
/// pinned by `issue_160_query_path_must_not_leak_internal_columns`.)
async fn counts(db: &Uni, program: &str, rule: &str) -> Result<(usize, usize)> {
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let derived = result
        .derived_rule(rule)
        .await
        .unwrap()
        .unwrap_or(&empty)
        .len();
    let queried = result.rows().unwrap_or(&empty).len();
    Ok((derived, queried))
}

const RP_BASE: &str = "CREATE RULE rp AS MATCH (r:Role)-[:GRANTS]->(p:Perm) YIELD KEY r, KEY p\n";

// ---------------------------------------------------------------------------
// Green: the paths that DO agree. These pin the trigger to fresh bindings.
// ---------------------------------------------------------------------------

/// A plain derived rule with no `IS`-ref agrees on both surfaces — so `QUERY`
/// is not broken in general.
#[tokio::test]
async fn issue_160_plain_rule_agrees_on_both_surfaces() -> Result<()> {
    let db = setup().await?;
    let program = format!("{RP_BASE}QUERY rp");
    let (derived, queried) = counts(&db, &program, "rp").await?;
    assert_eq!(derived, 2, "two GRANTS edges must derive two facts");
    assert_eq!(queried, derived, "QUERY must agree with derived");
    Ok(())
}

/// A tuple `IS`-ref agrees when **every** subject is already bound by the
/// MATCH pattern. This is the same rule shape as the red test below, differing
/// only in whether `(p:Perm)` appears in MATCH.
#[tokio::test]
async fn issue_160_is_ref_with_all_subjects_match_bound_agrees() -> Result<()> {
    let db = setup().await?;
    let program = format!(
        "{RP_BASE}\
         CREATE RULE h AS\n\
           MATCH (e:Person)-[:HAS_ROLE]->(r:Role), (p:Perm)\n\
           WHERE (r, p) IS rp\n\
           YIELD KEY e, KEY p\n\
         QUERY h"
    );
    let (derived, queried) = counts(&db, &program, "h").await?;
    assert!(derived > 0, "alice must hold at least one permission");
    assert_eq!(
        queried, derived,
        "with `p` bound in MATCH the SLG path can check the IS-ref and agrees"
    );
    Ok(())
}

/// A single-subject `IS`-ref whose subject is MATCH-bound also agrees, ruling
/// out "tuple form" as the trigger.
#[tokio::test]
async fn issue_160_single_subject_is_ref_match_bound_agrees() -> Result<()> {
    let db = setup().await?;
    let program = "CREATE RULE granting AS MATCH (r:Role)-[:GRANTS]->(:Perm) YIELD KEY r\n\
         CREATE RULE h2 AS\n\
           MATCH (e:Person)-[:HAS_ROLE]->(r:Role)\n\
           WHERE r IS granting\n\
           YIELD KEY e\n\
         QUERY h2";
    let (derived, queried) = counts(&db, program, "h2").await?;
    assert!(derived > 0);
    assert_eq!(queried, derived);
    Ok(())
}

// ---------------------------------------------------------------------------
// Red: the open bug. Each isolates one claim from the issue report.
// ---------------------------------------------------------------------------

/// The minimal case: one level, no recursion, one `IS`-ref that introduces `p`.
/// This is the true root shape, and it is two levels shallower than the report
/// claims is necessary.
#[tokio::test]
async fn issue_160_is_ref_introducing_a_binding_must_agree() -> Result<()> {
    let db = setup().await?;
    let program = format!(
        "{RP_BASE}\
         CREATE RULE h AS\n\
           MATCH (e:Person)-[:HAS_ROLE]->(r:Role)\n\
           WHERE (r, p) IS rp\n\
           YIELD KEY e, KEY p\n\
         QUERY h"
    );
    let (derived, queried) = counts(&db, &program, "h").await?;
    assert!(derived > 0, "precondition: the fixpoint must derive facts");
    assert_eq!(
        queried, derived,
        "QUERY returned {queried} rows but the fixpoint derived {derived}: \
         `p` is introduced by the IS-ref and the SLG resolver can only filter \
         on already-bound subjects, never bind"
    );
    Ok(())
}

/// The recursive rule queried **directly** — which the issue report explicitly
/// claims does not reproduce. It does: the transitively derived
/// `(senior, READ)` fact is missing under `QUERY`, because the recursive clause
/// `WHERE (parent, p) IS role_perm` introduces `p`.
#[tokio::test]
async fn issue_160_recursive_rule_queried_directly_must_agree() -> Result<()> {
    let db = setup().await?;
    let program = "CREATE RULE role_perm AS\n\
           MATCH (r:Role)-[:GRANTS]->(p:Perm)\n\
           YIELD KEY r, KEY p\n\
         CREATE RULE role_perm AS\n\
           MATCH (r:Role)-[:INHERITS]->(parent:Role)\n\
           WHERE (parent, p) IS role_perm\n\
           YIELD KEY r, KEY p\n\
         QUERY role_perm";
    let (derived, queried) = counts(&db, program, "role_perm").await?;
    assert_eq!(derived, 3, "2 direct grants + 1 inherited (senior -> READ)");
    assert_eq!(
        queried, derived,
        "QUERY dropped the inherited fact; the report's claim that querying \
         the recursive rule directly is unaffected does not hold"
    );
    Ok(())
}

/// The exact program from the issue: a segregation-of-duties check requiring
/// two permissions. Kept because it is the user-facing shape, but note the two
/// tests above show neither the depth nor the repeated `IS`-ref matters.
#[tokio::test]
async fn issue_160_original_sod_check_must_agree() -> Result<()> {
    let db = setup().await?;
    let program = "CREATE RULE role_perm AS\n\
           MATCH (r:Role)-[:GRANTS]->(p:Perm)\n\
           YIELD KEY r, KEY p\n\
         CREATE RULE role_perm AS\n\
           MATCH (r:Role)-[:INHERITS]->(parent:Role)\n\
           WHERE (parent, p) IS role_perm\n\
           YIELD KEY r, KEY p\n\
         CREATE RULE holds AS\n\
           MATCH (e:Person)-[:HAS_ROLE]->(r:Role)\n\
           WHERE (r, p) IS role_perm\n\
           YIELD KEY e, KEY p\n\
         CREATE RULE both AS\n\
           MATCH (e:Person), (a:Perm {action: \"READ\"}), (b:Perm {action: \"WRITE\"})\n\
           WHERE (e, a) IS holds, (e, b) IS holds\n\
           YIELD KEY e\n\
         QUERY both";
    let (derived, queried) = counts(&db, program, "both").await?;
    assert_eq!(derived, 1, "alice holds both READ and WRITE");
    assert_eq!(
        queried, derived,
        "an access-control check that silently reports zero violations"
    );
    Ok(())
}

/// A separate, smaller defect found while verifying the above: the SLG `QUERY`
/// path leaks the internal `overflow_json` storage column into user-visible
/// node properties, while the fixpoint path does not. Independent evidence that
/// the two engines were never held to output parity.
#[tokio::test]
async fn issue_160_query_path_must_not_leak_internal_columns() -> Result<()> {
    let db = setup().await?;
    let program = format!("{RP_BASE}QUERY rp");
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    for row in result.rows().unwrap_or(&empty) {
        if let Some(Value::Node(node)) = row.get("r") {
            assert!(
                !node.properties.contains_key("overflow_json"),
                "internal storage column leaked into QUERY output: {:?}",
                node.properties
            );
        }
    }
    Ok(())
}
