// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runtime companion to `uni-locy`'s `repro_assume_body_outer_rule_ref`.
//!
//! Compiling is not the bar here. `CompiledAssume` carries a `body_program`
//! whose `rule_catalog` is separate from the parent's, and the body's commands
//! look rules up **unqualified, straight from the AST** — `locy_assume.rs` and
//! `locy_query.rs` both do `program.rule_catalog.get(&rule_name)`. So a fix
//! that made the body's own rules module-qualified would compile cleanly and
//! then fail at run time with "rule not found".
//!
//! Running these end to end revealed a second, independent layer: the body's
//! rules are compiled as their own program, so its plan build saw only the
//! body's `rule_catalog` and an IS-reference to an outer rule failed with
//! `IS-reference to unknown rule` — in the no-`MODULE` control too, which
//! compiles cleanly. The feature had never worked at runtime, module or not.
//!
//! Both layers are fixed now. The compiler makes outer rules *visible* to the
//! body (under both their qualified and bare spellings), and the runtime merges
//! the parent's catalog with the body's before planning the body's strata and
//! before dispatching its commands — with body-local rules shadowing.

use anyhow::Result;
use uni_db::{Uni, locy::CommandResult};

async fn seeded() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Ada', age: 36})")
        .await?;
    tx.execute("CREATE (:Person {name: 'Kid', age: 9})").await?;
    tx.commit().await?;
    Ok(db)
}

const OUTER_RULE: &str = "CREATE RULE adult AS MATCH (p:Person) WHERE p.age >= 18 YIELD KEY p";
const ASSUME_BODY: &str = "ASSUME { CREATE (:Person {name: 'Guest', age: 30}) } THEN { \
       CREATE RULE eligible AS MATCH (p:Person) WHERE p IS adult YIELD KEY p \
       QUERY eligible RETURN p.name AS name \
     }";

/// Row count of the ASSUME body's one `QUERY`.
///
/// An `ASSUME … THEN { … }` is one top-level command, so its body's output
/// arrives as a single `CommandResult::Assume` holding one result per body
/// command — not as a trailing `Cypher` result. The body's only command is
/// its `QUERY`.
fn rows(result: &uni_db::locy::LocyResult) -> usize {
    match result.command_results.last().expect("no command results") {
        CommandResult::Assume { results, .. } => match results.as_slice() {
            [CommandResult::Query(r)] => r.len(),
            other => panic!("expected the ASSUME body's one QUERY result, got {other:?}"),
        },
        CommandResult::Query(r) | CommandResult::Cypher(r) => r.len(),
        other => panic!("expected the ASSUME's rows, got {other:?}"),
    }
}

/// The control: without `MODULE`, an ASSUME body rule may IS-reference an outer
/// rule, and the body's own `QUERY` must find it.
///
/// This failed too before the runtime fix, which is what showed the compile-time
/// half was not the whole story.
#[tokio::test]
async fn assume_body_references_outer_rule_without_module() -> Result<()> {
    let db = seeded().await?;
    let result = db
        .session()
        .locy(&format!("{OUTER_RULE} \n{ASSUME_BODY}"))
        .await?;
    assert!(
        rows(&result) > 0,
        "the body's QUERY must return the adults its rule derived"
    );
    Ok(())
}

/// The original bug: the same program under `MODULE m`.
#[tokio::test]
async fn assume_body_references_outer_rule_under_a_module() -> Result<()> {
    let db = seeded().await?;
    let result = db
        .session()
        .locy(&format!("MODULE m \n{OUTER_RULE} \n{ASSUME_BODY}"))
        .await?;
    assert!(
        rows(&result) > 0,
        "a module-qualified outer rule must be referenceable from an ASSUME body"
    );
    Ok(())
}

/// The two spellings must agree — that parity is what the compile fix bought,
/// and the runtime fix must not reintroduce a difference.
#[tokio::test]
async fn module_and_non_module_agree() -> Result<()> {
    let db = seeded().await?;
    let plain = db
        .session()
        .locy(&format!("{OUTER_RULE} \n{ASSUME_BODY}"))
        .await?;
    let moduled = db
        .session()
        .locy(&format!("MODULE m \n{OUTER_RULE} \n{ASSUME_BODY}"))
        .await?;
    assert_eq!(
        rows(&plain),
        rows(&moduled),
        "`MODULE` must not change what an ASSUME body returns"
    );
    Ok(())
}

/// The qualified spelling must keep working.
///
/// Exposing outer rules under their bare names must not remove the qualified
/// name a user may reasonably write.
#[tokio::test]
async fn assume_body_may_use_the_qualified_name() -> Result<()> {
    let db = seeded().await?;
    let result = db
        .session()
        .locy(
            "MODULE m \n\
             CREATE RULE adult AS MATCH (p:Person) WHERE p.age >= 18 YIELD KEY p \n\
             ASSUME { CREATE (:Person {name: 'Guest', age: 30}) } THEN { \
               CREATE RULE eligible AS MATCH (p:Person) WHERE p IS m.adult YIELD KEY p \
               QUERY eligible RETURN p.name AS name \
             }",
        )
        .await?;
    assert!(rows(&result) > 0, "`m.adult` must still resolve");
    Ok(())
}

/// Inverse guard: a genuinely undefined rule is still refused.
///
/// Widening what an ASSUME body can see must not make every name resolvable.
#[tokio::test]
async fn assume_body_still_rejects_an_unknown_rule() -> Result<()> {
    let db = seeded().await?;
    let res = db
        .session()
        .locy(
            "MODULE m \n\
             CREATE RULE adult AS MATCH (p:Person) WHERE p.age >= 18 YIELD KEY p \n\
             ASSUME { CREATE (:Person {name: 'Guest', age: 30}) } THEN { \
               CREATE RULE eligible AS MATCH (p:Person) WHERE p IS nosuchrule YIELD KEY p \
               QUERY eligible RETURN p.name AS name \
             }",
        )
        .await;
    let err = res
        .err()
        .expect("an undefined rule must still be an error inside an ASSUME body")
        .to_string();
    assert!(
        err.contains("nosuchrule"),
        "the error must name the unknown rule, got: {err}"
    );
    Ok(())
}
