// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end companion to `uni-locy`'s `repro_warded_parenthesized_path`.
//!
//! That repro pins the *compiler* fix: `check_wardedness` now recurses into
//! `PatternElement::Parenthesized`, so a variable bound inside parentheses is
//! seen as match-bound and a legal rule stops being rejected.
//!
//! Passing the wardedness check only proves the rule compiles, though. If the
//! planner cannot expose a variable bound inside a parenthesised sub-pattern,
//! lifting the false positive would just move the failure one stage
//! downstream — a worse outcome than the original error, because it would
//! surface as an opaque planner or runtime fault instead of a named compile
//! error. This runs the rule against a real database to show it actually
//! derives.

use anyhow::Result;
use uni_db::Uni;

/// A DERIVE whose companion is bound inside parentheses must compile *and* run.
#[tokio::test]
async fn parenthesized_match_derives_at_runtime() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})")
        .await?;
    tx.commit().await?;

    // `b` is bound inside the parentheses. Before the compiler fix this was
    // rejected as a WardednessViolation; the point here is that nothing
    // downstream trips over it either.
    let result = session
        .locy(
            "CREATE RULE tagged AS \
               MATCH ((a:P)-[:KNOWS]->(b:P)) \
               DERIVE (b)-[:LINKED]->(a) \n\
             DERIVE tagged \n\
             MATCH (x:P)-[:LINKED]->(y:P) \
             RETURN x.name AS src, y.name AS dst",
        )
        .await?;

    let rows = match result.command_results.last().expect("no command results") {
        uni_db::locy::CommandResult::Cypher(rows) => rows,
        other => panic!("expected trailing Cypher, got {other:?}"),
    };
    assert_eq!(
        rows.len(),
        1,
        "a rule whose companion is bound inside parentheses must derive the \
         same edge the unparenthesised form does"
    );

    Ok(())
}

/// The unparenthesised form, as a control.
///
/// If this ever fails the parenthesised test above proves nothing, so the two
/// are kept side by side.
#[tokio::test]
async fn unparenthesized_match_derives_at_runtime() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE tagged AS \
               MATCH (a:P)-[:KNOWS]->(b:P) \
               DERIVE (b)-[:LINKED]->(a) \n\
             DERIVE tagged \n\
             MATCH (x:P)-[:LINKED]->(y:P) \
             RETURN x.name AS src, y.name AS dst",
        )
        .await?;

    let rows = match result.command_results.last().expect("no command results") {
        uni_db::locy::CommandResult::Cypher(rows) => rows,
        other => panic!("expected trailing Cypher, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);

    Ok(())
}

/// The *quantified* form. A variable bound inside a quantifier is a GQL group
/// variable — a list with one element per iteration — so it is not a node and
/// cannot be a DERIVE subject. The endpoint is what a rule should derive
/// against, and the diagnostic must say so.
#[tokio::test]
async fn quantified_parenthesized_derive_uses_the_endpoint() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("name", uni_db::DataType::String)
        .apply()
        .await?;
    db.schema().label("Tag").apply().await?;
    db.schema()
        .edge_type("KNOWS", &["P"], &["P"])
        .apply()
        .await?;
    db.schema()
        .edge_type("LINKED", &["Tag"], &["P"])
        .apply()
        .await?;
    let session = db.session();

    // A 2-chain, so `{2}` runs two iterations.
    let tx = session.tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})-[:KNOWS]->(:P {name: 'c'})")
        .await?;
    tx.commit().await?;

    // Deriving against the endpoint is the supported form.
    let result = session
        .locy(
            "CREATE RULE tagged AS \
               MATCH (s:P)((a:P)-[:KNOWS]->(b:P)){2}(e:P) \
               DERIVE (NEW t:Tag)-[:LINKED]->(e) \n\
             DERIVE tagged \n\
             MATCH (x:Tag)-[:LINKED]->(y:P) \
             RETURN y.name AS dst",
        )
        .await?;

    let rows = match result.command_results.last().expect("no command results") {
        uni_db::locy::CommandResult::Cypher(rows) => rows,
        other => panic!("expected trailing Cypher, got {other:?}"),
    };
    assert_eq!(
        rows.len(),
        1,
        "the quantified pattern matches once (a -> b -> c), deriving against \
         its endpoint"
    );

    Ok(())
}

/// A DERIVE head that references a *group* variable derives one fact per
/// iteration — the implicit `UNWIND` a user would otherwise write by hand.
///
/// Without it the head received a list where it expected a node and produced a
/// single derived edge pointing at nothing.
#[tokio::test]
async fn quantified_parenthesized_derive_unwinds_a_group_variable() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("name", uni_db::DataType::String)
        .apply()
        .await?;
    db.schema().label("Tag").apply().await?;
    db.schema()
        .edge_type("KNOWS", &["P"], &["P"])
        .apply()
        .await?;
    db.schema()
        .edge_type("LINKED", &["Tag"], &["P"])
        .apply()
        .await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})-[:KNOWS]->(:P {name: 'c'})")
        .await?;
    tx.commit().await?;

    // `b` is a group variable holding the target of each iteration: [b, c].
    let result = session
        .locy(
            "CREATE RULE tagged AS \
               MATCH (s:P)((a:P)-[:KNOWS]->(b:P)){2}(e:P) \
               DERIVE (NEW t:Tag)-[:LINKED]->(b) \n\
             DERIVE tagged \n\
             MATCH (x:Tag)-[:LINKED]->(y:P) \
             RETURN y.name AS dst ORDER BY dst",
        )
        .await?;

    let rows = match result.command_results.last().expect("no command results") {
        uni_db::locy::CommandResult::Cypher(rows) => rows,
        other => panic!("expected trailing Cypher, got {other:?}"),
    };
    let mut dsts: Vec<String> = rows
        .iter()
        .map(|r| match r.get("dst") {
            Some(uni_query::Value::String(s)) => s.clone(),
            other => panic!("expected a string dst, got {other:?}"),
        })
        .collect();
    dsts.sort();
    assert_eq!(
        dsts,
        vec!["b".to_string(), "c".to_string()],
        "one derived fact per iteration of the quantifier"
    );

    Ok(())
}
