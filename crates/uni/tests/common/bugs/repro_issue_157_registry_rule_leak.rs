// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #157 — a registered rule's parameters must not leak into every program.
//!
//! `merge_registered_rules` used to prepend **every** registered stratum to
//! every compiled program. Because a stratum owns its rules and the runtime
//! evaluates the strata vector end to end, one registered rule taking a
//! parameter made that parameter mandatory for unrelated programs:
//!
//! ```text
//! LocyRuntimeError: Execution error: Sub-plan error: Unresolved parameter: $needed
//! ```
//!
//! The registry is now filtered to the strata a program can reach. These tests
//! pin both directions: unreferenced rules must not run, and referenced ones —
//! including transitive, recursive and negated targets — must still run.

use uni_db::{DataType, Uni};

/// A database with an `Item(tag)` label and one parameterized registered rule
/// that no test program references unless it says so.
async fn db_with_parameterized_rule() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Item")
        .property("tag", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    db.rules()
        .register("CREATE RULE needs_param AS MATCH (i:Item {tag: $needed}) YIELD KEY i")
        .await
        .unwrap();
    db
}

/// The reported repro: an `ASSUME` program naming no rule must not inherit
/// `$needed` from an unrelated registered rule.
#[tokio::test]
async fn assume_program_referencing_no_rule_ignores_registered_params() {
    let db = db_with_parameterized_rule().await;

    let result = db
        .session()
        .locy_with(
            "ASSUME { CREATE (:Item {tag: 'hypothetical'}) } THEN { MATCH (i:Item) RETURN i }",
        )
        .run()
        .await;

    assert!(
        result.is_ok(),
        "a program referencing no rule must not require a registered rule's \
         parameters, got: {:?}",
        result.err()
    );
}

/// The same leak on the plain (non-`ASSUME`) path.
#[tokio::test]
async fn plain_program_referencing_no_rule_ignores_registered_params() {
    let db = db_with_parameterized_rule().await;
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:Item {tag: 'a'})").await.unwrap();
    tx.commit().await.unwrap();

    let result = db
        .session()
        .locy_with("MATCH (i:Item) RETURN i.tag AS tag")
        .run()
        .await;

    assert!(
        result.is_ok(),
        "plain Locy must not require a registered rule's parameters, got: {:?}",
        result.err()
    );
}

/// Referencing the rule *without* binding its parameter must still fail. The
/// prune removes work, never an error the caller should see.
#[tokio::test]
async fn referencing_the_rule_without_its_param_still_errors() {
    let db = db_with_parameterized_rule().await;

    let result = db.session().locy_with("QUERY needs_param").run().await;

    assert!(
        result.is_err(),
        "a program that DOES reference the rule must still demand $needed"
    );
}

/// Referencing the rule with its parameter bound works, so the stratum really
/// is retained rather than merely skipped.
#[tokio::test]
async fn referencing_the_rule_with_its_param_bound_succeeds() {
    let db = db_with_parameterized_rule().await;
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:Item {tag: 'wanted'})").await.unwrap();
    tx.commit().await.unwrap();

    let result = db
        .session()
        .locy_with("QUERY needs_param")
        .param("needed", "wanted")
        .run()
        .await;

    assert!(
        result.is_ok(),
        "binding the parameter must let the referenced rule run, got: {:?}",
        result.err()
    );
}

/// A transitively referenced registered rule is retained, while a sibling that
/// nothing references is not.
#[tokio::test]
async fn a_transitive_chain_is_retained_and_a_sibling_is_not() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:Person {name: 'Alice'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    db.rules()
        .register("CREATE RULE base AS MATCH (p:Person) YIELD KEY p")
        .await
        .unwrap();
    db.rules()
        .register("CREATE RULE mid AS MATCH (p:Person) WHERE p IS base YIELD KEY p")
        .await
        .unwrap();
    // Unreferenced, and parameterized so a leak would surface as a hard error.
    db.rules()
        .register("CREATE RULE unrelated AS MATCH (p:Person {name: $other}) YIELD KEY p")
        .await
        .unwrap();

    let result = db.session().locy_with("QUERY mid").run().await;

    assert!(
        result.is_ok(),
        "referencing `mid` must pull in `base` but not `unrelated`, got: {:?}",
        result.err()
    );
    let derived = result.unwrap();
    assert!(
        derived.derived_rules().await.unwrap().contains_key("mid"),
        "the referenced rule must be derived"
    );
    assert!(
        derived.derived_rules().await.unwrap().contains_key("base"),
        "the transitively referenced rule must be derived"
    );
    assert!(
        !derived
            .derived_rules()
            .await
            .unwrap()
            .contains_key("unrelated"),
        "an unreferenced registered rule must NOT be evaluated"
    );
}

/// A recursive registered rule is retained whole, so its transitive closure is
/// complete rather than truncated to one iteration.
#[tokio::test]
async fn a_recursive_registered_rule_still_reaches_its_fixpoint() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute(
        "CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    db.rules()
        .register(
            "CREATE RULE reach AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY b\n\
             CREATE RULE reach AS MATCH (a:Node)-[:EDGE]->(b:Node) WHERE a IS reach YIELD KEY b",
        )
        .await
        .unwrap();

    let result = db.session().locy_with("QUERY reach").run().await;
    assert!(
        result.is_ok(),
        "a recursive registered rule must evaluate, got: {:?}",
        result.err()
    );
    // B is reachable in one hop, C only through the recursive clause; a
    // truncated stratum set would lose C.
    assert!(
        result
            .unwrap()
            .derived_rules()
            .await
            .unwrap()
            .contains_key("reach"),
        "the recursive rule must derive facts"
    );
}

/// A negated reference retains its target. An anti-join against a dropped
/// relation would admit every row, so this guards the one way the prune could
/// produce *more* results rather than fewer.
#[tokio::test]
async fn a_negated_reference_retains_its_target() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Account")
        .property("name", DataType::String)
        .property("blocked", DataType::Bool)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:Account {name: 'ok', blocked: false})")
        .await
        .unwrap();
    tx.execute("CREATE (:Account {name: 'bad', blocked: true})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    db.rules()
        .register("CREATE RULE blocked AS MATCH (a:Account) WHERE a.blocked = true YIELD KEY a")
        .await
        .unwrap();

    let result = db
        .session()
        .locy_with(
            "CREATE RULE safe AS MATCH (a:Account) WHERE a IS NOT blocked YIELD KEY a\n\
             QUERY safe",
        )
        .run()
        .await;

    assert!(
        result.is_ok(),
        "a negated reference to a registered rule must evaluate, got: {:?}",
        result.err()
    );
    let rows = result.unwrap();
    let safe = rows
        .derived_rules()
        .await
        .unwrap()
        .get("safe")
        .expect("safe must be derived");
    assert_eq!(
        safe.len(),
        1,
        "the anti-join must exclude the blocked account; a dropped target would \
         admit both rows"
    );
}

/// Fail-closed regression: after clearing the registry, referencing a rule that
/// no longer exists must still error rather than resolve to nothing.
#[tokio::test]
async fn an_unknown_rule_still_errors_after_clear() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    db.rules()
        .register("CREATE RULE gone AS MATCH (p:Person) YIELD KEY p")
        .await
        .unwrap();
    db.rules().clear().await.unwrap();

    let result = db.session().locy_with("QUERY gone").run().await;
    assert!(
        result.is_err(),
        "referencing a rule that no longer exists must error, not return empty"
    );
}
