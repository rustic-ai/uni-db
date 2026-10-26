// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #157 follow-up — registry pruning across every rule-naming construct.
//!
//! The prune in `merge_registered_rules` seeds from every site that can name a
//! rule. `QUERY` and `IS` references are covered by
//! `repro_issue_157_registry_rule_leak`; the remaining command forms, the
//! `MODULE` name-resolution path, and the `PreparedLocy` call site had no
//! end-to-end coverage at all before this file, yet the fix depends on all
//! three.
//!
//! Every test pairs a positive assertion — the referenced rule still runs —
//! with a negative one: a second registered rule, parameterized so a leak
//! surfaces as a hard error rather than a silent extra relation, must not be
//! evaluated.

use uni_db::{DataType, Uni};

/// The unreferenced tripwire. Parameterized, so if the prune ever stops
/// filtering, every program in this file fails loudly on `$unused`.
const TRIPWIRE: &str = "CREATE RULE tripwire AS MATCH (t:Trip {tag: $unused}) YIELD KEY t";

/// A database with `X(name)` and `Trip(tag)` labels plus the tripwire rule.
async fn db_with_tripwire() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("X")
        .property("name", DataType::String)
        .done()
        .label("Trip")
        .property("tag", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    db.rules().register(TRIPWIRE).await.unwrap();
    db
}

/// Seeds two connected `X` nodes.
async fn seed_pair(db: &Uni) {
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// Command forms that name a registered rule
// ---------------------------------------------------------------------------

/// `DERIVE <registered_rule>` seeds retention.
#[tokio::test]
async fn derive_command_retains_its_registered_rule() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;
    db.rules()
        .register("CREATE RULE link AS MATCH (a:X)-[:R]->(b:X) DERIVE (a)-[:LINKED]->(b)")
        .await
        .unwrap();

    let result = db.session().locy_with("DERIVE link").run().await;

    assert!(
        result.is_ok(),
        "DERIVE against a registered rule must evaluate it, got: {:?}",
        result.err()
    );
    assert!(
        !result
            .unwrap()
            .derived_rules()
            .await
            .unwrap()
            .contains_key("tripwire"),
        "the unreferenced registered rule must not be evaluated"
    );
}

/// `EXPLAIN RULE <registered_rule>` seeds retention.
#[tokio::test]
async fn explain_rule_command_retains_its_registered_rule() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;
    db.rules()
        .register("CREATE RULE linked AS MATCH (a:X)-[:R]->(b:X) YIELD KEY a, b")
        .await
        .unwrap();

    let result = db
        .session()
        .locy_with("EXPLAIN RULE linked WHERE a.name = 'A'")
        .run()
        .await;

    assert!(
        result.is_ok(),
        "EXPLAIN RULE against a registered rule must resolve it, got: {:?}",
        result.err()
    );
    let out = result.unwrap();
    assert_eq!(
        out.command_results.len(),
        1,
        "EXPLAIN RULE should yield one command result"
    );
    assert!(
        !out.derived_rules().await.unwrap().contains_key("tripwire"),
        "the unreferenced registered rule must not be evaluated"
    );
}

/// `ABDUCE NOT <registered_rule>` seeds retention.
#[tokio::test]
async fn abduce_command_retains_its_registered_rule() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;
    db.rules()
        .register("CREATE RULE reachable AS MATCH (a:X)-[:R]->(b:X) YIELD KEY a")
        .await
        .unwrap();

    let result = db.session().locy_with("ABDUCE NOT reachable").run().await;

    assert!(
        result.is_ok(),
        "ABDUCE against a registered rule must resolve it, got: {:?}",
        result.err()
    );
    assert!(
        !result
            .unwrap()
            .derived_rules()
            .await
            .unwrap()
            .contains_key("tripwire"),
        "the unreferenced registered rule must not be evaluated"
    );
}

// ---------------------------------------------------------------------------
// MODULE name resolution — registry keys are qualified, references are bare
// ---------------------------------------------------------------------------

/// A registered `MODULE m` rule is stored as `m.adult` but referenced bare from
/// inside `MODULE m`. The prune must bridge the two or drop a needed stratum.
#[tokio::test]
async fn a_bare_reference_inside_its_module_retains_the_qualified_rule() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;
    db.rules()
        .register("MODULE m\nCREATE RULE adult AS MATCH (a:X) YIELD KEY a")
        .await
        .unwrap();

    let result = db.session().locy_with("MODULE m\nQUERY adult").run().await;

    let out = result.unwrap_or_else(|e| {
        panic!("a bare reference inside MODULE m must resolve the registered `m.adult`: {e}")
    });
    // Assert the rows, not merely that the call succeeded: resolving to an empty
    // relation would be just as wrong as erroring, and far quieter.
    let facts = out
        .derived_rules()
        .await
        .unwrap()
        .get("m.adult")
        .expect("the qualified rule must appear in `derived`");
    assert_eq!(facts.len(), 2, "both seeded X nodes must be derived");
    assert!(
        !out.derived_rules().await.unwrap().contains_key("tripwire"),
        "the unreferenced registered rule must not be evaluated"
    );
}

/// The same rule named in full from outside its module.
#[tokio::test]
async fn a_fully_qualified_reference_retains_the_registered_rule() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;
    db.rules()
        .register("MODULE m\nCREATE RULE adult AS MATCH (a:X) YIELD KEY a")
        .await
        .unwrap();

    let result = db.session().locy_with("QUERY m.adult").run().await;

    let out = result.unwrap_or_else(|e| panic!("a fully qualified reference must resolve: {e}"));
    let facts = out
        .derived_rules()
        .await
        .unwrap()
        .get("m.adult")
        .expect("the qualified rule must appear in `derived`");
    assert_eq!(facts.len(), 2, "both seeded X nodes must be derived");
    assert!(
        !out.derived_rules().await.unwrap().contains_key("tripwire"),
        "the unreferenced registered rule must not be evaluated"
    );
}

/// Two modules exporting the same leaf name.
///
/// The *pruner* retains every candidate stratum rather than refusing, so
/// nothing the program needs is dropped. The compiler resolves `adult` to
/// `m1.adult` through the enclosing `MODULE m1`, and the compiled command now
/// carries that resolved name, so the query answers from `m1.adult`. It used to
/// keep the bare spelling, leaving the runtime only suffix matching, which
/// could not tell the two apart and refused the program. `m2.adult` derives
/// nothing here, so an answer from the wrong module would show.
#[tokio::test]
async fn a_bare_reference_resolves_through_the_enclosing_module() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;
    db.rules()
        .register("MODULE m1\nCREATE RULE adult AS MATCH (a:X) YIELD KEY a")
        .await
        .unwrap();
    db.rules()
        .register("MODULE m2\nCREATE RULE adult AS MATCH (a:X) WHERE a.name = 'none' YIELD KEY a")
        .await
        .unwrap();

    let result = db
        .session()
        .locy_with("MODULE m1\nQUERY adult")
        .run()
        .await
        .expect("the enclosing module resolves the bare name");
    let rows = result.rows().map_or(0, |r| r.len());
    assert_eq!(
        rows, 2,
        "QUERY adult in MODULE m1 must answer from m1.adult"
    );
}

/// An `ASSUME` body is evaluated against the enclosing program's strata, so a
/// rule it references must survive the prune applied to the outer program.
#[tokio::test]
async fn an_assume_body_reference_retains_the_registered_rule() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;
    db.rules()
        .register("CREATE RULE connected AS MATCH (a:X)-[:R]->(b:X) YIELD KEY a")
        .await
        .unwrap();

    let result = db
        .session()
        .locy_with("ASSUME { CREATE (:X {name: 'C'}) } THEN { QUERY connected }")
        .run()
        .await;

    assert!(
        result.is_ok(),
        "an ASSUME body referencing a registered rule must retain it, got: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// The third call site: PreparedLocy
// ---------------------------------------------------------------------------

/// `prepare_locy` merges the registry on its own path (`prepared.rs`), which
/// had no registered-rule coverage at all. A prepared program naming no rule
/// must not inherit a registered rule's parameters.
#[tokio::test]
async fn a_prepared_program_referencing_no_rule_ignores_registered_params() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;

    let prepared = db
        .session()
        .prepare_locy("MATCH (a:X) RETURN a.name AS name")
        .await
        .expect("preparing a rule-free program must succeed");

    let result = prepared.execute(&[]).await;

    assert!(
        result.is_ok(),
        "a prepared program referencing no rule must not require $unused, got: {:?}",
        result.err()
    );
}

/// The positive half: a prepared program that *does* reference a registered
/// rule still gets its facts.
#[tokio::test]
async fn a_prepared_program_referencing_a_rule_still_gets_its_facts() {
    let db = db_with_tripwire().await;
    seed_pair(&db).await;
    db.rules()
        .register("CREATE RULE connected AS MATCH (a:X)-[:R]->(b:X) YIELD KEY a")
        .await
        .unwrap();

    let prepared = db
        .session()
        .prepare_locy("QUERY connected")
        .await
        .expect("preparing a program that references a registered rule must succeed");

    let result = prepared.execute(&[]).await;

    assert!(
        result.is_ok(),
        "a prepared program must still evaluate the rule it names, got: {:?}",
        result.err()
    );
    let out = result.unwrap();
    assert!(
        out.derived_rules().await.unwrap().contains_key("connected"),
        "the referenced rule must be derived on the prepared path"
    );
    assert!(
        !out.derived_rules().await.unwrap().contains_key("tripwire"),
        "the unreferenced registered rule must not be evaluated"
    );
}
