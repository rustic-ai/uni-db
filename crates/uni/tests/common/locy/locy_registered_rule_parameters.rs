// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A parameter only a previously registered rule reads is refused up front,
//! with every missing name, like one the program's own text reads.
//!
//! The up-front check read the program's text alone, so a registered rule's
//! `$a` was not in it: the program ran, and failed when the rule's clause
//! first evaluated `$a`, naming that one parameter only. A prepared program
//! did not check up front at all.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{Uni, UniError, Value};

/// A rule reading `$a` and `$b`, and one over it reading `$d`.
const REGISTERED: &str = "CREATE RULE reg AS MATCH (n:T) WHERE n.k >= $a AND n.v <> $b YIELD KEY n\n\
     CREATE RULE reg2 AS MATCH (n:T) WHERE n IS reg AND n.k <> $d YIELD KEY n";

/// A registered rule no program below runs, reading `$z`.
const UNRELATED: &str = "CREATE RULE other AS MATCH (n:T) WHERE n.k = $z YIELD KEY n";

async fn seeded() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(1, 4) AS i CREATE (:T {k: i, v: i * 10})")
        .await?;
    tx.commit().await?;
    db.rules().register(REGISTERED).await?;
    db.rules().register(UNRELATED).await?;
    Ok(db)
}

fn missing_names(result: Result<uni_db::locy::LocyResult, UniError>, what: &str) -> Vec<String> {
    match result {
        Err(UniError::ParameterMissing { names }) => names,
        other => panic!("{what}: expected ParameterMissing, got {other:?}"),
    }
}

/// The program's own parameters first, then each registered rule's it runs,
/// in stratum order; a registered rule it does not run adds nothing.
#[tokio::test]
async fn registered_rule_parameters_are_checked_before_evaluation() -> Result<()> {
    let db = seeded().await?;
    let program = "QUERY reg2 WHERE n.v > $c RETURN n.k AS k";
    let all = ["c", "a", "b", "d"].map(String::from).to_vec();

    let session = db.session();
    assert_eq!(
        missing_names(session.locy(program).await, "session.locy"),
        all
    );
    assert_eq!(
        missing_names(
            session.locy_with(program).param("a", 2).run().await,
            "locy_with, a supplied"
        ),
        ["c", "b", "d"].map(String::from).to_vec()
    );
    let tx = session.tx().await?;
    assert_eq!(missing_names(tx.locy(program).await, "tx.locy"), all);
    tx.rollback();
    let prepared = session.prepare_locy(program).await?;
    assert_eq!(
        missing_names(prepared.execute(&[]).await, "prepared"),
        all,
        "a prepared program is checked before it runs too"
    );

    // Everything supplied: it runs, and `other`'s `$z` is not required.
    let params = [
        ("a", Value::Int(2)),
        ("b", Value::Int(30)),
        ("c", Value::Int(0)),
        ("d", Value::Int(4)),
    ];
    let mut builder = session.locy_with(program);
    for (k, v) in &params {
        builder = builder.param(*k, v.clone());
    }
    let result = builder.run().await?;
    let rows = result.rows().expect("the QUERY's rows");
    let mut ks: Vec<i64> = rows
        .iter()
        .map(|r| r.get("k").and_then(Value::as_i64).expect("k"))
        .collect();
    ks.sort_unstable();
    // k >= 2, v <> 30, k <> 4.
    assert_eq!(ks, [2]);
    let rows = prepared.execute(&params).await?;
    assert_eq!(rows.rows().map(Vec::len), Some(1));
    Ok(())
}
