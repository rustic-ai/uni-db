// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! SSI read-set coverage for Locy reads (architecture review finding §2.4).
//!
//! A read-modify-write through Locy — `tx.locy(...)` derives facts from base
//! data, then the same transaction writes a value computed from the result —
//! must be serializable: if a concurrent transaction modifies the base data
//! and commits first, the RMW transaction's commit has a read-write
//! antidependency and must abort with `SerializationConflict`, exactly like
//! the equivalent Cypher read does.

use anyhow::Result;
use uni_db::{Uni, UniError};

/// Base data: three :Fact nodes with values 1, 2, 3.
async fn facts_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Fact {name: 'a', value: 1}), \
                (:Fact {name: 'b', value: 2}), \
                (:Fact {name: 'c', value: 3})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// Control: the same RMW shape with a plain Cypher read. Cypher scan reads
/// are known to be recorded in the OCC read-set, so this MUST conflict —
/// it validates the harness for the Locy variant below.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cypher_rmw_control_conflicts() -> Result<()> {
    let db = facts_db().await?;

    let s_a = db.session();
    let tx_a = s_a.tx().await?;
    // tx_a reads the facts.
    let r = tx_a
        .query("MATCH (f:Fact) RETURN sum(f.value) AS total")
        .await?;
    let total = r.rows()[0].get::<i64>("total")?;
    assert_eq!(total, 6);

    // A concurrent transaction modifies a fact tx_a read, and commits first.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (f:Fact {name: 'a'}) SET f.value = 100")
            .await?;
        tx_b.commit().await?;
    }

    // tx_a writes a value computed from its (now stale) read.
    tx_a.execute("CREATE (:Summary {kind: 'cypher', total: 6})")
        .await?;
    match tx_a.commit().await {
        Err(UniError::SerializationConflict { .. }) => Ok(()),
        other => panic!("expected SerializationConflict, got {other:?}"),
    }
}

/// The Locy variant: tx_a's read happens inside `tx.locy(...)` instead of a
/// Cypher MATCH. The serializability requirement is identical — if Locy
/// clause-body reads bypass the OCC read-set, this commit succeeds and the
/// stale derivation is silently persisted (the §2.4 bug).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locy_rmw_conflicts_like_cypher() -> Result<()> {
    let db = facts_db().await?;

    let s_a = db.session();
    let tx_a = s_a.tx().await?;
    // tx_a reads the facts through Locy.
    let result = tx_a
        .locy(
            "CREATE RULE fact_values AS \
             MATCH (f:Fact) YIELD KEY f, f.value AS v",
        )
        .await?;
    let fact_values = result
        .derived_rules()
        .await
        .unwrap()
        .get("fact_values")
        .expect("rule 'fact_values' missing");
    assert_eq!(fact_values.len(), 3, "locy must see the 3 base facts");

    // A concurrent transaction modifies a fact tx_a read, and commits first.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (f:Fact {name: 'a'}) SET f.value = 100")
            .await?;
        tx_b.commit().await?;
    }

    // tx_a writes a value computed from its (now stale) Locy read.
    tx_a.execute("CREATE (:Summary {kind: 'locy', count: 3})")
        .await?;
    match tx_a.commit().await {
        Err(UniError::SerializationConflict { .. }) => Ok(()),
        other => panic!(
            "expected SerializationConflict (locy reads must be in the OCC \
             read-set), got {other:?}"
        ),
    }
}

/// Session-level DERIVE reads happen outside any transaction, so they can
/// never be OCC-validated — the version-gap check in `tx.apply()` is the
/// only staleness guard. It must therefore reject by default when a commit
/// happened between evaluation and apply.
#[tokio::test]
async fn stale_apply_rejected_by_default() -> Result<()> {
    let db = facts_db().await?;
    let session = db.session();

    // Derive at session level (captures evaluated_at_version).
    let result = session
        .locy(
            "CREATE RULE link AS \
               MATCH (a:Fact), (b:Fact) WHERE a.name = 'a' AND b.name = 'b' \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link",
        )
        .await?;
    let derived = result
        .derived_fact_set
        .clone()
        .expect("session DERIVE should produce DerivedFactSet");

    // A commit lands between evaluation and apply → version gap > 0.
    let tx = session.tx().await?;
    tx.execute("MATCH (f:Fact {name: 'c'}) SET f.value = 30")
        .await?;
    tx.commit().await?;

    // Default apply must reject the stale derivation.
    let tx = session.tx().await?;
    match tx.apply(derived.clone()).await {
        Err(UniError::StaleDerivedFacts { version_gap }) => {
            assert!(version_gap > 0);
        }
        other => panic!("expected StaleDerivedFacts by default, got {other:?}"),
    }

    // The explicit opt-out still works.
    let applied = tx.apply_with(derived).allow_stale().run().await?;
    assert!(applied.version_gap > 0);
    assert!(applied.facts_applied > 0);
    tx.commit().await?;
    Ok(())
}

/// A fresh apply (no commits between DERIVE and apply) succeeds untouched
/// under the new default.
#[tokio::test]
async fn fresh_apply_succeeds_by_default() -> Result<()> {
    let db = facts_db().await?;
    let session = db.session();

    let result = session
        .locy(
            "CREATE RULE link AS \
               MATCH (a:Fact), (b:Fact) WHERE a.name = 'a' AND b.name = 'b' \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link",
        )
        .await?;
    let derived = result
        .derived_fact_set
        .clone()
        .expect("session DERIVE should produce DerivedFactSet");

    let tx = session.tx().await?;
    let applied = tx.apply(derived).await?;
    assert_eq!(applied.version_gap, 0);
    assert!(applied.facts_applied > 0);
    tx.commit().await?;
    Ok(())
}
