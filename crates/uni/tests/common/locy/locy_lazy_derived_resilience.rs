// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A failed deferred derivation of left-out rules (#307) surfaces its error,
//! caches nothing, and succeeds when read again.
//!
//! Under the default `GoalDirected::Auto`, a rule only goal-directed `QUERY`
//! commands needed is derived on the first read of the result's facts.
//! Deterministic: the `locy::lazy-derive` seam makes that derivation fail the
//! way a timeout or a memory bound would, without depending on machine speed.
//!
//! Run with `--features failpoints` (the failpoint lane's `resilience`
//! filter). The test owns its failpoint and runs in its own process under
//! nextest, so the global registry does not bleed.

// Rust guideline compliant

#[cfg(feature = "failpoints")]
use anyhow::Result;
#[cfg(feature = "failpoints")]
use uni_db::Uni;
#[cfg(feature = "failpoints")]
use uni_db::locy::{GoalDirected, LocyConfig};

#[cfg(feature = "failpoints")]
const PROGRAM: &str = "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
     CREATE RULE reach AS MATCH (a:N)-[:E]->(m:N) WHERE m IS reach TO b YIELD KEY a, KEY b\n\
     QUERY reach WHERE a.name = 'a' RETURN b.name AS n";

#[cfg(feature = "failpoints")]
async fn seeded() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL N (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE E () FROM N TO N").await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:N {name: 'a'})-[:E]->(:N {name: 'b'})-[:E]->(:N {name: 'c'}), \
         (:N {name: 'x'})-[:E]->(:N {name: 'y'})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// The derivation fails while the seam is on: the error surfaces from both
/// accessors, nothing is cached, and the evaluated facts are untouched. With
/// the seam off the same result derives the facts full evaluation gives.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_deferred_derivation_caches_nothing_and_retries() -> Result<()> {
    let db = seeded().await?;
    let off = db
        .session()
        .locy_with(PROGRAM)
        .with_config(LocyConfig {
            goal_directed: GoalDirected::Off,
            ..Default::default()
        })
        .run()
        .await?;
    let want = off.derived_rule("reach").await?.map(Vec::len);
    assert_eq!(want, Some(4), "a->b, a->c, b->c, x->y");

    let auto = db.session().locy(PROGRAM).await?;
    assert_eq!(auto.lazily_derived_rules(), ["reach".to_string()]);

    fail::cfg("locy::lazy-derive", "return").unwrap();
    let all = auto.derived_rules().await.map(|_| ());
    let one = auto.derived_rule("reach").await.map(|_| ());
    fail::remove("locy::lazy-derive");

    for (what, outcome) in [("derived_rules", all), ("derived_rule", one)] {
        let err = outcome.expect_err("the derivation must fail while the seam is on");
        assert!(
            err.to_string().contains("injected failure"),
            "{what}: {err}"
        );
    }
    let lazy = auto.lazy_derived().expect("rules were left out");
    assert!(lazy.get().is_none(), "nothing partial is cached");
    assert!(auto.evaluated_rules().keys().all(|k| k != "reach"));

    assert_eq!(auto.derived_rule("reach").await?.map(Vec::len), want);
    assert!(lazy.get().is_some(), "cached after the retry");
    Ok(())
}
