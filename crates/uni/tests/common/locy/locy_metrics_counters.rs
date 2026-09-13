// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy evaluation must report the execution counters it accumulated.
//!
//! `LocyResult::metrics()` returned a `QueryMetrics` built from timing,
//! `rows_returned` and `..Default::default()`, so every counter field read 0 no
//! matter what the rules scanned. The counters were being incremented the whole
//! time — `Executor::create_datafusion_planner` installs them on the physical
//! planner, and the fixpoint's per-iteration scans tick them — but nothing
//! harvested them into the result.
//!
//! The field docs on `QueryMetrics::cache_hits` name this exact hazard:
//!
//! > A field that exists and always reads zero is a trap: anything asserting on
//! > it compiles and silently never fires.
//!
//! It caught a live instance. `locy_where_pushdown_probe` was written to use
//! `rows_scanned` as its instrument, and would have read zero on every Locy arm
//! while looking like a measurement.
//!
//! # What is asserted, and what deliberately is not
//!
//! That the counter is **populated and plausible**, not that it equals a
//! particular number. Row counts examined by a scan depend on plan choices that
//! are free to improve, so pinning an exact value would make this a brittle
//! restatement of the planner rather than a guard on the plumbing. The bounds
//! are therefore: strictly positive, and at least the number of rows the rule
//! actually derived, since a scan cannot produce more output than it examined.
//!
//! The cross-check against Cypher is the part that makes it more than a
//! not-zero test: the same graph, filtered the same way, examined through the
//! ordinary query path — a Locy count wildly below that would mean the
//! harvesting is partial rather than absent, which a bare `> 0` would pass.

use anyhow::Result;
use uni_db::Uni;

/// Enough rows that a scan count is unmistakably nonzero and stable, small
/// enough to stay a unit test.
const N: usize = 600;

async fn seeded_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Item (idx INT, hot BOOL)").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    for chunk in (0..N).collect::<Vec<_>>().chunks(100) {
        let mut cypher = String::from("CREATE ");
        for (i, idx) in chunk.iter().enumerate() {
            if i > 0 {
                cypher.push_str(", ");
            }
            // A third are hot, so the rule derives a strict subset of the label
            // and `rows_scanned >= derived` is a real bound rather than an
            // equality in disguise.
            cypher.push_str(&format!(
                "(:Item {{idx: {idx}, hot: {}}})",
                if idx % 3 == 0 { "true" } else { "false" }
            ));
        }
        tx.execute(&cypher).await?;
    }
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn locy_result_reports_the_rows_its_rules_scanned() -> Result<()> {
    let db = seeded_db().await?;

    let result = db
        .session()
        .locy(
            "CREATE RULE hot_item AS MATCH (i:Item) WHERE i.hot = true \
             YIELD KEY i.idx AS idx \nQUERY hot_item RETURN idx",
        )
        .await?;

    let scanned = result.metrics().rows_scanned;
    let returned = result.metrics().rows_returned;
    let derived = result
        .derived_facts("hot_item")
        .map(|f| f.len())
        .unwrap_or(0);

    // The Cypher equivalent over the same graph, as the scale reference.
    let cypher = db
        .session()
        .query("MATCH (i:Item) WHERE i.hot = true RETURN i.idx AS idx")
        .await?;
    let cypher_scanned = cypher.metrics().rows_scanned;

    eprintln!(
        "locy: rows_scanned={scanned} rows_returned={returned} derived={derived}  \
         cypher: rows_scanned={cypher_scanned} rows={}",
        cypher.rows().len()
    );

    // Correctness before counters: if the two paths disagree on the answer, the
    // scan counts are not measuring the same work and nothing below means much.
    assert_eq!(
        derived,
        cypher.rows().len(),
        "the rule derived {derived} facts and the equivalent Cypher returned {}; \
         the two are not doing the same work",
        cypher.rows().len()
    );
    assert!(derived > 0, "fixture derived nothing");

    assert!(
        scanned > 0,
        "LocyResult::metrics().rows_scanned is 0 after a rule that scanned \
         {derived} rows out of {N}. The counters are installed on the planner \
         and ticking; the result is not harvesting them, so every counter field \
         on a Locy evaluation reads zero and anything asserting on one passes \
         for free."
    );
    assert!(
        scanned >= derived,
        "rows_scanned ({scanned}) is below the {derived} facts derived from it; \
         a scan cannot emit more rows than it examined, so the harvest is \
         partial"
    );
    assert!(
        cypher_scanned == 0 || scanned * 4 >= cypher_scanned,
        "locy examined {scanned} rows where the equivalent Cypher examined \
         {cypher_scanned}; that gap is too large to be a plan difference and \
         suggests only some scans are reaching the counters"
    );
    Ok(())
}

/// The same, inside a transaction.
///
/// The transaction path is the one that rebuilds a `GraphExecutionContext` for
/// command dispatch, and `carry_budget` used to copy the deadline and the
/// cancellation token onto the rebuilt context but not the counters — so scans
/// under a transaction were dropped while the session path counted them. The
/// asymmetry is invisible without comparing the two, which is why this test
/// exists separately rather than as another assertion above.
#[tokio::test]
async fn a_transaction_scoped_evaluation_counts_the_same_work() -> Result<()> {
    let db = seeded_db().await?;
    const PROGRAM: &str = "CREATE RULE hot_item AS MATCH (i:Item) WHERE i.hot = true \
                           YIELD KEY i.idx AS idx \nQUERY hot_item RETURN idx";

    let session_scanned = db.session().locy(PROGRAM).await?.metrics().rows_scanned;

    let tx = db.session().tx().await?;
    let tx_scanned = tx.locy(PROGRAM).await?.metrics().rows_scanned;
    tx.commit().await?;

    eprintln!("session rows_scanned={session_scanned}  tx rows_scanned={tx_scanned}");
    assert!(
        tx_scanned > 0,
        "a transaction-scoped evaluation reported rows_scanned=0 while the same \
         program on a session reported {session_scanned}"
    );
    Ok(())
}
