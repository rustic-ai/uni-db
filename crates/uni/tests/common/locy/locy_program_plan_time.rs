// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `LocyResult::metrics().plan_time` reports the program's logical planning.
//!
//! A Locy evaluation plans the whole program (every stratum's clause bodies)
//! before running the fixpoint. That step was untimed, so `plan_time` read 0
//! for every Locy result while the physical re-planning inside the fixpoint was
//! counted (`physical_plan_time`, O1).
//!
//! `ASSUME` and `ABDUCE` plan the program's strata again for each
//! hypothetical state. That planning is counted too, and a stratum the
//! hypothetical state reuses is not planned at all.
//!
//! Run with:
//!   cargo nextest run -p uni-db -E 'test(locy_program_plan_time)'

use std::time::Duration;

use anyhow::{Result, ensure};
use uni_db::Uni;

/// Program-level planning is measured, and it is part of the evaluation.
#[tokio::test]
async fn locy_program_plan_time_is_reported() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'a'})-[:E]->(:N {name: 'b'})-[:E]->(:N {name: 'c'})")
        .await?;
    tx.commit().await?;
    let result = db
        .session()
        .locy(
            "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b \n\
             CREATE RULE reach AS MATCH (a:N)-[:E]->(m:N) WHERE m IS reach TO b YIELD KEY a, KEY b \n\
             QUERY reach RETURN a.name AS a, b.name AS b",
        )
        .await?;
    let m = result.metrics();
    eprintln!(
        "plan_time={:?} exec_time={:?} total_time={:?}",
        m.plan_time, m.exec_time, m.total_time
    );
    ensure!(
        m.plan_time > Duration::ZERO,
        "Locy program planning is not timed: plan_time = 0"
    );
    ensure!(
        m.plan_time + m.exec_time <= m.total_time,
        "plan_time {:?} + exec_time {:?} exceed total_time {:?}: planning is \
         counted twice",
        m.plan_time,
        m.exec_time,
        m.total_time
    );
    Ok(())
}

/// Independent rules over `P`, one stratum each, so planning them is the
/// dominant planning cost of the program.
fn wide_program(rules: usize) -> String {
    (0..rules)
        .map(|i| {
            format!(
                "CREATE RULE r{i} AS MATCH (a:P)-[:E]->(b:P) WHERE a.x > {i} \
                 YIELD KEY a, KEY b\n"
            )
        })
        .collect()
}

async fn wide_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(0, 9) AS i CREATE (:P {x: i})-[:E]->(:P {x: i + 1})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// Median `plan_time` over a few runs, to keep one slow run from deciding.
async fn median_plan_time(db: &Uni, program: &str) -> Result<(Duration, uni_locy::LocyStats)> {
    let mut times = Vec::new();
    let mut stats = uni_locy::LocyStats::default();
    for _ in 0..5 {
        let result = db.session().locy(program).await?;
        let m = result.metrics();
        ensure!(
            m.plan_time + m.exec_time <= m.total_time,
            "plan_time {:?} + exec_time {:?} exceed total_time {:?}",
            m.plan_time,
            m.exec_time,
            m.total_time
        );
        times.push(m.plan_time);
        stats = result.stats().clone();
    }
    times.sort();
    Ok((times[times.len() / 2], stats))
}

const WIDE_RULES: usize = 60;
const ASSUMES: usize = 8;

/// An `ASSUME` re-plans the strata its mutation affects, and that planning
/// is reported in `plan_time`.
///
/// Every rule reads `P`, so creating a `P` node affects every stratum and
/// each of the `ASSUMES` blocks plans the whole program again.
#[tokio::test]
async fn locy_program_plan_time_counts_assume_replanning() -> Result<()> {
    let db = wide_db().await?;
    let program = wide_program(WIDE_RULES);
    let (base, _) = median_plan_time(&db, &program).await?;
    let assumes: String = (0..ASSUMES)
        .map(|i| {
            format!(
                "ASSUME {{ CREATE (:P {{x: {}}}) }} THEN {{ QUERY r0 RETURN a }}\n",
                100 + i
            )
        })
        .collect();
    let (with, stats) = median_plan_time(&db, &format!("{program}{assumes}")).await?;
    eprintln!("plan_time: program {base:?}, with {ASSUMES} affecting ASSUMEs {with:?}");
    ensure!(
        stats.hypothetical_strata_evaluated == ASSUMES * WIDE_RULES,
        "every ASSUME should re-derive every stratum: {stats:?}"
    );
    ensure!(
        with > base * 3,
        "{ASSUMES} ASSUMEs that re-plan all {WIDE_RULES} strata report plan_time {with:?}, \
         barely above the program's own {base:?}: their re-planning is not counted"
    );
    Ok(())
}

/// An `ASSUME` whose mutation affects no stratum reuses every stratum, and
/// does not plan them again.
///
/// No rule reads `Q`, so each block reuses all strata; its planning should
/// cost next to nothing next to the program's own.
#[tokio::test]
async fn locy_program_plan_time_skips_reused_strata() -> Result<()> {
    let db = wide_db().await?;
    let program = wide_program(WIDE_RULES);
    let (base, _) = median_plan_time(&db, &program).await?;
    let assumes: String = (0..ASSUMES)
        .map(|i| format!("ASSUME {{ CREATE (:Q {{x: {i}}}) }} THEN {{ QUERY r0 RETURN a }}\n"))
        .collect();
    let (with, stats) = median_plan_time(&db, &format!("{program}{assumes}")).await?;
    eprintln!("plan_time: program {base:?}, with {ASSUMES} non-affecting ASSUMEs {with:?}");
    ensure!(
        stats.hypothetical_strata_reused == ASSUMES * WIDE_RULES
            && stats.hypothetical_strata_evaluated == 0,
        "every ASSUME should reuse every stratum: {stats:?}"
    );
    ensure!(
        with < base * 2,
        "{ASSUMES} ASSUMEs that reuse every stratum report plan_time {with:?} against the \
         program's own {base:?}: the reused strata are planned again"
    );
    Ok(())
}
