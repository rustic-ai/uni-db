// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `LocyResult::metrics().plan_time` reports the program's logical planning.
//!
//! A Locy evaluation plans the whole program (every stratum's clause bodies)
//! before running the fixpoint. That step was untimed, so `plan_time` read 0
//! for every Locy result while the physical re-planning inside the fixpoint was
//! counted (`physical_plan_time`, O1).
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
