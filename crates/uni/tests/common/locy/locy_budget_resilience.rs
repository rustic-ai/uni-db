// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A command that finishes past its program's deadline is reported, never
//! returned as on time.
//!
//! Deterministic: the `locy::after-command` seam sleeps past the deadline
//! between a command's completion and the budget check that follows it, so
//! the overrun does not depend on how fast the machine is. Under
//! `allow_partial` the late answer used to be returned with `incomplete`
//! unset.
//!
//! Run with `--features failpoints` (the failpoint lane's `resilience`
//! filter). Each test owns its failpoint and runs in its own process under
//! nextest, so the global registry does not bleed.

// Rust guideline compliant

#[cfg(feature = "failpoints")]
use std::time::Duration;

#[cfg(feature = "failpoints")]
use anyhow::{Result, bail};
#[cfg(feature = "failpoints")]
use uni_db::{LocyIncompleteReason, Uni, UniError};

#[cfg(feature = "failpoints")]
const PROGRAM: &str = "CREATE RULE big AS MATCH (n:T) WHERE n.k > 1 YIELD KEY n\n\
     QUERY big RETURN n.k AS k";

#[cfg(feature = "failpoints")]
async fn seeded() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(1, 4) AS i CREATE (:T {k: i})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// The QUERY completes within its 1.5 s budget, then the seam sleeps 2.5 s.
/// With `allow_partial` the answer stands and the overrun is a `Timeout`
/// `incomplete`; without it the program fails with `Timeout`. With the seam
/// off, the same program is complete.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_finishing_past_the_deadline_is_reported() -> Result<()> {
    let db = seeded().await?;
    let budget = Duration::from_millis(1500);
    let control = db
        .session()
        .locy_with(PROGRAM)
        .timeout(budget)
        .allow_partial(true)
        .run()
        .await?;
    assert!(
        control.incomplete.is_none(),
        "control: {:?}",
        control.incomplete
    );
    assert_eq!(control.command_results.len(), 1);

    fail::cfg("locy::after-command", "sleep(2500)").unwrap();
    let late = db
        .session()
        .locy_with(PROGRAM)
        .timeout(budget)
        .allow_partial(true)
        .run()
        .await;
    let strict = db.session().locy_with(PROGRAM).timeout(budget).run().await;
    fail::remove("locy::after-command");

    let late = late?;
    assert_eq!(late.command_results.len(), 1, "the late answer stands");
    let detail = late
        .incomplete
        .as_ref()
        .unwrap_or_else(|| panic!("a command finishing past the deadline must be reported"));
    assert_eq!(detail.reason, LocyIncompleteReason::Timeout);
    match strict {
        Err(UniError::Timeout { .. }) => Ok(()),
        other => bail!("without allow_partial: expected Timeout, got {other:?}"),
    }
}
