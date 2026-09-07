// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #233 Tier 1 — `Uni::periodic_cancel` bypassed the persisting path.
//!
//! `periodic_schedule` deliberately routes through `SchedulerHost`'s
//! `SchedulerControl` impl, with a comment saying it does so "so the
//! persistence layer captures the schedule kind for restart durability".
//! `periodic_cancel`, immediately below it, called the bare in-memory
//! `Scheduler` instead — so the sidecar row survived a cancel and the job
//! resurrected on the next restart while the caller had been told it was
//! cancelled.
//!
//! The `CALL uni.periodic.cancel(...)` procedure was never affected: it
//! holds an `Arc<dyn SchedulerControl>` pointing at the host. Only the Rust
//! API had the asymmetry, which is why the existing host-level test did not
//! catch it.

use std::time::Duration;

use uni_db::Uni;
use uni_plugin::QName;
use uni_plugin::traits::background::Schedule;

#[tokio::test]
async fn periodic_cancel_removes_the_persisted_row() {
    let dir = tempfile::tempdir().expect("tempdir");
    let uri = dir.path().to_str().expect("utf8 path").to_owned();
    let db = Uni::open(&uri).build().await.expect("open db");

    let id = QName::parse("test.job").expect("qname");

    db.periodic_schedule(id.clone(), Schedule::Periodic(Duration::from_secs(3600)))
        .expect("schedule is durable");

    // Control: the schedule really did reach the durable sidecar, so the
    // cancel assertion below is testing removal rather than absence.
    let persistence = db.scheduler_host().persistence();
    assert_eq!(
        persistence.load_all().expect("load").len(),
        1,
        "control: scheduling persisted exactly one row"
    );

    let cancelled = db.periodic_cancel(&id).expect("cancel is durable");
    assert!(cancelled, "the job existed, so cancel reports true");

    let surviving = persistence.load_all().expect("load");
    assert!(
        surviving.is_empty(),
        "a cancelled job's persisted row must be removed, or it resurrects on the next \
         restart while the caller was told it was cancelled; got {surviving:?}"
    );
}
