// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tier 3 — result-neutral config knobs, and whether any of them is observable.
//!
//! A Tier-3 lever holds two instances that differ **only** by a config knob and
//! asserts their result bags are equal. Unlike Tier 1 (a global irreversible
//! transition) and Tier 2 (two paths open at once), both sides here are ordinary
//! primary instances; the knob is supposed to change *how* the work is done and
//! not *what* comes back.
//!
//! # The VID question is settled, and it was not the blocker
//!
//! `vid_determinism` measured that VID assignment is stable across identical
//! builds, across a flush, across repeats, **and across a config change**. So the
//! plan's conditional resolves: a Tier-3 lever may compare bags directly, and
//! `Case::identity_free_projection` is not needed and has not been built.
//!
//! # The real blocker is the same one Phase 4B hit
//!
//! A lever is only worth its runtime if its two sides demonstrably exercise
//! different machinery. Every driver in this module enforces that with an
//! activation witness and an 80% floor, precisely because a differential oracle
//! whose sides are identical passes vacuously while looking busy.
//!
//! So before shipping Tier-3 levers, this module **measures** whether any
//! counter in [`Witness`] moves across each candidate knob. Knobs that move
//! something can carry a lever; knobs that do not would ship a guaranteed-vacuous
//! test, and the honest response is to defer them and record why — exactly as
//! Phase 4B did for the index and compaction levers.
//!
//! These are probes, not levers. They report; they do not gate.

use uni_db::UniConfig;

use super::lever::{Witness, observe};
use super::seed::{DEFAULT_DATA_SEED, Fixture, build_dqp_seed_tuned};

/// The result-neutral knobs the plan nominates for Tier 3.
///
/// Deliberately **excludes** `ssi_enabled` and `defer_embeddings`: neither is
/// result-neutral, so a bag difference across them would be correct behaviour
/// rather than a defect.
fn candidates() -> Vec<(&'static str, fn(&mut UniConfig))> {
    vec![
        ("batch_size=64", |c| c.batch_size = 64),
        ("batch_size=8192", |c| c.batch_size = 8192),
        ("parallelism=1", |c| c.parallelism = 1),
        ("partial_lance_writes", |c| {
            c.partial_lance_writes = !c.partial_lance_writes
        }),
        ("async_flush_enabled", |c| {
            c.async_flush_enabled = !c.async_flush_enabled
        }),
        ("auto_flush_threshold=1", |c| c.auto_flush_threshold = 1),
    ]
}

/// A query broad enough to touch scan, filter and traversal counters.
const PROBE_QUERIES: &[&str] = &[
    "MATCH (p:Person) WHERE p.age >= 25 AND p.age <= 40 RETURN p.name AS c0",
    "MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN b.name AS c0",
    "MATCH (p:Person) RETURN count(*) AS c",
];

fn differs(a: &Witness, b: &Witness) -> Vec<String> {
    let mut out = Vec::new();
    let fields: [(&str, u64, u64); 5] = [
        ("l0_reads", a.l0_reads as u64, b.l0_reads as u64),
        (
            "storage_reads",
            a.storage_reads as u64,
            b.storage_reads as u64,
        ),
        ("rows_scanned", a.rows_scanned as u64, b.rows_scanned as u64),
        ("branch_scans", a.branch_scans, b.branch_scans),
        ("snapshot_reads", a.snapshot_reads, b.snapshot_reads),
    ];
    for (name, x, y) in fields {
        if x != y {
            out.push(format!("{name} {x}->{y}"));
        }
    }
    out
}

/// **The measurement.** Does any witness counter move across a Tier-3 knob?
///
/// Prints one line per knob. A knob with no moving counter cannot support a
/// lever: its activation predicate would have nothing true to say, and the
/// driver's 80% floor would fail the run — or worse, a lever written without a
/// witness would pass forever while comparing two identical execution paths.
#[tokio::test(flavor = "multi_thread")]
async fn probe_which_tier3_knobs_are_observable() -> anyhow::Result<()> {
    let base = build_dqp_seed_tuned(Fixture::TINY, DEFAULT_DATA_SEED, |_| {}).await?;

    let mut observable = Vec::new();
    let mut inert = Vec::new();

    for (label, tune) in candidates() {
        let other = build_dqp_seed_tuned(Fixture::TINY, DEFAULT_DATA_SEED, tune).await?;

        let mut moved: Vec<String> = Vec::new();
        let mut bags_equal = true;
        for q in PROBE_QUERIES {
            let a = observe_raw(&base, q).await?;
            let b = observe_raw(&other, q).await?;
            moved.extend(differs(&a.0, &b.0));
            if a.1 != b.1 {
                bags_equal = false;
            }
        }

        if moved.is_empty() {
            inert.push(label);
        } else {
            observable.push((label, moved.join(", ")));
        }
        eprintln!(
            "[dqp:tier3-probe] {label:<28} witness={:<40} bags={}",
            if moved.is_empty() {
                "(no counter moved)".to_string()
            } else {
                moved.join(", ")
            },
            if bags_equal { "equal" } else { "DIFFER" }
        );
    }

    eprintln!(
        "[dqp:tier3-probe] observable={} inert={}",
        observable.len(),
        inert.len()
    );

    // **Tripwire, not a gate.** Measured 2026-08-13: all six knobs are inert,
    // which is why Tier-3 levers are deferred rather than shipped.
    //
    // If this fails, that is **good news**: a knob became observable, so a
    // Tier-3 lever over it would now have a real activation witness and could
    // ship. Move it into a lever with `activated` written against the counter
    // named below, and drop it from `candidates`.
    assert!(
        observable.is_empty(),
        "a Tier-3 knob is now observable, which unblocks a lever over it: {}",
        observable
            .iter()
            .map(|(k, m)| format!("{k} moved {m}"))
            .collect::<Vec<_>>()
            .join("; ")
    );
    Ok(())
}

/// Result-neutrality is the premise of Tier 3, so it is worth asserting even
/// while the levers themselves are deferred.
///
/// Narrow — three fixed queries, not a generated population — but it is a real
/// correctness check: if flipping `partial_lance_writes` or `async_flush_enabled`
/// changed what a query returns, that is a defect regardless of whether any
/// counter noticed. When Tier-3 levers do ship, this is the assertion they
/// generalize.
#[tokio::test(flavor = "multi_thread")]
async fn tier3_knobs_are_result_neutral() -> anyhow::Result<()> {
    let base = build_dqp_seed_tuned(Fixture::TINY, DEFAULT_DATA_SEED, |_| {}).await?;

    for (label, tune) in candidates() {
        let other = build_dqp_seed_tuned(Fixture::TINY, DEFAULT_DATA_SEED, tune).await?;
        for q in PROBE_QUERIES {
            let (_, a) = observe_raw(&base, q).await?;
            let (_, b) = observe_raw(&other, q).await?;
            assert_eq!(
                a, b,
                "{label} changed the result of `{q}` ({a} rows vs {b}). Either the \
                 knob is not result-neutral — in which case it must leave the \
                 Tier-3 candidate set — or this is a real correctness defect."
            );
        }
    }
    Ok(())
}

/// Runs `cypher` and returns its witness plus a cheap row-count fingerprint.
async fn observe_raw(db: &uni_db::Uni, cypher: &str) -> anyhow::Result<(Witness, usize)> {
    let q = uni_cypher::parse(cypher)?;
    let o = observe(&db.session(), &q).await?;
    Ok((o.witness, o.bag.total))
}
