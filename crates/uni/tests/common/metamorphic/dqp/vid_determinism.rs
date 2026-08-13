// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Is VID assignment deterministic given an identical insert sequence?
//!
//! This is the one genuinely open question in the DQP plan, and it decides
//! whether Tier-3 levers are cheap or expensive:
//!
//! * **Deterministic** → a Tier-3 lever (a result-neutral config knob such as
//!   `batch_size` or `parallelism`) can compare bags directly, including any
//!   projection that returns a node. Nothing extra needs building.
//! * **Nondeterministic** → every Tier-3 comparison must first strip identity,
//!   which means building a `Case::identity_free_projection` and accepting that
//!   the oracle can no longer see a class of bug that manifests as a wrong VID.
//!
//! The plan is explicit that neither answer should be designed around before it
//! is measured, so this module measures and records; it does not build for
//! either outcome.
//!
//! # What "deterministic" has to mean to be useful
//!
//! Not "the same VIDs appear somewhere". The useful property is that the
//! **mapping from a logical row to its VID** is identical across two builds — a
//! fixture whose VIDs were merely a permutation of the same integer set would
//! still break a direct bag comparison, because `id(p)` would pair with a
//! different `p.name` on each side.
//!
//! So the comparison is over `name → vid` pairs, not over the VID multiset.

use std::collections::BTreeMap;

use uni_db::Uni;

use super::seed::{Fixture, build_dqp_seed_for_with, build_dqp_seed_tuned};

/// A fixed seed, so both instances receive an identical insert sequence. Any
/// difference in the resulting VIDs is then attributable to the engine rather
/// than to the data.
const FIXED_SEED: u64 = 0x5EED_0F_1D_u64;

/// `name → id(p)` for every Person, which is the mapping a Tier-3 bag
/// comparison would depend on.
async fn name_to_vid(db: &Uni) -> anyhow::Result<BTreeMap<String, i64>> {
    let r = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name, id(p) AS vid")
        .await?;
    let mut out = BTreeMap::new();
    for row in r.rows() {
        out.insert(row.get::<String>("name")?, row.get::<i64>("vid")?);
    }
    Ok(out)
}

/// Summarises how two VID mappings differ, for a failure message that says
/// *how* nondeterministic rather than merely *that* it is.
fn describe(a: &BTreeMap<String, i64>, b: &BTreeMap<String, i64>) -> String {
    let differing: Vec<_> = a
        .iter()
        .filter_map(|(k, va)| b.get(k).filter(|vb| *vb != va).map(|vb| (k, va, vb)))
        .collect();
    let same_set = {
        let sa: std::collections::BTreeSet<_> = a.values().collect();
        let sb: std::collections::BTreeSet<_> = b.values().collect();
        sa == sb
    };
    let sample: Vec<String> = differing
        .iter()
        .take(5)
        .map(|(k, va, vb)| format!("{k}: {va} vs {vb}"))
        .collect();
    format!(
        "{}/{} rows map to a different VID; VID *sets* are {}. Sample: [{}]",
        differing.len(),
        a.len(),
        if same_set {
            "IDENTICAL (a permutation — still fatal for a direct bag comparison)"
        } else {
            "different"
        },
        sample.join(", ")
    )
}

/// **The experiment.** Two instances, one seed, compared directly.
///
/// Its outcome is recorded in `docs/perf/dqp-feasibility-2026-08-12.md`; if this
/// test ever starts failing, that record is stale and the Tier-3 design premise
/// has changed underneath it.
#[tokio::test(flavor = "multi_thread")]
async fn vid_assignment_is_deterministic_across_identical_builds() -> anyhow::Result<()> {
    let a = build_dqp_seed_for_with(Fixture::TINY, FIXED_SEED).await?;
    let b = build_dqp_seed_for_with(Fixture::TINY, FIXED_SEED).await?;

    let ma = name_to_vid(&a).await?;
    let mb = name_to_vid(&b).await?;

    assert!(
        !ma.is_empty(),
        "fixture returned no rows — nothing measured"
    );
    assert_eq!(
        ma.len(),
        mb.len(),
        "the two builds disagree on row count, so the insert sequences were not \
         identical and this measures nothing"
    );
    assert_eq!(
        ma,
        mb,
        "VID assignment is NOT deterministic across identical builds. {}",
        describe(&ma, &mb)
    );
    Ok(())
}

/// Determinism must also survive the transition a Tier-3 lever would sit across.
///
/// A knob like `batch_size` or `async_flush_enabled` changes *how* rows reach
/// L1, so a VID mapping that were stable only pre-flush would be useless: the
/// two sides of that lever differ precisely in their flush behaviour.
#[tokio::test(flavor = "multi_thread")]
async fn vid_assignment_survives_a_flush() -> anyhow::Result<()> {
    let a = build_dqp_seed_for_with(Fixture::TINY, FIXED_SEED).await?;
    let before = name_to_vid(&a).await?;
    a.flush().await?;
    let after = name_to_vid(&a).await?;

    assert_eq!(
        before,
        after,
        "VIDs moved across a flush. {}",
        describe(&before, &after)
    );
    Ok(())
}

/// **The question Tier 3 actually depends on**, which is not quite the one the
/// plan asked.
///
/// The plan phrases the experiment as "deterministic given an identical insert
/// sequence". But a Tier-3 lever does not compare two identical builds — it
/// compares two builds that differ by a **config knob**, and then asserts their
/// result bags are equal. So the load-bearing property is stability of the
/// `name → vid` mapping *across a config change*, and a fixture could easily be
/// deterministic build-to-build while allocating different VIDs at a different
/// `batch_size` or `parallelism`.
///
/// If this fails while the identical-build test passes, the answer to the plan's
/// question is "yes" and the answer to the useful question is still "no": Tier 3
/// would need `identity_free_projection` regardless.
///
/// `batch_size` (morsel size, default 1024) and `parallelism` are the two knobs
/// most likely to perturb allocation order, so they are what this varies.
#[tokio::test(flavor = "multi_thread")]
async fn vid_assignment_survives_a_config_change() -> anyhow::Result<()> {
    let base = build_dqp_seed_tuned(Fixture::TINY, FIXED_SEED, |_| {}).await?;
    let reference = name_to_vid(&base).await?;

    let variants: [(&str, fn(&mut uni_db::UniConfig)); 3] = [
        ("batch_size=64", |c| c.batch_size = 64),
        ("batch_size=4096", |c| c.batch_size = 4096),
        ("parallelism=1", |c| c.parallelism = 1),
    ];

    for (label, tune) in variants {
        let other = build_dqp_seed_tuned(Fixture::TINY, FIXED_SEED, tune).await?;
        let m = name_to_vid(&other).await?;
        assert_eq!(
            reference,
            m,
            "VIDs moved under {label}, so a Tier-3 lever over that knob cannot \
             compare bags containing node identity. {}",
            describe(&reference, &m)
        );
    }
    Ok(())
}

/// Guards the experiment against measuring nothing.
///
/// If `id(p)` returned a constant, or something derived from the row's own
/// properties, every assertion above would hold trivially and the recorded
/// answer would be worthless. This pins that VIDs are genuinely distinct
/// per-row values.
#[tokio::test(flavor = "multi_thread")]
async fn the_experiment_is_measuring_real_vids() -> anyhow::Result<()> {
    let db = build_dqp_seed_for_with(Fixture::TINY, FIXED_SEED).await?;
    let m = name_to_vid(&db).await?;

    let distinct: std::collections::BTreeSet<_> = m.values().collect();
    assert_eq!(
        distinct.len(),
        m.len(),
        "VIDs are not unique per row ({} distinct for {} rows) — the comparison \
         above would not detect a re-assignment",
        distinct.len(),
        m.len()
    );
    assert!(
        m.len() > 100,
        "only {} rows compared; too few to call the result a measurement",
        m.len()
    );
    Ok(())
}

/// Repeats the experiment to catch a race that only sometimes reorders.
///
/// A single comparison passing proves nothing about a nondeterminism that
/// manifests in, say, one build in five — and the fixture's bulk inserts are
/// batched, which is exactly where a concurrent id allocator would show up.
/// Three extra builds is cheap on the tiny tier (~0.16 s each) and turns a
/// single observation into a weak but real repetition.
#[tokio::test(flavor = "multi_thread")]
async fn vid_determinism_is_stable_across_repeats() -> anyhow::Result<()> {
    let base = build_dqp_seed_for_with(Fixture::TINY, FIXED_SEED).await?;
    let reference = name_to_vid(&base).await?;

    for attempt in 0..3 {
        let other = build_dqp_seed_for_with(Fixture::TINY, FIXED_SEED).await?;
        let m = name_to_vid(&other).await?;
        assert_eq!(
            reference,
            m,
            "build {attempt} diverged from the reference build. {}",
            describe(&reference, &m)
        );
    }
    Ok(())
}
