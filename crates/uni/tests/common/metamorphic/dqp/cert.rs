// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! CERT — cardinality restriction monotonicity.
//!
//! `|Q WHERE p AND q| <= |Q WHERE p|`. Adding a conjunct can only remove rows,
//! never add one, and every row the narrower query returns must appear in the
//! wider query's bag with at least the same multiplicity.
//!
//! # Why this is an assertion and not a harness
//!
//! CERT needs no lever. Both queries run against **one** instance in **one**
//! state, so there is no transition to drive and no second execution path to
//! prepare — it is a law relating two queries, in the family of `structural.rs`
//! rather than of the DQP levers. What it borrows from DQP is the *fixture* and
//! the *generator*, because those are what make it non-vacuous at scale.
//!
//! It also needs no new query builders: `Case::partition_query(Partition::True)`
//! is already `WHERE base AND predicate`, and `Case::base_query` is `WHERE base`.
//! The pair is exactly CERT's two sides, and `diff::bag_is_subset` is exactly its
//! comparator.
//!
//! # What CERT catches that bag equality does not
//!
//! The DQP levers compare one query across two execution paths, so a defect that
//! is *stable* — a filter that wrongly drops rows the same way every time — is
//! invisible to them: both sides agree, wrongly. CERT constrains the relationship
//! between two *different* queries on one path, so a conjunction that somehow
//! produces rows its own weaker form does not is caught without needing a second
//! path to disagree.
//!
//! The converse also holds, which is why this is additive rather than a
//! replacement: CERT is a one-sided inequality and cannot see a filter that drops
//! too many rows.

use std::cell::Cell;

use proptest::strategy::Strategy;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::driver::{CaseKind, smoke_cases, soak_cases, strategy_for};
use super::lever::observe;
use super::seed::{Fixture, build_dqp_seed_for};
use crate::diff::bag_is_subset;
use crate::querygen::{Case, Partition};

/// Fraction of cases in which the conjunct must genuinely *narrow* the result.
///
/// The floor exists for the same reason the levers have an activation floor: a
/// subset assertion over two identical bags passes for free, and a generator
/// drifting toward always-true predicates would leave this oracle green while it
/// tested nothing. Set below the levers' 0.80 because a predicate that legally
/// matches everything (or nothing, twice) is a normal draw here rather than a
/// malfunction — `arb_pred` has no obligation to be selective.
const MIN_NARROWING_RATE: f64 = 0.30;

/// Runs the CERT law over `cases` generated queries against the tiny fixture.
///
/// # Panics
///
/// Panics (failing the test) on a monotonicity violation, or if too few cases
/// narrowed for the run to have demonstrated anything.
fn drive_cert(cases: u32, kind: CaseKind) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let db = rt
        .block_on(build_dqp_seed_for(Fixture::TINY))
        .expect("build dqp seed");

    let total = Cell::new(0u64);
    let narrowed = Cell::new(0u64);

    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    });

    let strategy = strategy_for(kind, Fixture::TINY);

    let outcome = runner.run(&strategy, |case: Case| {
        if let Some(why) = kind.inadmissible(&case) {
            return Err(TestCaseError::reject(why));
        }

        let wider = case.base_query();
        let narrower = case.partition_query(Partition::True);

        let (w, n) = rt
            .block_on(async {
                let session = db.session();
                let w = observe(&session, &wider).await?;
                let n = observe(&session, &narrower).await?;
                Ok::<_, anyhow::Error>((w, n))
            })
            .map_err(|e| TestCaseError::fail(e.to_string()))?;

        total.set(total.get() + 1);
        if n.bag.total < w.bag.total {
            narrowed.set(narrowed.get() + 1);
        }

        // The cardinality half of the law, checked first because its failure
        // message is the readable one.
        if n.bag.total > w.bag.total {
            return Err(TestCaseError::fail(format!(
                "CERT violated: |WHERE p AND q| = {} exceeds |WHERE p| = {}\n  \
                 wider:    {}\n  narrower: {}",
                n.bag.total,
                w.bag.total,
                crate::querygen::render::render(&wider),
                crate::querygen::render::render(&narrower),
            )));
        }

        // The containment half. Strictly stronger: equal totals can still hide a
        // row swap, where the conjunct dropped one row and invented another.
        bag_is_subset(&n.bag, &w.bag).map_err(|d| {
            TestCaseError::fail(format!(
                "CERT violated: the narrower query returned rows absent from the \
                 wider one — the conjunct did not merely filter.\n{d}\n  \
                 wider:    {}\n  narrower: {}",
                crate::querygen::render::render(&wider),
                crate::querygen::render::render(&narrower),
            ))
        })?;

        Ok(())
    });

    let ran = total.get();
    let narrow = narrowed.get();
    let rate = if ran == 0 {
        0.0
    } else {
        narrow as f64 / ran as f64
    };
    eprintln!(
        "[dqp:cert] kind={} cases={ran} narrowed={narrow} ({:.1}%)",
        kind.name(),
        rate * 100.0
    );

    if let Err(e) = outcome {
        panic!("{e}");
    }

    assert!(
        rate >= MIN_NARROWING_RATE,
        "CERT ran {ran} cases but only {narrow} ({:.1}%) had a conjunct that \
         actually removed rows — below the {:.0}% floor. A subset assertion over \
         two identical bags passes for free, so this run demonstrated little: \
         either the generator drifted toward always-true predicates, or the \
         fixture stopped matching them.",
        rate * 100.0,
        MIN_NARROWING_RATE * 100.0
    );
}

/// PR lane.
#[test]
fn cert_smoke() {
    drive_cert(smoke_cases(), CaseKind::Plain);
}

/// Nightly volume.
#[test]
#[ignore = "soak: CERT cardinality monotonicity at nightly volume"]
fn cert_soak() {
    drive_cert(soak_cases(), CaseKind::Plain);
}

/// The pushdown predicate family, where a filter is lowered to Lance rather than
/// evaluated in process — a different code path for the same law.
#[test]
fn cert_pushdown_smoke() {
    drive_cert(smoke_cases(), CaseKind::Pushdown);
}

#[cfg(test)]
mod guards {
    use super::MIN_NARROWING_RATE;

    /// The narrowing floor must stay meaningful.
    ///
    /// At 0 this oracle would accept a run in which every conjunct was a no-op —
    /// i.e. it would assert `bag ⊆ bag` a few hundred times and report success.
    #[test]
    fn the_narrowing_floor_is_not_vacuous() {
        assert!(
            MIN_NARROWING_RATE > 0.0,
            "a zero narrowing floor makes CERT self-satisfying"
        );
    }
}
