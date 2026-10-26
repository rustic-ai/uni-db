// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #315: a shared probabilistic dependency is reported by what the
//! engine did with it.
//!
//! Two suppliers depend on one facility event (p = 0.20). Ordinary mode
//! folds the two proofs as independent and returns 0.36, an overestimate,
//! and warns. Exact mode resolves the shared event and returns 0.20 — the
//! overestimation warning was raised there too, though nothing was
//! overestimated. Exact resolution is now an informational status, and a
//! group exact inference could not resolve (over the BDD variable budget)
//! still warns, with the reason and the limits.

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::LocyConfig;
use uni_locy::{RuntimeWarning, RuntimeWarningCode};

const PROGRAM: &str = "CREATE RULE factory_event AS
  MATCH (f:Facility {id: 'F1'})
  YIELD KEY f, 0.2 AS probability PROB
CREATE RULE exposed AS
  MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility)
  WHERE f IS factory_event
  YIELD KEY s, probability AS risk PROB
CREATE RULE portfolio AS
  MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility {id: 'F1'})
  WHERE s IS exposed
  FOLD p = MNOR(risk)
  YIELD KEY f, p AS probability PROB
QUERY portfolio RETURN f.id AS factory, probability";

async fn run(config: LocyConfig) -> Result<(f64, Vec<RuntimeWarning>)> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Facility {id: 'F1'})").await?;
    tx.execute("MATCH (f:Facility {id: 'F1'}) CREATE (:Supplier {id: 'S1'})-[:DEPENDS_ON]->(f)")
        .await?;
    tx.execute("MATCH (f:Facility {id: 'F1'}) CREATE (:Supplier {id: 'S2'})-[:DEPENDS_ON]->(f)")
        .await?;
    tx.commit().await?;
    let result = db
        .session()
        .locy_with(PROGRAM)
        .with_config(config)
        .run()
        .await?;
    let rows = result.rows().expect("QUERY rows");
    assert_eq!(rows.len(), 1, "one facility: {rows:?}");
    let p = rows[0]
        .get("probability")
        .and_then(uni_db::Value::as_f64)
        .expect("probability");
    Ok((p, result.warnings().to_vec()))
}

fn config(exact: bool) -> LocyConfig {
    LocyConfig {
        exact_probability: exact,
        strict_probability_domain: true,
        ..Default::default()
    }
}

fn for_rule<'a>(warnings: &'a [RuntimeWarning], code: &str) -> Vec<&'a RuntimeWarning> {
    warnings
        .iter()
        .filter(|w| w.code.as_str() == code && w.rule_name == "portfolio")
        .collect()
}

/// Ordinary mode: 0.36, with the overestimation warning.
#[tokio::test]
async fn issue_315_ordinary_mode_warns_of_overestimate() -> Result<()> {
    let (p, warnings) = run(config(false)).await?;
    assert!((p - 0.36).abs() < 1e-9, "independence: 1 - 0.8^2; got {p}");
    let shared = for_rule(&warnings, "shared_probabilistic_dependency");
    assert_eq!(shared.len(), 1, "{warnings:?}");
    assert!(
        for_rule(&warnings, "shared_dependency_resolved").is_empty(),
        "nothing was resolved: {warnings:?}"
    );
    Ok(())
}

/// Exact mode: 0.20, reported as resolved, not as a possible overestimate.
#[tokio::test]
async fn issue_315_exact_mode_reports_resolution_not_overestimate() -> Result<()> {
    let (p, warnings) = run(config(true)).await?;
    assert!((p - 0.2).abs() < 1e-9, "one shared event: 0.2; got {p}");
    assert!(
        for_rule(&warnings, "shared_probabilistic_dependency").is_empty(),
        "exact inference resolved the dependency; no overestimate to warn of: {warnings:?}"
    );
    assert_eq!(
        for_rule(&warnings, "shared_dependency_resolved").len(),
        1,
        "the resolution is reported, not silently dropped: {warnings:?}"
    );
    Ok(())
}

/// Structured fields: what was detected, how it was evaluated, how it ended.
#[tokio::test]
async fn issue_315_diagnostics_are_structured() -> Result<()> {
    use uni_locy::{DependencyResolution, InferenceMode};

    let (_, warnings) = run(config(false)).await?;
    let w = for_rule(&warnings, "shared_probabilistic_dependency")[0];
    assert!(!w.code.is_informational());
    let d = w.probability.as_ref().expect("dependency diagnostic");
    assert_eq!(d.inference_mode, InferenceMode::Approximate);
    assert_eq!(d.resolution, DependencyResolution::NotAttempted);
    assert!(d.reason.is_some());
    assert_eq!(d.groups_detected, 1);

    let (_, warnings) = run(config(true)).await?;
    let w = for_rule(&warnings, "shared_dependency_resolved")[0];
    assert_eq!(w.code, RuntimeWarningCode::SharedDependencyResolved);
    assert!(w.code.is_informational());
    let d = w.probability.as_ref().expect("dependency diagnostic");
    assert_eq!(d.inference_mode, InferenceMode::Exact);
    assert_eq!(d.resolution, DependencyResolution::Completed);
    assert_eq!(d.groups_detected, 1);
    assert_eq!(d.groups_fallen_back, 0);
    assert_eq!(d.configured_bdd_variable_limit, Some(1000));
    Ok(())
}

/// Exact mode over the variable budget: the group falls back, and the
/// overestimation warning stays, saying why and under which limits.
#[tokio::test]
async fn issue_315_exact_fallback_still_warns_with_reason() -> Result<()> {
    let mut cfg = config(true);
    cfg.max_bdd_variables = 1;
    let (p, warnings) = run(cfg).await?;
    assert!(
        (p - 0.36).abs() < 1e-9,
        "fell back to independence; got {p}"
    );
    let shared = for_rule(&warnings, "shared_probabilistic_dependency");
    assert_eq!(shared.len(), 1, "{warnings:?}");
    assert!(
        for_rule(&warnings, "shared_dependency_resolved").is_empty(),
        "{warnings:?}"
    );
    assert!(
        !for_rule(&warnings, "bdd_limit_exceeded").is_empty(),
        "{warnings:?}"
    );
    let d = shared[0]
        .probability
        .as_ref()
        .expect("dependency diagnostic");
    assert_eq!(d.inference_mode, uni_locy::InferenceMode::Exact);
    assert_eq!(d.resolution, uni_locy::DependencyResolution::Fallback);
    assert_eq!(d.reason.as_deref(), Some("bdd_variable_limit_exceeded"));
    assert_eq!(d.configured_bdd_variable_limit, Some(1));
    assert_eq!(d.groups_fallen_back, 1);
    assert!(shared[0].variable_count.is_some_and(|n| n > 1));
    Ok(())
}
