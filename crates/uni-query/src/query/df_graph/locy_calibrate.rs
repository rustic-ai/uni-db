// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase C C2: `CALIBRATE` statement runtime.
//!
//! For each `CompiledCalibrate` command, this module:
//!
//! 1. Builds a Cypher `MATCH pattern [WHERE expr] RETURN <input vars>, <target>`
//!    query from the compiled command's pieces.
//! 2. Executes it through the same `execute_cypher_inline` path used for
//!    Phase 4 inline Cypher commands — gets back a list of `FactRow`s.
//! 3. Builds `ClassifyInput`s from each row using the model's INPUT
//!    binding names, then batch-calls the registered classifier.
//! 4. Converts target column values to bool labels.
//! 5. Splits train / holdout deterministically (index-based modulo).
//! 6. Fits the chosen `CalibratorFitter` on the training half.
//! 7. Computes Brier + ECE on the holdout pre- and post-calibration.
//! 8. Returns a [`uni_locy::CalibrationResult`] for surfacing in the
//!    `LocyResult.command_results` slot.

use std::collections::HashMap;
use std::sync::Arc;

use uni_common::Value;
use uni_cypher::ast::{Clause, Expr, MatchClause, ReturnClause, ReturnItem, Statement};
use uni_cypher::locy_ast::CalibrationMethod;
use uni_locy::{
    BetaFitter, CalibrationMethodKind, CalibrationResult, CalibratorFitter, ClassifierRegistry,
    ClassifyInput, CompiledCalibrate, CompiledModel, FactRow, FeatureValue, IdentityCalibrator,
    IsotonicFitter, NeuralClassifier, PlattFitter, TemperatureFitter, brier_score,
    expected_calibration_error,
};

/// Number of bins used for ECE reporting in the CALIBRATE holdout
/// summary. The Phase C C2 result block is informational; C3
/// `VALIDATE` will offer richer (debiased / classwise) variants.
const ECE_BINS: usize = 10;

/// Errors specific to `CALIBRATE` runtime. Wrapped into a
/// `DataFusionError::Execution` at the dispatch site.
#[derive(Debug)]
pub enum CalibrateRuntimeError {
    ClassifierMissing {
        model_name: String,
    },
    UnknownModelInCatalog {
        model_name: String,
    },
    EmptyDataset {
        model_name: String,
    },
    /// An INPUT's value was null on every collected row, so the classifier
    /// would be fitted on no information.
    NullFeature {
        model_name: String,
        input: String,
    },
    InsufficientData {
        model_name: String,
        train: usize,
        holdout: usize,
    },
    FitFailure {
        model_name: String,
        message: String,
    },
}

impl std::fmt::Display for CalibrateRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClassifierMissing { model_name } => write!(
                f,
                "CALIBRATE: classifier '{}' not registered; \
                 add it to LocyConfig::classifier_registry before evaluating",
                model_name
            ),
            Self::UnknownModelInCatalog { model_name } => write!(
                f,
                "CALIBRATE: model '{}' not in CompiledProgram.model_catalog \
                 (compiler should have rejected this earlier)",
                model_name
            ),
            Self::EmptyDataset { model_name } => write!(
                f,
                "CALIBRATE: model '{}' MATCH pattern produced zero rows",
                model_name
            ),
            Self::NullFeature { model_name, input } => write!(
                f,
                "CALIBRATE: model '{model_name}' INPUT '{input}' is null on every row; \
                 check the model's FEATURES expression for it against the MATCH pattern"
            ),
            Self::InsufficientData {
                model_name,
                train,
                holdout,
            } => write!(
                f,
                "CALIBRATE: model '{model_name}' needs at least 1 sample in each \
                 split (got train={train}, holdout={holdout}); increase the data \
                 set or pick a different HOLDOUT fraction"
            ),
            Self::FitFailure {
                model_name,
                message,
            } => {
                write!(f, "CALIBRATE: model '{model_name}' fitter error: {message}")
            }
        }
    }
}

impl std::error::Error for CalibrateRuntimeError {}

/// Build a Cypher `Query` from the CALIBRATE command's pattern + WHERE +
/// projected values. The projection returns one column per model INPUT
/// binding, named after it, followed by the TARGET expression.
///
/// An INPUT's value is the model's FEATURES expression at its position, as a
/// call site `model(n.score)` passes its arguments; a model without FEATURES
/// passes the INPUT variable itself.
fn build_collection_query(
    cmd: &CompiledCalibrate,
    model: &CompiledModel,
) -> uni_cypher::ast::Query {
    let mut items: Vec<ReturnItem> = Vec::with_capacity(model.inputs.len() + 1);
    for (i, binding) in model.inputs.iter().enumerate() {
        let expr = model
            .features
            .get(i)
            .cloned()
            .unwrap_or_else(|| Expr::Variable(binding.variable.clone()));
        items.push(ReturnItem::Expr {
            expr,
            alias: Some(binding.variable.clone()),
            source_text: None,
        });
    }
    items.push(ReturnItem::Expr {
        expr: cmd.target_expr.clone(),
        alias: Some("__calibrate_target".to_string()),
        source_text: None,
    });
    let stmt = Statement {
        clauses: vec![
            Clause::Match(MatchClause {
                optional: false,
                pattern: cmd.pattern.clone(),
                where_clause: cmd.where_expr.clone(),
                for_update: false,
            }),
            Clause::Return(ReturnClause {
                distinct: false,
                items,
                order_by: None,
                skip: None,
                limit: None,
            }),
        ],
    };
    uni_cypher::ast::Query::Single(stmt)
}

/// Pull the FactRow value for column `name` and convert to FeatureValue.
fn row_to_feature(row: &FactRow, name: &str) -> FeatureValue {
    match row.get(name) {
        Some(Value::Float(f)) => FeatureValue::Float(*f),
        Some(Value::Int(i)) => FeatureValue::Int(*i),
        Some(Value::String(s)) => FeatureValue::String(s.clone()),
        Some(Value::Bool(b)) => FeatureValue::Bool(*b),
        Some(Value::Null) | None => FeatureValue::Null,
        // A node is passed as its vid, as it is at a rule's call site.
        Some(Value::Node(node)) => FeatureValue::Int(node.vid.as_u64() as i64),
        // Other Value variants (List, Map, Edge, …) fall back to
        // Null in this slice — Slice 3+ may extend FeatureValue.
        Some(_) => FeatureValue::Null,
    }
}

/// Convert a target Value to a bool label. Non-null truthy values
/// (true, non-zero numbers, non-empty strings) become 1; null /
/// false / 0 become 0. A string spelling a false boolean (`'false'`, `'0'`,
/// any case) is false: a label column loaded as text would otherwise read
/// every row as positive.
pub(crate) fn target_to_label(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::Int(i)) => *i != 0,
        Some(Value::Float(f)) => *f != 0.0,
        Some(Value::String(s)) => {
            let s = s.trim();
            !(s.is_empty() || s == "0" || s.eq_ignore_ascii_case("false"))
        }
        Some(Value::Null) | None => false,
        Some(_) => false,
    }
}

/// Dispatch the chosen calibration method to its fitter, run the fit,
/// wrap the resulting `Arc<dyn Calibrator>` in a `CalibrateRuntimeError`
/// on failure.
fn fit_method(
    method: CalibrationMethod,
    preds: &[f64],
    labels: &[bool],
    model_name: &str,
) -> Result<Arc<dyn uni_locy::Calibrator>, CalibrateRuntimeError> {
    let result = match method {
        CalibrationMethod::PlattScaling => PlattFitter.fit(preds, labels),
        CalibrationMethod::IsotonicRegression => IsotonicFitter.fit(preds, labels),
        CalibrationMethod::TemperatureScaling => TemperatureFitter.fit(preds, labels),
        CalibrationMethod::BetaCalibration => BetaFitter.fit(preds, labels),
        CalibrationMethod::Conformal { alpha } => {
            uni_locy::calibration::ConformalFitter { alpha }.fit(preds, labels)
        }
        CalibrationMethod::None => {
            // Explicit "no-op" — caller asked for identity. Useful for
            // exercising the CALIBRATE plumbing without modeling.
            Ok(Arc::new(IdentityCalibrator) as Arc<dyn uni_locy::Calibrator>)
        }
        CalibrationMethod::Dirichlet => {
            // Phase D D-C1d surface: the grammar accepts the keyword,
            // but the binary CALIBRATE pipeline can't drive a
            // multi-class fit — the trait expects `labels: &[bool]`
            // and `preds: &[f64]`, whereas Dirichlet needs
            // `labels: &[u32]` + `preds: &[Vec<f64>]`. Pending a
            // surface form for multi-class CALIBRATE, callers should
            // instantiate `DirichletFitter` directly via the Rust
            // library API.
            Err(uni_locy::calibration::CalibrationError::NumericIssue(
                "Dirichlet is multi-class; the binary CALIBRATE statement \
                 cannot fit it. Use `uni_locy::calibration::DirichletFitter` \
                 directly until the multi-class CALIBRATE surface form ships.",
            ))
        }
    };
    result.map_err(|e| CalibrateRuntimeError::FitFailure {
        model_name: model_name.to_string(),
        message: e.to_string(),
    })
}

/// Match the chosen method to its [`CalibrationMethodKind`] for the
/// returned result block.
fn method_kind(method: CalibrationMethod) -> CalibrationMethodKind {
    match method {
        CalibrationMethod::PlattScaling => CalibrationMethodKind::Platt,
        CalibrationMethod::IsotonicRegression => CalibrationMethodKind::Isotonic,
        CalibrationMethod::TemperatureScaling => CalibrationMethodKind::Temperature,
        CalibrationMethod::BetaCalibration => CalibrationMethodKind::Beta,
        CalibrationMethod::Conformal { .. } => CalibrationMethodKind::Conformal,
        CalibrationMethod::Dirichlet => CalibrationMethodKind::Dirichlet,
        CalibrationMethod::None => CalibrationMethodKind::Identity,
    }
}

/// Run a `CALIBRATE` command end-to-end. The caller supplies an
/// already-collected (input_value, label) row set — typically by
/// driving the same `execute_cypher_inline` primitive used for Phase
/// 4 inline Cypher.
///
/// This separation keeps the runtime testable without standing up a
/// DataFusion session.
pub async fn run_calibrate(
    cmd: &CompiledCalibrate,
    model_catalog: &HashMap<String, CompiledModel>,
    classifier_registry: &Arc<ClassifierRegistry>,
    rows: Vec<FactRow>,
) -> Result<CalibrationResult, CalibrateRuntimeError> {
    let model = model_catalog.get(&cmd.model_name).ok_or_else(|| {
        CalibrateRuntimeError::UnknownModelInCatalog {
            model_name: cmd.model_name.clone(),
        }
    })?;
    let classifier: Arc<dyn NeuralClassifier> =
        classifier_registry
            .get(&cmd.model_name)
            .cloned()
            .ok_or_else(|| CalibrateRuntimeError::ClassifierMissing {
                model_name: cmd.model_name.clone(),
            })?;
    if rows.is_empty() {
        return Err(CalibrateRuntimeError::EmptyDataset {
            model_name: cmd.model_name.clone(),
        });
    }
    // Build ClassifyInputs and labels in row order.
    let mut inputs: Vec<ClassifyInput> = Vec::with_capacity(rows.len());
    let mut labels: Vec<bool> = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut features = HashMap::with_capacity(model.inputs.len());
        for binding in &model.inputs {
            features.insert(
                binding.variable.clone(),
                row_to_feature(row, &binding.variable),
            );
        }
        inputs.push(ClassifyInput { features });
        labels.push(target_to_label(row.get("__calibrate_target")));
    }
    for binding in &model.inputs {
        if inputs
            .iter()
            .all(|i| matches!(i.features.get(&binding.variable), Some(FeatureValue::Null)))
        {
            return Err(CalibrateRuntimeError::NullFeature {
                model_name: cmd.model_name.clone(),
                input: binding.variable.clone(),
            });
        }
    }
    // Classify everything once — same primitive Slice 3 uses for rule-body invocation.
    let predictions =
        classifier
            .classify(&inputs)
            .await
            .map_err(|e| CalibrateRuntimeError::FitFailure {
                model_name: cmd.model_name.clone(),
                message: e.to_string(),
            })?;
    if predictions.len() != labels.len() {
        return Err(CalibrateRuntimeError::FitFailure {
            model_name: cmd.model_name.clone(),
            message: format!(
                "classifier returned {} predictions for {} inputs",
                predictions.len(),
                labels.len()
            ),
        });
    }

    // Deterministic holdout split: the holdout takes the FIRST
    // ceil(n * holdout) rows in input order. A modulo-based stride
    // would alias with label patterns that have the same period
    // (e.g. label = `i % 2 == 0` aliases with stride 4), so prefix
    // selection keeps the split label-distribution-independent.
    // Tests rely on this exact behavior. Randomized splitting with
    // a seedable RNG is a follow-up.
    let n = predictions.len();
    let holdout_size = ((n as f64) * cmd.holdout).ceil().max(1.0) as usize;
    let holdout_size = holdout_size.min(n);
    let mut train_preds: Vec<f64> = Vec::new();
    let mut train_labels: Vec<bool> = Vec::new();
    let mut holdout_preds: Vec<f64> = Vec::new();
    let mut holdout_labels: Vec<bool> = Vec::new();
    for (i, (p, y)) in predictions.iter().zip(labels.iter()).enumerate() {
        if i < holdout_size {
            holdout_preds.push(*p);
            holdout_labels.push(*y);
        } else {
            train_preds.push(*p);
            train_labels.push(*y);
        }
    }
    if train_preds.is_empty() || holdout_preds.is_empty() {
        return Err(CalibrateRuntimeError::InsufficientData {
            model_name: cmd.model_name.clone(),
            train: train_preds.len(),
            holdout: holdout_preds.len(),
        });
    }

    let calibrator = fit_method(cmd.method, &train_preds, &train_labels, &cmd.model_name)?;
    let raw_brier = brier_score(&holdout_preds, &holdout_labels);
    let raw_ece = expected_calibration_error(&holdout_preds, &holdout_labels, ECE_BINS);
    let calibrated: Vec<f64> = calibrator.apply_batch(&holdout_preds);
    let calibrated_brier = brier_score(&calibrated, &holdout_labels);
    let calibrated_ece = expected_calibration_error(&calibrated, &holdout_labels, ECE_BINS);

    // Phase C C1a: surface the conformal quantile in the result row
    // for downstream EXPLAIN / band reporting. Only populated when
    // the method is Conformal — extracted via the calibrator's
    // confidence_band probe at p = 0.5 (the band half-width equals
    // the quantile regardless of the probe point).
    let confidence_band_quantile = calibrator
        .confidence_band(0.5)
        .map(|band| (band.upper - band.lower) / 2.0);

    Ok(CalibrationResult {
        model_name: cmd.model_name.clone(),
        method: method_kind(cmd.method),
        n_samples: predictions.len(),
        holdout_size: holdout_preds.len(),
        calibrator,
        raw_brier,
        raw_ece,
        calibrated_brier,
        calibrated_ece,
        confidence_band_quantile,
    })
}

/// Export the collection-query builder for the dispatch layer that
/// wraps `execute_cypher_inline`.
pub fn calibrate_collection_query(
    cmd: &CompiledCalibrate,
    model: &CompiledModel,
) -> uni_cypher::ast::Query {
    build_collection_query(cmd, model)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_cypher::locy_ast::{CalibrationMethod as AstCalibration, OutputType};
    use uni_locy::{CompiledInputBinding, MockClassifier};

    fn fact_row(pairs: &[(&str, Value)]) -> FactRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn model_with_one_input() -> CompiledModel {
        CompiledModel {
            name: "scorer".into(),
            inputs: vec![CompiledInputBinding {
                variable: "s".into(),
                label: Some("Supplier".into()),
            }],
            embedder_alias: None,
            features: vec![],
            path_context: None,
            output_type: OutputType::Prob,
            output_name: "risk".into(),
            xervo_alias: "classify/test".into(),
            calibration: None,
            version: None,
            annotations: Default::default(),
        }
    }

    fn dummy_pattern() -> uni_cypher::ast::Pattern {
        // A minimal pattern; the actual MATCH wouldn't be executed in
        // these tests since we feed `run_calibrate` rows directly.
        uni_cypher::ast::Pattern { paths: vec![] }
    }

    fn cmd(method: AstCalibration) -> CompiledCalibrate {
        CompiledCalibrate {
            model_name: "scorer".into(),
            pattern: dummy_pattern(),
            where_expr: None,
            target_expr: Expr::Variable("label".into()),
            method,
            holdout: 0.25,
        }
    }

    #[tokio::test]
    async fn calibrate_constant_classifier_improves_ece() {
        // Build a dataset of 100 rows, alternating labels, with a
        // mock classifier that always returns 0.95.
        let mut catalog = HashMap::new();
        catalog.insert("scorer".to_string(), model_with_one_input());
        let mut registry = ClassifierRegistry::new();
        let c: Arc<dyn NeuralClassifier> =
            Arc::new(MockClassifier::constant("classify/test", 0.95));
        registry.insert("scorer".into(), c);
        let registry = Arc::new(registry);
        let rows: Vec<FactRow> = (0..100)
            .map(|i| {
                fact_row(&[
                    ("s", Value::Int(i as i64)),
                    ("__calibrate_target", Value::Bool(i % 2 == 0)),
                ])
            })
            .collect();
        let result = run_calibrate(
            &cmd(AstCalibration::PlattScaling),
            &catalog,
            &registry,
            rows,
        )
        .await
        .unwrap();
        assert_eq!(result.model_name, "scorer");
        assert_eq!(result.method, CalibrationMethodKind::Platt);
        // Phase C gate: ECE should drop by at least 50% after Platt.
        assert!(
            result.calibrated_ece < result.raw_ece * 0.5,
            "Platt should reduce ECE by ≥50%: raw={} cal={}",
            result.raw_ece,
            result.calibrated_ece
        );
    }

    fn scored_model() -> CompiledModel {
        CompiledModel {
            features: vec![Expr::Property(
                Box::new(Expr::Variable("s".into())),
                "score".into(),
            )],
            ..model_with_one_input()
        }
    }

    #[test]
    fn collection_query_projects_features_under_input_names() {
        let query = build_collection_query(&cmd(AstCalibration::PlattScaling), &scored_model());
        let uni_cypher::ast::Query::Single(stmt) = query else {
            panic!("expected a single statement");
        };
        let Clause::Return(ret) = &stmt.clauses[1] else {
            panic!("expected RETURN");
        };
        match &ret.items[0] {
            ReturnItem::Expr { expr, alias, .. } => {
                assert_eq!(alias.as_deref(), Some("s"));
                assert!(
                    matches!(expr, Expr::Property(base, p) if p == "score"
                        && matches!(base.as_ref(), Expr::Variable(v) if v == "s")),
                    "the FEATURES expression supplies INPUT s: {expr:?}"
                );
            }
            other => panic!("unexpected item {other:?}"),
        }
        // Without FEATURES the INPUT variable itself is projected.
        let query =
            build_collection_query(&cmd(AstCalibration::PlattScaling), &model_with_one_input());
        let uni_cypher::ast::Query::Single(stmt) = query else {
            panic!("expected a single statement");
        };
        let Clause::Return(ret) = &stmt.clauses[1] else {
            panic!("expected RETURN");
        };
        assert!(matches!(
            &ret.items[0],
            ReturnItem::Expr { expr: Expr::Variable(v), .. } if v == "s"
        ));
    }

    #[test]
    fn a_node_feature_is_its_vid() {
        let node = uni_common::Node {
            vid: uni_common::core::id::Vid::new(42),
            labels: vec!["Supplier".into()],
            properties: Default::default(),
        };
        let row = fact_row(&[("s", Value::Node(node))]);
        assert!(matches!(row_to_feature(&row, "s"), FeatureValue::Int(42)));
    }

    #[test]
    fn a_false_string_target_is_a_negative_label() {
        for (text, label) in [
            ("true", true),
            ("FALSE", false),
            ("0", false),
            ("", false),
            ("yes", true),
        ] {
            assert_eq!(
                target_to_label(Some(&Value::String(text.into()))),
                label,
                "{text:?}"
            );
        }
    }

    #[tokio::test]
    async fn calibrate_rejects_an_all_null_feature() {
        let mut catalog = HashMap::new();
        catalog.insert("scorer".to_string(), scored_model());
        let mut registry = ClassifierRegistry::new();
        let c: Arc<dyn NeuralClassifier> = Arc::new(MockClassifier::constant("classify/test", 0.5));
        registry.insert("scorer".into(), c);
        let registry = Arc::new(registry);
        let rows: Vec<FactRow> = (0..10)
            .map(|i| {
                fact_row(&[
                    ("s", Value::Null),
                    ("__calibrate_target", Value::Bool(i % 2 == 0)),
                ])
            })
            .collect();
        let err = run_calibrate(
            &cmd(AstCalibration::PlattScaling),
            &catalog,
            &registry,
            rows,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, CalibrateRuntimeError::NullFeature { input, .. } if input == "s"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn calibrate_missing_classifier_errors() {
        let mut catalog = HashMap::new();
        catalog.insert("scorer".to_string(), model_with_one_input());
        let registry = Arc::new(ClassifierRegistry::new());
        let rows = vec![fact_row(&[
            ("s", Value::Int(1)),
            ("__calibrate_target", Value::Bool(true)),
        ])];
        let err = run_calibrate(
            &cmd(AstCalibration::PlattScaling),
            &catalog,
            &registry,
            rows,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            CalibrateRuntimeError::ClassifierMissing { .. }
        ));
    }

    #[tokio::test]
    async fn calibrate_empty_dataset_errors() {
        let mut catalog = HashMap::new();
        catalog.insert("scorer".to_string(), model_with_one_input());
        let mut registry = ClassifierRegistry::new();
        let c: Arc<dyn NeuralClassifier> = Arc::new(MockClassifier::constant("classify/test", 0.5));
        registry.insert("scorer".into(), c);
        let registry = Arc::new(registry);
        let err = run_calibrate(
            &cmd(AstCalibration::PlattScaling),
            &catalog,
            &registry,
            vec![],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CalibrateRuntimeError::EmptyDataset { .. }));
    }
}
