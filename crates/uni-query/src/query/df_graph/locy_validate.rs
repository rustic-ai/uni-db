// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase C C3: `VALIDATE` statement runtime.
//!
//! For a `CompiledValidate { rule_name, prob_column, pattern, ... }`:
//!
//! 1. Build a Cypher `MATCH pattern [WHERE ...] RETURN <KEY vars>, target`
//!    query — this is the ground-truth source.
//! 2. Execute, pull `(key_tuple, label)` rows.
//! 3. Look up the rule's derived facts in `DerivedStore`, indexed by
//!    KEY column tuple → PROB column value.
//! 4. Join the two on the key tuple to produce `(prediction, label)`
//!    pairs (rows in either side without a match are dropped — this is
//!    intentional, matches sklearn semantics).
//! 5. Compute each requested metric via the `uni_locy::calibration`
//!    library functions.
//!
//! Unlike `CALIBRATE`, this never invokes a classifier or fits
//! anything — the rule has already been evaluated by the fixpoint
//! loop and the metric pass just *measures*.

use std::collections::HashMap;
use std::sync::Arc;

use uni_common::Value;
use uni_cypher::ast::{Clause, Expr, MatchClause, ReturnClause, ReturnItem, Statement};
use uni_cypher::locy_ast::ValidationMetric;
use uni_locy::{
    CompiledValidate, FactRow, ValidationResult, accuracy, auc, brier_score, debiased_ece,
    expected_calibration_error, log_loss,
};

/// Number of bins for ECE / debiased_ECE in the VALIDATE pass.
const ECE_BINS: usize = 10;

#[derive(Debug)]
pub enum ValidateRuntimeError {
    RuleNotDerived { rule_name: String },
    EmptyDataset { rule_name: String },
    JoinKeysMissing { rule_name: String, key: String },
}

impl std::fmt::Display for ValidateRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuleNotDerived { rule_name } => write!(
                f,
                "VALIDATE: rule '{rule_name}' has no derived facts; \
                 ensure it appears in a stratum before VALIDATE"
            ),
            Self::EmptyDataset { rule_name } => write!(
                f,
                "VALIDATE: rule '{rule_name}' produced no \
                 (prediction, label) pairs (empty join)"
            ),
            Self::JoinKeysMissing { rule_name, key } => write!(
                f,
                "VALIDATE: rule '{rule_name}' KEY column '{key}' missing \
                 from either the rule's derived facts or the TARGET query rows"
            ),
        }
    }
}

impl std::error::Error for ValidateRuntimeError {}

/// Build the ground-truth collection query: the MATCH pattern + WHERE
/// + RETURN of the TARGET expression plus all KEY-shaped projections
///   needed to join with the rule's derived facts.
///
/// We use the YIELD-key variable names as both the pattern-bound
/// variables and the RETURN aliases — this mirrors the rule's own
/// KEY shape so the join can match by name.
pub fn validate_collection_query(
    cmd: &CompiledValidate,
    key_columns: &[String],
) -> uni_cypher::ast::Query {
    let mut items: Vec<ReturnItem> = Vec::with_capacity(key_columns.len() + 1);
    for col in key_columns {
        items.push(ReturnItem::Expr {
            expr: Expr::Variable(col.clone()),
            alias: Some(col.clone()),
            source_text: None,
        });
    }
    items.push(ReturnItem::Expr {
        expr: cmd.target_expr.clone(),
        alias: Some("__validate_target".to_string()),
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

/// Convert a target Value to a bool label. Same rules as CALIBRATE.
fn target_to_label(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::Int(i)) => *i != 0,
        Some(Value::Float(f)) => *f != 0.0,
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Null) | None => false,
        Some(_) => false,
    }
}

/// Canonicalize a Value for join-key comparison.
///
/// The rule's KEY column (typically a `Node` bound by the rule's
/// MATCH) can be stored either as a `Value::Node` (full node carrier)
/// or as `Value::Int(vid)` (just the integer vid) depending on which
/// runtime path produced it — DerivedStore-to-FactRow conversion
/// keeps vids as `Int`, while a fresh Cypher MATCH returns `Node`.
/// To make the join work in both directions we extract the vid where
/// applicable and stringify other primitive types directly.
fn canonical_key(v: &Value) -> String {
    // Critical: a rule's KEY column carrying a node-bound variable
    // may show up as either `Value::Node` (after a fresh Cypher MATCH)
    // or `Value::Int(vid)` (after the DerivedStore record-batch
    // round-trip strips the rich Node value down to its vid). Both
    // refer to the same node — canonicalize to the bare integer.
    match v {
        Value::Node(n) => format!("v:{}", n.vid),
        Value::Edge(e) => format!("e:{}", e.eid),
        // Treat any non-node integer as a potential vid for the same
        // join. False positives here are tolerable because the rule's
        // KEY column ALWAYS produces semantically-equivalent values
        // under both encoding paths.
        Value::Int(i) => format!("v:{i}"),
        Value::Float(f) => format!("f:{f}"),
        Value::Bool(b) => format!("b:{b}"),
        Value::String(s) => format!("s:{s}"),
        Value::Null => "null".into(),
        // A vertex also reaches here as a map carrying its id, and it has to
        // land on the same key as the two arms above or the two join sides
        // never meet.
        other => match other.entity_vid() {
            Some(vid) => format!("v:{}", vid.as_u64()),
            // Everything else — notably a path-valued KEY, which arrives as a
            // nested map on both sides — takes the canonical rendering. This was
            // `format!("{other:?}")`, and `Debug` over the `HashMap` inside a
            // path rendered its keys in a different order on each side, so
            // VALIDATE scored a random subset of its target rows and reported a
            // metric over it without any indication rows had been dropped
            // (#236).
            None => other.canonical_string(),
        },
    }
}

/// Build a stable join-key string from a row's KEY column values.
/// `canonical_key` normalizes Node vs. Int(vid) on each side.
fn join_key(row: &FactRow, key_columns: &[String]) -> Option<String> {
    let mut parts = Vec::with_capacity(key_columns.len());
    for col in key_columns {
        let v = row.get(col)?;
        parts.push(canonical_key(v));
    }
    Some(parts.join("|"))
}

/// Execute the validation pass. `rule_facts` is the rule's derived
/// fact set (read from `LocyResult.derived[rule_name]`); `target_rows`
/// is the Cypher MATCH+TARGET query result.
pub fn run_validate(
    cmd: &CompiledValidate,
    rule_key_columns: &[String],
    rule_facts: &[FactRow],
    target_rows: Vec<FactRow>,
) -> Result<ValidationResult, ValidateRuntimeError> {
    if rule_facts.is_empty() {
        return Err(ValidateRuntimeError::RuleNotDerived {
            rule_name: cmd.rule_name.clone(),
        });
    }
    // Index rule facts by KEY tuple → PROB value.
    let mut by_key: HashMap<String, f64> = HashMap::with_capacity(rule_facts.len());
    for row in rule_facts {
        let key = join_key(row, rule_key_columns).ok_or_else(|| {
            ValidateRuntimeError::JoinKeysMissing {
                rule_name: cmd.rule_name.clone(),
                key: rule_key_columns.join(","),
            }
        })?;
        let prob = match row.get(&cmd.prob_column) {
            Some(Value::Float(f)) => *f,
            Some(Value::Int(i)) => *i as f64,
            _ => continue,
        };
        by_key.insert(key, prob.clamp(0.0, 1.0));
    }

    // Join target rows onto the by_key index.
    let mut preds: Vec<f64> = Vec::new();
    let mut labels: Vec<bool> = Vec::new();
    for row in &target_rows {
        let key = join_key(row, rule_key_columns).ok_or_else(|| {
            ValidateRuntimeError::JoinKeysMissing {
                rule_name: cmd.rule_name.clone(),
                key: rule_key_columns.join(","),
            }
        })?;
        if let Some(&pred) = by_key.get(&key) {
            preds.push(pred);
            labels.push(target_to_label(row.get("__validate_target")));
        }
    }
    if preds.is_empty() {
        // Diagnostic: surface what we saw on each side of the join so
        // mismatches are debuggable from the error message.
        let rule_sample = rule_facts
            .first()
            .map(|r| r.keys().cloned().collect::<Vec<_>>().join(","));
        let target_sample = target_rows
            .first()
            .map(|r| r.keys().cloned().collect::<Vec<_>>().join(","));
        tracing::warn!(
            "VALIDATE empty join for rule '{}'. rule_facts={}, target_rows={}, \
             rule_cols={:?}, target_cols={:?}, key_columns={:?}, \
             rule_key_sample={:?}, target_key_sample={:?}",
            cmd.rule_name,
            rule_facts.len(),
            target_rows.len(),
            rule_sample,
            target_sample,
            rule_key_columns,
            rule_facts
                .first()
                .and_then(|r| r.get(&rule_key_columns[0]).cloned()),
            target_rows
                .first()
                .and_then(|r| r.get(&rule_key_columns[0]).cloned()),
        );
        return Err(ValidateRuntimeError::EmptyDataset {
            rule_name: cmd.rule_name.clone(),
        });
    }
    let mut metrics_out: Vec<(ValidationMetric, f64)> = Vec::with_capacity(cmd.metrics.len());
    for m in &cmd.metrics {
        let v = match m {
            ValidationMetric::BrierScore => brier_score(&preds, &labels),
            ValidationMetric::LogLoss => log_loss(&preds, &labels),
            ValidationMetric::Ece => expected_calibration_error(&preds, &labels, ECE_BINS),
            ValidationMetric::DebiasedEce => debiased_ece(&preds, &labels, ECE_BINS),
            ValidationMetric::Accuracy => accuracy(&preds, &labels),
            ValidationMetric::Auc => auc(&preds, &labels),
        };
        metrics_out.push((*m, v));
    }
    Ok(ValidationResult {
        rule_name: cmd.rule_name.clone(),
        prob_column: cmd.prob_column.clone(),
        n_samples: preds.len(),
        metrics: metrics_out,
    })
}

/// Re-export an `Arc<dyn ...>` wrapper for symmetry with locy_calibrate.
/// (The runtime doesn't actually need this — included so dispatch
/// callers can `?` on a unified `Result<ValidationResult, _>` shape.)
pub fn into_arc_error(e: ValidateRuntimeError) -> Arc<dyn std::error::Error + Send + Sync> {
    Arc::new(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_cypher::ast::Pattern;

    fn fact_row(pairs: &[(&str, Value)]) -> FactRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn dummy_cmd() -> CompiledValidate {
        CompiledValidate {
            rule_name: "risky".into(),
            pattern: Pattern { paths: vec![] },
            where_expr: None,
            target_expr: Expr::Variable("label".into()),
            metrics: vec![
                ValidationMetric::BrierScore,
                ValidationMetric::Accuracy,
                ValidationMetric::Auc,
            ],
            prob_column: "risk".into(),
        }
    }

    #[test]
    fn validate_joins_facts_with_target_rows() {
        let cmd = dummy_cmd();
        let rule_facts = vec![
            fact_row(&[("s", Value::Int(1)), ("risk", Value::Float(0.9))]),
            fact_row(&[("s", Value::Int(2)), ("risk", Value::Float(0.1))]),
            fact_row(&[("s", Value::Int(3)), ("risk", Value::Float(0.8))]),
            fact_row(&[("s", Value::Int(4)), ("risk", Value::Float(0.2))]),
        ];
        let target_rows = vec![
            fact_row(&[
                ("s", Value::Int(1)),
                ("__validate_target", Value::Bool(true)),
            ]),
            fact_row(&[
                ("s", Value::Int(2)),
                ("__validate_target", Value::Bool(false)),
            ]),
            fact_row(&[
                ("s", Value::Int(3)),
                ("__validate_target", Value::Bool(true)),
            ]),
            fact_row(&[
                ("s", Value::Int(4)),
                ("__validate_target", Value::Bool(false)),
            ]),
        ];
        let res = run_validate(&cmd, &["s".to_string()], &rule_facts, target_rows).unwrap();
        assert_eq!(res.n_samples, 4);
        // Perfect alignment: high probs on True, low on False.
        // Brier should be very small.
        let brier = res.metric(ValidationMetric::BrierScore).unwrap();
        assert!(brier < 0.05, "expected small Brier, got {brier}");
        // Accuracy = 1 (all predictions correct at threshold 0.5).
        let acc = res.metric(ValidationMetric::Accuracy).unwrap();
        assert_eq!(acc, 1.0);
        // AUC = 1 for perfect separation.
        let a = res.metric(ValidationMetric::Auc).unwrap();
        assert!((a - 1.0).abs() < 1e-12);
    }

    #[test]
    fn validate_drops_unjoinable_rows() {
        let cmd = dummy_cmd();
        let rule_facts = vec![fact_row(&[
            ("s", Value::Int(1)),
            ("risk", Value::Float(0.9)),
        ])];
        let target_rows = vec![fact_row(&[
            ("s", Value::Int(99)),
            ("__validate_target", Value::Bool(true)),
        ])];
        let err = run_validate(&cmd, &["s".to_string()], &rule_facts, target_rows).unwrap_err();
        assert!(matches!(err, ValidateRuntimeError::EmptyDataset { .. }));
    }

    #[test]
    fn validate_errors_on_no_rule_facts() {
        let cmd = dummy_cmd();
        let err = run_validate(&cmd, &["s".to_string()], &[], vec![]).unwrap_err();
        assert!(matches!(err, ValidateRuntimeError::RuleNotDerived { .. }));
    }
}
