// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Delta tracking and IS-reference parameter building for Locy native execution.
//!
//! Relocated from `uni-locy/src/orchestrator/delta.rs` and parts of
//! `uni-locy/src/orchestrator/fixpoint.rs` during Phase 7.

use std::collections::HashMap;

use arrow::array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow::compute::filter_record_batch;
use uni_common::Value;
use uni_common::value::EntityRef;
use uni_cypher::ast::Expr;
use uni_cypher::locy_ast::RuleCondition;
use uni_locy::binding_plan::{BindingStep, ClauseBindingPlan, Exposure, PositionAction, VarSource};
use uni_locy::types::{CompiledClause, CompiledRule};
use uni_locy::{FactRow, LocyError};

use super::locy_eval::{
    eval_condition, eval_locy_expr, record_batches_to_locy_rows, values_equal_for_join,
};
use super::locy_traits::DerivedFactSource;

/// A tuple of key column values used for indexing derived facts.
pub type KeyTuple = Vec<Value>;

/// A minimal row-based relation for command dispatch (QUERY, EXPLAIN, ABDUCE).
#[derive(Debug, Clone)]
pub struct RowRelation {
    pub columns: Vec<String>,
    pub rows: Vec<FactRow>,
}

impl RowRelation {
    pub fn new(columns: Vec<String>, rows: Vec<FactRow>) -> Self {
        Self { columns, rows }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

/// A map from rule name to its RowRelation (the row-based store commands read).
pub type RowStore = HashMap<String, RowRelation>;

/// Extracts the key tuple from a row given key column names.
pub fn extract_key(row: &FactRow, key_cols: &[String]) -> KeyTuple {
    key_cols
        .iter()
        .map(|k| row.get(k).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Extract Cypher expression conditions from rule conditions (skip IS references).
pub fn extract_cypher_conditions(conditions: &[RuleCondition]) -> Vec<Expr> {
    conditions
        .iter()
        .filter_map(|c| match c {
            RuleCondition::Expression(expr) => Some(expr.clone()),
            _ => None,
        })
        .collect()
}

// ── In-memory IS-ref join helpers ─────────────────────────────────────────────

/// Whether a derived fact agrees with a base row on every `(variable,
/// column)` pair: the row's value of the variable equals the fact's value of
/// the column. A variable absent from the row compares as `Null`.
fn key_matches(base_row: &FactRow, derived_fact: &FactRow, pairs: &[(String, String)]) -> bool {
    // Compared in place: the values are often whole nodes, and cloning two
    // per comparison dominated large joins.
    let null = Value::Null;
    pairs.iter().all(|(variable, column)| {
        let base_val = base_row.get(variable).unwrap_or(&null);
        let fact_val = derived_fact.get(column).unwrap_or(&null);
        values_equal_for_join(base_val, fact_val)
    })
}

/// Derived facts, indexed for the joins below when one compared column holds
/// an entity in every fact.
///
/// Join equality on entities is identity ([`values_equal_for_join`]), and a
/// non-entity never equals an entity, so a base row can only match the facts
/// whose entity in that column is the row's own. Indexing by that identity
/// turns a nested loop over rows × facts into a lookup; the candidates are
/// still checked with [`key_matches`] on every pair, in fact order, so the
/// result is the nested loop's. Without such a column it falls back to
/// scanning every fact.
enum FactIndex {
    Scan(usize),
    ByEntity {
        variable: String,
        facts: HashMap<EntityRef, Vec<usize>>,
    },
}

impl FactIndex {
    fn build(derived_facts: &[FactRow], pairs: &[(String, String)]) -> Self {
        for (variable, column) in pairs {
            let mut facts: HashMap<EntityRef, Vec<usize>> = HashMap::new();
            let all_entities = derived_facts.iter().enumerate().all(|(i, fact)| {
                match fact.get(column).and_then(Value::entity_ref) {
                    Some(entity) => {
                        facts.entry(entity).or_default().push(i);
                        true
                    }
                    None => false,
                }
            });
            if all_entities {
                return Self::ByEntity {
                    variable: variable.clone(),
                    facts,
                };
            }
        }
        Self::Scan(derived_facts.len())
    }

    /// The indices of the facts that may match `base_row`, in fact order.
    fn candidates<'a>(&'a self, base_row: &FactRow) -> Candidates<'a> {
        match self {
            Self::Scan(len) => Candidates::All(0..*len),
            Self::ByEntity { variable, facts } => Candidates::Some(
                base_row
                    .get(variable)
                    .and_then(Value::entity_ref)
                    .and_then(|entity| facts.get(&entity))
                    .map_or(&[][..], Vec::as_slice)
                    .iter(),
            ),
        }
    }
}

enum Candidates<'a> {
    All(std::ops::Range<usize>),
    Some(std::slice::Iter<'a, usize>),
}

impl Iterator for Candidates<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        match self {
            Self::All(range) => range.next(),
            Self::Some(iter) => iter.next().copied(),
        }
    }
}

/// Positive IS semi-join for reference `r` of `plan`: for each base row,
/// one output row per derived fact that agrees with it on every position the
/// plan tests, extended with the variables the plan binds there and the
/// value columns the reference exposes by name. Columns of the reference
/// ALONG's `prev` reads are stashed under `__prev_{col}`.
fn semi_join_is_ref(
    base_rows: &[FactRow],
    derived_facts: &[FactRow],
    plan: &ClauseBindingPlan,
    r: usize,
) -> Vec<FactRow> {
    let binding = &plan.refs[r];
    let tests: Vec<(String, String)> = binding
        .positions
        .iter()
        .filter(|p| {
            matches!(
                p.action,
                PositionAction::TestNode | PositionAction::TestScalar
            )
        })
        .map(|p| (p.variable.clone(), p.column.clone()))
        .collect();
    let binds: Vec<(&str, &str)> = binding
        .positions
        .iter()
        .filter(|p| {
            matches!(
                p.action,
                PositionAction::BindNode | PositionAction::BindScalar
            )
        })
        .map(|p| (p.variable.as_str(), p.column.as_str()))
        .collect();
    // Value columns readable by bare name through this reference. A name
    // several references expose is readable through none of them: reading it
    // is the compile error `AmbiguousRuleColumn`, so nothing can ask for it.
    let exposed: Vec<&str> = plan
        .exposure
        .iter()
        .filter(|(_, exposure)| match exposure {
            Exposure::Unique { r: owner, .. } => *owner == r,
            Exposure::Ambiguous(_) => false,
        })
        .map(|(name, _)| name.as_str())
        .collect();
    let stash_prev = plan.prev_ref == Some(r);

    let index = FactIndex::build(derived_facts, &tests);
    let mut result = Vec::new();
    for base_row in base_rows {
        for i in index.candidates(base_row) {
            let derived_fact = &derived_facts[i];
            if !key_matches(base_row, derived_fact, &tests) {
                continue;
            }
            let mut row = base_row.clone();
            for (variable, column) in &binds {
                let val = derived_fact.get(*column).cloned().unwrap_or(Value::Null);
                row.insert((*variable).to_string(), val);
            }
            for name in &exposed {
                if let Some(val) = derived_fact.get(*name) {
                    row.insert((*name).to_string(), val.clone());
                }
            }
            if stash_prev {
                for (col, val) in derived_fact {
                    row.insert(format!("__prev_{col}"), val.clone());
                }
            }
            result.push(row);
        }
    }
    result
}

/// The `(variable, column)` pairs a negated reference compares: its KEY
/// positions some literal binds, a YIELD alias read through the node it
/// names. Existential value positions are not compared.
fn negation_pairs(plan: &ClauseBindingPlan, r: usize) -> Vec<(String, String)> {
    plan.refs[r]
        .positions
        .iter()
        .filter(|p| p.is_key && p.action != PositionAction::Existential)
        .map(|p| {
            let variable = match plan.scope.get(&p.variable) {
                Some(VarSource::YieldAlias(source)) => source.clone(),
                _ => p.variable.clone(),
            };
            (variable, p.column.clone())
        })
        .collect()
}

/// Negative IS anti-join: retain base rows where no derived fact matches.
fn anti_join_is_ref(
    base_rows: &[FactRow],
    derived_facts: &[FactRow],
    pairs: &[(String, String)],
) -> Vec<FactRow> {
    let index = FactIndex::build(derived_facts, pairs);
    base_rows
        .iter()
        .filter(|base_row| {
            !index
                .candidates(base_row)
                .any(|i| key_matches(base_row, &derived_facts[i], pairs))
        })
        .cloned()
        .collect()
}

/// Probabilistic complement for IS NOT with PROB target.
///
/// Instead of filtering out matched rows (anti-join), retains ALL rows and
/// attaches a complement column: `1-p` for matched keys, `1.0` for absent keys.
/// When multiple derived facts match the same key, probabilities are combined
/// via noisy-OR: `p_combined = 1 - ∏(1 - pᵢ)`.
///
/// Mirrors [`super::locy_fixpoint::apply_prob_complement_composite`] but
/// operates on `Vec<FactRow>` instead of `Vec<RecordBatch>`.
fn prob_complement_is_ref(
    base_rows: &[FactRow],
    derived_facts: &[FactRow],
    pairs: &[(String, String)],
    target_prob_col: &str,
    complement_col_name: &str,
) -> Vec<FactRow> {
    let index = FactIndex::build(derived_facts, pairs);
    base_rows
        .iter()
        .map(|base_row| {
            // Find all matching derived facts and combine via noisy-OR.
            let mut combined_p = 0.0_f64;
            for i in index.candidates(base_row) {
                let df = &derived_facts[i];
                if key_matches(base_row, df, pairs) {
                    let p = df
                        .get(target_prob_col)
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0)
                        .clamp(0.0, 1.0);
                    combined_p = 1.0 - (1.0 - combined_p) * (1.0 - p);
                }
            }
            let complement = 1.0 - combined_p;
            let mut row = base_row.clone();
            row.insert(complement_col_name.to_string(), Value::Float(complement));
            row
        })
        .collect()
}

/// Multiply `__prob_complement_*` columns into the PROB column and remove them.
///
/// Mirrors [`super::locy_fixpoint::multiply_prob_factors`] but operates on
/// `Vec<FactRow>` instead of `Vec<RecordBatch>`.
pub fn multiply_prob_factors_rows(
    rows: &mut [FactRow],
    prob_col: &str,
    complement_cols: &[String],
) {
    for row in rows.iter_mut() {
        let mut factor = 1.0_f64;
        for col in complement_cols {
            if let Some(v) = row.get(col).and_then(|v| v.as_f64()) {
                factor *= v;
            }
            row.remove(col);
        }
        let current = row.get(prob_col).and_then(|v| v.as_f64()).unwrap_or(1.0);
        row.insert(prob_col.to_string(), Value::Float(current * factor));
    }
}

/// What a positive IS reference requires of the MATCH nodes it tests: the
/// vertex tuples its facts hold at those nodes.
///
/// The semi-join keeps only base rows that some fact matches, and a fact
/// matches a row only where each tested entity is the row's own (identity
/// equality), so a row whose tested nodes are not one fact's could never
/// survive: filtering on these tuples before the join is exact. It lets a
/// clause such as `MATCH (m), (e) WHERE (m, e) IS r` convert only the pairs
/// `r` holds, not the cartesian product, whose conversion to rows was most of
/// the time a DERIVE took. A reference whose facts hold anything other than
/// a vertex at a tested node constrains nothing.
struct SemiJoinPrefilter {
    variables: Vec<String>,
    tuples: std::collections::HashSet<Vec<u64>>,
}

impl SemiJoinPrefilter {
    fn for_plan<'a>(
        plan: &ClauseBindingPlan,
        facts_of: impl Fn(&str) -> Option<&'a [FactRow]>,
    ) -> Vec<Self> {
        let mut prefilters = Vec::new();
        for step in &plan.steps {
            let BindingStep::Ref(r) = *step else { continue };
            let binding = &plan.refs[r];
            let tested: Vec<_> = binding
                .positions
                .iter()
                .filter(|p| {
                    p.action == PositionAction::TestNode
                        && matches!(plan.scope.get(&p.variable), Some(VarSource::Match(_)))
                })
                .collect();
            if tested.is_empty() {
                continue;
            }
            let facts = facts_of(&binding.rule).unwrap_or(&[]);
            let tuples: Option<std::collections::HashSet<Vec<u64>>> = facts
                .iter()
                .map(|fact| {
                    tested
                        .iter()
                        .map(|p| match fact.get(&p.column).and_then(Value::entity_ref) {
                            Some(EntityRef::Vertex(vid)) => Some(vid.as_u64()),
                            _ => None,
                        })
                        .collect()
                })
                .collect();
            if let Some(tuples) = tuples {
                prefilters.push(Self {
                    variables: tested.iter().map(|p| p.variable.clone()).collect(),
                    tuples,
                });
            }
        }
        prefilters
    }

    /// `id(v) IN [...]` for each tested node, so the MATCH itself narrows.
    fn match_filters(&self) -> Vec<Expr> {
        use uni_cypher::ast::CypherLiteral;
        (0..self.variables.len())
            .map(|i| {
                let vids: std::collections::BTreeSet<u64> =
                    self.tuples.iter().map(|tuple| tuple[i]).collect();
                Expr::In {
                    expr: Box::new(Expr::FunctionCall {
                        name: "id".to_string(),
                        args: vec![Expr::Variable(self.variables[i].clone())],
                        distinct: false,
                        window_spec: None,
                    }),
                    list: Box::new(Expr::List(
                        vids.into_iter()
                            .map(|vid| Expr::Literal(CypherLiteral::Integer(vid as i64)))
                            .collect(),
                    )),
                }
            })
            .collect()
    }

    /// `batch` without the rows whose tested nodes form no fact's tuple.
    /// A batch lacking a tested node's `{var}._vid` column is kept whole.
    fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch, LocyError> {
        let columns: Option<Vec<&UInt64Array>> = self
            .variables
            .iter()
            .map(|v| {
                batch
                    .column_by_name(&format!("{v}._vid"))
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            })
            .collect();
        let Some(columns) = columns else {
            return Ok(batch.clone());
        };
        let mut tuple = Vec::with_capacity(columns.len());
        let keep: BooleanArray = (0..batch.num_rows())
            .map(|row| {
                tuple.clear();
                for column in &columns {
                    if column.is_null(row) {
                        return Some(false);
                    }
                    tuple.push(column.value(row));
                }
                Some(self.tuples.contains(&tuple))
            })
            .collect();
        filter_record_batch(batch, &keep).map_err(|e| LocyError::EvaluationError {
            message: format!("filtering a clause's MATCH rows: {e}"),
        })
    }
}

/// Resolve a clause by executing its MATCH pattern and applying IS-ref joins in memory.
///
/// Replaces the serialize-UNWIND-execute round-trip EXPLAIN Mode B used. The
/// clause's binding plan (`uni_locy::binding_plan`) decides everything the
/// evaluator decides: the WHERE conjuncts over MATCH variables run in the
/// MATCH, then the plan's steps run in order — a positive IS reference as an
/// in-memory semi-join that binds the plan's binders and tests its tests, a
/// filter over the names bound so far, a negation as an anti-join over the
/// KEY positions it compares. So EXPLAIN agrees with evaluation in every
/// conjunct order (issue #300). ALONG expressions are evaluated last, `prev`
/// reading the plan's recursive reference.
///
/// A generator step is not evaluated: rows do not carry what a generator
/// needs, and a filter over its outputs sees them absent.
pub async fn resolve_clause_with_is_refs(
    clause: &CompiledClause,
    fact_source: &dyn DerivedFactSource,
    derived_store: &RowStore,
    rule_catalog: &HashMap<String, CompiledRule>,
    calling_rule_prob_col: Option<&str>,
) -> Result<Vec<FactRow>, LocyError> {
    let plan = clause
        .binding
        .plan()
        .ok_or_else(|| LocyError::EvaluationError {
            message: "internal error: an EXPLAIN clause has no binding plan".into(),
        })?;
    let condition = |index: usize| -> Result<&Expr, LocyError> {
        match clause.where_conditions.get(index) {
            Some(RuleCondition::Expression(expr)) => Ok(expr),
            _ => Err(LocyError::EvaluationError {
                message: format!(
                    "internal error: binding plan filter {index} is not a WHERE expression"
                ),
            }),
        }
    };

    // Execute base MATCH query with the conjuncts over MATCH variables.
    let match_safe = plan
        .immediate_filters
        .iter()
        .map(|&index| condition(index).cloned())
        .collect::<Result<Vec<Expr>, LocyError>>()?;
    let facts_of = |rule_name: &str| -> Vec<FactRow> {
        derived_store
            .get(rule_name)
            .map(|relation| relation.rows.clone())
            .unwrap_or_default()
    };

    let prefilters = SemiJoinPrefilter::for_plan(plan, |rule| {
        derived_store
            .get(rule)
            .map(|relation| relation.rows.as_slice())
    });
    let mut match_safe = match_safe;
    match_safe.extend(prefilters.iter().flat_map(SemiJoinPrefilter::match_filters));
    let mut raw_batches = fact_source
        .execute_pattern(&clause.match_pattern, &match_safe)
        .await?;
    for prefilter in &prefilters {
        raw_batches = raw_batches
            .iter()
            .map(|batch| prefilter.apply(batch))
            .collect::<Result<_, _>>()?;
    }
    let mut rows = record_batches_to_locy_rows(&raw_batches);

    for step in &plan.steps {
        match *step {
            BindingStep::Ref(r) => {
                let derived_facts = facts_of(&plan.refs[r].rule);
                rows = semi_join_is_ref(&rows, &derived_facts, plan, r);
            }
            BindingStep::Filter(index) => {
                let expr = condition(index)?;
                let mut kept = Vec::with_capacity(rows.len());
                for row in rows {
                    if eval_condition(expr, &row, "rule WHERE")? {
                        kept.push(row);
                    }
                }
                rows = kept;
            }
            BindingStep::Generator(_) => {}
            BindingStep::AntiJoin(r) => {
                let rule_name = &plan.refs[r].rule;
                let derived_facts = facts_of(rule_name);
                let pairs = negation_pairs(plan, r);
                // Check if target rule has PROB column for complement semantics.
                let target_prob_col = rule_catalog.get(rule_name).and_then(|rule| {
                    rule.yield_schema
                        .iter()
                        .find(|c| c.is_prob)
                        .map(|c| c.name.clone())
                });
                rows = match (&target_prob_col, calling_rule_prob_col) {
                    // Probabilistic complement: keep all rows, compute 1-p.
                    (Some(tpc), Some(_)) => prob_complement_is_ref(
                        &rows,
                        &derived_facts,
                        &pairs,
                        tpc,
                        &format!("__prob_complement_{r}_{rule_name}"),
                    ),
                    // Boolean anti-join.
                    _ => anti_join_is_ref(&rows, &derived_facts, &pairs),
                };
            }
        }
    }

    // NOTE: __prob_complement_* columns are left in the rows for the caller
    // (locy_slg.rs) to multiply into the PROB column AFTER yield projections.
    // This is because apply_yield_projections() re-evaluates the YIELD expression
    // (e.g., "1.0 AS safety PROB") and would overwrite any pre-multiplied value.

    // Evaluate ALONG expressions (using __prev_* stash for PrevRef lookups).
    if !clause.along.is_empty() {
        let mut new_rows = Vec::with_capacity(rows.len());
        for mut row in rows {
            let prev_values: FactRow = row
                .iter()
                .filter_map(|(k, v)| {
                    k.strip_prefix("__prev_")
                        .map(|col| (col.to_string(), v.clone()))
                })
                .collect();

            for along in &clause.along {
                let result = eval_locy_expr(&along.expr, &row, Some(&prev_values))?;
                row.insert(along.name.clone(), result);
            }
            new_rows.push(row);
        }
        rows = new_rows;
    }

    // Remove __prev_* staging keys from output rows.
    for row in &mut rows {
        row.retain(|k, _| !k.starts_with("__prev_"));
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_common::Value;

    /// The anti-join key of `x IS NOT rule` on a one-KEY rule whose KEY is
    /// named like the subject.
    fn make_is_ref(subject: &str, _rule: &str) -> Vec<(String, String)> {
        vec![(subject.to_string(), subject.to_string())]
    }

    fn row_int(pairs: &[(&str, i64)]) -> FactRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Int(*v)))
            .collect()
    }

    #[test]
    fn test_prob_complement_basic() {
        // Alice in risky with p=0.7 → complement=0.3
        // Bob in risky with p=0.3 → complement=0.7
        // Charlie absent → complement=1.0
        let base = vec![
            row_int(&[("n", 1)]),
            row_int(&[("n", 2)]),
            row_int(&[("n", 3)]),
        ];
        let derived = vec![
            {
                let mut r = row_int(&[("n", 1)]);
                r.insert("risk_score".into(), Value::Float(0.7));
                r
            },
            {
                let mut r = row_int(&[("n", 2)]);
                r.insert("risk_score".into(), Value::Float(0.3));
                r
            },
        ];
        let is_ref = make_is_ref("n", "risky");

        let result = prob_complement_is_ref(&base, &derived, &is_ref, "risk_score", "__c");

        assert_eq!(result.len(), 3);
        let c0 = result[0].get("__c").unwrap().as_f64().unwrap();
        let c1 = result[1].get("__c").unwrap().as_f64().unwrap();
        let c2 = result[2].get("__c").unwrap().as_f64().unwrap();
        assert!((c0 - 0.3).abs() < 1e-10, "Alice: expected 0.3, got {c0}");
        assert!((c1 - 0.7).abs() < 1e-10, "Bob: expected 0.7, got {c1}");
        assert!((c2 - 1.0).abs() < 1e-10, "Charlie: expected 1.0, got {c2}");
    }

    #[test]
    fn test_prob_complement_noisy_or_duplicates() {
        // n=1 appears twice with p=0.3 and p=0.5
        // Noisy-OR: 1-(1-0.3)(1-0.5) = 0.65, complement = 0.35
        let base = vec![row_int(&[("n", 1)])];
        let derived = vec![
            {
                let mut r = row_int(&[("n", 1)]);
                r.insert("prob".into(), Value::Float(0.3));
                r
            },
            {
                let mut r = row_int(&[("n", 1)]);
                r.insert("prob".into(), Value::Float(0.5));
                r
            },
        ];
        let is_ref = make_is_ref("n", "rule");

        let result = prob_complement_is_ref(&base, &derived, &is_ref, "prob", "__c");
        let c = result[0].get("__c").unwrap().as_f64().unwrap();
        assert!((c - 0.35).abs() < 1e-10, "expected 0.35, got {c}");
    }

    #[test]
    fn test_multiply_prob_factors_rows_basic() {
        // safety=1.0, complement=0.3 → result safety=0.3, complement col removed
        let mut rows = vec![{
            let mut r = FactRow::new();
            r.insert("safety".into(), Value::Float(1.0));
            r.insert("__prob_complement_risky".into(), Value::Float(0.3));
            r
        }];
        multiply_prob_factors_rows(&mut rows, "safety", &["__prob_complement_risky".into()]);

        assert!(
            !rows[0].contains_key("__prob_complement_risky"),
            "complement col should be removed"
        );
        let safety = rows[0].get("safety").unwrap().as_f64().unwrap();
        assert!((safety - 0.3).abs() < 1e-10, "expected 0.3, got {safety}");
    }

    fn node(vid: u64, text: &str) -> Value {
        Value::Node(uni_common::Node {
            vid: uni_common::Vid::new(vid),
            labels: vec!["N".into()],
            properties: [("text".to_string(), Value::String(text.into()))].into(),
        })
    }

    fn row_nodes(pairs: &[(&str, Value)]) -> FactRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn indexed_joins_compare_entities_by_identity() {
        // The fact holds the same vertices hydrated differently: identity,
        // not the property map, decides the match.
        let base = vec![
            row_nodes(&[("m", node(1, "a")), ("e", node(10, "x"))]),
            row_nodes(&[("m", node(1, "a")), ("e", node(11, "y"))]),
            row_nodes(&[("m", node(2, "b")), ("e", node(10, "x"))]),
            row_nodes(&[("m", Value::Int(1)), ("e", node(10, "x"))]),
        ];
        let facts = vec![
            row_nodes(&[
                ("m", node(1, "other")),
                ("e", node(10, "")),
                ("p", Value::Float(0.5)),
            ]),
            row_nodes(&[
                ("m", node(1, "")),
                ("e", node(10, "")),
                ("p", Value::Float(0.5)),
            ]),
        ];
        let pairs = vec![
            ("m".to_string(), "m".to_string()),
            ("e".to_string(), "e".to_string()),
        ];
        assert!(matches!(
            FactIndex::build(&facts, &pairs),
            FactIndex::ByEntity { .. }
        ));

        let kept = anti_join_is_ref(&base, &facts, &pairs);
        assert_eq!(kept.len(), 3, "only the (1, 10) row is excluded");

        let complemented = prob_complement_is_ref(&base, &facts, &pairs, "p", "__c");
        let c: Vec<f64> = complemented
            .iter()
            .map(|r| r.get("__c").unwrap().as_f64().unwrap())
            .collect();
        // Both facts match the first row: 1 - (1 - (1-0.5)(1-0.5)) = 0.25.
        assert_eq!(c, vec![0.25, 1.0, 1.0, 1.0]);
    }

    #[test]
    fn a_column_holding_a_non_entity_falls_back_to_scanning() {
        let facts = vec![
            row_nodes(&[("m", node(1, ""))]),
            row_nodes(&[("m", Value::Int(1))]),
        ];
        let pairs = vec![("m".to_string(), "m".to_string())];
        assert!(matches!(
            FactIndex::build(&facts, &pairs),
            FactIndex::Scan(2)
        ));
        let base = vec![
            row_nodes(&[("m", Value::Int(1))]),
            row_nodes(&[("m", node(1, "a"))]),
        ];
        // Each base row matches the fact of its own kind; nothing is kept.
        assert!(anti_join_is_ref(&base, &facts, &pairs).is_empty());
    }

    #[test]
    fn test_anti_join_unchanged_without_prob() {
        // Boolean IS NOT: n=1 in flagged → excluded, n=2 not → kept
        let base = vec![row_int(&[("n", 1)]), row_int(&[("n", 2)])];
        let derived = vec![row_int(&[("n", 1)])];
        let is_ref = make_is_ref("n", "flagged");

        let result = anti_join_is_ref(&base, &derived, &is_ref);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("n").unwrap().as_i64(), Some(2));
    }
}
