// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! SLG (Selective Linear Definite clause) resolution for goal-directed evaluation.
//!
//! Ported from `uni-locy/src/orchestrator/slg.rs`. Uses `DerivedFactSource` instead
//! of `CypherExecutor` for query execution.
//!
//! Since strata are pre-computed bottom-up in the native path, `resolve_goal` hits
//! the "check derived_store" early return path for pre-populated rules. The full
//! tabling logic is preserved for correctness in case of partial population.

use std::collections::HashMap;
use std::time::Instant;

use uni_common::Value;
use uni_cypher::ast::{BinaryOp, Expr};
use uni_cypher::locy_ast::{
    GeneratorRef, LocyBinaryOp, LocyExpr, RuleCondition, RuleOutput, resolve_yield_column_names,
};
use uni_locy::types::{CompiledClause, CompiledRule};
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_ast_builder::value_to_expr;
use super::locy_delta::{
    RowRelation, RowStore, extract_cypher_conditions, extract_key, multiply_prob_factors_rows,
    resolve_clause_with_is_refs,
};
use super::locy_eval::{eval_expr, literal_to_value, record_batches_to_locy_rows};
use super::locy_traits::DerivedFactSource;

/// Status of a tabling cache entry.
#[derive(Debug, Clone, PartialEq)]
enum GoalStatus {
    InProgress,
    Complete,
}

/// A cache entry for a resolved goal.
#[derive(Debug, Clone)]
struct TableEntry {
    answers: Vec<FactRow>,
    status: GoalStatus,
}

/// Cache key: (rule_name, known key bindings sorted).
type CacheKey = (String, Vec<(String, Value)>);

/// SLG resolution engine for goal-directed evaluation.
///
/// Instead of computing the full fixpoint bottom-up, SLG starts from the query goal
/// and only computes facts relevant to that goal. Tabling prevents infinite loops.
pub struct SLGResolver<'a> {
    program: &'a CompiledProgram,
    fact_source: &'a dyn DerivedFactSource,
    cache: HashMap<CacheKey, TableEntry>,
    config: &'a LocyConfig,
    pub stats: LocyStats,
    derived_store: &'a mut RowStore,
    depth: usize,
    start: Instant,
}

impl<'a> SLGResolver<'a> {
    pub fn new(
        program: &'a CompiledProgram,
        fact_source: &'a dyn DerivedFactSource,
        config: &'a LocyConfig,
        derived_store: &'a mut RowStore,
        start: Instant,
    ) -> Self {
        Self {
            program,
            fact_source,
            cache: HashMap::new(),
            config,
            stats: LocyStats::default(),
            derived_store,
            depth: 0,
            start,
        }
    }

    /// Resolve a goal: find all facts for `rule_name` matching `goal_bindings`.
    ///
    /// Uses Box::pin for recursive async (subgoals call resolve_goal).
    pub fn resolve_goal<'s>(
        &'s mut self,
        rule_name: &'s str,
        goal_bindings: &'s HashMap<String, Value>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<FactRow>, LocyError>> + Send + 's>,
    > {
        Box::pin(async move {
            let elapsed = self.start.elapsed();
            if elapsed > self.config.effective_timeout() {
                return Err(LocyError::Timeout {
                    elapsed,
                    limit: self.config.effective_timeout(),
                });
            }
            if self.depth > self.config.max_slg_depth {
                return Err(LocyError::QueryResolutionError {
                    message: format!(
                        "SLG resolution depth exceeded {} for rule '{}'",
                        self.config.max_slg_depth, rule_name
                    ),
                });
            }

            let rule = self
                .program
                .rule_catalog
                .get(rule_name)
                .ok_or_else(|| LocyError::QueryResolutionError {
                    message: format!("rule '{}' not found", rule_name),
                })?
                .clone();

            let cache_key = make_cache_key(rule_name, goal_bindings);

            // Cache check
            if let Some(entry) = self.cache.get(&cache_key) {
                match entry.status {
                    GoalStatus::Complete => return Ok(entry.answers.clone()),
                    GoalStatus::InProgress => return Ok(entry.answers.clone()),
                }
            }

            // If derived_store already has facts (from fixpoint), use them directly.
            // This avoids re-executing queries for rules that were already computed.
            if let Some(relation) = self.derived_store.get(rule_name) {
                let all_facts = relation.rows.clone();
                if !all_facts.is_empty() {
                    let filtered: Vec<FactRow> = all_facts
                        .into_iter()
                        .filter(|row| matches_goal(row, goal_bindings))
                        .collect();
                    self.cache.insert(
                        cache_key,
                        TableEntry {
                            answers: filtered.clone(),
                            status: GoalStatus::Complete,
                        },
                    );
                    return Ok(filtered);
                }
            }

            // Mark InProgress
            self.cache.insert(
                cache_key.clone(),
                TableEntry {
                    answers: Vec::new(),
                    status: GoalStatus::InProgress,
                },
            );

            self.depth += 1;

            // Initial resolution
            let answers = self.resolve_rule_clauses(&rule, goal_bindings).await?;

            // Iterative completion for recursive rules
            let final_answers = self
                .iterative_complete(&rule, goal_bindings, answers)
                .await?;

            self.depth -= 1;

            // Mark Complete
            self.cache.insert(
                cache_key,
                TableEntry {
                    answers: final_answers.clone(),
                    status: GoalStatus::Complete,
                },
            );

            // Populate derived_store as side-effect
            store_derived_facts(self.derived_store, rule_name, &rule, &final_answers);

            Ok(final_answers)
        })
    }

    /// Resolve all clauses of a rule against goal bindings.
    async fn resolve_rule_clauses(
        &mut self,
        rule: &CompiledRule,
        goal_bindings: &HashMap<String, Value>,
    ) -> Result<Vec<FactRow>, LocyError> {
        let mut all_answers = Vec::new();

        for clause in &rule.clauses {
            let has_is_refs = clause
                .where_conditions
                .iter()
                .any(|c| matches!(c, RuleCondition::IsReference(_)));
            let has_along = !clause.along.is_empty();

            if has_is_refs || has_along {
                // Resolve IS ref subgoals first (populates derived_store).
                for cond in &clause.where_conditions {
                    if let RuleCondition::IsReference(is_ref) = cond {
                        let ref_rule_name = is_ref.rule_name.to_string();
                        self.resolve_goal(&ref_rule_name, &HashMap::new()).await?;
                    }
                }

                // Detect calling rule's PROB column for complement semantics.
                let prob_column_name: Option<String> = rule
                    .yield_schema
                    .iter()
                    .find(|c| c.is_prob)
                    .map(|c| c.name.clone());

                // In-memory join (no UNWIND serialization).
                let rows = resolve_clause_with_is_refs(
                    clause,
                    self.fact_source,
                    self.derived_store,
                    &self.program.rule_catalog,
                    prob_column_name.as_deref(),
                )
                .await?;
                self.stats.queries_executed += 1;

                // Explode any generator predicates (1:N) before projection, so
                // their bound output variables are present as columns.
                let rows = apply_generators(rows, clause)?;

                // Apply YIELD projections to compute non-key columns.
                let mut projected = apply_yield_projections(rows, clause)?;

                // Multiply __prob_complement_* columns into the PROB column.
                // This must happen AFTER yield projections because YIELD
                // re-evaluates the expression (e.g., "1.0 AS safety PROB").
                if let Some(ref pc) = prob_column_name {
                    let complement_cols: Vec<String> = projected
                        .first()
                        .map(|r| {
                            r.keys()
                                .filter(|k| k.starts_with("__prob_complement_"))
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_default();
                    if !complement_cols.is_empty() {
                        multiply_prob_factors_rows(&mut projected, pc, &complement_cols);
                    }
                }

                // Filter by goal bindings.
                let filtered: Vec<FactRow> = projected
                    .into_iter()
                    .filter(|row| matches_goal(row, goal_bindings))
                    .collect();
                all_answers.extend(filtered);
            } else {
                // Simple clause: inject goal constraints into WHERE
                let cypher_conditions = extract_cypher_conditions(&clause.where_conditions);
                let mut all_conditions = cypher_conditions;
                inject_goal_where(&mut all_conditions, goal_bindings);

                let raw_batches = self
                    .fact_source
                    .execute_pattern(&clause.match_pattern, &all_conditions)
                    .await?;
                self.stats.queries_executed += 1;
                let raw_rows = record_batches_to_locy_rows(&raw_batches);

                // Explode any generator predicates (1:N) before projection.
                let raw_rows = apply_generators(raw_rows, clause)?;

                // Apply YIELD projections to compute non-key columns.
                let projected = apply_yield_projections(raw_rows, clause)?;
                all_answers.extend(projected);
            }
        }

        Ok(all_answers)
    }

    /// Iterative completion: re-resolve if new answers are discovered.
    async fn iterative_complete(
        &mut self,
        rule: &CompiledRule,
        goal_bindings: &HashMap<String, Value>,
        initial_answers: Vec<FactRow>,
    ) -> Result<Vec<FactRow>, LocyError> {
        let key_columns: Vec<String> = rule
            .yield_schema
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.clone())
            .collect();

        let mut answers = initial_answers;
        let mut iteration = 0;

        loop {
            iteration += 1;
            if iteration > self.config.max_iterations {
                break;
            }

            let prev_count = answers.len();

            // Store current answers so recursive subgoals can see them
            store_derived_facts(self.derived_store, &rule.name, rule, &answers);

            // Update cache
            let cache_key = make_cache_key(&rule.name, goal_bindings);
            if let Some(entry) = self.cache.get_mut(&cache_key) {
                entry.answers = answers.clone();
            }

            let new_answers = self.resolve_rule_clauses(rule, goal_bindings).await?;

            // Merge new answers (dedup by key)
            for new_row in new_answers {
                let new_key = extract_key(&new_row, &key_columns);
                let already_exists = answers
                    .iter()
                    .any(|existing| extract_key(existing, &key_columns) == new_key);
                if !already_exists {
                    answers.push(new_row);
                }
            }

            if answers.len() == prev_count {
                break;
            }
        }

        Ok(answers)
    }
}

/// Convert a LocyExpr to a standard Cypher Expr for in-memory evaluation.
/// Returns None for PrevRef (only meaningful in recursive fixpoint).
fn locy_expr_to_cypher(locy: &LocyExpr) -> Option<Expr> {
    match locy {
        LocyExpr::Cypher(e) => Some(e.clone()),
        LocyExpr::PrevRef(_) => None,
        LocyExpr::BinaryOp { left, op, right } => {
            let l = locy_expr_to_cypher(left)?;
            let r = locy_expr_to_cypher(right)?;
            let cypher_op = match op {
                LocyBinaryOp::Add => BinaryOp::Add,
                LocyBinaryOp::Sub => BinaryOp::Sub,
                LocyBinaryOp::Mul => BinaryOp::Mul,
                LocyBinaryOp::Div => BinaryOp::Div,
                LocyBinaryOp::Mod => BinaryOp::Mod,
                LocyBinaryOp::Pow => BinaryOp::Pow,
                LocyBinaryOp::And => BinaryOp::And,
                LocyBinaryOp::Or => BinaryOp::Or,
                LocyBinaryOp::Xor => BinaryOp::Xor,
            };
            Some(Expr::BinaryOp {
                left: Box::new(l),
                op: cypher_op,
                right: Box::new(r),
            })
        }
        LocyExpr::UnaryOp(op, inner) => {
            let e = locy_expr_to_cypher(inner)?;
            Some(Expr::UnaryOp {
                op: *op,
                expr: Box::new(e),
            })
        }
    }
}

/// Apply YIELD projections to raw rows from pattern execution.
///
/// The SLG resolver executes raw `MATCH ... RETURN *` queries, which return full
/// graph entities (nodes, edges) but do NOT include non-key YIELD columns like
/// property accesses (`n.val AS v`), computed expressions (`1.0 - n.val AS sev`),
/// or literal constants (`0.5 AS lit`).  This function evaluates each clause's
/// YIELD items against the raw rows to produce the projected columns.
/// Explode generator predicates (`name(args) -> (out…)`) in a clause: for each
/// input row, evaluate every generator's input args, dispatch it to the
/// registered [`LocyGenerator`], and emit one widened row per generated tuple
/// (the parent row plus the generator's bound output columns). Multiple
/// generators compose left-to-right (each explodes the previous result).
///
/// A clause with no generator conditions returns its rows unchanged.
///
/// # Errors
/// Returns an error if a generator is not registered, or returns a tuple whose
/// arity differs from its declared output-variable count.
fn apply_generators(
    rows: Vec<FactRow>,
    clause: &CompiledClause,
) -> Result<Vec<FactRow>, LocyError> {
    let generators: Vec<&GeneratorRef> = clause
        .where_conditions
        .iter()
        .filter_map(|c| match c {
            RuleCondition::Generator(g) => Some(g),
            _ => None,
        })
        .collect();
    if generators.is_empty() {
        return Ok(rows);
    }

    let mut current = rows;
    for generator in generators {
        let mut next = Vec::new();
        for row in &current {
            let args = generator
                .args
                .iter()
                .map(|a| eval_expr(a, row))
                .collect::<Result<Vec<Value>, LocyError>>()?;
            let tuples = super::locy_eval::dispatch_locy_generator(&generator.name, &args)
                .ok_or_else(|| LocyError::EvaluationError {
                    message: format!("generator '{}' is not registered", generator.name),
                })??;
            for tuple in tuples {
                if tuple.len() != generator.outputs.len() {
                    return Err(LocyError::EvaluationError {
                        message: format!(
                            "generator '{}' returned {} values but binds {} output variable(s)",
                            generator.name,
                            tuple.len(),
                            generator.outputs.len()
                        ),
                    });
                }
                let mut widened = row.clone();
                for (var, val) in generator.outputs.iter().zip(tuple) {
                    widened.insert(var.clone(), val);
                }
                next.push(widened);
            }
        }
        current = next;
    }
    Ok(current)
}

fn apply_yield_projections(
    raw_rows: Vec<FactRow>,
    clause: &CompiledClause,
) -> Result<Vec<FactRow>, LocyError> {
    let yield_items = match &clause.output {
        RuleOutput::Yield(yc) => &yc.items,
        _ => return Ok(raw_rows),
    };

    // Projection is required when any YIELD item needs expression evaluation:
    // a non-key column, OR a KEY column whose expression is not a bare variable
    // already present in the raw row. A property-access KEY like
    // `YIELD KEY i.tag AS tag` is exposed by the raw `MATCH ... RETURN *` rows
    // only as `i.tag` / the node `i`, never as a top-level `tag`, so without
    // projecting it the column resolves to Null in `RETURN tag` (issue #112).
    let needs_projection = yield_items
        .iter()
        .any(|item| !item.is_key || !matches!(&item.expr, Expr::Variable(_)));
    if !needs_projection {
        return Ok(raw_rows);
    }

    // De-collided output names, shared with the type checker and planner so the
    // SLG resolver's column names match the rule's yield schema.
    let names = resolve_yield_column_names(yield_items);

    raw_rows
        .into_iter()
        .map(|raw_row| -> Result<FactRow, LocyError> {
            let mut projected = FactRow::new();
            for (item, name) in yield_items.iter().zip(names.iter()) {
                let name = name.clone();

                if item.is_key {
                    // KEY columns: copy a matching raw column (the output name,
                    // or a bare KEY variable that is the graph entity itself);
                    // otherwise evaluate the KEY expression against the raw row
                    // — e.g. a property-access KEY like `i.tag AS tag`, which is
                    // present only as `i.tag` in the raw rows (issue #112).
                    if let Some(val) = raw_row.get(&name) {
                        projected.insert(name, val.clone());
                    } else if let Expr::Variable(var_name) = &item.expr
                        && let Some(val) = raw_row.get(var_name)
                    {
                        // KEY variable might be the graph entity itself
                        projected.insert(name, val.clone());
                    } else {
                        // A property-access or expression KEY. Propagate an
                        // evaluation error rather than silently masking it as
                        // Null (which previously hid unsupported expressions).
                        projected.insert(name, eval_expr(&item.expr, &raw_row)?);
                    }
                } else {
                    // Non-key columns: evaluate the YIELD expression against the
                    // raw row. Propagate errors (see KEY branch above).
                    projected.insert(name, eval_expr(&item.expr, &raw_row)?);
                }
            }

            // Carry through __prob_complement_* columns for post-projection
            // multiplication (IS NOT PROB complement semantics).
            for (k, v) in &raw_row {
                if k.starts_with("__prob_complement_") {
                    projected.insert(k.clone(), v.clone());
                }
            }

            // Also carry through ALONG bindings from the raw row.
            // ALONG expressions use LocyExpr; extract the inner Cypher Expr
            // (PrevRef only applies in recursive fixpoint, not SLG resolution).
            for along in &clause.along {
                if !projected.contains_key(&along.name)
                    && let Some(cypher_expr) = locy_expr_to_cypher(&along.expr)
                {
                    projected.insert(along.name.clone(), eval_expr(&cypher_expr, &raw_row)?);
                }
            }

            // Carry through generator-bound output variables (already present in
            // the exploded rows from `apply_generators`), like ALONG bindings.
            for cond in &clause.where_conditions {
                if let RuleCondition::Generator(g) = cond {
                    for out in &g.outputs {
                        if !projected.contains_key(out)
                            && let Some(v) = raw_row.get(out)
                        {
                            projected.insert(out.clone(), v.clone());
                        }
                    }
                }
            }

            Ok(projected)
        })
        .collect()
}

/// Store resolved facts into derived_store (free function to avoid borrow conflicts).
fn store_derived_facts(
    derived_store: &mut RowStore,
    rule_name: &str,
    rule: &CompiledRule,
    facts: &[FactRow],
) {
    let columns: Vec<String> = rule.yield_schema.iter().map(|c| c.name.clone()).collect();

    let mut all_columns = columns;
    for clause in &rule.clauses {
        for along in &clause.along {
            if !all_columns.contains(&along.name) {
                all_columns.push(along.name.clone());
            }
        }
        // Generator-bound output variables are output columns of the rule too.
        for cond in &clause.where_conditions {
            if let RuleCondition::Generator(g) = cond {
                for out in &g.outputs {
                    if !all_columns.contains(out) {
                        all_columns.push(out.clone());
                    }
                }
            }
        }
    }

    let relation = RowRelation::new(all_columns, facts.to_vec());
    derived_store.insert(rule_name.to_string(), relation);
}

/// Build a cache key from rule name and goal bindings.
fn make_cache_key(rule_name: &str, goal_bindings: &HashMap<String, Value>) -> CacheKey {
    let mut bindings: Vec<(String, Value)> = goal_bindings
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    bindings.sort_by(|a, b| a.0.cmp(&b.0));
    (rule_name.to_string(), bindings)
}

/// Check if a row matches goal bindings.
fn matches_goal(row: &FactRow, goal_bindings: &HashMap<String, Value>) -> bool {
    goal_bindings
        .iter()
        .all(|(k, v)| row.get(k).map(|rv| rv == v).unwrap_or(false))
}

/// Inject goal bindings as equality WHERE conditions.
fn inject_goal_where(conditions: &mut Vec<Expr>, goal_bindings: &HashMap<String, Value>) {
    for (var, val) in goal_bindings {
        conditions.push(Expr::BinaryOp {
            left: Box::new(Expr::Variable(var.clone())),
            op: BinaryOp::Eq,
            right: Box::new(value_to_expr(val)),
        });
    }
}

/// Extract goal bindings from a WHERE expression.
///
/// Pattern-matches on `var = literal` and `literal = var` to extract
/// key constraints for the SLG resolver.
pub fn extract_goal_bindings(where_expr: &Expr, key_columns: &[String]) -> HashMap<String, Value> {
    let mut bindings = HashMap::new();
    collect_equality_bindings(where_expr, key_columns, &mut bindings);
    bindings
}

fn collect_equality_bindings(
    expr: &Expr,
    key_columns: &[String],
    bindings: &mut HashMap<String, Value>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            if let (Expr::Variable(var), Expr::Literal(lit)) = (left.as_ref(), right.as_ref())
                && key_columns.contains(var)
            {
                bindings.insert(var.clone(), literal_to_value(lit));
            }
            if let (Expr::Literal(lit), Expr::Variable(var)) = (left.as_ref(), right.as_ref())
                && key_columns.contains(var)
            {
                bindings.insert(var.clone(), literal_to_value(lit));
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_equality_bindings(left, key_columns, bindings);
            collect_equality_bindings(right, key_columns, bindings);
        }
        _ => {}
    }
}
