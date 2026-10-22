// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! QUERY evaluation.
//!
//! QUERY is answered from `derived_store` — the same semi-naive fixpoint result
//! that `.derived` reads — so the two agree by construction. SLG resolution
//! survives only as a fallback for rules containing a generator, which the
//! columnar fixpoint has no row-explosion operator for.
//!
//! Ported from `uni-locy/src/orchestrator/query.rs`. Uses `DerivedFactSource`
//! instead of `CypherExecutor`.

use std::collections::HashMap;

use uni_common::Value;
use uni_cypher::ast::{Expr, ReturnItem};
use uni_cypher::locy_ast::GoalQuery;
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError};

use super::locy_delta::RowStore;

use super::locy_eval::{eval_condition, eval_expr, value_cmp};
use uni_query_functions::df_udfs::canonical_cypher_value;

/// How many rows an in-memory QUERY evaluates between two checks of whether
/// it must stop (its program's deadline, a cancellation).
pub const QUERY_CHECK_INTERVAL: usize = 1024;

/// Calls `interrupt` before the first row and then every
/// [`QUERY_CHECK_INTERVAL`] rows an in-memory QUERY evaluates, over all its
/// passes: it runs inside no operator, so nothing else checks it.
struct Checkpoint<'a> {
    interrupt: &'a dyn Fn() -> Result<(), LocyError>,
    rows: usize,
}

impl Checkpoint<'_> {
    /// Counts one row, failing with `interrupt`'s error when it is time to
    /// check and the QUERY must stop.
    fn row(&mut self) -> Result<(), LocyError> {
        if self.rows.is_multiple_of(QUERY_CHECK_INTERVAL) {
            (self.interrupt)()?;
        }
        self.rows += 1;
        Ok(())
    }
}

/// Entry point for goal-directed QUERY evaluation.
///
/// Reads the rule's facts from `derived_store`, the facts the fixpoint
/// published, then applies the WHERE filter and RETURN clause.
///
/// The evaluation is in memory, inside no operator, so nothing else bounds
/// it: `interrupt` is called every [`QUERY_CHECK_INTERVAL`] rows its passes
/// (filter, grouping, projection) evaluate, and its error ends the QUERY. The caller
/// passes the program's deadline and cancellation check.
///
/// # Errors
///
/// Returns an error if the rule is unknown or has no published relation, if
/// WHERE or RETURN cannot be evaluated, or `interrupt`'s error.
pub fn evaluate_query(
    query: &GoalQuery,
    program: &CompiledProgram,
    config: &LocyConfig,
    derived_store: &RowStore,
    interrupt: &dyn Fn() -> Result<(), LocyError>,
) -> Result<Vec<FactRow>, LocyError> {
    let mut check = Checkpoint { interrupt, rows: 0 };
    // `QUERY adult` inside `MODULE m` names the rule bare, but the catalog keys
    // it as `m.adult`. The compiler validates the reference module-aware, so a
    // plain lookup here accepts the program at compile time and then fails at
    // run time. Resolve with the same policy the derived store uses.
    let rule_name = query.rule_name.to_string();
    uni_locy::names::resolve_unique(program.rule_catalog.keys().map(String::as_str), &rule_name)
        .ok_or_else(|| LocyError::QueryResolutionError {
            message: format!("rule '{}' not found", rule_name),
        })?;

    // Answer from the facts the fixpoint already derived.
    //
    // This used to re-derive the rule from scratch through the SLG resolver,
    // discarding `derived_store`, on the grounds that "the native fixpoint
    // stores node columns as VIDs (UInt64), not full node objects, so
    // orch_store rows would fail property-based WHERE/RETURN evaluation".
    // That reason is stale twice over: `FixpointState::reconcile_schema`
    // replaces the planner's inferred types with the physical plan's real
    // output schema, and `enrich_vids_with_nodes` hydrates VID columns into
    // node objects before commands are dispatched.
    //
    // The discard was the direct cause of issue #160: `.derived` read the
    // fixpoint's answer while `QUERY` read an independent SLG re-derivation
    // with weaker recursion and stratification, and the two disagreed. Reading
    // the same store makes them the same bytes by construction, which is a
    // stronger guarantee than any parity check could give.
    //
    // FOLD rules always took this path — the SLG resolver has no post-fixpoint
    // aggregation and would return raw pre-FOLD match rows — so this is that
    // branch generalized to every rule, not a new mechanism.
    // Module-aware: inside `MODULE m`, `QUERY adult` names the rule bare while
    // the store keys it `m.adult`. A plain lookup misses and falls through to
    // the SLG path, which then reports the rule as not found even though the
    // fixpoint derived it. `RowStore` is a bare map, so resolve the key with
    // the shared policy rather than a method.
    let store_key =
        uni_locy::names::resolve_unique(derived_store.keys().map(String::as_str), &rule_name)
            .map(str::to_string);
    if let Some(relation) = store_key.and_then(|k| derived_store.get(&k)) {
        let rows = relation.rows.clone();
        let filtered = filter_where(rows, query.where_expr.as_ref(), &config.params, &mut check)?;
        return apply_return_clause(filtered, &query.return_clause, &config.params, &mut check);
    }

    // Every rule the program defines has a published relation, empty when it
    // derived nothing; a generator rule too, now that the fixpoint plans
    // generators (`LocyGenerateExec`). There used to be a fallback here that
    // re-derived a missing rule through the in-memory evaluator, which has no
    // FOLD, BEST BY or post-FOLD WHERE, so it answered such a rule with its
    // pre-FOLD rows. A missing relation is now a fault, not a cue to derive
    // the rule a second way.
    Err(LocyError::EvaluationError {
        message: format!("QUERY {rule_name}: no facts were published for this rule"),
    })
}

/// Apply a RETURN clause (projection, ordering, skip, limit) to results.
fn apply_return_clause(
    rows: Vec<FactRow>,
    return_clause: &Option<uni_cypher::ast::ReturnClause>,
    params: &HashMap<String, Value>,
    check: &mut Checkpoint<'_>,
) -> Result<Vec<FactRow>, LocyError> {
    let rc = match return_clause {
        Some(rc) => rc,
        None => return Ok(rows),
    };
    // Aggregating RETURN: fold each group to one row, then project as usual.
    let aggregated;
    let (rows, rc) = match aggregate_return(rows, rc, params, check)? {
        Aggregation::None(rows) => (rows, rc),
        Aggregation::Grouped(rows, rewritten) => {
            aggregated = *rewritten;
            (rows, &aggregated)
        }
    };

    // Project columns. Params are merged into each row so $name references
    // in RETURN expressions (e.g. RETURN $agent_id AS id) resolve correctly.
    //
    // Sort keys are computed here too, from the row *before* projection with
    // the returned aliases laid over it, so `ORDER BY` can name a returned alias
    // or any expression over the rule's columns. Evaluated after projection, a
    // key that was not a returned column (`RETURN a.name AS n ORDER BY a.age`)
    // read a column that was gone; the error became NULL and every key tied, so
    // the clause silently did nothing.
    let mut entries: Vec<(FactRow, Vec<Value>)> = rows
        .into_iter()
        .map(|row| {
            check.row()?;
            let merged = merge_params(&row, params);
            let mut new_row = FactRow::new();
            for item in &rc.items {
                match item {
                    ReturnItem::All => {
                        new_row = row.clone();
                        break;
                    }
                    ReturnItem::Expr { expr, alias, .. } => {
                        let value = eval_expr(expr, &merged)?;
                        let name = alias.clone().unwrap_or_else(|| return_item_name(expr));
                        new_row.insert(name, value);
                    }
                }
            }
            let keys = match &rc.order_by {
                Some(sort_items) => {
                    let mut scope = merged;
                    scope.extend(new_row.iter().map(|(k, v)| (k.clone(), v.clone())));
                    sort_items
                        .iter()
                        .map(|item| eval_expr(&item.expr, &scope))
                        .collect::<Result<Vec<_>, LocyError>>()?
                }
                None => Vec::new(),
            };
            Ok((new_row, keys))
        })
        .collect::<Result<Vec<_>, LocyError>>()?;

    // Distinct
    if rc.distinct {
        // Key on a sorted `BTreeMap` rather than `format!("{row:?}")`: a
        // `FactRow` is a `HashMap`, whose `Debug` order is instance-dependent,
        // so byte-identical rows could render to different strings and survive
        // DISTINCT. `Value` has a canonical `Hash`/`Eq`, so this dedups by
        // content deterministically. Each value is keyed on its equivalence
        // class, so `1` and `1.0` are one value, as in Cypher; the row kept
        // is the first one seen.
        let mut seen = std::collections::HashSet::new();
        entries.retain(|(row, _)| {
            let key: std::collections::BTreeMap<String, uni_common::Value> = row
                .iter()
                .map(|(k, v)| (k.clone(), canonical_cypher_value(v.clone())))
                .collect();
            seen.insert(key)
        });
    }

    // Order by
    if let Some(sort_items) = &rc.order_by {
        entries.sort_by(|(_, ka), (_, kb)| {
            for (item, (va, vb)) in sort_items.iter().zip(ka.iter().zip(kb)) {
                let cmp = if item.ascending {
                    value_cmp(va, vb)
                } else {
                    value_cmp(vb, va)
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    let mut projected: Vec<FactRow> = entries.into_iter().map(|(row, _)| row).collect();

    // SKIP / LIMIT take any expression over the parameters. Only an integer
    // literal used to be honoured: `LIMIT $n` was silently ignored.
    if let Some(n) = row_count_bound(rc.skip.as_ref(), "SKIP", params)? {
        projected.drain(..n.min(projected.len()));
    }
    if let Some(n) = row_count_bound(rc.limit.as_ref(), "LIMIT", params)? {
        projected.truncate(n);
    }

    Ok(projected)
}

/// Evaluates a `SKIP` / `LIMIT` bound, which must be a non-negative integer.
fn row_count_bound(
    expr: Option<&Expr>,
    clause: &str,
    params: &HashMap<String, Value>,
) -> Result<Option<usize>, LocyError> {
    let Some(expr) = expr else {
        return Ok(None);
    };
    match eval_expr(expr, &merge_params(&FactRow::new(), params))? {
        Value::Int(n) if n >= 0 => Ok(Some(n as usize)),
        other => Err(LocyError::TypeError {
            message: format!("{clause} must be a non-negative integer, got {other:?}"),
        }),
    }
}

/// The rows a `RETURN` projects: the input, or one row per group.
enum Aggregation {
    None(Vec<FactRow>),
    /// One row per group, carrying each aggregate under a placeholder column,
    /// and the clause rewritten to read the placeholders.
    Grouped(Vec<FactRow>, Box<uni_cypher::ast::ReturnClause>),
}

const AGG_PLACEHOLDER: &str = "__locy_agg_";

/// Groups a `QUERY ... RETURN` that aggregates, as Cypher's `RETURN` does: the
/// items without an aggregate are the grouping key, every aggregate (anywhere
/// in an item or an `ORDER BY` key) is evaluated over its group, and with no
/// grouping key an empty input is still one row (`count(*)` is 0). Aggregates
/// were evaluated row by row, so `count(*)` failed ("unsupported expression:
/// Wildcard") and `sum(x)` was not a sum.
fn aggregate_return(
    rows: Vec<FactRow>,
    rc: &uni_cypher::ast::ReturnClause,
    params: &HashMap<String, Value>,
    check: &mut Checkpoint<'_>,
) -> Result<Aggregation, LocyError> {
    let item_exprs = rc.items.iter().filter_map(|item| match item {
        ReturnItem::Expr { expr, .. } => Some(expr),
        ReturnItem::All => None,
    });
    if !item_exprs.clone().any(contains_aggregate) {
        if rc
            .order_by
            .iter()
            .flatten()
            .any(|sort| contains_aggregate(&sort.expr))
        {
            return Err(LocyError::TypeError {
                message: "an aggregate in ORDER BY needs an aggregate in the RETURN".to_string(),
            });
        }
        return Ok(Aggregation::None(rows));
    }
    if rc.items.iter().any(|i| matches!(i, ReturnItem::All)) {
        return Err(LocyError::TypeError {
            message: "RETURN * cannot be combined with an aggregate".to_string(),
        });
    }

    let mut aggregates: Vec<Expr> = Vec::new();
    let mut rewritten = rc.clone();
    for item in &mut rewritten.items {
        if let ReturnItem::Expr { expr, alias, .. } = item
            && contains_aggregate(expr)
        {
            alias.get_or_insert_with(|| return_item_name(expr));
            *expr = lift_aggregates(expr.clone(), &mut aggregates);
        }
    }
    if let Some(order_by) = &mut rewritten.order_by {
        for sort in order_by {
            sort.expr = lift_aggregates(sort.expr.clone(), &mut aggregates);
        }
    }
    let keys: Vec<&Expr> = rc
        .items
        .iter()
        .filter_map(|item| match item {
            ReturnItem::Expr { expr, .. } if !contains_aggregate(expr) => Some(expr),
            _ => None,
        })
        .collect();

    // Groups in first-seen order.
    let mut index: HashMap<Vec<Value>, usize> = HashMap::new();
    let mut groups: Vec<Vec<FactRow>> = Vec::new();
    for row in rows {
        check.row()?;
        let merged = merge_params(&row, params);
        let key = keys
            .iter()
            .map(|k| eval_expr(k, &merged))
            .collect::<Result<Vec<_>, _>>()?;
        let slot = *index.entry(key).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[slot].push(merged);
    }
    if groups.is_empty() && keys.is_empty() {
        groups.push(Vec::new());
    }

    let mut out = Vec::with_capacity(groups.len());
    for group in groups {
        check.row()?;
        let mut row = group.first().cloned().unwrap_or_default();
        for (i, agg) in aggregates.iter().enumerate() {
            row.insert(format!("{AGG_PLACEHOLDER}{i}"), aggregate(agg, &group)?);
        }
        out.push(row);
    }
    Ok(Aggregation::Grouped(out, Box::new(rewritten)))
}

/// An aggregate function call itself. (`Expr::is_aggregate` answers "contains
/// an aggregate" for most nodes, but only the call's own name for a function.)
fn is_aggregate_call(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall { .. }) && expr.is_aggregate()
}

fn contains_aggregate(expr: &Expr) -> bool {
    if is_aggregate_call(expr) {
        return true;
    }
    let mut found = false;
    expr.for_each_child(&mut |child| found |= contains_aggregate(child));
    found
}

/// Replaces each aggregate in `expr` with a placeholder variable, recording it.
fn lift_aggregates(expr: Expr, aggregates: &mut Vec<Expr>) -> Expr {
    if is_aggregate_call(&expr) {
        let i = aggregates
            .iter()
            .position(|a| *a == expr)
            .unwrap_or_else(|| {
                aggregates.push(expr);
                aggregates.len() - 1
            });
        return Expr::Variable(format!("{AGG_PLACEHOLDER}{i}"));
    }
    expr.map_children(&mut |child| lift_aggregates(child, aggregates))
}

/// One aggregate over a group, with Cypher's semantics: NULLs are skipped,
/// `sum` of nothing is 0 (an integer sum stays an integer), `avg`, `min` and
/// `max` of nothing are NULL, and `collect` of nothing is `[]`.
fn aggregate(expr: &Expr, group: &[FactRow]) -> Result<Value, LocyError> {
    let Expr::FunctionCall {
        name,
        args,
        distinct,
        ..
    } = expr
    else {
        unreachable!("lifted only aggregates");
    };
    let name = name.to_lowercase();
    if name == "count" && matches!(args.as_slice(), [Expr::Wildcard]) {
        return Ok(Value::Int(group.len() as i64));
    }
    let [arg] = args.as_slice() else {
        return Err(LocyError::TypeError {
            message: format!("{name}() in a QUERY RETURN takes one argument"),
        });
    };
    // Under DISTINCT, the first value of each class of equal values (`1` and
    // `1.0` are one value, as in Cypher).
    let mut values = Vec::with_capacity(group.len());
    let mut seen = std::collections::HashSet::new();
    for row in group {
        let v = eval_expr(arg, row)?;
        if !v.is_null() && (!*distinct || seen.insert(canonical_cypher_value(v.clone()))) {
            values.push(v);
        }
    }
    match name.as_str() {
        "count" => Ok(Value::Int(values.len() as i64)),
        "collect" => Ok(Value::List(values)),
        // Cypher's `sum` / `avg`, shared with the Cypher aggregates: numbers
        // or durations. A duration used to be a type error here, and an
        // integer overflow an error where Cypher continues in floats.
        "sum" | "avg" => {
            let type_error = |e: anyhow::Error| LocyError::TypeError {
                message: e.to_string(),
            };
            let mut total = uni_query_functions::cypher_sum::CypherSum::default();
            for v in &values {
                total.add(v).map_err(type_error)?;
            }
            if name == "sum" {
                Ok(total.sum())
            } else {
                Ok(total.avg(values.len() as u64))
            }
        }
        "min" | "max" => {
            let want = if name == "min" {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
            Ok(values
                .into_iter()
                .reduce(|best, v| {
                    if value_cmp(&v, &best) == want {
                        v
                    } else {
                        best
                    }
                })
                .unwrap_or(Value::Null))
        }
        other => Err(LocyError::TypeError {
            message: format!("aggregate {other}() is not supported in a QUERY RETURN"),
        }),
    }
}

/// Merge query parameters into a row so that `Expr::Parameter(name)` can
/// resolve `$name` references during in-memory expression evaluation.
///
/// Row values take precedence — parameters only fill in keys that are absent.
pub(super) fn merge_params(row: &FactRow, params: &HashMap<String, Value>) -> FactRow {
    let mut merged: FactRow = params.clone();
    merged.extend(row.iter().map(|(k, v)| (k.clone(), v.clone())));
    merged
}

/// Apply a QUERY `WHERE` predicate to result rows.
///
/// Params are injected per row so `$name` references resolve. A row is kept only
/// when the predicate evaluates to a truthy value; an evaluation error or a
/// non-boolean result drops the row. Used uniformly by both the FOLD-rule and
/// non-FOLD result paths so the filter is never bypassed.
/// Keeps the rows `where_expr` is true for.
///
/// False and NULL ("unknown") drop a row, as in Cypher. An evaluation error, or
/// a condition that is not a boolean, is an error: it used to drop the row
/// too, so a malformed or unsupported filter silently returned fewer rows.
///
/// # Errors
///
/// [`LocyError`] when the condition fails to evaluate or is not boolean.
fn filter_where(
    rows: Vec<FactRow>,
    where_expr: Option<&Expr>,
    params: &HashMap<String, Value>,
    check: &mut Checkpoint<'_>,
) -> Result<Vec<FactRow>, LocyError> {
    let Some(expr) = where_expr else {
        return Ok(rows);
    };
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        check.row()?;
        let merged = merge_params(&row, params);
        if eval_condition(expr, &merged, "QUERY WHERE")? {
            kept.push(row);
        }
    }
    Ok(kept)
}

/// Derive a column name from a RETURN expression when no alias is given.
///
/// Follows OpenCypher convention: `RETURN p` yields `"p"`,
/// `RETURN a.name` yields `"a.name"`.  Falls back to `Debug` for
/// complex expressions.
fn return_item_name(expr: &Expr) -> String {
    match expr {
        Expr::Variable(v) => v.clone(),
        Expr::Property(base, prop) => format!("{}.{}", return_item_name(base), prop),
        _ => format!("{expr:?}"),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use uni_locy::CompiledCommand;

    use super::super::locy_delta::RowRelation;
    use super::*;

    /// `QUERY r WHERE x >= 0 RETURN x` over `n` facts of `r`.
    fn query_over(n: i64) -> (GoalQuery, CompiledProgram, RowStore) {
        let ast = uni_cypher::parse_locy(
            "CREATE RULE r AS MATCH (n:N) YIELD KEY n, n.x AS x\nQUERY r WHERE x >= 0 RETURN x",
        )
        .expect("parses");
        let program = uni_locy::compile(&ast).expect("compiles");
        let query = match program.commands.first() {
            Some(CompiledCommand::GoalQuery(q)) => q.clone(),
            other => panic!("expected the QUERY, got {other:?}"),
        };
        let rows = (0..n)
            .map(|i| {
                FactRow::from([
                    ("n".to_string(), Value::Int(i)),
                    ("x".to_string(), Value::Int(i)),
                ])
            })
            .collect();
        let store = RowStore::from([(
            "r".to_string(),
            RowRelation::new(vec!["n".into(), "x".into()], rows),
        )]);
        (query, program, store)
    }

    /// The QUERY checks before its first row and every
    /// `QUERY_CHECK_INTERVAL` rows, and stops at the first check that
    /// fails: with the third check failing it has evaluated two intervals
    /// of the filter and no more.
    #[test]
    fn an_in_memory_query_stops_at_the_check_that_fails() {
        let (query, program, store) = query_over(10 * QUERY_CHECK_INTERVAL as i64);
        let checks = Cell::new(0usize);
        let interrupt = || {
            checks.set(checks.get() + 1);
            if checks.get() == 3 {
                Err(LocyError::Timeout {
                    elapsed: std::time::Duration::from_millis(2),
                    limit: std::time::Duration::from_millis(1),
                })
            } else {
                Ok(())
            }
        };
        let err = evaluate_query(&query, &program, &LocyConfig::default(), &store, &interrupt)
            .expect_err("the third check stops the QUERY");
        assert!(matches!(err, LocyError::Timeout { .. }), "{err:?}");
        assert_eq!(checks.get(), 3, "no check after the one that failed");
    }

    /// Unbounded, it checks once per interval of the rows its passes (the
    /// filter, then the projection) evaluate between them.
    #[test]
    fn an_in_memory_query_checks_once_per_interval_of_each_pass() {
        let n = 3 * QUERY_CHECK_INTERVAL + 1;
        let (query, program, store) = query_over(n as i64);
        let checks = Cell::new(0usize);
        let rows = evaluate_query(&query, &program, &LocyConfig::default(), &store, &|| {
            checks.set(checks.get() + 1);
            Ok(())
        })
        .expect("runs");
        assert_eq!(rows.len(), n);
        assert_eq!(checks.get(), (2 * n).div_ceil(QUERY_CHECK_INTERVAL));
    }
}
