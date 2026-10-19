// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Per-source evaluation of correlated `EXISTS { … }` / `COUNT { … }` bodies.
//!
//! A correlated subquery is evaluated by executing its body once per outer row.
//! For `MATCH (a), (b) WHERE EXISTS { MATCH (a)-[:R*1..]->(b) }` that is one
//! sub-plan per *pair*: every execution walks out of `a` again only to test a
//! different `b` at the end, so the cost is `|a| x |b|` walks where `|a|` would
//! do.
//!
//! This module answers such a body per *source* instead. When one outer node
//! variable `b` occurs in the body only as a bare pattern node, the body with
//! `b` left unbound returns, for one binding of the remaining imports, every
//! `b` it matches — one row per match, exactly as many as the bound body would
//! return for that `b`. So one execution with `b` free, grouped by `b`'s
//! identity, answers every outer row that shares the other imports: binding a
//! pattern variable is an equality constraint on it, and nothing else in the
//! body can see the difference.
//!
//! The restriction to a *bare* `(b)` is what keeps that exact. A label, a
//! property map or a `WHERE` that names `b` would read `b` from storage once
//! `b` is free, where the bound body reads the outer row's columns for it; the
//! two can disagree inside a query that writes `b` before the subquery. Those
//! bodies keep the per-row evaluation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, Schema};
use datafusion::error::Result as DFResult;
use uni_cypher::ast::{Clause, Expr, PatternElement, Query};

use super::expr_compiler::free_variables;

/// How many matched identities the per-operator cache may hold in total.
///
/// Past it, answers are still computed per source but no longer kept across
/// batches; the bound keeps a query over a large graph from accumulating one
/// reachable set per distinct source without limit.
const MAX_CACHED_IDENTITIES: usize = 1 << 22;

/// What a subquery body admits for per-source evaluation, from its text alone.
#[derive(Debug, Clone)]
pub(crate) struct BodyShape {
    /// Node variables that occur in the body only as bare `(v)` pattern nodes.
    /// Any of them that the outer row binds can be left free.
    candidates: Vec<String>,
    /// Every name the body can read from the outer row: its pattern variables,
    /// the variables its expressions name, and the parameters it names.
    referenced: HashSet<String>,
    /// Pattern variables, which are what can anchor the body.
    pattern_vars: HashSet<String>,
}

impl BodyShape {
    /// Analyses `query`, or returns `None` if the body is not a plain
    /// conjunction of non-optional `MATCH` clauses whose references can all be
    /// determined.
    pub(crate) fn analyze(query: &Query) -> Option<Self> {
        let Query::Single(single) = query else {
            return None;
        };
        let mut referenced: HashSet<String> = HashSet::new();
        let mut pattern_vars: HashSet<String> = HashSet::new();
        // Node variables seen bare, and every variable seen any other way.
        let mut bare_nodes: HashSet<String> = HashSet::new();
        let mut disqualified: HashSet<String> = HashSet::new();
        let mut exprs: Vec<&Expr> = Vec::new();

        for clause in &single.clauses {
            let Clause::Match(m) = clause else {
                return None;
            };
            if m.optional {
                return None;
            }
            if let Some(w) = &m.where_clause {
                exprs.push(w);
            }
            for path in &m.pattern.paths {
                if path.shortest_path_mode.is_some() {
                    return None;
                }
                if let Some(p) = &path.variable {
                    pattern_vars.insert(p.clone());
                    disqualified.insert(p.clone());
                }
                for element in &path.elements {
                    match element {
                        PatternElement::Node(n) => {
                            exprs.extend(n.properties.iter());
                            exprs.extend(n.where_clause.iter());
                            let Some(v) = &n.variable else { continue };
                            pattern_vars.insert(v.clone());
                            let bare = n.labels.names().is_empty()
                                && n.properties.is_none()
                                && n.where_clause.is_none();
                            if bare {
                                bare_nodes.insert(v.clone());
                            } else {
                                disqualified.insert(v.clone());
                            }
                        }
                        PatternElement::Relationship(r) => {
                            exprs.extend(r.properties.iter());
                            exprs.extend(r.where_clause.iter());
                            if let Some(v) = &r.variable {
                                pattern_vars.insert(v.clone());
                                disqualified.insert(v.clone());
                            }
                        }
                        // A quantified group binds its variables per iteration;
                        // not modelled.
                        PatternElement::Parenthesized { .. } => return None,
                    }
                }
            }
        }

        for expr in exprs {
            let vars = free_variables(expr)?;
            disqualified.extend(vars.iter().cloned());
            referenced.extend(vars);
            let mut params = Vec::new();
            parameter_names(expr, &mut params);
            disqualified.extend(params.iter().cloned());
            referenced.extend(params);
        }
        referenced.extend(pattern_vars.iter().cloned());

        let mut candidates: Vec<String> = bare_nodes
            .into_iter()
            .filter(|v| !disqualified.contains(v))
            .collect();
        candidates.sort();
        if candidates.is_empty() {
            return None;
        }
        Some(Self {
            candidates,
            referenced,
            pattern_vars,
        })
    }
}

/// Collects the names of the parameters `expr` reads.
fn parameter_names(expr: &Expr, out: &mut Vec<String>) {
    if let Expr::Parameter(name) = expr {
        out.push(name.clone());
    }
    expr.for_each_child(&mut |child| parameter_names(child, out));
}

/// Whether column `col` belongs to variable `var`: the variable's own column
/// or one of its `var.*` columns.
fn column_of(col: &str, var: &str) -> bool {
    col == var
        || col
            .strip_prefix(var)
            .is_some_and(|rest| rest.starts_with('.'))
}

/// One batch's per-source plan: which variable is left free, and each row's
/// grouping key and free-variable identity.
pub(crate) struct BatchGrouping {
    /// The outer node variable the body is executed with unbound.
    pub(crate) free_var: String,
    /// Each row's key over the other imports, or `None` when the row must be
    /// evaluated on its own (an import's identity, or the free variable's, is
    /// NULL).
    keys: Vec<Option<Vec<u8>>>,
    /// Each row's identity for the free variable.
    targets: Vec<Option<u64>>,
    /// How many rows of this batch share each key.
    counts: HashMap<Vec<u8>, usize>,
    /// The batch without the free variable's columns: what the free body is
    /// planned against and bound to.
    pub(crate) projected: RecordBatch,
}

impl BatchGrouping {
    /// Chooses a free variable for `batch` and groups its rows, or returns
    /// `None` when no candidate applies.
    ///
    /// A candidate applies when the outer row binds it as a node and at least
    /// one other import that the body's pattern names is an outer entity, so
    /// the free body stays anchored. Without that anchor the free body would
    /// be uncorrelated — answered once for every row, but by enumerating the
    /// whole graph, which can cost far more than the rows it replaces.
    pub(crate) fn build(shape: &BodyShape, batch: &RecordBatch) -> DFResult<Option<Self>> {
        let schema = batch.schema();
        let mut best: Option<Self> = None;
        for v in &shape.candidates {
            let Ok(target_idx) = schema.index_of(&format!("{v}._vid")) else {
                continue;
            };
            let key_vars: Vec<&String> = shape.referenced.iter().filter(|n| *n != v).collect();
            let anchored = key_vars.iter().any(|n| {
                shape.pattern_vars.contains(*n)
                    && (schema.index_of(&format!("{n}._vid")).is_ok()
                        || schema.index_of(&format!("{n}._eid")).is_ok())
            });
            if !anchored {
                continue;
            }
            let mut key_cols: Vec<usize> = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| key_vars.iter().any(|n| column_of(f.name(), n)))
                .map(|(i, _)| i)
                .collect();
            key_cols.sort_unstable();
            // Identity columns whose NULL sends a row down the per-row path: a
            // NULL-bound import is left exactly as the bound body treats it.
            let identity_cols: Vec<usize> = key_cols
                .iter()
                .copied()
                .filter(|&i| {
                    let name = schema.field(i).name();
                    name.ends_with("._vid") || name.ends_with("._eid")
                })
                .collect();

            let Some(rows) = encode_rows(batch, &key_cols)? else {
                continue;
            };
            let targets = identities(batch.column(target_idx))?;
            let mut keys = Vec::with_capacity(batch.num_rows());
            let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
            for (row, key) in rows.into_iter().enumerate() {
                let usable = targets[row].is_some()
                    && identity_cols.iter().all(|&c| !batch.column(c).is_null(row));
                if usable {
                    *counts.entry(key.clone()).or_default() += 1;
                    keys.push(Some(key));
                } else {
                    keys.push(None);
                }
            }

            let keep: Vec<usize> = (0..schema.fields().len())
                .filter(|&i| !column_of(schema.field(i).name(), v))
                .collect();
            let projected = batch.project(&keep)?;
            let grouping = Self {
                free_var: v.clone(),
                keys,
                targets,
                counts,
                projected,
            };
            if best
                .as_ref()
                .is_none_or(|b| grouping.counts.len() < b.counts.len())
            {
                best = Some(grouping);
            }
        }
        Ok(best)
    }

    /// The row's key and free-variable identity, when it can be grouped.
    pub(crate) fn row(&self, row: usize) -> Option<(&[u8], u64)> {
        Some((self.keys[row].as_deref()?, self.targets[row]?))
    }

    /// How many rows of this batch share `key`.
    pub(crate) fn batch_count(&self, key: &[u8]) -> usize {
        self.counts.get(key).copied().unwrap_or(0)
    }
}

/// Encodes `cols` of every row as comparable bytes, or `None` if a column's
/// type has no row encoding.
fn encode_rows(batch: &RecordBatch, cols: &[usize]) -> DFResult<Option<Vec<Vec<u8>>>> {
    if cols.is_empty() {
        return Ok(Some(vec![Vec::new(); batch.num_rows()]));
    }
    let fields: Vec<SortField> = cols
        .iter()
        .map(|&i| SortField::new(batch.schema().field(i).data_type().clone()))
        .collect();
    let Ok(converter) = RowConverter::new(fields) else {
        return Ok(None);
    };
    let arrays: Vec<ArrayRef> = cols.iter().map(|&i| batch.column(i).clone()).collect();
    let Ok(rows) = converter.convert_columns(&arrays) else {
        return Ok(None);
    };
    Ok(Some(rows.iter().map(|r| r.as_ref().to_vec()).collect()))
}

/// An identity column as `u64`s, `None` where NULL.
fn identities(col: &ArrayRef) -> DFResult<Vec<Option<u64>>> {
    let cast = arrow::compute::cast(col, &DataType::UInt64)?;
    let ids = cast.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
        datafusion::error::DataFusionError::Internal(
            "identity column did not cast to UInt64".to_string(),
        )
    })?;
    Ok(ids.iter().collect())
}

/// Matches per free-variable identity, from the free body's output batches.
///
/// Returns `None` when the output carries no identity column for `free_var`;
/// the caller then evaluates per row, as it would have without this module.
pub(crate) fn matches_by_identity(
    batches: &[RecordBatch],
    free_var: &str,
) -> DFResult<Option<HashMap<u64, usize>>> {
    let col_name = format!("{free_var}._vid");
    let mut out: HashMap<u64, usize> = HashMap::new();
    for batch in batches {
        let schema: Arc<Schema> = batch.schema();
        let Ok(idx) = schema.index_of(&col_name) else {
            if batch.num_rows() == 0 {
                continue;
            }
            return Ok(None);
        };
        for id in identities(batch.column(idx))?.into_iter().flatten() {
            *out.entry(id).or_default() += 1;
        }
    }
    Ok(Some(out))
}

/// Matches per free-variable identity for one source.
pub(crate) type MatchCounts = HashMap<u64, usize>;

/// Answers computed per source, kept across the batches one operator sees.
///
/// Scoped to one physical expression, which is built per query execution, so
/// an answer never outlives the statement that computed it.
#[derive(Default)]
pub(crate) struct SourceCache {
    answers: HashMap<(String, Vec<u8>), Arc<MatchCounts>>,
    /// Keys evaluated once per row already: a second sighting is what makes
    /// a free execution pay for itself.
    seen: HashSet<(String, Vec<u8>)>,
    held: usize,
}

impl SourceCache {
    /// The cached answer for `key`, if any.
    pub(crate) fn get(&self, free_var: &str, key: &[u8]) -> Option<Arc<HashMap<u64, usize>>> {
        self.answers
            .get(&(free_var.to_string(), key.to_vec()))
            .cloned()
    }

    /// Whether `key` was already evaluated by the per-row path.
    pub(crate) fn seen(&self, free_var: &str, key: &[u8]) -> bool {
        self.seen.contains(&(free_var.to_string(), key.to_vec()))
    }

    /// Records that `key` was evaluated by the per-row path.
    pub(crate) fn mark_seen(&mut self, free_var: &str, key: &[u8]) {
        if self.held < MAX_CACHED_IDENTITIES {
            self.held += 1;
            self.seen.insert((free_var.to_string(), key.to_vec()));
        }
    }

    /// Keeps `answer` for `key` while the cache has room.
    pub(crate) fn insert(&mut self, free_var: &str, key: &[u8], answer: Arc<HashMap<u64, usize>>) {
        let size = answer.len() + 1;
        if self.held + size <= MAX_CACHED_IDENTITIES {
            self.held += size;
            self.answers
                .insert((free_var.to_string(), key.to_vec()), answer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(cypher: &str) -> Query {
        let q = uni_cypher::parse(&format!(
            "MATCH (a), (b) WHERE EXISTS {{ {cypher} }} RETURN a"
        ))
        .expect("parses");
        let Query::Single(s) = q else { panic!() };
        let Clause::Match(m) = &s.clauses[0] else {
            panic!()
        };
        match m.where_clause.as_ref().expect("where") {
            Expr::Exists { query, .. } => (**query).clone(),
            other => panic!("not EXISTS: {other:?}"),
        }
    }

    #[test]
    fn bare_endpoint_is_a_candidate() {
        let s = BodyShape::analyze(&body("MATCH (a)-[:R*1..]->(b)")).expect("shape");
        assert_eq!(s.candidates, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn endpoint_read_elsewhere_is_not_a_candidate() {
        for text in [
            "MATCH (a)-[:R*1..]->(b) WHERE b.x = 1",
            "MATCH (a)-[:R*1..]->(b:L)",
            "MATCH (a)-[:R*1..]->(b {x: 1})",
            "MATCH (a)-[:R*1..]->(b) WHERE $b = 1",
        ] {
            let s = BodyShape::analyze(&body(text));
            assert!(
                s.as_ref()
                    .is_none_or(|s| !s.candidates.contains(&"b".to_string())),
                "{text}: {s:?}"
            );
        }
    }

    #[test]
    fn unsupported_bodies_decline() {
        for text in [
            "OPTIONAL MATCH (a)-[:R]->(b)",
            "MATCH (a)-[:R]->(b) RETURN b",
            "MATCH p = shortestPath((a)-[:R*]->(b))",
            "MATCH (a)-[:R]->(b) WHERE EXISTS { MATCH (b)-->() }",
        ] {
            assert!(BodyShape::analyze(&body(text)).is_none(), "{text}");
        }
    }
}
