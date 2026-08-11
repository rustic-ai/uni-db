// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DERIVE command execution via `LocyExecutionContext`.
//!
//! Extracted from `uni-locy/src/orchestrator/mod.rs::derive_command`.
//! Uses `LocyExecutionContext` for fact lookup and mutation execution.
//!
//! Supports two modes:
//! - **execute mode** (`derive_command`): immediately applies mutations via `ctx.execute_mutation()`
//! - **collect mode** (`collect_derive_facts`): collects Cypher ASTs + vertex/edge data for deferred application

use std::collections::HashMap;

use uni_common::Properties;
use uni_cypher::ast::Query;
use uni_cypher::locy_ast::{DeriveClause, DeriveCommand, DerivePattern, RuleOutput};
use uni_locy::result::DerivedEdge;
use uni_locy::{CompiledProgram, FactRow, LocyError, LocyStats};

use super::locy_ast_builder::build_derive_create;
use super::locy_eval::eval_expr;
use super::locy_traits::LocyExecutionContext;
use crate::query::executor::result_normalizer::ResultNormalizer;

/// Output of `collect_derive_facts()` — collected but not yet executed.
pub struct CollectedDeriveOutput {
    pub queries: Vec<Query>,
    pub vertices: HashMap<String, Vec<Properties>>,
    pub edges: Vec<DerivedEdge>,
    pub affected: usize,
}

/// Execute a top-level DERIVE command (auto-apply mode).
///
/// Looks up facts from the native store via `ctx.lookup_derived()`, applies optional
/// WHERE filtering, and for each matching fact executes the DERIVE mutation via
/// `ctx.execute_mutation()`.
pub async fn derive_command(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    stats: &mut LocyStats,
) -> Result<usize, LocyError> {
    let collected = collect_derive_facts_inner(dc, program, ctx).await?;
    for query in collected.queries {
        ctx.execute_mutation(query, HashMap::new()).await?;
        stats.mutations_executed += 1;
    }
    Ok(collected.affected)
}

/// Collect derived facts without executing mutations (collect mode).
///
/// Returns the Cypher ASTs, vertex data, and edge data for deferred
/// application via `tx.apply()`.
pub async fn collect_derive_facts(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
) -> Result<CollectedDeriveOutput, LocyError> {
    collect_derive_facts_inner(dc, program, ctx).await
}

/// Shared implementation for both execute and collect modes.
async fn collect_derive_facts_inner(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
) -> Result<CollectedDeriveOutput, LocyError> {
    let rule_name = dc.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::EvaluationError {
            message: format!("rule '{}' not found for DERIVE command", rule_name),
        })?;

    let facts = ctx.lookup_derived_enriched(&rule_name).await?;

    // Apply optional WHERE filter
    let filtered: Vec<_> = if let Some(where_expr) = &dc.where_expr {
        facts
            .into_iter()
            .filter(|row| {
                eval_expr(where_expr, row)
                    .map(|v| v.as_bool().unwrap_or(false))
                    .unwrap_or(false)
            })
            .collect()
    } else {
        facts
    };

    let mut all_queries = Vec::new();
    let mut all_vertices: HashMap<String, Vec<Properties>> = HashMap::new();
    let mut all_edges = Vec::new();
    let mut affected = 0;

    for clause in &rule.clauses {
        if let RuleOutput::Derive(derive_clause) = &clause.output {
            for row in &filtered {
                for row in expand_group_bindings(derive_clause, row) {
                    let queries = build_derive_create(derive_clause, &row)?;
                    affected += queries.len();

                    // Extract vertex/edge data for inspection
                    extract_vertex_edge_data(
                        derive_clause,
                        &row,
                        &mut all_vertices,
                        &mut all_edges,
                    );

                    all_queries.extend(queries);
                }
            }
        }
    }

    Ok(CollectedDeriveOutput {
        queries: all_queries,
        vertices: all_vertices,
        edges: all_edges,
        affected,
    })
}

/// Expand a bindings row over any group variables the DERIVE head references.
///
/// A variable bound inside a quantified path pattern is a GQL group variable —
/// a list with one element per iteration of the quantifier — so a head that
/// references one is asking to derive a fact *per iteration*. Rather than
/// giving the head a list where it expects a node (which produced a derived
/// edge with an empty target), bind the i-th element of every referenced group
/// variable and emit one row per iteration. This is the implicit `UNWIND` a
/// user would otherwise have to write by hand.
///
/// Rows that reference no group variable are returned unchanged, so the common
/// case allocates nothing beyond the clone.
fn expand_group_bindings(derive_clause: &DeriveClause, row: &FactRow) -> Vec<FactRow> {
    use uni_common::Value;

    // Group variables all have one element per iteration, so they are the same
    // length and are zipped by index — not crossed. Position i of every list is
    // iteration i of the same match.
    // Locy's fact rows come from the raw read path, so a list element arrives
    // as a node-shaped map rather than a `Value::Node`. Normalize it to the
    // same shape a singleton binding has, or the head sees a map where it
    // expects a node.
    let as_nodes = |items: &[Value]| -> Option<Vec<Value>> {
        items
            .iter()
            .map(|v| match ResultNormalizer::normalize_value(v.clone()) {
                Ok(node @ Value::Node(_)) => Some(node),
                _ => None,
            })
            .collect()
    };

    let grouped: Vec<(String, Vec<Value>)> = derive_head_variables(derive_clause)
        .into_iter()
        .filter_map(|var| match row.get(var.as_str()) {
            Some(Value::List(items)) => as_nodes(items).map(|nodes| (var, nodes)),
            _ => None,
        })
        .collect();

    if grouped.is_empty() {
        return vec![row.clone()];
    }

    let iterations = grouped
        .iter()
        .map(|(_, items)| items.len())
        .min()
        .unwrap_or(0);
    (0..iterations)
        .map(|i| {
            let mut expanded = row.clone();
            for (name, items) in &grouped {
                expanded.insert(name.clone(), items[i].clone());
            }
            expanded
        })
        .collect()
}

/// The node variables a DERIVE head references, so `expand_group_bindings` only
/// expands what the head actually uses.
fn derive_head_variables(derive_clause: &DeriveClause) -> Vec<String> {
    match derive_clause {
        DeriveClause::Patterns(patterns) => patterns
            .iter()
            .flat_map(|p| [p.source.variable.clone(), p.target.variable.clone()])
            .filter(|v| !v.is_empty())
            .collect(),
        DeriveClause::Merge(a, b) => vec![a.clone(), b.clone()],
    }
}

/// Extract vertex and edge inspection data from a DeriveClause + bindings row.
fn extract_vertex_edge_data(
    derive_clause: &DeriveClause,
    row: &FactRow,
    vertices: &mut HashMap<String, Vec<Properties>>,
    edges: &mut Vec<DerivedEdge>,
) {
    match derive_clause {
        DeriveClause::Patterns(patterns) => {
            for pattern in patterns {
                extract_from_pattern(pattern, row, vertices, edges);
            }
        }
        DeriveClause::Merge(a, b) => {
            // MERGE produces an edge between two existing nodes, no new vertices
            let source_props = node_properties_from_binding(a, row);
            let target_props = node_properties_from_binding(b, row);
            edges.push(DerivedEdge {
                edge_type: "MERGED_WITH".to_string(),
                source_label: node_label_from_binding(a, row),
                source_properties: source_props,
                target_label: node_label_from_binding(b, row),
                target_properties: target_props,
                edge_properties: Properties::new(),
            });
        }
    }
}

/// Extract vertex/edge data from a single DerivePattern.
fn extract_from_pattern(
    pattern: &DerivePattern,
    row: &FactRow,
    vertices: &mut HashMap<String, Vec<Properties>>,
    edges: &mut Vec<DerivedEdge>,
) {
    let source = &pattern.source;
    let target = &pattern.target;
    let edge = &pattern.edge;

    let source_label = source
        .labels
        .first()
        .cloned()
        .unwrap_or_else(|| node_label_from_binding(&source.variable, row));
    let target_label = target
        .labels
        .first()
        .cloned()
        .unwrap_or_else(|| node_label_from_binding(&target.variable, row));

    let source_props = node_properties_from_binding(&source.variable, row);
    let target_props = node_properties_from_binding(&target.variable, row);

    if source.is_new {
        vertices
            .entry(source_label.clone())
            .or_default()
            .push(source_props.clone());
    }
    if target.is_new {
        vertices
            .entry(target_label.clone())
            .or_default()
            .push(target_props.clone());
    }

    let edge_props = edge
        .properties
        .as_ref()
        .and_then(|expr| eval_map_expr(expr, row))
        .unwrap_or_default();

    edges.push(DerivedEdge {
        edge_type: edge.edge_type.clone(),
        source_label,
        source_properties: source_props,
        target_label,
        target_properties: target_props,
        edge_properties: edge_props,
    });
}

/// Extract properties from a binding row for a node variable.
fn node_properties_from_binding(var: &str, row: &FactRow) -> Properties {
    use uni_common::Value;
    match row.get(var) {
        Some(Value::Node(node)) => node.properties.clone(),
        Some(Value::Map(map)) => map.clone(),
        _ => Properties::new(),
    }
}

/// Extract the label from a binding row for a node variable.
fn node_label_from_binding(var: &str, row: &FactRow) -> String {
    use uni_common::Value;
    match row.get(var) {
        Some(Value::Node(node)) => node.labels.first().cloned().unwrap_or_default(),
        _ => String::new(),
    }
}

/// Try to evaluate a map expression to Properties.
fn eval_map_expr(expr: &uni_cypher::ast::Expr, row: &FactRow) -> Option<Properties> {
    use uni_common::Value;
    match eval_expr(expr, row) {
        Ok(Value::Map(m)) => Some(m),
        _ => None,
    }
}
