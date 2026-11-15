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

use std::collections::{BTreeSet, HashMap};

use uni_common::Properties;
use uni_cypher::ast::Query;
use uni_cypher::locy_ast::{DeriveClause, DeriveCommand, DerivePattern, RuleOutput};
use uni_locy::incremental::{MutationFootprint, query_footprint};
use uni_locy::result::DerivedEdge;
use uni_locy::{CompiledProgram, FactRow, LocyError, LocyStats};

use super::locy_ast_builder::build_derive_create;
use super::locy_delta::RowStore;
use super::locy_eval::{eval_condition, eval_expr};
use super::locy_traits::LocyExecutionContext;
use crate::query::executor::result_normalizer::ResultNormalizer;

/// Output of `collect_derive_facts()` — collected but not yet executed.
pub struct CollectedDeriveOutput {
    pub queries: Vec<Query>,
    pub vertices: HashMap<String, Vec<Properties>>,
    pub edges: Vec<DerivedEdge>,
    pub affected: usize,
    /// The ids of the existing nodes the queries name (a DERIVE head's
    /// bound endpoints). A replay finds each by this id, so each must exist
    /// where the queries are replayed.
    pub bound_vids: BTreeSet<u64>,
}

/// Execute a top-level DERIVE command (auto-apply mode).
///
/// Looks up the rule's facts via `ctx.lookup_derived_enriched()` against
/// `derived_store` (the issuing command's store), applies optional WHERE
/// filtering, and for each matching fact executes the DERIVE mutation via
/// `ctx.execute_mutation()`.
pub async fn derive_command(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    derived_store: &RowStore,
    stats: &mut LocyStats,
) -> Result<usize, LocyError> {
    derive_command_footprint(dc, program, ctx, derived_store, stats)
        .await
        .map(|(affected, _)| affected)
}

/// Executes a DERIVE command as [`derive_command`] does, and also returns
/// the footprint of the mutations it ran (empty when it derived nothing).
///
/// A caller that keeps facts across commands (an `ASSUME` body) needs the
/// footprint to tell which of them the DERIVE made stale.
///
/// # Errors
///
/// A rule lookup, `WHERE` evaluation or mutation failure.
pub async fn derive_command_footprint(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    derived_store: &RowStore,
    stats: &mut LocyStats,
) -> Result<(usize, MutationFootprint), LocyError> {
    let collected = collect_derive_facts_inner(dc, program, ctx, derived_store).await?;
    let mut footprint = MutationFootprint::default();
    for query in &collected.queries {
        footprint.extend(&query_footprint(query));
    }
    for statement in batch_derive_statements(collected.queries) {
        ctx.execute_mutation(statement.query, statement.params)
            .await?;
        // One per derived fact, however many statements wrote them.
        stats.mutations_executed += statement.facts;
    }
    Ok((collected.affected, footprint))
}

/// The parameter a batched DERIVE statement unwinds, and the variable it
/// unwinds each fact's ids into.
const DERIVE_FACTS_PARAM: &str = "__derive_facts";
const DERIVE_FACT_VAR: &str = "__derive_fact";

/// The most facts one batched statement carries: beyond the `UNWIND` list
/// size the planner turns into an id filter on the scans, a statement would
/// scan its labels whole.
const DERIVE_BATCH_SIZE: usize = 10_000;

/// A statement writing one or more DERIVE facts.
struct DeriveStatement {
    query: Query,
    params: HashMap<String, uni_common::Value>,
    /// The facts the statement writes.
    facts: usize,
}

/// Writes DERIVE facts that differ only in the nodes they connect with one
/// statement, not one each.
///
/// A fact between existing nodes is written by
/// `MATCH (a), (b) WHERE id(a) = X AND id(b) = Y CREATE (a)-[:T]->(b)`
/// (`build_create_from_derive_pattern`). A thousand facts were a thousand
/// such statements, each planned and run on its own. Statements equal up to
/// the ids are grouped and run as
/// `UNWIND $__derive_facts AS f MATCH (a), (b) WHERE id(a) = f.a AND id(b) = f.b CREATE …`,
/// in the order their first fact appeared. A statement of any other shape
/// (a `NEW` node, `DERIVE MERGE`), and a group of one, runs as it was built.
fn batch_derive_statements(queries: Vec<Query>) -> Vec<DeriveStatement> {
    use uni_common::Value;

    enum Group {
        Single(Query),
        Batch {
            template: Query,
            first: Query,
            rows: Vec<Value>,
        },
    }
    let mut groups: Vec<Group> = Vec::new();
    for query in queries {
        let Some((template, row)) = lift_fact_ids(&query) else {
            groups.push(Group::Single(query));
            continue;
        };
        let existing = groups.iter_mut().find_map(|g| match g {
            Group::Batch {
                template: t, rows, ..
            } if *t == template => Some(rows),
            _ => None,
        });
        match existing {
            Some(rows) => rows.push(row),
            None => groups.push(Group::Batch {
                template,
                first: query,
                rows: vec![row],
            }),
        }
    }

    let mut statements = Vec::new();
    for group in groups {
        match group {
            Group::Single(query) => statements.push(DeriveStatement {
                query,
                params: HashMap::new(),
                facts: 1,
            }),
            Group::Batch { first, rows, .. } if rows.len() == 1 => {
                statements.push(DeriveStatement {
                    query: first,
                    params: HashMap::new(),
                    facts: 1,
                });
            }
            Group::Batch { template, rows, .. } => {
                for chunk in rows.chunks(DERIVE_BATCH_SIZE) {
                    statements.push(DeriveStatement {
                        query: template.clone(),
                        params: HashMap::from([(
                            DERIVE_FACTS_PARAM.to_string(),
                            Value::List(chunk.to_vec()),
                        )]),
                        facts: chunk.len(),
                    });
                }
            }
        }
    }
    statements
}

/// `query` with the node ids it names lifted into a row of the unwound
/// parameter: the statement every fact of its shape shares, and this fact's
/// row. `None` unless `query` is `MATCH … WHERE id(v) = <int> [AND …] CREATE …`
/// with nothing else in its `WHERE`.
fn lift_fact_ids(query: &Query) -> Option<(Query, uni_common::Value)> {
    use uni_common::Value;
    use uni_cypher::ast::{
        BinaryOp, Clause, CypherLiteral, Expr, MatchClause, Statement, UnwindClause,
    };

    fn lift(expr: &Expr, row: &mut HashMap<String, Value>) -> Option<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => Some(Expr::BinaryOp {
                left: Box::new(lift(left, row)?),
                op: BinaryOp::And,
                right: Box::new(lift(right, row)?),
            }),
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                let Expr::FunctionCall { name, args, .. } = left.as_ref() else {
                    return None;
                };
                let [Expr::Variable(variable)] = args.as_slice() else {
                    return None;
                };
                let Expr::Literal(CypherLiteral::Integer(id)) = right.as_ref() else {
                    return None;
                };
                if name != "id" || row.insert(variable.clone(), Value::Int(*id)).is_some() {
                    return None;
                }
                Some(Expr::BinaryOp {
                    left: left.clone(),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Property(
                        Box::new(Expr::Variable(DERIVE_FACT_VAR.to_string())),
                        variable.clone(),
                    )),
                })
            }
            _ => None,
        }
    }

    let Query::Single(statement) = query else {
        return None;
    };
    let [Clause::Match(m), create @ Clause::Create(_)] = statement.clauses.as_slice() else {
        return None;
    };
    let mut row = HashMap::new();
    let where_clause = lift(m.where_clause.as_ref()?, &mut row)?;
    let template = Query::Single(Statement {
        clauses: vec![
            Clause::Unwind(UnwindClause {
                expr: Expr::Parameter(DERIVE_FACTS_PARAM.to_string()),
                variable: DERIVE_FACT_VAR.to_string(),
            }),
            Clause::Match(MatchClause {
                where_clause: Some(where_clause),
                ..m.clone()
            }),
            create.clone(),
        ],
    });
    Some((template, Value::Map(row)))
}

/// Collect derived facts without executing mutations (collect mode).
///
/// Returns the Cypher ASTs, vertex data, and edge data for deferred
/// application via `tx.apply()`.
pub async fn collect_derive_facts(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    derived_store: &RowStore,
) -> Result<CollectedDeriveOutput, LocyError> {
    collect_derive_facts_inner(dc, program, ctx, derived_store).await
}

/// Shared implementation for both execute and collect modes.
async fn collect_derive_facts_inner(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    derived_store: &RowStore,
) -> Result<CollectedDeriveOutput, LocyError> {
    let rule_name = dc.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::EvaluationError {
            message: format!("rule '{}' not found for DERIVE command", rule_name),
        })?;

    let facts = ctx
        .lookup_derived_enriched(&rule_name, derived_store)
        .await?;

    // Apply optional WHERE filter
    let filtered: Vec<_> = if let Some(where_expr) = &dc.where_expr {
        facts
            .into_iter()
            .map(|row| Ok(eval_condition(where_expr, &row, "DERIVE WHERE")?.then_some(row)))
            .filter_map(Result::transpose)
            .collect::<Result<_, LocyError>>()?
    } else {
        facts
    };

    let mut all_queries = Vec::new();
    let mut all_vertices: HashMap<String, Vec<Properties>> = HashMap::new();
    let mut all_edges = Vec::new();
    let mut affected = 0;
    let mut bound_vids = BTreeSet::new();

    for clause in &rule.clauses {
        if let RuleOutput::Derive(derive_clause) = &clause.output {
            let head = derive_head_variables(derive_clause);
            for row in &filtered {
                for row in expand_group_bindings(derive_clause, row) {
                    let queries = build_derive_create(derive_clause, &row)?;
                    affected += queries.len();
                    if !queries.is_empty() {
                        bound_vids.extend(head.iter().filter_map(|var| match row.get(var) {
                            Some(uni_common::Value::Node(n)) => Some(n.vid.as_u64()),
                            _ => None,
                        }));
                    }

                    // Extract vertex/edge data for inspection
                    extract_vertex_edge_data(
                        derive_clause,
                        &row,
                        &mut all_vertices,
                        &mut all_edges,
                    )?;

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
        bound_vids,
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
) -> Result<(), LocyError> {
    match derive_clause {
        DeriveClause::Patterns(patterns) => {
            for pattern in patterns {
                extract_from_pattern(pattern, row, vertices, edges)?;
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
    Ok(())
}

/// Extract vertex/edge data from a single DerivePattern.
fn extract_from_pattern(
    pattern: &DerivePattern,
    row: &FactRow,
    vertices: &mut HashMap<String, Vec<Properties>>,
    edges: &mut Vec<DerivedEdge>,
) -> Result<(), LocyError> {
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

    let edge_props = match &edge.properties {
        Some(expr) => eval_map_expr(expr, row)?,
        None => Properties::new(),
    };

    edges.push(DerivedEdge {
        edge_type: edge.edge_type.clone(),
        source_label,
        source_properties: source_props,
        target_label,
        target_properties: target_props,
        edge_properties: edge_props,
    });
    Ok(())
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

/// Evaluates a derived edge's property map.
///
/// An evaluation error, or a value that is not a map, is an error; both used to
/// report the derived edge with no properties at all.
fn eval_map_expr(expr: &uni_cypher::ast::Expr, row: &FactRow) -> Result<Properties, LocyError> {
    use uni_common::Value;
    match eval_expr(expr, row)? {
        Value::Map(m) => Ok(m),
        other => Err(LocyError::TypeError {
            message: format!("DERIVE edge properties must be a map, got {other:?}"),
        }),
    }
}
