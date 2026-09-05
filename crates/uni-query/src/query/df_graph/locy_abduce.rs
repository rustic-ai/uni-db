// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Abductive reasoning (ABDUCE) via `LocyExecutionContext`.
//!
//! Ported from `uni-locy/src/orchestrator/abduce.rs`. Uses `LocyExecutionContext`
//! for L0 fork/restore, mutations, and strata re-evaluation. Three-phase pipeline:
//! Phase 1: Build derivation tree via EXPLAIN.
//! Phase 2: Generate candidate modifications from tree leaves.
//! Phase 3: Validate each candidate via ASSUME (fork L0 + mutate + re-eval + restore).

use std::collections::HashMap;

use uni_common::Value;
use uni_cypher::ast::{
    BinaryOp, Clause, DeleteClause, Direction, Expr, LabelExpr, MatchClause, NodePattern,
    PathPattern, Pattern, PatternElement, Query, RelationshipPattern, SetClause, SetItem,
    Statement,
};
use uni_cypher::locy_ast::AbduceQuery;
use uni_locy::result::{AbductionResult, Modification, ValidatedModification};
use uni_locy::types::CompiledRule;
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_delta::RowStore;

use super::locy_ast_builder::value_to_expr;
use super::locy_eval::eval_expr;
use super::locy_explain::{ProvenanceStore, explain_rule};
use super::locy_traits::LocyExecutionContext;

/// Evaluate an ABDUCE query using a three-phase pipeline.
pub async fn evaluate_abduce(
    query: &AbduceQuery,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    derived_store: &mut RowStore,
    stats: &mut LocyStats,
    tracker: Option<&ProvenanceStore>,
) -> Result<AbductionResult, LocyError> {
    let rule_name = query.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::AbductionError {
            message: format!("rule '{}' not found for ABDUCE", rule_name),
        })?;

    // Get derived facts for the target rule
    let facts = ctx.lookup_derived(&rule_name)?;

    // Filter by WHERE expression
    let matching: Vec<FactRow> = if let Some(where_expr) = &query.where_expr {
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

    // Phase 1: Build derivation tree
    let explain_query = uni_cypher::locy_ast::ExplainRule {
        rule_name: query.rule_name.clone(),
        where_expr: query.where_expr.clone(),
        return_clause: None,
    };
    let derivation_tree = explain_rule(
        &explain_query,
        program,
        ctx,
        config,
        derived_store,
        stats,
        tracker,
        None,
    )
    .await?;

    // Phase 2: Generate candidate modifications from tree
    let mut candidates: Vec<Modification> = if query.negated {
        extract_removal_candidates(&derivation_tree, rule, &matching, program)
    } else {
        extract_addition_candidates(rule)
    };
    candidates.truncate(config.max_abduce_candidates);

    // Phase 3: Validate each candidate via ASSUME
    let mut validated = Vec::new();
    for candidate in candidates {
        let cost = compute_cost(&candidate);
        let is_valid = validate_modification(
            &candidate,
            query.negated,
            &rule_name,
            query.where_expr.as_ref(),
            program,
            ctx,
            config,
            stats,
        )
        .await?;

        validated.push(ValidatedModification {
            modification: candidate,
            validated: is_valid,
            cost,
        });
    }

    // Sort by cost ascending, validated first
    validated.sort_by(|a, b| {
        b.validated.cmp(&a.validated).then(
            a.cost
                .partial_cmp(&b.cost)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });
    validated.truncate(config.max_abduce_results);

    Ok(AbductionResult {
        modifications: validated,
    })
}

/// Phase 2 (ABDUCE NOT): Extract removal/change candidates from the derivation tree.
fn extract_removal_candidates(
    tree: &uni_locy::DerivationNode,
    rule: &CompiledRule,
    _matching: &[FactRow],
    program: &CompiledProgram,
) -> Vec<Modification> {
    let mut candidates = Vec::new();
    collect_leaf_candidates(tree, rule, program, &mut candidates);
    candidates
}

/// Recursively collect candidates from derivation tree leaves.
///
/// Each leaf node carries a `rule` field naming the rule that produced it —
/// this may differ from the top-level `rule` parameter when IS-ref children
/// come from a different rule (e.g. a `scored_signal` leaf inside a
/// `threat_level` derivation).  We always look up the effective rule from
/// `program` so that `clause.match_pattern` corresponds to the correct rule.
fn collect_leaf_candidates(
    node: &uni_locy::DerivationNode,
    rule: &CompiledRule,
    program: &CompiledProgram,
    candidates: &mut Vec<Modification>,
) {
    if node.children.is_empty() && node.graph_fact.is_some() {
        // Use the node's own rule when available; fall back to the caller's rule.
        let effective_rule: &CompiledRule = program.rule_catalog.get(&node.rule).unwrap_or(rule);

        if node.clause_index < effective_rule.clauses.len() {
            let clause = &effective_rule.clauses[node.clause_index];
            for element in &clause.match_pattern.paths {
                extract_edge_candidates(element, &node.bindings, candidates);
            }
        }

        for (key, value) in &node.bindings {
            if let Value::Map(props) = value {
                for (prop_name, prop_val) in props {
                    if prop_val.as_f64().is_some() {
                        candidates.push(Modification::ChangeProperty {
                            element_var: key.clone(),
                            property: prop_name.clone(),
                            old_value: Box::new(prop_val.clone()),
                            new_value: Box::new(zero_like(prop_val)),
                        });
                    }
                }
            }
            if value.as_f64().is_some() {
                candidates.push(Modification::ChangeProperty {
                    element_var: key.clone(),
                    property: key.clone(),
                    old_value: Box::new(value.clone()),
                    new_value: Box::new(zero_like(value)),
                });
            }
        }
    }

    for child in &node.children {
        collect_leaf_candidates(child, rule, program, candidates);
    }
}

/// Build the zero proposal for `old`, preserving its numeric type.
///
/// An abduced `ChangeProperty` is applied through Cypher `SET`, which enforces
/// the property's *declared* type. Proposing `Float(0.0)` for an `Int64`
/// property is rejected at write time, so the whole abduction fails rather than
/// the candidate simply being ruled out. Mirroring the old value's variant keeps
/// the proposal representable in the column it targets.
///
/// Non-numeric values have no zero and are returned as `Float(0.0)`; callers
/// only reach this for values that answered `as_f64()`.
fn zero_like(old: &Value) -> Value {
    match old {
        Value::Int(_) => Value::Int(0),
        _ => Value::Float(0.0),
    }
}

/// Extract edge removal candidates from a path pattern.
fn extract_edge_candidates(
    path: &PathPattern,
    bindings: &FactRow,
    candidates: &mut Vec<Modification>,
) {
    let mut source_var = String::new();
    // Index (into `candidates`) of the RemoveEdge awaiting its target node — the
    // node that follows the relationship we just pushed. Using this per-hop index
    // (not `candidates.last_mut()`) attributes each hop's target to ITS OWN edge,
    // so a multi-hop path `(a)-[:R1]->(b)-[:R2]->(c)` yields (a,b),(b,c) instead of
    // an empty first target and a (b,b) self-loop on the last edge.
    let mut pending_target: Option<usize> = None;
    for element in &path.elements {
        match element {
            PatternElement::Node(node) => {
                if let Some(var) = &node.variable {
                    if let Some(idx) = pending_target.take()
                        && let Some(Modification::RemoveEdge { target_var, .. }) =
                            candidates.get_mut(idx)
                        && target_var.is_empty()
                    {
                        *target_var = var.clone();
                    }
                    source_var = var.clone();
                } else {
                    pending_target = None;
                }
            }
            PatternElement::Relationship(rel) => {
                let edge_var = rel.variable.clone().unwrap_or_default();
                let edge_type = rel.types.first().cloned().unwrap_or_default();
                let mut match_properties = HashMap::new();
                if let Some(Value::String(s)) = bindings.get(&source_var) {
                    match_properties.insert(source_var.clone(), Value::String(s.clone()));
                }
                candidates.push(Modification::RemoveEdge {
                    source_var: source_var.clone(),
                    target_var: String::new(),
                    edge_var: edge_var.clone(),
                    edge_type,
                    match_properties,
                });
                pending_target = Some(candidates.len() - 1);
            }
            _ => {}
        }
    }
}

/// Phase 2 (positive ABDUCE): Generate addition candidates from rule patterns.
fn extract_addition_candidates(rule: &CompiledRule) -> Vec<Modification> {
    let mut candidates = Vec::new();
    for clause in &rule.clauses {
        for path in &clause.match_pattern.paths {
            let mut source_var = String::new();
            // Per-hop target index (see extract_edge_candidates) — attributes each
            // edge's target to ITS OWN AddEdge, not `candidates.last_mut()`.
            let mut pending_target: Option<usize> = None;
            for element in &path.elements {
                match element {
                    PatternElement::Node(node) => {
                        if let Some(var) = &node.variable {
                            if let Some(idx) = pending_target.take()
                                && let Some(Modification::AddEdge { target_var, .. }) =
                                    candidates.get_mut(idx)
                                && target_var.is_empty()
                            {
                                *target_var = var.clone();
                            }
                            source_var = var.clone();
                        } else {
                            pending_target = None;
                        }
                    }
                    PatternElement::Relationship(rel) => {
                        let edge_type = rel.types.first().cloned().unwrap_or_default();
                        candidates.push(Modification::AddEdge {
                            source_var: source_var.clone(),
                            target_var: String::new(),
                            edge_type,
                            properties: HashMap::new(),
                        });
                        pending_target = Some(candidates.len() - 1);
                    }
                    _ => {}
                }
            }
        }
    }
    candidates
}

/// Phase 3: Validate a single modification via ASSUME (fork L0 + mutate + re-eval + restore).
#[expect(
    clippy::too_many_arguments,
    reason = "validation requires full program and execution context"
)]
async fn validate_modification(
    modification: &Modification,
    negated: bool,
    rule_name: &str,
    where_expr: Option<&Expr>,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    stats: &mut LocyStats,
) -> Result<bool, LocyError> {
    let mutation_query = modification_to_cypher(modification);

    // Fork L0 for hypothetical reasoning
    ctx.fork_l0()
        .await
        .map_err(|e| LocyError::SavepointFailed {
            message: format!("ABDUCE fork L0 failed: {}", e),
        })?;

    // Execute the mutation
    ctx.execute_mutation(mutation_query, HashMap::new()).await?;
    stats.mutations_executed += 1;

    // Re-evaluate strata
    let assume_store: RowStore = ctx.re_evaluate_strata(program, config).await?;

    // Check if the conclusion still holds
    let facts = assume_store
        .get(rule_name)
        .map(|r| r.rows.clone())
        .unwrap_or_default();

    let matching: Vec<FactRow> = if let Some(where_expr) = where_expr {
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

    // Restore L0 (discard hypothetical mutations)
    ctx.restore_l0()
        .await
        .map_err(|e| LocyError::SavepointFailed {
            message: format!("ABDUCE restore L0 failed: {}", e),
        })?;

    if negated {
        Ok(matching.is_empty())
    } else {
        Ok(!matching.is_empty())
    }
}

/// Convert a Modification to a Cypher mutation query.
fn modification_to_cypher(modification: &Modification) -> Query {
    match modification {
        Modification::RemoveEdge {
            source_var,
            target_var,
            edge_var,
            edge_type,
            match_properties,
        } => {
            let src_var = if source_var.is_empty() {
                "src".to_string()
            } else {
                source_var.clone()
            };
            let tgt_var = if target_var.is_empty() {
                "tgt".to_string()
            } else {
                target_var.clone()
            };
            // The rule body's relationship is frequently anonymous (empty
            // `edge_var`). The edge variable exists only so we can DELETE the
            // matched relationship, so any name works — EXCEPT one that collides
            // with an endpoint node variable. A rule body like
            // `(a)-[:REMEDIATED_BY]->(r:RemediationAction)` has an anonymous edge
            // and a target node literally named `r`; blindly defaulting the edge
            // var to `"r"` produced `MATCH (src)-[r:...]->(r) DELETE r`, which the
            // planner rejects as VariableTypeConflict. Pick a base default that is
            // unlikely to appear in a rule body, then disambiguate against both
            // endpoints (covers the case where the rule itself names the edge the
            // same as an endpoint).
            let edge_var_name = {
                let mut candidate = if edge_var.is_empty() {
                    "_abd_edge".to_string()
                } else {
                    edge_var.clone()
                };
                while candidate == src_var || candidate == tgt_var {
                    candidate.push('_');
                }
                candidate
            };

            let where_conditions: Vec<Expr> = match_properties
                .iter()
                .map(|(k, v)| Expr::BinaryOp {
                    left: Box::new(Expr::Property(
                        Box::new(Expr::Variable(k.clone())),
                        k.clone(),
                    )),
                    op: BinaryOp::Eq,
                    right: Box::new(value_to_expr(v)),
                })
                .collect();

            let where_clause = if where_conditions.is_empty() {
                None
            } else {
                Some(
                    where_conditions
                        .into_iter()
                        .reduce(|a, b| Expr::BinaryOp {
                            left: Box::new(a),
                            op: BinaryOp::And,
                            right: Box::new(b),
                        })
                        .unwrap(),
                )
            };

            let path = PathPattern {
                variable: None,
                elements: vec![
                    PatternElement::Node(NodePattern {
                        variable: Some(src_var),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Relationship(RelationshipPattern {
                        variable: Some(edge_var_name.clone()),
                        types: LabelExpr::Disjunction(vec![edge_type.clone()]),
                        direction: Direction::Outgoing,
                        range: None,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Node(NodePattern {
                        variable: Some(tgt_var),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                ],
                shortest_path_mode: None,
            };

            Query::Single(Statement {
                clauses: vec![
                    Clause::Match(MatchClause {
                        optional: false,
                        pattern: Pattern { paths: vec![path] },
                        where_clause,
                        for_update: false,
                    }),
                    Clause::Delete(DeleteClause {
                        detach: false,
                        items: vec![Expr::Variable(edge_var_name)],
                    }),
                ],
            })
        }

        Modification::ChangeProperty {
            element_var,
            property,
            new_value,
            ..
        } => {
            let path = PathPattern {
                variable: None,
                elements: vec![PatternElement::Node(NodePattern {
                    variable: Some(element_var.clone()),
                    labels: LabelExpr::Empty,
                    properties: None,
                    where_clause: None,
                })],
                shortest_path_mode: None,
            };

            Query::Single(Statement {
                clauses: vec![
                    Clause::Match(MatchClause {
                        optional: false,
                        pattern: Pattern { paths: vec![path] },
                        where_clause: None,
                        for_update: false,
                    }),
                    Clause::Set(SetClause {
                        items: vec![SetItem::Property {
                            expr: Expr::Property(
                                Box::new(Expr::Variable(element_var.clone())),
                                property.clone(),
                            ),
                            value: value_to_expr(new_value),
                        }],
                    }),
                ],
            })
        }

        Modification::AddEdge {
            source_var,
            target_var,
            edge_type,
            properties,
        } => {
            let src_var = if source_var.is_empty() {
                "src".to_string()
            } else {
                source_var.clone()
            };
            let tgt_var = if target_var.is_empty() {
                "tgt".to_string()
            } else {
                target_var.clone()
            };

            let match_path = PathPattern {
                variable: None,
                elements: vec![
                    PatternElement::Node(NodePattern {
                        variable: Some(src_var.clone()),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Node(NodePattern {
                        variable: Some(tgt_var.clone()),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                ],
                shortest_path_mode: None,
            };

            let edge_props = if properties.is_empty() {
                None
            } else {
                Some(Expr::Map(
                    properties
                        .iter()
                        .map(|(k, v)| (k.clone(), value_to_expr(v)))
                        .collect(),
                ))
            };

            let create_path = PathPattern {
                variable: None,
                elements: vec![
                    PatternElement::Node(NodePattern {
                        variable: Some(src_var),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Relationship(RelationshipPattern {
                        variable: None,
                        types: LabelExpr::Disjunction(vec![edge_type.clone()]),
                        direction: Direction::Outgoing,
                        range: None,
                        properties: edge_props,
                        where_clause: None,
                    }),
                    PatternElement::Node(NodePattern {
                        variable: Some(tgt_var),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                ],
                shortest_path_mode: None,
            };

            Query::Single(Statement {
                clauses: vec![
                    Clause::Match(MatchClause {
                        optional: false,
                        pattern: Pattern {
                            paths: vec![match_path],
                        },
                        where_clause: None,
                        for_update: false,
                    }),
                    Clause::Create(uni_cypher::ast::CreateClause {
                        pattern: Pattern {
                            paths: vec![create_path],
                        },
                    }),
                ],
            })
        }
    }
}

fn compute_cost(modification: &Modification) -> f64 {
    match modification {
        Modification::RemoveEdge { .. } => 1.0,
        Modification::ChangeProperty { .. } => 0.5,
        Modification::AddEdge { .. } => 1.5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Extract (source_var, edge_var, target_var) from the MATCH path of a
    /// RemoveEdge query produced by `modification_to_cypher`.
    fn extract_pattern_vars(query: &Query) -> (String, String, String) {
        let Query::Single(stmt) = query else {
            panic!("expected single statement");
        };
        let Clause::Match(m) = &stmt.clauses[0] else {
            panic!("expected MATCH as first clause");
        };
        let elems = &m.pattern.paths[0].elements;
        let mut src = String::new();
        let mut edge = String::new();
        let mut tgt = String::new();
        let mut seen_edge = false;
        for e in elems {
            match e {
                PatternElement::Node(n) => {
                    let v = n.variable.clone().unwrap_or_default();
                    if seen_edge {
                        tgt = v;
                    } else {
                        src = v;
                    }
                }
                PatternElement::Relationship(r) => {
                    edge = r.variable.clone().unwrap_or_default();
                    seen_edge = true;
                }
                _ => {}
            }
        }
        (src, edge, tgt)
    }

    /// Regression: an anonymous edge in the rule body into a node literally named
    /// `r` (`(a)-[:REMEDIATED_BY]->(r:RemediationAction)`) must NOT default the
    /// edge variable to the colliding `"r"`. Previously this produced
    /// `MATCH (a)-[r:...]->(r) DELETE r`, which the planner rejects with
    /// `VariableTypeConflict - Variable 'r' already defined as relationship`.
    #[test]
    fn remove_edge_anonymous_edge_into_node_named_r_no_var_collision() {
        let modification = Modification::RemoveEdge {
            source_var: "a".to_string(),
            target_var: "r".to_string(),
            edge_var: String::new(), // anonymous relationship in the rule body
            edge_type: "REMEDIATED_BY".to_string(),
            match_properties: HashMap::new(),
        };

        let query = modification_to_cypher(&modification);
        let (src, edge, tgt) = extract_pattern_vars(&query);

        assert_eq!(src, "a");
        assert_eq!(tgt, "r", "target node keeps its rule-body name");
        assert_ne!(
            edge, tgt,
            "edge var must not collide with the target node var (was the bug)"
        );
        assert_ne!(
            edge, src,
            "edge var must not collide with the source node var"
        );

        // The DELETE clause must target the (renamed) edge var, not the node.
        let Query::Single(stmt) = &query else {
            unreachable!()
        };
        let Clause::Delete(d) = &stmt.clauses[1] else {
            panic!("expected DELETE as second clause");
        };
        assert_eq!(
            d.items,
            vec![Expr::Variable(edge.clone())],
            "DELETE must reference the disambiguated edge variable"
        );
    }

    /// A named edge that happens to equal an endpoint node var is also
    /// disambiguated (defense in depth for pathological rule bodies).
    #[test]
    fn remove_edge_named_edge_colliding_with_endpoint_is_disambiguated() {
        let modification = Modification::RemoveEdge {
            source_var: "x".to_string(),
            target_var: "e".to_string(),
            edge_var: "e".to_string(), // collides with target node var
            edge_type: "R".to_string(),
            match_properties: HashMap::new(),
        };
        let query = modification_to_cypher(&modification);
        let (src, edge, tgt) = extract_pattern_vars(&query);
        assert_eq!(src, "x");
        assert_eq!(tgt, "e");
        assert_ne!(edge, tgt);
        assert_ne!(edge, src);
    }

    /// The common case (distinct named edge) is unchanged.
    #[test]
    fn remove_edge_named_edge_preserved() {
        let modification = Modification::RemoveEdge {
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_var: "rel".to_string(),
            edge_type: "R".to_string(),
            match_properties: HashMap::new(),
        };
        let query = modification_to_cypher(&modification);
        let (src, edge, tgt) = extract_pattern_vars(&query);
        assert_eq!(src, "a");
        assert_eq!(edge, "rel");
        assert_eq!(tgt, "b");
    }
}
