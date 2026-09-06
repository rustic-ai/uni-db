// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cypher AST building utilities for Locy native execution.
//!
//! Relocated from `uni-locy/src/orchestrator/ast_builder.rs` during Phase 7.

use std::collections::HashMap;

use uni_common::Value;
use uni_cypher::ast::{
    BinaryOp, Clause, CreateClause, CypherLiteral, Direction, Expr, LabelExpr, MatchClause,
    MergeClause, NodePattern, PathPattern, Pattern, PatternElement, Query, RelationshipPattern,
    ReturnClause, ReturnItem, Statement,
};
use uni_cypher::locy_ast::{DeriveClause, DeriveNodeSpec, DerivePattern};
use uni_locy::{FactRow, LocyError};

/// Build a `MATCH pattern RETURN *` query from a compiled clause's match pattern.
pub fn build_match_return_query(pattern: &Pattern, where_conditions: &[Expr]) -> Query {
    let where_clause = combine_where_conditions(where_conditions);

    let match_clause = Clause::Match(MatchClause {
        optional: false,
        pattern: pattern.clone(),
        where_clause,
        for_update: false,
    });

    let return_clause = Clause::Return(ReturnClause {
        distinct: false,
        items: vec![ReturnItem::All],
        order_by: None,
        skip: None,
        limit: None,
    });

    Query::Single(Statement {
        clauses: vec![match_clause, return_clause],
    })
}

/// Build a CREATE query for a DERIVE clause's patterns.
pub fn build_derive_create(
    derive: &DeriveClause,
    bindings: &FactRow,
) -> Result<Vec<Query>, LocyError> {
    match derive {
        DeriveClause::Patterns(patterns) => {
            let mut queries = Vec::new();
            for pattern in patterns {
                let query = build_create_from_derive_pattern(pattern, bindings)?;
                queries.push(query);
            }
            Ok(queries)
        }
        DeriveClause::Merge(a, b) => {
            let query = build_merge_query(a, b, bindings)?;
            Ok(vec![query])
        }
    }
}

/// Build a MATCH+CREATE query from a single derive pattern.
fn build_create_from_derive_pattern(
    pattern: &DerivePattern,
    bindings: &FactRow,
) -> Result<Query, LocyError> {
    let source = &pattern.source;
    let edge = &pattern.edge;
    let target = &pattern.target;
    let direction = pattern.direction.clone();

    let source_is_existing =
        !source.is_new && matches!(bindings.get(&source.variable), Some(Value::Node(_)));
    let target_is_existing =
        !target.is_new && matches!(bindings.get(&target.variable), Some(Value::Node(_)));

    let mut clauses = Vec::new();

    if source_is_existing || target_is_existing {
        let mut match_paths = Vec::new();
        let mut vid_filters: Vec<Expr> = Vec::new();
        if source_is_existing {
            let (node_pat, vid_filter) = match_node_from_binding(&source.variable, bindings);
            match_paths.push(PathPattern {
                variable: None,
                elements: vec![PatternElement::Node(node_pat)],
                shortest_path_mode: None,
            });
            if let Some(f) = vid_filter {
                vid_filters.push(f);
            }
        }
        if target_is_existing {
            let (node_pat, vid_filter) = match_node_from_binding(&target.variable, bindings);
            match_paths.push(PathPattern {
                variable: None,
                elements: vec![PatternElement::Node(node_pat)],
                shortest_path_mode: None,
            });
            if let Some(f) = vid_filter {
                vid_filters.push(f);
            }
        }
        let where_clause = combine_where_conditions(&vid_filters);
        clauses.push(Clause::Match(MatchClause {
            optional: false,
            pattern: Pattern { paths: match_paths },
            where_clause,
            for_update: false,
        }));
    }

    let source_create = if source_is_existing {
        NodePattern {
            variable: Some(source.variable.clone()),
            labels: LabelExpr::Empty,
            properties: None,
            where_clause: None,
        }
    } else {
        node_spec_to_pattern(source, bindings)
    };

    let target_create = if target_is_existing {
        NodePattern {
            variable: Some(target.variable.clone()),
            labels: LabelExpr::Empty,
            properties: None,
            where_clause: None,
        }
    } else {
        node_spec_to_pattern(target, bindings)
    };

    let rel = PatternElement::Relationship(RelationshipPattern {
        variable: None,
        types: LabelExpr::Disjunction(vec![edge.edge_type.clone()]),
        direction,
        range: None,
        properties: edge.properties.clone(),
        where_clause: None,
    });

    let path = PathPattern {
        variable: None,
        elements: vec![
            PatternElement::Node(source_create),
            rel,
            PatternElement::Node(target_create),
        ],
        shortest_path_mode: None,
    };

    clauses.push(Clause::Create(CreateClause {
        pattern: Pattern { paths: vec![path] },
    }));

    Ok(Query::Single(Statement { clauses }))
}

/// Build a MATCH node pattern that rebinds an existing graph node by VID.
///
/// Uses `WHERE id(var) = <vid>` on the enclosing MATCH clause rather than
/// property matching.  Property-based matching breaks in schema mode because
/// internal columns (e.g. `overflow_json`) may appear as `Null`-valued
/// properties.  Since `x = null` is always *unknown* in three-valued logic,
/// the MATCH returns zero rows and the subsequent CREATE does nothing.
///
/// Returns `(NodePattern, Option<Expr>)` — the node pattern and an optional
/// VID-equality predicate for the MATCH WHERE clause.
fn match_node_from_binding(var_name: &str, bindings: &FactRow) -> (NodePattern, Option<Expr>) {
    if let Some(Value::Node(node)) = bindings.get(var_name) {
        let vid_i64 = node.vid.as_u64() as i64;
        let vid_filter = Expr::BinaryOp {
            left: Box::new(Expr::FunctionCall {
                name: "id".to_string(),
                args: vec![Expr::Variable(var_name.to_string())],
                distinct: false,
                window_spec: None,
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(CypherLiteral::Integer(vid_i64))),
        };
        (
            NodePattern {
                variable: Some(var_name.to_string()),
                labels: LabelExpr::from_conjunction(node.labels.iter().cloned()),
                properties: None,
                where_clause: None,
            },
            Some(vid_filter),
        )
    } else {
        (
            NodePattern {
                variable: Some(var_name.to_string()),
                labels: LabelExpr::Empty,
                properties: None,
                where_clause: None,
            },
            None,
        )
    }
}

/// Build a MERGE query for DERIVE MERGE a, b.
pub fn build_merge_query(a: &str, b: &str, _bindings: &FactRow) -> Result<Query, LocyError> {
    let source = PatternElement::Node(NodePattern {
        variable: Some(a.to_string()),
        labels: LabelExpr::Empty,
        properties: None,
        where_clause: None,
    });

    let rel = PatternElement::Relationship(RelationshipPattern {
        variable: None,
        types: LabelExpr::Disjunction(vec!["MERGED_WITH".to_string()]),
        direction: Direction::Outgoing,
        range: None,
        properties: None,
        where_clause: None,
    });

    let target = PatternElement::Node(NodePattern {
        variable: Some(b.to_string()),
        labels: LabelExpr::Empty,
        properties: None,
        where_clause: None,
    });

    let path = PathPattern {
        variable: None,
        elements: vec![source, rel, target],
        shortest_path_mode: None,
    };

    Ok(Query::Single(Statement {
        clauses: vec![Clause::Merge(MergeClause {
            pattern: Pattern { paths: vec![path] },
            on_match: vec![],
            on_create: vec![],
        })],
    }))
}

fn node_spec_to_pattern(spec: &DeriveNodeSpec, bindings: &FactRow) -> NodePattern {
    let variable = Some(spec.variable.clone());
    let labels = LabelExpr::from_conjunction(spec.labels.iter().cloned());

    let properties = if spec.is_new {
        let skolem_id = generate_skolem_id(&spec.variable, bindings);
        let mut props = HashMap::new();
        props.insert("_skolem_id".to_string(), Value::String(skolem_id));
        Some(Expr::Map(
            props
                .into_iter()
                .map(|(k, v)| (k, value_to_expr(&v)))
                .collect(),
        ))
    } else {
        spec.properties.clone()
    };

    NodePattern {
        variable,
        labels,
        properties,
        where_clause: None,
    }
}

/// Generate a deterministic Skolem ID for a NEW node based on its variable name and bindings.
pub fn generate_skolem_id(var_name: &str, bindings: &FactRow) -> String {
    use std::collections::BTreeMap;
    let sorted: BTreeMap<&String, &Value> = bindings.iter().collect();
    let mut parts = vec![var_name.to_string()];
    for (k, v) in &sorted {
        parts.push(format!("{}={}", k, value_to_string(v)));
    }
    parts.join("::")
}

/// Render one binding for the Skolem id.
///
/// The scalar arms keep their plain spelling because the id is persisted and
/// human-legible. Everything else — in practice a `Value::Node`, which
/// `enrich_vids_with_nodes` substitutes for the vid column before this runs —
/// goes through the canonical rendering.
///
/// That catch-all was `format!("{v:?}")`, which put `Node.properties` through
/// `HashMap` iteration order. Since `RandomState` is seeded per map instance,
/// a five-property node produced a different "deterministic" id on each call
/// *within one process* (#252).
fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        other => other.canonical_string(),
    }
}

pub(crate) fn value_to_expr(v: &Value) -> Expr {
    match v {
        Value::Null => Expr::Literal(CypherLiteral::Null),
        Value::Bool(b) => Expr::Literal(CypherLiteral::Bool(*b)),
        Value::Int(i) => Expr::Literal(CypherLiteral::Integer(*i)),
        Value::Float(f) => Expr::Literal(CypherLiteral::Float(*f)),
        Value::String(s) => Expr::Literal(CypherLiteral::String(s.clone())),
        // Same defect as `value_to_string` above, and with a longer reach: this
        // literal is spliced into a CREATE, so a `Debug` rendering of a map-
        // bearing value was *persisted* as a property.
        other => Expr::Literal(CypherLiteral::String(other.canonical_string())),
    }
}

pub(crate) fn combine_where_conditions(conditions: &[Expr]) -> Option<Expr> {
    if conditions.is_empty() {
        return None;
    }
    let mut combined = conditions[0].clone();
    for cond in &conditions[1..] {
        combined = Expr::BinaryOp {
            left: Box::new(combined),
            op: BinaryOp::And,
            right: Box::new(cond.clone()),
        };
    }
    Some(combined)
}

pub(crate) fn expr_references_var(expr: &Expr, var_name: &str) -> bool {
    match expr {
        Expr::Variable(name) => name == var_name,
        Expr::Property(base, _) => expr_references_var(base, var_name),
        Expr::BinaryOp { left, right, .. } => {
            expr_references_var(left, var_name) || expr_references_var(right, var_name)
        }
        Expr::UnaryOp { expr: inner, .. } => expr_references_var(inner, var_name),
        Expr::FunctionCall { args, .. } => args.iter().any(|a| expr_references_var(a, var_name)),
        Expr::List(items) => items.iter().any(|e| expr_references_var(e, var_name)),
        Expr::Map(entries) => entries
            .iter()
            .any(|(_, e)| expr_references_var(e, var_name)),
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => {
            case_expr
                .as_ref()
                .is_some_and(|e| expr_references_var(e, var_name))
                || when_then.iter().any(|(w, t)| {
                    expr_references_var(w, var_name) || expr_references_var(t, var_name)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|e| expr_references_var(e, var_name))
        }
        Expr::In { expr: e, list } => {
            expr_references_var(e, var_name) || expr_references_var(list, var_name)
        }
        Expr::ArrayIndex { array, index } => {
            expr_references_var(array, var_name) || expr_references_var(index, var_name)
        }
        _ => false,
    }
}
