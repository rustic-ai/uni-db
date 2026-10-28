// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Abductive reasoning (ABDUCE) via `LocyExecutionContext`.
//!
//! Ported from `uni-locy/src/orchestrator/abduce.rs`. Uses `LocyExecutionContext`
//! for L0 fork/restore, mutations, and strata re-evaluation. Three-phase pipeline:
//! Phase 1: Build derivation tree via EXPLAIN.
//! Phase 2: Generate candidate modifications from graph evidence at every tree level.
//! Phase 3: Validate each candidate via ASSUME (fork L0 + mutate + re-eval + restore).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use uni_common::value::EntityRef;
use uni_common::{Eid, Value, Vid};
use uni_cypher::ast::{
    BinaryOp, Clause, CypherLiteral, DeleteClause, Direction, Expr, LabelExpr, MatchClause,
    NodePattern, PathPattern, Pattern, PatternElement, Query, RelationshipPattern, SetClause,
    SetItem, Statement,
};
use uni_cypher::locy_ast::AbduceQuery;
use uni_locy::result::{AbductionResult, Modification, ValidatedModification};
use uni_locy::types::CompiledRule;
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_delta::RowStore;

use super::locy_ast_builder::value_to_expr;
use super::locy_eval::eval_condition;
use super::locy_explain::explain_rule;
use super::locy_sequence::SequencePrograms;
use super::locy_traits::{LocyExecutionContext, StrataBaseline};

/// Evaluate an ABDUCE query using a three-phase pipeline.
pub async fn evaluate_abduce(
    query: &AbduceQuery,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    derived_store: &RowStore,
    stats: &mut LocyStats,
) -> Result<AbductionResult, LocyError> {
    let validation = SequencePrograms {
        parent: program,
        body: None,
    };
    evaluate_abduce_with(
        query,
        program,
        &validation,
        ctx,
        config,
        derived_store,
        stats,
    )
    .await
}

/// [`evaluate_abduce`] for an ABDUCE whose candidates are validated against
/// `validation`: inside an `ASSUME` body, the parent's strata followed by the
/// body's own rules, the programs the body's facts come from. `program`
/// names the rules (its catalog holds the body's rules too).
///
/// # Errors
///
/// An unknown rule, or a failure building the derivation tree, applying a
/// candidate or re-deriving in its state.
pub async fn evaluate_abduce_with(
    query: &AbduceQuery,
    program: &CompiledProgram,
    validation: &SequencePrograms<'_>,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    derived_store: &RowStore,
    stats: &mut LocyStats,
) -> Result<AbductionResult, LocyError> {
    let rule_name = query.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::AbductionError {
            message: format!("rule '{}' not found for ABDUCE", rule_name),
        })?;

    // Phase 1: Build derivation tree
    let explain_query = uni_cypher::locy_ast::ExplainRule {
        rule_name: query.rule_name.clone(),
        where_expr: query.where_expr.clone(),
        return_clause: None,
    };
    // Without provenance, so the tree is built from the published facts with
    // its children (Mode B). Recorded provenance (Mode A) has no children, and
    // the causes ABDUCE edits are in the children: with a tracker present (any
    // program with an EXPLAIN or a probabilistic FOLD) ABDUCE saw only the
    // root and could propose nothing that changes the answer.
    //
    // Built over a copy of the program whose rule bodies name every anonymous
    // node and relationship (see `name_anonymous_elements`): a derivation's
    // evidence row binds only named elements, and a hop is proposed only when
    // its edge can be identified, so a body such as
    // `(l:Lot)-[:OBSERVED]->(:Feature)-[:ON]->(t:Tool)` otherwise proposed
    // no edge at all.
    let (named_program, anonymous) = name_anonymous_elements(program);
    let derivation_tree = explain_rule(
        &explain_query,
        &named_program,
        ctx,
        config,
        derived_store,
        stats,
        None,
        None,
    )
    .await?;

    // Phase 2: Generate candidate modifications from tree
    let mut candidates: Vec<Modification> = if query.negated {
        let named_rule = named_program.rule_catalog.get(&rule_name).unwrap_or(rule);
        extract_removal_candidates(&derivation_tree, named_rule, &named_program, &anonymous)
    } else {
        concrete_additions(
            &extract_addition_candidates(rule),
            query.where_expr.as_ref(),
            ctx,
            config.max_abduce_candidates,
        )
        .await?
    };
    candidates.truncate(config.max_abduce_candidates);

    // Phase 3: Validate each candidate via ASSUME
    //
    // Every candidate is applied to the same state, the current one, so each
    // re-evaluation reuses that state's facts for the strata its modification
    // cannot affect (see `locy_assume::re_evaluate_hypothetical`). Taken once,
    // before the first candidate's mutation; each candidate's fork is
    // restored before the next, so the state it describes stays current.
    let baseline = ctx.reuse_baseline();
    let mut validated = Vec::new();
    for candidate in candidates {
        let cost = compute_cost(&candidate);
        let is_valid = validate_modification(
            &candidate,
            query.negated,
            &rule_name,
            query.where_expr.as_ref(),
            validation,
            ctx,
            config,
            baseline.as_ref(),
            stats,
        )
        .await?;

        validated.push(ValidatedModification {
            modification: candidate,
            validated: is_valid,
            cost,
        });
        // A candidate state cut short (`allow_partial`) validates nothing
        // further: the program drops this ABDUCE's result and reports the cut.
        if ctx.is_cut_short() {
            break;
        }
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

/// A copy of `program` in which every anonymous node and relationship of a
/// rule body's top-level paths carries a generated variable, and the set of
/// generated names.
///
/// Naming an element does not change which rows a pattern matches, only which
/// columns its rows carry; the copy is used solely to build ABDUCE's
/// derivation tree, never to evaluate. A generated name is reported back as
/// anonymous (an empty variable, see [`anonymize`]) and is never a property
/// edit candidate. Each name is checked against the clause's whole text, so it
/// cannot capture a variable the body already uses.
fn name_anonymous_elements(program: &CompiledProgram) -> (CompiledProgram, Vec<String>) {
    let mut named = program.clone();
    let mut generated = Vec::new();
    for rule in named.rule_catalog.values_mut() {
        for clause in &mut rule.clauses {
            let text = format!("{clause:?}");
            let mut next = 0usize;
            let mut fresh = || loop {
                let name = format!("abduce_anon_{next}");
                next += 1;
                if !text.contains(&name) {
                    break name;
                }
            };
            for path in &mut clause.match_pattern.paths {
                for element in &mut path.elements {
                    let variable = match element {
                        PatternElement::Node(node) => &mut node.variable,
                        PatternElement::Relationship(rel) => &mut rel.variable,
                        _ => continue,
                    };
                    if variable.as_deref().is_none_or(str::is_empty) {
                        let name = fresh();
                        generated.push(name.clone());
                        *variable = Some(name);
                    }
                }
            }
        }
    }
    (named, generated)
}

/// `var`, or the empty (anonymous) variable when it is a name
/// [`name_anonymous_elements`] generated.
fn anonymize(var: String, anonymous: &[String]) -> String {
    if anonymous.contains(&var) {
        String::new()
    } else {
        var
    }
}

/// Phase 2 (ABDUCE NOT): Extract removal/change candidates from the derivation tree.
fn extract_removal_candidates(
    tree: &uni_locy::DerivationNode,
    rule: &CompiledRule,
    program: &CompiledProgram,
    anonymous: &[String],
) -> Vec<Modification> {
    let mut candidates = Vec::new();
    collect_evidence_candidates(tree, rule, program, anonymous, &mut candidates);
    // Two evidence nodes can propose the same edit (one per derivation that reads the
    // same entity); keep the first, so duplicates spend neither the candidate
    // budget nor a validation run.
    let mut distinct: Vec<Modification> = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if !distinct.contains(&candidate) {
            distinct.push(candidate);
        }
    }
    distinct
}

/// Recursively collect candidates from every graph-backed derivation node.
///
/// A rule with IS dependencies still reads its own MATCH edges. Its children
/// explain referenced facts, not those edges, so restricting collection to
/// leaves loses parent-rule edits (including joins feeding a FOLD).
///
/// Each evidence node carries a `rule` field naming the rule that produced it —
/// this may differ from the top-level `rule` parameter when IS-ref children
/// come from a different rule (e.g. a `scored_signal` leaf inside a
/// `threat_level` derivation).  We always look up the effective rule from
/// `program` so that `clause.match_pattern` corresponds to the correct rule.
fn collect_evidence_candidates(
    node: &uni_locy::DerivationNode,
    rule: &CompiledRule,
    program: &CompiledProgram,
    anonymous: &[String],
    candidates: &mut Vec<Modification>,
) {
    if node.graph_fact.is_some() {
        // Use the node's own rule when available; fall back to the caller's rule.
        let effective_rule: &CompiledRule = program.rule_catalog.get(&node.rule).unwrap_or(rule);

        if node.clause_index < effective_rule.clauses.len() {
            let clause = &effective_rule.clauses[node.clause_index];
            for element in &clause.match_pattern.paths {
                extract_edge_candidates(element, &node.bindings, anonymous, candidates);
            }
        }

        // A numeric property of a bound entity is a candidate edit, applied
        // as `SET var.prop` to that one entity. A derivation binds nodes and
        // edges as entities (`Value::Node` / `Value::Edge`, or the map form
        // carrying `_vid` / `_eid`). A bare scalar binding (a generator
        // output, `KEY e.w AS w`) is not an entity: `SET w.w` cannot be
        // applied, so it is not a candidate.
        //
        // Only the user's variables are read. The evidence row also carries
        // the planner's exploded helper columns (`a._all_props`, ...), and
        // reading those proposed every property a second time under a
        // variable no rule body names.
        let mut entity_bindings: Vec<(&String, &Value)> = node
            .bindings
            .iter()
            .filter(|(key, _)| !key.contains('.') && !anonymous.contains(key))
            .collect();
        entity_bindings.sort_by(|a, b| a.0.cmp(b.0));
        for (key, value) in entity_bindings {
            let (vid, eid) = match value.entity_ref() {
                Some(EntityRef::Vertex(vid)) => (Some(vid), None),
                Some(EntityRef::Edge(eid)) => (None, Some(eid)),
                None => continue,
            };
            for (prop_name, prop_val) in entity_properties(value) {
                if uni_common::core::schema::is_reserved_property_name(prop_name)
                    || prop_val.as_f64().is_none()
                {
                    continue;
                }
                candidates.push(Modification::ChangeProperty {
                    element_var: key.clone(),
                    property: prop_name.clone(),
                    old_value: Box::new(prop_val.clone()),
                    new_value: Box::new(zero_like(prop_val)),
                    vid,
                    eid,
                });
            }
        }
    }

    for child in &node.children {
        collect_evidence_candidates(child, rule, program, anonymous, candidates);
    }
}

/// The user properties of the entity `value` denotes, sorted by name.
///
/// The map form keeps them under `_all_props` (and, on some paths, also
/// inline); the native forms keep them in `properties`.
fn entity_properties(value: &Value) -> BTreeMap<&String, &Value> {
    let mut props = BTreeMap::new();
    match value {
        Value::Node(n) => props.extend(&n.properties),
        Value::Edge(e) => props.extend(&e.properties),
        Value::Map(m) => {
            if let Some(Value::Map(all)) = m.get("_all_props") {
                props.extend(all);
            }
            props.extend(
                m.iter()
                    .filter(|(k, _)| !uni_common::core::schema::is_reserved_property_name(k)),
            );
        }
        _ => {}
    }
    props
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
///
/// Each candidate names the one edge this derivation read: by its `eid` when
/// the body binds the relationship, else by the vids of its endpoints. A hop
/// whose edge cannot be identified either way is not proposed — removing "an
/// edge of this type" would remove every edge of the type, so validation
/// would accept an edit far larger than the one reported.
fn extract_edge_candidates(
    path: &PathPattern,
    bindings: &FactRow,
    anonymous: &[String],
    candidates: &mut Vec<Modification>,
) {
    /// A hop whose target node has not been reached yet.
    struct PendingHop {
        source_var: String,
        edge_var: String,
        edge_type: String,
        direction: Direction,
    }

    let vid_of = |var: &str| -> Option<Vid> {
        if var.is_empty() {
            return None;
        }
        bindings.get(var).and_then(Value::entity_vid)
    };

    let mut source_var = String::new();
    let mut pending: Option<PendingHop> = None;
    for element in &path.elements {
        match element {
            PatternElement::Node(node) => {
                let var = node.variable.clone().unwrap_or_default();
                if let Some(hop) = pending.take() {
                    let eid: Option<Eid> = bindings
                        .get(&hop.edge_var)
                        .filter(|_| !hop.edge_var.is_empty())
                        .and_then(Value::entity_eid);
                    // Name the variables in the edge's own direction, so the
                    // source is the node the edge leaves.
                    let (src, tgt) = match hop.direction {
                        Direction::Incoming => (var.clone(), hop.source_var),
                        _ => (hop.source_var, var.clone()),
                    };
                    let (source_vid, target_vid) = (vid_of(&src), vid_of(&tgt));
                    let identified = eid.is_some()
                        || (source_vid.is_some()
                            && target_vid.is_some()
                            && hop.direction != Direction::Both);
                    // The type of the edge the derivation read, when the
                    // body binds it: under `[e:E|G]` the first alternative
                    // named another type, so the removal matched no edge
                    // and was never validated.
                    let edge_type = match bindings.get(&hop.edge_var) {
                        Some(Value::Edge(edge)) if !hop.edge_var.is_empty() => {
                            edge.edge_type.clone()
                        }
                        _ => hop.edge_type,
                    };
                    if identified {
                        candidates.push(Modification::RemoveEdge {
                            source_var: anonymize(src, anonymous),
                            target_var: anonymize(tgt, anonymous),
                            edge_var: anonymize(hop.edge_var, anonymous),
                            edge_type,
                            match_properties: HashMap::new(),
                            eid,
                            source_vid,
                            target_vid,
                        });
                    }
                }
                source_var = var;
            }
            PatternElement::Relationship(rel) => {
                pending = Some(PendingHop {
                    source_var: source_var.clone(),
                    edge_var: rel.variable.clone().unwrap_or_default(),
                    edge_type: rel.types.first().cloned().unwrap_or_default(),
                    direction: rel.direction.clone(),
                });
            }
            _ => {}
        }
    }
}

/// One relationship of a rule body, as a positive ABDUCE edge to add: the
/// endpoint variables in the edge's own direction, and their labels.
#[derive(Debug, Clone)]
struct AdditionHop {
    source_var: String,
    source_labels: LabelExpr,
    target_var: String,
    target_labels: LabelExpr,
    edge_type: String,
}

/// Phase 2 (positive ABDUCE): the relationships of the rule's patterns, each
/// a kind of edge whose addition could derive the goal.
fn extract_addition_candidates(rule: &CompiledRule) -> Vec<AdditionHop> {
    /// A relationship whose target node has not been reached yet.
    struct PendingHop {
        source_var: String,
        source_labels: LabelExpr,
        edge_type: String,
        direction: Direction,
    }

    let mut hops = Vec::new();
    for clause in &rule.clauses {
        for path in &clause.match_pattern.paths {
            let mut source: (String, LabelExpr) = (String::new(), LabelExpr::Empty);
            let mut pending: Option<PendingHop> = None;
            for element in &path.elements {
                match element {
                    PatternElement::Node(node) => {
                        let here = (
                            node.variable.clone().unwrap_or_default(),
                            node.labels.clone(),
                        );
                        if let Some(hop) = pending.take() {
                            // Name the endpoints in the edge's own direction,
                            // so the source is the node the edge leaves.
                            let ((sv, sl), (tv, tl)) = match hop.direction {
                                Direction::Incoming => {
                                    (here.clone(), (hop.source_var, hop.source_labels))
                                }
                                _ => ((hop.source_var, hop.source_labels), here.clone()),
                            };
                            hops.push(AdditionHop {
                                source_var: sv,
                                source_labels: sl,
                                target_var: tv,
                                target_labels: tl,
                                edge_type: hop.edge_type,
                            });
                        }
                        source = here;
                    }
                    PatternElement::Relationship(rel) => {
                        pending = Some(PendingHop {
                            source_var: source.0.clone(),
                            source_labels: source.1.clone(),
                            edge_type: rel.types.first().cloned().unwrap_or_default(),
                            direction: rel.direction.clone(),
                        });
                    }
                    _ => {}
                }
            }
        }
    }
    hops
}

/// Phase 2 (positive ABDUCE): concrete `AddEdge` candidates, one edge between
/// two existing nodes each, at most `budget` of them.
///
/// A candidate used to name only the rule's variables, and was validated by
/// `MATCH (a), (b) CREATE (a)-[:T]->(b)`: an edge between every pair of
/// nodes. Validation then judged a far larger edit than the one edge
/// reported, and accepted edits no single edge achieves (a two-hop rule
/// validated on one `AddEdge`), at a cost quadratic in the node count.
///
/// The endpoints of each hop are enumerated from the nodes carrying its
/// endpoint labels, in id order. When the goal's `WHERE` reads only the hop's
/// endpoint variables it filters the enumeration, so a goal such as
/// `a.name = 'A'` proposes edges out of `A` first; otherwise every labelled
/// pair is a candidate, up to the budget.
///
/// # Errors
///
/// A failure of the enumeration query.
async fn concrete_additions(
    hops: &[AdditionHop],
    goal: Option<&Expr>,
    ctx: &dyn LocyExecutionContext,
    budget: usize,
) -> Result<Vec<Modification>, LocyError> {
    let mut candidates = Vec::new();
    for hop in hops {
        if candidates.len() >= budget {
            break;
        }
        let remaining = budget - candidates.len();
        for (source_vid, target_vid) in enumerate_endpoints(hop, goal, ctx, remaining).await? {
            candidates.push(Modification::AddEdge {
                source_var: hop.source_var.clone(),
                target_var: hop.target_var.clone(),
                edge_type: hop.edge_type.clone(),
                properties: HashMap::new(),
                source_vid: Some(source_vid),
                target_vid: Some(target_vid),
            });
        }
    }
    Ok(candidates)
}

/// Up to `limit` `(source, target)` node pairs for `hop`, in id order.
async fn enumerate_endpoints(
    hop: &AdditionHop,
    goal: Option<&Expr>,
    ctx: &dyn LocyExecutionContext,
    limit: usize,
) -> Result<Vec<(Vid, Vid)>, LocyError> {
    let var = |v: &str, fallback: &str| {
        if v.is_empty() {
            fallback.to_string()
        } else {
            v.to_string()
        }
    };
    let src = var(&hop.source_var, "__abd_src");
    let mut tgt = var(&hop.target_var, "__abd_tgt");
    let self_loop = !hop.source_var.is_empty() && hop.source_var == hop.target_var;
    if !self_loop && tgt == src {
        tgt.push('_');
    }
    let node = |v: &str, labels: &LabelExpr| -> String {
        let names: Vec<String> = labels
            .names()
            .iter()
            .map(|l| format!("`{}`", l.replace('`', "``")))
            .collect();
        let labels = match labels {
            LabelExpr::Empty => String::new(),
            LabelExpr::Conjunction(_) => format!(":{}", names.join(":")),
            LabelExpr::Disjunction(_) => format!(":{}", names.join("|")),
        };
        format!("(`{}`{labels})", v.replace('`', "``"))
    };
    let pattern = if self_loop {
        node(&src, &hop.source_labels)
    } else {
        format!(
            "{}, {}",
            node(&src, &hop.source_labels),
            node(&tgt, &hop.target_labels)
        )
    };
    let quoted = |v: &str| format!("`{}`", v.replace('`', "``"));
    let cypher = format!(
        "MATCH {pattern} RETURN id({s}) AS __abd_s, id({t}) AS __abd_t \
         ORDER BY __abd_s, __abd_t LIMIT {limit}",
        s = quoted(&src),
        t = quoted(if self_loop { &src } else { &tgt }),
    );
    let mut ast = uni_cypher::parse(&cypher).map_err(|e| LocyError::AbductionError {
        message: format!("ABDUCE endpoint enumeration failed to parse: {e}"),
    })?;
    // The goal filters the enumeration only when every variable it reads is
    // one of this hop's endpoints.
    let mut endpoint_vars = vec![src.clone()];
    if !self_loop {
        endpoint_vars.push(tgt.clone());
    }
    let filter = goal.filter(|g| {
        let mut vars = Vec::new();
        expr_variables(g, &mut vars);
        !vars.is_empty() && vars.iter().all(|v| endpoint_vars.contains(v))
    });
    if let (Some(filter), Query::Single(stmt)) = (filter, &mut ast)
        && let Some(Clause::Match(m)) = stmt.clauses.first_mut()
    {
        m.where_clause = Some(filter.clone());
    }
    let rows = ctx.execute_cypher_read(ast).await?;
    let id_of = |row: &FactRow, col: &str| -> Option<Vid> {
        match row.get(col) {
            Some(Value::Int(i)) => u64::try_from(*i).ok().map(Vid::new),
            _ => None,
        }
    };
    Ok(rows
        .iter()
        .filter_map(|row| Some((id_of(row, "__abd_s")?, id_of(row, "__abd_t")?)))
        .collect())
}

/// Every variable `expr` reads, including ones a comprehension binds (which
/// only makes the caller more conservative).
fn expr_variables(expr: &Expr, out: &mut Vec<String>) {
    if let Expr::Variable(v) = expr {
        out.push(v.clone());
    }
    expr.for_each_child(&mut |child| expr_variables(child, out));
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
    validation: &SequencePrograms<'_>,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    baseline: Option<&StrataBaseline>,
    stats: &mut LocyStats,
) -> Result<bool, LocyError> {
    let mutation_query = modification_to_cypher(modification);
    let footprint = uni_locy::incremental::query_footprint(&mutation_query);

    // Fork L0 for hypothetical reasoning
    //
    // As in `locy_assume::evaluate_assume`, the fork/restore pair is not
    // exception-safe: an `Err` from the mutation or the re-evaluation below
    // returns without reaching `restore_l0()`. It is sound for the same
    // non-local reason -- `fork_l0` swaps only the per-evaluation
    // `NativeExecutionAdapter`'s own pointer to a deep clone, and that adapter
    // is dropped when the evaluation returns -- so the un-restored clone is
    // discarded rather than observed. See that function for the full argument
    // and the measurement.
    //
    // Coverage note: the reasoning was confirmed empirically at the ASSUME site.
    // The same could not be exercised here, because no ABDUCE failure between
    // fork and restore could be constructed on the transaction path (neither a
    // 1 ms timeout nor `max_iterations(1)` errors there). The invariant is
    // shared, not separately measured.
    ctx.fork_l0()
        .await
        .map_err(|e| LocyError::SavepointFailed {
            message: format!("ABDUCE fork L0 failed: {}", e),
        })?;

    // Execute the mutation
    ctx.execute_mutation(mutation_query, HashMap::new()).await?;
    stats.mutations_executed += 1;

    // Re-evaluate strata, reusing those the modification cannot affect.
    // Inside an ASSUME body the body's own rules are derived too, on top of
    // the parent's, as the body's facts are: validating against the
    // parent's strata alone found a body rule empty in every candidate state.
    let assume_store: RowStore = match validation.body {
        None => {
            super::locy_assume::re_evaluate_hypothetical(
                validation.parent,
                ctx,
                config,
                baseline,
                &footprint,
                stats,
            )
            .await?
            .rows
        }
        Some(_) => Arc::unwrap_or_clone(
            super::locy_sequence::derive_command_facts(
                validation,
                super::locy_sequence::SequenceScope::Hypothetical,
                ctx,
                config,
                baseline,
                &footprint,
                stats,
            )
            .await?
            .into_store(),
        ),
    };

    // Check if the conclusion still holds
    let facts = assume_store
        .get(rule_name)
        .map(|r| r.rows.clone())
        .unwrap_or_default();

    let matching: Vec<FactRow> = if let Some(where_expr) = where_expr {
        facts
            .into_iter()
            .map(|row| Ok(eval_condition(where_expr, &row, "ABDUCE WHERE")?.then_some(row)))
            .filter_map(Result::transpose)
            .collect::<Result<_, LocyError>>()?
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
            eid,
            source_vid,
            target_vid,
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

            // Identity first: the edge by its eid, or, for an anonymous
            // relationship, by its two endpoints. Without it the pattern
            // matched, and deleted, every edge of the type.
            let mut where_conditions: Vec<Expr> = Vec::new();
            if let Some(eid) = eid {
                where_conditions.push(id_equals(&edge_var_name, eid.as_u64()));
            }
            if let Some(vid) = source_vid {
                where_conditions.push(id_equals(&src_var, vid.as_u64()));
            }
            if let Some(vid) = target_vid {
                where_conditions.push(id_equals(&tgt_var, vid.as_u64()));
            }
            let mut constraints: Vec<_> = match_properties.iter().collect();
            constraints.sort_by(|a, b| a.0.cmp(b.0));
            where_conditions.extend(constraints.into_iter().map(|(k, v)| Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable(edge_var_name.clone())),
                    k.clone(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(value_to_expr(v)),
            }));
            let where_clause = conjunction(where_conditions);

            let types = if edge_type.is_empty() {
                LabelExpr::Empty
            } else {
                LabelExpr::Disjunction(vec![edge_type.clone()])
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
                        types,
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
            vid,
            eid,
            ..
        } => {
            // The one entity the derivation read: `MATCH (n) WHERE id(n) = …`
            // for a node, `MATCH ()-[e]-() WHERE id(e) = …` for an edge.
            let (element, id) = match (vid, eid) {
                (_, Some(eid)) => (
                    vec![
                        anonymous_node(),
                        PatternElement::Relationship(RelationshipPattern {
                            variable: Some(element_var.clone()),
                            types: LabelExpr::Empty,
                            direction: Direction::Both,
                            range: None,
                            properties: None,
                            where_clause: None,
                        }),
                        anonymous_node(),
                    ],
                    Some(eid.as_u64()),
                ),
                (vid, None) => (
                    vec![PatternElement::Node(NodePattern {
                        variable: Some(element_var.clone()),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    })],
                    vid.map(|v| v.as_u64()),
                ),
            };
            let path = PathPattern {
                variable: None,
                elements: element,
                shortest_path_mode: None,
            };

            Query::Single(Statement {
                clauses: vec![
                    Clause::Match(MatchClause {
                        optional: false,
                        pattern: Pattern { paths: vec![path] },
                        where_clause: id.map(|id| id_equals(element_var, id)),
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
            source_vid,
            target_vid,
        } => {
            // The one edge between the two nodes `source_vid` and
            // `target_vid` name: `MATCH (s), (t) WHERE id(s) = … AND
            // id(t) = … CREATE (s)-[:T]->(t)`. Every candidate
            // `concrete_additions` proposes names both; an endpoint without
            // an id matches no node, so the edit cannot widen to every pair.
            let src_var = if source_var.is_empty() {
                "src".to_string()
            } else {
                source_var.clone()
            };
            let mut tgt_var = if target_var.is_empty() {
                "tgt".to_string()
            } else {
                target_var.clone()
            };
            let self_loop = source_vid.is_some() && source_vid == target_vid;
            if self_loop {
                tgt_var = src_var.clone();
            } else if tgt_var == src_var {
                tgt_var.push('_');
            }
            let var_node = |v: &str| {
                PatternElement::Node(NodePattern {
                    variable: Some(v.to_string()),
                    labels: LabelExpr::Empty,
                    properties: None,
                    where_clause: None,
                })
            };
            let single = |e: PatternElement| PathPattern {
                variable: None,
                elements: vec![e],
                shortest_path_mode: None,
            };
            let mut match_paths = vec![single(var_node(&src_var))];
            if !self_loop {
                match_paths.push(single(var_node(&tgt_var)));
            }
            // An absent id saturates to `i64::MAX`, which names no stored
            // entity (see `id_equals`).
            let id_of = |vid: &Option<Vid>| vid.map_or(u64::MAX, |v| v.as_u64());
            let mut conditions = vec![id_equals(&src_var, id_of(source_vid))];
            if !self_loop {
                conditions.push(id_equals(&tgt_var, id_of(target_vid)));
            }

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
                    var_node(&src_var),
                    PatternElement::Relationship(RelationshipPattern {
                        variable: None,
                        types: LabelExpr::Disjunction(vec![edge_type.clone()]),
                        direction: Direction::Outgoing,
                        range: None,
                        properties: edge_props,
                        where_clause: None,
                    }),
                    var_node(&tgt_var),
                ],
                shortest_path_mode: None,
            };

            Query::Single(Statement {
                clauses: vec![
                    Clause::Match(MatchClause {
                        optional: false,
                        pattern: Pattern { paths: match_paths },
                        where_clause: conjunction(conditions),
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

/// `id(var) = id`.
fn id_equals(var: &str, id: u64) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::FunctionCall {
            name: "id".to_string(),
            args: vec![Expr::Variable(var.to_string())],
            distinct: false,
            window_spec: None,
        }),
        op: BinaryOp::Eq,
        // Ids are dense from 0; one past `i64::MAX` is not a stored entity,
        // so saturating cannot name a different one.
        right: Box::new(Expr::Literal(CypherLiteral::Integer(
            i64::try_from(id).unwrap_or(i64::MAX),
        ))),
    }
}

/// The conjunction of `conditions`, or `None` when there are none.
fn conjunction(conditions: Vec<Expr>) -> Option<Expr> {
    conditions.into_iter().reduce(|a, b| Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOp::And,
        right: Box::new(b),
    })
}

/// `()`.
fn anonymous_node() -> PatternElement {
    PatternElement::Node(NodePattern {
        variable: None,
        labels: LabelExpr::Empty,
        properties: None,
        where_clause: None,
    })
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
            eid: None,
            source_vid: Some(Vid::new(1)),
            target_vid: Some(Vid::new(2)),
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
            eid: None,
            source_vid: Some(Vid::new(1)),
            target_vid: Some(Vid::new(2)),
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
            eid: None,
            source_vid: Some(Vid::new(1)),
            target_vid: Some(Vid::new(2)),
        };
        let query = modification_to_cypher(&modification);
        let (src, edge, tgt) = extract_pattern_vars(&query);
        assert_eq!(src, "a");
        assert_eq!(edge, "rel");
        assert_eq!(tgt, "b");
    }

    /// The WHERE of a generated mutation, rendered for comparison.
    fn where_of(query: &Query) -> String {
        let Query::Single(stmt) = query else {
            panic!("expected single statement");
        };
        let Clause::Match(m) = &stmt.clauses[0] else {
            panic!("expected MATCH as first clause");
        };
        format!("{:?}", m.where_clause)
    }

    /// A removal is pinned to the edge's identity, so it deletes that edge
    /// and no other edge of the type.
    #[test]
    fn remove_edge_is_pinned_to_identity() {
        let modification = Modification::RemoveEdge {
            source_var: "a".to_string(),
            target_var: "b".to_string(),
            edge_var: "e".to_string(),
            edge_type: "R".to_string(),
            match_properties: HashMap::new(),
            eid: Some(Eid::new(42)),
            source_vid: Some(Vid::new(1)),
            target_vid: Some(Vid::new(2)),
        };
        let rendered = where_of(&modification_to_cypher(&modification));
        for id in ["Integer(42)", "Integer(1)", "Integer(2)"] {
            assert!(rendered.contains(id), "{id} missing from {rendered}");
        }
    }

    /// A property change is pinned to the entity's identity: a node by vid,
    /// an edge by eid through a relationship pattern.
    #[test]
    fn change_property_is_pinned_to_identity() {
        let node = Modification::ChangeProperty {
            element_var: "n".to_string(),
            property: "k".to_string(),
            old_value: Box::new(Value::Int(5)),
            new_value: Box::new(Value::Int(0)),
            vid: Some(Vid::new(9)),
            eid: None,
        };
        assert!(where_of(&modification_to_cypher(&node)).contains("Integer(9)"));

        let edge = Modification::ChangeProperty {
            element_var: "e".to_string(),
            property: "w".to_string(),
            old_value: Box::new(Value::Int(5)),
            new_value: Box::new(Value::Int(0)),
            vid: None,
            eid: Some(Eid::new(11)),
        };
        let query = modification_to_cypher(&edge);
        assert!(where_of(&query).contains("Integer(11)"));
        let Query::Single(stmt) = &query else {
            unreachable!()
        };
        let Clause::Match(m) = &stmt.clauses[0] else {
            unreachable!()
        };
        assert!(
            matches!(
                &m.pattern.paths[0].elements[1],
                PatternElement::Relationship(r) if r.variable.as_deref() == Some("e")
            ),
            "an edge property is set through a relationship pattern"
        );
    }
}
