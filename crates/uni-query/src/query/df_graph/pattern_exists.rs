// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Vectorized EXISTS evaluation for pattern predicates in WHERE clauses.
//!
//! When a WHERE clause contains a bare pattern predicate like
//! `(m)-[:SENT_BY]->(:Participant {name: $ename})`, the parser wraps it in
//! `Expr::Exists { from_pattern_predicate: true }`. Instead of running a full
//! plan-and-execute cycle per row (the generic `ExistsExecExpr` path), this
//! module evaluates the pattern using batch CSR neighbor lookups — the same
//! approach used by `PatternComprehensionExecExpr`.
//!
//! Returns a `BooleanArray` where each element indicates whether at least one
//! matching path exists for that row's anchor node.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Schema};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use uni_common::core::id::Vid;
use uni_common::core::schema::Schema as UniSchema;
use uni_common::value::Value;
use uni_cypher::ast::{Expr as CypherExpr, Pattern, PatternElement, Query};
use uni_store::storage::direction::Direction;

use super::GraphExecutionContext;
use super::pattern_comprehension::TraversalStep;
use crate::query::df_graph::common::{block_on_scoped, column_as_vid_array};

/// A property equality predicate extracted from a node pattern's property map.
///
/// For `(:Label {name: $param})`, this stores `property_name = "name"` and
/// `param_name = Some("param")`. For `(:Label {status: 'active'})`, it stores
/// `literal_value = Some(Value::String("active"))`.
#[derive(Debug, Clone)]
pub struct PropertyPredicate {
    /// Property name to check on the target node.
    pub property_name: String,
    /// Parameter name to resolve at evaluate time, if the value came from `$param`.
    pub param_name: Option<String>,
    /// Literal value known at compile time.
    pub literal_value: Option<Value>,
}

/// Vectorized EXISTS expression for simple pattern predicates.
///
/// Evaluates `(anchor)-[:EDGE]->(:Label {props})` patterns using batch CSR
/// lookups instead of per-row subquery execution.
#[derive(Debug)]
pub struct PatternExistsExecExpr {
    /// Shared graph context for CSR lookups and property materialization.
    graph_ctx: Arc<GraphExecutionContext>,
    /// Column name for anchor VIDs (e.g., `"m._vid"`).
    anchor_column: String,
    /// Traversal steps describing each hop in the pattern.
    traversal_steps: Vec<TraversalStep>,
    /// Schema of the outer input batch.
    input_schema: Arc<Schema>,
    /// Property predicates per traversal step (indexed by step).
    target_property_predicates: Vec<Vec<PropertyPredicate>>,
    /// Per-step bound target VID column name, if the target variable is also in
    /// the outer scope (e.g., `MATCH (n),(m) WHERE (n)-[:R]->(m)` — `m` is bound).
    /// When `Some`, the step checks adjacency to that specific VID instead of
    /// checking for the existence of *any* neighbor.
    bound_target_columns: Vec<Option<String>>,
    /// Parameters from the outer query for resolving `$param` references.
    params: HashMap<String, Value>,
}

impl Clone for PatternExistsExecExpr {
    fn clone(&self) -> Self {
        Self {
            graph_ctx: self.graph_ctx.clone(),
            anchor_column: self.anchor_column.clone(),
            traversal_steps: self.traversal_steps.clone(),
            input_schema: self.input_schema.clone(),
            target_property_predicates: self.target_property_predicates.clone(),
            bound_target_columns: self.bound_target_columns.clone(),
            params: self.params.clone(),
        }
    }
}

impl PatternExistsExecExpr {
    /// Creates a new vectorized pattern exists expression.
    pub fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        anchor_column: String,
        traversal_steps: Vec<TraversalStep>,
        input_schema: Arc<Schema>,
        target_property_predicates: Vec<Vec<PropertyPredicate>>,
        bound_target_columns: Vec<Option<String>>,
        params: HashMap<String, Value>,
    ) -> Self {
        Self {
            graph_ctx,
            anchor_column,
            traversal_steps,
            input_schema,
            target_property_predicates,
            bound_target_columns,
            params,
        }
    }

    /// Resolves a `PropertyPredicate` to a concrete `Value` at evaluate time.
    ///
    /// Fails closed. This used to return `Option` and the caller dropped a
    /// `None` from the predicate list, which meant an unsupplied parameter
    /// silently removed the property check: `WHERE (a)-[:R]->(:B {name: $p})`
    /// degenerated to "any neighbour labelled `B`" and matched, while under
    /// `NOT` it excluded rows that should have been kept. Both are silent wrong
    /// answers, and there is no residual re-check anywhere downstream that
    /// would re-apply the dropped predicate.
    ///
    /// An unbound parameter is a hard error everywhere else in this crate (see
    /// `common.rs::resolve_value_expr` and `unwind.rs`); this site was the
    /// outlier. Note that a parameter bound to an explicit `null` is *supplied*
    /// and still resolves to `Ok(Value::Null)` — "absent" and "null" must stay
    /// distinguishable.
    fn resolve_predicate_value(&self, pred: &PropertyPredicate) -> DFResult<Value> {
        if let Some(ref val) = pred.literal_value {
            Ok(val.clone())
        } else if let Some(ref param_name) = pred.param_name {
            self.params.get(param_name).cloned().ok_or_else(|| {
                DataFusionError::Execution(format!("Parameter '{}' not found", param_name))
            })
        } else {
            // Impossible by construction: `build_property_predicates` sets one
            // of the two arms or bails at plan time. Loud rather than silent.
            Err(DataFusionError::Execution(format!(
                "Pattern property predicate for '{}' has neither a literal nor a parameter",
                pred.property_name
            )))
        }
    }
}

impl Display for PatternExistsExecExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "PatternExists(anchor={}, steps={})",
            self.anchor_column,
            self.traversal_steps.len()
        )
    }
}

impl PartialEq for PatternExistsExecExpr {
    fn eq(&self, other: &Self) -> bool {
        self.anchor_column == other.anchor_column && Arc::ptr_eq(&self.graph_ctx, &other.graph_ctx)
    }
}

impl Eq for PatternExistsExecExpr {}

impl Hash for PatternExistsExecExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.anchor_column.hash(state);
        self.traversal_steps.len().hash(state);
    }
}

impl PartialEq<dyn Any> for PatternExistsExecExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl PhysicalExpr for PatternExistsExecExpr {
    fn data_type(&self, _input_schema: &Schema) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> DFResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let num_rows = batch.num_rows();

        // Step 1: Extract anchor VIDs from the input batch.
        let anchor_col = if let Some(col) = batch.column_by_name(&self.anchor_column) {
            col
        } else if let Some(var_name) = self.anchor_column.strip_suffix("._vid") {
            batch.column_by_name(var_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "PatternExists: anchor column '{}' not found in batch schema: {:?}",
                    self.anchor_column,
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                ))
            })?
        } else {
            return Err(DataFusionError::Execution(format!(
                "PatternExists: anchor column '{}' not found in batch schema: {:?}",
                self.anchor_column,
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
            )));
        };
        let anchor_vid_cow = column_as_vid_array(anchor_col.as_ref())?;
        let anchor_vids: &UInt64Array = &anchor_vid_cow;

        // Step 2: Warm CSR for all edge types in the traversal.
        for step in &self.traversal_steps {
            block_on_scoped(
                "CSR warming",
                self.graph_ctx
                    .ensure_adjacency_warmed(&step.edge_type_ids, step.direction),
            )?;
        }

        // Step 3: Evaluate pattern existence per row using batch CSR lookups.
        let mut result = vec![false; num_rows];
        // Rows whose bound target VID is NULL (e.g. an unmatched OPTIONAL MATCH):
        // the pattern references a null node, so its truth value is NULL, not
        // false or "any neighbor matches". Tracked here so the output emits NULL —
        // which excludes the row under both `(n)-[:R]->(m)` and `NOT (...)`, per
        // Cypher three-valued logic.
        let mut endpoint_null = vec![false; num_rows];
        let query_ctx = self.graph_ctx.query_context();

        // Build initial frontier: (row_index, vid) pairs for non-null anchors.
        let mut frontier: Vec<(u32, u64)> = Vec::with_capacity(num_rows);
        for (row_idx, vid_opt) in anchor_vids.iter().enumerate() {
            if let Some(vid_u64) = vid_opt {
                frontier.push((row_idx as u32, vid_u64));
            }
        }

        for (step_idx, step) in self.traversal_steps.iter().enumerate() {
            if frontier.is_empty() {
                break;
            }

            let is_last_step = step_idx == self.traversal_steps.len() - 1;
            let has_property_preds = step_idx < self.target_property_predicates.len()
                && !self.target_property_predicates[step_idx].is_empty();
            let is_undirected = step.direction == Direction::Both;

            // If the target variable is bound in the outer scope, extract its VIDs
            // so we check adjacency to a specific node rather than any neighbor.
            let bound_target_vids: Option<std::borrow::Cow<'_, UInt64Array>> =
                if let Some(Some(col_name)) = self.bound_target_columns.get(step_idx) {
                    let col = batch.column_by_name(col_name).or_else(|| {
                        col_name
                            .strip_suffix("._vid")
                            .and_then(|v| batch.column_by_name(v))
                    });
                    col.map(|c| column_as_vid_array(c.as_ref())).transpose()?
                } else {
                    None
                };

            // Resolve property predicate values for this step.
            // `collect::<DFResult<Vec<_>>>()` propagates instead of dropping:
            // a predicate that cannot be resolved must fail the query, never
            // silently vanish from the filter.
            let resolved_preds: Vec<(String, Value)> = if has_property_preds {
                self.target_property_predicates[step_idx]
                    .iter()
                    .map(|p| {
                        self.resolve_predicate_value(p)
                            .map(|v| (p.property_name.clone(), v))
                    })
                    .collect::<DFResult<Vec<_>>>()?
            } else {
                Vec::new()
            };

            // Expand neighbors for each frontier entry.
            let mut next_frontier: Vec<(u32, u64)> = Vec::new();

            // Helper closure: check if a target VID passes the label filter.
            // Resolves labels from the L0 chain then the persisted index, so
            // Lance-only vertices (e.g. on a fork) are matched correctly.
            let passes_label_filter = |target_vid: Vid| -> bool {
                if let Some(ref label_name) = step.target_label_name
                    && let Some(vertex_labels) =
                        self.graph_ctx.resolve_vertex_labels(target_vid, &query_ctx)
                    && !vertex_labels.contains(label_name)
                {
                    return false;
                }
                true
            };

            if !resolved_preds.is_empty() {
                // Property predicates present: collect candidates, batch-load, filter.
                let mut candidates: Vec<(u32, Vid)> = Vec::new();

                for &(row_idx, src_vid_u64) in &frontier {
                    if result[row_idx as usize] {
                        continue;
                    }
                    // NULL bound target -> pattern is NULL for this row; record
                    // and skip so it is never satisfied by an arbitrary neighbor.
                    if let Some(ref bound_vids) = bound_target_vids
                        && bound_vids.is_null(row_idx as usize)
                    {
                        endpoint_null[row_idx as usize] = true;
                        continue;
                    }
                    let vid = Vid::from(src_vid_u64);
                    let mut seen_eids = std::collections::HashSet::new();

                    for &edge_type in &step.edge_type_ids {
                        let neighbors =
                            self.graph_ctx.get_neighbors(vid, edge_type, step.direction);

                        for (target_vid, eid) in neighbors {
                            if is_undirected && !seen_eids.insert(eid.as_u64()) {
                                continue;
                            }
                            if !passes_label_filter(target_vid) {
                                continue;
                            }
                            // Bound target check: skip neighbors that aren't the expected VID.
                            if let Some(ref bound_vids) = bound_target_vids
                                && !bound_vids.is_null(row_idx as usize)
                                && target_vid.as_u64() != bound_vids.value(row_idx as usize)
                            {
                                continue;
                            }
                            candidates.push((row_idx, target_vid));
                        }
                    }
                }

                // Batch-load properties for all candidate target VIDs.
                if !candidates.is_empty() {
                    let unique_vids: Vec<Vid> = {
                        let mut v: Vec<Vid> = candidates.iter().map(|c| c.1).collect();
                        v.sort_unstable();
                        v.dedup();
                        v
                    };

                    let prop_names: Vec<&str> =
                        resolved_preds.iter().map(|(n, _)| n.as_str()).collect();

                    let props_map = block_on_scoped(
                        "Vertex prop load",
                        self.graph_ctx.property_manager().get_batch_vertex_props(
                            &unique_vids,
                            &prop_names,
                            Some(&query_ctx),
                        ),
                    )?;

                    for (row_idx, target_vid) in &candidates {
                        if result[*row_idx as usize] {
                            continue;
                        }

                        // `{k: v}` is `k = v` under Cypher equality: `1`
                        // matches `1.0`, NaN matches nothing, and a null on
                        // either side (a missing property included) is not a
                        // match. Structural `Value` equality got all three
                        // wrong.
                        let matches = resolved_preds.iter().all(|(name, expected)| {
                            props_map
                                .get(target_vid)
                                .and_then(|props| props.get(name))
                                .is_some_and(|actual| {
                                    crate::query::expr_eval::cypher_eq(actual, expected)
                                        == Some(true)
                                })
                        });

                        if matches {
                            if is_last_step {
                                result[*row_idx as usize] = true;
                            } else {
                                next_frontier.push((*row_idx, target_vid.as_u64()));
                            }
                        }
                    }
                }
            } else {
                // No property predicates — CSR expansion with label + bound target filter.
                for &(row_idx, src_vid_u64) in &frontier {
                    if result[row_idx as usize] {
                        continue;
                    }
                    // NULL bound target -> pattern is NULL for this row; record
                    // and skip so it is never satisfied by an arbitrary neighbor.
                    if let Some(ref bv) = bound_target_vids
                        && bv.is_null(row_idx as usize)
                    {
                        endpoint_null[row_idx as usize] = true;
                        continue;
                    }
                    let vid = Vid::from(src_vid_u64);
                    let mut found = false;
                    let mut seen_eids = std::collections::HashSet::new();

                    // For bound targets, extract the expected VID for this row.
                    // (Null-bound rows were already skipped above.)
                    let expected_target: Option<u64> = bound_target_vids.as_ref().and_then(|bv| {
                        if bv.is_null(row_idx as usize) {
                            None
                        } else {
                            Some(bv.value(row_idx as usize))
                        }
                    });

                    'edge_types: for &edge_type in &step.edge_type_ids {
                        let neighbors =
                            self.graph_ctx.get_neighbors(vid, edge_type, step.direction);

                        for (target_vid, eid) in neighbors {
                            if is_undirected && !seen_eids.insert(eid.as_u64()) {
                                continue;
                            }
                            if !passes_label_filter(target_vid) {
                                continue;
                            }
                            // Bound target check.
                            if let Some(expected) = expected_target
                                && target_vid.as_u64() != expected
                            {
                                continue;
                            }

                            if is_last_step {
                                found = true;
                                break 'edge_types;
                            } else {
                                next_frontier.push((row_idx, target_vid.as_u64()));
                            }
                        }
                    }

                    if found {
                        result[row_idx as usize] = true;
                    }
                }
            }

            frontier = next_frontier;
        }

        // Step 4: Build BooleanArray from the result vector. Rows whose bound
        // target was NULL emit NULL (three-valued logic), not false, so a `NOT`
        // wrapper also excludes them.
        let bool_array: BooleanArray = result
            .iter()
            .enumerate()
            .map(|(i, &r)| if endpoint_null[i] { None } else { Some(r) })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(bool_array)))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PatternExists({})", self.anchor_column)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helper functions for extracting pattern structure from EXISTS queries
// ═══════════════════════════════════════════════════════════════════════════════

/// Extracts the `Pattern` from a pattern-predicate `Query`.
///
/// Pattern predicate queries are always `Query::Single` with exactly one
/// `Clause::Match` containing the pattern. Returns `Err` if the structure
/// doesn't match (triggers fallback to `ExistsExecExpr`).
pub fn extract_pattern_from_exists_query(query: &Query) -> anyhow::Result<Pattern> {
    match query {
        Query::Single(stmt) if stmt.clauses.len() == 1 => {
            if let uni_cypher::ast::Clause::Match(m) = &stmt.clauses[0] {
                // Reject variable-length paths — they require full subquery execution.
                for path in &m.pattern.paths {
                    for elem in &path.elements {
                        if let PatternElement::Relationship(rel) = elem
                            && rel.range.is_some()
                        {
                            anyhow::bail!(
                                "Variable-length paths in pattern predicates require subquery evaluation"
                            );
                        }
                    }
                }
                Ok(m.pattern.clone())
            } else {
                anyhow::bail!("Expected Match clause in pattern predicate EXISTS query")
            }
        }
        _ => anyhow::bail!("Pattern predicate EXISTS query has unexpected structure"),
    }
}

/// Extracts property predicates from target nodes in the pattern.
///
/// Walks the pattern elements and, for each target node in each traversal step,
/// extracts property map entries as `PropertyPredicate` values. Only literal
/// values and `$param` references are supported; anything else triggers
/// fallback.
///
/// The returned vec is indexed by step — `result[i]` contains predicates for
/// step `i`'s target node.
pub fn extract_target_property_predicates(
    pattern: &Pattern,
    steps: &[TraversalStep],
    _uni_schema: &UniSchema,
) -> anyhow::Result<Vec<Vec<PropertyPredicate>>> {
    if pattern.paths.is_empty() {
        return Ok(vec![Vec::new(); steps.len()]);
    }

    let elements = &pattern.paths[0].elements;

    // Find the anchor index (first node whose variable is in an outer scope).
    // We walk forward from the anchor: elements[anchor+1] = rel, elements[anchor+2] = target, etc.
    let anchor_idx = elements
        .iter()
        .position(|e| matches!(e, PatternElement::Node(_)))
        .unwrap_or(0);

    let mut result = Vec::with_capacity(steps.len());

    for step_i in 0..steps.len() {
        // Target node is at anchor_idx + 2*(step_i+1)
        let target_elem_idx = anchor_idx + 2 * (step_i + 1);
        let preds = if target_elem_idx < elements.len() {
            if let PatternElement::Node(node) = &elements[target_elem_idx] {
                extract_node_property_predicates(node)?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        result.push(preds);
    }

    Ok(result)
}

/// Extracts property predicates from a single `NodePattern`'s properties map.
fn extract_node_property_predicates(
    node: &uni_cypher::ast::NodePattern,
) -> anyhow::Result<Vec<PropertyPredicate>> {
    let Some(ref props_expr) = node.properties else {
        return Ok(Vec::new());
    };

    let CypherExpr::Map(entries) = props_expr else {
        anyhow::bail!("Node properties must be a map literal for pattern exists optimization");
    };

    let mut predicates = Vec::with_capacity(entries.len());
    for (key, value_expr) in entries {
        match value_expr {
            CypherExpr::Parameter(param_name) => {
                predicates.push(PropertyPredicate {
                    property_name: key.clone(),
                    param_name: Some(param_name.clone()),
                    literal_value: None,
                });
            }
            CypherExpr::Literal(lit) => {
                predicates.push(PropertyPredicate {
                    property_name: key.clone(),
                    param_name: None,
                    literal_value: Some(lit.to_value()),
                });
            }
            _ => {
                anyhow::bail!(
                    "Unsupported property value expression in pattern exists: {:?}",
                    value_expr
                );
            }
        }
    }

    Ok(predicates)
}
