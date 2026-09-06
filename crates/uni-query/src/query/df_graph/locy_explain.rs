// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! EXPLAIN RULE derivation tree construction.
//!
//! Ported from `uni-locy/src/orchestrator/explain.rs`. Uses `DerivedFactSource`
//! instead of `CypherExecutor`. Uses `RowStore` for row-based fact storage.
//!
//! Implements Mode A (provenance-based, uses ProvenanceStore recorded during fixpoint)
//! with fallback to Mode B (re-execution) when tracker has no entries for the rule.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use uni_common::Value;
use uni_cypher::locy_ast::{ExplainRule, RuleCondition};
use uni_locy::types::CompiledRule;
use uni_locy::{CompiledProgram, DerivationNode, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_delta::{
    KeyTuple, RowStore, extract_cypher_conditions, extract_key, resolve_clause_with_is_refs,
};

use super::locy_eval::{
    eval_expr, normalize_graph_row, record_batches_to_locy_rows, values_equal_for_join,
};
use super::locy_slg::SLGResolver;
use super::locy_traits::DerivedFactSource;

/// A single term in a derivation proof: identifies one IS-ref dependency.
///
/// Each `ProofTerm` records a dependency edge in the proof DAG — the source
/// rule that was referenced and the opaque hash of the specific base fact
/// consumed (Cui & Widom 2000, lineage).
#[derive(Clone, Debug)]
pub struct ProofTerm {
    /// Name of the IS-ref rule that produced this dependency.
    pub source_rule: String,
    /// Opaque hash identifying the base fact consumed from `source_rule`.
    pub base_fact_id: Vec<u8>,
}

/// Provenance annotation for a derived fact (Green et al. 2007, Definition 3.1).
///
/// Captures a single derivation witness: the rule and clause that produced the
/// fact, plus the `support` set of proof terms that contributed to it.
#[derive(Clone, Debug)]
pub struct ProvenanceAnnotation {
    /// Name of the rule that derived this fact.
    pub rule_name: String,
    /// Index of the clause within the rule that produced this fact.
    pub clause_index: usize,
    /// Proof terms: IS-ref input facts (populated when IS-ref tracking is available).
    pub support: Vec<ProofTerm>,
    /// ALONG column values captured at derivation time.
    pub along_values: HashMap<String, Value>,
    /// Fixpoint iteration number when the fact was first derived.
    pub iteration: usize,
    /// Full fact row stored for Mode A filtering/display.
    pub fact_row: FactRow,
    /// Probability of this specific proof path (populated by top-k filtering).
    pub proof_probability: Option<f64>,
    /// Phase C B1–B3: per neural-model invocation that contributed
    /// to this fact's derivation. Populated by
    /// `LocyModelInvokeExec` when it runs a classifier; consumed
    /// by Mode A EXPLAIN to surface model_name + raw + calibrated
    /// + optional confidence band per call.
    pub neural_calls: Vec<uni_locy::NeuralProvenance>,
}

/// Provenance store for derived facts (Green et al. 2007, §3).
///
/// Stores provenance annotations keyed by opaque fact hashes. Enables
/// Mode A (provenance-based) EXPLAIN without re-execution.
/// First-derivation-wins: once a fact hash is recorded, later iterations
/// do not overwrite it.
#[derive(Debug)]
pub struct ProvenanceStore {
    entries: RwLock<HashMap<Vec<u8>, Vec<ProvenanceAnnotation>>>,
}

impl ProvenanceStore {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Record a provenance annotation. First-derivation-wins: if the hash is already
    /// present, the existing annotation is kept (unlimited mode).
    pub fn record(&self, fact_hash: Vec<u8>, entry: ProvenanceAnnotation) {
        if let Ok(mut guard) = self.entries.write() {
            guard.entry(fact_hash).or_insert_with(|| vec![entry]);
        }
    }

    /// Record a provenance annotation with top-k filtering.
    ///
    /// Retains at most `k` annotations per fact, ordered by `proof_probability`
    /// (highest first). Annotations without a proof probability are treated as
    /// having probability 0.0 for ordering purposes.
    pub fn record_top_k(&self, fact_hash: Vec<u8>, entry: ProvenanceAnnotation, k: usize) {
        if let Ok(mut guard) = self.entries.write() {
            let vec = guard.entry(fact_hash).or_default();
            vec.push(entry);
            // Sort descending by proof_probability.
            vec.sort_by(|a, b| {
                b.proof_probability
                    .unwrap_or(0.0)
                    .partial_cmp(&a.proof_probability.unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            vec.truncate(k);
        }
    }

    /// Look up the first (highest-priority) provenance annotation for a fact hash.
    pub fn lookup(&self, fact_hash: &[u8]) -> Option<ProvenanceAnnotation> {
        self.entries.read().ok()?.get(fact_hash)?.first().cloned()
    }

    /// Look up all provenance annotations for a fact hash.
    pub fn lookup_all(&self, fact_hash: &[u8]) -> Option<Vec<ProvenanceAnnotation>> {
        let guard = self.entries.read().ok()?;
        guard.get(fact_hash).cloned()
    }

    /// Collect base fact probabilities from stored annotations.
    ///
    /// Scans all annotations for base facts (those with empty support, i.e. leaf
    /// nodes in the proof tree) and extracts the PROB column value from their
    /// `fact_row`. Used by top-k proof filtering to compute `proof_probability`.
    pub fn base_fact_probs(&self) -> HashMap<Vec<u8>, f64> {
        let mut probs = HashMap::new();
        if let Ok(guard) = self.entries.read() {
            for (fact_hash, annotations) in guard.iter() {
                if let Some(ann) = annotations.first()
                    && ann.support.is_empty()
                    && let Some(uni_common::Value::Float(p)) = ann.fact_row.get("PROB")
                {
                    probs.insert(fact_hash.clone(), *p);
                }
            }
        }
        probs
    }

    /// Get all entries for a specific rule name (returns first annotation per fact).
    pub fn entries_for_rule(&self, rule_name: &str) -> Vec<(Vec<u8>, ProvenanceAnnotation)> {
        match self.entries.read() {
            Ok(guard) => guard
                .iter()
                .filter_map(|(k, annotations)| {
                    annotations
                        .first()
                        .filter(|e| e.rule_name == rule_name)
                        .map(|e| (k.clone(), e.clone()))
                })
                .collect(),
            Err(_) => vec![],
        }
    }
}

/// Compute the probability of a single proof path from its support terms.
///
/// Returns `None` if any base fact's probability is unknown.
pub fn compute_proof_probability(
    support: &[ProofTerm],
    base_probs: &HashMap<Vec<u8>, f64>,
) -> Option<f64> {
    if support.is_empty() {
        return None;
    }
    let mut product = 1.0;
    for term in support {
        let p = base_probs.get(&term.base_fact_id)?;
        product *= p;
    }
    Some(product)
}

impl Default for ProvenanceStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Set of (rule_name, key_tuple) to detect cycles during recursive derivation (Mode B).
type VisitedSet = HashSet<(String, KeyTuple)>;

/// Build a derivation tree for a rule, showing how each fact was derived.
///
/// Tries Mode A (provenance-based, uses ProvenanceStore) first when a store is
/// provided and has entries for the rule.  Falls through to Mode B (re-execution)
/// when Mode A cannot produce a result.
#[expect(
    clippy::too_many_arguments,
    reason = "explain requires full program context and tracker state"
)]
pub async fn explain_rule(
    query: &ExplainRule,
    program: &CompiledProgram,
    fact_source: &dyn DerivedFactSource,
    config: &LocyConfig,
    derived_store: &mut RowStore,
    stats: &mut LocyStats,
    tracker: Option<&ProvenanceStore>,
    approximate_groups: Option<&HashMap<String, Vec<String>>>,
) -> Result<DerivationNode, LocyError> {
    // Mode A: provenance-based (no re-execution required).
    // Falls through to Mode B when tracker is absent or has no matching entries.
    if let Some(t) = tracker
        && let Ok(node) = explain_rule_mode_a(
            query,
            program,
            t,
            fact_source,
            derived_store,
            approximate_groups,
        )
        .await
    {
        return Ok(node);
    }

    // Mode B: re-execution fallback
    explain_rule_mode_b(
        query,
        program,
        fact_source,
        config,
        derived_store,
        stats,
        approximate_groups,
    )
    .await
}

/// Mode A: build derivation tree using recorded provenance from the fixpoint loop.
///
/// Returns `Err` when no tracker entries exist for the rule (signals Mode B fallback).
async fn explain_rule_mode_a(
    query: &ExplainRule,
    program: &CompiledProgram,
    tracker: &ProvenanceStore,
    fact_source: &dyn DerivedFactSource,
    _derived_store: &RowStore,
    approximate_groups: Option<&HashMap<String, Vec<String>>>,
) -> Result<DerivationNode, LocyError> {
    let rule_name = query.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::EvaluationError {
            message: format!("rule '{}' not found for EXPLAIN RULE (Mode A)", rule_name),
        })?;

    let tracker_entries = tracker.entries_for_rule(&rule_name);
    if tracker_entries.is_empty() {
        return Err(LocyError::EvaluationError {
            message: format!("no tracker entries for rule '{rule_name}' (falling back to Mode B)"),
        });
    }

    // Enrich each tracker entry's fact_row so WHERE predicates like
    // `n.prop = X` or `e.prop = X` resolve against full Node / Edge
    // values. Two encodings to handle:
    //   - Node bindings: tracker stores `Value::Int(vid)` (Arrow UInt64).
    //     Resolved via the side-channel `lookup_nodes_by_vids` Cypher.
    //   - Edge bindings: tracker stores `Value::Map({_eid,_type,...})`
    //     (Arrow struct). Resolved by `normalize_graph_row`, the same
    //     helper Mode B uses, which converts Map → Value::Edge.
    //
    // Enrichment is scoped to KEY columns from the rule's yield_schema
    // (`is_key && !is_prob`) so non-KEY scalar Ints (e.g. count columns)
    // never get coerced into Nodes.
    let key_cols: HashSet<String> = rule
        .yield_schema
        .iter()
        .filter(|c| c.is_key && !c.is_prob)
        .map(|c| c.name.clone())
        .collect();

    let mut tracker_entries: Vec<(Vec<u8>, ProvenanceAnnotation)> = tracker_entries
        .into_iter()
        .map(|(h, mut ann)| {
            // Map → Edge / Node conversion for bare graph-entity vars.
            normalize_graph_row(&mut ann.fact_row);
            (h, ann)
        })
        .collect();

    // Collect residual Int-vid candidates from KEY columns only.
    let mut candidate_vids: Vec<u64> = Vec::new();
    for (_, entry) in &tracker_entries {
        for (k, v) in &entry.fact_row {
            if key_cols.contains(k)
                && let Value::Int(i) = v
                && *i >= 0
            {
                candidate_vids.push(*i as u64);
            }
        }
    }
    candidate_vids.sort_unstable();
    candidate_vids.dedup();
    let vid_to_node: HashMap<u64, Value> = if candidate_vids.is_empty() {
        HashMap::new()
    } else {
        fact_source
            .lookup_nodes_by_vids(&candidate_vids)
            .await
            .unwrap_or_default()
    };
    if !vid_to_node.is_empty() {
        for (_, ann) in &mut tracker_entries {
            for (k, v) in ann.fact_row.iter_mut() {
                if key_cols.contains(k)
                    && let Value::Int(i) = v
                    && *i >= 0
                    && let Some(node) = vid_to_node.get(&(*i as u64))
                {
                    *v = node.clone();
                }
            }
        }
    }

    // Filter tracker entries by WHERE expression
    let matching_entries: Vec<_> = if let Some(where_expr) = &query.where_expr {
        tracker_entries
            .into_iter()
            .filter(|(_, entry)| {
                eval_expr(where_expr, &entry.fact_row)
                    .map(|v| v.as_bool().unwrap_or(false))
                    .unwrap_or(false)
            })
            .collect()
    } else {
        tracker_entries
    };

    if matching_entries.is_empty() {
        return Err(LocyError::EvaluationError {
            message: format!("no tracker entries match WHERE clause for rule '{rule_name}'"),
        });
    }

    let is_approximate = approximate_groups
        .map(|ag| ag.contains_key(&rule_name))
        .unwrap_or(false);

    let mut root = DerivationNode {
        rule: rule_name.clone(),
        clause_index: 0,
        priority: rule.priority,
        bindings: HashMap::new(),
        along_values: HashMap::new(),
        children: Vec::new(),
        graph_fact: None,
        approximate: is_approximate,
        proof_probability: None,
        neural_calls: Vec::new(),
    };

    for (_, entry) in matching_entries {
        let along_values = extract_along_values(&entry.fact_row, rule);
        let clause_priority = rule
            .clauses
            .get(entry.clause_index)
            .and_then(|c| c.priority);
        let base_fact = format!(
            "[iter={}] {}",
            entry.iteration,
            format_graph_fact(&entry.fact_row)
        );
        let graph_fact = if is_approximate {
            format!("[APPROXIMATE] {}", base_fact)
        } else {
            base_fact
        };
        let node = DerivationNode {
            rule: rule_name.clone(),
            clause_index: entry.clause_index,
            priority: clause_priority.or(rule.priority),
            bindings: entry.fact_row.clone(),
            along_values,
            // Mode A: children not tracked (inputs list is reserved for future recursion)
            children: vec![],
            graph_fact: Some(graph_fact),
            approximate: is_approximate,
            proof_probability: entry.proof_probability,
            // Phase C B1–B3: surface the per-call neural metadata
            // captured by `collect_neural_calls_for_row` during
            // fixpoint into the EXPLAIN derivation tree.
            neural_calls: entry.neural_calls.clone(),
        };
        root.children.push(node);
    }

    Ok(root)
}

/// Mode B: re-execution fallback — re-executes clause queries to find which
/// clause produced each matching fact, then recurses into IS references.
async fn explain_rule_mode_b(
    query: &ExplainRule,
    program: &CompiledProgram,
    fact_source: &dyn DerivedFactSource,
    config: &LocyConfig,
    derived_store: &mut RowStore,
    stats: &mut LocyStats,
    approximate_groups: Option<&HashMap<String, Vec<String>>>,
) -> Result<DerivationNode, LocyError> {
    let rule_name = query.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::EvaluationError {
            message: format!("rule '{}' not found for EXPLAIN RULE", rule_name),
        })?;

    let key_columns: Vec<String> = rule
        .yield_schema
        .iter()
        .filter(|c| c.is_key)
        .map(|c| c.name.clone())
        .collect();

    // Re-evaluate the rule via SLG to obtain rows with full node objects and properties.
    //
    // Top-level QUERY no longer does this — it answers from the derived store,
    // so `QUERY r` and `.derived["r"]` are the same bytes (issue #160). Mode B
    // EXPLAIN cannot follow yet, and the reason is narrower than the one this
    // comment used to give: `enrich_vids_with_nodes` hydrates node VID columns
    // into `Value::Node`, but there is **no equivalent for edge EIDs**. A rule
    // with a KEY *edge* variable therefore yields `None` for that binding when
    // read from the derived store, which `test_edge_binding_where_filter_works_via_mode_a`
    // catches. Removing this block regresses exactly that test and nothing else.
    //
    // Closing the gap means hydrating edge columns the way node columns already
    // are; until then EXPLAIN keeps its own re-derivation. Mode A
    // (`explain_rule_mode_a`) reads the fixpoint's ProvenanceStore and is
    // unaffected either way.
    {
        let mut fresh_store = RowStore::new();
        let slg_start = std::time::Instant::now();
        let mut resolver =
            SLGResolver::new(program, fact_source, config, &mut fresh_store, slg_start);
        resolver.resolve_goal(&rule_name, &HashMap::new()).await?;
        stats.queries_executed += resolver.stats.queries_executed;
        // Merge full-node facts into derived_store so IS-ref lookups in
        // build_derivation_node also get proper node objects (not VIDs).
        for (name, relation) in fresh_store {
            derived_store.insert(name, relation);
        }
    }

    // Get all derived facts for this rule (now populated with full node objects)
    let facts = derived_store
        .get(&rule_name)
        .map(|r| r.rows.clone())
        .unwrap_or_default();

    // Filter facts by WHERE expression
    let filtered: Vec<FactRow> = if let Some(where_expr) = &query.where_expr {
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

    let is_approximate = approximate_groups
        .map(|ag| ag.contains_key(&rule_name))
        .unwrap_or(false);

    // Build derivation tree root
    let mut root = DerivationNode {
        rule: rule_name.clone(),
        clause_index: 0,
        priority: rule.priority,
        bindings: HashMap::new(),
        along_values: HashMap::new(),
        children: Vec::new(),
        graph_fact: None,
        approximate: is_approximate,
        proof_probability: None,
        neural_calls: Vec::new(),
    };

    // For each matching fact, recursively build a derivation node
    for fact in &filtered {
        let mut visited = VisitedSet::new();
        let mut node = build_derivation_node(
            &rule_name,
            fact,
            &key_columns,
            program,
            fact_source,
            derived_store,
            stats,
            &mut visited,
            config.max_explain_depth,
        )
        .await?;
        if is_approximate {
            node.approximate = true;
            if let Some(ref gf) = node.graph_fact {
                node.graph_fact = Some(format!("[APPROXIMATE] {}", gf));
            }
        }
        root.children.push(node);
    }

    Ok(root)
}

/// Recursively build a derivation node for a single fact of a rule.
///
/// Finds which clause produced this fact, extracts ALONG values,
/// and recurses into IS reference dependencies.
#[expect(
    clippy::too_many_arguments,
    reason = "recursive derivation node builder requires full fact context"
)]
fn build_derivation_node<'a>(
    rule_name: &'a str,
    fact: &'a FactRow,
    key_columns: &'a [String],
    program: &'a CompiledProgram,
    fact_source: &'a dyn DerivedFactSource,
    derived_store: &'a mut RowStore,
    stats: &'a mut LocyStats,
    visited: &'a mut VisitedSet,
    max_depth: usize,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<DerivationNode, LocyError>> + Send + 'a>,
> {
    Box::pin(async move {
        let rule =
            program
                .rule_catalog
                .get(rule_name)
                .ok_or_else(|| LocyError::EvaluationError {
                    message: format!("rule '{}' not found during EXPLAIN", rule_name),
                })?;

        let key_tuple = extract_key(fact, key_columns);
        let visit_key = (rule_name.to_string(), key_tuple);

        // Cycle detection
        if !visited.insert(visit_key.clone()) || max_depth == 0 {
            return Ok(DerivationNode {
                rule: rule_name.to_string(),
                clause_index: 0,
                priority: rule.priority,
                bindings: fact.clone(),
                along_values: extract_along_values(fact, rule),
                children: Vec::new(),
                graph_fact: Some("(cycle)".to_string()),
                approximate: false,
                proof_probability: None,
                neural_calls: Vec::new(),
            });
        }

        // Match on KEY columns only.  Clause-level resolution returns only
        // base graph bindings (vertex/edge identifiers); non-KEY yield columns
        // (FOLD-aggregated, similar_to, etc.) are absent from those rows.
        // KEY columns uniquely identify a derived fact, so this is sufficient.

        // Try each clause to find the one that produced this fact
        for (clause_idx, clause) in rule.clauses.iter().enumerate() {
            let has_is_refs = clause
                .where_conditions
                .iter()
                .any(|c| matches!(c, RuleCondition::IsReference(_)));
            let has_along = !clause.along.is_empty();

            let resolved = if has_is_refs || has_along {
                let rows = resolve_clause_with_is_refs(
                    clause,
                    fact_source,
                    derived_store,
                    &program.rule_catalog,
                    None, // EXPLAIN traces proofs, doesn't compute probabilities
                )
                .await?;
                stats.queries_executed += 1;
                rows
            } else {
                let cypher_conditions = extract_cypher_conditions(&clause.where_conditions);
                let raw_batches = fact_source
                    .execute_pattern(&clause.match_pattern, &cypher_conditions)
                    .await?;
                stats.queries_executed += 1;
                record_batches_to_locy_rows(&raw_batches)
            };

            // Use values_equal_for_join for VID/EID-based comparison: sidecar
            // schema mode can add `overflow_json: Null` to nodes in some query
            // paths, making structural equality unreliable.
            let matching_row = resolved.iter().find(|row| {
                key_columns.iter().all(|k| match (row.get(k), fact.get(k)) {
                    (Some(v1), Some(v2)) => values_equal_for_join(v1, v2),
                    (None, None) => true,
                    _ => false,
                })
            });

            if let Some(evidence_row) = matching_row {
                let along_values = extract_along_values(fact, rule);

                // Build children by recursing into IS references
                let mut children = Vec::new();
                for cond in &clause.where_conditions {
                    if let RuleCondition::IsReference(is_ref) = cond {
                        if is_ref.negated {
                            continue;
                        }
                        let ref_rule_name = is_ref.rule_name.to_string();
                        if let Some(ref_rule) = program.rule_catalog.get(&ref_rule_name) {
                            let ref_key_columns: Vec<String> = ref_rule
                                .yield_schema
                                .iter()
                                .filter(|c| c.is_key)
                                .map(|c| c.name.clone())
                                .collect();

                            let ref_facts: Vec<FactRow> = derived_store
                                .get(&ref_rule_name)
                                .map(|r| r.rows.clone())
                                .unwrap_or_default();

                            let matching_ref_facts: Vec<FactRow> = ref_facts
                                .into_iter()
                                .filter(|ref_fact| {
                                    let subjects_match =
                                        is_ref.subjects.iter().enumerate().all(|(i, subject)| {
                                            binding_matches_key(
                                                evidence_row,
                                                fact,
                                                subject,
                                                ref_fact,
                                                ref_key_columns.get(i),
                                            )
                                        });
                                    let target_matches =
                                        is_ref.target.as_ref().is_none_or(|target| {
                                            binding_matches_key(
                                                evidence_row,
                                                fact,
                                                target,
                                                ref_fact,
                                                ref_key_columns.get(is_ref.subjects.len()),
                                            )
                                        });
                                    subjects_match && target_matches
                                })
                                .collect();

                            for ref_fact in matching_ref_facts {
                                let child = build_derivation_node(
                                    &ref_rule_name,
                                    &ref_fact,
                                    &ref_key_columns,
                                    program,
                                    fact_source,
                                    derived_store,
                                    stats,
                                    visited,
                                    max_depth - 1,
                                )
                                .await?;
                                children.push(child);
                            }
                        }
                    }
                }

                // Backtrack visited set
                visited.remove(&visit_key);

                let mut merged_bindings = evidence_row.clone();
                for (k, v) in fact {
                    merged_bindings.entry(k.clone()).or_insert(v.clone());
                }

                return Ok(DerivationNode {
                    rule: rule_name.to_string(),
                    clause_index: clause_idx,
                    priority: rule.clauses[clause_idx].priority,
                    bindings: merged_bindings,
                    along_values,
                    children,
                    graph_fact: Some(format_graph_fact(evidence_row)),
                    approximate: false,
                    proof_probability: None,
                    neural_calls: Vec::new(),
                });
            }
        }

        // No clause matched — leaf node
        visited.remove(&visit_key);
        Ok(DerivationNode {
            rule: rule_name.to_string(),
            clause_index: 0,
            priority: rule.priority,
            bindings: fact.clone(),
            along_values: extract_along_values(fact, rule),
            children: Vec::new(),
            graph_fact: Some(format_graph_fact(fact)),
            approximate: false,
            proof_probability: None,
            neural_calls: Vec::new(),
        })
    })
}

/// Check if a binding variable matches a ref-fact key column via VID-based join.
///
/// Looks up `var_name` in `primary` (falling back to `fallback`), then compares
/// it against `ref_key_col` in `ref_fact` using `values_equal_for_join`.
/// Returns `true` when the key column is out of range or the binding is absent.
fn binding_matches_key(
    primary: &FactRow,
    fallback: &FactRow,
    var_name: &str,
    ref_fact: &FactRow,
    ref_key_col: Option<&String>,
) -> bool {
    let Some(key_col) = ref_key_col else {
        return true;
    };
    let Some(val) = primary.get(var_name).or_else(|| fallback.get(var_name)) else {
        return true;
    };
    ref_fact
        .get(key_col)
        .is_some_and(|rv| values_equal_for_join(rv, val))
}

fn extract_along_values(fact: &FactRow, rule: &CompiledRule) -> HashMap<String, Value> {
    let mut along_values = HashMap::new();
    for clause in &rule.clauses {
        for along in &clause.along {
            if let Some(v) = fact.get(&along.name) {
                along_values.insert(along.name.clone(), v.clone());
            }
        }
    }
    along_values
}

pub(crate) fn format_graph_fact(row: &FactRow) -> String {
    let mut entries: Vec<String> = row
        .iter()
        .map(|(k, v)| format!("{}: {}", k, format_value(v)))
        .collect();
    entries.sort();
    format!("{{{}}}", entries.join(", "))
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => format!("\"{}\"", s),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Map(m) => {
            let mut entries: Vec<String> = m
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                .collect();
            entries.sort();
            format!("{{{}}}", entries.join(", "))
        }
        Value::Node(n) => format!("Node({})", n.vid.as_u64()),
        Value::Edge(e) => format!("Edge({})", e.eid.as_u64()),
        // The arms above sort their map entries; this one did not, and it is
        // reachable for a `Value::Path`, whose nodes carry property maps. An
        // explain plan that renders differently on each call cannot be diffed.
        other => other.canonical_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_annotation(rule: &str, prob: Option<f64>) -> ProvenanceAnnotation {
        ProvenanceAnnotation {
            rule_name: rule.to_string(),
            clause_index: 0,
            support: vec![],
            along_values: HashMap::new(),
            iteration: 0,
            fact_row: HashMap::new(),
            proof_probability: prob,
            neural_calls: Vec::new(),
        }
    }

    #[test]
    fn record_first_derivation_wins() {
        let store = ProvenanceStore::new();
        let hash = b"fact1".to_vec();
        store.record(hash.clone(), make_annotation("rule_a", None));
        store.record(hash.clone(), make_annotation("rule_b", None));
        let entry = store.lookup(&hash).unwrap();
        assert_eq!(entry.rule_name, "rule_a");
    }

    #[test]
    fn lookup_returns_first_annotation() {
        let store = ProvenanceStore::new();
        let hash = b"fact1".to_vec();
        store.record(hash.clone(), make_annotation("rule_a", None));
        assert_eq!(store.lookup(&hash).unwrap().rule_name, "rule_a");
        assert!(store.lookup(b"nonexistent").is_none());
    }

    #[test]
    fn lookup_all_returns_all_annotations() {
        let store = ProvenanceStore::new();
        let hash = b"fact1".to_vec();
        // record() is first-wins, so only one entry per hash via record()
        store.record(hash.clone(), make_annotation("rule_a", None));
        let all = store.lookup_all(&hash).unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn record_top_k_retains_highest() {
        let store = ProvenanceStore::new();
        let hash = b"fact1".to_vec();
        store.record_top_k(hash.clone(), make_annotation("low", Some(0.1)), 2);
        store.record_top_k(hash.clone(), make_annotation("high", Some(0.9)), 2);
        store.record_top_k(hash.clone(), make_annotation("mid", Some(0.5)), 2);
        store.record_top_k(hash.clone(), make_annotation("highest", Some(0.95)), 2);
        store.record_top_k(hash.clone(), make_annotation("lowest", Some(0.01)), 2);

        let all = store.lookup_all(&hash).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].rule_name, "highest");
        assert_eq!(all[1].rule_name, "high");
    }

    #[test]
    fn compute_proof_probability_basic() {
        let support = vec![
            ProofTerm {
                source_rule: "r1".to_string(),
                base_fact_id: b"f1".to_vec(),
            },
            ProofTerm {
                source_rule: "r2".to_string(),
                base_fact_id: b"f2".to_vec(),
            },
        ];
        let base_probs = HashMap::from([(b"f1".to_vec(), 0.3), (b"f2".to_vec(), 0.5)]);
        let prob = compute_proof_probability(&support, &base_probs);
        assert!((prob.unwrap() - 0.15).abs() < 1e-10);
    }

    #[test]
    fn compute_proof_probability_empty_support() {
        let prob = compute_proof_probability(&[], &HashMap::new());
        assert!(prob.is_none());
    }

    #[test]
    fn compute_proof_probability_missing_fact() {
        let support = vec![ProofTerm {
            source_rule: "r1".to_string(),
            base_fact_id: b"unknown".to_vec(),
        }];
        let prob = compute_proof_probability(&support, &HashMap::new());
        assert!(prob.is_none());
    }

    #[test]
    fn entries_for_rule_filters_correctly() {
        let store = ProvenanceStore::new();
        store.record(b"f1".to_vec(), make_annotation("rule_a", None));
        store.record(b"f2".to_vec(), make_annotation("rule_b", None));
        store.record(b"f3".to_vec(), make_annotation("rule_a", None));

        let entries = store.entries_for_rule("rule_a");
        assert_eq!(entries.len(), 2);
        let entries_b = store.entries_for_rule("rule_b");
        assert_eq!(entries_b.len(), 1);
    }
}
