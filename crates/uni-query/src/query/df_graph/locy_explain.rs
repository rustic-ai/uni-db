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
    eval_condition, normalize_graph_row, record_batches_to_locy_rows, values_equal_for_join,
};
use super::locy_traits::DerivedFactSource;

/// A single term in a derivation proof: identifies one IS-ref dependency.
///
/// Each `ProofTerm` records a dependency edge in the proof DAG — the source
/// rule that was referenced and the specific fact consumed (Cui & Widom 2000,
/// lineage). Terms are read through the one reference resolution
/// (`locy_lineage::RefResolver`), so they are exactly the references exact
/// inference and `TopKProofs` evaluate.
#[derive(Clone, Debug)]
pub struct ProofTerm {
    /// Name of the IS-ref rule that produced this dependency.
    pub source_rule: String,
    /// The hash of the consumed fact's KEY columns: the
    /// [`ProvenanceAnnotation::fact_key`] of that fact's derivations.
    pub base_fact_id: Vec<u8>,
    /// The probability the derivation read for the consumed fact: the factor
    /// it contributes to the derivation's [`ProvenanceAnnotation::proof_probability`].
    pub prob: f64,
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
    /// The hash of the fact's KEY columns, which a [`ProofTerm`] of a fact
    /// derived from this one names it by.
    pub fact_key: Vec<u8>,
    /// The hash identifying this derivation among the fact's derivations:
    /// the key it is recorded under when proofs are not filtered. A
    /// recursive rule re-records a derivation as the facts it references are
    /// refined, under the same hash.
    pub derivation: Vec<u8>,
    /// Proof terms: the facts this derivation positively references through a
    /// probabilistic `IS` (to a PROB rule, from an `MNOR`/`MPROD` rule). A
    /// reference to a rule without a PROB column is a certain condition and
    /// is not listed.
    pub support: Vec<ProofTerm>,
    /// ALONG column values captured at derivation time.
    pub along_values: HashMap<String, Value>,
    /// Fixpoint iteration number when the fact was first derived.
    pub iteration: usize,
    /// Full fact row stored for Mode A filtering/display.
    pub fact_row: FactRow,
    /// The weight of this derivation: the row's own PROB value, which is its
    /// local weight times every probability it referenced (each carried on
    /// its [`ProofTerm`]). A leaf, referencing nothing, weighs its own PROB
    /// value. `None` for a rule without a PROB column. Top-k filtering keeps
    /// each fact's heaviest derivations by this weight.
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
    /// The lineage of every `MNOR`/`MPROD` rule evaluated in this run, which
    /// exact inference and shared-proof detection read.
    lineage: super::locy_lineage::LineageStore,
}

impl ProvenanceStore {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            lineage: super::locy_lineage::LineageStore::new(),
        }
    }

    /// The lineage of the run's `MNOR`/`MPROD` rules.
    pub(crate) fn lineage(&self) -> &super::locy_lineage::LineageStore {
        &self.lineage
    }

    /// Record a provenance annotation. First-derivation-wins: if the hash is already
    /// present, the existing annotation is kept (unlimited mode).
    pub fn record(&self, fact_hash: Vec<u8>, entry: ProvenanceAnnotation) {
        if let Ok(mut guard) = self.entries.write() {
            guard.entry(fact_hash).or_insert_with(|| vec![entry]);
        }
    }

    /// Record a derivation's provenance, keeping its first iteration and
    /// clause but its latest value.
    ///
    /// A recursive rule's derivation is re-recorded each iteration its value
    /// changes, as the facts it references are refined; first-derivation-wins
    /// would show the value of the iteration that first derived it.
    pub fn record_derivation(&self, fact_hash: Vec<u8>, entry: ProvenanceAnnotation) {
        if let Ok(mut guard) = self.entries.write() {
            match guard.entry(fact_hash) {
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    if let Some(first) = slot.get_mut().first_mut() {
                        first.fact_row = entry.fact_row;
                        first.support = entry.support;
                        first.along_values = entry.along_values;
                        first.proof_probability = entry.proof_probability;
                        first.neural_calls = entry.neural_calls;
                    }
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(vec![entry]);
                }
            }
        }
    }

    /// Record a derivation of the fact `fact_hash` with top-k filtering.
    ///
    /// `fact_hash` names the fact, not the derivation ([`top_k_fact_hash`]),
    /// so the fact's derivations compete: at most `k` are retained, heaviest
    /// [`ProvenanceAnnotation::proof_probability`] first, a derivation without
    /// a weight counting as 0.0, and earlier derivations winning ties. A
    /// derivation recorded again (a recursive rule refining it) replaces its
    /// earlier record rather than competing with it.
    pub fn record_top_k(&self, fact_hash: Vec<u8>, entry: ProvenanceAnnotation, k: usize) {
        if let Ok(mut guard) = self.entries.write() {
            let vec = guard.entry(fact_hash).or_default();
            match vec.iter_mut().find(|e| e.derivation == entry.derivation) {
                Some(existing) => *existing = entry,
                None => vec.push(entry),
            }
            // Stable, so equal weights keep their recording order.
            vec.sort_by(|a, b| {
                b.proof_probability
                    .unwrap_or(0.0)
                    .total_cmp(&a.proof_probability.unwrap_or(0.0))
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

    /// Every recorded derivation, of every rule: one per key, or under
    /// top-k filtering each fact's retained derivations.
    pub fn all_entries(&self) -> Vec<ProvenanceAnnotation> {
        match self.entries.read() {
            Ok(guard) => guard.values().flatten().cloned().collect(),
            Err(_) => vec![],
        }
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

/// The key top-k filtering records a derivation of `rule`'s fact with KEY
/// hash `fact_key` under: the fact, so that its derivations compete.
pub fn top_k_fact_hash(rule: &str, fact_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(rule.len() + 1 + fact_key.len());
    key.extend_from_slice(rule.as_bytes());
    key.push(0);
    key.extend_from_slice(fact_key);
    key
}

/// The weight of one derivation: `own`, the row's PROB value, when the rule
/// has a PROB column (`has_prob`); otherwise the product of the
/// probabilities it referenced.
///
/// The row's PROB value is its local weight times every probability it
/// referenced, so for a PROB rule it already is the product along the
/// derivation; a leaf, referencing nothing, weighs exactly its own value,
/// and `None` when that value is NULL. A rule without a PROB column derives
/// certain facts: a derivation referencing nothing weighs 1, as does a
/// reference to such a fact (its [`ProofTerm::prob`] is 1).
pub fn compute_proof_probability(
    has_prob: bool,
    own: Option<f64>,
    support: &[ProofTerm],
) -> Option<f64> {
    match own {
        Some(p) => Some(p.clamp(0.0, 1.0)),
        None if has_prob => None,
        None => Some(support.iter().map(|t| t.prob.clamp(0.0, 1.0)).product()),
    }
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
    derived_store: &RowStore,
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
            config.max_explain_depth,
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
/// Each recorded derivation of a matching fact is a child of the root, and
/// each derivation's children are the facts it references
/// ([`ProvenanceAnnotation::support`]), expanded into their own derivations
/// (see [`ModeATree::node`]).
///
/// Returns `Err` when no tracker entries exist for the rule (signals Mode B fallback).
async fn explain_rule_mode_a(
    query: &ExplainRule,
    program: &CompiledProgram,
    tracker: &ProvenanceStore,
    fact_source: &dyn DerivedFactSource,
    derived_store: &RowStore,
    approximate_groups: Option<&HashMap<String, Vec<String>>>,
    max_depth: usize,
) -> Result<DerivationNode, LocyError> {
    let rule_name = query.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::EvaluationError {
            message: format!("rule '{}' not found for EXPLAIN RULE (Mode A)", rule_name),
        })?;

    if tracker.entries_for_rule(&rule_name).is_empty() {
        return Err(LocyError::EvaluationError {
            message: format!("no tracker entries for rule '{rule_name}' (falling back to Mode B)"),
        });
    }

    // Every recorded derivation: the root's and those of the facts they
    // reference, hydrated alike.
    let mut entries = tracker.all_entries();
    hydrate_annotations(&mut entries, program, fact_source, derived_store).await;
    // Stable order: the store is a hash map.
    entries.sort_by_cached_key(|e| {
        (
            e.rule_name.clone(),
            e.iteration,
            format_graph_fact(&visible_row(&e.fact_row)),
        )
    });
    let mut by_fact: HashMap<(String, Vec<u8>), Vec<usize>> = HashMap::new();
    for (i, entry) in entries.iter().enumerate() {
        by_fact
            .entry((entry.rule_name.clone(), entry.fact_key.clone()))
            .or_default()
            .push(i);
    }

    // Filter the rule's entries by the WHERE expression.
    let mut matching: Vec<usize> = Vec::new();
    for (i, entry) in entries.iter().enumerate() {
        if entry.rule_name != rule_name {
            continue;
        }
        let keep = match &query.where_expr {
            Some(where_expr) => eval_condition(where_expr, &entry.fact_row, "EXPLAIN RULE WHERE")?,
            None => true,
        };
        if keep {
            matching.push(i);
        }
    }

    if matching.is_empty() {
        return Err(LocyError::EvaluationError {
            message: format!("no tracker entries match WHERE clause for rule '{rule_name}'"),
        });
    }

    let tree = ModeATree {
        program,
        entries: &entries,
        by_fact: &by_fact,
        approximate_groups,
    };
    let mut root = DerivationNode {
        rule: rule_name.clone(),
        clause_index: 0,
        priority: rule.priority,
        bindings: HashMap::new(),
        along_values: HashMap::new(),
        children: Vec::new(),
        graph_fact: None,
        approximate: tree.approximate(&rule_name),
        proof_probability: None,
        neural_calls: Vec::new(),
    };
    let mut expanded = HashSet::new();
    for i in matching {
        let mut path = HashSet::new();
        root.children
            .push(tree.node(i, &mut path, &mut expanded, max_depth));
    }
    Ok(root)
}

/// A fact row without the engine's hidden columns (`__deriv_*`), which are
/// derivation identity, not bindings.
fn visible_row(row: &FactRow) -> FactRow {
    row.iter()
        .filter(|(k, _)| {
            !k.starts_with(crate::query::planner_locy_types::FOLD_DISCRIMINATOR_COL_PREFIX)
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Turns the integer IDs left in each annotation's KEY columns into nodes and
/// edges, and its edge maps into edges, so EXPLAIN WHERE predicates like
/// `n.prop = X` resolve and every level of the tree reads alike.
///
/// Which KEY columns hold entities, and of which kind, is read from each
/// rule's published facts, which hydration already turned into nodes and
/// edges: tracker rows carry a VID, an EID and a plain integer KEY alike as an
/// integer, and the two ID spaces overlap, so guessing would bind an
/// unrelated entity. Non-KEY scalar integers (e.g. counts) are never coerced.
async fn hydrate_annotations(
    entries: &mut [ProvenanceAnnotation],
    program: &CompiledProgram,
    fact_source: &dyn DerivedFactSource,
    derived_store: &RowStore,
) {
    // Per rule: KEY column -> whether it holds an edge (else a node).
    let mut entity_cols: HashMap<String, HashMap<String, bool>> = HashMap::new();
    for entry in entries.iter_mut() {
        // Map → Edge / Node conversion for bare graph-entity vars.
        normalize_graph_row(&mut entry.fact_row);
        if entity_cols.contains_key(&entry.rule_name) {
            continue;
        }
        let mut kinds = HashMap::new();
        if let (Some(rule), Some(relation)) = (
            program.rule_catalog.get(&entry.rule_name),
            derived_store.get(&entry.rule_name),
        ) {
            for column in rule
                .yield_schema
                .iter()
                .filter(|c| c.is_key && !c.is_prob)
                .map(|c| &c.name)
            {
                let kind = relation.rows.iter().find_map(|row| match row.get(column) {
                    Some(Value::Node(_)) => Some(false),
                    Some(Value::Edge(_)) => Some(true),
                    _ => None,
                });
                if let Some(is_edge) = kind {
                    kinds.insert(column.clone(), is_edge);
                }
            }
        }
        entity_cols.insert(entry.rule_name.clone(), kinds);
    }
    let ids_in = |edges: bool| -> Vec<u64> {
        let mut ids: Vec<u64> = entries
            .iter()
            .flat_map(|entry| {
                let kinds = &entity_cols[&entry.rule_name];
                entry
                    .fact_row
                    .iter()
                    .filter(move |(k, _)| kinds.get(*k) == Some(&edges))
            })
            .filter_map(|(_, v)| match v {
                Value::Int(i) if *i >= 0 => Some(*i as u64),
                _ => None,
            })
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    let (vids, eids) = (ids_in(false), ids_in(true));
    let vid_to_node = if vids.is_empty() {
        HashMap::new()
    } else {
        fact_source
            .lookup_nodes_by_vids(&vids)
            .await
            .unwrap_or_default()
    };
    let eid_to_edge = if eids.is_empty() {
        HashMap::new()
    } else {
        fact_source
            .lookup_edges_by_eids(&eids)
            .await
            .unwrap_or_default()
    };
    for entry in entries.iter_mut() {
        let kinds = &entity_cols[&entry.rule_name];
        for (k, v) in entry.fact_row.iter_mut() {
            let resolved = match kinds.get(k) {
                Some(true) => &eid_to_edge,
                Some(false) => &vid_to_node,
                None => continue,
            };
            if let Value::Int(i) = v
                && *i >= 0
                && let Some(entity) = resolved.get(&(*i as u64))
            {
                *v = entity.clone();
            }
        }
    }
}

/// The recorded derivations Mode A builds its tree from.
struct ModeATree<'a> {
    program: &'a CompiledProgram,
    entries: &'a [ProvenanceAnnotation],
    /// Each fact's derivations, by `(rule, KEY hash)`.
    by_fact: &'a HashMap<(String, Vec<u8>), Vec<usize>>,
    approximate_groups: Option<&'a HashMap<String, Vec<String>>>,
}

impl ModeATree<'_> {
    /// Whether `rule` has a group that fell back to independence.
    fn approximate(&self, rule: &str) -> bool {
        self.approximate_groups
            .is_some_and(|ag| ag.contains_key(rule))
    }

    /// The node of derivation `index`, with the facts it references below
    /// it.
    ///
    /// A referenced fact's derivations are expanded where the fact first
    /// appears in the tree, `depth` levels deep; anywhere else it is a leaf
    /// carrying its bindings: `(cycle)` when it is on the path from the root
    /// (a fact deriving itself), `(derived above)` when expanded elsewhere,
    /// `(depth limit)` past `max_explain_depth`. Expanding every occurrence
    /// would repeat subtrees exponentially in the depth.
    fn node(
        &self,
        index: usize,
        path: &mut HashSet<(String, Vec<u8>)>,
        expanded: &mut HashSet<(String, Vec<u8>)>,
        depth: usize,
    ) -> DerivationNode {
        let entry = &self.entries[index];
        let rule = self.program.rule_catalog.get(&entry.rule_name);
        let approximate = self.approximate(&entry.rule_name);
        let row = visible_row(&entry.fact_row);
        let fact = format!("[iter={}] {}", entry.iteration, format_graph_fact(&row));
        let own = (entry.rule_name.clone(), entry.fact_key.clone());
        let pushed = path.insert(own.clone());
        let mut children = Vec::new();
        for term in &entry.support {
            let key = (term.source_rule.clone(), term.base_fact_id.clone());
            let derivations = self.by_fact.get(&key);
            let leaf = |what: &str| self.leaf(&term.source_rule, derivations, what);
            if derivations.is_none() {
                // Every rule of the run records provenance, so this is a fact
                // no derivation was recorded for: shown, not hidden.
                children.push(leaf("(not recorded)"));
            } else if path.contains(&key) {
                children.push(leaf("(cycle)"));
            } else if depth == 0 {
                children.push(leaf("(depth limit)"));
            } else if !expanded.insert(key) {
                children.push(leaf("(derived above)"));
            } else {
                for &d in derivations.into_iter().flatten() {
                    children.push(self.node(d, path, expanded, depth - 1));
                }
            }
        }
        if pushed {
            path.remove(&own);
        }
        DerivationNode {
            rule: entry.rule_name.clone(),
            clause_index: entry.clause_index,
            priority: rule
                .and_then(|r| r.clauses.get(entry.clause_index))
                .and_then(|c| c.priority)
                .or_else(|| rule.and_then(|r| r.priority)),
            along_values: rule.map_or_else(HashMap::new, |r| extract_along_values(&row, r)),
            bindings: row,
            children,
            graph_fact: Some(if approximate {
                format!("[APPROXIMATE] {fact}")
            } else {
                fact
            }),
            approximate,
            proof_probability: entry.proof_probability,
            // Phase C B1–B3: surface the per-call neural metadata
            // captured by `collect_neural_calls_for_row` during
            // fixpoint into the EXPLAIN derivation tree.
            neural_calls: entry.neural_calls.clone(),
        }
    }

    /// A leaf for a referenced fact of `rule` that is not expanded here,
    /// carrying the bindings of its first derivation, if any.
    fn leaf(&self, rule: &str, derivations: Option<&Vec<usize>>, what: &str) -> DerivationNode {
        let bindings = derivations
            .and_then(|d| d.first())
            .map(|&d| visible_row(&self.entries[d].fact_row))
            .unwrap_or_default();
        DerivationNode {
            rule: rule.to_string(),
            clause_index: 0,
            priority: self.program.rule_catalog.get(rule).and_then(|r| r.priority),
            bindings,
            along_values: HashMap::new(),
            children: Vec::new(),
            graph_fact: Some(what.to_string()),
            approximate: false,
            proof_probability: None,
            neural_calls: Vec::new(),
        }
    }
}

/// Mode B: re-execution fallback — re-executes clause queries to find which
/// clause produced each matching fact, then recurses into IS references.
async fn explain_rule_mode_b(
    query: &ExplainRule,
    program: &CompiledProgram,
    fact_source: &dyn DerivedFactSource,
    config: &LocyConfig,
    derived_store: &RowStore,
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

    // Read the facts the fixpoint published, already hydrated: node VIDs into
    // nodes and relationship EIDs into edges (`enrich_vids_with_nodes`). Mode
    // B used to re-derive the rule through the in-memory evaluator instead,
    // only because edge EIDs were not hydrated; that evaluator has no FOLD,
    // BEST BY, post-FOLD WHERE or set semantics, so its tree could explain
    // facts the program never derived.
    // Get all derived facts for this rule
    let facts = derived_store
        .get(&rule_name)
        .map(|r| r.rows.clone())
        .unwrap_or_default();

    // Filter facts by WHERE expression
    let filtered: Vec<FactRow> = if let Some(where_expr) = &query.where_expr {
        facts
            .into_iter()
            .map(|row| Ok(eval_condition(where_expr, &row, "EXPLAIN RULE WHERE")?.then_some(row)))
            .filter_map(Result::transpose)
            .collect::<Result<_, LocyError>>()?
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
    derived_store: &'a RowStore,
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

        // Try each clause to find the one that produced this fact, highest
        // PRIORITY first: PRIORITY keeps, per KEY, the rows of the
        // highest-priority clause that derived it, so a lower one that also
        // derives the KEY did not produce the fact. In text order, the first
        // clause deriving the KEY was taken, whichever PRIORITY kept.
        let mut clause_order: Vec<usize> = (0..rule.clauses.len()).collect();
        clause_order
            .sort_by_key(|&i| std::cmp::Reverse(rule.clauses[i].priority.unwrap_or(i64::MIN)));
        for clause_idx in clause_order {
            let clause = &rule.clauses[clause_idx];
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
                }) && yields_match(clause, row, fact)
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
                            // The column each subject, then the target, binds
                            // (`uni_locy::is_binding`), as in evaluation.
                            let ref_binding_columns =
                                uni_locy::is_binding::binding_column_names(&ref_rule.yield_schema);

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
                                                ref_binding_columns.get(i),
                                            )
                                        });
                                    let target_matches =
                                        is_ref.target.as_ref().is_none_or(|target| {
                                            binding_matches_key(
                                                evidence_row,
                                                fact,
                                                target,
                                                ref_fact,
                                                ref_binding_columns.get(is_ref.subjects.len()),
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

/// Whether `row`, a body row of `clause`, yields `fact`'s values beyond its
/// KEY: every non-KEY column the clause yields straight from its body
/// (`e.w AS w`) must equal the fact's.
///
/// Matched on the KEY alone, the first body row sharing the fact's KEY
/// explained it, even when BEST BY kept another row of that KEY (or the
/// rule's facts differ in a non-KEY column): the tree then showed a
/// derivation of another fact. A FOLD output and a PROB value are not
/// compared (a body row carries neither: a fold aggregates rows, and a PROB
/// value has referenced probabilities multiplied in), nor is a column whose
/// expression the body row cannot evaluate (one reading an ALONG value or a
/// referenced fact's column).
fn yields_match(clause: &uni_locy::types::CompiledClause, row: &FactRow, fact: &FactRow) -> bool {
    let uni_cypher::locy_ast::RuleOutput::Yield(yield_clause) = &clause.output else {
        return true;
    };
    let fold_outputs: HashSet<&str> = clause.fold.iter().map(|f| f.name.as_str()).collect();
    yield_clause.items.iter().all(|item| {
        if item.is_key || item.is_prob {
            return true;
        }
        let name = match (&item.alias, &item.expr) {
            (Some(alias), _) => alias.as_str(),
            (None, uni_cypher::ast::Expr::Variable(v)) => v.as_str(),
            _ => return true,
        };
        if fold_outputs.contains(name) {
            return true;
        }
        let Some(expected) = fact.get(name) else {
            return true;
        };
        match super::locy_eval::eval_expr(&item.expr, row) {
            Ok(value) => {
                matches!((&value, expected), (Value::Null, Value::Null))
                    || values_equal_for_join(&value, expected)
            }
            Err(_) => true,
        }
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
            fact_key: Vec::new(),
            derivation: rule.as_bytes().to_vec(),
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

    /// A derivation recorded again replaces its earlier record: it does not
    /// take a second slot, and its new weight decides its rank.
    #[test]
    fn record_top_k_replaces_a_rerecorded_derivation() {
        let store = ProvenanceStore::new();
        let hash = b"fact1".to_vec();
        store.record_top_k(hash.clone(), make_annotation("a", Some(0.2)), 2);
        store.record_top_k(hash.clone(), make_annotation("b", Some(0.5)), 2);
        store.record_top_k(hash.clone(), make_annotation("a", Some(0.9)), 2);
        let all = store.lookup_all(&hash).unwrap();
        let ranked: Vec<(&str, Option<f64>)> = all
            .iter()
            .map(|e| (e.rule_name.as_str(), e.proof_probability))
            .collect();
        assert_eq!(ranked, vec![("a", Some(0.9)), ("b", Some(0.5))]);
    }

    fn term(prob: f64) -> ProofTerm {
        ProofTerm {
            source_rule: "r".to_string(),
            base_fact_id: b"f".to_vec(),
            prob,
        }
    }

    #[test]
    fn compute_proof_probability_is_the_rows_own_weight() {
        // 0.5 local x 0.8 referenced: the row's PROB value already holds it.
        let prob = compute_proof_probability(true, Some(0.4), &[term(0.8)]);
        assert_eq!(prob, Some(0.4));
    }

    #[test]
    fn compute_proof_probability_of_a_leaf_is_its_own_value() {
        assert_eq!(compute_proof_probability(true, Some(0.9), &[]), Some(0.9));
        // A NULL PROB value is no weight.
        assert_eq!(compute_proof_probability(true, None, &[]), None);
    }

    #[test]
    fn compute_proof_probability_without_a_prob_column_multiplies_the_references() {
        let prob = compute_proof_probability(false, None, &[term(0.3), term(0.5)]);
        assert!((prob.unwrap() - 0.15).abs() < 1e-12);
        // A certain fact: referencing nothing, or only certain facts.
        assert_eq!(compute_proof_probability(false, None, &[]), Some(1.0));
        assert_eq!(
            compute_proof_probability(false, None, &[term(1.0), term(1.0)]),
            Some(1.0)
        );
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
