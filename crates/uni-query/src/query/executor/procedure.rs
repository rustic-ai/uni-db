// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::core::*;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use indexmap::IndexMap;
use std::collections::HashMap;
use uni_algo::algo::procedures::AlgoContext;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::{DistanceMetric, SchemaManager};
use uni_cypher::ast::Expr;
use uni_store::QueryContext;
use uni_store::embedding::create_embedding_service;
use uni_store::runtime::property_manager::PropertyManager;

/// Value type for procedure parameters and outputs.
#[derive(Debug, Clone, PartialEq)]
pub enum ProcedureValueType {
    /// Cypher STRING type.
    String,
    /// Cypher INTEGER type.
    Integer,
    /// Cypher FLOAT type.
    Float,
    /// Cypher NUMBER type (accepts both INTEGER and FLOAT).
    Number,
    /// Cypher BOOLEAN type.
    Boolean,
    /// Accepts any value type.
    Any,
}

/// Single parameter declaration for a registered procedure.
#[derive(Debug, Clone)]
pub struct ProcedureParam {
    /// Parameter name as declared in the procedure signature.
    pub name: String,
    /// Expected type for this parameter.
    pub param_type: ProcedureValueType,
}

/// Single output column declaration for a registered procedure.
#[derive(Debug, Clone)]
pub struct ProcedureOutput {
    /// Output column name as declared in the procedure signature.
    pub name: String,
    /// Type of the output column.
    pub output_type: ProcedureValueType,
}

/// A procedure registered at runtime with static mock data.
///
/// Used by the TCK harness to define test procedures that the query
/// engine can call via `CALL proc.name(args) YIELD columns`.
#[derive(Debug, Clone)]
pub struct RegisteredProcedure {
    /// Fully qualified procedure name (e.g. `test.my.proc`).
    pub name: String,
    /// Declared input parameters.
    pub params: Vec<ProcedureParam>,
    /// Declared output columns.
    pub outputs: Vec<ProcedureOutput>,
    /// Mock data rows keyed by column name.
    pub data: Vec<HashMap<String, Value>>,
}

/// Thread-safe registry of test procedures.
///
/// Procedures are registered before query execution (typically by TCK
/// step definitions) and looked up by the executor at runtime.
#[derive(Debug, Default)]
pub struct ProcedureRegistry {
    procedures: std::sync::RwLock<HashMap<String, RegisteredProcedure>>,
}

impl ProcedureRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a procedure, replacing any existing one with the same name.
    pub fn register(&self, proc_def: RegisteredProcedure) {
        self.procedures
            .write()
            .expect("ProcedureRegistry lock poisoned")
            .insert(proc_def.name.clone(), proc_def);
    }

    /// Looks up a procedure by fully qualified name.
    pub fn get(&self, name: &str) -> Option<RegisteredProcedure> {
        self.procedures
            .read()
            .expect("ProcedureRegistry lock poisoned")
            .get(name)
            .cloned()
    }

    /// Removes all registered procedures.
    pub fn clear(&self) {
        self.procedures
            .write()
            .expect("ProcedureRegistry lock poisoned")
            .clear();
    }
}

/// Calculate normalized score from distance based on the distance metric.
fn calculate_score(
    distance: f32,
    schema_manager: &SchemaManager,
    label: &str,
    property: &str,
) -> Result<f64> {
    // Get distance metric from schema
    let schema = schema_manager.schema();
    let metric = schema
        .vector_index_for_property(label, property)
        .map(|config| config.metric.clone())
        .unwrap_or(DistanceMetric::L2);

    let score = match metric {
        DistanceMetric::L2 => 1.0 / (1.0 + distance as f64),
        DistanceMetric::Cosine => {
            // Cosine distance is in [0, 2] range
            // Map to [1, 0] to get similarity
            (2.0 - distance as f64) / 2.0
        }
        DistanceMetric::Dot => distance as f64, // Raw, unbounded
        _ => 1.0 / (1.0 + distance as f64),     // Fallback to L2
    };

    Ok(score)
}

/// Filters a full result map to only the requested yield items.
/// If `yield_items` is empty, returns the full result unchanged.
fn filter_yield_items(
    full_result: HashMap<String, Value>,
    yield_items: &[String],
) -> HashMap<String, Value> {
    if yield_items.is_empty() {
        return full_result;
    }
    yield_items
        .iter()
        .filter_map(|name| full_result.get(name).map(|val| (name.clone(), val.clone())))
        .collect()
}

/// Reciprocal Rank Fusion (RRF) for combining search results.
/// RRF score = sum(1 / (k + rank)) for each result list.
fn fuse_rrf(vec_results: &[(Vid, f32)], fts_results: &[(Vid, f32)], k: usize) -> Vec<(Vid, f32)> {
    let mut scores: HashMap<Vid, f32> = HashMap::new();

    // Add RRF scores from vector results
    for (rank, (vid, _)) in vec_results.iter().enumerate() {
        let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
        *scores.entry(*vid).or_default() += rrf_score;
    }

    // Add RRF scores from FTS results
    for (rank, (vid, _)) in fts_results.iter().enumerate() {
        let rrf_score = 1.0 / (k as f32 + rank as f32 + 1.0);
        *scores.entry(*vid).or_default() += rrf_score;
    }

    // Sort by fused score descending
    let mut results: Vec<_> = scores.into_iter().collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    results
}

/// Weighted fusion: alpha * vec_score + (1 - alpha) * fts_score
/// Both score sets should be normalized to [0, 1] range.
fn fuse_weighted(
    vec_results: &[(Vid, f32)],
    fts_results: &[(Vid, f32)],
    alpha: f32,
) -> Vec<(Vid, f32)> {
    // Normalize vector scores (distance -> similarity)
    let vec_max = vec_results.iter().map(|(_, s)| *s).fold(f32::MIN, f32::max);
    let vec_min = vec_results.iter().map(|(_, s)| *s).fold(f32::MAX, f32::min);
    let vec_range = if vec_max > vec_min {
        vec_max - vec_min
    } else {
        1.0
    };

    // Normalize FTS scores
    let fts_max = fts_results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);

    let vec_scores: HashMap<Vid, f32> = vec_results
        .iter()
        .map(|(vid, dist)| {
            // Convert distance to similarity (1 - normalized distance)
            let norm = 1.0 - (dist - vec_min) / vec_range;
            (*vid, norm)
        })
        .collect();

    let fts_scores: HashMap<Vid, f32> = fts_results
        .iter()
        .map(|(vid, score)| {
            let norm = if fts_max > 0.0 { score / fts_max } else { 0.0 };
            (*vid, norm)
        })
        .collect();

    // Combine all VIDs
    let all_vids: std::collections::HashSet<Vid> = vec_scores
        .keys()
        .chain(fts_scores.keys())
        .cloned()
        .collect();

    // Compute weighted scores
    let mut results: Vec<(Vid, f32)> = all_vids
        .into_iter()
        .map(|vid| {
            let vec_score = *vec_scores.get(&vid).unwrap_or(&0.0);
            let fts_score = *fts_scores.get(&vid).unwrap_or(&0.0);
            let fused = alpha * vec_score + (1.0 - alpha) * fts_score;
            (vid, fused)
        })
        .collect();

    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    results
}

impl Executor {
    /// Maps a user-provided yield name to a canonical name.
    ///
    /// # Mapping Rules
    ///
    /// - "vid", "_vid" → "vid"
    /// - "distance", "dist", "_distance" → "distance"
    /// - "score", "_score" → "score"
    /// - anything else → "node" (treated as node variable)
    fn map_yield_to_canonical(yield_name: &str) -> &'static str {
        match yield_name.to_lowercase().as_str() {
            "vid" | "_vid" => "vid",
            "distance" | "dist" | "_distance" => "distance",
            "score" | "_score" => "score",
            _ => "node",
        }
    }

    /// Evaluate a procedure argument as a string, returning an error with the given description.
    async fn eval_string_arg<'a>(
        &'a self,
        arg: &Expr,
        description: &str,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<String> {
        let empty_row = HashMap::new();
        self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
            .await?
            .as_str()
            .ok_or_else(|| anyhow!("{} must be string", description))
            .map(|s| s.to_string())
    }

    /// Evaluate a procedure argument as a usize integer.
    async fn eval_usize_arg<'a>(
        &'a self,
        arg: &Expr,
        description: &str,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<usize> {
        let empty_row = HashMap::new();
        self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
            .await?
            .as_u64()
            .ok_or_else(|| anyhow!("{} must be integer", description))
            .map(|v| v as usize)
    }

    /// Extract an optional string filter argument at the given index.
    ///
    /// Returns `None` if the argument index is out of bounds or the value is null.
    /// Returns an error if the value is present but not a string, or is an empty string.
    async fn eval_optional_filter_arg<'a>(
        &'a self,
        args: &[Expr],
        index: usize,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Option<String>> {
        if args.len() <= index {
            return Ok(None);
        }
        let empty_row = HashMap::new();
        let filter_val = self
            .evaluate_expr(&args[index], &empty_row, prop_manager, params, ctx)
            .await?;
        if filter_val.is_null() {
            return Ok(None);
        }
        let filter_str = filter_val
            .as_str()
            .ok_or_else(|| anyhow!("Filter must be a string"))?
            .to_string();
        if filter_str.trim().is_empty() {
            return Err(anyhow!("Filter cannot be empty string"));
        }
        Ok(Some(filter_str))
    }

    /// Extract an optional non-negative threshold argument at the given index.
    ///
    /// Returns `None` if the argument index is out of bounds or the value is null.
    /// Returns an error if the value is present but not a number, or is negative.
    async fn eval_optional_threshold_arg<'a>(
        &'a self,
        args: &[Expr],
        index: usize,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Option<f64>> {
        if args.len() <= index {
            return Ok(None);
        }
        let empty_row = HashMap::new();
        let threshold_val = self
            .evaluate_expr(&args[index], &empty_row, prop_manager, params, ctx)
            .await?;
        if threshold_val.is_null() {
            return Ok(None);
        }
        let thresh = threshold_val
            .as_f64()
            .ok_or_else(|| anyhow!("Threshold must be a number"))?;
        if thresh < 0.0 {
            return Err(anyhow!("Threshold must be non-negative, got {}", thresh));
        }
        Ok(Some(thresh))
    }

    /// Load a node object with all vertex properties for a given VID and label.
    ///
    /// Returns `None` if the vertex has been deleted. The returned map contains
    /// `_vid`, `_labels`, and all vertex properties.
    async fn load_node_object<'a>(
        vid: Vid,
        label: &str,
        prop_manager: &'a PropertyManager,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Option<Value>> {
        let props_opt = prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await?;
        let Some(properties) = props_opt else {
            return Ok(None);
        };
        let mut node_obj = HashMap::new();
        node_obj.insert("_vid".to_string(), Value::Int(vid.as_u64() as i64));
        node_obj.insert(
            "_labels".to_string(),
            Value::List(vec![Value::String(label.to_string())]),
        );
        for (key, val) in properties {
            node_obj.insert(key, val);
        }
        Ok(Some(Value::Map(node_obj)))
    }

    pub(crate) async fn execute_procedure<'a>(
        &'a self,
        name: &str,
        args: &[Expr],
        yield_items: &[String],
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        if name.starts_with("uni.algo.") {
            // Dispatch to AlgorithmRegistry
            if let Some(procedure) = self.algo_registry.get(name) {
                let empty_row = HashMap::new();
                let mut evaluated_args = Vec::with_capacity(args.len());
                for arg in args {
                    evaluated_args.push(
                        self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
                            .await?,
                    );
                }

                // Extract L0Manager from Writer if available, otherwise from standalone field
                let l0_mgr = if let Some(writer_lock) = &self.writer {
                    let writer = writer_lock.read().await;
                    Some(writer.l0_manager.clone())
                } else {
                    self.l0_manager.clone()
                };
                let algo_ctx = AlgoContext::new(self.storage.clone(), l0_mgr);

                let signature = procedure.signature();
                // Convert uni_common::Value args to serde_json::Value for algo crate
                let serde_args: Vec<serde_json::Value> =
                    evaluated_args.into_iter().map(|v| v.into()).collect();
                let mut stream = procedure.execute(algo_ctx, serde_args);
                let mut results = Vec::new();
                let mut row_count = 0usize;

                while let Some(row_res) = stream.next().await {
                    // CWE-400: Check timeout periodically during algorithm execution
                    row_count += 1;
                    if row_count.is_multiple_of(Self::AGGREGATE_TIMEOUT_CHECK_INTERVAL)
                        && let Some(ctx) = ctx
                    {
                        ctx.check_timeout()?;
                    }

                    let row = row_res?;
                    let mut result_map = HashMap::new();

                    for yield_name in yield_items {
                        if let Some(idx) =
                            signature.yields.iter().position(|(n, _)| *n == yield_name)
                            && idx < row.values.len()
                        {
                            // Convert serde_json::Value from algo crate to uni_common::Value
                            result_map
                                .insert(yield_name.clone(), Value::from(row.values[idx].clone()));
                        }
                    }
                    results.push(result_map);
                }

                return Ok(results);
            }
        }

        match name {
            // uni.vector.query: Supports both pre-computed vectors and auto-embedding from text
            "uni.vector.query" => {
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;
                let property = self
                    .eval_string_arg(&args[1], "Property", prop_manager, params, ctx)
                    .await?;
                let empty_row = HashMap::new();
                let query_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;
                let k = self
                    .eval_usize_arg(&args[3], "k", prop_manager, params, ctx)
                    .await?;

                // Detect if query is text (string) or pre-computed vector (array)
                let query_vector: Vec<f32> = if let Some(query_text) = query_val.as_str() {
                    // Auto-embed: lookup index's embedding_config and embed the text
                    let schema = self.storage.schema_manager().schema();
                    let index_config = schema.vector_index_for_property(&label, &property);

                    let embedding_config = index_config
                        .and_then(|cfg| cfg.embedding_config.as_ref())
                        .ok_or_else(|| {
                            anyhow!(
                                "Cannot auto-embed: vector index for {}.{} has no embedding_config. \
                                 Either provide a pre-computed vector or create the index with embedding options.",
                                label,
                                property
                            )
                        })?;

                    let service = create_embedding_service(&embedding_config.model)?;
                    let embeddings = service.embed(&[query_text]).await?;
                    embeddings
                        .into_iter()
                        .next()
                        .ok_or_else(|| anyhow!("Embedding service returned no results"))?
                } else {
                    Vec::<f32>::try_from(&query_val)?
                };

                let filter = self
                    .eval_optional_filter_arg(args, 4, prop_manager, params, ctx)
                    .await?;
                let threshold = self
                    .eval_optional_threshold_arg(args, 5, prop_manager, params, ctx)
                    .await?;

                let mut results = self
                    .storage
                    .vector_search(&label, &property, &query_vector, k, filter.as_deref(), ctx)
                    .await?;

                if let Some(max_dist) = threshold {
                    results.retain(|(_, dist)| *dist <= max_dist as f32);
                }

                let mut matches = Vec::new();
                let schema_manager = self.storage.schema_manager();

                for (vid, dist) in results {
                    let mut canonical_yields: IndexMap<String, Value> = IndexMap::new();
                    let score = calculate_score(dist, schema_manager, &label, &property)?;

                    canonical_yields.insert("vid".to_string(), Value::Int(vid.as_u64() as i64));
                    canonical_yields.insert("distance".to_string(), Value::Float(dist as f64));
                    canonical_yields.insert("score".to_string(), Value::Float(score));

                    let mut node_obj_opt = None;
                    let mut result = HashMap::new();
                    for yield_name in yield_items {
                        let canonical_name = Self::map_yield_to_canonical(yield_name);

                        if canonical_name == "node" {
                            if node_obj_opt.is_none() {
                                node_obj_opt =
                                    Self::load_node_object(vid, &label, prop_manager, ctx).await?;
                                if node_obj_opt.is_none() {
                                    continue; // Skip deleted vertices
                                }
                            }
                            result.insert(yield_name.to_lowercase(), node_obj_opt.clone().unwrap());
                        } else if let Some(val) = canonical_yields.get(canonical_name) {
                            result.insert(yield_name.to_lowercase(), val.clone());
                        }
                    }

                    matches.push(result);
                }

                Ok(matches)
            }

            // Full-text search with BM25 scoring
            // Signature: uni.fts.query(label, property, search_term, k, filter?, threshold?)
            // YIELD vid, score, node
            "uni.fts.query" => {
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;
                let property = self
                    .eval_string_arg(&args[1], "Property", prop_manager, params, ctx)
                    .await?;
                let search_term = self
                    .eval_string_arg(&args[2], "Search term", prop_manager, params, ctx)
                    .await?;
                let k = self
                    .eval_usize_arg(&args[3], "k", prop_manager, params, ctx)
                    .await?;
                let filter = self
                    .eval_optional_filter_arg(args, 4, prop_manager, params, ctx)
                    .await?;
                let threshold = self
                    .eval_optional_threshold_arg(args, 5, prop_manager, params, ctx)
                    .await?;

                let mut results = self
                    .storage
                    .fts_search(&label, &property, &search_term, k, filter.as_deref(), ctx)
                    .await?;

                if let Some(min_score) = threshold {
                    results.retain(|(_, score)| *score as f64 >= min_score);
                }

                let max_score = results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);
                let mut matches = Vec::new();

                for (vid, raw_score) in results {
                    let mut canonical_yields: IndexMap<String, Value> = IndexMap::new();
                    let normalized_score = if max_score > 0.0 {
                        raw_score / max_score
                    } else {
                        0.0
                    };

                    canonical_yields.insert("vid".to_string(), Value::Int(vid.as_u64() as i64));
                    canonical_yields
                        .insert("score".to_string(), Value::Float(normalized_score as f64));
                    canonical_yields
                        .insert("raw_score".to_string(), Value::Float(raw_score as f64));

                    let mut node_obj_opt = None;
                    let mut result = HashMap::new();
                    for yield_name in yield_items {
                        let canonical_name = Self::map_yield_to_canonical(yield_name);

                        if canonical_name == "node" {
                            if node_obj_opt.is_none() {
                                node_obj_opt =
                                    Self::load_node_object(vid, &label, prop_manager, ctx).await?;
                                if node_obj_opt.is_none() {
                                    continue;
                                }
                            }
                            result.insert(yield_name.to_lowercase(), node_obj_opt.clone().unwrap());
                        } else if yield_name.to_lowercase() == "raw_score" {
                            result.insert("raw_score".to_string(), Value::Float(raw_score as f64));
                        } else if let Some(val) = canonical_yields.get(canonical_name) {
                            result.insert(yield_name.to_lowercase(), val.clone());
                        }
                    }

                    matches.push(result);
                }

                Ok(matches)
            }

            // Hybrid search combining vector + FTS with fusion
            // Signature: uni.search(label, properties, query_text, query_vector?, k, filter?, options?)
            // properties format: {vector: 'embedding', fts: 'content'} or just 'embedding' for vector-only
            // options: {method: 'rrf', alpha: 0.5, over_fetch: 2.0}
            // YIELD vid, score, vector_score, fts_score, node
            "uni.search" => {
                let empty_row = HashMap::new();
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;

                let properties_val = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;

                let (vector_prop, fts_prop) = if let Some(obj) = properties_val.as_object() {
                    let vec_prop = obj
                        .get("vector")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let fts_prop = obj
                        .get("fts")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    (vec_prop, fts_prop)
                } else if let Some(prop) = properties_val.as_str() {
                    // Shorthand: just property name means both vector and FTS
                    (Some(prop.to_string()), Some(prop.to_string()))
                } else {
                    return Err(anyhow!(
                        "Properties must be an object {{vector: '...', fts: '...'}} or a string"
                    ));
                };

                let query_text = self
                    .eval_string_arg(&args[2], "Query text", prop_manager, params, ctx)
                    .await?;

                // Arg 3: query vector (optional, can be null)
                let query_vector: Option<Vec<f32>> = if args.len() > 3 {
                    let vec_val = self
                        .evaluate_expr(&args[3], &empty_row, prop_manager, params, ctx)
                        .await?;
                    vec_val.as_array().map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect()
                    })
                } else {
                    None
                };

                let k = self
                    .eval_usize_arg(&args[4], "k", prop_manager, params, ctx)
                    .await?;

                // Arg 5: filter (optional, less strict than vector/fts - allows empty/any type)
                let filter = if args.len() > 5 {
                    let filter_val = self
                        .evaluate_expr(&args[5], &empty_row, prop_manager, params, ctx)
                        .await?;
                    if filter_val.is_null() {
                        None
                    } else {
                        filter_val.as_str().map(|s| s.to_string())
                    }
                } else {
                    None
                };

                // Arg 6: options (optional)
                let options_val = if args.len() > 6 {
                    self.evaluate_expr(&args[6], &empty_row, prop_manager, params, ctx)
                        .await?
                } else {
                    Value::Null
                };

                let options_map = options_val.as_object();
                let fusion_method = options_map
                    .and_then(|m| m.get("method"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("rrf")
                    .to_string();
                let alpha = options_map
                    .and_then(|m| m.get("alpha"))
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.5) as f32;
                let over_fetch_factor = options_map
                    .and_then(|m| m.get("over_fetch"))
                    .and_then(|v| v.as_f64())
                    .unwrap_or(2.0) as f32;
                let rrf_k = options_map
                    .and_then(|m| m.get("rrf_k"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(60) as usize;

                let over_fetch_k = (k as f32 * over_fetch_factor).ceil() as usize;

                // Execute vector search if configured
                let mut vector_results: Vec<(Vid, f32)> = Vec::new();
                if let Some(ref vec_prop) = vector_prop {
                    // Get or generate query vector
                    let qvec = if let Some(ref v) = query_vector {
                        v.clone()
                    } else {
                        // Auto-embed the query text if embedding config exists
                        let schema = self.storage.schema_manager().schema();
                        let index_config = schema.vector_index_for_property(&label, vec_prop);

                        if let Some(cfg) = index_config
                            && let Some(emb_cfg) = &cfg.embedding_config
                            && let Ok(service) = create_embedding_service(&emb_cfg.model)
                        {
                            let embeddings = service.embed(&[&query_text]).await?;
                            embeddings.into_iter().next().unwrap_or_default()
                        } else {
                            Vec::new()
                        }
                    };

                    if !qvec.is_empty() {
                        vector_results = self
                            .storage
                            .vector_search(
                                &label,
                                vec_prop,
                                &qvec,
                                over_fetch_k,
                                filter.as_deref(),
                                ctx,
                            )
                            .await?;
                    }
                }

                // Execute FTS search if configured
                let mut fts_results: Vec<(Vid, f32)> = Vec::new();
                if let Some(ref fts_prop) = fts_prop {
                    fts_results = self
                        .storage
                        .fts_search(
                            &label,
                            fts_prop,
                            &query_text,
                            over_fetch_k,
                            filter.as_deref(),
                            ctx,
                        )
                        .await?;
                }

                // Fuse results based on method
                let fused_results = match fusion_method.as_str() {
                    "weighted" => {
                        // Weighted fusion: alpha * vec_score + (1 - alpha) * fts_score
                        fuse_weighted(&vector_results, &fts_results, alpha)
                    }
                    _ => {
                        // Default: RRF (Reciprocal Rank Fusion)
                        fuse_rrf(&vector_results, &fts_results, rrf_k)
                    }
                };

                // Limit to k results
                let final_results: Vec<_> = fused_results.into_iter().take(k).collect();

                // Build result maps
                let mut matches = Vec::new();
                let schema_manager = self.storage.schema_manager();

                // Build lookup maps for original scores
                let vec_score_map: HashMap<Vid, f32> = vector_results.iter().cloned().collect();
                let fts_score_map: HashMap<Vid, f32> = fts_results.iter().cloned().collect();

                // Normalize FTS scores
                let fts_max = fts_results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);

                for (vid, fused_score) in final_results {
                    let mut canonical_yields: IndexMap<String, Value> = IndexMap::new();

                    canonical_yields.insert("vid".to_string(), Value::Int(vid.as_u64() as i64));
                    canonical_yields.insert("score".to_string(), Value::Float(fused_score as f64));

                    // Vector score (normalized 0-1 via calculate_score)
                    if let Some(&dist) = vec_score_map.get(&vid) {
                        if let Some(ref vec_prop) = vector_prop {
                            let vec_score = calculate_score(dist, schema_manager, &label, vec_prop)
                                .unwrap_or(0.0);
                            canonical_yields
                                .insert("vector_score".to_string(), Value::Float(vec_score));
                        }
                    } else {
                        canonical_yields.insert("vector_score".to_string(), Value::Null);
                    }

                    // FTS score (normalized 0-1)
                    if let Some(&raw_score) = fts_score_map.get(&vid) {
                        let norm_score = if fts_max > 0.0 {
                            raw_score / fts_max
                        } else {
                            0.0
                        };
                        canonical_yields
                            .insert("fts_score".to_string(), Value::Float(norm_score as f64));
                    } else {
                        canonical_yields.insert("fts_score".to_string(), Value::Null);
                    }

                    let mut node_obj_opt = None;
                    let mut result = HashMap::new();

                    for yield_name in yield_items {
                        let canonical_name = Self::map_yield_to_canonical(yield_name);

                        if canonical_name == "node" {
                            if node_obj_opt.is_none() {
                                node_obj_opt =
                                    Self::load_node_object(vid, &label, prop_manager, ctx).await?;
                                if node_obj_opt.is_none() {
                                    continue;
                                }
                            }
                            result.insert(yield_name.to_lowercase(), node_obj_opt.clone().unwrap());
                        } else if let Some(val) = canonical_yields.get(&yield_name.to_lowercase()) {
                            result.insert(yield_name.to_lowercase(), val.clone());
                        } else if let Some(val) = canonical_yields.get(canonical_name) {
                            result.insert(yield_name.to_lowercase(), val.clone());
                        }
                    }

                    matches.push(result);
                }

                Ok(matches)
            }

            "uni.admin.compact" => {
                let stats = self.storage.compact().await?;
                let full_result = HashMap::from([
                    (
                        "files_compacted".to_string(),
                        Value::Int(stats.files_compacted as i64),
                    ),
                    (
                        "bytes_before".to_string(),
                        Value::Int(stats.bytes_before as i64),
                    ),
                    (
                        "bytes_after".to_string(),
                        Value::Int(stats.bytes_after as i64),
                    ),
                    (
                        "duration_ms".to_string(),
                        Value::Int(stats.duration.as_millis() as i64),
                    ),
                ]);

                Ok(vec![filter_yield_items(full_result, yield_items)])
            }
            "uni.admin.compactionStatus" => {
                let status = self.storage.compaction_status();
                let full_result = HashMap::from([
                    ("l1_runs".to_string(), Value::Int(status.l1_runs as i64)),
                    (
                        "l1_size_bytes".to_string(),
                        Value::Int(status.l1_size_bytes as i64),
                    ),
                    (
                        "in_progress".to_string(),
                        Value::Bool(status.compaction_in_progress),
                    ),
                    (
                        "pending".to_string(),
                        Value::Int(status.compaction_pending as i64),
                    ),
                    (
                        "total_compactions".to_string(),
                        Value::Int(status.total_compactions as i64),
                    ),
                    (
                        "total_bytes_compacted".to_string(),
                        Value::Int(status.total_bytes_compacted as i64),
                    ),
                ]);

                Ok(vec![filter_yield_items(full_result, yield_items)])
            }
            "uni.admin.snapshot.create" => {
                let name = if !args.is_empty() {
                    Some(
                        self.eval_string_arg(&args[0], "Snapshot name", prop_manager, params, ctx)
                            .await?,
                    )
                } else {
                    None
                };

                let writer_arc = self
                    .writer
                    .as_ref()
                    .ok_or_else(|| anyhow!("Database is in read-only mode"))?;
                let mut writer = writer_arc.write().await;
                let snapshot_id = writer.flush_to_l1(name).await?;

                let mut result = HashMap::new();
                result.insert("snapshot_id".to_string(), Value::String(snapshot_id));
                Ok(vec![result])
            }
            "uni.admin.snapshot.list" => {
                let sm = self.storage.snapshot_manager();
                let ids = sm.list_snapshots().await?;
                let mut results = Vec::new();
                for id in ids {
                    if let Ok(m) = sm.load_snapshot(&id).await {
                        let mut row = HashMap::new();
                        row.insert("snapshot_id".to_string(), Value::String(m.snapshot_id));
                        row.insert(
                            "name".to_string(),
                            m.name.map(Value::String).unwrap_or(Value::Null),
                        );
                        row.insert(
                            "created_at".to_string(),
                            Value::String(m.created_at.to_rfc3339()),
                        );
                        row.insert(
                            "version_hwm".to_string(),
                            Value::Int(m.version_high_water_mark as i64),
                        );
                        results.push(row);
                    }
                }
                Ok(results)
            }
            "uni.admin.snapshot.restore" => {
                let id = self
                    .eval_string_arg(&args[0], "Snapshot ID", prop_manager, params, ctx)
                    .await?;

                self.storage
                    .snapshot_manager()
                    .set_latest_snapshot(&id)
                    .await?;
                let mut result = HashMap::new();
                result.insert("status".to_string(), Value::String("Restored".to_string()));
                Ok(vec![result])
            }
            "uni.schema.labels" => {
                let schema = self.storage.schema_manager().schema();
                let mut results = Vec::new();
                for label_name in schema.labels.keys() {
                    let mut row = HashMap::new();
                    row.insert("label".to_string(), Value::String(label_name.clone()));

                    let prop_count = schema
                        .properties
                        .get(label_name)
                        .map(|p| p.len())
                        .unwrap_or(0);
                    row.insert("propertyCount".to_string(), Value::Int(prop_count as i64));

                    let node_count = if let Ok(ds) = self.storage.vertex_dataset(label_name) {
                        if let Ok(raw) = ds.open_raw().await {
                            raw.count_rows(None).await.unwrap_or(0)
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                    row.insert("nodeCount".to_string(), Value::Int(node_count as i64));

                    let idx_count = schema
                        .indexes
                        .iter()
                        .filter(|i| i.label() == label_name)
                        .count();
                    row.insert("indexCount".to_string(), Value::Int(idx_count as i64));

                    results.push(row);
                }
                Ok(results)
            }
            "uni.schema.edgeTypes" | "uni.schema.relationshipTypes" => {
                let schema = self.storage.schema_manager().schema();
                let mut results = Vec::new();
                for (type_name, meta) in &schema.edge_types {
                    let mut row = HashMap::new();
                    row.insert("type".to_string(), Value::String(type_name.clone()));
                    row.insert(
                        "relationshipType".to_string(),
                        Value::String(type_name.clone()),
                    ); // Alias
                    row.insert(
                        "sourceLabels".to_string(),
                        Value::List(
                            meta.src_labels
                                .iter()
                                .map(|s| Value::String(s.clone()))
                                .collect(),
                        ),
                    );
                    row.insert(
                        "targetLabels".to_string(),
                        Value::List(
                            meta.dst_labels
                                .iter()
                                .map(|s| Value::String(s.clone()))
                                .collect(),
                        ),
                    );

                    let prop_count = schema
                        .properties
                        .get(type_name)
                        .map(|p| p.len())
                        .unwrap_or(0);
                    row.insert("propertyCount".to_string(), Value::Int(prop_count as i64));

                    results.push(row);
                }
                Ok(results)
            }
            "uni.schema.indexes" => {
                let schema = self.storage.schema_manager().schema();
                let mut results = Vec::new();
                for idx in schema.indexes {
                    let mut row = HashMap::new();
                    // Defaults
                    row.insert("state".to_string(), Value::String("ONLINE".to_string()));

                    match idx {
                        uni_common::core::schema::IndexDefinition::Vector(v) => {
                            row.insert("name".to_string(), Value::String(v.name));
                            row.insert("type".to_string(), Value::String("VECTOR".to_string()));
                            row.insert("label".to_string(), Value::String(v.label));
                            row.insert(
                                "properties".to_string(),
                                Value::List(vec![Value::String(v.property)]),
                            );
                        }
                        uni_common::core::schema::IndexDefinition::FullText(f) => {
                            row.insert("name".to_string(), Value::String(f.name));
                            row.insert("type".to_string(), Value::String("FULLTEXT".to_string()));
                            row.insert("label".to_string(), Value::String(f.label));
                            row.insert(
                                "properties".to_string(),
                                Value::List(f.properties.into_iter().map(Value::String).collect()),
                            );
                        }
                        uni_common::core::schema::IndexDefinition::Scalar(s) => {
                            row.insert("name".to_string(), Value::String(s.name));
                            row.insert("type".to_string(), Value::String("SCALAR".to_string()));
                            row.insert("label".to_string(), Value::String(s.label));
                            row.insert(
                                "properties".to_string(),
                                Value::List(s.properties.into_iter().map(Value::String).collect()),
                            );
                        }
                        uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                            row.insert("name".to_string(), Value::String(j.name));
                            row.insert("type".to_string(), Value::String("JSON_FTS".to_string()));
                            row.insert("label".to_string(), Value::String(j.label));
                            row.insert(
                                "properties".to_string(),
                                Value::List(vec![Value::String(j.column)]),
                            );
                        }
                        uni_common::core::schema::IndexDefinition::Inverted(inv) => {
                            row.insert("name".to_string(), Value::String(inv.name));
                            row.insert("type".to_string(), Value::String("INVERTED".to_string()));
                            row.insert("label".to_string(), Value::String(inv.label));
                            row.insert(
                                "properties".to_string(),
                                Value::List(vec![Value::String(inv.property)]),
                            );
                        }
                        _ => {
                            row.insert("name".to_string(), Value::String("UNKNOWN".to_string()));
                            row.insert("type".to_string(), Value::String("UNKNOWN".to_string()));
                        }
                    }
                    results.push(row);
                }
                Ok(results)
            }
            "uni.schema.constraints" => {
                let schema = self.storage.schema_manager().schema();
                let mut results = Vec::new();
                for c in schema.constraints {
                    let mut row = HashMap::new();
                    row.insert("name".to_string(), Value::String(c.name));
                    row.insert("enabled".to_string(), Value::Bool(c.enabled));

                    match c.constraint_type {
                        uni_common::core::schema::ConstraintType::Unique { properties } => {
                            row.insert("type".to_string(), Value::String("UNIQUE".to_string()));
                            row.insert(
                                "properties".to_string(),
                                Value::List(properties.into_iter().map(Value::String).collect()),
                            );
                        }
                        uni_common::core::schema::ConstraintType::Exists { property } => {
                            row.insert("type".to_string(), Value::String("EXISTS".to_string()));
                            row.insert(
                                "properties".to_string(),
                                Value::List(vec![Value::String(property)]),
                            );
                        }
                        uni_common::core::schema::ConstraintType::Check { expression } => {
                            row.insert("type".to_string(), Value::String("CHECK".to_string()));
                            row.insert("expression".to_string(), Value::String(expression));
                        }
                        _ => {
                            row.insert("type".to_string(), Value::String("UNKNOWN".to_string()));
                        }
                    }

                    match c.target {
                        uni_common::core::schema::ConstraintTarget::Label(l) => {
                            row.insert("label".to_string(), Value::String(l));
                        }
                        uni_common::core::schema::ConstraintTarget::EdgeType(t) => {
                            row.insert("relationshipType".to_string(), Value::String(t));
                        }
                        _ => {
                            row.insert("target".to_string(), Value::String("UNKNOWN".to_string()));
                        }
                    }

                    results.push(row);
                }
                Ok(results)
            }
            "uni.schema.labelInfo" => {
                let schema = self.storage.schema_manager().schema();
                let label_name = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;

                let mut results = Vec::new();
                if let Some(props) = schema.properties.get(&label_name) {
                    for (prop_name, prop_meta) in props {
                        let mut row = HashMap::new();
                        row.insert("property".to_string(), Value::String(prop_name.clone()));
                        row.insert(
                            "dataType".to_string(),
                            Value::String(format!("{:?}", prop_meta.r#type)),
                        );
                        row.insert("nullable".to_string(), Value::Bool(prop_meta.nullable));

                        let is_indexed = schema.indexes.iter().any(|idx| match idx {
                            uni_common::core::schema::IndexDefinition::Vector(v) => {
                                v.label == label_name && v.property == *prop_name
                            }
                            uni_common::core::schema::IndexDefinition::Scalar(s) => {
                                s.label == label_name && s.properties.contains(prop_name)
                            }
                            uni_common::core::schema::IndexDefinition::FullText(f) => {
                                f.label == label_name && f.properties.contains(prop_name)
                            }
                            uni_common::core::schema::IndexDefinition::Inverted(inv) => {
                                inv.label == label_name && inv.property == *prop_name
                            }
                            uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                                j.label == label_name
                            }
                            _ => false,
                        });
                        row.insert("indexed".to_string(), Value::Bool(is_indexed));

                        // Check unique constraints
                        let unique = schema.constraints.iter().any(|c| {
                            if let uni_common::core::schema::ConstraintTarget::Label(l) = &c.target
                                && l == &label_name
                                && c.enabled
                                && let uni_common::core::schema::ConstraintType::Unique {
                                    properties,
                                } = &c.constraint_type
                            {
                                return properties.contains(prop_name);
                            }
                            false
                        });
                        row.insert("unique".to_string(), Value::Bool(unique));

                        results.push(row);
                    }
                }
                Ok(results)
            }
            // DDL Procedures
            "uni.schema.createLabel" => {
                let empty_row = HashMap::new();
                let name = self
                    .eval_string_arg(&args[0], "Label name", prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;

                let success =
                    super::ddl_procedures::create_label(&self.storage, &name, &config).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.createEdgeType" => {
                let empty_row = HashMap::new();
                let name = self
                    .eval_string_arg(&args[0], "Edge type name", prop_manager, params, ctx)
                    .await?;
                let src_val = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;
                let dst_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[3], &empty_row, prop_manager, params, ctx)
                    .await?;

                // Convert src/dst to Vec<String>
                let src_labels = src_val
                    .as_array()
                    .ok_or(anyhow!("Source labels must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Label must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let dst_labels = dst_val
                    .as_array()
                    .ok_or(anyhow!("Target labels must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Label must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let success = super::ddl_procedures::create_edge_type(
                    &self.storage,
                    &name,
                    src_labels,
                    dst_labels,
                    &config,
                )
                .await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.createIndex" => {
                let empty_row = HashMap::new();
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;
                let property = self
                    .eval_string_arg(&args[1], "Property", prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;

                let success =
                    super::ddl_procedures::create_index(&self.storage, &label, &property, &config)
                        .await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.createConstraint" => {
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;
                let c_type = self
                    .eval_string_arg(&args[1], "Constraint type", prop_manager, params, ctx)
                    .await?;
                let empty_row = HashMap::new();
                let props_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;

                let properties = props_val
                    .as_array()
                    .ok_or(anyhow!("Properties must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Property must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let success = super::ddl_procedures::create_constraint(
                    &self.storage,
                    &label,
                    &c_type,
                    properties,
                )
                .await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.dropLabel" => {
                let name = self
                    .eval_string_arg(&args[0], "Label name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_label(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.dropEdgeType" => {
                let name = self
                    .eval_string_arg(&args[0], "Edge type name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_edge_type(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.dropIndex" => {
                let name = self
                    .eval_string_arg(&args[0], "Index name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_index(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.dropConstraint" => {
                let name = self
                    .eval_string_arg(&args[0], "Constraint name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_constraint(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            _ => {
                // Check external procedure registry
                if let Some(registry) = &self.procedure_registry
                    && let Some(proc_def) = registry.get(name)
                {
                    return self
                        .execute_registered_procedure(
                            &proc_def,
                            args,
                            yield_items,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await;
                }
                Err(anyhow!("ProcedureNotFound: Unknown procedure '{}'", name))
            }
        }
    }

    /// Executes a procedure from the external registry.
    ///
    /// Evaluates arguments, validates count and types against the procedure
    /// declaration, filters data rows by matching input columns, and projects
    /// the requested output columns.
    ///
    /// # Errors
    ///
    /// Returns `InvalidNumberOfArguments` if the argument count is wrong,
    /// or `InvalidArgumentType` if an argument has an incompatible type.
    async fn execute_registered_procedure<'a>(
        &'a self,
        proc_def: &RegisteredProcedure,
        args: &[Expr],
        yield_items: &[String],
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let empty_row = HashMap::new();

        // Evaluate arguments
        let mut evaluated_args = Vec::with_capacity(args.len());
        for arg in args {
            evaluated_args.push(
                self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
                    .await?,
            );
        }

        // Validate argument count
        if evaluated_args.len() != proc_def.params.len() {
            if evaluated_args.is_empty() && !proc_def.params.is_empty() {
                // Standalone CALL with no YIELD is treated as missing implicit parameter(s).
                if yield_items.is_empty() {
                    return Err(anyhow!(
                        "MissingParameter: Procedure '{}' requires {} implicit argument(s)",
                        proc_def.name,
                        proc_def.params.len()
                    ));
                }
                // In-query CALL with YIELD cannot pass implicit arguments.
                return Err(anyhow!(
                    "InvalidArgumentPassingMode: Procedure '{}' requires explicit argument passing in in-query CALL",
                    proc_def.name
                ));
            }
            return Err(anyhow!(
                "InvalidNumberOfArguments: Procedure '{}' expects {} argument(s), got {}",
                proc_def.name,
                proc_def.params.len(),
                evaluated_args.len()
            ));
        }

        // Validate argument types
        for (i, (arg_val, param)) in evaluated_args.iter().zip(&proc_def.params).enumerate() {
            if !arg_val.is_null() && !check_type_compatible(arg_val, &param.param_type) {
                return Err(anyhow!(
                    "InvalidArgumentType: Argument {} ('{}') of procedure '{}' has incompatible type",
                    i,
                    param.name,
                    proc_def.name
                ));
            }
        }

        // Filter data rows: keep rows where input columns match the provided args
        let filtered: Vec<&HashMap<String, Value>> = proc_def
            .data
            .iter()
            .filter(|row| {
                for (param, arg_val) in proc_def.params.iter().zip(&evaluated_args) {
                    if let Some(row_val) = row.get(&param.name)
                        && !values_match(row_val, arg_val)
                    {
                        return false;
                    }
                }
                true
            })
            .collect();

        // Collect output column names
        let output_names: Vec<&str> = proc_def.outputs.iter().map(|o| o.name.as_str()).collect();

        // Project output columns, applying yield_items filtering
        let results = filtered
            .into_iter()
            .map(|row| {
                let mut result = HashMap::new();
                if yield_items.is_empty() {
                    // Return all output columns
                    for name in &output_names {
                        if let Some(val) = row.get(*name) {
                            result.insert((*name).to_string(), val.clone());
                        }
                    }
                } else {
                    for yield_name in yield_items {
                        if let Some(val) = row.get(yield_name.as_str()) {
                            result.insert(yield_name.clone(), val.clone());
                        }
                    }
                }
                result
            })
            .collect();

        Ok(results)
    }
}

/// Checks whether a value is compatible with a procedure type.
fn check_type_compatible(val: &Value, expected: &ProcedureValueType) -> bool {
    match expected {
        ProcedureValueType::Any => true,
        ProcedureValueType::String => val.is_string(),
        ProcedureValueType::Boolean => val.is_bool(),
        ProcedureValueType::Integer => val.is_i64(),
        ProcedureValueType::Float => val.is_f64() || val.is_i64(),
        ProcedureValueType::Number => val.is_number(),
    }
}

/// Checks whether two values match for input-column filtering.
fn values_match(row_val: &Value, arg_val: &Value) -> bool {
    if arg_val.is_null() || row_val.is_null() {
        return arg_val.is_null() && row_val.is_null();
    }
    // Compare numbers by f64 to handle int/float cross-comparison
    if let (Some(a), Some(b)) = (row_val.as_f64(), arg_val.as_f64()) {
        return (a - b).abs() < f64::EPSILON;
    }
    row_val == arg_val
}
