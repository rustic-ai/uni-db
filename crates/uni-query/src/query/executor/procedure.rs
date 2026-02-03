// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::core::*;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use indexmap::IndexMap;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use uni_algo::algo::procedures::AlgoContext;
use uni_common::core::schema::{DistanceMetric, SchemaManager};
use uni_cypher::ast::Expr;
use uni_store::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;

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

impl Executor {
    /// Maps a user-provided yield name to a canonical name.
    ///
    /// # Mapping Rules
    ///
    /// - "vid", "_vid" → "vid"
    /// - "distance", "dist", "_distance" → "distance"
    /// - "score", "_score" → "score"
    /// - anything else → "node" (treated as node variable)
    fn map_yield_to_canonical(yield_name: &str) -> String {
        let lower = yield_name.to_lowercase();
        match lower.as_str() {
            "vid" | "_vid" => "vid".to_string(),
            "distance" | "dist" | "_distance" => "distance".to_string(),
            "score" | "_score" => "score".to_string(),
            _ => "node".to_string(),
        }
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
                    self.l0_manager.as_ref().map(|m| m.clone())
                };
                let algo_ctx = AlgoContext::new(self.storage.clone(), l0_mgr);

                let signature = procedure.signature();
                let mut stream = procedure.execute(algo_ctx, evaluated_args);
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
                            result_map.insert(yield_name.clone(), row.values[idx].clone());
                        }
                    }
                    results.push(result_map);
                }

                return Ok(results);
            }
        }

        match name {
            // NEW: uni.vector.query (primary namespace)
            "uni.vector.query" => {
                let empty_row = HashMap::new();
                let label = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label must be string"))?
                    .to_string();
                let property = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Property must be string"))?
                    .to_string();
                let query_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;
                let k = self
                    .evaluate_expr(&args[3], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_u64()
                    .ok_or(anyhow!("k must be integer"))? as usize;
                let query_vector: Vec<f32> = serde_json::from_value(query_val)?;

                // Extract optional filter (arg 4)
                let filter = if args.len() > 4 {
                    let filter_val = self
                        .evaluate_expr(&args[4], &empty_row, prop_manager, params, ctx)
                        .await?;
                    if filter_val.is_null() {
                        None
                    } else {
                        let filter_str = filter_val
                            .as_str()
                            .ok_or_else(|| anyhow!("Filter must be a string"))?
                            .to_string();
                        // Validate filter not empty
                        if filter_str.trim().is_empty() {
                            return Err(anyhow!("Filter cannot be empty string"));
                        }
                        Some(filter_str)
                    }
                } else {
                    None
                };

                // Extract optional threshold (arg 5)
                let threshold = if args.len() > 5 {
                    let threshold_val = self
                        .evaluate_expr(&args[5], &empty_row, prop_manager, params, ctx)
                        .await?;
                    if threshold_val.is_null() {
                        None
                    } else {
                        let thresh = threshold_val
                            .as_f64()
                            .ok_or_else(|| anyhow!("Threshold must be a number"))?;
                        // Validate threshold range
                        if thresh < 0.0 {
                            return Err(anyhow!("Threshold must be non-negative, got {}", thresh));
                        }
                        Some(thresh)
                    }
                } else {
                    None
                };

                // Call storage with filter
                let mut results = self
                    .storage
                    .vector_search(&label, &property, &query_vector, k, filter.as_deref(), ctx)
                    .await?;

                // Apply threshold post-filter (on distance)
                if let Some(max_dist) = threshold {
                    results.retain(|(_, dist)| *dist <= max_dist as f32);
                }

                let mut matches = Vec::new();
                let schema_manager = self.storage.schema_manager();

                for (vid, dist) in results {
                    // Build a map of standard yields (canonical names)
                    let mut canonical_yields: IndexMap<String, Value> = IndexMap::new();

                    // Calculate normalized score
                    let score = calculate_score(dist, schema_manager, &label, &property)?;

                    // Always prepare standard yields
                    canonical_yields.insert("vid".to_string(), json!(vid.as_u64()));
                    canonical_yields.insert("distance".to_string(), json!(dist as f64));
                    canonical_yields.insert("score".to_string(), json!(score));

                    // Load node object for node-like yields
                    let mut node_obj_opt = None;

                    // 6. Map user yield names to canonical yields (flexible matching)
                    let mut result = HashMap::new();
                    for yield_name in yield_items {
                        let canonical_name = Self::map_yield_to_canonical(yield_name);

                        // Handle node-like yields (anything not matching standard names)
                        if canonical_name == "node" {
                            // Lazy-load node properties only if needed
                            if node_obj_opt.is_none() {
                                let props_opt =
                                    prop_manager.get_all_vertex_props_with_ctx(vid, ctx).await?;

                                let Some(properties) = props_opt else {
                                    continue; // Skip deleted vertices
                                };

                                // Construct JSON object with flattened properties + _vid
                                let mut node_obj = serde_json::Map::new();
                                node_obj.insert("_vid".to_string(), json!(vid.as_u64()));
                                node_obj.insert("_label".to_string(), json!(label.clone()));

                                // Flatten properties into top level
                                for (key, val) in properties {
                                    node_obj.insert(key, val);
                                }

                                node_obj_opt = Some(Value::Object(node_obj));
                            }

                            result.insert(yield_name.to_lowercase(), node_obj_opt.clone().unwrap());
                        } else if let Some(val) = canonical_yields.get(&canonical_name) {
                            // Standard yields (vid, distance, score)
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
                    ("files_compacted".to_string(), json!(stats.files_compacted)),
                    ("bytes_before".to_string(), json!(stats.bytes_before)),
                    ("bytes_after".to_string(), json!(stats.bytes_after)),
                    (
                        "duration_ms".to_string(),
                        json!(stats.duration.as_millis() as u64),
                    ),
                ]);

                Ok(vec![filter_yield_items(full_result, yield_items)])
            }
            "uni.admin.compactionStatus" => {
                let status = self.storage.compaction_status();
                let full_result = HashMap::from([
                    ("l1_runs".to_string(), json!(status.l1_runs)),
                    ("l1_size_bytes".to_string(), json!(status.l1_size_bytes)),
                    (
                        "in_progress".to_string(),
                        json!(status.compaction_in_progress),
                    ),
                    ("pending".to_string(), json!(status.compaction_pending)),
                    (
                        "total_compactions".to_string(),
                        json!(status.total_compactions),
                    ),
                    (
                        "total_bytes_compacted".to_string(),
                        json!(status.total_bytes_compacted),
                    ),
                ]);

                Ok(vec![filter_yield_items(full_result, yield_items)])
            }
            "uni.admin.snapshot.create" => {
                let empty_row = HashMap::new();
                let name = if !args.is_empty() {
                    Some(
                        self.evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                            .await?
                            .as_str()
                            .ok_or(anyhow!("Snapshot name must be string"))?
                            .to_string(),
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
                        row.insert("created_at".to_string(), json!(m.created_at));
                        row.insert("version_hwm".to_string(), json!(m.version_high_water_mark));
                        results.push(row);
                    }
                }
                Ok(results)
            }
            "uni.admin.snapshot.restore" => {
                let empty_row = HashMap::new();
                let id = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Snapshot ID must be string"))?
                    .to_string();

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
                    row.insert("propertyCount".to_string(), json!(prop_count));

                    let node_count = if let Ok(ds) = self.storage.vertex_dataset(label_name) {
                        if let Ok(raw) = ds.open_raw().await {
                            raw.count_rows(None).await.unwrap_or(0)
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                    row.insert("nodeCount".to_string(), json!(node_count));

                    let idx_count = schema
                        .indexes
                        .iter()
                        .filter(|i| i.label() == label_name)
                        .count();
                    row.insert("indexCount".to_string(), json!(idx_count));

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
                    row.insert("sourceLabels".to_string(), json!(meta.src_labels));
                    row.insert("targetLabels".to_string(), json!(meta.dst_labels));

                    let prop_count = schema
                        .properties
                        .get(type_name)
                        .map(|p| p.len())
                        .unwrap_or(0);
                    row.insert("propertyCount".to_string(), json!(prop_count));

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
                            row.insert("properties".to_string(), json!(vec![v.property]));
                        }
                        uni_common::core::schema::IndexDefinition::FullText(f) => {
                            row.insert("name".to_string(), Value::String(f.name));
                            row.insert("type".to_string(), Value::String("FULLTEXT".to_string()));
                            row.insert("label".to_string(), Value::String(f.label));
                            row.insert("properties".to_string(), json!(f.properties));
                        }
                        uni_common::core::schema::IndexDefinition::Scalar(s) => {
                            row.insert("name".to_string(), Value::String(s.name));
                            row.insert("type".to_string(), Value::String("SCALAR".to_string()));
                            row.insert("label".to_string(), Value::String(s.label));
                            row.insert("properties".to_string(), json!(s.properties));
                        }
                        uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                            row.insert("name".to_string(), Value::String(j.name));
                            row.insert("type".to_string(), Value::String("JSON_FTS".to_string()));
                            row.insert("label".to_string(), Value::String(j.label));
                            row.insert("properties".to_string(), json!(vec![j.column]));
                        }
                        uni_common::core::schema::IndexDefinition::Inverted(inv) => {
                            row.insert("name".to_string(), Value::String(inv.name));
                            row.insert("type".to_string(), Value::String("INVERTED".to_string()));
                            row.insert("label".to_string(), Value::String(inv.label));
                            row.insert("properties".to_string(), json!(vec![inv.property]));
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
                            row.insert("properties".to_string(), json!(properties));
                        }
                        uni_common::core::schema::ConstraintType::Exists { property } => {
                            row.insert("type".to_string(), Value::String("EXISTS".to_string()));
                            row.insert("properties".to_string(), json!(vec![property]));
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
                let empty_row = HashMap::new();
                let label_name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label must be string"))?
                    .to_string();

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
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label name must be string"))?
                    .to_string();
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
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Edge type name must be string"))?
                    .to_string();
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
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label must be string"))?
                    .to_string();
                let property = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Property must be string"))?
                    .to_string();
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
                let empty_row = HashMap::new();
                let label = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label must be string"))?
                    .to_string();
                let c_type = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Constraint type must be string"))?
                    .to_string();
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
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Label name must be string"))?
                    .to_string();

                let success = super::ddl_procedures::drop_label(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.dropEdgeType" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Edge type name must be string"))?
                    .to_string();

                let success = super::ddl_procedures::drop_edge_type(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.dropIndex" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Index name must be string"))?
                    .to_string();

                let success = super::ddl_procedures::drop_index(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            "uni.schema.dropConstraint" => {
                let empty_row = HashMap::new();
                let name = self
                    .evaluate_expr(&args[0], &empty_row, prop_manager, params, ctx)
                    .await?
                    .as_str()
                    .ok_or(anyhow!("Constraint name must be string"))?
                    .to_string();

                let success = super::ddl_procedures::drop_constraint(&self.storage, &name).await?;
                Ok(vec![HashMap::from([(
                    "success".to_string(),
                    Value::Bool(success),
                )])])
            }
            _ => Err(anyhow!("Unknown procedure {}", name)),
        }
    }
}
