// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared async core functions used by both sync and async APIs.
//!
//! These are pure async Rust functions with no Python dependencies,
//! making them callable from both `block_on()` (sync) and
//! `future_into_py()` (async) contexts.

use ::uni_db::{Uni, Value, Vid};
use std::collections::HashMap;
use std::time::Duration;
use uni_common::core::schema::{
    DataType, DistanceMetric, IndexDefinition, ScalarIndexConfig, ScalarIndexType,
    VectorIndexConfig, VectorIndexType,
};

// Re-export types used by the sync and async API modules.
pub use ::uni_db::api::schema::LabelInfo as UniLabelInfo;
pub use ::uni_db::{ExplainOutput, ProfileOutput, QueryResult, Row};

// ============================================================================
// Query Core
// ============================================================================

/// Execute a query with parameters and return result rows.
pub async fn query_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
) -> Result<QueryResult, String> {
    let mut builder = db.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    builder.fetch_all().await.map_err(|e| e.to_string())
}

/// Execute a query with parameters, timeout, and memory limit.
pub async fn query_builder_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
) -> Result<QueryResult, String> {
    let mut builder = db.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    if let Some(t) = timeout_secs {
        builder = builder.timeout(Duration::from_secs_f64(t));
    }
    if let Some(m) = max_memory {
        builder = builder.max_memory(m);
    }
    builder.fetch_all().await.map_err(|e| e.to_string())
}

/// Execute a mutation query, returning affected row count.
pub async fn execute_core(db: &Uni, cypher: &str) -> Result<usize, String> {
    let result = db.execute(cypher).await.map_err(|e| e.to_string())?;
    Ok(result.affected_rows)
}

/// Execute a mutation query with parameters, returning affected row count.
pub async fn execute_with_params_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
) -> Result<usize, String> {
    let mut builder = db.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    let result = builder.fetch_all().await.map_err(|e| e.to_string())?;
    Ok(result.len())
}

/// Explain a query plan without executing.
pub async fn explain_core(db: &Uni, cypher: &str) -> Result<ExplainOutput, String> {
    db.explain(cypher).await.map_err(|e| e.to_string())
}

/// Profile query execution with operator-level statistics.
pub async fn profile_core(db: &Uni, cypher: &str) -> Result<(QueryResult, ProfileOutput), String> {
    db.profile(cypher).await.map_err(|e| e.to_string())
}

/// Flush all uncommitted changes to persistent storage.
pub async fn flush_core(db: &Uni) -> Result<(), String> {
    db.flush().await.map_err(|e| e.to_string())
}

// ============================================================================
// Transaction Core
// ============================================================================

/// Begin a new transaction.
pub async fn begin_transaction_core(db: &Uni) -> Result<(), String> {
    let writer_lock = db.writer().ok_or_else(|| "Read only".to_string())?;
    let mut writer = writer_lock.write().await;
    writer.begin_transaction().map_err(|e| e.to_string())?;
    Ok(())
}

/// Commit a transaction.
pub async fn commit_transaction_core(db: &Uni) -> Result<(), String> {
    let writer_lock = db.writer().ok_or_else(|| "Read only".to_string())?;
    let mut writer = writer_lock.write().await;
    writer
        .commit_transaction()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Rollback a transaction.
pub async fn rollback_transaction_core(db: &Uni) -> Result<(), String> {
    let writer_lock = db.writer().ok_or_else(|| "Read only".to_string())?;
    let mut writer = writer_lock.write().await;
    writer.rollback_transaction().map_err(|e| e.to_string())?;
    Ok(())
}

// ============================================================================
// Schema Core
// ============================================================================

/// Create a label.
pub async fn create_label_core(db: &Uni, name: &str) -> Result<u16, String> {
    let sm = db.schema_manager();
    let id = sm.add_label(name).map_err(|e| e.to_string())?;
    sm.save().await.map_err(|e| e.to_string())?;
    Ok(id)
}

/// Create an edge type.
pub async fn create_edge_type_core(
    db: &Uni,
    name: &str,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
) -> Result<u32, String> {
    let sm = db.schema_manager();
    let id = sm
        .add_edge_type(name, from_labels, to_labels)
        .map_err(|e| e.to_string())?;
    sm.save().await.map_err(|e| e.to_string())?;
    Ok(id)
}

/// Add a property to a label or edge type.
pub async fn add_property_core(
    db: &Uni,
    label_or_type: &str,
    name: &str,
    dt: DataType,
    nullable: bool,
) -> Result<(), String> {
    let sm = db.schema_manager();
    sm.add_property(label_or_type, name, dt, nullable)
        .map_err(|e| e.to_string())?;
    sm.save().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Check if a label exists.
pub async fn label_exists_core(db: &Uni, name: &str) -> Result<bool, String> {
    db.label_exists(name).await.map_err(|e| e.to_string())
}

/// Check if an edge type exists.
pub async fn edge_type_exists_core(db: &Uni, name: &str) -> Result<bool, String> {
    db.edge_type_exists(name).await.map_err(|e| e.to_string())
}

/// List all label names.
pub async fn list_labels_core(db: &Uni) -> Result<Vec<String>, String> {
    db.list_labels().await.map_err(|e| e.to_string())
}

/// List all edge type names.
pub async fn list_edge_types_core(db: &Uni) -> Result<Vec<String>, String> {
    db.list_edge_types().await.map_err(|e| e.to_string())
}

/// Get detailed information about a label.
pub async fn get_label_info_core(db: &Uni, name: &str) -> Result<Option<UniLabelInfo>, String> {
    db.get_label_info(name).await.map_err(|e| e.to_string())
}

/// Load schema from a JSON file.
pub async fn load_schema_core(db: &Uni, path: &str) -> Result<(), String> {
    db.load_schema(path).await.map_err(|e| e.to_string())
}

/// Save schema to a JSON file.
pub async fn save_schema_core(db: &Uni, path: &str) -> Result<(), String> {
    db.save_schema(path).await.map_err(|e| e.to_string())
}

/// Apply pending schema changes.
pub async fn apply_schema_core(
    db: &Uni,
    pending_labels: &[String],
    pending_edge_types: &[(String, Vec<String>, Vec<String>)],
    pending_properties: &[(String, String, DataType, bool)],
    pending_indexes: &[IndexDefinition],
) -> Result<(), String> {
    let sm = db.schema_manager();

    for name in pending_labels {
        sm.add_label(name).map_err(|e| e.to_string())?;
    }

    for (name, from, to) in pending_edge_types {
        sm.add_edge_type(name, from.clone(), to.clone())
            .map_err(|e| e.to_string())?;
    }

    for (label_or_type, prop_name, data_type, nullable) in pending_properties {
        sm.add_property(label_or_type, prop_name, data_type.clone(), *nullable)
            .map_err(|e| e.to_string())?;
    }

    for idx in pending_indexes {
        sm.add_index(idx.clone()).map_err(|e| e.to_string())?;
    }

    sm.save().await.map_err(|e| e.to_string())?;
    Ok(())
}

// ============================================================================
// Index Core
// ============================================================================

/// Create a scalar index.
pub async fn create_scalar_index_core(
    db: &Uni,
    label: &str,
    property: &str,
    index_type: &str,
) -> Result<(), String> {
    let it = match index_type.to_lowercase().as_str() {
        "btree" => ScalarIndexType::BTree,
        "hash" => ScalarIndexType::Hash,
        "bitmap" => ScalarIndexType::Bitmap,
        _ => return Err(format!("Unknown index type: {}", index_type)),
    };

    let sm = db.schema_manager();
    let idx_config = ScalarIndexConfig {
        name: format!("idx_{}_{}", label, property),
        label: label.to_string(),
        properties: vec![property.to_string()],
        index_type: it,
        where_clause: None,
    };
    let def = IndexDefinition::Scalar(idx_config);
    sm.add_index(def).map_err(|e| e.to_string())?;
    sm.save().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Create a vector index.
pub async fn create_vector_index_core(
    db: &Uni,
    label: &str,
    property: &str,
    metric: &str,
) -> Result<(), String> {
    let metric_type = match metric.to_lowercase().as_str() {
        "l2" => DistanceMetric::L2,
        "cosine" => DistanceMetric::Cosine,
        "dot" => DistanceMetric::Dot,
        _ => return Err(format!("Unknown metric: {}", metric)),
    };

    let sm = db.schema_manager();
    let idx_config = VectorIndexConfig {
        name: format!("idx_{}_{}_vec", label, property),
        label: label.to_string(),
        property: property.to_string(),
        index_type: VectorIndexType::Hnsw {
            m: 16,
            ef_construction: 200,
            ef_search: 50,
        },
        metric: metric_type,
        embedding_config: None,
    };

    let def = IndexDefinition::Vector(idx_config);
    sm.add_index(def).map_err(|e| e.to_string())?;
    sm.save().await.map_err(|e| e.to_string())?;
    Ok(())
}

// ============================================================================
// Bulk Loading Core
// ============================================================================

/// Bulk insert vertices.
pub async fn bulk_insert_vertices_core(
    db: &Uni,
    label: &str,
    rust_props: Vec<HashMap<String, serde_json::Value>>,
) -> Result<Vec<Vid>, String> {
    db.bulk_insert_vertices(label, rust_props)
        .await
        .map_err(|e| e.to_string())
}

/// Bulk insert edges.
pub async fn bulk_insert_edges_core(
    db: &Uni,
    edge_type: &str,
    edges: Vec<(Vid, Vid, HashMap<String, serde_json::Value>)>,
) -> Result<(), String> {
    db.bulk_insert_edges(edge_type, edges)
        .await
        .map_err(|e| e.to_string())
}

// ============================================================================
// Database Builder Core
// ============================================================================

/// Mode for opening database.
#[derive(Debug, Clone, Copy)]
pub enum OpenMode {
    Open,
    OpenExisting,
    Create,
    Temporary,
}

/// Build and open a Uni database.
pub async fn build_database_core(
    uri: &str,
    mode: OpenMode,
    hybrid_local: Option<&str>,
    hybrid_remote: Option<&str>,
    cache_size: Option<usize>,
    parallelism: Option<usize>,
) -> Result<Uni, String> {
    let mut builder = match mode {
        OpenMode::Open => Uni::open(uri),
        OpenMode::OpenExisting => Uni::open_existing(uri),
        OpenMode::Create => Uni::create(uri),
        OpenMode::Temporary => Uni::temporary(),
    };

    if let (Some(local), Some(remote)) = (hybrid_local, hybrid_remote) {
        builder = builder.hybrid(local, remote);
    }

    if let Some(size) = cache_size {
        builder = builder.cache_size(size);
    }

    if let Some(n) = parallelism {
        builder = builder.parallelism(n);
    }

    builder.build().await.map_err(|e| e.to_string())
}

// ============================================================================
// Helpers
// ============================================================================

/// Parse a data type string into a DataType enum.
pub fn parse_data_type(data_type: &str) -> Result<DataType, String> {
    if data_type.starts_with("vector:") {
        let dims = data_type
            .split(':')
            .nth(1)
            .ok_or_else(|| "Vector type must specify dimensions, e.g., 'vector:128'".to_string())?
            .parse::<usize>()
            .map_err(|_| "Invalid dimensions for vector type".to_string())?;
        Ok(DataType::Vector { dimensions: dims })
    } else if data_type.starts_with("list:") {
        let elem_type = data_type.split(':').nth(1).ok_or_else(|| {
            "List type must specify element type, e.g., 'list:string'".to_string()
        })?;
        let inner = parse_data_type(elem_type)?;
        Ok(DataType::List(Box::new(inner)))
    } else {
        match data_type.to_lowercase().as_str() {
            "string" => Ok(DataType::String),
            "int64" | "int" => Ok(DataType::Int64),
            "int32" => Ok(DataType::Int32),
            "float64" | "float" => Ok(DataType::Float64),
            "float32" => Ok(DataType::Float32),
            "bool" => Ok(DataType::Bool),
            "datetime" | "timestamp" => Ok(DataType::DateTime),
            "date" => Ok(DataType::Date),
            "time" => Ok(DataType::Time),
            "duration" => Ok(DataType::Duration),
            "json" => Ok(DataType::Json),
            "bytes" => Ok(DataType::String),
            _ => Err(format!("Unknown data type: {}", data_type)),
        }
    }
}

/// Create an index definition from parameters.
pub fn create_index_definition(
    label: &str,
    property: &str,
    index_type: &str,
) -> Result<IndexDefinition, String> {
    match index_type.to_lowercase().as_str() {
        "btree" | "scalar" => Ok(IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
        })),
        "hash" => Ok(IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            index_type: ScalarIndexType::Hash,
            where_clause: None,
        })),
        "vector" => Ok(IndexDefinition::Vector(VectorIndexConfig {
            name: format!("idx_{}_{}_vec", label, property),
            label: label.to_string(),
            property: property.to_string(),
            index_type: VectorIndexType::Hnsw {
                m: 16,
                ef_construction: 200,
                ef_search: 50,
            },
            metric: DistanceMetric::Cosine,
            embedding_config: None,
        })),
        _ => Err(format!("Unknown index type: {}", index_type)),
    }
}
