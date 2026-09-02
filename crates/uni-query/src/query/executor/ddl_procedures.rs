// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use uni_common::Value;
use uni_common::{
    UniError,
    core::schema::{
        Constraint, ConstraintTarget, ConstraintType, DataType, EmbeddingConfig, IndexDefinition,
        ScalarIndexConfig, ScalarIndexType, VectorIndexConfig, validate_identifier,
    },
};
use uni_store::storage::StorageManager;

#[derive(Deserialize)]
struct LabelConfig {
    #[serde(default)]
    properties: HashMap<String, PropertyConfig>,
    #[serde(default)]
    indexes: Vec<IndexConfig>,
    #[serde(default)]
    constraints: Vec<ConstraintConfig>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Deserialize)]
struct PropertyConfig {
    #[serde(rename = "type")]
    data_type: String,
    #[serde(default = "default_nullable")]
    nullable: bool,
    #[serde(default)]
    pub description: Option<String>,
}

fn default_nullable() -> bool {
    true
}

#[derive(Deserialize)]
struct IndexConfig {
    property: Option<String>,
    #[serde(rename = "type")]
    index_type: String,
    // Vector specific
    #[expect(dead_code)]
    dimensions: Option<usize>,
    metric: Option<String>,
    algorithm: Option<String>,
    partitions: Option<u32>,
    m: Option<u32>,
    ef_construction: Option<u32>,
    sub_vectors: Option<u32>,
    num_bits: Option<u8>,
    // MUVERA-specific (algorithm == "muvera"): FDE encoder params + inner ANN type.
    k_sim: Option<u32>,
    reps: Option<u32>,
    d_proj: Option<u32>,
    seed: Option<u64>,
    inner: Option<String>,
    embedding: Option<EmbeddingOptions>,
    // Sparse-specific: 8-bit weight quantization (default on).
    quantize: Option<bool>,
    // Generic
    name: Option<String>,
}

/// Embedding configuration for vector indexes via procedure API.
#[derive(Deserialize)]
struct EmbeddingOptions {
    alias: String,
    source: Vec<String>,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    #[serde(default)]
    document_prefix: Option<String>,
    #[serde(default)]
    query_prefix: Option<String>,
}

fn default_batch_size() -> usize {
    32
}

#[derive(Deserialize)]
struct ConstraintConfig {
    #[serde(rename = "type")]
    constraint_type: String,
    properties: Vec<String>,
    name: Option<String>,
}

/// Deserialize the `config` map of `create_label` / `create_edge_type`.
fn parse_label_config(config_val: &Value) -> Result<LabelConfig> {
    let json_val: serde_json::Value = config_val.clone().into();
    serde_json::from_value(json_val).map_err(|e| {
        UniError::InvalidArgument {
            arg: "config".to_string(),
            message: e.to_string(),
        }
        .into()
    })
}

/// Register every declared property against `name` (a label or an edge type).
fn add_properties(
    storage: &StorageManager,
    name: &str,
    properties: HashMap<String, PropertyConfig>,
) -> Result<()> {
    for (prop_name, prop_config) in properties {
        validate_identifier(&prop_name)?;
        let data_type = parse_data_type(&prop_config.data_type)?;
        storage.schema_manager().add_property_with_desc(
            name,
            &prop_name,
            data_type,
            prop_config.nullable,
            prop_config.description,
        )?;
    }
    Ok(())
}

pub async fn create_label(
    storage: &StorageManager,
    name: &str,
    config_val: &Value,
) -> Result<bool> {
    validate_identifier(name)?;

    if storage.schema_manager().schema().labels.contains_key(name) {
        return Err(UniError::LabelAlreadyExists {
            label: name.to_string(),
        }
        .into());
    }

    let config = parse_label_config(config_val)?;

    // Create label
    storage
        .schema_manager()
        .add_label_with_desc(name, config.description)?;

    // Add properties
    add_properties(storage, name, config.properties)?;

    // Add indexes
    for idx in config.indexes {
        if idx.property.is_none() {
            return Err(UniError::InvalidArgument {
                arg: "indexes".into(),
                message: "Property name required for index definition".into(),
            }
            .into());
        }
        create_index_internal(storage, name, &idx).await?;
    }

    // Add constraints
    for c in config.constraints {
        create_constraint_internal(storage, name, &c, true).await?;
    }

    storage.schema_manager().save().await?;
    Ok(true)
}

pub async fn create_edge_type(
    storage: &StorageManager,
    name: &str,
    src_labels: Vec<String>,
    dst_labels: Vec<String>,
    config_val: &Value,
) -> Result<bool> {
    validate_identifier(name)?;

    let config = parse_label_config(config_val)?;

    storage.schema_manager().add_edge_type_with_desc(
        name,
        src_labels,
        dst_labels,
        config.description,
    )?;

    add_properties(storage, name, config.properties)?;

    // Constraints
    for c in config.constraints {
        create_constraint_internal(storage, name, &c, false).await?;
    }

    storage.schema_manager().save().await?;
    Ok(true)
}

pub async fn create_index(
    storage: &StorageManager,
    label: &str,
    property: &str,
    config_val: &Value,
) -> Result<bool> {
    let json_val: serde_json::Value = config_val.clone().into();
    let mut config: IndexConfig =
        serde_json::from_value(json_val).map_err(|e| UniError::InvalidArgument {
            arg: "config".to_string(),
            message: e.to_string(),
        })?;

    // Override property from args
    config.property = Some(property.to_string());

    create_index_internal(storage, label, &config).await?;
    storage.schema_manager().save().await?;
    Ok(true)
}

pub async fn create_constraint(
    storage: &StorageManager,
    label: &str,
    constraint_type: &str,
    properties: Vec<String>,
) -> Result<bool> {
    let config = ConstraintConfig {
        constraint_type: constraint_type.to_string(),
        properties,
        name: None,
    };
    // Assume label target
    create_constraint_internal(storage, label, &config, true).await?;
    storage.schema_manager().save().await?;
    Ok(true)
}

pub async fn drop_label(storage: &StorageManager, name: &str) -> Result<bool> {
    storage.schema_manager().drop_label(name, true)?;
    storage.schema_manager().save().await?;
    Ok(true)
}

pub async fn drop_edge_type(storage: &StorageManager, name: &str) -> Result<bool> {
    storage.schema_manager().drop_edge_type(name, true)?;
    storage.schema_manager().save().await?;
    Ok(true)
}

pub async fn drop_index(storage: &StorageManager, name: &str) -> Result<bool> {
    storage.schema_manager().remove_index(name)?;
    storage.schema_manager().save().await?;
    Ok(true)
}

pub async fn drop_constraint(storage: &StorageManager, name: &str) -> Result<bool> {
    storage.schema_manager().drop_constraint(name, true)?;
    storage.schema_manager().save().await?;
    Ok(true)
}

// Internal helpers

async fn create_index_internal(
    storage: &StorageManager,
    label: &str,
    config: &IndexConfig,
) -> Result<()> {
    let prop_name = config
        .property
        .as_ref()
        .ok_or_else(|| UniError::InvalidArgument {
            arg: "property".into(),
            message: "Property is missing".into(),
        })?;

    let index_name = config.name.clone().unwrap_or_else(|| {
        format!(
            "{}_{}_{}",
            label,
            prop_name,
            config.index_type.to_lowercase()
        )
    });

    let def = match config.index_type.to_uppercase().as_str() {
        "VECTOR" => {
            // Distance metric + index type are parsed via the SAME helpers as the DDL
            // `CREATE VECTOR INDEX` path so dense / native-multivector / MUVERA behave
            // identically across both entry points (incl. the default ANN = IVF_PQ).
            let metric =
                uni_common::vector_index_opts::parse_vector_metric(config.metric.as_deref())
                    .map_err(|e| UniError::InvalidArgument {
                        arg: "metric".into(),
                        message: e.to_string(),
                    })?;

            // Parse embedding config from procedure options
            let embedding_config = config.embedding.as_ref().map(|emb| EmbeddingConfig {
                alias: emb.alias.clone(),
                source_properties: emb.source.clone(),
                batch_size: emb.batch_size,
                document_prefix: emb.document_prefix.clone(),
                query_prefix: emb.query_prefix.clone(),
            });

            let index_type = uni_common::vector_index_opts::build_vector_index_type(
                &uni_common::vector_index_opts::VectorIndexOpts {
                    type_name: config.algorithm.as_deref(),
                    partitions: config.partitions,
                    m: config.m,
                    ef_construction: config.ef_construction,
                    sub_vectors: config.sub_vectors,
                    num_bits: config.num_bits,
                    k_sim: config.k_sim,
                    reps: config.reps,
                    d_proj: config.d_proj,
                    seed: config.seed,
                    inner: config.inner.as_deref(),
                },
            );

            IndexDefinition::Vector(VectorIndexConfig {
                name: index_name,
                label: label.to_string(),
                property: prop_name.clone(),
                index_type,
                metric,
                embedding_config,
                metadata: Default::default(),
                default_refine_factor: None,
            })
        }
        "SCALAR" | "BTREE" => IndexDefinition::Scalar(ScalarIndexConfig {
            name: index_name,
            label: label.to_string(),
            properties: vec![prop_name.clone()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: Default::default(),
        }),
        "BITMAP" => IndexDefinition::Scalar(ScalarIndexConfig {
            name: index_name,
            label: label.to_string(),
            properties: vec![prop_name.clone()],
            index_type: ScalarIndexType::Bitmap,
            where_clause: None,
            metadata: Default::default(),
        }),
        "LABEL_LIST" | "LABELLIST" => IndexDefinition::Scalar(ScalarIndexConfig {
            name: index_name,
            label: label.to_string(),
            properties: vec![prop_name.clone()],
            index_type: ScalarIndexType::LabelList,
            where_clause: None,
            metadata: Default::default(),
        }),
        "INVERTED" => IndexDefinition::Inverted(uni_common::core::schema::InvertedIndexConfig {
            name: index_name,
            label: label.to_string(),
            property: prop_name.clone(),
            normalize: true,
            max_terms_per_doc: 10_000,
            metadata: Default::default(),
        }),
        "SPARSE" => {
            // The term-space cardinality comes from the declared
            // `DataType::SparseVector { dimensions }` of the column.
            let dimensions = storage
                .schema_manager()
                .schema()
                .properties
                .get(label)
                .and_then(|props| props.get(prop_name))
                .and_then(|meta| match &meta.r#type {
                    uni_common::DataType::SparseVector { dimensions } => Some(*dimensions),
                    _ => None,
                })
                .ok_or_else(|| UniError::InvalidArgument {
                    arg: "property".into(),
                    message: format!(
                        "Property '{prop_name}' is not a SparseVector column; cannot create a sparse index"
                    ),
                })?;
            // Auto-embed config, same shape as the VECTOR arm.
            let embedding_config = config.embedding.as_ref().map(|emb| EmbeddingConfig {
                alias: emb.alias.clone(),
                source_properties: emb.source.clone(),
                batch_size: emb.batch_size,
                document_prefix: emb.document_prefix.clone(),
                query_prefix: emb.query_prefix.clone(),
            });
            IndexDefinition::Sparse(uni_common::core::schema::SparseVectorIndexConfig {
                name: index_name,
                label: label.to_string(),
                property: prop_name.clone(),
                dimensions,
                // `OPTIONS{type:'sparse', quantize:false}` stores lossless f32.
                quantize: config.quantize.unwrap_or(true),
                embedding_config,
                metadata: Default::default(),
            })
        }
        _ => {
            return Err(UniError::InvalidArgument {
                arg: "type".into(),
                message: format!("Unsupported index type: {}", config.index_type),
            }
            .into());
        }
    };

    storage.schema_manager().add_index(def.clone())?;

    let idx_mgr = storage.index_manager();
    match def {
        // Vector indexes ALWAYS build, matching the DDL `CREATE VECTOR INDEX` path:
        // `create_vector_index` handles an empty/not-yet-created dataset gracefully, and
        // MUVERA must register (+ backfill when data exists) its derived FDE column even
        // before any rows exist (create-before-ingest). `prepare_muvera_index` is a no-op
        // for non-MUVERA vector indexes.
        IndexDefinition::Vector(cfg) => {
            idx_mgr.create_vector_index(cfg).await?;
        }
        // Sparse ALSO always builds, like Vector: `create_sparse_vector_index`
        // handles an empty table (create-before-ingest → populated on flush) and
        // backfills already-flushed rows via the storage *backend*. It must NOT be
        // gated by the raw-dataset `count` below, which reads 0 for a flushed
        // LanceDB-managed table (the table is not at the raw `{base}/vertices_<L>`
        // path); that gate would silently leave the index empty after a flush.
        IndexDefinition::Sparse(cfg) => {
            idx_mgr.create_sparse_vector_index(cfg).await?;
        }
        // Remaining non-vector indexes keep the build-if-data-exists optimization.
        other => {
            // Build-if-data-exists gate, read through the `StorageBackend`
            // (correct `.lance` path). With the prior raw-dataset read this
            // counted 0 for any flushed table, so the physical index was
            // silently skipped after a flush (#115). A not-yet-flushed table
            // legitimately has no rows here and is populated on the next flush.
            let backend = storage.backend();
            let table = uni_store::backend::table_names::vertex_table_name(label);
            let count = if backend.table_exists(&table).await? {
                backend.count_rows(&table, None).await?
            } else {
                0
            };
            if count > 0 {
                match other {
                    IndexDefinition::Scalar(cfg) => idx_mgr.create_scalar_index(cfg).await?,
                    IndexDefinition::Inverted(cfg) => idx_mgr.create_inverted_index(cfg).await?,
                    IndexDefinition::FullText(cfg) => idx_mgr.create_fts_index(cfg).await?,
                    IndexDefinition::JsonFullText(cfg) => {
                        idx_mgr.create_json_fts_index(cfg).await?
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

async fn create_constraint_internal(
    storage: &StorageManager,
    target_name: &str,
    config: &ConstraintConfig,
    is_label: bool,
) -> Result<()> {
    let name = config.name.clone().unwrap_or_else(|| {
        format!(
            "{}_{}_{}",
            target_name,
            config.constraint_type.to_lowercase(),
            config.properties.join("_")
        )
    });

    let constraint_type = match config.constraint_type.to_uppercase().as_str() {
        "UNIQUE" => ConstraintType::Unique {
            properties: config.properties.clone(),
        },
        // Accept "NODE_KEY", "NODEKEY", and "NODE KEY" spellings for parity with
        // the Cypher `IS NODE KEY` form.
        "NODE_KEY" | "NODEKEY" | "NODE KEY" => ConstraintType::NodeKey {
            properties: config.properties.clone(),
        },
        "EXISTS" => {
            if config.properties.len() != 1 {
                return Err(UniError::InvalidArgument {
                    arg: "properties".into(),
                    message: "EXISTS constraint requires exactly one property".into(),
                }
                .into());
            }
            ConstraintType::Exists {
                property: config.properties[0].clone(),
            }
        }
        _ => {
            return Err(UniError::InvalidArgument {
                arg: "type".into(),
                message: format!("Unsupported constraint type: {}", config.constraint_type),
            }
            .into());
        }
    };

    let target = if is_label {
        ConstraintTarget::Label(target_name.to_string())
    } else {
        ConstraintTarget::EdgeType(target_name.to_string())
    };

    let constraint = Constraint {
        name,
        constraint_type,
        target,
        enabled: true,
    };

    storage.schema_manager().add_constraint(constraint)?;
    Ok(())
}

fn parse_data_type(s: &str) -> Result<DataType> {
    let s = s.trim();
    if s.to_uppercase().starts_with("LIST<") && s.ends_with('>') {
        let inner = &s[5..s.len() - 1];
        let inner_type = parse_data_type(inner)?;
        return Ok(DataType::List(Box::new(inner_type)));
    }
    if s.to_uppercase().starts_with("MAP<") && s.ends_with('>') {
        let (k_str, v_str) = split_map_kv(&s[4..s.len() - 1])?;
        let key_type = parse_data_type(&k_str)?;
        if !matches!(key_type, DataType::String) {
            return Err(UniError::InvalidArgument {
                arg: "type".into(),
                message: format!("MAP key type must be STRING, got: {k_str}"),
            }
            .into());
        }
        let value_type = parse_data_type(&v_str)?;
        return Ok(DataType::Map(Box::new(key_type), Box::new(value_type)));
    }

    match s.to_uppercase().as_str() {
        "STRING" | "UTF8" => Ok(DataType::String),
        "INT" | "INTEGER" | "INT64" => Ok(DataType::Int64),
        "INT32" => Ok(DataType::Int32),
        "FLOAT" | "FLOAT64" | "DOUBLE" => Ok(DataType::Float64),
        "FLOAT32" => Ok(DataType::Float32),
        "BOOL" | "BOOLEAN" => Ok(DataType::Bool),
        "DATETIME" => Ok(DataType::DateTime),
        "DATE" => Ok(DataType::Date),
        "BTIC" => Ok(DataType::Btic),
        "VECTOR" => Ok(DataType::Vector { dimensions: 0 }),
        _ => Err(UniError::InvalidArgument {
            arg: "type".into(),
            message: format!("Unknown data type: {}", s),
        }
        .into()),
    }
}

/// Split a `MAP<K, V>` inner string on the top-level comma, respecting `<>`/`()` depth so
/// nested value types split at the right comma. Returns trimmed `(key, value)` strings.
fn split_map_kv(inner: &str) -> Result<(String, String)> {
    let mut depth = 0i32;
    for (i, c) in inner.char_indices() {
        match c {
            '<' | '(' => depth += 1,
            '>' | ')' => depth -= 1,
            ',' if depth == 0 => {
                let k = inner[..i].trim();
                let v = inner[i + 1..].trim();
                if k.is_empty() || v.is_empty() {
                    return Err(UniError::InvalidArgument {
                        arg: "type".into(),
                        message: "MAP<K,V> requires both a key and a value type".into(),
                    }
                    .into());
                }
                return Ok((k.to_string(), v.to_string()));
            }
            _ => {}
        }
    }
    Err(UniError::InvalidArgument {
        arg: "type".into(),
        message: "MAP<K,V> requires a comma separating key and value types".into(),
    }
    .into())
}
