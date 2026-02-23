// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use uni_common::Value;
use uni_common::{
    UniError,
    core::schema::{
        Constraint, ConstraintTarget, ConstraintType, DataType, DistanceMetric, EmbeddingConfig,
        IndexDefinition, ScalarIndexConfig, ScalarIndexType, VectorIndexConfig, VectorIndexType,
        validate_identifier,
    },
};
use uni_store::storage::{IndexManager, StorageManager};

#[derive(Deserialize)]
struct LabelConfig {
    #[serde(default)]
    properties: HashMap<String, PropertyConfig>,
    #[serde(default)]
    indexes: Vec<IndexConfig>,
    #[serde(default)]
    constraints: Vec<ConstraintConfig>,
}

#[derive(Deserialize)]
struct PropertyConfig {
    #[serde(rename = "type")]
    data_type: String,
    #[serde(default = "default_nullable")]
    nullable: bool,
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
    dimensions: Option<usize>,
    metric: Option<String>,
    embedding: Option<EmbeddingOptions>,
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

    let json_val: serde_json::Value = config_val.clone().into();
    let config: LabelConfig =
        serde_json::from_value(json_val).map_err(|e| UniError::InvalidArgument {
            arg: "config".to_string(),
            message: e.to_string(),
        })?;

    // Create label
    storage.schema_manager().add_label(name)?;

    // Add properties
    for (prop_name, prop_config) in config.properties {
        validate_identifier(&prop_name)?;
        let data_type = parse_data_type(&prop_config.data_type)?;
        storage
            .schema_manager()
            .add_property(name, &prop_name, data_type, prop_config.nullable)?;
    }

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

    let json_val: serde_json::Value = config_val.clone().into();
    let config: LabelConfig =
        serde_json::from_value(json_val).map_err(|e| UniError::InvalidArgument {
            arg: "config".to_string(),
            message: e.to_string(),
        })?;

    storage
        .schema_manager()
        .add_edge_type(name, src_labels, dst_labels)?;

    for (prop_name, prop_config) in config.properties {
        validate_identifier(&prop_name)?;
        let data_type = parse_data_type(&prop_config.data_type)?;
        storage
            .schema_manager()
            .add_property(name, &prop_name, data_type, prop_config.nullable)?;
    }

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
            let _dimensions = config.dimensions;
            let metric = match config.metric.as_deref().unwrap_or("cosine") {
                "cosine" => DistanceMetric::Cosine,
                "l2" | "euclidean" => DistanceMetric::L2,
                "dot" => DistanceMetric::Dot,
                _ => {
                    return Err(UniError::InvalidArgument {
                        arg: "metric".into(),
                        message: "Invalid metric".into(),
                    }
                    .into());
                }
            };

            // Parse embedding config from procedure options
            let embedding_config = config.embedding.as_ref().map(|emb| EmbeddingConfig {
                alias: emb.alias.clone(),
                source_properties: emb.source.clone(),
                batch_size: emb.batch_size,
            });

            IndexDefinition::Vector(VectorIndexConfig {
                name: index_name,
                label: label.to_string(),
                property: prop_name.clone(),
                index_type: VectorIndexType::Hnsw {
                    m: 16,
                    ef_construction: 200,
                    ef_search: 50,
                }, // Default params
                metric,
                embedding_config,
            })
        }
        "SCALAR" | "BTREE" => IndexDefinition::Scalar(ScalarIndexConfig {
            name: index_name,
            label: label.to_string(),
            properties: vec![prop_name.clone()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
        }),
        "INVERTED" => IndexDefinition::Inverted(uni_common::core::schema::InvertedIndexConfig {
            name: index_name,
            label: label.to_string(),
            property: prop_name.clone(),
            normalize: true,
            max_terms_per_doc: 10_000,
        }),
        _ => {
            return Err(UniError::InvalidArgument {
                arg: "type".into(),
                message: format!("Unsupported index type: {}", config.index_type),
            }
            .into());
        }
    };

    storage.schema_manager().add_index(def.clone())?;

    // Trigger build if data exists
    let count = if let Ok(ds) = storage.vertex_dataset(label) {
        if let Ok(raw) = ds.open_raw().await {
            raw.count_rows(None).await.unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    tracing::debug!("create_index_internal count for {}: {}", label, count);

    if count > 0 {
        let idx_mgr = IndexManager::new(
            storage.base_path(),
            storage.schema_manager_arc(),
            storage.lancedb_store_arc(),
        );
        match def {
            IndexDefinition::Vector(cfg) => {
                idx_mgr.create_vector_index(cfg).await?;
            }
            IndexDefinition::Scalar(cfg) => {
                idx_mgr.create_scalar_index(cfg).await?;
            }
            IndexDefinition::Inverted(cfg) => {
                idx_mgr.create_inverted_index(cfg).await?;
            }
            _ => {}
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

    match s.to_uppercase().as_str() {
        "STRING" | "UTF8" => Ok(DataType::String),
        "INT" | "INTEGER" | "INT64" => Ok(DataType::Int64),
        "INT32" => Ok(DataType::Int32),
        "FLOAT" | "FLOAT64" | "DOUBLE" => Ok(DataType::Float64),
        "FLOAT32" => Ok(DataType::Float32),
        "BOOL" | "BOOLEAN" => Ok(DataType::Bool),
        "DATETIME" => Ok(DataType::DateTime),
        "DATE" => Ok(DataType::Date),
        "VECTOR" => Ok(DataType::Vector { dimensions: 0 }),
        _ => Err(UniError::InvalidArgument {
            arg: "type".into(),
            message: format!("Unknown data type: {}", s),
        }
        .into()),
    }
}
