// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::storage::inverted_index::InvertedIndex;
use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use arrow_array::UInt64Array;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use lance::index::vector::VectorIndexParams;
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::MetricType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{info, instrument, warn};
use uni_common::core::id::Vid;
use uni_common::core::schema::{
    DistanceMetric, FullTextIndexConfig, IndexDefinition, InvertedIndexConfig, JsonFtsIndexConfig,
    ScalarIndexConfig, SchemaManager, VectorIndexConfig, VectorIndexType,
};

/// Status of an index rebuild task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexRebuildStatus {
    /// Task is waiting to be processed.
    Pending,
    /// Task is currently being processed.
    InProgress,
    /// Task completed successfully.
    Completed,
    /// Task failed with an error.
    Failed,
}

/// A task representing an index rebuild operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRebuildTask {
    /// Unique identifier for this task.
    pub id: String,
    /// The label for which indexes are being rebuilt.
    pub label: String,
    /// Current status of the task.
    pub status: IndexRebuildStatus,
    /// When the task was created.
    pub created_at: DateTime<Utc>,
    /// When the task started processing.
    pub started_at: Option<DateTime<Utc>>,
    /// When the task completed (successfully or with failure).
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message if the task failed.
    pub error: Option<String>,
    /// Number of retry attempts.
    pub retry_count: u32,
}

pub struct IndexManager {
    base_uri: String,
    schema_manager: Arc<SchemaManager>,
    lancedb_store: Arc<crate::lancedb::LanceDbStore>,
}

impl IndexManager {
    pub fn new(
        base_uri: &str,
        schema_manager: Arc<SchemaManager>,
        lancedb_store: Arc<crate::lancedb::LanceDbStore>,
    ) -> Self {
        Self {
            base_uri: base_uri.to_string(),
            schema_manager,
            lancedb_store,
        }
    }

    #[instrument(skip(self), level = "info")]
    pub async fn create_inverted_index(&self, config: InvertedIndexConfig) -> Result<()> {
        let label = &config.label;
        let property = &config.property;
        info!(
            "Creating Inverted Index '{}' on {}.{}",
            config.name, label, property
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let mut index = InvertedIndex::new(&self.base_uri, config.clone()).await?;
        index.create_if_missing().await?;

        let ds = VertexDataset::new(&self.base_uri, label, label_meta.id);

        // Check if dataset exists
        if ds.open_raw().await.is_ok() {
            index
                .build_from_dataset(&ds, |n| info!("Indexed {} terms", n))
                .await?;
        } else {
            warn!(
                "Dataset for label '{}' not found, creating empty inverted index",
                label
            );
        }

        self.schema_manager
            .add_index(IndexDefinition::Inverted(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn create_vector_index(&self, config: VectorIndexConfig) -> Result<()> {
        let label = &config.label;
        let property = &config.property;
        info!(
            "Creating vector index '{}' on {}.{}",
            config.name, label, property
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let metric_type = match config.metric {
                    DistanceMetric::L2 => MetricType::L2,
                    DistanceMetric::Cosine => MetricType::Cosine,
                    DistanceMetric::Dot => MetricType::Dot,
                    _ => return Err(anyhow!("Unsupported metric: {:?}", config.metric)),
                };

                let params = match config.index_type {
                    VectorIndexType::IvfPq {
                        num_partitions,
                        num_sub_vectors,
                        bits_per_subvector,
                    } => {
                        let ivf = IvfBuildParams::new(num_partitions as usize);
                        let pq = PQBuildParams::new(
                            num_sub_vectors as usize,
                            bits_per_subvector as usize,
                        );
                        VectorIndexParams::with_ivf_pq_params(metric_type, ivf, pq)
                    }
                    VectorIndexType::Hnsw {
                        m,
                        ef_construction,
                        ef_search: _,
                    } => {
                        let ivf = IvfBuildParams::new(1);
                        let hnsw = HnswBuildParams::default()
                            .num_edges(m as usize)
                            .ef_construction(ef_construction as usize);
                        let sq = SQBuildParams::default();
                        VectorIndexParams::with_ivf_hnsw_sq_params(metric_type, ivf, hnsw, sq)
                    }
                    VectorIndexType::Flat => {
                        // Fallback to basic IVF-PQ
                        let ivf = IvfBuildParams::new(1);
                        let pq = PQBuildParams::default();
                        VectorIndexParams::with_ivf_pq_params(metric_type, ivf, pq)
                    }
                    _ => {
                        return Err(anyhow!(
                            "Unsupported vector index type: {:?}",
                            config.index_type
                        ));
                    }
                };

                // Ignore errors during creation if dataset is empty or similar, but try
                if let Err(e) = lance_ds
                    .create_index(
                        &[property],
                        IndexType::Vector,
                        Some(config.name.clone()),
                        &params,
                        true,
                    )
                    .await
                {
                    warn!(
                        "Failed to create physical vector index (dataset might be empty): {}",
                        e
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Dataset not found for label '{}', skipping physical index creation but saving schema definition. Error: {}",
                    label, e
                );
            }
        }

        self.schema_manager
            .add_index(IndexDefinition::Vector(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn create_scalar_index(&self, config: ScalarIndexConfig) -> Result<()> {
        let label = &config.label;
        let properties = &config.properties;
        info!(
            "Creating scalar index '{}' on {}.{:?}",
            config.name, label, properties
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let columns: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();

                if let Err(e) = lance_ds
                    .create_index(
                        &columns,
                        IndexType::Scalar,
                        Some(config.name.clone()),
                        &ScalarIndexParams::default(),
                        true,
                    )
                    .await
                {
                    warn!(
                        "Failed to create physical scalar index (dataset might be empty): {}",
                        e
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Dataset not found for label '{}' (scalar index), skipping physical creation. Error: {}",
                    label, e
                );
            }
        }

        self.schema_manager
            .add_index(IndexDefinition::Scalar(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn create_fts_index(&self, config: FullTextIndexConfig) -> Result<()> {
        let label = &config.label;
        info!(
            "Creating FTS index '{}' on {}.{:?}",
            config.name, label, config.properties
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let columns: Vec<&str> = config.properties.iter().map(|s| s.as_str()).collect();

                let fts_params =
                    InvertedIndexParams::default().with_position(config.with_positions);

                if let Err(e) = lance_ds
                    .create_index(
                        &columns,
                        IndexType::Inverted,
                        Some(config.name.clone()),
                        &fts_params,
                        true,
                    )
                    .await
                {
                    warn!(
                        "Failed to create physical FTS index (dataset might be empty): {}",
                        e
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Dataset not found for label '{}' (FTS index), skipping physical creation. Error: {}",
                    label, e
                );
            }
        }

        self.schema_manager
            .add_index(IndexDefinition::FullText(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Creates a JSON Full-Text Search index on a column.
    ///
    /// This creates a Lance inverted index on the specified column,
    /// enabling BM25-based full-text search with optional phrase matching.
    #[instrument(skip(self), level = "info")]
    pub async fn create_json_fts_index(&self, config: JsonFtsIndexConfig) -> Result<()> {
        let label = &config.label;
        let column = &config.column;
        info!(
            "Creating JSON FTS index '{}' on {}.{}",
            config.name, label, column
        );

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        match ds_wrapper.open_raw().await {
            Ok(mut lance_ds) => {
                let fts_params =
                    InvertedIndexParams::default().with_position(config.with_positions);

                if let Err(e) = lance_ds
                    .create_index(
                        &[column],
                        IndexType::Inverted,
                        Some(config.name.clone()),
                        &fts_params,
                        true,
                    )
                    .await
                {
                    warn!(
                        "Failed to create physical JSON FTS index (dataset might be empty): {}",
                        e
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Dataset not found for label '{}' (JSON FTS index), skipping physical creation. Error: {}",
                    label, e
                );
            }
        }

        self.schema_manager
            .add_index(IndexDefinition::JsonFullText(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn drop_index(&self, name: &str) -> Result<()> {
        info!("Dropping index '{}'", name);

        let idx_def = self
            .schema_manager
            .get_index(name)
            .ok_or_else(|| anyhow!("Index '{}' not found in schema", name))?;

        let label = idx_def.label();

        if let Some(label_meta) = self.schema_manager.schema().labels.get(label) {
            let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);
            if let Ok(_lance_ds) = ds_wrapper.open_raw().await {
                // Attempt physical drop
                // if let Err(e) = lance_ds.drop_index(name).await {
                //     warn!("Failed to drop physical index (might not exist): {}", e);
                // }
                warn!(
                    "Physical index drop not supported by current Lance version, removing from schema only."
                );
            }
        }

        self.schema_manager.remove_index(name)?;
        self.schema_manager.save().await?;
        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn rebuild_indexes_for_label(&self, label: &str) -> Result<()> {
        info!("Rebuilding all indexes for label '{}'", label);
        let schema = self.schema_manager.schema();

        // Clone definitions to avoid holding lock while async awaiting
        let indexes = schema.indexes.clone();

        for index in indexes {
            match index {
                IndexDefinition::Vector(cfg) => {
                    if cfg.label == label {
                        self.create_vector_index(cfg).await?;
                    }
                }
                IndexDefinition::Scalar(cfg) => {
                    if cfg.label == label {
                        self.create_scalar_index(cfg).await?;
                    }
                }
                IndexDefinition::FullText(cfg) => {
                    if cfg.label == label {
                        self.create_fts_index(cfg).await?;
                    }
                }
                IndexDefinition::Inverted(cfg) => {
                    if cfg.label == label {
                        self.create_inverted_index(cfg).await?;
                    }
                }
                IndexDefinition::JsonFullText(cfg) => {
                    if cfg.label == label {
                        self.create_json_fts_index(cfg).await?;
                    }
                }
                _ => {
                    log::warn!("Unknown index type encountered during rebuild, skipping");
                }
            }
        }
        Ok(())
    }

    /// Create composite index for unique constraint
    pub async fn create_composite_index(&self, label: &str, properties: &[String]) -> Result<()> {
        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        // Lance supports multi-column indexes
        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);

        // We need to verify dataset exists
        if let Ok(mut ds) = ds_wrapper.open_raw().await {
            // Create composite BTree index
            let index_name = format!("{}_{}_composite", label, properties.join("_"));

            // Convert properties to slice of &str
            let columns: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();

            if let Err(e) = ds
                .create_index(
                    &columns,
                    IndexType::Scalar,
                    Some(index_name.clone()),
                    &ScalarIndexParams::default(),
                    true,
                )
                .await
            {
                warn!("Failed to create physical composite index: {}", e);
            }

            let config = ScalarIndexConfig {
                name: index_name,
                label: label.to_string(),
                properties: properties.to_vec(),
                index_type: uni_common::core::schema::ScalarIndexType::BTree,
                where_clause: None,
            };

            self.schema_manager
                .add_index(IndexDefinition::Scalar(config))?;
            self.schema_manager.save().await?;
        }

        Ok(())
    }

    /// Lookup by composite key
    pub async fn composite_lookup(
        &self,
        label: &str,
        key_values: &HashMap<String, Value>,
    ) -> Result<Option<Vid>> {
        use lancedb::query::{ExecutableQuery, QueryBase, Select};

        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;

        let ds_wrapper = VertexDataset::new(&self.base_uri, label, label_meta.id);
        let table = match ds_wrapper.open_lancedb(&self.lancedb_store).await {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };

        // Build filter from key values
        let filter = key_values
            .iter()
            .map(|(k, v)| {
                let val_str = match v {
                    Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "null".to_string(),
                    _ => v.to_string(),
                };
                // Quote column name for case sensitivity
                format!("\"{}\" = {}", k, val_str)
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        let query = table
            .query()
            .only_if(&filter)
            .limit(1)
            .select(Select::Columns(vec!["_vid".to_string()]));

        let stream = match query.execute().await {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        let batches: Vec<arrow_array::RecordBatch> = stream.try_collect().await.unwrap_or_default();
        for batch in batches {
            if batch.num_rows() > 0 {
                let vid_col = batch
                    .column_by_name("_vid")
                    .ok_or_else(|| anyhow!("Missing _vid column"))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| anyhow!("Invalid _vid column type"))?;

                let vid = vid_col.value(0);
                return Ok(Some(Vid::from(vid)));
            }
        }

        Ok(None)
    }

    /// Applies incremental updates to an inverted index.
    ///
    /// Instead of rebuilding the entire index, this method updates only the
    /// changed entries, making it much faster for small mutations.
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist or the update fails.
    #[instrument(skip(self, added, removed), level = "info", fields(
        label = %config.label,
        property = %config.property
    ))]
    pub async fn update_inverted_index_incremental(
        &self,
        config: &InvertedIndexConfig,
        added: &HashMap<Vid, Vec<String>>,
        removed: &HashSet<Vid>,
    ) -> Result<()> {
        info!(
            added = added.len(),
            removed = removed.len(),
            "Incrementally updating inverted index"
        );

        let mut index = InvertedIndex::new(&self.base_uri, config.clone()).await?;
        index.apply_incremental_updates(added, removed).await
    }
}
