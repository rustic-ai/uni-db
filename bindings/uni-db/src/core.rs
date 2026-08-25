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
use uni_common::UniError;
use uni_common::core::schema::{
    DataType, DistanceMetric, EmbeddingConfig, FullTextIndexConfig, IndexDefinition,
    InvertedIndexConfig, ScalarIndexConfig, ScalarIndexType, TokenizerConfig, VectorIndexConfig,
};

// Re-export types used by the sync and async API modules.
pub use ::uni_db::api::schema::EdgeTypeInfo as UniEdgeTypeInfo;
pub use ::uni_db::api::schema::LabelInfo as UniLabelInfo;
pub use ::uni_db::query_crate::QueryCursor;
pub use ::uni_db::{ExplainOutput, ProfileOutput, QueryResult, Row};

// ============================================================================
// Query Core
// ============================================================================

/// Execute a query with parameters and return result rows.
pub async fn query_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
) -> Result<QueryResult, UniError> {
    let session = db.session();
    let mut builder = session.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    builder.fetch_all().await
}

/// Execute a query with parameters, timeout, and memory limit.
pub async fn query_builder_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
) -> Result<QueryResult, UniError> {
    let session = db.session();
    let mut builder = session.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    if let Some(t) = timeout_secs {
        builder = builder.timeout(Duration::from_secs_f64(t));
    }
    if let Some(m) = max_memory {
        builder = builder.max_memory(m);
    }
    builder.fetch_all().await
}

/// Open a streaming cursor for a query with parameters, timeout, and memory limit.
pub async fn query_cursor_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
) -> Result<QueryCursor, UniError> {
    let session = db.session();
    let mut builder = session.query_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    if let Some(t) = timeout_secs {
        builder = builder.timeout(Duration::from_secs_f64(t));
    }
    if let Some(m) = max_memory {
        builder = builder.max_memory(m);
    }
    builder.cursor().await
}

/// Execute a mutation query, returning affected row count.
pub async fn execute_core(db: &Uni, cypher: &str) -> Result<usize, UniError> {
    let session = db.session();
    let tx = session.tx().await?;
    let result = tx.execute(cypher).await?;
    let affected = result.affected_rows();
    tx.commit().await?;
    Ok(affected)
}

/// Execute a mutation query with parameters, returning affected row count.
pub async fn execute_with_params_core(
    db: &Uni,
    cypher: &str,
    params: HashMap<String, Value>,
) -> Result<usize, UniError> {
    let session = db.session();
    let tx = session.tx().await?;
    let mut builder = tx.execute_with(cypher);
    for (k, v) in params {
        builder = builder.param(&k, v);
    }
    let result = builder.run().await?;
    let affected = result.affected_rows();
    tx.commit().await?;
    Ok(affected)
}

/// Flush all uncommitted changes to persistent storage.
pub async fn flush_core(db: &Uni) -> Result<(), UniError> {
    db.flush().await
}

// ============================================================================
// Schema Core — Pending Types
// ============================================================================

#[derive(Clone)]
pub(crate) struct PendingLabel {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Clone)]
pub(crate) struct PendingEdgeType {
    pub name: String,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub description: Option<String>,
}

#[derive(Clone)]
pub(crate) struct PendingProperty {
    pub label_or_type: String,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub description: Option<String>,
}

// ============================================================================
// Schema Core
// ============================================================================

/// Check if a label exists.
pub async fn label_exists_core(db: &Uni, name: &str) -> Result<bool, UniError> {
    db.label_exists(name).await
}

/// Check if an edge type exists.
pub async fn edge_type_exists_core(db: &Uni, name: &str) -> Result<bool, UniError> {
    db.edge_type_exists(name).await
}

/// List all label names.
pub async fn list_labels_core(db: &Uni) -> Result<Vec<String>, UniError> {
    db.list_labels().await
}

/// List all edge type names.
pub async fn list_edge_types_core(db: &Uni) -> Result<Vec<String>, UniError> {
    db.list_edge_types().await
}

/// Get detailed information about a label.
pub async fn get_label_info_core(db: &Uni, name: &str) -> Result<Option<UniLabelInfo>, UniError> {
    db.get_label_info(name).await
}

/// Get detailed information about an edge type.
pub async fn get_edge_type_info_core(
    db: &Uni,
    name: &str,
) -> Result<Option<UniEdgeTypeInfo>, UniError> {
    db.get_edge_type_info(name).await
}

/// Load schema from a JSON file.
pub async fn load_schema_core(db: &Uni, path: &str) -> Result<(), UniError> {
    db.load_schema(path).await
}

/// Save schema to a JSON file.
pub async fn save_schema_core(db: &Uni, path: &str) -> Result<(), UniError> {
    db.save_schema(path).await
}

/// Apply pending schema changes.
///
/// This is additive/idempotent: labels and edge types that already exist are
/// silently skipped so that the schema builder can be used to add properties
/// or indexes to existing schema elements.
pub(crate) async fn apply_schema_core(
    db: &Uni,
    pending_labels: &[PendingLabel],
    pending_edge_types: &[PendingEdgeType],
    pending_properties: &[PendingProperty],
    pending_indexes: &[IndexDefinition],
) -> Result<(), UniError> {
    use ::uni_db::api::schema::SchemaChange;

    let mut changes = Vec::new();

    for label in pending_labels {
        changes.push(SchemaChange::AddLabel {
            name: label.name.clone(),
            description: label.description.clone(),
        });
    }

    for et in pending_edge_types {
        changes.push(SchemaChange::AddEdgeType {
            name: et.name.clone(),
            from_labels: et.from.clone(),
            to_labels: et.to.clone(),
            description: et.description.clone(),
        });
    }

    for prop in pending_properties {
        changes.push(SchemaChange::AddProperty {
            label_or_type: prop.label_or_type.clone(),
            name: prop.name.clone(),
            data_type: prop.data_type.clone(),
            nullable: prop.nullable,
            description: prop.description.clone(),
        });
    }

    for idx in pending_indexes {
        changes.push(SchemaChange::AddIndex(idx.clone()));
    }

    db.schema().with_changes(changes).apply().await
}

// ============================================================================
// Index Core
// ============================================================================

// ============================================================================
// Bulk Loading Core
// ============================================================================

/// Bulk insert vertices.
pub async fn bulk_insert_vertices_core(
    db: &Uni,
    label: &str,
    rust_props: Vec<HashMap<String, serde_json::Value>>,
) -> Result<Vec<Vid>, UniError> {
    let uni_props: Vec<uni_common::Properties> = rust_props
        .into_iter()
        .map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect())
        .collect();
    let session = db.session();
    let tx = session.tx().await?;
    let vids = tx.bulk_insert_vertices(label, uni_props).await?;
    tx.commit().await?;
    Ok(vids)
}

/// Bulk insert edges.
pub async fn bulk_insert_edges_core(
    db: &Uni,
    edge_type: &str,
    edges: Vec<(Vid, Vid, HashMap<String, serde_json::Value>)>,
) -> Result<(), UniError> {
    let uni_edges: Vec<(Vid, Vid, uni_common::Properties)> = edges
        .into_iter()
        .map(|(s, d, m)| (s, d, m.into_iter().map(|(k, v)| (k, v.into())).collect()))
        .collect();
    let session = db.session();
    let tx = session.tx().await?;
    tx.bulk_insert_edges(edge_type, uni_edges).await?;
    tx.commit().await?;
    Ok(())
}

// ============================================================================
// Xervo Core
// ============================================================================

/// Embed texts using a configured Xervo model alias.
pub async fn xervo_embed_core(
    db: &Uni,
    alias: &str,
    texts: Vec<String>,
) -> Result<Vec<Vec<f32>>, UniError> {
    let xervo = db.xervo();
    let text_refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
    xervo.embed(alias, &text_refs).await
}

/// Generate text using structured messages via a configured Xervo model alias.
pub async fn xervo_generate_core(
    db: &Uni,
    alias: &str,
    messages: Vec<(String, String)>,
    max_tokens: Option<usize>,
    temperature: Option<f32>,
    top_p: Option<f32>,
) -> Result<::uni_db::api::xervo::GenerationResult, UniError> {
    use ::uni_db::api::xervo::{GenerationOptions, Message};
    let xervo = db.xervo();
    // Map each role explicitly and reject anything unrecognized. Silently
    // coercing an unknown role (a typo, or an unsupported role like "tool") to
    // `user` would misattribute the turn and corrupt the conversation.
    let rust_messages: Vec<Message> = messages
        .into_iter()
        .map(|(role, content)| match role.as_str() {
            "user" => Ok(Message::user(content)),
            "assistant" => Ok(Message::assistant(content)),
            "system" => Ok(Message::system(content)),
            _ => Err(UniError::InvalidArgument {
                arg: "role".to_string(),
                message: format!(
                    "unknown message role '{role}'; expected 'user', 'assistant', or 'system'"
                ),
            }),
        })
        .collect::<Result<Vec<Message>, UniError>>()?;
    let opts = GenerationOptions {
        max_tokens,
        temperature,
        top_p,
        width: None,
        height: None,
    };
    xervo.generate(alias, &rust_messages, opts).await
}

/// Pre-load and cache specific Xervo model aliases.
pub async fn xervo_prefetch_core(db: &Uni, aliases: Vec<String>) -> Result<(), UniError> {
    let xervo = db.xervo();
    let alias_refs: Vec<&str> = aliases.iter().map(|s| s.as_str()).collect();
    xervo.prefetch(&alias_refs).await
}

/// Pre-load and cache every model in the Xervo catalog.
pub async fn xervo_prefetch_all_core(db: &Uni) -> Result<(), UniError> {
    db.xervo().prefetch_all().await
}

// ============================================================================
// Snapshot Core
// ============================================================================

/// Create a point-in-time snapshot of the database.
pub async fn create_snapshot_core(db: &Uni, name: &str) -> Result<String, UniError> {
    db.create_snapshot(name).await
}

/// List all available snapshots.
pub async fn list_snapshots_core(
    db: &Uni,
) -> Result<Vec<uni_common::core::snapshot::SnapshotManifest>, UniError> {
    db.list_snapshots().await
}

/// Restore the database to a specific snapshot.
pub async fn restore_snapshot_core(db: &Uni, snapshot_id: &str) -> Result<(), UniError> {
    db.restore_snapshot(snapshot_id).await
}

// ============================================================================
// Compaction Core
// ============================================================================

/// Compact a label or edge type's storage files.
pub async fn compact_core(
    db: &Uni,
    name: &str,
) -> Result<crate::types::PyCompactionStats, UniError> {
    let stats = db.compaction().compact(name).await?;
    Ok(crate::types::PyCompactionStats {
        tables_optimized: stats.tables_optimized,
        fragments_removed: stats.fragments_removed,
        fragments_added: stats.fragments_added,
        files_removed: stats.files_removed,
        files_added: stats.files_added,
        bytes_reclaimed: stats.bytes_reclaimed,
        duration_secs: stats.duration.as_secs_f64(),
        semantic_passes: stats.semantic_passes,
        crdt_merges: stats.crdt_merges,
    })
}

/// Wait for any ongoing compaction to complete.
pub async fn wait_for_compaction_core(db: &Uni) -> Result<(), UniError> {
    db.compaction().wait().await
}

// ============================================================================
// Index Admin Core
// ============================================================================

/// Get status of background index rebuild tasks.
pub async fn index_rebuild_status_core(
    db: &Uni,
) -> Result<Vec<uni_store::storage::IndexRebuildTask>, UniError> {
    db.indexes().rebuild_status().await
}

/// Retry failed index rebuild tasks.
pub async fn retry_index_rebuilds_core(db: &Uni) -> Result<Vec<String>, UniError> {
    db.indexes().retry_failed().await
}

/// Force rebuild indexes for a label (background = true runs in background).
pub async fn rebuild_indexes_core(
    db: &Uni,
    label: &str,
    background: bool,
) -> Result<Option<String>, UniError> {
    db.indexes().rebuild(label, background).await
}

/// List all indexes defined on a specific label.
pub fn list_indexes_core(db: &Uni, label: &str) -> Vec<uni_common::core::schema::IndexDefinition> {
    db.indexes().list(Some(label))
}

/// List all indexes in the database.
pub fn list_all_indexes_core(db: &Uni) -> Vec<uni_common::core::schema::IndexDefinition> {
    db.indexes().list(None)
}

// ============================================================================
// Locy Core
// ============================================================================

/// Evaluate a Locy program with default configuration.
pub async fn locy_evaluate_core(
    db: &Uni,
    program: &str,
) -> Result<::uni_db::locy::LocyResult, UniError> {
    db.session().locy(program).await
}

/// Evaluate a Locy program with custom configuration.
pub async fn locy_evaluate_with_config_core(
    db: &Uni,
    program: &str,
    config: ::uni_db::locy::LocyConfig,
) -> Result<::uni_db::locy::LocyResult, UniError> {
    db.session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await
}

/// Compile a Locy program without executing it.
pub fn locy_compile_only_core(
    db: &Uni,
    program: &str,
) -> Result<::uni_locy::CompiledProgram, UniError> {
    db.session().compile_locy(program)
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
#[allow(clippy::too_many_arguments)]
pub async fn build_database_core(
    uri: &str,
    mode: OpenMode,
    hybrid_local: Option<&str>,
    hybrid_remote: Option<&str>,
    cache_size: Option<usize>,
    parallelism: Option<usize>,
    schema_file: Option<&str>,
    xervo_catalog_json: Option<&str>,
    xervo_catalog_file: Option<&str>,
    cloud_config: Option<uni_common::CloudStorageConfig>,
    uni_config: Option<uni_common::UniConfig>,
    read_only: bool,
    write_lease: Option<::uni_db::api::multi_agent::WriteLease>,
    skip_invalid_locy_rules: bool,
    xervo_runtime: Option<std::sync::Arc<::uni_db::ModelRuntime>>,
) -> Result<Uni, UniError> {
    let mut builder = match mode {
        OpenMode::Open => Uni::open(uri),
        OpenMode::OpenExisting => Uni::open_existing(uri),
        OpenMode::Create => Uni::create(uri),
        OpenMode::Temporary => Uni::temporary(),
    };

    // Apply uni_config first so cache_size/parallelism overrides below take effect
    let mut config = uni_config.unwrap_or_default();

    if let Some(size) = cache_size {
        config.cache_size = size;
    }

    if let Some(n) = parallelism {
        config.parallelism = n;
    }

    builder = builder.config(config);

    if let Some(path) = schema_file {
        builder = builder.schema_file(path);
    }

    // The three Xervo sources are mutually exclusive by construction: each
    // Python setter clears the other two, so at most one is ever `Some` and
    // this branch never has to arbitrate a precedence the caller can't see.
    if let Some(runtime) = xervo_runtime {
        builder = builder.xervo_runtime(runtime);
    } else if let Some(json) = xervo_catalog_json {
        let catalog = ::uni_db::xervo_catalog_from_str(json)
            .map_err(|e| UniError::Internal(anyhow::anyhow!(e.to_string())))?;
        builder = builder.xervo_catalog(catalog);
    } else if let Some(path) = xervo_catalog_file {
        let catalog = ::uni_db::xervo_catalog_from_file(path)
            .map_err(|e| UniError::Internal(anyhow::anyhow!(e.to_string())))?;
        builder = builder.xervo_catalog(catalog);
    }

    if let (Some(_local), Some(remote)) = (hybrid_local, hybrid_remote) {
        if let Some(cc) = cloud_config {
            builder = builder.remote_storage(remote, cc);
        } else {
            // remote_storage requires config; create a minimal one from the URL scheme
            return Err(UniError::Internal(anyhow::anyhow!(
                "remote_storage requires a CloudStorageConfig"
            )));
        }
    }

    if read_only {
        builder = builder.read_only();
    }

    if skip_invalid_locy_rules {
        builder = builder.skip_invalid_locy_rules(true);
    }

    if let Some(wl) = write_lease {
        builder = builder.write_lease(wl);
    }

    builder.build().await
}

// ============================================================================
// Helpers
// ============================================================================

/// Parse a data type string into a DataType enum.
pub fn parse_data_type(data_type: &str) -> Result<DataType, String> {
    // Checked before `vector:` — `sparse_vector:N` does not start with `vector:`,
    // but ordering it first keeps the learned-sparse case unambiguous.
    if let Some(dims_str) = data_type.strip_prefix("sparse_vector:") {
        let dims = dims_str.parse::<usize>().map_err(|_| {
            "Invalid dimensions for sparse_vector type, e.g., 'sparse_vector:30522'".to_string()
        })?;
        Ok(DataType::SparseVector { dimensions: dims })
    } else if data_type.starts_with("vector:") {
        let dims = data_type
            .split(':')
            .nth(1)
            .ok_or_else(|| "Vector type must specify dimensions, e.g., 'vector:128'".to_string())?
            .parse::<usize>()
            .map_err(|_| "Invalid dimensions for vector type".to_string())?;
        Ok(DataType::Vector { dimensions: dims })
    } else if let Some(elem_type) = data_type.strip_prefix("list:") {
        // Keep the FULL remainder (not `split(':').nth(1)`, which drops trailing
        // segments) so nested element types like `list:vector:128` recurse
        // correctly into `parse_data_type("vector:128")`.
        if elem_type.is_empty() {
            return Err("List type must specify element type, e.g., 'list:string'".to_string());
        }
        let inner = parse_data_type(elem_type)?;
        Ok(DataType::List(Box::new(inner)))
    } else if let Some(rest) = data_type.strip_prefix("map:") {
        // `map:KEY:VALUE` — KEY is always a scalar STRING (no `:`), so split on the FIRST
        // `:` and recurse on the FULL VALUE remainder so nested values parse, e.g.
        // `map:string:list:int64`, `map:string:vector:8`, `map:string:map:string:int64`.
        let (key_str, value_str) = rest.split_once(':').ok_or_else(|| {
            "Map type must be 'map:KEY:VALUE', e.g., 'map:string:float64'".to_string()
        })?;
        if key_str.is_empty() || value_str.is_empty() {
            return Err(
                "Map type must specify both key and value, e.g., 'map:string:int64'".to_string(),
            );
        }
        let key_type = parse_data_type(key_str)?;
        if !matches!(key_type, DataType::String) {
            return Err(format!("MAP key type must be STRING, got: {key_str}"));
        }
        let value_type = parse_data_type(value_str)?;
        Ok(DataType::Map(Box::new(key_type), Box::new(value_type)))
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
            "json" => Ok(DataType::CypherValue),
            "btic" => Ok(DataType::Btic),
            "bytes" => Ok(DataType::Bytes),
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
    // The scalar index kinds differ only by their `ScalarIndexType`; build the
    // shared config once and select the kind.
    let scalar_index = |index_type: ScalarIndexType| {
        IndexDefinition::Scalar(ScalarIndexConfig {
            name: format!("idx_{}_{}", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            index_type,
            where_clause: None,
            metadata: Default::default(),
        })
    };

    match index_type.to_lowercase().as_str() {
        "btree" | "scalar" => Ok(scalar_index(ScalarIndexType::BTree)),
        "hash" => Ok(scalar_index(ScalarIndexType::Hash)),
        "bitmap" => Ok(scalar_index(ScalarIndexType::Bitmap)),
        "label_list" | "labellist" => Ok(scalar_index(ScalarIndexType::LabelList)),
        // No options here, so use the canonical defaults from the shared parser (IVF_PQ /
        // Cosine) — identical to the DDL, procedure, and config-map paths.
        "vector" => Ok(IndexDefinition::Vector(VectorIndexConfig {
            name: format!("idx_{}_{}_vec", label, property),
            label: label.to_string(),
            property: property.to_string(),
            index_type: uni_common::vector_index_opts::build_vector_index_type(
                &uni_common::vector_index_opts::VectorIndexOpts::default(),
            ),
            metric: uni_common::vector_index_opts::parse_vector_metric(None)
                .unwrap_or(DistanceMetric::Cosine),
            embedding_config: None,
            metadata: Default::default(),
        })),
        "inverted" => Ok(IndexDefinition::Inverted(InvertedIndexConfig {
            name: format!("idx_{}_{}_inv", label, property),
            label: label.to_string(),
            property: property.to_string(),
            normalize: true,
            max_terms_per_doc: 1024,
            metadata: Default::default(),
        })),
        "fulltext" => Ok(IndexDefinition::FullText(FullTextIndexConfig {
            name: format!("idx_{}_{}_ft", label, property),
            label: label.to_string(),
            properties: vec![property.to_string()],
            tokenizer: TokenizerConfig::Standard,
            with_positions: true,
            metadata: Default::default(),
        })),
        _ => Err(format!("Unknown index type: {}", index_type)),
    }
}

/// Create an index definition from a rich configuration dict.
///
/// The config dict must have a `"type"` key. Supported types:
/// - `"vector"`: optional `algorithm` (`flat`, `ivf_flat`, `ivf_pq` (default),
///   `ivf_sq`, `ivf_rq`, `hnsw_flat`, `hnsw_sq`, `hnsw_pq`, `muvera`), `metric`
///   (`cosine` (default), `l2`, `dot`), `partitions`, `m`, `ef_construction`,
///   `sub_vectors`, `embedding`. For `algorithm: "muvera"` (ColBERT/MaxSim FDE over a
///   multi-vector column): also `k_sim`, `reps`, `d_proj`, `seed`, `inner`. The MUVERA
///   defaults (`k_sim=4, reps=20, d_proj=16`) are starting points, not corpus-validated;
///   recall is corpus-dependent, so tune per corpus (the exact MaxSim re-rank keeps results
///   precise — a weak FDE only costs recall).
/// - `"fulltext"`: optional `tokenizer`, `ngram_min`, `ngram_max`
/// - `"inverted"`: no extra options
/// - `"btree"`, `"hash"`, `"scalar"`: no extra options
pub fn create_index_definition_from_config(
    label: &str,
    property: &str,
    config: &HashMap<String, serde_json::Value>,
) -> Result<IndexDefinition, String> {
    let idx_type = config
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Index config must contain a 'type' key".to_string())?;

    match idx_type.to_lowercase().as_str() {
        "btree" | "scalar" => create_index_definition(label, property, "btree"),
        "hash" => create_index_definition(label, property, "hash"),
        "bitmap" => create_index_definition(label, property, "bitmap"),
        "label_list" | "labellist" => create_index_definition(label, property, "label_list"),
        "inverted" => create_index_definition(label, property, "inverted"),

        "vector" => {
            // Parsed via the SAME uni-common helpers as the Cypher DDL / procedure paths
            // so dense / native-multivector / MUVERA behave identically across surfaces
            // (incl. the canonical default ANN = IVF_PQ and `algorithm: "muvera"`).
            let cfg_u32 = |k: &str| config.get(k).and_then(|v| v.as_u64()).map(|n| n as u32);
            let cfg_u8 = |k: &str| config.get(k).and_then(|v| v.as_u64()).map(|n| n as u8);
            let metric = uni_common::vector_index_opts::parse_vector_metric(
                config.get("metric").and_then(|v| v.as_str()),
            )
            .map_err(|e| e.to_string())?;
            let index_type = uni_common::vector_index_opts::build_vector_index_type(
                &uni_common::vector_index_opts::VectorIndexOpts {
                    type_name: config.get("algorithm").and_then(|v| v.as_str()),
                    partitions: cfg_u32("partitions"),
                    m: cfg_u32("m"),
                    ef_construction: cfg_u32("ef_construction"),
                    sub_vectors: cfg_u32("sub_vectors"),
                    num_bits: cfg_u8("num_bits").or_else(|| cfg_u8("bits_per_subvector")),
                    k_sim: cfg_u32("k_sim"),
                    reps: cfg_u32("reps"),
                    d_proj: cfg_u32("d_proj"),
                    seed: config.get("seed").and_then(|v| v.as_u64()),
                    inner: config.get("inner").and_then(|v| v.as_str()),
                },
            );

            let embedding_config = config.get("embedding").and_then(|v| {
                let obj = v.as_object()?;
                Some(EmbeddingConfig {
                    alias: obj.get("alias")?.as_str()?.to_string(),
                    source_properties: obj
                        .get("source_properties")?
                        .as_array()?
                        .iter()
                        .filter_map(|s| s.as_str().map(|s| s.to_string()))
                        .collect(),
                    batch_size: obj
                        .get("batch_size")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(100) as usize,
                    document_prefix: obj
                        .get("document_prefix")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    query_prefix: obj
                        .get("query_prefix")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                })
            });

            Ok(IndexDefinition::Vector(VectorIndexConfig {
                name: format!("idx_{}_{}_vec", label, property),
                label: label.to_string(),
                property: property.to_string(),
                index_type,
                metric,
                embedding_config,
                metadata: Default::default(),
            }))
        }

        "fulltext" => {
            let tokenizer = match config
                .get("tokenizer")
                .and_then(|v| v.as_str())
                .unwrap_or("standard")
                .to_lowercase()
                .as_str()
            {
                "whitespace" => TokenizerConfig::Whitespace,
                "ngram" => {
                    let min = config
                        .get("ngram_min")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(2) as u8;
                    let max = config
                        .get("ngram_max")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(4) as u8;
                    TokenizerConfig::Ngram { min, max }
                }
                "custom" => {
                    let name = config
                        .get("custom_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("default")
                        .to_string();
                    TokenizerConfig::Custom { name }
                }
                _ => TokenizerConfig::Standard,
            };

            Ok(IndexDefinition::FullText(FullTextIndexConfig {
                name: format!("idx_{}_{}_ft", label, property),
                label: label.to_string(),
                properties: vec![property.to_string()],
                tokenizer,
                with_positions: config
                    .get("with_positions")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                metadata: Default::default(),
            }))
        }

        "sparse" => {
            // Vocabulary size is metadata (the engine reads term ids from the
            // data, not this bound); callers that know it — e.g. the OGM, which
            // has the declared `sparse_vector:N` column — pass it through.
            let dimensions = config
                .get("dimensions")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;
            Ok(IndexDefinition::Sparse(
                uni_common::core::schema::SparseVectorIndexConfig {
                    name: format!("idx_{}_{}_sparse", label, property),
                    label: label.to_string(),
                    property: property.to_string(),
                    dimensions,
                    quantize: true,
                    // OGM auto-embed is out of scope (no modality exposes it via OGM yet).
                    embedding_config: None,
                    metadata: Default::default(),
                },
            ))
        }

        _ => Err(format!("Unknown index type in config: {}", idx_type)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_data_type_scalars_and_vector() {
        assert_eq!(parse_data_type("string").unwrap(), DataType::String);
        assert_eq!(
            parse_data_type("vector:128").unwrap(),
            DataType::Vector { dimensions: 128 }
        );
        assert_eq!(
            parse_data_type("list:string").unwrap(),
            DataType::List(Box::new(DataType::String))
        );
    }

    #[test]
    fn parse_data_type_nested_list_vector() {
        // Regression: `split(':').nth(1)` dropped the trailing dim segment, so
        // `list:vector:128` parsed its element as bare "vector" and failed.
        assert_eq!(
            parse_data_type("list:vector:128").unwrap(),
            DataType::List(Box::new(DataType::Vector { dimensions: 128 }))
        );
        // Doubly-nested element types must also recurse fully.
        assert_eq!(
            parse_data_type("list:list:string").unwrap(),
            DataType::List(Box::new(DataType::List(Box::new(DataType::String))))
        );
    }

    #[test]
    fn parse_data_type_empty_list_element_errors() {
        assert!(parse_data_type("list:").is_err());
    }

    #[test]
    fn parse_data_type_map_scalar_and_nested() {
        assert_eq!(
            parse_data_type("map:string:float64").unwrap(),
            DataType::Map(Box::new(DataType::String), Box::new(DataType::Float64))
        );
        // Nested value types recurse on the full remainder after the first ':'.
        assert_eq!(
            parse_data_type("map:string:list:int64").unwrap(),
            DataType::Map(
                Box::new(DataType::String),
                Box::new(DataType::List(Box::new(DataType::Int64)))
            )
        );
        assert_eq!(
            parse_data_type("map:string:vector:8").unwrap(),
            DataType::Map(
                Box::new(DataType::String),
                Box::new(DataType::Vector { dimensions: 8 })
            )
        );
        assert_eq!(
            parse_data_type("map:string:map:string:int64").unwrap(),
            DataType::Map(
                Box::new(DataType::String),
                Box::new(DataType::Map(
                    Box::new(DataType::String),
                    Box::new(DataType::Int64)
                ))
            )
        );
    }

    #[test]
    fn parse_data_type_map_rejects_bad_forms() {
        assert!(parse_data_type("map:int64:string").is_err()); // non-STRING key
        assert!(parse_data_type("map:string").is_err()); // missing value
        assert!(parse_data_type("map:string:").is_err()); // empty value
    }
}
