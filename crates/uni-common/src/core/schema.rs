// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::core::edge_type::{MAX_SCHEMA_TYPE_ID, is_schemaless_edge_type, make_schemaless_id};
use crate::sync::{acquire_read, acquire_write};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum SchemaElementState {
    Active,
    Hidden {
        since: DateTime<Utc>,
        last_active_snapshot: String, // SnapshotId
    },
    Tombstone {
        since: DateTime<Utc>,
    },
}

use arrow_schema::{DataType as ArrowDataType, Field, TimeUnit};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum CrdtType {
    GCounter,
    GSet,
    ORSet,
    LWWRegister,
    LWWMap,
    Rga,
    VectorClock,
    VCRegister,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum PointType {
    Geographic,  // WGS84
    Cartesian2D, // x, y
    Cartesian3D, // x, y, z
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum DataType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Timestamp,
    Date,
    Time,
    DateTime,
    Duration,
    CypherValue,
    Point(PointType),
    Vector { dimensions: usize },
    Crdt(CrdtType),
    List(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
}

use arrow_schema::Fields;

impl DataType {
    // Alias for compatibility/convenience if needed, but preferable to use exact types.
    #[allow(non_upper_case_globals)]
    pub const Float: DataType = DataType::Float64;
    #[allow(non_upper_case_globals)]
    pub const Int: DataType = DataType::Int64;

    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            DataType::String => ArrowDataType::Utf8,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Timestamp => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            DataType::Date => ArrowDataType::Date32,
            DataType::Time => ArrowDataType::Time64(TimeUnit::Microsecond),
            DataType::DateTime => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            DataType::Duration => ArrowDataType::Duration(TimeUnit::Microsecond),
            DataType::CypherValue => ArrowDataType::LargeBinary, // MessagePack-tagged binary encoding
            DataType::Point(pt) => match pt {
                PointType::Geographic => ArrowDataType::Struct(Fields::from(vec![
                    Field::new("latitude", ArrowDataType::Float64, false),
                    Field::new("longitude", ArrowDataType::Float64, false),
                    Field::new("crs", ArrowDataType::Utf8, false),
                ])),
                PointType::Cartesian2D => ArrowDataType::Struct(Fields::from(vec![
                    Field::new("x", ArrowDataType::Float64, false),
                    Field::new("y", ArrowDataType::Float64, false),
                    Field::new("crs", ArrowDataType::Utf8, false),
                ])),
                PointType::Cartesian3D => ArrowDataType::Struct(Fields::from(vec![
                    Field::new("x", ArrowDataType::Float64, false),
                    Field::new("y", ArrowDataType::Float64, false),
                    Field::new("z", ArrowDataType::Float64, false),
                    Field::new("crs", ArrowDataType::Utf8, false),
                ])),
            },
            DataType::Vector { dimensions } => ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, true)),
                *dimensions as i32,
            ),
            DataType::Crdt(_) => ArrowDataType::Binary, // Store CRDT as binary MessagePack
            DataType::List(inner) => {
                ArrowDataType::List(Arc::new(Field::new("item", inner.to_arrow(), true)))
            }
            DataType::Map(key, value) => ArrowDataType::List(Arc::new(Field::new(
                "item",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("key", key.to_arrow(), false),
                    Field::new("value", value.to_arrow(), true),
                ])),
                true,
            ))),
        }
    }
}

fn default_created_at() -> DateTime<Utc> {
    Utc::now()
}

fn default_state() -> SchemaElementState {
    SchemaElementState::Active
}

fn default_version_1() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropertyMeta {
    pub r#type: DataType,
    pub nullable: bool,
    #[serde(default = "default_version_1")]
    pub added_in: u32, // SchemaVersion
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
    #[serde(default)]
    pub generation_expression: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelMeta {
    pub id: u16, // LabelId
    #[serde(default = "default_created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeTypeMeta {
    /// See [`crate::core::edge_type::EdgeTypeId`] for bit-layout details.
    pub id: u32,
    pub src_labels: Vec<String>,
    pub dst_labels: Vec<String>,
    #[serde(default = "default_state")]
    pub state: SchemaElementState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ConstraintType {
    Unique { properties: Vec<String> },
    Exists { property: String },
    Check { expression: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ConstraintTarget {
    Label(String),
    EdgeType(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Constraint {
    pub name: String,
    pub constraint_type: ConstraintType,
    pub target: ConstraintTarget,
    pub enabled: bool,
}

/// Bidirectional registry for dynamically-assigned schemaless edge type IDs.
///
/// Edge types not defined in the schema are assigned IDs at runtime with
/// bit 31 set (see [`crate::core::edge_type`]). This registry maintains
/// the name-to-ID and ID-to-name mappings for those types.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemalessEdgeTypeRegistry {
    name_to_id: HashMap<String, u32>,
    id_to_name: HashMap<u32, String>,
    /// Next local ID to assign (0 is reserved for invalid).
    next_local_id: u32,
}

impl SchemalessEdgeTypeRegistry {
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            id_to_name: HashMap::new(),
            next_local_id: 1,
        }
    }

    /// Returns the schemaless ID for `type_name`, assigning a new one if needed.
    pub fn get_or_assign_id(&mut self, type_name: &str) -> u32 {
        if let Some(&id) = self.name_to_id.get(type_name) {
            return id;
        }

        let id = make_schemaless_id(self.next_local_id);
        self.next_local_id += 1;

        self.name_to_id.insert(type_name.to_string(), id);
        self.id_to_name.insert(id, type_name.to_string());

        id
    }

    /// Looks up the edge type name for a schemaless ID.
    pub fn type_name_by_id(&self, type_id: u32) -> Option<&str> {
        self.id_to_name.get(&type_id).map(String::as_str)
    }

    /// Returns `true` if `type_name` has already been assigned a schemaless ID.
    pub fn contains(&self, type_name: &str) -> bool {
        self.name_to_id.contains_key(type_name)
    }

    /// Looks up the edge type ID for `type_name` with case-insensitive matching.
    pub fn id_by_name_case_insensitive(&self, type_name: &str) -> Option<u32> {
        self.name_to_id
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(type_name))
            .map(|(_, &id)| id)
    }

    /// Returns all registered schemaless type IDs.
    pub fn all_type_ids(&self) -> Vec<u32> {
        self.id_to_name.keys().copied().collect()
    }

    /// Returns true if the registry has any schemaless types.
    pub fn is_empty(&self) -> bool {
        self.name_to_id.is_empty()
    }
}

impl Default for SchemalessEdgeTypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub schema_version: u32,
    pub labels: HashMap<String, LabelMeta>,
    pub edge_types: HashMap<String, EdgeTypeMeta>,
    pub properties: HashMap<String, HashMap<String, PropertyMeta>>,
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
    #[serde(default)]
    pub constraints: Vec<Constraint>,
    /// Registry for schemaless edge types (dynamically assigned IDs)
    #[serde(default)]
    pub schemaless_registry: SchemalessEdgeTypeRegistry,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            schema_version: 1,
            labels: HashMap::new(),
            edge_types: HashMap::new(),
            properties: HashMap::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
            schemaless_registry: SchemalessEdgeTypeRegistry::new(),
        }
    }
}

impl Schema {
    /// Returns the label name for a given label ID.
    ///
    /// Performs a linear scan over all labels. This is efficient because
    /// the number of labels in a schema is typically small.
    pub fn label_name_by_id(&self, label_id: u16) -> Option<&str> {
        self.labels
            .iter()
            .find(|(_, meta)| meta.id == label_id)
            .map(|(name, _)| name.as_str())
    }

    /// Returns the label ID for a given label name.
    pub fn label_id_by_name(&self, label_name: &str) -> Option<u16> {
        self.labels.get(label_name).map(|meta| meta.id)
    }

    /// Returns the edge type name for a given type ID.
    ///
    /// Performs a linear scan over all edge types. This is efficient because
    /// the number of edge types in a schema is typically small.
    pub fn edge_type_name_by_id(&self, type_id: u32) -> Option<&str> {
        self.edge_types
            .iter()
            .find(|(_, meta)| meta.id == type_id)
            .map(|(name, _)| name.as_str())
    }

    /// Returns the edge type ID for a given type name.
    pub fn edge_type_id_by_name(&self, type_name: &str) -> Option<u32> {
        self.edge_types.get(type_name).map(|meta| meta.id)
    }

    /// Returns the vector index configuration for a given label and property.
    ///
    /// Performs a linear scan over all indexes. This is efficient because
    /// the number of indexes in a schema is typically small.
    pub fn vector_index_for_property(
        &self,
        label: &str,
        property: &str,
    ) -> Option<&VectorIndexConfig> {
        self.indexes.iter().find_map(|idx| {
            if let IndexDefinition::Vector(config) = idx
                && config.label == label
                && config.property == property
            {
                return Some(config);
            }
            None
        })
    }

    /// Get label metadata with case-insensitive lookup.
    ///
    /// This allows queries to match labels regardless of case, providing
    /// better user experience when label names vary in casing.
    pub fn get_label_case_insensitive(&self, name: &str) -> Option<&LabelMeta> {
        self.labels
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    }

    /// Get label ID with case-insensitive lookup.
    pub fn label_id_by_name_case_insensitive(&self, label_name: &str) -> Option<u16> {
        self.get_label_case_insensitive(label_name)
            .map(|meta| meta.id)
    }

    /// Get edge type metadata with case-insensitive lookup.
    ///
    /// This allows queries to match edge types regardless of case, providing
    /// better user experience when type names vary in casing.
    pub fn get_edge_type_case_insensitive(&self, name: &str) -> Option<&EdgeTypeMeta> {
        self.edge_types
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    }

    /// Get edge type ID with case-insensitive lookup (schema-defined types only).
    pub fn edge_type_id_by_name_case_insensitive(&self, type_name: &str) -> Option<u32> {
        self.get_edge_type_case_insensitive(type_name)
            .map(|meta| meta.id)
    }

    /// Get edge type ID with case-insensitive lookup, checking both
    /// schema-defined and schemaless registries.
    pub fn edge_type_id_unified_case_insensitive(&self, type_name: &str) -> Option<u32> {
        self.edge_type_id_by_name_case_insensitive(type_name)
            .or_else(|| {
                self.schemaless_registry
                    .id_by_name_case_insensitive(type_name)
            })
    }

    /// Returns the edge type ID for `type_name`, checking the schema first
    /// and falling back to the schemaless registry (assigning a new ID if needed).
    ///
    /// Requires `&mut self` because it may assign a new schemaless ID.
    /// Use [`edge_type_id_by_name`](Self::edge_type_id_by_name) for read-only schema lookups.
    pub fn get_or_assign_edge_type_id(&mut self, type_name: &str) -> u32 {
        if let Some(id) = self.edge_type_id_by_name(type_name) {
            return id;
        }
        self.schemaless_registry.get_or_assign_id(type_name)
    }

    /// Returns the edge type name for `type_id`, checking both the schema
    /// and schemaless registries. Returns an owned `String` because the
    /// name may come from either registry.
    pub fn edge_type_name_by_id_unified(&self, type_id: u32) -> Option<String> {
        if is_schemaless_edge_type(type_id) {
            self.schemaless_registry
                .type_name_by_id(type_id)
                .map(str::to_owned)
        } else {
            self.edge_type_name_by_id(type_id).map(str::to_owned)
        }
    }

    /// Returns all edge type IDs, including both schema-defined and schemaless types.
    /// Used when MATCH queries don't specify an edge type and need to scan all edges.
    pub fn all_edge_type_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.edge_types.values().map(|m| m.id).collect();
        ids.extend(self.schemaless_registry.all_type_ids());
        ids.sort_unstable();
        ids
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum IndexDefinition {
    Vector(VectorIndexConfig),
    FullText(FullTextIndexConfig),
    Scalar(ScalarIndexConfig),
    Inverted(InvertedIndexConfig),
    JsonFullText(JsonFtsIndexConfig),
}

impl IndexDefinition {
    /// Returns the index name for any variant.
    pub fn name(&self) -> &str {
        match self {
            IndexDefinition::Vector(c) => &c.name,
            IndexDefinition::FullText(c) => &c.name,
            IndexDefinition::Scalar(c) => &c.name,
            IndexDefinition::Inverted(c) => &c.name,
            IndexDefinition::JsonFullText(c) => &c.name,
        }
    }

    /// Returns the label this index is defined on.
    pub fn label(&self) -> &str {
        match self {
            IndexDefinition::Vector(c) => &c.label,
            IndexDefinition::FullText(c) => &c.label,
            IndexDefinition::Scalar(c) => &c.label,
            IndexDefinition::Inverted(c) => &c.label,
            IndexDefinition::JsonFullText(c) => &c.label,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InvertedIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    #[serde(default = "default_normalize")]
    pub normalize: bool,
    #[serde(default = "default_max_terms_per_doc")]
    pub max_terms_per_doc: usize,
}

fn default_normalize() -> bool {
    true
}

fn default_max_terms_per_doc() -> usize {
    10_000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    pub index_type: VectorIndexType,
    pub metric: DistanceMetric,
    pub embedding_config: Option<EmbeddingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingConfig {
    pub model: EmbeddingModel,
    pub source_properties: Vec<String>,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "provider")]
#[non_exhaustive]
pub enum EmbeddingModel {
    /// Candle-based text embeddings (default, native Rust).
    /// Uses sentence-transformer models like all-MiniLM-L6-v2.
    Candle {
        /// Model name (e.g., "all-MiniLM-L6-v2", "bge-small-en-v1.5")
        model_name: String,
        /// Optional HuggingFace model revision/tag
        revision: Option<String>,
    },
    /// Legacy ONNX-based FastEmbed (requires `fastembed` feature).
    FastEmbed {
        model_name: String,
        cache_dir: Option<String>,
        max_length: Option<usize>,
    },
    /// OpenAI embedding API (future).
    OpenAI {
        model: String,
        api_key_env: String,
        dimensions: Option<u32>,
    },
    /// Ollama local embedding service (future).
    Ollama { model: String, host: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum VectorIndexType {
    IvfPq {
        num_partitions: u32,
        num_sub_vectors: u32,
        bits_per_subvector: u8,
    },
    Hnsw {
        m: u32,
        ef_construction: u32,
        ef_search: u32,
    },
    Flat,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum DistanceMetric {
    Cosine,
    L2,
    Dot,
}

impl DistanceMetric {
    /// Computes the distance between two vectors using this metric.
    ///
    /// All metrics follow LanceDB conventions so that lower values indicate
    /// greater similarity:
    /// - **L2**: squared Euclidean distance.
    /// - **Cosine**: `1.0 - cosine_similarity` (range \[0, 2\]).
    /// - **Dot**: negative dot product.
    ///
    /// # Panics
    ///
    /// Panics if `a` and `b` have different lengths.
    pub fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        assert_eq!(a.len(), b.len(), "vector dimension mismatch");
        match self {
            DistanceMetric::L2 => a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum(),
            DistanceMetric::Cosine => {
                let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
                let norm_a: f32 = a.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
                let norm_b: f32 = b.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
                let denom = norm_a * norm_b;
                if denom == 0.0 { 1.0 } else { 1.0 - dot / denom }
            }
            DistanceMetric::Dot => {
                let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
                -dot
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FullTextIndexConfig {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub tokenizer: TokenizerConfig,
    pub with_positions: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum TokenizerConfig {
    Standard,
    Whitespace,
    Ngram { min: u8, max: u8 },
    Custom { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonFtsIndexConfig {
    pub name: String,
    pub label: String,
    pub column: String,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub with_positions: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScalarIndexConfig {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub index_type: ScalarIndexType,
    pub where_clause: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ScalarIndexType {
    BTree,
    Hash,
    Bitmap,
}

pub struct SchemaManager {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
    schema: RwLock<Schema>,
}

impl SchemaManager {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("Invalid schema path"))?;
        let filename = path
            .file_name()
            .ok_or_else(|| anyhow!("Invalid schema filename"))?
            .to_str()
            .ok_or_else(|| anyhow!("Invalid utf8 filename"))?;

        let store = Arc::new(LocalFileSystem::new_with_prefix(parent)?);
        let obj_path = ObjectStorePath::from(filename);

        Self::load_from_store(store, &obj_path).await
    }

    pub async fn load_from_store(
        store: Arc<dyn ObjectStore>,
        path: &ObjectStorePath,
    ) -> Result<Self> {
        match store.get(path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let content = String::from_utf8(bytes.to_vec())?;
                let schema: Schema = serde_json::from_str(&content)?;
                Ok(Self {
                    store,
                    path: path.clone(),
                    schema: RwLock::new(schema),
                })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self {
                store,
                path: path.clone(),
                schema: RwLock::new(Schema::default()),
            }),
            Err(e) => Err(anyhow::Error::from(e)),
        }
    }

    pub async fn save(&self) -> Result<()> {
        let content = {
            let schema_guard = acquire_read(&self.schema, "schema")?;
            serde_json::to_string_pretty(&*schema_guard)?
        };
        self.store
            .put(&self.path, content.into())
            .await
            .map_err(anyhow::Error::from)?;
        Ok(())
    }

    pub fn path(&self) -> &ObjectStorePath {
        &self.path
    }

    pub fn schema(&self) -> Schema {
        self.schema
            .read()
            .expect("Schema lock poisoned - a thread panicked while holding it")
            .clone()
    }

    /// Normalize function names in an expression to uppercase for case-insensitive matching.
    /// Examples: "lower(email)" -> "LOWER(email)", "trim(name)" -> "TRIM(name)"
    fn normalize_function_names(expr: &str) -> String {
        let mut result = String::with_capacity(expr.len());
        let mut chars = expr.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch.is_alphabetic() {
                // Collect identifier
                let mut ident = String::new();
                ident.push(ch);

                while let Some(&next) = chars.peek() {
                    if next.is_alphanumeric() || next == '_' {
                        ident.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                // If followed by '(', it's a function call - uppercase it
                if chars.peek() == Some(&'(') {
                    result.push_str(&ident.to_uppercase());
                } else {
                    result.push_str(&ident); // Keep property names as-is
                }
            } else {
                result.push(ch);
            }
        }

        result
    }

    /// Generate a consistent internal column name for an expression index.
    /// Uses a hash suffix to ensure uniqueness for different expressions that
    /// might sanitize to the same string (e.g., "a+b" and "a-b" both become "a_b").
    ///
    /// IMPORTANT: Uses FNV-1a hash which is stable across Rust versions and platforms.
    /// DefaultHasher is not guaranteed to be stable and could break persistent data
    /// if the hash changes after a compiler upgrade.
    pub fn generated_column_name(expr: &str) -> String {
        // Normalize function names to uppercase for case-insensitive matching
        let normalized = Self::normalize_function_names(expr);

        let sanitized = normalized
            .replace(|c: char| !c.is_alphanumeric(), "_")
            .trim_matches('_')
            .to_string();

        // FNV-1a 64-bit hash - stable across Rust versions and platforms
        const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;

        let mut hash = FNV_OFFSET_BASIS;
        for byte in normalized.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }

        format!("_gen_{}_{:x}", sanitized, hash)
    }

    pub fn replace_schema(&self, new_schema: Schema) {
        let mut schema = self
            .schema
            .write()
            .expect("Schema lock poisoned - a thread panicked while holding it");
        *schema = new_schema;
    }

    pub fn next_label_id(&self) -> u16 {
        self.schema()
            .labels
            .values()
            .map(|l| l.id)
            .max()
            .unwrap_or(0)
            + 1
    }

    pub fn next_type_id(&self) -> u32 {
        let max_schema_id = self
            .schema()
            .edge_types
            .values()
            .map(|t| t.id)
            .max()
            .unwrap_or(0);

        // Ensure we stay in schema'd ID space (bit 31 = 0)
        if max_schema_id >= MAX_SCHEMA_TYPE_ID {
            panic!("Schema edge type ID exhaustion");
        }

        max_schema_id + 1
    }

    pub fn add_label(&self, name: &str) -> Result<u16> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if schema.labels.contains_key(name) {
            return Err(anyhow!("Label '{}' already exists", name));
        }

        let id = schema.labels.values().map(|l| l.id).max().unwrap_or(0) + 1;
        schema.labels.insert(
            name.to_string(),
            LabelMeta {
                id,
                created_at: Utc::now(),
                state: SchemaElementState::Active,
            },
        );
        Ok(id)
    }

    pub fn add_edge_type(
        &self,
        name: &str,
        src_labels: Vec<String>,
        dst_labels: Vec<String>,
    ) -> Result<u32> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if schema.edge_types.contains_key(name) {
            return Err(anyhow!("Edge type '{}' already exists", name));
        }

        let id = schema.edge_types.values().map(|t| t.id).max().unwrap_or(0) + 1;

        // Ensure we stay in schema'd ID space (bit 31 = 0)
        if id >= MAX_SCHEMA_TYPE_ID {
            return Err(anyhow!("Schema edge type ID exhaustion"));
        }

        schema.edge_types.insert(
            name.to_string(),
            EdgeTypeMeta {
                id,
                src_labels,
                dst_labels,
                state: SchemaElementState::Active,
            },
        );
        Ok(id)
    }

    /// Delegates to [`Schema::get_or_assign_edge_type_id`].
    pub fn get_or_assign_edge_type_id(&self, type_name: &str) -> u32 {
        let mut schema = acquire_write(&self.schema, "schema")
            .expect("Schema lock poisoned - a thread panicked while holding it");
        schema.get_or_assign_edge_type_id(type_name)
    }

    /// Delegates to [`Schema::edge_type_name_by_id_unified`].
    pub fn edge_type_name_by_id_unified(&self, type_id: u32) -> Option<String> {
        let schema = acquire_read(&self.schema, "schema")
            .expect("Schema lock poisoned - a thread panicked while holding it");
        schema.edge_type_name_by_id_unified(type_id)
    }

    pub fn add_property(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        let version = schema.schema_version;
        let props = schema
            .properties
            .entry(label_or_type.to_string())
            .or_default();

        if props.contains_key(prop_name) {
            return Err(anyhow!(
                "Property '{}' already exists for '{}'",
                prop_name,
                label_or_type
            ));
        }

        props.insert(
            prop_name.to_string(),
            PropertyMeta {
                r#type: data_type,
                nullable,
                added_in: version,
                state: SchemaElementState::Active,
                generation_expression: None,
            },
        );
        Ok(())
    }

    pub fn add_generated_property(
        &self,
        label_or_type: &str,
        prop_name: &str,
        data_type: DataType,
        expr: String,
    ) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        let version = schema.schema_version;
        let props = schema
            .properties
            .entry(label_or_type.to_string())
            .or_default();

        if props.contains_key(prop_name) {
            return Err(anyhow!("Property '{}' already exists", prop_name));
        }

        props.insert(
            prop_name.to_string(),
            PropertyMeta {
                r#type: data_type,
                nullable: true,
                added_in: version,
                state: SchemaElementState::Active,
                generation_expression: Some(expr),
            },
        );
        Ok(())
    }

    pub fn add_index(&self, index_def: IndexDefinition) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        schema.indexes.push(index_def);
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<IndexDefinition> {
        let schema = self.schema.read().expect("Schema lock poisoned");
        schema.indexes.iter().find(|i| i.name() == name).cloned()
    }

    pub fn remove_index(&self, name: &str) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(pos) = schema.indexes.iter().position(|i| i.name() == name) {
            schema.indexes.remove(pos);
            Ok(())
        } else {
            Err(anyhow!("Index '{}' not found", name))
        }
    }

    pub fn add_constraint(&self, constraint: Constraint) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if schema.constraints.iter().any(|c| c.name == constraint.name) {
            return Err(anyhow!("Constraint '{}' already exists", constraint.name));
        }
        schema.constraints.push(constraint);
        Ok(())
    }

    pub fn drop_constraint(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(pos) = schema.constraints.iter().position(|c| c.name == name) {
            schema.constraints.remove(pos);
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Constraint '{}' not found", name))
        }
    }

    pub fn drop_property(&self, label_or_type: &str, prop_name: &str) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(props) = schema.properties.get_mut(label_or_type) {
            if props.remove(prop_name).is_some() {
                Ok(())
            } else {
                Err(anyhow!(
                    "Property '{}' not found for '{}'",
                    prop_name,
                    label_or_type
                ))
            }
        } else {
            Err(anyhow!("Label or Edge Type '{}' not found", label_or_type))
        }
    }

    pub fn rename_property(
        &self,
        label_or_type: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(props) = schema.properties.get_mut(label_or_type) {
            if let Some(meta) = props.remove(old_name) {
                if props.contains_key(new_name) {
                    // Rollback removal? Or just error.
                    props.insert(old_name.to_string(), meta); // Restore
                    return Err(anyhow!("Property '{}' already exists", new_name));
                }
                props.insert(new_name.to_string(), meta);
                Ok(())
            } else {
                Err(anyhow!(
                    "Property '{}' not found for '{}'",
                    old_name,
                    label_or_type
                ))
            }
        } else {
            Err(anyhow!("Label or Edge Type '{}' not found", label_or_type))
        }
    }

    pub fn drop_label(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(label_meta) = schema.labels.get_mut(name) {
            label_meta.state = SchemaElementState::Tombstone { since: Utc::now() };
            // Do not remove properties; they are implicitly tombstoned by the label
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Label '{}' not found", name))
        }
    }

    pub fn drop_edge_type(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut schema = acquire_write(&self.schema, "schema")?;
        if let Some(edge_meta) = schema.edge_types.get_mut(name) {
            edge_meta.state = SchemaElementState::Tombstone { since: Utc::now() };
            // Do not remove properties; they are implicitly tombstoned by the edge type
            Ok(())
        } else if if_exists {
            Ok(())
        } else {
            Err(anyhow!("Edge Type '{}' not found", name))
        }
    }
}

/// Validate identifier names to prevent injection and ensure compatibility.
pub fn validate_identifier(name: &str) -> Result<()> {
    // Length check
    if name.is_empty() || name.len() > 64 {
        return Err(anyhow!("Identifier '{}' must be 1-64 characters", name));
    }

    // First character must be letter or underscore
    let first = name.chars().next().unwrap();
    if !first.is_alphabetic() && first != '_' {
        return Err(anyhow!(
            "Identifier '{}' must start with letter or underscore",
            name
        ));
    }

    // Remaining characters: alphanumeric or underscore
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(anyhow!(
            "Identifier '{}' must contain only alphanumeric and underscore",
            name
        ));
    }

    // Reserved words
    const RESERVED: &[&str] = &[
        "MATCH", "CREATE", "DELETE", "SET", "RETURN", "WHERE", "MERGE", "CALL", "YIELD", "WITH",
        "UNION", "ORDER", "LIMIT",
    ];
    if RESERVED.contains(&name.to_uppercase().as_str()) {
        return Err(anyhow!("Identifier '{}' cannot be a reserved word", name));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_schema_management() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let path = ObjectStorePath::from("schema.json");
        let manager = SchemaManager::load_from_store(store.clone(), &path).await?;

        // Labels
        let lid = manager.add_label("Person")?;
        assert_eq!(lid, 1);
        assert!(manager.add_label("Person").is_err());

        // Properties
        manager.add_property("Person", "name", DataType::String, false)?;
        assert!(
            manager
                .add_property("Person", "name", DataType::String, false)
                .is_err()
        );

        // Edge types
        let tid = manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
        assert_eq!(tid, 1);

        manager.save().await?;
        // Check file exists
        assert!(store.get(&path).await.is_ok());

        let manager2 = SchemaManager::load_from_store(store, &path).await?;
        assert!(manager2.schema().labels.contains_key("Person"));
        assert!(
            manager2
                .schema()
                .properties
                .get("Person")
                .unwrap()
                .contains_key("name")
        );

        Ok(())
    }

    #[test]
    fn test_normalize_function_names() {
        assert_eq!(
            SchemaManager::normalize_function_names("lower(email)"),
            "LOWER(email)"
        );
        assert_eq!(
            SchemaManager::normalize_function_names("LOWER(email)"),
            "LOWER(email)"
        );
        assert_eq!(
            SchemaManager::normalize_function_names("Lower(email)"),
            "LOWER(email)"
        );
        assert_eq!(
            SchemaManager::normalize_function_names("trim(lower(email))"),
            "TRIM(LOWER(email))"
        );
    }

    #[test]
    fn test_generated_column_name_case_insensitive() {
        let col1 = SchemaManager::generated_column_name("lower(email)");
        let col2 = SchemaManager::generated_column_name("LOWER(email)");
        let col3 = SchemaManager::generated_column_name("Lower(email)");
        assert_eq!(col1, col2);
        assert_eq!(col2, col3);
        assert!(col1.starts_with("_gen_LOWER_email_"));
    }
}
