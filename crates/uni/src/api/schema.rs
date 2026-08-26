// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::api::Uni;
use std::collections::HashMap;
use std::path::Path;
use uni_common::core::schema::{
    AnalyzerConfig, DataType, DistanceMetric, EmbeddingConfig, FullTextIndexConfig,
    IndexDefinition, ScalarIndexConfig, ScalarIndexType, TokenizerConfig, VectorIndexConfig,
    VectorIndexType,
};
use uni_common::{Result, UniError};

/// Builder for defining and modifying the graph schema.
///
/// Use this builder to define labels, edge types, properties, and indexes.
/// Changes are batched and applied atomically when `.apply()` is called.
///
/// # Example
///
/// ```no_run
/// # async fn example(db: &uni_db::Uni) -> uni_db::Result<()> {
/// db.schema()
///     .label("Person")
///         .property("name", uni_db::DataType::String)
///         .property("age", uni_db::DataType::Int64)
///         .vector("embedding", 1536) // Adds property AND vector index
///         .index("name", uni_db::IndexType::Scalar(uni_db::ScalarType::BTree))
///     .edge_type("KNOWS", &["Person"], &["Person"])
///         .property("since", uni_db::DataType::Date)
///     .apply()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "schema builders do nothing until .apply() or .current() is called"]
pub struct SchemaBuilder<'a> {
    pub(crate) db: &'a Uni,
    pending: Vec<SchemaChange>,
}

pub enum SchemaChange {
    AddLabel {
        name: String,
        description: Option<String>,
    },
    AddProperty {
        label_or_type: String,
        name: String,
        data_type: DataType,
        nullable: bool,
        description: Option<String>,
    },
    AddIndex(IndexDefinition),
    AddEdgeType {
        name: String,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
        description: Option<String>,
    },
}

/// Stage an `AddProperty` change; shared by the identical `property` /
/// `property_nullable` / `property_described` methods on `LabelBuilder`
/// and `EdgeTypeBuilder`.
fn push_add_property(
    pending: &mut Vec<SchemaChange>,
    owner: &str,
    name: &str,
    data_type: DataType,
    nullable: bool,
    description: Option<String>,
) {
    pending.push(SchemaChange::AddProperty {
        label_or_type: owner.to_string(),
        name: name.to_string(),
        data_type,
        nullable,
        description,
    });
}

impl<'a> SchemaBuilder<'a> {
    pub fn new(db: &'a Uni) -> Self {
        Self {
            db,
            pending: Vec::new(),
        }
    }

    /// Get the current schema (read-only snapshot).
    pub fn current(&self) -> std::sync::Arc<uni_common::core::schema::Schema> {
        self.db.inner.schema.schema()
    }

    /// Add pre-built schema changes to this builder.
    pub fn with_changes(mut self, changes: Vec<SchemaChange>) -> Self {
        self.pending.extend(changes);
        self
    }

    /// Create a label (node type) in the schema.
    ///
    /// Labels can be **schemaless** (no properties defined) or **typed** (with properties).
    ///
    /// # Schemaless Labels
    ///
    /// Labels without property definitions support flexible, dynamic properties:
    /// - Properties not in schema are stored in `overflow_json` (JSONB binary)
    /// - Queries on overflow properties are automatically rewritten to JSONB functions
    /// - No schema migration needed to add new properties
    ///
    /// # Example: Schemaless Label
    ///
    /// ```ignore
    /// // Create label with no properties
    /// db.schema().label("Document").apply().await?;
    ///
    /// // Create with arbitrary properties
    /// db.execute("CREATE (:Document {title: 'Article', author: 'Alice', year: 2024})").await?;
    ///
    /// // Query works transparently (automatic query rewriting)
    /// db.query("MATCH (d:Document) WHERE d.author = 'Alice' RETURN d.title, d.year").await?;
    /// ```
    ///
    /// # Example: Typed Label with Overflow
    ///
    /// ```ignore
    /// // Define core properties in schema
    /// db.schema()
    ///     .label("Person")
    ///     .property("name", DataType::String)
    ///     .property("age", DataType::Int)
    ///     .apply().await?;
    ///
    /// // Can still add overflow properties dynamically
    /// db.execute("CREATE (:Person {name: 'Bob', age: 25, city: 'NYC'})").await?;
    /// //                                                   ^^^^^^^^^^^
    /// //                                                   overflow property
    ///
    /// // Query mixing schema and overflow properties
    /// db.query("MATCH (p:Person) WHERE p.name = 'Bob' AND p.city = 'NYC' RETURN p.age").await?;
    /// ```
    ///
    /// **Performance Note**: Schema properties use typed columns (faster filtering/sorting),
    /// while overflow properties use JSONB (flexible but slower). Use schema properties
    /// for core, frequently-queried fields.
    pub fn label(self, name: &str) -> LabelBuilder<'a> {
        LabelBuilder::new(self, name.to_string())
    }

    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilder<'a> {
        EdgeTypeBuilder::new(
            self,
            name.to_string(),
            from.iter().map(|s| s.to_string()).collect(),
            to.iter().map(|s| s.to_string()).collect(),
        )
    }

    pub async fn apply(self) -> Result<()> {
        let manager = &self.db.inner.schema;
        let mut indexes_to_build = Vec::new();

        for change in self.pending {
            match change {
                SchemaChange::AddLabel { name, description } => {
                    match manager.add_label_with_desc(&name, description) {
                        Ok(_) => {}
                        Err(e) if e.to_string().contains("already exists") => {}
                        Err(e) => {
                            return Err(UniError::Schema {
                                message: e.to_string(),
                            });
                        }
                    }
                }
                SchemaChange::AddProperty {
                    label_or_type,
                    name,
                    data_type,
                    nullable,
                    description,
                } => {
                    // `declare_property` is idempotent for an identical re-declaration
                    // (the register-on-every-open pattern) but errors on a type or
                    // nullability conflict. The old `add_property_with_desc` +
                    // swallow-"already exists" combination silently ignored dim
                    // changes like VECTOR(4) → VECTOR(8) (issue #137).
                    manager
                        .declare_property(&label_or_type, &name, data_type, nullable, description)
                        .map_err(|e| UniError::Schema {
                            message: e.to_string(),
                        })?;
                }
                SchemaChange::AddIndex(idx) => {
                    // The Python config paths build an `IndexDefinition` with no
                    // schema in scope, so a vector index can still carry the
                    // `AUTO_SUB_VECTORS` sentinel here. Resolve it (and attach a
                    // refine default) against the declared column type before it
                    // is registered, so a persisted schema never holds the
                    // sentinel regardless of which surface created the index.
                    let idx = match idx {
                        uni_common::core::schema::IndexDefinition::Vector(mut cfg) => {
                            let snapshot = manager.schema();
                            let dim =
                                uni_store::storage::index_manager::index_build_dim(&snapshot, &cfg);
                            uni_store::storage::index_manager::resolve_vector_index_defaults(
                                &mut cfg, dim, None,
                            );
                            uni_common::core::schema::IndexDefinition::Vector(cfg)
                        }
                        other => other,
                    };
                    // Skip the synchronous Lance rebuild when the index is
                    // already registered with the same config — re-applying
                    // the same schema is the documented "register on every
                    // KB-open" pattern, and rebuilding all indexes per
                    // re-apply is what made KB-open take minutes (issue
                    // rustic-ai/uni-db#63). The `add_index` call below is
                    // upsert-by-name, so it stays cheap regardless.
                    let already_present = manager
                        .get_index(idx.name())
                        .is_some_and(|existing| existing == idx);
                    manager
                        .add_index(idx.clone())
                        .map_err(|e| UniError::Schema {
                            message: e.to_string(),
                        })?;
                    if !already_present {
                        indexes_to_build.push(idx.label().to_string());
                    }
                }
                SchemaChange::AddEdgeType {
                    name,
                    from_labels,
                    to_labels,
                    description,
                } => {
                    // Keep the requested endpoint labels to compare against the
                    // stored definition if the edge type already exists.
                    let requested_from = from_labels.clone();
                    let requested_to = to_labels.clone();
                    match manager.add_edge_type_with_desc(
                        &name,
                        from_labels,
                        to_labels,
                        description,
                    ) {
                        Ok(_) => {}
                        Err(e) if e.to_string().contains("already exists") => {
                            // `add_edge_type_with_desc` errors on NAME collision
                            // and returns BEFORE updating the stored labels. A
                            // re-declaration is idempotent only when the existing
                            // endpoint labels match; a re-declaration with
                            // different from/to labels is a real conflict that
                            // must not be silently swallowed (which would leave
                            // the stale definition). Mirror `declare_property`
                            // and surface it.
                            let existing_schema = manager.schema();
                            let matches = existing_schema
                                .get_edge_type_case_insensitive(&name)
                                .is_some_and(|m| {
                                    m.src_labels == requested_from && m.dst_labels == requested_to
                                });
                            if !matches {
                                return Err(UniError::Schema {
                                    message: format!(
                                        "edge type '{name}' already exists with different \
                                         endpoint labels; drop it before re-declaring with \
                                         from={requested_from:?} to={requested_to:?}"
                                    ),
                                });
                            }
                        }
                        Err(e) => {
                            return Err(UniError::Schema {
                                message: e.to_string(),
                            });
                        }
                    }
                }
            }
        }

        manager.save().await.map_err(UniError::Internal)?;

        // Trigger index builds for affected labels
        // We use a set to avoid rebuilding same label multiple times if multiple indexes added
        indexes_to_build.sort();
        indexes_to_build.dedup();
        for label in indexes_to_build {
            // Trigger async rebuild
            // Note: If synchronous behavior is desired, pass false.
            // But usually schema changes should be fast, so async build is better?
            // The prompt says "Indexes Not Built During Schema Changes", implying they should be.
            // Let's do it synchronously to ensure they are ready, matching user expectation.
            self.db.indexes().rebuild(&label, false).await?;
        }

        Ok(())
    }
}

#[must_use = "builders do nothing until .done() or .apply() is called"]
pub struct LabelBuilder<'a> {
    builder: SchemaBuilder<'a>,
    name: String,
    description: Option<String>,
}

impl<'a> LabelBuilder<'a> {
    fn new(builder: SchemaBuilder<'a>, name: String) -> Self {
        Self {
            builder,
            name,
            description: None,
        }
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    pub fn property(mut self, name: &str, data_type: DataType) -> Self {
        push_add_property(
            &mut self.builder.pending,
            &self.name,
            name,
            data_type,
            false,
            None,
        );
        self
    }

    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self {
        push_add_property(
            &mut self.builder.pending,
            &self.name,
            name,
            data_type,
            true,
            None,
        );
        self
    }

    pub fn property_described(mut self, name: &str, data_type: DataType, desc: &str) -> Self {
        push_add_property(
            &mut self.builder.pending,
            &self.name,
            name,
            data_type,
            false,
            Some(desc.to_string()),
        );
        self
    }

    pub fn vector(self, name: &str, dimensions: usize) -> Self {
        self.property(name, DataType::Vector { dimensions })
    }

    pub fn index(mut self, property: &str, index_type: IndexType) -> Self {
        let idx = match index_type {
            IndexType::Vector(cfg) => IndexDefinition::Vector(VectorIndexConfig {
                name: format!("idx_{}_{}", self.name, property),
                label: self.name.clone(),
                property: property.to_string(),
                index_type: cfg.algorithm.into_internal(),
                metric: cfg.metric.into_internal(),
                embedding_config: cfg.embedding.map(|e| e.into_internal()),
                // The typed `VectorAlgo` takes an explicit `sub_vectors`, so
                // there is no sentinel to resolve here; the refine default is
                // still filled in on apply.
                default_refine_factor: None,
                metadata: Default::default(),
            }),
            IndexType::FullText => IndexDefinition::FullText(FullTextIndexConfig {
                name: format!("fts_{}_{}", self.name, property),
                label: self.name.clone(),
                properties: vec![property.to_string()],
                tokenizer: TokenizerConfig::Standard,
                with_positions: true,
                metadata: Default::default(),
            }),
            IndexType::FullTextWithAnalyzer(analyzer) => {
                IndexDefinition::FullText(FullTextIndexConfig {
                    name: format!("fts_{}_{}", self.name, property),
                    label: self.name.clone(),
                    properties: vec![property.to_string()],
                    tokenizer: TokenizerConfig::Analyzer(analyzer),
                    with_positions: true,
                    metadata: Default::default(),
                })
            }
            IndexType::Scalar(stype) => IndexDefinition::Scalar(ScalarIndexConfig {
                name: format!("idx_{}_{}", self.name, property),
                label: self.name.clone(),
                properties: vec![property.to_string()],
                index_type: stype.into_internal(),
                where_clause: None,
                metadata: Default::default(),
            }),
            IndexType::Inverted(config) => IndexDefinition::Inverted(config),
            IndexType::Sparse {
                dimensions,
                quantize,
                embedding,
            } => IndexDefinition::Sparse(uni_common::core::schema::SparseVectorIndexConfig {
                name: format!("idx_{}_{}", self.name, property),
                label: self.name.clone(),
                property: property.to_string(),
                dimensions,
                quantize,
                embedding_config: embedding.map(EmbeddingCfg::into_internal),
                metadata: Default::default(),
            }),
        };
        self.builder.pending.push(SchemaChange::AddIndex(idx));
        self
    }

    pub fn done(mut self) -> SchemaBuilder<'a> {
        self.builder.pending.insert(
            0,
            SchemaChange::AddLabel {
                name: self.name,
                description: self.description,
            },
        );
        self.builder
    }

    // Chaining
    pub fn label(self, name: &str) -> LabelBuilder<'a> {
        self.done().label(name)
    }

    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilder<'a> {
        self.done().edge_type(name, from, to)
    }

    pub async fn apply(self) -> Result<()> {
        self.done().apply().await
    }
}

#[must_use = "builders do nothing until .done() or .apply() is called"]
pub struct EdgeTypeBuilder<'a> {
    builder: SchemaBuilder<'a>,
    name: String,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    description: Option<String>,
}

impl<'a> EdgeTypeBuilder<'a> {
    fn new(
        builder: SchemaBuilder<'a>,
        name: String,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
    ) -> Self {
        Self {
            builder,
            name,
            from_labels,
            to_labels,
            description: None,
        }
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    pub fn property(mut self, name: &str, data_type: DataType) -> Self {
        push_add_property(
            &mut self.builder.pending,
            &self.name,
            name,
            data_type,
            false,
            None,
        );
        self
    }

    pub fn property_nullable(mut self, name: &str, data_type: DataType) -> Self {
        push_add_property(
            &mut self.builder.pending,
            &self.name,
            name,
            data_type,
            true,
            None,
        );
        self
    }

    pub fn property_described(mut self, name: &str, data_type: DataType, desc: &str) -> Self {
        push_add_property(
            &mut self.builder.pending,
            &self.name,
            name,
            data_type,
            false,
            Some(desc.to_string()),
        );
        self
    }

    pub fn done(mut self) -> SchemaBuilder<'a> {
        self.builder.pending.insert(
            0,
            SchemaChange::AddEdgeType {
                name: self.name,
                from_labels: self.from_labels,
                to_labels: self.to_labels,
                description: self.description,
            },
        );
        self.builder
    }

    pub fn label(self, name: &str) -> LabelBuilder<'a> {
        self.done().label(name)
    }

    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> EdgeTypeBuilder<'a> {
        self.done().edge_type(name, from, to)
    }

    pub async fn apply(self) -> Result<()> {
        self.done().apply().await
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LabelInfo {
    pub name: String,
    pub count: usize,
    pub properties: Vec<PropertyInfo>,
    pub indexes: Vec<IndexInfo>,
    pub constraints: Vec<ConstraintInfo>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EdgeTypeInfo {
    pub name: String,
    pub count: usize,
    pub source_labels: Vec<String>,
    pub target_labels: Vec<String>,
    pub properties: Vec<PropertyInfo>,
    pub indexes: Vec<IndexInfo>,
    pub constraints: Vec<ConstraintInfo>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PropertyInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_indexed: bool,
    pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub index_type: String,
    pub properties: Vec<String>,
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConstraintInfo {
    pub name: String,
    pub constraint_type: String,
    pub properties: Vec<String>,
    pub enabled: bool,
}

#[non_exhaustive]
pub enum IndexType {
    Vector(VectorIndexCfg),
    /// Full-text index using the default (standard) analyzer.
    FullText,
    /// Full-text index with an explicit analyzer pipeline (tokenizer, language,
    /// stemming, stop words, ...). Construct via [`IndexType::full_text_with_analyzer`].
    FullTextWithAnalyzer(AnalyzerConfig),
    Scalar(ScalarType),
    Inverted(uni_common::core::schema::InvertedIndexConfig),
    /// Scored sparse-vector (SPLADE / learned-sparse) index. `dimensions` is the
    /// term-space cardinality of the column; `quantize` stores 8-bit per-term
    /// quantized weights (≈ lossless, ~4× smaller; default on); `embedding`
    /// auto-embeds a text column into the sparse column via a xervo sparse model.
    Sparse {
        dimensions: usize,
        quantize: bool,
        embedding: Option<EmbeddingCfg>,
    },
}

impl IndexType {
    /// A full-text index with an explicit analyzer pipeline.
    ///
    /// Use this to configure the base tokenizer, language, stemming, stop-word
    /// removal, ASCII folding and token-length limits. Plain
    /// [`IndexType::FullText`] keeps the default (standard) analyzer.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use uni_common::core::schema::{AnalyzerConfig, FtsLanguage};
    /// let idx = IndexType::full_text_with_analyzer(AnalyzerConfig {
    ///     language: FtsLanguage::French,
    ///     ..AnalyzerConfig::default()
    /// });
    /// ```
    #[must_use]
    pub fn full_text_with_analyzer(analyzer: AnalyzerConfig) -> Self {
        Self::FullTextWithAnalyzer(analyzer)
    }

    /// A sparse-vector index over a `dimensions`-wide term space with 8-bit
    /// weight quantization enabled (the default; use the [`IndexType::Sparse`]
    /// struct variant directly to disable it or to set auto-embedding).
    #[must_use]
    pub fn sparse(dimensions: usize) -> Self {
        Self::Sparse {
            dimensions,
            quantize: true,
            embedding: None,
        }
    }

    /// A sparse-vector index that auto-embeds a text column into the sparse
    /// column via the given xervo sparse model (quantization on).
    #[must_use]
    pub fn sparse_with_embedding(dimensions: usize, embedding: EmbeddingCfg) -> Self {
        Self::Sparse {
            dimensions,
            quantize: true,
            embedding: Some(embedding),
        }
    }
}

pub struct VectorIndexCfg {
    pub algorithm: VectorAlgo,
    pub metric: VectorMetric,
    pub embedding: Option<EmbeddingCfg>,
}

/// Embedding configuration for auto-embedding during index writes.
pub struct EmbeddingCfg {
    /// Model alias from the Uni-Xervo catalog (for example: "embed/default").
    pub alias: String,
    pub source_properties: Vec<String>,
    pub batch_size: usize,
    /// Prefix prepended to text before embedding during auto-embed (document side).
    /// Example: `"search_document: "` for Nomic models. Include any trailing space.
    pub document_prefix: Option<String>,
    /// Prefix prepended to text before embedding during query-time embed calls.
    /// Example: `"search_query: "` for Nomic models. Include any trailing space.
    pub query_prefix: Option<String>,
}

impl EmbeddingCfg {
    fn into_internal(self) -> EmbeddingConfig {
        EmbeddingConfig {
            alias: self.alias,
            source_properties: self.source_properties,
            batch_size: self.batch_size,
            document_prefix: self.document_prefix,
            query_prefix: self.query_prefix,
        }
    }
}

#[non_exhaustive]
pub enum VectorAlgo {
    Flat,
    IvfFlat {
        partitions: u32,
    },
    IvfPq {
        partitions: u32,
        sub_vectors: u32,
    },
    IvfSq {
        partitions: u32,
    },
    IvfRq {
        partitions: u32,
        num_bits: Option<u8>,
    },
    Hnsw {
        m: u32,
        ef_construction: u32,
        partitions: Option<u32>,
    },
    HnswFlat {
        m: u32,
        ef_construction: u32,
        partitions: Option<u32>,
    },
    HnswSq {
        m: u32,
        ef_construction: u32,
        partitions: Option<u32>,
    },
    HnswPq {
        m: u32,
        ef_construction: u32,
        sub_vectors: u32,
        partitions: Option<u32>,
    },
    /// MUVERA (ColBERT/MaxSim) Fixed-Dimensional Encoding: the source multi-vector is
    /// encoded into a derived single-vector column indexed by `inner`. Use
    /// [`crate::api::schema::DEFAULT_FDE_SEED`] for `seed` unless reproducing a specific
    /// transform. See `uni_common::muvera`.
    Muvera {
        k_sim: u32,
        reps: u32,
        d_proj: u32,
        seed: u64,
        inner: Box<VectorAlgo>,
    },
}

/// Default MUVERA FDE seed for [`VectorAlgo::Muvera`] (re-exported for ergonomics).
pub use uni_common::muvera::DEFAULT_FDE_SEED;

impl VectorAlgo {
    fn into_internal(self) -> VectorIndexType {
        match self {
            VectorAlgo::Flat => VectorIndexType::Flat,
            VectorAlgo::IvfFlat { partitions } => VectorIndexType::IvfFlat {
                num_partitions: partitions,
            },
            VectorAlgo::IvfPq {
                partitions,
                sub_vectors,
            } => VectorIndexType::IvfPq {
                num_partitions: partitions,
                num_sub_vectors: sub_vectors,
                bits_per_subvector: 8,
            },
            VectorAlgo::IvfSq { partitions } => VectorIndexType::IvfSq {
                num_partitions: partitions,
            },
            VectorAlgo::IvfRq {
                partitions,
                num_bits,
            } => VectorIndexType::IvfRq {
                num_partitions: partitions,
                num_bits,
            },
            VectorAlgo::HnswFlat {
                m,
                ef_construction,
                partitions,
            } => VectorIndexType::HnswFlat {
                m,
                ef_construction,
                num_partitions: partitions,
            },
            VectorAlgo::Hnsw {
                m,
                ef_construction,
                partitions,
            }
            | VectorAlgo::HnswSq {
                m,
                ef_construction,
                partitions,
            } => VectorIndexType::HnswSq {
                m,
                ef_construction,
                num_partitions: partitions,
            },
            VectorAlgo::HnswPq {
                m,
                ef_construction,
                sub_vectors,
                partitions,
            } => VectorIndexType::HnswPq {
                m,
                ef_construction,
                num_sub_vectors: sub_vectors,
                num_partitions: partitions,
            },
            VectorAlgo::Muvera {
                k_sim,
                reps,
                d_proj,
                seed,
                inner,
            } => VectorIndexType::Muvera {
                k_sim,
                reps,
                d_proj,
                seed,
                inner: Box::new(inner.into_internal()),
            },
        }
    }
}

#[non_exhaustive]
pub enum VectorMetric {
    Cosine,
    L2,
    Dot,
    /// L1 / Manhattan distance. Searched exact/brute-force — L1 columns cannot
    /// build an ANN vector index.
    L1,
}

impl VectorMetric {
    fn into_internal(self) -> DistanceMetric {
        match self {
            VectorMetric::Cosine => DistanceMetric::Cosine,
            VectorMetric::L2 => DistanceMetric::L2,
            VectorMetric::Dot => DistanceMetric::Dot,
            VectorMetric::L1 => DistanceMetric::L1,
        }
    }
}

#[non_exhaustive]
pub enum ScalarType {
    BTree,
    Hash,
    Bitmap,
    LabelList,
}

impl ScalarType {
    fn into_internal(self) -> ScalarIndexType {
        match self {
            ScalarType::BTree => ScalarIndexType::BTree,
            ScalarType::Hash => ScalarIndexType::Hash,
            ScalarType::Bitmap => ScalarIndexType::Bitmap,
            ScalarType::LabelList => ScalarIndexType::LabelList,
        }
    }
}

// ============================================================================
// Schema introspection
// ============================================================================

/// Whether a schema element (label or edge type) is present and `Active`.
///
/// Shared by `label_exists` / `edge_type_exists`; `state` is the looked-up
/// element's state (`None` when the element is absent from the schema).
fn element_active(state: Option<&uni_common::core::schema::SchemaElementState>) -> bool {
    matches!(
        state,
        Some(uni_common::core::schema::SchemaElementState::Active)
    )
}

/// Backtick-quote a schema element name for interpolation into Cypher.
///
/// `validate_schema_element_name` admits far more than Cypher's unquoted
/// identifier grammar (`[A-Za-z_][A-Za-z0-9_]*`): punctuation such as `-` and
/// `.` — the latter explicitly documented as supported for qualified names —
/// leading digits, and every non-ASCII character, since `cypher.pest` matches
/// `ASCII_ALPHA` only. Interpolating any of those unquoted produces a parse
/// error rather than a query.
///
/// # Errors
///
/// A name containing a backtick cannot be expressed at all: the grammar's
/// quoted form is `` "`" ~ (!"`" ~ ANY)* ~ "`" `` with no doubling or escape
/// rule, so there is no encoding for it. Such a name is refused rather than
/// silently mis-parsed.
fn quote_cypher_identifier(name: &str) -> Result<String> {
    if name.contains('`') {
        return Err(UniError::Query {
            message: format!(
                "schema element name {name:?} contains a backtick, which Cypher's \
                 quoted-identifier syntax cannot escape"
            ),
            query: None,
        });
    }
    Ok(format!("`{name}`"))
}

/// Build the `PropertyInfo` projection for a label or edge type.
///
/// Shared by [`Uni::get_label_info`] and [`Uni::get_edge_type_info`];
/// `is_indexed` is supplied per element kind because labels consult more
/// index variants (vector / JSON-FTS) than edge types do — keeping the
/// exact per-kind predicate preserves the original behavior.
fn property_infos_for(
    schema: &uni_common::core::schema::Schema,
    name: &str,
    is_indexed: impl Fn(&uni_common::core::schema::IndexDefinition, &str, &str) -> bool,
) -> Vec<crate::api::schema::PropertyInfo> {
    let mut properties = Vec::new();
    if let Some(props) = schema.properties.get(name) {
        for (prop_name, prop_meta) in props {
            properties.push(crate::api::schema::PropertyInfo {
                name: prop_name.clone(),
                data_type: format!("{:?}", prop_meta.r#type),
                nullable: prop_meta.nullable,
                is_indexed: schema
                    .indexes
                    .iter()
                    .any(|idx| is_indexed(idx, name, prop_name)),
                description: prop_meta.description.clone(),
            });
        }
    }
    properties
}

/// Build the `IndexInfo` projection for a label or edge type.
///
/// `descriptor` maps each index targeting `name` to its `(type, props)`
/// pair, returning `None` to skip variants that do not apply to this
/// element kind (e.g. edge types skip vector / JSON-FTS indexes).
fn index_infos_for(
    schema: &uni_common::core::schema::Schema,
    name: &str,
    descriptor: impl Fn(
        &uni_common::core::schema::IndexDefinition,
    ) -> Option<(&'static str, Vec<String>)>,
) -> Vec<crate::api::schema::IndexInfo> {
    let mut indexes = Vec::new();
    for idx in schema.indexes.iter().filter(|i| i.label() == name) {
        let Some((idx_type, idx_props)) = descriptor(idx) else {
            continue;
        };
        indexes.push(crate::api::schema::IndexInfo {
            name: idx.name().to_string(),
            index_type: idx_type.to_string(),
            properties: idx_props,
            status: "ONLINE".to_string(), // TODO: Check actual status
        });
    }
    indexes
}

/// Build the `ConstraintInfo` projection for a label or edge type.
///
/// `target_matches` selects the constraints whose target matches `name`
/// (`ConstraintTarget::Label` for labels, `EdgeType` for edge types).
fn constraint_infos_for(
    schema: &uni_common::core::schema::Schema,
    target_matches: impl Fn(&uni_common::core::schema::Constraint) -> bool,
) -> Vec<crate::api::schema::ConstraintInfo> {
    use uni_common::core::schema::ConstraintType;
    let mut constraints = Vec::new();
    for c in &schema.constraints {
        if !target_matches(c) {
            continue;
        }
        let (ctype, cprops) = match &c.constraint_type {
            ConstraintType::Unique { properties } => ("UNIQUE", properties.clone()),
            ConstraintType::Exists { property } => ("EXISTS", vec![property.clone()]),
            ConstraintType::Check { expression } => ("CHECK", vec![expression.clone()]),
            ConstraintType::NodeKey { properties } => ("NODE KEY", properties.clone()),
            _ => ("UNKNOWN", vec![]),
        };
        constraints.push(crate::api::schema::ConstraintInfo {
            name: c.name.clone(),
            constraint_type: ctype.to_string(),
            properties: cprops,
            enabled: c.enabled,
        });
    }
    constraints
}

/// `is_indexed` predicate for label properties (consults vector, scalar,
/// full-text, inverted, and JSON-FTS index variants).
fn label_property_is_indexed(
    idx: &uni_common::core::schema::IndexDefinition,
    name: &str,
    prop_name: &str,
) -> bool {
    use uni_common::core::schema::IndexDefinition;
    match idx {
        IndexDefinition::Vector(v) => v.label == name && v.property.as_str() == prop_name,
        IndexDefinition::Scalar(s) => {
            s.label == name && s.properties.iter().any(|p| p == prop_name)
        }
        IndexDefinition::FullText(f) => {
            f.label == name && f.properties.iter().any(|p| p == prop_name)
        }
        IndexDefinition::Inverted(inv) => inv.label == name && inv.property.as_str() == prop_name,
        IndexDefinition::JsonFullText(j) => j.label == name,
        _ => false,
    }
}

/// `is_indexed` predicate for edge-type properties (scalar, full-text,
/// and inverted only — edges carry no vector / JSON-FTS indexes).
///
/// Every other variant defers to [`label_property_is_indexed`]; a future
/// `IndexDefinition` variant therefore starts applying to edge types too
/// unless it is added to the skip list here.
fn edge_property_is_indexed(
    idx: &uni_common::core::schema::IndexDefinition,
    name: &str,
    prop_name: &str,
) -> bool {
    use uni_common::core::schema::IndexDefinition;
    match idx {
        IndexDefinition::Vector(_) | IndexDefinition::JsonFullText(_) => false,
        other => label_property_is_indexed(other, name, prop_name),
    }
}

/// Index `(type, props)` descriptor for labels (maps all five variants).
fn label_index_descriptor(
    idx: &uni_common::core::schema::IndexDefinition,
) -> Option<(&'static str, Vec<String>)> {
    use uni_common::core::schema::IndexDefinition;
    match idx {
        IndexDefinition::Vector(v) => Some(("VECTOR", vec![v.property.clone()])),
        IndexDefinition::Scalar(s) => Some(("SCALAR", s.properties.clone())),
        IndexDefinition::FullText(f) => Some(("FULLTEXT", f.properties.clone())),
        IndexDefinition::Inverted(inv) => Some(("INVERTED", vec![inv.property.clone()])),
        IndexDefinition::JsonFullText(j) => Some(("JSON_FTS", vec![j.column.clone()])),
        _ => None,
    }
}

/// Index `(type, props)` descriptor for edge types (skips vector /
/// JSON-FTS variants).
///
/// Every other variant defers to [`label_index_descriptor`]; a future
/// `IndexDefinition` variant therefore starts being reported for edge types
/// too unless it is added to the skip list here.
fn edge_index_descriptor(
    idx: &uni_common::core::schema::IndexDefinition,
) -> Option<(&'static str, Vec<String>)> {
    use uni_common::core::schema::IndexDefinition;
    match idx {
        IndexDefinition::Vector(_) | IndexDefinition::JsonFullText(_) => None,
        other => label_index_descriptor(other),
    }
}

impl Uni {
    pub fn schema(&self) -> SchemaBuilder<'_> {
        SchemaBuilder::new(self)
    }

    pub async fn load_schema(&self, path: impl AsRef<Path>) -> Result<()> {
        // We can't easily "replace" the SchemaManager's schema in-place if it's already Arc-ed around.
        // But SchemaManager has internal RwLock<Schema>.
        // Let's check if we can add a method to SchemaManager to reload.
        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(UniError::Io)?;
        let schema: uni_common::core::schema::Schema =
            serde_json::from_str(&content).map_err(|e| UniError::Schema {
                message: e.to_string(),
            })?;

        // We need a way to update the schema in SchemaManager.
        // I'll add a `replace_schema` or similar to SchemaManager.
        self.inner.schema.replace_schema(schema);
        Ok(())
    }

    pub async fn save_schema(&self, path: impl AsRef<Path>) -> Result<()> {
        let content = serde_json::to_string_pretty(&self.inner.schema.schema()).map_err(|e| {
            UniError::Schema {
                message: e.to_string(),
            }
        })?;
        tokio::fs::write(path, content)
            .await
            .map_err(UniError::Io)?;
        Ok(())
    }

    /// Check if a label exists in the schema.
    pub async fn label_exists(&self, name: &str) -> Result<bool> {
        let schema = self.inner.schema.schema();
        Ok(element_active(schema.labels.get(name).map(|l| &l.state)))
    }

    /// Check if an edge type exists in the schema.
    pub async fn edge_type_exists(&self, name: &str) -> Result<bool> {
        let schema = self.inner.schema.schema();
        Ok(element_active(
            schema.edge_types.get(name).map(|e| &e.state),
        ))
    }

    /// Get all label names.
    /// Returns the union of schema-registered labels (Active state) and labels
    /// discovered from data (for schemaless mode where labels may not be in the
    /// schema). This is consistent with `list_edge_types()` for schema labels
    /// while also supporting schemaless workflows.
    pub async fn list_labels(&self) -> Result<Vec<String>> {
        let mut all_labels = std::collections::HashSet::new();

        // Schema labels (covers schema-defined labels that may not have data yet)
        for (name, label) in self.inner.schema.schema().labels.iter() {
            if matches!(
                label.state,
                uni_common::core::schema::SchemaElementState::Active
            ) {
                all_labels.insert(name.clone());
            }
        }

        // Data labels (covers schemaless labels that aren't in the schema)
        let query = "MATCH (n) RETURN DISTINCT labels(n) AS labels";
        let result = self.inner.execute_internal(query, HashMap::new()).await?;
        for row in result.rows() {
            if let Ok(labels_list) = row.get::<Vec<String>>("labels") {
                for label in labels_list {
                    all_labels.insert(label);
                }
            }
        }

        Ok(all_labels.into_iter().collect())
    }

    /// Get all edge type names.
    pub async fn list_edge_types(&self) -> Result<Vec<String>> {
        Ok(self
            .inner
            .schema
            .schema()
            .edge_types
            .iter()
            .filter(|(_, e)| {
                matches!(
                    e.state,
                    uni_common::core::schema::SchemaElementState::Active
                )
            })
            .map(|(name, _)| name.clone())
            .collect())
    }

    // (schema-projection helpers `property_infos_for` / `index_infos_for`
    //  / `constraint_infos_for` are free functions defined above this impl.)

    /// Get detailed information about a label.
    pub async fn get_label_info(
        &self,
        name: &str,
    ) -> Result<Option<crate::api::schema::LabelInfo>> {
        let schema = self.inner.schema.schema();
        if let Some(label_meta) = schema.labels.get(name) {
            // Row count via Cypher, matching `get_edge_type_info`.
            //
            // `backend.count_rows` reads flushed storage only, so a label whose
            // rows were still in the L0 buffers reported `count: 0` — a silent
            // wrong answer, and the reason a Python assertion on this value was
            // once weakened rather than fixed.
            //
            // This does not reintroduce #115: that fix moved the count off the
            // raw-dataset `open_raw()` path, whose `.lance` URI was wrong so it
            // reported 0 for *flushed* tables. Cypher is a third path and is
            // subject to neither failure.
            let quoted = quote_cypher_identifier(name)?;
            let query = format!("MATCH (n:{quoted}) RETURN count(n) AS cnt");
            let count = self
                .inner
                .execute_internal(&query, HashMap::new())
                .await?
                .rows()
                .first()
                .and_then(|r| r.get::<i64>("cnt").ok())
                .unwrap_or(0) as usize;

            Ok(Some(crate::api::schema::LabelInfo {
                name: name.to_string(),
                count,
                properties: property_infos_for(&schema, name, label_property_is_indexed),
                indexes: index_infos_for(&schema, name, label_index_descriptor),
                constraints: constraint_infos_for(
                    &schema,
                    |c| matches!(&c.target, uni_common::core::schema::ConstraintTarget::Label(l) if l == name),
                ),
                description: label_meta.description.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Get detailed information about an edge type.
    pub async fn get_edge_type_info(
        &self,
        name: &str,
    ) -> Result<Option<crate::api::schema::EdgeTypeInfo>> {
        let schema = self.inner.schema.schema();
        let edge_meta = match schema.edge_types.get(name) {
            Some(meta) => meta,
            None => return Ok(None),
        };

        // Count edges via internal query.
        //
        // The Cypher round-trip is deliberate: unlike `count_rows` it sees the
        // L0 buffers as well as flushed storage, and it respects MVCC — the
        // main tables are append-only with `_deleted`/`_version` columns, so a
        // bare row count would include tombstones and superseded versions.
        //
        // The type name MUST be backtick-quoted. `relationship_types` in
        // `cypher.pest` accepts `identifier_or_keyword`, whose unquoted form is
        // `[A-Za-z_][A-Za-z0-9_]*` — so an unquoted interpolation is a parse
        // error for any name outside that shape, and
        // `validate_schema_element_name` admits far more than that: punctuation
        // (including `.`, which it documents as supported), leading digits, and
        // all non-ASCII. Paired with the `Err(_) => 0` this silently reported an
        // empty edge type instead of failing.
        let count = {
            let quoted = quote_cypher_identifier(name)?;
            let query = format!("MATCH ()-[r:{quoted}]->() RETURN count(r) AS cnt");
            let result = self.inner.execute_internal(&query, HashMap::new()).await?;
            result
                .rows()
                .first()
                .and_then(|r| r.get::<i64>("cnt").ok())
                .unwrap_or(0) as usize
        };

        let source_labels = edge_meta.src_labels.clone();
        let target_labels = edge_meta.dst_labels.clone();

        Ok(Some(crate::api::schema::EdgeTypeInfo {
            name: name.to_string(),
            count,
            source_labels,
            target_labels,
            properties: property_infos_for(&schema, name, edge_property_is_indexed),
            indexes: index_infos_for(&schema, name, edge_index_descriptor),
            constraints: constraint_infos_for(
                &schema,
                |c| matches!(&c.target, uni_common::core::schema::ConstraintTarget::EdgeType(et) if et == name),
            ),
            description: edge_meta.description.clone(),
        }))
    }
}
