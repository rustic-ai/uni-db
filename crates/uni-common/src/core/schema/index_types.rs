// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Index configuration types: the `IndexDefinition` variant table and the
//! per-kind config structs it carries (vector, full-text, scalar, inverted,
//! JSON full-text, sparse), plus their shared lifecycle metadata.
//!
//! Re-exported from the parent `schema` module, so every
//! `uni_common::core::schema::*` path keeps resolving.

use super::*;

/// Lifecycle status of an index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum IndexStatus {
    /// Index is up-to-date and available for queries.
    #[default]
    Online,
    /// Index is currently being rebuilt.
    Building,
    /// Index is outdated and scheduled for rebuild.
    Stale,
    /// Index rebuild failed after exhausting retries.
    Failed,
}

/// Metadata tracking the lifecycle state of an index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct IndexMetadata {
    /// Current lifecycle status.
    #[serde(default)]
    pub status: IndexStatus,
    /// When the index was last successfully built.
    #[serde(default)]
    pub last_built_at: Option<DateTime<Utc>>,
    /// Row count of the dataset when the index was last built.
    #[serde(default)]
    pub row_count_at_build: Option<u64>,
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
    /// Scored sparse-vector (SPLADE / learned-sparse) inverted index.
    Sparse(SparseVectorIndexConfig),
}

/// Single source of truth for the `IndexDefinition` variant table.
///
/// Every accessor below (`name`, `label`, `metadata`, `metadata_mut`) drives
/// off this list, so adding an index kind means editing exactly one place.
/// Mirrors the `for_each_crdt_variant!` idiom in `uni-crdt`.
macro_rules! for_each_index_variant {
    ($mac:ident) => {
        $mac! { Vector, FullText, Scalar, Inverted, JsonFullText, Sparse }
    };
}

/// Generate a `&self -> &T` accessor that forwards to the same field on
/// whichever config the variant carries.
macro_rules! index_field_accessor {
    ($($variant:ident),*) => {
        impl IndexDefinition {
            /// Returns the index name for any variant.
            pub fn name(&self) -> &str {
                match self { $(IndexDefinition::$variant(c) => &c.name,)* }
            }

            /// Returns the label this index is defined on.
            pub fn label(&self) -> &str {
                match self { $(IndexDefinition::$variant(c) => &c.label,)* }
            }

            /// Returns a reference to the index lifecycle metadata.
            pub fn metadata(&self) -> &IndexMetadata {
                match self { $(IndexDefinition::$variant(c) => &c.metadata,)* }
            }

            /// Returns a mutable reference to the index lifecycle metadata.
            pub fn metadata_mut(&mut self) -> &mut IndexMetadata {
                match self { $(IndexDefinition::$variant(c) => &mut c.metadata,)* }
            }
        }
    };
}

for_each_index_variant!(index_field_accessor);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InvertedIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    #[serde(default = "default_normalize")]
    pub normalize: bool,
    #[serde(default = "default_max_terms_per_doc")]
    pub max_terms_per_doc: usize,
    #[serde(default)]
    pub metadata: IndexMetadata,
}

fn default_normalize() -> bool {
    true
}

fn default_max_terms_per_doc() -> usize {
    10_000
}

/// Configuration for a scored sparse-vector (SPLADE / learned-sparse) index.
///
/// The index stores per-term postings `(term_id, vids, weights, max_impact)`
/// and scores by dot product. `quantize` controls 8-bit weight quantization at
/// the postings boundary (≈ lossless, ~4× smaller; default on). P2 block-max
/// pruning knobs are added in a later milestone.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SparseVectorIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    /// Term-space cardinality (max term id + 1), for validation/config.
    pub dimensions: usize,
    /// Quantize stored weights to 8-bit (per-term scale). Default on.
    #[serde(default = "default_sparse_quantize")]
    pub quantize: bool,
    /// Auto-embedding source. When set, a declared text column is embedded into
    /// this sparse column via the xervo sparse model at write time (and a text
    /// query is embedded at query time) — mirrors `VectorIndexConfig`.
    #[serde(default)]
    pub embedding_config: Option<EmbeddingConfig>,
    #[serde(default)]
    pub metadata: IndexMetadata,
}

fn default_sparse_quantize() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    pub index_type: VectorIndexType,
    pub metric: DistanceMetric,
    pub embedding_config: Option<EmbeddingConfig>,
    #[serde(default)]
    pub metadata: IndexMetadata,
    /// `refine_factor` applied when a query does not pass one, for quantized
    /// (PQ/SQ/RQ) indexes.
    ///
    /// PQ ranks candidates on lossy codes; a refine pass re-scores them against
    /// the original vectors. Without it recall is capped by quantization rather
    /// than by search breadth — measured at 0.56 on a 32x-compressed 1M corpus
    /// versus 0.99 with a refine pass, for ~3% throughput. Resolved at index
    /// creation from the compression ratio; `None` on indexes created before
    /// this existed, in which case the query path computes an equivalent value.
    /// An explicit `refine_factor` in the query always wins, including `1`.
    #[serde(default)]
    pub default_refine_factor: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingConfig {
    /// Model alias in the Uni-Xervo catalog (for example: "embed/default").
    pub alias: String,
    pub source_properties: Vec<String>,
    pub batch_size: usize,
    /// Prefix prepended to text before embedding during auto-embed (document side).
    /// Example: `"search_document: "` for Nomic models. Include any trailing space.
    #[serde(default)]
    pub document_prefix: Option<String>,
    /// Prefix prepended to text before embedding during query-time embed calls.
    /// Example: `"search_query: "` for Nomic models. Include any trailing space.
    #[serde(default)]
    pub query_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum VectorIndexType {
    Flat,
    IvfFlat {
        num_partitions: u32,
    },
    IvfPq {
        num_partitions: u32,
        num_sub_vectors: u32,
        bits_per_subvector: u8,
    },
    IvfSq {
        num_partitions: u32,
    },
    IvfRq {
        num_partitions: u32,
        #[serde(default)]
        num_bits: Option<u8>,
    },
    HnswFlat {
        m: u32,
        ef_construction: u32,
        #[serde(default)]
        num_partitions: Option<u32>,
    },
    HnswSq {
        m: u32,
        ef_construction: u32,
        #[serde(default)]
        num_partitions: Option<u32>,
    },
    HnswPq {
        m: u32,
        ef_construction: u32,
        num_sub_vectors: u32,
        #[serde(default)]
        num_partitions: Option<u32>,
    },
    /// MUVERA (arXiv:2405.19504) Fixed-Dimensional Encoding for multi-vector
    /// (ColBERT/MaxSim) columns. The source multi-vector is encoded into a single
    /// derived `Vector<fde_dim>` column, and `inner` is the single-vector ANN index
    /// type built over that derived column (always with the `Dot` metric — the FDE
    /// inner product approximates MaxSim). The exact MaxSim re-rank still uses the
    /// `VectorIndexConfig.metric`. See `uni_query_functions::muvera`.
    Muvera {
        /// SimHash hyperplanes per repetition (`2^k_sim` buckets).
        k_sim: u32,
        /// Independent repetitions concatenated into the FDE.
        reps: u32,
        /// Inner-projection target dim (`0` = no projection, use the source dim).
        d_proj: u32,
        /// Master seed; persisted so query-time encoding matches doc-time encoding.
        seed: u64,
        /// The single-vector ANN index built over the derived FDE column.
        inner: Box<VectorIndexType>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum DistanceMetric {
    Cosine,
    L2,
    Dot,
    /// L1 / Manhattan distance (`Σ|xᵢ − yᵢ|`). Exact/brute-force only — Lance
    /// ANN indexes do not support it, so an L1 column cannot build an ANN index.
    L1,
    /// Hamming distance over binary vectors: the number of differing bits
    /// (`Σ popcount(aᵢ ⊕ bᵢ)` across `u8` lanes). Applies only to
    /// [`DataType::BinaryVector`] columns. Exact/brute-force only — Lance ANN
    /// over binary metrics is not wired here.
    Hamming,
    /// Jaccard distance over binary vectors: `1 − |A ∩ B| / |A ∪ B|` computed
    /// bitwise (two all-zero vectors are defined as distance `0`). Applies only
    /// to [`DataType::BinaryVector`] columns. Exact/brute-force only — Lance has
    /// no Jaccard ANN.
    Jaccard,
}

impl DistanceMetric {
    /// Computes the distance between two vectors using this metric.
    ///
    /// All metrics follow LanceDB conventions so that lower values indicate
    /// greater similarity:
    /// - **L2**: squared Euclidean distance.
    /// - **Cosine**: `1.0 - cosine_similarity` (range \[0, 2\]).
    /// - **Dot**: negative dot product.
    /// - **L1**: Manhattan distance (`Σ|xᵢ − yᵢ|`).
    ///
    /// # Panics
    ///
    /// Panics if `a` and `b` have different lengths.
    pub fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        assert_eq!(a.len(), b.len(), "vector dimension mismatch");
        match self {
            DistanceMetric::L2 => a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum(),
            DistanceMetric::L1 => a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum(),
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
            // Binary metrics operate on `&[u8]`, not float lanes; routing a
            // float vector here is a programming error (a `BinaryVector` column
            // is scored via `compute_distance_binary`).
            DistanceMetric::Hamming | DistanceMetric::Jaccard => {
                panic!("{self:?} is a binary-vector metric; use compute_distance_binary")
            }
        }
    }

    /// Returns `true` if this metric operates on binary vectors (`&[u8]` lanes)
    /// rather than float vectors — i.e. [`DistanceMetric::Hamming`] or
    /// [`DistanceMetric::Jaccard`].
    ///
    /// Callers use this to route between [`DistanceMetric::compute_distance`]
    /// and [`DistanceMetric::compute_distance_binary`], and to decide that a
    /// column is scored exact/brute-force (binary metrics have no ANN backend).
    pub fn is_binary(&self) -> bool {
        matches!(self, DistanceMetric::Hamming | DistanceMetric::Jaccard)
    }

    /// Computes the distance between two binary vectors (`u8` lanes) using this
    /// metric, following the LanceDB "lower is more similar" convention.
    ///
    /// - **Hamming**: number of differing bits, `Σ popcount(aᵢ ⊕ bᵢ)`.
    /// - **Jaccard**: `1 − |A ∩ B| / |A ∪ B|` computed bitwise; two all-zero
    ///   vectors are defined as distance `0`.
    ///
    /// # Panics
    ///
    /// Panics if `a` and `b` have different lengths, or if `self` is a
    /// float metric ([`DistanceMetric::L2`], `Cosine`, `Dot`, or `L1`) — those
    /// are computed via [`DistanceMetric::compute_distance`].
    pub fn compute_distance_binary(&self, a: &[u8], b: &[u8]) -> f32 {
        assert_eq!(a.len(), b.len(), "binary vector dimension mismatch");
        match self {
            DistanceMetric::Hamming => a
                .iter()
                .zip(b)
                .map(|(x, y)| (x ^ y).count_ones())
                .sum::<u32>() as f32,
            DistanceMetric::Jaccard => {
                let mut inter: u32 = 0;
                let mut union: u32 = 0;
                for (x, y) in a.iter().zip(b) {
                    inter += (x & y).count_ones();
                    union += (x | y).count_ones();
                }
                if union == 0 {
                    0.0
                } else {
                    1.0 - (inter as f32) / (union as f32)
                }
            }
            other => panic!("{other:?} is a float-vector metric; use compute_distance"),
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
    #[serde(default)]
    pub metadata: IndexMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum TokenizerConfig {
    Standard,
    Whitespace,
    Ngram {
        min: u8,
        max: u8,
    },
    Custom {
        name: String,
    },
    /// Fully specified analyzer pipeline (base tokenizer + language + filters).
    ///
    /// Carries stemming, stop-word, lowercasing, ASCII-folding and token-length
    /// configuration so full-text indexes honor the requested analysis instead
    /// of falling back to the hardcoded standard tokenizer.
    Analyzer(AnalyzerConfig),
}

/// Full analyzer pipeline configuration for a full-text index.
///
/// This is a backend-agnostic, plainly serializable description of the token
/// analysis chain. The `uni-store` Lance backend maps it onto the underlying
/// `InvertedIndexParams`; this crate stays free of any Lance dependency.
///
/// Every field carries `#[serde(default)]` so schemas persisted before a field
/// existed still deserialize.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnalyzerConfig {
    /// Base tokenizer that splits raw text into tokens.
    #[serde(default)]
    pub base: BaseTokenizer,
    /// Language used for stemming and built-in stop-word lists.
    #[serde(default)]
    pub language: FtsLanguage,
    /// Whether to lowercase tokens.
    #[serde(default = "default_true")]
    pub lower_case: bool,
    /// Whether to apply language-specific stemming.
    #[serde(default = "default_true")]
    pub stem: bool,
    /// Whether to drop stop words (built-in list, or `custom_stop_words`).
    #[serde(default = "default_true")]
    pub remove_stop_words: bool,
    /// Explicit stop-word list overriding the language's built-in list.
    #[serde(default)]
    pub custom_stop_words: Option<Vec<String>>,
    /// Whether to fold accented characters to ASCII (é → e).
    #[serde(default = "default_true")]
    pub ascii_folding: bool,
    /// Drop tokens longer than this many bytes (`None` keeps the backend default).
    #[serde(default)]
    pub max_token_length: Option<u32>,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            base: BaseTokenizer::default(),
            language: FtsLanguage::default(),
            lower_case: true,
            stem: true,
            remove_stop_words: true,
            custom_stop_words: None,
            ascii_folding: true,
            max_token_length: None,
        }
    }
}

/// Base tokenizer that produces the initial token stream.
///
/// `Custom` is a passthrough for backend-native tokenizers such as
/// `"lindera/*"` or `"jieba/*"` (which require external dictionaries).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum BaseTokenizer {
    /// Split on whitespace and punctuation (recommended default).
    #[default]
    Simple,
    /// Split on whitespace only.
    Whitespace,
    /// No tokenization; the whole field is one token.
    Raw,
    /// Character N-gram tokenizer with inclusive `[min, max]` gram lengths.
    Ngram {
        /// Minimum gram length (must be `>= 1` and `<= max`).
        min: u32,
        /// Maximum gram length.
        max: u32,
    },
    /// Backend-native tokenizer name, passed through verbatim (e.g. `"jieba/default"`).
    Custom(String),
}

/// Language used for stemming and built-in stop-word removal.
///
/// Mirrors the 18 languages the Lance tokenizer supports. Note that not every
/// language ships a built-in stop-word list; the backend mapper handles those
/// cases (see `uni-store`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum FtsLanguage {
    /// Arabic.
    Arabic,
    /// Danish.
    Danish,
    /// Dutch.
    Dutch,
    /// English (default).
    #[default]
    English,
    /// Finnish.
    Finnish,
    /// French.
    French,
    /// German.
    German,
    /// Greek.
    Greek,
    /// Hungarian.
    Hungarian,
    /// Italian.
    Italian,
    /// Norwegian.
    Norwegian,
    /// Portuguese.
    Portuguese,
    /// Romanian.
    Romanian,
    /// Russian.
    Russian,
    /// Spanish.
    Spanish,
    /// Swedish.
    Swedish,
    /// Tamil.
    Tamil,
    /// Turkish.
    Turkish,
}

/// Serde default helper: boolean fields that default to `true`.
fn default_true() -> bool {
    true
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
    #[serde(default)]
    pub metadata: IndexMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScalarIndexConfig {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub index_type: ScalarIndexType,
    pub where_clause: Option<String>,
    #[serde(default)]
    pub metadata: IndexMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ScalarIndexType {
    BTree,
    Hash,
    Bitmap,
    LabelList,
}
