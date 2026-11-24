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

impl IndexStatus {
    /// Returns the user-visible name of this status.
    ///
    /// This is the single rendering used by every surface that reports index
    /// state — the `Schema` introspection API and the `uni.schema.indexes`
    /// procedure — so a status can never read differently depending on which
    /// one is asked. The names are uppercase to match the openCypher
    /// `SHOW INDEXES` convention that `uni.schema.indexes` implements.
    ///
    /// # Examples
    ///
    /// ```
    /// use uni_common::core::schema::IndexStatus;
    ///
    /// assert_eq!(IndexStatus::Online.as_str(), "ONLINE");
    /// assert_eq!(IndexStatus::Stale.as_str(), "STALE");
    /// ```
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Online => "ONLINE",
            Self::Building => "BUILDING",
            Self::Stale => "STALE",
            Self::Failed => "FAILED",
        }
    }
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
    /// The [`FTS_ANALYSIS_REVISION`] the index was last built under; `0` for
    /// an index built before the revision was recorded.
    ///
    /// A full-text index stores the terms its analyzer produced when it was
    /// built. When the analysis of an unchanged configuration changes — a
    /// built-in stop-word list is revised — the stored terms no longer match
    /// what a query is analyzed into, with no error: only missed matches. An
    /// index built under an older revision is rebuilt (see
    /// [`IndexDefinition::built_under_outdated_analysis`]).
    #[serde(default)]
    pub analysis_revision: u32,
}

/// The current revision of built-in full-text analysis.
///
/// Bump it when the terms a built-in analyzer produces for an unchanged
/// configuration change, so that existing indexes are rebuilt.
///
/// - `1`: Lance 14 replaced the built-in stop-word lists. English lost its 49
///   negations and contractions (`no`, `not`, `nor`, `don't`, ...), which are
///   now indexed and searchable; several other languages changed too.
pub const FTS_ANALYSIS_REVISION: u32 = 1;

impl TokenizerConfig {
    /// Whether the tokenizer drops words from a built-in stop-word list, so
    /// that a revision of that list changes the terms an index stores.
    ///
    /// Every configuration but n-grams removes English stop words by default;
    /// an analyzer does unless it turns removal off or supplies its own list.
    #[must_use]
    pub fn uses_builtin_stop_words(&self) -> bool {
        match self {
            Self::Ngram { .. } => false,
            Self::Analyzer(a) => {
                a.remove_stop_words
                    && a.custom_stop_words.is_none()
                    && !matches!(a.base, BaseTokenizer::Ngram { .. })
            }
            _ => true,
        }
    }
}

impl IndexDefinition {
    /// Whether this is a full-text index that was built, and built under an
    /// analysis revision older than [`FTS_ANALYSIS_REVISION`], with a
    /// tokenizer the revision affects. Such an index holds stale terms and
    /// needs a rebuild.
    #[must_use]
    pub fn built_under_outdated_analysis(&self) -> bool {
        let Self::FullText(config) = self else {
            return false;
        };
        config.metadata.last_built_at.is_some()
            && config.metadata.analysis_revision < FTS_ANALYSIS_REVISION
            && config.tokenizer.uses_builtin_stop_words()
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

            /// Compares two definitions by configuration alone, ignoring the
            /// lifecycle `metadata`.
            ///
            /// Use this, not `==`, whenever one side is a *declaration* and the
            /// other is what is *stored*. `metadata` (status, `last_built_at`,
            /// `row_count_at_build`) is written only by the storage layer after
            /// a build, and is part of the derived `PartialEq`. A declaration
            /// from the schema builder always carries
            /// `IndexMetadata::default()`, so `==` against a stored definition
            /// reports "different" for every index that has actually been
            /// built — which silently defeated the guard that exists to stop
            /// `apply()` rebuilding every index on every open (issue #63).
            #[must_use]
            pub fn same_config_as(&self, other: &Self) -> bool {
                let mut a = self.clone();
                let mut b = other.clone();
                *a.metadata_mut() = IndexMetadata::default();
                *b.metadata_mut() = IndexMetadata::default();
                a == b
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
    /// Reweight query terms by inverse document frequency before scoring.
    ///
    /// Off by default, and deliberately so: SPLADE-style learned-sparse weights
    /// already encode term importance during training, so IDF is largely
    /// redundant for them and can double-count. It earns its keep on BM25-like
    /// and BGE-M3 sparse heads, whose raw weights are closer to term-frequency
    /// than to calibrated importance.
    ///
    /// Applied **query-side**: the query's weights are scaled once, before
    /// retrieval. That is what keeps the candidate-generating index scan and
    /// the exact `sparse_dot` re-score consistent — applying it inside the
    /// index alone would let the re-score silently undo it.
    #[serde(default)]
    pub idf_modifier: bool,
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

/// A full-text index over the string values of a `JSON` property.
///
/// The index is built on a derived text column, [`Self::derived_column`],
/// which a flush fills with [`json_fts_text`] of the property's value: the
/// document's string values, one per line, under `paths` when it names any.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonFtsIndexConfig {
    pub name: String,
    pub label: String,
    pub column: String,
    /// Dotted key paths (`title`, `meta.author`; a leading `$.` is ignored)
    /// whose string values are indexed. Empty indexes the whole document.
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub with_positions: bool,
    #[serde(default)]
    pub metadata: IndexMetadata,
}

/// Name prefix of a JSON full-text index's derived text column.
pub const JSON_FTS_COLUMN_PREFIX: &str = "__jfts_";

/// The derived text column a JSON full-text index on `column` searches.
pub fn json_fts_derived_column(column: &str) -> String {
    format!("{JSON_FTS_COLUMN_PREFIX}{column}")
}

impl JsonFtsIndexConfig {
    /// The derived text column this index is built on.
    pub fn derived_column(&self) -> String {
        json_fts_derived_column(&self.column)
    }

    /// Whether a `CONTAINS` on `path` inside the document (`None` for the
    /// value itself) only ever matches text the derived column holds.
    pub fn covers_path(&self, path: Option<&str>) -> bool {
        if self.paths.is_empty() {
            return true;
        }
        let Some(path) = path else {
            return false;
        };
        self.paths.iter().any(|p| {
            let p = normalize_json_path(p);
            path == p || path.starts_with(&format!("{p}."))
        })
    }
}

fn normalize_json_path(path: &str) -> &str {
    path.strip_prefix("$.").unwrap_or(path)
}

/// The text a JSON full-text index holds for `value`: every string value in
/// it (map values and list elements, recursively; keys and non-string scalars
/// are not text), one per line. With `paths`, only the values under those
/// dotted paths. `None` when there is none.
pub fn json_fts_text(value: &crate::Value, paths: &[String]) -> Option<String> {
    fn collect<'a>(value: &'a crate::Value, out: &mut Vec<&'a str>) {
        match value {
            crate::Value::String(s) => out.push(s),
            crate::Value::List(items) => items.iter().for_each(|v| collect(v, out)),
            crate::Value::Map(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                for k in keys {
                    collect(&map[k], out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    if paths.is_empty() {
        collect(value, &mut out);
    } else {
        for path in paths {
            let mut at = Some(value);
            for key in normalize_json_path(path).split('.') {
                at = match at {
                    Some(crate::Value::Map(map)) => map.get(key),
                    _ => None,
                };
            }
            if let Some(v) = at {
                collect(v, &mut out);
            }
        }
    }
    (!out.is_empty()).then(|| out.join("\n"))
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

impl ScalarIndexType {
    /// Whether this index kind cannot be built over a column of type `dt`.
    ///
    /// BTree, Hash (built as a BTree) and Bitmap index flat columns only:
    /// Lance refuses a nested field (a struct, list, map or fixed-size list),
    /// so a property stored as one (`DateTime`, `Time`, points, lists, maps,
    /// vectors) never gets an index (#356). `LabelList` is meant for lists.
    #[must_use]
    pub fn cannot_index(&self, dt: &DataType) -> bool {
        match self {
            Self::LabelList => false,
            Self::BTree | Self::Hash | Self::Bitmap => dt.to_arrow().is_nested(),
        }
    }
}

impl Schema {
    /// Why `config` can never be built, naming the property, when one of its
    /// properties is declared with a type its index kind cannot index (see
    /// [`ScalarIndexType::cannot_index`]). An undeclared property is not
    /// judged: it has no column type yet.
    #[must_use]
    pub fn scalar_index_refusal(&self, config: &ScalarIndexConfig) -> Option<String> {
        let declared = self.properties.get(&config.label)?;
        config.properties.iter().find_map(|prop| {
            let meta = declared.get(prop)?;
            config.index_type.cannot_index(&meta.r#type).then(|| {
                format!(
                    "a {kind:?} index cannot index {label}.{prop}: the property is {ty}, \
                     stored as a nested column, which Lance's scalar indexes do not \
                     support. Declare it TIMESTAMP to index it, or leave it unindexed \
                     (queries on it scan)",
                    kind = config.index_type,
                    label = config.label,
                    ty = meta.r#type.repr_name(),
                )
            })
        })
    }
}

#[cfg(test)]
mod same_config_tests {
    use super::*;

    fn scalar(name: &str, property: &str) -> IndexDefinition {
        IndexDefinition::Scalar(ScalarIndexConfig {
            name: name.to_string(),
            label: "Foo".to_string(),
            properties: vec![property.to_string()],
            index_type: ScalarIndexType::Hash,
            where_clause: None,
            metadata: IndexMetadata::default(),
        })
    }

    /// Lifecycle metadata is written by the storage layer only, so a stored
    /// definition that has been built must still match the declaration it came
    /// from. `==` does not, which is the whole reason this method exists.
    #[test]
    fn metadata_differences_are_ignored() {
        let declared = scalar("idx", "name");
        let mut stored = declared.clone();
        stored.metadata_mut().status = IndexStatus::Online;
        stored.metadata_mut().last_built_at = Some(Utc::now());
        stored.metadata_mut().row_count_at_build = Some(200);

        assert_ne!(stored, declared, "guard: PartialEq still includes metadata");
        assert!(stored.same_config_as(&declared));
        assert!(declared.same_config_as(&stored));
    }

    /// A real configuration change must still register as different, or the
    /// rebuild that change requires would never be triggered.
    #[test]
    fn configuration_differences_are_reported() {
        let a = scalar("idx", "name");
        let b = scalar("idx", "other");
        assert!(!a.same_config_as(&b));
    }
}
