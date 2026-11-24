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
    /// The logical properties indexed (`["ts"]`), as declared and reported.
    pub properties: Vec<String>,
    pub index_type: ScalarIndexType,
    pub where_clause: Option<String>,
    #[serde(default)]
    pub metadata: IndexMetadata,
    /// Set when the property's type is indexed through derived key columns
    /// rather than its own column (see [`IndexKey`]). Chosen from the declared
    /// type by [`Schema::resolve_scalar_index`]; a user never spells it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<IndexKey>,
}

impl ScalarIndexConfig {
    /// The table columns this index is built on: the key columns for a keyed
    /// index, the properties otherwise.
    #[must_use]
    pub fn physical_columns(&self) -> Vec<&str> {
        match &self.key {
            Some(key) => key.physical_columns().iter().map(String::as_str).collect(),
            None => self.properties.iter().map(String::as_str).collect(),
        }
    }

    /// The Lance index names this index is built as, one per
    /// [`Self::physical_columns`] entry for a keyed index (`idx_ts__dt`), the
    /// index's own name otherwise.
    #[must_use]
    pub fn physical_index_names(&self) -> Vec<String> {
        match &self.key {
            Some(key) => key
                .kind
                .column_roles()
                .iter()
                .map(|role| format!("{}__{role}", self.name))
                .collect(),
            None => vec![self.name.clone()],
        }
    }
}

/// Name prefix of a keyed scalar index's key columns.
pub const INDEX_KEY_COLUMN_PREFIX: &str = "__key_";

/// How a keyed scalar index indexes its property: the key kind and the
/// derived columns that hold the key.
///
/// A key column is computed from the property at every batch build of the
/// label's table, like a JSON full-text index's text column, and is never
/// stored in L0 or hashed into the content UID. A predicate on the property is
/// rewritten into a bound on the key for the storage scan only; the original
/// predicate is always kept, so the key may admit extra rows but never drops a
/// match.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexKey {
    pub kind: KeyKind,
    /// Physical key columns, in [`KeyKind::column_roles`] order.
    pub columns: Vec<String>,
}

impl IndexKey {
    /// The key of `kind` for `property`, with its reserved column names.
    #[must_use]
    pub fn new(kind: KeyKind, property: &str) -> Self {
        let columns = kind.column_names(property);
        Self { kind, columns }
    }

    /// The derived columns the index is built on.
    #[must_use]
    pub fn physical_columns(&self) -> &[String] {
        &self.columns
    }
}

/// The kind of an [`IndexKey`]: which flat values are derived from the
/// property.
///
/// [`KeyKind::DateTimeInstant`], [`KeyKind::TimeUtc`],
/// [`KeyKind::BticBounds`] and [`KeyKind::PointCoords`] are chosen by
/// [`KeyKind::for_property`], and [`KeyKind::MapPath`] by
/// [`KeyKind::for_map_path`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
#[non_exhaustive]
pub enum KeyKind {
    /// A DATETIME's instant: `[__key_dt_<p>]`, Int64 nanoseconds since the
    /// epoch. `WHERE` compares DATETIMEs by instant, so the key is exact for
    /// `=` and ranges.
    DateTimeInstant,
    /// A TIME's UTC nanoseconds: `[__key_time_<p>]`, Int64
    /// `nanos_since_midnight − offset_seconds · 10⁹` with no modulo, which is
    /// what `WHERE` compares, so the key is exact for `=` and ranges.
    TimeUtc,
    /// A BTIC's raw bounds: `[__key_lo_<p>, __key_hi_<p>]`, Int64 ms, the
    /// unbounded sentinels (`i64::MIN` / `i64::MAX`) kept. Every interval
    /// predicate compares exactly these, so a predicate with one constant
    /// side is a range (or a union of two) on the bounds.
    BticBounds,
    /// The value at a key path of a typed MAP: `[__key_map_<p>_<path>]`.
    /// `value` is the map's declared value type (STRING, INT32/64,
    /// FLOAT32/64); the key column holds the value as a read of `n.m.k`
    /// returns it (Int64, Float64 or String), NULL where the key is missing.
    /// The index's property is the dotted path (`m.k`).
    MapPath { path: Vec<String>, value: DataType },
    /// A point's coordinates: `[__key_px_<p>, __key_py_<p>]`, Float64:
    /// `(x, y)`, or `(longitude, latitude)` when `geographic`; a 3-D point's
    /// `z` is not keyed. `three_d` records a `Cartesian3D` column, whose
    /// points carry another CRS name than a 2-D column's (see
    /// [`KeyKind::point_crs`]).
    PointCoords {
        geographic: bool,
        #[serde(default)]
        three_d: bool,
    },
}

/// What [`KeyKind::for_property`] decides for a declared type.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyChoice {
    /// A flat type: the property's own column is indexed.
    Plain,
    /// The property is indexed through a key of this kind.
    Key(KeyKind),
    /// The type cannot take a scalar index; the reason names what to do.
    Refuse(String),
    /// A new scalar index on the type is refused, with the reason; one
    /// already in a catalog is kept as a plain index (it builds, and no
    /// predicate uses it), so that the catalog keeps opening.
    RefuseNew(String),
}

impl KeyKind {
    /// The key a BTree, Hash or Bitmap index on a property of type `dt` is
    /// built through.
    ///
    /// A nested Arrow column cannot take a Lance scalar index (#356). A
    /// DATETIME is indexed through its instant, BTIC (a fixed-size binary
    /// column no predicate compares) through its bounds, and POINT through
    /// its coordinates. A list, vector, duration, whole JSON value or whole
    /// map is refused with the index kind that fits (see
    /// `refusal_pointer`); every other nested type is refused.
    #[must_use]
    pub fn for_property(dt: &DataType) -> KeyChoice {
        if let Some(refusal) = Self::refusal_pointer(dt) {
            return refusal;
        }
        match dt {
            DataType::DateTime => KeyChoice::Key(Self::DateTimeInstant),
            // `WHERE` compares TIMEs by their UTC nanoseconds.
            DataType::Time => KeyChoice::Key(Self::TimeUtc),
            // A BTIC is stored packed in a fixed-size binary column, which a
            // BTree can index but no predicate compares; its bounds are keyed.
            DataType::Btic => KeyChoice::Key(Self::BticBounds),
            // A point is a struct; its two horizontal coordinates are keyed.
            DataType::Point(pt) => KeyChoice::Key(Self::PointCoords {
                geographic: matches!(pt, PointType::Geographic),
                three_d: matches!(pt, PointType::Cartesian3D),
            }),
            dt if dt.to_arrow().is_nested() => KeyChoice::Refuse(format!(
                "the property is {ty}, stored as a nested column, which Lance's scalar \
                 indexes do not support. Declare it TIMESTAMP to index it, or leave it \
                 unindexed (queries on it scan)",
                ty = dt.repr_name(),
            )),
            _ => KeyChoice::Plain,
        }
    }

    /// The key an index on the key path `path` of a MAP property whose
    /// values are of type `value` (`n.m.k`) is built through: the value at
    /// the path, in a column of the type the engine reads the map's values
    /// as. Only a flat, ordered value type is keyed (STRING, the integers,
    /// the floats).
    #[must_use]
    pub fn for_map_path(value: &DataType, path: &[String]) -> KeyChoice {
        if Self::map_value_column_type(value).is_none() {
            return KeyChoice::Refuse(format!(
                "the map's values are {ty}; an index on a key path needs STRING, \
                 integer or float values (queries on it scan)",
                ty = value.repr_name(),
            ));
        }
        if path.len() != 1 || path[0].is_empty() {
            return KeyChoice::Refuse(
                "a key path into a typed MAP names one key (`n.m.k`)".to_string(),
            );
        }
        KeyChoice::Key(Self::MapPath {
            path: path.to_vec(),
            value: value.clone(),
        })
    }

    /// The key column type for a MAP value type: the type a read of `n.m.k`
    /// returns (an INT32 value reads as an integer, a FLOAT32 one as a
    /// float). `None` for a value type a key path cannot index.
    fn map_value_column_type(value: &DataType) -> Option<DataType> {
        match value {
            DataType::String => Some(DataType::String),
            DataType::Int64 | DataType::Int32 => Some(DataType::Int64),
            DataType::Float64 | DataType::Float32 => Some(DataType::Float64),
            _ => None,
        }
    }

    /// The property a key of this kind is derived from, for an index on
    /// `property`: the MAP property for a key path (`m` for `m.k`), the
    /// property itself otherwise.
    #[must_use]
    pub fn source_property<'a>(&self, property: &'a str) -> &'a str {
        match self {
            Self::MapPath { .. } => property.split_once('.').map_or(property, |(head, _)| head),
            _ => property,
        }
    }

    /// The refusal, naming the index kind that does fit, for a type no
    /// scalar index serves: a list, a vector, a duration, a whole JSON value
    /// or a whole map. `None` for any other type.
    ///
    /// DURATION and JSON are flat (binary) columns a BTree can be built on,
    /// but its byte order serves no predicate: a new index is refused, and
    /// one already in a catalog is kept ([`KeyChoice::RefuseNew`]).
    fn refusal_pointer(dt: &DataType) -> Option<KeyChoice> {
        Some(match dt {
            DataType::List(inner) => match inner.as_ref() {
                DataType::String => KeyChoice::Refuse(String::from(
                    "the property is a LIST<STRING>, which a scalar index does not index. \
                     Index its elements with an inverted index instead: \
                     CREATE INVERTED INDEX <name> FOR (n:<Label>) ON (n.<property>)",
                )),
                DataType::Vector { .. } => KeyChoice::Refuse(String::from(
                    "the property is a list of vectors, which a scalar index does \
                     not index. Use a vector index: CREATE VECTOR INDEX <name> \
                     FOR (n:<Label>) ON (n.<property>)",
                )),
                _ => KeyChoice::Refuse(String::from(
                    "the property is a LIST, which a scalar index does not index, and \
                     no index kind serves a list of this element type yet (a \
                     LIST<STRING> takes an inverted index): queries on it scan",
                )),
            },
            DataType::Vector { .. } => KeyChoice::Refuse(String::from(
                "the property is a VECTOR, which a scalar index does not index. \
                 Use a vector index: CREATE VECTOR INDEX <name> FOR (n:<Label>) \
                 ON (n.<property>)",
            )),
            DataType::Duration => KeyChoice::RefuseNew(
                "the property is DURATION. Durations are not ordered (`<` between two \
                 durations is NULL), so no scalar index serves them: queries on it scan"
                    .to_string(),
            ),
            DataType::CypherValue => KeyChoice::RefuseNew(
                "the property is JSON, stored as an encoded value whose byte order no \
                 predicate follows. For text search use a JSON full-text index: \
                 CREATE JSON FULLTEXT INDEX <name> FOR (n:<Label>) ON <property>; to \
                 compare values, declare it a typed MAP and index a key path: \
                 CREATE INDEX <name> FOR (n:<Label>) ON (n.<property>.<key>)"
                    .to_string(),
            ),
            DataType::Map(..) => KeyChoice::Refuse(String::from(
                "the property is a whole MAP, which a scalar index does not index. \
                 Index a key path instead: CREATE INDEX <name> FOR (n:<Label>) \
                 ON (n.<property>.<key>)",
            )),
            _ => return None,
        })
    }

    /// The CRS name a point of a [`KeyKind::PointCoords`] column carries when
    /// `point(...)` wrote it: `WGS84`, `Cartesian` or `Cartesian-3D`.
    /// `distance` compares CRS names exactly and raises on a mismatch, so
    /// only a stored point carrying this name is keyed, and only a constant
    /// carrying it is turned into a distance bound. `None` for other kinds.
    #[must_use]
    pub fn point_crs(&self) -> Option<&'static str> {
        match self {
            Self::PointCoords {
                geographic: true, ..
            } => Some("WGS84"),
            Self::PointCoords { three_d: true, .. } => Some("Cartesian-3D"),
            Self::PointCoords { .. } => Some("Cartesian"),
            _ => None,
        }
    }

    /// The role of each key column, in column order; also the suffix of the
    /// Lance index built on it.
    #[must_use]
    pub fn column_roles(&self) -> &'static [&'static str] {
        match self {
            Self::DateTimeInstant => &["dt"],
            Self::TimeUtc => &["time"],
            Self::BticBounds => &["lo", "hi"],
            Self::MapPath { .. } => &["map"],
            Self::PointCoords { .. } => &["px", "py"],
        }
    }

    /// The reserved key column names for `property`.
    #[must_use]
    pub fn column_names(&self, property: &str) -> Vec<String> {
        match self {
            Self::MapPath { path, .. } => {
                // Sanitized for readability, with a hash of the exact path so
                // two paths never share a column.
                let joined = path.join(".");
                let readable: String = path
                    .join("_")
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect();
                vec![format!(
                    "{INDEX_KEY_COLUMN_PREFIX}map_{property}_{readable}_{:08x}",
                    fnv1a32(joined.as_bytes())
                )]
            }
            _ => self
                .column_roles()
                .iter()
                .map(|role| format!("{INDEX_KEY_COLUMN_PREFIX}{role}_{property}"))
                .collect(),
        }
    }

    /// The declared type of each key column, in column order.
    #[must_use]
    pub fn column_types(&self) -> Vec<DataType> {
        match self {
            Self::DateTimeInstant | Self::TimeUtc => vec![DataType::Int64],
            Self::BticBounds => vec![DataType::Int64, DataType::Int64],
            Self::MapPath { value, .. } => {
                vec![Self::map_value_column_type(value).unwrap_or_else(|| value.clone())]
            }
            Self::PointCoords { .. } => vec![DataType::Float64, DataType::Float64],
        }
    }

    /// A short user-facing description, as reported by index listings.
    #[must_use]
    pub fn describe(&self) -> &'static str {
        match self {
            Self::DateTimeInstant => "datetime instant",
            Self::TimeUtc => "time utc",
            Self::BticBounds => "btic lo/hi",
            Self::MapPath { .. } => "map path",
            Self::PointCoords { .. } => "point coordinates",
        }
    }
}

/// FNV-1a, 32 bits: a stable, dependency-free hash for column names.
fn fnv1a32(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0x811c_9dc5_u32, |h, b| {
        (h ^ u32::from(*b)).wrapping_mul(0x0100_0193)
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum ScalarIndexType {
    BTree,
    Hash,
    Bitmap,
    LabelList,
}

impl Schema {
    /// Resolve how `config` is built from the declared types of its
    /// properties: set [`ScalarIndexConfig::key`] for a property indexed
    /// through a key, clear it for a flat one, and return why the index can
    /// never be built, naming the property, when its type cannot take one.
    ///
    /// An undeclared property is not judged: it has no column type yet, so
    /// the index is plain until the property is declared and the index next
    /// built (which is how an index recorded before its key existed is
    /// upgraded). A key serves one property: a composite index over a keyed
    /// type is refused.
    ///
    /// # Errors
    ///
    /// The reason the index is refused.
    pub fn resolve_scalar_index(
        &self,
        config: &mut ScalarIndexConfig,
    ) -> std::result::Result<(), String> {
        if matches!(config.index_type, ScalarIndexType::LabelList) {
            config.key = None;
            return Ok(());
        }
        let declared = self.properties.get(&config.label);
        let mut key = None;
        for prop in &config.properties {
            let refuse = |why: &str| {
                format!(
                    "a {kind:?} index cannot index {label}.{prop}: {why}",
                    kind = config.index_type,
                    label = config.label,
                )
            };
            let Some(meta) = declared.and_then(|d| d.get(prop)) else {
                // `m.k` on a declared MAP property `m`: a key path. Any other
                // undeclared name is not judged.
                let Some((head, choice)) = self.map_path_choice(&config.label, prop) else {
                    continue;
                };
                match choice {
                    KeyChoice::Key(kind) if config.properties.len() == 1 => {
                        key = Some(IndexKey::new(kind, head));
                    }
                    KeyChoice::Key(_) => {
                        return Err(refuse(
                            "a key path is indexed through a derived key, and a key serves \
                             a single-property index only. Index it on its own",
                        ));
                    }
                    // A key path never resolves to a kept-when-recorded refusal.
                    KeyChoice::Refuse(why) | KeyChoice::RefuseNew(why) => {
                        return Err(refuse(&why));
                    }
                    KeyChoice::Plain => {}
                }
                continue;
            };
            match KeyKind::for_property(&meta.r#type) {
                KeyChoice::Plain => {}
                KeyChoice::Refuse(why) => return Err(refuse(&why)),
                // Refused when new; kept, plain, when already recorded.
                KeyChoice::RefuseNew(why) if !self.records_scalar_index(config) => {
                    return Err(refuse(&why));
                }
                KeyChoice::RefuseNew(_) => {}
                KeyChoice::Key(kind) if config.properties.len() == 1 => {
                    key = Some(IndexKey::new(kind, prop));
                }
                KeyChoice::Key(_) => {
                    return Err(refuse(&format!(
                        "the property is {ty}, which is indexed through a derived key, \
                         and a key serves a single-property index only. Index it on its own",
                        ty = meta.r#type.repr_name(),
                    )));
                }
            }
        }
        config.key = key;
        Ok(())
    }

    /// For a dotted `property` (`m.k`) whose head is a declared MAP property
    /// of `label`, the head and what [`KeyKind::for_map_path`] decides for
    /// the rest of the path; `None` for anything else.
    #[must_use]
    pub fn map_path_choice<'a>(
        &self,
        label: &str,
        property: &'a str,
    ) -> Option<(&'a str, KeyChoice)> {
        let (head, path) = property.split_once('.')?;
        let meta = self.properties.get(label)?.get(head)?;
        let DataType::Map(_, value) = &meta.r#type else {
            return None;
        };
        let path: Vec<String> = path.split('.').map(str::to_string).collect();
        Some((head, KeyKind::for_map_path(value, &path)))
    }

    /// Why `config` cannot be recorded under its name: another index already
    /// holds the name with another kind, label or property list. Re-declaring
    /// the same index is not a clash. Replacing it by name silently dropped
    /// the index the name had (and let a refused type in as "recorded").
    #[must_use]
    pub fn scalar_index_name_clash(&self, config: &ScalarIndexConfig) -> Option<String> {
        let existing = self.indexes.iter().find(|i| i.name() == config.name)?;
        if self.records_scalar_index(config) {
            return None;
        }
        let on = match existing {
            IndexDefinition::Scalar(c) => format!("{}({})", c.label, c.properties.join(", ")),
            other => format!("{} (a different index kind)", other.label()),
        };
        Some(format!(
            "an index named '{}' already exists on {on}; drop it first or choose another name",
            config.name
        ))
    }

    /// Whether the catalog already records a scalar index named as `config`,
    /// on the same label and properties.
    #[must_use]
    pub fn records_scalar_index(&self, config: &ScalarIndexConfig) -> bool {
        self.indexes.iter().any(|idx| {
            matches!(idx, IndexDefinition::Scalar(c)
                if c.name == config.name
                    && c.label == config.label
                    && c.properties == config.properties)
        })
    }

    /// Why `config` can never be built, or `None` (see
    /// [`Self::resolve_scalar_index`]).
    #[must_use]
    pub fn scalar_index_refusal(&self, config: &ScalarIndexConfig) -> Option<String> {
        self.resolve_scalar_index(&mut config.clone()).err()
    }

    /// The keyed scalar index serving `(label, property)`, if one is
    /// `Online` and its key columns are registered on the label.
    #[must_use]
    pub fn keyed_scalar_index(&self, label: &str, property: &str) -> Option<&ScalarIndexConfig> {
        let props = self.properties.get(label)?;
        self.indexes.iter().find_map(|idx| match idx {
            IndexDefinition::Scalar(cfg)
                if cfg.label == label
                    && cfg.properties.len() == 1
                    && cfg.properties[0] == property
                    && cfg.metadata.status == IndexStatus::Online
                    && cfg
                        .key
                        .as_ref()
                        .is_some_and(|k| k.columns.iter().all(|c| props.contains_key(c))) =>
            {
                Some(cfg)
            }
            _ => None,
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
            key: None,
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

    /// The key is part of the configuration: a keyed and an unkeyed index on
    /// the same property are different builds.
    #[test]
    fn key_differences_are_reported() {
        let plain = scalar("idx", "ts");
        let mut keyed = plain.clone();
        if let IndexDefinition::Scalar(cfg) = &mut keyed {
            cfg.key = Some(IndexKey::new(KeyKind::DateTimeInstant, "ts"));
        }
        assert!(!plain.same_config_as(&keyed));
        assert!(keyed.same_config_as(&keyed.clone()));
    }
}

#[cfg(test)]
mod index_key_tests {
    use super::*;

    fn config(prop: &str, key: Option<IndexKey>) -> ScalarIndexConfig {
        ScalarIndexConfig {
            name: "idx_ts".to_string(),
            label: "Event".to_string(),
            properties: vec![prop.to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: IndexMetadata::default(),
            key,
        }
    }

    fn schema_with(props: &[(&str, DataType)]) -> Schema {
        let mut schema = Schema::default();
        let entry = schema.properties.entry("Event".to_string()).or_default();
        for (name, ty) in props {
            entry.insert(
                (*name).to_string(),
                PropertyMeta {
                    r#type: ty.clone(),
                    nullable: true,
                    added_in: 0,
                    state: SchemaElementState::Active,
                    generation_expression: None,
                    description: None,
                    default: None,
                },
            );
        }
        schema
    }

    #[test]
    fn keyed_config_round_trips_through_json() {
        let cfg = config("ts", Some(IndexKey::new(KeyKind::DateTimeInstant, "ts")));
        let json = serde_json::to_string(&IndexDefinition::Scalar(cfg.clone())).unwrap();
        assert!(json.contains("__key_dt_ts"), "{json}");
        let back: IndexDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(back, IndexDefinition::Scalar(cfg));

        for kind in [
            KeyKind::TimeUtc,
            KeyKind::BticBounds,
            KeyKind::MapPath {
                path: vec!["a".into(), "b c".into()],
                value: DataType::Int64,
            },
            KeyKind::PointCoords {
                geographic: true,
                three_d: false,
            },
        ] {
            let key = IndexKey::new(kind, "p");
            let json = serde_json::to_string(&key).unwrap();
            assert_eq!(serde_json::from_str::<IndexKey>(&json).unwrap(), key);
        }
    }

    /// A catalog written before keys existed has no `key` field and reads as
    /// unkeyed; an unkeyed index writes no `key` field, so the catalog an
    /// older binary reads is unchanged.
    #[test]
    fn old_catalog_json_deserializes_unchanged() {
        let old = r#"{"type":"Scalar","name":"idx_ts","label":"Event","properties":["ts"],
                      "index_type":"BTree","where_clause":null,
                      "metadata":{"status":"Failed","last_built_at":null,"row_count_at_build":null}}"#;
        let IndexDefinition::Scalar(cfg) = serde_json::from_str::<IndexDefinition>(old).unwrap()
        else {
            panic!("not scalar");
        };
        assert_eq!(cfg.key, None);
        assert_eq!(cfg.metadata.status, IndexStatus::Failed);
        let json = serde_json::to_string(&IndexDefinition::Scalar(config("ts", None))).unwrap();
        assert!(!json.contains("key"), "{json}");
    }

    #[test]
    fn column_names_are_reserved_and_distinct() {
        let names = KeyKind::DateTimeInstant.column_names("ts");
        assert_eq!(names, vec!["__key_dt_ts".to_string()]);
        assert!(names.iter().all(|n| is_reserved_property_name(n)));
        assert!(names.iter().all(|n| is_derived_property_name(n)));
        let path = |p: &[&str]| KeyKind::MapPath {
            path: p.iter().map(|s| (*s).to_string()).collect(),
            value: DataType::String,
        };
        assert_ne!(
            path(&["a_b"]).column_names("m"),
            path(&["a", "b"]).column_names("m"),
            "two paths must never share a column"
        );
        let cfg = config("ts", Some(IndexKey::new(KeyKind::DateTimeInstant, "ts")));
        assert_eq!(cfg.physical_columns(), vec!["__key_dt_ts"]);
        assert_eq!(cfg.physical_index_names(), vec!["idx_ts__dt".to_string()]);
        assert_eq!(config("ts", None).physical_columns(), vec!["ts"]);
    }

    #[test]
    fn resolve_chooses_the_key_from_the_declared_type() {
        let schema = schema_with(&[
            ("ts", DataType::DateTime),
            ("at", DataType::Time),
            ("tags", DataType::List(Box::new(DataType::String))),
            ("n", DataType::Int64),
            ("valid", DataType::Btic),
        ]);
        let mut cfg = config("ts", None);
        schema.resolve_scalar_index(&mut cfg).unwrap();
        assert_eq!(cfg.key, Some(IndexKey::new(KeyKind::DateTimeInstant, "ts")));

        // A BTIC is keyed by its two raw bounds.
        let mut cfg = config("valid", None);
        schema.resolve_scalar_index(&mut cfg).unwrap();
        let key = cfg.key.clone().expect("a BTIC index is keyed");
        assert_eq!(key, IndexKey::new(KeyKind::BticBounds, "valid"));
        assert_eq!(
            cfg.physical_columns(),
            vec!["__key_lo_valid", "__key_hi_valid"]
        );
        assert_eq!(
            cfg.physical_index_names(),
            vec!["idx_ts__lo".to_string(), "idx_ts__hi".to_string()]
        );

        let mut cfg = config("n", Some(IndexKey::new(KeyKind::DateTimeInstant, "n")));
        schema.resolve_scalar_index(&mut cfg).unwrap();
        assert_eq!(cfg.key, None, "a flat type has no key");

        let mut cfg = config("undeclared", None);
        schema.resolve_scalar_index(&mut cfg).unwrap();
        assert_eq!(cfg.key, None);

        // A TIME is keyed by its UTC nanoseconds.
        let mut cfg = config("at", None);
        schema.resolve_scalar_index(&mut cfg).unwrap();
        assert_eq!(cfg.key, Some(IndexKey::new(KeyKind::TimeUtc, "at")));
        assert_eq!(cfg.physical_columns(), vec!["__key_time_at"]);

        // Not yet keyed: refused as #356 refuses them.
        let err = schema
            .resolve_scalar_index(&mut config("tags", None))
            .unwrap_err();
        assert!(err.contains("tags") && err.contains("cannot"), "{err}");

        let mut composite = config("ts", None);
        composite.properties.push("n".to_string());
        assert!(schema.resolve_scalar_index(&mut composite).is_err());

        let mut label_list = config("tags", None);
        label_list.index_type = ScalarIndexType::LabelList;
        schema.resolve_scalar_index(&mut label_list).unwrap();
    }

    #[test]
    fn resolve_keys_a_typed_map_path() {
        let map = |v: DataType| DataType::Map(Box::new(DataType::String), Box::new(v));
        let schema = schema_with(&[
            ("m", map(DataType::Int32)),
            ("f", map(DataType::Float32)),
            ("s", map(DataType::String)),
            ("b", map(DataType::Bool)),
            ("l", map(DataType::List(Box::new(DataType::Int64)))),
            ("n", DataType::Int64),
        ]);
        let path_key = |head: &str, k: &str, value: DataType| {
            IndexKey::new(
                KeyKind::MapPath {
                    path: vec![k.to_string()],
                    value,
                },
                head,
            )
        };
        // The key column has the type the engine reads the values as.
        for (prop, head, k, value, column) in [
            ("m.a", "m", "a", DataType::Int32, DataType::Int64),
            ("f.x", "f", "x", DataType::Float32, DataType::Float64),
            ("s.name", "s", "name", DataType::String, DataType::String),
        ] {
            let mut cfg = config(prop, None);
            schema.resolve_scalar_index(&mut cfg).unwrap();
            let key = cfg.key.clone().expect("a typed MAP path is keyed");
            assert_eq!(key, path_key(head, k, value), "{prop}");
            assert_eq!(key.kind.column_types(), vec![column], "{prop}");
            assert_eq!(key.kind.source_property(prop), head);
            assert!(
                key.columns[0].starts_with(&format!("__key_map_{head}_{k}_")),
                "{key:?}"
            );
        }
        // Two paths on one map have distinct columns.
        let (mut a, mut b) = (config("m.a", None), config("m.b", None));
        schema.resolve_scalar_index(&mut a).unwrap();
        schema.resolve_scalar_index(&mut b).unwrap();
        assert_ne!(a.physical_columns(), b.physical_columns());

        // Refused: an unordered or nested value type, a deeper path, a
        // composite index.
        for prop in ["b.k", "l.k", "m.a.b"] {
            let err = schema
                .resolve_scalar_index(&mut config(prop, None))
                .unwrap_err();
            assert!(err.contains(prop), "{err}");
        }
        let mut composite = config("m.a", None);
        composite.properties.push("n".to_string());
        assert!(schema.resolve_scalar_index(&mut composite).is_err());

        // A dotted name whose head is not a MAP, or is undeclared, is not
        // judged.
        for prop in ["n.k", "undeclared.k"] {
            let mut cfg = config(prop, None);
            schema.resolve_scalar_index(&mut cfg).unwrap();
            assert_eq!(cfg.key, None, "{prop}");
        }
    }
}

#[cfg(test)]
mod point_and_refusal_tests {
    use super::*;

    fn config(prop: &str) -> ScalarIndexConfig {
        ScalarIndexConfig {
            name: format!("idx_{prop}"),
            label: "Place".to_string(),
            properties: vec![prop.to_string()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: IndexMetadata::default(),
            key: None,
        }
    }

    fn schema_with(props: &[(&str, DataType)]) -> Schema {
        let mut schema = Schema::default();
        let entry = schema.properties.entry("Place".to_string()).or_default();
        for (name, ty) in props {
            entry.insert(
                (*name).to_string(),
                PropertyMeta {
                    r#type: ty.clone(),
                    nullable: true,
                    added_in: 0,
                    state: SchemaElementState::Active,
                    generation_expression: None,
                    description: None,
                    default: None,
                },
            );
        }
        schema
    }

    /// A key with a space or a hyphen is a key.
    #[test]
    fn quoted_keys_are_keys() {
        let mut schema = Schema::default();
        schema.labels.insert(
            "Doc".into(),
            LabelMeta {
                id: 1,
                created_at: chrono::Utc::now(),
                state: SchemaElementState::Active,
                description: None,
            },
        );
        let mut props = HashMap::new();
        props.insert(
            "m".to_string(),
            PropertyMeta {
                r#type: DataType::Map(Box::new(DataType::String), Box::new(DataType::Int64)),
                nullable: true,
                added_in: 1,
                state: SchemaElementState::Active,
                generation_expression: None,
                description: None,
                default: None,
            },
        );
        schema.properties.insert("Doc".into(), props);
        // The catalog API takes a key as named: Cypher's planner
        // parenthesizes an expression before it gets here (see
        // `normalize_expression_for_storage`).
        let (_, choice) = schema
            .map_path_choice("Doc", "m.content-type")
            .expect("a map head");
        assert!(
            matches!(choice, KeyChoice::Key(KeyKind::MapPath { .. })),
            "{choice:?}"
        );
        let (_, choice) = schema.map_path_choice("Doc", "m.a b").expect("a map head");
        assert!(
            matches!(choice, KeyChoice::Key(KeyKind::MapPath { .. })),
            "{choice:?}"
        );
    }
    #[test]
    fn a_point_is_keyed_by_its_coordinates() {
        let schema = schema_with(&[
            ("geo", DataType::Point(PointType::Geographic)),
            ("xy", DataType::Point(PointType::Cartesian2D)),
            ("xyz", DataType::Point(PointType::Cartesian3D)),
        ]);
        for (prop, geographic, three_d, crs) in [
            ("geo", true, false, "WGS84"),
            ("xy", false, false, "Cartesian"),
            ("xyz", false, true, "Cartesian-3D"),
        ] {
            let mut cfg = config(prop);
            schema.resolve_scalar_index(&mut cfg).unwrap();
            let key = cfg.key.expect("a point index is keyed");
            assert_eq!(
                key.kind,
                KeyKind::PointCoords {
                    geographic,
                    three_d
                }
            );
            assert_eq!(key.kind.point_crs(), Some(crs));
            assert_eq!(
                key.columns,
                vec![format!("__key_px_{prop}"), format!("__key_py_{prop}")]
            );
            assert_eq!(key.kind.column_types(), vec![DataType::Float64; 2]);
        }
        assert_eq!(KeyKind::DateTimeInstant.point_crs(), None);
        // A key recorded without `three_d` reads as 2-D.
        let old: KeyKind =
            serde_json::from_str(r#"{"kind":"PointCoords","geographic":false}"#).unwrap();
        assert_eq!(
            old,
            KeyKind::PointCoords {
                geographic: false,
                three_d: false
            }
        );
    }

    /// Each refusal names the property and the index kind that fits.
    #[test]
    fn refusals_name_the_index_kind_that_fits() {
        let schema = schema_with(&[
            ("tags", DataType::List(Box::new(DataType::String))),
            ("nums", DataType::List(Box::new(DataType::Int64))),
            ("emb", DataType::Vector { dimensions: 4 }),
            (
                "embs",
                DataType::List(Box::new(DataType::Vector { dimensions: 4 })),
            ),
            ("took", DataType::Duration),
            ("doc", DataType::CypherValue),
            (
                "attrs",
                DataType::Map(Box::new(DataType::String), Box::new(DataType::Int64)),
            ),
        ]);
        for (prop, pointer) in [
            ("tags", "CREATE INVERTED INDEX"),
            ("nums", "queries on it scan"),
            ("emb", "CREATE VECTOR INDEX"),
            ("embs", "CREATE VECTOR INDEX"),
            ("took", "not ordered"),
            ("doc", "CREATE JSON FULLTEXT INDEX"),
            ("doc", "ON (n.<property>.<key>)"),
            ("attrs", "ON (n.<property>.<key>)"),
        ] {
            let err = schema.resolve_scalar_index(&mut config(prop)).unwrap_err();
            assert!(
                err.contains(&format!("Place.{prop}")) && err.contains(pointer),
                "{prop}: {err}"
            );
        }
    }

    /// A DURATION or whole-JSON BTree already in a catalog is kept, plain;
    /// a new one is refused. A refused nested type stays refused either way.
    #[test]
    fn a_recorded_duration_or_json_index_is_kept_plain() {
        let mut schema = schema_with(&[
            ("took", DataType::Duration),
            ("doc", DataType::CypherValue),
            ("tags", DataType::List(Box::new(DataType::Int64))),
        ]);
        for prop in ["took", "doc", "tags"] {
            schema.indexes.push(IndexDefinition::Scalar(config(prop)));
        }
        for prop in ["took", "doc"] {
            let mut cfg = config(prop);
            schema.resolve_scalar_index(&mut cfg).unwrap();
            assert_eq!(cfg.key, None);
            // Another name is a new index.
            let mut renamed = config(prop);
            renamed.name = "other".to_string();
            assert!(schema.resolve_scalar_index(&mut renamed).is_err());
        }
        // The same name on another property is new too.
        let mut moved = config("took");
        moved.properties = vec!["doc".to_string()];
        assert!(schema.resolve_scalar_index(&mut moved).is_err());
        assert!(schema.resolve_scalar_index(&mut config("tags")).is_err());
    }
}
