// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Neural classifier abstraction for Locy `CREATE MODEL` (Phase B).
//!
//! [`NeuralClassifier`] is the row-at-a-time surface that
//! `LocyModelInvoke` (Phase B Slice 3) will drive. This module ships the
//! trait and a deterministic [`MockClassifier`] used by unit tests and
//! TCK; a Xervo-backed implementation lives behind a separate adapter PR
//! (uni-xervo is an external crate so we can't extend `ModelTask::*`
//! directly).
//!
//! ### Scope (Slice 1+2)
//!
//! The trait exposes `classify` returning probabilities in `[0, 1]` and
//! an optional `classify_logits` for calibration paths (Phase C). The
//! default `classify_logits` derives logits from probabilities via
//! inverse-sigmoid so providers that only emit probabilities work out of
//! the box.
//!
//! Phase B Slice 3 will wire `LocyModelInvoke` to call `classify` once per
//! batch per `(model, feature-hash)` group with memoization.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

/// A feature value passed to a neural classifier. Mirrors the value types
/// the property graph emits; `Vector` carries embedding inputs.
#[derive(Debug, Clone)]
pub enum FeatureValue {
    Float(f64),
    Int(i64),
    String(String),
    Vector(Vec<f32>),
    Bool(bool),
    Null,
}

// Phase B Slice 1 (post-Slice-3 follow-up): `Eq` + `Hash` so
// `FeatureValue` can be used as a cache key. Float bit-comparison
// is intentional — NaN-bit-equal is fine for an internal cache
// (a single classifier invocation will reproduce the same NaN bit
// pattern). `PartialEq` mirrors the bit-comparison so `Eq` is sound.
impl PartialEq for FeatureValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Float(a), Self::Float(b)) => a.to_bits() == b.to_bits(),
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Vector(a), Self::Vector(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|(x, y)| x.to_bits() == y.to_bits())
            }
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl Eq for FeatureValue {}

impl std::hash::Hash for FeatureValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Discriminant first, so e.g. `Float(0.0)` and `Int(0)` hash
        // distinctly. `Hash::hash_slice` is used for vector elements to
        // avoid an allocation.
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Float(f) => f.to_bits().hash(state),
            Self::Int(i) => i.hash(state),
            Self::String(s) => s.hash(state),
            Self::Vector(v) => {
                v.len().hash(state);
                for f in v {
                    f.to_bits().hash(state);
                }
            }
            Self::Bool(b) => b.hash(state),
            Self::Null => {}
        }
    }
}

/// One row of input to a classifier. Field names match the `FEATURES`
/// clause identifiers from the `CREATE MODEL` declaration.
#[derive(Debug, Clone, Default)]
pub struct ClassifyInput {
    pub features: HashMap<String, FeatureValue>,
}

impl ClassifyInput {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with(mut self, name: impl Into<String>, value: FeatureValue) -> Self {
        self.features.insert(name.into(), value);
        self
    }

    /// Order-independent stable hash used as a memoization key.
    /// `HashMap` iteration order is non-deterministic; we collect
    /// to a sorted Vec by feature name before hashing.
    pub fn stable_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut entries: Vec<(&String, &FeatureValue)> = self.features.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let mut h = std::collections::hash_map::DefaultHasher::new();
        entries.len().hash(&mut h);
        for (k, v) in entries {
            k.hash(&mut h);
            v.hash(&mut h);
        }
        h.finish()
    }
}

impl PartialEq for ClassifyInput {
    fn eq(&self, other: &Self) -> bool {
        self.features == other.features
    }
}

impl Eq for ClassifyInput {}

/// Errors raised by a [`NeuralClassifier`] impl.
#[derive(Debug, Clone, PartialEq)]
pub enum ClassifierError {
    /// Input length didn't match output length, or the provider returned
    /// a malformed batch.
    ArityMismatch { expected: usize, actual: usize },
    /// Output value fell outside `[0, 1]` after calibration.
    DomainViolation { value: f64 },
    /// Upstream provider error.
    Provider(String),
}

impl std::fmt::Display for ClassifierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArityMismatch { expected, actual } => write!(
                f,
                "classifier arity mismatch: expected {expected} outputs, got {actual}"
            ),
            Self::DomainViolation { value } => {
                write!(f, "classifier output {value} outside [0, 1]")
            }
            Self::Provider(msg) => write!(f, "classifier provider error: {msg}"),
        }
    }
}

impl std::error::Error for ClassifierError {}

pub type ClassifierResult<T> = std::result::Result<T, ClassifierError>;

/// Row-at-a-time neural classifier.
///
/// A provider returns one probability per input row. Logits are optional
/// and used by Phase C calibration paths (Platt scaling, temperature
/// scaling) that operate on pre-sigmoid scores; the default impl derives
/// them from probabilities.
#[async_trait]
pub trait NeuralClassifier: Send + Sync + std::fmt::Debug {
    /// Return probabilities in `[0, 1]`. `output.len() == inputs.len()`
    /// is the trait contract — implementers MUST enforce it.
    async fn classify(&self, inputs: &[ClassifyInput]) -> ClassifierResult<Vec<f64>>;

    /// Return pre-sigmoid logits. The default implementation calls
    /// [`NeuralClassifier::classify`] and applies inverse-sigmoid; bespoke providers
    /// (Candle, MistralRS) override to expose raw logits cheaply.
    async fn classify_logits(&self, inputs: &[ClassifyInput]) -> ClassifierResult<Vec<f64>> {
        let probs = self.classify(inputs).await?;
        Ok(probs.into_iter().map(inverse_sigmoid).collect())
    }

    /// Provider identifier for EXPLAIN / telemetry. Should match the
    /// `xervo_alias` from `CREATE MODEL`.
    fn name(&self) -> &str;

    /// Phase C B1–B3 follow-up: introspect a wrapped Calibrator
    /// when this classifier composes one (e.g.,
    /// [`CalibratedClassifier`]). Default `None` — bare classifiers
    /// don't expose a calibrator. EXPLAIN uses this to surface the
    /// active calibrator's `confidence_band(p)` on derivations.
    fn get_calibrator(&self) -> Option<Arc<dyn crate::calibration::Calibrator>> {
        None
    }

    /// Phase C B1–B3 follow-up: return `(raw, Some(calibrated))`
    /// per input when this classifier wraps a Calibrator, or
    /// `(raw, None)` otherwise. The runtime writes both into the
    /// per-query [`NeuralProvenanceStore`] so EXPLAIN can show
    /// pre- and post-calibrator values side-by-side. The default
    /// impl delegates to `classify` and reports `None` for the
    /// calibrated half (the raw output IS whatever the classifier
    /// emits — no introspection without an override).
    async fn raw_and_calibrated(
        &self,
        inputs: &[ClassifyInput],
    ) -> ClassifierResult<Vec<(f64, Option<f64>)>> {
        let raw = self.classify(inputs).await?;
        Ok(raw.into_iter().map(|p| (p, None)).collect())
    }
}

/// Deterministic mock classifier for tests and TCK scenarios.
///
/// Holds a closure `Fn(&ClassifyInput) -> f64` so each scenario can
/// configure the mapping it wants. Output clamps to `[0, 1]` and emits
/// a `DomainViolation` error when the closure returns NaN.
pub struct MockClassifier {
    name: String,
    f: Arc<dyn Fn(&ClassifyInput) -> f64 + Send + Sync>,
}

impl MockClassifier {
    pub fn new<F>(name: impl Into<String>, f: F) -> Self
    where
        F: Fn(&ClassifyInput) -> f64 + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            f: Arc::new(f),
        }
    }

    /// Construct a constant-output classifier, the canonical "always
    /// returns 0.7" test fixture.
    pub fn constant(name: impl Into<String>, value: f64) -> Self {
        let v = value.clamp(0.0, 1.0);
        Self::new(name, move |_| v)
    }
}

impl std::fmt::Debug for MockClassifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockClassifier")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl NeuralClassifier for MockClassifier {
    async fn classify(&self, inputs: &[ClassifyInput]) -> ClassifierResult<Vec<f64>> {
        let mut out = Vec::with_capacity(inputs.len());
        for inp in inputs {
            let v = (self.f)(inp);
            if v.is_nan() {
                return Err(ClassifierError::DomainViolation { value: v });
            }
            out.push(v.clamp(0.0, 1.0));
        }
        Ok(out)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Adapter wrapping a base classifier with a fitted [`crate::calibration::Calibrator`]
/// (Phase C C2). After running `CALIBRATE`, users construct one of
/// these and re-register it under the same model name to make
/// subsequent invocations produce calibrated probabilities.
///
/// ```ignore
/// let result = locy_result.command_results().iter().find_map(|(_, r)| match r {
///     CommandResult::Calibrate(c) => Some(c.clone()),
///     _ => None,
/// }).unwrap();
/// let wrapped = CalibratedClassifier::new(
///     "scorer",
///     Arc::clone(&base_classifier),
///     Arc::clone(&result.calibrator),
/// );
/// config.classifier_registry.insert("scorer".into(), Arc::new(wrapped));
/// ```
pub struct CalibratedClassifier {
    name: String,
    base: Arc<dyn NeuralClassifier>,
    calibrator: Arc<dyn crate::calibration::Calibrator>,
}

impl CalibratedClassifier {
    pub fn new(
        name: impl Into<String>,
        base: Arc<dyn NeuralClassifier>,
        calibrator: Arc<dyn crate::calibration::Calibrator>,
    ) -> Self {
        Self {
            name: name.into(),
            base,
            calibrator,
        }
    }
}

impl std::fmt::Debug for CalibratedClassifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CalibratedClassifier")
            .field("name", &self.name)
            .field("base", &self.base.name())
            .field("method", &self.calibrator.method())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl NeuralClassifier for CalibratedClassifier {
    async fn classify(&self, inputs: &[ClassifyInput]) -> ClassifierResult<Vec<f64>> {
        let raw = self.base.classify(inputs).await?;
        Ok(self.calibrator.apply_batch(&raw))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get_calibrator(&self) -> Option<Arc<dyn crate::calibration::Calibrator>> {
        Some(Arc::clone(&self.calibrator))
    }

    async fn raw_and_calibrated(
        &self,
        inputs: &[ClassifyInput],
    ) -> ClassifierResult<Vec<(f64, Option<f64>)>> {
        let raw = self.base.classify(inputs).await?;
        let calibrated = self.calibrator.apply_batch(&raw);
        Ok(raw
            .into_iter()
            .zip(calibrated)
            .map(|(r, c)| (r, Some(c)))
            .collect())
    }
}

/// Phase C B1–B3 follow-up: per-query side-channel store
/// recording the raw / calibrated / confidence-band tuple for
/// every classifier invocation. EXPLAIN reads from this store
/// when building `NeuralProvenance` entries on each
/// `DerivationNode`.
///
/// Keyed by `(model_name, ClassifyInput::stable_hash)` — the same
/// shape as [`ModelInvocationCache`], so rows with identical
/// feature values share one record (consistent with the existing
/// memoization semantics; the classifier output for a given input
/// is deterministic).
/// Shared `RwLock<HashMap<(model, input_hash), V>>` backing the two
/// per-query side-channel stores ([`NeuralProvenanceStore`] and
/// [`ModelInvocationCache`]). Centralizes the lock-poison-tolerant
/// accessors so each store only adds its domain-specific surface
/// (eviction policy, record shape) on top.
#[derive(Debug)]
struct KeyedStore<V> {
    inner: std::sync::RwLock<HashMap<(String, u64), V>>,
}

impl<V> Default for KeyedStore<V> {
    fn default() -> Self {
        Self {
            inner: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

impl<V: Clone> KeyedStore<V> {
    fn get(&self, model: &str, input_hash: u64) -> Option<V> {
        self.inner
            .read()
            .ok()
            .and_then(|g| g.get(&(model.to_string(), input_hash)).cloned())
    }
}

impl<V> KeyedStore<V> {
    fn insert(&self, model: &str, input_hash: u64, value: V) {
        if let Ok(mut g) = self.inner.write() {
            g.insert((model.to_string(), input_hash), value);
        }
    }

    /// Insert, but first drop the whole map when it has reached
    /// `max_entries` (`max_entries == 0` disables the bound). The
    /// size check and insert happen under one write lock so the bound
    /// holds even under concurrent inserts.
    fn insert_bounded(&self, model: &str, input_hash: u64, value: V, max_entries: usize) {
        if let Ok(mut g) = self.inner.write() {
            if max_entries > 0 && g.len() >= max_entries {
                g.clear();
            }
            g.insert((model.to_string(), input_hash), value);
        }
    }

    fn clear(&self) {
        if let Ok(mut g) = self.inner.write() {
            g.clear();
        }
    }

    fn len(&self) -> usize {
        self.inner.read().map(|g| g.len()).unwrap_or(0)
    }
}

#[derive(Debug, Default)]
pub struct NeuralProvenanceStore {
    inner: KeyedStore<NeuralProvenanceRecord>,
}

/// A single stored record. Matches the user-visible
/// [`crate::NeuralProvenance`] shape so EXPLAIN can construct
/// derivation entries without further transformation.
///
/// `feature_inputs` (Phase 12 EXPLAIN follow-up) carries the
/// per-binding `FeatureValue` map that fed the classifier on the
/// hot path. EXPLAIN Mode B reads from this map (when available)
/// instead of re-evaluating feature expressions against the
/// fact_row — which is the only way to surface authoritative values
/// for graph-structural FEATURE functions (`degree_centrality`,
/// `avg_neighbor`, etc.) whose evaluation requires the
/// `GraphAlgoHandle` that isn't threaded into the EXPLAIN path.
#[derive(Debug, Clone)]
pub struct NeuralProvenanceRecord {
    pub raw_probability: f64,
    pub calibrated_probability: Option<f64>,
    pub confidence_band: Option<crate::result::ConfidenceBand>,
    pub feature_inputs: HashMap<String, FeatureValue>,
}

impl NeuralProvenanceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, model: &str, input_hash: u64, record: NeuralProvenanceRecord) {
        self.inner.insert(model, input_hash, record);
    }

    pub fn get(&self, model: &str, input_hash: u64) -> Option<NeuralProvenanceRecord> {
        self.inner.get(model, input_hash)
    }

    pub fn clear(&self) {
        self.inner.clear();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Memoization cache for neural classifier outputs across a single
/// query evaluation. Per impl plan §1.4 decision D-4: cache is scoped
/// per-query (cleared at the start of evaluation); the key is
/// `(model_name, ClassifyInput::stable_hash)`.
///
/// Eviction policy: a naive "clear-when-full" heuristic — when the
/// cache reaches `max_entries`, the entire map is dropped. This keeps
/// the type allocation-light and avoids dragging in an LRU dep for
/// v1. Documented in impl plan as Stage 1 trade-off; a proper LRU
/// follow-up can swap the inner type without changing the public API.
#[derive(Debug, Default)]
pub struct ModelInvocationCache {
    inner: KeyedStore<f64>,
    max_entries: usize,
}

impl ModelInvocationCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: KeyedStore::default(),
            max_entries,
        }
    }

    /// Lookup. Returns `Some(prob)` on hit, `None` on miss.
    pub fn get(&self, model: &str, input_hash: u64) -> Option<f64> {
        self.inner.get(model, input_hash)
    }

    /// Insert. On overflow (cache size ≥ `max_entries`), drops the
    /// entire cache before inserting — naive but bounded. Callers
    /// should size `max_entries` for the expected working set.
    pub fn insert(&self, model: &str, input_hash: u64, value: f64) {
        self.inner
            .insert_bounded(model, input_hash, value, self.max_entries);
    }

    /// Empty the cache. Useful for `LocyConfig` users who reuse a
    /// shared cache across evaluations and want explicit reset.
    pub fn clear(&self) {
        self.inner.clear();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ─── Safetensors linear classifier ─────────────────────────────────

/// CPU single-layer logistic regression loaded from a `safetensors` file.
/// The historical name is retained for API compatibility; loading and
/// inference do not require Candle. Feature encoding and arithmetic remain
/// f32, with probabilities widened to f64 at the trait boundary.
///
/// **Expected safetensors layout:**
/// - `"weight"` — shape `[n_features]`, dtype `f32`.
/// - `"bias"` — shape `[1]`, dtype `f32`.
///
/// **Feature encoding** (deterministic, matches the TCK fixture):
/// - `FeatureValue::Float(f)` → `f as f32`.
/// - `FeatureValue::Int(i)` → `i as f32`.
/// - `FeatureValue::Bool(b)` → `0.0` / `1.0`.
/// - `FeatureValue::String(s)` → stable hash projected to `[-1, 1]`
///   via the djb2 algorithm divided by `i32::MAX` (production
///   classifiers should route String features through embedding
///   lookups in M3 D1; this is a pragmatic stand-in).
/// - `FeatureValue::Null`, `Vector` (or missing feature) → `0.0`.
pub struct CandleLinearClassifier {
    name: String,
    /// Feature names in the order the weight vector expects them.
    feature_order: Vec<String>,
    /// Loaded weight vector, length == `feature_order.len()`.
    weight: Vec<f32>,
    /// Scalar bias term.
    bias: f32,
}

impl std::fmt::Debug for CandleLinearClassifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CandleLinearClassifier")
            .field("name", &self.name)
            .field("feature_order", &self.feature_order)
            .field("n_features", &self.weight.len())
            .finish_non_exhaustive()
    }
}

impl CandleLinearClassifier {
    /// Load weights from a safetensors file on disk.
    ///
    /// `feature_order` must list features in the order matching the
    /// weight tensor's columns. Returns
    /// [`ClassifierError::Provider`] if the file is missing or the
    /// tensor shapes don't match.
    pub fn load(
        name: impl Into<String>,
        feature_order: Vec<String>,
        weights_path: impl AsRef<std::path::Path>,
    ) -> ClassifierResult<Self> {
        let path = weights_path.as_ref();
        let data = std::fs::read(path).map_err(|e| {
            ClassifierError::Provider(format!("linear: failed to read {path:?}: {e}"))
        })?;
        let tensors = safetensors::SafeTensors::deserialize(&data).map_err(|e| {
            ClassifierError::Provider(format!("linear: invalid safetensors in {path:?}: {e}"))
        })?;
        let read_f32 = |name: &str| -> ClassifierResult<Vec<f32>> {
            let tensor = tensors
                .tensor(name)
                .map_err(|e| ClassifierError::Provider(format!("linear: tensor '{name}': {e}")))?;
            if tensor.dtype() != safetensors::Dtype::F32 {
                return Err(ClassifierError::Provider(format!(
                    "linear: '{name}' must have dtype F32; got {:?}",
                    tensor.dtype()
                )));
            }
            // SafeTensors validates byte lengths. Decode little-endian values
            // without assuming the file buffer's alignment or host endianness.
            Ok(tensor
                .data()
                .as_chunks::<4>()
                .0
                .iter()
                .map(|bytes| f32::from_le_bytes(*bytes))
                .collect())
        };
        // Match the former flatten_all behavior, including singleton bias shapes.
        let weight = read_f32("weight")?;
        let bias_vec = read_f32("bias")?;
        if bias_vec.len() != 1 {
            return Err(ClassifierError::Provider(format!(
                "linear: 'bias' must be scalar (shape [1]); got len={}",
                bias_vec.len()
            )));
        }
        if weight.len() != feature_order.len() {
            return Err(ClassifierError::Provider(format!(
                "linear: weight length {} != feature_order length {}",
                weight.len(),
                feature_order.len()
            )));
        }
        Ok(Self {
            name: name.into(),
            feature_order,
            weight,
            bias: bias_vec[0],
        })
    }

    /// Project a feature value to an `f32` deterministically. See the
    /// struct-level documentation for the per-variant policy.
    fn encode_feature(&self, v: Option<&FeatureValue>) -> f32 {
        match v {
            Some(FeatureValue::Float(f)) => *f as f32,
            Some(FeatureValue::Int(i)) => *i as f32,
            Some(FeatureValue::Bool(b)) => f32::from(*b),
            Some(FeatureValue::String(s)) => {
                // djb2 → i32 → [-1, 1].
                let mut h: u32 = 5381;
                for byte in s.as_bytes() {
                    h = h.wrapping_mul(33).wrapping_add(*byte as u32);
                }
                (h as i32) as f32 / i32::MAX as f32
            }
            Some(FeatureValue::Null) | None => 0.0,
            _ => 0.0,
        }
    }
}

#[async_trait]
impl NeuralClassifier for CandleLinearClassifier {
    async fn classify(&self, inputs: &[ClassifyInput]) -> ClassifierResult<Vec<f64>> {
        Ok(inputs
            .iter()
            .map(|input| {
                let dot: f32 = self
                    .feature_order
                    .iter()
                    .zip(&self.weight)
                    .map(|(name, weight)| self.encode_feature(input.features.get(name)) * weight)
                    .sum();
                let logit = dot + self.bias;
                (1.0_f32 / (1.0_f32 + (-logit).exp())) as f64
            })
            .collect())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Numerically stable inverse sigmoid (logit) used by the default
/// `classify_logits`. Probabilities exactly at 0 or 1 produce `±∞`
/// logits which downstream calibration treats as a degenerate score.
fn inverse_sigmoid(p: f64) -> f64 {
    let p = p.clamp(0.0, 1.0);
    if p == 0.0 {
        f64::NEG_INFINITY
    } else if p == 1.0 {
        f64::INFINITY
    } else {
        (p / (1.0 - p)).ln()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_constant_returns_value_per_row() {
        let sr = MockClassifier::constant("classify/test", 0.7);
        let inputs = vec![
            ClassifyInput::new().with("x", FeatureValue::Float(1.0)),
            ClassifyInput::new().with("x", FeatureValue::Float(2.0)),
            ClassifyInput::new().with("x", FeatureValue::Float(3.0)),
        ];
        let out = sr.classify(&inputs).await.unwrap();
        assert_eq!(out, vec![0.7, 0.7, 0.7]);
        assert_eq!(out.len(), inputs.len());
        assert_eq!(sr.name(), "classify/test");
    }

    #[tokio::test]
    async fn mock_feature_driven() {
        let sr = MockClassifier::new("classify/feature", |inp| {
            match inp.features.get("severity") {
                Some(FeatureValue::Float(v)) => (*v / 10.0).clamp(0.0, 1.0),
                _ => 0.0,
            }
        });
        let inputs = vec![
            ClassifyInput::new().with("severity", FeatureValue::Float(2.0)),
            ClassifyInput::new().with("severity", FeatureValue::Float(9.0)),
            ClassifyInput::new().with("severity", FeatureValue::Float(15.0)), // clamps to 1.0
        ];
        let out = sr.classify(&inputs).await.unwrap();
        assert_eq!(out, vec![0.2, 0.9, 1.0]);
    }

    #[tokio::test]
    async fn classify_logits_default_inverse_sigmoid() {
        let sr = MockClassifier::constant("classify/test", 0.5);
        let out = sr.classify_logits(&[ClassifyInput::new()]).await.unwrap();
        // sigmoid⁻¹(0.5) = 0
        assert!((out[0] - 0.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn mock_rejects_nan() {
        let sr = MockClassifier::new("classify/nan", |_| f64::NAN);
        let err = sr.classify(&[ClassifyInput::new()]).await.unwrap_err();
        assert!(matches!(err, ClassifierError::DomainViolation { .. }));
    }

    #[test]
    fn feature_value_hash_distinguishes_variants() {
        // Slice 1: Float(0.0) and Int(0) MUST hash differently so the
        // memoization cache doesn't conflate them.
        fn h(v: FeatureValue) -> u64 {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }
        assert_ne!(h(FeatureValue::Float(0.0)), h(FeatureValue::Int(0)));
        assert_ne!(h(FeatureValue::Null), h(FeatureValue::Bool(false)));
        // Same variant + value → same hash.
        assert_eq!(h(FeatureValue::Float(0.5)), h(FeatureValue::Float(0.5)));
    }

    #[test]
    fn classify_input_hash_order_independent() {
        // Slice 1: HashMap insertion order shouldn't affect the
        // stable_hash output — same set of features must hash equal.
        let a = ClassifyInput::new()
            .with("country", FeatureValue::String("US".into()))
            .with("revenue", FeatureValue::Float(1.0e6));
        let b = ClassifyInput::new()
            .with("revenue", FeatureValue::Float(1.0e6))
            .with("country", FeatureValue::String("US".into()));
        assert_eq!(a.stable_hash(), b.stable_hash());
        let c = ClassifyInput::new()
            .with("country", FeatureValue::String("DE".into()))
            .with("revenue", FeatureValue::Float(1.0e6));
        assert_ne!(a.stable_hash(), c.stable_hash());
    }

    #[test]
    fn feature_value_vector_hash() {
        fn h(v: FeatureValue) -> u64 {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }
        let a = FeatureValue::Vector(vec![1.0, 2.0, 3.0]);
        let b = FeatureValue::Vector(vec![1.0, 2.0, 3.0]);
        let c = FeatureValue::Vector(vec![1.0, 2.0, 3.5]);
        assert_eq!(h(a.clone()), h(b));
        assert_ne!(h(a), h(c));
    }

    #[test]
    fn model_invocation_cache_hit_miss() {
        let cache = ModelInvocationCache::new(100);
        assert!(cache.get("m", 42).is_none());
        cache.insert("m", 42, 0.7);
        assert_eq!(cache.get("m", 42), Some(0.7));
        // Different model with same hash → miss.
        assert!(cache.get("other", 42).is_none());
        // Different hash → miss.
        assert!(cache.get("m", 43).is_none());
    }

    #[test]
    fn model_invocation_cache_evicts_on_overflow() {
        let cache = ModelInvocationCache::new(2);
        cache.insert("m", 1, 0.1);
        cache.insert("m", 2, 0.2);
        assert_eq!(cache.len(), 2);
        // Third insert triggers clear() then inserts; net size = 1.
        cache.insert("m", 3, 0.3);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get("m", 3), Some(0.3));
    }

    #[test]
    fn inverse_sigmoid_endpoints() {
        assert!(inverse_sigmoid(0.0).is_infinite() && inverse_sigmoid(0.0) < 0.0);
        assert!(inverse_sigmoid(1.0).is_infinite() && inverse_sigmoid(1.0) > 0.0);
        assert!((inverse_sigmoid(0.5) - 0.0).abs() < 1e-12);
    }
}

#[cfg(test)]
mod linear_classifier_tests {
    use super::*;
    use safetensors::tensor::{Dtype, TensorView, serialize_to_file};

    fn fixture(tensors: &[(&str, Dtype, Vec<usize>, Vec<u8>)]) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        let views = tensors.iter().map(|(name, dtype, shape, data)| {
            (*name, TensorView::new(*dtype, shape.clone(), data).unwrap())
        });
        serialize_to_file(views, None, file.path()).unwrap();
        file
    }

    fn floats(values: &[f32]) -> Vec<u8> {
        values.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    #[tokio::test]
    async fn matches_candle_reference_for_feature_encoding_and_extreme_logits() {
        let file = fixture(&[
            (
                "weight",
                Dtype::F32,
                vec![6],
                floats(&[0.75, -0.125, 0.5, 1.25, 9.0, 7.0]),
            ),
            ("bias", Dtype::F32, vec![1], floats(&[-0.25])),
        ]);
        let model = CandleLinearClassifier::load(
            "reference",
            ["x", "i", "b", "s", "v", "missing"]
                .map(String::from)
                .to_vec(),
            file.path(),
        )
        .unwrap();
        // Captured from the previous Candle CPU implementation with the same
        // tensors and rows. Allow f32 rounding differences in dot/exp kernels.
        let expected: [f64; 9] = [
            0.0,
            1.6159972671395084e-33,
            0.0003335594665259123,
            0.2217743992805481,
            0.37628287076950073,
            0.5608578324317932,
            0.9990841150283813,
            1.0,
            1.0,
        ];
        let inputs: Vec<_> = [-1000.0, -100.0, -10.0, -1.0, 0.0, 1.0, 10.0, 100.0, 1000.0]
            .into_iter()
            .map(|x| {
                ClassifyInput::new()
                    .with("x", FeatureValue::Float(x))
                    .with("i", FeatureValue::Int(3))
                    .with("b", FeatureValue::Bool(true))
                    .with("s", FeatureValue::String("supplier".into()))
                    .with("v", FeatureValue::Vector(vec![123.0]))
            })
            .collect();
        // Loading owns the weights; inference must not depend on the file.
        drop(file);
        let actual = model.classify(&inputs).await.unwrap();
        for (actual, expected) in actual.iter().zip(expected) {
            let tolerance = expected.abs() * 2e-6 + 1e-40;
            assert!(
                (actual - expected).abs() <= tolerance,
                "{actual} != {expected}"
            );
        }
        assert_eq!(model.name(), "reference");
        assert!(model.classify(&[]).await.unwrap().is_empty());
        for input in &inputs {
            assert_eq!(
                model
                    .classify(std::slice::from_ref(input))
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn retains_flattened_shapes_and_bias_only_models() {
        let file = fixture(&[
            ("weight", Dtype::F32, vec![1, 2], floats(&[2.0, -1.0])),
            ("bias", Dtype::F32, vec![], floats(&[0.0])),
        ]);
        let model = CandleLinearClassifier::load("flat", vec!["a".into(), "b".into()], file.path())
            .unwrap();
        let inputs = [
            ClassifyInput::new()
                .with("a", FeatureValue::Int(1))
                .with("b", FeatureValue::Float(2.0)),
            ClassifyInput::new()
                .with("a", FeatureValue::Null)
                .with("b", FeatureValue::Bool(false)),
        ];
        assert_eq!(model.classify(&inputs).await.unwrap(), vec![0.5, 0.5]);
        let file = fixture(&[
            ("weight", Dtype::F32, vec![0], vec![]),
            ("bias", Dtype::F32, vec![1, 1], floats(&[0.0])),
        ]);
        let model = CandleLinearClassifier::load("bias", vec![], file.path()).unwrap();
        assert_eq!(model.classify(&inputs).await.unwrap(), vec![0.5, 0.5]);
    }

    #[test]
    fn rejects_invalid_model_files_and_tensor_contracts() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(CandleLinearClassifier::load("bad", vec![], file.path()).is_err());
        let path = file.path().to_owned();
        drop(file);
        assert!(CandleLinearClassifier::load("missing", vec![], path).is_err());
        let weight = ("weight", Dtype::F32, vec![1], floats(&[1.0]));
        let bias = ("bias", Dtype::F32, vec![1], floats(&[0.0]));
        for tensors in [
            vec![weight.clone()],
            vec![bias.clone()],
            vec![
                weight.clone(),
                ("bias", Dtype::F32, vec![2], floats(&[0.0, 1.0])),
            ],
            vec![weight.clone(), ("bias", Dtype::F32, vec![0], vec![])],
            vec![
                (
                    "weight",
                    Dtype::F64,
                    vec![1],
                    1.0_f64.to_le_bytes().to_vec(),
                ),
                bias.clone(),
            ],
            vec![
                weight.clone(),
                ("bias", Dtype::F64, vec![1], 0.0_f64.to_le_bytes().to_vec()),
            ],
        ] {
            let file = fixture(&tensors);
            assert!(matches!(
                CandleLinearClassifier::load("bad", vec!["x".into()], file.path()),
                Err(ClassifierError::Provider(_))
            ));
        }
        let file = fixture(&[weight, bias]);
        assert!(CandleLinearClassifier::load("mismatch", vec![], file.path()).is_err());
        assert!(
            CandleLinearClassifier::load("mismatch", vec!["a".into(), "b".into()], file.path())
                .is_err()
        );
    }
}
