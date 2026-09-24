use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use uni_common::Value;

use crate::neural::{ModelInvocationCache, NeuralClassifier, NeuralProvenanceStore};
use crate::semiring::ResolvedSemiringConfig;
use crate::types::SemiringKind;

/// Type alias for the runtime registry mapping a Locy model name to its
/// concrete classifier implementation. Populated by callers (Python /
/// Rust application code, TCK steps) before invoking a Locy program that
/// references neural models.
pub type ClassifierRegistry = HashMap<String, Arc<dyn NeuralClassifier>>;

/// Configuration error raised by [`LocyConfig::resolve`].
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigError {
    /// `exact_probability = true` combined with a non-`AddMultProb`
    /// semiring is incoherent: weighted model counting requires the
    /// independence semiring as its base. Most commonly hit by setting
    /// `semiring = MaxMinProb` while `exact_probability` remains on.
    IncoherentSemiring {
        semiring: SemiringKind,
        message: &'static str,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncoherentSemiring { semiring, message } => {
                write!(f, "incoherent semiring {semiring:?}: {message}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Configuration for the Locy orchestrator.
#[derive(Debug, Clone)]
pub struct LocyConfig {
    /// Maximum fixpoint iterations per recursive stratum.
    pub max_iterations: usize,
    /// Overall evaluation timeout, when the caller set one.
    ///
    /// `Some(t)` is the whole budget: the strata / fixpoint / SLG checks and
    /// the deadline every operator inside a clause body checks are both `t`,
    /// exactly as `query_with(..).timeout(t)` replaces `query_timeout` for a
    /// Cypher statement. `None` evaluates under [`Self::DEFAULT_TIMEOUT`]
    /// between strata while operators keep the database's `query_timeout`,
    /// so an unconfigured program is never looser than a Cypher query.
    ///
    /// An `Option` rather than a `Duration` defaulting to 300s because the
    /// engine must tell "asked for 300s" from "asked for nothing": with a
    /// plain value the only safe combination was `min(query_timeout, t)`,
    /// which silently discarded any explicit timeout above the database
    /// default (issue #289).
    pub timeout: Option<Duration>,
    /// When `false` (default), an evaluation that exceeds `timeout` or
    /// `max_iterations` returns [`UniError::LocyIncomplete`] rather than
    /// silently yielding partial facts. Set to `true` for anytime / best-effort
    /// semantics: the partial `LocyResult` is returned (`Ok`) with its
    /// `incomplete` diagnostics populated, and the caller is responsible for
    /// checking them.
    ///
    /// [`UniError::LocyIncomplete`]: uni_common::UniError::LocyIncomplete
    pub allow_partial: bool,
    /// Maximum recursion depth for EXPLAIN derivation trees.
    pub max_explain_depth: usize,
    /// Maximum recursion depth for SLG resolution.
    pub max_slg_depth: usize,
    /// Maximum candidate modifications to generate during ABDUCE.
    pub max_abduce_candidates: usize,
    /// Maximum validated results to return from ABDUCE.
    pub max_abduce_results: usize,
    /// Maximum bytes of derived facts to hold in memory per relation.
    pub max_derived_bytes: usize,
    /// When true, BEST BY applies a secondary sort on remaining columns for
    /// deterministic tie-breaking. Set to false for non-deterministic (faster)
    /// selection.
    pub deterministic_best_by: bool,
    /// When true, MNOR/MPROD reject values outside \[0,1\] with an error instead
    /// of clamping. When false (default), values are clamped silently.
    pub strict_probability_domain: bool,
    /// Underflow threshold for MPROD log-space switch (spec §5.3).
    ///
    /// When the running product drops below this value, `product_f64`
    /// switches to log-space accumulation to prevent floating-point
    /// underflow.
    pub probability_epsilon: f64,
    /// When true, groups flagged with shared probabilistic dependencies use
    /// exact BDD-based probability computation instead of the independence
    /// assumption (MNOR/MPROD). Defaults to false (independence mode).
    pub exact_probability: bool,
    /// Maximum number of BDD variables (unique base facts) allowed per
    /// aggregation group. If a group exceeds this limit, it falls back to
    /// the independence-mode result and emits a `BddLimitExceeded` warning.
    pub max_bdd_variables: usize,
    /// Top-k proof filtering (Scallop, Huang et al. 2021): retain at most k
    /// proofs per derived fact, ranked by proof probability. When 0 (default),
    /// all proofs are retained (unlimited mode).
    pub top_k_proofs: usize,
    /// Override `top_k_proofs` during training. When `Some(k)`, training
    /// evaluation uses this value instead of `top_k_proofs`. When `None`,
    /// `top_k_proofs` is used for both training and inference.
    pub top_k_proofs_training: Option<usize>,
    /// Active probability semiring (rollout decision D-7). Defaults to
    /// [`SemiringKind::AddMultProb`] which matches the Phase 1/2 noisy-OR
    /// and product behavior byte-for-byte. Opting into
    /// [`SemiringKind::MaxMinProb`] (Viterbi / fuzzy) triggers a
    /// non-suppressible `FuzzyNotProbabilistic` warning on PROB-bearing
    /// rules (D-9).
    ///
    /// When `exact_probability` is `true`, [`LocyConfig::resolve`]
    /// promotes `AddMultProb` to [`SemiringKind::BddExact`] for the
    /// duration of evaluation. A `MaxMinProb` + `exact_probability` combo
    /// is rejected as incoherent.
    pub semiring: SemiringKind,
    /// When `true`, the compiler accepts `CREATE MODEL` statements.
    /// Default flipped from `false` to `true` in the Phase C
    /// gate-closure cycle — the neural stack is GA. The flag remains
    /// settable for explicit opt-out: setting it to `false`
    /// re-imposes the original Phase B compile-time rejection
    /// (`LocyCompileError::NeuralPreviewDisabled`) for environments
    /// that need to disable the feature surface entirely. The
    /// grammar always parses neural syntax regardless — the gate
    /// lives at compile time.
    pub neural_predicates_preview: bool,
    /// Phase B Slice 3 runtime registry mapping a model name (the
    /// identifier from `CREATE MODEL <name> AS ...`) to its concrete
    /// [`NeuralClassifier`]. Rule bodies that invoke `<name>(args)` will
    /// dispatch through this map at runtime. Models referenced by name
    /// in a rule body but absent from the registry produce a runtime
    /// error (`UnknownNeuralModel`) at the first invocation attempt;
    /// programs that declare models but never invoke them at runtime
    /// don't need entries here.
    ///
    /// Defaults to an empty map. The registry is held by `Arc` so the
    /// same shared map can flow through fixpoint executor clones without
    /// per-iteration deep-copy.
    pub classifier_registry: ClassifierRegistry,
    /// Phase B Slice 4 (post-Slice-3 follow-up): optional shared
    /// memoization cache for neural classifier outputs. When `None`
    /// (default), the runtime materializes a fresh cache per query
    /// sized to `classifier_cache_max`. When `Some`, the cache is
    /// reused across queries — useful for batch evaluation pipelines.
    pub classifier_cache: Option<Arc<ModelInvocationCache>>,
    /// Maximum entries in the per-query memoization cache. Default
    /// 100_000. Naive eviction (clear when full) — see
    /// `ModelInvocationCache::insert` doc.
    pub classifier_cache_max: usize,
    /// Phase C B1-B3 follow-up: optional side-channel store
    /// recording (raw, calibrated, confidence_band) per
    /// classifier invocation. When `Some`, the runtime
    /// `apply_model_invocations` writes one record per
    /// (model, input_hash); EXPLAIN reads from this store to
    /// populate `NeuralProvenance` entries on derivations.
    /// When `None`, EXPLAIN's neural_calls fall back to the
    /// (raw-only) yield-alias lookup path. The store flows
    /// through alongside `classifier_cache`.
    pub classifier_provenance_store: Option<Arc<NeuralProvenanceStore>>,
    /// Parameters bound to `$name` references inside rules and QUERY/RETURN
    /// expressions.  Equivalent to the parameter map passed to `db.query()`.
    /// Keys are parameter names **without** the leading `$` (e.g., `"agent_id"`
    /// binds `$agent_id`).
    pub params: HashMap<String, Value>,
}

impl LocyConfig {
    /// The strata / fixpoint budget used when [`Self::timeout`] is `None`.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

    /// The overall evaluation budget: the explicit [`Self::timeout`], else
    /// [`Self::DEFAULT_TIMEOUT`].
    pub fn effective_timeout(&self) -> Duration {
        self.timeout.unwrap_or(Self::DEFAULT_TIMEOUT)
    }

    /// Resolve scattered probability-related fields into a single
    /// [`ResolvedSemiringConfig`] for threading through the planner and
    /// executors. Performs the `exact_probability` → `BddExact`
    /// promotion (decision D-7) and rejects incoherent combinations
    /// (decision D-9).
    pub fn resolve(&self) -> Result<ResolvedSemiringConfig, ConfigError> {
        let kind = match (self.semiring, self.exact_probability) {
            (SemiringKind::AddMultProb, true) => SemiringKind::BddExact,
            (SemiringKind::AddMultProb, false) => SemiringKind::AddMultProb,
            (SemiringKind::MaxMinProb, true) => {
                return Err(ConfigError::IncoherentSemiring {
                    semiring: SemiringKind::MaxMinProb,
                    message: "MaxMinProb cannot be combined with exact_probability=true; \
                              weighted model counting requires the AddMultProb semiring",
                });
            }
            (SemiringKind::MaxMinProb, false) => SemiringKind::MaxMinProb,
            (SemiringKind::BddExact, _) => SemiringKind::BddExact,
            // Phase C C0: TopKProofs is its own correction story
            // (per-tag DNF inclusion-exclusion) and is incoherent
            // with `exact_probability` — that knob means "promote to
            // whole-group BDD" and conflicts with the per-tag form.
            (SemiringKind::TopKProofs { k: 0 }, _) => {
                return Err(ConfigError::IncoherentSemiring {
                    semiring: SemiringKind::TopKProofs { k: 0 },
                    message: "TopKProofs requires k > 0; \
                              k=0 would retain no proofs and reduce to ⊥",
                });
            }
            (SemiringKind::TopKProofs { .. }, true) => {
                return Err(ConfigError::IncoherentSemiring {
                    semiring: self.semiring,
                    message: "TopKProofs cannot be combined with exact_probability=true; \
                              TopKProofs already provides its own correction via \
                              per-tag inclusion-exclusion (impl plan §3.0)",
                });
            }
            (SemiringKind::TopKProofs { k }, false) => SemiringKind::TopKProofs { k },
        };
        Ok(ResolvedSemiringConfig {
            kind,
            strict_probability_domain: self.strict_probability_domain,
            probability_epsilon: self.probability_epsilon,
            max_bdd_variables: self.max_bdd_variables,
        })
    }
}

impl Default for LocyConfig {
    fn default() -> Self {
        Self {
            max_iterations: 1000,
            timeout: None,
            allow_partial: false,
            max_explain_depth: 100,
            max_slg_depth: 1000,
            max_abduce_candidates: 20,
            max_abduce_results: 10,
            max_derived_bytes: 256 * 1024 * 1024,
            deterministic_best_by: true,
            strict_probability_domain: false,
            probability_epsilon: 1e-15,
            exact_probability: false,
            max_bdd_variables: 1000,
            top_k_proofs: 0,
            top_k_proofs_training: None,
            semiring: SemiringKind::AddMultProb,
            // Phase C gate-closure: neural stack is GA; default flag
            // flipped from false to true. Set to false for explicit
            // opt-out (re-imposes the Phase B compile-time rejection).
            neural_predicates_preview: true,
            classifier_registry: HashMap::new(),
            classifier_cache: None,
            classifier_cache_max: 100_000,
            classifier_provenance_store: None,
            params: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_default_is_add_mult_prob() {
        let cfg = LocyConfig::default();
        let resolved = cfg.resolve().unwrap();
        assert_eq!(resolved.kind, SemiringKind::AddMultProb);
    }

    #[test]
    fn resolve_promotes_exact_probability_to_bdd() {
        let cfg = LocyConfig {
            exact_probability: true,
            ..Default::default()
        };
        let resolved = cfg.resolve().unwrap();
        assert_eq!(resolved.kind, SemiringKind::BddExact);
    }

    #[test]
    fn resolve_rejects_maxmin_plus_exact() {
        let cfg = LocyConfig {
            exact_probability: true,
            semiring: SemiringKind::MaxMinProb,
            ..Default::default()
        };
        assert!(matches!(
            cfg.resolve(),
            Err(ConfigError::IncoherentSemiring { .. })
        ));
    }

    #[test]
    fn resolve_passes_maxmin_through() {
        let cfg = LocyConfig {
            semiring: SemiringKind::MaxMinProb,
            ..Default::default()
        };
        let resolved = cfg.resolve().unwrap();
        assert_eq!(resolved.kind, SemiringKind::MaxMinProb);
    }

    #[test]
    fn resolve_passes_topkproofs_through() {
        let cfg = LocyConfig {
            semiring: SemiringKind::TopKProofs { k: 4 },
            ..Default::default()
        };
        let resolved = cfg.resolve().unwrap();
        assert_eq!(resolved.kind, SemiringKind::TopKProofs { k: 4 });
    }

    #[test]
    fn resolve_rejects_topkproofs_k_zero() {
        let cfg = LocyConfig {
            semiring: SemiringKind::TopKProofs { k: 0 },
            ..Default::default()
        };
        assert!(matches!(
            cfg.resolve(),
            Err(ConfigError::IncoherentSemiring { .. })
        ));
    }

    #[test]
    fn resolve_rejects_topkproofs_plus_exact_probability() {
        let cfg = LocyConfig {
            semiring: SemiringKind::TopKProofs { k: 4 },
            exact_probability: true,
            ..Default::default()
        };
        assert!(matches!(
            cfg.resolve(),
            Err(ConfigError::IncoherentSemiring { .. })
        ));
    }
}
