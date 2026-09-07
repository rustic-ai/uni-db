//! M10 per-major `Linker` cache for multi-version ABI coexistence.
//!
//! A plugin's manifest carries a semver range describing which host
//! ABI majors it tolerates ([`uni_plugin::AbiRange`]). The host can
//! support several majors concurrently — `^1` plugins use the v1
//! linker, `^2` plugins use the v2 linker — so an ABI bump does not
//! force every plugin to be rebuilt in lockstep with the host.
//!
//! [`MultiVersionLinker`] is the dispatch point. It owns a wasmtime
//! `Engine` and, for each `(major, caps_signature)` pair, lazily
//! constructs and caches an `Arc<Linker<HostState>>`.
//!
//! # Why cache?
//!
//! `Linker::new` plus per-host-fn `func_wrap` registrations are cheap
//! (microseconds), but constructing a fresh linker on every plugin
//! `load()` adds avoidable allocation churn — the same `(major, caps)`
//! combination hits on every hot-reload of any plugin in that
//! configuration. The cache reuses the Arc-shared linker across all
//! compatible plugins.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uni_plugin::AbiRange;
use wasmtime::Engine;
use wasmtime::component::Linker;

use crate::error::WasmError;
use crate::host_state::HostState;
use crate::linker::{build_scalar_linker_v1, build_scalar_linker_v2};

/// Major versions the host can link against.
///
/// Probed in order by [`MultiVersionLinker::linker_for`] — the first
/// major whose plugin's [`AbiRange`] matches wins. v2 is a placeholder
/// today (see `build_scalar_linker_v2`) but the dispatch path is
/// already exercised so a real v2 cutover is purely additive.
pub const SUPPORTED_MAJORS: &[u64] = &[1, 2];

/// Cache key: `(host_major, caps_signature)`. The caps signature is a
/// deterministic concatenation of the sorted capability strings.
type CacheKey = (u64, String);

/// Resolve a plugin's declared [`AbiRange`] to the host major it links against.
///
/// Probes [`SUPPORTED_MAJORS`] in order; the first major whose `abi.matches`
/// is `true` wins. Shared by [`MultiVersionLinker::linker_for`] and the
/// loader's per-pool linker selection so both apply the same dispatch.
///
/// # Errors
///
/// Returns [`WasmError::AbiUnsupported`] when no supported major matches.
pub(crate) fn major_for_abi(abi: &AbiRange) -> Result<u64, WasmError> {
    SUPPORTED_MAJORS
        .iter()
        .copied()
        .find(|m| abi.matches(*m))
        .ok_or_else(|| WasmError::AbiUnsupported {
            requested: abi.as_str().to_owned(),
            supported: SUPPORTED_MAJORS.to_vec(),
        })
}

/// Per-major `Linker` cache.
///
/// Construct once at host startup (e.g., alongside the `Engine`),
/// then call [`Self::linker_for`] on every plugin load.
pub struct MultiVersionLinker {
    engine: Engine,
    cache: RwLock<HashMap<CacheKey, Arc<Linker<HostState>>>>,
}

impl std::fmt::Debug for MultiVersionLinker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiVersionLinker")
            .field("cached_entries", &self.cache.read().len())
            .finish_non_exhaustive()
    }
}

impl MultiVersionLinker {
    /// Construct a new cache over `engine`.
    #[must_use]
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Resolve and return the `Linker` matching the plugin's declared
    /// ABI range and effective capability set.
    ///
    /// Probes [`SUPPORTED_MAJORS`] in order; the first major whose
    /// `abi.matches(major)` is `true` is selected. The corresponding
    /// `build_scalar_linker_vN` is invoked on cache miss; subsequent
    /// calls with the same `(major, caps)` return the cached Arc.
    ///
    /// # Errors
    ///
    /// Returns [`WasmError::AbiUnsupported`] when no supported major
    /// matches the plugin's `abi` range.
    pub fn linker_for(
        &self,
        abi: &AbiRange,
        effective_caps: &uni_plugin::CapabilitySet,
    ) -> Result<Arc<Linker<HostState>>, WasmError> {
        let major = major_for_abi(abi)?;
        let key: CacheKey = (major, caps_signature(effective_caps));
        if let Some(cached) = self.cache.read().get(&key) {
            return Ok(Arc::clone(cached));
        }
        // Miss — build under the write lock. Use the entry pattern so
        // a concurrent racer's insert is observed.
        let mut cache = self.cache.write();
        if let Some(cached) = cache.get(&key) {
            return Ok(Arc::clone(cached));
        }
        let built = match major {
            1 => build_scalar_linker_v1(&self.engine, effective_caps)?,
            2 => build_scalar_linker_v2(&self.engine, effective_caps)?,
            _ => {
                return Err(WasmError::AbiUnsupported {
                    requested: abi.as_str().to_owned(),
                    supported: SUPPORTED_MAJORS.to_vec(),
                });
            }
        };
        let arc = Arc::new(built);
        cache.insert(key, Arc::clone(&arc));
        Ok(arc)
    }

    /// Reset the cache. Intended for tests; production callers don't
    /// need to clear because cached linkers are immutable after build.
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }
}

/// Build a deterministic signature for an effective capability set (linker
/// cache key).
///
/// `CapabilitySet` is backed by a `BTreeSet`, so its serialization is sorted
/// and stable — including the attenuation patterns, so two grants that differ
/// only in (say) their network allow-list key distinct linkers.
fn caps_signature(caps: &uni_plugin::CapabilitySet) -> String {
    // Serializing a `BTreeSet<Capability>` cannot fail. `expect` rather than
    // `unwrap_or_default` because the default is the empty string, which would
    // make every capability set hash to the SAME linker cache key — silently
    // handing one plugin a linker built for another plugin's grants. A panic
    // on the impossible is the safer failure here.
    serde_json::to_string(caps).expect("a CapabilitySet always serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> Engine {
        let mut cfg = wasmtime::Config::new();
        cfg.wasm_component_model(true);
        Engine::new(&cfg).expect("engine")
    }

    #[test]
    fn linker_for_v1_matches_caret_one() {
        let mv = MultiVersionLinker::new(engine());
        let abi = AbiRange::parse("^1").unwrap();
        let l = mv
            .linker_for(&abi, &uni_plugin::CapabilitySet::new())
            .expect("v1 selected");
        assert!(Arc::strong_count(&l) >= 2, "cache holds an Arc clone");
    }

    #[test]
    fn linker_for_v2_matches_caret_two() {
        let mv = MultiVersionLinker::new(engine());
        let abi = AbiRange::parse("^2").unwrap();
        let _ = mv
            .linker_for(&abi, &uni_plugin::CapabilitySet::new())
            .expect("v2 selected");
    }

    #[test]
    fn linker_for_rejects_unsupported_major() {
        let mv = MultiVersionLinker::new(engine());
        let abi = AbiRange::parse("^99").unwrap();
        let err = match mv.linker_for(&abi, &uni_plugin::CapabilitySet::new()) {
            Ok(_) => panic!("expected AbiUnsupported"),
            Err(e) => e,
        };
        match err {
            WasmError::AbiUnsupported {
                requested,
                supported,
            } => {
                assert_eq!(requested, "^99");
                assert_eq!(supported, vec![1, 2]);
            }
            other => panic!("expected AbiUnsupported, got {other:?}"),
        }
    }

    #[test]
    fn cache_returns_same_arc_on_repeat_lookup() {
        let mv = MultiVersionLinker::new(engine());
        let abi = AbiRange::parse("^1").unwrap();
        let a = mv
            .linker_for(&abi, &uni_plugin::CapabilitySet::new())
            .unwrap();
        let b = mv
            .linker_for(&abi, &uni_plugin::CapabilitySet::new())
            .unwrap();
        assert!(Arc::ptr_eq(&a, &b), "expected cache hit to return same Arc");
    }

    #[test]
    fn caps_signature_is_order_invariant() {
        use uni_plugin::{Capability, CapabilitySet};
        // CapabilitySet is a BTreeSet, so insertion order can't change the
        // signature; distinct attenuation must, though.
        let a = CapabilitySet::from_iter_of([Capability::ScalarFn, Capability::Procedure]);
        let b = CapabilitySet::from_iter_of([Capability::Procedure, Capability::ScalarFn]);
        assert_eq!(caps_signature(&a), caps_signature(&b));
        let net1 = CapabilitySet::from_iter_of([Capability::Network {
            allow: vec!["https://a/**".into()],
        }]);
        let net2 = CapabilitySet::from_iter_of([Capability::Network {
            allow: vec!["https://b/**".into()],
        }]);
        assert_ne!(
            caps_signature(&net1),
            caps_signature(&net2),
            "different allow-lists must key distinct linkers"
        );
    }
}
