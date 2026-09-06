#![allow(clippy::all)]
//! WS-D (P0.4): proof that the L0 durable merge path routes CRDT merges
//! through the plugin registry.
//!
//! `L0Buffer::merge_crdt_properties` previously called native `try_merge`
//! directly, so a registered `CrdtKindProvider` was silently bypassed on
//! the L0 path. This test registers an invocation-counting provider,
//! stamps it onto an `L0Buffer`, drives a merge (two inserts of a CRDT
//! property for the same vid), and asserts the provider's `from_persisted`
//! was actually called — i.e. the merge went through the registry, not the
//! native fallback. A companion assertion confirms the merged value still
//! matches native semantics.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use uni_common::Properties;
use uni_common::core::id::Vid;
use uni_crdt::{Crdt, GCounter};
use uni_plugin::traits::crdt::{CrdtKind, CrdtKindProvider, CrdtOp, CrdtState, ScalarValue};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry};
use uni_store::runtime::l0::L0Buffer;

#[derive(Default)]
struct CountingProvider {
    from_persisted_calls: AtomicUsize,
}

impl CrdtKindProvider for CountingProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("uni-crdt:g-counter")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(NativeState {
            inner: Crdt::GCounter(GCounter::new()),
        })
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        self.from_persisted_calls.fetch_add(1, Ordering::SeqCst);
        let inner = Crdt::from_msgpack(bytes)
            .map_err(|e| FnError::new(0xA01, format!("from_persisted: {e}")))?;
        Ok(Box::new(NativeState { inner }))
    }
}

struct NativeState {
    inner: Crdt,
}

impl CrdtState for NativeState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, _op: &CrdtOp) -> Result<(), FnError> {
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        let other = other
            .as_any()
            .downcast_ref::<NativeState>()
            .ok_or_else(|| FnError::new(0xA10, "merge: type mismatch"))?;
        self.inner
            .try_merge(&other.inner)
            .map_err(|e| FnError::new(0xA11, format!("native merge: {e}")))
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Utf8(Some(self.inner.type_name().to_owned())))
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        self.inner
            .to_msgpack()
            .map_err(|e| FnError::new(0xA12, format!("persist: {e}")))
    }
}

fn registry_with_provider() -> (Arc<PluginRegistry>, Arc<CountingProvider>) {
    let registry = Arc::new(PluginRegistry::new());
    let provider = Arc::new(CountingProvider::default());
    let caps = CapabilitySet::from_iter_of([Capability::Crdt]);
    let mut r = PluginRegistrar::new(PluginId::new("test.counting-gcounter"), &caps, &registry);
    r.crdt_kind(
        CrdtKind::new("uni-crdt:g-counter"),
        Arc::clone(&provider) as Arc<dyn CrdtKindProvider>,
    )
    .expect("register provider");
    r.commit_to_registry().expect("commit");
    (registry, provider)
}

fn gcounter_value(replica: &str, by: u64) -> uni_common::Value {
    let mut g = GCounter::new();
    g.increment(replica, by);
    uni_common::Value::from(serde_json::to_value(Crdt::GCounter(g)).unwrap())
}

#[test]
fn l0_merge_routes_through_registry() {
    let (registry, provider) = registry_with_provider();

    let mut buf = L0Buffer::new(0, None);
    buf.set_plugin_registry(Arc::clone(&registry));

    let vid = Vid::new(1);
    // First insert creates the entry (fast path, no merge).
    let mut p1 = Properties::new();
    p1.insert("counter".to_string(), gcounter_value("r1", 5));
    buf.insert_vertex(vid, p1);

    // Second insert for the same vid merges the CRDT property — this is
    // the path that previously bypassed the registry.
    let mut p2 = Properties::new();
    p2.insert("counter".to_string(), gcounter_value("r2", 7));
    buf.insert_vertex(vid, p2);

    // The registry path ran (from_persisted was called for the merge).
    assert!(
        provider.from_persisted_calls.load(Ordering::SeqCst) > 0,
        "L0 merge must route through the registered CrdtKindProvider"
    );

    // Merged value still matches native GCounter semantics (5 + 7 = 12).
    let merged = buf
        .vertex_properties
        .get(&vid)
        .and_then(|p| p.get("counter"))
        .expect("merged counter present");
    let crdt: Crdt = serde_json::from_value(merged.clone().into()).unwrap();
    match crdt {
        Crdt::GCounter(g) => assert_eq!(g.value(), 12, "5 + 7 = 12"),
        other => panic!("expected GCounter, got {other:?}"),
    }
}

#[test]
fn l0_merge_falls_back_to_native_without_provider() {
    // No registry set → behaviour-preserving native try_merge.
    let mut buf = L0Buffer::new(0, None);

    let vid = Vid::new(1);
    let mut p1 = Properties::new();
    p1.insert("counter".to_string(), gcounter_value("r1", 3));
    buf.insert_vertex(vid, p1);
    let mut p2 = Properties::new();
    p2.insert("counter".to_string(), gcounter_value("r2", 4));
    buf.insert_vertex(vid, p2);

    let merged = buf
        .vertex_properties
        .get(&vid)
        .and_then(|p| p.get("counter"))
        .expect("merged counter present");
    let crdt: Crdt = serde_json::from_value(merged.clone().into()).unwrap();
    match crdt {
        Crdt::GCounter(g) => assert_eq!(g.value(), 7, "3 + 4 = 7 via native fallback"),
        other => panic!("expected GCounter, got {other:?}"),
    }
}

/// A provider whose `from_persisted` always fails.
#[derive(Default)]
struct FailingProvider;

impl CrdtKindProvider for FailingProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("uni-crdt:g-counter")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(NativeState {
            inner: Crdt::GCounter(GCounter::new()),
        })
    }
    fn from_persisted(&self, _bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        Err(FnError::new(0xDEAD, "provider is broken"))
    }
}

/// A provider failure is not a variant mismatch, and must not be reported as one.
///
/// #233. The L0 merge used `merge_via_registry(..).is_ok()`, collapsing a
/// provider's `from_persisted` / `merge` / `persist` failure into the same
/// `false` as a genuine `TypeMismatch`. It then fell through to a warning
/// reading "overwriting CRDT property with a different CRDT variant" — naming
/// the wrong cause — while discarding merged state. Both cases still end in
/// last-writer-wins; what changed is that the provider failure is now counted
/// and named.
#[test]
fn a_provider_failure_is_counted_separately_from_a_variant_mismatch() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let registry = Arc::new(PluginRegistry::new());
    let caps = CapabilitySet::from_iter_of([Capability::Crdt]);
    let mut r = PluginRegistrar::new(PluginId::new("test.failing-gcounter"), &caps, &registry);
    r.crdt_kind(
        CrdtKind::new("uni-crdt:g-counter"),
        Arc::new(FailingProvider) as Arc<dyn CrdtKindProvider>,
    )
    .expect("register provider");
    r.commit_to_registry().expect("commit");

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let mut buf = L0Buffer::new(0, None);
        buf.set_plugin_registry(Arc::clone(&registry));
        let vid = Vid::new(1);

        let mut p1 = Properties::new();
        p1.insert("counter".to_string(), gcounter_value("r1", 5));
        buf.insert_vertex(vid, p1);

        // Same variant on both sides, so any failure here is the provider's.
        let mut p2 = Properties::new();
        p2.insert("counter".to_string(), gcounter_value("r2", 7));
        buf.insert_vertex(vid, p2);
    });

    let failures: u64 = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(ckey, _, _, _)| ckey.key().name() == "uni_crdt_provider_merge_failures_total")
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(v) => v,
            _ => 0,
        })
        .sum();

    assert_eq!(
        failures, 1,
        "a broken provider must be counted as a provider failure, not reported as a CRDT \
         variant mismatch"
    );
}
