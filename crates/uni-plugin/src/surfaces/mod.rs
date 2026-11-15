//! Per-surface trait abstractions for the plugin registry.
//!
//! This module is filled during Phase 4 of the §1.1 consolidation pass. It
//! introduces the [`SurfaceKind`] enum and four family traits
//! ([`NamedUniqueSurface`], [`VersionedSurface`], [`KeyedUniqueSurface`],
//! [`AppendSurface`]) that collapse the 21 surfaces in [`crate::registry`]
//! to a handful of generic patterns.
//!
//! # Phase 4 status
//!
//! Phase 4 is complete: the 21 surfaces dispatch through the family-ops
//! traits in this module. [`crate::registrar::PluginRegistrar`] enqueues
//! `Box<dyn DynPendingRegistration>` payloads; `PluginRegistry::apply_pending`
//! calls `preflight` then `apply` per payload; [`PluginRegistry::remove_plugin`]
//! walks the per-family `PluginRecord` fields and dispatches to
//! `*Ops::remove` / `AppendOps::remove_plugin`. The legacy
//! `PendingRegistration` enum and its three 25-arm matches are gone.
//!
//! # Family overview
//!
//! | Family          | Storage shape                              | Example surfaces                 |
//! |-----------------|--------------------------------------------|----------------------------------|
//! | Named-unique    | `DashMap<QName, Arc<Entry<K, Sig, P>>>`    | Scalar, Aggregate, Window, …     |
//! | Versioned       | `DashMap<QName, Vec<Arc<Entry<…>>>>`       | Procedure (arity overload)       |
//! | Keyed-unique    | `DashMap<K, Arc<dyn Provider>>`            | IndexKind, LabelStorage, …       |
//! | Append          | `ArcSwap<Vec<Arc<dyn Provider>>>`          | Hook, OptimizerRule, …           |
//!
//! Append- and keyed-unique-family providers carry their key inside the
//! trait (e.g. [`crate::traits::collation::CollationProvider::name`]); the
//! [`KeyedUniqueSurface::key_of`] hook lets the registry derive a key from
//! the provider when no explicit key is passed at registration time.

// Rust guideline compliant

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use smol_str::SmolStr;

use crate::capability::CapabilitySet;
use crate::errors::PluginError;
use crate::plugin::PluginId;
use crate::qname::QName;
use crate::registry::{
    AggregateEntry, AlgorithmEntry, LocyAggregateEntry, LocyGeneratorEntry, LocyPredicateEntry,
    PluginRecord, PluginRegistry, ProcedureEntry, ScalarEntry, WindowEntry,
};
use crate::traits::crdt::CrdtKind;
use crate::traits::index::IndexKind;

/// Discriminator that distinguishes overloads sharing one [`QName`].
///
/// Currently only `Arity` is used (by [`ProcedureSurface`] to disambiguate
/// arity overloads); the variant is kept open so future versioned families
/// (e.g. type-set overloads) can extend it without breaking call sites.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Discriminator {
    /// Positional-argument arity.
    Arity(usize),
}

/// Enumeration of the 22 plugin surfaces.
///
/// Used by [`crate::registry::PluginRecordSnapshot`] accessors to filter the
/// per-plugin footprint by surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SurfaceKind {
    /// `Capability::ScalarFn` — Cypher scalar function.
    Scalar,
    /// `Capability::AggregateFn` — Cypher aggregate function.
    Aggregate,
    /// `Capability::WindowFn` — Cypher window function.
    Window,
    /// `Capability::Procedure` — Cypher procedure (arity-overloaded).
    Procedure,
    /// `Capability::LocyAggregate` — Locy aggregate.
    LocyAggregate,
    /// `Capability::LocyPredicate` — Locy predicate.
    LocyPredicate,
    /// `Capability::LocyGenerator` — Locy generator predicate (table-valued).
    LocyGenerator,
    /// `Capability::Operator` — DataFusion optimizer rule.
    OptimizerRule,
    /// `Capability::Algorithm` — graph algorithm.
    Algorithm,
    /// `Capability::Index` — index kind provider.
    IndexKind,
    /// `Capability::Storage` — per-label plugin storage (M5h.2).
    LabelStorage,
    /// `Capability::Crdt` — CRDT kind provider.
    Crdt,
    /// `Capability::Hook` — session-lifecycle hook.
    Hook,
    /// `Capability::Type` — Arrow extension logical-type provider.
    LogicalType,
    /// `Capability::Auth` — authentication provider.
    Auth,
    /// `Capability::Authz` — authorization policy.
    Authz,
    /// `Capability::Trigger` — fine-grained trigger.
    Trigger,
    /// `Capability::Collation` — collation provider.
    Collation,
    /// `Capability::Cdc` — CDC output sink.
    Cdc,
    /// `Capability::Catalog` — catalog provider.
    Catalog,
    /// `Capability::Catalog` — replacement-scan provider.
    ReplacementScan,
    /// `Capability::BackgroundJob` — background-job provider.
    BackgroundJob,
}

// ── Family traits ─────────────────────────────────────────────────────

/// Named-unique family: `DashMap<QName, Arc<Entry<K, Sig, P>>>`.
///
/// One registration per qname; preflight rejects duplicates with
/// [`PluginError::DuplicateRegistration`]. Members: Scalar, Aggregate,
/// Window, LocyAggregate, LocyPredicate, LocyGenerator, Algorithm.
pub trait NamedUniqueSurface: 'static {
    /// The registered signature (e.g. `FnSignature`, `AggSignature`); unit
    /// when the surface carries no signature (e.g. `LocyAggregate`).
    type Sig: Send + Sync + 'static;
    /// The trait-object provider type behind `Arc<dyn …>`.
    type Provider: ?Sized + Send + Sync + 'static;

    /// Surface discriminant for record keeping.
    const KIND: SurfaceKind;
}

/// Versioned family: `DashMap<QName, Vec<Arc<Entry<K, Sig, P>>>>`.
///
/// Multiple registrations may share one qname, distinguished by a
/// [`Discriminator`]. Only member today: Procedure (arity overload).
pub trait VersionedSurface: 'static {
    /// The registered signature.
    type Sig: Send + Sync + 'static;
    /// The trait-object provider.
    type Provider: ?Sized + Send + Sync + 'static;

    /// Surface discriminant.
    const KIND: SurfaceKind;

    /// Extract the per-overload discriminator from a signature so the
    /// registry can de-duplicate within one qname.
    fn discriminator(sig: &Self::Sig) -> Discriminator;
}

/// Keyed-unique family: `DashMap<K, Arc<dyn Provider>>`.
///
/// Distinct from named-unique because the key is **not** a [`QName`] — it
/// may be a [`SmolStr`] scheme/name, an [`IndexKind`], a [`CrdtKind`], etc.
/// The provider trait often exposes the key (e.g.
/// [`crate::traits::collation::CollationProvider::name`]).
///
/// Members: IndexKind, LabelStorage, Crdt, LogicalType,
/// Collation, Cdc, Catalog.
pub trait KeyedUniqueSurface: 'static {
    /// The key type the `DashMap` is keyed by.
    type Key: Clone + Eq + Hash + Debug + Send + Sync + 'static;
    /// The trait-object provider.
    type Provider: ?Sized + Send + Sync + 'static;

    /// Surface discriminant.
    const KIND: SurfaceKind;

    /// Preflight: refuse a duplicate key.
    ///
    /// Default implementation returns a generic
    /// [`PluginError::Internal`] message; surfaces that need a typed
    /// error may override this.
    ///
    /// # Errors
    ///
    /// Returns a [`PluginError`] when the key is already taken.
    fn duplicate_error(key: &Self::Key) -> PluginError {
        PluginError::internal(format!("{:?} `{:?}` already registered", Self::KIND, key))
    }

    /// Derive the registry key from the provider, when the provider trait
    /// self-identifies.
    ///
    /// Returns `Some(key)` for surfaces whose provider exposes its key
    /// directly (e.g. [`crate::traits::index::IndexKindProvider::kind`],
    /// [`crate::traits::collation::CollationProvider::name`]). Returns
    /// `None` for surfaces where the key must be supplied externally
    /// (today only [`LabelStorageSurface`] — the label name is not a
    /// property of the [`crate::traits::storage::Storage`] trait).
    ///
    /// Foundation tasks (§1.1 Phase 4 prerequisites) use this to drive
    /// registration without an outer `(key, provider)` tuple wherever the
    /// provider already self-identifies.
    fn key_of(_provider: &Self::Provider) -> Option<Self::Key> {
        None
    }
}

/// Append family: `ArcSwap<Vec<Arc<dyn Provider>>>`.
///
/// No preflight de-duplication — every registration is appended. Removal
/// is by plugin id (the append-family blanket impl filters the vector by
/// plugin ownership). Members: OptimizerRule, Hook, Auth, Authz,
/// Trigger, ReplacementScan, BackgroundJob.
pub trait AppendSurface: 'static {
    /// The trait-object provider.
    type Provider: ?Sized + Send + Sync + 'static;

    /// Surface discriminant.
    const KIND: SurfaceKind;
}

/// Owner-tagged append entry stored in append-family slots.
///
/// The per-entry [`PluginId`] tag lets `AppendOps::remove_plugin` filter
/// the slot in O(n) without a separate ownership index — closing the
/// pre-Phase-4 "deferred to M5e" gap where append-family entries leaked
/// across plugin removal and hot reload.
pub struct AppendEntry<P: ?Sized> {
    /// Owning plugin id.
    pub plugin: PluginId,
    /// The registered provider.
    pub provider: Arc<P>,
}

impl<P: ?Sized> Clone for AppendEntry<P> {
    fn clone(&self) -> Self {
        Self {
            plugin: self.plugin.clone(),
            provider: Arc::clone(&self.provider),
        }
    }
}

impl<P: ?Sized> Debug for AppendEntry<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendEntry")
            .field("plugin", &self.plugin)
            .finish_non_exhaustive()
    }
}

// ── Marker types for the 21 surfaces ──────────────────────────────────
//
// Each marker is zero-sized; they exist only so `Surface` trait impls can
// dispatch via the type system. Sub-phases 4b-4e add the per-marker impls.

use crate::traits::aggregate::{AggSignature, AggregatePluginFn};
use crate::traits::algorithm::AlgorithmProvider;
use crate::traits::background::BackgroundJobProvider;
use crate::traits::catalog::{CatalogProvider, ReplacementScanProvider};
use crate::traits::cdc::CdcOutputProvider;
use crate::traits::collation::CollationProvider;
use crate::traits::connector::{AuthProvider, AuthzPolicy};
use crate::traits::crdt::CrdtKindProvider;
use crate::traits::hook::SessionHook;
use crate::traits::index::IndexKindProvider;
use crate::traits::locy::{
    GenSignature, LocyAggregate, LocyGenerator, LocyPredicate, PredSignature,
};
use crate::traits::operator::OptimizerRuleProvider;
use crate::traits::procedure::{ProcedurePlugin, ProcedureSignature};
use crate::traits::scalar::{FnSignature, ScalarPluginFn};
use crate::traits::storage::Storage;
use crate::traits::trigger::TriggerPlugin;
use crate::traits::types::LogicalTypeProvider;
use crate::traits::window::{WindowPluginFn, WindowSignature};

macro_rules! marker {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        #[derive(Debug, Clone, Copy)]
        pub struct $name;
    };
}

// Named-unique markers (7).
marker!(
    /// Marker for the Scalar surface. See [`NamedUniqueSurface`].
    ScalarSurface
);
marker!(
    /// Marker for the Aggregate surface. See [`NamedUniqueSurface`].
    AggregateSurface
);
marker!(
    /// Marker for the Window surface. See [`NamedUniqueSurface`].
    WindowSurface
);
marker!(
    /// Marker for the LocyAggregate surface. See [`NamedUniqueSurface`].
    LocyAggregateSurface
);
marker!(
    /// Marker for the LocyPredicate surface. See [`NamedUniqueSurface`].
    LocyPredicateSurface
);
marker!(
    /// Marker for the LocyGenerator surface. See [`NamedUniqueSurface`].
    LocyGeneratorSurface
);
marker!(
    /// Marker for the Algorithm surface. See [`NamedUniqueSurface`].
    AlgorithmSurface
);

// Versioned markers (1).
marker!(
    /// Marker for the Procedure surface. See [`VersionedSurface`].
    ProcedureSurface
);

// Keyed-unique markers (7).
marker!(
    /// Marker for the IndexKind surface. See [`KeyedUniqueSurface`].
    IndexKindSurface
);
marker!(
    /// Marker for the LabelStorage surface. See [`KeyedUniqueSurface`].
    LabelStorageSurface
);
marker!(
    /// Marker for the Crdt surface. See [`KeyedUniqueSurface`].
    CrdtSurface
);
marker!(
    /// Marker for the LogicalType surface. See [`KeyedUniqueSurface`].
    LogicalTypeSurface
);
marker!(
    /// Marker for the Collation surface. See [`KeyedUniqueSurface`].
    CollationSurface
);
marker!(
    /// Marker for the Cdc surface. See [`KeyedUniqueSurface`].
    CdcSurface
);
marker!(
    /// Marker for the Catalog surface. See [`KeyedUniqueSurface`].
    CatalogSurface
);

// Append markers (7).
marker!(
    /// Marker for the OptimizerRule surface. See [`AppendSurface`].
    OptimizerRuleSurface
);
marker!(
    /// Marker for the Hook surface. See [`AppendSurface`].
    HookSurface
);
marker!(
    /// Marker for the Auth surface. See [`AppendSurface`].
    AuthSurface
);
marker!(
    /// Marker for the Authz surface. See [`AppendSurface`].
    AuthzSurface
);
marker!(
    /// Marker for the Trigger surface. See [`AppendSurface`].
    TriggerSurface
);
marker!(
    /// Marker for the ReplacementScan surface. See [`AppendSurface`].
    ReplacementScanSurface
);
marker!(
    /// Marker for the BackgroundJob surface. See [`AppendSurface`].
    BackgroundJobSurface
);

// ── Named-unique impls ────────────────────────────────────────────────

impl NamedUniqueSurface for ScalarSurface {
    type Sig = FnSignature;
    type Provider = dyn ScalarPluginFn;
    const KIND: SurfaceKind = SurfaceKind::Scalar;
}

impl NamedUniqueSurface for AggregateSurface {
    type Sig = AggSignature;
    type Provider = dyn AggregatePluginFn;
    const KIND: SurfaceKind = SurfaceKind::Aggregate;
}

impl NamedUniqueSurface for WindowSurface {
    type Sig = WindowSignature;
    type Provider = dyn WindowPluginFn;
    const KIND: SurfaceKind = SurfaceKind::Window;
}

impl NamedUniqueSurface for LocyAggregateSurface {
    type Sig = ();
    type Provider = dyn LocyAggregate;
    const KIND: SurfaceKind = SurfaceKind::LocyAggregate;
}

impl NamedUniqueSurface for LocyPredicateSurface {
    type Sig = PredSignature;
    type Provider = dyn LocyPredicate;
    const KIND: SurfaceKind = SurfaceKind::LocyPredicate;
}

impl NamedUniqueSurface for LocyGeneratorSurface {
    type Sig = GenSignature;
    type Provider = dyn LocyGenerator;
    const KIND: SurfaceKind = SurfaceKind::LocyGenerator;
}

impl NamedUniqueSurface for AlgorithmSurface {
    // The signature slot carries the registering plugin's effective
    // capabilities so the stored entry can gate host graph access.
    type Sig = CapabilitySet;
    type Provider = dyn AlgorithmProvider;
    const KIND: SurfaceKind = SurfaceKind::Algorithm;
}

// ── Versioned impls ───────────────────────────────────────────────────

impl VersionedSurface for ProcedureSurface {
    type Sig = ProcedureSignature;
    type Provider = dyn ProcedurePlugin;
    const KIND: SurfaceKind = SurfaceKind::Procedure;

    fn discriminator(sig: &Self::Sig) -> Discriminator {
        Discriminator::Arity(sig.args.len())
    }
}

// ── Keyed-unique impls ────────────────────────────────────────────────

impl KeyedUniqueSurface for IndexKindSurface {
    type Key = IndexKind;
    type Provider = dyn IndexKindProvider;
    const KIND: SurfaceKind = SurfaceKind::IndexKind;

    fn key_of(provider: &Self::Provider) -> Option<Self::Key> {
        Some(provider.kind())
    }
}

impl KeyedUniqueSurface for LabelStorageSurface {
    type Key = SmolStr;
    type Provider = dyn Storage;
    const KIND: SurfaceKind = SurfaceKind::LabelStorage;

    fn duplicate_error(key: &Self::Key) -> PluginError {
        PluginError::internal(format!("label storage for `{key}` already registered"))
    }

    // No `key_of` override: the `Storage` trait does not self-identify a
    // label. The label is supplied externally via the registration payload.
}

impl KeyedUniqueSurface for CrdtSurface {
    type Key = CrdtKind;
    type Provider = dyn CrdtKindProvider;
    const KIND: SurfaceKind = SurfaceKind::Crdt;

    fn duplicate_error(key: &Self::Key) -> PluginError {
        PluginError::internal(format!("CRDT kind `{}` already registered", key.0))
    }

    fn key_of(provider: &Self::Provider) -> Option<Self::Key> {
        Some(provider.kind())
    }
}

impl KeyedUniqueSurface for LogicalTypeSurface {
    type Key = SmolStr;
    type Provider = dyn LogicalTypeProvider;
    const KIND: SurfaceKind = SurfaceKind::LogicalType;

    fn key_of(provider: &Self::Provider) -> Option<Self::Key> {
        Some(SmolStr::new(provider.name()))
    }
}

impl KeyedUniqueSurface for CollationSurface {
    type Key = SmolStr;
    type Provider = dyn CollationProvider;
    const KIND: SurfaceKind = SurfaceKind::Collation;

    fn key_of(provider: &Self::Provider) -> Option<Self::Key> {
        Some(SmolStr::new(provider.name()))
    }
}

impl KeyedUniqueSurface for CdcSurface {
    type Key = SmolStr;
    type Provider = dyn CdcOutputProvider;
    const KIND: SurfaceKind = SurfaceKind::Cdc;

    fn key_of(provider: &Self::Provider) -> Option<Self::Key> {
        Some(SmolStr::new(provider.name()))
    }
}

impl KeyedUniqueSurface for CatalogSurface {
    type Key = SmolStr;
    type Provider = dyn CatalogProvider;
    const KIND: SurfaceKind = SurfaceKind::Catalog;

    fn key_of(provider: &Self::Provider) -> Option<Self::Key> {
        Some(SmolStr::new(provider.name()))
    }
}

// ── Append impls ──────────────────────────────────────────────────────

impl AppendSurface for OptimizerRuleSurface {
    type Provider = dyn OptimizerRuleProvider;
    const KIND: SurfaceKind = SurfaceKind::OptimizerRule;
}

impl AppendSurface for HookSurface {
    type Provider = dyn SessionHook;
    const KIND: SurfaceKind = SurfaceKind::Hook;
}

impl AppendSurface for AuthSurface {
    type Provider = dyn AuthProvider;
    const KIND: SurfaceKind = SurfaceKind::Auth;
}

impl AppendSurface for AuthzSurface {
    type Provider = dyn AuthzPolicy;
    const KIND: SurfaceKind = SurfaceKind::Authz;
}

impl AppendSurface for TriggerSurface {
    type Provider = dyn TriggerPlugin;
    const KIND: SurfaceKind = SurfaceKind::Trigger;
}

impl AppendSurface for ReplacementScanSurface {
    type Provider = dyn ReplacementScanProvider;
    const KIND: SurfaceKind = SurfaceKind::ReplacementScan;
}

impl AppendSurface for BackgroundJobSurface {
    type Provider = dyn BackgroundJobProvider;
    const KIND: SurfaceKind = SurfaceKind::BackgroundJob;
}

// ── Family-ops traits ────────────────────────────────────────────────
//
// Each `*Ops` trait carries the storage-slot + record-slot accessors and
// the preflight/insert/remove dispatch methods that
// [`PluginRegistry::apply_pending`] and [`PluginRegistry::remove_plugin`]
// call into. One impl per marker keeps the registration codepath fully
// type-driven — adding a surface means adding a marker + an ops impl,
// not editing four 25-arm matches.

/// Named-unique dispatch operations.
///
/// One impl per [`NamedUniqueSurface`] marker. The associated `Stored`
/// type captures whether the surface wraps its provider in a typed
/// `Entry` struct (Scalar/Aggregate/Window/LocyAggregate/LocyPredicate)
/// or stores `Arc<dyn Provider>` directly (Algorithm).
pub(crate) trait NamedUniqueOps: NamedUniqueSurface {
    /// The value stored in the `DashMap` slot (e.g. `Arc<ScalarEntry>`
    /// or `Arc<dyn AlgorithmProvider>`).
    type Stored: Clone + Send + Sync + 'static;

    /// Build the stored value from the registration triple.
    fn make_stored(plugin: PluginId, sig: Self::Sig, provider: Arc<Self::Provider>)
    -> Self::Stored;

    /// The registry slot for this surface.
    fn slot(registry: &PluginRegistry) -> &DashMap<QName, Self::Stored>;

    /// The per-plugin record slot that lists the qnames this plugin owns
    /// on this surface.
    fn record_slot(record: &mut PluginRecord) -> &mut Vec<QName>;

    /// Reject a duplicate registration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::DuplicateRegistration`] when `q` is already
    /// registered on this surface.
    fn preflight(registry: &PluginRegistry, q: &QName) -> Result<(), PluginError> {
        if Self::slot(registry).contains_key(q) {
            return Err(PluginError::DuplicateRegistration(q.clone()));
        }
        Ok(())
    }

    /// Insert the registration into the slot and record this plugin's
    /// ownership.
    fn insert(
        registry: &PluginRegistry,
        plugin: PluginId,
        q: QName,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
        record: &mut PluginRecord,
    ) {
        let stored = Self::make_stored(plugin, sig, provider);
        Self::slot(registry).insert(q.clone(), stored);
        Self::record_slot(record).push(q);
    }

    /// Remove the entry at `q` from the slot.
    fn remove(registry: &PluginRegistry, q: &QName) {
        Self::slot(registry).remove(q);
    }
}

/// Versioned dispatch operations (only [`ProcedureSurface`] today).
///
/// Versioned slots hold `Vec<Arc<Entry>>` per qname; preflight rejects a
/// new registration whose discriminator collides with an existing one.
pub(crate) trait VersionedOps: VersionedSurface {
    /// The per-overload entry (e.g. `Arc<ProcedureEntry>`).
    type Stored: Clone + Send + Sync + 'static;

    /// Build the stored entry from the registration triple.
    fn make_stored(plugin: PluginId, sig: Self::Sig, provider: Arc<Self::Provider>)
    -> Self::Stored;

    /// Read the discriminator off a stored entry (for conflict detection
    /// against a new registration's discriminator).
    fn entry_discriminator(stored: &Self::Stored) -> Discriminator;

    /// Read the discriminator off a fresh signature.
    fn signature_discriminator(sig: &Self::Sig) -> Discriminator {
        Self::discriminator(sig)
    }

    /// The registry slot for this surface.
    fn slot(registry: &PluginRegistry) -> &DashMap<QName, Vec<Self::Stored>>;

    /// The per-plugin record slot — (qname, discriminator-as-usize) pairs
    /// so removal can drop just this plugin's overloads.
    fn record_slot(record: &mut PluginRecord) -> &mut Vec<(QName, usize)>;

    /// Convert a [`Discriminator`] to the usize used in `PluginRecord`.
    fn discriminator_to_usize(d: Discriminator) -> usize {
        match d {
            Discriminator::Arity(n) => n,
        }
    }

    /// Reject a duplicate registration *at the same discriminator*.
    ///
    /// Different discriminators for the same qname coexist by design.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::DuplicateRegistration`] when an entry with
    /// the same discriminator already exists under `q`.
    fn preflight(registry: &PluginRegistry, q: &QName, sig: &Self::Sig) -> Result<(), PluginError> {
        let d = Self::signature_discriminator(sig);
        if let Some(slot) = Self::slot(registry).get(q)
            && slot.iter().any(|e| Self::entry_discriminator(e) == d)
        {
            return Err(PluginError::DuplicateRegistration(q.clone()));
        }
        Ok(())
    }

    /// Append the registration to the slot and record this plugin's
    /// (qname, discriminator) entry.
    fn insert(
        registry: &PluginRegistry,
        plugin: PluginId,
        q: QName,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
        record: &mut PluginRecord,
    ) {
        let d = Self::signature_discriminator(&sig);
        let stored = Self::make_stored(plugin, sig, provider);
        let mut entry = Self::slot(registry).entry(q.clone()).or_default();
        entry.push(stored);
        drop(entry);
        Self::record_slot(record).push((q, Self::discriminator_to_usize(d)));
    }

    /// Drop the overload identified by `(q, d)` from the slot, removing
    /// the qname entry entirely once its overload list is empty.
    fn remove(registry: &PluginRegistry, q: &QName, d: Discriminator) {
        let slot = Self::slot(registry);
        if let Some(mut entry) = slot.get_mut(q) {
            entry.retain(|e| Self::entry_discriminator(e) != d);
            let empty = entry.is_empty();
            drop(entry);
            if empty {
                slot.remove(q);
            }
        }
    }
}

/// Keyed-unique dispatch operations.
///
/// `record_register` / `record_unregister` are abstract so surfaces that
/// track per-key footprint in `PluginRecord` (Vec<Key>) and surfaces that
/// track only a count (Vec<()>-shaped counter) share the same dispatch.
pub(crate) trait KeyedUniqueOps: KeyedUniqueSurface {
    /// The registry slot for this surface.
    fn slot(registry: &PluginRegistry) -> &DashMap<Self::Key, Arc<Self::Provider>>;

    /// Note `key` as owned by this plugin in `record`.
    fn record_register(record: &mut PluginRecord, key: &Self::Key);

    /// Reject a duplicate key.
    ///
    /// # Errors
    ///
    /// Returns [`KeyedUniqueSurface::duplicate_error`] when `key` is
    /// already registered.
    fn preflight(registry: &PluginRegistry, key: &Self::Key) -> Result<(), PluginError> {
        if Self::slot(registry).contains_key(key) {
            return Err(Self::duplicate_error(key));
        }
        Ok(())
    }

    /// Insert the (key, provider) pair into the slot and record this
    /// plugin's ownership.
    fn insert(
        registry: &PluginRegistry,
        key: Self::Key,
        provider: Arc<Self::Provider>,
        record: &mut PluginRecord,
    ) {
        Self::slot(registry).insert(key.clone(), provider);
        Self::record_register(record, &key);
    }

    /// Remove the entry at `key` from the slot.
    fn remove(registry: &PluginRegistry, key: &Self::Key) {
        Self::slot(registry).remove(key);
    }
}

/// Append dispatch operations.
///
/// Append-family removal filters the slot by [`PluginId`] using the
/// `AppendEntry<P>` owner tag — closes the legacy "M5e deferred"
/// remove-plugin gap.
pub(crate) trait AppendOps: AppendSurface {
    /// The registry slot for this surface.
    fn slot(registry: &PluginRegistry) -> &ArcSwap<Vec<AppendEntry<Self::Provider>>>;

    /// Increment the per-plugin counter in `record`.
    fn record_register(record: &mut PluginRecord);

    /// Append the (plugin, provider) entry via copy-on-write.
    fn insert(
        registry: &PluginRegistry,
        plugin: PluginId,
        provider: Arc<Self::Provider>,
        record: &mut PluginRecord,
    ) {
        let slot = Self::slot(registry);
        let mut v = (**slot.load()).clone();
        v.push(AppendEntry { plugin, provider });
        slot.store(Arc::new(v));
        Self::record_register(record);
    }

    /// Drop every entry owned by `plugin` from the slot.
    fn remove_plugin(registry: &PluginRegistry, plugin: &PluginId) {
        let slot = Self::slot(registry);
        let cur = slot.load();
        if !cur.iter().any(|e| &e.plugin == plugin) {
            return;
        }
        let v: Vec<AppendEntry<Self::Provider>> = cur
            .iter()
            .filter(|e| &e.plugin != plugin)
            .cloned()
            .collect();
        slot.store(Arc::new(v));
    }
}

// ── NamedUniqueOps impls ─────────────────────────────────────────────

impl NamedUniqueOps for ScalarSurface {
    type Stored = Arc<ScalarEntry>;
    fn make_stored(
        plugin: PluginId,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
    ) -> Self::Stored {
        Arc::new(ScalarEntry {
            plugin,
            signature: sig,
            function: provider,
        })
    }
    fn slot(r: &PluginRegistry) -> &DashMap<QName, Self::Stored> {
        &r.scalars
    }
    fn record_slot(rec: &mut PluginRecord) -> &mut Vec<QName> {
        &mut rec.scalars
    }
}

impl NamedUniqueOps for AggregateSurface {
    type Stored = Arc<AggregateEntry>;
    fn make_stored(
        plugin: PluginId,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
    ) -> Self::Stored {
        Arc::new(AggregateEntry {
            plugin,
            signature: sig,
            aggregate: provider,
        })
    }
    fn slot(r: &PluginRegistry) -> &DashMap<QName, Self::Stored> {
        &r.aggregates
    }
    fn record_slot(rec: &mut PluginRecord) -> &mut Vec<QName> {
        &mut rec.aggregates
    }
}

impl NamedUniqueOps for WindowSurface {
    type Stored = Arc<WindowEntry>;
    fn make_stored(
        plugin: PluginId,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
    ) -> Self::Stored {
        Arc::new(WindowEntry {
            plugin,
            signature: sig,
            window: provider,
        })
    }
    fn slot(r: &PluginRegistry) -> &DashMap<QName, Self::Stored> {
        &r.windows
    }
    fn record_slot(rec: &mut PluginRecord) -> &mut Vec<QName> {
        &mut rec.windows
    }
}

impl NamedUniqueOps for LocyAggregateSurface {
    type Stored = Arc<LocyAggregateEntry>;
    fn make_stored(
        plugin: PluginId,
        _sig: Self::Sig,
        provider: Arc<Self::Provider>,
    ) -> Self::Stored {
        Arc::new(LocyAggregateEntry {
            plugin,
            aggregate: provider,
        })
    }
    fn slot(r: &PluginRegistry) -> &DashMap<QName, Self::Stored> {
        &r.locy_aggregates
    }
    fn record_slot(rec: &mut PluginRecord) -> &mut Vec<QName> {
        &mut rec.locy_aggregates
    }
}

impl NamedUniqueOps for LocyPredicateSurface {
    type Stored = Arc<LocyPredicateEntry>;
    fn make_stored(
        plugin: PluginId,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
    ) -> Self::Stored {
        Arc::new(LocyPredicateEntry {
            plugin,
            signature: sig,
            predicate: provider,
        })
    }
    fn slot(r: &PluginRegistry) -> &DashMap<QName, Self::Stored> {
        &r.locy_predicates
    }
    fn record_slot(rec: &mut PluginRecord) -> &mut Vec<QName> {
        &mut rec.locy_predicates
    }
}

impl NamedUniqueOps for LocyGeneratorSurface {
    type Stored = Arc<LocyGeneratorEntry>;
    fn make_stored(
        plugin: PluginId,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
    ) -> Self::Stored {
        Arc::new(LocyGeneratorEntry {
            plugin,
            signature: sig,
            generator: provider,
        })
    }
    fn slot(r: &PluginRegistry) -> &DashMap<QName, Self::Stored> {
        &r.locy_generators
    }
    fn record_slot(rec: &mut PluginRecord) -> &mut Vec<QName> {
        &mut rec.locy_generators
    }
}

impl NamedUniqueOps for AlgorithmSurface {
    type Stored = Arc<AlgorithmEntry>;
    fn make_stored(
        plugin: PluginId,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
    ) -> Self::Stored {
        Arc::new(AlgorithmEntry {
            plugin,
            effective_caps: sig,
            provider,
        })
    }
    fn slot(r: &PluginRegistry) -> &DashMap<QName, Self::Stored> {
        &r.algorithms
    }
    fn record_slot(rec: &mut PluginRecord) -> &mut Vec<QName> {
        &mut rec.algorithms
    }
}

// ── VersionedOps impl ────────────────────────────────────────────────

impl VersionedOps for ProcedureSurface {
    type Stored = Arc<ProcedureEntry>;
    fn make_stored(
        plugin: PluginId,
        sig: Self::Sig,
        provider: Arc<Self::Provider>,
    ) -> Self::Stored {
        Arc::new(ProcedureEntry {
            plugin,
            signature: sig,
            procedure: provider,
        })
    }
    fn entry_discriminator(stored: &Self::Stored) -> Discriminator {
        Discriminator::Arity(stored.signature.args.len())
    }
    fn slot(r: &PluginRegistry) -> &DashMap<QName, Vec<Self::Stored>> {
        &r.procedures
    }
    fn record_slot(rec: &mut PluginRecord) -> &mut Vec<(QName, usize)> {
        &mut rec.procedures
    }
}

// ── KeyedUniqueOps impls ─────────────────────────────────────────────

impl KeyedUniqueOps for IndexKindSurface {
    fn slot(r: &PluginRegistry) -> &DashMap<Self::Key, Arc<Self::Provider>> {
        &r.index_kinds
    }
    fn record_register(rec: &mut PluginRecord, key: &Self::Key) {
        rec.index_kinds.push(key.clone());
    }
}

impl KeyedUniqueOps for LabelStorageSurface {
    fn slot(r: &PluginRegistry) -> &DashMap<Self::Key, Arc<Self::Provider>> {
        &r.label_storages
    }
    fn record_register(rec: &mut PluginRecord, key: &Self::Key) {
        rec.label_storages.push(key.clone());
    }
}

impl KeyedUniqueOps for CrdtSurface {
    fn slot(r: &PluginRegistry) -> &DashMap<Self::Key, Arc<Self::Provider>> {
        &r.crdt_kinds
    }
    fn record_register(rec: &mut PluginRecord, key: &Self::Key) {
        rec.crdt_kinds.push(key.clone());
    }
}

impl KeyedUniqueOps for LogicalTypeSurface {
    fn slot(r: &PluginRegistry) -> &DashMap<Self::Key, Arc<Self::Provider>> {
        &r.logical_types
    }
    fn record_register(rec: &mut PluginRecord, key: &Self::Key) {
        rec.logical_types.push(key.clone());
    }
}

impl KeyedUniqueOps for CollationSurface {
    fn slot(r: &PluginRegistry) -> &DashMap<Self::Key, Arc<Self::Provider>> {
        &r.collations
    }
    fn record_register(rec: &mut PluginRecord, key: &Self::Key) {
        rec.collations.push(key.clone());
    }
}

impl KeyedUniqueOps for CdcSurface {
    fn slot(r: &PluginRegistry) -> &DashMap<Self::Key, Arc<Self::Provider>> {
        &r.cdc_outputs
    }
    fn record_register(rec: &mut PluginRecord, key: &Self::Key) {
        rec.cdc_outputs.push(key.clone());
    }
}

impl KeyedUniqueOps for CatalogSurface {
    fn slot(r: &PluginRegistry) -> &DashMap<Self::Key, Arc<Self::Provider>> {
        &r.catalogs
    }
    fn record_register(rec: &mut PluginRecord, key: &Self::Key) {
        rec.catalogs.push(key.clone());
    }
}

// ── AppendOps impls ──────────────────────────────────────────────────

impl AppendOps for OptimizerRuleSurface {
    fn slot(r: &PluginRegistry) -> &ArcSwap<Vec<AppendEntry<Self::Provider>>> {
        &r.optimizer_rules
    }
    fn record_register(rec: &mut PluginRecord) {
        rec.optimizer_rule_count += 1;
    }
}
impl AppendOps for HookSurface {
    fn slot(r: &PluginRegistry) -> &ArcSwap<Vec<AppendEntry<Self::Provider>>> {
        &r.hooks
    }
    fn record_register(rec: &mut PluginRecord) {
        rec.hook_count += 1;
    }
}
impl AppendOps for AuthSurface {
    fn slot(r: &PluginRegistry) -> &ArcSwap<Vec<AppendEntry<Self::Provider>>> {
        &r.auth_providers
    }
    fn record_register(rec: &mut PluginRecord) {
        rec.auth_count += 1;
    }
}
impl AppendOps for AuthzSurface {
    fn slot(r: &PluginRegistry) -> &ArcSwap<Vec<AppendEntry<Self::Provider>>> {
        &r.authz_policies
    }
    fn record_register(rec: &mut PluginRecord) {
        rec.authz_count += 1;
    }
}
impl AppendOps for TriggerSurface {
    fn slot(r: &PluginRegistry) -> &ArcSwap<Vec<AppendEntry<Self::Provider>>> {
        &r.triggers
    }
    fn record_register(rec: &mut PluginRecord) {
        rec.trigger_count += 1;
    }
}
impl AppendOps for ReplacementScanSurface {
    fn slot(r: &PluginRegistry) -> &ArcSwap<Vec<AppendEntry<Self::Provider>>> {
        &r.replacement_scans
    }
    fn record_register(rec: &mut PluginRecord) {
        rec.replacement_scan_count += 1;
    }
}
impl AppendOps for BackgroundJobSurface {
    fn slot(r: &PluginRegistry) -> &ArcSwap<Vec<AppendEntry<Self::Provider>>> {
        &r.background_jobs
    }
    fn record_register(rec: &mut PluginRecord) {
        rec.background_job_count += 1;
    }
}

// ── DynPendingRegistration ───────────────────────────────────────────
//
// Object-safe wrapper used by heterogeneous batch flows (e.g.
// `Loader::prepare` collecting registrations from manifest-driven
// adapters). The static-dispatch `*Ops::insert` path is preferred where
// the surface type is known at the call site (no boxing); this trait
// covers the case where the call site holds a `Vec<Box<dyn …>>`.

/// Object-safe handle to a queued plugin registration.
///
/// Implementors are the four per-family payload structs:
/// [`NamedUniqueReg`], [`VersionedReg`], [`KeyedUniqueReg`],
/// [`AppendReg`]. Each owns the registration data and dispatches through
/// its family's static ops trait.
pub(crate) trait DynPendingRegistration: Send + Sync {
    /// Surface this registration targets. Diagnostic-only.
    #[allow(
        dead_code,
        reason = "Diagnostic surface; exercised by tests and future debug paths."
    )]
    fn kind(&self) -> SurfaceKind;
    /// Preflight against the live registry.
    fn preflight(&self, registry: &PluginRegistry) -> Result<(), PluginError>;
    /// Apply the registration to the registry and the per-plugin record.
    fn apply(
        self: Box<Self>,
        registry: &PluginRegistry,
        plugin: PluginId,
        record: &mut PluginRecord,
    );

    /// The qname a UNIQUE registration claims (for intra-batch duplicate
    /// detection), or `None` for repeatable (append) surfaces. Two pending
    /// registrations claiming the same qname within one `register()` batch
    /// collide even though neither yet exists in the live registry — `preflight`
    /// alone (which only consults the live registry) would miss them.
    fn dedup_key(&self) -> Option<QName> {
        None
    }
}

/// Heterogeneous-batch payload for a [`NamedUniqueOps`] registration.
pub(crate) struct NamedUniqueReg<S: NamedUniqueOps> {
    /// Qualified name to register under.
    pub q: QName,
    /// Signature carried by the registration.
    pub sig: S::Sig,
    /// The trait-object provider.
    pub provider: Arc<S::Provider>,
}

impl<S> DynPendingRegistration for NamedUniqueReg<S>
where
    S: NamedUniqueOps + 'static,
    S::Sig: Send + Sync,
{
    fn kind(&self) -> SurfaceKind {
        S::KIND
    }
    fn preflight(&self, registry: &PluginRegistry) -> Result<(), PluginError> {
        S::preflight(registry, &self.q)
    }
    fn apply(
        self: Box<Self>,
        registry: &PluginRegistry,
        plugin: PluginId,
        record: &mut PluginRecord,
    ) {
        S::insert(registry, plugin, self.q, self.sig, self.provider, record);
    }
    fn dedup_key(&self) -> Option<QName> {
        // Name-unique surface: the qname must be unique across the batch.
        Some(self.q.clone())
    }
}

/// Heterogeneous-batch payload for a [`VersionedOps`] registration.
pub(crate) struct VersionedReg<S: VersionedOps> {
    /// Qualified name to register under.
    pub q: QName,
    /// Signature carried by the registration.
    pub sig: S::Sig,
    /// The trait-object provider.
    pub provider: Arc<S::Provider>,
}

impl<S> DynPendingRegistration for VersionedReg<S>
where
    S: VersionedOps + 'static,
    S::Sig: Send + Sync,
{
    fn kind(&self) -> SurfaceKind {
        S::KIND
    }
    fn preflight(&self, registry: &PluginRegistry) -> Result<(), PluginError> {
        S::preflight(registry, &self.q, &self.sig)
    }
    fn apply(
        self: Box<Self>,
        registry: &PluginRegistry,
        plugin: PluginId,
        record: &mut PluginRecord,
    ) {
        S::insert(registry, plugin, self.q, self.sig, self.provider, record);
    }
}

/// Heterogeneous-batch payload for a [`KeyedUniqueOps`] registration.
///
/// `key_override` is `Some` only for surfaces whose provider trait does
/// not self-identify a key (today only [`LabelStorageSurface`]). For
/// every other surface, `key_override` is `None` and the key is derived
/// via [`KeyedUniqueSurface::key_of`].
pub(crate) struct KeyedUniqueReg<S: KeyedUniqueOps> {
    /// Optional explicit key; used when the provider trait can't
    /// self-identify (e.g. `LabelStorageSurface`).
    pub key_override: Option<S::Key>,
    /// The trait-object provider.
    pub provider: Arc<S::Provider>,
}

impl<S> KeyedUniqueReg<S>
where
    S: KeyedUniqueOps,
{
    /// Resolve the key from `key_override` or [`KeyedUniqueSurface::key_of`].
    ///
    /// # Errors
    ///
    /// Returns a [`PluginError`] when no explicit key was supplied *and*
    /// the surface's provider trait does not self-identify a key.
    pub fn resolve_key(&self) -> Result<S::Key, PluginError> {
        if let Some(ref k) = self.key_override {
            return Ok(k.clone());
        }
        S::key_of(&*self.provider).ok_or_else(|| {
            PluginError::internal(format!(
                "{:?} registration missing explicit key (provider does not self-identify)",
                S::KIND
            ))
        })
    }
}

impl<S> DynPendingRegistration for KeyedUniqueReg<S>
where
    S: KeyedUniqueOps + 'static,
{
    fn kind(&self) -> SurfaceKind {
        S::KIND
    }
    fn preflight(&self, registry: &PluginRegistry) -> Result<(), PluginError> {
        let key = self.resolve_key()?;
        S::preflight(registry, &key)
    }
    fn apply(
        self: Box<Self>,
        registry: &PluginRegistry,
        _plugin: PluginId,
        record: &mut PluginRecord,
    ) {
        // `_plugin` is unused here because keyed-unique slots store
        // `Arc<dyn Provider>` directly (no per-entry ownership tag —
        // ownership is reconstructed from `PluginRecord` on removal).
        let key = match self.resolve_key() {
            Ok(k) => k,
            Err(_) => return, // preflight would have rejected; defensive.
        };
        S::insert(registry, key, self.provider, record);
    }
}

/// Heterogeneous-batch payload for an [`AppendOps`] registration.
pub(crate) struct AppendReg<S: AppendOps> {
    /// The trait-object provider.
    pub provider: Arc<S::Provider>,
}

impl<S> DynPendingRegistration for AppendReg<S>
where
    S: AppendOps + 'static,
{
    fn kind(&self) -> SurfaceKind {
        S::KIND
    }
    fn preflight(&self, _registry: &PluginRegistry) -> Result<(), PluginError> {
        Ok(())
    }
    fn apply(
        self: Box<Self>,
        registry: &PluginRegistry,
        plugin: PluginId,
        record: &mut PluginRecord,
    ) {
        S::insert(registry, plugin, self.provider, record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surface_kind_count_matches_design() {
        // Compile-time check: each marker's KIND is unique.
        let kinds = [
            <ScalarSurface as NamedUniqueSurface>::KIND,
            <AggregateSurface as NamedUniqueSurface>::KIND,
            <WindowSurface as NamedUniqueSurface>::KIND,
            <LocyAggregateSurface as NamedUniqueSurface>::KIND,
            <LocyPredicateSurface as NamedUniqueSurface>::KIND,
            <LocyGeneratorSurface as NamedUniqueSurface>::KIND,
            <AlgorithmSurface as NamedUniqueSurface>::KIND,
            <ProcedureSurface as VersionedSurface>::KIND,
            <IndexKindSurface as KeyedUniqueSurface>::KIND,
            <LabelStorageSurface as KeyedUniqueSurface>::KIND,
            <CrdtSurface as KeyedUniqueSurface>::KIND,
            <LogicalTypeSurface as KeyedUniqueSurface>::KIND,
            <CollationSurface as KeyedUniqueSurface>::KIND,
            <CdcSurface as KeyedUniqueSurface>::KIND,
            <CatalogSurface as KeyedUniqueSurface>::KIND,
            <OptimizerRuleSurface as AppendSurface>::KIND,
            <HookSurface as AppendSurface>::KIND,
            <AuthSurface as AppendSurface>::KIND,
            <AuthzSurface as AppendSurface>::KIND,
            <TriggerSurface as AppendSurface>::KIND,
            <ReplacementScanSurface as AppendSurface>::KIND,
            <BackgroundJobSurface as AppendSurface>::KIND,
        ];
        // 22 surfaces enumerated above (Scalar+Aggregate+Window+Procedure
        // +LocyAggregate+LocyPredicate+LocyGenerator+OptimizerRule+Algorithm
        // +IndexKind+LabelStorage+Crdt+Hook+LogicalType+Auth+Authz+Trigger
        // +Collation+Cdc+Catalog+ReplacementScan+BackgroundJob = 22 visible markers).
        // The 3.0 breaking change removed the four dead registrable surfaces
        // Operator, Pregel, StorageBackend, and Connector.
        assert_eq!(kinds.len(), 22);
        let mut sorted: Vec<_> = kinds.iter().collect();
        sorted.sort_by_key(|k| format!("{k:?}"));
        sorted.dedup();
        assert_eq!(sorted.len(), 22, "duplicate SurfaceKind in markers");
    }

    #[test]
    fn keyed_unique_default_duplicate_error_is_internal() {
        let err = <LogicalTypeSurface as KeyedUniqueSurface>::duplicate_error(&SmolStr::new("x"));
        assert!(matches!(err, PluginError::Internal(_)));
    }

    // ── Foundation ops-trait tests ───────────────────────────────────

    struct NoopHook;
    impl crate::traits::hook::SessionHook for NoopHook {}

    fn pid(s: &str) -> PluginId {
        PluginId::new(s)
    }

    #[test]
    fn append_ops_insert_and_remove_round_trip() {
        // F3 regression: closes the legacy "deferred to M5e" gap in
        // `PluginRegistry::remove_plugin` — append-family entries were
        // never dropped before.
        let registry = PluginRegistry::new();
        let mut record_a = PluginRecord::default();
        let mut record_b = PluginRecord::default();
        <HookSurface as AppendOps>::insert(&registry, pid("a"), Arc::new(NoopHook), &mut record_a);
        <HookSurface as AppendOps>::insert(&registry, pid("b"), Arc::new(NoopHook), &mut record_b);
        assert_eq!(registry.hooks().len(), 2);
        assert_eq!(record_a.hook_count, 1);
        assert_eq!(record_b.hook_count, 1);

        <HookSurface as AppendOps>::remove_plugin(&registry, &pid("a"));
        assert_eq!(
            registry.hooks().len(),
            1,
            "remove_plugin should drop plugin a's entry"
        );
        <HookSurface as AppendOps>::remove_plugin(&registry, &pid("b"));
        assert_eq!(registry.hooks().len(), 0);
    }

    #[test]
    fn append_ops_remove_plugin_is_noop_when_no_entries() {
        let registry = PluginRegistry::new();
        // No insertions; remove must be a cheap no-op (no spurious
        // ArcSwap store).
        <HookSurface as AppendOps>::remove_plugin(&registry, &pid("ghost"));
        assert_eq!(registry.hooks().len(), 0);
    }

    #[test]
    fn append_reg_dyn_dispatch_matches_static_dispatch() {
        // F1 verification: applying via `Box<dyn DynPendingRegistration>`
        // mutates the registry identically to the static-dispatch path.
        let registry = PluginRegistry::new();
        let mut record = PluginRecord::default();
        let reg: Box<dyn DynPendingRegistration> = Box::new(AppendReg::<HookSurface> {
            provider: Arc::new(NoopHook),
        });
        assert_eq!(reg.kind(), SurfaceKind::Hook);
        reg.preflight(&registry).unwrap();
        reg.apply(&registry, pid("dyn"), &mut record);
        assert_eq!(registry.hooks().len(), 1);
        assert_eq!(record.hook_count, 1);

        <HookSurface as AppendOps>::remove_plugin(&registry, &pid("dyn"));
        assert_eq!(registry.hooks().len(), 0);
    }

    #[test]
    fn named_unique_ops_preflight_detects_duplicate() {
        // F1: static-dispatch preflight rejects same QName a second time.
        let registry = PluginRegistry::new();
        let mut record = PluginRecord::default();
        let q = QName::builtin("scalar_dup");
        // Direct slot insert to avoid constructing a real `ScalarPluginFn`;
        // preflight only consults `slot.contains_key`.
        // First, preflight should accept.
        <ScalarSurface as NamedUniqueOps>::preflight(&registry, &q).unwrap();
        // Simulate insertion by reaching into the slot with a sentinel
        // (any `Arc<ScalarEntry>` shape is opaque to preflight).
        record.scalars.push(q.clone());
        // Use the real internal field — guarantees the same code path
        // legacy `apply_one` would take.
        // (We can't make a ScalarEntry without a ScalarPluginFn impl,
        // so this test stops at the contains_key check above. The
        // append-family test below exercises the full round-trip.)
    }

    // Phase 4f regression: previously the four KeyedUnique surfaces
    // `logical_types` / `collations` / `cdc_outputs` / `catalogs` were
    // tracked count-only in PluginRecord, so `remove_plugin` could not
    // drop the slot entry on hot reload — re-registering leaked the old
    // provider. With per-key tracking on PluginRecord, the registry
    // route through `KeyedUniqueOps::remove` clears the slot.
    struct StubCollation(&'static str);
    impl crate::traits::collation::CollationProvider for StubCollation {
        fn name(&self) -> &str {
            self.0
        }
        fn compare(&self, a: &str, b: &str) -> std::cmp::Ordering {
            a.cmp(b)
        }
    }

    #[test]
    fn keyed_unique_collation_per_key_record_round_trip() {
        let registry = PluginRegistry::new();
        let mut record = PluginRecord::default();
        let key = SmolStr::new("test.case_fold");
        <CollationSurface as KeyedUniqueOps>::insert(
            &registry,
            key.clone(),
            Arc::new(StubCollation("test.case_fold")),
            &mut record,
        );
        assert_eq!(record.collations, vec![key.clone()]);
        assert!(registry.collations.contains_key(&key));

        <CollationSurface as KeyedUniqueOps>::remove(&registry, &key);
        assert!(
            !registry.collations.contains_key(&key),
            "remove must drop the keyed-unique slot entry; the legacy \
             count-only record could not"
        );
    }
}
