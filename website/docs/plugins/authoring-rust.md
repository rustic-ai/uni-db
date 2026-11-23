---
title: Authoring native (Rust) plugins
status: ga
---

# Authoring native (Rust) plugins

The sandboxed loaders ([WASM Component Model](loaders/wasm-components.md),
[Extism](loaders/extism.md), [Rhai](loaders/rhai.md), [PyO3](loaders/pyo3.md)) cover scalar
functions, aggregates, procedures, and graph algorithms. **Native Rust plugins** are the only kind
that can author *every* extension surface — optimizer rules, index kinds, label storage, CRDTs,
hooks, triggers, logical types, collations, and more — because those surfaces are in-process Rust
traits with no cross-ABI wire format.

A native plugin is a Rust type that implements the [`Plugin`](#the-plugin-trait) trait and is added
to the database with `Uni::add_plugin`. It is **trusted**: it runs in-process with no sandbox, so
only load native plugins you compiled or audited.

## The `Plugin` trait

The trait is deliberately small — all per-surface detail lives in the capability traits. A plugin
is a *bundle* of registrations:

```rust
use uni_plugin::{Plugin, PluginManifest, PluginRegistrar, PluginError};

pub struct GeoPlugin {
    manifest: PluginManifest,
}

impl Plugin for GeoPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.scalar_fn(/* qname, signature, impl */)?;
        Ok(())
    }

    // `init(&self, cx)` and `shutdown(&self)` are optional (default no-ops).
}
```

- **`manifest()`** returns the plugin's [`PluginManifest`](concepts.md#manifest-abi) — id (reverse-DNS),
  version, ABI range, declared capabilities, determinism, and an optional Ed25519 signature.
- **`register()`** is called once at load time with a `PluginRegistrar`. Every registration is
  capability-gated and staged; the registrar commits atomically only if `register()` returns `Ok`.
- **`init()` / `shutdown()`** are optional lifecycle callbacks.

Add it to a database:

```rust
let db = Uni::in_memory().build().await?;
db.add_plugin(GeoPlugin::new())?;   // trust policy (signature) is enforced here
```

`Uni::add_plugin<P: Plugin>(&self, plugin: P) -> Result<()>` validates the manifest against the
host [trust policy](trust-and-capabilities.md), runs `register()`, and commits.

## Surfaces, traits, and registrar methods

Each surface is a trait in `uni_plugin::traits` plus a `PluginRegistrar` method that registers an
implementation. The method requires the matching [capability](reference.md#extension-surfaces); a
plugin whose manifest didn't declare it fails registration with `PluginError::CapabilityRequired`.

| Surface | Trait | Registrar method |
|---|---|---|
| Scalar function | `ScalarPluginFn` | `scalar_fn(qname, sig, f)` |
| Aggregate function | `AggregatePluginFn` | `aggregate_fn(qname, sig, f)` |
| Window function | `WindowPluginFn` | `window_fn(qname, sig, f)` |
| Procedure | `ProcedurePlugin` | `procedure(qname, sig, p)` |
| Optimizer rule | `OptimizerRuleProvider` | `optimizer_rule(r)` |
| Index kind | `IndexKindProvider` | `index_kind(kind, p)` |
| Label storage | `Storage` | `label_storage(label, s)` |
| CRDT kind | `CrdtKindProvider` | `crdt_kind(kind, p)` |
| Session / query hook | `SessionHook` | `hook(h)` |
| Trigger | `TriggerPlugin` | `trigger(t)` |
| Graph algorithm | `AlgorithmProvider` | `algorithm(qname, p)` |
| Locy aggregate | `LocyAggregate` | `locy_aggregate(qname, a)` |
| Locy predicate | `LocyPredicate` | `locy_predicate(qname, sig, p)` |
| Locy generator | `LocyGenerator` | `locy_generator(qname, sig, g)` |
| Logical (Arrow extension) type | `LogicalTypeProvider` | `logical_type(t)` |
| Authentication | `AuthProvider` | `auth_provider(p)` |
| Authorization | `AuthzPolicy` | `authz_policy(p)` |
| Collation | `CollationProvider` | `collation(c)` |
| CDC output sink | `CdcOutputProvider` | `cdc_output(c)` |
| Catalog / virtual schema | `CatalogProvider` | `catalog(c)` |
| Replacement scan | `ReplacementScanProvider` | `replacement_scan(s)` |
| Background job | `BackgroundJobProvider` | `background_job(j)` |

There are **22** registrable surfaces. The list is asserted against the code in
`uni_plugin::surfaces` (`assert_eq!(kinds.len(), 22)`), so it cannot drift
silently.

!!! warning "Removed in 3.0"
    `OperatorProvider`, `StorageBackend`, `PregelProgramProvider` and `Connector`
    — and their registrar methods `operator()`, `pregel()`, `connector()` and
    `storage_backend()` — were **deleted** in 3.0 as dead registrable surfaces.
    Custom per-label storage now goes through `label_storage`.

Registrar methods return `Result<&mut Self, PluginError>`, so registrations chain:

```rust
// A plugin must register within its own namespace — the QName namespace has to
// match the manifest id (here `ai.example.geo`), or registration is rejected.
fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.scalar_fn(QName::new("ai.example.geo", "haversine"), sig_haversine(), Arc::new(Haversine))?
     .scalar_fn(QName::new("ai.example.geo", "bearing"), sig_bearing(), Arc::new(Bearing))?;
    Ok(())
}
```

A scalar implementation is an `Arc<dyn ScalarPluginFn>` that exposes its `FnSignature` and an
invoke method over Arrow batches. See the built-in `identity` scalar in
`crates/uni-plugin-builtin/src/scalar_fns/mod.rs` for the full shape, including `FnSignature`
(`ArgType` / `NullHandling`) and error reporting via `FnError`.

!!! note "Window functions"
    `WindowPluginFn` is implementable today, but no built-in window function ships as a reference
    yet — model it on the scalar/aggregate built-ins.

## Built-in plugins are the reference

Every built-in in uni-db is itself a native plugin, so the source is the canonical worked example
for each non-trivial surface:

- **Index kind** — `crates/uni-plugin-builtin/src/index_vector.rs` (the exact-KNN vector index;
  `IndexKindProvider` + the build / handle traits).
- **CRDTs** — `crates/uni-plugin-builtin/src/crdts.rs` (`lww-register`, `or-set`, `g-counter`,
  `mv-register`, `rga`; `CrdtKindProvider` + `CrdtState`).
- **Label storage** — `crates/uni-plugin-builtin/src/storage.rs` (`MemoryStorage`, an in-memory
  `Storage` implementation used by the conformance suite; registered per label with
  `PluginRegistrar::label_storage`). Lance storage is native, not a plugin registration.
- **Scalars** — `crates/uni-plugin-builtin/src/scalar_fns/mod.rs`.

## Capabilities and trust

A native plugin still declares capabilities in its manifest. `add_plugin` takes no host grant set:
the registrar admits exactly the manifest's declared capabilities, so a plugin that didn't declare
`Crdt` cannot call `crdt_kind`, even in-process. (There is no `declared ∩ granted` step on this
path; trust comes from the signature policy and from compiling the code in.) Host-import capabilities
(`Network`, `Filesystem`, `HostQuery`, `Kms`, `Secret`, `Config`) gate the host services a plugin
may call. See [Trust & Capabilities](trust-and-capabilities.md) for the signature policy and the
declared-∩-granted model, and [Reference](reference.md) for the full capability list.

## See also

- [Concepts](concepts.md) — the plugin / manifest / registrar / registry model.
- [Authoring (sandboxed loaders)](authoring.md) — WASM Component Model and Extism.
- [Trust & Capabilities](trust-and-capabilities.md) — signatures, trust roots, capability intersection.
- [Reference](reference.md) — load APIs, capability names, and resource quotas.
