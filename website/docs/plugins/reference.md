# Plugins API Reference

This page is the precise API surface for loading plugins into a Uni instance.
For the conceptual model see [Concepts](concepts.md); for the permission model
see [Trust & Capabilities](trust-and-capabilities.md); for per-runtime loading
guides see [Loaders](loaders/index.md).

The Python load methods are instance-scoped: a plugin loaded through `Uni` /
`AsyncUni` is visible to every session on that instance until the instance is
dropped.

## Python API

Both the sync `Uni` and async `AsyncUni` expose the same two load methods. They
are gated behind the cargo features `wasm-plugins` (for `load_wasm_component`)
and `extism-plugins` (for `load_wasm_extism`). The published wheels (`uni-db`
and its variants) are built without them; build the bindings from source with
`maturin develop --features wasm-plugins,extism-plugins` (in `bindings/uni-db`)
to get them. If a method is absent at runtime, the wheel was built without the
corresponding feature.

`grants` is an optional list of capability/grant variant names (see
[Capability names](#capability-names)). When omitted it defaults to the surface
grants `ScalarFn`, `AggregateFn`, `Procedure`. The same list drives both the
**surface-registration gate** (which kinds the plugin may register) and the
**host-fn grant set** (which capability-gated host imports are linked in).

=== "Sync (Uni)"

    ```python
    def load_wasm_component(
        self,
        wasm_bytes: bytes,
        grants: list[str] | None = None,
    ) -> dict: ...

    def load_wasm_extism(
        self,
        wasm_bytes: bytes,
        grants: list[str] | None = None,
    ) -> dict: ...
    ```

    ```python
    from uni_db import Uni

    db = Uni.open("graph.uni")
    outcome = db.load_wasm_component(
        wasm_bytes,
        grants=["ScalarFn", "AggregateFn"],
    )
    print(outcome["plugin_id"], outcome["scalars_registered"])
    ```

=== "Async (AsyncUni)"

    ```python
    async def load_wasm_component(
        self,
        wasm_bytes: bytes,
        grants: list[str] | None = None,
    ) -> dict: ...

    async def load_wasm_extism(
        self,
        wasm_bytes: bytes,
        grants: list[str] | None = None,
    ) -> dict: ...
    ```

    The async variants return an awaitable.

    ```python
    from uni_db import AsyncUni

    db = await AsyncUni.open("graph.uni")
    async with db:
        outcome = await db.load_wasm_component(
            wasm_bytes,
            grants=["ScalarFn"],
        )
    ```

### Returned dict

Both methods return a `dict` with the following keys:

| Key | Type | Meaning |
| --- | --- | --- |
| `plugin_id` | `str` | Reverse-DNS plugin id read from the manifest. |
| `version` | `str` | Plugin version from the manifest. |
| `scalars_registered` | `list[str]` | Qnames registered as Cypher scalar functions. |
| `aggregates_registered` | `list[str]` | Qnames registered as Cypher aggregate functions. |
| `procedures_registered` | `list[str]` | Qnames registered as Cypher procedures. |
| `algorithms_registered` | `list[str]` | Qnames registered as graph algorithms. |
| `effective_capabilities` | `list[str]` | Capabilities granted (manifest declared ∩ host grants). |
| `denied_capabilities` | `list[str]` | Capabilities the plugin declared but the host did not grant. |

### Rhai and Python (PyO3) loads

The Rhai and PyO3 loaders are also Python-callable, but on different objects and
with different signatures from the two WASM methods above:

| Method | On | Async? | Signature | Scope |
| --- | --- | --- | --- | --- |
| `load_rhai_plugin` | `Uni` / `AsyncUni` | both | `(script: str, grants: list[str] \| None = None) -> dict` | instance |
| `load_python_plugin` | `Uni` / `AsyncUni` | both | `(module_src: str, module_name: str, grants: list[str] \| None = None) -> dict` | instance |
| `load_python_plugin` | `Session` / `AsyncSession` (from `db.session()`) | both | `(module_src: str, module_name: str, grants: list[str] \| None = None) -> dict` | session |

`Session` also exposes the decorator surface — `@session.scalar_fn(...)` /
`@session.aggregate_fn(...)` / `@session.procedure(...)` plus
`session.finalize_plugin(plugin_id, version=None, grants=None) -> dict` — as an
alternative to loading from a source string. All of these return a dict with
`plugin_id`, `version`, `scalars_registered`, `aggregates_registered`,
`procedures_registered`, `algorithms_registered`, and `denied_capabilities` (note: no
`effective_capabilities` key, unlike the two WASM methods). See
[Rhai](loaders/rhai.md) and [PyO3](loaders/pyo3.md) for usage.

## Rust host API

The Rust host owns the loaders explicitly: you construct a `WasmLoader` /
`ExtismLoader`, pass the plugin bytes plus two capability sets, and receive a
`LoadOutcome`. `host_grants` gates which host fns are linked into the plugin's
import table; `registrar_caps` gates which registration surfaces the plugin may
use.

```rust
#[cfg(feature = "wasm-plugins")]
pub fn load_wasm_component(
    &self,
    loader: &uni_plugin_wasm::WasmLoader,
    bytes: &[u8],
    host_grants: &uni_plugin::CapabilitySet,
    registrar_caps: &uni_plugin::CapabilitySet,
) -> Result<uni_plugin_wasm::loader::LoadOutcome>;

#[cfg(feature = "extism-plugins")]
pub fn load_wasm_extism(
    &self,
    loader: &uni_plugin_extism::ExtismLoader,
    bytes: &[u8],
    host_grants: &uni_plugin::CapabilitySet,
    registrar_caps: &uni_plugin::CapabilitySet,
) -> Result<uni_plugin_extism::loader::LoadOutcome>;
```

For typed, in-process plugins use `add_plugin`:

```rust
pub fn add_plugin<P: uni_plugin::Plugin>(&self, plugin: P) -> Result<()>;
```

`add_plugin` is where the host **signature trust policy is enforced today**: the
plugin's manifest is checked against the configured `PluginTrustConfig` before
any registration runs. The default policy (`SignaturePolicy::Disabled`) accepts
everything; `RequireSigned` rejects an unsigned manifest or one signed by an
untrusted key. Compile-time built-in plugins are implicitly trusted.

The trust policy is set at build time:

```rust
pub fn plugin_trust(mut self, cfg: plugin_trust::PluginTrustConfig) -> Self;
```

```rust
let db = Uni::open("graph.uni")
    .plugin_trust(PluginTrustConfig::new(SignaturePolicy::RequireSigned, trust_root))
    .build()
    .await?;
```

See [Trust & Capabilities](trust-and-capabilities.md) for the full trust model.

### LoadOutcome

`uni_plugin_wasm::loader::LoadOutcome` and
`uni_plugin_extism::loader::LoadOutcome` share the same observable shape (the
two differ only in the concrete instance-pool type, which is held internally to
keep adapters alive):

| Field | Type | Meaning |
| --- | --- | --- |
| `plugin_id` | `String` | Reverse-DNS plugin id from the manifest. |
| `version` | `String` | Plugin version from the manifest. |
| `effective_capabilities` | `Vec<String>` | Capabilities granted (declared ∩ host). |
| `denied_capabilities` | `Vec<String>` | Capabilities declared but not granted. |
| `scalars_registered` | `Vec<String>` | Qnames registered as scalar fns. |
| `aggregates_registered` | `Vec<String>` | Qnames registered as aggregate fns. |
| `procedures_registered` | `Vec<String>` | Qnames registered as procedures. |
| `algorithms_registered` | `Vec<String>` | Qnames registered as graph algorithms. |
| `pool` | `Arc<…InstancePool>` | Instance pool shared by every adapter bound to this plugin; keeps the pool alive while any adapter remains registered. |

The Python load methods project every field except `pool` into the returned
dict.

## Capability names

Grant strings (Python `grants`, parsed by `Capability::parse_grant` into a
Rust `CapabilitySet`) use the variant names of `uni_plugin::Capability`. The
entries in `effective_capabilities` / `denied_capabilities` are each
capability's `Debug` rendering: the bare name for a variant without data
(`ScalarFn`), and the name with its attenuation for one that carries data
(`Network { allow: ["https://api.example.com/**"] }`). The authoritative set follows; see
[Trust & Capabilities](trust-and-capabilities.md) for how declared capabilities,
host grants, and the effective (intersected) set interact.

### Host-import surfaces

These gate capability-backed host functions. Most carry attenuation data in
Rust; as plain grant strings they request the variant.

| Name | Attenuation | Gates |
| --- | --- | --- |
| `Network` | `allow` URI glob patterns | HTTP / TCP egress. |
| `Filesystem` | `read` / `write` path globs | Filesystem read / write. |
| `HostQuery` | `read_only`, `scopes` | Cypher / Locy queries back into the host session. |
| `Kms` | `key_ids` | KMS sign / verify. |
| `Secret` | `ids` | Acquiring named secret handles. |
| `Lock` | `granularity` (`Nodes`/`Edges`/`Both`/`Global`) | Explicit node / edge lock primitives. **Reserved** — the capability is declared and intersected, but the host functions are not yet callable. |
| `Config` | `keys` patterns | Scoped configuration K/V access. |

Grant strings accept either the PascalCase variant name (`HostQuery`) or the
serde kebab-case tag (`host-query`); both fold to the same capability. Bucket-B
names build with a permissive default payload (e.g. `HostQuery` → `read_only`,
`scopes: ["**"]`), which the host then attenuates against its own grant.

### Extension surfaces

These gate the registrar methods — which kinds of extension the plugin may
register.

| Name | Registers |
| --- | --- |
| `ScalarFn` | Cypher scalar functions. |
| `AggregateFn` | Cypher aggregate functions. |
| `WindowFn` | Cypher window functions. |
| `Procedure` | Cypher procedures (read-only). |
| `ProcedureWrites` | Procedures that may mutate the graph. |
| `ProcedureSchema` | Procedures that may issue DDL. |
| `ProcedureDbms` | Administrative procedures. |
| `LocyAggregate` | Locy aggregate functions. |
| `LocyPredicate` | Locy predicates (including neural). |
| `LocyGenerator` | Locy value generators. |
| `Operator` | Physical operators / optimizer rules. |
| `Index` | Index kinds. |
| `Storage` | Storage backends by URI scheme. |
| `Algorithm` | Graph algorithms. |
| `GraphCompute` | GraphCompute coarse kernels (the handle-table runtime that guest-authored graph algorithms drive) — see [Graph Algorithms](graph-algorithms.md). |
| `Crdt` | CRDT kinds. |
| `Hook` | Session / query lifecycle hooks. |
| `Trigger` | Fine-grained mutation triggers. |
| `Type` | Logical (Arrow extension) types. |
| `Collation` | Collations (sort orders). |
| `PluginStorage` | Per-plugin scoped K/V store. **Reserved** — declared and intersected, host functions not yet callable. |

The Python load methods default to `ScalarFn`, `AggregateFn`, `Procedure` when
`grants` is omitted. A guest-authored graph algorithm needs the triple
`Algorithm` (register the algorithm), `GraphCompute` (drive the kernels), and
`HostQuery` (project the graph) — see
[Graph Algorithms](graph-algorithms.md) for the kernel catalogue and a worked example.

### Capability slices

Separately from grants, an algorithm declares the **kernel slices** it needs, and
the host checks them at registration. A slice is a versioned kernel family, so a
guest written against a newer host fails to load with a clear typed error
(`0x86A`) instead of trapping later on an unknown kernel.

| Slice | Provides |
| --- | --- |
| `graph-compute@1` | The read-only coarse kernels over a projected graph: traversal, SpMV, set and edge-mask operations, sampling, similarity, emit. |
| `graph-arena@1` | Mutable session-local structure a guest **grows** during a call — search trees, residual networks, agent populations — plus `arena_freeze` to turn one into an ordinary graph. |

Declare them on your `AlgorithmSignature`:

```rust
slices: vec![
    SliceReq { slice: "graph-compute".into(), version: 1 },
    SliceReq { slice: "graph-arena".into(),   version: 1 },
],
```

Omitting a slice you use is not an error today — the kernels are gated by the
`GraphCompute` grant, not by the declaration. Declaring them is what buys you the
load-time compatibility check, so declare what you use.

### Algorithm argument typing

`AlgorithmSignature.args` declares the arguments the algorithm expects. The
declared list is **type- and arity-checked at call time** (previously it was
inert metadata). The arg-type token vocabulary:

| Token | Accepts |
| --- | --- |
| `int` / `float` / `string` / `bool` | The corresponding primitive scalar. |
| `value` / `cypherValue` | A scalar **or** an array — e.g. a variable-length seed set through one declared parameter. |
| `list` / `array` | An array (required). |

The trailing projection-config object (`{nodeLabels, edgeTypes, projectAll}`) is
an implicit optional last argument and does **not** count against the declared
arity. Note that an unscoped GraphCompute projection now errors — pass
`projectAll: true` for the whole graph, or name `nodeLabels` / `edgeTypes`. See
[Graph Algorithms → Arguments and projecting the graph](graph-algorithms.md#arguments-and-projecting-the-graph)
for the full contract and the worked `CALL` examples.

### Not guest-grantable (internal / first-party)

These capabilities exist but are **never** grantable from a bare grant string —
`Capability::parse_grant` rejects them, and only first-party host code may
construct them directly. Requesting one is an error on the strict path (the
Rhai loader) and is dropped on the lenient paths (the WASM and Extism methods,
the Python plugin loaders, the decorator surface, and the CLI, which warns).

| Name | Purpose |
| --- | --- |
| `Auth` | Authentication providers. |
| `Authz` | Authorization policies. |
| `Cdc` | CDC output sinks. |
| `Catalog` | Catalogs / virtual schemas. |
| `PluginDeclare` | Authority to call `uni.plugin.declare*` meta-procedures. |

### Resource quotas

Quota variants carry a numeric bound.

Quotas are **manifest-only** — they carry a value and cannot be requested as a
bare grant string; `Capability::parse_grant` rejects a quota name with guidance
to declare it in the plugin manifest's `capabilities:` list instead.

| Name | Bound | Limits |
| --- | --- | --- |
| `MemoryBytes` | `u64` | Max wasm linear memory per instance. |
| `FuelPerCall` | `u64` | Max wasmtime fuel per call. |
| `WallClockMillisPerCall` | `u64` | Max wall-clock milliseconds per call. |
| `ConcurrentInstances` | `u32` | Max concurrent instances in the wasm pool. |
| `TotalMemoryBytes` | `u64` | Max total memory across all instances. |
| `MaxResultRows` | `u64` | Cap on rows yielded by a procedure. |
| `GraphComputeWork` | `u64` | Max GraphCompute work budget (kernel operations) per algorithm call. |
| `GraphComputeArenaBytes` | `u64` | Max GraphCompute handle-arena bytes per algorithm call. |
| `BackgroundJob` | `max_concurrent: u32` | Background / scheduled jobs; a registration gate that carries a concurrency quota. |

Today `FuelPerCall` and `MemoryBytes` are enforced (Rhai from the granted
capability; the WASM Component Model and Extism loaders from their manifest
fields). The remaining quota variants are recognized but not yet enforced.

> **Contract for adding a capability.** `Capability::grant_name` is an exhaustive
> match with no wildcard arm, and the `every_variant_classified_exactly_once`
> test in `uni-plugin` asserts every variant is classified as grantable, quota,
> or internal. A new `Capability` variant therefore fails to compile / fails the
> test until it is classified here — this is what keeps the grant surface (and
> this table) from drifting.

## See also

- [Concepts](concepts.md) — the plugin model and lifecycle.
- [Trust & Capabilities](trust-and-capabilities.md) — the permission and trust model.
- [Loaders](loaders/index.md) — per-runtime loading guides.
- [Authoring](authoring.md) — writing your own plugin.
- Generated API docs: [Python API](../api/python/index.md), [Rust API](../api/rust/index.md).
