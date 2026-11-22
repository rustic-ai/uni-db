# Extism Plugins

The Extism loader runs plugins built against the [Extism](https://extism.org/)
host-function ABI — plain WebAssembly modules that exchange bytes with the host
over linear memory. It is one of the two WASM plugin front-ends in uni-db; the
other is the [WASM Component Model loader](wasm-components.md). Both register
scalar functions, aggregates, and procedures into the same plugin registry and
speak the same Arrow IPC wire format, so a plugin's output is byte-identical
regardless of which ABI it was built against.

This page assumes familiarity with the [plugin concepts](../concepts.md) and
[capability model](../trust-and-capabilities.md).

## Overview

An Extism plugin is a `wasm32-unknown-unknown` module that exports a handful of
plain functions. Instead of the typed, WIT-described interface of the Component
Model, the contract is a raw call-by-bytes ABI: the host passes an input buffer,
the plugin returns an output buffer. uni-db layers a small export convention on
top of that ABI — `manifest`, `register`, and one `invoke_<qname>` per
registered function — and uses Arrow IPC as the payload format.

Choose the Extism loader when:

- You want the **simplest possible ABI**. There is no WIT, no component tooling,
  and no `wasm-tools` step — just `cargo build --target wasm32-unknown-unknown`.
- You want the **broadest language toolchain support**. Extism PDKs exist for
  many languages, and the target (`wasm32-unknown-unknown`) is the most widely
  supported WASM target.

Choose the [WASM Component Model loader](wasm-components.md) instead when you
want a typed WIT interface.

### Capability gating at load time

The Extism loader applies a **host-function filter at load time**. It intersects
the plugin's declared capabilities with the host's grants and adds only the host
functions whose `required_capability` is in that effective set to the plugin's
import table. A host function that is always available (no `required_capability`)
is added unconditionally; one gated behind an ungranted capability is left out,
so the plugin's call to it is unresolvable.

## Authoring

The worked example is `examples/example-extism-geo`, a `geo.haversine` scalar
that computes great-circle distance between two lat/lon points. It registers the
qname `ai.example.geo.haversine`.

### Cargo.toml

The crate is a standalone `cdylib` (not a workspace member) built with
`extism-pdk`. Arrow is pulled in with default features off and only `ipc`
enabled — that keeps the wasm small while still providing the
`StreamReader`/`StreamWriter` used for the wire format:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
extism-pdk = "1"
serde_json = "1"
arrow = { version = "57", default-features = false, features = ["ipc"] }
arrow-array = "57"
arrow-schema = "57"

[profile.release]
opt-level = "s"
lto = true
strip = true
codegen-units = 1
```

### Exports

Every Extism plugin exports `#[plugin_fn]` functions. uni-db requires three
kinds of export.

`manifest` returns the canonical plugin manifest as JSON — id, version, declared
capabilities, and determinism class. The geo example declares no capabilities
(it is pure):

```rust
use extism_pdk::*;

#[plugin_fn]
pub fn manifest(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "id": "ai.example.geo",
        "version": "0.1.0",
        "abi-extism": "^1",
        "capabilities": [],
        "determinism": "pure",
        "description": "Great-circle distance via the haversine formula."
    })
    .to_string())
}
```

`register` returns the registration manifest: one entry per surface the plugin
exposes. Here it declares `ai.example.geo.haversine` as a scalar taking four
`float64`s and returning one `float64`:

```rust
#[plugin_fn]
pub fn register(_: ()) -> FnResult<String> {
    Ok(serde_json::json!({
        "entries": [{
            "kind": "scalar",
            "qname": "ai.example.geo.haversine",
            "signature": {
                "args": [
                    {"kind": "primitive", "arrow": "float64"},
                    {"kind": "primitive", "arrow": "float64"},
                    {"kind": "primitive", "arrow": "float64"},
                    {"kind": "primitive", "arrow": "float64"}
                ],
                "returns": {"kind": "primitive", "arrow": "float64"},
                "volatility": "immutable",
                "null_handling": "propagate"
            }
        }]
    })
    .to_string())
}
```

One `invoke_<qname>` export carries the actual computation. The host derives the
export symbol by replacing the dots in the qname with underscores, so
`ai.example.geo.haversine` is dispatched to `invoke_ai_example_geo_haversine`.
The body decodes an Arrow IPC `RecordBatch` from the input bytes, computes, and
encodes the result batch back to Arrow IPC bytes:

```rust
#[plugin_fn]
pub fn invoke_ai_example_geo_haversine(input: Vec<u8>) -> FnResult<Vec<u8>> {
    let batch = decode_input(&input).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    let out_batch =
        compute_haversine_batch(&batch).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    let out_bytes =
        encode_output(&out_batch).map_err(|e| WithReturnCode::new(Error::msg(e), 2))?;
    Ok(out_bytes)
}
```

`decode_input` reads the four `Float64` input columns with an
`arrow::ipc::reader::StreamReader`; `encode_output` writes the single
`distance_km` `Float64` result column with a
`arrow::ipc::writer::StreamWriter`. The host always passes a `RecordBatch` whose
columns match the declared signature and rows match the invocation's row count,
so the plugin can compute the result row-by-row over the batch. See the full
`src/lib.rs` for the column extraction and the haversine math.

!!! note "Aggregates, procedures and algorithms"
    The geo example is a scalar. Aggregate and procedure surfaces are declared
    with `"kind": "aggregate"` / `"kind": "procedure"` entries in the `register`
    manifest. Their exports use the qname with dots replaced by underscores
    (`<q>`): an aggregate exports `agg_<q>_new`, `agg_<q>_update`,
    `agg_<q>_merge` and `agg_<q>_evaluate`; a procedure exports
    `proc_<q>_invoke`; a graph algorithm exports `algo_<q>_invoke` (see
    [Graph Algorithms](../graph-algorithms.md)). The `register` and
    `LoadOutcome` shapes account for all of them.

### Building

```bash
cd examples/example-extism-geo
cargo build --target wasm32-unknown-unknown --release
```

This produces
`target/wasm32-unknown-unknown/release/example_extism_geo.wasm`, the artifact
loaded in the sections below.

## Loading

Loading an Extism plugin reads its `manifest` export, intersects the declared
capabilities with the host grants, instantiates with the effective grant set,
reads the `register` export, and wires each declared surface into the plugin
registry.

=== "Python"

    `load_wasm_extism` is sync on `Uni` and awaitable on `AsyncUni`. It takes
    the raw wasm bytes and an optional list of capability grants, and returns a
    dict describing what was registered. It is compiled behind the
    `extism-plugins` cargo feature, which the published wheels omit; build the
    bindings with `maturin develop --features extism-plugins` to get it.

    ```python
    from uni_db import Uni

    db = Uni.open("graph.uni")
    wasm = open("example_extism_geo.wasm", "rb").read()

    # grants defaults to None (the host's default surface grants).
    outcome = db.load_wasm_extism(wasm)

    print(outcome["plugin_id"])              # "ai.example.geo"
    print(outcome["version"])                # "0.1.0"
    print(outcome["scalars_registered"])     # ["ai.example.geo.haversine"]
    print(outcome["effective_capabilities"]) # []
    print(outcome["denied_capabilities"])    # []
    ```

    The returned dict has the same shape as `load_wasm_component`:
    `plugin_id`, `version`, `scalars_registered`, `aggregates_registered`,
    `procedures_registered`, `algorithms_registered`, `effective_capabilities`,
    and `denied_capabilities`. Pass `grants=["Network", ...]` to grant
    capability-gated host functions; only grants that also appear in the
    plugin's manifest end up in `effective_capabilities`, the rest of the
    manifest's requests land in `denied_capabilities`.

    On `AsyncUni`:

    ```python
    outcome = await db.load_wasm_extism(wasm, grants=None)
    ```

=== "Rust"

    The host API takes an `ExtismLoader`, the wasm bytes, a host-grant
    `CapabilitySet` (which host functions are linked), and a `CapabilitySet`
    gating which surfaces the plugin may register. It
    returns a `LoadOutcome`.

    ```rust
    use uni_plugin::{Capability, CapabilitySet};
    use uni_plugin_extism::ExtismLoader;

    let bytes = std::fs::read("example_extism_geo.wasm")?;

    let loader = ExtismLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let host_grants = CapabilitySet::new(); // no capability-gated host fns

    let outcome = db.load_wasm_extism(&loader, &bytes, &host_grants, &caps)?;

    assert_eq!(outcome.plugin_id, "ai.example.geo");
    assert_eq!(outcome.version, "0.1.0");
    assert!(
        outcome
            .scalars_registered
            .iter()
            .any(|q| q == "ai.example.geo.haversine")
    );
    ```

    `LoadOutcome` carries `plugin_id`, `version`, `effective_capabilities`,
    `denied_capabilities`, `scalars_registered`, `aggregates_registered`,
    `procedures_registered`, and `algorithms_registered`. `effective_capabilities` is the intersection of the
    manifest's declared capabilities with `host_grants`; anything declared but
    not granted appears in `denied_capabilities`.

    Capability-gated host functions must be registered on the loader before
    loading. `uni_plugin_extism::register_default_host_svc(&mut loader)` adds
    `uni_http_get` / `uni_http_post` (`Network`), `uni_kms_sign` /
    `uni_kms_verify` (`Kms`), `uni_secret_acquire` (`Secret`) and
    `uni_graph_call` (`GraphCompute`); back them with `with_http`, `with_kms`,
    `with_secret_store` and `with_graph`; without a backing service a granted
    call fails with a "no … configured" error. The Python method registers
    these functions and a GraphCompute registry, but no HTTP, KMS or secret
    service.

Once loaded, the registered qname is callable from Cypher and Locy exactly like
any other plugin function. The host encodes the call's arguments as an Arrow IPC
`RecordBatch`, invokes the plugin's `invoke_<qname>` export, and decodes the
returned Arrow IPC batch — for the geo example, invoking
`ai.example.geo.haversine` with Paris and London coordinates returns
≈343.557 km.

## WASM Component Model vs Extism

Both loaders register into the same registry and emit byte-identical output, but
they differ in interface shape, build target, and how capabilities are enforced.

| | Extism | [WASM Component Model](wasm-components.md) |
|---|---|---|
| Interface | Raw host-fn ABI (call by bytes) | Typed WIT worlds |
| Build target | `wasm32-unknown-unknown` | `wasm32-wasip2` |
| Tooling | `extism-pdk`, `crate-type = ["cdylib"]` | WIT bindings + component build |
| Capability gating | Load-time host-fn filter (HTTP / KMS / secret / GraphCompute wired) | `declared ∩ granted`, reported (host-net, host-graph / host-arena and host-trace-context wired; host-fs not yet) |
| Wire format | Arrow IPC | Arrow IPC |
| Output | Byte-identical across both ABIs | Byte-identical across both ABIs |

The byte-identical guarantee is verified by the cross-ABI parity test
(`crates/uni/tests/common/loaders/m6_cross_abi_parity.rs`): the same logical function built as
an Extism plugin and as a Component produces the same Arrow IPC output for the
same input.

In short, reach for Extism when you want the simplest ABI and the widest
language support, and the Component Model when you want a typed interface with
link-time capability enforcement.

## See also

- [Authoring guide](../authoring.md) — full manifest and registration reference.
- [Trust and capabilities](../trust-and-capabilities.md) — the grant model and
  how `effective`/`denied` capabilities are computed.
- [Plugin reference](../reference.md) — the complete API surface.
- [WASM Component Model loader](wasm-components.md) — the typed-interface sibling.
