# WASM Component Model

The Component Model loader runs sandboxed plugins on [wasmtime](https://wasmtime.dev/)
against **typed WIT worlds**. It is the most strongly isolated of the
plugin loaders: a plugin is a portable `.wasm` artifact, and its contract
is a compile-checked interface rather than a loose host-function ABI.

## Overview

A Component Model plugin is a WebAssembly component compiled for the
`wasm32-wasip2` target. Instead of the loose host-function ABI used by
[Extism](extism.md), it implements one of a small set of **typed WIT
worlds**. The host generates Rust bindings from the same WIT and calls
the plugin's exports through them, so argument and return shapes are
checked at the ABI boundary rather than discovered at runtime.

Choose the Component Model loader when you want:

- **The strongest sandbox.** Plugins run in a wasmtime instance with no
  ambient host access. Host services are reached only through explicitly
  linked WIT imports: `host-log` and `host-trace-context` are always
  available, and the capability-gated `host-net` (HTTP GET/POST) is linked
  in only when `Network` is granted.
- **A typed contract.** The WIT worlds pin down the exports a plugin
  must provide; binding generators (`wit-bindgen` on the guest side)
  keep guest and host in sync.
- **Portability.** A component is a single self-describing `.wasm` file
  with no host-language dependency.

Prefer [Extism](extism.md) instead when you need the broad ecosystem of
Extism PDKs (Go, JS, C, …) or its host-function model; the two loaders
produce **byte-identical results** for the same plugin logic (see
[Cross-ABI parity](#cross-abi-parity)).

### Capabilities

The loader intersects the capabilities a plugin's manifest declares with the
host's grants and reports the result as `effective_capabilities` /
`denied_capabilities` on the load outcome. The always-available host imports are
`host-log` (plugin-side tracing) and `host-trace-context` (W3C `traceparent`
propagation). The capability-gated `host-net` interface (HTTP GET/POST) is added
to the linker only when `Network` is granted — a plugin importing it without the
grant fails at link time, and the host additionally enforces the granted URL
allow-list and byte/timeout ceilings at call time. `host-graph` and `host-arena`
(the GraphCompute kernels an `algorithm-plugin` drives) are linked only when
`GraphCompute` is granted. (`host-fs` is not yet exposed on the Component Model;
[Extism](extism.md) wires HTTP, KMS, secret and GraphCompute host functions, and
neither ABI has filesystem or host-query functions.) See `examples/example-wasm-net/` for a worked
`host-net` consumer.

## The WIT worlds

The canonical worlds live in `crates/uni-plugin-wasm/wit/world.wit`,
package `uni:plugin@0.1.0`. There are four plugin worlds, one per
plugin kind:

- **`scalar-plugin`** — implements one or more Cypher scalar functions.
- **`aggregate-plugin`** — implements aggregate functions, carrying
  opaque state bytes between `agg-new` / `agg-update` / `agg-merge` /
  `agg-evaluate`.
- **`procedure-plugin`** — implements Cypher procedures that yield rows.
- **`algorithm-plugin`** — implements graph algorithms over the imported
  `host-graph` / `host-arena` GraphCompute interfaces (see
  [Graph Algorithms](../graph-algorithms.md)).

Every world shares the same control surface and wire convention:

- `manifest` returns the plugin's canonical JSON manifest (id, version,
  declared capabilities, optional resource limits).
- `register` returns a JSON registration manifest enumerating every
  qname the plugin provides and its wire-level signature.
- The columnar payload crosses the boundary as **Arrow IPC stream
  bytes**, typed in WIT as `list<u8>`. The IPC content itself is opaque
  to WIT.

Here is the `scalar-plugin` world, trimmed to its essentials:

```wit
package uni:plugin@0.1.0;

interface types {
    record fn-error {
        code: u32,
        message: string,
        retryable: bool,
    }
}

// Always-available host import (plugin-side tracing). The
// capability-gated host-net interface (HTTP GET/POST) and the
// always-on host-trace-context interface are also declared in the
// full world.wit; host-net is linked only when Network is granted.
// host-fs is not yet exposed on the Component Model.
interface host-log {
    log: func(level: string, message: string);
}

world scalar-plugin {
    use types.{fn-error};

    import host-log;

    export manifest: func() -> string;
    export register: func() -> string;
    export invoke-scalar: func(qname: string, ipc-bytes: list<u8>)
        -> result<list<u8>, fn-error>;
}
```

The `aggregate-plugin` world replaces `invoke-scalar` with `agg-new` /
`agg-update` / `agg-merge` / `agg-evaluate` (state and values both flow
as `list<u8>` Arrow IPC), and `procedure-plugin` replaces it with
`invoke-procedure: func(qname, args-ipc) -> result<list<u8>, fn-error>`.

## Authoring

The worked example is `examples/example-wasm-geo`, a `scalar-plugin`
that registers `ai.example.geo.haversine` (great-circle distance). It is
a standalone crate (not a workspace member) built for `wasm32-wasip2`.

### Cargo setup

A component plugin is a `cdylib` that depends on `wit-bindgen` plus a
`no-default-features` Arrow build (only the `ipc` feature is needed):

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.51"
arrow = { version = "57", default-features = false, features = ["ipc"] }
arrow-array = "57"
arrow-schema = "57"
```

### Generate the bindings and implement the exports

`wit_bindgen::generate!` reads the WIT world and emits a `Guest` trait
to implement plus an `export!` macro to register your type. Point it at
the world you are implementing and the `wit` directory:

```rust
wit_bindgen::generate!({
    world: "scalar-plugin",
    path: "wit",
});

struct GeoPlugin;

impl Guest for GeoPlugin {
    fn manifest() -> String {
        r#"{
            "id": "ai.example.geo",
            "version": "0.1.0",
            "capabilities": [],
            "determinism": "pure",
            "description": "Great-circle distance via the haversine formula (CM)."
        }"#
            .to_owned()
    }

    fn register() -> String {
        r#"{
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
        }"#
            .to_owned()
    }

    fn invoke_scalar(qname: String, ipc_bytes: Vec<u8>) -> Result<Vec<u8>, FnError> {
        if qname != "ai.example.geo.haversine" {
            return Err(FnError { code: 1, message: format!("unknown qname: {qname}"), retryable: false });
        }
        compute_and_encode(&ipc_bytes).map_err(|e| FnError { code: 2, message: e, retryable: false })
    }
}

export!(GeoPlugin);
```

The `manifest` declares no capabilities (`"capabilities": []`) because
haversine is pure compute. The `register` payload's signature shape
(`kind`/`arrow` primitives, `volatility`, `null_handling`) is exactly
what the host's loader deserializes to build the registered function.

### Decode args, encode results (Arrow IPC)

`invoke_scalar` receives the argument columns as Arrow IPC stream bytes
and must return one batch with one output column, also as Arrow IPC.
Decode with `arrow::ipc::reader::StreamReader` and encode with
`StreamWriter`:

```rust
fn decode_input(bytes: &[u8]) -> Result<RecordBatch, String> {
    let reader = StreamReader::try_new(bytes, None).map_err(|e| format!("reader: {e}"))?;
    reader
        .into_iter()
        .next()
        .ok_or_else(|| "empty IPC stream".to_owned())?
        .map_err(|e| format!("read: {e}"))
}

fn encode_output(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    {
        let mut w = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| format!("writer: {e}"))?;
        w.write(batch).map_err(|e| format!("write: {e}"))?;
        w.finish().map_err(|e| format!("finish: {e}"))?;
    }
    Ok(buf)
}
```

The geo example downcasts the four input columns to `Float64Array`,
computes the haversine distance per row, and packs the results into a
single `Float64Array` column named `distance_km` before encoding. Argument
columns arrive in the order declared in the `register` signature.

### Build

Compile for `wasm32-wasip2`:

```bash
cd examples/example-wasm-geo
cargo build --target wasm32-wasip2 --release
```

This produces `target/wasm32-wasip2/release/example_wasm_geo.wasm` — a
self-describing Component Model binary ready to load.

## Loading

Load the component bytes and pass the capability grants. Grants use the
variant names `ScalarFn` / `AggregateFn` / `Procedure` (surface gates)
and `Filesystem` / `Network` / `HostQuery` / `Kms` / `Secret` (host-fn
gates); they drive **both** which surfaces the plugin may register and
which host functions are linked into it.

=== "Python"

    ```python
    from uni_db import Uni

    db = Uni.temporary()

    with open("example_wasm_geo.wasm", "rb") as f:
        wasm_bytes = f.read()

    outcome = db.load_wasm_component(wasm_bytes, grants=["ScalarFn"])
    # outcome is a dict:
    #   plugin_id              -> "ai.example.geo"
    #   version                -> "0.1.0"
    #   scalars_registered     -> ["ai.example.geo.haversine"]
    #   aggregates_registered  -> []
    #   procedures_registered  -> []
    #   algorithms_registered  -> []
    #   effective_capabilities -> []   (declared ∩ granted)
    #   denied_capabilities    -> []

    # Now callable from Cypher by its registered qname:
    rows = db.session().query(
        "RETURN `ai.example.geo.haversine`(48.8566, 2.3522, 51.5074, -0.1278) AS km"
    )
    ```

    `grants` defaults to `None`, which grants the scalar / aggregate /
    procedure surfaces. On `AsyncUni`, `load_wasm_component` is awaitable
    with the same signature and return shape. The method is gated behind
    the `wasm-plugins` cargo feature, which the published wheels omit; build
    the bindings with `maturin develop --features wasm-plugins` to get it.

=== "Rust"

    ```rust
    use uni_plugin::{Capability, CapabilitySet};
    use uni_plugin_wasm::WasmLoader;

    let loader = WasmLoader::new();
    let bytes = std::fs::read("example_wasm_geo.wasm")?;

    // registrar_caps gates which surfaces may register;
    // host_grants gates which host-fn imports are linked.
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let host_grants = CapabilitySet::new();

    let outcome = db.load_wasm_component(&loader, &bytes, &host_grants, &caps)?;

    assert_eq!(outcome.plugin_id, "ai.example.geo");
    assert_eq!(outcome.version, "0.1.0");
    assert!(outcome
        .scalars_registered
        .iter()
        .any(|q| q == "ai.example.geo.haversine"));
    ```

    `load_wasm_component` returns a `LoadOutcome` carrying `plugin_id`,
    `version`, `scalars_registered`, `aggregates_registered`,
    `procedures_registered`, `algorithms_registered`,
    `effective_capabilities`, and `denied_capabilities`. `effective_capabilities` is the intersection
    of the manifest's declared capabilities with the host grants;
    `denied_capabilities` lists declared-but-ungranted ones for
    diagnostics.

Internally the loader does a two-pass negotiation: it instantiates once
to read the `manifest` export, intersects the declared capabilities with
the host grants, rebuilds the engine with the effective capability set (and
any per-call fuel / memory / timeout limits from the manifest), then reads
the `register` export and installs an adapter for each qname.

## Cross-ABI parity

The Component Model and [Extism](extism.md) loaders share the same Arrow
IPC payload format, so the same plugin logic produces **byte-identical
output** across both ABIs. The geo example exists in both flavours
(`example-wasm-geo` and `example-extism-geo`), and the parity test in
`crates/uni/tests/common/loaders/m6_cross_abi_parity.rs` byte-compares their results for
the same input. This means you can develop against whichever ABI fits
your toolchain and switch later without changing behaviour — only the
sandbox and capability-enforcement model differ.

---

See also:

- [Authoring plugins](../authoring.md) — the shared manifest /
  registration model across loaders.
- [Trust and capabilities](../trust-and-capabilities.md) — how grants,
  trust policy, and capability enforcement fit together.
- [Reference](../reference.md) — full loader and capability reference.
