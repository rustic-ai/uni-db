# Authoring a Plugin

This guide builds a plugin from scratch — `geo.haversine`, a scalar function
that computes the great-circle distance between two `(lat, lon)` points — and
loads it into a running database where it becomes callable from Cypher.

`geo.haversine` is the example used throughout the plugin docs because it is
deliberately small (four `f64` inputs, one `f64` output, pure math) yet
exercises the full plugin contract: a manifest, a registration, an
Arrow-IPC invoke path, and a capability declaration. It is implemented
identically across every loader, which is exactly what makes it a good
teaching example — and what lets the repository prove that the loaders agree
bit-for-bit (see [Test for parity](#test-for-parity)).

We show the two WASM paths side by side — the **Component Model** (WASM-CM)
and **Extism** — because they are the production sandboxed loaders. If you
want the in-process scripting loaders instead, the same surface and manifest
concepts apply; jump to [Rhai](loaders/rhai.md) or [PyO3](loaders/pyo3.md)
once you've read this page. To author the Rust-only surfaces — optimizer rules,
index kinds, label storage, CRDTs, hooks, triggers, and more — see
[Authoring native (Rust) plugins](authoring-rust.md).

The worked sources for this guide live in the repository:

- `examples/example-wasm-geo/` — the Component Model plugin.
- `examples/example-extism-geo/` — the Extism plugin.

---

## Pick a surface

A plugin registers one or more **surfaces** — the kinds of extension point it
plugs into. Across all four non-Rust loaders (WASM-CM, Extism, Rhai, PyO3),
v1 covers three surfaces, plus guest-authored graph algorithms (covered in
[Graph Algorithms](graph-algorithms.md)):

| Surface | What it is | Capability gate |
| --- | --- | --- |
| **Scalar** | A Cypher function — N input columns in, one output column out, row-aligned. | `ScalarFn` |
| **Aggregate** | A Cypher aggregate — folds many rows into one value via `new` / `update` / `merge` / `evaluate`. | `AggregateFn` |
| **Procedure** | A Cypher procedure called with `CALL` — a 1-row argument batch in, zero or more yield rows out. | `Procedure` (and `ProcedureWrites` / `ProcedureSchema` / `ProcedureDbms` for the privileged variants) |

`geo.haversine` is a **scalar**: every input row maps to exactly one output
row, the output depends only on that row's inputs, and there's no state to
carry. When you're choosing, the rule of thumb is:

- One row in, one row out, no memory → **scalar**.
- Many rows collapse to one (a running total, a custom percentile) →
  **aggregate**.
- You need to `CALL` it as a statement, possibly yielding several rows, or it
  has side effects → **procedure**.

The full catalogue of surfaces the framework defines (label storage,
index kinds, CRDTs, hooks, algorithms, and more) — including which are
reachable from native Rust plugins only — is on the
[Concepts](concepts.md) page. This guide builds a scalar; the manifest and
build mechanics are the same for the other two.

---

## Declare the manifest

Before any code runs, a plugin **declares** three things to the host:

1. **Identity** — a reverse-DNS `plugin_id` (here `ai.example.geo`) and a
   semantic `version`.
2. **The surfaces it registers** — each as a qualified name (`QName`) plus a
   wire-level signature.
3. **The capabilities it wants** — the permissions it needs to do its job.

The host reads two JSON control surfaces from the plugin at load time: the
**manifest** (identity + capabilities) and the **registration** (the
`QName`s and signatures). Both are plain JSON strings returned by exported
functions, so a plugin in any language can produce them.

### Identity and capabilities

The sandboxed loaders parse a loader-specific manifest — `ComponentManifest`
(`crates/uni-plugin-wasm/src/loader.rs`) for the Component Model and
`ExtismPluginManifest` (`crates/uni-plugin-extism/src/loader.rs`) for Extism —
both of which carry the `description` field shown below. These mirror, but are
distinct from, the in-process `PluginManifest` in
`crates/uni-plugin/src/manifest.rs` (which has no `description`).
For the geo plugin:

```json
{
    "id": "ai.example.geo",
    "version": "0.1.0",
    "capabilities": [],
    "determinism": "pure",
    "description": "Great-circle distance via the haversine formula."
}
```

- `id` must be reverse-DNS for third-party plugins (a handful of single-token
  ids like `builtin` are reserved for the framework).
- `determinism: "pure"` tells the planner the result depends only on its
  inputs, so calls are cacheable and hoistable out of loops. Pure math like
  haversine qualifies; anything reading the clock or the network does not.
- `capabilities: []` — haversine needs no host access (no network, no
  filesystem). The *registration* below still requires the `ScalarFn`
  capability to be granted; the empty list here is the set of **host-access**
  capabilities the plugin requests.

### The registered QName and signature

Every registered item is addressed by a `QName` of the form
`namespace.local`, where the namespace is the plugin id. Parsing
`ai.example.geo.haversine` splits it into namespace `ai.example.geo` and local
`haversine` (see `crates/uni-plugin/src/qname.rs`). The registration manifest
enumerates each surface:

```json
{
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
}
```

The signature is in Arrow terms: four `float64` arguments
(`lat1, lon1, lat2, lon2`), one `float64` return. `volatility: "immutable"`
is the planner-side echo of `determinism: "pure"`, and
`null_handling: "propagate"` means a null in any argument yields a null
result without the plugin being called.

### Declared vs granted: effective capabilities

A capability the plugin **declares** is only useful if the host **grants** it
at load time. The framework computes:

```
effective = declared ∩ granted
```

The intersection is enforced in `crates/uni-plugin/src/capability.rs`. A
registration attempted without the matching capability in the effective set
is rejected. So loading `geo.haversine` requires the host to grant
`ScalarFn` (the `grants` / capability-set argument to the loader — see
[Load & invoke](#load-invoke)). Anything the plugin asks for but the host
withholds simply isn't in the effective set, and the corresponding host
imports are never linked in. The full grant model — patterns, attenuation,
and the trust pipeline — is covered in
[Trust & Capabilities](trust-and-capabilities.md).

---

## Implement

The implementation is the same three pieces in both ABIs: a `manifest`
export, a `register` export, and an `invoke` path that decodes an Arrow IPC
batch, computes, and re-encodes. The haversine math is byte-for-byte
identical — that's deliberate.

The math, shared verbatim by both plugins:

```rust
const EARTH_RADIUS_KM: f64 = 6371.0;

fn haversine_km(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
    let lat1 = lat1_deg.to_radians();
    let lat2 = lat2_deg.to_radians();
    let dlat = (lat2_deg - lat1_deg).to_radians();
    let dlon = (lon2_deg - lon1_deg).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_KM * c
}
```

The wire shape is also shared: input is an Arrow IPC stream carrying one
`RecordBatch` with four `Float64` columns; output is an Arrow IPC stream with
a single `Float64` column. The `decode_input` / `compute_haversine_batch` /
`encode_output` helpers (StreamReader in, StreamWriter out) are identical
across both example crates — only the *export plumbing* differs.

=== "WASM Component Model"

    The Component Model plugin uses `wit-bindgen` to generate typed bindings
    from a WIT world, then implements the world's `Guest` trait. The world is
    `scalar-plugin`, defined in
    `crates/uni-plugin-wasm/wit/world.wit`:

    ```wit
    world scalar-plugin {
        use types.{fn-error};

        import host-log;

        export manifest: func() -> string;
        export register: func() -> string;
        export invoke-scalar: func(qname: string, ipc-bytes: list<u8>)
            -> result<list<u8>, fn-error>;
    }
    ```

    Copy that world into your crate's `wit/` directory, then generate and
    implement:

    ```rust
    wit_bindgen::generate!({
        world: "scalar-plugin",
        path: "wit",
    });

    struct GeoPlugin;

    impl Guest for GeoPlugin {
        fn manifest() -> String {
            // ... the manifest JSON from "Declare the manifest" ...
        }

        fn register() -> String {
            // ... the registration JSON from "Declare the manifest" ...
        }

        fn invoke_scalar(qname: String, ipc_bytes: Vec<u8>) -> Result<Vec<u8>, FnError> {
            if qname != "ai.example.geo.haversine" {
                return Err(FnError {
                    code: 1,
                    message: format!("unknown qname: {qname}"),
                    retryable: false,
                });
            }
            compute_and_encode(&ipc_bytes).map_err(|e| FnError {
                code: 2,
                message: e,
                retryable: false,
            })
        }
    }

    export!(GeoPlugin);
    ```

    The `invoke-scalar` export takes `(qname, ipc-bytes)` and returns either
    the output IPC bytes or a typed `fn-error`. The `qname` argument lets one
    plugin serve several scalars from a single export by dispatching on it.
    `FnError` is the WIT-generated error type; framework error codes occupy
    `0..=0xFF`, so use codes above that for your own. Full source:
    `examples/example-wasm-geo/src/lib.rs`.

=== "Extism"

    The Extism plugin uses `extism-pdk` and the `#[plugin_fn]` macro. There's
    no WIT — each export is a plain Rust function. The manifest and register
    exports return JSON strings:

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

    #[plugin_fn]
    pub fn register(_: ()) -> FnResult<String> {
        // ... the registration JSON from "Declare the manifest" ...
    }
    ```

    The invoke function is named by convention, **not** by a `qname` argument:
    the host derives the export symbol from the qname by replacing dots with
    underscores and prefixing `invoke_`. For `ai.example.geo.haversine` that
    symbol is `invoke_ai_example_geo_haversine`:

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

    `WithReturnCode` attaches the plugin-defined error code to the host
    boundary, the Extism analogue of the Component Model's `FnError.code`.
    Full source: `examples/example-extism-geo/src/lib.rs`.

Both example crates trim Arrow to just the IPC feature
(`arrow = { version = "57", default-features = false, features = ["ipc"] }`)
so the wasm stays small — the default features pull in chrono / csv / json,
none of which a scalar needs.

---

## Build

Both plugins are standalone crates (each declares an empty `[workspace]` so it
opts out of the host workspace) with `crate-type = ["cdylib"]`. They differ
only in target triple.

=== "WASM Component Model"

    Build to `wasm32-wasip2`:

    ```bash
    cd examples/example-wasm-geo
    cargo build --target wasm32-wasip2 --release
    ```

    This produces `target/wasm32-wasip2/release/example_wasm_geo.wasm`. The
    `Cargo.toml` essentials:

    ```toml
    [lib]
    crate-type = ["cdylib"]

    [dependencies]
    wit-bindgen = "0.51"
    arrow = { version = "57", default-features = false, features = ["ipc"] }
    arrow-array = "57"
    arrow-schema = "57"

    [profile.release]
    opt-level = "s"
    lto = true
    strip = true
    codegen-units = 1
    ```

    !!! note
        Under the current Rust toolchain the `wasm32-wasip2` build emits a
        Component Model binary directly — no `wit-component` / `wasm-tools`
        post-processing step is required; the host loads the bytes as-is.

=== "Extism"

    Build to `wasm32-unknown-unknown`:

    ```bash
    cd examples/example-extism-geo
    cargo build --target wasm32-unknown-unknown --release
    ```

    This produces
    `target/wasm32-unknown-unknown/release/example_extism_geo.wasm`. The
    `Cargo.toml` essentials:

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

After `lto = true`, `opt-level = "s"`, and `strip = true`, the geo artifact is
on the order of ~1 MB.

---

## Load & invoke

Loading registers the surface immediately — there is no separate "activate"
step, and loading is instance-scoped, so the scalar is visible to every
session on that handle.

=== "Python"

    The Component Model loader is `load_wasm_component` (the `wasm-plugins`
    feature) and Extism plugins load via `load_wasm_extism` (the
    `extism-plugins` feature). The published wheels omit both; build the
    bindings with `maturin develop --features wasm-plugins,extism-plugins`. Pass `grants` for the capabilities
    the host is willing to grant — `["ScalarFn"]` is enough here.

    ```python
    import uni_db

    db = uni_db.Uni.open("./geo.db")

    # Component Model:
    with open(
        "examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm",
        "rb",
    ) as f:
        outcome = db.load_wasm_component(f.read(), grants=["ScalarFn"])

    # ...or Extism:
    # with open(".../example_extism_geo.wasm", "rb") as f:
    #     outcome = db.load_wasm_extism(f.read(), grants=["ScalarFn"])

    print(outcome["plugin_id"], outcome["version"])      # ai.example.geo 0.1.0
    print(outcome["scalars_registered"])                 # ['ai.example.geo.haversine']
    ```

    On `AsyncUni`, both methods are awaitable (wasmtime instantiation runs off
    the event loop).

=== "Rust host API"

    The Rust path takes an explicit loader and capability set — this is
    exactly what the Python methods wrap:

    ```rust
    use uni_db::Uni;
    use uni_plugin::{Capability, CapabilitySet};
    use uni_plugin_wasm::WasmLoader;

    let db = Uni::open("./geo.db").build().await?;
    let bytes = std::fs::read(
        "examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm",
    )?;

    let loader = WasmLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    // host grants, then registrar capabilities; the Python methods pass the
    // same set as both.
    let outcome = db.load_wasm_component(&loader, &bytes, &caps, &caps)?;
    assert_eq!(outcome.plugin_id, "ai.example.geo");
    ```

    For Extism, use `uni_plugin_extism::ExtismLoader` with
    `db.load_wasm_extism(...)`.

Once loaded, the scalar is callable by its `QName` from Cypher — same query
regardless of which ABI delivered it:

```cypher
RETURN `ai.example.geo.haversine`(48.8566, 2.3522, 51.5074, -0.1278) AS km
```

For the full loader walkthrough and the `LoadOutcome` field reference, see the
[Quickstart](quickstart.md) and [Reference](reference.md).

---

## Test for parity

The plugin framework's strongest correctness guarantee is **cross-loader
parity**: the same logical plugin, loaded through different ABIs, returns the
same answer.

- **WASM-CM vs Extism — byte-identical.** Because both plugins ship the same
  haversine math and the same Arrow-IPC wire format, the host's
  `ColumnarValue → IPC → wasm → IPC → ColumnarValue` round-trip is identical
  regardless of which ABI delivered the batch. The repository's `m6` parity
  test (`crates/uni/tests/common/loaders/m6_cross_abi_parity.rs`) loads both
  geo artifacts, invokes `ai.example.geo.haversine` over five test rows, and
  asserts the output `f64`s are **bit-for-bit equal** (`to_bits()`), not
  merely close.
- **Rhai / PyO3 — within ≤4 ULP.** The in-process scripting loaders go
  through a host language whose floating-point evaluation differs slightly
  from the compiled Rust path, so the `m7` (Rhai) and `m8` (PyO3) parity tests
  assert agreement to within 4 units in the last place rather than exact bytes.

When you author your own plugin, hold it to the same bar: keep a **native Rust
reference** implementation of the math and test your plugin's output against
it. The `m6` test is the pattern to copy — load the plugin, build an input
batch, invoke by `QName`, and compare against the reference, byte-equal for
the compiled WASM paths or within a small ULP tolerance for scripted loaders.
The geo example also ships unit tests on the math itself (Paris→London
≈ 343.557 km, antipodes ≈ half-circumference) that you can mirror for your
own formula.

---

## Next steps

- The two WASM loaders in depth: [WASM Components](loaders/wasm-components.md)
  and [Extism](loaders/extism.md).
- The in-process scripting loaders: [Rhai](loaders/rhai.md) and
  [PyO3](loaders/pyo3.md).
- The permission model behind `grants` and the trust pipeline:
  [Trust & Capabilities](trust-and-capabilities.md).
- The full surface catalogue and the manifest/registration data model:
  [Concepts](concepts.md) and [Reference](reference.md).
