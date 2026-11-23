# Plugins Quickstart

Load a prebuilt plugin and call it from a query in under five minutes — no
plugin authoring required.

This page uses the bundled `ai.example.geo` plugin, which registers a single
scalar function: `ai.example.geo.haversine`, the great-circle distance (in
kilometres) between two latitude/longitude points. You will read the
prebuilt `.wasm` artifact, hand its bytes to `load_wasm_component`, inspect
what was registered, and then call the function from Cypher.

If you want to understand *how* plugins fit together first, start with
[Concepts](concepts.md). To build your own, see [Authoring](authoring.md).

---

## Prerequisites

- **A `uni-db` build with the WASM loaders.** The published `uni-db` wheels
  do not include them: `load_wasm_component` needs the `wasm-plugins` feature
  and `load_wasm_extism` the `extism-plugins` feature. Build the Python
  bindings from source with both enabled:

    ```bash
    cd bindings/uni-db
    maturin develop --features wasm-plugins,extism-plugins
    ```

    In Rust, enable the same features on the `uni-db` crate. If
    `load_wasm_component` is missing from `Uni` at runtime, the build lacks the
    feature. (The [Rhai](loaders/rhai.md) and [PyO3](loaders/pyo3.md) loaders
    are in the default wheel.)

- **The prebuilt `.wasm` fixture must exist on disk.** The example plugin is
  *not* checked in as a binary; it is compiled on demand. Build it once from
  the repo root:

    ```bash
    rustup target add wasm32-wasip2          # one-time
    ./scripts/build-wasm-fixtures.sh
    ```

    This produces the Component Model artifact at:

    ```
    examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm
    ```

    The `wasm32-wasip2` target emits a Component Model binary directly — no
    `wasm-tools` post-processing step is needed. For the full build story (and
    how to compile your own plugin), see
    [WASM Components](loaders/wasm-components.md) and [Authoring](authoring.md).

!!! note
    If the fixture is missing, the e2e tests skip rather than fail. Make sure
    the path above exists before running the snippets below.

---

## Load it

Read the component bytes and pass them to `load_wasm_component`. The `grants`
argument is a list of capability **name** strings; the geo plugin only
registers a scalar function, so `["ScalarFn"]` is sufficient. (Omitting
`grants` defaults to scalar / aggregate / procedure.)

=== "Python (sync)"

    ```python
    import uni_db

    db = uni_db.Uni.open("./geo.db")

    wasm_path = (
        "examples/example-wasm-geo/target/wasm32-wasip2/"
        "release/example_wasm_geo.wasm"
    )
    with open(wasm_path, "rb") as f:
        outcome = db.load_wasm_component(f.read(), grants=["ScalarFn"])

    print(outcome["plugin_id"], outcome["version"])
    print(outcome["scalars_registered"])
    ```

=== "Python (async)"

    ```python
    import uni_db

    db = await uni_db.AsyncUni.temporary()

    wasm_path = (
        "examples/example-wasm-geo/target/wasm32-wasip2/"
        "release/example_wasm_geo.wasm"
    )
    with open(wasm_path, "rb") as f:
        # Awaited: wasmtime instantiation runs off the event loop.
        outcome = await db.load_wasm_component(f.read(), grants=["ScalarFn"])

    print(outcome["plugin_id"], outcome["version"])
    print(outcome["scalars_registered"])
    ```

=== "Rust"

    ```rust
    use uni_db::Uni;
    use uni_plugin::{Capability, CapabilitySet};
    use uni_plugin_wasm::WasmLoader;

    let db = Uni::open("./geo.db").build().await?;

    let bytes = std::fs::read(
        "examples/example-wasm-geo/target/wasm32-wasip2/\
         release/example_wasm_geo.wasm",
    )?;

    // The Rust path takes an explicit loader and two capability sets: the
    // host grants (host functions linked in) and the registrar capabilities
    // (surfaces the plugin may register). The Python methods pass the same
    // set as both.
    let loader = WasmLoader::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    let outcome = db.load_wasm_component(&loader, &bytes, &caps, &caps)?;

    println!("{} {}", outcome.plugin_id, outcome.version);
    println!("{:?}", outcome.scalars_registered);
    ```

The function is registered the moment the call returns — there is no separate
"activate" step. Loading is instance-scoped: the scalar is available to every
session and query on this `Uni` handle.

---

## Inspect the LoadOutcome

Both Python methods return a `dict` describing exactly what the loader did.
For the geo plugin you'll see:

```python
{
    "plugin_id": "ai.example.geo",
    "version": "0.1.0",
    "scalars_registered": ["ai.example.geo.haversine"],
    "aggregates_registered": [],
    "procedures_registered": [],
    "algorithms_registered": [],
    "effective_capabilities": [],
    "denied_capabilities": [],
}
```

| Key | Meaning |
| --- | --- |
| `plugin_id` | The plugin's self-declared identity (from its manifest). |
| `version` | The plugin's declared semantic version. |
| `scalars_registered` | Fully-qualified names of scalar functions now callable. |
| `aggregates_registered` | Aggregate function QNames registered. |
| `procedures_registered` | Procedure QNames registered. |
| `algorithms_registered` | Graph algorithm QNames registered. |
| `effective_capabilities` | Granted ∩ declared — what the plugin actually got. |
| `denied_capabilities` | Declared-but-not-granted capabilities (diagnostics). |

`effective_capabilities` is the intersection of what you granted and what the
plugin's manifest *declares*. The geo plugin declares no capabilities, so its
effective set is empty even though you passed `grants=["ScalarFn"]` — that grant
is a *surface* grant that authorizes registering the scalar, distinct from the
host-service capabilities a manifest can declare. If you withhold a capability a
plugin does declare, it shows up under `denied_capabilities` instead — a quick
way to spot a misconfigured grant set. (A plugin that also imports the host
functions behind that capability, such as `example-wasm-net` with `Network`,
fails to load instead: the import is not linked.) In Rust, the same fields are typed members
of `uni_plugin_wasm::loader::LoadOutcome`.

See [Reference](reference.md) for the complete field table and the full list
of capability names. The grant/deny model is covered in
[Trust & Capabilities](trust-and-capabilities.md).

---

## Call it from Cypher

A registered scalar is callable by its fully-qualified name anywhere a Cypher
expression is allowed:

=== "Python (sync)"

    ```python
    # Paris (48.8566, 2.3522) -> London (51.5074, -0.1278)
    rows = db.session().query(
        "RETURN ai.example.geo.haversine(48.8566, 2.3522, 51.5074, -0.1278) "
        "AS km"
    )
    print(rows[0]["km"])  # ~= 343.556
    ```

=== "Rust"

    ```rust
    let rows = db
        .session()
        .query(
            "RETURN ai.example.geo.haversine(48.8566, 2.3522, 51.5074, -0.1278) \
             AS km",
        )
        .await?;
    // km ~= 343.556
    ```

The four `f64` arguments are `lat1, lon1, lat2, lon2`; the result is the
distance in kilometres (~343.556 km for Paris→London). Like any DataFusion
scalar, it is vectorised — pass column expressions over a `MATCH` and it
evaluates row-by-row across the whole batch.

The name `ai.example.geo.haversine` is a **QName** (qualified name). At plan
time the query engine resolves it against the local plugin registry, so a
registered plugin scalar slots in alongside built-in functions with no special
syntax. See [Concepts](concepts.md) for how QName resolution and the registry
fit into the wider plugin model.

---

## Next steps

- [Concepts](concepts.md) — loaders, registry, QName resolution, and the
  plugin lifecycle.
- [Loaders](loaders/index.md) — the available loaders (WASM Component Model,
  Extism, Rhai, PyO3) and when to use each.
- [Authoring](authoring.md) — write and build your own plugin, including the
  `wasm32-wasip2` toolchain setup.
- [Trust & Capabilities](trust-and-capabilities.md) — the grant model, the
  full capability list, and host trust policy.
