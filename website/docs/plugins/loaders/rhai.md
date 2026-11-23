# Rhai Plugins

The Rhai loader runs [Rhai](https://rhai.rs) scripts as uni-db extensions. Rhai
is a sandboxed, pure-Rust scripting language embedded directly in the host
process, so a plugin is just script source — there is no build toolchain, no
WASM wrapper, and no separate runtime to install.

!!! note "Scope"
    The Rhai loader is loadable from both Rust (`Uni::load_rhai_plugin`) and
    Python (`Uni.load_rhai_plugin(script, grants=...)`, or its awaitable
    `AsyncUni` counterpart). It authors scalar / aggregate / procedure surfaces
    and graph algorithms in v1. See [Scope & limitations](#scope-limitations).

## Overview

Rhai fills the "dynamic *and* sandboxed" quadrant of the [loader
matrix](../concepts.md): you get quick custom scalars, aggregates, and
procedures written as plain script, without compiling a WASM component or
maintaining a C toolchain. The engine builds anywhere uni-db builds.

Choose Rhai when you want to:

- add a small custom scalar or aggregate without a WASM build step;
- ship logic as source the host can review and load at runtime;
- run untrusted or semi-trusted snippets under a sandbox.

The sandbox is a property of the language: Rhai has **no built-in I/O**. Every
effectful operation comes from a host-registered function, and registering one
is opt-in. The loader registers a capability-gated host function (for example
`uni_fs_read`) on a plugin's engine **only when** the matching capability is in
that plugin's effective grant set. A plugin without `Capability::Filesystem`
cannot call the filesystem host fn — Rhai raises a "function not found" error at
parse-resolution time. This *import-absence* posture makes Rhai the
untrusted-script counterpart to the trusted, in-process [PyO3 loader](pyo3.md).

This contrasts with [WASM components](wasm-components.md): Rhai trades the strong
process/memory isolation of WASM for zero build friction and a smaller surface,
relying on the language's lack of ambient I/O plus first-class engine resource
limits (max operations, call depth, string/array/map sizes).

## Authoring

A Rhai plugin is a single script that exports a `uni_manifest()` function
describing what it provides, plus the implementation functions themselves. The
manifest is a Rhai object map; the loader calls `uni_manifest()` once at load
time to discover the declared entries.

The worked example in `examples/example-rhai-geo/geo.rhai` declares one scalar,
`haversine`, that computes the great-circle distance between two
latitude/longitude points in kilometres:

```rhai
fn uni_manifest() {
    #{
        id: "ai.dragonscale.geo",
        version: "0.3.1",
        determinism: "pure",
        scalar_fns: [
            #{ name: "haversine",
               args: ["float", "float", "float", "float"],
               returns: "float" },
        ],
    }
}

const R = 6371.0;  // Earth radius in km

fn haversine(lat1, lon1, lat2, lon2) {
    let rlat1 = lat1.to_radians();
    let rlat2 = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin() ** 2
          + rlat1.cos() * rlat2.cos() * (dlon / 2.0).sin() ** 2;
    global::R * 2.0 * a.sqrt().asin()
}
```

How the script declares its surface:

- **`id`** and **`version`** identify the plugin; `id` becomes the namespace of
  every registered name (the scalar above registers as
  `ai.dragonscale.geo.haversine`).
- **`determinism`** (for example `"pure"`) flags whether the function is
  side-effect-free, which the host carries onto the registered signature.
- **`scalar_fns`** lists scalar entries. Each entry gives the function `name`
  (matching a `fn` in the script), the argument type names, and the `returns`
  type. The loader maps these type names to the registered Arrow signature.

Aggregates and procedures are declared the same way through `aggregate_fns` and
`procedures` arrays. Aggregate entries add a `state` field; procedure entries
declare `yields` columns and a `mode` (`read` / `write` / `schema` / `dbms`).
Each entry's function name must resolve to a `fn` defined in the same script.

## Loading

A Rhai plugin is loaded directly from its script source. The loader reads the
manifest, intersects declared capabilities with your grants, registers each
entry on the plugin registry, and returns a `LoadOutcome` reporting the resolved
plugin id, version, the names that were registered, and any capabilities the
script declared but the host did not grant.

=== "Python"

    The sync `Uni.load_rhai_plugin(script, grants=None)` method takes the Rhai
    script as a string and a list of capability grant names (defaulting to
    `ScalarFn` / `AggregateFn` / `Procedure` when omitted). It returns a dict
    with `plugin_id`, `version`, `scalars_registered`, `aggregates_registered`,
    `procedures_registered`, `algorithms_registered`, and `denied_capabilities`.
    Unknown, quota and internal grant names raise `ValueError`. The plugin is registered
    on the instance, so every session sees it. `AsyncUni` exposes an awaitable
    `load_rhai_plugin` with the same signature and return shape.

    ```python
    from uni_db import Uni

    db = Uni.open("graph.uni")
    with open("geo.rhai") as f:
        outcome = db.load_rhai_plugin(f.read(), grants=["ScalarFn"])

    print(outcome["plugin_id"], outcome["scalars_registered"])
    # then call ai.dragonscale.geo.haversine(...) from Cypher
    ```

=== "Rust"

    The host method `Uni::load_rhai_plugin` takes a constructed `RhaiLoader`, the
    script, and a `CapabilitySet`.

    ```rust
    use uni_db::Uni;
    use uni_plugin::{Capability, CapabilitySet, QName};
    use uni_plugin_rhai::RhaiLoader;

    const SCRIPT: &str = include_str!("../geo.rhai");

    let db = Uni::in_memory().build().await?;
    let loader = RhaiLoader::new();

    // `registrar_caps` is both the registration gate (it must include the
    // extension-surface caps the script declares) and the host-fn grant set.
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let outcome = db.load_rhai_plugin(&loader, SCRIPT, &caps)?;

    println!(
        "loaded `{}` v{} ({} scalar fn(s))",
        outcome.plugin_id.as_str(),
        outcome.version,
        outcome.scalars_registered.len(),
    );

    // The scalar is now resolvable by its qualified name.
    let qn = QName::new("ai.dragonscale.geo", "haversine");
    let entry = db.plugin_registry().scalar_fn(&qn).expect("registered");
    ```

On the Rust path, `registrar_caps` plays a dual role: it must contain the
extension-surface
capability for each declared entry (`Capability::ScalarFn`, `AggregateFn`, or
`Procedure`), and it is the grant set that decides which host functions get
registered on the engine. Capabilities a script declares but the host does not
grant are skipped at registration and surfaced in
`LoadOutcome::denied_capabilities` rather than failing the load.

Rhai-backed functions match the native implementation closely: the cross-loader
parity test confirms the `haversine` scalar agrees with the native computation
to **within 4 ULP** on the canonical inputs (Rhai uses its own math package, so
trig functions differ by at most a few ULP).

## Scope & limitations

!!! note "Sandboxed, v1 surface"
    - **Loadable from Rust and Python.** Use `Uni::load_rhai_plugin` from Rust,
      or `Uni.load_rhai_plugin(script, grants=...)` from Python — `AsyncUni`
      exposes an awaitable counterpart.
    - **Scalar / aggregate / procedure / algorithm.** v1 lets a script author
      scalar functions, aggregate functions, procedures, and graph algorithms
      (see [Graph Algorithms](../graph-algorithms.md)). Other extension surfaces
      are not yet supported.
    - **Sandboxed posture.** Rhai has no ambient I/O; all effects flow through
      capability-gated host functions, and ungranted host fns are simply not
      registered on the engine. This is the untrusted-script posture, distinct
      from the trusted, in-process PyO3 loader. See
      [Trust & capabilities](../trust-and-capabilities.md) for how grants are
      decided and enforced.

## See also

- [PyO3 loader](pyo3.md) — trusted, in-process Python source plugins.
- [WASM components](wasm-components.md) — Component Model plugins with strong
  isolation.
- [Authoring plugins](../authoring.md) — manifests, surfaces, and signatures.
- [Plugin reference](../reference.md) — API and type reference.
