---
status: beta
---

# Plugins

A **plugin** is the unit of extension in Uni. Concretely, it is a type that implements the `Plugin` trait, describes itself with a `PluginManifest`, registers what it provides through a `PluginRegistrar`, and is resolved at call time from a shared `PluginRegistry`. That single registry-backed mechanism is how every extension — a Cypher scalar function, an aggregate, a custom Locy aggregate, a stored procedure, a label-storage provider, an index kind, a CRDT, a graph algorithm, a hook, a trigger — enters the engine.

The framing to keep in mind: **every built-in is itself a plugin.** The vector index is one index-kind provider among many, the five built-in CRDTs are CRDT registrations, the ten Locy aggregates (`MNOR`/`MPROD` and friends) are Locy-aggregate registrations, and the 38 APOC procedures are procedure registrations. Uni dogfoods the same extension path it offers you. The design rule is blunt: if the framework cannot express a built-in, the framework is wrong and gets fixed — not worked around.

This section explains the model and routes you to the right page. If you just want to add a Cypher UDF in five minutes, jump to the [Quickstart](quickstart.md).

---

## The two axes

Extending Uni is a choice along two independent axes.

**Surface — *what* you extend.** The plugin framework defines 22 extension surfaces (`SurfaceKind`): scalar / aggregate / window functions, procedures, Locy aggregates, predicates and generators, optimizer rules, graph algorithms, index kinds, label storage, CRDTs, hooks, logical types, auth and authz providers, triggers, collations, CDC outputs, catalog providers, replacement scans, and background jobs. A plugin declares which surfaces it touches, and the registrar gates each registration against that declaration.

**Loader — *how* you author and sandbox.** The same registry is reached by five loaders: compile-time **Rust** (native, trusted), the **WASM Component Model** (wasmtime + WIT) and **Extism** (host-fn ABI) for sandboxed polyglot code, **PyO3** for in-process Python, and **Rhai** for sandboxed pure-Rust scripting. The execution layer is loader-agnostic — a registered scalar function looks identical to the executor whether it arrived as native Rust or as a sandboxed `.wasm`.

The two axes are orthogonal: you pick a surface (what) and a loader (how) independently — subject to the v1 coverage matrix below.

---

## Surface × loader matrix

Today, only the Rust path can author all 22 surfaces. The four non-Rust loaders author **scalar / aggregate / procedure** and **graph algorithms** in v1; the remaining surfaces are compile-time-Rust-only (see [Status & scope](#status-scope)).

| Loader | Scalar / aggregate / procedure | Graph algorithms | Locy aggregates & predicates | Index / label storage | CRDTs / hooks / triggers / jobs | Types / collations / auth / CDC / catalog / optimizer rules |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Rust** (`uni-plugin-builtin`) | yes | yes | yes | yes | yes | yes |
| **WASM Component Model** | yes | yes | — | — | — | — |
| **Extism** | yes | yes | — | — | — | — |
| **PyO3** (Python) | yes | yes | — | — | — | — |
| **Rhai** | yes | yes | — | — | — | — |

"Scalar / aggregate / procedure" covers the vast majority of user extensions: a Cypher UDF, a custom aggregate, or a stored procedure. Guest-authored graph algorithms drive the GraphCompute kernels (see [Graph Algorithms](graph-algorithms.md)). Reach for the Rust loader when you need a deeper surface — a new index kind, label storage, or a CRDT.

---

## When to reach for plugins

- **Cypher UDFs** — add a domain scalar function (`geo.haversine`, `text.slugify`) and call it inline from Cypher, in any loader.
- **Custom Locy aggregates and predicates** — extend the logic engine with your own fold operators or predicates, dispatched through the same trait path as the built-in `MNOR`/`MPROD`.
- **Polyglot or untrusted third-party code** — ship sandboxed WASM (Component Model or Extism) so foreign code runs under a capability budget, with byte-identical parity to the Rust reference.
- **In-process Python authoring** — register scalars, aggregates, and procedures straight from a notebook with the `@db.scalar_fn` decorator, including vectorized (per-`RecordBatch`) evaluation.
- **Packaging your own built-ins** — bundle a coherent set of extensions as a versioned, manifest-described, optionally signed plugin — exactly how Uni packages the vector index, the CRDTs, and APOC.

---

## Security in one sentence

A plugin's effective permissions are `effective = declared ∩ granted` — the plugin declares the capabilities it wants, the host grants a subset, and the effective set is the intersection; a registration that needs a capability outside that set is rejected, and a sandboxed loader never even links the host imports it was denied.

Note the distinction: **trust** (whether to load a plugin at all — signature verification) is separate from **capabilities** (what a loaded plugin may do). See [Trust & Capabilities](trust-and-capabilities.md) for both.

---

## Section map

| Page | What you'll find |
|---|---|
| [Quickstart](quickstart.md) | Add a working Cypher UDF in a few minutes, end to end. |
| [Concepts](concepts.md) | The `Plugin` / `PluginManifest` / `PluginRegistrar` / `PluginRegistry` model and lifecycle. |
| [Loaders](loaders/index.md) | The five loaders compared, with per-loader authoring guides and the loader matrix. |
| [Authoring](authoring.md) | Writing a plugin against the surface traits, with the `geo.haversine` reference across loaders. |
| [Graph Algorithms](graph-algorithms.md) | Authoring graph algorithms in any loader: the 72-kernel catalogue, the mutable arena, and a worked guest-authored MCTS. |
| [Trust & Capabilities](trust-and-capabilities.md) | The capability model, `effective = declared ∩ granted`, and signing. |
| [Reference](reference.md) | The full surface-trait, manifest, and host-API reference. |

---

## Status & scope

The plugin framework is shipped and test-verified, with an honestly bounded v1 scope.

**Shipped:**

- Scalar, aggregate, procedure and graph-algorithm authoring across **all five loaders** (Rust, Component Model, Extism, PyO3, Rhai), with cross-loader byte-identical (or ≤ 4 ULP) parity tests.
- All 22 extension surfaces authorable from **Rust**, including the built-ins Uni ships on the same path.
- The **trust-policy foundation**: the `declared ∩ granted` capability model, host-configurable grants, and opt-in Ed25519 signature verification on the in-process `add_plugin` path (enforcement defaults to off), and an opt-in artifact hash-pin allowlist checked by all four byte loaders (WASM, Extism, Rhai, PyO3).

**Deferred (Phase-D / pending need):**

- A full **plugin CLI** (`uni plugin {install,list,grant,remove,info,reload,verify}`) — only `uni plugin install foo.rhai` ships today.
- **OCI** (`oci://…`) and **Extism Hub** (`extism://hub/…`) distribution.
- **Signature enforcement on sandboxed loads** — today only the in-process `add_plugin` path checks signatures; sandboxed loads (WASM, Extism, Rhai, Python) are integrity-checked only through the host's artifact hash-pins.
- **Non-Rust authoring beyond scalar / aggregate / procedure / algorithm** — the other 18 surfaces are Rust-only in v1; some WIT worlds (e.g. CRDT) are tractable but deferred, while in-process surfaces like optimizer rules and label storage are infeasible across the Component Model boundary.

!!! note "Honest by design"
    This scope mirrors the plugin-framework implementation gap analysis. Where a capability is in place but not yet end-to-end, the documentation says so rather than implying completeness.
