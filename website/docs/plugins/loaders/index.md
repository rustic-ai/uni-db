# Plugin Loaders

A plugin's *surface* (see [Plugin concepts](../concepts.md)) is what it does — a scalar function, an aggregate, a procedure, a graph algorithm. A *loader* is the other axis: it determines **how** that surface is authored, what language it is written in, how it is built, and whether it runs trusted in-process or inside a sandbox. The execution layer is loader-agnostic — a registered scalar looks identical to the query engine no matter which loader produced it — so the same surface can be delivered by any of the five loaders below.

Uni ships **five loaders**: Rust (native), WASM Component Model, Extism, Rhai, and PyO3. They split into two trust postures: Rust and PyO3 run **trusted, in-process**; WASM Component Model, Extism, and Rhai run **sandboxed**. The same logic produces byte-identical output across the two WASM ABIs (verified by the cross-ABI parity test), and matches to within 4 ULP for Rhai and PyO3.

---

## Choosing a loader

Pick by trust, language, and how the plugin is delivered:

- **First-party extension, all surfaces, full speed → Rust.** The native loader is the only one that can author all 22 extension surfaces (CRDTs, indexes, label storage, hooks, triggers, and more — not just scalar/aggregate/procedure/algorithm). It runs trusted in-process at native speed and is the path every built-in uses. Choose it when you control the code and are packaging it into the host. Loaded via `add_plugin`, which is trust-enforced.
- **Sandboxed polyglot binary → WASM Component Model (preferred) or Extism.** Both run untrusted third-party code in a wasmtime sandbox and are loadable from Rust and from Python (in a build with the `wasm-plugins` / `extism-plugins` features; the published wheels omit them). Prefer **WASM Component Model** when you want a typed contract: it uses typed WIT worlds, so argument and return shapes are checked at the ABI boundary. Choose **Extism** for a simpler host-function ABI, which links only the host functions for granted capabilities.
- **Lightweight scripting → Rhai.** A sandboxed, pure-Rust scripting engine with no separate build step — author a script and load it. Good for small ops-style logic. Loadable from Python via `Uni.load_rhai_plugin(script, grants=...)`, with an awaitable `AsyncUni` counterpart.
- **Trusted Python with vectorization → PyO3.** Runs Python trusted in-process with vectorized scalar evaluation. Choose it when data scientists author extensions directly in Python and you trust the code. Loadable from Python via `Uni.load_python_plugin(...)` (instance-wide) or `Session.load_python_plugin(...)` (and the `@session.scalar_fn` decorator surface, session-scoped).

---

## Comparison matrix

| Loader | Runtime / host | Build target | Wire format | Capability gating | Trust posture | Surfaces (v1) | Load entry point | Python-loadable today |
|---|---|---|---|---|---|---|---|---|
| **Rust** | in-process native | (host crate) | native trait | compile-time | trusted | all 22 | `add_plugin` | no (Rust host only) |
| **WASM Component Model** | wasmtime + typed WIT worlds | `wasm32-wasip2` | Arrow IPC | declared ∩ granted (reported) | sandboxed | scalar / aggregate / procedure / algorithm | `load_wasm_component` | **yes** (with the `wasm-plugins` feature) |
| **Extism** | Extism host-fn ABI | `wasm32-unknown-unknown` | Arrow IPC / JSON over linear memory | load-time (host-fn filter) | sandboxed | scalar / aggregate / procedure / algorithm | `load_wasm_extism` | **yes** (with the `extism-plugins` feature) |
| **Rhai** | Rhai scripting engine | none (script) | `rhai::Engine` values | load-time (engine factory) | sandboxed | scalar / aggregate / procedure / algorithm | `load_rhai_plugin` | **yes** (`Uni` / `AsyncUni`) |
| **PyO3** | in-process Python | (Python module) | PyCapsule / Arrow C Data Interface | manifest + runtime | trusted | scalar / aggregate / procedure / algorithm (vectorized scalars) | `load_python_plugin` | **yes** (`Uni` / `AsyncUni`, `Session` / `AsyncSession`) |

Only Rust authors all 22 surfaces today; the other four are limited to scalar, aggregate, procedure, and graph algorithm in v1. WASM Component Model and Extism produce byte-identical results for the same logic; Rhai and PyO3 agree to within 4 ULP. All four non-Rust loaders are loadable from Python: WASM Component Model, Extism and Rhai load on the instance (`Uni` / `AsyncUni`); PyO3 (Python) plugins load on the instance or on a `Session` / `AsyncSession` (session-scoped). The WASM Component Model and Extism methods exist only in a build with their features enabled. The native Rust loader is the only one without a Python load path — by nature, it is the host embedding.

---

## Per-loader guides

- [WASM Component Model](wasm-components.md) — typed WIT worlds, `wasm32-wasip2`.
- [Extism](extism.md) — host-fn ABI, load-time capability filtering, `wasm32-unknown-unknown`.
- [Rhai](rhai.md) — sandboxed pure-Rust scripting, no build step.
- [PyO3](pyo3.md) — trusted in-process Python with vectorized scalars.

See also [Authoring plugins](../authoring.md) for the surface-by-surface authoring model, and [Trust and capabilities](../trust-and-capabilities.md) for how grants, sandboxing, and signature policy interact with each loader.
