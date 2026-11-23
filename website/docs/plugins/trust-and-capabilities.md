# Trust & Capabilities

The host security model for plugins answers two independent questions. **Trust**
decides *whether, and whose,* code is allowed to load at all — by checking
manifest signatures, signing-key roots, and content hash-pins. **Capabilities**
decide *what a plugin, once loaded, is permitted to do* — the grant set that
gates each extension surface and host import, plus the resource quotas that bound
its execution.

These are deliberately separate axes. A plugin can be fully trusted (signed by a
key in your root) and still be granted nothing but a single scalar-function
surface; conversely, an unsigned plugin loaded under a permissive policy is still
subject to the capability set you grant it. This page covers both, and is precise
about what is enforced today versus deferred — see the [Scope note](#scope-note).

For the broader model, see [Concepts](concepts.md); for exact type signatures,
see [Reference](reference.md); for the per-loader details, see
[Loaders](loaders/index.md).

## Trust vs capabilities

| Concern | Question | Mechanism | Configured on |
| --- | --- | --- | --- |
| **Trust** | Whether/whom to load | Signature policy, trust root, artifact hash-pin | The host builder (`plugin_trust`) |
| **Capabilities** | What a loaded plugin may do | Grant set (`effective = declared ∩ granted`), quotas | The load call (`grants=[...]`) |

Trust is a per-instance, host-level decision: one signature policy and one trust
root govern the whole `Uni` instance. Capabilities are a per-load decision: every
plugin you load is granted its own set, and two loads of the same artifact may be
granted differently.

## Signature policy

`SignaturePolicy` (in `uni-plugin`, `crates/uni-plugin/src/verify.rs`) has three
levels, dialled up over time without changing call sites:

- **`Disabled`** — the default. Signature checks are skipped entirely; signed and
  unsigned manifests both pass *without inspection*. This is the back-compatible
  v1 behavior.
- **`WarnIfUnsigned`** — runs the verifier. A manifest with a signature is
  verified against the trust root; a manifest *without* one is accepted but emits
  a `tracing::warn!`. Use this to observe your plugin surface before enforcing.
- **`RequireSigned`** — runs the verifier and rejects anything that is not
  validly signed: an unsigned manifest, a signature whose `key_id` is not in the
  trust root, or a bad signature all produce a `PluginError`.

The host sets the policy on the builder via `plugin_trust`. Because the trust
config is a builder-level runtime object (see below), this is Rust-side
configuration; loaders called from Python inherit whatever the instance was built
with.

=== "Rust"

    ```rust
    use std::sync::Arc;
    use uni_db::Uni;
    use uni_db::api::plugin_trust::PluginTrustConfig;
    use uni_plugin::verify::{SignaturePolicy, TrustRoot};

    let mut root = TrustRoot::new();
    root.allow_with_key("release-2026", release_pubkey_bytes); // [u8; 32]

    let db = Uni::open("./db")
        .plugin_trust(PluginTrustConfig::new(
            SignaturePolicy::RequireSigned,
            root,
        ))
        .build()
        .await?;
    ```

    The default — equivalent to omitting `plugin_trust` entirely — is
    `SignaturePolicy::Disabled` with an empty trust root, which accepts every
    plugin. `PluginTrustConfig::default()` constructs exactly that.

## Trust root & keys

A `TrustRoot` is the set of signing keys the host accepts. You build it
explicitly:

```rust
let mut root = TrustRoot::new();
root.allow_with_key("ops@example.com", pubkey_bytes); // 32-byte Ed25519 key
```

`allow_with_key(key_id, public_key)` binds a key id to its 32-byte Ed25519 public
key; the related `allow(key_id)` adds a key id *without* key material, which is
useful for shape-only verification and tests.

Cryptographic verification of a signed manifest (`verify_signed_manifest` /
`verify_ed25519`) is **always compiled** — there is no Cargo feature gating it
and no degraded shape-only fallback. `ed25519-dalek` is an unconditional
dependency of `uni-plugin`, whose only feature is `otel`. The verifier is also
fail-closed: a `key_id` present in the trust root but carrying no public-key
bytes (the `allow()` shape-only path) is **rejected**, not waved through.

### Artifact hash-pinning

`PluginTrustConfig::pinned_artifacts` is a set of Blake3 hex digests. Left empty
(the default) pinning is off. When populated, every loader entry point that
receives payload bytes — WASM component, Extism, Rhai, PyO3 — rejects a payload
whose digest is not in the set, before it is instantiated and before any
capability is granted.

```rust
let mut trust = PluginTrustConfig::default();
trust.pinned_artifacts.insert(
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
);
```

The pin is deliberately **external** to the artifact. A digest carried inside a
plugin's own manifest is self-certifying — anyone able to rewrite the payload can
rewrite the digest beside it — so an embedded `hash` field is only meaningful
when an Ed25519 signature covers the whole manifest (see
`canonical_payload`, which is exactly why the signature spans every field rather
than the hash alone).

### Why it is not in `UniConfig`

`TrustRoot` holds raw public-key material and is intentionally **neither `Clone`
nor `Serialize`**. The serializable `UniConfig` is cloned into every session and
fork, so a non-`Clone`, non-`Serialize` object cannot live there. If you go
looking for a `trust_root` field in [Configuration](../reference/configuration.md),
you will not find one — by design.

Instead the trust policy is a *builder-level runtime object*: you pass
`PluginTrustConfig` to `UniBuilder::plugin_trust`, it is stored on the instance's
internal state (`UniInner`), wrapped in an `Arc` so it can be shared across
`at_snapshot` / `at_fork` clones, and consulted at every plugin-load site. There
is intentionally no global, serializable plugin-config struct.

## Grants & capabilities

A loaded plugin's authority is its **effective capability set**, which is the
intersection of what its manifest *declares* and what the host *grants*:

```text
effective = declared ∩ granted
```

A capability the plugin never declared cannot be granted into existence, and a
capability the host did not grant is stripped even if declared. On the load APIs,
the `grants` argument is a list of capability *name* strings. Common grants:

| Grant | Surface |
| --- | --- |
| `ScalarFn` | Register Cypher scalar functions |
| `AggregateFn` | Register Cypher aggregate functions |
| `Procedure` | Register Cypher procedures (read-only) |
| `Algorithm` | Register graph algorithms |
| `GraphCompute` | Drive the GraphCompute coarse kernels (guest-authored graph algorithms) |
| `Filesystem` | Filesystem read/write host import |
| `Network` | HTTP/TCP egress host import |
| `HostQuery` | Query back into the host session (also: project a graph for `GraphCompute`) |
| `Kms` | KMS sign/verify host import |
| `Secret` | Acquire named secret handles |

The full grantable set — 21 extension registration-gates plus 7 allow-list host
surfaces — is the single source of truth in `uni_plugin::Capability`; see the
[reference](reference.md#capability-names) for the complete tables. A
guest-authored graph algorithm needs the triple `Algorithm` + `GraphCompute` +
`HostQuery`.

### What is *not* grantable from a string

Two classes of capability are deliberately unreachable through the `grants` list:

- **Resource quotas** (`MemoryBytes`, `FuelPerCall`, `GraphComputeWork`, …) carry
  a numeric value and are declared in the plugin **manifest's** `capabilities:`
  list, never as a bare grant string.
- **Internal / first-party** capabilities (`Auth`, `Authz`, `Cdc`, `Catalog`,
  `PluginDeclare`) are never granted to a guest; only host code constructs them.

Grant names are case- and separator-insensitive: `HostQuery` and `host-query`
resolve identically. The strict load path (Rhai) raises on an unknown, quota, or
internal name; the lenient paths (the WASM and Extism methods, the Python
source-string loader `load_python_plugin`, the decorator surface, and the CLI)
drop it. Adding a new `Capability` variant is caught by
the `every_variant_classified_exactly_once` test in `uni-plugin`, which forces it
to be classified as grantable, quota, or internal before it can ship — the
mechanism that keeps this surface from drifting.

### How withheld capabilities take effect

- **Registration surfaces** — a registrar method (e.g. `scalar_fn`) is rejected
  with `PluginError::CapabilityRequired` when the corresponding capability is not
  in the effective set. This applies to every loader, and every loader reports it
  the same way: the load fails with `UniError::InvalidArgument` (Python
  `UniInvalidArgumentError`) naming the missing capability, for a WASM component
  or Extism plugin as for a native or Rhai one. (The WASM and Extism loaders
  used to flatten it into `UniInternalError`.)
- **Extism** — the host-function set is filtered at load: only the host functions
  for granted capabilities are linked into the plugin.
- **WASM Component Model** — the loader computes the effective set and reports it
  as `effective_capabilities` / `denied_capabilities`. The capability-gated
  `host-net` import (HTTP GET/POST) is added to the linker only when `Network` is
  granted — a plugin importing it without the grant fails at link time. The
  always-available `host-log` and `host-trace-context` imports are linked
  unconditionally. (`host-fs` is not yet exposed on the Component Model. Extism
  wires `uni_http_get`, `uni_http_post`, `uni_kms_sign`, `uni_kms_verify`,
  `uni_secret_acquire` and `uni_graph_call`; neither ABI has filesystem or
  host-query functions.)

=== "Python"

    ```python
    from uni_db import Uni

    db = Uni.open("./db")

    with open("geo_component.wasm", "rb") as f:
        outcome = db.load_wasm_component(f.read(), grants=["ScalarFn"])

    # Extism plugins use the same grants list. (Shown on a second instance:
    # both geo builds register `ai.example.geo.haversine`, and a qname can be
    # registered only once per instance.)
    db2 = Uni.temporary()
    with open("geo_extism.wasm", "rb") as f:
        outcome = db2.load_wasm_extism(f.read(), grants=["ScalarFn"])
    ```

    Both methods exist only in a build with the `wasm-plugins` /
    `extism-plugins` features; the published wheels omit them.

    Passing `grants=["ScalarFn"]` grants exactly the scalar-function surface and
    nothing else — no network, filesystem, or host-query host imports are linked.
    The Python bindings currently expose the coarse capability *variants*; the
    narrowed forms (specific path globs for `Filesystem`, URL globs for
    `Network`) are configured Rust-side.

## Quotas

Per-call resource limits are applied at load:

| Limit | Bounds |
| --- | --- |
| `FuelPerCall` | Maximum fuel / operations consumed per call |
| `MemoryBytes` | Maximum linear memory per instance |

The Rhai engine enforces these from the granted quota capabilities; the WASM
Component Model and Extism loaders take the equivalent fuel/memory limits from
their manifest fields. There is intentionally **no global `PluginConfig`**
struct — per-load grants and limits are the only knobs.

## Scope note

!!! warning "Signature enforcement is wired today only on the `add_plugin` path"

    The signature policy and trust root described above are enforced **today only
    on the compile-time `add_plugin` path** — the path the wiring tests exercise
    (`Disabled` accepts unsigned, `RequireSigned` rejects unsigned,
    `WarnIfUnsigned` accepts and warns).

    The sandboxed loader manifest formats — `ComponentManifest` for WASM Component
    Model and `ExtismPluginManifest` for Extism — **do not yet carry signature
    fields**. Until they do, signature enforcement on WASM, Extism, Rhai, and
    Python *loads* is **deferred (Phase D)**. Their artifact integrity is
    checked only by the host's [artifact hash-pins](#artifact-hash-pinning),
    which all four of those loaders enforce when `pinned_artifacts` is set.

    Capability *grants*, by contrast, take effect on the sandboxed load path
    today: registrar surfaces reject ungranted registrations, and the Extism
    loader links only the host functions for granted capabilities (see
    [Grants & capabilities](#grants-capabilities)).
