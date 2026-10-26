# Workspace Test Layout

This workspace consolidates integration tests into a single binary per crate
(with separate binaries only for documented exceptions). Without
the convention, Cargo's default behavior — one binary per file in `tests/` —
turns a routine test workload into dozens of independent rustc + link
cycles, each statically linking the full transitive dep set
(datafusion, lance, candle, ...).

This document codifies the convention so new crates and new test files
follow it without per-author discovery.

## The rule (hard cap)

**A crate must not exceed 3 integration-test binaries.** Aim for 1.
The unit-test binary Cargo builds from the crate's own `src/` (the one
nextest lists as the bare crate name) does not count against the cap —
only binaries produced from files under `tests/`.

Concretely, when you add a test:

- **Adding a repro or an integration test?** Add a `mod <name>;` line to
  the crate's existing integration module tree. In `crates/uni`, add the
  file under `tests/common/<group>/` and register it in that group's `mod.rs`;
  `tests/integration.rs` already includes the groups. Do **not** create a new
  top-level `tests/<name>.rs` — that silently mints a whole new binary (full relink of datafusion/lance/candle/...),
  which is the exact drift this convention exists to prevent.
- **Crate has no `integration.rs` yet?** If it already has more than ~3
  loose `tests/*.rs` files, consolidate them first (see the migration
  recipe below), then add yours as a `mod`.
- **Need a 2nd or 3rd binary?** Only for a genuine, documented reason —
  a mutually-exclusive feature set (e.g. `provider-onnx-dynamic`), a
  loom/model-check target built under a different feature, or a
  registry/linker-section test that needs process isolation. Record the
  reason in a comment on the `[[test]]` entry or, for an auto-discovered
  target, in the test file's module documentation. "It's a separate topic" is
  **not** a reason — that's what modules are for.

Custom test harnesses also require a separate binary; see `uni-tck` below.
A topic or a large source-file count alone does not justify another binary.

## The pattern

For any crate with more than ~3 integration test files, configure
`Cargo.toml` as follows:

```toml
[package]
# ...usual fields...
# Integration tests are consolidated into a single binary (`tests/integration.rs`).
# See `docs/test_layout.md` for the rationale and the workspace convention.
autotests = false

[[test]]
name = "integration"
path = "tests/integration.rs"
```

Then create `tests/integration.rs` with one `mod` declaration per
sibling test file:

```rust
// Consolidated integration-test binary for `<crate>`.
//
// Cargo defaults to one binary per `.rs` file in `tests/`. `autotests = false`
// in `Cargo.toml` disables that auto-discovery; a single `[[test]]` entry
// points here, and every sibling test file is pulled in as a module below.

mod foo_test;
mod bar_test;
mod baz_test;
// ...
```

The sibling `.rs` files stay where they are; nothing about their contents
needs to change. Cargo resolves `mod foo_test;` to `tests/foo_test.rs`
because the integration binary's root (`tests/integration.rs`) lives in
the same directory.

## Why this works

- Cargo treats each `.rs` file directly under `tests/` as its own
  integration-test crate by default. Setting `autotests = false`
  disables that, leaving the test author in explicit control.
- With one consolidated binary, rustc builds and links a single test
  executable instead of N. The dep graph is shared; codegen and link
  costs amortize.
- `cargo nextest run --test integration -E 'test(<pattern>)'` filters by
  test name. Module prefixes (e.g. `foo_test::it_works`) let you select a
  group without creating a separate binary.
- Nextest runs tests in separate processes and schedules them in parallel.
  Consolidation saves compilation and linking without requiring serial test
  execution; `.config/nextest.toml` controls timeouts and constrained groups.

## Edge cases

### Feature-gated tests

If a test file's contents only compile when a feature is enabled, put
the inner attribute at the top of the file:

```rust
// tests/lance_only_test.rs
#![cfg(feature = "lance-backend")]

#[test]
fn ... { ... }
```

An inner `#![cfg(...)]` at the head of a module file scopes naturally to
that module — when the feature is off, the module body compiles to
empty. **No separate binary is required.** Avoid carving out a
`tests/integration_<feature>.rs` unless the feature gate is genuinely
cross-cutting at the binary level (rare).

`crates/uni-store/tests/integration.rs` uses module-level feature gates for
feature-specific groups. Feature gates control test inclusion; they do not
establish that the entire crate builds under every feature combination.

### Cross-cutting lint allowances

Inner attributes other than `cfg` (e.g.
`#![allow(clippy::cloned_ref_to_slice_refs)]`) also scope to the
containing module on a module-file, so you can leave them in the
individual test file and they'll still apply post-consolidation.

If you want them workspace-uniform, hoist them to `tests/integration.rs`.

### Genuinely incompatible feature combinations

`crates/uni/tests/reranker_integration.rs` remains separate because CI also
builds it with `--no-default-features --features provider-onnx-dynamic`.
The main integration suite requires a different feature configuration.
Rhai and PyO3 test groups are included in the main integration module tree;
their feature gates do not require separate binaries.

### Large test suites and process isolation

`crates/uni/tests/integration.rs` includes the groups under `tests/common/`
in one binary. Add tests to those groups; do not restore the historical
categorical shims or introduce a binary per topic.

`repro_nested_fork_capture_race.rs` is a separate, feature-gated target
because its process-global failpoint must be isolated from other fork-creating
tests. Its module documentation records that reason and the run command.

## Migration recipe

To convert an unconverted crate:

1. Inspect the test files for risks before converting:
   ```bash
   # Inner attributes worth migrating or surfacing:
   for f in crates/<crate>/tests/*.rs; do head -10 "$f" | grep -H "^#!" "$f"; done
   # Top-level mod declarations (rare; flag any that could collide):
   grep -nE "^(pub )?mod " crates/<crate>/tests/*.rs | grep -v "mod tests\b"
   # `#[path = \"...\"]` attrs (almost never present in flat layouts):
   grep -n "#\[path" crates/<crate>/tests/*.rs
   ```
2. Edit `Cargo.toml`: add `autotests = false` and one `[[test]]` entry as shown above.
3. Generate `tests/integration.rs` with one `mod <basename>;` per existing
   `tests/<basename>.rs`.
4. Run `cargo nextest list -p <crate>` before and after; the test count
   must match.
5. Run `cargo nextest run -p <crate>` to confirm all tests still pass.

## Crates currently following this convention

| Crate | Integration binaries | Notes |
|---|---|---|
| `uni` (`uni-db`) | 3 (`integration`, `reranker_integration`, `repro_nested_fork_capture_race`) | Main suite, alternate reranker feature configuration, and isolated failpoint regression |
| `uni-store` | 2 (`integration` + `occ_model`) | `occ_model` is the loom/shuttle model-check target (`#![cfg(any(feature = "loom", feature = "shuttle"))]`) and must stay standalone — merging it drags real-`Writer` tests into the loom build, which panics |
| `uni-query` | 1 (`integration`) | Consolidated integration modules |
| `uni-cypher` | 1 (`integration`) | Consolidated integration modules |
| `uni-plugin-host` | 1 (`integration`) | Consolidated integration modules |
| `uni-locy` | 1 (`integration`) | Consolidated integration modules |
| `uni-query-functions` | 1 (`integration`) | Consolidated integration modules |
| `uni-tck` | 2 (`tck` + `integration`) | `tck` is the cucumber/openCypher runner and must stay standalone — it is `harness = false`, supplying its own `fn main()` via libtest-mimic, which cannot host ordinary `#[test]` functions |

Every crate above is at or under the 3-binary cap. When adding a test to any
of them, add a `mod` to its `tests/integration.rs` — do not create a new
top-level `tests/*.rs` file. In crates with `autotests = false`, an unregistered
file is not built at all. `uni-db` currently uses auto-discovery, so a new
top-level file there creates another binary immediately.

Crates with ≤3 integration test files are already within the cap and need no
`integration.rs` yet — but the moment a 4th would be added, consolidate first
(migration recipe above) rather than minting a 4th binary.
