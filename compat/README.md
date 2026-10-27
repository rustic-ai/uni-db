# zstd security compatibility facades

Arrow IPC/Parquet 58, Lance 11, compression-codecs and Wasmtime 49 require
`zstd` 0.13; compression-codecs also requires `zstd-safe` 7. Updating only the
lockfile cannot select the patched 0.14 / 8 releases for those requirements.

These two private crates preserve the required package versions and feature
names, but contain **no compression implementation**. Each re-exports the
corresponding patched registry crate:

| Required package | Actual implementation |
| --- | --- |
| zstd 0.13.3 (local facade) | zstd **0.14.3** |
| zstd-safe 7.2.4 (local facade) | zstd-safe **8.2.0** |

The fixed implementations remain ordinary registry dependencies with checksums
in Cargo.lock. The facades do not copy vulnerable source, relabel an old
implementation, or suppress advisories. Default features and each older
release's optional features forward to the patched dependency; zstd-safe
remains `no_std` without its `std` feature.

The root workspace and standalone fuzz workspace both apply these patches.
Workspace nextest runs the facade tests, including abandoned-context isolation,
malformed legacy-frame output lengths, seekable offsets beyond EOF, and bulk
compression with dictionaries. Seekable and legacy tests enable their required
features through dev-dependencies; production builds retain feature selection.

## Scope

Cargo patches apply only at the root of a dependency graph. These patches cover
repository builds (including CLI and Python wheels) and fuzz builds. They are
**not inherited by applications depending on a published Uni Rust crate**.
Those consumers must apply the same patches in their own root manifest until
the upstream dependencies accept the patched release lines. The facades are
`publish = false`; they are not additional Uni public crates.

## Removal

Remove the facades and both patch tables once all upstream consumers accept
zstd >=0.14.3 and zstd-safe >=8.2.0. Verify the root and fuzz lockfiles contain
only patched registry implementations, run the security regressions and storage
round trips, and require `cargo deny check` to pass without new ignores.

Upstream fixes:
- https://rustsec.org/advisories/RUSTSEC-2026-0391
- https://rustsec.org/advisories/RUSTSEC-2026-0398
- https://rustsec.org/advisories/RUSTSEC-2026-0403
