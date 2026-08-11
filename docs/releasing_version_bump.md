# Releasing: bumping the version and keeping it in sync

This repo ships **one Rust workspace** and **seven Python packages** (the `uni-db` maturin wheel and
its five accelerator variants, plus the pure-Python `uni-pydantic`) from a single version number.
Most of those derive the version automatically; a few carry a **hand-maintained literal** that must
be bumped in lock-step. Getting one of them out of sync is what caused the `v2.4.0` release to fail
at the validation gate and publish nothing (see [the 2.4.0 note](release_notes/RELEASE_NOTES_2.4.1.md)).

This document is the checklist that prevents that.

## The single source of truth

```
Cargo.toml  →  [workspace.package]  →  version = "X.Y.Z"
```

Everything else either reads this automatically or must be manually matched to it. The release
workflow derives the release version from the **git tag** (`vX.Y.Z`) and asserts it equals this
field, so the tag, this field, and every synced literal below must all agree.

## What derives the version automatically (do NOT touch)

| Surface | Mechanism |
| --- | --- |
| Every workspace crate (`crates/*`, incl. `uni-sparse-vector`) | `version.workspace = true` in its `Cargo.toml` |
| The `uni-db` maturin wheel + 5 accelerator variants (`bindings/uni-db*/pyproject.toml`) | `dynamic = ["version"]` — maturin reads the crate `Cargo.toml` at build time |
| `uni_pydantic.__version__` | derived via `_package_version()` from installed package metadata |

The release workflow actively **forbids** reintroducing a hardcoded literal in the 6 maturin
`pyproject.toml` files (it greps for `^version = ` and fails if present), so these stay correct by
construction.

## What you MUST bump by hand (the synced literals)

`uni-pydantic` is pure-Python (hatchling, no Cargo manifest), so its isolated build cannot read the
Rust workspace version. It therefore carries the version literal in **two** places that both have
to match `Cargo.toml`:

| # | File | Line | Enforced by CI? |
| --- | --- | --- | --- |
| 1 | `bindings/uni-pydantic/pyproject.toml` | `version = "X.Y.Z"` (and the `uni-db>=X.Y.Z` floor) | ✅ `release.yml` validate-versions **and** `scripts/ci/check_version_consistency.py` in `release-guards`, per-PR |
| 2 | `bindings/uni-pydantic/uv.lock` | the `version` line under `name = "uni-pydantic"` | ⚠️ indirectly — a stale lock fails `uv lock --check` / `uv sync` checks |

> **`__version__` is no longer one of them.** It used to be a hand-written literal (the known gap
> this document warned about, which had drifted to `2.5.0` against a `3.3.0` package). It is now
> *derived* from installed package metadata by `_package_version()`, and
> `scripts/ci/check_version_consistency.py` actively **fails** if a literal `__version__ = "..."`
> reappears. Do not set it by hand — doing so now breaks `release-guards`.

Also bump the internal `[workspace.dependencies]` requirements in the root `Cargo.toml` (the
`uni-* = { path = ..., version = "X.Y.Z" }` lines), which crates.io publishing resolves against.
Third-party crates that happen to sit on a nearby version are left alone.

## Bump procedure

For a release `X.Y.Z` (e.g. `2.4.1`):

1. **Workspace (source of truth)** — edit `Cargo.toml`:
   ```toml
   [workspace.package]
   version = "X.Y.Z"
   ```

2. **uni-pydantic literals** — set both to the same `X.Y.Z` (do NOT touch `__init__.py`; its
   `__version__` is derived and a literal there fails `release-guards`):
   - `bindings/uni-pydantic/pyproject.toml` → `version = "X.Y.Z"` and the `uni-db>=X.Y.Z` floor
   - `bindings/uni-pydantic/uv.lock` → regenerate it rather than hand-editing if you can:
     ```bash
     cd bindings/uni-pydantic && uv lock
     ```
     (A pure version bump only changes the one `version` line under `name = "uni-pydantic"`. Hand-edit
     that single line only if `uv` is unavailable.)

3. **Verify everything agrees** — run the same assertion the release workflow runs:
   ```bash
   VERSION=$(grep -m1 '^version = ' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')
   echo "workspace:   $VERSION"
   echo "pyproject:   $(grep -m1 '^version = ' bindings/uni-pydantic/pyproject.toml | sed 's/.*"\(.*\)".*/\1/')"
   echo "uv.lock:     $(grep -A1 'name = "uni-pydantic"' bindings/uni-pydantic/uv.lock | grep version | sed 's/.*"\(.*\)".*/\1/')"
   # All three lines must print the same value.

   # And the generated Python symbol reference embeds the version, so it goes stale on a bump:
   python3 scripts/gen_python_api_reference.py        # regenerate, never hand-edit
   python3 scripts/ci/check_version_consistency.py

   # And the 6 maturin bindings must NOT carry a literal:
   for p in bindings/uni-db/pyproject.toml bindings/uni-db-onnx/pyproject.toml \
            bindings/uni-db-cuda/pyproject.toml bindings/uni-db-metal/pyproject.toml \
            bindings/uni-db-onnx-cuda/pyproject.toml bindings/uni-db-onnx-metal/pyproject.toml; do
     grep -qE '^version = ' "$p" && echo "BAD: $p hardcodes a version" || echo "OK: $p is dynamic"
   done
   ```

4. **Release notes** — add `docs/release_notes/RELEASE_NOTES_X.Y.Z.md`.

5. **Commit** the bump (workspace + the three uni-pydantic literals + release notes) together.

## Cutting the release (tag)

The release workflow triggers on a pushed tag `vX.Y.Z` and derives the release version from it
(`VERSION="${GITHUB_REF#refs/tags/v}"`), then asserts `VERSION == Cargo.toml workspace version`. So:

- The tag name, the workspace version, and the synced literals must all be identical **before** you
  tag.
- If the validate-versions step fails (as it did for `v2.4.0`), **nothing is published** — it runs
  before any build/publish job. The tag and an empty GitHub release may exist, but no wheels reach
  PyPI.

### If a tag was already cut with a version bug

Because the validate gate runs first and publishes nothing on failure, a failed tag has no immutable
artifacts. Two options:

- **Re-tag the same version** (only safe while nothing has been published): fix the bump, move the
  tag to the corrected commit, and force-push it (`git push --force origin vX.Y.Z`). Verify first
  that no PyPI wheels and no GitHub release assets exist for that version.
- **Skip to the next patch** (simplest, no force-push): bump to `X.Y.(Z+1)`, leave the broken tag
  abandoned, and tag the fixed commit. This is what was done for `2.4.0` → `2.4.1`.

## Quick reference

| File | What to set | Auto? |
| --- | --- | --- |
| `Cargo.toml` `[workspace.package] version` | `X.Y.Z` | source of truth |
| `Cargo.toml` `[workspace.dependencies]` `uni-*` | `X.Y.Z` | ❌ manual |
| `crates/*/Cargo.toml` | — | ✅ `version.workspace = true` |
| `bindings/uni-db*/pyproject.toml` (×6) | — | ✅ `dynamic = ["version"]` |
| `bindings/uni-pydantic/pyproject.toml` | `X.Y.Z` + `uni-db>=X.Y.Z` | ❌ manual (CI-enforced) |
| `bindings/uni-pydantic/uv.lock` | `X.Y.Z` | ❌ manual / `uv lock` |
| `bindings/uni-pydantic/src/uni_pydantic/__init__.py` | — | ✅ derived; a literal **fails** CI |
| `Cargo.lock` | `X.Y.Z` | ✅ regenerated by any cargo command |
| `website/docs/reference/python-api-symbols.md` | — | ✅ regenerate via `gen_python_api_reference.py` (CI-gated) |
| `website/docs/reference/python-api.md` | `vX.Y.Z` in the header line | ❌ manual (not CI-enforced) |
| git tag | `vX.Y.Z` | ❌ manual, must match workspace |
