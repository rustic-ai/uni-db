# Local CI Runbook — replicating the full GitHub CI locally

This document lists every CI job from `.github/workflows/pr.yml` and `.github/workflows/ci.yml`
with the **exact command** to run it locally, plus prerequisites, ordering, and the local-only
gotchas that bite.

> **Source of truth = the workflow YAML.** This runbook mirrors the workflows as of 2026-07-26.
> If a command here disagrees with `.github/workflows/{pr,ci}.yml`, the YAML wins — update this doc.
> Run commands from the repo root unless a "working dir" is noted. **Do not substitute workarounds**
> for the documented commands; if a command fails, that is a real signal — report it.

## 0. Prerequisites (one-time)

```bash
# Rust toolchain targets used by the wasm plugin fixtures
rustup target add wasm32-unknown-unknown wasm32-wasip2

# Test runner + wasm tooling
cargo install cargo-nextest --locked
cargo install wasm-tools --locked   # only for wasm32-unknown-unknown fixtures;
                                   # the wasip2 ones are components already

# Python tooling (bindings)
#   install uv:  https://docs.astral.sh/uv/   (CI uses python 3.12)

# System deps CI installs (Debian/Ubuntu names; install equivalents on Fedora)
#   mold, protobuf-compiler

# Docker — only needed for the Cloud/LocalStack job
```

Network access is required for: HuggingFace model pulls (reranker real-ONNX tests), the ONNX Runtime
tarball (reranker load-dynamic), and the LocalStack image (cloud).

### Environment normalization

```bash
# CI runs with NO rustc wrapper. If you have a global sccache/RUSTC_WRAPPER configured locally,
# unset it for every cargo/maturin command or the build can fail. Prefix commands with:
export RUSTC_WRAPPER=""
```

### Ordering / contention

- **All `cargo`/`maturin` commands serialize on the build-dir lock** — run them one at a time.
- Docker (LocalStack) and pure-Python (`uv`) steps do **not** contend with a cargo build, so they can
  warm up in parallel (e.g. start LocalStack while a build runs).
- Heavy builds (provider-onnx static link, release wheel) are best run in the background while you
  watch a log.

---

## 1. Quick path — a Rust-only change

The jobs a Rust change can actually move. Run these first:

```bash
export RUSTC_WRAPPER=""
cargo fmt --all -- --check
cargo clippy --workspace \
  --exclude uni-tck --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal \
  --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace \
  --exclude uni-python --exclude uni-tck --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
./scripts/build-wasm-fixtures.sh
cargo nextest run --workspace \
  --exclude uni-tck --exclude uni-python --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
```

The full job list follows.

---

## 2. `pr.yml` — PR checks

### Lint
```bash
cargo fmt --all -- --check
cargo clippy --workspace \
  --exclude uni-tck --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal \
  --all-targets -- -D warnings
```

### Rust Tests (workspace suite)
```bash
./scripts/build-wasm-fixtures.sh      # builds the geo/net example wasm plugin fixtures first
cargo nextest run --workspace \
  --exclude uni-tck --exclude uni-python --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
```

### Concurrency Model Check (loom smoke)
```bash
# MUST set the preemption bound, or the exhaustive search blows past the nextest timeout.
LOOM_MAX_PREEMPTIONS=2 cargo nextest run -p uni-store --features loom --test occ_model
```

### Metamorphic Query Oracles (smoke)
```bash
METAMORPHIC_CASES=64 cargo nextest run -p uni-db --test integration \
  -E 'test(/metamorphic::/) and not test(soak)'
```

### GraphCompute handle trace (`UNI_GC_TRACE`)
```bash
# The trace is off by default and its assertions follow the env var, so this job
# is the only place the real `UNI_GC_TRACE` read is exercised. It must pass in
# BOTH states — the ordinary workspace run above covers the off case.
UNI_GC_TRACE=1 cargo nextest run -p uni-plugin-builtin -p uni-plugin-rhai -E 'test(graph_compute)'
```

### Failpoint Crash Suite
```bash
# The fail-rs crash/reopen tests are compiled out unless this feature is on.
# `--run-ignored all` is load-bearing: the env-var fault-injection tests are
# #[ignore]d because fail-rs's registry and the counters in lance_branch.rs are
# process-global.
cargo nextest run -p uni-db -p uni-store --features failpoints --run-ignored all \
  -E 'test(/resilience|recovery|durability/)'

# And the other half of the contract -- the seams must stay inert without the
# feature, since that is what every other job builds:
cargo nextest run -p uni-store -E 'test(/resilience|recovery/)'
```
116 tests, 6 s warm. The cold cost is ~3 min 40 s and is almost entirely the
second feature configuration compiling, so expect a slow first run after any
dependency change.

Worth knowing: this suite existed for a long time before any CI job ran it. If
you add a `fail_point!` seam, add its test to a file matching the filter above,
or it will be dormant on arrival.

### Miri (UB interpreter)
```bash
# Needs the PINNED nightly plus the miri component. This is the one pinned
# toolchain in the repo: `miri` is a rustup component that is not built for
# every nightly, so an unpinned lane goes red on a random day on the
# PR-blocking side of the fence.
rustup toolchain install nightly-2026-07-11 --component miri rust-src
cargo +nightly-2026-07-11 miri setup

# `cargo miri test`, NOT nextest -- the documented exception to the nextest
# rule. Nextest runs a process per test and each pays a fresh interpreter + std
# startup; these suites are only affordable because their tests share one
# interpreted process.
PROPTEST_CASES=16 cargo +nightly-2026-07-11 miri test -p uni-btic --lib --tests
PROPTEST_CASES=16 cargo +nightly-2026-07-11 miri test -p uni-sparse-vector --lib --tests

# uni-common needs -Zmiri-disable-isolation (it calls Utc::now() and does real
# filesystem I/O through object_store); the two codec crates above deliberately
# do not, so a new syscall dependency there surfaces instead of passing quietly.
#
# The trailing `::` in the skip is load-bearing: libtest's --skip is a plain
# substring match, and the bare `muvera` form also swallows
# `vector_index_opts::tests::muvera_defaults_and_inner`. Measured: bare removes
# 14 tests, anchored removes exactly the 13 in the module.
MIRIFLAGS="-Zmiri-disable-isolation" \
  cargo +nightly-2026-07-11 miri test -p uni-common --lib --tests \
  -- --test-threads=1 --skip 'muvera::'
```
Measured 1 min 41 s warm for all three. `muvera` is excluded outright rather
than budgeted -- its tests were killed at 132 minutes. `uni-crdt` runs in
`nightly.yml` only.

A miri failure is real signal even though these crates contain zero `unsafe`:
the UB is reached *through* a dependency, or it is a leak. It has already found
one here -- a `std::mem::forget(TempDir)` leaking a directory on disk every run
(`crates/uni-common/tests/repro_rename_property_bypass.rs:19-27`). If the fault
is upstream, file it and `#[cfg_attr(miri, ignore)]` the single test with a
comment linking the issue. Do not add `-Zmiri-ignore-leaks`, and do not drop the
crate from the lane.

### openCypher TCK (schemaless)
```bash
cargo nextest run -p uni-tck --test tck
```

### Python Tests
```bash
# uni-db  (working dir: bindings/uni-db)
( cd bindings/uni-db
  uv sync --group dev
  uv run maturin develop
  uv run ruff format --check .
  uv run ruff check .
  uv run pytest tests/ -v -n auto )

# pyo3 loader Rust tests — these exist ONLY under `--features pyo3`, and every file in
# the crate is `#![cfg(feature = "pyo3")]`, so the workspace run above lists zero tests
# for it. They live in this job because it is the one with a `pyarrow` environment: the
# vectorized scalar path hands guests an Arrow array and guest bodies compute on it.
# pyo3 `auto-initialize` links the SYSTEM interpreter, whose sys.path excludes the venv —
# so without PYTHONPATH the package is installed and still invisible, and the tests skip.
( cd bindings/uni-db
  SITE=$(uv run python -c "import site; print(site.getsitepackages()[0])")
  PYTHONPATH="$SITE" cargo nextest run --manifest-path ../../Cargo.toml \
    -p uni-plugin-pyo3 --features pyo3 )

# uni-pydantic  (working dir: bindings/uni-pydantic) — imports the uni-db .so via editable path dep
( cd bindings/uni-pydantic
  uv sync --group dev
  uv run ruff format --check .
  uv run ruff check .
  uv run pytest tests/ -v -n auto )
```

---

## 3. `ci.yml` — main-push thorough suite (everything above, plus)

### TCK sidecar + Locy TCK (both lanes)
```bash
UNI_TCK_SCHEMA_MODE=sidecar cargo nextest run -p uni-tck --test tck
cargo nextest run -p uni-locy-tck --test locy_tck
UNI_LOCY_TCK_SCHEMA_MODE=sidecar cargo nextest run -p uni-locy-tck --test locy_tck
```

### Reranker Integration (ONNX)
```bash
# Bundled CPU ONNX (statically links libonnxruntime.a; pulls real models from HF).
# One test is filtered out — its model is only served via an unsupported xet-bridge redirect.
cargo nextest run -p uni-db --features provider-onnx --test reranker_integration --run-ignored all \
  -E 'not test(=test_real_onnx_cross_encoder_reranks_by_relevance)'

# Load-dynamic ONNX — needs the ORT shared lib at runtime; --no-default-features is required
# (default `provider-onnx` and `provider-onnx-dynamic` are mutually exclusive at the `ort` level).
curl -sSL -o /tmp/ort.tgz \
  https://github.com/microsoft/onnxruntime/releases/download/v1.20.1/onnxruntime-linux-x64-1.20.1.tgz
tar xzf /tmp/ort.tgz -C /tmp
export ORT_DYLIB_PATH=/tmp/onnxruntime-linux-x64-1.20.1/lib/libonnxruntime.so
cargo nextest run -p uni-db --no-default-features --features provider-onnx-dynamic \
  --test reranker_integration
unset ORT_DYLIB_PATH
```

### Cloud Integration (LocalStack)
```bash
docker run -d --name uni-localstack -p 4566:4566 \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 \
  localstack/localstack:4.13.1
timeout 120 bash -c 'until curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; do sleep 2; done'

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
AWS_ENDPOINT_URL=http://localhost:4566 AWS_ALLOW_HTTP=true \
  cargo nextest run -p uni-store --test integration --run-ignored all \
    -E 'test(/^cloud_integration_test::/)'
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
AWS_ENDPOINT_URL=http://localhost:4566 AWS_ALLOW_HTTP=true \
  cargo nextest run -p uni-db --test integration --run-ignored all \
    -E 'test(/^hybrid_localstack_e2e::/)'

docker rm -f uni-localstack          # teardown
```

### Documentation
```bash
# Generated-notebook freshness (no compile)
python3 website/scripts/generate_locy_notebooks.py --check
python3 website/scripts/generate_semiconductor_flagship_notebook.py --check
python3 website/scripts/generate_pharma_flagship_notebook.py --check
python3 website/scripts/generate_cyber_flagship_notebook.py --check

# rustdoc gate
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace \
  --exclude uni-python --exclude uni-tck --exclude uni-python-onnx \
  --exclude uni-python-cuda --exclude uni-python-metal \
  --exclude uni-python-onnx-cuda --exclude uni-python-onnx-metal
```

### Flagship Notebooks (heaviest; release wheel + neural execution)
```bash
( cd bindings/uni-db
  uv sync --group dev --extra notebook-runtime
  rm -f dist/*.whl                         # else the glob below matches two versions
  uv run maturin build --out dist          # NOTE: `dev` profile — maturin only builds
                                           # release with an explicit `--release`.
                                           # ci.yml's notebooks job omits it, so the
                                           # notebooks execute an UNOPTIMIZED build.
                                           # The published wheels are unaffected:
                                           # release-wheels.yml passes `--release`.
  uv pip install --force-reinstall dist/*.whl
  # Assert the notebooks will actually run against what was just built.
  .venv/bin/python3 -c "from importlib.metadata import version; print(version('uni-db'))" )

# Run the 6 notebooks SERIALLY (they fail spuriously under concurrent CPU/GIL load).
# `--no-sync` is REQUIRED: a plain `uv run` re-syncs the project, uninstalls the wheel
# installed above and restores the editable install, so the notebooks would silently
# exercise a `maturin develop` build instead of the release wheel.
for nb in semiconductor pharma cyber predictive_maintenance adverse_drug_reaction drug_drug_interaction; do
  uv run --no-sync --project bindings/uni-db python website/scripts/verify_${nb}_flagship_notebook.py
done
```

### Python WASM/Extism Loader Tests
```bash
# The loaders are `#[cfg]`-gated on `wasm-plugins` / `extism-plugins`, which
# `bindings/uni-db` drops from its default set for wheel size, so every loader
# test SKIPS in pr.yml's default-feature pytest run. This lane is the only place
# they actually execute. Build the fixtures BEFORE the wheel — the tests skip on
# a missing fixture exactly as they do on a missing loader, so without them the
# run is vacuously green.
( ./scripts/build-wasm-fixtures.sh
  cd bindings/uni-db
  uv run maturin develop --features wasm-plugins,extism-plugins
  # Guard: assert the feature build took effect, else the tests below just skip
  # and the lane passes green.
  uv run python -c "
import sys, uni_db._uni_db as ext
missing = [m for m in ('load_wasm_component', 'load_wasm_extism')
           if not hasattr(ext.Uni, m)]
sys.exit('feature build did not take effect; missing: %r' % missing) if missing else None
"
  uv run pytest tests/test_wasm_plugin.py tests/test_plugin_conformance.py \
    tests/test_stub_drift.py -v
  # Restore the default-feature wheel for any later local step.
  uv run maturin develop )
```

### Release Guards
```bash
python3 scripts/ci/check_wheel_variant_features.py
python3 scripts/ci/check_version_consistency.py   # pyproject == workspace; no hardcoded __version__
python3 scripts/ci/check_documented_counts.py     # docs match `assert_eq!(kinds.len(), 22)`
python3 scripts/gen_python_api_reference.py --check  # generated symbol page is current
python3 scripts/ci/check_doc_symbols.py           # documented Python methods exist in __init__.pyi
python3 scripts/ci/check_publish_list.py
cargo check -p uni-python-onnx          # slim default-features=false wheel compile guard
```

If `gen_python_api_reference.py --check` fails, regenerate rather than hand-editing:
`python3 scripts/gen_python_api_reference.py`. The page is generated from
`bindings/uni-db/uni_db/__init__.pyi` and must never be edited directly.

### CUDA wheel-graph smoke
```bash
cargo metadata --format-version=1 --manifest-path bindings/uni-db-cuda/Cargo.toml > /dev/null
```

---

## 4. Not run locally

- **`gate`** (`ci.yml`) — an aggregator that just depends on the jobs above; nothing to execute.
- **`release-wheels.yml`, `deploy-docs.yml`, `publish-pydantic.yml`** — tag/push-triggered artifact
  publishing; no local validation value.

---

## 5. Local-only gotchas

- **`RUSTC_WRAPPER=""`** — see §0. Unset any global wrapper for cargo/maturin.
- **Wheel size: use `--profile dist`, not `--release`.** `maturin build --release` produces a
  wheel ~1.4x the published size (measured: 115.8 MiB vs 83.6 MiB; `lib_uni_db.so` 317.3 vs
  223.5 MiB). `release` uses `codegen-units = 16` for build speed; `dist` uses `1`, which lets
  LLVM dead-strip across crate boundaries — and ~94% of `.text` here is dependencies.
  `release-wheels.yml` passes `--profile dist` in every build job, so a `--release` wheel is not
  comparable to a published one. Profiles live in `.cargo/config.toml`, not `Cargo.toml`.
- **loom timeout** — always pass `LOOM_MAX_PREEMPTIONS=2`; without it the exhaustive model runs past
  the nextest `terminate-after` and reports a false TIMEOUT.
- **A version bump does not reach the editable install.** `bindings/uni-db` declares
  `dynamic = ["version"]`, so maturin derives the version from `Cargo.toml` — but uv caches the
  built editable metadata and does not invalidate it when only `Cargo.toml` moves. After a
  release bump the venv keeps reporting the *old* version indefinitely, so
  `importlib.metadata.version('uni-db')` is not a trustworthy freshness check on its own.
  `maturin develop` writes the right version; the next `uv run`/`uv sync` clobbers it again from
  cache. Fix with `uv sync --reinstall-package uni-db` (add `--extra notebook-runtime` if you
  need it, or the sync strips numpy/onnxruntime/protobuf). `uv cache clean uni-db` alone is not
  enough — sync still treats the existing install as satisfying.
- **`uv run` silently reverts a wheel install.** `uv run` syncs the project environment
  first, which uninstalls anything `uv pip install`-ed over the editable project and restores
  the editable install. In the notebooks job that means the built wheel is discarded before a
  single notebook runs, and the job passes having tested a `maturin develop` build. Pass
  `--no-sync` (or invoke `.venv/bin/python3` directly) whenever the installed artifact matters.
- **Stale wheels in `bindings/uni-db/dist/` after a version bump** — the notebooks job does
  `uv pip install --force-reinstall dist/*.whl`, and that glob matches *every* wheel ever built
  there. Two versions present makes `uv` refuse with "Requirements contain conflicting URLs for
  package `uni-db`". CI never sees this (fresh checkout, empty `dist/`), so it is local-only —
  but the failure is quiet in the worst way: the notebooks that run afterwards use whatever
  `maturin develop` left installed (a *debug* build), so the job appears to pass while never
  exercising the release wheel it exists to test. `rm bindings/uni-db/dist/*.whl` before the
  build, and check `importlib.metadata.version('uni-db')` matches the workspace version before
  trusting a notebook run.
- **`maturin develop` needs a `TMPDIR` that exists** — a missing one fails with
  `Failed to create temporary directory ... (os error 2)`. And never judge it through a pipe:
  `maturin develop | tail -N` returns *tail's* exit status, so a failed build looks like success
  and the tests then run against the previous `.so`.
- **Python steps need a venv-scoped `LD_PRELOAD`** of the built extension — see
  `docs/` note on static-TLS exhaustion. A global `export LD_PRELOAD` poisons `uv` and other
  non-Python subprocesses with `undefined symbol: _Py_IncRef`.
- **Python static-TLS (glibc)** — on some boxes `uv run pytest` can fail with
  `ImportError: ... cannot allocate memory in static TLS block` (a large debug `.so` exhausting
  glibc's static-TLS surplus; CI runners have more surplus so they never hit it). If it triggers,
  preload the built lib via the venv interpreter directly — do **not** `export LD_PRELOAD` globally
  (it poisons `uv`/non-python subprocesses):
  ```bash
  SO=bindings/uni-db/uni_db/_uni_db.abi3.so
  PY=bindings/uni-db/.venv/bin/python3
  LD_PRELOAD="$SO" "$PY" -m pytest tests/ -v -n auto
  ```
  This is a last-resort local fix, not part of the workflow.
- **Notebooks** — run serially (see §3); concurrent runs fail spuriously, not from a code bug.
- **Cloud** — always `docker rm -f uni-localstack` when done so the port/container doesn't linger.
- **Cloud: published ports can be unreachable from the host.** On some machines a
  `-p 4566:4566` container is healthy and serving *inside* the container while every
  connection from the host times out (`curl` gives `http_code=000`, and the tests fail with
  `curl: (56) Recv failure: Connection reset by peer` / `failed to create localstack bucket`).
  This is Docker's port-publishing/NAT path, not LocalStack. Confirm with a minimal case —
  `docker run -d -p 8098:80 nginx:alpine`, then `curl 127.0.0.1:8098` from the host versus
  from inside the container. If it reproduces, start LocalStack with **`--network host`**
  (drop `-p`); the container binds 4566 in the host namespace and the NAT path is bypassed.
  Nothing else changes — endpoint, credentials and every S3 code path under test are
  identical, so the run is still valid evidence. Note this is *not* CI's topology (GitHub
  Actions publishes the port normally), so a pass here means "the cloud tests pass against
  LocalStack", not "the cloud job as CI runs it is reproduced".
- **Cloud: check the readiness wait's exit status.** `timeout 120 bash -c 'until curl ...'`
  returns **124** when it gives up, and the snippet above does not abort on it — the tests
  then run against a dead endpoint and fail ~134s later on connection timeouts, which reads
  like a code failure. Capture the status and stop if it is non-zero.
- **Reranker real-ONNX tests** need network (HF). A flaky download is an infra failure, not a code
  failure — re-run before concluding.
