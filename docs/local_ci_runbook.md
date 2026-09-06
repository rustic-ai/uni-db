# Local CI Runbook — replicating the full GitHub CI locally

This document lists every CI job from `.github/workflows/pr.yml` and `.github/workflows/ci.yml`
with the **exact command** to run it locally, plus prerequisites, ordering, and the local-only
gotchas that bite.

> **Source of truth = the workflow YAML.** This runbook mirrors the workflows as of 2026-09-05.
> If a command here disagrees with `.github/workflows/{pr,ci}.yml`, the YAML wins — update this doc.
> **Bump the date above whenever you add a job or change a command.** The stamp is how drift gets
> noticed: it sat at 2026-07-26 through four later edits while three PR-blocking jobs went
> undocumented, which is what #231 reported.
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

# Supply Chain. PINNED in CI: cargo-deny ships new advisory *checks*, so an unpinned
# install lets the same commit pass today and fail tomorrow. Bump deliberately,
# alongside re-testing deny.toml's ignores.
cargo install cargo-deny --version 0.19.8 --locked

# Fuzz Smoke. Needs a nightly toolchain (libFuzzer) in addition to cargo-fuzz.
rustup toolchain install nightly
cargo install cargo-fuzz --locked

# Perf Gate. The runner version must equal crates/uni/Cargo.toml's `iai-callgrind`
# EXACTLY (0.16.1) or it refuses to parse the harness output.
cargo install iai-callgrind-runner --version 0.16.1 --locked

# Python tooling (bindings)
#   install uv:  https://docs.astral.sh/uv/   (CI uses python 3.12)

# System deps CI installs (Debian/Ubuntu names; install equivalents on Fedora)
#   mold, protobuf-compiler, valgrind
#   valgrind is what makes the perf gate's metric deterministic and is in no
#   other job. python3 is needed for scripts/perf/*.py and scripts/ci/*.py.

# Docker — only needed for the Cloud/LocalStack job
```

The pinned `nightly-2026-07-11` toolchain for miri is installed in its own section
below (§2), because it is the one pinned toolchain in the repo and the reason is
specific to that lane.

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
- Heavy builds (provider-onnx static link, the notebooks wheel) are best run in the background while you
  watch a log.

---

## 1. Quick path — a Rust-only change

The jobs a Rust change can actually move. Run these first:

```bash
export RUSTC_WRAPPER=""
cargo fmt --all -- --check
cargo deny check                      # no build, seconds; the advisory DB floats,
                                      # so this can go red with no change to the repo
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

### Supply Chain (cargo-deny)
```bash
# All four checks in one invocation so the summary line reports each:
# `advisories ok, bans ok, licenses ok, sources ok`.
cargo deny check
```
Needs the pinned `cargo-deny 0.19.8` from §0. This lane can go red with **no
change to this repo** — see §5.

### Fuzz Smoke (parsers & codecs)
```bash
# working dir: fuzz (subshell, so a failure does not leave you outside the repo root)
( cd fuzz
for target in cypher_parse locy_parse wal_decode btic_decode; do
  # Corpus-dir order is load-bearing. libFuzzer writes newly-discovered inputs
  # into the FIRST directory listed and treats the rest as read-only, so
  # `corpus/$target` must come first: naming `seeds/$target` first makes the
  # curated, git-tracked seed corpus the output dir, and a single 30 s run
  # buries its handful of regression inputs under a few hundred generated files.
  mkdir -p "corpus/$target"
  seeds=""
  # Only some targets have a seed dir (today: btic_decode alone), and a missing
  # path makes libFuzzer error rather than skip it.
  [ -d "seeds/$target" ] && seeds="seeds/$target"
  cargo +nightly fuzz run --target x86_64-unknown-linux-gnu \
    "$target" "corpus/$target" $seeds -- -max_total_time=30 -timeout=10
done )
```
`--target x86_64-unknown-linux-gnu` is load-bearing: without it a musl-built
cargo-fuzz statically links libc, which AddressSanitizer rejects outright.

The nightly job is the one that finds *new* bugs; this 30 s/target lane exists so
a parser or codec regression fails the PR that introduces it. Replaying the seed
corpus is the real regression check — 30 s of blind mutation mostly is not.

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
  -E 'test(/resilience|recovery|durability|crash_harness/)'

# And the other half of the contract -- the seams must stay inert without the
# feature, since that is what every other job builds:
cargo nextest run -p uni-store -E 'test(/resilience|recovery/)'
```
152 tests, 19 s warm. The cold cost is 3 min 41 s and is almost entirely the
second feature configuration compiling, so expect a slow first run after any
dependency change.

Two things worth knowing:

* **This suite existed for a long time before any CI job ran it.** If you add a
  `fail_point!` seam, add its test to a file matching the filter above, or it
  will be dormant on arrival.
* **A "crash" test that panics and drops the `Uni` is not testing a crash.**
  `Drop for Uni` broadcasts shutdown, and the auto-flush task answers with a
  full `flush_to_l1` that nothing awaits — so the test gets graceful-close
  semantics, racing its own reopen. For real crash semantics use the abort
  harness in `crates/uni/tests/common/crash_harness.rs`: it re-invokes this
  test binary as a child and kills it with `SIGABRT` at the seam. Grep for
  `abort_child` for the pattern. Keep the panic-path test too where the
  graceful path is itself worth pinning.

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
# No `PROPTEST_CASES` here, deliberately -- and this is the same trap the lane
# already documents below. Miri's isolation HIDES the environment, so setting it
# did nothing: the var was silently ignored and the proptests ran at their
# library default. The case count now lives in `miri_safe_config` in
# `crates/uni-sparse-vector/tests/proptest.rs`, in code, where miri can see it.
cargo +nightly-2026-07-11 miri test -p uni-btic --lib --tests
cargo +nightly-2026-07-11 miri test -p uni-sparse-vector --lib --tests

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
Budget ~4 min warm for all three; `uni-sparse-vector`'s proptest target alone was
3 min 17 s on a 22-core box on 2026-09-05 (single sample, so treat it as an order
of magnitude, not a threshold). Two older numbers are still quoted elsewhere and
both understate it: `pr.yml`'s 1 min 41 s predates the sparse proptests actually
running (under isolation they aborted at startup on a blocked `getcwd`, so that
timing covered a target that verified nothing), and this document's own earlier
2 min 08 s predates the case count moving into `miri_safe_config`. The proptest
target dominates; if the lane ever needs trimming, drop `--tests` from
`uni-common` before touching a timeout.

Two miri-isolation traps, both silent, both hit by this lane: `current_dir` is
blocked, which is what aborted the proptest target, and **the environment is
hidden**, so any test tuned through an env var quietly reverts to its library
default. Set such knobs in code under `cfg!(miri)`, not through the job's
`env:`. `muvera` is excluded outright rather
than budgeted -- its tests were killed at 132 minutes. `uni-crdt` runs in
`nightly.yml` only.

A miri failure is real signal even though these crates contain zero `unsafe`:
the UB is reached *through* a dependency, or it is a leak. It has already found
one here -- a `std::mem::forget(TempDir)` leaking a directory on disk every run
(`crates/uni-common/tests/repro_rename_property_bypass.rs:19-27`). If the fault
is upstream, file it and `#[cfg_attr(miri, ignore)]` the single test with a
comment linking the issue. Do not add `-Zmiri-ignore-leaks`, and do not drop the
crate from the lane.

### Perf Gate (instruction counts)
```bash
# valgrind is what makes this metric deterministic, and is in no other PR job.
# RUNS=5 is measured, not chosen: against a 25-sample cross-runner set the worst
# drift of a median with NO code change is 0.997% at 3 runs and 0.599% at 5;
# 7 runs only reaches 0.581% for another 4.6 minutes.
bash scripts/perf/iai_pilot.sh 5

# The gate verifies itself in the same run that trusts it. No lane runs tests
# under scripts/, so a test file alone would never execute -- which is the shape
# the gate was found to have.
python3 scripts/perf/test_iai_gate.py

# Thresholds are explicit arguments -- iai_gate.py has no defaults -- so the
# number a build fails on is visible and traceable to the measurement that
# produced it. `--fail-improve-pct` is the other side: an implausible *drop* is a
# collection failure, and used to pass green.
python3 scripts/perf/iai_gate.py \
  --baseline docs/perf/iai-baseline.json \
  --current target/iai-pilot \
  --fail-pct 2 --warn-pct 1 --fail-improve-pct 50 --markdown
```
Expect ~15 min of cold compile (no other lane builds `--benches`) plus ~12 min of
measurement. The job's 45-minute `timeout-minutes` is the ceiling that budget sits
under, not the expected runtime.

**This lane does not reproduce off a CI runner — see §5 before believing its
result.**

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

## 3. `ci.yml` — main-push thorough suite (the extra lanes ONLY)

`ci.yml` does **not** re-run §2. Its own header says the PR gates (format,
clippy, workspace suite, openCypher TCK schemaless, ruff + pytest) are
"deliberately NOT repeated here", and its `gate` job depends on none of them. To
reproduce a full main-push run you need §2 **and** §3.

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
  -e SERVICES=s3 \
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

### Flagship Notebooks (heaviest; built wheel + neural execution)

Called "the wheel" and not "the release wheel" throughout: the lane builds with
maturin's **dev** profile, so what it exercises is an unoptimized wheel. It is
still the built *wheel* rather than the editable `maturin develop` install, and
that distinction is the one the lane exists to protect.
```bash
( cd bindings/uni-db
  uv sync --group dev --extra notebook-runtime
  rm -f dist/*.whl                         # else the glob below matches two versions
  uv run maturin build --out dist          # NOTE: `dev` profile — maturin only builds
                                           # optimized with an explicit profile flag.
                                           # ci.yml's notebooks job passes none, so the
                                           # notebooks execute an UNOPTIMIZED build.
                                           # The published wheels are unaffected:
                                           # release-wheels.yml passes `--profile dist`
                                           # in every build job (NOT `--release` — see §5).
  uv pip install --force-reinstall dist/*.whl
  # Assert the notebooks will actually run against what was just built. CI makes
  # this an ASSERTION that exits non-zero on a mismatch, not a print -- a stale
  # editable install silently exercises a `maturin develop` debug build instead
  # of the wheel, and the job then passes having tested the wrong artifact.
  uv run --no-sync python -c "
import tomllib, sys
from importlib.metadata import version
want = tomllib.load(open('../../Cargo.toml','rb'))['workspace']['package']['version']
got = version('uni-db')
sys.exit(f'version mismatch: wheel {got!r} != workspace {want!r}') if got != want else print('ok', got)
" )

# Run the 6 notebooks SERIALLY (they fail spuriously under concurrent CPU/GIL load).
# `--no-sync` is REQUIRED: a plain `uv run` re-syncs the project, uninstalls the wheel
# installed above and restores the editable install, so the notebooks would silently
# exercise a `maturin develop` build instead of the built wheel.
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
  uv sync --group dev
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
python3 scripts/ci/check_documented_counts.py     # every documented count claim: plugin
                                                  # surfaces, graph algorithms, both TCK
                                                  # suites' feature files + scenario totals,
                                                  # and the skill reference pages
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
  the editable install — discarding the built wheel before a single notebook runs, so the job
  passes having tested a `maturin develop` build. `ci.yml`'s notebooks job now passes
  `--no-sync` on every `uv run`, so it is no longer exposed; the hazard is live for any command
  you add. Pass `--no-sync` (or invoke `.venv/bin/python3` directly) whenever the installed
  artifact matters.
- **Stale wheels in `bindings/uni-db/dist/` after a version bump** — the notebooks job does
  `uv pip install --force-reinstall dist/*.whl`, and that glob matches *every* wheel ever built
  there. Two versions present makes `uv` refuse with "Requirements contain conflicting URLs for
  package `uni-db`". `ci.yml` guards against it explicitly (`rm -f dist/*.whl`, ci.yml:297) —
  its own comment cites a cached runner or a local replication after a version bump, so this is
  not local-only. The failure is quiet in the worst way: the notebooks that run afterwards use
  whatever `maturin develop` left installed, so the job appears to pass while never exercising
  the wheel it exists to test. Keep the `rm` before the build, and assert
  `importlib.metadata.version('uni-db')` matches the workspace version before trusting a run.
- **`maturin develop` needs a `TMPDIR` that exists** — a missing one fails with
  `Failed to create temporary directory ... (os error 2)`. And never judge it through a pipe:
  `maturin develop | tail -N` returns *tail's* exit status, so a failed build looks like success
  and the tests then run against the previous `.so`.
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
  This is a last-resort local fix, not part of the workflow. Keep the preload **venv-scoped**
  as above: a global `export LD_PRELOAD` poisons `uv` and other non-Python subprocesses with
  `undefined symbol: _Py_IncRef`.
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
- **The perf gate does not reproduce off a CI runner.** Measured 2026-09-05: every
  gated target lands **88–97% below** `docs/perf/iai-baseline.json`, which trips
  `--fail-improve-pct` ("an implausible improvement is a collection failure").
  Before treating that as a regression in your branch, know what has already been
  established, so you do not re-derive it:
  - `baselines::baseline_noop.noop` matches the baseline **exactly** (4 Ir), so
    collection works and the bench binary is not stripped — `[profile.bench]` in
    `.cargo/config.toml` is doing its job.
  - **It reproduces on `origin/main` within ±1.6%**, so it is not introduced by any
    branch. The meaningful comparison for a PR is branch-vs-main, not
    branch-vs-baseline.
  - **The tmpfs theory is falsified.** `Uni::temporary()` honors `TMPDIR`; pointing
    it at a real disk changed nothing (−83.67% either way). Do not re-run that.
  - `--allow-foreign-machine` turns the improvement check off and still fails on
    regressions. It is a **diagnostic**, not the documented command: CI never
    passes it, and a local pass with it means "no regressions on this machine",
    not "the perf gate is green".
  Why a GitHub runner measures ~10x more instructions for identical code is
  unresolved and needs a CI run, not another local experiment. See #230.
- **`cargo deny check` can go red with no change to this repo.** The advisory
  database floats, so a new RUSTSEC entry against a pinned transitive dep turns
  the lane red on a commit that passed yesterday. That is a real finding to
  triage (or add to `deny.toml`'s ignores, deliberately) — not a broken local
  setup, and not something to work around.
- **`fuzz/artifacts/` is gitignored and outlives the run that produced it.** A
  crash file there is not evidence *this* run crashed: check its mtime, and
  replay it (`cargo +nightly fuzz run <target> <artifact>`) before concluding
  anything. The tracked regression inputs live in `fuzz/seeds/<target>`, which is
  a different directory for a different purpose.
