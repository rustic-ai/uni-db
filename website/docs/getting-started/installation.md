# Installation

This guide covers all methods for installing Uni.

## System Requirements

| Component | Requirement |
|-----------|-------------|
| **OS** | Linux (x86_64, aarch64), macOS (x86_64, Apple Silicon) |
| **Memory** | 4 GB RAM minimum, 16 GB recommended for large graphs |
| **Disk** | SSD recommended for optimal performance |

---

## Rust Library

Add `uni-db` to your project from [crates.io](https://crates.io/crates/uni-db):

```bash
cargo add uni-db
```

Or add it to your `Cargo.toml` directly:

```toml
[dependencies]
uni-db = "3"
```

!!! tip "Enable mimalloc for ~3× throughput on mutation-heavy workloads"
    Allocation-heavy workloads (many small Cypher `CREATE`/`MERGE` statements, concurrent writers) bottleneck on the default glibc allocator. Opt in to mimalloc:

    ```toml
    [dependencies]
    uni-db = { version = "*", features = ["mimalloc"] }
    ```

    ```rust
    // in your binary's main.rs:
    #[global_allocator]
    static GLOBAL: uni_db::MiMalloc = uni_db::MiMalloc;
    ```

    Measured: `concurrent_mutations` bench wall time at sess=24 drops from **1012 ms → 394 ms** (2.5×). See the [Performance Tuning Guide](../guides/performance-tuning.md#use-mimalloc-as-global-allocator) for the full breakdown.

---

## Python Package

Install from [PyPI](https://pypi.org/project/uni-db/). The default wheel bundles 12 providers (candle, mistralrs, ONNX, 8 hosted APIs, and the llama.cpp embedding-server adapter) and runs on CPU:

```bash
pip install uni-db
```

!!! info "mimalloc included by default"
    Every wheel ships with mimalloc as the Rust-side global allocator — no configuration needed. Python's own `PyMem_*` allocator is untouched; only Rust allocations (the entire Cypher pipeline) route through mimalloc. Mutation-heavy workloads see ~3× throughput vs. glibc.

For the Pydantic OGM layer:

```bash
pip install uni-pydantic
```

### Wheel variants

Uni ships **6 wheel variants** using uni-xervo's three-axis capability matrix (provider × linking × acceleration). Pick by hardware first, then by whether you need local LLM inference:

| Wheel | Local providers | Accelerator |
|---|---|---|
| `uni-db` *(default)* | candle + mistralrs + ONNX | CPU |
| `uni-db-onnx` *(slim)* | ONNX only | CPU |
| `uni-db-cuda` | candle + mistralrs + ONNX | NVIDIA CUDA |
| `uni-db-metal` | candle + mistralrs + ONNX | Apple GPU/ANE (Apple Silicon) |
| `uni-db-onnx-cuda` | ONNX only | NVIDIA CUDA |
| `uni-db-onnx-metal` | ONNX only | Apple GPU/ANE |

```bash
pip install uni-db-cuda          # Linux/Windows + NVIDIA GPU
pip install uni-db-metal         # Apple Silicon Mac
pip install uni-db-onnx          # smaller wheel — drops candle/mistralrs
```

The Python API is identical across all six. Programmatic recommendation:

```python
from uni_db import recommend
print(recommend())   # e.g. "uni-db-cuda" on a Linux NVIDIA host
```

CUDA wheels require an NVIDIA driver supporting the bundled CUDA toolkit version, plus cuDNN ≥ 9 on the host loader path (not bundled — typically `/usr/local/cuda-X.X/...`). Metal wheels need a supported macOS arm64 host; CoreML/Metal frameworks ship with the OS.

---

## CLI (Build from Source)

The CLI is not published as a standalone binary. Build it from source:

### Step 1: Install Rust and System Dependencies

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**System dependencies** (needed for Lance native code):

=== "Ubuntu / Debian"

    ```bash
    sudo apt update
    sudo apt install -y build-essential pkg-config libssl-dev protobuf-compiler clang llvm
    ```

=== "Fedora / RHEL"

    ```bash
    sudo dnf install -y gcc gcc-c++ pkg-config openssl-devel protobuf-compiler clang llvm
    ```

=== "macOS"

    ```bash
    brew install protobuf llvm pkg-config openssl@3
    # Apple Silicon: add LLVM to PATH
    echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
    source ~/.zshrc
    ```

=== "Arch Linux"

    ```bash
    sudo pacman -S base-devel pkg-config openssl protobuf clang llvm
    ```

### Step 2: Clone and Build

```bash
git clone https://github.com/rustic-ai/uni-db.git
cd uni
cargo build --release
```

### Step 3: Install to PATH (Optional)

```bash
# Option A: Copy to /usr/local/bin
sudo cp target/release/uni /usr/local/bin/

# Option B: Use cargo install
cargo install --path crates/uni-cli
```

---

## Verification

After installation, verify Uni is working correctly:

### Check Version

```bash
uni --version
# Output: uni 2.0.0
```

### Display Help

```bash
uni --help
```

Expected output:
```
Uni Graph Database

Usage: uni <COMMAND>

Commands:
  import    Import data from JSONL
  query     Execute a Cypher query
  repl      Start the interactive REPL
  snapshot  Manage snapshots
  plugin    Manage runtime-loaded plugins
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### Run a Simple Query

```bash
# Create a test directory
mkdir -p /tmp/uni-test

# Run a query (will create empty storage)
uni query "RETURN 1 + 1 AS result" --path /tmp/uni-test

# Expected output:
# ┌────────┐
# │ result │
# ├────────┤
# │ 2      │
# └────────┘
```

---

## Troubleshooting Installation

### Common Issues

#### "protoc not found"

```bash
# Ubuntu/Debian
sudo apt install protobuf-compiler

# macOS
brew install protobuf

# Verify
protoc --version
```

#### "failed to run custom build command for `ring`"

This usually indicates missing C compiler or LLVM:

```bash
# Ubuntu/Debian
sudo apt install build-essential clang

# macOS
xcode-select --install
```

#### "openssl not found"

```bash
# Ubuntu/Debian
sudo apt install libssl-dev pkg-config

# macOS
brew install openssl@3
export OPENSSL_DIR=$(brew --prefix openssl@3)
```

#### Slow Compilation

Enable parallel compilation and use the `mold` linker:

```bash
# Install mold (Linux)
sudo apt install mold

# Configure Cargo to use mold
cat >> ~/.cargo/config.toml << EOF
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
EOF

# Rebuild
cargo build --release
```

---

## Feature Flags

`uni-db` follows uni-xervo's three-axis capability model: **provider** × **ONNX linking** × **acceleration**.

### Provider features

The Rust crate enables the providers below by default, except the alternative
`provider-onnx-dynamic` linking mode. Disable defaults and select providers
explicitly for a smaller build. Standard Python wheels inherit these defaults;
slim ONNX variants select ONNX and the eight hosted APIs.

| Feature | Backend | Tasks |
|---|---|---|
| `provider-candle` | HuggingFace Candle | Local embeddings |
| `provider-mistralrs` | mistral.rs | Local generation, multimodal, embeddings |
| `provider-onnx` | ONNX Runtime (bundled, statically linked) | Local embed, rerank, raw tensor |
| `provider-onnx-dynamic` | ONNX Runtime (BYO, `dlopen` at runtime via `ORT_DYLIB_PATH`) | Same as `provider-onnx`, mutually exclusive |
| `provider-openai` | OpenAI | Remote embed + generation |
| `provider-gemini` | Google Gemini | Remote embed + generation |
| `provider-vertexai` | Google Vertex AI | Remote embed + generation |
| `provider-mistral` | Mistral API | Remote embed + generation |
| `provider-anthropic` | Anthropic | Remote generation |
| `provider-voyageai` | Voyage AI | Remote embed + rerank |
| `provider-cohere` | Cohere | Remote embed + rerank + generation |
| `provider-azure-openai` | Azure OpenAI | Remote embed + generation |
| `provider-llamacpp` | llama.cpp server (`remote/llamacpp`) | Remote embeddings with tokenizer-aware input truncation |

### Acceleration features

| Feature | Hardware |
|---|---|
| `gpu-cuda` | NVIDIA CUDA (Linux + Windows). Activates ORT CUDA EP and the `candle?/cuda` and `mistralrs?/cuda` kernels for any local provider also enabled. |
| `gpu-metal` | Apple GPU/ANE (macOS). Activates the ORT CoreML EP and the `candle?/metal` and `mistralrs?/metal` kernels. |

The previous nine `gpu-*` features (`gpu-tensorrt`, `gpu-rocm`, `gpu-coreml`, `gpu-directml`, `gpu-openvino`, `gpu-qnn`, `gpu-wgpu`, plus the two above) collapsed to two in `uni-db 2.0.0`. The retired EPs remain reachable via `provider-onnx-dynamic` plus a vendor-supplied ORT shared library at runtime (`ORT_DYLIB_PATH`).

### Backend features

| Feature | Description | Default |
|---|---|---|
| `lance-backend` | Lance columnar storage backend. **Effectively mandatory** — Lance is currently the only `StorageBackend` implementation, and building without this feature does not compile. Keep it enabled; it is listed here for completeness, not as a supported toggle. | Enabled |
| `mimalloc` | Re-export `mimalloc::MiMalloc` as `uni_db::MiMalloc` so consumers can install it as the global allocator (~3× throughput on mutation-heavy workloads). Setting `#[global_allocator]` is still the binary's job. | Disabled |
| `snapshot-internals` | Expose snapshot internals (advanced) | Disabled |
| `storage-internals` | Expose storage internals (advanced) | Disabled |

### Custom build examples

```bash
# Default — Lance, built-in plugins, and all default model providers
cargo build --release

# Slim — no providers at all
cargo build --release --no-default-features --features lance-backend

# Everything-CPU (matches the default `uni-db` wheel)
cargo build --release \
  --features "provider-candle,provider-mistralrs,provider-onnx,\
provider-openai,provider-gemini,provider-vertexai,provider-mistral,\
provider-anthropic,provider-voyageai,provider-cohere,provider-azure-openai"

# Everything + NVIDIA CUDA (matches the `uni-db-cuda` wheel)
cargo build --release \
  --features "provider-candle,provider-mistralrs,gpu-cuda,\
provider-openai,provider-gemini,provider-vertexai,provider-mistral,\
provider-anthropic,provider-voyageai,provider-cohere,provider-azure-openai"

# BYO ORT (e.g. AMD ROCm, Intel OpenVINO, DirectML)
cargo build --release \
  --no-default-features \
  --features "lance-backend,provider-onnx-dynamic,provider-openai"
# Then at runtime:
export ORT_DYLIB_PATH=/path/to/your/libonnxruntime.so
```

---

## Development Setup

For contributing to Uni, set up the full development environment:

```bash
# Clone with submodules
git clone --recursive https://github.com/rustic-ai/uni-db.git
cd uni

# Install development tools
cargo install cargo-watch cargo-nextest

# Run tests
cargo nextest run

# Run with hot-reload during development
cargo watch -x "run -- query 'RETURN 1'"

# Check code quality
cargo fmt --check
cargo clippy -- -D warnings
```

---

## Next Steps

Now that Uni is installed:

1. **[Quick Start](quickstart.md)** — Import data and run your first queries
2. **[CLI Reference](cli-reference.md)** — Learn all available commands
3. **[Data Model](../concepts/data-model.md)** — Understand vertices, edges, and properties
