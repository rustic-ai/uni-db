# Installation

This guide covers all methods for installing Uni, from building from source to using pre-built binaries.

## System Requirements

### Minimum Requirements

| Component | Requirement |
|-----------|-------------|
| **OS** | Linux (x86_64, aarch64), macOS (x86_64, Apple Silicon) |
| **Memory** | 4 GB RAM minimum, 16 GB recommended for large graphs |
| **Disk** | SSD recommended for optimal performance |
| **Rust** | 1.75+ (if building from source) |

### Build Dependencies

Building Uni requires several system dependencies:

| Dependency | Purpose | Installation |
|------------|---------|--------------|
| **Rust toolchain** | Compilation | [rustup.rs](https://rustup.rs/) |
| **Clang/LLVM** | Lance native dependencies | System package manager |
| **Protocol Buffers** | Lance serialization | System package manager |
| **pkg-config** | Build configuration | System package manager |
| **OpenSSL** | TLS for object store access | System package manager |

---

## Installation Methods

### Method 1: Build from Source (Recommended)

Building from source provides the latest features and allows customization.

#### Step 1: Install Rust

```bash
# Install rustup (Rust toolchain manager)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Reload your shell configuration
source $HOME/.cargo/env

# Verify installation
rustc --version
cargo --version
```

#### Step 2: Install System Dependencies

**Ubuntu / Debian:**
```bash
sudo apt update
sudo apt install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang \
    llvm
```

**Fedora / RHEL:**
```bash
sudo dnf install -y \
    gcc \
    gcc-c++ \
    pkg-config \
    openssl-devel \
    protobuf-compiler \
    clang \
    llvm
```

**macOS:**
```bash
# Install Homebrew if not present
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install dependencies
brew install protobuf llvm pkg-config openssl@3

# Add LLVM to PATH (for Apple Silicon)
echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

**Arch Linux:**
```bash
sudo pacman -S base-devel pkg-config openssl protobuf clang llvm
```

#### Step 3: Clone and Build

```bash
# Clone the repository
git clone https://github.com/rustic-ai/uni.git
cd uni

# Build in release mode (optimized)
cargo build --release

# The binary is now at target/release/uni
ls -la target/release/uni
```

#### Step 4: Install to PATH (Optional)

```bash
# Option A: Copy to /usr/local/bin
sudo cp target/release/uni /usr/local/bin/

# Option B: Add cargo bin to PATH
echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.bashrc
cargo install --path .
```

---

### Method 2: Using Cargo Install

If you only need the CLI and don't require source modifications:

```bash
# Install directly from the repository
cargo install --git https://github.com/rustic-ai/uni.git

# Or from crates.io (when published)
cargo install uni
```

---

### Method 3: Docker (Planned)

Container images and a supported Docker workflow are not published yet. If you need containerization today, build Uni from source inside your own base image and run it via the CLI.

---

## Verification

After installation, verify Uni is working correctly:

### Check Version

```bash
uni --version
# Output: uni 0.1.0
```

### Display Help

```bash
uni --help
```

Expected output:
```
Uni - The Embedded Multi-Model Graph Database

Usage: uni <COMMAND>

Commands:
  import    Import data from JSONL
  query     Execute a Cypher query
  repl      Start the interactive REPL
  snapshot  Manage snapshots
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

Uni supports optional features that can be enabled during compilation:

| Feature | Description | Default |
|---------|-------------|---------|
| `candle-text` | Native Rust embedding models (Candle) | Enabled |
| `fastembed` | ONNX-based embedding models (legacy) | Disabled |
| `s3` | Amazon S3 object store | Enabled |
| `gcs` | Google Cloud Storage | Disabled |
| `azure` | Azure Blob Storage | Disabled |

### Custom Build Example

```bash
# Minimal build (local filesystem only)
cargo build --release --no-default-features

# Build with all cloud providers
cargo build --release --features "s3,gcs,azure"

# Build without embedding support (smaller binary)
cargo build --release --no-default-features --features "s3"
```

---

## Development Setup

For contributing to Uni, set up the full development environment:

```bash
# Clone with submodules
git clone --recursive https://github.com/rustic-ai/uni.git
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
