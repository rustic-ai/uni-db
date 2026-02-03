# Getting Started

Get up and running with Uni in minutes.

<div class="feature-grid">

<div class="feature-card">

### [Installation](installation.md)
Build Uni from source, install prerequisites, and verify your setup.

</div>

<div class="feature-card">

### [Quick Start](quickstart.md)
Create your first graph, import data, and run queries in 5 minutes.

</div>

<div class="feature-card">

### [CLI Reference](cli-reference.md)
Complete documentation for all Uni command-line tools.

</div>

</div>

## Prerequisites

Before installing Uni, ensure you have:

- **Rust 1.75+** — Install via [rustup](https://rustup.rs/)
- **8GB+ RAM** — Recommended for development
- **Git** — For cloning the repository

## Quick Install

```bash
# Clone and build
git clone https://github.com/rustic-ai/uni.git
cd uni
cargo build --release

# Verify installation
./target/release/uni --version
```

## Next Steps

Once installed, follow the [Quick Start](quickstart.md) guide to create your first graph database.
