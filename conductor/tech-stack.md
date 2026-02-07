# Technology Stack

## Core Engine
- **Programming Language:** Rust (2024 edition)
- **Concurrency:** Tokio (Async runtime)
- **Error Handling:** `anyhow` and `thiserror`

## Query & Data Processing
- **Query Language:** OpenCypher (Parser based on `sqlparser`)
- **Memory Format:** Apache Arrow (Columnar memory)
- **Query Engine:** DataFusion (Vectorized query execution)
- **Type System:** Custom VID (64-bit) and UniId (Content-addressed SHA3-256)

## Storage Layer
- **On-Disk Format:** Lance (High-performance columnar storage)
- **Database Engine:** LanceDB integration
- **Storage Abstraction:** `object_store` (Native support for Local FS, S3, GCS, Azure)
- **LSM Architecture:** Custom L0 (In-memory) -> L1 (Sorted Runs) -> L2 (Base Lance)

## Connectivity & Integration
- **Rust Library:** `uni_db` (Public Rust API facade)
- **Python Bindings:** PyO3 (Native Rust bindings for Python)
- **OGM Layer:** Pydantic v2 (for Python `uni-pydantic` package)

## Observability & Tooling
- **Logging:** `tracing` and `tracing-subscriber`
- **Metrics:** `metrics` and `metrics-exporter-prometheus`
- **CLI:** `clap` (Rust CLI) and `poetry` (Python dependency management)
- **Testing:** `cargo nextest` (Rust), `pytest` (Python), Cucumber (TCK)