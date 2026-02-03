# Reference

Complete reference documentation for Uni.

<div class="feature-grid">

<div class="feature-card">

### [Rust API](rust-api.md)
Programmatic access to Uni from Rust applications.

</div>

<div class="feature-card">

### [Python API](python-api.md)
Python bindings for direct database access.

</div>

<div class="feature-card">

### [Pydantic OGM](pydantic-ogm.md)
Type-safe Python models with Pydantic validation.

</div>

<div class="feature-card">

### [Configuration](configuration.md)
All configuration options for storage, runtime, and queries.

</div>

<div class="feature-card">

### [Troubleshooting](troubleshooting.md)
Common issues, error messages, and solutions.

</div>

<div class="feature-card">

### [Glossary](glossary.md)
Terminology and abbreviations used in Uni documentation.

</div>

</div>

## Quick Reference

### Common Configuration

```rust
use uni_db::UniConfig;

let mut config = UniConfig::default();
config.cache_size = 1_000_000_000; // 1 GB
config.parallelism = 4;
```

### Supported Data Types

| Type | Description | Example |
|------|-------------|---------|
| `String` | UTF-8 text | `"hello"` |
| `Int32` | 32-bit integer | `42` |
| `Int64` | 64-bit integer | `9223372036854775807` |
| `Float32` | 32-bit float | `3.14` |
| `Float64` | 64-bit float | `3.141592653589793` |
| `Boolean` | True/false | `true` |
| `Vector[N]` | N-dimensional float vector | `[0.1, 0.2, 0.3]` |
| `Json` | Nested JSON document | `{"key": "value"}` |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Log level (`uni=debug`, `uni_db::storage=trace`) |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | AWS region for S3 access |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | AWS credentials |
| `AWS_SESSION_TOKEN` | AWS session token (optional) |
| `AWS_ENDPOINT_URL` | Custom S3 endpoint (MinIO/LocalStack) |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCP service account JSON path |
| `AZURE_STORAGE_ACCOUNT` | Azure storage account |
| `AZURE_STORAGE_ACCESS_KEY` | Azure access key |
| `AZURE_STORAGE_SAS_TOKEN` | Azure SAS token |

## Next Steps

- For API details, see [Rust API](rust-api.md)
- For tuning options, see [Configuration](configuration.md)
- Having issues? Check [Troubleshooting](troubleshooting.md)
