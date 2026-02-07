# Embedding Service Design

This document captures the design decisions for Uni's embedding infrastructure, including the migration from FastEmbed (ONNX) to Candle (native Rust).

## Overview

Uni supports auto-embedding for vector indexes, allowing users to define text source columns that are automatically converted to vector embeddings during data ingestion and query time.

```sql
CREATE VECTOR INDEX doc_embed_idx
FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        provider: 'Candle',
        model: 'all-MiniLM-L6-v2',
        source: ['content']
    }
}
```

## Architecture

### Current Implementation (Candle-based)

```
┌─────────────────────────────────────────────────────────────┐
│                    EmbeddingService (trait)                  │
│  async fn embed(&self, texts: &[&str]) -> Vec<Vec<f32>>     │
│  fn dimensions(&self) -> u32                                 │
│  fn model_name(&self) -> &str                                │
└─────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│CandleTextEmbed  │  │  OpenAI (future)│  │ FastEmbedService│
│ (default)       │  │                 │  │ (opt, legacy)   │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
 candle-transformers    (not yet impl)        fastembed
   (BERT/MiniLM)                              (ONNX)
```

**Key files:**
- `crates/uni-store/src/embedding/candle_text.rs` - CandleTextEmbedding implementation
- `crates/uni-store/src/embedding/service.rs` - EmbeddingService trait and factory functions
- `crates/uni-store/src/runtime/writer.rs` - Embedding orchestration at L0 write time
- `crates/uni-query/src/query/executor/procedure.rs` - Query-time embedding for vector search

### Embedding Flow

```
INSERT vertex with text
         │
         ▼
┌─────────────────────────────┐
│ process_embeddings_for_batch│  ← Embeddings generated HERE (pre-L0)
│ - Batch all texts by config │
│ - Single API call per model │
└─────────────────────────────┘
         │
         ▼
    L0 Buffer (in-memory, includes embeddings)
         │
         ▼
    flush_to_l1() → LanceDB tables
```

## Embedding Providers

### Candle (Default)

Candle is HuggingFace's native Rust ML framework. It provides:

- **No FFI overhead**: Pure Rust implementation
- **No stack overflow issues**: Unlike ONNX Runtime, no 8MB stack workaround needed
- **Memory-mapped models**: Uses safetensors format for efficient loading
- **HuggingFace Hub integration**: Automatic model download and caching

**Supported Models:**

| Model | Dimensions | Speed | Use Case |
|-------|------------|-------|----------|
| all-MiniLM-L6-v2 | 384 | Fastest | Default, general text |
| bge-small-en-v1.5 | 384 | Fast | High quality English |
| bge-base-en-v1.5 | 768 | Medium | Higher quality English |

**Usage:**
```cypher
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    embedding: {
        provider: 'Candle',
        model: 'all-MiniLM-L6-v2',
        source: ['content']
    }
}
```

### FastEmbed (Legacy, Optional)

FastEmbed uses ONNX Runtime. It requires the `fastembed` feature flag:

```bash
cargo build --features fastembed
```

**Note:** FastEmbed requires an 8MB stack workaround due to ONNX Runtime's memory requirements. Candle does not have this limitation.

**Usage:**
```cypher
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    embedding: {
        provider: 'FastEmbed',
        model: 'AllMiniLML6V2',
        source: ['content']
    }
}
```

## Feature Flags

```toml
[features]
default = ["candle-text"]

# Text embeddings using Candle (default)
candle-text = ["dep:candle-core", "dep:candle-nn", "dep:candle-transformers", "dep:tokenizers", "dep:hf-hub"]

# GPU acceleration via CUDA (optional)
candle-cuda = ["candle-core/cuda", "candle-nn/cuda", "candle-transformers/cuda"]

# Legacy ONNX-based embeddings (optional)
fastembed = ["dep:fastembed"]
```

## Model Cache

Models are cached in `~/.uni/models/` by default. This can be configured via:
- `UNI_MODEL_CACHE` environment variable
- Database configuration option (future)

On first use, models are downloaded from HuggingFace Hub.

## Schema Configuration

Embedding configuration is stored in the schema:

```rust
pub enum EmbeddingModel {
    // Candle-based (default, native Rust)
    Candle {
        model_name: String,        // e.g., "all-MiniLM-L6-v2"
        revision: Option<String>,  // HF model revision
    },
    // Legacy ONNX-based (requires fastembed feature)
    FastEmbed {
        model_name: String,
        cache_dir: Option<String>,
        max_length: Option<usize>,
    },
    // Cloud providers (future)
    OpenAI { model: String, api_key_env: String, dimensions: Option<u32> },
    Ollama { model: String, host: String },
}

pub struct EmbeddingConfig {
    pub model: EmbeddingModel,
    pub source_properties: Vec<String>,
    pub batch_size: usize,
}

pub struct VectorIndexConfig {
    pub name: String,
    pub label: String,
    pub property: String,
    pub index_type: VectorIndexType,
    pub metric: DistanceMetric,
    pub embedding_config: Option<EmbeddingConfig>,  // Auto-embed config
}
```

## Migration from FastEmbed

### Why Migrate?

1. **Stack overflow fix**: ONNX Runtime requires 8MB stack, causing issues for library consumers
2. **Lighter weight**: Candle has fewer dependencies than ONNX Runtime
3. **Native Rust**: Better integration with the Rust ecosystem
4. **Memory efficiency**: Safetensors memory-mapping

### Migration Guide

**Before (FastEmbed):**
```cypher
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    embedding: {
        provider: 'fastembed',
        model: 'AllMiniLML6V2',
        source: ['content']
    }
}
```

**After (Candle):**
```cypher
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    embedding: {
        provider: 'Candle',
        model: 'all-MiniLM-L6-v2',
        source: ['content']
    }
}
```

**Model Name Mapping:**

| FastEmbed Model | Candle Model |
|-----------------|--------------|
| AllMiniLML6V2 | all-MiniLM-L6-v2 |
| BGESmallENV15 | bge-small-en-v1.5 |
| BGEBaseENV15 | bge-base-en-v1.5 |

## LanceDB Embedding Investigation

We investigated whether to adopt LanceDB's embedding infrastructure. Key findings:

1. **No query-time convenience in Rust**: LanceDB's Rust SDK requires manual embedding calls
2. **Python-only auto-embed**: The `table.search("text")` convenience only exists in Python
3. **Different embedding timing**: LanceDB embeds at flush time; we embed at L0 write time

See the original investigation notes in git history for full details.

## Future Considerations

### Adding Cloud Embeddings

When adding OpenAI/Ollama support:
1. Add `OpenAIEmbeddingService` implementing `EmbeddingService` trait
2. Add `OllamaEmbeddingService` for local LLM embeddings
3. Use `create_embedding_service()` factory for unified creation

### GPU Acceleration

Enable CUDA support with:
```bash
cargo build --features candle-cuda
```

This requires CUDA toolkit installed on the system.

### Multimodal Embeddings (Future)

Phase 2 could add image embeddings via Candle:
- CLIP (ViT-B/32) for image+text
- BLIP for image captioning
- PaliGemma for advanced multimodal

## Conclusion

The Candle-based embedding implementation provides:

1. **Native Rust execution** - no FFI, no stack issues
2. **Efficient model loading** - memory-mapped safetensors
3. **Automatic model download** - HuggingFace Hub integration
4. **Feature-gated legacy support** - FastEmbed still available via feature flag
