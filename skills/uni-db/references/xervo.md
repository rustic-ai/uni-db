# Xervo ML Runtime Reference

## 1. Overview

Xervo is uni-db's built-in ML runtime for embeddings, reranking, text generation and structured NLP (entity and fact extraction). It supports local inference (Candle, ONNX Runtime — embed, rerank, raw, nlp), a self-hosted llama.cpp server, and remote APIs (OpenAI, Anthropic, Gemini, Mistral, Cohere, Voyage AI, Vertex AI, Azure OpenAI). Models are configured via a **catalog** of alias specs, and accessed through the `db.xervo()` facade.

**Key concepts:**
- **Alias**: human-readable model identifier in `task/name` format (e.g., `embed/default`, `llm/gpt4`)
- **Provider**: inference backend (`local/candle`, `remote/openai`, etc.)
- **Catalog**: JSON array of model alias specs loaded at database startup
- **Auto-embedding**: vector indexes can automatically embed text on write using a catalog alias

---

## 2. Providers

### Local Providers

Run inference on the host machine. No API keys needed.

| Provider ID | Engine | Tasks | Best For |
|---|---|---|---|
| `remote/llamacpp` | llama.cpp server (`llama-server` you run) | Embed, Rerank, Generate, Document extract | Private local generation (text + vision), GGUF models, GPU via llama.cpp; one server per model, so one alias per task |
| `local/candle` | HuggingFace Candle | Embed | Lightweight CPU embeddings (Bert, JinaBert, Gemma) |
| `local/onnx` | ONNX Runtime | Embed, Rerank, Raw, Nlp | Fast CPU embeddings (FastEmbed alias strings supported), cross-encoder reranking (BERT-style), raw tensor execution, and kniv v7 entity/fact extraction (`uni.nlp.*`) |

**Local provider options:**

```json
// candle / onnx
{ "cache_dir": "/path/to/model/cache" }

// onnx: device placement (GPU wheels default to the GPU, then CPU)
{ "execution_providers": ["coreml", "cpu"] }  // or ["cuda", "cpu"], ["cpu"]

// llamacpp: point the alias at a running llama-server
{ "base_url": "http://localhost:8080" }
```

`local/mistralrs` was removed in uni-xervo 0.19. For local generation, run a `llama-server` and use `remote/llamacpp`; for local embeddings, use `local/onnx` or `local/candle`. Image generation and speech synthesis have no replacement.

On a GPU wheel, an ONNX model that the accelerator cannot run (Qwen3 decoder rerankers and embedders on CoreML, for example) fails a probe at load and falls back to CPU with a warning. A list without `"cpu"`, such as `["coreml"]`, makes that a load error instead.

### Remote Providers

Call external APIs. Require API keys via environment variables.

| Provider ID | Tasks | Default Env Var | Auth Header |
|---|---|---|---|
| `remote/openai` | Embed, Generate | `OPENAI_API_KEY` | `Authorization: Bearer` |
| `remote/anthropic` | Generate only | `ANTHROPIC_API_KEY` | `x-api-key` |
| `remote/gemini` | Embed, Generate | `GEMINI_API_KEY` | Query parameter |
| `remote/vertexai` | Embed, Generate | `VERTEXAI_API_TOKEN` | `Authorization: Bearer` |
| `remote/mistral` | Embed, Generate | `MISTRAL_API_KEY` | `Authorization: Bearer` |
| `remote/voyageai` | Embed, Rerank | `VOYAGEAI_API_KEY` | `Authorization: Bearer` |
| `remote/cohere` | Embed, Rerank, Generate | `CO_API_KEY` | `Authorization: Bearer` |
| `remote/azure-openai` | Embed, Generate | `AZURE_OPENAI_API_KEY` | `api-key` header |

**Provider-specific options:**

```json
// All remote providers — custom API key env var
{ "api_key_env": "MY_CUSTOM_KEY" }

// Anthropic
{ "anthropic_version": "2023-06-01" }

// Vertex AI
{
  "api_token_env": "MY_TOKEN",
  "project_id": "my-gcp-project",
  "location": "us-central1",
  "publisher": "google",
  "embedding_dimensions": 768
}

// Cohere
{ "input_type": "search_document" }

// Azure OpenAI
{
  "resource_name": "my-azure-resource",
  "api_version": "2024-02-01"
}
```

---

## 3. Catalog Format

The catalog is a JSON array of model alias specs:

```json
[
  {
    "alias": "embed/default",
    "task": "embed",
    "provider_id": "local/onnx",
    "model_id": "all-MiniLM-L6-v2",
    "revision": null,
    "warmup": "lazy",
    "required": false,
    "timeout": null,
    "load_timeout": null,
    "retry": null,
    "options": {}
  },
  {
    "alias": "llm/default",
    "task": "generate",
    "provider_id": "remote/openai",
    "model_id": "gpt-4o-mini",
    "warmup": "lazy",
    "required": false,
    "timeout": 30,
    "retry": { "max_attempts": 3, "initial_backoff_ms": 200 },
    "options": {}
  }
]
```

### Field Reference

| Field | Required | Default | Description |
|---|---|---|---|
| `alias` | Yes | — | Identifier in `task/name` format (must contain `/`) |
| `task` | Yes | — | Lowercase snake_case: `"embed"`, `"generate"`, `"rerank"`, `"embed_sparse"`, `"embed_multi_vector"`, `"embed_hybrid"`, `"raw"`, `"nlp"`, … (`"Embed"` is rejected with `unknown variant`) |
| `provider_id` | Yes | — | Provider identifier (e.g., `"remote/openai"`) |
| `model_id` | Yes | — | HuggingFace repo ID (local) or API model name (remote) |
| `revision` | No | `null` | HuggingFace branch/tag/commit hash (local only) |
| `warmup` | No | `"lazy"` | `"lazy"`, `"eager"`, or `"background"` |
| `required` | No | `false` | If true, failed eager warmup aborts database startup |
| `timeout` | No | `null` | Per-inference timeout in seconds |
| `load_timeout` | No | `600` | Model load timeout in seconds |
| `retry` | No | `null` | `{ "max_attempts": N, "initial_backoff_ms": N }` |
| `options` | No | `{}` | Provider-specific JSON (see provider sections above) |

### Warmup Policies

| Policy | Behavior |
|---|---|
| `Lazy` | Load model on first inference request (default) |
| `Eager` | Load during database startup; blocks until loaded. If `required: true`, startup fails on error |
| `Background` | Spawn background load at startup; requests before load completes will wait |

### Model Deduplication

Multiple aliases pointing to the same `(provider_id, model_id, revision, options)` share a single loaded model instance.

---

## 4. Configuration

### Wheels and feature flags

The Python wheels (since `uni-db 1.2.0`) bundle providers per the wheel matrix — pick the wheel whose providers/accelerator match your deployment:

| Wheel | Local providers | Accelerator |
|---|---|---|
| `uni-db` *(default)* | `local/candle` + `local/onnx` | CPU |
| `uni-db-onnx` *(slim)* | `local/onnx` only | CPU |
| `uni-db-cuda` | `local/candle` + `local/onnx` | NVIDIA CUDA |
| `uni-db-metal` | `local/candle` + `local/onnx` | Apple GPU/ANE |
| `uni-db-onnx-cuda` | `local/onnx` only | NVIDIA CUDA |
| `uni-db-onnx-metal` | `local/onnx` only | Apple GPU/ANE |

All 8 hosted-API providers (`remote/openai`, `remote/gemini`, etc.) and `remote/llamacpp` are bundled in every wheel. The Python API is identical across all six. The Rust defaults enable all providers, including `remote/llamacpp`. To reduce dependencies, disable defaults and select `lance-backend` plus the required provider features. Acceleration: `gpu-cuda` (NVIDIA), `gpu-metal` (Apple). Other ORT execution providers (TensorRT, ROCm, DirectML, OpenVINO, QNN) are reachable via `provider-onnx-dynamic` + `ORT_DYLIB_PATH`.

### Python

```python
from uni_db import Uni, UniBuilder

# From JSON file
db = UniBuilder.open("./my_db") \
    .xervo_catalog_from_file("./catalog.json") \
    .build()

# From JSON string
catalog_json = '[{"alias": "embed/default", "task": "embed", ...}]'
db = UniBuilder.open("./my_db") \
    .xervo_catalog_from_str(catalog_json) \
    .build()
```

### Rust

```rust
use uni_db::Uni;

// From catalog vector
let db = Uni::open("./my_db")
    .xervo_catalog(catalog_vec)
    .build()
    .await?;

// From JSON file
let db = Uni::open("./my_db")
    .xervo_catalog_from_file("./catalog.json")?
    .build()
    .await?;

// From JSON string
let db = Uni::open("./my_db")
    .xervo_catalog_from_str(&json_string)?
    .build()
    .await?;
```

### Environment Variables

Set API keys before opening the database:

```bash
export OPENAI_API_KEY="sk-..."
export ANTHROPIC_API_KEY="sk-ant-..."
export GEMINI_API_KEY="..."
```

Or use custom env var names per alias:

```json
{ "options": { "api_key_env": "MY_PROJECT_OPENAI_KEY" } }
```

---

## 5. Usage API

### Embeddings

```python
xervo = db.xervo()

# Embed a batch of texts
vectors = xervo.embed("embed/default", [
    "Graph databases store relationships.",
    "Vector search enables semantic retrieval."
])
# vectors: list[list[float]], one vector per input text

# Async
vectors = await async_db.xervo().embed("embed/default", texts)
```

### Text Generation

```python
from uni_db import Message

xervo = db.xervo()

# Simple text prompt
result = xervo.generate_text("llm/default", "What is a graph database?")
print(result.text)

# With options
result = xervo.generate_text(
    "llm/default",
    "Explain HNSW indexing.",
    max_tokens=500,
    temperature=0.3,
    top_p=0.9
)

# Structured messages
result = xervo.generate("llm/default", [
    Message.system("You are a graph database expert."),
    Message.user("What is the difference between BFS and DFS?"),
])
print(result.text)
print(result.usage)  # TokenUsage(prompt_tokens=..., completion_tokens=..., total_tokens=...)

# Dict messages also accepted
result = xervo.generate("llm/default", [
    {"role": "system", "content": "Be concise."},
    {"role": "user", "content": "What is PageRank?"},
])
```

### Prefetching (Best Practice)

**Always prefetch models at startup** to avoid cold-start latency on first inference. Without prefetch, the first call to `embed()`, `generate()`, or `rerank()` pays the full model load cost (download, session initialization, warmup). Subsequent calls are served from cache.

```python
xervo = db.xervo()

# Prefetch specific aliases (recommended — explicit and selective)
xervo.prefetch(["embed/default", "llm/default"])

# Or prefetch everything in the catalog
xervo.prefetch_all()

# Async equivalents
await async_db.xervo().prefetch(["embed/default"])
await async_db.xervo().prefetch_all()
```

```rust
// Rust
db.xervo().prefetch(&["embed/default", "llm/default"]).await?;
db.xervo().prefetch_all().await?;
```

Both methods are **awaitable** and **fail-fast** — they return when all requested models are fully loaded, or error on the first failure. Models already loaded are skipped.

**When to use `prefetch()` vs `warmup: "eager"`:**
- Use **`warmup: "eager"`** in the catalog when the model is always needed and you want startup to fail if loading fails (`required: true`)
- Use **`prefetch()`** after startup when you want programmatic control over which models to load and when (e.g., load models conditionally based on runtime config, or load after other initialization is complete)

### Availability Check

```python
if db.xervo().is_available():
    vectors = db.xervo().embed("embed/default", texts)
else:
    # No catalog configured — use fallback
    pass
```

### NLP Extraction

A kniv v7 model under an `nlp` alias (default `nlp/default`) extracts typed
entities and the facts between them, locally, with no LLM. Relations and
entity types are given per call by name and description.

```python
facts = db.xervo().extract_facts(
    [{"speaker": "user", "text": "I moved to Lisbon last year."}],   # or a str / list[str]
    relations=[{"name": "lives in", "description": "the place where the head resides"}],
    entity_types=["location", "organization"],
)
for f in facts:
    print(f.head_text, f.relation, f.tail_text, f.polarity, f.modality, f.margin)

entities = db.xervo().extract_entities("Ada Lovelace met Babbage in London.")
```

Same rows from Cypher (they reach Python through `session.query` too):

```cypher
CALL uni.nlp.extract($text, {schema: 'graph'})          // relations from declared edge types
YIELD head_text, relation, tail_text, polarity, modality, margin, tail_start, tail_end
WHERE modality IN ['asserted', 'reported']
RETURN *

CALL uni.nlp.entities($text, {entity_types: ['person']})
YIELD entity, text, entity_type, mentions, mention_starts, mention_ends RETURN *
```

- **Options:** `alias`, `relations`, `entity_types`, `schema` (`'graph'` or
  `{labels, edge_types}`, exclusive with the previous two), `top_per_pair`,
  `rel_offsets` (relation → offset; lower is a higher bar). Unknown keys are
  rejected. `uni.nlp.extract` needs `relations` or `schema`.
- **Only `asserted` / `reported` facts are claims about the world**; `planned`,
  `desired`, `questioned`, `hypothetical` are not. `negative` polarity is
  evidence against the same triple.
- **Spans** are UTF-8 byte offsets into the text, or into the turn named by
  `head_turn` / `tail_turn` for a conversation. Speakers have none.
- **`confidence`** is `sigmoid(margin)`, not a calibrated probability — use
  Locy `CALIBRATE` against labels.
- A v1 `kniv-deberta-nlp-*` model is refused: it has no fact extraction.
- **Prefetch first.** Loading the ~2 GB model on the first query exceeds the
  default query timeout; call `db.xervo().prefetch(["nlp/default"])` or use
  `"warmup": "eager"` / `"background"`.
- Rust: `db.xervo().extract_facts(&NlpInput::Text(..), &ExtractOptions::new().relation(..))`;
  `db.xervo().nlp(alias, requests)` is the raw per-window uni-xervo surface.

### Result Types

```python
class GenerationResult:
    text: str                    # Generated text
    usage: TokenUsage | None     # Token counts (if provider reports them)

class TokenUsage:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int

class ExtractedFact:             # rows of uni.nlp.extract
    doc: int; window: int
    head: int; head_text: str; head_type: str; head_speaker: bool
    head_start: int | None; head_end: int | None; head_turn: int | None
    relation: str
    tail: int; tail_text: str; tail_type: str; tail_speaker: bool
    tail_start: int | None; tail_end: int | None; tail_turn: int | None
    polarity: str; modality: str; holder: int | None; time: int | None
    margin: float; confidence: float; model_alias: str; model_id: str

class ExtractedEntity:           # rows of uni.nlp.entities
    doc: int; window: int; entity: int; text: str; entity_type: str; speaker: bool
    mentions: list[str]; mention_starts: list[int | None]; mention_ends: list[int | None]
    mention_turns: list[int | None]
    model_alias: str; model_id: str
```

---

## 6. Auto-Embedding

Vector indexes can automatically embed text properties on write and query.

### Schema Configuration

```python
db.schema() \
    .label("Document") \
        .property("title", DataType.STRING()) \
        .property("content", DataType.STRING()) \
        .vector("embedding", 384) \
        .index("embedding", {
            "type": "vector",
            "metric": "cosine",
            "embedding": {
                "alias": "embed/default",
                "source_properties": ["title", "content"],
                "batch_size": 32
            }
        }) \
    .apply()
```

### Cypher DDL

```cypher
CREATE VECTOR INDEX doc_embeddings
FOR (d:Document) ON d.embedding
OPTIONS {
  embedding: {
    alias: 'embed/default',
    source: ['title', 'content'],
    batch_size: 32
  }
}
```

### Embedding Config Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `alias` | Yes | — | Model alias from Xervo catalog |
| `source_properties` (Python, Rust) / `source` (Cypher DDL) | Yes | — | Non-empty list of text properties to concatenate and embed |
| `batch_size` | No | `32` | Texts per batch for embedding calls |
| `document_prefix` | No | none | Prepended to text embedded on write, e.g. `"passage: "` for E5-style models |
| `query_prefix` | No | none | Prepended to query text embedded at query time, e.g. `"query: "` |

**Use `source_properties` in Python.** Cypher DDL reads `source`, and the Python
binding now accepts either and rejects malformed configs, but released wheels up
to 5.0.0 silently ignore `source`: the index is created without auto-embedding,
and the first insert fails with `Property 'embedding' cannot be null`. Those
wheels also default `batch_size` to 100 rather than 32.

**Behavior:**
- **On write**: text from `source` properties is concatenated and embedded automatically
- **On query**: string arguments to `uni.vector.query()` and `similar_to()` are auto-embedded using the same alias
- **Validation**: database startup fails if schema references an alias not in the catalog

---

## 7. Reliability

### Circuit Breaker

Built-in per provider/model, protects against cascading failures:

| Parameter | Default | Description |
|---|---|---|
| Failure threshold | 5 | Consecutive failures before opening circuit |
| Recovery wait | 10s | Time before attempting half-open probe |
| State machine | Closed -> Open -> HalfOpen -> Closed | Auto-recovers on successful probe |

### Retry

Exponential backoff for transient errors (429 rate limits, 5xx server errors, timeouts):

```json
{
  "retry": {
    "max_attempts": 3,
    "initial_backoff_ms": 100
  }
}
```

Backoff formula: `initial_backoff_ms * 2^(attempt - 1)` (100ms, 200ms, 400ms with defaults).

### Metrics

Automatic instrumentation (when metrics enabled):

| Metric | Type | Labels |
|---|---|---|
| `model_load.duration_seconds` | Histogram | alias, provider |
| `model_load.total` | Counter | alias, provider, status |
| `model_inference.duration_seconds` | Histogram | alias, task, provider |
| `model_inference.total` | Counter | alias, task, provider, status |

---

## 8. Recommended Models

### Embedding Models

| Use Case | Provider | Model ID | Dimensions | Notes |
|---|---|---|---|---|
| **General purpose (local)** | `local/onnx` | `all-MiniLM-L6-v2` (preset) | 384 | Fast, small, good quality. Best default for local. |
| **Higher quality (local)** | `local/candle` | `BAAI/bge-base-en-v1.5` | 768 | Better quality, larger model |
| **Best quality (local)** | `local/onnx` | `Qwen3Embedding06B` (preset) | 1024 | Decoder-style embedder, ~600 MB; heavier than BGE |
| **Production (remote)** | `remote/openai` | `text-embedding-3-small` | 1536 | Best cost/quality ratio for OpenAI |
| **Max quality (remote)** | `remote/openai` | `text-embedding-3-large` | 3072 | Highest quality OpenAI embeddings |
| **Privacy-focused (remote)** | `remote/voyageai` | `voyage-large-2` | 1536 | Strong retrieval, data not used for training |
| **Multilingual (remote)** | `remote/cohere` | `embed-multilingual-v3.0` | 1024 | 100+ languages |
| **Multi-head hybrid (local)** | `local/onnx` | `aapot/bge-m3-onnx` | 1024 | BGE-M3: dense + learned-sparse + multivector/ColBERT from ONE forward pass. Task `EmbedHybrid` (see §9). |

### Generation Models

| Use Case | Provider | Model ID | Notes |
|---|---|---|---|
| **Fast + cheap** | `remote/openai` | `gpt-4o-mini` | Good for RAG summarization, structured extraction |
| **Best quality** | `remote/openai` | `gpt-4o` | Complex reasoning, detailed analysis |
| **Long context** | `remote/anthropic` | `claude-sonnet-4-6` | 200k context, strong instruction following |
| **Code generation** | `remote/anthropic` | `claude-sonnet-4-6` | Best for code tasks |
| **Local (private)** | `remote/llamacpp` | Any GGUF model loaded in `llama-server` | Fully private; GPU via llama.cpp (CUDA / Metal), CPU-viable with Q4/Q5 quantization. Vision needs the server started with `--mmproj` |

### Reranking Models

| Use Case | Provider | Model ID | Notes |
|---|---|---|---|
| **Local (CPU)** | `local/onnx` | `cross-encoder/ms-marco-MiniLM-L6-v2` | ~80MB, no API key, `provider-onnx` feature |
| **Best quality** | `remote/voyageai` | `rerank-2` | Top-tier reranking accuracy |
| **Cost-effective** | `remote/cohere` | `rerank-english-v3.0` | Good quality, lower cost |

The `local/onnx` provider handles tokenization (WordPiece) and batched ONNX inference internally. Any BERT-style cross-encoder from HuggingFace that accepts `input_ids`, `attention_mask`, `token_type_ids` and outputs a single logit per pair should work.

### Reranker API

```rust
// Via UniXervo facade
let scored = db.xervo().rerank("rerank/minilm", "query text", &["doc1", "doc2"]).await?;
// scored: Vec<ScoredDoc> { index, score, text }

// Via search procedures (see vector-hybrid-search.md Section 11)
// Add reranker options to any CALL procedure:
// {reranker: 'rerank/minilm', reranker_property: 'content'}
```

### NLP Extraction Models

| Use Case | Provider | Model ID | Notes |
|---|---|---|---|
| **Agent memory, zero-shot types, long inputs** | `local/onnx` | `dragonscale-ai/kniv-v7-xlarge-u-en` | UltraBERT; ~0.47 s per 200-word window INT8, ~2 GB resident |
| **Parsing, fixed-schema relations, plans vs assertions** | `local/onnx` | `dragonscale-ai/kniv-v7-xlarge-d-en` | DeBERTa; ~0.58 s, ~3 GB resident |

Use `"options": {"variant": "int8,fp16"}` on CPU and `"fp16"` on a GPU. English only. Cost grows with the square of the entities in a window, so keep entity-type lists narrow.

```json
{"alias": "nlp/default", "task": "nlp", "provider_id": "local/onnx",
 "model_id": "dragonscale-ai/kniv-v7-xlarge-u-en", "warmup": "lazy",
 "options": {"variant": "int8,fp16", "execution_providers": ["cpu"]}}
```

---

## 9. Example Catalogs

### Minimal Local-Only

```json
[
  {
    "alias": "embed/default",
    "task": "embed",
    "provider_id": "local/onnx",
    "model_id": "all-MiniLM-L6-v2"
  }
]
```

### Production RAG Pipeline

```json
[
  {
    "alias": "embed/default",
    "task": "embed",
    "provider_id": "remote/openai",
    "model_id": "text-embedding-3-small",
    "warmup": "background",
    "timeout": 10,
    "retry": { "max_attempts": 3, "initial_backoff_ms": 200 }
  },
  {
    "alias": "llm/default",
    "task": "generate",
    "provider_id": "remote/openai",
    "model_id": "gpt-4o-mini",
    "timeout": 30,
    "retry": { "max_attempts": 2, "initial_backoff_ms": 500 }
  },
  {
    "alias": "llm/reasoning",
    "task": "generate",
    "provider_id": "remote/anthropic",
    "model_id": "claude-sonnet-4-6",
    "timeout": 60,
    "retry": { "max_attempts": 2, "initial_backoff_ms": 1000 }
  }
]
```

### Hybrid Local + Remote

```json
[
  {
    "alias": "embed/local",
    "task": "embed",
    "provider_id": "local/onnx",
    "model_id": "all-MiniLM-L6-v2",
    "warmup": "eager",
    "required": true
  },
  {
    "alias": "embed/remote",
    "task": "embed",
    "provider_id": "remote/openai",
    "model_id": "text-embedding-3-small",
    "warmup": "lazy"
  },
  {
    "alias": "llm/default",
    "task": "generate",
    "provider_id": "remote/openai",
    "model_id": "gpt-4o-mini",
    "timeout": 30
  }
]
```

### BGE-M3 Multi-Head Hybrid (embed_hybrid)

A single catalog alias with task `embed_hybrid` (`ModelTask::EmbedHybrid` in Rust) produces **dense + learned-sparse + multivector/ColBERT** from ONE forward pass.

```json
[
  {
    "alias": "embed/bge-m3",
    "task": "embed_hybrid",
    "provider_id": "local/onnx",
    "model_id": "aapot/bge-m3-onnx"
  }
]
```

**One-alias-many-indexes single-pass pattern:** reference the **same alias** + **same source property** from multiple vector index `embedding: {alias, source}` configs — a dense `Vector` index, a sparse `SparseVector` index, and a multivector `List<Vector>` index. The engine auto-detects these as one hybrid group and runs a single forward pass per document.

**Head routing is inferred from the destination column's `DataType`** — there is **no** `head:` sub-key in the embedding config:

| Destination column DataType | Head used |
|---|---|
| `Vector` | dense |
| `SparseVector` | learned-sparse |
| `List<Vector>` | multivector / ColBERT |

```cypher
// All three indexes share alias 'embed/bge-m3' + source ['content']
// → one forward pass, head chosen by each property's column type.
CREATE VECTOR INDEX doc_dense  FOR (d:Document) ON (d.dense)
OPTIONS { embedding: { alias: 'embed/bge-m3', source: ['content'] } }

CREATE VECTOR INDEX doc_sparse FOR (d:Document) ON (d.sparse)
OPTIONS { type: 'sparse', embedding: { alias: 'embed/bge-m3', source: ['content'] } }

CREATE VECTOR INDEX doc_multi  FOR (d:Document) ON (d.colbert)
OPTIONS { embedding: { alias: 'embed/bge-m3', source: ['content'] } }
```

The `EmbeddingCfg` fields are `{alias, source_properties, batch_size, document_prefix, query_prefix}`.

### Azure Enterprise

```json
[
  {
    "alias": "embed/default",
    "task": "embed",
    "provider_id": "remote/azure-openai",
    "model_id": "text-embedding-3-small",
    "options": {
      "resource_name": "my-company-openai",
      "api_version": "2024-02-01",
      "api_key_env": "AZURE_OPENAI_KEY"
    }
  },
  {
    "alias": "llm/default",
    "task": "generate",
    "provider_id": "remote/azure-openai",
    "model_id": "gpt-4o",
    "options": {
      "resource_name": "my-company-openai",
      "api_version": "2024-02-01"
    }
  }
]
```

---

## 10. Common Patterns

### RAG Pipeline

```python
db = UniBuilder.open("./rag_db") \
    .xervo_catalog_from_file("./catalog.json") \
    .build()

xervo = db.xervo()
xervo.prefetch(["embed/default", "llm/default"])  # avoid cold-start on first call
session = db.session()

# Embed and insert documents
with session.tx() as tx:
    docs = ["Graph databases model relationships.", "Vector search finds similar items."]
    vectors = xervo.embed("embed/default", docs)
    for text, vec in zip(docs, vectors):
        tx.execute(
            "CREATE (:Doc {text: $t, embedding: $e})",
            params={"t": text, "e": vec}
        )
    tx.commit()

# Query: embed question, retrieve, generate
question = "How do graph databases work?"
q_vec = xervo.embed("embed/default", [question])[0]

results = session.query("""
    CALL uni.vector.query('Doc', 'embedding', $qv, 5)
    YIELD node, score
    RETURN node.text AS text, score
""", params={"qv": q_vec})

context = "\n".join(row["text"] for row in results)
answer = xervo.generate_text("llm/default", f"Context:\n{context}\n\nQ: {question}")
print(answer.text)
```

### Auto-Embedding (No Manual Embed Calls)

```python
# Configure catalog + schema with embedding config
db = UniBuilder.open("./auto_db") \
    .xervo_catalog_from_file("./catalog.json") \
    .build()

db.schema() \
    .label("Doc") \
        .property("text", DataType.STRING()) \
        .vector("embedding", 384) \
        .index("embedding", {
            "type": "vector",
            "metric": "cosine",
            "embedding": {"alias": "embed/default", "source_properties": ["text"]}
        }) \
    .apply()

# Just insert text — embedding happens automatically
with db.session().tx() as tx:
    tx.execute("CREATE (:Doc {text: 'Graph databases are powerful.'})")
    tx.commit()

# Query with text — auto-embedded at query time too
results = db.session().query("""
    CALL uni.vector.query('Doc', 'embedding', 'How do graphs work?', 5)
    YIELD node, score RETURN node.text, score
""")
```

---

## 11. Gotchas

1. **Xervo must be configured at startup** -- `db.xervo()` returns a facade that checks `is_available()`. If no catalog was provided via `xervo_catalog_from_file()` or `xervo_catalog_from_str()`, all embed/generate calls will error.

2. **Schema validation at startup** -- If your schema has vector indexes with `embedding.alias` config, the database will fail to open if the referenced alias is not in the catalog.

3. **Embedding dimensions must match** -- The vector property dimensions (e.g., `vector("embedding", 384)`) must match the model's output dimensions. Mismatches cause runtime errors on insert.

4. **Anthropic is generate-only** -- `remote/anthropic` does not support embeddings. Use a different provider for `Embed` tasks.

5. **Local models download on first use** -- `local/candle` and `local/onnx` download models from HuggingFace on first load. Use `warmup: "eager"` with `required: true` to catch download failures at startup, or call `xervo.prefetch(["alias"])` after startup to pre-load specific models before first inference.

6. **API keys are read at model load time** -- Environment variables are resolved when the model is first loaded (or at startup for eager warmup), not when the database is opened.

7. **Retry only applies to transient errors** -- 4xx errors (except 429 rate limits) are not retried. Invalid API keys, malformed requests, and model-not-found errors fail immediately.

8. **Alias format requires a slash** -- Aliases must be in `task/name` format (e.g., `embed/default`). Aliases without `/` are rejected during catalog validation.
