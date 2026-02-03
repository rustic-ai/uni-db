# RAG & Knowledge Graphs

Uni is uniquely positioned for Retrieval-Augmented Generation (RAG) by combining **vector search** (for semantic retrieval) and **knowledge graphs** (for structured reasoning) in a single, embedded engine.

## Why Uni for RAG?

| Challenge | Traditional Approach | Uni Approach |
|-----------|----------------------|--------------|
| **Latency** | Vector DB query + Graph DB query + App logic merge (~50-100ms) | Single execution plan, local memory access (~5ms) |
| **Complexity** | Maintaining sync between Pinecone/Weaviate and Neo4j | One schema, one storage engine, one transaction log |
| **Context** | "Dumb" retrieval of chunks based only on similarity | **GraphRAG**: Retrieve chunks + related entities + relationships |

---

## Scenario: Technical Support Bot

We want to build a support bot that answers questions about a software product. It needs to:
1. Find documentation chunks semantically similar to the user's query.
2. Traverse to related API methods, known issues, and version history.
3. Return a rich context window to the LLM.

### 1. Schema Definition

We model `Documents` (chunked text) and `Entities` (API endpoints, Error codes) linked together.

**Conceptual schema (illustrative):**
```json
{
  "labels": {
    "Chunk": {
      "id": 1
    },
    "Entity": {
      "id": 2
    }
  },
  "edge_types": {
    "MENTIONS": { "id": 1, "src_labels": ["Chunk"], "dst_labels": ["Entity"] },
    "RELATED_TO": { "id": 2, "src_labels": ["Entity"], "dst_labels": ["Entity"] },
    "NEXT_CHUNK": { "id": 3, "src_labels": ["Chunk"], "dst_labels": ["Chunk"] }
  },
  "properties": {
    "Chunk": {
      "text": { "type": "String", "nullable": false },
      "embedding": { "type": "Vector", "dimensions": 768 }
    },
    "Entity": {
      "name": { "type": "String", "nullable": false },
      "type": { "type": "String", "nullable": true } // "function", "class", "error"
    }
  }
}
```

**Schema (Rust example):**
```rust
use uni_db::{DataType, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

db.schema()
    .label("Chunk")
        .property("text", DataType::String)
        .vector("embedding", 384)
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::Hnsw { m: 32, ef_construction: 200 },
            metric: VectorMetric::Cosine,
        }))
        .done()
    .label("Entity")
        .property("name", DataType::String)
        .property_nullable("type", DataType::String)
        .done()
    .edge_type("MENTIONS", &["Chunk"], &["Entity"])
        .done()
    .apply()
    .await?;
```

### 2. Configuration

For RAG, we prioritize read latency. We ensure the **Adjacency Cache** is large enough to hold the relationship graph, and the **Vector Index** fits in memory/cache.

**Rust config example:**
```rust
use uni_db::UniConfig;

let mut config = UniConfig::default();
config.cache_size = 500_000_000; // Cache topology for fast expansion

let db = Uni::open("./rag_data")
    .config(config)
    .build()
    .await?;
```

### 3. Data Ingestion

Embed your documents (using OpenAI/Cohere/FastEmbed) and extract entities (using LLM or NER) before inserting.

```bash
# Example JSONL structure for import
# chunks.jsonl
# {"id": "c1", "text": "Function verify() checks signatures.", "embedding": [...]} 
# entities.jsonl
# {"id": "e1", "name": "verify", "type": "function"}
# relations.jsonl
# {"src": "c1", "dst": "e1", "type": "MENTIONS"}

uni import support-bot \
  --papers chunks.jsonl \
  --citations relations.jsonl
```

### 4. Querying (GraphRAG)

This single query performs the entire retrieval pipeline:

1.  **Vector Search**: Finds the top 5 relevant text chunks.
2.  **Graph Expansion**: Finds entities mentioned in those chunks.
3.  **2nd Hop**: Finds *other* chunks that mention those entities (expanding context).

```cypher
// 1. Vector Search for relevant chunks
CALL uni.vector.query('Chunk', 'embedding', $user_query_vector, 5)
YIELD node AS primary_chunk, distance

// 2. Find connected Entities (e.g., "verify function")
MATCH (primary_chunk)-[:MENTIONS]->(topic:Entity)

// 3. Find other chunks mentioning these topics (Context Expansion)
MATCH (related_chunk:Chunk)-[:MENTIONS]->(topic)
WHERE related_chunk.id <> primary_chunk.id

// 4. Return unique relevant text blocks
RETURN DISTINCT 
    primary_chunk.text AS main_answer,
    topic.name AS related_concept,
    related_chunk.text AS additional_context,
    distance
ORDER BY distance ASC
LIMIT 10
```

### Key Advantages

*   **Speed**: No network round-trip between Vector DB and Graph DB. The join happens in-memory via the adjacency cache.
*   **Simplicity**: Just one Docker container (or embedded binary) to manage.
*   **Precision**: We filter vector noise using graph structure (only chunks related to specific entities).
