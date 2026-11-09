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
            algorithm: VectorAlgo::HnswSq { m: 32, ef_construction: 200, partitions: None },
            metric: VectorMetric::Cosine,
            embedding: None,
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

With Uni-Xervo auto-embedding, you don't need to pre-compute embeddings externally. Configure an embedding alias in your vector index and Uni generates embeddings automatically on insert:

```cypher
// Create index with auto-embedding
CREATE VECTOR INDEX chunk_embed FOR (c:Chunk) ON (c.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        alias: 'embed/default',
        source: ['text'],
        batch_size: 64
    }
}

// Insert text — embedding generated automatically
CREATE (c:Chunk {id: 'c1', text: 'Function verify() checks signatures.'})
```

Alternatively, you can pre-compute embeddings externally or via the Uni-Xervo runtime directly:

```rust
let xervo = db.xervo()?;
let embeddings = xervo.embed("embed/default", &["Function verify() checks signatures."]).await?;
```

For bulk import, use JSONL with either pre-computed embeddings or rely on the auto-embed pipeline:

```bash
# With pre-computed embeddings
# chunks.jsonl: {"id": "c1", "text": "Function verify() checks signatures.", "embedding": [...]}
# Without embeddings (auto-generated if index has embedding config)
# chunks.jsonl: {"id": "c1", "text": "Function verify() checks signatures."}

uni import support-bot \
  --papers chunks.jsonl \
  --citations relations.jsonl
```

### Linking chunks to entities

The expansion query below follows `MENTIONS` edges, so something has to
create them. Rather than call an LLM outside the database, you can extract
entities in-process with kniv v7 and `uni.nlp.entities` (see
[NLP Extraction](../features/nlp-extraction.md) for the catalog entry):

```cypher
MATCH (c:Chunk)
CALL uni.nlp.entities(c.text, {entity_types: ['function', 'class', 'error code']})
YIELD text, entity_type
MERGE (e:Entity {name: text, type: entity_type})
MERGE (c)-[:MENTIONS]->(e)
```

`MERGE` on name joins only identical spellings; to link "verify" and
"verify()" to one entity, see
[Resolving Entities](../features/nlp-extraction.md#resolving-entities). To
extract only the chunks a write adds, run the same statement from a declared
trigger; [NLP Extraction](../features/nlp-extraction.md) shows how.
`uni.nlp.extract` goes one step further and yields typed facts between the
entities, which can become `RELATED_TO` edges.

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

### 5. Generation with Retrieved Context

With Uni-Xervo's generation API, you can close the RAG loop entirely within Uni — retrieve context via graph+vector queries, then pass it to an LLM:

```rust
use uni_db::xervo::{Message, GenerationOptions};

// 1. Retrieve context (from the GraphRAG query above)
let context_rows = db.query_with(
    "CALL uni.vector.query('Chunk', 'embedding', $q, 5) YIELD node, distance
     MATCH (node)-[:MENTIONS]->(topic:Entity)
     RETURN node.text AS chunk, topic.name AS entity, distance"
)
    .param("q", "how does verify() work?")
    .fetch_all()
    .await?;

// 2. Build context string from results
let context: String = context_rows.iter()
    .map(|r| r.get::<String>("chunk").unwrap())
    .collect::<Vec<_>>()
    .join("\n\n");

// 3. Generate answer using Uni-Xervo
let xervo = db.xervo();
xervo.prefetch(&["llm/default"]).await?;  // pre-load model if not already cached
let result = xervo.generate("llm/default", &[
    Message::system("Answer using only the provided context."),
    Message::user(&format!("Context:\n{context}\n\nQuestion: how does verify() work?")),
], GenerationOptions::default()).await?;

println!("{}", result.text);
```

### Key Advantages

*   **Speed**: No network round-trip between Vector DB and Graph DB. The join happens in-memory via the adjacency cache.
*   **Simplicity**: Just one Docker container (or embedded binary) to manage.
*   **Precision**: We filter vector noise using graph structure (only chunks related to specific entities).
