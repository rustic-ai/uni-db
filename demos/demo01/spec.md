# Demo #2: Semantic + Structural Search in One Query

## Executive Summary

**Demo Title**: "One Query to Rule Them All"

**Core Message**: Uni eliminates the integration tax. Where competitors require stitching together a graph database, a vector store, and a document database — Uni handles semantic similarity, multi-hop traversal, and JSON filtering in a single OpenCypher query.

**Target Audience**: 
- Data engineers tired of maintaining multiple systems
- ML engineers building RAG pipelines
- Product teams building knowledge-intensive applications

**Duration**: 12–15 minutes live demo

---

## The Problem We're Solving

### The Status Quo: Three Systems, Three Problems

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     TYPICAL "MODERN" ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│   │   Pinecone   │    │    Neo4j     │    │  MongoDB     │              │
│   │   (vectors)  │    │   (graph)    │    │  (documents) │              │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘              │
│          │                   │                   │                       │
│          └───────────────────┼───────────────────┘                       │
│                              ▼                                           │
│                    ┌─────────────────┐                                   │
│                    │   Glue Code     │  ← 2000 lines of Python          │
│                    │   - ID mapping  │  ← Consistency bugs              │
│                    │   - Result merge│  ← N+1 query patterns            │
│                    │   - Error handling│← 3x operational burden         │
│                    └─────────────────┘                                   │
│                                                                          │
│   Query: "Find papers similar to X that cite influential NeurIPS work"   │
│                                                                          │
│   Reality:                                                               │
│   1. Vector search in Pinecone → candidate IDs                          │
│   2. Fetch documents from MongoDB → filter by venue                      │
│   3. Graph traversal in Neo4j → citation chains                         │
│   4. Join results in application → pray IDs match                       │
│   5. Handle partial failures → give up and show error                   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### The Uni Way: One System, One Query

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           UNI ARCHITECTURE                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│                        ┌──────────────────┐                              │
│                        │       Uni        │                              │
│                        │  ┌────────────┐  │                              │
│                        │  │  Vectors   │  │                              │
│                        │  ├────────────┤  │                              │
│                        │  │   Graph    │  │                              │
│                        │  ├────────────┤  │                              │
│                        │  │ Documents  │  │                              │
│                        │  └────────────┘  │                              │
│                        └────────┬─────────┘                              │
│                                 │                                        │
│                                 ▼                                        │
│                    ┌─────────────────────┐                               │
│                    │   Single Cypher     │                               │
│                    │   Query             │                               │
│                    └─────────────────────┘                               │
│                                                                          │
│   MATCH (p:Paper)-[:CITES*1..3]->(cited:Paper)                          │
│   WHERE vector_similarity(p.embedding, $query) > 0.85                   │
│     AND cited._doc.venue = "NeurIPS"                                    │
│   RETURN cited.title ORDER BY cited.citation_count DESC                 │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Technical Specification

### Hybrid Query Capabilities

| Capability | Syntax | Implementation |
|------------|--------|----------------|
| **Vector Similarity** | `vector_similarity(node.embedding, $vec)` | Lance IVF-PQ index, HNSW |
| **K-NN Search** | `CALL vector.knn(label, field, $vec, k)` | Returns top-k with scores |
| **Graph Traversal** | `(a)-[:TYPE*min..max]->(b)` | gryf BFS/DFS on WorkingGraph |
| **JSON Filtering** | `node._doc.path.to.field = value` | DataFusion predicate pushdown |
| **JSON Path Expressions** | `node._doc.authors[0].name` | Arrow nested field access |

### Query Planning Strategy

The optimizer chooses execution order based on estimated selectivity:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        QUERY PLANNING DECISION TREE                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Input: MATCH + WHERE with vector, graph, and document predicates       │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Step 1: Estimate Selectivity                                     │   │
│   │                                                                  │   │
│   │   vector_similarity > 0.85  →  ~0.1% of nodes (highly selective)│   │
│   │   _doc.venue = "NeurIPS"    →  ~5% of papers                    │   │
│   │   [:CITES*1..3]             →  expansion factor ~50x            │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Step 2: Choose Execution Order                                   │   │
│   │                                                                  │   │
│   │   Strategy A: Vector-First (when similarity is most selective)   │   │
│   │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │   │
│   │   │ Vector k-NN  │ →  │ Graph Expand │ →  │ JSON Filter  │     │   │
│   │   │ (100 seeds)  │    │ (3 hops)     │    │ (venue check)│     │   │
│   │   └──────────────┘    └──────────────┘    └──────────────┘     │   │
│   │                                                                  │   │
│   │   Strategy B: Graph-First (when structure is most selective)     │   │
│   │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │   │
│   │   │ Anchor Node  │ →  │ Graph Expand │ →  │ Vector Score │     │   │
│   │   │ (known start)│    │ (neighbors)  │    │ (re-rank)    │     │   │
│   │   └──────────────┘    └──────────────┘    └──────────────┘     │   │
│   │                                                                  │   │
│   │   Strategy C: Document-First (when JSON filter is selective)     │   │
│   │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │   │
│   │   │ JSON Index   │ →  │ Vector Score │ →  │ Graph Expand │     │   │
│   │   │ (venue=X)    │    │ (similarity) │    │ (if needed)  │     │   │
│   │   └──────────────┘    └──────────────┘    └──────────────┘     │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Model for Demo

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SCHEMA: RESEARCH PAPERS                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Vertex Labels:                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  :Paper                                                          │   │
│   │    ├── title: String                                            │   │
│   │    ├── abstract: String                                         │   │
│   │    ├── year: Int                                                │   │
│   │    ├── citation_count: Int                                      │   │
│   │    ├── embedding: Vector[768]        ← BERT/SciBERT embedding   │   │
│   │    └── _doc: JSON                    ← Flexible metadata        │   │
│   │          ├── venue: String                                      │   │
│   │          ├── authors: [{name, affiliation, orcid}]              │   │
│   │          ├── keywords: [String]                                 │   │
│   │          ├── doi: String                                        │   │
│   │          └── pdf_url: String                                    │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  :Author                                                         │   │
│   │    ├── name: String                                             │   │
│   │    ├── h_index: Int                                             │   │
│   │    └── _doc: JSON {orcid, affiliations[], homepage}             │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  :Institution                                                    │   │
│   │    ├── name: String                                             │   │
│   │    └── _doc: JSON {country, type, ror_id}                       │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│   Edge Types:                                                            │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  :CITES        (Paper)-[:CITES]->(Paper)                        │   │
│   │    └── context: String (citation context snippet)               │   │
│   │                                                                  │   │
│   │  :AUTHORED_BY  (Paper)-[:AUTHORED_BY]->(Author)                 │   │
│   │    └── position: Int (1 = first author, -1 = last author)       │   │
│   │                                                                  │   │
│   │  :AFFILIATED   (Author)-[:AFFILIATED]->(Institution)            │   │
│   │    └── years: [Int] (years of affiliation)                      │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Dataset Statistics (Demo Size)

| Entity | Count | Storage |
|--------|-------|---------|
| Papers | 2.3M | ~4GB (with embeddings) |
| Authors | 1.8M | ~200MB |
| Institutions | 45K | ~5MB |
| CITES edges | 28M | ~1.2GB |
| AUTHORED_BY edges | 8M | ~400MB |
| AFFILIATED edges | 2.5M | ~150MB |
| **Total** | | **~6GB** |

**Source**: Semantic Scholar Open Research Corpus (subset)

---

## Demo Script

### Pre-Demo Setup

```bash
# 1. Prepare the dataset (done before demo)
uni import semantic-scholar \
  --papers papers.jsonl \
  --citations citations.jsonl \
  --output s3://uni-demos/semantic-scholar/

# 2. Verify graph is accessible
uni info s3://uni-demos/semantic-scholar/
# Output:
#   Vertices: 4,145,000 (Paper: 2.3M, Author: 1.8M, Institution: 45K)
#   Edges: 38,500,000 (CITES: 28M, AUTHORED_BY: 8M, AFFILIATED: 2.5M)
#   Snapshots: 1
#   Vector indexes: Paper.embedding (IVF-PQ, 768d)
#   JSON indexes: Paper._doc.venue, Paper._doc.authors[*].name
```

### Act 1: Set the Stage (2 minutes)

**[SLIDE: The Integration Tax]**

> "Let me show you something that should be simple but isn't.
> 
> I want to find research papers. Not just any papers — papers that are:
> - **Semantically similar** to a paper I'm reading
> - **Within 3 citation hops** of influential work
> - **Published at top venues** like NeurIPS or ICML
> 
> This is a natural question. But in most architectures, it requires three systems:
> - Pinecone for the semantic search
> - Neo4j for the citation graph
> - MongoDB for the venue metadata
> 
> And then 2,000 lines of glue code to stitch them together."

**[SLIDE: One Query]**

> "With Uni, it's one query. Let me show you."

### Act 2: Connect and Explore (2 minutes)

**[TERMINAL: Connect to the graph]**

```python
import uni
from uni import Q  # Query builder

# Connect to the demo graph on S3
graph = uni.open("s3://uni-demos/semantic-scholar/")

# Quick stats
print(graph.stats())
```

**Output:**
```
UniGraph: semantic-scholar
  Storage: s3://uni-demos/semantic-scholar/
  Vertices: 4,145,000
  Edges: 38,500,000
  Snapshot: v42 (2024-03-15T10:30:00Z)
  Cache: warm (L1: 2.1GB, L2: 4.8GB)
```

> "We're connected to a graph with 2.3 million papers and 28 million citation links, stored entirely on S3. No cluster, no warmup ritual — just open and query."

**[TERMINAL: Show the schema]**

```python
graph.schema()
```

**Output:**
```
Vertex Labels:
  :Paper (2,300,000 vertices)
    - title: String
    - abstract: String
    - year: Int
    - citation_count: Int
    - embedding: Vector[768]
    - _doc: JSON {venue, authors, keywords, doi}

  :Author (1,800,000 vertices)
  :Institution (45,000 vertices)

Edge Types:
  :CITES (28,000,000 edges)
  :AUTHORED_BY (8,000,000 edges)
  :AFFILIATED (2,500,000 edges)

Indexes:
  - Paper.embedding: IVF-PQ (k=100, nprobe=10)
  - Paper._doc.venue: Inverted
  - Paper._doc.authors[*].name: Inverted
```

### Act 3: The Hero Query (4 minutes)

**[TERMINAL: Define the seed paper]**

```python
# Let's start with a famous paper: "Attention Is All You Need"
seed = graph.query("""
    MATCH (p:Paper)
    WHERE p._doc.doi = "10.5555/3295222.3295349"
    RETURN p.title, p.embedding
""").first()

print(f"Seed paper: {seed['p.title']}")
# Output: "Seed paper: Attention Is All You Need"
```

> "We have our seed — the Transformer paper. Now let's find related work."

**[TERMINAL: The hybrid query]**

```python
# THE MONEY QUERY: Vector + Graph + JSON in one
results = graph.query("""
    MATCH (p:Paper)-[:CITES*1..3]->(cited:Paper)
    WHERE vector_similarity(p.embedding, $seed_embedding) > 0.82
      AND cited._doc.venue IN ["NeurIPS", "ICML", "ICLR"]
      AND cited.year >= 2020
    RETURN DISTINCT cited.title, 
           cited.year,
           cited.citation_count,
           cited._doc.venue,
           cited._doc.authors[0].name AS first_author
    ORDER BY cited.citation_count DESC
    LIMIT 15
""", seed_embedding=seed['p.embedding'])

for r in results:
    print(f"{r['cited.year']} | {r['cited._doc.venue']:8} | {r['cited.citation_count']:5} cites | {r['cited.title'][:50]}...")
```

**Output:**
```
Query executed in 847ms (vector: 23ms, graph: 612ms, filter: 89ms, sort: 123ms)

2023 | NeurIPS  | 12847 cites | LLaMA: Open and Efficient Foundation Language Model...
2022 | NeurIPS  |  9823 cites | Chinchilla: Training Compute-Optimal Large Language...
2021 | NeurIPS  |  8234 cites | LoRA: Low-Rank Adaptation of Large Language Models...
2020 | ICLR     |  7891 cites | An Image is Worth 16x16 Words: Transformers for Im...
2022 | ICML     |  6543 cites | FlashAttention: Fast and Memory-Efficient Exact At...
2021 | ICML     |  5678 cites | Scaling Laws for Neural Language Models...
2023 | ICLR     |  4532 cites | Constitutional AI: Harmlessness from AI Feedback...
...
```

**[Pause for effect]**

> "Let's unpack what just happened:
> 
> 1. **Vector search** found papers semantically similar to the Transformer paper — that's the `vector_similarity > 0.82`
> 2. **Graph traversal** followed citation chains up to 3 hops — the `[:CITES*1..3]`
> 3. **JSON filtering** restricted to top ML venues — the `_doc.venue IN [...]`
> 4. **Columnar aggregation** sorted by citation count
> 
> All in **847 milliseconds**. One query. No glue code."

### Act 4: Explain the Plan (2 minutes)

**[TERMINAL: Show query plan]**

```python
graph.explain("""
    MATCH (p:Paper)-[:CITES*1..3]->(cited:Paper)
    WHERE vector_similarity(p.embedding, $seed_embedding) > 0.82
      AND cited._doc.venue IN ["NeurIPS", "ICML", "ICLR"]
      AND cited.year >= 2020
    RETURN DISTINCT cited.title, cited.citation_count
    ORDER BY cited.citation_count DESC
    LIMIT 15
""", seed_embedding=seed['p.embedding'])
```

**Output:**
```
Query Plan:
  ┌─ VectorKnn(Paper.embedding, k=500, threshold=0.82)     23ms   │ 487 candidates
  │    └─ Selectivity: 0.02% (highly selective)
  │
  ├─ GraphExpand([:CITES*1..3], direction=OUT)            612ms   │ 23,847 reached  
  │    ├─ L1 cache hits: 94.2%
  │    ├─ Chunks loaded: 127
  │    └─ Pruned by visited set: 89,234
  │
  ├─ Filter(venue IN [...], year >= 2020)                  89ms   │ 2,341 passed
  │    ├─ Pushdown: venue index scan
  │    └─ Predicate eval: DataFusion
  │
  ├─ Distinct(cited.vid)                                   12ms   │ 1,876 unique
  │
  └─ Sort(citation_count DESC) + Limit(15)                123ms   │ 15 returned

Total: 847ms
```

> "The optimizer chose **vector-first** because similarity was the most selective predicate. It found 487 seed papers, expanded through the citation graph to reach ~24,000 papers, filtered down to 2,341 from top venues, and returned the top 15 by citations.
> 
> Notice the **94% L1 cache hit rate** — the citation graph's adjacency structure is hot in memory, so traversal is fast."

### Act 5: Variations (3 minutes)

**[TERMINAL: Author-centric query]**

```python
# Find authors whose work is semantically related AND structurally connected
results = graph.query("""
    MATCH (p:Paper)-[:CITES*1..2]->(cited:Paper)-[:AUTHORED_BY]->(author:Author)
    WHERE vector_similarity(p.embedding, $seed_embedding) > 0.85
      AND author.h_index > 50
    RETURN author.name, 
           author.h_index,
           COUNT(DISTINCT cited) AS related_papers,
           COLLECT(DISTINCT cited._doc.venue)[..3] AS venues
    ORDER BY related_papers DESC
    LIMIT 10
""", seed_embedding=seed['p.embedding'])
```

**Output:**
```
Query executed in 1,234ms

Yoshua Bengio    | h=182 | 34 papers | [NeurIPS, ICML, JMLR]
Geoffrey Hinton  | h=168 | 28 papers | [NeurIPS, Nature, Science]
Yann LeCun       | h=145 | 25 papers | [NeurIPS, CVPR, PAMI]
Ilya Sutskever   | h=98  | 22 papers | [NeurIPS, ICML, ICLR]
...
```

> "Same pattern — vector similarity finds related work, graph traversal follows citations to authors, and we aggregate by author. The `COLLECT` function gathers their publication venues into an array."

**[TERMINAL: Institution influence]**

```python
# Which institutions are hubs in this research area?
results = graph.query("""
    MATCH (p:Paper)-[:CITES*1..2]->(cited:Paper)
          -[:AUTHORED_BY]->(:Author)-[:AFFILIATED]->(inst:Institution)
    WHERE vector_similarity(p.embedding, $seed_embedding) > 0.80
    RETURN inst.name,
           inst._doc.country,
           COUNT(DISTINCT cited) AS paper_count,
           SUM(cited.citation_count) AS total_citations
    ORDER BY total_citations DESC
    LIMIT 10
""", seed_embedding=seed['p.embedding'])
```

**Output:**
```
Query executed in 2,891ms

Google Research       | USA | 156 papers | 234,567 citations
OpenAI                | USA | 89 papers  | 198,234 citations
Meta AI (FAIR)        | USA | 78 papers  | 145,678 citations
DeepMind              | UK  | 67 papers  | 134,567 citations
Stanford University   | USA | 54 papers  | 98,765 citations
...
```

> "Now we've traversed **four hops**: Paper → Paper → Author → Institution. Still one query, still under 3 seconds."

### Act 6: The Comparison (2 minutes)

**[SLIDE: Side-by-side code comparison]**

**Traditional (3 systems):**
```python
# Step 1: Vector search in Pinecone
pinecone_results = pinecone_index.query(
    vector=seed_embedding,
    top_k=500,
    filter={"year": {"$gte": 2020}}
)
candidate_ids = [r.id for r in pinecone_results.matches]

# Step 2: Fetch metadata from MongoDB  
mongo_papers = list(mongo_db.papers.find({
    "_id": {"$in": candidate_ids},
    "venue": {"$in": ["NeurIPS", "ICML", "ICLR"]}
}))
paper_ids = [p["_id"] for p in mongo_papers]

# Step 3: Graph traversal in Neo4j
neo4j_query = """
    MATCH (p:Paper)-[:CITES*1..3]->(cited:Paper)
    WHERE p.id IN $paper_ids
    RETURN DISTINCT cited.id, cited.title, cited.citation_count
    ORDER BY cited.citation_count DESC
    LIMIT 15
"""
neo4j_results = neo4j_session.run(neo4j_query, paper_ids=paper_ids)

# Step 4: Join results (pray IDs match across systems)
final_results = []
for r in neo4j_results:
    paper = mongo_db.papers.find_one({"_id": r["cited.id"]})
    if paper:  # Might be missing due to sync lag
        final_results.append({...})
```

**Uni (1 system):**
```python
results = graph.query("""
    MATCH (p:Paper)-[:CITES*1..3]->(cited:Paper)
    WHERE vector_similarity(p.embedding, $seed_embedding) > 0.82
      AND cited._doc.venue IN ["NeurIPS", "ICML", "ICLR"]
      AND cited.year >= 2020
    RETURN cited.title, cited.citation_count
    ORDER BY cited.citation_count DESC
    LIMIT 15
""", seed_embedding=seed_embedding)
```

> "47 lines vs 8 lines. Three systems vs one. And with Uni, you'll never have a sync bug where IDs don't match across databases."

### Act 7: Close (1 minute)

**[SLIDE: Key Takeaways]**

> "What you've seen:
> 
> 1. **One query language** — OpenCypher extended for vectors and JSON
> 2. **One storage system** — graph, vectors, and documents in Lance format
> 3. **One optimization pass** — the planner chooses vector-first, graph-first, or document-first based on selectivity
> 4. **Sub-second latency** — even on S3, even cold
> 
> This is what 'hybrid' should mean. Not three systems duct-taped together — one engine that natively understands all three."

**[SLIDE: Call to Action]**

> "Want to try it? `pip install uni-db` and `uni demo semantic-scholar` — you'll have this exact dataset running locally in 5 minutes."

---

## Appendix A: Query Syntax Reference

### Vector Functions

```cypher
// Cosine similarity (returns 0.0 to 1.0)
vector_similarity(node.embedding, $query_vector)

// Euclidean distance
vector_distance(node.embedding, $query_vector)

// K-nearest neighbors (returns nodes with _score property)
CALL uni.vector.knn('Paper', 'embedding', $query_vector, 100)
YIELD node, score
WHERE score > 0.8
RETURN node.title, score
```

### JSON Path Expressions

```cypher
// Direct field access
node._doc.venue

// Array indexing
node._doc.authors[0].name

// Array wildcard (any element matches)
node._doc.authors[*].affiliation = "Stanford"

// Nested paths
node._doc.metadata.review.scores[0]
```

### Variable-Length Paths

```cypher
// Exactly 2 hops
(a)-[:CITES*2]->(b)

// 1 to 3 hops
(a)-[:CITES*1..3]->(b)

// 0 or more hops (includes self)
(a)-[:CITES*]->(b)

// Collect path information
MATCH path = (a)-[:CITES*1..3]->(b)
RETURN nodes(path), relationships(path), length(path)
```

---

## Appendix B: Performance Benchmarks

### Query Latencies (p50/p95/p99)

| Query Type | Cold (S3) | Warm (L1 Cache) |
|------------|-----------|-----------------|
| Vector k-NN (k=100) | 45/82/120ms | 8/15/25ms |
| 2-hop traversal (10K edges) | 230/450/890ms | 45/89/150ms |
| 3-hop traversal (100K edges) | 1.2/2.4/4.1s | 280/520/890ms |
| JSON filter (indexed field) | 12/25/45ms | 3/8/15ms |
| Hybrid (vector + 2-hop + JSON) | 890/1.8/3.2s | 180/350/620ms |

### Comparison vs. Multi-System Architecture

| Metric | Pinecone+Neo4j+Mongo | Uni |
|--------|---------------------|-----|
| Latency (p50) | 2.4s | 850ms |
| Latency (p99) | 8.2s | 3.2s |
| Lines of code | 150+ | 15 |
| Failure modes | 6 | 1 |
| Monthly cost (1M queries) | $2,400 | $180 |

---

## Appendix C: Sample Data Generator

```python
#!/usr/bin/env python3
"""Generate synthetic demo data matching Semantic Scholar schema."""

import json
import random
import numpy as np
from faker import Faker

fake = Faker()

VENUES = ["NeurIPS", "ICML", "ICLR", "CVPR", "ACL", "EMNLP", "AAAI", "IJCAI"]

def generate_paper(paper_id: int) -> dict:
    """Generate a single paper with embedding."""
    return {
        "vid": paper_id,
        "title": fake.sentence(nb_words=8),
        "abstract": fake.paragraph(nb_sentences=5),
        "year": random.randint(2018, 2024),
        "citation_count": int(np.random.power(0.3) * 10000),
        "embedding": np.random.randn(768).astype(np.float32).tolist(),
        "_doc": {
            "venue": random.choice(VENUES),
            "doi": f"10.{random.randint(1000,9999)}/{fake.uuid4()[:8]}",
            "authors": [
                {
                    "name": fake.name(),
                    "affiliation": fake.company(),
                    "orcid": f"0000-000{random.randint(1,9)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}"
                }
                for _ in range(random.randint(1, 6))
            ],
            "keywords": [fake.word() for _ in range(random.randint(3, 8))]
        }
    }

def generate_citations(num_papers: int, avg_citations: int = 12) -> list:
    """Generate citation edges with power-law distribution."""
    edges = []
    for src in range(num_papers):
        # Power-law: some papers cite many, most cite few
        num_cites = int(np.random.power(0.5) * avg_citations * 2)
        targets = random.sample(range(num_papers), min(num_cites, num_papers - 1))
        for dst in targets:
            if dst != src:
                edges.append({
                    "src_vid": src,
                    "dst_vid": dst,
                    "type": "CITES",
                    "context": fake.sentence()
                })
    return edges

if __name__ == "__main__":
    # Generate 10K papers for local demo
    papers = [generate_paper(i) for i in range(10000)]
    citations = generate_citations(10000)
    
    with open("papers.jsonl", "w") as f:
        for p in papers:
            f.write(json.dumps(p) + "\n")
    
    with open("citations.jsonl", "w") as f:
        for c in citations:
            f.write(json.dumps(c) + "\n")
    
    print(f"Generated {len(papers)} papers and {len(citations)} citations")
```

---
