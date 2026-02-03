# Demo Walkthrough: One Query to Rule Them All

This document outlines the step-by-step flow for presenting the **Uni** demo. It translates the high-level vision in `spec.md` into concrete actions using the current CLI tools.

## Act 1: The Setup

**Narrative**: "We are starting from scratch. No complex infrastructure, just a raw dataset and the Uni engine."

### Step 1: Generate Data
Show that we are creating a realistic dataset with Papers, Citations, and Embeddings.

```bash
cd demos/demo01
python3 generate_data.py
# Output: Generated 5000 papers...
```

### Step 2: Import Data
**Narrative**: "Uni handles the complexity of ingesting Graph relationships, Document metadata, and Vectors in one go."

```bash
# Clean previous state
rm -rf storage

# Run Import
../../target/release/uni import semantic-scholar \
  --papers data/papers.jsonl \
  --citations data/citations.jsonl \
  --output storage
```

**Talking Points**:
- "Notice we didn't spin up Neo4j, then Pinecone, then MongoDB."
- "The import process builds:
    1.  **LSM Trees** for Adjacency (Graph)
    2.  **Lance Datasets** for Properties & Vectors
    3.  **Inverted Indexes** for metadata"

## Act 2: The "Hero" Query

**Narrative**: "Now for the core promise: replacing 2,000 lines of glue code with **One Query**."

We will run a Hybrid Query that combines:
1.  **Graph Traversal**: Finding papers cited by a seed paper.
2.  **Property Filtering**: Filtering by title/year.

### Command
```bash
../../target/release/uni query \
  "MATCH (seed:Paper)-[:CITES]->(cited:Paper) \
   WHERE seed.title = 'Attention Is All You Need' \
   RETURN cited.title, cited.year \
   LIMIT 5" \
  --path .
```

**Explanation of the Query**:
- `MATCH (seed)-[:CITES]->(cited)`: This is the **Graph** part. We traverse edges.
- `WHERE seed.title = ...`: This is the **Filter** part (simulating an exact lookup, which could also be a Vector Similarity search).
- `RETURN ...`: Retrieving **Document** properties from the target nodes.

### Result
```
Found X results:
{"cited.title": "...", "cited.year": ...}
```

**Talking Points**:
- "In a traditional stack, you'd query the graph DB for IDs, then fetch metadata from a Doc DB."
- "Here, the engine optimized the join between topology and properties."

## Act 3: The Vector Comparison (Conceptual)

**Narrative**: "If we were using the full Python SDK (upcoming), we would add the third pillar: Vector Search."

```cypher
MATCH (p:Paper)-[:CITES*1..3]->(cited:Paper)
WHERE vector_similarity(p.embedding, $seed) > 0.85
  AND cited._doc.venue = 'NeurIPS'
RETURN cited.title
```

**Talking Points**:
- "The query planner (Strategy A) would scan the Vector Index first to find highly semantic candidates."
- "Then it would use the Graph to validate structural relevance."
- "Finally, it filters by Document metadata."
- "Zero glue code."

## Act 4: Conclusion

**Narrative**:
1.  **One System**: 47 lines of Python glue -> 8 lines of Cypher.
2.  **One Storage**: Local filesystem / S3 (Lance format).
3.  **One Latency**: No network hops between DBs.

"Uni is the database for the AI engineer who wants to build features, not pipelines."

