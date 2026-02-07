# Uni Function and Procedure Reference

**Version**: 1.0
**Updated**: 2026-01-26

## Overview

All Uni functions and procedures use a clean, hierarchical namespace structure under `uni.*` for easy discovery and organization.

## Table of Contents

- [Vector Search](#vector-search-univector)
- [Full-Text Search](#full-text-search-unifts)
- [Hybrid Search](#hybrid-search-unisearch)
- [Database Administration](#database-administration-uniadmin)
- [Schema Management](#schema-management-unischema)
- [Temporal Queries](#temporal-queries-unitemporal)
- [Graph Algorithms](#graph-algorithms-unialgo)
- [Bitwise Operations](#bitwise-operations-unibitwise)

---

## Vector Search (`uni.vector.*`)

Vector similarity search and nearest neighbor queries.

### `uni.vector.query(label, property, query_input, k [, filter] [, threshold])`

Search for k-nearest neighbors using vector similarity. Supports both pre-computed vectors and automatic text embedding.

**Parameters:**
- `label` (String): Node label to search
- `property` (String): Property containing embeddings
- `query_input` (List<Float> or String): Either:
  - Pre-computed query vector as list of floats
  - Text string to auto-embed using the index's embedding configuration
- `k` (Integer): Number of results to return
- `filter` (String, optional): WHERE clause for pre-filtering (e.g., `'category = "tech"'`)
- `threshold` (Float, optional): Minimum similarity score to include (0-1)

**Yields:**
- `vid` (Integer): Vertex ID of matching node
- `score` (Float): Similarity score (normalized 0-1)
- `node` (Map): Full node with properties (lazy-loaded)

**Examples:**

```cypher
-- Basic vector search with pre-computed embedding
CALL uni.vector.query('Document', 'embedding', [0.1, 0.2, 0.3, ...], 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC

-- Auto-embed text query (requires embedding config on index)
CALL uni.vector.query('Document', 'embedding', 'machine learning tutorial', 10)
YIELD node, score
RETURN node.title, score

-- With pre-filter
CALL uni.vector.query('Document', 'embedding', $query_vec, 10, 'status = "published"')
YIELD node, score
RETURN node.title, score

-- With threshold
CALL uni.vector.query('Document', 'embedding', $query_vec, 10, null, 0.7)
YIELD node, score
WHERE score > 0.7
RETURN node.title, score
```

**Auto-Embedding Requirements:**

To use text input for auto-embedding, create the vector index with an embedding configuration:

```cypher
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        provider: 'Candle',
        model: 'all-MiniLM-L6-v2',
        source: ['content']
    }
}
```

Supported providers:
- `Candle` - Native Rust embeddings via HuggingFace Candle (default: all-MiniLM-L6-v2, bge-small-en-v1.5, bge-base-en-v1.5)
- `FastEmbed` - ONNX-based embeddings (optional, requires `fastembed` feature flag)
- `ollama` - Local embeddings via Ollama (planned)
- `openai` - OpenAI embeddings API (planned)

---

## Full-Text Search (`uni.fts.*`)

BM25-based full-text search with relevance scoring.

### `uni.fts.query(label, property, search_term, k [, threshold])`

Perform full-text search using BM25 scoring algorithm.

**Parameters:**
- `label` (String): Node label to search
- `property` (String): Text property with inverted index
- `search_term` (String): Search query string
- `k` (Integer): Number of results to return
- `threshold` (Float, optional): Minimum normalized BM25 score (0-1)

**Yields:**
- `vid` (Integer): Vertex ID of matching node
- `score` (Float): Normalized BM25 score (0-1, relative to top match)
- `node` (Map): Full node with properties (lazy-loaded)

**Examples:**

```cypher
-- Basic full-text search
CALL uni.fts.query('Article', 'content', 'database optimization', 20)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC

-- With threshold filtering
CALL uni.fts.query('Article', 'content', 'graph algorithms', 10, 0.5)
YIELD node, score
RETURN node.title, score

-- Combine with graph traversal
CALL uni.fts.query('Document', 'content', 'machine learning', 10)
YIELD node, score
MATCH (node)-[:AUTHORED_BY]->(author:Person)
RETURN node.title, score, author.name
```

**Score Normalization:**

BM25 scores are normalized to the 0-1 range relative to the best match in the result set:
- `normalized_score = raw_score / max_score`
- Score of 1.0 indicates the best match
- Scores are comparable within a single query but not across queries

**Prerequisites:**

Requires a full-text index on the property:

```cypher
CREATE FULLTEXT INDEX article_content FOR (a:Article) ON (a.content)
```

---

## Hybrid Search (`uni.search.*`)

Combined vector and full-text search with rank fusion.

### `uni.search(label, properties, query_text [, query_vector] [, k] [, filter] [, options])`

Perform hybrid search combining vector similarity and full-text search using rank fusion.

**Parameters:**
- `label` (String): Node label to search
- `properties` (Map): Property mapping specifying which properties to search:
  - `vector` - Property containing embeddings
  - `fts` - Property containing text for full-text search
- `query_text` (String): Text query (used for FTS; auto-embedded for vector if no vector provided)
- `query_vector` (List<Float>, optional): Pre-computed query vector; if null, auto-embeds `query_text`
- `k` (Integer, optional): Number of results (default: 10)
- `filter` (String, optional): WHERE clause applied to both search branches
- `options` (Map, optional): Fusion configuration options

**Fusion Options:**
- `method` (String): Fusion algorithm
  - `'rrf'` - Reciprocal Rank Fusion (default, no tuning required)
  - `'weighted'` - Weighted linear combination
- `alpha` (Float): Vector weight for weighted fusion (default: 0.5)
  - `0.0` = FTS only
  - `0.5` = Equal weight
  - `1.0` = Vector only
- `over_fetch` (Float): Over-fetch factor for pagination (default: 2.0)

**Yields:**
- `vid` (Integer): Vertex ID of matching node
- `score` (Float): Fused relevance score
- `node` (Map): Full node with properties (lazy-loaded)
- `vector_score` (Float, optional): Individual vector similarity score
- `fts_score` (Float, optional): Individual BM25 score

**Examples:**

```cypher
-- Basic hybrid search with auto-embedding
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'machine learning optimization',
    null,  -- auto-embed the query text
    20
)
YIELD node, score
RETURN node.title, score

-- Hybrid search with pre-computed vector
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'machine learning',
    [0.1, 0.2, 0.3, ...],  -- pre-computed embedding
    10
)
YIELD node, score
RETURN node.title, score

-- With detailed score breakdown
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'neural networks',
    null,
    10
)
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score

-- With pre-filter
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'graph databases',
    null,
    10,
    'category = "technology"'
)
YIELD node, score
RETURN node.title, score

-- With weighted fusion (favor semantic similarity)
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'deep learning',
    null,
    10,
    null,
    {method: 'weighted', alpha: 0.7}
)
YIELD node, score
RETURN node.title, score
```

**Fusion Algorithms:**

| Method | Formula | Best For |
|--------|---------|----------|
| `rrf` | `Σ 1/(k + rank)` where k=60 | General use, no tuning needed |
| `weighted` | `α × vec_score + (1-α) × fts_score` | When you need to bias toward semantic or keyword match |

**RRF (Reciprocal Rank Fusion):**
- Robust, rank-based fusion that works well without tuning
- Each result gets a score based on its rank in each list
- Results appearing in both lists get boosted

**Weighted Fusion:**
- Linear combination of normalized scores
- Use `alpha > 0.5` to favor semantic/vector similarity
- Use `alpha < 0.5` to favor keyword/FTS matching

**Performance Notes:**
- Both branches execute in parallel
- Pre-filters are applied before search (not post-filter)
- Over-fetching ensures good fusion quality with pagination

---

## Database Administration (`uni.admin.*`)

Database maintenance, compaction, and snapshot management.

### `uni.admin.compact()`

Compact storage by merging L1 delta runs into L2 base CSR.

**Yields:**
- `files_compacted` (Integer): Number of files merged
- `bytes_before` (Integer): Total size before compaction
- `bytes_after` (Integer): Total size after compaction
- `duration_ms` (Integer): Compaction duration in milliseconds

**Example:**
```cypher
CALL uni.admin.compact()
  YIELD files_compacted, bytes_before, bytes_after
RETURN files_compacted, bytes_before, bytes_after
```

### `uni.admin.compactionStatus()`

Get current compaction statistics and status.

**Yields:**
- `l1_runs` (Integer): Number of L1 delta runs
- `l1_size_bytes` (Integer): Total L1 size
- `in_progress` (Boolean): Whether compaction is running
- `pending` (Boolean): Whether compaction is needed
- `total_compactions` (Integer): Lifetime compaction count
- `total_bytes_compacted` (Integer): Total bytes compacted lifetime

**Example:**
```cypher
CALL uni.admin.compactionStatus()
  YIELD l1_runs, l1_size_bytes, in_progress, pending
RETURN l1_runs, l1_size_bytes, in_progress, pending
```

### `uni.admin.snapshot.create([name])`

Create a named snapshot of current database state.

**Parameters:**
- `name` (String, optional): Snapshot name

**Yields:**
- `snapshot_id` (String): Generated snapshot ID

**Example:**
```cypher
CALL uni.admin.snapshot.create('backup-2026-01-26')
  YIELD snapshot_id
RETURN snapshot_id
```

### `uni.admin.snapshot.list()`

List all available snapshots.

**Yields:**
- `snapshot_id` (String): Snapshot identifier
- `name` (String or null): User-provided name
- `created_at` (Integer): Creation timestamp
- `version_hwm` (Integer): Version high water mark

**Example:**
```cypher
CALL uni.admin.snapshot.list()
  YIELD snapshot_id, name, created_at
RETURN snapshot_id, name, created_at
ORDER BY created_at DESC
```

### `uni.admin.snapshot.restore(snapshot_id)`

Restore database to a specific snapshot.

**Parameters:**
- `snapshot_id` (String): Snapshot to restore

**Yields:**
- `status` (String): Restore status message

**Example:**
```cypher
CALL uni.admin.snapshot.restore('01HJP3QW8XKZT4QGFXVM2C7D9R')
  YIELD status
RETURN status
```

---

## Schema Management (`uni.schema.*`)

Schema introspection and DDL operations.

### Schema Introspection

#### `uni.schema.labels()`

List all node labels with statistics.

**Yields:**
- `label` (String): Label name
- `propertyCount` (Integer): Number of properties
- `nodeCount` (Integer): Total nodes with this label
- `indexCount` (Integer): Number of indexes on this label

**Example:**
```cypher
CALL uni.schema.labels()
  YIELD label, nodeCount, propertyCount, indexCount
RETURN label, nodeCount, propertyCount, indexCount
ORDER BY nodeCount DESC
```

#### `uni.schema.edgeTypes()` / `uni.schema.relationshipTypes()`

List all edge types with metadata.

**Yields:**
- `type` (String): Edge type name
- `relationshipType` (String): Alias for `type`
- `sourceLabels` (List<String>): Allowed source labels
- `targetLabels` (List<String>): Allowed target labels
- `propertyCount` (Integer): Number of properties

**Example:**
```cypher
CALL uni.schema.edgeTypes()
  YIELD type, sourceLabels, targetLabels, propertyCount
RETURN type, sourceLabels, targetLabels, propertyCount
```

#### `uni.schema.indexes()`

List all indexes in the database.

**Yields:**
- `name` (String): Index name
- `type` (String): Index type (VECTOR, FULLTEXT, SCALAR, JSON_FTS)
- `label` (String): Label or edge type
- `properties` (List<String>): Indexed properties
- `state` (String): Index state (ONLINE, BUILDING, FAILED)

**Example:**
```cypher
CALL uni.schema.indexes()
  YIELD name, type, label, properties
RETURN name, type, label, properties
```

#### `uni.schema.constraints()`

List all constraints in the database.

**Yields:**
- `name` (String): Constraint name
- `type` (String): Constraint type (UNIQUE, EXISTS, CHECK)
- `enabled` (Boolean): Whether constraint is active
- `label` (String, optional): Node label
- `relationshipType` (String, optional): Edge type
- `properties` (List<String>): Constrained properties
- `expression` (String, optional): Check expression

**Example:**
```cypher
CALL uni.schema.constraints()
  YIELD name, type, enabled, label, properties
RETURN name, type, enabled, label, properties
```

#### `uni.schema.labelInfo(label)`

Get detailed property information for a label.

**Parameters:**
- `label` (String): Label name to inspect

**Yields:**
- `property` (String): Property name
- `dataType` (String): Data type
- `nullable` (Boolean): Whether property allows nulls
- `indexed` (Boolean): Whether property is indexed
- `unique` (Boolean): Whether property has unique constraint

**Example:**
```cypher
CALL uni.schema.labelInfo('Person')
  YIELD property, dataType, indexed, unique
RETURN property, dataType, indexed, unique
```

### Schema DDL Operations

#### `uni.schema.createLabel(name, config)`

Create a new node label with properties.

**Parameters:**
- `name` (String): Label name
- `config` (Map): Configuration with `properties` map

**Yields:**
- `success` (Boolean): Creation status

**Example:**
```cypher
CALL uni.schema.createLabel('Person', {
  properties: {
    name: 'String',
    age: 'Int64',
    email: 'String'
  }
})
YIELD success
RETURN success
```

#### `uni.schema.createEdgeType(name, sourceLabels, targetLabels, config)`

Create a new edge type.

**Parameters:**
- `name` (String): Edge type name
- `sourceLabels` (List<String>): Allowed source labels
- `targetLabels` (List<String>): Allowed target labels
- `config` (Map): Configuration with `properties` map

**Yields:**
- `success` (Boolean): Creation status

**Example:**
```cypher
CALL uni.schema.createEdgeType('KNOWS', ['Person'], ['Person'], {
  properties: {
    since: 'Int64',
    weight: 'Float64'
  }
})
YIELD success
RETURN success
```

#### `uni.schema.createIndex(label, property, config)`

Create an index on a property.

**Parameters:**
- `name` (String): Label name
- `property` (String): Property to index
- `config` (Map): Index configuration with `type` field

**Yields:**
- `success` (Boolean): Creation status

**Example:**
```cypher
-- Scalar index
CALL uni.schema.createIndex('Person', 'email', {type: 'scalar'})
  YIELD success
RETURN success

-- Vector index
CALL uni.schema.createIndex('Person', 'embedding', {
  type: 'vector',
  dimension: 384,
  metric: 'cosine'
})
YIELD success
RETURN success

-- Full-text index
CALL uni.schema.createIndex('Article', 'content', {
  type: 'fulltext',
  analyzer: 'standard'
})
YIELD success
RETURN success
```

#### `uni.schema.createConstraint(label, type, properties)`

Create a constraint on a label.

**Parameters:**
- `label` (String): Label name
- `type` (String): Constraint type ('unique', 'exists')
- `properties` (List<String>): Properties to constrain

**Yields:**
- `success` (Boolean): Creation status

**Example:**
```cypher
-- Unique constraint
CALL uni.schema.createConstraint('Person', 'unique', ['email'])
  YIELD success
RETURN success

-- Existence constraint
CALL uni.schema.createConstraint('Person', 'exists', ['name'])
  YIELD success
RETURN success
```

#### `uni.schema.dropLabel(name)`

Drop a node label and all its nodes.

**Parameters:**
- `name` (String): Label name

**Yields:**
- `success` (Boolean): Deletion status

**Example:**
```cypher
CALL uni.schema.dropLabel('TempNode')
  YIELD success
RETURN success
```

#### `uni.schema.dropEdgeType(name)`

Drop an edge type and all its edges.

**Parameters:**
- `name` (String): Edge type name

**Yields:**
- `success` (Boolean): Deletion status

**Example:**
```cypher
CALL uni.schema.dropEdgeType('TEMP_RELATION')
  YIELD success
RETURN success
```

#### `uni.schema.dropIndex(name)`

Drop an index by name.

**Parameters:**
- `name` (String): Index name

**Yields:**
- `success` (Boolean): Deletion status

**Example:**
```cypher
CALL uni.schema.dropIndex('person_email_idx')
  YIELD success
RETURN success
```

#### `uni.schema.dropConstraint(name)`

Drop a constraint by name.

**Parameters:**
- `name` (String): Constraint name

**Yields:**
- `success` (Boolean): Deletion status

**Example:**
```cypher
CALL uni.schema.dropConstraint('person_email_unique')
  YIELD success
RETURN success
```

---

## Temporal Queries (`uni.temporal.*`)

Temporal validity checking for bitemporal graph patterns.

### `uni.temporal.validAt(entity, startProp, endProp, timestamp)`

Check if a node or edge was valid at a specific point in time using half-open interval semantics: `[startProp, endProp)`.

**Parameters:**
- `entity` (Node or Edge): Entity to check
- `startProp` (String): Property containing start timestamp
- `endProp` (String): Property containing end timestamp
- `timestamp` (DateTime): Query timestamp

**Returns:** Boolean

**Semantics:**
- Returns `true` if `startProp <= timestamp < endProp`
- If `endProp` is NULL, the interval is open-ended (valid indefinitely)
- If `startProp` is NULL, returns `false`

**Example:**
```cypher
MATCH (e:Event)
WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime('2023-06-15'))
RETURN e.name, e.valid_from, e.valid_to
```

**Use Cases:**
- Temporal graph snapshots: "Show me the graph as it was on date X"
- Historical queries: "Which employees were active in Q2 2023?"
- Bitemporal versioning: "Find all valid relationships at a specific time"

---

## Graph Algorithms (`uni.algo.*`)

Comprehensive graph algorithm library for analytics and centrality.

### Centrality Algorithms

#### `uni.algo.pageRank([labels], [edgeTypes])`

Compute PageRank scores for nodes.

**Parameters:**
- `labels` (List<String>): Node labels to include
- `edgeTypes` (List<String>): Edge types to traverse

**Yields:**
- `nodeId` (Integer): Node VID
- `score` (Float): PageRank score

**Example:**
```cypher
CALL uni.algo.pageRank(['Person'], ['KNOWS'])
  YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 10
```

#### `uni.algo.betweenness([labels], [edgeTypes])`

Compute betweenness centrality.

**Yields:**
- `nodeId` (Integer): Node VID
- `score` (Float): Betweenness score

#### `uni.algo.closeness([labels], [edgeTypes])`

Compute closeness centrality.

**Yields:**
- `nodeId` (Integer): Node VID
- `score` (Float): Closeness score

#### `uni.algo.degreeCentrality([labels], [edgeTypes])`

Compute degree centrality.

**Yields:**
- `nodeId` (Integer): Node VID
- `score` (Float): Degree centrality score

#### `uni.algo.harmonicCentrality([labels], [edgeTypes])`

Compute harmonic centrality.

**Yields:**
- `nodeId` (Integer): Node VID
- `score` (Float): Harmonic centrality score

#### `uni.algo.eigenvectorCentrality([labels], [edgeTypes])`

Compute eigenvector centrality.

**Yields:**
- `nodeId` (Integer): Node VID
- `score` (Float): Eigenvector centrality score

#### `uni.algo.katzCentrality([labels], [edgeTypes])`

Compute Katz centrality.

**Yields:**
- `nodeId` (Integer): Node VID
- `score` (Float): Katz centrality score

### Community Detection

#### `uni.algo.wcc([labels], [edgeTypes])`

Weakly connected components.

**Yields:**
- `nodeId` (Integer): Node VID
- `componentId` (Integer): Component ID

**Example:**
```cypher
CALL uni.algo.wcc(['Node'], ['LINK'])
  YIELD nodeId, componentId
RETURN componentId, COUNT(nodeId) AS size
ORDER BY size DESC
```

#### `uni.algo.louvain([labels], [edgeTypes])`

Louvain modularity optimization for community detection.

**Yields:**
- `nodeId` (Integer): Node VID
- `communityId` (Integer): Community ID

#### `uni.algo.labelPropagation([labels], [edgeTypes])`

Label propagation algorithm for community detection.

**Yields:**
- `nodeId` (Integer): Node VID
- `communityId` (Integer): Community ID

#### `uni.algo.scc([labels], [edgeTypes])`

Strongly connected components.

**Yields:**
- `nodeId` (Integer): Node VID
- `componentId` (Integer): Component ID

#### `uni.algo.kcore([labels], [edgeTypes])`

K-core decomposition.

**Yields:**
- `nodeId` (Integer): Node VID
- `coreNumber` (Integer): K-core number

### Path Finding

#### `uni.algo.shortestPath(sourceId, targetId, [edgeTypes])`

Single-source shortest path (Dijkstra).

**Parameters:**
- `sourceId` (String or Integer): Source node ID
- `targetId` (String or Integer): Target node ID
- `edgeTypes` (List<String>): Edge types to traverse

**Yields:**
- `nodeId` (Integer): Node in path
- `distance` (Float): Distance from source

#### `uni.algo.allPairsShortestPath([labels], [edgeTypes])`

All-pairs shortest paths.

**Yields:**
- `sourceNodeId` (Integer): Source node
- `targetNodeId` (Integer): Target node
- `distance` (Float): Shortest path distance

#### `uni.algo.astar(sourceId, targetId, [edgeTypes])`

A* pathfinding algorithm.

**Yields:**
- `nodeId` (Integer): Node in path
- `distance` (Float): Distance from source

#### `uni.algo.bellmanFord(sourceId, [edgeTypes])`

Bellman-Ford shortest paths (handles negative weights).

**Yields:**
- `nodeId` (Integer): Node VID
- `distance` (Float): Distance from source

#### `uni.algo.bidirectionalDijkstra(sourceId, targetId, [edgeTypes])`

Bidirectional Dijkstra for faster pathfinding.

**Yields:**
- `nodeId` (Integer): Node in path
- `distance` (Float): Distance from source

#### `uni.algo.kShortestPaths(sourceId, targetId, k, [edgeTypes])`

K shortest paths between two nodes.

**Parameters:**
- `k` (Integer): Number of paths to find

**Yields:**
- `pathIndex` (Integer): Path rank (0 = shortest)
- `nodeId` (Integer): Node in path
- `distance` (Float): Path length

#### `uni.algo.allSimplePaths(sourceId, targetId, [edgeTypes])`

Find all simple paths (no repeated nodes).

**Yields:**
- `path` (List): Path as list of node IDs

### Graph Structure

#### `uni.algo.triangleCount([labels], [edgeTypes])`

Count triangles per node.

**Yields:**
- `nodeId` (Integer): Node VID
- `triangleCount` (Integer): Number of triangles

#### `uni.algo.hasCycle([labels], [edgeTypes])`

Detect cycles in directed graphs.

**Yields:**
- `hasCycle` (Boolean): Whether graph has cycles

#### `uni.algo.topologicalSort([labels], [edgeTypes])`

Topological ordering of directed acyclic graph.

**Yields:**
- `nodeId` (Integer): Node VID
- `order` (Integer): Topological order

#### `uni.algo.isBipartite([labels], [edgeTypes])`

Check if graph is bipartite.

**Yields:**
- `isBipartite` (Boolean): Bipartite status
- `partition` (Integer): Partition assignment (0 or 1)

#### `uni.algo.bridges([labels], [edgeTypes])`

Find bridge edges (critical connections).

**Yields:**
- `sourceNodeId` (Integer): Source node
- `targetNodeId` (Integer): Target node

#### `uni.algo.articulationPoints([labels], [edgeTypes])`

Find articulation points (cut vertices).

**Yields:**
- `nodeId` (Integer): Articulation point VID

#### `uni.algo.maximalCliques([labels], [edgeTypes])`

Find all maximal cliques.

**Yields:**
- `clique` (List): Clique as list of node IDs

#### `uni.algo.elementaryCircuits([labels], [edgeTypes])`

Find all elementary circuits.

**Yields:**
- `circuit` (List): Circuit as list of node IDs

### Graph Metrics

#### `uni.algo.graphMetrics([labels], [edgeTypes])`

Compute overall graph statistics.

**Yields:**
- `nodeCount` (Integer): Number of nodes
- `edgeCount` (Integer): Number of edges
- `density` (Float): Graph density
- `avgDegree` (Float): Average degree
- `maxDegree` (Integer): Maximum degree

#### `uni.algo.diameter([labels], [edgeTypes])`

Compute graph diameter (longest shortest path).

**Yields:**
- `diameter` (Integer): Graph diameter

#### `uni.algo.nodeSimilarity([labels], [edgeTypes])`

Compute pairwise node similarity scores.

**Yields:**
- `node1` (Integer): First node
- `node2` (Integer): Second node
- `similarity` (Float): Similarity score

### Optimization Algorithms

#### `uni.algo.mst([labels], [edgeTypes])`

Minimum spanning tree.

**Yields:**
- `sourceNodeId` (Integer): Edge source
- `targetNodeId` (Integer): Edge target
- `weight` (Float): Edge weight

#### `uni.algo.maxMatching([labels], [edgeTypes])`

Maximum matching in bipartite graphs.

**Yields:**
- `sourceNodeId` (Integer): Matched source
- `targetNodeId` (Integer): Matched target

#### `uni.algo.graphColoring([labels], [edgeTypes])`

Graph coloring with minimum colors.

**Yields:**
- `nodeId` (Integer): Node VID
- `color` (Integer): Assigned color

#### `uni.algo.dinic(sourceId, sinkId, [edgeTypes])`

Dinic's maximum flow algorithm.

**Parameters:**
- `sourceId` (String or Integer): Source node
- `sinkId` (String or Integer): Sink node

**Yields:**
- `maxFlow` (Float): Maximum flow value

#### `uni.algo.fordFulkerson(sourceId, sinkId, [edgeTypes])`

Ford-Fulkerson maximum flow.

**Yields:**
- `maxFlow` (Float): Maximum flow value

### Traversal

#### `uni.algo.randomWalk([labels], [edgeTypes], steps, walks)`

Generate random walks for graph sampling.

**Parameters:**
- `steps` (Integer): Walk length
- `walks` (Integer): Number of walks per node

**Yields:**
- `path` (List): Random walk path

---

## Bitwise Operations (`uni.bitwise.*`)

Integer bitwise operations for binary data manipulation.

### `uni.bitwise.or(a, b)`

Bitwise OR operation.

**Parameters:**
- `a` (Integer): First operand
- `b` (Integer): Second operand

**Returns:** Integer

**Example:**
```cypher
RETURN uni.bitwise.or(5, 3) AS result
// Result: 7 (0101 | 0011 = 0111)
```

### `uni.bitwise.and(a, b)`

Bitwise AND operation.

**Example:**
```cypher
RETURN uni.bitwise.and(12, 10) AS result
// Result: 8 (1100 & 1010 = 1000)
```

### `uni.bitwise.xor(a, b)`

Bitwise XOR operation.

**Example:**
```cypher
RETURN uni.bitwise.xor(5, 3) AS result
// Result: 6 (0101 ^ 0011 = 0110)
```

### `uni.bitwise.not(a)`

Bitwise NOT operation.

**Parameters:**
- `a` (Integer): Operand

**Returns:** Integer

**Example:**
```cypher
RETURN uni.bitwise.not(5) AS result
// Result: -6 (two's complement inversion)
```

### `uni.bitwise.shiftLeft(value, n)`

Left shift operation.

**Parameters:**
- `value` (Integer): Value to shift
- `n` (Integer): Number of positions

**Returns:** Integer

**Example:**
```cypher
RETURN uni.bitwise.shiftLeft(5, 2) AS result
// Result: 20 (0101 << 2 = 10100)
```

### `uni.bitwise.shiftRight(value, n)`

Right shift operation.

**Example:**
```cypher
RETURN uni.bitwise.shiftRight(20, 2) AS result
// Result: 5 (10100 >> 2 = 00101)
```

---

## Quick Reference by Use Case

### Database Maintenance
- **Compact storage**: `uni.admin.compact()`
- **Create backup**: `uni.admin.snapshot.create('name')`
- **Restore backup**: `uni.admin.snapshot.restore(id)`

### Schema Discovery
- **List labels**: `uni.schema.labels()`
- **List edge types**: `uni.schema.edgeTypes()`
- **Check indexes**: `uni.schema.indexes()`
- **View properties**: `uni.schema.labelInfo('Label')`

### Schema Management
- **Create label**: `uni.schema.createLabel(name, config)`
- **Create index**: `uni.schema.createIndex(label, prop, config)`
- **Add constraint**: `uni.schema.createConstraint(label, type, props)`
- **Drop label**: `uni.schema.dropLabel(name)`

### Search
- **Vector similarity**: `uni.vector.query(label, prop, vec_or_text, k)`
- **Full-text search**: `uni.fts.query(label, prop, search_term, k)`
- **Hybrid search**: `uni.search(label, {vector: prop1, fts: prop2}, query, null, k)`

### Graph Analytics
- **Most influential nodes**: `uni.algo.pageRank(...)`
- **Community detection**: `uni.algo.louvain(...)`
- **Shortest path**: `uni.algo.shortestPath(source, target)`
- **Connected components**: `uni.algo.wcc(...)`

### Temporal Queries
- **Historical graph state**: `WHERE uni.temporal.validAt(e, 'from', 'to', datetime(...))`
