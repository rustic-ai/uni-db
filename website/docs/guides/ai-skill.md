# AI Agent Skill

Uni ships an agent skill (`uni-db`) that gives AI coding assistants deep knowledge of the Uni API, Cypher dialect, Locy language, vector/hybrid search, Pydantic OGM, graph algorithms, and schema design. With the skill installed, your AI assistant can build Uni-powered applications without you having to paste documentation into every conversation.

The skill works with **Claude Code, GitHub Copilot, Cursor, Cline**, and other agents that support the [skills.sh](https://skills.sh/) ecosystem.

## What the skill provides

The skill uses progressive disclosure — a compact quick-reference is always loaded (~450 lines), and detailed references are loaded on demand only when relevant to the task:

| Reference | Covers |
|-----------|--------|
| Cypher reference | Full clause syntax, operators, built-in functions, window functions, DDL, time travel, EXPLAIN/PROFILE |
| Python API | Uni/AsyncUni, Session, Transaction, query builders, Locy builders, schema builders, bulk ops, result types, Xervo ML runtime |
| Rust API | Uni, UniBuilder, Session, Transaction, query/execute/Locy builders, schema types, blocking API, error types |
| Pydantic OGM | UniNode/UniEdge models, relationships, Vector[N] type, QueryBuilder, filter expressions, lifecycle hooks, schema generation |
| Vector & hybrid search | `uni.vector.query`, `similar_to()`, `uni.fts.query`, `uni.search` (RRF/weighted fusion), index configuration (HNSW/IVF-PQ/Flat), auto-embedding |
| Locy reference | CREATE RULE syntax, IS references, YIELD/KEY/PROB, ALONG, FOLD (MNOR/MPROD), BEST BY, PRIORITY, DERIVE, ASSUME, ABDUCE, EXPLAIN RULE |
| Schema & indexing | Identity model (ext_id/VID/UniId), complete data type table, CRDT types, all index types, predicate pushdown, schema introspection |
| Graph algorithms | 36+ algorithms (path, centrality, community, similarity, structural, flow), execution modes, configuration |

## Installation

### From the skills.sh directory

```bash
npx skills add https://github.com/rustic-ai/uni-db --skill uni-db
```

This fetches the skill from the [Uni repository](https://github.com/rustic-ai/uni-db) and installs it into your agent's configuration. Works with any agent that supports the skills.sh directory.

### Local installation (Claude Code)

If you have the Uni repository cloned locally, point your Claude Code settings to the skill directory:

```json
{
  "skills": ["/path/to/uni-db/skills/uni-db"]
}
```

## When the skill triggers

The skill activates automatically when the agent detects any of these signals:

- Code imports `uni_db` or `uni_pydantic` (Python) or depends on the `uni-db` crate (Rust)
- You mention "uni", "uni-db", or "embedded graph database" in context of code
- You write or ask about Cypher queries for a graph database
- You work with Locy programs or mention ALONG/FOLD/BEST BY/DERIVE/ASSUME/ABDUCE
- You need vector search, full-text search, or hybrid search on a graph
- You ask about graph algorithms like PageRank, shortest path, or community detection
- You ask about schema design, data types, indexes, or Pydantic OGM models

## Usage examples

Once installed, just ask your agent what you need. The skill provides context automatically.

**Build a schema:**
```
Design a schema for a social network with users, posts, and follows.
Include vector embeddings on posts for semantic search.
```

**Write queries:**
```
Write a Cypher query that finds friends-of-friends who liked
the same posts, ordered by overlap count.
```

**Set up hybrid search:**
```
Add hybrid search (vector + full-text) over a Document label
with a 768-dim embedding and an abstract field. Use RRF fusion.
```

**Write Locy rules:**
```
Write Locy rules for transitive risk propagation through
a supply chain network using MNOR to combine probabilities.
```

**Pydantic OGM models:**
```
Using uni-pydantic, model a knowledge graph with Document nodes
that have vector embeddings and Category nodes connected by
TAGGED edges. Include lifecycle hooks and vector search.
```

**Graph algorithms:**
```
Run PageRank on my user network to find the most influential users,
then find communities using Louvain with a minimum size of 5.
```

## Architecture

The skill is organized as a single `SKILL.md` entry point with 12 reference files in a `references/` directory:

```
skills/uni-db/
├── SKILL.md                      ← always loaded (routing + quick reference)
└── references/
    ├── btic.md                   ← loaded for temporal-interval tasks
    ├── cypher.md                 ← loaded for Cypher tasks
    ├── forks.md                  ← loaded for fork tasks
    ├── graph-algorithms.md       ← loaded for algorithm tasks
    ├── locy.md                   ← loaded for Locy tasks
    ├── neural-predicates.md      ← loaded for neural-predicate tasks
    ├── pydantic-ogm.md           ← loaded for Pydantic OGM tasks
    ├── python-api.md             ← loaded for Python API tasks
    ├── rust-api.md               ← loaded for Rust API tasks
    ├── schema-indexing.md        ← loaded for schema/index tasks
    ├── vector-hybrid-search.md   ← loaded for search tasks
    └── xervo.md                  ← loaded for embedding/model tasks
```

The agent reads `SKILL.md` first, which contains a routing table that maps your task to the right reference file. Only the relevant reference is loaded into context, keeping token usage efficient.

## Updating the skill

To get the latest version after a Uni release, re-run the install command:

```bash
npx skills add https://github.com/rustic-ai/uni-db --skill uni-db
```
