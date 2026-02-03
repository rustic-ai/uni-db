# Demo 01: One Query to Rule Them All

This demo showcases the capabilities of **Uni**, specifically:
1.  **Hybrid Storage**: Storing Graph (Adjacency), Document (JSON), and Vector data.
2.  **Cypher Query**: Executing queries that combine property filtering and graph traversal.
3.  **CLI Loading**: Importing data from JSONL files.

## Prerequisites

- Rust (cargo)
- Python 3
- `pip install faker numpy`

## Running the Demo

Simply run the shell script:

```bash
./run_demo.sh
```

This will:
1.  Generate synthetic data (papers and citations) in `data/`.
2.  Build the `uni` database engine.
3.  Import the data into `storage/`.
4.  Execute sample queries.

## Data Schema

- **Vertices**: `Paper`
    - Properties: `title`, `year`, `citation_count`, `embedding` (768d vector)
    - Document: `_doc` (venue, authors, etc.)
- **Edges**: `CITES` (Paper -> Paper)
