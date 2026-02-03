# CLI Reference

Complete reference for the `uni` command-line interface. The CLI provides tools for data import, query execution, and server management.

## Synopsis

```bash
uni <COMMAND> [OPTIONS]
```

## Global Options

| Option | Description |
|--------|-------------|
| `-h, --help` | Print help information |
| `-V, --version` | Print version information |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Log level filter (e.g., `info`, `debug`, `uni=debug`) |
| `AWS_REGION` / `AWS_DEFAULT_REGION` | AWS region for S3 access |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | AWS credentials |
| `AWS_SESSION_TOKEN` | AWS session token (optional) |
| `AWS_ENDPOINT_URL` | Custom S3 endpoint (MinIO/LocalStack) |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCP service account JSON path |
| `AZURE_STORAGE_ACCOUNT` | Azure storage account |
| `AZURE_STORAGE_ACCESS_KEY` | Azure access key |
| `AZURE_STORAGE_SAS_TOKEN` | Azure SAS token |

---

## Commands

### `import` — Import Data

Import data from JSONL files into a new or existing database.

#### Synopsis

```bash
uni import <NAME> --papers <PATH> --citations <PATH> [--output <PATH>]
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Dataset name identifier (e.g., `semantic-scholar`) |

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--papers <PATH>` | Path to vertices JSONL file | — |
| `--citations <PATH>` | Path to edges JSONL file | — |
| `--output <PATH>` | Output directory for storage | `./storage` |

#### Examples

```bash
uni import semantic-scholar \
  --papers demos/demo01/data/papers.jsonl \
  --citations demos/demo01/data/citations.jsonl \
  --output ./storage
```

---

### `query` — Execute Queries

Run OpenCypher queries against a database.

#### Synopsis

```bash
uni query <STATEMENT> [--path <PATH>]
```

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--path <PATH>` | Storage directory path | `./storage` |

#### Example

```bash
uni query "MATCH (n:Person) RETURN n.name LIMIT 10" --path ./social-graph
```

---

### `repl` — Interactive Shell

Start the interactive UniDB shell for running Cypher queries.

#### Synopsis

```bash
uni repl [--path <PATH>]
# or simply
uni
```

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--path <PATH>` | Storage directory path | `./storage` |

#### Shell Commands

| Command | Description |
|---------|-------------|
| `help` | Show available commands |
| `clear` | Clear the screen |
| `exit`, `quit` | Exit the REPL |
| `<cypher>` | Execute a Cypher query |

---

### `snapshot` — Manage Snapshots

Create, list, and restore database snapshots.

#### Synopsis

```bash
uni snapshot <SUBCOMMAND> [--path <PATH>]
```

#### Subcommands

**`list`** — List all available snapshots.
```bash
uni snapshot list --path ./storage
```

**`create`** — Create a new named snapshot.
```bash
uni snapshot create --path ./storage
# or
uni snapshot create "nightly" --path ./storage
```

**`restore`** — Restore the database to a specific snapshot ID.
```bash
uni snapshot restore <ID> --path ./storage
```

---

### `start` — Start Server

Start the Uni HTTP API server for remote access.

#### Synopsis

```bash
uni start [OPTIONS]
```

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--port <PORT>` | HTTP port to listen on | 8080 |
| `--path <PATH>` | Storage directory path | `./storage` |
| `--cors-origins <ORIGINS>` | Allowed CORS origins (comma-separated, `*` for any) | `http://localhost:3000` |
| `--api-key <KEY>` | Require `X-API-Key` header for API requests | (unset) |

#### Examples

**Start on default port:**
```bash
uni start --path ./my-graph
```

**Restrict CORS + require API key:**
```bash
uni start \
  --path ./my-graph \
  --cors-origins "https://myapp.example" \
  --api-key "secret"
```

#### API Endpoints

When the server is running:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Web UI (static) |
| `/assets/*` | GET | Static assets |
| `/health` | GET | Health check |
| `/api/v1/query` | POST | Execute Cypher query |
| `/api/v1/metrics` | GET | Prometheus metrics |

**Query API example:**
```bash
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -H "X-API-Key: secret" \
  -d '{"query": "MATCH (n) RETURN COUNT(n)"}'
```

---

### Schema and Index Management

Schema inspection and index management are available through Cypher queries in the REPL or via the `query` command:

```bash
# View labels
uni query "CALL uni.schema.labels() YIELD label RETURN label" --path ./graph

# View relationship types
uni query "CALL uni.schema.relationshipTypes() YIELD relationshipType RETURN relationshipType" --path ./graph

# View indexes
uni query "SHOW INDEXES" --path ./graph
```

**Create indexes:**
```bash
# Vector index (HNSW)
uni query "CREATE VECTOR INDEX paper_embeddings FOR (p:Paper) ON p.embedding OPTIONS { type: 'hnsw' }" --path ./graph

# Scalar index
uni query "CREATE INDEX author_name FOR (a:Author) ON (a.name)" --path ./graph
```

---

## See Also

- [Quick Start](quickstart.md) — Tutorial introduction
- [Cypher Querying](../guides/cypher-querying.md) — Query language reference
- [Configuration](../reference/configuration.md) — Configuration options
