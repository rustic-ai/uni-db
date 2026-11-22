# CLI Reference

Complete reference for the `uni` command-line interface. The CLI provides tools for data import, query execution, and snapshot management.

## Synopsis

```bash
uni [OPTIONS] [COMMAND]
```

With no command, `uni` starts the REPL on `./storage`.

## Global Options

| Option | Description |
|--------|-------------|
| `-h, --help` | Print help information |
| `--storage-option KEY=VALUE` | Object-store option for a store opened at an `s3://`, `gs://` or `az://` path (for example `aws_endpoint=http://localhost:9000`). Repeatable; overrides the environment. Accepted before or after the command. Prefer the environment for secrets, which would otherwise land in shell history |

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

### `query` — Run a Query

Run an OpenCypher query against a database.

#### Synopsis

```bash
uni query <STATEMENT> [--path <PATH>]
```

#### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--path <PATH>` | Local directory or object-store URI (`s3://`, `gs://`, `az://`, `file://`) | `./storage` |

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
| `--path <PATH>` | Local directory or object-store URI (`s3://`, `gs://`, `az://`, `file://`) | `./storage` |

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
uni snapshot [--path <PATH>] <SUBCOMMAND>
```

`--path` belongs to `snapshot`, so it goes before the subcommand.

#### Subcommands

**`list`** — List all available snapshots.
```bash
uni snapshot --path ./storage list
```

**`create`** — Create a new named snapshot. The `<NAME>` argument is required.
```bash
uni snapshot --path ./storage create <NAME>
# e.g.
uni snapshot --path ./storage create "nightly"
```

**`restore`** — Restore the database to a specific snapshot ID.
```bash
uni snapshot --path ./storage restore <ID>
```

---

### `plugin` — Manage Runtime-Loaded Plugins

Install runtime-loaded plugins into a database.

#### Synopsis

```bash
uni plugin [--path <PATH>] <SUBCOMMAND>
```

`--path` belongs to `plugin`, so it goes before the subcommand.

#### Subcommands

**`install`** — Install a plugin from a local file.

```bash
uni plugin --path ./storage install <SOURCE> [--grants <NAMES>]
```

| Argument / Option | Description | Default |
|-------------------|-------------|---------|
| `<SOURCE>` | Local path to install from. Dispatched by extension: `*.rhai` is loaded today. `*.wasm`, `http://`, `https://`, `oci://…` and `extism://…` sources are refused as not yet supported. | — |
| `--grants <NAMES>` | Comma-separated capability grant names (e.g. `ScalarFn,Filesystem,Network`). | `ScalarFn,AggregateFn,Procedure` |

Available grant names include `ScalarFn`, `AggregateFn`, `Procedure`, `Filesystem`, `Network`, `HostQuery`, `Kms`, and `Secret`.

```bash
# Install a Rhai plugin with the default scalar/aggregate/procedure grants
uni plugin --path ./storage install ./my_plugin.rhai

# Grant filesystem and network access as well
uni plugin --path ./storage install ./my_plugin.rhai --grants "ScalarFn,Filesystem,Network"
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
