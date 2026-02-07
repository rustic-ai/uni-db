# Troubleshooting Guide

This guide covers common issues, error messages, and their solutions when working with Uni.

## Quick Diagnostics

```bash
# Check database info
uni query "SHOW DATABASE" --path ./storage

# Check schema
uni query "CALL uni.schema.labels() YIELD label RETURN label" --path ./storage

# Basic stats
uni query "SHOW STATISTICS" --path ./storage

# View recent logs
RUST_LOG=uni_db=debug uni query "RETURN 1" --path ./storage 2>&1 | tail -50
```

---

## Common Issues

### Installation Problems

#### Rust Version Incompatible

**Symptom:**
```
error: package `uni-db v0.1.0` cannot be built because it requires rustc 1.75 or newer
```

**Solution:**
```bash
# Update Rust
rustup update stable

# Verify version
rustc --version  # Should be 1.75+
```

#### Missing System Dependencies

**Symptom:**
```
error: failed to run custom build command for `openssl-sys`
```

**Solution:**
```bash
# Ubuntu/Debian
sudo apt install pkg-config libssl-dev

# macOS
brew install openssl
export OPENSSL_DIR=$(brew --prefix openssl)

# Fedora
sudo dnf install openssl-devel
```

#### Build Fails with SIMD Errors

**Symptom:**
```
error: unknown feature 'avx2'
```

**Solution:**
```bash
# Build without SIMD optimizations
cargo build --release --no-default-features

# Or specify target CPU
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

---

### Storage Issues

#### Cannot Open Storage

**Symptom:**
```
Error: Failed to open storage at ./storage: No such file or directory
```

**Solution:**
```bash
# Check path exists
ls -la ./storage

# Create storage (embedded mode)
uni query "RETURN 1" --path ./storage
```

```rust
// Or create programmatically
let db = Uni::open("./storage").build().await?;
```

#### Corrupted Storage

**Symptom:**
```
Error: Invalid manifest at version 42: CRC mismatch
```

**Solution:**
```bash
# Restore from snapshot (recommended)
uni snapshot list --path ./storage
uni snapshot restore <snapshot_id> --path ./storage

# Or via Cypher
uni query "CALL uni.admin.snapshot.list()" --path ./storage
uni query "CALL uni.admin.snapshot.restore('<snapshot_id>')" --path ./storage

# If no snapshot exists, re-import or rebuild the database
```

#### Out of Disk Space

**Symptom:**
```
Error: Failed to write to storage: No space left on device
```

**Solution:**
```bash
# Check disk usage
df -h ./storage

# Compact storage
uni query "CALL uni.admin.compact()" --path ./storage

# Move storage to a larger disk (stop any running processes first)
mv ./storage /larger-disk/storage
```

---

### Query Issues

#### Parse Errors

**Symptom:**
```
Error: Parse error at line 1, column 15: unexpected token
```

**Common Causes:**

1. **Missing quotes around strings:**
   ```cypher
   // Wrong
   WHERE p.title = My Paper

   // Correct
   WHERE p.title = 'My Paper'
   ```

2. **Wrong comparison operator:**
   ```cypher
   // Wrong (SQL style)
   WHERE p.year == 2023

   // Correct (Cypher style)
   WHERE p.year = 2023
   ```

3. **Missing relationship direction:**
   ```cypher
   // Wrong
   MATCH (a)-[r]-(b)  // Ambiguous in some contexts

   // Better (explicit direction)
   MATCH (a)-[r]->(b)
   ```

#### Semantic Errors

**Symptom:**
```
Error: Unknown label 'Paper'
```

**Solution:**
```bash
# List available labels
uni query "CALL uni.schema.labels() YIELD label RETURN label" --path ./storage

# Check spelling/case
# Labels are case-sensitive: Paper != paper
```

**Symptom:**
```
Error: Unknown property 'year' for label 'Paper'
```

**Solution:**
```bash
# List properties for label
uni query "CALL uni.schema.labelInfo('Paper')" --path ./storage

# Add property to schema if needed
uni query "ALTER LABEL Paper ADD PROPERTY year INT32" --path ./storage
```

#### Query Timeout

**Symptom:**
```
Error: Query timeout after 300 seconds
```

**Solutions:**

1. **Add LIMIT:**
   ```cypher
   MATCH (p:Paper)-[:CITES]->(cited)
   RETURN p.title, cited.title
   LIMIT 1000  -- Add limit
   ```

2. **Add filters:**
   ```cypher
   MATCH (p:Paper)-[:CITES]->(cited)
   WHERE p.year > 2020  -- Filter early
   RETURN p.title, cited.title
   ```

3. **Increase timeout:**
   ```rust
   use uni_db::UniConfig;

   let mut config = UniConfig::default();
   config.query_timeout = Duration::from_secs(600);  // 10 minutes
   ```

4. **Check query plan:**
   ```bash
   uni query "EXPLAIN MATCH (p:Paper)..." --path ./storage
   # Look for missing indexes
   ```

#### Out of Memory

**Symptom:**
```
Error: Query execution failed: Out of memory
```

**Solutions:**

1. **Reduce result size:**
   ```cypher
   MATCH (p:Paper)
   RETURN p.title  -- Only needed columns
   LIMIT 10000     -- Reasonable limit
   ```

2. **Stream results:**
   ```rust
   let mut cursor = db.query_with(query).query_cursor().await?;
   while let Some(batch) = cursor.next_batch().await {
       let rows = batch?;
       // Process a batch at a time
   }
   ```

3. **Increase memory limit:**
   ```rust
   use uni_db::UniConfig;

   let mut config = UniConfig::default();
   config.max_query_memory = 8 * 1024 * 1024 * 1024;  // 8 GB
   ```

4. **Reduce batch size:**
   ```rust
   use uni_db::UniConfig;

   let mut config = UniConfig::default();
   config.batch_size = 1024;  // Smaller batches
   ```

---

### Index Issues

#### Vector Search Returns Poor Results

**Symptom:** Results are not semantically similar to the query.

**Causes and Solutions:**

1. **Embedding model mismatch:** Ensure the same embedding model is used for indexing and querying.
   - Auto-embedding uses `Candle` (default) or `FastEmbed` (optional feature). `openai`/`ollama` configs are planned but not yet implemented.

2. **Dimension mismatch:**
   ```bash
   # Check schema dimensions
   uni query "CALL uni.schema.labelInfo('Paper')" --path ./storage
   ```
   ```rust
   // Ensure query vector matches
   assert_eq!(query_vec.len(), 768);
   ```

3. **Index mismatch or missing index:**
   ```bash
   # Check indexes
   uni query "SHOW INDEXES" --path ./storage
   # or
   uni query "CALL uni.schema.indexes()" --path ./storage
   ```
   If you need a different **distance metric**, recreate the index via Rust/Python APIs (Cypher DDL uses cosine today).

#### Index Not Being Used

**Symptom:** Query is slow despite having an index.

**Diagnosis:**
```bash
uni query "EXPLAIN MATCH (p:Paper) WHERE p.year > 2020 RETURN p" --path ./storage
```

**Common Causes:**

1. **Function on indexed column:**
   ```cypher
   // Index NOT used (function applied to column)
   WHERE LOWER(p.venue) = 'neurips'

   // Index used
   WHERE p.venue = 'NeurIPS'
   ```

2. **OR conditions:**
   ```cypher
   // May not use index efficiently
   WHERE p.year > 2020 OR p.venue = 'NeurIPS'

   // Better: split into UNION (if supported)
   ```

3. **Leading wildcard:**
   ```cypher
   // Cannot use index
   WHERE p.title CONTAINS 'attention'

   // Can use full-text index (if available)
   ```

4. **Low selectivity:**
   ```cypher
   // If 80% of data matches, full scan may be faster
   WHERE p.year > 2000
   ```

#### Index Build Fails

**Symptom:**
```
Error: Failed to build vector index: out of memory
```

**Solutions:**

1. **Use IVF_PQ or Flat instead of HNSW:**
   ```cypher
   CREATE VECTOR INDEX paper_embeddings
   FOR (p:Paper) ON p.embedding
   OPTIONS { type: 'ivf_pq' }  -- Less memory
   ```

2. **Build asynchronously after bulk load:**
   ```rust
   let _task_id = db.rebuild_indexes("Paper", true).await?;
   ```
   Or use `bulk_writer().async_indexes(true)`.

---

### Import Issues (CLI)

The CLI importer expects the **Semantic Scholar demo format** (`vid`, `src_vid`, `dst_vid`).

#### Missing Required Fields

**Symptom:**
```
Error: Missing vid/src_vid/dst_vid
```

**Solution:** Ensure your JSONL includes:
- Papers: `vid`, `title`, `year`, `citation_count`, `embedding`
- Citations: `src_vid`, `dst_vid`

#### Invalid VID References

**Symptom:**
```
Error: Missing src_vid or dst_vid
```

**Solution:** Ensure citations reference existing paper VIDs.

#### Embedding Dimension Mismatch

**Symptom:**
```
Error: Vector dimension mismatch: expected 768, got 384
```

**Solution:**
```bash
# Check schema
uni query "CALL uni.schema.labelInfo('Paper')" --path ./storage
```
If you need a different dimension, create a new label/property and re-import.

---

### Performance Issues

#### Slow Traversals

**Symptom:** Graph traversals take >100ms.

**Diagnosis:**
```bash
uni query "PROFILE MATCH (p:Paper)-[:CITES]->(c) RETURN COUNT(c)" --path ./storage
```

**Solutions:**

1. **Warm the adjacency cache:**
   ```cypher
   // Run a small traversal once to warm cache
   MATCH (p:Paper)-[:CITES]->(c) RETURN COUNT(c)
   ```

2. **Increase cache size:**
   ```rust
   use uni_db::UniConfig;

   let mut config = UniConfig::default();
   config.cache_size = 2_000_000_000; // bytes
   ```

3. **Add LIMIT to multi-hop:**
   ```cypher
   MATCH (p:Paper)-[:CITES*1..3]->(end)
   RETURN DISTINCT end
   LIMIT 1000
   ```

#### High Memory Usage

**Symptom:** Process using more memory than expected.

**Diagnosis:** Use OS-level tools (`top`, `htop`) and `PROFILE` output.

**Solutions:**

1. **Reduce cache sizes:**
   ```rust
   use uni_db::UniConfig;

   let mut config = UniConfig::default();
   config.cache_size = 500_000_000; // bytes
   ```

2. **Flush L0 more frequently:**
   ```rust
   use uni_db::UniConfig;

   let mut config = UniConfig::default();
   config.auto_flush_threshold = 5_000;
   config.auto_flush_interval = Some(Duration::from_secs(2));
   ```

3. **Use streaming queries:**
   ```rust
   let mut cursor = db.query_with(query).query_cursor().await?;
   while let Some(batch) = cursor.next_batch().await {
       let _rows = batch?;
       // Process results without loading all into memory
   }
   ```

---

## Error Reference

### Storage / IO Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `DatabaseLocked` | Another process holds the lock | Close other process or use a different path |
| `ReadOnly` | Write attempted on a read-only database | Open in write mode or avoid writes |
| `Storage` | Underlying storage failure | Check storage config/permissions |
| `Io` | OS I/O error (e.g., disk full) | Fix disk/permissions |
| `NotFound` | Database path does not exist | Create the DB or fix the path |

### Query / Schema Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Parse` | Invalid Cypher syntax | Fix query syntax |
| `Schema` | Invalid schema definition | Fix schema input |
| `LabelNotFound` | Label not in schema | Check schema or create label |
| `EdgeTypeNotFound` | Edge type not in schema | Check schema or create edge type |
| `PropertyNotFound` | Property not in schema | Check schema or fix query |
| `Type` | Incompatible types (e.g., vector dims mismatch) | Fix query or data types |
| `InvalidArgument` | Invalid procedure argument | Fix argument values |
| `IndexNotFound` | Index does not exist | Create index first |
| `Constraint` | Constraint violation | Fix data or drop constraint |
| `Timeout` | Query exceeded time limit | Optimize query or increase timeout |
| `MemoryLimitExceeded` | Memory limit exceeded | Reduce result size or increase limit |

---

## Debugging Tips

### Enable Verbose Logging

```bash
# All Uni logs at debug level
RUST_LOG=uni_db=debug uni query "..." --path ./storage

# Specific module
RUST_LOG=uni_db::storage=trace,uni_db::query=debug uni query "..."

# Include Lance logs
RUST_LOG=uni_db=debug,lance=info uni query "..."
```

### Query Profiling

```bash
# Get execution profile
uni query "PROFILE MATCH (p:Paper) WHERE p.year > 2020 RETURN COUNT(p)" --path ./storage

# Output shows:
# - Time per operator
# - Rows processed
# - Index usage
# - Memory usage
```

### Storage Inspection

```bash
# List all datasets
ls -la ./storage/vertices/
ls -la ./storage/edges/
ls -la ./storage/adjacency/

# Check Lance dataset info
# View on-disk schema
cat ./storage/catalog/schema.json | jq .
```

### Memory Profiling

```bash
# Run with verbose logging
RUST_LOG=uni_db=debug uni query "..."

# Use heaptrack (Linux)
heaptrack uni query "..."
heaptrack_gui heaptrack.uni.*.gz
```

---

## Getting Help

### Resources

- **Documentation**: [https://uni.dev/docs](https://uni.dev/docs)
- **GitHub Issues**: [https://github.com/rustic-ai/uni/issues](https://github.com/rustic-ai/uni/issues)
- **Discussions**: [https://github.com/rustic-ai/uni/discussions](https://github.com/rustic-ai/uni/discussions)

### Reporting Bugs

When reporting issues, include:

1. **Uni version:** `uni --version`
2. **Rust version:** `rustc --version`
3. **OS and version**
4. **Minimal reproduction steps**
5. **Error messages (full output)**
6. **Query plan** (if query-related): `EXPLAIN ...`
7. **Storage stats**: `uni stats --path ./storage`

---

## Next Steps

- [Configuration Reference](configuration.md) — All configuration options
- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
- [Glossary](glossary.md) — Terminology reference
