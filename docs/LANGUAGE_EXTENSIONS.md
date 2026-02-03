# Uni Cypher Language Extensions

This document lists Uni-specific extensions beyond OpenCypher.

## Operators

- `~=` Approximate vector match. Example: `n.embedding ~= $query`.

## Functions

- `vector_similarity(a, b)` returns cosine similarity of two vectors.
- `vector_distance(a, b [, metric])` returns distance using `cosine`, `l2`, or `dot`.
- `uni.validAt(node, start_prop, end_prop, time)` checks temporal validity.

## Procedures

### Algorithms

- `CALL algo.*` runs graph algorithms like PageRank, WCC, Louvain, shortest path, etc.

### Vector Search

- `CALL db.idx.vector.query(label, property, vector, k)` performs KNN search.

### Admin and Metadata

- `CALL db.compact()` and `CALL db.compactionStatus()`.
- `CALL db.snapshot.create([name])`, `CALL db.snapshot.list()`, `CALL db.snapshot.restore(id)`.
- `CALL db.labels()`, `CALL db.edgeTypes()` (alias: `db.relationshipTypes()`).
- `CALL db.indexes()`, `CALL db.constraints()`, `CALL db.schema.labelInfo(label)`.

### DDL via Procedures

- `CALL db.createLabel(name, config)`, `CALL db.createEdgeType(name, src, dst, config)`.
- `CALL db.createIndex(label, property, config)`, `CALL db.dropIndex(name)`.
- `CALL db.createConstraint(label, type, properties)`, `CALL db.dropConstraint(name)`.
- `CALL db.dropLabel(name)`, `CALL db.dropEdgeType(name)`.

## DDL and Admin Clauses

- Indexes: `CREATE VECTOR INDEX`, `CREATE FULLTEXT INDEX`, `CREATE JSON FULLTEXT INDEX`,
  `CREATE SCALAR INDEX`, `DROP INDEX`, `SHOW INDEXES`.
- Schema: `CREATE LABEL`, `CREATE EDGE TYPE`, `ALTER LABEL`, `ALTER EDGE TYPE`,
  `DROP LABEL`, `DROP EDGE TYPE`.
- Constraints: `CREATE CONSTRAINT`, `DROP CONSTRAINT`, `SHOW CONSTRAINTS`.
- Utilities: `COPY`, `BACKUP`, `SHOW DATABASE`, `SHOW CONFIG`, `SHOW STATISTICS`,
  `VACUUM`, `CHECKPOINT`.
- Transactions: `BEGIN`, `COMMIT`, `ROLLBACK`.
- Recursive CTE: `WITH RECURSIVE`.

## Document and JSON Extensions

- JSON full-text predicates such as `n._doc CONTAINS 'term'`, including path-specific
  form like `n._doc.title CONTAINS 'term'`.
