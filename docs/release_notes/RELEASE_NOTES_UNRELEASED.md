# uni-db — unreleased

Changes since [5.3.0](RELEASE_NOTES_5.3.0.md).

Add new changes here.

## Fixes

- **Cypher:** `a - b` on two dates or two datetimes gives the duration from `b` to `a`; it was negated.
- **Cypher:** window functions work in `WITH`, including its `WHERE` and `ORDER BY`. In an aggregating `WITH` or `RETURN`, a window runs over the grouped rows (`rank() OVER (ORDER BY count(*))`, `sum(count(*)) OVER ()`).
- **Functions:** `pow(x, y)` / `power(x, y)` accept integers and return a float, like `^`.
- **Functions:** `uni.bitwise.shiftLeft` / `shiftRight` can be called; a shift amount outside 0..63 is an error. `uni.bitwise.*` accepts a column mixed with a literal and returns NULL for a NULL operand.
- **Functions:** `btic_lo` / `btic_hi` raise an error for a bound outside the datetime range (about 1677 to 2262) instead of returning a wrapped value.
- **Functions:** `similar_to` scores sparse-vector properties (dot product) and `LIST<VECTOR>` properties (MaxSim); a sparse vector passed as a parameter is no longer NULL.
- **Traversal:** the 100-hop default-bound warning is raised for schemaless edge types too.
- **Schema:** `SHOW INDEXES` and `uni.schema.indexes()` name inverted, JSON full-text and sparse indexes; `uni.schema.constraints()` fills `target`; `DROP INDEX IF EXISTS` on a missing index succeeds.
- **Schema:** `uni.schema.createLabel` requires `VECTOR(N)` with N > 0, and a vector index over a property without a dimension is refused. An existing vector index stored with dimension 0 fails to rebuild until its property is declared with a dimension.
- **Algorithms:** the id outputs of `bridges`, `articulationPoints`, `mst`, `maxMatching` and `dijkstra` are declared INTEGER, which is what they return.
- **OGM:** a `Vector[N] | None` field is a nullable vector column, so saving a node without a vector no longer fails with "cannot be null".
- **OGM:** `SparseVector[N]` fields are saved as `uni_db.SparseVector`; a `TypeMappingError` is raised if the bindings lack that class.
- **OGM:** `tx.create_edge` accepts nodes added to the same transaction (sync and async). A failed commit clears the vid, session and identity-map entry of the nodes it had created.
- **CLI:** `uni import <name>` runs the importer the name selects; an unknown name fails and lists the available importers (`semantic-scholar`).
