# OpenCypher Querying

Uni uses OpenCypher as its primary query language, with extensions for vector search, time travel, and admin procedures. This keeps graph queries expressive and readable.

`id(node)` and `id(relationship)` return signed integer identities. Virtual
entities created by procedures use negative IDs, so `id(node) < 0` identifies
a virtual node. Comparisons, ordering and arithmetic use the same integer
returned to the caller; `elementId(entity)` is its string representation.

## What It Provides

- Pattern matching for nodes and relationships, including `OPTIONAL MATCH` for left-outer-join semantics.
- Aggregations, ordering, filtering, and window functions (`OVER` clause).
- `MERGE` for upsert operations with `ON CREATE SET` / `ON MATCH SET`.
- `SET` / `REMOVE` for updating properties and labels on existing nodes and relationships.
- Procedure calls for algorithms, snapshots, and indexes.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let results = session
        .query_with("MATCH (p:Person) WHERE p.age > $min RETURN p.name")
        .param("min", 30)
        .fetch_all()
        .await?;

    println!("{:?}", results);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()
    results = session.query_with(
        "MATCH (p:Person) WHERE p.age > $min RETURN p.name"
    ).param("min", 30).fetch_all()

    print(results)
    ```

## Semantics Worth Knowing

- **Parameters must be supplied.** A query that references `$x` without a
  value for `x` fails with `UniError::ParameterMissing { names }` before it
  reads or writes anything -- including `CREATE`, `SET`, `MERGE` and Locy
  rules. `names` lists every missing parameter of the statement, in order of
  first appearance (`RETURN $a, $b` with no parameters: `["a", "b"]`), even one
  in a branch no row would reach. `EXPLAIN` plans without parameters. A
  parameter supplied as `null` is a value, not a missing parameter.
- **Relationship type disjunctions** (`[r:R|Q]`) match any listed type, whether
  each type is declared in the schema or not.
- **Declared endpoints never drop a match.** An edge type's declared source and
  destination labels are enforced when edges are written, including by the
  bulk loader. A pattern that does not label an endpoint is narrowed to them
  only when every stored edge of the type is known to conform: types created
  by this release are, types in a store written by an earlier one are
  `unverified` until `CALL uni.admin.verifyEdgeEndpoints()` (or
  `uni.admin.verifyEdgeEndpoints('R')` for one type) finds none violating.
  Unverified, the pattern reads every vertex and matches violating edges --
  correct, but on a large graph `MATCH ()-[r:R]->()` is much slower
  (LDBC SF1 `WORK_AT`: about 6 s unverified, 0.3 s verified). The procedure yields
  `edge_type`, `edges_checked`, `source_violations`, `target_violations` and
  `conformance`; it runs on the primary database, not on a fork.

## Use Cases

- Relationship queries and graph traversals.
- Pattern matching with filters and aggregations.
- Admin and metadata procedures.

## When To Use

OpenCypher is the best choice when your data model is graph-shaped and you need expressive relationship queries, not just key-value lookups or SQL tables.

## Locy Integration

When you need recursive rule-based reasoning, hypothetical analysis, or abductive remediation workflows, use Locy on top of Cypher.

- Start at [Locy Overview](../locy/index.md)
- Learn syntax in [Locy Language Guide](../locy/language-guide.md)
- See advanced workflows in [DERIVE / ASSUME / ABDUCE](../locy/advanced/derive-assume-abduce.md)
