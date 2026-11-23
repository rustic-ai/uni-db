# Locy Quickstart

This walkthrough shows non-recursive and recursive reasoning, then explains one inferred result.

## Rust Example

```rust
use uni_db::Uni;

let db = Uni::in_memory().build().await?;
let session = db.session();

let tx = session.tx().await?;
tx.execute("CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})-[:EDGE]->(:Node {name: 'C'})").await?;
tx.commit().await?;

let program = r#"
CREATE RULE reachable AS
MATCH (a:Node)-[:EDGE]->(b:Node)
YIELD KEY a, KEY b

CREATE RULE reachable AS
MATCH (a:Node)-[:EDGE]->(mid:Node)
WHERE mid IS reachable TO b
YIELD KEY a, KEY b

QUERY reachable WHERE a.name = 'A' RETURN b.name AS target
"#;

let result = session.locy(program).await?;
println!("rows = {:?}", result.rows());
```

## Python Example

```python
import uni_db

db = uni_db.Uni.temporary()
session = db.session()

program = """
CREATE RULE adults AS
MATCH (p:Person)
WHERE p.age >= 18
YIELD KEY p, p.name AS name

QUERY adults RETURN name
"""

out = session.locy(program)
print(out.derived.keys())
print(out.stats)
```

## Explain a Derivation

```locy
EXPLAIN RULE reachable WHERE a.name = 'A' AND b.name = 'C'
```

Use this when you need proof-style traceability for compliance or debugging.

## Next

- [Language Guide](language-guide.md)
- [Rule Semantics](rule-semantics.md)
