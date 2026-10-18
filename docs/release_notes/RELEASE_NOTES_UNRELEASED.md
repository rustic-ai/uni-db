# uni-db — unreleased

Changes on `main` since 4.2.0 that are not yet in a release. Rename this file to
the version it ships in.

---

## ⚠️ Behaviour and API changes

### Results that change

**A `shortestPath` pair with no path is dropped by a mandatory `MATCH`, as in
openCypher.** `MATCH p = shortestPath((a)-[:R*]->(b))` (and `allShortestPaths`)
used to return a pair whose endpoints are not connected as a row with `p` NULL.
It now returns no row for that pair, as openCypher and Neo4j do; `OPTIONAL MATCH`
keeps the row with `p` NULL. The same applies to a NULL endpoint and to a source
equal to its target under `*1..` (a node reaches itself only through the
zero-length path of `*0..`). A query that counted rows to count candidate pairs,
or tested `p IS NULL` after a mandatory `MATCH`, should use `OPTIONAL MATCH`.

**`OPTIONAL MATCH` with an unbound `shortestPath` target is null-extended per
entering row.** `MATCH (a) OPTIONAL MATCH p = shortestPath((a)-[:R*]->(b:N))`
returned one NULL-path row for every `N` node `a` does not reach, beside the ones
it does. It now returns only the reachable targets, or a single row with `b` and
`p` NULL when `a` reaches none of them. An entering row with no candidate target
at all (an empty label, or a property map nothing satisfies) used to be dropped;
it now gets its NULL row.

### New

**The unnamed form `MATCH shortestPath(...)` plans.** It failed with
`shortestPath must be assigned to a variable`; it now matches like the named form.
