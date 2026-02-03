# Graph Algorithms

Uni includes a built-in graph algorithm library exposed through Cypher procedures. Use it to compute centrality, clustering, paths, and connectivity without exporting data.

## What It Provides

- Centrality: PageRank, betweenness, closeness, eigenvector, Katz.
- Connectivity: WCC, SCC, bridges, articulation points.
- Paths: shortest paths, k-shortest, A*, BFS/DFS.
- Community detection: Louvain, label propagation.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let rows = db.query(
        "CALL uni.algo.pageRank() YIELD nodeId, score RETURN nodeId, score ORDER BY score DESC LIMIT 10"
    ).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    rows = db.query(
        "CALL uni.algo.pageRank() YIELD nodeId, score RETURN nodeId, score ORDER BY score DESC LIMIT 10"
    )
    print(rows)
    ```

## Use Cases

- Identify important nodes in a graph.
- Detect communities and clusters.
- Compute routing or similarity metrics.

## When To Use

Use built-in algorithms when you need graph analytics without exporting data or building your own pipeline.
