# APIs & Tooling

Uni provides first-class Rust and Python APIs, plus a CLI for exploration and scripting.

## What It Provides

- Rust async API (`uni_db` crate).
- Python sync and async APIs (`uni_db` module).
- CLI with REPL, query, and snapshot commands.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let rows = db.query("MATCH (n) RETURN count(n)").await?;
    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    rows = db.query("MATCH (n) RETURN count(n)")
    print(rows)
    ```

## Use Cases

- Rust services and embedded apps.
- Python data pipelines and notebooks.
- Interactive exploration via `uni repl`.

## When To Use

Use the Rust API for low-latency services and the Python API for analytics or ML workflows. The CLI is great for quick inspection and demos.
