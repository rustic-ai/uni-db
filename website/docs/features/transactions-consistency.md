# Transactions & Consistency

Uni provides ACID transactions with snapshot isolation. Reads see a consistent snapshot, and writes are applied atomically on commit. Uni uses a single-writer model for simplicity and predictable performance.

## What It Provides

- Snapshot isolation for consistent reads.
- Single writer with concurrent readers.
- WAL-backed durability for committed changes.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    let mut tx = db.begin().await?;
    tx.execute("CREATE (:User {email: 'a@b.com'})").await?;
    tx.commit().await?;
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    tx = db.begin()
    tx.query("CREATE (:User {email: 'a@b.com'})")
    tx.commit()
    ```

## Use Cases

- Multi-step writes that must commit atomically.
- Consistent reads during complex queries.
- Predictable concurrency without distributed locking.

## When To Use

Use transactions for any workflow where partial writes are unacceptable or where multiple updates must be consistent.
