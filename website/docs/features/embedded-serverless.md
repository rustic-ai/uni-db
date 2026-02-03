# Embedded & Serverless

Uni runs as a library inside your application process. There is no separate server to deploy, monitor, or scale. You open a database path and start querying immediately.

## What It Provides

- In-process execution with no network hops.
- Simple local deployment for desktop, backend services, or edge devices.
- Optional durability to local disk or object storage.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    #[tokio::main]
    async fn main() -> Result<(), uni_db::UniError> {
        let db = Uni::open("./my_db").build().await?;
        let rows = db.query("MATCH (n) RETURN count(n) as c").await?;
        println!("{:?}", rows);
        Ok(())
    }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")
    rows = db.query("MATCH (n) RETURN count(n) AS c")
    print(rows)
    ```

## Use Cases

- Local search and recommendation inside a service.
- Edge or on-device knowledge graphs.
- Testing and CI environments without external dependencies.

## When To Use

Choose embedded mode when you want predictable latency and minimal ops. If you need multi-tenant hosted access or cross-region write scaling, use a separate managed system.
