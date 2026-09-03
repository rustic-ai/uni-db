//! Separates the three mechanisms #229 proposes for the cost of
//! `x IN <collected list>`.
//!
//! The issue attributes 245 us per row to three compounding causes: the list
//! argument is decoded once per row, an interned list is deep-cloned on every
//! decode, and the membership test is a linear scan with no hash set. All three
//! predict a cost proportional to the number of rows and to the length of the
//! list, so neither of those axes can tell them apart.
//!
//! **Element width can.** A deep clone and a msgpack decode both cost bytes; the
//! membership test reads only `_vid` and never looks at a property. So holding
//! the row count and the list length fixed while growing a payload string on
//! each collected element separates them:
//!
//! * cost grows with payload width -> the decode/clone dominates
//! * cost flat in payload width    -> the linear scan dominates
//!
//! A second discriminator is the interning threshold. `COLLECT_INTERN_MIN_BYTES`
//! is 1024, so a list below it is re-decoded from msgpack per row and one above
//! it is resolved-and-cloned per row. If the deep clone is the dominant term,
//! crossing that threshold should show a discontinuity; if it is not, the curve
//! should be smooth across it.
//!
//! Three arms per point, all answering the same count so the numbers compare:
//!
//! * `control` - the same scan with no list in scope
//! * `unread`  - the list in scope but never read, which prices carrying it
//! * `in`      - the list read by `x IN xs`, which prices the predicate
//!
//!     cargo run --release -p uni-db --example in_list_probe

use std::time::Instant;

use uni_db::Uni;

/// `list_len` P nodes to be collected, each padded to `pad` bytes, and
/// `rows` Q nodes each pointing at one P.
///
/// The predicate matches exactly half the rows: every Q points at a P, and only
/// the first `list_len` P nodes are collected, so a probe that accidentally
/// admits everything or nothing is visible in the row count rather than silent.
async fn fixture(list_len: usize, rows: usize, pad: usize) -> anyhow::Result<Uni> {
    fixture_at(list_len, rows, pad, None).await
}

/// As `fixture`, but when `target` is set every Q points at that one P index
/// instead of being spread over all of them. That fixes where in the collected
/// list the match is found, which is the only way to separate the scan from the
/// decode: an early return is position-sensitive and a decode is not.
async fn fixture_at(
    list_len: usize,
    rows: usize,
    pad: usize,
    target: Option<usize>,
) -> anyhow::Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (idx INT, payload STRING)")
        .await?;
    tx.execute("CREATE LABEL Q (idx INT)").await?;
    tx.execute("CREATE EDGE TYPE PTR FROM Q TO P").await?;

    // 2 * list_len P nodes: the lower half is collected, the upper half is not.
    let filler = "x".repeat(pad);
    for chunk in (0..2 * list_len).collect::<Vec<_>>().chunks(200) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:P {{idx:{i}, payload:'{filler}'}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await?;
    }
    for chunk in (0..rows).collect::<Vec<_>>().chunks(500) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:Q {{idx:{i}}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await?;
    }
    // Each Q points at one P, spread evenly over all 2*list_len of them, so
    // exactly half the rows survive the predicate.
    match target {
        None => {
            tx.execute(&format!(
                "MATCH (q:Q), (p:P) WHERE p.idx = q.idx % {} CREATE (q)-[:PTR]->(p)",
                2 * list_len
            ))
            .await?;
        }
        Some(idx) => {
            tx.execute(&format!(
                "MATCH (q:Q), (p:P) WHERE p.idx = {idx} CREATE (q)-[:PTR]->(p)"
            ))
            .await?;
        }
    }
    tx.commit().await?;
    Ok(db)
}

fn q_control() -> String {
    "MATCH (q:Q)-[:PTR]->(p:P) RETURN count(*) AS n".into()
}

fn q_unread(list_len: usize) -> String {
    format!(
        "MATCH (c:P) WHERE c.idx < {list_len} WITH collect(c) AS xs \
         MATCH (q:Q)-[:PTR]->(p:P) RETURN count(*) AS n"
    )
}

fn q_in(list_len: usize) -> String {
    format!(
        "MATCH (c:P) WHERE c.idx < {list_len} WITH collect(c) AS xs \
         MATCH (q:Q)-[:PTR]->(p:P) WHERE p IN xs RETURN count(*) AS n"
    )
}

async fn timed(db: &Uni, q: &str) -> anyhow::Result<(i64, f64)> {
    let t = Instant::now();
    let r = db.session().query(q).await?;
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    match r.rows().first().map(|row| row.values()[0].clone()) {
        Some(uni_db::Value::Int(n)) => Ok((n, ms)),
        other => anyhow::bail!("expected a count, got {other:?}"),
    }
}

struct Point {
    rows: i64,
    matched: i64,
    control_ms: f64,
    unread_ms: f64,
    in_ms: f64,
}

async fn measure(list_len: usize, rows: usize, pad: usize) -> anyhow::Result<Point> {
    let db = fixture(list_len, rows, pad).await?;
    // One untimed pass so plan-cache and warm-up costs do not land on the first
    // arm measured.
    let _ = timed(&db, &q_in(list_len)).await?;

    let (n_c, control_ms) = timed(&db, &q_control()).await?;
    let (n_u, unread_ms) = timed(&db, &q_unread(list_len)).await?;
    let (n_i, in_ms) = timed(&db, &q_in(list_len)).await?;

    // Discrimination checks. A probe whose predicate admits everything or
    // nothing measures a scan, not a membership test, and would report a
    // plausible number either way.
    anyhow::ensure!(
        n_c == rows as i64 && n_u == n_c,
        "control/unread must see every row: control={n_c} unread={n_u} expected={rows}"
    );
    anyhow::ensure!(
        n_i > 0 && n_i < n_c,
        "the IN predicate must filter: matched={n_i} of {n_c}"
    );
    Ok(Point {
        rows: n_c,
        matched: n_i,
        control_ms,
        unread_ms,
        in_ms,
    })
}

/// One point with every Q pointing at `target`, so the match is at a known
/// position in the collected list (or absent).
async fn measure_at(
    list_len: usize,
    rows: usize,
    target: usize,
) -> anyhow::Result<(i64, f64, f64)> {
    let db = fixture_at(list_len, rows, 0, Some(target)).await?;
    let _ = timed(&db, &q_in(list_len)).await?;
    let (n_c, control_ms) = timed(&db, &q_control()).await?;
    let (n_i, in_ms) = timed(&db, &q_in(list_len)).await?;
    anyhow::ensure!(
        n_c == rows as i64,
        "control must see every row: {n_c} != {rows}"
    );
    // A hit arm must match every row and the miss arm none; anything between
    // means the fixture did not put the match where this arm intends.
    anyhow::ensure!(
        n_i == n_c || n_i == 0,
        "expected all or nothing, got {n_i} of {n_c}"
    );
    Ok((n_i, control_ms, in_ms))
}

fn header(varying: &str) {
    println!(
        "\n{:>10} {:>8} {:>8} {:>8} {:>11} {:>10} {:>10} {:>12} {:>12}",
        varying,
        "list",
        "rows",
        "matched",
        "control ms",
        "unread ms",
        "in ms",
        "in-control",
        "us/row"
    );
}

fn row(varying: usize, list_len: usize, p: &Point) {
    let delta = p.in_ms - p.control_ms;
    println!(
        "{varying:>10} {list_len:>8} {:>8} {:>8} {:>11.1} {:>10.1} {:>10.1} {:>12.1} {:>12.1}",
        p.rows,
        p.matched,
        p.control_ms,
        p.unread_ms,
        p.in_ms,
        delta,
        delta * 1000.0 / p.rows as f64,
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    const ROWS: usize = 20_000;
    const LIST: usize = 200;

    println!(
        "\n=== A. payload width, at list={LIST} rows={ROWS} ===\n\
         Growing only the bytes per collected element. The membership test reads\n\
         `_vid` and never a property, so a cost that grows here is the decode or\n\
         the clone, not the scan."
    );
    header("pad B");
    for &pad in &[0usize, 64, 256, 1024] {
        let p = measure(LIST, ROWS, pad).await?;
        row(pad, LIST, &p);
    }

    println!(
        "\n=== B. list length across the 1024-byte intern threshold, rows={ROWS}, pad=0 ===\n\
         A jump at the crossing is the deep clone on handle resolve; a smooth\n\
         curve says interning is not where the time goes."
    );
    header("list");
    for &list_len in &[8usize, 32, 64, 128, 256, 512, 1024] {
        let p = measure(list_len, ROWS, 0).await?;
        row(list_len, list_len, &p);
    }

    println!(
        "\n=== D. match position, at list={LIST} rows={ROWS} pad=0 ===\n\
         Every Q points at one P, so the match sits at a known position. The scan\n\
         returns early on a hit; the decode runs in full either way. So if `first`\n\
         is far cheaper than `last`, the scan dominates. If all three are equal,\n\
         the decode does and the scan is noise."
    );
    println!(
        "\n{:>10} {:>8} {:>8} {:>11} {:>10} {:>12} {:>12}",
        "position", "list", "rows", "control ms", "in ms", "in-control", "us/row"
    );
    for (label, target) in [
        ("first", 0usize),
        ("middle", LIST / 2),
        ("last", LIST - 1),
        ("miss", 2 * LIST - 1),
    ] {
        let (matched, control_ms, in_ms) = measure_at(LIST, ROWS, target).await?;
        let delta = in_ms - control_ms;
        println!(
            "{label:>10} {LIST:>8} {ROWS:>8} {control_ms:>11.1} {in_ms:>10.1} {delta:>12.1} {:>12.1}   (matched {matched})",
            delta * 1000.0 / ROWS as f64
        );
    }

    println!("\n=== C. rows, at list={LIST} pad=0 ===\nus/row flat confirms the cost is per row.");
    header("rows");
    for &rows in &[5_000usize, 10_000, 20_000, 40_000] {
        let p = measure(LIST, rows, 0).await?;
        row(rows, LIST, &p);
    }
    Ok(())
}
