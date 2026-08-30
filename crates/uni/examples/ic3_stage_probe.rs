//! Bisects LDBC IC3 to find which clause owns its allocation.
//!
//! IC3 is OOM-killed at SF1 under a 16 GiB cap and has now been attributed
//! twice to a mechanism it does not use: first to `UNWIND` (#198 — IC3 contains
//! none), then to the collected-list carry (its `cities` list is 20 elements,
//! and interning it changed nothing). Both attributions came from reasoning
//! about the query rather than measuring it, so this probe measures it.
//!
//! Each stage is a prefix of IC3 ending in `count(*)`, so the stages grow one
//! clause at a time and the first one that spikes names the clause that owns
//! the memory. Peak is counted at the allocator rather than read from RSS,
//! because RSS is a process high-water mark that never falls and so cannot
//! report one stage after another.
//!
//! Every line is flushed as it is produced: the expected outcome is that some
//! stage kills the process, and the last line printed is then the answer.
//!
//!     LDBC_DB=$HOME/uni-bench-tmp/sf1 cargo run --release -p uni-db \
//!         --example ic3_stage_probe

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use uni_db::{Uni, UniConfig};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

// SAFETY: every method forwards to `System` with the layout it was given and
// returns its pointer unchanged; the counters are atomics and do not allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            let live = LIVE.fetch_add(new_size, Ordering::Relaxed) + new_size;
            PEAK.fetch_max(live, Ordering::Relaxed);
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// The post-#208 derived parameters for SF1, inlined so the stages are literal
/// Cypher and nothing depends on the bench's derivation.
const PERSON_ID: i64 = 2199023262543;
const COUNTRY_X: &str = "Belarus";
const COUNTRY_Y: &str = "Belgium";
const START_DATE: i64 = 1262566914233;
const END_DATE: i64 = 1347528753054;

/// The head every stage shares: the anchors, and the collected city list.
fn head() -> String {
    format!(
        "MATCH (countryX:Country {{name: '{COUNTRY_X}'}}), \
               (countryY:Country {{name: '{COUNTRY_Y}'}}), \
               (person:Person {{id: {PERSON_ID}}}) \
         WITH person, countryX, countryY LIMIT 1 "
    )
}

fn stages() -> Vec<(&'static str, String)> {
    let h = head();
    let with_cities = format!(
        "{h} MATCH (city:City)-[:IS_PART_OF]->(country:Country) \
         WHERE country IN [countryX, countryY] \
         WITH person, countryX, countryY, collect(city) AS cities "
    );
    // Everything through DISTINCT friend, shared by the last two stages.
    let friends = format!(
        "{with_cities} \
         MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city) \
         WHERE NOT person=friend AND NOT city IN cities \
         WITH DISTINCT friend, countryX, countryY "
    );
    vec![
        ("1 anchors", format!("{h} RETURN count(*) AS n")),
        (
            "2 cities collected",
            format!("{with_cities} RETURN size(cities) AS n"),
        ),
        (
            // The var-length expansion alone, with no location hop and no
            // predicate — isolates the KNOWS*1..2 fan-out itself.
            "3 KNOWS*1..2 only",
            format!("{h} MATCH (person)-[:KNOWS*1..2]-(friend) RETURN count(*) AS n"),
        ),
        (
            // Adds the location hop and the list predicate on top of it.
            "4 + located_in + NOT IN cities",
            format!(
                "{with_cities} \
                 MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city) \
                 WHERE NOT person=friend AND NOT city IN cities \
                 RETURN count(*) AS n"
            ),
        ),
        (
            "5 + DISTINCT friend",
            format!("{friends} RETURN count(*) AS n"),
        ),
        (
            // The message join: every message each surviving friend created.
            // This is the stage that multiplies friends by their messages, and
            // `message` is bound as a whole entity.
            "6 + HAS_CREATOR message",
            format!("{friends} MATCH (friend)<-[:HAS_CREATOR]-(message) RETURN count(*) AS n"),
        ),
        (
            // Reads one small scalar property of `message`. Compare against
            // stage 6, which binds `message` but reads nothing off it.
            "7 read creationDate (small)",
            format!(
                "{friends} MATCH (friend)<-[:HAS_CREATOR]-(message) \
                 RETURN count(message.creationDate) AS n"
            ),
        ),
        (
            // Reads one *large* property instead. If 7 and 7b cost the same,
            // the engine is materialising the whole entity either way and the
            // property named is irrelevant — which is the hypothesis. If 7b is
            // far worse, per-property narrowing works and the cost is content.
            "7b read content (large)",
            format!(
                "{friends} MATCH (friend)<-[:HAS_CREATOR]-(message) \
                 RETURN count(message.content) AS n"
            ),
        ),
        (
            // The date filter IC3 actually applies, which cuts the 2.84M rows
            // down before anything else runs.
            "7c filter on creationDate",
            format!(
                "{friends} MATCH (friend)<-[:HAS_CREATOR]-(message) \
                 WHERE {END_DATE} > message.creationDate AND message.creationDate >= {START_DATE} \
                 RETURN count(*) AS n"
            ),
        ),
        (
            "8 + message country filter",
            format!(
                "{friends} \
                 MATCH (friend)<-[:HAS_CREATOR]-(message), \
                       (message)-[:IS_LOCATED_IN]->(country) \
                 WHERE {END_DATE} > message.creationDate AND message.creationDate >= {START_DATE} \
                   AND country IN [countryX, countryY] \
                 RETURN count(*) AS n"
            ),
        ),
    ]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The 30 s default is an interactive-latency guard; a stage that takes
    // minutes must still report its peak rather than be cut off mid-measure.
    let config = UniConfig {
        query_timeout: std::time::Duration::from_secs(1800),
        ..Default::default()
    };
    let db = Uni::open(std::env::var("LDBC_DB")?)
        .config(config)
        .build()
        .await?;

    // The unbounded stages end by killing the process (that is the finding),
    // so they are opt-in — otherwise they would prevent the bounded arms below
    // from ever running.
    let run_stages = std::env::var("IC3_STAGES").is_ok();
    if run_stages {
        println!(
            "\n{:<32} {:>12} {:>12} {:>10}",
            "stage", "rows", "peak MiB", "ms"
        );
        std::io::stdout().flush().ok();
    }

    for (name, q) in stages().into_iter().filter(|_| run_stages) {
        let before = LIVE.load(Ordering::Relaxed);
        PEAK.store(before, Ordering::Relaxed);
        let t = Instant::now();
        let out = db.session().query(&q).await;
        let ms = t.elapsed().as_secs_f64() * 1e3;
        let peak = (PEAK.load(Ordering::Relaxed).saturating_sub(before)) as f64 / (1024.0 * 1024.0);
        let rows = match &out {
            Ok(r) => r
                .rows()
                .first()
                .map(|row| format!("{:?}", row.values()[0]))
                .unwrap_or_else(|| "-".to_string()),
            Err(e) => format!("ERROR {e}"),
        };
        println!("{name:<32} {rows:>12} {peak:>12.1} {ms:>10.0}");
        std::io::stdout().flush().ok();
    }

    // Which property is read, at a bounded row count so every arm survives.
    //
    // `count(*)` binds `message` and reads nothing; `creationDate` is an
    // 8-byte scalar; `content` is up to 2000 chars. If the two property arms
    // cost the same, the engine materialises the whole entity regardless of
    // what was asked for. If `content` is far worse, per-property narrowing
    // works and the cost is simply the size of that column.
    println!(
        "\n{:<32} {:>10} {:>12} {:>12} {:>10}",
        "bounded arm", "friends", "rows", "peak MiB", "ms"
    );
    std::io::stdout().flush().ok();

    let friends = stages_friends_prefix();
    // Cap the *input* to the join, not its output: a `LIMIT` after the join
    // bounds nothing, because the join materialises fully before it applies
    // (`count(*)` at LIMIT 50000 costs the same 4.2 GB as the full 2.84M-row
    // join). Limiting friends shrinks the join itself.
    for limit in [50usize, 100, 200] {
        // `HAS_CREATOR` declares two source labels, so `(message)` is
        // unlabelled and takes the multi-label property path; naming a label
        // takes the per-label one. Same rows, same column read -- the only
        // difference is which hydration path runs.
        for (arm, pattern, projection) in [
            ("a count(*) unlabelled", "(message)", "count(*)"),
            (
                "b creationDate unlabelled",
                "(message)",
                "count(message.creationDate)",
            ),
            (
                "c creationDate :Comment",
                "(message:Comment)",
                "count(message.creationDate)",
            ),
            (
                "d creationDate :Post",
                "(message:Post)",
                "count(message.creationDate)",
            ),
        ] {
            let q = format!(
                "{friends} WITH friend LIMIT {limit} \
                 MATCH (friend)<-[:HAS_CREATOR]-{pattern} RETURN {projection} AS n"
            );
            run(&db, arm, limit, &q).await;
        }

        // How many properties are read, holding rows fixed. If the per-row
        // cost is a heap-allocated map per vid, one property and three cost
        // nearly the same; if it is per value, three costs about triple.
        for (arm, projection) in [
            ("g 1 property", "count(message.creationDate)"),
            (
                "h 2 properties",
                "count(message.creationDate) + count(message.length)",
            ),
            (
                "i 3 properties",
                "count(message.creationDate) + count(message.length) \
                 + count(message.browserUsed)",
            ),
        ] {
            let q = format!(
                "{friends} WITH friend LIMIT {limit} \
                 MATCH (friend)<-[:HAS_CREATOR]-(message) \
                 WITH message RETURN {projection} AS n"
            );
            run(&db, arm, limit, &q).await;
        }

        // Same read with the carried entity columns projected away first. If
        // this is much cheaper, the cost is not the property read at all but
        // `friend`/`countryX`/`countryY` being copied onto every fan-out row --
        // the entity-struct analogue of #184's collected-list carry.
        for (arm, projection) in [
            (
                "e WITH message, then creationDate",
                "count(message.creationDate)",
            ),
            ("f WITH message, then count(*)", "count(*)"),
        ] {
            let q = format!(
                "{friends} WITH friend LIMIT {limit} \
                 MATCH (friend)<-[:HAS_CREATOR]-(message) \
                 WITH message RETURN {projection} AS n"
            );
            run(&db, arm, limit, &q).await;
        }
        #[allow(clippy::never_loop)]
        for _ in 0..0 {}
    }
    // Scan versus traversal, reading the same column off the same label.
    // A scan reads columns from storage columnarly; a traversal hydrates each
    // target vid through `PropertyManager`. If the scan pays no per-row
    // overhead for the read and the traversal does, the fix is to make
    // traversal hydration columnar rather than per-vid.
    println!(
        "\n{:<34} {:>8} {:>12} {:>12} {:>10}",
        "scan arm", "-", "rows", "peak MiB", "ms"
    );
    std::io::stdout().flush().ok();
    for (arm, projection) in [
        ("j scan Comment count(*)", "count(*)"),
        ("k scan Comment creationDate", "count(m.creationDate)"),
        (
            "l scan Comment 3 properties",
            "count(m.creationDate) + count(m.length) + count(m.browserUsed)",
        ),
    ] {
        let q = format!("MATCH (m:Comment) RETURN {projection} AS n");
        run(&db, arm, 0, &q).await;
    }

    Ok(())
}

/// Run one arm and print its row count, peak and wall time.
async fn run(db: &Uni, arm: &str, limit: usize, q: &str) {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let t = Instant::now();
    let out = db.session().query(q).await;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let peak = (PEAK.load(Ordering::Relaxed).saturating_sub(before)) as f64 / (1024.0 * 1024.0);
    let rows = match &out {
        Ok(r) => r
            .rows()
            .first()
            .map(|row| format!("{:?}", row.values()[0]))
            .unwrap_or_else(|| "-".to_string()),
        Err(e) => format!("ERROR {e}"),
    };
    println!("{arm:<34} {limit:>8} {rows:>12} {peak:>12.1} {ms:>10.0}");
    std::io::stdout().flush().ok();
}

/// IC3 through `WITH DISTINCT friend`, shared by the bounded arms.
fn stages_friends_prefix() -> String {
    let h = head();
    let with_cities = format!(
        "{h} MATCH (city:City)-[:IS_PART_OF]->(country:Country) \
         WHERE country IN [countryX, countryY] \
         WITH person, countryX, countryY, collect(city) AS cities "
    );
    format!(
        "{with_cities} \
         MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city) \
         WHERE NOT person=friend AND NOT city IN cities \
         WITH DISTINCT friend, countryX, countryY "
    )
}
