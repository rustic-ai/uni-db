// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `COPY ... TO` writes exactly the user-visible properties, and `COPY ...
//! FROM` reads them back.
//!
//! Before the fix, `COPY L TO 'f.csv'` dumped the label's Lance table: the
//! declared columns plus the raw `overflow_json` blob, never the undeclared
//! properties themselves, and nothing still in L0. A schemaless label was
//! "unknown", CSV edge export was unsupported, and Parquet export wrote the
//! storage table verbatim (`_uid`, `_version`, `overflow_json`, ...).
//!
//! The file layout is: identity columns (`_vid`; or `_eid`, `_src`, `_dst`)
//! then one column per property — the union of `properties()` keys over every
//! exported entity, sorted by name.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use uni_db::common::config::FileSandboxConfig;
use uni_db::{DataType, Uni, UniConfig, Value};

async fn exec(db: &Uni, cypher: &str) -> anyhow::Result<Vec<HashMap<String, Value>>> {
    let tx = db.session().tx().await?;
    let res = tx.query(cypher).await?;
    let cols: Vec<String> = res.columns().to_vec();
    let rows = res
        .rows()
        .iter()
        .map(|r| {
            cols.iter()
                .cloned()
                .zip(r.values().iter().cloned())
                .collect()
        })
        .collect();
    tx.commit().await?;
    Ok(rows)
}

async fn rows(db: &Uni, cypher: &str) -> anyhow::Result<Vec<HashMap<String, Value>>> {
    let res = db.session().query(cypher).await?;
    let cols: Vec<String> = res.columns().to_vec();
    Ok(res
        .rows()
        .iter()
        .map(|r| {
            cols.iter()
                .cloned()
                .zip(r.values().iter().cloned())
                .collect()
        })
        .collect())
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn map(v: &Value) -> HashMap<String, Value> {
    match v {
        Value::Map(m) => m.clone(),
        other => panic!("expected a map, got {other:?}"),
    }
}

/// `id -> properties()` for every entity a query yields as `(i, p)`.
async fn expected(db: &Uni, cypher: &str) -> anyhow::Result<BTreeMap<i64, HashMap<String, Value>>> {
    Ok(rows(db, cypher)
        .await?
        .iter()
        .map(|r| (int(&r["i"]), map(&r["p"])))
        .collect())
}

/// A property as CSV text, the way the exporter writes it.
fn csv_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        other => format!("{other}"),
    }
}

/// `(header, rows)` of a CSV file.
fn read_csv(path: &Path) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut rdr = csv::Reader::from_path(path)?;
    let header = rdr.headers()?.iter().map(str::to_string).collect();
    let rows = rdr
        .records()
        .map(|r| r.map(|r| r.iter().map(str::to_string).collect()))
        .collect::<Result<_, _>>()?;
    Ok((header, rows))
}

/// `(header, rows)` of a Parquet file, cells decoded to values.
fn read_parquet(path: &Path) -> anyhow::Result<(Vec<String>, Vec<HashMap<String, Value>>)> {
    let file = std::fs::File::open(path)?;
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut header = Vec::new();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let schema = batch.schema();
        header = schema.fields().iter().map(|f| f.name().clone()).collect();
        for row in 0..batch.num_rows() {
            let mut m = HashMap::new();
            for (field, col) in schema.fields().iter().zip(batch.columns()) {
                let hint = uni_store::storage::arrow_convert::type_hint_for_field(field);
                let v = uni_store::storage::arrow_convert::arrow_to_value(col.as_ref(), row, hint)?;
                m.insert(field.name().clone(), v);
            }
            out.push(m);
        }
    }
    Ok((header, out))
}

/// The header COPY TO must write for these entities.
fn expected_header(ids: &[&str], props: &BTreeMap<i64, HashMap<String, Value>>) -> Vec<String> {
    let keys: BTreeSet<String> = props.values().flat_map(|m| m.keys().cloned()).collect();
    ids.iter().map(|s| (*s).to_string()).chain(keys).collect()
}

/// Check one COPY TO export of `target` against `properties()`; one line per defect.
async fn check_export(
    db: &Uni,
    dir: &Path,
    target: &str,
    is_edge: bool,
    state: &str,
) -> anyhow::Result<Vec<String>> {
    let (q, ids): (String, &[&str]) = if is_edge {
        (
            format!("MATCH ()-[r:{target}]->() RETURN id(r) AS i, properties(r) AS p"),
            &["_eid", "_src", "_dst"],
        )
    } else {
        (
            format!("MATCH (n:{target}) RETURN id(n) AS i, properties(n) AS p"),
            &["_vid"],
        )
    };
    let want = expected(db, &q).await?;
    let header_want = expected_header(ids, &want);
    let mut defects = Vec::new();
    let what = format!("[{state}] {target}");

    let csv_path = dir.join(format!("{target}_{state}.csv"));
    exec(db, &format!("COPY {target} TO '{}'", csv_path.display())).await?;
    let (header, records) = read_csv(&csv_path)?;
    if header != header_want {
        defects.push(format!("{what} csv header: {header:?} != {header_want:?}"));
    } else {
        if records.len() != want.len() {
            defects.push(format!(
                "{what} csv rows: {} != {}",
                records.len(),
                want.len()
            ));
        }
        for rec in &records {
            let id: i64 = rec[0].parse()?;
            let Some(props) = want.get(&id) else {
                defects.push(format!("{what} csv: unknown id {id}"));
                continue;
            };
            for (col, cell) in header.iter().zip(rec).skip(ids.len()) {
                let expect = props.get(col).map(csv_text).unwrap_or_default();
                if *cell != expect {
                    defects.push(format!("{what} csv {id}.{col}: {cell:?} != {expect:?}"));
                }
            }
        }
    }

    let pq_path = dir.join(format!("{target}_{state}.parquet"));
    exec(db, &format!("COPY {target} TO '{}'", pq_path.display())).await?;
    let (header, records) = read_parquet(&pq_path)?;
    if header != header_want {
        defects.push(format!(
            "{what} parquet header: {header:?} != {header_want:?}"
        ));
    } else {
        if records.len() != want.len() {
            defects.push(format!(
                "{what} parquet rows: {} != {}",
                records.len(),
                want.len()
            ));
        }
        for rec in &records {
            let id = int(&rec[ids[0]]);
            let got: HashMap<String, Value> = rec
                .iter()
                .filter(|(k, v)| !ids.contains(&k.as_str()) && !v.is_null())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if want.get(&id) != Some(&got) {
                defects.push(format!(
                    "{what} parquet {id}: {got:?} != {:?}",
                    want.get(&id)
                ));
            }
        }
    }
    Ok(defects)
}

async fn seed(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("D")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .done()
        .edge_type("E", &["D"], &["D"])
        .property_nullable("w", DataType::Float64)
        .apply()
        .await?;
    exec(
        db,
        "CREATE (a:D {name: 'a', age: 1, nick: 'x', score: 2.5, tags: ['p', 'q'], _id: 'u1'}), \
                (b:D {name: 'b', flag: true, label: 'l', type: 't'}), \
                (c:D {name: 'c'}), \
                (a)-[:E {w: 1.5, note: 'n1'}]->(b), (b)-[:E {since: 2020}]->(c), \
                (s1:S {k: 1, label: 'x'}), (s2:S {other: 'y'}), \
                (s1)-[:R {via: 'z'}]->(s2), (s2)-[:R]->(s1)",
    )
    .await?;
    Ok(())
}

/// Every label and edge type, declared and schemaless, in L0 and flushed,
/// exports exactly `properties()` in both formats.
#[tokio::test]
async fn copy_to_exports_exactly_the_user_visible_properties() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::temporary().build().await?;
    seed(&db).await?;

    let mut defects = Vec::new();
    for state in ["l0", "flushed"] {
        if state == "flushed" {
            db.flush().await?;
        }
        for (target, is_edge) in [("D", false), ("S", false), ("E", true), ("R", true)] {
            defects.extend(check_export(&db, dir.path(), target, is_edge, state).await?);
        }
    }
    assert!(
        defects.is_empty(),
        "{} defect(s):\n{}",
        defects.len(),
        defects.join("\n")
    );
    Ok(())
}

/// A value rendered with map keys sorted, so equal values render equally.
fn canon(v: &Value) -> String {
    match v {
        Value::Map(m) => {
            let sorted: BTreeMap<&String, String> = m.iter().map(|(k, v)| (k, canon(v))).collect();
            format!("{sorted:?}")
        }
        Value::List(items) => format!("{:?}", items.iter().map(canon).collect::<Vec<_>>()),
        other => format!("{other:?}"),
    }
}

/// Multiset of the rows a query yields, each rendered canonically.
async fn property_bag(db: &Uni, cypher: &str) -> anyhow::Result<Vec<String>> {
    let mut bag: Vec<String> = rows(db, cypher)
        .await?
        .iter()
        .map(|r| {
            let sorted: BTreeMap<&String, String> = r.iter().map(|(k, v)| (k, canon(v))).collect();
            format!("{sorted:?}")
        })
        .collect();
    bag.sort();
    Ok(bag)
}

/// COPY TO then COPY FROM reproduces the same properties: vertices into a
/// fresh database, edges back into the same one after deleting them.
#[tokio::test]
async fn copy_to_then_copy_from_round_trips_properties() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    for fmt in ["csv", "parquet"] {
        let db = Uni::temporary().build().await?;
        db.schema()
            .label("D")
            .property("name", DataType::String)
            .property_nullable("age", DataType::Int64)
            .done()
            .edge_type("E", &["D"], &["D"])
            .property_nullable("w", DataType::Float64)
            .apply()
            .await?;
        // CSV is text: an undeclared property comes back as a string, so the
        // CSV round trip uses string-valued undeclared properties only.
        let extra = if fmt == "csv" {
            "nick: 'x', note: 'two words, comma'"
        } else {
            "nick: 'x', score: 2.5, tags: ['p', 'q'], flag: true, n: 7"
        };
        exec(
            &db,
            &format!(
                "CREATE (a:D {{name: 'a', age: 1, {extra}}}), (b:D {{name: 'b', other: 'o'}}), \
                 (a)-[:E {{w: 1.5, why: 'w'}}]->(b), (b)-[:E {{w: 2.0}}]->(a), \
                 (:S {{k: 'v'}}), (:S {{j: 'u'}})"
            ),
        )
        .await?;
        db.flush().await?;

        let fresh = Uni::temporary().build().await?;
        fresh
            .schema()
            .label("D")
            .property("name", DataType::String)
            .property_nullable("age", DataType::Int64)
            .apply()
            .await?;
        for label in ["D", "S"] {
            let file = dir.path().join(format!("{label}.{fmt}"));
            exec(&db, &format!("COPY {label} TO '{}'", file.display())).await?;
            exec(&fresh, &format!("COPY {label} FROM '{}'", file.display())).await?;
            let q = format!("MATCH (n:{label}) RETURN properties(n) AS p");
            assert_eq!(
                property_bag(&db, &q).await?,
                property_bag(&fresh, &q).await?,
                "{fmt} {label} vertex round trip"
            );
        }

        let q = "MATCH (a)-[r:E]->(b) RETURN properties(r) AS p, id(a) AS s, id(b) AS d";
        let before = property_bag(&db, q).await?;
        let file = dir.path().join(format!("E.{fmt}"));
        exec(&db, &format!("COPY E TO '{}'", file.display())).await?;
        exec(&db, "MATCH ()-[r:E]->() DELETE r").await?;
        assert!(property_bag(&db, q).await?.is_empty());
        exec(&db, &format!("COPY E FROM '{}'", file.display())).await?;
        assert_eq!(before, property_bag(&db, q).await?, "{fmt} edge round trip");
    }
    Ok(())
}

/// COPY and BACKUP touch files only where `UniConfig::file_sandbox` allows.
#[tokio::test]
async fn copy_honors_the_configured_file_sandbox() -> anyhow::Result<()> {
    let allowed = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    let db = Uni::in_memory()
        .config(UniConfig {
            file_sandbox: FileSandboxConfig {
                enabled: true,
                allowed_paths: vec![allowed.path().canonicalize()?],
            },
            ..Default::default()
        })
        .build()
        .await?;
    exec(&db, "CREATE (:P {v: 1})").await?;

    let inside = allowed.path().join("p.csv");
    exec(&db, &format!("COPY P TO '{}'", inside.display())).await?;
    assert!(inside.exists());

    let refused = outside.path().join("p.csv");
    let err = exec(&db, &format!("COPY P TO '{}'", refused.display()))
        .await
        .expect_err("COPY TO outside the sandbox must fail");
    assert!(
        format!("{err:#}").contains("Path validation failed"),
        "{err:#}"
    );
    assert!(!refused.exists());

    std::fs::copy(&inside, &refused)?;
    let err = exec(&db, &format!("COPY P FROM '{}'", refused.display()))
        .await
        .expect_err("COPY FROM outside the sandbox must fail");
    assert!(
        format!("{err:#}").contains("Path validation failed"),
        "{err:#}"
    );

    // BACKUP shares the sandbox check.
    let backup = outside.path().join("b");
    let err = exec(&db, &format!("BACKUP TO '{}'", backup.display()))
        .await
        .expect_err("BACKUP outside the sandbox must fail");
    assert!(
        format!("{err:#}").contains("Path validation failed"),
        "{err:#}"
    );
    Ok(())
}
