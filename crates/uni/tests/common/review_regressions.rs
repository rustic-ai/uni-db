use anyhow::Result;
use uni_db::{
    DataType, IndexType, Uni, UniConfig, Value, VectorAlgo, VectorIndexCfg, VectorMetric,
};

async fn db() -> Result<Uni> {
    Ok(Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: usize::MAX,
            disable_fork_sweeper: true,
            disable_fork_index_builder: true,
            ..Default::default()
        })
        .build()
        .await?)
}

#[tokio::test]
async fn review_empty_fork_must_not_see_later_primary_rows() -> Result<()> {
    let db = db().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let fork = primary.fork("empty").await?;
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Item {name:'primary-later'})").await?;
    tx.commit().await?;
    db.flush().await?;
    let rows = fork.query("MATCH (n:Item) RETURN n.name AS name").await?;
    assert!(
        rows.is_empty(),
        "post-fork primary data leaked: {:?}",
        rows.rows()
    );
    Ok(())
}

#[tokio::test]
async fn review_empty_label_on_populated_fork_must_stay_empty() -> Result<()> {
    let db = db().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .done()
        .label("Seed")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Seed {name:'seed'})").await?;
    tx.commit().await?;
    let fork = primary.fork("empty-label").await?;
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Item {name:'primary-later'})").await?;
    tx.commit().await?;
    db.flush().await?;
    let rows = fork.query("MATCH (n:Item) RETURN n.name AS name").await?;
    assert!(
        rows.is_empty(),
        "post-fork primary label data leaked: {:?}",
        rows.rows()
    );
    Ok(())
}

#[tokio::test]
async fn review_fork_cosine_top_one_matches_primary() -> Result<()> {
    let db = db().await?;
    db.schema()
        .label("Doc")
        .property("name", DataType::String)
        .property("v", DataType::Vector { dimensions: 2 })
        .index(
            "v",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Doc {name:'cosine-best',v:[100.0,0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {name:'l2-best',v:[1.0,0.1]})")
        .await?;
    tx.commit().await?;
    let fork = primary.fork("vectors").await?;
    let query = "CALL uni.vector.query('Doc','v',[1.0,0.0],1) YIELD node, distance RETURN node.name AS name,distance";
    let base = primary.query(query).await?;
    let branch = fork.query(query).await?;
    assert_eq!(base.rows()[0].get::<String>("name")?, "cosine-best");
    assert_eq!(
        branch.rows()[0].get::<String>("name")?,
        "cosine-best",
        "fork chose wrong candidate: {:?}",
        branch.rows()
    );
    Ok(())
}

#[tokio::test]
async fn review_recursive_cte_must_not_succeed_with_truncated_closure() -> Result<()> {
    let db = db().await?;
    db.schema()
        .label("Item")
        .property("i", DataType::Int64)
        .done()
        .edge_type("NEXT", &["Item"], &["Item"])
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    let vids = tx
        .bulk_insert_vertices(
            "Item",
            (0..1002)
                .map(|i| [("i".to_owned(), Value::Int(i))].into_iter().collect())
                .collect(),
        )
        .await?;
    tx.bulk_insert_edges(
        "NEXT",
        vids.windows(2)
            .map(|p| (p[0], p[1], Default::default()))
            .collect(),
    )
    .await?;
    tx.commit().await?;
    let result = s
        .query(
            "WITH RECURSIVE reach AS (
        MATCH (root:Item {i:0}) RETURN root
        UNION
        MATCH (a:Item)-[:NEXT]->(b:Item) WHERE a IN reach RETURN b
    ) MATCH (n:Item) WHERE n IN reach RETURN count(n) AS c, max(n.i) AS last",
        )
        .await;
    match result {
        Ok(rows) => {
            eprintln!("recursive CTE returned {:?}", rows.rows());
            assert_eq!(
                rows.rows()[0].get::<i64>("c")?,
                1002,
                "successful closure must include all reachable nodes"
            );
        }
        Err(err) => {
            assert!(
                err.to_string().to_lowercase().contains("iteration"),
                "unexpected error: {err}"
            );
        }
    }
    Ok(())
}

async fn vector_mutation_case(mutation: &str, flush: bool) -> Result<String> {
    let db = db().await?;
    db.schema()
        .label("Doc")
        .property("name", DataType::String)
        .property("v", DataType::Vector { dimensions: 2 })
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:Doc {name:'a',v:[0.0,0.0]}), (:Doc {name:'b',v:[1.0,0.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let tx = s.tx().await?;
    tx.execute(&format!("MATCH (n:Doc {{name:'a'}}) {mutation}"))
        .await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    let rows = s
        .query("CALL uni.vector.query('Doc','v',[0.0,0.0],1) YIELD node RETURN node.name AS name")
        .await?;
    Ok(if rows.is_empty() {
        "<empty>".to_owned()
    } else {
        rows.rows()[0].get::<String>("name")?
    })
}

#[tokio::test]
async fn review_vector_update_must_refill_disk_candidates() -> Result<()> {
    assert_eq!(
        vector_mutation_case("SET n.v=[100.0,0.0]", true).await?,
        "b",
        "flushed control"
    );
    assert_eq!(
        vector_mutation_case("SET n.v=[100.0,0.0]", false).await?,
        "b",
        "L0 update must not hide disk runner-up"
    );
    Ok(())
}

#[tokio::test]
async fn review_vector_delete_must_refill_disk_candidates() -> Result<()> {
    assert_eq!(
        vector_mutation_case("DELETE n", true).await?,
        "b",
        "flushed control"
    );
    assert_eq!(
        vector_mutation_case("DELETE n", false).await?,
        "b",
        "L0 delete must not hide disk runner-up"
    );
    Ok(())
}

#[tokio::test]
async fn review_vector_label_removal_must_exclude_old_hit() -> Result<()> {
    assert_eq!(
        vector_mutation_case("REMOVE n:Doc", true).await?,
        "b",
        "flushed control"
    );
    assert_eq!(
        vector_mutation_case("REMOVE n:Doc", false).await?,
        "b",
        "search result must still have requested label"
    );
    Ok(())
}

async fn concurrent_key(flush_between: bool, mode: &str) -> Result<bool> {
    let db = db().await?;
    db.schema()
        .label("Account")
        .property("key", DataType::String)
        .apply()
        .await?;
    let s1 = db.session();
    let s2 = db.session();
    if mode == "unique" {
        let tx = s1.tx().await?;
        tx.execute("CREATE CONSTRAINT account_key ON (n:Account) ASSERT n.key IS UNIQUE")
            .await?;
        tx.commit().await?;
    }
    let t1 = s1.tx().await?;
    let t2 = s2.tx().await?;
    let q = match mode {
        "merge" => "MERGE (n:Account {key:'same'})",
        "ext_id" => "CREATE (:Account {key:'same', ext_id:'same'})",
        _ => "CREATE (:Account {key:'same'})",
    };
    t1.execute(q).await?;
    t2.execute(q).await?;
    t1.commit().await?;
    if flush_between {
        db.flush().await?;
    }
    let second = t2.commit().await;
    let rows = s1
        .query("MATCH (n:Account) RETURN n.key AS key, id(n) AS id")
        .await?;
    eprintln!(
        "mode={mode}, flush={flush_between}, second_commit={second:?}, rows={:?}",
        rows.rows()
    );
    Ok(second.is_err())
}

#[tokio::test]
async fn review_unique_constraint_must_survive_concurrent_flush() -> Result<()> {
    assert!(
        concurrent_key(false, "unique").await?,
        "no-flush control must reject"
    );
    assert!(
        concurrent_key(true, "unique").await?,
        "flush must not allow a duplicate unique key"
    );
    Ok(())
}

#[tokio::test]
async fn review_merge_guard_must_survive_concurrent_flush() -> Result<()> {
    assert!(
        concurrent_key(false, "merge").await?,
        "no-flush control must reject"
    );
    assert!(
        concurrent_key(true, "merge").await?,
        "flush must not allow duplicate MERGE creates"
    );
    Ok(())
}

#[tokio::test]
async fn review_ext_id_guard_must_survive_concurrent_flush() -> Result<()> {
    assert!(
        concurrent_key(false, "ext_id").await?,
        "no-flush control must reject"
    );
    assert!(
        concurrent_key(true, "ext_id").await?,
        "flush must not allow duplicate ext_id"
    );
    Ok(())
}

#[tokio::test]
async fn review_anonymous_merge_must_deduplicate_within_statement() -> Result<()> {
    for (label, binding) in [("Named", "n"), ("Anonymous", "")] {
        let db = db().await?;
        db.schema()
            .label(label)
            .property("key", DataType::String)
            .apply()
            .await?;
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute(&format!(
            "UNWIND range(1,3) AS i MERGE ({binding}:{label} {{key:'same'}})"
        ))
        .await?;
        tx.commit().await?;
        let rows = s
            .query(&format!("MATCH (n:{label}) RETURN count(n) AS c"))
            .await?;
        assert_eq!(
            rows.rows()[0].get::<i64>("c")?,
            1,
            "{label} MERGE must create only one node for a repeated key"
        );
    }
    Ok(())
}

#[tokio::test]
async fn review_fork_absence_survives_first_write_nesting_and_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    {
        let db = Uni::open(path)
            .config(UniConfig {
                auto_flush_interval: None,
                auto_flush_threshold: usize::MAX,
                disable_fork_sweeper: true,
                disable_fork_index_builder: true,
                ..Default::default()
            })
            .build()
            .await?;
        db.schema()
            .label("Item")
            .property("name", DataType::String)
            .done()
            .edge_type("R", &["Item"], &["Item"])
            .apply()
            .await?;
        let main = db.session();
        let fork = main.fork("absent").await?;
        let tx = main.tx().await?;
        tx.execute("CREATE (:Item {name:'main-a'})-[:R]->(:Item {name:'main-b'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        let empty_child = fork.fork("empty-child").await?;
        assert_eq!(
            empty_child
                .query("MATCH (n:Item) RETURN count(n) AS c")
                .await?
                .rows()[0]
                .get::<i64>("c")?,
            0
        );
        assert_eq!(
            fork.query("MATCH ()-[r:R]->() RETURN count(r) AS c")
                .await?
                .rows()[0]
                .get::<i64>("c")?,
            0
        );
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Item {name:'fork-a'})-[:R]->(:Item {name:'fork-b'})")
            .await?;
        tx.commit().await?;
        // Nested creation must flush the parent's newly created datasets and
        // capture those new branches, not primary's tip or an empty view.
        let child = fork.fork("with-parent-data").await?;
        assert_eq!(
            child
                .query("MATCH (n:Item) RETURN count(n) AS c")
                .await?
                .rows()[0]
                .get::<i64>("c")?,
            2
        );
        assert_eq!(
            child
                .query("MATCH ()-[r:R]->() RETURN count(r) AS c")
                .await?
                .rows()[0]
                .get::<i64>("c")?,
            1
        );
        assert_eq!(
            empty_child
                .query("MATCH (n:Item) RETURN count(n) AS c")
                .await?
                .rows()[0]
                .get::<i64>("c")?,
            0
        );
        let tx = empty_child.tx().await?;
        tx.execute("CREATE (:Item {name:'child-only'})").await?;
        tx.commit().await?;
        empty_child.flush().await?;
        for session in [&main, &fork] {
            assert_eq!(
                session
                    .query("MATCH (n:Item) RETURN count(n) AS c")
                    .await?
                    .rows()[0]
                    .get::<i64>("c")?,
                2
            );
        }
        drop((main, fork, child, empty_child));
        db.shutdown().await?;
    }
    let db = Uni::open(path).build().await?;
    for (name, expected) in [("absent", 2), ("with-parent-data", 2), ("empty-child", 1)] {
        let fork = db.session().fork(name).await?;
        assert_eq!(
            fork.query("MATCH (n:Item) RETURN count(n) AS c")
                .await?
                .rows()[0]
                .get::<i64>("c")?,
            expected
        );
        let names = fork.query("MATCH (n:Item) RETURN n.name AS name").await?;
        assert!(
            names
                .rows()
                .iter()
                .all(|r| !r.get::<String>("name").unwrap().starts_with("main"))
        );
    }
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn review_edge_and_node_key_conflicts_survive_flush() -> Result<()> {
    use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
    for (edge, node_key) in [(false, true), (true, false), (true, true)] {
        let db = db().await?;
        db.schema()
            .label("Item")
            .property("key", DataType::String)
            .done()
            .edge_type("R", &["Item"], &["Item"])
            .property("key", DataType::String)
            .apply()
            .await?;
        db.schema_manager().add_constraint(Constraint {
            name: "key_constraint".into(),
            constraint_type: if node_key {
                ConstraintType::NodeKey {
                    properties: vec!["key".into()],
                }
            } else {
                ConstraintType::Unique {
                    properties: vec!["key".into()],
                }
            },
            target: if edge {
                ConstraintTarget::EdgeType("R".into())
            } else {
                ConstraintTarget::Label("Item".into())
            },
            enabled: true,
        })?;
        let s = db.session();
        if edge {
            let tx = s.tx().await?;
            tx.execute("CREATE (:Item {key:'a'}), (:Item {key:'b'})")
                .await?;
            tx.commit().await?;
            db.flush().await?;
        }
        let a = s.tx().await?;
        let b = db.session().tx().await?;
        let query = if edge {
            "MATCH (a:Item {key:'a'}), (b:Item {key:'b'}) CREATE (a)-[:R {key:'same'}]->(b)"
        } else {
            "CREATE (:Item {key:'same'})"
        };
        a.execute(query).await?;
        b.execute(query).await?;
        a.commit().await?;
        db.flush().await?;
        assert!(b.commit().await.is_err(), "edge={edge} node_key={node_key}");
    }
    Ok(())
}

#[tokio::test]
async fn review_anonymous_merge_concurrency_with_and_without_flush() -> Result<()> {
    for flush in [false, true] {
        let db = db().await?;
        db.schema()
            .label("Account")
            .property("key", DataType::String)
            .apply()
            .await?;
        let s = db.session();
        let a = s.tx().await?;
        let b = db.session().tx().await?;
        a.execute("MERGE (:Account {key:'same'})").await?;
        b.execute("MERGE (:Account {key:'same'})").await?;
        a.commit().await?;
        if flush {
            db.flush().await?;
        }
        assert!(b.commit().await.is_err());
        assert_eq!(
            s.query("MATCH (n:Account) RETURN count(n) AS c")
                .await?
                .rows()[0]
                .get::<i64>("c")?,
            1
        );
    }
    Ok(())
}

#[tokio::test]
async fn review_vector_many_shadowed_candidates_in_private_and_committed_overlays() -> Result<()> {
    for mutation in ["DELETE n", "SET n.v=[100.0,0.0]", "REMOVE n:Doc"] {
        let db = db().await?;
        db.schema()
            .label("Doc")
            .property("i", DataType::Int64)
            .property("v", DataType::Vector { dimensions: 2 })
            .apply()
            .await?;
        let s = db.session();
        let tx = s.tx().await?;
        tx.bulk_insert_vertices(
            "Doc",
            (0..20)
                .map(|i| {
                    [
                        ("i".into(), Value::Int(i)),
                        ("v".into(), Value::Vector(vec![i as f32, 0.0])),
                    ]
                    .into_iter()
                    .collect()
                })
                .collect(),
        )
        .await?;
        tx.commit().await?;
        db.flush().await?;
        let tx = s.tx().await?;
        tx.execute(&format!("MATCH (n:Doc) WHERE n.i<12 {mutation}"))
            .await?;
        let query = "CALL uni.vector.query('Doc','v',[0.0,0.0],3) YIELD node,distance RETURN node.i AS i ORDER BY distance,i";
        let expected = vec![12, 13, 14];
        let private = tx.query(query).await?;
        assert_eq!(
            private
                .rows()
                .iter()
                .map(|r| r.get::<i64>("i").unwrap())
                .collect::<Vec<_>>(),
            expected,
            "private {mutation}"
        );
        tx.commit().await?;
        for flush in [false, true] {
            if flush {
                db.flush().await?;
            }
            let rows = s.query(query).await?;
            assert_eq!(
                rows.rows()
                    .iter()
                    .map(|r| r.get::<i64>("i").unwrap())
                    .collect::<Vec<_>>(),
                expected,
                "{mutation} flush={flush}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn review_recursive_frontier_avoids_unrelated_label_rows() -> Result<()> {
    let db = db().await?;
    db.schema()
        .label("Item")
        .property("i", DataType::Int64)
        .done()
        .edge_type("NEXT", &["Item"], &["Item"])
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    let vids = tx
        .bulk_insert_vertices(
            "Item",
            (0..220)
                .map(|i| [("i".into(), Value::Int(i))].into_iter().collect())
                .collect(),
        )
        .await?;
    tx.bulk_insert_edges(
        "NEXT",
        (0..20)
            .map(|i| (vids[i], vids[(i + 1) % 20], Default::default()))
            .collect(),
    )
    .await?;
    tx.commit().await?;
    let query = "WITH RECURSIVE reach AS (MATCH (root:Item {i:0}) RETURN root UNION MATCH (a:Item)-[:NEXT]->(b:Item) WHERE a IN reach RETURN b) MATCH (n:Item) WHERE n IN reach RETURN count(n) AS c";
    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        let rows = s.query(query).await?;
        assert_eq!(rows.rows()[0].get::<i64>("c")?, 20);
        assert!(
            rows.metrics().rows_scanned <= 3 * 220,
            "repeated full scans: {}",
            rows.metrics().rows_scanned
        );
    }
    Ok(())
}

#[tokio::test]
async fn review_recursive_budget_boundary_reports_exhaustion() -> Result<()> {
    for n in [999usize, 1000, 1001] {
        let db = db().await?;
        db.schema()
            .label("Item")
            .property("i", DataType::Int64)
            .done()
            .edge_type("NEXT", &["Item"], &["Item"])
            .apply()
            .await?;
        let s = db.session();
        let tx = s.tx().await?;
        let vids = tx
            .bulk_insert_vertices(
                "Item",
                (0..n)
                    .map(|i| [("i".into(), Value::Int(i as i64))].into_iter().collect())
                    .collect(),
            )
            .await?;
        tx.bulk_insert_edges(
            "NEXT",
            vids.windows(2)
                .map(|v| (v[0], v[1], Default::default()))
                .collect(),
        )
        .await?;
        tx.commit().await?;
        let result = s.query("WITH RECURSIVE reach AS (MATCH (root:Item {i:0}) RETURN root UNION MATCH (a:Item)-[:NEXT]->(b:Item) WHERE a IN reach RETURN b) MATCH (n:Item) WHERE n IN reach RETURN count(n) AS c").await;
        if n <= 1000 {
            assert_eq!(result?.rows()[0].get::<i64>("c")?, n as i64);
        } else {
            assert!(result.unwrap_err().to_string().contains("iteration limit"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn review_branch_metrics_for_inherited_and_fork_local_vectors() -> Result<()> {
    for metric in [VectorMetric::Cosine, VectorMetric::Dot, VectorMetric::L2] {
        let expected = if matches!(metric, VectorMetric::L2) {
            "near"
        } else {
            "aligned"
        };
        let db = db().await?;
        db.schema()
            .label("Doc")
            .property("name", DataType::String)
            .property("v", DataType::Vector { dimensions: 2 })
            .index(
                "v",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric,
                    embedding: None,
                }),
            )
            .apply()
            .await?;
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute(
            "CREATE (:Doc {name:'aligned',v:[100.0,0.0]}), (:Doc {name:'near',v:[1.0,0.1]})",
        )
        .await?;
        tx.commit().await?;
        db.flush().await?;
        let f = s.fork("metrics").await?;
        let query =
            "CALL uni.vector.query('Doc','v',[1.0,0.0],1) YIELD node RETURN node.name AS name";
        for session in [&s, &f] {
            assert_eq!(
                session.query(query).await?.rows()[0].get::<String>("name")?,
                expected
            );
        }
        let tx = f.tx().await?;
        tx.execute("CREATE (:Doc {name:'other',v:[0.0,100.0]})")
            .await?;
        tx.commit().await?;
        f.flush().await?;
        assert_eq!(
            f.query(query).await?.rows()[0].get::<String>("name")?,
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn review_persisted_unique_owner_is_enforced_after_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    {
        let db = Uni::open(path).build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL Account (key STRING UNIQUE)")
            .await?;
        tx.execute("CREATE (:Account {key:'same'})").await?;
        tx.commit().await?;
        db.flush().await?;
        db.shutdown().await?;
    }
    let db = Uni::open(path).build().await?;
    let tx = db.session().tx().await?;
    assert!(tx.execute("CREATE (:Account {key:'same'})").await.is_err());
    tx.rollback();
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (n:Account {key:'same'}) DELETE n")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let tx = s.tx().await?;
    tx.execute("CREATE (:Account {key:'same'})").await?;
    tx.commit().await?;
    assert_eq!(
        s.query("MATCH (n:Account) RETURN count(n) AS c")
            .await?
            .rows()[0]
            .get::<i64>("c")?,
        1
    );
    drop(s);
    db.shutdown().await?;
    Ok(())
}

// Exercise the real Arrow/Parquet callers of the patched zstd implementation,
// including repeated contexts across batches and row groups.
#[test]
fn review_zstd_compressed_arrow_and_parquet_round_trips() -> Result<()> {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_ipc::{
        CompressionType,
        reader::FileReader,
        writer::{FileWriter, IpcWriteOptions},
    };
    use arrow_schema::{DataType as ArrowType, Field, Schema};
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    };
    use std::{io::Cursor, sync::Arc};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("text", ArrowType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..512)),
            Arc::new(StringArray::from_iter(
                (0..512).map(|i| (i % 3 != 0).then_some("repeated graph property")),
            )),
        ],
    )?;
    let options = IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;
    let mut ipc = Vec::new();
    {
        let mut writer = FileWriter::try_new_with_options(&mut ipc, &schema, options)?;
        for _ in 0..3 {
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    let decoded =
        FileReader::try_new(Cursor::new(ipc), None)?.collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(decoded, vec![batch.clone(); 3]);

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(512))
        .build();
    let mut file = tempfile::tempfile()?;
    {
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props))?;
        for _ in 0..3 {
            writer.write(&batch)?;
        }
        writer.close()?;
    }
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(512)
        .build()?;
    assert_eq!(
        reader.collect::<std::result::Result<Vec<_>, _>>()?,
        vec![batch; 3]
    );
    Ok(())
}
