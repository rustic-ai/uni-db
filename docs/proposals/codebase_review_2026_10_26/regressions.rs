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
