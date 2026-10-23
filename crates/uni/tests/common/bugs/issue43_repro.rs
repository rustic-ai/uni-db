// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Reproduction test for issue #43: Auto-embed insert latency grows over time.
//
// This test mirrors the scenario from the issue: 300 Message nodes with SENT_BY
// and MENTIONS edges, using the Nomic Embed Text v1.5 model (768d).
//
// Run with:
//   cargo nextest run --features provider-onnx --test issue43_repro --run-ignored all --no-capture

#[cfg(feature = "provider-onnx")]
mod tests {
    use anyhow::Result;
    use serde_json::json;
    use std::time::Instant;
    use uni_db::api::schema::{EmbeddingCfg, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
    use uni_db::{DataType, ModelAliasSpec, ModelTask, Uni, WarmupPolicy};

    const NUM_INSERTS: usize = 300;

    fn nomic_embed_alias() -> ModelAliasSpec {
        ModelAliasSpec {
            alias: "embed/default".to_string(),
            task: ModelTask::Embed,
            provider_id: "local/onnx".to_string(),
            model_id: "NomicEmbedTextV15".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: json!({}),
        }
    }

    async fn setup_schema(db: &Uni) -> Result<()> {
        db.schema()
            .label("Message")
            .property("content", DataType::String)
            .property_nullable("embedding", DataType::Vector { dimensions: 768 })
            .index(
                "embedding",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: Some(EmbeddingCfg {
                        alias: "embed/default".to_string(),
                        source_properties: vec!["content".to_string()],
                        batch_size: 32,
                        document_prefix: None,
                        query_prefix: None,
                    }),
                }),
            )
            .done()
            .label("Entity")
            .property("name", DataType::String)
            .done()
            .label("Participant")
            .property("name", DataType::String)
            .done()
            .edge_type("MENTIONS", &["Message"], &["Entity"])
            .done()
            .edge_type("SENT_BY", &["Message"], &["Participant"])
            .done()
            .apply()
            .await?;
        Ok(())
    }

    fn print_report(label: &str, latencies: &[u64], total_secs: f64) {
        let num = latencies.len();

        eprintln!("\n--- Latency trend (50-insert buckets) ---");
        for chunk_start in (0..num).step_by(50) {
            let chunk_end = (chunk_start + 50).min(num);
            let start_idx = if chunk_start == 0 { 1 } else { chunk_start };
            if start_idx >= chunk_end {
                continue;
            }
            let avg = latencies[start_idx..chunk_end].iter().sum::<u64>()
                / (chunk_end - start_idx) as u64;
            let max = latencies[start_idx..chunk_end].iter().max().unwrap_or(&0);
            eprintln!(
                "  [{:>3}-{:>3}]: avg={:>4}ms  max={:>6}ms",
                start_idx,
                chunk_end - 1,
                avg,
                max
            );
        }

        let first_50_avg = latencies[1..50].iter().sum::<u64>() / 49;
        let last_start = num - 50;
        let last_50_avg = latencies[last_start..].iter().sum::<u64>() / 50;
        let ratio = last_50_avg as f64 / first_50_avg.max(1) as f64;

        eprintln!("\n--- {} Summary ---", label);
        eprintln!("Total time:          {:.1}s", total_secs);
        eprintln!("Cold start:          {}ms", latencies[0]);
        eprintln!("First 50 avg:        {}ms", first_50_avg);
        eprintln!("Last 50 avg:         {}ms", last_50_avg);
        eprintln!("Growth ratio:        {:.1}x", ratio);

        if ratio > 5.0 {
            eprintln!("*** ISSUE REPRODUCED: {:.1}x growth ***", ratio);
        } else if ratio > 2.0 {
            eprintln!(
                "*** MODERATE GROWTH: {:.1}x — worth investigating ***",
                ratio
            );
        } else {
            eprintln!("No significant growth detected");
        }
    }

    /// Full reproduction: 300 messages with edges, Nomic 768d, tx per insert.
    #[tokio::test]
    #[ignore]
    async fn issue43_full_repro() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![nomic_embed_alias()])
            .build()
            .await?;
        setup_schema(&db).await?;

        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Entity {name: 'Jon'})").await?;
        tx.execute("CREATE (:Entity {name: 'Gina'})").await?;
        tx.execute("CREATE (:Participant {name: 'Jon'})").await?;
        tx.execute("CREATE (:Participant {name: 'Gina'})").await?;
        tx.commit().await?;

        eprintln!(
            "\n=== Issue #43 Full Repro: {} msgs + edges, Nomic 768d, tx-per-insert ===",
            NUM_INSERTS
        );
        eprintln!("---");

        let mut latencies = Vec::with_capacity(NUM_INSERTS);
        let total_start = Instant::now();

        for i in 0..NUM_INSERTS {
            let speaker = if i % 2 == 0 { "Jon" } else { "Gina" };
            let other = if i % 2 == 0 { "Gina" } else { "Jon" };
            let content = format!("Message {} from {} about business and dance", i, speaker);

            let start = Instant::now();
            let tx = session.tx().await?;
            tx.execute_with(&format!(
                "CREATE (m:Message {{content: $c}}) \
                     WITH m MATCH (p:Participant {{name: '{}'}}) CREATE (m)-[:SENT_BY]->(p) \
                     WITH m MATCH (e:Entity {{name: '{}'}}) CREATE (m)-[:MENTIONS]->(e)",
                speaker, other
            ))
            .param("c", content)
            .run()
            .await?;
            tx.commit().await?;
            let elapsed = start.elapsed();
            latencies.push(elapsed.as_millis() as u64);

            if i % 20 == 0 || i == NUM_INSERTS - 1 {
                eprintln!(
                    "insert {:>4}: {:>6}ms  (cumulative: {:.1}s)",
                    i,
                    elapsed.as_millis(),
                    total_start.elapsed().as_secs_f64()
                );
            }
        }

        let total_secs = total_start.elapsed().as_secs_f64();
        print_report("tx-per-insert", &latencies, total_secs);
        Ok(())
    }

    /// Same workload but using PreparedQuery (skip re-parse + re-plan each insert).
    #[tokio::test]
    #[ignore]
    async fn issue43_prepared_query() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![nomic_embed_alias()])
            .build()
            .await?;
        setup_schema(&db).await?;

        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Entity {name: 'Jon'})").await?;
        tx.execute("CREATE (:Entity {name: 'Gina'})").await?;
        tx.execute("CREATE (:Participant {name: 'Jon'})").await?;
        tx.execute("CREATE (:Participant {name: 'Gina'})").await?;
        tx.commit().await?;

        // Prepare queries once — one per speaker since MATCH patterns differ.
        // A prepared write must come from a transaction (session-level
        // prepared queries are read-only), so the whole run is one
        // transaction rather than one per insert.
        let tx = session.tx().await?;
        let stmt_jon = tx
            .prepare(
                "CREATE (m:Message {content: $c}) \
                 WITH m MATCH (p:Participant {name: 'Jon'}) CREATE (m)-[:SENT_BY]->(p) \
                 WITH m MATCH (e:Entity {name: 'Gina'}) CREATE (m)-[:MENTIONS]->(e)",
            )
            .await?;
        let stmt_gina = tx
            .prepare(
                "CREATE (m:Message {content: $c}) \
                 WITH m MATCH (p:Participant {name: 'Gina'}) CREATE (m)-[:SENT_BY]->(p) \
                 WITH m MATCH (e:Entity {name: 'Jon'}) CREATE (m)-[:MENTIONS]->(e)",
            )
            .await?;

        eprintln!(
            "\n=== Issue #43 PreparedQuery: {} msgs + edges, Nomic 768d ===",
            NUM_INSERTS
        );
        eprintln!("---");

        let mut latencies = Vec::with_capacity(NUM_INSERTS);
        let total_start = Instant::now();

        for i in 0..NUM_INSERTS {
            let stmt = if i % 2 == 0 { &stmt_jon } else { &stmt_gina };
            let content = format!("Message {} about business and dance", i);

            let start = Instant::now();
            stmt.bind().param("c", content).execute().await?;
            let elapsed = start.elapsed();
            latencies.push(elapsed.as_millis() as u64);

            if i % 20 == 0 || i == NUM_INSERTS - 1 {
                eprintln!(
                    "insert {:>4}: {:>6}ms  (cumulative: {:.1}s)",
                    i,
                    elapsed.as_millis(),
                    total_start.elapsed().as_secs_f64()
                );
            }
        }

        tx.commit().await?;
        let total_secs = total_start.elapsed().as_secs_f64();
        print_report("prepared-query", &latencies, total_secs);

        Ok(())
    }
}
