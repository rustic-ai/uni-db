// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Smoke tests against the MinIO, Azurite and fake-gcs-server emulators.
//!
//! Each opens a store remote-only under a fresh prefix, writes, flushes,
//! reopens, forks, and backs it up to a second prefix of the same bucket,
//! which it then opens. They run in the nightly and CI `cloud` jobs, which
//! start the emulators and create the bucket or container (see
//! `.github/workflows/ci.yml`); locally, start them the same way. Endpoints
//! and names come from the environment, with the CI defaults:
//!
//! | Emulator | Variables (defaults) |
//! |---|---|
//! | MinIO | `UNI_MINIO_ENDPOINT` (`http://127.0.0.1:9000`), `UNI_MINIO_BUCKET` (`uni-test`), keys `minioadmin` |
//! | Azurite | `UNI_AZURITE_ENDPOINT` (`http://127.0.0.1:10000/devstoreaccount1`), `UNI_AZURITE_CONTAINER` (`uni-test`) |
//! | fake-gcs-server | `UNI_FAKE_GCS_ENDPOINT` (`http://127.0.0.1:4443`), `UNI_FAKE_GCS_BUCKET` (`uni-test`) |

use std::collections::BTreeMap;

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig};

fn var(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn quiet() -> UniConfig {
    UniConfig {
        auto_flush_interval: None,
        auto_flush_threshold: 1_000_000,
        ..Default::default()
    }
}

async fn open(uri: &str, options: &BTreeMap<String, String>) -> Result<Uni> {
    Ok(Uni::open(uri)
        .storage_options(options.clone())
        .config(quiet())
        .build()
        .await?)
}

async fn names(db: &Uni) -> Result<Vec<String>> {
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;
    rows.rows()
        .iter()
        .map(|r| Ok(r.get::<String>("name")?))
        .collect()
}

async fn create(db: &Uni, name: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(&format!("CREATE (:Person {{name: '{name}'}})"))
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Write, flush, reopen, fork, back up and open the backup, at `base/{run}`.
async fn smoke(base: &str, options: BTreeMap<String, String>) -> Result<()> {
    let run = uuid::Uuid::new_v4().simple().to_string();
    let uri = format!("{base}/{run}/store");
    let db = open(&uri, &options).await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    create(&db, "Alice").await?;
    db.flush().await?;
    create(&db, "Bob").await?;
    db.shutdown().await?;

    let db = open(&uri, &options).await?;
    assert_eq!(names(&db).await?, ["Alice", "Bob"], "reopened");
    let fork = db.session().fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    let rows = fork.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(
        rows.rows()[0].get::<i64>("c")?,
        3,
        "the fork reads its own and primary's"
    );
    assert_eq!(
        names(&db).await?,
        ["Alice", "Bob"],
        "primary unchanged by the fork"
    );
    drop(fork);
    let backup = format!("{base}/{run}/backup");
    db.session().run(&format!("BACKUP TO '{backup}'")).await?;
    db.shutdown().await?;

    let restored = open(&backup, &options).await?;
    assert_eq!(names(&restored).await?, ["Alice", "Bob"], "the backup");
    restored.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires MinIO; see the module docs"]
async fn minio_store_round_trips() -> Result<()> {
    let endpoint = var("UNI_MINIO_ENDPOINT", "http://127.0.0.1:9000");
    let bucket = var("UNI_MINIO_BUCKET", "uni-test");
    let options = BTreeMap::from([
        ("aws_endpoint".to_string(), endpoint),
        ("aws_access_key_id".to_string(), "minioadmin".to_string()),
        (
            "aws_secret_access_key".to_string(),
            "minioadmin".to_string(),
        ),
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("aws_allow_http".to_string(), "true".to_string()),
    ]);
    smoke(&format!("s3://{bucket}"), options).await
}

#[tokio::test]
#[ignore = "Requires Azurite; see the module docs"]
async fn azurite_store_round_trips() -> Result<()> {
    let endpoint = var(
        "UNI_AZURITE_ENDPOINT",
        "http://127.0.0.1:10000/devstoreaccount1",
    );
    let container = var("UNI_AZURITE_CONTAINER", "uni-test");
    let options = BTreeMap::from([
        (
            "azure_storage_account_name".to_string(),
            "devstoreaccount1".to_string(),
        ),
        (
            "azure_storage_account_key".to_string(),
            // Azurite's well-known development key.
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
                .to_string(),
        ),
        ("azure_storage_endpoint".to_string(), endpoint),
        ("azure_allow_http".to_string(), "true".to_string()),
    ]);
    smoke(&format!("az://{container}"), options).await
}

#[tokio::test]
#[ignore = "Requires fake-gcs-server; see the module docs"]
async fn fake_gcs_store_round_trips() -> Result<()> {
    let endpoint = var("UNI_FAKE_GCS_ENDPOINT", "http://127.0.0.1:4443");
    let bucket = var("UNI_FAKE_GCS_BUCKET", "uni-test");
    // object_store's emulator form of a service account: no OAuth, and the
    // emulator's base URL.
    let dir = tempfile::tempdir()?;
    let account = dir.path().join("service-account.json");
    std::fs::write(
        &account,
        serde_json::to_vec(&serde_json::json!({
            "gcs_base_url": endpoint,
            "disable_oauth": true,
            "client_email": "",
            "private_key": "",
            "private_key_id": ""
        }))?,
    )?;
    let options = BTreeMap::from([(
        "google_service_account".to_string(),
        account.display().to_string(),
    )]);
    smoke(&format!("gs://{bucket}"), options).await
}
