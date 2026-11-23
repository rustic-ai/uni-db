// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use prettytable::{Cell, Row, Table};
use std::path::PathBuf;
use uni_plugin::{Capability, CapabilitySet};

// Use mimalloc as the global allocator. Profile showed ~50% of CPU time in
// glibc malloc + kernel page-fault zeroing under heavy concurrent allocation;
// mimalloc's thread-local arenas reduce that to ~5%, yielding ~3x throughput
// on allocation-heavy workloads (many small CREATE / mutation statements).
// See crates/uni/benches/concurrent_mutations.rs.
#[global_allocator]
static GLOBAL: uni_db::MiMalloc = uni_db::MiMalloc;

pub mod demo;
pub mod repl;

#[derive(Parser)]
#[command(name = "uni")]
#[command(about = "Uni Graph Database", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    /// Object-store option for a store opened at an `s3://`, `gs://` or
    /// `az://` path, as KEY=VALUE (for example `aws_endpoint=http://localhost:9000`).
    /// Repeatable. Overrides the environment (`AWS_*`, `GOOGLE_*`, `AZURE_*`).
    #[arg(long = "storage-option", global = true, value_name = "KEY=VALUE", value_parser = parse_storage_option)]
    storage_options: Vec<(String, String)>,
}

/// Parses one `--storage-option KEY=VALUE`.
fn parse_storage_option(raw: &str) -> std::result::Result<(String, String), String> {
    match raw.split_once('=') {
        Some((k, v)) if !k.trim().is_empty() => Ok((k.trim().to_string(), v.to_string())),
        // The value may be a secret: never echo it.
        _ => Err("expected KEY=VALUE".to_string()),
    }
}

/// The datasets `uni import <name>` can load.
const IMPORTERS: &[&str] = &["semantic-scholar"];

#[derive(Subcommand)]
enum Commands {
    /// Import a dataset from JSONL with a built-in importer
    Import {
        /// Importer to run (available: semantic-scholar)
        name: String,
        /// Path to papers JSONL
        #[arg(long)]
        papers: PathBuf,
        /// Path to citations JSONL
        #[arg(long)]
        citations: PathBuf,
        /// Output directory for DB storage
        #[arg(long, default_value = "./storage")]
        output: PathBuf,
    },
    /// Run a query
    Query {
        statement: String,
        /// Path or object-store URI (s3://, gs://, az://, file://) of the DB
        #[arg(long, default_value = "./storage")]
        path: String,
    },
    /// Start the interactive REPL
    Repl {
        /// Path or object-store URI (s3://, gs://, az://, file://) of the DB
        #[arg(long, default_value = "./storage")]
        path: String,
    },
    /// Manage snapshots
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCmd,
        /// Path or object-store URI (s3://, gs://, az://, file://) of the DB
        #[arg(long, default_value = "./storage")]
        path: String,
    },
    /// Manage runtime-loaded plugins
    Plugin {
        #[command(subcommand)]
        command: PluginCmd,
        /// Path or object-store URI (s3://, gs://, az://, file://) of the DB
        #[arg(long, default_value = "./storage")]
        path: String,
    },
}

#[derive(Subcommand)]
enum PluginCmd {
    /// Install a plugin from a local file or URL.
    ///
    /// Dispatch by file extension / scheme:
    ///   - `*.rhai`       → load via `Uni::load_rhai_plugin` (the only
    ///     loader Phase 11 wires; the WASM / OCI / Extism Hub branches
    ///     land in M12).
    ///   - `*.wasm`       → not yet supported in this CLI (M12).
    ///   - `oci://...`    → not yet supported (M12).
    ///   - `extism://...` → not yet supported (M12).
    Install {
        /// Local path or URL to install from.
        source: String,
        /// Comma-separated capability grant names (e.g.
        /// `ScalarFn,Filesystem,Network`). Defaults to
        /// `ScalarFn,AggregateFn,Procedure`.
        #[arg(long)]
        grants: Option<String>,
    },
}

#[derive(Subcommand)]
enum SnapshotCmd {
    /// List all snapshots
    List,
    /// Create a new snapshot
    Create {
        /// Name for the snapshot
        name: String,
    },
    /// Restore the database to a specific snapshot
    Restore {
        /// Snapshot ID to restore to
        id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();

    let storage_options = cli.storage_options;
    let command = cli.command.unwrap_or(Commands::Repl {
        path: "./storage".to_string(),
    });

    match command {
        Commands::Import {
            name,
            papers,
            citations,
            output,
        } => match name.as_str() {
            "semantic-scholar" => {
                println!("Importing dataset '{name}'...");
                crate::demo::semantic_scholar::import_semantic_scholar(
                    &papers, &citations, &output,
                )
                .await?;
            }
            _ => anyhow::bail!(
                "unknown importer '{name}'; available importers: {}",
                IMPORTERS.join(", ")
            ),
        },
        Commands::Query { statement, path } => {
            let db = open_db(&path, &storage_options).await?;
            let succeeded = repl::execute_query(&db, &statement).await;
            // Close before exiting: `std::process::exit` runs no destructors, and
            // the durability flush lives in shutdown rather than in `Drop`.
            close_db(db).await?;
            // Exit non-zero when the one-shot query fails, so scripts and CI can
            // detect failure instead of the previous always-0 exit.
            if !succeeded {
                std::process::exit(1);
            }
        }
        Commands::Repl { path } => {
            let db = open_db(&path, &storage_options).await?;
            let outcome = repl::run_repl(&db).await;
            close_db(db).await?;
            outcome?;
        }
        Commands::Plugin { command, path } => {
            let db = open_db(&path, &storage_options).await?;
            let outcome = match command {
                PluginCmd::Install { source, grants } => {
                    install_plugin(&db, &source, grants.as_deref()).await
                }
            };
            close_db(db).await?;
            outcome?;
        }
        Commands::Snapshot { command, path } => {
            let db = open_db(&path, &storage_options).await?;
            let outcome = run_snapshot_command(&db, command).await;
            close_db(db).await?;
            outcome?;
        }
    }

    Ok(())
}

/// Run a `uni snapshot` subcommand against an already-open database.
///
/// Split out of `main` so the caller keeps ownership of the handle and can
/// close it on both the success and the error path — see [`close_db`].
async fn run_snapshot_command(db: &uni_db::Uni, command: SnapshotCmd) -> Result<()> {
    match command {
        SnapshotCmd::List => {
            let snapshots = db.list_snapshots().await?;
            if snapshots.is_empty() {
                println!("No snapshots found.");
            } else {
                let mut table = Table::new();
                table.add_row(Row::new(vec![
                    Cell::new("ID").style_spec("bf"),
                    Cell::new("Name").style_spec("bf"),
                    Cell::new("Created At").style_spec("bf"),
                    Cell::new("Schema Ver").style_spec("bf"),
                ]));

                for s in snapshots {
                    table.add_row(Row::new(vec![
                        Cell::new(&s.snapshot_id),
                        Cell::new(s.name.as_deref().unwrap_or("-")),
                        Cell::new(&s.created_at.to_string()),
                        Cell::new(&s.schema_version.to_string()),
                    ]));
                }
                table.printstd();
            }
        }
        SnapshotCmd::Create { name } => {
            let id = db.create_snapshot(&name).await?;
            println!("{} Snapshot created: {}", "Success:".green(), id);
        }
        SnapshotCmd::Restore { id } => {
            db.restore_snapshot(&id).await?;
            println!("{} Restored to snapshot: {}", "Success:".green(), id);
            println!(
                "Note: You may need to restart any running servers/REPLs for changes to fully take effect."
            );
        }
    }

    Ok(())
}

/// Close the database opened by [`open_db`], making its writes durable.
///
/// `Drop for Uni` only signals the background tasks; the L0 -> L1 flush that
/// writes the snapshot manifest happens in `shutdown`. A CLI process that
/// returns without calling this leaves a store carrying WAL segments and no
/// manifest, which the next `Uni::open` refuses to reopen.
///
/// The flush is issued explicitly rather than left to shutdown, which logs a
/// flush failure instead of propagating it. A CLI write that did not reach
/// disk must not report success.
///
/// # Errors
///
/// Returns an error if the final flush or the shutdown fails. Both are run
/// before either is reported, so a failed flush still stops the background
/// tasks.
async fn close_db(db: uni_db::Uni) -> Result<()> {
    let flushed = db.flush().await;
    let stopped = db.shutdown().await;
    flushed?;
    stopped?;
    Ok(())
}

/// Open the database at `path` with the CLI's standard settings.
///
/// `path` is a local directory or an object-store URI; `storage_options`
/// configure the object-store client (see `--storage-option`).
///
/// # Errors
///
/// Returns an error if the storage at `path` cannot be opened or built.
async fn open_db(path: &str, storage_options: &[(String, String)]) -> Result<uni_db::Uni> {
    Ok(uni_db::Uni::open(path)
        .storage_options(storage_options.iter().cloned())
        .build()
        .await?)
}

/// Dispatch `uni plugin install <source>` by extension / scheme.
async fn install_plugin(db: &uni_db::Uni, source: &str, grants: Option<&str>) -> Result<()> {
    let caps = parse_grants(grants);

    // Scheme dispatch — OCI / extism Hub / HTTP land in M12; CLI rejects
    // them with a clear "not yet supported" message rather than silent
    // failure. The order of the `http`/`https` prefixes is irrelevant
    // because they share the same message.
    const M12_SCHEMES: &[(&str, &str)] = &[
        ("oci://", "OCI plugin installation lands in M12"),
        ("extism://", "Extism Hub installation lands in M12"),
        (
            "https://",
            "HTTP plugin fetch lands in M12 (download, signature pin)",
        ),
        (
            "http://",
            "HTTP plugin fetch lands in M12 (download, signature pin)",
        ),
    ];
    for (prefix, message) in M12_SCHEMES {
        if source.starts_with(prefix) {
            anyhow::bail!("{} {message}", "not yet supported:".red());
        }
    }

    // Local file by extension.
    let path = std::path::Path::new(source);
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match ext.as_str() {
        "rhai" => {
            #[cfg(feature = "rhai-plugins-cli")]
            {
                let script = std::fs::read_to_string(path)
                    .map_err(|e| anyhow::anyhow!("reading {source}: {e}"))?;
                let loader = uni_plugin_rhai::RhaiLoader::new();
                let outcome = db
                    .load_rhai_plugin(&loader, &script, &caps)
                    .map_err(|e| anyhow::anyhow!("load_rhai_plugin: {e}"))?;
                println!(
                    "{} loaded plugin `{}` v{}",
                    "ok:".green(),
                    outcome.plugin_id.as_str(),
                    outcome.version
                );
                if !outcome.scalars_registered.is_empty() {
                    println!("  scalars:    {}", outcome.scalars_registered.join(", "));
                }
                if !outcome.aggregates_registered.is_empty() {
                    println!("  aggregates: {}", outcome.aggregates_registered.join(", "));
                }
                if !outcome.procedures_registered.is_empty() {
                    println!("  procedures: {}", outcome.procedures_registered.join(", "));
                }
                if !outcome.algorithms_registered.is_empty() {
                    println!("  algorithms: {}", outcome.algorithms_registered.join(", "));
                }
                if !outcome.denied_capabilities.is_empty() {
                    println!(
                        "{} denied capabilities: {:?}",
                        "warn:".yellow(),
                        outcome.denied_capabilities
                    );
                }
                Ok(())
            }
            #[cfg(not(feature = "rhai-plugins-cli"))]
            {
                let _ = (db, caps);
                anyhow::bail!(
                    "{} Rhai plugin support requires the `rhai-plugins-cli` feature",
                    "build:".red()
                )
            }
        }
        "wasm" => anyhow::bail!(
            "{} WASM plugin installation via CLI lands in M12",
            "not yet supported:".red()
        ),
        _ => anyhow::bail!("unknown plugin format for `{source}` (expected .rhai)"),
    }
}

fn parse_grants(grants: Option<&str>) -> CapabilitySet {
    let names: Vec<&str> = match grants {
        Some(s) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect(),
        None => vec!["ScalarFn", "AggregateFn", "Procedure"],
    };
    let mut set = CapabilitySet::new();
    for n in names {
        // Delegate to the single source of truth shared with the Python binding;
        // warn and skip on anything not grantable from a bare string.
        match Capability::parse_grant(n) {
            Ok(cap) => {
                set.insert(cap);
            }
            Err(e) => {
                eprintln!("{} {e}", "warn:".yellow());
            }
        }
    }
    set
}

#[cfg(test)]
mod storage_option_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn storage_options_parse_and_repeat() {
        let cli = Cli::try_parse_from([
            "uni",
            "query",
            "RETURN 1",
            "--path",
            "s3://bucket/graph",
            "--storage-option",
            "aws_endpoint=http://localhost:9000",
            "--storage-option",
            "aws_region=us-east-1",
        ])
        .unwrap();
        assert_eq!(
            cli.storage_options,
            vec![
                (
                    "aws_endpoint".to_string(),
                    "http://localhost:9000".to_string()
                ),
                ("aws_region".to_string(), "us-east-1".to_string()),
            ]
        );
        match cli.command {
            Some(Commands::Query { path, .. }) => assert_eq!(path, "s3://bucket/graph"),
            _ => panic!("expected a query command"),
        }
    }

    #[test]
    fn a_value_may_contain_equals_signs() {
        assert_eq!(
            parse_storage_option("azure_storage_sas_key=sv=1&sig=a=b").unwrap(),
            (
                "azure_storage_sas_key".to_string(),
                "sv=1&sig=a=b".to_string()
            )
        );
    }

    #[test]
    fn a_malformed_option_is_rejected() {
        assert!(parse_storage_option("no-equals").is_err());
        assert!(parse_storage_option("=value").is_err());
    }
}
