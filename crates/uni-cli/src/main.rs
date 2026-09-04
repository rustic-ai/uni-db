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
}

#[derive(Subcommand)]
enum Commands {
    /// Import data from JSONL
    Import {
        /// Dataset name (e.g. semantic-scholar)
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
        /// Path to DB storage
        #[arg(long, default_value = "./storage")]
        path: PathBuf,
    },
    /// Start the interactive REPL
    Repl {
        /// Path to DB storage
        #[arg(long, default_value = "./storage")]
        path: PathBuf,
    },
    /// Manage snapshots
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCmd,
        /// Path to DB storage
        #[arg(long, default_value = "./storage")]
        path: PathBuf,
    },
    /// Manage runtime-loaded plugins
    Plugin {
        #[command(subcommand)]
        command: PluginCmd,
        /// Path to DB storage
        #[arg(long, default_value = "./storage")]
        path: PathBuf,
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

    let command = cli.command.unwrap_or(Commands::Repl {
        path: PathBuf::from("./storage"),
    });

    match command {
        Commands::Import {
            name,
            papers,
            citations,
            output,
        } => {
            println!("Importing dataset '{name}'...");
            crate::demo::semantic_scholar::import_semantic_scholar(&papers, &citations, &output)
                .await?;
        }
        Commands::Query { statement, path } => {
            let db = open_db(&path).await?;
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
            let db = open_db(&path).await?;
            let outcome = repl::run_repl(&db).await;
            close_db(db).await?;
            outcome?;
        }
        Commands::Plugin { command, path } => {
            let db = open_db(&path).await?;
            let outcome = match command {
                PluginCmd::Install { source, grants } => {
                    install_plugin(&db, &source, grants.as_deref()).await
                }
            };
            close_db(db).await?;
            outcome?;
        }
        Commands::Snapshot { command, path } => {
            let db = open_db(&path).await?;
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
/// # Errors
///
/// Returns an error if the storage at `path` cannot be opened or built.
async fn open_db(path: &std::path::Path) -> Result<uni_db::Uni> {
    Ok(uni_db::Uni::open(path.to_string_lossy()).build().await?)
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
