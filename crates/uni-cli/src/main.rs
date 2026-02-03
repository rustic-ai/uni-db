// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::*;
use prettytable::{Cell, Row, Table};
use std::path::PathBuf;

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
}

#[derive(Subcommand)]
enum SnapshotCmd {
    /// List all snapshots
    List,
    /// Create a new snapshot
    Create {
        /// Optional name for the snapshot
        name: Option<String>,
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
            println!("Importing dataset '{}'...", name);
            crate::demo::semantic_scholar::import_semantic_scholar(&papers, &citations, &output)
                .await?;
        }
        Commands::Query { statement, path } => {
            let builder = uni_db::Uni::open(path.to_string_lossy().to_string());
            let db = builder.build().await?;

            repl::execute_query(&db, &statement).await;
        }
        Commands::Repl { path } => {
            let builder = uni_db::Uni::open(path.to_string_lossy().to_string());
            let db = builder.build().await?;
            repl::run_repl(db).await?;
        }
        Commands::Snapshot { command, path } => {
            let builder = uni_db::Uni::open(path.to_string_lossy().to_string());
            let db = builder.build().await?;

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
                    let id = db.create_snapshot(name.as_deref()).await?;
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
        }
    }

    Ok(())
}
