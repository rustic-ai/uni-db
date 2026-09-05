// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `crates/uni-cli/src/main.rs:139`.
//!
//! The one-shot `uni query` command discards the result of
//! `repl::execute_query`, which returns `()` and swallows every error into a
//! `println!` (stdout, red text). So a *failed* query still exits with status
//! `0`, and the error text lands on stdout rather than stderr — a shell/CI
//! pipeline treats the failure as success.
//!
//! This test drives the REAL compiled `uni` binary (`CARGO_BIN_EXE_uni`) with a
//! syntactically invalid Cypher statement and observes the exit code and which
//! stream the error went to.

use std::path::PathBuf;
use std::process::Command;

/// A unique temp directory for this test's throwaway database.
fn unique_tmp_dir(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!(
        "uni_cli_repro_{tag}_{}_{nanos}",
        std::process::id()
    ));
    p
}

/// Running `uni query <malformed>` exits non-zero and prints the error to stderr.
///
/// Fixed (main.rs:139 + repl.rs): the one-shot command now propagates a failure
/// as a non-zero exit code so a caller inspecting `$?` (or CI) can detect it,
/// and the error goes to stderr so stdout stays clean for piping.
#[test]
fn one_shot_query_failure_exits_nonzero_and_errors_to_stderr() {
    let bin = env!("CARGO_BIN_EXE_uni");
    let db_path = unique_tmp_dir("exit");

    // Malformed Cypher — unbalanced parenthesis; parse must fail at run().
    let output = Command::new(bin)
        .arg("query")
        .arg("--path")
        .arg(&db_path)
        .arg("MATCH (n RETURN n")
        .output()
        .expect("failed to spawn uni binary");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Best-effort cleanup of the throwaway db.
    let _ = std::fs::remove_dir_all(&db_path);

    eprintln!("--- exit code: {:?}", output.status.code());
    eprintln!("--- stdout:\n{stdout}");
    eprintln!("--- stderr:\n{stderr}");

    // A failed query now exits non-zero (fix for crates/uni-cli/src/main.rs:139),
    // so a caller inspecting `$?` / CI can detect the failure.
    assert_ne!(
        output.status.code(),
        Some(0),
        "expected non-zero exit on failed query; got {:?}",
        output.status.code()
    );

    // The error text is present, proving the query actually failed...
    assert!(
        stderr.contains("Error:"),
        "expected the error on STDERR; stderr was:\n{stderr}"
    );

    // ...and it went to stderr, NOT stdout (eprintln!, keeping stdout clean).
    assert!(
        !stdout.contains("Error:"),
        "error unexpectedly appeared on stdout; stdout was:\n{stdout}"
    );
}

/// A CLI write must survive the process that made it.
///
/// Repro for the store left with WAL segments and no snapshot manifest: every
/// subcommand built a `Uni` and dropped it, and `Drop for Uni` only signals the
/// background tasks — the L0 -> L1 flush that writes the manifest lives in
/// `shutdown`. So `uni query "CREATE ..."` printed success and exited `0` while
/// the write never landed, and the next `Uni::open` refused the store with
/// "Database has WAL segments but no snapshot manifest".
///
/// Before the fix this reproduced 9/9. The assertion that matters is the second
/// invocation: a store that cannot be reopened is indistinguishable from one
/// whose data was never written.
#[test]
fn a_cli_write_is_durable_and_the_store_reopens() {
    let bin = env!("CARGO_BIN_EXE_uni");
    let db_path = unique_tmp_dir("durable");

    let write = Command::new(bin)
        .arg("query")
        .arg("--path")
        .arg(&db_path)
        .arg("CREATE (n:Person {name: 'durable'})")
        .output()
        .expect("failed to spawn uni binary for the write");

    let write_stderr = String::from_utf8_lossy(&write.stderr);
    assert_eq!(
        write.status.code(),
        Some(0),
        "the write itself should succeed; stderr was:\n{write_stderr}"
    );
    // The write must not report success from a process that panicked on the way
    // out — that panic was how the truncated shutdown first surfaced.
    assert!(
        !write_stderr.contains("panicked"),
        "the write panicked during shutdown; stderr was:\n{write_stderr}"
    );

    // A second process: this is the half that failed before the fix.
    let read = Command::new(bin)
        .arg("query")
        .arg("--path")
        .arg(&db_path)
        .arg("MATCH (n:Person) RETURN n.name")
        .output()
        .expect("failed to spawn uni binary for the read");

    let read_stdout = String::from_utf8_lossy(&read.stdout);
    let read_stderr = String::from_utf8_lossy(&read.stderr);

    let _ = std::fs::remove_dir_all(&db_path);

    assert_eq!(
        read.status.code(),
        Some(0),
        "the store did not reopen after a CLI write; stderr was:\n{read_stderr}"
    );
    assert!(
        !read_stderr.contains("no snapshot manifest"),
        "store reopened into the WAL-without-manifest state; stderr was:\n{read_stderr}"
    );
    // Reopening is necessary but not sufficient — the row has to be there too.
    assert!(
        read_stdout.contains("durable"),
        "the written row did not survive the writing process; stdout was:\n{read_stdout}"
    );
}
