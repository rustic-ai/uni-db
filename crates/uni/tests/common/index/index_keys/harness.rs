// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The differential harness for index keys: two databases receive the same
//! schema and writes, one with the index and one without, and every query
//! must answer the same on both.
//!
//! [`Differential::stages`] runs the queries at four points: rows unflushed;
//! flushed; after updates and deletes that are not yet flushed; and after
//! those are flushed. A [`Case`] carries a query with its constant as a
//! literal or as a parameter; one marked `served` must also be reported by
//! EXPLAIN as using the index and, once rows are flushed, consult it in
//! PROFILE.
//!
//! Generic over the index kind: a later key kind supplies its own schema,
//! index statement, writes and cases.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{Uni, UniConfig, Value};

/// One query of a differential run.
#[derive(Debug, Clone)]
pub struct Case {
    /// The Cypher query. Its rows are compared as a sorted multiset.
    pub query: String,
    /// Its parameters.
    pub params: Vec<(String, Value)>,
    /// Whether the index must serve it: EXPLAIN reports the property's index
    /// `used`, and a flushed-stage PROFILE of a non-empty answer shows the
    /// scan consulting it.
    pub served: bool,
}

impl Case {
    /// A query the index serves.
    pub fn served(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            params: Vec::new(),
            served: true,
        }
    }

    /// A query the index must not change the answer of, and need not serve.
    pub fn unserved(query: impl Into<String>) -> Self {
        Self {
            served: false,
            ..Self::served(query)
        }
    }

    /// The case with a parameter.
    #[must_use]
    pub fn param(mut self, name: &str, value: Value) -> Self {
        self.params.push((name.to_string(), value));
        self
    }
}

/// The writes of a differential run.
#[derive(Debug, Clone, Default)]
pub struct Writes {
    /// Statements before the first stage.
    pub initial: Vec<String>,
    /// Updates and deletes applied after the first flush.
    pub changes: Vec<String>,
}

/// Two databases, identical but for the index under test.
pub struct Differential {
    /// The database with the index.
    pub indexed: Uni,
    /// The database without it.
    pub plain: Uni,
    /// The label the index is on.
    pub label: String,
    /// The property the index is on.
    pub property: String,
}

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

/// Run `stmt` in its own transaction.
pub async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

/// The rows of `case` on `db`, each rendered, sorted.
pub async fn rows(db: &Uni, case: &Case) -> Result<Vec<String>> {
    let session = db.session();
    let mut builder = session.query_with(&case.query);
    for (k, v) in &case.params {
        builder = builder.param(k.as_str(), v.clone());
    }
    let result = builder.fetch_all().await?;
    let mut out: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    out.sort();
    Ok(out)
}

impl Differential {
    /// Open both databases with `schema` applied, and `index` applied to the
    /// indexed one only.
    pub async fn open(schema: &[&str], index: &str, label: &str, property: &str) -> Result<Self> {
        let indexed = Uni::in_memory().config(config()).build().await?;
        let plain = Uni::in_memory().config(config()).build().await?;
        for stmt in schema {
            run(&indexed, stmt).await?;
            run(&plain, stmt).await?;
        }
        run(&indexed, index).await?;
        Ok(Self {
            indexed,
            plain,
            label: label.to_string(),
            property: property.to_string(),
        })
    }

    /// Apply `stmt` to both databases.
    pub async fn write(&self, stmt: &str) -> Result<()> {
        run(&self.indexed, stmt).await?;
        run(&self.plain, stmt).await
    }

    /// Flush both databases.
    pub async fn flush(&self) -> Result<()> {
        self.indexed.flush().await?;
        self.plain.flush().await?;
        Ok(())
    }

    /// Whether the indexed database's EXPLAIN of `case` reports the index as
    /// used on the property.
    pub async fn explained_as_used(&self, case: &Case) -> Result<bool> {
        let session = self.indexed.session();
        let mut builder = session.query_with(&case.query);
        for (k, v) in &case.params {
            builder = builder.param(k.as_str(), v.clone());
        }
        let explain = builder.explain().await?;
        Ok(explain
            .index_usage
            .iter()
            .any(|u| u.label_or_type == self.label && u.property == self.property && u.used))
    }

    /// The index lookups the indexed database's scan made for `case`.
    pub async fn index_hits(&self, case: &Case) -> Result<usize> {
        let session = self.indexed.session();
        let mut builder = session.query_with(&case.query);
        for (k, v) in &case.params {
            builder = builder.param(k.as_str(), v.clone());
        }
        let (_, profile) = builder.profile().await?;
        Ok(profile
            .runtime_stats
            .iter()
            .filter(|s| s.operator == "GraphScanExec")
            .filter_map(|s| s.index_hits)
            .sum())
    }

    /// Assert every case answers the same on both databases; at a
    /// `flushed` stage, that every served case consults the index.
    pub async fn check(&self, stage: &str, cases: &[Case], flushed: bool) -> Result<()> {
        for case in cases {
            let want = rows(&self.plain, case).await?;
            let got = rows(&self.indexed, case).await?;
            assert_eq!(
                got, want,
                "{stage}: answers differ with the index for {case:?}"
            );
            let used = self.explained_as_used(case).await?;
            if case.served {
                assert!(
                    used,
                    "{stage}: EXPLAIN does not report the index for {case:?}"
                );
                // Lance reports no index activity for a lookup that matched
                // nothing, so only a non-empty answer proves the index read.
                if flushed && !want.is_empty() {
                    let hits = self.index_hits(case).await?;
                    if hits == 0 {
                        let session = self.indexed.session();
                        let mut builder = session.query_with(&case.query);
                        for (k, v) in &case.params {
                            builder = builder.param(k.as_str(), v.clone());
                        }
                        let (_, profile) = builder.profile().await?;
                        panic!(
                            "{stage}: the scan did not consult the index for {case:?}: {:#?}",
                            profile.runtime_stats
                        );
                    }
                }
            } else {
                assert!(!used, "{stage}: EXPLAIN reports the index for {case:?}");
            }
        }
        Ok(())
    }

    /// Run `cases` at the four stages over `writes`.
    pub async fn stages(&self, writes: &Writes, cases: &[Case]) -> Result<()> {
        for stmt in &writes.initial {
            self.write(stmt).await?;
        }
        self.check("unflushed", cases, false).await?;
        self.flush().await?;
        self.check("flushed", cases, true).await?;
        for stmt in &writes.changes {
            self.write(stmt).await?;
        }
        self.check("changed, unflushed", cases, false).await?;
        self.flush().await?;
        self.check("changed, flushed", cases, true).await
    }
}
