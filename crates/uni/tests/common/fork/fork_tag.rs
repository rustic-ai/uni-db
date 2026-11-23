// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — `Uni::tag_fork` GC-exempt tags.
//!
//! Lance tags pin specific versions of branches GC-exempt so a tagged
//! fork's state survives compaction and even fork drops. Tests cover:
//! - Round-trip: tag a fork, list tags, untag, list tags.
//! - Pin-at-create: subsequent fork writes do not move the tag.
//! - Drop-tagged-fork: branches go away with the fork; the tagged
//!   versions remain referenceable through the underlying Lance refs
//!   (since tags hold a separate ref).

// Rust guideline compliant

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::Uni;

#[tokio::test]
async fn tag_list_untag_roundtrip() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = Uni::open(&uri).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = primary.fork("scenario").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'fork-row'})").await?;
    tx.commit().await?;
    fork.flush().await?;
    drop(fork);

    db.tag_fork("scenario", "audit-2026-q1").await?;
    let tags = db.list_fork_tags("scenario").await?;
    assert!(
        tags.iter().any(|t| t == "audit-2026-q1"),
        "tag should appear in list; got {tags:?}"
    );

    db.untag_fork("scenario", "audit-2026-q1").await?;
    let tags = db.list_fork_tags("scenario").await?;
    assert!(
        !tags.iter().any(|t| t == "audit-2026-q1"),
        "tag should be gone after untag; got {tags:?}"
    );

    // Untag is idempotent.
    db.untag_fork("scenario", "audit-2026-q1").await?;

    db.shutdown().await?;
    Ok(())
}

/// L9: re-tagging an already-tagged fork fails CLEANLY at the pre-check
/// (before mutating any dataset), instead of partially re-tagging and then
/// hitting a mid-loop `RefConflict`. The existing tag is left untouched.
#[tokio::test]
async fn tag_fork_pre_check_prevents_partial_retag() -> Result<()> {
    use uni_common::api::error::UniError;

    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = Uni::open(&uri).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = primary.fork("scenario").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'fork-row'})").await?;
    tx.commit().await?;
    fork.flush().await?;
    drop(fork);

    // First tag spans every fork dataset.
    db.tag_fork("scenario", "hold").await?;

    // Re-tagging the SAME name must fail at the pre-check (the tag already
    // exists on at least one dataset), not mid-loop.
    match db.tag_fork("scenario", "hold").await {
        Err(UniError::ForkLifecycle { stage: "tag", .. }) => {}
        Err(other) => panic!("expected a tag ForkLifecycle error, got {other:?}"),
        Ok(()) => panic!("re-tagging an already-tagged fork must error"),
    }

    // The tag is still present exactly once — nothing was duplicated or left
    // in a partial state.
    let tags = db.list_fork_tags("scenario").await?;
    assert_eq!(
        tags.iter().filter(|t| *t == "hold").count(),
        1,
        "the original tag must be intact and unique; got {tags:?}"
    );

    // Untag cleanly removes it everywhere.
    db.untag_fork("scenario", "hold").await?;
    assert!(
        db.list_fork_tags("scenario").await?.is_empty(),
        "untag must remove the tag from every dataset"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn tag_fork_unknown_fork_errors() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db.tag_fork("does-not-exist", "v1").await.unwrap_err();
    match err {
        uni_common::api::error::UniError::ForkNotFound { name } => {
            assert_eq!(name, "does-not-exist");
        }
        other => panic!("expected ForkNotFound, got {other:?}"),
    }
    db.shutdown().await?;
    Ok(())
}

/// Seed a persistent primary with one flushed `Person` row and return the db.
async fn seeded_db(uri: &str) -> Result<Uni> {
    let db = Uni::open(uri).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Write one row through fork `name` (creating it), flush it, release it.
async fn write_fork_row(db: &Uni, name: &str, row: &str) -> Result<()> {
    let fork = db.session().fork(name).await?;
    let tx = fork.tx().await?;
    tx.execute(&format!("CREATE (:Person {{name: '{row}'}})"))
        .await?;
    tx.commit().await?;
    fork.flush().await?;
    Ok(())
}

/// A tagged branch cannot be deleted, so dropping a tagged fork is refused up
/// front with the tag names, and nothing is tombstoned: the fork stays usable,
/// and once untagged it drops cleanly.
#[tokio::test]
async fn drop_of_tagged_fork_is_refused_and_leaves_it_active() -> Result<()> {
    use uni_common::api::error::UniError;

    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = seeded_db(&uri).await?;
    write_fork_row(&db, "audited", "fork-row").await?;
    db.tag_fork("audited", "hold").await?;

    match db.drop_fork("audited").await {
        Err(UniError::ForkTagged { name, tags }) => {
            assert_eq!(name, "audited");
            assert_eq!(tags, vec!["hold".to_string()]);
        }
        other => panic!("expected ForkTagged, got {other:?}"),
    }
    // Still Active and readable; the tag is still listed.
    let info = db.fork_info("audited").await?;
    assert_eq!(format!("{:?}", info.status), "Active");
    assert_eq!(
        db.list_fork_tags("audited").await?,
        vec!["hold".to_string()]
    );
    let fork = db.session().fork("audited").await?;
    let rows = fork.query("MATCH (p:Person) RETURN count(p) AS c").await?;
    assert_eq!(rows.rows()[0].get::<i64>("c")?, 2);
    drop(fork);

    db.untag_fork("audited", "hold").await?;
    db.drop_fork("audited").await?;
    assert!(db.fork_info("audited").await.is_err());
    db.shutdown().await?;
    Ok(())
}

/// The cascade checks every node for tags before tombstoning any of them.
#[tokio::test]
async fn drop_fork_cascade_refuses_a_tagged_descendant() -> Result<()> {
    use uni_common::api::error::UniError;

    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = seeded_db(&uri).await?;
    {
        let parent = db.session().fork("parent").await?;
        let tx = parent.tx().await?;
        tx.execute("CREATE (:Person {name: 'p'})").await?;
        tx.commit().await?;
        parent.flush().await?;
        let child = parent.fork("child").await?;
        let tx = child.tx().await?;
        tx.execute("CREATE (:Person {name: 'c'})").await?;
        tx.commit().await?;
        child.flush().await?;
    }
    db.tag_fork("child", "keep").await?;

    match db.drop_fork_cascade("parent").await {
        Err(UniError::ForkTagged { name, tags }) => {
            assert_eq!(name, "child");
            assert_eq!(tags, vec!["keep".to_string()]);
        }
        other => panic!("expected ForkTagged, got {other:?}"),
    }
    let mut names: Vec<String> = db.list_forks().await.into_iter().map(|f| f.name).collect();
    names.sort();
    assert_eq!(names, vec!["child".to_string(), "parent".to_string()]);

    db.untag_fork("child", "keep").await?;
    db.drop_fork_cascade("parent").await?;
    assert!(db.list_forks().await.is_empty());
    db.shutdown().await?;
    Ok(())
}

/// Tags are per fork: two forks can use the same tag name, each lists only its
/// own, and untagging one leaves the other's tag in place.
#[tokio::test]
async fn tags_are_scoped_to_their_fork() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = seeded_db(&uri).await?;
    write_fork_row(&db, "a", "a-row").await?;
    write_fork_row(&db, "b", "b-row").await?;

    db.tag_fork("a", "audit").await?;
    db.tag_fork("b", "audit").await?;
    db.tag_fork("b", "b-only").await?;
    assert_eq!(db.list_fork_tags("a").await?, vec!["audit".to_string()]);
    assert_eq!(
        db.list_fork_tags("b").await?,
        vec!["audit".to_string(), "b-only".to_string()]
    );

    db.untag_fork("a", "audit").await?;
    assert!(db.list_fork_tags("a").await?.is_empty());
    assert_eq!(
        db.list_fork_tags("b").await?,
        vec!["audit".to_string(), "b-only".to_string()]
    );
    // `a` has no tags now, so it drops; `b` is still refused.
    db.drop_fork("a").await?;
    assert!(db.drop_fork("b").await.is_err());
    db.untag_fork("b", "audit").await?;
    db.untag_fork("b", "b-only").await?;
    db.drop_fork("b").await?;
    db.shutdown().await?;
    Ok(())
}

/// A tag written in the earlier `fork_{tag}_{dataset}` form is still listed,
/// blocks the drop, and is removed by `untag_fork`.
#[tokio::test]
async fn legacy_tag_names_are_still_listed_and_untagged() -> Result<()> {
    use uni_common::api::error::UniError;

    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = seeded_db(&uri).await?;
    write_fork_row(&db, "old", "old-row").await?;

    let info = db.fork_info("old").await?;
    assert!(!info.datasets.is_empty());
    let storage = db.storage();
    let backend = storage.backend();
    let branching = backend.branching().expect("lance backend branches");
    for (dataset, branch) in &info.datasets {
        branching
            .create_tag(dataset, &format!("fork_legacy_{dataset}"), branch)
            .await?;
    }

    assert_eq!(db.list_fork_tags("old").await?, vec!["legacy".to_string()]);
    assert!(matches!(
        db.drop_fork("old").await,
        Err(UniError::ForkTagged { .. })
    ));
    db.untag_fork("old", "legacy").await?;
    assert!(db.list_fork_tags("old").await?.is_empty());
    db.drop_fork("old").await?;
    db.shutdown().await?;
    Ok(())
}

/// Tagging flushes the fork first, so a fork whose writes are still in L0 (and
/// that has therefore branched nothing yet) gets its tag.
#[tokio::test]
async fn tag_fork_flushes_unflushed_writes_first() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = Uni::open(&uri).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    {
        let fork = db.session().fork("fresh").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'only-in-l0'})").await?;
        tx.commit().await?;
    }
    db.tag_fork("fresh", "t1").await?;
    assert_eq!(db.list_fork_tags("fresh").await?, vec!["t1".to_string()]);
    assert!(!db.fork_info("fresh").await?.datasets.is_empty());
    db.untag_fork("fresh", "t1").await?;
    db.shutdown().await?;
    Ok(())
}

/// A fork with nothing branched (no writes, nothing on primary) cannot be
/// tagged, and says so instead of reporting success.
#[tokio::test]
async fn tag_fork_without_branched_datasets_errors() -> Result<()> {
    use uni_common::api::error::UniError;

    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = Uni::open(&uri).build().await?;
    db.session().fork("empty").await?;
    match db.tag_fork("empty", "t1").await {
        Err(UniError::ForkLifecycle { stage: "tag", .. }) => {}
        other => panic!("expected a tag ForkLifecycle error, got {other:?}"),
    }
    assert!(db.list_fork_tags("empty").await?.is_empty());
    db.shutdown().await?;
    Ok(())
}
