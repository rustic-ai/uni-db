// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Which WAL a hybrid store follows.
//!
//! A hybrid store keeps its WAL in a local directory and everything else in
//! the object store. The WAL directory carries an id (`wal_id.json`, beside
//! the WAL), and the store records the id of the WAL it follows
//! (`catalog/wal_claim.json`, in the object store). A writable open claims
//! the store for its own WAL before anything is committed. A WAL the store
//! does not follow, still holding commits, belongs to an earlier writer: a
//! host that crashed or was dropped with unflushed commits, whose store
//! another host has opened since. Its commits were made against a state the
//! store has moved on from, so replaying them would corrupt it.
//!
//! A store whose WAL lives beside it (a local or remote-only store) shares
//! that WAL with every writer and needs none of this.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload};
use serde::{Deserialize, Serialize};

/// The WAL's id, beside the WAL (`wal/`) in the same store.
pub const WAL_ID_FILE: &str = "wal_id.json";
/// The WAL's segments.
pub const WAL_PREFIX: &str = "wal";
/// Where a stale WAL's segments are moved, one directory per setting-aside.
pub const SET_ASIDE_PREFIX: &str = "wal_set_aside";

#[derive(Debug, Serialize, Deserialize)]
struct WalIdFile {
    wal_id: String,
}

/// The store's record of the WAL it follows, in its metadata root.
pub const CLAIM_FILE: &str = "catalog/wal_claim.json";

#[derive(Debug, Serialize, Deserialize)]
struct ClaimFile {
    wal_id: String,
    /// Where the claiming WAL lives, for an operator reading the file.
    #[serde(default)]
    wal: String,
    #[serde(default)]
    claimed_at: String,
}

/// The id of the WAL the store follows, if it has recorded one.
///
/// # Errors
/// The file could not be read or parsed.
pub async fn load_claim(meta: &Arc<dyn ObjectStore>) -> Result<Option<String>> {
    match meta.get(&Path::from(CLAIM_FILE)).await {
        Ok(got) => {
            let bytes = got.bytes().await?;
            let file: ClaimFile =
                serde_json::from_slice(&bytes).with_context(|| format!("parse {CLAIM_FILE}"))?;
            Ok(Some(file.wal_id))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Records that the store follows the WAL `wal_id`, found at `wal`. The
/// caller holds the store for writing.
///
/// # Errors
/// The write failed.
pub async fn claim(meta: &Arc<dyn ObjectStore>, wal_id: &str, wal: &str) -> Result<()> {
    let file = ClaimFile {
        wal_id: wal_id.to_string(),
        wal: wal.to_string(),
        claimed_at: chrono::Utc::now().to_rfc3339(),
    };
    meta.put(
        &Path::from(CLAIM_FILE),
        PutPayload::from(serde_json::to_vec_pretty(&file)?),
    )
    .await?;
    Ok(())
}

/// The WAL's id, if it has one.
///
/// # Errors
/// The file could not be read or parsed.
pub async fn load(store: &Arc<dyn ObjectStore>) -> Result<Option<String>> {
    match store.get(&Path::from(WAL_ID_FILE)).await {
        Ok(got) => {
            let bytes = got.bytes().await?;
            let file: WalIdFile =
                serde_json::from_slice(&bytes).with_context(|| format!("parse {WAL_ID_FILE}"))?;
            Ok(Some(file.wal_id))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The WAL's id, giving it a new one if it has none.
///
/// # Errors
/// The file could not be read or written.
pub async fn load_or_create(store: &Arc<dyn ObjectStore>) -> Result<String> {
    if let Some(id) = load(store).await? {
        return Ok(id);
    }
    let fresh = WalIdFile {
        wal_id: uuid::Uuid::new_v4().to_string(),
    };
    let payload = PutPayload::from(serde_json::to_vec(&fresh)?);
    match store
        .put_opts(&Path::from(WAL_ID_FILE), payload, PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(fresh.wal_id),
        // Another open created it first: that one is the WAL's.
        Err(object_store::Error::AlreadyExists { .. }) => load(store)
            .await?
            .context("wal_id.json vanished after it was created"),
        // A store without conditional writes: the open holds the store, so
        // no other open races this one.
        Err(object_store::Error::NotImplemented { .. })
        | Err(object_store::Error::NotSupported { .. }) => {
            store
                .put(
                    &Path::from(WAL_ID_FILE),
                    PutPayload::from(serde_json::to_vec(&fresh)?),
                )
                .await?;
            Ok(fresh.wal_id)
        }
        Err(e) => Err(e.into()),
    }
}

/// The WAL's segment paths.
///
/// # Errors
/// The listing failed.
pub async fn segments(store: &Arc<dyn ObjectStore>) -> Result<Vec<Path>> {
    use futures::TryStreamExt;
    let metas: Vec<_> = store
        .list(Some(&Path::from(WAL_PREFIX)))
        .try_collect()
        .await?;
    Ok(metas.into_iter().map(|m| m.location).collect())
}

/// How many of the WAL's segments hold commits: carry mutations, or cannot
/// be read to tell. A segment without mutations (the one a seal writes on a
/// clean close, recording how far commits were announced) loses nothing.
///
/// # Errors
/// A listing or read failed.
pub async fn segments_with_commits(store: &Arc<dyn ObjectStore>) -> Result<usize> {
    let mut with_commits = 0;
    for segment in segments(store).await? {
        let bytes = store.get(&segment).await?.bytes().await?;
        if bytes.is_empty() {
            continue;
        }
        match super::wal::decode_segment(&bytes) {
            Ok(decoded) if decoded.mutations.is_empty() => {}
            _ => with_commits += 1,
        }
    }
    Ok(with_commits)
}

/// Moves the WAL's segments to `wal_set_aside/{wal_id}-{time}/`, keeping
/// their names, and returns that directory. Nothing is deleted before its
/// copy is written.
///
/// # Errors
/// A copy or delete failed; the segments not yet moved stay in place.
pub async fn set_aside(store: &Arc<dyn ObjectStore>, wal_id: &str) -> Result<Path> {
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
    let dir = Path::from(format!("{SET_ASIDE_PREFIX}/{wal_id}-{stamp}"));
    let wal = Path::from(WAL_PREFIX);
    for segment in segments(store).await? {
        let rel: Vec<_> = segment
            .prefix_match(&wal)
            .context("a WAL segment outside the WAL")?
            .collect();
        let target = rel
            .iter()
            .fold(dir.clone(), |p, part| p.clone().join(part.as_ref()));
        store.copy(&segment, &target).await?;
        store.delete(&segment).await?;
    }
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn an_id_is_created_once_and_kept() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(load(&store).await.unwrap(), None);
        let a = load_or_create(&store).await.unwrap();
        let b = load_or_create(&store).await.unwrap();
        assert_eq!(a, b);
        assert_eq!(load(&store).await.unwrap(), Some(a));
    }

    #[tokio::test]
    async fn a_claim_round_trips() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(load_claim(&store).await.unwrap(), None);
        claim(&store, "a", "/x/wal").await.unwrap();
        claim(&store, "b", "/y/wal").await.unwrap();
        assert_eq!(load_claim(&store).await.unwrap().as_deref(), Some("b"));
    }

    /// Only a segment that may hold commits counts: an empty file and a
    /// mutation-free seal do not, an unreadable segment does.
    #[tokio::test]
    async fn segments_with_commits_counts_what_could_be_lost() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (name, body) in [
            ("wal/0001.wal", ""),
            ("wal/0002.wal", r#"{"lsn":2,"mutations":[]}"#),
        ] {
            store
                .put(
                    &Path::from(name),
                    PutPayload::from(body.as_bytes().to_vec()),
                )
                .await
                .unwrap();
        }
        assert_eq!(segments_with_commits(&store).await.unwrap(), 0);
        store
            .put(
                &Path::from("wal/0003.wal"),
                PutPayload::from(b"torn".to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(segments_with_commits(&store).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn setting_aside_moves_every_segment_and_keeps_its_bytes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (name, body) in [("wal/0001.wal", "one"), ("wal/0002.wal", "two")] {
            store
                .put(
                    &Path::from(name),
                    PutPayload::from(body.as_bytes().to_vec()),
                )
                .await
                .unwrap();
        }
        store
            .put(&Path::from("other.json"), PutPayload::from(b"x".to_vec()))
            .await
            .unwrap();
        let dir = set_aside(&store, "abc").await.unwrap();
        assert!(segments(&store).await.unwrap().is_empty());
        assert!(dir.as_ref().starts_with("wal_set_aside/abc-"));
        let moved = store
            .get(&dir.clone().join("0002.wal"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&moved[..], b"two");
        store.head(&Path::from("other.json")).await.unwrap();
    }
}
