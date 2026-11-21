# Proposal: correct, complete object-store support

- **Date:** 2026-11-20
- **Status:** Phase A implemented (2026-11-21); Phases B–E proposed
- **Issues:** #349 (schema ignores the URL prefix), #350 (remote-only open
  can't take an endpoint or credentials). Related: #348 (no store lock).
- **Scope:** S3 and S3-compatibles (MinIO, Cloudflare R2, LocalStack), GCS,
  Azure, in both remote modes:
  - **Remote-only:** `Uni::open("s3://…")`, with every artifact in the bucket.
  - **Hybrid:** `hybrid(url)` (plus `cloud_storage(cfg)`), with the WAL and ID allocator in a
    local directory and everything else in the bucket.

---

## Implementation status (Phase A)

Phase A shipped with one change from §5, made at review: **one layout for every store kind**. Plain paths, `file://` and object stores all use today's local layout under the store root `R` (`R/catalog`, `R/wal`, `R/id_allocator.json`, `R/storage/…`). §5.4's flat remote layout is kept only for stores written before 5.4 (`catalog/layout.json` = `flat`). Lance writes full table URIs into fork `BasePath`s, so existing tables are never moved. Stores written by ≤ 5.3.0 under a prefix are refused with an actionable error. Automatic adoption (§5.5) was not shipped: the case was already corrupt-prone, and a manual copy is documented in `website/docs/features/storage-cloud.md`.

Where it lives:

- `crates/uni-store/src/location.rs`: `StoreLocation`, the resolver and layout probing.
- `UniBuilder::{cloud_storage, storage_option(s), hybrid}`, with `remote_storage` deprecated.
- `IdAllocator::open` with a floor and typed conditional writes (ETag plus version, so GCS works).
- `SnapshotManifest::{next_vid, next_eid}`, stamped in `SnapshotManager::save_snapshot`.
- `StorageManager::{new_with_location, scan_id_floor}` and `Writer::raise_id_floor_from_l0`.
- Recovery of a lost `latest` by the newest manifest.
- Python `storage_options` and `cloud_config` in every mode.
- CLI `--storage-option` and URL `--path`.

Finished after the first commit:

- Storage options registered for Lance's by-URI opens live only as long as the database that registered them.
- A remote store's resilience layer keeps its timeouts and circuit breaker but no longer retries on top of the client, which retries with the same settings.
- Unlocked compaction on an object store logs a warning, once per store.
- `WriteLease` is documented as recorded-only, and a non-`Local` strategy logs a warning.

**Behaviour and API changes for release notes:**

- `CloudStorageConfig::azure_from_env` returns `anyhow::Result<Self>` instead of panicking.
- `uni_store::cloud::build_store_from_url` is `async` and resolves through `StoreLocation`.
- `CloudStorageConfig::to_url` for Azure is `az://container`.
- `UniBuilder::remote_storage` is deprecated in favour of `hybrid(url).cloud_storage(cfg)`.
- uni's own object-store client now reads the environment, as Lance's already did. A dev shell's `AWS_ENDPOINT_URL` now applies to the catalog and WAL too.
- `UniConfig.object_store.max_retries` now also sets the inner client's retry count (Lance: `client_max_retries`), replacing `object_store`'s default of 10.
- CLI `--path` takes a URI string.
- `cloud_storage` (or Python `cloud_config`) on a plain local path now fails the open. Python used to ignore `cloud_config` outside hybrid mode.
- Python `hybrid(local_path, …)` raises when `local_path` is not the opened path.
- `az://account/container` is deprecated.
- `CloudStorageConfig`'s `Debug` output redacts secrets.
- `uni_db::CloudStorageConfig` is now re-exported.
- On an object store without conditional writes, ID reservation fails instead of overwriting the counter file.
- Without an endpoint, a region lookup failure fails the open. It used to fall back silently to `us-east-1`.
- BACKUP and COPY URLs are configured from the environment only.
- Named AWS profiles (`AWS_PROFILE`) are not read by uni's client, which uses `object_store` 0.13.
- A new remote store keeps its tables under `R/storage/`. Bucket-root stores written before 5.4 open as `flat`. Prefixed stores written before 5.4 are refused until their catalog is copied under the prefix.

**Follow-ups not in Phase A:**

- Phases B–E.
- Fork allocator bootstrap seeds from the primary's cursor, not its reserved batch end (`fork.rs`, review note).
- Fork snapshot managers do not stamp ID marks.
- Fork allocators still allow an unconditional put on remote stores.
- A bulk-load roll-forward during open saves its manifest before the allocator is attached, so it carries the previous ID marks.
- `CLOUD_STORAGE_OPTIONS` in `lance_directory.rs` is keyed by base URI: while two databases are open at one URI with different credentials, datasets opened by URI use the later one's. Entries now go away when their database closes.
- BACKUP and COPY should resolve against the open database's location (Phase E).
- AWS profile support for uni's client needs `aws-config` or an `object_store` upgrade.

## 1. Summary

Object-store support today works in one narrow case: a store at the bucket
root, opened on EC2 with instance credentials, and used by a single writer.
Real deployments break on all three conditions:

- **Off-AWS endpoints and static keys can't be configured for remote-only
  stores (#350).** MinIO, R2, LocalStack and any static-key setup are affected.
- **A store under a URL prefix is unsafe (#349).** Its schema, snapshot
  pointer, manifests, fork registry, WAL and ID allocator are written at the
  bucket root. So two stores in one bucket corrupt each other.
- **Hybrid stores lose data on host loss.** Reopening with a fresh local
  directory reuses VIDs from 0 and overwrites existing vertices. This was
  reproduced.

Both issues come from one missing piece: uni has no single resolved
**store location**.
- The open path builds two object-store clients from different inputs: uni's
  `object_store` 0.13 client for metadata and Lance's 0.14 client for tables.
- It roots them differently: the bucket for uni, the URL prefix for Lance.
- It configures them differently: uni ignores the environment, while Lance
  reads it along with the full AWS credential chain.

This proposal adds that location type and routes every store access through
it. That fixes #349, #350 and the VID reuse. It also lays out how to make
the commit protocol safe on object stores (#348).

## 2. Goals and non-goals

**Goals**

1. Remote-only and hybrid stores open from the environment alone, from a
   typed config, or from raw options. The same precedence applies in Rust,
   Python and the CLI.
2. Every byte a store writes lands under its URL prefix. Two stores in one
   bucket are fully independent.
3. uni's client and Lance's client always see the same endpoint,
   credentials, region and network settings.
4. Existing bucket-root stores open unchanged. Stores written under a prefix
   by ≤ 5.3.0 migrate only when the user opts in, never silently.
5. Losing a hybrid store's local directory never causes VID or EID reuse.
6. CI covers remote-only, prefixed stores, and GCS, Azure and MinIO
   emulators.

**Non-goals for Phase A** (planned in later phases, §8)

- Multi-writer support. The target is *enforced* single-writer (Phase B).
- Atomic flush visibility through Lance version pinning (Phase D).
- A cross-host shared hybrid mode.

## 3. Current architecture

`UniBuilder::build` (`crates/uni/src/api/mod.rs:1990-2100`) picks a mode
with `is_remote_uri = uri.contains("://")` and produces three handles.

| Handle | Local | Hybrid | Remote-only (`s3://b/p`) |
|---|---|---|---|
| `data_store` and `storage.store()`: schema, catalog, manifests, fork registry | `<dir>`, `<dir>/storage` | **bucket root** | **bucket root** |
| `local_store`: WAL, `id_allocator.json` | `<dir>` | `<dir>` | none, falls back to `data_store` (bucket root) |
| `storage_uri`: Lance tables and indexes | `<dir>/storage` | `s3://b/p` | `s3://b/p` |

The two remote branches build `data_store` in one of two ways:
- `build_cloud_store(cfg)` (`crates/uni-store/src/cloud.rs:56`). It starts
  from `AmazonS3Builder::new()` and receives only the bucket.
- `object_store::parse_url(&url)`, whose returned path is discarded
  (`let (os, _path) = …`, `mod.rs:2013, 2056`).

`cloud_config` has a single setter, `remote_storage(url, cfg)`
(`mod.rs:1937`), and that setter also turns on hybrid mode.

## 4. Findings

Evidence comes from three code audits and a live LocalStack run (§10). Items
marked *(inferred)* were derived from the code and not reproduced.

### 4.1 Metadata escapes the URL prefix (#349 and its class)

| Artifact | Path | Escapes in |
|---|---|---|
| schema, Locy rules | `catalog/schema.json`, `catalog/locy_rules.json` | hybrid, remote-only |
| snapshot manifests, `latest`, named snapshots, compaction horizon, restore intent | `catalog/…` (`snapshot/manager.rs:109-312`) | hybrid, remote-only |
| fork registry, fork schemas and tombstones, fork ID allocators, fork WAL | `catalog/fork_*`, `catalog/forks/{id}/…`, `wal_forks/{id}/` | hybrid, remote-only |
| bulk flush intents, label backfill markers, `index_rebuild_state.json` | `catalog/…`, root | hybrid, remote-only |
| primary WAL, `id_allocator.json` | `wal/`, `id_allocator.json` (`mod.rs:2317, 2401, 2418`) | remote-only |

These were reproduced on LocalStack:
- Store `b` saw store `a`'s labels.
- `b` overwrote `catalog/latest`, and its manifest named `a`'s snapshot as
  its parent.
- `b`'s manifest summed vertex counts across both stores.

Inferred from the code: in remote-only mode a second store replays and
truncates the first store's WAL, runs its restore intent, and sweeps its
forks.

Other bugs in the same class:
- **Restore mixes the two roots.** `index_datasets::list` lists `indexes/` at
  the bucket root but opens the datasets under the prefix
  (`snapshot/restore.rs:479-499`).
  - Under a prefix the list comes back empty, so a rewind leaves the indexes
    newer than the data.
  - If another store sits at the bucket root, a rewind deletes that store's
    index objects.
- **`file://` URIs take the remote branch.** `parse_url` roots the result at
  `/`, so the catalog and WAL land in `/catalog` and `/wal`, with no fsync.
- **`StorageManager::build_store_from_uri` drops the path too**
  (`storage/manager.rs:1023-1029`).
- **Trailing slashes produce double slashes.** Index URIs are built as
  `format!("{base}/indexes/…")`, so `s3://b/p/` produces `s3://b/p//indexes`.
- **Percent-encoding differs between parsers.** `build_store_from_url`
  double-encodes the path, while `parse_url` decodes it.
- **Buckets are never cross-checked.** Nothing verifies that the bucket in the
  URL matches `cfg.bucket`.

### 4.2 Configuration and credentials (#350 and its class)

| | uni client | Lance client |
|---|---|---|
| Environment (`AWS_*`, `GOOGLE_*`, `AZURE_*`) | ignored | read |
| Credentials when none are given | IMDS only | profiles, SSO, web identity (IRSA), ECS, IMDS |
| Region when none is given | `us-east-1` | auto-detected |
| Root | bucket | URL prefix |
| Timeouts and retries | outer `ResilientObjectStore` (3×) on top of object_store's default 10 | Lance defaults |

The two clients can therefore talk to different services. For example, Lance
writes to LocalStack while uni contacts IMDS or real AWS.

The #350 repro fails with
`PUT http://169.254.169.254/latest/api/token … after 10 retries`. Every
workaround failed: `AWS_ENDPOINT`, `AWS_ENDPOINT_URL_S3`, URL query options
and an `http://` URL.

**Configuration surface**
- Rust:
  - Remote-only mode can't take a config at all.
  - Hybrid mode can't run without one.
  - `UniConfig.object_store` never reaches `ClientOptions` or Lance.
- Python:
  - `cloud_config` is silently dropped outside hybrid mode
    (`bindings/uni-db/src/core.rs:633`), even though the Black Book documents
    `open("s3://…").cloud_config(…)`.
  - The `local` argument to `hybrid(local, remote)` is ignored.
- CLI: storage can't be configured at all.

**Provider gaps**

- **S3**
  - On uni's side: no `AWS_ENDPOINT_URL[_S3]`, `AWS_ALLOW_HTTP`,
    `AWS_PROFILE`, SSO, or web identity.
  - `s3_from_env` hard-codes `virtual_hosted_style = false`, and Lance
    receives it as an explicit override.
  - No passthrough for `aws_conditional_put`, SSE-KMS, request-payer or proxy
    settings.
- **GCS**
  - No endpoint setting, so fake-gcs-server can't be used.
  - `GOOGLE_APPLICATION_CREDENTIALS` is mapped to `service_account_path`.
    That likely rejects an authorized-user ADC file *(inferred)*.
- **Azure**
  - uni uses `az://account/container` (`cloud.rs:213-233`,
    `config.rs:1050`, docs). object_store and Lance use `az://container/…`
    and `abfss://container@account.dfs.core.windows.net/…`.
  - No support for Azurite, client secrets, managed identity or workload
    identity.
  - `azure_from_env` panics when `AZURE_STORAGE_ACCOUNT` is unset
    (`config.rs:1015-1021`).
- **Schemes are inconsistent.** Four checks disagree: `is_remote_uri`,
  `is_cloud_url`, `parse_url`, and Lance's registry. `s3a://` and `azure://`
  pass uni's checks and then fail in Lance; `gcs://` passes none.
- **Secrets leak.**
  - `CloudStorageConfig` derives `Debug`, which prints keys, tokens, SAS
    strings and service-account JSON.
  - The Python `DatabaseBuilder` embeds that output in its own `Debug`.
  - The process-global `CLOUD_STORAGE_OPTIONS` map keeps secrets after the
    store closes.
- **COPY and BACKUP ignore the database's config.** They call
  `build_store_from_url`, which reads only the environment.
  - `backup_to_*` treats `base_path()` as a local path
    (`uni-query/src/query/executor/read.rs:4603-4670`).
  - From a remote source it copies only `schema.json` and still reports
    "Backup completed".
  - `_snapshot_id` is ignored.

### 4.3 Commit protocol on object stores (#348 and its class)

- **Plain PUTs, so the last writer wins**, for:
  - `catalog/latest` and manifests (`snapshot/manager.rs:131, 211`);
  - the schema (`uni-common/src/core/schema.rs:1578`);
  - named snapshots;
  - the compaction horizon;
  - the fork registry, which only an in-process mutex guards.
- **The manifest is not the commit point.** Flush stamps `lance_version: 0`
  (`runtime/writer.rs:7332-7337, 7513-7518`), a sentinel that readers filter
  out (`storage/manager.rs:2291-2296`). Readers use Lance's table heads, so
  table data becomes visible as soon as Lance commits it.
- **Lance's own table commits are safe**, through
  `ConditionalPutCommitHandler`. The exception is semantic compaction's
  `WriteMode::Overwrite` (`backend/lance.rs:793`), which drops concurrent
  appends.
- **The compaction lock is a no-op remotely**, hybrid included.
  `lock_compaction_dir` returns `Ok(None)` when there is no local root
  (`storage/manager.rs:2689-2692`).
- **ID allocator** (`runtime/id_allocator.rs:284-345`):
  - A `Create` that hits `AlreadyExists` overwrites unconditionally.
  - Errors whose text says "not yet implemented" or "not supported" fall back
    to an unconditional put.
  - It is seeded only from `id_allocator.json`.
  - LocalStack enforces `If-Match`: a stale ETag gets 412, and the allocator
    reports it (reproduced).
- **Recovering a lost `latest` picks an arbitrary manifest.** The code takes
  `list_snapshots().last()` (`mod.rs:2321-2327`), but snapshot IDs are v4
  UUIDs, so "last" has no meaning.
- **`WriteLease` is never used.** It is stored and reported, but `acquire` and
  `heartbeat` are never called.

### 4.4 Hybrid coherence

- **Where artifacts live.** Locally: the primary WAL and
  `id_allocator.json`. Remotely: everything else, including the fork WAL and
  fork allocators.
- **The docs disagree.** "Keep metadata local" does not match the code.
- **Losing the local directory:**
  - Unflushed commits are lost; by default that is up to 5 s or 10k
    mutations.
  - VIDs restart at 0. In the repro,
    `CREATE (:Person {name:'Carol'})` got VID 0 and replaced Alice.
- **Two hosts on one remote** are guaranteed to collide on VIDs.

### 4.5 Tests and docs

- **CI** runs LocalStack S3 only.
  - `cloud_integration_test.rs` (17/17 passing) never opens a `Uni`.
  - `hybrid_localstack_e2e.rs` (3/3 passing) always uses the bucket root.
- **Never tested:** remote-only mode, a prefix, environment-only config, a
  fresh-directory reopen, GCS, Azure, MinIO, and remote compaction, index
  builds, bulk ingest, backup, restore or crash recovery.
- **Docs mislead.**
  - Their examples (`s3://my-bucket/graph-data`) use exactly the untested,
    broken prefix shape.
  - The programming guide says credentials resolve from env or config files.
    That is false for uni's client.

## 5. Design (Phase A)

### 5.1 `StoreLocation`

A new module, `crates/uni-store/src/location.rs`, replaces
`build_cloud_store`, `build_store_from_url`'s use on the open path,
`StorageManager::build_store_from_uri`, and the three branches of
`UniBuilder::build`.

```rust
pub enum LocationKind {
    Local { dir: PathBuf },
    Remote { scheme: Scheme, bucket: String, prefix: object_store::path::Path },
}

pub enum Scheme { S3, Gcs, Azure, Memory }

pub struct StoreLocation {
    kind: LocationKind,
    /// Fully resolved object_store config keys (e.g. "aws_endpoint",
    /// "aws_region", "google_service_account", "azure_storage_account_name").
    /// Values are wrapped so Debug/Display never print secrets.
    options: BTreeMap<String, Secret<String>>,
    /// uni's handle: PrefixStore(bucket_store, prefix); identity for an empty prefix.
    root: Arc<dyn ObjectStore>,
    /// Lance URI: normalized, no trailing slash.
    lance_uri: String,
}

impl StoreLocation {
    pub fn resolve(spec: &LocationSpec, net: &ObjectStoreConfig) -> Result<Self>;
    pub fn root(&self) -> &Arc<dyn ObjectStore>;
    pub fn lance_uri(&self) -> &str;
    pub fn lance_storage_options(&self) -> HashMap<String, String>;
    /// Resolve a foreign URL (COPY / BACKUP target) inheriting this
    /// location's credentials when the provider and bucket match.
    pub fn resolve_sibling(&self, url: &str, extra: &BTreeMap<String, String>) -> Result<Self>;
}
```

`LocationSpec` is what the builder collects: the URI, an optional
`CloudStorageConfig`, and the raw `storage_options`.

**Parsing.** One parser handles every scheme check.
- `file://…` and plain paths are local.
- Accepted schemes are `s3`, `gs`, `az`, `abfss`, `memory`. `s3a` maps to
  `s3`, and `gcs` and `gs` are both accepted.
- Anything else fails at open with `UniError::InvalidUri { uri, supported }`.
- Azure follows object_store's convention: `az://container/prefix` with the
  account taken from config or env, or
  `abfss://container@account.dfs.core.windows.net/prefix`.
- The prefix is `Path::from_url_path(url.path())`, which percent-decodes it.

**Resolution order**, lowest to highest precedence:

1. Provider defaults.
2. The environment, read with object_store's own `from_env` key set. Lance
   uses the same key set, so both clients agree.
3. The URL: bucket or container, account (abfss), and prefix.
4. Fields of the typed `CloudStorageConfig`. A `None` field means "not set",
   so the environment applies, which matches what the current docs promise.
5. Raw `storage_options`.

**Rules**
- Bucket mismatch: if the URL names a bucket and the config names a different
  one, open fails with `UniError::InvalidConfig`.
- `allow_http` is derived from an `http://` endpoint only when it isn't set
  explicitly.
- `UniConfig.object_store` (timeouts, retries, backoff) is lowered to
  object_store keys: `timeout`, `connect_timeout`, and the retry
  `max_retries`/`backoff` settings. Remote stores drop the outer
  `ResilientObjectStore` retry layer, so retries happen in one place.

**Both clients come from one map.**
- uni: build the provider builder, apply each option with `with_config`, and
  wrap the result in `object_store::prefix::PrefixStore` (present in 0.13.2;
  it passes conditional `put_opts` through and strips the prefix from
  listings).
- Lance: receives the same map as `storage_options`, so its own merge from
  the environment changes nothing.
- The per-URI registration in `CLOUD_STORAGE_OPTIONS`
  (`backend/lance_directory.rs:77-101`) is kept but scoped to the database:
  it is removed when the `Uni` is dropped.

**Invariant**, enforced by a test in every mode: `root.list(rel)` returns
exactly the objects under `lance_uri/rel`. This also fixes the restore
listing.

**Wiring**
- `UniBuilder::build` computes one `StoreLocation`.
- In remote-only mode, `data_store`, `storage.store()`, `wal_store` and
  `id_store` are all `location.root()`.
- In hybrid mode, `wal_store` and `id_store` are the local directory.
- The location is stored on `StorageManager`, which COPY, BACKUP and the fork
  code paths then use instead of `build_store_from_url`.
- `is_remote_uri` and `is_cloud_url` are deleted.

### 5.2 Builder API

Rust (`UniBuilder`):

| Method | Meaning |
|---|---|
| `Uni::open(uri)` | Local directory, or remote-only for an object-store URL. The environment is read. **No signature change.** |
| `.cloud_storage(CloudStorageConfig)` | **New.** Typed config, valid in remote-only and hybrid modes. Fixes #350. |
| `.storage_option(k, v)`, `.storage_options(map)` | **New.** Raw object_store keys, passed to both clients. |
| `.hybrid(remote_url)` | **New.** Hybrid mode without a typed config, using the environment. |
| `.remote_storage(url, cfg)` | Kept as sugar for `.hybrid(url).cloud_storage(cfg)`. |

Python (`UniBuilder` and `AsyncUniBuilder`, stubs in `__init__.pyi`), per
the cross-language symmetry rule:
- `cloud_config(dict)` is honored in every mode. Today it is dropped outside
  hybrid.
- `storage_options(dict)` is new.
- `hybrid(local_path, remote_url)` keeps its signature. It fails if
  `local_path` differs from the path passed to `open`, and stops requiring a
  `cloud_config`.

CLI (`crates/uni-cli`):
- `--path` changes from `PathBuf` to `String` so it can take object-store
  URLs.
- `--storage-option k=v` is new and repeatable.
- `--hybrid-remote <url>` is new.
- Because the environment is read, `uni --path s3://bucket/prefix` works with
  standard AWS env vars or a profile.

### 5.3 `CloudStorageConfig` changes

- Add `options: BTreeMap<String, String>` to every variant, with
  `#[serde(default)]`.
- Add `endpoint: Option<String>` to `Gcs`. Add `endpoint`, `use_emulator`,
  `client_id`, `client_secret`, `tenant_id` and `use_managed_identity` to
  `Azure`.
- Give `s3_from_env` / `gcs_from_env` / `azure_from_env` correct semantics:
  they return `Result`, never panic, and set only what the environment
  actually provides. `virtual_hosted_style` becomes `Option<bool>`.
- Replace the derived `Debug` with a manual one that prints `"<redacted>"`
  for keys, tokens, SAS strings and service-account JSON. Do the same for the
  Python `DatabaseBuilder`.

Changing `virtual_hosted_style` to `Option<bool>` and adding fields breaks
code that builds the enum with struct literals. The enum gets
`#[non_exhaustive]` plus constructor helpers (`CloudStorageConfig::s3(bucket)`
with chained setters), and the change is noted in the release notes.

### 5.4 Layout

Remote stores become prefix-relative. Remote-only mode already uses this
layout at the bucket root:

```
<prefix>/catalog/            schema, locy rules, manifests, latest, named snapshots,
                             fork registry, fork schemas/tombstones, forks/{id}/…,
                             bulk intents, label backfill
<prefix>/catalog/layout.json {"version": 2}   (new; written on first open)
<prefix>/wal/                primary WAL           (remote-only)
<prefix>/id_allocator.json                         (remote-only)
<prefix>/wal_forks/{id}/
<prefix>/index_rebuild_state.json
<prefix>/<table>.lance, <prefix>/indexes/…
```

A store at the bucket root has an empty prefix, so its layout doesn't change
by a single byte. Existing bucket-root stores and the current CI tests are
unaffected. `layout.json` marks a store written by a prefix-aware binary;
§5.5 uses it to distinguish legacy stores. The Black Book and
`website/docs/features/storage-cloud.md` document this layout.

### 5.5 Migrating stores written under a prefix by ≤ 5.3.0

Such a store keeps its tables under `<P>/` and its metadata at the bucket
root. Several prefixes may share that root metadata, so adopting it must be
explicit.

The open logic for a store with a non-empty prefix:

1. `<P>/catalog/layout.json` exists → normal open.
2. `<P>/catalog/` has content but no `layout.json` → normal open. This case
   arises only from a partial adoption, which step 3e completes on the next
   adopting open.
3. `<P>/catalog/` is empty, tables exist under `<P>/`, and the bucket root
   holds `catalog/latest` or `catalog/schema.json`:
   - **Read-only, or no opt-in** → fail with `UniError::LegacyLayout`. The
     message names the root files and tells the user to call
     `.adopt_legacy_root_metadata()` or run `uni migrate-layout --path …`.
   - **Opt-in** →
     1. Check that every label and edge type in the root manifest has a table
        under `<P>/`. Refuse if not.
     2. Claim the root with `PutMode::Create` on
        `catalog/.legacy_owner = {"prefix": P}`. Refuse if the file already
        names a different prefix.
     3. Copy (not move) `catalog/**`, `wal/**`, `wal_forks/**`,
        `id_allocator.json` and `index_rebuild_state.json` into `<P>/`.
     4. Run the usual allocator seeding (§5.6), which guards against a stale
        copied allocator.
     5. Write `<P>/catalog/layout.json` last; it is the commit point.
4. Otherwise, it is a fresh store: write `layout.json` and never read the
   root.

The root is left in place. `uni migrate-layout --cleanup-root` deletes it,
and refuses while `.legacy_owner` exists and the named prefix lacks
`layout.json`.

**The schema fallback.** The existing `schema.json` → `catalog/schema.json`
fallback (`mod.rs:2102-2130`) stays scoped to `location.root()`, so it
never reaches the bucket-root copy for a prefixed store. It is also skipped
for read-only opens, which today perform a write.

### 5.6 Allocator seeding (host-loss VID reuse)

- Add `next_vid: u64` and `next_eid: u64` to `SnapshotManifest`
  (`#[serde(default)]`). Flush stamps them from the allocator's reserved
  high-water mark.
- At open, `IdAllocator::new` takes a floor and starts at
  `max(id_allocator.json, manifest.next_vid/next_eid)`.
- Manifests from older versions lack these fields. For them the floor comes
  from a scan: `max(_vid) + 1` over every vertex table and `max(_eid) + 1`
  over every edge table: one aggregate query per table, run only for
  manifests that lack the fields. Whether Lance can answer it from fragment
  statistics, without a column scan, is not yet checked.
- Whenever the floor moves the allocator forward, a `warn!` reports it.
- The same seeding applies to fork allocators, which already start from the
  primary's HWM at fork creation.

This holds in every mode. In local mode it also guards against a deleted or
stale `id_allocator.json`.

### 5.7 Smaller fixes in Phase A

- Normalize `lance_uri` (strip a trailing `/`) and build all index URIs
  through one `join_uri` helper.
- When `latest` is lost, recover from the manifest with the greatest
  `(version_high_water_mark, created_at)`, not `list_snapshots().last()`.
- Remove the silent fallbacks in the ID allocator on remote stores:
  - `AlreadyExists` on create → reload and retry;
  - "not supported" → error. A local store keeps the plain put.
- Make `CompactionDirLock`'s remote no-op explicit with a `warn!` until
  Phase B replaces it with the lease.
- Mark `WriteLease::DynamoDB`/`Custom` `#[doc(hidden)]` with an
  "unimplemented" note until Phase B.

## 6. Compatibility

| Case | Effect |
|---|---|
| Local stores | None. |
| Remote store at the bucket root | Layout unchanged; `layout.json` added on first open. |
| Remote store under a prefix, written by ≤ 5.3.0 | Fails at open with a clear error until the user opts in (§5.5). This is deliberate: silently adopting shared root metadata could graft another store's schema and snapshots onto this one. |
| Older binary opening a store written after the change | A bucket-root store opens fine. For a prefixed store, the older binary reads the root as before (and is unsafe as before). Release notes recommend against mixed versions. |
| `CloudStorageConfig` struct literals | Break (§5.3). Constructors are provided. |
| Azure `az://account/container` URLs | Accepted for one release with a deprecation `warn!`, rewritten to the object_store form; removed after that. |
| Environment newly honored by uni's client | Settings that only Lance used to honor now apply to metadata too. Real behavior changes only for setups that were already split-brained. |

## 7. Alternatives considered

1. **Hard-code prefix joins** (`format!("{prefix}/catalog/schema.json")`) at
   each call site. Rejected: there are dozens of bucket-relative paths
   across `uni`, `uni-store`, `uni-bulk` and `uni-common`, and the next new
   artifact would regress. `PrefixStore` makes the prefix
   impossible to forget.
2. **Switch uni to object_store 0.14 and reuse Lance's client**
   (`lance-io` `ObjectStore::inner`). This gives a single credential chain
   and retry policy by construction, and is the best long-term state. It is
   deferred because it couples uni's dependency upgrades to Lance's, and the
   0.13 → 0.14 bump touches every crate. Phase A's shared option map gets
   the same behavior now; this alternative is a follow-up.
3. **Auto-migrate legacy root metadata on open.** Rejected: the root may be
   shared by several prefixes, and there is no safe way to tell which store
   owns it.
4. **Keep remote-only env-blind and require `cloud_storage(cfg)`.**
   Rejected: it breaks the standard deployment patterns (IRSA, ECS task
   roles, profiles, `AWS_ENDPOINT_URL`) that Lance's side already supports.

## 8. Later phases

| Phase | Content | Fixes | Risk |
|---|---|---|---|
| **A** | §5 | #349, #350, host-loss VID reuse | Low for bucket-root stores |
| **B** | Single writer: a lease at `catalog/writer_lease.json {owner, epoch, expires_at}` acquired with `PutMode::Create`, taken over with `Update(etag)` after expiry, and renewed in the background (a failed renewal stops writes). An OS lock on the directory for local stores. CAS (`Update(etag)`) on `latest`, the schema, named snapshots and the fork registry, where `Precondition` makes the writer step down. Compaction runs under the lease. Fail at open if the provider lacks conditional puts, unless the user opts out explicitly. Wire up or remove `WriteLeaseProvider`. | #348 | Medium |
| **C** | Hybrid semantics. Recommendation: hybrid means "local WAL for low-latency durable commits; everything else remote". Document it as single-host, with unflushed commits living on that host; the Phase B lease prevents a second host. Fix the docs. | doc drift | Low |
| **D** | Make the manifest the commit point: record real Lance versions in `lance_version`, have readers check out those versions, and remove the `0` sentinel. This is the only way to fence out a stale writer's table appends, and it also makes flush visibility atomic. Gets its own proposal. | fencing, atomic flush | High |
| **E** | COPY and BACKUP through `StoreLocation`: snapshot-consistent (pinned to the named manifest), correct source paths, streaming multipart, and a non-zero exit when nothing was copied. | backup correctness | Medium |

## 9. Test plan

Per `docs/test_layout.md`, new tests are modules in each crate's existing
`tests/integration.rs`. The CI cloud job gains MinIO, Azurite and
fake-gcs-server service containers next to LocalStack.

Phase A regression tests:
- remote-only open from the environment alone; with `cloud_storage(cfg)`;
  with raw `storage_options` (#350);
- two stores at `s3://b/a` and `s3://b/b` with separate schemas, data, WAL,
  allocators and forks, where a full bucket listing finds nothing outside
  the two prefixes (#349);
- `root.list(rel)` ⇔ `lance_uri/rel` in local, hybrid and remote-only modes;
- a prefix with and without a trailing slash, a percent-encoded prefix, and a
  `file://` URI;
- a bucket mismatch between URL and config fails;
- a legacy-root store: a plain open fails, the opt-in adoption succeeds, a
  second prefix's adoption is refused, and a partial adoption resumes;
- hybrid reopen with a fresh local directory, then a new vertex: no existing
  vertex is shadowed. This uses both a `next_vid` manifest and an old one
  that forces the scan;
- restore and rewind under a prefix pin and rewind the indexes;
- `Debug` of `CloudStorageConfig` and the Python builder contains no secret;
- Python: `open("s3://…").cloud_config({...})` opens remote-only;
- GCS (fake-gcs-server), Azure (Azurite), MinIO smoke tests: open, write,
  flush, reopen, fork, promote.

Phase B adds: a second writable open fails; a stale lease holder's publish is
rejected; compaction under contention loses no appends.

## 10. Evidence: live reproduction

The run used LocalStack 4.13.1 against main at `ed1048e0` (5.3.0).

| Claim | Result |
|---|---|
| #350: remote-only open with `AWS_ENDPOINT_URL`, keys, region, `AWS_ALLOW_HTTP` | Fails with `PUT http://169.254.169.254/latest/api/token … after 10 retries`; 0 keys written |
| #350 workarounds (`AWS_ENDPOINT`, `AWS_ENDPOINT_URL_S3`, URL query options, `http://` URL) | None work. Passing `cloud_config` with an `s3://` primary URI is rejected with "Hybrid mode requires a local path". |
| #349: store `b` sees `a`'s labels; a schema change to one rewrites the other | Confirmed |
| Hybrid `s3://repro/a`: keys outside `a/` | `catalog/latest`, `catalog/manifests/<uuid>.json`, `catalog/schema.json`. All 73 table and index keys were under `a/`. |
| A second hybrid store `s3://repro/b` in the same bucket | Overwrote `catalog/latest`. Its manifest's `parent_snapshot` is `a`'s, `Person.count` sums both stores, and its WAL LSNs continue from `a`'s high-water mark. |
| Conditional puts on LocalStack | Enforced: an `If-Match` with a stale ETag gets 412, and the allocator reports it |
| Hybrid reopen with a fresh local directory | Data reads back, but the allocator restarts at VID 0. `CREATE (:Person {name:'Carol'})` got VID 0 and **replaced Alice**. Reproduced for two prefixes. |
| Existing CI tests | `cloud_integration_test` 17/17 and `hybrid_localstack_e2e` 3/3 pass. They use the bucket root and never reopen with a fresh directory, so they cover none of the above. |

The repro crate and logs are in the session scratchpad, not the repo.

## 11. Open questions

1. **Hybrid (Phase C):** keep it with the semantics above, or deprecate it in
   favor of remote-only with an optional local WAL?
2. **Fencing:** is the Phase B lease plus CAS on `latest` enough until Phase D,
   given that a stale writer's Lance appends stay visible?
3. **Azure URLs:** a one-release deprecation window for
   `az://account/container`, or a hard break?
4. **Client unification:** do the object_store 0.14 upgrade and the shared
   Lance client (Alternative 2) right after Phase A, or wait for the next
   Lance bump?
