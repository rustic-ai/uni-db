# Correctness Scan — Deferred Findings

**Date:** 2026-07-06
**Context:** deferrals from `docs/correctness_scan_2026-07-05.md` (see the triage in
`docs/correctness_scan_triage_2026-07-05.md`). This doc captures the findings that were
consciously *not* fixed in each remediation wave, with enough analysis to resume without
re-investigating.

**Wave 0** (P0 stop-the-bleeding): 18 of 22 findings fixed and committed to `main`
(16 commits, `6ec2c9963..b659ca0fa`, FF-merged, **not pushed**). Deferrals **D1–D4** below.
**Update (2026-07-06):** **D2** (`uni[6]`) and **D4** (`uni-query[29]`) since fixed on branch
`fix/correctness-deferred-d2-d4` (D4 `c393dfb87`, D2 `c9e80cff2`). **D1 (`uni[2]`, perf) since
fixed on `main` (`01fb4ca16`); D3 (`uni-fork[3]`) since fixed (uncommitted) via a promote-local
content re-verify — no `UidIndex` design change was needed.** All Wave-0 findings are now resolved.

**Wave 1** (shared-helper clusters R7/R6/R4/R5/R10/R16/R17/R11): all 8 regions fixed on
branch `fix/correctness-scan-wave1` (17 commits, base `88f0c52cd`, **not pushed**; see memory
`correctness_scan_wave1_fixed_2026_07_06`). Two sub-findings deferred as design changes rather
than mechanical fixes: **D5** (`uni-query-functions[2]`, sort-key int precision) and **D6**
(part of R5, bulk UNIQUE vs. the main Writer's unflushed L0). Sections below.

Each entry gives: the exact code locations, the root cause, what was tried (if anything),
**why it was deferred**, the **concrete fix plan**, the **existing repro** to flip/un-ignore,
and the **verification** commands. Repro files already exist in-tree (untracked) unless noted.

Shared build/test conventions (from the fixed work):
- Rust: `RUSTC_WRAPPER="" cargo nextest run -p <crate> ...` (nextest, not `cargo test`).
- uni-store repros are gated behind the default `lance-backend` feature and aggregated via
  `crates/uni-store/tests/common/bugs/mod.rs` → `integration.rs`.
- uni-db (crate `uni-python` / test crate `uni-db`) integration tests live under
  `crates/uni/tests/common/bugs/` aggregated via `crates/uni/tests/integration.rs`.
- A repro is only "done" when its assertion is flipped from capturing the bug to asserting
  the fix, and the full crate suite stays green.

---

## D1 — `uni[2]` fork-local index kind collision (perf; NOT data-loss) — ✅ DONE (`01fb4ca16`)

**Severity:** perf/planner defect — retrieval stays correct (falls back to a plain scan);
only the fused-index-scan *plan* is missed. This is why it was safe to defer.

**Status:** fixed per the plan below. `ForkScope.fork_local_indexes` now holds a
`HashSet<ForkLocalIndexKind>` per `(label, column)`; `register_fork_local_index` inserts into
the set; `fork_local_index` → `has_fork_local_index(label, column, kind)`; `manager.rs`
exposes `has_fork_index`; and every planner fusion site (`ForkIndexLookup::fork_index_has` /
`_label_id`) probes for the exact kind it emits (equality Scan → VidUid then ScalarBtree;
VectorKnn → Vector; InvertedIndexLookup → FullText; Sort → Sorted; procedure call → expected).
The `fork_maintenance.rs` skip-check probes the specific kind, ending the rebuild ping-pong.
Repro flipped to `two_fork_index_kinds_on_one_column_coexist` (BtreeUnion fusion survives a
FullText build on the same column), wired into `mod.rs`; all 27 `fork_index`/repro tests green.

### Locations
- **Storage (root):** `crates/uni-store/src/fork/scope.rs:125`
  `fork_local_indexes: Arc<DashMap<(String, String), ForkLocalIndexKind>>` — one kind per
  `(label, column)`. `register_fork_local_index` (`scope.rs:205`) does `.insert(...)`, which
  **overwrites**; `fork_local_index` accessor (`scope.rs:215`) returns `Option<one kind>`;
  `all_fork_local_indexes` (`scope.rs:223`).
- `ForkLocalIndexKind` enum: `scope.rs:46` — `#[derive(Clone, Copy, Debug, Eq, PartialEq,
  Hash)]`, variants `ScalarBtree, Sorted, VidUid, Vector, FullText, Sparse`, `#[non_exhaustive]`.
  (Already `Eq + Hash` → usable in a `HashSet` with no derive change.)
- **Maintenance skip check:** `crates/uni/src/api/fork_maintenance.rs:194`
  `if scope.fork_local_index(label, column) == Some(*kind) { continue; }` — the comment at
  `:178` already acknowledges a column can carry multiple kinds, but the map can't represent it.
- **Storage accessor:** `crates/uni-store/src/storage/manager.rs:607` `fork_index_exists`
  → `s.fork_local_index(label, column)` (`:614`).
- **Planner consumers** (`crates/uni-query/src/query/planner.rs`), all read "the one stored
  kind" then map via `into_fusion_kind` (`:10438`, `kind → FusionKind`):
  - `:10185` equality-scan fusion (`fork_index_for(&labels[0], &col)` → `into_fusion_kind`) —
    **the site the repro exercises** (`{email: 'x'}` equality → `BtreeUnion`).
  - `:10248` `VectorKnn` (wants `Vector` → `AnnRerank`).
  - `:10279` `InvertedIndexLookup` (wants `FullText` → `Bm25Rrf`).
  - `:10342` Sorted / ORDER-BY (`Some(ForkLocalIndexKind::Sorted)` → `SortedKWayMerge`).
  - `:10428` `procedure_call_fusion_kind` — `let registered = fork_index_for(...)?;` compares
    `registered == expected`.
  - Helper defs: `fork_index_for` / `fork_index_for_label_id` at `:9891`/`:9899` both delegate
    to `fork_index_exists`.

### Root cause
The map holds a single kind per column, so building a second kind (e.g. `FullText` on a column
that already has a `ScalarBtree`) **clobbers** the first. Each planner site then reads whatever
one kind survived and blindly maps it with `into_fusion_kind` — so the fusion for the *other*
kind is lost, and (worse in principle) a site can wrap a scan with a fusion kind that doesn't
match the scan shape.

### Why deferred
The correct fix isn't just "store a set" — each planner read-site must ask for the **specific
kind that its scan shape can fuse** (equality scan → `VidUid`/`ScalarBtree`; `VectorKnn` →
`Vector`; `InvertedIndexLookup` → `FullText`; ORDER BY → `Sorted`). The current code is loose
(maps "the one kind" generically), so naively returning a set and picking "any" risks a
**subtle mis-fusion** — exactly the kind of planner regression the scan is trying to prevent.
Needs careful per-site reasoning + plan-shape tests.

### Fix plan
1. `scope.rs`: change the value type to `HashSet<ForkLocalIndexKind>` (enum is already
   `Eq + Hash`). `register_fork_local_index` → `entry(key).or_default().insert(kind)`. Update
   `all_fork_local_indexes` to flatten. Add `has_fork_local_index(label, column, kind) -> bool`.
2. `manager.rs`: add `fork_index_has_kind(label, column, kind) -> bool` (delegates to scope);
   keep or drop `fork_index_exists` depending on remaining callers.
3. `fork_maintenance.rs:194`: `if scope.has_fork_local_index(label, column, *kind) { continue; }`.
4. **Planner** — make each site kind-specific:
   - `:10185` equality scan: check for `VidUid` first, else `ScalarBtree` (mirror the current
     precedence implied by `into_fusion_kind`), producing `VidUidForkFirst` / `BtreeUnion`.
   - `:10248` `VectorKnn`: `has_kind(..., Vector)` → `AnnRerank`.
   - `:10279` `InvertedIndexLookup`: `has_kind(..., FullText)` → `Bm25Rrf`.
   - `:10342` Sorted: `has_kind(..., Sorted)` → `SortedKWayMerge`.
   - `:10428` `procedure_call_fusion_kind`: `has_kind(..., expected)` instead of
     `registered == expected`.
   Keep `into_fusion_kind` as the kind→FusionKind mapper for whichever kind each site selects.

### Repro (flip)
`crates/uni/tests/common/bugs/repro_fork_index_kind_collision.rs` →
`two_fork_index_kinds_on_one_column_cannot_coexist` (currently asserts `!plan.contains("BtreeUnion")`
after building `FullText` over an existing `ScalarBtree`). **After fix:** the ScalarBtree
equality fusion must survive alongside FullText → assert the plan **still** contains
`FusedIndexScan` + `BtreeUnion`. Register in `crates/uni/tests/common/bugs/mod.rs`.

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-db -E 'test(two_fork_index_kinds_on_one_column_cannot_coexist)'`
then full `-p uni-query` and `-p uni-db` suites (planner regressions surface there).

---

## D2 — `uni[6]` nested-fork tip capture race (fork-creation concurrency) — ✅ DONE (`c9e80cff2`)

**Severity:** snapshot-isolation violation at fork creation — a nested fork can branch off a
**post-fork-point** version of its parent. Distinct from issue #103 (that was a read-path
scalar-index bug on an existing fork; confirmed different code path/failure mode).

### Locations
- `crates/uni/src/api/fork.rs:449-471` — `build_datasets_for_fork`, nested-fork arm. Line
  **452**: `current_version_on_branch(&dataset_uri, &parent_branch)` reads the parent **branch**
  tip **live**, then `create_branch_from(..., parent_v)`.
- `crates/uni-store/src/runtime/writer.rs:4153-4190` — `flush_and_capture_fork_point`. The
  `flush_lock` is acquired at `:4160` and dropped when this fn returns. It captures
  `current_version(&uri)` per dataset at `:4180` — that is **main's** tip, NOT the parent
  branch's tip.
- Primary-parent arm (contrast, already correct): `fork.rs:491-513` branches at
  `captured_versions.get(&dataset_name)` (captured under the lock; the M1 fix). The live
  re-read at `:497-506` is a should-never-fire fallback.

### Root cause
The nested arm has **no captured value to use**, because `flush_and_capture_fork_point` only
snapshots main's `current_version`, never the parent *branch's* tip (branch tips advance
independently of main — see `lance_branch.rs:103-116`). So the nested path is forced to read the
parent-branch tip **live and unserialized, after `flush_lock` dropped**. A concurrent commit+flush
on the parent fork between capture and line 452 advances the parent branch tip; the child then
`create_branch_from(..., parent_v)` at that advanced version → sees parent writes committed after
the fork point.

### Why deferred
Not a one-line "move the read inside the lock" — the value the nested arm needs (the parent
*branch* tip) is **never captured under the lock today**. The fix extends the capture path and
threads a new per-dataset map into the nested arm — a change to the fork-creation concurrency
path, which is race-sensitive and hard to test deterministically (see repro note).

### Fix plan
Capture the parent-branch tip **under `flush_lock`** and thread it into the nested arm:
- Option A (preferred): extend `flush_and_capture_fork_point` (or the fork setup at
  `fork.rs:271-279`) to also record `current_version_on_branch(uri, parent_branch)` per dataset
  while the guard is held (requires the parent branch names at capture time), then have the
  nested arm branch at that captured version — mirroring the primary path's capture-under-lock.
- Option B: hold/re-acquire `flush_lock` across `build_datasets_for_fork` for the nested case so
  the `:452` read is serialized against parent commit/flush.

### Repro
`crates/uni/tests/common/bugs/repro_nested_fork_capture_race.rs` →
`nested_fork_reads_parent_live_tip_under_concurrency` (`#[ignore]`, non-deterministic stress; its
module doc names `fork.rs:452`). Deterministic firing needs an injected suspension point between
capture and branch in production code. Aim: make it a deterministic guarding regression (test-only
await/hook between capture and branch, or assert the captured-tip path is taken); if full
determinism is impractical, keep the stress loop but assert **no live read** occurs (captured
version used).

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-db -E 'test(nested_fork)'` plus the fork suites
(`test(fork)`); run repeatedly under load if the test stays stress-based.

---

## D3 — `uni-fork[3]` promote content-UID mismatch — ✅ DONE

**Severity:** non-idempotent promote (unbounded twins on re-promote for ext_id-bearing rows).

**Status:** fixed via a **promote-local content re-verify** (uncommitted), sidestepping the
`UidIndex`-staleness coupling that forced the earlier revert (`b659ca0fa`) — the engine-wide
append-only `UidIndex` is left untouched. Two coordinated changes, both in
`crates/uni-fork/src/diff.rs`:
1. **New helper** `content_uid_with_ext_id(label, ext_id, stripped_props)` re-injects the
   `"ext_id"` key into the Cypher-stripped props before `compute_vertex_uid`, reproducing the
   registered UID's **double-fold** (`ext_id` folded once via the arg + once via the property key)
   exactly. `run_promote`'s candidate loop now uses it, so the content-UID dedup finally fires for
   ext_id-bearing rows.
2. **`batch_resolve_primary_vids` now verifies LIVE content, not just presence.** It takes a
   `primary_ext_ids: &HashMap<Vid, String>`, changes its verify Cypher from `RETURN id(n)` to
   `RETURN n`, recomputes each resolved vid's **current** content-UID (via the same helper), and
   counts a UID as on-primary only when a resolved vid *still hashes to that exact UID*. This
   rejects the stale pre-edit UID that the append-only index keeps — so a fork vertex whose content
   diverged from a primary edit is correctly inserted as a **twin** (the insert-only-twin contract
   holds by construction). This extends the re-verification `UidIndex`'s own doc-comment already
   mandates (consumers re-verify resolved vids against live storage) from *liveness* to *content*.

Tests: the pinning unit test flipped to `promote_uid_matches_registered_uid_via_ext_reinjection`
(`assert_eq!` — re-injection reproduces the registered UID); 3 inline `diff::tests` unit tests cover
the helper directly; a new end-to-end `promote_default_is_idempotent_for_unchanged_ext_id_row`
(`crates/uni/tests/common/fork/fork_promote.rs`) promotes an unchanged fork **twice** and asserts one
Alice / zero inserts; the canary `promote_default_is_insert_only_twin` stays green (still twins on
divergence). Verified: `uni-fork` 12/12, `uni-db test(promote)` 18/18, full `uni-db test(fork)`
175/175. **Scope note:** the edge path's *endpoint* content-UIDs remain single-fold (ext_id-bearing
edge endpoints were already unresolved before this change — a separate latent issue, no regression;
passing `primary_ext_ids` there is non-regressing since non-ext_id endpoints resolve identically).

### Original analysis (retained for context)

A fix was committed then **reverted** — commit `b659ca0fa` — because it exposed a
deeper issue. The finding's own NOTE comment (now removed) captured the mismatching behavior.

### Locations
- `crates/uni-fork/src/diff.rs:643` — `run_promote` recomputes the content-UID from
  `node.properties` (a Cypher query result, which **strips** the `ext_id` key).
- `crates/uni-store/src/runtime/writer.rs:5182` — the **registered** UID is hashed from the
  **stored** props, which **still contain** the `ext_id` key.
- `crates/uni-store/src/storage/vertex.rs:47` — `compute_vertex_uid(label, ext_id, properties)`
  folds `ext_id` in **twice**: once as the dedicated arg AND once via every property key
  (including `ext_id`). Changing this hasher would invalidate every persisted `UidIndex` entry
  (rejected).
- Consuming path: `diff.rs:660-685` — ext_id-bearing candidates resolve via
  `batch_resolve_primary_by_ext_id` (ext-id keyed); non-resolved ones fall to the content-UID
  `uid_candidates` path resolved against the primary `UidIndex`.
- **Conflicting contract / regression:** `crates/uni/tests/common/fork/fork_promote.rs:316` —
  `promote_default_is_insert_only_twin`. It creates Alice (ext_id `p1`, age 30), forks, then the
  **primary** edits age→99, then `promote_from_fork` under default (insert-only) options; asserts
  `vertices_inserted == 1` and primary ends with `ages == [30, 99]` (twin + edited).

### Root cause & what was tried
The tried fix re-injected the `ext_id` key into `node.properties` before `compute_vertex_uid`, so
the promote-side UID matched the registered UID and the dedup could fire. **Correct in isolation.**
But it regressed `promote_default_is_insert_only_twin`: the fork's Alice (age 30) then matched a
**stale** age-30 UID in the primary's `UidIndex` (the primary was edited to age 99, but the index
apparently still carried the pre-edit UID), so the promote wrongly **skipped** the insert instead
of twinning the divergent fork vertex.

So there are really **two coupled issues**:
1. The promote-side vs registered content-UID mismatch (the finding), and
2. **`UidIndex` staleness on property update** — the index does not appear to drop/replace a
   vertex's content-UID when its properties change (age 30 → 99), so a content-UID dedup can match
   a version of the vertex that no longer exists.

Fixing only (1) surfaces (2) and breaks a deliberate contract.

### Why deferred
The finding cannot be fixed safely until the `UidIndex`-on-update semantics are decided:
- If content-UID dedup is meant to reflect **current** content, then `UidIndex` must be updated
  (remove old UID, add new) whenever properties change — then re-applying the `ext_id` re-injection
  is correct and the insert-only-twin test still passes (fork age-30 ≠ primary age-99 → no dedup →
  twin).
- If default promote is meant to be strictly insert-only (twin regardless), then the finding's
  "dedup should fire" premise is wrong for the default path and only applies to the
  no-content-change re-promote case (idempotency), which needs a narrower guard.

### Fix plan (pick one, after deciding semantics)
- **Preferred:** (a) make the writer's UID registration update-aware (drop the old content-UID and
  register the new one on property update), then (b) re-apply the `ext_id` re-injection at
  `diff.rs:643`. Verify `promote_default_is_insert_only_twin` still twins (fork age-30 ≠ primary
  age-99) AND the no-change re-promote is idempotent (no twin).
- **Narrower alternative:** leave `diff.rs:643` as-is and add an idempotency guard keyed on the
  ext_id + fork-point baseline so a *no-content-change* re-promote skips, without touching the
  content-UID path (avoids the staleness interaction).

### Repro
`crates/uni-fork/tests/promote_diff_bugs.rs` →
`finding2_promote_uid_can_never_match_registered_uid` (currently `assert_ne!`, pinning the
mismatch). The reverted version asserted `assert_eq!` after re-injection. Add an **end-to-end**
idempotency test (CREATE ext_id vertex, flush, fork with NO changes, promote twice, assert exactly
one primary vertex) and keep `promote_default_is_insert_only_twin` green.

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-fork --test promote_diff_bugs` **and**
`cargo nextest run -p uni-db -E 'test(promote_default_is_insert_only_twin)'` (the regression
canary) plus the full `-p uni-db` fork suite.

---

## D4 — `uni-query[29]` L0 label overlay union-only — ✅ DONE (`c393dfb87`)

**Severity:** wrong query results — a `REMOVE n:Label` on a flushed vertex is invisible; the
removed label resurrects in `labels(n)` and in `MATCH (n:RemovedLabel)`.

### Locations
- `crates/uni-query/src/query/df_graph/scan.rs:2803-2813` — `map_to_output_schema` label-column
  build (schemaless `RETURN n` path). Union-only overlay; never consults
  `vertex_label_overwrites`.
- `crates/uni-query/src/query/df_graph/scan.rs:1817-1827` — `build_labels_column_for_known_label`
  (known-label scan). Same union-only overlay, plus a defensive "ensure scanned label present"
  push at `:1812-1815`.
- Buffer iteration order: `crates/uni-query/src/query/df_graph/mod.rs:269` `iter_l0_buffers` yields
  **oldest → newest** (pending-flush oldest first, then current, then transaction L0).
- Marker semantics (reference): `crates/uni-store/src/runtime/l0.rs:372` live `set_vertex_labels`
  sets `vertex_label_overwrites`; the M8 flush pass at `l0.rs:1408` treats the marker as REPLACE.

### What was tried (and validated — then reverted)
A label-COLUMN fix at both sites: walk buffers oldest→newest; if a buffer has the vid in
`vertex_label_overwrites`, its `vertex_labels[vid]` is the resolved full set and **REPLACES** the
stored labels (newest overwrite wins); otherwise union additive labels. This **worked** — after
`CREATE (n:A:B)`, flush, `REMOVE n:B`, `labels(n)` correctly returned `['A']`.

**But** `MATCH (n:B)` still returned the node. Reverted to avoid a confusing partial state
(`labels(n)` says `[A]` while `MATCH (n:B)` still matches).

### Root cause of the remaining half
`MATCH (n:B)` resolves its **candidate set** through the label-scan / label-index path, **not**
through the label-column builder that was fixed. After `REMOVE n:B` the L0 reverse index
(`label_to_vids`) no longer lists the vid under `B`, but the **flushed Lance row** still carries
`B`, so the label-B scan re-includes the vid. The scan must consult the L0 overwrite marker to
**exclude** a vid whose current (overwrite-resolved) label set no longer contains the scanned
label.

### Why deferred
The candidate-filtering change lives in the schemaless label-scan candidate resolution (core scan
path), separate from the two label-column sites. Needs to find where label-B candidates are
gathered from Lance + L0 and apply an L0-overwrite exclusion — a riskier, less-localized change
than the label-column overlay.

### Fix plan
1. Re-apply the **label-column** overlay fix at `scan.rs:2803` and `:1817` (REPLACE on
   `vertex_label_overwrites`, honoring newest-buffer-wins; also drop labels for tombstoned vids).
   This was validated to fix `labels(n)`.
2. Add **candidate filtering** in the label-scan path: when gathering candidates for a known label
   `L`, for any vid whose newest L0 buffer flags it in `vertex_label_overwrites`, include it only
   if that buffer's resolved `vertex_labels[vid]` contains `L` (and exclude tombstoned vids). Find
   the schemaless known-label candidate resolution in `scan.rs` (the path feeding `MATCH (n:L)`)
   and apply there.

### Repro (un-ignore)
`crates/uni-query/tests/correctness_repros.rs` → `repro_10_label_remove_resurrect`. Currently
`#[ignore = "uni-query[29]: label-scan candidate filtering for REMOVE label pending"]` with the
**target assertions already written**: `labels(n) == ['A']`, `A` present, and `MATCH (n:B)` empty.
Remove the `#[ignore]` when both halves land.

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-query --test correctness_repros -E 'test(repro_10_label_remove_resurrect)'`
then full `-p uni-query` (scan-path regressions) and a spot-check of the schemaless MATCH/label
tests.

---

# Wave 1 deferrals

## D5 — `uni-query-functions[2]` ORDER BY sort-key collapses large i64 (precision) — ✅ DONE

**Severity:** wrong ORDER BY / sort results — two distinct i64 values above 2^53 produce a
**byte-identical** sort key, so they collapse to an arbitrary relative order (and can tie
where they must not). Localized to the DataFusion sort-key encoder; no data loss.

**Status:** fixed (uncommitted). The sort key is transient (per-query `LargeBinary`, never
persisted → no migration), but it is *also* the join-key unifier, so the fix had to keep
`Int(n)`/`Float(n.0)` **byte-identical** (Cypher `n = n.0`). Approach — **f64 bucket + exact i64
tie-break**: the rank-`0x08` payload is now `encode_order_preserving_f64(primary)` ++
`encode_order_preserving_i64(int_delta)` for **both** Int and Float (`encode_numeric_payload`,
`df_udfs.rs`), where Int's `int_delta = i - (i as f64)` and Float's is `0`. Round-to-nearest is
monotonic, so different buckets order by `primary`; within a shared bucket the delta orders by true
value — exact across the full i64 range, still interleaving with floats, and any exactly-representable
integer (delta 0) stays byte-identical to its float (joins keep matching; the constant-0 tie-break on
Float prevents a prefix mismatch).

The same `i64 as f64` cast was **mirrored in 4 `compare_values` copies** (cross-type Int/Float arm
only; Int/Int was already exact). All now route through a shared exact `pub fn cmp_i64_f64` in
`uni-common` (`value.rs`, re-exported at crate root), preserving each site's NaN handling:
`crates/uni-query/src/query/executor/core.rs` (fallback ORDER BY/window/list/map),
`crates/uni-store/src/runtime/writer.rs` (single-writer CHECK), `crates/uni-bulk/src/bulk.rs` (bulk
CHECK), `crates/uni-query/src/query/df_graph/apply.rs` (correlated-subquery pushdown filter).

Repros were written **first** to pin each bug, then flipped: encoder
`repro_finding_13_sort_key_int_collapse` (`assert_ne!` + ordering + Int/Float equality); inline unit
repros in `core.rs` and `apply.rs`; e2e `repro_check_constraint_int_float_precision_collapse`
(writer CHECK) and `bug_bulk_check_large_int_repro.rs` (bulk CHECK, + control); plus a new e2e
`test_order_by_large_integers_above_2p53`. Verified: `uni-common` (`cmp_i64_f64` units + doctest),
`uni-query-functions`/`uni-query` full suites, `uni-store`/`uni-bulk` constraint tests, and
`uni-db` (174 bulk/subquery/pushdown regressions + the flipped repros); clippy `-D warnings` clean.

**Remaining follow-up (separate D10-family bug, NOT fixed here):** writer.rs (`:2450-2451`) and
apply.rs `Eq`/`NotEq` (`:338-339`) still compare `=`/`!=` via strict `Value` `PartialEq` (no
Int↔Float coercion), so cross-type numeric *equality* stays wrong there — the D10 defect (fixed only
for bulk), a different root than this precision cast.

### Original analysis (retained for context)

### Locations
- `crates/uni-query-functions/src/df_udfs.rs:2964-2965` — `encode_sort_key_to_buf`, the
  `Value::Int(i)` arm: `let f = *i as f64; buf.extend_from_slice(&encode_order_preserving_f64(f))`.
  The `*i as f64` cast rounds any `|i| > 2^53` before encoding.
- `crates/uni-query-functions/src/df_udfs.rs:2967-2968` — the `Value::Float(f)` arm encodes
  via the **same** `encode_order_preserving_f64(*f)`.
- `crates/uni-query-functions/src/df_udfs.rs:3007` — `sort_key_type_rank`: **both** `Int` and
  (non-NaN) `Float` return rank byte **`0x08`** — they share one key space and must interleave.
- `crates/uni-query-functions/src/df_udfs.rs:3231` — `encode_order_preserving_f64` (the f64
  order-preserving byte codec, the only numeric encoder today).
- Public entry: `encode_cypher_sort_key` (`:2908`); used wherever the CypherValue ORDER BY key
  is materialized.

### Root cause
`Int` and `Float` share sort-rank `0x08`, so within that rank a key must order numerically
across both types (Cypher `ORDER BY` interleaves `1` and `1.5`). The only encoder is
`encode_order_preserving_f64`, so the `Int` arm is forced through `f64` — and `2^53` vs
`2^53 + 1` both round to `2^53`, yielding identical bytes.

### Why deferred (NOT a mechanical swap)
The obvious "encode `Int` as an order-preserving i64 layout" **breaks cross-type ordering**:
an `Int` key and a `Float` key would then compare by raw bytes in the same rank space, not by
numeric value, so `Int(2)` could sort before `Float(1.5)`. Because the two types must
interleave under one rank byte, a correct fix needs a **unified order-preserving numeric
encoding** that is (a) exact for the full i64 range and (b) correctly ordered against f64 —
a redesign of the rank-`0x08` key codec, with broad ORDER BY regression surface. That is a
design change, not a checked-arithmetic sweep, so it was carved out of R10.

### Fix plan
Design a single order-preserving encoding for all rank-`0x08` numerics with ≥64 mantissa bits
so i64 is exact while still ordering against f64. Candidate approaches:
1. **Normalized big-decimal / sign-exponent-mantissa key.** Encode every number as
   `sign · exponent · mantissa` with a 64-bit mantissa (holds any i64 exactly) in an
   order-preserving byte layout; both `Int` and `Float` route through it. Most robust; most work.
2. **f64 bucket + exact tie-break.** Keep `encode_order_preserving_f64` as the primary key, and
   for an `Int` not exactly representable as f64 append a suffix that orders the exact integer
   *within* its f64 bucket. Subtle: the suffix must also order correctly against neighbouring
   floats that fall in the same bucket — needs careful proof.
Do NOT give `Int`/`Float` distinct rank bytes (that would stop them interleaving — wrong for
`ORDER BY` over mixed numeric columns).

### Repro (flip)
`crates/uni-query-functions/tests/repro_df_udfs_sync.rs:168` →
`repro_finding_13_sort_key_int_collapse` currently pins the bug: `k_lo == k_hi` for
`Int(2^53)` vs `Int(2^53 + 1)`. **After fix:** assert `k_lo != k_hi` **and** `k_lo < k_hi`
(ordering, not just distinctness), plus a mixed Int/Float ordering case
(e.g. `Int(2^53) < Float(2^53 + 0.5) < Int(2^53 + 1)` via key comparison).

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-query-functions -E 'test(repro_finding_13_sort_key_int_collapse)'`
then full `-p uni-query-functions`, plus an end-to-end `-p uni-db` ORDER BY test over a column
mixing large integers and floats.

---

## D6 — R5 bulk UNIQUE misses the main Writer's unflushed L0 (cross-channel) — ✅ DONE

**Severity:** silent duplicate — a bulk load can twin a UNIQUE key that already exists on the
**main write channel but is not yet flushed to Lance**. The common case (loading onto existing
*flushed* data) IS now covered by the R5 storage probe; this is the residual cross-channel
window.

**Status:** fixed per the "preferred" plan below. The boolean core of
`Writer::check_unique_constraint_multi` is extracted into a public
`Writer::unique_key_exists_full_horizon(label, key_values, exclude_vid: Option<Vid>, tx_l0)`
(`writer.rs`) that probes current L0 + pending-flush + tx L0 + storage; the Writer's own check
is now a thin wrapper passing `Some(current_vid)`. The bulk validation
(`crates/uni-bulk/src/bulk.rs`, formerly `unique_key_exists_in_storage`, now
`unique_key_exists_full_horizon`) delegates to it via the `Arc<Writer>` that `BulkBackend`
already holds, passing `exclude_vid = None` (a brand-new bulk vertex has no VID to exclude). The
`exclude_vid` was made `Option<Vid>` so the storage filter omits the `_vid != …` clause when
there is no self — a `u64::MAX` sentinel would overflow the filter's i64 literal parsing (caught
by `test_bulk_unique_constraint_spans_buffer_flushes`). Repro sibling
`bulk_unique_ignores_preexisting_unflushed_l0_row` (no `db.flush()`) added as a live regression
test (NOT `#[ignore]`d); the ext_id global-uniqueness gap in the bulk path is a separate, still-open
follow-up (not part of D6). All 38 uni-db bulk tests + 10 uni-store constraint/unique tests green.

### Locations
- `crates/uni-bulk/src/bulk.rs:501` `validate_vertex_batch_constraints` → the UNIQUE branch
  calls `crates/uni-bulk/src/bulk.rs:689` `unique_key_exists_in_storage`, which probes only
  committed Lance rows via `self.backend.storage.backend().count_rows(...)` (the R5 fix).
- `crates/uni-store/src/storage/manager.rs` — `StorageManager` holds **no** `L0Manager`; the L0
  buffers and the O(1) `constraint_index` live on the `Writer`, which the `BulkWriter` does not
  hold. So the bulk path structurally cannot see the main channel's unflushed L0.
- Contrast (full-horizon reference): `crates/uni-store/src/runtime/writer.rs:2437`
  `check_unique_constraint_multi` consults current L0 + `get_pending_flush()` + tx L0 + storage;
  `check_extid_globally_unique` (`writer.rs:2201`) likewise.

### Root cause
The bulk loader is a **separate write channel** that shares the `StorageManager` (Lance) but
not the `Writer`'s L0 / constraint index. R5 closed the storage-visibility half (probe Lance),
but a key committed via the regular path (`tx.commit()` merges into main L0) and not yet
auto-flushed is invisible to the bulk validation — so a concurrent/interleaved bulk load twins
it. The single-vertex path sees it because it runs *on the Writer* and checks L0.

### Why deferred
The triage's ideal — "one constraint-lookup surface consulting the full horizon; batch and
bulk paths call it" — is a cross-boundary refactor. Either the `BulkWriter` must gain a handle
to the `Writer`'s `L0Manager`/constraint index (widening the bulk↔writer boundary), or bulk
UNIQUE checks must route through `Writer::check_unique_constraint_multi`. Both touch the
constraint-check surface the whole engine relies on and are riskier than the localized storage
probe shipped in R5.

### Fix plan
1. **Preferred:** expose a full-horizon constraint lookup as a shared service (consulting
   current L0 + pending_flush + tx L0 + storage) that both `Writer::check_unique_constraint_multi`
   and the bulk validation call. Give the `BulkWriter` the `Writer` handle (or its `L0Manager`)
   so `unique_key_exists_in_storage` becomes `unique_key_exists_full_horizon`.
2. **Narrower:** have the bulk load force-flush the main L0 to Lance before validation, so the
   existing storage probe sees everything. Simpler but pessimistic (forces a flush per load).

### Repro
`crates/uni/tests/common/bugs/bug_bulk_unique_preexisting_repro.rs` →
`bulk_unique_ignores_preexisting_committed_row` currently **flushes** the committed row
(`db.flush().await?`) before the bulk insert, so the storage probe catches it (R5 regression,
green). For D6, add a sibling that **omits the flush** (leaving the committed row in the main
L0) and asserts the bulk UNIQUE check still rejects the duplicate — that variant fails today
and should be `#[ignore]`d until the full-horizon surface lands. The file's doc comment already
notes the cross-channel limitation.

### Verify
After the fix: the no-flush variant rejects the duplicate; `RUSTC_WRAPPER="" cargo nextest run
-p uni-db -E 'test(~bulk)'` stays green (35 bulk tests).

---

# Wave 3 — no deferrals

**Wave 3** (localized P2 + harness P3 regions R18/L1/L8/L9/L3/L4/L5/L10/L11): **all 52
findings fixed** on branch `fix/correctness-scan-wave3` (base `d379cb88f`, **not pushed**;
19 commits `72740e032..544d5a0d8`). Every repro was flipped from pinning the bug to
asserting the fix, and each crate's suite is green. No finding was deferred.

Two things surfaced during the work that are **out of Wave 3 scope**, recorded here so they
are not mistaken for regressions:

- **`uni-query[33]` has a second, distinct root.** The L4 fix corrects `cypher_cross_type_cmp`
  for the legacy row-based `Accumulator` path (`executor/core.rs`), which is only reached via
  `execute_subplan`. A plain read `RETURN min(<temporal>)` routes through the DataFusion
  aggregate engine, which *also* returns the first-encountered value for MIN/MAX over a
  temporal column — a separate bug with a different root, not touched. Repro
  `repro_14_minmax_temporal` documents the routing and passes as a guard.
- **Pre-existing `uni-locy-tck` gap (NOT introduced by Wave 3).** `combinations::
  AssumeAbduceExtended::4d-1` ("ASSUME on empty graph with FOLD MNOR returns no rows")
  fails identically on the base commit `d379cb88f` (verified in a throwaway worktree):
  ASSUME/FOLD-MNOR yields 0 rows where 1 is expected. Docstring-independent, unrelated to
  the L11 `having executed:` fix. Left for a future Locy semiring pass.

---

# Wave 2 deferrals

**Wave 2** (P1 correctness clusters R9/R8/L6/R14/L7/R12/R15/L2): all 8 regions fixed on branch
`fix/correctness-scan-wave2` (base `595340a8f`, **not pushed**; see memory
`correctness_scan_wave2_progress`). Two L2 sub-findings deferred as larger, non-gating
planner changes:

## D7 — `uni-query[27]` pattern-comprehension inner column order — **FIXED**

**Status:** fixed. `build_inner_schema` now declares property columns interleaved per step
(this step's vertex properties, then this step's edge properties), matching the order the batch
is filled in — see `b9fb7b5de fix(query): keep a pattern comprehension's inner columns in one
order`, which is on `origin/main`.

The **guard**, however, landed separately and later: `repro_08_pattern_comprehension_colorder`
only `println!`'d its findings, so it passed identically with and without the bug and protected
nothing (the vacuous-fixture class, #205). It now asserts the exact projected value and has been
verified discriminating — reverting `build_inner_schema` to the pre-fix order fails it. The
observed failure is `List([])` rather than the value swap predicted below: reading `r1.w` as
`c.name` makes the `WHERE` predicate reject every candidate. Same root cause, different surface,
which is why asserting on the *correct* value rather than on a predicted *wrong* one matters.

The location line below is also stale: `build_inner_schema` lives in
`pattern_comprehension.rs`, not `expr_compiler.rs`. Retained for the record.

**Severity (as originally filed):** wrong results for a multi-hop pattern comprehension that references both an edge
property on one step and a vertex property on a later step (e.g. `[(a)-[r1:X]->(b)-[r2:Y]->(c) |
r1.w] WHERE c.name = ...`). Both columns are LargeBinary so `RecordBatch::try_new` succeeds, and
the predicate/map read the wrong column.

**Locations:** `crates/uni-query/src/query/df_graph/pattern_comprehension.rs:301` (evaluate —
pushes per-step vertex-then-edge property columns, interleaved) vs
`crates/uni-query/src/query/df_graph/expr_compiler.rs:1345` (`build_inner_schema` — declares all
vertex props across steps, then all edge props).

**Why deferred:** the two functions must agree on column order; re-aligning the materialization
loop (or the schema) is a careful cross-function change that could misalign further if done
hastily, and the in-tree repro (`repro_08_pattern_comprehension_colorder`) is non-gating (prints,
doesn't assert). **Fix plan:** make evaluate() push property columns in the SAME order
build_inner_schema declares (all vertex props across steps, then all edge props), or vice-versa;
tighten the repro to assert `['WEIGHT']`.

## D8 — `uni-query[32]` virtual-edge `Both` traverses only outgoing

**Severity:** an undirected match over a **plugin-registered virtual edge type**
(`MATCH (b)-[:VE]-(a)`) matches only the outgoing orientation — incoming matches are silently
dropped. Plugin-only: no built-in virtual edge type exists, so unreachable via schemaless Cypher
(the repro `repro_find32_virtual_edge_both_is_outgoing` is a native-edge control guard).

**Location:** `crates/uni-query/src/query/df_planner.rs:2973` — `plan_traverse_virtual_edge`'s
`AstDirection::Both` arm maps to `(edge_src_col, edge_dst_col)`, identical to `Outgoing`.

**Why deferred:** a correct `Both` needs a UNION of two HashJoins (bound node = `_src_vid`
producing target `_dst_vid`, UNION bound node = `_dst_vid` producing target `_src_vid`) — a
substantial rewrite of the single-join path, for a plugin-only unreachable surface. **Fix plan:**
build both orientation joins + projections and union them (mirroring how the native traverse Both
handles both directions).

---

# Wave 0 — findings that fell through (neither fixed nor deferred)

These two `uni-bulk` findings from `docs/correctness_scan_2026-07-05.md` were **not** among
Wave 0's 18 fixes **nor** its D1–D4 deferrals — they were simply missed, then tracked here and
**now both FIXED** (uncommitted). Their in-tree repros (wired into
`crates/uni/tests/common/bugs/mod.rs`) were un-`#[ignore]`d and flipped from pinning the bug to
asserting the fix. See each section's **Status** line.

## D9 — `uni-bulk[2]` interrupted-bulk recovery abandons a table on rollback failure (High) — ✅ DONE

**Severity:** durability / data-integrity — after a crash mid bulk-load, if a table's rollback
fails during recovery the intent marker is deleted anyway, so the divergent table is **never
reconciled again** (permanent per-label/main divergence).

**Status:** fixed. `recover_interrupted_bulk_load`'s `Active` arm now threads its rollback
`failures` count out of the `match`; after the match, a non-zero count returns an error
**without** clearing the marker (mirroring the `Committed` branch's fail-before-clear
contract), so the marker survives for a later reopen to retry. A subtlety surfaced during the
fix: a table created during the load (`pre_version = None`) that was never committed — the load
faulted before writing it — is already **absent**, so its "failed" `drop_table` is a benign
no-op, not divergence (this is exactly the H9 `bulk_partial_flush_rolled_back_on_reopen`
recovery path). So a `None`-pre_version drop error is only counted as a failure when
`table_exists` still reports the table present; a `Some`-pre_version rollback error is always
genuine (the pre-existing table must be restored). Repro un-`#[ignore]`d and flipped to
`active_recovery_retains_marker_when_rollback_fails` (drives the real recovery with `Some`
pre-versions on absent tables → genuine failure → asserts `Err` + marker retained). All 41
uni-db bulk tests green.

### Location
- `crates/uni-bulk/src/flush_intent.rs:159-185` — `recover_interrupted_bulk_load`, `Active` arm.
  On rollback error it does `failures += 1` + `tracing::warn!` (`:170-173`) but **`failures`
  never gates control flow**; `clear(&store).await?` (`:184`) runs unconditionally and deletes
  `catalog/bulk_flush_intent.json`.
- Contrast: the `Committed` arm (`:145-158`) propagates `set_latest_snapshot` failure via `?`
  **before** `clear`, preserving the marker for retry — the correct pattern not applied to `Active`.

### Root cause
The `Active` branch treats a failed rollback as a warning, not a control-flow error, then clears
the marker regardless — so recovery reports `Ok(())` and the next reopen's `read()` returns `None`.

### Fix plan
Gate the clear on success: after the loop, if `failures > 0` return an error (or skip `clear`) so
the marker survives for a subsequent recovery attempt — mirroring the `Committed` branch's
`?`-propagation. Then flip the repro to assert the marker is **RETAINED** on rollback failure.

### Repro
`crates/uni/tests/common/bugs/bug_bulk_flush_intent_abandon_repro.rs` →
`active_recovery_deletes_marker_even_when_rollback_fails` (`#[ignore]`d; drives the real
`recover_interrupted_bulk_load` with an intent naming non-existent tables so rollback genuinely
fails). Pins current behavior: marker deleted, recovery returns `Ok`.

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-db --run-ignored all -E 'test(active_recovery_deletes_marker_even_when_rollback_fails)'`
(currently passes = bug present; after fix, un-ignore + flip to require the marker retained).

## D10 — `uni-bulk[5]` bulk CHECK `=`/`!=` don't coerce Int/Float (Low) — ✅ DONE

**Severity:** wrong constraint result — a numeric CHECK equality against a Float property
(`(n.score = 5)` with stored `Float(5.0)`) spuriously **fails**, while the equivalent bounding
form (`>= 5`) passes.

**Status:** fixed. `evaluate_check_expression` now routes `=`/`==`/`!=`/`<>` through the
existing `compare_values` helper (which already coerces Int↔Float) when **both** operands are
`is_number()`, falling back to `Value`'s strict `PartialEq` otherwise (preserving String/Bool
semantics and avoiding `compare_values` erroring on mixed non-numeric types). Repro
un-`#[ignore]`d and flipped to `bulk_check_equality_float_vs_int_literal_passes` (asserts the
insert succeeds + commits); the `>=` control test is unchanged.

### Location
- `crates/uni-bulk/src/bulk.rs:781-783` — `evaluate_check_expression`: `=`/`==`/`!=`/`<>` use
  `prop_val == &target_val` (Value's type-strict `PartialEq`), while `>`/`<`/`>=`/`<=` route
  through `compare_values` (`:796-815`), which **does** coerce Int↔Float.
- Root: `crates/uni-common/src/value.rs:802` `impl PartialEq for Value` has **no** `(Int, Float)`
  arm, and the literal `5` parses to `Int` first (`bulk.rs:771`), so `Float(5.0) == Int(5)` = false.

### Fix plan
Route `=`/`!=` through numeric coercion: when both operands `is_number()`, compare via
`compare_values(...).is_eq()`; otherwise fall back to `PartialEq`. Then flip the repro to assert
the insert SUCCEEDS.

### Repro
`crates/uni/tests/common/bugs/bug_bulk_check_int_float_repro.rs` →
`bulk_check_equality_float_vs_int_literal_false_reject` (`#[ignore]`d, pins the spurious
violation) + `bulk_check_ge_float_vs_int_literal_passes_control` (active control: `>=` coerces
and passes, isolating the defect to `=`).

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-db --run-ignored all -E 'test(~bulk_check_)'`
(equality test currently passes = bug present; after fix, un-ignore + flip to `is_ok()`).

---

## Quick resume checklist
- [x] **D4** (`uni-query[29]`) — DONE `c393dfb87` (label-column replace + candidate-set
      overwrite filter; repro un-ignored).
- [x] **D2** (`uni[6]`) — DONE `c9e80cff2` (parent-branch tip captured under `flush_lock`;
      deterministic failpoint repro, negative-control proven).
- [x] **D3** (`uni-fork[3]`) — DONE (uncommitted): promote-local content re-verify — ext_id
      re-injection (`content_uid_with_ext_id`) + live-content check in `batch_resolve_primary_vids`;
      `UidIndex` left append-only, staleness filtered at read time. Canary
      `promote_default_is_insert_only_twin` stays green; new e2e idempotency test added.
- [x] **D1** (`uni[2]`) — DONE `01fb4ca16` (per-kind set + kind-specific planner probes; repro
      `two_fork_index_kinds_on_one_column_coexist` flipped and wired in).
- [x] **D5** (`uni-query-functions[2]`) — DONE (uncommitted): f64 bucket + exact i64 tie-break in
      the encoder (`encode_numeric_payload`), shared `cmp_i64_f64` for the 4 comparator mirrors;
      repro `repro_finding_13_sort_key_int_collapse` flipped + 4 mirror repros + e2e ORDER BY.
- [x] **D6** (R5 bulk cross-channel) — DONE (shared `Writer::unique_key_exists_full_horizon`
      with `exclude_vid: Option<Vid>`; bulk delegates via `BulkBackend.writer`; repro
      `bulk_unique_ignores_preexisting_unflushed_l0_row` un-`#[ignore]`d/live).
- [x] **D9** (`uni-bulk[2]`, High) — DONE (gate marker `clear` on `failures == 0` in
      `flush_intent.rs` `Active` recovery; benign already-absent drops excluded; repro flipped to
      `active_recovery_retains_marker_when_rollback_fails`).
- [x] **D10** (`uni-bulk[5]`, Low) — DONE (coerce Int/Float via `compare_values` in CHECK
      `=`/`!=`; `bulk.rs` `evaluate_check_expression`; repro flipped to
      `bulk_check_equality_float_vs_int_literal_passes`).

**All correctness-scan findings are now resolved.** D5 (Wave 1) is FIXED (uncommitted): f64 bucket +
exact i64 tie-break in the sort-key encoder plus a shared `cmp_i64_f64` for the 4 comparator mirrors.
D3 (Wave 0) is FIXED (uncommitted, promote-local content re-verify). D9/D10 (Wave 0, missed) are also
FIXED (uncommitted). One narrow **follow-up** remains outside the scan: the writer/apply `=`/`!=`
cross-type numeric equality gap (D10-family, fixed only for bulk) — see the D5 section.
D1 (Wave 0) is now fixed on `main` (`01fb4ca16`), resolving the sole real correctness-scan
Wave 4 (unverified) finding — the other 16 unverified findings were already fixed in Waves 2-3.
Branch `fix/correctness-scan-wave0` is FF-merged into local `main`; `fix/correctness-deferred-d2-d4`
(D4 + D2) and `fix/correctness-scan-wave1` (all 8 Wave-1 regions) are not yet merged. Nothing
pushed.
