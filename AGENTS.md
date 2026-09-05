# Repository Guidelines

## Project Structure & Module Organization
- `crates/`: the Rust workspace (~27 crates). Load-bearing ones:
  - `uni`: public API facade (`Uni` / `Session` / `Transaction`, forks, plugins host wiring) — also hosts the bulk of the integration tests under `crates/uni/tests/`.
  - `uni-store`: storage engine — Lance (L1) backend, L0 buffers, WAL, OCC/SSI commit machinery, snapshots, compaction.
  - `uni-query`: Cypher planner + DataFusion-based executor, plus the Locy fixpoint runtime (`df_graph/locy_*`).
  - `uni-cypher`: pest grammar, parser, AST (shared by Cypher and Locy front-ends).
  - `uni-locy`: Locy compiler front-end (typecheck, stratification, probabilistic semantics).
  - `uni-plugin*`: plugin framework — `uni-plugin` (kernel/traits), `-host`, `-builtin`, and the WASM/Extism/Rhai/PyO3 loaders.
  - `uni-algo`, `uni-crdt`, `uni-fork`, `uni-bulk`, `uni-cli`: graph algorithms, CRDT types, fork engine, bulk ingest, CLI.
  - `uni-tck` / `uni-locy-tck`: openCypher and Locy conformance suites (cucumber).
- `bindings/`: PyO3 Python wheels (`uni-db` + 5 feature variants sharing one source tree) and `uni-pydantic` (OGM).
- `website/docs/`: user-facing documentation (mkdocs).
- `docs/UNI_BLACK_BOOK.md`: the internals reference (architecture source of truth).
- `examples/`, `demos/`: runnable examples, plugin samples, demo walkthroughs.

## Build, Test, and Development Commands
- `cargo build`: compile the workspace.
- `cargo run -p uni-cli`: run the CLI binary.
- `cargo nextest run`: run all tests (parallel, preferred).
- `cargo nextest run -E 'test(<name>)'`: run a specific test by name.
- `cargo nextest run --run-ignored all`: include ignored/slow perf tests.
- `cargo bench`: run Criterion benchmarks.
- `cargo fmt`: format code.
- `cargo clippy`: lint for common issues.
- Use `cargo nextest` instead of `cargo test` for regular test runs. The one
  exception is the miri lane, which uses `cargo miri test` — see the `miri` job
  in `.github/workflows/pr.yml` for why.

## Coding Style & Naming Conventions
- Follow standard Rust style and format with `rustfmt`.
- `snake_case` for modules/functions, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Keep file names aligned with module paths (e.g., `crates/uni-store/src/runtime/wal.rs`).
- Add brief comments only when logic is not obvious.

## Testing Guidelines
- Unit tests live next to code in `crates/*/src/**` with `#[cfg(test)] mod tests`.
- Integration tests live in `crates/*/tests/` (mostly `crates/uni/tests/common/`) and often use `#[tokio::test]`.
- **Consolidate integration-test binaries — hard cap of 3 per crate.** Cargo builds
  one binary per `.rs` file directly under `tests/`, each statically linking the full
  dep set (datafusion/lance/candle), so loose files explode build/link time. When
  adding a repro or integration test, add a `mod <name>;` to the crate's existing
  `tests/integration.rs` — do **not** create a new top-level `tests/<name>.rs`. A 2nd/3rd
  binary is allowed only for a documented reason (mutually-exclusive feature set, a
  loom/model-check target, or a registry test needing process isolation). Full rationale,
  mechanism, and migration recipe: `docs/test_layout.md`.
- Benchmarks go in `crates/uni/benches/` using Criterion.
- If a test is flaky or sensitive to parallelism, document the constraint in a comment at the test site.
- For TCK compliance runs, use `scripts/run_tck_with_report.sh`.
- For filtered TCK subsets, use `scripts/run_tck_with_report.sh "~Match1"` (replace filter as needed).
- TCK run artifacts are written under `target/cucumber/` (results/report) and synced into `compliance_reports/` by mode.

## Commit & Pull Request Guidelines
- Use Conventional Commits as in history: `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`.
- PRs should include a short rationale, tests run, and links to related issues.
- Update `docs/UNI_BLACK_BOOK.md` when architecture or internals change, and `website/docs/` when user-facing behavior changes.

## Agent Git Safety Rule
- Do not perform any git action unless the user has explicitly instructed it in the current conversation turn.
- This includes all git commands and git-related workflows (for example: `status`, `diff`, `add`, `commit`, `push`, `pull`, `checkout`, `reset`, `merge`, `rebase`, `tag`, `stash`, `cherry-pick`).
- **Never `git commit` without the user's explicit approval in the current turn.**
  Staging, writing files, and reporting what *would* be committed are fine;
  creating the commit is not. The same applies to `push`.

## Agent GitHub Safety Rule
- **Do not create GitHub issues** unless it is genuinely a bug you have
  identified *and* have a working reproduction for. No speculative filings, no
  "class" or "proposal" issues, no filing a batch because a review or audit
  produced a list. Report findings in the conversation, or in
  `docs/proposals/`, instead.
- **Do not comment on existing issues or PRs, and do not post progress to
  GitHub.** Status belongs in the conversation and in this repo's own
  documents. `gh issue comment` / `gh pr comment` are off-limits.
- **Do not close issues by hand** (`gh issue close`). An issue closes only
  through a `Fixes #N` / `Closes #N` keyword in the body of the commit that
  resolves it, so the close follows the merge.
- Read-only `gh` usage (`issue list`, `issue view`, `pr view`) is fine.
- Rationale: an issue or comment is an outward-facing, hard-to-retract claim
  other people schedule work around. A filing without a repro asserts a defect
  that has not been shown to exist; a progress comment asserts a state the
  merged code has not reached, since local commits sit unpushed.

## Fork system — current invariants (post Phase 7)

The fork track shipped phases 0 through 7. This block captures the
*current* contract an agent needs to honor when touching fork code.
The phase-specific blocks below preserve the rationale and gotchas
from each phase for deeper context.

- **Registry single-writer rule.** Every mutation to
  `catalog/fork_registry.json`, `catalog/fork_schemas/*`,
  `catalog/fork_tombstones/*`, and `catalog/forks/{id}/id_allocator.json`
  goes through `ForkRegistryHandle`. The 2PC state machine in
  `crates/uni-store/src/fork/registry.rs` is the only writer; never
  patch the JSON files directly.
- **Reads are routed through `BranchedBackend`.** A forked session's
  `Session::query` and `Session::tx` go through the fork's branched
  backend, which chains Lance `base_paths` from the fork's branch
  up to main. Never bypass it to read fork data — the branch
  resolution is what gives the fork its isolated view.
- **`UidIndex` is NOT branch-isolated.** The shared
  `indexes/uni_id_to_vid/{label}/index.lance` dataset accumulates
  entries from both primary and fork branches. Anything that
  resolves UID → primary VID must verify the candidate via
  `MATCH (n:Label) WHERE id(n) = $vid` on a primary session. The
  `resolve_primary_vid` helper in `crates/uni/src/api/fork_diff.rs`
  is the canonical implementation.
- **Diff identity is content UID, not VID.** Vertex pairing is by
  `compute_vertex_uid(label, ext_id, properties)`; edge pairing is
  by `(src_uid, dst_uid)` scoped to edge type. VIDs on `DiffVertex`
  are informational. Two unrelated forks with overlapping VIDs
  still pair correctly.
- **Promote is atomic across patterns.** `Uni::promote_from_fork`
  opens one primary transaction, runs every pattern (vertex first,
  then edge — within-call vertex inserts feed the edge endpoint
  cache), and commits once. Don't split into per-pattern
  transactions.
- **Promote flushes the fork.** `Uni::promote_from_fork` calls
  `fork.flush().await?` on the fresh fork session it opens.
  Vertices happen to be visible without flush in practice but
  edges may not be from a now-dropped earlier session — relying on
  the asymmetry would silently miss promotion targets.
- **Schema growth on primary is safe; per-fork property additions
  are not.** Adding a new label or edge type on primary does not
  break existing forks. Adding a property column to an existing
  primary label *would* either leak to primary or break branch
  read-merge — not supported. Drop and recreate the fork after
  evolving primary if you need the new column.
- **Fork-local index files live under the fork's branch.** Drop
  and `drop_fork_cascade` clean them up automatically — never
  special-case index file deletion in the fork drop path.
- **`UidIndex` registers UIDs from forks too,** which is why every
  Phase 5 fusion type and every Phase 6/6b promote dedup uses
  `resolve_primary_vid` rather than raw `UidIndex::get_vid`.

## Fork invariants (Phase 2)
- **Registry edits go through `ForkRegistryHandle`.** Never write `catalog/fork_registry.json`, `catalog/fork_schemas/*`, `catalog/fork_tombstones/*`, or `catalog/forks/{fork_id}/id_allocator.json` directly — the 2PC state machine assumes single-writer access through the handle.
- **Fork creation must not hold any global lock during `lance_branch::create_branch`.** The registry mutex covers metadata PUTs only. Spec §10 requires fork creation not to block primary.
- **`lance::Dataset::create_branch` is not idempotent** at the same `parent_version`. Recovery must call `lance_branch::delete_branch` (force-delete wrapper) before re-attempting. Day 1 spike confirmed this contract.
- **Lance compaction retention must not be tightened below the longest live fork chain.** Silent fork-data corruption otherwise. Verified by `crates/uni-store/tests/lance_branch_retention.rs`.
- **Per-fork allocator** lives at `catalog/forks/{fork_id}/id_allocator.json` and is bootstrapped from primary's in-memory HWM at fork creation. Never let it start at 0 against a non-empty primary — Lance read-merge would shadow the fork's writes.
- **Per-fork WAL** lives at `wal_forks/{fork_id}/`, NOT under `wal/` — that prefix collides with primary's listing under recursive `ObjectStore::list`. See `crates/uni-store/src/fork/wal.rs` rustdoc.
- **Same-fork sessions share an `Arc<UniInner>`** via `UniInner.fork_inners` (cached as `Weak`). A commit on one session is immediately visible to siblings — preserve this when refactoring `ForkBuilder::build` or `Uni::drop_fork`.
- **`Uni::drop_fork` must check `inflight_tx_count` before `begin_drop`.** The registry transition to Tombstoned cannot be cleanly rolled back; surfacing `ForkInflightTx` early is the contract. The counter is on `UniInner` and paired with `Transaction::new` / `Transaction::drop` (incremented and decremented unconditionally).
- **On-the-fly fork dataset creation must keep main empty.** `BranchedBackend::ensure_branch_for_new` materializes an empty parent on `main`, branches from it, then writes the actual batches to the branch. Calling `create_dataset_then_branch` directly with the batches would leak fork data into primary's view.
- **Fork compaction is deferred to Phase 5.** Long-lived heavy-write forks should be `drop_fork`-and-recreate. The `uni_fork_l1_flushes` gauge plus `fork_fragment_warn_threshold` warn surface the risk operationally.
- **Fork-local schema additions go through `Session::fork_schema()`.** Never call `SchemaManager::save()` from a fork code path — the fork's `SchemaManager` shares primary's `path` and `store` (per `with_overlay`'s implementation), so a `save()` would overwrite primary's `catalog/schema.json` with the fork's merged view. Persistence for fork-local additions goes via `catalog/fork_schemas/{fork_id}.json` through `ForkRegistryHandle::update_schema_overlay`.
- **`StorageManager::at_fork_with_schema` must receive the merged schema.** `at_fork` (no-schema variant) clones primary's `schema_manager` into the fork's storage, which means `uni-query`'s strict-schema check sees primary's view, not the fork's. Always use `at_fork_with_schema(scope, merged_schema)` from `UniInner::at_fork` so the fork's storage and its `UniInner` share the same `Arc<SchemaManager>` and a fork-local `add_label` is visible to subsequent writes.

## Fork invariants (Phase 6b — diff & promote)
- **Diff identity is content-addressed UID, not VID.** `compute_diff` in `crates/uni/src/api/fork_diff.rs` buckets vertex rows by `compute_vertex_uid(label, None, properties)` and edge rows by `(src_uid, dst_uid)` scoped to the edge type — siblings off the same parent and totally unrelated forks compare correctly even when VIDs collide. `DiffVertex.vid` survives as an informational `Option<Vid>`; never branch on it for pairing.
- **Promote runs inside one primary transaction.** `Uni::promote_from_fork` opens a single `Session::tx()` against primary, runs every pattern, and commits at the end. Partial-failure semantics inherit from the underlying tx — a failure on pattern N rolls back patterns 1..N-1. Don't refactor into per-pattern transactions without preserving that atomicity.
- **Edge endpoints must exist on primary at edge-promote time.** `run_promote` checks the `just_inserted` cache (vertices added earlier in the same call) before falling back to `resolve_primary_vid` (UidIndex + Cypher MATCH-by-VID verify on primary). Fork-only endpoints surface as `edges_skipped_no_endpoint`; mix `PromotePattern::label(...)` and `PromotePattern::edge_type(...)` in one call to promote endpoints together with their edges.
- **The shared `UidIndex` is not branch-isolated.** Both primary and the fork write UID→VID entries into the same Lance dataset under `indexes/uni_id_to_vid/{label}/index.lance`. `get_vid` returns *any* registered VID (fork or primary), so always verify via `MATCH (n:Label) WHERE id(n) = $vid` on a primary session before treating the result as a primary endpoint. The helper `resolve_primary_vid` does this round-trip; route every primary UID lookup through it.
- **Promote flushes the fork before scanning.** `Uni::promote_from_fork` calls `fork.flush().await?` immediately after opening the fresh fork session. Without this, edges committed via a now-dropped earlier fork session aren't visible to the new session's read path. Vertices happen to be visible without flush in practice, but edges depend on it — relying on that asymmetry would silently miss edge promotion targets.
- **Promote enforces label / edge-type presence on primary before opening the tx.** Surfacing `UniError::LabelNotFound` or `UniError::EdgeTypeNotFound` early is preferable to a half-committed tx that fails in `bulk_insert_*`. Fork-only labels need a separate `db.schema().label(...).apply()` on primary first; same for fork-only edge types.

## Fork invariants (Phase 5b — vector + FTS fork-local fusion)
- **Per-branch vector + FTS index builds use Lance's native API.** Confirmed by the Phase 5b spike (`crates/uni-store/src/backend/lance_branch.rs::tests::phase5b_spike_per_branch_vector`): `Dataset::create_index` against a branch-checked-out dataset writes a branch-local vector index — main sees zero, branch sees one. Same pattern as Phase 5a's scalar BTree spike. FTS uses the same `IndexType::Inverted` path as primary's `IndexManager::create_fts_index` (with `InvertedIndexParams::default` — `ScalarIndexParams::for_builtin(Inverted)` is missing the required `base_tokenizer` field and will fail).
- **`BranchedBackend::vector_search` and `full_text_search` route through the fork's branch when one exists.** Phase 5b lifted the Phase 1 stub. `lance_branch::vector_search_on_branch` and `full_text_search_on_branch` open the branch and call Lance's native nearest-K / FTS — the `base_paths` chain on the branch surfaces both fork-local and parent-inherited rows in one scan, giving fused results. When the fork has no branch for the table (label never written through the fork), the BranchedBackend falls back to primary's path.
- **`FilterExpr` pushdown is dropped on the branch path.** Phase 5b's MVP doesn't push predicates through to `lance::Dataset::scan`; the caller above re-applies. Tighten in a 5b-followup if filter pushdown shows up in benchmarks.
- **Phase 5b doesn't add bespoke `FusedVectorSearchExec` / `FusedFullTextSearchExec` operators.** Lance's per-branch reads via `base_paths` are sufficient for the MVP. The runtime fusion happens at the `BranchedBackend` layer; bespoke physical operators are reserved for the case where recall benchmarks demand an explicit two-side merge with exact rerank.
- **Planner emission for vector/FTS uses `LogicalPlan::FusedIndexScanWrapped`** — a thin wrapper variant that carries `Box<LogicalPlan>` for the original operator (`VectorKnn`, `InvertedIndexLookup`, or a `ProcedureCall` to `uni.vector.query` / `uni.fts.query`) plus a `kind: FusionKind`. The physical planner unwraps and recurses on `inner`; runtime behavior is identical to the unwrapped node. The wrap is purely for explain-plan and runtime-stats observability so users can see that fusion is engaged. **The CALL-style invocations land as `ProcedureCall` not `VectorKnn`/`InvertedIndexLookup`** — the rewrite in `planner.rs::procedure_call_fusion_kind` matches by procedure name and the shape of the first two arguments.
- **Recall is measured at `crates/uni/tests/fork_index_recall_bench.rs`.** Phase 5b's MVP test (n=1000+100, q=20, K=10) shows recall@10 = 1.000 because Lance falls back to brute-force on small datasets. For spec §8.2's 95% recall@K target on N=100k+ items, write a Criterion bench at `crates/uni/benches/fork_index.rs` and capture in `compliance_reports/fork_index_<date>.md` — the test scaffold is the on-ramp.

## Fork invariants (Phase 5a-impl — fork-local index fusion)
- **Fork-local index files live under the fork's branch directory.** Confirmed by the Phase 5a spike (`crates/uni-store/src/backend/lance_branch.rs::tests::phase5a_spike_per_branch_index`): Lance writes index files branch-locally when `create_index_builder` runs against a branch-checked-out dataset. `drop_fork`/`drop_fork_cascade` cleans them up automatically — never special-case index file deletion in the fork drop path.
- **`ForkScope.fork_local_indexes` is registered ONLY after a successful build.** The `IndexRebuildManager`-style auto-builder and the manual `Session::build_fork_local_index` trigger both call `scope.register_fork_local_index` only on success; a failed build leaves the registry untouched so the trigger fires again on the next tick.
- **`VidUid` fork-local "index" is a planner marker, not a Lance file.** Lance's `base_paths` chain on the fork's branch already provides fork-first lookup semantics. The registry entry tells the planner to emit `FusedIndexScan { kind: VidUidForkFirst }`; no on-disk artifact is written. Don't add Lance build code for VidUid.
- **The planner emits `FusedIndexScan` only when the active session is forked AND `StorageManager::fork_index_exists(label, column)` returns `Some(_)`.** The rewrite happens in `uni_query::rewrite_for_fork_fusion`, called once after `planner.plan(ast)` at every query call site in `crates/uni/src/api/impl_query.rs`. Anywhere else in the codebase that constructs a logical plan from Cypher must apply the same rewrite — otherwise fork-local indexes silently won't fuse.
- **Phase 5a-impl decays `FusedIndexScan` to `Scan` in the physical planner.** This is intentional: Lance's per-branch index reads via `base_paths` produce correct fused results without bespoke physical operators. Phase 5b will replace the decay with `FusedVectorSearchExec` and `FusedFullTextSearchExec` for lossy types that genuinely need rerank/RRF.
- **The auto-builder (`fork_index_builder.rs`) only builds `ScalarBtree` for now.** `Sorted` and `VidUid` are explicit-only (manual trigger) — the auto-builder doesn't know if the user wants ORDER BY semantics or fork-first lookup semantics on a given column. Tests that need those kinds use `Session::build_fork_local_index` directly.
- **`ForkScope.fragment_counts` is in-memory only.** A process restart resets it to 0, and the trigger re-fires on the next flush. Don't try to persist it — Lance's per-fragment row counts on disk are the ground truth; the in-memory counter is a flush-time accumulator that drives the *trigger heuristic*, not the build itself.

## Fork invariants (Phase 4b — Python bindings)
- **Cross-language symmetry contract.** Any new fork API added to the Rust surface must land matching `Database` *and* `AsyncDatabase` (or `Session`/`AsyncSession`) wrappers in `bindings/uni-db/src/{sync_api.rs,async_api.rs}` plus a stub in `bindings/uni-db/uni_db/__init__.pyi`. The Phase 4b binding pairs are the template — sync calls `get_runtime().block_on(...)`, async wraps `future_into_py(...)`.
- **Fork builders return synchronously from the Python facade — only `.build()` / `.apply()` is awaitable.** This keeps the `await session.fork(name).ttl(td).build()` chain ergonomic; rebuilding the chain through awaitable factory methods (`(await session.fork(name)).build()`) confused early test users. `AsyncForkBuilder` / `AsyncForkSchemaBuilder` rely on `tokio::sync::Mutex::try_lock` to grab the wrapped session synchronously — surface a clear error if the lock is held instead of blocking.
- **Typed exception payloads use `setattr` after `create_exception!`.** PyO3's `create_exception!` macro doesn't support attribute fields; the pattern in `exceptions.rs` is to construct via `new_err(msg)` then `Python::attach(|py| err.value(py).setattr(...))` to attach typed payloads (`holder_count`, `children`, `blockers`, `current`/`max`, `name`, `stage`). Don't switch to `#[pyclass(extends=PyException)]` — it doesn't compose with the existing `create_exception!`-based hierarchy.
- **`Uni.in_memory()` returns the database directly; use `Uni.builder()` for config.** Tests and examples that need fork-config kwargs (`disable_fork_sweeper`, `max_forks`, etc.) must go through the builder — `in_memory()` skips the config plumbing.

## Fork invariants (Phase 4a — lifecycle & admin)
- **`UniConfig::disable_fork_sweeper` must be `true` in tests that race against TTL.** The sweeper runs every `fork_sweeper_interval` (default 60s) and calls `drop_fork_cascade` on any fork whose `ttl_expires_at` is in the past. Tests that don't disable the sweeper *and* set short intervals will produce flakes when timing slips. The Phase 4a TTL tests (`fork_ttl.rs`) intentionally exercise the sweeper end-to-end and use the multi-threaded tokio flavor so the sweeper makes progress.
- **`Uni::tag_fork` writes Lance tags namespaced `fork_{tag}_{dataset}`.** Each tag pins the fork branch's *current* version; subsequent fork writes do not move the tag. Tagged refs survive Lance compaction's GC sweep — this is what makes a tag-then-drop safe for audit retention. Never strip the namespace prefix when listing user-visible tags; `Uni::list_fork_tags` already does this.
- **`max_forks` budget counts Active + Pending + Tombstoned.** Tombstoned forks still hold branch state on disk until recovery completes, so counting them prevents create/drop churn from slipping past the cap.
- **Cancellation: forked sessions hold `parent_token.child_token()` — never rebuild the token from scratch in a forked-session code path.** Cancelling the parent cascades to children; cancelling a child does not affect the parent. `Session::cancel()` cancels the currently-held token and replaces it with a fresh one — to observe propagation in tests, capture token clones BEFORE calling cancel.
- **`StorageManager::pinned()` now preserves `fork_scope`.** Phase 4a lifted the Phase 1 debug_assert that forbade pin+fork. A pinned forked session reads through both the fork's branches (`fork_scope`) and at the pinned version (`pinned_snapshot`); writes are gated by `Session::is_pinned` at the API layer.

## Fork invariants (Phase 3 — nested forks)
- **`drop_fork` refuses parents with children.** Use `drop_fork_cascade(name)` to remove the whole subtree; that path pre-validates every node for `ForkInUse`/`ForkInflightTx` *before* tombstoning anything so a partial cascade never leaves orphans.
- **Nested fork branch lineage runs through `create_branch_from`, not `create_branch`.** When the parent is a forked session, `build_datasets_for_fork` reads the parent branch's version with `current_version_on_branch` and creates the child branch with `create_branch_from(uri, child, parent_branch, version)` so Lance's `base_paths` chains `child → parent_branch → main`. Never branch a nested child off main when the parent has a branch — the child would skip the parent's writes and lose snapshot isolation against post-creation parent writes.
- **`create_fork_2pc` auto-flushes the parent fork's L0.** A nested child branches off the parent's Lance tip; if the parent's writes are still buffered in L0, the child sees a stale view. The flush is inside the create path, *before* `current_version_on_branch` reads the parent branch's tip. Don't remove this without an equivalent guarantee elsewhere.
- **Schema overlay composition is implicit through chained `at_fork`.** `SchemaManager::with_overlay` clones the *current* manager into a new merged snapshot, so nesting at_fork onto a fork-scoped UniInner produces `primary ⊕ parent_overlay ⊕ child_overlay` for free. `SchemaDelta::merge_atop` is available as a primitive but is not on this hot path.
