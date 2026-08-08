# Documentation remediation — audit findings and sequenced execution plan

> **Status: RESOLVED — historical snapshot as of 2026-08-06.**
> The findings below describe the tree *at that date*. Remediation is complete; see
> `documentation_remediation_plan_2026-08-07.md` §6 for the per-workstream landing
> commits and the four decisions that closed the open questions. The counts, line
> numbers and severities here are preserved as written and are no longer accurate —
> `search_procedures.rs` line numbers in particular have drifted by ~40-60.

**Status:** Audit complete, evidence-verified · remediation **complete** (see banner). · **Date:** 2026-08-06
· **Trigger:** full-surface documentation evaluation, 10 parallel evaluators over ~190 markdown
files. · **Baseline:** local `main` `d8f6f3abc` (v3.3.0). · **Supersedes:** nothing — additive.
· **Lineage:** the drift class named in `python_plugin_abi_gaps_2026-07-16.md` (#150) and
`graphcompute_projection_parity_2026-07-19.md` (#151) — guest/native surface drift — recurs here
as **doc/source drift**, with the same root shape: a surface with no compiler between the claim
and the truth.

---

## 0. How to read this

Every finding below was verified against source. Findings are cited `file:line — claim —
evidence`. Where an evaluator could not verify a claim it is marked **unverified** and excluded
from the counts; those are listed in §8.

Verification was not prose review. The methods used were:

| Method | Applied to | Caught |
|---|---|---|
| pest-grammar parse harness | every Cypher/Locy snippet in the audited slices | Tier E, all 10 items |
| live execution against the built extension | `getting-started/` Python snippets | A2, the `.get()` class |
| symbol grep against `crates/` | every documented type/method/trait | Tiers C, D, F |
| `__init__.pyi` diffing | Python + Pydantic reference | §4 S1 coverage figures |
| `Default` impl comparison | `reference/configuration.md` | 66/66 clean — a negative result |
| CI workflow / Cargo.toml cross-check | runbook, release, test-layout docs | R2, R5, R6 |

**Counts are the verifier's, not an impression.** Where a briefed premise turned out to be wrong,
the evaluator graded against source and said so — two such corrections are recorded in §8.

---

## 1. Health map

| Slice | Files | Broken | Needs work | Good |
|---|---|---|---|---|
| getting-started | 7 | 2 | 3 | 2 |
| features | 18 | 4 | 7 | 7 |
| guides | 11 | 4 | 3 | 4 |
| concepts + internals | 13 | 3 | 10 | 0 |
| locy | 8 | 0 | 4 | 4 |
| plugins | 11 | 1 | 6 | 4 |
| reference + `complete_*_api` | 12 | 0 | 9 | 3 |
| use-cases + examples | 12 | 3 | 9 | 0 |
| `docs/` internal | 12 | 0 | 6 | 6¹ |
| root + bindings | 14 | 3 | 8 | 3 |

¹ includes 5 dated reports classed stale-by-design.

---

## 2. Repo defects surfaced by the audit

These are **code and config bugs**, not doc bugs. They are listed first because three of them
are cheap and one blocks a release.

| # | Defect | Evidence |
|---|---|---|
| **R1** | `bindings/uni-pydantic/src/uni_pydantic/__init__.py:33` — `__version__ = "2.5.0"` against workspace 3.3.0. **Blocks the next `uni-pydantic` publish.** | `releasing_version_bump.md:42-46` warns this exact drift is un-CI-enforced; the warning came true |
| **R2** | `crates/uni-tck` has 4 integration binaries, violating the documented cap of 3. No `autotests = false`, so 3 `repro_*.rs` auto-discover alongside the `[[test]]` at `Cargo.toml:24` | none of the three documented exception classes applies — `repro_side_effects_false_pass.rs:1` is a plain step-handler test |
| **R3** | `verify_hash_pin` (`uni-plugin/src/verify.rs:32`) has **zero call sites**. `uni/src/api/plugin_trust.rs:78` claims it "is applied separately at load sites" — false | grep, workspace-wide |
| **R4** | `docs/migrations/0.9.0-wheel-matrix-collapse.md` was never committed — it exists only inside `.claude/worktrees/tx_commit_profile/`. It is linked from all 5 variant wheel READMEs, i.e. a 404 on 5 PyPI pages | `git ls-files docs/migrations` → empty |
| **R5** | Released versions with no release notes: **2.4.0** (tagged), **3.0.1** (tagged), **3.1.0** (shipped at `65289f3db`, untagged), **3.3.0** (current, untagged). Newest file is `RELEASE_NOTES_3.2.0.md` | `git tag --list` vs `docs/release_notes/` |
| **R6** | No `rust-version` key in `Cargo.toml` despite `edition = "2024"` (requires 1.85+); root README badge advertises 1.75+ | `Cargo.toml:81` |

---

## 3. Findings, by tier

### Tier A — front doors

The highest reader-volume-per-line in the tree.

- **A1.** `bindings/uni-db/README.md:20-38,50-95,120-127` — the entire quickstart/schema/txn/
  vector/async section uses the removed 1.x API: `uni_db.Database(...)`, `db.create_label`,
  `db.add_property`, `db.execute`, `db.query(str, dict)`, `db.begin()`, `db.bulk_writer()`,
  `AsyncDatabase.open`. None exist. `#[pyclass(name = "Uni")]` at `sync_api.rs:626` is the only
  DB class; `__init__.pyi` has `Uni`/`AsyncUni` only (`:1837,1983`). **The file is
  half-migrated** — its Forks section at `:152+` correctly uses `Uni.builder()`. This is the
  page a `pip install uni-db` user reads first.
- **A2.** `getting-started/quickstart.md:130-133,223-224,242,298,317,366-367` — **executed
  verbatim**: `AttributeError: 'builtins.QueryCommandResult' object has no attribute 'get'`.
  The six command-result pyclasses (`bindings/uni-db/src/types.rs:3262-3420`) are `frozen`,
  exposing `command_type`, one payload getter, `__repr__`, `__getitem__`. See §5 for why the
  naive fix is the wrong one.
- **A3.** `README.md:31` — `uni-db = "2"`, two majors behind 3.3.0. Contradicts
  `installation.md:27` (`uni-db = "*"`); neither says 3.
- **A4.** `crates/uni/README.md:26,48,63,73,87-91,104` — 6 nonexistent symbols; names the crate
  `uni` @ `0.1.0` (actual `uni-db` @ 3.3.0, `crates/uni/Cargo.toml:2`); `DataType::Integer`
  (real: `Int32`/`Int64`); `db.query(...)` (not on `Uni`); `db.query_builder()/.knn()/.k()`
  (zero hits).

### Tier B — silent wrong answers

No error surfaces. The user gets results back; they are the wrong results. **This is the worst
class in the audit** and should outrank cosmetic breakage.

- **B1.** `guides/vector-search.md:180-181,245,293,297` — `threshold` documented as *minimum
  similarity*. On the dense path it is a **maximum distance** (`dist <= max_dist`,
  `search_procedures.rs:1352-1354`). The page contradicts itself at `:233,235`. Examples at
  `:238,287-294` return wrong rows.
- **B2.** `features/full-text-json-search.md:79` — "`score` — normalized BM25 (0-1)" is
  inverted. `search_procedures.rs:1455` passes raw BM25 into `build_search_result_batch(...,
  &DistanceMetric::L2, ...)`; `calculate_score` computes `1/(1+bm25)`
  (`uni-query-functions/src/similar_to.rs:286-292`) — strictly **decreasing** in BM25. The
  doc's own `ORDER BY score DESC` (`:65,105`) therefore returns the **worst** matches first.
  **No test asserts fts.query score value or order** — this path is untested as well as
  mis-documented.
- **B3.** `features/full-text-json-search.md:74` — `threshold` filters *raw* BM25 before any
  transform (`search_procedures.rs:1430-1432`): a different scale **and** direction from the
  yielded score.
- **B4.** `use-cases/recommendation-engine.md:44` — declares an edge type to a `Category` label
  never created. `add_edge_type` (`uni-common/src/core/schema.rs:1470-1506`) does not validate
  dst-label existence, so `apply()` **succeeds** and the failure surfaces later at query time.
- **B5.** `features/hybrid-search.md:91` — "`k` … (default: 10)". `k` is **required** and
  hard-errors if absent (`search_procedures.rs:1533` → `require_int_arg` `:45-54`; registered
  via `arg()`, i.e. no default). Same for `uni.fts.query` (`:1407`) and `uni.vector.query`
  (`:1317`).

### Tier C — fabricated architecture

Documents types that do not exist and, in two cases, never did. More dangerous than staleness:
stale docs describe something that *was* true, so trying them fails cleanly; these send a
contributor hunting for a symbol that was never written.

- **C1.** `internals/query-planning.md:428-1074` — `OptimizationRule`, `QueryOptimizer`,
  `PredicatePushdown`, `ProjectionPushdown`, `ScanToIndex`, `LimitPushdown`, `JoinReorder`,
  `CostEstimator`, `TableStatistics`, `ColumnStatistics`, `PhysicalPlan` + all 10 variants,
  `PlannerConfig`. **None exist repo-wide.** Optimization is delegated to DataFusion
  (`df_planner.rs` / `HybridPhysicalPlanner`).
- **C2.** `concepts/identity.md:41-125` — VID/EID bit-packing (16-bit `label_id` + 48-bit
  offset) with `label_id()`/`local_offset()`/`type_id()` accessors. `core/id.rs:118-124` states
  the opposite: *"pure auto-increment… they no longer embed label information."* `Vid::new`
  takes **one** arg (`id.rs:26`); the top bit is `EPHEMERAL_BIT` (`id.rs:47`). Repeated at
  `data-model.md:40,155,431`.
- **C3.** `internals/storage-engine.md:241,208,609,342-369,746,791` — `L2Layer`,
  `L0ManagerConfig`, `Wal`/`WalReader`, `LanceFragment`, `FragmentStatistics`, `ColumnStats`,
  `VectorIndexStorage`, `ScalarIndexStorage`. Real WAL type is `WriteAheadLog`
  (`runtime/wal.rs:231`) with `replay_since()` — no `recover()`, no per-entry CRC API. Also
  `:868-876,997,1012` `json_get_string/_int/_float/_bool` (zero hits; extraction is inline at
  `df_graph/scan.rs:582-592`) and `:505-529` an invented chunked-CSR adjacency schema (real:
  3 columns, one row per vertex — `storage/adjacency.rs:151-169`).
- **C4.** "Parser built on sqlparser" — `internals/index.md:57`, `query-planning.md:31,34`,
  `architecture.md:153,511`. `grep sqlparser **/Cargo.toml` → **0 hits**. The parser is pest.
- **C5.** `guides/cypher-querying.md:1824-1863` — the entire `crdt.increment` / `orset_add` /
  `orset_remove` / `map_put` section plus an 18-function table. **No `crdt.*` function is
  registered anywhere.**

### Tier D — post-3.0 plugin fallout

Third-party plugin authors have no compiler between them and this page.

- **D1.** `plugins/authoring-rust.md:73,76,81,87` documents traits `OperatorProvider`,
  `StorageBackend`, `PregelProgramProvider`, `Connector` and registrar methods `operator()`,
  `pregel()`, `connector()`, `storage_backend()`. All four were deleted in 3.0 — the code
  asserts it directly:

  ```rust
  // crates/uni-plugin/src/surfaces/mod.rs:1270-1276
  assert_eq!(kinds.len(), 22);
  // "The 3.0 breaking change removed the four dead registrable surfaces
  //  Operator, Pregel, StorageBackend, and Connector."
  ```

  Same four at `plugins/concepts.md:29,32,34,42`. Both files also **omit** the two live surfaces
  `locy_generator` (`traits/locy.rs:308`) and `replacement_scan` (`traits/catalog.rs:120`).
  The real method is `label_storage`, not `storage_backend(scheme, b)`.
- **D2.** `plugins/trust-and-capabilities.md:89-91` describes verification happening "under the
  default-on `ed25519` Cargo feature" with a degraded fallback when disabled. **Neither the
  feature nor the fallback exists** — `crates/uni-plugin/Cargo.toml` `[features]` contains only
  `otel`; ed25519-dalek and base64 are unconditional deps (`:47-48`). Security-relevant
  fabrication: a reader could audit for a flag that was never real. Companion source bug is R3.
- **D3.** `plugins/trust-and-capabilities.md:24` lists hash-pin as a live trust mechanism (R3).

### Tier E — syntax verified broken against the grammar

| Finding | Sites |
|---|---|
| `--` as a Cypher comment; grammar has only `//` and `/* */` (`cypher.pest:6-9`). **Some are inside `tx.execute("…")` string literals**, i.e. runtime parse failures, not doc-block noise | 6 guides (~47 lines): `schema-design.md` (20, incl. `:380-384,520-522,578-580`), `cypher-querying.md` (12), `vector-search.md` (9), `sparse-vectors.md` (6), `bge-m3-hybrid-retrieval.md` (6), `performance-tuning.md` (4); plus `use-cases/rag-knowledge-graph.md:101,112` |
| `EXPLAIN RULE r WHERE a=…, b=…` — comma list; `explain_rule_query` takes one expression (`locy.pest:631-636`). Confirmed twice: by grammar **and** by execution | `locy/quickstart.md:63`, `index.md:73-75` |
| `DERIVE (a:FlaggedAccount)` — `derive_pattern` admits only edge forms + `DERIVE MERGE`. The prose describes a feature that does not exist | `locy/language-guide.md:118` |
| `CALIBRATE r USING platt_scaling` / `VALIDATE r USING brier` — real form is `… ON MATCH <pat> TARGET <expr> METHOD <m>` / `METRICS <m>,…` (`locy.pest:578-586,602-609`); metric is `brier_score` | `features/neural-predicates.md:119-120` |
| `PROFILE <query>` as executable Cypher — no PROFILE token; only `explain_query` (`cypher.pest:800`) | `guides/performance-tuning.md:383-390`, `skills/uni-db/references/cypher.md:565-589` |
| `CREATE CONSTRAINT … FOR … REQUIRE` — Neo4j 5 syntax; grammar is `ON (p:L) ASSERT` (`cypher.pest:701-716`) | `guides/cypher-querying.md:1608-1617` |
| `DataType::Json` — not among the 21 real variants (`schema.rs:168-200`); DDL `JSON` maps to `CypherValue` | `use-cases/supply-chain.md:33`, `concepts/data-model.md:241,295,375` |
| `CREATE LABEL Document;` — parens + ≥1 property mandatory (`cypher.pest:725-728,770`) | `concepts/data-model.md:94` |
| `CALL uni.algo.pageRank() YIELD nodeId, score` — parses, **fails at execution**; 2 args required (`procedure_template.rs:216-219,305`) | `features/graph-algorithms.md:26,41` |
| `CALL uni.vector.knn(...)` — no such procedure; registry has `uni.vector.query` | `demos/demo01/spec.md:562` |
| `session.tx()` missing `.await?` — `pub async fn tx(&self) -> Result<Transaction>` (`session.rs:743`) | `schema-design.md:348,378,497,518,546,575`; `data-ingestion.md:209`; `performance-tuning.md:204` |

### Tier F — coverage holes

- **F1. The entire fork API is undocumented in the published reference.**
  `grep -ci fork website/docs/reference/python-api.md` → **0**. Missing on `Uni`/`AsyncUni`:
  `list_forks, fork_info, drop_fork, drop_fork_cascade, tag_fork, untag_fork, list_fork_tags,
  diff_fork_primary, diff_forks, promote_from_fork, promote_from_fork_with_options`
  (`__init__.pyi:1897-1908` sync / `:2050-2069` async); on `Session`/`AsyncSession`:
  `fork(), fork_schema(), is_forked()` (`:1525-1527,2121-2123`). Same hole on the Rust side —
  29 of 60 `Uni` methods undocumented, including every plugin loader and
  `periodic_schedule/list/cancel`.
  The cross-language symmetry contract in `CLAUDE.md` is satisfied **in the stubs** and reflected
  in **neither** wrapper's docs.
- **F2.** `docs/complete_rust_api.md` — `ssi_enabled` appears **0 times** despite SSI being
  default-on.
- **F3.** `guides/index.md:5-56` links 7 guides; `sparse-vectors.md`,
  `bge-m3-hybrid-retrieval.md`, `ai-skill.md` are in the nav but absent from the hub.
- **F4.** `examples/index.md:19-30` lists 8 Locy notebooks; 16 exist.

---

## 4. Systemic causes

Fixing the tiers above without these will regenerate them.

**S1 — Two hand-maintained API references for one surface; the better one is unpublished.**

| | website `reference/` | `docs/complete_*.md` |
|---|---|---|
| Python | 188/246 (76%) | 215/246 (87%) |
| Rust | 82/127 (65%) | 94/127 (74%) |
| Pydantic | 45/70 (64%) | 68/70 (**97%**) |
| In `mkdocs.yml` nav | **yes** | **no** |

10,054 lines total, neither generated. They drift **independently**, which is why each has
invented methods the other lacks — `xervo.rerank` (`python-api.md:879`) and `row.get_by_index`
(`rust-api.md:1361`) in the website set; `session.profile_with` (`complete_python_api.md:779`)
in the `docs/` set. Users only ever see the weaker one.

**S2 — Rust/Python builder confusion. Found independently by 4 of 10 evaluators.**
`UniBuilder::hybrid()` / `.cloud_config()` documented on the **Rust** builder at
`reference/configuration.md:451-455,481-486,779-784`, `programming-guide.md:150-167`,
`performance-tuning.md:561-595,643-647`, `internals/storage-engine.md:1096-1100`,
`crates/uni/README.md:87-91`. They exist only in the Python bindings
(`bindings/uni-db/src/builders.rs:224,267`); the Rust API is
`remote_storage(remote_url, CloudStorageConfig)` (`api/mod.rs:1242`). Two same-named builders
with divergent method sets across the FFI boundary is an **ergonomic hazard**, not just rot —
independent convergence by four evaluators is the tell.

**S3 — Builder terminals documented as session methods. 3+ docs.**
`session.set()/get()` are on the `Params` facade via `session.params()`
(`session.rs:1450-1457,580`). `session.explain()/profile()` are `QueryBuilder` terminals
(`session.rs:1613,1628`). Wrong in `programming-guide.md:1022-1063,1079-1102`,
`skills/uni-db/SKILL.md:217,278`, `guides/cypher-querying.md:787-790,804-809,1254-1284`.
When the same wrong API recurs independently, the API is surprising — not the writers careless.

**S4 — Dated reports with no resolution status read as live issues.**
`fork_production_readiness_review_2026-06-20.md:7` still leads with "**NOT production-ready
as-is**" and a CRITICAL primary-data-corruption finding. It is **fixed**:
`storage/manager.rs:666` now builds `SnapshotManager::new_for_fork(...)`, and the shared
`vid_labels_index` is deep-copied at `:689-690`. Every `manager.rs:NNN` citation in it is also
dangling. Same for `correctness_performance_review_2026-06-13.md` (C1 fixed at
`backend/lance.rs:362`) and the three `correctness_scan_*_2026-07-05.md`.
`docs/correctness-deferred.md` demonstrates the correct convention — per-finding resolution with
commit SHAs — and was rated exemplary.

**S5 — Unattributed performance numbers.** No date/version/hardware/dataset on
`internals/benchmarks.md` (stamped 2026-04-01, 4 months stale, every table),
`demos/demo01/spec.md:605-627` (p50/p95/p99 plus a cost table vs Pinecone + Neo4j + Mongo,
citing a 2.3M-paper/28M-edge corpus when `generate_data.py:64` sets `NUM_PAPERS = 5000`),
`storage-engine.md:1145-1167`, `concurrency.md:381-385`, and both
`crates/uni/examples/locomo_*_results.md`. `benchmarks.md:319` also lists a
`pushdown_performance` suite with **no `[[bench]]` target** — the command it prints fails.

**S6 — Counting claims never reconcile.** "23 extension surfaces" ×7 (real: 22 registrable /
21 grantable / 26 raw variants — no reading yields 23); "59-kernel catalogue"
(`plugins/index.md:69`) vs 72 (`graph-algorithms.md:226`, correct); "36 graph algorithms" vs 37
vs 40; "1,339 scenarios / 220 feature files" vs 221 / 3,926; "8 reference files" vs 12;
"12 predicates" with 11 listed.

**S7 — Behavior-changing commits update docs inconsistently.** `2f3c0802f` (QUERY answered from
the derived store, not SLG) touched 5 `.rs` files and **zero** docs — `complete_locy.md:62,978,
1307-1315,1694` and `UNI_BLACK_BOOK.md:3652` still teach the old model, now inverted
("use QUERY instead of materializing the whole relation" is exactly backwards). `baed1e98f`'s
breaking `IsNotSubjectNotANode` is documented nowhere. By contrast `fbe60af91` (recursive FOLD
across derivations) *did* update its docs. The discipline exists; it is applied unevenly.

**S8 — Rot runs bottom-up.** The consistent shape across all 10 slices: recently built surfaces
are documented accurately — SSI came back **completely clean** (no LWW residue anywhere), forks
check against every Phase 0-7 invariant in `CLAUDE.md`, and `pydantic-ogm.md`,
`temporal-intervals.md`, `sparse-vectors.md`, `bge-m3-hybrid-retrieval.md` verified defect-free
end to end. Rot concentrates in foundational material written once and never revisited: VID
encoding, the optimizer chapter, and "LanceDB" in 27 Black Book sites (`lancedb` is not a
dependency anywhere; root `Cargo.toml:132` pins `lance = "7.0.0"`, and `LanceDbStore` is now
free functions in `backend/table_names.rs`).
**One inversion worth noting:** `crates/uni-tck/README.md` *under*claims — "Phase 2+ TODO",
"many steps use `todo!()`", "will panic" — when `src/` contains zero `todo!()` and the newest
report is **3925/3926, 100.0%**.

---

## 5. Do NOT do

Four items where the naive version of a confirmed finding is actively wrong.

1. **Do not "fix" the 25 `examples/**/*.md` nav entries.** They look broken — `comm` of nav vs
   disk yields 34 alarming diffs — and **all 34 are false positives**. Those pages are generated
   at build time by `website/scripts/convert_notebooks.py` from 39 committed `.ipynb` files, and
   `.gitignore:43` ignores the generated `.md` while negating `index.md` and `*_overview.md`.
   Nav reconciles with **0 true breaks**. Removing the entries would delete 20 real pages from
   the site.
   *Related trap:* two `--` "findings" were correctly ruled out — `guides/index.md:82-84` are
   `--papers/--citations/--output` CLI flags in a bash block, and `data-ingestion.md`'s hits are
   markdown `---` rules outside code fences. Grep for `--` inside ```cypher fences only.

2. **Do not fix B1/B2 by editing the prose alone.** Rewriting `guides/vector-search.md` to say
   "threshold is a maximum distance" documents a genuinely confusing API rather than repairing
   it. A parameter whose meaning flips between *similarity* and *distance* depending on path
   will be mis-documented again. Rename on the dense path (`max_distance`) — or normalize the
   score — and the doc becomes self-correcting. Same for B2: `1/(1+bm25)` labelled `score` and
   sorted `DESC` is a trap regardless of what the doc says. **B2's path is also untested** —
   land the test with the fix.

3. **Do not merge the two API references by hand (S1).** Hand-merging recreates the drift with
   extra steps, and both inputs contain invented methods, so a merge propagates fiction from
   each into one file. Generate from `__init__.pyi` + rustdoc, or pick one and delete the other.
   Do **not** simply add `docs/complete_*.md` to the nav either — it has its own defects (F2,
   `profile_with`).

4. **Do not delete the dated review reports (S4).** They have forensic value and
   `correctness-deferred.md` proves the archive pattern works. Add a
   `> **Status: RESOLVED — snapshot as of <date>, see <commit>**` header, or move to
   `docs/archive/`. Deleting loses the record of *why* the fixes were made.

One more, narrower: **do not resolve S3 by documenting `session.set()` as nonexistent.** A Rust
`Session::set` does exist (returning `()`); the doc bug is the `.build()` chain around it and the
Python side, which has no such method. Check per-language before editing.

---

## 6. Sequenced execution plan

Ordered by harm-per-unit-effort. Each tier is independently landable.

| Tier | Scope | Items | Rationale |
|---|---|---|---|
| **0** | Front doors | A1, A2, A3, A4 | Highest reader volume. A1 is the PyPI landing page; A2 is the literal first thing anyone runs |
| **1** | Silent wrong answers | B1, B2, B3, B5 (+ the renames and the missing fts-score test per §5.2) | Only class that returns plausible wrong data with no error |
| **2** | Cheap repo defects | R1, R4, R5, R6 | R1 blocks a publish; R4 is 5 live 404s; all four are small |
| **3** | Report status headers | S4 — 4 files | One header each. The fork review is actively misleading about data corruption |
| **4** | Plugin authoring surface | D1, D2, D3 (+R3) | Third-party authors have no compiler; D2 is security-relevant |
| **5** | Reference consolidation | S1 decision, then F1, F2 | Decide the single source **before** writing fork docs, or the work is done twice |
| **6** | Fabricated internals | C1-C5 | Deleting a chapter is a legitimate fix here: an absent optimizer chapter beats a fictional one |
| **7** | Mechanical grammar fixes | Tier E, ~60 sites | Cheapest **after** the §7 harness exists, so fixes verify themselves |
| **8** | Long tail | S6 counts, S5 attribution, B4, F3, F4, R2, Black Book LanceDB sweep | Bulk of the line count, lowest per-item harm |

Tiers 0-3 are the release-blocking set. Tiers 4-8 can proceed incrementally.

---

## 7. Guardrails

The audit's own methods, made permanent. Ordered by findings-prevented per line of harness.

1. **Extract-and-parse doctest** — pull every fenced ```cypher / ```locy block from
   `website/docs/**`, `docs/**`, and `skills/**` and assert it parses. **Would have caught all
   of Tier E automatically.** The Locy evaluator built a throwaway version in a single function
   (appended `parse_locy()` to `uni-cypher/src/lib.rs`, ran 12 snippets, reverted); making it
   permanent is a small job. Must also scan Cypher inside Rust/Python string literals — several
   Tier E hits are there, where they are runtime failures.
2. **Compile the Rust snippets** — `trybuild`-style harness over extracted ```rust blocks.
   Catches S2 and most of Tiers C/D.
3. **Assert documented counts against source** — e.g. `PluginSurfaceKind::all().len()`. The
   codebase *already* asserts the structural fact internally (`assert_eq!(kinds.len(), 22)`);
   the docs simply are not wired to it. Kills S6.
4. **CI assertion: `uni_pydantic.__version__ == workspace version`** — recommended by
   `releasing_version_bump.md:45`, never implemented. Prevents R1 recurring.
5. **Generate the API reference** from `__init__.pyi` + rustdoc (S1, F1). Removes the largest
   hand-maintained drift surface in the tree.
6. **Provenance header required on any table of numbers**:
   `Run: <date> · uni <version> · <CPU>/<RAM> · <dataset>` (S5).
7. **Docs checklist in the PR template** for behavior-changing commits (S7) — `fbe60af91` shows
   the discipline works when applied.

---

## 8. Unverified / out of scope

Excluded from all counts above. Listed so the sweep's edges are explicit.

- `bindings/uni-db/REVIEW.md` (917 lines) — not audited, budget.
- `reference/glossary.md` term accuracy; `reference/troubleshooting.md` remedy correctness;
  the schema JSON block at `configuration.md:585-679`.
- "11 providers" / "8 remote API providers" counts in the 5 variant READMEs — internally
  consistent, not checked against `uni-xervo`.
- `uni_pitch_final.md:94,270,274` — "117/117 scenarios / 100% TCK" for OpenCypher **and** Locy.
  Not verified; the TCK was not run. Worth checking before this doc is used externally.
- `website/docs/index.md:29-30,96,101,121-127` — "36 graph algorithms", "8 ANN index
  algorithms", latency table.
- Whether `uni-db = "2"` matches what is actually published on crates.io (no network access).
- The two wasm example READMEs' `cargo build --target …` invocations — verified by path/target/
  script inspection only; not executed.

**Two briefed premises were wrong and were graded against source instead.** Recording them so
the correction propagates:
- *"LocyFold runs in the body plan before IS NOT"* — **inverted**. The anti-join is inside the
  iteration body (`locy_fixpoint.rs:1329-1350`); FOLD is post-fixpoint (`:4724`), i.e. strictly
  after. Confirmed independently.
- The post-fixpoint chain is **PRIORITY → FOLD → HAVING → BEST BY → post-fold projection**
  (`apply_post_fixpoint_chain_inner`, `locy_fixpoint.rs:4589-4780`), not FOLD → HAVING → BEST BY.
  The source carries the comment *"Must run before FOLD"* on the PRIORITY stage.
No doc in the audited set asserted either ordering, so no finding was affected.
