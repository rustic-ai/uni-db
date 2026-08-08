# Documentation remediation — plan of action

> **Status: RESOLVED — historical snapshot as of 2026-08-07.**
> All five workstreams shipped. §6 below records what landed, the four decisions
> from §5, and the residuals that a follow-up sweep closed on 2026-08-07. The
> workstream text is preserved as written; line numbers have drifted.

**Status:** complete (see §6) · **Date:** 2026-08-07
· **Input:** `documentation_remediation_2026-08-06.md` (audit, evidence-verified)
· **Baseline:** local `main` `e8c548eaf` (v3.3.0) — one commit ahead of the audit's `d8f6f3abc`.

This is the execution plan for the audit. It does not restate findings; it decides **order,
grouping, ownership boundaries, and the gate that closes each workstream**. Read the audit
first for the evidence.

---

## 0. Deltas from the audit

Re-verified against `e8c548eaf` before planning. Three changes:

| Audit item | Status now | Evidence |
|---|---|---|
| **R2** (uni-tck 4 test binaries) | **ALREADY FIXED — drop from scope** | `crates/uni-tck/Cargo.toml:7` has `autotests = false`; the three `repro_*.rs` are `mod`-included from `tests/integration.rs:16,18,20`. Landed in `e8c548eaf`, after the audit baseline |
| **B1** (threshold direction) | **Worse than reported — rescoped from doc-fix to source-fix** | The split is *intra-procedure*, not cross-page: `uni.vector.query` treats `threshold` as min-similarity at `search_procedures.rs:1286` and as max-distance at `:1361-1362`. Source comment at `:1238` acknowledges it |
| **B2** (fts score inverted) | Confirmed; fix is smaller than feared | `:1455` passes `DistanceMetric::L2` → `calculate_score` maps L2 to `1/(1+d)` (`similar_to.rs:286-292`). A **correct** BM25 normalizer already exists directly below it (`score/(score+fts_k)`). The fix is routing to the existing function, not writing new math |

All line numbers in the audit have drifted by ~40-60 in `search_procedures.rs`. Cite by symbol,
not line, when fixing.

---

## 1. The reordering, and why

The audit's §6 sequences by harm-per-effort and lands the guardrails (§7) at the end. That is
inverted for two of the eight tiers.

**Move the doctest harness to the front.** Tier 7 is ~60 mechanical grammar sites across 8+
files. Fixing those by hand and *then* building the parser harness means verifying twice — once
by eye, once by machine — and the by-eye pass is exactly the process that produced the defects.
Build the harness first and Tier 7 becomes: run it, fix what it reports, run it again. The audit
says so itself (§7.1: "would have caught all of Tier E automatically") and notes the Locy
evaluator already built a throwaway version in one function.

**Everything else keeps the audit's order.** Harm-per-effort is the right metric and §6 applies
it correctly.

The result is five workstreams. W0 is a prerequisite for W4; W1/W2/W3 are independent and can run
in parallel.

---

## 2. Workstreams

### W0 — Harness (prerequisite for W4, gates everything after)

Build the checks before the edits, so the edits verify themselves and cannot regress.

| # | Check | Kills | Notes |
|---|---|---|---|
| W0.1 | Extract-and-parse every ` ```cypher ` / ` ```locy ` fence across `website/docs/**`, `docs/**`, `skills/**`; assert it parses | **All of Tier E** | Must also scan Cypher inside Rust/Python **string literals** — several Tier E hits are there, where they are runtime failures, not doc-block noise |
| W0.2 | Assert documented counts against source | **S6** | `PluginSurfaceKind::all().len()` is *already* asserted internally at `surfaces/mod.rs:1276`. Wire the docs to the same fact rather than adding a second source of truth |
| W0.3 | CI: `uni_pydantic.__version__ == workspace version` | **R1 recurrence** | Recommended by `releasing_version_bump.md:45`, never implemented. The warning came true — implement it in the same PR that fixes R1 |

Deferred from W0 (real, but larger than this remediation): `trybuild` over ` ```rust ` fences
(§7.2), and generated API reference (§7.5 — see W3 gate). Note them as follow-ups; do not let
them block.

**Gate:** harness runs green in CI, with a documented count of currently-failing sites (expected
non-zero — that list *is* W4's worklist).

---

### W1 — Source defects (code PRs, not doc PRs)

Separate from all doc work: these change runtime behavior and need review as such.

- **B1 — `threshold` means two opposite things inside one procedure.** Pick one meaning for
  `uni.vector.query` and make both paths honor it. Per §5.2, do **not** resolve this in prose:
  a parameter that flips between similarity and distance by path will be mis-documented again.
  Either normalize the dense path's score, or split the name (`max_distance`). Land a test that
  asserts both paths agree.
- **B2 — FTS score inverted.** Route FTS through the existing BM25 normalizer instead of
  `DistanceMetric::L2`. **Land the test with the fix** — §5.2 flags that no test asserts
  `fts.query` score value or ordering, so this path is untested as well as mis-documented. The
  missing test is the reason the bug survived.
- **B3 — FTS `threshold` filters raw BM25** while the yielded score is transformed: different
  scale *and* direction. Falls out of B2; fix together.
- **R3 — `verify_hash_pin` has zero call sites** yet `plugin_trust.rs:78` claims it "is applied
  separately at load sites". Either wire it in or delete it and correct the claim. **Security-
  relevant** — pairs with D2 in W2, so decide R3's disposition *before* writing D2's prose.

**Gate:** `cargo nextest run` green, new tests present for B2 and B1.

---

### W2 — Front doors + release hygiene (highest reader volume, cheapest fixes)

Audit tiers 0, 2, 3, 4 collapsed into one workstream — they share no dependencies and are all
small.

- **A1** `bindings/uni-db/README.md` — the `pip install uni-db` landing page, still on the removed
  1.x API. Note it is **half-migrated**: the Forks section at `:152+` is already correct and shows
  the target style. Fix the rest to match.
- **A2** `getting-started/quickstart.md` — fails on the literal first thing anyone runs. The six
  command-result pyclasses are `frozen`; `.get()` does not exist. **§5 warns the naive fix is
  wrong** — read it before touching this.
- **A3, A4** — root `README.md` version (`"2"` vs 3.3.0) and `crates/uni/README.md` (6 nonexistent
  symbols, wrong crate name and version).
- **R1** — `uni-pydantic` `__version__ = "2.5.0"` vs workspace 3.3.0. **Blocks the next publish.**
  Ship with W0.3 so it cannot recur.
- **R4** — `docs/migrations/0.9.0-wheel-matrix-collapse.md` was never committed (`git ls-files
  docs/migrations` → empty) but is linked from all 5 variant wheel READMEs = **5 live 404s on
  PyPI**. Recover it from `.claude/worktrees/tx_commit_profile/` and commit, or delete the links.
- **R5** — release notes missing for 2.4.0, 3.0.1, 3.1.0, 3.3.0. Confirmed: newest file is
  `RELEASE_NOTES_3.2.0.md`, and `v2.4.0`/`v3.0.1` are tagged with no notes.
- **R6** — no `rust-version` key despite `edition = "2024"` (needs 1.85+); README badge says 1.75+.
  One line in `Cargo.toml` plus a badge edit.
- **S4** — status headers on 4 dated reports. `fork_production_readiness_review_2026-06-20.md:7`
  still leads with "NOT production-ready" and a CRITICAL data-corruption finding that **is fixed**.
  **§5.4: do not delete these** — add `> **Status: RESOLVED — snapshot as of <date>, see
  <commit>**`. `docs/correctness-deferred.md` is the exemplar to copy.
- **D1, D2, D3** — plugin authoring surface. D1: four traits deleted in 3.0 still documented, and
  the two live surfaces (`locy_generator`, `replacement_scan`) omitted. D2: documents an `ed25519`
  Cargo feature and degraded fallback that **never existed** — a reader could audit for a flag
  that was never real. Sequence D2 after W1's R3 decision.

**Gate:** every changed page's snippets pass W0.1; R1 passes W0.3; links resolve.

---

### W3 — Reference consolidation (one decision, then execution)

**This is a decision, not a task, and it gates F1.** Two hand-maintained references, 10,054 lines,
neither generated, drifting independently — each has invented methods the other lacks. The
published one is the *weaker* one (Python 76% vs 87%, Pydantic 64% vs **97%**).

§5.3 rules out both naive moves: hand-merging propagates fiction from each input into one file,
and simply adding `docs/complete_*.md` to the nav ships its own defects (F2, `profile_with`).

Decide between:
- **(a) Generate** from `__init__.pyi` + rustdoc — kills the drift surface permanently, larger job.
- **(b) Pick one, delete the other** — cheap, but the drift mechanism survives.

**Do not write F1 before this is decided**, or the entire fork API gets documented twice. F1 is
substantial: `grep -ci fork website/docs/reference/python-api.md` → **0** (re-verified). 11 `Uni`
methods, 3 `Session` methods, both sync and async, plus 29 of 60 Rust `Uni` methods.

Then: **F1** (fork API), **F2** (`ssi_enabled` absent from `complete_rust_api.md` despite SSI
being default-on).

**Gate:** decision recorded in this file before F1 work starts.

---

### W4 — Mechanical + long tail (runs after W0)

- **Tier E** — ~60 sites. Worklist comes from W0.1's failure output, not from re-reading the audit
  table. **§5.1 trap:** grep for `--` **inside ```cypher fences only** — `guides/index.md:82-84`
  are CLI flags and `data-ingestion.md`'s hits are markdown `---` rules. Both are false positives.
- **Tier C (C1-C5)** — fabricated architecture. §6 is right that **deleting a chapter is a
  legitimate fix**: an absent optimizer chapter beats a fictional one. C1 is ~650 lines describing
  an optimizer that does not exist (optimization is delegated to DataFusion). C4 ("built on
  sqlparser") re-verified: **0 hits** in any `Cargo.toml` — the parser is pest. C5 re-verified:
  **no `crdt.*` function registered anywhere**.
- **S5** — provenance headers on every table of numbers. `benchmarks.md` is 4 months stale;
  `demos/demo01/spec.md:605-627` cites a 2.3M-paper corpus when `generate_data.py:64` sets
  `NUM_PAPERS = 5000`. Also `benchmarks.md:319` prints a command for a `pushdown_performance`
  bench target that does not exist.
- **S6** counts (fixed permanently by W0.2), **S7** (Locy QUERY docs now teach the *inverted*
  model after `2f3c0802f`), **S8** (27 Black Book "LanceDB" sites — `lancedb` is not a dependency;
  root `Cargo.toml:132` pins `lance = "7.0.0"`), **B4**, **F3**, **F4**.
- **`crates/uni-tck/README.md` under-claims** — says "Phase 2+ TODO", "many steps use `todo!()`",
  "will panic"; `src/` has zero `todo!()` and the newest report is 3925/3926 (100.0%). Cheap, and
  the only finding where the docs are *pessimistic* about a shipped surface.

**Gate:** W0.1 and W0.2 green with zero failures.

---

## 3. Traps — read before touching the relevant item

Reproduced from audit §5 because each one is a case where the *obvious* fix is wrong:

1. **The 25 `examples/**/*.md` nav entries are NOT broken.** All 34 apparent diffs are false
   positives — the pages are generated at build time by `website/scripts/convert_notebooks.py`
   and `.gitignore:43` ignores the generated `.md`. **Removing them deletes 20 real pages.**
2. **B1/B2 are not prose bugs** (see W1).
3. **Do not hand-merge the two API references** (see W3).
4. **Do not delete the dated reports** (see W2/S4).
5. **`session.set()` exists in Rust but not Python.** S3's doc bug is the `.build()` chain around
   it, plus the Python side. **Check per-language before editing** — the naive "document it as
   nonexistent" fix is wrong for Rust.

---

## 4. Sequencing

```
W0 (harness) ─────────────┬──────────────────────► W4 (mechanical + long tail)
                          │
W1 (source: B1/B2/B3/R3) ─┼─► [R3 decision] ─► W2's D2
                          │
W2 (front doors, release) ┘

W3 (reference) : [decide (a) vs (b)] ─► F1, F2
```

- **W1, W2, W3-decision can start immediately and in parallel.** Only W2's D2 waits on W1's R3.
- **W4 waits on W0.** That is the whole point of the reordering.
- **F1 waits on the W3 decision**, or it gets written twice.

Release-blocking set: **W1 + W2** (the audit's tiers 0-3 plus the plugin surface). W3 and W4
proceed incrementally after.

---

## 5. Open decisions — need a call before the dependent work starts

1. **W3: generate the API reference, or pick-one-and-delete?** Gates F1, the single largest
   remaining doc job.
2. **W1/B1: rename the parameter, or normalize the dense score?** Rename is a breaking API change
   and needs a release-notes entry; normalize is silent but changes returned values.
3. **W1/R3: wire `verify_hash_pin` in, or delete it?** Security-relevant, and D2's prose depends
   on the answer.
4. **W2/R4: recover the migration doc from the worktree, or drop the 5 links?** Recovery is
   better if the content is sound — it is currently unread.

---

## 6. Outcome — decisions taken and what landed

Added 2026-08-07, after the fact. §5's gate ("decision recorded in this file before F1 work
starts") was not honored at the time: all four questions were answered in code and none was
written down here. Recorded now so the record matches the tree.

### 6.1 The four decisions

| §5 | Decision | Commit |
|---|---|---|
| 1. Reference: generate vs pick-one | **Generate** from `__init__.pyi` — 190 classes vs the 76%/87% the hand-written pages reached. The three `docs/complete_*.md` were retired to stubs (6368 → 52 lines) rather than merged, since merging two drifting sources propagates the fiction in each | `0d591ca20` |
| 2. B1 `threshold`: rename vs normalize | **Normalize**, shipped as breaking (`fix(query)!: make search scores and thresholds monotone in relevance`) with the Python threshold tests updated in `0597a7a0e` | `ed965c716` |
| 3. R3 `verify_hash_pin`: wire in vs delete | **Wired in** — an artifact hash-pin allowlist enforced at load sites, which also settled D2's prose | `bf4d50bad` |
| 4. R4 migration doc: recover vs drop links | **Recovered** — `docs/migrations/` is tracked; the 5 wheel-README links resolve | `97d2a0b05` |

### 6.2 Workstreams

W0 harness `7bfff4abb` (grammar) + `0d591ca20` (version/count/symbol gates) · W1 `ed965c716`,
`bf4d50bad` · W2 `f39ae75c7`, `97d2a0b05`, `f4474c819` · W3 `0d591ca20` · W4 `f39ae75c7`,
`639eeab24`.

### 6.3 Corrections to this document

- **§0's R2 row misattributes the fix.** `autotests = false` is real, but the tck consolidation
  landed in `639eeab24`, *after* the stated `e8c548eaf` baseline — not in it.
- **§2/W4's C4 line over-reads its grep.** `grep sqlparser **/Cargo.toml` → 0 is accurate and the
  "parser built on sqlparser" doc claim is genuinely false, but sqlparser *is* in `Cargo.lock`
  transitively via DataFusion (~9.4 MiB of the shipped wheel). The correct phrasing is "the Cypher
  parser is pest; sqlparser enters only through DataFusion."

### 6.4 Residuals closed 2026-08-07

A verification sweep against `main` found six items the workstreams left open:

- **B5** — `k` documented with a default it does not have. `uni.search` reads it via
  `require_int_arg(args, 4, …)`; omitting it is a hard error. Fixed in `hybrid-search.md`, the
  skill's `vector-hybrid-search.md` and `cypher.md`, including the signature lines that bracketed
  `k` as optional. The RRF `k` (genuinely `default 60`) was left alone.
- **S6** — the count gate was surface-only. Generalized to seven families with path scoping, since
  "N scenarios" means different totals for the openCypher and Locy suites. This surfaced **more
  drift than the audit found**: `uni-tck/README.md` contradicted itself (220 vs 221 files; 1,339 vs
  3,926 scenarios, both in the same file); `complete_locy.md` claimed 37/273 for a Locy suite that
  is 70/519; the Black Book, the landing page and the pitch claimed 35, 36 and 36 graph algorithms,
  none of which was 42. A new `builtin_algorithm_count_is_pinned` test pins the algorithm count,
  because no source assertion existed to read.
- **S5** — `demos/demo01/spec.md` retitled to "Design Target" with one admonition covering the
  table, plus notes on the two transcript blocks. `internals/benchmarks.md` keeps its provenance
  disclosure; regenerating the numbers is filed as follow-up, not done here.
- **S8** — the Black Book's dependency table listed `lancedb`, which is not a dependency. The
  other `LanceDbStore` hits in that file are a real type and were left alone.
- **D1 remainder** — not a defect: `plugins/concepts.md` already lists both live surfaces in its
  table. Two other defects in that section were fixed instead — a duplicated clause, and an
  Algorithm built-ins count of "label propagation + 36 via adapter".

**Guardrail note.** The generalized gate was verified by falsification, not by observing it pass:
reverting one corrected number makes it exit 1 naming the file and line. An earlier version of the
algorithm-count test passed vacuously with zero, because `BuiltinPlugin::register` deliberately
does not register `uni.algo.*` — the host does, under the `uni` plugin id. A gate only ever seen
green has not been tested.

**Scenario counts are computed, not read from the compliance reports.** The first version of the
gate took its TCK totals from `compliance_reports/*/last_run_report.md`. Running the TCK lanes
falsified that: the Locy harness runs **519** scenarios while its report — generated 2026-06-12 —
says 501. Anchoring a gate to a hand-regenerated artifact would have "corrected" the docs *to a
stale number*, which is the same drift class this whole remediation exists to close. The gate now
expands the `.feature` files directly (a `Scenario:` is one; a `Scenario Outline:` is one per
Examples data row), verified exact against both live harnesses: openCypher 3926, Locy 519. The
openCypher report happened to be current, so checking only that suite would have hidden the flaw.
