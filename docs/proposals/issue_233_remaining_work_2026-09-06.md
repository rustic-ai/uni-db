# #233 — the remaining work, triaged against source — 2026-09-06

Tier 1 of #233 is closed in `uni-store` / `uni-query` (earlier rounds) and in
the plugin / bulk / CLI / CRDT crates (2026-09-06). This document is the
triage of what is left, and it exists because the previous status was wrong.

> **Correction.** `issue_triage_2026-09-03.md`'s 2026-09-06 status concludes
> that #233's severity is discharged and that it now ranks below #214+#240 and
> #224. That was written from the audits' own Tier 2 / Tier 3 labels.
> Re-triaging those labels against the source found **three Tier 1 sites still
> open**, all three sitting in the Tier 2/3 candidate lists. The ranking above
> is therefore premature; P0 below outranks #214.

Method note, because it is now the fourth time it has mattered: every entry
here was re-tiered against the code rather than accepted from the audit.
That produced **three promotions and nine dismissals** — the labels were wrong
in both directions by roughly equal amounts, the same shape as the Tier 1
round, where one listed site was unreachable, one was a documented contract,
and one was a documented shorthand whose "fix" broke four passing tests.

The discriminator that did most of the work is not the tier but **"does
anything correct this on its own?"** A dropped error the next tick re-attempts
is nearly harmless; the identical line where nothing retries is a permanent
loss. Tier 2/3 splits on *what* was lost (speed vs. status); it does not split
on whether it comes back.

---

## P0 — silent wrong answers — **CLOSED 2026-09-06**

| site | consequence | cost |
|---|---|---|
| ~~`uni-plugin-host/src/notifications.rs:117`~~ **DONE** | `RecvError::Lagged` on the **public** `session.watch()` / `CommitStream` surface (also `PyCommitStream`, `AsyncCommitStream`), over a 256-slot channel. A slow consumer silently loses commits and `next()` returns the following one as if contiguous, so an incremental view built on `watch()` is permanently wrong with no signal. **`CdcRuntime` treats this exact condition as fatal** (`halt_all_streams`); the user-facing stream logs and continues. One mechanism, two opposite policies, and the safe one is on the internal path. | **DESIGN** — needs a lag signal on the API. Breaking: 2 Rust call sites (`session.rs:1233/1239`, `sync.rs:184/189`), 3 Python wrappers, `.pyi` |
| ~~`uni-plugin-rhai/src/manifest.rs:154`~~ **DONE** (declared-but-wrong-typed only; the absent-key default is unchanged and remains a product decision) | `optional_string(&map, "determinism").unwrap_or_else(\|\| "pure")`. A mistyped key or non-string value yields `"pure"` → `Volatility::Immutable`, so DataFusion may constant-fold or CSE a **nondeterministic** Rhai scalar or aggregate. The *value* path is already fail-safe (an unknown string maps to `Volatile`); only the absent / wrong-type path fails open. | **TRIVIAL** — one line, plus the existing test that asserts the `"pure"` default |
| ~~`uni-plugin-host/src/triggers.rs:2005`~~ **DONE** | A deferral sidecar read error logs at `debug!` and returns 0 deferrals; the next `push` then `persist_locked`s the whole in-memory map, **overwriting the rows it failed to read**. The retry is what makes a transient failure permanent. Strictly worse than the scheduler site it resembles: `scheduler.rs:304` is survivable precisely because that path does not rewrite the sidecar. | TRIVIAL–MODERATE — latch a `load_failed` flag and refuse to persist while set, mirroring `persistence_degraded` |

## P1 — no wrong answer, but nothing self-corrects — **CLOSED 2026-09-06**

| site | consequence | cost |
|---|---|---|
| ~~`uni-crdt/src/registry_dispatch.rs:104`~~ **NOT A DEFECT — see below** | **`merge_via_registry` has never dispatched to a shipped provider.** `Crdt::kind()` emits `uni-crdt:g-counter` / `uni-crdt:or-set` / `uni-crdt:lww-register`; `uni-plugin-builtin/src/crdts.rs:34-38` registers `g-counter` / `or-set` / `lww-register`. Every host enabling the builtin CRDT plugin silently gets native merge. No wrong answer *today* — the fallback is the correct native merge — but the feature's whole purpose is to let semantics differ, and a user plugin that genuinely differs is bypassed without a trace. | MODERATE — alias map or an opt-in strict-dispatch flag; ~5 call sites |
| ~~`uni-bulk/src/bulk.rs:1270`~~ **DONE** | `count_rows(...).ok()` persists `row_count_at_build = None`, and `index_rebuild.rs:94-96` gates its growth trigger on `if let Some(built_count)` — so that index **never auto-rebuilds on growth again**, until some later build happens to write a count. | TRIVIAL |
| ~~`uni-plugin-pyo3/src/watchdog.rs:106`~~ **DONE** | `.spawn(...).ok()` — on thread-spawn failure the forced-deadline layer is silently absent, and a pure-Python `while True: pass` guest becomes unbounded, hanging the query. OS-exhaustion-only trigger. | TRIVIAL fix; needs new fault-injection machinery to test |
| ~~`uni-store/src/runtime/l0.rs:433`~~ **DONE** | `merge_via_registry(...).is_ok()` collapses a *provider* failure into the same `false` as a variant mismatch, so the fall-through warns "overwriting CRDT property with a different CRDT variant" — a misattributed cause — and LWW-discards merged state. Masked in-tree today by the kind mismatch above. | TRIVIAL — split the `Result` before the bool |

## The CRDT registry dispatch, reversed

I described this as a dead feature — `merge_via_registry` never dispatching to
a shipped provider — and as a fourth instance of infrastructure wired and never
consumed. **That was wrong, and fixing the names would have been harmful.**

The two sides are deliberately different surfaces that happen to share
`CrdtKind`:

- A provider registered under **`uni-crdt:<kind>`** overrides *host merge* and
  must speak the `Crdt` MessagePack envelope, because that is what
  `to_msgpack` hands it. `uni-store/tests/l0_crdt_registry_dispatch.rs`
  registers exactly such a provider and round-trips through
  `Crdt::from_msgpack` — the feature works and is tested.
- `uni-plugin-builtin`'s **unprefixed** providers (`or-set`, `g-counter`, …)
  are the *plugin CRDT-kind* surface — `empty` / `apply(CrdtOp)` / `value` —
  and speak their own JSON shapes. `OrSetProvider::from_persisted` parses
  `serde_json` of `(Vec<(String, u64)>, Vec<u64>)`.

Renaming either side to make them match would not enable anything:
`from_persisted` would fail on the wrong format, `merge_via_registry` would
return `Serialization`, and the `l0.rs` caller would discard merged state under
a last-writer-wins overwrite. **The namespace is what keeps two incompatible
wire formats from colliding.** The contract is now written at the fallback
site, with a `debug!` so a genuine registration mistake is greppable.

The sibling at `l0.rs:433` was real and is fixed: a provider failure no longer
reports itself as a variant mismatch.

## P2 — observability — **CLOSED 2026-09-06**

~~`cdc_runtime.rs:323` `halted_stream_count()` and `scheduler.rs:378`
`persistence_degraded()` are read by nothing outside their own crates.~~
**Done via metrics**, which needs no new API surface and no breaking change:

- `uni_cdc_stream_halted_total` — incremented on the `false -> true` edge only,
  through a `halt()` helper shared by all three halt sites, so the total stays
  meaningful when `halt_all_streams` sweeps a partly-halted list. The helper
  also logs at `error` with the reason.
- `uni_scheduler_persistence_degraded` — a gauge set on both the healthy and
  the degraded startup path, so its absence means "no scheduler", not "fine".

A `uni.system.*` procedure was considered and not built: it is a larger surface
for the same information, and an operator watching a halted feed wants a
scrape, not a query.

## P3 — trivial honesty fixes — **CLOSED 2026-09-06**

- `uni-plugin-builtin/.../graph_compute/scratch.rs:1153,1165,1175` — a
  `ScratchResponse` that fails to serialize returns the **empty string** to the
  guest. The sibling ABI at `dispatch.rs:299` already emits an in-band
  `{"t":"e",...}`; copy it. Latent (JSON has no NaN literal).
- `uni-plugin-custom/src/{lib.rs:380, procedures.rs:967, :1012}` — three
  `let _ = self.persistence.save(...)`. Self-healing (the restart path
  re-derives the shadow downgrade), so this is an inconsistency with the
  sibling paths that propagate, not data loss.
- `triggers.rs:2117` — drained deferrals stay in a stale sidecar on a write
  failure, so they can fire twice after a restart. Document at-least-once.
- `scheduler.rs:474` — `mark_finished(&id, false)` on a breaker-open skip
  counts a failure for a run that never happened.
- `cdc_runtime.rs:429` — checkpoint write failure logged below default level.
- `shutdown.rs:63` — `let _ = handle.await` makes a panicking driver task
  indistinguishable from a clean exit.
- `uni-plugin-rhai/src/manifest.rs:206,217,228` — `vectorized` / `state` /
  `mode` silently default on a wrong-typed value. Same fix as P0's.
- `uni-plugin-pyo3/src/plugin_handle.rs:81` — a malformed guest version is
  reported as `0.0.0`; `depends_on` then fails loudly, so this is cosmetic.
- `uni-bulk/src/bulk.rs:1425` — `let _ = storage.warm_adjacency(...)`.
  **Demoted from Tier 2**: `uni-query`'s `ensure_adjacency_warmed` warms
  lazily and propagates, so the cost is first-query latency only.
- `uni-plugin-conformance/src/lib.rs:276` — a documented always-pass probe
  inflating the conformance count by one.

## Closed with no work — nine candidates

Recorded because reclassifying is the same work as fixing.

| candidate | why not |
|---|---|
| `graph_compute/session.rs:2508` `expected_len.unwrap_or(0)` | **Unreachable.** The loop at `:2445` sets `Some(t.len())` on the first column, so `None` survives only when `cols` is empty — where a 0 charge is correct. |
| `procedures.rs:403,743,933` "signature persisted as `{}`" | **Unreachable.** The encode inputs are `json!`-built strings and arrays; no NaN can enter, so `to_string` cannot fail. The decode side was fixed on 2026-09-06. |
| `procedures.rs:441,765` "rollback unrecorded" | By design — `let _ = drop_declared(...)` on a path that immediately returns the real error. |
| `scheduler.rs:494,561,485` persistence warns | Self-healing — the next run's `record_started` / `record_finished` overwrites. |
| `scheduler_persistence.rs:117` `_BackgroundJob` mirror | By design, explicitly best-effort; the sidecar is authoritative. |
| `cdc_runtime.rs:346` `discover_new_providers` | Retried every commit. |
| `persistence.rs:67`, `scheduler.rs:121` `let _ = OnceLock::set` | Documented idempotent, single wiring point in `Uni::build`. |
| `scheduler.rs:507` `cancel_token_for().unwrap_or_default()` | Not reachable in practice. |
| `uni-cli/.../semantic_scholar.rs:34-51` `let _ = add_property(...)` | `SchemaManager::add_property` errors **only** on "already exists" and does no type check, so there is no conflict signal being swallowed; a genuine mismatch is caught by the #68 write-time guard. |

**`uni-cli` finishes both rounds at zero real findings.** Worth recording as a
fact about the crate rather than as an absence of looking.

---

## The `CommitStream` decision, as taken

Investigating it changed the answer, so the reasoning is recorded rather than
just the outcome.

The severity was **over-stated**. No doc claims delivery, ordering or
completeness; the only documented loss mode is `debounce`. All six in-repo
callers take exactly one notification, and no test counts commits or asserts
contiguity. So nothing in-tree was getting a wrong answer — what existed was a
public API that silently permits an incorrect usage pattern, with no way to
detect it and (capacity hardcoded twice) no way to avoid it.

The CDC contrast is real but does not carry the conclusion. CDC halts because
**it is a feed**; `watch()` is not documented as one, and a halt-on-lag policy
here would have broken the debounced invalidate-and-re-read use the API was
built for — a fix making the common path worse to protect a path that should
be using CDC.

Rejected: widening `next() -> Option<CommitNotification>` to a `Result`. It is
breaking across 2 Rust call sites, 3 Python wrappers, the `.pyi` and 5 variant
wheels, and it taxes the majority use for a case no in-tree consumer has.

Shipped instead, all additive:

1. `CommitNotification::dropped_before: u64`, matching the `mutations_failed`
   idiom added to the same struct earlier in this cycle — put the
   discriminator on the payload, not in the control flow. Because it is
   attached on delivery and set only from the lag path, a filter skip cannot
   produce a false alarm **by construction**.
2. The delivery contract documented on `next()`, in `rust-api.md` and
   `python-api.md`, each pointing completeness-needing consumers at CDC.
3. `UniConfig::commit_channel_capacity`, replacing the 256 hardcoded at two
   sites.

Left open deliberately: the absent-key `determinism` default of `"pure"` is
documented, tested and shared with the Python binding, so changing it is a
product decision, not a bug fix.

## Sequencing

1. ~~The two trivial P0s~~ and ~~the `CommitStream` decision~~ — **done**.
2. **P1**, with the CRDT registry dispatch split out into **its own issue** —
   it is a dead feature, not a fail-open, and will be lost inside #233. It is
   also the fourth instance this cycle of infrastructure wired and never
   consumed (`ScanRequest::with_limit` #239, `index_consulted` #195,
   `max_impact` #118), which is a class in its own right.
4. **P2**, then **P3** as a single sweep.

#233 itself should close on a judgement about tiers, not by reaching zero.

---

## Closing state — 2026-09-06

**P0, P1, P2 and P3 are all closed.** Tier 1, Tier 2 and Tier 3 of #233 are
now discharged in every crate the issue names, and in the crates it scoped out.

Two P3 candidates were audited and deliberately left alone, for the reason
that closes this document rather than despite it:

- **`uni-plugin-conformance/src/lib.rs:276`** — the always-pass
  `capabilities.declared` probe is documented in place as deliberate,
  reserving a stable `id` for future capability cross-checks. It inflates the
  conformance count by one and says so.
- **The absent-key `determinism` default** of `"pure"` — documented, tested,
  and shared with the Python binding. Changing it is a product decision.

Two more were split rather than uniformly fixed, on the principle this whole
round converged on: **propagate where a caller asked for the work, record
where there is nobody to tell.**

- `uni-plugin-custom/procedures.rs` (x2) propagates, matching its
  `declareFunction` sibling — the user invoked a declare, and the durable half
  failing is theirs to know.
- `uni-plugin-custom/lib.rs:380` records, because it is boot hydration and the
  downgrade is re-derived on every restart. Failing startup over a
  self-healing condition would be worse than the condition.

That distinction, not the tier label, is what did the work throughout: the
useful question was never "how bad is this" but **"does anything correct it,
and is there anyone to tell?"**

---

# The workspace audit — 2026-09-06, later

The section above closed P0–P3 and stated that Tier 1 was discharged "in every
crate". **That was wrong in the same way #233's original scope note was wrong**:
it generalised from the crates that had been audited to the whole workspace.
Audited by then: `uni-store`, `uni-query`, the `uni-plugin*` family (six of
them), `uni-bulk`, `uni-cli`, `uni-crdt`. Never audited: everything else.

Five parallel auditors then covered the remainder. **~22 code-level Tier 1
sites, plus three design calls.** So #233 does not close.

## Findings by crate

### `uni-fork` — 2 Tier 1

| site | consequence |
|---|---|
| `diff.rs:880` | `if let Ok(rs) = primary.query(..)` pre-fetches primary's existing parallel edges for dedup. On failure the set is empty, every fork edge looks new, and promote inserts **duplicate edges on primary** — reported as clean `edges_inserted` with `edges_skipped_duplicate = 0`. |
| `diff.rs:510` | The upsert `(label, ext_id)` resolve. On failure an *edited* fork vertex fails to match its primary twin and is **inserted as a duplicate instead of updating in place**. |
| `diff.rs:838` (Tier 3) | `let (resolved, _degraded)` — the flag is computed and discarded. The identical call at `:703` consumes it. |

The remedy is uniform: `batch_resolve_primary_vids` already returns a
`degraded` flag that `run_promote` surfaces as `vertices_inserted_unverified`
with a `warn!`. All three sites are the same omission relative to that sibling.

### `uni-query-functions` — 4 solid Tier 1, 4 narrower

| site | consequence |
|---|---|
| `datetime.rs:1990`, `:2043` | `datetime(localtime('12:00'))` substitutes **today's wall-clock date**. The same query answers differently tomorrow. `eval_time`, one screen up, errors on the identical condition. |
| `expr_eval.rs:1716`, `:1730` | `left('hello', -1)`: `as_i64` gives `-1`, `as usize` wraps to `usize::MAX`, `take(MAX)` returns **the whole string**. `left('hello', 2.0)` returns `""` (`as_i64` is `None` for Float). `substring` guards both — with a comment describing this exact bug. |
| `df_udfs.rs:2349` | Third decode fallback: the row silently becomes NULL rather than erroring. |
| `df_udfs.rs:5699` | An undecodable `LargeBinary` becomes `Value::Null`, which ranks *smallest*, so `min(p)` returns NULL over a column that has values. |

Narrower: `df_udfs.rs:4621/4650` (truncated bool payload drops a row from
`WHERE`), `datetime.rs:3663` (DST-boundary offset misresolve),
`df_udfs.rs:5687` (NaN compares equal, so `min`/`max` is input-order
dependent). ~170 of 202 raw hits are ordinary correct Rust.

### `uni-algo` — 4 Tier 1

| site | consequence |
|---|---|
| `projection.rs:565` | `if let Ok(Some(batch))` — an IO/decode error on one label's vertex table **drops all that label's vertices**, and PageRank/WCC return confident scores for a subgraph the user never asked for. The only `if let Ok` in the file; its sibling `collect_edges` propagates. |
| `random_walk.rs:47` | A bad `startNodes` entry is dropped; if all are bad the empty vector is treated as "start from all nodes", so `randomWalk(startNodes: ['abc'])` **walks the whole graph**. `astar.rs` and `all_simple_paths.rs` carry comments saying they were changed to propagate exactly this. |
| `node_similarity.rs:36`, `degree_centrality.rs:31` | `metric:'COSIN'` silently returns **Jaccard**; `direction:'INCOMMING'` returns **outgoing** degrees, with `include_reverse(false)` making genuine incoming degrees 0. |
| `projection.rs:981` | A `weightColumn` the edge query does not yield makes **every edge weight 1.0**. The Native path was already hardened for this, with an error whose comment reads *"a column of silent 1.0s that is indistinguishable from real weights"*. The Cypher path never got the guard. |

### `uni-common` — 3 Tier 1, 2 design

| site | consequence |
|---|---|
| `value.rs:775-784` | `canonical_entity` gives an edge map with no `_src`/`_dst` an `Edge{src: Vid::INVALID, dst: Vid::INVALID}` and `type(r) == ""`. So `startNode(r)` returns a **bogus vid instead of the `null` the same map produced before canonicalization** — at ~18 call sites. Its sibling `edge_endpoints` is correct, and the function's own doc says such a map is "left alone rather than guessed at". |
| `value.rs:1876`, `:1897` | `TryFrom<&Value> for Vid`: `Int(-1)` wraps to `Vid(u64::MAX)` = `Vid::INVALID`, returned as `Ok`. `coerce_vid` documents rejecting negatives. |
| `check_constraint.rs:113` **(design)** | A declared `CHECK (age >= 18 AND age < 100)` — 7 tokens against a 3-token parser — returns `Ok(true)` and is **never enforced**; violating rows commit. Documented as deliberate, and rejecting at evaluation time would block legitimate writes. The real gap is that **DDL accepts a constraint it cannot enforce**: fail the declaration, not the write. |
| `cypher_value_codec.rs:476-500` **(design)** | The fast-decode quartet returns `Option`, conflating "wrong tag" with "tag matched, payload corrupt". Downstream `decode_bool(..).unwrap_or(false)` turns a corrupt blob into a false predicate and the row vanishes. The slow-path sibling `decode_msgpack` returns `Result`. ~14 call sites. |

### Plugin loaders — 9 Tier 1

| site | consequence |
|---|---|
| `extism/loader.rs:40` | **No ABI check at all.** `abi_extism` is parsed and read by nothing in the repo, so a guest declaring `"abi-extism":"^9"` loads against a v1 host. `uni-plugin-wasm` does `AbiRange::parse` + `major_for_abi` and returns `AbiUnsupported`. Same family as the `AbiRange` → `STAR` defect fixed this session, with validation not weakened but simply absent. |
| `extism/exports.rs:40/47`, `wasm/loader.rs:91/98` | `default_volatility() = "immutable"` — a registration omitting `volatility` lets DataFusion constant-fold a **nondeterministic guest function**. Verbatim the Rhai `determinism` defect fixed this session. Both loaders *reject* a bad value and *accept* an absent one. |
| `extism/loader.rs:47` | `determinism` also parsed and never read; the Extism loader never synthesizes a `PluginManifest`, so host-level determinism / signature / hash policy never sees an Extism plugin. |
| `wasm-rt/ipc.rs:64` | The FU-2 secret-handle membrane walks Struct / List / LargeList / FixedSizeList / Map. `Union` and `RunEndEncoded` also carry nested `Field`s and the guest controls the IPC schema, so a secret-handle column buried in a Union child crosses unrejected. There is a test for the Struct case and none for Union. |
| `apoc-core/convert.rs:158` | `apoc.convert.toString([1,2,3])` returns **NULL** where Neo4j returns `"[1, 2, 3]"` — everything but Bool/Int/Float/Utf8 hits the catch-all. The highest-impact APOC finding: an everyday ported query turns real data into NULL. |
| `apoc-core/create.rs:149`, `text.rs:335` | `apoc.create.uuids(2_000_000)` silently returns 1,000,000 rows; `apoc.text.repeat` silently truncates. The caps are deliberate; the silence is the defect. |
| `apoc-core/text.rs:345` | `indexOf` returns a UTF-8 **byte** offset where Neo4j returns a character index (`indexOf('cafés','s')` → 5 vs 4), and `text.length` counts scalar values, so the pair is not even self-consistent. |

Two were explicitly **not** flagged despite matching the shape:
`wasm/loader.rs:899` `let _ = store.set_fuel(..)` and `multi_version.rs:150`
`to_string(caps).unwrap_or_default()` are unreachable as the code stands —
but each is one refactor away from failing open silently (a dropped fuel cap;
a linker cache key collapsing across capability sets).

### Clean, with the mechanism stated

`uni-cypher` (the parser is error-returning end to end; its one dangerous
decode is unreachable because nothing constructs `CypherLiteral::Bytes`),
`uni-locy` (its one "missing marginal becomes certain" default is fenced off
because the sole caller inserts a weight for every RV it interns),
`uni-sidecar` (`load()` already distinguishes `NotFound` → default from a real
IO error → typed `Err`; both known defects were caller-side), `uni-btic` (pure
in-memory codec, no IO).

## What this audit is actually evidence for

**One mechanism explains nearly all 22, and in every crate the evidence was a
sibling that already does it right.**

| crate | correct sibling | site that didn't get it |
|---|---|---|
| `uni-algo` | `collect_edges` propagates | `collect_vertices` swallows |
| `uni-algo` | `astar` / `all_simple_paths` propagate, with comments | `random_walk` drops |
| `uni-algo` | Native weight path errors, with the defect named | Cypher weight path defaults to 1.0 |
| `uni-query-functions` | `eval_time` errors | `eval_datetime` substitutes `Utc::now()` |
| `uni-query-functions` | `substring` guards, with the bug described | `left` / `right` do not |
| `uni-common` | `edge_endpoints` returns Options | `canonical_entity` invents endpoints |
| `uni-common` | `decode_msgpack` returns `Result` | the fast-decode quartet returns `Option` |
| `uni-fork` | `batch_resolve_primary_vids` returns `degraded` | two callers ignore it, one discards it |
| loaders | `uni-plugin-wasm` enforces ABI | `uni-plugin-extism` reads the field nowhere |

So this is not carelessness. **The class keeps being fixed one site at a time,
and the un-fixed sibling is what makes the next instance findable.** That is
Class 1's founding observation, still producing instances in crates the class
review never reached — which is exactly why its scope note mattered.

Three of these are the *same* defect as one fixed earlier this session:
`volatility` defaulting to `Immutable` on a missing field, in Rhai (fixed),
Extism and Wasm. All three reject a bad value and accept an absent one. That
one is greppable and belongs in `arch_fail_open.rs` as a fourth rule.

## Recommended handling

1. **File this scope as its own issue.** #233 opened claiming 27 sites; ~45
   have been fixed and ~22 more are here, in crates its scope note never
   named. Tracking by that number has now misled twice. Unlike the original
   speculative note, this audit is complete and reproducible, so it meets the
   repo's "a bug you can reproduce" bar.
2. **Fix order**: the three-loader `volatility` default and the Extism ABI gap
   first (safety, and one rule covers the first); then `uni-fork`'s promote
   duplicates and `uni-common`'s `canonical_entity`, both of which corrupt data
   or manufacture a wrong answer from a correct one; then `uni-algo` and
   `uni-query-functions`, which are mostly one-line `?` changes inside
   functions that already return `Result`; then the APOC divergences.
3. **Extend the ratchet** with the `volatility`/`determinism` rule.
4. `check_constraint` and the codec quartet are **design calls**, not patches.
