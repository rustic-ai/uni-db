# Fail-open remediation: the workspace remainder — plan — 2026-09-06

## Status — all 21 items implemented, 2026-09-06

Every item in Phases 1-5 is fixed and every decision D1-D4 is taken (see
*Decisions taken*, below). Each fix carries a regression test that was
**verified to fail with the fix reverted**; the observed failure was recorded
before the fix was restored. Six of the audit's claims changed under
verification and are listed in *Corrections found while implementing*.

| gate | result |
|---|---|
| `uni-db` (facade integration) | 2827 passed, 0 failed, 95 skipped |
| `uni-store` / `uni-bulk` / `uni-algo` / `uni-plugin-{apoc-core,extism,wasm-rt}` / `uni-query` | 1755 passed, 0 failed |
| `uni-common` / `uni-fork` / `uni-query-functions` | 488 passed, 0 failed |
| `cargo clippy --all-targets` (7 touched crates) | zero warnings |
| `cargo fmt --all --check` | clean |

22 new test functions; 32 files changed. P1-P5 ship as one `feat!` with a
release note. Not committed — awaiting review.

## What this is

#233 ("a failure on a read/decode/index path is swallowed and a default
returned") has been closed, tier by tier, in `uni-store`, `uni-query`, the six
audited `uni-plugin*` crates, `uni-bulk`, `uni-cli` and `uni-crdt`. A
five-auditor pass then covered **every remaining crate in the workspace**. This
document is the resulting site inventory and the plan to work it.

It exists as a separate file because #233 should not absorb it. That issue
opened claiming 27 sites; roughly 45 have been fixed and the ~20 below are in
crates its scope note never named. Tracking the work by that number has misled
twice already.

**Provenance.** Read-only audit, five parallel readers, one per crate group,
each required to state the wrong answer a user would observe before calling
anything Tier 1, and told the measured prior: in four earlier audits of this
class about a third of candidate Tier-1 calls were wrong. Two candidates were
re-checked by hand before this plan was written, and **one of them downgraded**
(see *Corrections*, below). Anything here is worth re-verifying against source
before it is worked — that has changed the answer four times in this cycle.

## The mechanism, which decides the plan

In **every** crate, the evidence for a finding was a sibling that already does
it right:

| crate | correct sibling | site that did not get it |
|---|---|---|
| `uni-algo` | `collect_edges` propagates | `collect_vertices` swallows |
| `uni-algo` | `astar` / `all_simple_paths` propagate, with comments saying so | `random_walk` drops |
| `uni-algo` | the Native weight path errors, its comment naming the defect | the Cypher weight path defaults to 1.0 |
| `uni-query-functions` | `eval_time` errors on a missing component | `eval_datetime` substitutes `Utc::now()` |
| `uni-query-functions` | `substring` guards a negative length, with a comment describing this bug | `left` / `right` do not |
| `uni-common` | `edge_endpoints` returns `Option` per endpoint | `canonical_entity` fabricates `Vid::INVALID` |
| `uni-common` | `decode_msgpack` returns `Result` | the fast-decode quartet returns `Option` |
| `uni-fork` | `batch_resolve_primary_vids` returns a `degraded` flag | two callers ignore it, one discards it |
| loaders | `uni-plugin-wasm` calls `major_for_abi` | `uni-plugin-extism` reads its ABI field nowhere |

This is not carelessness. **The class has only ever been fixed one site at a
time, and the un-fixed sibling is what makes the next instance findable.** The
plan is therefore ordered by mechanism, not by crate, and every phase ends by
asking whether the fix can be made structural rather than repeated.

---

## Corrections to the audit, made before planning

**The loader `volatility` default is NOT the Rhai defect, and is downgraded.**
The auditors reported `extism/exports.rs:47` and `wasm/loader.rs:98` as
"verbatim the Rhai `determinism` defect". Checked against source:

- The Rhai bug had two halves. A **present-but-wrong-typed** value was silently
  discarded (fixed), and an **absent** key took a documented default (left
  alone deliberately — documented, tested, shared with the Python binding's
  `determinism: str = 'pure'`).
- In Extism and Wasm the field is `volatility: String` behind
  `#[serde(default)]`. A wrong type is already a serde error, so the half that
  was a bug **cannot occur here**. Only the absent-field default remains, and
  it is documented in rustdoc ("Default `\"immutable\"`"), asserted by a test
  (`exports.rs:289`), and shown in three user-facing docs pages.

So this is the same *product decision* I declined to take unilaterally for
Rhai, now visible on four surfaces at once. It moves out of the fix list and
into **Decision D3**.

**The Extism ABI gap is confirmed and stands.** `abi_extism` appears at exactly
two sites: its declaration (`loader.rs:41`) and a `None` in a test
(`loader.rs:433`). Nothing reads it. `uni-plugin-wasm` calls `major_for_abi`
and returns `AbiUnsupported`.

---

## Phase 1 — safety properties that fail open

| id | site | consequence | fix | test |
|---|---|---|---|---|
| S1 | `uni-plugin-extism/src/loader.rs:41` | **No ABI check exists on the Extism path.** A guest declaring `"abi-extism":"^9"` loads against a v1 host and calls a mismatched host-fn surface. The `determinism` field is likewise parsed and never read, and the loader never synthesizes a `PluginManifest`, so host determinism / signature / hash policy never sees an Extism plugin at all. | Mirror `uni-plugin-wasm`: `AbiRange::parse` + `major_for_abi`, reject with an `AbiUnsupported`-equivalent. Decide separately whether an absent `abi_extism` is an error or assumes `^1` (see D4). | Load a manifest declaring an unsupported major; assert refusal. Existing loader tests are the seam. |
| S2 | `uni-plugin-wasm-rt/src/ipc.rs:64` | The FU-2 secret-handle membrane walks Struct / List / LargeList / FixedSizeList / Map and returns `Ok(())` for everything else. `DataType::Union` and `RunEndEncoded` also carry nested `Field`s, and **the guest controls the IPC schema** on `decode_batches`, so a secret-handle-tagged column buried in a Union child crosses unrejected. | Add the two arms. Consider inverting to a deny-by-default walk so a future Arrow nested type cannot reopen this. | There is a test for the Struct case (`encode_batch_rejects_secret_handle_inside_struct`) and none for Union — add the Union twin. |

Phase 1 first because these are properties, not answers: a wrong answer is
bounded by the query that asked for it; a bypassed ABI or membrane is not.

## Phase 2 — data corruption and manufactured wrong answers

| id | site | consequence | fix | test |
|---|---|---|---|---|
| C1 | `uni-fork/src/diff.rs:880` | `if let Ok(rs) = primary.query(..)` pre-fetches primary's parallel edges for dedup. On failure the set is empty, every fork edge looks new, and promote **inserts duplicate edges on primary** — reported as clean `edges_inserted`, `edges_skipped_duplicate = 0`. Durable; not self-healing. | Thread a `degraded` bool out, as `batch_resolve_primary_vids` already does; add `edges_inserted_unverified` to `PromoteReport`. | `ForkQueryHost` stub returning `Err` for a matching Cypher prefix — the trait is the existing seam. |
| C2 | `uni-fork/src/diff.rs:510` | `batch_resolve_primary_by_ext_id` swallows a query failure into an empty map, so an *edited* fork vertex fails to match its primary twin and is **inserted as a duplicate instead of updated in place**. Unlike its sibling it returns no `degraded` flag, so `vertices_inserted_unverified` stays 0. | Return `(map, degraded)`. | As C1. |
| C3 | `uni-fork/src/diff.rs:838` | `let (resolved, _degraded)` — the flag is computed and thrown away. The identical call at `:703` consumes it. Edges land in `edges_skipped_no_endpoint`, telling the user to promote endpoints that already exist. | Consume it. | As C1. |
| C4 | `uni-common/src/value.rs:775-784` | `canonical_entity` gives an edge map lacking `_src`/`_dst` an `Edge{src: Vid::INVALID, dst: Vid::INVALID}` and `type(r) == ""`. `startNode(r)` then returns a **bogus vid instead of the `null` the same map produced before canonicalization** — at ~18 call sites in `uni-query`. The function's own doc says such a map is "left alone rather than guessed at"; its sibling `edge_endpoints` is correct. | Return `self` unchanged when the endpoints or type are not recoverable. No signature change. | Unit test beside the existing `canonical_entity` tests. |
| C5 | `uni-common/src/value.rs:1876`, `:1897` | `TryFrom<&Value> for Vid`/`Eid`: `Int(-1)` wraps to `Vid(u64::MAX)` = `Vid::INVALID` and returns `Ok`. `coerce_vid` documents rejecting negatives. | One-line `if *i >= 0` guard. | Unit test. |

C4 is the sharpest shape in the whole audit: a layer added to make identity
*consistent* converts a correct `null` into a plausible lie.

## Phase 3 — wrong answers from user-facing functions

| id | site | consequence | fix | test |
|---|---|---|---|---|
| Q1 | `uni-query-functions/src/expr_eval.rs:1716`, `:1730` | `left('hello', -1)`: `as_i64` → `Some(-1)`, `as usize` wraps to `usize::MAX`, `take(MAX)` returns **the whole string**. `left('hello', 2.0)` → `""`, because `as_i64` is `None` for `Float`. `substring` guards both, with a comment describing this exact bug. | Mirror `substring`. Fn already returns `Result`. | Pure unit test; **no database needed**. |
| Q2 | `uni-query-functions/src/datetime.rs:1990`, `:2043` | `datetime(localtime('12:00'))` substitutes **today's wall-clock date** — the same query answers differently tomorrow. `eval_time` errors on the identical condition. | `ok_or_else` matching `eval_time`. | Pure unit test. |
| Q3 | `uni-query-functions/src/df_udfs.rs:2349` | Third decode fallback: a row silently becomes NULL instead of erroring. Enclosing fn returns `DFResult`. | One line. | Feed a non-codec blob. |
| Q4 | `uni-query-functions/src/df_udfs.rs:5699` | An undecodable `LargeBinary` becomes `Value::Null`, which has `cypher_type_rank` 0 — the *smallest* — so `min(p)` returns NULL over a column that has values. | MODERATE: 7 call sites, several inside `Accumulator::update_batch` (which does return `DFResult`). | `_cypher_min` over a raw-bytes column. |
| Q5 | `uni-query-functions/src/df_udfs.rs:5687` | `partial_cmp(..).unwrap_or(Ordering::Equal)` makes NaN equal to everything, so `min`/`max` returns an input-order-dependent element. | MODERATE, one UDAF. | Unit test. |
| Q6 | `uni-query-functions/src/datetime.rs:3663` | `truncate_time` unconditionally uses `Utc::now().date_naive()` for a `Temporal`, misresolving a **named-timezone** offset across a DST boundary. The `Value::String` arm below parses the date out correctly. | Use the value's own date. | Needs a DST-crossing fixture. |
| Q7 | `uni-query-functions/src/df_udfs.rs:4621`, `:4650` | `decode_bool(..).unwrap_or(false)` after a matched tag, so a truncated payload silently makes a `WHERE` predicate false and **the row vanishes**. | Depends on D2 — this is the call site that makes the codec quartet's `Option` dangerous. | Hand-built truncated blob. |

## Phase 4 — graph algorithms returning confident wrong scores

| id | site | consequence | fix |
|---|---|---|---|
| A1 | `uni-algo/src/algo/projection.rs:565` | `if let Ok(Some(batch))` — an IO/decode error on one label's vertex table **drops all that label's vertices**, and PageRank/WCC then score a subgraph the user never asked for. The only `if let Ok` in the file; `collect_edges` propagates. | `?`. TRIVIAL. |
| A2 | `uni-algo/src/algo/cypher/random_walk.rs:47` | A bad `startNodes` entry is dropped; if all are bad the empty vector is treated as "start from all nodes", so `randomWalk(startNodes: ['abc'])` **walks the whole graph**. `astar` and `all_simple_paths` use `parse_vid_arg` for exactly this. | Use `parse_vid_arg`. TRIVIAL. |
| A3 | `uni-algo/src/algo/cypher/node_similarity.rs:36`, `degree_centrality.rs:31`, `:45` | `metric:'COSIN'` silently returns **Jaccard**; `direction:'INCOMMING'` returns **outgoing** degrees, and `include_reverse(false)` makes genuine incoming degrees 0. | Reject unknown enum strings in `to_config`. TRIVIAL. |
| A4 | `uni-algo/src/algo/projection.rs:981` | A `weightColumn` the edge query does not yield makes **every edge weight 1.0**; Dijkstra/MST/Louvain return plausible unweighted answers. The Native path was hardened for this, its error comment reading *"a column of silent 1.0s that is indistinguishable from real weights"*. | MODERATE (~20 lines mirroring `weight_resolved`). |

All four are testable through `graphRef: {nodeQuery, edgeQuery, weightColumn}`.

## Phase 5 — APOC compatibility divergences

These are **behaviour changes**, not swallowed errors, so they carry migration
risk that Phases 1–4 do not. Sequenced last for that reason.

| id | site | consequence |
|---|---|---|
| P1 | `uni-plugin-apoc-core/src/convert.rs:158` | `apoc.convert.toString([1,2,3])` returns **NULL** where Neo4j returns `"[1, 2, 3]"`. Only Bool/Int64/Float64/Utf8 are handled; everything else arrives as `LargeBinary`(JSON) and hits the catch-all. Highest-impact APOC finding — an everyday ported query turns real data into NULL. |
| P2 | `uni-plugin-apoc-core/src/create.rs:149` | `apoc.create.uuids(2_000_000)` silently returns 1,000,000 rows. The cap is deliberate; the silence is the defect. |
| P3 | `uni-plugin-apoc-core/src/text.rs:335` | `apoc.text.repeat` silently truncates. Same shape as P2 — two siblings truncating identically, so neither corrects the other. |
| P4 | `uni-plugin-apoc-core/src/text.rs:345` | `indexOf` returns a UTF-8 **byte** offset; Neo4j returns a character index (`indexOf('cafés','s')` → 5 vs 4). `text.length` counts scalar values, so the pair is not even self-consistent. **Fixing this changes results for existing users** — needs a note in the release notes at minimum. |
| P5 | `uni-plugin-apoc-core/src/number.rs:144` | `parse().ok()` where APOC uses `DecimalFormat`: `"3.7"` → 3 and `"1,234"` → 1234 in Neo4j, both NULL here. NULL on genuine garbage is correct. Medium confidence; verify against APOC's documented behaviour before changing. |

## Decisions needed — not patches

| id | question | why it is a decision |
|---|---|---|
| **D1** | `uni-common/src/core/check_constraint.rs:113` — a declared `CHECK (age >= 18 AND age < 100)` (7 tokens against a 3-token parser) returns `Ok(true)` and is **never enforced**; violating rows commit. | Documented as deliberate: rejecting at evaluation time would block legitimate writes. But the effect is Tier 1 and the user is never told their constraint is inert. **Proposed: reject at DDL time** — fail the declaration, not the write. That is the same propagate-vs-record split used throughout this cycle, applied one layer up. |
| **D2** | `uni-common/src/cypher_value_codec.rs:476-500` — the fast-decode quartet returns `Option`, conflating "wrong tag" with "tag matched, payload corrupt". | The slow-path sibling `decode_msgpack` returns `Result`. Changing to `Result<Option<T>>` touches ~14 call sites, all in `uni-query-functions`, and is what makes Q7 fixable at the mechanism rather than the call site. |
| **D3** | The absent-field `volatility` / `determinism` default of `immutable` / `pure`, in **Rhai, Extism, Wasm and the Python binding**. | Documented, tested, and shown in user docs on all four surfaces. Changing it to `volatile` is fail-safe (a nondeterministic guest function can no longer be constant-folded) but costs constant folding for every existing plugin that omits the field. One coherent decision across four surfaces, or none. |
| **D4** | Should an **absent** `abi_extism` be an error, or assume `^1`? | `uni-plugin-wasm` assumes `^1` (`loader.rs:1158`) and fails closed at link time with a misleading diagnostic. Worth settling once for both loaders. |

## Decisions taken — 2026-09-06

| id | decision | consequence |
|---|---|---|
| **D1** | **Fix the undocumented half only.** `evaluate` now finds the operator regardless of spacing, so `CHECK (age>=18)` enforces exactly as `CHECK (age >= 18)` does. The documented permissive path for genuinely complex expressions is **kept** — it has a reasoned, published contract in two doc pages telling users to split compound rules, and no standard contradicts it. | Both operands must stay whitespace-free, which is what the docs already specify. That is load-bearing, not incidental: without it, `age >= 18 AND age < 100` would split into `age`/`>=`/`18 AND age < 100`, and comparing an `Int` to that string returns `Err` — turning a permissively-allowed constraint into one that fails **every** write. A dedicated test pins this. |
| **D2** | **Change the fast-decode quartet to `Result<Option<T>>`.** `Ok(None)` = wrong tag (a type mismatch the caller handles); `Err` = tag matched but payload corrupt. Matches the slow-path sibling `decode_msgpack`. | The 13 call sites split into three treatments, not one blanket `?`: fast-path comparison helpers bail to `None` (their caller re-decodes through the `Result`-returning slow path, so this defers to the layer that can report — a fallback, not a swallow); the arithmetic helpers use the existing `CvArithOutcome::Error`; the sites that produce a final answer propagate. |
| **D3** | **Keep the `immutable` / `pure` default for an absent volatility field, on all four surfaces** (Rhai, Extism, Wasm, the Python binding). No code change. | It is documented, tested and shown in user docs on every surface; flipping it costs constant folding for every existing plugin that omits the field. Recorded here so the next audit does not re-open it. |
| **D4** | **An absent `abi-extism` assumes `^1`**, mirroring what the Component Model loader does for a manifest omitting `abi` (`uni-plugin-wasm/src/loader.rs:1158`). | A plugin that never declared the field keeps loading; only a declared-and-unsupported range is refused. |
| **P4 / P5** | **Fix both; ship as `feat!` with a release note.** | `apoc.text.indexOf` returning a byte offset is also self-inconsistent with `apoc.text.length` in the same file, so it is a correctness bug independent of Neo4j parity — and it is a silent numeric change to a function used in slicing arithmetic, which is why it warrants a breaking-change marker rather than a footnote. |

## Corrections found while implementing

Verification against source before each fix changed six of the audit's claims.
The document predicted this ("about a third of candidate Tier-1 calls were
wrong"); recording the specifics so the next pass starts from the corrected
picture.

| id | correction |
|---|---|
| **S2** | **Worse than reported.** Not two missed Arrow variants but five: `Union`, `RunEndEncoded`, `ListView`, `LargeListView`, and `Dictionary` (indirect — its value type carries no `Field` of its own but can be a tagged `Struct`). Fixed by deleting the `_` arm entirely, so the next Arrow bump fails to compile rather than failing open. |
| **C2** | **Understated.** The second call site also silently **skips a delete-promotion**, leaving the row on primary — not only the reported duplicate insert. |
| **C4** | Call sites are 13, not ~18. But a **live** wrong-answer chain is confirmed: `endpoint_hydrate.rs:224` canonicalises, then `:302` calls `edge_endpoints`, which now takes the native arm and returns `Some(Vid::INVALID)` where the raw map correctly gave `None`. |
| **Q4** | **Consequence refuted.** `accumulate` opens with `if val.is_null() { return; }`, so `cypher_type_rank(Null) == 0` is unreachable and NULL never wins the min. The real symptom is a **silently dropped row** — still a defect, different story. |
| **P1** | Mechanism wrong: the payload is the tagged `cypher_value_codec` encoding, **not JSON** (`plugin_adapter.rs:234` deliberately replaced `serde_json`). The fix must decode through the codec. |
| **P5** | **Broader.** Four functions, not two — `convert.toInteger` and `convert.toFloat` share the identical `parse().ok()`. |

## Explicitly declined — recorded so they are not re-litigated

| site | why |
|---|---|
| `uni-plugin-wasm/src/loader.rs:899` `let _ = store.set_fuel(..)` | Unreachable: `build_engine(&limits)` sets `consume_fuel(true)` iff `limits.fuel_per_call.is_some()`, and every store is built from the same `limits`. **But it is one refactor from silently dropping the fuel cap** — worth an `expect`, not a fix. |
| `uni-plugin-wasm/src/multi_version.rs:150` | Serializing a `BTreeSet<Capability>` cannot fail. If it did, `""` would collapse the linker cache key across capability sets. Same note as above. |
| `uni-plugin-conformance/src/lib.rs:112` | The scaffold `passed: true` is documented as a marker. Arguably should be `passed: false` so a mis-wired gate is loud; judgement call, not a defect. |
| `uni-cypher/src/ast.rs:596` | Would be Tier 1, but nothing in `grammar/` constructs `CypherLiteral::Bytes`; every other consumer maps it straight to `Value::Bytes`. Inconsistency, not a defect. |
| `uni-locy/src/dependency_dnf.rs:274` | The `locy_aggregates` shape (a missing marginal becomes "certain"), but the sole caller inserts a weight for every RV it interns. `pub` and re-exported, so a `debug_assert!` is cheap insurance. |
| `uni-btic`, `uni-sidecar` | Clean. `uni-sidecar`'s `load()` already distinguishes `NotFound` → default from a real IO error → typed `Err`; both known #233 defects were caller-side. |

## Structural follow-ups

1. **Extend `arch_fail_open.rs`.** The existing three rules are greppable and
   caught two sites hand-auditing missed. Candidate fourth rule: an `if let Ok`
   on a `.query(` or `scan_*` call — the `uni-algo` / `uni-fork` shape. Verify
   the budget is small enough to be useful before adding it; a rule with a
   fifty-entry budget is noise.
2. **`uni-sidecar` load-token API.** Not a defect in the primitive, but a
   `modify()` — or a `LoadedSidecar` token that `store()` requires — would make
   "failed read, then write destroys the unread rows" *unrepresentable* rather
   than fixed in each of three callers. ~40 lines plus mechanical migration.
3. **A `degraded`-flag convention for `uni-fork`.** C1–C3 are three instances of
   one omission; the fix should make the flag hard to drop, not just present.

## Sequencing and effort

| phase | items | effort | gate |
|---|---|---|---|
| 1 — safety | S1, S2 | ~half a day | loader + ipc tests |
| 2 — corruption | C1–C5 | ~1 day | `ForkQueryHost` stub; `value.rs` unit tests |
| 3 — functions | Q1–Q3 trivial, Q4–Q7 moderate | ~1 day | pure unit tests, no DB |
| 4 — algorithms | A1–A3 trivial, A4 moderate | ~half a day | `graphRef` seams |
| 5 — APOC | P1–P5 | ~1 day + release note | behaviour change, needs sign-off |
| decisions | D1–D4 | — | human |

Every fix follows the rules this cycle established: **a test verified to fail
with the fix reverted**, no test written for an unreachable path, and
`Refs`-not-`Fixes` until the scope is genuinely complete.

## Recommended issue handling

File this as its own issue. Unlike #233's original speculative scope note, the
audit is complete and reproducible, so it clears the repo's "a bug you can
reproduce" bar. #233 itself should close on a judgement about its own named
crates, where all three tiers are now discharged.
