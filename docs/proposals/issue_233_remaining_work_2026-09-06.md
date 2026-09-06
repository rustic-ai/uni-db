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

## P0 — silent wrong answers

| site | consequence | cost |
|---|---|---|
| `uni-plugin-host/src/notifications.rs:117` | `RecvError::Lagged` on the **public** `session.watch()` / `CommitStream` surface (also `PyCommitStream`, `AsyncCommitStream`), over a 256-slot channel. A slow consumer silently loses commits and `next()` returns the following one as if contiguous, so an incremental view built on `watch()` is permanently wrong with no signal. **`CdcRuntime` treats this exact condition as fatal** (`halt_all_streams`); the user-facing stream logs and continues. One mechanism, two opposite policies, and the safe one is on the internal path. | **DESIGN** — needs a lag signal on the API. Breaking: 2 Rust call sites (`session.rs:1233/1239`, `sync.rs:184/189`), 3 Python wrappers, `.pyi` |
| `uni-plugin-rhai/src/manifest.rs:154` | `optional_string(&map, "determinism").unwrap_or_else(\|\| "pure")`. A mistyped key or non-string value yields `"pure"` → `Volatility::Immutable`, so DataFusion may constant-fold or CSE a **nondeterministic** Rhai scalar or aggregate. The *value* path is already fail-safe (an unknown string maps to `Volatile`); only the absent / wrong-type path fails open. | **TRIVIAL** — one line, plus the existing test that asserts the `"pure"` default |
| `uni-plugin-host/src/triggers.rs:2005` | A deferral sidecar read error logs at `debug!` and returns 0 deferrals; the next `push` then `persist_locked`s the whole in-memory map, **overwriting the rows it failed to read**. The retry is what makes a transient failure permanent. Strictly worse than the scheduler site it resembles: `scheduler.rs:304` is survivable precisely because that path does not rewrite the sidecar. | TRIVIAL–MODERATE — latch a `load_failed` flag and refuse to persist while set, mirroring `persistence_degraded` |

## P1 — no wrong answer, but nothing self-corrects

| site | consequence | cost |
|---|---|---|
| `uni-crdt/src/registry_dispatch.rs:104` | **`merge_via_registry` has never dispatched to a shipped provider.** `Crdt::kind()` emits `uni-crdt:g-counter` / `uni-crdt:or-set` / `uni-crdt:lww-register`; `uni-plugin-builtin/src/crdts.rs:34-38` registers `g-counter` / `or-set` / `lww-register`. Every host enabling the builtin CRDT plugin silently gets native merge. No wrong answer *today* — the fallback is the correct native merge — but the feature's whole purpose is to let semantics differ, and a user plugin that genuinely differs is bypassed without a trace. | MODERATE — alias map or an opt-in strict-dispatch flag; ~5 call sites |
| `uni-bulk/src/bulk.rs:1270` | `count_rows(...).ok()` persists `row_count_at_build = None`, and `index_rebuild.rs:94-96` gates its growth trigger on `if let Some(built_count)` — so that index **never auto-rebuilds on growth again**, until some later build happens to write a count. | TRIVIAL |
| `uni-plugin-pyo3/src/watchdog.rs:106` | `.spawn(...).ok()` — on thread-spawn failure the forced-deadline layer is silently absent, and a pure-Python `while True: pass` guest becomes unbounded, hanging the query. OS-exhaustion-only trigger. | TRIVIAL fix; needs new fault-injection machinery to test |
| `uni-store/src/runtime/l0.rs:433` (`:470`) | `merge_via_registry(...).is_ok()` collapses a *provider* failure into the same `false` as a variant mismatch, so the fall-through warns "overwriting CRDT property with a different CRDT variant" — a misattributed cause — and LWW-discards merged state. Masked in-tree today by the kind mismatch above. | TRIVIAL — split the `Result` before the bool |

## P2 — observability

`cdc_runtime.rs:323` `halted_stream_count()` and `scheduler.rs:378`
`persistence_degraded()` are **read by nothing outside their own crates**. A
halted CDC feed and a scheduler that could not load its jobs are both
correct-but-stopped, and invisible to an operator. Surface them through a
`uni.system.*` procedure or a gauge. MODERATE.

## P3 — trivial honesty fixes

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

## Sequencing

1. **The two trivial P0s** — the `determinism` default and the deferral latch.
2. **The `CommitStream` decision**, which is a product call and not a patch:
   a `Lagged { n }` variant on `next()` versus a `lagged()` flag. Either is
   breaking across Rust and Python. The argument for doing it at all is that
   CDC already decided this same question the other way.
3. **P1**, with the CRDT registry dispatch split out into **its own issue** —
   it is a dead feature, not a fail-open, and will be lost inside #233. It is
   also the fourth instance this cycle of infrastructure wired and never
   consumed (`ScanRequest::with_limit` #239, `index_consulted` #195,
   `max_impact` #118), which is a class in its own right.
4. **P2**, then **P3** as a single sweep.

#233 itself should close on a judgement about tiers, not by reaching zero.
