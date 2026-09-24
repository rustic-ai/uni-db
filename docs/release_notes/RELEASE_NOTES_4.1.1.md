# uni-db 4.1.1

**Release focus: a Locy timeout means what it says, and a refusal says why.** An explicit Locy
timeout above the database's `query_timeout` was silently cut down to it, and a program refused for
cost raised the same error as a program that was simply wrong. Separately, full-text search could
hang forever once the runtime that first loaded an index was gone.

7 commits since 4.1.0.

---

## ⚠️ API and behaviour changes

This patch release has **source-level API changes** in Rust and changes **which exception class**
some errors raise. They are listed first so that upgrading callers can check them.

**`LocyConfig::timeout` is `Option<Duration>`.** It was a `Duration` defaulting to 300s, which the
engine could not tell apart from a value the caller asked for (see below). Wrap an explicit value
in `Some`: `LocyConfig { timeout: Some(Duration::from_secs(60)), ..Default::default() }`. The
300s budget is `LocyConfig::DEFAULT_TIMEOUT`, and `effective_timeout()` returns whichever
applies. Code that sets the timeout through `locy_with(..).timeout(..)`, or from Python, is
unaffected.

**`UniError::MemoryLimitExceeded` has a `message` field** that holds the refusing layer's own
text: which operator or rule asked, and for how much. Patterns written as
`MemoryLimitExceeded { .. }` are unaffected.

**Cost refusals are no longer `UniError::Query` / `UniQueryError`.** A query or Locy program
refused for exceeding a memory budget now raises `MemoryLimitExceeded`
(`UniMemoryLimitExceededError`). This covers the operator pool (`max_memory` /
`max_query_memory`), the result-size estimate and a Locy relation's `max_derived_bytes`. A Locy
program that runs out of time now raises `Timeout` (`UniTimeoutError`), as Cypher already did. Code
that caught `UniQueryError` for these cases, or matched on the message, should catch the specific
class instead. A program that is simply wrong still raises `UniQueryError`.

---

## An explicit Locy timeout is honoured (#289)

`session.locy_with(q).timeout(120).run()` stopped at 30 seconds, while
`session.query_with(q).timeout(120)` ran for the full 120. Values under 30s worked, so the setting
appeared to work until it was raised past the database default, and then it was discarded with no
error and no warning.

The Locy engine combined its own timeout with the database's `query_timeout` by taking the smaller
of the two. That was deliberate: with the Locy default a plain 300s, letting it win would have
loosened the 30s deadline for anyone who never set one. But an explicit value could then never
raise the deadline. With the timeout now optional, a value the caller sets replaces
`query_timeout` for that evaluation, as it does for Cypher. An unset timeout behaves as before.

From Python there was a second way to lose it. The builders applied `.with_config(..)` *after*
`.timeout(..)` and `.max_iterations(..)`, and `with_config` replaces the whole configuration, so
`locy_with(q).timeout(5).with_config({...})` dropped the 5. The configuration is now applied first
and the individual setters on top of it.

## A refusal is distinguishable from a failure (#289)

On the Locy path a timeout, a memory refusal and an unknown function all raised `UniQueryError`, and
only the message separated them. A caller that needs to tell "this program was too expensive"
(retry it differently, record a budget event) from "this program was wrong" had to match on text.

Every executor error now goes through one classification step shared by the Cypher and Locy paths,
so the same condition raises the same class whichever surface hit it, and carries the budget that
was actually applied (`timeout_ms`, `limit_bytes`). `UniMemoryLimitExceededError` has been in the
Python API since 1.0.0 but no code path ever raised it until now, on Cypher as well as Locy.

## Full-text search no longer hangs after the first runtime is gone (#290)

Two `similar_to` full-text calls with different query text: the first returned, the second hung
forever with every thread idle. Lance's inverted index caches its readers, and with them an I/O
loop spawned on whichever tokio runtime first loaded the index. `similar_to` ran that load on a
short-lived runtime built per call, so the loop died when the call returned, and every later query
for a term not yet cached waited on it forever.

This was a class, not one call site: subqueries, `block_on_scoped`, a caller dropping its own
runtime while the database lived on, `build_sync`, and flush streams all had the same shape. The
flush case wedged the in-order finalizer, so every later flush hung. That work now runs on a
process-wide runtime that is never shut down (`uni_store::runtime::io_runtime`). Background tasks
started by `build()` still run on the caller's runtime; the Black Book documents this.

---

## Dependencies

- **`imbl` 7.0.0 → 7.0.2** for RUSTSEC-2026-0292: a double free in `imbl-sized-chunks` 0.1.x
  when an element's `Drop` panics. `imbl` is a dev-dependency only, so no shipped artifact linked
  the affected code.
- **`uni-xervo` 0.18.0 → 0.18.1.** A lock-only update within the declared range. The feature
  surface is identical.
