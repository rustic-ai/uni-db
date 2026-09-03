# Open-issue triage and reprioritization — 2026-09-03

44 open issues, ordered against the principle the LDBC remediation document
already established: **wrong answers before crashes before gaps before speed**,
and anything that stops the system defending itself outranks optimization.

Two things have changed since the last ordering was written, and both bear on it.

**SF1 is 14 of 14.** The latency emergency that shaped the last two orderings is
over. Nothing here is queued because a benchmark query fails.

**The IC5 round is evidence for putting observability first.** It took four
wrong attributions to find one cause, and every one of them was an inference
that resembled a measurement. What ended it was an instrument that reported its
own total beside its parts. That is the same argument the class review made for
Class 7 — "it is why the others hid" — now with a worked example.

---

## The count overstates the work

Fourteen of the 44 are instances of a class also filed. Counting both makes the
backlog look larger than it is and invites fixing the instance while the class
survives, which this project has watched happen repeatedly.

| class | instances also open |
|---|---|
| #234 entity identity hand-rolled | #217, #235, #236, #216 |
| #243 edge orientation re-derived four ways | #193 |
| #224 no cost model / syntax-driven plan shape | #219, #225, #226, #237, #247 |
| #220 per-item storage round-trips | #221, #222, #228 |

Recommendation: keep the instances open only where they carry a repro the class
issue does not, and say on each which class owns it. The classes are the work
items; the instances are symptoms.

---

## Tier 1 — silent wrong answers

Invisible by construction, so no amount of running the system surfaces them.

| # | why it is first |
|---|---|
| **#233** | fail-open class: 27 sites, **17 silent**. Six verified while filing. Two project-wide decisions (does a failed read ever return a default; does a failed index/status write ever go unrecorded) settle most of it |
| **#234** | identity hand-rolled ~30 times; `Value::entity_vid`, written to end that, has **two** call sites. Fix the boundary, not the sites |
| **#243** | orientation re-derived four ways, four faces still live and untested. This class already produced a regression *during* a fix whose soundness argument checked one of the four |
| **#236** | Locy join keys are `Debug` over a `HashMap` — **inconsistently** wrong, so a rerun does not confirm anything |
| **#235** | `COUNT(DISTINCT n)` has no entity arm at all — a wrong aggregate nothing downstream catches |

#193, #216, #217 fold into #234/#243.

## Tier 2 — the system cannot bound itself

Not wrong answers, but the difference between a slow query and an outage.

| # | note |
|---|---|
| **#242** | zero first-party operators reserve from the pool; a pool that sees a minority of the allocation is a sampler, not a bound. Peak RSS is 14.5 GiB against a 1 GiB pool |
| **#240** | `execute_stream` emits one item, so the incremental memory check fires after everything is resident |
| **#239** | `ScanRequest::limit` has zero callers — `LIMIT 1` reads the whole table |
| **#241** | variable-length traversal has neither chunking nor slicing; ten of fourteen LDBC queries use VLP |
| **#214** | the scan slices but still builds the whole result first |
| **#213** | `ORDER BY … LIMIT` is always a full sort; needs a spillable TopK, not the one-line pushdown |
| **#238** | two comments assert no disk spill and drive the pool choice. Cheap, and it is the premise Tier 2 reasoning rests on — do it first within this tier |

## Tier 3 — instruments that cannot fail

| # | note |
|---|---|
| **#230** | **live and lying now**: 5 of 5 gated targets 88–97% below baseline, reported "worst +0.00%". A gate that passes on a collapsed measurement corrupts everything measured after it |
| **#247** | indexes are built and never consulted, verified by a point lookup no faster than a scan written to defeat pushdown |
| **#179**, **#223** | operators and scans invisible to `PROFILE` / `scans_reported`, so the shape tests that would pin them cannot be written |
| **#205**, **#177** | the vacuous-fixture audit and the unproven-operator ratchet |
| **#195** | no benchmark exercises the index counters |

## Tier 4 — performance, once there is something to plan with

**#224 is the root** — `estimate_costs` ignores its plan and returns constants,
and no graph operator overrides `statistics()`. #237, #247, #219, #225 and #226
are all decisions made without cardinality. Sequencing anything else in this
tier before #224 means fitting more constants to one dataset.

Then #220 with #221/#222/#228, and #206 and #215.

## Tier 5 — hygiene and features

#174, #176, #178, #200, #227, #231; and #118–#123, which are planned features,
not defects.

---

## Recommended order

1. **#230**, then **#231**. Hours, not days, and everything measured afterwards
   is only as good as the gate that reports it.
2. **#233 Tier 1 only** — the wrong-answer rows, with the two project-wide
   decisions made once rather than per site.
3. **#234**, taking #217/#235/#236/#216 with it. Make the encoding
   unrepresentable-in-two-ways, or make `entity_vid` the only way to ask, so the
   remaining sites fail to compile rather than failing to match.
4. **#243**, taking #193. Carry orientation with the edge value, or make the
   storage probe the only answer and delete the reconstructions.
5. **Tier 2 as one track**, starting with #238, because these share a root — no
   operator accounts for what it allocates — and fixing them one at a time
   re-litigates the same design each time.
6. **#224**, then the rest of Tier 4.
7. Tier 5, and Track 0 (the differential oracle) whenever a Neo4j instance
   exists — it remains the one thing that would have found most of the Tier 1
   items without anyone probing.

## What this ordering deliberately does not do

It does not put IC5-adjacent performance work near the top. #206, #219, #225 and
#226 were queued there when a benchmark query could not answer; it now can, and
their remaining justification is throughput, which is Tier 4's business.

It also drops Wave 5.2–5.5 from the front of the queue. Its gating argument —
that latency work needed the index counters as a witness — did not survive: the
gate is still shut (#247) and IC5 was attributed with `PROFILE` instead.
