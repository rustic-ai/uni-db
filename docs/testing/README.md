# `docs/testing/` — evidence about the tests themselves

`docs/perf/` records how fast things run. This directory records something
harder: **whether a test would notice if the code were wrong.**

The theme running through every document here is one failure mode — a check that
reports success while doing nothing. A CI job green because it skipped. A
coverage-invisible operator whose own 15-test suite could not tell "the
optimization fired" from "it silently fell back". A benchmark reporting
recall@10 = 1.000 because the index under test never ran. These are the record
of finding and closing those.

## The index

| document | what it establishes | date |
|---|---|---|
| [teeth-2026-08-13.md](teeth-2026-08-13.md) | per bug, what happened when the defect was **deliberately put back**. A test only ever observed passing is not evidence | 2026-08-13 |
| [silent-downgrades-2026-08-15.md](silent-downgrades-2026-08-15.md) | every planner site where an optimization is attempted and falls back to a slower-but-correct path with no error, warning, or trace | 2026-08-15 |
| [madsim-spike-2026-08-25.md](madsim-spike-2026-08-25.md) | deterministic-simulation spike for C2 — **verdict REJECT**, with the evidence, plus the cheaper alternative that replaced it | 2026-08-25 |
| [reverts/](reverts/) | the revert patches the teeth ledger replays — 7 of them, one per pinned defect | ongoing |

## The reverts directory

`reverts/*.patch` are not history. Each one re-introduces a specific fixed defect
so the test that pins it can be observed **failing**. Replay them with:

```bash
scripts/testing/teeth_validate.sh            # all of them
scripts/testing/teeth_validate.sh issue_097  # one
```

A patch that no longer makes its test fail is itself a finding: either the test
stopped discriminating, or the code moved out from under the patch. Both want
investigating rather than deleting.

## The discipline these encode

**Prove the checker bites.** Every assertion added by this track was verified to
*fail* when pointed at something that should not satisfy it — a crash test aimed
at a non-existent failpoint seam, a fixture fetch pointed at a corrupted file, a
contention sweep with no cell that can collide. An assertion never observed
failing is an assumption wearing a test's clothes.

**A denominator that cannot discriminate is not a denominator.** The
`vid_lookup_join` case in `silent-downgrades` is the canonical one: the operator
sat behind six `return Ok(None)` guards for four months, executing 0 of 441
lines, while its dedicated suite passed — because those tests asserted result
bags and the operator's entire contract is to be bag-identical to what it
replaces. No bag assertion could ever have caught it.

**A spike's deliverable is a decision with evidence, not an adoption.** The
madsim document reaches *reject* and says why on measurement, including that the
proposal's own hand-wave for adopting it was directionally right but never
checked.

## Related

- `docs/perf/` — timing and instruction-count measurements, and which of them
  gate.
- `docs/test_layout.md` — the 3-integration-binary-per-crate rule and why it
  exists.
- `docs/local_ci_runbook.md` — reproducing the CI lanes locally.
- `docs/proposals/test_harness_track_e_poa_2026-08-25.md` — current plan of
  action.
