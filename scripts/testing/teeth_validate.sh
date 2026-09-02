#!/usr/bin/env bash
# Validate the "teeth" of the test suite: for each historical bug, deliberately
# reinstate the defect and confirm something actually catches it.
#
# # Why this exists
#
# A regression test that has only ever been observed passing is not evidence.
# It may assert something the bug never violated, or route around the fix site
# entirely. The only way to know a test bites is to put the bug back.
#
# `bugs/hash_index_range_quoting.rs` is the cautionary example: of its three
# tests, `range_still_constrains_when_it_excludes_the_equality` asserts the
# result is empty — and the bug *also* produced empty, so it passes identically
# with and without the defect. It is a fine test of the opposite direction and a
# non-witness for the bug it lives next to. Only running it against the
# reinstated defect reveals that.
#
# # The positive control is not optional
#
# Each entry names a CONTROL test that must FAIL under the patch. If it passes,
# the run aborts rather than reporting on the oracle: the usual cause is cargo
# serving a cached binary built before the patch was applied, and every result
# after that point would be measuring the fixed code while claiming otherwise.
#
# # Nothing is applied to your working tree
#
# Every patch is applied inside a throwaway `git worktree`, so an interrupted
# run cannot leave a reverted fix behind. The patches under
# `docs/testing/reverts/` are checked-in *evidence*, never scaffolding that
# builds by default.
#
# Usage:
#   scripts/testing/teeth_validate.sh              # all bugs
#   scripts/testing/teeth_validate.sh issue_097    # one or more by name
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REVERTS="$ROOT/docs/testing/reverts"
NEXTEST=(cargo nextest run -p uni-db --test integration --no-fail-fast)

# name | control test (must fail under the patch) | oracle test (recorded)
#
# The control is a pre-existing regression test wherever one exists — writing a
# second copy of it here would add maintenance surface and detect nothing new.
# The oracle column is the generated/harness-driven run, and is `-` where no
# generated case can reach the bug (recorded as such, not silently omitted).
ENTRIES=(
  "lance_col_fusion|eq_plus_two_sided_range_on_hash_indexed_column|flush_pushdown_smoke"
  "issue_097|fork_inherits_unflushed_single_node|-"
  "issue_099|fork_rejects_wrong_target_endpoint_label_schemaless|-"
  "issue_103|issue_103_nested_fork_schemaless_edge_reads_inherited|-"
  "issue_110|typed_fork_set_reverse_reach_is_complete|-"
  "issue_135|issue_135_traversal_props_survive_flush|-"
  "compaction_property_loss|vacuum_preserves_schemaless_properties|-"
)

only=("$@")
wanted() {
    [ ${#only[@]} -eq 0 ] && return 0
    for o in "${only[@]}"; do [ "$o" = "$1" ] && return 0; done
    return 1
}

# A shared target dir across all six runs.
#
# Without it every worktree pays a cold build of the full dependency set
# (datafusion/lance/candle) and the harness takes about an hour. Cargo
# fingerprints by content, so a shared dir still rebuilds whatever the patch
# touched — and the positive control below is what actually guarantees the
# binary under test contains the defect, so this cannot mask a stale build.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target/teeth}"

# Mirrors the *working tree* into $WT, not just HEAD.
#
# `git worktree add HEAD` checks out the last commit, which silently excludes
# uncommitted work — the exact trap this harness exists to catch, one level up.
# A tooth added but not yet committed would be absent from the worktree, its
# filter would match no tests, and the run would report "the oracle missed it"
# when in truth the oracle was never built.
sync_working_tree() {
    git -C "$ROOT" diff HEAD >"$WT.wip.patch"
    if [ -s "$WT.wip.patch" ]; then
        git -C "$WT" apply "$WT.wip.patch" || return 1
    fi
    # Untracked-but-not-ignored files (new test modules, new patches).
    git -C "$ROOT" ls-files --others --exclude-standard -z | while IFS= read -r -d '' f; do
        mkdir -p "$WT/$(dirname "$f")"
        cp -a "$ROOT/$f" "$WT/$f"
    done
    rm -f "$WT.wip.patch"
}

# Number of tests a filter matches. Zero is an error, never a pass.
#
# Parsed permissively on purpose: nextest has emitted test names both indented
# and flush-left across versions, and an over-precise pattern here fails closed
# in the worst way — it reports "no such test", which looks like a missing tooth
# rather than a broken counter. So: keep any line naming a test path, drop the
# build chatter.
count_tests() {
    (cd "$WT" && RUSTC_WRAPPER="" cargo nextest list -p uni-db --test integration \
        -E "test($1)" 2>/dev/null |
        grep -E '::' |
        grep -vcE '^\s*(Compiling|Finished|Building|warning|note|error|Downloaded)') || true
}

# Runs a filter in $WT; returns 0 if the suite passed, 1 if it failed.
run_filter() {
    (cd "$WT" && RUSTC_WRAPPER="" "${NEXTEST[@]}" -E "test($1)" >"$LOG" 2>&1)
}

printf '%-20s %-10s %-10s %s\n' NAME CONTROL ORACLE NOTE
printf '%s\n' "--------------------------------------------------------------"

rc=0
for entry in "${ENTRIES[@]}"; do
    IFS='|' read -r name control oracle <<<"$entry"
    wanted "$name" || continue

    patch="$REVERTS/$name.patch"
    if [ ! -f "$patch" ]; then
        printf '%-20s %-10s %-10s %s\n' "$name" - - "MISSING $patch"
        rc=1
        continue
    fi

    WT="$(mktemp -d "${TMPDIR:-/tmp}/teeth-$name.XXXXXX")"
    LOG="$WT.log"
    git -C "$ROOT" worktree add --detach --quiet "$WT" HEAD || {
        printf '%-20s %s\n' "$name" "could not create worktree"
        rc=1
        continue
    }

    if ! sync_working_tree; then
        printf '%-20s %-10s %-10s %s\n' "$name" - - "could not mirror the working tree"
        git -C "$ROOT" worktree remove --force "$WT" >/dev/null 2>&1
        rc=1
        continue
    fi

    if ! git -C "$WT" apply "$patch" 2>/dev/null; then
        printf '%-20s %-10s %-10s %s\n' "$name" - - "PATCH DOES NOT APPLY (fix site moved)"
        git -C "$ROOT" worktree remove --force "$WT" >/dev/null 2>&1
        rc=1
        continue
    fi

    # A filter matching nothing exits 0, which would read as "passed".
    # Checked before anything is believed: a mistyped or renamed test would
    # otherwise report the defect as uncaught when it was never run.
    #
    # This also covers a subtler failure, and it is worth keeping for that alone:
    # a patch that does not **compile** makes the control test "fail", which a
    # naive harness reads as "caught" — a tooth certified by a build error.
    # `count_tests` shells out to `nextest list`, which requires a successful
    # build, so a non-compiling patch lands here as ABSENT instead.
    if [ "$(count_tests "$control")" -eq 0 ]; then
        printf '%-20s %-10s %-10s %s\n' "$name" "ABSENT" - \
            "ABORT: control test '$control' matched no tests"
        rc=1
        git -C "$ROOT" worktree remove --force "$WT" >/dev/null 2>&1
        continue
    fi

    # Positive control: the defect is back, so this MUST fail.
    if run_filter "$control"; then
        printf '%-20s %-10s %-10s %s\n' "$name" "PASSED!" - \
            "ABORT: control passed with the defect reinstated — is it a witness at all?"
        rc=1
        git -C "$ROOT" worktree remove --force "$WT" >/dev/null 2>&1
        continue
    fi
    control_result="caught"

    oracle_result="-"
    if [ "$oracle" != "-" ]; then
        if [ "$(count_tests "$oracle")" -eq 0 ]; then
            oracle_result="ABSENT"
            rc=1
        elif run_filter "$oracle"; then
            oracle_result="MISSED"
            rc=1
        else
            oracle_result="caught"
        fi
    fi

    printf '%-20s %-10s %-10s %s\n' "$name" "$control_result" "$oracle_result" ""
    git -C "$ROOT" worktree remove --force "$WT" >/dev/null 2>&1
    rm -f "$LOG"
done

echo
if [ $rc -eq 0 ]; then
    echo "All requested teeth bite."
else
    echo "At least one tooth did not bite, or a patch no longer applies."
fi
exit $rc
