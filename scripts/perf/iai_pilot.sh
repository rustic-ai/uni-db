#!/usr/bin/env bash
# Phase 0B qualification pilot driver.
#
# Runs the iai-callgrind bench N times, collecting instruction counts after each
# run, then reports per-target coefficient of variation.
#
# This measures **repeatability only**. Whether a stable target is also a
# *meaningful* one — whether its instruction count tracks wall-clock under a real
# regression — is the separate correlation leg. A target must pass both.
#
# Requires: valgrind, and an `iai-callgrind-runner` at exactly the
# `iai-callgrind` dev-dependency version in crates/uni/Cargo.toml.
#
# Usage: scripts/perf/iai_pilot.sh [runs]   (default 20)
set -euo pipefail

RUNS="${1:-20}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="$ROOT/target/iai-pilot"

cd "$ROOT"

if ! command -v valgrind >/dev/null 2>&1; then
    echo "error: valgrind not installed (dnf install valgrind)" >&2
    exit 1
fi

# The bench profile must keep symbols. Callgrind toggles collection on entry to
# the benchmark function *by name*; against a stripped binary the toggle never
# matches, every counter reads zero, and iai-callgrind still exits 0. See
# [profile.bench] in .cargo/config.toml.
if ! grep -q '^\[profile\.bench\]' "$ROOT/.cargo/config.toml"; then
    echo "error: [profile.bench] missing from .cargo/config.toml — the bench" >&2
    echo "       binary will inherit release's strip=\"symbols\" and every" >&2
    echo "       measurement will silently read zero." >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/run-*.json

echo "== iai-callgrind pilot: $RUNS runs =="
for i in $(seq 1 "$RUNS"); do
    printf '[%2d/%2d] ' "$i" "$RUNS"
    # Wipe prior callgrind output so each run's collection is unambiguous.
    rm -rf "$ROOT/target/iai"
    RUSTC_WRAPPER="" cargo bench -p uni-db --bench hot_paths_iai >/dev/null 2>&1
    python3 "$ROOT/scripts/perf/iai_collect.py" \
        --iai-dir "$ROOT/target/iai" \
        --out "$OUT_DIR/$(printf 'run-%02d.json' "$i")"
done

echo
echo "== per-target coefficient of variation =="
python3 "$ROOT/scripts/perf/iai_cv.py" "$OUT_DIR"/run-*.json
