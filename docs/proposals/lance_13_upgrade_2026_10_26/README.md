# Upgrade evidence

See [the validation report](../lance_13_upgrade_2026_10_26.md) for scope and
limitations. JSON files retain exact query results, dependency versions,
performance repetitions and index-use profiles. `binding-binaries.json`
identifies the two native extensions used; the large binaries are not
vendored here.

The Python scripts are the exact host-local probes used for this run. Their
temporary database and extension paths are under `/tmp/uni-lance-upgrade`;
adapt those paths when reproducing on another machine. `fixture.py` must run
with the saved Lance 11 extension before rebuilding the current binding.
`verify.py mutate` runs with the rebuilt Lance 13 extension, followed by
`verify.py rollback` with Lance 11. Select that import root through
`UPGRADE_BINDING_ROOT`. Run each in a separate Python process.

`perf.py old` and `perf.py new` likewise select their extension through
`UPGRADE_BINDING_ROOT`. Each requires an unused temporary database prefix.
The separately run `perf-profile.py` probes assert actual index hits for
both query shapes; profiling is not part of the reported timing samples.

Builds and tests ran as foreground commands at normal OS priority, with
`RUSTC_WRAPPER=` and `CARGO_BUILD_JOBS=6`. Cargo lanes were serialized through
test completion. The initial sandbox-denied TCP tests were rerun with host
access in the final full workspace run.

`remaining-ci-validation.json` records the additional CI follow-up. Its status
and phase distinguish completed checks from pending work; it is not an all-CI
green assertion. The nightly Cypher fuzz run found a reproducible parser timeout.
`cypher-timeout-input.bin` preserves the exact 1,633-byte input, and
`cypher-timeout-replay.json` / `.log` retain the bounded replay command and result.
The original nightly fuzz log is also retained. No fuzz timeout was increased.

The follow-up completed 66 command checks: 63 passed and three failed. The
report describes the failures; `remaining-ci-validation.json` records every
command, result, summary and raw-log hash. `remaining-ci-logs.tar.gz` contains
the complete logs, host-local runner scripts and build-maintenance inventories.
Adapt the runner's absolute paths when reproducing elsewhere.

`remaining-ci-environment.json` records toolchains and hardware;
`installed-wheel-binaries.json` verifies that the three isolated Python
environments used the same Lance 13 wheel. `remaining-ci-build-maintenance.json`
records cache cleanup and a 211-second build-only pause to reclaim disk space
through transparent example-binary compression. No test was paused, and the
full workflow commands, filters and case counts were retained. The default
Python extension and the example directory's original compression preference
were restored and verified.

Replay the saved parser timeout from the repository root:

```sh
cd fuzz
CARGO_BUILD_JOBS=6 RUSTC_WRAPPER='' cargo +nightly fuzz run \
  --target x86_64-unknown-linux-gnu cypher_parse \
  ../docs/proposals/lance_13_upgrade_2026_10_26/cypher-timeout-input.bin \
  -- -runs=1 -timeout=10
```
