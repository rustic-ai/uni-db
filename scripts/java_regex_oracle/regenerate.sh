#!/usr/bin/env bash
# Regenerates crates/uni-query-functions/testdata/java_regex_oracle.tsv: the
# Java answer for every case in java_regex_cases.tsv. Needs a JDK (21+) on
# PATH; Neo4j runs on Java 21.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
java "$root/scripts/java_regex_oracle/JavaRegexOracle.java" \
  < "$root/crates/uni-query-functions/testdata/java_regex_cases.tsv" \
  > "$root/crates/uni-query-functions/testdata/java_regex_oracle.tsv"
