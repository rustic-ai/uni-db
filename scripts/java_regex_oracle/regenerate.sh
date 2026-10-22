#!/usr/bin/env bash
# Regenerates crates/uni-query-functions/testdata/java_regex_oracle.tsv: the
# Java answer for every case in java_regex_cases.tsv, recorded with the JDK
# the `=~` semantics are pinned to: Java 21, the minimum JVM the current
# Neo4j release supports.
#
#   JAVA=/path/to/jdk-21/bin/java [JAVAC=/path/to/any/javac] regenerate.sh
#
# JAVA must be a Java 21 runtime (a JRE is enough). JAVAC is any javac able to
# target release 21; it defaults to the javac beside JAVA, then to PATH.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
java="${JAVA:-java}"
major="$("$java" -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java.specification.version = //p')"
if [ "$major" != "21" ]; then
  echo "JAVA must be a Java 21 runtime, got specification version '$major'" >&2
  exit 1
fi
javac="${JAVAC:-$(dirname "$(command -v "$java")")/javac}"
[ -x "$javac" ] || javac="javac"
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT
"$javac" --release 21 -d "$out" "$root/scripts/java_regex_oracle/JavaRegexOracle.java"
"$java" -cp "$out" JavaRegexOracle \
  < "$root/crates/uni-query-functions/testdata/java_regex_cases.tsv" \
  > "$root/crates/uni-query-functions/testdata/java_regex_oracle.tsv"
