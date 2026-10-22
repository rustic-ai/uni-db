#!/usr/bin/env bash
# Regenerates crates/uni-query-functions/src/java_regex/ucd_tables.rs from
# the JDK the `=~` semantics are pinned to: Java 21, the minimum JVM the
# current Neo4j release supports (Unicode 15.0).
#
#   JAVA=/path/to/jdk-21/bin/java [JAVAC=/path/to/any/javac] regenerate_ucd_tables.sh
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
"$javac" --release 21 -d "$out" "$root/scripts/java_regex_oracle/GenUcdTables.java"
"$java" --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-exports java.base/jdk.internal.icu.lang=ALL-UNNAMED \
  -cp "$out" GenUcdTables "$root/crates/uni-query-functions/src/java_regex/ucd_tables.rs"
