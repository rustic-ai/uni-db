// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Answers Cypher's {@code subject =~ pattern} the way Neo4j does:
 * {@code Pattern.compile(pattern).matcher(subject).matches()} with no flags
 * (see Neo4j's {@code ValueBooleanLogic.regex}).
 *
 * <p>Reads tab-separated lines from stdin. Fields 1 and 2 are the pattern and
 * the subject, escaped as {@code \\}, {@code \t}, {@code \n}, {@code \r} and
 * {@code \x{HEX}}; any further fields are copied through. Writes each line
 * back with Java's answer inserted as field 3: {@code true} or
 * {@code false}; {@code error} when {@code Pattern.compile} throws
 * {@code PatternSyntaxException}; {@code exception:<class>} when the match
 * throws (Java's matcher can read outside the string);
 * {@code stackoverflow} when it runs out of stack; or {@code timeout} when
 * Java needs more than the time limit for one case (it has no step limit,
 * so a catastrophic pattern would otherwise never return). The limit is
 * {@link #LIMIT_NANOS}, or the {@code oracle.timeout.ms} system property.
 * Empty lines and lines starting with {@code #} are copied unchanged.
 *
 * <p>With {@code -Doracle.detail=true} a syntax error is written as
 * {@code error:<description>@<index>} (the description escaped like the
 * fields), so error messages can be compared too.
 */
public final class JavaRegexOracle {
    private JavaRegexOracle() {}

    /** How long one case may run by default. */
    static final long LIMIT_NANOS = 2_000_000_000L;

    /** Thrown when a case runs past its deadline. */
    static final class Timeout extends RuntimeException {
        Timeout() {
            super(null, null, false, false);
        }
    }

    /** The subject, checking the deadline on every read. */
    record Timed(String s, long deadline) implements CharSequence {
        public char charAt(int i) {
            if (System.nanoTime() > deadline) {
                throw new Timeout();
            }
            return s.charAt(i);
        }

        public int length() {
            return s.length();
        }

        public CharSequence subSequence(int a, int b) {
            return new Timed(s.substring(a, b), deadline);
        }

        @Override
        public String toString() {
            return s;
        }
    }

    static String unescape(String s) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c != '\\') {
                out.append(c);
                continue;
            }
            char e = s.charAt(++i);
            switch (e) {
                case '\\' -> out.append('\\');
                case 't' -> out.append('\t');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 'x' -> {
                    int close = s.indexOf('}', i);
                    out.appendCodePoint(Integer.parseInt(s.substring(i + 2, close), 16));
                    i = close;
                }
                default -> throw new IllegalArgumentException("bad escape \\" + e + " in " + s);
            }
        }
        return out.toString();
    }

    /** The inverse of {@link #unescape}, escaping everything outside printable ASCII. */
    static String escape(String s) {
        StringBuilder out = new StringBuilder();
        s.codePoints().forEach(c -> {
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '\t' -> out.append("\\t");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                default -> {
                    if (c >= 0x20 && c < 0x7f) {
                        out.append((char) c);
                    } else {
                        out.append("\\x{").append(Integer.toHexString(c).toUpperCase()).append('}');
                    }
                }
            }
        });
        return out.toString();
    }

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        long limit = Long.getLong("oracle.timeout.ms", LIMIT_NANOS / 1_000_000L) * 1_000_000L;
        boolean detail = Boolean.getBoolean("oracle.detail");
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isEmpty() || line.startsWith("#")) {
                out.println(line);
                continue;
            }
            String[] f = line.split("\t", -1);
            String answer;
            try {
                Pattern p = Pattern.compile(unescape(f[0]));
                Timed subject = new Timed(unescape(f[1]), System.nanoTime() + limit);
                answer = p.matcher(subject).matches() ? "true" : "false";
            } catch (PatternSyntaxException e) {
                answer = detail ? "error:" + escape(e.getDescription()) + "@" + e.getIndex() : "error";
            } catch (Timeout e) {
                answer = "timeout";
            } catch (StackOverflowError e) {
                answer = "stackoverflow";
            } catch (RuntimeException e) {
                answer = "exception:" + e.getClass().getName();
            }
            StringBuilder row = new StringBuilder(f[0]).append('\t').append(f[1]).append('\t').append(answer);
            for (int k = 2; k < f.length; k++) {
                row.append('\t').append(f[k]);
            }
            out.println(row);
        }
    }
}
