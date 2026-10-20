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
 * back with Java's answer ({@code true}, {@code false} or {@code error})
 * inserted as field 3. Empty lines and lines starting with {@code #} are
 * copied unchanged.
 */
public final class JavaRegexOracle {
    private JavaRegexOracle() {}

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

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isEmpty() || line.startsWith("#")) {
                out.println(line);
                continue;
            }
            String[] f = line.split("\t", -1);
            String answer;
            try {
                answer = Pattern.compile(unescape(f[0])).matcher(unescape(f[1])).matches() ? "true" : "false";
            } catch (PatternSyntaxException e) {
                answer = "error";
            }
            StringBuilder row = new StringBuilder(f[0]).append('\t').append(f[1]).append('\t').append(answer);
            for (int k = 2; k < f.length; k++) {
                row.append('\t').append(f[k]);
            }
            out.println(row);
        }
    }
}
