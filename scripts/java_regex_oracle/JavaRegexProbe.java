// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.regex.*;

/**
 * Black-box probe of {@code java.util.regex} through its public API.
 *
 * <p>Reads lines {@code mode<TAB>pattern<TAB>subject}, where {@code @{HEX}}
 * stands for a code point (so lone surrogates can be written), and prints
 * each line with the answer: mode {@code M} the {@code matches()} result,
 * {@code G} the result with every group's span, {@code F} every
 * {@code find()} span, {@code C} whether the pattern compiles. Syntax errors
 * print the {@code PatternSyntaxException} description and index; a
 * {@code StackOverflowError} or a runtime exception is printed too.
 *
 *     javac --release 21 -d out JavaRegexProbe.java
 *     java -cp out JavaRegexProbe < probes.txt
 */
public final class JavaRegexProbe {
    static String unescape(String s) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '@' && i + 1 < s.length() && s.charAt(i + 1) == '{') {
                int close = s.indexOf('}', i); int v = Integer.parseInt(s.substring(i + 2, close), 16);
                if (v <= 0xFFFF) out.append((char) v); else out.appendCodePoint(v); i = close; continue;
            }
            out.append(c);
        }
        return out.toString();
    }
    static String show(String s) {
        if (s == null) return "null";
        StringBuilder b = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) { char c = s.charAt(i);
            if (c >= 0x20 && c < 0x7f) b.append(c); else b.append(String.format("\\u%04X", (int) c)); }
        return b.append('"').toString();
    }
    public static void main(String[] a) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] f = line.split("\t", -1);
            String mode = f[0], p = unescape(f[1]), s = f.length > 2 ? unescape(f[2]) : "";
            StringBuilder r = new StringBuilder(line).append("  =>  ");
            try {
                Pattern pat = Pattern.compile(p);
                Matcher m = pat.matcher(s);
                switch (mode) {
                    case "C" -> r.append("OK groups=").append(m.groupCount());
                    case "M" -> r.append(m.matches());
                    case "G" -> { boolean ok = m.matches(); r.append(ok);
                        if (ok) for (int g = 1; g <= m.groupCount(); g++) r.append(" g").append(g).append("=")
                            .append(m.start(g) < 0 ? "unset" : "[" + m.start(g) + "," + m.end(g) + "]" + show(m.group(g))); }
                    case "F" -> { while (m.find()) r.append(" [").append(m.start()).append(",").append(m.end()).append("]"); }
                    default -> r.append("bad mode");
                }
            } catch (PatternSyntaxException e) {
                r.append("PSE ").append(show(e.getDescription())).append(" @").append(e.getIndex());
            } catch (StackOverflowError e) {
                r.append("STACKOVERFLOW");
            } catch (RuntimeException e) {
                r.append("EXC ").append(e.getClass().getName()).append(": ").append(e.getMessage());
            }
            out.println(r);
        }
    }
}
