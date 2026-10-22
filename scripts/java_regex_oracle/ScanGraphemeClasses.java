// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Black-box scan of how {@code \X} classifies every code point, through the
 * public {@code Pattern} API only.
 *
 * <p>For each code point {@code c} it prints {@code c} (hex) and ten bits,
 * each the answer of {@code \X} matching one whole string: {@code a+c},
 * {@code c+a}, {@code U+0600+c}, {@code U+1100+c}, {@code U+AC00+c},
 * {@code U+AC01+c}, {@code U+1F1E6+c}, {@code c+U+0301},
 * {@code U+1F468 U+200D+c} and {@code c} alone. The bits separate the
 * grapheme-cluster-break classes {@code gen_ucd_extra.py} needs.
 */
public final class ScanGraphemeClasses {
    private ScanGraphemeClasses() {}

    public static void main(String[] args) throws Exception {
        Pattern x = Pattern.compile("\\X");
        String[][] ctx = {{"a", ""}, {"", "a"}, {"؀", ""}, {"ᄀ", ""}, {"가", ""}, {"각", ""},
            {"🇦", ""}, {"", "́"}, {"👨‍", ""}, {"", ""}};
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (int c = 0; c <= 0x10FFFF; c++) {
            String cs = new String(Character.toChars(c));
            sb.setLength(0);
            sb.append(Integer.toHexString(c)).append(' ');
            for (String[] k : ctx) {
                sb.append(x.matcher(k[0] + cs + k[1]).matches() ? '1' : '0');
            }
            out.write(sb.toString());
            out.newLine();
        }
        out.flush();
    }
}
