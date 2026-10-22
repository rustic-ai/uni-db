// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.Normalizer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntPredicate;
import java.util.regex.Pattern;

/**
 * Writes {@code crates/uni-query-functions/src/java_regex/ucd_tables.rs}: the
 * Unicode data {@code java.util.regex} reads, taken from the running JDK.
 *
 * <p>Run it on the JDK the regex semantics are pinned to (see
 * {@code regenerate_ucd_tables.sh}): the general category, script, the
 * binary properties {@code CharPredicates} consults, and the simple case
 * mappings, for every code point including the surrogates; the
 * {@code Character.isXxx} predicates behind {@code \p{java…}}; Unicode
 * blocks and every name {@code UnicodeBlock.forName} accepts; the canonical
 * combining classes and decompositions {@code Normalizer} uses for
 * {@code (?c)}; and the characters whose full case mapping is ASCII, for
 * Java's case-insensitive name lookups. It also records, for each property
 * name the translation resolves, a fingerprint of the set {@code Pattern}
 * itself matches, so a Rust test can check the derived sets against Java's
 * exhaustively.
 *
 * <p>Beside the table source it writes {@code ucd_names.bin}, the names
 * {@code Character.getName} takes from {@code CharacterName} (what
 * {@code \N{…}} resolves through {@code Character.codePointOf}), in a
 * word-dictionary encoding read by {@code java_regex::names}. Reading
 * {@code CharacterName} and the normalizer's combining classes needs
 * {@code --add-opens java.base/java.lang=ALL-UNNAMED} and
 * {@code --add-exports java.base/jdk.internal.icu.lang=ALL-UNNAMED}.
 */
public final class GenUcdTables {
    private GenUcdTables() {}

    static final int MAX = 0x10FFFF;

    /** Sorted disjoint ranges of the code points where {@code p} holds. */
    static List<int[]> ranges(IntPredicate p) {
        List<int[]> out = new ArrayList<>();
        int start = -1;
        for (int cp = 0; cp <= MAX + 1; cp++) {
            boolean in = cp <= MAX && p.test(cp);
            if (in && start < 0) {
                start = cp;
            } else if (!in && start >= 0) {
                out.add(new int[] {start, cp - 1});
                start = -1;
            }
        }
        return out;
    }

    /** Ranges of equal {@code f} values, covering every code point. */
    static List<int[]> valueRanges(java.util.function.IntUnaryOperator f) {
        List<int[]> out = new ArrayList<>();
        int start = 0;
        int cur = f.applyAsInt(0);
        for (int cp = 1; cp <= MAX + 1; cp++) {
            int v = cp <= MAX ? f.applyAsInt(cp) : Integer.MIN_VALUE;
            if (v != cur) {
                out.add(new int[] {start, cp - 1, cur});
                start = cp;
                cur = v;
            }
        }
        return out;
    }

    /** FNV-1a over each range's ends as little-endian u32s. */
    static long fingerprint(List<int[]> rs) {
        long h = 0xcbf29ce484222325L;
        for (int[] r : rs) {
            for (int k = 0; k < 2; k++) {
                int v = r[k];
                for (int b = 0; b < 4; b++) {
                    h ^= (v >>> (8 * b)) & 0xFF;
                    h *= 0x100000001b3L;
                }
            }
        }
        return h;
    }

    static String hex(int v) {
        return String.format("0x%X", v);
    }

    static void pairs(StringBuilder sb, String doc, String name, List<int[]> rs) {
        sb.append("/// ").append(doc).append('\n');
        sb.append("pub(crate) const ").append(name).append(": &[(u32, u32)] = &[\n");
        int k = 0;
        for (int[] r : rs) {
            if (k % 6 == 0) {
                sb.append("   ");
            }
            sb.append(" (").append(hex(r[0])).append(", ").append(hex(r[1])).append("),");
            if (++k % 6 == 0) {
                sb.append('\n');
            }
        }
        if (k % 6 != 0) {
            sb.append('\n');
        }
        sb.append("];\n\n");
    }

    static void triples(StringBuilder sb, String doc, String name, List<int[]> rs) {
        sb.append("/// ").append(doc).append('\n');
        sb.append("pub(crate) const ").append(name).append(": &[(u32, u32, u8)] = &[\n");
        int k = 0;
        for (int[] r : rs) {
            if (k % 4 == 0) {
                sb.append("   ");
            }
            sb.append(" (").append(hex(r[0])).append(", ").append(hex(r[1])).append(", ").append(r[2]).append("),");
            if (++k % 4 == 0) {
                sb.append('\n');
            }
        }
        if (k % 4 != 0) {
            sb.append('\n');
        }
        sb.append("];\n\n");
    }

    static void wideTriples(StringBuilder sb, String doc, String name, List<int[]> rs) {
        sb.append("/// ").append(doc).append('\n');
        sb.append("pub(crate) const ").append(name).append(": &[(u32, u32, u16)] = &[\n");
        for (int[] r : rs) {
            sb.append("    (").append(hex(r[0])).append(", ").append(hex(r[1])).append(", ").append(r[2]).append("),\n");
        }
        sb.append("];\n\n");
    }

    static String quote(String s) {
        StringBuilder q = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') {
                q.append('\\');
            }
            if (c < 0x20 || c > 0x7E) {
                throw new IllegalStateException("non-ASCII table string " + s);
            }
            q.append(c);
        }
        return q.append('"').toString();
    }

    /**
     * The non-ASCII code points whose full {@code toUpperCase} (or
     * {@code toLowerCase}) is an ASCII string, under {@code Locale.ROOT},
     * which maps every code point as {@code Locale.US} and
     * {@code Locale.ENGLISH} do.
     */
    static List<Object[]> asciiCaseMappings(boolean upper) {
        List<Object[]> out = new ArrayList<>();
        for (int cp = 0x80; cp <= MAX; cp++) {
            if (cp >= 0xD800 && cp <= 0xDFFF) {
                continue;
            }
            String s = new String(Character.toChars(cp));
            String m = upper ? s.toUpperCase(Locale.ROOT) : s.toLowerCase(Locale.ROOT);
            String us = upper ? s.toUpperCase(Locale.US) : s.toLowerCase(Locale.US);
            if (!m.equals(us)) {
                throw new IllegalStateException("ROOT and US case mappings differ at " + hex(cp));
            }
            if (m.chars().allMatch(c -> c < 0x80)) {
                out.add(new Object[] {cp, m});
            }
        }
        return out;
    }

    /**
     * Writes the {@code CharacterName} names: a dictionary of the words they
     * are made of, then each named code point with its words.
     *
     * <p>Layout (little-endian): {@code u32} word count, each word as a
     * {@code u8} length and its ASCII bytes; {@code u32} name count, each
     * name as a {@code u24} code point, a {@code u8} token count and that
     * many {@code u16} tokens, a word index whose top bit says the separator
     * before the word is {@code '-'} rather than {@code ' '} (the first
     * token has none). Names are printable ASCII (control characters have
     * names such as {@code LINE FEED (LF)}).
     */
    static int writeNames(Path path) throws Exception {
        Class<?> cn = Class.forName("java.lang.CharacterName");
        Method getInstance = cn.getDeclaredMethod("getInstance");
        getInstance.setAccessible(true);
        Object names = getInstance.invoke(null);
        Method getName = cn.getDeclaredMethod("getName", int.class);
        getName.setAccessible(true);
        LinkedHashMap<String, Integer> words = new LinkedHashMap<>();
        List<int[]> encoded = new ArrayList<>();
        for (int cp = 0; cp <= MAX; cp++) {
            String name = (String) getName.invoke(names, cp);
            if (name == null) {
                continue;
            }
            if (!name.equals(Character.getName(cp)) || Character.codePointOf(name) != cp) {
                throw new IllegalStateException("name of " + hex(cp) + " does not round-trip");
            }
            List<Integer> tokens = new ArrayList<>();
            int start = 0;
            char sep = ' ';
            for (int k = 0; k <= name.length(); k++) {
                char c = k < name.length() ? name.charAt(k) : ' ';
                if (k < name.length() && !(c == ' ' || c == '-')) {
                    if (c <= 0x20 || c > 0x7E) {
                        throw new IllegalStateException("unexpected character in " + name);
                    }
                    continue;
                }
                String w = name.substring(start, k);
                Integer id = words.get(w);
                if (id == null) {
                    id = words.size();
                    words.put(w, id);
                }
                tokens.add(id | (sep == '-' ? 0x8000 : 0));
                sep = c;
                start = k + 1;
            }
            int[] row = new int[tokens.size() + 1];
            row[0] = cp;
            for (int k = 0; k < tokens.size(); k++) {
                row[k + 1] = tokens.get(k);
            }
            encoded.add(row);
        }
        if (words.size() >= 0x8000) {
            throw new IllegalStateException("too many name words: " + words.size());
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        java.util.function.IntConsumer u8 = out::write;
        java.util.function.IntConsumer u16 = v -> {
            out.write(v & 0xFF);
            out.write((v >>> 8) & 0xFF);
        };
        java.util.function.IntConsumer u32 = v -> {
            u16.accept(v & 0xFFFF);
            u16.accept(v >>> 16);
        };
        u32.accept(words.size());
        for (String w : words.keySet()) {
            u8.accept(w.length());
            out.writeBytes(w.getBytes(StandardCharsets.US_ASCII));
        }
        u32.accept(encoded.size());
        for (int[] row : encoded) {
            u8.accept(row[0] & 0xFF);
            u8.accept((row[0] >>> 8) & 0xFF);
            u8.accept(row[0] >>> 16);
            u8.accept(row.length - 1);
            for (int k = 1; k < row.length; k++) {
                u16.accept(row[k]);
            }
        }
        Files.write(path, out.toByteArray());
        return encoded.size();
    }

    /** The code points {@code regex} matches as a one-code-point string. */
    static List<int[]> patternSet(String regex) {
        var m = Pattern.compile(regex).matcher("");
        return ranges(cp -> m.reset(new String(Character.toChars(cp))).matches());
    }

    public static void main(String[] args) throws Exception {
        Path out = Path.of(args[0]);
        String version = System.getProperty("java.version");
        StringBuilder sb = new StringBuilder();
        sb.append("// SPDX-License-Identifier: Apache-2.0\n");
        sb.append("// Copyright 2024-2026 Dragonscale Team\n\n");
        sb.append("//! The Unicode data `java.util.regex` reads, generated from JDK ").append(version)
          .append(" by\n//! `scripts/java_regex_oracle/regenerate_ucd_tables.sh`. Do not edit.\n");
        sb.append("//!\n//! Every table covers every code point, surrogates included, and is what\n");
        sb.append("//! that JDK's `Character` methods answer, so the regex semantics follow\n");
        sb.append("//! one pinned Unicode version rather than the Rust toolchain's.\n\n");
        sb.append("/// The JDK the tables were taken from.\n");
        sb.append("pub(crate) const JAVA_VERSION: &str = \"").append(version).append("\";\n\n");

        triples(sb, "`Character.getType`, by range, covering every code point.", "GENERAL_CATEGORY",
                valueRanges(Character::getType));

        Character.UnicodeScript[] scripts = Character.UnicodeScript.values();
        sb.append("/// `Character.UnicodeScript` names, in ordinal order.\n");
        sb.append("pub(crate) const SCRIPT_NAMES: &[&str] = &[\n");
        for (Character.UnicodeScript s : scripts) {
            sb.append("    \"").append(s.name()).append("\",\n");
        }
        sb.append("];\n\n");
        // Every name `UnicodeScript.forName` accepts that is not an enum name.
        TreeMap<String, String> aliases = new TreeMap<>();
        char[] buf = new char[4];
        for (int a = 0; a < 26 * 26 * 26 * 26; a++) {
            int x = a;
            for (int k = 3; k >= 0; k--) {
                buf[k] = (char) ('A' + x % 26);
                x /= 26;
            }
            String name = new String(buf);
            try {
                Character.UnicodeScript s = Character.UnicodeScript.forName(name);
                if (!s.name().equals(name)) {
                    aliases.put(name, s.name());
                }
            } catch (IllegalArgumentException e) {
                // not a script name
            }
        }
        sb.append("/// The aliases `Character.UnicodeScript.forName` accepts, upper case, with\n");
        sb.append("/// the script each names.\n");
        sb.append("pub(crate) const SCRIPT_ALIASES: &[(&str, &str)] = &[\n");
        for (Map.Entry<String, String> e : aliases.entrySet()) {
            sb.append("    (\"").append(e.getKey()).append("\", \"").append(e.getValue()).append("\"),\n");
        }
        sb.append("];\n\n");
        triples(sb, "`Character.UnicodeScript.of`, as an index into `SCRIPT_NAMES`, by range.", "SCRIPT",
                valueRanges(cp -> Character.UnicodeScript.of(cp).ordinal()));

        pairs(sb, "`Character.isAlphabetic`.", "ALPHABETIC", ranges(Character::isAlphabetic));
        pairs(sb, "`Character.isIdeographic`.", "IDEOGRAPHIC", ranges(Character::isIdeographic));
        pairs(sb, "`Character.isLowerCase`.", "LOWERCASE", ranges(Character::isLowerCase));
        pairs(sb, "`Character.isUpperCase`.", "UPPERCASE", ranges(Character::isUpperCase));
        pairs(sb, "`Character.isEmoji`.", "EMOJI", ranges(Character::isEmoji));
        pairs(sb, "`Character.isEmojiPresentation`.", "EMOJI_PRESENTATION",
              ranges(Character::isEmojiPresentation));
        pairs(sb, "`Character.isEmojiModifier`.", "EMOJI_MODIFIER", ranges(Character::isEmojiModifier));
        pairs(sb, "`Character.isEmojiModifierBase`.", "EMOJI_MODIFIER_BASE",
              ranges(Character::isEmojiModifierBase));
        pairs(sb, "`Character.isEmojiComponent`.", "EMOJI_COMPONENT", ranges(Character::isEmojiComponent));
        pairs(sb, "`Character.isExtendedPictographic`.", "EXTENDED_PICTOGRAPHIC",
              ranges(Character::isExtendedPictographic));

        List<int[]> upper = new ArrayList<>();
        List<int[]> lower = new ArrayList<>();
        for (int cp = 0; cp <= MAX; cp++) {
            int u = Character.toUpperCase(cp);
            int l = Character.toLowerCase(cp);
            if (u != cp) {
                upper.add(new int[] {cp, u});
            }
            if (l != cp) {
                lower.add(new int[] {cp, l});
            }
        }
        pairs(sb, "`Character.toUpperCase(int)` for every code point it changes, sorted.", "TO_UPPER", upper);
        pairs(sb, "`Character.toLowerCase(int)` for every code point it changes, sorted.", "TO_LOWER", lower);

        // The `Character.isXxx` predicates `\\p{java…}` names that no table
        // above already holds.
        pairs(sb, "`Character.isTitleCase`.", "JAVA_TITLE_CASE", ranges(Character::isTitleCase));
        pairs(sb, "`Character.isDigit`.", "JAVA_DIGIT", ranges(Character::isDigit));
        pairs(sb, "`Character.isDefined`.", "JAVA_DEFINED", ranges(Character::isDefined));
        pairs(sb, "`Character.isLetter`.", "JAVA_LETTER", ranges(Character::isLetter));
        pairs(sb, "`Character.isLetterOrDigit`.", "JAVA_LETTER_OR_DIGIT", ranges(Character::isLetterOrDigit));
        pairs(sb, "`Character.isJavaIdentifierStart`.", "JAVA_IDENTIFIER_START",
              ranges(Character::isJavaIdentifierStart));
        pairs(sb, "`Character.isJavaIdentifierPart`.", "JAVA_IDENTIFIER_PART",
              ranges(Character::isJavaIdentifierPart));
        pairs(sb, "`Character.isUnicodeIdentifierStart`.", "UNICODE_IDENTIFIER_START",
              ranges(Character::isUnicodeIdentifierStart));
        pairs(sb, "`Character.isUnicodeIdentifierPart`.", "UNICODE_IDENTIFIER_PART",
              ranges(Character::isUnicodeIdentifierPart));
        pairs(sb, "`Character.isIdentifierIgnorable`.", "IDENTIFIER_IGNORABLE",
              ranges(Character::isIdentifierIgnorable));
        pairs(sb, "`Character.isSpaceChar`.", "SPACE_CHAR", ranges(Character::isSpaceChar));
        pairs(sb, "`Character.isWhitespace`.", "JAVA_WHITESPACE", ranges(Character::isWhitespace));
        pairs(sb, "`Character.isISOControl`.", "ISO_CONTROL", ranges(Character::isISOControl));
        pairs(sb, "`Character.isMirrored`.", "MIRRORED", ranges(Character::isMirrored));

        // Unicode blocks: `UnicodeBlock.of`, every block's `toString`, and
        // every name `UnicodeBlock.forName` accepts (its upper-cased key).
        Field mapField = Character.UnicodeBlock.class.getDeclaredField("map");
        mapField.setAccessible(true);
        Map<?, ?> blockMap = (Map<?, ?>) mapField.get(null);
        List<Character.UnicodeBlock> blockList = new ArrayList<>();
        Map<Character.UnicodeBlock, Integer> blockIndex = new java.util.IdentityHashMap<>();
        List<int[]> blockRanges = new ArrayList<>();
        for (int[] r : valueRanges(cp -> {
            Character.UnicodeBlock b = Character.UnicodeBlock.of(cp);
            if (b == null) {
                return 0xFFFF;
            }
            return blockIndex.computeIfAbsent(b, k -> {
                blockList.add(k);
                return blockList.size() - 1;
            });
        })) {
            if (r[2] != 0xFFFF) {
                blockRanges.add(r);
            }
        }
        // Blocks `forName` knows that `of` never returns (the deprecated
        // `SURROGATES_AREA`): they match no code point.
        for (Object b : new java.util.TreeMap<>(blockMap).values()) {
            Character.UnicodeBlock ub = (Character.UnicodeBlock) b;
            blockIndex.computeIfAbsent(ub, k -> {
                blockList.add(k);
                return blockList.size() - 1;
            });
        }
        if (blockList.size() >= 0xFFFF) {
            throw new IllegalStateException("too many blocks");
        }
        wideTriples(sb, "`Character.UnicodeBlock.of`, as an index into `BLOCK_NAMES`, by range; code\n"
                    + "/// points in no block are absent.", "BLOCK", blockRanges);
        sb.append("/// `Character.UnicodeBlock` names (`toString`), by index.\n");
        sb.append("pub(crate) const BLOCK_NAMES: &[&str] = &[\n");
        for (Character.UnicodeBlock b : blockList) {
            sb.append("    ").append(quote(b.toString())).append(",\n");
        }
        sb.append("];\n\n");
        sb.append("/// Every key of `UnicodeBlock.forName`'s map (names are looked up\n");
        sb.append("/// upper-cased), sorted, with the block's index.\n");
        sb.append("pub(crate) const BLOCK_KEYS: &[(&str, u16)] = &[\n");
        for (Map.Entry<?, ?> e : new java.util.TreeMap<>(blockMap).entrySet()) {
            sb.append("    (").append(quote((String) e.getKey())).append(", ")
              .append(blockIndex.get((Character.UnicodeBlock) e.getValue())).append("),\n");
        }
        sb.append("];\n\n");

        // Canonical combining classes and decompositions, for `(?c)`.
        Method combiningClass = Class.forName("jdk.internal.icu.lang.UCharacter")
            .getDeclaredMethod("getCombiningClass", int.class);
        List<int[]> ccc = new ArrayList<>();
        for (int[] r : valueRanges(cp -> {
            try {
                return (Integer) combiningClass.invoke(null, cp);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        })) {
            if (r[2] != 0) {
                ccc.add(r);
            }
        }
        triples(sb, "The canonical combining class `Normalizer` uses, by range; absent code\n"
                + "/// points have class 0.", "COMBINING_CLASS", ccc);
        sb.append("/// For each code point with a canonical decomposition, Hangul syllables\n");
        sb.append("/// excepted: its full decomposition (`Normalizer` NFD) and its NFC, or\n");
        sb.append("/// `u32::MAX` when the NFC is more than one code point.\n");
        sb.append("pub(crate) const DECOMPOSITIONS: &[(u32, &[u32], u32)] = &[\n");
        int decompositions = 0;
        for (int cp = 0; cp <= MAX; cp++) {
            if (cp >= 0xD800 && cp <= 0xDFFF) {
                continue;
            }
            String one = new String(Character.toChars(cp));
            String nfd = Normalizer.normalize(one, Normalizer.Form.NFD);
            String nfc = Normalizer.normalize(one, Normalizer.Form.NFC);
            boolean hangul = cp >= 0xAC00 && cp < 0xAC00 + 11172;
            if (hangul) {
                int s0 = cp - 0xAC00;
                StringBuilder want = new StringBuilder();
                want.append((char) (0x1100 + s0 / 588)).append((char) (0x1161 + (s0 % 588) / 28));
                if (s0 % 28 != 0) {
                    want.append((char) (0x11A7 + s0 % 28));
                }
                if (!nfd.equals(want.toString()) || !nfc.equals(one)
                    || !Normalizer.normalize(nfd, Normalizer.Form.NFC).equals(one)) {
                    throw new IllegalStateException("Hangul syllable " + hex(cp) + " is not algorithmic");
                }
                continue;
            }
            if (nfd.equals(one)) {
                if (!nfc.equals(one)) {
                    throw new IllegalStateException(hex(cp) + " has an NFC but no NFD");
                }
                continue;
            }
            sb.append("    (").append(hex(cp)).append(", &[");
            int[] parts = nfd.codePoints().toArray();
            for (int k = 0; k < parts.length; k++) {
                sb.append(k > 0 ? ", " : "").append(hex(parts[k]));
            }
            int single = nfc.codePointCount(0, nfc.length()) == 1 ? nfc.codePointAt(0) : -1;
            sb.append("], ").append(single < 0 ? "u32::MAX" : hex(single)).append("),\n");
            decompositions++;
        }
        sb.append("];\n\n");

        // Characters whose full case mapping is ASCII: the only non-ASCII
        // characters that can spell an ASCII name after Java upper- or
        // lower-cases it.
        for (boolean up : new boolean[] {true, false}) {
            List<Object[]> m = asciiCaseMappings(up);
            sb.append("/// The non-ASCII code points whose `String.to").append(up ? "Upper" : "Lower")
              .append("Case(Locale.ROOT)` is ASCII, with\n/// that mapping.\n");
            sb.append("pub(crate) const ").append(up ? "UPPER" : "LOWER").append("_TO_ASCII: &[(u32, &str)] = &[\n");
            for (Object[] e : m) {
                sb.append("    (").append(hex((Integer) e[0])).append(", ").append(quote((String) e[1])).append("),\n");
            }
            sb.append("];\n\n");
        }
        int nameCount = writeNames(out.resolveSibling("ucd_names.bin"));
        sb.append("/// How many names `ucd_names.bin` holds.\n#[cfg(test)]\n");
        sb.append("pub(crate) const NAME_COUNT: usize = ").append(nameCount).append(";\n\n");

        // Fingerprints of the sets `Pattern` matches, per property spelling.
        String[] cats = {"Cn", "Lu", "Ll", "Lt", "Lm", "Lo", "Mn", "Me", "Mc", "Nd", "Nl", "No", "Zs", "Zl",
            "Zp", "Cc", "Cf", "Co", "Cs", "Pd", "Ps", "Pe", "Pc", "Po", "Sm", "Sc", "Sk", "So", "Pi", "Pf",
            "L", "M", "N", "Z", "C", "P", "S", "LC", "LD", "L1", "all", "ASCII", "Alnum", "Alpha", "Blank",
            "Cntrl", "Digit", "Graph", "Lower", "Print", "Punct", "Space", "Upper", "XDigit"};
        String[] uprops = {"Alphabetic", "Assigned", "Control", "Emoji", "Emoji_Presentation",
            "Emoji_Modifier", "Emoji_Modifier_Base", "Emoji_Component", "Extended_Pictographic", "HexDigit",
            "Ideographic", "JoinControl", "Letter", "Lowercase", "Uppercase", "Titlecase",
            "NoncharacterCodePoint", "Punctuation", "WhiteSpace", "Word", "Alpha", "Lower", "Upper", "Space",
            "Punct", "XDigit", "Alnum", "Cntrl", "Digit", "Blank", "Graph", "Print"};
        String[] posix = {"Alpha", "Lower", "Upper", "Space", "Punct", "XDigit", "Alnum", "Cntrl", "Digit",
            "Blank", "Graph", "Print"};
        List<String[]> specs = new ArrayList<>();
        for (String flags : new String[] {"", "(?i)"}) {
            for (String c : cats) {
                specs.add(new String[] {flags, c});
            }
            for (String u : uprops) {
                specs.add(new String[] {flags, "Is" + u});
            }
            for (String p : posix) {
                specs.add(new String[] {flags + "(?U)", p});
            }
        }
        for (Character.UnicodeScript s : scripts) {
            specs.add(new String[] {"", "Is" + s.name()});
        }
        String[] javaProps = {"javaLowerCase", "javaUpperCase", "javaAlphabetic", "javaIdeographic",
            "javaTitleCase", "javaDigit", "javaDefined", "javaLetter", "javaLetterOrDigit",
            "javaJavaIdentifierStart", "javaJavaIdentifierPart", "javaUnicodeIdentifierStart",
            "javaUnicodeIdentifierPart", "javaIdentifierIgnorable", "javaSpaceChar", "javaWhitespace",
            "javaISOControl", "javaMirrored"};
        for (String flags : new String[] {"", "(?i)"}) {
            for (String j : javaProps) {
                specs.add(new String[] {flags, j});
            }
        }
        for (Character.UnicodeBlock b : blockList) {
            specs.add(new String[] {"", "In" + b.toString()});
        }
        sb.append("/// For each property spelling: the flags, the name inside `\\p{…}`, and a\n");
        sb.append("/// fingerprint (FNV-1a over the range ends as little-endian `u32`s) of the\n");
        sb.append("/// code points `Pattern` matches with it.\n");
        sb.append("#[cfg(test)]\n");
        sb.append("pub(crate) const PATTERN_FINGERPRINTS: &[(&str, &str, u64)] = &[\n");
        for (String[] s : specs) {
            long fp = fingerprint(patternSet(s[0] + "\\p{" + s[1] + "}"));
            sb.append("    (\"").append(s[0]).append("\", \"").append(s[1]).append("\", 0x")
              .append(Long.toHexString(fp)).append("),\n");
        }
        for (String[] s : new String[][] {{"(?U)", "\\\\w"}, {"(?U)", "\\\\d"}, {"(?U)", "\\\\s"}}) {
            long fp = fingerprint(patternSet(s[0] + s[1].replace("\\\\", "\\")));
            sb.append("    (\"").append(s[0]).append("\", \"").append(s[1]).append("\", 0x")
              .append(Long.toHexString(fp)).append("),\n");
        }
        sb.append("];\n");
        Files.writeString(out, sb.toString(), StandardCharsets.UTF_8);
        System.err.println("JDK " + version + ": scripts=" + scripts.length + " aliases=" + aliases.size()
            + " upper=" + upper.size() + " lower=" + lower.size() + " fingerprints=" + (specs.size() + 3)
            + " blocks=" + blockList.size() + " block-keys=" + blockMap.size() + " ccc=" + ccc.size()
            + " decompositions=" + decompositions + " names=" + nameCount);
    }
}
