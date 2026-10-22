//! Doc-snippet parse harness.
//!
//! Extracts every fenced ```cypher / ```locy block from the documentation tree
//! and asserts it parses against the real pest grammar. This is the compiler
//! that the documentation surface previously lacked: prose review cannot catch
//! `--` used as a comment, Neo4j-5 `CREATE CONSTRAINT ... REQUIRE` syntax, or a
//! `PROFILE` token that the grammar has never had.
//!
//! Three escape hatches, all deliberate and all visible in the source of the
//! doc being skipped:
//!
//! * An HTML comment `<!-- doctest: skip -->` on the line immediately before the
//!   opening fence. **Preferred**, because it is invisible in rendered output and
//!   leaves the fence info string alone, so mkdocs highlighting is unaffected.
//!   Use for clause fragments, API-signature notation, and deliberate
//!   counter-examples ("// Wrong" blocks) that are *supposed* not to parse.
//! * A fence whose info string carries `no-parse` (e.g. ```` ```cypher no-parse ````).
//! * A snippet containing an elision marker (`...` / `…`), because a
//!   deliberately abbreviated fragment is not expected to parse.
//!
//! Everything else must parse. When this test fails it prints `file:line` for
//! each offending snippet along with the parse error.

use std::fs;
use std::path::{Path, PathBuf};

/// Documentation roots scanned for snippets, relative to the repo root.
const DOC_ROOTS: &[&str] = &["website/docs", "docs", "skills"];

/// Info-string marker opting a fence out of the parse check.
const OPT_OUT: &str = "no-parse";

/// Render-invisible marker, placed on the line before the opening fence.
const SKIP_COMMENT: &str = "<!-- doctest: skip -->";

#[derive(Debug)]
struct Snippet {
    file: PathBuf,
    /// 1-based line of the opening fence.
    line: usize,
    lang: Lang,
    body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lang {
    Cypher,
    Locy,
}

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<root>/crates/uni-cypher`.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root above crates/uni-cypher")
        .to_path_buf()
}

fn collect_markdown(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_markdown(&path, out);
        } else if path.extension().is_some_and(|e| e == "md") {
            out.push(path);
        }
    }
}

/// Classify a fence info string, e.g. `cypher title="x"` -> `Some(Cypher)`.
///
/// Returns `None` for languages we do not check and for opted-out fences.
fn classify(info: &str) -> Option<Lang> {
    let info = info.trim();
    if info.contains(OPT_OUT) {
        return None;
    }
    // The language is the first whitespace-delimited token; mkdocs allows
    // trailing attributes such as `title=` and `linenums=`.
    match info.split_whitespace().next()? {
        "cypher" => Some(Lang::Cypher),
        "locy" => Some(Lang::Locy),
        _ => None,
    }
}

fn extract(path: &Path) -> Vec<Snippet> {
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let all: Vec<&str> = text.lines().collect();
    let mut snippets = Vec::new();
    let mut lines = text.lines().enumerate();

    while let Some((idx, line)) = lines.next() {
        let trimmed = line.trim_start();
        let Some(info) = trimmed.strip_prefix("```") else {
            continue;
        };
        // Opening fence. Consume through the closing fence regardless of
        // whether we intend to check this block, so nested prose containing
        // ``` cannot desynchronise the scanner.
        //
        // Scan back past blank lines for the skip marker, so it may sit above a
        // blank separator rather than jammed against the fence.
        let skipped_by_comment = all[..idx]
            .iter()
            .rev()
            .find(|l| !l.trim().is_empty())
            .is_some_and(|l| l.trim() == SKIP_COMMENT);
        let lang = if skipped_by_comment {
            None
        } else {
            classify(info)
        };
        let indent = line.len() - trimmed.len();
        let mut body = String::new();
        for (_, inner) in lines.by_ref() {
            if inner.trim_start().starts_with("```") {
                break;
            }
            // Strip the fence's own indentation so list-nested blocks parse.
            let stripped = if inner.len() >= indent && inner[..indent].trim().is_empty() {
                &inner[indent..]
            } else {
                inner
            };
            body.push_str(stripped);
            body.push('\n');
        }
        if let Some(lang) = lang {
            snippets.push(Snippet {
                file: path.to_path_buf(),
                line: idx + 1,
                lang,
                body,
            });
        }
    }
    snippets
}

/// A snippet showing an abbreviated fragment is not expected to parse.
fn is_elided(body: &str) -> bool {
    body.contains("...") || body.contains('…')
}

/// True if a chunk carries no statement — blank, or comments only.
///
/// `COMMENT` in `cypher.pest:6-9` is `//` and `/* */`; `locy.pest` inherits it
/// by grammar concatenation. A chunk of pure comments parses to nothing and
/// must not be reported as a failure.
fn is_blank(chunk: &str) -> bool {
    chunk.lines().all(|l| {
        let l = l.trim();
        l.is_empty() || l.starts_with("//") || l.starts_with("/*") || l.starts_with('*')
    })
}

/// Split a leading `MODULE x.y` / `USE a.b` prelude off a Locy snippet.
///
/// Returns `(prelude, remainder)`; the prelude is empty when the snippet does
/// not open with one. Comments and blank lines above the prelude are kept with
/// it so the remainder starts at the first real statement.
fn split_locy_prelude(body: &str) -> (String, &str) {
    let mut prelude = Vec::new();
    let mut consumed = 0usize;
    for line in body.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with("//") {
            consumed += line.len() + 1;
            continue;
        }
        let head = t
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if head == "MODULE" || head == "USE" {
            prelude.push(t.to_string());
            consumed += line.len() + 1;
        } else {
            break;
        }
    }
    if prelude.is_empty() {
        return (String::new(), body);
    }
    (
        prelude.join("\n"),
        body[consumed.min(body.len())..].trim_start_matches('\n'),
    )
}

/// Documentation fences routinely hold several statements in one block,
/// separated by a blank line. `parse`/`parse_locy` accept a single query, so a
/// whole-fence parse is tried first and a blank-line split is the fallback.
fn parses(body: &str, lang: Lang) -> Result<(), String> {
    let parse_one = |s: &str| -> Result<(), String> {
        match lang {
            Lang::Cypher => uni_cypher::parse(s).map(|_| ()).map_err(|e| e.to_string()),
            Lang::Locy => uni_cypher::parse_locy(s)
                .map(|_| ())
                .map_err(|e| e.to_string()),
        }
    };

    if parse_one(body).is_ok() {
        return Ok(());
    }

    // `locy_query` (locy.pest:102) admits at most one `MODULE`/`USE` prelude
    // followed by a *single* query, but a documented module listing naturally
    // shows the prelude once and several rules under it. Lift the prelude off
    // and re-attach it to each statement when splitting below.
    let (prelude, body) = split_locy_prelude(body);
    let reattach = |chunk: &str| -> String {
        if prelude.is_empty() {
            chunk.to_string()
        } else {
            format!("{prelude}\n{chunk}")
        }
    };
    let parse_one = |s: &str| parse_one(&reattach(s));

    if !prelude.is_empty() && parse_one(body).is_ok() {
        return Ok(());
    }

    // Fall back to per-statement parsing, coarse split first. Every non-blank
    // chunk must parse, and there must be at least two, so a single-statement
    // body cannot pass by a different route than the whole-body parse above.
    // A chunk may itself hold several one-line statements with no blank line
    // between them (`SHOW DATABASE` / `SHOW INDEXES` / ...), so each chunk is
    // accepted if it parses whole *or* line by line. Without the per-chunk
    // recursion, one such chunk would force the entire body down to
    // line-splitting, where a legitimate multi-line statement then fails.
    //
    // Limitation: a broken multi-line statement whose every line happens to
    // parse standalone would slip through. The whole-body attempt runs first,
    // so this only ever sees blocks that already failed.
    let chunk_ok = |c: &str| -> bool {
        if parse_one(c).is_ok() {
            return true;
        }
        let lines: Vec<&str> = c.lines().filter(|l| !is_blank(l)).collect();
        lines.len() >= 2 && lines.iter().all(|l| parse_one(l).is_ok())
    };

    let chunks: Vec<&str> = body.split("\n\n").filter(|c| !is_blank(c)).collect();
    if !chunks.is_empty() && chunks.iter().all(|c| chunk_ok(c)) {
        return Ok(());
    }

    // Nothing parsed; surface the original whole-body error, which is the most
    // informative one for a reader fixing the doc.
    parse_one(body)
}

fn gather() -> Vec<Snippet> {
    let root = repo_root();
    let mut files = Vec::new();
    for rel in DOC_ROOTS {
        collect_markdown(&root.join(rel), &mut files);
    }
    files.sort();
    files.iter().flat_map(|f| extract(f)).collect()
}

/// Host languages whose fences may embed Cypher in a string literal.
const HOST_LANGS: &[&str] = &["rust", "python", "py", "javascript", "typescript"];

/// Keywords that mark a string literal as Cypher rather than prose.
const CYPHER_KEYWORDS: &[&str] = &[
    "MATCH ",
    "CREATE ",
    "MERGE ",
    "RETURN ",
    "UNWIND ",
    "DETACH DELETE",
];

/// Extract string literals from a host-language fence that look like Cypher.
///
/// Deliberately simple: it finds `"`-delimited runs (and Python `"""` blocks)
/// and keeps the ones containing a Cypher keyword. Cypher embedded this way is
/// a *runtime* failure when wrong — the doc block renders fine and the query
/// blows up at `execute()` — which is why it is worth checking separately from
/// the fenced snippets.
///
/// A Python f-string (`f"..."`) is a template, not the query: `{{` and `}}`
/// are literal braces and each `{expr}` is replaced at runtime. It is checked
/// after the same substitution, with every replacement field standing in as
/// the identifier `x`, which parses wherever an interpolated value, label or
/// property name can appear.
fn embedded_cypher(body: &str) -> Vec<String> {
    let mut found = Vec::new();
    let bytes: Vec<char> = body.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != '"' {
            i += 1;
            continue;
        }
        let mut is_fstring = is_fstring_prefix(&bytes, i);
        // Python triple-quoted block.
        let triple = bytes[i..].starts_with(&['"', '"', '"']);
        let delim_len = if triple { 3 } else { 1 };
        let start = i + delim_len;
        let mut j = start;
        let mut lit = String::new();
        while j < bytes.len() {
            if !triple && bytes[j] == '\\' {
                j += 2;
                lit.push(' ');
                continue;
            }
            let closes = if triple {
                bytes[j..].starts_with(&['"', '"', '"'])
            } else {
                bytes[j] == '"'
            };
            if closes {
                break;
            }
            // Rust and Python both allow a plain `"` literal to span newlines,
            // and multi-line embedded queries are the common case, so do not
            // stop at a line break — only the closing delimiter ends the run.
            lit.push(bytes[j]);
            j += 1;
        }
        i = j + delim_len;
        if is_fstring {
            lit = fill_fstring(&lit);
        }

        // Python/C-style adjacent string concatenation: `"CALL ... " "YIELD ..."`
        // is one query split across literals. Absorb following literals that are
        // separated only by whitespace, else each half is checked in isolation
        // and both fail spuriously.
        loop {
            let mut k = i;
            while k < bytes.len()
                && (bytes[k] == ' ' || bytes[k] == '\n' || bytes[k] == '\t' || bytes[k] == '\r')
            {
                k += 1;
            }
            if k + 1 < bytes.len() && matches!(bytes[k], 'f' | 'F') && bytes[k + 1] == '"' {
                k += 1;
                is_fstring = true;
            } else {
                is_fstring = false;
            }
            if k >= bytes.len() || bytes[k] != '"' {
                break;
            }
            let t2 = bytes[k..].starts_with(&['"', '"', '"']);
            let d2 = if t2 { 3 } else { 1 };
            let mut m = k + d2;
            let mut next = String::new();
            while m < bytes.len() {
                if !t2 && bytes[m] == '\\' {
                    m += 2;
                    next.push(' ');
                    continue;
                }
                let closes = if t2 {
                    bytes[m..].starts_with(&['"', '"', '"'])
                } else {
                    bytes[m] == '"'
                };
                if closes {
                    break;
                }
                next.push(bytes[m]);
                m += 1;
            }
            if is_fstring {
                next = fill_fstring(&next);
            }
            lit.push_str(&next);
            i = m + d2;
        }

        if CYPHER_KEYWORDS.iter().any(|k| lit.contains(k)) {
            found.push(lit);
        }
    }
    found
}

/// Whether the `"` at `quote` opens a Python f-string: it is preceded by a
/// string prefix (`f`, `rf`, `fr`, any case) that is not the tail of an
/// identifier.
fn is_fstring_prefix(chars: &[char], quote: usize) -> bool {
    let mut start = quote;
    while start > 0 && quote - start < 2 && matches!(chars[start - 1], 'f' | 'F' | 'r' | 'R') {
        start -= 1;
    }
    let prefix = &chars[start..quote];
    let standalone = start == 0 || !(chars[start - 1].is_alphanumeric() || chars[start - 1] == '_');
    standalone && prefix.iter().any(|c| matches!(c, 'f' | 'F'))
}

/// Render an f-string template as its runtime text: `{{`/`}}` become braces
/// and each replacement field `{...}` (nesting counted) becomes `x`.
fn fill_fstring(template: &str) -> String {
    let chars: Vec<char> = template.chars().collect();
    let mut out = String::with_capacity(template.len());
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '{' if chars.get(i + 1) == Some(&'{') => {
                out.push('{');
                i += 2;
            }
            '}' if chars.get(i + 1) == Some(&'}') => {
                out.push('}');
                i += 2;
            }
            '{' => {
                let mut depth = 0usize;
                while i < chars.len() {
                    match chars[i] {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }
                out.push('x');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

#[test]
fn fstring_templates_are_filled_before_parsing() {
    let body = r#"q = f"CREATE (:Drug {{id: '{c['id']}', n: {len(xs)}}})""#;
    assert_eq!(
        embedded_cypher(body),
        vec!["CREATE (:Drug {id: 'x', n: x})"]
    );
    // A plain literal keeps its braces, and an identifier ending in `f` is not
    // a prefix.
    let plain = r#"elf"MATCH (n {id: 1}) RETURN n""#;
    assert_eq!(embedded_cypher(plain), vec!["MATCH (n {id: 1}) RETURN n"]);
}

#[test]
fn embedded_cypher_literals_parse() {
    let root = repo_root();
    let mut files = Vec::new();
    for rel in DOC_ROOTS {
        collect_markdown(&root.join(rel), &mut files);
    }
    files.sort();

    let mut failures = Vec::new();
    let mut checked = 0usize;

    for path in &files {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        let mut lang: Option<String> = None;
        let mut buf = String::new();
        let mut fence_line = 0usize;
        for (idx, line) in text.lines().enumerate() {
            if line.trim_start().starts_with("```") {
                if lang.as_deref().is_some_and(|l| HOST_LANGS.contains(&l)) {
                    for lit in embedded_cypher(&buf) {
                        if is_elided(&lit) {
                            continue;
                        }
                        checked += 1;
                        // A host-language literal carries no language tag, so
                        // a Locy program (`locy_with("CREATE RULE ...")`) is
                        // indistinguishable from Cypher here. Accept either.
                        if parses(&lit, Lang::Locy).is_ok() {
                            continue;
                        }
                        if let Err(e) = parses(&lit, Lang::Cypher) {
                            let rel = path.strip_prefix(&root).unwrap_or(path);
                            failures.push(format!(
                                "{}:{} {}\n    | {}",
                                rel.display(),
                                fence_line,
                                e,
                                lit.lines().next().unwrap_or("").trim()
                            ));
                        }
                    }
                }
                lang = line
                    .trim_start()
                    .strip_prefix("```")
                    .map(|s| s.split_whitespace().next().unwrap_or("").to_string())
                    .filter(|s| !s.is_empty());
                buf.clear();
                fence_line = idx + 1;
            } else {
                buf.push_str(line);
                buf.push('\n');
            }
        }
    }

    eprintln!(
        "embedded cypher literals: {checked} checked, {} failed",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} Cypher string literal(s) embedded in host-language doc blocks do not \
         parse — these fail at runtime, not render time:\n\n{}\n",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn documented_snippets_parse() {
    let root = repo_root();
    let snippets = gather();
    assert!(
        !snippets.is_empty(),
        "harness found no snippets — the doc roots moved?"
    );

    let mut failures = Vec::new();
    let mut mistagged = Vec::new();
    let mut checked = 0usize;
    let mut skipped = 0usize;

    for s in &snippets {
        if is_elided(&s.body) {
            skipped += 1;
            continue;
        }
        checked += 1;
        let Err(e) = parses(&s.body, s.lang) else {
            continue;
        };
        let rel = s.file.strip_prefix(&root).unwrap_or(&s.file);
        let head = s.body.lines().next().unwrap_or("").trim();

        // A ```cypher fence holding Locy is a tagging bug, not a syntax bug:
        // the code is correct, the fence label is wrong. Report it apart so the
        // fix is `retag`, not `rewrite`.
        if s.lang == Lang::Cypher && parses(&s.body, Lang::Locy).is_ok() {
            mistagged.push(format!("{}:{}  | {}", rel.display(), s.line, head));
            continue;
        }
        failures.push(format!(
            "{}:{} [{:?}] {}\n    | {}",
            rel.display(),
            s.line,
            s.lang,
            e,
            head
        ));
    }

    eprintln!(
        "doc snippets: {} total, {} checked, {} skipped (elided), {} mistagged, {} failed",
        snippets.len(),
        checked,
        skipped,
        mistagged.len(),
        failures.len()
    );

    assert!(
        failures.is_empty() && mistagged.is_empty(),
        "{} snippet(s) do not parse and {} fence(s) are tagged `cypher` but hold Locy:\n\n\
         -- PARSE FAILURES --\n{}\n\n-- MISTAGGED FENCES (retag to ```locy) --\n{}\n",
        failures.len(),
        mistagged.len(),
        failures.join("\n"),
        mistagged.join("\n"),
    );
}
