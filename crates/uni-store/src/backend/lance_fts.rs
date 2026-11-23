// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A full-text search that also scores documents a Lance table does not
//! hold yet, exactly as its search will once they are stored.
//!
//! A search merges unflushed rows into the hits Lance returns for flushed
//! ones. Scoring them any other way than Lance will after the flush makes the
//! ranking, and so which rows make the top `k`, change at the flush.
//!
//! Lance splits a search by index coverage. Rows its inverted index covers
//! are scored on the index's statistics alone; a flush does not change those
//! scores, since flushed rows are not indexed until the index is optimized.
//! Rows it does not cover (fragments written since the index was built, or
//! every row when there is no index) are scored by a flat BM25 pass whose
//! statistics are the index's plus those rows' own, counted after the
//! search's filter. Once flushed, the unflushed rows are such rows.
//!
//! So the search here runs the same split: the index search restricted to
//! the covered fragments, and one flat pass over the uncovered rows whose
//! statistics start from the index's plus the unflushed documents'. That pass
//! scores the uncovered rows on exactly the statistics Lance will use after
//! the flush; the unflushed documents are then scored on the same statistics.
//! The one change this cannot hide is Lance's own: a row's score moves when
//! an index optimization brings it under the index.

// Rust guideline compliant

use std::collections::HashSet;
use std::sync::Arc;

use super::fts_query::{FtsQueryNode, parse_fts_query};
use super::types::TextSearch;
use anyhow::{Result, anyhow};
use arrow_array::{Array, Float32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::scanner::Scanner;
use lance::deps::datafusion::error::DataFusionError;
use lance::deps::datafusion::execution::SendableRecordBatchStream;
use lance::deps::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use lance::index::DatasetIndexInternalExt;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::InvertedIndexParams;
use lance_index::scalar::inverted::query::{
    BooleanQuery, FtsQuery, FtsSearchParams, MatchQuery, Occur, Operator, PhraseQuery,
    collect_query_tokens,
};
use lance_index::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer;
use lance_index::scalar::inverted::{
    DocumentGranularity, FlatBm25SearchOptions, InvertedIndex, MemBM25Scorer, Scorer,
    build_global_bm25_scorer, flat_bm25_search_stream_with_options_and_scorer,
};

/// The row-id column Lance's flat search keys its output by.
const ROW_ID: &str = "_rowid";

/// The score column a Lance full-text search appends.
const SCORE: &str = "_score";

/// Sets `scanner` up for the full-text search of `query` on `column`: the top
/// `k` by BM25, with `filter` applied before the top `k` is taken.
///
/// # Errors
/// Returns an error if Lance rejects the query, limit or filter.
pub(crate) fn configure_search(
    scanner: &mut Scanner,
    column: &str,
    query: &str,
    k: usize,
    filter: Option<&str>,
) -> Result<()> {
    scanner
        .full_text_search(FullTextSearchQuery {
            query: lance_query(column, query)?,
            limit: Some(k as i64),
            wand_factor: None,
        })
        .map_err(|e| anyhow!("FTS query on '{column}': {e}"))?;
    // `k` is applied both inside the FTS query and as a scan limit: the inner
    // bound caps the BM25 candidate set, the outer one the returned rows.
    scanner
        .limit(Some(k as i64), None)
        .map_err(|e| anyhow!("FTS limit on '{column}': {e}"))?;
    if let Some(sql) = filter {
        // Prefilter: the FTS query is bounded by `k` before the filter runs,
        // so postfiltering lets excluded rows consume top-k slots. Measured
        // without this, on 50 high-scoring excluded docs and 50 matching ones,
        // `k = 10` with a filter returned zero rows while the same query
        // unfiltered returned ten.
        scanner.prefilter(true);
        scanner
            .filter(sql)
            .map_err(|e| anyhow!("FTS filter '{sql}' on '{column}': {e}"))?;
    }
    Ok(())
}

/// The Lance query for `query` (see [`super::fts_query`]) on `column`.
///
/// A plain word list, or a blank query, stays one OR match of the original
/// text, exactly the search it was before the syntax existed.
///
/// # Errors
/// Returns an error if `query` is malformed.
pub(crate) fn lance_query(column: &str, query: &str) -> Result<FtsQuery> {
    Ok(match parse_fts_query(query)? {
        Some(node) if !node.is_plain() => node_to_lance(&node, column),
        _ => MatchQuery::new(query.to_string())
            .with_column(Some(column.to_string()))
            .into(),
    })
}

/// `node` as Lance's query tree. A negated member of a group becomes the
/// group's `MUST_NOT` clause; the parser has refused groups without a
/// positive member.
fn node_to_lance(node: &FtsQueryNode, column: &str) -> FtsQuery {
    let group = |children: &[FtsQueryNode], occur: fn() -> Occur| {
        let (positive, negated) = FtsQueryNode::split_negated(children);
        FtsQuery::Boolean(BooleanQuery::new(
            positive
                .into_iter()
                .map(|c| (occur(), node_to_lance(c, column)))
                .chain(
                    negated
                        .into_iter()
                        .map(|c| (Occur::MustNot, node_to_lance(c, column))),
                ),
        ))
    };
    match node {
        FtsQueryNode::Term(t) => MatchQuery::new(t.clone())
            .with_column(Some(column.to_string()))
            .into(),
        FtsQueryNode::Phrase(p) => {
            FtsQuery::Phrase(PhraseQuery::new(p.clone()).with_column(Some(column.to_string())))
        }
        FtsQueryNode::And(children) => group(children, || Occur::Must),
        FtsQueryNode::Or(children) => group(children, || Occur::Should),
        // Only reachable inside a group, which handles it above.
        FtsQueryNode::Not(inner) => group(std::slice::from_ref(inner.as_ref()), || Occur::Should),
    }
}

/// A parsed query that needs the evaluator below: one with phrases,
/// negations, `AND` or groups. `None` for a plain word list or a blank query,
/// which Lance's flat pass scores itself.
fn structured(query: &str) -> Result<Option<FtsQueryNode>> {
    Ok(parse_fts_query(query)?.filter(|node| !node.is_plain()))
}

/// A document's analyzed tokens: each token's text and position.
struct DocTokens {
    tokens: Vec<(String, usize)>,
}

impl DocTokens {
    fn new(tokenizer: &mut Box<dyn LanceTokenizer>, text: &str) -> Self {
        let mut tokens = Vec::new();
        let mut stream = tokenizer.token_stream_for_doc(text);
        while let Some(token) = stream.next() {
            tokens.push((token.text.clone(), token.position));
        }
        Self { tokens }
    }

    fn freq(&self, token: &str) -> u32 {
        self.tokens.iter().filter(|(t, _)| t == token).count() as u32
    }

    /// Whether the query tokens `phrase` (text, position) occur at the same
    /// relative positions somewhere in the document.
    fn has_phrase(&self, phrase: &[(String, usize)]) -> bool {
        let Some((first, first_pos)) = phrase.first() else {
            return false;
        };
        let at: std::collections::HashSet<(&str, usize)> =
            self.tokens.iter().map(|(t, p)| (t.as_str(), *p)).collect();
        self.tokens.iter().any(|(t, start)| {
            t == first
                && phrase.iter().all(|(pt, pp)| {
                    (start + pp)
                        .checked_sub(*first_pos)
                        .is_some_and(|pos| at.contains(&(pt.as_str(), pos)))
                })
        })
    }
}

/// The score of `doc` for `node` on `scorer`'s statistics, `None` when it
/// does not match. A leaf scores as Lance's flat pass does, the sum over its
/// tokens of `idf * doc_weight(freq, doc_tokens)`; a group sums its matching
/// positive members, as Lance's boolean query does.
fn score_node(
    node: &FtsQueryNode,
    doc: &DocTokens,
    scorer: &MemBM25Scorer,
    tokenizer: &mut Box<dyn LanceTokenizer>,
) -> Option<f32> {
    let doc_len = doc.tokens.len() as u32;
    let leaf_score = |tokens: &[(String, usize)]| -> f32 {
        tokens
            .iter()
            .map(|(t, _)| scorer.query_weight(t) * scorer.doc_weight(doc.freq(t), doc_len))
            .sum()
    };
    let query_tokens = |tokenizer: &mut Box<dyn LanceTokenizer>, text: &str| {
        let mut out = Vec::new();
        let mut stream = tokenizer.token_stream_for_search(text);
        while let Some(token) = stream.next() {
            out.push((token.text.clone(), token.position));
        }
        out
    };
    match node {
        FtsQueryNode::Term(t) => {
            let tokens = query_tokens(tokenizer, t);
            tokens
                .iter()
                .any(|(t, _)| doc.freq(t) > 0)
                .then(|| leaf_score(&tokens))
        }
        FtsQueryNode::Phrase(p) => {
            let tokens = query_tokens(tokenizer, p);
            doc.has_phrase(&tokens).then(|| leaf_score(&tokens))
        }
        FtsQueryNode::And(children) | FtsQueryNode::Or(children) => {
            let (positive, negated) = FtsQueryNode::split_negated(children);
            if negated
                .iter()
                .any(|c| score_node(c, doc, scorer, tokenizer).is_some())
            {
                return None;
            }
            let scores: Vec<Option<f32>> = positive
                .iter()
                .map(|c| score_node(c, doc, scorer, tokenizer))
                .collect();
            let all = matches!(node, FtsQueryNode::And(_));
            let matched = if all {
                scores.iter().all(Option::is_some)
            } else {
                scores.iter().any(Option::is_some)
            };
            matched.then(|| scores.into_iter().flatten().sum())
        }
        FtsQueryNode::Not(inner) => score_node(inner, doc, scorer, tokenizer)
            .is_none()
            .then_some(0.0),
    }
}

/// `docs` scored for `node` against `scorer`'s statistics without adding to
/// them; the structured counterpart of [`rescore_checked`].
fn score_structured(
    scorer: &MemBM25Scorer,
    tokenizer: &mut Box<dyn LanceTokenizer>,
    node: &FtsQueryNode,
    docs: Vec<(u64, String)>,
    check: impl Fn() -> Result<()>,
) -> Result<Vec<(u64, f32)>> {
    let mut out = Vec::new();
    for (id, text) in docs {
        check()?;
        let doc = DocTokens::new(tokenizer, &text);
        if doc.tokens.is_empty() {
            continue;
        }
        if let Some(score) = score_node(node, &doc, scorer, tokenizer) {
            out.push((id, score));
        }
    }
    Ok(out)
}

/// The rows of a scan of `column` (with row ids), collected as documents.
async fn collect_docs(
    stream: SendableRecordBatchStream,
    column: &str,
) -> Result<Vec<(u64, String)>> {
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .map_err(|e| anyhow!("scan '{column}': {e}"))?;
    let mut docs = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name(ROW_ID)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow!("scan of '{column}' has no {ROW_ID} column"))?;
        let col = batch
            .column_by_name(column)
            .ok_or_else(|| anyhow!("scan has no '{column}' column"))?;
        let col = arrow::compute::cast(col, &DataType::Utf8)
            .map_err(|e| anyhow!("'{column}' is not text: {e}"))?;
        let texts = col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("'{column}' is not text"))?;
        for i in 0..batch.num_rows() {
            if ids.is_valid(i) && texts.is_valid(i) {
                docs.push((ids.value(i), texts.value(i).to_string()));
            }
        }
    }
    Ok(docs)
}

/// The top `k` stored rows of `dataset` for `query` on `column`, and the
/// score of each `unstored` document (id, text), both as Lance's search will
/// give them once the unstored documents are stored.
///
/// The stored hits are the dataset's rows with a `_score` column, in at most
/// two batches (index-covered, then uncovered), not sorted across them.
/// `dataset` is `None` when the table has never been written. `filter` is
/// applied to stored rows, before the top `k` and before their statistics are
/// counted, as Lance's prefiltered search does; the caller filters the
/// unstored documents. `configure` is applied to the index-covered search
/// (to attach statistics callbacks).
///
/// # Errors
/// Returns an error if an index segment cannot be opened, or a scan, the
/// index search or the flat pass fails.
pub(crate) async fn search_with_unstored(
    dataset: Option<&Dataset>,
    column: &str,
    query: &str,
    k: usize,
    filter: Option<&str>,
    unstored: Vec<(u64, String)>,
    configure: impl FnOnce(&mut Scanner),
) -> Result<TextSearch> {
    let Some(dataset) = dataset else {
        return Ok(TextSearch {
            stored: Vec::new(),
            unstored: score_documents(column, query, unstored, || Ok(())).await?,
        });
    };
    let node = structured(query)?;
    // The statistics a structured query ranks by are those of its positive
    // tokens; a plain query's are its own.
    let stats_query = node
        .as_ref()
        .map_or_else(|| query.to_string(), FtsQueryNode::positive_text);

    let (indexes, covered) = open_segments(dataset, column).await?;
    let (tokenizer, base) = match indexes.first() {
        Some(first) => {
            let mut tokenizer = first.tokenizer();
            let tokens = collect_query_tokens(&stats_query, &mut tokenizer);
            let base = build_global_bm25_scorer(&indexes, &tokens, &FtsSearchParams::new(), None)
                .await
                .map_err(|e| anyhow!("BM25 statistics for '{column}': {e}"))?;
            (first.tokenizer(), Some(base))
        }
        None => (simple_tokenizer()?, None),
    };
    let (covered_fragments, uncovered_fragments): (Vec<_>, Vec<_>) = dataset
        .fragments()
        .iter()
        .cloned()
        .partition(|f| u32::try_from(f.id).is_ok_and(|id| covered.contains(&id)));

    let mut stored = Vec::new();
    if !covered_fragments.is_empty() {
        let mut scanner = dataset.scan();
        scanner.with_fragments(covered_fragments);
        configure_search(&mut scanner, column, query, k, filter)?;
        configure(&mut scanner);
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(|e| anyhow!("FTS search on '{column}': {e}"))?
            .try_collect()
            .await
            .map_err(|e| anyhow!("FTS search on '{column}': {e}"))?;
        stored.extend(batches);
    }

    // The unflushed documents join the statistics first, so the pass over
    // the uncovered rows below scores them on the full set.
    let (_, with_docs) = flat_bm25(
        unstored_stream(column, &unstored)?,
        column,
        &stats_query,
        tokenizer.box_clone(),
        base,
    )
    .await?;
    let mut stats = with_docs;
    if !uncovered_fragments.is_empty() {
        let mut scanner = dataset.scan();
        scanner.with_fragments(uncovered_fragments);
        scanner
            .project(&[column])
            .map_err(|e| anyhow!("project '{column}': {e}"))?;
        scanner.with_row_id();
        if let Some(sql) = filter {
            scanner
                .filter(sql)
                .map_err(|e| anyhow!("filter '{sql}': {e}"))?;
        }
        let schema = scanner
            .schema()
            .await
            .map_err(|e| anyhow!("scan '{column}': {e}"))?;
        let rows = scanner
            .try_into_stream()
            .await
            .map_err(|e| anyhow!("scan '{column}': {e}"))?;
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            rows.map(|b| b.map_err(|e| DataFusionError::External(Box::new(e)))),
        ));
        let mut hits = match &node {
            None => {
                let (scored, all) =
                    flat_bm25(input, column, query, tokenizer.box_clone(), Some(stats)).await?;
                stats = all;
                scored_ids(&scored)?
            }
            Some(node) => {
                // Statistics first, over the positive tokens, then every row
                // scored on them, as the flat pass does for a plain query.
                let docs = collect_docs(input, column).await?;
                let (_, all) = flat_bm25(
                    unstored_stream(column, &docs)?,
                    column,
                    &stats_query,
                    tokenizer.box_clone(),
                    Some(stats),
                )
                .await?;
                stats = all;
                let mut tokenizer = tokenizer.box_clone();
                score_structured(&stats, &mut tokenizer, node, docs, || Ok(()))?
            }
        };
        hits.sort_by(|a, b| b.1.total_cmp(&a.1));
        hits.truncate(k);
        if !hits.is_empty() {
            stored.push(take_scored(dataset, hits).await?);
        }
    }

    let mut tokenizer = tokenizer;
    let unstored = match &node {
        None => rescore_checked(&stats, &mut tokenizer, query, unstored, || Ok(()))?,
        Some(node) => score_structured(&stats, &mut tokenizer, node, unstored, || Ok(()))?,
    };
    Ok(TextSearch { stored, unstored })
}

/// `docs` as the input stream of Lance's flat pass.
fn unstored_stream(column: &str, docs: &[(u64, String)]) -> Result<SendableRecordBatchStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(ROW_ID, DataType::UInt64, false),
        Field::new(column, DataType::Utf8, true),
    ]));
    let batches: Vec<_> = docs
        .chunks(1024)
        .map(|docs| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        docs.iter().map(|(id, _)| *id),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        docs.iter().map(|(_, t)| t.as_str()),
                    )),
                ],
            )
        })
        .collect::<std::result::Result<_, _>>()?;
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(batches.into_iter().map(Ok)),
    )))
}

/// `(row id, score)` for each row of the flat pass's output.
fn scored_ids(batches: &[RecordBatch]) -> Result<Vec<(u64, f32)>> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name(ROW_ID)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow!("flat BM25 output has no {ROW_ID} column"))?;
        let scores = batch
            .column_by_name(SCORE)
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
            .ok_or_else(|| anyhow!("flat BM25 output has no {SCORE} column"))?;
        for i in 0..batch.num_rows() {
            if ids.is_valid(i) && scores.is_valid(i) {
                out.push((ids.value(i), scores.value(i)));
            }
        }
    }
    Ok(out)
}

/// The rows `hits` names, every column, with their scores as `_score`, the
/// shape an index search returns.
async fn take_scored(dataset: &Dataset, mut hits: Vec<(u64, f32)>) -> Result<RecordBatch> {
    // Ascending ids, so the rows come back in the order the scores are in
    // whether or not the take preserves the requested order.
    hits.sort_by_key(|(id, _)| *id);
    let ids: Vec<u64> = hits.iter().map(|(id, _)| *id).collect();
    let rows = dataset
        .take_rows(&ids, dataset.schema().clone())
        .await
        .map_err(|e| anyhow!("take FTS hits: {e}"))?;
    if rows.num_rows() != ids.len() {
        return Err(anyhow!(
            "take FTS hits: asked for {} rows, got {}",
            ids.len(),
            rows.num_rows()
        ));
    }
    let mut fields: Vec<Field> = rows
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(SCORE, DataType::Float32, true));
    let mut columns = rows.columns().to_vec();
    columns.push(Arc::new(Float32Array::from_iter_values(
        hits.iter().map(|(_, s)| *s),
    )));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Flat BM25 for a visible corpus without a persistent text column.
/// Uses the same tokenizer and statistics as the unindexed dataset path.
pub(crate) async fn score_documents(
    column: &str,
    query: &str,
    docs: Vec<(u64, String)>,
    check: impl Fn() -> Result<()> + Clone + Send + Sync + 'static,
) -> Result<Vec<(u64, f32)>> {
    check()?;
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    let input = unstored_stream(column, &docs)?;
    let schema = input.schema();
    let check_stats = check.clone();
    let input = RecordBatchStreamAdapter::new(
        schema,
        input.map(move |batch| {
            check_stats().map_err(|e| DataFusionError::External(e.into()))?;
            batch
        }),
    );
    let node = structured(query)?;
    let stats_query = node
        .as_ref()
        .map_or_else(|| query.to_string(), FtsQueryNode::positive_text);
    let (_, stats) = flat_bm25(
        Box::pin(input),
        column,
        &stats_query,
        simple_tokenizer()?,
        None,
    )
    .await?;
    check()?;
    match &node {
        None => rescore_checked(&stats, &mut simple_tokenizer()?, query, docs, check),
        Some(node) => score_structured(&stats, &mut simple_tokenizer()?, node, docs, check),
    }
}

/// `docs` scored against `scorer`'s statistics without adding to them, by the
/// formula and token counting of Lance's flat pass (`flat_bm25_score`):
/// the sum over query tokens of `idf * doc_weight(freq, doc_tokens)`.
fn rescore_checked(
    scorer: &MemBM25Scorer,
    tokenizer: &mut Box<dyn LanceTokenizer>,
    query: &str,
    docs: Vec<(u64, String)>,
    check: impl Fn() -> Result<()>,
) -> Result<Vec<(u64, f32)>> {
    let tokens = collect_query_tokens(query, tokenizer);
    let mut index_of: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for i in 0..tokens.len() {
        index_of
            .entry(tokens.get_token(i).to_string())
            .or_default()
            .push(i);
    }
    let mut out = Vec::with_capacity(docs.len());
    for (id, text) in docs {
        check()?;
        let mut freqs = vec![0u32; tokens.len()];
        let mut doc_tokens = 0u32;
        let mut stream = tokenizer.token_stream_for_doc(&text);
        while let Some(token) = stream.next() {
            doc_tokens += 1;
            if doc_tokens.is_multiple_of(1024) {
                check()?;
            }
            if let Some(indices) = index_of.get(&token.text) {
                for &i in indices {
                    freqs[i] += 1;
                }
            }
        }
        if doc_tokens == 0 {
            continue;
        }
        let score: f32 = (0..tokens.len())
            .map(|i| {
                scorer.query_weight(tokens.get_token(i)) * scorer.doc_weight(freqs[i], doc_tokens)
            })
            .sum();
        if score > 0.0 {
            out.push((id, score));
        }
    }
    Ok(out)
}

/// Every committed inverted-index segment on `column`, or none, and the
/// fragments they cover between them.
async fn open_segments(
    dataset: &Dataset,
    column: &str,
) -> Result<(Vec<Arc<InvertedIndex>>, HashSet<u32>)> {
    let Some(segments) =
        lance::index::scalar::load_segments(dataset, column, DocumentGranularity::Row)
            .await
            .map_err(|e| anyhow!("FTS segments for '{column}': {e}"))?
    else {
        return Ok((Vec::new(), HashSet::new()));
    };
    let mut out = Vec::with_capacity(segments.len());
    let mut covered = HashSet::new();
    for segment in &segments {
        let index = dataset
            .open_scalar_index(column, &segment.uuid, &NoOpMetricsCollector)
            .await
            .map_err(|e| anyhow!("open FTS segment {} of '{column}': {e}", segment.uuid))?;
        let inverted = index
            .as_any()
            .downcast_ref::<InvertedIndex>()
            .ok_or_else(|| anyhow!("index {} on '{column}' is not inverted", segment.uuid))?;
        out.push(Arc::new(inverted.clone()));
        // Lance refuses to tell indexed from unindexed fragments without the
        // bitmap (`unindexed_fragments`); so does this.
        let bitmap = segment.fragment_bitmap.as_ref().ok_or_else(|| {
            anyhow!(
                "FTS segment {} of '{column}' records no fragment coverage",
                segment.uuid
            )
        })?;
        covered.extend(bitmap.iter());
    }
    Ok((out, covered))
}

/// The tokenizer Lance's flat search uses when a column has no index: a bare
/// simple tokenizer, no lowercasing, stemming or stop words.
fn simple_tokenizer() -> Result<Box<dyn LanceTokenizer>> {
    InvertedIndexParams::default()
        .base_tokenizer("simple".to_string())
        .max_token_length(None)
        .lower_case(false)
        .stem(false)
        .remove_stop_words(false)
        .ascii_folding(false)
        .build()
        .map_err(|e| anyhow!("simple tokenizer: {e}"))
}

/// Lance's flat BM25 pass, with `MatchQuery`'s defaults (`OR`, boost 1).
async fn flat_bm25(
    input: SendableRecordBatchStream,
    column: &str,
    query: &str,
    tokenizer: Box<dyn LanceTokenizer>,
    base: Option<MemBM25Scorer>,
) -> Result<(Vec<RecordBatch>, MemBM25Scorer)> {
    let (stream, scorer) = flat_bm25_search_stream_with_options_and_scorer(
        input,
        column.to_string(),
        query.to_string(),
        tokenizer,
        base,
        FlatBm25SearchOptions {
            target_batch_size: 8192,
            elapsed_compute: None,
            document_granularity: DocumentGranularity::Row,
            operator: Operator::Or,
            boost: 1.0,
            phrase_slop: None,
        },
    )
    .await
    .map_err(|e| anyhow!("flat BM25 on '{column}': {e}"))?;
    let batches = stream
        .try_collect()
        .await
        .map_err(|e| anyhow!("flat BM25 on '{column}': {e}"))?;
    Ok((batches, scorer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn schemaless_bm25_checks_budget_during_statistics() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let checks = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&checks);
        let docs = (0..2049).map(|id| (id, "supplier".to_string())).collect();
        let error = score_documents("text", "supplier", docs, move || {
            // Entry passes; cancellation happens as statistics read the corpus.
            if checks.fetch_add(1, Ordering::Relaxed) > 0 {
                anyhow::bail!("Query cancelled");
            }
            Ok(())
        })
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("Query cancelled"));
        assert!(
            error.to_string().contains("flat BM25 on 'text'"),
            "cancellation must interrupt the statistics pass, not only its caller: {error:#}"
        );
        assert!(observed.load(Ordering::Relaxed) >= 2);
    }
}
