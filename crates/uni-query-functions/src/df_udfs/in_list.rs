// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Membership tests behind the `_cypher_in` UDF.

use super::*;
use std::collections::HashMap;

#[cfg(test)]
thread_local! {
    /// Element-against-item equality checks made by this module on this thread.
    ///
    /// A deterministic stand-in for wall time: the cost of `IN` is the number
    /// of `entity_aware_eq` calls, and a test can read that exactly.
    static COMPARISONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Read and reset this thread's comparison counter.
#[cfg(test)]
pub(super) fn take_comparisons() -> u64 {
    COMPARISONS.with(|c| c.replace(0))
}

/// One element-against-item equality check, counted under test.
fn item_eq(element: &Value, item: &Value) -> Option<bool> {
    #[cfg(test)]
    COMPARISONS.with(|c| c.set(c.get() + 1));
    entity_aware_eq(element, item)
}

/// Cypher `element IN items` by a linear scan of `items`.
///
/// Three-valued: `true` on a match; otherwise `null` if any comparison was
/// indeterminate (a `null` item, or a list or map holding one); otherwise
/// `false`. A `null` element is `false` against an empty list and `null`
/// against any other.
pub(super) fn scan_membership(element: &Value, items: &[Value]) -> Value {
    if element.is_null() {
        return if items.is_empty() {
            Value::Bool(false)
        } else {
            Value::Null
        };
    }
    let mut has_null = false;
    for item in items {
        match item_eq(element, item) {
            Some(true) => return Value::Bool(true),
            None => has_null = true,
            Some(false) => {}
        }
    }
    if has_null {
        Value::Null
    } else {
        Value::Bool(false)
    }
}

/// Whether `value` is compared by a key the index can bucket.
///
/// These are the kinds for which `entity_aware_eq` never returns `None` against
/// another such kind, and returns `Some(true)` only for an equal
/// `encode_cypher_join_key` — or, for an entity against a bare `Int` id, for
/// the same id, which [`InListIndex`] looks up separately. Lists, maps, vectors,
/// paths and `null` can be indeterminate or compare structurally, so they are
/// kept out of the buckets and compared one by one.
fn is_bucketable(value: &Value) -> bool {
    value.entity_ref().is_some()
        || matches!(
            value,
            Value::Bool(_)
                | Value::Int(_)
                | Value::Float(_)
                | Value::String(_)
                | Value::Bytes(_)
                | Value::Temporal(_)
        )
}

/// The id `entity_aware_eq` compares an entity by when the other side is a
/// bare integer.
fn entity_id(value: &Value) -> Option<i64> {
    value.entity_ref().map(|e| match e {
        EntityRef::Vertex(vid) => vid.as_u64() as i64,
        EntityRef::Edge(eid) => eid.as_u64() as i64,
    })
}

/// A hash index over a list that is constant across a batch.
///
/// `x IN list` is a scan of `list` per row, which made a filter over `n` rows
/// with a literal or parameter list of `k` items cost `n × k` comparisons —
/// 38.6 ms for 2k ids and 585.8 ms for 8k (15.2x for 4x). Built once per
/// batch, this answers each row with the few items that can equal it.
///
/// Answers are exactly the scan's ([`scan_membership`]):
///
/// - A bucketable element (see [`is_bucketable`]) is compared, through the same
///   `entity_aware_eq`, against the items sharing its join key, the entities
///   or bare ids that `entity_aware_eq` equates with it across that key, and
///   every non-bucketable item. Any other item is a bucketable kind with a
///   different key, which `entity_aware_eq` answers `Some(false)` for, so
///   skipping it changes neither the match nor the `null` outcome.
/// - The key is only a necessary condition for equality (integers above 2^53
///   can share one), so a key hit is re-checked, never trusted.
/// - Any other element — a list, a map, a vector — falls back to the scan.
pub(super) struct InListIndex<'a> {
    items: &'a [Value],
    /// Join key of each bucketable item → its positions in `items`.
    buckets: HashMap<Vec<u8>, Vec<usize>>,
    /// Entity id of each entity item → its positions, for a bare `Int` element.
    entity_ids: HashMap<i64, Vec<usize>>,
    /// Positions of the items that are not bucketable, `null` among them.
    residual: Vec<usize>,
}

impl<'a> InListIndex<'a> {
    /// Index `items`; O(`items.len()`).
    pub(super) fn new(items: &'a [Value]) -> Self {
        let mut buckets: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        let mut entity_ids: HashMap<i64, Vec<usize>> = HashMap::new();
        let mut residual = Vec::new();
        for (pos, item) in items.iter().enumerate() {
            match encode_cypher_join_key(item) {
                Some(key) if is_bucketable(item) => {
                    buckets.entry(key).or_default().push(pos);
                    if let Some(id) = entity_id(item) {
                        entity_ids.entry(id).or_default().push(pos);
                    }
                }
                _ => residual.push(pos),
            }
        }
        Self {
            items,
            buckets,
            entity_ids,
            residual,
        }
    }

    /// Cypher `element IN items`, with the scan's exact three-valued answer.
    pub(super) fn contains(&self, element: &Value) -> Value {
        if element.is_null() || !is_bucketable(element) {
            return scan_membership(element, self.items);
        }
        let none: &[usize] = &[];
        let bucket = |key: Option<Vec<u8>>| {
            key.and_then(|k| self.buckets.get(&k))
                .map_or(none, Vec::as_slice)
        };
        // Items equal to the element under its own key.
        let same_key = bucket(encode_cypher_join_key(element));
        // An entity equals a bare `Int` of its id, which is keyed as an integer.
        let id_items = entity_id(element).map_or(none, |id| bucket(Some(int_key(id))));
        // A bare `Int` equals an entity of that id, which is keyed as an entity.
        let entity_items = match element {
            Value::Int(i) => self.entity_ids.get(i).map_or(none, Vec::as_slice),
            _ => none,
        };
        let mut has_null = false;
        for &pos in same_key
            .iter()
            .chain(id_items)
            .chain(entity_items)
            .chain(&self.residual)
        {
            match item_eq(element, &self.items[pos]) {
                Some(true) => return Value::Bool(true),
                None => has_null = true,
                Some(false) => {}
            }
        }
        if has_null {
            Value::Null
        } else {
            Value::Bool(false)
        }
    }
}

/// The join key of the integer `i`.
fn int_key(i: i64) -> Vec<u8> {
    encode_cypher_join_key(&Value::Int(i)).unwrap_or_default()
}

/// `_cypher_in` over a list argument that is constant across the batch.
///
/// Returns `None` — use the per-row path — unless the list is a scalar (a
/// literal or a parameter) and the element a column of at least
/// [`MIN_INDEXED_ROWS`] rows. A list that arrives as a column is handled per
/// run of identical rows by [`invoke_column_list`].
pub(super) fn invoke_constant_list(args: &ScalarFunctionArgs) -> DFResult<Option<ColumnarValue>> {
    let [ColumnarValue::Array(elements), ColumnarValue::Scalar(list)] = args.args.as_slice() else {
        return Ok(None);
    };
    let rows = elements.len();
    if rows < MIN_INDEXED_ROWS {
        return Ok(None);
    }
    let list = scalar_to_value(list)?;
    let items = match &list {
        Value::Null => {
            return Ok(Some(ColumnarValue::Array(Arc::new(
                arrow_array::BooleanArray::new_null(rows),
            ))));
        }
        Value::List(items) => items.as_slice(),
        other => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "_cypher_in(): second argument must be a list, got {other:?}"
            )));
        }
    };
    let index = InListIndex::new(items);
    let field = args.arg_fields.first().map(|f| f.as_ref());
    let mut out = arrow_array::builder::BooleanBuilder::with_capacity(rows);
    for row in 0..rows {
        let element = get_value_from_array(elements, row, field)?;
        match index.contains(&element) {
            Value::Bool(b) => out.append_value(b),
            _ => out.append_null(),
        }
    }
    Ok(Some(ColumnarValue::Array(Arc::new(out.finish()))))
}

/// `_cypher_in` over a list argument that arrives as a column.
///
/// A list bound by `WITH` or `collect()` reaches the membership test once per
/// row, and rows that come from one `UNWIND` of it carry the same list in a
/// run (the LDBC IC5 shape, `WITH collect(f) AS fs UNWIND … WHERE x IN fs`).
/// Scanning it per row made the test `rows × len`; this splits the column
/// into runs of identical rows, decodes each run's list once, and answers a
/// run of two or more rows from an [`InListIndex`] built for it, a single row
/// by the scan. The answers are the scan's, row by row.
///
/// Rows are identical when their bytes are (a CypherValue `LargeBinary`), or
/// when their Arrow values are, for any other list encoding — exact equality,
/// never Cypher equality, so a run never merges two lists the scan could
/// answer differently.
///
/// Returns `None` — use the per-row path — unless the list is a column of at
/// least [`MIN_INDEXED_ROWS`] rows.
pub(super) fn invoke_column_list(args: &ScalarFunctionArgs) -> DFResult<Option<ColumnarValue>> {
    let [element_arg, ColumnarValue::Array(lists)] = args.args.as_slice() else {
        return Ok(None);
    };
    let rows = lists.len();
    if rows < MIN_INDEXED_ROWS {
        return Ok(None);
    }
    let element_field = args.arg_fields.first().map(|f| f.as_ref());
    let list_field = args.arg_fields.get(1).map(|f| f.as_ref());
    let constant_element = match element_arg {
        ColumnarValue::Scalar(s) => Some(scalar_to_value(s)?),
        ColumnarValue::Array(_) => None,
    };
    let element_at = |row: usize| -> DFResult<Value> {
        match (element_arg, &constant_element) {
            (_, Some(v)) => Ok(v.clone()),
            (ColumnarValue::Array(arr), None) => get_value_from_array(arr, row, element_field),
            (ColumnarValue::Scalar(s), None) => scalar_to_value(s),
        }
    };
    let same_row = |a: usize, b: usize| -> bool {
        if lists.is_null(a) || lists.is_null(b) {
            return false;
        }
        match (large_binary_row(lists, a), large_binary_row(lists, b)) {
            (Some(x), Some(y)) => x == y,
            _ => lists.slice(a, 1).to_data() == lists.slice(b, 1).to_data(),
        }
    };
    let mut out = arrow_array::builder::BooleanBuilder::with_capacity(rows);
    let mut start = 0;
    while start < rows {
        let mut end = start + 1;
        while end < rows && same_row(start, end) {
            end += 1;
        }
        let list = if lists.is_null(start) {
            Value::Null
        } else {
            get_value_from_array(lists, start, list_field)?
        };
        let items = match &list {
            Value::Null => {
                out.append_nulls(end - start);
                start = end;
                continue;
            }
            Value::List(items) => items.as_slice(),
            other => {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "_cypher_in(): second argument must be a list, got {other:?}"
                )));
            }
        };
        let index = (end - start >= MIN_INDEXED_ROWS).then(|| InListIndex::new(items));
        for row in start..end {
            let element = element_at(row)?;
            let answer = match &index {
                Some(index) => index.contains(&element),
                None => scan_membership(&element, items),
            };
            match answer {
                Value::Bool(b) => out.append_value(b),
                _ => out.append_null(),
            }
        }
        start = end;
    }
    Ok(Some(ColumnarValue::Array(Arc::new(out.finish()))))
}

/// Fewest element rows for which [`invoke_constant_list`] builds an index.
///
/// Building costs about one scan of the list, so a single row gains nothing;
/// two rows already save a scan.
const MIN_INDEXED_ROWS: usize = 2;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use arrow_array::{Array, BooleanArray, LargeBinaryArray};
    use datafusion::config::ConfigOptions;
    use std::collections::HashMap;
    use uni_common::TemporalValue;
    use uni_common::core::id::{Eid, Vid};
    use uni_common::value::{Edge, Node};

    const BIG: i64 = 1 << 53;

    fn node(id: u64) -> Value {
        Value::Node(Node {
            vid: Vid::new(id),
            labels: vec!["P".into()],
            properties: HashMap::from([("k".to_string(), Value::Int(id as i64))]),
        })
    }

    fn node_map(id: u64) -> Value {
        Value::Map(HashMap::from([
            ("_vid".to_string(), Value::Int(id as i64)),
            (
                "_labels".to_string(),
                Value::List(vec![Value::String("P".into())]),
            ),
        ]))
    }

    fn edge(id: u64) -> Value {
        Value::Edge(Edge {
            eid: Eid::new(id),
            edge_type: "R".into(),
            src: Vid::new(1),
            dst: Vid::new(2),
            properties: HashMap::new(),
        })
    }

    fn map(entries: &[(&str, Value)]) -> Value {
        Value::Map(
            entries
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        )
    }

    fn date(days: i32) -> Value {
        Value::Temporal(TemporalValue::Date {
            days_since_epoch: days,
        })
    }

    fn datetime(nanos: i64, offset: i32, tz: Option<&str>) -> Value {
        Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch: nanos,
            offset_seconds: offset,
            timezone_name: tz.map(str::to_string),
        })
    }

    /// Every kind `IN` must answer for, including the values where Cypher
    /// equality is not transitive (around 2^53) or not reflexive (NaN).
    fn values() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Int(0),
            Value::Int(1),
            Value::Int(2),
            Value::Int(-1),
            Value::Int(5),
            Value::Int(BIG),
            Value::Int(BIG + 1),
            Value::Float(0.0),
            Value::Float(-0.0),
            Value::Float(1.0),
            Value::Float(2.5),
            Value::Float(BIG as f64),
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Bool(true),
            Value::Bool(false),
            Value::String(String::new()),
            Value::String("1".into()),
            Value::String("a".into()),
            Value::Bytes(vec![1, 2]),
            Value::List(vec![]),
            Value::List(vec![Value::Int(1)]),
            Value::List(vec![Value::Float(1.0)]),
            Value::List(vec![Value::Int(1), Value::Null]),
            Value::List(vec![Value::Null]),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
            map(&[("k", Value::Int(1))]),
            map(&[("k", Value::Float(1.0))]),
            map(&[("k", Value::Null)]),
            map(&[("_vid", Value::Int(5))]),
            node(5),
            node(6),
            node_map(5),
            edge(5),
            date(1),
            datetime(86_400_000_000_000, 0, None),
            datetime(5, 0, None),
            datetime(5, 3600, Some("Europe/Paris")),
            Value::Temporal(TemporalValue::Duration {
                months: 1,
                days: 0,
                nanos: 0,
            }),
            Value::Vector(vec![1.0, 2.0]),
        ]
    }

    /// The lists probed: each value alone, the whole corpus, and the shapes
    /// whose `null`-propagation differs.
    fn lists() -> Vec<Vec<Value>> {
        let all = values();
        let mut lists: Vec<Vec<Value>> = all.iter().map(|v| vec![v.clone()]).collect();
        lists.push(vec![]);
        lists.push(all.clone());
        lists.push(all.iter().filter(|v| !v.is_null()).cloned().collect());
        lists.push(vec![Value::Int(1), Value::Null, Value::String("1".into())]);
        lists.push(vec![Value::Int(BIG + 1), Value::Int(BIG + 2)]);
        lists.push(vec![
            Value::List(vec![Value::Int(1), Value::Null]),
            Value::Int(7),
        ]);
        lists.push(vec![map(&[("k", Value::Null)]), Value::Int(7)]);
        lists.push((0..64).map(Value::Int).collect());
        lists.push(vec![node(5), node(6), Value::Int(9)]);
        lists.push(vec![Value::Int(6), Value::Int(7)]);
        lists
    }

    fn element_array(elements: &[Value]) -> ArrayRef {
        let encoded: Vec<Option<Vec<u8>>> = elements
            .iter()
            .map(|v| (!v.is_null()).then(|| uni_common::cypher_value_codec::encode(v)))
            .collect();
        Arc::new(LargeBinaryArray::from_iter(encoded))
    }

    fn invoke(element: ArrayRef, list: ColumnarValue) -> Vec<Option<bool>> {
        let rows = element.len();
        let args = ScalarFunctionArgs {
            arg_fields: vec![
                Arc::new(Field::new("e", DataType::LargeBinary, true)),
                Arc::new(Field::new("l", DataType::LargeBinary, true)),
            ],
            args: vec![ColumnarValue::Array(element), list],
            number_rows: rows,
            return_field: Arc::new(Field::new("r", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let out = create_cypher_in_udf()
            .invoke_with_args(args)
            .expect("_cypher_in")
            .into_array(rows)
            .expect("array");
        let out = out
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean");
        (0..out.len())
            .map(|i| (!out.is_null(i)).then(|| out.value(i)))
            .collect()
    }

    /// `elements IN list` with `list` a batch constant, as a literal or a
    /// parameter arrives.
    fn constant(elements: &[Value], list: &[Value]) -> Vec<Option<bool>> {
        let bytes = uni_common::cypher_value_codec::encode(&Value::List(list.to_vec()));
        invoke(
            element_array(elements),
            ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes))),
        )
    }

    /// The reference: [`scan_membership`] per element, with no index.
    fn scan(elements: &[Value], list: &[Value]) -> Vec<Option<bool>> {
        elements
            .iter()
            .map(|e| match scan_membership(e, list) {
                Value::Bool(b) => Some(b),
                _ => None,
            })
            .collect()
    }

    /// `elements[i] IN lists[i]`, the lists as a column — a collected or
    /// `WITH`-bound list — with `None` for a `NULL` list.
    fn column(elements: &[Value], lists: &[Option<Vec<Value>>]) -> Vec<Option<bool>> {
        let encoded: Vec<Option<Vec<u8>>> = lists
            .iter()
            .map(|l| {
                l.as_ref()
                    .map(|l| uni_common::cypher_value_codec::encode(&Value::List(l.clone())))
            })
            .collect();
        invoke(
            element_array(elements),
            ColumnarValue::Array(Arc::new(LargeBinaryArray::from_iter(encoded))),
        )
    }

    /// The same, with `list` repeated per row as a column: one run.
    fn per_row(elements: &[Value], list: &[Value]) -> Vec<Option<bool>> {
        let bytes = uni_common::cypher_value_codec::encode(&Value::List(list.to_vec()));
        let column: ArrayRef = Arc::new(LargeBinaryArray::from_iter_values(std::iter::repeat_n(
            bytes.as_slice(),
            elements.len(),
        )));
        invoke(element_array(elements), ColumnarValue::Array(column))
    }

    /// The constant-list path and the column path answer exactly what the
    /// scan answers, for every element against every list in the matrix.
    #[test]
    fn constant_list_matches_per_row_scan() {
        let elements = values();
        let (mut trues, mut nulls, mut falses) = (0, 0, 0);
        for list in lists() {
            let got = constant(&elements, &list);
            let got_column = per_row(&elements, &list);
            let want = scan(&elements, &list);
            for (i, ((g, c), w)) in got.iter().zip(&got_column).zip(&want).enumerate() {
                assert_eq!(
                    g, w,
                    "{:?} IN {list:?}: constant list said {g:?}, scan {w:?}",
                    elements[i]
                );
                assert_eq!(
                    c, w,
                    "{:?} IN {list:?}: list column said {c:?}, scan {w:?}",
                    elements[i]
                );
                match w {
                    Some(true) => trues += 1,
                    Some(false) => falses += 1,
                    None => nulls += 1,
                }
            }
        }
        // Discrimination floor: all three outcomes are exercised in bulk, so
        // agreement is not two paths both answering one constant.
        assert!(
            trues > 100 && nulls > 100 && falses > 1000,
            "{trues}/{nulls}/{falses}"
        );
    }

    /// Known Cypher answers on the constant path, so the differential above is
    /// anchored to the language and not only to the old scan.
    #[test]
    fn constant_list_known_answers() {
        let one = |e: Value, l: Vec<Value>| constant(&[e, Value::Int(-99)], &l)[0];
        assert_eq!(one(Value::Int(1), vec![Value::Float(1.0)]), Some(true));
        assert_eq!(one(Value::Float(1.0), vec![Value::Int(1)]), Some(true));
        assert_eq!(one(Value::Int(2), vec![Value::Int(1), Value::Null]), None);
        assert_eq!(
            one(Value::Int(1), vec![Value::Int(1), Value::Null]),
            Some(true)
        );
        assert_eq!(one(Value::Null, vec![]), Some(false));
        assert_eq!(one(Value::Null, vec![Value::Int(1)]), None);
        assert_eq!(
            one(Value::Float(f64::NAN), vec![Value::Float(f64::NAN)]),
            Some(false)
        );
        assert_eq!(one(Value::Int(BIG + 1), vec![Value::Int(BIG)]), Some(false));
        // Exact: `2^53 + 1` is not the float `2^53`, which `2^53` is.
        assert_eq!(
            one(Value::Int(BIG + 1), vec![Value::Float(BIG as f64)]),
            Some(false)
        );
        assert_eq!(
            one(Value::Int(BIG), vec![Value::Float(BIG as f64)]),
            Some(true)
        );
        assert_eq!(
            one(Value::String("1".into()), vec![Value::Int(1)]),
            Some(false)
        );
        assert_eq!(one(node(5), vec![Value::Int(5)]), Some(true));
        assert_eq!(one(Value::Int(5), vec![node(5)]), Some(true));
        assert_eq!(one(node_map(5), vec![node(5)]), Some(true));
        assert_eq!(one(edge(5), vec![node(5)]), Some(false));
        assert_eq!(one(node(5), vec![Value::Float(5.0)]), Some(false));
        let l1 = Value::List(vec![Value::Int(1)]);
        let l1f = Value::List(vec![Value::Float(1.0)]);
        let l_null = Value::List(vec![Value::Null]);
        assert_eq!(one(l1.clone(), vec![l1f]), Some(true));
        assert_eq!(one(l1.clone(), vec![l_null.clone()]), None);
        // A list holding a null is indeterminate against a scalar-only list's
        // list items, but plainly unequal to scalars.
        assert_eq!(one(l_null, vec![Value::Int(1)]), Some(false));
        assert_eq!(
            one(
                map(&[("k", Value::Int(1))]),
                vec![map(&[("k", Value::Null)])]
            ),
            None
        );
        assert_eq!(one(date(1), vec![date(1), Value::Null]), Some(true));
        assert_eq!(one(date(2), vec![date(1), Value::Null]), None);
    }

    /// Comparisons made for `rows` misses against a constant list of `len`.
    fn constant_cost(rows: i64, len: i64) -> u64 {
        let elements: Vec<Value> = (0..rows).map(|i| Value::Int(1_000_000 + i)).collect();
        let list: Vec<Value> = (0..len).map(Value::Int).collect();
        take_comparisons();
        let got = constant(&elements, &list);
        assert!(got.iter().all(|r| *r == Some(false)));
        take_comparisons()
    }

    /// A constant list is probed, not scanned: the comparisons per row do not
    /// grow with the list. The per-row scan makes `rows × len`.
    #[test]
    fn constant_list_cost_does_not_grow_with_list() {
        let small = constant_cost(512, 256);
        let large = constant_cost(512, 4096);
        assert!(
            large <= small,
            "4096-item list made {large} comparisons, 256-item list {small}"
        );
        assert!(large < 512 * 4, "{large} comparisons for 512 rows");

        // Control: a list that differs on every row is a run of one per row,
        // answered by the linear scan this guards against.
        let elements: Vec<Value> = (0..64).map(|i| Value::Int(1_000_000 + i)).collect();
        let distinct: Vec<Option<Vec<Value>>> = (0..64)
            .map(|r| Some((0..256).map(|i| Value::Int(r * 1000 + i)).collect()))
            .collect();
        take_comparisons();
        column(&elements, &distinct);
        assert_eq!(take_comparisons(), 64 * 256);
    }

    /// A list column whose rows repeat in runs: lists change between runs,
    /// some hold `NULL`, some rows are a `NULL` list, a run may be one row.
    /// Every row answers what the scan of its own list answers.
    #[test]
    fn list_column_runs_match_the_scan() {
        let pool = lists();
        let elements = values();
        let mut row_elements = Vec::new();
        let mut row_lists: Vec<Option<Vec<Value>>> = Vec::new();
        // Runs of 1, 2, 3, ... rows, cycling through the lists, with a NULL
        // list every fifth run, and the same list twice in a row once.
        for run in 0..60usize {
            let list = if run % 5 == 4 {
                None
            } else {
                Some(pool[(run / 2) % pool.len()].clone())
            };
            for r in 0..(run % 4 + 1) {
                // Every other row probes an item of its own list.
                let element = match &list {
                    Some(l) if r % 2 == 0 && !l.is_empty() => l[(run + r) % l.len()].clone(),
                    _ => elements[(run * 7 + r * 3) % elements.len()].clone(),
                };
                row_elements.push(element);
                row_lists.push(list.clone());
            }
        }
        let got = column(&row_elements, &row_lists);
        let (mut trues, mut nulls, mut falses) = (0, 0, 0);
        for (i, g) in got.iter().enumerate() {
            let want = match &row_lists[i] {
                None => None,
                Some(list) => scan(&row_elements[i..=i], list)[0],
            };
            assert_eq!(
                *g, want,
                "{:?} IN {:?}: list column said {g:?}, scan {want:?}",
                row_elements[i], row_lists[i]
            );
            match want {
                Some(true) => trues += 1,
                Some(false) => falses += 1,
                None => nulls += 1,
            }
        }
        assert!(
            trues > 5 && nulls > 5 && falses > 5,
            "{trues}/{nulls}/{falses}"
        );
    }

    /// Comparisons made for `groups` runs of `rows` misses against a list
    /// column whose runs hold `len` items each.
    fn column_runs_cost(groups: i64, rows: i64, len: i64) -> u64 {
        let mut elements = Vec::new();
        let mut lists = Vec::new();
        for g in 0..groups {
            let list: Vec<Value> = (0..len).map(|i| Value::Int(g * 100_000 + i)).collect();
            for r in 0..rows {
                elements.push(Value::Int(1_000_000_000 + g * 1000 + r));
                lists.push(Some(list.clone()));
            }
        }
        take_comparisons();
        let got = column(&elements, &lists);
        assert!(got.iter().all(|r| *r == Some(false)));
        take_comparisons()
    }

    /// A list column repeated in runs is probed per row, not scanned: the
    /// comparisons do not grow with the list (IC5).
    #[test]
    fn list_column_runs_cost_does_not_grow_with_list() {
        let small = column_runs_cost(8, 64, 256);
        let large = column_runs_cost(8, 64, 4096);
        assert!(
            large <= small,
            "4096-item runs made {large} comparisons, 256-item runs {small}"
        );
        assert!(large < 8 * 64 * 4, "{large} comparisons for 512 rows");
    }
}
