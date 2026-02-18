// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use uni_algo::algo::AlgorithmRegistry;
use uni_common::{TemporalType, TemporalValue, Value};
use uni_cypher::ast::{BinaryOp, Expr};
use uni_store::QueryContext;
use uni_store::runtime::l0_manager::L0Manager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

use crate::query::expr_eval::eval_binary_op;
use crate::query::datetime::{classify_temporal, eval_datetime_function};

use super::procedure::ProcedureRegistry;

#[derive(Debug)]
pub(crate) enum Accumulator {
    Count(i64),
    Sum(f64),
    Min(Option<Value>),
    Max(Option<Value>),
    Avg { sum: f64, count: i64 },
    Collect(Vec<Value>),
    CountDistinct(HashSet<String>),
    PercentileDisc { values: Vec<f64>, percentile: f64 },
    PercentileCont { values: Vec<f64>, percentile: f64 },
}

/// Convert f64 to Value, preserving integer representation when possible.
fn numeric_to_value(val: f64) -> Value {
    if val.fract() == 0.0 && val >= i64::MIN as f64 && val <= i64::MAX as f64 {
        Value::Int(val as i64)
    } else {
        Value::Float(val)
    }
}

/// Cross-type ordering rank for Cypher min/max (lower rank = smaller).
fn cypher_type_rank(val: &Value) -> u8 {
    match val {
        Value::Null => 0,
        Value::List(_) => 1,
        Value::String(_) => 2,
        Value::Bool(_) => 3,
        Value::Int(_) | Value::Float(_) => 4,
        _ => 5,
    }
}

/// Compare two Cypher values for min/max with cross-type ordering.
fn cypher_cross_type_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let ra = cypher_type_rank(a);
    let rb = cypher_type_rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Int(l), Value::Int(r)) => l.cmp(r),
        (Value::Float(l), Value::Float(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Int(l), Value::Float(r)) => (*l as f64).partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Float(l), Value::Int(r)) => l.partial_cmp(&(*r as f64)).unwrap_or(Ordering::Equal),
        (Value::String(l), Value::String(r)) => l.cmp(r),
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        _ => Ordering::Equal,
    }
}

impl Accumulator {
    pub(crate) fn new(op: &str, distinct: bool) -> Self {
        Self::new_with_percentile(op, distinct, 0.0)
    }

    pub(crate) fn new_with_percentile(op: &str, distinct: bool, percentile: f64) -> Self {
        let op_upper = op.to_uppercase();
        match op_upper.as_str() {
            "COUNT" if distinct => Accumulator::CountDistinct(HashSet::new()),
            "COUNT" => Accumulator::Count(0),
            "SUM" => Accumulator::Sum(0.0),
            "MIN" => Accumulator::Min(None),
            "MAX" => Accumulator::Max(None),
            "AVG" => Accumulator::Avg { sum: 0.0, count: 0 },
            "COLLECT" => Accumulator::Collect(Vec::new()),
            "PERCENTILEDISC" => Accumulator::PercentileDisc {
                values: Vec::new(),
                percentile,
            },
            "PERCENTILECONT" => Accumulator::PercentileCont {
                values: Vec::new(),
                percentile,
            },
            _ => Accumulator::Count(0),
        }
    }

    pub(crate) fn update(&mut self, val: &Value, is_wildcard: bool) {
        match self {
            Accumulator::Count(c) => {
                if is_wildcard || !val.is_null() {
                    *c += 1;
                }
            }
            Accumulator::Sum(s) => {
                if let Some(n) = val.as_f64() {
                    *s += n;
                }
            }
            Accumulator::Min(current) => {
                if !val.is_null() {
                    *current = Some(match current.take() {
                        None => val.clone(),
                        Some(cur) => {
                            if cypher_cross_type_cmp(val, &cur) == std::cmp::Ordering::Less {
                                val.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
            Accumulator::Max(current) => {
                if !val.is_null() {
                    *current = Some(match current.take() {
                        None => val.clone(),
                        Some(cur) => {
                            if cypher_cross_type_cmp(val, &cur) == std::cmp::Ordering::Greater {
                                val.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
            Accumulator::Avg { sum, count } => {
                if let Some(n) = val.as_f64() {
                    *sum += n;
                    *count += 1;
                }
            }
            Accumulator::Collect(v) => {
                if !val.is_null() {
                    v.push(val.clone());
                }
            }
            Accumulator::CountDistinct(s) => {
                if !val.is_null() {
                    s.insert(val.to_string());
                }
            }
            Accumulator::PercentileDisc { values, .. }
            | Accumulator::PercentileCont { values, .. } => {
                if let Some(n) = val.as_f64() {
                    values.push(n);
                }
            }
        }
    }

    pub(crate) fn finish(&self) -> Value {
        match self {
            Accumulator::Count(c) => Value::Int(*c),
            Accumulator::Sum(s) => numeric_to_value(*s),
            Accumulator::Min(opt) => opt.as_ref().cloned().unwrap_or(Value::Null),
            Accumulator::Max(opt) => opt.as_ref().cloned().unwrap_or(Value::Null),
            Accumulator::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float(*sum / (*count as f64))
                } else {
                    Value::Null
                }
            }
            Accumulator::Collect(v) => Value::List(v.clone()),
            Accumulator::CountDistinct(s) => Value::Int(s.len() as i64),
            Accumulator::PercentileDisc { values, percentile } => {
                if values.is_empty() {
                    return Value::Null;
                }
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = sorted.len();
                let idx = (percentile * (n as f64 - 1.0)).round() as usize;
                let idx = idx.min(n - 1);
                numeric_to_value(sorted[idx])
            }
            Accumulator::PercentileCont { values, percentile } => {
                if values.is_empty() {
                    return Value::Null;
                }
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = sorted.len();
                if n == 1 {
                    return Value::Float(sorted[0]);
                }
                let pos = percentile * (n as f64 - 1.0);
                let lower = (pos.floor() as usize).min(n - 1);
                let upper = (pos.ceil() as usize).min(n - 1);
                if lower == upper {
                    Value::Float(sorted[lower])
                } else {
                    let frac = pos - lower as f64;
                    Value::Float(sorted[lower] + frac * (sorted[upper] - sorted[lower]))
                }
            }
        }
    }
}

/// Cache key for parsed generation expressions: (label_name, property_name)
pub(crate) type GenExprCacheKey = (String, String);

#[derive(Clone)]
pub struct Executor {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) writer: Option<Arc<RwLock<Writer>>>,
    pub(crate) l0_manager: Option<Arc<L0Manager>>,
    pub(crate) algo_registry: Arc<AlgorithmRegistry>,
    pub(crate) use_transaction: bool,
    /// File sandbox configuration for BACKUP/COPY/EXPORT commands
    pub(crate) file_sandbox: uni_common::config::FileSandboxConfig,
    pub(crate) config: uni_common::config::UniConfig,
    /// Cache for parsed generation expressions to avoid re-parsing on every row
    pub(crate) gen_expr_cache: Arc<RwLock<HashMap<GenExprCacheKey, Expr>>>,
    /// External procedure registry for test/user-defined procedures.
    pub(crate) procedure_registry: Option<Arc<ProcedureRegistry>>,
}

impl Executor {
    pub fn new(storage: Arc<StorageManager>) -> Self {
        Self {
            storage,
            writer: None,
            l0_manager: None,
            algo_registry: Arc::new(AlgorithmRegistry::new()),
            use_transaction: false,
            file_sandbox: uni_common::config::FileSandboxConfig::default(),
            config: uni_common::config::UniConfig::default(),
            gen_expr_cache: Arc::new(RwLock::new(HashMap::new())),
            procedure_registry: None,
        }
    }

    pub fn new_with_writer(storage: Arc<StorageManager>, writer: Arc<RwLock<Writer>>) -> Self {
        let mut executor = Self::new(storage);
        executor.writer = Some(writer);
        executor
    }

    /// Sets the external procedure registry for user-defined procedures.
    pub fn set_procedure_registry(&mut self, registry: Arc<ProcedureRegistry>) {
        self.procedure_registry = Some(registry);
    }

    /// Set the file sandbox configuration for BACKUP/COPY/EXPORT commands.
    /// MUST be called with sandboxed config in server mode.
    pub fn set_file_sandbox(&mut self, sandbox: uni_common::config::FileSandboxConfig) {
        self.file_sandbox = sandbox;
    }

    pub fn set_config(&mut self, config: uni_common::config::UniConfig) {
        self.config = config;
    }

    /// Validate a file path against the sandbox configuration.
    pub(crate) fn validate_path(&self, path: &str) -> Result<std::path::PathBuf> {
        self.file_sandbox
            .validate_path(path)
            .map_err(|e| anyhow!("Path validation failed: {}", e))
    }

    pub fn set_writer(&mut self, writer: Arc<RwLock<Writer>>) {
        self.writer = Some(writer);
    }

    pub fn set_use_transaction(&mut self, use_transaction: bool) {
        self.use_transaction = use_transaction;
    }

    pub(crate) async fn get_context(&self) -> Option<QueryContext> {
        if let Some(writer_lock) = &self.writer {
            let writer = writer_lock.read().await;
            // Include pending_flush L0s so data being flushed remains visible
            let mut ctx = QueryContext::new_with_pending(
                writer.l0_manager.get_current(),
                writer.transaction_l0.clone(),
                writer.l0_manager.get_pending_flush(),
            );
            ctx.set_deadline(Instant::now() + self.config.query_timeout);
            Some(ctx)
        } else {
            self.l0_manager.as_ref().map(|m| {
                let mut ctx = QueryContext::new(m.get_current());
                ctx.set_deadline(Instant::now() + self.config.query_timeout);
                ctx
            })
        }
    }

    pub(crate) fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        let temporal_a = Self::extract_temporal_value(a);
        let temporal_b = Self::extract_temporal_value(b);

        if let (Some(ta), Some(tb)) = (&temporal_a, &temporal_b) {
            return Self::compare_temporal(ta, tb);
        }

        // Temporal strings (e.g. "1984-10-11T...") and Value::Temporal should
        // compare using Cypher temporal semantics when compatible.
        if matches!(
            (a, b),
            (Value::String(_), Value::Temporal(_)) | (Value::Temporal(_), Value::String(_))
        ) && let Some(ord) = Self::try_eval_ordering(a, b)
        {
            return ord;
        }
        if let (Value::String(_), Some(tb)) = (a, temporal_b)
            && let Some(ord) = Self::try_eval_ordering(a, &Value::Temporal(tb))
        {
            return ord;
        }
        if let (Some(ta), Value::String(_)) = (temporal_a, b)
            && let Some(ord) = Self::try_eval_ordering(&Value::Temporal(ta), b)
        {
            return ord;
        }

        let ra = Self::order_by_type_rank(a);
        let rb = Self::order_by_type_rank(b);
        if ra != rb {
            return ra.cmp(&rb);
        }

        match (a, b) {
            (Value::Map(l), Value::Map(r)) => Self::compare_maps(l, r),
            (Value::Node(l), Value::Node(r)) => Self::compare_nodes(l, r),
            (Value::Edge(l), Value::Edge(r)) => Self::compare_edges(l, r),
            (Value::List(l), Value::List(r)) => Self::compare_lists(l, r),
            (Value::Path(l), Value::Path(r)) => Self::compare_paths(l, r),
            (Value::String(l), Value::String(r)) => {
                let lv = Value::String(l.clone());
                let rv = Value::String(r.clone());

                if matches!(
                    eval_binary_op(&lv, &BinaryOp::Lt, &rv),
                    Ok(Value::Bool(true))
                ) {
                    std::cmp::Ordering::Less
                } else if matches!(
                    eval_binary_op(&lv, &BinaryOp::Gt, &rv),
                    Ok(Value::Bool(true))
                ) {
                    std::cmp::Ordering::Greater
                } else {
                    l.cmp(r)
                }
            }
            (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
            (Value::Temporal(l), Value::Temporal(r)) => Self::compare_temporal(l, r),
            (Value::Int(l), Value::Int(r)) => l.cmp(r),
            (Value::Float(l), Value::Float(r)) => {
                if l.is_nan() && r.is_nan() {
                    std::cmp::Ordering::Equal
                } else if l.is_nan() {
                    std::cmp::Ordering::Greater
                } else if r.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
                }
            }
            (Value::Int(l), Value::Float(r)) => {
                if r.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    (*l as f64)
                        .partial_cmp(r)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            }
            (Value::Float(l), Value::Int(r)) => {
                if l.is_nan() {
                    std::cmp::Ordering::Greater
                } else {
                    l.partial_cmp(&(*r as f64))
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            }
            (Value::Bytes(l), Value::Bytes(r)) => l.cmp(r),
            (Value::Vector(l), Value::Vector(r)) => {
                let min_len = l.len().min(r.len());
                for i in 0..min_len {
                    let ord = l[i].total_cmp(&r[i]);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                l.len().cmp(&r.len())
            }
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn try_eval_ordering(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        if matches!(eval_binary_op(a, &BinaryOp::Lt, b), Ok(Value::Bool(true))) {
            Some(std::cmp::Ordering::Less)
        } else if matches!(eval_binary_op(a, &BinaryOp::Gt, b), Ok(Value::Bool(true))) {
            Some(std::cmp::Ordering::Greater)
        } else if matches!(eval_binary_op(a, &BinaryOp::Eq, b), Ok(Value::Bool(true))) {
            Some(std::cmp::Ordering::Equal)
        } else {
            None
        }
    }

    /// Cypher ORDER BY total precedence:
    /// MAP < NODE < RELATIONSHIP < LIST < PATH < STRING < BOOLEAN < TEMPORAL < NUMBER < NaN < NULL
    fn order_by_type_rank(v: &Value) -> u8 {
        match v {
            Value::Map(map) => Self::map_order_rank(map),
            Value::Node(_) => 1,
            Value::Edge(_) => 2,
            Value::List(_) => 3,
            Value::Path(_) => 4,
            Value::String(_) => 5,
            Value::Bool(_) => 6,
            Value::Temporal(_) => 7,
            Value::Int(_) => 8,
            Value::Float(f) if f.is_nan() => 9,
            Value::Float(_) => 8,
            Value::Null => 10,
            Value::Bytes(_) | Value::Vector(_) => 11,
            _ => 11,
        }
    }

    fn map_order_rank(map: &HashMap<String, Value>) -> u8 {
        if Self::map_as_temporal(map).is_some() {
            7
        } else if map.contains_key("nodes")
            && (map.contains_key("relationships") || map.contains_key("edges"))
        {
            4
        } else if map.contains_key("_eid")
            || map.contains_key("_src")
            || map.contains_key("_dst")
            || map.contains_key("_type")
            || map.contains_key("_type_name")
        {
            2
        } else if map.contains_key("_vid")
            || map.contains_key("_labels")
            || map.contains_key("_label")
        {
            1
        } else {
            0
        }
    }

    fn extract_temporal_value(value: &Value) -> Option<TemporalValue> {
        match value {
            Value::Temporal(t) => Some(t.clone()),
            Value::Map(map) => Self::map_as_temporal(map),
            Value::String(s) => Self::string_as_temporal(s),
            _ => None,
        }
    }

    fn string_as_temporal(s: &str) -> Option<TemporalValue> {
        let fn_name = match classify_temporal(s)? {
            TemporalType::Date => "DATE",
            TemporalType::LocalTime => "LOCALTIME",
            TemporalType::Time => "TIME",
            TemporalType::LocalDateTime => "LOCALDATETIME",
            TemporalType::DateTime => "DATETIME",
            TemporalType::Duration => "DURATION",
        };
        match eval_datetime_function(fn_name, &[Value::String(s.to_string())]).ok()? {
            Value::Temporal(tv) => Some(tv),
            _ => None,
        }
    }

    fn map_as_temporal(map: &HashMap<String, Value>) -> Option<TemporalValue> {
        if map.len() != 1 {
            return None;
        }

        let as_i32 = |v: &Value| v.as_i64().and_then(|n| i32::try_from(n).ok());
        let as_i64 = |v: &Value| v.as_i64();

        if let Some(Value::Map(inner)) = map.get("Date") {
            let days = inner.get("days_since_epoch").and_then(as_i32)?;
            return Some(TemporalValue::Date {
                days_since_epoch: days,
            });
        }
        if let Some(Value::Map(inner)) = map.get("LocalTime") {
            let nanos = inner.get("nanos_since_midnight").and_then(as_i64)?;
            return Some(TemporalValue::LocalTime {
                nanos_since_midnight: nanos,
            });
        }
        if let Some(Value::Map(inner)) = map.get("Time") {
            let nanos = inner.get("nanos_since_midnight").and_then(as_i64)?;
            let offset = inner.get("offset_seconds").and_then(as_i32)?;
            return Some(TemporalValue::Time {
                nanos_since_midnight: nanos,
                offset_seconds: offset,
            });
        }
        if let Some(Value::Map(inner)) = map.get("LocalDateTime") {
            let nanos = inner.get("nanos_since_epoch").and_then(as_i64)?;
            return Some(TemporalValue::LocalDateTime {
                nanos_since_epoch: nanos,
            });
        }
        if let Some(Value::Map(inner)) = map.get("DateTime") {
            let nanos = inner.get("nanos_since_epoch").and_then(as_i64)?;
            let offset = inner.get("offset_seconds").and_then(as_i32)?;
            let timezone_name = match inner.get("timezone_name") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            };
            return Some(TemporalValue::DateTime {
                nanos_since_epoch: nanos,
                offset_seconds: offset,
                timezone_name,
            });
        }
        if let Some(Value::Map(inner)) = map.get("Duration") {
            let months = inner.get("months").and_then(as_i64)?;
            let days = inner.get("days").and_then(as_i64)?;
            let nanos = inner.get("nanos").and_then(as_i64)?;
            return Some(TemporalValue::Duration {
                months,
                days,
                nanos,
            });
        }
        None
    }

    fn compare_lists(left: &[Value], right: &[Value]) -> std::cmp::Ordering {
        for (l, r) in left.iter().zip(right.iter()) {
            let ord = Self::compare_values(l, r);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        left.len().cmp(&right.len())
    }

    fn compare_maps(
        left: &HashMap<String, Value>,
        right: &HashMap<String, Value>,
    ) -> std::cmp::Ordering {
        let mut l_pairs: Vec<_> = left.iter().collect();
        let mut r_pairs: Vec<_> = right.iter().collect();
        l_pairs.sort_by(|(lk, _), (rk, _)| lk.cmp(rk));
        r_pairs.sort_by(|(lk, _), (rk, _)| lk.cmp(rk));

        for ((lk, lv), (rk, rv)) in l_pairs.iter().zip(r_pairs.iter()) {
            let key_ord = lk.cmp(rk);
            if key_ord != std::cmp::Ordering::Equal {
                return key_ord;
            }
            let val_ord = Self::compare_values(lv, rv);
            if val_ord != std::cmp::Ordering::Equal {
                return val_ord;
            }
        }

        l_pairs.len().cmp(&r_pairs.len())
    }

    fn compare_nodes(left: &uni_common::Node, right: &uni_common::Node) -> std::cmp::Ordering {
        let mut l_labels = left.labels.clone();
        let mut r_labels = right.labels.clone();
        l_labels.sort();
        r_labels.sort();

        let labels_ord = l_labels.cmp(&r_labels);
        if labels_ord != std::cmp::Ordering::Equal {
            return labels_ord;
        }

        let vid_ord = left.vid.cmp(&right.vid);
        if vid_ord != std::cmp::Ordering::Equal {
            return vid_ord;
        }

        Self::compare_maps(&left.properties, &right.properties)
    }

    fn compare_edges(left: &uni_common::Edge, right: &uni_common::Edge) -> std::cmp::Ordering {
        let edge_type_ord = left.edge_type.cmp(&right.edge_type);
        if edge_type_ord != std::cmp::Ordering::Equal {
            return edge_type_ord;
        }

        let src_ord = left.src.cmp(&right.src);
        if src_ord != std::cmp::Ordering::Equal {
            return src_ord;
        }

        let dst_ord = left.dst.cmp(&right.dst);
        if dst_ord != std::cmp::Ordering::Equal {
            return dst_ord;
        }

        let eid_ord = left.eid.cmp(&right.eid);
        if eid_ord != std::cmp::Ordering::Equal {
            return eid_ord;
        }

        Self::compare_maps(&left.properties, &right.properties)
    }

    fn compare_paths(left: &uni_common::Path, right: &uni_common::Path) -> std::cmp::Ordering {
        for (ln, rn) in left.nodes.iter().zip(right.nodes.iter()) {
            let ord = Self::compare_nodes(ln, rn);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        let node_len_ord = left.nodes.len().cmp(&right.nodes.len());
        if node_len_ord != std::cmp::Ordering::Equal {
            return node_len_ord;
        }

        for (le, re) in left.edges.iter().zip(right.edges.iter()) {
            let ord = Self::compare_edges(le, re);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        left.edges.len().cmp(&right.edges.len())
    }

    fn compare_temporal(left: &TemporalValue, right: &TemporalValue) -> std::cmp::Ordering {
        match (left, right) {
            (
                TemporalValue::Date {
                    days_since_epoch: l,
                },
                TemporalValue::Date {
                    days_since_epoch: r,
                },
            ) => l.cmp(r),
            (
                TemporalValue::LocalTime {
                    nanos_since_midnight: l,
                },
                TemporalValue::LocalTime {
                    nanos_since_midnight: r,
                },
            ) => l.cmp(r),
            (
                TemporalValue::Time {
                    nanos_since_midnight: lm,
                    offset_seconds: lo,
                },
                TemporalValue::Time {
                    nanos_since_midnight: rm,
                    offset_seconds: ro,
                },
            ) => {
                let l_utc = *lm as i128 - (*lo as i128) * 1_000_000_000;
                let r_utc = *rm as i128 - (*ro as i128) * 1_000_000_000;
                l_utc.cmp(&r_utc)
            }
            (
                TemporalValue::LocalDateTime {
                    nanos_since_epoch: l,
                },
                TemporalValue::LocalDateTime {
                    nanos_since_epoch: r,
                },
            ) => l.cmp(r),
            (
                TemporalValue::DateTime {
                    nanos_since_epoch: l,
                    ..
                },
                TemporalValue::DateTime {
                    nanos_since_epoch: r,
                    ..
                },
            ) => l.cmp(r),
            (
                TemporalValue::Duration {
                    months: lm,
                    days: ld,
                    nanos: ln,
                },
                TemporalValue::Duration {
                    months: rm,
                    days: rd,
                    nanos: rn,
                },
            ) => (*lm, *ld, *ln).cmp(&(*rm, *rd, *rn)),
            _ => Self::temporal_variant_rank(left).cmp(&Self::temporal_variant_rank(right)),
        }
    }

    fn temporal_variant_rank(v: &TemporalValue) -> u8 {
        match v {
            TemporalValue::Date { .. } => 0,
            TemporalValue::LocalTime { .. } => 1,
            TemporalValue::Time { .. } => 2,
            TemporalValue::LocalDateTime { .. } => 3,
            TemporalValue::DateTime { .. } => 4,
            TemporalValue::Duration { .. } => 5,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileOutput {
    pub explain: crate::query::planner::ExplainOutput,
    pub runtime_stats: Vec<OperatorStats>,
    pub total_time_ms: u64,
    pub peak_memory_bytes: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OperatorStats {
    pub operator: String,
    pub actual_rows: usize,
    pub time_ms: f64,
    pub memory_bytes: usize,
    pub index_hits: Option<usize>,
    pub index_misses: Option<usize>,
}

impl Executor {
    /// Profiles query execution and returns results with timing statistics.
    ///
    /// Uses the DataFusion-based executor for query execution. Granular operator
    /// profiling will be added in a future release.
    pub async fn profile(
        &self,
        plan: crate::query::planner::LogicalPlan,
        params: &HashMap<String, Value>,
    ) -> Result<(Vec<HashMap<String, Value>>, ProfileOutput)> {
        // Generate ExplainOutput first
        let planner =
            crate::query::planner::QueryPlanner::new(self.storage.schema_manager().schema().into());
        let explain_output = planner.explain_logical_plan(&plan)?;

        let start = Instant::now();

        // Execute using the standard execute path (DataFusion-based)
        let prop_manager = self.create_prop_manager();
        let results = self.execute(plan.clone(), &prop_manager, params).await?;

        let total_time = start.elapsed();

        // Return aggregate stats (granular operator profiling to be added later)
        let stats = vec![OperatorStats {
            operator: "DataFusion Execution".to_string(),
            actual_rows: results.len(),
            time_ms: total_time.as_secs_f64() * 1000.0,
            memory_bytes: 0,
            index_hits: None,
            index_misses: None,
        }];

        Ok((
            results,
            ProfileOutput {
                explain: explain_output,
                runtime_stats: stats,
                total_time_ms: total_time.as_millis() as u64,
                peak_memory_bytes: 0,
            },
        ))
    }

    fn create_prop_manager(&self) -> uni_store::runtime::property_manager::PropertyManager {
        uni_store::runtime::property_manager::PropertyManager::new(
            self.storage.clone(),
            self.storage.schema_manager_arc(),
            1000,
        )
    }
}
