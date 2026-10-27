//! Experimental collection kernels shared by the evaluation bench and its
//! differential tests. No index in this module is installed in a live database.

use std::collections::{BTreeMap, HashMap, HashSet};
use uni_common::{Properties, Value};
use uni_store::backend::types::CmpOp;
use uni_store::runtime::columnar_scan::L0PropertyPrefilter;
use uni_store::runtime::l0_visibility::L0Context;

pub fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

pub fn sparse_tree(pairs: &[(u32, f32)]) -> Value {
    let mut map = BTreeMap::<u32, f32>::new();
    for &(term, weight) in pairs {
        if weight.is_finite() {
            *map.entry(term).or_default() += weight;
        }
    }
    Value::SparseVector {
        indices: map.keys().copied().collect(),
        values: map.values().copied().collect(),
    }
}

pub fn sparse_sort(pairs: &[(u32, f32)]) -> Value {
    let mut pairs: Vec<_> = pairs.iter().copied().filter(|p| p.1.is_finite()).collect();
    // Stability preserves the floating-point addition order for duplicate IDs.
    pairs.sort_by_key(|p| p.0);
    let mut indices = Vec::with_capacity(pairs.len());
    let mut values = Vec::<f32>::with_capacity(pairs.len());
    for (term, weight) in pairs {
        if indices.last() == Some(&term) {
            *values.last_mut().unwrap() += weight;
        } else {
            indices.push(term);
            // The old entry().or_default() starts every sum at positive zero.
            values.push(0.0 + weight);
        }
    }
    Value::SparseVector { indices, values }
}

pub fn canonical_tree(params: &Properties) -> BTreeMap<String, Value> {
    params.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

pub fn canonical_vec(params: &Properties) -> Vec<(String, Value)> {
    let mut entries: Vec<_> = params.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    // Keys of a HashMap are unique; stability adds no semantic guarantee here.
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    entries
}

/// Identical index semantics with statically dispatched ordered-key backends.
pub trait OrderedKeys: Default {
    fn insert(&mut self, key: (i64, u64));
    fn remove(&mut self, key: (i64, u64));
    fn vids(&self, lo: i64, hi: i64) -> impl Iterator<Item = u64>;
}

#[derive(Default)]
pub struct StdKeys(BTreeMap<(i64, u64), ()>);
#[derive(Default)]
pub struct BbKeys(bbtree::unsync::BBTreeMap<(i64, u64), ()>);
#[derive(Default)]
pub struct StdPackedKeys(BTreeMap<u128, ()>);
#[derive(Default)]
pub struct BbPackedKeys(bbtree::unsync::BBTreeMap<u128, ()>);

fn packed(value: i64, vid: u64) -> u128 {
    (((value as u64 ^ (1 << 63)) as u128) << 64) | vid as u128
}

macro_rules! tuple_keys {
    ($ty:ty) => {
        impl OrderedKeys for $ty {
            fn insert(&mut self, key: (i64, u64)) {
                self.0.insert(key, ());
            }
            fn remove(&mut self, key: (i64, u64)) {
                self.0.remove(&key);
            }
            fn vids(&self, lo: i64, hi: i64) -> impl Iterator<Item = u64> {
                self.0.range((lo, 0)..=(hi, u64::MAX)).map(|(k, _)| k.1)
            }
        }
    };
}
tuple_keys!(StdKeys);
tuple_keys!(BbKeys);
macro_rules! packed_keys {
    ($ty:ty) => {
        impl OrderedKeys for $ty {
            fn insert(&mut self, key: (i64, u64)) {
                self.0.insert(packed(key.0, key.1), ());
            }
            fn remove(&mut self, key: (i64, u64)) {
                self.0.remove(&packed(key.0, key.1));
            }
            fn vids(&self, lo: i64, hi: i64) -> impl Iterator<Item = u64> {
                self.0
                    .range(packed(lo, 0)..=packed(hi, u64::MAX))
                    .map(|(k, _)| *k as u64)
            }
        }
    };
}
packed_keys!(StdPackedKeys);
packed_keys!(BbPackedKeys);

pub type IntPropertyIndex = PropertyIndex<StdKeys>;
pub type BbPropertyIndex = PropertyIndex<BbKeys>;
pub type StdPackedPropertyIndex = PropertyIndex<StdPackedKeys>;
pub type BbPackedPropertyIndex = PropertyIndex<BbPackedKeys>;

/// Frozen, flattened INT64 property view. A live implementation must maintain
/// per-generation indexes and obey freeze/flush/transaction visibility rules.
/// The rows table is retained to charge and measure replacement maintenance.
#[derive(Default)]
pub struct PropertyIndex<T: OrderedKeys> {
    property: Option<String>,
    ordered: T,
    fallback: HashSet<u64>,
    rows: HashMap<u64, Value>,
}

impl<T: OrderedKeys> PropertyIndex<T> {
    pub fn build(
        ctx: &L0Context,
        label: &str,
        property: &str,
        label_only: &HashMap<u64, (Properties, u64)>,
    ) -> Self {
        let mut index = Self {
            property: Some(property.to_owned()),
            ..Self::default()
        };
        for buffer in ctx.iter_l0_buffers() {
            let buffer = buffer.read();
            if let Some(members) = buffer.label_to_vids.get(label) {
                for vid in members {
                    if let Some(value) = buffer
                        .vertex_properties
                        .get(vid)
                        .and_then(|p| p.get(property))
                    {
                        index.replace(vid.as_u64(), value.clone());
                    }
                }
            }
        }
        // A relabel starts from the flushed base row, replacing prior verdicts.
        for (vid, (props, _)) in label_only {
            index.replace(*vid, props.get(property).cloned().unwrap_or(Value::Null));
        }
        index
    }

    pub fn replace(&mut self, vid: u64, value: Value) {
        self.remove(vid);
        match &value {
            Value::Int(v) => {
                self.ordered.insert((*v, vid));
            }
            Value::Null => {}
            _ => {
                self.fallback.insert(vid);
            }
        }
        self.rows.insert(vid, value);
    }

    pub fn remove(&mut self, vid: u64) {
        if let Some(Value::Int(v)) = self.rows.remove(&vid) {
            self.ordered.remove((v, vid));
        }
        self.fallback.remove(&vid);
    }

    pub fn candidates(
        &self,
        prefilter: &L0PropertyPrefilter,
        vid_range: Option<(u64, u64)>,
        shadowed: &[u64],
    ) -> Option<HashSet<u64>> {
        if self
            .property
            .as_ref()
            .is_some_and(|name| name != &prefilter.property)
        {
            return None;
        }
        if prefilter.one_of.is_none() && prefilter.bounds.is_empty() {
            return None;
        }
        let in_range = |vid: &u64| vid_range.is_none_or(|(lo, hi)| *vid >= lo && *vid < hi);
        let mut lo = i64::MIN;
        let mut hi = i64::MAX;
        let mut empty = false;
        for (op, value) in &prefilter.bounds {
            let Value::Int(v) = value else {
                return None;
            };
            match op {
                CmpOp::Gt => match v.checked_add(1) {
                    Some(v) => lo = lo.max(v),
                    None => empty = true,
                },
                CmpOp::GtEq => lo = lo.max(*v),
                CmpOp::Lt => match v.checked_sub(1) {
                    Some(v) => hi = hi.min(v),
                    None => empty = true,
                },
                CmpOp::LtEq => hi = hi.min(*v),
                _ => return None,
            }
        }
        let one_of = prefilter.one_of.as_ref().map(|values| {
            values
                .iter()
                .map(|v| match v {
                    Value::Int(v) => Some(*v),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
        });
        let one_of = match one_of {
            Some(Some(v)) => Some(v),
            Some(None) => return None,
            None => None,
        };
        if one_of.as_ref().is_some_and(Vec::is_empty) {
            return Some(
                shadowed
                    .iter()
                    .filter(|vid| in_range(vid))
                    .copied()
                    .collect(),
            );
        }
        let mut out: HashSet<_> = self
            .fallback
            .iter()
            .filter(|vid| in_range(vid))
            .copied()
            .collect();
        if !empty && lo <= hi {
            if let Some(values) = one_of {
                for v in values.into_iter().filter(|v| *v >= lo && *v <= hi) {
                    out.extend(self.ordered.vids(v, v).filter(in_range));
                }
            } else {
                out.extend(self.ordered.vids(lo, hi).filter(in_range));
            }
        }
        out.extend(shadowed.iter().filter(|vid| in_range(vid)).copied());
        Some(out)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

/// Sorted intervals with ordered insertion, not an append-only assumption.
/// Replacement at the same start retains BTreeMap::insert semantics.
#[derive(Default)]
pub struct IntervalVec(pub Vec<(u64, (u64, String))>);

impl IntervalVec {
    pub fn insert(&mut self, start: u64, end: u64, label: String) {
        match self.0.binary_search_by_key(&start, |e| e.0) {
            Ok(i) => self.0[i].1 = (end, label),
            Err(i) => self.0.insert(i, (start, (end, label))),
        }
    }

    pub fn get(&self, vid: u64) -> Option<&str> {
        let i = self.0.partition_point(|e| e.0 <= vid).checked_sub(1)?;
        let (_, (end, label)) = &self.0[i];
        (vid < *end).then_some(label.as_str())
    }
}
