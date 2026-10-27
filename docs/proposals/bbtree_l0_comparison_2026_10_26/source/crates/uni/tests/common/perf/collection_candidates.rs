//! Differential correctness for experimental collection kernels.
#[path = "../../../benches/support/collection_candidates.rs"]
mod candidates;

use candidates::*;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;
use uni_common::core::id::Vid;
use uni_common::core::schema::DataType;
use uni_common::{Properties, Value};
use uni_store::backend::types::CmpOp;
use uni_store::runtime::L0Buffer;
use uni_store::runtime::columnar_scan::{L0PropertyPrefilter, l0_prefilter_candidates};
use uni_store::runtime::l0_visibility::L0Context;

fn bits(v: Value) -> (Vec<u32>, Vec<u32>) {
    let Value::SparseVector { indices, values } = v else {
        panic!()
    };
    (indices, values.into_iter().map(f32::to_bits).collect())
}

#[test]
fn collection_sparse_preserves_bits_and_sum_order() {
    let cases = [
        vec![],
        vec![(1, -0.0)],
        vec![(1, f32::NAN), (2, f32::INFINITY)],
        vec![(2, f32::MAX), (1, -0.0), (2, f32::MAX), (2, -f32::MAX)],
        vec![(0, 1e20), (1, 3.), (0, -1e20), (0, 1.)],
    ];
    for pairs in cases {
        assert_eq!(bits(sparse_tree(&pairs)), bits(sparse_sort(&pairs)));
    }
    for seed in 0..64 {
        let pairs: Vec<_> = (0..1024)
            .map(|i| {
                (
                    (mix(i + seed) % 47) as u32,
                    f32::from_bits(mix(i * 7 + seed) as u32),
                )
            })
            .collect();
        assert_eq!(
            bits(sparse_tree(&pairs)),
            bits(sparse_sort(&pairs)),
            "seed {seed}"
        );
    }
}

#[derive(Default)]
struct CollidingHasher;
impl Hasher for CollidingHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}

#[test]
fn collection_canonical_values_survive_hash_collisions() {
    let a: Properties = [
        ("x".into(), Value::Int(1)),
        (
            "nested".into(),
            Value::List(vec![Value::Map(HashMap::from([("a".into(), Value::Null)]))]),
        ),
    ]
    .into();
    let entries: Vec<_> = a.iter().collect();
    let b: Properties = entries
        .into_iter()
        .rev()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(canonical_vec(&a), canonical_vec(&b));
    assert_eq!(
        canonical_tree(&a).into_iter().collect::<Vec<_>>(),
        canonical_vec(&a)
    );
    let mut cache: HashMap<_, _, BuildHasherDefault<CollidingHasher>> = HashMap::default();
    let mut other = a.clone();
    other.insert("x".into(), Value::Int(2));
    cache.insert(canonical_vec(&a), 1);
    cache.insert(canonical_vec(&other), 2);
    assert_eq!(cache.get(&canonical_vec(&b)), Some(&1));
    assert_eq!(cache.get(&canonical_vec(&other)), Some(&2));
    for seed in 0..32 {
        let p: Properties = (0..32)
            .map(|i| (format!("k{}", mix(i + seed) % 17), Value::Int(i as i64)))
            .collect();
        assert_eq!(
            canonical_tree(&p).into_iter().collect::<Vec<_>>(),
            canonical_vec(&p)
        );
    }
    let special: Properties = HashMap::from([
        ("zero".into(), Value::Float(-0.0)),
        (
            "nan".into(),
            Value::Float(f64::from_bits(0x7ff8_0000_0000_0042)),
        ),
    ]);
    let mut equivalent = special.clone();
    equivalent.insert("zero".into(), Value::Float(0.0));
    equivalent.insert("nan".into(), Value::Float(f64::NAN));
    cache.insert(canonical_vec(&special), 3);
    assert_eq!(cache.get(&canonical_vec(&equivalent)), Some(&3));
}

#[test]
fn collection_int_index_matches_visibility_prefilter() {
    for seed in 0..16 {
        let mut buffers = Vec::new();
        for generation in 0..3 {
            let mut l0 = L0Buffer::new(generation, None);
            for vid in 0..96 {
                if mix(vid + generation + seed) % 4 == 0 {
                    continue;
                }
                let value = match mix(vid * 7 + generation + seed) % 7 {
                    0 => Value::Null,
                    1 => Value::Float(3.2),
                    2 => Value::String("coerced".into()),
                    _ => Value::Int((mix(vid + generation) % 23) as i64 - 11),
                };
                l0.insert_vertex_with_labels(
                    Vid::new(vid),
                    HashMap::from([("x".into(), value)]),
                    &["N".into()],
                );
            }
            buffers.push(Arc::new(RwLock::new(l0)));
        }
        let ctx = L0Context {
            pending_flush_l0s: vec![buffers[0].clone()],
            current_l0: Some(buffers[1].clone()),
            transaction_l0: Some(buffers[2].clone()),
        };
        let label_only = HashMap::from([
            (5, (Properties::new(), 4)),
            (6, (HashMap::from([("x".into(), Value::Int(9))]), 4)),
        ]);
        let index = IntPropertyIndex::build(&ctx, "N", "x", &label_only);
        let bb = BbPropertyIndex::build(&ctx, "N", "x", &label_only);
        let sp = StdPackedPropertyIndex::build(&ctx, "N", "x", &label_only);
        let bp = BbPackedPropertyIndex::build(&ctx, "N", "x", &label_only);
        for (op, bound) in [
            (CmpOp::Gt, i64::MAX),
            (CmpOp::Lt, i64::MIN),
            (CmpOp::GtEq, 0),
            (CmpOp::LtEq, 4),
        ] {
            for one_of in [None, Some(vec![]), Some(vec![Value::Int(1), Value::Int(9)])] {
                for range in [None, Some((3, 80))] {
                    let pre = L0PropertyPrefilter {
                        property: "x".into(),
                        one_of: one_of.clone(),
                        bounds: vec![(op, Value::Int(bound))],
                    };
                    let expected = l0_prefilter_candidates(
                        &ctx,
                        "N",
                        range,
                        &DataType::Int64,
                        &pre,
                        &label_only,
                        [2, 5, 500],
                    );
                    assert_eq!(bb.candidates(&pre, range, &[2, 5, 500]).unwrap(), expected);
                    assert_eq!(sp.candidates(&pre, range, &[2, 5, 500]).unwrap(), expected);
                    assert_eq!(bp.candidates(&pre, range, &[2, 5, 500]).unwrap(), expected);
                    assert_eq!(
                        index.candidates(&pre, range, &[2, 5, 500]).unwrap(),
                        expected,
                        "seed {seed}"
                    );
                }
            }
        }
        let unsupported = L0PropertyPrefilter {
            property: "x".into(),
            one_of: Some(vec![Value::Float(1.0)]),
            bounds: vec![],
        };
        assert!(index.candidates(&unsupported, None, &[]).is_none());
    }
}

#[test]
fn collection_index_replacements_and_interval_interleavings() {
    let mut index = IntPropertyIndex::default();
    let pre = L0PropertyPrefilter {
        property: "x".into(),
        one_of: None,
        bounds: vec![(CmpOp::Lt, Value::Int(5))],
    };
    index.replace(1, Value::Int(2));
    assert!(index.candidates(&pre, None, &[]).unwrap().contains(&1));
    index.replace(1, Value::Int(8));
    assert!(!index.candidates(&pre, None, &[]).unwrap().contains(&1));
    index.replace(1, Value::Float(8.1));
    assert!(index.candidates(&pre, None, &[]).unwrap().contains(&1));
    index.remove(1);
    assert!(index.candidates(&pre, None, &[]).unwrap().is_empty());
    let mut tree = BTreeMap::new();
    let mut vector = IntervalVec::default();
    for i in 0..256 {
        let start = (mix(i) % 128) * 16;
        let end = start + 8;
        let label = format!("L{i}");
        tree.insert(start, (end, label.clone()));
        vector.insert(start, end, label);
        for vid in [0, 7, start, start + 7, start + 8, u64::MAX] {
            let expected = tree
                .range(..=vid)
                .next_back()
                .and_then(|(_, (end, label))| (vid < *end).then_some(label.as_str()));
            assert_eq!(vector.get(vid), expected);
        }
    }
}

#[test]
fn collection_backend_mutation_equivalence() {
    fn check<T: OrderedKeys>() {
        let mut index = PropertyIndex::<T>::default();
        let mut oracle = IntPropertyIndex::default();
        for i in 0..4096 {
            let vid = mix(i) % 257;
            if i % 11 == 0 {
                index.remove(vid);
                oracle.remove(vid);
            } else {
                let v = match i % 7 {
                    0 => Value::Int(i64::MIN),
                    1 => Value::Int(i64::MAX),
                    2 => Value::Null,
                    3 => Value::String("fallback".into()),
                    _ => Value::Int(mix(i + 1) as i64),
                };
                index.replace(vid, v.clone());
                oracle.replace(vid, v);
            }
            if i % 31 == 0 {
                for (lo, hi) in [
                    (i64::MIN, i64::MAX),
                    (-1, 1),
                    (i64::MIN, i64::MIN),
                    (i64::MAX, i64::MAX),
                ] {
                    let pre = L0PropertyPrefilter {
                        property: "x".into(),
                        one_of: None,
                        bounds: vec![(CmpOp::GtEq, Value::Int(lo)), (CmpOp::LtEq, Value::Int(hi))],
                    };
                    assert_eq!(
                        index.candidates(&pre, None, &[999]),
                        oracle.candidates(&pre, None, &[999])
                    );
                }
            }
        }
    }
    check::<BbKeys>();
    check::<StdPackedKeys>();
    check::<BbPackedKeys>();
}

#[test]
fn collection_packed_backend_sustained_churn() {
    let n = 32000;
    let mut standard = StdPackedPropertyIndex::default();
    let mut bb = BbPackedPropertyIndex::default();
    for vid in 0..n {
        let value = Value::Int((mix(vid) % n) as i64);
        standard.replace(vid, value.clone());
        bb.replace(vid, value);
    }
    for i in 0..1_000_000 {
        let vid = mix(i) % n;
        let value = Value::Int(mix(i + 99) as i64);
        standard.replace(vid, value.clone());
        bb.replace(vid, value.clone());
        if i % 100_000 == 99_999 {
            for (lo, hi) in [(i64::MIN, i64::MAX), (i64::MIN, -1), (0, 32000)] {
                let pre = L0PropertyPrefilter {
                    property: "x".into(),
                    one_of: None,
                    bounds: vec![(CmpOp::GtEq, Value::Int(lo)), (CmpOp::LtEq, Value::Int(hi))],
                };
                assert_eq!(
                    bb.candidates(&pre, None, &[]),
                    standard.candidates(&pre, None, &[])
                );
            }
            let pre = L0PropertyPrefilter {
                property: "x".into(),
                one_of: Some(vec![value]),
                bounds: vec![],
            };
            assert_eq!(
                bb.candidates(&pre, None, &[]),
                standard.candidates(&pre, None, &[])
            );
        }
    }
    assert_eq!(bb.len(), n as usize);
}
