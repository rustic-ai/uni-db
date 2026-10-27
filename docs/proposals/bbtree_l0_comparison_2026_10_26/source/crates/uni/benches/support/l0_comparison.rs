//! Controlled backend comparison. This measures an auxiliary index, not a live WAL.
use super::{fixture, kernels::*};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;
use uni_common::Value;
use uni_store::backend::types::CmpOp;
use uni_store::runtime::{columnar_scan::L0PropertyPrefilter, l0_visibility::L0Context};

struct Meter;
static ENABLED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
static CALLS: AtomicU64 = AtomicU64::new(0);
#[global_allocator]
static ALLOCATOR: Meter = Meter;
fn add(n: usize) {
    let live = LIVE.fetch_add(n as i64, Relaxed) + n as i64;
    PEAK.fetch_max(live, Relaxed);
    ALLOCATED.fetch_add(n as u64, Relaxed);
    CALLS.fetch_add(1, Relaxed);
}
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() && ENABLED.load(Relaxed) {
            add(layout.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() && ENABLED.load(Relaxed) {
            add(layout.size());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        if ENABLED.load(Relaxed) {
            LIVE.fetch_sub(layout.size() as i64, Relaxed);
        }
        unsafe { System.dealloc(p, layout) }
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() && ENABLED.load(Relaxed) {
            LIVE.fetch_sub(layout.size() as i64, Relaxed);
            add(size);
        }
        q
    }
}
fn pre(n: usize, width: usize) -> L0PropertyPrefilter {
    L0PropertyPrefilter {
        property: "x".into(),
        one_of: None,
        bounds: vec![
            (CmpOp::GtEq, Value::Int(n as i64 / 2)),
            (CmpOp::Lt, Value::Int((n / 2 + width) as i64)),
        ],
    }
}
fn timed(name: &str, n: usize, round: usize, op: &str, reps: usize, f: impl FnOnce()) {
    let start = Instant::now();
    f();
    println!(
        "TIME,{name},{n},{round},{op},{reps},{}",
        start.elapsed().as_nanos()
    );
}
fn run<T: OrderedKeys>(name: &str, n: usize, round: usize, ctx: &L0Context) {
    let start = Instant::now();
    let mut index = PropertyIndex::<T>::build(ctx, "N", "x", &HashMap::new());
    println!(
        "TIME,{name},{n},{round},build,1,{}",
        start.elapsed().as_nanos()
    );
    for (op, width, reps) in [
        ("point", 1, 2000),
        ("range16", 16, 2000),
        ("range_half", n / 2, 20),
    ] {
        let pred = pre(n, width);
        timed(name, n, round, op, reps, || {
            for _ in 0..reps {
                black_box(index.candidates(black_box(&pred), None, &[]));
            }
        });
    }
    let hit = (mix(0) % n as u64) as i64;
    let pred = L0PropertyPrefilter {
        property: "x".into(),
        one_of: Some(vec![Value::Int(hit)]),
        bounds: vec![],
    };
    timed(name, n, round, "point_hit", 2000, || {
        for _ in 0..2000 {
            black_box(index.candidates(black_box(&pred), None, &[]));
        }
    });
    timed(name, n, round, "insert", 2048, || {
        for i in 0..2048 {
            index.replace((n + i) as u64, Value::Int(mix(i as u64) as i64));
        }
    });
    timed(name, n, round, "replace", 4096, || {
        for i in 0..4096 {
            index.replace(
                mix(i as u64) % n as u64,
                Value::Int(mix(i as u64 + 99) as i64),
            );
        }
    });
    timed(name, n, round, "delete", 2048, || {
        for i in 0..2048 {
            index.remove(mix(i as u64) % n as u64);
        }
    });
    // A freeze transfers ownership; retiring it drops the entire auxiliary index.
    timed(name, n, round, "retire_drop", 1, || drop(index));
    timed(name, n, round, "rebuild_drop", 1, || {
        black_box(PropertyIndex::<T>::build(ctx, "N", "x", &HashMap::new()));
    });
}
fn memory<T: OrderedKeys>(name: &str, n: usize, ctx: &L0Context) {
    LIVE.store(0, Relaxed);
    PEAK.store(0, Relaxed);
    ALLOCATED.store(0, Relaxed);
    CALLS.store(0, Relaxed);
    ENABLED.store(true, Relaxed);
    let mut index = PropertyIndex::<T>::build(ctx, "N", "x", &HashMap::new());
    let retained = LIVE.load(Relaxed);
    let build_peak = PEAK.load(Relaxed);
    let allocated = ALLOCATED.load(Relaxed);
    let calls = CALLS.load(Relaxed);
    for i in 0..n {
        index.replace(i as u64, Value::Int(mix(i as u64 + 11) as i64));
    }
    let replaced = LIVE.load(Relaxed);
    for i in 0..n {
        index.remove(i as u64);
    }
    let empty = LIVE.load(Relaxed);
    drop(index);
    let after_drop = LIVE.load(Relaxed);
    ENABLED.store(false, Relaxed);
    assert_eq!(after_drop, 0);
    println!(
        "MEM,{name},{n},{retained},{build_peak},{allocated},{calls},{replaced},{empty},{after_drop}"
    );
}
fn dispatch(name: &str, n: usize, round: usize, ctx: &L0Context, mem: bool) {
    macro_rules! go {
        ($ty:ty) => {
            if mem {
                memory::<$ty>(name, n, ctx)
            } else {
                run::<$ty>(name, n, round, ctx)
            }
        };
    }
    match name {
        "std_tuple" => go!(StdKeys),
        "bb_tuple" => go!(BbKeys),
        "std_packed" => go!(StdPackedKeys),
        "bb_packed" => go!(BbPackedKeys),
        _ => panic!(),
    }
}
fn profile<T: OrderedKeys>(name: &str, op: &str) {
    let n = 32000;
    let ctx = fixture(n);
    let mut index = PropertyIndex::<T>::build(&ctx, "N", "x", &HashMap::new());
    let pred = if op == "point_hit" {
        L0PropertyPrefilter {
            property: "x".into(),
            one_of: Some(vec![Value::Int((mix(0) % n as u64) as i64)]),
            bounds: vec![],
        }
    } else {
        pre(n, n / 2)
    };
    let reps = if op == "range_half" { 1000 } else { 1000000 };
    timed(name, n, 0, op, reps, || {
        for i in 0..reps {
            if op == "replace" {
                index.replace(
                    mix(i as u64) % n as u64,
                    Value::Int(mix(i as u64 + 99) as i64),
                );
            } else {
                black_box(index.candidates(black_box(&pred), None, &[]));
            }
        }
    });
}
pub fn main() {
    if let Ok(selector) = std::env::var("COLLECTION_COMPARE_PROFILE") {
        let (name, op) = selector.split_once(':').unwrap();
        assert!(["point_hit", "range_half", "replace"].contains(&op));
        match name {
            "std_tuple" => profile::<StdKeys>(name, op),
            "bb_tuple" => profile::<BbKeys>(name, op),
            "std_packed" => profile::<StdPackedKeys>(name, op),
            "bb_packed" => profile::<BbPackedKeys>(name, op),
            _ => panic!(),
        }
        return;
    }

    let variants = ["std_tuple", "bb_tuple", "std_packed", "bb_packed"];
    for n in [2000, 8000, 32000, 128000] {
        let ctx = fixture(n);
        // Same context for all backends; preflight exact candidate equivalence.
        let a = IntPropertyIndex::build(&ctx, "N", "x", &HashMap::new());
        let b = BbPropertyIndex::build(&ctx, "N", "x", &HashMap::new());
        let c = StdPackedPropertyIndex::build(&ctx, "N", "x", &HashMap::new());
        let d = BbPackedPropertyIndex::build(&ctx, "N", "x", &HashMap::new());
        for width in [1, 16, n / 2] {
            let p = pre(n, width);
            let expected = a.candidates(&p, None, &[]);
            assert_eq!(expected, b.candidates(&p, None, &[]));
            assert_eq!(expected, c.candidates(&p, None, &[]));
            assert_eq!(expected, d.candidates(&p, None, &[]));
        }
        for (op, pred) in [
            ("point", pre(n, 1)),
            ("range16", pre(n, 16)),
            ("range_half", pre(n, n / 2)),
            (
                "point_hit",
                L0PropertyPrefilter {
                    property: "x".into(),
                    one_of: Some(vec![Value::Int((mix(0) % n as u64) as i64)]),
                    bounds: vec![],
                },
            ),
        ] {
            let expected = a.candidates(&pred, None, &[]);
            assert_eq!(expected, b.candidates(&pred, None, &[]));
            assert_eq!(expected, c.candidates(&pred, None, &[]));
            assert_eq!(expected, d.candidates(&pred, None, &[]));
            println!("MATCHES,{n},{op},{}", expected.unwrap().len());
        }
        drop((a, b, c, d));
        for round in 0..8 {
            for j in 0..4 {
                dispatch(variants[(j + round) % 4], n, round, &ctx, false);
            }
        }
        for name in variants {
            dispatch(name, n, 0, &ctx, true);
        }
    }
}
