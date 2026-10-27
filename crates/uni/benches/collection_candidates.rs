// SPDX-License-Identifier: Apache-2.0
//! Real-Value collection experiments; indexes remain outside the live database.
//! COLLECTION_PROFILE selects one kernel for perf; COLLECTION_E2E=1 profiles
//! public queries instead of running Criterion. See the evaluation report.
#[path = "support/collection_candidates.rs"]
mod kernels;
#[path = "support/l0_comparison.rs"]
mod l0_comparison;

use criterion::{BenchmarkId, Criterion, Throughput};
use kernels::*;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uni_common::core::{id::Vid, schema::DataType};
use uni_common::{Properties, Value};
use uni_store::backend::types::CmpOp;
use uni_store::runtime::{
    L0Buffer,
    columnar_scan::{L0PropertyPrefilter, l0_prefilter_candidates},
    l0_visibility::L0Context,
};

fn sparse_input(n: usize, duplicate: bool) -> Vec<(u32, f32)> {
    (0..n)
        .map(|i| {
            (
                (mix(i as u64)
                    % if duplicate {
                        (n / 4).max(1) as u64
                    } else {
                        1 << 30
                    }) as u32,
                (i % 7) as f32 * 0.125,
            )
        })
        .collect()
}

fn params(n: usize) -> Properties {
    (0..n)
        .map(|i| {
            (
                format!("parameter_{:04}", mix(i as u64) % 10000),
                match i % 4 {
                    0 => Value::Int(i as i64),
                    1 => Value::String("payload".repeat(8)),
                    2 => Value::List(vec![Value::Int(i as i64), Value::Null]),
                    _ => Value::Map(HashMap::from([("nested".into(), Value::Float(i as f64))])),
                },
            )
        })
        .collect()
}

fn digest<T: Hash>(value: T) -> u64 {
    let mut h = DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

fn fixture(n: usize) -> L0Context {
    let mut l0 = L0Buffer::new(1, None);
    for i in 0..n {
        l0.insert_vertex_with_labels(
            Vid::new(i as u64),
            HashMap::from([
                ("x".into(), Value::Int((mix(i as u64) % n as u64) as i64)),
                ("payload".into(), Value::String("payload".repeat(8))),
            ]),
            &["N".into()],
        );
    }
    L0Context::with_current(Arc::new(RwLock::new(l0)))
}

fn predicate(n: usize) -> L0PropertyPrefilter {
    L0PropertyPrefilter {
        property: "x".into(),
        one_of: None,
        bounds: vec![
            (CmpOp::GtEq, Value::Int(n as i64 / 2)),
            (CmpOp::Lt, Value::Int(n as i64 / 2 + 16)),
        ],
    }
}

#[inline(never)]
fn scan(ctx: &L0Context, pre: &L0PropertyPrefilter) -> std::collections::HashSet<u64> {
    l0_prefilter_candidates(ctx, "N", None, &DataType::Int64, pre, &HashMap::new(), [])
}

fn intervals(n: usize, random: bool, vector: bool) -> u64 {
    let mut tree = BTreeMap::new();
    let mut vec = IntervalVec::default();
    let mut sum = 0;
    for i in 0..n as u64 {
        // The random case models allocation interleaving, not just an immutable lookup.
        let start = if random {
            mix(i) % n as u64 * 16
        } else {
            i * 16
        };
        if vector {
            vec.insert(start, start + 8, format!("L{}", i % 8));
        } else {
            tree.insert(start, (start + 8, format!("L{}", i % 8)));
        }
        for raw in [start, start + 8, mix(i) % ((n as u64) * 16)] {
            let label = if vector {
                vec.get(raw)
            } else {
                tree.range(..=raw)
                    .next_back()
                    .and_then(|(_, (end, label))| (raw < *end).then_some(label.as_str()))
            };
            sum += label.map_or(0, |l| l.len() as u64);
        }
    }
    sum
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("collection/sparse_value");
    for n in [16, 128, 1024, 4096] {
        for duplicate in [false, true] {
            let input = sparse_input(n, duplicate);
            for (name, kernel) in [
                ("tree", sparse_tree as fn(&[(u32, f32)]) -> Value),
                ("sort", sparse_sort),
            ] {
                group.throughput(Throughput::Elements(n as u64));
                group.bench_with_input(
                    BenchmarkId::new(format!("{name}/dup{duplicate}"), n),
                    &input,
                    |b, input| b.iter(|| black_box(kernel(black_box(input)))),
                );
            }
        }
    }
    group.finish();
    let mut group = c.benchmark_group("collection/canonical_value");
    for n in [4, 16, 64] {
        let input = params(n);
        group.bench_with_input(BenchmarkId::new("tree_build_hash", n), &input, |b, p| {
            b.iter(|| black_box(digest(canonical_tree(black_box(p)))))
        });
        group.bench_with_input(BenchmarkId::new("vec_build_hash", n), &input, |b, p| {
            b.iter(|| black_box(digest(canonical_vec(black_box(p)))))
        });
        group.bench_with_input(BenchmarkId::new("bbtree_build_hash", n), &input, |b, p| {
            b.iter(|| {
                let map: bbtree::unsync::BBTreeMap<String, Value> = black_box(p)
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                black_box(digest(map))
            })
        });
    }
    group.finish();
    let mut group = c.benchmark_group("collection/l0_index");
    for n in [2000, 8000, 32000] {
        let ctx = fixture(n);
        let pre = predicate(n);
        let index = IntPropertyIndex::build(&ctx, "N", "x", &HashMap::new());
        assert_eq!(scan(&ctx, &pre), index.candidates(&pre, None, &[]).unwrap());
        group.bench_function(BenchmarkId::new("scan", n), |b| {
            b.iter(|| black_box(scan(black_box(&ctx), black_box(&pre))))
        });
        group.bench_function(BenchmarkId::new("query", n), |b| {
            b.iter(|| black_box(index.candidates(black_box(&pre), None, &[])))
        });
        group.bench_function(BenchmarkId::new("build_plus_query", n), |b| {
            b.iter(|| {
                let index = IntPropertyIndex::build(black_box(&ctx), "N", "x", &HashMap::new());
                black_box(index.candidates(&pre, None, &[]))
            })
        });
        let mut maintained = IntPropertyIndex::build(&ctx, "N", "x", &HashMap::new());
        let mut i = 0;
        group.bench_function(BenchmarkId::new("replace", n), |b| {
            b.iter(|| {
                i += 1;
                maintained.replace(
                    mix(i) % n as u64,
                    Value::Int((mix(i + 1) % n as u64) as i64),
                );
                black_box(maintained.len())
            })
        });
    }
    group.finish();
    let mut group = c.benchmark_group("collection/interval_mixed");
    for n in [16, 1024, 16384] {
        for random in [false, true] {
            assert_eq!(intervals(n, random, false), intervals(n, random, true));
            for vector in [false, true] {
                group.bench_function(
                    BenchmarkId::new(
                        format!("{}/random{random}", if vector { "vec" } else { "tree" }),
                        n,
                    ),
                    |b| b.iter(|| black_box(intervals(black_box(n), random, vector))),
                );
            }
        }
    }
    group.finish();
    let mut group = c.benchmark_group("collection/immutable_open");
    for n in [2000usize, 8000, 32000] {
        let entries: Vec<_> = (0..n as u64).map(|k| (k, k % 8)).collect();
        let map: bbtree::unsync::BBTreeMap<_, _> = entries.iter().copied().collect();
        let bytes = bbtree::file::FileWriter::from(&map).to_buf();
        let checked = bbtree::file::FileMap::<u64, u64>::open(&bytes).unwrap();
        for (k, v) in &entries {
            assert_eq!(checked.get(k), Some(v));
        }
        group.bench_function(BenchmarkId::new("hash_rebuild", n), |b| {
            b.iter(|| {
                black_box(
                    black_box(&entries)
                        .iter()
                        .copied()
                        .collect::<HashMap<_, _>>(),
                )
            })
        });
        group.bench_function(BenchmarkId::new("bbtree_rebuild", n), |b| {
            b.iter(|| {
                black_box(
                    black_box(&entries)
                        .iter()
                        .copied()
                        .collect::<bbtree::unsync::BBTreeMap<_, _>>(),
                )
            })
        });
        group.bench_function(BenchmarkId::new("validated_open", n), |b| {
            b.iter(|| {
                black_box(bbtree::file::FileMap::<u64, u64>::open(black_box(&bytes)).unwrap())
            })
        });
        group.bench_function(BenchmarkId::new("trusted_open", n), |b| {
            b.iter(|| {
                // SAFETY: these immutable, owned bytes came from FileWriter and
                // passed full validation above; no other process can modify them.
                black_box(unsafe {
                    bbtree::file::FileMap::<u64, u64>::open_unchecked(black_box(&bytes)).unwrap()
                })
            })
        });
    }
    group.finish();
}

fn profile(name: &str) {
    let n = std::env::var("COLLECTION_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8000);
    let repetitions = std::env::var("COLLECTION_REPETITIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let input = sparse_input(n, false);
    let params = params(16);
    let ctx = fixture(n);
    let pre = predicate(n);
    let index = IntPropertyIndex::build(&ctx, "N", "x", &HashMap::new());
    let start = Instant::now();
    for _ in 0..repetitions {
        match name {
            "sparse_tree" => {
                black_box(sparse_tree(black_box(&input)));
            }
            "sparse_sort" => {
                black_box(sparse_sort(black_box(&input)));
            }
            "canonical_tree" => {
                black_box(digest(canonical_tree(black_box(&params))));
            }
            "canonical_vec" => {
                black_box(digest(canonical_vec(black_box(&params))));
            }
            "l0_scan" => {
                black_box(scan(black_box(&ctx), black_box(&pre)));
            }
            "l0_index_query" => {
                black_box(index.candidates(black_box(&pre), None, &[]));
            }
            "l0_index_build" => {
                black_box(IntPropertyIndex::build(
                    black_box(&ctx),
                    "N",
                    "x",
                    &HashMap::new(),
                ));
            }
            "interval_tree" => {
                black_box(intervals(n, true, false));
            }
            "interval_vec" => {
                black_box(intervals(n, true, true));
            }
            _ => panic!("unknown COLLECTION_PROFILE={name}"),
        }
    }
    eprintln!(
        "PROFILE,{name},size={n},repetitions={repetitions},ns_per_iteration={:.1}",
        start.elapsed().as_nanos() as f64 / repetitions as f64
    );
}

fn literal_hint(index: &IntPropertyIndex, pre: &L0PropertyPrefilter) -> String {
    let mut vids: Vec<_> = index
        .candidates(pre, None, &[])
        .unwrap()
        .into_iter()
        .collect();
    vids.sort_unstable();
    let ids = vids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!("MATCH (n:N) WHERE id(n) IN [{ids}] AND n.x >= $lo AND n.x < $hi RETURN count(*) AS c")
}

fn e2e() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for n in [2000usize,8000,32000] {
            let db=uni_db::Uni::in_memory().config(uni_db::UniConfig {
                auto_flush_interval:None,auto_flush_threshold:usize::MAX/2,
                ..Default::default()
            }).build().await.unwrap();
            db.schema().label("N").property("x",DataType::Int64).apply().await.unwrap();
            let session=db.session();let tx=session.tx().await.unwrap();
            tx.query_with("UNWIND range(0, $last) AS i CREATE (:N {x: i})").param("last",Value::Int(n as i64-1)).fetch_all().await.unwrap();
            tx.commit().await.unwrap();
            let ddl=session.tx().await.unwrap();
            ddl.execute("CREATE INDEX collection_x FOR (n:N) ON (n.x)").await.unwrap();
            ddl.commit().await.unwrap();
            // This view is stable only because the fixture has no concurrent
            // writers, background flush, or L1 rows. This is an experiment,
            // not a way to bypass transaction visibility in production.
            let ctx=db.writer().unwrap().committed_l0_context();
            let index_start=Instant::now();
            let index=IntPropertyIndex::build(&ctx,"N","x",&HashMap::new());
            eprintln!("E2E_INDEX_BUILD,n={n},ns={}",index_start.elapsed().as_nanos());
            let pre=predicate(n);
            assert_eq!(scan(&ctx,&pre),index.candidates(&pre,None,&[]).unwrap());
            let literal=literal_hint(&index,&pre);
            for (name,query) in [
                ("range","MATCH (n:N) WHERE n.x >= $lo AND n.x < $hi RETURN count(*) AS c"),
                ("range_index_hint","MATCH (n:N) WHERE id(n) IN $vids AND n.x >= $lo AND n.x < $hi RETURN count(*) AS c"),
                ("range_literal_hint",literal.as_str()),
                ("correlated","UNWIND range(0, 127) AS i WITH i % 8 AS k CALL { WITH k MATCH (n:N) WHERE n.x = k RETURN count(*) AS c } RETURN sum(c) AS c"),
            ] {
                let warm_vids=Value::List(index.candidates(&pre,None,&[]).unwrap().into_iter().map(|v|Value::Int(v as i64)).collect());
                let warm=session.query_with(query).param("lo",Value::Int(n as i64/2)).param("hi",Value::Int(n as i64/2+16)).param("vids",warm_vids).fetch_all().await.unwrap();
                assert_eq!(warm.rows()[0].get::<i64>("c").unwrap(),if name.starts_with("range") {16} else {128});
                let mut times=Vec::new();
                for _ in 0..7 {
                    let start=Instant::now();
                    let rebuilt_literal;
                    let query=if name=="range_literal_hint" {rebuilt_literal=literal_hint(&index,&pre);rebuilt_literal.as_str()} else {query};
                    let vids=if name=="range_index_hint" {Value::List(index.candidates(&pre,None,&[]).unwrap().into_iter().map(|v|Value::Int(v as i64)).collect())} else {Value::List(vec![])};
                    let (result,plan)=session.query_with(query).param("lo",Value::Int(n as i64/2)).param("hi",Value::Int(n as i64/2+16)).param("vids",vids).profile().await.unwrap();
                    times.push(start.elapsed().as_nanos());
                    assert_eq!(result.rows()[0].get::<i64>("c").unwrap(),if name.starts_with("range") {16} else {128});
                    eprintln!("E2E_SAMPLE,{name},n={n},ns={},rows_scanned={},l0_reads={}",times.last().unwrap(),result.metrics().rows_scanned,result.metrics().l0_reads);
                    if times.len()==1 {for op in &plan.runtime_stats {eprintln!("OP,{name},n={n},{op:?}");}}
                }
                times.sort();eprintln!("E2E_MEDIAN,{name},n={n},ns={}",times[3]);
            }
            db.shutdown().await.unwrap();
        }
    });
}

fn main() {
    if std::env::var_os("COLLECTION_COMPARE").is_some() {
        l0_comparison::main();
        return;
    }
    if std::env::var_os("COLLECTION_E2E").is_some() {
        e2e();
        return;
    }
    if let Ok(name) = std::env::var("COLLECTION_PROFILE") {
        profile(&name);
        return;
    }
    let mut criterion = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_secs(1))
        .configure_from_args();
    benchmark(&mut criterion);
    criterion.final_summary();
}
