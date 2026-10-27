# Standalone exploratory probe

This is an archived research listing, not a new workspace test or benchmark
binary. If promoted to an ongoing performance check, migrate it into the existing
Criterion benchmark layout under `crates/uni/benches/`.

The code exercises `bbtree::unsync::BBTreeMap` only. No concurrency correctness or
Uni end-to-end performance claim follows from its output.

## Reproduce

Create `/tmp/uni-bbtree-eval/src/`, save the manifest below as `Cargo.toml` and the
Rust listing below as `src/main.rs`. Download and extract the unmodified crate:

```bash
curl --fail --location https://static.crates.io/crates/bbtree/bbtree-0.1.5.crate -o /tmp/uni-bbtree-eval/bbtree-0.1.5.crate
tar -xf /tmp/uni-bbtree-eval/bbtree-0.1.5.crate -C /tmp/uni-bbtree-eval
```

Its SHA256 must be
`5fcbde4ae7859bc7cbeabf81fd0fb834fd135a6167e90828341724e9884f2f25`.
Copy the adjacent `Cargo.lock.txt` to that temporary project's `Cargo.lock` to
reuse dependency resolution. Build and run (choose a suitable CPU for your host):

```bash
RUSTC_WRAPPER= RUSTFLAGS='-C target-cpu=native' cargo build --locked --release --manifest-path /tmp/uni-bbtree-eval/Cargo.toml
taskset -c 1 /tmp/uni-bbtree-eval/target/release/uni-bbtree-probe > /tmp/uni-bbtree-eval/results-native.csv
```

The measurements include warmup and rotate implementation order over five
rounds, each sample at least 100 ms. Pinning controls migration, not clock
frequency, thermal conditions or sibling-core load. All cases are warmed; point
and interval probes repeatedly use a fixed query set. See the report for limits.
An earlier 3 x 30 ms screening run showed sample spread and was replaced with
this longer final protocol; no final-protocol samples are discarded.

## Manifest

```toml
[package]
name = "uni-bbtree-probe"
version = "0.0.0"
edition = "2024"

[workspace]

[dependencies]
bbtree = { path = "bbtree-0.1.5" }

[profile.release]
lto = "thin"
codegen-units = 1
```

## Source

```rust
use bbtree::unsync::BBTreeMap;
use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::time::Instant;

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

// Five interleaved rounds, rotating implementation order. Warm each case,
// then time at least 100ms. Samples include output construction and destruction.
fn measure(group: &str, n: usize, labels: &[&str], mut f: impl FnMut(usize) -> u64) {
    let mut checks = Vec::new();
    for i in 0..labels.len() { checks.push(f(i)); }
    assert!(checks.iter().all(|x| *x == checks[0]), "{group} output mismatch");
    for round in 0..5 {
        for j in 0..labels.len() {
            let i = (j + round) % labels.len();
            black_box(f(i));
            let start = Instant::now();
            let mut reps = 0u64;
            while start.elapsed().as_millis() < 100 {
                black_box(f(black_box(i)));
                reps += 1;
            }
            println!("{group},{n},{},{round},{:.3},{reps}", labels[i], start.elapsed().as_nanos() as f64 / reps as f64);
        }
    }
}

fn sparse_std(pairs: &[(u32, f32)]) -> (Vec<u32>, Vec<f32>) {
    let mut m = BTreeMap::<u32,f32>::new();
    for &(k,v) in pairs { if v.is_finite() { *m.entry(k).or_insert(0.) += v; } }
    (m.keys().copied().collect(), m.values().copied().collect())
}
fn sparse_bb(pairs: &[(u32, f32)]) -> (Vec<u32>, Vec<f32>) {
    let mut m = BBTreeMap::<u32,f32>::new();
    for &(k,v) in pairs { if v.is_finite() { *m.entry(k).or_insert(0.) += v; } }
    (m.keys().copied().collect(), m.values().copied().collect())
}
fn sparse_wide(pairs: &[(u32, f32)]) -> (Vec<u32>, Vec<f32>) {
    let mut m = BBTreeMap::<u64,f32>::new();
    for &(k,v) in pairs { if v.is_finite() { *m.entry(k as u64).or_insert(0.) += v; } }
    (m.keys().map(|&k| k as u32).collect(), m.values().copied().collect())
}
fn sparse_vec(pairs: &[(u32, f32)]) -> (Vec<u32>, Vec<f32>) {
    let mut pairs: Vec<_> = pairs.iter().copied().filter(|x|x.1.is_finite()).collect();
    // Stable sorting preserves duplicate summation order, including float rounding.
    pairs.sort_by_key(|x|x.0);
    let mut ids = Vec::new();
    let mut weights = Vec::<f32>::new();
    for (k,v) in pairs {
        if ids.last() == Some(&k) { *weights.last_mut().unwrap() += v; }
        else { ids.push(k); weights.push(0.0 + v); }
    }
    (ids, weights)
}

fn main() {
    println!("group,n,implementation,round,ns_per_invocation,repetitions");
    for n in [16usize,128,1024,4096] {
        for shape in ["unique", "duplicate"] {
            let pairs: Vec<_> = (0..n).map(|i| ((mix(i as u64) % if shape=="unique" {1<<30} else { (n/4) as u64 }) as u32, (i%7) as f32 * 0.125)).collect();
            let reference = sparse_std(&pairs);
            assert_eq!(reference, sparse_bb(&pairs));
            assert_eq!(reference, sparse_wide(&pairs));
            assert_eq!(reference, sparse_vec(&pairs));
            measure(&format!("sparse_{shape}"),n,&["std","bb_u32","bb_u64","stable_sort_vec"],|i| {
                let out = match i {0=>sparse_std(black_box(&pairs)),1=>sparse_bb(black_box(&pairs)),2=>sparse_wide(black_box(&pairs)),_=>sparse_vec(black_box(&pairs))};
                black_box(&out); out.0.iter().map(|&x|x as u64).sum::<u64>() ^ out.1.iter().map(|x|x.to_bits() as u64).sum::<u64>()
            });
        }
    }
    for n in [16usize,1024,100_000] {
        // Bulk label intervals: starts in ascending order, predecessor query.
        let entries:Vec<_>=(0..n).map(|i|((i*16) as u64,((i*16+16) as u64,format!("Label{}",i%8)))).collect();
        let std:BTreeMap<_,_>=entries.iter().cloned().collect();
        let bb:BBTreeMap<_,_>=entries.iter().cloned().collect();
        let queries:Vec<_>=(0..4096).map(|i|mix(i) % (n*16) as u64).collect();
        measure("bulk_predecessor_4096",n,&["std","bb","sorted_vec"],|i| {
            let mut sum=0;
            for q in black_box(&queries) {
                let (k,(end,label))=match i {0=>std.range(..=q).next_back().unwrap(),1=>bb.range(..=q).next_back().unwrap(),_=>{let j=entries.partition_point(|e|e.0<=*q)-1;let e=&entries[j];(&e.0,&e.1)}};
                assert!(*q<*end); sum += *k + label.len() as u64;
            } sum
        });
    }
    for n in [8usize,64,1024] {
        let events:Vec<_>=(0..2048).map(|i|mix(i)%(n as u64)).collect();
        measure("pin_churn_2048",n,&["std","bb"],|i| {
            let mut sum=0;
            if i==0 {
                let mut m:BTreeMap<_,usize>=(0..n as u64).map(|k|(k,1)).collect();
                for k in black_box(&events) { *m.entry(*k).or_insert(0)+=1;sum+=*m.first_key_value().unwrap().0;*m.get_mut(k).unwrap()-=1; }
            } else {
                let mut m:BBTreeMap<_,usize>=(0..n as u64).map(|k|(k,1)).collect();
                for k in black_box(&events) { *m.entry(*k).or_insert(0)+=1;sum+=*m.first_key_value().unwrap().0;*m.get_mut(k).unwrap()-=1; }
            } sum
        });
    }
    for n in [4usize,16,64] {
        let fields:Vec<_>=(0..n).map(|i|(format!("parameter_{:04}",mix(i as u64)%10000),i as u64)).collect();
        // Unique keys, intentionally random arrival order. Value=u64 is a
        // surrogate: does not model Uni Value cloning or subplan execution.
        let mut sorted=fields.clone();sorted.sort_by(|a,b|a.0.cmp(&b.0));
        assert!(sorted.windows(2).all(|w|w[0].0!=w[1].0));
        measure("canonical_key_build_hash",n,&["std","bb","sorted_vec"],|i| {
            let mut h=DefaultHasher::new();
            match i {
                0=>{let m:BTreeMap<_,_>=black_box(&fields).iter().cloned().collect();for (k,v) in &m{k.hash(&mut h);v.hash(&mut h);}},
                1=>{let m:BBTreeMap<_,_>=black_box(&fields).iter().cloned().collect();for (k,v) in &m{k.hash(&mut h);v.hash(&mut h);}},
                _=>{let mut v=black_box(&fields).clone();v.sort_by(|a,b|a.0.cmp(&b.0));for (k,v) in &v{k.hash(&mut h);v.hash(&mut h);}},
            }h.finish()
        });
    }
    for n in [1024usize,100_000] {
        // Hypothetical L0 property index, not Uni's production prefilter.
        // Properties may repeat; (property, VID) distinguishes rows. Real
        // coercion, MVCC and duplicate versions need more work.
        let rows:Vec<_>=(0..n as u64).map(|i|((mix(i)%n as u64),i)).collect();
        let std:BTreeMap<_,_>=rows.iter().map(|&(p,vid)|((p,vid),vid)).collect();
        let bb:BBTreeMap<_,_>=rows.iter().map(|&(p,vid)|((p,vid),vid)).collect();
        let packed:BBTreeMap<u128,u64>=rows.iter().map(|&(p,vid)|(((p as u128)<<64 | vid as u128),vid)).collect();
        let mut sorted=rows.clone(); sorted.sort();
        let queries:Vec<_>=(0..16).map(|i|mix(i)%(n-16) as u64).collect();
        measure("property_range_16queries",n,&["scan","std","bb_tuple","bb_packed_u128","sorted_vec"],|i| {
            let mut sum=0u64;
            for &lo in black_box(&queries) {
                match i {
                    0=>{for &(p,vid) in &rows {if p>=lo && p<lo+16 {sum+=vid;}}},
                    1=>{for (_,v) in std.range((lo,0)..(lo+16,0)){sum+=*v;}},
                    2=>{for (_,v) in bb.range((lo,0)..(lo+16,0)){sum+=*v;}},
                    3=>{for (_,v) in packed.range(((lo as u128)<<64)..(((lo+16) as u128)<<64)){sum+=*v;}},
                    _=>{let start=sorted.partition_point(|e|e.0<lo);let end=sorted.partition_point(|e|e.0<lo+16);for e in &sorted[start..end]{sum+=e.1;}},
                }
            }sum
        });
    }
    for n in [1024usize,100_000,1_000_000] {
        let keys:Vec<_>=(0..n as u64).map(mix).collect();
        let std:BTreeMap<_,_>=keys.iter().map(|&k|(k,k^42)).collect();
        let bb:BBTreeMap<_,_>=keys.iter().map(|&k|(k,k^42)).collect();
        let hash:HashMap<_,_>=keys.iter().map(|&k|(k,k^42)).collect();
        let queries:Vec<_>=(0..4096).map(|i|keys[(mix(i) % n as u64) as usize]).collect();
        measure("point_hits_4096",n,&["std","bb","hash","bb_batch32"],|i| {
            let mut sum=0u64;
            if i==3 {for chunk in black_box(&queries).chunks_exact(32) {let qs=std::array::from_fn::<_,32,_>(|j|&chunk[j]);for v in bb.get_many(qs){sum=sum.wrapping_add(*v.unwrap());}}}
            else {for q in black_box(&queries) {sum=sum.wrapping_add(*match i {0=>std.get(q),1=>bb.get(q),_=>hash.get(q)}.unwrap());}}sum
        });
    }
}
```
