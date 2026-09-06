// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::CrdtMerge;
use fxhash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use uuid::Uuid;

/// A causal dot: `(actor id, per-actor monotonic counter)`. Globally unique by
/// construction — a given actor never re-issues a counter.
pub type Dot = (String, u64);

/// Mint a fresh, globally-unique actor id for a replica.
fn new_actor() -> String {
    Uuid::new_v4().to_string()
}

/// An Observed-Remove Set, implemented as an **ORSWOT** (Observed-Remove Set
/// Without Tombstones).
///
/// Conflict resolution is add-wins (a concurrent add + remove leaves the element
/// present). Unlike a classic tombstone-based OR-Set, this representation is
/// **tombstone-free**: element provenance is tracked with causal *dots*
/// `(actor, counter)` plus a *version vector* recording the highest counter seen
/// per actor. `remove` simply drops the element's dots; `merge` keeps a dot when
/// both replicas still hold it, or when the *other* replica's version vector has
/// not yet observed it (i.e. it is a genuinely new add, not a remove). State is
/// therefore bounded by the number of *live* elements and participating actors,
/// not by the total number of operations ever performed.
///
/// ## Wire format & backward compatibility
///
/// Serialized (v2) shape is `{ dots, vv }`. Legacy v1 payloads
/// (`{ elements, tombstones }`, opaque `Uuid` tags) are still accepted on
/// decode and upgraded in place: every element with at least one non-tombstoned
/// tag is preserved (its dead tags discarded). The per-replica `actor` is
/// runtime-only and never serialized — it is minted fresh on construction and on
/// decode, so two processes loading the same persisted blob can never collide on
/// a shared actor id.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    from = "ORSetWire<T>",
    into = "ORSetWireV2<T>",
    bound(
        serialize = "T: Serialize + Hash + Eq + Clone",
        deserialize = "T: Deserialize<'de> + Hash + Eq + Clone"
    )
)]
pub struct ORSet<T: Hash + Eq + Clone> {
    /// element -> set of causal dots that currently keep it present.
    dots: FxHashMap<T, FxHashSet<Dot>>,
    /// version vector: actor -> highest counter observed from that actor.
    vv: FxHashMap<String, u64>,
    /// This replica's actor id. Runtime-only (not serialized).
    actor: String,
}

impl<T: Hash + Eq + Clone> Default for ORSet<T> {
    fn default() -> Self {
        Self {
            dots: FxHashMap::default(),
            vv: FxHashMap::default(),
            actor: new_actor(),
        }
    }
}

impl<T: Hash + Eq + Clone> ORSet<T> {
    /// Create a new, empty ORSet with a fresh replica actor id.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an independent replica: an exact copy of the observed state but
    /// with a **new** actor id. Use this — not `clone()` — when forking a set so
    /// that the two replicas can be mutated concurrently and later merged
    /// without minting colliding dots. (`clone()` is an exact snapshot copy,
    /// including the actor id.)
    pub fn fork(&self) -> Self {
        let mut forked = self.clone();
        forked.actor = new_actor();
        forked
    }

    /// Add an element. Mints a fresh dot under this replica's actor that
    /// supersedes any earlier dots for the element, and returns it.
    pub fn add(&mut self, element: T) -> Dot {
        let counter = self.vv.entry(self.actor.clone()).or_insert(0);
        *counter += 1;
        let dot: Dot = (self.actor.clone(), *counter);
        let mut set = FxHashSet::default();
        set.insert(dot.clone());
        // A new add supersedes the element's prior dots on this replica;
        // concurrent dots from other replicas are reconciled in `merge`.
        self.dots.insert(element, set);
        dot
    }

    /// Remove an element by dropping its dots. No tombstone is retained: the
    /// version vector already records these dots as "observed", so a stale copy
    /// of them cannot resurrect the element on merge, while a concurrent add
    /// (a dot the remover never saw) still wins.
    pub fn remove(&mut self, element: &T) {
        self.dots.remove(element);
    }

    /// Check if an element is in the set (present iff it has at least one dot).
    pub fn contains(&self, element: &T) -> bool {
        self.dots.get(element).is_some_and(|dots| !dots.is_empty())
    }

    /// Returns a vector of all visible elements in the set.
    pub fn elements(&self) -> Vec<T> {
        self.dots
            .iter()
            .filter(|(_, dots)| !dots.is_empty())
            .map(|(elem, _)| elem.clone())
            .collect()
    }

    /// Returns the number of visible elements.
    pub fn len(&self) -> usize {
        self.dots.values().filter(|dots| !dots.is_empty()).count()
    }

    /// Returns true if the set has no visible elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Semantic equality: two replicas are equal when they hold the same observed
/// state, regardless of their (runtime-only) actor ids.
impl<T: Hash + Eq + Clone> PartialEq for ORSet<T> {
    fn eq(&self, other: &Self) -> bool {
        self.dots == other.dots && self.vv == other.vv
    }
}

impl<T: Hash + Eq + Clone> CrdtMerge for ORSet<T> {
    fn merge(&mut self, other: &Self) {
        // Union of element keys across both replicas. Per-key reconciliation is
        // order-independent, so a plain dedup'd set of keys suffices.
        let keys: FxHashSet<T> = self.dots.keys().chain(other.dots.keys()).cloned().collect();

        let empty: FxHashSet<Dot> = FxHashSet::default();
        for key in keys {
            // Clone self's dots for this key so we can mutate `self.dots` below.
            let sd: FxHashSet<Dot> = self.dots.get(&key).cloned().unwrap_or_default();
            let od: &FxHashSet<Dot> = other.dots.get(&key).unwrap_or(&empty);

            let mut surviving: FxHashSet<Dot> = FxHashSet::default();
            // Dots both replicas still hold are kept.
            surviving.extend(sd.intersection(od).cloned());
            // Self-only dots survive unless `other` has already observed them
            // (observed-but-absent ⇒ removed by other).
            surviving.extend(
                sd.difference(od)
                    .filter(|d| d.1 > other.vv.get(&d.0).copied().unwrap_or(0))
                    .cloned(),
            );
            // Symmetric: other-only dots survive unless self has observed them.
            surviving.extend(
                od.difference(&sd)
                    .filter(|d| d.1 > self.vv.get(&d.0).copied().unwrap_or(0))
                    .cloned(),
            );

            if surviving.is_empty() {
                self.dots.remove(&key);
            } else {
                self.dots.insert(key, surviving);
            }
        }

        // Join the version vectors (pointwise max).
        for (actor, &counter) in &other.vv {
            let entry = self.vv.entry(actor.clone()).or_insert(0);
            *entry = (*entry).max(counter);
        }
    }
}

// --- Serialization wire formats -------------------------------------------

/// v2 on-disk shape (the only form we *write*).
#[derive(Serialize)]
#[serde(bound(serialize = "T: Serialize + Hash + Eq + Clone"))]
struct ORSetWireV2<T: Hash + Eq + Clone> {
    dots: FxHashMap<T, FxHashSet<Dot>>,
    vv: FxHashMap<String, u64>,
}

impl<T: Hash + Eq + Clone> From<ORSet<T>> for ORSetWireV2<T> {
    fn from(set: ORSet<T>) -> Self {
        ORSetWireV2 {
            dots: set.dots,
            vv: set.vv,
        }
    }
}

/// Permissive decode shape accepting both v2 (`dots`/`vv`) and legacy v1
/// (`elements`/`tombstones`) payloads. All fields are optional so a payload
/// carrying only one shape's fields deserializes cleanly.
#[derive(Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de> + Hash + Eq + Clone"))]
struct ORSetWire<T: Hash + Eq + Clone> {
    #[serde(default)]
    dots: Option<FxHashMap<T, FxHashSet<Dot>>>,
    #[serde(default)]
    vv: Option<FxHashMap<String, u64>>,
    // Legacy v1 fields (opaque Uuid tags).
    #[serde(default)]
    elements: Option<FxHashMap<T, FxHashSet<Uuid>>>,
    #[serde(default)]
    tombstones: Option<FxHashSet<Uuid>>,
}

impl<T: Hash + Eq + Clone> From<ORSetWire<T>> for ORSet<T> {
    fn from(wire: ORSetWire<T>) -> Self {
        let ORSetWire {
            dots,
            vv,
            elements,
            tombstones,
        } = wire;

        // v2: use as-is, mint a fresh local actor.
        //
        // #233 Tier 1: this was a permissive `if let`, so a v2 payload
        // carrying `dots` but no `vv` fell through to the v1 upgrade branch —
        // where `elements` is `None` — and decoded to an EMPTY ORSet. Total
        // silent element loss where the payload plainly identified itself as
        // v2. This crate's own writer always emits both, so the shape is not
        // producible here, but the decoder accepts foreign bytes.
        match (dots, vv) {
            (Some(dots), Some(vv)) => {
                return ORSet {
                    dots,
                    vv,
                    actor: new_actor(),
                };
            }
            (Some(dots), None) => {
                // The version vector is derivable from the dots: every dot an
                // element carries has been observed by definition. This is a
                // lower bound (a `vv` entry can legitimately outlive the dots
                // it dominates), but it keeps every element, where falling
                // through to the v1 branch dropped all of them.
                tracing::warn!(
                    "ORSet decode: v2 payload carries `dots` but no `vv`; deriving the version \
                     vector from the observed dots rather than decoding an empty set",
                );
                let mut vv: FxHashMap<String, u64> = FxHashMap::default();
                for tags in dots.values() {
                    for (actor, counter) in tags {
                        let slot = vv.entry(actor.clone()).or_default();
                        *slot = (*slot).max(*counter);
                    }
                }
                return ORSet {
                    dots,
                    vv,
                    actor: new_actor(),
                };
            }
            (None, Some(vv)) => {
                // No dots is a legitimate state (everything removed), so the
                // version vector alone decodes faithfully to an empty set.
                return ORSet {
                    dots: FxHashMap::default(),
                    vv,
                    actor: new_actor(),
                };
            }
            (None, None) => {}
        }

        // v1 → v2 upgrade: keep elements with at least one live tag, assigning
        // each a synthetic dot under a freshly-minted actor unique to this
        // decode; discard tombstones.
        //
        // The upgrade actor MUST be per-decode and globally unique (never a
        // shared constant). Two replicas that independently upgrade v1 payloads
        // would otherwise mint colliding dots such as `(shared, 1)`, and each
        // replica's version vector would falsely claim to have "observed" the
        // other's synthetic dots. On merge those self-only dots fail the strict
        // `> other.vv` survival test and the elements are silently dropped.
        // A distinct actor per decode makes every upgraded dot genuinely
        // unobserved by any other replica, preserving add-wins semantics.
        let tombstones = tombstones.unwrap_or_default();
        let actor = new_actor();
        let mut new_dots: FxHashMap<T, FxHashSet<Dot>> = FxHashMap::default();
        let mut counter: u64 = 0;
        if let Some(elements) = elements {
            for (elem, tags) in elements {
                if tags.iter().any(|tag| !tombstones.contains(tag)) {
                    counter += 1;
                    let mut set = FxHashSet::default();
                    set.insert((actor.clone(), counter));
                    new_dots.insert(elem, set);
                }
            }
        }
        let mut new_vv = FxHashMap::default();
        if counter > 0 {
            new_vv.insert(actor.clone(), counter);
        }
        ORSet {
            dots: new_dots,
            vv: new_vv,
            actor,
        }
    }
}

#[cfg(test)]
mod tests {

    /// A v2 payload missing `vv` must keep its elements, not decode as empty.
    ///
    /// #233 Tier 1. `From<ORSetWire>` matched `(Some(dots), Some(vv))` with a
    /// permissive `if let`, so a payload carrying `dots` but no `vv` fell
    /// through to the v1 upgrade branch — where `elements` is `None` — and
    /// produced an EMPTY set. Total silent element loss for a payload that
    /// plainly identified itself as v2. This crate's own writer always emits
    /// both fields, but the decoder accepts foreign bytes.
    #[test]
    fn a_v2_payload_without_a_version_vector_keeps_its_elements() {
        let mut set: ORSet<String> = ORSet::new();
        set.add("alpha".to_owned());
        set.add("beta".to_owned());

        // Round-trip through JSON and drop `vv`, leaving a half-populated v2.
        let full = serde_json::to_value(&set).expect("serializes");
        let dots = full.get("dots").expect("v2 payload carries dots").clone();
        let half = serde_json::json!({ "dots": dots });

        let decoded: ORSet<String> = serde_json::from_value(half).expect("decodes");
        let mut got: Vec<String> = decoded.elements();
        got.sort();
        assert_eq!(
            got,
            vec!["alpha".to_owned(), "beta".to_owned()],
            "a v2 payload without `vv` must keep its elements; an empty set here is total \
             silent element loss"
        );
    }
    use super::*;
    use crate::Crdt;

    #[test]
    fn test_add_remove() {
        let mut os = ORSet::new();
        os.add("apple".to_string());
        assert!(os.contains(&"apple".to_string()));

        os.remove(&"apple".to_string());
        assert!(!os.contains(&"apple".to_string()));
    }

    #[test]
    fn test_add_wins() {
        let mut a = ORSet::new();
        a.add("apple".to_string());

        // Fork to an independent replica (new actor) before diverging.
        let mut b = a.fork();
        b.remove(&"apple".to_string());

        // Concurrent add on 'a'.
        a.add("apple".to_string());

        a.merge(&b);

        // Add wins: a's new dot was never observed by b, so b's remove can't
        // suppress it.
        assert!(a.contains(&"apple".to_string()));
    }

    #[test]
    fn test_merge() {
        let mut a = ORSet::new();
        a.add(1);
        a.add(2);

        let mut b = ORSet::new();
        b.add(2);
        b.add(3);

        a.merge(&b);

        let elements = a.elements();
        assert!(elements.contains(&1));
        assert!(elements.contains(&2));
        assert!(elements.contains(&3));
        assert_eq!(elements.len(), 3);
    }

    #[test]
    fn merge_is_commutative_and_idempotent() {
        let mut a = ORSet::new();
        a.add("x".to_string());
        let mut b = a.fork();
        b.add("y".to_string());
        b.remove(&"x".to_string());

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        assert_eq!(ab, ba, "merge must be commutative");

        // Idempotent: merging again changes nothing.
        let mut ab2 = ab.clone();
        ab2.merge(&b);
        assert_eq!(ab, ab2, "merge must be idempotent");
    }

    /// The core H10 fix: serialized state stays bounded under churn instead of
    /// growing with the number of operations (the old tombstone set leaked).
    #[test]
    fn serialized_size_bounded_under_churn() {
        let mut a = ORSet::new();
        let mut b = a.fork();

        let size_after =
            |set: &ORSet<String>| -> usize { Crdt::ORSet(set.clone()).to_msgpack().unwrap().len() };

        // Churn the same element many times across two replicas + merge.
        for _ in 0..1000 {
            a.add("k".to_string());
            a.remove(&"k".to_string());
            b.add("k".to_string());
            b.remove(&"k".to_string());
            a.merge(&b);
            b.merge(&a);
        }

        // Two actors, no live elements → tiny, O(actors) state. The old
        // tombstone-based ORSet grew ~4000 dead tags here.
        let bytes = size_after(&a);
        assert!(
            bytes < 256,
            "serialized churned ORSet should stay small, got {bytes} bytes"
        );
        assert!(a.is_empty());
    }

    /// Legacy v1 `{elements, tombstones}` payloads must still decode, recovering
    /// exactly the live element set and dropping tombstoned ones.
    #[test]
    fn v1_payload_decodes_and_upgrades() {
        // Hand-built v1 JSON: "live" present, "dead" tombstoned.
        let v1 = serde_json::json!({
            "t": "os",
            "d": {
                "elements": {
                    "live": ["6f9619ff-8b86-d011-b42d-00cf4fc964ff"],
                    "dead": ["7f9619ff-8b86-d011-b42d-00cf4fc964ff"]
                },
                "tombstones": ["7f9619ff-8b86-d011-b42d-00cf4fc964ff"]
            }
        });

        let crdt: Crdt = serde_json::from_value(v1).expect("v1 payload must decode");
        let Crdt::ORSet(os) = crdt else {
            panic!("expected ORSet");
        };
        assert!(os.contains(&"live".to_string()), "live element preserved");
        assert!(
            !os.contains(&"dead".to_string()),
            "tombstoned element dropped"
        );
        assert_eq!(os.len(), 1);

        // And it re-serializes in the v2 shape.
        let json = serde_json::to_value(Crdt::ORSet(os)).unwrap();
        let d = json.get("d").unwrap();
        assert!(d.get("dots").is_some(), "re-serializes as v2 (dots)");
        assert!(d.get("vv").is_some(), "re-serializes as v2 (vv)");
    }

    #[test]
    fn v2_roundtrip_preserves_visibility() {
        let mut os = ORSet::new();
        os.add("a".to_string());
        os.add("b".to_string());
        os.remove(&"b".to_string());

        let bytes = Crdt::ORSet(os.clone()).to_msgpack().unwrap();
        let Crdt::ORSet(decoded) = Crdt::from_msgpack(&bytes).unwrap() else {
            panic!("expected ORSet");
        };
        assert!(decoded.contains(&"a".to_string()));
        assert!(!decoded.contains(&"b".to_string()));
        assert_eq!(os, decoded, "v2 round-trip is state-preserving");
    }
}
